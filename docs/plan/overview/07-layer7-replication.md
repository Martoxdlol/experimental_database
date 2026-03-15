# Layer 7: Replication (Optional)

**Layer purpose:** Implements the `ReplicationHook` trait defined by L5 (Transactions). Handles cluster membership, WAL streaming, snapshot transfer, transaction promotion, and recovery tier selection. This layer provides the replication TRANSPORT and CLUSTER COORDINATION — L5's replication task calls `replicate_and_wait` for each committed WAL record; L7 handles the network transport and quorum acknowledgement.

## Design Principles

1. **Implements, never imported by Database**: L7 implements the `ReplicationHook` trait from L5 (injected into L5's `ReplicationRunner` at construction time by L6). The database never imports L7. Dependency flows: L7 → L5 (trait), L7 → L2 (WAL access).
2. **Single connection per node pair**: Every pair of nodes shares exactly one bidirectional TCP connection. The node with the lower `NodeId` initiates. All traffic — heartbeats, WAL streaming, acks, snapshots — flows on this single connection.
3. **Optional**: A single-node embedded database uses `NoReplication` (from L6) and never touches this layer.
4. **Quorum-aware**: All replication decisions (commit acknowledgement, read serving, state transitions) are gated on quorum — a majority of the cluster being reachable.

## Modules

### `cluster.rs` — Cluster Topology & Mesh

**WHY HERE:** Maintains the cluster topology and a live view of which nodes are up, suspect, or down. Every other module in L7 depends on this to make quorum decisions. This is pure cluster-coordination infrastructure — no WAL logic, no data transport.

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::storage::wal::Lsn;
use crate::types::Ts;

pub type NodeId = u64;
pub type Generation = u64;

pub struct ClusterConfig {
    pub self_id: NodeId,
    pub topology: HashMap<NodeId, SocketAddr>,  // all nodes including self
    pub heartbeat_interval: Duration,           // default 500ms
    pub suspect_timeout: Duration,              // default 3s
    pub down_timeout: Duration,                 // default 10s
    pub primary_ack_timeout: Duration,          // default 5s
    pub replica_fence_timeout: Duration,        // default 2s
    pub wal_retention_duration: Duration,       // default 24h — keep WAL segments at least this long
    pub wal_retention_size: usize,              // default 1GB — keep at least this much WAL
}

pub enum NodeState { Online, Suspect, Down }

pub struct NodeStatus {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: NodeState,
    pub last_heartbeat: Instant,
    pub applied_lsn: Lsn,
    pub applied_ts: Ts,
}

pub struct ClusterMembership {
    config: ClusterConfig,
    nodes: RwLock<HashMap<NodeId, NodeStatus>>,
}

impl ClusterMembership {
    pub fn new(config: ClusterConfig) -> Self;

    /// Number of nodes currently Online (including self)
    pub fn online_count(&self) -> usize;

    /// Whether we have quorum (online_count > cluster_size / 2)
    pub fn has_quorum(&self) -> bool;

    /// List of online replica node IDs (excludes self)
    pub fn online_replicas(&self) -> Vec<NodeId>;

    /// Mark a node's heartbeat as received
    pub fn record_heartbeat(&self, node_id: NodeId, lsn: Lsn, ts: Ts);

    /// Check and update node states based on timeouts
    fn check_timeouts(&self);

    /// Get current cluster size (for quorum calculation)
    pub fn cluster_size(&self) -> usize;
}
```

#### Dynamic Topology Updates

The cluster topology can be modified at runtime to support operational changes without full restarts:

```rust
impl ClusterMembership {
    /// Update peer addresses at runtime (e.g., node moved to new address)
    pub fn update_peer_addr(&self, node_id: NodeId, new_addr: SocketAddr);

    /// Add a new node to the topology (cluster expansion)
    pub fn add_node(&self, node_id: NodeId, addr: SocketAddr);

    /// Remove a node from the topology (cluster shrink)
    /// Only allowed if node is Down and cluster retains quorum without it
    pub fn remove_node(&self, node_id: NodeId) -> Result<()>;
}
```

`update_peer_addr` is used when a node migrates to a new IP or port — the mesh reconnects to the new address on the next heartbeat cycle. `add_node` expands the cluster: the new node is initially marked `Down` and transitions to `Online` once it completes its handshake and begins heartbeating. `remove_node` shrinks the cluster but is guarded: it fails if the target node is not `Down`, or if removing it would break quorum (e.g., removing a node from a 3-node cluster that already has one node down).

#### All-to-All Heartbeat Protocol

Every node in the cluster participates in a fully connected heartbeat mesh:

- Every node sends a heartbeat message to every other node at `heartbeat_interval` (default 500ms). Heartbeats flow on the same bidirectional TCP connection used for all other peer communication (see PeerMesh below).
- Each heartbeat includes: `node_id`, `applied_lsn`, `applied_ts`. This gives every node a reasonably fresh view of every other node's replication progress.
- The `check_timeouts()` method runs on a tick (at `heartbeat_interval`) and transitions node states:
  - If no heartbeat received from a node within `suspect_timeout` (default 3s, ~6 missed heartbeats) → mark **Suspect**. Suspect nodes still count toward quorum — this avoids flapping on transient network blips.
  - If no heartbeat received within `down_timeout` (default 10s, ~20 missed heartbeats) → mark **Down**. Down nodes are excluded from quorum calculation.
- On the **primary**: Down nodes are excluded from the set of replicas that must acknowledge commits.
- On **any node**: if `online_count <= cluster_size / 2`, the node has lost quorum and must self-fence. For the primary this means entering hold state (stop accepting commits). For a replica this means stop accepting new read transactions.

The heartbeat mesh is symmetric: every node runs the same logic regardless of role. Role-specific behavior (hold state, read fencing) is handled by the modules that consume `ClusterMembership`.

### `peer_mesh.rs` — Bidirectional Peer Connections

**WHY HERE:** Manages the single-connection-per-node-pair mesh that all L7 communication flows through. Heartbeats, WAL streaming, acks, catch-up requests, and snapshot transfers all share the same TCP connection per peer. This replaces the previous model where the primary listened for replica connections separately.

#### Connection Model

Each node pair has exactly ONE TCP connection. The node with the **lower** `NodeId` initiates the connection. The node with the higher `NodeId` listens. All communication is bidirectional on this single connection.

On connection establishment, both sides exchange a handshake:

```
Handshake message:
  node_id: NodeId
  generation: Generation  // incremented when a node restarts fresh (no local state)
  role: NodeRole (Primary | Replica)
  applied_lsn: Lsn
  applied_ts: Ts
```

**Generation** is determined locally by the connecting node, not stored in `ClusterConfig`:
- If the node has intact local data from a previous run → same generation as before (read from L2's `FileHeader`).
- If the node is a fresh replacement with no local state → `generation = old_generation + 1` (or `1` if entirely new).

The cluster uses `(node_id, generation)` to distinguish **reconnection** from **replacement**:
- **Same generation** → the node has local state. Try Tier 1 (incremental WAL catch-up) or Tier 2 (local recovery then catch-up).
- **New generation** → fresh instance with no local data. Try Tier 1 (if sufficient WAL is retained on the primary) or fall back to Tier 3 (full snapshot).

#### Message Protocol

All messages on a peer connection use a framed protocol. The message types are:

```rust
enum PeerMessage {
    Heartbeat { node_id: NodeId, applied_lsn: Lsn, applied_ts: Ts },
    WalRecord { lsn: Lsn, data: Vec<u8> },
    WalAck { lsn: Lsn },
    RequestCatchup { from_lsn: Lsn },  // replica asks primary for WAL from this point
    SnapshotBegin,                      // full snapshot transfer follows
    SnapshotData { chunk: Vec<u8> },
    SnapshotEnd,
}
```

- **Heartbeat**: Sent by both sides at `heartbeat_interval`. Carries replication progress.
- **WalRecord**: Primary → replica. A committed WAL record that the replica must apply and acknowledge.
- **WalAck**: Replica → primary. Confirms that a WAL record has been durably applied.
- **RequestCatchup**: Replica → primary. After handshake, if the replica is behind, it requests WAL records starting from `from_lsn`. The primary responds with a stream of `WalRecord` messages.
- **SnapshotBegin/Data/End**: Primary → replica. Used for Tier 3 recovery when incremental catch-up is not possible. The primary streams a consistent snapshot as a sequence of chunks.

#### Struct Design

```rust
pub struct PeerMesh {
    cluster: Arc<ClusterMembership>,
    connections: RwLock<HashMap<NodeId, PeerConnection>>,
    self_role: RwLock<NodeRole>,
}

struct PeerConnection {
    node_id: NodeId,
    generation: Generation,
    role: NodeRole,
    stream: TcpStream,  // bidirectional
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
}

pub enum NodeRole { Primary, Replica }

impl PeerMesh {
    pub fn new(cluster: Arc<ClusterMembership>, role: NodeRole) -> Self;

    /// Start the mesh: connect to peers with lower ID, listen for peers with higher ID
    pub async fn start(&self) -> Result<()>;

    /// Send WAL record to all connected replicas, wait for quorum acks
    pub async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;

    /// Broadcast heartbeat to all peers (runs on interval)
    async fn heartbeat_loop(&self);

    /// Handle incoming messages from a peer (heartbeat, ack, WAL record)
    async fn handle_peer(&self, conn: &mut PeerConnection);
}
```

The `start()` method spawns two kinds of tasks:
- For each peer with a **lower** `NodeId` than self: listen for an incoming connection on the configured address.
- For each peer with a **higher** `NodeId` than self: initiate a TCP connection, perform the handshake, and begin the message loop.

If a connection drops, the initiating side (lower `NodeId`) reconnects with exponential backoff. The handshake on reconnection provides `generation` and `applied_lsn`, allowing the peer to determine whether incremental catch-up or full snapshot is needed.

### WAL Retention Policy

WAL segments are retained beyond checkpoint based on multiple constraints. The primary computes:

```
min_safe_truncation_lsn = min(
    oldest_lsn_within_retention_duration,  // time-based: keep 24h of WAL
    oldest_lsn_within_retention_size,      // size-based: keep 1GB of WAL
    min_replica_applied_lsn,               // can't truncate what replicas haven't consumed
    visible_ts_lsn                         // rollback vacuum might need WAL after visible_ts
)
```

Only segments whose highest LSN < `min_safe_truncation_lsn` may be deleted.

This enables **Tier 1 recovery**: a replica that was offline for less than `wal_retention_duration` can reconnect and catch up incrementally via `RequestCatchup` without requiring a full snapshot. The retention window provides a buffer — as long as the replica comes back within the retention period (default 24 hours) and its `applied_lsn` falls within the retained WAL range, it can resume from where it left off.

The `wal_retention_size` constraint (default 1GB) provides a secondary bound: even if the time-based retention would keep more, WAL is capped at this size. Conversely, if WAL is small, the time-based retention still applies.

**Single-node mode**: Without replication (single-node deployment), WAL retention defaults to 0 — segments are truncated immediately after checkpoint to save disk space. The retention parameters in `ClusterConfig` only apply when replication is active.

### `primary_server.rs` — Primary Replication Server

**WHY HERE:** Implements the primary-side replication by wrapping `PeerMesh` and implementing the `ReplicationHook` trait from L5. Called by L5's `ReplicationRunner` (not the writer task) — the writer is never blocked on replication. The mesh handles all transport; this module adds the commit protocol and hold-state logic.

```rust
use std::sync::atomic::AtomicBool;
use crate::database::ReplicationHook;
use crate::replication::peer_mesh::PeerMesh;

/// Implements ReplicationHook for primary-side replication
pub struct PrimaryReplicator {
    mesh: Arc<PeerMesh>,
    hold_state: AtomicBool,
}

impl ReplicationHook for PrimaryReplicator {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()> {
        if self.hold_state.load(Acquire) {
            return Err(Error::HoldState);
        }
        self.mesh.replicate_and_wait(lsn, record).await
    }

    fn has_quorum(&self) -> bool { self.mesh.cluster.has_quorum() }
    fn is_holding(&self) -> bool { self.hold_state.load(Acquire) }
}

impl PrimaryReplicator {
    pub fn new(mesh: Arc<PeerMesh>) -> Self;

    /// Get the minimum applied_lsn across all replicas (for WAL retention)
    pub fn min_replica_lsn(&self) -> Lsn;

    /// Enter hold/readonly state — stop accepting new commits
    pub fn enter_hold_state(&self);
}
```

Note that `PrimaryReplicator` no longer has its own `start()` method or listener — all connection management is handled by `PeerMesh`. The replicator is a thin wrapper that adds commit semantics on top of the mesh's transport.

#### `replicate_and_wait` Protocol Detail

The quorum commit protocol is the core of replication correctness:

1. **Pre-check:** If `is_holding()` or `!cluster.has_quorum()`, immediately enter hold state and return `Err`. No WAL records are sent.
2. **Broadcast:** Send the WAL record to all replicas that `cluster.online_replicas()` reports as online. This is best-effort — a replica going down mid-send is not fatal. The send goes through `PeerMesh`, which routes the `WalRecord` message on the existing bidirectional connection to each replica.
3. **Wait:** Block for up to `primary_ack_timeout` (default 5s) for `WalAck` messages from all online replicas.
4. **Timeout handling:** If the timeout fires before all acks arrive:
   - Re-read `cluster.online_count()` — some replicas may have transitioned to Down during the wait, shrinking the required set.
   - If the acks received (plus self) still form a majority of `cluster_size` → the commit succeeds. The timed-out replicas will catch up via `RequestCatchup` later.
   - If the acks received (plus self) do not form a majority → enter hold state, return `Err`. The caller (L6) must roll back this transaction.
5. **Success:** Return `Ok`. The transaction is durably committed on a majority of nodes.

### `replica_client.rs` — Replica Replication Client

**WHY HERE:** The replica side of replication. Rather than managing its own TCP connection to the primary, the replica participates in the `PeerMesh` and receives WAL records through it. Applies received records locally via the Database API and self-fences when quorum is lost.

```rust
pub struct ReplicaClient {
    cluster: Arc<ClusterMembership>,
    mesh: Arc<PeerMesh>,
    database: Arc<Database>,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    fenced: AtomicBool,  // true when quorum lost — stop accepting reads
}

impl ReplicaClient {
    pub fn new(mesh: Arc<PeerMesh>, database: Arc<Database>,
               cluster: Arc<ClusterMembership>) -> Self;

    /// Whether the replica is fenced (no new read transactions allowed)
    pub fn is_fenced(&self) -> bool;

    pub fn applied_lsn(&self) -> Lsn;
    pub fn applied_ts(&self) -> Ts;
}
```

The replica receives `WalRecord` messages through the mesh's `handle_peer` loop. For each received record it:
1. Writes to local WAL via `Database::storage()`.
2. Applies to the local page store.
3. Invalidates local subscriptions.
4. Sends a `WalAck` back to the primary through the same connection.

#### Replica Fencing

The replica continuously monitors `cluster.has_quorum()`. When quorum is lost (the replica cannot reach a majority of the cluster within `replica_fence_timeout`, default 2s), it sets `fenced = true` and stops accepting new read transactions. Existing in-flight reads are allowed to complete, but no new `begin_read()` calls are accepted.

This is critical for linearizability: a partitioned replica must not serve reads from stale data while the primary (on the other side of the partition) may be committing new writes that the replica has not received. See the Timeout Hierarchy section below for why this works.

When quorum is restored, the replica unfences and resumes normal operation after catching up on any missed WAL records.

### `promotion.rs` — Transaction Promotion

**WHY HERE:** Handles write transactions originating on replicas by forwarding to the primary for commit validation. Network transport concern. Promotion requests use the existing `PeerMesh` connection to the primary rather than opening a separate TCP stream. Requests are rejected when the replica is fenced (no quorum), since the primary is likely unreachable anyway.

```rust
pub struct PromotionClient {
    mesh: Arc<PeerMesh>,
}

impl PromotionClient {
    /// Send a transaction's write set to the primary for validation and commit.
    /// Returns Err if the replica is fenced (no quorum).
    pub async fn promote(
        &self,
        begin_ts: Ts,
        read_set: ReadSet,
        write_set: WriteSet,
    ) -> Result<PromotionResult>;
}

pub enum PromotionResult {
    Success { commit_ts: Ts },
    Conflict { error: ConflictError },
}
```

### `snapshot.rs` — Snapshot Transfer (Tier 3 Recovery)

**WHY HERE:** Full database reconstruction by streaming pages from a healthy node over the network. Snapshot transfer uses the existing `PeerMesh` connection via `SnapshotBegin`/`SnapshotData`/`SnapshotEnd` messages rather than separate TCP streams. The sender verifies it has quorum before starting a snapshot (to avoid sending a potentially stale snapshot from a partitioned node).

```rust
pub struct SnapshotSender {
    database: Arc<Database>,
    cluster: Arc<ClusterMembership>,
    mesh: Arc<PeerMesh>,
}

impl SnapshotSender {
    /// Stream a consistent snapshot of the entire database to a recovering node.
    /// Sends SnapshotBegin, then SnapshotData chunks, then SnapshotEnd on the
    /// peer's mesh connection.
    /// Returns Err if this node does not have quorum.
    pub async fn send_snapshot(&self, target_node: NodeId) -> Result<()>;
}

pub struct SnapshotReceiver {
    target_path: PathBuf,
    mesh: Arc<PeerMesh>,
}

impl SnapshotReceiver {
    /// Receive a full snapshot (SnapshotBegin → SnapshotData* → SnapshotEnd)
    /// from the mesh connection and write to target path.
    pub async fn receive_snapshot(&self, source_node: NodeId) -> Result<()>;
}
```

### `recovery_tiers.rs` — Recovery Tier Selection

**WHY HERE:** Determines which recovery strategy to use based on local state, the handshake information (generation and applied_lsn), and primary WAL availability. Recovery tier selection does not itself require quorum — a recovering node that has lost quorum will still attempt local recovery (Tier 2) and wait for quorum before requesting a snapshot (Tier 3) from a quorum-holding node.

```rust
pub enum RecoveryTier {
    /// Tier 1: Incremental catch-up (replica briefly offline, WAL available on primary)
    IncrementalCatchup { from_lsn: Lsn },
    /// Tier 2: Local crash recovery + incremental catch-up
    LocalRecoveryThenCatchup,
    /// Tier 3: Full reconstruction from snapshot
    FullReconstruction,
}

pub fn select_recovery_tier(
    local_state: &LocalState,
    primary_oldest_retained_lsn: Lsn,
    peer_generation: Generation,
    previous_generation: Option<Generation>,
) -> RecoveryTier;

pub struct LocalState {
    pub data_file_exists: bool,
    pub data_file_valid: bool,
    pub wal_intact: bool,
    pub applied_lsn: Option<Lsn>,
}
```

The selection logic incorporates the handshake's `generation` and `applied_lsn`:

- **Same generation** (reconnection, not replacement):
  - **Tier 1** is viable when `local_state.applied_lsn >= primary_oldest_retained_lsn`. The replica's applied_lsn falls within the primary's retained WAL window, so incremental catch-up via `RequestCatchup { from_lsn }` is sufficient.
  - **Tier 2** applies when the local data file exists and is valid but the WAL needs replay. After local recovery, the node attempts Tier 1 catch-up.
- **New generation** (fresh replacement):
  - **Tier 1** is still attempted first if the primary has retained enough WAL (i.e., `from_lsn: 0` or the earliest LSN falls within retention). This allows a fresh node to bootstrap from WAL alone without a full snapshot.
  - **Tier 3** (full snapshot) is the fallback when WAL retention is insufficient to cover the entire history needed.

The WAL retention policy (see above) directly determines Tier 1 viability: as long as the primary retains WAL back to the replica's `applied_lsn`, incremental catch-up works.

## FileHeader Generation Field

The `generation` value must survive node restarts. It is stored in L2's `FileHeader`:

- **Existing data file on startup**: Read the generation from `FileHeader`. This is the node's current generation — it has intact local state.
- **No data file (fresh start)**: `generation = 1`. This is a brand-new node.
- **Fresh replacement of an existing node**: The operator must set `generation = old_generation + 1` to signal to the cluster that this is a new instance of the same `NodeId`, not a reconnection. In practice, if no data file exists for a known `NodeId`, the node defaults to `generation = 1` and the cluster treats any generation mismatch as a new instance.

The cluster uses `(NodeId, Generation)` as a composite identity. If a peer connects with the same `NodeId` but a different `Generation` than previously seen, the cluster knows the node was replaced and adjusts recovery tier selection accordingly.

## Timeout Hierarchy

```
replica_fence_timeout (2s)  <  primary_ack_timeout (5s)
```

This ordering is critical for correctness. Here is why:

When a network partition splits the cluster, two things happen concurrently:
- The **primary** is trying to commit a write and waiting for replica acks (up to `primary_ack_timeout`).
- The **replica** notices it cannot reach a majority of the cluster and starts its fence timer (`replica_fence_timeout`).

Because `replica_fence_timeout < primary_ack_timeout`, the replica will stop serving reads **before** the primary gives up on the commit. This prevents the following anomaly:

1. Primary commits write W at time T.
2. Replica never receives W (partition).
3. Client reads from replica and gets stale data (missing W).

With the timeout ordering, by the time the primary could possibly commit W (after waiting up to 5s for acks), the replica has already fenced itself (after 2s) and is no longer serving reads. Any client that was reading from the replica has already been told to go away.

The 3s gap between the two timeouts provides margin for clock skew and network jitter. In practice, the replica fences almost immediately when it loses heartbeats (within one `suspect_timeout` cycle), well before the primary's ack timeout fires.

## Wiring: How Replication Connects to Database

```rust
// All nodes use the same mesh infrastructure.
// The topology includes all nodes (including self).

let cluster_config = ClusterConfig {
    self_id: 1,
    topology: HashMap::from([
        (1, "node1:5001".parse()?),
        (2, "node2:5001".parse()?),
        (3, "node3:5001".parse()?),
    ]),
    ..ClusterConfig::default()
};
let cluster = Arc::new(ClusterMembership::new(cluster_config));

// === Primary node ===
let mesh = Arc::new(PeerMesh::new(cluster.clone(), NodeRole::Primary));
mesh.start().await?;
let replicator = PrimaryReplicator::new(mesh.clone());
// replicator is injected into L5's ReplicationRunner (via L6's Database::open)
// The ReplicationRunner calls replicate_and_wait() — the writer is never blocked.
let db = Database::open("./data", config, Some(Box::new(replicator))).await?;

// === Replica node (same mesh infrastructure, different role) ===
let mesh = Arc::new(PeerMesh::new(cluster.clone(), NodeRole::Replica));
mesh.start().await?;
let db = Database::open("./data", config, None).await?;
// mesh handles receiving WAL records and applying them via db

// === Embedded single-node (no replication) ===
let db = Database::open("./data", config, None).await?;  // NoReplication (default)
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `ClusterMembership::new` | Operator/L8 (startup) | Initialize cluster topology |
| `ClusterMembership::has_quorum` | L7 internals, L8 (health checks) | Quorum gate for all operations |
| `ClusterMembership::update_peer_addr` / `add_node` / `remove_node` | Operator/L8 (runtime) | Dynamic topology changes |
| `ClusterMembership::cluster_size` | L7 internals | Quorum calculation |
| `PeerMesh::new` / `start` | Operator/L8 (startup) | Initialize and start the peer connection mesh |
| `PeerMesh::replicate_and_wait` | `PrimaryReplicator` | Send WAL record and wait for quorum acks |
| `PrimaryReplicator` (implements `ReplicationHook`) | L5 (ReplicationRunner, injected by L6) | Quorum-based replication, called by replication task (not writer) |
| `PrimaryReplicator::is_holding` | L8 (health checks, load balancers) | Whether primary is in hold state |
| `ReplicaClient::is_fenced` | L8 (health checks, request routing) | Whether replica is accepting reads |
| `PromotionClient::promote` | L8 (session on replica) | Write forwarding via mesh |
| `SnapshotSender` / `SnapshotReceiver` | Operator/L8 (recovery) | Full reconstruction via mesh |
| `min_replica_lsn` | L6 (WAL retention) | Segment reclamation |
