# Layer 7: Replication (Optional)

**Layer purpose:** Implements the `ReplicationHook` trait defined by L6 (Database). Handles cluster membership, WAL streaming, snapshot transfer, transaction promotion, and recovery tier selection. This layer provides the replication TRANSPORT and CLUSTER COORDINATION — the database (L6) provides the replication PROTOCOL (when to call, what data).

## Design Principles

1. **Implements, never imported by Database**: L7 implements the `ReplicationHook` trait from L6. The database never imports L7. Dependency flows: L7 → L6 (and L7 → L2 for WAL access).
2. **Transport-agnostic within reason**: Uses TCP by default, but the actual transport could be anything. The database doesn't care.
3. **Optional**: A single-node embedded database uses `NoReplication` (from L6) and never touches this layer.
4. **Quorum-aware**: All replication decisions (commit acknowledgement, read serving, state transitions) are gated on quorum — a majority of the cluster being reachable.

## Modules

### `cluster.rs` — Cluster Membership & Heartbeat

**WHY HERE:** Maintains a live view of which nodes are up, suspect, or down. Every other module in L7 depends on this to make quorum decisions. This is pure cluster-coordination infrastructure — no WAL logic, no data transport.

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::storage::wal::Lsn;
use crate::types::Ts;

pub struct ClusterConfig {
    pub node_id: u64,
    pub peers: Vec<SocketAddr>,            // all other nodes in the cluster
    pub heartbeat_interval: Duration,       // default 500ms
    pub suspect_timeout: Duration,          // default 3s — mark as suspect after this many missed heartbeats
    pub down_timeout: Duration,             // default 10s — mark as down
    pub primary_ack_timeout: Duration,      // default 5s — how long primary waits for quorum acks
    pub replica_fence_timeout: Duration,    // default 2s — replicas stop accepting reads after losing quorum
    // CRITICAL: replica_fence_timeout < primary_ack_timeout
}

pub enum NodeState { Online, Suspect, Down }

pub struct NodeStatus {
    pub node_id: u64,
    pub addr: SocketAddr,
    pub state: NodeState,
    pub last_heartbeat: Instant,
    pub applied_lsn: Lsn,
    pub applied_ts: Ts,
}

pub struct ClusterMembership {
    config: ClusterConfig,
    self_id: u64,
    nodes: RwLock<HashMap<u64, NodeStatus>>,
    cluster_size: usize,  // total nodes including self
}

impl ClusterMembership {
    pub fn new(config: ClusterConfig) -> Self;

    /// Start the all-to-all heartbeat mesh (background task)
    pub async fn start(&self) -> Result<()>;

    /// Number of nodes currently Online (including self)
    pub fn online_count(&self) -> usize;

    /// Whether we have quorum (online_count > cluster_size / 2)
    pub fn has_quorum(&self) -> bool;

    /// List of online replica node IDs (excludes self)
    pub fn online_replicas(&self) -> Vec<u64>;

    /// Mark a node's heartbeat as received
    pub fn record_heartbeat(&self, node_id: u64, lsn: Lsn, ts: Ts);

    /// Check and update node states based on timeouts
    fn check_timeouts(&self);
}
```

#### All-to-All Heartbeat Protocol

Every node in the cluster participates in a fully connected heartbeat mesh:

- Every node sends a heartbeat message to every other node at `heartbeat_interval` (default 500ms). This is a small UDP-style message (could be TCP; the point is it is lightweight).
- Each heartbeat includes: `node_id`, `applied_lsn`, `applied_ts`. This gives every node a reasonably fresh view of every other node's replication progress.
- The `check_timeouts()` method runs on a tick (at `heartbeat_interval`) and transitions node states:
  - If no heartbeat received from a node within `suspect_timeout` (default 3s, ~6 missed heartbeats) → mark **Suspect**. Suspect nodes still count toward quorum — this avoids flapping on transient network blips.
  - If no heartbeat received within `down_timeout` (default 10s, ~20 missed heartbeats) → mark **Down**. Down nodes are excluded from quorum calculation.
- On the **primary**: Down nodes are excluded from the set of replicas that must acknowledge commits.
- On **any node**: if `online_count <= cluster_size / 2`, the node has lost quorum and must self-fence. For the primary this means entering hold state (stop accepting commits). For a replica this means stop accepting new read transactions.

The heartbeat mesh is symmetric: every node runs the same logic regardless of role. Role-specific behavior (hold state, read fencing) is handled by the modules that consume `ClusterMembership`.

### `primary_server.rs` — Primary Replication Server

**WHY HERE:** Implements the primary-side replication: streams committed WAL records to replicas and collects acknowledgements. This is replication transport — it knows how to send bytes over the network. Now cluster-aware: it consults `ClusterMembership` for quorum before committing.

```rust
use std::sync::atomic::AtomicBool;
use crate::storage::wal::{WalReader, Lsn};
use crate::database::ReplicationHook;
use crate::replication::cluster::{ClusterMembership, ClusterConfig};

/// Implements ReplicationHook for primary-side replication
pub struct PrimaryReplicator {
    cluster: Arc<ClusterMembership>,
    wal_reader: WalReader,
    replicas: RwLock<Vec<ReplicaConnection>>,
    commit_notify: broadcast::Sender<Lsn>,
    mode: ReplicationMode,
    hold_state: AtomicBool,  // true when quorum lost — stop accepting commits
}

struct ReplicaConnection {
    id: u64,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    writer: TcpStream,
}

pub enum ReplicationMode {
    Quorum,  // majority of cluster (including self) must ack
}

impl ReplicationHook for PrimaryReplicator {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()> {
        // 1. Check has_quorum(). If not, enter hold state, return Err.
        // 2. Send WAL record to all online replicas.
        // 3. Wait for acks from ALL online replicas, with primary_ack_timeout.
        // 4. If timeout before all acks:
        //    a. Re-check has_quorum() (some replicas may have gone down during wait)
        //    b. If still has quorum with acks received → success
        //    c. If lost quorum → enter hold state, trigger rollback, return Err
        // 5. On success, return Ok
    }
}

impl PrimaryReplicator {
    pub fn new(wal_reader: WalReader, cluster: Arc<ClusterMembership>) -> Self;

    /// Start listening for replica connections (runs in background)
    pub async fn start(&self, addr: SocketAddr) -> Result<()>;

    /// Get the minimum applied_lsn across all replicas (for WAL retention)
    pub fn min_replica_lsn(&self) -> Lsn;

    /// Enter hold/readonly state — stop accepting new commits
    pub fn enter_hold_state(&self);

    /// Whether in hold state
    pub fn is_holding(&self) -> bool;

    // Per-connection handler:
    // 1. Receive replica's applied_lsn
    // 2. Stream missing WAL records from that point
    // 3. On new commits: stream new records
    // 4. Receive ack messages, update applied_lsn/applied_ts
    async fn handle_replica(&self, conn: TcpStream) -> Result<()>;
}
```

#### `replicate_and_wait` Protocol Detail

The quorum commit protocol is the core of replication correctness:

1. **Pre-check:** If `is_holding()` or `!cluster.has_quorum()`, immediately enter hold state and return `Err`. No WAL records are sent.
2. **Broadcast:** Send the WAL record to all replicas that `cluster.online_replicas()` reports as online. This is best-effort — a replica going down mid-send is not fatal.
3. **Wait:** Block for up to `primary_ack_timeout` (default 5s) for acknowledgements from all online replicas.
4. **Timeout handling:** If the timeout fires before all acks arrive:
   - Re-read `cluster.online_count()` — some replicas may have transitioned to Down during the wait, shrinking the required set.
   - If the acks received (plus self) still form a majority of `cluster_size` → the commit succeeds. The timed-out replicas will catch up via WAL streaming later.
   - If the acks received (plus self) do not form a majority → enter hold state, return `Err`. The caller (L6) must roll back this transaction.
5. **Success:** Return `Ok`. The transaction is durably committed on a majority of nodes.

### `replica_client.rs` — Replica Replication Client

**WHY HERE:** Connects to primary, receives WAL records, applies them locally via the Database API. Inverse of `primary_server.rs`. Participates in cluster membership and self-fences when quorum is lost.

```rust
pub struct ReplicaClient {
    cluster: Arc<ClusterMembership>,
    primary_addr: SocketAddr,
    database: Arc<Database>,  // uses Database API to apply records
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    shutdown: CancellationToken,
    fenced: AtomicBool,  // true when quorum lost — stop accepting reads
}

impl ReplicaClient {
    pub fn new(primary_addr: SocketAddr, database: Arc<Database>,
               cluster: Arc<ClusterMembership>,
               shutdown: CancellationToken) -> Self;

    /// Start the replica client (reconnects with exponential backoff)
    pub async fn run(&self) -> Result<()>;

    /// Whether the replica is fenced (no new read transactions allowed)
    pub fn is_fenced(&self) -> bool;

    // Inner loop:
    // 1. Connect to primary
    // 2. Send applied_lsn
    // 3. Receive and apply WAL records:
    //    a. Write to local WAL via Database::storage()
    //    b. Apply to local page store
    //    c. Invalidate local subscriptions
    //    d. Send ack to primary
    async fn run_replication_loop(&self) -> Result<()>;

    pub fn applied_lsn(&self) -> Lsn;
    pub fn applied_ts(&self) -> Ts;
}
```

#### Replica Fencing

The replica continuously monitors `cluster.has_quorum()`. When quorum is lost (the replica cannot reach a majority of the cluster within `replica_fence_timeout`, default 2s), it sets `fenced = true` and stops accepting new read transactions. Existing in-flight reads are allowed to complete, but no new `begin_read()` calls are accepted.

This is critical for linearizability: a partitioned replica must not serve reads from stale data while the primary (on the other side of the partition) may be committing new writes that the replica has not received. See the Timeout Hierarchy section below for why this works.

When quorum is restored, the replica unfences and resumes normal operation after catching up on any missed WAL records.

### `promotion.rs` — Transaction Promotion

**WHY HERE:** Handles write transactions originating on replicas by forwarding to the primary for commit validation. Network transport concern. Promotion requests are rejected when the replica is fenced (no quorum), since the primary is likely unreachable anyway.

```rust
pub struct PromotionClient {
    primary_addr: SocketAddr,
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

**WHY HERE:** Full database reconstruction by streaming pages from a healthy node over the network. Snapshot transfer is quorum-aware: the sender verifies it has quorum before starting a snapshot (to avoid sending a potentially stale snapshot from a partitioned node).

```rust
pub struct SnapshotSender {
    database: Arc<Database>,
    cluster: Arc<ClusterMembership>,
}

impl SnapshotSender {
    /// Stream a consistent snapshot of the entire database to a recovering node.
    /// Returns Err if this node does not have quorum.
    pub async fn send_snapshot(&self, conn: TcpStream) -> Result<()>;
}

pub struct SnapshotReceiver {
    target_path: PathBuf,
}

impl SnapshotReceiver {
    /// Receive a full snapshot and write to target path
    pub async fn receive_snapshot(&self, conn: TcpStream) -> Result<()>;
}
```

### `recovery_tiers.rs` — Recovery Tier Selection

**WHY HERE:** Determines which recovery strategy to use based on local state and primary WAL availability. Recovery tier selection does not itself require quorum — a recovering node that has lost quorum will still attempt local recovery (Tier 2) and wait for quorum before requesting a snapshot (Tier 3) from a quorum-holding node.

```rust
pub enum RecoveryTier {
    /// Tier 1: Incremental catch-up (replica briefly offline, WAL available)
    IncrementalCatchup { from_lsn: Lsn },
    /// Tier 2: Local crash recovery + incremental catch-up
    LocalRecoveryThenCatchup,
    /// Tier 3: Full reconstruction from snapshot
    FullReconstruction,
}

pub fn select_recovery_tier(
    local_state: &LocalState,
    primary_oldest_lsn: Lsn,
) -> RecoveryTier;

pub struct LocalState {
    pub data_file_exists: bool,
    pub data_file_valid: bool,
    pub wal_intact: bool,
    pub applied_lsn: Option<Lsn>,
}
```

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
// At startup, the operator decides the role:

// === Primary node ===
let cluster_config = ClusterConfig {
    node_id: 1,
    peers: vec!["node2:5001".parse()?, "node3:5001".parse()?],
    ..ClusterConfig::default()
};
let cluster = Arc::new(ClusterMembership::new(cluster_config));
cluster.start().await?;

let replicator = PrimaryReplicator::new(wal_reader, cluster.clone());
replicator.start("0.0.0.0:5001").await?;
let db = Database::open("./data", config, Some(Box::new(replicator))).await?;

// === Replica node ===
let cluster_config = ClusterConfig {
    node_id: 2,
    peers: vec!["node1:5001".parse()?, "node3:5001".parse()?],
    ..ClusterConfig::default()
};
let cluster = Arc::new(ClusterMembership::new(cluster_config));
cluster.start().await?;

let db = Database::open("./data", config, None).await?;  // NoReplication during commit
let replica = ReplicaClient::new("node1:5001".parse()?, db.clone(), cluster.clone(), shutdown.clone());
tokio::spawn(replica.run());

// === Embedded single-node (no replication) ===
let db = Database::open("./data", config, None).await?;  // NoReplication (default)
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `ClusterMembership::new` / `start` | Operator/L8 (startup) | Initialize cluster membership mesh |
| `ClusterMembership::has_quorum` | L7 internals, L8 (health checks) | Quorum gate for all operations |
| `PrimaryReplicator` (implements `ReplicationHook`) | L6 (Database, via injection) | Quorum-based replication during commit |
| `PrimaryReplicator::start` | Operator/L8 (startup) | Start replica listener |
| `PrimaryReplicator::is_holding` | L8 (health checks, load balancers) | Whether primary is in hold state |
| `ReplicaClient::run` | Operator/L8 (startup) | Start replica |
| `ReplicaClient::is_fenced` | L8 (health checks, request routing) | Whether replica is accepting reads |
| `PromotionClient::promote` | L8 (session on replica) | Write forwarding |
| `SnapshotSender/Receiver` | Operator/L8 (recovery) | Full reconstruction |
| `min_replica_lsn` | L6 (WAL retention) | Segment reclamation |
