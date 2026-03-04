# Layer 7: Replication (Optional)

**Layer purpose:** Implements the `ReplicationHook` trait defined by L6 (Database). Handles WAL streaming, snapshot transfer, transaction promotion, and recovery tier selection. This layer provides the replication TRANSPORT — the database (L6) provides the replication PROTOCOL (when to call, what data).

## Design Principles

1. **Implements, never imported by Database**: L7 implements the `ReplicationHook` trait from L6. The database never imports L7. Dependency flows: L7 → L6 (and L7 → L2 for WAL access).
2. **Transport-agnostic within reason**: Uses TCP by default, but the actual transport could be anything. The database doesn't care.
3. **Optional**: A single-node embedded database uses `NoReplication` (from L6) and never touches this layer.

## Modules

### `primary_server.rs` — Primary Replication Server

**WHY HERE:** Implements the primary-side replication: streams committed WAL records to replicas and collects acknowledgements. This is replication transport — it knows how to send bytes over the network.

```rust
use crate::storage::wal::{WalReader, Lsn};
use crate::database::ReplicationHook;

/// Implements ReplicationHook for primary-side replication
pub struct PrimaryReplicator {
    wal_reader: WalReader,
    replicas: RwLock<Vec<ReplicaConnection>>,
    commit_notify: broadcast::Sender<Lsn>,
    mode: ReplicationMode,
}

struct ReplicaConnection {
    id: u64,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    writer: TcpStream,
}

impl ReplicationHook for PrimaryReplicator {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()> {
        // 1. Broadcast WAL record to all connected replicas
        // 2. Wait for acks per replication mode (strict = all, primary+1 = any 1)
    }
}

impl PrimaryReplicator {
    pub fn new(wal_reader: WalReader, mode: ReplicationMode) -> Self;

    /// Start listening for replica connections (runs in background)
    pub async fn start(&self, addr: SocketAddr) -> Result<()>;

    /// Get the minimum applied_lsn across all replicas (for WAL retention)
    pub fn min_replica_lsn(&self) -> Lsn;

    // Per-connection handler:
    // 1. Receive replica's applied_lsn
    // 2. Stream missing WAL records from that point
    // 3. On new commits: stream new records
    // 4. Receive ack messages, update applied_lsn/applied_ts
    async fn handle_replica(&self, conn: TcpStream) -> Result<()>;
}

pub enum ReplicationMode {
    Strict,         // wait for ALL replicas
    PrimaryPlusOne, // wait for at least 1 replica
}
```

### `replica_client.rs` — Replica Replication Client

**WHY HERE:** Connects to primary, receives WAL records, applies them locally via the Database API. Inverse of `primary_server.rs`.

```rust
pub struct ReplicaClient {
    primary_addr: SocketAddr,
    database: Arc<Database>,  // uses Database API to apply records
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    shutdown: CancellationToken,
}

impl ReplicaClient {
    pub fn new(primary_addr: SocketAddr, database: Arc<Database>,
               shutdown: CancellationToken) -> Self;

    /// Start the replica client (reconnects with exponential backoff)
    pub async fn run(&self) -> Result<()>;

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

### `promotion.rs` — Transaction Promotion

**WHY HERE:** Handles write transactions originating on replicas by forwarding to the primary for commit validation. Network transport concern.

```rust
pub struct PromotionClient {
    primary_addr: SocketAddr,
}

impl PromotionClient {
    /// Send a transaction's write set to the primary for validation and commit
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

**WHY HERE:** Full database reconstruction by streaming pages from a healthy node over the network.

```rust
pub struct SnapshotSender {
    database: Arc<Database>,
}

impl SnapshotSender {
    /// Stream a consistent snapshot of the entire database to a recovering node
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

**WHY HERE:** Determines which recovery strategy to use based on local state and primary WAL availability.

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

## Wiring: How Replication Connects to Database

```
// At startup, the operator decides the role:

// === Primary node ===
let replicator = PrimaryReplicator::new(wal_reader, ReplicationMode::Strict);
replicator.start("0.0.0.0:5001").await?;
let db = Database::open("./data", config, Some(Box::new(replicator))).await?;

// === Replica node ===
let db = Database::open("./data", config, None).await?;  // NoReplication during commit
let replica = ReplicaClient::new("primary:5001", db.clone(), shutdown.clone());
tokio::spawn(replica.run());

// === Embedded single-node (no replication) ===
let db = Database::open("./data", config, None).await?;  // NoReplication (default)
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `PrimaryReplicator` (implements `ReplicationHook`) | L6 (Database, via injection) | Synchronous replication during commit |
| `PrimaryReplicator::start` | Operator/L8 (startup) | Start replica listener |
| `ReplicaClient::run` | Operator/L8 (startup) | Start replica |
| `PromotionClient::promote` | L8 (session on replica) | Write forwarding |
| `SnapshotSender/Receiver` | Operator/L8 (recovery) | Full reconstruction |
| `min_replica_lsn` | L6 (WAL retention) | Segment reclamation |
