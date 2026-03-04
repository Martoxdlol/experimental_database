# Layer 6: Replication

**Layer purpose:** WAL streaming from primary to replicas, transaction promotion (write commits from replicas), 3-tier recovery (incremental catch-up, local crash recovery, full reconstruction), and snapshot transfer.

## Modules

### `primary_server.rs` — Primary Replication Server

**WHY HERE:** Streams committed WAL records to replicas and collects acknowledgements. Replication is a distributed systems concern that sits above the commit protocol.

```rust
use crate::storage::wal::{WalReader, WalRecord, Lsn};

pub struct PrimaryReplicationServer {
    wal_reader: WalReader,
    replicas: RwLock<Vec<ReplicaConnection>>,
    commit_notify: broadcast::Receiver<Lsn>,  // notified on each new commit
}

struct ReplicaConnection {
    id: u64,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    writer: TcpStream,
}

impl PrimaryReplicationServer {
    pub fn new(wal_reader: WalReader, commit_notify: broadcast::Receiver<Lsn>) -> Self;

    /// Start the replication server listening for replica connections
    pub async fn start(&self, addr: SocketAddr) -> Result<()>;

    /// Wait for all (or required) replicas to acknowledge up to lsn
    pub async fn wait_for_ack(&self, lsn: Lsn, mode: ReplicationMode) -> Result<()>;

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
    Strict,        // wait for ALL replicas
    PrimaryPlusOne, // wait for at least 1 replica
}
```

### `replica_client.rs` — Replica Replication Client

**WHY HERE:** Connects to primary, receives WAL records, applies them locally. Inverse of `primary_server.rs`.

```rust
pub struct ReplicaClient {
    primary_addr: SocketAddr,
    storage: Arc<StorageEngine>,
    commit_coordinator: CommitHandle,  // to apply received records
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    shutdown: CancellationToken,
}

impl ReplicaClient {
    pub fn new(primary_addr: SocketAddr, storage: Arc<StorageEngine>,
               commit: CommitHandle, shutdown: CancellationToken) -> Self;

    /// Start the replica client (reconnects with exponential backoff)
    pub async fn run(&self) -> Result<()>;

    // Inner loop:
    // 1. Connect to primary
    // 2. Send applied_lsn
    // 3. Receive and apply WAL records:
    //    a. Write to local WAL
    //    b. Apply to local page store
    //    c. Invalidate local subscriptions
    //    d. Send ack to primary
    async fn run_replication_loop(&self) -> Result<()>;

    pub fn applied_lsn(&self) -> Lsn;
    pub fn applied_ts(&self) -> Ts;
}
```

### `promotion.rs` — Transaction Promotion

**WHY HERE:** Handles write transactions originating on replicas by forwarding to the primary for commit validation.

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

**WHY HERE:** Full database reconstruction by streaming pages from a healthy node.

```rust
pub struct SnapshotSender {
    storage: Arc<StorageEngine>,
}

impl SnapshotSender {
    /// Stream a consistent snapshot of the entire data.db to a recovering node
    /// 1. Begin read-only snapshot at current applied_ts
    /// 2. Stream pages one by one with checksums
    /// 3. Buffer new WAL records arriving during transfer
    /// 4. Send buffered WAL records after page transfer completes
    pub async fn send_snapshot(&self, conn: TcpStream) -> Result<()>;
}

pub struct SnapshotReceiver {
    target_path: PathBuf,
}

impl SnapshotReceiver {
    /// Receive a full snapshot and write to target path
    /// 1. Receive pages, verify checksums, write to new data.db
    /// 2. Receive WAL records, write to wal/
    /// 3. Write meta.json with snapshot checkpoint_lsn
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

/// Determine the appropriate recovery tier
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

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `PrimaryReplicationServer::start` | Integration (startup) | Start replication |
| `PrimaryReplicationServer::wait_for_ack` | L5 (commit step 8) | Synchronous replication |
| `ReplicaClient::run` | Integration (startup) | Start replica |
| `PromotionClient::promote` | L5 (commit on replica) | Write forwarding |
| `SnapshotSender/Receiver` | Integration (recovery) | Full reconstruction |
| `min_replica_lsn` | L2 (WAL retention) | Segment reclamation |
