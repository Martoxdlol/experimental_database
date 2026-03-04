# Layer 6: Distributed / Replication

## Purpose

WAL streaming from primary to replicas, transaction promotion, replica recovery (3 tiers), and WAL retention management.

## Sub-Modules

### `replication/primary.rs` — Primary WAL Streaming Server

```rust
pub struct PrimaryReplicationServer {
    wal_reader: Arc<WalReader>,
    replicas: RwLock<HashMap<ReplicaId, ReplicaConnection>>,
    wal_retention: WalRetentionPolicy,
}

pub struct ReplicaConnection {
    pub replica_id: ReplicaId,
    pub applied_lsn: Lsn,
    pub applied_ts: Ts,
    pub tcp_stream: TcpStream,
    pub ack_channel: mpsc::Receiver<Lsn>,
}

impl PrimaryReplicationServer {
    pub async fn start(addr: SocketAddr, database: Arc<Database>) -> Result<Self>;

    // Stream WAL records to a specific replica from their applied_lsn
    pub async fn stream_to_replica(&self, replica_id: ReplicaId) -> Result<()>;

    // Broadcast a new commit to all replicas and collect acks
    pub async fn broadcast_and_wait(
        &self,
        wal_record_bytes: &[u8],
        lsn: Lsn,
        mode: ReplicationMode,
    ) -> Result<()>;

    // Compute retention_lsn for WAL segment reclamation
    pub fn retention_lsn(&self, checkpoint_lsn: Lsn) -> Lsn;
}

pub enum ReplicationMode {
    Strict,       // Wait for all replicas
    PrimaryPlusOne, // Wait for at least one
}
```

### `replication/replica.rs` — Replica Client

```rust
pub struct ReplicaClient {
    primary_addr: SocketAddr,
    database: Arc<Database>,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    shutdown: CancellationToken,
}

impl ReplicaClient {
    pub async fn start(
        database: Arc<Database>,
        primary_addr: SocketAddr,
    ) -> Result<Self>;

    // Main loop: connect, receive WAL records, apply, ack
    async fn run_replica(&self) -> Result<()>;

    // Apply a single WAL record to local state
    async fn apply_wal_record(&self, record: &WalRecord) -> Result<()>;

    // Reconnect with exponential backoff
    async fn reconnect(&self) -> Result<TcpStream>;
}
```

### `replication/promotion.rs` — Transaction Promotion

```rust
pub struct PromotionRequest {
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
}

pub enum PromotionResult {
    Success { commit_ts: Ts },
    Conflict { error: ConflictError, new_tx: Option<(TxId, Ts)> },
}

// On the replica side:
pub async fn promote_transaction(
    primary_addr: SocketAddr,
    request: PromotionRequest,
) -> Result<PromotionResult>;

// On the primary side (within the writer task):
pub fn handle_promotion(
    request: PromotionRequest,
    tx_manager: &TransactionManager,
) -> PromotionResult;
```

### `replication/snapshot.rs` — Full Reconstruction (Tier 3)

```rust
pub struct SnapshotTransfer { /* ... */ }

impl SnapshotTransfer {
    // Source side: stream data.db pages + buffered WAL
    pub async fn send_snapshot(
        database: &Database,
        stream: &mut TcpStream,
    ) -> Result<()>;

    // Receiving side: write pages, then WAL, then finalize
    pub async fn receive_snapshot(
        db_dir: &Path,
        stream: &mut TcpStream,
        config: &DatabaseConfig,
    ) -> Result<()>;
}
```

## Interfaces

### Depends On

| Layer | Interface |
|---|---|
| Layer 2 | `WalWriter::append()`, `WalReader::raw_bytes_from()`, `BufferPool`, `CheckpointManager` |
| Layer 3 | `PrimaryIndex::insert_version()`, `SecondaryIndex::apply_delta()` |
| Layer 5 | `TransactionManager::commit()`, `SubscriptionRegistry::check_invalidation()` |

### Exposes To

| Consumer | Interface |
|---|---|
| Layer 5 (commit protocol) | `PrimaryReplicationServer::broadcast_and_wait()` |
| Layer 7 (session, for replica writes) | `promote_transaction()` |
| `database.rs` | `ReplicaClient::start()`, `PrimaryReplicationServer::start()` |

## Replication Flow Diagram

```
                PRIMARY                                          REPLICA
  ┌──────────────────────────────┐                  ┌──────────────────────────────┐
  │  Writer Task                 │                  │  ReplicaClient               │
  │                              │                  │                              │
  │  commit → WAL record         │                  │                              │
  │           │                  │   TCP stream     │                              │
  │           ├──── WAL bytes ──────────────────────▶│  receive WAL record          │
  │           │                  │                  │  │                            │
  │           │                  │                  │  ├── write to local WAL       │
  │           │                  │                  │  ├── apply to page store      │
  │           │                  │                  │  ├── update indexes           │
  │           │                  │                  │  ├── invalidate local subs    │
  │           │                  │                  │  │                            │
  │           │      ack         │                  │  ├── send ACK ───────────────▶│
  │           │◀─────────────────────────────────────  │                            │
  │           │                  │                  │  └── advance applied_ts       │
  │  advance latest_committed_ts │                  │                              │
  │  notify client               │                  │                              │
  └──────────────────────────────┘                  └──────────────────────────────┘
```

## Replica Recovery Tiers

```
  Replica comes back online
          │
          ▼
  ┌──────────────────────┐
  │ Check local state    │
  │ (disk intact? WAL?)  │
  └──────────┬───────────┘
             │
     ┌───────┴────────┐
     │ Disk intact?   │
     └──┬──────────┬──┘
        │yes       │no → TIER 3 (full reconstruction)
        ▼
  ┌──────────────────┐
  │ Local recovery   │  (DWB + WAL replay)
  │ (TIER 2)         │
  └──────┬───────────┘
         │
  ┌──────▼───────────────┐
  │ Check primary WAL    │
  │ gap covered?         │
  └──┬──────────────┬────┘
     │yes           │no → TIER 3
     ▼
  ┌──────────────────┐
  │ Incremental      │
  │ catch-up (TIER 1)│
  │ stream missing   │
  │ WAL records      │
  └──────────────────┘
```
