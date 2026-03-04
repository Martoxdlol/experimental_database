# Layer 5: Transaction Manager

## Purpose

Manages transaction lifecycle, OCC validation, read/write sets, subscriptions, and the commit protocol. This is the core concurrency control layer.

## Sub-Modules

### `tx/timestamp.rs` — Monotonic Timestamp Allocator

```rust
pub struct TsAllocator {
    current: AtomicU64,
}

impl TsAllocator {
    pub fn current(&self) -> Ts;
    pub fn advance(&self) -> Ts;  // Returns new ts (commit_ts)
}
```

### `tx/read_set.rs` — Read Set Intervals

```rust
pub struct ReadSet {
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
}

pub struct ReadInterval {
    pub query_id: QueryId,
    pub lower: Vec<u8>,         // Inclusive lower bound (encoded key)
    pub upper: Bound<Vec<u8>>,  // Excluded(key) or Unbounded
}

impl ReadSet {
    pub fn add_interval(
        &mut self,
        collection_id: CollectionId,
        index_id: IndexId,
        interval: ReadInterval,
    );
    // Merge overlapping/adjacent intervals within same (collection, index) group
    fn merge_intervals(&mut self, collection_id: CollectionId, index_id: IndexId);
}
```

### `tx/write_set.rs` — Write Set Buffer

```rust
pub struct WriteSet {
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
}

pub struct MutationEntry {
    pub op: OpType,
    pub body: Option<Document>,     // None for Delete
    pub previous_ts: Option<Ts>,    // None for Insert
}

pub enum OpType { Insert, Replace, Delete }

impl WriteSet {
    pub fn insert(&mut self, collection_id: CollectionId, doc_id: DocId, body: Document);
    pub fn replace(&mut self, collection_id: CollectionId, doc_id: DocId, body: Document, prev_ts: Ts);
    pub fn delete(&mut self, collection_id: CollectionId, doc_id: DocId, prev_ts: Ts);
    pub fn get(&self, collection_id: CollectionId, doc_id: DocId) -> Option<&MutationEntry>;

    // Compute index deltas at commit time (§5.5.1)
    pub fn compute_index_deltas(
        &self,
        catalog: &CatalogCache,
        primary_indexes: &HashMap<CollectionId, PrimaryIndex>,
    ) -> Vec<WalIndexDelta>;
}
```

### `tx/commit_log.rs` — Commit Log + OCC Validation

```rust
pub struct CommitLog {
    entries: Vec<CommitLogEntry>,
}

pub struct CommitLogEntry {
    pub commit_ts: Ts,
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
}

pub struct IndexKeyWrite {
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,
    pub new_key: Option<Vec<u8>>,
}

impl CommitLog {
    pub fn append(&mut self, entry: CommitLogEntry);

    // OCC validation (§5.7)
    pub fn validate(
        &self,
        read_set: &ReadSet,
        begin_ts: Ts,
        commit_ts: Ts,
    ) -> Result<(), ConflictError>;

    // Prune entries no longer needed
    pub fn prune(&mut self, oldest_active_begin_ts: Ts);
}

pub struct ConflictError {
    pub conflicting_queries: Vec<QueryId>,
}
```

### `tx/subs.rs` — Subscription Registry

```rust
pub struct SubscriptionRegistry {
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    subscriptions: HashMap<SubscriptionId, SubscriptionState>,
}

pub struct SubscriptionInterval {
    pub subscription_id: SubscriptionId,
    pub query_id: QueryId,
    pub lower: Vec<u8>,
    pub upper: Bound<Vec<u8>>,
}

pub enum SubscriptionMode { Notify, Subscribe }

pub struct InvalidationNotification {
    pub subscription_id: SubscriptionId,
    pub tx_id: TxId,
    pub affected_query_ids: Vec<QueryId>,  // sorted ascending
    pub commit_ts: Ts,
    pub new_tx: Option<(TxId, Ts)>,        // For subscribe mode
}

impl SubscriptionRegistry {
    pub fn register(
        &mut self,
        id: SubscriptionId,
        mode: SubscriptionMode,
        read_set: ReadSet,
        tx_id: TxId,
    );

    pub fn unregister(&mut self, id: SubscriptionId);

    // Check all subscriptions against a commit's index writes
    pub fn check_invalidation(
        &self,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    ) -> Vec<InvalidationNotification>;

    // Update subscription's read set on chain commit
    pub fn update_read_set(
        &mut self,
        id: SubscriptionId,
        new_read_set: ReadSet,
        new_tx_id: TxId,
    );
}
```

### `tx/mod.rs` — Transaction Manager (Single Writer)

```rust
pub struct TransactionManager {
    ts_allocator: TsAllocator,
    commit_log: RwLock<CommitLog>,
    subscriptions: RwLock<SubscriptionRegistry>,
    writer_channel: mpsc::Sender<CommitRequest>,
}

pub struct Transaction {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub readonly: bool,
    pub subscribe: bool,
    pub notify: bool,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub next_query_id: QueryId,
}

pub struct CommitRequest {
    pub tx: Transaction,
    pub response: oneshot::Sender<CommitResult>,
}

pub enum CommitResult {
    Success { commit_ts: Ts },
    Conflict { error: ConflictError, new_tx: Option<(TxId, Ts)> },
}

impl TransactionManager {
    pub fn begin(&self, database: &str, readonly: bool, subscribe: bool, notify: bool) -> Transaction;
    pub async fn commit(&self, tx: Transaction) -> CommitResult;
    pub fn rollback(&self, tx: Transaction);
}
```

## Commit Protocol Sequence Diagram

```
Client           Session          Writer Task        WAL Writer       Buffer Pool      Subscriptions    Replication
  │                │                  │                  │                 │                 │               │
  │── commit ─────▶│                  │                  │                 │                 │               │
  │                │── CommitReq ────▶│                  │                 │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              1. VALIDATE            │                 │                 │               │
  │                │              (OCC check vs          │                 │                 │               │
  │                │               commit log)           │                 │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              2. TIMESTAMP           │                 │                 │               │
  │                │              (ts_allocator.advance) │                 │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              3. PERSIST             │                 │                 │               │
  │                │                  │── WAL record ───▶│                 │                 │               │
  │                │                  │◀─── LSN ────────│                 │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              4. MATERIALIZE         │                 │                 │               │
  │                │                  │── apply mutations ──────────────▶│                 │               │
  │                │                  │  (ExclusivePageGuard on B-trees) │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              5. LOG                 │                 │                 │               │
  │                │              (commit_log.append)    │                 │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │              6. INVALIDATE          │                 │                 │               │
  │                │                  │── check_invalidation ───────────────────────────▶│               │
  │                │                  │◀─── notifications ──────────────────────────────│               │
  │                │                  │                  │                 │                 │               │
  │                │              7. REPLICATE           │                 │                 │               │
  │                │                  │── WAL stream ────────────────────────────────────────────────────▶│
  │                │                  │                  │                 │                 │               │
  │                │              8. SYNC (wait acks)    │                 │                 │               │
  │                │                  │◀─── ack ────────────────────────────────────────────────────────│
  │                │                  │                  │                 │                 │               │
  │                │              9. VISIBLE             │                 │                 │               │
  │                │              (advance latest_committed_ts)           │                 │               │
  │                │                  │                  │                 │                 │               │
  │                │◀── Success ─────│                  │                 │                 │               │
  │◀── ok ────────│                  │                  │                 │                 │               │
```

## Subscription Invalidation Diagram

```
  New Commit arrives
       │
       ▼
  ┌─────────────────────────┐
  │ Extract index_writes    │
  │ from CommitLogEntry     │
  │ (old_key, new_key per   │
  │  collection, index)     │
  └───────────┬─────────────┘
              │
              ▼
  For each (collection_id, index_id) in index_writes:
       │
       ▼
  ┌─────────────────────────┐
  │ Lookup SubscriptionIntervals │
  │ for this (collection, index) │
  └───────────┬─────────────┘
              │
              ▼
  For each IndexKeyWrite (old_key, new_key):
       │
       ├── old_key falls in interval? → conflict
       │
       └── new_key falls in interval? → conflict (phantom)
              │
              ▼
  ┌─────────────────────────┐
  │ Group by subscription_id│
  │ Collect affected query_ids │
  │ Sort query_ids ascending│
  └───────────┬─────────────┘
              │
              ▼
  ┌─────────────────────────┐
  │ Fire ONE notification   │
  │ per affected subscription│
  │ with ALL query_ids      │
  └─────────────────────────┘
```
