# Layer 5: Transaction Manager

**Layer purpose:** Timestamp allocation, read/write set management, OCC validation via commit log, subscription registry with invalidation, and the single-writer commit protocol. This is the concurrency and consistency layer.

## Modules

### `timestamp.rs` — Monotonic Timestamp Allocator

**WHY HERE:** Allocates globally-ordered timestamps for transactions. Central coordination point for MVCC.

```rust
pub struct TsAllocator {
    current: AtomicU64,
}

impl TsAllocator {
    pub fn new(initial: Ts) -> Self;
    pub fn latest(&self) -> Ts;
    pub fn allocate(&self) -> Ts;  // fetch_add(1)
}
```

### `read_set.rs` — Read Set (Scanned Intervals)

**WHY HERE:** Tracks which portions of the index key space a transaction has observed. Used for OCC validation and subscription invalidation — core transaction semantics.

```rust
/// A single read interval on an index
pub struct ReadInterval {
    pub query_id: u32,
    pub lower: Vec<u8>,           // inclusive lower bound (encoded key)
    pub upper: Bound<Vec<u8>>,    // Excluded(key) or Unbounded
}

/// All intervals for a transaction, grouped by (collection, index)
pub struct ReadSet {
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    pub next_query_id: u32,
}

impl ReadSet {
    pub fn new() -> Self;
    pub fn add_interval(&mut self, collection_id: CollectionId, index_id: IndexId,
                        interval: ReadInterval);
    pub fn next_query_id(&mut self) -> u32;
    pub fn merge_overlapping(&mut self);  // merge adjacent/overlapping intervals
    pub fn interval_count(&self) -> usize;
}
```

### `write_set.rs` — Write Set (Buffered Mutations)

**WHY HERE:** Buffers document mutations until commit. Contains domain-specific operations (insert, replace, delete) with document bodies.

```rust
pub struct WriteSet {
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
}

pub struct MutationEntry {
    pub op: MutationOp,
    pub body: Option<serde_json::Value>,  // None for Delete
    pub previous_ts: Option<Ts>,           // Version being replaced (None for Insert)
}

pub enum MutationOp { Insert, Replace, Delete }

/// Index delta computed at commit time (section 5.5.1)
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,  // None for inserts
    pub new_key: Option<Vec<u8>>,  // None for deletes
}

impl WriteSet {
    pub fn new() -> Self;
    pub fn insert(&mut self, collection_id: CollectionId, doc_id: DocId,
                  body: serde_json::Value);
    pub fn replace(&mut self, collection_id: CollectionId, doc_id: DocId,
                   body: serde_json::Value, previous_ts: Ts);
    pub fn delete(&mut self, collection_id: CollectionId, doc_id: DocId,
                  previous_ts: Ts);
    pub fn get(&self, collection_id: CollectionId, doc_id: &DocId) -> Option<&MutationEntry>;

    /// Compute index deltas for all mutations (section 5.5.1)
    pub fn compute_index_deltas(&self, catalog: &CatalogCache,
                                 primary_indexes: &HashMap<CollectionId, PrimaryIndex>,
                                 ) -> Result<Vec<IndexDelta>>;
}
```

### `commit_log.rs` — Commit Log (Recent Commit Tracking)

**WHY HERE:** In-memory structure tracking recent commits for OCC validation and subscription invalidation. Core transaction infrastructure.

```rust
/// Entry in the commit log tracking what a commit wrote
pub struct CommitLogEntry {
    pub commit_ts: Ts,
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
}

pub struct IndexKeyWrite {
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,
    pub new_key: Option<Vec<u8>>,
}

pub struct CommitLog {
    entries: Vec<CommitLogEntry>,  // ordered by commit_ts
}

impl CommitLog {
    pub fn new() -> Self;
    pub fn append(&mut self, entry: CommitLogEntry);
    /// Get all entries with commit_ts in (begin_ts, commit_ts)
    pub fn entries_in_range(&self, begin_ts: Ts, commit_ts: Ts) -> &[CommitLogEntry];
    /// Prune entries no longer needed (commit_ts <= oldest_active_begin_ts)
    pub fn prune(&mut self, oldest_active_begin_ts: Ts);
}
```

### `occ.rs` — OCC Validation (Conflict Detection)

**WHY HERE:** Validates a transaction's read set against concurrent commits. Core consistency logic.

```rust
/// Validate read set against commit log
/// Returns Ok(()) if no conflicts, Err(ConflictError) if conflicts found
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError>;

pub struct ConflictError {
    pub conflicting_ts: Ts,
    pub conflicting_collection: CollectionId,
    pub conflicting_index: IndexId,
}

/// Check if a key falls within any interval in a group
fn key_overlaps_intervals(key: &[u8], intervals: &[ReadInterval]) -> Option<u32>;
// Returns the query_id of the overlapping interval, if any
```

### `subscriptions.rs` — Subscription Registry

**WHY HERE:** Manages persistent read set watches for reactive invalidation on commit. Indexes intervals by (collection, index) for fast overlap checks.

```rust
pub type SubscriptionId = u64;
pub type QueryId = u32;

pub enum SubscriptionMode { Notify, Subscribe }

pub struct SubscriptionInterval {
    pub subscription_id: SubscriptionId,
    pub query_id: QueryId,
    pub lower: Vec<u8>,
    pub upper: Bound<Vec<u8>>,
}

pub struct SubscriptionRegistry {
    /// Grouped by (collection, index) for range overlap checks
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    subscriptions: HashMap<SubscriptionId, SubscriptionMeta>,
    next_id: AtomicU64,
}

pub struct SubscriptionMeta {
    pub mode: SubscriptionMode,
    pub session_id: u64,  // which client session owns this
    pub tx_id: TxId,
}

pub struct InvalidationEvent {
    pub subscription_id: SubscriptionId,
    pub affected_query_ids: Vec<QueryId>,  // sorted ascending
    pub commit_ts: Ts,
}

impl SubscriptionRegistry {
    pub fn new() -> Self;

    /// Register a read set as a subscription
    pub fn register(&mut self, mode: SubscriptionMode, session_id: u64,
                    tx_id: TxId, read_set: &ReadSet) -> SubscriptionId;

    /// Remove a subscription
    pub fn remove(&mut self, id: SubscriptionId);

    /// Check all subscriptions against a new commit's index writes
    /// Returns all affected subscriptions with their invalidated query IDs
    pub fn check_invalidation(
        &self,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    ) -> Vec<InvalidationEvent>;

    /// Update a subscription's read set (for chain continuation)
    pub fn update_read_set(&mut self, id: SubscriptionId, new_read_set: &ReadSet);
}
```

### `commit.rs` — Single-Writer Commit Protocol

**WHY HERE:** Orchestrates the full 10-step commit sequence. The serialization point for all writes.

```rust
pub struct CommitRequest {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub subscribe: bool,
    pub notify: bool,
    pub session_id: u64,
}

pub enum CommitResult {
    Success {
        commit_ts: Ts,
        subscription_id: Option<SubscriptionId>,
    },
    Conflict {
        error: ConflictError,
        new_tx_id: Option<TxId>,   // if subscribe: true
        new_ts: Option<Ts>,
    },
}

/// Trait for replication hook — defined in L5, implemented in L6.
/// This breaks the L5 → L6 dependency via dependency inversion.
/// Integration wires L6's implementation into L5 at construction time.
/// When no replication is needed (embedded single-node), a no-op impl is used.
pub trait ReplicationHook: Send + Sync {
    /// Replicate a committed WAL record to replicas and wait for acks
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;
}

/// No-op implementation for embedded/single-node usage
pub struct NoReplication;
impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> { Ok(()) }
}

pub struct CommitCoordinator {
    ts_allocator: TsAllocator,
    commit_log: RwLock<CommitLog>,
    subscriptions: RwLock<SubscriptionRegistry>,
    wal_writer: WalWriter,
    replication: Box<dyn ReplicationHook>,  // injected by Integration
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
}

impl CommitCoordinator {
    /// The single-writer commit loop
    pub async fn run(&mut self);
    // For each request:
    //   1. OCC validation
    //   2. Assign commit_ts
    //   3. Write WAL record + fsync
    //   4. Apply mutations to page store (via L3)
    //   5. Update commit log
    //   6. Invalidate subscriptions
    //   7-8. self.replication.replicate_and_wait() — calls L6 if wired, no-op otherwise
    //   9. Advance latest_committed_ts
    //   10. Notify client
}

/// Handle for submitting commit requests from any task
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
}

impl CommitHandle {
    pub async fn commit(&self, request: CommitRequest) -> CommitResult;
}
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `TsAllocator` | L5 internal, Integration | Timestamp management |
| `ReadSet`, `ReadInterval` | L4 (produces), L5 (validates), L6 (replication) | Conflict surface |
| `WriteSet`, `MutationEntry` | L4 (read-your-writes), L5 (commit) | Buffered mutations |
| `CommitHandle::commit` | L7 (session), L6 (promotion) | Submit commit request |
| `CommitResult` | L7 (session response) | Success/conflict reporting |
| `SubscriptionRegistry` | L5 internal, Integration | Subscription management |
| `InvalidationEvent` | L7 (push notification to client) | Subscription invalidation |
| `CommitLog` | L5 internal | OCC validation data |
