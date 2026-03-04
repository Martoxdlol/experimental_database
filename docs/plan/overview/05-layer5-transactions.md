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

> **Note:** `visible_ts` (the latest timestamp safe for new readers) lives on `CommitCoordinator`, not here. `TsAllocator.latest()` tracks the highest *allocated* timestamp, but that may be ahead of what has been replicated. See the "Visibility Fence" section below.

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
    QuorumLost {
        /// The commit was materialized then rolled back. Client should retry
        /// when cluster recovers.
    },
}

/// NOTE: The ReplicationHook trait is defined in L6 (database/replication_hook.rs).
/// CommitCoordinator receives it as an injected dependency from L6.
/// L5 does NOT define the trait — it only consumes it.

pub struct CommitCoordinator {
    ts_allocator: TsAllocator,
    visible_ts: AtomicU64,  // latest timestamp whose data is safe for new readers
    commit_log: RwLock<CommitLog>,
    subscriptions: RwLock<SubscriptionRegistry>,
    wal_writer: WalWriter,
    replication: Box<dyn ReplicationHook>,  // injected by L6 (Database)
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
}

impl CommitCoordinator {
    pub fn visible_ts(&self) -> Ts;  // latest timestamp safe for new readers

    /// The single-writer commit loop
    pub async fn run(&mut self);
    // For each request:
    //   1. OCC validation
    //   2. Assign commit_ts
    //   3. Write WAL record + fsync
    //   4. Apply mutations to page store (via L3)
    //   5. Update commit log
    //   6. START CONCURRENT:
    //      a. self.replication.replicate_and_wait(lsn, record)
    //      b. subscriptions.check_invalidation(index_writes)
    //   7. AWAIT replication (6a)
    //      ON SUCCESS: continue to step 8
    //      ON FAILURE (quorum lost):
    //        a. Rollback this commit: delete materialized keys using write_set + index_deltas
    //        b. Remove this commit from commit_log
    //        c. Enter hold state (reject new commits)
    //        d. Return CommitResult::QuorumLost to client
    //   8. Write WAL_RECORD_VISIBLE_TS(commit_ts) + fsync
    //   9. Advance visible_ts = commit_ts (in-memory)
    //  10. Respond to committing client
    //  11. FIRE-AND-FORGET: push invalidation events (6b results) to subscriber sessions
    //
    // Notes:
    // - Steps 6a and 6b run concurrently (tokio::join! or similar).
    // - Without replication (NoReplication), step 6a is a no-op so
    //   visible_ts advances immediately after WAL fsync.
    // - visible_ts is persisted to WAL (WAL_RECORD_VISIBLE_TS) so
    //   recovery knows the last replicated point.
    // - New readers use visible_ts (not ts_allocator.latest()) for
    //   their read_ts.
    // - On recovery: visible_ts = last WAL_RECORD_VISIBLE_TS found
    //   during replay.
}

/// Handle for submitting commit requests from any task
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
}

impl CommitHandle {
    pub async fn commit(&self, request: CommitRequest) -> CommitResult;
    pub fn visible_ts(&self) -> Ts;  // delegates to CommitCoordinator's AtomicU64
}
```

### Visibility Fence (`visible_ts`)

`visible_ts` is the latest timestamp that is safe for new read transactions to observe. It is separate from `ts_allocator.current` (which tracks the highest *allocated* timestamp).

**Key rules:**

- `begin_readonly()` uses `visible_ts`, **NOT** `ts_allocator.latest()`.
- `visible_ts` only advances after replication confirms the commit **AND** the advancement is persisted to WAL (via `WAL_RECORD_VISIBLE_TS`).
- Without replication (`NoReplication`): `visible_ts` advances immediately after the commit's WAL fsync — no fence is needed.

**Why this matters (phantom read prevention):**

Without a visibility fence, the following scenario is possible:
1. Primary commits `ts=101` and advances its read timestamp.
2. A new reader on the primary observes data at `ts=101`.
3. Primary is destroyed before replication completes.
4. Replica only has data through `ts=100`.
5. The reader saw a "phantom" — data that is permanently lost.

With `visible_ts`, step 2 cannot happen until replication of `ts=101` is confirmed, preventing phantom reads of un-replicated data.

**Recovery behavior:**

On startup, `visible_ts` is set to the last `WAL_RECORD_VISIBLE_TS` found during WAL replay. Any commits that exist locally beyond `visible_ts` were written to WAL but not yet replicated — they remain locally present but invisible to new readers until they are re-replicated and `visible_ts` advances again. After WAL replay, if committed transactions exist beyond visible_ts, the rollback vacuum (see Layer 3) removes their materialized entries. This ensures the database state matches what replicas have.

**Vacuum interaction:**

Vacuum must use `min(oldest_active_read_ts, visible_ts)` as its safe threshold -- see Layer 3 vacuum strategy.

**WAL record type:**

`WAL_RECORD_VISIBLE_TS` (type tag `0x09`) is added to the WAL record type registry in S5. Its payload is a single `u64` — the new visible timestamp. This record is written and fsynced after replication confirms, before `visible_ts` is advanced in memory.

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `TsAllocator` | L5 internal, L6 (Database) | Timestamp management |
| `ReadSet`, `ReadInterval` | L4 (produces), L5 (validates), L7 (replication) | Conflict surface |
| `WriteSet`, `MutationEntry` | L4 (read-your-writes), L5 (commit) | Buffered mutations |
| `CommitHandle::commit` | L6 (Database), L8 (session) | Submit commit request |
| `CommitResult` | L6 (transaction API), L8 (session response) | Success/conflict reporting |
| `SubscriptionRegistry` | L5 internal, L6 (Database) | Subscription management |
| `InvalidationEvent` | L6 (callback), L8 (push to client) | Subscription invalidation |
| `CommitLog` | L5 internal | OCC validation data |
