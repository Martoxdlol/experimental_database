# Layer 5: Transaction Manager

**Layer purpose:** Timestamp allocation, read/write set management, OCC validation via commit log, subscription registry with three modes (Notify / Watch / Subscribe), read set carry-forward for subscription chains, and the single-writer commit protocol. This is the concurrency and consistency layer.

**Detailed implementation plan:** See [docs/plan/transactions/](../transactions/00-overview.md)

## Subscription Mode Enum

Previous design had two separate booleans (`notify`, `subscribe`). This is replaced by a single enum:

```rust
/// Subscription mode — controls post-commit read set behavior.
/// Mutually exclusive: exactly one mode per transaction.
pub enum SubscriptionMode {
    /// No subscription. Read set discarded after commit.
    None,
    /// One-shot: fire once on first invalidation, then remove.
    Notify,
    /// Persistent: fire on every invalidation, subscription persists.
    /// No new transaction is auto-started. Client manages re-queries.
    Watch,
    /// Persistent with chain continuation: fire, auto-start new tx,
    /// carry forward unaffected read set intervals.
    Subscribe,
}
```

**Behavior matrix:**

| Scenario | `None` | `Notify` | `Watch` | `Subscribe` |
|----------|--------|----------|---------|-------------|
| After commit | Read set discarded | Subscription (one-shot) | Subscription (persistent) | Subscription (persistent) |
| On invalidation | — | Notify + remove | Notify (keep watching) | Notify + new tx + carry read set |
| On OCC conflict | Error | Error | Error | Error + auto-retry tx |
| Read set update | — | — | Manual | Automatic on chain commit |

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

### `read_set.rs` — Read Set (Scanned Intervals + Carry-Forward)

**WHY HERE:** Tracks which portions of the index key space a transaction has observed, including catalog reads. Used for OCC validation, subscription invalidation, and carry-forward — core transaction semantics.

```rust
pub type QueryId = u32;

/// A single read interval on an index
pub struct ReadInterval {
    pub query_id: QueryId,
    pub lower: Vec<u8>,           // inclusive lower bound (encoded key)
    pub upper: Bound<Vec<u8>>,    // Excluded(key) or Unbounded
}

/// All intervals for a transaction, grouped by (collection, index)
pub struct ReadSet {
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    pub catalog_reads: Vec<CatalogRead>,
    next_query_id: QueryId,
}

/// Records a catalog observation made during the transaction.
/// Used for OCC conflict detection against concurrent DDL.
pub enum CatalogRead {
    /// Transaction resolved a collection name (e.g., tx.insert("users", ...)).
    /// Conflicts with: create or drop of collection with this name.
    CollectionByName(String),

    /// Transaction listed all collections.
    /// Conflicts with: any collection create or drop.
    ListCollections,

    /// Transaction resolved an index by name within a collection.
    /// Conflicts with: create or drop of index with this name in this collection.
    IndexByName(CollectionId, String),

    /// Transaction listed indexes for a collection.
    /// Conflicts with: any index create or drop in this collection.
    ListIndexes(CollectionId),
}

impl ReadSet {
    pub fn new() -> Self;
    /// Create with a starting query_id offset (for carry-forward).
    pub fn with_starting_query_id(first_query_id: QueryId) -> Self;
    pub fn add_interval(&mut self, collection_id: CollectionId, index_id: IndexId,
                        interval: ReadInterval);
    pub fn add_catalog_read(&mut self, read: CatalogRead);
    pub fn next_query_id(&mut self) -> QueryId;
    pub fn peek_next_query_id(&self) -> QueryId;
    /// Explicitly set the counter (used by begin_chain_continuation in L6).
    pub fn set_next_query_id(&mut self, id: QueryId);
    pub fn merge_overlapping(&mut self);  // merge adjacent/overlapping intervals
    pub fn interval_count(&self) -> usize;

    /// Extract intervals with query_id < threshold into a new ReadSet.
    /// Used for subscription chain carry-forward.
    pub fn split_before(&self, threshold: QueryId) -> ReadSet;

    /// Merge another ReadSet into this one (carried + new intervals).
    pub fn merge_from(&mut self, other: &ReadSet);
}
```

**Carry-forward:** When a subscription chain fires at `query_id = Q_min`, `split_before(Q_min)` extracts unaffected intervals. These are provably unchanged (no concurrent commit touched them), so carrying them to the new transaction is equivalent to re-executing the same queries.

**Interval merging:** When intervals overlap, the merged interval takes `query_id = min(both)`. This is conservative — a wider carried interval may cause more future conflicts, but never fewer.

### `write_set.rs` — Write Set (Buffered Mutations) + IndexResolver Trait

**WHY HERE:** Buffers document mutations and catalog mutations until commit. Also defines the `IndexResolver` trait used by the commit coordinator to look up index metadata without depending on L6's `CatalogCache` type.

```rust
pub struct WriteSet {
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
    pub catalog_mutations: Vec<CatalogMutation>,
}

pub struct MutationEntry {
    pub op: MutationOp,
    pub body: Option<serde_json::Value>,  // None for Delete
    pub previous_ts: Option<Ts>,           // Version being replaced (None for Insert)
}

pub enum MutationOp { Insert, Replace, Delete }

/// Catalog DDL operations buffered in the write set.
/// Applied atomically with document mutations at commit time.
pub enum CatalogMutation {
    CreateCollection {
        name: String,
        provisional_id: CollectionId,  // allocated eagerly from atomic counter
    },
    DropCollection {
        collection_id: CollectionId,
        name: String,
    },
    CreateIndex {
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        provisional_id: IndexId,       // allocated eagerly from atomic counter
    },
    DropIndex {
        collection_id: CollectionId,
        index_id: IndexId,
        name: String,
    },
}

/// Index delta computed at commit time (section 5.5.1)
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,  // None for inserts
    pub new_key: Option<Vec<u8>>,  // None for deletes
}

/// Minimal index metadata needed for delta computation.
pub struct IndexInfo {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub field_paths: Vec<FieldPath>,
}

/// Trait for looking up index metadata during commit.
/// Defined in L5, implemented by L6 (wrapping CatalogCache).
/// Avoids L5 depending on L6's CatalogCache type.
pub trait IndexResolver: Send + Sync {
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo>;
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
    pub fn add_catalog_mutation(&mut self, mutation: CatalogMutation);
    pub fn resolve_pending_collection(&self, name: &str) -> Option<CollectionId>;
    pub fn is_collection_dropped(&self, collection_id: CollectionId) -> bool;
    /// Returns true when BOTH mutations and catalog_mutations are empty.
    pub fn is_empty(&self) -> bool;
    pub fn mutations_for_collection(&self, collection_id: CollectionId)
        -> impl Iterator<Item = (&DocId, &MutationEntry)>;
}

/// Compute index deltas for all mutations in the write set.
/// Async because reading old documents for Replace/Delete requires
/// PrimaryIndex::get_at_ts (async I/O). Uses IndexResolver (not CatalogCache)
/// to avoid L5 → L6 dependency.
pub async fn compute_index_deltas(
    write_set: &WriteSet,
    index_resolver: &dyn IndexResolver,
    primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
) -> Result<Vec<IndexDelta>>;
```

### `commit_log.rs` — Commit Log (Recent Commit Tracking)

**WHY HERE:** In-memory structure tracking recent commits for OCC validation and subscription invalidation. Core transaction infrastructure.

```rust
/// Entry in the commit log tracking what a commit wrote
pub struct CommitLogEntry {
    pub commit_ts: Ts,
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    pub catalog_mutations: Vec<CatalogMutation>,  // DDL ops in this commit (if any)
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
    /// Remove specific entry (for rollback on quorum loss)
    pub fn remove(&mut self, commit_ts: Ts) -> Option<CommitLogEntry>;
}
```

### `occ.rs` — OCC Validation (Conflict Detection)

**WHY HERE:** Validates a transaction's read set (including catalog reads) against concurrent commits. Core consistency logic.

```rust
/// Validate read set against commit log.
/// Checks both index interval overlaps and catalog read/write conflicts.
/// Returns Ok(()) if no conflicts, Err(ConflictError) if conflicts found.
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError>;

/// Check if any catalog read conflicts with any catalog mutation.
/// Returns true if a conflict exists.
/// PUBLIC — also used by T6 (subscriptions) to check catalog invalidations.
pub fn catalog_conflicts(
    catalog_reads: &[CatalogRead],
    catalog_mutations: &[CatalogMutation],
) -> bool;

pub struct ConflictError {
    pub conflicting_ts: Ts,
    pub kind: ConflictKind,
    /// Query IDs whose intervals were overlapped (sorted ascending).
    pub affected_query_ids: Vec<QueryId>,
}

pub enum ConflictKind {
    /// Data interval overlap on an index
    IndexInterval {
        collection_id: CollectionId,
        index_id: IndexId,
    },
    /// Catalog conflict (concurrent DDL vs this transaction's catalog reads)
    Catalog {
        description: String,
    },
}
```

### `subscriptions.rs` — Subscription Registry (Notify / Watch / Subscribe)

**WHY HERE:** Manages persistent read set watches for reactive invalidation on commit. Indexes intervals by (collection, index) for fast overlap checks. Supports three modes with different lifecycles.

```rust
pub type SubscriptionId = u64;

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
    pub session_id: u64,
    pub tx_id: TxId,
    pub read_ts: Ts,
    pub read_set: ReadSet,  // full read set (for split_before on chain continuation)
    /// Channel sender for pushing InvalidationEvents to the client.
    /// Created at registration time (either by CommitCoordinator step 10
    /// for write commits, or by L6 directly for read-only commits).
    pub event_tx: mpsc::Sender<InvalidationEvent>,
}

/// Invalidation event — one per affected subscription per commit.
pub struct InvalidationEvent {
    pub subscription_id: SubscriptionId,
    pub affected_query_ids: Vec<QueryId>,  // sorted ascending
    pub commit_ts: Ts,
    /// Chain continuation (present only for Subscribe mode).
    pub continuation: Option<ChainContinuation>,
}

/// Chain continuation for Subscribe mode.
pub struct ChainContinuation {
    /// New transaction auto-started at the invalidation timestamp.
    pub new_tx_id: TxId,
    pub new_ts: Ts,
    /// Read set intervals from queries BEFORE the first invalidated query.
    /// Carried forward because they were provably unaffected.
    pub carried_read_set: ReadSet,
    /// The first query_id that needs re-execution (= min(affected_query_ids)).
    pub first_query_id: QueryId,
}

impl SubscriptionRegistry {
    pub fn new() -> Self;

    /// Register a read set as a subscription.
    /// `event_tx`: sender for pushing InvalidationEvents to the client.
    /// L6 wraps the receiver in a SubscriptionHandle.
    pub fn register(&mut self, mode: SubscriptionMode, session_id: u64,
                    tx_id: TxId, read_ts: Ts, read_set: ReadSet,
                    event_tx: mpsc::Sender<InvalidationEvent>) -> SubscriptionId;

    /// Remove a subscription.
    pub fn remove(&mut self, id: SubscriptionId);

    /// Check all subscriptions against a new commit's writes.
    /// For Subscribe mode: computes ChainContinuation with carried read set.
    /// For Notify mode: marks subscription for removal.
    pub fn check_invalidation(
        &mut self,
        commit_ts: Ts,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
        catalog_mutations: &[CatalogMutation],
        allocate_tx: impl FnMut() -> TxId,
    ) -> Vec<InvalidationEvent>;

    /// Update a subscription's read set (for chain commit or Watch refresh).
    pub fn update_read_set(&mut self, id: SubscriptionId, new_read_set: ReadSet);

    /// Remove all subscriptions for a session (on disconnect).
    pub fn remove_session(&mut self, session_id: u64);
}
```

### `commit.rs` — Single-Writer Commit Protocol

**WHY HERE:** Orchestrates the full 11-step commit sequence. The serialization point for all writes.

```rust
pub struct CommitRequest {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub subscription: SubscriptionMode,
    pub session_id: u64,
}

pub enum CommitResult {
    Success {
        commit_ts: Ts,
        subscription_id: Option<SubscriptionId>,
        /// Event receiver for the newly registered subscription (Some when subscription_id is Some).
        /// L6 wraps this in a SubscriptionHandle for the client.
        event_rx: Option<mpsc::Receiver<InvalidationEvent>>,
    },
    Conflict {
        error: ConflictError,
        /// For Subscribe mode: auto-retry transaction.
        retry: Option<ConflictRetry>,
    },
    QuorumLost,
}

pub struct ConflictRetry {
    pub new_tx_id: TxId,
    pub new_ts: Ts,
}

/// Trait for replication — defined in L5, implemented by L6 or L7.
/// Injected into CommitCoordinator at construction time.
pub trait ReplicationHook: Send + Sync {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;
    fn has_quorum(&self) -> bool { true }
    fn is_holding(&self) -> bool { false }
}

/// No-op replication for embedded/single-node. Provided by L5 as a default.
pub struct NoReplication;

/// Trait for applying/rolling back catalog mutations.
/// Defined in L5, implemented by L6 (owns CatalogCache + catalog B-trees).
pub trait CatalogMutator: Send + Sync {
    async fn apply(&self, mutation: &CatalogMutation, commit_ts: Ts) -> Result<()>;
    async fn rollback(&self, mutation: &CatalogMutation) -> Result<()>;
}

pub struct CommitCoordinator {
    ts_allocator: TsAllocator,
    visible_ts: AtomicU64,
    commit_log: CommitLog,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    replication: Box<dyn ReplicationHook>,
    catalog_mutator: Arc<dyn CatalogMutator>,
    index_resolver: Arc<dyn IndexResolver>,
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
    next_tx_id: AtomicU64,
}

impl CommitCoordinator {
    pub fn new(
        initial_ts: Ts,
        visible_ts: Ts,
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        catalog_mutator: Arc<dyn CatalogMutator>,
        index_resolver: Arc<dyn IndexResolver>,
        channel_size: usize,
    ) -> (Self, CommitHandle);

    pub fn visible_ts(&self) -> Ts;

    /// The single-writer commit loop
    pub async fn run(&mut self);
    // For each request:
    //   1. OCC validation (skip for empty write_set — read-only subscription commits)
    //      On conflict + Subscribe mode: auto-start retry tx, return ConflictRetry
    //   2. Assign commit_ts
    //   3. WAL_RECORD_TX_COMMIT (0x01) + fsync
    //   4a. Apply catalog mutations via CatalogMutator (BEFORE data)
    //   4b. Apply data mutations via PrimaryIndex + SecondaryIndex (L3)
    //       Compute index deltas via compute_index_deltas()
    //   5. Append to commit log
    //   6. START CONCURRENT:
    //      a. replication.replicate_and_wait(lsn, record)
    //      b. subscriptions.check_invalidation(index_writes, catalog_mutations)
    //   7. AWAIT replication
    //      ON FAILURE: rollback data + catalog + commit log; respond QuorumLost
    //   8. WAL_RECORD_VISIBLE_TS (0x09) + fsync
    //   9. Advance visible_ts = commit_ts
    //  10. Create mpsc channel, register subscription (event_tx → registry, event_rx → result)
    //  11. FIRE-AND-FORGET: push step-6b events via try_send on each sub's event_tx
}

/// Handle for submitting commit requests from any task
#[derive(Clone)]
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
    visible_ts: Arc<AtomicU64>,
    ts_allocator: Arc<TsAllocator>,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    next_tx_id: Arc<AtomicU64>,
}

impl CommitHandle {
    pub async fn commit(&self, request: CommitRequest) -> CommitResult;
    pub fn visible_ts(&self) -> Ts;
    pub fn allocate_tx_id(&self) -> TxId;
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>>;
}
```

### Visibility Fence (`visible_ts`)

`visible_ts` is the latest timestamp that is safe for new read transactions to observe. It is separate from `ts_allocator.current` (which tracks the highest *allocated* timestamp).

**Key rules:**

- `begin()` uses `visible_ts`, **NOT** `ts_allocator.latest()`.
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
| `SubscriptionMode` | L6 (TransactionOptions) | Transaction subscription mode |
| `TsAllocator` | L5 internal, L6 (Database) | Timestamp management |
| `ReadSet`, `ReadInterval`, `CatalogRead` | L4 (produces), L5 (validates), L6 (DDL), L7 (replication) | Conflict surface + carry-forward |
| `WriteSet`, `MutationEntry`, `CatalogMutation` | L4 (read-your-writes), L5 (commit), L6 (DDL) | Buffered mutations (data + catalog) |
| `CommitHandle::commit` | L6 (Database) | Submit commit request |
| `CommitResult`, `ConflictRetry` | L6 (transaction API), L8 (session response) | Success/conflict/retry reporting |
| `SubscriptionRegistry` | L5 internal, L6 (Database) | Subscription management |
| `InvalidationEvent`, `ChainContinuation` | L6 (callback), L8 (push to client) | Subscription invalidation + chain |
| `CommitLog` | L5 internal | OCC validation data |
