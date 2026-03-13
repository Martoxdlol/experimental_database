# T7: Commit Coordinator — Single-Writer Protocol

**File:** `crates/tx/src/commit.rs`
**Depends on:** T1–T6, L2 (`StorageEngine`, WAL), L3 (`PrimaryIndex`, `SecondaryIndex`)
**Depended on by:** L6 (Database)
**Defines traits:** `ReplicationHook` (implemented by L6), `CatalogMutator` (implemented by L6)

## Purpose

Orchestrates the full commit sequence: OCC validation, timestamp assignment, WAL persistence, page store materialization, commit log update, subscription invalidation, replication, and visibility advancement.

This is the serialization point for all writes. A single tokio task runs the commit loop, processing one commit at a time. All other code paths submit requests and await results via channels.

## Trait Definitions

L5 defines traits for capabilities it needs from L6. This avoids a circular dependency (L6 depends on L5, L5 defines traits that L6 implements).

### ReplicationHook

```rust
use exdb_storage::wal::Lsn;

/// Trait for replication — defined in L5, implemented by L6 or L7.
/// The commit coordinator calls this during commit (step 6a/7).
/// The implementor decides how to transport the data (TCP, QUIC, etc.).
pub trait ReplicationHook: Send + Sync {
    /// Replicate a committed WAL record and wait for acknowledgment.
    /// Called by CommitCoordinator after WAL fsync (step 6a).
    /// `lsn` — the log sequence number of the committed record.
    /// `record` — the raw WAL record bytes.
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;

    /// Whether the cluster currently has quorum.
    /// Called before accepting new commit requests.
    fn has_quorum(&self) -> bool { true }

    /// Whether the replicator is in hold state (quorum lost during commit).
    fn is_holding(&self) -> bool { false }
}

/// No-op implementation for embedded/single-node usage.
/// Provided by L5 as a convenience. L6 uses this by default.
pub struct NoReplication;

impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> {
        Ok(())
    }
}
```

### CatalogMutator

```rust
/// Trait for applying catalog mutations to the in-memory catalog and catalog B-tree.
/// Defined in L5, implemented by L6 (which owns CatalogCache + catalog B-trees).
///
/// The commit coordinator calls these during materialization (step 4a)
/// and rollback (step 7f).
#[async_trait]
pub trait CatalogMutator: Send + Sync {
    /// Apply a catalog mutation (create/drop collection/index).
    /// Updates both the catalog B-tree and in-memory CatalogCache.
    async fn apply(&self, mutation: &CatalogMutation, commit_ts: Ts) -> Result<()>;

    /// Rollback a catalog mutation (reverse of apply).
    /// Used on quorum loss (step 7f) to undo materialized catalog changes.
    async fn rollback(&self, mutation: &CatalogMutation) -> Result<()>;
}
```

## Data Structures

```rust
use tokio::sync::{mpsc, oneshot, RwLock};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::collections::HashMap;

use exdb_core::types::{CollectionId, Ts};
use exdb_storage::StorageEngine;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};

use super::timestamp::TsAllocator;
use super::read_set::ReadSet;
use super::write_set::{WriteSet, CatalogMutation, IndexDelta, IndexResolver};
use super::commit_log::CommitLog;
use super::occ;
use super::subscriptions::{
    SubscriptionRegistry, SubscriptionMode, SubscriptionId,
    InvalidationEvent, TxId,
};

/// Request submitted to the commit coordinator.
pub struct CommitRequest {
    /// Unique transaction identifier.
    pub tx_id: TxId,
    /// The read snapshot timestamp.
    pub begin_ts: Ts,
    /// Scanned intervals and catalog reads.
    pub read_set: ReadSet,
    /// Buffered mutations and catalog DDL.
    pub write_set: WriteSet,
    /// Subscription mode for this transaction.
    pub subscription: SubscriptionMode,
    /// Session that owns this transaction (for subscription routing).
    pub session_id: u64,
}

/// Result of a commit attempt.
#[derive(Debug)]
pub enum CommitResult {
    /// Commit succeeded.
    Success {
        /// The assigned commit timestamp.
        commit_ts: Ts,
        /// Subscription ID if subscription mode is not None.
        subscription_id: Option<SubscriptionId>,
        /// Receiver for invalidation events (present when subscription_id is Some).
        /// L6 wraps this in a SubscriptionHandle.
        event_rx: Option<mpsc::Receiver<InvalidationEvent>>,
    },
    /// OCC conflict detected.
    Conflict {
        /// Details about the conflict.
        error: occ::ConflictError,
        /// For Subscribe mode: a new write transaction is automatically
        /// started for retry. Contains the new tx_id and begin_ts.
        retry: Option<ConflictRetry>,
    },
    /// Replication quorum lost after materialization.
    /// The commit was materialized then rolled back.
    /// Client should retry when cluster recovers.
    QuorumLost,
}

/// Auto-retry info for Subscribe mode on OCC conflict.
#[derive(Debug)]
pub struct ConflictRetry {
    /// New transaction ID for the retry attempt.
    pub new_tx_id: TxId,
    /// New begin_ts (= current visible_ts at time of conflict).
    pub new_ts: Ts,
}

/// The commit coordinator — runs as a dedicated tokio task.
pub struct CommitCoordinator {
    // ─── Timestamp Management ───
    ts_allocator: TsAllocator,
    /// Latest timestamp safe for new readers.
    /// Only advanced after replication confirms + WAL persists.
    visible_ts: AtomicU64,

    // ─── Concurrency State ───
    commit_log: CommitLog,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,

    // ─── Storage ───
    storage: Arc<StorageEngine>,

    // ─── Index Access (from L3, injected by L6) ───
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,

    // ─── Injected Traits (implemented by L6) ───
    replication: Box<dyn ReplicationHook>,
    catalog_mutator: Arc<dyn CatalogMutator>,
    index_resolver: Arc<dyn IndexResolver>,

    // ─── Channel ───
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,

    // ─── Transaction ID Allocator ───
    next_tx_id: AtomicU64,
}

/// Handle for submitting commit requests from any task.
/// Cheaply cloneable (wraps an mpsc::Sender).
#[derive(Clone)]
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
    /// Shared reference to visible_ts for read-only transactions.
    visible_ts: Arc<AtomicU64>,
    /// Shared reference to ts_allocator for begin_ts assignment.
    ts_allocator: Arc<TsAllocator>,
    /// Shared reference for subscription management.
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    /// Transaction ID allocator (shared with coordinator).
    next_tx_id: Arc<AtomicU64>,
}
```

## CommitHandle API

```rust
impl CommitHandle {
    /// Submit a commit request and wait for the result.
    /// Called by L6 Transaction::commit().
    pub async fn commit(&self, request: CommitRequest) -> CommitResult;

    /// Get the current visible_ts (for read-only transaction begin_ts).
    pub fn visible_ts(&self) -> Ts;

    /// Allocate a new transaction ID.
    pub fn allocate_tx_id(&self) -> TxId;

    /// Get the subscriptions registry (for L6/L8 access).
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>>;
}
```

## Commit Protocol — 11 Steps

The coordinator processes one request at a time from `commit_rx`:

```
async fn process_commit(request, response_tx):

    // ── Step 1: VALIDATE ──
    // Skip OCC for transactions with empty write set (no document mutations
    // AND no catalog mutations). These are read-only transactions that are
    // only committing to register a subscription.
    if !request.write_set.is_empty():
        let commit_ts_candidate = ts_allocator.latest() + 1  // peek, don't allocate yet
        match occ::validate(&request.read_set, &commit_log,
                            request.begin_ts, commit_ts_candidate):
            Err(conflict) =>
                // For Subscribe mode: auto-start retry transaction
                let retry = if request.subscription == Subscribe:
                    let new_tx_id = allocate_tx_id()
                    let new_ts = visible_ts()
                    Some(ConflictRetry { new_tx_id, new_ts })
                else:
                    None
                response_tx.send(CommitResult::Conflict { error: conflict, retry })
                return
            Ok(()) => ()

    // ── Step 2: TIMESTAMP ──
    let commit_ts = ts_allocator.allocate()

    // ── Step 3: PERSIST (WAL) ──
    // Serialize: write_set mutations + catalog_mutations + commit_ts
    let wal_record = serialize_commit_record(&request.write_set, commit_ts)
    let lsn = storage.append_wal(WAL_RECORD_TX_COMMIT, &wal_record).await?
    // fsync is handled by WAL writer (group commit)

    // ── Step 4a: MATERIALIZE CATALOG ──
    // Apply catalog mutations FIRST so newly created collections
    // exist before documents are inserted.
    for each mutation in request.write_set.catalog_mutations:
        catalog_mutator.apply(&mutation, commit_ts).await?

    // ── Step 4b: MATERIALIZE DATA ──
    // Compute index deltas, then apply mutations to primary + secondary indexes.
    let index_deltas = write_set::compute_index_deltas(
        &request.write_set,
        index_resolver.as_ref(),
        &*primary_indexes.read().await,
    ).await?

    // Apply to primary indexes
    let primaries = primary_indexes.read().await
    for each (collection_id, doc_id), entry in request.write_set.mutations:
        let primary = &primaries[&collection_id]
        let body_bytes = entry.body.as_ref().map(|v| serde_json::to_vec(v)).transpose()?
        primary.insert_version(&doc_id, commit_ts, body_bytes.as_deref()).await?

    // Apply to secondary indexes
    let secondaries = secondary_indexes.read().await
    for each delta in &index_deltas:
        let secondary = &secondaries[&delta.index_id]
        if let Some(old_key) = &delta.old_key:
            // Build full key: old_key_prefix || doc_id || inv_ts(commit_ts)
            // Note: old entries are NOT removed immediately — MVCC keeps them.
            // Vacuum handles cleanup. But we DO need to insert the new version's
            // secondary key pointing to the new primary version.
            ()
        if let Some(new_key) = &delta.new_key:
            let full_key = make_secondary_key_from_prefix(new_key, &delta.doc_id, commit_ts)
            secondary.insert_entry(&full_key).await?

    // ── Step 5: LOG ──
    let log_entry = CommitLogEntry {
        commit_ts,
        index_writes: index_deltas_to_key_writes(&index_deltas),
        catalog_mutations: request.write_set.catalog_mutations.clone(),
    }
    commit_log.append(log_entry)

    // ── Step 6: CONCURRENT START ──
    // 6a: Replication (may be no-op for NoReplication)
    // 6b: Subscription invalidation check
    let (replication_result, invalidation_events) = tokio::join!(
        // 6a
        replication.replicate_and_wait(lsn, &wal_record),
        // 6b
        async {
            let mut subs = subscriptions.write().await;
            subs.check_invalidation(
                commit_ts,
                &log_entry.index_writes,
                &log_entry.catalog_mutations,
                || self.allocate_tx_id(),
            )
        }
    )

    // ── Step 7: AWAIT REPLICATION ──
    match replication_result:
        Ok(()) => ()  // continue to success path
        Err(_) =>
            // ── FAILURE PATH: Quorum Lost ──
            // 7f-a: Rollback data mutations (delete inserted versions)
            let primaries = primary_indexes.read().await
            for each (collection_id, doc_id), entry in request.write_set.mutations:
                let primary = &primaries[&collection_id]
                // Insert a tombstone at commit_ts to logically delete the version
                // we just inserted. Vacuum will physically remove both.
                primary.insert_version(&doc_id, commit_ts, None).await?

            // 7f-b: Rollback secondary index entries
            let secondaries = secondary_indexes.read().await
            for each delta in &index_deltas:
                if let Some(new_key) = &delta.new_key:
                    let full_key = make_secondary_key_from_prefix(new_key, &delta.doc_id, commit_ts)
                    secondaries[&delta.index_id].remove_entry(&full_key).await?

            // 7f-c: Rollback catalog mutations (reverse order)
            for mutation in request.write_set.catalog_mutations.iter().rev():
                catalog_mutator.rollback(&mutation).await?

            // 8f: Remove from commit log
            commit_log.remove(commit_ts)

            // 9f: Discard invalidation events (they reference rolled-back state)
            // (events were computed in step 6b but commit is being rolled back)
            drop(invalidation_events)

            // 10f: Respond
            response_tx.send(CommitResult::QuorumLost)
            return

    // ═══ SUCCESS PATH ═══

    // ── Step 8: PERSIST VISIBLE_TS ──
    storage.append_wal(WAL_RECORD_VISIBLE_TS, &commit_ts.to_be_bytes()).await?

    // ── Step 9: ADVANCE visible_ts ──
    visible_ts.store(commit_ts, Ordering::SeqCst)

    // ── Step 10: REGISTER SUBSCRIPTION + RESPOND ──
    let (subscription_id, event_rx) = if request.subscription != SubscriptionMode::None:
        // Create channel for pushing invalidation events to the client.
        // The sender goes into the registry; the receiver goes to the client (via CommitResult).
        let (event_tx, event_rx) = mpsc::channel(64)  // bounded, capacity configurable
        let mut subs = subscriptions.write().await;
        let id = subs.register(
            request.subscription,
            request.session_id,
            request.tx_id,
            commit_ts,  // read_ts for the subscription
            request.read_set,
            event_tx,
        )
        (Some(id), Some(event_rx))
    else:
        (None, None)

    response_tx.send(CommitResult::Success { commit_ts, subscription_id, event_rx })

    // ── Step 11: PUSH INVALIDATION (fire-and-forget) ──
    // Route invalidation events from step 6b to existing subscriber sessions.
    // Uses the event_tx stored in SubscriptionMeta (from prior registrations).
    // This does NOT block the commit response (response sent in step 10).
    let subs = subscriptions.read().await
    for event in invalidation_events:
        if let Some(meta) = subs.subscriptions.get(&event.subscription_id):
            // try_send: non-blocking. If channel full, event is dropped.
            // Client can detect missed events via commit_ts gap.
            let _ = meta.event_tx.try_send(event)
```

## Helper Functions

### `serialize_commit_record`

```rust
/// Serialize a commit's write set into a WAL record payload.
/// Format: commit_ts[8] || mutation_count[4] || mutations[...] ||
///         catalog_mutation_count[4] || catalog_mutations[...]
///
/// Each mutation: collection_id[8] || doc_id[16] || op[1] || body_len[4] || body[body_len]
///   op: 0x01=Insert, 0x02=Replace, 0x03=Delete
///   body_len: 0 for Delete
///
/// Catalog mutations are serialized via a simple tag-length-value scheme:
///   tag[1] || payload_len[4] || payload[...]
///   Tags: 0x01=CreateCollection, 0x02=DropCollection, 0x03=CreateIndex, 0x04=DropIndex
fn serialize_commit_record(write_set: &WriteSet, commit_ts: Ts) -> Vec<u8>;

/// Deserialize a WAL_RECORD_TX_COMMIT payload for recovery.
/// Returns (commit_ts, mutations, catalog_mutations).
pub fn deserialize_commit_record(payload: &[u8])
    -> Result<(Ts, Vec<(CollectionId, DocId, MutationOp, Option<Vec<u8>>)>, Vec<CatalogMutation>)>;
```

### `index_deltas_to_key_writes`

```rust
/// Convert IndexDeltas into IndexKeyWrites for the commit log.
/// Groups by (collection_id, index_id), maps old_key/new_key directly.
fn index_deltas_to_key_writes(
    deltas: &[IndexDelta],
) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>;
```

### `WriteSet::is_empty` Semantics

`is_empty()` returns `true` when **both** `mutations` (document ops) and `catalog_mutations` (DDL ops) are empty. A transaction with only catalog mutations (e.g., `CREATE COLLECTION`) is NOT empty — it goes through full OCC validation and WAL persistence.

## Visibility Fence (`visible_ts`)

`visible_ts` is the latest timestamp safe for new read transactions:

- **Read-only transactions** get `begin_ts = visible_ts` (NOT `ts_allocator.latest()`)
- `visible_ts` only advances after replication confirms AND WAL records this
- Without replication (`NoReplication`): advances immediately after commit WAL fsync
- On recovery: set to last `WAL_RECORD_VISIBLE_TS` found during WAL replay

**Why this matters (phantom read prevention):**

Without a visibility fence, a reader on the primary could see data that hasn't been replicated yet. If the primary crashes, that data is lost, but the reader already acted on it. The fence ensures readers only see fully replicated data.

**Vacuum interaction:** Vacuum uses `min(oldest_active_read_ts, visible_ts)` as its safe threshold.

## OCC Validation Timing

The validation at Step 1 uses `ts_allocator.latest() + 1` as the candidate `commit_ts`. This is an optimistic check — the actual `commit_ts` is allocated at Step 2. Because the commit coordinator processes requests sequentially, no other commit can interleave between Steps 1 and 2, so the candidate equals the actual `commit_ts`.

**Subtle point:** If validation were to happen AFTER `commit_ts` allocation, a conflict would "waste" a timestamp. By peeking first, we only allocate on the success path. The wasted timestamp is harmless (just a gap in the sequence), but avoiding it is cleaner.

## Read-Only Transaction Commits

Read-only transactions with a subscription mode (Notify/Watch/Subscribe) need to "commit" to register their read set. The protocol is simplified:

1. Skip Steps 1–4 (no validation needed, no writes)
2. Skip Step 5 (no commit log entry)
3. Skip Steps 6a, 7, 8, 9 (no replication, no visibility change)
4. Step 10: Register subscription (L6 creates the mpsc channel, calls `registry.register()` directly)
5. Step 11: N/A (no writes = no invalidations)

Read-only commits don't go through the single-writer channel at all — they can register subscriptions directly via the shared `SubscriptionRegistry` lock. This avoids serializing read-only commits behind write commits.

## Conflict Retry for Subscribe Mode

When a write transaction with `SubscriptionMode::Subscribe` hits an OCC conflict:

1. A new `TxId` is allocated
2. The new `begin_ts` is set to current `visible_ts`
3. `ConflictRetry { new_tx_id, new_ts }` is returned in the `CommitResult`
4. L6 creates a new `Transaction` with these values
5. The client can retry their mutations in the new transaction

Note: conflict retry does NOT carry forward the read set. Carry-forward only happens on subscription invalidation (post-commit). On conflict, the transaction never committed, so there's no read set to carry — the client must re-execute everything.

## Startup

```rust
impl CommitCoordinator {
    /// Create a new commit coordinator.
    /// Called by L6 Database::open() after storage recovery.
    pub fn new(
        initial_ts: Ts,          // highest commit_ts from WAL replay
        visible_ts: Ts,          // last WAL_RECORD_VISIBLE_TS from replay
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        catalog_mutator: Arc<dyn CatalogMutator>,
        index_resolver: Arc<dyn IndexResolver>,
        channel_size: usize,     // bounded channel capacity (default: 256)
    ) -> (Self, CommitHandle);

    /// Run the commit loop. Spawned as a dedicated tokio task.
    /// Processes requests until the channel is closed (shutdown).
    pub async fn run(&mut self);
}
```

## Shutdown

1. L6 drops all `CommitHandle` clones → channel closes
2. `run()` loop exits when `commit_rx.recv()` returns `None`
3. Any in-flight commit completes before the loop exits
4. The coordinator is dropped, releasing all resources

## WAL Record Format

The commit coordinator writes two WAL record types:

| Type Tag | Name | Payload |
|----------|------|---------|
| `0x01` | `WAL_RECORD_TX_COMMIT` | `commit_ts[8] \|\| mutation_count[4] \|\| mutations[...] \|\| catalog_mutation_count[4] \|\| catalog_mutations[...]` |
| `0x09` | `WAL_RECORD_VISIBLE_TS` | `visible_ts[8]` |

The `TxCommit` record contains everything needed to replay the commit:
- Document mutations: `collection_id[8] \|\| doc_id[16] \|\| op[1] \|\| body_len[4] \|\| body[body_len]`
  - `op`: `0x01`=Insert, `0x02`=Replace, `0x03`=Delete (body_len=0 for Delete)
- Catalog mutations: `tag[1] \|\| payload_len[4] \|\| payload[payload_len]`
  - Tags: `0x01`=CreateCollection, `0x02`=DropCollection, `0x03`=CreateIndex, `0x04`=DropIndex

**Note on existing WAL record types:** L2 defines separate WAL records for catalog operations (`WAL_RECORD_CREATE_COLLECTION` etc., tags 0x03–0x06). These are used by L3 for non-transactional catalog operations during recovery/startup. At runtime, all catalog mutations go through the `WAL_RECORD_TX_COMMIT` record for atomicity with data mutations.

## Tests

```
t7_commit_success_basic
    Create in-memory StorageEngine + CommitCoordinator (with mock traits).
    Submit a commit with one insert. Assert CommitResult::Success.
    Verify commit_ts > begin_ts.

t7_commit_occ_conflict
    Submit commit A (writes new_key=15 into index).
    Submit commit B (begin_ts before A, reads interval [10,20)).
    B gets ConflictError because A wrote key 15 into B's read interval.

t7_commit_occ_no_conflict
    Submit two commits with non-overlapping read/write sets.
    Both succeed.

t7_commit_subscribe_conflict_retry
    Submit commit with Subscribe mode that conflicts.
    CommitResult::Conflict has retry with new_tx_id and new_ts.

t7_commit_subscribe_none_conflict_no_retry
    Submit commit with SubscriptionMode::None that conflicts.
    CommitResult::Conflict has retry = None.

t7_commit_registers_subscription
    Submit commit with SubscriptionMode::Notify.
    CommitResult::Success has subscription_id = Some(...) and event_rx = Some(...).

t7_commit_no_subscription_mode_none
    Submit commit with SubscriptionMode::None.
    CommitResult::Success has subscription_id = None and event_rx = None.

t7_commit_visible_ts_advances
    visible_ts starts at 0. Submit commit.
    visible_ts advances to commit_ts.

t7_commit_visible_ts_not_advanced_until_replication
    Mock ReplicationHook that delays response.
    Submit commit. Verify visible_ts is NOT advanced until replication completes.

t7_commit_invalidation_fires
    Register subscription S1 watching interval [10, 20) (with event channel).
    Submit commit writing key 15.
    S1's event_rx receives InvalidationEvent.

t7_commit_subscribe_chain_continuation
    Register Subscribe subscription with queries Q0, Q1, Q2.
    Submit commit that invalidates Q1.
    InvalidationEvent has continuation with carried Q0 + first_query_id=1.

t7_readonly_commit_registers_subscription
    Call CommitHandle directly (bypass channel) with empty write set + Watch.
    Subscription registered. No WAL record written.
    event_rx returned to caller.

t7_commit_ordering
    Submit 5 commits sequentially. Verify commit_ts is strictly increasing.

t7_commit_sequential_processing
    Submit 3 commits concurrently via CommitHandle.
    All succeed with different commit_ts values.
    Verify serialization (no interleaving).

t7_commit_quorum_lost_rollback
    Mock ReplicationHook that returns Err.
    Submit commit. Verify:
    - CommitResult::QuorumLost returned
    - Materialized data rolled back (primary versions removed)
    - Secondary index entries removed
    - Commit log entry removed
    - visible_ts NOT advanced

t7_commit_catalog_only
    Submit commit with only CatalogMutation::CreateCollection (no doc mutations).
    write_set.is_empty() is false. Full OCC + WAL + materialize path runs.
    CommitResult::Success.

t7_commit_catalog_occ_conflict
    Transaction A reads CatalogRead::CollectionByName("users").
    Concurrent commit creates collection "users".
    Transaction A gets ConflictError with ConflictKind::Catalog.

t7_shutdown_graceful
    Drop all CommitHandle clones. Verify run() exits cleanly.

t7_event_channel_full_no_block
    Register subscription with small channel capacity (1).
    Submit two rapid commits that invalidate the subscription.
    Verify commit loop does NOT block (try_send drops the second event).
```
