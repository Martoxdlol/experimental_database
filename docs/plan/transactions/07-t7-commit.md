# T7: Commit Protocol

## Purpose

Two-task commit architecture implementing the commit protocol from DESIGN.md 5.12. The **writer task** serializes page mutations (steps 1–5) with no network dependency. The **replication task** handles visibility advancement, subscription management, and client notification (steps 6–11). This split ensures the writer is never blocked on replication latency.

## Dependencies

- **T1 (`timestamp.rs`)**: `TsAllocator`
- **T2 (`read_set.rs`)**: `ReadSet`, `ReadInterval`, `LimitBoundary`, `QueryId`
- **T3 (`write_set.rs`)**: `WriteSet`, `MutationEntry`, `MutationOp`, `CatalogMutation`, `IndexDelta`, `IndexResolver`, `compute_index_deltas`
- **T4 (`commit_log.rs`)**: `CommitLog`, `CommitLogEntry`, `IndexKeyWrite`
- **T5 (`occ.rs`)**: `validate`, `ConflictError`
- **T6 (`subscriptions.rs`)**: `SubscriptionRegistry`, `SubscriptionMode`, `SubscriptionId`, `InvalidationEvent`, `TxId`
- **L2 (`storage/engine.rs`)**: `StorageEngine::append_wal`
- **L2 (`storage/wal.rs`)**: `WAL_RECORD_TX_COMMIT`, `WAL_RECORD_VISIBLE_TS`, `Lsn`
- **L3 (`primary_index.rs`)**: `PrimaryIndex::insert_version`
- **L3 (`secondary_index.rs`)**: `SecondaryIndex::insert_entry`, `SecondaryIndex::remove_entry`
- **L3 (`key_encoding.rs`)**: `make_secondary_key_from_prefix`
- **L3 (`array_indexing.rs`)**: `compute_index_entries`
- **tokio**: `mpsc`, `oneshot` (commit request/response channels)

## Architecture Overview

```
                    ┌─────────────────────┐
  CommitHandle ───► │   Writer Task       │  steps 1–5
  (mpsc)            │   (single-writer)   │  OCC → ts → WAL → mutations → commit log
                    └────────┬────────────┘
                             │ replication_tx (mpsc)
                             ▼
                    ┌─────────────────────┐
                    │  Replication Task   │  steps 6–11
                    │  (serial, ordered)  │  replicate → visible_ts → subs → respond
                    └─────────────────────┘
```

## Rust Types

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_storage::engine::StorageEngine;
use exdb_storage::wal::Lsn;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};

use crate::timestamp::TsAllocator;
use crate::read_set::ReadSet;
use crate::write_set::{WriteSet, IndexDelta, IndexResolver};
use crate::commit_log::CommitLog;
use crate::occ::ConflictError;
use crate::subscriptions::{
    SubscriptionRegistry, SubscriptionMode, SubscriptionId,
    InvalidationEvent, TxId,
};

/// Request submitted to the commit coordinator.
pub struct CommitRequest {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub subscription: SubscriptionMode,
    pub session_id: u64,
}

/// Successful commit result.
pub enum CommitResult {
    Success {
        commit_ts: Ts,
        subscription_id: Option<SubscriptionId>,
        /// Event receiver for the newly registered subscription.
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

/// Auto-retry info for Subscribe mode OCC conflicts.
pub struct ConflictRetry {
    pub new_tx_id: TxId,
    pub new_ts: Ts,
}

/// Trait for replication — defined in L5, implemented by L6 or L7.
///
/// Injected into the replication task at construction time.
/// Default implementation: `NoReplication` (for embedded/single-node).
#[async_trait::async_trait]
pub trait ReplicationHook: Send + Sync {
    /// Replicate a WAL record and wait for quorum acknowledgement.
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<(), String>;

    /// Check if replication quorum is available.
    fn has_quorum(&self) -> bool { true }

    /// Check if replication is in a holding state (e.g., waiting for leader election).
    fn is_holding(&self) -> bool { false }
}

/// No-op replication for embedded/single-node. Always succeeds.
pub struct NoReplication;

#[async_trait::async_trait]
impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// Commit error types.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    #[error("OCC conflict: {0}")]
    Conflict(ConflictError),
    #[error("replication quorum lost")]
    QuorumLost,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Entry enqueued from writer task to replication task after steps 1–5 complete.
struct ReplicationEntry {
    commit_ts: Ts,
    lsn: Lsn,
    wal_payload: Vec<u8>,
    read_set: ReadSet,
    index_deltas: Vec<IndexDelta>,
    subscription: SubscriptionMode,
    session_id: u64,
    tx_id: TxId,
    response_tx: oneshot::Sender<CommitResult>,
}

/// The single-writer commit coordinator.
///
/// **`!Send`**: Contains parking_lot guards from StorageEngine async
/// methods. Must be spawned on a `tokio::task::LocalSet` or single-threaded
/// runtime. The `CommitHandle` is `Send + Clone` and can be used from any task.
pub struct CommitCoordinator {
    ts_allocator: Arc<TsAllocator>,
    visible_ts: Arc<AtomicU64>,
    commit_log: Arc<RwLock<CommitLog>>,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    index_resolver: Arc<dyn IndexResolver>,
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
    replication_tx: mpsc::Sender<ReplicationEntry>,
    next_tx_id: Arc<AtomicU64>,
}

/// The replication task — processes committed entries in order.
///
/// `Send` — runs on any tokio task. No page guards held.
pub struct ReplicationRunner {
    visible_ts: Arc<AtomicU64>,
    commit_log: Arc<RwLock<CommitLog>>,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    storage: Arc<StorageEngine>,
    replication: Box<dyn ReplicationHook>,
    replication_rx: mpsc::Receiver<ReplicationEntry>,
    next_tx_id: Arc<AtomicU64>,
}

/// Clone-able handle for submitting commit requests from any task.
///
/// `Send + Clone` — can be distributed to multiple reader/writer tasks.
#[derive(Clone)]
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
    visible_ts: Arc<AtomicU64>,
    ts_allocator: Arc<TsAllocator>,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    next_tx_id: Arc<AtomicU64>,
}
```

## Implementation Details

### Constructor

```rust
impl CommitCoordinator {
    /// Create a new commit coordinator, replication runner, and handle.
    ///
    /// `initial_ts`: highest committed timestamp from WAL recovery.
    /// `visible_ts`: last WAL_RECORD_VISIBLE_TS from recovery.
    /// `channel_size`: bounded capacity for the commit request channel.
    /// `replication_queue_size`: bounded capacity for the writer→replication queue.
    ///
    /// Returns (coordinator, replication_runner, handle).
    /// - Coordinator must be `.run()` on a LocalSet (owns page guards).
    /// - ReplicationRunner must be `.run()` on any tokio task.
    /// - Handle is distributed to application code.
    pub fn new(
        initial_ts: Ts,
        visible_ts: Ts,
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        index_resolver: Arc<dyn IndexResolver>,
        channel_size: usize,
        replication_queue_size: usize,
    ) -> (Self, ReplicationRunner, CommitHandle);
}
```

### CommitHandle methods

```rust
impl CommitHandle {
    /// Submit a commit request and await the result.
    ///
    /// The response arrives after the replication task has confirmed
    /// visibility — not immediately after the writer processes the commit.
    pub async fn commit(&self, request: CommitRequest) -> CommitResult {
        let (response_tx, response_rx) = oneshot::channel();
        // If send fails, the coordinator has been dropped
        if self.tx.send((request, response_tx)).await.is_err() {
            return CommitResult::QuorumLost; // coordinator gone
        }
        response_rx.await.unwrap_or(CommitResult::QuorumLost)
    }

    /// Get the current visible_ts (latest safe read timestamp).
    pub fn visible_ts(&self) -> Ts {
        self.visible_ts.load(Ordering::Acquire)
    }

    /// Allocate a new TxId.
    pub fn allocate_tx_id(&self) -> TxId {
        self.next_tx_id.fetch_add(1, Ordering::AcqRel)
    }
}
```

### Writer Task (Steps 1–5)

```rust
impl CommitCoordinator {
    /// The single-writer commit loop. Processes one commit at a time.
    ///
    /// After steps 1–5, enqueues to the replication task and immediately
    /// loops back for the next commit. Never blocked on network I/O.
    ///
    /// Runs until the channel is closed (all handles dropped).
    pub async fn run(&mut self) {
        while let Some((request, response_tx)) = self.commit_rx.recv().await {
            match self.process_commit(request, response_tx).await {
                Ok(()) => {} // enqueued to replication task
                Err(result) => {} // OCC conflict or error, already responded
            }
        }
    }

    /// Returns Ok(()) if enqueued to replication, Err if responded directly
    /// (OCC conflict, read-only, or I/O error).
    async fn process_commit(
        &mut self,
        req: CommitRequest,
        response_tx: oneshot::Sender<CommitResult>,
    ) -> Result<(), ()> {
        // see steps below
    }
}
```

#### Step 1: OCC Validation

```rust
// Skip OCC for read-only transactions (empty write set) with no
// subscription — they have nothing to validate.
// Read-only with subscription: still validate to ensure the subscription
// is based on a consistent read.
if !req.write_set.is_empty() || req.subscription != SubscriptionMode::None {
    let commit_log = self.commit_log.read();
    // commit_ts_candidate = next timestamp that will be assigned
    let commit_ts_candidate = self.ts_allocator.latest() + 1;
    if let Err(conflict) = validate(&req.read_set, &commit_log, req.begin_ts, commit_ts_candidate) {
        // On conflict + Subscribe mode: auto-start retry tx
        let retry = if req.subscription == SubscriptionMode::Subscribe {
            Some(ConflictRetry {
                new_tx_id: self.next_tx_id.fetch_add(1, Ordering::AcqRel),
                new_ts: self.visible_ts.load(Ordering::Acquire),
            })
        } else {
            None
        };
        let _ = response_tx.send(CommitResult::Conflict { error: conflict, retry });
        return Err(());
    }
}
```

#### Step 2: Assign commit_ts

```rust
let commit_ts = self.ts_allocator.allocate();
```

#### Step 3: WAL Persist + fsync

```rust
let wal_payload = serialize_wal_payload(commit_ts, &req.write_set);
let lsn = self.storage.append_wal(WAL_RECORD_TX_COMMIT, &wal_payload).await?;
```

**WAL payload format** (length-prefixed binary):
```
commit_ts:      u64 (LE)
mutation_count: u32 (LE)
for each mutation:
    collection_id: u64 (LE)
    doc_id:        [u8; 16]
    op_tag:        u8  (0x01=Insert, 0x02=Replace, 0x03=Delete)
    body_len:      u32 (LE)  (0 for Delete)
    body:          [u8; body_len]  (JSON bytes)
catalog_count:  u32 (LE)
for each catalog mutation:
    type_tag:   u8  (0x01=CreateCollection, 0x02=DropCollection, 0x03=CreateIndex, 0x04=DropIndex)
    payload:    type-specific binary encoding
```

#### Step 4: Apply Mutations + Compute Deltas

Catalog mutations are handled by L6 as a post-commit callback — L5 treats
them as regular mutations on the CATALOG_COLLECTIONS / CATALOG_INDEXES
pseudo-collections. The physical side effects (allocating B-tree root pages,
updating CatalogCache) are triggered by L6 when it detects writes to
reserved CollectionIds in the commit result. No CatalogMutator trait needed.

```rust
// Apply mutations to primary and secondary indexes
let primaries = self.primary_indexes.read();
let secondaries = self.secondary_indexes.read();

for ((coll_id, doc_id), entry) in &req.write_set.mutations {
    let primary = &primaries[coll_id];
    let body_bytes = entry.body.as_ref().map(|v| serde_json::to_vec(v).unwrap());
    primary.insert_version(doc_id, commit_ts, body_bytes.as_deref()).await?;
}

// Compute index deltas (reads old docs for Replace/Delete)
let index_deltas = compute_index_deltas(
    &req.write_set,
    self.index_resolver.as_ref(),
    &primaries.iter().map(|(&k, v)| (k, v.clone())).collect(),
    commit_ts,
).await?;

// Apply secondary index mutations
for delta in &index_deltas {
    if let Some(secondary) = secondaries.get(&delta.index_id) {
        if let Some(old_key) = &delta.old_key {
            secondary.remove_entry(old_key).await?;
        }
        if let Some(new_key) = &delta.new_key {
            secondary.insert_entry(new_key).await?;
        }
    }
}
```

#### Step 5: Append to Commit Log + Enqueue to Replication

```rust
let log_entry = build_commit_log_entry(commit_ts, &index_deltas);
self.commit_log.write().append(log_entry);

// Enqueue to replication task — writer is done with this commit.
// The replication task will handle visibility, subscriptions, and client response.
let entry = ReplicationEntry {
    commit_ts,
    lsn,
    wal_payload,
    read_set: req.read_set,
    index_deltas,
    subscription: req.subscription,
    session_id: req.session_id,
    tx_id: req.tx_id,
    response_tx,
};

if self.replication_tx.send(entry).await.is_err() {
    // Replication task has been dropped — should not happen in normal operation
    tracing::error!("replication task dropped, cannot complete commit {commit_ts}");
}
// Writer immediately loops back for next commit
```

### Replication Task (Steps 6–11)

```rust
impl ReplicationRunner {
    /// Process the replication queue in order.
    ///
    /// For each entry: replicate → advance visible_ts → subscriptions → respond.
    /// Replications are pipelined but visible_ts advances strictly in order.
    ///
    /// On replication failure: rollback all entries beyond visible_ts and stop.
    pub async fn run(&mut self) {
        while let Some(entry) = self.replication_rx.recv().await {
            if !self.process_entry(entry).await {
                // Replication failure — drain remaining entries with QuorumLost
                self.drain_with_error().await;
                break;
            }
        }
    }

    /// Returns true on success, false on replication failure.
    async fn process_entry(&mut self, entry: ReplicationEntry) -> bool {
        // see steps below
    }

    /// After replication failure, drain the queue and respond QuorumLost.
    async fn drain_with_error(&mut self) {
        while let Ok(entry) = self.replication_rx.try_recv() {
            let _ = entry.response_tx.send(CommitResult::QuorumLost);
        }
        // Also remove commit log entries beyond visible_ts
        let visible = self.visible_ts.load(Ordering::Acquire);
        self.commit_log.write().remove_after(visible);
    }
}
```

#### Step 6: Replicate

```rust
if let Err(msg) = self.replication.replicate_and_wait(entry.lsn, &entry.wal_payload).await {
    // Replication failed — respond QuorumLost to this client
    let _ = entry.response_tx.send(CommitResult::QuorumLost);
    tracing::error!("replication failed at commit_ts={}: {msg}", entry.commit_ts);
    // Remove commit log entries beyond visible_ts (this entry + any enqueued after)
    let visible = self.visible_ts.load(Ordering::Acquire);
    self.commit_log.write().remove_after(visible);
    return false; // signals caller to drain remaining entries
}
```

#### Step 7: WAL visible_ts Record

```rust
self.storage.append_wal(WAL_RECORD_VISIBLE_TS, &entry.commit_ts.to_le_bytes()).await?;
```

#### Step 8: Advance visible_ts

```rust
self.visible_ts.store(entry.commit_ts, Ordering::Release);
```

#### Step 9: Subscription Invalidation Check

```rust
// Run invalidation check (fast, in-memory)
// Now safe because visible_ts has advanced — these commits are confirmed visible.
let index_writes = build_index_writes(&entry.index_deltas);
let mut subs = self.subscriptions.write();
let invalidation_events = subs.check_invalidation(
    entry.commit_ts,
    &index_writes,
    || self.next_tx_id.fetch_add(1, Ordering::AcqRel),
);
```

#### Step 10: Register Subscription (if requested)

```rust
let (subscription_id, event_rx) = if entry.subscription != SubscriptionMode::None {
    // 10a: Clear stale limit boundaries from tx's own writes
    let mut read_set = entry.read_set;
    read_set.extend_for_deltas(&entry.index_deltas);

    // 10b: Create channel and register
    let (event_tx, event_rx) = mpsc::channel(64);
    let sub_id = subs.register(
        entry.subscription,
        entry.session_id,
        entry.tx_id,
        entry.commit_ts,
        read_set,
        event_tx,
    );
    (Some(sub_id), Some(event_rx))
} else {
    (None, None)
};
```

#### Step 11: Push Invalidation Events + Respond

```rust
// Fire-and-forget: push events via try_send on each sub's event_tx
subs.push_events(invalidation_events);
drop(subs); // release subscription lock before responding

// Respond to client — commit is now fully visible and replicated
let _ = entry.response_tx.send(CommitResult::Success {
    commit_ts: entry.commit_ts,
    subscription_id,
    event_rx,
});
```

### Helper: build_commit_log_entry

```rust
fn build_commit_log_entry(
    commit_ts: Ts,
    index_deltas: &[IndexDelta],
) -> CommitLogEntry {
    let mut index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> = BTreeMap::new();
    for delta in index_deltas {
        index_writes
            .entry((delta.collection_id, delta.index_id))
            .or_default()
            .push(IndexKeyWrite {
                doc_id: delta.doc_id,
                old_key: delta.old_key.clone(),
                new_key: delta.new_key.clone(),
            });
    }
    CommitLogEntry { commit_ts, index_writes }
}
```

### Helper: build_index_writes

```rust
/// Extract index writes from deltas for subscription invalidation check.
/// Same structure as CommitLogEntry.index_writes but built from the
/// ReplicationEntry's deltas directly.
fn build_index_writes(
    index_deltas: &[IndexDelta],
) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> {
    // identical to build_commit_log_entry but returns just the map
}
```

### Helper: serialize_wal_payload / deserialize_wal_payload

Binary serialization of commit data for WAL. Format documented in Step 3 above. These are private helper functions within commit.rs.

## Design Decisions

### 1. Two-task architecture (writer + replication)

The writer task (steps 1–5) handles only local operations: OCC validation, WAL fsync, page mutations, and commit log updates. No network I/O ever blocks the writer. After step 5, the commit is enqueued to the replication task via a bounded mpsc channel.

The replication task (steps 6–11) processes entries in strict commit order. It handles replication, visibility advancement, subscription management, and client notification. This task can be `Send` — it holds no page guards.

**Why**: Replication latency (network round-trips, quorum waiting) is orders of magnitude slower than local page mutations. Decoupling them means the writer can process N commits ahead while replication catches up. Without this split, every commit would be blocked on the slowest replica.

### 2. Shared CommitLog via Arc<RwLock>

The commit log is shared between the writer (appends entries at step 5) and the replication task (removes entries on rollback). The writer takes a write lock briefly to append; OCC validation (step 1) takes a read lock to scan. The replication task takes a write lock only on failure (to remove entries beyond visible_ts).

### 3. Replication failure and rollback

On replication failure at `commit_ts = N`:

1. The failing entry's client receives `QuorumLost`.
2. All commit log entries with `ts > visible_ts` are removed. This is critical because the writer may have validated subsequent commits (ts = N+1, N+2, ...) against the now-invalid entry at ts = N.
3. All remaining entries in the replication queue receive `QuorumLost`.
4. The replication task stops — no further entries are processed.
5. The writer must be signaled to stop accepting new commits (the replication_tx channel being dropped serves as this signal — the writer's `replication_tx.send()` will fail).
6. Page store cleanup: mutations for `ts > visible_ts` are physically present but invisible. Rollback vacuum (L3) cleans them up on restart.

### 4. Channel-based request submission

CommitHandle sends `(CommitRequest, oneshot::Sender<CommitResult>)` via a bounded mpsc channel. The writer processes one request at a time. The oneshot sender is forwarded to the replication task, which sends the response after visibility is confirmed. Default channel capacities: commit channel = 256, replication queue = 256.

### 5. Shared state via Arc

`TsAllocator`, `visible_ts`, `next_tx_id`, `CommitLog`, and `SubscriptionRegistry` are shared between CommitCoordinator, ReplicationRunner, and CommitHandle via `Arc`. The replication task is the only writer for `visible_ts`; the writer and handles only read it.

### 6. Read-only subscription commits

Read-only transactions with `subscription != None` still go through the commit coordinator. OCC validation runs (to ensure the subscription is based on a consistent read), but steps 3–5 (WAL, mutations, commit log) are skipped. The entry is still enqueued to the replication task for subscription registration — but with no WAL payload, replication is a no-op, so visibility is immediate.

### 7. Unified catalog handling

Catalog mutations are regular writes on reserved pseudo-collections (CATALOG_COLLECTIONS, CATALOG_INDEXES). L5 has no CatalogMutator trait — it treats catalog writes identically to data writes. L6 detects writes to reserved CollectionIds in the commit result and triggers physical side effects (page allocation for new B-trees, CatalogCache updates) as a post-commit callback. This eliminates ~100 lines of catalog-specific code from L5.

### 8. Subscription invalidation after visible_ts

Subscriptions are checked for invalidation (step 9) only after `visible_ts` advances (step 8). This ensures clients never receive invalidation events for commits that might be rolled back due to replication failure. In the old architecture, invalidation happened before replication — a failing replication would have already sent events that need to be retracted.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| `CommitResult::Conflict` | OCC validation fails (step 1) | Writer responds directly to caller. Subscribe mode includes ConflictRetry. |
| `CommitResult::QuorumLost` | Replication failure or coordinator/replication task dropped | Replication task responds to affected client + all queued clients. |
| `CommitError::Io` | WAL write or index mutation I/O failure | Writer propagates to caller. Writer remains usable for subsequent commits. |

## Tests

### Basic commit flow

1. **commit_read_only**: Empty write set, no subscription → success, no WAL write.
2. **commit_single_insert**: One insert → success, commit_ts assigned, WAL written.
3. **commit_advances_visible_ts**: After commit response received, `handle.visible_ts()` == commit_ts.
4. **commit_sequential_timestamps**: Two commits get sequential commit_ts values.
5. **commit_handle_clone**: Multiple handles can submit commits.

### Writer–replication pipeline

6. **writer_does_not_block_on_replication**: Mock slow replication (100ms). Submit two commits — both get commit_ts assigned immediately. Second commit does not wait for first replication.
7. **visible_ts_lags_commit_ts**: With slow replication, `visible_ts` is behind `ts_allocator.latest()` until replication completes.
8. **visible_ts_advances_in_order**: Three commits replicated — visible_ts advances 1→2→3 in strict order.

### OCC conflict

9. **commit_occ_conflict**: Two transactions read overlapping ranges, first commits, second conflicts.
10. **commit_occ_conflict_subscribe_retry**: Subscribe mode conflict includes ConflictRetry.
11. **commit_occ_passes_disjoint**: Transactions with disjoint ranges both commit.

### Subscription registration

12. **commit_with_notify**: Notify mode → subscription_id returned, event_rx present.
13. **commit_with_watch**: Watch mode subscription persists across invalidations.
14. **commit_with_subscribe_chain**: Subscribe mode → on invalidation, ChainContinuation delivered.

### Invalidation flow

15. **invalidation_fires_on_overlap**: Commit A registers subscription. Commit B writes into interval → A receives event.
16. **invalidation_respects_limit**: Write beyond LimitBoundary → no invalidation.
17. **extend_for_deltas_before_registration**: Own writes clear stale limit before subscription registration.
18. **invalidation_after_visible_ts**: Subscription invalidation events are only delivered after visible_ts advances (not before replication confirms).

### Catalog (unified)

19. **commit_catalog_write_produces_delta**: Write to CATALOG_COLLECTIONS produces IndexDelta on reserved collection.
20. **commit_catalog_conflict_via_interval**: Concurrent DDL write overlaps catalog read interval → conflict.

### Replication

21. **commit_no_replication**: NoReplication passes through — visible_ts advances immediately.
22. **replication_failure_responds_quorum_lost**: Mock replication fails → client receives QuorumLost.
23. **replication_failure_drains_queue**: Two commits enqueued, first fails replication → both receive QuorumLost.
24. **replication_failure_removes_commit_log**: After failure, commit log entries beyond visible_ts are removed.
25. **replication_failure_stops_writer**: After replication task stops, writer's replication_tx.send() fails — writer stops accepting commits.

### WAL payload

26. **wal_payload_roundtrip**: Serialize and deserialize WAL payload, verify contents match.

### Read-only with subscription

27. **read_only_subscription_validates**: Read-only tx with subscription → OCC runs, subscription registered, no WAL write.
