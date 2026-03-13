# T7: Commit Protocol

## Purpose

Single-writer commit coordinator implementing the 11-step commit protocol from DESIGN.md 5.12. Serializes all writes through one tokio task, orchestrating OCC validation, WAL persistence, mutation application, commit log updates, subscription invalidation, replication, and visibility fence advancement.

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
/// Injected into CommitCoordinator at construction time.
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

/// The single-writer commit coordinator.
///
/// **`!Send`**: Contains parking_lot guards from StorageEngine async
/// methods. Must be spawned on a `tokio::task::LocalSet` or single-threaded
/// runtime. The `CommitHandle` is `Send + Clone` and can be used from any task.
pub struct CommitCoordinator {
    ts_allocator: Arc<TsAllocator>,
    visible_ts: Arc<AtomicU64>,
    commit_log: CommitLog,
    subscriptions: SubscriptionRegistry,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    replication: Box<dyn ReplicationHook>,
    index_resolver: Arc<dyn IndexResolver>,
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
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
    /// Create a new commit coordinator and its handle.
    ///
    /// `initial_ts`: highest committed timestamp from WAL recovery.
    /// `visible_ts`: last WAL_RECORD_VISIBLE_TS from recovery.
    /// `channel_size`: bounded capacity for the commit request channel.
    ///
    /// Returns (coordinator, handle). Coordinator must be `.run()` on a
    /// LocalSet. Handle is distributed to application code.
    pub fn new(
        initial_ts: Ts,
        visible_ts: Ts,
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        index_resolver: Arc<dyn IndexResolver>,
        channel_size: usize,
    ) -> (Self, CommitHandle);
}
```

### CommitHandle methods

```rust
impl CommitHandle {
    /// Submit a commit request and await the result.
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

### The 11-Step Commit Loop

```rust
impl CommitCoordinator {
    /// The single-writer commit loop. Processes one commit at a time.
    ///
    /// Runs until the channel is closed (all handles dropped).
    pub async fn run(&mut self) {
        while let Some((request, response_tx)) = self.commit_rx.recv().await {
            let result = self.process_commit(request).await;
            let _ = response_tx.send(result);
        }
    }

    async fn process_commit(&mut self, req: CommitRequest) -> CommitResult {
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
    // commit_ts_candidate = next timestamp that will be assigned
    let commit_ts_candidate = self.ts_allocator.latest() + 1;
    if let Err(conflict) = validate(&req.read_set, &self.commit_log, req.begin_ts, commit_ts_candidate) {
        // On conflict + Subscribe mode: auto-start retry tx
        let retry = if req.subscription == SubscriptionMode::Subscribe {
            Some(ConflictRetry {
                new_tx_id: self.next_tx_id.fetch_add(1, Ordering::AcqRel),
                new_ts: self.visible_ts.load(Ordering::Acquire),
            })
        } else {
            None
        };
        return CommitResult::Conflict { error: conflict, retry };
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

#### Step 5: Append to Commit Log

```rust
let log_entry = build_commit_log_entry(commit_ts, &index_deltas);
self.commit_log.append(log_entry);
```

#### Step 6: Subscription Invalidation Check

```rust
// Run invalidation check (fast, in-memory)
let invalidation_events = self.subscriptions.check_invalidation(
    commit_ts,
    &self.commit_log.entries_in_range(commit_ts - 1, commit_ts + 1)[0].index_writes,
    || self.next_tx_id.fetch_add(1, Ordering::AcqRel),
);
```

**Note:** Since CommitCoordinator is `!Send`, steps 6 and 7 run sequentially (not via `tokio::spawn`). Invalidation check is fast (in-memory), replication may be slow.

#### Step 7: Await Replication

```rust
if let Err(msg) = self.replication.replicate_and_wait(lsn, &wal_payload).await {
    // Rollback: remove commit log entry
    self.commit_log.remove(commit_ts);
    // Data mutations (including catalog) are in the page store but invisible
    // (visible_ts not advanced). Rollback vacuum (L3) handles cleanup on restart.
    tracing::error!("replication failed, rolling back commit {commit_ts}: {msg}");
    return CommitResult::QuorumLost;
}
```

#### Step 8: WAL visible_ts Record

```rust
self.storage.append_wal(WAL_RECORD_VISIBLE_TS, &commit_ts.to_le_bytes()).await?;
```

#### Step 9: Advance visible_ts

```rust
self.visible_ts.store(commit_ts, Ordering::Release);
```

#### Step 10: Register Subscription (if requested)

```rust
let (subscription_id, event_rx) = if req.subscription != SubscriptionMode::None {
    // 10a: Clear stale limit boundaries from tx's own writes
    let mut read_set = req.read_set;
    read_set.extend_for_deltas(&index_deltas);

    // 10b: Create channel and register
    let (event_tx, event_rx) = mpsc::channel(64);
    let sub_id = self.subscriptions.register(
        req.subscription,
        req.session_id,
        req.tx_id,
        commit_ts,
        read_set,
        event_tx,
    );
    (Some(sub_id), Some(event_rx))
} else {
    (None, None)
};
```

#### Step 11: Push Invalidation Events

```rust
// Fire-and-forget: push events via try_send on each sub's event_tx
self.subscriptions.push_events(invalidation_events);
```

#### Return Success

```rust
CommitResult::Success { commit_ts, subscription_id, event_rx }
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

### Helper: serialize_wal_payload / deserialize_wal_payload

Binary serialization of commit data for WAL. Format documented in Step 3 above. These are private helper functions within commit.rs.

## Design Decisions

### 1. Sequential steps 6+7 (not concurrent)

Since `CommitCoordinator` is `!Send` (parking_lot guards in StorageEngine), we cannot use `tokio::spawn` for concurrent execution. Steps 6 (invalidation check) and 7 (replication) run sequentially. This is acceptable because:
- Invalidation check is fast (in-memory HashMap lookups)
- Replication latency is the bottleneck regardless
- The single-writer model means only one commit is in-flight at a time

### 2. Rollback on replication failure

Initial implementation removes the commit log entry. All mutations (data and catalog, which are now unified) remain in the page store but are invisible because `visible_ts` is not advanced. On restart, rollback vacuum (L3) cleans up versions beyond `visible_ts`. Full reverse-write rollback is a future optimization.

### 3. Channel-based request submission

CommitHandle sends `(CommitRequest, oneshot::Sender<CommitResult>)` via a bounded mpsc channel. The coordinator processes one request at a time. This provides natural backpressure — if the channel is full, callers block on `send`. Default channel capacity: 256.

### 4. Shared state via Arc

`TsAllocator`, `visible_ts`, `next_tx_id` are shared between CommitCoordinator and CommitHandle via `Arc`. The coordinator is the only writer for `visible_ts`; handles only read it.

### 5. Read-only subscription commits

Read-only transactions with `subscription != None` still go through the commit coordinator. OCC validation runs (to ensure the subscription is based on a consistent read), but steps 3–5 (WAL, mutations, commit log) are skipped. Steps 10–11 (subscription registration + invalidation push) still execute.

### 6. Unified catalog handling

Catalog mutations are regular writes on reserved pseudo-collections (CATALOG_COLLECTIONS, CATALOG_INDEXES). L5 has no CatalogMutator trait — it treats catalog writes identically to data writes. L6 detects writes to reserved CollectionIds in the commit result and triggers physical side effects (page allocation for new B-trees, CatalogCache updates) as a post-commit callback. This eliminates ~100 lines of catalog-specific code from L5 (CatalogRead enum, catalog_conflicts function, CatalogMutator trait, special-casing in OCC and subscriptions).

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| `CommitResult::Conflict` | OCC validation fails | Returned to caller. Subscribe mode includes ConflictRetry. |
| `CommitResult::QuorumLost` | Replication failure or coordinator dropped | Commit rolled back, returned to caller. |
| `CommitError::Io` | WAL write or index mutation I/O failure | Propagated. Coordinator remains usable for subsequent commits. |

## Tests

### Basic commit flow

1. **commit_read_only**: Empty write set, no subscription → success, no WAL write.
2. **commit_single_insert**: One insert → success, commit_ts assigned, WAL written.
3. **commit_advances_visible_ts**: After commit, `handle.visible_ts()` == commit_ts.
4. **commit_sequential_timestamps**: Two commits get sequential commit_ts values.
5. **commit_handle_clone**: Multiple handles can submit commits.

### OCC conflict

6. **commit_occ_conflict**: Two transactions read overlapping ranges, first commits, second conflicts.
7. **commit_occ_conflict_subscribe_retry**: Subscribe mode conflict includes ConflictRetry.
8. **commit_occ_passes_disjoint**: Transactions with disjoint ranges both commit.

### Subscription registration

9. **commit_with_notify**: Notify mode → subscription_id returned, event_rx present.
10. **commit_with_watch**: Watch mode subscription persists across invalidations.
11. **commit_with_subscribe_chain**: Subscribe mode → on invalidation, ChainContinuation delivered.

### Invalidation flow

12. **invalidation_fires_on_overlap**: Commit A registers subscription. Commit B writes into interval → A receives event.
13. **invalidation_respects_limit**: Write beyond LimitBoundary → no invalidation.
14. **extend_for_deltas_before_registration**: Own writes clear stale limit before subscription registration.

### Catalog (unified)

15. **commit_catalog_write_produces_delta**: Write to CATALOG_COLLECTIONS produces IndexDelta on reserved collection.
16. **commit_catalog_conflict_via_interval**: Concurrent DDL write overlaps catalog read interval → conflict.

### Replication

17. **commit_no_replication**: NoReplication passes through.
18. **commit_replication_failure**: Mock replication fails → QuorumLost, commit log entry removed.

### WAL payload

19. **wal_payload_roundtrip**: Serialize and deserialize WAL payload, verify contents match.

### Read-only with subscription

20. **read_only_subscription_validates**: Read-only tx with subscription → OCC runs, subscription registered, no WAL write.
