# exdb-tx — Layer 5: Transactions

Timestamp allocation, read/write set management, OCC validation via commit log,
subscription registry with three modes (Notify / Watch / Subscribe), read-set
carry-forward for subscription chains, and the two-task commit architecture.

## Quick Start

### Starting the Commit System

L6 (Database facade) creates and owns the three components. Here is the
minimal wiring:

```rust
use std::sync::Arc;
use std::collections::HashMap;
use exdb_tx::{CommitCoordinator, NoReplication, IndexResolver, IndexInfo};
use exdb_storage::engine::{StorageEngine, StorageConfig};
use parking_lot::RwLock;

// Storage + empty index maps (L6 populates these from the catalog)
let storage = Arc::new(
    StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap()
);
let primaries = Arc::new(RwLock::new(HashMap::new()));
let secondaries = Arc::new(RwLock::new(HashMap::new()));

// IndexResolver — L6 implements this over its CatalogCache
struct MyResolver;
impl IndexResolver for MyResolver {
    fn indexes_for_collection(&self, _coll: exdb_core::types::CollectionId) -> Vec<IndexInfo> {
        vec![] // populated by L6
    }
}

// Create the three components
let (mut coordinator, mut runner, handle) = CommitCoordinator::new(
    0,                              // initial_ts (from WAL recovery)
    0,                              // visible_ts (from WAL recovery)
    storage,
    primaries,
    secondaries,
    Box::new(NoReplication),        // or a real ReplicationHook
    Arc::new(MyResolver),
    256,                            // commit channel capacity
    256,                            // replication queue capacity
);

// Spawn the coordinator on a LocalSet (it is !Send)
let local = tokio::task::LocalSet::new();
local.spawn_local(async move { coordinator.run().await });

// Spawn the replication runner on any tokio task (it is Send)
tokio::spawn(async move { runner.run().await });

// Distribute `handle` to application code — it is Send + Clone
```

### Committing a Transaction

```rust
use exdb_tx::{CommitRequest, CommitResult, ReadSet, WriteSet, SubscriptionMode};
use exdb_core::types::{CollectionId, DocId};
use serde_json::json;

// Build the write set
let mut write_set = WriteSet::new();
write_set.insert(
    CollectionId(10),
    DocId([1; 16]),
    json!({"name": "Alice", "age": 30}),
);

// Submit the commit (awaits until replication confirms visibility)
let result = handle.commit(CommitRequest {
    tx_id: handle.allocate_tx_id(),
    begin_ts: handle.visible_ts(),   // snapshot timestamp
    read_set: ReadSet::new(),        // L6 populates from query execution
    write_set,
    subscription: SubscriptionMode::None,
    session_id: 1,
}).await;

match result {
    CommitResult::Success { commit_ts, .. } => {
        println!("committed at ts={commit_ts}");
    }
    CommitResult::Conflict { error, retry } => {
        println!("OCC conflict: {error}");
        // retry contains new tx info for Subscribe mode
    }
    CommitResult::QuorumLost => {
        println!("replication failure");
    }
}
```

### Recording Read Intervals (L6 does this during query execution)

```rust
use exdb_tx::{ReadSet, ReadInterval, LimitBoundary, QueryId};
use exdb_core::types::{CollectionId, IndexId};
use std::ops::Bound;

let mut read_set = ReadSet::new();
let query_id = read_set.next_query_id();

// Record a range scan on a secondary index
read_set.add_interval(
    CollectionId(10),
    IndexId(5),
    ReadInterval {
        query_id,
        lower: Bound::Included(vec![0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A]),
        upper: Bound::Excluded(vec![0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]),
        // ASC scan with LIMIT 10 stopped at sort key K:
        limit_boundary: Some(LimitBoundary::Upper(vec![/* last returned key */])),
    },
);
```

### OCC Validation (standalone, used internally by T7)

```rust
use exdb_tx::{validate, ReadSet, CommitLog, ReadInterval};
use exdb_core::types::{CollectionId, IndexId};
use std::ops::Bound;

let mut read_set = ReadSet::new();
read_set.add_interval(
    CollectionId(1), IndexId(1),
    ReadInterval {
        query_id: 0,
        lower: Bound::Included(vec![10]),
        upper: Bound::Excluded(vec![20]),
        limit_boundary: None,
    },
);

let commit_log = CommitLog::new();

// Check for conflicts in the danger window (begin_ts=5, commit_ts=15)
match validate(&read_set, &commit_log, 5, 15) {
    Ok(()) => println!("no conflicts"),
    Err(e) => println!("conflict at ts={}: {:?}", e.conflicting_ts, e.affected_query_ids),
}
```

### Subscriptions (Notify / Watch / Subscribe)

```rust
use exdb_tx::{
    CommitRequest, CommitResult, ReadSet, WriteSet,
    SubscriptionMode, InvalidationEvent,
};

// Commit with a Watch subscription — fires on every invalidation
let result = handle.commit(CommitRequest {
    tx_id: handle.allocate_tx_id(),
    begin_ts: handle.visible_ts(),
    read_set,       // intervals to watch
    write_set: WriteSet::new(),
    subscription: SubscriptionMode::Watch,
    session_id: 1,
}).await;

if let CommitResult::Success { event_rx: Some(mut rx), .. } = result {
    // Wait for invalidation events
    while let Some(event) = rx.recv().await {
        println!(
            "invalidated by commit_ts={}, affected queries: {:?}",
            event.commit_ts, event.affected_query_ids
        );
        // For Subscribe mode, event.continuation carries the new tx + read set
    }
}
```

### Catalog Conflicts (DDL)

Catalog operations use reserved pseudo-collections — no special API needed:

```rust
use exdb_tx::{
    ReadSet, ReadInterval, WriteSet, CatalogMutation,
    CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX,
};
use std::ops::Bound;
use exdb_core::types::CollectionId;

// Record a catalog read (e.g., "does collection 'users' exist?")
let mut read_set = ReadSet::new();
read_set.add_interval(
    CATALOG_COLLECTIONS,
    CATALOG_COLLECTIONS_NAME_IDX,
    ReadInterval {
        query_id: 0,
        lower: Bound::Included(b"users".to_vec()),
        upper: Bound::Excluded(b"users\x01".to_vec()),
        limit_boundary: None,
    },
);

// Buffer a catalog write (e.g., CREATE COLLECTION)
let mut write_set = WriteSet::new();
write_set.add_catalog_mutation(CatalogMutation::CreateCollection {
    name: "orders".into(),
    provisional_id: CollectionId(42),
});
```

### Implementing `ReplicationHook` (for replicated deployments)

```rust
use exdb_tx::ReplicationHook;
use exdb_storage::wal::Lsn;

struct RaftReplication { /* raft client */ }

#[async_trait::async_trait]
impl ReplicationHook for RaftReplication {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<(), String> {
        // Send WAL record to replicas, wait for quorum ack
        // self.raft.append_and_wait(lsn, record).await
        Ok(())
    }

    fn has_quorum(&self) -> bool { true }
    fn is_holding(&self) -> bool { false }
}
```

## Architecture

```
T7 (Commit) ──→ T1, T2, T3, T4, T5, T6, L2, L3
T6 (Subscriptions) ──→ T2, T3, T4
T5 (OCC) ──→ T2, T3, T4
T4 (CommitLog) ──→ T3
T3 (WriteSet) ──→ L1, L3
T2 (ReadSet) ──→ L1
T1 (Timestamp) ──→ L1
```

## Modules

| Module | File | Purpose |
|--------|------|---------|
| **T1** | `timestamp.rs` | Monotonic `TsAllocator` — lock-free atomic counter for MVCC timestamps |
| **T2** | `read_set.rs` | `ReadInterval`, `LimitBoundary`, `ReadSet` — tracks observed key intervals |
| **T3** | `write_set.rs` | `WriteSet`, `IndexDelta`, `IndexResolver` trait, `compute_index_deltas` |
| **T4** | `commit_log.rs` | In-memory `CommitLog` indexed by `(collection, index)` for OCC scanning |
| **T5** | `occ.rs` | `validate()` — OCC conflict detection via `ReadInterval::contains_key` |
| **T6** | `subscriptions.rs` | `SubscriptionRegistry` — reactive invalidation with three modes |
| **T7** | `commit.rs` | `CommitCoordinator` (writer) + `ReplicationRunner` + `CommitHandle` |

## Two-Task Commit Protocol

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

### Commit Steps

**Writer task (steps 1–5, `CommitCoordinator`, `!Send`):**

1. **OCC validation** — scan commit log danger window `(begin_ts, commit_ts)`,
   check `ReadInterval::contains_key` on both `old_key` and `new_key`
2. **Allocate `commit_ts`** — `TsAllocator::allocate()`
3. **WAL persist** — `StorageEngine::append_wal(WAL_RECORD_TX_COMMIT, payload)`
4. **Apply mutations** — `PrimaryIndex::insert_version` + `SecondaryIndex::insert_entry/remove_entry`
5. **Commit log append** — `CommitLog::append(entry)`, then enqueue to replication

**Replication task (steps 6–11, `ReplicationRunner`, `Send`):**

6. **Replicate** — `ReplicationHook::replicate_and_wait(lsn, payload)`
7. **WAL visible_ts** — `append_wal(WAL_RECORD_VISIBLE_TS, commit_ts)`
8. **Advance `visible_ts`** — atomic store
9. **Subscription invalidation** — `SubscriptionRegistry::check_invalidation`
10. **Register subscription** — `extend_for_deltas` then `register()`
11. **Push events + respond** — `try_send` to subscriber channels, `oneshot` to client

### Threading Model

| Component | `Send`? | Where to spawn | Why |
|-----------|---------|---------------|-----|
| `CommitCoordinator` | No | `tokio::task::LocalSet` | Holds `parking_lot` page guards across `.await` |
| `ReplicationRunner` | Yes | `tokio::spawn` | Only calls WAL append (channel-based) + sync ops |
| `CommitHandle` | Yes + Clone | Anywhere | Thin mpsc sender + atomic reads |

## Key Design Decisions

1. **Unified catalog conflict detection**: DDL modeled as intervals on reserved
   pseudo-collections (`CATALOG_COLLECTIONS`, `CATALOG_INDEXES`). Same
   `contains_key` handles both data and catalog conflicts.

2. **`visible_ts` ≠ `ts_allocator.latest()`**: `visible_ts` is the safe read
   fence, only advances after replication + WAL persist. Controlled exclusively
   by the replication task.

3. **LimitBoundary tightening**: LIMIT clauses tighten read intervals. OCC
   validates the raw (tightened) interval. `extend_for_deltas` is only called
   before subscription registration (step 10a), not before OCC.

4. **Subscription invalidation after visibility**: Events only fire after
   `visible_ts` advances — clients never see events for potentially-rolled-back
   commits.

5. **Replication failure rollback**: On failure, commit log entries beyond
   `visible_ts` are removed. All pending clients receive `QuorumLost`. Page
   store cleanup is handled by L3's rollback vacuum on restart.

## Layer Boundaries

- **Depends on:** L1 (Core Types), L2 (Storage Engine), L3 (Document Store)
- **No knowledge of:** L6+ (Database, Replication, Wire Protocol)
- L5 defines `IndexResolver` trait — L6 implements it
- L6 owns the user-facing `Transaction` type (`begin`/`commit`/`rollback`)
- L6 bridges L4 (Query) ↔ L5 (Transactions)

### What L6 Does With L5

| L6 Responsibility | L5 Types Used |
|-------------------|---------------|
| `Transaction::begin()` | `CommitHandle::visible_ts()`, `CommitHandle::allocate_tx_id()` |
| Query execution | `ReadSet::add_interval()`, `ReadSet::next_query_id()` |
| Read-your-writes | `WriteSet::get()`, `WriteSet::mutations_for_collection()` |
| `Transaction::commit()` | `CommitHandle::commit(CommitRequest)` |
| `Transaction::rollback()` | Discard `ReadSet`/`WriteSet` (no L5 call needed) |
| Session disconnect | `CommitHandle::subscriptions().write().remove_session(id)` |

## Testing

116 tests across all 7 sub-layers:

| Module | Tests | Key coverage |
|--------|-------|-------------|
| T1 timestamp | 6 | Sequential, concurrent (10K allocations), monotonicity |
| T2 read_set | 26 | `contains_key` with all bound types + limits, `apply_delta`, merge, split |
| T3 write_set | 12 | CRUD, catalog mutations, collection iteration |
| T4 commit_log | 14 | Range queries, prune, remove, remove_after |
| T5 occ | 23 | Conflicts, limits, catalog, K2/K3/K4 correctness example |
| T6 subscriptions | 16 | Notify/Watch/Subscribe modes, chain continuation, session cleanup |
| T7 commit | 19 | End-to-end commits, OCC flow, subscriptions, WAL roundtrip |

Run with:

```sh
cargo test -p exdb-tx
```
