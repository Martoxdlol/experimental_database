# exdb-tx

Layer 5 of the exdb database. Adds serializable transactions on top of the document store.

This crate takes the MVCC building blocks from `exdb-docstore` (which knows how to store
and retrieve versioned documents) and wraps them in a full transaction protocol: timestamp
allocation, read/write set tracking, OCC conflict detection, a commit coordinator, and
live query subscriptions. It is the bridge between "versioned document mutations" and
"ACID-committed transactions".

## What this crate does

Every operation in the database belongs to a transaction. A transaction:

1. **Reads** documents and index ranges — recorded in the **read set**
2. **Writes** mutations (insert / replace / delete) — accumulated in the **write set**
3. **Commits** through a single-writer coordinator that validates, timestamps, persists to WAL,
   materialises index changes, waits for replication, then advances the visible timestamp

The crate provides:

- **Timestamp allocator** — a monotonic `u64` counter shared by all transactions; reads use
  `visible_ts` as their snapshot timestamp, commits allocate the next slot
- **Read set** — intervals on `(collection, index)` key ranges plus catalog observations
  (which collection/index names were resolved), with automatic adjacent-interval merging
- **Write set** — mutations keyed by `(collection, doc_id)`, plus catalog DDL mutations
- **Commit log** — an in-memory ordered log of recent commits used for OCC validation
- **OCC validation** — checks whether any commit between `begin_ts` and `commit_ts`
  wrote a key that falls within this transaction's read intervals
- **Subscription registry** — live query subscriptions (Notify / Watch / Subscribe) that
  receive invalidation events whenever a committed key overlaps their read set
- **Commit coordinator** — a single-writer loop that serialises all commits via an async
  channel, executing an 11-step protocol

## What this crate does NOT do

- Execute queries or filter documents — that is the query engine (L4)
- Manage which collections and indexes exist (the catalog) — that is L6; L5 accepts a
  `CatalogMutator` trait to apply DDL atomically within a commit
- Persist subscription state across restarts — subscriptions are in-memory only
- Implement distributed consensus — it accepts a `ReplicationHook` trait that L7 fills in;
  the coordinator just waits for `replicate_and_wait` to return before marking a commit visible

## How it fits in the stack

```
L6  Database        ← catalog, session management, composes everything
L5  Transactions    ← this crate
L4  Query Engine    ← plans and executes queries
L3  Document Store  ← versioned documents, secondary indexes, vacuum
L2  Storage Engine  ← raw B-trees, heap, WAL, buffer pool
L1  Core Types      ← DocId, Scalar, FieldPath, encoding
```

L5 depends on L1, L2 (WAL append), and L3 (primary + secondary index writes).
L6 drives L5 by creating a `CommitCoordinator` and dispatching commit requests through it.

## Usage

### Creating a coordinator

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use exdb_storage::engine::{StorageEngine, StorageConfig};
use exdb_tx::{CommitCoordinator, CommitHandle};
use exdb_tx::commit::{NoReplication, NoopCatalogMutator};
use exdb_tx::write_set::EmptyIndexResolver;

let engine = Arc::new(
    StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap()
);

let (mut coordinator, handle) = CommitCoordinator::new(
    0,                                      // initial_ts
    0,                                      // visible_ts_initial
    engine.clone(),
    Arc::new(RwLock::new(HashMap::new())),  // primary indexes
    Arc::new(RwLock::new(HashMap::new())),  // secondary indexes
    Box::new(NoReplication),                // replication hook
    Arc::new(NoopCatalogMutator),           // catalog mutator
    Arc::new(EmptyIndexResolver),           // index resolver
    256,                                    // commit channel capacity
);

// Drive the commit loop in a dedicated task
// (CommitCoordinator is !Send — use spawn_local or drive directly in tests)
tokio::task::spawn_local(async move {
    coordinator.run().await;
});
```

### Submitting a commit

```rust
use exdb_tx::{CommitRequest, CommitResult, WriteSet};
use exdb_tx::subscriptions::SubscriptionMode;
use exdb_core::types::{CollectionId, DocId};

let mut ws = WriteSet::new();
ws.insert(CollectionId(1), doc_id, serde_json::json!({"name": "Alice", "age": 30}));

let request = CommitRequest {
    tx_id: 1,
    begin_ts: handle.visible_ts(),
    read_set: read_set,         // intervals scanned during the transaction
    write_set: ws,
    subscription: SubscriptionMode::None,
    session_id: 0,
};

match handle.commit(request).await {
    CommitResult::Success { commit_ts, .. } => {
        println!("committed at ts={commit_ts}");
    }
    CommitResult::Conflict { error, retry } => {
        // OCC conflict — retry from a fresh snapshot if retry.is_some()
    }
    CommitResult::QuorumLost => {
        // WAL write or replication failed — transaction is not durable
    }
}
```

### Read sets and OCC

The read set records every key range the transaction observed. If any concurrent commit
writes a key that falls inside one of those ranges between `begin_ts` and the candidate
`commit_ts`, the commit is rejected.

```rust
use exdb_tx::{ReadSet, ReadInterval};
use exdb_core::types::{CollectionId, IndexId};
use std::ops::Bound;

let mut rs = ReadSet::new();
let query_id = rs.next_query_id();
rs.add_interval(
    CollectionId(1),
    IndexId(10),
    ReadInterval {
        query_id,
        lower: vec![/* encoded lower bound */],
        upper: Bound::Excluded(vec![/* encoded upper bound */]),
    },
);
```

Overlapping or adjacent intervals on the same `(collection, index)` pair are merged
automatically, keeping the minimum `query_id`. For **Subscribe** mode, non-adjacent
intervals are important — they let the continuation carry only the intervals from
before the conflict point.

### Subscriptions

After a successful commit, the transaction's read set can be registered as a live query
subscription. The subscription registry sends `InvalidationEvent`s whenever a subsequent
commit writes into the registered intervals.

```rust
use exdb_tx::subscriptions::SubscriptionMode;
use exdb_tx::{CommitRequest, CommitResult};

// Request a Watch subscription (invalidated once, then re-evaluated)
let req = CommitRequest {
    subscription: SubscriptionMode::Watch,
    ..
};

if let CommitResult::Success { subscription_id: Some(id), event_rx: Some(mut rx), .. } =
    handle.commit(req).await
{
    // Wait for an invalidation
    if let Some(event) = rx.recv().await {
        println!("query {id} invalidated at ts={}", event.commit_ts);
    }
}
```

Three modes:

| Mode | Behaviour |
|------|-----------|
| `Notify` | One-shot: fires once and auto-unregisters |
| `Watch` | Persistent: fires on every overlapping commit |
| `Subscribe` | Chain: fires with a `ChainContinuation` carrying the unaffected prefix of the read set, ready for the next transaction to pick up |

### Catalog mutations (DDL)

DDL operations (create/drop collection or index) are treated as mutations within a
transaction's write set and committed atomically with data mutations:

```rust
use exdb_tx::write_set::CatalogMutation;
use exdb_core::types::CollectionId;

ws.add_catalog_mutation(CatalogMutation::CreateCollection {
    name: "orders".into(),
    provisional_id: CollectionId(42),
});
```

The `CatalogMutator` trait is called during commit to apply or roll back DDL. Catalog
reads (e.g. "I looked up collection 'orders' by name") are recorded in the read set to
detect DDL-vs-DDL conflicts via OCC.

## The 11-step commit protocol

```
1.  VALIDATE      — OCC: check read set against commit log entries since begin_ts
2.  TIMESTAMP     — allocate commit_ts via ts_allocator.allocate()
3.  PERSIST       — append commit record to WAL
4a. CATALOG       — apply catalog mutations via CatalogMutator
4b. MATERIALISE   — compute index deltas; write primary + secondary index entries
5.  LOG           — append CommitLogEntry (index key writes) to commit log
6.  CONCURRENT    — tokio::join!(replicate_and_wait, check_invalidation)
7.  AWAIT REPL    — if replication fails: rollback (tombstones + secondary removals + catalog rollback + log removal)
8.  VISIBLE WAL   — append VISIBLE_TS record to WAL
9.  ADVANCE       — store commit_ts into visible_ts (atomic)
10. SUBSCRIBE     — register subscription if requested; send CommitResult::Success
11. PUSH          — push invalidation events to registered subscriptions
```

Steps 6a (replication) and 6b (invalidation check) run concurrently via `tokio::join!`.
Only after both complete does the protocol proceed to step 7.

## Module map

```
tx/
  timestamp.rs      ← TsAllocator (atomic monotonic counter)
  read_set.rs       ← ReadSet, ReadInterval, CatalogRead
  write_set.rs      ← WriteSet, MutationEntry, CatalogMutation, IndexDelta, IndexResolver
  commit_log.rs     ← CommitLog, CommitLogEntry, IndexKeyWrite
  occ.rs            ← validate(), catalog_conflicts(), ConflictError
  subscriptions.rs  ← SubscriptionRegistry, InvalidationEvent, ChainContinuation
  commit.rs         ← CommitCoordinator, CommitHandle, ReplicationHook, CatalogMutator
```

Dependencies flow strictly downward within the crate:
`commit → occ + subscriptions + write_set + commit_log + timestamp`.
No circular dependencies.

## Important implementation notes

- **`CommitCoordinator` is `!Send`** — the storage layer uses `parking_lot` mutexes
  internally; their guards are `!Send` and can appear inside async fn bodies. The
  coordinator must be driven on a single-threaded tokio runtime or via `spawn_local`.
- **`StorageEngine` path** — `exdb_storage::engine::StorageEngine` (not `exdb_storage::StorageEngine`)
- **`StorageEngine::open_in_memory`** takes a `StorageConfig` argument
- **`engine.create_btree()`** takes no arguments
- **Adjacent intervals merge** — `add_interval` merges overlapping or adjacent intervals;
  Subscribe-mode tests must use non-adjacent intervals to preserve distinct `query_id`s

## Tests

```
cargo test -p exdb-tx
```

122 tests covering: timestamp monotonicity + concurrent allocation, interval merging and
splitting, write set mutation ops, index delta computation (insert / replace / delete /
array fields), commit log range queries and pruning, OCC conflict and non-conflict
scenarios, subscription invalidation for all three modes, chain continuation carry-forward,
commit coordinator success / OCC conflict / subscribe retry / quorum rollback /
invalidation firing / visible-ts fencing / graceful shutdown, and WAL serialisation
round-trips.
