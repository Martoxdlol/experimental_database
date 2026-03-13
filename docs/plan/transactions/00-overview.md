# Transaction Manager Implementation Plan — Overview

## Scope

Layer 5: Transactions. Timestamp allocation, read/write set management, OCC validation via commit log, subscription registry with three modes (Notify / Watch / Subscribe), read set carry-forward for subscription chains, and the single-writer commit protocol. This is the concurrency and consistency layer.

**Depends on:** Layer 1 (Core Types), Layer 2 (Storage Engine), Layer 3 (Document Store).
**No knowledge of:** Layer 6+ (Database, Replication, Wire Protocol).
**Design authority:** DESIGN.md sections 5.1–5.12, `docs/plan/overview/05-layer5-transactions.md`.

## Sub-Layer Organization (Bottom-Up Build Order)

| # | Sub-Layer | File | Dependencies | Testable Alone? |
|---|-----------|------|-------------|-----------------|
| T1 | Timestamp Allocator | `timestamp.rs` | L1 (types) | Yes |
| T2 | Read Set | `read_set.rs` | L1 (types) | Yes |
| T3 | Write Set | `write_set.rs` | L1 (types, encoding, field_path), L3 (PrimaryIndex, compute_index_entries, key_encoding) | Yes (with mocks) |
| T4 | Commit Log | `commit_log.rs` | T3 (IndexDelta type) | Yes |
| T5 | OCC Validation | `occ.rs` | T2, T4 | Yes |
| T6 | Subscriptions | `subscriptions.rs` | T2, T3, T4 | Yes |
| T7 | Commit Protocol | `commit.rs` | T1–T6, L2 (StorageEngine), L3 (PrimaryIndex, SecondaryIndex) | Yes (with mocks) |

## Implementation Phases

### Phase A: Foundations (T1 + T2 + T3) — Parallel, No Internal Dependencies
Build and fully test timestamp allocation, read set management (including LimitBoundary), and write set management independently. T1 and T2 are pure data structures. T3 defines IndexDelta and IndexResolver trait.

### Phase B: Commit Log (T4) — Depends on Phase A
Build the in-memory commit log that indexes writes by (collection, index) for OCC scanning. Depends on T3's IndexDelta type.

### Phase C: OCC + Subscriptions (T5 + T6) — Parallel, Depend on Phases A + B
OCC validation and subscription invalidation both use ReadInterval::contains_key as the core overlap check. Build and test in parallel.

### Phase D: Commit Protocol (T7) — Depends on Phases A–C
The single-writer commit coordinator orchestrating the 11-step protocol. Integrates all prior modules with L2 (WAL) and L3 (PrimaryIndex, SecondaryIndex).

## File Map

```
tx/
  lib.rs              — public facade, re-exports
  timestamp.rs        — T1: monotonic timestamp allocator
  read_set.rs         — T2: ReadInterval, LimitBoundary, ReadSet + reserved catalog constants
  write_set.rs        — T3: WriteSet, MutationEntry, IndexDelta, IndexResolver trait
  commit_log.rs       — T4: CommitLog, CommitLogEntry, IndexKeyWrite
  occ.rs              — T5: OCC validation, ConflictError
  subscriptions.rs    — T6: SubscriptionRegistry, InvalidationEvent, ChainContinuation
  commit.rs           — T7: CommitCoordinator, CommitHandle, 11-step protocol
```

## Dependency Direction

```
T7 (Commit) ──→ T1, T2, T3, T4, T5, T6, L2, L3
T6 (Subscriptions) ──→ T2, T3, T4, T5
T5 (OCC) ──→ T2, T3, T4
T4 (CommitLog) ──→ T3
T3 (WriteSet) ──→ L1, L3
T2 (ReadSet) ──→ L1
T1 (Timestamp) ──→ L1
```

No circular dependencies. Each sub-layer depends only on sub-layers below it and on L1/L2/L3.

## Crate Configuration

```toml
[package]
name = "exdb-tx"
version.workspace = true
edition.workspace = true

[dependencies]
exdb-core     = { workspace = true }
exdb-storage  = { workspace = true }
exdb-docstore = { workspace = true }
tokio         = { workspace = true }
serde         = { workspace = true }
serde_json    = { workspace = true }
thiserror     = { workspace = true }
tracing       = { workspace = true }
parking_lot   = { workspace = true }
async-trait   = { workspace = true }

[dev-dependencies]
tokio    = { version = "1", features = ["full", "test-util"] }
tempfile = { workspace = true }
```

Does NOT depend on `exdb-query`. L5 defines `IndexResolver` trait that L6 implements. L6 bridges L4 and L5.

## Public Facade

```rust
// Timestamp
pub use timestamp::TsAllocator;

// Read set (used by L4 indirectly via L6, L5 internal, L6 transaction API)
pub use read_set::{
    QueryId, LimitBoundary, ReadInterval, ReadSet,
    CATALOG_COLLECTIONS, CATALOG_INDEXES,
    CATALOG_COLLECTIONS_NAME_IDX, CATALOG_INDEXES_NAME_IDX,
};

// Write set (used by L4 read-your-writes via L6, L5 commit, L6 DDL)
pub use write_set::{
    WriteSet, MutationOp, MutationEntry,
    IndexDelta, IndexInfo, IndexResolver, compute_index_deltas,
};

// Commit log (L5 internal)
pub use commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite, PRIMARY_INDEX_SENTINEL};

// OCC (L5 internal, L6 error handling)
pub use occ::{validate, ConflictError, ConflictKind};

// Subscriptions (L5 internal, L6 subscription API, L8 push events)
pub use subscriptions::{
    SubscriptionId, SubscriptionMode, SubscriptionRegistry,
    SubscriptionMeta, SubscriptionInterval,
    InvalidationEvent, ChainContinuation,
};

// Commit protocol (L6 owns CommitCoordinator, distributes CommitHandle)
pub use commit::{
    CommitCoordinator, CommitHandle, CommitRequest, CommitResult, CommitError,
    ConflictRetry, ReplicationHook, NoReplication,
};
```

## Key Correctness Properties

1. **`ReadInterval::contains_key` is the foundation**: Used by BOTH OCC validation (T5) AND subscription invalidation (T6). Must correctly check the effective interval: original bounds AND limit_boundary tightening.

2. **`apply_delta` is conservative**: Clearing `limit_boundary` only widens the interval — never causes false negatives for conflicts. Called at commit step 10a ONLY (not before OCC).

3. **OCC does NOT call `extend_for_deltas`**: OCC validates the raw read set with LimitBoundary intact. `extend_for_deltas` is only called at step 10a before subscription registration.

4. **Unified catalog conflict detection**: Catalog operations (DDL) are modeled as regular read/write intervals on reserved pseudo-collections (`CATALOG_COLLECTIONS`, `CATALOG_INDEXES`). No separate `CatalogRead`/`catalog_conflicts` — the same `contains_key` check handles both data and catalog conflicts. L6 provides a thin wrapper that encodes catalog operations into intervals. This means catalog subscriptions (e.g., "notify me when a new collection is created") work automatically.

5. **Visibility fence (`visible_ts`)**: Separate from `ts_allocator.latest()`. Only advances after replication confirms + WAL persists `WAL_RECORD_VISIBLE_TS`.

6. **CommitCoordinator is `!Send`**: parking_lot guards inside StorageEngine async methods. Must run on `LocalSet` or single-threaded runtime. CommitHandle is `Send + Clone`.

## L5/L6 Boundary — Transaction Lifecycle

L5 provides the commit machinery (`CommitCoordinator`, `CommitHandle`, `ReadSet`, `WriteSet`). The **user-facing `Transaction` type** with `begin()`, `reset()`, `rollback()`, and `drop()` semantics (DESIGN.md 5.3) lives in **L6** (Database facade). L6 constructs a `Transaction` holding a `ReadSet`, `WriteSet`, `begin_ts`, and a `CommitHandle` reference. On commit, L6 packages these into a `CommitRequest` and submits to `CommitHandle::commit()`.

- **`begin(options)`**: L6 reads `visible_ts` from `CommitHandle`, allocates a `TxId`, creates empty `ReadSet`/`WriteSet`.
- **`reset()`**: L6 clears `ReadSet`/`WriteSet`, resets `next_query_id` to 0. Same `begin_ts`.
- **`rollback()` / `drop(tx)`**: L6 discards `ReadSet`/`WriteSet`. No L5 interaction needed (no commit request sent).
- **`commit()`**: L6 submits `CommitRequest` to `CommitHandle::commit()`. Returns `CommitResult`.

## L3 APIs Consumed

| Function | File | Used By |
|----------|------|---------|
| `PrimaryIndex::insert_version(doc_id, ts, body)` | `primary_index.rs:74` | T7 commit step 4b |
| `PrimaryIndex::get_at_ts(doc_id, ts)` | `primary_index.rs:122` | T3 `compute_index_deltas` |
| `SecondaryIndex::insert_entry(key)` | `secondary_index.rs:39` | T7 commit step 4b |
| `SecondaryIndex::remove_entry(key)` | `secondary_index.rs:44` | T7 commit step 4b (delete old keys) |
| `compute_index_entries(doc, fields)` | `array_indexing.rs:17` | T3 `compute_index_deltas` |
| `make_secondary_key_from_prefix(prefix, doc_id, ts)` | `key_encoding.rs` | T3 `compute_index_deltas` |
| `make_primary_key(doc_id, ts)` | `key_encoding.rs` | T2 point-read intervals |
| `successor_key(key)` | `key_encoding.rs` | T2 exclusive upper bounds |

## L2 APIs Consumed

| Function | File | Used By |
|----------|------|---------|
| `StorageEngine::append_wal(type, payload) -> Lsn` | `engine.rs:529` | T7 steps 3, 8 |
| `WAL_RECORD_TX_COMMIT (0x01)` | `wal.rs:34` | T7 step 3 |
| `WAL_RECORD_VISIBLE_TS (0x09)` | `wal.rs:50` | T7 step 8 |
