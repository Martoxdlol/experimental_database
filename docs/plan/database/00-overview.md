# Database Instance Implementation Plan — Overview

## Scope

Layer 6: Database Instance. Composes layers 1–5 into a fully functional embedded database. Owns lifecycle (open/close), collection and index management, the user-facing transaction API, catalog caching, and implements the L5-defined trait interfaces (`IndexResolver`). This is the primary public API — fully functional without networking.

**Depends on:** Layer 1 (Core Types), Layer 2 (Storage Engine), Layer 3 (Document Store), Layer 4 (Query Engine), Layer 5 (Transactions).
**No knowledge of:** Layer 7+ (Replication, Wire Protocol).
**Design authority:** DESIGN.md sections 1.1–1.12, 5.1–5.12, `docs/plan/overview/06-layer6-database.md`.

## Sub-Layer Organization (Bottom-Up Build Order)

| # | Sub-Layer | File | Dependencies | Testable Alone? |
|---|-----------|------|-------------|-----------------|
| B1 | Config | `config.rs` | L1 (types) | Yes |
| B2 | Catalog Cache | `catalog_cache.rs` | L1 (types) | Yes |
| B3 | Index Resolver | `index_resolver.rs` | B2, L5 (`IndexResolver` trait) | Yes |
| B4 | Catalog Tracker | `catalog_tracker.rs` | B2, L5 (ReadSet, WriteSet) | Yes |
| B5 | Transaction | `transaction.rs` | B2, B3, B4, L4, L5 | Yes (with mocks) |
| B6 | Database | `database.rs` | B1–B5, L2, L3, L4, L5 | Yes (in-memory) |
| B7 | Subscription Handle | `subscription.rs` | L5 (SubscriptionRegistry) | Yes |
| B8 | System Database | `system_database.rs` | B6 | Yes |

**B6 is the integration point** — it creates and wires all components. B5 (Transaction) is the primary user-facing type with the richest API surface.

## Implementation Phases

### Phase A: Foundations (B1 + B2 + B3) — Pure Data Structures, No I/O

Build and fully test configuration, catalog cache (dual-indexed by name and ID), and the `IndexResolver` implementation. These are in-memory data structures with no storage dependencies.

### Phase B: Catalog Tracking + Transaction Core (B4 + B5) — Depends on Phase A

Build the catalog read tracker (encodes DDL operations into intervals on pseudo-collections) and the unified `Transaction` type. B5 is the largest module — it composes L4 (query execution) and L5 (read/write sets) into a coherent transaction API with read-your-writes, catalog name resolution, and `LimitBoundary` computation.

### Phase C: Database + Subscription Handle (B6 + B7) — Depends on Phases A + B

Build the `Database` struct that owns all components, handles lifecycle (open/close), spawns background tasks, and implements `begin()`. Build the `SubscriptionHandle` RAII wrapper. Integration tests exercise the full stack with an in-memory storage engine.

### Phase D: System Database (B8) — Depends on Phase C

Build the multi-database registry. This is optional for the embedded use case but required for the server (L8).

## File Map

```
database/
  lib.rs                — public facade, re-exports
  config.rs             — B1: DatabaseConfig, TransactionConfig
  catalog_cache.rs      — B2: dual-indexed catalog (by-name, by-id)
  index_resolver.rs     — B3: IndexResolver implementation over CatalogCache
  catalog_tracker.rs    — B4: catalog read/write encoding into pseudo-collections
  transaction.rs        — B5: unified Transaction<'db> with reads, writes, DDL
  database.rs           — B6: Database struct, open/close, begin, background tasks
  subscription.rs       — B7: SubscriptionHandle RAII wrapper
  system_database.rs    — B8: multi-database registry
  error.rs              — shared error types
```

## Dependency Direction

```
B8 (SystemDatabase) ──→ B6
B7 (SubscriptionHandle) ──→ L5 (SubscriptionRegistry)
B6 (Database) ──→ B1, B2, B3, B4, B5, B7, L2, L3, L5
B5 (Transaction) ──→ B2, B3, B4, L4, L5
B4 (CatalogTracker) ──→ B2, L5 (ReadSet, WriteSet)
B3 (IndexResolver) ──→ B2, L5 (IndexResolver trait)
B2 (CatalogCache) ──→ L1
B1 (Config) ──→ L1
```

No circular dependencies. Each sub-layer depends only on sub-layers below it and on L1–L5.

## Crate Configuration

```toml
[package]
name = "exdb"
version.workspace = true
edition.workspace = true

[dependencies]
exdb-core     = { workspace = true }
exdb-storage  = { workspace = true }
exdb-docstore = { workspace = true }
exdb-query    = { workspace = true }
exdb-tx       = { workspace = true }
tokio         = { workspace = true }
serde         = { workspace = true }
serde_json    = { workspace = true }
thiserror     = { workspace = true }
tracing       = { workspace = true }
parking_lot   = { workspace = true }

[dev-dependencies]
tokio    = { version = "1", features = ["full", "test-util"] }
tempfile = { workspace = true }
```

This is the **first layer that depends on both L4 (Query) and L5 (Transactions)**. L6 bridges them — L4 produces scan results and `ReadIntervalInfo`, L5 provides `ReadSet`/`WriteSet`/`CommitHandle`, and L6 wires them together inside `Transaction`.

## Public Facade

```rust
// Config
pub use config::{DatabaseConfig, TransactionConfig};

// Catalog
pub use catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

// Transaction API
pub use transaction::{Transaction, TransactionOptions};

// Database lifecycle
pub use database::Database;

// Subscription
pub use subscription::SubscriptionHandle;

// System database
pub use system_database::{SystemDatabase, DatabaseMeta, DatabaseState};

// Errors
pub use error::{DatabaseError, TransactionError};

// Re-exports from lower layers (convenience)
pub use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
pub use exdb_core::field_path::FieldPath;
pub use exdb_core::filter::{Filter, RangeExpr};
pub use transaction::TransactionResult;
pub use exdb_tx::{
    SubscriptionMode, InvalidationEvent, ChainContinuation,
    ConflictRetry, ReplicationHook, NoReplication,
};
pub use exdb_storage::btree::ScanDirection;
```

## Key Design Decisions

### 1. Unified Transaction Entry Point

A single `db.begin(opts)` creates both read-only and read-write transactions (DESIGN.md 5.1). The `TransactionOptions.readonly` flag controls whether mutations are allowed. This avoids API duplication and makes the subscription flow uniform — both read-only and read-write transactions can register subscriptions.

```rust
let mut tx = db.begin(TransactionOptions::default())?;     // read-write
let mut tx = db.begin(TransactionOptions::readonly())?;     // read-only
// L8 sets session_id for subscription cleanup on disconnect:
let mut tx = db.begin(TransactionOptions {
    session_id: session.id(),
    ..TransactionOptions::default()
})?;
```

### 2. Transaction Borrows Database

`Transaction<'db>` holds a `&'db Database` reference. This enables:
- Zero-cost access to the catalog cache, storage engine, and commit handle
- Compile-time prevention of using a transaction after the database is closed
- Automatic cleanup via `Drop` (implicit rollback)

`Transaction<'db>` is `Send` — all its fields are `Send` (`&Database` is `Send` because `Database: Sync`, and `ReadSet`/`WriteSet` are plain data). This means futures holding a transaction work naturally on tokio's multi-threaded runtime. The `'db` lifetime prevents `tokio::spawn` (which requires `'static`), which is the correct constraint — a transaction must not outlive the database. Normal server use (hold a transaction within a request handler, `.await` queries and commit) works seamlessly without `spawn_local`.

### 3. Read-Your-Writes in Query

When `tx.query()` is called on a read-write transaction, L6 builds a `MergeView` from the current `WriteSet` and passes it to L4's `merge_with_writes`. This handles:
- Newly inserted documents appearing in scan results
- Deleted documents being excluded
- Replaced documents showing updated values
- The merged result affecting `LimitBoundary` computation (critical for OCC correctness — see DESIGN.md 5.7 example)

For `tx.get()`, L6 checks the write set first (O(1) BTreeMap lookup) and short-circuits if found.

### 4. Catalog Name Resolution with Write-Set Awareness

When a transaction creates a collection and then inserts into it, the name resolution must find the internal `CollectionId` from the write set's pending `CatalogMutation`. The resolution order is:
1. Check `WriteSet::resolve_pending_collection(name)` — returns internal ID if pending
2. Check `CatalogCache::get_collection_by_name(name)` — returns committed ID
3. Error: collection not found

IDs are allocated internally — the user always references collections and indexes by name. `create_collection` and `create_index` return `()`, not IDs. This keeps the API clean and avoids leaking internal identifiers.

### 5. Catalog Reads as Pseudo-Collection Intervals

Every catalog lookup (resolve collection name, list indexes, etc.) is recorded as a `ReadInterval` on the reserved pseudo-collections (`CATALOG_COLLECTIONS`, `CATALOG_INDEXES`). This gives unified OCC conflict detection for both data and DDL operations — no separate catalog conflict path needed. The `CatalogTracker` module (B4) encapsulates this encoding.

### 6. LimitBoundary Computation Lives in L6

L4 returns a `ReadIntervalInfo` (plan bounds, no query_id, no limit boundary). L6:
1. Assigns a `query_id` from the `ReadSet`
2. Calls `merge_with_writes` (for read-write transactions)
3. Drains the result stream
4. If the limit was hit, extracts the last row's sort key and builds a `LimitBoundary`
5. Constructs a `ReadInterval` with all three components
6. Records it in the `ReadSet`

This keeps L4 stateless across queries and keeps L5 unaware of query mechanics.

### 7. Async Transaction Methods

`get()`, `query()`, `insert()`, `replace()`, `patch()`, `delete()`, and `commit()` are all `async fn`. Even operations that are conceptually synchronous (like buffering a write) may need async I/O:
- `patch()` reads the current document version (async primary index I/O)
- `get()` may fall through to primary index I/O
- `query()` drives the L4 scan pipeline (async I/O throughout)
- `commit()` awaits the commit result from the replication task

`rollback()` and `reset()` are synchronous — they only discard in-memory state.

### 8. Background Tasks Owned by Database

The `Database` struct spawns and owns:
- **CommitCoordinator** — on a `LocalSet` (it is `!Send`)
- **ReplicationRunner** — on a regular tokio task
- **Checkpoint task** — periodic, submits checkpoint through storage engine
- **Vacuum task** — periodic, submits vacuum mutations through the commit channel
- **Index builder task** — builds newly-created secondary indexes in the background

All tasks are cancelled on `Database::close()` via a shared `CancellationToken`.

### 9. `_meta` Handling Boundary

L6 is responsible for `_meta` processing on document ingestion (DESIGN.md 1.12):
- **`_meta.unset`**: processed by `patch()` to remove fields (DESIGN.md 1.12.1).
- **`_meta.types`**: NOT processed by L6. Type disambiguation (`int64` vs `float64`, `bytes` vs `string`, `id` vs `string`) is a wire-format concern handled by L8. By the time documents reach L6, they are `serde_json::Value` with types already resolved. L6 strips any remaining `_meta` field before persisting.
- **`_meta` in responses**: L6 does not add `_meta` to returned documents. L8 may add it for wire-format metadata.

### 10. `_created_at` Auto-Index

Every collection automatically has a `_created_at` secondary index (DESIGN.md 1.11). `create_collection()` buffers both `CreateCollection` and `CreateIndex` for `_created_at` in a single transaction. The `_created_at` and `_id` (primary) indexes cannot be dropped — `drop_index()` rejects attempts with `SystemIndex` error.

## L4/L5/L6 Integration Summary

| L6 Operation | L4 Used | L5 Used |
|-------------|---------|---------|
| `tx.get(coll, id)` | `execute_scan(PrimaryGet)` | `ReadSet::add_interval`, `WriteSet::get` |
| `tx.query(coll, idx, range, filter, dir, limit)` | `resolve_access` → `execute_scan` → `merge_with_writes` | `ReadSet::add_interval`, `WriteSet::mutations_for_collection` |
| `tx.insert(coll, doc)` | — | `WriteSet::insert` |
| `tx.replace(coll, id, doc)` | — | `WriteSet::replace` |
| `tx.patch(coll, id, patch)` | — | `WriteSet::replace` (after patch resolution) |
| `tx.delete(coll, id)` | — | `WriteSet::delete` |
| `tx.create_collection(name)` | — | `WriteSet::add_catalog_mutation` (x2: collection + `_created_at` index) |
| `tx.drop_collection(name)` | — | `WriteSet::add_catalog_mutation` (cascade: collection + all indexes) |
| `tx.create_index(coll, name, fields)` | — | `WriteSet::add_catalog_mutation` |
| `tx.drop_index(coll, name)` | — | `WriteSet::add_catalog_mutation` |
| `tx.commit()` | — | `CommitHandle::commit(CommitRequest)` |
| `tx.list_collections()` | — | `ReadSet::add_interval` (catalog pseudo-coll) |
| `tx.list_indexes(coll)` | — | `ReadSet::add_interval` (catalog pseudo-coll) |

## Startup Sequence

```
1. StorageEngine::open() — DWB restore + WAL replay
2. Scan catalog B-tree → build CatalogCache
3. Open primary + secondary index B-tree handles
4. Rollback vacuum: if committed_ts > visible_ts, clean up un-replicated entries
5. Create CommitCoordinator + ReplicationRunner + CommitHandle
6. Spawn CommitCoordinator on LocalSet (!Send)
7. Spawn ReplicationRunner on tokio task (Send)
8. Start checkpoint background task
9. Start vacuum background task
10. Ready
```

## Shutdown Sequence

```
1. Signal CancellationToken → all background tasks
2. Wait for active transactions (with timeout)
3. Stop background tasks (vacuum, checkpoint, index builds)
4. Final checkpoint
5. Close StorageEngine (flush buffer pool)
6. Done
```

## Error Types

```rust
/// Top-level database errors.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),
    #[error("document not found")]
    DocNotFound,
    #[error("readonly transaction cannot write")]
    ReadonlyWrite,
    #[error("read limit exceeded: {0}")]
    ReadLimitExceeded(String),
    #[error("document too large: {size} bytes (max {max})")]
    DocTooLarge { size: usize, max: usize },
    #[error("collection dropped in this transaction")]
    CollectionDropped,
    #[error("index not ready (still building)")]
    IndexNotReady,
    #[error("invalid field path: {0}")]
    InvalidFieldPath(String),
    #[error("range error: {0}")]
    Range(#[from] exdb_query::RangeError),
    #[error("commit error: {0}")]
    Commit(String),
}

/// Alias for Result<T, DatabaseError>.
pub type Result<T> = std::result::Result<T, DatabaseError>;
```

## Embedded Usage Examples

```rust
use exdb::{Database, DatabaseConfig, TransactionOptions, SubscriptionMode};
use exdb::ScanDirection;
use serde_json::json;

// ── Open (file-backed, durable) ──
let db = Database::open("./mydata", DatabaseConfig::default(), None).await?;

// ── Create collection + insert in one atomic transaction ──
let mut tx = db.begin(TransactionOptions::default())?;
tx.create_collection("users").await?;
let alice_id = tx.insert("users", json!({
    "name": "Alice", "email": "alice@example.com", "age": 30
})).await?;

// Read-your-writes: get the document we just inserted
let alice = tx.get("users", &alice_id).await?;
assert!(alice.is_some());

tx.commit().await?;

// ── Read-only query ──
let mut tx = db.begin(TransactionOptions::readonly())?;
let young_users = tx.query(
    "users",                              // collection
    "_created_at",                        // index (table scan)
    &[],                                  // no range restrictions
    Some(&Filter::Lt(                     // post-filter: age < 40
        FieldPath::single("age"),
        Scalar::Int64(40),
    )),
    Some(ScanDirection::Asc),             // oldest first
    Some(10),                             // limit
).await?;
tx.rollback();  // or just drop

// ── DDL in separate transaction ──
let mut tx = db.begin(TransactionOptions::default())?;
tx.create_index(
    "users", "by_email",
    vec![FieldPath::single("email")],
).await?;
tx.commit().await?;

// ── Subscribe to live changes ──
let mut tx = db.begin(TransactionOptions {
    readonly: true,
    subscription: SubscriptionMode::Watch,
})?;
let users = tx.query("users", "_created_at", &[], None, None, Some(50)).await?;
let result = tx.commit().await?;
if let TransactionResult::Success { subscription_handle, .. } = result {
    // Every future commit touching the watched interval fires an event
    let mut handle = subscription_handle.unwrap();
    while let Some(event) = handle.next_event().await {
        println!("changed: queries {:?}", event.affected_query_ids);
    }
}

db.close().await?;
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `Database::open/close` | L7 (replication), L8 (server) | Lifecycle |
| `Database::begin(TransactionOptions)` | L8 (session) | Unified transaction entry point |
| `Transaction::get/query` | L8 (session) | Document reads |
| `Transaction::insert/replace/patch/delete` | L8 (session) | Document mutations |
| `Transaction::create/drop_collection` | L8 (session) | Transactional DDL |
| `Transaction::create/drop_index` | L8 (session) | Transactional DDL |
| `Transaction::commit()` | L8 (session) | Commit with subscription mode |
| `Database::subscriptions()` | L8 (push notifications) | Subscription access |
| `Database::storage()` | L7 (WAL streaming) | Storage access for replication |
| `SystemDatabase` | L8 (server) | Multi-database management |
| `SubscriptionHandle` | L8 (push to client) | RAII subscription wrapper |
| `CommitResult`, `ConflictRetry` | L8 (session response) | Success/conflict reporting |
| `ReplicationHook` trait | L7 (implements) | Replication callback |
