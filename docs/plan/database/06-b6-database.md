# B6: Database

## Purpose

The core integration point. `Database` owns all components (storage engine, indexes, catalog cache, commit system) and provides the lifecycle API (`open`, `open_in_memory`, `close`) plus the transaction entry point (`begin`). Spawns and manages background tasks.

## Dependencies

- **B1 (`config.rs`)**: `DatabaseConfig`
- **B2 (`catalog_cache.rs`)**: `CatalogCache`, `CollectionMeta`, `IndexMeta`
- **B3 (`index_resolver.rs`)**: `IndexResolverImpl`
- **B5 (`transaction.rs`)**: `Transaction`, `TransactionOptions`
- **B7 (`subscription.rs`)**: `SubscriptionHandle`
- **B9 (`catalog_persistence.rs`)**: `CatalogPersistence` — catalog B-tree read/write
- **B10 (`catalog_recovery.rs`)**: `DatabaseRecoveryHandler` — WAL replay for crash recovery
- **L2 (`exdb-storage`)**: `StorageEngine`, `StorageConfig`
- **L3 (`exdb-docstore`)**: `PrimaryIndex`, `SecondaryIndex`
- **L5 (`exdb-tx`)**: `CommitCoordinator`, `ReplicationRunner`, `CommitHandle`, `TsAllocator`, `SubscriptionRegistry`, `ReplicationHook`, `NoReplication`
- **tokio**: `task::LocalSet`, `sync::CancellationToken`

> **See also:** `10-durability.md` for the full durability design (crash scenarios, idempotency requirements, rollback vacuum) and `11-test-plan.md` for the exhaustive test matrix.

## Rust Types

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use exdb_core::types::{CollectionId, IndexId};
use exdb_storage::engine::StorageEngine;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_tx::{CommitHandle, TsAllocator, SubscriptionRegistry, ReplicationHook};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;  // add tokio-util to [dependencies]

use crate::catalog_cache::CatalogCache;
use crate::config::DatabaseConfig;
use crate::transaction::{Transaction, TransactionOptions};
use crate::error::DatabaseError;

/// A database instance — the primary public API.
///
/// Fully functional without networking. Composes L1–L5 into a single
/// operational unit with collection management, transactions, and
/// background maintenance.
///
/// # Thread Safety
///
/// `Database` is `Send + Sync`. Multiple tasks can call `begin()` concurrently.
/// Transactions borrow `&Database` and run on the caller's task.
pub struct Database {
    /// Database name (for multi-db management).
    name: String,
    /// Configuration.
    config: DatabaseConfig,
    /// Data directory path (None for in-memory).
    path: Option<PathBuf>,

    // ── Layer 2: Storage ──
    storage: Arc<StorageEngine>,

    // ── Layer 3: Indexes ──
    /// Primary indexes by CollectionId.
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    /// Secondary indexes by IndexId.
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,

    // ── Layer 5: Transactions ──
    commit_handle: CommitHandle,
    ts_allocator: Arc<TsAllocator>,

    // ── Layer 6: Catalog ──
    catalog: Arc<RwLock<CatalogCache>>,

    // ── State ──
    /// Active transaction count (for graceful shutdown).
    active_tx_count: Arc<AtomicU64>,
    shutdown: CancellationToken,
}
```

## Public API

### Lifecycle

```rust
impl Database {
    /// Open a file-backed database at the given path.
    ///
    /// Performs the full startup sequence:
    /// 1. StorageEngine::open (DWB restore + WAL replay)
    /// 2. Scan catalog B-tree → build CatalogCache
    /// 3. Open primary + secondary B-tree handles
    /// 4. Rollback vacuum (if needed)
    /// 5. Create CommitCoordinator + ReplicationRunner + CommitHandle
    /// 6–9. Spawn background tasks
    ///
    /// `replication`: optional replication hook. Defaults to `NoReplication`.
    pub async fn open(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self, DatabaseError>;

    /// Open an ephemeral in-memory database.
    ///
    /// No files, no durability. Useful for testing, caching, temporary data.
    pub async fn open_in_memory(
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self, DatabaseError>;

    /// Close the database.
    ///
    /// 1. Signal shutdown to all background tasks
    /// 2. Wait for active transactions (with timeout)
    /// 3. Stop background tasks
    /// 4. Final checkpoint
    /// 5. Close storage engine
    pub async fn close(self) -> Result<(), DatabaseError>;
}
```

### Transactions

```rust
impl Database {
    /// Begin a transaction.
    ///
    /// - `begin_ts` is set to `commit_handle.visible_ts()` (latest committed +
    ///   replicated timestamp).
    /// - `tx_id` is allocated via `commit_handle.allocate_tx_id()`.
    /// - `wall_clock_ms` is captured from `SystemTime::now()` (for `_created_at`).
    /// - `session_id` for subscription cleanup comes from `opts.session_id`
    ///   (default 0 for embedded; L8 sets it to the connection's session ID).
    /// - Increments `active_tx_count`.
    ///
    /// # Errors
    ///
    /// Returns `QuorumLost` if the replication hook reports no quorum.
    pub fn begin(&self, opts: TransactionOptions) -> Result<Transaction<'_>, DatabaseError>;
}
```

### Read-Only Accessors

```rust
impl Database {
    /// List all collections (convenience, outside transactions).
    ///
    /// Does NOT record catalog reads for OCC. For transactional consistency,
    /// use `Transaction::list_collections()`.
    pub fn list_collections(&self) -> Vec<CollectionMeta>;

    /// Get a collection by name (convenience, outside transactions).
    pub fn get_collection(&self, name: &str) -> Option<CollectionMeta>;

    /// Get the subscription registry (for L7/L8 push notifications).
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>>;

    /// Get the storage engine (for L7 WAL access).
    pub fn storage(&self) -> &Arc<StorageEngine>;

    /// Get the commit handle (for advanced L7/L8 use).
    pub fn commit_handle(&self) -> &CommitHandle;

    /// Get the database name.
    pub fn name(&self) -> &str;

    /// Get the configuration.
    pub fn config(&self) -> &DatabaseConfig;

    /// Check if the database is shutting down.
    pub fn is_shutting_down(&self) -> bool;
}
```

### Internal (for Transaction)

```rust
impl Database {
    /// Get the catalog cache (used by Transaction).
    pub(crate) fn catalog(&self) -> &Arc<RwLock<CatalogCache>>;

    /// Get primary indexes (used by Transaction for queries).
    pub(crate) fn primary_indexes(&self) -> &Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>;

    /// Get secondary indexes (used by Transaction for queries).
    pub(crate) fn secondary_indexes(&self) -> &Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>;

    /// Get transaction config (used by Transaction for limit checks).
    pub(crate) fn transaction_config(&self) -> &TransactionConfig;
}
```

## Background Tasks

### Checkpoint Task

```rust
/// Periodic checkpoint task.
///
/// Runs on a regular tokio task. Sleeps for `checkpoint_interval`, then
/// checks if enough WAL has been written since the last checkpoint.
/// If so, triggers a checkpoint through the storage engine.
async fn checkpoint_task(
    storage: Arc<StorageEngine>,
    config: DatabaseConfig,
    shutdown: CancellationToken,
);
```

### Vacuum Task

```rust
/// Periodic vacuum task.
///
/// Scans for old document versions eligible for cleanup and submits
/// vacuum mutations through the commit channel.
async fn vacuum_task(
    commit_handle: CommitHandle,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    config: DatabaseConfig,
    shutdown: CancellationToken,
);
```

### Index Builder Task

```rust
/// Background index building task.
///
/// When a new index is created (via DDL commit), it starts in `Building`
/// state. This task:
/// 1. Scans the primary index for all documents in the collection.
/// 2. For each document, computes the secondary key(s) using the index's
///    field_paths (including array expansion via L3's compute_index_entries).
/// 3. Inserts entries into the secondary B-tree via the commit channel
///    (batched for efficiency).
/// 4. On completion, marks the index as `Ready` via a CatalogMutation.
///
/// Concurrent writes during building are handled by the CommitCoordinator:
/// it also inserts into Building indexes during step 4, so no entries are
/// missed. Duplicate entries (from overlap between builder scan and
/// concurrent inserts) are harmless — the secondary B-tree is idempotent
/// on (key, doc_id, ts) tuples.
///
/// On crash during building: the index is dropped on startup (step 5c)
/// and must be recreated by the user.
async fn index_builder_task(
    commit_handle: CommitHandle,
    catalog: Arc<RwLock<CatalogCache>>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    shutdown: CancellationToken,
);
```

### Transaction Timeout Task

```rust
/// Transaction timeout enforcement (DESIGN.md 5.6.6).
///
/// This task does NOT track individual transactions (that would require
/// a shared registry). Instead, timeout enforcement is cooperative:
///
/// Each `Transaction` records its `created_at: Instant` and
/// `last_activity: Instant`. Before each operation (get, query, insert,
/// etc.), the transaction checks:
///   - `elapsed > max_lifetime` → return TransactionTimeout error
///   - `elapsed since last_activity > idle_timeout` → return TransactionTimeout error
///
/// This is a self-policing model: the timeout fires on the next operation
/// after the deadline, not proactively. For truly idle transactions (no
/// operations at all), the active_tx_count and `close()` timeout handle
/// cleanup. L8 (server) can add proactive timeout enforcement per-session.
```

## Catalog Update Mechanism (Post-Commit Callback)

When the CommitCoordinator applies a commit containing `CatalogMutation` entries
(step 4a), the physical side effects (B-tree allocation, CatalogCache update,
opening new index handles) must happen within the writer task. **All catalog
B-tree writes go through `CatalogPersistence` (B9)** — see `10-durability.md`
for the full design.

The mechanism:

1. CommitCoordinator detects `CatalogMutation` entries in the `WriteSet`.
2. For each `CreateCollection`:
   - `storage.create_btree()` twice (primary + `_created_at`)
   - `CatalogPersistence::apply_create_collection(...)` → writes to both catalog B-trees + updates CatalogCache
   - `CatalogPersistence::apply_create_index(...)` → for the `_created_at` auto-index
   - Create `PrimaryIndex` and `SecondaryIndex` handles, insert into shared maps
3. For each `DropCollection`:
   - `CatalogPersistence::apply_drop_collection(...)` → removes from both B-trees, cascades indexes
   - Remove handles from shared index maps
4. For each `CreateIndex`:
   - `storage.create_btree()`
   - `CatalogPersistence::apply_create_index(...)` → writes to both B-trees + updates cache
   - Create `SecondaryIndex` handle
5. For each `DropIndex`:
   - `CatalogPersistence::apply_drop_index(...)` → removes from both B-trees
   - Remove handle from shared map

The catalog cache, catalog B-trees, and index handle maps are all updated atomically
within the same writer step — no intermediate state is visible to readers.

**Idempotency**: All `CatalogPersistence::apply_*` methods are idempotent. This is
essential because the same mutations may be replayed during WAL recovery if a crash
occurs after the WAL write but before the B-tree pages are flushed to disk. See
`10-durability.md` "Idempotency Requirement" for details.

## Active Transaction Tracking

`Database` tracks the count of active transactions via `active_tx_count: Arc<AtomicU64>`.

- `begin()` increments the counter.
- `Transaction::drop()` decrements it (whether via commit, rollback, or implicit drop).
- `close()` waits until the counter reaches zero (with a configurable timeout from `DatabaseConfig`).

This is intentionally a counter, not a registry — L6 does not need to enumerate or
individually cancel transactions. The counter is sufficient for graceful shutdown.

## Startup Sequence Detail

### File-backed (`Database::open`)

> **Full durability analysis:** See `10-durability.md` for crash scenario analysis and idempotency requirements for each step below.

```
1.  Create data directory if needed

2.  StorageEngine::open(path, StorageConfig::from(config))
    → Reads file header (page 0) for checkpoint_lsn, catalog root pages
    → DWB recovery (S8): restore any torn pages in data.db
    → Returns (engine, file_header)

3.  Open catalog B-trees from file header root pages (by-ID and by-Name)

4.  CatalogPersistence::load_catalog(id_btree, name_btree)      [B9]
    → Scans both catalog B-trees → populates CatalogCache
    → Sets next_collection_id = max(collection_ids) + 1
    → Sets next_index_id = max(index_ids) + 1
    → Opens primary B-tree handle → PrimaryIndex for each collection
    → Opens secondary B-tree handle → SecondaryIndex for each index
    → Drops indexes in Building state (incomplete builds from prior crash)
    → Validates _created_at invariant for every collection

5.  Create DatabaseRecoveryHandler with catalog + index handles    [B10]

6.  Recovery::run(storage, wal, dwb_path, checkpoint_lsn, handler) [S10]
    → WAL replay from checkpoint_lsn
    → Handler applies TxCommit (catalog + data + index), IndexReady, Vacuum, VisibleTs
    → All replay operations are IDEMPOTENT (critical for correctness)
    → Returns (end_lsn, RecoveredState)

7.  Extract RecoveredState: updated catalog, index handles, recovered_ts, visible_ts

8.  Rollback vacuum: if recovered_ts > visible_ts                  [B10]
    → Scan WAL backwards for un-replicated commits in (visible_ts, recovered_ts]
    → Reverse their effects: data mutations, index deltas, catalog mutations
    → Write RollbackVacuum WAL record (so this is not repeated on next restart)
    → See 10-durability.md "Rollback Vacuum" section for full algorithm

9.  Update file header: next_collection_id, next_index_id from catalog

10. Create CommitCoordinator::new(recovered_ts, visible_ts, ...)
    → (coordinator, runner, handle)

11. Spawn coordinator on LocalSet (it is !Send)
12. Spawn runner on tokio::spawn (it is Send)
13. Spawn checkpoint_task
14. Spawn vacuum_task
15. Return Database
```

### In-memory (`Database::open_in_memory`)

```
1. StorageEngine::open_in_memory(StorageConfig::from(config))
2. CatalogCache::new() (empty)
3. CommitCoordinator::new(0, 0, ...)
4. Spawn coordinator + runner
5. Return Database (no checkpoint/vacuum tasks needed)
```

## Tests

The tests below are the B6 integration tests (in-memory). For the complete test matrix including durability, concurrency, and stress tests, see **`11-test-plan.md`** which specifies ~300 tests across all L6 modules.

### B6 Integration Tests (In-Memory)

| # | Test | Validates |
|---|------|-----------|
| 1 | `open_in_memory` | In-memory database opens successfully |
| 2 | `begin_and_commit_empty` | Empty transaction commits successfully |
| 3 | `create_collection_and_query` | Full cycle: create collection, insert, query |
| 4 | `multiple_concurrent_readers` | Multiple read-only txns run concurrently |
| 5 | `close_flushes_state` | Close + reopen preserves data (file-backed) |
| 6 | `begin_returns_visible_ts` | begin_ts matches visible_ts |
| 7 | `list_collections_outside_tx` | Convenience method works |
| 8 | `shutdown_cancels_background_tasks` | Tasks stopped on close |
| 9 | `transaction_after_close_errors` | Cannot begin after close |
| 10 | `occ_conflict_across_transactions` | Two concurrent write txns conflict |
| 11 | `subscription_fires_on_write` | Watch subscription invalidated by commit |
| 12 | `create_collection_persists` | Collection visible after commit in new tx |
| 13 | `create_index_persists` | Index visible and queryable after commit |
| 14 | `insert_then_query_new_transaction` | Data visible in subsequent transaction |
| 15 | `drop_collection_removes_data` | Dropped collection not visible |
| 16 | `multiple_collections_in_one_tx` | Create multiple collections atomically |
| 17 | `ddl_and_data_in_one_tx` | DDL + inserts in same transaction |
| 18 | `rollback_leaves_no_trace` | Rolled back tx leaves database unchanged |
| 19 | `reset_clears_transaction_state` | Reset allows re-reading fresh state |
| 20 | `in_memory_no_background_tasks` | In-memory mode skips checkpoint/vacuum |
| 21 | `concurrent_ddl_same_collection_conflicts` | Two txns create "users" → one OCC conflict |
| 22 | `catalog_consistent_after_restart` | Create coll, close, reopen → catalog rebuilt |
| 23 | `created_at_index_auto_created` | After create_collection, `_created_at` index queryable |
| 24 | `active_tx_count_tracks_lifecycle` | begin increments, drop/commit decrements |
| 25 | `close_waits_for_active_transactions` | Close blocks until tx commits |
| 26 | `transaction_timeout_self_check` | Operation after max_lifetime → TransactionTimeout |
| 27 | `open_in_memory_then_insert_and_query` | Full CRUD cycle with in-memory storage |
| 28 | `two_readers_one_writer_no_conflict` | Reader tx + writer tx on disjoint keys → both succeed |
| 29 | `write_write_conflict_detected` | Two writers on same key → one gets Conflict |
| 30 | `ddl_data_atomic_rollback` | Create collection + insert, rollback → neither visible |
| 31 | `subscription_watch_fires_on_relevant_write` | Watch on range, write in range → event fired |
| 32 | `subscription_watch_silent_on_irrelevant_write` | Watch on range [A,M], write in [N,Z] → no event |
| 33 | `subscription_notify_fires_once` | Notify mode fires exactly once |
| 34 | `begin_after_shutdown_returns_error` | close() then begin() → ShuttingDown |
| 35 | `checkpoint_reduces_recovery_time` | Insert many, checkpoint, insert few, restart → only few replayed |
| 36 | `vacuum_removes_old_versions` | Insert, replace 5x, vacuum → old versions cleaned |
| 37 | `index_builder_completes` | Create index on populated collection → Building → Ready |
| 38 | `query_building_index_returns_error` | Query on Building index → IndexNotReady |
| 39 | `concurrent_index_build_and_writes` | Create index + concurrent inserts → all indexed |
| 40 | `large_document_insert_and_retrieve` | Insert 1MB doc → get returns identical bytes |
| 41 | `document_at_size_limit` | Insert at exactly max_doc_size → succeeds |
| 42 | `document_over_size_limit` | Insert at max_doc_size + 1 → DocTooLarge |
| 43 | `100_collections_in_one_tx` | Create 100 collections atomically |
| 44 | `drop_collection_then_recreate_same_name` | Drop + recreate "users" → new CollectionId |
| 45 | `query_created_at_ascending` | Insert A,B,C → _created_at ASC → A,B,C |
| 46 | `query_created_at_descending` | Insert A,B,C → _created_at DESC → C,B,A |
| 47 | `list_collections_includes_pending_creates` | In-tx: create, list → includes pending |
| 48 | `list_indexes_includes_pending_creates` | In-tx: create index, list → includes pending |
| 49 | `three_way_occ_conflict` | TX1+TX2+TX3 on same key → only one succeeds |
| 50 | `snapshot_isolation_read_consistency` | TX1 reads, TX2 commits, TX1 re-reads → same result |
| 51 | `reset_then_see_same_snapshot` | TX1.reset() → same begin_ts, same snapshot |
| 52 | `active_tx_count_correct_after_drop` | Begin, drop without commit → count decrements |
| 53 | `close_timeout_with_hung_transaction` | Abandoned tx, close with short timeout → completes |
| 54 | `startup_drops_building_indexes` | Building index + crash → index gone after reopen |
| 55 | `startup_validates_created_at_invariant` | Missing _created_at → startup error |
