# B6: Database

## Purpose

The core integration point. `Database` owns all components (storage engine, indexes, catalog cache, commit system) and provides the lifecycle API (`open`, `open_in_memory`, `close`) plus the transaction entry point (`begin`). Spawns and manages background tasks.

## Dependencies

- **B1 (`config.rs`)**: `DatabaseConfig`
- **B2 (`catalog_cache.rs`)**: `CatalogCache`, `CollectionMeta`, `IndexMeta`
- **B3 (`index_resolver.rs`)**: `IndexResolverImpl`
- **B5 (`transaction.rs`)**: `Transaction`, `TransactionOptions`
- **B7 (`subscription.rs`)**: `SubscriptionHandle`
- **L2 (`exdb-storage`)**: `StorageEngine`, `StorageConfig`
- **L3 (`exdb-docstore`)**: `PrimaryIndex`, `SecondaryIndex`
- **L5 (`exdb-tx`)**: `CommitCoordinator`, `ReplicationRunner`, `CommitHandle`, `TsAllocator`, `SubscriptionRegistry`, `ReplicationHook`, `NoReplication`
- **tokio**: `task::LocalSet`, `sync::CancellationToken`

## Rust Types

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use exdb_core::types::{CollectionId, IndexId};
use exdb_storage::engine::StorageEngine;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_tx::{CommitHandle, TsAllocator, SubscriptionRegistry, ReplicationHook};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

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
    /// The snapshot timestamp (`begin_ts`) is set to `visible_ts` — the latest
    /// committed + replicated timestamp. This ensures the snapshot only includes
    /// fully committed and replicated data.
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
opening new index handles) must happen within the writer task. The mechanism:

1. CommitCoordinator detects `CatalogMutation` entries in the `WriteSet`.
2. For each `CreateCollection`: call `storage.create_btree()` twice (primary + `_created_at`),
   insert into both catalog B-trees (by-id and by-name), then call `catalog.write().add_collection()`
   and `catalog.write().add_index()`. Create `PrimaryIndex` and `SecondaryIndex` handles,
   insert into the shared index maps.
3. For each `DropCollection`: call `catalog.write().remove_collection()` (cascades indexes),
   remove handles from the shared index maps.
4. For each `CreateIndex`: call `storage.create_btree()`, insert into catalog B-trees,
   call `catalog.write().add_index()`, create `SecondaryIndex` handle.
5. For each `DropIndex`: set index state to `Dropping`, remove from catalog.

The catalog cache, catalog B-trees, and index handle maps are all updated atomically
within the same writer step — no intermediate state is visible to readers.

## Active Transaction Tracking

`Database` tracks the count of active transactions via `active_tx_count: Arc<AtomicU64>`.

- `begin()` increments the counter.
- `Transaction::drop()` decrements it (whether via commit, rollback, or implicit drop).
- `close()` waits until the counter reaches zero (with a configurable timeout from `DatabaseConfig`).

This is intentionally a counter, not a registry — L6 does not need to enumerate or
individually cancel transactions. The counter is sufficient for graceful shutdown.

## Startup Sequence Detail

### File-backed (`Database::open`)

```
1. Create data directory if needed
2. StorageEngine::open(path, StorageConfig::from(config))
   → DWB restore (if data.dwb exists)
   → WAL replay from checkpoint_lsn
   → Returns (engine, recovered_ts, visible_ts)
   Note: visible_ts is read from the last WAL_RECORD_VISIBLE_TS during replay.
   On single-node (NoReplication), visible_ts == recovered_ts.
3. Open catalog B-trees from file header root pages
4. Scan both catalog B-trees → populate CatalogCache
5. For each collection in catalog:
   a. Open primary B-tree handle → PrimaryIndex
   b. For each index: open secondary B-tree handle → SecondaryIndex
   c. Drop any indexes in Building state (incomplete builds from prior crash)
   d. Validate that every collection has `_created_at` index (invariant check)
6. Rollback vacuum: if recovered_ts > visible_ts, clean unreplicated entries
7. Create CommitCoordinator::new(recovered_ts, visible_ts, ...)
   → (coordinator, runner, handle)
8. Spawn coordinator on LocalSet (it is !Send)
9. Spawn runner on tokio::spawn (it is Send)
10. Spawn checkpoint_task
11. Spawn vacuum_task
12. Return Database
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
