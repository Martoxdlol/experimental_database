# B5: Transaction

## Purpose

The unified user-facing transaction type. Composes L4 (query execution) and L5 (read/write sets, commit) into a coherent API with read-your-writes, catalog name resolution, `LimitBoundary` computation, and transactional DDL. This is the largest and most complex module in L6.

## Dependencies

- **B2 (`catalog_cache.rs`)**: `CatalogCache`, `CollectionMeta`, `IndexMeta`
- **B3 (`index_resolver.rs`)**: `IndexResolverImpl`
- **B4 (`catalog_tracker.rs`)**: `CatalogTracker`
- **L4 (`exdb-query`)**: `resolve_access`, `execute_scan`, `merge_with_writes`, `MergeView`, `AccessMethod`, `IndexInfo`, `ReadIntervalInfo`, `ScanRow`, `ScanStats`
- **L5 (`exdb-tx`)**: `ReadSet`, `ReadInterval`, `LimitBoundary`, `WriteSet`, `MutationEntry`, `MutationOp`, `CatalogMutation`, `CommitHandle`, `CommitRequest`, `CommitResult`, `SubscriptionMode`
- **L3 (`exdb-docstore`)**: `PrimaryIndex`, `SecondaryIndex`, `key_encoding`
- **L2 (`exdb-storage`)**: `ScanDirection`
- **L1 (`exdb-core`)**: `CollectionId`, `DocId`, `IndexId`, `Ts`, `Scalar`, `FieldPath`, `Filter`, `RangeExpr`

## Rust Types

```rust
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::{Filter, RangeExpr};
use exdb_storage::btree::ScanDirection;
use exdb_tx::{
    ReadSet, WriteSet, CommitHandle, CommitRequest, CommitResult,
    SubscriptionMode, ReadInterval, LimitBoundary,
};
use crate::catalog_cache::CatalogCache;
use crate::config::TransactionConfig;
use crate::error::DatabaseError;
use crate::subscription::SubscriptionHandle;

/// Options for beginning a transaction (DESIGN.md 5.1).
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    /// If true, write operations return `ReadonlyWrite` error.
    /// No OCC validation at commit. No write set allocated.
    pub readonly: bool,
    /// Subscription mode — controls post-commit read set behavior.
    pub subscription: SubscriptionMode,
    /// Session ID for subscription association. Used by L8 to clean up
    /// all subscriptions for a session on disconnect via
    /// `SubscriptionRegistry::remove_session(session_id)`.
    /// Embedded callers leave this at the default (0).
    pub session_id: u64,
}

impl TransactionOptions {
    /// Convenience constructor for read-only transactions.
    pub fn readonly() -> Self {
        Self { readonly: true, subscription: SubscriptionMode::None, session_id: 0 }
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self { readonly: false, subscription: SubscriptionMode::None, session_id: 0 }
    }
}

/// Unified transaction — handles both reads and writes.
///
/// Created by `Database::begin()`. Borrows `&'db Database` for zero-cost
/// access to shared infrastructure (catalog, storage, commit handle).
///
/// # Lifecycle (DESIGN.md 5.3)
///
/// - **`commit()`**: OCC validate → WAL → materialize → replicate → respond.
///   Readonly + subscription: registers read set (no OCC).
///   Readonly + None: no-op (read set discarded).
/// - **`rollback()`**: explicit discard, equivalent to drop. Consumes self.
/// - **`reset()`**: clears read + write sets, resets query_id. Same snapshot.
/// - **`drop`**: implicit rollback if not committed.
///
/// # Send
///
/// `Transaction` is `Send` — all fields are `Send` (`&Database` is `Send`
/// because `Database: Sync`). Futures holding a transaction work on tokio's
/// multi-threaded runtime. The `'db` lifetime prevents `tokio::spawn`
/// (requires `'static`), which is correct — a transaction must not outlive
/// the database.
pub struct Transaction<'db> {
    db: &'db Database,
    /// Allocated via `CommitHandle::allocate_tx_id()` in `Database::begin()`.
    tx_id: TxId,
    opts: TransactionOptions,
    /// Set to `CommitHandle::visible_ts()` in `Database::begin()`.
    begin_ts: Ts,
    /// Wall-clock milliseconds since epoch, captured once at `begin()`.
    /// Used for `_created_at` on all inserts in this transaction (DESIGN.md 1.11).
    wall_clock_ms: u64,
    read_set: ReadSet,
    write_set: WriteSet,
    committed: bool,
    /// For self-policing timeout checks (DESIGN.md 5.6.6).
    created_at: std::time::Instant,
    last_activity: std::time::Instant,
}
```

## Public API

### Reads (always available)

```rust
impl<'db> Transaction<'db> {
    /// Get a document by ID.
    ///
    /// 1. Check timeout.
    /// 2. Assign query_id.
    /// 3. Resolve collection name → CollectionId (+ catalog read interval
    ///    with this query_id).
    /// 4. Check write set (read-your-writes). If found, return immediately
    ///    WITHOUT recording a data read interval (DESIGN.md 5.4 — the write
    ///    set mutation subsumes any read for OCC purposes). The catalog
    ///    read interval from step 3 is still recorded.
    /// 5. Execute primary get via L4.
    /// 6. Build ReadInterval (point interval, no LimitBoundary).
    /// 7. Record in read_set.
    /// 8. Return document or None.
    pub async fn get(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<Option<serde_json::Value>, DatabaseError>;

    /// Query documents using an index.
    ///
    /// 1. Check timeout.
    /// 2. Assign query_id.
    /// 3. Resolve collection name → CollectionId (+ catalog read interval
    ///    with this query_id).
    /// 4. Resolve index name → IndexMeta (+ catalog read interval with
    ///    this query_id).
    ///    4a. Check index state == Ready. If Building → IndexNotReady error.
    /// 5. Validate and encode range via L4.
    /// 6. Execute scan via L4.
    /// 7. If read-write tx: build MergeView from write_set, call merge_with_writes.
    /// 8. Drain results, check read set size limits.
    /// 9. Compute LimitBoundary (if limit hit and exactly N rows returned).
    /// 10. Build ReadInterval with query_id + limit_boundary.
    /// 11. Record in read_set.
    /// 12. Return documents.
    pub async fn query(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<&Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>, DatabaseError>;

    /// List all collections (with catalog read tracking).
    pub fn list_collections(&mut self) -> Vec<CollectionMeta>;

    /// List all indexes for a collection (with catalog read tracking).
    pub fn list_indexes(
        &mut self,
        collection: &str,
    ) -> Result<Vec<IndexMeta>, DatabaseError>;
}
```

### Writes (error if readonly)

```rust
impl<'db> Transaction<'db> {
    /// Insert a document. Returns the auto-generated DocId.
    ///
    /// 1. Check readonly + timeout.
    /// 2. Resolve collection (write-set-aware, with catalog read interval).
    /// 3. Generate ULID → DocId.
    /// 4. Set `_id` (ULID string) and `_created_at` (wall-clock ms since epoch,
    ///    captured once at `begin()` — DESIGN.md 1.11) on the document.
    /// 5. Strip `_meta` from the body (reserved, never persisted).
    /// 6. Check document size limit.
    /// 7. Buffer in write_set as Insert.
    pub async fn insert(
        &mut self,
        collection: &str,
        body: serde_json::Value,
    ) -> Result<DocId, DatabaseError>;

    /// Replace a document entirely.
    ///
    /// 1. Check readonly + timeout.
    /// 2. Resolve collection.
    /// 3. Read current version (write-set first, then primary index).
    ///    Error if not found (DESIGN.md 1.8).
    /// 4. Preserve `_id` and `_created_at` from the original.
    /// 5. Check size.
    /// 6. Buffer as Replace with previous_ts.
    pub async fn replace(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        body: serde_json::Value,
    ) -> Result<(), DatabaseError>;

    /// Patch a document (shallow merge, DESIGN.md 1.8, 1.12.1).
    ///
    /// 1. Check readonly + timeout.
    /// 2. Resolve collection.
    /// 3. Read current version (write-set first, then primary index).
    ///    Error if not found (DESIGN.md 1.8).
    /// 4. Strip `_meta` from patch body (reserved, never persisted — DESIGN.md 1.12).
    /// 5. Apply shallow merge algorithm:
    ///    a. For each top-level field in the patch: overwrite in the current doc.
    ///       Setting a field to `null` stores explicit null (not removal).
    ///    b. Process `_meta.unset` (from the original patch before stripping):
    ///       for each field path in the array, remove that field from the doc.
    ///       Field paths follow the standard format: string for top-level,
    ///       array of strings for nested (e.g., `["address", "zip"]`).
    ///    c. Validate unset paths are syntactically valid FieldPaths.
    /// 6. Preserve `_id` and `_created_at` (immutable after insert).
    /// 7. Check document size limit.
    /// 8. Buffer as Replace with previous_ts (patch is resolved eagerly —
    ///    the write set always contains the final document body, never a delta).
    pub async fn patch(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        patch: serde_json::Value,
    ) -> Result<(), DatabaseError>;

    /// Delete a document.
    ///
    /// 1. Check readonly + timeout.
    /// 2. Resolve collection.
    /// 3. Verify document exists (write-set first, then primary index).
    ///    Error if not found (DESIGN.md 1.8).
    /// 4. Buffer as Delete with previous_ts.
    pub async fn delete(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<(), DatabaseError>;
}
```

### DDL (error if readonly)

```rust
impl<'db> Transaction<'db> {
    /// Create a collection.
    ///
    /// 1. Check readonly.
    /// 2. Check: name not in committed catalog AND not in pending creates.
    /// 3. Allocate internal CollectionId from catalog cache (not exposed).
    /// 4. Allocate internal IndexId for the `_created_at` secondary index.
    /// 5. Buffer CatalogMutation::CreateCollection in write_set.
    /// 6. Buffer CatalogMutation::CreateIndex for `_created_at` (auto, DESIGN.md 1.11).
    ///
    /// The allocated IDs are internal — the user always references collections
    /// and indexes by name. Subsequent operations in the same transaction
    /// resolve the name via `write_set.resolve_pending_collection(name)`.
    ///
    /// The `_created_at` index is always created alongside the collection.
    /// It cannot be dropped (see `drop_index`).
    pub async fn create_collection(
        &mut self,
        name: &str,
    ) -> Result<(), DatabaseError>;

    /// Drop a collection (cascades: all indexes are also dropped).
    ///
    /// 1. Check readonly.
    /// 2. Resolve collection (must exist, not already dropped in this tx).
    /// 3. Look up all indexes for this collection (from catalog + pending creates).
    /// 4. Buffer CatalogMutation::DropIndex for each index (including `_created_at`).
    /// 5. Buffer CatalogMutation::DropCollection in write_set.
    /// 6. Mark collection as dropped in write_set (so subsequent inserts fail).
    pub async fn drop_collection(
        &mut self,
        name: &str,
    ) -> Result<(), DatabaseError>;

    /// Create a secondary index on a collection.
    ///
    /// 1. Check readonly.
    /// 2. Resolve collection.
    /// 3. Check: index name not already in use for this collection.
    /// 4. Allocate internal IndexId (not exposed).
    /// 5. Buffer CatalogMutation::CreateIndex in write_set.
    pub async fn create_index(
        &mut self,
        collection: &str,
        name: &str,
        fields: Vec<FieldPath>,
    ) -> Result<(), DatabaseError>;

    /// Drop an index.
    ///
    /// 1. Check readonly.
    /// 2. Resolve collection + index.
    /// 3. Prevent dropping `_id` or `_created_at` (system indexes).
    /// 4. Buffer CatalogMutation::DropIndex.
    pub async fn drop_index(
        &mut self,
        collection: &str,
        name: &str,
    ) -> Result<(), DatabaseError>;
}
```

### Lifecycle

```rust
impl<'db> Transaction<'db> {
    /// Commit the transaction.
    ///
    /// - Readonly + None: no-op, returns Success with commit_ts = begin_ts.
    /// - Readonly + subscription: registers read set (no OCC), returns handle.
    /// - Read-write: full commit protocol via CommitHandle.
    pub async fn commit(mut self) -> Result<TransactionResult, DatabaseError>;

    /// Explicit rollback. Discards read + write sets.
    pub fn rollback(self);

    /// Reset: clear read + write sets, reset query_id to 0.
    /// Keeps the same begin_ts (same snapshot).
    pub fn reset(&mut self);

    // ── Introspection ──

    pub fn tx_id(&self) -> u64;
    pub fn begin_ts(&self) -> Ts;
    pub fn is_readonly(&self) -> bool;
    pub fn subscription_mode(&self) -> SubscriptionMode;
}

/// Drop = implicit rollback. No subscription registered.
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Cleanup: remove from active transaction tracking.
        }
    }
}
```

### TransactionResult

```rust
/// Result of a committed transaction.
///
/// Wraps L5's `CommitResult`, converting the raw `event_rx` channel
/// into an RAII `SubscriptionHandle` for ergonomic use.
pub enum TransactionResult {
    /// Commit succeeded.
    Success {
        commit_ts: Ts,
        /// Subscription handle (present when subscription mode != None).
        /// Constructed by L6 from `CommitResult::Success::event_rx`.
        subscription_handle: Option<SubscriptionHandle>,
    },
    /// OCC conflict detected.
    Conflict {
        error: exdb_tx::ConflictError,
        /// For Subscribe mode: auto-retry transaction info.
        retry: Option<exdb_tx::ConflictRetry>,
    },
    /// Replication quorum lost.
    QuorumLost,
}
```

**Mapping from L5 `CommitResult` to L6 `TransactionResult`:**

L5's `CommitResult::Success` contains `subscription_id: Option<SubscriptionId>` and
`event_rx: Option<mpsc::Receiver<InvalidationEvent>>`. L6 wraps these into a
`SubscriptionHandle`:

```rust
// Inside Transaction::commit():
match commit_result {
    CommitResult::Success { commit_ts, subscription_id, event_rx } => {
        let handle = match (subscription_id, event_rx) {
            (Some(id), Some(rx)) => Some(SubscriptionHandle::new(id, registry, rx)),
            _ => None,
        };
        Ok(TransactionResult::Success { commit_ts, subscription_handle: handle })
    }
    CommitResult::Conflict { error, retry } => {
        Ok(TransactionResult::Conflict { error, retry })
    }
    CommitResult::QuorumLost => Ok(TransactionResult::QuorumLost),
}
```

## Internal Helpers

```rust
impl<'db> Transaction<'db> {
    /// Resolve a collection name to CollectionId.
    /// Checks pending creates in write_set first, then committed catalog.
    /// Records a catalog read interval.
    fn resolve_collection(
        &mut self,
        name: &str,
    ) -> Result<CollectionId, DatabaseError>;

    /// Resolve an index name to IndexMeta.
    /// Checks pending creates in write_set first, then committed catalog.
    /// Records a catalog read interval.
    fn resolve_index(
        &mut self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<IndexMeta, DatabaseError>;

    /// Read a document from write set or primary index.
    /// Used by replace/patch/delete to get the current version.
    async fn read_current(
        &self,
        collection_id: CollectionId,
        doc_id: &DocId,
    ) -> Result<(serde_json::Value, Ts), DatabaseError>;

    /// Build a MergeView from the write set for a given collection.
    /// Decomposes pending mutations into the plain slices that L4 expects.
    fn build_merge_view(
        &self,
        collection_id: CollectionId,
    ) -> MergeView<'_>;

    /// Compute LimitBoundary from the last returned row.
    /// Returns None if limit was not hit (fewer rows than limit returned).
    fn compute_limit_boundary(
        &self,
        last_row: &ScanRow,
        index_meta: &IndexMeta,
        direction: ScanDirection,
        limit: usize,
        returned_count: usize,
    ) -> Option<LimitBoundary>;

    /// Check read set size limits. Returns error if exceeded.
    fn check_read_limits(&self) -> Result<(), DatabaseError>;

    /// Assert that the transaction is not readonly.
    fn require_writable(&self) -> Result<(), DatabaseError>;

    /// Check transaction timeout (DESIGN.md 5.6.6).
    /// Called at the start of every operation (get, query, insert, etc.).
    /// Returns TransactionTimeout if max_lifetime or idle_timeout exceeded.
    /// Updates `last_activity` on success.
    fn check_timeout(&mut self) -> Result<(), DatabaseError>;
}
```

## LimitBoundary Computation (Critical for OCC Correctness)

When a query with `limit = Some(N)` returns exactly `N` rows, L6 computes a `LimitBoundary` from the last returned row's sort key:

1. Extract the indexed field values from `last_row.doc` using `index_meta.field_paths`.
2. Encode them using `encode_key_prefix`.
3. Append `last_row.doc_id` and `inv_ts(last_row.version_ts)` to form the full secondary key.
4. For `Asc` direction: `LimitBoundary::Upper(full_key)`.
5. For `Desc` direction: `LimitBoundary::Lower(full_key)`.

If fewer than N rows are returned, the scan exhausted the range → `limit_boundary = None` (full original coverage).

**This is the mechanism that makes the DESIGN.md 5.7 K2/K3/K4 correctness example work.** The `MergeView` from the write set affects which rows are returned, which changes the last row, which changes the `LimitBoundary`, which changes the effective conflict surface. See the detailed example in `docs/plan/overview/05-layer5-transactions.md`.

## Read-Your-Writes Flow (query)

```
tx.query("users", "by_age", [gte("age", 18)], None, Asc, Some(10))
  │
  ├── 1. check_timeout()
  │
  ├── 2. query_id = read_set.next_query_id()
  │
  ├── 3. resolve_collection("users") → CollectionId(5)
  │       └── CatalogTracker::record_collection_name_lookup(read_set, query_id, "users")
  │
  ├── 4. resolve_index(5, "by_age") → IndexMeta { index_id: 3, fields: ["age"] }
  │       └── CatalogTracker::record_index_name_lookup(read_set, query_id, 5, "by_age")
  │       └── check index state == Ready
  │
  ├── 5. L4::resolve_access → AccessMethod::IndexScan { lower, upper, ... }
  │
  ├── 6. L4::execute_scan → QueryScanStream + ReadIntervalInfo
  │
  ├── 7. build_merge_view(CollectionId(5)) → MergeView { inserts, deletes, replaces }
  │
  ├── 8. L4::merge_with_writes(stream, merge_view, sort_fields, range, filter, dir, limit)
  │       → Vec<ScanRow>  (merged, sorted, limit-capped)
  │
  ├── 9. check_read_limits()
  │
  ├── 10. compute_limit_boundary(last_row, index_meta, Asc, 10, returned_count)
  │        → Some(LimitBoundary::Upper(last_key)) if returned_count == 10
  │
  ├── 11. ReadInterval { query_id, lower, upper, limit_boundary }
  │        └── read_set.add_interval(CollectionId(5), IndexId(3), interval)
  │
  └── 12. Return Vec<serde_json::Value>
```

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `begin_readonly` | Readonly tx created, writes rejected |
| 2 | `begin_readwrite` | Read-write tx created, writes accepted |
| 3 | `get_nonexistent_returns_none` | get on empty collection → None |
| 4 | `insert_and_get` | Insert + get in same tx (read-your-writes) |
| 5 | `insert_generates_ulid` | Returned DocId is valid ULID, `_id` set on doc |
| 6 | `insert_sets_created_at` | `_created_at` set to begin_ts wall clock |
| 7 | `replace_updates_body` | Replace + get returns new body |
| 8 | `replace_preserves_id_and_created_at` | `_id` and `_created_at` unchanged |
| 9 | `replace_nonexistent_errors` | Replace on missing doc → DocNotFound |
| 10 | `patch_shallow_merge` | Only patched fields change |
| 11 | `patch_unset_removes_fields` | `_meta.unset` removes specified fields |
| 12 | `patch_preserves_id_and_created_at` | System fields unchanged |
| 13 | `delete_and_get` | Delete + get in same tx → None |
| 14 | `delete_nonexistent_errors` | Delete on missing doc → DocNotFound |
| 15 | `readonly_insert_errors` | Insert on readonly tx → ReadonlyWrite |
| 16 | `readonly_create_collection_errors` | DDL on readonly tx → ReadonlyWrite |
| 17 | `create_collection_then_insert` | Create + insert in same tx works |
| 18 | `create_duplicate_collection_errors` | Same name twice → AlreadyExists |
| 19 | `drop_collection_then_insert_errors` | Insert into dropped collection fails |
| 20 | `create_index_then_query` | Create index + query in same tx |
| 21 | `drop_system_index_errors` | Cannot drop `_id` or `_created_at` |
| 22 | `query_with_limit_boundary` | LimitBoundary computed when limit hit |
| 23 | `query_without_limit_no_boundary` | No LimitBoundary when fewer rows returned |
| 24 | `query_read_your_writes_insert` | Inserted doc appears in scan results |
| 25 | `query_read_your_writes_delete` | Deleted doc excluded from scan results |
| 26 | `query_read_your_writes_replace` | Replaced doc shows new body in results |
| 27 | `commit_readonly_none` | Readonly + None = no-op commit |
| 28 | `commit_readonly_watch` | Readonly + Watch = subscription registered |
| 29 | `commit_readwrite_success` | Full commit, data visible after |
| 30 | `commit_occ_conflict` | Concurrent write detected → Conflict |
| 31 | `rollback_discards_writes` | Rollback + new tx = no writes visible |
| 32 | `reset_clears_state` | Reset + re-read = fresh read set |
| 33 | `drop_without_commit_rolls_back` | Implicit rollback on drop |
| 34 | `list_collections_records_catalog_read` | Catalog read interval recorded |
| 35 | `list_indexes_records_catalog_read` | Catalog read interval recorded |
| 36 | `doc_too_large_rejected` | Body exceeding max_doc_size → error |
| 37 | `resolve_collection_checks_write_set_first` | Pending create resolved before catalog |
| 38 | `catalog_read_conflict_with_ddl` | DDL in concurrent tx causes OCC conflict |
| 39 | `query_merged_limit_boundary_correctness` | K2/K3/K4 example from DESIGN.md 5.7 |
| 40 | `subscription_handle_returned_on_commit` | SubscriptionHandle in TransactionResult |
| 41 | `patch_unset_nested_field` | `_meta.unset: [["address", "zip"]]` removes nested field |
| 42 | `patch_unset_invalid_path_errors` | Invalid field path in unset → error |
| 43 | `patch_meta_stripped` | `_meta` not persisted in document body |
| 44 | `create_collection_auto_creates_created_at_index` | `_created_at` index exists after create |
| 45 | `drop_collection_cascades_indexes` | All indexes removed when collection dropped |
| 46 | `cross_collection_atomic_commit` | Create 2 colls, insert into both, commit atomically |
| 47 | `concurrent_create_same_collection_conflicts` | TX1 + TX2 create "users" → one conflicts |
| 48 | `query_building_index_errors` | Query on Building index → IndexNotReady |
| 49 | `insert_after_read_your_writes_patch` | Insert, patch, get returns patched body |
| 50 | `catalog_read_conflict_with_concurrent_ddl` | TX1 reads catalog, TX2 creates coll → TX1 conflicts |
