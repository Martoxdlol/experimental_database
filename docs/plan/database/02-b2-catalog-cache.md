# B2: Catalog Cache

## Purpose

In-memory dual-indexed catalog providing O(1) lookups by both name and ID for collections and indexes. Derived from the durable catalog B-tree at startup. All mutations go through the commit coordinator — the cache is updated atomically alongside the catalog B-tree during commit step 4a.

## Dependencies

- **L1 (`exdb-core`)**: `CollectionId`, `IndexId`, `FieldPath`, `Ts`

## Rust Types

```rust
use exdb_core::types::{CollectionId, IndexId};
use exdb_core::field_path::FieldPath;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Metadata for a collection in the catalog.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    /// Root page of the primary B-tree for this collection.
    pub primary_root_page: u32,
    /// Approximate document count (updated on commit).
    pub doc_count: u64,
}

/// Metadata for a secondary index in the catalog.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    /// Fields indexed (in order). Compound indexes have multiple entries.
    pub field_paths: Vec<FieldPath>,
    /// Root page of the secondary index B-tree.
    pub root_page: u32,
    /// Current state of the index.
    pub state: IndexState,
}

/// Index lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Index is being built in the background. Queries cannot use it yet.
    Building,
    /// Index is ready for queries.
    Ready,
    /// Index is being dropped (cleanup in progress).
    Dropping,
}

/// In-memory dual-indexed catalog cache.
///
/// Provides O(1) lookup by name and by ID for both collections and indexes.
/// Thread-safe reads via external `RwLock` (L6 wraps it).
///
/// # Invariant
///
/// The cache is always consistent with the catalog B-tree. Both are updated
/// atomically during the commit coordinator's apply step (step 4a).
pub struct CatalogCache {
    // Collection lookup (bidirectional)
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,

    // Index lookup (bidirectional)
    indexes: HashMap<IndexId, IndexMeta>,
    index_name_to_id: HashMap<(CollectionId, String), IndexId>,
    /// All indexes for a given collection (for index listing + delta computation).
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,

    // ID allocators (monotonic, gap-tolerant)
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}
```

## Public API

```rust
impl CatalogCache {
    /// Create an empty catalog cache.
    pub fn new() -> Self;

    // ── By Name ──

    /// Look up a collection by name. O(1).
    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta>;

    /// Look up an index by (collection_id, name). O(1).
    pub fn get_index_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Option<&IndexMeta>;

    // ── By ID ──

    /// Look up a collection by ID. O(1).
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<&CollectionMeta>;

    /// Look up an index by ID. O(1).
    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexMeta>;

    // ── Listing ──

    /// List all collections. O(N) where N is the number of collections.
    pub fn list_collections(&self) -> Vec<&CollectionMeta>;

    /// List all indexes for a collection. O(K) where K is the index count.
    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;

    /// List all Ready indexes for a collection (used by IndexResolver).
    pub fn ready_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;

    // ── Mutation (called by commit coordinator during step 4a) ──

    /// Add a collection to the cache.
    ///
    /// # Panics
    /// Panics if a collection with the same name or ID already exists.
    pub fn add_collection(&mut self, meta: CollectionMeta);

    /// Remove a collection and all its indexes from the cache.
    /// Returns the removed CollectionMeta, or None if not found.
    pub fn remove_collection(&mut self, id: CollectionId) -> Option<CollectionMeta>;

    /// Add an index to the cache.
    ///
    /// # Panics
    /// Panics if an index with the same (collection_id, name) or ID already exists.
    pub fn add_index(&mut self, meta: IndexMeta);

    /// Remove an index from the cache.
    /// Returns the removed IndexMeta, or None if not found.
    pub fn remove_index(&mut self, id: IndexId) -> Option<IndexMeta>;

    /// Update an index's state (Building → Ready, Ready → Dropping).
    pub fn set_index_state(&mut self, id: IndexId, state: IndexState);

    // ── ID Allocation ──

    /// Allocate the next collection ID (atomic, gap-tolerant).
    pub fn next_collection_id(&self) -> CollectionId;

    /// Allocate the next index ID (atomic, gap-tolerant).
    pub fn next_index_id(&self) -> IndexId;

    // ── Counts ──

    pub fn collection_count(&self) -> usize;
    pub fn index_count(&self) -> usize;
}
```

## Internal Implementation Notes

- `next_collection_id` and `next_index_id` use `AtomicU64::fetch_add(1, Relaxed)`. The initial value is set to `max(existing_ids) + 1` at startup. Gaps from aborted transactions are harmless.
- `collection_indexes` is a denormalization for fast iteration. It is kept in sync by `add_index` and `remove_index`.
- `remove_collection` cascades: all indexes for the collection are removed from all maps.
- The `_created_at` secondary index is a regular `IndexMeta` entry — not a special field on `CollectionMeta`. It is created automatically when a collection is created (L6 handles this in `create_collection`).

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `new_cache_is_empty` | Empty state, zero counts |
| 2 | `add_and_lookup_collection_by_name` | Name → CollectionMeta round-trip |
| 3 | `add_and_lookup_collection_by_id` | ID → CollectionMeta round-trip |
| 4 | `add_and_lookup_index_by_name` | (CollectionId, name) → IndexMeta round-trip |
| 5 | `add_and_lookup_index_by_id` | ID → IndexMeta round-trip |
| 6 | `list_collections` | All collections returned |
| 7 | `list_indexes_for_collection` | Only indexes for the given collection |
| 8 | `ready_indexes_filters_building` | Building/Dropping indexes excluded |
| 9 | `remove_collection_cascades_indexes` | Removing a collection removes all its indexes |
| 10 | `remove_index` | Single index removal, other indexes unaffected |
| 11 | `set_index_state` | State transitions work correctly |
| 12 | `next_collection_id_monotonic` | Sequential allocation, no duplicates |
| 13 | `next_index_id_monotonic` | Sequential allocation, no duplicates |
| 14 | `duplicate_collection_name_panics` | Adding duplicate name panics |
| 15 | `duplicate_index_name_panics` | Adding duplicate (coll_id, name) panics |
| 16 | `lookup_nonexistent_returns_none` | Missing lookups return None |
