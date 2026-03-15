# B4: Catalog Tracker

## Purpose

Encodes catalog reads and writes as intervals/deltas on reserved pseudo-collections (`CATALOG_COLLECTIONS`, `CATALOG_INDEXES`). This thin adapter translates human-readable catalog operations into the byte-interval representation used by OCC validation and subscription invalidation (DESIGN.md 5.6.7).

L6's `Transaction` calls `CatalogTracker` methods whenever it performs a catalog lookup (resolve collection name, list indexes, etc.). The tracker records the appropriate `ReadInterval` in the transaction's `ReadSet`.

## Dependencies

- **B2 (`catalog_cache.rs`)**: `CatalogCache` (for name-to-id resolution)
- **L5 (`exdb-tx`)**: `ReadSet`, `ReadInterval`, `CATALOG_COLLECTIONS`, `CATALOG_INDEXES`, `CATALOG_COLLECTIONS_NAME_IDX`, `CATALOG_INDEXES_NAME_IDX`, `PRIMARY_INDEX_SENTINEL`
- **L3 (`exdb-docstore`)**: `encode_key_prefix`, `successor_key` (for encoding catalog keys)
- **L1 (`exdb-core`)**: `CollectionId`, `IndexId`, `Scalar`

## Public API

```rust
use exdb_core::types::{CollectionId, IndexId, Scalar};
use exdb_tx::{ReadSet, ReadInterval, QueryId};
use std::ops::Bound;

/// Catalog read tracking — encodes catalog lookups as intervals on pseudo-collections.
///
/// Each method records a ReadInterval in the provided ReadSet. The intervals
/// use the same key encoding as data intervals, ensuring unified OCC and
/// subscription conflict detection for both data and DDL operations.
pub struct CatalogTracker;

impl CatalogTracker {
    /// Record a catalog read for resolving a collection name.
    ///
    /// Produces a point interval on `(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)`
    /// covering the encoded name key.
    ///
    /// Called by `Transaction` every time it resolves a collection name for
    /// get, query, insert, replace, patch, delete, list_indexes, or DDL.
    pub fn record_collection_name_lookup(
        read_set: &mut ReadSet,
        query_id: QueryId,
        name: &str,
    );

    /// Record a catalog read for listing all collections.
    ///
    /// Produces a full-range interval on `(CATALOG_COLLECTIONS, PRIMARY_INDEX_SENTINEL)`.
    ///
    /// Called by `Transaction::list_collections()`.
    pub fn record_list_collections(
        read_set: &mut ReadSet,
        query_id: QueryId,
    );

    /// Record a catalog read for resolving an index name within a collection.
    ///
    /// Produces a point interval on `(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)`
    /// covering the encoded `(collection_id, name)` key.
    ///
    /// Called by `Transaction::query()` when resolving the index name.
    pub fn record_index_name_lookup(
        read_set: &mut ReadSet,
        query_id: QueryId,
        collection_id: CollectionId,
        name: &str,
    );

    /// Record a catalog read for listing all indexes of a collection.
    ///
    /// Produces a prefix-range interval on `(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)`
    /// covering the `collection_id` prefix.
    ///
    /// Called by `Transaction::list_indexes(collection)`.
    pub fn record_list_indexes(
        read_set: &mut ReadSet,
        query_id: QueryId,
        collection_id: CollectionId,
    );
}
```

## Key Encoding

Catalog keys use the same `encode_key_prefix` from L3:

| Catalog Operation | Key Encoding | Pseudo-Collection | Pseudo-Index |
|-------------------|-------------|-------------------|-------------|
| Resolve collection name | `encode_key_prefix(&[Scalar::String(name)])` | `CATALOG_COLLECTIONS` | `CATALOG_COLLECTIONS_NAME_IDX` |
| List all collections | `(Unbounded, Unbounded)` | `CATALOG_COLLECTIONS` | `PRIMARY_INDEX_SENTINEL` |
| Resolve index name | `encode_key_prefix(&[Scalar::Int64(coll_id), Scalar::String(name)])` | `CATALOG_INDEXES` | `CATALOG_INDEXES_NAME_IDX` |
| List indexes for collection | `encode_key_prefix(&[Scalar::Int64(coll_id)])` → prefix range | `CATALOG_INDEXES` | `CATALOG_INDEXES_NAME_IDX` |

Point intervals use `lower = Included(key)`, `upper = Excluded(successor_key(key))`.
Prefix ranges use `lower = Included(prefix)`, `upper = Excluded(prefix_successor(prefix))`.
Full ranges use `lower = Unbounded`, `upper = Unbounded`.

All intervals have `limit_boundary = None` (catalog reads are always exhaustive).

## Implementation Notes

- `CatalogTracker` is stateless — all methods are free functions (the struct is used for namespacing).
- The `query_id` is passed in by the caller (`Transaction`). For catalog reads that are part of a larger operation (e.g., resolving a collection name before an insert), the catalog read shares the same `query_id` as the data operation. This ensures that if a concurrent DDL invalidates the catalog read, the entire operation (including the data read) is marked as affected.
- For standalone catalog operations (`list_collections`, `list_indexes`), the caller allocates a dedicated `query_id`.

## Catalog Write Encoding

Catalog writes (DDL mutations) are encoded as `IndexDelta` entries on the same pseudo-collections. This happens during `compute_index_deltas` in the commit coordinator (T7 step 4). L6 does NOT need to encode catalog writes manually — the commit coordinator handles this by detecting `CatalogMutation` entries in the write set and producing the corresponding `IndexDelta`/`IndexKeyWrite` entries.

However, L6 needs a helper to compute the catalog key for a DDL mutation so the commit coordinator can produce the `IndexKeyWrite`. This is provided here:

```rust
impl CatalogTracker {
    /// Encode a collection name into a catalog key for conflict detection.
    /// Used by the commit coordinator to produce IndexKeyWrite entries
    /// for CatalogMutation::CreateCollection and DropCollection.
    pub fn encode_collection_name_key(name: &str) -> Vec<u8>;

    /// Encode a (collection_id, index_name) pair into a catalog key.
    /// Used by the commit coordinator for CreateIndex/DropIndex.
    pub fn encode_index_name_key(collection_id: CollectionId, name: &str) -> Vec<u8>;
}
```

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `collection_name_lookup_point_interval` | Correct pseudo-collection, point interval bounds |
| 2 | `list_collections_full_range` | Unbounded interval on PRIMARY_INDEX_SENTINEL |
| 3 | `index_name_lookup_point_interval` | Correct (coll_id, name) encoding |
| 4 | `list_indexes_prefix_range` | Prefix range covers all indexes for the collection |
| 5 | `query_id_preserved` | Intervals carry the provided query_id |
| 6 | `encode_collection_name_key_roundtrip` | Key encoding is deterministic |
| 7 | `encode_index_name_key_roundtrip` | Compound key encoding is deterministic |
| 8 | `point_interval_detects_exact_match` | Key within interval → contains_key = true |
| 9 | `point_interval_rejects_different_name` | Different name → contains_key = false |
| 10 | `prefix_range_covers_all_names_in_collection` | Any index name under the collection prefix matches |
