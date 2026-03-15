# B3: Index Resolver

## Purpose

Implements the L5-defined `IndexResolver` trait over `CatalogCache`. This bridges the dependency inversion: L5 defines the trait (so it doesn't depend on L6), and L6 provides the implementation (reading from its catalog cache).

Used by `compute_index_deltas` during commit step 4 to look up which indexes exist for a collection and extract their field paths for delta computation.

## Dependencies

- **B2 (`catalog_cache.rs`)**: `CatalogCache`, `IndexMeta`, `IndexState`
- **L5 (`exdb-tx`)**: `IndexResolver` trait, `IndexInfo`

## Rust Types

```rust
use std::sync::Arc;
use parking_lot::RwLock;
use exdb_tx::{IndexResolver, IndexInfo};
use crate::catalog_cache::CatalogCache;
use exdb_core::types::CollectionId;

/// L6 implementation of L5's `IndexResolver` trait.
///
/// Reads from the shared `CatalogCache` to provide index metadata
/// during commit-time delta computation. Only returns `Ready` indexes —
/// `Building` and `Dropping` indexes are excluded from delta computation
/// since they are handled separately by the index builder / vacuum.
pub struct IndexResolverImpl {
    catalog: Arc<RwLock<CatalogCache>>,
}

impl IndexResolverImpl {
    pub fn new(catalog: Arc<RwLock<CatalogCache>>) -> Self {
        Self { catalog }
    }
}

impl IndexResolver for IndexResolverImpl {
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo> {
        let cache = self.catalog.read();
        cache
            .ready_indexes(collection_id)
            .into_iter()
            .map(|meta| IndexInfo {
                index_id: meta.index_id,
                collection_id: meta.collection_id,
                field_paths: meta.field_paths.clone(),
            })
            .collect()
    }
}
```

## Implementation Notes

- The `RwLock::read()` guard is held briefly — only for the duration of the iterator. No async work happens while the lock is held.
- `Building` indexes are excluded because their entries are populated by the background index builder, not by the commit coordinator.
- `Dropping` indexes are excluded because their entries are being cleaned up by vacuum.
- The `_created_at` index IS included (it has state `Ready`) — deltas are computed for it like any other secondary index.

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `empty_collection_returns_empty` | No indexes → empty vec |
| 2 | `returns_ready_indexes_only` | Building/Dropping filtered out |
| 3 | `returns_correct_field_paths` | IndexInfo has correct field_paths |
| 4 | `multiple_indexes_for_collection` | All ready indexes returned |
| 5 | `different_collections_isolated` | Indexes from other collections not returned |
