//! B3: IndexResolver implementation over CatalogCache.
//!
//! Bridges L5's `IndexResolver` trait with L6's `CatalogCache`.

use std::sync::Arc;

use parking_lot::RwLock;

use exdb_core::types::CollectionId;
use exdb_tx::write_set::{IndexInfo, IndexResolver};

use crate::catalog_cache::CatalogCache;

/// L6 implementation of L5's `IndexResolver` trait.
///
/// Only returns `Ready` indexes — `Building` and `Dropping` indexes are
/// excluded from delta computation.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_cache::{CollectionMeta, IndexMeta, IndexState};
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::IndexId;

    fn make_cache() -> Arc<RwLock<CatalogCache>> {
        let mut cache = CatalogCache::new();
        cache.add_collection(CollectionMeta {
            collection_id: CollectionId(10),
            name: "users".into(),
            primary_root_page: 0,
            doc_count: 0,
        });
        Arc::new(RwLock::new(cache))
    }

    #[test]
    fn empty_collection_returns_empty() {
        let catalog = make_cache();
        let resolver = IndexResolverImpl::new(catalog);
        let result = resolver.indexes_for_collection(CollectionId(10));
        assert!(result.is_empty());
    }

    #[test]
    fn returns_ready_indexes_only() {
        let catalog = make_cache();
        {
            let mut cache = catalog.write();
            cache.add_index(IndexMeta {
                index_id: IndexId(20),
                collection_id: CollectionId(10),
                name: "ready".into(),
                field_paths: vec![FieldPath::single("email")],
                root_page: 0,
                state: IndexState::Ready,
            });
            cache.add_index(IndexMeta {
                index_id: IndexId(21),
                collection_id: CollectionId(10),
                name: "building".into(),
                field_paths: vec![FieldPath::single("age")],
                root_page: 0,
                state: IndexState::Building,
            });
            cache.add_index(IndexMeta {
                index_id: IndexId(22),
                collection_id: CollectionId(10),
                name: "dropping".into(),
                field_paths: vec![FieldPath::single("name")],
                root_page: 0,
                state: IndexState::Dropping,
            });
        }
        let resolver = IndexResolverImpl::new(catalog);
        let result = resolver.indexes_for_collection(CollectionId(10));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_id, IndexId(20));
    }

    #[test]
    fn returns_correct_field_paths() {
        let catalog = make_cache();
        {
            let mut cache = catalog.write();
            cache.add_index(IndexMeta {
                index_id: IndexId(20),
                collection_id: CollectionId(10),
                name: "compound".into(),
                field_paths: vec![FieldPath::single("a"), FieldPath::single("b")],
                root_page: 0,
                state: IndexState::Ready,
            });
        }
        let resolver = IndexResolverImpl::new(catalog);
        let result = resolver.indexes_for_collection(CollectionId(10));
        assert_eq!(result[0].field_paths.len(), 2);
    }

    #[test]
    fn multiple_indexes_for_collection() {
        let catalog = make_cache();
        {
            let mut cache = catalog.write();
            for i in 0..5 {
                cache.add_index(IndexMeta {
                    index_id: IndexId(20 + i),
                    collection_id: CollectionId(10),
                    name: format!("idx_{i}"),
                    field_paths: vec![FieldPath::single("f")],
                    root_page: 0,
                    state: IndexState::Ready,
                });
            }
        }
        let resolver = IndexResolverImpl::new(catalog);
        let result = resolver.indexes_for_collection(CollectionId(10));
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn different_collections_isolated() {
        let catalog = make_cache();
        {
            let mut cache = catalog.write();
            cache.add_collection(CollectionMeta {
                collection_id: CollectionId(11),
                name: "orders".into(),
                primary_root_page: 0,
                doc_count: 0,
            });
            cache.add_index(IndexMeta {
                index_id: IndexId(20),
                collection_id: CollectionId(10),
                name: "user_idx".into(),
                field_paths: vec![FieldPath::single("email")],
                root_page: 0,
                state: IndexState::Ready,
            });
            cache.add_index(IndexMeta {
                index_id: IndexId(21),
                collection_id: CollectionId(11),
                name: "order_idx".into(),
                field_paths: vec![FieldPath::single("status")],
                root_page: 0,
                state: IndexState::Ready,
            });
        }
        let resolver = IndexResolverImpl::new(catalog);
        let users = resolver.indexes_for_collection(CollectionId(10));
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].index_id, IndexId(20));

        let orders = resolver.indexes_for_collection(CollectionId(11));
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].index_id, IndexId(21));
    }
}
