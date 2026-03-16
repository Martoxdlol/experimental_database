//! B3: IndexResolver implementation for L5.
//!
//! Bridges the L5 `IndexResolver` trait with the L6 `CatalogCache`.
//! Only returns Ready indexes (Building/Dropping are filtered out).

use std::sync::Arc;

use exdb_core::types::CollectionId;
use exdb_tx::IndexInfo;
use parking_lot::RwLock;

use crate::catalog_cache::{CatalogCache, IndexState};

/// L6 implementation of the L5 `IndexResolver` trait.
pub struct IndexResolverImpl {
    catalog: Arc<RwLock<CatalogCache>>,
}

impl IndexResolverImpl {
    pub fn new(catalog: Arc<RwLock<CatalogCache>>) -> Self {
        IndexResolverImpl { catalog }
    }
}

impl exdb_tx::IndexResolver for IndexResolverImpl {
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo> {
        let cache = self.catalog.read();
        cache
            .list_indexes(collection_id)
            .into_iter()
            .filter(|m| m.state == IndexState::Ready)
            .map(|m| IndexInfo {
                index_id: m.index_id,
                collection_id: m.collection_id,
                field_paths: m.field_paths.clone(),
            })
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_cache::{CollectionMeta, IndexMeta, IndexState};
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::IndexId;
    use exdb_tx::IndexResolver;

    fn setup() -> (Arc<RwLock<CatalogCache>>, IndexResolverImpl) {
        let cache = Arc::new(RwLock::new(CatalogCache::new(10, 10)));
        let resolver = IndexResolverImpl::new(Arc::clone(&cache));
        (cache, resolver)
    }

    #[test]
    fn empty_collection() {
        let (cache, resolver) = setup();
        cache.write().add_collection(CollectionMeta {
            collection_id: CollectionId(1),
            name: "users".into(),
            primary_root_page: 10,
            doc_count: 0,
        });
        let result = resolver.indexes_for_collection(CollectionId(1));
        assert!(result.is_empty());
    }

    #[test]
    fn returns_only_ready() {
        let (cache, resolver) = setup();
        let cid = CollectionId(1);
        {
            let mut c = cache.write();
            c.add_collection(CollectionMeta {
                collection_id: cid,
                name: "users".into(),
                primary_root_page: 10,
                doc_count: 0,
            });
            c.add_index(IndexMeta {
                index_id: IndexId(1),
                collection_id: cid,
                name: "email".into(),
                field_paths: vec![FieldPath::single("email")],
                root_page: 20,
                state: IndexState::Ready,
            });
            c.add_index(IndexMeta {
                index_id: IndexId(2),
                collection_id: cid,
                name: "name".into(),
                field_paths: vec![FieldPath::single("name")],
                root_page: 30,
                state: IndexState::Building,
            });
            c.add_index(IndexMeta {
                index_id: IndexId(3),
                collection_id: cid,
                name: "age".into(),
                field_paths: vec![FieldPath::single("age")],
                root_page: 40,
                state: IndexState::Dropping,
            });
        }
        let result = resolver.indexes_for_collection(cid);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_id, IndexId(1));
    }

    #[test]
    fn nonexistent_collection() {
        let (_cache, resolver) = setup();
        let result = resolver.indexes_for_collection(CollectionId(999));
        assert!(result.is_empty());
    }

    #[test]
    fn field_paths_preserved() {
        let (cache, resolver) = setup();
        let cid = CollectionId(1);
        {
            let mut c = cache.write();
            c.add_collection(CollectionMeta {
                collection_id: cid,
                name: "users".into(),
                primary_root_page: 10,
                doc_count: 0,
            });
            c.add_index(IndexMeta {
                index_id: IndexId(1),
                collection_id: cid,
                name: "compound".into(),
                field_paths: vec![
                    FieldPath::new(vec!["a".into(), "b".into()]),
                    FieldPath::single("c"),
                ],
                root_page: 20,
                state: IndexState::Ready,
            });
        }
        let result = resolver.indexes_for_collection(cid);
        assert_eq!(result[0].field_paths.len(), 2);
        assert_eq!(result[0].field_paths[0].segments(), &["a", "b"]);
    }

    #[test]
    fn multiple_ready_indexes() {
        let (cache, resolver) = setup();
        let cid = CollectionId(1);
        {
            let mut c = cache.write();
            c.add_collection(CollectionMeta {
                collection_id: cid,
                name: "users".into(),
                primary_root_page: 10,
                doc_count: 0,
            });
            for i in 1..=5 {
                c.add_index(IndexMeta {
                    index_id: IndexId(i),
                    collection_id: cid,
                    name: format!("idx_{i}"),
                    field_paths: vec![FieldPath::single(&format!("field_{i}"))],
                    root_page: i as u32 * 20,
                    state: IndexState::Ready,
                });
            }
        }
        let result = resolver.indexes_for_collection(cid);
        assert_eq!(result.len(), 5);
    }
}
