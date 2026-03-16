//! L6 implementation of L5's `CatalogMutationHandler` trait.
//!
//! Called by the `CommitCoordinator` during step 4a to create B-trees,
//! update the catalog cache, and register index handles when DDL commits.

use std::collections::HashMap;
use std::sync::Arc;

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_tx::CatalogMutationHandler;
use parking_lot::RwLock;

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};
use crate::catalog_persistence;

/// L6 implementation of L5's `CatalogMutationHandler`.
///
/// Holds shared references to all the state it needs to update:
/// storage (for B-tree creation), catalog cache, index handle maps,
/// and the persistent catalog B-tree.
pub(crate) struct CatalogMutationHandlerImpl {
    storage: Arc<StorageEngine>,
    catalog: Arc<RwLock<CatalogCache>>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    /// The catalog B-tree (rooted at FileHeader::catalog_root_page).
    /// `None` for in-memory databases (no persistence needed).
    catalog_btree: Option<Arc<BTreeHandle>>,
}

impl CatalogMutationHandlerImpl {
    pub fn new(
        storage: Arc<StorageEngine>,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        catalog_btree: Option<Arc<BTreeHandle>>,
    ) -> Self {
        Self {
            storage,
            catalog,
            primary_indexes,
            secondary_indexes,
            catalog_btree,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl CatalogMutationHandler for CatalogMutationHandlerImpl {
    async fn handle_create_collection(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> std::io::Result<()> {
        // Create primary B-tree
        let btree = self.storage.create_btree().await?;
        let root_page = btree.root_page();

        let primary = Arc::new(PrimaryIndex::new(
            btree,
            Arc::clone(&self.storage),
            self.storage.config().page_size / 4,
        ));

        // Register in index map
        self.primary_indexes
            .write()
            .insert(collection_id, Arc::clone(&primary));

        // Update catalog cache (scoped to drop guard before any .await)
        {
            let mut catalog = self.catalog.write();
            if catalog.get_collection_by_id(collection_id).is_none() {
                catalog.add_collection(CollectionMeta {
                    collection_id,
                    name: name.to_string(),
                    primary_root_page: root_page,
                    doc_count: 0,
                });
            }
        }

        // Persist to catalog B-tree
        if let Some(cat_btree) = &self.catalog_btree {
            catalog_persistence::write_collection(cat_btree, collection_id, name, root_page)
                .await?;
            self.storage.update_file_header(|fh| {
                let current = fh.next_collection_id.get();
                if collection_id.0 >= current {
                    fh.next_collection_id.set(collection_id.0 + 1);
                }
            }).await?;
        }

        Ok(())
    }

    async fn handle_drop_collection(
        &self,
        collection_id: CollectionId,
    ) -> std::io::Result<()> {
        // Remove from catalog + index handles (scoped to avoid !Send across .await)
        let indexes: Vec<IndexId> = {
            let mut catalog = self.catalog.write();
            let ids: Vec<IndexId> = catalog
                .list_indexes(collection_id)
                .iter()
                .map(|m| m.index_id)
                .collect();
            catalog.remove_collection(collection_id);
            ids
        };

        {
            self.primary_indexes.write().remove(&collection_id);
            let mut sec = self.secondary_indexes.write();
            for idx_id in &indexes {
                sec.remove(idx_id);
            }
        }

        // Remove from persistent catalog
        if let Some(cat_btree) = &self.catalog_btree {
            catalog_persistence::remove_collection(cat_btree, collection_id).await?;
            for idx_id in &indexes {
                catalog_persistence::remove_index(cat_btree, *idx_id).await?;
            }
        }

        Ok(())
    }

    async fn handle_create_index(
        &self,
        index_id: IndexId,
        collection_id: CollectionId,
        name: &str,
        field_paths: &[FieldPath],
    ) -> std::io::Result<()> {
        // Create secondary B-tree
        let btree = self.storage.create_btree().await?;
        let root_page = btree.root_page();

        // Get primary + register secondary (scoped to avoid !Send across .await)
        {
            let primary = {
                let primaries = self.primary_indexes.read();
                primaries.get(&collection_id).cloned()
            };
            if let Some(primary) = primary {
                let secondary = Arc::new(SecondaryIndex::new(btree, primary));
                self.secondary_indexes.write().insert(index_id, secondary);
            }
        }

        // Update catalog cache
        {
            let mut catalog = self.catalog.write();
            if catalog.get_index_by_id(index_id).is_none() {
                catalog.add_index(IndexMeta {
                    index_id,
                    collection_id,
                    name: name.to_string(),
                    field_paths: field_paths.to_vec(),
                    root_page,
                    state: IndexState::Ready,
                });
            }
        }

        // Persist to catalog B-tree
        if let Some(cat_btree) = &self.catalog_btree {
            catalog_persistence::write_index(
                cat_btree, index_id, collection_id, name, field_paths, root_page,
            ).await?;
            self.storage.update_file_header(|fh| {
                let current = fh.next_index_id.get();
                if index_id.0 >= current {
                    fh.next_index_id.set(index_id.0 + 1);
                }
            }).await?;
        }

        Ok(())
    }

    async fn handle_drop_index(
        &self,
        index_id: IndexId,
    ) -> std::io::Result<()> {
        {
            self.catalog.write().remove_index(index_id);
            self.secondary_indexes.write().remove(&index_id);
        }

        // Remove from persistent catalog
        if let Some(cat_btree) = &self.catalog_btree {
            catalog_persistence::remove_index(cat_btree, index_id).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::StorageConfig;

    async fn make_handler() -> (
        CatalogMutationHandlerImpl,
        Arc<RwLock<CatalogCache>>,
        Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    ) {
        let storage = Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        );
        let catalog = Arc::new(RwLock::new(CatalogCache::new()));
        let primaries = Arc::new(RwLock::new(HashMap::new()));
        let secondaries = Arc::new(RwLock::new(HashMap::new()));
        let handler = CatalogMutationHandlerImpl::new(
            storage,
            Arc::clone(&catalog),
            Arc::clone(&primaries),
            Arc::clone(&secondaries),
            None, // no persistent catalog for tests
        );
        (handler, catalog, primaries, secondaries)
    }

    #[tokio::test]
    async fn create_collection_creates_primary_index() {
        let (handler, catalog, primaries, _) = make_handler().await;
        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();

        assert!(primaries.read().contains_key(&CollectionId(10)));
        assert!(catalog.read().get_collection_by_name("users").is_some());
        assert_eq!(
            catalog.read().get_collection_by_id(CollectionId(10)).unwrap().name,
            "users"
        );
    }

    #[tokio::test]
    async fn create_index_creates_secondary_index() {
        let (handler, catalog, _, secondaries) = make_handler().await;

        // Must create collection first
        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        handler
            .handle_create_index(
                IndexId(20),
                CollectionId(10),
                "_created_at",
                &[FieldPath::single("_created_at")],
            )
            .await
            .unwrap();

        assert!(secondaries.read().contains_key(&IndexId(20)));
        assert!(catalog.read().get_index_by_id(IndexId(20)).is_some());
    }

    #[tokio::test]
    async fn drop_collection_removes_everything() {
        let (handler, catalog, primaries, secondaries) = make_handler().await;

        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        handler
            .handle_create_index(
                IndexId(20),
                CollectionId(10),
                "by_email",
                &[FieldPath::single("email")],
            )
            .await
            .unwrap();

        handler.handle_drop_collection(CollectionId(10)).await.unwrap();

        assert!(!primaries.read().contains_key(&CollectionId(10)));
        assert!(!secondaries.read().contains_key(&IndexId(20)));
        assert!(catalog.read().get_collection_by_name("users").is_none());
        assert!(catalog.read().get_index_by_id(IndexId(20)).is_none());
    }

    #[tokio::test]
    async fn drop_index_removes_secondary() {
        let (handler, catalog, _, secondaries) = make_handler().await;

        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        handler
            .handle_create_index(
                IndexId(20),
                CollectionId(10),
                "by_email",
                &[FieldPath::single("email")],
            )
            .await
            .unwrap();

        handler.handle_drop_index(IndexId(20)).await.unwrap();

        assert!(!secondaries.read().contains_key(&IndexId(20)));
        assert!(catalog.read().get_index_by_id(IndexId(20)).is_none());
        // Collection still exists
        assert!(catalog.read().get_collection_by_name("users").is_some());
    }

    #[tokio::test]
    async fn create_collection_idempotent() {
        let (handler, catalog, _, _) = make_handler().await;

        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        // Second call should not panic (idempotent check)
        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();

        assert_eq!(catalog.read().collection_count(), 1);
    }

    #[tokio::test]
    async fn create_multiple_collections() {
        let (handler, catalog, primaries, _) = make_handler().await;

        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        handler
            .handle_create_collection(CollectionId(11), "orders")
            .await
            .unwrap();

        assert_eq!(primaries.read().len(), 2);
        assert_eq!(catalog.read().collection_count(), 2);
    }

    #[tokio::test]
    async fn create_multiple_indexes() {
        let (handler, catalog, _, secondaries) = make_handler().await;

        handler
            .handle_create_collection(CollectionId(10), "users")
            .await
            .unwrap();
        handler
            .handle_create_index(
                IndexId(20),
                CollectionId(10),
                "_created_at",
                &[FieldPath::single("_created_at")],
            )
            .await
            .unwrap();
        handler
            .handle_create_index(
                IndexId(21),
                CollectionId(10),
                "by_email",
                &[FieldPath::single("email")],
            )
            .await
            .unwrap();

        assert_eq!(secondaries.read().len(), 2);
        assert_eq!(catalog.read().index_count(), 2);
    }
}
