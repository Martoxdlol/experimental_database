//! CatalogMutationHandler implementation for L5 commit protocol.
//!
//! Implements the L5 `CatalogMutationHandler` trait to allocate B-tree pages
//! and apply catalog mutations during the commit step sequence.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_tx::CatalogMutationHandler;
use parking_lot::RwLock;

use crate::catalog_cache::{CatalogCache, IndexMeta, IndexState};
use crate::catalog_persistence::CatalogPersistence;

/// L6 implementation of the L5 CatalogMutationHandler trait.
pub struct CatalogMutationHandlerImpl {
    storage: Arc<StorageEngine>,
    catalog: Arc<RwLock<CatalogCache>>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    catalog_id_btree: BTreeHandle,
    catalog_name_btree: BTreeHandle,
}

impl CatalogMutationHandlerImpl {
    pub fn new(
        storage: Arc<StorageEngine>,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        catalog_id_btree: BTreeHandle,
        catalog_name_btree: BTreeHandle,
    ) -> Self {
        Self {
            storage,
            catalog,
            primary_indexes,
            secondary_indexes,
            catalog_id_btree,
            catalog_name_btree,
        }
    }
}

#[async_trait(?Send)]
impl CatalogMutationHandler for CatalogMutationHandlerImpl {
    /// Allocate B-tree pages for a new collection (primary + _created_at index).
    async fn allocate_collection_pages(&self) -> std::io::Result<(u32, u32)> {
        let primary_btree = self.storage.create_btree().await?;
        let created_at_btree = self.storage.create_btree().await?;
        Ok((primary_btree.root_page(), created_at_btree.root_page()))
    }

    /// Allocate a B-tree page for a new secondary index.
    async fn allocate_index_page(&self) -> std::io::Result<u32> {
        let btree = self.storage.create_btree().await?;
        Ok(btree.root_page())
    }

    /// Apply a CreateCollection mutation: persist to catalog B-trees, update
    /// cache, create PrimaryIndex handle.
    async fn apply_create_collection(
        &self,
        provisional_id: CollectionId,
        name: &str,
        primary_root_page: u32,
        created_at_root_page: u32,
    ) -> std::io::Result<()> {
        // Persist to catalog B-trees + update cache
        {
            let mut cache = self.catalog.write();
            CatalogPersistence::apply_create_collection(
                &self.catalog_id_btree,
                &self.catalog_name_btree,
                &mut cache,
                provisional_id,
                name,
                primary_root_page,
            )
            .await?;
        }

        // Create PrimaryIndex handle
        let btree_handle = self.storage.open_btree(primary_root_page);
        let external_threshold = self.storage.config().page_size / 4;
        let primary = Arc::new(PrimaryIndex::new(
            btree_handle,
            Arc::clone(&self.storage),
            external_threshold,
        ));
        self.primary_indexes
            .write()
            .insert(provisional_id, primary);

        // Create _created_at secondary index handle + metadata
        let created_at_btree = self.storage.open_btree(created_at_root_page);
        let primary_ref = self.primary_indexes.read().get(&provisional_id).cloned();
        if let Some(primary_ref) = primary_ref {
            let secondary = Arc::new(SecondaryIndex::new(created_at_btree, primary_ref));
            let idx_id = self.catalog.read().allocate_index_id();
            self.secondary_indexes.write().insert(idx_id, secondary);

            let mut cache = self.catalog.write();
            let meta = IndexMeta {
                index_id: idx_id,
                collection_id: provisional_id,
                name: "_created_at".to_string(),
                field_paths: vec![FieldPath::single("_created_at")],
                root_page: created_at_root_page,
                state: IndexState::Ready,
            };
            CatalogPersistence::apply_create_index(
                &self.catalog_id_btree,
                &self.catalog_name_btree,
                &mut cache,
                &meta,
            )
            .await?;
        }

        Ok(())
    }

    /// Apply a DropCollection mutation.
    async fn apply_drop_collection(
        &self,
        collection_id: CollectionId,
    ) -> std::io::Result<()> {
        // Get index IDs to remove handles
        let index_ids: Vec<IndexId> = {
            let cache = self.catalog.read();
            cache
                .list_indexes(collection_id)
                .iter()
                .map(|m| m.index_id)
                .collect()
        };

        // Remove index handles
        {
            let mut sec = self.secondary_indexes.write();
            for idx_id in &index_ids {
                sec.remove(idx_id);
            }
        }

        // Remove primary handle
        self.primary_indexes.write().remove(&collection_id);

        // Persist to catalog B-trees + update cache
        let mut cache = self.catalog.write();
        CatalogPersistence::apply_drop_collection(
            &self.catalog_id_btree,
            &self.catalog_name_btree,
            &mut cache,
            collection_id,
        )
        .await?;

        Ok(())
    }

    /// Apply a CreateIndex mutation.
    async fn apply_create_index(
        &self,
        provisional_id: IndexId,
        collection_id: CollectionId,
        name: &str,
        field_paths: &[FieldPath],
        root_page: u32,
    ) -> std::io::Result<()> {
        // Create SecondaryIndex handle
        let btree_handle = self.storage.open_btree(root_page);
        let primary = self
            .primary_indexes
            .read()
            .get(&collection_id)
            .cloned();
        if let Some(primary) = primary {
            let secondary = Arc::new(SecondaryIndex::new(btree_handle, primary));
            self.secondary_indexes
                .write()
                .insert(provisional_id, secondary);
        }

        // Persist to catalog B-trees + update cache
        let meta = IndexMeta {
            index_id: provisional_id,
            collection_id,
            name: name.to_string(),
            field_paths: field_paths.to_vec(),
            root_page,
            state: IndexState::Building,
        };
        let mut cache = self.catalog.write();
        CatalogPersistence::apply_create_index(
            &self.catalog_id_btree,
            &self.catalog_name_btree,
            &mut cache,
            &meta,
        )
        .await?;

        Ok(())
    }

    /// Apply a DropIndex mutation.
    async fn apply_drop_index(&self, index_id: IndexId) -> std::io::Result<()> {
        // Remove handle
        self.secondary_indexes.write().remove(&index_id);

        // Persist to catalog B-trees + update cache
        let mut cache = self.catalog.write();
        CatalogPersistence::apply_drop_index(
            &self.catalog_id_btree,
            &self.catalog_name_btree,
            &mut cache,
            index_id,
        )
        .await?;

        Ok(())
    }
}
