//! B9: Catalog persistence — bridges CatalogCache with durable catalog B-trees.
//!
//! All `apply_*` methods are **idempotent** for WAL replay safety: calling them
//! twice with the same arguments produces the same result.


use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};
use exdb_storage::catalog_btree::{
    self, CatalogEntityType, CatalogIndexState, CollectionEntry, IndexEntry, IndexType,
};
use exdb_storage::engine::{BTreeHandle, StorageEngine};

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

/// Bridges the in-memory CatalogCache with durable catalog B-trees.
pub struct CatalogPersistence;

impl CatalogPersistence {
    /// Load the entire catalog from the durable B-trees into a fresh CatalogCache.
    pub async fn load_catalog(
        storage: &StorageEngine,
        id_btree: &BTreeHandle,
        _name_btree: &BTreeHandle,
    ) -> std::io::Result<CatalogCache> {
        let fh = storage.file_header().await;
        let next_coll_id = fh.next_collection_id.get();
        let next_idx_id = fh.next_index_id.get();
        let mut cache = CatalogCache::new(next_coll_id, next_idx_id);

        // Scan all collections from the ID B-tree
        Self::load_collections(id_btree, &mut cache).await?;

        // Scan all indexes from the ID B-tree
        Self::load_indexes(id_btree, &mut cache).await?;

        Ok(cache)
    }

    /// Scan all collections from the catalog ID B-tree.
    async fn load_collections(
        id_btree: &BTreeHandle,
        cache: &mut CatalogCache,
    ) -> std::io::Result<()> {
        use exdb_storage::btree::ScanDirection;
        use std::ops::Bound;
        use tokio_stream::StreamExt;

        let prefix = catalog_btree::collection_id_scan_prefix();
        let upper: [u8; 1] = [CatalogEntityType::Index as u8];
        let stream = id_btree.scan(
            Bound::Included(prefix.as_slice()),
            Bound::Excluded(upper.as_slice()),
            ScanDirection::Forward,
        );
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            let (_key, value) = result?;
            let entry = catalog_btree::deserialize_collection(&value)?;
            cache.add_collection(CollectionMeta {
                collection_id: CollectionId(entry.collection_id),
                name: entry.name,
                primary_root_page: entry.primary_root_page,
                doc_count: entry.doc_count,
            });
            cache.ensure_collection_id_at_least(entry.collection_id + 1);
        }

        Ok(())
    }

    /// Scan all indexes from the catalog ID B-tree.
    async fn load_indexes(
        id_btree: &BTreeHandle,
        cache: &mut CatalogCache,
    ) -> std::io::Result<()> {
        use exdb_storage::btree::ScanDirection;
        use std::ops::Bound;
        use tokio_stream::StreamExt;

        let prefix = catalog_btree::index_id_scan_prefix();
        let upper: [u8; 1] = [CatalogEntityType::Index as u8 + 1];
        let stream = id_btree.scan(
            Bound::Included(prefix.as_slice()),
            Bound::Excluded(upper.as_slice()),
            ScanDirection::Forward,
        );
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            let (_key, value) = result?;
            let entry = catalog_btree::deserialize_index(&value)?;
            let state = match entry.state {
                CatalogIndexState::Building => IndexState::Building,
                CatalogIndexState::Ready => IndexState::Ready,
                CatalogIndexState::Dropping => IndexState::Dropping,
            };
            let field_paths = entry
                .field_paths
                .iter()
                .map(|segments| FieldPath::new(segments.clone()))
                .collect();
            cache.add_index(IndexMeta {
                index_id: IndexId(entry.index_id),
                collection_id: CollectionId(entry.collection_id),
                name: entry.name,
                field_paths,
                root_page: entry.root_page,
                state,
            });
            cache.ensure_index_id_at_least(entry.index_id + 1);
        }

        Ok(())
    }

    // ─── Idempotent Apply Methods ───

    /// Persist a new collection to the catalog B-trees and update the cache.
    /// Idempotent: re-inserting the same collection is safe.
    pub async fn apply_create_collection(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        collection_id: CollectionId,
        name: &str,
        primary_root_page: u32,
    ) -> std::io::Result<()> {
        let entry = CollectionEntry {
            collection_id: collection_id.0,
            name: name.to_string(),
            primary_root_page,
            doc_count: 0,
        };

        // Write to ID B-tree
        let id_key =
            catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, collection_id.0);
        let id_value = catalog_btree::serialize_collection(&entry);
        id_btree.insert(&id_key, &id_value).await?;

        // Write to Name B-tree
        let name_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, name);
        let name_value = catalog_btree::serialize_name_value(collection_id.0);
        name_btree.insert(&name_key, &name_value).await?;

        // Update cache (idempotent — add_collection overwrites)
        cache.add_collection(CollectionMeta {
            collection_id,
            name: name.to_string(),
            primary_root_page,
            doc_count: 0,
        });
        cache.ensure_collection_id_at_least(collection_id.0 + 1);

        Ok(())
    }

    /// Remove a collection from the catalog B-trees and update the cache.
    /// Idempotent: removing a non-existent collection is a no-op.
    pub async fn apply_drop_collection(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        collection_id: CollectionId,
    ) -> std::io::Result<()> {
        // Get name before removal (for name B-tree)
        let name = cache
            .get_collection(collection_id)
            .map(|m| m.name.clone());

        // Remove from ID B-tree
        let id_key =
            catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, collection_id.0);
        id_btree.delete(&id_key).await?;

        // Remove from Name B-tree
        if let Some(name) = &name {
            let name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, name);
            name_btree.delete(&name_key).await?;
        }

        // Remove indexes for this collection from B-trees
        let index_metas: Vec<IndexMeta> = cache.list_indexes(collection_id);
        for idx in &index_metas {
            let idx_id_key =
                catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx.index_id.0);
            id_btree.delete(&idx_id_key).await?;

            let idx_name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &idx.name);
            name_btree.delete(&idx_name_key).await?;
        }

        // Update cache
        cache.remove_collection(collection_id);

        Ok(())
    }

    /// Persist a new index to the catalog B-trees and update the cache.
    /// Idempotent: re-inserting the same index is safe.
    pub async fn apply_create_index(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        meta: &IndexMeta,
    ) -> std::io::Result<()> {
        let state = match meta.state {
            IndexState::Building => CatalogIndexState::Building,
            IndexState::Ready => CatalogIndexState::Ready,
            IndexState::Dropping => CatalogIndexState::Dropping,
        };
        let field_paths: Vec<Vec<String>> = meta
            .field_paths
            .iter()
            .map(|fp| fp.segments().iter().map(|s| s.to_string()).collect())
            .collect();

        let entry = IndexEntry {
            index_id: meta.index_id.0,
            collection_id: meta.collection_id.0,
            name: meta.name.clone(),
            field_paths,
            root_page: meta.root_page,
            state,
            index_type: IndexType::BTree,
            aux_root_pages: vec![],
            config: vec![],
        };

        // Write to ID B-tree
        let id_key =
            catalog_btree::make_catalog_id_key(CatalogEntityType::Index, meta.index_id.0);
        let id_value = catalog_btree::serialize_index(&entry);
        id_btree.insert(&id_key, &id_value).await?;

        // Write to Name B-tree
        let name_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &meta.name);
        let name_value = catalog_btree::serialize_name_value(meta.index_id.0);
        name_btree.insert(&name_key, &name_value).await?;

        // Update cache
        cache.add_index(meta.clone());
        cache.ensure_index_id_at_least(meta.index_id.0 + 1);

        Ok(())
    }

    /// Remove an index from the catalog B-trees and update the cache.
    /// Idempotent: removing a non-existent index is a no-op.
    pub async fn apply_drop_index(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        index_id: IndexId,
    ) -> std::io::Result<()> {
        let name = cache.get_index(index_id).map(|m| m.name.clone());

        // Remove from ID B-tree
        let id_key =
            catalog_btree::make_catalog_id_key(CatalogEntityType::Index, index_id.0);
        id_btree.delete(&id_key).await?;

        // Remove from Name B-tree
        if let Some(name) = &name {
            let name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Index, name);
            name_btree.delete(&name_key).await?;
        }

        // Update cache
        cache.remove_index(index_id);

        Ok(())
    }

    /// Mark an index as Ready in the catalog B-trees and update the cache.
    /// Idempotent.
    pub async fn apply_index_ready(
        id_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        index_id: IndexId,
    ) -> std::io::Result<()> {
        // Update in cache first
        cache.set_index_state(index_id, IndexState::Ready);

        // Re-serialize the full index entry to the ID B-tree
        if let Some(meta) = cache.get_index(index_id) {
            let field_paths: Vec<Vec<String>> = meta
                .field_paths
                .iter()
                .map(|fp| fp.segments().iter().map(|s| s.to_string()).collect())
                .collect();
            let entry = IndexEntry {
                index_id: meta.index_id.0,
                collection_id: meta.collection_id.0,
                name: meta.name.clone(),
                field_paths,
                root_page: meta.root_page,
                state: CatalogIndexState::Ready,
                index_type: IndexType::BTree,
                aux_root_pages: vec![],
                config: vec![],
            };

            let id_key =
                catalog_btree::make_catalog_id_key(CatalogEntityType::Index, index_id.0);
            let id_value = catalog_btree::serialize_index(&entry);
            id_btree.insert(&id_key, &id_value).await?;
        }

        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use exdb_storage::engine::StorageConfig;

    async fn setup() -> (Arc<StorageEngine>, BTreeHandle, BTreeHandle) {
        let storage =
            Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let fh = storage.file_header().await;
        let id_btree = storage.open_btree(fh.catalog_root_page.get());
        let name_btree = storage.open_btree(fh.catalog_name_root_page.get());
        (storage, id_btree, name_btree)
    }

    #[tokio::test]
    async fn load_empty_catalog() {
        let (storage, id_btree, name_btree) = setup().await;
        let cache =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert!(cache.list_collections().is_empty());
    }

    #[tokio::test]
    async fn create_and_load_collection() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        // Verify in cache
        assert!(cache.has_collection("users"));

        // Reload from B-trees
        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        let coll = reloaded.get_collection_by_name("users").unwrap();
        assert_eq!(coll.collection_id, CollectionId(1));
        assert_eq!(coll.primary_root_page, 10);
    }

    #[tokio::test]
    async fn create_collection_idempotent() {
        let (_storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        for _ in 0..3 {
            CatalogPersistence::apply_create_collection(
                &id_btree,
                &name_btree,
                &mut cache,
                CollectionId(1),
                "users",
                10,
            )
            .await
            .unwrap();
        }

        assert_eq!(cache.list_collections().len(), 1);
    }

    #[tokio::test]
    async fn drop_collection() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        CatalogPersistence::apply_drop_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
        )
        .await
        .unwrap();

        assert!(!cache.has_collection("users"));

        // Reload — should be empty
        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert!(reloaded.list_collections().is_empty());
    }

    #[tokio::test]
    async fn drop_collection_idempotent() {
        let (_storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        // Drop twice
        for _ in 0..2 {
            CatalogPersistence::apply_drop_collection(
                &id_btree,
                &name_btree,
                &mut cache,
                CollectionId(1),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn create_and_load_index() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Building,
        };
        CatalogPersistence::apply_create_index(
            &id_btree,
            &name_btree,
            &mut cache,
            &meta,
        )
        .await
        .unwrap();

        // Reload
        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        let idx = reloaded.get_index(IndexId(1)).unwrap();
        assert_eq!(idx.name, "email_idx");
        assert_eq!(idx.state, IndexState::Building);
    }

    #[tokio::test]
    async fn apply_index_ready() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Building,
        };
        CatalogPersistence::apply_create_index(
            &id_btree,
            &name_btree,
            &mut cache,
            &meta,
        )
        .await
        .unwrap();

        CatalogPersistence::apply_index_ready(&id_btree, &mut cache, IndexId(1))
            .await
            .unwrap();

        assert_eq!(
            cache.get_index(IndexId(1)).unwrap().state,
            IndexState::Ready
        );

        // Reload — should be Ready
        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert_eq!(
            reloaded.get_index(IndexId(1)).unwrap().state,
            IndexState::Ready
        );
    }

    #[tokio::test]
    async fn drop_index() {
        let (_storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(
            &id_btree,
            &name_btree,
            &mut cache,
            &meta,
        )
        .await
        .unwrap();

        CatalogPersistence::apply_drop_index(
            &id_btree,
            &name_btree,
            &mut cache,
            IndexId(1),
        )
        .await
        .unwrap();

        assert!(cache.get_index(IndexId(1)).is_none());
    }

    #[tokio::test]
    async fn drop_collection_cascades_indexes() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
            "users",
            10,
        )
        .await
        .unwrap();

        for i in 1..=3 {
            let meta = IndexMeta {
                index_id: IndexId(i),
                collection_id: CollectionId(1),
                name: format!("idx_{i}"),
                field_paths: vec![FieldPath::single(&format!("f{i}"))],
                root_page: i as u32 * 20,
                state: IndexState::Ready,
            };
            CatalogPersistence::apply_create_index(
                &id_btree,
                &name_btree,
                &mut cache,
                &meta,
            )
            .await
            .unwrap();
        }

        CatalogPersistence::apply_drop_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(1),
        )
        .await
        .unwrap();

        // Reload — all gone
        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert!(reloaded.list_collections().is_empty());
        assert!(reloaded.get_index(IndexId(1)).is_none());
    }

    #[tokio::test]
    async fn id_allocators_updated_on_load() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(42),
            "users",
            10,
        )
        .await
        .unwrap();

        let meta = IndexMeta {
            index_id: IndexId(99),
            collection_id: CollectionId(42),
            name: "idx".to_string(),
            field_paths: vec![FieldPath::single("f")],
            root_page: 20,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(
            &id_btree,
            &name_btree,
            &mut cache,
            &meta,
        )
        .await
        .unwrap();

        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert!(reloaded.next_collection_id() >= 43);
        assert!(reloaded.next_index_id() >= 100);
    }

    #[tokio::test]
    async fn multiple_collections() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        for i in 1..=5 {
            CatalogPersistence::apply_create_collection(
                &id_btree,
                &name_btree,
                &mut cache,
                CollectionId(i),
                &format!("col_{i}"),
                i as u32 * 10,
            )
            .await
            .unwrap();
        }

        let reloaded =
            CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
                .await
                .unwrap();
        assert_eq!(reloaded.list_collections().len(), 5);
    }
}
