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

        Self::validate_catalog(&cache)?;

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
    async fn load_indexes(id_btree: &BTreeHandle, cache: &mut CatalogCache) -> std::io::Result<()> {
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

    fn validate_catalog(cache: &CatalogCache) -> std::io::Result<()> {
        for idx in cache.list_all_indexes() {
            if cache.get_collection(idx.collection_id).is_none() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "catalog index {} references missing collection {}",
                        idx.index_id.0, idx.collection_id.0
                    ),
                ));
            }
        }

        for coll in cache.list_collections() {
            let Some(created_at) = cache.get_index_by_name(coll.collection_id, "_created_at")
            else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "collection {} is missing required _created_at index",
                        coll.collection_id.0
                    ),
                ));
            };

            if created_at.state != IndexState::Ready
                || created_at.field_paths != vec![FieldPath::single("_created_at")]
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "collection {} has invalid _created_at index",
                        coll.collection_id.0
                    ),
                ));
            }
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
        let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, name);
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
        let name = cache.get_collection(collection_id).map(|m| m.name.clone());

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
                catalog_btree::make_catalog_index_name_key(idx.collection_id.0, &idx.name);
            name_btree.delete(&idx_name_key).await?;
            let legacy_idx_name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &idx.name);
            name_btree.delete(&legacy_idx_name_key).await?;
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
        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, meta.index_id.0);
        let id_value = catalog_btree::serialize_index(&entry);
        id_btree.insert(&id_key, &id_value).await?;

        // Write to Name B-tree. Index names are scoped by collection.
        let name_key = catalog_btree::make_catalog_index_name_key(meta.collection_id.0, &meta.name);
        let name_value = catalog_btree::serialize_name_value(meta.index_id.0);
        name_btree.insert(&name_key, &name_value).await?;
        let legacy_name_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &meta.name);
        name_btree.delete(&legacy_name_key).await?;

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
        let meta = cache.get_index(index_id).cloned();

        // Remove from ID B-tree
        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, index_id.0);
        id_btree.delete(&id_key).await?;

        // Remove from Name B-tree
        if let Some(meta) = &meta {
            let name_key =
                catalog_btree::make_catalog_index_name_key(meta.collection_id.0, &meta.name);
            name_btree.delete(&name_key).await?;
            let legacy_name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &meta.name);
            name_btree.delete(&legacy_name_key).await?;
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
            Self::apply_index_ready_btree(id_btree, meta).await?;
        }

        Ok(())
    }

    /// Write the Ready state to the durable B-tree only (no cache update).
    /// Used by the background index builder where the cache update must happen
    /// outside the async call to avoid holding a parking_lot guard across .await.
    pub async fn apply_index_ready_btree(
        id_btree: &BTreeHandle,
        meta: &IndexMeta,
    ) -> std::io::Result<()> {
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

        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, meta.index_id.0);
        let id_value = catalog_btree::serialize_index(&entry);
        id_btree.insert(&id_key, &id_value).await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::StorageConfig;
    use std::sync::Arc;

    async fn setup() -> (Arc<StorageEngine>, BTreeHandle, BTreeHandle) {
        let storage = Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        );
        let fh = storage.file_header().await;
        let id_btree = storage.open_btree(fh.catalog_root_page.get());
        let name_btree = storage.open_btree(fh.catalog_name_root_page.get());
        (storage, id_btree, name_btree)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_collection_with_created_at(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        collection_id: u64,
        name: &str,
        primary_root_page: u32,
        created_at_index_id: u64,
        created_at_root_page: u32,
    ) {
        CatalogPersistence::apply_create_collection(
            id_btree,
            name_btree,
            cache,
            CollectionId(collection_id),
            name,
            primary_root_page,
        )
        .await
        .unwrap();

        let created_at = IndexMeta {
            index_id: IndexId(created_at_index_id),
            collection_id: CollectionId(collection_id),
            name: "_created_at".to_string(),
            field_paths: vec![FieldPath::single("_created_at")],
            root_page: created_at_root_page,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(id_btree, name_btree, cache, &created_at)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_empty_catalog() {
        let (storage, id_btree, name_btree) = setup().await;
        let cache = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert!(cache.list_collections().is_empty());
    }

    #[tokio::test]
    async fn create_and_load_collection() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            20,
        )
        .await;

        // Verify in cache
        assert!(cache.has_collection("users"));

        // Reload from B-trees
        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        let coll = reloaded.get_collection_by_name("users").unwrap();
        assert_eq!(coll.collection_id, CollectionId(1));
        assert_eq!(coll.primary_root_page, 10);
        assert!(
            reloaded
                .get_index_by_name(CollectionId(1), "_created_at")
                .is_some()
        );
    }

    #[tokio::test]
    async fn create_updates_name_btree() {
        let (_storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        CatalogPersistence::apply_create_collection(
            &id_btree,
            &name_btree,
            &mut cache,
            CollectionId(7),
            "users",
            10,
        )
        .await
        .unwrap();

        let key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
        let value = name_btree.get(&key).await.unwrap().unwrap();
        assert_eq!(catalog_btree::deserialize_name_value(&value).unwrap(), 7);
    }

    #[tokio::test]
    async fn load_unicode_collection_name() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "datos_espanoles",
            10,
            2,
            20,
        )
        .await;

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        let coll = reloaded.get_collection_by_name("datos_espanoles").unwrap();
        assert_eq!(coll.collection_id, CollectionId(1));
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
    async fn load_validates_created_at_exists() {
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

        let err = match CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree).await {
            Ok(_) => panic!("catalog missing _created_at should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("missing required _created_at"));
    }

    #[tokio::test]
    async fn load_validates_index_references_collection() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);
        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(99),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Ready,
        };

        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        let err = match CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree).await {
            Ok(_) => panic!("index referencing missing collection should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("references missing collection"));
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
        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
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

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Building,
        };
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        // Reload
        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        let idx = reloaded.get_index(IndexId(1)).unwrap();
        assert_eq!(idx.name, "email_idx");
        assert_eq!(idx.state, IndexState::Building);
    }

    #[tokio::test]
    async fn create_index_idempotent() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Ready,
        };

        for _ in 0..3 {
            CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
                .await
                .unwrap();
        }

        let email_indexes = cache
            .list_indexes(CollectionId(1))
            .into_iter()
            .filter(|idx| idx.name == "email_idx")
            .count();
        assert_eq!(email_indexes, 1);

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert_eq!(
            reloaded
                .list_indexes(CollectionId(1))
                .into_iter()
                .filter(|idx| idx.name == "email_idx")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn drop_index_idempotent() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        for _ in 0..2 {
            CatalogPersistence::apply_drop_index(&id_btree, &name_btree, &mut cache, IndexId(1))
                .await
                .unwrap();
        }

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert!(reloaded.get_index(IndexId(1)).is_none());
        assert!(
            reloaded
                .get_index_by_name(CollectionId(1), "_created_at")
                .is_some()
        );

        let scoped_key = catalog_btree::make_catalog_index_name_key(1, "email_idx");
        assert!(name_btree.get(&scoped_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_compound_index_fields() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "address_idx".to_string(),
            field_paths: vec![
                FieldPath::new(vec!["address".to_string(), "city".to_string()]),
                FieldPath::new(vec!["address".to_string(), "zip".to_string()]),
            ],
            root_page: 20,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert_eq!(
            reloaded.get_index(IndexId(1)).unwrap().field_paths,
            meta.field_paths
        );
    }

    #[tokio::test]
    async fn index_name_entries_are_scoped_by_collection() {
        let (_storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        for (collection_id, collection_name) in [(1, "users"), (2, "admins")] {
            CatalogPersistence::apply_create_collection(
                &id_btree,
                &name_btree,
                &mut cache,
                CollectionId(collection_id),
                collection_name,
                collection_id as u32 * 10,
            )
            .await
            .unwrap();

            let meta = IndexMeta {
                index_id: IndexId(collection_id),
                collection_id: CollectionId(collection_id),
                name: "email_idx".to_string(),
                field_paths: vec![FieldPath::single("email")],
                root_page: collection_id as u32 * 20,
                state: IndexState::Ready,
            };
            CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
                .await
                .unwrap();
        }

        for collection_id in [1, 2] {
            let key = catalog_btree::make_catalog_index_name_key(collection_id, "email_idx");
            let value = name_btree.get(&key).await.unwrap().unwrap();
            assert_eq!(
                catalog_btree::deserialize_name_value(&value).unwrap(),
                collection_id
            );
        }

        let legacy_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");
        assert!(name_btree.get(&legacy_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_and_drop_index_remove_legacy_name_entries() {
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

        let legacy_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");
        name_btree
            .insert(&legacy_key, &catalog_btree::serialize_name_value(1))
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
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();
        assert!(name_btree.get(&legacy_key).await.unwrap().is_none());

        name_btree
            .insert(&legacy_key, &catalog_btree::serialize_name_value(1))
            .await
            .unwrap();
        CatalogPersistence::apply_drop_index(&id_btree, &name_btree, &mut cache, IndexId(1))
            .await
            .unwrap();

        let scoped_key = catalog_btree::make_catalog_index_name_key(1, "email_idx");
        assert!(name_btree.get(&scoped_key).await.unwrap().is_none());
        assert!(name_btree.get(&legacy_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn apply_index_ready() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            1,
            "users",
            10,
            2,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".to_string(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 20,
            state: IndexState::Building,
        };
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
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
        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
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
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        CatalogPersistence::apply_drop_index(&id_btree, &name_btree, &mut cache, IndexId(1))
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
            CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
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
        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert!(reloaded.list_collections().is_empty());
        assert!(reloaded.get_index(IndexId(1)).is_none());
    }

    #[tokio::test]
    async fn id_allocators_updated_on_load() {
        let (storage, id_btree, name_btree) = setup().await;
        let mut cache = CatalogCache::new(1, 1);

        create_collection_with_created_at(
            &id_btree,
            &name_btree,
            &mut cache,
            42,
            "users",
            10,
            98,
            30,
        )
        .await;

        let meta = IndexMeta {
            index_id: IndexId(99),
            collection_id: CollectionId(42),
            name: "idx".to_string(),
            field_paths: vec![FieldPath::single("f")],
            root_page: 20,
            state: IndexState::Ready,
        };
        CatalogPersistence::apply_create_index(&id_btree, &name_btree, &mut cache, &meta)
            .await
            .unwrap();

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
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
            create_collection_with_created_at(
                &id_btree,
                &name_btree,
                &mut cache,
                i,
                &format!("col_{i}"),
                i as u32 * 10,
                100 + i,
                i as u32 * 20,
            )
            .await;
        }

        let reloaded = CatalogPersistence::load_catalog(&storage, &id_btree, &name_btree)
            .await
            .unwrap();
        assert_eq!(reloaded.list_collections().len(), 5);
    }
}
