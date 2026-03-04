use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::types::*;

/// Collection metadata stored in the catalog.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: PageId,
    pub created_at_root_page: PageId,
    pub doc_count: u64,
    pub config: CollectionConfig,
}

/// Index metadata stored in the catalog.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
    pub root_page: PageId,
    pub state: IndexState,
}

/// Collection-level configuration.
#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub max_doc_size: u32,
}

impl Default for CollectionConfig {
    fn default() -> Self {
        Self { max_doc_size: 16 * 1024 * 1024 }
    }
}

/// O(1) in-memory catalog lookup.
pub struct CatalogCache {
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,
    indexes: HashMap<IndexId, IndexMeta>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl CatalogCache {
    pub fn new(next_collection_id: u64, next_index_id: u64) -> Self {
        Self {
            name_to_collection: HashMap::new(),
            collections: HashMap::new(),
            indexes: HashMap::new(),
            collection_indexes: HashMap::new(),
            next_collection_id: AtomicU64::new(next_collection_id),
            next_index_id: AtomicU64::new(next_index_id),
        }
    }

    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta> {
        let id = self.name_to_collection.get(name)?;
        self.collections.get(id)
    }

    pub fn get_collection(&self, id: CollectionId) -> Option<&CollectionMeta> {
        self.collections.get(&id)
    }

    pub fn get_index(&self, id: IndexId) -> Option<&IndexMeta> {
        self.indexes.get(&id)
    }

    pub fn get_indexes_for_collection(&self, id: CollectionId) -> Vec<IndexId> {
        self.collection_indexes.get(&id).cloned().unwrap_or_default()
    }

    pub fn all_collections(&self) -> impl Iterator<Item = &CollectionMeta> {
        self.collections.values()
    }

    pub fn all_indexes(&self) -> impl Iterator<Item = &IndexMeta> {
        self.indexes.values()
    }

    pub fn insert_collection(&mut self, meta: CollectionMeta) {
        self.name_to_collection.insert(meta.name.clone(), meta.collection_id);
        self.collections.insert(meta.collection_id, meta);
    }

    pub fn remove_collection(&mut self, id: CollectionId) {
        if let Some(meta) = self.collections.remove(&id) {
            self.name_to_collection.remove(&meta.name);
        }
        if let Some(idx_ids) = self.collection_indexes.remove(&id) {
            for idx_id in idx_ids {
                self.indexes.remove(&idx_id);
            }
        }
    }

    pub fn insert_index(&mut self, meta: IndexMeta) {
        self.collection_indexes
            .entry(meta.collection_id)
            .or_default()
            .push(meta.index_id);
        self.indexes.insert(meta.index_id, meta);
    }

    pub fn remove_index(&mut self, id: IndexId) {
        if let Some(meta) = self.indexes.remove(&id)
            && let Some(idx_list) = self.collection_indexes.get_mut(&meta.collection_id) {
            idx_list.retain(|&i| i != id);
        }
    }

    pub fn update_index_state(&mut self, id: IndexId, state: IndexState) {
        if let Some(meta) = self.indexes.get_mut(&id) {
            meta.state = state;
        }
    }

    pub fn update_collection_root(&mut self, id: CollectionId, root: PageId) {
        if let Some(meta) = self.collections.get_mut(&id) {
            meta.primary_root_page = root;
        }
    }

    pub fn update_index_root(&mut self, id: IndexId, root: PageId) {
        if let Some(meta) = self.indexes.get_mut(&id) {
            meta.root_page = root;
        }
    }

    pub fn next_collection_id(&self) -> CollectionId {
        CollectionId(self.next_collection_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn next_index_id(&self) -> IndexId {
        IndexId(self.next_index_id.fetch_add(1, Ordering::SeqCst))
    }
}

impl Clone for CatalogCache {
    fn clone(&self) -> Self {
        Self {
            name_to_collection: self.name_to_collection.clone(),
            collections: self.collections.clone(),
            indexes: self.indexes.clone(),
            collection_indexes: self.collection_indexes.clone(),
            next_collection_id: AtomicU64::new(self.next_collection_id.load(Ordering::SeqCst)),
            next_index_id: AtomicU64::new(self.next_index_id.load(Ordering::SeqCst)),
        }
    }
}

/// Shared catalog access via ArcSwap for lock-free reads.
pub struct SharedCatalog {
    inner: arc_swap::ArcSwap<CatalogCache>,
}

impl SharedCatalog {
    pub fn new(cache: CatalogCache) -> Self {
        Self {
            inner: arc_swap::ArcSwap::new(std::sync::Arc::new(cache)),
        }
    }

    pub fn read(&self) -> arc_swap::Guard<std::sync::Arc<CatalogCache>> {
        self.inner.load()
    }

    pub fn update(&self, new_cache: CatalogCache) {
        self.inner.store(std::sync::Arc::new(new_cache));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup_collection() {
        let mut cache = CatalogCache::new(1, 1);
        let meta = CollectionMeta {
            collection_id: CollectionId(1),
            name: "users".into(),
            primary_root_page: PageId(10),
            created_at_root_page: PageId(11),
            doc_count: 0,
            config: CollectionConfig::default(),
        };
        cache.insert_collection(meta);
        assert!(cache.get_collection_by_name("users").is_some());
        assert!(cache.get_collection(CollectionId(1)).is_some());
    }

    #[test]
    fn test_insert_and_lookup_index() {
        let mut cache = CatalogCache::new(1, 1);
        let idx = IndexMeta {
            index_id: IndexId(1),
            collection_id: CollectionId(1),
            name: "email_idx".into(),
            field_paths: vec![vec!["email".into()]],
            root_page: PageId(20),
            state: IndexState::Building,
        };
        cache.insert_index(idx);
        assert!(cache.get_index(IndexId(1)).is_some());
        assert_eq!(cache.get_indexes_for_collection(CollectionId(1)).len(), 1);
    }

    #[test]
    fn test_remove_collection() {
        let mut cache = CatalogCache::new(1, 1);
        cache.insert_collection(CollectionMeta {
            collection_id: CollectionId(1),
            name: "test".into(),
            primary_root_page: PageId(10),
            created_at_root_page: PageId(11),
            doc_count: 0,
            config: CollectionConfig::default(),
        });
        cache.remove_collection(CollectionId(1));
        assert!(cache.get_collection_by_name("test").is_none());
    }

    #[test]
    fn test_shared_catalog() {
        let cache = CatalogCache::new(1, 1);
        let shared = SharedCatalog::new(cache);
        let guard = shared.read();
        assert!(guard.get_collection_by_name("nonexistent").is_none());
    }
}
