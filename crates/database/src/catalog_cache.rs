//! B2: In-memory dual-indexed catalog cache.
//!
//! Provides O(1) lookup by both name and ID for collections and indexes.
//! Thread-safe reads via external `RwLock` (L6 wraps it).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};

/// Metadata for a collection in the catalog.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: u32,
    pub doc_count: u64,
}

/// Metadata for a secondary index in the catalog.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
    pub root_page: u32,
    pub state: IndexState,
}

/// Index lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Building,
    Ready,
    Dropping,
}

/// In-memory dual-indexed catalog cache.
pub struct CatalogCache {
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,
    indexes: HashMap<IndexId, IndexMeta>,
    index_name_to_id: HashMap<(CollectionId, String), IndexId>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl CatalogCache {
    /// Create an empty catalog cache.
    pub fn new() -> Self {
        // Start IDs at 10 to leave room for reserved IDs (0, 1 for pseudo-collections)
        Self {
            name_to_collection: HashMap::new(),
            collections: HashMap::new(),
            indexes: HashMap::new(),
            index_name_to_id: HashMap::new(),
            collection_indexes: HashMap::new(),
            next_collection_id: AtomicU64::new(10),
            next_index_id: AtomicU64::new(10),
        }
    }

    /// Set the next ID counters (used during recovery from FileHeader).
    pub fn set_next_ids(&self, next_collection_id: u64, next_index_id: u64) {
        self.next_collection_id.store(next_collection_id, Ordering::Relaxed);
        self.next_index_id.store(next_index_id, Ordering::Relaxed);
    }

    // ── By Name ──

    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta> {
        self.name_to_collection
            .get(name)
            .and_then(|id| self.collections.get(id))
    }

    pub fn get_index_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Option<&IndexMeta> {
        self.index_name_to_id
            .get(&(collection_id, name.to_string()))
            .and_then(|id| self.indexes.get(id))
    }

    // ── By ID ──

    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<&CollectionMeta> {
        self.collections.get(&id)
    }

    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexMeta> {
        self.indexes.get(&id)
    }

    // ── Listing ──

    pub fn list_collections(&self) -> Vec<&CollectionMeta> {
        self.collections.values().collect()
    }

    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta> {
        self.collection_indexes
            .get(&collection_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.indexes.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn ready_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta> {
        self.list_indexes(collection_id)
            .into_iter()
            .filter(|m| m.state == IndexState::Ready)
            .collect()
    }

    // ── Mutation ──

    /// Add a collection to the cache.
    ///
    /// # Panics
    /// Panics if a collection with the same name or ID already exists.
    pub fn add_collection(&mut self, meta: CollectionMeta) {
        assert!(
            !self.name_to_collection.contains_key(&meta.name),
            "duplicate collection name: {}",
            meta.name
        );
        assert!(
            !self.collections.contains_key(&meta.collection_id),
            "duplicate collection id: {:?}",
            meta.collection_id
        );
        self.name_to_collection
            .insert(meta.name.clone(), meta.collection_id);
        self.collections.insert(meta.collection_id, meta);
    }

    /// Remove a collection and all its indexes from the cache.
    pub fn remove_collection(&mut self, id: CollectionId) -> Option<CollectionMeta> {
        let meta = self.collections.remove(&id)?;
        self.name_to_collection.remove(&meta.name);
        // Cascade: remove all indexes for this collection
        if let Some(index_ids) = self.collection_indexes.remove(&id) {
            for index_id in index_ids {
                if let Some(idx_meta) = self.indexes.remove(&index_id) {
                    self.index_name_to_id
                        .remove(&(id, idx_meta.name));
                }
            }
        }
        Some(meta)
    }

    /// Add an index to the cache.
    ///
    /// # Panics
    /// Panics if an index with the same (collection_id, name) or ID already exists.
    pub fn add_index(&mut self, meta: IndexMeta) {
        let key = (meta.collection_id, meta.name.clone());
        assert!(
            !self.index_name_to_id.contains_key(&key),
            "duplicate index name: {:?}",
            key
        );
        assert!(
            !self.indexes.contains_key(&meta.index_id),
            "duplicate index id: {:?}",
            meta.index_id
        );
        self.index_name_to_id.insert(key, meta.index_id);
        self.collection_indexes
            .entry(meta.collection_id)
            .or_default()
            .push(meta.index_id);
        self.indexes.insert(meta.index_id, meta);
    }

    /// Remove an index from the cache.
    pub fn remove_index(&mut self, id: IndexId) -> Option<IndexMeta> {
        let meta = self.indexes.remove(&id)?;
        self.index_name_to_id
            .remove(&(meta.collection_id, meta.name.clone()));
        if let Some(ids) = self.collection_indexes.get_mut(&meta.collection_id) {
            ids.retain(|&i| i != id);
        }
        Some(meta)
    }

    /// Update an index's state.
    pub fn set_index_state(&mut self, id: IndexId, state: IndexState) {
        if let Some(meta) = self.indexes.get_mut(&id) {
            meta.state = state;
        }
    }

    // ── ID Allocation ──

    pub fn next_collection_id(&self) -> CollectionId {
        CollectionId(self.next_collection_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn next_index_id(&self) -> IndexId {
        IndexId(self.next_index_id.fetch_add(1, Ordering::Relaxed))
    }

    // ── Counts ──

    pub fn collection_count(&self) -> usize {
        self.collections.len()
    }

    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

impl Default for CatalogCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_collection(id: u64, name: &str) -> CollectionMeta {
        CollectionMeta {
            collection_id: CollectionId(id),
            name: name.to_string(),
            primary_root_page: 0,
            doc_count: 0,
        }
    }

    fn make_index(id: u64, coll_id: u64, name: &str, state: IndexState) -> IndexMeta {
        IndexMeta {
            index_id: IndexId(id),
            collection_id: CollectionId(coll_id),
            name: name.to_string(),
            field_paths: vec![FieldPath::single(name)],
            root_page: 0,
            state,
        }
    }

    #[test]
    fn new_cache_is_empty() {
        let cache = CatalogCache::new();
        assert_eq!(cache.collection_count(), 0);
        assert_eq!(cache.index_count(), 0);
    }

    #[test]
    fn add_and_lookup_collection_by_name() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        let meta = cache.get_collection_by_name("users").unwrap();
        assert_eq!(meta.collection_id, CollectionId(10));
        assert_eq!(meta.name, "users");
    }

    #[test]
    fn add_and_lookup_collection_by_id() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        let meta = cache.get_collection_by_id(CollectionId(10)).unwrap();
        assert_eq!(meta.name, "users");
    }

    #[test]
    fn add_and_lookup_index_by_name() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        let meta = cache.get_index_by_name(CollectionId(10), "by_email").unwrap();
        assert_eq!(meta.index_id, IndexId(20));
    }

    #[test]
    fn add_and_lookup_index_by_id() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        let meta = cache.get_index_by_id(IndexId(20)).unwrap();
        assert_eq!(meta.name, "by_email");
    }

    #[test]
    fn list_collections() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_collection(make_collection(11, "orders"));
        let collections = cache.list_collections();
        assert_eq!(collections.len(), 2);
    }

    #[test]
    fn list_indexes_for_collection() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_collection(make_collection(11, "orders"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        cache.add_index(make_index(21, 10, "by_age", IndexState::Ready));
        cache.add_index(make_index(22, 11, "by_status", IndexState::Ready));

        let user_indexes = cache.list_indexes(CollectionId(10));
        assert_eq!(user_indexes.len(), 2);
        let order_indexes = cache.list_indexes(CollectionId(11));
        assert_eq!(order_indexes.len(), 1);
    }

    #[test]
    fn ready_indexes_filters_building() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "ready_idx", IndexState::Ready));
        cache.add_index(make_index(21, 10, "building_idx", IndexState::Building));
        cache.add_index(make_index(22, 10, "dropping_idx", IndexState::Dropping));

        let ready = cache.ready_indexes(CollectionId(10));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "ready_idx");
    }

    #[test]
    fn remove_collection_cascades_indexes() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        cache.add_index(make_index(21, 10, "by_age", IndexState::Ready));

        let removed = cache.remove_collection(CollectionId(10));
        assert!(removed.is_some());
        assert_eq!(cache.collection_count(), 0);
        assert_eq!(cache.index_count(), 0);
        assert!(cache.get_collection_by_name("users").is_none());
        assert!(cache.get_index_by_id(IndexId(20)).is_none());
        assert!(cache.get_index_by_id(IndexId(21)).is_none());
    }

    #[test]
    fn remove_index() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        cache.add_index(make_index(21, 10, "by_age", IndexState::Ready));

        let removed = cache.remove_index(IndexId(20));
        assert!(removed.is_some());
        assert_eq!(cache.index_count(), 1);
        assert!(cache.get_index_by_name(CollectionId(10), "by_email").is_none());
        assert!(cache.get_index_by_name(CollectionId(10), "by_age").is_some());
    }

    #[test]
    fn set_index_state() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Building));
        assert_eq!(
            cache.get_index_by_id(IndexId(20)).unwrap().state,
            IndexState::Building
        );
        cache.set_index_state(IndexId(20), IndexState::Ready);
        assert_eq!(
            cache.get_index_by_id(IndexId(20)).unwrap().state,
            IndexState::Ready
        );
    }

    #[test]
    fn next_collection_id_monotonic() {
        let cache = CatalogCache::new();
        let id1 = cache.next_collection_id();
        let id2 = cache.next_collection_id();
        let id3 = cache.next_collection_id();
        assert!(id1.0 < id2.0);
        assert!(id2.0 < id3.0);
    }

    #[test]
    fn next_index_id_monotonic() {
        let cache = CatalogCache::new();
        let id1 = cache.next_index_id();
        let id2 = cache.next_index_id();
        assert!(id1.0 < id2.0);
    }

    #[test]
    #[should_panic(expected = "duplicate collection name")]
    fn duplicate_collection_name_panics() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_collection(make_collection(11, "users"));
    }

    #[test]
    #[should_panic(expected = "duplicate index name")]
    fn duplicate_index_name_panics() {
        let mut cache = CatalogCache::new();
        cache.add_collection(make_collection(10, "users"));
        cache.add_index(make_index(20, 10, "by_email", IndexState::Ready));
        cache.add_index(make_index(21, 10, "by_email", IndexState::Ready));
    }

    #[test]
    fn lookup_nonexistent_returns_none() {
        let cache = CatalogCache::new();
        assert!(cache.get_collection_by_name("nope").is_none());
        assert!(cache.get_collection_by_id(CollectionId(999)).is_none());
        assert!(cache.get_index_by_name(CollectionId(1), "nope").is_none());
        assert!(cache.get_index_by_id(IndexId(999)).is_none());
    }

    #[test]
    fn list_indexes_empty_collection() {
        let cache = CatalogCache::new();
        assert!(cache.list_indexes(CollectionId(1)).is_empty());
    }

    #[test]
    fn remove_nonexistent_collection() {
        let mut cache = CatalogCache::new();
        assert!(cache.remove_collection(CollectionId(999)).is_none());
    }

    #[test]
    fn remove_nonexistent_index() {
        let mut cache = CatalogCache::new();
        assert!(cache.remove_index(IndexId(999)).is_none());
    }
}
