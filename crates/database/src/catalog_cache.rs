//! B2: In-memory catalog cache with dual-indexed lookups.
//!
//! Provides O(1) lookups by both name and ID for collections and indexes.
//! Updated atomically during commit (step 4a) and during WAL recovery.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};

// ─── Collection Metadata ───

/// In-memory metadata for a collection.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: u32,
    pub doc_count: u64,
}

// ─── Index Metadata ───

/// Index lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Index is being built in the background.
    Building,
    /// Index is ready for queries.
    Ready,
    /// Index is being dropped.
    Dropping,
}

/// In-memory metadata for a secondary index.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
    pub root_page: u32,
    pub state: IndexState,
}

// ─── CatalogCache ───

/// Thread-safe in-memory catalog with bidirectional lookups.
pub struct CatalogCache {
    // Collection lookups
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,
    // Index lookups
    indexes: HashMap<IndexId, IndexMeta>,
    index_name_to_id: HashMap<(CollectionId, String), IndexId>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,
    // ID allocators
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl CatalogCache {
    /// Create an empty catalog cache.
    pub fn new(next_collection_id: u64, next_index_id: u64) -> Self {
        CatalogCache {
            name_to_collection: HashMap::new(),
            collections: HashMap::new(),
            indexes: HashMap::new(),
            index_name_to_id: HashMap::new(),
            collection_indexes: HashMap::new(),
            next_collection_id: AtomicU64::new(next_collection_id),
            next_index_id: AtomicU64::new(next_index_id),
        }
    }

    // ─── Collection Lookups ───

    /// Look up a collection by name.
    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta> {
        self.name_to_collection
            .get(name)
            .and_then(|id| self.collections.get(id))
    }

    /// Look up a collection by ID.
    pub fn get_collection(&self, id: CollectionId) -> Option<&CollectionMeta> {
        self.collections.get(&id)
    }

    /// List all collections.
    pub fn list_collections(&self) -> Vec<CollectionMeta> {
        self.collections.values().cloned().collect()
    }

    /// Check if a collection name exists.
    pub fn has_collection(&self, name: &str) -> bool {
        self.name_to_collection.contains_key(name)
    }

    // ─── Index Lookups ───

    /// Look up an index by ID.
    pub fn get_index(&self, id: IndexId) -> Option<&IndexMeta> {
        self.indexes.get(&id)
    }

    /// Look up an index by (collection_id, name).
    pub fn get_index_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Option<&IndexMeta> {
        self.index_name_to_id
            .get(&(collection_id, name.to_string()))
            .and_then(|id| self.indexes.get(id))
    }

    /// List all indexes for a collection.
    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<IndexMeta> {
        self.collection_indexes
            .get(&collection_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.indexes.get(id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List all indexes for a collection that are in Ready state.
    pub fn ready_indexes(&self, collection_id: CollectionId) -> Vec<IndexMeta> {
        self.list_indexes(collection_id)
            .into_iter()
            .filter(|m| m.state == IndexState::Ready)
            .collect()
    }

    // ─── Mutations ───

    /// Allocate a new collection ID.
    pub fn allocate_collection_id(&self) -> CollectionId {
        CollectionId(self.next_collection_id.fetch_add(1, Ordering::AcqRel))
    }

    /// Allocate a new index ID.
    pub fn allocate_index_id(&self) -> IndexId {
        IndexId(self.next_index_id.fetch_add(1, Ordering::AcqRel))
    }

    /// Get the next collection ID without allocating.
    pub fn next_collection_id(&self) -> u64 {
        self.next_collection_id.load(Ordering::Acquire)
    }

    /// Get the next index ID without allocating.
    pub fn next_index_id(&self) -> u64 {
        self.next_index_id.load(Ordering::Acquire)
    }

    /// Add a collection to the cache.
    pub fn add_collection(&mut self, meta: CollectionMeta) {
        self.name_to_collection
            .insert(meta.name.clone(), meta.collection_id);
        self.collections.insert(meta.collection_id, meta);
    }

    /// Remove a collection (and all its indexes) from the cache.
    /// Returns the removed metadata, if any.
    pub fn remove_collection(&mut self, id: CollectionId) -> Option<CollectionMeta> {
        let meta = self.collections.remove(&id)?;
        self.name_to_collection.remove(&meta.name);
        // Remove all indexes for this collection
        if let Some(idx_ids) = self.collection_indexes.remove(&id) {
            for idx_id in idx_ids {
                if let Some(idx_meta) = self.indexes.remove(&idx_id) {
                    self.index_name_to_id
                        .remove(&(id, idx_meta.name));
                }
            }
        }
        Some(meta)
    }

    /// Add an index to the cache.
    pub fn add_index(&mut self, meta: IndexMeta) {
        self.index_name_to_id
            .insert((meta.collection_id, meta.name.clone()), meta.index_id);
        self.collection_indexes
            .entry(meta.collection_id)
            .or_default()
            .push(meta.index_id);
        self.indexes.insert(meta.index_id, meta);
    }

    /// Remove an index from the cache. Returns the removed metadata, if any.
    pub fn remove_index(&mut self, id: IndexId) -> Option<IndexMeta> {
        let meta = self.indexes.remove(&id)?;
        self.index_name_to_id
            .remove(&(meta.collection_id, meta.name.clone()));
        if let Some(ids) = self.collection_indexes.get_mut(&meta.collection_id) {
            ids.retain(|&idx_id| idx_id != id);
        }
        Some(meta)
    }

    /// Update the state of an index.
    pub fn set_index_state(&mut self, id: IndexId, state: IndexState) {
        if let Some(meta) = self.indexes.get_mut(&id) {
            meta.state = state;
        }
    }

    /// Update the next_collection_id if the given value is higher.
    pub fn ensure_collection_id_at_least(&self, id: u64) {
        self.next_collection_id
            .fetch_max(id, Ordering::AcqRel);
    }

    /// Update the next_index_id if the given value is higher.
    pub fn ensure_index_id_at_least(&self, id: u64) {
        self.next_index_id.fetch_max(id, Ordering::AcqRel);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn make_collection(id: u64, name: &str) -> CollectionMeta {
        CollectionMeta {
            collection_id: CollectionId(id),
            name: name.to_string(),
            primary_root_page: id as u32 * 10,
            doc_count: 0,
        }
    }

    fn make_index(id: u64, coll_id: u64, name: &str, fields: &[&str]) -> IndexMeta {
        IndexMeta {
            index_id: IndexId(id),
            collection_id: CollectionId(coll_id),
            name: name.to_string(),
            field_paths: fields
                .iter()
                .map(|f| FieldPath::single(f))
                .collect(),
            root_page: id as u32 * 20,
            state: IndexState::Ready,
        }
    }

    #[test]
    fn empty_cache() {
        let cache = CatalogCache::new(1, 1);
        assert!(cache.list_collections().is_empty());
        assert_eq!(cache.get_collection_by_name("users"), None);
        assert_eq!(cache.get_collection(CollectionId(1)), None);
    }

    #[test]
    fn add_and_lookup_collection() {
        let mut cache = CatalogCache::new(1, 1);
        let meta = make_collection(1, "users");
        cache.add_collection(meta.clone());

        let found = cache.get_collection_by_name("users").unwrap();
        assert_eq!(found.collection_id, CollectionId(1));
        assert_eq!(found.name, "users");

        let found_by_id = cache.get_collection(CollectionId(1)).unwrap();
        assert_eq!(found_by_id.name, "users");

        assert!(cache.has_collection("users"));
        assert!(!cache.has_collection("orders"));
    }

    #[test]
    fn remove_collection() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));

        let removed = cache.remove_collection(CollectionId(1)).unwrap();
        assert_eq!(removed.name, "users");
        assert!(cache.get_collection_by_name("users").is_none());
        assert!(cache.get_index(IndexId(1)).is_none());
    }

    #[test]
    fn add_and_lookup_index() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));

        let found = cache
            .get_index_by_name(CollectionId(1), "email_idx")
            .unwrap();
        assert_eq!(found.index_id, IndexId(1));

        let found_by_id = cache.get_index(IndexId(1)).unwrap();
        assert_eq!(found_by_id.name, "email_idx");
    }

    #[test]
    fn list_indexes() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));
        cache.add_index(make_index(2, 1, "name_idx", &["name"]));

        let indexes = cache.list_indexes(CollectionId(1));
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn ready_indexes() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));
        let mut building = make_index(2, 1, "name_idx", &["name"]);
        building.state = IndexState::Building;
        cache.add_index(building);

        let ready = cache.ready_indexes(CollectionId(1));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "email_idx");
    }

    #[test]
    fn remove_index() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));
        cache.add_index(make_index(2, 1, "name_idx", &["name"]));

        let removed = cache.remove_index(IndexId(1)).unwrap();
        assert_eq!(removed.name, "email_idx");
        assert!(cache.get_index(IndexId(1)).is_none());
        // Other index still there
        assert!(cache.get_index(IndexId(2)).is_some());
    }

    #[test]
    fn set_index_state() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        let mut idx = make_index(1, 1, "email_idx", &["email"]);
        idx.state = IndexState::Building;
        cache.add_index(idx);

        assert_eq!(
            cache.get_index(IndexId(1)).unwrap().state,
            IndexState::Building
        );
        cache.set_index_state(IndexId(1), IndexState::Ready);
        assert_eq!(
            cache.get_index(IndexId(1)).unwrap().state,
            IndexState::Ready
        );
    }

    #[test]
    fn allocate_ids() {
        let cache = CatalogCache::new(10, 20);
        assert_eq!(cache.allocate_collection_id(), CollectionId(10));
        assert_eq!(cache.allocate_collection_id(), CollectionId(11));
        assert_eq!(cache.allocate_index_id(), IndexId(20));
        assert_eq!(cache.allocate_index_id(), IndexId(21));
    }

    #[test]
    fn ensure_id_at_least() {
        let cache = CatalogCache::new(1, 1);
        cache.ensure_collection_id_at_least(100);
        assert_eq!(cache.next_collection_id(), 100);
        // Lower value is a no-op
        cache.ensure_collection_id_at_least(50);
        assert_eq!(cache.next_collection_id(), 100);
    }

    #[test]
    fn list_collections_multiple() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "a"));
        cache.add_collection(make_collection(2, "b"));
        cache.add_collection(make_collection(3, "c"));

        let list = cache.list_collections();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn remove_nonexistent() {
        let mut cache = CatalogCache::new(1, 1);
        assert!(cache.remove_collection(CollectionId(999)).is_none());
        assert!(cache.remove_index(IndexId(999)).is_none());
    }

    #[test]
    fn add_collection_overwrites() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        let mut updated = make_collection(1, "users");
        updated.doc_count = 42;
        cache.add_collection(updated);

        assert_eq!(
            cache.get_collection(CollectionId(1)).unwrap().doc_count,
            42
        );
    }

    #[test]
    fn cross_collection_indexes() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "users"));
        cache.add_collection(make_collection(2, "orders"));
        cache.add_index(make_index(1, 1, "email_idx", &["email"]));
        cache.add_index(make_index(2, 2, "order_idx", &["total"]));

        assert_eq!(cache.list_indexes(CollectionId(1)).len(), 1);
        assert_eq!(cache.list_indexes(CollectionId(2)).len(), 1);
        assert!(cache.list_indexes(CollectionId(3)).is_empty());
    }

    #[test]
    fn index_name_collision_across_collections() {
        let mut cache = CatalogCache::new(1, 1);
        cache.add_collection(make_collection(1, "a"));
        cache.add_collection(make_collection(2, "b"));
        cache.add_index(make_index(1, 1, "idx", &["x"]));
        cache.add_index(make_index(2, 2, "idx", &["y"]));

        let a = cache.get_index_by_name(CollectionId(1), "idx").unwrap();
        let b = cache.get_index_by_name(CollectionId(2), "idx").unwrap();
        assert_eq!(a.index_id, IndexId(1));
        assert_eq!(b.index_id, IndexId(2));
    }
}
