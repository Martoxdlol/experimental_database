//! Catalog: manages collection and index metadata.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use anyhow::{anyhow, Result};

use crate::types::{CollectionId, FieldPath, IndexId, IndexSpec, IndexState, Ts};

// ── IndexMeta ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub spec: IndexSpec,
    pub state: IndexState,
}

// ── CollectionMeta ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct CollectionMeta {
    pub id: CollectionId,
    pub name: String,
    /// Indexes: index_id -> IndexMeta
    pub indexes: HashMap<IndexId, IndexMeta>,
}

impl CollectionMeta {
    pub fn new(id: CollectionId, name: String) -> Self {
        CollectionMeta { id, name, indexes: HashMap::new() }
    }

    pub fn find_ready_index_for_field(&self, field: &FieldPath) -> Option<&IndexMeta> {
        self.indexes.values().find(|idx| {
            matches!(idx.state, IndexState::Ready { .. }) && idx.spec.field == *field
        })
    }
}

// ── Catalog ───────────────────────────────────────────────────────────────────

/// Global catalog of all collections and their indexes.
pub struct Catalog {
    inner: RwLock<CatalogInner>,
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

struct CatalogInner {
    /// collection name -> CollectionMeta
    by_name: HashMap<String, CollectionMeta>,
    /// collection id -> name (for reverse lookup)
    id_to_name: HashMap<CollectionId, String>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            inner: RwLock::new(CatalogInner {
                by_name: HashMap::new(),
                id_to_name: HashMap::new(),
            }),
            next_collection_id: AtomicU64::new(1),
            next_index_id: AtomicU64::new(1),
        }
    }

    pub fn alloc_collection_id(&self) -> CollectionId {
        self.next_collection_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn alloc_index_id(&self) -> IndexId {
        self.next_index_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a collection. Returns the assigned CollectionId.
    pub fn create_collection(&self, name: &str) -> Result<CollectionId> {
        let mut inner = self.inner.write();
        if inner.by_name.contains_key(name) {
            return Err(anyhow!("collection '{}' already exists", name));
        }
        let id = self.alloc_collection_id();
        let meta = CollectionMeta::new(id, name.to_string());
        inner.id_to_name.insert(id, name.to_string());
        inner.by_name.insert(name.to_string(), meta);
        Ok(id)
    }

    /// Create a collection with a specific id (used during WAL replay).
    pub fn create_collection_with_id(&self, id: CollectionId, name: &str) -> Result<()> {
        let mut inner = self.inner.write();
        if inner.by_name.contains_key(name) {
            return Ok(()); // idempotent
        }
        let meta = CollectionMeta::new(id, name.to_string());
        inner.id_to_name.insert(id, name.to_string());
        inner.by_name.insert(name.to_string(), meta);
        // Advance the allocator past this id
        let next = self.next_collection_id.load(Ordering::SeqCst);
        if id >= next {
            self.next_collection_id.store(id + 1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Delete a collection.
    pub fn delete_collection(&self, id: CollectionId) -> Result<()> {
        let mut inner = self.inner.write();
        let name = inner
            .id_to_name
            .remove(&id)
            .ok_or_else(|| anyhow!("collection id {} not found", id))?;
        inner.by_name.remove(&name);
        Ok(())
    }

    /// Delete a collection by name.
    pub fn delete_collection_by_name(&self, name: &str) -> Result<CollectionId> {
        let mut inner = self.inner.write();
        let meta = inner.by_name.remove(name).ok_or_else(|| anyhow!("collection '{}' not found", name))?;
        inner.id_to_name.remove(&meta.id);
        Ok(meta.id)
    }

    /// Look up a collection by name.
    pub fn get_collection_by_name(&self, name: &str) -> Result<CollectionMeta> {
        let inner = self.inner.read();
        inner
            .by_name
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("collection '{}' not found", name))
    }

    /// Look up a collection by id.
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<CollectionMeta> {
        let inner = self.inner.read();
        inner.id_to_name.get(&id).and_then(|name| inner.by_name.get(name)).cloned()
    }

    /// List all collection names.
    pub fn collection_names(&self) -> Vec<String> {
        let inner = self.inner.read();
        inner.by_name.keys().cloned().collect()
    }

    /// Add an index to a collection (in Building state).
    pub fn add_index(
        &self,
        collection_name: &str,
        spec: IndexSpec,
        started_at_ts: Ts,
    ) -> Result<IndexId> {
        let mut inner = self.inner.write();
        let meta = inner
            .by_name
            .get_mut(collection_name)
            .ok_or_else(|| anyhow!("collection '{}' not found", collection_name))?;
        let index_id = self.next_index_id.fetch_add(1, Ordering::SeqCst);
        let index_meta = IndexMeta {
            index_id,
            collection_id: meta.id,
            spec,
            state: IndexState::Building { started_at_ts },
        };
        meta.indexes.insert(index_id, index_meta);
        Ok(index_id)
    }

    /// Add an index with a specific ID (used during WAL replay).
    pub fn add_index_with_id(
        &self,
        collection_id: CollectionId,
        index_id: IndexId,
        spec: IndexSpec,
        state: IndexState,
    ) -> Result<()> {
        let mut inner = self.inner.write();
        let name = inner
            .id_to_name
            .get(&collection_id)
            .cloned()
            .ok_or_else(|| anyhow!("collection id {} not found", collection_id))?;
        let meta = inner
            .by_name
            .get_mut(&name)
            .ok_or_else(|| anyhow!("collection '{}' not found", name))?;
        let index_meta = IndexMeta { index_id, collection_id, spec, state };
        meta.indexes.insert(index_id, index_meta);
        // Advance the allocator past this id
        let next = self.next_index_id.load(Ordering::SeqCst);
        if index_id >= next {
            self.next_index_id.store(index_id + 1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Set an index state to Ready.
    pub fn set_index_ready(
        &self,
        collection_id: CollectionId,
        index_id: IndexId,
        ready_at_ts: Ts,
    ) -> Result<()> {
        self.update_index_state(collection_id, index_id, IndexState::Ready { ready_at_ts })
    }

    /// Set an index state to Failed.
    pub fn set_index_failed(
        &self,
        collection_id: CollectionId,
        index_id: IndexId,
        message: String,
    ) -> Result<()> {
        self.update_index_state(collection_id, index_id, IndexState::Failed { message })
    }

    fn update_index_state(
        &self,
        collection_id: CollectionId,
        index_id: IndexId,
        new_state: IndexState,
    ) -> Result<()> {
        let mut inner = self.inner.write();
        let name = inner
            .id_to_name
            .get(&collection_id)
            .cloned()
            .ok_or_else(|| anyhow!("collection id {} not found", collection_id))?;
        let meta = inner
            .by_name
            .get_mut(&name)
            .ok_or_else(|| anyhow!("collection '{}' not found", name))?;
        let idx = meta
            .indexes
            .get_mut(&index_id)
            .ok_or_else(|| anyhow!("index {} not found in collection '{}'", index_id, name))?;
        idx.state = new_state;
        Ok(())
    }

    /// Get the state of an index.
    pub fn index_state(
        &self,
        collection_name: &str,
        index_id: IndexId,
    ) -> Result<IndexState> {
        let inner = self.inner.read();
        let meta = inner
            .by_name
            .get(collection_name)
            .ok_or_else(|| anyhow!("collection '{}' not found", collection_name))?;
        let idx = meta
            .indexes
            .get(&index_id)
            .ok_or_else(|| anyhow!("index {} not found", index_id))?;
        Ok(idx.state.clone())
    }

    /// Remove an index.
    pub fn remove_index(&self, collection_id: CollectionId, index_id: IndexId) -> Result<()> {
        let mut inner = self.inner.write();
        let name = inner
            .id_to_name
            .get(&collection_id)
            .cloned()
            .ok_or_else(|| anyhow!("collection id {} not found", collection_id))?;
        let meta = inner
            .by_name
            .get_mut(&name)
            .ok_or_else(|| anyhow!("collection '{}' not found", name))?;
        meta.indexes.remove(&index_id);
        Ok(())
    }
}
