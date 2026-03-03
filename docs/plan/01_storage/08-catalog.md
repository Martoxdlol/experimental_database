# 08 — Catalog (`storage/catalog.rs`)

The catalog manages collection and index metadata using a B-tree in `data.db` (DESIGN §2.12).

## Structs

```rust
/// Metadata for a collection.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: PageId,
    pub created_at_root_page: PageId,  // _created_at index B-tree root
    pub doc_count: u64,                // approximate
    pub config: CollectionConfig,
}

/// Per-collection configuration.
#[derive(Debug, Clone, Default)]
pub struct CollectionConfig {
    pub max_doc_size: Option<u32>,
}

/// Metadata for a secondary index.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
    pub root_page: PageId,
    pub state: IndexState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexState {
    Building = 0x01,
    Ready    = 0x02,
    Dropping = 0x03,
}

/// Database registry entry (for _system catalog).
#[derive(Debug, Clone)]
pub struct DatabaseEntry {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub created_at: u64,
    pub config: DatabaseConfig,
    pub state: DatabaseState,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub page_size: u32,
    pub memory_budget: u64,
    pub max_doc_size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DatabaseState {
    Active   = 0x01,
    Creating = 0x02,
    Dropping = 0x03,
}

// ── In-memory catalog cache (DESIGN §2.12.4) ──────────────────────

/// In-memory catalog cache. All lookups go through here, never the B-tree.
pub struct CatalogCache {
    pub name_to_collection: HashMap<String, CollectionId>,
    pub collections: HashMap<CollectionId, CollectionMeta>,
    pub indexes: HashMap<IndexId, IndexMeta>,
    pub collection_indexes: HashMap<CollectionId, Vec<IndexId>>,
    pub next_collection_id: AtomicU64,
    pub next_index_id: AtomicU64,
}
```

## Catalog B-Tree Operations

```rust
/// Operations on the catalog B-tree stored in data.db.
/// This wraps a BTree with catalog-specific key encoding and value
/// serialization.
pub struct Catalog<'a> {
    btree: BTree<'a>,
    cache: CatalogCache,
}

impl<'a> Catalog<'a> {
    /// Load the catalog from the B-tree. Scans all entries and populates
    /// the in-memory cache.
    pub async fn load(
        pool: &'a BufferPool,
        catalog_root_page: PageId,
    ) -> Result<Self, StorageError>;

    // ── Collection operations ───────────────────────────────

    /// Create a new collection. Allocates pages for primary + _created_at trees.
    /// Updates the catalog B-tree and cache.
    pub async fn create_collection(
        &mut self,
        name: &str,
        free_list: &mut FreeList,
    ) -> Result<CollectionMeta, StorageError>;

    /// Drop a collection. Removes from B-tree and cache.
    pub async fn drop_collection(
        &mut self,
        collection_id: CollectionId,
        free_list: &mut FreeList,
    ) -> Result<(), StorageError>;

    /// Lookup collection by name.
    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta>;

    /// Lookup collection by ID.
    pub fn get_collection(&self, id: CollectionId) -> Option<&CollectionMeta>;

    /// List all collections.
    pub fn list_collections(&self) -> Vec<&CollectionMeta>;

    /// Update a collection's root page (e.g., after a B-tree root split).
    pub async fn update_primary_root(
        &mut self,
        collection_id: CollectionId,
        new_root: PageId,
    ) -> Result<(), StorageError>;

    /// Update approximate doc count.
    pub fn update_doc_count(&mut self, collection_id: CollectionId, delta: i64);

    // ── Index operations ────────────────────────────────────

    /// Create a new secondary index. Allocates a page for the B-tree root.
    /// State is set to Building.
    pub async fn create_index(
        &mut self,
        collection_id: CollectionId,
        name: &str,
        field_paths: Vec<FieldPath>,
        free_list: &mut FreeList,
    ) -> Result<IndexMeta, StorageError>;

    /// Drop an index. Removes from B-tree and cache.
    pub async fn drop_index(
        &mut self,
        index_id: IndexId,
        free_list: &mut FreeList,
    ) -> Result<(), StorageError>;

    /// Mark an index as Ready.
    pub async fn mark_index_ready(&mut self, index_id: IndexId) -> Result<(), StorageError>;

    /// Lookup index by ID.
    pub fn get_index(&self, id: IndexId) -> Option<&IndexMeta>;

    /// List indexes for a collection.
    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;

    /// Update an index's root page.
    pub async fn update_index_root(
        &mut self,
        index_id: IndexId,
        new_root: PageId,
    ) -> Result<(), StorageError>;

    // ── Catalog root ────────────────────────────────────────

    /// Current root page of the catalog B-tree (for file header updates).
    pub fn root_page(&self) -> PageId;
}
```

## Catalog B-Tree Key Format

Per DESIGN §2.12.2:

```
entity_type[1] || entity_id[8 big-endian]
```

Where `entity_type`:
- `0x01` = Collection
- `0x02` = Index

Values are serialized as compact binary (not BSON) matching the struct fields.

## Serialization

```rust
/// Serialize a CollectionMeta into catalog B-tree value bytes.
fn serialize_collection(meta: &CollectionMeta) -> Vec<u8>;

/// Deserialize a CollectionMeta from catalog B-tree value bytes.
fn deserialize_collection(data: &[u8]) -> Result<CollectionMeta, StorageError>;

/// Serialize an IndexMeta into catalog B-tree value bytes.
fn serialize_index(meta: &IndexMeta) -> Vec<u8>;

/// Deserialize an IndexMeta from catalog B-tree value bytes.
fn deserialize_index(data: &[u8]) -> Result<IndexMeta, StorageError>;
```
