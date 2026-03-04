# 09 — Catalog

Implements DESIGN.md §2.12. Catalog B-tree + in-memory cache.

## File: `src/storage/catalog.rs`

### Catalog B-Tree

The catalog is stored as a B-tree in the data file with the same page format as data B-trees. The root page ID is stored in the file header (page 0).

**Key format**: `entity_type[1] || entity_id[8]` (big-endian u64)

**Entity types**:
- `0x01` = Collection
- `0x02` = Index

### Catalog Entries

```rust
/// Collection metadata stored in the catalog B-tree.
#[derive(Debug, Clone)]
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: PageId,
    pub created_at_root_page: PageId,
    pub doc_count: u64,
    pub config: CollectionConfig,
}

/// Index metadata stored in the catalog B-tree.
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
    pub max_doc_size: u32,    // default: 16 MB
}

/// Catalog B-tree key.
fn catalog_key(entity_type: u8, entity_id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = entity_type;
    key[1..9].copy_from_slice(&entity_id.to_be_bytes());
    key
}
```

### Serialization

Catalog entries are serialized as compact binary values in the B-tree leaf cells.

```rust
impl CollectionMeta {
    /// Serialize to bytes for B-tree cell value.
    pub fn serialize(&self) -> Vec<u8>;
    /// Deserialize from B-tree cell value bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self>;
}

impl IndexMeta {
    pub fn serialize(&self) -> Vec<u8>;
    pub fn deserialize(data: &[u8]) -> Result<Self>;
}
```

### In-Memory Cache

```rust
/// O(1) in-memory catalog lookup. Derived from the catalog B-tree.
pub struct CatalogCache {
    // Collection lookup
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,

    // Index lookup
    indexes: HashMap<IndexId, IndexMeta>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,

    // ID allocators (monotonically increasing)
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl CatalogCache {
    /// Build cache by scanning the catalog B-tree.
    pub fn load(
        pool: &BufferPool,
        catalog_root_page: PageId,
        next_collection_id: u64,
        next_index_id: u64,
    ) -> Result<Self>;

    // --- Read operations (called by any task) ---

    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta>;
    pub fn get_collection(&self, id: CollectionId) -> Option<&CollectionMeta>;
    pub fn get_index(&self, id: IndexId) -> Option<&IndexMeta>;
    pub fn get_indexes_for_collection(&self, id: CollectionId) -> &[IndexId];
    pub fn all_collections(&self) -> impl Iterator<Item = &CollectionMeta>;
    pub fn all_indexes(&self) -> impl Iterator<Item = &IndexMeta>;

    // --- Write operations (called by the single writer) ---

    pub fn insert_collection(&mut self, meta: CollectionMeta);
    pub fn remove_collection(&mut self, id: CollectionId);
    pub fn insert_index(&mut self, meta: IndexMeta);
    pub fn remove_index(&mut self, id: IndexId);
    pub fn update_index_state(&mut self, id: IndexId, state: IndexState);
    pub fn update_collection_root(&mut self, id: CollectionId, root: PageId);
    pub fn update_index_root(&mut self, id: IndexId, root: PageId);

    // --- ID allocation ---

    pub fn next_collection_id(&self) -> CollectionId;
    pub fn next_index_id(&self) -> IndexId;
}
```

### Catalog Manager

Coordinates B-tree mutations + cache updates + WAL writes.

```rust
/// High-level catalog operations.
/// Called by the single writer.
pub struct CatalogManager {
    catalog_root_page: PageId,
    cache: CatalogCache,
}

impl CatalogManager {
    /// Load catalog from B-tree on startup.
    pub fn load(pool: &BufferPool, file_header: &FileHeader) -> Result<Self>;

    /// Create a new collection.
    /// 1. Assign collection_id.
    /// 2. Allocate primary + _created_at root pages.
    /// 3. Insert into catalog B-tree.
    /// 4. Update cache.
    /// Returns CollectionMeta and the WAL record to write.
    pub fn create_collection(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        name: &str,
        config: CollectionConfig,
        lsn: Lsn,
    ) -> Result<(CollectionMeta, CreateCollectionRecord)>;

    /// Drop a collection.
    /// 1. Remove from catalog B-tree.
    /// 2. Reclaim all B-tree pages (primary + secondary indexes) to free list.
    /// 3. Update cache.
    pub fn drop_collection(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        collection_id: CollectionId,
        lsn: Lsn,
    ) -> Result<DropCollectionRecord>;

    /// Create a new secondary index.
    /// 1. Assign index_id.
    /// 2. Allocate root page.
    /// 3. Insert into catalog B-tree with state = Building.
    /// 4. Update cache.
    pub fn create_index(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        collection_id: CollectionId,
        name: &str,
        field_paths: Vec<FieldPath>,
        lsn: Lsn,
    ) -> Result<(IndexMeta, CreateIndexRecord)>;

    /// Mark an index as Ready.
    pub fn mark_index_ready(
        &mut self,
        pool: &BufferPool,
        index_id: IndexId,
        lsn: Lsn,
    ) -> Result<IndexReadyRecord>;

    /// Drop an index.
    pub fn drop_index(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        index_id: IndexId,
        lsn: Lsn,
    ) -> Result<DropIndexRecord>;

    /// Current catalog root page (may change if catalog B-tree root splits).
    pub fn root_page(&self) -> PageId;

    /// Read-only cache access.
    pub fn cache(&self) -> &CatalogCache;
}
```

### Concurrency

- **Reads**: `CatalogCache` is read concurrently by query tasks. The cache is behind a `RwLock` (or is `Arc<CatalogCache>` with copy-on-write updates by the writer).
- **Writes**: only the single writer mutates the catalog B-tree and cache.
- **Cache consistency**: WAL record is written first, then B-tree updated, then cache updated. If crash between B-tree and cache: cache is rebuilt from B-tree on recovery.

### Approach for Concurrent Cache Reads

Two options:

**Option A: `Arc<RwLock<CatalogCache>>`** — writer takes write lock to update. Simple but readers block briefly during updates.

**Option B: `ArcSwap<CatalogCache>`** — writer clones, modifies, swaps atomically. Readers never block. Slightly more memory. **Recommended** for this project since catalog mutations are rare and reads are on the hot path.

```rust
use arc_swap::ArcSwap;
use std::sync::Arc;

pub struct SharedCatalog {
    inner: ArcSwap<CatalogCache>,
}

impl SharedCatalog {
    pub fn read(&self) -> arc_swap::Guard<Arc<CatalogCache>>;

    /// Called by writer to atomically swap in updated cache.
    pub fn update(&self, new_cache: CatalogCache);
}
```
