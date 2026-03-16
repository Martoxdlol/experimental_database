# Layer 2: Storage Engine

**Layer purpose:** Generic storage primitives with **no domain knowledge**. Operates on bytes, pages, and raw keys/values. Can be used as a standalone embedded storage library independent of any document/MVCC/index semantics.

## Storage Backend Traits

The storage engine is **backend-agnostic**. All physical I/O is behind two traits, enabling both durable (filesystem) and ephemeral (in-memory) storage with the same engine code.

```rust
/// Page-level I/O backend. Implementations handle actual storage.
/// All methods are async (#[async_trait]).
#[async_trait]
pub trait PageStorage: Send + Sync {
    async fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<()>;
    async fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<()>;
    async fn sync(&self) -> Result<()>;
    async fn page_count(&self) -> u64;
    async fn extend(&self, new_count: u64) -> Result<()>;
}

/// WAL I/O backend. Implementations handle log persistence.
/// All methods are async (#[async_trait]).
#[async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&self, data: &[u8]) -> Result<u64>;       // returns offset
    async fn sync(&self) -> Result<()>;
    async fn read_from(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn truncate_before(&self, offset: u64) -> Result<()>;
    async fn size(&self) -> u64;
}

/// File-backed storage (durable, crash-safe)
pub struct FilePageStorage { file: File, page_size: usize }
pub struct FileWalStorage { dir: PathBuf, segment_size: usize }

/// In-memory storage (ephemeral, zero I/O)
/// No fsync, no DWB, no crash recovery — data lives only in RAM.
pub struct MemoryPageStorage { pages: tokio::sync::RwLock<Vec<Vec<u8>>>, page_size: usize }
pub struct MemoryWalStorage { log: tokio::sync::RwLock<Vec<u8>> }
```

**Design rules:**
- `BufferPool` takes `Arc<dyn PageStorage>` instead of `File`
- `WalWriter` takes `Arc<dyn WalStorage>` instead of a path
- `DoubleWriteBuffer` is skipped entirely for in-memory backends (no torn writes possible)
- Checkpoint is a no-op for in-memory backends (nothing to flush)
- Recovery is a no-op for in-memory backends (nothing to recover)
- All other engine code (B-tree, heap, free list, slotted pages) works identically regardless of backend

## Public Facade

```rust
pub struct StorageEngine { /* ... */ }

impl StorageEngine {
    // Lifecycle — file-backed (durable), all async
    pub async fn open(path: &Path, config: StorageConfig) -> Result<Self>;

    // Lifecycle — in-memory (ephemeral)
    pub async fn open_in_memory(config: StorageConfig) -> Result<Self>;

    // Lifecycle — custom backend
    pub async fn open_with_backend(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
    ) -> Result<Self>;

    pub async fn close(&mut self) -> Result<()>;

    /// Returns true if this engine uses durable (file-backed) storage
    pub fn is_durable(&self) -> bool;

    // B-tree management (async)
    pub async fn create_btree(&self) -> Result<BTreeHandle>;
    pub async fn open_btree(&self, root_page: PageId) -> BTreeHandle;

    // Large value storage (async)
    pub async fn heap_store(&self, data: &[u8]) -> Result<HeapRef>;
    pub async fn heap_load(&self, href: HeapRef) -> Result<Vec<u8>>;
    pub async fn heap_free(&self, href: HeapRef) -> Result<()>;

    // WAL (async)
    pub async fn append_wal(&self, record_type: u8, payload: &[u8]) -> Result<Lsn>;
    pub fn read_wal_from(&self, lsn: Lsn) -> WalStream;

    // Maintenance (async)
    pub async fn checkpoint(&self) -> Result<()>;  // no-op for in-memory
    pub async fn recover(&mut self) -> Result<()>; // no-op for in-memory

    // Internal access (for integration layer)
    pub fn buffer_pool(&self) -> &BufferPool;
    pub fn file_header(&self) -> &FileHeader;
}

pub struct BTreeHandle { /* ... */ }

impl BTreeHandle {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn delete(&self, key: &[u8]) -> Result<bool>;
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                direction: ScanDirection) -> ScanStream;
    pub fn root_page(&self) -> PageId;
}
```

## Modules

### `page.rs` — Slotted Page Format

**WHY HERE:** Generic byte-level page format operating on fixed-size buffers. No knowledge of what data the slots contain.

```rust
pub type PageId = u32;

pub enum PageType {
    BTreeInternal = 0x01,
    BTreeLeaf     = 0x02,
    Heap          = 0x03,
    Overflow      = 0x04,
    Free          = 0x05,
    FileHeader    = 0x06,
    FileHeaderShadow = 0x07,
}

/// 32-byte page header
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub flags: u8,
    pub num_slots: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub prev_or_ptr: u32,    // right_sibling / leftmost_child / next_free
    pub _reserved: u32,
    pub checksum: u32,
    pub lsn: u64,
}

/// Operations on a page buffer
pub struct SlottedPage<'a> { buf: &'a mut [u8] }

impl<'a> SlottedPage<'a> {
    pub fn init(buf: &'a mut [u8], page_id: PageId, page_type: PageType) -> Self;
    pub fn from_buf(buf: &'a mut [u8]) -> Self;
    pub fn header(&self) -> PageHeader;
    pub fn set_header(&mut self, header: &PageHeader);
    pub fn num_slots(&self) -> u16;
    pub fn slot_data(&self, slot: u16) -> &[u8];
    pub fn insert_slot(&mut self, data: &[u8]) -> Result<u16>;
    pub fn delete_slot(&mut self, slot: u16);
    pub fn free_space(&self) -> usize;
    pub fn compact(&mut self);  // defragment
    pub fn compute_checksum(&self) -> u32;
    pub fn verify_checksum(&self) -> bool;
}
```

### `buffer_pool.rs` — In-Memory Page Cache

**WHY HERE:** Generic page caching layer. Knows about pages as byte buffers, not about what they store.

```rust
pub struct BufferPool {
    page_table: tokio::sync::RwLock<HashMap<PageId, FrameId>>,
    frames: Vec<FrameSlot>,
    clock_hand: AtomicU32,
    page_storage: Arc<dyn PageStorage>,  // backend-agnostic
    page_size: usize,
}

pub struct FrameSlot {
    lock: tokio::sync::RwLock<FrameData>,
}

struct FrameData {
    data: Vec<u8>,       // [u8; PAGE_SIZE]
    page_id: Option<PageId>,
    pin_count: u32,
    dirty: bool,
    ref_bit: bool,
}

/// RAII shared page guard (multiple concurrent readers)
pub struct SharedPageGuard<'a> { /* ... */ }
impl SharedPageGuard<'_> {
    pub fn data(&self) -> &[u8];
    pub fn page_id(&self) -> PageId;
}

/// RAII exclusive page guard (single writer)
pub struct ExclusivePageGuard<'a> { /* ... */ }
impl ExclusivePageGuard<'_> {
    pub fn data(&self) -> &[u8];
    pub fn data_mut(&mut self) -> &mut [u8];  // marks dirty on drop
    pub fn page_id(&self) -> PageId;
}

impl BufferPool {
    pub fn new(config: BufferPoolConfig, page_storage: Arc<dyn PageStorage>) -> Self;
    pub async fn fetch_page_shared(&self, page_id: PageId) -> Result<SharedPageGuard>;
    pub async fn fetch_page_exclusive(&self, page_id: PageId) -> Result<ExclusivePageGuard>;
    pub async fn new_page(&self) -> Result<ExclusivePageGuard>;
    pub async fn flush_page(&self, page_id: PageId) -> Result<()>;
    pub fn dirty_frames(&self) -> Vec<(PageId, Vec<u8>)>;  // snapshot for checkpoint
}
```

### `btree.rs` — Generic B+ Tree

**WHY HERE:** Operates on raw byte keys and values with `memcmp` ordering. No knowledge of what keys represent (could be doc_id||ts, encoded scalars, or anything else).

```rust
pub enum ScanDirection { Forward, Backward }

pub struct ScanStream { /* ... */ }
impl Stream for ScanStream {
    type Item = Result<(Vec<u8>, Vec<u8>)>;  // (key, value) pairs
}

pub struct BTree {
    root_page: PageId,       // permanent, never changes after creation
    buffer_pool: Arc<BufferPool>,
}

impl BTree {
    pub async fn new(buffer_pool: Arc<BufferPool>) -> Result<Self>;  // allocates root
    pub fn open(root_page: PageId, buffer_pool: Arc<BufferPool>) -> Self;
    pub fn root_page(&self) -> PageId;
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn delete(&self, key: &[u8]) -> Result<bool>;
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                dir: ScanDirection) -> ScanStream;

    // Internal operations
    fn search_leaf(&self, key: &[u8]) -> Result<SharedPageGuard>;
    fn split_leaf(&self, page: ExclusivePageGuard, key: &[u8]) -> Result<()>;
    fn split_internal(&self, page: ExclusivePageGuard) -> Result<()>;
    fn merge_or_redistribute(&self, page: ExclusivePageGuard) -> Result<()>;
}
```

### `wal.rs` — Write-Ahead Log

**WHY HERE:** Appends opaque byte records with CRC verification. Does not interpret record payloads.

```rust
pub type Lsn = u64;

/// WAL segment file header (32 bytes)
pub struct SegmentHeader {
    pub magic: u32,        // 0x57414C00
    pub version: u16,
    pub segment_id: u32,
    pub base_lsn: Lsn,
    pub created_at_ms: u64,
}

/// Raw WAL record (header + payload)
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: u8,
    pub payload: Vec<u8>,
}

pub struct WalWriter {
    tx: mpsc::Sender<WalWriteRequest>,
}

impl WalWriter {
    pub fn new(storage: Arc<dyn WalStorage>, config: WalConfig) -> Result<Self>;
    pub async fn append(&self, record_type: u8, payload: &[u8]) -> Result<Lsn>;
    pub async fn append_raw_frame(&self, raw: &[u8]) -> Result<Lsn>;  // for replication
    pub async fn shutdown(&self) -> Result<()>;
}

pub struct WalReader { /* ... */ }
impl WalReader {
    pub fn open(storage: Arc<dyn WalStorage>) -> Result<Self>;
    pub fn read_from(&self, lsn: Lsn) -> WalStream;
    pub fn latest_lsn(&self) -> Lsn;
}

pub struct WalStream { /* ... */ }
impl Stream for WalStream {
    type Item = Result<WalRecord>;
}
```

### `heap.rs` — External Large Value Storage

**WHY HERE:** Stores arbitrary byte blobs in heap pages with overflow chains. No knowledge of document structure.

```rust
#[derive(Clone, Copy)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

pub struct Heap {
    buffer_pool: Arc<BufferPool>,
    free_space_map: HashMap<PageId, usize>,
}

impl Heap {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self;
    pub async fn store(&mut self, data: &[u8]) -> Result<HeapRef>;
    pub async fn load(&self, href: HeapRef) -> Result<Vec<u8>>;
    pub async fn free(&mut self, href: HeapRef) -> Result<()>;
    pub async fn rebuild_free_space_map(&mut self) -> Result<()>;  // on startup
}
```

### `free_list.rs` — Free Page Management

**WHY HERE:** Tracks available pages. Pure page-level bookkeeping with no domain awareness.

```rust
pub struct FreeList {
    head: PageId,  // 0 = empty
    buffer_pool: Arc<BufferPool>,
}

impl FreeList {
    pub fn new(head: PageId, buffer_pool: Arc<BufferPool>) -> Self;
    pub async fn allocate(&mut self) -> Result<PageId>;  // pop from list, or extend file
    pub async fn deallocate(&mut self, page_id: PageId) -> Result<()>;  // push to list
    pub async fn count(&self) -> Result<usize>;
    pub fn head(&self) -> PageId;
}
```

### `dwb.rs` — Double-Write Buffer

**WHY HERE:** Torn-write protection for the page store. Operates on raw page bytes. **Only used with file-backed storage** — in-memory backends skip DWB entirely (no torn writes possible).

```rust
pub struct DoubleWriteBuffer {
    page_storage: Arc<dyn PageStorage>,  // writes restored pages to backend
    dwb_path: PathBuf,                   // DWB file is always on disk
    page_size: usize,
}

impl DoubleWriteBuffer {
    pub fn new(path: &Path, page_storage: Arc<dyn PageStorage>, page_size: usize) -> Self;
    pub fn write_pages(&self, pages: &[(PageId, &[u8])]) -> Result<()>;  // sequential write + fsync
    pub fn recover(&self) -> Result<u32>;  // returns pages restored
    pub fn truncate(&self) -> Result<()>;
    pub fn is_empty(&self) -> Result<bool>;
}
```

### `checkpoint.rs` — Checkpoint Coordinator

**WHY HERE:** Flushes dirty pages through DWB to data file. Operates on pages and LSNs, not on document semantics.

```rust
pub struct Checkpoint {
    buffer_pool: Arc<BufferPool>,
    dwb: DoubleWriteBuffer,
    wal_writer: WalWriter,
}

impl Checkpoint {
    pub fn new(bp: Arc<BufferPool>, dwb: DoubleWriteBuffer, wal: WalWriter) -> Self;
    pub async fn run(&self) -> Result<Lsn>;  // returns checkpoint_lsn (async)
    // Steps: snapshot dirty → DWB write → scatter-write → mark clean → WAL record
}
```

### `recovery.rs` — Crash Recovery

**WHY HERE:** Restores page store from DWB + WAL replay. Calls back to higher layers for record interpretation.

```rust
pub trait WalRecordHandler {
    fn handle_record(&mut self, record: &WalRecord) -> Result<()>;
}

pub struct Recovery {
    storage: StorageEngine,
}

impl Recovery {
    pub async fn run(path: &Path, config: StorageConfig,
               handler: &mut dyn WalRecordHandler) -> Result<StorageEngine>;
    pub async fn needs_recovery(path: &Path) -> Result<bool>;
    // Steps: read meta.json → DWB recovery → open data.db → WAL replay
}
```

### `vacuum.rs` — Page-Level Vacuum

**WHY HERE:** Removes entries from B-trees and reclaims pages. Operates on raw keys — the higher layer tells it which keys to remove.

```rust
pub struct VacuumTask {
    engine: Arc<StorageEngine>,
}

impl VacuumTask {
    /// Remove a set of key/value entries from specified B-trees and reclaim freed pages
    pub fn remove_entries(&self, removals: &[VacuumRemoval]) -> Result<()>;
}

pub struct VacuumRemoval {
    pub btree_root: PageId,
    pub key: Vec<u8>,
}
```

### `catalog_btree.rs` — Catalog B-Tree Schema

**WHY HERE:** Defines the key format for catalog entries stored in the catalog B-tree. The catalog B-tree is a regular B-tree in Layer 2 with a well-known key schema. No MVCC or document awareness.

```rust
/// Catalog entity types stored in the catalog B-tree
pub enum CatalogEntityType {
    Collection = 0x01,
    Index      = 0x02,
}

/// Catalog B-tree key: entity_type[1] || entity_id[8] (big-endian)
pub fn make_catalog_key(entity_type: CatalogEntityType, entity_id: u64) -> [u8; 9];

/// Secondary catalog key for name lookups:
/// entity_type[1] || name_bytes[var] || 0x00
pub fn make_catalog_name_key(entity_type: CatalogEntityType, name: &str) -> Vec<u8>;

/// Serialize a collection entry to bytes
pub fn serialize_collection_entry(entry: &CollectionEntry) -> Vec<u8>;
pub fn deserialize_collection_entry(data: &[u8]) -> Result<CollectionEntry>;

/// Serialize an index entry to bytes
pub fn serialize_index_entry(entry: &IndexEntry) -> Vec<u8>;
pub fn deserialize_index_entry(data: &[u8]) -> Result<IndexEntry>;

/// Catalog entries — raw storage representation (no MVCC)
pub struct CollectionEntry {
    pub collection_id: u64,
    pub name: String,
    pub primary_root_page: PageId,
    pub created_at_root_page: PageId,
    pub doc_count: u64,
}

pub struct IndexEntry {
    pub index_id: u64,
    pub collection_id: u64,
    pub name: String,
    pub field_paths: Vec<Vec<String>>,  // raw segments, not FieldPath (no L1 dep)
    pub root_page: PageId,
    pub state: u8,  // Building=0x01, Ready=0x02, Dropping=0x03
}
```

### `engine.rs` — StorageEngine Coordinator

**WHY HERE:** Composes all Layer 2 sub-modules into the unified facade. No domain logic.

```rust
pub struct StorageConfig {
    pub page_size: usize,
    pub memory_budget: usize,
    pub wal_segment_size: usize,
    pub checkpoint_wal_threshold: usize,
    pub checkpoint_interval: Duration,
}

pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub free_list_head: PageId,
    pub catalog_root_page: PageId,
    pub next_collection_id: u64,
    pub next_index_id: u64,
    pub checkpoint_lsn: Lsn,
    pub created_at: u64,
}
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `StorageEngine` | L3, L6 (Database) | Full storage facade |
| `BTreeHandle` | L3 (PrimaryIndex, SecondaryIndex) | Raw byte B-tree ops |
| `ScanStream` | L3, L4 | Range scan results |
| `HeapRef`, `heap_store/load/free` | L3 (large doc storage) | External blob storage |
| `WalWriter`, `WalReader`, `WalRecord` | L5 (commit), L7 (replication) | Durable logging |
| `Lsn` | L5, L7 | WAL position tracking |
| `PageStorage`, `WalStorage` traits | L2 internal, custom backends | Backend abstraction |
| `FilePageStorage`, `MemoryPageStorage` | L2 (built-in backends) | Concrete implementations |
| `BufferPool` | Checkpoint, Recovery | Page management |
| `FileHeader` | L6 (Database) | Metadata (catalog root, ID allocators) |
| `CollectionEntry`, `IndexEntry` | L6 (CatalogCache) | Catalog B-tree data |
| `Checkpoint` | L6 (background task) | Maintenance |
