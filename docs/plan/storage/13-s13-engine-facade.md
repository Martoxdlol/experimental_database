# S13: StorageEngine Facade

## Purpose

Compose all sub-layers into the unified StorageEngine struct. This is the public API of Layer 2. All access from higher layers goes through this facade.

## Dependencies

- **All sub-layers S1–S12**

## Rust Types

```rust
use crate::storage::backend::*;
use crate::storage::page::*;
use crate::storage::buffer_pool::*;
use crate::storage::free_list::*;
use crate::storage::wal::*;
use crate::storage::btree::*;
use crate::storage::heap::*;
use crate::storage::dwb::*;
use crate::storage::checkpoint::*;
use crate::storage::recovery::*;
use crate::storage::vacuum::*;
use crate::storage::catalog_btree::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Storage engine configuration.
pub struct StorageConfig {
    pub page_size: usize,            // default 8192
    pub memory_budget: usize,        // default 256 MB (determines frame count)
    pub wal_segment_size: usize,     // default 64 MB
    pub checkpoint_wal_threshold: usize, // bytes of WAL before auto-checkpoint
    pub checkpoint_interval: Duration,   // time between auto-checkpoints
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            page_size: 8192,
            memory_budget: 256 * 1024 * 1024,
            wal_segment_size: 64 * 1024 * 1024,
            checkpoint_wal_threshold: 64 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(300),
        }
    }
}

/// File header stored in page 0 of data.db.
/// Uses `zerocopy` with LE wrapper types (same as PageHeader — read/written via buffer pool).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Debug)]
#[repr(C)]
pub struct FileHeader {
    pub magic: U32<LittleEndian>,               // 0x45584442 ("EXDB")
    pub version: U32<LittleEndian>,             // format version (1)
    pub page_size: U32<LittleEndian>,
    pub page_count: U64<LittleEndian>,
    pub free_list_head: U32<LittleEndian>,      // PageId
    pub catalog_root_page: U32<LittleEndian>,   // PageId
    pub catalog_name_root_page: U32<LittleEndian>, // by-name B-tree root
    pub _reserved: [u8; 4],                     // reserved for future use
    pub next_collection_id: U64<LittleEndian>,
    pub next_index_id: U64<LittleEndian>,
    pub checkpoint_lsn: U64<LittleEndian>,      // Lsn
    pub visible_ts: U64<LittleEndian>,          // latest visible timestamp
    pub generation: U64<LittleEndian>,          // Cluster generation counter. Incremented when a node starts fresh (no local data). Used by replication to distinguish reconnection from replacement.
    pub created_at: U64<LittleEndian>,          // millis since epoch
}
// Layout is packed (zerocopy LE wrapper types have alignment 1).
// Total size: 4+4+4+8+4+4+4+4+8+8+8+8+8+8 = 84 bytes.
// Remainder of page 0 is zeroed (reserved for future fields).

impl FileHeader {
    pub fn verify(&self) -> Result<()>;  // check magic + version
}
// Note: FileHeader derives zerocopy's FromBytes + IntoBytes, so it can be
// read/written directly via those traits (e.g. read_from_bytes / as_bytes).
// No serialize()/deserialize() methods are needed.

/// The main storage engine.
pub struct StorageEngine {
    // Backends
    page_storage: Arc<dyn PageStorage>,
    wal_storage: Arc<dyn WalStorage>,

    // Sub-components
    buffer_pool: Arc<BufferPool>,
    free_list: Mutex<FreeList>,
    wal_writer: Arc<WalWriter>,
    wal_reader: WalReader,
    heap: Mutex<Heap>,
    checkpoint: Checkpoint,  // owns the DWB (if durable)
    vacuum_task: VacuumTask,

    // Metadata
    file_header: Mutex<FileHeader>,
    config: StorageConfig,
    is_durable: bool,
    path: Option<PathBuf>,  // None for in-memory
}

/// A handle to a B-tree, bound to the specific components it needs.
/// Holds references to the free list and buffer pool directly, avoiding
/// an Arc<StorageEngine> cycle (StorageEngine owns BTreeHandles indirectly
/// through the catalog, so a back-Arc would create a reference cycle).
pub struct BTreeHandle {
    btree: BTree,
    free_list: Arc<Mutex<FreeList>>,
    buffer_pool: Arc<BufferPool>,
}

impl StorageEngine {
    // ─── Lifecycle ───

    /// Open a file-backed storage engine (durable).
    /// Runs recovery if needed.
    pub fn open(
        path: &Path,
        config: StorageConfig,
        handler: &mut dyn WalRecordHandler,
    ) -> Result<Self>;

    /// Open an ephemeral in-memory storage engine.
    pub fn open_in_memory(config: StorageConfig) -> Result<Self>;

    /// Open with custom backends.
    /// If the backend is durable and `handler` is Some, runs recovery.
    /// If the backend is durable and `handler` is None, recovery is skipped
    /// (caller's responsibility to ensure consistency).
    /// If the backend is not durable, `handler` is ignored.
    pub fn open_with_backend(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
        handler: Option<&mut dyn WalRecordHandler>,
    ) -> Result<Self>;

    /// Close the engine: final checkpoint, flush, shutdown WAL writer.
    pub async fn close(&mut self) -> Result<()>;

    /// Whether this engine uses durable storage.
    pub fn is_durable(&self) -> bool;

    // ─── B-Tree Management ───

    /// Create a new B-tree with an empty root. Returns a handle.
    pub fn create_btree(&self) -> Result<BTreeHandle>;

    /// Open an existing B-tree by root page.
    pub fn open_btree(&self, root_page: PageId) -> BTreeHandle;

    // ─── Heap ───

    /// Store a blob in the heap. Returns a reference for later retrieval.
    pub fn heap_store(&self, data: &[u8]) -> Result<HeapRef>;

    /// Load a blob from the heap.
    pub fn heap_load(&self, href: HeapRef) -> Result<Vec<u8>>;

    /// Free a blob from the heap.
    pub fn heap_free(&self, href: HeapRef) -> Result<()>;

    // ─── WAL ───

    /// Append a WAL record. Returns the assigned LSN after fsync.
    pub async fn append_wal(&self, record_type: u8, payload: &[u8]) -> Result<Lsn>;

    /// Read WAL records starting from a given LSN.
    pub fn read_wal_from(&self, lsn: Lsn) -> WalIterator;

    // ─── Maintenance ───

    /// Run a checkpoint. No-op for non-durable engines (except WAL record).
    pub async fn checkpoint(&self) -> Result<()>;

    /// Vacuum entries from B-trees.
    pub fn vacuum(&self, entries: &[VacuumEntry]) -> Result<usize>;

    // ─── Accessors (for integration layer) ───

    pub fn buffer_pool(&self) -> &Arc<BufferPool>;
    pub fn file_header(&self) -> FileHeader;
    pub fn update_file_header<F>(&self, f: F) -> Result<()>
        where F: FnOnce(&mut FileHeader);
    pub fn wal_writer(&self) -> &Arc<WalWriter>;
    pub fn config(&self) -> &StorageConfig;
}

impl BTreeHandle {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<bool>;
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                direction: ScanDirection) -> ScanIterator;
    pub fn root_page(&self) -> PageId;
}
```

## Implementation Details

### open() — File-Backed

```
StorageEngine::open(path, config, handler):

1. Create/open directories:
   - Ensure path/ exists
   - Ensure path/wal/ exists

2. Open backends:
   - page_storage = FilePageStorage::open(path/data.db, config.page_size)
   - wal_storage = FileWalStorage::open(path/wal/, config.wal_segment_size)

3. Check if this is a new database:
   - If data.db is empty (0 pages):
     a. Initialize file header on page 0
     b. Create initial catalog B-trees (by-ID and by-Name, each an empty leaf)
     c. Write file header with catalog root pages
     d. No recovery needed
   - If data.db exists:
     a. Read page 0 → FileHeader
     b. Verify magic + version
     c. Read checkpoint_lsn and visible_ts from FileHeader

4. Recovery (if existing database):
   - dwb = DoubleWriteBuffer::new(path/data.dwb, page_storage, config.page_size)
   - Recovery::run(page_storage, wal_storage, Some(path/data.dwb),
                   checkpoint_lsn, config.page_size, handler)

5. Build components:
   - buffer_pool = BufferPool::new(config, page_storage)
   - free_list = FreeList::new(file_header.free_list_head, buffer_pool)
   - wal_writer = WalWriter::new(wal_storage, wal_config)
   - wal_reader = WalReader::new(wal_storage)
   - heap = Heap::new(buffer_pool)
   - heap.rebuild_free_space_map()
   - checkpoint = Checkpoint::new(buffer_pool, Some(dwb), wal_writer, true)
   - vacuum_task = VacuumTask::new(buffer_pool)

6. Return StorageEngine
```

### open_in_memory()

```
StorageEngine::open_in_memory(config):

1. Create backends:
   - page_storage = MemoryPageStorage::new(config.page_size)
   - wal_storage = MemoryWalStorage::new()

2. Initialize:
   - Extend page_storage to 1 page (page 0)
   - Write file header on page 0
   - Create initial catalog B-trees

3. Build components (same as open, but):
   - dwb = None
   - checkpoint = Checkpoint::new(buffer_pool, None, wal_writer, false)
   - No recovery needed

4. Return StorageEngine
```

### open_with_backend()

Like open_in_memory() but with user-provided backends. Checks `page_storage.is_durable()` to determine DWB/checkpoint behavior. If the backend is durable and `handler` is `Some`, recovery runs automatically (same as `open()`). If the backend is durable and `handler` is `None`, recovery is skipped — the caller is responsible for ensuring consistency. If the backend is not durable, `handler` is ignored and no recovery runs.

### close()

```
1. Run final checkpoint (if durable)
2. Shutdown WAL writer
3. Flush buffer pool (discard remaining clean frames)
4. Write final file header to page 0 (including checkpoint_lsn and visible_ts)
5. page_storage.sync()
```

### Recovery

Recovery is **not** a public API method. It runs automatically inside `open()` (step 4) when an existing database is detected. There is no `StorageEngine::recover()` — callers simply call `open()` and recovery happens transparently.

For testing crash recovery, tests should:
1. Open an engine and write data (without a clean `close()`).
2. Drop the engine (simulating a crash).
3. Call `open()` again on the same path — recovery runs automatically.

### Auto-Checkpoint

The `checkpoint_wal_threshold` and `checkpoint_interval` fields in `StorageConfig` control a background checkpoint task that is spawned inside `open()` for durable engines.

The background task works as follows:
- It sleeps for `checkpoint_interval` (default 300s), then wakes and checks whether a checkpoint is needed.
- It also wakes early if the WAL size exceeds `checkpoint_wal_threshold` (default 64 MB). The WAL writer notifies the task when this threshold is crossed.
- On wake, it calls `self.checkpoint()` to flush dirty pages and advance the checkpoint LSN.
- The task is a `tokio::spawn`ed future that holds an `Arc<StorageEngine>` (or a weak reference) and listens on a shutdown channel.
- `close()` sends a shutdown signal to the task and awaits its completion before proceeding with the final checkpoint and flush.

For in-memory engines, no background checkpoint task is spawned (checkpointing is a no-op).

### BTreeHandle Operations

BTreeHandle wraps a BTree and holds `Arc` references to the free list and buffer pool directly (not to the entire `StorageEngine`). This avoids a reference cycle since `StorageEngine` owns the catalog which indirectly owns `BTreeHandle`s.

```rust
impl BTreeHandle {
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut free_list = self.free_list.lock().unwrap();
        self.btree.insert(key, value, &mut free_list)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut free_list = self.free_list.lock().unwrap();
        self.btree.delete(key, &mut free_list)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.btree.get(key)  // no free list needed for reads
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                direction: ScanDirection) -> ScanIterator {
        self.btree.scan(lower, upper, direction)
    }
}
```

### File Header Management

- File header is on page 0, read at startup.
- Updated via `update_file_header()` which acquires the file header lock, applies the update, and writes page 0 through the buffer pool.
- Updated after: checkpoint (checkpoint_lsn, visible_ts), create/drop collection (catalog roots, ID allocators), free list changes (free_list_head).

Both `checkpoint_lsn` and `visible_ts` are read from the FileHeader (page 0) on startup. No sidecar files are needed.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Directory not found | Invalid path | Return error |
| Magic mismatch | Wrong file or corrupt | Return error |
| Version mismatch | Incompatible format | Return error |
| Recovery failure | Corrupt DWB/WAL | Propagate |
| Backend failure | I/O errors | Propagate |

## Tests

### Unit Tests

1. **open + close file-backed**: Create engine, close, reopen, verify empty.
2. **open_in_memory**: Create in-memory engine, verify is_durable() == false.
3. **create_btree + insert + get**: Create B-tree, insert key, get it back.
4. **Multiple B-trees**: Create 3 B-trees, insert into each, verify isolation.
5. **Heap store + load**: Store blob, load back, verify.
6. **WAL append + read**: Append record, read back via read_wal_from.
7. **Checkpoint persists data**: Insert data, checkpoint, close, reopen, data still there.
8. **Recovery**: Insert data, DON'T checkpoint, simulate crash (just close without clean shutdown), reopen with recovery, data still there (WAL replay).
9. **File header updates**: Create collection (updates catalog root), checkpoint (updates checkpoint_lsn), verify header reflects changes.

### Integration Tests

10. **Full lifecycle**: open → create btree → insert 1000 entries → checkpoint → close → reopen → verify all entries.
11. **In-memory lifecycle**: open_in_memory → same operations → verify (no persistence check).
12. **Custom backend**: open_with_backend with MemoryPageStorage + MemoryWalStorage → same operations.
13. **Crash recovery end-to-end**: open → insert data → WAL record written → kill (don't close) → reopen → verify data recovered.
14. **Multiple engines**: Open two separate engines (different paths). Verify complete isolation.
15. **Catalog B-trees**: Insert collection/index entries into catalog B-trees. Reopen engine. Scan catalog and verify entries persisted.
