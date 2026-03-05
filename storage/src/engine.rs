//! StorageEngine facade composing all sub-layers.
//!
//! This is the public API of Layer 2. All access from higher layers goes through
//! this module. Composes backends, buffer pool, free list, WAL, B-trees, heap,
//! checkpoint, recovery, and vacuum into a single coherent interface.

use crate::backend::{MemoryPageStorage, MemoryWalStorage, PageId, PageStorage, WalStorage};
use crate::btree::{BTree, ScanDirection, ScanIterator};
use crate::buffer_pool::{BufferPool, BufferPoolConfig};
use crate::checkpoint::Checkpoint;
use crate::dwb::DoubleWriteBuffer;
use crate::free_list::FreeList;
use crate::heap::{Heap, HeapRef};
use crate::page::{PageType, SlottedPage};
use crate::recovery::{Recovery, RecoveryMode, WalRecordHandler};
use crate::vacuum::{VacuumEntry, VacuumTask};
use crate::wal::{Lsn, WalConfig, WalIterator, WalReader, WalWriter};

use parking_lot::Mutex;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use zerocopy::byteorder::{LittleEndian, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ─── Constants ───

/// Size of the page header in bytes (mirrors page.rs PAGE_HEADER_SIZE).
const PAGE_HEADER_SIZE: usize = 32;

/// Magic number for the file header: "EXDB" in little-endian.
const FILE_HEADER_MAGIC: u32 = 0x45584442;

/// Current file format version.
const FILE_HEADER_VERSION: u32 = 1;

/// Size of the FileHeader struct in bytes.
const FILE_HEADER_SIZE: usize = 84;

// Compile-time assertion that FileHeader is exactly 84 bytes.
const _: () = assert!(std::mem::size_of::<FileHeader>() == FILE_HEADER_SIZE);

// ─── StorageConfig ───

/// Storage engine configuration.
pub struct StorageConfig {
    /// Page size in bytes. Default: 8192.
    pub page_size: usize,
    /// Memory budget for the buffer pool in bytes. Default: 256 MB.
    /// Determines frame count as `memory_budget / page_size`.
    pub memory_budget: usize,
    /// WAL segment size in bytes. Default: 64 MB.
    pub wal_segment_size: usize,
    /// Bytes of WAL written before triggering an auto-checkpoint.
    pub checkpoint_wal_threshold: usize,
    /// Time between auto-checkpoint checks.
    pub checkpoint_interval: Duration,
}

impl StorageConfig {
    /// Validate the configuration, returning an error on invalid values.
    pub fn validate(&self) -> io::Result<()> {
        // Page header uses u16 for offsets/lengths, so max page size is 65535.
        // DwbHeader::page_size is also u16.
        if self.page_size > u16::MAX as usize {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_size {} exceeds maximum of {} (u16 offset limit)",
                self.page_size,
                u16::MAX
            )).into());
        }
        if self.page_size < 64 {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_size {} is too small (minimum 64)", self.page_size
            )).into());
        }
        if self.memory_budget < self.page_size {
            return Err(crate::error::StorageError::InvalidConfig(
                "memory_budget must be at least page_size".into()
            ).into());
        }
        Ok(())
    }
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

// ─── FileHeader ───

/// File header stored in page 0 of the data file.
///
/// Uses `zerocopy` with LE wrapper types for zero-copy read/write.
/// Stored at offset `PAGE_HEADER_SIZE` (32) within page 0.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Debug)]
#[repr(C)]
pub struct FileHeader {
    /// Magic number: 0x45584442 ("EXDB").
    pub magic: U32<LittleEndian>,
    /// Format version (1).
    pub version: U32<LittleEndian>,
    /// Page size in bytes.
    pub page_size: U32<LittleEndian>,
    /// Total number of pages in the data file.
    pub page_count: U64<LittleEndian>,
    /// Head of the free page list (PageId). 0 = empty.
    pub free_list_head: U32<LittleEndian>,
    /// Root page of the catalog by-ID B-tree.
    pub catalog_root_page: U32<LittleEndian>,
    /// Root page of the catalog by-name B-tree.
    pub catalog_name_root_page: U32<LittleEndian>,
    /// Reserved for future use.
    pub _reserved: [u8; 4],
    /// Next collection ID to allocate.
    pub next_collection_id: U64<LittleEndian>,
    /// Next index ID to allocate.
    pub next_index_id: U64<LittleEndian>,
    /// LSN of the last completed checkpoint.
    pub checkpoint_lsn: U64<LittleEndian>,
    /// Latest visible timestamp for MVCC.
    pub visible_ts: U64<LittleEndian>,
    /// Cluster generation counter.
    pub generation: U64<LittleEndian>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: U64<LittleEndian>,
}

impl FileHeader {
    /// Create a new FileHeader with default values for a fresh database.
    fn new(page_size: usize) -> Self {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        FileHeader {
            magic: U32::new(FILE_HEADER_MAGIC),
            version: U32::new(FILE_HEADER_VERSION),
            page_size: U32::new(page_size as u32),
            page_count: U64::new(0),
            free_list_head: U32::new(0),
            catalog_root_page: U32::new(0),
            catalog_name_root_page: U32::new(0),
            _reserved: [0u8; 4],
            next_collection_id: U64::new(1),
            next_index_id: U64::new(1),
            checkpoint_lsn: U64::new(0),
            visible_ts: U64::new(0),
            generation: U64::new(1),
            created_at: U64::new(now_millis),
        }
    }

    /// Verify the magic number and version.
    pub fn verify(&self) -> io::Result<()> {
        if self.magic.get() != FILE_HEADER_MAGIC {
            return Err(crate::error::StorageError::Corruption(format!(
                "file header magic mismatch: expected 0x{:08X}, got 0x{:08X}",
                FILE_HEADER_MAGIC,
                self.magic.get()
            )).into());
        }
        if self.version.get() != FILE_HEADER_VERSION {
            return Err(crate::error::StorageError::Corruption(format!(
                "file header version mismatch: expected {}, got {}",
                FILE_HEADER_VERSION,
                self.version.get()
            )).into());
        }
        Ok(())
    }
}

/// Read a FileHeader from a page buffer at offset PAGE_HEADER_SIZE.
fn read_file_header(buf: &[u8]) -> io::Result<FileHeader> {
    let start = PAGE_HEADER_SIZE;
    let end = start + FILE_HEADER_SIZE;
    if buf.len() < end {
        return Err(crate::error::StorageError::Corruption(
            "page 0 buffer too small for FileHeader".into()
        ).into());
    }
    FileHeader::read_from_bytes(&buf[start..end]).map_err(|e| {
        io::Error::from(crate::error::StorageError::Corruption(
            format!("failed to read FileHeader: {:?}", e),
        ))
    })
}

/// Write a FileHeader into a page buffer at offset PAGE_HEADER_SIZE.
fn write_file_header(buf: &mut [u8], header: &FileHeader) {
    let start = PAGE_HEADER_SIZE;
    let end = start + FILE_HEADER_SIZE;
    buf[start..end].copy_from_slice(header.as_bytes());
}

// ─── BTreeHandle ───

/// A handle to a B-tree, bound to the specific components it needs.
///
/// Holds references to the free list and buffer pool directly, avoiding
/// an `Arc<StorageEngine>` cycle.
pub struct BTreeHandle {
    btree: BTree,
    free_list: Arc<Mutex<FreeList>>,
    #[allow(dead_code)]
    buffer_pool: Arc<BufferPool>,
}

impl BTreeHandle {
    /// Point lookup. Returns value bytes if found.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.btree.get(key)
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut free_list = self.free_list.lock();
        self.btree.insert(key, value, &mut free_list)
    }

    /// Delete a key. Returns true if the key existed.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        let mut free_list = self.free_list.lock();
        self.btree.delete(key, &mut free_list)
    }

    /// Range scan.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        direction: ScanDirection,
    ) -> ScanIterator {
        self.btree.scan(lower, upper, direction)
    }

    /// Root page of the underlying B-tree.
    pub fn root_page(&self) -> PageId {
        self.btree.root_page()
    }
}

// ─── StorageEngine ───

/// The main storage engine facade.
///
/// Composes all sub-layers (backends, buffer pool, free list, WAL, heap,
/// checkpoint, recovery, vacuum) into a single coherent API.
pub struct StorageEngine {
    // Backends
    page_storage: Arc<dyn PageStorage>,
    #[allow(dead_code)]
    wal_storage: Arc<dyn WalStorage>,

    // Sub-components
    buffer_pool: Arc<BufferPool>,
    free_list: Arc<Mutex<FreeList>>,
    wal_writer: Arc<WalWriter>,
    wal_reader: WalReader,
    heap: Mutex<Heap>,
    checkpoint: Checkpoint,
    vacuum_task: VacuumTask,

    // Metadata
    file_header: Mutex<FileHeader>,
    config: StorageConfig,
    is_durable: bool,
    #[allow(dead_code)]
    path: Option<PathBuf>,
}

impl StorageEngine {
    // ─── Lifecycle ───

    /// Open a file-backed storage engine (durable).
    ///
    /// Creates directories if needed, opens or creates the data file and WAL,
    /// and runs recovery if an existing database is detected.
    pub fn open(
        path: &Path,
        config: StorageConfig,
        handler: &mut dyn WalRecordHandler,
    ) -> io::Result<Self> {
        use crate::backend::{FilePageStorage, FileWalStorage};

        // Ensure directories exist.
        std::fs::create_dir_all(path)?;
        let wal_dir = path.join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let data_path = path.join("data.db");
        let dwb_path = path.join("data.dwb");

        // Open or create page storage.
        let page_storage: Arc<dyn PageStorage> = if data_path.exists() {
            Arc::new(FilePageStorage::open(&data_path, config.page_size)?)
        } else {
            Arc::new(FilePageStorage::create(&data_path, config.page_size)?)
        };

        // Open or create WAL storage.
        let wal_storage: Arc<dyn WalStorage> = if wal_dir.join("segment-000001.wal").exists() {
            Arc::new(FileWalStorage::open(&wal_dir, config.wal_segment_size)?)
        } else {
            Arc::new(FileWalStorage::create(&wal_dir, config.wal_segment_size)?)
        };

        let is_new = page_storage.page_count() == 0;

        if is_new {
            // Initialize new database.
            Self::init_new_database(
                page_storage.clone(),
                wal_storage.clone(),
                config,
                true,
                Some(dwb_path),
                Some(path.to_path_buf()),
            )
        } else {
            // Existing database: read file header, run recovery.
            let mut buf = vec![0u8; config.page_size];
            page_storage.read_page(0, &mut buf)?;
            let file_header = read_file_header(&buf)?;
            file_header.verify()?;

            // Validate page_size matches the stored value.
            let stored_page_size = file_header.page_size.get() as usize;
            if stored_page_size != config.page_size {
                return Err(crate::error::StorageError::Corruption(format!(
                    "page size mismatch: file has {}, config has {}",
                    stored_page_size, config.page_size
                )).into());
            }

            let checkpoint_lsn = file_header.checkpoint_lsn.get();

            // Run recovery.
            let (_end_lsn, _stats) = Recovery::run(
                page_storage.as_ref(),
                wal_storage.as_ref(),
                Some(dwb_path.as_path()),
                checkpoint_lsn,
                config.page_size,
                handler,
                RecoveryMode::Strict,
            )?;

            // Build components.
            Self::build_from_existing(
                page_storage,
                wal_storage,
                file_header,
                config,
                true,
                Some(dwb_path),
                Some(path.to_path_buf()),
            )
        }
    }

    /// Open an ephemeral in-memory storage engine.
    pub fn open_in_memory(config: StorageConfig) -> io::Result<Self> {
        let page_storage: Arc<dyn PageStorage> =
            Arc::new(MemoryPageStorage::new(config.page_size));
        let wal_storage: Arc<dyn WalStorage> = Arc::new(MemoryWalStorage::new());

        Self::init_new_database(
            page_storage,
            wal_storage,
            config,
            false,
            None,
            None,
        )
    }

    /// Open with custom backends.
    ///
    /// If the backend is durable and `handler` is Some, runs recovery.
    /// If the backend is durable and `handler` is None, recovery is skipped.
    /// If the backend is not durable, `handler` is ignored.
    pub fn open_with_backend(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
        handler: Option<&mut dyn WalRecordHandler>,
    ) -> io::Result<Self> {
        config.validate()?;
        let is_durable = page_storage.is_durable();
        let is_new = page_storage.page_count() == 0;

        if is_new {
            Self::init_new_database(
                page_storage,
                wal_storage,
                config,
                is_durable,
                None,
                None,
            )
        } else {
            // Existing database.
            let mut buf = vec![0u8; config.page_size];
            page_storage.read_page(0, &mut buf)?;
            let file_header = read_file_header(&buf)?;
            file_header.verify()?;

            // Run recovery if durable and handler provided.
            if is_durable
                && let Some(h) = handler {
                    let checkpoint_lsn = file_header.checkpoint_lsn.get();
                    let (_end_lsn, _stats) = Recovery::run(
                        page_storage.as_ref(),
                        wal_storage.as_ref(),
                        None,
                        checkpoint_lsn,
                        config.page_size,
                        h,
                        RecoveryMode::Strict,
                    )?;
                }

            Self::build_from_existing(
                page_storage,
                wal_storage,
                file_header,
                config,
                is_durable,
                None,
                None,
            )
        }
    }

    /// Close the engine: final checkpoint if durable, shutdown WAL writer.
    pub async fn close(&self) -> io::Result<()> {
        // Run final checkpoint if durable.
        if self.is_durable {
            self.checkpoint().await?;
        }

        // Update file header with current free list head and page count
        // before writing to disk.
        {
            let free_list_head = self.free_list.lock().head();
            let page_count = self.page_storage.page_count();
            self.update_file_header(|fh| {
                fh.free_list_head = U32::new(free_list_head);
                fh.page_count = U64::new(page_count);
            })?;
        }

        // Write final file header to page 0.
        self.write_file_header_to_page0()?;

        // Sync page storage.
        self.page_storage.sync()?;

        Ok(())
    }

    /// Whether this engine uses durable storage.
    pub fn is_durable(&self) -> bool {
        self.is_durable
    }

    // ─── B-Tree Management ───

    /// Create a new B-tree with an empty root. Returns a handle.
    pub fn create_btree(&self) -> io::Result<BTreeHandle> {
        let mut free_list = self.free_list.lock();
        let btree = BTree::create(self.buffer_pool.clone(), &mut free_list)?;
        Ok(BTreeHandle {
            btree,
            free_list: self.free_list.clone(),
            buffer_pool: self.buffer_pool.clone(),
        })
    }

    /// Open an existing B-tree by root page.
    pub fn open_btree(&self, root_page: PageId) -> BTreeHandle {
        let btree = BTree::open(root_page, self.buffer_pool.clone());
        BTreeHandle {
            btree,
            free_list: self.free_list.clone(),
            buffer_pool: self.buffer_pool.clone(),
        }
    }

    // ─── Heap ───

    /// Store a blob in the heap. Returns a reference for later retrieval.
    pub fn heap_store(&self, data: &[u8]) -> io::Result<HeapRef> {
        let mut heap = self.heap.lock();
        let mut free_list = self.free_list.lock();
        heap.store(data, &mut free_list)
    }

    /// Load a blob from the heap.
    pub fn heap_load(&self, href: HeapRef) -> io::Result<Vec<u8>> {
        let heap = self.heap.lock();
        heap.load(href)
    }

    /// Free a blob from the heap.
    pub fn heap_free(&self, href: HeapRef) -> io::Result<()> {
        let mut heap = self.heap.lock();
        let mut free_list = self.free_list.lock();
        heap.free(href, &mut free_list)
    }

    // ─── WAL ───

    /// Append a WAL record. Returns the assigned LSN after fsync.
    pub async fn append_wal(&self, record_type: u8, payload: &[u8]) -> io::Result<Lsn> {
        self.wal_writer.append(record_type, payload).await
    }

    /// Read WAL records starting from a given LSN.
    pub fn read_wal_from(&self, lsn: Lsn) -> WalIterator {
        self.wal_reader.read_from(lsn)
    }

    // ─── Maintenance ───

    /// Run a checkpoint. Flushes dirty pages and writes a checkpoint WAL record.
    pub async fn checkpoint(&self) -> io::Result<()> {
        let checkpoint_lsn = self.checkpoint.run().await?;
        // Update file header with new checkpoint LSN.
        self.update_file_header(|fh| {
            fh.checkpoint_lsn = U64::new(checkpoint_lsn);
        })?;
        Ok(())
    }

    /// Vacuum entries from B-trees. Returns the number of entries removed.
    pub fn vacuum(&self, entries: &[VacuumEntry]) -> io::Result<usize> {
        let mut free_list = self.free_list.lock();
        self.vacuum_task.remove_entries(entries, &mut free_list)
    }

    // ─── Accessors (for integration layer) ───

    /// Return a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Return a copy of the current file header.
    pub fn file_header(&self) -> FileHeader {
        self.file_header.lock().clone()
    }

    /// Update the file header via a closure, then write page 0 through
    /// the buffer pool.
    pub fn update_file_header<F>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        let mut fh = self.file_header.lock();
        f(&mut fh);
        // Write updated header to page 0 through buffer pool.
        let mut guard = self.buffer_pool.fetch_page_exclusive(0)?;
        let buf = guard.data_mut();
        write_file_header(buf, &fh);
        // guard.mark_dirty() is called implicitly by data_mut()
        Ok(())
    }

    /// Return a reference to the WAL writer.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal_writer
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Return a reference to the free list.
    pub fn free_list(&self) -> &Arc<Mutex<FreeList>> {
        &self.free_list
    }

    // ─── Internal: Initialize new database ───

    /// Initialize a brand-new database: extend to page 0, write file header,
    /// create catalog B-trees.
    fn init_new_database(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
        is_durable: bool,
        dwb_path: Option<PathBuf>,
        db_path: Option<PathBuf>,
    ) -> io::Result<Self> {
        config.validate()?;
        let page_size = config.page_size;
        let frame_count = config.memory_budget / page_size;

        // Extend page storage to have page 0.
        page_storage.extend(1)?;

        // Initialize page 0 as FileHeader page.
        {
            let mut buf = vec![0u8; page_size];
            SlottedPage::init(&mut buf, 0, PageType::FileHeader);
            let fh = FileHeader::new(page_size);
            write_file_header(&mut buf, &fh);
            page_storage.write_page(0, &buf)?;
        }

        // Build buffer pool.
        let buffer_pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size,
                frame_count,
            },
            page_storage.clone(),
        ));

        // Build free list (empty).
        let free_list = Arc::new(Mutex::new(FreeList::new(0, buffer_pool.clone())));

        // Create catalog B-trees.
        let catalog_root_page;
        let catalog_name_root_page;
        {
            let mut fl = free_list.lock();
            let id_btree = BTree::create(buffer_pool.clone(), &mut fl)?;
            catalog_root_page = id_btree.root_page();

            let name_btree = BTree::create(buffer_pool.clone(), &mut fl)?;
            catalog_name_root_page = name_btree.root_page();
        }

        // Update file header with catalog root pages and current page count.
        let file_header = {
            let mut fh = FileHeader::new(page_size);
            fh.catalog_root_page = U32::new(catalog_root_page);
            fh.catalog_name_root_page = U32::new(catalog_name_root_page);
            fh.page_count = U64::new(page_storage.page_count());
            fh.free_list_head = U32::new(free_list.lock().head());
            fh
        };

        // Write updated header to page 0 through buffer pool.
        {
            let mut guard = buffer_pool.fetch_page_exclusive(0)?;
            let buf = guard.data_mut();
            write_file_header(buf, &file_header);
        }

        // Flush page 0 to storage so it's durable.
        buffer_pool.flush_page(0)?;

        // Build WAL writer and reader.
        let wal_writer = Arc::new(WalWriter::new(
            wal_storage.clone(),
            WalConfig {
                segment_size: config.wal_segment_size,
            },
        )?);
        let wal_reader = WalReader::new(wal_storage.clone());

        // Build heap (empty).
        let heap = Mutex::new(Heap::new(buffer_pool.clone()));

        // Build checkpoint.
        let dwb = dwb_path.map(|p| {
            DoubleWriteBuffer::new(&p, page_storage.clone(), page_size)
        });
        let checkpoint = Checkpoint::new(
            buffer_pool.clone(),
            dwb,
            wal_writer.clone(),
            is_durable,
        );

        // Build vacuum.
        let vacuum_task = VacuumTask::new(buffer_pool.clone());

        Ok(StorageEngine {
            page_storage,
            wal_storage,
            buffer_pool,
            free_list,
            wal_writer,
            wal_reader,
            heap,
            checkpoint,
            vacuum_task,
            file_header: Mutex::new(file_header),
            config,
            is_durable,
            path: db_path,
        })
    }

    /// Build a StorageEngine from an existing database (after recovery).
    fn build_from_existing(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        file_header: FileHeader,
        config: StorageConfig,
        is_durable: bool,
        dwb_path: Option<PathBuf>,
        db_path: Option<PathBuf>,
    ) -> io::Result<Self> {
        let page_size = config.page_size;
        let frame_count = config.memory_budget / page_size;

        // Build buffer pool.
        let buffer_pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size,
                frame_count,
            },
            page_storage.clone(),
        ));

        // Build free list from saved head.
        let free_list_head = file_header.free_list_head.get();
        let free_list = Arc::new(Mutex::new(FreeList::new(
            free_list_head,
            buffer_pool.clone(),
        )));

        // Build WAL writer and reader.
        let wal_writer = Arc::new(WalWriter::new(
            wal_storage.clone(),
            WalConfig {
                segment_size: config.wal_segment_size,
            },
        )?);
        let wal_reader = WalReader::new(wal_storage.clone());

        // Build heap and rebuild free space map.
        let mut heap = Heap::new(buffer_pool.clone());
        heap.rebuild_free_space_map()?;

        // Build checkpoint.
        let dwb = dwb_path.map(|p| {
            DoubleWriteBuffer::new(&p, page_storage.clone(), page_size)
        });
        let checkpoint = Checkpoint::new(
            buffer_pool.clone(),
            dwb,
            wal_writer.clone(),
            is_durable,
        );

        // Build vacuum.
        let vacuum_task = VacuumTask::new(buffer_pool.clone());

        Ok(StorageEngine {
            page_storage,
            wal_storage,
            buffer_pool,
            free_list,
            wal_writer,
            wal_reader,
            heap: Mutex::new(heap),
            checkpoint,
            vacuum_task,
            file_header: Mutex::new(file_header),
            config,
            is_durable,
            path: db_path,
        })
    }

    /// Write the current file header to page 0 via the buffer pool and flush.
    fn write_file_header_to_page0(&self) -> io::Result<()> {
        let fh = self.file_header.lock().clone();
        let mut guard = self.buffer_pool.fetch_page_exclusive(0)?;
        let buf = guard.data_mut();
        write_file_header(buf, &fh);
        drop(guard);
        self.buffer_pool.flush_page(0)?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::NoOpHandler;

    // ─── Test 1: open + close file-backed ───

    #[tokio::test]
    async fn test_open_close_file_backed() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");

        // Open new database.
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(
            &path,
            config,
            &mut NoOpHandler,
        )
        .unwrap();

        assert!(engine.is_durable());
        let fh = engine.file_header();
        assert_eq!(fh.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh.version.get(), FILE_HEADER_VERSION);
        assert_eq!(fh.page_size.get(), 4096);
        assert!(fh.catalog_root_page.get() > 0);
        assert!(fh.catalog_name_root_page.get() > 0);

        // Close.
        engine.close().await.unwrap();

        // Reopen.
        let config2 = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine2 = StorageEngine::open(
            &path,
            config2,
            &mut NoOpHandler,
        )
        .unwrap();

        let fh2 = engine2.file_header();
        assert_eq!(fh2.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh2.catalog_root_page.get(), fh.catalog_root_page.get());
        assert_eq!(
            fh2.catalog_name_root_page.get(),
            fh.catalog_name_root_page.get()
        );

        engine2.close().await.unwrap();
    }

    // ─── Test 2: open_in_memory ───

    #[tokio::test]
    async fn test_open_in_memory() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        assert!(!engine.is_durable());
        let fh = engine.file_header();
        assert_eq!(fh.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh.version.get(), FILE_HEADER_VERSION);
        assert!(fh.catalog_root_page.get() > 0);
    }

    // ─── Test 3: create_btree + insert + get ───

    #[tokio::test]
    async fn test_create_btree_insert_get() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        let handle = engine.create_btree().unwrap();
        handle.insert(b"key1", b"value1").unwrap();
        handle.insert(b"key2", b"value2").unwrap();
        handle.insert(b"key3", b"value3").unwrap();

        assert_eq!(
            handle.get(b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            handle.get(b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            handle.get(b"key3").unwrap(),
            Some(b"value3".to_vec())
        );
        assert_eq!(handle.get(b"key4").unwrap(), None);
    }

    // ─── Test 4: Multiple B-trees ───

    #[tokio::test]
    async fn test_multiple_btrees() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        let bt1 = engine.create_btree().unwrap();
        let bt2 = engine.create_btree().unwrap();
        let bt3 = engine.create_btree().unwrap();

        bt1.insert(b"a", b"tree1").unwrap();
        bt2.insert(b"a", b"tree2").unwrap();
        bt3.insert(b"a", b"tree3").unwrap();

        assert_eq!(bt1.get(b"a").unwrap(), Some(b"tree1".to_vec()));
        assert_eq!(bt2.get(b"a").unwrap(), Some(b"tree2".to_vec()));
        assert_eq!(bt3.get(b"a").unwrap(), Some(b"tree3".to_vec()));

        // Verify isolation: keys from other trees are not visible.
        assert_eq!(bt1.get(b"b").unwrap(), None);
    }

    // ─── Test 5: Heap store + load ───

    #[tokio::test]
    async fn test_heap_store_load() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        let data = b"hello, heap storage!";
        let href = engine.heap_store(data).unwrap();
        let loaded = engine.heap_load(href).unwrap();

        assert_eq!(loaded, data);
    }

    // ─── Test 6: WAL append + read ───

    #[tokio::test]
    async fn test_wal_append_read() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        let lsn = engine.append_wal(0x01, b"test-payload").await.unwrap();
        assert_eq!(lsn, 0);

        let mut iter = engine.read_wal_from(0);
        let record = iter.next().unwrap().unwrap();
        assert_eq!(record.lsn, 0);
        assert_eq!(record.record_type, 0x01);
        assert_eq!(record.payload, b"test-payload");

        assert!(iter.next().is_none());
    }

    // ─── Test 7: Checkpoint persists data ───

    #[tokio::test]
    async fn test_checkpoint_persists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");

        // Open, insert into B-tree, checkpoint, close.
        let root_page;
        {
            let config = StorageConfig {
                page_size: 4096,
                memory_budget: 4096 * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(
                &path,
                config,
                &mut NoOpHandler,
            )
            .unwrap();

            let handle = engine.create_btree().unwrap();
            handle.insert(b"persist-key", b"persist-value").unwrap();
            root_page = handle.root_page();

            engine.checkpoint().await.unwrap();
            engine.close().await.unwrap();
        }

        // Reopen and verify data.
        {
            let config = StorageConfig {
                page_size: 4096,
                memory_budget: 4096 * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(
                &path,
                config,
                &mut NoOpHandler,
            )
            .unwrap();

            let handle = engine.open_btree(root_page);
            let value = handle.get(b"persist-key").unwrap();
            assert_eq!(value, Some(b"persist-value".to_vec()));

            engine.close().await.unwrap();
        }
    }

    // ─── Test 8: File header verify ───

    #[test]
    fn test_file_header_verify() {
        let fh = FileHeader::new(8192);
        assert!(fh.verify().is_ok());

        // Bad magic.
        let mut bad_magic = fh.clone();
        bad_magic.magic = U32::new(0xDEADBEEF);
        assert!(bad_magic.verify().is_err());

        // Bad version.
        let mut bad_version = fh.clone();
        bad_version.version = U32::new(99);
        assert!(bad_version.verify().is_err());
    }

    // ─── Test 9: File header updates ───

    #[tokio::test]
    async fn test_file_header_updates() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).unwrap();

        let fh_before = engine.file_header();
        assert_eq!(fh_before.visible_ts.get(), 0);

        engine
            .update_file_header(|fh| {
                fh.visible_ts = U64::new(12345);
                fh.next_collection_id = U64::new(100);
            })
            .unwrap();

        let fh_after = engine.file_header();
        assert_eq!(fh_after.visible_ts.get(), 12345);
        assert_eq!(fh_after.next_collection_id.get(), 100);

        // Verify catalog root pages are still set.
        assert!(fh_after.catalog_root_page.get() > 0);
        assert!(fh_after.catalog_name_root_page.get() > 0);
    }

    // ─── Test 10: Page size > 65535 should be rejected ───
    // BUG: Page header uses u16 for offsets, so max page size is 65535.
    // No validation exists — larger sizes cause silent truncation/corruption.

    #[tokio::test]
    async fn test_page_size_too_large_rejected() {
        let config = StorageConfig {
            page_size: 65536, // One byte over max u16
            memory_budget: 65536 * 64,
            ..Default::default()
        };
        let result = StorageEngine::open_in_memory(config);
        assert!(
            result.is_err(),
            "page_size > 65535 should be rejected (u16 overflow in page header)"
        );
    }

    // ─── Test 11: Page size of exactly 65535 should work ───

    #[tokio::test]
    async fn test_page_size_max_valid() {
        let config = StorageConfig {
            page_size: 65535, // Max valid u16
            memory_budget: 65535 * 64,
            ..Default::default()
        };
        // Should succeed.
        let engine = StorageEngine::open_in_memory(config).unwrap();
        let fh = engine.file_header();
        assert_eq!(fh.page_size.get(), 65535);
    }
}
