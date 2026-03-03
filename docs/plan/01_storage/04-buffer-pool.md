# 04 — Buffer Pool (`storage/buffer_pool.rs`)

Manages a fixed-size pool of in-memory page frames (DESIGN §2.7). All page reads/writes go through the buffer pool.

## Structs

```rust
/// A single frame in the buffer pool.
struct Frame {
    data: Box<[u8]>,       // PAGE_SIZE bytes
    page_id: Option<PageId>,
    pin_count: u32,
    dirty: bool,
    ref_bit: bool,         // for clock eviction
}

/// Configuration for the buffer pool.
pub struct BufferPoolConfig {
    pub page_size: u32,
    pub frame_count: u32,        // memory_budget / page_size
    pub data_file: PathBuf,      // path to data.db
}

/// The buffer pool. Thread-safe — uses interior mutability.
pub struct BufferPool {
    frames: Vec<parking_lot::Mutex<Frame>>,
    page_table: parking_lot::RwLock<HashMap<PageId, FrameId>>,
    page_size: u32,
    frame_count: u32,
    clock_hand: AtomicU32,
    file: tokio::sync::Mutex<tokio::fs::File>, // data.db handle
    // Callback for triggering early checkpoint when no clean frames available
    checkpoint_trigger: Option<Box<dyn Fn() + Send + Sync>>,
}
```

## RAII Guard for Pinned Pages

```rust
/// RAII guard for a pinned page. Automatically unpins on drop.
/// Derefs to the page buffer for direct read access.
pub struct PinnedPage<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    // Cached pointer to frame data for the lifetime of the pin.
}

impl<'a> PinnedPage<'a> {
    /// Get the page data as a byte slice.
    pub fn data(&self) -> &[u8];

    /// Get a mutable reference to the page data. Marks the frame as dirty.
    pub fn data_mut(&mut self) -> &mut [u8];

    /// Get the page ID.
    pub fn page_id(&self) -> PageId;

    /// Explicitly mark as dirty (also done automatically by data_mut).
    pub fn mark_dirty(&mut self);
}

impl<'a> Drop for PinnedPage<'a> {
    fn drop(&mut self) {
        // Decrement pin_count on the frame
    }
}
```

## Methods

```rust
impl BufferPool {
    /// Create a new buffer pool. Opens (or creates) data.db.
    pub async fn new(config: BufferPoolConfig) -> Result<Self, StorageError>;

    /// Fetch a page by ID. If cached, pin and return. Otherwise, evict a
    /// clean frame, read from disk, pin, return.
    /// Verifies page checksum on disk read.
    pub async fn fetch_page(&self, page_id: PageId) -> Result<PinnedPage<'_>, StorageError>;

    /// Allocate a new page. Uses free list (via `alloc_fn`) or extends the file.
    /// `alloc_fn` is called to pop from the free list — see freelist.rs.
    pub async fn new_page(
        &self,
        page_type: PageType,
        alloc_fn: &dyn Fn(&BufferPool) -> Option<PageId>,
    ) -> Result<PinnedPage<'_>, StorageError>;

    /// Collect all dirty frame page IDs. Used by checkpoint.
    pub fn dirty_pages(&self) -> Vec<(PageId, FrameId)>;

    /// Read a frame's data by FrameId (for DWB/checkpoint to read dirty pages).
    pub fn read_frame(&self, frame_id: FrameId) -> Vec<u8>;

    /// Mark a frame as clean (after checkpoint has flushed it).
    pub fn mark_clean(&self, frame_id: FrameId);

    /// Total number of frames.
    pub fn frame_count(&self) -> u32;

    /// Read a page directly from disk (bypassing cache). Used by DWB recovery.
    pub async fn read_page_raw(&self, page_id: PageId) -> Result<Vec<u8>, StorageError>;

    /// Write a page directly to disk at its position. Used by DWB recovery.
    pub async fn write_page_raw(&self, page_id: PageId, data: &[u8]) -> Result<(), StorageError>;

    /// Flush (fsync) the data file.
    pub async fn sync_data_file(&self) -> Result<(), StorageError>;

    /// Extend the data file by one page. Returns the new page ID.
    pub async fn extend_file(&self) -> Result<PageId, StorageError>;
}
```

## Clock Eviction (internal)

```rust
impl BufferPool {
    /// Find a victim frame using the clock algorithm.
    /// Only considers unpinned, clean frames.
    /// Returns None if all frames are pinned or dirty (caller triggers checkpoint).
    fn find_victim(&self) -> Option<FrameId>;
}
```

## Key Design Decisions

1. **No dirty eviction**: per DESIGN §2.7, dirty frames are never evicted. If `find_victim()` returns `None`, the caller triggers an early checkpoint. After checkpoint marks frames clean, eviction can proceed.

2. **Checksum on every read**: `fetch_page()` verifies the page checksum when reading from disk. A checksum mismatch returns `StorageError::CorruptPage`.

3. **Pin contract**: callers hold `PinnedPage` for the duration of access. The RAII guard auto-unpins on drop.

4. **Concurrency**: each frame has its own `Mutex`. The page table is a `RwLock<HashMap>`. This allows concurrent reads to different pages without contention.

## Error Type

```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Page {0:?} checksum mismatch (corrupt page)")]
    CorruptPage(PageId),
    #[error("Buffer pool full: no clean frames available")]
    BufferPoolFull,
    #[error("Page {0:?} not found in data file")]
    PageNotFound(PageId),
}
```
