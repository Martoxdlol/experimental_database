# 04 — Buffer Pool

Implements DESIGN.md §2.7. Per-frame RwLock, clock eviction, positional I/O.

## File: `src/storage/buffer_pool.rs`

### Core Structures

```rust
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::os::unix::fs::FileExt; // pread/pwrite

/// The buffer pool manages page frames in memory.
pub struct BufferPool {
    /// Maps on-disk PageId → in-memory FrameId.
    page_table: RwLock<HashMap<PageId, FrameId>>,
    /// Fixed-size array of page frames, allocated once at startup.
    frames: Vec<FrameSlot>,
    /// Clock eviction hand (index into frames).
    clock_hand: AtomicU32,
    /// Number of frames.
    frame_count: u32,
    /// Page size in bytes.
    page_size: u32,
    /// Data file handle (opened once, shared via pread/pwrite).
    data_file: std::fs::File,
    /// Current file size in pages (for file extension).
    page_count: RwLock<u64>,
}

/// One frame in the buffer pool.
pub struct FrameSlot {
    /// Per-frame lock protecting all mutable state.
    lock: RwLock<FrameInner>,
}

struct FrameInner {
    /// Page-sized buffer.
    data: Vec<u8>,
    /// Which page is loaded (None = frame is free).
    page_id: Option<PageId>,
    /// Number of active pins (shared + exclusive).
    pin_count: u32,
    /// Whether the page has been modified since last flush.
    dirty: bool,
    /// Clock reference bit for eviction.
    ref_bit: bool,
    /// LSN of last modification (used by checkpoint mark-clean).
    lsn: Lsn,
}
```

### Page Guards (RAII)

```rust
/// Shared (read) access to a page. Multiple readers allowed per frame.
pub struct SharedPageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    guard: RwLockReadGuard<'a, FrameInner>,
    pool: &'a BufferPool,
}

impl<'a> SharedPageGuard<'a> {
    /// Read-only access to page data.
    pub fn data(&self) -> &[u8];

    /// The page ID.
    pub fn page_id(&self) -> PageId;

    /// Convenience: wrap data as SlottedPage.
    pub fn as_slotted_page(&self) -> SlottedPage<'_>;
}

impl Drop for SharedPageGuard<'_> {
    fn drop(&mut self) {
        // Decrement pin_count. (Requires briefly upgrading to write —
        // or use an AtomicU32 for pin_count outside the RwLock.)
    }
}

/// Exclusive (write) access to a page. One writer, no concurrent readers.
pub struct ExclusivePageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    guard: RwLockWriteGuard<'a, FrameInner>,
    pool: &'a BufferPool,
    modified: bool,
}

impl<'a> ExclusivePageGuard<'a> {
    /// Read-only access.
    pub fn data(&self) -> &[u8];

    /// Mutable access. Sets the internal `modified` flag.
    pub fn data_mut(&mut self) -> &mut [u8];

    /// The page ID.
    pub fn page_id(&self) -> PageId;

    /// Convenience: wrap data as SlottedPageMut.
    pub fn as_slotted_page_mut(&mut self) -> SlottedPageMut<'_>;
}

impl Drop for ExclusivePageGuard<'_> {
    fn drop(&mut self) {
        // If modified: mark frame dirty, set ref_bit.
        // Decrement pin_count.
    }
}
```

### BufferPool Operations

```rust
impl BufferPool {
    /// Create a new buffer pool.
    /// `frame_count` = memory_budget / page_size.
    pub fn new(
        data_file: std::fs::File,
        page_size: u32,
        frame_count: u32,
        initial_page_count: u64,
    ) -> Self;

    /// Fetch a page for reading (shared lock on the frame).
    /// On cache miss: evict a frame, read from disk via pread.
    pub fn fetch_page_shared(&self, page_id: PageId) -> Result<SharedPageGuard<'_>>;

    /// Fetch a page for writing (exclusive lock on the frame).
    /// On cache miss: evict a frame, read from disk via pread.
    pub fn fetch_page_exclusive(&self, page_id: PageId) -> Result<ExclusivePageGuard<'_>>;

    /// Allocate a new page. Tries free list first, then extends file.
    /// Returns an exclusively pinned guard for the new page.
    pub fn new_page(
        &self,
        page_type: PageType,
        free_list: &mut FreePageList,
    ) -> Result<ExclusivePageGuard<'_>>;

    /// Flush a specific dirty page to disk via pwrite.
    /// Used by checkpoint. Acquires exclusive frame lock briefly.
    pub fn flush_page(&self, page_id: PageId) -> Result<()>;

    /// Snapshot dirty frames: returns Vec<(PageId, page_data_copy, lsn)>.
    /// Called by checkpoint while writer lock is held.
    pub fn snapshot_dirty_frames(&self) -> Vec<(PageId, Vec<u8>, Lsn)>;

    /// Mark a frame clean if its LSN matches `expected_lsn`.
    /// Used by checkpoint after scatter-write.
    pub fn mark_clean_if_lsn(&self, page_id: PageId, expected_lsn: Lsn);

    /// Extend the data file by one page. Returns the new page's PageId.
    fn extend_file(&self) -> Result<PageId>;
}
```

### Clock Eviction Algorithm

```rust
impl BufferPool {
    /// Find a victim frame to evict. Only evicts clean, unpinned frames.
    /// Returns Err if no frame is available (triggers early checkpoint).
    fn find_victim(&self) -> Result<FrameId> {
        // Algorithm:
        // 1. Start at clock_hand.fetch_add(1) % frame_count.
        // 2. Scan up to 2 * frame_count frames.
        // 3. For each frame, try_read() on the lock:
        //    - If locked (in use): skip.
        //    - If page_id is None (free frame): use it.
        //    - If dirty: skip (dirty frames never evicted).
        //    - If pin_count > 0: skip.
        //    - If ref_bit: clear ref_bit, skip (second chance).
        //    - Else: evict — this is the victim.
        // 4. If no victim found after full scan: return Err(BufferPoolFull).
    }
}
```

### Positional I/O

All disk reads/writes use `FileExt::read_exact_at` / `write_all_at` (pread/pwrite semantics). No file-level mutex needed. The data file's seek position is never used.

```rust
impl BufferPool {
    fn read_page_from_disk(&self, page_id: PageId, buf: &mut [u8]) -> Result<()> {
        let offset = page_id.0 as u64 * self.page_size as u64;
        self.data_file.read_exact_at(buf, offset)?;
        // Verify checksum
        let page = SlottedPage::new(buf, self.page_size);
        if !page.verify_checksum() {
            return Err(StorageError::ChecksumMismatch { page_id });
        }
        Ok(())
    }

    fn write_page_to_disk(&self, page_id: PageId, buf: &[u8]) -> Result<()> {
        let offset = page_id.0 as u64 * self.page_size as u64;
        self.data_file.write_all_at(buf, offset)?;
        Ok(())
    }
}
```

### Pin Count Design Choice

The `pin_count` can be either:
- **Inside `FrameInner`** (requires write lock to modify) — simpler but more contention.
- **As `AtomicU32` outside the RwLock** — allows pin/unpin without write lock.

Recommendation: use `AtomicU32` for `pin_count` and `AtomicBool` for `ref_bit` alongside the RwLock, to avoid needing write access just for pin counting.

```rust
pub struct FrameSlot {
    lock: RwLock<FrameData>,      // protects data[], page_id, dirty, lsn
    pin_count: AtomicU32,          // outside lock
    ref_bit: AtomicBool,           // outside lock
}

struct FrameData {
    data: Vec<u8>,
    page_id: Option<PageId>,
    dirty: bool,
    lsn: Lsn,
}
```

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum BufferPoolError {
    #[error("buffer pool full: no clean, unpinned frames available")]
    BufferPoolFull,

    #[error("page checksum mismatch for page {page_id:?}")]
    ChecksumMismatch { page_id: PageId },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

### Concurrency Properties (§2.7.1)

1. **Per-frame RwLock**: readers on different pages never contend.
2. **Positional I/O**: no file mutex; concurrent pread + pwrite.
3. **Page table RwLock**: held briefly (lookup/insert only); never held during I/O.
4. **Clock hand**: AtomicU32, no lock needed.
5. **Latch ordering**: always ascending `page_id` when acquiring multiple frames.
6. **No latches across await**: frame locks are `parking_lot::RwLock` (sync), held in sync blocks. I/O is done outside the latch into temp buffers.
