# S3: Buffer Pool

## Purpose

In-memory page cache with clock eviction. Provides RAII page guards (shared/exclusive) for safe concurrent access. Abstracts page I/O through the PageStorage trait.

## Dependencies

- **S1 (Backend)**: `PageStorage` trait — all page reads/writes go through it
- **S2 (Page Format)**: `PageId` type, page size concept

## Rust Types

```rust
use crate::storage::backend::{PageId, PageStorage};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use parking_lot::RwLock;

type FrameId = u32;

pub struct BufferPoolConfig {
    pub page_size: usize,
    pub frame_count: usize,  // memory_budget / page_size
}

pub struct BufferPool {
    /// Maps on-disk page IDs to in-memory frame indices.
    page_table: RwLock<HashMap<PageId, FrameId>>,

    /// Fixed-size array of page frames, allocated once at startup.
    frames: Vec<FrameSlot>,

    /// Clock eviction hand position.
    clock_hand: AtomicU32,

    /// Backend for page I/O.
    page_storage: Arc<dyn PageStorage>,

    page_size: usize,
}

/// One slot in the frame array. Each has its own RwLock.
struct FrameSlot {
    lock: parking_lot::RwLock<FrameData>,

    /// Pin count lives OUTSIDE the RwLock so both SharedPageGuard (which holds
    /// only a read guard) and ExclusivePageGuard can atomically decrement it
    /// on Drop without needing write access to the frame.
    pin_count: AtomicU32,
}

struct FrameData {
    data: Vec<u8>,             // [u8; page_size] — the page buffer
    page_id: Option<PageId>,   // None if frame is unused
    dirty: bool,               // modified since last flush
    ref_bit: bool,             // clock algorithm reference bit
}

/// RAII shared page guard. Multiple readers can hold this simultaneously.
pub struct SharedPageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockReadGuard<'a, FrameData>,
}

impl<'a> SharedPageGuard<'a> {
    pub fn data(&self) -> &[u8];
    pub fn page_id(&self) -> PageId;
}
// Drop: atomically decrement pool.frames[frame_id].pin_count, release read lock

/// RAII exclusive page guard. Only one writer per frame.
pub struct ExclusivePageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockWriteGuard<'a, FrameData>,
    modified: bool,  // tracks if data_mut() was called
}

impl<'a> ExclusivePageGuard<'a> {
    pub fn data(&self) -> &[u8];
    pub fn data_mut(&mut self) -> &mut [u8];  // sets modified = true
    pub fn page_id(&self) -> PageId;
    pub fn mark_dirty(&mut self);  // explicit dirty marking
}
// Drop: if modified, set dirty = true. Atomically decrement
// pool.frames[frame_id].pin_count, release write lock.

impl BufferPool {
    pub fn new(config: BufferPoolConfig, page_storage: Arc<dyn PageStorage>) -> Self;

    /// Fetch a page for reading. Loads from backend on cache miss.
    pub async fn fetch_page_shared(&self, page_id: PageId) -> Result<SharedPageGuard>;

    /// Fetch a page for writing. Loads from backend on cache miss.
    pub async fn fetch_page_exclusive(&self, page_id: PageId) -> Result<ExclusivePageGuard>;

    /// Allocate a new page (from free list or file extension) and return exclusive guard.
    /// The page buffer is zero-filled.
    pub async fn new_page(&self, page_id: PageId) -> Result<ExclusivePageGuard>;

    /// Flush a specific page to the backend (for checkpoint use).
    pub async fn flush_page(&self, page_id: PageId) -> Result<()>;

    /// Snapshot all dirty frames: returns (page_id, page_data_copy, lsn) tuples.
    /// Does NOT clear dirty flags (checkpoint does that after DWB write).
    pub fn dirty_pages(&self) -> Vec<(PageId, Vec<u8>, Lsn)>;

    /// Mark a specific frame as clean (after checkpoint scatter-write).
    /// Only clears dirty if the frame's LSN hasn't changed since snapshot.
    pub fn mark_clean(&self, page_id: PageId, expected_lsn: u64);

    /// Number of frames currently in use (pinned or cached).
    pub fn used_frames(&self) -> usize;

    /// The underlying page storage.
    pub fn page_storage(&self) -> &Arc<dyn PageStorage>;
}
```

## Implementation Details

### new()

1. Allocate `frame_count` FrameSlots, each with a zero-filled `Vec<u8>` of `page_size` bytes.
2. Initialize empty page_table HashMap.
3. Set clock_hand = 0.

### fetch_page_shared() — Page Read Path

```
                    ┌─────────────┐
                    │ page_table  │
                    │ .read()     │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │ Found frame_id?         │
              ├────── YES ──────────────┤
              │                         │
    ┌─────────▼─────────┐    ┌─────────▼─────────┐
    │ Release page_table│    │ find_victim()      │
    │ Acquire frame     │    │ evict if needed    │
    │ .read()           │    │ page_storage       │
    │ Set ref_bit=true  │    │   .read_page()     │
    │ pin_count.fetch_  │    │ into temp buf      │
    │   add(1, AcqRel)  │    │ page_table.write() │
    │ Return guard      │    │ insert mapping     │
    └───────────────────┘    │ Copy to frame      │
                             │ Acquire .read()    │
                             │ pin_count.store(   │
                             │   1, Release)      │
                             │ Return guard       │
                             └───────────────────┘
```

**Detailed steps (cache miss path)**:

1. Acquire `page_table.read()`. Look up page_id. Not found → release page_table.
2. Read page from backend into a **temporary buffer** (no latch held during I/O).
3. Call `find_victim()` to get a free frame_id.
4. Acquire `page_table.write()`. Check again if page_id was loaded by another thread (double-check). If so, use that frame (SharedPageGuard::new() atomically increments pin_count as part of construction, before returning). Otherwise insert `(page_id, frame_id)`.
5. Acquire `frames[frame_id].lock.write()`. Copy temp buffer into frame data. Set `page_id = Some(page_id)`, `dirty = false`, `ref_bit = true`. Atomically store `frames[frame_id].pin_count = 1` (Release ordering).
6. Downgrade to read lock (or release write + acquire read). Return SharedPageGuard.

**Key**: No latch is held during the `page_storage.read_page()` call (step 2). This prevents blocking other threads on I/O.

### find_victim() — Clock Eviction

1. Start at `clock_hand.fetch_add(1) % frame_count`.
2. Scan frames in a circle:
   a. If `frame.pin_count.load(Acquire) > 0`, skip (frame is pinned — optimization to skip obviously-pinned frames without attempting the lock).
   b. Try to acquire `frame.lock.try_write()`. If fails (frame in use), skip.
   c. If `dirty`, skip (dirty frames are NOT evicted — checkpoint flushes them).
   d. If `ref_bit`, clear it, skip (second chance).
   e. Otherwise: this frame is the victim. Remove its old mapping from page_table. Return frame_id.
3. If full circle with no victim found: all frames are dirty or pinned. Return `Err(BufferPoolFull)`. The caller (or an upper layer) should trigger an emergency checkpoint to flush dirty pages and retry.

**Dirty frames are never evicted**: All page writes to the backend go through the double-write buffer during checkpoint. No unprotected writes. This means a full buffer pool of dirty pages triggers an emergency checkpoint.

### fetch_page_exclusive()

Same as fetch_page_shared() but acquire write lock on the frame instead of read lock. Only one exclusive guard per frame.

### new_page()

1. The caller provides a `page_id` (allocated by free list or file extension).
2. Find/evict a frame.
3. Zero-fill the frame buffer.
4. Insert into page_table.
5. Return exclusive guard with `dirty = false` (caller will initialize and mark dirty).

### dirty_pages() — Checkpoint Snapshot

1. Iterate all frames.
2. For each frame, acquire read lock briefly.
3. If dirty and page_id is Some: copy data to output vec, record page_id, data, and LSN.
4. Release lock.
5. Return all (page_id, data, lsn) tuples.

### mark_clean()

1. Find frame_id for page_id in page_table.
2. Acquire frame write lock.
3. If frame's current LSN == expected_lsn: set dirty = false.
4. If LSN has changed (writer modified page since snapshot): leave dirty = true.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error on read | Backend read_page fails | Propagate |
| `BufferPoolFull` | All frames dirty + pinned, no victim available | Return error to caller; caller triggers emergency checkpoint and retries |
| Page already loaded | Concurrent fetch race | Use the existing frame (double-check pattern) |

## Tests

1. **Basic fetch + read**: Write a page to backend, fetch via pool, verify data matches.
2. **Cache hit**: Fetch same page twice, second should not read from backend (mock backend, count reads).
3. **Exclusive write + dirty**: Fetch exclusive, write data, drop guard, verify dirty flag set.
4. **Multiple shared guards**: Fetch same page shared from two calls, both should succeed.
5. **Eviction**: Create pool with 3 frames. Load 4 different pages. Verify eviction happened, all data correct.
6. **Clock eviction skips dirty**: Load 3 pages exclusive + modify all. Try to load a 4th. Should fail/trigger checkpoint signal.
7. **dirty_pages snapshot**: Modify several pages, call dirty_pages(), verify all dirty pages returned.
8. **mark_clean**: Modify page, snapshot, mark_clean with correct LSN. Verify dirty cleared.
9. **mark_clean with stale LSN**: Modify page, snapshot, modify again (new LSN), mark_clean with old LSN. Verify dirty NOT cleared.
10. **new_page**: Allocate new page, verify zero-filled, write data, verify readable.
11. **Works with MemoryPageStorage**: Same tests pass with in-memory backend.
12. **Works with FilePageStorage**: Same tests pass with file backend (use tempdir).
13. **Pin prevents eviction**: Fetch a page shared and hold the guard. Load enough pages to trigger eviction. Verify pinned page is never evicted.
