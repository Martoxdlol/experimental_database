//! In-memory page cache with clock eviction.
//!
//! Provides RAII page guards (shared/exclusive) for safe concurrent access.
//! Abstracts page I/O through the [`PageStorage`] trait.
//!
//! Key design invariants:
//! - `pin_count` lives **outside** the frame `RwLock` so both shared and exclusive
//!   guards can atomically decrement it on `Drop` without needing write access.
//! - No frame locks are held across I/O operations (reads from the backend happen
//!   into a temporary buffer before the frame lock is acquired).
//! - Dirty frames are **never** evicted. All writes to the backend go through the
//!   double-write buffer during checkpoint.

use crate::backend::{PageId, PageStorage};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

// ─── Types ───

type FrameId = u32;

/// Log sequence number (placeholder — just a u64 for now).
pub type Lsn = u64;

/// Byte offset of the `lsn` field within the page header (see `page.rs`).
const LSN_OFFSET: usize = 24;
/// Size of the `lsn` field in bytes.
const LSN_SIZE: usize = 8;

// ─── Errors ───

/// Returned when all buffer pool frames are pinned and no victim can be
/// evicted.
///
/// This is a returned error (not a panic). The checkpoint process does
/// **not** trigger this error because it reads dirty-frame metadata
/// without allocating new frames.
///
/// Common causes:
/// - All frames are held by concurrent readers/writers (page guards not
///   yet dropped).
/// - All frames are dirty and the clock eviction sweep skipped them.
///
/// Callers should release any held page guards and retry the operation.
#[derive(Debug)]
pub struct BufferPoolFull;

impl std::fmt::Display for BufferPoolFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "buffer pool full: all frames are dirty or pinned, no victim available"
        )
    }
}

impl std::error::Error for BufferPoolFull {}

// ─── Configuration ───

/// Configuration for a [`BufferPool`].
pub struct BufferPoolConfig {
    /// Size of each page (must match the backend's page size).
    pub page_size: usize,
    /// Number of page frames in the pool (memory_budget / page_size).
    pub frame_count: usize,
}

// ─── Frame internals ───

/// Data protected by the per-frame `RwLock`.
struct FrameData {
    /// The page buffer (always `page_size` bytes).
    data: Vec<u8>,
    /// Which page is currently loaded, or `None` if the frame is empty.
    page_id: Option<PageId>,
    /// Whether the page has been modified since the last flush.
    dirty: bool,
}

/// One slot in the fixed-size frame array.
struct FrameSlot {
    /// Protects the mutable frame data.
    lock: RwLock<FrameData>,
    /// Pin count lives **outside** the RwLock so both `SharedPageGuard` (holding
    /// only a read guard) and `ExclusivePageGuard` can atomically decrement it
    /// on `Drop` without needing write access to the frame.
    pin_count: AtomicU32,
    /// Clock algorithm reference bit (second-chance). Lives **outside** the
    /// RwLock so it can be set on cache hits without acquiring a write lock,
    /// which would deadlock when multiple shared guards exist on the same frame.
    ref_bit: AtomicBool,
}

// ─── BufferPool ───

/// In-memory page cache with clock eviction.
pub struct BufferPool {
    /// Maps on-disk page IDs to in-memory frame indices.
    page_table: RwLock<HashMap<PageId, FrameId>>,
    /// Fixed-size array of page frames, allocated once at startup.
    frames: Vec<FrameSlot>,
    /// Clock eviction hand position.
    clock_hand: AtomicU32,
    /// Backend for page I/O.
    page_storage: Arc<dyn PageStorage>,
    /// Page size (bytes).
    page_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool.
    ///
    /// Allocates `config.frame_count` frames, each with a zero-filled buffer
    /// of `config.page_size` bytes.
    pub fn new(config: BufferPoolConfig, page_storage: Arc<dyn PageStorage>) -> Self {
        let mut frames = Vec::with_capacity(config.frame_count);
        for _ in 0..config.frame_count {
            frames.push(FrameSlot {
                lock: RwLock::new(FrameData {
                    data: vec![0u8; config.page_size],
                    page_id: None,
                    dirty: false,
                }),
                pin_count: AtomicU32::new(0),
                ref_bit: AtomicBool::new(false),
            });
        }

        BufferPool {
            page_table: RwLock::new(HashMap::new()),
            frames,
            clock_hand: AtomicU32::new(0),
            page_storage,
            page_size: config.page_size,
        }
    }

    /// Fetch a page for reading. Loads from the backend on a cache miss.
    ///
    /// Returns a shared guard that allows concurrent readers.
    pub fn fetch_page_shared(&self, page_id: PageId) -> io::Result<SharedPageGuard<'_>> {
        // Fast path: check if the page is already cached.
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                // Page is in the cache. Pin, set ref_bit (atomic, no write lock
                // needed), acquire read lock, then re-validate.
                let slot = &self.frames[frame_id as usize];
                slot.pin_count.fetch_add(1, Ordering::AcqRel);
                slot.ref_bit.store(true, Ordering::Release);
                let guard = slot.lock.read();

                // Re-validate: between our page_table lookup and acquiring the
                // read lock, another thread may have evicted this frame and
                // loaded a different page into it (TOCTOU race). If the frame
                // no longer holds our page, undo the pin and fall through to
                // the slow path.
                if guard.page_id == Some(page_id) {
                    return Ok(SharedPageGuard {
                        pool: self,
                        frame_id,
                        guard,
                    });
                }
                // Frame was evicted and reused. Undo pin and fall through.
                drop(guard);
                slot.pin_count.fetch_sub(1, Ordering::Release);
            }
        }
        // page_table lock is released here.

        // Cache miss: read from backend into a temporary buffer (no locks held during I/O).
        let mut tmp_buf = vec![0u8; self.page_size];
        self.page_storage.read_page(page_id, &mut tmp_buf)?;

        // Find a victim frame.
        let victim_frame_id = self.find_victim()?;

        // Acquire page_table write lock and double-check.
        let mut pt = self.page_table.write();

        if let Some(&existing_frame_id) = pt.get(&page_id) {
            // Another thread loaded this page while we were doing I/O.
            // Use the existing frame. We need to "return" the victim frame — but
            // since find_victim only removes old mappings and doesn't modify frame
            // contents yet beyond what we do below, we just don't use victim_frame_id.
            drop(pt);

            let slot = &self.frames[existing_frame_id as usize];
            slot.pin_count.fetch_add(1, Ordering::AcqRel);
            slot.ref_bit.store(true, Ordering::Release);
            let guard = slot.lock.read();
            return Ok(SharedPageGuard {
                pool: self,
                frame_id: existing_frame_id,
                guard,
            });
        }

        // Install the page into the victim frame.
        pt.insert(page_id, victim_frame_id);
        drop(pt);

        let slot = &self.frames[victim_frame_id as usize];
        let mut guard = slot.lock.write();
        guard.data.copy_from_slice(&tmp_buf);
        guard.page_id = Some(page_id);
        guard.dirty = false;
        slot.ref_bit.store(true, Ordering::Release);
        slot.pin_count.store(1, Ordering::Release);
        drop(guard);

        // Downgrade: release write, acquire read.
        let guard = slot.lock.read();
        Ok(SharedPageGuard {
            pool: self,
            frame_id: victim_frame_id,
            guard,
        })
    }

    /// Fetch a page for writing. Loads from the backend on a cache miss.
    ///
    /// Returns an exclusive guard — only one writer per frame.
    pub fn fetch_page_exclusive(&self, page_id: PageId) -> io::Result<ExclusivePageGuard<'_>> {
        // Fast path: check if the page is already cached.
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                let slot = &self.frames[frame_id as usize];
                slot.pin_count.fetch_add(1, Ordering::AcqRel);
                slot.ref_bit.store(true, Ordering::Release);
                let guard = slot.lock.write();

                // Re-validate: between our page_table lookup and acquiring the
                // write lock, another thread may have evicted this frame.
                if guard.page_id == Some(page_id) {
                    return Ok(ExclusivePageGuard {
                        pool: self,
                        frame_id,
                        guard,
                        modified: false,
                    });
                }
                // Frame was evicted and reused. Undo pin and fall through.
                drop(guard);
                slot.pin_count.fetch_sub(1, Ordering::Release);
            }
        }

        // Cache miss: read from backend into temp buffer.
        let mut tmp_buf = vec![0u8; self.page_size];
        self.page_storage.read_page(page_id, &mut tmp_buf)?;

        // Find a victim frame.
        let victim_frame_id = self.find_victim()?;

        // Double-check under write lock.
        let mut pt = self.page_table.write();

        if let Some(&existing_frame_id) = pt.get(&page_id) {
            drop(pt);

            let slot = &self.frames[existing_frame_id as usize];
            slot.pin_count.fetch_add(1, Ordering::AcqRel);
            slot.ref_bit.store(true, Ordering::Release);
            let guard = slot.lock.write();
            return Ok(ExclusivePageGuard {
                pool: self,
                frame_id: existing_frame_id,
                guard,
                modified: false,
            });
        }

        pt.insert(page_id, victim_frame_id);
        drop(pt);

        let slot = &self.frames[victim_frame_id as usize];
        let mut guard = slot.lock.write();
        guard.data.copy_from_slice(&tmp_buf);
        guard.page_id = Some(page_id);
        guard.dirty = false;
        slot.ref_bit.store(true, Ordering::Release);
        slot.pin_count.store(1, Ordering::Release);

        Ok(ExclusivePageGuard {
            pool: self,
            frame_id: victim_frame_id,
            guard,
            modified: false,
        })
    }

    /// Allocate a new page in the pool. The caller provides a `page_id`
    /// (allocated by the free list or file extension). The frame buffer is zero-filled.
    ///
    /// Returns an exclusive guard with `dirty = false` — the caller will
    /// initialize the page contents and mark it dirty.
    pub fn new_page(&self, page_id: PageId) -> io::Result<ExclusivePageGuard<'_>> {
        let victim_frame_id = self.find_victim()?;

        let mut pt = self.page_table.write();
        pt.insert(page_id, victim_frame_id);
        drop(pt);

        let slot = &self.frames[victim_frame_id as usize];
        let mut guard = slot.lock.write();
        // Zero-fill the frame buffer.
        guard.data.fill(0);
        guard.page_id = Some(page_id);
        guard.dirty = false;
        slot.ref_bit.store(true, Ordering::Release);
        slot.pin_count.store(1, Ordering::Release);

        Ok(ExclusivePageGuard {
            pool: self,
            frame_id: victim_frame_id,
            guard,
            modified: false,
        })
    }

    /// Flush a specific page to the backend (for checkpoint use).
    pub fn flush_page(&self, page_id: PageId) -> io::Result<()> {
        let frame_id = {
            let pt = self.page_table.read();
            match pt.get(&page_id) {
                Some(&fid) => fid,
                None => {
                    return Err(crate::error::StorageError::InternalBug(
                        format!("page {} not in buffer pool", page_id),
                    )
                    .into())
                }
            }
        };

        let slot = &self.frames[frame_id as usize];
        let guard = slot.lock.read();
        self.page_storage.write_page(page_id, &guard.data)?;
        drop(guard);

        // Clear dirty flag after successful write.
        let mut guard = slot.lock.write();
        guard.dirty = false;
        Ok(())
    }

    /// Snapshot all dirty frames. Returns `(page_id, page_data_copy, lsn)` tuples.
    ///
    /// Stamps the CRC-32C checksum on each dirty page before copying, so that
    /// the DWB and recovery can use checksums for torn-write detection.
    ///
    /// Does **not** clear dirty flags (the checkpoint layer does that after the
    /// double-write buffer write via [`mark_clean`]).
    pub fn dirty_pages(&self) -> Vec<(PageId, Vec<u8>, Lsn)> {
        use crate::page::SlottedPage;

        let mut result = Vec::new();

        for slot in &self.frames {
            // Acquire write lock so we can stamp the checksum in-place.
            let mut guard = slot.lock.write();
            if guard.dirty
                && let Some(pid) = guard.page_id {
                    // Stamp the checksum before snapshotting.
                    let mut page = SlottedPage::from_buf(&mut guard.data)
                        .expect("pool frame is always page_size");
                    page.stamp_checksum();

                    let data_copy = guard.data.clone();
                    // Read LSN from the page buffer at offset 24 (little-endian u64).
                    let lsn = read_lsn_from_buf(&guard.data);
                    result.push((pid, data_copy, lsn));
                }
        }

        result
    }

    /// Mark a specific frame as clean (after checkpoint scatter-write).
    ///
    /// Only clears the dirty flag if the frame's current LSN matches
    /// `expected_lsn`. If the LSN has changed (a writer modified the page
    /// since the snapshot), the dirty flag is left set.
    pub fn mark_clean(&self, page_id: PageId, expected_lsn: Lsn) {
        let frame_id = {
            let pt = self.page_table.read();
            match pt.get(&page_id) {
                Some(&fid) => fid,
                None => return,
            }
        };

        let slot = &self.frames[frame_id as usize];
        let mut guard = slot.lock.write();
        let current_lsn = read_lsn_from_buf(&guard.data);
        if current_lsn == expected_lsn {
            guard.dirty = false;
        }
    }

    /// Number of frames currently in use (have a page loaded).
    pub fn used_frames(&self) -> usize {
        let mut count = 0;
        for slot in &self.frames {
            let guard = slot.lock.read();
            if guard.page_id.is_some() {
                count += 1;
            }
        }
        count
    }

    /// The underlying page storage backend.
    pub fn page_storage(&self) -> &Arc<dyn PageStorage> {
        &self.page_storage
    }

    // ─── Internal: Clock Eviction ───

    /// Find a victim frame using the clock (second-chance) algorithm.
    ///
    /// Rules:
    /// 1. Skip frames with `pin_count > 0` (in use).
    /// 2. Try to acquire write lock; skip if contended.
    /// 3. Skip dirty frames (never evicted — checkpoint flushes them).
    /// 4. If `ref_bit` is set, clear it and move on (second chance).
    /// 5. Otherwise this frame is the victim. Remove its old mapping.
    ///
    /// If a full circle completes with no victim: all frames are dirty or
    /// pinned. Returns `Err(BufferPoolFull)`.
    fn find_victim(&self) -> io::Result<FrameId> {
        let frame_count = self.frames.len() as u32;
        let max_scan = frame_count * 2; // two full passes to allow ref_bit clearing

        for _ in 0..max_scan {
            let idx = self.clock_hand.fetch_add(1, Ordering::Relaxed) % frame_count;
            let slot = &self.frames[idx as usize];

            // Quick check: skip obviously pinned frames.
            if slot.pin_count.load(Ordering::Acquire) > 0 {
                continue;
            }

            // Try to acquire write lock without blocking.
            let mut guard = match slot.lock.try_write() {
                Some(g) => g,
                None => continue,
            };

            // Skip dirty frames (never evicted).
            if guard.dirty {
                drop(guard);
                continue;
            }

            // Empty frame — use it immediately.
            if guard.page_id.is_none() {
                drop(guard);
                return Ok(idx);
            }

            // Second chance: if ref_bit is set, clear it and skip.
            if slot.ref_bit.load(Ordering::Acquire) {
                slot.ref_bit.store(false, Ordering::Release);
                drop(guard);
                continue;
            }

            // Re-check pin_count under the write lock (another thread may have
            // pinned between our earlier load and acquiring the lock).
            if slot.pin_count.load(Ordering::Acquire) > 0 {
                drop(guard);
                continue;
            }

            // This frame is the victim. Remove its old page_table mapping.
            let old_page_id = guard.page_id.take();
            drop(guard);

            if let Some(old_pid) = old_page_id {
                let mut pt = self.page_table.write();
                pt.remove(&old_pid);
            }

            return Ok(idx);
        }

        Err(io::Error::other(
            BufferPoolFull,
        ))
    }
}

// ─── Helper ───

/// Read the LSN (little-endian u64) from a page buffer at offset 24.
fn read_lsn_from_buf(buf: &[u8]) -> Lsn {
    if buf.len() < LSN_OFFSET + LSN_SIZE {
        return 0;
    }
    u64::from_le_bytes(
        buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE]
            .try_into()
            .unwrap(),
    )
}

// ─── SharedPageGuard ───

/// RAII shared page guard. Multiple readers can hold this simultaneously.
///
/// On drop, atomically decrements `pin_count` and releases the read lock.
pub struct SharedPageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockReadGuard<'a, FrameData>,
}

impl<'a> SharedPageGuard<'a> {
    /// Access the raw page data.
    pub fn data(&self) -> &[u8] {
        &self.guard.data
    }

    /// The page ID of the loaded page.
    pub fn page_id(&self) -> PageId {
        self.guard.page_id.expect("SharedPageGuard has no page_id")
    }
}

impl<'a> Drop for SharedPageGuard<'a> {
    fn drop(&mut self) {
        self.pool.frames[self.frame_id as usize]
            .pin_count
            .fetch_sub(1, Ordering::Release);
        // RwLockReadGuard drops automatically.
    }
}

// ─── ExclusivePageGuard ───

/// RAII exclusive page guard. Only one writer per frame.
///
/// On drop: if `modified`, sets `dirty = true` in FrameData. Then
/// atomically decrements `pin_count` and releases the write lock.
pub struct ExclusivePageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockWriteGuard<'a, FrameData>,
    modified: bool,
}

impl<'a> ExclusivePageGuard<'a> {
    /// Access the raw page data (read-only).
    pub fn data(&self) -> &[u8] {
        &self.guard.data
    }

    /// Access the raw page data (read-write). Marks the guard as modified.
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.modified = true;
        &mut self.guard.data
    }

    /// The page ID of the loaded page.
    pub fn page_id(&self) -> PageId {
        self.guard.page_id.expect("ExclusivePageGuard has no page_id")
    }

    /// Explicitly mark the page as dirty (without going through `data_mut`).
    pub fn mark_dirty(&mut self) {
        self.modified = true;
    }
}

impl<'a> Drop for ExclusivePageGuard<'a> {
    fn drop(&mut self) {
        if self.modified {
            self.guard.dirty = true;
        }
        self.pool.frames[self.frame_id as usize]
            .pin_count
            .fetch_sub(1, Ordering::Release);
        // RwLockWriteGuard drops automatically.
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{FilePageStorage, MemoryPageStorage};
    use tempfile::TempDir;

    const PAGE_SIZE: usize = 4096;

    /// Helper: create a MemoryPageStorage with `n` pages, optionally writing
    /// a known pattern to each page.
    fn make_memory_storage(n: u64) -> Arc<MemoryPageStorage> {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(n).unwrap();
        storage
    }

    /// Helper: write a recognizable pattern into page `page_id` of the storage.
    fn write_pattern(storage: &dyn PageStorage, page_id: PageId) {
        let mut page = vec![0u8; PAGE_SIZE];
        // Fill with page_id as the repeating byte.
        page.fill(page_id as u8);
        // Stamp the page_id in the first 4 bytes (LE) for easy identification.
        page[0..4].copy_from_slice(&page_id.to_le_bytes());
        storage.write_page(page_id, &page).unwrap();
    }

    /// Helper: verify a page buffer has the pattern written by `write_pattern`.
    fn verify_pattern(data: &[u8], page_id: PageId) {
        let stored_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        assert_eq!(stored_id, page_id, "page_id mismatch in buffer");
        // Check a few bytes in the middle.
        assert_eq!(data[100], page_id as u8);
        assert_eq!(data[PAGE_SIZE - 1], page_id as u8);
    }

    fn make_pool(frame_count: usize, storage: Arc<dyn PageStorage>) -> BufferPool {
        BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count,
            },
            storage,
        )
    }

    // ─── Test 1: Basic fetch + read ───
    // Write a page to backend, fetch via pool, verify data matches.

    #[test]
    fn test_basic_fetch_and_read() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 0);
        write_pattern(&*storage, 1);
        write_pattern(&*storage, 2);
        write_pattern(&*storage, 3);

        let pool = make_pool(8, storage.clone());

        let guard = pool.fetch_page_shared(2).unwrap();
        verify_pattern(guard.data(), 2);
        assert_eq!(guard.page_id(), 2);
    }

    // ─── Test 2: Cache hit ───
    // Fetch same page twice. Second fetch should be a cache hit.

    #[test]
    fn test_cache_hit() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 1);

        let pool = make_pool(8, storage.clone());

        // First fetch — cache miss, loads from backend.
        let guard1 = pool.fetch_page_shared(1).unwrap();
        verify_pattern(guard1.data(), 1);
        drop(guard1);

        // Overwrite page 1 in the backend with a different pattern.
        // If the pool re-reads from backend, it would get this new data.
        let mut new_data = vec![0xFFu8; PAGE_SIZE];
        new_data[0..4].copy_from_slice(&1u32.to_le_bytes());
        storage.write_page(1, &new_data).unwrap();

        // Second fetch — cache hit, should still see the OLD data from the first load.
        let guard2 = pool.fetch_page_shared(1).unwrap();
        // The pattern should match the original write_pattern, not the 0xFF overwrite.
        verify_pattern(guard2.data(), 1);
    }

    // ─── Test 3: Exclusive write + dirty ───
    // Fetch exclusive, write data, drop guard, verify dirty flag set.

    #[test]
    fn test_exclusive_write_and_dirty() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 0);

        let pool = make_pool(8, storage.clone());

        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            // Write some data.
            let buf = guard.data_mut();
            buf[0] = 0xDE;
            buf[1] = 0xAD;
            // Guard drops here, setting dirty = true.
        }

        // Verify the page shows up in dirty_pages.
        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, 0); // page_id
        assert_eq!(dirty[0].1[0], 0xDE);
        assert_eq!(dirty[0].1[1], 0xAD);
    }

    // ─── Test 4: Multiple shared guards ───
    // Fetch same page shared from two calls, both should succeed.

    #[test]
    fn test_multiple_shared_guards() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 2);

        let pool = make_pool(8, storage.clone());

        let guard1 = pool.fetch_page_shared(2).unwrap();
        let guard2 = pool.fetch_page_shared(2).unwrap();

        verify_pattern(guard1.data(), 2);
        verify_pattern(guard2.data(), 2);

        // Both guards alive simultaneously — shared access.
        drop(guard1);
        drop(guard2);
    }

    // ─── Test 5: Eviction ───
    // Create pool with 3 frames. Load 4 different pages. Verify eviction
    // happened and all data is correct.

    #[test]
    fn test_eviction() {
        let storage = make_memory_storage(4);
        for i in 0..4u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(3, storage.clone());

        // Load pages 0, 1, 2 — fills all 3 frames.
        for i in 0..3u32 {
            let guard = pool.fetch_page_shared(i).unwrap();
            verify_pattern(guard.data(), i);
            drop(guard);
        }
        assert_eq!(pool.used_frames(), 3);

        // Load page 3 — must evict one of the earlier pages.
        let guard = pool.fetch_page_shared(3).unwrap();
        verify_pattern(guard.data(), 3);
        drop(guard);

        // Still only 3 frames used (one was evicted and reused).
        assert_eq!(pool.used_frames(), 3);

        // The evicted page can still be re-loaded from backend.
        // One of pages 0, 1, 2 was evicted. Try loading all — they should all work.
        for i in 0..4u32 {
            let guard = pool.fetch_page_shared(i).unwrap();
            verify_pattern(guard.data(), i);
            drop(guard);
        }
    }

    // ─── Test 6: Clock eviction skips dirty ───
    // Load 3 pages exclusive + modify all. Try to load a 4th. Should fail
    // (BufferPoolFull).

    #[test]
    fn test_clock_eviction_skips_dirty() {
        let storage = make_memory_storage(4);
        for i in 0..4u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(3, storage.clone());

        // Load and dirty all 3 frames.
        for i in 0..3u32 {
            let mut guard = pool.fetch_page_exclusive(i).unwrap();
            guard.data_mut()[0] = 0xFF;
            drop(guard);
        }

        // All 3 frames are dirty. Loading a 4th page should fail.
        let result = pool.fetch_page_shared(3);
        assert!(result.is_err(), "expected BufferPoolFull error");
    }

    // ─── Test 7: dirty_pages snapshot ───
    // Modify several pages, call dirty_pages(), verify all dirty pages returned.

    #[test]
    fn test_dirty_pages_snapshot() {
        let storage = make_memory_storage(8);
        for i in 0..8u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(8, storage.clone());

        // Load all pages, dirty only the even ones.
        for i in 0..8u32 {
            if i % 2 == 0 {
                let mut guard = pool.fetch_page_exclusive(i).unwrap();
                guard.data_mut()[0] = 0xAA;
                drop(guard);
            } else {
                let guard = pool.fetch_page_shared(i).unwrap();
                drop(guard);
            }
        }

        let dirty = pool.dirty_pages();
        let mut dirty_ids: Vec<PageId> = dirty.iter().map(|(pid, _, _)| *pid).collect();
        dirty_ids.sort();
        assert_eq!(dirty_ids, vec![0, 2, 4, 6]);

        // Verify data copies.
        for (pid, data, _lsn) in &dirty {
            assert_eq!(data[0], 0xAA, "dirty page {} should have modified data", pid);
        }
    }

    // ─── Test 8: mark_clean ───
    // Modify page, snapshot, mark_clean with correct LSN. Verify dirty cleared.

    #[test]
    fn test_mark_clean() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 0);

        let pool = make_pool(8, storage.clone());

        // Load and dirty page 0, writing a known LSN.
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            // Write LSN = 42 at offset 24 (little-endian).
            buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE].copy_from_slice(&42u64.to_le_bytes());
        }

        // Snapshot dirty pages.
        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);
        let (pid, _data, lsn) = &dirty[0];
        assert_eq!(*pid, 0);
        assert_eq!(*lsn, 42);

        // Mark clean with matching LSN.
        pool.mark_clean(0, 42);

        // Should no longer be dirty.
        let dirty = pool.dirty_pages();
        assert!(dirty.is_empty(), "page should be clean after mark_clean");
    }

    // ─── Test 9: mark_clean with stale LSN ───
    // Modify page, snapshot, modify again (new LSN), mark_clean with old LSN.
    // Verify dirty NOT cleared.

    #[test]
    fn test_mark_clean_stale_lsn() {
        let storage = make_memory_storage(4);
        write_pattern(&*storage, 0);

        let pool = make_pool(8, storage.clone());

        // First modification: LSN = 10.
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE].copy_from_slice(&10u64.to_le_bytes());
        }

        // Snapshot.
        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].2, 10);

        // Second modification: LSN = 20 (page modified since snapshot).
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE].copy_from_slice(&20u64.to_le_bytes());
        }

        // Try to mark clean with the OLD LSN (10). Should NOT clear dirty.
        pool.mark_clean(0, 10);

        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1, "page should still be dirty (stale LSN)");
        assert_eq!(dirty[0].2, 20, "page should have the new LSN");
    }

    // ─── Test 10: new_page ───
    // Allocate new page, verify zero-filled, write data, verify readable.

    #[test]
    fn test_new_page() {
        let storage = make_memory_storage(4);
        let pool = make_pool(8, storage.clone());

        // Allocate page_id = 0 as a new page.
        {
            let guard = pool.new_page(0).unwrap();
            // Verify zero-filled.
            assert!(
                guard.data().iter().all(|&b| b == 0),
                "new page should be zero-filled"
            );
            drop(guard);
        }

        // Write data to the new page.
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            buf[0..4].copy_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE]);
        }

        // Read it back.
        {
            let guard = pool.fetch_page_shared(0).unwrap();
            assert_eq!(&guard.data()[0..4], &[0xCA, 0xFE, 0xBA, 0xBE]);
        }
    }

    // ─── Test 11: Works with MemoryPageStorage ───
    // (All tests above use MemoryPageStorage, but this test explicitly
    // exercises the full flow end-to-end.)

    #[test]
    fn test_works_with_memory_page_storage() {
        let storage = make_memory_storage(8);
        for i in 0..8u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(4, storage.clone());

        // Load, read, evict, re-load cycle.
        for round in 0..3 {
            for i in 0..8u32 {
                let guard = pool.fetch_page_shared(i).unwrap();
                verify_pattern(guard.data(), i);
                drop(guard);
            }
            // Verify pool stays at 4 frames max.
            assert!(
                pool.used_frames() <= 4,
                "round {}: used_frames should be <= 4, got {}",
                round,
                pool.used_frames()
            );
        }
    }

    // ─── Test 12: Works with FilePageStorage ───
    // Same tests pass with file backend (use tempdir).

    #[test]
    fn test_works_with_file_page_storage() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.db");

        let storage = Arc::new(FilePageStorage::create(&path, PAGE_SIZE).unwrap());
        storage.extend(4).unwrap();

        for i in 0..4u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(4, storage.clone());

        // Fetch each page and verify.
        for i in 0..4u32 {
            let guard = pool.fetch_page_shared(i).unwrap();
            verify_pattern(guard.data(), i);
            drop(guard);
        }

        // Exclusive write + dirty.
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            buf[0] = 0xFF;
        }

        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, 0);
        assert_eq!(dirty[0].1[0], 0xFF);

        // Flush to disk.
        pool.flush_page(0).unwrap();

        // Read directly from file backend to verify flush.
        let mut raw = vec![0u8; PAGE_SIZE];
        storage.read_page(0, &mut raw).unwrap();
        assert_eq!(raw[0], 0xFF);
    }

    // ─── Test 13: Pin prevents eviction ───
    // Fetch a page shared and hold the guard. Load enough pages to trigger
    // eviction. Verify pinned page is never evicted.

    #[test]
    fn test_pin_prevents_eviction() {
        let storage = make_memory_storage(8);
        for i in 0..8u32 {
            write_pattern(&*storage, i);
        }

        let pool = make_pool(3, storage.clone());

        // Pin page 0 by holding a shared guard.
        let pinned_guard = pool.fetch_page_shared(0).unwrap();

        // Load pages 1, 2 to fill the remaining frames.
        {
            let g1 = pool.fetch_page_shared(1).unwrap();
            drop(g1);
        }
        {
            let g2 = pool.fetch_page_shared(2).unwrap();
            drop(g2);
        }

        // Now load pages 3, 4, 5, 6, 7 — eviction must skip the pinned frame.
        for i in 3..8u32 {
            let guard = pool.fetch_page_shared(i).unwrap();
            verify_pattern(guard.data(), i);
            drop(guard);
        }

        // The pinned page 0 should still be valid.
        verify_pattern(pinned_guard.data(), 0);
        drop(pinned_guard);
    }

    // ─── Test 14: dirty_pages snapshots have valid checksums ───
    // BUG: Checksum is never re-stamped after page modification, so
    // dirty_pages() returns pages with stale checksums. DWB recovery
    // relies on valid checksums to detect torn writes.

    #[test]
    fn test_dirty_pages_have_valid_checksums() {
        use crate::page::{PageType, SlottedPage, SlottedPageRef};

        let storage = make_memory_storage(4);
        // Initialize page 0 with a valid checksum.
        {
            let mut buf = vec![0u8; PAGE_SIZE];
            SlottedPage::init(&mut buf, 0, PageType::Heap);
            // init already stamps checksum
            storage.write_page(0, &buf).unwrap();
        }

        let pool = make_pool(8, storage.clone());

        // Modify page 0 through the buffer pool (simulates a B-tree insert).
        {
            let mut guard = pool.fetch_page_exclusive(0).unwrap();
            let buf = guard.data_mut();
            // Write LSN and some data (simulating a real modification).
            buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE].copy_from_slice(&100u64.to_le_bytes());
            let mut page = SlottedPage::from_buf(buf).unwrap();
            page.insert_slot(b"test data after modification").unwrap();
            // Note: no stamp_checksum() call — this is the bug.
        }

        // Snapshot dirty pages (this is what checkpoint does).
        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);

        // Verify the snapshot has a valid checksum.
        let (pid, data, _lsn) = &dirty[0];
        assert_eq!(*pid, 0);
        let page_ref = SlottedPageRef::from_buf(data).unwrap();
        assert!(
            page_ref.verify_checksum(),
            "BUG: dirty page snapshot should have a valid checksum for DWB recovery"
        );
    }

    // ─── Test 15: buffer pool fast-path validates page_id after lock ───
    // BUG: TOCTOU race in fetch_page_shared fast path. After incrementing
    // pin_count and acquiring the read lock, the frame may have been evicted
    // and reused for a different page.

    #[test]
    fn test_fast_path_validates_page_id() {
        let storage = make_memory_storage(8);
        for i in 0..8u32 {
            write_pattern(&*storage, i);
        }

        // Pool with 3 frames — forces eviction.
        let pool = make_pool(3, storage.clone());

        // Load pages 0, 1, 2.
        for i in 0..3u32 {
            let g = pool.fetch_page_shared(i).unwrap();
            drop(g);
        }

        // Load pages 3, 4, 5 — evicts 0, 1, 2.
        for i in 3..6u32 {
            let g = pool.fetch_page_shared(i).unwrap();
            drop(g);
        }

        // Now fetch page 0 again — should get correct data (page 0, not
        // whatever was in the frame before). This is a basic correctness
        // test; the race itself is hard to trigger deterministically.
        let g = pool.fetch_page_shared(0).unwrap();
        assert_eq!(g.page_id(), 0);
        verify_pattern(g.data(), 0);
    }
}
