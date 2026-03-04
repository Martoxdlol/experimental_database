use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::free_list::FreePageList;
use crate::page::{SlottedPage, SlottedPageMut};
use crate::types::*;

#[derive(Debug, thiserror::Error)]
pub enum BufferPoolError {
    #[error("buffer pool full: no clean, unpinned frames available")]
    BufferPoolFull,

    #[error("page checksum mismatch for page {page_id:?}")]
    ChecksumMismatch { page_id: PageId },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Abstraction over page-level disk I/O.
pub trait PageIO: Send + Sync {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), BufferPoolError>;
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), BufferPoolError>;
    fn extend(&self) -> Result<PageId, BufferPoolError>;
    fn sync_file(&self) -> Result<(), BufferPoolError>;
    fn page_count(&self) -> u64;
}

/// In-memory implementation for testing.
pub struct MemPageIO {
    pages: RwLock<Vec<Vec<u8>>>,
    page_size: u32,
}

impl MemPageIO {
    pub fn new(page_size: u32, initial_pages: usize) -> Self {
        let mut pages = Vec::new();
        for _ in 0..initial_pages {
            pages.push(vec![0u8; page_size as usize]);
        }
        Self {
            pages: RwLock::new(pages),
            page_size,
        }
    }
}

impl PageIO for MemPageIO {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), BufferPoolError> {
        let pages = self.pages.read();
        let idx = page_id.0 as usize;
        if idx >= pages.len() {
            return Err(BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("page {idx} not found"),
            )));
        }
        buf.copy_from_slice(&pages[idx]);
        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), BufferPoolError> {
        let mut pages = self.pages.write();
        let idx = page_id.0 as usize;
        if idx >= pages.len() {
            return Err(BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("page {idx} not found"),
            )));
        }
        pages[idx].copy_from_slice(buf);
        Ok(())
    }

    fn extend(&self) -> Result<PageId, BufferPoolError> {
        let mut pages = self.pages.write();
        let id = pages.len() as u32;
        pages.push(vec![0u8; self.page_size as usize]);
        Ok(PageId(id))
    }

    fn sync_file(&self) -> Result<(), BufferPoolError> {
        Ok(())
    }

    fn page_count(&self) -> u64 {
        self.pages.read().len() as u64
    }
}

/// File-backed implementation using pread/pwrite.
pub struct FilePageIO {
    file: std::fs::File,
    page_size: u32,
    page_count: RwLock<u64>,
}

impl FilePageIO {
    pub fn new(file: std::fs::File, page_size: u32, initial_page_count: u64) -> Self {
        Self {
            file,
            page_size,
            page_count: RwLock::new(initial_page_count),
        }
    }

    pub fn page_count_mut(&self) -> parking_lot::RwLockWriteGuard<'_, u64> {
        self.page_count.write()
    }
}

#[cfg(unix)]
impl PageIO for FilePageIO {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), BufferPoolError> {
        use std::os::unix::fs::FileExt;
        let offset = page_id.0 as u64 * self.page_size as u64;
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), BufferPoolError> {
        use std::os::unix::fs::FileExt;
        let offset = page_id.0 as u64 * self.page_size as u64;
        self.file.write_all_at(buf, offset)?;
        Ok(())
    }

    fn extend(&self) -> Result<PageId, BufferPoolError> {
        let mut count = self.page_count.write();
        let id = *count as u32;
        *count += 1;
        let new_size = *count * self.page_size as u64;
        self.file.set_len(new_size)?;
        Ok(PageId(id))
    }

    fn sync_file(&self) -> Result<(), BufferPoolError> {
        self.file.sync_data()?;
        Ok(())
    }

    fn page_count(&self) -> u64 {
        *self.page_count.read()
    }
}

struct FrameData {
    data: Vec<u8>,
    page_id: Option<PageId>,
    dirty: bool,
    lsn: Lsn,
}

/// One frame in the buffer pool.
pub struct FrameSlot {
    lock: RwLock<FrameData>,
    pin_count: AtomicU32,
    ref_bit: AtomicBool,
}

/// The buffer pool manages page frames in memory.
pub struct BufferPool {
    page_table: RwLock<HashMap<PageId, FrameId>>,
    frames: Vec<FrameSlot>,
    clock_hand: AtomicU32,
    frame_count: u32,
    page_size: u32,
    io: Box<dyn PageIO>,
}

/// Shared (read) access to a page.
pub struct SharedPageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    guard: parking_lot::RwLockReadGuard<'a, FrameData>,
    pool: &'a BufferPool,
}

impl<'a> SharedPageGuard<'a> {
    pub fn data(&self) -> &[u8] {
        &self.guard.data
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn as_slotted_page(&self) -> SlottedPage<'_> {
        SlottedPage::new(&self.guard.data, self.pool.page_size)
    }
}

impl Drop for SharedPageGuard<'_> {
    fn drop(&mut self) {
        self.pool.frames[self.frame_id.0 as usize]
            .pin_count
            .fetch_sub(1, Ordering::Release);
    }
}

/// Exclusive (write) access to a page.
pub struct ExclusivePageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    guard: parking_lot::RwLockWriteGuard<'a, FrameData>,
    pool: &'a BufferPool,
    modified: bool,
}

impl<'a> ExclusivePageGuard<'a> {
    pub fn data(&self) -> &[u8] {
        &self.guard.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.modified = true;
        &mut self.guard.data
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn as_slotted_page(&self) -> SlottedPage<'_> {
        SlottedPage::new(&self.guard.data, self.pool.page_size)
    }

    pub fn as_slotted_page_mut(&mut self) -> SlottedPageMut<'_> {
        self.modified = true;
        SlottedPageMut::new(&mut self.guard.data, self.pool.page_size)
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.guard.lsn = lsn;
        self.modified = true;
        // Also write to page header bytes.
        let data = &mut self.guard.data;
        data[24..32].copy_from_slice(&lsn.0.to_le_bytes());
    }
}

impl Drop for ExclusivePageGuard<'_> {
    fn drop(&mut self) {
        if self.modified {
            self.guard.dirty = true;
            self.pool.frames[self.frame_id.0 as usize]
                .ref_bit
                .store(true, Ordering::Release);
        }
        self.pool.frames[self.frame_id.0 as usize]
            .pin_count
            .fetch_sub(1, Ordering::Release);
    }
}

impl BufferPool {
    pub fn new(io: Box<dyn PageIO>, page_size: u32, frame_count: u32) -> Self {
        let mut frames = Vec::with_capacity(frame_count as usize);
        for _ in 0..frame_count {
            frames.push(FrameSlot {
                lock: RwLock::new(FrameData {
                    data: vec![0u8; page_size as usize],
                    page_id: None,
                    dirty: false,
                    lsn: Lsn::ZERO,
                }),
                pin_count: AtomicU32::new(0),
                ref_bit: AtomicBool::new(false),
            });
        }

        Self {
            page_table: RwLock::new(HashMap::new()),
            frames,
            clock_hand: AtomicU32::new(0),
            frame_count,
            page_size,
            io,
        }
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn io(&self) -> &dyn PageIO {
        &*self.io
    }

    /// Fetch a page for reading (shared lock).
    pub fn fetch_page_shared(
        &self,
        page_id: PageId,
    ) -> Result<SharedPageGuard<'_>, BufferPoolError> {
        // Check page table.
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                let frame = &self.frames[frame_id.0 as usize];
                frame.pin_count.fetch_add(1, Ordering::Acquire);
                frame.ref_bit.store(true, Ordering::Release);
                let guard = frame.lock.read();
                return Ok(SharedPageGuard {
                    frame_id,
                    page_id,
                    guard,
                    pool: self,
                });
            }
        }

        // Cache miss: find a victim frame.
        let frame_id = self.find_victim()?;
        let frame = &self.frames[frame_id.0 as usize];

        // Read page from disk into a temp buffer.
        let mut temp = vec![0u8; self.page_size as usize];
        self.io.read_page(page_id, &mut temp)?;

        // Acquire exclusive lock to load data.
        let mut guard = frame.lock.write();

        // Evict old page if any.
        if let Some(old_page_id) = guard.page_id {
            let mut pt = self.page_table.write();
            pt.remove(&old_page_id);
        }

        guard.data.copy_from_slice(&temp);
        guard.page_id = Some(page_id);
        guard.dirty = false;
        guard.lsn = Lsn::ZERO;

        // Update page table.
        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        frame.pin_count.fetch_add(1, Ordering::Acquire);
        frame.ref_bit.store(true, Ordering::Release);

        // Downgrade to shared.
        let guard = parking_lot::RwLockWriteGuard::downgrade(guard);

        Ok(SharedPageGuard {
            frame_id,
            page_id,
            guard,
            pool: self,
        })
    }

    /// Fetch a page for writing (exclusive lock).
    pub fn fetch_page_exclusive(
        &self,
        page_id: PageId,
    ) -> Result<ExclusivePageGuard<'_>, BufferPoolError> {
        // Check page table.
        {
            let pt = self.page_table.read();
            if let Some(&frame_id) = pt.get(&page_id) {
                let frame = &self.frames[frame_id.0 as usize];
                frame.pin_count.fetch_add(1, Ordering::Acquire);
                frame.ref_bit.store(true, Ordering::Release);
                let guard = frame.lock.write();
                return Ok(ExclusivePageGuard {
                    frame_id,
                    page_id,
                    guard,
                    pool: self,
                    modified: false,
                });
            }
        }

        // Cache miss.
        let frame_id = self.find_victim()?;
        let frame = &self.frames[frame_id.0 as usize];

        let mut temp = vec![0u8; self.page_size as usize];
        self.io.read_page(page_id, &mut temp)?;

        let mut guard = frame.lock.write();

        if let Some(old_page_id) = guard.page_id {
            let mut pt = self.page_table.write();
            pt.remove(&old_page_id);
        }

        guard.data.copy_from_slice(&temp);
        guard.page_id = Some(page_id);
        guard.dirty = false;
        guard.lsn = Lsn::ZERO;

        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        frame.pin_count.fetch_add(1, Ordering::Acquire);
        frame.ref_bit.store(true, Ordering::Release);

        Ok(ExclusivePageGuard {
            frame_id,
            page_id,
            guard,
            pool: self,
            modified: false,
        })
    }

    /// Allocate a new page.
    pub fn new_page(
        &self,
        page_type: PageType,
        free_list: &mut FreePageList,
    ) -> Result<ExclusivePageGuard<'_>, BufferPoolError> {
        // Try free list first.
        let page_id = if let Some(pid) = free_list.pop(self)? {
            pid
        } else {
            self.io.extend()?
        };

        let frame_id = self.find_victim()?;
        let frame = &self.frames[frame_id.0 as usize];

        let mut guard = frame.lock.write();

        if let Some(old_page_id) = guard.page_id {
            let mut pt = self.page_table.write();
            pt.remove(&old_page_id);
        }

        // Initialize page.
        guard.data.fill(0);
        let mut sp = SlottedPageMut::new(&mut guard.data, self.page_size);
        sp.init(page_id, page_type);
        guard.page_id = Some(page_id);
        guard.dirty = true;
        guard.lsn = Lsn::ZERO;

        {
            let mut pt = self.page_table.write();
            pt.insert(page_id, frame_id);
        }

        frame.pin_count.fetch_add(1, Ordering::Acquire);
        frame.ref_bit.store(true, Ordering::Release);

        Ok(ExclusivePageGuard {
            frame_id,
            page_id,
            guard,
            pool: self,
            modified: true,
        })
    }

    /// Flush a specific dirty page to disk.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        let pt = self.page_table.read();
        let frame_id = match pt.get(&page_id) {
            Some(&fid) => fid,
            None => return Ok(()),
        };
        drop(pt);

        let frame = &self.frames[frame_id.0 as usize];
        let guard = frame.lock.read();
        if guard.dirty {
            self.io.write_page(page_id, &guard.data)?;
        }
        Ok(())
    }

    /// Snapshot dirty frames for checkpoint.
    pub fn snapshot_dirty_frames(&self) -> Vec<(PageId, Vec<u8>, Lsn)> {
        let mut result = Vec::new();
        for frame in &self.frames {
            let guard = frame.lock.read();
            if guard.dirty {
                if let Some(page_id) = guard.page_id {
                    result.push((page_id, guard.data.clone(), guard.lsn));
                }
            }
        }
        result
    }

    /// Mark a frame clean if its LSN matches.
    pub fn mark_clean_if_lsn(&self, page_id: PageId, expected_lsn: Lsn) {
        let pt = self.page_table.read();
        let frame_id = match pt.get(&page_id) {
            Some(&fid) => fid,
            None => return,
        };
        drop(pt);

        let frame = &self.frames[frame_id.0 as usize];
        let mut guard = frame.lock.write();
        if guard.lsn == expected_lsn {
            guard.dirty = false;
        }
    }

    /// Clock eviction: find a clean, unpinned frame to evict.
    fn find_victim(&self) -> Result<FrameId, BufferPoolError> {
        let max_scan = self.frame_count * 2;
        for _ in 0..max_scan {
            let idx = self.clock_hand.fetch_add(1, Ordering::Relaxed) % self.frame_count;
            let frame = &self.frames[idx as usize];

            // Skip pinned frames.
            if frame.pin_count.load(Ordering::Acquire) > 0 {
                continue;
            }

            // Try to read-lock the frame.
            if let Some(guard) = frame.lock.try_read() {
                if guard.page_id.is_none() {
                    // Free frame — use it.
                    drop(guard);
                    return Ok(FrameId(idx));
                }
                if guard.dirty {
                    // Dirty — skip.
                    continue;
                }
                if frame.ref_bit.load(Ordering::Acquire) {
                    // Second chance.
                    frame.ref_bit.store(false, Ordering::Release);
                    continue;
                }
                // Clean, unpinned, no ref bit — evict.
                drop(guard);
                return Ok(FrameId(idx));
            }
            // Locked — skip.
        }

        Err(BufferPoolError::BufferPoolFull)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _make_pool(page_count: usize, frame_count: u32) -> BufferPool {
        let io = Box::new(MemPageIO::new(DEFAULT_PAGE_SIZE, page_count));
        BufferPool::new(io, DEFAULT_PAGE_SIZE, frame_count)
    }

    fn init_page_in_mem(io: &MemPageIO, page_id: PageId) {
        let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
        let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
        sp.init(page_id, PageType::BTreeLeaf);
        sp.compute_checksum();
        io.write_page(page_id, &buf).unwrap();
    }

    #[test]
    fn test_fetch_shared_cache_miss() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        let guard = pool.fetch_page_shared(PageId(1)).unwrap();
        assert_eq!(guard.page_id(), PageId(1));
        let sp = guard.as_slotted_page();
        assert_eq!(sp.header().page_id, PageId(1));
    }

    #[test]
    fn test_fetch_shared_cache_hit() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        let g1 = pool.fetch_page_shared(PageId(1)).unwrap();
        drop(g1);
        let g2 = pool.fetch_page_shared(PageId(1)).unwrap();
        assert_eq!(g2.page_id(), PageId(1));
    }

    #[test]
    fn test_multiple_shared_guards() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        let g1 = pool.fetch_page_shared(PageId(1)).unwrap();
        let g2 = pool.fetch_page_shared(PageId(1)).unwrap();
        assert_eq!(g1.page_id(), g2.page_id());
    }

    #[test]
    fn test_exclusive_marks_dirty() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        {
            let mut guard = pool.fetch_page_exclusive(PageId(1)).unwrap();
            guard.data_mut()[100] = 0xFF;
        }
        let dirty = pool.snapshot_dirty_frames();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, PageId(1));
    }

    #[test]
    fn test_clock_eviction() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 5);
        for i in 1..5 {
            init_page_in_mem(&io, PageId(i));
        }
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 2);

        // Load pages 1 and 2.
        let g1 = pool.fetch_page_shared(PageId(1)).unwrap();
        drop(g1);
        let g2 = pool.fetch_page_shared(PageId(2)).unwrap();
        drop(g2);

        // Loading page 3 should evict one of 1 or 2.
        let g3 = pool.fetch_page_shared(PageId(3)).unwrap();
        assert_eq!(g3.page_id(), PageId(3));
    }

    #[test]
    fn test_new_page() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 1);
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        let mut fl = FreePageList::new(None);
        let guard = pool.new_page(PageType::BTreeLeaf, &mut fl).unwrap();
        assert_eq!(guard.page_id(), PageId(1)); // extended from page 0.
    }

    #[test]
    fn test_mark_clean_if_lsn() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        {
            let mut guard = pool.fetch_page_exclusive(PageId(1)).unwrap();
            guard.set_lsn(Lsn(100));
            guard.data_mut()[100] = 0x42;
        }
        // Match: should mark clean.
        pool.mark_clean_if_lsn(PageId(1), Lsn(100));
        assert!(pool.snapshot_dirty_frames().is_empty());
    }

    #[test]
    fn test_mark_clean_lsn_mismatch() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 3);
        init_page_in_mem(&io, PageId(1));
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 4);
        {
            let mut guard = pool.fetch_page_exclusive(PageId(1)).unwrap();
            guard.set_lsn(Lsn(200));
            guard.data_mut()[100] = 0x42;
        }
        // Mismatch: should stay dirty.
        pool.mark_clean_if_lsn(PageId(1), Lsn(100));
        assert_eq!(pool.snapshot_dirty_frames().len(), 1);
    }
}
