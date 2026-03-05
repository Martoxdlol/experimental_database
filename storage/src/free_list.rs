//! Free list for page allocation and deallocation.
//!
//! Tracks and manages free (deallocated) pages using a linked list embedded
//! in the pages themselves. The `prev_or_ptr` field in a free page's header
//! stores the next free page ID (0 = end of list). Page 0 is never freed.

use crate::backend::PageId;
use crate::buffer_pool::BufferPool;
use crate::page::{PageType, SlottedPage};
use std::io;
use std::sync::Arc;

/// Free list for page allocation/deallocation.
///
/// Uses a LIFO (stack) discipline: deallocated pages are pushed onto the head,
/// allocated pages are popped from the head. When the list is empty, the file
/// is extended to allocate a new page.
///
/// Thread safety: The free list is accessed only by the single writer.
pub struct FreeList {
    /// Head of the free list. 0 = empty list.
    head: PageId,
    /// Buffer pool for page access.
    buffer_pool: Arc<BufferPool>,
}

impl FreeList {
    /// Create a FreeList with the given head page.
    /// head = 0 means the free list is empty.
    pub fn new(head: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self { head, buffer_pool }
    }

    /// Allocate a page. Pops from the free list, or extends the file.
    /// Returns the allocated PageId.
    pub fn allocate(&mut self) -> io::Result<PageId> {
        if self.head == 0 {
            // List empty — extend the file by 1 page
            let storage = self.buffer_pool.page_storage();
            let old_count = storage.page_count();
            storage.extend(old_count + 1)?;
            let new_page_id = old_count as PageId;
            Ok(new_page_id)
        } else {
            // Pop from head
            let old_head = self.head;
            let guard = self.buffer_pool.fetch_page_exclusive(old_head)?;
            let page = SlottedPage::from_buf_ref(guard.data());
            let next_free = page.prev_or_ptr();
            drop(page);
            drop(guard);
            self.head = next_free;
            Ok(old_head)
        }
    }

    /// Deallocate a page. Pushes it onto the free list.
    ///
    /// # Panics
    /// Panics in debug mode if `page_id == 0` (page 0 is the file header
    /// and must never be freed).
    pub fn deallocate(&mut self, page_id: PageId) -> io::Result<()> {
        debug_assert!(page_id != 0, "page 0 must never be placed on the free list");
        if page_id == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot deallocate page 0",
            ));
        }

        let mut guard = self.buffer_pool.fetch_page_exclusive(page_id)?;
        {
            let buf = guard.data_mut();
            let mut page = SlottedPage::init(buf, page_id, PageType::Free);
            page.set_prev_or_ptr(self.head);
            page.stamp_checksum();
        }
        guard.mark_dirty();
        drop(guard);
        self.head = page_id;
        Ok(())
    }

    /// Current head of the free list (0 = empty).
    pub fn head(&self) -> PageId {
        self.head
    }

    /// Count free pages (walks the list — O(n), for diagnostics only).
    pub fn count(&self) -> io::Result<usize> {
        let mut count = 0;
        let mut current = self.head;
        while current != 0 {
            count += 1;
            let guard = self.buffer_pool.fetch_page_shared(current)?;
            let page = SlottedPage::from_buf_ref(guard.data());
            current = page.prev_or_ptr();
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, PageStorage};
    use crate::buffer_pool::BufferPoolConfig;

    fn setup(num_pages: u64) -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(4096));
        storage.extend(num_pages).unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: 4096,
                frame_count: 64,
            },
            storage,
        ));
        let free_list = FreeList::new(0, pool.clone());
        (pool, free_list)
    }

    // Test 1: Allocate from empty list extends file
    #[test]
    fn allocate_from_empty() {
        let (pool, mut fl) = setup(1); // page 0 exists
        assert_eq!(fl.head(), 0);
        let page_id = fl.allocate().unwrap();
        assert_eq!(page_id, 1); // extended file, new page
        assert_eq!(pool.page_storage().page_count(), 2);
    }

    // Test 2: Deallocate + allocate roundtrip
    #[test]
    fn deallocate_allocate_roundtrip() {
        let (_pool, mut fl) = setup(10);
        fl.deallocate(5).unwrap();
        assert_eq!(fl.head(), 5);
        let page_id = fl.allocate().unwrap();
        assert_eq!(page_id, 5);
        assert_eq!(fl.head(), 0);
    }

    // Test 3: LIFO order
    #[test]
    fn lifo_order() {
        let (_pool, mut fl) = setup(20);
        fl.deallocate(3).unwrap();
        fl.deallocate(7).unwrap();
        fl.deallocate(11).unwrap();
        assert_eq!(fl.allocate().unwrap(), 11);
        assert_eq!(fl.allocate().unwrap(), 7);
        assert_eq!(fl.allocate().unwrap(), 3);
        assert_eq!(fl.head(), 0);
    }

    // Test 4: Mixed allocate/deallocate
    #[test]
    fn mixed_operations() {
        let (_pool, mut fl) = setup(20);
        fl.deallocate(2).unwrap();
        fl.deallocate(4).unwrap();
        let a = fl.allocate().unwrap();
        assert_eq!(a, 4);
        fl.deallocate(6).unwrap();
        let b = fl.allocate().unwrap();
        assert_eq!(b, 6);
        let c = fl.allocate().unwrap();
        assert_eq!(c, 2);
        assert_eq!(fl.head(), 0);
    }

    // Test 5: count()
    #[test]
    fn count_free_pages() {
        let (_pool, mut fl) = setup(20);
        fl.deallocate(1).unwrap();
        fl.deallocate(2).unwrap();
        fl.deallocate(3).unwrap();
        fl.deallocate(4).unwrap();
        fl.deallocate(5).unwrap();
        assert_eq!(fl.count().unwrap(), 5);
    }

    // Test 6: head() tracking
    #[test]
    fn head_tracking() {
        let (_pool, mut fl) = setup(20);
        assert_eq!(fl.head(), 0);
        fl.deallocate(5).unwrap();
        assert_eq!(fl.head(), 5);
        fl.deallocate(10).unwrap();
        assert_eq!(fl.head(), 10);
        fl.allocate().unwrap();
        assert_eq!(fl.head(), 5);
        fl.allocate().unwrap();
        assert_eq!(fl.head(), 0);
    }

    // Test 7: File growth
    #[test]
    fn file_growth() {
        let (pool, mut fl) = setup(10);
        assert_eq!(pool.page_storage().page_count(), 10);
        // Empty free list, should extend
        let page_id = fl.allocate().unwrap();
        assert_eq!(page_id, 10);
        assert_eq!(pool.page_storage().page_count(), 11);
    }

    // Test 8: Deallocate page 0 panics in debug mode
    #[test]
    #[should_panic(expected = "page 0 must never be placed on the free list")]
    fn deallocate_page_zero_errors() {
        let (_pool, mut fl) = setup(10);
        let _ = fl.deallocate(0);
    }
}
