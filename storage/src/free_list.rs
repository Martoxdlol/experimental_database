//! Free page allocator.
//!
//! Maintains a singly-linked list of deallocated pages. The next-pointer is
//! stored in each free page's `prev_or_ptr` header field, forming an intrusive
//! linked list that requires no additional storage. Accessed exclusively by
//! the single writer task — no locking needed.

use crate::buffer_pool::{BufferPool, BufferPoolError};
use crate::types::*;

/// Manages the free page linked list.
/// Accessed exclusively by the single writer — no locking needed.
pub struct FreePageList {
    head: Option<PageId>,
}

impl FreePageList {
    pub fn new(head: Option<PageId>) -> Self {
        Self { head }
    }

    /// Pop a free page from the list.
    pub fn pop(&mut self, pool: &BufferPool) -> Result<Option<PageId>, BufferPoolError> {
        let page_id = match self.head {
            Some(pid) => pid,
            None => return Ok(None),
        };

        // Read the free page to get the next pointer.
        let guard = pool.fetch_page_shared(page_id)?;
        let sp = guard.as_slotted_page();
        let next = sp.header().prev_or_ptr;
        drop(guard);

        self.head = if next == 0 { None } else { Some(PageId(next)) };
        Ok(Some(page_id))
    }

    /// Push a deallocated page onto the free list.
    pub fn push(&mut self, page_id: PageId, pool: &BufferPool) -> Result<(), BufferPoolError> {
        let mut guard = pool.fetch_page_exclusive(page_id)?;
        let mut sp = guard.as_slotted_page_mut();
        sp.init(page_id, PageType::Free);
        let next = self.head.map(|p| p.0).unwrap_or(0);
        sp.set_prev_or_ptr(next);
        sp.compute_checksum();
        self.head = Some(page_id);
        Ok(())
    }

    pub fn head(&self) -> Option<PageId> {
        self.head
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::{MemPageIO, PageIO};
    use crate::page::SlottedPageMut;

    fn _init_free_page(io: &MemPageIO, page_id: PageId, next: u32) {
        let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
        let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
        sp.init(page_id, PageType::Free);
        sp.set_prev_or_ptr(next);
        sp.compute_checksum();
        io.write_page(page_id, &buf).unwrap();
    }

    #[test]
    fn test_push_pop() {
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 5);
        for i in 1..5 {
            let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
            let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
            sp.init(PageId(i), PageType::BTreeLeaf);
            sp.compute_checksum();
            io.write_page(PageId(i), &buf).unwrap();
        }
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 8);
        let mut fl = FreePageList::new(None);

        // Push pages onto free list.
        fl.push(PageId(3), &pool).unwrap();
        fl.push(PageId(1), &pool).unwrap();

        // Pop them back.
        assert_eq!(fl.pop(&pool).unwrap(), Some(PageId(1)));
        assert_eq!(fl.pop(&pool).unwrap(), Some(PageId(3)));
        assert_eq!(fl.pop(&pool).unwrap(), None);
    }
}
