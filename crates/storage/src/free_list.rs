//! Free list for page allocation and deallocation.
//!
//! Tracks and manages free (deallocated) pages using a linked list embedded
//! in the pages themselves. The `prev_or_ptr` field in a free page's header
//! stores the next free page ID (0 = end of list). Page 0 is never freed.

use crate::backend::{PageId, WalStorage};
use crate::buffer_pool::BufferPool;
use crate::page::{PageType, SlottedPage};
use std::collections::BTreeSet;
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
    /// Whether the final physical page is reserved for storage metadata.
    reserve_trailing_page: bool,
    /// Optional total disk quota for page store plus retained WAL.
    max_disk_usage_bytes: Option<u64>,
    /// WAL storage used for quota checks when this free list is engine-owned.
    wal_storage: Option<Arc<dyn WalStorage>>,
}

impl FreeList {
    /// Create a FreeList with the given head page.
    /// head = 0 means the free list is empty.
    pub fn new(head: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            head,
            buffer_pool,
            reserve_trailing_page: false,
            max_disk_usage_bytes: None,
            wal_storage: None,
        }
    }

    /// Create a FreeList that keeps the last page reserved.
    ///
    /// Durable engines use this for the file-header shadow page. When the free
    /// list is empty, allocation converts the old shadow page into the returned
    /// data page and extends the file by one page for the new shadow.
    pub fn new_with_trailing_reservation(head: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            head,
            buffer_pool,
            reserve_trailing_page: true,
            max_disk_usage_bytes: None,
            wal_storage: None,
        }
    }

    /// Attach a total disk quota to page-extension decisions.
    ///
    /// The quota includes page-store bytes plus retained WAL bytes when
    /// `wal_storage` is provided. Existing free pages can still be reused.
    pub fn with_disk_quota(
        mut self,
        max_disk_usage_bytes: Option<u64>,
        wal_storage: Option<Arc<dyn WalStorage>>,
    ) -> Self {
        self.max_disk_usage_bytes = max_disk_usage_bytes;
        self.wal_storage = wal_storage;
        self
    }

    /// Allocate a page. Pops from the free list, or extends the file.
    /// Returns the allocated PageId.
    pub async fn allocate(&mut self) -> io::Result<PageId> {
        if self.head == 0 {
            // List empty — extend the file by 1 page
            let storage = self.buffer_pool.page_storage();
            let old_count = storage.page_count();
            let new_page_id = if self.reserve_trailing_page && old_count > 0 {
                PageId::try_from(old_count - 1).map_err(|_| {
                    crate::error::StorageError::InternalBug(format!(
                        "page count {} exceeds PageId range",
                        old_count
                    ))
                })?
            } else {
                PageId::try_from(old_count).map_err(|_| {
                    crate::error::StorageError::InternalBug(format!(
                        "page count {} exceeds PageId range",
                        old_count
                    ))
                })?
            };
            self.check_extend_quota(old_count + 1)?;
            storage.extend(old_count + 1).await?;
            Ok(new_page_id)
        } else {
            // Pop from head
            let old_head = self.head;
            let guard = self.buffer_pool.fetch_page_exclusive(old_head).await?;
            let page = SlottedPage::from_buf_ref(guard.data())?;
            let next_free = page.prev_or_ptr();
            drop(page);
            drop(guard);
            self.head = next_free;
            Ok(old_head)
        }
    }

    fn check_extend_quota(&self, new_count: u64) -> io::Result<()> {
        let Some(limit) = self.max_disk_usage_bytes else {
            return Ok(());
        };
        let storage = self.buffer_pool.page_storage();
        let projected_page_bytes = new_count
            .checked_mul(storage.page_size() as u64)
            .ok_or_else(|| {
                crate::error::StorageError::InvalidConfig(
                    "page storage size overflow while checking disk usage limit".into(),
                )
            })?;
        let wal_bytes = self
            .wal_storage
            .as_ref()
            .map(|wal| wal.retained_size())
            .unwrap_or(0);
        let projected = projected_page_bytes.saturating_add(wal_bytes);
        if projected > limit {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "disk usage limit exceeded: page extension would grow retained usage to {} bytes (limit {})",
                projected, limit
            ))
            .into());
        }
        Ok(())
    }

    /// Deallocate a page. Pushes it onto the free list.
    ///
    /// # Panics
    /// Panics in debug mode if `page_id == 0` (page 0 is the file header
    /// and must never be freed).
    pub async fn deallocate(&mut self, page_id: PageId) -> io::Result<()> {
        debug_assert!(page_id != 0, "page 0 must never be placed on the free list");
        if page_id == 0 {
            return Err(
                crate::error::StorageError::InternalBug("cannot deallocate page 0".into()).into(),
            );
        }
        if self.reserve_trailing_page
            && page_id as u64 + 1 == self.buffer_pool.page_storage().page_count()
        {
            return Err(crate::error::StorageError::InternalBug(
                "cannot deallocate reserved trailing page".into(),
            )
            .into());
        }

        let mut guard = self.buffer_pool.fetch_page_exclusive(page_id).await?;
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

    /// Return true when `page_id` is already present in the free-list chain.
    ///
    /// This is intended for recovery/repair idempotence. A page only counts as
    /// present when the chain reaches it and the page is actually typed Free.
    pub async fn contains_free_page(&self, page_id: PageId) -> io::Result<bool> {
        if page_id == 0 {
            return Ok(false);
        }

        let mut seen = BTreeSet::new();
        let mut current = self.head;
        while current != 0 {
            if !seen.insert(current) {
                return Ok(false);
            }

            let guard = self.buffer_pool.fetch_page_shared(current).await?;
            let page = SlottedPage::from_buf_ref(guard.data())?;
            if current == page_id {
                return Ok(page.try_page_type() == Some(PageType::Free));
            }
            if page.try_page_type() != Some(PageType::Free) {
                return Ok(false);
            }
            current = page.prev_or_ptr();
        }

        Ok(false)
    }

    /// Rebuild the free-list chain, ensuring the supplied pages are included
    /// exactly once and stamped as Free pages.
    ///
    /// Recovery uses this when rolling back page allocations that may already
    /// appear in a stale durable free-list chain. Rebuilding avoids duplicate
    /// links and fixes pages whose content was initialized after an uncheckpointed
    /// allocation.
    pub async fn rebuild_including_pages<I>(&mut self, page_ids: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = PageId>,
    {
        let storage = self.buffer_pool.page_storage();
        let page_count = storage.page_count();
        let mut pages = BTreeSet::new();
        let mut current = self.head;

        while current != 0 {
            if current as u64 >= page_count || !pages.insert(current) {
                break;
            }

            let guard = self.buffer_pool.fetch_page_shared(current).await?;
            let page = SlottedPage::from_buf_ref(guard.data())?;
            if page.try_page_type() != Some(PageType::Free) {
                break;
            }
            current = page.prev_or_ptr();
        }

        for page_id in page_ids {
            if page_id == 0 {
                continue;
            }
            if page_id as u64 >= page_count {
                return Err(crate::error::StorageError::InternalBug(format!(
                    "cannot add page {page_id} outside page_count {page_count} to free list"
                ))
                .into());
            }
            if self.reserve_trailing_page && page_id as u64 + 1 == page_count {
                return Err(crate::error::StorageError::InternalBug(
                    "cannot deallocate reserved trailing page".into(),
                )
                .into());
            }
            pages.insert(page_id);
        }

        let mut head = 0;
        for page_id in pages.iter().rev().copied() {
            let mut guard = self.buffer_pool.fetch_page_exclusive(page_id).await?;
            {
                let buf = guard.data_mut();
                let mut page = SlottedPage::init(buf, page_id, PageType::Free);
                page.set_prev_or_ptr(head);
                page.stamp_checksum();
            }
            guard.mark_dirty();
            head = page_id;
        }

        self.head = head;
        Ok(pages.len())
    }

    /// Rebuild the free-list chain from pages that are currently stamped Free.
    ///
    /// Crash recovery uses this after WAL replay because the checkpointed
    /// free-list head can point at a page that replay has since reallocated.
    /// Rebuilding from physical page types drops stale links to allocated pages
    /// while preserving all pages that are still genuinely free.
    pub async fn rebuild_from_existing_free_pages(&mut self) -> io::Result<usize> {
        let storage = self.buffer_pool.page_storage();
        let page_count = storage.page_count();
        let mut pages = BTreeSet::new();
        let end = if self.reserve_trailing_page && page_count > 0 {
            page_count - 1
        } else {
            page_count
        };

        for page_id_u64 in 1..end {
            let page_id = PageId::try_from(page_id_u64).map_err(|_| {
                crate::error::StorageError::InternalBug(format!(
                    "page id {} exceeds PageId range",
                    page_id_u64
                ))
            })?;
            let Ok(guard) = self.buffer_pool.fetch_page_shared(page_id).await else {
                continue;
            };
            let Ok(page) = SlottedPage::from_buf_ref(guard.data()) else {
                continue;
            };
            if page.try_page_type() == Some(PageType::Free) {
                pages.insert(page_id);
            }
        }

        let mut head = 0;
        for page_id in pages.iter().rev().copied() {
            let mut guard = self.buffer_pool.fetch_page_exclusive(page_id).await?;
            {
                let buf = guard.data_mut();
                let mut page = SlottedPage::init(buf, page_id, PageType::Free);
                page.set_prev_or_ptr(head);
                page.stamp_checksum();
            }
            guard.mark_dirty();
            head = page_id;
        }

        self.head = head;
        Ok(pages.len())
    }

    /// Replace the free-list head during crash recovery after pages have been
    /// initialized directly in page storage.
    pub(crate) fn set_head_for_recovery(&mut self, head: PageId) {
        self.head = head;
    }

    /// Count free pages (walks the list — O(n), for diagnostics only).
    pub async fn count(&self) -> io::Result<usize> {
        let mut count = 0;
        let mut current = self.head;
        while current != 0 {
            count += 1;
            let guard = self.buffer_pool.fetch_page_shared(current).await?;
            let page = SlottedPage::from_buf_ref(guard.data())?;
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

    async fn setup(num_pages: u64) -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(4096));
        storage.extend(num_pages).await.unwrap();
        for page_id in 0..num_pages {
            let mut buf = vec![0u8; 4096];
            SlottedPage::init(&mut buf, page_id as PageId, PageType::Heap);
            storage.write_page(page_id as PageId, &buf).await.unwrap();
        }
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
    #[tokio::test]
    async fn allocate_from_empty() {
        let (pool, mut fl) = setup(1).await; // page 0 exists
        assert_eq!(fl.head(), 0);
        let page_id = fl.allocate().await.unwrap();
        assert_eq!(page_id, 1); // extended file, new page
        assert_eq!(pool.page_storage().page_count(), 2);
    }

    // Test 2: Deallocate + allocate roundtrip
    #[tokio::test]
    async fn deallocate_allocate_roundtrip() {
        let (_pool, mut fl) = setup(10).await;
        fl.deallocate(5).await.unwrap();
        assert_eq!(fl.head(), 5);
        let page_id = fl.allocate().await.unwrap();
        assert_eq!(page_id, 5);
        assert_eq!(fl.head(), 0);
    }

    // Test 3: LIFO order
    #[tokio::test]
    async fn lifo_order() {
        let (_pool, mut fl) = setup(20).await;
        fl.deallocate(3).await.unwrap();
        fl.deallocate(7).await.unwrap();
        fl.deallocate(11).await.unwrap();
        assert_eq!(fl.allocate().await.unwrap(), 11);
        assert_eq!(fl.allocate().await.unwrap(), 7);
        assert_eq!(fl.allocate().await.unwrap(), 3);
        assert_eq!(fl.head(), 0);
    }

    // Test 4: Mixed allocate/deallocate
    #[tokio::test]
    async fn mixed_operations() {
        let (_pool, mut fl) = setup(20).await;
        fl.deallocate(2).await.unwrap();
        fl.deallocate(4).await.unwrap();
        let a = fl.allocate().await.unwrap();
        assert_eq!(a, 4);
        fl.deallocate(6).await.unwrap();
        let b = fl.allocate().await.unwrap();
        assert_eq!(b, 6);
        let c = fl.allocate().await.unwrap();
        assert_eq!(c, 2);
        assert_eq!(fl.head(), 0);
    }

    // Test 5: count()
    #[tokio::test]
    async fn count_free_pages() {
        let (_pool, mut fl) = setup(20).await;
        fl.deallocate(1).await.unwrap();
        fl.deallocate(2).await.unwrap();
        fl.deallocate(3).await.unwrap();
        fl.deallocate(4).await.unwrap();
        fl.deallocate(5).await.unwrap();
        assert_eq!(fl.count().await.unwrap(), 5);
    }

    // Test 6: head() tracking
    #[tokio::test]
    async fn head_tracking() {
        let (_pool, mut fl) = setup(20).await;
        assert_eq!(fl.head(), 0);
        fl.deallocate(5).await.unwrap();
        assert_eq!(fl.head(), 5);
        fl.deallocate(10).await.unwrap();
        assert_eq!(fl.head(), 10);
        fl.allocate().await.unwrap();
        assert_eq!(fl.head(), 5);
        fl.allocate().await.unwrap();
        assert_eq!(fl.head(), 0);
    }

    // Test 7: File growth
    #[tokio::test]
    async fn file_growth() {
        let (pool, mut fl) = setup(10).await;
        assert_eq!(pool.page_storage().page_count(), 10);
        // Empty free list, should extend
        let page_id = fl.allocate().await.unwrap();
        assert_eq!(page_id, 10);
        assert_eq!(pool.page_storage().page_count(), 11);
    }

    #[tokio::test]
    async fn trailing_reservation_keeps_last_page_unallocated() {
        let storage = Arc::new(MemoryPageStorage::new(4096));
        storage.extend(2).await.unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: 4096,
                frame_count: 64,
            },
            storage,
        ));
        let mut fl = FreeList::new_with_trailing_reservation(0, pool.clone());

        let first = fl.allocate().await.unwrap();
        assert_eq!(first, 1);
        assert_eq!(pool.page_storage().page_count(), 3);

        let second = fl.allocate().await.unwrap();
        assert_eq!(second, 2);
        assert_eq!(pool.page_storage().page_count(), 4);

        let reserved_last = pool.page_storage().page_count() as PageId - 1;
        assert!(fl.deallocate(reserved_last).await.is_err());
    }

    #[tokio::test]
    async fn rebuild_including_pages_is_idempotent_and_stamps_pages_free() {
        let (_pool, mut fl) = setup(10).await;
        fl.deallocate(3).await.unwrap();

        let rebuilt = fl.rebuild_including_pages([3, 5, 5]).await.unwrap();
        assert_eq!(rebuilt, 2);
        assert!(fl.contains_free_page(3).await.unwrap());
        assert!(fl.contains_free_page(5).await.unwrap());
        assert_eq!(fl.count().await.unwrap(), 2);
        assert_eq!(fl.allocate().await.unwrap(), 3);
        assert_eq!(fl.allocate().await.unwrap(), 5);
        assert_eq!(fl.head(), 0);
    }

    // Test 8: Deallocate page 0 panics in debug mode
    #[tokio::test]
    #[should_panic(expected = "page 0 must never be placed on the free list")]
    async fn deallocate_page_zero_errors() {
        let (_pool, mut fl) = setup(10).await;
        let _ = fl.deallocate(0).await;
    }
}
