use async_trait::async_trait;

use crate::pager::{
    error::PagerError,
    types::{PageBuffer, PageId},
};

#[async_trait]
pub trait Pager: Send + Sync {
    /// Returns the configured page size for this pager instance.
    fn page_size(&self) -> usize;

    /// Returns the total number of pages available in this pager.
    fn page_count(&self) -> u64;

    /// Indicates whether this pager is in memory only (non-durable) or backed by persistent storage.
    fn is_durable(&self) -> bool;

    /// Reads a page from disk.
    async fn read_page(&self, id: PageId) -> Result<PageBuffer, PagerError>;

    /// Writes a buffer to disk. Must validate that buffer.len() == self.page_size().
    async fn write_page(&self, id: PageId, data: &PageBuffer) -> Result<(), PagerError>;

    /// Flushes any in-memory buffers to disk to ensure durability. This is a no-op for in-memory pagers.
    async fn sync(&self) -> Result<(), PagerError>;

    /// Extends the pager by allocating additional pages.
    async fn extend(&self, new_page_count: u64) -> Result<(), PagerError>;

    /// Truncates the pager to a smaller size.
    async fn truncate(&self, new_page_count: u64) -> Result<(), PagerError>;
}
