use async_trait::async_trait;

use crate::pager::{
    error::PagerError,
    types::{PageBuffer, PageId},
};

#[async_trait]
pub trait Pager: Send + Sync {
    /// Returns the configured page size for this pager instance.
    fn page_size(&self) -> usize;

    /// Reads a page from disk.
    async fn read_page(&self, id: PageId) -> Result<PageBuffer, PagerError>;

    /// Writes a buffer to disk. Must validate that buffer.len() == self.page_size().
    async fn write_page(&self, id: PageId, data: &PageBuffer) -> Result<(), PagerError>;

    /// Allocates a new page and returns its ID. The pager is responsible for tracking free pages and reusing them as needed.
    async fn allocate_page(&self) -> Result<PageId, PagerError>;

    /// Deallocates a page, marking it as free for future reuse.
    async fn deallocate_page(&self, id: PageId) -> Result<(), PagerError>;

    /// Flushes any in-memory buffers to disk to ensure durability. This is a no-op for in-memory pagers.
    async fn sync(&self) -> Result<(), PagerError>;
}
