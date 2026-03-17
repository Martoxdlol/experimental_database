use std::ops::{Deref, DerefMut};

use async_trait::async_trait;

use crate::{buffer_pool::error::BufferError, pager::types::PageId};

/// A RAII guard that manages the "Pin" life-cycle of a page. (I took this from Gemini AI's suggestion and I think it's a great idea)
/// When this is dropped, the Buffer Pool will decrement the pin count.
pub trait PageHandle: Deref<Target = [u8]> + DerefMut + Send + Sync {
    fn id(&self) -> PageId;
    /// Explicitly mark the page as dirty so the pool knows to flush it.
    fn mark_dirty(&mut self);
}

#[async_trait]
pub trait BufferPool: Send + Sync {
    /// Retrieves a page. If not in memory, the Pager is used to load it.
    /// Increments the pin count of the page.
    async fn fetch_page(&self, id: PageId) -> Result<Box<dyn PageHandle + '_>, BufferError>;

    /// Requests a new page from the Pager and pins it in the pool.
    async fn new_page(&self) -> Result<Box<dyn PageHandle + '_>, BufferError>;

    /// Forces a specific page to disk if it is dirty.
    async fn flush_page(&self, id: PageId) -> Result<(), BufferError>;

    /// Flushes all dirty pages to disk (Checkpoint).
    async fn flush_all(&self) -> Result<(), BufferError>;

    /// Deletes a page from the cache and tells the Pager to deallocate it.
    async fn delete_page(&self, id: PageId) -> Result<(), BufferError>;
}
