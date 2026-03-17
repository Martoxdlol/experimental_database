use crate::pager::{error::PagerError, types::PageId};

#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error(transparent)]
    Pager(#[from] PagerError),
    #[error("No free frames available in buffer pool")]
    NoFreeFrames,
    #[error("Page {0} is pinned and cannot be evicted")]
    PagePinned(PageId),
    #[error("Page {0} not found in buffer pool")]
    PageNotFound(PageId),
}
