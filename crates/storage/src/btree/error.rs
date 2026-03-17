use crate::buffer_pool::error::BufferError;

#[derive(Debug, thiserror::Error)]
pub enum BTreeError {
    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error("Key exceeds maximum size of {max} bytes (got {actual})")]
    KeyTooLarge { max: usize, actual: usize },

    #[error("Value exceeds maximum size of {max} bytes (got {actual})")]
    ValueTooLarge { max: usize, actual: usize },

    #[error("Page {0} has invalid structure")]
    Corrupted(crate::pager::types::PageId),
}
