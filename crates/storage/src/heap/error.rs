use crate::buffer_pool::error::BufferError;

#[derive(Debug, thiserror::Error)]
pub enum HeapError {
    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error("Invalid heap reference")]
    InvalidRef,
}
