use crate::buffer_pool::error::BufferError;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("B-tree {0:?} not found")]
    BTreeNotFound(String),

    #[error("B-tree {0:?} already exists")]
    BTreeExists(String),

    #[error("Key not found")]
    KeyNotFound,

    #[error(transparent)]
    Buffer(#[from] BufferError),
}
