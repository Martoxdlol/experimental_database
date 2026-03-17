use crate::{btree::error::BTreeError, buffer_pool::error::BufferError};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    BTree(#[from] BTreeError),

    #[error(transparent)]
    Buffer(#[from] BufferError),
}
