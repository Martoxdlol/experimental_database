use crate::{
    btree::error::BTreeError,
    buffer_pool::error::BufferError,
    heap::error::HeapError,
    wal::error::WalError,
};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    BTree(#[from] BTreeError),

    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error(transparent)]
    Heap(#[from] HeapError),

    #[error(transparent)]
    Wal(#[from] WalError),
}
