use async_trait::async_trait;

use crate::{
    btree::{traits::BTreeHandle, types::BTreeId},
    engine::error::EngineError,
    heap::types::HeapRef,
};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Allocate a new B-tree and return its opaque identifier.
    async fn create_btree(&self) -> Result<BTreeId, EngineError>;

    /// Open a B-tree by its identifier and return a handle to operate on it.
    async fn open_btree(&self, id: BTreeId) -> Result<Box<dyn BTreeHandle>, EngineError>;

    // Checkpoint & recovery

    /// Flush all dirty pages and write a checkpoint record.
    async fn checkpoint(&self) -> Result<(), EngineError>;

    // Heap (overflow blob storage)

    /// Store a blob in the heap, returning an opaque reference.
    async fn heap_store(&self, data: &[u8]) -> Result<HeapRef, EngineError>;

    /// Load a blob from the heap by reference.
    async fn heap_load(&self, href: HeapRef) -> Result<Vec<u8>, EngineError>;

    /// Free a heap-allocated blob.
    async fn heap_free(&self, href: HeapRef) -> Result<(), EngineError>;
}
