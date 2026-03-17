use async_trait::async_trait;

use crate::{btree::BTree, engine::error::EngineError};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    // B-tree lifecycle

    async fn create_btree(&self, name: &str) -> Result<BTree, EngineError>;

    async fn get_btree(&self, name: &str) -> Result<BTree, EngineError>;

    async fn delete_btree(&self, name: &str) -> Result<(), EngineError>;

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

/// Opaque reference to a heap-allocated blob (page_id + slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapRef {
    pub page_id: u64,
    pub slot_id: u16,
}
