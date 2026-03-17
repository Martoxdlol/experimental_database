use async_trait::async_trait;

use crate::heap::{error::HeapError, types::HeapRef};

/// Overflow blob storage for values too large to inline in B-tree pages.
#[async_trait]
pub trait Heap: Send + Sync {
    /// Store a blob, returning an opaque reference.
    async fn store(&self, data: &[u8]) -> Result<HeapRef, HeapError>;

    /// Load a blob by reference.
    async fn load(&self, href: HeapRef) -> Result<Vec<u8>, HeapError>;

    /// Free a heap-allocated blob.
    async fn free(&self, href: HeapRef) -> Result<(), HeapError>;
}
