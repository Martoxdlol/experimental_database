use async_trait::async_trait;

use crate::btree::{
    error::BTreeError,
    types::{BTree, ScanDirection},
};

/// Iterator over B-tree key-value pairs.
///
/// Async because advancing may require fetching the next leaf page
/// from the buffer pool.
#[async_trait]
pub trait ScanIterator: Send {
    /// Returns the next key-value pair, or `None` when exhausted.
    async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BTreeError>;
}

/// Core key-value operations on an open B-tree.
#[async_trait]
pub trait BTreeHandle: Send + Sync {
    /// Point lookup by exact key.
    async fn get(&self, tree: &BTree, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError>;

    /// Insert or overwrite a key-value pair.
    async fn insert(&self, tree: &BTree, key: &[u8], value: &[u8]) -> Result<(), BTreeError>;

    /// Delete a key. Returns `true` if the key existed.
    async fn delete(&self, tree: &BTree, key: &[u8]) -> Result<bool, BTreeError>;

    /// Range scan over key-value pairs.
    ///
    /// `start` is inclusive (or scan from the beginning if `None`).
    /// `end` is exclusive (or scan to the end if `None`).
    async fn scan(
        &self,
        tree: &BTree,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
    ) -> Result<Box<dyn ScanIterator + '_>, BTreeError>;
}
