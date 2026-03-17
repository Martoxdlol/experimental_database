use crate::pager::types::PageId;

/// Opaque identifier for a B-tree managed by the storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BTreeId(pub(crate) u64);

/// Handle to an open B-tree, identified by its root page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BTree {
    pub root_page: PageId,
}

/// Direction for B-tree range scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}
