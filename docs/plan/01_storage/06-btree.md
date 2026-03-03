# 06 — B-Tree (`storage/btree/`)

Three files: `node.rs`, `cursor.rs`, `ops.rs`. Implements B+ tree operations on slotted pages (DESIGN §2.4, §3.2).

## Node Operations (`btree/node.rs`)

Low-level operations on individual B-tree pages (internal and leaf nodes).

```rust
/// Read-only view of a B-tree leaf cell (primary index).
#[derive(Debug)]
pub struct PrimaryLeafCell<'a> {
    pub doc_id: DocId,
    pub inv_ts: InvTs,
    pub flags: CellFlags,
    pub body: CellBody<'a>,
}

/// The body of a leaf cell — either inline data or a heap reference.
#[derive(Debug)]
pub enum CellBody<'a> {
    Inline(&'a [u8]),          // BSON-encoded document slice
    External(HeapRef),         // pointer to external heap
    Tombstone,                 // no body
}

/// Read-only view of a B-tree internal cell.
#[derive(Debug)]
pub struct InternalCell {
    pub key: Vec<u8>,           // raw encoded key bytes
    pub child_page_id: PageId,  // subtree with keys >= key
}

/// Read-only view of a secondary index leaf cell.
#[derive(Debug)]
pub struct SecondaryLeafCell<'a> {
    pub key: &'a [u8],         // encoded index key (type_tag || value || doc_id || inv_ts)
    pub doc_id: DocId,         // extracted from key suffix
    pub inv_ts: InvTs,         // extracted from key suffix
}

/// Operations on a B-tree node page.
pub struct BTreeNode<'a> {
    page: SlottedPage<'a>,
}

impl<'a> BTreeNode<'a> {
    pub fn from_page(page: SlottedPage<'a>) -> Self;

    // ── Leaf node operations ────────────────────────────────

    /// Number of cells in this node.
    pub fn cell_count(&self) -> u16;

    /// Read primary leaf cell at index.
    pub fn read_primary_leaf_cell(&self, index: u16) -> PrimaryLeafCell<'_>;

    /// Read secondary index leaf cell at index.
    pub fn read_secondary_leaf_cell(&self, index: u16) -> SecondaryLeafCell<'_>;

    /// Binary search for a key in a leaf node. Returns the insertion point.
    pub fn search_leaf(&self, key: &[u8]) -> SearchResult;

    /// Insert a cell into a leaf node at the given position.
    pub fn insert_leaf_cell(&mut self, pos: u16, cell_data: &[u8]) -> Result<(), PageFullError>;

    /// Remove a cell from a leaf node.
    pub fn remove_leaf_cell(&mut self, pos: u16);

    /// Right sibling page ID (for leaf nodes only).
    pub fn right_sibling(&self) -> Option<PageId>;

    /// Set the right sibling pointer.
    pub fn set_right_sibling(&mut self, sibling: Option<PageId>);

    // ── Internal node operations ────────────────────────────

    /// Read internal cell at index.
    pub fn read_internal_cell(&self, index: u16) -> InternalCell;

    /// Leftmost child (stored in page header prev_or_ptr).
    pub fn leftmost_child(&self) -> PageId;

    /// Set the leftmost child.
    pub fn set_leftmost_child(&mut self, child: PageId);

    /// Binary search for a key in an internal node.
    /// Returns the child page to descend into.
    pub fn search_internal(&self, key: &[u8]) -> (PageId, u16);

    /// Insert a key + child pointer into an internal node.
    pub fn insert_internal_cell(
        &mut self,
        pos: u16,
        key: &[u8],
        child: PageId,
    ) -> Result<(), PageFullError>;

    /// Remove an internal cell.
    pub fn remove_internal_cell(&mut self, pos: u16);

    // ── Split ───────────────────────────────────────────────

    /// Split a full leaf node. Returns (median_key, new_right_page_data).
    /// The current node keeps the left half; the right half is written to
    /// the returned buffer (caller allocates a new page).
    pub fn split_leaf(&mut self, right_buf: &mut [u8]) -> Vec<u8>;

    /// Split a full internal node. Returns (promoted_key, new_right_page_data).
    pub fn split_internal(&mut self, right_buf: &mut [u8]) -> Vec<u8>;
}

/// Result of a binary search within a node.
#[derive(Debug)]
pub enum SearchResult {
    Found(u16),       // exact match at this slot index
    NotFound(u16),    // insertion point
}
```

---

## B-Tree Cursor (`btree/cursor.rs`)

Stateful cursor for range scans across leaf pages.

```rust
/// Direction of cursor traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

/// A positioned cursor within a B-tree. Holds a pinned leaf page.
pub struct BTreeCursor<'a> {
    pool: &'a BufferPool,
    root_page: PageId,
    current_page: PageId,
    current_slot: u16,
    direction: Direction,
    exhausted: bool,
}

impl<'a> BTreeCursor<'a> {
    /// Seek to the first cell >= `key` (forward) or last cell <= `key` (backward).
    pub async fn seek(
        pool: &'a BufferPool,
        root_page: PageId,
        key: &[u8],
        direction: Direction,
    ) -> Result<Self, StorageError>;

    /// Seek to the very beginning (forward) or end (backward) of the B-tree.
    pub async fn seek_start(
        pool: &'a BufferPool,
        root_page: PageId,
        direction: Direction,
    ) -> Result<Self, StorageError>;

    /// Read the cell data at the current cursor position.
    /// Returns None if cursor is exhausted.
    pub async fn current(&self) -> Result<Option<&[u8]>, StorageError>;

    /// Current page and slot position.
    pub fn position(&self) -> Option<(PageId, u16)>;

    /// Advance the cursor to the next cell.
    /// Follows right_sibling links across pages for forward scans.
    pub async fn advance(&mut self) -> Result<bool, StorageError>;

    /// Whether the cursor has been exhausted.
    pub fn is_exhausted(&self) -> bool;
}
```

---

## High-Level B-Tree Operations (`btree/ops.rs`)

```rust
/// High-level B-tree operations. Works with any B-tree (primary, secondary, catalog).
pub struct BTree<'a> {
    pool: &'a BufferPool,
    root_page: PageId,
}

impl<'a> BTree<'a> {
    pub fn new(pool: &'a BufferPool, root_page: PageId) -> Self;

    /// Point lookup: find the cell with the given key.
    /// Returns the cell data bytes if found.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Insert a key-value cell. Handles splits, updating the root if needed.
    /// Returns the (possibly new) root page ID.
    pub async fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        free_list: &mut FreeList,
    ) -> Result<PageId, StorageError>;

    /// Delete a cell by key. Handles merges if underflow.
    /// Returns the (possibly new) root page ID.
    pub async fn delete(
        &mut self,
        key: &[u8],
        free_list: &mut FreeList,
    ) -> Result<PageId, StorageError>;

    /// Range scan: returns a cursor positioned at the lower bound.
    pub async fn range_scan(
        &self,
        lower: &[u8],
        direction: Direction,
    ) -> Result<BTreeCursor<'a>, StorageError>;

    /// Count all cells in the B-tree (for diagnostics / approximate doc count).
    pub async fn count(&self) -> Result<u64, StorageError>;

    /// Current root page ID.
    pub fn root_page(&self) -> PageId;
}
```

## Key Design Decisions

1. **All keys are raw byte slices**: the B-tree layer is generic — it compares keys with `memcmp`. Key encoding/decoding is the responsibility of the caller (index module).

2. **No lock coupling**: pages are pinned only for the duration of the operation. For splits, the parent path is tracked on the stack during descent.

3. **Split strategy**: when a leaf is full, split at the median. Promote the median key to the parent. If the parent is also full, split recursively. If the root splits, allocate a new root page.

4. **No rebalancing on delete**: B-tree cells are logically deleted (MVCC tombstones) and physically removed during vacuuming. Under-full pages are tolerated. Merges happen only during vacuum-triggered compaction, not on every delete.
