# 07 — B-Tree

Implements DESIGN.md §2.4, §3.2. B+ tree for primary clustered index and secondary indexes.

## Files

```
src/storage/btree/
  mod.rs      — re-exports
  node.rs     — BTreeNode abstraction over slotted pages
  cursor.rs   — BTreeCursor (seek, next, prev)
  insert.rs   — insert with split
  delete.rs   — delete with merge/redistribute
  scan.rs     — range scan iterator
```

## Node Abstraction

### `node.rs`

```rust
/// Unified view of a B-tree page (internal or leaf) via the slotted page.
/// Interprets cells based on page_type.
pub struct BTreeNode;

impl BTreeNode {
    // --- Internal node operations ---

    /// Read the leftmost child from prev_or_ptr field.
    pub fn leftmost_child(page: &SlottedPage) -> PageId;

    /// Read a cell from an internal node: (key, child_page_id).
    pub fn internal_cell(page: &SlottedPage, slot: u16) -> (EncodedKey, PageId);

    /// Write a cell to an internal node at the given slot.
    pub fn write_internal_cell(
        page: &mut SlottedPageMut,
        slot: u16,
        key: &EncodedKey,
        child: PageId,
    ) -> Result<()>;

    /// Insert an internal cell at the given position. Shifts slots.
    pub fn insert_internal_cell(
        page: &mut SlottedPageMut,
        slot: u16,
        key: &EncodedKey,
        child: PageId,
    ) -> Result<()>;

    /// Set the leftmost child pointer.
    pub fn set_leftmost_child(page: &mut SlottedPageMut, child: PageId);

    /// Number of keys in an internal node.
    pub fn internal_key_count(page: &SlottedPage) -> u16;

    /// Find the child page for a given key (binary search).
    /// Returns the child page to descend into.
    pub fn find_child(page: &SlottedPage, key: &EncodedKey) -> PageId;

    // --- Leaf node operations ---

    /// Read a leaf cell: returns the raw cell bytes at a slot.
    pub fn leaf_cell(page: &SlottedPage, slot: u16) -> &[u8];

    /// Extract the key from a leaf cell.
    /// For primary B-tree: first 24 bytes (doc_id[16] || inv_ts[8]).
    /// For secondary index: variable-length encoded values || doc_id[16] || inv_ts[8].
    pub fn leaf_cell_key(cell: &[u8], key_size: KeySizeHint) -> &[u8];

    /// Insert a leaf cell (key + value). Shifts slots.
    pub fn insert_leaf_cell(
        page: &mut SlottedPageMut,
        slot: u16,
        cell_data: &[u8],
    ) -> Result<()>;

    /// Delete a leaf cell at the given slot.
    pub fn delete_leaf_cell(page: &mut SlottedPageMut, slot: u16);

    /// Binary search for a key in a leaf page.
    /// Returns Ok(slot) for exact match, Err(slot) for insertion point.
    pub fn leaf_search(
        page: &SlottedPage,
        key: &[u8],
        key_size: KeySizeHint,
    ) -> Result<u16, u16>;

    /// Right sibling page ID (from prev_or_ptr in leaf).
    pub fn right_sibling(page: &SlottedPage) -> Option<PageId>;

    /// Set the right sibling pointer.
    pub fn set_right_sibling(page: &mut SlottedPageMut, sibling: Option<PageId>);

    /// Number of cells in a leaf.
    pub fn leaf_cell_count(page: &SlottedPage) -> u16;
}

/// Hint for how to parse key length from a cell.
#[derive(Debug, Clone, Copy)]
pub enum KeySizeHint {
    /// Fixed-size key (e.g., primary key: 24 bytes).
    Fixed(usize),
    /// Variable-size key terminated by doc_id[16] || inv_ts[8] suffix.
    /// The key is everything up to the last 24 bytes of the cell.
    VariableWithSuffix,
}
```

### Internal Cell Format (28 bytes for primary)

```
[key: 24 bytes (doc_id || inv_ts)] [child_page_id: 4 bytes u32 LE]
```

### Leaf Cell Formats

**Primary B-tree (inline)**:
```
[doc_id: 16 bytes] [inv_ts: 8 bytes] [flags: 1 byte] [body_len: 4 bytes u32 LE] [body: body_len bytes]
```

**Primary B-tree (external)**:
```
[doc_id: 16 bytes] [inv_ts: 8 bytes] [flags: 1 byte (EXTERNAL set)] [body_len: 4 bytes] [heap_page_id: 4 bytes] [heap_slot_id: 2 bytes]
```

**Secondary index leaf cell**:
```
[type_tag₁ || value₁ || ... || doc_id: 16 bytes || inv_ts: 8 bytes]
```
No value payload — the doc_id in the key is the value.

## Cursor

### `cursor.rs`

```rust
/// A positioned cursor into a B-tree. Supports seek, next, prev.
/// Holds a shared page guard on the current leaf page.
pub struct BTreeCursor<'a> {
    pool: &'a BufferPool,
    root_page_id: PageId,
    /// Current leaf page guard (shared).
    current_leaf: Option<SharedPageGuard<'a>>,
    /// Current slot within the leaf.
    current_slot: u16,
    /// Key size hint for this B-tree.
    key_hint: KeySizeHint,
}

impl<'a> BTreeCursor<'a> {
    /// Create a new cursor for the B-tree rooted at `root_page_id`.
    pub fn new(pool: &'a BufferPool, root_page_id: PageId, key_hint: KeySizeHint) -> Self;

    /// Seek to the first key >= `target`.
    /// Descends from root to leaf, using binary search at each level.
    pub fn seek(&mut self, target: &[u8]) -> Result<()>;

    /// Seek to the first entry in the B-tree (leftmost leaf, slot 0).
    pub fn seek_first(&mut self) -> Result<()>;

    /// Seek to the last entry in the B-tree (rightmost leaf, last slot).
    pub fn seek_last(&mut self) -> Result<()>;

    /// Is the cursor positioned at a valid cell?
    pub fn is_valid(&self) -> bool;

    /// Read the current cell's key bytes.
    pub fn key(&self) -> Option<&[u8]>;

    /// Read the current cell's full data (key + value).
    pub fn cell_data(&self) -> Option<&[u8]>;

    /// Advance to the next cell.
    /// If at end of leaf page, follow right_sibling pointer.
    pub fn next(&mut self) -> Result<()>;

    /// Move to the previous cell.
    /// If at beginning of leaf page, need to re-traverse from root
    /// (B+ tree has no left sibling pointers).
    pub fn prev(&mut self) -> Result<()>;
}
```

### Seek Algorithm

```
1. Start at root_page_id.
2. Fetch page (shared guard).
3. If internal: binary search → find_child → descend. Release guard.
4. If leaf: binary search → position cursor at slot.
   Hold shared guard as current_leaf.
```

## Insert + Split

### `insert.rs`

```rust
/// Insert a cell into a B-tree.
/// Descends from root, inserts into leaf, splits if necessary.
/// Returns the new root page ID if the root split.
///
/// Called only by the single writer (exclusive page guards).
pub fn btree_insert(
    pool: &BufferPool,
    root_page_id: PageId,
    cell_data: &[u8],
    key_hint: KeySizeHint,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<PageId>;  // returns (possibly new) root
```

### Insert Algorithm

```
1. Descend from root to target leaf, collecting the path (stack of page_ids).
2. Try inserting the cell into the leaf page:
   a. Binary search for insertion slot.
   b. insert_leaf_cell(). If success → done (set LSN, return root).
3. If leaf is full → split:
   a. Allocate a new page from free list.
   b. Move upper half of cells to the new page.
   c. Set right_sibling pointers (old_leaf → new_leaf → old_leaf's old sibling).
   d. The "separator key" is the first key in the new page.
   e. Insert the cell into the correct half.
   f. Propagate: insert (separator_key, new_page_id) into the parent.
4. If parent is full → split parent (same approach, recursively up the path).
5. If root splits → allocate new root page, set as BTreeInternal,
   leftmost_child = old_root, insert one cell (separator, new_sibling).
   Return new root page ID.
```

### Latch Ordering During Split

Split acquires exclusive guards on multiple pages. Rule: acquire in ascending `page_id` order.

For a leaf split: old_leaf (already held), new_page (allocated, higher ID). Safe.
For a parent split propagating up: release child guards before acquiring parent.

## Delete + Merge

### `delete.rs`

```rust
/// Delete a cell from a B-tree by key.
/// Returns the (possibly new) root page ID.
///
/// Called only by the single writer.
pub fn btree_delete(
    pool: &BufferPool,
    root_page_id: PageId,
    key: &[u8],
    key_hint: KeySizeHint,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<PageId>;
```

### Delete Algorithm

```
1. Descend to the leaf containing the key.
2. Binary search → find exact slot. If not found → Err.
3. Delete the cell from the leaf.
4. If leaf is at least half full → done.
5. If underfull:
   a. Try redistribute from right sibling (borrow cells).
   b. If redistribute fails → merge with right sibling:
      - Move all cells from right sibling into current page.
      - Update current page's right_sibling to right sibling's right_sibling.
      - Free the right sibling page.
      - Remove the separator key from the parent.
      - Recurse up if parent is now underfull.
6. If root becomes empty (0 keys, 1 child): child becomes new root. Free old root.
```

### Minimum occupancy

- Leaf: half full (≥ `page_usable_size / 2` bytes of cell data).
- Internal: ≥ `ceil(max_keys / 2)` keys.

## Range Scan

### `scan.rs`

```rust
/// Iterator over a B-tree range [lower, upper).
pub struct BTreeScan<'a> {
    cursor: BTreeCursor<'a>,
    upper_bound: ScanBound,
    exhausted: bool,
}

#[derive(Debug, Clone)]
pub enum ScanBound {
    Excluded(Vec<u8>),
    Unbounded,
}

impl<'a> BTreeScan<'a> {
    /// Create a scan from `lower` (inclusive) to `upper`.
    pub fn new(
        pool: &'a BufferPool,
        root_page_id: PageId,
        lower: &[u8],
        upper: ScanBound,
        key_hint: KeySizeHint,
    ) -> Result<Self>;

    /// Get the next cell in the scan.
    /// Returns None when past upper_bound or end of tree.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>>;
    // returns (key_bytes, cell_data)
}

/// Reverse scan (for order: "desc").
pub struct BTreeReverseScan<'a> {
    // ... similar but uses cursor.prev()
}
```

### Scan Algorithm

```
1. cursor.seek(lower_bound).
2. Loop:
   a. If !cursor.is_valid() → return None.
   b. key = cursor.key()
   c. Check upper_bound:
      - Excluded(bound): if key >= bound → return None.
      - Unbounded: continue.
   d. Yield (key, cell_data).
   e. cursor.next().
```
