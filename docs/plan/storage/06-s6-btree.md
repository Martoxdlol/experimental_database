# S6: B+ Tree

## Purpose

Generic B+ tree operating on raw byte keys and values with `memcmp` ordering. Supports insert, get, delete, and range scan. Uses the buffer pool for all page access. No knowledge of what keys or values represent.

## Dependencies

- **S2 (Page Format)**: SlottedPage for node layout
- **S3 (Buffer Pool)**: page access via guards

## Rust Types

```rust
use crate::storage::backend::PageId;
use crate::storage::buffer_pool::{BufferPool, SharedPageGuard, ExclusivePageGuard};
use crate::storage::page::{SlottedPage, SlottedPageRef, PageType};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

/// A B+ tree instance. Manages a tree rooted at a specific page.
/// The root page ID is stable — it never changes after creation.
pub struct BTree {
    root_page: PageId,       // permanent, never changes
    buffer_pool: Arc<BufferPool>,
}

impl BTree {
    /// Create a new B-tree with an empty leaf root page.
    /// Allocates one page from the buffer pool.
    pub async fn create(buffer_pool: Arc<BufferPool>, free_list: &mut FreeList) -> Result<Self>;

    /// Open an existing B-tree rooted at `root_page`.
    pub fn open(root_page: PageId, buffer_pool: Arc<BufferPool>) -> Self;

    /// Root page ID. Stable — never changes after creation.
    pub fn root_page(&self) -> PageId;

    /// Point lookup. Returns value bytes if found, None if not.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Insert or update a key-value pair.
    /// If key exists, the value is replaced.
    pub async fn insert(&self, key: &[u8], value: &[u8],
                  free_list: &mut FreeList) -> Result<()>;

    /// Delete a key. Returns true if the key was found and deleted.
    pub async fn delete(&self, key: &[u8], free_list: &mut FreeList) -> Result<bool>;

    /// Range scan with bounds and direction.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                direction: ScanDirection) -> ScanStream;
}

/// Stream over (key, value) pairs from a range scan.
pub struct ScanStream {
    buffer_pool: Arc<BufferPool>,
    current_page: Option<PageId>,
    current_slot: u16,
    lower: Option<(Vec<u8>, bool)>,  // None = unbounded, Some((key, inclusive))
    upper: Option<(Vec<u8>, bool)>,  // None = unbounded, Some((key, inclusive))
    direction: ScanDirection,
    root_page: PageId,  // stable, needed for backward scan re-traversal
    done: bool,
}

impl Stream for ScanStream {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
}
```

## Page Layout

### Leaf Node (PageType::BTreeLeaf)

```
┌──────────────────────────────────────────┐
│ PageHeader                               │
│   page_type = BTreeLeaf                  │
│   prev_or_ptr = right_sibling PageId     │ ← linked list for scans
│   num_slots = N                          │
│   lsn = last modification LSN           │
├──────────────────────────────────────────┤
│ Slot Directory [0..N)                    │
│   Each slot → one key-value cell         │
├──────────────────────────────────────────┤
│ Cell format (per slot):                  │
│   key_len: u16                           │
│   key:     [u8; key_len]                 │
│   value:   [u8; remaining]               │ ← value_len = cell_len - 2 - key_len
└──────────────────────────────────────────┘
```

**Ordering**: Slots are kept in sorted order by key (memcmp). New inserts are placed at the correct position in the slot directory.

### Internal Node (PageType::BTreeInternal)

```
┌──────────────────────────────────────────┐
│ PageHeader                               │
│   page_type = BTreeInternal              │
│   prev_or_ptr = leftmost_child PageId    │ ← subtree with keys < key[0]
│   num_slots = N                          │
│   lsn = last modification LSN           │
├──────────────────────────────────────────┤
│ Slot Directory [0..N)                    │
│   Each slot → one (key, child_page_id)   │
├──────────────────────────────────────────┤
│ Cell format (per slot):                  │
│   key_len: u16                           │
│   key:     [u8; key_len]                 │
│   child:   u32 (PageId)                  │ ← subtree with keys >= this key
└──────────────────────────────────────────┘
```

**Structure**: An internal node with N slots has N+1 children:
- `prev_or_ptr` → children with keys < slot[0].key
- `slot[i].child` → children with keys >= slot[i].key and (if i+1 exists) < slot[i+1].key

## Implementation Details

### get() — Point Lookup

1. Start at root_page.
2. **Traverse internal nodes**: fetch page shared, binary search slots for key, follow the appropriate child pointer.
3. **At leaf node**: binary search slots for key. If found, copy value and return Some(value). If not found, return None.

### insert() — With Split

```
Insert key=K, value=V into tree:

1. Find leaf page containing K (traverse from root)
2. Acquire leaf EXCLUSIVE
3. If leaf has space:
   a. Insert (K, V) at correct position in slot directory
   b. Done
4. If leaf is full → SPLIT:
   a. Allocate new leaf page (from free_list)
   b. Move upper half of entries to new leaf
   c. Set new_leaf.prev_or_ptr = old_leaf.prev_or_ptr (right sibling chain)
   d. Set old_leaf.prev_or_ptr = new_leaf.page_id
   e. Insert (K, V) into the appropriate leaf (old or new)
   f. Promote: push (median_key, new_leaf.page_id) up to parent

   Parent insert:
   g. Fetch parent EXCLUSIVE
   h. If parent has space: insert promoted key + child pointer. Done.
   i. If parent full → split parent (same algorithm, recursive)
   j. If parent is root and splits → evacuate root and rewrite:
      - Allocate evacuated_page from free_list
      - Copy root contents to evacuated_page (fix page_id in header)
      - Rewrite root in-place as Internal node
      - Set root.prev_or_ptr = evacuated_page
      - Insert one slot: (median_key, new_page)
      - Root page ID stays the same
```

**Split Diagram**:

```
BEFORE (leaf full, 5 entries):
┌─────────────────────────────┐
│ [A] [B] [C] [D] [E]        │  ← leaf, right_sibling = X
└─────────────────────────────┘

Insert F (between E and end):

AFTER (split at median C):
┌───────────────────┐     ┌───────────────────┐
│ [A] [B]           │ ──► │ [C] [D] [E] [F]   │ ──► X
│ right_sibling=new │     │ right_sibling=X    │
└───────────────────┘     └───────────────────┘
         old leaf               new leaf

Promote key=C to parent:
┌──────────────────────────┐
│ parent: ... [C, new_pg] ...│
└──────────────────────────┘
```

### delete()

1. Find leaf containing key.
2. Acquire exclusive.
3. If key found: delete slot.
4. If leaf becomes underfull (< 50% used) AND is not root:
   - Try to **redistribute** with a sibling (move entries from sibling that has excess).
   - If sibling is also near-minimum: **merge** the two leaves. Remove the separator key from parent. Deallocate merged page.
5. **Simplification for v1**: Skip merge/redistribute. Allow underfull leaves. This is common in practice (SQLite, WiredTiger). Vacuum or periodic rebalance can clean up later.

### scan() — Range Scan

```
Forward scan [lower, upper]:

1. Find leaf containing lower bound
   (or leftmost leaf if lower = Unbounded)
2. Position at first slot >= lower bound
3. Loop:
   a. Read current slot (key, value)
   b. If key > upper bound (or >= if exclusive): STOP
   c. Yield (key, value)
   d. Advance to next slot
   e. If past last slot in page:
      - Follow right_sibling pointer
      - If right_sibling = 0: STOP
      - Fetch next leaf page shared
      - Position at slot 0
```

**Scan Diagram**:

```
Leaf pages linked by right_sibling:

┌─────────┐    ┌─────────┐    ┌─────────┐
│ [A] [B] │───►│ [C] [D] │───►│ [E] [F] │───► 0
└─────────┘    └─────────┘    └─────────┘

scan(lower=B, upper=E, Forward):
  1. Find leaf containing B → page 1
  2. Position at slot 1 (key=B)
  3. Yield B, advance to page 2
  4. Yield C, D, advance to page 3
  5. Yield E. Next would be F > E → STOP.
```

**Backward scan**: Instead of following right_sibling, we need to navigate via parent. For v1, backward scan can re-traverse from root for each page. Alternatively, maintain a stack of parent pages during initial descent.

### Latch Coupling (Crab Protocol)

For concurrent B-tree access:

1. **Read traversal** (get, scan): acquire child shared lock, then release parent shared lock. Only one page latched at a time.
2. **Write traversal** (insert, delete):
   - Acquire root exclusive.
   - At each level, check if current node is "safe" (won't split/merge after modification).
   - If safe: release all ancestor latches. Continue down with only current latch.
   - If unsafe: keep ancestor latches (they might need modification).

**Safe node definition**:
- For insert: node has enough free space that a split won't be needed.
- For delete: node has enough entries that a merge won't be triggered.

**In single-writer model**: Since only one writer exists (Section 5.10), we can simplify: the writer holds exclusive latches as needed without worrying about concurrent writers. Readers use shared latches and are never blocked by other readers. The writer blocks readers only on the specific pages being modified.

## Cell Encoding/Decoding Helpers

```rust
/// Encode a leaf cell: key_len(u16) || key || value
fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8>;

/// Decode a leaf cell from raw bytes.
fn decode_leaf_cell(cell: &[u8]) -> (&[u8], &[u8]);  // (key, value)

/// Encode an internal cell: key_len(u16) || key || child_page_id(u32)
fn encode_internal_cell(key: &[u8], child: PageId) -> Vec<u8>;

/// Decode an internal cell.
fn decode_internal_cell(cell: &[u8]) -> (&[u8], PageId);  // (key, child)

/// Binary search slots for a key. Returns (found, index).
/// If found: slot at index contains the key.
/// If not found: index is where the key would be inserted.
fn binary_search_slots(page: &SlottedPageRef, key: &[u8], is_leaf: bool) -> (bool, u16);
```

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Page read I/O error | Backend failure | Propagate |
| Page full after split | Should not happen (bug) | Panic |
| Corrupt page | Bad checksum during traversal | Return error |
| Key too large | Key exceeds page capacity | Return error (max key size = page_size / 4) |

## Tests

1. **Create empty tree**: Create B-tree, verify root is a leaf with 0 entries.
2. **Insert + get single**: Insert (key, value), get(key) → returns value.
3. **Insert + get missing**: Insert (key, value), get(other_key) → returns None.
4. **Insert 10K random keys**: Insert 10,000 random byte keys with random values. Get each back. All should match.
5. **Insert sorted keys**: Insert keys in sorted order (append-only pattern). Verify all retrievable.
6. **Insert reverse sorted**: Insert in reverse order. Verify all retrievable.
7. **Leaf split**: Insert enough entries to force a leaf split. Verify all entries still accessible. Verify root_page is unchanged (stable root).
8. **Internal split**: Insert enough to force multiple leaf splits and an internal node split. Verify all entries.
9. **Root split**: Force root to split. Verify new root is internal with 2 children.
10. **Delete single**: Insert keys, delete one, verify get returns None, others still present.
11. **Delete all**: Insert 100 keys, delete all, verify all get returns None.
12. **Scan forward all**: Insert [A, B, C, D, E]. Scan(Unbounded, Unbounded, Forward) → [A, B, C, D, E].
13. **Scan with bounds**: Insert [A, B, C, D, E]. Scan(Included(B), Excluded(E)) → [B, C, D].
14. **Scan backward**: Insert [A, B, C, D, E]. Scan(Unbounded, Unbounded, Backward) → [E, D, C, B, A].
15. **Scan empty range**: Scan with lower > upper → empty.
16. **Scan across leaf boundaries**: Insert enough keys to span multiple leaves. Scan all. Verify continuous and sorted.
17. **Insert duplicate key**: Insert same key twice with different values. Second should overwrite. Get returns second value.
18. **Large values**: Insert entries where value is 2KB. Verify B-tree handles page splits with large entries.
19. **Empty tree operations**: get on empty tree → None. scan → empty. delete → false.
20. **Concurrent readers**: Spawn multiple threads doing get() simultaneously. All should succeed.
