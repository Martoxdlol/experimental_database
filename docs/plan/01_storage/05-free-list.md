# 05 — Free Space Management

Implements DESIGN.md §2.6. Free page list + heap free space map.

## File: `src/storage/free_list.rs`

### Free Page List

A linked list of free pages stored in the data file itself. The file header (page 0) stores the head. Each free page stores a `next` pointer.

```rust
/// Manages the free page linked list.
/// Accessed exclusively by the single writer — no locking needed.
pub struct FreePageList {
    /// Head of the free list (PageId). None = list is empty.
    head: Option<PageId>,
}

impl FreePageList {
    /// Create from the file header's free_list_head value.
    pub fn new(head: Option<PageId>) -> Self;

    /// Pop a free page from the list.
    /// Reads the page to get the `next` pointer, updates head.
    /// Returns None if the list is empty.
    pub fn pop(&mut self, pool: &BufferPool) -> Result<Option<PageId>>;

    /// Push a deallocated page onto the free list.
    /// Writes the current head as the page's `next` pointer, updates head.
    pub fn push(&mut self, page_id: PageId, pool: &BufferPool) -> Result<()>;

    /// Current head (for writing to file header during checkpoint).
    pub fn head(&self) -> Option<PageId>;
}
```

### Free Page On-Disk Format

A free page uses the standard page header with `page_type = Free`. The `prev_or_ptr` field stores the next free page ID (0 = end of list). No slot directory or cell data.

```
┌──────────────────────────────┐
│ PageHeader                   │
│   page_type: Free            │
│   prev_or_ptr: next_free_id  │  ← 0 means end of list
│   ...                        │
├──────────────────────────────┤
│ (rest of page unused)        │
└──────────────────────────────┘
```

### Heap Free Space Map

Tracks partially-filled heap pages for efficient large-document insertion.

```rust
/// In-memory map of heap pages with available space.
/// Rebuilt on startup from heap pages. Updated incrementally.
/// Accessed exclusively by the single writer.
pub struct HeapFreeSpaceMap {
    /// Maps heap page_id → approximate free bytes.
    pages: BTreeMap<PageId, u16>,
}

impl HeapFreeSpaceMap {
    pub fn new() -> Self;

    /// Find a heap page with at least `needed` bytes free.
    /// Returns the best-fit page (smallest sufficient free space).
    pub fn find_page(&self, needed: u16) -> Option<PageId>;

    /// Update free space for a page after an insert or delete.
    pub fn update(&mut self, page_id: PageId, free_bytes: u16);

    /// Remove a page from the map (page is full or deallocated).
    pub fn remove(&mut self, page_id: PageId);

    /// Rebuild from all heap pages during startup.
    pub fn rebuild(pool: &BufferPool, page_count: u64) -> Result<Self>;
}
```

### Integration Notes

- `FreePageList::pop/push` are called by `BufferPool::new_page` and B-tree merge/split operations.
- Both structures are owned by the writer task — no concurrent access.
- The free list head is persisted in the file header during checkpoint.
- The heap free space map is ephemeral (rebuilt on startup) — not persisted.
