# 05 — Free Space Management (`storage/freelist.rs`)

Manages the free page list and the heap free space map (DESIGN §2.6).

## Structs

```rust
/// Manages free pages in the data file. The free list is a singly-linked
/// list of pages where each free page stores the next free page's ID.
pub struct FreeList {
    head: Option<PageId>,
    count: u32,
}

/// Tracks partially-filled heap pages for external document allocation.
pub struct HeapFreeSpaceMap {
    /// Maps page_id → available free bytes on that heap page.
    pages: BTreeMap<PageId, u16>,
}
```

## Methods

```rust
impl FreeList {
    /// Create from the free_list_head stored in the file header.
    pub fn new(head: Option<PageId>) -> Self;

    /// Pop a free page from the list. Reads the page to find the next pointer.
    pub async fn pop(&mut self, pool: &BufferPool) -> Result<Option<PageId>, StorageError>;

    /// Push a page onto the free list. Writes the current head as the page's
    /// next pointer.
    pub async fn push(&mut self, pool: &BufferPool, page_id: PageId) -> Result<(), StorageError>;

    /// Current head of the free list (for writing to file header).
    pub fn head(&self) -> Option<PageId>;

    /// Number of free pages (approximate).
    pub fn count(&self) -> u32;
}

impl HeapFreeSpaceMap {
    pub fn new() -> Self;

    /// Find a heap page with at least `needed` bytes free.
    pub fn find_page(&self, needed: u16) -> Option<PageId>;

    /// Update the free space for a heap page.
    pub fn update(&mut self, page_id: PageId, free_bytes: u16);

    /// Remove a page from the map (page deallocated or full).
    pub fn remove(&mut self, page_id: PageId);

    /// Rebuild from heap pages during startup.
    pub async fn rebuild(pool: &BufferPool, heap_pages: &[PageId]) -> Result<Self, StorageError>;
}
```

## Free Page Format

A free page stores a single `u32` (next page ID) at a fixed offset after the page header. `0` means end of list.

```
┌──────────────────────┐
│ PageHeader (32 bytes) │  page_type = Free
├──────────────────────┤
│ next_free: u32 LE    │  next page in free list (0 = end)
├──────────────────────┤
│ (unused)             │
└──────────────────────┘
```

Alternatively, we can use the `prev_or_ptr` field in the page header itself to store the next free pointer, avoiding any extra reads into the cell area.
