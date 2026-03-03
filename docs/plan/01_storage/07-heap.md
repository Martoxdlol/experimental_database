# 07 — External Heap (`storage/heap.rs`)

Stores large document bodies that exceed `EXTERNAL_THRESHOLD` (DESIGN §2.5).

## Structs

```rust
/// Manages external heap pages for large documents.
pub struct ExternalHeap<'a> {
    pool: &'a BufferPool,
    free_space_map: HeapFreeSpaceMap,
    free_list: &'a mut FreeList,
    page_size: u32,
}

/// Result of reading a document from the external heap.
pub struct HeapDocument {
    pub body: Vec<u8>,  // full BSON document body (reassembled from overflow if needed)
}
```

## Methods

```rust
impl<'a> ExternalHeap<'a> {
    pub fn new(
        pool: &'a BufferPool,
        free_space_map: HeapFreeSpaceMap,
        free_list: &'a mut FreeList,
        page_size: u32,
    ) -> Self;

    /// Store a document body in the heap. Returns a HeapRef for the B-tree cell.
    /// For bodies that fit in a single heap page, allocates one slot.
    /// For larger bodies, allocates a slot + overflow pages.
    pub async fn store(&mut self, body: &[u8]) -> Result<HeapRef, StorageError>;

    /// Read a document body from the heap, given its HeapRef.
    /// Follows overflow page chains for multi-page documents.
    pub async fn read(&self, heap_ref: HeapRef) -> Result<HeapDocument, StorageError>;

    /// Delete a document from the heap. Frees the slot and any overflow pages.
    pub async fn delete(&mut self, heap_ref: HeapRef) -> Result<(), StorageError>;

    /// Replace a document in the heap. May reuse the same slot if the new
    /// body fits, otherwise deallocates the old and allocates new.
    pub async fn replace(
        &mut self,
        heap_ref: HeapRef,
        new_body: &[u8],
    ) -> Result<HeapRef, StorageError>;

    /// Access the free space map (for checkpoint/startup).
    pub fn free_space_map(&self) -> &HeapFreeSpaceMap;
}
```

## Overflow Page Format

Per DESIGN §2.5:

```
┌─────────────────────────────────────┐
│ PageHeader (32 bytes)               │  page_type = Overflow
│   prev_or_ptr = next_overflow_page  │  (0 = last page)
├─────────────────────────────────────┤
│ data_length: u16                    │
│ data: [u8; data_length]            │
└─────────────────────────────────────┘
```

The `prev_or_ptr` in the page header stores the next overflow page ID. Maximum usable data per overflow page = `page_size - PAGE_HEADER_SIZE - 2`.
