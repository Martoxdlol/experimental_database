# 08 — External Heap + Overflow Pages

Implements DESIGN.md §2.5. Large document storage outside the B-tree.

## File: `src/storage/heap.rs`

### When External Storage Is Used

Documents whose BSON-encoded body exceeds `EXTERNAL_THRESHOLD` (default: `page_size / 2` = 4 KB for 8 KB pages) are stored in heap pages instead of inline in B-tree leaf cells. The B-tree leaf cell stores a `HeapRef` pointer.

### Structures

```rust
/// Manages the external heap: insertion, reading, and deletion of large documents.
/// Accessed exclusively by the single writer (no concurrent access control).
pub struct ExternalHeap {
    free_space_map: HeapFreeSpaceMap,
    page_size: u32,
    external_threshold: u32,
}

impl ExternalHeap {
    pub fn new(page_size: u32, external_threshold: u32) -> Self;

    /// Store a document body in the heap.
    /// Returns the HeapRef for the B-tree leaf cell.
    ///
    /// If body fits in a single heap page slot → single-page mode.
    /// If body is larger → first slot + overflow chain.
    pub fn store(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        body: &[u8],
        lsn: Lsn,
    ) -> Result<HeapRef>;

    /// Read a document body from the heap.
    /// Follows overflow chain if needed.
    pub fn read(
        &self,
        pool: &BufferPool,
        heap_ref: HeapRef,
    ) -> Result<Vec<u8>>;

    /// Delete a document from the heap.
    /// Frees the heap slot and all overflow pages.
    pub fn delete(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        heap_ref: HeapRef,
        lsn: Lsn,
    ) -> Result<()>;

    /// Replace a document body in the heap.
    /// Deletes old, stores new. Returns new HeapRef.
    pub fn replace(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        old_ref: HeapRef,
        new_body: &[u8],
        lsn: Lsn,
    ) -> Result<HeapRef>;

    /// Rebuild the free space map from all heap pages.
    pub fn rebuild_free_space_map(&mut self, pool: &BufferPool, page_count: u64) -> Result<()>;
}
```

### Heap Page Layout

Heap pages use the standard slotted page format with `page_type = Heap`. Each slot holds one document body (or the first chunk for multi-page documents).

**Single-page document** (body fits in usable page space):

```
Slot data: [body_length: u32 LE] [body: body_length bytes]
```

**Multi-page document** (body exceeds usable page space):

First slot:
```
[total_body_length: u32 LE] [overflow_page_id: u32 LE] [chunk: remaining slot bytes]
```

The `overflow_page_id` points to the first overflow page in a chain.

### Overflow Page Layout

```
┌─────────────────────────────────┐
│ Page Header (32 bytes)          │
│   page_type: Overflow           │
│   prev_or_ptr: next_overflow_id │  ← 0 = last page
├─────────────────────────────────┤
│ data_length: u16 LE             │
│ data: [u8; data_length]         │
└─────────────────────────────────┘
```

Overflow pages do NOT use the slot directory. They store a single contiguous chunk of the document body after the page header + 2-byte length field.

Usable data per overflow page: `page_size - PAGE_HEADER_SIZE - 2`.

### Store Algorithm

```
1. If body.len() <= external_threshold:
   → Not external. Caller should store inline. (Assertion.)

2. Compute usable space per heap slot:
   usable = page_size - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE - 4 (body_len prefix)

3. If body.len() <= usable (single-page):
   a. Find a heap page with enough space via free_space_map.
   b. If found: fetch_page_exclusive, insert slot, update free_space_map.
   c. If not found: allocate new heap page via free_list or extend.
   d. Return HeapRef { page_id, slot_id }.

4. If body.len() > usable (multi-page):
   a. First slot: store initial chunk + overflow_page_id.
   b. Remaining body: allocate overflow pages, chain via next_overflow.
   c. Return HeapRef { page_id, slot_id } pointing to the first slot.
```

### Read Algorithm

```
1. Fetch heap page (shared guard).
2. Read slot at heap_ref.slot_id.
3. Parse body_length.
4. If body fits in slot (no overflow pointer): return body.
5. If overflow: read first chunk, follow overflow_page_id chain:
   a. Fetch each overflow page (shared guard).
   b. Append data_length bytes.
   c. Follow next_overflow until 0.
6. Assert total == body_length.
7. Return concatenated body.
```

### Delete Algorithm

```
1. Fetch heap page (exclusive guard).
2. Read slot: check for overflow pointer.
3. If overflow: walk the chain, free each overflow page to free_list.
4. Delete the heap slot.
5. Update free_space_map with the new free space.
6. If heap page is now completely empty: free it to free_list, remove from map.
```

### Integration with B-Tree

The B-tree insert/delete code checks `body.len() > external_threshold`:

- **Insert**: if external, call `heap.store()` → get `HeapRef` → build leaf cell with `flags = EXTERNAL`.
- **Delete**: if cell has `flags & EXTERNAL`, parse `HeapRef` from cell → call `heap.delete()`.
- **Vacuum**: same as delete — the vacuumed version's heap data must be freed.
