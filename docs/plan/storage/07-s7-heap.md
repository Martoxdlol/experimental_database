# S7: Heap (Large Value Storage)

## Purpose

Store arbitrary byte blobs that don't fit inline in B-tree leaf cells. Uses heap pages with overflow chains for multi-page values. No knowledge of document structure.

## Dependencies

- **S2 (Page Format)**: SlottedPage for heap page layout
- **S3 (Buffer Pool)**: page access
- **S4 (Free List)**: page allocation/deallocation

## Rust Types

```rust
use crate::storage::backend::PageId;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::free_list::FreeList;
use std::sync::Arc;
use std::collections::HashMap;

/// Reference to a stored blob: identifies the first page and slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl HeapRef {
    /// Serialize to 6 bytes: page_id(u32 LE) + slot_id(u16 LE)
    pub fn to_bytes(&self) -> [u8; 6];
    pub fn from_bytes(bytes: &[u8; 6]) -> Self;
}

/// Heap storage manager.
pub struct Heap {
    buffer_pool: Arc<BufferPool>,
    /// In-memory map of heap pages with available space.
    /// page_id → approximate free bytes
    free_space_map: HashMap<PageId, usize>,
}

impl Heap {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self;

    /// Store a blob. Returns a HeapRef for later retrieval.
    /// Handles single-page and multi-page (overflow chain) blobs.
    pub fn store(&mut self, data: &[u8], free_list: &mut FreeList) -> Result<HeapRef>;

    /// Load a blob by reference. Reassembles overflow chains.
    pub fn load(&self, href: HeapRef) -> Result<Vec<u8>>;

    /// Free a blob and reclaim its pages.
    pub fn free(&mut self, href: HeapRef, free_list: &mut FreeList) -> Result<()>;

    /// Rebuild the free space map by scanning all heap pages.
    /// Called on startup after recovery.
    pub fn rebuild_free_space_map(&mut self) -> Result<()>;
}
```

## Implementation Details

### Single-Page Blob

If `data.len() <= usable_space_per_slot` (page_size - header - slot_overhead):

1. Find a heap page with enough free space (from `free_space_map`), or allocate a new one.
2. Fetch page exclusive.
3. Insert data as a slot in the slotted page.
4. Update free_space_map entry.
5. Return `HeapRef { page_id, slot_id }`.

### Multi-Page Blob (Overflow Chain)

If `data.len() > usable_space_per_slot`:

1. Calculate how many overflow pages are needed.
2. Store the first chunk in a heap page slot. The slot format for overflow:

```
┌─────────────────────────────────────┐
│ flags: u8                           │  bit 0 = has_overflow
│ total_length: u32                   │  total blob size
│ overflow_page: PageId (u32)         │  first overflow page (0 if none)
│ chunk: [u8; remaining]              │  first chunk of data
└─────────────────────────────────────┘
```

3. For each overflow page:

```
Overflow page (PageType::Overflow):
┌─────────────────────────────────────┐
│ PageHeader                          │
│   page_type = Overflow              │
│   prev_or_ptr = next_overflow_page  │  0 = last page
├─────────────────────────────────────┤
│ data_length: u32                    │
│ data: [u8; data_length]            │
└─────────────────────────────────────┘
```

4. Chain overflow pages via `prev_or_ptr` (next pointer).
5. Return `HeapRef` pointing to the first heap slot.

### load()

1. Fetch the heap page shared. Read the slot.
2. If `flags & HAS_OVERFLOW == 0`: return the data directly.
3. If overflow: read `total_length` and `overflow_page`. Allocate output buffer.
4. Copy first chunk from the slot.
5. Follow overflow chain: fetch each overflow page shared, copy `data_length` bytes.
6. Continue until `next_overflow_page == 0`.
7. Verify total bytes read == `total_length`.

### free()

1. Fetch the heap page exclusive. Read the slot.
2. If overflow: follow the chain, deallocate each overflow page to the free list.
3. Delete the slot in the heap page.
4. Update free_space_map.
5. If heap page is now completely empty: deallocate it too.

### Free Space Map

- **In-memory only**: starts empty on startup, populates incrementally.
- **During operation**: updated incrementally on store/free.
- **Not persisted**: after restart, partially-full heap pages from previous sessions are not immediately reusable. New stores allocate fresh pages until previous pages are touched by `free()`. This trades a small amount of wasted space for O(1) startup — critical for large databases where a full page scan would read hundreds of GB.
- **`rebuild_free_space_map()`**: available for diagnostics (e.g. studio) but NOT called on the startup path.

### Concurrency

Like the free list, the heap is accessed only by the single writer for store/free operations. `load()` is read-only and uses shared page guards — safe for concurrent readers.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error | Page read/write failure | Propagate |
| Corrupt overflow chain | Bad next pointer or length mismatch | Return error |
| Slot not found | HeapRef points to deleted/invalid slot | Return error |
| Data too large | Exceeds max (~16 MB) | Return error |

### Maximum Blob Size Derivation

With `page_size = 8192`, the usable data per overflow page is:

```
8192 - 32 (PageHeader) - 4 (data_length) = 8156 bytes
```

Note: `next_overflow_page` is stored in `PageHeader.prev_or_ptr` (part of the 32-byte header), so it is not subtracted again.

As a safety limit, the maximum overflow chain length is capped at ~2000 pages, giving:

```
2000 × 8156 = 16,312,000 bytes ≈ 16 MB
```

This is a configurable safety limit to prevent runaway chains from a single blob. It can be raised if needed, but keeping it bounded avoids unbounded I/O for a single `load()` call.

## Tests

1. **Store + load small blob**: Store 100 bytes, load back, verify identical.
2. **Store + load page-sized blob**: Store exactly one page worth of data.
3. **Store + load large blob**: Store 50 KB (requires overflow). Load back, verify.
4. **Store + load maximum blob**: Store 1 MB with overflow chain. Verify.
5. **Free small blob**: Store, free, verify the HeapRef is no longer loadable (or returns error).
6. **Free large blob with overflow**: Store 50 KB, free, verify overflow pages deallocated.
7. **Multiple blobs**: Store 20 blobs of varying sizes, load all, verify all correct.
8. **Free space reuse**: Store blob A, free it, store blob B in same-sized slot. Verify B stored in reused space.
9. **rebuild_free_space_map**: Store several blobs, create new Heap instance, rebuild map, verify store works (finds free space correctly).
10. **HeapRef serialization**: Roundtrip to_bytes/from_bytes.
11. **Empty heap operations**: Load from non-existent HeapRef → error.
