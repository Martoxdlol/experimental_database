# S4: Free List

## Purpose

Track and manage free (deallocated) pages using a linked list embedded in the pages themselves. Provides allocation (pop) and deallocation (push) of pages.

## Dependencies

- **S3 (Buffer Pool)**: page access via guards
- **S1 (Backend)**: `PageId` type, `PageStorage::extend()` for file growth

## Rust Types

```rust
use crate::storage::backend::PageId;
use crate::storage::buffer_pool::BufferPool;
use std::sync::Arc;

pub struct FreeList {
    /// Head of the free list. 0 = empty list.
    head: PageId,

    /// Buffer pool for page access.
    buffer_pool: Arc<BufferPool>,
}

impl FreeList {
    /// Create a FreeList with the given head page.
    /// head = 0 means the free list is empty.
    pub fn new(head: PageId, buffer_pool: Arc<BufferPool>) -> Self;

    /// Allocate a page. Pops from the free list, or extends the file.
    /// Returns the allocated PageId.
    pub fn allocate(&mut self) -> Result<PageId>;

    /// Deallocate a page. Pushes it onto the free list.
    pub fn deallocate(&mut self, page_id: PageId) -> Result<()>;

    /// Current head of the free list (0 = empty).
    pub fn head(&self) -> PageId;

    /// Count free pages (walks the list — O(n), for diagnostics only).
    pub fn count(&self) -> Result<usize>;
}
```

## Implementation Details

### Free Page Format

A free page uses the standard page layout with `page_type = Free`. The `prev_or_ptr` field in the header stores the **next free page** in the list (0 = end of list). No slot data is needed.

```
┌───────────────────────────┐
│ PageHeader                │
│   page_type = Free        │
│   prev_or_ptr = next_free │ ← pointer to next free page
│   (rest is don't-care)    │
└───────────────────────────┘
```

### allocate()

1. If `head == 0` (list empty):
   - Extend the file by 1 page: `page_storage.extend(current_count + 1)`.
   - The new page_id = old page_count.
   - Return the new page_id. (Caller will initialize it.)
2. If `head != 0` (list has pages):
   - Fetch `head` page exclusively from buffer pool.
   - Read `prev_or_ptr` from header → this is `next_free`.
   - Update `self.head = next_free`.
   - Return old head page_id. (Caller will reinitialize the page for its purpose.)

### deallocate()

1. Fetch `page_id` page exclusively from buffer pool.
2. Initialize it as a Free page: set `page_type = Free`, `prev_or_ptr = self.head`.
3. Mark dirty, drop guard.
4. Update `self.head = page_id`.

### Invariants

- **Page 0 is always the file header and must never be placed on the free list.** The sentinel value `0` means "empty list." This means `deallocate(0)` is a programming error and should panic or return an error in debug builds.

### Concurrency

The free list is accessed **only by the single writer** (Section 5.10 of DESIGN.md). No concurrent access control needed within the free list itself. The writer serializes all allocations and deallocations.

Readers never access the free list — they only read data pages that are already allocated.

### File Extension

When extending the file:
1. Call `page_storage.extend(new_count)`.
2. The new page is zero-filled by the backend.
3. No need to write the page to the free list — it's returned directly to the caller.

**Batch extension optimization** (optional future): extend by multiple pages at once and push extras onto the free list, to reduce extension frequency.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error | Backend extend or page read fails | Propagate |
| Corrupt free page | page_type != Free when traversing list | Return error |

## Tests

1. **Allocate from empty**: Start with head=0, allocate → should extend file, return new page_id.
2. **Deallocate + allocate roundtrip**: Deallocate page 5, allocate → should return page 5.
3. **LIFO order**: Deallocate pages 3, 7, 11. Allocate three times → should return 11, 7, 3 (stack order).
4. **Mixed allocate/deallocate**: Interleave allocations and deallocations, verify all page_ids are valid and no duplicates.
5. **count()**: Deallocate 5 pages, verify count() returns 5.
6. **head() tracking**: Verify head() updates correctly after each operation.
7. **File growth**: Start with 10 pages, empty free list. Allocate → should get page 10 (file extended to 11).
8. **Deallocate already-free page**: This is a programming error — document that behavior is undefined (or add a debug assertion checking page_type before dealloc).
