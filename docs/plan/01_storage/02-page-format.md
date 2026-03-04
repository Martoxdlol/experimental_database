# 02 — Page Format (Slotted Pages)

Implements DESIGN.md §2.3. All page types share the slotted page layout.

## File: `src/storage/page.rs`

### Page Header (32 bytes)

```rust
/// In-memory representation of the 32-byte page header.
/// Parsed from / serialized to raw page bytes.
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: PageId,           // offset 0, u32 LE
    pub page_type: PageType,       // offset 4, u8
    pub flags: u8,                 // offset 5, u8
    pub num_slots: u16,            // offset 6, u16 LE
    pub free_space_start: u16,     // offset 8, u16 LE
    pub free_space_end: u16,       // offset 10, u16 LE
    pub prev_or_ptr: u32,          // offset 12, u32 LE (context-dependent)
    pub _reserved: u32,            // offset 16, u32 LE
    pub checksum: u32,             // offset 20, u32 LE (CRC-32C)
    pub lsn: Lsn,                  // offset 24, u64 LE
}
```

### SlottedPage — Read/Write API

```rust
/// Zero-copy view over a page buffer. Provides cell-level operations.
/// Does NOT own the buffer — works with &[u8] or &mut [u8] from guards.
pub struct SlottedPage<'a> {
    data: &'a [u8],
    page_size: u32,
}

pub struct SlottedPageMut<'a> {
    data: &'a mut [u8],
    page_size: u32,
}
```

### Core Operations

```rust
impl<'a> SlottedPage<'a> {
    /// Parse the 32-byte header.
    pub fn header(&self) -> PageHeader;

    /// Number of slots.
    pub fn num_slots(&self) -> u16;

    /// Read slot directory entry: (offset, length).
    pub fn slot(&self, slot_idx: u16) -> Option<(u16, u16)>;

    /// Read cell data for a given slot.
    pub fn cell_data(&self, slot_idx: u16) -> Option<&[u8]>;

    /// Free space available for new cells (including slot entry overhead).
    pub fn free_space(&self) -> usize;

    /// Verify page checksum (CRC-32C over all bytes except the checksum field).
    pub fn verify_checksum(&self) -> bool;

    /// Iterate all cells: yields (slot_idx, cell_data) pairs.
    pub fn cells(&self) -> impl Iterator<Item = (u16, &[u8])>;
}

impl<'a> SlottedPageMut<'a> {
    /// Initialize a fresh page with the given type and page_id.
    pub fn init(&mut self, page_id: PageId, page_type: PageType);

    /// Insert a cell at the given slot position.
    /// Shifts subsequent slot entries forward in the directory.
    /// Returns Err if insufficient free space.
    pub fn insert_cell(&mut self, slot_idx: u16, data: &[u8]) -> Result<(), PageFullError>;

    /// Replace cell data at an existing slot.
    /// If new data is same size or smaller, in-place update.
    /// If larger, delete + re-insert (may fail if no space).
    pub fn update_cell(&mut self, slot_idx: u16, data: &[u8]) -> Result<(), PageFullError>;

    /// Delete a cell at the given slot position.
    /// Removes slot directory entry and marks cell space as reclaimable.
    pub fn delete_cell(&mut self, slot_idx: u16);

    /// Compact free space: defragment cells so all free space is contiguous.
    /// Called when insert fails due to fragmentation but total free space is sufficient.
    pub fn compact(&mut self);

    /// Set the page header's prev_or_ptr field.
    pub fn set_prev_or_ptr(&mut self, value: u32);

    /// Set LSN in the page header.
    pub fn set_lsn(&mut self, lsn: Lsn);

    /// Compute and write CRC-32C checksum into the header.
    pub fn compute_checksum(&mut self);

    /// All SlottedPage (immutable) methods also available.
    pub fn header(&self) -> PageHeader;
    pub fn slot(&self, slot_idx: u16) -> Option<(u16, u16)>;
    pub fn cell_data(&self, slot_idx: u16) -> Option<&[u8]>;
    pub fn free_space(&self) -> usize;
}
```

### Page Layout Constants

```
Offset 0..32:              Page header
Offset 32..32+4*num_slots: Slot directory (grows forward →)
   Each entry: [offset: u16 LE, length: u16 LE]
...free space...
Cell data:                 Grows backward from page_size (←)
```

### Checksum

- Algorithm: CRC-32C (hardware-accelerated via `crc32c` crate).
- Scope: entire page buffer except bytes 20..24 (the checksum field itself).
- Computed on every flush to disk. Verified on every read from disk.

### Cell Format Notes

The `page.rs` module is agnostic to cell contents — it stores and retrieves opaque byte slices. Cell interpretation (B-tree keys, heap data, etc.) is handled by higher-level modules (`btree/node.rs`, `heap.rs`).

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum PageError {
    #[error("page full: need {needed} bytes, have {available}")]
    PageFull { needed: usize, available: usize },

    #[error("invalid slot index: {0}")]
    InvalidSlot(u16),

    #[error("checksum mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { expected: u32, computed: u32 },

    #[error("invalid page type: {0:#04x}")]
    InvalidPageType(u8),
}
```
