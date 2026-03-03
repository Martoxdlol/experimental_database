# 02 — Page Format (`storage/page.rs`)

Implements the slotted page layout from DESIGN §2.3. All page types share the same 32-byte header and slot directory structure.

## Structs

```rust
/// In-memory representation of a page header. Parsed from / serialized to
/// the first 32 bytes of every page.
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub flags: u8,
    pub num_slots: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub prev_or_ptr: u32,       // overloaded: right_sibling | leftmost_child | next_free
    pub reserved: u32,
    pub checksum: u32,
    pub lsn: Lsn,
}

/// A single slot directory entry — points to a cell within the page.
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    pub offset: u16,
    pub length: u16,
}
```

## Functions / Methods

```rust
impl PageHeader {
    /// Read a PageHeader from the first 32 bytes of a page buffer.
    pub fn read_from(buf: &[u8]) -> Self;

    /// Write the PageHeader into the first 32 bytes of a page buffer.
    pub fn write_to(&self, buf: &mut [u8]);
}

/// Slotted page operations on a raw page buffer `[u8; PAGE_SIZE]`.
pub struct SlottedPage<'a> {
    buf: &'a mut [u8],
    page_size: u32,
}

impl<'a> SlottedPage<'a> {
    pub fn new(buf: &'a mut [u8], page_size: u32) -> Self;

    /// Parse the page header.
    pub fn header(&self) -> PageHeader;

    /// Set the page header.
    pub fn set_header(&mut self, header: &PageHeader);

    /// Read slot directory entry at index.
    pub fn slot(&self, index: u16) -> SlotEntry;

    /// Read cell data for a given slot index. Returns the byte slice.
    pub fn cell(&self, index: u16) -> &[u8];

    /// Available free space in bytes.
    pub fn free_space(&self) -> u16;

    /// Insert a cell at logical position `index` (shifts subsequent slots).
    /// Returns Err if not enough space.
    pub fn insert_cell(&mut self, index: u16, data: &[u8]) -> Result<(), PageFullError>;

    /// Remove cell at slot index. Marks space as reclaimable.
    pub fn remove_cell(&mut self, index: u16);

    /// Replace cell at slot index with new data. May fail if new data is
    /// larger and page is full.
    pub fn update_cell(&mut self, index: u16, data: &[u8]) -> Result<(), PageFullError>;

    /// Compact the page: defragment cell data, reclaim gaps.
    pub fn compact(&mut self);

    /// Initialize a fresh empty page with the given type.
    pub fn init(&mut self, page_id: PageId, page_type: PageType);

    /// Compute CRC-32C over the entire page (excluding the checksum field).
    pub fn compute_checksum(&self) -> u32;

    /// Verify the page checksum. Returns false if corrupted.
    pub fn verify_checksum(&self) -> bool;

    /// Update the checksum field in the page header.
    pub fn update_checksum(&mut self);
}
```

## Key Implementation Notes

- All multi-byte integers are **little-endian** (matching the WAL and the host architecture on x86/ARM).
- Slot directory grows forward from byte 32; cell data grows backward from the end.
- `insert_cell` writes data at `free_space_end - data.len()`, then inserts a 4-byte slot entry at `free_space_start`.
- `remove_cell` sets the slot entry to a tombstone marker (offset=0, length=0) and does NOT immediately reclaim cell space — `compact()` does that.
- `compute_checksum` uses `crc32fast` (hardware-accelerated CRC-32C). The checksum covers bytes `[0..checksum_offset]` + `[checksum_offset+4..page_size]`.

## Error Types

```rust
#[derive(Debug)]
pub struct PageFullError;
```
