# S2: Page Format (Slotted Pages)

## Purpose

Fixed-size page layout with a slot directory for variable-length records. Used by B-trees, heap, and free list. Operates on raw byte buffers — no knowledge of what data the slots contain.

## Dependencies

None. Operates on `&mut [u8]` buffers.

## Rust Types

```rust
pub type PageId = u32; // re-export from backend

/// Page types in the storage engine.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    FileHeader       = 0x06,
    FileHeaderShadow = 0x07,
    BTreeInternal    = 0x01,
    BTreeLeaf        = 0x02,
    Heap             = 0x03,
    Overflow         = 0x04,
    Free             = 0x05,
}

/// 32-byte page header. All multi-byte fields are LITTLE-ENDIAN on disk.
///
/// Uses `zerocopy` for zero-copy read/write directly from page buffers.
/// The `U16`, `U32`, `U64` wrapper types enforce LE encoding at the type level.
/// On LE architectures (x86/ARM) the conversions are no-ops.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct PageHeader {
    pub page_id: U32<LittleEndian>,           // 4 bytes, offset 0
    pub page_type: u8,                         // 1 byte,  offset 4
    pub flags: u8,                             // 1 byte,  offset 5
    pub num_slots: U16<LittleEndian>,          // 2 bytes, offset 6
    pub free_space_start: U16<LittleEndian>,   // 2 bytes, offset 8
    pub free_space_end: U16<LittleEndian>,     // 2 bytes, offset 10
    pub prev_or_ptr: U32<LittleEndian>,        // 4 bytes, offset 12
    pub _reserved: U32<LittleEndian>,          // 4 bytes, offset 16
    pub checksum: U32<LittleEndian>,           // 4 bytes, offset 20
    pub lsn: U64<LittleEndian>,                // 8 bytes, offset 24
}
// Total: 32 bytes
//
// Usage:
//   let header = PageHeader::ref_from_bytes(&buf[..32]).unwrap();
//   let id: u32 = header.page_id.get();
//   header.page_id.set(42);

const PAGE_HEADER_SIZE: usize = 32;
const SLOT_ENTRY_SIZE: usize = 4; // offset: u16 + length: u16

/// Slot directory entry.
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    pub offset: u16,  // byte offset from start of page to cell data
    pub length: u16,  // byte length of cell data (0 = deleted/tombstone)
}

/// Operations on a page buffer. Borrows the buffer mutably.
pub struct SlottedPage<'a> {
    buf: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// Initialize a fresh page buffer.
    pub fn init(buf: &'a mut [u8], page_id: PageId, page_type: PageType) -> Self;

    /// Wrap an existing page buffer (no validation).
    /// Precondition: `buf.len()` must equal `page_size`. This is a programming
    /// error if violated — enforced with `debug_assert_eq!(buf.len(), page_size)`.
    pub fn from_buf(buf: &'a mut [u8]) -> Self;

    /// Read-only wrapper for shared access.
    /// Precondition: `buf.len()` must equal `page_size`. This is a programming
    /// error if violated — enforced with `debug_assert_eq!(buf.len(), page_size)`.
    pub fn from_buf_ref(buf: &'a [u8]) -> SlottedPageRef<'a>;

    // ─── Header access ───
    pub fn header(&self) -> PageHeader;
    pub fn set_header(&mut self, header: &PageHeader);
    pub fn page_id(&self) -> PageId;
    pub fn page_type(&self) -> PageType;
    pub fn num_slots(&self) -> u16;
    pub fn lsn(&self) -> u64;
    pub fn set_lsn(&mut self, lsn: u64);
    pub fn prev_or_ptr(&self) -> u32;
    pub fn set_prev_or_ptr(&mut self, val: u32);

    // ─── Slot operations ───

    /// Read slot data. Returns empty slice for deleted slots.
    pub fn slot_data(&self, slot: u16) -> &[u8];

    /// Insert data as a new slot. Returns the slot index.
    /// Fails if not enough free space (even after compaction).
    pub fn insert_slot(&mut self, data: &[u8]) -> Result<u16, PageFullError>;

    /// Update an existing slot's data in-place.
    /// If new data fits in old space, reuse. Otherwise mark old cell space
    /// as reclaimable, write new cell data at free_space_end (growing backward),
    /// and update the EXISTING slot entry's offset and length to point to the
    /// new cell. This preserves the slot index.
    pub fn update_slot(&mut self, slot: u16, data: &[u8]) -> Result<(), PageFullError>;

    /// Delete a slot (mark as tombstone, length = 0).
    /// Does NOT compact — leaves fragmented space.
    pub fn delete_slot(&mut self, slot: u16);

    // ─── Space management ───

    /// Available contiguous free space (between slot directory end and cell data start).
    pub fn free_space(&self) -> usize;

    /// Total reclaimable space (free_space + fragmented deleted slots).
    pub fn total_reclaimable(&self) -> usize;

    /// Defragment: compact cell data, remove gaps from deleted slots.
    /// Slot indices are preserved (external references remain valid).
    pub fn compact(&mut self);

    // ─── Checksum ───

    /// Compute CRC-32C over entire page EXCEPT the checksum field itself.
    pub fn compute_checksum(&self) -> u32;

    /// Verify the stored checksum matches computed.
    pub fn verify_checksum(&self) -> bool;

    /// Set the checksum field to the computed value.
    pub fn stamp_checksum(&mut self);
}

/// Read-only page view (for SharedPageGuard).
pub struct SlottedPageRef<'a> {
    buf: &'a [u8],
}

impl<'a> SlottedPageRef<'a> {
    pub fn from_buf(buf: &'a [u8]) -> Self;
    pub fn header(&self) -> PageHeader;
    pub fn page_id(&self) -> PageId;
    pub fn page_type(&self) -> PageType;
    pub fn num_slots(&self) -> u16;
    pub fn slot_data(&self, slot: u16) -> &[u8];
    pub fn lsn(&self) -> u64;
    pub fn prev_or_ptr(&self) -> u32;
    pub fn free_space(&self) -> usize;
    pub fn verify_checksum(&self) -> bool;
}

#[derive(Debug)]
pub struct PageFullError;
```

## Implementation Details

### Page Layout

```
Offset 0:                      Page Header (32 bytes)
Offset 32:                     Slot directory (grows forward →)
  slot[0]: offset:u16 + length:u16
  slot[1]: ...
  slot[N-1]: ...
Offset free_space_start:       Free space (uncommitted)
Offset free_space_end:         Cell data (grows backward ←)
  cell[N-1] data
  ...
  cell[0] data
Offset page_size:              End of page
```

### init()

1. Zero-fill the entire buffer.
2. Write PageHeader at offset 0:
   - `page_id`, `page_type` as specified
   - `num_slots = 0`
   - `free_space_start = PAGE_HEADER_SIZE` (32)
   - `free_space_end = page_size as u16`
   - All other fields = 0
3. Stamp checksum.

### insert_slot()

1. Calculate needed space: `SLOT_ENTRY_SIZE + data.len()`.
2. Check if `free_space() >= needed`. If not, try `compact()` then recheck. If still not enough, return `PageFullError`.
3. Write cell data at `free_space_end - data.len()`. Update `free_space_end`.
4. Write slot entry at `free_space_start`: `(offset=new_free_space_end, length=data.len())` (i.e., free_space_end after step 3: old_free_space_end - data.len()).
5. Update `free_space_start += SLOT_ENTRY_SIZE`.
6. Increment `num_slots`.
7. Return slot index (= old num_slots).

### delete_slot()

1. Read slot entry at index `slot`.
2. Set `length = 0` (tombstone marker).
3. Do NOT update `free_space_start` or `free_space_end` — space is fragmented.

### compact()

Compaction only moves cell data to eliminate gaps between live cells. Tombstone directory entries are preserved so that slot indices remain stable. This is required because HeapRef uses `(page_id, slot_id)` and external references must survive compaction.

1. Allocate a temporary buffer (or work in-place from the end).
2. Walk all slots in order. For live slots (length > 0), move cell data to be contiguous from the end of the page and update the slot entry's offset. Tombstone entries (length = 0) are left in the directory unchanged.
3. Update `free_space_end` to reflect the new contiguous cell region.
4. `free_space_start` and `num_slots` are unchanged (tombstone directory entries are kept).

### compute_checksum()

1. Copy the 4-byte checksum field position (offset 20–23).
2. Zero the checksum field in the buffer.
3. Compute CRC-32C over the entire page buffer.
4. Restore the checksum field.
5. Return the CRC-32C value.

**Implementation shortcut**: compute CRC-32C in three parts: bytes [0..20], four zero bytes, bytes [24..page_size]. This avoids modifying the buffer.

### verify_checksum()

1. Read stored checksum from header.
2. Compute checksum (zeroing the stored field).
3. Return `stored == computed`.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| `PageFullError` | Not enough space even after compaction | Caller must split (B-tree) or use overflow (heap) |
| Slot out of bounds | `slot >= num_slots` | Panic (programming error, not runtime) |

## Tests

1. **init + header roundtrip**: Init a page, read header back, verify all fields.
2. **insert one slot**: Insert data, read back via slot_data, verify identical.
3. **insert many slots**: Fill a page with small records until PageFullError. Verify all readable.
4. **delete + recount**: Insert 5 slots, delete slot 2, verify slot_data(2) is empty (length=0).
5. **compact reclaims space**: Insert 10 slots, delete 5, compact, verify free_space increased.
6. **insert after compact**: Insert, delete, compact, insert more — verify it works.
7. **checksum roundtrip**: Init page, stamp checksum, verify_checksum returns true.
8. **checksum detects corruption**: Stamp checksum, flip a byte, verify_checksum returns false.
9. **page types**: Verify each PageType variant has correct u8 value.
10. **prev_or_ptr field**: Set and read back for different page types.
11. **large slot**: Insert a slot that takes most of the page. Verify it works.
12. **boundary: exactly full page**: Insert slots until free_space is exactly 0 (or < SLOT_ENTRY_SIZE).
13. **SlottedPageRef**: Verify read-only operations work identically.
14. **lsn field**: Set LSN, read back, verify.
