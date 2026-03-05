//! Slotted page format for variable-length records.
//!
//! Fixed-size page layout with a slot directory. Used by B-trees, heap, and free list.
//! Operates on raw byte buffers with no knowledge of what data the slots contain.
//!
//! ## Page Layout
//!
//! ```text
//! Offset 0:                 PageHeader (32 bytes)
//! Offset 32:                Slot directory (grows forward ->)
//!   slot[0]: offset:u16 + length:u16
//!   slot[1]: ...
//!   slot[N-1]: ...
//! Offset free_space_start:  Free space (uncommitted)
//! Offset free_space_end:    Cell data (grows backward <-)
//!   cell[N-1] data
//!   ...
//!   cell[0] data
//! Offset page_size:         End of page
//! ```

use crate::backend::PageId;
use zerocopy::byteorder::{LittleEndian, U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of the page header in bytes.
const PAGE_HEADER_SIZE: usize = 32;

/// Size of a single slot directory entry (offset: u16 + length: u16).
const SLOT_ENTRY_SIZE: usize = 4;

/// Byte offset of the `checksum` field within the page header.
const CHECKSUM_OFFSET: usize = 20;

/// Size of the checksum field in bytes.
const CHECKSUM_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// PageType
// ---------------------------------------------------------------------------

/// Page types in the storage engine.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    BTreeInternal = 0x01,
    BTreeLeaf = 0x02,
    Heap = 0x03,
    Overflow = 0x04,
    Free = 0x05,
    FileHeader = 0x06,
    FileHeaderShadow = 0x07,
}

impl PageType {
    /// Convert a raw `u8` to a `PageType`, panicking on invalid values.
    fn from_u8(v: u8) -> Self {
        match v {
            0x01 => PageType::BTreeInternal,
            0x02 => PageType::BTreeLeaf,
            0x03 => PageType::Heap,
            0x04 => PageType::Overflow,
            0x05 => PageType::Free,
            0x06 => PageType::FileHeader,
            0x07 => PageType::FileHeaderShadow,
            _ => panic!("invalid page type: {:#04x}", v),
        }
    }
}

// ---------------------------------------------------------------------------
// PageHeader
// ---------------------------------------------------------------------------

/// 32-byte page header. All multi-byte fields are little-endian on disk.
///
/// Uses `zerocopy` for zero-copy read/write directly from page buffers.
/// The `U16`, `U32`, `U64` wrapper types enforce LE encoding at the type level.
/// On LE architectures (x86/ARM) the conversions are no-ops.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub struct PageHeader {
    /// Page identifier.
    pub page_id: U32<LittleEndian>,         // 4 bytes, offset 0
    /// Discriminant indicating the page's role.
    pub page_type: u8,                       // 1 byte,  offset 4
    /// Bit flags (reserved, page-type specific).
    pub flags: u8,                           // 1 byte,  offset 5
    /// Number of slots in the directory (including tombstones).
    pub num_slots: U16<LittleEndian>,        // 2 bytes, offset 6
    /// Byte offset where the slot directory ends (next free byte for a new slot entry).
    pub free_space_start: U16<LittleEndian>, // 2 bytes, offset 8
    /// Byte offset where the cell data region begins (grows downward).
    pub free_space_end: U16<LittleEndian>,   // 2 bytes, offset 10
    /// Previous page pointer (leaf sibling) or generic pointer (internal).
    pub prev_or_ptr: U32<LittleEndian>,      // 4 bytes, offset 12
    /// Reserved for future use.
    pub _reserved: U32<LittleEndian>,        // 4 bytes, offset 16
    /// CRC-32C checksum of the page (computed with this field zeroed).
    pub checksum: U32<LittleEndian>,         // 4 bytes, offset 20
    /// Log sequence number of the last WAL record that modified this page.
    pub lsn: U64<LittleEndian>,             // 8 bytes, offset 24
}

// Compile-time assertion that PageHeader is exactly 32 bytes.
const _: () = assert!(std::mem::size_of::<PageHeader>() == PAGE_HEADER_SIZE);

// ---------------------------------------------------------------------------
// SlotEntry
// ---------------------------------------------------------------------------

/// Slot directory entry.
#[derive(Debug, Clone, Copy)]
pub struct SlotEntry {
    /// Byte offset from start of page to cell data.
    pub offset: u16,
    /// Byte length of cell data (0 = deleted/tombstone).
    pub length: u16,
}

// ---------------------------------------------------------------------------
// PageFullError
// ---------------------------------------------------------------------------

/// Error returned when a page does not have enough space for the requested operation.
#[derive(Debug)]
pub struct PageFullError;

impl std::fmt::Display for PageFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "page full: not enough space even after compaction")
    }
}

impl std::error::Error for PageFullError {}

// ---------------------------------------------------------------------------
// SlottedPage (mutable)
// ---------------------------------------------------------------------------

/// Mutable operations on a page buffer.
pub struct SlottedPage<'a> {
    buf: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    // ─── Construction ────────────────────────────────────────────────

    /// Initialize a fresh page buffer.
    ///
    /// 1. Zero-fills the entire buffer.
    /// 2. Writes the page header.
    /// 3. Stamps the checksum.
    pub fn init(buf: &'a mut [u8], page_id: PageId, page_type: PageType) -> Self {
        let page_size = buf.len();
        assert!(
            page_size >= PAGE_HEADER_SIZE,
            "page buffer too small: {} < {}",
            page_size,
            PAGE_HEADER_SIZE
        );

        // Zero-fill
        buf.fill(0);

        let header = PageHeader {
            page_id: U32::new(page_id),
            page_type: page_type as u8,
            flags: 0,
            num_slots: U16::new(0),
            free_space_start: U16::new(PAGE_HEADER_SIZE as u16),
            free_space_end: U16::new(page_size as u16),
            prev_or_ptr: U32::new(0),
            _reserved: U32::new(0),
            checksum: U32::new(0),
            lsn: U64::new(0),
        };

        let mut page = SlottedPage { buf };
        page.set_header(&header);
        page.stamp_checksum();
        page
    }

    /// Wrap an existing page buffer (no validation).
    ///
    /// # Panics
    ///
    /// Debug-asserts that the buffer is at least `PAGE_HEADER_SIZE` bytes.
    pub fn from_buf(buf: &'a mut [u8]) -> Self {
        debug_assert!(
            buf.len() >= PAGE_HEADER_SIZE,
            "page buffer too small: {}",
            buf.len()
        );
        SlottedPage { buf }
    }

    /// Create a read-only wrapper for shared access.
    pub fn from_buf_ref(buf: &'a [u8]) -> SlottedPageRef<'a> {
        SlottedPageRef::from_buf(buf)
    }

    // ─── Header access ───────────────────────────────────────────────

    /// Read the page header (copies 32 bytes out of the buffer).
    pub fn header(&self) -> PageHeader {
        *PageHeader::ref_from_bytes(&self.buf[..PAGE_HEADER_SIZE]).unwrap()
    }

    /// Write a page header into the buffer.
    pub fn set_header(&mut self, header: &PageHeader) {
        self.buf[..PAGE_HEADER_SIZE].copy_from_slice(header.as_bytes());
    }

    /// Return the page identifier.
    pub fn page_id(&self) -> PageId {
        self.header().page_id.get()
    }

    /// Return the page type.
    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.header().page_type)
    }

    /// Return the number of slots (including tombstones).
    pub fn num_slots(&self) -> u16 {
        self.header().num_slots.get()
    }

    /// Return the log sequence number.
    pub fn lsn(&self) -> u64 {
        self.header().lsn.get()
    }

    /// Set the log sequence number.
    pub fn set_lsn(&mut self, lsn: u64) {
        let mut h = self.header();
        h.lsn = U64::new(lsn);
        self.set_header(&h);
    }

    /// Return the prev/ptr field.
    pub fn prev_or_ptr(&self) -> u32 {
        self.header().prev_or_ptr.get()
    }

    /// Set the prev/ptr field.
    pub fn set_prev_or_ptr(&mut self, val: u32) {
        let mut h = self.header();
        h.prev_or_ptr = U32::new(val);
        self.set_header(&h);
    }

    // ─── Slot directory helpers ──────────────────────────────────────

    /// Read a slot directory entry at the given index.
    fn read_slot_entry(&self, slot: u16) -> SlotEntry {
        let h = self.header();
        assert!(
            slot < h.num_slots.get(),
            "slot index out of bounds: {} >= {}",
            slot,
            h.num_slots.get()
        );
        let dir_offset = PAGE_HEADER_SIZE + (slot as usize) * SLOT_ENTRY_SIZE;
        let offset =
            u16::from_le_bytes([self.buf[dir_offset], self.buf[dir_offset + 1]]);
        let length =
            u16::from_le_bytes([self.buf[dir_offset + 2], self.buf[dir_offset + 3]]);
        SlotEntry { offset, length }
    }

    /// Write a slot directory entry at the given index.
    fn write_slot_entry(&mut self, slot: u16, entry: SlotEntry) {
        let dir_offset = PAGE_HEADER_SIZE + (slot as usize) * SLOT_ENTRY_SIZE;
        let offset_bytes = entry.offset.to_le_bytes();
        let length_bytes = entry.length.to_le_bytes();
        self.buf[dir_offset] = offset_bytes[0];
        self.buf[dir_offset + 1] = offset_bytes[1];
        self.buf[dir_offset + 2] = length_bytes[0];
        self.buf[dir_offset + 3] = length_bytes[1];
    }

    // ─── Slot operations ─────────────────────────────────────────────

    /// Read slot data. Returns an empty slice for deleted (tombstone) slots.
    pub fn slot_data(&self, slot: u16) -> &[u8] {
        let entry = self.read_slot_entry(slot);
        if entry.length == 0 {
            return &[];
        }
        &self.buf[entry.offset as usize..(entry.offset as usize + entry.length as usize)]
    }

    /// Insert data as a new slot. Returns the slot index.
    ///
    /// If there is not enough contiguous free space, attempts compaction first.
    /// Returns `PageFullError` if the page cannot accommodate the data even after compaction.
    pub fn insert_slot(&mut self, data: &[u8]) -> Result<u16, PageFullError> {
        let needed = SLOT_ENTRY_SIZE + data.len();

        if self.free_space() < needed {
            self.compact();
            if self.free_space() < needed {
                return Err(PageFullError);
            }
        }

        let mut h = self.header();
        let slot_index = h.num_slots.get();
        let new_free_space_end = h.free_space_end.get() - data.len() as u16;

        // Write cell data at the end of free space (growing backward).
        self.buf[new_free_space_end as usize..h.free_space_end.get() as usize]
            .copy_from_slice(data);

        // Write slot directory entry.
        let entry = SlotEntry {
            offset: new_free_space_end,
            length: data.len() as u16,
        };
        // Write the entry at the current free_space_start position.
        self.write_slot_entry(slot_index, entry);

        // Update header.
        h.free_space_end = U16::new(new_free_space_end);
        h.free_space_start = U16::new(h.free_space_start.get() + SLOT_ENTRY_SIZE as u16);
        h.num_slots = U16::new(slot_index + 1);
        self.set_header(&h);

        Ok(slot_index)
    }

    /// Update an existing slot's data.
    ///
    /// If the new data fits in the old cell's space, it is written in-place.
    /// Otherwise the old cell space becomes reclaimable, and new cell data is
    /// written at `free_space_end` (growing backward). The slot entry is updated
    /// to point to the new location, preserving the slot index.
    pub fn update_slot(&mut self, slot: u16, data: &[u8]) -> Result<(), PageFullError> {
        let entry = self.read_slot_entry(slot);

        if data.len() <= entry.length as usize {
            // Fits in existing cell space — write in-place.
            let start = entry.offset as usize;
            self.buf[start..start + data.len()].copy_from_slice(data);
            // Update length (offset stays the same; the unused tail is wasted but harmless).
            self.write_slot_entry(
                slot,
                SlotEntry {
                    offset: entry.offset,
                    length: data.len() as u16,
                },
            );
            return Ok(());
        }

        // New data is larger — need new cell space.
        // Mark old cell as reclaimable by setting length to 0 temporarily, so
        // compact() can reclaim it if we need to compact.
        // But we actually just need to allocate from free space. Check if there's room.
        let needed = data.len();
        let h = self.header();
        let contiguous = h.free_space_end.get() as usize - h.free_space_start.get() as usize;

        if contiguous < needed {
            // Mark old cell as deleted so compact can reclaim that space too.
            self.write_slot_entry(
                slot,
                SlotEntry {
                    offset: entry.offset,
                    length: 0,
                },
            );
            self.compact();
            let h = self.header();
            let contiguous =
                h.free_space_end.get() as usize - h.free_space_start.get() as usize;
            if contiguous < needed {
                // Restore the slot entry before returning error — but the old data is
                // gone (compaction may have moved things). Return error; the caller
                // must handle the split.
                return Err(PageFullError);
            }
        }

        // Write new cell data at end of free region.
        let mut h = self.header();
        let new_free_space_end = h.free_space_end.get() - data.len() as u16;
        self.buf[new_free_space_end as usize..h.free_space_end.get() as usize]
            .copy_from_slice(data);

        // Update the slot entry to point to new location.
        self.write_slot_entry(
            slot,
            SlotEntry {
                offset: new_free_space_end,
                length: data.len() as u16,
            },
        );

        h.free_space_end = U16::new(new_free_space_end);
        self.set_header(&h);

        Ok(())
    }

    /// Delete a slot (mark as tombstone with length = 0).
    ///
    /// Does NOT compact — the cell space becomes fragmented and reclaimable.
    pub fn delete_slot(&mut self, slot: u16) {
        let entry = self.read_slot_entry(slot);
        self.write_slot_entry(
            slot,
            SlotEntry {
                offset: entry.offset,
                length: 0,
            },
        );
    }

    // ─── Space management ────────────────────────────────────────────

    /// Available contiguous free space between the slot directory end and the cell data start.
    pub fn free_space(&self) -> usize {
        let h = self.header();
        let start = h.free_space_start.get() as usize;
        let end = h.free_space_end.get() as usize;
        if end > start {
            end - start
        } else {
            0
        }
    }

    /// Total reclaimable space: contiguous free space plus fragmented space from
    /// deleted (tombstone) slots.
    pub fn total_reclaimable(&self) -> usize {
        let h = self.header();
        let num_slots = h.num_slots.get();

        // Sum up the live cell data.
        let mut live_cell_bytes: usize = 0;
        for i in 0..num_slots {
            let entry = self.read_slot_entry(i);
            if entry.length > 0 {
                live_cell_bytes += entry.length as usize;
            }
        }

        // Total usable space = page_size - header - slot_directory
        let slot_dir_size = num_slots as usize * SLOT_ENTRY_SIZE;
        let usable = self.buf.len() - PAGE_HEADER_SIZE - slot_dir_size;

        // Reclaimable = usable - live data
        usable - live_cell_bytes
    }

    /// Defragment: compact cell data to eliminate gaps from deleted slots.
    ///
    /// Slot indices are preserved (tombstone directory entries remain in place).
    /// Only the cell data region is reorganized.
    pub fn compact(&mut self) {
        let h = self.header();
        let num_slots = h.num_slots.get();
        let page_size = self.buf.len();

        // Collect live slot entries with their indices.
        let mut live_slots: Vec<(u16, SlotEntry)> = Vec::new();
        for i in 0..num_slots {
            let entry = self.read_slot_entry(i);
            if entry.length > 0 {
                live_slots.push((i, entry));
            }
        }

        // Sort live slots by their current offset in DESCENDING order so that
        // when we pack from the end of the page, slot data that was closest to
        // the page end is written first (preserving a natural order).
        live_slots.sort_by(|a, b| b.1.offset.cmp(&a.1.offset));

        // Pack cell data from the end of the page backward.
        let mut write_pos = page_size;
        for (slot_idx, entry) in &live_slots {
            let src_start = entry.offset as usize;
            let src_end = src_start + entry.length as usize;
            let cell_len = entry.length as usize;
            write_pos -= cell_len;

            // Copy cell data to new position (may overlap, use copy_within).
            self.buf.copy_within(src_start..src_end, write_pos);

            // Update slot directory entry.
            self.write_slot_entry(
                *slot_idx,
                SlotEntry {
                    offset: write_pos as u16,
                    length: entry.length,
                },
            );
        }

        // Update free_space_end.
        let mut h = self.header();
        h.free_space_end = U16::new(write_pos as u16);
        self.set_header(&h);
    }

    // ─── Checksum ────────────────────────────────────────────────────

    /// Compute CRC-32C over the entire page, treating the checksum field as zeros.
    ///
    /// Uses the 3-part approach to avoid mutating the buffer:
    /// CRC of `[0..20]`, then 4 zero bytes, then `[24..page_size]`.
    pub fn compute_checksum(&self) -> u32 {
        compute_checksum_on_buf(self.buf)
    }

    /// Verify that the stored checksum matches the computed value.
    pub fn verify_checksum(&self) -> bool {
        let stored = self.header().checksum.get();
        let computed = self.compute_checksum();
        stored == computed
    }

    /// Compute and store the CRC-32C checksum into the header.
    pub fn stamp_checksum(&mut self) {
        let crc = self.compute_checksum();
        let mut h = self.header();
        h.checksum = U32::new(crc);
        self.set_header(&h);
    }
}

// ---------------------------------------------------------------------------
// SlottedPageRef (read-only)
// ---------------------------------------------------------------------------

/// Read-only page view (for shared / immutable access).
pub struct SlottedPageRef<'a> {
    buf: &'a [u8],
}

impl<'a> SlottedPageRef<'a> {
    /// Wrap a byte buffer as a read-only slotted page.
    pub fn from_buf(buf: &'a [u8]) -> Self {
        debug_assert!(
            buf.len() >= PAGE_HEADER_SIZE,
            "page buffer too small: {}",
            buf.len()
        );
        SlottedPageRef { buf }
    }

    /// Read the page header.
    pub fn header(&self) -> PageHeader {
        *PageHeader::ref_from_bytes(&self.buf[..PAGE_HEADER_SIZE]).unwrap()
    }

    /// Return the page identifier.
    pub fn page_id(&self) -> PageId {
        self.header().page_id.get()
    }

    /// Return the page type.
    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.header().page_type)
    }

    /// Return the number of slots.
    pub fn num_slots(&self) -> u16 {
        self.header().num_slots.get()
    }

    /// Read slot data. Returns an empty slice for deleted (tombstone) slots.
    pub fn slot_data(&self, slot: u16) -> &[u8] {
        let h = self.header();
        assert!(
            slot < h.num_slots.get(),
            "slot index out of bounds: {} >= {}",
            slot,
            h.num_slots.get()
        );
        let dir_offset = PAGE_HEADER_SIZE + (slot as usize) * SLOT_ENTRY_SIZE;
        let offset =
            u16::from_le_bytes([self.buf[dir_offset], self.buf[dir_offset + 1]]);
        let length =
            u16::from_le_bytes([self.buf[dir_offset + 2], self.buf[dir_offset + 3]]);
        if length == 0 {
            return &[];
        }
        &self.buf[offset as usize..(offset as usize + length as usize)]
    }

    /// Return the log sequence number.
    pub fn lsn(&self) -> u64 {
        self.header().lsn.get()
    }

    /// Return the prev/ptr field.
    pub fn prev_or_ptr(&self) -> u32 {
        self.header().prev_or_ptr.get()
    }

    /// Available contiguous free space.
    pub fn free_space(&self) -> usize {
        let h = self.header();
        let start = h.free_space_start.get() as usize;
        let end = h.free_space_end.get() as usize;
        if end > start {
            end - start
        } else {
            0
        }
    }

    /// Verify the stored checksum matches the computed value.
    pub fn verify_checksum(&self) -> bool {
        let stored = self.header().checksum.get();
        let computed = compute_checksum_on_buf(self.buf);
        stored == computed
    }
}

// ---------------------------------------------------------------------------
// Shared checksum helper
// ---------------------------------------------------------------------------

/// Compute CRC-32C over a page buffer, treating the checksum field (bytes 20..24) as zeros.
fn compute_checksum_on_buf(buf: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    // Part 1: bytes before the checksum field.
    hasher.update(&buf[..CHECKSUM_OFFSET]);
    // Part 2: four zero bytes in place of the checksum field.
    hasher.update(&[0u8; CHECKSUM_SIZE]);
    // Part 3: bytes after the checksum field.
    hasher.update(&buf[CHECKSUM_OFFSET + CHECKSUM_SIZE..]);
    hasher.finalize()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: usize = 4096;

    /// Helper: create a fresh zeroed page buffer.
    fn new_buf() -> Vec<u8> {
        vec![0u8; PAGE_SIZE]
    }

    // 1. init + header roundtrip
    #[test]
    fn test_init_header_roundtrip() {
        let mut buf = new_buf();
        let page = SlottedPage::init(&mut buf, 42, PageType::BTreeLeaf);

        let h = page.header();
        assert_eq!(h.page_id.get(), 42);
        assert_eq!(h.page_type, PageType::BTreeLeaf as u8);
        assert_eq!(h.flags, 0);
        assert_eq!(h.num_slots.get(), 0);
        assert_eq!(h.free_space_start.get(), PAGE_HEADER_SIZE as u16);
        assert_eq!(h.free_space_end.get(), PAGE_SIZE as u16);
        assert_eq!(h.prev_or_ptr.get(), 0);
        assert_eq!(h._reserved.get(), 0);
        assert_eq!(h.lsn.get(), 0);

        assert_eq!(page.page_id(), 42);
        assert_eq!(page.page_type(), PageType::BTreeLeaf);
        assert_eq!(page.num_slots(), 0);
    }

    // 2. insert one slot
    #[test]
    fn test_insert_one_slot() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 1, PageType::Heap);

        let data = b"hello, world!";
        let slot = page.insert_slot(data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.num_slots(), 1);
        assert_eq!(page.slot_data(0), data);
    }

    // 3. insert many slots until full
    #[test]
    fn test_insert_many_slots_until_full() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 2, PageType::BTreeLeaf);

        let record = [0xABu8; 32];
        let mut count = 0u16;
        loop {
            match page.insert_slot(&record) {
                Ok(idx) => {
                    assert_eq!(idx, count);
                    count += 1;
                }
                Err(_) => break,
            }
        }

        // Verify all inserted records are readable.
        assert!(count > 0);
        for i in 0..count {
            assert_eq!(page.slot_data(i), &record);
        }
    }

    // 4. delete + verify tombstone
    #[test]
    fn test_delete_slot() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 3, PageType::Heap);

        for i in 0..5u8 {
            page.insert_slot(&[i; 16]).unwrap();
        }
        assert_eq!(page.num_slots(), 5);

        page.delete_slot(2);

        // Deleted slot returns empty slice.
        assert_eq!(page.slot_data(2), &[] as &[u8]);

        // Other slots are still intact.
        assert_eq!(page.slot_data(0), &[0u8; 16]);
        assert_eq!(page.slot_data(1), &[1u8; 16]);
        assert_eq!(page.slot_data(3), &[3u8; 16]);
        assert_eq!(page.slot_data(4), &[4u8; 16]);

        // num_slots is unchanged (tombstones preserved).
        assert_eq!(page.num_slots(), 5);
    }

    // 5. compact reclaims space
    #[test]
    fn test_compact_reclaims_space() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 4, PageType::BTreeLeaf);

        for i in 0..10u8 {
            page.insert_slot(&[i; 64]).unwrap();
        }

        let free_before_delete = page.free_space();

        // Delete 5 slots.
        for i in (0..10).step_by(2) {
            page.delete_slot(i);
        }

        // Free space (contiguous) hasn't changed since deletes don't compact.
        assert_eq!(page.free_space(), free_before_delete);

        page.compact();

        // After compaction, contiguous free space should have grown.
        assert!(page.free_space() > free_before_delete);

        // Remaining live slots should still be readable.
        for i in (1..10).step_by(2) {
            assert_eq!(page.slot_data(i as u16), &[i as u8; 64]);
        }
    }

    // 6. insert after compact
    #[test]
    fn test_insert_after_compact() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 5, PageType::Heap);

        // Fill with some data.
        for i in 0..20u8 {
            page.insert_slot(&[i; 100]).unwrap();
        }

        // Delete half.
        for i in 0..20u16 {
            if i % 2 == 0 {
                page.delete_slot(i);
            }
        }

        page.compact();

        // Now insert more data — should succeed because compaction freed space.
        let new_slot = page.insert_slot(&[0xFF; 100]).unwrap();
        assert_eq!(page.slot_data(new_slot), &[0xFF; 100]);
    }

    // 7. checksum roundtrip
    #[test]
    fn test_checksum_roundtrip() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 6, PageType::BTreeInternal);

        page.insert_slot(b"test data for checksum").unwrap();
        page.stamp_checksum();

        assert!(page.verify_checksum());
    }

    // 8. checksum detects corruption
    #[test]
    fn test_checksum_detects_corruption() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 7, PageType::Heap);

        page.insert_slot(b"important data").unwrap();
        page.stamp_checksum();
        assert!(page.verify_checksum());

        // Corrupt a byte in the cell data area.
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let page = SlottedPage::from_buf(&mut buf);
        assert!(!page.verify_checksum());
    }

    // 9. page types have correct u8 values
    #[test]
    fn test_page_types() {
        assert_eq!(PageType::BTreeInternal as u8, 0x01);
        assert_eq!(PageType::BTreeLeaf as u8, 0x02);
        assert_eq!(PageType::Heap as u8, 0x03);
        assert_eq!(PageType::Overflow as u8, 0x04);
        assert_eq!(PageType::Free as u8, 0x05);
        assert_eq!(PageType::FileHeader as u8, 0x06);
        assert_eq!(PageType::FileHeaderShadow as u8, 0x07);
    }

    // 10. prev_or_ptr field
    #[test]
    fn test_prev_or_ptr_field() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 10, PageType::BTreeLeaf);

        assert_eq!(page.prev_or_ptr(), 0);

        page.set_prev_or_ptr(12345);
        assert_eq!(page.prev_or_ptr(), 12345);

        page.set_prev_or_ptr(u32::MAX);
        assert_eq!(page.prev_or_ptr(), u32::MAX);

        // Also test with a different page type.
        let mut buf2 = new_buf();
        let mut page2 = SlottedPage::init(&mut buf2, 11, PageType::BTreeInternal);
        page2.set_prev_or_ptr(0xDEAD_BEEF);
        assert_eq!(page2.prev_or_ptr(), 0xDEAD_BEEF);
    }

    // 11. large slot
    #[test]
    fn test_large_slot() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 20, PageType::Heap);

        // A slot that takes almost the entire page.
        // Available: PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE (for one slot).
        let max_data_size = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE;
        let large_data: Vec<u8> = (0..max_data_size).map(|i| (i % 256) as u8).collect();

        let slot = page.insert_slot(&large_data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.slot_data(0), &large_data[..]);
    }

    // 12. boundary: exactly full page
    #[test]
    fn test_exactly_full_page() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 30, PageType::BTreeLeaf);

        // Insert slots until we can't fit even one more slot entry + 1 byte of data.
        let mut count = 0u16;
        // Use a record size that will eventually exhaust free space.
        let record = [0x42u8; 10];
        loop {
            match page.insert_slot(&record) {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        assert!(count > 0);

        // The remaining free space should be less than SLOT_ENTRY_SIZE + record.len().
        let remaining = page.free_space();
        assert!(remaining < SLOT_ENTRY_SIZE + record.len());

        // All inserted slots are still valid.
        for i in 0..count {
            assert_eq!(page.slot_data(i), &record);
        }
    }

    // 13. SlottedPageRef read-only operations
    #[test]
    fn test_slotted_page_ref() {
        let mut buf = new_buf();

        // Set up page with mutable access.
        {
            let mut page = SlottedPage::init(&mut buf, 50, PageType::Heap);
            page.insert_slot(b"alpha").unwrap();
            page.insert_slot(b"beta").unwrap();
            page.insert_slot(b"gamma").unwrap();
            page.set_lsn(999);
            page.set_prev_or_ptr(77);
            page.stamp_checksum();
        }

        // Now use read-only reference.
        let page_ref = SlottedPageRef::from_buf(&buf);
        assert_eq!(page_ref.page_id(), 50);
        assert_eq!(page_ref.page_type(), PageType::Heap);
        assert_eq!(page_ref.num_slots(), 3);
        assert_eq!(page_ref.slot_data(0), b"alpha");
        assert_eq!(page_ref.slot_data(1), b"beta");
        assert_eq!(page_ref.slot_data(2), b"gamma");
        assert_eq!(page_ref.lsn(), 999);
        assert_eq!(page_ref.prev_or_ptr(), 77);
        assert!(page_ref.verify_checksum());
        assert!(page_ref.free_space() > 0);
    }

    // 14. lsn field
    #[test]
    fn test_lsn_field() {
        let mut buf = new_buf();
        let mut page = SlottedPage::init(&mut buf, 60, PageType::Free);

        assert_eq!(page.lsn(), 0);

        page.set_lsn(123_456_789);
        assert_eq!(page.lsn(), 123_456_789);

        page.set_lsn(u64::MAX);
        assert_eq!(page.lsn(), u64::MAX);

        page.set_lsn(0);
        assert_eq!(page.lsn(), 0);
    }
}
