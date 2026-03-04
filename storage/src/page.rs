//! Slotted page format.
//!
//! Every page in the data file uses the same 32-byte header followed by a
//! slot directory and cell data region. The slot directory grows forward from
//! the header; cells are allocated backward from the end of the page.
//!
//! ```text
//! ┌──────────────┬───────────────┬───────────────┬──────────────┐
//! │ Header (32B) │ Slot dir →    │  Free space   │  ← Cell data │
//! └──────────────┴───────────────┴───────────────┴──────────────┘
//! ```
//!
//! Each slot entry is 4 bytes: `(offset: u16, length: u16)`.
//! CRC-32C checksums cover the entire page (except the checksum field itself).

use crate::types::*;

/// In-memory representation of the 32-byte page header.
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub flags: u8,
    pub num_slots: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub prev_or_ptr: u32,
    pub _reserved: u32,
    pub checksum: u32,
    pub lsn: Lsn,
}

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

/// Zero-copy read-only view over a page buffer.
pub struct SlottedPage<'a> {
    data: &'a [u8],
    #[allow(dead_code)]
    page_size: u32,
}

/// Mutable view over a page buffer.
pub struct SlottedPageMut<'a> {
    data: &'a mut [u8],
    page_size: u32,
}

fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

fn write_u16_le(buf: &mut [u8], offset: usize, val: u16) {
    buf[offset..offset + 2].copy_from_slice(&val.to_le_bytes());
}

fn write_u32_le(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

fn write_u64_le(buf: &mut [u8], offset: usize, val: u64) {
    buf[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

fn parse_header(data: &[u8]) -> PageHeader {
    PageHeader {
        page_id: PageId(read_u32_le(data, 0)),
        page_type: PageType::from_u8(data[4]).unwrap_or(PageType::Free),
        flags: data[5],
        num_slots: read_u16_le(data, 6),
        free_space_start: read_u16_le(data, 8),
        free_space_end: read_u16_le(data, 10),
        prev_or_ptr: read_u32_le(data, 12),
        _reserved: read_u32_le(data, 16),
        checksum: read_u32_le(data, 20),
        lsn: Lsn(read_u64_le(data, 24)),
    }
}

fn write_header(data: &mut [u8], header: &PageHeader) {
    write_u32_le(data, 0, header.page_id.0);
    data[4] = header.page_type as u8;
    data[5] = header.flags;
    write_u16_le(data, 6, header.num_slots);
    write_u16_le(data, 8, header.free_space_start);
    write_u16_le(data, 10, header.free_space_end);
    write_u32_le(data, 12, header.prev_or_ptr);
    write_u32_le(data, 16, header._reserved);
    write_u32_le(data, 20, header.checksum);
    write_u64_le(data, 24, header.lsn.0);
}

/// Compute CRC-32C over the entire page except bytes 20..24 (checksum field).
fn compute_crc(data: &[u8]) -> u32 {
    let mut digest = 0u32;
    digest = crc32c::crc32c_append(digest, &data[..20]);
    digest = crc32c::crc32c_append(digest, &data[24..]);
    digest
}

/// Slot directory entry offset within the page.
fn slot_dir_offset(slot_idx: u16) -> usize {
    PAGE_HEADER_SIZE + (slot_idx as usize) * SLOT_ENTRY_SIZE
}

impl<'a> SlottedPage<'a> {
    pub fn new(data: &'a [u8], page_size: u32) -> Self {
        debug_assert_eq!(data.len(), page_size as usize);
        Self { data, page_size }
    }

    pub fn header(&self) -> PageHeader {
        parse_header(self.data)
    }

    pub fn num_slots(&self) -> u16 {
        read_u16_le(self.data, 6)
    }

    /// Read slot directory entry: (offset, length).
    pub fn slot(&self, slot_idx: u16) -> Option<(u16, u16)> {
        if slot_idx >= self.num_slots() {
            return None;
        }
        let dir_off = slot_dir_offset(slot_idx);
        Some((read_u16_le(self.data, dir_off), read_u16_le(self.data, dir_off + 2)))
    }

    /// Read cell data for a given slot.
    pub fn cell_data(&self, slot_idx: u16) -> Option<&[u8]> {
        let (offset, length) = self.slot(slot_idx)?;
        Some(&self.data[offset as usize..(offset + length) as usize])
    }

    /// Free space available for new cells (including slot entry overhead).
    pub fn free_space(&self) -> usize {
        let fs_start = read_u16_le(self.data, 8) as usize;
        let fs_end = read_u16_le(self.data, 10) as usize;
        fs_end.saturating_sub(fs_start)
    }

    /// Verify page checksum.
    pub fn verify_checksum(&self) -> bool {
        let stored = read_u32_le(self.data, 20);
        let computed = compute_crc(self.data);
        stored == computed
    }

    /// Iterate all cells: yields (slot_idx, cell_data) pairs.
    pub fn cells(&self) -> impl Iterator<Item = (u16, &[u8])> {
        let n = self.num_slots();
        (0..n).filter_map(move |i| self.cell_data(i).map(|d| (i, d)))
    }
}

impl<'a> SlottedPageMut<'a> {
    pub fn new(data: &'a mut [u8], page_size: u32) -> Self {
        debug_assert_eq!(data.len(), page_size as usize);
        Self { data, page_size }
    }

    /// Initialize a fresh page.
    pub fn init(&mut self, page_id: PageId, page_type: PageType) {
        self.data.fill(0);
        let header = PageHeader {
            page_id,
            page_type,
            flags: 0,
            num_slots: 0,
            free_space_start: PAGE_HEADER_SIZE as u16,
            free_space_end: self.page_size as u16,
            prev_or_ptr: 0,
            _reserved: 0,
            checksum: 0,
            lsn: Lsn::ZERO,
        };
        write_header(self.data, &header);
    }

    /// Insert a cell at the given slot position.
    pub fn insert_cell(&mut self, slot_idx: u16, cell: &[u8]) -> Result<(), PageError> {
        let num_slots = self.num_slots();
        let needed = cell.len() + SLOT_ENTRY_SIZE;
        let available = self.free_space();
        if needed > available {
            return Err(PageError::PageFull { needed, available });
        }

        let fs_end = read_u16_le(self.data, 10);
        let cell_offset = fs_end as usize - cell.len();

        // Write cell data at the end of the free space region (grows backward).
        self.data[cell_offset..cell_offset + cell.len()].copy_from_slice(cell);

        // Shift slot entries forward to make room at slot_idx.
        if slot_idx < num_slots {
            let src_start = slot_dir_offset(slot_idx);
            let src_end = slot_dir_offset(num_slots);
            self.data.copy_within(src_start..src_end, src_start + SLOT_ENTRY_SIZE);
        }

        // Write new slot entry.
        let dir_off = slot_dir_offset(slot_idx);
        write_u16_le(self.data, dir_off, cell_offset as u16);
        write_u16_le(self.data, dir_off + 2, cell.len() as u16);

        // Update header: num_slots, free_space_start, free_space_end.
        let new_num_slots = num_slots + 1;
        let new_fs_start = PAGE_HEADER_SIZE as u16 + new_num_slots * SLOT_ENTRY_SIZE as u16;
        let new_fs_end = cell_offset as u16;
        write_u16_le(self.data, 6, new_num_slots);
        write_u16_le(self.data, 8, new_fs_start);
        write_u16_le(self.data, 10, new_fs_end);

        Ok(())
    }

    /// Replace cell data at an existing slot.
    pub fn update_cell(&mut self, slot_idx: u16, cell: &[u8]) -> Result<(), PageError> {
        let num_slots = self.num_slots();
        if slot_idx >= num_slots {
            return Err(PageError::InvalidSlot(slot_idx));
        }
        let (old_offset, old_len) = (
            read_u16_le(self.data, slot_dir_offset(slot_idx)),
            read_u16_le(self.data, slot_dir_offset(slot_idx) + 2),
        );

        if cell.len() == old_len as usize {
            // In-place update.
            self.data[old_offset as usize..(old_offset + old_len) as usize]
                .copy_from_slice(cell);
            return Ok(());
        }

        // Delete then re-insert.
        self.delete_cell(slot_idx);
        self.insert_cell(slot_idx, cell)
    }

    /// Delete a cell at the given slot position.
    pub fn delete_cell(&mut self, slot_idx: u16) {
        let num_slots = self.num_slots();
        if slot_idx >= num_slots {
            return;
        }

        // Shift subsequent slot entries backward.
        if slot_idx + 1 < num_slots {
            let src_start = slot_dir_offset(slot_idx + 1);
            let src_end = slot_dir_offset(num_slots);
            let dst = slot_dir_offset(slot_idx);
            self.data.copy_within(src_start..src_end, dst);
        }

        let new_num_slots = num_slots - 1;
        let new_fs_start = PAGE_HEADER_SIZE as u16 + new_num_slots * SLOT_ENTRY_SIZE as u16;
        write_u16_le(self.data, 6, new_num_slots);
        write_u16_le(self.data, 8, new_fs_start);
        // Note: free_space_end not adjusted — space is fragmented.
        // Call compact() to reclaim.
    }

    /// Compact free space: defragment cells so all free space is contiguous.
    pub fn compact(&mut self) {
        let num_slots = self.num_slots();
        if num_slots == 0 {
            write_u16_le(self.data, 10, self.page_size as u16);
            return;
        }

        // Collect all cells.
        let mut cells: Vec<(u16, Vec<u8>)> = Vec::with_capacity(num_slots as usize);
        for i in 0..num_slots {
            let dir_off = slot_dir_offset(i);
            let offset = read_u16_le(self.data, dir_off);
            let length = read_u16_le(self.data, dir_off + 2);
            let cell = self.data[offset as usize..(offset + length) as usize].to_vec();
            cells.push((i, cell));
        }

        // Rewrite cells from the end of the page.
        let mut write_pos = self.page_size as usize;
        for (slot_idx, cell_data) in &cells {
            write_pos -= cell_data.len();
            self.data[write_pos..write_pos + cell_data.len()].copy_from_slice(cell_data);
            let dir_off = slot_dir_offset(*slot_idx);
            write_u16_le(self.data, dir_off, write_pos as u16);
            write_u16_le(self.data, dir_off + 2, cell_data.len() as u16);
        }

        let new_fs_start = PAGE_HEADER_SIZE as u16 + num_slots * SLOT_ENTRY_SIZE as u16;
        write_u16_le(self.data, 8, new_fs_start);
        write_u16_le(self.data, 10, write_pos as u16);
    }

    pub fn set_prev_or_ptr(&mut self, value: u32) {
        write_u32_le(self.data, 12, value);
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        write_u64_le(self.data, 24, lsn.0);
    }

    /// Compute and write CRC-32C checksum into the header.
    pub fn compute_checksum(&mut self) {
        // Zero checksum field before computing.
        write_u32_le(self.data, 20, 0);
        let crc = compute_crc(self.data);
        write_u32_le(self.data, 20, crc);
    }

    // Immutable accessors (delegate to SlottedPage).
    pub fn header(&self) -> PageHeader {
        parse_header(self.data)
    }

    pub fn num_slots(&self) -> u16 {
        read_u16_le(self.data, 6)
    }

    pub fn slot(&self, slot_idx: u16) -> Option<(u16, u16)> {
        if slot_idx >= self.num_slots() {
            return None;
        }
        let dir_off = slot_dir_offset(slot_idx);
        Some((read_u16_le(self.data, dir_off), read_u16_le(self.data, dir_off + 2)))
    }

    pub fn cell_data(&self, slot_idx: u16) -> Option<&[u8]> {
        let (offset, length) = self.slot(slot_idx)?;
        Some(&self.data[offset as usize..(offset + length) as usize])
    }

    pub fn free_space(&self) -> usize {
        let fs_start = read_u16_le(self.data, 8) as usize;
        let fs_end = read_u16_le(self.data, 10) as usize;
        fs_end.saturating_sub(fs_start)
    }

    pub fn data_ref(&self) -> &[u8] {
        self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PS: u32 = 8192;

    fn make_page() -> Vec<u8> {
        vec![0u8; PS as usize]
    }

    #[test]
    fn test_init_and_header() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        let h = p.header();
        assert_eq!(h.page_id, PageId(1));
        assert_eq!(h.page_type, PageType::BTreeLeaf);
        assert_eq!(h.num_slots, 0);
        assert_eq!(p.free_space(), PS as usize - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_insert_single_cell() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"hello").unwrap();
        assert_eq!(p.num_slots(), 1);
        assert_eq!(p.cell_data(0).unwrap(), b"hello");
    }

    #[test]
    fn test_insert_multiple_cells() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"aaa").unwrap();
        p.insert_cell(1, b"bbb").unwrap();
        p.insert_cell(2, b"ccc").unwrap();
        assert_eq!(p.cell_data(0).unwrap(), b"aaa");
        assert_eq!(p.cell_data(1).unwrap(), b"bbb");
        assert_eq!(p.cell_data(2).unwrap(), b"ccc");
    }

    #[test]
    fn test_insert_at_middle() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"aaa").unwrap();
        p.insert_cell(1, b"ccc").unwrap();
        p.insert_cell(1, b"bbb").unwrap();
        assert_eq!(p.cell_data(0).unwrap(), b"aaa");
        assert_eq!(p.cell_data(1).unwrap(), b"bbb");
        assert_eq!(p.cell_data(2).unwrap(), b"ccc");
    }

    #[test]
    fn test_delete_cell() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"aaa").unwrap();
        p.insert_cell(1, b"bbb").unwrap();
        p.insert_cell(2, b"ccc").unwrap();
        p.delete_cell(1);
        assert_eq!(p.num_slots(), 2);
        assert_eq!(p.cell_data(0).unwrap(), b"aaa");
        assert_eq!(p.cell_data(1).unwrap(), b"ccc");
    }

    #[test]
    fn test_page_full() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        let big = vec![0u8; PS as usize]; // larger than page
        let result = p.insert_cell(0, &big);
        assert!(result.is_err());
    }

    #[test]
    fn test_compact() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        // Insert 10 cells.
        for i in 0..10 {
            let data = vec![i as u8; 100];
            p.insert_cell(i, &data).unwrap();
        }
        let space_before = p.free_space();
        // Delete cells 2, 5, 8 (creates gaps).
        p.delete_cell(8);
        p.delete_cell(5);
        p.delete_cell(2);
        // After delete, free_space may not increase (fragmented).
        p.compact();
        // After compact, free space should be larger than before deletes.
        assert!(p.free_space() > space_before);
        // Remaining cells should be correct.
        assert_eq!(p.num_slots(), 7);
    }

    #[test]
    fn test_checksum_roundtrip() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"test data").unwrap();
        p.compute_checksum();
        let ro = SlottedPage::new(&buf, PS);
        assert!(ro.verify_checksum());
    }

    #[test]
    fn test_checksum_corruption() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"test data").unwrap();
        p.compute_checksum();
        // Corrupt a byte.
        buf[100] ^= 0xFF;
        let ro = SlottedPage::new(&buf, PS);
        assert!(!ro.verify_checksum());
    }

    #[test]
    fn test_update_cell_same_size() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"hello").unwrap();
        p.update_cell(0, b"world").unwrap();
        assert_eq!(p.cell_data(0).unwrap(), b"world");
    }

    #[test]
    fn test_update_cell_larger() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.insert_cell(0, b"hi").unwrap();
        p.update_cell(0, b"hello world").unwrap();
        assert_eq!(p.cell_data(0).unwrap(), b"hello world");
    }

    #[test]
    fn test_prev_or_ptr() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.set_prev_or_ptr(42);
        assert_eq!(p.header().prev_or_ptr, 42);
    }

    #[test]
    fn test_lsn() {
        let mut buf = make_page();
        let mut p = SlottedPageMut::new(&mut buf, PS);
        p.init(PageId(1), PageType::BTreeLeaf);
        p.set_lsn(Lsn(12345));
        assert_eq!(p.header().lsn, Lsn(12345));
    }
}
