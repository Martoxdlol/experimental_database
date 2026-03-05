//! B+ tree index operating on raw byte keys and values.
//!
//! Supports insert, get, delete, and range scan with forward/backward iteration.
//! Uses the buffer pool for all page access. Keys are compared with `memcmp` ordering.
//! No knowledge of what keys or values represent.
//!
//! ## Page Layout
//!
//! - **Leaf**: `PageType::BTreeLeaf`, `prev_or_ptr` = right_sibling, cells = `key_len(u16) || key || value`
//! - **Internal**: `PageType::BTreeInternal`, `prev_or_ptr` = leftmost_child, cells = `key_len(u16) || key || child_page_id(u32)`

use crate::backend::PageId;
use crate::buffer_pool::BufferPool;
use crate::free_list::FreeList;
use crate::page::{PageFullError, PageType, SlottedPage, SlottedPageRef};
use std::ops::Bound;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::io;

// ─── Constants ───

/// Size of the page header in bytes.
const PAGE_HEADER_SIZE: usize = 32;

/// Size of a single slot directory entry (offset: u16 + length: u16).
const SLOT_ENTRY_SIZE: usize = 4;

// ─── ScanDirection ───

/// Direction for range scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

// ─── Cell encoding/decoding helpers ───

/// Encode a leaf cell: `key_len(u16 LE) || key || value`.
fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut cell = Vec::with_capacity(2 + key.len() + value.len());
    cell.extend_from_slice(&key_len.to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(value);
    cell
}

/// Decode a leaf cell: returns `(key, value)`.
fn decode_leaf_cell(cell: &[u8]) -> (&[u8], &[u8]) {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    let key = &cell[2..2 + key_len];
    let value = &cell[2 + key_len..];
    (key, value)
}

/// Encode an internal cell: `key_len(u16 LE) || key || child_page_id(u32 LE)`.
fn encode_internal_cell(key: &[u8], child: PageId) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut cell = Vec::with_capacity(2 + key.len() + 4);
    cell.extend_from_slice(&key_len.to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&child.to_le_bytes());
    cell
}

/// Decode an internal cell: returns `(key, child_page_id)`.
fn decode_internal_cell(cell: &[u8]) -> (&[u8], PageId) {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    let key = &cell[2..2 + key_len];
    let child_bytes = &cell[2 + key_len..2 + key_len + 4];
    let child = u32::from_le_bytes([
        child_bytes[0],
        child_bytes[1],
        child_bytes[2],
        child_bytes[3],
    ]);
    (key, child)
}

/// Extract the key from a cell (works for both leaf and internal cells).
fn cell_key(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2..2 + key_len]
}

/// Binary search slots for a key. Returns `(found, index)`.
///
/// If found: slot at `index` contains the key.
/// If not found: `index` is where the key would be inserted.
fn binary_search_slots(page: &SlottedPageRef, key: &[u8], _is_leaf: bool) -> (bool, u16) {
    let num_slots = page.num_slots();
    if num_slots == 0 {
        return (false, 0);
    }

    let mut lo: u16 = 0;
    let mut hi: u16 = num_slots;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cell = page.slot_data(mid);
        let cell_k = cell_key(cell);

        match cell_k.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Equal => return (true, mid),
            std::cmp::Ordering::Greater => hi = mid,
        }
    }

    (false, lo)
}

// ─── Slot directory manipulation helpers ───

/// Read a raw slot directory entry (offset, length) from a page buffer.
fn read_raw_slot_entry(buf: &[u8], slot: u16) -> (u16, u16) {
    let dir_offset = PAGE_HEADER_SIZE + (slot as usize) * SLOT_ENTRY_SIZE;
    let offset = u16::from_le_bytes([buf[dir_offset], buf[dir_offset + 1]]);
    let length = u16::from_le_bytes([buf[dir_offset + 2], buf[dir_offset + 3]]);
    (offset, length)
}

/// Write a raw slot directory entry to a page buffer.
fn write_raw_slot_entry(buf: &mut [u8], slot: u16, offset: u16, length: u16) {
    let dir_offset = PAGE_HEADER_SIZE + (slot as usize) * SLOT_ENTRY_SIZE;
    let offset_bytes = offset.to_le_bytes();
    let length_bytes = length.to_le_bytes();
    buf[dir_offset] = offset_bytes[0];
    buf[dir_offset + 1] = offset_bytes[1];
    buf[dir_offset + 2] = length_bytes[0];
    buf[dir_offset + 3] = length_bytes[1];
}

/// Insert cell data at a sorted position, working directly on the raw page buffer.
///
/// Appends the cell using `SlottedPage`, then shifts directory entries to place
/// the new cell at `target_pos`.
fn insert_cell_at_pos(buf: &mut [u8], data: &[u8], target_pos: u16) -> Result<(), PageFullError> {
    // Buffer from pool is always page_size.
    let mut page = SlottedPage::from_buf(buf).expect("buffer from pool is always page_size");
    let old_num_slots = page.num_slots();

    // Append the cell data (this allocates cell space + directory entry at end).
    let appended_slot = page.insert_slot(data)?;
    debug_assert_eq!(appended_slot, old_num_slots);
    drop(page);

    if target_pos >= old_num_slots {
        // Already at the correct position (end).
        return Ok(());
    }

    // Read the newly appended slot entry.
    let (new_offset, new_length) = read_raw_slot_entry(buf, appended_slot);

    // Shift entries from [target_pos..old_num_slots] to [target_pos+1..old_num_slots+1].
    // Work backwards to avoid overwriting.
    for i in (target_pos..old_num_slots).rev() {
        let (o, l) = read_raw_slot_entry(buf, i);
        write_raw_slot_entry(buf, i + 1, o, l);
    }

    // Write the new entry at target_pos.
    write_raw_slot_entry(buf, target_pos, new_offset, new_length);

    Ok(())
}

/// Delete a slot at a given position and shift subsequent entries backward
/// to maintain a contiguous sorted directory (no tombstones).
fn delete_cell_at_pos(buf: &mut [u8], pos: u16) {
    // Buffer from pool is always page_size.
    let mut page = SlottedPage::from_buf(buf).expect("buffer from pool is always page_size");
    let num_slots = page.num_slots();
    assert!(pos < num_slots);

    // Mark the cell data as deleted (tombstone the slot entry for space reclamation).
    page.delete_slot(pos);
    drop(page);

    // Shift entries from [pos+1..num_slots] backward by one to fill the gap.
    for i in pos..(num_slots - 1) {
        let (o, l) = read_raw_slot_entry(buf, i + 1);
        write_raw_slot_entry(buf, i, o, l);
    }

    // Zero out the last directory entry (now unused).
    write_raw_slot_entry(buf, num_slots - 1, 0, 0);

    // Decrement num_slots and free_space_start in the header.
    // Buffer from pool is always page_size.
    let mut page = SlottedPage::from_buf(buf).expect("buffer from pool is always page_size");
    let mut h = page.header();
    let new_num_slots = h.num_slots.get() - 1;
    h.num_slots = zerocopy::byteorder::U16::<zerocopy::byteorder::LittleEndian>::new(new_num_slots);
    let new_fss = h.free_space_start.get() - SLOT_ENTRY_SIZE as u16;
    h.free_space_start =
        zerocopy::byteorder::U16::<zerocopy::byteorder::LittleEndian>::new(new_fss);
    page.set_header(&h);
}

// ─── BTree ───

/// A B+ tree instance. Manages a tree rooted at a specific page.
pub struct BTree {
    root_page: AtomicU32,
    buffer_pool: Arc<BufferPool>,
}

/// Represents a promoted key and new child after a split.
struct SplitResult {
    /// The median key promoted to the parent.
    median_key: Vec<u8>,
    /// The page ID of the newly allocated sibling.
    new_page_id: PageId,
}

impl BTree {
    /// Create a new B-tree with an empty leaf root page.
    /// Allocates one page from the free list.
    pub fn create(buffer_pool: Arc<BufferPool>, free_list: &mut FreeList) -> io::Result<Self> {
        let page_id = free_list.allocate()?;
        let mut guard = buffer_pool.new_page(page_id)?;
        {
            let buf = guard.data_mut();
            SlottedPage::init(buf, page_id, PageType::BTreeLeaf);
        }
        drop(guard);

        Ok(Self {
            root_page: AtomicU32::new(page_id),
            buffer_pool,
        })
    }

    /// Open an existing B-tree rooted at `root_page`.
    pub fn open(root_page: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            root_page: AtomicU32::new(root_page),
            buffer_pool,
        }
    }

    /// Root page ID (may change after splits).
    pub fn root_page(&self) -> PageId {
        self.root_page.load(Ordering::Acquire)
    }

    /// Point lookup. Returns value bytes if found, None if not.
    ///
    /// Uses latch coupling: acquire child shared, release parent shared.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id)?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            let page_type = page.page_type_checked()?;

            match page_type {
                PageType::BTreeLeaf => {
                    let (found, idx) = binary_search_slots(&page, key, true);
                    if found {
                        let cell = page.slot_data(idx);
                        let (_k, v) = decode_leaf_cell(cell);
                        return Ok(Some(v.to_vec()));
                    }
                    return Ok(None);
                }
                PageType::BTreeInternal => {
                    let child = find_child_in_internal(&page, key);
                    // Release parent before fetching child (latch coupling).
                    drop(page);
                    drop(guard);
                    current_page_id = child;
                }
                _ => {
                    return Err(crate::error::StorageError::Corruption(
                        format!(
                            "unexpected page type {:?} during B-tree traversal",
                            page_type
                        ),
                    )
                    .into());
                }
            }
        }
    }

    /// Insert or update a key-value pair.
    ///
    /// If key exists, the value is replaced. Handles leaf and internal splits,
    /// including root splits.
    pub fn insert(
        &self,
        key: &[u8],
        value: &[u8],
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        // Collect the path from root to leaf.
        let mut path: Vec<PageId> = Vec::new();
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id)?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            let page_type = page.page_type_checked()?;

            match page_type {
                PageType::BTreeLeaf => {
                    drop(page);
                    drop(guard);
                    break;
                }
                PageType::BTreeInternal => {
                    path.push(current_page_id);
                    let child = find_child_in_internal(&page, key);
                    drop(page);
                    drop(guard);
                    current_page_id = child;
                }
                _ => {
                    return Err(crate::error::StorageError::Corruption(
                        format!(
                            "unexpected page type {:?} during B-tree insert traversal",
                            page_type
                        ),
                    )
                    .into());
                }
            }
        }

        // Now `current_page_id` is the leaf. Acquire exclusive lock.
        let mut guard = self.buffer_pool.fetch_page_exclusive(current_page_id)?;
        let leaf_page_id = guard.page_id();

        // Check if key already exists for update.
        {
            let page = SlottedPageRef::from_buf(guard.data())?;
            let (found, idx) = binary_search_slots(&page, key, true);
            drop(page);

            if found {
                // Update existing key: replace the cell.
                let new_cell = encode_leaf_cell(key, value);
                let buf = guard.data_mut();
                let mut page = SlottedPage::from_buf(buf)?;
                match page.update_slot(idx, &new_cell) {
                    Ok(()) => {
                        return Ok(());
                    }
                    Err(_) => {
                        // Page full even for update -- delete old entry and re-insert.
                        drop(page);
                        let buf = guard.data_mut();
                        delete_cell_at_pos(buf, idx);
                    }
                }
            }
        }

        // Try to insert into the leaf.
        let cell = encode_leaf_cell(key, value);

        {
            let page = SlottedPageRef::from_buf(guard.data())?;
            let (_found, insert_pos) = binary_search_slots(&page, key, true);
            drop(page);

            let buf = guard.data_mut();
            match insert_cell_at_pos(buf, &cell, insert_pos) {
                Ok(()) => return Ok(()),
                Err(_) => {
                    // Leaf is full, need to split.
                }
            }
        }

        // -- Leaf split --
        let split = self.split_leaf(&mut guard, key, value, free_list)?;
        drop(guard);

        // Propagate the split up through parents.
        self.propagate_split(split, &mut path, leaf_page_id, free_list)?;

        Ok(())
    }

    /// Delete a key. Returns true if the key was found and deleted.
    ///
    /// Simple delete: just removes from the leaf (no merge/redistribute for v1).
    pub fn delete(&self, key: &[u8], _free_list: &mut FreeList) -> io::Result<bool> {
        // Traverse to the leaf.
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id)?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            let page_type = page.page_type_checked()?;

            match page_type {
                PageType::BTreeLeaf => {
                    drop(page);
                    drop(guard);
                    break;
                }
                PageType::BTreeInternal => {
                    let child = find_child_in_internal(&page, key);
                    drop(page);
                    drop(guard);
                    current_page_id = child;
                }
                _ => {
                    return Err(crate::error::StorageError::Corruption(
                        "unexpected page type during delete traversal".into(),
                    )
                    .into());
                }
            }
        }

        // Acquire exclusive lock on the leaf.
        let mut guard = self.buffer_pool.fetch_page_exclusive(current_page_id)?;
        let page = SlottedPageRef::from_buf(guard.data())?;
        let (found, idx) = binary_search_slots(&page, key, true);
        drop(page);

        if !found {
            return Ok(false);
        }

        let buf = guard.data_mut();
        delete_cell_at_pos(buf, idx);

        Ok(true)
    }

    /// Range scan with bounds and direction.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        direction: ScanDirection,
    ) -> ScanIterator {
        let root_page = self.root_page();

        let lower_bound = match lower {
            Bound::Unbounded => None,
            Bound::Included(k) => Some((k.to_vec(), true)),
            Bound::Excluded(k) => Some((k.to_vec(), false)),
        };

        let upper_bound = match upper {
            Bound::Unbounded => None,
            Bound::Included(k) => Some((k.to_vec(), true)),
            Bound::Excluded(k) => Some((k.to_vec(), false)),
        };

        match direction {
            ScanDirection::Forward => {
                let (start_page, start_slot) =
                    self.find_scan_start(&lower_bound, root_page);
                ScanIterator {
                    buffer_pool: self.buffer_pool.clone(),
                    current_page: start_page,
                    current_slot: start_slot,
                    lower: lower_bound,
                    upper: upper_bound,
                    direction,
                    root_page,
                    done: start_page.is_none(),
                }
            }
            ScanDirection::Backward => {
                let (start_page, start_slot) =
                    self.find_scan_start_backward(&upper_bound, root_page);
                ScanIterator {
                    buffer_pool: self.buffer_pool.clone(),
                    current_page: start_page,
                    current_slot: start_slot,
                    lower: lower_bound,
                    upper: upper_bound,
                    direction,
                    root_page,
                    done: start_page.is_none(),
                }
            }
        }
    }

    // ─── Internal helpers ───

    /// Find the starting leaf page and slot for a forward scan.
    fn find_scan_start(
        &self,
        lower: &Option<(Vec<u8>, bool)>,
        root_page: PageId,
    ) -> (Option<PageId>, u16) {
        let mut current_page_id = root_page;

        loop {
            let guard = match self.buffer_pool.fetch_page_shared(current_page_id) {
                Ok(g) => g,
                Err(_) => return (None, 0),
            };
            // Buffer from pool is always page_size.
            let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");

            match page.try_page_type() {
                Some(PageType::BTreeLeaf) => {
                    let num_slots = page.num_slots();
                    if num_slots == 0 {
                        return (None, 0);
                    }

                    let start_slot = match lower {
                        None => 0,
                        Some((key, inclusive)) => {
                            let (found, idx) = binary_search_slots(&page, key, true);
                            if found {
                                if *inclusive { idx } else { idx + 1 }
                            } else {
                                idx
                            }
                        }
                    };

                    if start_slot >= num_slots {
                        // All keys on this page are less than lower bound.
                        // Move to right sibling.
                        let right_sibling = page.prev_or_ptr();
                        drop(page);
                        drop(guard);
                        if right_sibling == 0 {
                            return (None, 0);
                        }
                        return (Some(right_sibling), 0);
                    }

                    return (Some(current_page_id), start_slot);
                }
                Some(PageType::BTreeInternal) => {
                    let child = match lower {
                        None => page.prev_or_ptr(),
                        Some((key, _)) => find_child_in_internal(&page, key),
                    };
                    drop(page);
                    drop(guard);
                    current_page_id = child;
                }
                _ => return (None, 0),
            }
        }
    }

    /// Find the starting leaf page and slot for a backward scan.
    fn find_scan_start_backward(
        &self,
        upper: &Option<(Vec<u8>, bool)>,
        root_page: PageId,
    ) -> (Option<PageId>, u16) {
        let mut current_page_id = root_page;

        loop {
            let guard = match self.buffer_pool.fetch_page_shared(current_page_id) {
                Ok(g) => g,
                Err(_) => return (None, 0),
            };
            // Buffer from pool is always page_size.
            let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");

            match page.try_page_type() {
                Some(PageType::BTreeLeaf) => {
                    let num_slots = page.num_slots();
                    if num_slots == 0 {
                        return (None, 0);
                    }

                    let start_slot = match upper {
                        None => num_slots - 1,
                        Some((key, inclusive)) => {
                            let (found, idx) = binary_search_slots(&page, key, true);
                            if found {
                                if *inclusive {
                                    idx
                                } else if idx > 0 {
                                    idx - 1
                                } else {
                                    // Need to go to previous leaf.
                                    drop(page);
                                    drop(guard);
                                    return match find_prev_leaf_page(
                                        &self.buffer_pool,
                                        current_page_id,
                                        root_page,
                                    ) {
                                        Some((pid, slot)) => (Some(pid), slot),
                                        None => (None, 0),
                                    };
                                }
                            } else if idx > 0 {
                                idx - 1
                            } else {
                                // All keys on this leaf >= upper bound.
                                drop(page);
                                drop(guard);
                                return match find_prev_leaf_page(
                                    &self.buffer_pool,
                                    current_page_id,
                                    root_page,
                                ) {
                                    Some((pid, slot)) => (Some(pid), slot),
                                    None => (None, 0),
                                };
                            }
                        }
                    };

                    return (Some(current_page_id), start_slot);
                }
                Some(PageType::BTreeInternal) => {
                    let child = match upper {
                        None => {
                            // Go to rightmost child.
                            let num_slots = page.num_slots();
                            if num_slots == 0 {
                                page.prev_or_ptr()
                            } else {
                                let cell = page.slot_data(num_slots - 1);
                                let (_, child) = decode_internal_cell(cell);
                                child
                            }
                        }
                        Some((key, _)) => find_child_in_internal(&page, key),
                    };
                    drop(page);
                    drop(guard);
                    current_page_id = child;
                }
                _ => return (None, 0),
            }
        }
    }

    /// Split a leaf page. Returns the split result (median key + new page id).
    fn split_leaf(
        &self,
        guard: &mut crate::buffer_pool::ExclusivePageGuard<'_>,
        new_key: &[u8],
        new_value: &[u8],
        free_list: &mut FreeList,
    ) -> io::Result<SplitResult> {
        let old_page_id = guard.page_id();
        let page = SlottedPageRef::from_buf(guard.data())?;
        let num_slots = page.num_slots();
        let old_right_sibling = page.prev_or_ptr();

        // Collect all existing entries plus the new one.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(num_slots as usize + 1);
        for i in 0..num_slots {
            let cell = page.slot_data(i);
            let (k, v) = decode_leaf_cell(cell);
            entries.push((k.to_vec(), v.to_vec()));
        }
        drop(page);

        // Insert the new key-value pair in sorted order.
        let insert_pos = entries.partition_point(|(k, _)| k.as_slice() < new_key);
        entries.insert(insert_pos, (new_key.to_vec(), new_value.to_vec()));

        let total = entries.len();
        let split_point = total / 2;

        // The median key is the first key of the upper half.
        let median_key = entries[split_point].0.clone();

        // Allocate a new leaf page for the upper half.
        let new_page_id = free_list.allocate()?;
        let mut new_guard = self.buffer_pool.new_page(new_page_id)?;
        {
            let buf = new_guard.data_mut();
            let mut new_page = SlottedPage::init(buf, new_page_id, PageType::BTreeLeaf);
            // Set right sibling of new leaf to old leaf's right sibling.
            new_page.set_prev_or_ptr(old_right_sibling);
        }

        // Write upper half entries to new leaf.
        for (k, v) in &entries[split_point..] {
            let cell = encode_leaf_cell(k, v);
            let buf = new_guard.data_mut();
            let num = SlottedPageRef::from_buf(buf)?.num_slots();
            insert_cell_at_pos(buf, &cell, num).map_err(|_| {
                io::Error::other(crate::error::StorageError::InternalBug(
                    "new leaf page should have space after split".into(),
                ))
            })?;
        }
        drop(new_guard);

        // Rewrite old leaf with lower half.
        {
            let buf = guard.data_mut();
            SlottedPage::init(buf, old_page_id, PageType::BTreeLeaf);
            let mut page = SlottedPage::from_buf(buf)?;
            // Set right sibling of old leaf to new leaf.
            page.set_prev_or_ptr(new_page_id);
            drop(page);

            for (k, v) in &entries[..split_point] {
                let cell = encode_leaf_cell(k, v);
                let num = SlottedPageRef::from_buf(buf)?.num_slots();
                insert_cell_at_pos(buf, &cell, num).map_err(|_| {
                    io::Error::other(crate::error::StorageError::InternalBug(
                        "old leaf page should have space after split".into(),
                    ))
                })?;
            }
        }

        Ok(SplitResult {
            median_key,
            new_page_id,
        })
    }

    /// Propagate a split result up through parent pages.
    fn propagate_split(
        &self,
        mut split: SplitResult,
        path: &mut Vec<PageId>,
        _child_page_id: PageId,
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        while let Some(parent_page_id) = path.pop() {
            let mut parent_guard = self.buffer_pool.fetch_page_exclusive(parent_page_id)?;

            // Try to insert the promoted key + child into the parent.
            let cell = encode_internal_cell(&split.median_key, split.new_page_id);

            {
                let page = SlottedPageRef::from_buf(parent_guard.data())?;
                let (_, insert_pos) = binary_search_slots(&page, &split.median_key, false);
                drop(page);

                let buf = parent_guard.data_mut();
                match insert_cell_at_pos(buf, &cell, insert_pos) {
                    Ok(()) => {
                        drop(parent_guard);
                        return Ok(());
                    }
                    Err(_) => {
                        // Parent is full, need to split the internal node.
                    }
                }
            }

            // Split the internal node.
            let new_split = self.split_internal(
                &mut parent_guard,
                &split.median_key,
                split.new_page_id,
                free_list,
            )?;
            let this_page_id = parent_guard.page_id();
            drop(parent_guard);

            // If this was the root, create a new root.
            if path.is_empty() && this_page_id == self.root_page() {
                self.create_new_root(this_page_id, &new_split, free_list)?;
                return Ok(());
            }

            split = new_split;
        }

        // If we get here, we split the root without a parent.
        let old_root = self.root_page();
        self.create_new_root(old_root, &split, free_list)?;
        Ok(())
    }

    /// Split an internal node. Returns the split result.
    fn split_internal(
        &self,
        guard: &mut crate::buffer_pool::ExclusivePageGuard<'_>,
        new_key: &[u8],
        new_child: PageId,
        free_list: &mut FreeList,
    ) -> io::Result<SplitResult> {
        let old_page_id = guard.page_id();
        let page = SlottedPageRef::from_buf(guard.data())?;
        let num_slots = page.num_slots();
        let leftmost_child = page.prev_or_ptr();

        // Collect all existing entries.
        let mut entries: Vec<(Vec<u8>, PageId)> = Vec::with_capacity(num_slots as usize + 1);
        for i in 0..num_slots {
            let cell = page.slot_data(i);
            let (k, c) = decode_internal_cell(cell);
            entries.push((k.to_vec(), c));
        }
        drop(page);

        // Insert new entry in sorted position.
        let insert_pos = entries.partition_point(|(k, _)| k.as_slice() < new_key);
        entries.insert(insert_pos, (new_key.to_vec(), new_child));

        let total = entries.len();
        let median_idx = total / 2;

        // The median key is promoted to the parent (NOT kept in either child).
        let median_key = entries[median_idx].0.clone();
        let median_child = entries[median_idx].1;

        // Allocate new internal page for the upper half.
        let new_page_id = free_list.allocate()?;
        let mut new_guard = self.buffer_pool.new_page(new_page_id)?;
        {
            let buf = new_guard.data_mut();
            let mut new_page = SlottedPage::init(buf, new_page_id, PageType::BTreeInternal);
            // The leftmost child of the new internal node is the child pointer
            // from the median entry.
            new_page.set_prev_or_ptr(median_child);
        }

        // Write upper half entries (after median) to new internal page.
        for (k, c) in &entries[median_idx + 1..] {
            let cell = encode_internal_cell(k, *c);
            let buf = new_guard.data_mut();
            let num = SlottedPageRef::from_buf(buf)?.num_slots();
            insert_cell_at_pos(buf, &cell, num).map_err(|_| {
                io::Error::other(crate::error::StorageError::InternalBug(
                    "new internal page should have space after split".into(),
                ))
            })?;
        }
        drop(new_guard);

        // Rewrite old internal page with lower half.
        {
            let buf = guard.data_mut();
            SlottedPage::init(buf, old_page_id, PageType::BTreeInternal);
            let mut page = SlottedPage::from_buf(buf)?;
            page.set_prev_or_ptr(leftmost_child);
            drop(page);

            for (k, c) in &entries[..median_idx] {
                let cell = encode_internal_cell(k, *c);
                let num = SlottedPageRef::from_buf(buf)?.num_slots();
                insert_cell_at_pos(buf, &cell, num).map_err(|_| {
                    io::Error::other(crate::error::StorageError::InternalBug(
                        "old internal page should have space after split".into(),
                    ))
                })?;
            }
        }

        Ok(SplitResult {
            median_key,
            new_page_id,
        })
    }

    /// Create a new root page after the old root splits.
    fn create_new_root(
        &self,
        old_root: PageId,
        split: &SplitResult,
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        let new_root_id = free_list.allocate()?;
        let mut guard = self.buffer_pool.new_page(new_root_id)?;
        {
            let buf = guard.data_mut();
            let mut page = SlottedPage::init(buf, new_root_id, PageType::BTreeInternal);
            // Leftmost child = old root.
            page.set_prev_or_ptr(old_root);
            drop(page);

            // Insert one slot: (median_key, new_page_id).
            let cell = encode_internal_cell(&split.median_key, split.new_page_id);
            insert_cell_at_pos(buf, &cell, 0).map_err(|_| {
                io::Error::other(crate::error::StorageError::InternalBug(
                    "new root should have space for one entry".into(),
                ))
            })?;
        }
        drop(guard);

        self.root_page.store(new_root_id, Ordering::Release);
        Ok(())
    }
}

/// Find the child page to follow in an internal node for a given key.
fn find_child_in_internal(page: &SlottedPageRef, key: &[u8]) -> PageId {
    let num_slots = page.num_slots();
    if num_slots == 0 {
        return page.prev_or_ptr();
    }

    let (found, idx) = binary_search_slots(page, key, false);

    if found {
        // Key matches slot[idx]; go to slot[idx].child (keys >= slot[idx].key).
        let cell = page.slot_data(idx);
        let (_, child) = decode_internal_cell(cell);
        child
    } else if idx == 0 {
        // Key < all separator keys; go to leftmost child.
        page.prev_or_ptr()
    } else {
        // Key falls between slot[idx-1] and slot[idx] (or after the last slot).
        // Go to slot[idx-1].child.
        let cell = page.slot_data(idx - 1);
        let (_, child) = decode_internal_cell(cell);
        child
    }
}

// ─── ScanIterator ───

/// Iterator over `(key, value)` pairs from a B+ tree range scan.
pub struct ScanIterator {
    buffer_pool: Arc<BufferPool>,
    current_page: Option<PageId>,
    current_slot: u16,
    lower: Option<(Vec<u8>, bool)>, // None = unbounded, Some((key, inclusive))
    upper: Option<(Vec<u8>, bool)>, // None = unbounded, Some((key, inclusive))
    direction: ScanDirection,
    root_page: PageId, // needed for backward scan re-traversal
    done: bool,
}

impl Iterator for ScanIterator {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.direction {
            ScanDirection::Forward => self.next_forward(),
            ScanDirection::Backward => self.next_backward(),
        }
    }
}

impl ScanIterator {
    fn next_forward(&mut self) -> Option<io::Result<(Vec<u8>, Vec<u8>)>> {
        loop {
            let page_id = match self.current_page {
                Some(p) => p,
                None => {
                    self.done = true;
                    return None;
                }
            };

            let guard = match self.buffer_pool.fetch_page_shared(page_id) {
                Ok(g) => g,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };

            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(p) => p,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            let num_slots = page.num_slots();

            if self.current_slot >= num_slots {
                // Move to next leaf (right sibling).
                let right_sibling = page.prev_or_ptr();
                drop(page);
                drop(guard);

                if right_sibling == 0 {
                    self.done = true;
                    return None;
                }
                self.current_page = Some(right_sibling);
                self.current_slot = 0;
                continue;
            }

            let cell = page.slot_data(self.current_slot);
            if cell.is_empty() {
                self.current_slot += 1;
                continue;
            }

            let (key, value) = decode_leaf_cell(cell);

            // Check upper bound.
            if let Some((ref upper_key, inclusive)) = self.upper {
                match key.cmp(upper_key.as_slice()) {
                    std::cmp::Ordering::Greater => {
                        self.done = true;
                        return None;
                    }
                    std::cmp::Ordering::Equal if !inclusive => {
                        self.done = true;
                        return None;
                    }
                    _ => {}
                }
            }

            let result = (key.to_vec(), value.to_vec());
            self.current_slot += 1;
            drop(page);
            drop(guard);

            return Some(Ok(result));
        }
    }

    fn next_backward(&mut self) -> Option<io::Result<(Vec<u8>, Vec<u8>)>> {
        loop {
            let page_id = match self.current_page {
                Some(p) => p,
                None => {
                    self.done = true;
                    return None;
                }
            };

            let guard = match self.buffer_pool.fetch_page_shared(page_id) {
                Ok(g) => g,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };

            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(p) => p,
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            };
            let num_slots = page.num_slots();

            if num_slots == 0 || self.current_slot >= num_slots {
                // Empty page or invalid position. Try previous leaf.
                drop(page);
                drop(guard);
                match find_prev_leaf_page(&self.buffer_pool, page_id, self.root_page) {
                    Some((prev_id, last_slot)) => {
                        self.current_page = Some(prev_id);
                        self.current_slot = last_slot;
                        continue;
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }

            let cell = page.slot_data(self.current_slot);
            if cell.is_empty() {
                if self.current_slot == 0 {
                    drop(page);
                    drop(guard);
                    match find_prev_leaf_page(&self.buffer_pool, page_id, self.root_page) {
                        Some((prev_id, last_slot)) => {
                            self.current_page = Some(prev_id);
                            self.current_slot = last_slot;
                            continue;
                        }
                        None => {
                            self.done = true;
                            return None;
                        }
                    }
                }
                self.current_slot -= 1;
                continue;
            }

            let (key, value) = decode_leaf_cell(cell);

            // Check lower bound.
            if let Some((ref lower_key, inclusive)) = self.lower {
                match key.cmp(lower_key.as_slice()) {
                    std::cmp::Ordering::Less => {
                        self.done = true;
                        return None;
                    }
                    std::cmp::Ordering::Equal if !inclusive => {
                        self.done = true;
                        return None;
                    }
                    _ => {}
                }
            }

            let result = (key.to_vec(), value.to_vec());
            drop(page);
            drop(guard);

            // Advance backward.
            if self.current_slot == 0 {
                match find_prev_leaf_page(&self.buffer_pool, page_id, self.root_page) {
                    Some((prev_id, last_slot)) => {
                        self.current_page = Some(prev_id);
                        self.current_slot = last_slot;
                    }
                    None => {
                        self.done = true;
                    }
                }
            } else {
                self.current_slot -= 1;
            }

            return Some(Ok(result));
        }
    }
}

/// Find the previous leaf page by traversing from root.
/// Returns `(page_id, last_slot_index)` of the previous leaf, or `None`.
fn find_prev_leaf_page(
    buffer_pool: &BufferPool,
    target_page_id: PageId,
    root_page: PageId,
) -> Option<(PageId, u16)> {
    // Find the leftmost leaf by traversing down left children from root.
    let leftmost_leaf = {
        let mut current = root_page;
        loop {
            let guard = buffer_pool.fetch_page_shared(current).ok()?;
            // Buffer from pool is always page_size.
            let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");
            match page.try_page_type() {
                Some(PageType::BTreeLeaf) => break current,
                Some(PageType::BTreeInternal) => {
                    let child = page.prev_or_ptr();
                    drop(page);
                    drop(guard);
                    current = child;
                }
                _ => return None,
            }
        }
    };

    if leftmost_leaf == target_page_id {
        return None; // target is the leftmost leaf; no predecessor.
    }

    // Walk the leaf chain from the leftmost leaf to find the predecessor of target.
    let mut prev_page_id = leftmost_leaf;
    loop {
        let guard = buffer_pool.fetch_page_shared(prev_page_id).ok()?;
        // Buffer from pool is always page_size.
        let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");
        let right = page.prev_or_ptr();
        let ns = page.num_slots();
        drop(page);
        drop(guard);

        if right == target_page_id {
            if ns == 0 {
                return None;
            }
            return Some((prev_page_id, ns - 1));
        }
        if right == 0 {
            return None; // Reached end without finding target.
        }
        prev_page_id = right;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, PageStorage};
    use crate::buffer_pool::BufferPoolConfig;

    const PAGE_SIZE: usize = 4096;

    /// Helper: set up MemoryPageStorage + BufferPool + FreeList for tests.
    fn setup() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        // Pre-allocate page 0 (reserved).
        storage.extend(1).unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 256,
            },
            storage,
        ));
        let free_list = FreeList::new(0, pool.clone());
        (pool, free_list)
    }

    /// Helper: set up with a larger buffer pool for big tests.
    fn setup_large() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(1).unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 4096,
            },
            storage,
        ));
        let free_list = FreeList::new(0, pool.clone());
        (pool, free_list)
    }

    // ─── Test 1: Create empty tree ───
    #[test]
    fn test_create_empty_tree() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let root = tree.root_page();
        let guard = pool.fetch_page_shared(root).unwrap();
        let page = SlottedPageRef::from_buf(guard.data()).unwrap();
        assert_eq!(page.page_type(), PageType::BTreeLeaf);
        assert_eq!(page.num_slots(), 0);
    }

    // ─── Test 2: Insert + get single ───
    #[test]
    fn test_insert_get_single() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        tree.insert(b"hello", b"world", &mut fl).unwrap();
        let val = tree.get(b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    // ─── Test 3: Insert + get missing ───
    #[test]
    fn test_insert_get_missing() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        tree.insert(b"hello", b"world", &mut fl).unwrap();
        let val = tree.get(b"other_key").unwrap();
        assert_eq!(val, None);
    }

    // ─── Test 4: Insert 10K random keys ───
    #[test]
    fn test_insert_10k_random_keys() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Generate deterministic "random" key-value pairs using a simple LCG.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut seed: u64 = 12345;
        for _ in 0..10_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = format!("key_{:016x}", seed).into_bytes();
            let value = format!("val_{:016x}", seed).into_bytes();
            entries.push((key, value));
        }

        for (k, v) in &entries {
            tree.insert(k, v, &mut fl).unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(v.as_slice()),
                "mismatch for key {:?}",
                String::from_utf8_lossy(k)
            );
        }
    }

    // ─── Test 5: Insert sorted keys ───
    #[test]
    fn test_insert_sorted_keys() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..1000u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            entries.push((key, value));
        }

        for (k, v) in &entries {
            tree.insert(k, v, &mut fl).unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).unwrap();
            assert_eq!(got.as_deref(), Some(v.as_slice()));
        }
    }

    // ─── Test 6: Insert reverse sorted ───
    #[test]
    fn test_insert_reverse_sorted() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in (0..1000u32).rev() {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            entries.push((key, value));
        }

        for (k, v) in &entries {
            tree.insert(k, v, &mut fl).unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).unwrap();
            assert_eq!(got.as_deref(), Some(v.as_slice()));
        }
    }

    // ─── Test 7: Leaf split ───
    #[test]
    fn test_leaf_split() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();
        let initial_root = tree.root_page();

        // Insert enough entries to force a leaf split.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        // Root should have changed (original root was a leaf that split).
        assert_ne!(tree.root_page(), initial_root);

        // Verify all entries are still retrievable.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            let got = tree.get(&key).unwrap();
            assert_eq!(got.as_deref(), Some(value.as_slice()), "missing key {}", i);
        }
    }

    // ─── Test 8: Internal split ───
    #[test]
    fn test_internal_split() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Insert enough to cause many leaf splits and at least one internal split.
        let count = 5000;
        for i in 0..count {
            let key = format!("k{:06}", i).into_bytes();
            let value = format!("v{:06}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        // Verify all entries.
        for i in 0..count {
            let key = format!("k{:06}", i).into_bytes();
            let value = format!("v{:06}", i).into_bytes();
            let got = tree.get(&key).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(value.as_slice()),
                "missing key k{:06}",
                i
            );
        }
    }

    // ─── Test 9: Root split ───
    #[test]
    fn test_root_split() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Force a root split by inserting enough entries.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("v{}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        // Verify new root is internal.
        let root = tree.root_page();
        let guard = pool.fetch_page_shared(root).unwrap();
        let page = SlottedPageRef::from_buf(guard.data()).unwrap();
        assert_eq!(page.page_type(), PageType::BTreeInternal);
        // Should have at least 1 slot (separator key) pointing to 2 children.
        assert!(page.num_slots() >= 1);
    }

    // ─── Test 10: Delete single ───
    #[test]
    fn test_delete_single() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        tree.insert(b"aaa", b"val_a", &mut fl).unwrap();
        tree.insert(b"bbb", b"val_b", &mut fl).unwrap();
        tree.insert(b"ccc", b"val_c", &mut fl).unwrap();

        let deleted = tree.delete(b"bbb", &mut fl).unwrap();
        assert!(deleted);

        assert_eq!(tree.get(b"bbb").unwrap(), None);
        assert_eq!(tree.get(b"aaa").unwrap(), Some(b"val_a".to_vec()));
        assert_eq!(tree.get(b"ccc").unwrap(), Some(b"val_c".to_vec()));
    }

    // ─── Test 11: Delete all ───
    #[test]
    fn test_delete_all() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let deleted = tree.delete(&key, &mut fl).unwrap();
            assert!(deleted, "key_{:04} should be deletable", i);
        }

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(tree.get(&key).unwrap(), None, "key_{:04} should be gone", i);
        }
    }

    // ─── Test 12: Scan forward all ───
    #[test]
    fn test_scan_forward_all() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).unwrap();
        }

        let results: Vec<(Vec<u8>, Vec<u8>)> = tree
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, keys);
    }

    // ─── Test 13: Scan with bounds ───
    #[test]
    fn test_scan_with_bounds() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).unwrap();
        }

        // Scan [B, E) = [B, C, D]
        let results: Vec<(Vec<u8>, Vec<u8>)> = tree
            .scan(
                Bound::Included(b"B"),
                Bound::Excluded(b"E"),
                ScanDirection::Forward,
            )
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, vec![b"B" as &[u8], b"C", b"D"]);
    }

    // ─── Test 14: Scan backward ───
    #[test]
    fn test_scan_backward() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).unwrap();
        }

        let results: Vec<(Vec<u8>, Vec<u8>)> = tree
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Backward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, vec![b"E" as &[u8], b"D", b"C", b"B", b"A"]);
    }

    // ─── Test 15: Scan empty range ───
    #[test]
    fn test_scan_empty_range() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).unwrap();
        }

        // lower > upper: should be empty.
        let results: Vec<(Vec<u8>, Vec<u8>)> = tree
            .scan(
                Bound::Included(b"Z"),
                Bound::Included(b"A"),
                ScanDirection::Forward,
            )
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        assert!(results.is_empty());
    }

    // ─── Test 16: Scan across leaf boundaries ───
    #[test]
    fn test_scan_across_leaf_boundaries() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let count = 500;
        let mut expected_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("val_{:06}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
            expected_keys.push(key);
        }
        expected_keys.sort();

        let results: Vec<(Vec<u8>, Vec<u8>)> = tree
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        let result_keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(result_keys.len(), count);
        assert_eq!(result_keys, expected_keys);

        // Verify sorted order.
        for i in 1..result_keys.len() {
            assert!(
                result_keys[i - 1] < result_keys[i],
                "keys not sorted at index {}",
                i
            );
        }
    }

    // ─── Test 17: Insert duplicate key (overwrite) ───
    #[test]
    fn test_insert_duplicate_key() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        tree.insert(b"key", b"first_value", &mut fl).unwrap();
        tree.insert(b"key", b"second_value", &mut fl).unwrap();

        let val = tree.get(b"key").unwrap();
        assert_eq!(val, Some(b"second_value".to_vec()));
    }

    // ─── Test 18: Large values ───
    #[test]
    fn test_large_values() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        let value = vec![0xABu8; 512];
        let count = 100;
        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            let got = tree.get(&key).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(value.as_slice()),
                "key_{:04} mismatch",
                i
            );
        }
    }

    // ─── Test 19: Empty tree operations ───
    #[test]
    fn test_empty_tree_operations() {
        let (pool, mut fl) = setup();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        // get on empty tree.
        assert_eq!(tree.get(b"anything").unwrap(), None);

        // scan on empty tree.
        let results: Vec<_> = tree
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert!(results.is_empty());

        // delete on empty tree.
        assert!(!tree.delete(b"anything", &mut fl).unwrap());
    }

    // ─── Test 20: Concurrent readers ───
    #[test]
    fn test_concurrent_readers() {
        let (pool, mut fl) = setup_large();
        let tree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Insert some entries.
        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).unwrap();
        }

        let root_page = tree.root_page();

        // Spawn multiple reader threads.
        let mut handles = Vec::new();
        for t in 0..8 {
            let pool_clone = pool.clone();
            let handle = std::thread::spawn(move || {
                let tree = BTree::open(root_page, pool_clone);
                for i in 0..100u32 {
                    let key = format!("key_{:04}", i).into_bytes();
                    let value = format!("val_{:04}", i).into_bytes();
                    let got = tree.get(&key).unwrap();
                    assert_eq!(
                        got.as_deref(),
                        Some(value.as_slice()),
                        "thread {} failed on key_{:04}",
                        t,
                        i
                    );
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
