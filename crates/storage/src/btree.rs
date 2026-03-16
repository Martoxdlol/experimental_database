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
use futures_core::Stream;
use std::ops::Bound;
use std::pin::Pin;
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

// ─── ScanStream ───

/// Async stream of `(key, value)` pairs from a B+ tree range scan.
pub type ScanStream<'a> = Pin<Box<dyn Stream<Item = io::Result<(Vec<u8>, Vec<u8>)>> + Send + 'a>>;

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
///
/// The root page ID is permanent — it never changes after creation.
/// When a root split occurs, the old root's contents are evacuated to a
/// new page and the root page is rewritten in-place as the new internal node.
pub struct BTree {
    root_page: PageId,
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
    pub async fn create(buffer_pool: Arc<BufferPool>, free_list: &mut FreeList) -> io::Result<Self> {
        let page_id = free_list.allocate().await?;
        let mut guard = buffer_pool.new_page(page_id)?;
        {
            let buf = guard.data_mut();
            SlottedPage::init(buf, page_id, PageType::BTreeLeaf);
        }
        drop(guard);

        Ok(Self {
            root_page: page_id,
            buffer_pool,
        })
    }

    /// Open an existing B-tree rooted at `root_page`.
    pub fn open(root_page: PageId, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            root_page,
            buffer_pool,
        }
    }

    /// Root page ID. Stable — never changes after creation.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// Point lookup. Returns value bytes if found, None if not.
    ///
    /// Uses latch coupling: acquire child shared, release parent shared.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id).await?;
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
    pub async fn insert(
        &self,
        key: &[u8],
        value: &[u8],
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        // Collect the path from root to leaf.
        let mut path: Vec<PageId> = Vec::new();
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id).await?;
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
        let mut guard = self.buffer_pool.fetch_page_exclusive(current_page_id).await?;
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
        let split = self.split_leaf(&mut guard, key, value, free_list).await?;
        drop(guard);

        // Propagate the split up through parents.
        self.propagate_split(split, &mut path, leaf_page_id, free_list).await?;

        Ok(())
    }

    /// Delete a key. Returns true if the key was found and deleted.
    ///
    /// Simple delete: just removes from the leaf (no merge/redistribute for v1).
    pub async fn delete(&self, key: &[u8], _free_list: &mut FreeList) -> io::Result<bool> {
        // Traverse to the leaf.
        let mut current_page_id = self.root_page();

        loop {
            let guard = self.buffer_pool.fetch_page_shared(current_page_id).await?;
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
        let mut guard = self.buffer_pool.fetch_page_exclusive(current_page_id).await?;
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

    /// Range scan with bounds and direction. Returns an async `Stream`.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        direction: ScanDirection,
    ) -> ScanStream<'_> {
        let root_page = self.root_page();

        let lower_bound: Option<(Vec<u8>, bool)> = match lower {
            Bound::Unbounded => None,
            Bound::Included(k) => Some((k.to_vec(), true)),
            Bound::Excluded(k) => Some((k.to_vec(), false)),
        };

        let upper_bound: Option<(Vec<u8>, bool)> = match upper {
            Bound::Unbounded => None,
            Bound::Included(k) => Some((k.to_vec(), true)),
            Bound::Excluded(k) => Some((k.to_vec(), false)),
        };

        let buffer_pool = self.buffer_pool.clone();

        match direction {
            ScanDirection::Forward => {
                Box::pin(async_stream::try_stream! {
                    let (mut current_page, mut current_slot) =
                        find_scan_start_async(&buffer_pool, &lower_bound, root_page).await;

                    while let Some(page_id) = current_page {
                        match forward_step(&buffer_pool, page_id, current_slot, &upper_bound).await? {
                            ForwardStep::Yield(k, v) => {
                                current_slot += 1;
                                yield (k, v);
                            }
                            ForwardStep::NextSibling(sibling) => {
                                current_page = Some(sibling);
                                current_slot = 0;
                            }
                            ForwardStep::SkipSlot => {
                                current_slot += 1;
                            }
                            ForwardStep::Done => break,
                        }
                    }
                })
            }
            ScanDirection::Backward => {
                Box::pin(async_stream::try_stream! {
                    let (mut current_page, mut current_slot) =
                        find_scan_start_backward_async(&buffer_pool, &upper_bound, root_page).await;

                    while let Some(page_id) = current_page {
                        match backward_step(&buffer_pool, page_id, current_slot, &lower_bound).await? {
                            BackwardStep::Yield(k, v) => {
                                if current_slot == 0 {
                                    match find_prev_leaf_page_async(&buffer_pool, page_id, root_page).await {
                                        Some((prev_id, last_slot)) => {
                                            current_page = Some(prev_id);
                                            current_slot = last_slot;
                                        }
                                        None => {
                                            current_page = None;
                                        }
                                    }
                                } else {
                                    current_slot -= 1;
                                }
                                yield (k, v);
                            }
                            BackwardStep::PrevLeaf => {
                                match find_prev_leaf_page_async(&buffer_pool, page_id, root_page).await {
                                    Some((prev_id, last_slot)) => {
                                        current_page = Some(prev_id);
                                        current_slot = last_slot;
                                    }
                                    None => break,
                                }
                            }
                            BackwardStep::SkipSlot => {
                                current_slot -= 1;
                            }
                            BackwardStep::Done => break,
                        }
                    }
                })
            }
        }
    }

    // ─── Internal helpers ───

    /// Split a leaf page. Returns the split result (median key + new page id).
    async fn split_leaf(
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
        let new_page_id = free_list.allocate().await?;
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
    async fn propagate_split(
        &self,
        mut split: SplitResult,
        path: &mut Vec<PageId>,
        _child_page_id: PageId,
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        while let Some(parent_page_id) = path.pop() {
            let mut parent_guard = self.buffer_pool.fetch_page_exclusive(parent_page_id).await?;

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
            ).await?;
            let this_page_id = parent_guard.page_id();
            drop(parent_guard);

            // If this was the root, evacuate and rewrite it.
            if path.is_empty() && this_page_id == self.root_page() {
                self.create_new_root(&new_split, free_list).await?;
                return Ok(());
            }

            split = new_split;
        }

        // If we get here, we split the root without a parent.
        self.create_new_root(&split, free_list).await?;
        Ok(())
    }

    /// Split an internal node. Returns the split result.
    async fn split_internal(
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
        let new_page_id = free_list.allocate().await?;
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

    /// Handle a root split by evacuating the root's current contents to a
    /// new page and rewriting the root in-place as the new internal node.
    ///
    /// This keeps the root page ID stable — it never changes after creation.
    async fn create_new_root(
        &self,
        split: &SplitResult,
        free_list: &mut FreeList,
    ) -> io::Result<()> {
        let root_id = self.root_page;

        // 1. Allocate a page to evacuate the root's current contents into.
        let evacuated_id = free_list.allocate().await?;

        // 2. Copy root page data to the evacuated page.
        let mut root_guard = self.buffer_pool.fetch_page_exclusive(root_id).await?;
        let root_data = root_guard.data().to_vec();

        let mut evac_guard = self.buffer_pool.new_page(evacuated_id)?;
        evac_guard.data_mut().copy_from_slice(&root_data);

        // 3. Fix the page_id in the evacuated page's header.
        {
            let mut page = SlottedPage::from_buf(evac_guard.data_mut())?;
            let mut h = page.header();
            h.page_id = zerocopy::byteorder::U32::<zerocopy::byteorder::LittleEndian>::new(evacuated_id);
            page.set_header(&h);
        }
        drop(evac_guard);

        // 4. Rewrite the root page as a new internal node.
        {
            let buf = root_guard.data_mut();
            let mut page = SlottedPage::init(buf, root_id, PageType::BTreeInternal);
            // Leftmost child = evacuated page (old root contents).
            page.set_prev_or_ptr(evacuated_id);
            drop(page);

            // Insert one slot: (median_key, split's new sibling page).
            let cell = encode_internal_cell(&split.median_key, split.new_page_id);
            insert_cell_at_pos(buf, &cell, 0).map_err(|_| {
                io::Error::other(crate::error::StorageError::InternalBug(
                    "new root should have space for one entry".into(),
                ))
            })?;
        }
        drop(root_guard);

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

// ─── Scan step helpers (separate async fns so guards don't cross yield points) ───

/// Result of reading one step in a forward scan.
enum ForwardStep {
    /// Yield this key-value pair, then advance slot.
    Yield(Vec<u8>, Vec<u8>),
    /// Move to the right sibling page.
    NextSibling(PageId),
    /// Current slot was empty, skip it.
    SkipSlot,
    /// Scan is done.
    Done,
}

/// Read one forward-scan step from the given page/slot. The page guard is
/// acquired, read, and dropped entirely inside this function so it never
/// crosses a yield point.
async fn forward_step(
    buffer_pool: &BufferPool,
    page_id: PageId,
    current_slot: u16,
    upper_bound: &Option<(Vec<u8>, bool)>,
) -> io::Result<ForwardStep> {
    let guard = buffer_pool.fetch_page_shared(page_id).await?;
    let page = SlottedPageRef::from_buf(guard.data())?;
    let num_slots = page.num_slots();

    if current_slot >= num_slots {
        let right_sibling = page.prev_or_ptr();
        if right_sibling == 0 {
            return Ok(ForwardStep::Done);
        } else {
            return Ok(ForwardStep::NextSibling(right_sibling));
        }
    }

    let cell = page.slot_data(current_slot);
    if cell.is_empty() {
        return Ok(ForwardStep::SkipSlot);
    }

    let (key, value) = decode_leaf_cell(cell);

    if let Some((ref upper_key, inclusive)) = *upper_bound {
        match key.cmp(upper_key.as_slice()) {
            std::cmp::Ordering::Greater => return Ok(ForwardStep::Done),
            std::cmp::Ordering::Equal if !inclusive => return Ok(ForwardStep::Done),
            _ => {}
        }
    }

    Ok(ForwardStep::Yield(key.to_vec(), value.to_vec()))
}

/// Result of reading one step in a backward scan.
enum BackwardStep {
    /// Yield this key-value pair.
    Yield(Vec<u8>, Vec<u8>),
    /// Need to navigate to the previous leaf.
    PrevLeaf,
    /// Current slot was empty, skip backward.
    SkipSlot,
    /// Scan is done.
    Done,
}

/// Read one backward-scan step from the given page/slot.
async fn backward_step(
    buffer_pool: &BufferPool,
    page_id: PageId,
    current_slot: u16,
    lower_bound: &Option<(Vec<u8>, bool)>,
) -> io::Result<BackwardStep> {
    let guard = buffer_pool.fetch_page_shared(page_id).await?;
    let page = SlottedPageRef::from_buf(guard.data())?;
    let num_slots = page.num_slots();

    if num_slots == 0 || current_slot >= num_slots {
        return Ok(BackwardStep::PrevLeaf);
    }

    let cell = page.slot_data(current_slot);
    if cell.is_empty() {
        if current_slot == 0 {
            return Ok(BackwardStep::PrevLeaf);
        } else {
            return Ok(BackwardStep::SkipSlot);
        }
    }

    let (key, value) = decode_leaf_cell(cell);

    if let Some((ref lower_key, inclusive)) = *lower_bound {
        match key.cmp(lower_key.as_slice()) {
            std::cmp::Ordering::Less => return Ok(BackwardStep::Done),
            std::cmp::Ordering::Equal if !inclusive => return Ok(BackwardStep::Done),
            _ => {}
        }
    }

    Ok(BackwardStep::Yield(key.to_vec(), value.to_vec()))
}

// ─── Async scan helpers (free functions) ───

/// Result of reading a page during forward scan start traversal.
enum ScanStartResult {
    /// Found a leaf: return (page_id, start_slot).
    Found(PageId, u16),
    /// Follow right sibling.
    RightSibling(PageId),
    /// Follow child (internal node).
    Child(PageId),
    /// No result (empty or error).
    None,
}

/// Find the starting leaf page and slot for a forward scan (async version).
async fn find_scan_start_async(
    buffer_pool: &BufferPool,
    lower: &Option<(Vec<u8>, bool)>,
    root_page: PageId,
) -> (Option<PageId>, u16) {
    let mut current_page_id = root_page;

    loop {
        let result = read_scan_start_page(buffer_pool, current_page_id, lower).await;
        match result {
            ScanStartResult::Found(pid, slot) => return (Some(pid), slot),
            ScanStartResult::RightSibling(sibling) => return (Some(sibling), 0),
            ScanStartResult::Child(child) => current_page_id = child,
            ScanStartResult::None => return (None, 0),
        }
    }
}

/// Read a single page for forward scan start. Guard is fully scoped here.
async fn read_scan_start_page(
    buffer_pool: &BufferPool,
    current_page_id: PageId,
    lower: &Option<(Vec<u8>, bool)>,
) -> ScanStartResult {
    let guard = match buffer_pool.fetch_page_shared(current_page_id).await {
        Ok(g) => g,
        Err(_) => return ScanStartResult::None,
    };
    let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");

    match page.try_page_type() {
        Some(PageType::BTreeLeaf) => {
            let num_slots = page.num_slots();
            if num_slots == 0 {
                return ScanStartResult::None;
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
                let right_sibling = page.prev_or_ptr();
                if right_sibling == 0 {
                    ScanStartResult::None
                } else {
                    ScanStartResult::RightSibling(right_sibling)
                }
            } else {
                ScanStartResult::Found(current_page_id, start_slot)
            }
        }
        Some(PageType::BTreeInternal) => {
            let child = match lower {
                None => page.prev_or_ptr(),
                Some((key, _)) => find_child_in_internal(&page, key),
            };
            ScanStartResult::Child(child)
        }
        _ => ScanStartResult::None,
    }
}

/// Result of reading a page during backward scan start traversal.
enum BackwardStartResult {
    /// Found a leaf: return (page_id, start_slot).
    Found(PageId, u16),
    /// Need to find previous leaf from this page.
    NeedPrevLeaf(PageId),
    /// Follow child (internal node).
    Child(PageId),
    /// No result.
    None,
}

/// Find the starting leaf page and slot for a backward scan (async version).
async fn find_scan_start_backward_async(
    buffer_pool: &BufferPool,
    upper: &Option<(Vec<u8>, bool)>,
    root_page: PageId,
) -> (Option<PageId>, u16) {
    let mut current_page_id = root_page;

    loop {
        let result = read_backward_start_page(buffer_pool, current_page_id, upper).await;
        match result {
            BackwardStartResult::Found(pid, slot) => return (Some(pid), slot),
            BackwardStartResult::NeedPrevLeaf(leaf_pid) => {
                return match find_prev_leaf_page_async(buffer_pool, leaf_pid, root_page).await {
                    Some((pid, slot)) => (Some(pid), slot),
                    None => (None, 0),
                };
            }
            BackwardStartResult::Child(child) => current_page_id = child,
            BackwardStartResult::None => return (None, 0),
        }
    }
}

/// Read a single page for backward scan start. Guard is fully scoped here.
async fn read_backward_start_page(
    buffer_pool: &BufferPool,
    current_page_id: PageId,
    upper: &Option<(Vec<u8>, bool)>,
) -> BackwardStartResult {
    let guard = match buffer_pool.fetch_page_shared(current_page_id).await {
        Ok(g) => g,
        Err(_) => return BackwardStartResult::None,
    };
    let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");

    match page.try_page_type() {
        Some(PageType::BTreeLeaf) => {
            let num_slots = page.num_slots();
            if num_slots == 0 {
                return BackwardStartResult::None;
            }

            match upper {
                None => BackwardStartResult::Found(current_page_id, num_slots - 1),
                Some((key, inclusive)) => {
                    let (found, idx) = binary_search_slots(&page, key, true);
                    if found {
                        if *inclusive {
                            BackwardStartResult::Found(current_page_id, idx)
                        } else if idx > 0 {
                            BackwardStartResult::Found(current_page_id, idx - 1)
                        } else {
                            BackwardStartResult::NeedPrevLeaf(current_page_id)
                        }
                    } else if idx > 0 {
                        BackwardStartResult::Found(current_page_id, idx - 1)
                    } else {
                        BackwardStartResult::NeedPrevLeaf(current_page_id)
                    }
                }
            }
        }
        Some(PageType::BTreeInternal) => {
            let child = match upper {
                None => {
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
            BackwardStartResult::Child(child)
        }
        _ => BackwardStartResult::None,
    }
}

/// Read page type and leftmost child from a page. Guard scoped here.
async fn read_page_type_and_leftmost(
    buffer_pool: &BufferPool,
    page_id: PageId,
) -> Option<(PageType, PageId)> {
    let guard = buffer_pool.fetch_page_shared(page_id).await.ok()?;
    let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");
    let pt = page.try_page_type()?;
    let ptr = page.prev_or_ptr();
    Some((pt, ptr))
}

/// Read right sibling and num_slots from a leaf page. Guard scoped here.
async fn read_leaf_chain_info(
    buffer_pool: &BufferPool,
    page_id: PageId,
) -> Option<(PageId, u16)> {
    let guard = buffer_pool.fetch_page_shared(page_id).await.ok()?;
    let page = SlottedPageRef::from_buf(guard.data()).expect("buffer from pool is always page_size");
    let right = page.prev_or_ptr();
    let ns = page.num_slots();
    Some((right, ns))
}

/// Find the previous leaf page by traversing from root (async version).
/// Returns `(page_id, last_slot_index)` of the previous leaf, or `None`.
async fn find_prev_leaf_page_async(
    buffer_pool: &BufferPool,
    target_page_id: PageId,
    root_page: PageId,
) -> Option<(PageId, u16)> {
    // Find the leftmost leaf by traversing down left children from root.
    let leftmost_leaf = {
        let mut current = root_page;
        loop {
            let (pt, ptr) = read_page_type_and_leftmost(buffer_pool, current).await?;
            match pt {
                PageType::BTreeLeaf => break current,
                PageType::BTreeInternal => current = ptr,
                _ => return None,
            }
        }
    };

    if leftmost_leaf == target_page_id {
        return None;
    }

    // Walk the leaf chain from the leftmost leaf to find the predecessor of target.
    let mut prev_page_id = leftmost_leaf;
    loop {
        let (right, ns) = read_leaf_chain_info(buffer_pool, prev_page_id).await?;

        if right == target_page_id {
            if ns == 0 {
                return None;
            }
            return Some((prev_page_id, ns - 1));
        }
        if right == 0 {
            return None;
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
    use tokio_stream::StreamExt;

    const PAGE_SIZE: usize = 4096;

    /// Helper: set up MemoryPageStorage + BufferPool + FreeList for tests.
    async fn setup() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        // Pre-allocate page 0 (reserved).
        storage.extend(1).await.unwrap();
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
    async fn setup_large() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(1).await.unwrap();
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

    /// Helper: collect all items from a ScanStream into a Vec.
    async fn collect_scan(stream: ScanStream<'_>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        tokio::pin!(stream);
        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item?);
        }
        Ok(results)
    }

    // ─── Test 1: Create empty tree ───
    #[tokio::test]
    async fn test_create_empty_tree() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let root = tree.root_page();
        let guard = pool.fetch_page_shared(root).await.unwrap();
        let page = SlottedPageRef::from_buf(guard.data()).unwrap();
        assert_eq!(page.page_type(), PageType::BTreeLeaf);
        assert_eq!(page.num_slots(), 0);
    }

    // ─── Test 2: Insert + get single ───
    #[tokio::test]
    async fn test_insert_get_single() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        tree.insert(b"hello", b"world", &mut fl).await.unwrap();
        let val = tree.get(b"hello").await.unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    // ─── Test 3: Insert + get missing ───
    #[tokio::test]
    async fn test_insert_get_missing() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        tree.insert(b"hello", b"world", &mut fl).await.unwrap();
        let val = tree.get(b"other_key").await.unwrap();
        assert_eq!(val, None);
    }

    // ─── Test 4: Insert 10K random keys ───
    #[tokio::test]
    async fn test_insert_10k_random_keys() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

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
            tree.insert(k, v, &mut fl).await.unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(v.as_slice()),
                "mismatch for key {:?}",
                String::from_utf8_lossy(k)
            );
        }
    }

    // ─── Test 5: Insert sorted keys ───
    #[tokio::test]
    async fn test_insert_sorted_keys() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..1000u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            entries.push((key, value));
        }

        for (k, v) in &entries {
            tree.insert(k, v, &mut fl).await.unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).await.unwrap();
            assert_eq!(got.as_deref(), Some(v.as_slice()));
        }
    }

    // ─── Test 6: Insert reverse sorted ───
    #[tokio::test]
    async fn test_insert_reverse_sorted() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in (0..1000u32).rev() {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            entries.push((key, value));
        }

        for (k, v) in &entries {
            tree.insert(k, v, &mut fl).await.unwrap();
        }

        for (k, v) in &entries {
            let got = tree.get(k).await.unwrap();
            assert_eq!(got.as_deref(), Some(v.as_slice()));
        }
    }

    // ─── Test 7: Leaf split ───
    #[tokio::test]
    async fn test_leaf_split() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();
        let initial_root = tree.root_page();

        // Insert enough entries to force a leaf split.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        // Root page ID should be stable (never changes).
        assert_eq!(tree.root_page(), initial_root);

        // Verify all entries are still retrievable.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("val_{:08}", i).into_bytes();
            let got = tree.get(&key).await.unwrap();
            assert_eq!(got.as_deref(), Some(value.as_slice()), "missing key {}", i);
        }
    }

    // ─── Test 8: Internal split ───
    #[tokio::test]
    async fn test_internal_split() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        // Insert enough to cause many leaf splits and at least one internal split.
        let count = 5000;
        for i in 0..count {
            let key = format!("k{:06}", i).into_bytes();
            let value = format!("v{:06}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        // Verify all entries.
        for i in 0..count {
            let key = format!("k{:06}", i).into_bytes();
            let value = format!("v{:06}", i).into_bytes();
            let got = tree.get(&key).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(value.as_slice()),
                "missing key k{:06}",
                i
            );
        }
    }

    // ─── Test 9: Root split ───
    #[tokio::test]
    async fn test_root_split() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        // Force a root split by inserting enough entries.
        for i in 0..200u32 {
            let key = format!("key_{:08}", i).into_bytes();
            let value = format!("v{}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        // Verify new root is internal.
        let root = tree.root_page();
        let guard = pool.fetch_page_shared(root).await.unwrap();
        let page = SlottedPageRef::from_buf(guard.data()).unwrap();
        assert_eq!(page.page_type(), PageType::BTreeInternal);
        // Should have at least 1 slot (separator key) pointing to 2 children.
        assert!(page.num_slots() >= 1);
    }

    // ─── Test 10: Delete single ───
    #[tokio::test]
    async fn test_delete_single() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        tree.insert(b"aaa", b"val_a", &mut fl).await.unwrap();
        tree.insert(b"bbb", b"val_b", &mut fl).await.unwrap();
        tree.insert(b"ccc", b"val_c", &mut fl).await.unwrap();

        let deleted = tree.delete(b"bbb", &mut fl).await.unwrap();
        assert!(deleted);

        assert_eq!(tree.get(b"bbb").await.unwrap(), None);
        assert_eq!(tree.get(b"aaa").await.unwrap(), Some(b"val_a".to_vec()));
        assert_eq!(tree.get(b"ccc").await.unwrap(), Some(b"val_c".to_vec()));
    }

    // ─── Test 11: Delete all ───
    #[tokio::test]
    async fn test_delete_all() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let deleted = tree.delete(&key, &mut fl).await.unwrap();
            assert!(deleted, "key_{:04} should be deletable", i);
        }

        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(tree.get(&key).await.unwrap(), None, "key_{:04} should be gone", i);
        }
    }

    // ─── Test 12: Scan forward all ───
    #[tokio::test]
    async fn test_scan_forward_all() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).await.unwrap();
        }

        let results = collect_scan(
            tree.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward),
        ).await.unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, keys);
    }

    // ─── Test 13: Scan with bounds ───
    #[tokio::test]
    async fn test_scan_with_bounds() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).await.unwrap();
        }

        // Scan [B, E) = [B, C, D]
        let results = collect_scan(
            tree.scan(
                Bound::Included(b"B"),
                Bound::Excluded(b"E"),
                ScanDirection::Forward,
            ),
        ).await.unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, vec![b"B" as &[u8], b"C", b"D"]);
    }

    // ─── Test 14: Scan backward ───
    #[tokio::test]
    async fn test_scan_backward() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).await.unwrap();
        }

        let results = collect_scan(
            tree.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Backward),
        ).await.unwrap();

        let result_keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(result_keys, vec![b"E" as &[u8], b"D", b"C", b"B", b"A"]);
    }

    // ─── Test 15: Scan empty range ───
    #[tokio::test]
    async fn test_scan_empty_range() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let keys: Vec<&[u8]> = vec![b"A", b"B", b"C", b"D", b"E"];
        for &k in &keys {
            tree.insert(k, k, &mut fl).await.unwrap();
        }

        // lower > upper: should be empty.
        let results = collect_scan(
            tree.scan(
                Bound::Included(b"Z"),
                Bound::Included(b"A"),
                ScanDirection::Forward,
            ),
        ).await.unwrap();

        assert!(results.is_empty());
    }

    // ─── Test 16: Scan across leaf boundaries ───
    #[tokio::test]
    async fn test_scan_across_leaf_boundaries() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let count = 500;
        let mut expected_keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("val_{:06}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
            expected_keys.push(key);
        }
        expected_keys.sort();

        let results = collect_scan(
            tree.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward),
        ).await.unwrap();

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
    #[tokio::test]
    async fn test_insert_duplicate_key() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        tree.insert(b"key", b"first_value", &mut fl).await.unwrap();
        tree.insert(b"key", b"second_value", &mut fl).await.unwrap();

        let val = tree.get(b"key").await.unwrap();
        assert_eq!(val, Some(b"second_value".to_vec()));
    }

    // ─── Test 18: Large values ───
    #[tokio::test]
    async fn test_large_values() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        let value = vec![0xABu8; 512];
        let count = 100;
        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            let got = tree.get(&key).await.unwrap();
            assert_eq!(
                got.as_deref(),
                Some(value.as_slice()),
                "key_{:04} mismatch",
                i
            );
        }
    }

    // ─── Test 19: Empty tree operations ───
    #[tokio::test]
    async fn test_empty_tree_operations() {
        let (pool, mut fl) = setup().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        // get on empty tree.
        assert_eq!(tree.get(b"anything").await.unwrap(), None);

        // scan on empty tree.
        let results = collect_scan(
            tree.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward),
        ).await.unwrap();
        assert!(results.is_empty());

        // delete on empty tree.
        assert!(!tree.delete(b"anything", &mut fl).await.unwrap());
    }

    // ─── Test 20: Concurrent readers ───
    #[tokio::test]
    async fn test_concurrent_readers() {
        let (pool, mut fl) = setup_large().await;
        let tree = BTree::create(pool.clone(), &mut fl).await.unwrap();

        // Insert some entries.
        for i in 0..100u32 {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("val_{:04}", i).into_bytes();
            tree.insert(&key, &value, &mut fl).await.unwrap();
        }

        let root_page = tree.root_page();

        // Spawn multiple reader tasks.
        let mut handles = Vec::new();
        for t in 0..8 {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                let tree = BTree::open(root_page, pool_clone);
                for i in 0..100u32 {
                    let key = format!("key_{:04}", i).into_bytes();
                    let value = format!("val_{:04}", i).into_bytes();
                    let got = tree.get(&key).await.unwrap();
                    assert_eq!(
                        got.as_deref(),
                        Some(value.as_slice()),
                        "task {} failed on key_{:04}",
                        t,
                        i
                    );
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }
    }
}
