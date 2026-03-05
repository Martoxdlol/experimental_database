//! Heap storage for arbitrary byte blobs.
//!
//! Stores variable-length blobs that don't fit inline in B-tree leaf cells.
//! Uses slotted heap pages for the first chunk and overflow page chains for
//! multi-page values. No knowledge of document structure.

use crate::backend::PageId;
use crate::buffer_pool::BufferPool;
use crate::free_list::FreeList;
use crate::page::{PageType, SlottedPage, SlottedPageRef};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of the page header in bytes (must match page.rs).
const PAGE_HEADER_SIZE: usize = 32;

/// Size of one slot directory entry (offset: u16 + length: u16).
const SLOT_ENTRY_SIZE: usize = 4;

/// Heap slot header size: flags(1) + total_length(4) + overflow_page(4) = 9.
const HEAP_SLOT_HEADER_SIZE: usize = 9;

/// Flag bit indicating the slot has an overflow chain.
const HAS_OVERFLOW: u8 = 0x01;

/// Maximum overflow chain length (safety limit).
const MAX_OVERFLOW_CHAIN: usize = 2000;

/// Size of the data_length field stored at the start of overflow page data area.
const OVERFLOW_DATA_LEN_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// HeapRef
// ---------------------------------------------------------------------------

/// Reference to a stored blob: identifies the first page and slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl HeapRef {
    /// Serialize to 6 bytes: page_id(u32 LE) + slot_id(u16 LE).
    pub fn to_bytes(&self) -> [u8; 6] {
        let mut bytes = [0u8; 6];
        bytes[0..4].copy_from_slice(&self.page_id.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.slot_id.to_le_bytes());
        bytes
    }

    /// Deserialize from 6 bytes: page_id(u32 LE) + slot_id(u16 LE).
    pub fn from_bytes(bytes: &[u8; 6]) -> Self {
        let page_id = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let slot_id = u16::from_le_bytes([bytes[4], bytes[5]]);
        Self { page_id, slot_id }
    }
}

// ---------------------------------------------------------------------------
// Heap
// ---------------------------------------------------------------------------

/// Heap storage manager.
///
/// Stores arbitrary byte blobs using slotted heap pages and overflow chains.
/// The free space map is maintained in memory and rebuilt on startup.
pub struct Heap {
    buffer_pool: Arc<BufferPool>,
    /// In-memory map of heap pages with available space.
    /// page_id -> approximate free bytes
    free_space_map: HashMap<PageId, usize>,
}

impl Heap {
    /// Create a new Heap with an empty free space map.
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            buffer_pool,
            free_space_map: HashMap::new(),
        }
    }

    /// Store a blob. Returns a HeapRef for later retrieval.
    ///
    /// Always uses the overflow header format (flags + total_length + overflow_page + chunk).
    /// When no overflow is needed, flags=0 and overflow_page=0.
    pub fn store(&mut self, data: &[u8], free_list: &mut FreeList) -> io::Result<HeapRef> {
        let page_size = self.buffer_pool.page_storage().page_size();

        // Maximum data per heap page slot (one slot):
        // page_size - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE
        let max_slot_data = page_size - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE;

        // First chunk capacity = max_slot_data - HEAP_SLOT_HEADER_SIZE
        let first_chunk_capacity = max_slot_data - HEAP_SLOT_HEADER_SIZE;

        // Usable data per overflow page:
        // page_size - PAGE_HEADER_SIZE - OVERFLOW_DATA_LEN_SIZE
        let overflow_page_capacity = page_size - PAGE_HEADER_SIZE - OVERFLOW_DATA_LEN_SIZE;

        let total_length = data.len();

        // Check max blob size
        let max_blob = first_chunk_capacity + MAX_OVERFLOW_CHAIN * overflow_page_capacity;
        if total_length > max_blob {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "blob too large: {} bytes exceeds maximum {} bytes",
                    total_length, max_blob
                ),
            ));
        }

        // Determine how much data goes in the first chunk
        let first_chunk_len = total_length.min(first_chunk_capacity);
        let remaining_data = &data[first_chunk_len..];

        // Build overflow chain (back-to-front so we know each page's next pointer)
        let first_overflow_page = if remaining_data.is_empty() {
            0u32
        } else {
            self.build_overflow_chain(remaining_data, overflow_page_capacity, free_list)?
        };

        // Build the heap slot payload
        let flags: u8 = if first_overflow_page != 0 {
            HAS_OVERFLOW
        } else {
            0
        };

        let slot_payload_len = HEAP_SLOT_HEADER_SIZE + first_chunk_len;
        let mut slot_payload = vec![0u8; slot_payload_len];
        slot_payload[0] = flags;
        slot_payload[1..5].copy_from_slice(&(total_length as u32).to_le_bytes());
        slot_payload[5..9].copy_from_slice(&first_overflow_page.to_le_bytes());
        slot_payload[9..].copy_from_slice(&data[..first_chunk_len]);

        // The total space needed in a heap page slot: slot_payload_len + SLOT_ENTRY_SIZE
        let needed_space = slot_payload_len + SLOT_ENTRY_SIZE;

        // Find or allocate a heap page with enough space
        let heap_page_id = self.find_or_allocate_heap_page(needed_space, free_list)?;

        // Insert the slot
        let mut guard = self.buffer_pool.fetch_page_exclusive(heap_page_id)?;
        let slot_id = {
            let buf = guard.data_mut();
            let mut page = SlottedPage::from_buf(buf);
            page.insert_slot(&slot_payload).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "page full during heap insert")
            })?
        };
        guard.mark_dirty();

        // Update free space map
        {
            let free = SlottedPageRef::from_buf(guard.data()).free_space();
            self.free_space_map.insert(heap_page_id, free);
        }

        drop(guard);

        Ok(HeapRef {
            page_id: heap_page_id,
            slot_id,
        })
    }

    /// Load a blob by reference. Reassembles overflow chains.
    pub fn load(&self, href: HeapRef) -> io::Result<Vec<u8>> {
        let guard = self.buffer_pool.fetch_page_shared(href.page_id)?;
        let page = SlottedPageRef::from_buf(guard.data());

        if href.slot_id >= page.num_slots() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "slot {} out of range (num_slots {}) on page {}",
                    href.slot_id,
                    page.num_slots(),
                    href.page_id,
                ),
            ));
        }

        let slot_data = page.slot_data(href.slot_id);
        if slot_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "slot {} on page {} is deleted (tombstone)",
                    href.slot_id, href.page_id,
                ),
            ));
        }

        if slot_data.len() < HEAP_SLOT_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "slot {} on page {} too small for heap header ({} < {})",
                    href.slot_id,
                    href.page_id,
                    slot_data.len(),
                    HEAP_SLOT_HEADER_SIZE,
                ),
            ));
        }

        // Parse heap slot header
        let flags = slot_data[0];
        let total_length =
            u32::from_le_bytes([slot_data[1], slot_data[2], slot_data[3], slot_data[4]]) as usize;
        let overflow_page =
            u32::from_le_bytes([slot_data[5], slot_data[6], slot_data[7], slot_data[8]]);
        let first_chunk = &slot_data[HEAP_SLOT_HEADER_SIZE..];

        let mut result = Vec::with_capacity(total_length);
        result.extend_from_slice(first_chunk);

        drop(page);
        drop(guard);

        // Follow overflow chain if present
        if flags & HAS_OVERFLOW != 0 {
            let mut next_page = overflow_page;
            let mut chain_count = 0;

            while next_page != 0 {
                chain_count += 1;
                if chain_count > MAX_OVERFLOW_CHAIN {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "overflow chain exceeds maximum length",
                    ));
                }

                let ov_guard = self.buffer_pool.fetch_page_shared(next_page)?;
                let ov_page = SlottedPageRef::from_buf(ov_guard.data());

                // Validate page type
                if ov_page.page_type() != PageType::Overflow {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("expected overflow page at {}, got {:?}", next_page, ov_page.page_type()),
                    ));
                }

                // next_overflow_page is stored in prev_or_ptr
                let next_overflow = ov_page.prev_or_ptr();

                // Read data_length (u32 LE) at PAGE_HEADER_SIZE offset
                let buf = ov_guard.data();
                if buf.len() < PAGE_HEADER_SIZE + OVERFLOW_DATA_LEN_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "overflow page too small for data_length field",
                    ));
                }
                let data_len = u32::from_le_bytes([
                    buf[PAGE_HEADER_SIZE],
                    buf[PAGE_HEADER_SIZE + 1],
                    buf[PAGE_HEADER_SIZE + 2],
                    buf[PAGE_HEADER_SIZE + 3],
                ]) as usize;

                let data_start = PAGE_HEADER_SIZE + OVERFLOW_DATA_LEN_SIZE;
                let data_end = data_start + data_len;
                if data_end > buf.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "overflow page {} data_length {} exceeds page bounds",
                            next_page, data_len
                        ),
                    ));
                }

                result.extend_from_slice(&buf[data_start..data_end]);

                next_page = next_overflow;
            }
        }

        // Verify total bytes
        if result.len() != total_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "blob length mismatch: expected {}, got {}",
                    total_length,
                    result.len()
                ),
            ));
        }

        Ok(result)
    }

    /// Free a blob and reclaim its pages.
    pub fn free(&mut self, href: HeapRef, free_list: &mut FreeList) -> io::Result<()> {
        // Read the slot to find overflow chain info
        let (flags, overflow_page) = {
            let guard = self.buffer_pool.fetch_page_shared(href.page_id)?;
            let page = SlottedPageRef::from_buf(guard.data());

            if href.slot_id >= page.num_slots() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "slot {} out of range (num_slots {}) on page {}",
                        href.slot_id,
                        page.num_slots(),
                        href.page_id,
                    ),
                ));
            }

            let slot_data = page.slot_data(href.slot_id);
            if slot_data.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "slot {} on page {} is already deleted",
                        href.slot_id, href.page_id,
                    ),
                ));
            }

            if slot_data.len() < HEAP_SLOT_HEADER_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "heap slot too small for header",
                ));
            }

            let flags = slot_data[0];
            let overflow_page =
                u32::from_le_bytes([slot_data[5], slot_data[6], slot_data[7], slot_data[8]]);

            (flags, overflow_page)
        };

        // Free overflow pages
        if flags & HAS_OVERFLOW != 0 {
            let mut next_page = overflow_page;
            let mut chain_count = 0;

            while next_page != 0 {
                chain_count += 1;
                if chain_count > MAX_OVERFLOW_CHAIN {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "overflow chain exceeds maximum length during free",
                    ));
                }

                // Read the next pointer before deallocating
                let next_overflow = {
                    let guard = self.buffer_pool.fetch_page_shared(next_page)?;
                    let page = SlottedPageRef::from_buf(guard.data());
                    page.prev_or_ptr()
                };

                free_list.deallocate(next_page)?;
                next_page = next_overflow;
            }
        }

        // Delete the slot in the heap page
        let should_deallocate_page = {
            let mut guard = self.buffer_pool.fetch_page_exclusive(href.page_id)?;
            {
                let buf = guard.data_mut();
                let mut page = SlottedPage::from_buf(buf);
                page.delete_slot(href.slot_id);
            }
            guard.mark_dirty();

            // Check if page is now completely empty (all slots are tombstones)
            let page = SlottedPageRef::from_buf(guard.data());
            let num_slots = page.num_slots();
            let all_deleted = (0..num_slots).all(|i| page.slot_data(i).is_empty());

            let free = page.free_space();

            if all_deleted {
                true
            } else {
                // Update free space map with new free space
                self.free_space_map.insert(href.page_id, free);
                false
            }
        };

        if should_deallocate_page {
            self.free_space_map.remove(&href.page_id);
            free_list.deallocate(href.page_id)?;
        }

        Ok(())
    }

    /// Rebuild the free space map by scanning all pages.
    ///
    /// Called on startup after recovery. Iterates all pages (0..page_count),
    /// checks each page header; if page_type == Heap, records (page_id, free_space).
    pub fn rebuild_free_space_map(&mut self) -> io::Result<()> {
        self.free_space_map.clear();

        let page_count = self.buffer_pool.page_storage().page_count();

        for pid in 0..page_count as PageId {
            let guard = self.buffer_pool.fetch_page_shared(pid)?;
            let page = SlottedPageRef::from_buf(guard.data());

            if page.page_type() == PageType::Heap {
                let free = page.free_space();
                self.free_space_map.insert(pid, free);
            }
        }

        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────

    /// Build an overflow chain for the given remaining data.
    /// Returns the PageId of the first overflow page in the chain.
    ///
    /// Pages are allocated and written back-to-front: the last overflow page
    /// is written first (with next=0), then the second-to-last (with next=last), etc.
    fn build_overflow_chain(
        &self,
        data: &[u8],
        overflow_page_capacity: usize,
        free_list: &mut FreeList,
    ) -> io::Result<PageId> {
        // Split data into chunks
        let chunks: Vec<&[u8]> = data.chunks(overflow_page_capacity).collect();

        if chunks.len() > MAX_OVERFLOW_CHAIN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data requires too many overflow pages",
            ));
        }

        // Build chain back-to-front
        let mut next_page: PageId = 0;

        for chunk in chunks.iter().rev() {
            let ov_page_id = free_list.allocate()?;

            let mut guard = self.buffer_pool.new_page(ov_page_id)?;
            {
                let buf = guard.data_mut();
                // Initialize the page as an Overflow page
                let mut page = SlottedPage::init(buf, ov_page_id, PageType::Overflow);
                // Store next_overflow_page in prev_or_ptr
                page.set_prev_or_ptr(next_page);
            }

            // Write data_length (u32 LE) at PAGE_HEADER_SIZE
            let buf = guard.data_mut();
            let data_len = chunk.len() as u32;
            buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
                .copy_from_slice(&data_len.to_le_bytes());

            // Write chunk data after the data_length field
            let data_start = PAGE_HEADER_SIZE + OVERFLOW_DATA_LEN_SIZE;
            buf[data_start..data_start + chunk.len()].copy_from_slice(chunk);

            guard.mark_dirty();
            drop(guard);

            next_page = ov_page_id;
        }

        Ok(next_page)
    }

    /// Find a heap page with enough free space, or allocate a new one.
    fn find_or_allocate_heap_page(
        &mut self,
        needed_space: usize,
        free_list: &mut FreeList,
    ) -> io::Result<PageId> {
        // Search the free space map for a page with enough space
        let candidate = self
            .free_space_map
            .iter()
            .find(|(_, free)| **free >= needed_space)
            .map(|(&pid, _)| pid);

        if let Some(page_id) = candidate {
            return Ok(page_id);
        }

        // No existing page has enough space; allocate a new heap page
        let page_id = free_list.allocate()?;

        let mut guard = self.buffer_pool.new_page(page_id)?;
        {
            let buf = guard.data_mut();
            SlottedPage::init(buf, page_id, PageType::Heap);
        }
        guard.mark_dirty();

        // Record the initial free space
        let free = SlottedPageRef::from_buf(guard.data()).free_space();
        self.free_space_map.insert(page_id, free);
        drop(guard);

        Ok(page_id)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, PageStorage};
    use crate::buffer_pool::BufferPoolConfig;

    const PAGE_SIZE: usize = 8192;

    /// Helper: create MemoryPageStorage + BufferPool + FreeList + Heap.
    /// Page 0 is pre-allocated and reserved for the file header.
    fn setup() -> (Heap, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(1).unwrap(); // page 0 reserved

        // Initialize page 0 as FileHeader so it's not confused during scans
        {
            let mut buf = vec![0u8; PAGE_SIZE];
            SlottedPage::init(&mut buf, 0, PageType::FileHeader);
            storage.write_page(0, &buf).unwrap();
        }

        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 256,
            },
            storage,
        ));

        let free_list = FreeList::new(0, pool.clone());
        let heap = Heap::new(pool.clone());

        (heap, free_list)
    }

    // ─── Test 1: Store + load small blob ───

    #[test]
    fn test_store_load_small_blob() {
        let (mut heap, mut free_list) = setup();

        let data: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
        let href = heap.store(&data, &mut free_list).unwrap();
        let loaded = heap.load(href).unwrap();

        assert_eq!(loaded, data);
    }

    // ─── Test 2: Store + load page-sized blob ───

    #[test]
    fn test_store_load_page_sized_blob() {
        let (mut heap, mut free_list) = setup();

        // Maximum data that fits in one slot:
        // page_size - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE - HEAP_SLOT_HEADER_SIZE
        let max_first_chunk = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE - HEAP_SLOT_HEADER_SIZE;
        let data: Vec<u8> = (0..max_first_chunk).map(|i| (i % 256) as u8).collect();
        let href = heap.store(&data, &mut free_list).unwrap();
        let loaded = heap.load(href).unwrap();

        assert_eq!(loaded, data);
    }

    // ─── Test 3: Store + load large blob (overflow) ───

    #[test]
    fn test_store_load_large_blob() {
        let (mut heap, mut free_list) = setup();

        // 50 KB blob - requires overflow
        let size = 50 * 1024;
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let href = heap.store(&data, &mut free_list).unwrap();
        let loaded = heap.load(href).unwrap();

        assert_eq!(loaded.len(), data.len());
        assert_eq!(loaded, data);
    }

    // ─── Test 4: Store + load maximum blob ───

    #[test]
    fn test_store_load_maximum_blob() {
        let (mut heap, mut free_list) = setup();

        // 1 MB blob with overflow chain
        let size = 1024 * 1024;
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let href = heap.store(&data, &mut free_list).unwrap();
        let loaded = heap.load(href).unwrap();

        assert_eq!(loaded.len(), data.len());
        assert_eq!(loaded, data);
    }

    // ─── Test 5: Free small blob ───

    #[test]
    fn test_free_small_blob() {
        let (mut heap, mut free_list) = setup();

        let data = vec![42u8; 100];
        let href = heap.store(&data, &mut free_list).unwrap();

        // Verify it loads
        assert_eq!(heap.load(href).unwrap(), data);

        // Free it
        heap.free(href, &mut free_list).unwrap();

        // Loading should now fail (slot is deleted/tombstone)
        let result = heap.load(href);
        assert!(result.is_err());
    }

    // ─── Test 6: Free large blob with overflow ───

    #[test]
    fn test_free_large_blob_with_overflow() {
        let (mut heap, mut free_list) = setup();

        // 50 KB blob requires overflow pages
        let data: Vec<u8> = (0..50 * 1024).map(|i| (i % 256) as u8).collect();
        let href = heap.store(&data, &mut free_list).unwrap();

        // Count pages allocated (file extensions)
        let pages_before_free = heap.buffer_pool.page_storage().page_count();

        // Free the blob
        heap.free(href, &mut free_list).unwrap();

        // Overflow pages should have been returned to the free list
        let free_count = free_list.count().unwrap();
        assert!(
            free_count > 0,
            "overflow pages should have been deallocated to free list"
        );

        // Loading should fail
        let result = heap.load(href);
        assert!(result.is_err());

        // Total pages should remain the same (pages are deallocated, not removed from file)
        assert_eq!(
            heap.buffer_pool.page_storage().page_count(),
            pages_before_free
        );
    }

    // ─── Test 7: Multiple blobs ───

    #[test]
    fn test_multiple_blobs() {
        let (mut heap, mut free_list) = setup();

        let mut refs = Vec::new();
        let mut data_sets = Vec::new();

        // Store 20 blobs of varying sizes
        for i in 0..20 {
            let size = 50 + i * 500; // sizes from 50 to 9550 bytes
            let data: Vec<u8> = (0..size).map(|j| ((i + j) % 256) as u8).collect();
            let href = heap.store(&data, &mut free_list).unwrap();
            refs.push(href);
            data_sets.push(data);
        }

        // Load all and verify
        for (i, href) in refs.iter().enumerate() {
            let loaded = heap.load(*href).unwrap();
            assert_eq!(
                loaded, data_sets[i],
                "blob {} mismatch (size {})",
                i,
                data_sets[i].len()
            );
        }
    }

    // ─── Test 8: Free space reuse ───

    #[test]
    fn test_free_space_reuse() {
        let (mut heap, mut free_list) = setup();

        // Store blob A
        let data_a = vec![0xAAu8; 200];
        let href_a = heap.store(&data_a, &mut free_list).unwrap();
        // Free blob A
        heap.free(href_a, &mut free_list).unwrap();

        // Store blob B (similar size) - should reuse the freed page (via free list)
        let data_b = vec![0xBBu8; 200];
        let href_b = heap.store(&data_b, &mut free_list).unwrap();

        // Verify blob B is correct
        let loaded = heap.load(href_b).unwrap();
        assert_eq!(loaded, data_b);

        // The page should have been reused (either same heap page or free list page reused).
        // We can verify by checking that no additional pages were allocated beyond what's needed.
        // The key invariant: blob B is stored and loads correctly.
    }

    // ─── Test 9: rebuild_free_space_map ───

    #[test]
    fn test_rebuild_free_space_map() {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(1).unwrap(); // page 0 reserved

        // Initialize page 0 as FileHeader
        {
            let mut buf = vec![0u8; PAGE_SIZE];
            SlottedPage::init(&mut buf, 0, PageType::FileHeader);
            storage.write_page(0, &buf).unwrap();
        }

        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 256,
            },
            storage.clone(),
        ));

        let mut free_list = FreeList::new(0, pool.clone());

        // Store some blobs with the first Heap instance
        let href1;
        let href2;
        {
            let mut heap1 = Heap::new(pool.clone());
            href1 = heap1.store(&vec![1u8; 100], &mut free_list).unwrap();
            href2 = heap1.store(&vec![2u8; 200], &mut free_list).unwrap();
        }

        // Create a new Heap instance (simulating restart) with empty free space map
        let mut heap2 = Heap::new(pool.clone());
        assert!(heap2.free_space_map.is_empty());

        // Rebuild free space map
        heap2.rebuild_free_space_map().unwrap();

        // Verify the free space map is not empty (should have found heap pages)
        assert!(!heap2.free_space_map.is_empty());

        // Verify we can still load existing blobs
        assert_eq!(heap2.load(href1).unwrap(), vec![1u8; 100]);
        assert_eq!(heap2.load(href2).unwrap(), vec![2u8; 200]);

        // Verify store works with rebuilt map (finds free space)
        let href3 = heap2.store(&vec![3u8; 150], &mut free_list).unwrap();
        assert_eq!(heap2.load(href3).unwrap(), vec![3u8; 150]);
    }

    // ─── Test 10: HeapRef serialization ───

    #[test]
    fn test_heap_ref_serialization() {
        let href = HeapRef {
            page_id: 0x12345678,
            slot_id: 0xABCD,
        };
        let bytes = href.to_bytes();
        let decoded = HeapRef::from_bytes(&bytes);
        assert_eq!(decoded, href);
        assert_eq!(decoded.page_id, 0x12345678);
        assert_eq!(decoded.slot_id, 0xABCD);

        // Also test with zero values
        let href_zero = HeapRef {
            page_id: 0,
            slot_id: 0,
        };
        let bytes_zero = href_zero.to_bytes();
        assert_eq!(bytes_zero, [0u8; 6]);
        let decoded_zero = HeapRef::from_bytes(&bytes_zero);
        assert_eq!(decoded_zero, href_zero);

        // Test with max values
        let href_max = HeapRef {
            page_id: u32::MAX,
            slot_id: u16::MAX,
        };
        let bytes_max = href_max.to_bytes();
        let decoded_max = HeapRef::from_bytes(&bytes_max);
        assert_eq!(decoded_max, href_max);
    }

    // ─── Test 11: Empty heap operations ───

    #[test]
    fn test_empty_heap_load_error() {
        let (heap, _free_list) = setup();

        // Load from a non-existent HeapRef (page 0 is FileHeader, not Heap)
        // This should fail because page 0 doesn't have the right slot
        let href = HeapRef {
            page_id: 0,
            slot_id: 0,
        };
        let result = heap.load(href);
        assert!(result.is_err(), "loading from non-existent HeapRef should error");

        // Also test with a page_id that doesn't exist at all
        let href_bad = HeapRef {
            page_id: 999,
            slot_id: 0,
        };
        let result2 = heap.load(href_bad);
        assert!(
            result2.is_err(),
            "loading from non-existent page should error"
        );
    }
}
