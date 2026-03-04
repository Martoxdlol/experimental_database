use std::collections::BTreeMap;
use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError};
use crate::free_list::FreePageList;

/// In-memory map of heap pages with available space.
#[derive(Default)]
pub struct HeapFreeSpaceMap {
    pages: BTreeMap<PageId, u16>,
}

impl HeapFreeSpaceMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn find_page(&self, needed: u16) -> Option<PageId> {
        // Best-fit: smallest sufficient free space.
        let mut best: Option<(PageId, u16)> = None;
        for (&pid, &free) in &self.pages {
            if free >= needed {
                match best {
                    None => best = Some((pid, free)),
                    Some((_, bf)) if free < bf => best = Some((pid, free)),
                    _ => {}
                }
            }
        }
        best.map(|(pid, _)| pid)
    }

    pub fn update(&mut self, page_id: PageId, free_bytes: u16) {
        if free_bytes > 0 {
            self.pages.insert(page_id, free_bytes);
        } else {
            self.pages.remove(&page_id);
        }
    }

    pub fn remove(&mut self, page_id: PageId) {
        self.pages.remove(&page_id);
    }
}

/// Manages external heap storage for large documents.
pub struct ExternalHeap {
    free_space_map: HeapFreeSpaceMap,
    page_size: u32,
    external_threshold: u32,
}

impl ExternalHeap {
    pub fn new(page_size: u32, external_threshold: u32) -> Self {
        Self {
            free_space_map: HeapFreeSpaceMap::new(),
            page_size,
            external_threshold,
        }
    }

    /// Store a document body in the heap. Returns the HeapRef.
    pub fn store(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        body: &[u8],
        lsn: Lsn,
    ) -> Result<HeapRef, BufferPoolError> {
        // Slot data: [body_length: u32 LE] [body]
        let slot_data_len = 4 + body.len();
        let usable_per_slot = self.page_size as usize - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE;

        if slot_data_len <= usable_per_slot {
            // Single-page mode.
            let mut slot_data = Vec::with_capacity(slot_data_len);
            slot_data.extend_from_slice(&(body.len() as u32).to_le_bytes());
            slot_data.extend_from_slice(body);

            // Find a heap page with space.
            if let Some(page_id) = self.free_space_map.find_page((slot_data_len + SLOT_ENTRY_SIZE) as u16) {
                let mut guard = pool.fetch_page_exclusive(page_id)?;
                let slot_id = {
                    let sp = guard.as_slotted_page();
                    sp.num_slots()
                };
                {
                    let mut sp = guard.as_slotted_page_mut();
                    sp.insert_cell(slot_id, &slot_data)
                        .map_err(|_| BufferPoolError::BufferPoolFull)?;
                    sp.set_lsn(lsn);
                    sp.compute_checksum();
                    let free = sp.free_space() as u16;
                    self.free_space_map.update(page_id, free);
                }
                return Ok(HeapRef { page_id, slot_id });
            }

            // Allocate new heap page.
            let mut guard = pool.new_page(PageType::Heap, free_list)?;
            let page_id = guard.page_id();
            {
                let mut sp = guard.as_slotted_page_mut();
                sp.insert_cell(0, &slot_data)
                    .map_err(|_| BufferPoolError::BufferPoolFull)?;
                sp.set_lsn(lsn);
                sp.compute_checksum();
                let free = sp.free_space() as u16;
                self.free_space_map.update(page_id, free);
            }
            return Ok(HeapRef { page_id, slot_id: 0 });
        }

        // Multi-page mode with overflow pages.
        let overflow_data_per_page = self.page_size as usize - PAGE_HEADER_SIZE - 2;
        let first_chunk_size = usable_per_slot - 4 - 4; // minus body_len, minus overflow_page_id

        // Allocate overflow pages for remaining data.
        let remaining = &body[first_chunk_size..];
        let mut overflow_pages: Vec<PageId> = Vec::new();
        let mut offset = 0;
        while offset < remaining.len() {
            let chunk_end = std::cmp::min(offset + overflow_data_per_page, remaining.len());
            let chunk = &remaining[offset..chunk_end];

            let mut ov_guard = pool.new_page(PageType::Overflow, free_list)?;
            let ov_page_id = ov_guard.page_id();
            {
                let data = ov_guard.data_mut();
                // Write data_length after header.
                let dl = chunk.len() as u16;
                data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2].copy_from_slice(&dl.to_le_bytes());
                data[PAGE_HEADER_SIZE + 2..PAGE_HEADER_SIZE + 2 + chunk.len()].copy_from_slice(chunk);
            }
            overflow_pages.push(ov_page_id);
            offset = chunk_end;
        }

        // Link overflow pages.
        for i in 0..overflow_pages.len() {
            let next = if i + 1 < overflow_pages.len() { overflow_pages[i + 1].0 } else { 0 };
            let mut guard = pool.fetch_page_exclusive(overflow_pages[i])?;
            {
                let mut sp = guard.as_slotted_page_mut();
                sp.set_prev_or_ptr(next);
                sp.set_lsn(lsn);
                sp.compute_checksum();
            }
        }

        // Build first slot data.
        let mut slot_data = Vec::new();
        slot_data.extend_from_slice(&(body.len() as u32).to_le_bytes());
        slot_data.extend_from_slice(&overflow_pages[0].0.to_le_bytes());
        slot_data.extend_from_slice(&body[..first_chunk_size]);

        let mut guard = pool.new_page(PageType::Heap, free_list)?;
        let page_id = guard.page_id();
        {
            let mut sp = guard.as_slotted_page_mut();
            sp.insert_cell(0, &slot_data)
                .map_err(|_| BufferPoolError::BufferPoolFull)?;
            sp.set_lsn(lsn);
            sp.compute_checksum();
        }

        Ok(HeapRef { page_id, slot_id: 0 })
    }

    /// Read a document body from the heap.
    pub fn read(
        &self,
        pool: &BufferPool,
        heap_ref: HeapRef,
    ) -> Result<Vec<u8>, BufferPoolError> {
        let guard = pool.fetch_page_shared(heap_ref.page_id)?;
        let sp = guard.as_slotted_page();
        let slot_data = sp.cell_data(heap_ref.slot_id)
            .ok_or_else(|| BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid heap slot",
            )))?;

        if slot_data.len() < 4 {
            return Err(BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "heap slot too small",
            )));
        }

        let body_len = u32::from_le_bytes(slot_data[0..4].try_into().unwrap()) as usize;

        if slot_data.len() >= 4 + body_len {
            // Single-page: body is right after body_len.
            return Ok(slot_data[4..4 + body_len].to_vec());
        }

        // Multi-page: has overflow pointer.
        if slot_data.len() < 8 {
            return Err(BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing overflow pointer",
            )));
        }
        let overflow_page_id = PageId(u32::from_le_bytes(slot_data[4..8].try_into().unwrap()));
        let first_chunk = &slot_data[8..];

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(first_chunk);
        drop(guard);

        // Follow overflow chain.
        let mut next_page = overflow_page_id;
        while next_page.0 != 0 && body.len() < body_len {
            let ov_guard = pool.fetch_page_shared(next_page)?;
            let data = ov_guard.data();
            let header = crate::page::SlottedPage::new(data, self.page_size).header();
            let dl = u16::from_le_bytes(data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 2].try_into().unwrap()) as usize;
            body.extend_from_slice(&data[PAGE_HEADER_SIZE + 2..PAGE_HEADER_SIZE + 2 + dl]);
            next_page = PageId(header.prev_or_ptr);
        }

        Ok(body)
    }

    /// Delete a document from the heap.
    pub fn delete(
        &mut self,
        pool: &BufferPool,
        free_list: &mut FreePageList,
        heap_ref: HeapRef,
        lsn: Lsn,
    ) -> Result<(), BufferPoolError> {
        // Read slot to check for overflow.
        let guard = pool.fetch_page_shared(heap_ref.page_id)?;
        let sp = guard.as_slotted_page();
        let slot_data = sp.cell_data(heap_ref.slot_id)
            .ok_or_else(|| BufferPoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid heap slot",
            )))?;

        let body_len = u32::from_le_bytes(slot_data[0..4].try_into().unwrap()) as usize;
        let has_overflow = slot_data.len() < 4 + body_len;
        let overflow_start = if has_overflow && slot_data.len() >= 8 {
            Some(PageId(u32::from_le_bytes(slot_data[4..8].try_into().unwrap())))
        } else {
            None
        };
        drop(guard);

        // Free overflow pages.
        if let Some(mut next) = overflow_start {
            while next.0 != 0 {
                let ov_guard = pool.fetch_page_shared(next)?;
                let header = ov_guard.as_slotted_page().header();
                let next_overflow = PageId(header.prev_or_ptr);
                drop(ov_guard);
                free_list.push(next, pool)?;
                next = next_overflow;
            }
        }

        // Delete the heap slot.
        let mut guard = pool.fetch_page_exclusive(heap_ref.page_id)?;
        let (free, num_slots) = {
            let mut sp = guard.as_slotted_page_mut();
            sp.delete_cell(heap_ref.slot_id);
            sp.compact();
            sp.set_lsn(lsn);
            sp.compute_checksum();
            (sp.free_space() as u16, sp.num_slots())
        };
        if num_slots == 0 {
            self.free_space_map.remove(heap_ref.page_id);
            drop(guard);
            free_list.push(heap_ref.page_id, pool)?;
        } else {
            self.free_space_map.update(heap_ref.page_id, free);
        }

        Ok(())
    }

    pub fn external_threshold(&self) -> u32 {
        self.external_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::MemPageIO;

    fn make_pool(frames: u32) -> BufferPool {
        let io = Box::new(MemPageIO::new(DEFAULT_PAGE_SIZE, 2));
        BufferPool::new(io, DEFAULT_PAGE_SIZE, frames)
    }

    #[test]
    fn test_store_and_read_single_page() {
        let pool = make_pool(16);
        let mut fl = FreePageList::new(None);
        let mut heap = ExternalHeap::new(DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE / 2);

        let body = vec![0x42u8; 5000]; // > 4096 threshold, < page usable
        let href = heap.store(&pool, &mut fl, &body, Lsn(1)).unwrap();
        let read_back = heap.read(&pool, href).unwrap();
        assert_eq!(read_back, body);
    }

    #[test]
    fn test_store_and_read_multi_page() {
        let pool = make_pool(32);
        let mut fl = FreePageList::new(None);
        let mut heap = ExternalHeap::new(DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE / 2);

        let body = vec![0xABu8; 20000]; // needs overflow pages
        let href = heap.store(&pool, &mut fl, &body, Lsn(1)).unwrap();
        let read_back = heap.read(&pool, href).unwrap();
        assert_eq!(read_back, body);
    }

    #[test]
    fn test_delete() {
        let pool = make_pool(16);
        let mut fl = FreePageList::new(None);
        let mut heap = ExternalHeap::new(DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE / 2);

        let body = vec![0x42u8; 5000];
        let href = heap.store(&pool, &mut fl, &body, Lsn(1)).unwrap();
        heap.delete(&pool, &mut fl, href, Lsn(2)).unwrap();
        // Reading after delete should fail or return wrong data.
    }
}
