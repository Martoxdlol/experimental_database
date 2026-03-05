use std::io;
use std::path::Path;
use std::sync::Arc;

use exdb_storage::catalog_btree::{self, CatalogEntityType, CatalogIndexState};
use exdb_storage::engine::{FileHeader, StorageConfig, StorageEngine};
use exdb_storage::heap::HeapRef;
use exdb_storage::page::{PageType, SlottedPage, SlottedPageRef};
use exdb_storage::recovery::NoOpHandler;

// ─── Display types ───

/// Decoded page information for the UI.
pub struct PageInfo {
    pub page_id: u32,
    pub page_type: Option<PageType>,
    pub page_type_raw: u8,
    pub flags: u8,
    pub num_slots: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub prev_or_ptr: u32,
    pub checksum: u32,
    pub checksum_valid: bool,
    pub lsn: u64,
    pub free_space: usize,
    pub raw_bytes: Vec<u8>,
    pub slots: Vec<SlotInfo>,
}

/// A single slot in a slotted page.
pub struct SlotInfo {
    pub index: u16,
    pub offset: u16,
    pub length: u16,
    pub data: Vec<u8>,
}

/// WAL segment summary.
pub struct WalSegmentInfo {
    pub oldest_lsn: Option<u64>,
    pub current_lsn: u64,
    pub retained_size: u64,
    pub total_size: u64,
}

/// A single WAL record for display.
pub struct WalFrameInfo {
    pub lsn: u64,
    pub record_type: u8,
    pub record_type_name: String,
    pub payload: Vec<u8>,
}

/// B-tree node info for the tree viewer.
pub struct BTreeNodeInfo {
    pub page_id: u32,
    pub is_leaf: bool,
    pub num_keys: u16,
    pub entries: Vec<BTreeEntryInfo>,
    pub children: Vec<u32>,
}

pub struct BTreeEntryInfo {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Collection info from the catalog.
pub struct CollectionInfo {
    pub id: u64,
    pub name: String,
    pub data_root_page: u32,
    pub doc_count: u64,
    pub indexes: Vec<IndexInfo>,
}

/// Index info from the catalog.
pub struct IndexInfo {
    pub id: u64,
    pub name: String,
    pub root_page: u32,
    pub status: String,
    pub field_paths: Vec<Vec<String>>,
}

/// Free list chain.
pub struct FreeListInfo {
    pub head: u32,
    pub chain: Vec<u32>,
}

/// Heap slot info.
pub struct HeapSlotInfo {
    pub page_id: u32,
    pub slot_id: u16,
    pub flags: u8,
    pub total_length: u32,
    pub first_chunk: Vec<u8>,
    pub overflow_chain: Vec<u32>,
}

// ─── DbHandle ───

/// Full read-write handle to an open database.
pub struct DbHandle {
    engine: Arc<StorageEngine>,
    pub path: String,
    pub page_size: usize,
}

impl DbHandle {
    /// Open a database directory with full read-write access.
    pub fn open(path: &Path) -> io::Result<Self> {
        let config = StorageConfig::default();
        let page_size = config.page_size;
        let engine = StorageEngine::open(path, config, &mut NoOpHandler)?;
        Ok(Self {
            engine: Arc::new(engine),
            path: path.display().to_string(),
            page_size,
        })
    }

    /// Close the database (checkpoint + shutdown).
    pub async fn close(&self) -> io::Result<()> {
        self.engine.close().await
    }

    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }

    // ─── FileHeader ───

    pub fn read_file_header(&self) -> FileHeader {
        self.engine.file_header()
    }

    pub fn update_file_header_field(&self, field: &str, value: u64) -> io::Result<()> {
        use zerocopy::byteorder::LittleEndian;
        use zerocopy::U32;
        use zerocopy::U64;
        self.engine.update_file_header(|fh| match field {
            "page_count" => fh.page_count = U64::<LittleEndian>::new(value),
            "free_list_head" => fh.free_list_head = U32::<LittleEndian>::new(value as u32),
            "catalog_root_page" => {
                fh.catalog_root_page = U32::<LittleEndian>::new(value as u32)
            }
            "catalog_name_root_page" => {
                fh.catalog_name_root_page = U32::<LittleEndian>::new(value as u32)
            }
            "checkpoint_lsn" => fh.checkpoint_lsn = U64::<LittleEndian>::new(value),
            "visible_ts" => fh.visible_ts = U64::<LittleEndian>::new(value),
            "generation" => fh.generation = U64::<LittleEndian>::new(value),
            "next_collection_id" => fh.next_collection_id = U64::<LittleEndian>::new(value),
            "next_index_id" => fh.next_index_id = U64::<LittleEndian>::new(value),
            _ => {}
        })
    }

    // ─── Page Access ───

    pub fn page_count(&self) -> u64 {
        self.engine.buffer_pool().page_storage().page_count()
    }

    pub fn read_page(&self, page_id: u32) -> io::Result<PageInfo> {
        let guard = self.engine.buffer_pool().fetch_page_shared(page_id)?;
        let data = guard.data();
        let raw_bytes = data.to_vec();
        let page = SlottedPageRef::from_buf(data)?;
        let header = page.header();
        let checksum_valid = page.verify_checksum();
        let page_type = page.try_page_type();
        let num_slots = page.num_slots();

        let mut slots = Vec::new();
        for i in 0..num_slots {
            let slot_data = page.slot_data(i);
            // Compute offset from raw bytes (slot directory entry)
            let dir_offset = 32 + (i as usize) * 4; // header is 32 bytes, each slot entry is 4 bytes
            let offset = if dir_offset + 3 < raw_bytes.len() {
                u16::from_le_bytes([raw_bytes[dir_offset], raw_bytes[dir_offset + 1]])
            } else {
                0
            };
            slots.push(SlotInfo {
                index: i,
                offset,
                length: slot_data.len() as u16,
                data: slot_data.to_vec(),
            });
        }

        Ok(PageInfo {
            page_id,
            page_type,
            page_type_raw: header.page_type,
            flags: header.flags,
            num_slots,
            free_space_start: header.free_space_start.get(),
            free_space_end: header.free_space_end.get(),
            prev_or_ptr: header.prev_or_ptr.get(),
            checksum: header.checksum.get(),
            checksum_valid,
            lsn: header.lsn.get(),
            free_space: page.free_space(),
            raw_bytes,
            slots,
        })
    }

    pub fn scan_page_types(&self) -> io::Result<Vec<(u32, Option<PageType>, u8)>> {
        let count = self.page_count();
        let mut result = Vec::new();
        for i in 0..count as u32 {
            let guard = self.engine.buffer_pool().fetch_page_shared(i)?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            let header = page.header();
            result.push((i, page.try_page_type(), header.page_type));
        }
        Ok(result)
    }

    // ─── Page Write ───

    pub fn write_page_raw(&self, page_id: u32, bytes: &[u8]) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let len = buf.len().min(bytes.len());
        buf[..len].copy_from_slice(&bytes[..len]);
        Ok(())
    }

    pub fn init_page(&self, page_id: u32, page_type: PageType) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        SlottedPage::init(buf, page_id, page_type);
        Ok(())
    }

    pub fn insert_slot(&self, page_id: u32, data: &[u8]) -> io::Result<u16> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.insert_slot(data)
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    pub fn update_slot(&self, page_id: u32, slot: u16, data: &[u8]) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.update_slot(slot, data)
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    pub fn delete_slot(&self, page_id: u32, slot: u16) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.delete_slot(slot);
        Ok(())
    }

    pub fn compact_page(&self, page_id: u32) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.compact();
        Ok(())
    }

    pub fn stamp_checksum(&self, page_id: u32) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id)?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.stamp_checksum();
        Ok(())
    }

    // ─── B-Tree ───

    pub fn create_btree(&self) -> io::Result<u32> {
        let handle = self.engine.create_btree()?;
        Ok(handle.root_page())
    }

    pub fn btree_insert(&self, root_page: u32, key: &[u8], value: &[u8]) -> io::Result<()> {
        let handle = self.engine.open_btree(root_page);
        handle.insert(key, value)
    }

    pub fn btree_delete(&self, root_page: u32, key: &[u8]) -> io::Result<bool> {
        let handle = self.engine.open_btree(root_page);
        handle.delete(key)
    }

    pub fn btree_get(&self, root_page: u32, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let handle = self.engine.open_btree(root_page);
        handle.get(key)
    }

    pub fn btree_scan(
        &self,
        root_page: u32,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use exdb_storage::btree::ScanDirection;
        use std::ops::Bound;
        let handle = self.engine.open_btree(root_page);
        let iter = handle.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward);
        let mut results = Vec::new();
        for entry in iter {
            let (k, v) = entry?;
            results.push((k, v));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    pub fn read_btree_node(&self, page_id: u32) -> io::Result<BTreeNodeInfo> {
        let guard = self.engine.buffer_pool().fetch_page_shared(page_id)?;
        let page = SlottedPageRef::from_buf(guard.data())?;
        let pt = page.try_page_type();
        let is_leaf = pt == Some(PageType::BTreeLeaf);
        let num_slots = page.num_slots();
        let mut entries = Vec::new();
        let mut children = Vec::new();

        if is_leaf {
            for i in 0..num_slots {
                let data = page.slot_data(i);
                if data.len() >= 2 {
                    let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
                    if data.len() >= 2 + key_len {
                        let key = data[2..2 + key_len].to_vec();
                        let value = data[2 + key_len..].to_vec();
                        entries.push(BTreeEntryInfo { key, value });
                    }
                }
            }
        } else {
            // Internal node: first slot is the leftmost child pointer
            // Then alternating key, child_page pairs
            for i in 0..num_slots {
                let data = page.slot_data(i);
                if i == 0 && data.len() >= 4 {
                    // Leftmost child pointer
                    let child = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    children.push(child);
                } else if data.len() >= 2 {
                    let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
                    if data.len() >= 2 + key_len + 4 {
                        let key = data[2..2 + key_len].to_vec();
                        let child_bytes = &data[2 + key_len..2 + key_len + 4];
                        let child =
                            u32::from_le_bytes([child_bytes[0], child_bytes[1], child_bytes[2], child_bytes[3]]);
                        entries.push(BTreeEntryInfo {
                            key,
                            value: child_bytes.to_vec(),
                        });
                        children.push(child);
                    }
                }
            }
        }

        Ok(BTreeNodeInfo {
            page_id,
            is_leaf,
            num_keys: entries.len() as u16,
            entries,
            children,
        })
    }

    // ─── WAL ───

    pub fn wal_info(&self) -> WalSegmentInfo {
        let wal_storage = self.engine.wal_writer();
        let current_lsn = wal_storage.current_lsn();
        // Read from engine's wal reader
        WalSegmentInfo {
            oldest_lsn: None,
            current_lsn,
            retained_size: 0,
            total_size: 0,
        }
    }

    pub fn read_wal_frames(&self, from_lsn: u64, limit: usize) -> Vec<WalFrameInfo> {
        let mut iter = self.engine.read_wal_from(from_lsn);
        let mut frames = Vec::new();
        for record in &mut iter {
            match record {
                Ok(r) => {
                    frames.push(WalFrameInfo {
                        lsn: r.lsn,
                        record_type: r.record_type,
                        record_type_name: wal_record_type_name(r.record_type),
                        payload: r.payload,
                    });
                    if frames.len() >= limit {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        frames
    }

    pub async fn append_wal(&self, record_type: u8, payload: &[u8]) -> io::Result<u64> {
        self.engine.append_wal(record_type, payload).await
    }

    // ─── Heap ───

    pub fn heap_store(&self, data: &[u8]) -> io::Result<(u32, u16)> {
        let href = self.engine.heap_store(data)?;
        Ok((href.page_id, href.slot_id))
    }

    pub fn heap_load(&self, page_id: u32, slot_id: u16) -> io::Result<Vec<u8>> {
        let href = HeapRef { page_id, slot_id };
        self.engine.heap_load(href)
    }

    pub fn heap_free(&self, page_id: u32, slot_id: u16) -> io::Result<()> {
        let href = HeapRef { page_id, slot_id };
        self.engine.heap_free(href)
    }

    // ─── Free List ───

    pub fn walk_free_list(&self) -> io::Result<FreeListInfo> {
        let fh = self.engine.file_header();
        let head = fh.free_list_head.get();
        let mut chain = Vec::new();
        let mut current = head;
        while current != 0 {
            chain.push(current);
            let guard = self.engine.buffer_pool().fetch_page_shared(current)?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            current = page.prev_or_ptr();
            if chain.len() > 10000 {
                break; // safety limit
            }
        }
        Ok(FreeListInfo { head, chain })
    }

    pub fn free_list_allocate(&self) -> io::Result<u32> {
        let mut fl = self.engine.free_list().lock();
        fl.allocate()
    }

    pub fn free_list_deallocate(&self, page_id: u32) -> io::Result<()> {
        let mut fl = self.engine.free_list().lock();
        fl.deallocate(page_id)
    }

    // ─── Catalog ───

    pub fn list_collections(&self) -> io::Result<Vec<CollectionInfo>> {
        let fh = self.engine.file_header();
        let catalog_root = fh.catalog_root_page.get();
        if catalog_root == 0 {
            return Ok(Vec::new());
        }

        let tree = self.engine.open_btree(catalog_root);
        let mut collections = Vec::new();

        // Scan all entries with collection entity type prefix
        use std::ops::Bound;
        let prefix = [CatalogEntityType::Collection as u8];
        let start = prefix.to_vec();
        let mut end = start.clone();
        end[0] += 1;

        let iter = tree.scan(
            Bound::Included(&start[..]),
            Bound::Excluded(&end[..]),
            exdb_storage::btree::ScanDirection::Forward,
        );

        for entry in iter {
            let (_key, value) = entry?;
            if let Ok(ce) = catalog_btree::deserialize_collection(&value) {
                // Now find indexes for this collection
                let indexes = self.list_indexes_for(catalog_root, ce.collection_id)?;
                collections.push(CollectionInfo {
                    id: ce.collection_id,
                    name: ce.name,
                    data_root_page: ce.primary_root_page,
                    doc_count: ce.doc_count,
                    indexes,
                });
            }
        }

        Ok(collections)
    }

    fn list_indexes_for(&self, catalog_root: u32, collection_id: u64) -> io::Result<Vec<IndexInfo>> {
        let tree = self.engine.open_btree(catalog_root);
        let mut indexes = Vec::new();

        use std::ops::Bound;
        let prefix = [CatalogEntityType::Index as u8];
        let start = prefix.to_vec();
        let mut end = start.clone();
        end[0] += 1;

        let iter = tree.scan(
            Bound::Included(&start[..]),
            Bound::Excluded(&end[..]),
            exdb_storage::btree::ScanDirection::Forward,
        );

        for entry in iter {
            let (_, value) = entry?;
            if let Ok(ie) = catalog_btree::deserialize_index(&value)
                && ie.collection_id == collection_id {
                    indexes.push(IndexInfo {
                        id: ie.index_id,
                        name: ie.name,
                        root_page: ie.root_page,
                        status: match ie.state {
                            CatalogIndexState::Ready => "Ready".to_string(),
                            CatalogIndexState::Building => "Building".to_string(),
                            CatalogIndexState::Dropping => "Dropping".to_string(),
                        },
                        field_paths: ie.field_paths,
                    });
                }
        }

        Ok(indexes)
    }

    // ─── Checkpoint + Maintenance ───

    pub async fn checkpoint(&self) -> io::Result<()> {
        self.engine.checkpoint().await
    }

    pub fn dirty_page_count(&self) -> usize {
        self.engine.buffer_pool().dirty_pages().len()
    }
}

fn wal_record_type_name(t: u8) -> String {
    match t {
        0x01 => "TX_COMMIT".to_string(),
        0x02 => "CHECKPOINT".to_string(),
        0x03 => "CREATE_COLLECTION".to_string(),
        0x04 => "DROP_COLLECTION".to_string(),
        0x05 => "CREATE_INDEX".to_string(),
        0x06 => "DROP_INDEX".to_string(),
        0x07 => "INDEX_READY".to_string(),
        0x08 => "VACUUM".to_string(),
        0x09 => "VISIBLE_TS".to_string(),
        0x0A => "ROLLBACK_VACUUM".to_string(),
        _ => format!("UNKNOWN(0x{t:02X})"),
    }
}
