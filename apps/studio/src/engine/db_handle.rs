use std::io;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use exdb_core::types::{DocId, Scalar};
use exdb_docstore::key_encoding;
use exdb_docstore::{CellFlags, PrimaryIndex, SecondaryIndex};
use exdb_storage::btree::ScanDirection;
use exdb_storage::catalog_btree::{self, CatalogEntityType, CatalogIndexState};
use exdb_storage::engine::{FileHeader, StorageConfig, StorageEngine};
use exdb_storage::heap::HeapRef;
use exdb_storage::page::{PageType, SlottedPage, SlottedPageRef};
use exdb_storage::recovery::NoOpHandler;
use tokio_stream::StreamExt;

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
    pub current_lsn: u64,
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

// ─── DbHandle ───

/// Full read-write handle to an open database.
pub struct DbHandle {
    engine: Arc<StorageEngine>,
    pub path: String,
    pub page_size: usize,
}

impl DbHandle {
    /// Access the underlying storage engine (for L4 query operations).
    pub fn engine(&self) -> &Arc<StorageEngine> {
        &self.engine
    }

    /// Open a database directory with full read-write access.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let config = StorageConfig::default();
        let page_size = config.page_size;
        let engine = StorageEngine::open(path, config, &mut NoOpHandler).await?;
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

    // ─── FileHeader ───

    pub async fn read_file_header(&self) -> FileHeader {
        self.engine.file_header().await
    }

    pub async fn update_file_header_field(&self, field: &str, value: u64) -> io::Result<()> {
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
        }).await
    }

    // ─── Page Access ───

    pub fn page_count(&self) -> u64 {
        self.engine.buffer_pool().page_storage().page_count()
    }

    pub async fn read_page(&self, page_id: u32) -> io::Result<PageInfo> {
        let guard = self.engine.buffer_pool().fetch_page_shared(page_id).await?;
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

    pub async fn scan_page_types(&self) -> io::Result<Vec<(u32, Option<PageType>, u8)>> {
        let count = self.page_count();
        let mut result = Vec::new();
        for i in 0..count as u32 {
            let guard = self.engine.buffer_pool().fetch_page_shared(i).await?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            let header = page.header();
            result.push((i, page.try_page_type(), header.page_type));
        }
        Ok(result)
    }

    // ─── Page Write ───

    pub async fn init_page(&self, page_id: u32, page_type: PageType) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        SlottedPage::init(buf, page_id, page_type);
        Ok(())
    }

    pub async fn insert_slot(&self, page_id: u32, data: &[u8]) -> io::Result<u16> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.insert_slot(data)
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    pub async fn update_slot(&self, page_id: u32, slot: u16, data: &[u8]) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.update_slot(slot, data)
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    pub async fn delete_slot(&self, page_id: u32, slot: u16) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.delete_slot(slot);
        Ok(())
    }

    pub async fn compact_page(&self, page_id: u32) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.compact();
        Ok(())
    }

    pub async fn stamp_checksum(&self, page_id: u32) -> io::Result<()> {
        let mut guard = self.engine.buffer_pool().fetch_page_exclusive(page_id).await?;
        let buf = guard.data_mut();
        let mut page = SlottedPage::from_buf(buf)?;
        page.stamp_checksum();
        Ok(())
    }

    // ─── B-Tree ───

    pub async fn create_btree(&self) -> io::Result<u32> {
        let handle = self.engine.create_btree().await?;
        Ok(handle.root_page())
    }

    pub async fn btree_insert(&self, root_page: u32, key: &[u8], value: &[u8]) -> io::Result<()> {
        let handle = self.engine.open_btree(root_page);
        handle.insert(key, value).await
    }

    pub async fn btree_delete(&self, root_page: u32, key: &[u8]) -> io::Result<bool> {
        let handle = self.engine.open_btree(root_page);
        handle.delete(key).await
    }

    pub async fn btree_get(&self, root_page: u32, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let handle = self.engine.open_btree(root_page);
        handle.get(key).await
    }

    pub async fn btree_scan(
        &self,
        root_page: u32,
        limit: usize,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let handle = self.engine.open_btree(root_page);
        let mut scanner = handle.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward);
        let mut results = Vec::new();
        while let Some(entry) = scanner.next().await {
            let (k, v) = entry?;
            results.push((k, v));
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    pub async fn read_btree_node(&self, page_id: u32) -> io::Result<BTreeNodeInfo> {
        let guard = self.engine.buffer_pool().fetch_page_shared(page_id).await?;
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
        WalSegmentInfo { current_lsn }
    }

    pub async fn read_wal_frames(&self, from_lsn: u64, limit: usize) -> Vec<WalFrameInfo> {
        use tokio_stream::StreamExt;
        let mut stream = self.engine.read_wal_from(from_lsn);
        let mut frames = Vec::new();
        while let Some(record) = stream.next().await {
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

    pub async fn heap_store(&self, data: &[u8]) -> io::Result<(u32, u16)> {
        let href = self.engine.heap_store(data).await?;
        Ok((href.page_id, href.slot_id))
    }

    pub async fn heap_load(&self, page_id: u32, slot_id: u16) -> io::Result<Vec<u8>> {
        let href = HeapRef { page_id, slot_id };
        self.engine.heap_load(href).await
    }

    pub async fn heap_free(&self, page_id: u32, slot_id: u16) -> io::Result<()> {
        let href = HeapRef { page_id, slot_id };
        self.engine.heap_free(href).await
    }

    // ─── Free List ───

    pub async fn walk_free_list(&self) -> io::Result<FreeListInfo> {
        let fh = self.engine.file_header().await;
        let head = fh.free_list_head.get();
        let mut chain = Vec::new();
        let mut current = head;
        while current != 0 {
            chain.push(current);
            let guard = self.engine.buffer_pool().fetch_page_shared(current).await?;
            let page = SlottedPageRef::from_buf(guard.data())?;
            current = page.prev_or_ptr();
            if chain.len() > 10000 {
                break; // safety limit
            }
        }
        Ok(FreeListInfo { head, chain })
    }

    pub async fn free_list_allocate(&self) -> io::Result<u32> {
        let mut fl = self.engine.free_list().lock().await;
        fl.allocate().await
    }

    pub async fn free_list_deallocate(&self, page_id: u32) -> io::Result<()> {
        let mut fl = self.engine.free_list().lock().await;
        fl.deallocate(page_id).await
    }

    // ─── Catalog ───

    pub async fn list_collections(&self) -> io::Result<Vec<CollectionInfo>> {
        let fh = self.engine.file_header().await;
        let catalog_root = fh.catalog_root_page.get();
        if catalog_root == 0 {
            return Ok(Vec::new());
        }

        let tree = self.engine.open_btree(catalog_root);
        let mut collections = Vec::new();

        // Scan all entries with collection entity type prefix
        let prefix = [CatalogEntityType::Collection as u8];
        let start = prefix.to_vec();
        let mut end = start.clone();
        end[0] += 1;

        let mut scanner = tree.scan(
            Bound::Included(&start[..]),
            Bound::Excluded(&end[..]),
            exdb_storage::btree::ScanDirection::Forward,
        );

        while let Some(entry) = scanner.next().await {
            let (_key, value) = entry?;
            if let Ok(ce) = catalog_btree::deserialize_collection(&value) {
                // Now find indexes for this collection
                let indexes = self.list_indexes_for(catalog_root, ce.collection_id).await?;
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

    async fn list_indexes_for(&self, catalog_root: u32, collection_id: u64) -> io::Result<Vec<IndexInfo>> {
        let tree = self.engine.open_btree(catalog_root);
        let mut indexes = Vec::new();

        let prefix = [CatalogEntityType::Index as u8];
        let start = prefix.to_vec();
        let mut end = start.clone();
        end[0] += 1;

        let mut scanner = tree.scan(
            Bound::Included(&start[..]),
            Bound::Excluded(&end[..]),
            exdb_storage::btree::ScanDirection::Forward,
        );

        while let Some(entry) = scanner.next().await {
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

    // ─── Catalog Write ───

    pub async fn catalog_create_collection(&self, name: &str) -> io::Result<CollectionInfo> {
        let fh = self.engine.file_header().await;
        let mut catalog_root = fh.catalog_root_page.get();
        let mut name_root = fh.catalog_name_root_page.get();

        // Auto-create catalog B-trees if needed
        if catalog_root == 0 {
            let tree = self.engine.create_btree().await?;
            catalog_root = tree.root_page();
            self.update_file_header_field("catalog_root_page", catalog_root as u64).await?;
        }
        if name_root == 0 {
            let tree = self.engine.create_btree().await?;
            name_root = tree.root_page();
            self.update_file_header_field("catalog_name_root_page", name_root as u64).await?;
        }

        // Allocate IDs
        let fh = self.engine.file_header().await;
        let col_id = fh.next_collection_id.get();
        self.update_file_header_field("next_collection_id", col_id + 1).await?;

        // Create data B-tree for the collection
        let data_tree = self.engine.create_btree().await?;
        let data_root = data_tree.root_page();

        let entry = catalog_btree::CollectionEntry {
            collection_id: col_id,
            name: name.to_string(),
            primary_root_page: data_root,
            doc_count: 0,
        };

        // Insert into by-id catalog
        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, col_id);
        let value = catalog_btree::serialize_collection(&entry);
        let tree = self.engine.open_btree(catalog_root);
        tree.insert(&id_key, &value).await?;

        // Insert into by-name catalog
        let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, name);
        let name_value = catalog_btree::serialize_name_value(col_id);
        let name_tree = self.engine.open_btree(name_root);
        name_tree.insert(&name_key, &name_value).await?;

        Ok(CollectionInfo {
            id: col_id,
            name: name.to_string(),
            data_root_page: data_root,
            doc_count: 0,
            indexes: vec![],
        })
    }

    pub async fn catalog_drop_collection(&self, collection_id: u64, name: &str) -> io::Result<()> {
        let fh = self.engine.file_header().await;
        let catalog_root = fh.catalog_root_page.get();
        let name_root = fh.catalog_name_root_page.get();
        if catalog_root == 0 {
            return Err(io::Error::other("No catalog"));
        }

        // Delete from by-id
        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, collection_id);
        let tree = self.engine.open_btree(catalog_root);
        tree.delete(&id_key).await?;

        // Delete from by-name
        if name_root != 0 {
            let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, name);
            let name_tree = self.engine.open_btree(name_root);
            name_tree.delete(&name_key).await?;
        }

        Ok(())
    }

    pub async fn catalog_create_index(
        &self,
        collection_id: u64,
        name: &str,
        field_paths: Vec<Vec<String>>,
    ) -> io::Result<IndexInfo> {
        let fh = self.engine.file_header().await;
        let catalog_root = fh.catalog_root_page.get();
        if catalog_root == 0 {
            return Err(io::Error::other("No catalog"));
        }

        let idx_id = fh.next_index_id.get();
        self.update_file_header_field("next_index_id", idx_id + 1).await?;

        let idx_tree = self.engine.create_btree().await?;
        let root_page = idx_tree.root_page();

        let entry = catalog_btree::IndexEntry {
            index_id: idx_id,
            collection_id,
            name: name.to_string(),
            field_paths: field_paths.clone(),
            root_page,
            state: CatalogIndexState::Building,
            index_type: catalog_btree::IndexType::BTree,
            aux_root_pages: vec![],
            config: vec![],
        };

        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx_id);
        let value = catalog_btree::serialize_index(&entry);
        let tree = self.engine.open_btree(catalog_root);
        tree.insert(&id_key, &value).await?;

        Ok(IndexInfo {
            id: idx_id,
            name: name.to_string(),
            root_page,
            status: "Building".to_string(),
            field_paths,
        })
    }

    pub async fn catalog_drop_index(&self, index_id: u64) -> io::Result<()> {
        let fh = self.engine.file_header().await;
        let catalog_root = fh.catalog_root_page.get();
        if catalog_root == 0 {
            return Err(io::Error::other("No catalog"));
        }

        let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, index_id);
        let tree = self.engine.open_btree(catalog_root);
        tree.delete(&id_key).await?;
        Ok(())
    }

    // ─── Checkpoint + Maintenance ───

    pub async fn checkpoint(&self) -> io::Result<()> {
        self.engine.checkpoint().await
    }

    pub fn dirty_page_count(&self) -> usize {
        self.engine.buffer_pool().dirty_pages().len()
    }

    // ─── L3 Docstore ───

    fn make_primary_index(&self, root_page: u32) -> PrimaryIndex {
        let btree = self.engine.open_btree(root_page);
        PrimaryIndex::new(btree, self.engine.clone(), self.page_size / 2)
    }

    /// MVCC scan at read_ts, returning decoded documents.
    pub async fn docstore_scan(
        &self,
        root_page: u32,
        read_ts: u64,
        limit: usize,
    ) -> io::Result<Vec<DocstoreEntry>> {
        let primary = self.make_primary_index(root_page);
        let mut scanner = primary.scan_at_ts(read_ts, ScanDirection::Forward);
        let mut results = Vec::new();
        while let Some(item) = scanner.next().await {
            let (doc_id, version_ts, body) = item?;
            results.push(DocstoreEntry {
                doc_id_hex: hex_encode(doc_id.as_bytes()),
                version_ts,
                is_tombstone: false,
                is_external: false, // scan_at_ts resolves external refs
                body_len: body.len() as u32,
                body_json: decode_body_json(&body),
            });
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    /// All versions of a single document (raw, no version resolution).
    pub async fn docstore_versions(
        &self,
        root_page: u32,
        doc_id_hex: &str,
    ) -> io::Result<Vec<VersionInfo>> {
        let doc_id_bytes = parse_hex(doc_id_hex)
            .ok_or_else(|| io::Error::other("invalid hex doc_id"))?;
        if doc_id_bytes.len() != 16 {
            return Err(io::Error::other("doc_id must be 16 bytes (32 hex chars)"));
        }

        // Scan the raw btree for keys with this doc_id prefix
        let btree = self.engine.open_btree(root_page);
        let mut prefix = [0u8; 16];
        prefix.copy_from_slice(&doc_id_bytes);

        let start = prefix.to_vec();
        let mut end = prefix.to_vec();
        // Increment last byte for exclusive upper bound
        let mut i = end.len();
        while i > 0 {
            i -= 1;
            if end[i] < 0xFF {
                end[i] += 1;
                break;
            }
            // overflow: set to 0 and carry
            end[i] = 0;
        }

        let mut scanner = btree.scan(
            Bound::Included(start.as_slice()),
            Bound::Excluded(end.as_slice()),
            ScanDirection::Forward,
        );

        let mut versions = Vec::new();
        while let Some(entry) = scanner.next().await {
            let (key, value) = entry?;
            if key.len() < 24 {
                continue;
            }
            let (_, ts) = key_encoding::parse_primary_key(&key)
                .map_err(io::Error::other)?;
            let flags = CellFlags::from_byte(value[0]);
            let body_preview = if flags.tombstone {
                "(tombstone)".to_string()
            } else if flags.external {
                let body_len = u32::from_le_bytes(value[1..5].try_into().unwrap());
                format!("(external, {} bytes)", body_len)
            } else if value.len() > 5 {
                let body = &value[5..];
                match decode_body_json(body) {
                    Some(json) => {
                        if json.len() > 80 {
                            format!("{}...", &json[..80])
                        } else {
                            json
                        }
                    }
                    None => format!("{} bytes (binary)", body.len()),
                }
            } else {
                "(empty)".to_string()
            };
            versions.push(VersionInfo {
                ts,
                is_tombstone: flags.tombstone,
                body_preview,
            });
        }
        Ok(versions)
    }

    /// Scan secondary index, returning verified (doc_id, ts) pairs.
    pub async fn secondary_scan(
        &self,
        sec_root: u32,
        primary_root: u32,
        scalars_json: &str,
        read_ts: u64,
        limit: usize,
    ) -> io::Result<Vec<SecondaryHit>> {
        let scalars = parse_scalar_array(scalars_json)
            .map_err(io::Error::other)?;

        let primary = Arc::new(self.make_primary_index(primary_root));
        let sec_btree = self.engine.open_btree(sec_root);
        let secondary = SecondaryIndex::new(sec_btree, primary);

        let prefix = key_encoding::encode_key_prefix(&scalars);
        let upper = key_encoding::successor_key(&prefix);

        let mut scanner = secondary.scan_at_ts(
            Bound::Included(prefix.as_slice()),
            Bound::Excluded(upper.as_slice()),
            read_ts,
            ScanDirection::Forward,
        );

        let mut results = Vec::new();
        while let Some(item) = scanner.next().await {
            let (doc_id, version_ts) = item?;
            results.push(SecondaryHit {
                doc_id_hex: hex_encode(doc_id.as_bytes()),
                version_ts,
            });
            if results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    /// Decode key bytes according to mode.
    pub fn decode_key_bytes(&self, hex: &str, mode: &str) -> Result<String, String> {
        let bytes = parse_hex(hex).ok_or("invalid hex input")?;
        if bytes.is_empty() {
            return Err("empty input".into());
        }

        match mode {
            "primary" => decode_primary(&bytes),
            "secondary" => decode_secondary(&bytes),
            "cell" => decode_cell(&bytes),
            "scalar" => decode_scalar_display(&bytes),
            "auto" => {
                // Try primary (exactly 24 bytes), then secondary (>24), then cell, then scalar
                if bytes.len() == 24
                    && let Ok(r) = decode_primary(&bytes)
                {
                    return Ok(format!("[Primary Key]\n{r}"));
                }
                if bytes.len() > 24
                    && let Ok(r) = decode_secondary(&bytes)
                {
                    return Ok(format!("[Secondary Key]\n{r}"));
                }
                if let Ok(r) = decode_cell(&bytes) {
                    return Ok(format!("[Cell Value]\n{r}"));
                }
                if let Ok(r) = decode_scalar_display(&bytes) {
                    return Ok(format!("[Scalar]\n{r}"));
                }
                Err("Could not auto-detect key format".into())
            }
            _ => Err(format!("unknown mode: {mode}")),
        }
    }

    /// Encode key bytes according to mode.
    pub fn encode_key_bytes(
        &self,
        mode: &str,
        doc_id_hex: &str,
        ts_str: &str,
        scalars_json: &str,
    ) -> Result<String, String> {
        match mode {
            "primary" => {
                let doc_id = parse_doc_id(doc_id_hex)?;
                let ts: u64 = ts_str.parse().map_err(|e| format!("invalid ts: {e}"))?;
                let key = key_encoding::make_primary_key(&doc_id, ts);
                Ok(hex_encode(&key))
            }
            "secondary" => {
                let doc_id = parse_doc_id(doc_id_hex)?;
                let ts: u64 = ts_str.parse().map_err(|e| format!("invalid ts: {e}"))?;
                let scalars = parse_scalar_array(scalars_json)?;
                let key = key_encoding::make_secondary_key(&scalars, &doc_id, ts);
                Ok(hex_encode(&key))
            }
            "scalar" => {
                let scalars = parse_scalar_array(scalars_json)?;
                let mut out = Vec::new();
                for s in &scalars {
                    out.extend_from_slice(&key_encoding::encode_scalar(s));
                }
                Ok(hex_encode(&out))
            }
            _ => Err(format!("unknown mode: {mode}")),
        }
    }

    /// Per-collection MVCC overhead stats.
    pub async fn vacuum_stats(&self) -> io::Result<Vec<VacuumStats>> {
        let collections = self.list_collections().await?;
        let mut stats = Vec::new();

        for col in &collections {
            // Count raw B-tree entries
            let btree = self.engine.open_btree(col.data_root_page);
            let mut scanner = btree.scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward);
            let mut total_raw: u64 = 0;
            let mut tombstone_count: u64 = 0;
            while let Some(entry) = scanner.next().await {
                let (_key, value) = entry?;
                total_raw += 1;
                if !value.is_empty() {
                    let flags = CellFlags::from_byte(value[0]);
                    if flags.tombstone {
                        tombstone_count += 1;
                    }
                }
            }

            // Count visible docs via MVCC scan
            let fh = self.engine.file_header().await;
            let visible_ts = fh.visible_ts.get();
            let read_ts = if visible_ts == 0 { u64::MAX } else { visible_ts };
            let primary = self.make_primary_index(col.data_root_page);
            let mut mvcc_scanner = primary.scan_at_ts(read_ts, ScanDirection::Forward);
            let mut visible: u64 = 0;
            while let Some(item) = mvcc_scanner.next().await {
                item?;
                visible += 1;
            }

            let overhead = total_raw.saturating_sub(visible + tombstone_count);

            stats.push(VacuumStats {
                collection_name: col.name.clone(),
                total_raw_entries: total_raw,
                visible_docs: visible,
                tombstones: tombstone_count,
                version_overhead: overhead,
            });
        }

        Ok(stats)
    }
}

// ─── L3 Display Types ───

pub struct DocstoreEntry {
    pub doc_id_hex: String,
    pub version_ts: u64,
    pub is_tombstone: bool,
    pub is_external: bool,
    pub body_len: u32,
    pub body_json: Option<String>,
}

pub struct VersionInfo {
    pub ts: u64,
    pub is_tombstone: bool,
    pub body_preview: String,
}

pub struct SecondaryHit {
    pub doc_id_hex: String,
    pub version_ts: u64,
}

pub struct VacuumStats {
    pub collection_name: String,
    pub total_raw_entries: u64,
    pub visible_docs: u64,
    pub tombstones: u64,
    pub version_overhead: u64,
}

// ─── L3 Helpers ───

fn decode_body_json(body: &[u8]) -> Option<String> {
    // Try exdb_core document decoding first, fall back to raw UTF-8 JSON
    if let Ok(doc) = exdb_core::encoding::decode_document(body) {
        Some(serde_json::to_string_pretty(&doc).unwrap_or_default())
    } else if let Ok(text) = std::str::from_utf8(body) {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
            Some(serde_json::to_string_pretty(&json).unwrap_or_default())
        } else {
            None
        }
    } else {
        None
    }
}

fn parse_doc_id(hex: &str) -> Result<DocId, String> {
    let bytes = parse_hex(hex).ok_or("invalid hex")?;
    if bytes.len() != 16 {
        return Err(format!("doc_id must be 16 bytes, got {}", bytes.len()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Ok(DocId(arr))
}

fn parse_scalar_array(json: &str) -> Result<Vec<Scalar>, String> {
    let json = json.trim();
    if json.is_empty() {
        return Ok(vec![]);
    }
    let arr: Vec<serde_json::Value> =
        serde_json::from_str(json).map_err(|e| format!("invalid JSON array: {e}"))?;
    arr.iter().map(json_to_scalar).collect()
}

fn json_to_scalar(v: &serde_json::Value) -> Result<Scalar, String> {
    match v {
        serde_json::Value::Null => Ok(Scalar::Null),
        serde_json::Value::Bool(b) => Ok(Scalar::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Scalar::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Scalar::Float64(f))
            } else {
                Err("unsupported number".into())
            }
        }
        serde_json::Value::String(s) => Ok(Scalar::String(s.clone())),
        _ => Err("unsupported JSON type (expected scalar)".into()),
    }
}

fn decode_primary(bytes: &[u8]) -> Result<String, String> {
    let (doc_id, ts) = key_encoding::parse_primary_key(bytes)?;
    Ok(format!(
        "doc_id: {}\nts:     {}",
        hex_encode(doc_id.as_bytes()),
        ts
    ))
}

fn decode_secondary(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() < 25 {
        return Err("too short for secondary key".into());
    }
    let value_prefix = &bytes[..bytes.len() - 24];
    let (doc_id, ts) = key_encoding::parse_secondary_key_suffix(bytes)?;

    // Decode scalars from value prefix
    let mut scalars = Vec::new();
    let mut pos = 0;
    while pos < value_prefix.len() {
        match key_encoding::decode_scalar(&value_prefix[pos..]) {
            Ok((scalar, consumed)) => {
                scalars.push(format!("{scalar:?}"));
                pos += consumed;
            }
            Err(_) => break,
        }
    }

    Ok(format!(
        "scalars: [{}]\ndoc_id:  {}\nts:      {}",
        scalars.join(", "),
        hex_encode(doc_id.as_bytes()),
        ts
    ))
}

fn decode_cell(bytes: &[u8]) -> Result<String, String> {
    if bytes.is_empty() {
        return Err("empty cell".into());
    }
    let flags = CellFlags::from_byte(bytes[0]);
    if flags.tombstone {
        return Ok("Tombstone (deleted)".into());
    }
    if bytes.len() < 5 {
        return Err("cell too short for inline/external".into());
    }
    let body_len = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
    if flags.external {
        if bytes.len() < 11 {
            return Err("cell too short for external ref".into());
        }
        let page_id = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
        let slot_id = u16::from_le_bytes(bytes[9..11].try_into().unwrap());
        Ok(format!(
            "External\nbody_len:  {body_len}\nheap_page: {page_id}\nheap_slot: {slot_id}"
        ))
    } else {
        let preview = if bytes.len() > 5 {
            let body = &bytes[5..];
            decode_body_json(body)
                .unwrap_or_else(|| format!("{} bytes", body.len()))
        } else {
            "(empty)".into()
        };
        Ok(format!(
            "Inline\nbody_len: {body_len}\npreview:  {preview}"
        ))
    }
}

fn decode_scalar_display(bytes: &[u8]) -> Result<String, String> {
    let (scalar, consumed) = key_encoding::decode_scalar(bytes)?;
    let remaining = bytes.len() - consumed;
    let mut out = format!("{scalar:?}");
    if remaining > 0 {
        out.push_str(&format!("\n({remaining} bytes remaining)"));
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.replace([' ', '\n'], "");
    if s.is_empty() {
        return Some(Vec::new());
    }
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
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
