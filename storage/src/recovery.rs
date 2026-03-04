use std::path::Path;

use crate::types::*;
use crate::buffer_pool::BufferPool;
use crate::free_list::FreePageList;
use crate::heap::ExternalHeap;
use crate::catalog::CatalogCache;
use crate::btree::insert::btree_insert;
use crate::btree::delete::btree_delete;
use crate::btree::node::KeySizeHint;
use crate::key_encoding::encode_primary_key;
use crate::checkpoint::recover_dwb;
use crate::wal::reader::WalReader;
use crate::wal::record::*;

/// Result of crash recovery.
#[derive(Debug)]
pub struct RecoveryResult {
    pub records_replayed: u64,
    pub recovered_lsn: Lsn,
    pub dwb_pages_repaired: u32,
}

/// Database metadata persisted alongside data.db.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatabaseMeta {
    pub checkpoint_lsn: u64,
    pub page_size: u32,
    pub created_at: u64,
}

/// Read meta.json.
pub fn read_meta_json(db_dir: &Path) -> Result<DatabaseMeta, std::io::Error> {
    let path = db_dir.join("meta.json");
    let contents = std::fs::read_to_string(path)?;
    serde_json::from_str(&contents).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
    })
}

/// Write meta.json atomically (write temp + fsync + rename).
pub fn write_meta_json(db_dir: &Path, meta: &DatabaseMeta) -> Result<(), std::io::Error> {
    use std::io::Write;

    let path = db_dir.join("meta.json");
    let tmp_path = db_dir.join("meta.json.tmp");

    let contents = serde_json::to_string_pretty(meta).map_err(|e| {
        std::io::Error::other(e.to_string())
    })?;

    let mut file = std::fs::File::create(&tmp_path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;

    Ok(())
}

/// Full crash recovery sequence.
pub fn recover_database(
    db_dir: &Path,
    pool: &BufferPool,
    catalog: &mut CatalogCache,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
    checkpoint_lsn: Lsn,
) -> Result<RecoveryResult, RecoveryError> {
    let page_size = pool.page_size();

    // 1. DWB recovery: repair torn pages.
    let dwb_repaired = recover_dwb(db_dir, pool.io(), page_size)
        .map_err(RecoveryError::Io)?;

    // 2. Replay WAL from checkpoint_lsn.
    let wal_dir = db_dir.join("wal");
    if !wal_dir.exists() {
        return Ok(RecoveryResult {
            records_replayed: 0,
            recovered_lsn: checkpoint_lsn,
            dwb_pages_repaired: dwb_repaired,
        });
    }

    let mut reader = WalReader::open(&wal_dir, checkpoint_lsn)
        .map_err(RecoveryError::Wal)?;
    let mut records_replayed = 0u64;

    while let Some(record) = reader.next_record().map_err(RecoveryError::Wal)? {
        match &record.payload {
            WalPayload::TxCommit(tx) => {
                replay_tx_commit(pool, catalog, free_list, heap, tx, record.lsn)?;
            }
            WalPayload::CreateCollection(cc) => {
                replay_create_collection(pool, catalog, free_list, cc, record.lsn)?;
            }
            WalPayload::DropCollection(dc) => {
                catalog.remove_collection(dc.collection_id);
            }
            WalPayload::CreateIndex(ci) => {
                replay_create_index(catalog, free_list, pool, ci, record.lsn)?;
            }
            WalPayload::DropIndex(di) => {
                catalog.remove_index(di.index_id);
            }
            WalPayload::IndexReady(ir) => {
                catalog.update_index_state(ir.index_id, IndexState::Ready);
            }
            WalPayload::Checkpoint(_) => {
                // No-op during replay.
            }
            WalPayload::Vacuum(v) => {
                replay_vacuum(pool, catalog, free_list, heap, v, record.lsn)?;
            }
            _ => {
                // System-level records not replayed here.
            }
        }
        records_replayed += 1;
    }

    Ok(RecoveryResult {
        records_replayed,
        recovered_lsn: reader.current_lsn(),
        dwb_pages_repaired: dwb_repaired,
    })
}

fn replay_tx_commit(
    pool: &BufferPool,
    catalog: &mut CatalogCache,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
    tx: &TxCommitRecord,
    lsn: Lsn,
) -> Result<(), RecoveryError> {
    for mutation in &tx.mutations {
        let col = catalog.get_collection(mutation.collection_id)
            .ok_or(RecoveryError::CollectionNotFound(mutation.collection_id))?
            .clone();

        let primary_key = encode_primary_key(mutation.doc_id, tx.commit_ts);

        match mutation.op_type {
            OpType::Insert | OpType::Replace => {
                let cell = build_primary_cell(
                    &primary_key,
                    &mutation.body,
                    pool,
                    free_list,
                    heap,
                    lsn,
                )?;
                let new_root = btree_insert(
                    pool,
                    col.primary_root_page,
                    &cell,
                    KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
                    free_list,
                    lsn,
                ).map_err(RecoveryError::BufferPool)?;
                if new_root != col.primary_root_page {
                    catalog.update_collection_root(col.collection_id, new_root);
                }
            }
            OpType::Delete => {
                let cell = build_tombstone_cell(&primary_key);
                let new_root = btree_insert(
                    pool,
                    col.primary_root_page,
                    &cell,
                    KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
                    free_list,
                    lsn,
                ).map_err(RecoveryError::BufferPool)?;
                if new_root != col.primary_root_page {
                    catalog.update_collection_root(col.collection_id, new_root);
                }
            }
        }
    }

    // Apply index deltas.
    for delta in &tx.index_deltas {
        let idx = catalog.get_index(delta.index_id)
            .ok_or(RecoveryError::IndexNotFound(delta.index_id))?
            .clone();

        if let Some(old_key) = &delta.old_key {
            let (new_root, _) = btree_delete(
                pool,
                idx.root_page,
                old_key.as_ref(),
                KeySizeHint::VariableWithSuffix,
                free_list,
                lsn,
            ).map_err(RecoveryError::BufferPool)?;
            if new_root != idx.root_page {
                catalog.update_index_root(delta.index_id, new_root);
            }
        }
        if let Some(new_key) = &delta.new_key {
            let new_root = btree_insert(
                pool,
                idx.root_page,
                new_key.as_ref(),
                KeySizeHint::VariableWithSuffix,
                free_list,
                lsn,
            ).map_err(RecoveryError::BufferPool)?;
            if new_root != idx.root_page {
                catalog.update_index_root(delta.index_id, new_root);
            }
        }
    }

    Ok(())
}

fn replay_create_collection(
    pool: &BufferPool,
    catalog: &mut CatalogCache,
    free_list: &mut FreePageList,
    cc: &CreateCollectionRecord,
    lsn: Lsn,
) -> Result<(), RecoveryError> {
    // Allocate root page for the new collection's primary B-tree.
    let mut root_guard = pool.new_page(PageType::BTreeLeaf, free_list)
        .map_err(RecoveryError::BufferPool)?;
    let root_page = root_guard.page_id();
    {
        let mut sp = root_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    // Allocate created_at index root.
    let mut cat_guard = pool.new_page(PageType::BTreeLeaf, free_list)
        .map_err(RecoveryError::BufferPool)?;
    let cat_page = cat_guard.page_id();
    {
        let mut sp = cat_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    let meta = crate::catalog::CollectionMeta {
        collection_id: cc.collection_id,
        name: cc.name.clone(),
        primary_root_page: root_page,
        created_at_root_page: cat_page,
        doc_count: 0,
        config: crate::catalog::CollectionConfig::default(),
    };
    catalog.insert_collection(meta);
    Ok(())
}

fn replay_create_index(
    catalog: &mut CatalogCache,
    free_list: &mut FreePageList,
    pool: &BufferPool,
    ci: &CreateIndexRecord,
    lsn: Lsn,
) -> Result<(), RecoveryError> {
    let mut root_guard = pool.new_page(PageType::BTreeLeaf, free_list)
        .map_err(RecoveryError::BufferPool)?;
    let root_page = root_guard.page_id();
    {
        let mut sp = root_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    let meta = crate::catalog::IndexMeta {
        index_id: ci.index_id,
        collection_id: ci.collection_id,
        name: ci.name.clone(),
        field_paths: ci.field_paths.clone(),
        root_page,
        state: IndexState::Building,
    };
    catalog.insert_index(meta);
    Ok(())
}

fn replay_vacuum(
    pool: &BufferPool,
    catalog: &mut CatalogCache,
    free_list: &mut FreePageList,
    _heap: &mut ExternalHeap,
    v: &VacuumRecord,
    lsn: Lsn,
) -> Result<(), RecoveryError> {
    let col = catalog.get_collection(v.collection_id)
        .ok_or(RecoveryError::CollectionNotFound(v.collection_id))?
        .clone();

    for entry in &v.entries {
        // Delete the old version from primary B-tree.
        let key = encode_primary_key(entry.doc_id, entry.removed_ts);
        let (new_root, _found) = btree_delete(
            pool,
            col.primary_root_page,
            key.as_ref(),
            KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
            free_list,
            lsn,
        ).map_err(RecoveryError::BufferPool)?;
        if new_root != col.primary_root_page {
            catalog.update_collection_root(col.collection_id, new_root);
        }

        // Delete index keys.
        for ik in &entry.index_keys {
            if let Some(idx) = catalog.get_index(ik.index_id).cloned() {
                let (new_idx_root, _) = btree_delete(
                    pool,
                    idx.root_page,
                    ik.key.as_ref(),
                    KeySizeHint::VariableWithSuffix,
                    free_list,
                    lsn,
                ).map_err(RecoveryError::BufferPool)?;
                if new_idx_root != idx.root_page {
                    catalog.update_index_root(ik.index_id, new_idx_root);
                }
            }
        }
    }

    Ok(())
}

/// Build a primary leaf cell: key || flags(1) || body_len(4) || body
pub fn build_primary_cell(
    key: &crate::types::EncodedKey,
    body: &[u8],
    pool: &BufferPool,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
    lsn: Lsn,
) -> Result<Vec<u8>, RecoveryError> {
    let external_threshold = heap.external_threshold() as usize;

    if body.len() > external_threshold {
        // Store externally.
        let heap_ref = heap.store(pool, free_list, body, lsn)
            .map_err(RecoveryError::BufferPool)?;
        let mut cell = Vec::with_capacity(key.0.len() + 1 + 4 + 2);
        cell.extend_from_slice(&key.0);
        cell.push(cell_flags::EXTERNAL);
        cell.extend_from_slice(&heap_ref.page_id.0.to_le_bytes());
        cell.extend_from_slice(&heap_ref.slot_id.to_le_bytes());
        Ok(cell)
    } else {
        let mut cell = Vec::with_capacity(key.0.len() + 1 + 4 + body.len());
        cell.extend_from_slice(&key.0);
        cell.push(0); // flags: inline
        cell.extend_from_slice(&(body.len() as u32).to_le_bytes());
        cell.extend_from_slice(body);
        Ok(cell)
    }
}

/// Build a tombstone cell: key || TOMBSTONE flag
pub fn build_tombstone_cell(key: &crate::types::EncodedKey) -> Vec<u8> {
    let mut cell = Vec::with_capacity(key.0.len() + 1);
    cell.extend_from_slice(&key.0);
    cell.push(cell_flags::TOMBSTONE);
    cell
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(WalError),

    #[error("buffer pool error: {0}")]
    BufferPool(crate::buffer_pool::BufferPoolError),

    #[error("collection not found: {0:?}")]
    CollectionNotFound(CollectionId),

    #[error("index not found: {0:?}")]
    IndexNotFound(IndexId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_json_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let meta = DatabaseMeta {
            checkpoint_lsn: 12345,
            page_size: 8192,
            created_at: 1700000000000,
        };
        write_meta_json(dir.path(), &meta).unwrap();
        let read_back = read_meta_json(dir.path()).unwrap();
        assert_eq!(read_back.checkpoint_lsn, 12345);
        assert_eq!(read_back.page_size, 8192);
        assert_eq!(read_back.created_at, 1700000000000);
    }

    #[test]
    fn test_build_primary_cell_inline() {
        let key = encode_primary_key(DocId(1), Timestamp(100));
        let body = b"hello world";

        use crate::buffer_pool::MemPageIO;
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 2);
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 8);
        let mut fl = FreePageList::new(None);
        let mut heap = ExternalHeap::new(DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE / 2);

        let cell = build_primary_cell(&key, body, &pool, &mut fl, &mut heap, Lsn(1)).unwrap();
        assert_eq!(&cell[..PRIMARY_KEY_SIZE], &key.0);
        assert_eq!(cell[PRIMARY_KEY_SIZE], 0); // inline flag
        let body_len = u32::from_le_bytes(cell[PRIMARY_KEY_SIZE + 1..PRIMARY_KEY_SIZE + 5].try_into().unwrap()) as usize;
        assert_eq!(body_len, body.len());
    }

    #[test]
    fn test_build_tombstone_cell() {
        let key = encode_primary_key(DocId(42), Timestamp(200));
        let cell = build_tombstone_cell(&key);
        assert_eq!(&cell[..PRIMARY_KEY_SIZE], &key.0);
        assert_eq!(cell[PRIMARY_KEY_SIZE], cell_flags::TOMBSTONE);
    }

    #[test]
    fn test_recovery_empty_wal() {
        let dir = tempfile::tempdir().unwrap();
        use crate::buffer_pool::MemPageIO;

        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 2);
        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 8);
        let mut catalog = CatalogCache::new(1, 1);
        let mut fl = FreePageList::new(None);
        let mut heap = ExternalHeap::new(DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE / 2);

        let result = recover_database(
            dir.path(),
            &pool,
            &mut catalog,
            &mut fl,
            &mut heap,
            Lsn::ZERO,
        ).unwrap();

        assert_eq!(result.records_replayed, 0);
        assert_eq!(result.recovered_lsn, Lsn::ZERO);
    }
}
