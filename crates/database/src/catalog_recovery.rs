//! B10: WAL recovery handler for crash recovery.
//!
//! Implements the L2 `WalRecordHandler` trait to replay WAL records during
//! startup. Handles TxCommit, IndexReady, Vacuum, VisibleTs, and
//! RollbackVacuum records.

use std::collections::HashMap;
use std::sync::Arc;

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId, Ts};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_storage::wal::{
    WalRecord, WAL_RECORD_INDEX_READY, WAL_RECORD_TX_COMMIT, WAL_RECORD_VACUUM,
    WAL_RECORD_VISIBLE_TS,
};
use exdb_tx::deserialize_wal_payload;

use crate::catalog_cache::{CatalogCache, IndexMeta, IndexState};
use crate::catalog_persistence::CatalogPersistence;

/// State recovered from WAL replay, returned to Database::open.
pub struct RecoveredState {
    pub catalog: CatalogCache,
    pub primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
    pub secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
    pub recovered_ts: Ts,
    pub visible_ts: Ts,
}

/// WAL recovery handler that replays records to rebuild database state.
pub struct DatabaseRecoveryHandler {
    storage: Arc<StorageEngine>,
    catalog_id_btree: BTreeHandle,
    catalog_name_btree: BTreeHandle,
    catalog: CatalogCache,
    primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
    secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
    recovered_ts: Ts,
    visible_ts: Ts,
}

impl DatabaseRecoveryHandler {
    /// Create a new recovery handler.
    pub async fn new(storage: Arc<StorageEngine>) -> std::io::Result<Self> {
        let fh = storage.file_header().await;
        let catalog_id_btree = storage.open_btree(fh.catalog_root_page.get());
        let catalog_name_btree = storage.open_btree(fh.catalog_name_root_page.get());
        let visible_ts = fh.visible_ts.get();

        // Load initial catalog from B-trees
        let catalog = CatalogPersistence::load_catalog(
            &storage,
            &catalog_id_btree,
            &catalog_name_btree,
        )
        .await?;

        // Open index handles for existing catalog entries
        let mut primary_indexes = HashMap::new();
        let mut secondary_indexes = HashMap::new();
        let external_threshold = storage.config().page_size / 4;

        for coll in catalog.list_collections() {
            let btree = storage.open_btree(coll.primary_root_page);
            let primary = Arc::new(PrimaryIndex::new(
                btree,
                Arc::clone(&storage),
                external_threshold,
            ));
            primary_indexes.insert(coll.collection_id, primary);
        }

        for coll in catalog.list_collections() {
            for idx in catalog.list_indexes(coll.collection_id) {
                if let Some(primary) = primary_indexes.get(&idx.collection_id) {
                    let btree = storage.open_btree(idx.root_page);
                    let secondary =
                        Arc::new(SecondaryIndex::new(btree, Arc::clone(primary)));
                    secondary_indexes.insert(idx.index_id, secondary);
                }
            }
        }

        Ok(Self {
            storage,
            catalog_id_btree,
            catalog_name_btree,
            catalog,
            primary_indexes,
            secondary_indexes,
            recovered_ts: visible_ts,
            visible_ts,
        })
    }

    /// Consume this handler and return the recovered state.
    pub fn into_state(self) -> RecoveredState {
        RecoveredState {
            catalog: self.catalog,
            primary_indexes: self.primary_indexes,
            secondary_indexes: self.secondary_indexes,
            recovered_ts: self.recovered_ts,
            visible_ts: self.visible_ts,
        }
    }

    /// Replay a TxCommit WAL record.
    async fn replay_tx_commit(&mut self, payload: &[u8]) -> std::io::Result<()> {
        // deserialize_wal_payload returns:
        // (version: u8, commit_ts: Ts, mutations: Vec<(CollectionId, DocId, u8, Option<Vec<u8>>)>, catalog_bytes: Vec<u8>)
        let (_version, commit_ts, mutations, catalog_bytes) =
            deserialize_wal_payload(payload).map_err(|e| {
                std::io::Error::other(format!("failed to deserialize WAL payload: {e}"))
            })?;

        // Track highest timestamp
        if commit_ts > self.recovered_ts {
            self.recovered_ts = commit_ts;
        }

        // Step 1: Apply catalog mutations FIRST
        self.replay_catalog_mutations(&catalog_bytes).await?;

        // Step 2: Apply data mutations
        let external_threshold = self.storage.config().page_size / 4;
        for (collection_id, doc_id, op_tag, body_bytes) in &mutations {
            // Ensure primary index handle exists
            if !self.primary_indexes.contains_key(collection_id) {
                if let Some(coll) = self.catalog.get_collection(*collection_id) {
                    let btree = self.storage.open_btree(coll.primary_root_page);
                    let primary = Arc::new(PrimaryIndex::new(
                        btree,
                        Arc::clone(&self.storage),
                        external_threshold,
                    ));
                    self.primary_indexes.insert(*collection_id, primary);
                }
            }

            if let Some(primary) = self.primary_indexes.get(collection_id) {
                match op_tag {
                    0x01 | 0x02 => {
                        // Insert or Replace
                        if let Some(body) = body_bytes {
                            primary
                                .insert_version(doc_id, commit_ts, Some(body))
                                .await?;
                        }
                    }
                    0x03 => {
                        // Delete
                        primary.insert_version(doc_id, commit_ts, None).await?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Replay catalog mutations from the serialized catalog bytes.
    async fn replay_catalog_mutations(&mut self, data: &[u8]) -> std::io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut offset = 0;
        if offset + 4 > data.len() {
            return Ok(());
        }
        let count =
            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..count {
            if offset >= data.len() {
                break;
            }
            let type_tag = data[offset];
            offset += 1;

            match type_tag {
                0x01 => {
                    // CreateCollection
                    if offset + 8 > data.len() {
                        break;
                    }
                    let provisional_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    if offset + name_len > data.len() {
                        break;
                    }
                    let name =
                        String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
                    offset += name_len;
                    if offset + 8 > data.len() {
                        break;
                    }
                    let _primary_root_page = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    );
                    offset += 4;
                    let _created_at_root_page = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    );
                    offset += 4;

                    let cid = CollectionId(provisional_id);

                    // Idempotency: skip if already loaded from checkpoint B-trees
                    if self.catalog.get_collection(cid).is_some() {
                        continue;
                    }

                    // During WAL replay, the pre-allocated root pages from the
                    // original commit may not exist on disk (crash before flush).
                    // Create fresh B-trees instead.
                    let primary_btree = self.storage.create_btree().await?;
                    let actual_primary_root = primary_btree.root_page();

                    CatalogPersistence::apply_create_collection(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        cid,
                        &name,
                        actual_primary_root,
                    )
                    .await?;

                    // Open primary index handle with the fresh B-tree
                    let external_threshold = self.storage.config().page_size / 4;
                    let primary = Arc::new(PrimaryIndex::new(
                        primary_btree,
                        Arc::clone(&self.storage),
                        external_threshold,
                    ));
                    self.primary_indexes.insert(cid, Arc::clone(&primary));

                    // Create _created_at index with a fresh B-tree
                    let idx_id = self.catalog.allocate_index_id();
                    let cat_btree = self.storage.create_btree().await?;
                    let actual_cat_root = cat_btree.root_page();
                    let secondary = Arc::new(SecondaryIndex::new(cat_btree, primary));
                    self.secondary_indexes.insert(idx_id, secondary);

                    let meta = IndexMeta {
                        index_id: idx_id,
                        collection_id: cid,
                        name: "_created_at".to_string(),
                        field_paths: vec![FieldPath::single("_created_at")],
                        root_page: actual_cat_root,
                        state: IndexState::Ready,
                    };
                    CatalogPersistence::apply_create_index(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        &meta,
                    )
                    .await?;
                }
                0x02 => {
                    // DropCollection
                    if offset + 8 > data.len() {
                        break;
                    }
                    let collection_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    // Skip name_len + name
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    offset += name_len;
                    // Skip primary_root_page
                    if offset + 4 > data.len() {
                        break;
                    }
                    offset += 4;
                    // Skip dropped indexes
                    if offset + 4 > data.len() {
                        break;
                    }
                    let dropped_count = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    for _ in 0..dropped_count {
                        if offset + 8 > data.len() {
                            break;
                        }
                        offset += 8; // index_id
                        if offset + 4 > data.len() {
                            break;
                        }
                        let nl = u32::from_le_bytes(
                            data[offset..offset + 4].try_into().unwrap(),
                        ) as usize;
                        offset += 4;
                        offset += nl; // name
                        offset = skip_field_paths(data, offset);
                        if offset + 4 > data.len() {
                            break;
                        }
                        offset += 4; // root_page
                    }

                    let cid = CollectionId(collection_id);
                    let idx_ids: Vec<IndexId> = self
                        .catalog
                        .list_indexes(cid)
                        .iter()
                        .map(|m| m.index_id)
                        .collect();
                    for idx_id in idx_ids {
                        self.secondary_indexes.remove(&idx_id);
                    }
                    self.primary_indexes.remove(&cid);

                    CatalogPersistence::apply_drop_collection(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        cid,
                    )
                    .await?;
                }
                0x03 => {
                    // CreateIndex
                    if offset + 16 > data.len() {
                        break;
                    }
                    let provisional_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    let collection_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    if offset + name_len > data.len() {
                        break;
                    }
                    let name =
                        String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
                    offset += name_len;
                    let (field_paths, new_offset) = parse_field_paths(data, offset);
                    offset = new_offset;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let _root_page = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    );
                    offset += 4;

                    let cid = CollectionId(collection_id);
                    let iid = IndexId(provisional_id);

                    // Idempotency: skip if already loaded from checkpoint B-trees
                    if self.catalog.get_index(iid).is_some() {
                        continue;
                    }

                    // Create fresh B-tree (original pages may not have been flushed)
                    let idx_btree = self.storage.create_btree().await?;
                    let actual_root = idx_btree.root_page();

                    let meta = IndexMeta {
                        index_id: iid,
                        collection_id: cid,
                        name,
                        field_paths,
                        root_page: actual_root,
                        state: IndexState::Building,
                    };
                    CatalogPersistence::apply_create_index(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        &meta,
                    )
                    .await?;

                    if let Some(primary) = self.primary_indexes.get(&cid) {
                        let secondary =
                            Arc::new(SecondaryIndex::new(idx_btree, Arc::clone(primary)));
                        self.secondary_indexes.insert(iid, secondary);
                    }
                }
                0x04 => {
                    // DropIndex
                    if offset + 16 > data.len() {
                        break;
                    }
                    let index_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    let _collection_id = u64::from_le_bytes(
                        data[offset..offset + 8].try_into().unwrap(),
                    );
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;
                    offset += name_len;
                    offset = skip_field_paths(data, offset);
                    if offset + 4 > data.len() {
                        break;
                    }
                    offset += 4; // root_page

                    let iid = IndexId(index_id);
                    self.secondary_indexes.remove(&iid);
                    CatalogPersistence::apply_drop_index(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        iid,
                    )
                    .await?;
                }
                _ => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Replay an IndexReady WAL record.
    async fn replay_index_ready(&mut self, payload: &[u8]) -> std::io::Result<()> {
        if payload.len() < 8 {
            return Ok(());
        }
        let index_id =
            u64::from_le_bytes(payload[0..8].try_into().unwrap());
        CatalogPersistence::apply_index_ready(
            &self.catalog_id_btree,
            &mut self.catalog,
            IndexId(index_id),
        )
        .await
    }

    /// Replay a VisibleTs WAL record.
    fn replay_visible_ts(&mut self, payload: &[u8]) {
        if payload.len() >= 8 {
            let ts = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            if ts > self.visible_ts {
                self.visible_ts = ts;
            }
        }
    }
}

impl DatabaseRecoveryHandler {
    /// Handle a single WAL record during replay.
    /// Called manually instead of via WalRecordHandler trait because
    /// B-tree operations hold parking_lot guards across .await points,
    /// making the handler !Send.
    pub async fn handle_record(&mut self, record: &WalRecord) -> std::io::Result<()> {
        match record.record_type {
            WAL_RECORD_TX_COMMIT => self.replay_tx_commit(&record.payload).await?,
            WAL_RECORD_INDEX_READY => self.replay_index_ready(&record.payload).await?,
            WAL_RECORD_VISIBLE_TS => self.replay_visible_ts(&record.payload),
            WAL_RECORD_VACUUM => { /* vacuum records are informational during replay */ }
            _ => {
                tracing::warn!(
                    "unknown WAL record type 0x{:02x} at LSN {}, skipping",
                    record.record_type,
                    record.lsn,
                );
            }
        }
        Ok(())
    }
}

// ─── Helpers ───

fn parse_field_paths(data: &[u8], mut offset: usize) -> (Vec<FieldPath>, usize) {
    let mut result = Vec::new();
    if offset + 1 > data.len() {
        return (result, offset);
    }
    let count = data[offset] as usize;
    offset += 1;
    for _ in 0..count {
        if offset + 1 > data.len() {
            break;
        }
        let seg_count = data[offset] as usize;
        offset += 1;
        let mut segments = Vec::new();
        for _ in 0..seg_count {
            if offset + 2 > data.len() {
                break;
            }
            let seg_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + seg_len > data.len() {
                break;
            }
            let seg = String::from_utf8_lossy(&data[offset..offset + seg_len]).to_string();
            offset += seg_len;
            segments.push(seg);
        }
        result.push(FieldPath::new(segments));
    }
    (result, offset)
}

fn skip_field_paths(data: &[u8], mut offset: usize) -> usize {
    if offset + 1 > data.len() {
        return offset;
    }
    let count = data[offset] as usize;
    offset += 1;
    for _ in 0..count {
        if offset + 1 > data.len() {
            break;
        }
        let seg_count = data[offset] as usize;
        offset += 1;
        for _ in 0..seg_count {
            if offset + 2 > data.len() {
                break;
            }
            let seg_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            offset += seg_len;
        }
    }
    offset
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::StorageConfig;

    async fn setup() -> Arc<StorageEngine> {
        Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn new_handler_loads_empty_catalog() {
        let storage = setup().await;
        let handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let state = handler.into_state();
        assert!(state.catalog.list_collections().is_empty());
        assert_eq!(state.recovered_ts, 0);
        assert_eq!(state.visible_ts, 0);
    }

    #[tokio::test]
    async fn replay_visible_ts() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        let record = WalRecord {
            lsn: 0,
            record_type: WAL_RECORD_VISIBLE_TS,
            payload: 42u64.to_le_bytes().to_vec(),
        };
        handler.handle_record(&record).await.unwrap();

        let state = handler.into_state();
        assert_eq!(state.visible_ts, 42);
    }

    #[tokio::test]
    async fn replay_visible_ts_monotonic() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        for ts in [10u64, 20, 15, 30, 25] {
            let record = WalRecord {
                lsn: 0,
                record_type: WAL_RECORD_VISIBLE_TS,
                payload: ts.to_le_bytes().to_vec(),
            };
            handler.handle_record(&record).await.unwrap();
        }

        let state = handler.into_state();
        assert_eq!(state.visible_ts, 30);
    }

    #[tokio::test]
    async fn unknown_record_type_skipped() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        let record = WalRecord {
            lsn: 0,
            record_type: 0xFF,
            payload: vec![1, 2, 3],
        };
        handler.handle_record(&record).await.unwrap();
    }

    #[test]
    fn parse_field_paths_empty() {
        let data = [0u8];
        let (paths, offset) = parse_field_paths(&data, 0);
        assert!(paths.is_empty());
        assert_eq!(offset, 1);
    }

    #[test]
    fn parse_field_paths_single() {
        let data = [
            1u8,
            1,
            3, 0,
            b'a', b'b', b'c',
        ];
        let (paths, offset) = parse_field_paths(&data, 0);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].segments(), &["abc"]);
        assert_eq!(offset, data.len());
    }

    #[test]
    fn skip_field_paths_matches_parse() {
        let data = [
            2u8,
            1, 3, 0, b'a', b'b', b'c',
            2, 1, 0, b'x', 2, 0, b'y', b'z',
        ];
        let (paths, parse_offset) = parse_field_paths(&data, 0);
        let skip_offset = skip_field_paths(&data, 0);
        assert_eq!(parse_offset, skip_offset);
        assert_eq!(paths.len(), 2);
    }
}
