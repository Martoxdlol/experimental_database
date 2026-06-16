//! B10: WAL recovery handler for crash recovery.
//!
//! Implements the L2 `WalRecordHandler` trait to replay WAL records during
//! startup. Handles TxCommit, IndexReady, Vacuum, VisibleTs, and
//! RollbackVacuum records.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use exdb_core::encoding::decode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_docstore::{
    IndexBuilder, PrimaryIndex, SecondaryIndex, compute_index_entries,
    make_secondary_key_from_prefix,
};
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_storage::wal::{
    WAL_RECORD_CHECKPOINT, WAL_RECORD_INDEX_READY, WAL_RECORD_ROLLBACK_VACUUM,
    WAL_RECORD_TX_COMMIT, WAL_RECORD_VACUUM, WAL_RECORD_VISIBLE_TS, WalRecord,
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
    replayed_commits: Vec<ReplayedCommit>,
}

struct ReplayedCommit {
    commit_ts: Ts,
    mutations: Vec<ReplayedDataMutation>,
    catalog_bytes: Vec<u8>,
}

#[derive(Clone)]
struct ReplayedDataMutation {
    collection_id: CollectionId,
    doc_id: DocId,
    body: Option<Vec<u8>>,
}

impl DatabaseRecoveryHandler {
    /// Create a new recovery handler.
    pub async fn new(storage: Arc<StorageEngine>) -> std::io::Result<Self> {
        let fh = storage.file_header().await;
        let catalog_id_btree = storage.open_btree(fh.catalog_root_page.get());
        let catalog_name_btree = storage.open_btree(fh.catalog_name_root_page.get());
        let visible_ts = fh.visible_ts.get();

        // Load initial catalog from B-trees
        let catalog =
            CatalogPersistence::load_catalog(&storage, &catalog_id_btree, &catalog_name_btree)
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
                    let secondary = Arc::new(SecondaryIndex::new(btree, Arc::clone(primary)));
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
            replayed_commits: Vec::new(),
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

    /// Rebuild Ready secondary indexes after WAL replay.
    ///
    /// WAL commits carry primary document versions, while secondary index pages
    /// may still be dirty at crash time. Replaying Ready indexes from the
    /// recovered primary indexes makes recovery independent of which dirty
    /// secondary pages reached `data.db` before the crash. The pass is
    /// idempotent because duplicate B-tree inserts replace the same key.
    pub async fn rebuild_ready_indexes(&self) -> std::io::Result<()> {
        for collection in self.catalog.list_collections() {
            let Some(primary) = self.primary_indexes.get(&collection.collection_id) else {
                continue;
            };

            for index in self.catalog.ready_indexes(collection.collection_id) {
                let Some(secondary) = self.secondary_indexes.get(&index.index_id) else {
                    continue;
                };

                let builder = IndexBuilder::new(
                    Arc::clone(primary),
                    Arc::clone(secondary),
                    index.field_paths.clone(),
                );
                builder.build(self.visible_ts, None).await?;
            }
        }

        Ok(())
    }

    /// Remove effects of TxCommit records that were persisted locally but never
    /// made visible. This handles the core startup rollback-vacuum invariant:
    /// recovered page/catalog state must not expose commits after `visible_ts`.
    pub async fn rollback_unreplicated_commits(&mut self) -> std::io::Result<bool> {
        let unreplicated: Vec<ReplayedCommit> = self
            .replayed_commits
            .iter()
            .filter(|commit| commit.commit_ts > self.visible_ts)
            .map(|commit| ReplayedCommit {
                commit_ts: commit.commit_ts,
                mutations: commit.mutations.clone(),
                catalog_bytes: commit.catalog_bytes.clone(),
            })
            .collect();

        for commit in unreplicated.iter().rev() {
            self.rollback_data_mutations(commit).await?;
            self.rollback_catalog_mutations(&commit.catalog_bytes)
                .await?;
        }

        if !unreplicated.is_empty() {
            self.recovered_ts = self.visible_ts;
        }

        Ok(!unreplicated.is_empty())
    }

    async fn rollback_data_mutations(&mut self, commit: &ReplayedCommit) -> std::io::Result<()> {
        for mutation in &commit.mutations {
            if let Some(body) = &mutation.body {
                self.rollback_secondary_entries(mutation, commit.commit_ts, body)
                    .await?;
            }

            if let Some(primary) = self.primary_indexes.get(&mutation.collection_id) {
                primary
                    .remove_version(&mutation.doc_id, commit.commit_ts)
                    .await?;
            }
        }
        Ok(())
    }

    async fn rollback_secondary_entries(
        &self,
        mutation: &ReplayedDataMutation,
        commit_ts: Ts,
        body: &[u8],
    ) -> std::io::Result<()> {
        let doc = decode_document(body)
            .map_err(|err| std::io::Error::other(format!("document decode error: {err}")))?;

        for index in self.catalog.ready_indexes(mutation.collection_id) {
            let Some(secondary) = self.secondary_indexes.get(&index.index_id) else {
                continue;
            };
            let key_prefixes =
                compute_index_entries(&doc, &index.field_paths).map_err(std::io::Error::other)?;
            for prefix in key_prefixes {
                let key = make_secondary_key_from_prefix(&prefix, &mutation.doc_id, commit_ts);
                secondary.remove_entry(&key).await?;
            }
        }

        Ok(())
    }

    async fn rollback_catalog_mutations(&mut self, data: &[u8]) -> std::io::Result<()> {
        if data.len() < 4 {
            return Ok(());
        }

        let mut offset = 0;
        let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..count {
            if offset >= data.len() {
                break;
            }

            let type_tag = data[offset];
            offset += 1;

            match type_tag {
                0x01 => {
                    if offset + 8 > data.len() {
                        break;
                    }
                    let collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4 + name_len;
                    if offset + 8 > data.len() {
                        break;
                    }
                    offset += 8; // primary_root_page + created_at_root_page

                    let cid = CollectionId(collection_id);
                    let mut root_pages = BTreeSet::new();
                    if let Some(collection) = self.catalog.get_collection(cid) {
                        root_pages.insert(collection.primary_root_page);
                    }
                    let index_ids: Vec<IndexId> = self
                        .catalog
                        .list_indexes(cid)
                        .iter()
                        .map(|idx| {
                            root_pages.insert(idx.root_page);
                            idx.index_id
                        })
                        .collect();
                    for index_id in index_ids {
                        self.secondary_indexes.remove(&index_id);
                    }
                    self.primary_indexes.remove(&cid);
                    CatalogPersistence::apply_drop_collection(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        cid,
                    )
                    .await?;
                    self.deallocate_rollback_root_pages(root_pages).await?;
                }
                0x02 => {
                    if offset + 8 > data.len() {
                        break;
                    }
                    let collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    if offset + name_len > data.len() {
                        break;
                    }
                    let name =
                        String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
                    offset += name_len;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let primary_root_page =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let dropped_count =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;

                    let cid = CollectionId(collection_id);
                    CatalogPersistence::apply_create_collection(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        cid,
                        &name,
                        primary_root_page,
                    )
                    .await?;

                    let external_threshold = self.storage.config().page_size / 4;
                    let primary_btree = self.storage.open_btree(primary_root_page);
                    let primary = Arc::new(PrimaryIndex::new(
                        primary_btree,
                        Arc::clone(&self.storage),
                        external_threshold,
                    ));
                    self.primary_indexes.insert(cid, Arc::clone(&primary));

                    for _ in 0..dropped_count {
                        if offset + 8 > data.len() {
                            break;
                        }
                        let index_id =
                            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                        if offset + 4 > data.len() {
                            break;
                        }
                        let nl = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
                            as usize;
                        offset += 4;
                        if offset + nl > data.len() {
                            break;
                        }
                        let index_name =
                            String::from_utf8_lossy(&data[offset..offset + nl]).to_string();
                        offset += nl;
                        let (field_paths, new_offset) = parse_field_paths(data, offset);
                        offset = new_offset;
                        if offset + 4 > data.len() {
                            break;
                        }
                        let root_page =
                            u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                        offset += 4;

                        let iid = IndexId(index_id);
                        let idx_btree = self.storage.open_btree(root_page);
                        let secondary =
                            Arc::new(SecondaryIndex::new(idx_btree, Arc::clone(&primary)));
                        self.secondary_indexes.insert(iid, secondary);

                        let meta = IndexMeta {
                            index_id: iid,
                            collection_id: cid,
                            name: index_name,
                            field_paths,
                            root_page,
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
                }
                0x03 => {
                    if offset + 16 > data.len() {
                        break;
                    }
                    let index_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 16; // provisional_id + collection_id
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4 + name_len;
                    offset = skip_field_paths(data, offset);
                    if offset + 4 > data.len() {
                        break;
                    }
                    offset += 4; // root_page

                    let iid = IndexId(index_id);
                    let root_page = self.catalog.get_index(iid).map(|index| index.root_page);
                    self.secondary_indexes.remove(&iid);
                    CatalogPersistence::apply_drop_index(
                        &self.catalog_id_btree,
                        &self.catalog_name_btree,
                        &mut self.catalog,
                        iid,
                    )
                    .await?;
                    if let Some(root_page) = root_page {
                        self.deallocate_rollback_root_pages([root_page].into())
                            .await?;
                    }
                }
                0x04 => {
                    if offset + 16 > data.len() {
                        break;
                    }
                    let index_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let root_page =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    let cid = CollectionId(collection_id);
                    let iid = IndexId(index_id);
                    if let Some(primary) = self.primary_indexes.get(&cid) {
                        let idx_btree = self.storage.open_btree(root_page);
                        let secondary =
                            Arc::new(SecondaryIndex::new(idx_btree, Arc::clone(primary)));
                        self.secondary_indexes.insert(iid, secondary);
                    }
                    let meta = IndexMeta {
                        index_id: iid,
                        collection_id: cid,
                        name,
                        field_paths,
                        root_page,
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
                _ => break,
            }
        }

        Ok(())
    }

    async fn deallocate_rollback_root_pages(
        &self,
        root_pages: BTreeSet<u32>,
    ) -> std::io::Result<()> {
        if root_pages.is_empty() {
            return Ok(());
        }

        let mut free_list = self.storage.free_list().lock().await;
        free_list.rebuild_including_pages(root_pages).await?;
        drop(free_list);
        self.storage.sync_file_header().await?;
        Ok(())
    }

    /// Replay a TxCommit WAL record.
    async fn replay_tx_commit(&mut self, payload: &[u8]) -> std::io::Result<()> {
        // deserialize_wal_payload returns:
        // (version: u8, commit_ts: Ts, mutations: Vec<(CollectionId, DocId, u8, Option<Vec<u8>>)>, catalog_bytes: Vec<u8>)
        let (_version, commit_ts, mutations, catalog_bytes) = deserialize_wal_payload(payload)
            .map_err(|e| {
                std::io::Error::other(format!("failed to deserialize WAL payload: {e}"))
            })?;

        // Track highest timestamp
        if commit_ts > self.recovered_ts {
            self.recovered_ts = commit_ts;
        }

        self.replayed_commits.push(ReplayedCommit {
            commit_ts,
            mutations: mutations
                .iter()
                .map(
                    |(collection_id, doc_id, _op_tag, body)| ReplayedDataMutation {
                        collection_id: *collection_id,
                        doc_id: *doc_id,
                        body: body.clone(),
                    },
                )
                .collect(),
            catalog_bytes: catalog_bytes.clone(),
        });

        // Step 1: Apply catalog mutations FIRST
        self.replay_catalog_mutations(&catalog_bytes).await?;

        // Step 2: Apply data mutations
        let external_threshold = self.storage.config().page_size / 4;
        for (collection_id, doc_id, op_tag, body_bytes) in &mutations {
            // Ensure primary index handle exists
            if !self.primary_indexes.contains_key(collection_id)
                && let Some(coll) = self.catalog.get_collection(*collection_id)
            {
                let btree = self.storage.open_btree(coll.primary_root_page);
                let primary = Arc::new(PrimaryIndex::new(
                    btree,
                    Arc::clone(&self.storage),
                    external_threshold,
                ));
                self.primary_indexes.insert(*collection_id, primary);
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
        let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let provisional_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let _primary_root_page =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let _created_at_root_page =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    let cid = CollectionId(provisional_id);

                    // Idempotency: skip catalog mutation if already loaded from
                    // checkpoint B-trees, but still ensure in-memory handles exist
                    // so that subsequent data mutations in this WAL replay succeed.
                    if self.catalog.get_collection(cid).is_some() {
                        if !self.primary_indexes.contains_key(&cid) {
                            let coll = self.catalog.get_collection(cid).unwrap();
                            let external_threshold = self.storage.config().page_size / 4;
                            let btree = self.storage.open_btree(coll.primary_root_page);
                            let primary = Arc::new(PrimaryIndex::new(
                                btree,
                                Arc::clone(&self.storage),
                                external_threshold,
                            ));
                            self.primary_indexes.insert(cid, Arc::clone(&primary));

                            // Also ensure secondary index handles exist
                            for idx in self.catalog.list_indexes(cid) {
                                if !self.secondary_indexes.contains_key(&idx.index_id) {
                                    let idx_btree = self.storage.open_btree(idx.root_page);
                                    let secondary = Arc::new(SecondaryIndex::new(
                                        idx_btree,
                                        Arc::clone(&primary),
                                    ));
                                    self.secondary_indexes.insert(idx.index_id, secondary);
                                }
                            }
                        }
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
                    let collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    // Skip name_len + name
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let dropped_count =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    for _ in 0..dropped_count {
                        if offset + 8 > data.len() {
                            break;
                        }
                        offset += 8; // index_id
                        if offset + 4 > data.len() {
                            break;
                        }
                        let nl = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap())
                            as usize;
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
                    let provisional_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
                    let _root_page =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    let cid = CollectionId(collection_id);
                    let iid = IndexId(provisional_id);

                    // Idempotency: skip catalog mutation if already loaded from
                    // checkpoint B-trees, but ensure in-memory handle exists.
                    if self.catalog.get_index(iid).is_some() {
                        if !self.secondary_indexes.contains_key(&iid)
                            && let Some(idx) = self.catalog.get_index(iid)
                            && let Some(primary) = self.primary_indexes.get(&cid)
                        {
                            let idx_btree = self.storage.open_btree(idx.root_page);
                            let secondary =
                                Arc::new(SecondaryIndex::new(idx_btree, Arc::clone(primary)));
                            self.secondary_indexes.insert(iid, secondary);
                        }
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
                    let index_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let _collection_id =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    if offset + 4 > data.len() {
                        break;
                    }
                    let name_len =
                        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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
        let index_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
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
            WAL_RECORD_CHECKPOINT | WAL_RECORD_ROLLBACK_VACUUM => {}
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
    if offset + 4 > data.len() {
        return (result, offset);
    }
    let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    for _ in 0..count {
        if offset + 4 > data.len() {
            break;
        }
        let seg_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let mut segments = Vec::new();
        for _ in 0..seg_count {
            if offset + 4 > data.len() {
                break;
            }
            let seg_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
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
    if offset + 4 > data.len() {
        return offset;
    }
    let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    for _ in 0..count {
        if offset + 4 > data.len() {
            break;
        }
        let seg_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        for _ in 0..seg_count {
            if offset + 4 > data.len() {
                break;
            }
            let seg_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
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
    use exdb_core::encoding::{decode_document, encode_document};
    use exdb_core::types::Scalar;
    use exdb_docstore::{encode_key_prefix, successor_key};
    use exdb_storage::btree::ScanDirection;
    use exdb_storage::engine::StorageConfig;
    use exdb_storage::wal::{
        WAL_RECORD_CHECKPOINT, WAL_RECORD_CREATE_COLLECTION, WAL_RECORD_CREATE_INDEX,
        WAL_RECORD_DROP_COLLECTION, WAL_RECORD_DROP_INDEX, WAL_RECORD_INDEX_READY,
        WAL_RECORD_ROLLBACK_VACUUM, WAL_RECORD_TX_COMMIT, WAL_RECORD_VACUUM,
    };
    use exdb_tx::WAL_PAYLOAD_VERSION;
    use serde_json::json;
    use std::ops::Bound;
    use tokio_stream::StreamExt;

    const TEST_DOC_ID: DocId = DocId([0xA5; 16]);

    async fn setup() -> Arc<StorageEngine> {
        Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        )
    }

    fn tx_commit_record(lsn: u64, payload: Vec<u8>) -> WalRecord {
        WalRecord {
            lsn,
            record_type: WAL_RECORD_TX_COMMIT,
            payload,
        }
    }

    fn wal_payload(
        commit_ts: Ts,
        mutations: Vec<(CollectionId, DocId, u8, Option<serde_json::Value>)>,
        catalog: Vec<u8>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(WAL_PAYLOAD_VERSION);
        buf.extend_from_slice(&commit_ts.to_le_bytes());
        buf.extend_from_slice(&(mutations.len() as u32).to_le_bytes());
        for (collection_id, doc_id, op_tag, body) in mutations {
            buf.extend_from_slice(&collection_id.0.to_le_bytes());
            buf.extend_from_slice(doc_id.as_bytes());
            buf.push(op_tag);
            if let Some(body) = body {
                let body = encode_document(&body);
                buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
                buf.extend_from_slice(&body);
            } else {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
        buf.extend_from_slice(&catalog);
        buf
    }

    fn catalog_create_collection(id: CollectionId, name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x01);
        buf.extend_from_slice(&id.0.to_le_bytes());
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    fn catalog_drop_collection(id: CollectionId, name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x02);
        buf.extend_from_slice(&id.0.to_le_bytes());
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    fn serialize_field_paths(buf: &mut Vec<u8>, field_paths: &[FieldPath]) {
        buf.extend_from_slice(&(field_paths.len() as u32).to_le_bytes());
        for path in field_paths {
            buf.extend_from_slice(&(path.segments().len() as u32).to_le_bytes());
            for segment in path.segments() {
                buf.extend_from_slice(&(segment.len() as u32).to_le_bytes());
                buf.extend_from_slice(segment.as_bytes());
            }
        }
    }

    fn catalog_create_index(
        collection_id: CollectionId,
        index_id: IndexId,
        name: &str,
        field_paths: Vec<FieldPath>,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x03);
        buf.extend_from_slice(&index_id.0.to_le_bytes());
        buf.extend_from_slice(&collection_id.0.to_le_bytes());
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        serialize_field_paths(&mut buf, &field_paths);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf
    }

    fn catalog_mutations(mut entries: Vec<Vec<u8>>) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for entry in entries.iter_mut() {
            buf.extend_from_slice(&entry[4..]);
        }
        buf
    }

    fn index_ready_record(index_id: IndexId) -> WalRecord {
        WalRecord {
            lsn: 0,
            record_type: WAL_RECORD_INDEX_READY,
            payload: index_id.0.to_le_bytes().to_vec(),
        }
    }

    fn visible_ts_record(ts: Ts) -> WalRecord {
        WalRecord {
            lsn: 0,
            record_type: WAL_RECORD_VISIBLE_TS,
            payload: ts.to_le_bytes().to_vec(),
        }
    }

    fn vacuum_record() -> WalRecord {
        WalRecord {
            lsn: 0,
            record_type: WAL_RECORD_VACUUM,
            payload: vec![1, 2, 3, 4],
        }
    }

    fn typed_record(record_type: u8) -> WalRecord {
        WalRecord {
            lsn: 0,
            record_type,
            payload: vec![1, 2, 3, 4],
        }
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
    async fn create_collection_and_insert_replayed() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        let collection_id = CollectionId(1);
        let record = tx_commit_record(
            0,
            wal_payload(
                7,
                vec![(
                    collection_id,
                    TEST_DOC_ID,
                    0x01,
                    Some(json!({"name": "Ada", "age": 37})),
                )],
                catalog_create_collection(collection_id, "users"),
            ),
        );
        handler.handle_record(&record).await.unwrap();

        let state = handler.into_state();
        let collection = state.catalog.get_collection_by_name("users").unwrap();
        assert_eq!(collection.collection_id, collection_id);
        assert_eq!(state.recovered_ts, 7);

        let primary = state.primary_indexes.get(&collection_id).unwrap();
        let body = primary.get_at_ts(&TEST_DOC_ID, 7).await.unwrap().unwrap();
        let doc = decode_document(&body).unwrap();
        assert_eq!(doc["name"], "Ada");
        assert_eq!(doc["age"], 37);
    }

    #[tokio::test]
    async fn data_without_catalog_uses_existing_collection() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();
        handler
            .handle_record(&tx_commit_record(
                1,
                wal_payload(
                    8,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x01,
                        Some(json!({"name": "Ada"})),
                    )],
                    Vec::new(),
                ),
            ))
            .await
            .unwrap();

        let state = handler.into_state();
        let primary = state.primary_indexes.get(&collection_id).unwrap();
        let body = primary.get_at_ts(&TEST_DOC_ID, 8).await.unwrap().unwrap();
        let doc = decode_document(&body).unwrap();
        assert_eq!(doc["name"], "Ada");
    }

    #[tokio::test]
    async fn replay_same_insert_twice_is_idempotent() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);
        let record = tx_commit_record(
            0,
            wal_payload(
                5,
                vec![(
                    collection_id,
                    TEST_DOC_ID,
                    0x01,
                    Some(json!({"name": "Ada"})),
                )],
                catalog_create_collection(collection_id, "users"),
            ),
        );

        handler.handle_record(&record).await.unwrap();
        handler.handle_record(&record).await.unwrap();

        let state = handler.into_state();
        let primary = state.primary_indexes.get(&collection_id).unwrap();
        let rows: Vec<_> = primary
            .scan_at_ts(5, ScanDirection::Forward)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, TEST_DOC_ID);
        assert_eq!(rows[0].1, 5);
    }

    #[tokio::test]
    async fn replace_and_delete_replayed() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x01,
                        Some(json!({"name": "Ada", "age": 37})),
                    )],
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();
        handler
            .handle_record(&tx_commit_record(
                1,
                wal_payload(
                    9,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x02,
                        Some(json!({"name": "Grace", "age": 38})),
                    )],
                    Vec::new(),
                ),
            ))
            .await
            .unwrap();
        handler
            .handle_record(&tx_commit_record(
                2,
                wal_payload(
                    12,
                    vec![(collection_id, TEST_DOC_ID, 0x03, None)],
                    Vec::new(),
                ),
            ))
            .await
            .unwrap();

        let state = handler.into_state();
        assert_eq!(state.recovered_ts, 12);
        let primary = state.primary_indexes.get(&collection_id).unwrap();

        let original = primary.get_at_ts(&TEST_DOC_ID, 5).await.unwrap().unwrap();
        let original = decode_document(&original).unwrap();
        assert_eq!(original["name"], "Ada");

        let replaced = primary.get_at_ts(&TEST_DOC_ID, 9).await.unwrap().unwrap();
        let replaced = decode_document(&replaced).unwrap();
        assert_eq!(replaced["name"], "Grace");

        assert!(primary.get_at_ts(&TEST_DOC_ID, 12).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn replay_same_delete_twice_is_idempotent() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x01,
                        Some(json!({"name": "Ada"})),
                    )],
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();

        let delete = tx_commit_record(
            1,
            wal_payload(
                8,
                vec![(collection_id, TEST_DOC_ID, 0x03, None)],
                Vec::new(),
            ),
        );
        handler.handle_record(&delete).await.unwrap();
        handler.handle_record(&delete).await.unwrap();

        let state = handler.into_state();
        let primary = state.primary_indexes.get(&collection_id).unwrap();
        assert!(primary.get_at_ts(&TEST_DOC_ID, 8).await.unwrap().is_none());
        let rows: Vec<_> = primary
            .scan_at_ts(8, ScanDirection::Forward)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn ready_secondary_index_rebuilt_after_replay() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);
        let index_id = IndexId(2);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_mutations(vec![
                        catalog_create_collection(collection_id, "users"),
                        catalog_create_index(
                            collection_id,
                            index_id,
                            "email_idx",
                            vec![FieldPath::single("email")],
                        ),
                    ]),
                ),
            ))
            .await
            .unwrap();
        handler
            .handle_record(&index_ready_record(index_id))
            .await
            .unwrap();
        handler
            .handle_record(&tx_commit_record(
                1,
                wal_payload(
                    8,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x01,
                        Some(json!({"email": "ada@example.com"})),
                    )],
                    Vec::new(),
                ),
            ))
            .await
            .unwrap();
        handler.handle_record(&visible_ts_record(8)).await.unwrap();

        handler.rebuild_ready_indexes().await.unwrap();

        let state = handler.into_state();
        let secondary = state.secondary_indexes.get(&index_id).unwrap();
        let lower = encode_key_prefix(&[Scalar::String("ada@example.com".to_string())]);
        let upper = successor_key(&lower);
        let rows: Vec<_> = secondary
            .scan_at_ts(
                Bound::Included(lower.as_slice()),
                Bound::Excluded(upper.as_slice()),
                8,
                ScanDirection::Forward,
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows, vec![(TEST_DOC_ID, 8)]);
    }

    #[tokio::test]
    async fn index_ready_replayed() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);
        let index_id = IndexId(2);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_mutations(vec![
                        catalog_create_collection(collection_id, "users"),
                        catalog_create_index(
                            collection_id,
                            index_id,
                            "email_idx",
                            vec![FieldPath::single("email")],
                        ),
                    ]),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(
            handler.catalog.get_index(index_id).unwrap().state,
            IndexState::Building,
        );

        handler
            .handle_record(&index_ready_record(index_id))
            .await
            .unwrap();

        let state = handler.into_state();
        let index = state.catalog.get_index(index_id).unwrap();
        assert_eq!(index.name, "email_idx");
        assert_eq!(index.state, IndexState::Ready);
    }

    #[tokio::test]
    async fn drop_collection_replayed_removes_catalog_and_handles() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();
        assert!(handler.catalog.get_collection(collection_id).is_some());
        assert!(handler.primary_indexes.contains_key(&collection_id));

        handler
            .handle_record(&tx_commit_record(
                1,
                wal_payload(
                    9,
                    Vec::new(),
                    catalog_drop_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();

        let state = handler.into_state();
        assert_eq!(state.recovered_ts, 9);
        assert!(state.catalog.get_collection(collection_id).is_none());
        assert!(state.catalog.get_collection_by_name("users").is_none());
        assert!(!state.primary_indexes.contains_key(&collection_id));
        assert!(
            state.catalog.list_indexes(collection_id).is_empty(),
            "cascade drop should remove collection indexes from recovered catalog",
        );
    }

    #[tokio::test]
    async fn create_then_drop_same_collection_in_separate_txs() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();
        handler
            .handle_record(&tx_commit_record(
                1,
                wal_payload(
                    8,
                    Vec::new(),
                    catalog_drop_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();

        let state = handler.into_state();
        assert!(state.catalog.list_collections().is_empty());
        assert!(state.primary_indexes.is_empty());
        assert!(state.secondary_indexes.is_empty());
        assert_eq!(state.recovered_ts, 8);
    }

    #[tokio::test]
    async fn replay_same_create_collection_twice_is_idempotent() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);
        let record = tx_commit_record(
            0,
            wal_payload(
                5,
                vec![(
                    collection_id,
                    TEST_DOC_ID,
                    0x01,
                    Some(json!({"name": "Ada"})),
                )],
                catalog_create_collection(collection_id, "users"),
            ),
        );

        handler.handle_record(&record).await.unwrap();
        handler.handle_record(&record).await.unwrap();

        let state = handler.into_state();
        assert_eq!(state.catalog.list_collections().len(), 1);
        assert_eq!(
            state.catalog.get_collection(collection_id).unwrap().name,
            "users"
        );
        let primary = state.primary_indexes.get(&collection_id).unwrap();
        let body = primary.get_at_ts(&TEST_DOC_ID, 5).await.unwrap().unwrap();
        let doc = decode_document(&body).unwrap();
        assert_eq!(doc["name"], "Ada");
    }

    #[tokio::test]
    async fn replay_same_index_ready_twice_is_idempotent() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);
        let index_id = IndexId(2);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_mutations(vec![
                        catalog_create_collection(collection_id, "users"),
                        catalog_create_index(
                            collection_id,
                            index_id,
                            "email_idx",
                            vec![FieldPath::single("email")],
                        ),
                    ]),
                ),
            ))
            .await
            .unwrap();

        let record = index_ready_record(index_id);
        handler.handle_record(&record).await.unwrap();
        handler.handle_record(&record).await.unwrap();

        let state = handler.into_state();
        assert_eq!(
            state.catalog.get_index(index_id).unwrap().state,
            IndexState::Ready,
        );
    }

    #[tokio::test]
    async fn recovered_ts_tracks_highest_commit_ts() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        for ts in [10, 30, 20] {
            let record = tx_commit_record(ts, wal_payload(ts, Vec::new(), Vec::new()));
            handler.handle_record(&record).await.unwrap();
        }

        let state = handler.into_state();
        assert_eq!(state.recovered_ts, 30);
    }

    #[tokio::test]
    async fn recovered_ts_gt_visible_ts_triggers_rollback() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();
        let collection_id = CollectionId(1);

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    30,
                    vec![(
                        collection_id,
                        TEST_DOC_ID,
                        0x01,
                        Some(json!({"name": "Ada"})),
                    )],
                    catalog_create_collection(collection_id, "users"),
                ),
            ))
            .await
            .unwrap();
        handler.handle_record(&visible_ts_record(20)).await.unwrap();

        assert_eq!(handler.recovered_ts, 30);
        assert_eq!(handler.visible_ts, 20);
        assert!(handler.rollback_unreplicated_commits().await.unwrap());

        let state = handler.into_state();
        assert_eq!(state.recovered_ts, 20);
        assert_eq!(state.visible_ts, 20);
        assert!(state.catalog.list_collections().is_empty());
    }

    #[tokio::test]
    async fn corrupt_txcommit_payload_returns_error() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        let record = tx_commit_record(0, vec![WAL_PAYLOAD_VERSION, 1, 2, 3]);
        let err = handler.handle_record(&record).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to deserialize WAL payload"),
            "unexpected error: {err}",
        );
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
    async fn vacuum_record_is_informational_during_replay() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        handler
            .handle_record(&tx_commit_record(
                0,
                wal_payload(
                    5,
                    Vec::new(),
                    catalog_create_collection(CollectionId(1), "users"),
                ),
            ))
            .await
            .unwrap();
        handler.handle_record(&vacuum_record()).await.unwrap();

        let state = handler.into_state();
        assert!(state.catalog.get_collection(CollectionId(1)).is_some());
        assert_eq!(state.recovered_ts, 5);
    }

    #[tokio::test]
    async fn checkpoint_and_rollback_vacuum_records_are_informational_during_replay() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        for record_type in [WAL_RECORD_CHECKPOINT, WAL_RECORD_ROLLBACK_VACUUM] {
            handler
                .handle_record(&typed_record(record_type))
                .await
                .unwrap();
        }

        let state = handler.into_state();
        assert!(state.catalog.list_collections().is_empty());
        assert_eq!(state.recovered_ts, 0);
        assert_eq!(state.visible_ts, 0);
    }

    #[tokio::test]
    async fn legacy_reserved_ddl_record_types_are_skipped() {
        let storage = setup().await;
        let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage))
            .await
            .unwrap();

        for record_type in [
            WAL_RECORD_CREATE_COLLECTION,
            WAL_RECORD_DROP_COLLECTION,
            WAL_RECORD_CREATE_INDEX,
            WAL_RECORD_DROP_INDEX,
        ] {
            handler
                .handle_record(&typed_record(record_type))
                .await
                .unwrap();
        }

        let state = handler.into_state();
        assert!(state.catalog.list_collections().is_empty());
        assert_eq!(state.recovered_ts, 0);
        assert_eq!(state.visible_ts, 0);
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
        let data = 0u32.to_le_bytes();
        let (paths, offset) = parse_field_paths(&data, 0);
        assert!(paths.is_empty());
        assert_eq!(offset, 4);
    }

    #[test]
    fn parse_field_paths_single() {
        // Format: count(u32) || seg_count(u32) || seg_len(u32) || seg_bytes
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes()); // 1 field path
        data.extend_from_slice(&1u32.to_le_bytes()); // 1 segment
        data.extend_from_slice(&3u32.to_le_bytes()); // segment length 3
        data.extend_from_slice(b"abc");
        let (paths, offset) = parse_field_paths(&data, 0);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].segments(), &["abc"]);
        assert_eq!(offset, data.len());
    }

    #[test]
    fn skip_field_paths_matches_parse() {
        // 2 field paths: ["abc"] and ["x", "yz"]
        let mut data = Vec::new();
        data.extend_from_slice(&2u32.to_le_bytes()); // 2 field paths
        // First: 1 segment "abc"
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"abc");
        // Second: 2 segments "x", "yz"
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(b"x");
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(b"yz");
        let (paths, parse_offset) = parse_field_paths(&data, 0);
        let skip_offset = skip_field_paths(&data, 0);
        assert_eq!(parse_offset, skip_offset);
        assert_eq!(paths.len(), 2);
    }
}
