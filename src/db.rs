//! Main Database handle, sessions, and startup/shutdown.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::{anyhow, Result};

use crate::catalog::Catalog;
use crate::doc::{apply_patch, parse_json};
use crate::index::make_index_key;
use crate::mvcc::TsAllocator;
use crate::query::{execute_query, plan_query};
use crate::storage::engine::StorageEngine;
use crate::storage::wal::{
    replay_committed, CatalogWalEntry, WalFsyncPolicy, WalReader, WalRecord, WalWriter,
};
use crate::subs::SubscriptionRegistry;
use crate::tx::{has_conflict, CommitLog, DocWrite, ReadSet, WriteSummary, WriteSet};
use crate::types::{
    CollectionId, DocId, Filter, IndexId, IndexSpec, IndexState, MutationCommitOutcome,
    PatchOp, QueryCommitOutcome, QueryOptions, QueryType, ReplicationConfig, Ts, TxId,
};

// ── IndexInfo ─────────────────────────────────────────────────────────────────

/// Describes a secondary index on a collection.
pub struct IndexInfo {
    pub index_id: u64,
    pub field: String,
    pub unique: bool,
    pub state: String,
}

// ── DbConfig ──────────────────────────────────────────────────────────────────

pub struct DbConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub wal_fsync: WalFsyncPolicy,
    pub replication: ReplicationConfig,
}

impl DbConfig {
    /// Create a simple in-memory-style config using a temp directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let wal_dir = data_dir.join("wal");
        DbConfig {
            wal_dir,
            data_dir,
            wal_fsync: WalFsyncPolicy::Never,
            replication: ReplicationConfig::default(),
        }
    }

    pub fn with_fsync(mut self, policy: WalFsyncPolicy) -> Self {
        self.wal_fsync = policy;
        self
    }

    pub fn with_replication(mut self, cfg: ReplicationConfig) -> Self {
        self.replication = cfg;
        self
    }
}

// ── Inner database state ──────────────────────────────────────────────────────

struct Inner {
    ts_alloc: TsAllocator,
    next_tx_id: AtomicU64,
    next_doc_counter: AtomicU64,
    catalog: Catalog,
    engine: StorageEngine,
    commit_log: CommitLog,
    subscription_registry: SubscriptionRegistry,
    wal: Arc<WalWriter>,
    /// Highest commit_ts applied from the WAL stream (used by replicas).
    applied_ts: AtomicU64,
    /// Optional broadcast channel for replication; set on primary databases.
    repl_broadcast: Option<tokio::sync::broadcast::Sender<std::sync::Arc<Vec<u8>>>>,
}

impl Inner {
    fn alloc_tx_id(&self) -> TxId {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst)
    }

    fn alloc_doc_id(&self) -> DocId {
        let ts_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let counter = self.next_doc_counter.fetch_add(1, Ordering::SeqCst);
        DocId::new(ts_micros, counter)
    }
}

// ── Database ──────────────────────────────────────────────────────────────────

/// The main database handle. Clone-able and thread-safe.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

impl Database {
    /// Open the database at the given config path. Creates directories if needed.
    pub async fn open(cfg: DbConfig) -> Result<Self> {
        // Create directories
        tokio::fs::create_dir_all(&cfg.data_dir).await?;
        tokio::fs::create_dir_all(&cfg.wal_dir).await?;

        let wal_path = cfg.wal_dir.join("wal.log");

        // Replay WAL
        let catalog = Catalog::new();
        let engine = StorageEngine::new();

        // Read all WAL records first
        let all_records = {
            let reader = WalReader::open(&wal_path)?;
            reader.read_all()?
        };

        // Replay catalog updates first (in order)
        for record in &all_records {
            if let WalRecord::CatalogUpdate(entry) = record {
                apply_catalog_update(&catalog, &engine, entry)?;
            }
        }

        // Replay committed transactions
        let committed = replay_committed(all_records);
        let mut max_ts = 1u64;

        for (commit_ts, records) in committed {
            if commit_ts == 0 {
                // Catalog-only transactions already handled
                continue;
            }
            if commit_ts > max_ts {
                max_ts = commit_ts;
            }
            for record in records {
                match record {
                    WalRecord::PutDoc { collection_id, doc_id, json, .. } => {
                        engine.apply_put(collection_id, &doc_id, commit_ts, json);
                    }
                    WalRecord::DeleteDoc { collection_id, doc_id, .. } => {
                        engine.apply_delete(collection_id, &doc_id, commit_ts);
                    }
                    _ => {}
                }
            }
        }

        // Open WAL writer
        let wal = WalWriter::open(&wal_path, cfg.wal_fsync).await?;

        // Set up replication broadcast channel for primary role.
        let repl_broadcast = match cfg.replication.role {
            crate::types::ReplicationRole::Primary => {
                let (tx, _) = tokio::sync::broadcast::channel(1024);
                Some(tx)
            }
            _ => None,
        };

        let inner = Arc::new(Inner {
            ts_alloc: TsAllocator::new(max_ts + 1),
            next_tx_id: AtomicU64::new(1),
            next_doc_counter: AtomicU64::new(1),
            catalog,
            engine,
            commit_log: CommitLog::new(),
            subscription_registry: SubscriptionRegistry::new(),
            wal: Arc::new(wal),
            applied_ts: AtomicU64::new(max_ts),
            repl_broadcast,
        });

        Ok(Database { inner })
    }

    /// Shut down the database. (Currently a no-op; future: flush WAL, join tasks.)
    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }

    /// Current applied_ts (highest commit_ts applied on this node).
    pub fn applied_ts(&self) -> Ts {
        self.inner.applied_ts.load(Ordering::Acquire)
    }

    /// Subscribe to raw WAL frame bytes streamed from the primary.
    /// Returns `None` if this database is not configured as a primary.
    pub fn replication_subscribe(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<std::sync::Arc<Vec<u8>>>> {
        self.inner.repl_broadcast.as_ref().map(|tx| tx.subscribe())
    }

    /// Apply a single WAL record received from the primary (used by replicas).
    pub fn apply_wal_record(&self, record: &WalRecord, commit_ts: Ts) -> Result<()> {
        match record {
            WalRecord::PutDoc { collection_id, doc_id, json, .. } => {
                self.inner.engine.apply_put(*collection_id, doc_id, commit_ts, json.clone());
            }
            WalRecord::DeleteDoc { collection_id, doc_id, .. } => {
                self.inner.engine.apply_delete(*collection_id, doc_id, commit_ts);
            }
            WalRecord::CatalogUpdate(entry) => {
                apply_catalog_update(&self.inner.catalog, &self.inner.engine, entry)?;
            }
            WalRecord::Commit { commit_ts: ts, .. } => {
                // Advance applied_ts to the committed timestamp.
                let mut current = self.inner.applied_ts.load(Ordering::Acquire);
                while *ts > current {
                    match self.inner.applied_ts.compare_exchange(
                        current,
                        *ts,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(v) => current = v,
                    }
                }
                // Also advance the timestamp allocator so queries use fresh snapshots.
                self.inner.ts_alloc.advance_to_at_least(*ts);
            }
            _ => {}
        }
        Ok(())
    }

    /// Get the replication broadcast sender (primary only). Used to forward WAL
    /// frames to the WalWriter which then broadcasts them after disk write.
    pub fn replication_broadcast_tx(
        &self,
    ) -> Option<tokio::sync::broadcast::Sender<std::sync::Arc<Vec<u8>>>> {
        self.inner.repl_broadcast.clone()
    }

    /// Append one WAL record, broadcasting to replicas if this is a primary.
    async fn wal_append(&self, record: &WalRecord) -> Result<()> {
        if let Some(bcast) = self.inner.repl_broadcast.clone() {
            self.inner.wal.append_broadcast(record, bcast).await
        } else {
            self.inner.wal.append(record).await
        }
    }

    // ── Collection management ─────────────────────────────────────────────────

    pub async fn create_collection(&self, name: &str) -> Result<()> {
        let id = self.inner.catalog.create_collection(name)?;
        self.inner.engine.create_collection(id);

        // Write to WAL
        self.wal_append(&WalRecord::CatalogUpdate(CatalogWalEntry::CreateCollection {
            id,
            name: name.to_string(),
        }))
        .await?;

        Ok(())
    }

    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let id = self.inner.catalog.delete_collection_by_name(name)?;
        self.inner.engine.drop_collection(id);

        self.wal_append(&WalRecord::CatalogUpdate(CatalogWalEntry::DeleteCollection { id }))
            .await?;

        Ok(())
    }

    /// Return an alphabetically sorted list of all collection names.
    pub fn list_collections(&self) -> Vec<String> {
        let mut names = self.inner.catalog.collection_names();
        names.sort();
        names
    }

    /// Return metadata for all secondary indexes on `collection`.
    pub fn list_indexes(&self, collection: &str) -> Result<Vec<IndexInfo>> {
        let meta = self.inner.catalog.get_collection_by_name(collection)?;
        let mut infos: Vec<IndexInfo> = meta
            .indexes
            .values()
            .map(|idx| IndexInfo {
                index_id: idx.index_id,
                field: idx.spec.field.0.join("."),
                unique: idx.spec.unique,
                state: format!("{:?}", idx.state),
            })
            .collect();
        infos.sort_by_key(|i| i.index_id);
        Ok(infos)
    }

    // ── Index management ──────────────────────────────────────────────────────

    pub async fn create_index(&self, collection: &str, spec: IndexSpec) -> Result<IndexId> {
        let start_ts = self.inner.ts_alloc.current_ts();
        let collection_meta = self.inner.catalog.get_collection_by_name(collection)?;

        let index_id = self.inner.catalog.add_index(collection, spec.clone(), start_ts)?;

        // Add index store to engine
        if let Some(col_store) = self.inner.engine.get_collection(collection_meta.id) {
            col_store.add_index(index_id);
        }

        // Write catalog update to WAL
        self.wal_append(&WalRecord::CatalogUpdate(CatalogWalEntry::CreateIndex {
            collection_id: collection_meta.id,
            index_id,
            field_path: spec.field.0,
            unique: spec.unique,
            state_tag: 0, // Building
            state_ts: start_ts,
        }))
        .await?;

        // Spawn background index build task
        let db = self.clone();
        let collection_name = collection.to_string();
        tokio::spawn(async move {
            if let Err(e) = db.build_index_background(collection_name, index_id, start_ts).await {
                tracing::error!("index build failed for index {index_id}: {e}");
            }
        });

        Ok(index_id)
    }

    /// Background index build: snapshot at start_ts, then mark ready.
    async fn build_index_background(
        &self,
        collection: String,
        index_id: IndexId,
        build_ts: Ts,
    ) -> Result<()> {
        let collection_meta = self.inner.catalog.get_collection_by_name(&collection)?;
        let collection_id = collection_meta.id;

        let index_meta = collection_meta
            .indexes
            .get(&index_id)
            .cloned()
            .ok_or_else(|| anyhow!("index {} not found", index_id))?;

        let col_store = self
            .inner
            .engine
            .get_collection(collection_id)
            .ok_or_else(|| anyhow!("collection not found"))?;

        let index_store = col_store
            .get_index_store(index_id)
            .ok_or_else(|| anyhow!("index store not found"))?;

        // Snapshot scan at build_ts
        let field = index_meta.spec.field.clone();
        col_store.scan_all_docs_at(build_ts, |doc_id, ts, val| {
            if val.is_tombstone() {
                return;
            }
            if let Some(json) = val.payload {
                if let Some(scalar) = crate::doc::extract_scalar(&json, &field) {
                    let key = make_index_key(&scalar, &doc_id, ts);
                    let mut store = index_store.write();
                    store.insert(key, ());
                }
            }
        });

        // Mark index as Ready
        let ready_ts = self.inner.ts_alloc.current_ts();
        self.inner.catalog.set_index_ready(collection_id, index_id, ready_ts)?;

        self.wal_append(&WalRecord::CatalogUpdate(CatalogWalEntry::SetIndexReady {
            collection_id,
            index_id,
            ready_at_ts: ready_ts,
        }))
        .await?;

        tracing::info!("index {index_id} on collection '{collection}' is ready at ts={ready_ts}");
        Ok(())
    }

    pub async fn index_state(&self, collection: &str, index_id: IndexId) -> Result<IndexState> {
        self.inner.catalog.index_state(collection, index_id)
    }

    // ── Query session ─────────────────────────────────────────────────────────

    pub async fn start_query(
        &self,
        query_type: QueryType,
        _query_id: u64,
        opts: QueryOptions,
    ) -> Result<QuerySession> {
        let start_ts = self.inner.ts_alloc.current_ts();
        Ok(QuerySession {
            db: self.clone(),
            start_ts,
            query_type,
            opts,
            read_set: ReadSet::default(),
            invalidated: false,
        })
    }

    // ── Mutation session ──────────────────────────────────────────────────────

    pub async fn start_mutation(&self) -> Result<MutationSession> {
        let start_ts = self.inner.ts_alloc.current_ts();
        let tx_id = self.inner.alloc_tx_id();
        Ok(MutationSession {
            db: self.clone(),
            tx_id,
            start_ts,
            read_set: ReadSet::default(),
            write_set: WriteSet::default(),
        })
    }
}

// ── Apply catalog update during WAL replay ────────────────────────────────────

fn apply_catalog_update(
    catalog: &Catalog,
    engine: &StorageEngine,
    entry: &CatalogWalEntry,
) -> Result<()> {
    match entry {
        CatalogWalEntry::CreateCollection { id, name } => {
            catalog.create_collection_with_id(*id, name)?;
            engine.create_collection(*id);
        }
        CatalogWalEntry::DeleteCollection { id } => {
            let _ = catalog.delete_collection(*id); // idempotent
            engine.drop_collection(*id);
        }
        CatalogWalEntry::CreateIndex {
            collection_id,
            index_id,
            field_path,
            unique,
            state_tag,
            state_ts,
        } => {
            let spec = IndexSpec {
                field: crate::types::FieldPath(field_path.clone()),
                unique: *unique,
            };
            let state = match state_tag {
                0 => IndexState::Building { started_at_ts: *state_ts },
                1 => IndexState::Ready { ready_at_ts: *state_ts },
                _ => IndexState::Failed { message: "unknown".to_string() },
            };
            let _ = catalog.add_index_with_id(*collection_id, *index_id, spec, state);
            if let Some(col_store) = engine.get_collection(*collection_id) {
                col_store.add_index(*index_id);
            }
        }
        CatalogWalEntry::SetIndexReady { collection_id, index_id, ready_at_ts } => {
            let _ = catalog.set_index_ready(*collection_id, *index_id, *ready_at_ts);
        }
        CatalogWalEntry::SetIndexFailed { collection_id, index_id, message } => {
            let _ = catalog.set_index_failed(*collection_id, *index_id, message.clone());
        }
        CatalogWalEntry::DeleteIndex { collection_id, index_id } => {
            let _ = catalog.remove_index(*collection_id, *index_id);
            if let Some(col_store) = engine.get_collection(*collection_id) {
                col_store.remove_index(*index_id);
            }
        }
    }
    Ok(())
}

// ── QuerySession ──────────────────────────────────────────────────────────────

pub struct QuerySession {
    db: Database,
    pub start_ts: Ts,
    #[allow(dead_code)]
    query_type: QueryType,
    opts: QueryOptions,
    read_set: ReadSet,
    invalidated: bool,
}

impl QuerySession {
    pub async fn get(
        &mut self,
        collection: &str,
        id: DocId,
    ) -> Result<Option<Vec<u8>>> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let col_store = self.db.inner.engine.get_collection(col_meta.id)
            .ok_or_else(|| anyhow!("collection '{}' has no store", collection))?;

        match col_store.get_visible(&id, self.start_ts) {
            Some((ts, val)) => {
                self.read_set.add_point_read(col_meta.id, id, Some(ts));
                if val.is_tombstone() {
                    Ok(None)
                } else {
                    Ok(val.payload)
                }
            }
            None => {
                self.read_set.add_point_read(col_meta.id, id, None);
                Ok(None)
            }
        }
    }

    pub async fn find(
        &mut self,
        collection: &str,
        filter: Option<Filter>,
    ) -> Result<Vec<Vec<u8>>> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let col_store = self.db.inner.engine.get_collection(col_meta.id)
            .ok_or_else(|| anyhow!("collection '{}' has no store", collection))?;

        let plan = plan_query(&col_meta, filter.as_ref());
        let result = execute_query(
            plan,
            &col_store,
            self.start_ts,
            self.opts.limit,
            &mut self.read_set,
            col_meta.id,
        )?;

        Ok(result.docs.into_iter().map(|(_, json)| json).collect())
    }

    pub async fn find_ids(
        &mut self,
        collection: &str,
        filter: Option<Filter>,
    ) -> Result<Vec<DocId>> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let col_store = self.db.inner.engine.get_collection(col_meta.id)
            .ok_or_else(|| anyhow!("collection '{}' has no store", collection))?;

        let plan = plan_query(&col_meta, filter.as_ref());
        let result = execute_query(
            plan,
            &col_store,
            self.start_ts,
            self.opts.limit,
            &mut self.read_set,
            col_meta.id,
        )?;

        Ok(result.docs.into_iter().map(|(id, _)| id).collect())
    }

    pub async fn commit(self) -> Result<QueryCommitOutcome> {
        if self.invalidated {
            return Ok(QueryCommitOutcome::InvalidatedDuringRun);
        }

        if !self.opts.subscribe {
            return Ok(QueryCommitOutcome::NotSubscribed);
        }

        let subscription = self.db.inner.subscription_registry.register(self.read_set);
        Ok(QueryCommitOutcome::Subscribed { subscription })
    }
}

// ── MutationSession ───────────────────────────────────────────────────────────

pub struct MutationSession {
    db: Database,
    tx_id: TxId,
    pub start_ts: Ts,
    read_set: ReadSet,
    write_set: WriteSet,
}

impl MutationSession {
    /// Read a document, respecting write-ahead in this transaction.
    pub async fn get(
        &mut self,
        collection: &str,
        id: DocId,
    ) -> Result<Option<Vec<u8>>> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let col_id = col_meta.id;

        // Read-your-own-writes
        if let Some(write) = self.write_set.get(col_id, &id) {
            match write {
                DocWrite::Insert { json } | DocWrite::Update { json } => {
                    return Ok(Some(json.clone()));
                }
                DocWrite::Delete => return Ok(None),
            }
        }

        // Read from storage at start_ts
        let col_store = self.db.inner.engine.get_collection(col_id)
            .ok_or_else(|| anyhow!("collection '{}' has no store", collection))?;

        match col_store.get_visible(&id, self.start_ts) {
            Some((ts, val)) => {
                self.read_set.add_point_read(col_id, id, Some(ts));
                if val.is_tombstone() {
                    Ok(None)
                } else {
                    Ok(val.payload)
                }
            }
            None => {
                self.read_set.add_point_read(col_id, id, None);
                Ok(None)
            }
        }
    }

    pub async fn find_ids(
        &mut self,
        collection: &str,
        filter: Option<Filter>,
        limit: Option<usize>,
    ) -> Result<Vec<DocId>> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let col_store = self.db.inner.engine.get_collection(col_meta.id)
            .ok_or_else(|| anyhow!("collection '{}' has no store", collection))?;

        let plan = plan_query(&col_meta, filter.as_ref());
        let mut read_set = std::mem::take(&mut self.read_set);
        let result = execute_query(
            plan,
            &col_store,
            self.start_ts,
            limit,
            &mut read_set,
            col_meta.id,
        )?;
        self.read_set = read_set;

        Ok(result.docs.into_iter().map(|(id, _)| id).collect())
    }

    pub async fn insert(
        &mut self,
        collection: &str,
        json: Vec<u8>,
    ) -> Result<DocId> {
        // Validate JSON
        let _ = parse_json(&json)?;

        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        let doc_id = self.db.inner.alloc_doc_id();

        self.write_set.put(col_meta.id, doc_id, DocWrite::Insert { json });
        Ok(doc_id)
    }

    pub async fn patch(
        &mut self,
        collection: &str,
        id: DocId,
        op: PatchOp,
    ) -> Result<()> {
        let existing = self.get(collection, id).await?;
        let new_json = apply_patch(existing.as_deref(), &op)?;

        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        self.write_set.put(col_meta.id, id, DocWrite::Update { json: new_json });
        Ok(())
    }

    pub async fn delete(
        &mut self,
        collection: &str,
        id: DocId,
    ) -> Result<()> {
        let col_meta = self.db.inner.catalog.get_collection_by_name(collection)?;
        self.write_set.put(col_meta.id, id, DocWrite::Delete);
        Ok(())
    }

    pub async fn commit(self) -> Result<MutationCommitOutcome> {
        let db = self.db;
        let write_set = self.write_set;
        let read_set = self.read_set;
        let start_ts = self.start_ts;
        let tx_id = self.tx_id;

        // Allocate commit timestamp
        let commit_ts = db.inner.ts_alloc.next();

        // Validate for conflicts
        if has_conflict(&read_set, start_ts, commit_ts, &db.inner.commit_log) {
            // Abort
            db.inner.wal.append(&WalRecord::Abort { tx_id }).await?;
            return Ok(MutationCommitOutcome::Conflict);
        }

        // Build WAL records for this transaction
        let mut wal_records = vec![WalRecord::Begin { tx_id, start_ts }];
        let mut doc_writes = Vec::new();

        for ((col_id, doc_id), write) in write_set.iter_writes() {
            match write {
                DocWrite::Insert { json } | DocWrite::Update { json } => {
                    wal_records.push(WalRecord::PutDoc {
                        tx_id,
                        collection_id: *col_id,
                        doc_id: *doc_id,
                        json: json.clone(),
                    });
                    doc_writes.push((*col_id, *doc_id));
                }
                DocWrite::Delete => {
                    wal_records.push(WalRecord::DeleteDoc {
                        tx_id,
                        collection_id: *col_id,
                        doc_id: *doc_id,
                    });
                    doc_writes.push((*col_id, *doc_id));
                }
            }
        }
        wal_records.push(WalRecord::Commit { tx_id, commit_ts });

        // Append to WAL (durable first), and broadcast to replicas if primary.
        if let Some(bcast) = db.inner.repl_broadcast.clone() {
            db.inner.wal.append_batch_broadcast(&wal_records, bcast).await?;
        } else {
            db.inner.wal.append_batch(&wal_records).await?;
        }

        // Apply to in-memory engine and update secondary indexes
        for ((col_id, doc_id), write) in write_set.iter_writes() {
            match write {
                DocWrite::Insert { json } | DocWrite::Update { json } => {
                    // Update secondary indexes first
                    update_indexes_for_write(&db, *col_id, doc_id, Some(json), commit_ts);
                    db.inner.engine.apply_put(*col_id, doc_id, commit_ts, json.clone());
                }
                DocWrite::Delete => {
                    update_indexes_for_write(&db, *col_id, doc_id, None, commit_ts);
                    db.inner.engine.apply_delete(*col_id, doc_id, commit_ts);
                }
            }
        }

        // Record in commit log for future conflict detection
        let summary = WriteSummary { commit_ts, doc_writes };
        db.inner.commit_log.append(summary.clone());

        // Invalidate subscriptions
        db.inner.subscription_registry.invalidate(&summary);

        Ok(MutationCommitOutcome::Committed { commit_ts })
    }
}

/// Update all ready secondary indexes for a document write.
fn update_indexes_for_write(
    db: &Database,
    collection_id: CollectionId,
    doc_id: &DocId,
    new_json: Option<&Vec<u8>>,
    commit_ts: Ts,
) {
    let col_meta = match db.inner.catalog.get_collection_by_id(collection_id) {
        Some(m) => m,
        None => return,
    };
    let col_store = match db.inner.engine.get_collection(collection_id) {
        Some(s) => s,
        None => return,
    };

    for (index_id, idx_meta) in &col_meta.indexes {
        if !matches!(idx_meta.state, IndexState::Ready { .. }) {
            continue;
        }
        let index_store = match col_store.get_index_store(*index_id) {
            Some(s) => s,
            None => continue,
        };

        // Remove old index entry (if any previous version exists)
        // We look up the previous version
        // For simplicity, we just add the new entry; old entries are filtered during scan
        // via the MVCC visibility check (the index has inv_ts so old entries aren't visible).

        // Insert new index entry (if not a delete)
        if let Some(json) = new_json {
            if let Some(scalar) = crate::doc::extract_scalar(json, &idx_meta.spec.field) {
                let key = make_index_key(&scalar, doc_id, commit_ts);
                let mut store = index_store.write();
                store.insert(key, ());
            }
        }
    }
}
