//! Top-level storage engine facade.
//!
//! [`StorageEngine`] is the main entry point. It opens (or creates) a database
//! directory, runs crash recovery, starts the WAL writer and the single writer
//! task, and exposes an async API for DDL and DML operations.
//!
//! ## Concurrency model
//!
//! - **Reads** use [`StorageEngine::catalog()`] (lock-free ArcSwap snapshot)
//!   and [`StorageEngine::pool()`] (shared page guards) — safe from any thread.
//! - **Writes** are sent as [`CommitRequest`] messages to the writer task via
//!   an mpsc channel. The writer task applies mutations sequentially, then
//!   publishes a new catalog snapshot for readers.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use storage::engine::{StorageEngine, DatabaseConfig};
//! use storage::catalog::CollectionConfig;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = StorageEngine::open("my_db".as_ref(), DatabaseConfig::default()).await?;
//!
//! // Create a collection
//! let col = engine.create_collection("users", CollectionConfig::default()).await?;
//!
//! // Read catalog
//! let catalog = engine.catalog();
//! assert!(catalog.get_collection_by_name("users").is_some());
//!
//! // Graceful shutdown (triggers final checkpoint)
//! engine.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError, FilePageIO, PageIO};
use crate::free_list::FreePageList;
use crate::heap::ExternalHeap;
use crate::catalog::*;
use crate::file_header::FileHeader;
use crate::page::SlottedPageMut;
use crate::wal::writer::{WalWriter, WalConfig, WriteRequest};
use crate::wal::record::*;
use crate::btree::insert::btree_insert;
use crate::btree::delete::btree_delete;
use crate::btree::node::KeySizeHint;
use crate::key_encoding::encode_primary_key;
use crate::checkpoint::{run_checkpoint, CheckpointConfig};
use crate::recovery::{recover_database, write_meta_json, DatabaseMeta};

/// Database configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub page_size: u32,
    pub memory_budget: u64,
    pub max_doc_size: u32,
    pub external_threshold: u32,
    pub wal_config: WalConfig,
    pub checkpoint_config: CheckpointConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            memory_budget: 256 * 1024 * 1024,
            max_doc_size: 16 * 1024 * 1024,
            external_threshold: DEFAULT_PAGE_SIZE / 2,
            wal_config: WalConfig::default(),
            checkpoint_config: CheckpointConfig::default(),
        }
    }
}

/// Storage engine error types.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("file header error: {0}")]
    FileHeader(#[from] crate::file_header::FileHeaderError),

    #[error("recovery error: {0}")]
    Recovery(#[from] crate::recovery::RecoveryError),

    #[error("collection not found: {0:?}")]
    CollectionNotFound(CollectionId),

    #[error("index not found: {0:?}")]
    IndexNotFound(IndexId),

    #[error("collection already exists: {0}")]
    CollectionExists(String),

    #[error("writer channel closed")]
    WriterClosed,

    #[error("shutdown in progress")]
    ShuttingDown,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// A request submitted to the writer task.
pub enum CommitRequest {
    TxCommit {
        tx_commit: TxCommitRecord,
        response: oneshot::Sender<Result<Lsn, StorageError>>,
    },
    CreateCollection {
        name: String,
        config: CollectionConfig,
        response: oneshot::Sender<Result<CollectionMeta, StorageError>>,
    },
    DropCollection {
        collection_id: CollectionId,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    CreateIndex {
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        response: oneshot::Sender<Result<IndexMeta, StorageError>>,
    },
    DropIndex {
        index_id: IndexId,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    IndexReady {
        index_id: IndexId,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    Checkpoint {
        response: oneshot::Sender<Result<(), StorageError>>,
    },
}

/// State owned exclusively by the writer task.
struct WriterState {
    pool: Arc<BufferPool>,
    catalog: CatalogCache,
    free_list: FreePageList,
    heap: ExternalHeap,
    wal_sender: mpsc::Sender<WriteRequest>,
    file_header: FileHeader,
    current_lsn: Arc<AtomicU64>,
    db_dir: PathBuf,
}

/// The top-level storage engine.
pub struct StorageEngine {
    pool: Arc<BufferPool>,
    catalog: Arc<SharedCatalog>,
    writer_sender: mpsc::Sender<CommitRequest>,
    shutdown: CancellationToken,
    current_lsn: Arc<AtomicU64>,
    latest_committed_ts: Arc<AtomicU64>,
    page_size: u32,
    _wal_task: tokio::task::JoinHandle<()>,
    _writer_task: tokio::task::JoinHandle<()>,
}

impl StorageEngine {
    /// Open an existing database or create a new one.
    pub async fn open(db_dir: &Path, config: DatabaseConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(db_dir)?;

        let page_size = config.page_size;
        let data_path = db_dir.join("data.db");
        let wal_dir = db_dir.join("wal");

        let (io, file_header): (Box<dyn PageIO>, FileHeader) = if data_path.exists() {
            // Open existing.
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)?;
            let file_io = FilePageIO::new(file, page_size, 0);
            // Read page count from file size.
            let meta = std::fs::metadata(&data_path)?;
            let pc = meta.len() / page_size as u64;
            *file_io.page_count_mut() = pc;
            let header = FileHeader::read_with_fallback(&file_io, page_size)?;
            (Box::new(file_io), header)
        } else {
            // Create new.
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&data_path)?;
            // Pre-allocate: header page(0) + catalog root(1) + shadow(2) = 3 pages.
            file.set_len(3 * page_size as u64)?;

            let file_io = FilePageIO::new(file, page_size, 3);
            let header = FileHeader::new_database(page_size, PageId(1));

            // Write header to page 0.
            let header_buf = header.serialize(page_size);
            file_io.write_page(PageId::FILE_HEADER, &header_buf)?;
            // Write shadow to page 2.
            file_io.write_page(PageId(2), &header_buf)?;

            // Init catalog root as empty leaf.
            let mut leaf_buf = vec![0u8; page_size as usize];
            {
                let mut sp = SlottedPageMut::new(&mut leaf_buf, page_size);
                sp.init(PageId(1), PageType::BTreeLeaf);
                sp.compute_checksum();
            }
            file_io.write_page(PageId(1), &leaf_buf)?;
            file_io.sync_file()?;

            // Write meta.json.
            let meta = DatabaseMeta {
                checkpoint_lsn: 0,
                page_size,
                created_at: header.created_at,
            };
            write_meta_json(db_dir, &meta)?;

            (Box::new(file_io), header)
        };

        // Create buffer pool.
        let frame_count = (config.memory_budget / page_size as u64) as u32;
        let pool = Arc::new(BufferPool::new(io, page_size, frame_count.max(16)));

        // Run crash recovery.
        let mut catalog_cache = CatalogCache::new(
            file_header.next_collection_id,
            file_header.next_index_id,
        );
        let mut free_list = FreePageList::new(file_header.free_list_head);
        let mut heap = ExternalHeap::new(page_size, config.external_threshold);

        let recovery_result = recover_database(
            db_dir,
            &pool,
            &mut catalog_cache,
            &mut free_list,
            &mut heap,
            file_header.checkpoint_lsn,
        )?;

        let current_lsn = Arc::new(AtomicU64::new(recovery_result.recovered_lsn.0));
        let latest_committed_ts = Arc::new(AtomicU64::new(0));

        // Start WAL writer.
        let wal_writer = WalWriter::new(&wal_dir, config.wal_config)?;
        let wal_sender = wal_writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown_wal = shutdown.clone();
        let wal_task = tokio::spawn(async move {
            wal_writer.run(shutdown_wal).await;
        });

        // Set up writer channel and shared catalog.
        let shared_catalog = Arc::new(SharedCatalog::new(catalog_cache.clone()));
        let (writer_tx, writer_rx) = mpsc::channel(256);

        let writer_state = WriterState {
            pool: pool.clone(),
            catalog: catalog_cache,
            free_list,
            heap,
            wal_sender,
            file_header,
            current_lsn: current_lsn.clone(),
            db_dir: db_dir.to_path_buf(),
        };

        let catalog_for_writer = shared_catalog.clone();
        let shutdown_writer = shutdown.clone();
        let writer_task = tokio::spawn(async move {
            writer_task_loop(writer_state, writer_rx, catalog_for_writer, shutdown_writer).await;
        });

        Ok(Self {
            pool,
            catalog: shared_catalog,
            writer_sender: writer_tx,
            shutdown,
            current_lsn,
            latest_committed_ts,
            page_size,
            _wal_task: wal_task,
            _writer_task: writer_task,
        })
    }

    /// Read the catalog (lock-free via ArcSwap).
    pub fn catalog(&self) -> arc_swap::Guard<Arc<CatalogCache>> {
        self.catalog.read()
    }

    /// Get the buffer pool reference.
    pub fn pool(&self) -> &BufferPool {
        &self.pool
    }

    /// Current page size.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Latest committed timestamp.
    pub fn latest_committed_ts(&self) -> Timestamp {
        Timestamp(self.latest_committed_ts.load(Ordering::Acquire))
    }

    /// Current WAL LSN.
    pub fn current_lsn(&self) -> Lsn {
        Lsn(self.current_lsn.load(Ordering::Acquire))
    }

    /// Submit a transaction commit.
    pub async fn commit_tx(&self, tx_commit: TxCommitRecord) -> Result<Lsn, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::TxCommit {
                tx_commit,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Create a collection.
    pub async fn create_collection(
        &self,
        name: &str,
        config: CollectionConfig,
    ) -> Result<CollectionMeta, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::CreateCollection {
                name: name.to_string(),
                config,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Drop a collection.
    pub async fn drop_collection(&self, collection_id: CollectionId) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::DropCollection {
                collection_id,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Create an index.
    pub async fn create_index(
        &self,
        collection_id: CollectionId,
        name: &str,
        field_paths: Vec<FieldPath>,
    ) -> Result<IndexMeta, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::CreateIndex {
                collection_id,
                name: name.to_string(),
                field_paths,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Drop an index.
    pub async fn drop_index(&self, index_id: IndexId) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::DropIndex {
                index_id,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Mark an index as ready.
    pub async fn mark_index_ready(&self, index_id: IndexId) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::IndexReady {
                index_id,
                response: tx,
            })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Trigger a checkpoint.
    pub async fn checkpoint(&self) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.writer_sender
            .send(CommitRequest::Checkpoint { response: tx })
            .await
            .map_err(|_| StorageError::WriterClosed)?;
        rx.await.map_err(|_| StorageError::WriterClosed)?
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> Result<(), StorageError> {
        // Trigger final checkpoint.
        let _ = self.checkpoint().await;

        // Signal shutdown.
        self.shutdown.cancel();

        // Drop sender to unblock writer task.
        drop(self.writer_sender);

        // Wait for tasks.
        let _ = self._writer_task.await;
        let _ = self._wal_task.await;

        Ok(())
    }
}

/// Writer task loop — processes CommitRequests sequentially.
async fn writer_task_loop(
    mut state: WriterState,
    mut receiver: mpsc::Receiver<CommitRequest>,
    catalog_swap: Arc<SharedCatalog>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            req = receiver.recv() => {
                let Some(request) = req else { break };
                match request {
                    CommitRequest::TxCommit { tx_commit, response } => {
                        let result = handle_tx_commit(&mut state, &tx_commit).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::CreateCollection { name, config, response } => {
                        let result = handle_create_collection(&mut state, &name, config).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::DropCollection { collection_id, response } => {
                        let result = handle_drop_collection(&mut state, collection_id).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::CreateIndex { collection_id, name, field_paths, response } => {
                        let result = handle_create_index(&mut state, collection_id, &name, field_paths).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::DropIndex { index_id, response } => {
                        let result = handle_drop_index(&mut state, index_id).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::IndexReady { index_id, response } => {
                        let result = handle_index_ready(&mut state, index_id).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::Checkpoint { response } => {
                        let result = handle_checkpoint(&mut state);
                        let _ = response.send(result);
                    }
                }
            }
        }
    }
}

async fn handle_tx_commit(
    state: &mut WriterState,
    tx: &TxCommitRecord,
) -> Result<Lsn, StorageError> {
    // 1. Serialize and write WAL record.
    let payload = WalPayload::TxCommit(TxCommitRecord {
        tx_id: tx.tx_id,
        commit_ts: tx.commit_ts,
        mutations: tx.mutations.iter().map(|m| Mutation {
            collection_id: m.collection_id,
            doc_id: m.doc_id,
            op_type: m.op_type,
            body: m.body.clone(),
        }).collect(),
        index_deltas: tx.index_deltas.iter().map(|d| IndexDelta {
            index_id: d.index_id,
            collection_id: d.collection_id,
            doc_id: d.doc_id,
            old_key: d.old_key.clone(),
            new_key: d.new_key.clone(),
        }).collect(),
    });
    let frame = serialize_payload(WalRecordType::TxCommit, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    // 2. Apply mutations to page store.
    for mutation in &tx.mutations {
        let col = state.catalog.get_collection(mutation.collection_id)
            .ok_or(StorageError::CollectionNotFound(mutation.collection_id))?
            .clone();

        let primary_key = encode_primary_key(mutation.doc_id, tx.commit_ts);

        match mutation.op_type {
            OpType::Insert | OpType::Replace => {
                let cell = crate::recovery::build_primary_cell(
                    &primary_key,
                    &mutation.body,
                    &state.pool,
                    &mut state.free_list,
                    &mut state.heap,
                    lsn,
                )?;
                let new_root = btree_insert(
                    &state.pool,
                    col.primary_root_page,
                    &cell,
                    KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
                    &mut state.free_list,
                    lsn,
                )?;
                if new_root != col.primary_root_page {
                    state.catalog.update_collection_root(col.collection_id, new_root);
                }
            }
            OpType::Delete => {
                let mut cell = Vec::with_capacity(primary_key.0.len() + 1);
                cell.extend_from_slice(&primary_key.0);
                cell.push(cell_flags::TOMBSTONE);
                let new_root = btree_insert(
                    &state.pool,
                    col.primary_root_page,
                    &cell,
                    KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
                    &mut state.free_list,
                    lsn,
                )?;
                if new_root != col.primary_root_page {
                    state.catalog.update_collection_root(col.collection_id, new_root);
                }
            }
        }
    }

    // 3. Apply index deltas.
    for delta in &tx.index_deltas {
        let idx = state.catalog.get_index(delta.index_id)
            .ok_or(StorageError::IndexNotFound(delta.index_id))?
            .clone();

        if let Some(old_key) = &delta.old_key {
            let (new_root, _) = btree_delete(
                &state.pool,
                idx.root_page,
                old_key.as_ref(),
                KeySizeHint::VariableWithSuffix,
                &mut state.free_list,
                lsn,
            )?;
            if new_root != idx.root_page {
                state.catalog.update_index_root(delta.index_id, new_root);
            }
        }
        if let Some(new_key) = &delta.new_key {
            let new_root = btree_insert(
                &state.pool,
                idx.root_page,
                new_key.as_ref(),
                KeySizeHint::VariableWithSuffix,
                &mut state.free_list,
                lsn,
            )?;
            if new_root != idx.root_page {
                state.catalog.update_index_root(delta.index_id, new_root);
            }
        }
    }

    // 4. Update current LSN.
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(lsn)
}

async fn handle_create_collection(
    state: &mut WriterState,
    name: &str,
    config: CollectionConfig,
) -> Result<CollectionMeta, StorageError> {
    // Check for duplicates.
    if state.catalog.get_collection_by_name(name).is_some() {
        return Err(StorageError::CollectionExists(name.to_string()));
    }

    let collection_id = state.catalog.next_collection_id();

    // Write WAL record.
    let payload = WalPayload::CreateCollection(CreateCollectionRecord {
        collection_id,
        name: name.to_string(),
    });
    let frame = serialize_payload(WalRecordType::CreateCollection, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    // Allocate root pages.
    let mut root_guard = state.pool.new_page(PageType::BTreeLeaf, &mut state.free_list)?;
    let root_page = root_guard.page_id();
    {
        let mut sp = root_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }
    drop(root_guard);

    let mut cat_guard = state.pool.new_page(PageType::BTreeLeaf, &mut state.free_list)?;
    let cat_page = cat_guard.page_id();
    {
        let mut sp = cat_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }
    drop(cat_guard);

    let meta = CollectionMeta {
        collection_id,
        name: name.to_string(),
        primary_root_page: root_page,
        created_at_root_page: cat_page,
        doc_count: 0,
        config,
    };
    state.catalog.insert_collection(meta.clone());
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(meta)
}

async fn handle_drop_collection(
    state: &mut WriterState,
    collection_id: CollectionId,
) -> Result<(), StorageError> {
    if state.catalog.get_collection(collection_id).is_none() {
        return Err(StorageError::CollectionNotFound(collection_id));
    }

    let payload = WalPayload::DropCollection(DropCollectionRecord { collection_id });
    let frame = serialize_payload(WalRecordType::DropCollection, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    state.catalog.remove_collection(collection_id);
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(())
}

async fn handle_create_index(
    state: &mut WriterState,
    collection_id: CollectionId,
    name: &str,
    field_paths: Vec<FieldPath>,
) -> Result<IndexMeta, StorageError> {
    if state.catalog.get_collection(collection_id).is_none() {
        return Err(StorageError::CollectionNotFound(collection_id));
    }

    let index_id = state.catalog.next_index_id();

    let payload = WalPayload::CreateIndex(CreateIndexRecord {
        index_id,
        collection_id,
        name: name.to_string(),
        field_paths: field_paths.clone(),
    });
    let frame = serialize_payload(WalRecordType::CreateIndex, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    let mut root_guard = state.pool.new_page(PageType::BTreeLeaf, &mut state.free_list)?;
    let root_page = root_guard.page_id();
    {
        let mut sp = root_guard.as_slotted_page_mut();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }
    drop(root_guard);

    let meta = IndexMeta {
        index_id,
        collection_id,
        name: name.to_string(),
        field_paths,
        root_page,
        state: IndexState::Building,
    };
    state.catalog.insert_index(meta.clone());
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(meta)
}

async fn handle_drop_index(
    state: &mut WriterState,
    index_id: IndexId,
) -> Result<(), StorageError> {
    if state.catalog.get_index(index_id).is_none() {
        return Err(StorageError::IndexNotFound(index_id));
    }

    let payload = WalPayload::DropIndex(DropIndexRecord { index_id });
    let frame = serialize_payload(WalRecordType::DropIndex, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    state.catalog.remove_index(index_id);
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(())
}

async fn handle_index_ready(
    state: &mut WriterState,
    index_id: IndexId,
) -> Result<(), StorageError> {
    if state.catalog.get_index(index_id).is_none() {
        return Err(StorageError::IndexNotFound(index_id));
    }

    let payload = WalPayload::IndexReady(IndexReadyRecord { index_id });
    let frame = serialize_payload(WalRecordType::IndexReady, &payload);
    let lsn = WalWriter::write(&state.wal_sender, frame).await?;

    state.catalog.update_index_state(index_id, IndexState::Ready);
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(())
}

fn handle_checkpoint(state: &mut WriterState) -> Result<(), StorageError> {
    let checkpoint_lsn = Lsn(state.current_lsn.load(Ordering::Acquire));
    run_checkpoint(
        &state.db_dir,
        &state.pool,
        &mut state.file_header,
        checkpoint_lsn,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_open_new_database() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::default();
        let engine = StorageEngine::open(dir.path(), config).await.unwrap();

        assert_eq!(engine.page_size(), DEFAULT_PAGE_SIZE);
        assert_eq!(engine.current_lsn(), Lsn::ZERO);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_create_collection() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::default();
        let engine = StorageEngine::open(dir.path(), config).await.unwrap();

        let meta = engine
            .create_collection("users", CollectionConfig::default())
            .await
            .unwrap();
        assert_eq!(meta.name, "users");
        assert_eq!(meta.doc_count, 0);

        // Verify catalog sees it.
        let catalog = engine.catalog();
        assert!(catalog.get_collection_by_name("users").is_some());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_duplicate_collection() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::default();
        let engine = StorageEngine::open(dir.path(), config).await.unwrap();

        engine
            .create_collection("users", CollectionConfig::default())
            .await
            .unwrap();

        let result = engine
            .create_collection("users", CollectionConfig::default())
            .await;
        assert!(result.is_err());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_drop_collection() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::default();
        let engine = StorageEngine::open(dir.path(), config).await.unwrap();

        let meta = engine
            .create_collection("users", CollectionConfig::default())
            .await
            .unwrap();
        engine.drop_collection(meta.collection_id).await.unwrap();

        let catalog = engine.catalog();
        assert!(catalog.get_collection_by_name("users").is_none());

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_create_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = DatabaseConfig::default();
        let engine = StorageEngine::open(dir.path(), config).await.unwrap();

        let col = engine
            .create_collection("users", CollectionConfig::default())
            .await
            .unwrap();

        let idx = engine
            .create_index(col.collection_id, "email_idx", vec![vec!["email".into()]])
            .await
            .unwrap();
        assert_eq!(idx.name, "email_idx");
        assert_eq!(idx.state, IndexState::Building);

        // Mark ready.
        engine.mark_index_ready(idx.index_id).await.unwrap();
        let catalog = engine.catalog();
        let idx_meta = catalog.get_index(idx.index_id).unwrap();
        assert_eq!(idx_meta.state, IndexState::Ready);

        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_engine_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // First open: create a collection.
        {
            let config = DatabaseConfig::default();
            let engine = StorageEngine::open(dir.path(), config).await.unwrap();
            engine
                .create_collection("orders", CollectionConfig::default())
                .await
                .unwrap();
            engine.shutdown().await.unwrap();
        }

        // Second open: should recover.
        {
            let config = DatabaseConfig::default();
            let engine = StorageEngine::open(dir.path(), config).await.unwrap();
            let catalog = engine.catalog();
            assert!(
                catalog.get_collection_by_name("orders").is_some(),
                "collection should survive reopen"
            );
            engine.shutdown().await.unwrap();
        }
    }
}
