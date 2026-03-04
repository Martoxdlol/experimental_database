# 12 — StorageEngine Facade

Top-level entry point that ties all storage components together.

## File: `src/storage/engine.rs`

### StorageEngine

```rust
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// The top-level storage engine for a single database.
/// Owns all storage components and provides the interface
/// used by the transaction layer.
pub struct StorageEngine {
    /// Database directory path.
    db_dir: PathBuf,

    /// Page size for this database.
    page_size: u32,

    /// Buffer pool (concurrent read access).
    pool: Arc<BufferPool>,

    /// Catalog (concurrent read access via ArcSwap).
    catalog: SharedCatalog,

    /// WAL writer sender (cloneable, for submitting write requests).
    wal_sender: mpsc::Sender<WriteRequest>,

    /// Channel for submitting commit requests to the writer task.
    writer_sender: mpsc::Sender<CommitRequest>,

    /// Cancellation token for graceful shutdown.
    shutdown: CancellationToken,

    /// Current WAL LSN (updated by writer).
    current_lsn: Arc<AtomicU64>,

    /// Latest committed timestamp (advanced after commit completes).
    latest_committed_ts: Arc<AtomicU64>,
}
```

### Writer Task State (owned exclusively by the writer task)

```rust
/// State owned exclusively by the writer task.
struct WriterState {
    pool: Arc<BufferPool>,
    catalog: CatalogManager,
    free_list: FreePageList,
    heap: ExternalHeap,
    wal_sender: mpsc::Sender<WriteRequest>,
    file_header: FileHeader,
    current_lsn: Arc<AtomicU64>,
}
```

### CommitRequest

```rust
/// A request submitted to the writer task.
pub enum CommitRequest {
    /// Apply a transaction's mutations to the page store.
    TxCommit {
        tx_commit: TxCommitRecord,
        response: oneshot::Sender<Result<Lsn>>,
    },
    /// Create a collection.
    CreateCollection {
        name: String,
        config: CollectionConfig,
        response: oneshot::Sender<Result<CollectionMeta>>,
    },
    /// Drop a collection.
    DropCollection {
        collection_id: CollectionId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Create an index.
    CreateIndex {
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        response: oneshot::Sender<Result<IndexMeta>>,
    },
    /// Drop an index.
    DropIndex {
        index_id: IndexId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Mark index as ready.
    IndexReady {
        index_id: IndexId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Vacuum old versions.
    Vacuum {
        vacuum: VacuumRecord,
        response: oneshot::Sender<Result<()>>,
    },
    /// Trigger a checkpoint.
    Checkpoint {
        response: oneshot::Sender<Result<()>>,
    },
}
```

### Engine Lifecycle

```rust
impl StorageEngine {
    /// Open an existing database or create a new one.
    pub async fn open(db_dir: &Path, config: DatabaseConfig) -> Result<Self> {
        // 1. Read or create file header.
        // 2. Create buffer pool.
        // 3. Run crash recovery (DWB + WAL replay).
        // 4. Load catalog from B-tree.
        // 5. Rebuild heap free space map.
        // 6. Start WAL writer task.
        // 7. Start writer task.
        // 8. Start checkpoint background task.
        // 9. Return StorageEngine.
    }

    /// Graceful shutdown.
    pub async fn shutdown(self) -> Result<()> {
        // 1. Signal shutdown via CancellationToken.
        // 2. Run final checkpoint.
        // 3. Wait for WAL writer and writer tasks to complete.
        // 4. Flush and close data file.
    }
}
```

### Read Operations (called from any task)

```rust
impl StorageEngine {
    /// Read the catalog (lock-free via ArcSwap).
    pub fn catalog(&self) -> arc_swap::Guard<Arc<CatalogCache>>;

    /// Fetch a page for reading.
    pub fn fetch_page_shared(&self, page_id: PageId) -> Result<SharedPageGuard<'_>>;

    /// Get the buffer pool reference (for cursor/scan operations).
    pub fn pool(&self) -> &BufferPool;

    /// Current page size.
    pub fn page_size(&self) -> u32;

    /// Latest committed timestamp.
    pub fn latest_committed_ts(&self) -> Timestamp;
}
```

### Write Operations (go through writer channel)

```rust
impl StorageEngine {
    /// Submit a transaction commit to the writer.
    pub async fn commit_tx(&self, tx_commit: TxCommitRecord) -> Result<Lsn>;

    /// Create a collection.
    pub async fn create_collection(
        &self,
        name: &str,
        config: CollectionConfig,
    ) -> Result<CollectionMeta>;

    /// Drop a collection.
    pub async fn drop_collection(&self, collection_id: CollectionId) -> Result<()>;

    /// Create an index.
    pub async fn create_index(
        &self,
        collection_id: CollectionId,
        name: &str,
        field_paths: Vec<FieldPath>,
    ) -> Result<IndexMeta>;

    /// Drop an index.
    pub async fn drop_index(&self, index_id: IndexId) -> Result<()>;

    /// Mark index as ready.
    pub async fn mark_index_ready(&self, index_id: IndexId) -> Result<()>;

    /// Submit vacuum request.
    pub async fn vacuum(&self, vacuum: VacuumRecord) -> Result<()>;

    /// Trigger checkpoint manually.
    pub async fn checkpoint(&self) -> Result<()>;
}
```

### Writer Task Loop

```rust
async fn writer_task(
    mut state: WriterState,
    mut receiver: mpsc::Receiver<CommitRequest>,
    catalog_swap: SharedCatalog,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            Some(request) = receiver.recv() => {
                match request {
                    CommitRequest::TxCommit { tx_commit, response } => {
                        let result = handle_tx_commit(&mut state, &tx_commit).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.cache().clone());
                        }
                        let _ = response.send(result);
                    }
                    CommitRequest::CreateCollection { name, config, response } => {
                        let result = handle_create_collection(&mut state, &name, config).await;
                        if result.is_ok() {
                            catalog_swap.update(state.catalog.cache().clone());
                        }
                        let _ = response.send(result);
                    }
                    // ... other request types ...
                }
            }
        }
    }
}

async fn handle_tx_commit(
    state: &mut WriterState,
    tx: &TxCommitRecord,
) -> Result<Lsn> {
    // 1. Serialize WAL record.
    let record = WalRecord::tx_commit(tx);
    let frame_bytes = record.serialize();

    // 2. Write to WAL (via channel, await group commit fsync).
    let lsn = WalWriter::write(&state.wal_sender, frame_bytes).await?;

    // 3. Apply mutations to page store.
    apply_mutations(state, tx, lsn)?;

    // 4. Apply index deltas to secondary indexes.
    apply_index_deltas(state, tx, lsn)?;

    // 5. Update current LSN.
    state.current_lsn.store(lsn.0, Ordering::Release);

    Ok(lsn)
}
```

### DatabaseConfig

```rust
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub page_size: u32,           // default: 8192
    pub memory_budget: u64,       // default: 256 MB
    pub max_doc_size: u32,        // default: 16 MB
    pub external_threshold: u32,  // default: page_size / 2
    pub wal_target_segment_size: u64, // default: 64 MB
    pub checkpoint_wal_threshold: u64, // default: 64 MB
    pub checkpoint_interval: Duration, // default: 5 min
}
```

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("page error: {0}")]
    Page(#[from] PageError),

    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("file header error: {0}")]
    FileHeader(#[from] FileHeaderError),

    #[error("catalog error: {0}")]
    Catalog(String),

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
```
