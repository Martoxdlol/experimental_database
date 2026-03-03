# 10 — Storage Engine (`storage/engine.rs`)

The top-level coordinator that ties all storage components together. One `StorageEngine` instance per open database.

## Structs

```rust
/// File header (page 0) — in-memory representation (DESIGN §2.12.3).
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub free_list_head: Option<PageId>,
    pub catalog_root_page: PageId,
    pub next_collection_id: u64,
    pub next_index_id: u64,
    pub checkpoint_lsn: Lsn,
    pub created_at: u64,
}

impl FileHeader {
    /// Read from a page buffer (page 0).
    pub fn read_from(buf: &[u8]) -> Result<Self, StorageError>;

    /// Write to a page buffer.
    pub fn write_to(&self, buf: &mut [u8]);

    /// Verify the header magic and checksum.
    pub fn verify(&self) -> Result<(), StorageError>;
}

/// Configuration for opening/creating a database.
pub struct EngineConfig {
    pub db_dir: PathBuf,
    pub page_size: u32,
    pub memory_budget: u64,       // buffer pool size
    pub wal_segment_size: u64,
    pub checkpoint_config: CheckpointConfig,
}

/// The storage engine. Owns all components for a single database.
pub struct StorageEngine {
    config: EngineConfig,
    pool: BufferPool,
    wal_writer: WalWriter,
    wal_reader: WalReader,
    dwb: DoubleWriteBuffer,
    catalog: Catalog<'static>,  // lifetime managed via self-referencing or Arc
    free_list: parking_lot::Mutex<FreeList>,
    heap: parking_lot::Mutex<ExternalHeap<'static>>,
    file_header: parking_lot::RwLock<FileHeader>,
    checkpoint_task: Option<tokio::task::JoinHandle<()>>,
    shutdown: tokio::sync::watch::Sender<bool>,
}
```

## Lifecycle

```rust
impl StorageEngine {
    /// Open an existing database or create a new one.
    ///
    /// 1. Read or create meta.json
    /// 2. Open data.db (or create with page 0 + shadow header)
    /// 3. Initialize buffer pool
    /// 4. DWB recovery (if needed)
    /// 5. Open WAL (discover segments)
    /// 6. WAL replay from checkpoint_lsn
    /// 7. Load catalog from catalog B-tree
    /// 8. Rebuild heap free space map
    /// 9. Start WAL writer
    /// 10. Start background checkpoint task
    pub async fn open(config: EngineConfig) -> Result<Self, StorageError>;

    /// Create a brand-new empty database.
    pub async fn create(config: EngineConfig) -> Result<Self, StorageError>;

    /// Graceful shutdown: flush, checkpoint, close files.
    pub async fn shutdown(self) -> Result<(), StorageError>;
}
```

## Data Operations

These are the operations called by the transaction layer above the storage engine.

```rust
impl StorageEngine {
    // ── Read operations (called by transaction read path) ───

    /// Get a document by ID at a given read timestamp.
    /// Traverses the primary B-tree, resolves MVCC version.
    pub async fn get_document(
        &self,
        collection_id: CollectionId,
        doc_id: DocId,
        read_ts: Timestamp,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    /// Scan a secondary index within a byte range at a given read timestamp.
    /// Returns a cursor that yields (doc_id, document_body) pairs.
    pub async fn index_scan(
        &self,
        index_id: IndexId,
        lower_bound: &[u8],
        upper_bound: Option<&[u8]>,
        direction: Direction,
        read_ts: Timestamp,
    ) -> Result<DocumentCursor, StorageError>;

    // ── Write operations (called at commit time) ────────────

    /// Apply a committed transaction's mutations to the page store.
    /// Called AFTER the WAL record is fsynced.
    ///
    /// For each mutation:
    ///   - Insert/Replace: write document version to primary B-tree
    ///   - Delete: write tombstone to primary B-tree
    ///   - Update secondary indexes from index deltas
    pub async fn apply_mutations(
        &self,
        commit_ts: Timestamp,
        mutations: &[Mutation],
        index_deltas: &[IndexDelta],
    ) -> Result<(), StorageError>;

    // ── WAL ─────────────────────────────────────────────────

    /// Write a WAL record and return its LSN once durably fsynced.
    pub async fn write_wal(&self, record: &WalRecord) -> Result<Lsn, StorageError>;

    // ── Catalog (delegated) ─────────────────────────────────

    pub fn catalog(&self) -> &Catalog<'_>;
    pub async fn catalog_mut(&self) -> /* guarded access to catalog */;

    // ── Checkpoint ──────────────────────────────────────────

    /// Trigger an immediate checkpoint.
    pub async fn checkpoint(&self, replica_lsns: &[Lsn]) -> Result<CheckpointResult, StorageError>;
}
```

## Document Cursor

```rust
/// Async iterator over documents from an index scan, with MVCC resolution.
pub struct DocumentCursor {
    btree_cursor: BTreeCursor<'static>,
    read_ts: Timestamp,
    pool: Arc<BufferPool>,
    // Tracks last seen doc_id to skip older versions of the same doc.
    last_doc_id: Option<DocId>,
}

impl DocumentCursor {
    /// Get the next visible document. Handles:
    /// - MVCC version resolution (skip versions > read_ts)
    /// - Deduplication (within same doc_id, take first visible)
    /// - Tombstone detection (skip deleted documents)
    /// - Secondary index verification (§3.5 step 4)
    pub async fn next(&mut self) -> Result<Option<DocumentResult>, StorageError>;
}

/// A single document result from a scan.
#[derive(Debug)]
pub struct DocumentResult {
    pub doc_id: DocId,
    pub body: Vec<u8>,     // BSON-encoded document
    pub version_ts: Timestamp,
    pub index_key: Vec<u8>, // the encoded index key (for read set recording)
}
```

## Lifetime / Ownership Notes

The `StorageEngine` owns everything. The tricky part is that several components need references to the buffer pool:
- `Catalog` needs `&BufferPool` for B-tree operations
- `ExternalHeap` needs `&BufferPool`
- `BTree` needs `&BufferPool`

**Approach**: use `Arc<BufferPool>` internally. The engine creates the pool in an `Arc` and passes clones to sub-components. This avoids self-referential lifetimes.

```rust
pub struct StorageEngine {
    pool: Arc<BufferPool>,
    // ... other fields use Arc<BufferPool> instead of references
}
```

The `Catalog`, `ExternalHeap`, and `BTree` types are adjusted to hold `Arc<BufferPool>` rather than `&'a BufferPool`.
