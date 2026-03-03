# 03 — Write-Ahead Log (`storage/wal/`)

Four files: `record.rs`, `segment.rs`, `writer.rs`, `reader.rs`.

## WAL Record Types (`wal/record.rs`)

### Structs

```rust
/// Deserialized WAL record — the in-memory representation of a single WAL entry.
#[derive(Debug, Clone)]
pub enum WalRecord {
    TxCommit(TxCommitRecord),
    Checkpoint(CheckpointRecord),
    CreateCollection(CreateCollectionRecord),
    DropCollection(DropCollectionRecord),
    CreateIndex(CreateIndexRecord),
    DropIndex(DropIndexRecord),
    IndexReady(IndexReadyRecord),
    Vacuum(VacuumRecord),
    CreateDatabase(CreateDatabaseRecord),
    DropDatabase(DropDatabaseRecord),
}

#[derive(Debug, Clone)]
pub struct TxCommitRecord {
    pub tx_id: u64,
    pub commit_ts: Timestamp,
    pub mutations: Vec<Mutation>,
    pub index_deltas: Vec<IndexDelta>,
}

#[derive(Debug, Clone)]
pub struct Mutation {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub op_type: OpType,
    pub body: Vec<u8>,  // BSON-encoded, empty for Delete
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Insert  = 0x01,
    Replace = 0x02,
    Delete  = 0x03,
}

#[derive(Debug, Clone)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,  // encoded index key
    pub new_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct CheckpointRecord {
    pub checkpoint_lsn: Lsn,
}

#[derive(Debug, Clone)]
pub struct CreateCollectionRecord {
    pub collection_id: CollectionId,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DropCollectionRecord {
    pub collection_id: CollectionId,
}

#[derive(Debug, Clone)]
pub struct CreateIndexRecord {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
}

#[derive(Debug, Clone)]
pub struct DropIndexRecord {
    pub index_id: IndexId,
}

#[derive(Debug, Clone)]
pub struct IndexReadyRecord {
    pub index_id: IndexId,
}

#[derive(Debug, Clone)]
pub struct VacuumRecord {
    pub collection_id: CollectionId,
    pub entries: Vec<VacuumEntry>,
}

#[derive(Debug, Clone)]
pub struct VacuumEntry {
    pub doc_id: DocId,
    pub removed_ts: Timestamp,
    pub index_keys: Vec<VacuumIndexKey>,
}

#[derive(Debug, Clone)]
pub struct VacuumIndexKey {
    pub index_id: IndexId,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CreateDatabaseRecord {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub config: Vec<u8>,  // BSON-encoded DatabaseConfig
}

#[derive(Debug, Clone)]
pub struct DropDatabaseRecord {
    pub database_id: DatabaseId,
}
```

### Serialization Trait

```rust
/// Binary serialization for WAL record payloads.
/// All implementations follow DESIGN §2.8.4 encoding conventions.
impl WalRecord {
    /// Serialize the record into a byte buffer (payload only, no frame header).
    pub fn serialize(&self, buf: &mut Vec<u8>);

    /// Deserialize a record from payload bytes + record_type tag.
    pub fn deserialize(record_type: WalRecordType, payload: &[u8]) -> Result<Self, WalError>;

    /// Return the record type code.
    pub fn record_type(&self) -> WalRecordType;
}

/// Serialize a complete WAL frame: 9-byte header + payload.
/// Returns the serialized bytes (frame_header || payload).
pub fn serialize_wal_frame(record: &WalRecord) -> Vec<u8>;

/// Parse one WAL frame from a byte slice. Returns (record, bytes_consumed).
/// Verifies CRC-32C.
pub fn deserialize_wal_frame(data: &[u8]) -> Result<(WalRecord, usize), WalError>;
```

---

## WAL Segment (`wal/segment.rs`)

```rust
/// Header of a WAL segment file.
#[derive(Debug, Clone, Copy)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u16,
    pub segment_id: u32,
    pub base_lsn: Lsn,
    pub created_at_ms: u64,
}

/// Handle to an open WAL segment file.
pub struct WalSegment {
    file: tokio::fs::File,
    header: SegmentHeader,
    write_offset: u64,  // current append position
}

impl WalSegment {
    /// Create a new segment file at the given path with the given header.
    /// Pre-allocates to WAL_TARGET_SEGMENT_SIZE.
    pub async fn create(
        path: &Path,
        segment_id: u32,
        base_lsn: Lsn,
    ) -> Result<Self, WalError>;

    /// Open an existing segment file for reading/appending.
    pub async fn open(path: &Path) -> Result<Self, WalError>;

    pub fn header(&self) -> &SegmentHeader;

    /// Current write position as an LSN.
    pub fn current_lsn(&self) -> Lsn;

    /// Append raw bytes (already serialized WAL frames) to the segment.
    /// Does NOT fsync — the caller batches fsyncs.
    pub async fn append(&mut self, data: &[u8]) -> Result<(), WalError>;

    /// fsync the segment file.
    pub async fn sync(&self) -> Result<(), WalError>;

    /// Whether the segment has exceeded the target size.
    pub fn should_rollover(&self) -> bool;

    /// Read bytes starting at the given file offset.
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize, WalError>;
}
```

---

## WAL Writer (`wal/writer.rs`)

The single-writer with group commit (DESIGN §2.8.6).

```rust
/// A pending WAL write request.
struct WriteRequest {
    frames: Vec<u8>,                           // serialized WAL frame(s)
    notify: tokio::sync::oneshot::Sender<Lsn>, // assigned LSN on completion
}

/// The WAL writer task. Owns the active segment and handles group commit.
pub struct WalWriter {
    /// Send side for submitting write requests.
    tx: mpsc::Sender<WriteRequest>,
    /// Handle to the background writer task.
    task: tokio::task::JoinHandle<()>,
}

/// Configuration for the WAL writer.
pub struct WalWriterConfig {
    pub wal_dir: PathBuf,
    pub segment_target_size: u64,
}

impl WalWriter {
    /// Start the WAL writer background task. Opens/creates the active segment.
    pub async fn start(config: WalWriterConfig) -> Result<Self, WalError>;

    /// Submit a WAL record for writing. Returns the assigned LSN once
    /// the record is durably fsynced.
    pub async fn write(&self, record: &WalRecord) -> Result<Lsn, WalError>;

    /// Submit multiple records as a batch (single fsync).
    pub async fn write_batch(&self, records: &[WalRecord]) -> Result<Vec<Lsn>, WalError>;

    /// Current LSN (next write position).
    pub fn current_lsn(&self) -> Lsn;

    /// Graceful shutdown — flush pending writes, fsync, close segment.
    pub async fn shutdown(self) -> Result<(), WalError>;
}
```

**Writer task loop** (internal):

```
loop {
    // 1. Block until at least one request arrives
    // 2. Drain all pending requests (greedy)
    // 3. Concatenate all frames into a single buffer
    // 4. Append to active segment
    // 5. If segment should rollover: seal + create new segment
    // 6. fsync
    // 7. Notify each request with its assigned LSN
}
```

---

## WAL Reader (`wal/reader.rs`)

```rust
/// Forward-scanning WAL reader. Reads records sequentially from an LSN.
pub struct WalReader {
    segments: Vec<SegmentInfo>,  // sorted by base_lsn
}

/// Metadata about a segment file (without opening it).
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub path: PathBuf,
    pub segment_id: u32,
    pub base_lsn: Lsn,
}

impl WalReader {
    /// Discover all segments in the WAL directory.
    pub async fn open(wal_dir: &Path) -> Result<Self, WalError>;

    /// Iterate all WAL records from `start_lsn` forward.
    /// Yields (Lsn, WalRecord) pairs. Stops on EOF or CRC mismatch.
    pub fn scan_from(&self, start_lsn: Lsn) -> WalIterator;

    /// Find the segment containing the given LSN.
    pub fn segment_for_lsn(&self, lsn: Lsn) -> Option<&SegmentInfo>;

    /// Delete segments whose max LSN is below `reclaim_lsn`.
    pub async fn reclaim_segments(&mut self, reclaim_lsn: Lsn) -> Result<u32, WalError>;
}

/// Async iterator over WAL records.
pub struct WalIterator { /* ... */ }

impl WalIterator {
    /// Read the next record. Returns None on EOF or corruption.
    pub async fn next(&mut self) -> Option<Result<(Lsn, WalRecord), WalError>>;
}
```

---

## Error Type

```rust
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CRC mismatch at LSN {lsn}: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { lsn: Lsn, expected: u32, actual: u32 },
    #[error("Invalid record type: {0:#04x}")]
    InvalidRecordType(u8),
    #[error("Corrupt segment header at {path}")]
    CorruptSegmentHeader { path: PathBuf },
    #[error("Unexpected end of data at LSN {0}")]
    UnexpectedEof(Lsn),
    #[error("Deserialization error: {0}")]
    Deserialize(String),
}
```

## Dependencies

```toml
tokio = { version = "1", features = ["fs", "sync", "rt"] }
crc32fast = "1"
thiserror = "2"
```
