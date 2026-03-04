# 03 — Write-Ahead Log (WAL)

Implements DESIGN.md §2.8. Covers segments, records, writer (group commit), and reader.

## Files

```
src/storage/wal/
  mod.rs       — re-exports
  segment.rs   — segment file format, header, open/create
  record.rs    — WAL record types, serialization/deserialization
  writer.rs    — WalWriter (single task, group commit via mpsc)
  reader.rs    — WalReader (forward scan with CRC verification)
```

## Segment File Format (§2.8.2)

### `segment.rs`

```rust
/// Segment header: 32 bytes at file offset 0.
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub magic: u32,           // 0x57414C00
    pub version: u16,         // 1
    pub _reserved: u16,
    pub segment_id: u32,      // matches filename number
    pub base_lsn: Lsn,        // LSN of first record in segment
    pub created_at_ms: u64,   // wall-clock ms
}

/// Manages a single WAL segment file.
pub struct WalSegment {
    file: std::fs::File,      // opened for read or read+append
    header: SegmentHeader,
    write_offset: u64,        // current append position (header.base_lsn + data written)
}

impl WalSegment {
    /// Create a new segment file with pre-allocation.
    pub fn create(
        dir: &Path,
        segment_id: u32,
        base_lsn: Lsn,
        target_size: u64,     // ~64 MB pre-alloc
    ) -> Result<Self>;

    /// Open an existing segment for reading.
    pub fn open_read(path: &Path) -> Result<Self>;

    /// Open the active segment for appending.
    pub fn open_append(path: &Path) -> Result<Self>;

    /// Append raw bytes (one or more serialized records). No fsync.
    pub fn append(&mut self, data: &[u8]) -> Result<()>;

    /// fsync the segment file.
    pub fn sync(&self) -> Result<()>;

    /// Current write position as LSN.
    pub fn current_lsn(&self) -> Lsn;

    /// Whether the segment has exceeded the target size.
    pub fn should_rollover(&self, target_size: u64) -> bool;

    /// Segment ID for filename construction.
    pub fn segment_id(&self) -> u32;

    /// Segment file path.
    pub fn path(&self) -> &Path;
}

/// Scan a WAL directory and return segments ordered by base_lsn.
pub fn list_segments(dir: &Path) -> Result<Vec<PathBuf>>;

/// Segment filename: segment-{id:06}.wal
pub fn segment_filename(segment_id: u32) -> String;
```

## WAL Record Format (§2.8.3–2.8.5)

### `record.rs`

```rust
/// Frame header: 9 bytes prepended to every WAL record.
/// [payload_len: u32 LE] [crc32c: u32 LE] [record_type: u8]
pub const FRAME_HEADER_SIZE: usize = 9;

/// Deserialized WAL record with its LSN.
#[derive(Debug)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: WalRecordType,
    pub payload: WalPayload,
}

/// Typed WAL record payloads.
#[derive(Debug)]
pub enum WalPayload {
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

#[derive(Debug)]
pub struct TxCommitRecord {
    pub tx_id: TxId,
    pub commit_ts: Timestamp,
    pub mutations: Vec<Mutation>,
    pub index_deltas: Vec<IndexDelta>,
}

#[derive(Debug)]
pub struct Mutation {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub op_type: OpType,
    pub body: Vec<u8>,    // BSON, empty for Delete
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Insert  = 0x01,
    Replace = 0x02,
    Delete  = 0x03,
}

#[derive(Debug)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<EncodedKey>,
    pub new_key: Option<EncodedKey>,
}

#[derive(Debug)]
pub struct CheckpointRecord {
    pub checkpoint_lsn: Lsn,
}

#[derive(Debug)]
pub struct CreateCollectionRecord {
    pub collection_id: CollectionId,
    pub name: String,
}

#[derive(Debug)]
pub struct DropCollectionRecord {
    pub collection_id: CollectionId,
}

#[derive(Debug)]
pub struct CreateIndexRecord {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
}

#[derive(Debug)]
pub struct DropIndexRecord {
    pub index_id: IndexId,
}

#[derive(Debug)]
pub struct IndexReadyRecord {
    pub index_id: IndexId,
}

#[derive(Debug)]
pub struct VacuumRecord {
    pub collection_id: CollectionId,
    pub entries: Vec<VacuumEntry>,
}

#[derive(Debug)]
pub struct VacuumEntry {
    pub doc_id: DocId,
    pub removed_ts: Timestamp,
    pub index_keys: Vec<VacuumIndexKey>,
}

#[derive(Debug)]
pub struct VacuumIndexKey {
    pub index_id: IndexId,
    pub key: EncodedKey,
}

// System-level records (§2.8.5)
#[derive(Debug)]
pub struct CreateDatabaseRecord {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub config: Vec<u8>, // BSON-encoded DatabaseConfig
}

#[derive(Debug)]
pub struct DropDatabaseRecord {
    pub database_id: DatabaseId,
}
```

### Serialization / Deserialization

```rust
impl WalRecord {
    /// Serialize into a frame: [payload_len][crc32c][record_type][payload].
    /// Returns the complete frame bytes including the 9-byte header.
    pub fn serialize(&self) -> Vec<u8>;
}

/// Deserialize a WAL record from frame bytes.
/// Verifies CRC-32C. Returns Err on corruption.
pub fn deserialize_record(frame: &[u8]) -> Result<WalRecord, WalError>;

/// Low-level payload serialization helpers (little-endian binary).
mod encoding {
    pub fn write_u8(buf: &mut Vec<u8>, v: u8);
    pub fn write_u16(buf: &mut Vec<u8>, v: u16);
    pub fn write_u32(buf: &mut Vec<u8>, v: u32);
    pub fn write_u64(buf: &mut Vec<u8>, v: u64);
    pub fn write_u128(buf: &mut Vec<u8>, v: u128);
    pub fn write_str(buf: &mut Vec<u8>, s: &str);     // u16 len + UTF-8
    pub fn write_blob(buf: &mut Vec<u8>, b: &[u8]);   // u32 len + bytes
    pub fn write_key(buf: &mut Vec<u8>, k: &EncodedKey); // u16 len + bytes
    pub fn write_field_path(buf: &mut Vec<u8>, fp: &FieldPath);

    pub fn read_u8(cursor: &mut &[u8]) -> Result<u8>;
    pub fn read_u16(cursor: &mut &[u8]) -> Result<u16>;
    pub fn read_u32(cursor: &mut &[u8]) -> Result<u32>;
    pub fn read_u64(cursor: &mut &[u8]) -> Result<u64>;
    pub fn read_u128(cursor: &mut &[u8]) -> Result<u128>;
    pub fn read_str(cursor: &mut &[u8]) -> Result<String>;
    pub fn read_blob(cursor: &mut &[u8]) -> Result<Vec<u8>>;
    pub fn read_key(cursor: &mut &[u8]) -> Result<EncodedKey>;
    pub fn read_field_path(cursor: &mut &[u8]) -> Result<FieldPath>;
}
```

## WAL Writer — Group Commit (§2.8.6)

### `writer.rs`

```rust
/// A request to write one WAL record, submitted by a committer.
pub struct WriteRequest {
    pub record_bytes: Vec<u8>,           // pre-serialized frame
    pub response_tx: oneshot::Sender<Lsn>, // assigned LSN sent back
}

/// WAL writer task — drains write queue, batches, fsyncs.
pub struct WalWriter {
    /// Bounded mpsc channel for incoming write requests.
    write_rx: mpsc::Receiver<WriteRequest>,
    /// Handle for submitting writes.
    write_tx: mpsc::Sender<WriteRequest>,
    /// Active segment being appended to.
    active_segment: WalSegment,
    /// WAL directory path.
    wal_dir: PathBuf,
    /// Next segment ID for rollover.
    next_segment_id: u32,
    /// Target segment size for rollover (~64 MB).
    target_segment_size: u64,
    /// Current LSN (next write position).
    current_lsn: Lsn,
}

impl WalWriter {
    /// Create a new WAL writer. Opens or creates the active segment.
    pub fn new(wal_dir: &Path, config: WalConfig) -> Result<Self>;

    /// Get a sender handle for submitting write requests.
    pub fn sender(&self) -> mpsc::Sender<WriteRequest>;

    /// Run the writer loop (called as a tokio::spawn task).
    /// Loop:
    ///   1. recv (blocking) — wait for at least one request.
    ///   2. try_recv (greedy drain) — collect all pending requests.
    ///   3. Append all records to the active segment.
    ///   4. fsync once.
    ///   5. Notify each request with its assigned LSN.
    ///   6. Check rollover.
    pub async fn run(mut self, shutdown: CancellationToken);

    /// Submit a write and wait for the assigned LSN.
    pub async fn write(
        sender: &mpsc::Sender<WriteRequest>,
        record_bytes: Vec<u8>,
    ) -> Result<Lsn>;
}

pub struct WalConfig {
    pub target_segment_size: u64,   // default: 64 * 1024 * 1024
    pub channel_capacity: usize,    // default: 1024
}
```

### Group Commit Flow

```
Committer tasks:
  serialize record → send to channel → await oneshot

WalWriter task (single):
  loop {
    batch = recv_one() + try_recv_all()
    for req in batch:
      lsn = current_lsn
      segment.append(req.record_bytes)
      current_lsn += req.record_bytes.len()
    segment.sync()          // one fsync for entire batch
    for (req, lsn) in zip(batch, lsns):
      req.response_tx.send(lsn)
    if segment.should_rollover():
      rollover()
  }
```

## WAL Reader (§2.8.3)

### `reader.rs`

```rust
/// Forward-scanning WAL reader. Used for crash recovery and replication.
pub struct WalReader {
    segments: Vec<WalSegment>,   // sorted by base_lsn
    current_segment_idx: usize,
    read_offset: u64,            // within current segment
}

impl WalReader {
    /// Open all segments in a WAL directory, starting from `start_lsn`.
    pub fn open(wal_dir: &Path, start_lsn: Lsn) -> Result<Self>;

    /// Read the next WAL record. Returns None at end-of-log.
    /// Verifies CRC-32C. Returns Err on corruption (reader stops).
    pub fn next(&mut self) -> Result<Option<WalRecord>>;

    /// Current read position as LSN.
    pub fn current_lsn(&self) -> Lsn;

    /// Iterator adapter.
    pub fn iter(&mut self) -> WalRecordIterator<'_>;
}
```

### Read Algorithm

```
1. Read 9-byte frame header from current offset.
2. If payload_len == 0 → end of data in this segment.
   - If there's a next segment → advance, continue.
   - Else → return None (end of log).
3. Read record_type (1 byte) + payload (payload_len bytes).
4. Compute CRC-32C over record_type || payload. Compare with crc32c from header.
5. If mismatch → return Err (corruption, stop replay).
6. Deserialize payload based on record_type.
7. Return WalRecord { lsn, record_type, payload }.
```

## Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC mismatch at LSN {lsn:?}: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { lsn: Lsn, expected: u32, computed: u32 },

    #[error("invalid record type: {0:#04x}")]
    InvalidRecordType(u8),

    #[error("truncated record at LSN {0:?}")]
    TruncatedRecord(Lsn),

    #[error("invalid segment header: {0}")]
    InvalidSegmentHeader(String),

    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(u32),
}
```
