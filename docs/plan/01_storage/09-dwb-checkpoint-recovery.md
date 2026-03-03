# 09 — DWB, Checkpoint, and Recovery

Three files: `dwb.rs`, `checkpoint.rs`, `recovery.rs`.

---

## Double-Write Buffer (`storage/dwb.rs`)

Implements DESIGN §2.9.1 — torn write protection.

```rust
/// DWB file header (16 bytes).
#[derive(Debug, Clone, Copy)]
pub struct DwbHeader {
    pub magic: u32,        // 0x44574200
    pub version: u16,      // 1
    pub page_size: u16,
    pub page_count: u32,
    pub checksum: u32,     // CRC-32C of header fields
}

/// A single entry in the DWB file.
#[derive(Debug)]
pub struct DwbEntry {
    pub page_id: PageId,
    pub data: Vec<u8>,     // full page data (page_size bytes)
}

/// Operations on the data.dwb file.
pub struct DoubleWriteBuffer {
    path: PathBuf,
    page_size: u32,
}

impl DoubleWriteBuffer {
    pub fn new(path: PathBuf, page_size: u32) -> Self;

    /// Write dirty pages to the DWB file. Creates the file, writes header +
    /// entries sequentially, fsyncs.
    pub async fn write_pages(
        &self,
        pages: &[(PageId, &[u8])],  // (page_id, page_data)
    ) -> Result<(), StorageError>;

    /// Read all entries from an existing DWB file.
    /// Returns empty vec if file doesn't exist or is empty.
    pub async fn read_entries(&self) -> Result<Vec<DwbEntry>, StorageError>;

    /// Truncate the DWB file to zero length and fsync.
    pub async fn truncate(&self) -> Result<(), StorageError>;

    /// Whether the DWB file exists and is non-empty.
    pub async fn needs_recovery(&self) -> Result<bool, StorageError>;
}
```

---

## Checkpoint (`storage/checkpoint.rs`)

Orchestrates the checkpoint protocol (DESIGN §2.9).

```rust
/// Checkpoint configuration.
pub struct CheckpointConfig {
    pub wal_size_threshold: u64,        // default: 64 MB
    pub time_threshold: Duration,       // default: 5 minutes
}

/// Checkpoint orchestrator. Coordinates buffer pool, DWB, WAL, and file header.
pub struct Checkpointer<'a> {
    pool: &'a BufferPool,
    dwb: &'a DoubleWriteBuffer,
    wal_writer: &'a WalWriter,
    wal_reader: &'a mut WalReader,
}

impl<'a> Checkpointer<'a> {
    pub fn new(
        pool: &'a BufferPool,
        dwb: &'a DoubleWriteBuffer,
        wal_writer: &'a WalWriter,
        wal_reader: &'a mut WalReader,
    ) -> Self;

    /// Execute a full checkpoint.
    ///
    /// 1. Record checkpoint_lsn = current WAL position
    /// 2. Collect dirty frames from buffer pool
    /// 3. Write dirty pages to DWB, fsync DWB
    /// 4. Scatter-write from DWB to data.db, mark frames clean
    /// 5. fsync data.db
    /// 6. Truncate DWB, fsync
    /// 7. Write Checkpoint WAL record
    /// 8. Update file header (meta.json) with checkpoint LSN
    /// 9. Compute reclaim_lsn, delete old WAL segments
    pub async fn run(
        &mut self,
        file_header: &mut FileHeader,
        replica_lsns: &[Lsn],  // applied_lsn from each replica
    ) -> Result<CheckpointResult, StorageError>;
}

/// Result of a checkpoint operation.
#[derive(Debug)]
pub struct CheckpointResult {
    pub checkpoint_lsn: Lsn,
    pub pages_flushed: u32,
    pub segments_reclaimed: u32,
}
```

---

## Crash Recovery (`storage/recovery.rs`)

Implements DESIGN §2.10 — startup recovery.

```rust
/// Recovery result.
#[derive(Debug)]
pub struct RecoveryResult {
    pub records_replayed: u64,
    pub dwb_pages_restored: u32,
    pub final_lsn: Lsn,
}

/// Crash recovery: DWB repair + WAL replay.
pub async fn recover(
    pool: &BufferPool,
    dwb: &DoubleWriteBuffer,
    wal_reader: &WalReader,
    catalog: &mut Catalog<'_>,
    file_header: &FileHeader,
) -> Result<RecoveryResult, StorageError> {
    // 1. Read meta.json → checkpoint_lsn
    // 2. DWB recovery: if data.dwb is non-empty:
    //    - Read entries
    //    - For each: verify data.db page checksum
    //    - If checksum fails: overwrite from DWB entry
    //    - fsync data.db, truncate DWB
    // 3. WAL replay from checkpoint_lsn:
    //    - TxCommit: redo mutations in primary B-tree + index deltas in secondary B-trees
    //    - CreateCollection / DropCollection: update catalog B-tree + cache
    //    - CreateIndex / DropIndex: update catalog B-tree + cache
    //    - IndexReady: transition index Building → Ready
    //    - Vacuum: remove listed versions (idempotent)
    //    - Checkpoint: no-op
    // 4. Return recovery stats
}

/// Replay a single WAL record against the storage engine.
/// Called during crash recovery and by replicas applying WAL streams.
pub async fn replay_record(
    record: &WalRecord,
    lsn: Lsn,
    pool: &BufferPool,
    catalog: &mut Catalog<'_>,
    free_list: &mut FreeList,
    heap: &mut ExternalHeap<'_>,
) -> Result<(), StorageError>;
```

## Key Design Notes

1. **No undo phase**: per DESIGN §2.10, only redo is needed. The write set is never applied to pages until after WAL commit (no-steal policy).

2. **Idempotent replay**: every replay operation is idempotent. If a page's LSN is already >= the WAL record's LSN, the record is skipped for that page.

3. **Page LSN check**: before applying a WAL record to a page, compare the page's `lsn` field with the record's LSN. If `page.lsn >= record_lsn`, skip (already applied by a previous checkpoint or recovery).
