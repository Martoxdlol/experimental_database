# Layer 2: Storage Engine

## Purpose

Manages durable persistence: WAL, page store, buffer pool, checkpoint, crash recovery. This is the lowest I/O layer — all disk reads/writes go through here.

## Sub-Modules

### `storage/page.rs` — Slotted Page Format

```rust
pub const PAGE_HEADER_SIZE: usize = 32;

pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub flags: u8,
    pub num_slots: u16,
    pub free_space_start: u16,
    pub free_space_end: u16,
    pub prev_or_ptr: u32,       // right_sibling (leaf) or leftmost_child (internal)
    pub reserved: u32,
    pub checksum: u32,
    pub lsn: Lsn,
}

pub struct SlottedPage {
    data: [u8; PAGE_SIZE],
}

impl SlottedPage {
    pub fn header(&self) -> PageHeader;
    pub fn set_header(&mut self, h: &PageHeader);
    pub fn slot_count(&self) -> u16;
    pub fn get_slot(&self, idx: u16) -> (u16, u16);  // (offset, length)
    pub fn get_cell(&self, idx: u16) -> &[u8];
    pub fn insert_cell(&mut self, key: &[u8], value: &[u8]) -> Result<u16>;
    pub fn delete_cell(&mut self, idx: u16);
    pub fn available_space(&self) -> usize;
    pub fn compute_checksum(&self) -> u32;
    pub fn verify_checksum(&self) -> bool;
}
```

### `storage/buffer_pool.rs` — Buffer Pool

```rust
pub struct BufferPool {
    page_table: RwLock<HashMap<PageId, FrameId>>,
    frames: Vec<FrameSlot>,
    clock_hand: AtomicU32,
    data_file: File,          // positional I/O (pread/pwrite)
    page_size: usize,
}

// RAII guards
pub struct SharedPageGuard<'a> { /* ... */ }
pub struct ExclusivePageGuard<'a> { /* ... */ }

impl BufferPool {
    pub fn new(capacity: usize, page_size: usize, data_file: File) -> Self;
    pub fn fetch_page_shared(&self, page_id: PageId) -> Result<SharedPageGuard>;
    pub fn fetch_page_exclusive(&self, page_id: PageId) -> Result<ExclusivePageGuard>;
    pub fn new_page(&self) -> Result<ExclusivePageGuard>;
    pub fn flush_page(&self, page_id: PageId) -> Result<()>;
    pub fn collect_dirty_frames(&self) -> Vec<(PageId, Vec<u8>)>;  // For checkpoint
    pub fn mark_clean(&self, page_id: PageId, expected_lsn: Lsn) -> bool;
}
```

### `storage/wal.rs` — Write-Ahead Log

```rust
// WAL Record Types
pub enum WalRecordType {
    TxCommit = 0x01,
    Checkpoint = 0x02,
    CreateCollection = 0x03,
    DropCollection = 0x04,
    CreateIndex = 0x05,
    DropIndex = 0x06,
    IndexReady = 0x07,
    Vacuum = 0x08,
    CreateDatabase = 0x10,
    DropDatabase = 0x11,
}

pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: WalRecordType,
    pub payload: Vec<u8>,
}

// WAL record payloads (serialized/deserialized from binary)
pub struct TxCommitPayload {
    pub tx_id: TxId,
    pub commit_ts: Ts,
    pub mutations: Vec<WalMutation>,
    pub index_deltas: Vec<WalIndexDelta>,
}

pub struct WalMutation {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub op_type: OpType,    // Insert(0x01), Replace(0x02), Delete(0x03)
    pub body: Vec<u8>,      // BSON bytes (empty for delete)
}

pub struct WalIndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<Vec<u8>>,
    pub new_key: Option<Vec<u8>>,
}

// WAL Writer (async, group commit via mpsc)
pub struct WalWriter {
    tx: mpsc::Sender<WalWriteRequest>,
}

pub struct WalWriteRequest {
    pub record: WalRecord,
    pub response: oneshot::Sender<Lsn>,
}

impl WalWriter {
    pub async fn append(&self, record: WalRecord) -> Result<Lsn>;
    pub async fn flush(&self) -> Result<()>;
}

// WAL Reader (for recovery and replication)
pub struct WalReader { /* ... */ }

impl WalReader {
    pub fn open(wal_dir: &Path) -> Result<Self>;
    pub fn read_from(&mut self, lsn: Lsn) -> impl Iterator<Item = Result<WalRecord>>;
    pub fn raw_bytes_from(&mut self, lsn: Lsn) -> impl Iterator<Item = Result<Vec<u8>>>;
}
```

### `storage/heap.rs` — External Heap

```rust
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

pub struct HeapManager {
    buffer_pool: Arc<BufferPool>,
    free_space_map: HashMap<PageId, usize>,  // page → free bytes
}

impl HeapManager {
    pub fn store(&mut self, body: &[u8]) -> Result<HeapRef>;
    pub fn load(&self, href: &HeapRef) -> Result<Vec<u8>>;
    pub fn free(&mut self, href: &HeapRef) -> Result<()>;
}
```

### `storage/free_list.rs` — Free Page Management

```rust
pub struct FreeList {
    head: Option<PageId>,
    buffer_pool: Arc<BufferPool>,
}

impl FreeList {
    pub fn pop(&mut self) -> Result<Option<PageId>>;
    pub fn push(&mut self, page_id: PageId) -> Result<()>;
}
```

### `storage/dwb.rs` — Double-Write Buffer

```rust
pub struct DoubleWriteBuffer {
    path: PathBuf,
    page_size: usize,
}

impl DoubleWriteBuffer {
    pub fn write_pages(&self, pages: &[(PageId, &[u8])]) -> Result<()>;
    pub fn recover(&self, data_file: &File, page_size: usize) -> Result<u32>;  // returns pages restored
    pub fn truncate(&self) -> Result<()>;
}
```

### `storage/checkpoint.rs` — Checkpoint Protocol

```rust
pub struct CheckpointManager {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    dwb: DoubleWriteBuffer,
    writer_lock: Arc<tokio::sync::Mutex<()>>,
}

impl CheckpointManager {
    pub async fn run_checkpoint(&self) -> Result<Lsn>;
    pub fn should_checkpoint(&self, wal_size: u64, last_checkpoint: Instant) -> bool;
}
```

### `storage/recovery.rs` — Crash Recovery & Integrity Check

```rust
pub struct RecoveryManager { /* ... */ }

impl RecoveryManager {
    pub fn recover(db_dir: &Path, config: &DatabaseConfig) -> Result<RecoveryResult>;
    pub fn check_integrity(db_dir: &Path) -> Result<IntegrityReport>;
    pub fn repair(db_dir: &Path) -> Result<RepairReport>;
}

pub struct RecoveryResult {
    pub checkpoint_lsn: Lsn,
    pub recovered_lsn: Lsn,
    pub records_replayed: u64,
    pub dwb_pages_restored: u32,
}
```

### `storage/vacuum.rs` — Version Cleanup

```rust
pub struct VacuumTask { /* ... */ }

impl VacuumTask {
    pub async fn run_vacuum(
        &self,
        oldest_active_read_ts: Ts,
        collection_id: CollectionId,
    ) -> Result<VacuumStats>;
}
```

## Interfaces Exposed to Higher Layers

| Consumer | Interface |
|---|---|
| Layer 3 (Indexing) | `BufferPool::fetch_page_shared/exclusive`, `SlottedPage` cell operations |
| Layer 5 (Transactions) | `WalWriter::append()`, `BufferPool::fetch_page_exclusive()` |
| Layer 6 (Replication) | `WalReader::raw_bytes_from()`, `WalWriter::append()` |
| `database.rs` | `CheckpointManager`, `RecoveryManager`, `VacuumTask` |

## Key Invariants

1. **WAL-first**: no page store mutation without a prior WAL record.
2. **No-steal**: dirty pages from uncommitted transactions never reach disk.
3. **Force**: all committed data is in WAL before client notification.
4. **DWB protection**: all `data.db` writes go through the double-write buffer.
5. **Single writer**: only one task modifies pages at a time (via writer lock).
