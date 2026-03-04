# S5: Write-Ahead Log (WAL)

## Purpose

Append-only durable log for crash recovery. Records are opaque byte payloads with CRC verification. Supports group commit for throughput. Operates through the WalStorage trait.

## Dependencies

- **S1 (Backend)**: `WalStorage` trait for actual I/O

## Rust Types

```rust
use crate::storage::backend::WalStorage;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot}; // NOTE: tokio channels, not std

/// Log Sequence Number — byte offset in the logical WAL stream.
pub type Lsn = u64;

/// WAL record frame header: 9 bytes.
/// Layout: payload_len(u32 LE) + crc32c(u32 LE) + record_type(u8)
/// Serialized manually with from_le_bytes()/to_le_bytes() (hot path, simple format).
const WAL_FRAME_HEADER_SIZE: usize = 9;

/// Maximum WAL record payload size. Records exceeding this are treated
/// as corrupt during iteration. 64 MB is generous for any legitimate record.
const MAX_WAL_RECORD_SIZE: usize = 64 * 1024 * 1024;

// ─── WAL Record Type Constants ───
// Defined here as a central registry. The WAL itself treats these as opaque
// byte tags — the SEMANTICS are defined by higher layers (catalog, storage
// engine, etc.) that produce and consume these records.
pub const WAL_RECORD_TX_COMMIT: u8 = 0x01;
pub const WAL_RECORD_CHECKPOINT: u8 = 0x02;
pub const WAL_RECORD_CREATE_COLLECTION: u8 = 0x03;
pub const WAL_RECORD_DROP_COLLECTION: u8 = 0x04;
pub const WAL_RECORD_CREATE_INDEX: u8 = 0x05;
pub const WAL_RECORD_DROP_INDEX: u8 = 0x06;
pub const WAL_RECORD_INDEX_READY: u8 = 0x07;
pub const WAL_RECORD_VACUUM: u8 = 0x08;
pub const WAL_RECORD_VISIBLE_TS: u8 = 0x09;

/// WAL segment file header: 32 bytes.
/// Serialized manually with from_le_bytes()/to_le_bytes() (written once per segment).
pub struct SegmentHeader {
    pub magic: u32,          // u32 LE, 0x57414C00 ("WAL\0")
    pub version: u16,        // u16 LE, 1
    pub _reserved: u16,
    pub segment_id: u32,     // u32 LE
    pub base_lsn: Lsn,      // u64 LE
    pub created_at_ms: u64,  // u64 LE
    pub _padding: u32,       // u32 LE, reserved (zero). Brings total to 32 bytes.
}

/// A deserialized WAL record.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: u8,
    pub payload: Vec<u8>,
}

pub struct WalConfig {
    pub segment_size: usize,   // target segment size (default 64 MB)
}

// ─── Writer ───

/// WAL writer with group commit support.
/// Internally runs a background task that batches writes.
pub struct WalWriter {
    /// Channel to send write requests to the background task.
    tx: mpsc::Sender<WalWriteRequest>,
    /// Current write position (LSN of next record).
    current_lsn: Arc<AtomicU64>,
}

enum WalWriteRequest {
    /// Normal record: writer serializes the frame header + payload.
    Record {
        record_type: u8,
        payload: Vec<u8>,
        response: oneshot::Sender<Result<Lsn>>,
    },
    /// Pre-encoded raw frame (for replication). Data includes the 9-byte
    /// header + payload. Writer appends it to the batch buffer as-is.
    RawFrame {
        data: Vec<u8>,
        response: oneshot::Sender<Result<Lsn>>,
    },
}

impl WalWriter {
    /// Create a new WAL writer. Spawns a background task for group commit.
    /// Requires an active tokio runtime. Must be called from within an async
    /// context or after runtime setup.
    pub fn new(storage: Arc<dyn WalStorage>, config: WalConfig) -> Result<Self>;

    /// Append a record. Blocks until fsynced. Returns the assigned LSN.
    pub async fn append(&self, record_type: u8, payload: &[u8]) -> Result<Lsn>;

    /// Append a pre-encoded raw WAL frame (for replication).
    /// The frame bytes include the 9-byte header + payload.
    pub async fn append_raw_frame(&self, raw: &[u8]) -> Result<Lsn>;

    /// Current write position (LSN of next record to be written).
    pub fn current_lsn(&self) -> Lsn;

    /// Shut down the background writer task.
    pub async fn shutdown(&self);
}

// ─── Reader ───

/// WAL reader for sequential scanning from a given LSN.
pub struct WalReader {
    storage: Arc<dyn WalStorage>,
}

impl WalReader {
    pub fn new(storage: Arc<dyn WalStorage>) -> Self;

    /// Create an iterator starting at the given LSN.
    pub fn read_from(&self, lsn: Lsn) -> WalIterator;

    /// Find the latest valid LSN by scanning from a starting point.
    /// Returns the LSN just past the last valid record.
    pub fn find_end(&self, start_lsn: Lsn) -> Result<Lsn>;
}

/// Iterator over WAL records.
/// Owns an Arc<dyn WalStorage> so it is self-contained and can be
/// returned from functions without lifetime issues.
pub struct WalIterator {
    storage: Arc<dyn WalStorage>,
    current_lsn: Lsn,
}

impl Iterator for WalIterator {
    type Item = Result<WalRecord>;
    // Reads next record, verifies CRC, advances position.
    // Returns None at end-of-log or on first corrupt record.
}
```

## Implementation Details

### WAL Record Frame Format

```
┌───────────────────────────────────┐
│ payload_len:  u32 LE   (4 bytes) │
│ crc32c:       u32 LE   (4 bytes) │  CRC-32C(record_type || payload)
│ record_type:  u8        (1 byte) │
│ payload:      [u8; payload_len]  │
└───────────────────────────────────┘
Total: 9 + payload_len bytes
```

LSN = byte offset of the `payload_len` field in the logical stream.
Next record's LSN = current LSN + 9 + payload_len.

### Writer Background Task (Group Commit)

```
┌──────────────────────────────────────────────────┐
│              Writer Task Loop                     │
│                                                   │
│  1. Block on rx.recv() — wait for first request  │
│  2. Drain: while rx.try_recv() → collect more    │
│  3. Serialize batch:                              │
│     For each request:                             │
│       match request {                             │
│         Record { .. } =>                          │
│           - Compute CRC-32C(record_type||payload)│
│           - Write frame header + payload to buf  │
│           - Assign LSN = current write position  │
│         RawFrame { data, .. } =>                  │
│           - Append data to buffer as-is          │
│           - Assign LSN = current write position  │
│       }                                           │
│  4. WalStorage::append(entire batch buffer)      │
│  5. WalStorage::sync()         ← single fsync!  │
│  6. For each request:                             │
│       - Send LSN via oneshot response channel    │
│  7. Loop back to step 1                          │
└──────────────────────────────────────────────────┘
```

**Key properties:**
- Single fsync covers the entire batch.
- Under high concurrency, batches naturally grow (multiple sends between drains).
- Under low concurrency, single-record batches are common.
- No artificial delay — drain is greedy and immediate.

### append()

1. Serialize the record into frame bytes: `payload_len(u32) || crc32c(u32) || record_type(u8) || payload`.
2. Create a oneshot channel `(tx, rx)`.
3. Send `WalWriteRequest::Record { record_type, payload, response: tx }` to the writer channel.
4. Await `rx.recv()` → returns the assigned LSN.

### append_raw_frame()

1. Validate the frame: read payload_len from first 4 bytes, verify total length matches.
2. Optionally verify CRC.
3. Send `WalWriteRequest::RawFrame { data: raw.to_vec(), response: tx }` to the writer channel.
4. The writer task appends it to the batch buffer as-is.

### WalIterator

1. Start at `current_lsn`.
2. Read 9 bytes from `WalStorage::read_from(current_lsn)`.
3. Parse `payload_len`, `crc32c`, `record_type`.
4. If `payload_len == 0`: end of log. Return None.
4.5. If `payload_len > MAX_WAL_RECORD_SIZE` (64 MB): treat as corrupt/end-of-log. Return None.
5. Read `payload_len` bytes from offset `current_lsn + 9`.
6. Compute CRC-32C(`record_type || payload`). Compare with stored crc32c.
7. If mismatch: corrupt record. Return None (stop iteration — partial write from crash).
8. Advance `current_lsn += 9 + payload_len`.
9. Return `Some(WalRecord { lsn: original_lsn, record_type, payload })`.

### Segment Management

Segment management is handled by `FileWalStorage` (S1), not by the WAL writer. The writer just calls `storage.append()` and `storage.sync()`. The storage backend decides when to roll over segments internally.

For `MemoryWalStorage`, there are no segments — it's a single contiguous byte vector.

## WAL Write Path Diagram

```
   Thread 1              Thread 2              Writer Task
   ────────              ────────              ───────────
   append(0x01, data1)   append(0x01, data2)
     │                     │
     ▼                     ▼
   tx.send(req1)         tx.send(req2)
     │                     │
     ▼                     ▼
   await rx1             await rx2
                                               rx.recv() → req1
                                               rx.try_recv() → req2
                                               rx.try_recv() → None
                                               ┌─────────────────┐
                                               │ Batch:          │
                                               │  frame1 bytes   │
                                               │  frame2 bytes   │
                                               └────────┬────────┘
                                               storage.append(batch)
                                               storage.sync()
                                               send LSN1 → rx1
                                               send LSN2 → rx2
   ◄─── LSN1                ◄─── LSN2
```

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error on append | Disk full, backend failure | Propagate via oneshot channel |
| I/O error on sync | fsync failure | Propagate — CRITICAL, database should halt |
| CRC mismatch on read | Corrupt or partial record (crash) | Stop iteration, return None |
| payload_len = 0 | End of log (zero-fill from pre-allocation) | Stop iteration, return None |
| payload_len > 64 MB | Corrupt record (implausible size) | Stop iteration, return None |
| Channel closed | Writer task crashed | Return error on append |

## Tests

1. **Single record roundtrip**: Write one record, read back via iterator, verify type + payload.
2. **Multiple records**: Write 100 records, read all back in order, verify LSNs are ascending.
3. **CRC verification**: Write record, corrupt a byte in the backend, read → should get None (stop).
4. **Group commit batching**: Spawn 10 concurrent appends, verify all get distinct LSNs and all are readable.
5. **Empty WAL**: Create reader on empty WAL, iterate → should return None immediately.
6. **read_from mid-stream**: Write 10 records. Read from record 5's LSN. Verify only records 5-9 returned.
7. **find_end**: Write records, call find_end(0), verify it returns LSN past last record.
8. **append_raw_frame**: Encode a frame manually, append via append_raw_frame, read back, verify identical.
9. **Large payload**: Write a 1 MB payload, read back, verify.
10. **current_lsn tracking**: Verify current_lsn advances correctly after each append.
11. **Shutdown**: Call shutdown(), verify subsequent appends fail gracefully.
12. **Works with MemoryWalStorage**: All tests pass with in-memory backend.
13. **Works with FileWalStorage**: All tests pass with file backend (tempdir).
14. **Concurrent readers + writer**: Write records while simultaneously reading earlier records.
