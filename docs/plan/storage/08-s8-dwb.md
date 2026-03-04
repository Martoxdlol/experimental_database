# S8: Double-Write Buffer (DWB)

## Purpose

Torn-write protection for the page store during checkpoint. Every dirty page is written to the DWB file and fsynced before being scatter-written to `data.db`. If a crash interrupts the scatter-write, the DWB has intact copies for recovery.

**File-backed only**: In-memory backends skip DWB entirely (no torn writes possible).

## Dependencies

- **S1 (Backend)**: `PageStorage` for restoring pages during recovery

## Rust Types

```rust
use crate::storage::backend::{PageId, PageStorage};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// DWB file header: 16 bytes.
/// Serialized manually with from_le_bytes()/to_le_bytes() (written once per checkpoint).
pub struct DwbHeader {
    pub magic: u32,       // u32 LE, 0x44574200 ("DWB\0")
    pub version: u16,     // u16 LE, 1
    pub page_size: u16,   // u16 LE, must match data.db
    pub page_count: u32,  // u32 LE, number of page entries in this DWB batch
    pub checksum: u32,    // u32 LE, CRC-32C of header fields (first 12 bytes)
}

const DWB_HEADER_SIZE: usize = 16;

pub struct DoubleWriteBuffer {
    dwb_path: PathBuf,
    page_storage: Arc<dyn PageStorage>,
    page_size: usize,
}

impl DoubleWriteBuffer {
    /// Create a DWB manager for the given path.
    pub fn new(
        path: &Path,
        page_storage: Arc<dyn PageStorage>,
        page_size: usize,
    ) -> Self;

    /// Write a batch of dirty pages to the DWB file, then scatter-write to page storage.
    /// Steps:
    ///   1. Write DWB header + all pages to dwb_path sequentially
    ///   2. fsync dwb_path
    ///   3. Scatter-write each page to page_storage at its correct position
    ///   4. page_storage.sync()
    ///   5. Truncate dwb_path to 0
    ///   6. fsync dwb_path (the truncation)
    pub fn write_pages(&self, pages: &[(PageId, Vec<u8>)]) -> Result<()>;

    /// Recover from a crash: restore any torn pages in page_storage from DWB.
    /// Returns the number of pages restored.
    pub fn recover(&self) -> Result<u32>;

    /// Truncate the DWB file to zero (called after successful checkpoint).
    pub fn truncate(&self) -> Result<()>;

    /// Check if the DWB file exists and is non-empty.
    pub fn is_empty(&self) -> Result<bool>;
}
```

## DWB File Format

```
┌──────────────────────────────────────────┐
│ DWB Header (16 bytes)                    │
│   magic:       u32  (0x44574200)         │
│   version:     u16  (1)                  │
│   page_size:   u16                       │
│   page_count:  u32                       │
│   checksum:    u32                       │
├──────────────────────────────────────────┤
│ Entry[0]: page_id(u32) + page_data       │  4 + page_size bytes
│ Entry[1]: page_id(u32) + page_data       │
│ ...                                      │
│ Entry[N-1]: page_id(u32) + page_data     │
└──────────────────────────────────────────┘

Total size: 16 + page_count × (4 + page_size)
```

## Implementation Details

### write_pages()

```
Checkpoint Flush via DWB:

1. Open/create dwb_path (truncate if exists)
2. Write DWB header:
   - magic = 0x44574200
   - version = 1
   - page_size
   - page_count = pages.len()
   - checksum = CRC-32C of first 12 bytes
3. For each (page_id, page_data) in pages:
   - Write page_id as u32 LE (4 bytes)
   - Write &page_data (page_size bytes)
4. fsync dwb_path ← THIS IS THE CRITICAL POINT
   After this, all dirty pages are safely in the DWB.
5. For each (page_id, page_data) in pages:
   - page_storage.write_page(page_id, &page_data)
6. page_storage.sync()  ← data.db is now up to date
7. Truncate dwb_path to 0
8. fsync dwb_path
```

**Key invariant**: Step 4 (DWB fsync) completes before step 5 (scatter-write) begins. If crash during step 5, the DWB has intact copies.

### recover()

```
Recovery Protocol:

1. If dwb_path does not exist or file_size == 0:
   - Return 0 (clean shutdown, nothing to recover)
2. Read DWB header (16 bytes):
   - Verify magic == 0x44574200
   - Verify checksum
   - Read page_size, page_count
3. For each entry (i = 0..page_count):
   a. Read page_id (u32 LE, 4 bytes)
   b. Read page_data (page_size bytes) from DWB
   c. Verify page_data checksum (from its PageHeader)
      - If DWB page checksum invalid: skip (DWB itself was partially written — crash during step 3)
   d. Read the same page from page_storage
   e. Verify page_storage page checksum
   f. If page_storage checksum INVALID (torn write):
      - Overwrite with DWB copy: page_storage.write_page(page_id, page_data)
      - Increment restored count
   g. If page_storage checksum VALID:
      - Skip (scatter-write for this page completed before crash)
4. page_storage.sync()
5. Truncate dwb_path to 0, fsync
6. Return restored count
```

### Checkpoint Flow Diagram

```
   Checkpoint                     DWB File                  data.db
   ─────────                     ────────                  ───────
   dirty_pages() snapshot
        │
        ▼
   write DWB header ──────────► [Header]
   write entries    ──────────► [Entry0][Entry1]...[EntryN]
        │
        ▼
   fsync DWB        ──────────► ✓ DURABLE
        │
        ▼                                                 ┌──────────┐
   scatter-write pg0 ─────────────────────────────────────►│ page 0   │
   scatter-write pg1 ─────────────────────────────────────►│ page 1   │
   ...                                                    │ ...      │
   scatter-write pgN ─────────────────────────────────────►│ page N   │
        │                                                 └──────────┘
        ▼
   sync data.db     ──────────────────────────────────────► ✓ DURABLE
        │
        ▼
   truncate DWB     ──────────► [empty]
   fsync DWB        ──────────► ✓
```

**Crash scenarios**:
- Crash before DWB fsync (step 4): DWB is incomplete. Recovery sees partial DWB, verifies each entry's checksum, skips bad ones. data.db was never modified (no torn pages).
- Crash during scatter-write (step 5): Some data.db pages may be torn. DWB has all intact copies. Recovery restores torn pages.
- Crash after data.db sync (step 6): Everything is fine. DWB may or may not be truncated. Recovery finds all data.db pages valid, does nothing.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error on DWB write | Disk full | Propagate — checkpoint fails, retry later |
| I/O error on DWB fsync | Hardware failure | Propagate — CRITICAL |
| Corrupt DWB header | Crash during header write | Recovery treats as empty DWB (clean state) |
| Corrupt DWB entry | Crash during entry write | Recovery skips that entry (checksum fail) |
| I/O error on scatter-write | Single page write fails | Propagate — will be recovered on restart |

## Tests

1. **write_pages + verify data.db**: Write 10 dirty pages via DWB. Verify all pages in page_storage match.
2. **DWB file format**: Write pages, read DWB file manually, verify header and entries.
3. **is_empty after clean checkpoint**: After write_pages completes, DWB should be empty.
4. **recover with no DWB**: Call recover when DWB doesn't exist → returns 0.
5. **recover with empty DWB**: Create empty file, recover → returns 0.
6. **Simulated torn write recovery**: Write DWB but don't scatter-write (simulate crash between steps 4-5). Corrupt a page in page_storage. Call recover. Verify page restored from DWB.
7. **Partial DWB recovery**: Write DWB header + 3 entries, truncate file mid-entry (simulate crash during DWB write). Recover should handle gracefully (skip incomplete entry).
8. **All pages valid**: Write DWB, scatter-write all pages successfully. Don't truncate DWB. Recover → 0 pages restored (all checksums valid).
9. **Large batch**: Write 1000 pages through DWB. Verify correctness.
10. **Page size mismatch**: DWB with different page_size than current → recover returns error.
