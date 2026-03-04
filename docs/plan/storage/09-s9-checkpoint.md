# S9: Checkpoint

## Purpose

Flush dirty buffer pool pages to the data file through the DWB, write a checkpoint WAL record, and update metadata. Allows old WAL segments to be reclaimed.

## Dependencies

- **S3 (Buffer Pool)**: dirty_pages() snapshot, mark_clean()
- **S5 (WAL)**: WalWriter to write checkpoint record
- **S8 (DWB)**: write_pages() for torn-write protection

## Rust Types

```rust
use crate::storage::buffer_pool::BufferPool;
use crate::storage::dwb::DoubleWriteBuffer;
use crate::storage::wal::{WalWriter, Lsn};
use std::sync::Arc;

/// Checkpoint WAL record type code.
pub const WAL_RECORD_CHECKPOINT: u8 = 0x02;

/// Checkpoint coordinator.
pub struct Checkpoint {
    buffer_pool: Arc<BufferPool>,
    dwb: Option<DoubleWriteBuffer>,  // None for in-memory backends
    wal_writer: Arc<WalWriter>,
    is_durable: bool,
}

impl Checkpoint {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        dwb: Option<DoubleWriteBuffer>,
        wal_writer: Arc<WalWriter>,
        is_durable: bool,
    ) -> Self;

    /// Run a full checkpoint. Returns the checkpoint LSN.
    /// For in-memory backends: writes WAL record only (DWB + scatter-write skipped).
    pub async fn run(&self) -> Result<Lsn>;
}
```

## Implementation Details

### Checkpoint Protocol

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Record checkpoint_lsn = wal_writer.current_lsn()   │
│         (This is the LSN up to which we'll flush)           │
│         Note: current_lsn() returns the position of the    │
│         NEXT record to be written, not the last written     │
│         record. This means recovery will replay from a      │
│         position that includes all records written before   │
│         the checkpoint started. Any concurrent WAL writes   │
│         between this point and the checkpoint completion    │
│         will also be replayed on recovery — this is safe    │
│         because WAL replay is idempotent.                   │
├─────────────────────────────────────────────────────────────┤
│ Step 2: Snapshot dirty pages                                │
│         dirty = buffer_pool.dirty_pages()                   │
│         Each entry: (page_id, page_data_copy, lsn)          │
│         NOTE: This copies page data — brief but no I/O      │
├─────────────────────────────────────────────────────────────┤
│ Step 3: If is_durable AND dirty is non-empty:               │
│         dwb.write_pages(&dirty)                             │
│         This does:                                          │
│           a. Write all pages to DWB file sequentially       │
│           b. fsync DWB                                      │
│           c. Scatter-write each page to data.db             │
│           d. fsync data.db                                  │
│           e. Truncate DWB                                   │
│           f. fsync DWB                                      │
├─────────────────────────────────────────────────────────────┤
│ Step 4: Mark flushed pages as clean                         │
│         For each (page_id, _, lsn) in dirty:                │
│           buffer_pool.mark_clean(page_id, lsn)              │
│         (Only clears dirty if LSN hasn't changed)           │
├─────────────────────────────────────────────────────────────┤
│ Step 5: Write Checkpoint WAL record                         │
│         payload = checkpoint_lsn as u64 LE (8 bytes)        │
│         wal_writer.append(0x02, &payload)                   │
├─────────────────────────────────────────────────────────────┤
│ Step 6: Return checkpoint_lsn                               │
└─────────────────────────────────────────────────────────────┘
```

> **Post-Checkpoint Steps (caller responsibility)**
>
> `Checkpoint::run()` returns `Result<Lsn>`. The caller (StorageEngine, S13) is responsible for:
> 1. Updating `FileHeader.checkpoint_lsn` in the data file header page.
> 2. Updating `meta.json` with the new checkpoint LSN.
>
> This keeps S9 free of any dependency on S13 and avoids a circular dependency.

### Checkpoint Flow Diagram

```
         Checkpoint::run()
              │
              ▼
    ┌─────────────────────┐
    │ checkpoint_lsn =    │
    │ current WAL position│
    └─────────┬───────────┘
              │
              ▼
    ┌─────────────────────┐
    │ dirty_pages =       │
    │ buffer_pool         │
    │  .dirty_pages()     │  ← copies page data (no I/O)
    └─────────┬───────────┘
              │
              ▼
     ┌────────┴────────┐
     │ is_durable?     │
     ├── YES ──────────┤───── NO ──────┐
     │                 │               │
     ▼                 │               ▼
┌──────────────┐       │      ┌──────────────┐
│ DWB.write_   │       │      │ (skip DWB)   │
│ pages(dirty) │       │      └──────┬───────┘
│              │       │             │
│ sequential   │       │             │
│ write + fsync│       │             │
│ scatter-write│       │             │
│ + fsync      │       │             │
│ truncate DWB │       │             │
└──────┬───────┘       │             │
       │               │             │
       ▼               │             ▼
┌──────────────┐       │    ┌──────────────┐
│ mark_clean   │       │    │ mark_clean   │
│ each page    │       │    │ each page    │
└──────┬───────┘       │    └──────┬───────┘
       │               │           │
       ▼               │           ▼
┌──────────────────────┴───────────────────┐
│ WAL.append(Checkpoint, checkpoint_lsn)   │
└──────────────────────┬───────────────────┘
                       │
                       ▼
                 return checkpoint_lsn
```

### Concurrency with Writer

Per DESIGN.md Section 2.9:
- Steps 1-2: Checkpoint may need brief coordination with the writer to get a consistent snapshot. In practice, `dirty_pages()` iterates frames and copies data — this is safe because each frame has its own RwLock.
- Steps 3-6: No writer lock needed. The writer can proceed concurrently.
- Step 4: mark_clean uses LSN comparison — if the writer modified a page after the snapshot, it stays dirty.

### In-Memory Backend Behavior

When `is_durable == false`:
- Skip DWB entirely (no torn writes possible).
- Skip scatter-write (pages stay in memory only).
- Still write the Checkpoint WAL record (for LSN tracking consistency).
- Still mark pages clean (reduces dirty count for buffer pool pressure).

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| DWB write failure | Disk full, I/O error | Propagate — checkpoint fails, retry later |
| WAL append failure | WAL backend failure | Propagate — critical |
| No dirty pages | Nothing to flush | Still write WAL record (valid checkpoint) |

## Tests

1. **Clean checkpoint**: Modify pages, run checkpoint, verify DWB is empty after, dirty flags cleared.
2. **Checkpoint WAL record**: Run checkpoint, read WAL, verify Checkpoint record present with correct LSN.
3. **Checkpoint with no dirty pages**: Run checkpoint on clean buffer pool. Should succeed (WAL record only).
4. **In-memory checkpoint**: Use MemoryPageStorage. Run checkpoint. Verify no DWB operations, WAL record written.
5. **mark_clean with concurrent modification**: Modify page, snapshot, modify again (new LSN), checkpoint. Page should remain dirty after checkpoint.
6. **Multiple checkpoints**: Run three checkpoints in sequence. Each should flush only pages dirtied since last checkpoint.
7. **Checkpoint LSN advances**: Each checkpoint returns a higher LSN than the previous.
