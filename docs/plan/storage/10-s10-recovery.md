# S10: Recovery

## Purpose

Restore the storage engine to a consistent state after a crash. First restores any torn pages via the DWB, then replays WAL records from the last checkpoint. Higher layers provide a callback to interpret domain-specific WAL records.

## Dependencies

- **S1 (Backend)**: PageStorage, WalStorage
- **S5 (WAL)**: WalReader, WalRecord
- **S8 (DWB)**: DoubleWriteBuffer.recover()

## Rust Types

```rust
use crate::storage::wal::{WalRecord, Lsn};

/// Callback trait for higher layers to handle WAL records during replay.
/// The storage engine calls this for each record during recovery.
/// Layer 3+ implements this to rebuild document/index state.
pub trait WalRecordHandler {
    /// Handle a single WAL record during replay.
    /// Called in LSN order, only for records after checkpoint_lsn.
    fn handle_record(&mut self, record: &WalRecord) -> Result<()>;
}

/// No-op handler for when no higher-layer replay is needed
/// (e.g., storage engine self-test).
pub struct NoOpHandler;
impl WalRecordHandler for NoOpHandler {
    fn handle_record(&mut self, _record: &WalRecord) -> Result<()> { Ok(()) }
}

/// Recovery coordinator.
pub struct Recovery;

impl Recovery {
    /// Run full crash recovery on a database directory.
    ///
    /// Steps:
    /// 1. Read meta.json → checkpoint_lsn
    /// 2. DWB recovery (restore torn pages)
    /// 3. Open WAL, replay from checkpoint_lsn
    /// 4. Call handler for each replayed record
    ///
    /// Returns the LSN after the last valid record (new write position).
    pub fn run(
        page_storage: &dyn PageStorage,
        wal_storage: &dyn WalStorage,
        dwb_path: Option<&Path>,   // None for in-memory
        checkpoint_lsn: Lsn,
        page_size: usize,
        handler: &mut dyn WalRecordHandler,
    ) -> Result<Lsn>;

    /// Check if recovery is needed (DWB non-empty or WAL has records past checkpoint).
    pub fn needs_recovery(
        dwb_path: Option<&Path>,
        wal_storage: &dyn WalStorage,
        checkpoint_lsn: Lsn,
    ) -> Result<bool>;
}
```

## Implementation Details

### Recovery Flow Diagram

```
Recovery::run()
     │
     ▼
┌─────────────────────────────┐
│ 1. Read checkpoint_lsn      │
│    (passed as parameter)     │
└─────────────┬───────────────┘
              │
              ▼
     ┌────────┴────────┐
     │ dwb_path.is_    │
     │ some()?         │
     ├── YES ──────────┤───── NO ──────┐
     │                 │               │
     ▼                 │               │
┌──────────────┐       │               │
│ 2. DWB       │       │               │
│ .recover()   │       │               │
│              │       │               │
│ For each DWB │       │               │
│ entry:       │       │               │
│  read data.db│       │               │
│  page        │       │               │
│  if checksum │       │               │
│  invalid →   │       │               │
│  restore from│       │               │
│  DWB         │       │               │
│              │       │               │
│ sync data.db │       │               │
│ truncate DWB │       │               │
└──────┬───────┘       │               │
       │               │               │
       ▼               ▼               ▼
┌──────────────────────────────────────────┐
│ 3. Open WAL reader                        │
│    Create iterator from checkpoint_lsn    │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│ 4. WAL Replay Loop                        │
│                                           │
│    for record in wal_reader.read_from(    │
│        checkpoint_lsn) {                  │
│                                           │
│      // Storage engine handles known      │
│      // record types internally:          │
│      match record.record_type {           │
│        CHECKPOINT => skip (informational) │
│        _ => handler.handle_record(&rec)   │
│      }                                    │
│    }                                      │
│                                           │
│    (Iterator stops at first corrupt       │
│     record or end of log)                 │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│ 5. Return end_lsn                         │
│    (LSN after last valid record =         │
│     new WAL write position)               │
└──────────────────────────────────────────┘
```

### WAL Replay Details

The storage engine itself does NOT interpret most WAL records — it passes them to the `WalRecordHandler`. However, it recognizes:

- **Checkpoint (0x02)**: Skip during replay (informational marker).
- All other record types: passed to `handler.handle_record()`.

The handler (provided by Layer 6 Database) interprets domain-specific records:
- `TxCommit (0x01)`: redo mutations to primary B-tree + secondary indexes
- `CreateCollection (0x03)`: update catalog B-tree
- `DropCollection (0x04)`: update catalog B-tree
- `CreateIndex (0x05)`: update catalog B-tree
- `DropIndex (0x06)`: update catalog B-tree
- `IndexReady (0x07)`: transition index state
- `Vacuum (0x08)`: remove entries (idempotent)

### In-Memory Backend

When `dwb_path` is None (in-memory):
- Skip DWB recovery (step 2).
- WAL replay still happens if the WAL storage has records (e.g., MemoryWalStorage that survived).
- In practice, in-memory databases don't crash-recover. `needs_recovery()` returns false.

### Idempotency

WAL replay MUST be idempotent. A record may be replayed even if it was already partially or fully applied before the crash. The handler must handle this:
- Insert into B-tree: if key already exists, overwrite (same value = no-op).
- Delete from B-tree: if key not found, no-op.
- Catalog updates: if entry already exists, overwrite.

### Page LSN Check (Optional Optimization)

During replay, before applying a mutation to a page, check if the page's LSN >= the WAL record's LSN. If so, the mutation was already applied (flushed during checkpoint). Skip it. This is an optimization — without it, replay is still correct because operations are idempotent.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| DWB recovery failure | Corrupt DWB file | Log warning, proceed (data.db may still be valid) |
| WAL read error | Corrupt segment file | Stop replay at corruption point |
| CRC mismatch in WAL | Partial write from crash | Stop replay (records after this point are lost) |
| Handler error | Higher layer can't process record | Propagate — recovery fails |

## Tests

1. **Clean recovery**: Checkpoint, then recover → no records replayed, end_lsn == checkpoint_lsn.
2. **WAL replay**: Write 5 WAL records after checkpoint. Recover with a counting handler. Verify 5 records delivered.
3. **DWB + WAL recovery**: Write DWB with torn pages + WAL records. Recover. Verify pages restored AND WAL replayed.
4. **Corrupt WAL record**: Write 3 valid + 1 corrupt WAL record. Recover. Verify only 3 records replayed.
5. **Empty WAL**: Recover with no WAL records after checkpoint → returns checkpoint_lsn.
6. **needs_recovery false**: Clean state → returns false.
7. **needs_recovery true (DWB)**: Non-empty DWB → returns true.
8. **needs_recovery true (WAL)**: WAL has records past checkpoint → returns true.
9. **Idempotent replay**: Replay same records twice. Handler should produce same result.
10. **In-memory recovery**: MemoryPageStorage, no DWB. Verify recovery is a no-op.
11. **Handler error propagation**: Handler returns error on record 3. Recovery stops and propagates error.
