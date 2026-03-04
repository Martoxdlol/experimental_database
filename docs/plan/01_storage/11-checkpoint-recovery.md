# 11 — Checkpoint, DWB, and Crash Recovery

Implements DESIGN.md §2.9, §2.9.1, §2.10.

## Files

```
src/storage/checkpoint.rs   — Checkpoint + DWB
src/storage/recovery.rs     — DWB repair + WAL replay
```

## Double-Write Buffer (§2.9.1)

### `checkpoint.rs` — DWB section

```rust
/// DWB header: 16 bytes.
#[derive(Debug, Clone)]
pub struct DwbHeader {
    pub magic: u32,        // 0x44574200
    pub version: u16,      // 1
    pub page_size: u16,
    pub page_count: u32,
    pub checksum: u32,     // CRC-32C of header fields
}

/// Write dirty pages to the double-write buffer file.
pub fn write_dwb(
    dwb_path: &Path,
    page_size: u32,
    pages: &[(PageId, &[u8])],  // (page_id, page_data)
) -> Result<()> {
    // 1. Write DWB header.
    // 2. Write entries: [page_id: u32 LE][page_data: page_size bytes] × page_count.
    // 3. fsync the DWB file.
}

/// Read the DWB file for recovery.
pub fn read_dwb(
    dwb_path: &Path,
    page_size: u32,
) -> Result<Vec<(PageId, Vec<u8>)>> {
    // 1. Read and verify DWB header (magic, version, page_size, checksum).
    // 2. Read all entries.
    // 3. Return list of (page_id, page_data).
}

/// Truncate the DWB file to zero length. fsync.
pub fn clear_dwb(dwb_path: &Path) -> Result<()>;
```

## Checkpoint (§2.9)

```rust
/// Checkpoint manager. Runs as a background task.
pub struct CheckpointManager {
    db_dir: PathBuf,
    page_size: u32,
}

impl CheckpointManager {
    /// Execute a fuzzy checkpoint.
    ///
    /// `writer_lock` is held during steps 1-2 only (brief, no I/O).
    /// Steps 3-9 run concurrently with new commits.
    pub async fn run_checkpoint(
        &self,
        pool: &BufferPool,
        wal_sender: &mpsc::Sender<WriteRequest>,
        file_header: &mut FileHeader,
        current_lsn: Lsn,
    ) -> Result<()> {
        // --- Steps 1-2: snapshot under writer lock ---
        // (Caller must hold the writer lock before calling this.)
        //
        // 1. checkpoint_lsn = current_lsn
        // 2. dirty_snapshot = pool.snapshot_dirty_frames()
        //    → Vec<(PageId, page_data_copy, lsn)>
        //    Copies page bytes (no disk I/O).
        //
        // --- Release writer lock ---

        // 3. Write dirty_snapshot to DWB.
        //    write_dwb(dwb_path, page_size, &pages)
        //    This does: sequential write + fsync to data.dwb

        // 4. Scatter-write: for each page in dirty_snapshot,
        //    write page_data to its position in data.db via pwrite.
        //    After each page: pool.mark_clean_if_lsn(page_id, snapshot_lsn)

        // 5. fsync data.db

        // 6. clear_dwb (truncate + fsync)

        // 7. Write Checkpoint WAL record (via wal_sender)
        //    CheckpointRecord { checkpoint_lsn }

        // 8. Update file_header: page_count, free_list_head, checkpoint_lsn.
        //    Write page 0 + shadow. fsync data.db.

        // 9. Reclaim old WAL segments.
        //    Delete segments where max_lsn < reclaim_lsn.
    }
}
```

### Checkpoint Trigger Conditions

```rust
pub struct CheckpointConfig {
    /// Maximum WAL size before triggering checkpoint (bytes).
    pub wal_size_threshold: u64,        // default: 64 MB
    /// Maximum time between checkpoints.
    pub interval: Duration,              // default: 5 minutes
}
```

The checkpoint background task runs on a timer or is triggered by:
1. WAL size exceeding `wal_size_threshold`.
2. Timer exceeding `interval`.
3. Graceful shutdown.
4. Buffer pool full (early checkpoint to free dirty frames).

### Checkpoint Concurrency (§2.9, §5.10)

```
Steps 1-2: writer lock held (brief)
  - Record checkpoint_lsn.
  - Copy dirty frame data (in-memory only, no I/O).
  - Release writer lock.

Steps 3-5: NO writer lock
  - DWB write + data.db scatter-write run concurrently with new commits.
  - New commits can dirty frames that were already snapshotted.
  - Scatter-writes use pwrite (no file mutex contention with readers).

Step 4 (mark clean): acquire frame RwLock exclusively
  - Only marks clean if frame LSN == snapshot LSN.
  - If writer modified the frame since snapshot (LSN advanced) → skip.

Steps 6-9: NO writer lock
  - DWB truncate, WAL record, file header update, segment reclaim.
```

## Crash Recovery (§2.10)

### `recovery.rs`

```rust
/// Full crash recovery sequence. Called on database startup.
pub fn recover_database(
    db_dir: &Path,
    pool: &BufferPool,
    catalog: &mut CatalogManager,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
) -> Result<RecoveryResult> {
    // 1. Read meta.json → checkpoint_lsn.
    let checkpoint_lsn = read_meta_json(db_dir)?;

    // 2. DWB recovery: repair torn pages in data.db.
    recover_dwb(db_dir, pool)?;

    // 3. Open WAL, locate segment containing checkpoint_lsn.
    // 4. Replay from checkpoint_lsn forward.
    let replay_result = replay_wal(db_dir, pool, catalog, free_list, heap, checkpoint_lsn)?;

    Ok(replay_result)
}

/// DWB recovery: restore torn pages.
fn recover_dwb(db_dir: &Path, pool: &BufferPool) -> Result<()> {
    let dwb_path = db_dir.join("data.dwb");

    // 1. If DWB doesn't exist or is empty → clean, nothing to do.
    if !dwb_path.exists() || std::fs::metadata(&dwb_path)?.len() == 0 {
        return Ok(());
    }

    // 2. Read DWB entries.
    let entries = read_dwb(&dwb_path, pool.page_size())?;

    // 3. For each entry:
    for (page_id, dwb_data) in &entries {
        //    a. Read corresponding page from data.db.
        //    b. Verify data.db page's checksum.
        //    c. If checksum invalid (torn write): overwrite with DWB copy.
        //    d. If checksum valid: skip (write completed before crash).
    }

    // 4. fsync data.db.
    // 5. Truncate DWB to zero. fsync.
    clear_dwb(&dwb_path)?;

    Ok(())
}

/// Replay WAL records from checkpoint_lsn.
fn replay_wal(
    db_dir: &Path,
    pool: &BufferPool,
    catalog: &mut CatalogManager,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
    checkpoint_lsn: Lsn,
) -> Result<RecoveryResult> {
    let wal_dir = db_dir.join("wal");
    let mut reader = WalReader::open(&wal_dir, checkpoint_lsn)?;
    let mut records_replayed = 0u64;

    while let Some(record) = reader.next()? {
        match record.payload {
            WalPayload::TxCommit(tx) => {
                // Apply mutations to primary B-tree.
                // Apply index deltas to secondary index B-trees.
                replay_tx_commit(pool, catalog, free_list, heap, &tx, record.lsn)?;
            }
            WalPayload::CreateCollection(cc) => {
                // Insert into catalog B-tree + cache.
                replay_create_collection(pool, catalog, free_list, &cc, record.lsn)?;
            }
            WalPayload::DropCollection(dc) => {
                replay_drop_collection(pool, catalog, free_list, &dc, record.lsn)?;
            }
            WalPayload::CreateIndex(ci) => {
                replay_create_index(pool, catalog, free_list, &ci, record.lsn)?;
            }
            WalPayload::DropIndex(di) => {
                replay_drop_index(pool, catalog, free_list, &di, record.lsn)?;
            }
            WalPayload::IndexReady(ir) => {
                // Transition index from Building → Ready.
                replay_index_ready(pool, catalog, &ir, record.lsn)?;
            }
            WalPayload::Vacuum(v) => {
                // Remove listed versions + index keys (idempotent).
                replay_vacuum(pool, catalog, free_list, heap, &v, record.lsn)?;
            }
            WalPayload::Checkpoint(_) => {
                // No-op during replay.
            }
            _ => {
                // System-level records (CreateDatabase etc.) not replayed here.
            }
        }
        records_replayed += 1;
    }

    Ok(RecoveryResult {
        records_replayed,
        recovered_lsn: reader.current_lsn(),
    })
}

/// Result of crash recovery.
pub struct RecoveryResult {
    pub records_replayed: u64,
    pub recovered_lsn: Lsn,
}
```

### Replay TxCommit

```rust
fn replay_tx_commit(
    pool: &BufferPool,
    catalog: &CatalogManager,
    free_list: &mut FreePageList,
    heap: &mut ExternalHeap,
    tx: &TxCommitRecord,
    lsn: Lsn,
) -> Result<()> {
    // For each mutation:
    for mutation in &tx.mutations {
        let col = catalog.cache().get_collection(mutation.collection_id)
            .ok_or_else(|| /* unknown collection error */)?;

        let primary_key = encode_primary_key(mutation.doc_id, tx.commit_ts);

        match mutation.op_type {
            OpType::Insert | OpType::Replace => {
                // Build leaf cell (inline or external).
                let cell = build_primary_leaf_cell(
                    mutation.doc_id, tx.commit_ts, &mutation.body,
                    pool, free_list, heap, lsn,
                )?;
                // Insert into primary B-tree.
                let new_root = btree_insert(
                    pool, col.primary_root_page, &cell,
                    KeySizeHint::Fixed(24), free_list, lsn,
                )?;
                // Update root if it changed (split).
                if new_root != col.primary_root_page {
                    catalog.update_collection_root(col.collection_id, new_root);
                }
            }
            OpType::Delete => {
                // Build tombstone cell.
                let cell = build_tombstone_cell(mutation.doc_id, tx.commit_ts);
                let new_root = btree_insert(
                    pool, col.primary_root_page, &cell,
                    KeySizeHint::Fixed(24), free_list, lsn,
                )?;
                if new_root != col.primary_root_page {
                    catalog.update_collection_root(col.collection_id, new_root);
                }
            }
        }
    }

    // For each index delta:
    for delta in &tx.index_deltas {
        let idx = catalog.cache().get_index(delta.index_id)
            .ok_or_else(|| /* unknown index error */)?;

        if let Some(old_key) = &delta.old_key {
            // Delete old entry from secondary index B-tree.
            btree_delete(pool, idx.root_page, old_key.as_ref(),
                KeySizeHint::VariableWithSuffix, free_list, lsn)?;
        }
        if let Some(new_key) = &delta.new_key {
            // Insert new entry (key only, no value body).
            let cell = new_key.0.clone(); // secondary index cell IS the key
            btree_insert(pool, idx.root_page, &cell,
                KeySizeHint::VariableWithSuffix, free_list, lsn)?;
        }
    }

    Ok(())
}
```

### Recovery Invariants

1. **No undo needed**: only committed transactions are in the WAL (no-steal policy). Only redo is required.
2. **Page LSN check**: during replay, if a page's LSN ≥ record's LSN, the page already has this change (from a pre-crash flush). Skip to avoid double-applying.
3. **Idempotent**: replay can be repeated safely. B-tree inserts with duplicate keys are no-ops. Vacuum deletes of absent keys are no-ops.
4. **DWB before WAL**: DWB recovery always runs first, ensuring data.db pages are consistent before WAL replay modifies them.

### meta.json

```rust
/// Database metadata persisted alongside data.db.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseMeta {
    pub checkpoint_lsn: u64,
    pub page_size: u32,
    pub created_at: u64,
}

/// Read meta.json.
pub fn read_meta_json(db_dir: &Path) -> Result<DatabaseMeta>;

/// Write meta.json atomically (write temp + fsync + rename).
pub fn write_meta_json(db_dir: &Path, meta: &DatabaseMeta) -> Result<()>;
```
