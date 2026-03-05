//! Checkpoint coordinator.
//!
//! Flushes dirty buffer pool pages to the data file through the DWB, writes a
//! checkpoint WAL record, and updates metadata. Allows old WAL segments to be
//! reclaimed.

use crate::buffer_pool::BufferPool;
use crate::dwb::DoubleWriteBuffer;
use crate::wal::{Lsn, WalWriter, WAL_RECORD_CHECKPOINT};
use std::io;
use std::sync::Arc;

// ─── Checkpoint ───

/// Checkpoint coordinator.
///
/// Orchestrates flushing dirty pages from the buffer pool to durable storage
/// through the double-write buffer, then writes a checkpoint WAL record.
pub struct Checkpoint {
    buffer_pool: Arc<BufferPool>,
    dwb: Option<DoubleWriteBuffer>,
    wal_writer: Arc<WalWriter>,
    is_durable: bool,
}

impl Checkpoint {
    /// Create a new checkpoint coordinator.
    ///
    /// - `buffer_pool`: the buffer pool to flush dirty pages from.
    /// - `dwb`: the double-write buffer (None for in-memory backends).
    /// - `wal_writer`: the WAL writer for appending checkpoint records.
    /// - `is_durable`: whether this is a durable (file-backed) database.
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        dwb: Option<DoubleWriteBuffer>,
        wal_writer: Arc<WalWriter>,
        is_durable: bool,
    ) -> Self {
        Self {
            buffer_pool,
            dwb,
            wal_writer,
            is_durable,
        }
    }

    /// Run a full checkpoint. Returns the checkpoint LSN.
    ///
    /// For in-memory backends: writes WAL record only (DWB + scatter-write skipped).
    ///
    /// Steps:
    /// 1. Record checkpoint_lsn = wal_writer.current_lsn()
    /// 2. Snapshot dirty pages from buffer pool
    /// 3. If durable and non-empty: write pages through DWB
    /// 4. Mark flushed pages as clean
    /// 5. Write checkpoint WAL record
    /// 6. Return checkpoint_lsn
    pub async fn run(&self) -> io::Result<Lsn> {
        // Step 1: Record checkpoint LSN.
        let checkpoint_lsn = self.wal_writer.current_lsn();

        // Step 2: Snapshot dirty pages.
        let dirty = self.buffer_pool.dirty_pages();

        // Step 3: If durable and non-empty, write through DWB.
        if self.is_durable && !dirty.is_empty()
            && let Some(ref dwb) = self.dwb {
                // Strip the LSN from dirty_pages results; DWB takes &[(PageId, Vec<u8>)].
                let pages_for_dwb: Vec<_> = dirty
                    .iter()
                    .map(|(page_id, data, _lsn)| (*page_id, data.clone()))
                    .collect();
                dwb.write_pages(&pages_for_dwb)?;
            }

        // Step 4: Mark flushed pages as clean.
        for (page_id, _data, lsn) in &dirty {
            self.buffer_pool.mark_clean(*page_id, *lsn);
        }

        // Step 5: Write checkpoint WAL record.
        let payload = checkpoint_lsn.to_le_bytes();
        self.wal_writer
            .append(WAL_RECORD_CHECKPOINT, &payload)
            .await?;

        // Step 6: Return checkpoint LSN.
        Ok(checkpoint_lsn)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, MemoryWalStorage, PageStorage, WalStorage};
    use crate::buffer_pool::{BufferPool, BufferPoolConfig};
    use crate::page::{PageType, SlottedPage};
    use crate::wal::{WalConfig, WalReader, WalRecord, WAL_RECORD_CHECKPOINT, WAL_RECORD_TX_COMMIT};

    const PAGE_SIZE: usize = 4096;

    /// Helper: create a MemoryPageStorage with `n` initialized pages.
    fn make_page_storage(n: u64) -> Arc<MemoryPageStorage> {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(n).unwrap();
        for i in 0..n {
            let mut buf = vec![0u8; PAGE_SIZE];
            SlottedPage::init(&mut buf, i as u32, PageType::Heap);
            storage.write_page(i as u32, &buf).unwrap();
        }
        storage
    }

    /// Helper: create a BufferPool with the given storage.
    fn make_pool(frame_count: usize, storage: Arc<dyn PageStorage>) -> Arc<BufferPool> {
        Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count,
            },
            storage,
        ))
    }

    /// Helper: create a WalWriter + MemoryWalStorage.
    fn make_wal() -> (Arc<WalWriter>, Arc<MemoryWalStorage>) {
        let wal_storage = Arc::new(MemoryWalStorage::new());
        let writer = Arc::new(
            WalWriter::new(wal_storage.clone() as Arc<dyn WalStorage>, WalConfig::default())
                .unwrap(),
        );
        (writer, wal_storage)
    }

    /// Byte offset of the `lsn` field within the page header.
    const LSN_OFFSET: usize = 24;
    const LSN_SIZE: usize = 8;

    /// Helper: write an LSN into a page buffer's header.
    fn write_lsn_to_page(pool: &BufferPool, page_id: u32, lsn: u64) {
        let mut guard = pool.fetch_page_exclusive(page_id).unwrap();
        let buf = guard.data_mut();
        buf[LSN_OFFSET..LSN_OFFSET + LSN_SIZE].copy_from_slice(&lsn.to_le_bytes());
    }

    // ─── Test 1: Clean checkpoint ───
    // Modify pages, run checkpoint, verify dirty flags cleared.

    #[tokio::test]
    async fn clean_checkpoint() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, _wal_storage) = make_wal();

        // Dirty some pages.
        write_lsn_to_page(&pool, 0, 10);
        write_lsn_to_page(&pool, 1, 11);

        assert_eq!(pool.dirty_pages().len(), 2);

        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);
        let _lsn = checkpoint.run().await.unwrap();

        // Dirty flags should be cleared.
        assert_eq!(pool.dirty_pages().len(), 0, "dirty pages should be cleared after checkpoint");
    }

    // ─── Test 2: Checkpoint WAL record ───
    // Run checkpoint, read WAL, verify checkpoint record present with correct LSN.

    #[tokio::test]
    async fn checkpoint_wal_record() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, wal_storage) = make_wal();

        // Write a regular WAL record first.
        wal_writer
            .append(WAL_RECORD_TX_COMMIT, b"some-tx")
            .await
            .unwrap();

        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);
        let checkpoint_lsn = checkpoint.run().await.unwrap();

        // Read WAL and find the checkpoint record.
        let reader = WalReader::new(wal_storage.clone() as Arc<dyn WalStorage>);
        let records: Vec<WalRecord> = reader
            .read_from(0)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Should have at least 2 records: the TX_COMMIT and the CHECKPOINT.
        assert!(records.len() >= 2, "expected at least 2 records, got {}", records.len());

        // Find the checkpoint record.
        let checkpoint_record = records
            .iter()
            .find(|r| r.record_type == WAL_RECORD_CHECKPOINT)
            .expect("should find a checkpoint record");

        // Verify the payload is the checkpoint LSN.
        let stored_lsn = u64::from_le_bytes(
            checkpoint_record.payload[0..8].try_into().unwrap(),
        );
        assert_eq!(stored_lsn, checkpoint_lsn);
    }

    // ─── Test 3: Checkpoint with no dirty pages ───
    // Run checkpoint on clean buffer pool. Should succeed (WAL record only).

    #[tokio::test]
    async fn checkpoint_no_dirty_pages() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, wal_storage) = make_wal();

        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);
        let _lsn = checkpoint.run().await.unwrap();

        // Verify WAL record was still written.
        let reader = WalReader::new(wal_storage.clone() as Arc<dyn WalStorage>);
        let records: Vec<WalRecord> = reader
            .read_from(0)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, WAL_RECORD_CHECKPOINT);
    }

    // ─── Test 4: In-memory checkpoint ───
    // Use MemoryPageStorage. Run checkpoint. Verify no DWB operations, WAL record written.

    #[tokio::test]
    async fn in_memory_checkpoint() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, wal_storage) = make_wal();

        // Dirty some pages.
        write_lsn_to_page(&pool, 0, 10);
        write_lsn_to_page(&pool, 1, 11);

        // In-memory: is_durable = false, no DWB.
        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);
        let _lsn = checkpoint.run().await.unwrap();

        // Dirty flags should be cleared.
        assert_eq!(pool.dirty_pages().len(), 0);

        // WAL record should be present.
        let reader = WalReader::new(wal_storage.clone() as Arc<dyn WalStorage>);
        let records: Vec<WalRecord> = reader
            .read_from(0)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, WAL_RECORD_CHECKPOINT);
    }

    // ─── Test 5: mark_clean with concurrent modification ───
    // Modify page, snapshot, modify again (new LSN), checkpoint.
    // Page should remain dirty after checkpoint.

    #[tokio::test]
    async fn mark_clean_concurrent_modification() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (_wal_writer, _wal_storage) = make_wal();

        // First modification: LSN = 10.
        write_lsn_to_page(&pool, 0, 10);

        // Create checkpoint but manually simulate the concurrent modification.
        // We can't easily interleave with the checkpoint, so we test mark_clean directly.
        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 1);
        let (_, _, original_lsn) = &dirty[0];
        assert_eq!(*original_lsn, 10);

        // Second modification: LSN = 20 (page modified since snapshot).
        write_lsn_to_page(&pool, 0, 20);

        // Mark clean with the OLD LSN. Should NOT clear dirty.
        pool.mark_clean(0, 10);

        let dirty_after = pool.dirty_pages();
        assert_eq!(dirty_after.len(), 1, "page should remain dirty (stale LSN)");
        assert_eq!(dirty_after[0].2, 20);
    }

    // ─── Test 6: Multiple checkpoints ───
    // Run three checkpoints in sequence. Each should flush only pages dirtied since last checkpoint.

    #[tokio::test]
    async fn multiple_checkpoints() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, wal_storage) = make_wal();

        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);

        // Checkpoint 1: dirty pages 0, 1.
        write_lsn_to_page(&pool, 0, 10);
        write_lsn_to_page(&pool, 1, 11);
        assert_eq!(pool.dirty_pages().len(), 2);
        let _lsn1 = checkpoint.run().await.unwrap();
        assert_eq!(pool.dirty_pages().len(), 0);

        // Checkpoint 2: dirty page 2 only.
        write_lsn_to_page(&pool, 2, 12);
        assert_eq!(pool.dirty_pages().len(), 1);
        let _lsn2 = checkpoint.run().await.unwrap();
        assert_eq!(pool.dirty_pages().len(), 0);

        // Checkpoint 3: no dirty pages.
        assert_eq!(pool.dirty_pages().len(), 0);
        let _lsn3 = checkpoint.run().await.unwrap();
        assert_eq!(pool.dirty_pages().len(), 0);

        // Verify WAL has 3 checkpoint records.
        let reader = WalReader::new(wal_storage.clone() as Arc<dyn WalStorage>);
        let records: Vec<WalRecord> = reader
            .read_from(0)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let checkpoint_records: Vec<_> = records
            .iter()
            .filter(|r| r.record_type == WAL_RECORD_CHECKPOINT)
            .collect();
        assert_eq!(checkpoint_records.len(), 3);
    }

    // ─── Test 7: Checkpoint LSN advances ───
    // Each checkpoint returns a higher LSN than the previous.

    #[tokio::test]
    async fn checkpoint_lsn_advances() {
        let page_storage = make_page_storage(4);
        let pool = make_pool(8, page_storage.clone());
        let (wal_writer, _wal_storage) = make_wal();

        let checkpoint = Checkpoint::new(pool.clone(), None, wal_writer.clone(), false);

        let lsn1 = checkpoint.run().await.unwrap();

        // Write something to advance WAL LSN.
        wal_writer
            .append(WAL_RECORD_TX_COMMIT, b"data")
            .await
            .unwrap();

        let lsn2 = checkpoint.run().await.unwrap();
        assert!(lsn2 > lsn1, "checkpoint LSN should advance: {} > {}", lsn2, lsn1);

        // Write more.
        wal_writer
            .append(WAL_RECORD_TX_COMMIT, b"more-data")
            .await
            .unwrap();

        let lsn3 = checkpoint.run().await.unwrap();
        assert!(lsn3 > lsn2, "checkpoint LSN should advance: {} > {}", lsn3, lsn2);
    }
}
