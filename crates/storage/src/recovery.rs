//! Crash recovery coordinator.
//!
//! Restores the storage engine to a consistent state after a crash. First
//! restores any torn pages via the DWB, then replays WAL records from the last
//! checkpoint. Higher layers provide a callback to interpret domain-specific
//! WAL records.

use crate::backend::{PageStorage, WalStorage};
use crate::page::SlottedPageRef;
use crate::wal::{Lsn, WalRecord, WAL_RECORD_CHECKPOINT};
use async_trait::async_trait;
use std::io;
use std::path::Path;

// ─── Recovery Mode & Stats ───

/// Controls how WAL replay handles corrupt records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Stop at the first corrupt record (default, safest).
    Strict,
    /// Skip corrupt records and continue replaying. May recover more data
    /// after a mid-WAL corruption, at the risk of applying records that
    /// depended on the skipped ones.
    BestEffort,
}

/// Statistics from a recovery run.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of WAL records successfully replayed.
    pub records_replayed: u64,
    /// Number of corrupt WAL records skipped (BestEffort mode only).
    pub records_skipped_corrupt: u64,
    /// Number of pages restored from the DWB.
    pub dwb_pages_restored: u32,
    /// Number of DWB pages skipped due to corruption.
    pub dwb_pages_skipped_corrupt: u32,
}

// ─── Constants ───

/// WAL frame header size: 4 (payload_len) + 4 (crc32c) + 1 (record_type) = 9 bytes.
const WAL_FRAME_HEADER_SIZE: usize = 9;

/// Maximum WAL record payload size (64 MB).
const MAX_WAL_RECORD_SIZE: usize = 64 * 1024 * 1024;

/// DWB magic number.
const DWB_MAGIC: u32 = 0x44574200;

/// DWB header size.
const DWB_HEADER_SIZE: usize = 16;

/// DWB entry page_id prefix size.
const DWB_ENTRY_PREFIX_SIZE: usize = 4;

// ─── WalRecordHandler ───

/// Callback trait for higher layers to handle WAL records during replay.
///
/// The storage engine calls this for each record during recovery.
/// Layer 3+ implements this to rebuild document/index state.
#[async_trait]
pub trait WalRecordHandler: Send {
    /// Handle a single WAL record during replay.
    /// Called in LSN order, only for records after checkpoint_lsn.
    async fn handle_record(&mut self, record: &WalRecord) -> io::Result<()>;
}

// ─── NoOpHandler ───

/// No-op handler for when no higher-layer replay is needed
/// (e.g., storage engine self-test).
pub struct NoOpHandler;

#[async_trait]
impl WalRecordHandler for NoOpHandler {
    async fn handle_record(&mut self, _record: &WalRecord) -> io::Result<()> {
        Ok(())
    }
}

// ─── Recovery ───

/// Recovery coordinator.
///
/// Provides static methods to run crash recovery and check if recovery is needed.
pub struct Recovery;

impl Recovery {
    /// Run full crash recovery.
    ///
    /// Steps:
    /// 1. Use provided checkpoint_lsn parameter
    /// 2. DWB recovery (restore torn pages) -- if dwb_path is Some
    /// 3. Open WAL, replay from checkpoint_lsn
    /// 4. Call handler for each replayed record (skip CHECKPOINT records)
    ///
    /// Returns the LSN after the last valid record and recovery statistics.
    pub async fn run(
        page_storage: &dyn PageStorage,
        wal_storage: &dyn WalStorage,
        dwb_path: Option<&Path>,
        checkpoint_lsn: Lsn,
        page_size: usize,
        handler: &mut dyn WalRecordHandler,
        mode: RecoveryMode,
    ) -> io::Result<(Lsn, RecoveryStats)> {
        let mut stats = RecoveryStats::default();

        // Step 2: DWB recovery (if applicable).
        if let Some(path) = dwb_path {
            let (restored, skipped) =
                Self::dwb_recover(path, page_storage, page_size).await?;
            stats.dwb_pages_restored = restored;
            stats.dwb_pages_skipped_corrupt = skipped;
        }

        // Step 3: WAL replay from checkpoint_lsn.
        let end_lsn =
            Self::replay_wal(wal_storage, checkpoint_lsn, handler, mode, &mut stats).await?;

        Ok((end_lsn, stats))
    }

    /// Check if recovery is needed (DWB non-empty or WAL has records past checkpoint).
    pub async fn needs_recovery(
        dwb_path: Option<&Path>,
        wal_storage: &dyn WalStorage,
        checkpoint_lsn: Lsn,
    ) -> io::Result<bool> {
        // Check DWB.
        if let Some(path) = dwb_path {
            let path_buf = path.to_path_buf();
            let exists_and_nonempty = tokio::task::spawn_blocking(move || -> io::Result<bool> {
                if path_buf.exists() {
                    let metadata = std::fs::metadata(&path_buf)?;
                    Ok(metadata.len() > 0)
                } else {
                    Ok(false)
                }
            })
            .await
            .map_err(|e| io::Error::other(format!("spawn_blocking join error: {e}")))??;

            if exists_and_nonempty {
                return Ok(true);
            }
        }

        // Check WAL: see if there are any valid records past checkpoint_lsn.
        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        let n = wal_storage.read_from(checkpoint_lsn, &mut header_buf).await?;
        if n >= WAL_FRAME_HEADER_SIZE {
            let payload_len = u32::from_le_bytes(
                [header_buf[0], header_buf[1], header_buf[2], header_buf[3]],
            ) as usize;
            if payload_len > 0 && payload_len <= MAX_WAL_RECORD_SIZE {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Replay WAL records starting from `start_lsn`, calling the handler for
    /// each non-checkpoint record. Returns the LSN after the last valid record.
    async fn replay_wal(
        wal_storage: &dyn WalStorage,
        start_lsn: Lsn,
        handler: &mut dyn WalRecordHandler,
        mode: RecoveryMode,
        stats: &mut RecoveryStats,
    ) -> io::Result<Lsn> {
        let mut end_lsn = start_lsn;
        let mut current_lsn = start_lsn;

        loop {
            // Read frame header.
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
            let n = wal_storage.read_from(current_lsn, &mut header_buf).await?;
            if n < WAL_FRAME_HEADER_SIZE {
                break;
            }

            let payload_len = u32::from_le_bytes(
                [header_buf[0], header_buf[1], header_buf[2], header_buf[3]],
            ) as usize;
            let stored_crc = u32::from_le_bytes(
                [header_buf[4], header_buf[5], header_buf[6], header_buf[7]],
            );
            let record_type = header_buf[8];

            // End-of-log sentinel.
            if payload_len == 0 {
                break;
            }

            // Implausibly large payload.
            if payload_len > MAX_WAL_RECORD_SIZE {
                if mode == RecoveryMode::BestEffort {
                    stats.records_skipped_corrupt += 1;
                    current_lsn = match Self::scan_forward_for_valid_frame(
                        wal_storage, current_lsn + 1,
                    )
                    .await?
                    {
                        Some(next) => next,
                        None => break,
                    };
                    continue;
                }
                break;
            }

            // Read payload.
            let mut payload = vec![0u8; payload_len];
            let n = wal_storage
                .read_from(current_lsn + WAL_FRAME_HEADER_SIZE as u64, &mut payload)
                .await?;
            if n < payload_len {
                // Incomplete payload.
                break;
            }

            // Verify CRC.
            let computed_crc = {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&[record_type]);
                hasher.update(&payload);
                hasher.finalize()
            };
            if computed_crc != stored_crc {
                if mode == RecoveryMode::BestEffort {
                    stats.records_skipped_corrupt += 1;
                    current_lsn = match Self::scan_forward_for_valid_frame(
                        wal_storage, current_lsn + 1,
                    )
                    .await?
                    {
                        Some(next) => next,
                        None => break,
                    };
                    continue;
                }
                // Strict mode: stop replay.
                break;
            }

            let record = WalRecord {
                lsn: current_lsn,
                record_type,
                payload,
            };

            let frame_size = WAL_FRAME_HEADER_SIZE as u64 + payload_len as u64;
            end_lsn = current_lsn + frame_size;
            current_lsn = end_lsn;

            // Skip checkpoint records (informational).
            if record_type == WAL_RECORD_CHECKPOINT {
                continue;
            }

            // Deliver to handler.
            handler.handle_record(&record).await?;
            stats.records_replayed += 1;
        }

        Ok(end_lsn)
    }

    /// Scan forward byte-by-byte from `start` looking for a valid WAL frame.
    /// Returns the LSN of the next valid frame, or None if not found within 1MB.
    async fn scan_forward_for_valid_frame(
        wal_storage: &dyn WalStorage,
        start: Lsn,
    ) -> io::Result<Option<Lsn>> {
        const MAX_SCAN: u64 = 1024 * 1024; // 1 MB

        let mut probe = start;
        let limit = start.saturating_add(MAX_SCAN);

        while probe < limit {
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
            let n = wal_storage.read_from(probe, &mut header_buf).await?;
            if n < WAL_FRAME_HEADER_SIZE {
                return Ok(None);
            }

            let payload_len = u32::from_le_bytes(
                [header_buf[0], header_buf[1], header_buf[2], header_buf[3]],
            ) as usize;
            let record_type = header_buf[8];

            // Quick plausibility check.
            if payload_len == 0 || payload_len > MAX_WAL_RECORD_SIZE {
                probe += 1;
                continue;
            }

            // Try to read the payload and verify CRC.
            let mut payload = vec![0u8; payload_len];
            let n = wal_storage
                .read_from(probe + WAL_FRAME_HEADER_SIZE as u64, &mut payload)
                .await?;
            if n < payload_len {
                // Payload extends past WAL end at this probe — try next byte.
                probe += 1;
                continue;
            }

            let computed_crc = {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&[record_type]);
                hasher.update(&payload);
                hasher.finalize()
            };
            let stored_crc = u32::from_le_bytes(
                [header_buf[4], header_buf[5], header_buf[6], header_buf[7]],
            );

            if computed_crc == stored_crc {
                return Ok(Some(probe));
            }

            probe += 1;
        }

        Ok(None)
    }

    /// DWB recovery: restore torn pages from the DWB file.
    /// Returns (pages_restored, pages_skipped_corrupt).
    async fn dwb_recover(
        dwb_path: &Path,
        page_storage: &dyn PageStorage,
        page_size: usize,
    ) -> io::Result<(u32, u32)> {
        // Read the entire DWB file into memory via spawn_blocking.
        let path_buf = dwb_path.to_path_buf();
        let file_data = tokio::task::spawn_blocking(move || -> io::Result<Option<Vec<u8>>> {
            use std::io::Read;

            if !path_buf.exists() {
                return Ok(None);
            }

            let metadata = std::fs::metadata(&path_buf)?;
            if metadata.len() == 0 {
                return Ok(None);
            }

            let mut file = std::fs::File::open(&path_buf)?;
            let mut data = Vec::with_capacity(metadata.len() as usize);
            file.read_to_end(&mut data)?;
            Ok(Some(data))
        })
        .await
        .map_err(|e| io::Error::other(format!("spawn_blocking join error: {e}")))??;

        let file_data = match file_data {
            Some(data) => data,
            None => return Ok((0, 0)),
        };

        if file_data.len() < DWB_HEADER_SIZE {
            // Partial header -- treat as empty.
            Self::truncate_dwb(dwb_path).await?;
            return Ok((0, 0));
        }

        let header_buf = &file_data[..DWB_HEADER_SIZE];
        let magic = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let _version = u16::from_le_bytes([header_buf[4], header_buf[5]]);
        let dwb_page_size = u16::from_le_bytes([header_buf[6], header_buf[7]]);
        let page_count = u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
        let stored_checksum = u32::from_le_bytes([header_buf[12], header_buf[13], header_buf[14], header_buf[15]]);

        // Verify magic.
        if magic != DWB_MAGIC {
            Self::truncate_dwb(dwb_path).await?;
            return Ok((0, 0));
        }

        // Verify header checksum.
        let expected_checksum = crc32fast::hash(&header_buf[0..12]);
        if stored_checksum != expected_checksum {
            Self::truncate_dwb(dwb_path).await?;
            return Ok((0, 0));
        }

        // Verify page size.
        if dwb_page_size as usize != page_size {
            return Err(crate::error::StorageError::Corruption(format!(
                "DWB page_size mismatch: DWB has {}, expected {}",
                dwb_page_size, page_size
            ))
            .into());
        }

        let mut restored = 0u32;
        let mut skipped_corrupt = 0u32;
        let entry_size = DWB_ENTRY_PREFIX_SIZE + page_size;
        let mut offset = DWB_HEADER_SIZE;

        for _ in 0..page_count {
            if offset + entry_size > file_data.len() {
                // Incomplete entry -- crash during DWB write.
                break;
            }

            let entry_buf = &file_data[offset..offset + entry_size];
            offset += entry_size;

            let page_id = crate::util::read_u32_le(entry_buf, 0)?;
            let dwb_page_data = &entry_buf[DWB_ENTRY_PREFIX_SIZE..];

            // Verify DWB page checksum.
            let dwb_ref = SlottedPageRef::from_buf(dwb_page_data)?;
            if !dwb_ref.verify_checksum() {
                // DWB page itself is corrupt. Skip.
                skipped_corrupt += 1;
                continue;
            }

            // Read storage page and check its checksum.
            let mut storage_page = vec![0u8; page_size];
            match page_storage.read_page(page_id, &mut storage_page).await {
                Ok(()) => {
                    let storage_ref = SlottedPageRef::from_buf(&storage_page)?;
                    if !storage_ref.verify_checksum() {
                        // Torn write -- restore from DWB.
                        page_storage.write_page(page_id, dwb_page_data).await?;
                        restored += 1;
                    }
                }
                Err(_) => {
                    // Can't read page -- restore from DWB.
                    page_storage.write_page(page_id, dwb_page_data).await?;
                    restored += 1;
                }
            }
        }

        page_storage.sync().await?;
        Self::truncate_dwb(dwb_path).await?;

        if skipped_corrupt > 0 {
            tracing::warn!(
                "DWB recovery skipped {} corrupt page(s). \
                 Data may be lost if the corresponding storage pages are also corrupt.",
                skipped_corrupt,
            );
        }

        Ok((restored, skipped_corrupt))
    }

    /// Truncate the DWB file.
    async fn truncate_dwb(path: &Path) -> io::Result<()> {
        let path_buf = path.to_path_buf();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            if !path_buf.exists() {
                return Ok(());
            }
            let file = std::fs::OpenOptions::new().write(true).open(&path_buf)?;
            file.set_len(0)?;
            file.sync_data()?;
            Ok(())
        })
        .await
        .map_err(|e| io::Error::other(format!("spawn_blocking join error: {e}")))?
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, MemoryWalStorage};
    use crate::page::{PageType, SlottedPage};
    use crate::wal::WalRecord;

    const PAGE_SIZE: usize = 4096;

    /// Helper: encode a WAL frame (matches wal.rs encode_frame).
    fn encode_frame(record_type: u8, payload: &[u8]) -> Vec<u8> {
        let payload_len = payload.len() as u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type]);
        hasher.update(payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(9 + payload.len());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.push(record_type);
        frame.extend_from_slice(payload);
        frame
    }

    /// A counting handler that records all handled records.
    struct CountingHandler {
        records: Vec<WalRecord>,
    }

    impl CountingHandler {
        fn new() -> Self {
            Self {
                records: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl WalRecordHandler for CountingHandler {
        async fn handle_record(&mut self, record: &WalRecord) -> io::Result<()> {
            self.records.push(record.clone());
            Ok(())
        }
    }

    /// A handler that returns an error on the Nth record.
    struct ErrorOnNthHandler {
        count: usize,
        error_on: usize,
    }

    impl ErrorOnNthHandler {
        fn new(error_on: usize) -> Self {
            Self {
                count: 0,
                error_on,
            }
        }
    }

    #[async_trait]
    impl WalRecordHandler for ErrorOnNthHandler {
        async fn handle_record(&mut self, _record: &WalRecord) -> io::Result<()> {
            self.count += 1;
            if self.count == self.error_on {
                return Err(io::Error::other("handler error"));
            }
            Ok(())
        }
    }

    /// Helper: create a valid page buffer.
    fn make_valid_page(page_id: u32) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::init(&mut buf, page_id, PageType::Heap);
        let data = format!("page-{}", page_id);
        page.insert_slot(data.as_bytes()).unwrap();
        page.stamp_checksum();
        buf
    }

    // ─── Test 1: Clean recovery ───
    // Checkpoint, then recover -> no records replayed, end_lsn == checkpoint_lsn.

    #[tokio::test]
    async fn clean_recovery() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        let checkpoint_lsn = 0;
        let mut handler = CountingHandler::new();

        let (end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        assert_eq!(end_lsn, checkpoint_lsn);
        assert_eq!(handler.records.len(), 0);
    }

    // ─── Test 2: WAL replay ───
    // Write 5 WAL records after checkpoint. Recover with counting handler. Verify 5 delivered.

    #[tokio::test]
    async fn wal_replay() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 5 records.
        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).await.unwrap();
        }

        let checkpoint_lsn = 0;
        let mut handler = CountingHandler::new();

        let (end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        assert_eq!(handler.records.len(), 5);
        assert!(end_lsn > checkpoint_lsn);

        // Verify records.
        for (i, record) in handler.records.iter().enumerate() {
            let expected = format!("record-{}", i);
            assert_eq!(record.payload, expected.as_bytes());
            assert_eq!(record.record_type, 0x01);
        }
    }

    // ─── Test 3: DWB + WAL recovery ───
    // Write DWB with torn pages + WAL records. Recover. Verify pages restored AND WAL replayed.

    #[tokio::test]
    async fn dwb_and_wal_recovery() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        page_storage.extend(5).await.unwrap();

        // Initialize pages in storage.
        for i in 0..5u32 {
            let page = make_valid_page(i);
            page_storage.write_page(i, &page).await.unwrap();
        }

        // Write DWB file with valid page data.
        let dwb_pages: Vec<Vec<u8>> = (0..5u32).map(make_valid_page).collect();
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            // Header.
            let mut header = [0u8; 16];
            header[0..4].copy_from_slice(&0x44574200u32.to_le_bytes());
            header[4..6].copy_from_slice(&1u16.to_le_bytes());
            header[6..8].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
            header[8..12].copy_from_slice(&5u32.to_le_bytes());
            let checksum = crc32fast::hash(&header[0..12]);
            header[12..16].copy_from_slice(&checksum.to_le_bytes());
            file.write_all(&header).unwrap();

            for i in 0..5u32 {
                file.write_all(&i.to_le_bytes()).unwrap();
                file.write_all(&dwb_pages[i as usize]).unwrap();
            }
            file.sync_data().unwrap();
        }

        // Corrupt page 2 in storage.
        let corrupt = vec![0xFFu8; PAGE_SIZE];
        page_storage.write_page(2, &corrupt).await.unwrap();

        // Write WAL records.
        let wal_storage = MemoryWalStorage::new();
        for i in 0..3u32 {
            let payload = format!("wal-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).await.unwrap();
        }

        let mut handler = CountingHandler::new();
        let (_end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            Some(dwb_path.as_path()),
            0,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        // Verify WAL records replayed.
        assert_eq!(handler.records.len(), 3);

        // Verify page 2 was restored.
        let mut buf = vec![0u8; PAGE_SIZE];
        page_storage.read_page(2, &mut buf).await.unwrap();
        assert_eq!(&buf, &dwb_pages[2]);
    }

    // ─── Test 4: Corrupt WAL record ───
    // Write 3 valid + 1 corrupt WAL record. Recover. Verify only 3 records replayed.

    #[tokio::test]
    async fn corrupt_wal_record() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 3 valid records.
        for i in 0..3u32 {
            let payload = format!("valid-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).await.unwrap();
        }

        // Write a corrupt record (bad CRC).
        let mut bad_frame = encode_frame(0x01, b"corrupt");
        bad_frame[4] ^= 0xFF; // Flip CRC.
        wal_storage.append(&bad_frame).await.unwrap();

        // Write another valid record after the corrupt one.
        let good_after = encode_frame(0x01, b"after-corrupt");
        wal_storage.append(&good_after).await.unwrap();

        let mut handler = CountingHandler::new();
        let (_end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        // Only 3 records should be replayed (stops at corrupt).
        assert_eq!(handler.records.len(), 3);
    }

    // ─── Test 5: Empty WAL ───
    // Recover with no WAL records after checkpoint -> returns checkpoint_lsn.

    #[tokio::test]
    async fn empty_wal() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        let checkpoint_lsn = 42;
        let mut handler = CountingHandler::new();

        let (end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        assert_eq!(end_lsn, checkpoint_lsn);
        assert_eq!(handler.records.len(), 0);
    }

    // ─── Test 6: needs_recovery false ───
    // Clean state -> returns false.

    #[tokio::test]
    async fn needs_recovery_false() {
        let wal_storage = MemoryWalStorage::new();

        let result = Recovery::needs_recovery(None, &wal_storage, 0).await.unwrap();
        assert!(!result);
    }

    // ─── Test 7: needs_recovery true (DWB) ───
    // Non-empty DWB -> returns true.

    #[tokio::test]
    async fn needs_recovery_true_dwb() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        // Create non-empty DWB.
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&dwb_path).unwrap();
            file.write_all(b"some data").unwrap();
        }

        let wal_storage = MemoryWalStorage::new();
        let result =
            Recovery::needs_recovery(Some(dwb_path.as_path()), &wal_storage, 0).await.unwrap();
        assert!(result);
    }

    // ─── Test 8: needs_recovery true (WAL) ───
    // WAL has records past checkpoint -> returns true.

    #[tokio::test]
    async fn needs_recovery_true_wal() {
        let wal_storage = MemoryWalStorage::new();

        // Write a record.
        let frame = encode_frame(0x01, b"data");
        wal_storage.append(&frame).await.unwrap();

        let result = Recovery::needs_recovery(None, &wal_storage, 0).await.unwrap();
        assert!(result);
    }

    // ─── Test 9: Idempotent replay ───
    // Replay same records twice. Handler should produce same result.

    #[tokio::test]
    async fn idempotent_replay() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).await.unwrap();
        }

        // First replay.
        let mut handler1 = CountingHandler::new();
        let (end1, _stats1) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler1,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        // Second replay.
        let mut handler2 = CountingHandler::new();
        let (end2, _stats2) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler2,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        assert_eq!(end1, end2);
        assert_eq!(handler1.records.len(), handler2.records.len());

        for (r1, r2) in handler1.records.iter().zip(handler2.records.iter()) {
            assert_eq!(r1.lsn, r2.lsn);
            assert_eq!(r1.record_type, r2.record_type);
            assert_eq!(r1.payload, r2.payload);
        }
    }

    // ─── Test 10: In-memory recovery ───
    // MemoryPageStorage, no DWB. Verify recovery is a no-op.

    #[tokio::test]
    async fn in_memory_recovery() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // No DWB, no WAL records.
        let mut handler = NoOpHandler;
        let (end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        assert_eq!(end_lsn, 0);

        // needs_recovery should be false.
        let needs = Recovery::needs_recovery(None, &wal_storage, 0).await.unwrap();
        assert!(!needs);
    }

    // ─── Test 11: Handler error propagation ───
    // Handler returns error on record 3. Recovery stops and propagates error.

    #[tokio::test]
    async fn handler_error_propagation() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 5 records.
        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).await.unwrap();
        }

        // Handler errors on the 3rd record.
        let mut handler = ErrorOnNthHandler::new(3);
        let result = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_string(), "handler error");
        // Handler should have been called exactly 3 times (error on 3rd).
        assert_eq!(handler.count, 3);
    }

    // ─── Test: Checkpoint records are skipped during replay ───

    #[tokio::test]
    async fn checkpoint_records_skipped() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write: record, checkpoint, record, checkpoint, record.
        let frame1 = encode_frame(0x01, b"tx-1");
        wal_storage.append(&frame1).await.unwrap();

        let ckpt = encode_frame(WAL_RECORD_CHECKPOINT, &42u64.to_le_bytes());
        wal_storage.append(&ckpt).await.unwrap();

        let frame2 = encode_frame(0x01, b"tx-2");
        wal_storage.append(&frame2).await.unwrap();

        let ckpt2 = encode_frame(WAL_RECORD_CHECKPOINT, &100u64.to_le_bytes());
        wal_storage.append(&ckpt2).await.unwrap();

        let frame3 = encode_frame(0x01, b"tx-3");
        wal_storage.append(&frame3).await.unwrap();

        let mut handler = CountingHandler::new();
        let (_end_lsn, _stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();

        // Only the 3 non-checkpoint records should be delivered.
        assert_eq!(handler.records.len(), 3);
        assert_eq!(handler.records[0].payload, b"tx-1");
        assert_eq!(handler.records[1].payload, b"tx-2");
        assert_eq!(handler.records[2].payload, b"tx-3");
    }

    // ─── Test 11: BestEffort mode recovers records around corruption ───

    #[tokio::test]
    async fn best_effort_skips_corrupt() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write: valid record, corrupt bytes, valid record.
        let frame1 = encode_frame(0x01, b"before");
        wal_storage.append(&frame1).await.unwrap();

        // Write corrupt bytes (invalid CRC).
        let corrupt = vec![0x05, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x42, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        wal_storage.append(&corrupt).await.unwrap();

        let frame2 = encode_frame(0x01, b"after");
        wal_storage.append(&frame2).await.unwrap();

        // Strict mode: only gets first record.
        let mut strict_handler = CountingHandler::new();
        let (_end_lsn, strict_stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut strict_handler,
            RecoveryMode::Strict,
        )
        .await
        .unwrap();
        assert_eq!(strict_handler.records.len(), 1);
        assert_eq!(strict_stats.records_skipped_corrupt, 0);

        // BestEffort mode: gets both records, reports 1 skipped.
        let mut best_handler = CountingHandler::new();
        let (_end_lsn, best_stats) = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut best_handler,
            RecoveryMode::BestEffort,
        )
        .await
        .unwrap();
        assert_eq!(best_handler.records.len(), 2);
        assert_eq!(best_stats.records_skipped_corrupt, 1);
        assert_eq!(best_handler.records[0].payload, b"before");
        assert_eq!(best_handler.records[1].payload, b"after");
    }
}
