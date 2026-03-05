//! Crash recovery coordinator.
//!
//! Restores the storage engine to a consistent state after a crash. First
//! restores any torn pages via the DWB, then replays WAL records from the last
//! checkpoint. Higher layers provide a callback to interpret domain-specific
//! WAL records.

use crate::backend::{PageStorage, WalStorage};
use crate::page::SlottedPageRef;
use crate::wal::{Lsn, WalRecord, WAL_RECORD_CHECKPOINT};
use std::io;
use std::path::Path;

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
pub trait WalRecordHandler {
    /// Handle a single WAL record during replay.
    /// Called in LSN order, only for records after checkpoint_lsn.
    fn handle_record(&mut self, record: &WalRecord) -> io::Result<()>;
}

// ─── NoOpHandler ───

/// No-op handler for when no higher-layer replay is needed
/// (e.g., storage engine self-test).
pub struct NoOpHandler;

impl WalRecordHandler for NoOpHandler {
    fn handle_record(&mut self, _record: &WalRecord) -> io::Result<()> {
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
    /// Returns the LSN after the last valid record (new write position).
    pub fn run(
        page_storage: &dyn PageStorage,
        wal_storage: &dyn WalStorage,
        dwb_path: Option<&Path>,
        checkpoint_lsn: Lsn,
        page_size: usize,
        handler: &mut dyn WalRecordHandler,
    ) -> io::Result<Lsn> {
        // Step 2: DWB recovery (if applicable).
        if let Some(path) = dwb_path {
            Self::dwb_recover(path, page_storage, page_size)?;
        }

        // Step 3: WAL replay from checkpoint_lsn.
        let end_lsn = Self::replay_wal(wal_storage, checkpoint_lsn, handler)?;

        Ok(end_lsn)
    }

    /// Check if recovery is needed (DWB non-empty or WAL has records past checkpoint).
    pub fn needs_recovery(
        dwb_path: Option<&Path>,
        wal_storage: &dyn WalStorage,
        checkpoint_lsn: Lsn,
    ) -> io::Result<bool> {
        // Check DWB.
        if let Some(path) = dwb_path
            && path.exists() {
                let metadata = std::fs::metadata(path)?;
                if metadata.len() > 0 {
                    return Ok(true);
                }
            }

        // Check WAL: see if there are any valid records past checkpoint_lsn.
        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        let n = wal_storage.read_from(checkpoint_lsn, &mut header_buf)?;
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
    fn replay_wal(
        wal_storage: &dyn WalStorage,
        start_lsn: Lsn,
        handler: &mut dyn WalRecordHandler,
    ) -> io::Result<Lsn> {
        let mut end_lsn = start_lsn;
        let mut current_lsn = start_lsn;

        loop {
            // Read frame header.
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
            let n = wal_storage.read_from(current_lsn, &mut header_buf)?;
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
                break;
            }

            // Read payload.
            let mut payload = vec![0u8; payload_len];
            let n = wal_storage.read_from(
                current_lsn + WAL_FRAME_HEADER_SIZE as u64,
                &mut payload,
            )?;
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
                // CRC mismatch -- stop replay.
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
            handler.handle_record(&record)?;
        }

        Ok(end_lsn)
    }

    /// DWB recovery: restore torn pages from the DWB file.
    fn dwb_recover(
        dwb_path: &Path,
        page_storage: &dyn PageStorage,
        page_size: usize,
    ) -> io::Result<u32> {
        use std::io::Read;

        if !dwb_path.exists() {
            return Ok(0);
        }

        let metadata = std::fs::metadata(dwb_path)?;
        if metadata.len() == 0 {
            return Ok(0);
        }

        let mut file = std::fs::File::open(dwb_path)?;

        // Read header.
        let mut header_buf = [0u8; DWB_HEADER_SIZE];
        let bytes_read = file.read(&mut header_buf)?;
        if bytes_read < DWB_HEADER_SIZE {
            // Partial header -- treat as empty.
            Self::truncate_dwb(dwb_path)?;
            return Ok(0);
        }

        let magic = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let _version = u16::from_le_bytes([header_buf[4], header_buf[5]]);
        let dwb_page_size = u16::from_le_bytes([header_buf[6], header_buf[7]]);
        let page_count = u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
        let stored_checksum = u32::from_le_bytes([header_buf[12], header_buf[13], header_buf[14], header_buf[15]]);

        // Verify magic.
        if magic != DWB_MAGIC {
            Self::truncate_dwb(dwb_path)?;
            return Ok(0);
        }

        // Verify header checksum.
        let expected_checksum = crc32fast::hash(&header_buf[0..12]);
        if stored_checksum != expected_checksum {
            Self::truncate_dwb(dwb_path)?;
            return Ok(0);
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
        let entry_size = DWB_ENTRY_PREFIX_SIZE + page_size;

        for _ in 0..page_count {
            let mut entry_buf = vec![0u8; entry_size];
            let bytes_read = file.read(&mut entry_buf)?;
            if bytes_read < entry_size {
                // Incomplete entry -- crash during DWB write.
                break;
            }

            let page_id = crate::util::read_u32_le(&entry_buf, 0)?;
            let dwb_page_data = &entry_buf[DWB_ENTRY_PREFIX_SIZE..];

            // Verify DWB page checksum.
            let dwb_ref = SlottedPageRef::from_buf(dwb_page_data)?;
            if !dwb_ref.verify_checksum() {
                // DWB page itself is corrupt. Skip.
                continue;
            }

            // Read storage page and check its checksum.
            let mut storage_page = vec![0u8; page_size];
            match page_storage.read_page(page_id, &mut storage_page) {
                Ok(()) => {
                    let storage_ref = SlottedPageRef::from_buf(&storage_page)?;
                    if !storage_ref.verify_checksum() {
                        // Torn write -- restore from DWB.
                        page_storage.write_page(page_id, dwb_page_data)?;
                        restored += 1;
                    }
                }
                Err(_) => {
                    // Can't read page -- restore from DWB.
                    page_storage.write_page(page_id, dwb_page_data)?;
                    restored += 1;
                }
            }
        }

        page_storage.sync()?;
        Self::truncate_dwb(dwb_path)?;

        Ok(restored)
    }

    /// Truncate the DWB file.
    fn truncate_dwb(path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(0)?;
        file.sync_data()?;
        Ok(())
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

    impl WalRecordHandler for CountingHandler {
        fn handle_record(&mut self, record: &WalRecord) -> io::Result<()> {
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

    impl WalRecordHandler for ErrorOnNthHandler {
        fn handle_record(&mut self, _record: &WalRecord) -> io::Result<()> {
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

    #[test]
    fn clean_recovery() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        let checkpoint_lsn = 0;
        let mut handler = CountingHandler::new();

        let end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        assert_eq!(end_lsn, checkpoint_lsn);
        assert_eq!(handler.records.len(), 0);
    }

    // ─── Test 2: WAL replay ───
    // Write 5 WAL records after checkpoint. Recover with counting handler. Verify 5 delivered.

    #[test]
    fn wal_replay() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 5 records.
        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).unwrap();
        }

        let checkpoint_lsn = 0;
        let mut handler = CountingHandler::new();

        let end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
        )
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

    #[test]
    fn dwb_and_wal_recovery() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        page_storage.extend(5).unwrap();

        // Initialize pages in storage.
        for i in 0..5u32 {
            let page = make_valid_page(i);
            page_storage.write_page(i, &page).unwrap();
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
        page_storage.write_page(2, &corrupt).unwrap();

        // Write WAL records.
        let wal_storage = MemoryWalStorage::new();
        for i in 0..3u32 {
            let payload = format!("wal-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).unwrap();
        }

        let mut handler = CountingHandler::new();
        let _end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            Some(dwb_path.as_path()),
            0,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        // Verify WAL records replayed.
        assert_eq!(handler.records.len(), 3);

        // Verify page 2 was restored.
        let mut buf = vec![0u8; PAGE_SIZE];
        page_storage.read_page(2, &mut buf).unwrap();
        assert_eq!(&buf, &dwb_pages[2]);
    }

    // ─── Test 4: Corrupt WAL record ───
    // Write 3 valid + 1 corrupt WAL record. Recover. Verify only 3 records replayed.

    #[test]
    fn corrupt_wal_record() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 3 valid records.
        for i in 0..3u32 {
            let payload = format!("valid-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).unwrap();
        }

        // Write a corrupt record (bad CRC).
        let mut bad_frame = encode_frame(0x01, b"corrupt");
        bad_frame[4] ^= 0xFF; // Flip CRC.
        wal_storage.append(&bad_frame).unwrap();

        // Write another valid record after the corrupt one.
        let good_after = encode_frame(0x01, b"after-corrupt");
        wal_storage.append(&good_after).unwrap();

        let mut handler = CountingHandler::new();
        let _end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        // Only 3 records should be replayed (stops at corrupt).
        assert_eq!(handler.records.len(), 3);
    }

    // ─── Test 5: Empty WAL ───
    // Recover with no WAL records after checkpoint -> returns checkpoint_lsn.

    #[test]
    fn empty_wal() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        let checkpoint_lsn = 42;
        let mut handler = CountingHandler::new();

        let end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            checkpoint_lsn,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        assert_eq!(end_lsn, checkpoint_lsn);
        assert_eq!(handler.records.len(), 0);
    }

    // ─── Test 6: needs_recovery false ───
    // Clean state -> returns false.

    #[test]
    fn needs_recovery_false() {
        let wal_storage = MemoryWalStorage::new();

        let result = Recovery::needs_recovery(None, &wal_storage, 0).unwrap();
        assert!(!result);
    }

    // ─── Test 7: needs_recovery true (DWB) ───
    // Non-empty DWB -> returns true.

    #[test]
    fn needs_recovery_true_dwb() {
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
            Recovery::needs_recovery(Some(dwb_path.as_path()), &wal_storage, 0).unwrap();
        assert!(result);
    }

    // ─── Test 8: needs_recovery true (WAL) ───
    // WAL has records past checkpoint -> returns true.

    #[test]
    fn needs_recovery_true_wal() {
        let wal_storage = MemoryWalStorage::new();

        // Write a record.
        let frame = encode_frame(0x01, b"data");
        wal_storage.append(&frame).unwrap();

        let result = Recovery::needs_recovery(None, &wal_storage, 0).unwrap();
        assert!(result);
    }

    // ─── Test 9: Idempotent replay ───
    // Replay same records twice. Handler should produce same result.

    #[test]
    fn idempotent_replay() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).unwrap();
        }

        // First replay.
        let mut handler1 = CountingHandler::new();
        let end1 = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler1,
        )
        .unwrap();

        // Second replay.
        let mut handler2 = CountingHandler::new();
        let end2 = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler2,
        )
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

    #[test]
    fn in_memory_recovery() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // No DWB, no WAL records.
        let mut handler = NoOpHandler;
        let end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        assert_eq!(end_lsn, 0);

        // needs_recovery should be false.
        let needs = Recovery::needs_recovery(None, &wal_storage, 0).unwrap();
        assert!(!needs);
    }

    // ─── Test 11: Handler error propagation ───
    // Handler returns error on record 3. Recovery stops and propagates error.

    #[test]
    fn handler_error_propagation() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write 5 records.
        for i in 0..5u32 {
            let payload = format!("record-{}", i);
            let frame = encode_frame(0x01, payload.as_bytes());
            wal_storage.append(&frame).unwrap();
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
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.to_string(), "handler error");
        // Handler should have been called exactly 3 times (error on 3rd).
        assert_eq!(handler.count, 3);
    }

    // ─── Test: Checkpoint records are skipped during replay ───

    #[test]
    fn checkpoint_records_skipped() {
        let page_storage = MemoryPageStorage::new(PAGE_SIZE);
        let wal_storage = MemoryWalStorage::new();

        // Write: record, checkpoint, record, checkpoint, record.
        let frame1 = encode_frame(0x01, b"tx-1");
        wal_storage.append(&frame1).unwrap();

        let ckpt = encode_frame(WAL_RECORD_CHECKPOINT, &42u64.to_le_bytes());
        wal_storage.append(&ckpt).unwrap();

        let frame2 = encode_frame(0x01, b"tx-2");
        wal_storage.append(&frame2).unwrap();

        let ckpt2 = encode_frame(WAL_RECORD_CHECKPOINT, &100u64.to_le_bytes());
        wal_storage.append(&ckpt2).unwrap();

        let frame3 = encode_frame(0x01, b"tx-3");
        wal_storage.append(&frame3).unwrap();

        let mut handler = CountingHandler::new();
        let _end_lsn = Recovery::run(
            &page_storage,
            &wal_storage,
            None,
            0,
            PAGE_SIZE,
            &mut handler,
        )
        .unwrap();

        // Only the 3 non-checkpoint records should be delivered.
        assert_eq!(handler.records.len(), 3);
        assert_eq!(handler.records[0].payload, b"tx-1");
        assert_eq!(handler.records[1].payload, b"tx-2");
        assert_eq!(handler.records[2].payload, b"tx-3");
    }
}
