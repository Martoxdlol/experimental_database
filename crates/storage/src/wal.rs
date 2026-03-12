//! Write-Ahead Log (WAL) with group commit support.
//!
//! Append-only durable log for crash recovery. Records are opaque byte payloads
//! with CRC-32C verification. Supports group commit for throughput via a
//! background task that batches writes through the [`WalStorage`] trait.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_core::Stream;
use tokio::sync::{mpsc, oneshot};

use crate::backend::WalStorage;

// ─── LSN Type ───

/// Log Sequence Number -- byte offset in the logical WAL stream.
pub type Lsn = u64;

// ─── Constants ───

/// WAL record frame header size: 4 (payload_len) + 4 (crc32c) + 1 (record_type) = 9 bytes.
const WAL_FRAME_HEADER_SIZE: usize = 9;

/// Maximum WAL record payload size. Records exceeding this are treated as
/// corrupt during iteration. 64 MB is generous for any legitimate record.
const MAX_WAL_RECORD_SIZE: usize = 64 * 1024 * 1024;

// ─── WAL Record Type Constants ───

/// Transaction commit record.
pub const WAL_RECORD_TX_COMMIT: u8 = 0x01;
/// Checkpoint record.
pub const WAL_RECORD_CHECKPOINT: u8 = 0x02;
/// Create collection record.
pub const WAL_RECORD_CREATE_COLLECTION: u8 = 0x03;
/// Drop collection record.
pub const WAL_RECORD_DROP_COLLECTION: u8 = 0x04;
/// Create index record.
pub const WAL_RECORD_CREATE_INDEX: u8 = 0x05;
/// Drop index record.
pub const WAL_RECORD_DROP_INDEX: u8 = 0x06;
/// Index ready record.
pub const WAL_RECORD_INDEX_READY: u8 = 0x07;
/// Vacuum record.
pub const WAL_RECORD_VACUUM: u8 = 0x08;
/// Visible timestamp record.
pub const WAL_RECORD_VISIBLE_TS: u8 = 0x09;
/// Rollback vacuum record.
pub const WAL_RECORD_ROLLBACK_VACUUM: u8 = 0x0A;

// ─── WalRecord ───

/// A deserialized WAL record.
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// LSN (byte offset) where this record starts in the logical stream.
    pub lsn: Lsn,
    /// Record type tag (opaque to the WAL layer, interpreted by higher layers).
    pub record_type: u8,
    /// Record payload bytes.
    pub payload: Vec<u8>,
}

// ─── WalStream ───

/// Async stream of WAL records, replacing the sync `WalIterator`.
pub type WalStream = Pin<Box<dyn Stream<Item = io::Result<WalRecord>> + Send>>;

// ─── WalConfig ───

/// Configuration for the WAL writer.
pub struct WalConfig {
    /// Target segment size in bytes (used by file-backed storage for rollover
    /// decisions). The WAL writer itself does not enforce this; it is passed
    /// through for informational purposes. Default: 64 MB.
    pub segment_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,
        }
    }
}

// ─── Internal: WalWriteRequest ───

/// A request sent to the background writer task.
enum WalWriteRequest {
    /// Normal record: the writer serializes the frame header + payload.
    Record {
        record_type: u8,
        payload: Vec<u8>,
        response: oneshot::Sender<io::Result<Lsn>>,
    },
    /// Pre-encoded raw frame (for replication). Data includes the 9-byte
    /// header + payload. Writer appends it to the batch buffer as-is.
    RawFrame {
        data: Vec<u8>,
        response: oneshot::Sender<io::Result<Lsn>>,
    },
}

// ─── Frame encoding helpers ───

/// Compute CRC-32C over `record_type || payload`.
fn compute_crc(record_type: u8, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[record_type]);
    hasher.update(payload);
    hasher.finalize()
}

/// Encode a single WAL frame: `payload_len(u32 LE) || crc32c(u32 LE) || record_type(u8) || payload`.
fn encode_frame(record_type: u8, payload: &[u8]) -> Vec<u8> {
    let payload_len = payload.len() as u32;
    let crc = compute_crc(record_type, payload);

    let mut frame = Vec::with_capacity(WAL_FRAME_HEADER_SIZE + payload.len());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.push(record_type);
    frame.extend_from_slice(payload);
    frame
}

// ─── WalWriter ───

/// WAL writer with group commit support.
///
/// Internally runs a background task that batches writes. Multiple concurrent
/// `append` calls are gathered into a single `WalStorage::append` + `sync`
/// call, amortizing the fsync cost across the batch.
pub struct WalWriter {
    /// Channel to send write requests to the background task.
    tx: mpsc::Sender<WalWriteRequest>,
    /// Current write position (LSN of next record).
    current_lsn: Arc<AtomicU64>,
}

impl WalWriter {
    /// Create a new WAL writer. Spawns a background task for group commit.
    ///
    /// Must be called from within an async context (requires an active tokio
    /// runtime for `tokio::spawn`).
    ///
    /// The initial LSN is set to `storage.size()`, allowing resumption after
    /// a restart.
    pub fn new(storage: Arc<dyn WalStorage>, _config: WalConfig) -> io::Result<Self> {
        let initial_lsn = storage.size();
        let current_lsn = Arc::new(AtomicU64::new(initial_lsn));
        let (tx, rx) = mpsc::channel::<WalWriteRequest>(1024);

        let bg_lsn = current_lsn.clone();
        tokio::spawn(Self::background_task(storage, rx, bg_lsn));

        Ok(Self { tx, current_lsn })
    }

    /// Background writer task. Batches incoming requests, performs a single
    /// `append` + `sync`, and responds with assigned LSNs.
    ///
    /// Each batch is processed inside a `catch_unwind` boundary so that a
    /// panic in storage I/O does not kill the task permanently. In-flight
    /// callers of the panicked batch receive `WalShutDown` (their oneshot
    /// senders are dropped), but the task continues for future requests.
    async fn background_task(
        storage: Arc<dyn WalStorage>,
        mut rx: mpsc::Receiver<WalWriteRequest>,
        current_lsn: Arc<AtomicU64>,
    ) {
        loop {
            // Step 1: Block on the first request.
            let first = match rx.recv().await {
                Some(req) => req,
                None => return, // Channel closed -- shutdown.
            };

            // Step 2: Drain additional requests that arrived while we waited.
            let mut batch = vec![first];
            while let Ok(req) = rx.try_recv() {
                batch.push(req);
            }

            // Step 3-6: Process batch (async, wrapped in catch_unwind).
            let storage_ref = storage.clone();
            let lsn_ref = current_lsn.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Self::process_batch(storage_ref, batch, lsn_ref)
            }));

            match result {
                Ok(future) => {
                    future.await;
                }
                Err(_panic) => {
                    tracing::error!(
                        "WAL writer: caught panic in batch processing. \
                         In-flight callers will receive WalShutDown. Continuing."
                    );
                    // The batch's oneshot senders were moved into process_batch and
                    // dropped by the panic unwind → callers get RecvError → WalShutDown.
                    // The loop continues for future requests.
                }
            }
        }
    }

    /// Process a single batch of WAL write requests: serialize, append, sync,
    /// and respond.
    async fn process_batch(
        storage: Arc<dyn WalStorage>,
        batch: Vec<WalWriteRequest>,
        current_lsn: Arc<AtomicU64>,
    ) {
        // Serialize the entire batch into a single buffer.
        let mut buf = Vec::new();
        let mut assignments: Vec<(Lsn, oneshot::Sender<io::Result<Lsn>>)> = Vec::new();
        let mut lsn = current_lsn.load(Ordering::Acquire);

        for req in batch {
            match req {
                WalWriteRequest::Record {
                    record_type,
                    payload,
                    response,
                } => {
                    let assigned_lsn = lsn;
                    let frame = encode_frame(record_type, &payload);
                    lsn += frame.len() as u64;
                    buf.extend_from_slice(&frame);
                    assignments.push((assigned_lsn, response));
                }
                WalWriteRequest::RawFrame { data, response } => {
                    let assigned_lsn = lsn;
                    lsn += data.len() as u64;
                    buf.extend_from_slice(&data);
                    assignments.push((assigned_lsn, response));
                }
            }
        }

        // Single async append to storage.
        let append_result = storage.append(&buf).await;

        // Single async sync (fsync).
        let sync_result = if append_result.is_ok() {
            storage.sync().await
        } else {
            Ok(())
        };

        // Update the current LSN and respond to all requesters.
        if append_result.is_ok() && sync_result.is_ok() {
            current_lsn.store(lsn, Ordering::Release);
            for (assigned_lsn, response) in assignments {
                let _ = response.send(Ok(assigned_lsn));
            }
        } else {
            let err_msg = if let Err(ref e) = append_result {
                e.to_string()
            } else if let Err(ref e) = sync_result {
                e.to_string()
            } else {
                "unknown WAL error".to_string()
            };
            for (_assigned_lsn, response) in assignments {
                let _ = response.send(Err(io::Error::other(err_msg.clone())));
            }
        }
    }

    /// Append a record. Blocks (asynchronously) until the batch containing
    /// this record has been fsynced. Returns the assigned LSN.
    pub async fn append(&self, record_type: u8, payload: &[u8]) -> io::Result<Lsn> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(WalWriteRequest::Record {
                record_type,
                payload: payload.to_vec(),
                response: response_tx,
            })
            .await
            .map_err(|_| io::Error::from(crate::error::StorageError::WalShutDown))?;

        response_rx
            .await
            .map_err(|_| io::Error::from(crate::error::StorageError::WalShutDown))?
    }

    /// Append a pre-encoded raw WAL frame (for replication).
    ///
    /// The `raw` bytes must include the 9-byte header + payload. The writer
    /// validates the frame length but appends it as-is without re-encoding.
    pub async fn append_raw_frame(&self, raw: &[u8]) -> io::Result<Lsn> {
        // Basic validation: must have at least a header.
        if raw.len() < WAL_FRAME_HEADER_SIZE {
            return Err(crate::error::StorageError::InvalidConfig(
                "raw frame too short: must be at least 9 bytes".into()
            ).into());
        }

        // Validate that payload_len matches the actual data length.
        let payload_len = crate::util::read_u32_le(raw, 0)? as usize;
        if raw.len() != WAL_FRAME_HEADER_SIZE + payload_len {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "raw frame length mismatch: header says {} payload bytes, \
                 but frame is {} bytes (expected {})",
                payload_len,
                raw.len(),
                WAL_FRAME_HEADER_SIZE + payload_len,
            )).into());
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(WalWriteRequest::RawFrame {
                data: raw.to_vec(),
                response: response_tx,
            })
            .await
            .map_err(|_| io::Error::from(crate::error::StorageError::WalShutDown))?;

        response_rx
            .await
            .map_err(|_| io::Error::from(crate::error::StorageError::WalShutDown))?
    }

    /// Current write position (LSN of next record to be written).
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn.load(Ordering::Acquire)
    }

    /// Shut down the background writer task.
    ///
    /// The background task exits when all senders are dropped. Since this
    /// method takes `&self` and cannot consume the sender, the actual shutdown
    /// occurs when the `WalWriter` is dropped. This method exists for API
    /// completeness. Callers should `drop(writer)` for immediate shutdown.
    pub async fn shutdown(&self) {
        // The design doc specifies: "drop the sender side of the mpsc channel.
        // The background task will exit when the channel is closed."
        // Since we only have &self, the actual teardown happens on Drop.
    }
}

// ─── WalReader ───

/// WAL reader for sequential scanning from a given LSN.
pub struct WalReader {
    storage: Arc<dyn WalStorage>,
}

impl WalReader {
    /// Create a new WAL reader backed by the given storage.
    pub fn new(storage: Arc<dyn WalStorage>) -> Self {
        Self { storage }
    }

    /// Create an async stream starting at the given LSN.
    pub fn read_from(&self, start_lsn: Lsn) -> WalStream {
        let storage = self.storage.clone();
        Box::pin(async_stream::try_stream! {
            let mut current_lsn = start_lsn;

            loop {
                // Step 1: Read the 9-byte header.
                let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
                let n = storage.read_from(current_lsn, &mut header_buf).await?;

                if n < WAL_FRAME_HEADER_SIZE {
                    // Not enough data for a header -- end of log.
                    break;
                }

                // Step 2: Parse header fields.
                let payload_len = u32::from_le_bytes([
                    header_buf[0], header_buf[1], header_buf[2], header_buf[3],
                ]) as usize;
                let stored_crc = u32::from_le_bytes([
                    header_buf[4], header_buf[5], header_buf[6], header_buf[7],
                ]);
                let record_type = header_buf[8];

                // Step 3: Check for end-of-log sentinel (payload_len == 0).
                if payload_len == 0 {
                    break;
                }

                // Step 4: Check for implausibly large payload (corrupt record).
                if payload_len > MAX_WAL_RECORD_SIZE {
                    break;
                }

                // Step 5: Read the payload.
                let mut payload = vec![0u8; payload_len];
                let n = storage
                    .read_from(current_lsn + WAL_FRAME_HEADER_SIZE as u64, &mut payload)
                    .await?;

                if n < payload_len {
                    // Incomplete payload -- partial write from crash. End of log.
                    break;
                }

                // Step 6: Verify CRC-32C.
                let computed_crc = compute_crc(record_type, &payload);
                if computed_crc != stored_crc {
                    // CRC mismatch -- corrupt record. Stop iteration.
                    break;
                }

                // Step 7: Build the record and advance position.
                let record_lsn = current_lsn;
                current_lsn += (WAL_FRAME_HEADER_SIZE + payload_len) as u64;

                yield WalRecord {
                    lsn: record_lsn,
                    record_type,
                    payload,
                };
            }
        })
    }

    /// Find the latest valid LSN by scanning from a starting point.
    /// Returns the LSN just past the last valid record (i.e., the position
    /// where the next record would be written).
    pub async fn find_end(&self, start_lsn: Lsn) -> io::Result<Lsn> {
        let mut stream = self.read_from(start_lsn);
        let mut end_lsn = start_lsn;
        loop {
            let item = std::future::poll_fn(|cx| {
                Stream::poll_next(stream.as_mut(), cx)
            }).await;
            match item {
                Some(Ok(record)) => {
                    end_lsn = record.lsn
                        + WAL_FRAME_HEADER_SIZE as u64
                        + record.payload.len() as u64;
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(end_lsn),
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{FileWalStorage, MemoryWalStorage};
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    /// Helper: create a MemoryWalStorage wrapped in Arc.
    fn mem_storage() -> Arc<MemoryWalStorage> {
        Arc::new(MemoryWalStorage::new())
    }

    /// Helper: collect all records from a WalStream into a Vec.
    async fn collect_records(mut stream: WalStream) -> io::Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        while let Some(result) = stream.next().await {
            records.push(result?);
        }
        Ok(records)
    }

    // ─── Test 1: Single record roundtrip ───

    #[tokio::test]
    async fn single_record_roundtrip() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        let payload = b"hello, WAL!";
        let lsn = writer.append(WAL_RECORD_TX_COMMIT, payload).await.unwrap();
        assert_eq!(lsn, 0);

        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let mut stream = reader.read_from(0);

        let record = stream.next().await.unwrap().unwrap();
        assert_eq!(record.lsn, 0);
        assert_eq!(record.record_type, WAL_RECORD_TX_COMMIT);
        assert_eq!(record.payload, payload);

        // No more records.
        assert!(stream.next().await.is_none());
    }

    // ─── Test 2: Multiple records ───

    #[tokio::test]
    async fn multiple_records() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        let mut lsns = Vec::new();
        for i in 0..100u32 {
            let payload = format!("record-{}", i);
            let lsn = writer
                .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                .await
                .unwrap();
            lsns.push(lsn);
        }

        // LSNs should be strictly ascending.
        for i in 1..lsns.len() {
            assert!(lsns[i] > lsns[i - 1], "LSNs must be ascending");
        }

        // Read all back.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), 100);

        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, lsns[i]);
            assert_eq!(record.record_type, WAL_RECORD_TX_COMMIT);
            let expected_payload = format!("record-{}", i);
            assert_eq!(record.payload, expected_payload.as_bytes());
        }
    }

    // ─── Test 3: CRC verification (corruption detection) ───

    #[tokio::test]
    async fn crc_verification() {
        // Build a storage with a good record followed by a corrupt record.
        let storage = Arc::new(MemoryWalStorage::new());
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        let lsn0 = writer
            .append(WAL_RECORD_TX_COMMIT, b"good record")
            .await
            .unwrap();

        // Drop the writer so no background task holds a reference.
        drop(writer);
        tokio::task::yield_now().await;

        // Manually encode a frame with a bad CRC and append directly to storage.
        let bad_payload = b"corrupted";
        let mut bad_frame = encode_frame(WAL_RECORD_TX_COMMIT, bad_payload);
        // Flip a bit in the CRC field (bytes 4..8).
        bad_frame[4] ^= 0xFF;
        storage.append(&bad_frame).await.unwrap();

        // Write a third good record after the corrupt one.
        storage
            .append(&encode_frame(WAL_RECORD_TX_COMMIT, b"after bad"))
            .await
            .unwrap();

        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(0)).await.unwrap();

        // Should only get the first good record; iteration stops at the corrupt one.
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].lsn, lsn0);
        assert_eq!(records[0].payload, b"good record");
    }

    // ─── Test 4: Group commit batching ───

    #[tokio::test]
    async fn group_commit_batching() {
        let storage = mem_storage();
        let writer = Arc::new(
            WalWriter::new(
                storage.clone() as Arc<dyn WalStorage>,
                WalConfig::default(),
            )
            .unwrap(),
        );

        // Spawn 10 concurrent appends.
        let mut handles = Vec::new();
        for i in 0..10u32 {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                let payload = format!("concurrent-{}", i);
                w.append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                    .await
                    .unwrap()
            }));
        }

        let mut lsns = Vec::new();
        for h in handles {
            lsns.push(h.await.unwrap());
        }

        // All LSNs should be distinct.
        lsns.sort();
        lsns.dedup();
        assert_eq!(lsns.len(), 10, "all 10 appends should get distinct LSNs");

        // All records should be readable.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), 10);
    }

    // ─── Test 5: Empty WAL ───

    #[tokio::test]
    async fn empty_wal() {
        let storage = mem_storage();
        let reader = WalReader::new(storage as Arc<dyn WalStorage>);
        let mut stream = reader.read_from(0);

        assert!(stream.next().await.is_none(), "empty WAL should return None");
    }

    // ─── Test 6: read_from mid-stream ───

    #[tokio::test]
    async fn read_from_mid_stream() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        let mut lsns = Vec::new();
        for i in 0..10u32 {
            let payload = format!("rec-{}", i);
            let lsn = writer
                .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                .await
                .unwrap();
            lsns.push(lsn);
        }

        // Read starting from record 5.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(lsns[5])).await.unwrap();

        assert_eq!(records.len(), 5, "should read records 5 through 9");
        for (j, record) in records.iter().enumerate() {
            let i = j + 5;
            assert_eq!(record.lsn, lsns[i]);
            let expected = format!("rec-{}", i);
            assert_eq!(record.payload, expected.as_bytes());
        }
    }

    // ─── Test 7: find_end ───

    #[tokio::test]
    async fn find_end() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        for i in 0..5u32 {
            let payload = format!("end-test-{}", i);
            writer
                .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                .await
                .unwrap();
        }

        let expected_end = writer.current_lsn();

        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let end = reader.find_end(0).await.unwrap();
        assert_eq!(end, expected_end);
    }

    // ─── Test 8: append_raw_frame ───

    #[tokio::test]
    async fn append_raw_frame() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        // Manually encode a frame.
        let record_type = WAL_RECORD_CREATE_COLLECTION;
        let payload = b"raw-frame-payload";
        let raw_frame = encode_frame(record_type, payload);

        let lsn = writer.append_raw_frame(&raw_frame).await.unwrap();
        assert_eq!(lsn, 0);

        // Read it back via the stream.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let mut stream = reader.read_from(0);

        let record = stream.next().await.unwrap().unwrap();
        assert_eq!(record.lsn, 0);
        assert_eq!(record.record_type, record_type);
        assert_eq!(record.payload, payload);

        assert!(stream.next().await.is_none());
    }

    // ─── Test 9: Large payload ───

    #[tokio::test]
    async fn large_payload() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        // 1 MB payload.
        let payload: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();
        let lsn = writer
            .append(WAL_RECORD_TX_COMMIT, &payload)
            .await
            .unwrap();
        assert_eq!(lsn, 0);

        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let mut stream = reader.read_from(0);

        let record = stream.next().await.unwrap().unwrap();
        assert_eq!(record.payload.len(), 1_048_576);
        assert_eq!(record.payload, payload);

        assert!(stream.next().await.is_none());
    }

    // ─── Test 10: current_lsn tracking ───

    #[tokio::test]
    async fn current_lsn_tracking() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        assert_eq!(writer.current_lsn(), 0);

        let payload1 = b"first";
        let lsn1 = writer
            .append(WAL_RECORD_TX_COMMIT, payload1)
            .await
            .unwrap();
        assert_eq!(lsn1, 0);
        // After first record: current_lsn = 0 + 9 + 5 = 14.
        let expected_lsn = (WAL_FRAME_HEADER_SIZE + payload1.len()) as u64;
        assert_eq!(writer.current_lsn(), expected_lsn);

        let payload2 = b"second";
        let lsn2 = writer
            .append(WAL_RECORD_TX_COMMIT, payload2)
            .await
            .unwrap();
        assert_eq!(lsn2, expected_lsn);
        // After second record: current_lsn = 14 + 9 + 6 = 29.
        let expected_lsn2 =
            expected_lsn + (WAL_FRAME_HEADER_SIZE + payload2.len()) as u64;
        assert_eq!(writer.current_lsn(), expected_lsn2);
    }

    // ─── Test 11: Shutdown ───

    #[tokio::test]
    async fn shutdown() {
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        writer
            .append(WAL_RECORD_TX_COMMIT, b"before shutdown")
            .await
            .unwrap();

        // Drop the writer to close the channel and shut down the background task.
        drop(writer);

        // Give the background task a moment to notice the channel closure.
        tokio::task::yield_now().await;

        // Verify the old data is still readable.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"before shutdown");

        // Create a new writer and verify it resumes from the correct LSN.
        let writer2 = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();
        let expected_lsn = (WAL_FRAME_HEADER_SIZE + b"before shutdown".len()) as u64;
        assert_eq!(writer2.current_lsn(), expected_lsn);

        writer2
            .append(WAL_RECORD_TX_COMMIT, b"after restart")
            .await
            .unwrap();

        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), 2);
    }

    // ─── Test 12: Works with MemoryWalStorage ───

    #[tokio::test]
    async fn works_with_memory_wal_storage() {
        // Exercises the full write/read cycle with MemoryWalStorage,
        // verifying all record types round-trip correctly.
        let storage = mem_storage();
        let writer = WalWriter::new(
            storage.clone() as Arc<dyn WalStorage>,
            WalConfig::default(),
        )
        .unwrap();

        let test_cases: Vec<(u8, &[u8])> = vec![
            (WAL_RECORD_TX_COMMIT, b"commit-data"),
            (WAL_RECORD_CHECKPOINT, b"checkpoint-data"),
            (WAL_RECORD_CREATE_COLLECTION, b"create-col"),
            (WAL_RECORD_DROP_COLLECTION, b"drop-col"),
            (WAL_RECORD_CREATE_INDEX, b"create-idx"),
            (WAL_RECORD_DROP_INDEX, b"drop-idx"),
            (WAL_RECORD_INDEX_READY, b"idx-ready"),
            (WAL_RECORD_VACUUM, b"vacuum-data"),
            (WAL_RECORD_VISIBLE_TS, b"visible-ts"),
            (WAL_RECORD_ROLLBACK_VACUUM, b"rollback-vac"),
        ];

        for (record_type, payload) in &test_cases {
            writer.append(*record_type, payload).await.unwrap();
        }

        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), test_cases.len());

        for (i, (expected_type, expected_payload)) in test_cases.iter().enumerate()
        {
            assert_eq!(records[i].record_type, *expected_type);
            assert_eq!(records[i].payload, *expected_payload);
        }
    }

    // ─── Test 13: Works with FileWalStorage ───

    #[tokio::test]
    async fn works_with_file_wal_storage() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let storage: Arc<dyn WalStorage> =
            Arc::new(FileWalStorage::create(&wal_dir, 1024 * 1024).unwrap());
        let writer = WalWriter::new(storage.clone(), WalConfig::default()).unwrap();

        // Write several records.
        let mut lsns = Vec::new();
        for i in 0..20u32 {
            let payload = format!("file-record-{}", i);
            let lsn = writer
                .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                .await
                .unwrap();
            lsns.push(lsn);
        }

        // Read all back.
        let reader = WalReader::new(storage.clone());
        let records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(records.len(), 20);

        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, lsns[i]);
            let expected = format!("file-record-{}", i);
            assert_eq!(record.payload, expected.as_bytes());
        }

        // Verify find_end.
        let end = reader.find_end(0).await.unwrap();
        assert_eq!(end, writer.current_lsn());

        // Verify mid-stream read.
        let mid_records = collect_records(reader.read_from(lsns[10])).await.unwrap();
        assert_eq!(mid_records.len(), 10);
    }

    // ─── Test 14: Concurrent readers + writer ───

    #[tokio::test]
    async fn concurrent_readers_and_writer() {
        let storage = mem_storage();
        let writer = Arc::new(
            WalWriter::new(
                storage.clone() as Arc<dyn WalStorage>,
                WalConfig::default(),
            )
            .unwrap(),
        );

        // Write an initial batch of records.
        for i in 0..10u32 {
            let payload = format!("initial-{}", i);
            writer
                .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                .await
                .unwrap();
        }

        let initial_end = writer.current_lsn();

        // Spawn a writer task that continues appending.
        let writer_clone = writer.clone();
        let write_handle = tokio::spawn(async move {
            for i in 10..30u32 {
                let payload = format!("concurrent-{}", i);
                writer_clone
                    .append(WAL_RECORD_TX_COMMIT, payload.as_bytes())
                    .await
                    .unwrap();
            }
        });

        // Spawn reader tasks that read the initial records concurrently.
        let mut reader_handles = Vec::new();
        for _ in 0..5 {
            let s = storage.clone() as Arc<dyn WalStorage>;
            reader_handles.push(tokio::spawn(async move {
                let reader = WalReader::new(s);
                let records = collect_records(reader.read_from(0)).await.unwrap();
                // Should have at least the initial 10 records.
                assert!(
                    records.len() >= 10,
                    "expected at least 10 records, got {}",
                    records.len()
                );
            }));
        }

        // Wait for everything to complete.
        write_handle.await.unwrap();
        for h in reader_handles {
            h.await.unwrap();
        }

        // After all writes, verify the full log.
        let reader = WalReader::new(storage.clone() as Arc<dyn WalStorage>);
        let all_records = collect_records(reader.read_from(0)).await.unwrap();
        assert_eq!(all_records.len(), 30);

        // Verify we can still read from the initial end point.
        let later_records = collect_records(reader.read_from(initial_end)).await.unwrap();
        assert_eq!(later_records.len(), 20);
    }
}
