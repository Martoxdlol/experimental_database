use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::types::*;
use super::segment::WalSegment;
use super::record::WalError;

/// A request to write one WAL record.
pub struct WriteRequest {
    pub record_bytes: Vec<u8>,
    pub response_tx: oneshot::Sender<Result<Lsn, WalError>>,
}

/// WAL writer configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    pub target_segment_size: u64,
    pub channel_capacity: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            target_segment_size: 64 * 1024 * 1024,
            channel_capacity: 1024,
        }
    }
}

/// WAL writer task — drains write queue, batches, fsyncs.
pub struct WalWriter {
    write_rx: mpsc::Receiver<WriteRequest>,
    write_tx: mpsc::Sender<WriteRequest>,
    active_segment: WalSegment,
    wal_dir: PathBuf,
    next_segment_id: u32,
    target_segment_size: u64,
    current_lsn: Lsn,
}

impl WalWriter {
    /// Create a new WAL writer. Creates or opens the active segment.
    pub fn new(wal_dir: &Path, config: WalConfig) -> Result<Self, WalError> {
        std::fs::create_dir_all(wal_dir)?;

        let segments = super::segment::list_segments(wal_dir)?;
        let (active_segment, next_segment_id, current_lsn) = if let Some(last_path) = segments.last() {
            let seg = WalSegment::open_append(last_path)?;
            let next_id = seg.segment_id() + 1;
            let lsn = seg.current_lsn();
            (seg, next_id, lsn)
        } else {
            let seg = WalSegment::create(wal_dir, 1, Lsn::ZERO, config.target_segment_size)?;
            (seg, 2, Lsn::ZERO)
        };

        let (write_tx, write_rx) = mpsc::channel(config.channel_capacity);

        Ok(Self {
            write_rx,
            write_tx,
            active_segment,
            wal_dir: wal_dir.to_path_buf(),
            next_segment_id,
            target_segment_size: config.target_segment_size,
            current_lsn,
        })
    }

    /// Get a sender handle for submitting write requests.
    pub fn sender(&self) -> mpsc::Sender<WriteRequest> {
        self.write_tx.clone()
    }

    /// Current LSN.
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    /// Run the writer loop.
    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                req = self.write_rx.recv() => {
                    let Some(first) = req else { break };
                    let mut batch = vec![first];
                    // Greedy drain.
                    while let Ok(r) = self.write_rx.try_recv() {
                        batch.push(r);
                    }
                    self.process_batch(batch);
                }
            }
        }
    }

    fn process_batch(&mut self, batch: Vec<WriteRequest>) {
        let mut assigned: Vec<(oneshot::Sender<Result<Lsn, WalError>>, Lsn)> = Vec::with_capacity(batch.len());

        for req in batch {
            let lsn = self.current_lsn;
            if let Err(e) = self.active_segment.append(&req.record_bytes) {
                let _ = req.response_tx.send(Err(e));
                continue;
            }
            self.current_lsn = self.current_lsn.advance(req.record_bytes.len() as u64);
            assigned.push((req.response_tx, lsn));
        }

        // One fsync for the entire batch.
        let sync_result = self.active_segment.sync();

        for (tx, lsn) in assigned {
            let _ = tx.send(match &sync_result {
                Ok(()) => Ok(lsn),
                Err(_) => Err(WalError::Io(std::io::Error::other("fsync failed"))),
            });
        }

        // Check for rollover.
        if self.active_segment.should_rollover(self.target_segment_size) {
            let _ = self.rollover();
        }
    }

    fn rollover(&mut self) -> Result<(), WalError> {
        let new_seg = WalSegment::create(
            &self.wal_dir,
            self.next_segment_id,
            self.current_lsn,
            self.target_segment_size,
        )?;
        self.active_segment = new_seg;
        self.next_segment_id += 1;
        Ok(())
    }

    /// Submit a write and wait for the assigned LSN.
    pub async fn write(
        sender: &mpsc::Sender<WriteRequest>,
        record_bytes: Vec<u8>,
    ) -> Result<Lsn, WalError> {
        let (tx, rx) = oneshot::channel();
        sender
            .send(WriteRequest {
                record_bytes,
                response_tx: tx,
            })
            .await
            .map_err(|_| WalError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL writer channel closed",
            )))?;
        rx.await.map_err(|_| WalError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "WAL writer response dropped",
        )))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::*;

    #[tokio::test]
    async fn test_single_write() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let writer = WalWriter::new(&wal_dir, WalConfig::default()).unwrap();
        let sender = writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        tokio::spawn(async move { writer.run(shutdown2).await });

        let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(0) });
        let frame = serialize_payload(WalRecordType::Checkpoint, &payload);
        let lsn = WalWriter::write(&sender, frame).await.unwrap();
        assert_eq!(lsn, Lsn::ZERO);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let writer = WalWriter::new(&wal_dir, WalConfig::default()).unwrap();
        let sender = writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        tokio::spawn(async move { writer.run(shutdown2).await });

        let mut handles = Vec::new();
        for _ in 0..10 {
            let s = sender.clone();
            handles.push(tokio::spawn(async move {
                let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(0) });
                let frame = serialize_payload(WalRecordType::Checkpoint, &payload);
                WalWriter::write(&s, frame).await.unwrap()
            }));
        }

        let mut lsns: Vec<Lsn> = Vec::new();
        for h in handles {
            lsns.push(h.await.unwrap());
        }
        // All LSNs should be unique.
        lsns.sort();
        lsns.dedup();
        assert_eq!(lsns.len(), 10);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_rollover() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalConfig {
            target_segment_size: 128, // tiny segment to trigger rollover
            channel_capacity: 64,
        };
        let writer = WalWriter::new(&wal_dir, config).unwrap();
        let sender = writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        tokio::spawn(async move { writer.run(shutdown2).await });

        // Write enough to trigger rollover.
        for _ in 0..20 {
            let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(0) });
            let frame = serialize_payload(WalRecordType::Checkpoint, &payload);
            WalWriter::write(&sender, frame).await.unwrap();
        }

        shutdown.cancel();

        // Check multiple segments were created.
        let segs = super::super::segment::list_segments(&wal_dir).unwrap();
        assert!(segs.len() > 1, "expected multiple segments, got {}", segs.len());
    }
}
