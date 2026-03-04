use std::path::Path;

use crate::types::*;
use super::record::*;
use super::segment::{WalSegment, list_segments};

/// Forward-scanning WAL reader.
pub struct WalReader {
    segments: Vec<WalSegment>,
    current_segment_idx: usize,
    read_offset: u64,
    current_lsn: Lsn,
}

impl WalReader {
    /// Open all segments in a WAL directory, starting from `start_lsn`.
    pub fn open(wal_dir: &Path, start_lsn: Lsn) -> Result<Self, WalError> {
        let paths = list_segments(wal_dir)?;
        if paths.is_empty() {
            return Ok(Self {
                segments: Vec::new(),
                current_segment_idx: 0,
                read_offset: 0,
                current_lsn: start_lsn,
            });
        }

        let mut segments: Vec<WalSegment> = Vec::new();
        for p in &paths {
            segments.push(WalSegment::open_read(p)?);
        }

        // Find the segment containing start_lsn.
        let mut seg_idx = 0;
        for (i, seg) in segments.iter().enumerate() {
            if seg.base_lsn().0 <= start_lsn.0 {
                seg_idx = i;
            } else {
                break;
            }
        }

        // Calculate file offset within the segment.
        let base_lsn = segments[seg_idx].base_lsn();
        let file_offset = WAL_SEGMENT_HEADER_SIZE as u64 + (start_lsn.0 - base_lsn.0);

        Ok(Self {
            segments,
            current_segment_idx: seg_idx,
            read_offset: file_offset,
            current_lsn: start_lsn,
        })
    }

    /// Read the next WAL record. Returns None at end-of-log.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        loop {
            if self.current_segment_idx >= self.segments.len() {
                return Ok(None);
            }

            let seg = &mut self.segments[self.current_segment_idx];

            // Check if we're past the data end.
            if self.read_offset + WAL_FRAME_HEADER_SIZE as u64 > seg.data_end_offset() {
                // Try next segment.
                if self.current_segment_idx + 1 < self.segments.len() {
                    self.current_segment_idx += 1;
                    self.read_offset = WAL_SEGMENT_HEADER_SIZE as u64;
                    continue;
                }
                return Ok(None);
            }

            // Read frame header (9 bytes).
            let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
            let n = seg.read_at(self.read_offset, &mut header_buf)?;
            if n < WAL_FRAME_HEADER_SIZE {
                return Ok(None);
            }

            let payload_len = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());

            // End-of-data detection.
            if payload_len == 0 {
                if self.current_segment_idx + 1 < self.segments.len() {
                    self.current_segment_idx += 1;
                    self.read_offset = WAL_SEGMENT_HEADER_SIZE as u64;
                    continue;
                }
                return Ok(None);
            }

            let stored_crc = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
            let record_type_byte = header_buf[8];

            // Read payload.
            let mut payload_buf = vec![0u8; payload_len as usize];
            let n = seg.read_at(self.read_offset + WAL_FRAME_HEADER_SIZE as u64, &mut payload_buf)?;
            if n < payload_len as usize {
                return Err(WalError::TruncatedRecord(self.current_lsn));
            }

            // Verify CRC.
            let mut crc_data = vec![record_type_byte];
            crc_data.extend_from_slice(&payload_buf);
            let computed_crc = crc32c::crc32c(&crc_data);
            if computed_crc != stored_crc {
                return Err(WalError::CrcMismatch {
                    lsn: self.current_lsn,
                    expected: stored_crc,
                    computed: computed_crc,
                });
            }

            let record_type = WalRecordType::from_u8(record_type_byte)
                .ok_or(WalError::InvalidRecordType(record_type_byte))?;

            let payload = deserialize_payload(record_type, &payload_buf)?;

            let record_lsn = self.current_lsn;
            let frame_size = WAL_FRAME_HEADER_SIZE as u64 + payload_len as u64;
            self.read_offset += frame_size;
            self.current_lsn = self.current_lsn.advance(frame_size);

            return Ok(Some(WalRecord {
                lsn: record_lsn,
                record_type,
                payload,
            }));
        }
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let writer = WalWriter::new(&wal_dir, WalConfig::default()).unwrap();
        let sender = writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        tokio::spawn(async move { writer.run(shutdown2).await });

        // Write a few records.
        for i in 0..5u64 {
            let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(i * 100) });
            let frame = serialize_payload(WalRecordType::Checkpoint, &payload);
            WalWriter::write(&sender, frame).await.unwrap();
        }

        shutdown.cancel();
        // Small delay to let writer shut down.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Read back.
        let mut reader = WalReader::open(&wal_dir, Lsn::ZERO).unwrap();
        let mut count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            assert_eq!(record.record_type, WalRecordType::Checkpoint);
            if let WalPayload::Checkpoint(cp) = &record.payload {
                assert_eq!(cp.checkpoint_lsn, Lsn(count * 100));
            }
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_crc_mismatch_detection() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Manually write a corrupt frame.
        std::fs::create_dir_all(&wal_dir).unwrap();
        let mut seg = crate::wal::segment::WalSegment::create(&wal_dir, 1, Lsn::ZERO, 1024).unwrap();
        let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(0) });
        let mut frame = serialize_payload(WalRecordType::Checkpoint, &payload);
        // Corrupt the payload.
        if frame.len() > 10 {
            frame[10] ^= 0xFF;
        }
        seg.append(&frame).unwrap();
        seg.sync().unwrap();

        let mut reader = WalReader::open(&wal_dir, Lsn::ZERO).unwrap();
        let result = reader.next_record();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cross_segment_read() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let config = WalConfig {
            target_segment_size: 128, // tiny
            channel_capacity: 64,
        };
        let writer = WalWriter::new(&wal_dir, config).unwrap();
        let sender = writer.sender();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        tokio::spawn(async move { writer.run(shutdown2).await });

        let mut expected_lsns = Vec::new();
        for _ in 0..20 {
            let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(0) });
            let frame = serialize_payload(WalRecordType::Checkpoint, &payload);
            let lsn = WalWriter::write(&sender, frame).await.unwrap();
            expected_lsns.push(lsn);
        }

        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Read all back across segments.
        let mut reader = WalReader::open(&wal_dir, Lsn::ZERO).unwrap();
        let mut read_lsns = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            read_lsns.push(record.lsn);
        }
        assert_eq!(read_lsns, expected_lsns);
    }
}
