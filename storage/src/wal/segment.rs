use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::types::*;
use super::record::WalError;

/// Segment header: 32 bytes at file offset 0.
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u16,
    pub _reserved: u16,
    pub segment_id: u32,
    pub base_lsn: Lsn,
    pub created_at_ms: u64,
}

impl SegmentHeader {
    pub fn serialize(&self) -> [u8; WAL_SEGMENT_HEADER_SIZE] {
        let mut buf = [0u8; WAL_SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self._reserved.to_le_bytes());
        buf[8..12].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.base_lsn.0.to_le_bytes());
        buf[20..28].copy_from_slice(&self.created_at_ms.to_le_bytes());
        // bytes 28..32 reserved/zero
        buf
    }

    pub fn deserialize(buf: &[u8; WAL_SEGMENT_HEADER_SIZE]) -> Result<Self, WalError> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(WalError::InvalidSegmentHeader(
                format!("bad magic: {magic:#010x}, expected {WAL_MAGIC:#010x}")
            ));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if version != 1 {
            return Err(WalError::InvalidSegmentHeader(format!("unsupported version: {version}")));
        }
        Ok(Self {
            magic,
            version,
            _reserved: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            segment_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            base_lsn: Lsn(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
            created_at_ms: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        })
    }
}

/// Manages a single WAL segment file.
pub struct WalSegment {
    file: File,
    header: SegmentHeader,
    write_offset: u64,
    path: PathBuf,
}

impl WalSegment {
    /// Create a new segment file.
    pub fn create(
        dir: &Path,
        segment_id: u32,
        base_lsn: Lsn,
        target_size: u64,
    ) -> Result<Self, WalError> {
        let filename = segment_filename(segment_id);
        let path = dir.join(&filename);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let header = SegmentHeader {
            magic: WAL_MAGIC,
            version: 1,
            _reserved: 0,
            segment_id,
            base_lsn,
            created_at_ms: now_ms,
        };

        file.write_all(&header.serialize())?;

        // Pre-allocate.
        if target_size > WAL_SEGMENT_HEADER_SIZE as u64 {
            file.set_len(target_size)?;
        }

        Ok(Self {
            file,
            header,
            write_offset: WAL_SEGMENT_HEADER_SIZE as u64,
            path,
        })
    }

    /// Open an existing segment for reading.
    pub fn open_read(path: &Path) -> Result<Self, WalError> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut hdr_buf = [0u8; WAL_SEGMENT_HEADER_SIZE];
        file.read_exact(&mut hdr_buf)?;
        let header = SegmentHeader::deserialize(&hdr_buf)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            file,
            header,
            write_offset: file_len,
            path: path.to_path_buf(),
        })
    }

    /// Open the active segment for appending.
    pub fn open_append(path: &Path) -> Result<Self, WalError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut hdr_buf = [0u8; WAL_SEGMENT_HEADER_SIZE];
        file.read_exact(&mut hdr_buf)?;
        let header = SegmentHeader::deserialize(&hdr_buf)?;

        // Find the actual write position by scanning for the first zero payload_len.
        let mut offset = WAL_SEGMENT_HEADER_SIZE as u64;
        let file_len = file.metadata()?.len();
        loop {
            if offset + WAL_FRAME_HEADER_SIZE as u64 > file_len {
                break;
            }
            file.seek(SeekFrom::Start(offset))?;
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len_buf);
            if payload_len == 0 {
                break;
            }
            offset += WAL_FRAME_HEADER_SIZE as u64 + payload_len as u64;
        }

        Ok(Self {
            file,
            header,
            write_offset: offset,
            path: path.to_path_buf(),
        })
    }

    /// Append raw bytes. No fsync.
    pub fn append(&mut self, data: &[u8]) -> Result<(), WalError> {
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        self.file.write_all(data)?;
        self.write_offset += data.len() as u64;
        Ok(())
    }

    /// fsync the segment file.
    pub fn sync(&self) -> Result<(), WalError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Current write position as LSN.
    pub fn current_lsn(&self) -> Lsn {
        let data_written = self.write_offset - WAL_SEGMENT_HEADER_SIZE as u64;
        Lsn(self.header.base_lsn.0 + data_written)
    }

    /// Whether the segment has exceeded the target size.
    pub fn should_rollover(&self, target_size: u64) -> bool {
        self.write_offset >= target_size
    }

    pub fn segment_id(&self) -> u32 {
        self.header.segment_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn base_lsn(&self) -> Lsn {
        self.header.base_lsn
    }

    /// Read data at a specific offset within the segment file.
    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, WalError> {
        self.file.seek(SeekFrom::Start(offset))?;
        let n = self.file.read(buf)?;
        Ok(n)
    }

    pub fn data_end_offset(&self) -> u64 {
        self.write_offset
    }
}

/// Segment filename: segment-{id:06}.wal
pub fn segment_filename(segment_id: u32) -> String {
    format!("segment-{segment_id:06}.wal")
}

/// Scan a WAL directory and return segment paths ordered by filename.
pub fn list_segments(dir: &Path) -> Result<Vec<PathBuf>, WalError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut segments: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_str()?.to_string();
            if name.starts_with("segment-") && name.ends_with(".wal") {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect();
    segments.sort();
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_header_roundtrip() {
        let header = SegmentHeader {
            magic: WAL_MAGIC,
            version: 1,
            _reserved: 0,
            segment_id: 5,
            base_lsn: Lsn(1000),
            created_at_ms: 1234567890,
        };
        let buf = header.serialize();
        let decoded = SegmentHeader::deserialize(&buf).unwrap();
        assert_eq!(decoded.magic, WAL_MAGIC);
        assert_eq!(decoded.segment_id, 5);
        assert_eq!(decoded.base_lsn, Lsn(1000));
    }

    #[test]
    fn test_segment_filename() {
        assert_eq!(segment_filename(1), "segment-000001.wal");
        assert_eq!(segment_filename(42), "segment-000042.wal");
    }

    #[test]
    fn test_create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 1, Lsn::ZERO, 1024).unwrap();
        assert_eq!(seg.current_lsn(), Lsn::ZERO);
        seg.append(b"hello world").unwrap();
        assert_eq!(seg.current_lsn(), Lsn(11));
        seg.sync().unwrap();
    }

    #[test]
    fn test_open_append_finds_position() {
        let dir = tempfile::tempdir().unwrap();
        let path;
        {
            let mut seg = WalSegment::create(dir.path(), 1, Lsn::ZERO, 1024).unwrap();
            // Write a fake WAL frame: payload_len=5, crc=0, type=0, payload=5 bytes.
            let mut frame = Vec::new();
            frame.extend_from_slice(&5u32.to_le_bytes()); // payload_len
            frame.extend_from_slice(&0u32.to_le_bytes()); // crc
            frame.push(0x01); // record_type
            frame.extend_from_slice(b"hello"); // payload
            seg.append(&frame).unwrap();
            seg.sync().unwrap();
            path = seg.path().to_path_buf();
        }
        let seg2 = WalSegment::open_append(&path).unwrap();
        // Expected: header(32) + frame(9+5=14) = 46.
        assert_eq!(seg2.write_offset, 46);
    }

    #[test]
    fn test_list_segments() {
        let dir = tempfile::tempdir().unwrap();
        WalSegment::create(dir.path(), 1, Lsn::ZERO, 256).unwrap();
        WalSegment::create(dir.path(), 2, Lsn(100), 256).unwrap();
        let segs = list_segments(dir.path()).unwrap();
        assert_eq!(segs.len(), 2);
    }
}
