//! Backend traits and implementations for page and WAL I/O.
//!
//! This module provides the [`PageStorage`] and [`WalStorage`] traits that abstract
//! physical I/O, along with file-backed (durable) and in-memory (ephemeral)
//! implementations. Custom backends (S3, etc.) can plug in via the same traits.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use parking_lot::{Mutex, RwLock};

// ─── Type Aliases ───

/// A page identifier. Pages are numbered from 0.
pub type PageId = u32;

// ─── Page Storage Trait ───

/// Backend for fixed-size page I/O.
/// Implementations must be thread-safe (Send + Sync).
pub trait PageStorage: Send + Sync {
    /// Read page `page_id` into `buf`. `buf.len()` must equal `page_size`.
    /// Returns an error if `page_id >= page_count`.
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()>;

    /// Write `buf` to page `page_id`. `buf.len()` must equal `page_size`.
    /// Returns an error if `page_id >= page_count`.
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> io::Result<()>;

    /// Flush all pending writes to durable storage.
    /// No-op for in-memory backends.
    fn sync(&self) -> io::Result<()>;

    /// Current number of allocated pages.
    fn page_count(&self) -> u64;

    /// Extend the store to at least `new_count` pages.
    /// Newly allocated pages are zero-filled.
    fn extend(&self, new_count: u64) -> io::Result<()>;

    /// Returns the page size this backend was configured with.
    fn page_size(&self) -> usize;

    /// Whether this backend provides durable storage.
    fn is_durable(&self) -> bool;
}

// ─── WAL Storage Trait ───

/// Backend for append-only WAL I/O.
pub trait WalStorage: Send + Sync {
    /// Append `data` to the log. Returns the byte offset where data was written.
    fn append(&self, data: &[u8]) -> io::Result<u64>;

    /// Flush all pending appends to durable storage.
    fn sync(&self) -> io::Result<()>;

    /// Read up to `buf.len()` bytes starting at `offset`.
    /// Returns number of bytes actually read.
    fn read_from(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Discard all data before `offset` (segment reclamation).
    ///
    /// **WAL Retention Policy**: This trait does not enforce retention semantics.
    /// Callers (StorageEngine, replication layer) are responsible for computing
    /// the safe truncation point based on:
    /// - Oldest active transaction (for MVCC recovery)
    /// - Replication lag (for followers to catch up)
    /// - Snapshot checkpoints (for incremental restore)
    ///
    /// Do not call this with an offset that would discard data needed for recovery.
    fn truncate_before(&self, offset: u64) -> io::Result<()>;

    /// Returns the LSN of the oldest retained WAL segment.
    ///
    /// Used by retention policy logic to check if truncation is necessary.
    /// Returns `None` if no segments exist.
    fn oldest_lsn(&self) -> Option<u64>;

    /// Returns the total size of all retained WAL segments in bytes.
    ///
    /// Used by retention policy to monitor WAL growth and decide when to truncate.
    fn retained_size(&self) -> u64;

    /// Total bytes written so far (logical end offset).
    fn size(&self) -> u64;

    /// Whether this backend provides durable storage.
    fn is_durable(&self) -> bool;
}

// ═══════════════════════════════════════════════════════════════════════
// File-Backed Implementations
// ═══════════════════════════════════════════════════════════════════════

// ─── FilePageStorage ───

/// Durable page storage backed by a single data file.
/// Uses pread/pwrite (`read_at`/`write_at`) for concurrent positional I/O.
pub struct FilePageStorage {
    file: File,
    page_size: usize,
    page_count: AtomicU64,
}

impl FilePageStorage {
    /// Open an existing data file. Computes `page_count = file_size / page_size`.
    pub fn open(path: &Path, page_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let page_count = file_size / page_size as u64;
        Ok(Self {
            file,
            page_size,
            page_count: AtomicU64::new(page_count),
        })
    }

    /// Create a new data file. Sets `page_count = 0`.
    pub fn create(path: &Path, page_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file,
            page_size,
            page_count: AtomicU64::new(0),
        })
    }
}

impl PageStorage for FilePageStorage {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()> {
        if page_id as u64 >= self.page_count.load(Ordering::Acquire) {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_id {} out of range (page_count {})",
                page_id,
                self.page_count.load(Ordering::Acquire)
            )).into());
        }
        let offset = page_id as u64 * self.page_size as u64;
        self.file.read_at(buf, offset)?;
        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> io::Result<()> {
        if page_id as u64 >= self.page_count.load(Ordering::Acquire) {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_id {} out of range (page_count {})",
                page_id,
                self.page_count.load(Ordering::Acquire)
            )).into());
        }
        let offset = page_id as u64 * self.page_size as u64;
        self.file.write_at(buf, offset)?;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn page_count(&self) -> u64 {
        self.page_count.load(Ordering::Acquire)
    }

    fn extend(&self, new_count: u64) -> io::Result<()> {
        let current = self.page_count.load(Ordering::Acquire);
        if new_count <= current {
            return Ok(());
        }
        let new_len = new_count * self.page_size as u64;
        self.file.set_len(new_len)?;
        self.file.sync_data()?;
        self.page_count.store(new_count, Ordering::Release);
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn is_durable(&self) -> bool {
        true
    }
}

// ─── FileWalStorage ───

/// Magic bytes for WAL segment header validation.
const WAL_SEGMENT_MAGIC: [u8; 4] = *b"WALS";

/// Size of the segment header in bytes.
/// Layout (32 bytes):
///   [0..4]   magic "WALS"
///   [4..8]   version (u32 LE) = 1
///   [8..16]  base_lsn (u64 LE)
///   [16..20] segment_id (u32 LE)
///   [20..32] reserved (zero)
const SEGMENT_HEADER_SIZE: u64 = 32;

/// Metadata about a single WAL segment file.
struct SegmentInfo {
    #[allow(dead_code)]
    segment_id: u32,
    base_lsn: u64,
    file_size: u64,
    path: PathBuf,
}

/// Mutable inner state of [`FileWalStorage`], protected by a mutex.
struct FileWalInner {
    active_segment: File,
    active_segment_id: u32,
    #[allow(dead_code)]
    active_base_lsn: u64,
    /// Current logical write position (bytes written so far across all segments).
    write_offset: u64,
    /// All segments ordered by base_lsn.
    segments: Vec<SegmentInfo>,
}

/// Durable WAL storage backed by segment files in a directory.
///
/// Each segment file has a 32-byte header followed by raw WAL data. Segments
/// are named `segment-NNNNNN.wal` where NNNNNN is the segment id (zero-padded).
pub struct FileWalStorage {
    dir: PathBuf,
    segment_size: usize,
    inner: Mutex<FileWalInner>,
}

impl FileWalStorage {
    /// Open an existing WAL directory. Scans for `segment-*.wal` files, parses
    /// headers, and resumes appending to the latest segment.
    pub fn open(dir: &Path, segment_size: usize) -> io::Result<Self> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with("segment-") && name.ends_with(".wal") {
                    let seg_id_str = &name["segment-".len()..name.len() - ".wal".len()];
                    let segment_id: u32 = seg_id_str.parse().map_err(|_| {
                        io::Error::from(crate::error::StorageError::Corruption(
                            format!("bad segment name: {}", name),
                        ))
                    })?;
                    let metadata = fs::metadata(&path)?;
                    let file_size = metadata.len();

                    // Read and validate header
                    let base_lsn = Self::read_segment_header(&path)?;

                    segments.push(SegmentInfo {
                        segment_id,
                        base_lsn,
                        file_size,
                        path,
                    });
                }
        }

        if segments.is_empty() {
            return Err(crate::error::StorageError::Corruption(
                "no WAL segments found in directory".into()
            ).into());
        }

        // Sort by base_lsn
        segments.sort_by_key(|s| s.base_lsn);

        // Open the latest segment for appending
        let last = segments.last().unwrap();
        let active_segment = OpenOptions::new().read(true).append(true).open(&last.path)?;
        let active_segment_id = last.segment_id;
        let active_base_lsn = last.base_lsn;

        // write_offset = base_lsn of last segment + (file_size - header)
        let write_offset = last.base_lsn + (last.file_size - SEGMENT_HEADER_SIZE);

        Ok(Self {
            dir: dir.to_path_buf(),
            segment_size,
            inner: Mutex::new(FileWalInner {
                active_segment,
                active_segment_id,
                active_base_lsn,
                write_offset,
                segments,
            }),
        })
    }

    /// Create a new WAL directory with an initial empty segment.
    pub fn create(dir: &Path, segment_size: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let segment_id = 1u32;
        let base_lsn = 0u64;
        let path = dir.join(Self::segment_filename(segment_id));

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Self::write_segment_header(&mut file, segment_id, base_lsn)?;
        file.sync_data()?;

        let file_size = SEGMENT_HEADER_SIZE;

        let segments = vec![SegmentInfo {
            segment_id,
            base_lsn,
            file_size,
            path: path.clone(),
        }];

        // Re-open in append mode for writing
        let active_segment = OpenOptions::new().read(true).append(true).open(&path)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            segment_size,
            inner: Mutex::new(FileWalInner {
                active_segment,
                active_segment_id: segment_id,
                active_base_lsn: base_lsn,
                write_offset: 0,
                segments,
            }),
        })
    }

    /// Generate the filename for a segment given its id.
    fn segment_filename(segment_id: u32) -> String {
        format!("segment-{:06}.wal", segment_id)
    }

    /// Write a 32-byte segment header.
    fn write_segment_header(file: &mut File, segment_id: u32, base_lsn: u64) -> io::Result<()> {
        let mut header = [0u8; SEGMENT_HEADER_SIZE as usize];
        header[0..4].copy_from_slice(&WAL_SEGMENT_MAGIC);
        header[4..8].copy_from_slice(&1u32.to_le_bytes()); // version = 1
        header[8..16].copy_from_slice(&base_lsn.to_le_bytes());
        header[16..20].copy_from_slice(&segment_id.to_le_bytes());
        // [20..32] reserved, already zero
        file.write_all(&header)?;
        Ok(())
    }

    /// Read and validate a segment header, returning the base_lsn.
    fn read_segment_header(path: &Path) -> io::Result<u64> {
        let mut file = File::open(path)?;
        let mut header = [0u8; SEGMENT_HEADER_SIZE as usize];
        file.read_exact(&mut header)?;

        if header[0..4] != WAL_SEGMENT_MAGIC {
            return Err(crate::error::StorageError::Corruption(
                format!("corrupt WAL segment header (bad magic) in {:?}", path),
            ).into());
        }

        let version = crate::util::read_u32_le(&header, 4)?;
        if version != 1 {
            return Err(crate::error::StorageError::Corruption(format!(
                "unsupported WAL segment version {} in {:?}",
                version, path
            )).into());
        }

        let base_lsn = crate::util::read_u64_le(&header, 8)?;
        Ok(base_lsn)
    }

    /// Roll over to a new segment file. Must be called with the inner lock held.
    fn rollover(inner: &mut FileWalInner, dir: &Path) -> io::Result<()> {
        let new_segment_id = inner.active_segment_id + 1;
        let new_base_lsn = inner.write_offset;
        let path = dir.join(Self::segment_filename(new_segment_id));

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        Self::write_segment_header(&mut file, new_segment_id, new_base_lsn)?;
        file.sync_data()?;

        let file_size = SEGMENT_HEADER_SIZE;

        inner.segments.push(SegmentInfo {
            segment_id: new_segment_id,
            base_lsn: new_base_lsn,
            file_size,
            path: path.clone(),
        });

        // Re-open in append mode
        let active = OpenOptions::new().read(true).append(true).open(&path)?;

        inner.active_segment = active;
        inner.active_segment_id = new_segment_id;
        inner.active_base_lsn = new_base_lsn;

        Ok(())
    }
}

impl WalStorage for FileWalStorage {
    fn append(&self, data: &[u8]) -> io::Result<u64> {
        let mut inner = self.inner.lock();

        let offset = inner.write_offset;

        // Write to active segment
        inner.active_segment.write_all(data)?;
        inner.write_offset += data.len() as u64;

        // Update the file_size in the segment info for the active segment
        if let Some(seg) = inner.segments.last_mut() {
            seg.file_size += data.len() as u64;
        }

        // Check if we need to roll over
        if let Some(seg) = inner.segments.last()
            && seg.file_size > self.segment_size as u64 {
                Self::rollover(&mut inner, &self.dir)?;
            }

        Ok(offset)
    }

    fn sync(&self) -> io::Result<()> {
        let inner = self.inner.lock();
        inner.active_segment.sync_data()
    }

    fn read_from(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let inner = self.inner.lock();

        if buf.is_empty() || offset >= inner.write_offset {
            return Ok(0);
        }

        let mut total_read = 0usize;
        let mut current_offset = offset;
        let end_offset = std::cmp::min(offset + buf.len() as u64, inner.write_offset);

        while current_offset < end_offset && total_read < buf.len() {
            // Find the segment containing current_offset via binary search.
            // We want the last segment whose base_lsn <= current_offset.
            let seg_idx =
                match inner
                    .segments
                    .binary_search_by_key(&current_offset, |s| s.base_lsn)
                {
                    Ok(i) => i,
                    Err(i) => {
                        if i == 0 {
                            // current_offset is before the first segment
                            return Ok(total_read);
                        }
                        i - 1
                    }
                };

            let seg = &inner.segments[seg_idx];

            // File position within this segment
            let offset_in_segment = current_offset - seg.base_lsn;
            let file_offset = SEGMENT_HEADER_SIZE + offset_in_segment;

            // How many data bytes are available in this segment
            let data_in_segment = seg.file_size - SEGMENT_HEADER_SIZE;
            let remaining_in_segment = data_in_segment.saturating_sub(offset_in_segment);

            if remaining_in_segment == 0 {
                break;
            }

            // How many bytes to read from this segment
            let want = std::cmp::min(
                (end_offset - current_offset) as usize,
                remaining_in_segment as usize,
            );
            let want = std::cmp::min(want, buf.len() - total_read);

            // Open the segment file for reading and use pread
            let file = File::open(&seg.path)?;
            let n = file.read_at(&mut buf[total_read..total_read + want], file_offset)?;

            total_read += n;
            current_offset += n as u64;

            if n == 0 {
                break;
            }
        }

        Ok(total_read)
    }

    fn truncate_before(&self, offset: u64) -> io::Result<()> {
        let mut inner = self.inner.lock();

        // Delete segments whose highest LSN is below the given offset.
        // A segment's "highest LSN" is base_lsn + (file_size - header_size).
        // We retain a segment if its data range overlaps with [offset, ...).
        let mut to_remove = Vec::new();

        for (i, seg) in inner.segments.iter().enumerate() {
            let seg_end_lsn = seg.base_lsn + (seg.file_size - SEGMENT_HEADER_SIZE);
            if seg_end_lsn <= offset {
                to_remove.push(i);
            }
        }

        // Remove in reverse order to preserve indices, and delete files.
        for &i in to_remove.iter().rev() {
            let seg = inner.segments.remove(i);
            let _ = fs::remove_file(&seg.path);
        }

        Ok(())
    }

    fn oldest_lsn(&self) -> Option<u64> {
        let inner = self.inner.lock();
        inner.segments.first().map(|s| s.base_lsn)
    }

    fn retained_size(&self) -> u64 {
        let inner = self.inner.lock();
        inner.segments.iter().map(|s| s.file_size).sum()
    }

    fn size(&self) -> u64 {
        let inner = self.inner.lock();
        inner.write_offset
    }

    fn is_durable(&self) -> bool {
        true
    }
}

// ═══════════════════════════════════════════════════════════════════════
// In-Memory Implementations
// ═══════════════════════════════════════════════════════════════════════

// ─── MemoryPageStorage ───

/// Ephemeral page storage. Data lives only in RAM.
pub struct MemoryPageStorage {
    pages: RwLock<Vec<Vec<u8>>>,
    page_size: usize,
}

impl MemoryPageStorage {
    /// Create a new in-memory page store with the given page size.
    pub fn new(page_size: usize) -> Self {
        Self {
            pages: RwLock::new(Vec::new()),
            page_size,
        }
    }
}

impl PageStorage for MemoryPageStorage {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()> {
        let pages = self.pages.read();
        if page_id as usize >= pages.len() {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_id {} out of range (page_count {})",
                page_id,
                pages.len()
            )).into());
        }
        buf.copy_from_slice(&pages[page_id as usize]);
        Ok(())
    }

    fn write_page(&self, page_id: PageId, buf: &[u8]) -> io::Result<()> {
        let mut pages = self.pages.write();
        if page_id as usize >= pages.len() {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_id {} out of range (page_count {})",
                page_id,
                pages.len()
            )).into());
        }
        pages[page_id as usize].copy_from_slice(buf);
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    fn page_count(&self) -> u64 {
        self.pages.read().len() as u64
    }

    fn extend(&self, new_count: u64) -> io::Result<()> {
        let mut pages = self.pages.write();
        let current = pages.len() as u64;
        if new_count <= current {
            return Ok(());
        }
        for _ in current..new_count {
            pages.push(vec![0u8; self.page_size]);
        }
        Ok(())
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn is_durable(&self) -> bool {
        false
    }
}

// ─── MemoryWalStorage ───

/// Ephemeral WAL storage. Append-only byte vector with truncation support.
pub struct MemoryWalStorage {
    inner: RwLock<MemoryWalInner>,
}

/// Internal state for MemoryWalStorage, tracking both data and base offset.
struct MemoryWalInner {
    log: Vec<u8>,
    /// The logical offset corresponding to log[0]. Advances when truncate_before
    /// is called, so that old offsets continue to map correctly.
    base_offset: u64,
}

impl MemoryWalStorage {
    /// Create a new empty in-memory WAL store.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(MemoryWalInner {
                log: Vec::new(),
                base_offset: 0,
            }),
        }
    }
}

impl Default for MemoryWalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl WalStorage for MemoryWalStorage {
    fn append(&self, data: &[u8]) -> io::Result<u64> {
        let mut inner = self.inner.write();
        let offset = inner.base_offset + inner.log.len() as u64;
        inner.log.extend_from_slice(data);
        Ok(offset)
    }

    fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    fn read_from(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let inner = self.inner.read();

        if buf.is_empty() {
            return Ok(0);
        }

        let end_offset = inner.base_offset + inner.log.len() as u64;

        if offset >= end_offset || offset < inner.base_offset {
            return Ok(0);
        }

        let local_start = (offset - inner.base_offset) as usize;
        let available = inner.log.len() - local_start;
        let to_read = std::cmp::min(available, buf.len());

        buf[..to_read].copy_from_slice(&inner.log[local_start..local_start + to_read]);
        Ok(to_read)
    }

    fn truncate_before(&self, offset: u64) -> io::Result<()> {
        let mut inner = self.inner.write();

        if offset <= inner.base_offset {
            return Ok(());
        }

        let end_offset = inner.base_offset + inner.log.len() as u64;

        if offset >= end_offset {
            // Truncate everything
            inner.log.clear();
            inner.base_offset = end_offset;
            return Ok(());
        }

        let split_point = (offset - inner.base_offset) as usize;
        inner.log.drain(..split_point);
        inner.base_offset = offset;

        Ok(())
    }

    fn oldest_lsn(&self) -> Option<u64> {
        let inner = self.inner.read();
        if inner.log.is_empty() && inner.base_offset == 0 {
            None
        } else {
            Some(inner.base_offset)
        }
    }

    fn retained_size(&self) -> u64 {
        let inner = self.inner.read();
        inner.log.len() as u64
    }

    fn size(&self) -> u64 {
        let inner = self.inner.read();
        inner.base_offset + inner.log.len() as u64
    }

    fn is_durable(&self) -> bool {
        false
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const PAGE_SIZE: usize = 4096;

    // ─── Test 1: FilePageStorage roundtrip ───

    #[test]
    fn file_page_storage_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.db");

        // Create and write
        {
            let storage = FilePageStorage::create(&path, PAGE_SIZE).unwrap();
            storage.extend(4).unwrap();

            for i in 0..4u32 {
                let mut page = vec![0u8; PAGE_SIZE];
                // Fill with a known pattern
                for (j, byte) in page.iter_mut().enumerate() {
                    *byte = ((i as usize + j) % 256) as u8;
                }
                storage.write_page(i, &page).unwrap();
            }
            storage.sync().unwrap();
        }

        // Reopen and read back
        {
            let storage = FilePageStorage::open(&path, PAGE_SIZE).unwrap();
            assert_eq!(storage.page_count(), 4);

            for i in 0..4u32 {
                let mut buf = vec![0u8; PAGE_SIZE];
                storage.read_page(i, &mut buf).unwrap();

                for (j, &byte) in buf.iter().enumerate() {
                    assert_eq!(byte, ((i as usize + j) % 256) as u8);
                }
            }
        }
    }

    // ─── Test 2: FilePageStorage extend ───

    #[test]
    fn file_page_storage_extend() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.db");

        let storage = FilePageStorage::create(&path, PAGE_SIZE).unwrap();
        assert_eq!(storage.page_count(), 0);

        storage.extend(100).unwrap();
        assert_eq!(storage.page_count(), 100);

        // Write and read every page
        for i in 0..100u32 {
            let page = vec![i as u8; PAGE_SIZE];
            storage.write_page(i, &page).unwrap();

            let mut buf = vec![0u8; PAGE_SIZE];
            storage.read_page(i, &mut buf).unwrap();
            assert_eq!(buf, page);
        }

        // Out of range should fail
        let mut buf = vec![0u8; PAGE_SIZE];
        assert!(storage.read_page(100, &mut buf).is_err());
    }

    // ─── Test 3: FilePageStorage concurrent reads ───

    #[test]
    fn file_page_storage_concurrent_reads() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.db");

        let storage = std::sync::Arc::new(FilePageStorage::create(&path, PAGE_SIZE).unwrap());
        storage.extend(16).unwrap();

        // Write distinct patterns to each page
        for i in 0..16u32 {
            let page = vec![i as u8; PAGE_SIZE];
            storage.write_page(i, &page).unwrap();
        }
        storage.sync().unwrap();

        // Spawn threads to read different pages concurrently
        let mut handles = Vec::new();
        for i in 0..16u32 {
            let s = storage.clone();
            handles.push(std::thread::spawn(move || {
                let mut buf = vec![0u8; PAGE_SIZE];
                s.read_page(i, &mut buf).unwrap();
                assert!(buf.iter().all(|&b| b == i as u8));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    // ─── Test 4: MemoryPageStorage roundtrip ───

    #[test]
    fn memory_page_storage_roundtrip() {
        let storage = MemoryPageStorage::new(PAGE_SIZE);
        storage.extend(4).unwrap();
        assert_eq!(storage.page_count(), 4);

        // Write pages with known patterns
        for i in 0..4u32 {
            let mut page = vec![0u8; PAGE_SIZE];
            for (j, byte) in page.iter_mut().enumerate() {
                *byte = ((i as usize * 37 + j) % 256) as u8;
            }
            storage.write_page(i, &page).unwrap();
        }

        // Read back and verify
        for i in 0..4u32 {
            let mut buf = vec![0u8; PAGE_SIZE];
            storage.read_page(i, &mut buf).unwrap();

            for (j, &byte) in buf.iter().enumerate() {
                assert_eq!(byte, ((i as usize * 37 + j) % 256) as u8);
            }
        }

        // Out of range
        let mut buf = vec![0u8; PAGE_SIZE];
        assert!(storage.read_page(4, &mut buf).is_err());
    }

    // ─── Test 5: MemoryPageStorage extend ───

    #[test]
    fn memory_page_storage_extend() {
        let storage = MemoryPageStorage::new(PAGE_SIZE);
        assert_eq!(storage.page_count(), 0);

        storage.extend(10).unwrap();
        assert_eq!(storage.page_count(), 10);

        // Verify zero-fill
        for i in 0..10u32 {
            let mut buf = vec![0xFFu8; PAGE_SIZE];
            storage.read_page(i, &mut buf).unwrap();
            assert!(
                buf.iter().all(|&b| b == 0),
                "page {} should be zero-filled",
                i
            );
        }

        // Extend again (should be a no-op for smaller count)
        storage.extend(5).unwrap();
        assert_eq!(storage.page_count(), 10);

        // Extend further
        storage.extend(20).unwrap();
        assert_eq!(storage.page_count(), 20);
    }

    // ─── Test 6: FileWalStorage write + read ───

    #[test]
    fn file_wal_storage_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let storage = FileWalStorage::create(&wal_dir, 1024 * 1024).unwrap();

        let data1 = b"hello world";
        let data2 = b"second record";
        let data3 = b"third record!";

        let off1 = storage.append(data1).unwrap();
        let off2 = storage.append(data2).unwrap();
        let off3 = storage.append(data3).unwrap();

        assert_eq!(off1, 0);
        assert_eq!(off2, data1.len() as u64);
        assert_eq!(off3, (data1.len() + data2.len()) as u64);
        assert_eq!(
            storage.size(),
            (data1.len() + data2.len() + data3.len()) as u64
        );

        // Read everything from offset 0
        let total_len = data1.len() + data2.len() + data3.len();
        let mut buf = vec![0u8; total_len];
        let n = storage.read_from(0, &mut buf).unwrap();
        assert_eq!(n, total_len);
        assert_eq!(&buf[..data1.len()], data1);
        assert_eq!(
            &buf[data1.len()..data1.len() + data2.len()],
            data2.as_slice()
        );
        assert_eq!(&buf[data1.len() + data2.len()..], data3.as_slice());

        // Read from a specific offset
        let mut buf2 = vec![0u8; data2.len()];
        let n = storage.read_from(off2, &mut buf2).unwrap();
        assert_eq!(n, data2.len());
        assert_eq!(&buf2, data2.as_slice());
    }

    // ─── Test 7: FileWalStorage segment rollover ───

    #[test]
    fn file_wal_storage_segment_rollover() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        // Small segment size to trigger rollover quickly.
        // Header is 32 bytes, so 1024 means ~992 bytes of data per segment.
        let segment_size = 1024;
        let storage = FileWalStorage::create(&wal_dir, segment_size).unwrap();

        // Write enough data to trigger multiple rollovers
        let record = vec![0xABu8; 200];
        let num_records = 20;
        let mut offsets = Vec::new();

        for _ in 0..num_records {
            let off = storage.append(&record).unwrap();
            offsets.push(off);
        }

        // Verify we have multiple segments
        {
            let inner = storage.inner.lock();
            assert!(
                inner.segments.len() > 1,
                "expected multiple segments, got {}",
                inner.segments.len()
            );
        }

        // Read all data back and verify
        for (i, &off) in offsets.iter().enumerate() {
            let mut buf = vec![0u8; record.len()];
            let n = storage.read_from(off, &mut buf).unwrap();
            assert_eq!(n, record.len(), "record {} short read", i);
            assert_eq!(buf, record, "record {} mismatch", i);
        }

        // Also read the entire stream at once
        let total_size = storage.size();
        assert_eq!(total_size, (num_records * record.len()) as u64);

        let mut all = vec![0u8; total_size as usize];
        let n = storage.read_from(0, &mut all).unwrap();
        assert_eq!(n, total_size as usize);

        for i in 0..num_records {
            let start = i * record.len();
            assert_eq!(&all[start..start + record.len()], &record[..]);
        }
    }

    // ─── Test 8: FileWalStorage reopen ───

    #[test]
    fn file_wal_storage_reopen() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let data1 = b"before close";
        let data2 = b"after reopen";

        // Create, append, close
        let size_before;
        {
            let storage = FileWalStorage::create(&wal_dir, 1024 * 1024).unwrap();
            storage.append(data1).unwrap();
            storage.sync().unwrap();
            size_before = storage.size();
            assert_eq!(size_before, data1.len() as u64);
        }

        // Reopen and verify size, then append more
        {
            let storage = FileWalStorage::open(&wal_dir, 1024 * 1024).unwrap();
            assert_eq!(storage.size(), size_before);

            // Read old data
            let mut buf = vec![0u8; data1.len()];
            let n = storage.read_from(0, &mut buf).unwrap();
            assert_eq!(n, data1.len());
            assert_eq!(&buf, data1.as_slice());

            // Append new data
            let off2 = storage.append(data2).unwrap();
            assert_eq!(off2, data1.len() as u64);

            // Read everything
            let total = data1.len() + data2.len();
            let mut all = vec![0u8; total];
            let n = storage.read_from(0, &mut all).unwrap();
            assert_eq!(n, total);
            assert_eq!(&all[..data1.len()], data1.as_slice());
            assert_eq!(&all[data1.len()..], data2.as_slice());
        }
    }

    // ─── Test 9: FileWalStorage truncate_before ───

    #[test]
    fn file_wal_storage_truncate_before() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        // Use small segments to get multiple segment files
        let segment_size = 256;
        let storage = FileWalStorage::create(&wal_dir, segment_size).unwrap();

        // Write several records spanning multiple segments
        let record = vec![0xCDu8; 100];
        let mut offsets = Vec::new();
        for _ in 0..10 {
            let off = storage.append(&record).unwrap();
            offsets.push(off);
        }

        let seg_count_before = {
            let inner = storage.inner.lock();
            inner.segments.len()
        };
        assert!(seg_count_before > 1, "need multiple segments for this test");

        // Truncate before the 5th record's offset
        let truncate_point = offsets[5];
        storage.truncate_before(truncate_point).unwrap();

        let seg_count_after = {
            let inner = storage.inner.lock();
            inner.segments.len()
        };
        assert!(
            seg_count_after < seg_count_before,
            "expected fewer segments after truncation"
        );

        // Data at and after the truncate point should still be readable
        for &off in &offsets[5..] {
            let mut buf = vec![0u8; record.len()];
            let n = storage.read_from(off, &mut buf).unwrap();
            assert_eq!(
                n,
                record.len(),
                "record at offset {} should be readable",
                off
            );
            assert_eq!(buf, record);
        }
    }

    // ─── Test 10: MemoryWalStorage roundtrip ───

    #[test]
    fn memory_wal_storage_roundtrip() {
        let storage = MemoryWalStorage::new();

        let data1 = b"alpha";
        let data2 = b"bravo";
        let data3 = b"charlie";

        let off1 = storage.append(data1).unwrap();
        let off2 = storage.append(data2).unwrap();
        let off3 = storage.append(data3).unwrap();

        assert_eq!(off1, 0);
        assert_eq!(off2, 5);
        assert_eq!(off3, 10);
        assert_eq!(storage.size(), 17);

        // Read all
        let mut buf = vec![0u8; 17];
        let n = storage.read_from(0, &mut buf).unwrap();
        assert_eq!(n, 17);
        assert_eq!(&buf[0..5], b"alpha");
        assert_eq!(&buf[5..10], b"bravo");
        assert_eq!(&buf[10..17], b"charlie");

        // Read from offset
        let mut buf2 = vec![0u8; 5];
        let n = storage.read_from(5, &mut buf2).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf2, b"bravo");

        // Truncate before offset 10
        storage.truncate_before(10).unwrap();

        // Old data gone
        let mut buf3 = vec![0u8; 5];
        let n = storage.read_from(0, &mut buf3).unwrap();
        assert_eq!(n, 0, "data before truncation point should be gone");

        // New data still there
        let mut buf4 = vec![0u8; 7];
        let n = storage.read_from(10, &mut buf4).unwrap();
        assert_eq!(n, 7);
        assert_eq!(&buf4, b"charlie");

        // Size unchanged (logical end)
        assert_eq!(storage.size(), 17);

        // retained_size reflects truncation
        assert_eq!(storage.retained_size(), 7);

        // oldest_lsn reflects new base
        assert_eq!(storage.oldest_lsn(), Some(10));

        // Can still append after truncation
        let off4 = storage.append(b"delta").unwrap();
        assert_eq!(off4, 17);
        assert_eq!(storage.size(), 22);
    }

    // ─── Test 11: is_durable ───

    #[test]
    fn is_durable() {
        let tmp = TempDir::new().unwrap();

        // File backends are durable
        let fp_path = tmp.path().join("data.db");
        let fp = FilePageStorage::create(&fp_path, PAGE_SIZE).unwrap();
        assert!(PageStorage::is_durable(&fp));

        let fw_dir = tmp.path().join("wal");
        let fw = FileWalStorage::create(&fw_dir, 1024 * 1024).unwrap();
        assert!(WalStorage::is_durable(&fw));

        // Memory backends are not durable
        let mp = MemoryPageStorage::new(PAGE_SIZE);
        assert!(!PageStorage::is_durable(&mp));

        let mw = MemoryWalStorage::new();
        assert!(!WalStorage::is_durable(&mw));
    }

    // ─── Test 12: FileWalStorage empty read ───

    #[test]
    fn file_wal_storage_empty_read() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let storage = FileWalStorage::create(&wal_dir, 1024 * 1024).unwrap();

        // Write a small amount
        storage.append(b"data").unwrap();

        // Read beyond the end
        let mut buf = vec![0u8; 100];
        let n = storage.read_from(1000, &mut buf).unwrap();
        assert_eq!(n, 0, "read beyond size should return 0 bytes");

        // Read with empty buffer
        let mut empty_buf = vec![];
        let n = storage.read_from(0, &mut empty_buf).unwrap();
        assert_eq!(n, 0, "read with empty buffer should return 0 bytes");
    }
}
