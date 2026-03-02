//! A minimal, crash-consistent, disk-backed page store with a WAL and buffer
//! pool.
//!
//! This crate implements the **lowest-level storage engine primitives** you can
//! build a database on top of:
//!
//! - A **page file** (`data.db`) storing fixed-size pages on disk.
//! - A **write-ahead log (WAL)** (`wal.log`) storing redo records for updates.
//! - A bounded in-memory **buffer pool** (page cache) with CLOCK eviction.
//! - **Redo recovery** at startup to restore a consistent state after a crash.
//!
//! # What this is (and is not)
//!
//! This is intentionally minimal. It provides:
//!
//! - Addressable pages by `PageId`
//! - In-page reads/writes by offset
//! - Durability for acknowledged writes (WAL is fsynced before returning)
//! - Crash recovery via redo
//!
//! It does *not* provide:
//!
//! - Transactions across multiple pages
//! - Concurrency control / locking
//! - A B+Tree/LSM index
//! - Compaction, checkpoints, WAL truncation
//!
//! # Strong consistency model
//!
//! `Db::write_at` implements a **commit-like** durability point: it appends an
//! update to the WAL, applies it in the buffer pool, and then **fsyncs the WAL**
//! before returning `Ok(())`.
//!
//! Dirty data pages are written later (on eviction or explicit flush), but only
//! after the WAL is durable up to the page LSN (the classic WAL rule).
//!
//! # Files and layout
//!
//! - `data.db`: fixed-size pages of `PAGE_SIZE` bytes at offsets
//!   `page_id * PAGE_SIZE`.
//! - `wal.log`: append-only log with page patch records.
//!
//! Each data page includes a small header:
//!
//! - `page_lsn: u64` at bytes `0..8`
//! - `checksum: u64` at bytes `8..16`
//! - payload at bytes `16..PAGE_SIZE`
//!
//! # Example
//!
//! ```no_run
//! use std::path::PathBuf;
//!
//! fn main() -> std::io::Result<()> {
//!     let dir = std::env::temp_dir().join("mini_page_db_example");
//!     std::fs::create_dir_all(&dir)?;
//!
//!     let data_path = dir.join("data.db");
//!     let wal_path = dir.join("wal.log");
//!
//!     let mut db = mini_page_db::Db::open(&data_path, &wal_path, 128)?;
//!
//!     // Write into page 0, payload offset 16 (start of payload).
//!     db.write_at(0, 16, b"hello")?;
//!
//!     let bytes = db.read_at(0, 16, 5)?;
//!     assert_eq!(&bytes, b"hello");
//!
//!     db.flush_all()?;
//!     Ok(())
//! }
//! ```
//!
//! # Portability notes
//!
//! On Unix, the implementation uses positioned I/O (`FileExt::read_at` and
//! `FileExt::write_at`) to avoid global file cursor seeks. On non-Unix targets,
//! it falls back to cloning the file handle and using `seek + read_exact` /
//! `seek + write_all`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Size of a single on-disk page, in bytes.
///
/// This is the unit of I/O between disk and memory.
const PAGE_SIZE: usize = 4096;

/// Size of the page header, in bytes.
///
/// Header layout:
/// - bytes `0..8`: page LSN (`u64`)
/// - bytes `8..16`: checksum (`u64`)
const PAGE_HEADER_SIZE: usize = 16;

/// A logical page identifier.
///
/// A page is stored at byte offset `page_id * PAGE_SIZE` in `data.db`.
type PageId = u64;

/// A frame index in the buffer pool.
type FrameId = usize;

/// Log sequence number.
///
/// In this implementation, an LSN is the WAL byte offset *after* appending a
/// record (i.e., the end position).
type Lsn = u64;

/// Compute a simple 64-bit FNV-1a hash.
///
/// This is used as a lightweight page checksum for corruption detection.
///
/// Note: FNV-1a is not cryptographic.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn read_u64_le(buf: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[0..8]);
    u64::from_le_bytes(a)
}

fn write_u64_le(buf: &mut [u8], v: u64) {
    buf[0..8].copy_from_slice(&v.to_le_bytes());
}

fn page_get_lsn(page: &[u8; PAGE_SIZE]) -> Lsn {
    read_u64_le(&page[0..8])
}

fn page_set_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    write_u64_le(&mut page[0..8], lsn);
}

fn page_get_checksum(page: &[u8; PAGE_SIZE]) -> u64 {
    read_u64_le(&page[8..16])
}

fn page_set_checksum(page: &mut [u8; PAGE_SIZE], cksum: u64) {
    write_u64_le(&mut page[8..16], cksum);
}

fn page_recompute_checksum(page: &mut [u8; PAGE_SIZE]) {
    let cksum = fnv1a64(&page[PAGE_HEADER_SIZE..]);
    page_set_checksum(page, cksum);
}

fn page_verify_checksum(page: &[u8; PAGE_SIZE]) -> bool {
    let stored = page_get_checksum(page);
    let computed = fnv1a64(&page[PAGE_HEADER_SIZE..]);
    stored == computed
}

#[cfg(unix)]
mod positioned_io {
    //! Platform-specific positioned I/O helpers (Unix).
    //!
    //! On Unix, we can avoid using a shared file cursor by using `FileExt`
    //! positioned reads/writes. This tends to be simpler and faster in the
    //! presence of caching and background flushing (if added later).

    use super::*;
    use std::os::unix::fs::FileExt;

    /// Read exactly `buf.len()` bytes at `offset`.
    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut read = 0usize;
        while read < buf.len() {
            let n = file.read_at(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }
            read += n;
        }
        Ok(())
    }

    /// Write all bytes from `buf` at `offset`.
    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.write_at(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
            }
            written += n;
        }
        Ok(())
    }
}

#[cfg(not(unix))]
mod positioned_io {
    //! Platform-specific positioned I/O helpers (non-Unix).
    //!
    //! Rust's stable standard library offers `FileExt` positioned I/O on Unix.
    //! For other platforms, we implement a conservative fallback:
    //! clone the file handle, seek, then read/write.
    //!
    //! This avoids moving any shared cursor on the original `File` handle.

    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    /// Read exactly `buf.len()` bytes at `offset`.
    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.read_exact(buf)?;
        Ok(())
    }

    /// Write all bytes from `buf` at `offset`.
    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.write_all(buf)?;
        Ok(())
    }
}

/// The on-disk page file manager.
///
/// This is responsible for reading and writing whole pages to `data.db`.
struct DiskManager {
    data: File,
}

impl DiskManager {
    /// Open (or create) the data file at `path`.
    fn open(path: &Path) -> io::Result<Self> {
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { data })
    }

    /// Read `page_id` into `out`.
    ///
    /// If the page doesn't exist yet (past EOF), returns a zeroed, valid page
    /// with an LSN of 0 and a computed checksum.
    fn read_page(&self, page_id: PageId, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        let offset = page_id * PAGE_SIZE as u64;

        let len = self.data.metadata()?.len();
        if offset + PAGE_SIZE as u64 > len {
            out.fill(0);
            page_set_lsn(out, 0);
            page_recompute_checksum(out);
            return Ok(());
        }

        positioned_io::read_at(&self.data, offset, out)?;
        if !page_verify_checksum(out) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page checksum mismatch",
            ));
        }
        Ok(())
    }

    /// Write the provided page bytes to `page_id`.
    ///
    /// The caller is responsible for ensuring the checksum is correct.
    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> io::Result<()> {
        let offset = page_id * PAGE_SIZE as u64;
        positioned_io::write_at(&self.data, offset, page)?;
        Ok(())
    }

    /// Flush buffered data for `data.db` to durable storage.
    fn sync(&self) -> io::Result<()> {
        self.data.sync_data()
    }
}

/// WAL record types.
///
/// Only one record type is implemented: an in-page update (patch).
#[derive(Clone, Copy, Debug)]
enum WalRecType {
    /// A patch to a page's payload bytes.
    Update = 1,
}

/// A write-ahead log (WAL) for redo recovery.
///
/// The WAL is append-only. Each update is written as a small patch record.
/// The WAL is **fsynced** (via `sync_data`) to provide durability.
struct Wal {
    path: PathBuf,
    file: File,
    writer: BufWriter<File>,
    end_offset: u64,
    flushed_offset: u64,
}

impl Wal {
    /// Open (or create) the WAL file at `path`.
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let end_offset = file.metadata()?.len();
        let writer = BufWriter::with_capacity(1 << 20, file.try_clone()?);

        Ok(Self {
            path: path.to_path_buf(),
            file,
            writer,
            end_offset,
            flushed_offset: end_offset,
        })
    }

    /// Append a page update record to the WAL (buffered).
    ///
    /// Returns the LSN for the record, defined as the WAL end offset after
    /// appending.
    ///
    /// Record format:
    ///
    /// - `type: u8` (1 for update)
    /// - `page_id: u64`
    /// - `offset_in_page: u16`
    /// - `len: u16`
    /// - `payload: [u8; len]`
    fn append_update(
        &mut self,
        page_id: PageId,
        offset_in_page: u16,
        bytes: &[u8],
    ) -> io::Result<Lsn> {
        if offset_in_page as usize + bytes.len() > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "update exceeds page size",
            ));
        }

        let rec_type = WalRecType::Update as u8;
        self.writer.write_all(&[rec_type])?;
        self.writer.write_all(&page_id.to_le_bytes())?;
        self.writer.write_all(&offset_in_page.to_le_bytes())?;

        let len_u16: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len > u16"))?;
        self.writer.write_all(&len_u16.to_le_bytes())?;
        self.writer.write_all(bytes)?;

        self.end_offset += 1 + 8 + 2 + 2 + bytes.len() as u64;

        Ok(self.end_offset)
    }

    /// Flush buffered WAL bytes and fsync the WAL file.
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.file.sync_data()?;
        self.flushed_offset = self.end_offset;
        Ok(())
    }

    /// Ensure the WAL is durable up to `lsn`.
    fn flush_up_to(&mut self, lsn: Lsn) -> io::Result<()> {
        if lsn <= self.flushed_offset {
            return Ok(());
        }
        self.flush()
    }

    /// Perform redo recovery by scanning the WAL from the beginning and
    /// re-applying updates whose LSN is newer than a page's `page_lsn`.
    ///
    /// This is a **redo-only** recovery algorithm.
    fn recover_redo(&self, disk: &DiskManager) -> io::Result<()> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        let mut reader = BufReader::with_capacity(1 << 20, &mut f);

        let mut offset: u64 = 0;

        loop {
            let mut typ = [0u8; 1];
            match reader.read_exact(&mut typ) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            offset += 1;

            let rec_type = typ[0];
            if rec_type != WalRecType::Update as u8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown WAL record type",
                ));
            }

            let mut page_id_buf = [0u8; 8];
            reader.read_exact(&mut page_id_buf)?;
            offset += 8;
            let page_id = u64::from_le_bytes(page_id_buf);

            let mut off_buf = [0u8; 2];
            reader.read_exact(&mut off_buf)?;
            offset += 2;
            let off_in_page = u16::from_le_bytes(off_buf) as usize;

            let mut len_buf = [0u8; 2];
            reader.read_exact(&mut len_buf)?;
            offset += 2;
            let len = u16::from_le_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload)?;
            offset += len as u64;

            let lsn = offset;

            let mut page = [0u8; PAGE_SIZE];
            disk.read_page(page_id, &mut page)?;
            let page_lsn = page_get_lsn(&page);

            if lsn > page_lsn {
                if off_in_page < PAGE_HEADER_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WAL update touches page header",
                    ));
                }

                page[off_in_page..off_in_page + len].copy_from_slice(&payload);
                page_set_lsn(&mut page, lsn);
                page_recompute_checksum(&mut page);
                disk.write_page(page_id, &page)?;
            }
        }

        disk.sync()?;
        Ok(())
    }
}

/// A single in-memory buffer pool frame.
struct Frame {
    page_id: Option<PageId>,
    data: Box<[u8; PAGE_SIZE]>,
    dirty: bool,
    pin_count: u32,
    page_lsn: Lsn,
    ref_bit: bool,
}

impl Frame {
    fn new() -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        page_set_lsn(&mut data, 0);
        page_recompute_checksum(&mut data);

        Self {
            page_id: None,
            data,
            dirty: false,
            pin_count: 0,
            page_lsn: 0,
            ref_bit: false,
        }
    }
}

/// An in-memory page cache (buffer pool) with CLOCK eviction.
struct BufferPool {
    frames: Vec<Frame>,
    page_table: HashMap<PageId, FrameId>,
    clock_hand: usize,
}

impl BufferPool {
    fn new(capacity_pages: usize) -> Self {
        let mut frames = Vec::with_capacity(capacity_pages);
        for _ in 0..capacity_pages {
            frames.push(Frame::new());
        }
        Self {
            frames,
            page_table: HashMap::new(),
            clock_hand: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Find a frame to load a page into, evicting if necessary.
    ///
    /// CLOCK eviction:
    /// - Skip pinned frames
    /// - If `ref_bit` is set, clear it and give a second chance
    /// - Otherwise, evict
    fn find_victim(&mut self) -> io::Result<FrameId> {
        let n = self.frames.len();

        for _ in 0..(2 * n + 1) {
            let fid = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % n;

            let f = &mut self.frames[fid];
            if f.pin_count > 0 {
                continue;
            }
            if f.page_id.is_none() {
                return Ok(fid);
            }
            if f.ref_bit {
                f.ref_bit = false;
                continue;
            }
            return Ok(fid);
        }

        Err(io::Error::new(
            io::ErrorKind::Other,
            "no evictable frame (all pages pinned?)",
        ))
    }
}

/// A minimal database storage engine: page file + WAL + buffer pool.
///
/// `Db` is the main entry point. It supports reading and writing bytes in page
/// payload areas. Writes are made durable via a WAL.
///
/// See the crate-level documentation for details and an example.
pub struct Db {
    disk: DiskManager,
    wal: Wal,
    pool: BufferPool,
}

impl Db {
    /// Open (or create) a database at the given file paths.
    ///
    /// This method performs **redo recovery** before returning, scanning the WAL
    /// and applying any missing updates to pages.
    ///
    /// Parameters:
    /// - `data_path`: path to the page file (`data.db`)
    /// - `wal_path`: path to the write-ahead log (`wal.log`)
    /// - `cache_pages`: number of pages to keep in memory
    pub fn open(data_path: &Path, wal_path: &Path, cache_pages: usize) -> io::Result<Self> {
        let disk = DiskManager::open(data_path)?;
        let wal = Wal::open(wal_path)?;

        wal.recover_redo(&disk)?;

        Ok(Self {
            disk,
            wal,
            pool: BufferPool::new(cache_pages),
        })
    }

    fn load_into_frame(&mut self, page_id: PageId, fid: FrameId) -> io::Result<()> {
        if let Some(old_pid) = self.pool.frames[fid].page_id {
            self.flush_frame(fid)?;
            self.pool.page_table.remove(&old_pid);
        }

        let frame = &mut self.pool.frames[fid];
        self.disk.read_page(page_id, &mut frame.data)?;
        frame.page_id = Some(page_id);
        frame.dirty = false;
        frame.pin_count = 0;
        frame.page_lsn = page_get_lsn(&frame.data);
        frame.ref_bit = true;

        self.pool.page_table.insert(page_id, fid);
        Ok(())
    }

    fn get_frame(&mut self, page_id: PageId) -> io::Result<FrameId> {
        if let Some(&fid) = self.pool.page_table.get(&page_id) {
            let f = &mut self.pool.frames[fid];
            f.ref_bit = true;
            f.pin_count += 1;
            return Ok(fid);
        }

        let victim = self.pool.find_victim()?;
        self.load_into_frame(page_id, victim)?;

        let f = &mut self.pool.frames[victim];
        f.pin_count += 1;
        Ok(victim)
    }

    fn unpin(&mut self, fid: FrameId) {
        let f = &mut self.pool.frames[fid];
        f.pin_count = f.pin_count.saturating_sub(1);
    }

    fn flush_frame(&mut self, fid: FrameId) -> io::Result<()> {
        let frame = &mut self.pool.frames[fid];
        if !frame.dirty {
            return Ok(());
        }

        let page_id = match frame.page_id {
            Some(pid) => pid,
            None => return Ok(()),
        };

        // WAL rule: ensure WAL is durable up to this page's LSN before
        // flushing the data page.
        self.wal.flush_up_to(frame.page_lsn)?;

        page_set_lsn(&mut frame.data, frame.page_lsn);
        page_recompute_checksum(&mut frame.data);

        self.disk.write_page(page_id, &frame.data)?;
        frame.dirty = false;
        Ok(())
    }

    /// Flush all dirty pages currently resident in the buffer pool.
    ///
    /// This obeys the WAL rule, ensuring required WAL bytes are durable before
    /// writing each page.
    pub fn flush_all(&mut self) -> io::Result<()> {
        for fid in 0..self.pool.capacity() {
            self.flush_frame(fid)?;
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Read `len` bytes from a page's payload.
    ///
    /// Constraints:
    /// - Reads may not touch the page header; `offset_in_page` must be
    ///   `>= 16`.
    /// - The range must be fully within the page.
    pub fn read_at(
        &mut self,
        page_id: PageId,
        offset_in_page: u16,
        len: u16,
    ) -> io::Result<Vec<u8>> {
        let off = offset_in_page as usize;
        let len = len as usize;

        if off < PAGE_HEADER_SIZE || off + len > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read out of bounds or touches header",
            ));
        }

        let fid = self.get_frame(page_id)?;
        let out = self.pool.frames[fid].data[off..off + len].to_vec();
        self.unpin(fid);
        Ok(out)
    }

    /// Write bytes into a page's payload and make the update durable.
    ///
    /// Procedure:
    /// 1. Append a redo record to the WAL (buffered).
    /// 2. Apply the update in the buffer pool and mark the page dirty.
    /// 3. Flush the WAL up to the update LSN (fsync) and then return.
    ///
    /// This provides **strong consistency** for each call: after `Ok(())`
    /// returns, a crash will not lose the update (it will be replayed from the
    /// WAL if the page itself wasn't flushed yet).
    ///
    /// Constraints:
    /// - Writes may not touch the page header; `offset_in_page` must be
    ///   `>= 16`.
    /// - The range must be fully within the page.
    pub fn write_at(
        &mut self,
        page_id: PageId,
        offset_in_page: u16,
        bytes: &[u8],
    ) -> io::Result<()> {
        let off = offset_in_page as usize;
        if off < PAGE_HEADER_SIZE || off + bytes.len() > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write out of bounds or touches header",
            ));
        }

        let lsn = self.wal.append_update(page_id, offset_in_page, bytes)?;

        let fid = self.get_frame(page_id)?;
        {
            let frame = &mut self.pool.frames[fid];
            frame.data[off..off + bytes.len()].copy_from_slice(bytes);
            frame.dirty = true;
            frame.page_lsn = lsn;
            frame.ref_bit = true;
        }
        self.unpin(fid);

        // Durability point for the update.
        self.wal.flush_up_to(lsn)?;
        Ok(())
    }
}
