## Design document: minimal, fast, strongly-consistent, disk-backed store (page + buffer pool + WAL)

### Goals (your bullets mapped)
- **Large collection (bigger than RAM):** store data in a **page file** on disk; only a bounded number of pages live in memory.
- **Strong consistency (crash-safe):** **WAL** with the WAL rule: *log must reach disk before dirty pages depending on it can reach disk*.
- **Fast writes:** append-only WAL (sequential), **batched flushing** (group commit), and **no-force** (don’t force data pages on commit).
- **Intelligent in-memory cache of “top pages”:** a **buffer pool** with an O(1) lookup map and an efficient eviction policy (CLOCK-sweep; can be upgraded to 2Q/TinyLFU later).

---

## Storage layout

### Files
1. `data.db` — fixed-size **pages** of `PAGE_SIZE` bytes at offsets `page_id * PAGE_SIZE`.
2. `wal.log` — append-only log of updates.

### Page format (in `data.db`)
Each page begins with a small header:
- `page_lsn: u64` (bytes 0..8) — the latest WAL LSN whose effects are reflected in this page.
- `checksum: u64` (bytes 8..16) — checksum of the rest of the page (optional but useful).
- payload bytes 16..PAGE_SIZE — your actual page content (btree nodes, heap tuples, etc.).

This lets recovery do **REDO idempotently**:
- if WAL record LSN \(>\) page_lsn, apply it
- else skip

---

## WAL design

### Record type
For minimal but efficient updates, log **page patches** (not full pages):
- `Update { lsn, page_id, offset, len, bytes... }`

This is much smaller than logging a full 4KB/8KB page for small edits.

### LSN
Use the WAL byte offset (or “end offset after append”) as the `lsn`. This makes ordering and “flush up to lsn” easy.

### Durability / strong consistency
- On write, append to WAL buffer.
- To “commit” (or to acknowledge a user write), call `wal.flush()` + `fsync` (`sync_data`).
- Data pages can be flushed later (no-force), but **only after WAL is durable up to that page’s LSN**.

This gives strong crash consistency for committed updates.

---

## Buffer pool (cache) design

### Structures
- `HashMap<PageId, FrameId>` for O(1) page lookup.
- Fixed `Vec<Frame>` of size `N` for memory bound.
- Eviction: **CLOCK-sweep**:
  - Each frame has `ref_bit` set on access.
  - Eviction scans circularly, giving second chances.
  - Skips pinned frames (`pin_count > 0`).

### Metadata per frame
- `page_id: Option<PageId>`
- `data: [u8; PAGE_SIZE]`
- `dirty: bool`
- `pin_count: u32`
- `page_lsn: u64` (mirrors header; used for WAL rule)
- `ref_bit: bool`

### Flushing dirty pages
When evicting or explicit flush:
1. Ensure `wal.flush_up_to(page_lsn)` (WAL rule)
2. Write page to `data.db`
3. Mark clean

---

## Recovery
On startup:
1. Open `data.db` and `wal.log`
2. Scan WAL sequentially:
   - for each `Update` record:
     - read page header’s `page_lsn`
     - if record LSN \(>\) page_lsn:
       - apply patch to page payload
       - set page_lsn in header
       - write page back (or stage in buffer pool then flush)
3. DB is consistent.

This minimal design does **redo-only**. If you later add multi-step transactions, you’ll add BEGIN/COMMIT and UNDO/CLR records.

---

## What makes this faster than the naive version you had
- **No O(n) LRU vector churn:** CLOCK is cheap and standard in DB buffer pools.
- **Positioned I/O (optional):** avoids seek cursor contention and is generally faster/cleaner.
- **WAL patch records:** smaller log writes than full-page images.
- **Group commit ready:** WAL buffering + `flush()` at commit boundaries avoids `fsync` per small write.
- **Steal/no-force:** commits don’t force random page writes, keeping writes mostly sequential.

---

# Code (single-file minimal implementation with the improvements)

This is a compact, working skeleton showing:
- page file with header
- WAL patch records with LSN
- buffer pool with CLOCK eviction
- WAL rule enforced on flush/evict
- crash recovery (redo)

```rust
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const PAGE_SIZE: usize = 4096;
const PAGE_HEADER_SIZE: usize = 16; // lsn (8) + checksum (8)

type PageId = u64;
type FrameId = usize;
type Lsn = u64;

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
    use super::*;
    use std::os::unix::fs::FileExt;

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

    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let n = file.write_at(&buf[written..], offset + written as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned 0",
                ));
            }
            written += n;
        }
        Ok(())
    }
}

#[cfg(not(unix))]
mod positioned_io {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut f = file;
        let mut guard = f;
        // Fallback (non-unix): use a cloned handle + seek.
        let mut cloned = guard.try_clone()?;
        cloned.seek(SeekFrom::Start(offset))?;
        cloned.read_exact(buf)?;
        Ok(())
    }

    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut cloned = file.try_clone()?;
        cloned.seek(std::io::SeekFrom::Start(offset))?;
        cloned.write_all(buf)?;
        Ok(())
    }
}

struct DiskManager {
    data: File,
}

impl DiskManager {
    fn open(path: &Path) -> io::Result<Self> {
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { data })
    }

    fn read_page(&self, page_id: PageId, out: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        let offset = page_id * PAGE_SIZE as u64;

        // If page is past EOF, return a zeroed page.
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

    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) -> io::Result<()> {
        let offset = page_id * PAGE_SIZE as u64;
        positioned_io::write_at(&self.data, offset, page)?;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        // sync_data is usually enough for durability of file contents.
        self.data.sync_data()
    }
}

#[derive(Clone, Copy, Debug)]
enum WalRecType {
    Update = 1,
}

struct Wal {
    path: PathBuf,
    file: File,
    writer: BufWriter<File>,
    end_offset: u64,
    flushed_offset: u64,
}

impl Wal {
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

        // Record format:
        // [type: u8]
        // [page_id: u64]
        // [offset: u16]
        // [len: u16]
        // [payload: len]
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

        // LSN is the end offset after this append.
        Ok(self.end_offset)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.file.sync_data()?;
        self.flushed_offset = self.end_offset;
        Ok(())
    }

    fn flush_up_to(&mut self, lsn: Lsn) -> io::Result<()> {
        if lsn <= self.flushed_offset {
            return Ok(());
        }
        self.flush()
    }

    fn recover_redo(&self, disk: &DiskManager) -> io::Result<()> {
        // Simple redo recovery: scan wal from start, apply updates if needed.
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

            let lsn = offset; // end offset after this record

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

pub struct Db {
    disk: DiskManager,
    wal: Wal,
    pool: BufferPool,
}

impl Db {
    pub fn open(
        data_path: &Path,
        wal_path: &Path,
        cache_pages: usize,
    ) -> io::Result<Self> {
        let disk = DiskManager::open(data_path)?;
        let wal = Wal::open(wal_path)?;

        // REDO recovery before serving reads/writes.
        wal.recover_redo(&disk)?;

        Ok(Self {
            disk,
            wal,
            pool: BufferPool::new(cache_pages),
        })
    }

    fn load_into_frame(&mut self, page_id: PageId, fid: FrameId) -> io::Result<()> {
        // If this frame currently contains a page, evict it.
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

        // WAL rule: WAL must be durable up to page_lsn before writing page.
        self.wal.flush_up_to(frame.page_lsn)?;

        page_set_lsn(&mut frame.data, frame.page_lsn);
        page_recompute_checksum(&mut frame.data);

        self.disk.write_page(page_id, &frame.data)?;
        frame.dirty = false;
        Ok(())
    }

    pub fn flush_all(&mut self) -> io::Result<()> {
        for fid in 0..self.pool.capacity() {
            self.flush_frame(fid)?;
        }
        self.disk.sync()?;
        Ok(())
    }

    /// Read raw bytes from within a page payload.
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

    /// Write bytes into a page payload, strongly consistent:
    /// returns only after WAL is fsynced (commit-like).
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

        // 1) Append WAL record (buffered) and get LSN.
        let lsn = self
            .wal
            .append_update(page_id, offset_in_page, bytes)?;

        // 2) Apply in buffer pool page.
        let fid = self.get_frame(page_id)?;
        {
            let frame = &mut self.pool.frames[fid];
            frame.data[off..off + bytes.len()].copy_from_slice(bytes);
            frame.dirty = true;
            frame.page_lsn = lsn;
            frame.ref_bit = true;
        }
        self.unpin(fid);

        // 3) Strong consistency point: flush WAL (fsync) before returning.
        self.wal.flush_up_to(lsn)?;
        Ok(())
    }
}
```

---

## Next improvements (if you want to push performance further)
If you tell me your workload pattern (random reads vs scans, write rate, value sizes), I can tailor these:

1. **Group commit API:** expose `begin_batch()/commit_batch()` so you can fsync once for many writes (still strongly consistent per batch).
2. **Background flusher thread:** flush dirty unpinned pages asynchronously to reduce eviction stalls.
3. **Better cache admission:** CLOCK-Pro / 2Q / TinyLFU-style admission to keep “top used” pages under mixed scan+hotset loads.
4. **B+Tree index pages:** store a real index on top of this page layer so you can do fast key lookups without loading everything.
5. **Pre-allocation & file growth strategy:** reduce file extension overhead for large DBs.
6. **Checksums via CRC32C:** faster + better error detection than the simple hash (would use a small crate).

If you answer these two questions, I’ll adapt the design + code to the exact use case:
- Are writes mostly *appends/new keys* or *in-place updates*?
- Do you need *transactions across multiple pages*, or is *single-page atomic updates* enough for now?