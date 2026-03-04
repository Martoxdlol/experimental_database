# S1: Backend Traits + Implementations

## Purpose

Abstract physical I/O behind traits so the same engine code works with file-backed (durable) and in-memory (ephemeral) storage. Custom backends (S3, etc.) plug in via the same traits.

## Dependencies

None. This is the foundation layer.

## Rust Types

```rust
use std::path::{Path, PathBuf};
use std::io::Result;

pub type PageId = u32;

// ─── Page Storage Trait ───

/// Backend for fixed-size page I/O.
/// Implementations must be thread-safe (Send + Sync).
pub trait PageStorage: Send + Sync {
    /// Read page `page_id` into `buf`. `buf.len() == page_size`.
    /// Returns IoError if page_id >= page_count (unless the impl auto-extends).
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<()>;

    /// Write `buf` to page `page_id`. `buf.len() == page_size`.
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<()>;

    /// Flush all pending writes to durable storage.
    /// No-op for in-memory backends.
    fn sync(&self) -> Result<()>;

    /// Current number of allocated pages.
    fn page_count(&self) -> u64;

    /// Extend the store to at least `new_count` pages.
    /// Newly allocated pages are zero-filled.
    fn extend(&self, new_count: u64) -> Result<()>;

    /// Returns the page size this backend was configured with.
    fn page_size(&self) -> usize;

    /// Whether this backend provides durable storage.
    fn is_durable(&self) -> bool;
}

// ─── WAL Storage Trait ───

/// Backend for append-only WAL I/O.
pub trait WalStorage: Send + Sync {
    /// Append `data` to the log. Returns the byte offset where data was written.
    fn append(&self, data: &[u8]) -> Result<u64>;

    /// Flush all pending appends to durable storage.
    fn sync(&self) -> Result<()>;

    /// Read up to `buf.len()` bytes starting at `offset`.
    /// Returns number of bytes actually read.
    fn read_from(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Discard all data before `offset` (segment reclamation).
    fn truncate_before(&self, offset: u64) -> Result<()>;

    /// Total bytes written so far.
    fn size(&self) -> u64;

    /// Whether this backend provides durable storage.
    fn is_durable(&self) -> bool;
}

// ─── File-Backed Implementations ───

/// Durable page storage backed by a single data file.
/// Uses pread/pwrite for concurrent positional I/O.
pub struct FilePageStorage {
    file: std::fs::File,
    page_size: usize,
    page_count: std::sync::atomic::AtomicU64,
}

impl FilePageStorage {
    pub fn open(path: &Path, page_size: usize) -> Result<Self>;
    pub fn create(path: &Path, page_size: usize) -> Result<Self>;
}

/// Durable WAL storage backed by segment files in a directory.
pub struct FileWalStorage {
    dir: PathBuf,
    segment_size: usize,
    // Internal: manages active segment, segment list
    inner: parking_lot::Mutex<FileWalInner>,
}

struct FileWalInner {
    active_segment: std::fs::File,
    active_segment_id: u32,
    active_base_lsn: u64,
    write_offset: u64,         // current position in logical stream
    segments: Vec<SegmentInfo>, // all segments ordered by base_lsn
}

struct SegmentInfo {
    segment_id: u32,
    base_lsn: u64,
    file_size: u64,
    path: PathBuf,
}

impl FileWalStorage {
    pub fn open(dir: &Path, segment_size: usize) -> Result<Self>;
    pub fn create(dir: &Path, segment_size: usize) -> Result<Self>;
}

// ─── In-Memory Implementations ───

/// Ephemeral page storage. Data lives only in RAM.
pub struct MemoryPageStorage {
    pages: parking_lot::RwLock<Vec<Vec<u8>>>,
    page_size: usize,
}

impl MemoryPageStorage {
    pub fn new(page_size: usize) -> Self;
}

/// Ephemeral WAL storage. Append-only byte vector.
pub struct MemoryWalStorage {
    log: parking_lot::RwLock<Vec<u8>>,
}

impl MemoryWalStorage {
    pub fn new() -> Self;
}
```

## Implementation Details

### FilePageStorage

1. **open()**: Open existing `data.db` file. Compute `page_count = file_size / page_size`.
2. **create()**: Create new `data.db` file. Set `page_count = 0`.
3. **read_page()**: `pread(file, buf, page_id as u64 * page_size as u64)`. Fails if `page_id >= page_count`.
4. **write_page()**: `pwrite(file, buf, page_id as u64 * page_size as u64)`. Fails if `page_id >= page_count`.
5. **sync()**: `file.sync_data()` (fdatasync on Linux).
6. **extend()**: `file.set_len(new_count * page_size)`, then `file.sync_data()`. Update atomic page_count.

**Positional I/O**: Use `std::os::unix::fs::FileExt` traits (`read_at`/`write_at`) for pread/pwrite. These do not modify the file seek position, allowing concurrent access from multiple threads without a mutex on the file descriptor.

### FileWalStorage

1. **open()**: Scan `wal/` directory for `segment-*.wal` files. Parse headers. Build segment list sorted by `base_lsn`. Open the latest segment for appending. Set `write_offset` = base_lsn of last segment + (file_size - 32).
2. **create()**: Create `wal/` directory. Create `segment-000001.wal` with header (base_lsn=0). Set `write_offset = 0`.
3. **append()**: Lock inner mutex. Write to active segment file. If segment exceeds target size after write, roll over to new segment. Return logical offset (= write_offset before append). Advance write_offset by data.len().
4. **sync()**: Lock inner mutex. `active_segment.sync_data()`.
5. **read_from()**: Find segment containing offset (binary search on base_lsn). Read from that segment at file offset `32 + (offset - segment.base_lsn)`. If read crosses segment boundary, continue into next segment.
6. **truncate_before()**: Delete segment files whose highest LSN < offset.

**Segment rollover**: After an append, if current segment file size > segment_size: create new segment, write header, switch active segment.

### MemoryPageStorage

1. **read_page()**: `pages.read()[page_id].clone_into(buf)`.
2. **write_page()**: `pages.write()[page_id].copy_from_slice(buf)`.
3. **sync()**: No-op.
4. **extend()**: Push zero-filled `Vec<u8>` entries.
5. **is_durable()**: `false`.

### MemoryWalStorage

1. **append()**: `log.write().extend_from_slice(data)`. Return previous length as offset.
2. **sync()**: No-op.
3. **read_from()**: Slice from the log vector.
4. **truncate_before()**: Real truncation — drops all data before the given LSN. Implementation: find the split point in the log vector, drain `0..split_point`, and adjust internal offsets so that subsequent `read_from()` calls with old offsets still map correctly. This keeps memory bounded if the caller periodically reclaims old WAL data.
5. **is_durable()**: `false`.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| `IoError` | Disk full, permission denied, file not found | Propagate to caller via `Result` |
| Page out of range | `page_id >= page_count` in read | Return `IoError(InvalidInput)` |
| Corrupt segment header | Bad magic in WAL segment | Return `IoError(InvalidData)` |
| Empty WAL directory | No segments found on open | Return error (distinct from create) |

## Tests

1. **FilePageStorage roundtrip**: Create, write pages with known patterns, close, reopen, read back, verify.
2. **FilePageStorage extend**: Start with 0 pages, extend to 100, write/read each.
3. **FilePageStorage concurrent reads**: Spawn multiple threads, each reads different pages via pread.
4. **MemoryPageStorage roundtrip**: Write and read back pages.
5. **MemoryPageStorage extend**: Extend and verify zero-fill.
6. **FileWalStorage write + read**: Append several records, read them back from offset 0.
7. **FileWalStorage segment rollover**: Set small segment_size (1 KB), write enough to trigger rollover, verify all data readable across segments.
8. **FileWalStorage reopen**: Append, close, reopen, verify size() correct, append more, verify continuity.
9. **FileWalStorage truncate_before**: Append, truncate, verify old data gone but new data intact.
10. **MemoryWalStorage roundtrip**: Append and read back.
11. **is_durable()**: File returns true, Memory returns false.
12. **FileWalStorage empty read**: Read from offset beyond size returns 0 bytes.
