# 10 — File Header + Shadow Copy

Implements DESIGN.md §2.12.3 and §2.13.4.

## File: `src/storage/file_header.rs`

### File Header (Page 0)

```rust
/// File header fields stored in page 0 of data.db.
/// Not a standard slotted page — fixed layout.
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub magic: u32,                   // 0x45584442 "EXDB"
    pub version: u32,                 // file format version (1)
    pub page_size: u32,               // e.g., 8192
    pub page_count: u64,              // total pages in data file
    pub free_list_head: Option<PageId>, // first free page (None = 0)
    pub catalog_root_page: PageId,    // root of catalog B-tree
    pub next_collection_id: u64,      // monotonic allocator
    pub next_index_id: u64,           // monotonic allocator
    pub checkpoint_lsn: Lsn,          // LSN of last checkpoint
    pub created_at: u64,              // database creation timestamp (ms)
}
```

### Layout (within page_size bytes)

```
Offset  Size  Field
0       4     magic (u32 LE)
4       4     version (u32 LE)
8       4     page_size (u32 LE)
12      8     page_count (u64 LE)
20      4     free_list_head (u32 LE, 0 = none)
24      4     catalog_root_page (u32 LE)
28      8     next_collection_id (u64 LE)
36      8     next_index_id (u64 LE)
44      8     checkpoint_lsn (u64 LE)
52      8     created_at (u64 LE)
60      4     checksum (CRC-32C of bytes 0..60 + 64..page_size)
64      ...   reserved (zeroed)
```

### Operations

```rust
impl FileHeader {
    /// Serialize into a page-sized buffer.
    pub fn serialize(&self, page_size: u32) -> Vec<u8>;

    /// Deserialize from a page-sized buffer. Verifies checksum.
    pub fn deserialize(buf: &[u8]) -> Result<Self, FileHeaderError>;

    /// Create a fresh file header for a new database.
    pub fn new_database(page_size: u32, catalog_root_page: PageId) -> Self;

    /// Write header to buffer pool (page 0) as an exclusive page guard operation.
    pub fn write_to_page(&self, guard: &mut ExclusivePageGuard);

    /// Read from a shared page guard.
    pub fn read_from_page(guard: &SharedPageGuard) -> Result<Self>;
}
```

### Shadow Header (§2.13.4)

A copy of the file header stored at the **last page** of data.db (`page_count - 1`). Has `page_type = FileHeaderShadow`.

```rust
impl FileHeader {
    /// Write the shadow copy to the last page of the data file.
    /// Called during checkpoint, after writing page 0.
    pub fn write_shadow(
        &self,
        pool: &BufferPool,
        page_count: u64,
    ) -> Result<()>;

    /// Read the shadow copy from the last page.
    pub fn read_shadow(
        pool: &BufferPool,
        page_count: u64,
        page_size: u32,
    ) -> Result<Self>;
}
```

### Recovery Protocol

```rust
/// Attempt to read the file header. Falls back to shadow on corruption.
pub fn read_file_header_with_fallback(
    data_file: &std::fs::File,
    page_size: u32,
) -> Result<FileHeader> {
    // 1. Read page 0, verify checksum.
    // 2. If valid → return.
    // 3. If corrupt → read shadow from last page.
    // 4. If shadow valid → restore page 0 from shadow, log warning, return.
    // 5. If both corrupt → return Err (unrecoverable).
}
```

### Initialization for New Database

```
1. Create data.db file.
2. Write page 0 (file header).
3. Write page 1 (catalog B-tree root — empty leaf).
4. Write page 2 (shadow header copy).
5. page_count = 3.
6. catalog_root_page = PageId(1).
```

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum FileHeaderError {
    #[error("invalid magic: expected {expected:#010x}, found {found:#010x}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("checksum mismatch")]
    ChecksumMismatch,

    #[error("both primary and shadow headers are corrupt")]
    BothCorrupt,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```
