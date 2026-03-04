# 13 — Core Traits and Interfaces

Defines the key trait boundaries in the storage layer for testability and modularity.

## Trait: PageIO

Abstracts disk I/O for the buffer pool. Allows in-memory testing without real files.

```rust
/// Abstraction over page-level disk I/O.
/// Production: backed by a real file with pread/pwrite.
/// Test: backed by an in-memory Vec<Vec<u8>>.
pub trait PageIO: Send + Sync {
    /// Read a page from storage into `buf`.
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<()>;

    /// Write a page to storage from `buf`.
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<()>;

    /// Extend storage by one page, returning the new page's ID.
    fn extend(&self) -> Result<PageId>;

    /// Sync all pending writes to durable storage.
    fn sync(&self) -> Result<()>;

    /// Current number of pages.
    fn page_count(&self) -> u64;
}

/// File-backed implementation using pread/pwrite.
pub struct FilePageIO {
    file: std::fs::File,
    page_size: u32,
    page_count: RwLock<u64>,
}

impl PageIO for FilePageIO { /* ... */ }

/// In-memory implementation for testing.
pub struct MemPageIO {
    pages: RwLock<Vec<Vec<u8>>>,
    page_size: u32,
}

impl PageIO for MemPageIO { /* ... */ }
```

## Trait: WalSink

Abstracts WAL writing for testability.

```rust
/// Abstraction over WAL write + fsync.
pub trait WalSink: Send + Sync {
    /// Write a serialized WAL record frame and wait for durable commit.
    /// Returns the assigned LSN.
    fn write(&self, frame_bytes: Vec<u8>) -> impl Future<Output = Result<Lsn>> + Send;
}

/// Production implementation: sends to WalWriter via mpsc channel.
pub struct ChannelWalSink {
    sender: mpsc::Sender<WriteRequest>,
}

impl WalSink for ChannelWalSink { /* ... */ }

/// Test implementation: stores frames in memory.
pub struct MemWalSink {
    frames: Mutex<Vec<(Lsn, Vec<u8>)>>,
    next_lsn: AtomicU64,
}

impl WalSink for MemWalSink { /* ... */ }
```

## Trait: BTreeOps

High-level B-tree operations used by the engine. Not a trait in practice (just functions), but the interface contract.

```rust
/// B-tree operation results.
pub struct InsertResult {
    pub new_root: PageId,  // possibly changed if root split
}

pub struct DeleteResult {
    pub new_root: PageId,
    pub found: bool,
}

/// The B-tree functions (btree_insert, btree_delete, BTreeCursor, BTreeScan)
/// operate on the BufferPool directly. They are generic over any B-tree
/// in the data file — primary, secondary index, or catalog.
///
/// The caller specifies:
/// - root_page_id: which B-tree to operate on
/// - key_hint: how to parse keys (Fixed for primary, VariableWithSuffix for secondary)
/// - cell_data: the complete cell bytes to insert
```

## Interface Contracts

### Buffer Pool → PageIO

```
BufferPool uses PageIO for:
  - read_page: on cache miss during fetch_page_shared / fetch_page_exclusive
  - write_page: during checkpoint scatter-write
  - extend: when allocating new pages and free list is empty
  - sync: during checkpoint fsync step
```

### Engine → WAL

```
Engine (writer task) uses WalSink for:
  - write(frame_bytes): every commit, catalog mutation, checkpoint record
  Returns LSN assigned by the WAL writer.
```

### Engine → BufferPool → B-tree

```
Read path (any task):
  engine.pool() → BTreeCursor::new(pool, root, key_hint) → cursor.seek() / cursor.next()
  pool.fetch_page_shared() internally

Write path (writer task only):
  btree_insert(pool, root, cell, key_hint, free_list, lsn)
  btree_delete(pool, root, key, key_hint, free_list, lsn)
  pool.fetch_page_exclusive() internally
```

### Engine → Catalog

```
Read path (any task):
  engine.catalog() → CatalogCache (via ArcSwap, lock-free)

Write path (writer task only):
  CatalogManager.create_collection() / drop_collection() / create_index() / ...
  → modifies catalog B-tree + updates cache
  → SharedCatalog.update(new_cache)
```

## Dependency Graph

```
StorageEngine
  ├── BufferPool ← PageIO (FilePageIO)
  │     └── FrameSlot[] (parking_lot::RwLock each)
  ├── WalWriter ← WalSegment (file I/O)
  │     └── mpsc channel (WriteRequest)
  ├── WriterState (single writer task)
  │     ├── CatalogManager
  │     │     ├── Catalog B-tree (via BufferPool)
  │     │     └── CatalogCache
  │     ├── FreePageList (via BufferPool)
  │     └── ExternalHeap (via BufferPool)
  ├── SharedCatalog (ArcSwap<CatalogCache>)
  └── CheckpointManager
        ├── BufferPool (snapshot + mark-clean)
        └── DWB file I/O
```
