# Storage Engine (Layer 2)

A page-oriented storage engine library providing B+ trees, heap storage, write-ahead logging, and crash recovery. It operates on raw bytes and pages with no domain-specific knowledge — no DocId, Ts, Document, Filter, MVCC, or any higher-layer types.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  StorageEngine                      │  ← Public facade (engine.rs)
│    open / close / checkpoint / vacuum               │
├────────┬──────────┬──────────┬──────────────────────┤
│ BTree  │   Heap   │   WAL    │   Catalog BTree      │  ← Data structures
│ B+ tree│ overflow │ write-   │   key/value codec     │
│ index  │ chains   │ ahead log│   for collections     │
├────────┴──────────┴──────────┴──────────────────────┤
│              BufferPool (clock eviction)             │  ← Page cache
│         SharedPageGuard / ExclusivePageGuard         │
├──────────┬──────────┬───────────┬───────────────────┤
│ FreeList │   DWB    │Checkpoint │    Recovery        │  ← Page management
│ page     │ torn-    │ dirty     │    DWB restore     │
│ allocator│ write    │ page      │    + WAL replay    │
│          │ protect  │ flush     │                    │
├──────────┴──────────┴───────────┴───────────────────┤
│  SlottedPage / PageHeader (zerocopy, little-endian) │  ← On-disk page format
├─────────────────────────────────────────────────────┤
│  PageStorage (async trait) │  WalStorage (async trait) │  ← Backend abstraction
│    File / Memory           │    File / Memory          │
└─────────────────────────────────────────────────────┘
```

### Module Map

| Module | File | Purpose |
|--------|------|---------|
| **backend** | `backend.rs` | `PageStorage` and `WalStorage` async traits (`#[async_trait]`) + File/Memory implementations |
| **page** | `page.rs` | `PageHeader` (32-byte zerocopy), `PageType` enum, `SlottedPage` (mutable), `SlottedPageRef` (read-only), CRC-32C checksum |
| **buffer_pool** | `buffer_pool.rs` | In-memory page cache with clock (second-chance) eviction. RAII guards: `SharedPageGuard`, `ExclusivePageGuard`. Pin count as `AtomicU32` outside frame lock. |
| **free_list** | `free_list.rs` | LIFO page allocator. Pops from head or extends file. Page 0 is never freed. |
| **wal** | `wal.rs` | Write-ahead log with group commit (tokio mpsc). `WalWriter`, `WalReader`, `WalIterator`. Record types `0x01`–`0x0A`. |
| **btree** | `btree.rs` | B+ tree with memcmp key ordering. Insert/get/delete/scan with leaf and internal page splits. Latch coupling (crab protocol). |
| **heap** | `heap.rs` | Large value storage with overflow page chains. `HeapRef` (6 bytes: page_id + slot_id). |
| **dwb** | `dwb.rs` | Double-write buffer for torn-write protection. File-backed only; no-op for in-memory. |
| **checkpoint** | `checkpoint.rs` | Flush dirty buffer pool pages through DWB, write checkpoint WAL record. |
| **recovery** | `recovery.rs` | DWB restore + WAL replay via `WalRecordHandler` trait callback. |
| **vacuum** | `vacuum.rs` | Batch entry removal from B-trees. |
| **catalog_btree** | `catalog_btree.rs` | Key format + serialization for collection/index catalog entries. Supports `IndexType` (BTree/Gin/FullText/Vector) with aux root pages and opaque config. |
| **posting** | `posting.rs` | Sorted posting list codec for inverted indexes (GIN/FTS). Encode/decode + set operations (union/intersect/difference). |
| **engine** | `engine.rs` | `StorageEngine` facade composing all sub-layers. `FileHeader` (84-byte zerocopy) on page 0. |

## How It Works

### Page Format

Every page is a fixed-size buffer (default 8192 bytes). The first 32 bytes are a `PageHeader`:

```
Offset  Size  Field
──────  ────  ─────
  0       4   page_id (u32 LE)
  4       1   page_type (BTreeLeaf=1, BTreeInternal=2, Heap=3, Overflow=4, Free=5, FileHeader=6)
  5       1   flags
  6       2   slot_count (u16 LE)
  8       4   free_space_start (u32 LE) — offset where next cell starts
 12       4   free_space_end (u32 LE) — end of free region
 16       4   prev_or_ptr (u32 LE) — context-dependent: parent page, next-free page, overflow ptr
 20       4   checksum (CRC-32C of the entire page with this field zeroed)
 24       8   lsn (u64 LE) — log sequence number of last modification
```

After the header, the page uses a **slotted page** layout: a slot directory at the front, cells (data) growing backward from the end.

### Buffer Pool

The buffer pool is an in-memory cache of pages. Frames hold page data plus metadata (dirty flag, pin count, ref bit). Key properties:

- **Clock eviction**: second-chance algorithm skips pinned and dirty frames
- **Dirty pages are never evicted** — they stay in the pool until checkpoint flushes them
- **RAII guards**: `SharedPageGuard` (read) and `ExclusivePageGuard` (write) manage pin counts automatically
- **Pin count** is `AtomicU32` outside the frame `RwLock`, so both guard types can decrement on `Drop` without acquiring the write lock
- **Latch hierarchy**: component Mutex → page_table RwLock → frame RwLock (never reversed)

### B+ Tree

B+ trees store sorted key-value pairs across leaf pages, with internal pages for navigation:

- **Leaf pages**: cells = `key_len(u16) || key || value`
- **Internal pages**: cells = `key_len(u16) || key || child_page_id(u32)`
- **memcmp ordering**: keys are compared as raw bytes
- **Splits**: when a leaf/internal page overflows, it splits at the midpoint; the parent is updated
- **Root splits**: create a new root page with two children
- **Scans**: forward and backward range scans with inclusive/exclusive bounds

### Write-Ahead Log

The WAL ensures durability. Every mutation is logged before it takes effect:

- **Group commit**: multiple concurrent writers batch their records into a single fsync
- **Segmented files**: WAL is split into fixed-size segments (default 64 MB)
- **Record format**: `len(u32 LE) | crc32(u32 LE) | record_type(u8) | payload(bytes)`
- **Record types**: `TX_COMMIT(0x01)`, `CHECKPOINT(0x02)`, `CREATE_COLLECTION(0x03)`, `DROP_COLLECTION(0x04)`, `CREATE_INDEX(0x05)`, `DROP_INDEX(0x06)`, `INDEX_READY(0x07)`, `VACUUM(0x08)`, `VISIBLE_TS(0x09)`, `ROLLBACK_VACUUM(0x0A)`

### Checkpoint & Recovery

**Checkpoint** flushes dirty pages to durable storage:
1. Record `checkpoint_lsn = current WAL LSN`
2. Snapshot all dirty pages from buffer pool
3. Write through DWB (double-write buffer) for torn-write protection
4. Scatter-write pages to data file
5. Mark pages clean in buffer pool
6. Write checkpoint WAL record

**Recovery** runs automatically on `open()`:
1. DWB restore: if DWB file exists with valid header, restore any torn pages
2. WAL replay: iterate records from `checkpoint_lsn` to end
3. Call `WalRecordHandler::handle_record()` for each record (letting higher layers rebuild state)

### Heap Storage

For values too large to fit inline in a B-tree cell:

- Data is stored in heap pages using slotted page format
- If data exceeds the available space, **overflow page chains** are used (linked via `prev_or_ptr`)
- Returns a `HeapRef` (6 bytes: `page_id(u32) + slot_id(u16)`) that can be stored in a B-tree value

## Usage Guide

### Add Dependency

```toml
[dependencies]
storage = { path = "../storage" }
```

### Opening a Database

#### File-backed (durable)

```rust
use storage::engine::{StorageConfig, StorageEngine};
use storage::recovery::NoOpHandler;

let config = StorageConfig {
    page_size: 8192,
    memory_budget: 256 * 1024 * 1024, // 256 MB buffer pool
    ..Default::default()
};

// First argument is a directory path. The engine creates:
//   <path>/data    — page storage file
//   <path>/wal/    — WAL segment files
//   <path>/dwb     — double-write buffer
let mut handler = NoOpHandler;
let engine = StorageEngine::open(
    std::path::Path::new("/tmp/mydb"),
    config,
    &mut handler,
).await.unwrap();

assert!(engine.is_durable());
```

#### In-memory (ephemeral)

```rust
use storage::engine::{StorageConfig, StorageEngine};

let engine = StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap();
assert!(!engine.is_durable());
```

#### Custom backends

```rust
use std::sync::Arc;
use storage::backend::{MemoryPageStorage, MemoryWalStorage, PageStorage, WalStorage};
use storage::engine::{StorageConfig, StorageEngine};

let page_storage: Arc<dyn PageStorage> = Arc::new(MemoryPageStorage::new(8192));
let wal_storage: Arc<dyn WalStorage> = Arc::new(MemoryWalStorage::new());

let engine = StorageEngine::open_with_backend(
    page_storage,
    wal_storage,
    StorageConfig::default(),
    None, // no WalRecordHandler for fresh database
).await.unwrap();
```

### B-Tree Operations

```rust
use std::ops::Bound;
use storage::btree::ScanDirection;

// Create a new B-tree (allocates a root page).
let handle = engine.create_btree().await.unwrap();

// Insert key-value pairs (keys and values are raw bytes).
handle.insert(b"user:001", b"Alice").await.unwrap();
handle.insert(b"user:002", b"Bob").await.unwrap();
handle.insert(b"user:003", b"Charlie").await.unwrap();

// Point lookup.
let value = handle.get(b"user:002").await.unwrap();
assert_eq!(value, Some(b"Bob".to_vec()));

// Delete.
let deleted = handle.delete(b"user:003").await.unwrap();
assert!(deleted);

// Range scan (forward, inclusive bounds).
use tokio_stream::StreamExt;
let results: Vec<_> = handle
    .scan(
        Bound::Included(b"user:001".as_slice()),
        Bound::Included(b"user:002".as_slice()),
        ScanDirection::Forward,
    )
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

assert_eq!(results.len(), 2);
assert_eq!(results[0].0, b"user:001");
assert_eq!(results[1].0, b"user:002");

// Backward scan.
let results: Vec<_> = handle
    .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Backward)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
// Results are in descending key order.

// Save the root page to reopen this B-tree later.
// IMPORTANT: Save root_page() AFTER all inserts — root may change due to splits.
let root_page = handle.root_page();

// Reopen an existing B-tree by root page.
let handle2 = engine.open_btree(root_page);
assert_eq!(handle2.get(b"user:001").await.unwrap(), Some(b"Alice".to_vec()));
```

### Heap Storage (Large Blobs)

```rust
// Store a large value in the heap.
let large_data = vec![0u8; 50_000];
let href = engine.heap_store(&large_data).await.unwrap();

// Load it back.
let loaded = engine.heap_load(href).await.unwrap();
assert_eq!(loaded, large_data);

// Store the HeapRef in a B-tree for later retrieval.
let btree = engine.create_btree().await.unwrap();
btree.insert(b"doc:big", &href.to_bytes()).await.unwrap();

// Later: retrieve HeapRef from B-tree, load from heap.
use storage::heap::HeapRef;
let href_bytes = btree.get(b"doc:big").await.unwrap().unwrap();
let href = HeapRef::from_bytes(href_bytes[..6].try_into().unwrap());
let data = engine.heap_load(href).await.unwrap();

// Free when no longer needed.
engine.heap_free(href).await.unwrap();
```

### Write-Ahead Log

```rust
use storage::wal::{WAL_RECORD_TX_COMMIT, WAL_RECORD_CREATE_COLLECTION};

// Append WAL records (async — uses group commit).
let lsn1 = engine.append_wal(WAL_RECORD_CREATE_COLLECTION, b"users").await.unwrap();
let lsn2 = engine.append_wal(WAL_RECORD_TX_COMMIT, b"tx-001").await.unwrap();

// Read WAL records from a given LSN.
let mut iter = engine.read_wal_from(lsn1);
while let Some(Ok(record)) = iter.next() {
    println!("LSN={} type=0x{:02X} payload={:?}",
        record.lsn, record.record_type, record.payload);
}
```

### Checkpoint & Close

```rust
// Manual checkpoint: flush dirty pages to disk, write checkpoint WAL record.
engine.checkpoint().await.unwrap();

// Close: runs a final checkpoint (if durable), writes file header, syncs.
engine.close().await.unwrap();
```

### Crash Recovery

Recovery happens automatically during `open()`. To receive replayed WAL records, implement `WalRecordHandler`:

```rust
use storage::recovery::WalRecordHandler;
use storage::wal::WalRecord;

struct MyHandler;

impl WalRecordHandler for MyHandler {
    fn handle_record(&mut self, record: &WalRecord) -> std::io::Result<()> {
        // Process replayed WAL record.
        // This is called for each record after the last checkpoint LSN.
        match record.record_type {
            0x03 => { /* CREATE_COLLECTION: rebuild catalog */ }
            0x01 => { /* TX_COMMIT: replay transaction */ }
            _ => {}
        }
        Ok(())
    }
}

let engine = StorageEngine::open(
    std::path::Path::new("/tmp/mydb"),
    StorageConfig::default(),
    &mut MyHandler,
).await.unwrap();
```

### Vacuum (Garbage Collection)

```rust
use storage::vacuum::VacuumEntry;

let btree = engine.create_btree().await.unwrap();
btree.insert(b"key1", b"val1").await.unwrap();
btree.insert(b"key2", b"val2").await.unwrap();

// Remove entries from B-trees in batch (idempotent).
let entries = vec![
    VacuumEntry {
        btree_root: btree.root_page(),
        key: b"key1".to_vec(),
    },
];
let removed = engine.vacuum(&entries).await.unwrap();
assert_eq!(removed, 1);
```

### Catalog B-Tree (Collection/Index Metadata)

There is no `list_btrees()` API — B-tree discovery is hierarchical through the catalog. Each B-tree is identified solely by its root `PageId`. The only roots stored in a fixed location are the two catalog B-trees in the file header. All other B-trees (collection data, indexes) are discovered by looking them up in the catalog.

The engine pre-creates two catalog B-trees whose root pages are persisted in the file header:

| File header field | Key format | Value | Role |
|---|---|---|---|
| `catalog_root_page` | `entity_type \|\| entity_id` | Full serialized `CollectionEntry` / `IndexEntry` | **Primary index** — lookup by numeric ID |
| `catalog_name_root_page` | `entity_type \|\| name` | `entity_id` (8 bytes) | **Secondary index** — lookup by name, returns ID |

The name index stores only the entity ID, acting as a pointer back to the full entry in the ID catalog. The lookup flow is: **name → id → full entry → root page → open B-tree**.

#### Registering a collection and index

```rust
use storage::catalog_btree::{
    self, CatalogEntityType, CatalogIndexState, CollectionEntry, IndexEntry, IndexType,
};

let fh = engine.file_header();
let catalog = engine.open_btree(fh.catalog_root_page.get());
let name_idx = engine.open_btree(fh.catalog_name_root_page.get());

// Register a collection (in both ID and name catalogs).
let col = CollectionEntry {
    collection_id: 1,
    name: "users".to_string(),
    primary_root_page: data_btree.root_page(),
    doc_count: 0,
};

let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, col.collection_id);
catalog.insert(&id_key, &catalog_btree::serialize_collection(&col)).await.unwrap();

let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, &col.name);
name_idx.insert(&name_key, &catalog_btree::serialize_name_value(col.collection_id)).await.unwrap();

// Register an index (same pattern — insert into both catalogs).
let idx = IndexEntry {
    index_id: 1,
    collection_id: 1,
    name: "users_tags_gin".to_string(),
    field_paths: vec![vec!["tags".to_string()]],
    root_page: posting_btree.root_page(),
    state: CatalogIndexState::Ready,
    index_type: IndexType::Gin,
    aux_root_pages: vec![pending_btree.root_page(), docterm_btree.root_page()],
    config: vec![],
};

let idx_id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx.index_id);
catalog.insert(&idx_id_key, &catalog_btree::serialize_index(&idx)).await.unwrap();

let idx_name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &idx.name);
name_idx.insert(&idx_name_key, &catalog_btree::serialize_name_value(idx.index_id)).await.unwrap();
```

#### Looking up by name after reopen

```rust
// After engine.close() + reopen:
let fh = engine.file_header();
let catalog = engine.open_btree(fh.catalog_root_page.get());
let name_idx = engine.open_btree(fh.catalog_name_root_page.get());

// name → id
let lookup = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
let id_bytes = name_idx.get(&lookup).await.unwrap().expect("not found");
let collection_id = catalog_btree::deserialize_name_value(&id_bytes);

// id → full entry
let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, collection_id);
let col_bytes = catalog.get(&id_key).await.unwrap().expect("not found");
let col = catalog_btree::deserialize_collection(&col_bytes).unwrap();

// full entry → open data B-tree
let data_btree = engine.open_btree(col.primary_root_page);
let val = data_btree.get(b"some_key").await.unwrap();
```

#### Enumerating all collections or indexes

```rust
use std::ops::Bound;
use storage::btree::ScanDirection;

use tokio_stream::StreamExt;

// Scan all collections (prefix 0x01 in the ID catalog).
let prefix = catalog_btree::collection_id_scan_prefix();
let collections: Vec<_> = catalog
    .scan(
        Bound::Included(prefix.as_slice()),
        Bound::Unbounded,
        ScanDirection::Forward,
    )
    .take_while(|r| r.as_ref().map(|(k, _)| k[0] == prefix[0]).unwrap_or(true))
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

// Scan all indexes (prefix 0x02).
let idx_prefix = catalog_btree::index_id_scan_prefix();
let indexes: Vec<_> = catalog
    .scan(
        Bound::Included(idx_prefix.as_slice()),
        Bound::Unbounded,
        ScanDirection::Forward,
    )
    .take_while(|r| r.as_ref().map(|(k, _)| k[0] == idx_prefix[0]).unwrap_or(true))
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
```

### File Header Access

```rust
use zerocopy::byteorder::U64;

// Read current file header.
let fh = engine.file_header();
println!("page_size={}", fh.page_size.get());
println!("checkpoint_lsn={}", fh.checkpoint_lsn.get());
println!("catalog_root={}", fh.catalog_root_page.get());

// Update file header fields atomically.
engine.update_file_header(|fh| {
    fh.visible_ts = U64::new(12345);
    fh.next_collection_id = U64::new(100);
}).unwrap();
```

### Configuration Reference

| Field | Default | Description |
|-------|---------|-------------|
| `page_size` | 8192 | Page size in bytes. Must be consistent across open/close. |
| `memory_budget` | 256 MB | Buffer pool memory. Frame count = `memory_budget / page_size`. |
| `wal_segment_size` | 64 MB | Maximum WAL segment file size before rotation. |
| `checkpoint_wal_threshold` | 64 MB | WAL bytes written before auto-checkpoint (for higher layers). |
| `checkpoint_interval` | 300s | Time between auto-checkpoint checks (for higher layers). |

## Design Constraints

1. **All on-disk formats are little-endian.** PageHeader and FileHeader use zerocopy LE wrapper types.
2. **Latch hierarchy (strict):** component Mutex (free_list/heap/file_header) → page_table RwLock → frame RwLock. Never reversed.
3. **Multiple frame locks:** always in ascending page_id order.
4. **B-tree uses latch coupling** (crab protocol) for concurrent access.
5. **pin_count is AtomicU32 OUTSIDE the Frame RwLock** — both guard types can decrement on Drop.
6. **No frame locks held across I/O or await points.**
7. **Page 0 is the FileHeader page** — never freed, never evicted.
8. **Recovery is internal to open()** — no public `recover()` method.
9. **In-memory backends skip** DWB, checkpoint scatter-write, and recovery (all no-ops).
10. **Dirty pages are never evicted** from the buffer pool — checkpoint flushes them.

## Testing

```bash
# Run all tests (148 unit + 21 integration)
cargo test -p storage

# Run only integration tests
cargo test -p storage --test integration

# Run a specific test
cargo test -p storage -- test_full_lifecycle_1000_entries
```
