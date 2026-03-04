# 14 — Implementation Phases

Step-by-step build order. Each phase produces testable, compiling code.

## Phase A: Foundation

Build the primitives that everything else depends on. No disk I/O yet — just data structures and encoding.

### A1: Types (`types.rs`)
- All newtypes: `PageId`, `FrameId`, `Lsn`, `Timestamp`, `CollectionId`, `IndexId`, `DocId`, `TxId`
- Enums: `PageType`, `WalRecordType`, `IndexState`, `DatabaseState`, `OpType`
- Constants: page sizes, header sizes, magic numbers
- `HeapRef`, `FieldPath`, `EncodedKey`
- **Tests**: basic construction, Ord for newtypes, Lsn arithmetic

### A2: Page Format (`page.rs`)
- `PageHeader` read/write (32-byte binary format)
- `SlottedPage` / `SlottedPageMut` on `&[u8]` / `&mut [u8]`
- Cell operations: `insert_cell`, `delete_cell`, `update_cell`, `compact`
- Slot directory management
- CRC-32C checksum compute + verify
- **Tests**: see test plan §T1

### A3: Key Encoding (`key_encoding.rs`)
- `encode_scalar` / `decode_scalar` for all types
- `encode_primary_key` / `decode_primary_key`
- `encode_secondary_key` / `decode_secondary_key_suffix`
- `successor_prefix` / `successor_key`
- `extract_field_value` from BSON documents
- **Tests**: see test plan §T2

### A4: WAL Records (`wal/record.rs`)
- Binary serialization/deserialization for all record types
- `encoding` module (read/write primitives)
- CRC-32C computation in frame header
- **Tests**: round-trip serialize/deserialize for every record type

### A5: WAL Segments (`wal/segment.rs`)
- `SegmentHeader` read/write
- `WalSegment::create`, `open_read`, `open_append`
- `append()`, `sync()`, `should_rollover()`
- `list_segments()`, `segment_filename()`
- **Tests**: create segment, write records, read back

### A6: WAL Writer (`wal/writer.rs`)
- `WalWriter` with mpsc channel and group commit loop
- `WriteRequest` with oneshot response
- Segment rollover
- `WalWriter::write()` async helper
- **Tests**: single write, concurrent writes, group commit batching, rollover

### A7: WAL Reader (`wal/reader.rs`)
- `WalReader::open`, `next()`
- Forward scan with CRC verification
- Cross-segment scanning
- End-of-data detection
- **Tests**: read back what writer wrote, detect corruption

## Phase B: Buffer Pool + Free List + File Header

### B1: PageIO Trait + Implementations (`traits.rs` or inline)
- `PageIO` trait
- `FilePageIO` (pread/pwrite)
- `MemPageIO` (in-memory for testing)
- **Tests**: read/write round-trip, extend

### B2: Buffer Pool (`buffer_pool.rs`)
- `BufferPool::new` with `PageIO`
- `FrameSlot` with `parking_lot::RwLock`
- `SharedPageGuard` / `ExclusivePageGuard` with RAII drop
- `fetch_page_shared` / `fetch_page_exclusive`
- Clock eviction (`find_victim`)
- `new_page`, `flush_page`
- `snapshot_dirty_frames`, `mark_clean_if_lsn`
- **Tests**: see test plan §T3

### B3: Free Page List (`free_list.rs`)
- `FreePageList::new`, `push`, `pop`
- Read/write via buffer pool
- **Tests**: push/pop round-trip, empty list, multi-page chain

### B4: File Header (`file_header.rs`)
- `FileHeader` serialize/deserialize
- Checksum verification
- Shadow header write/read
- `read_file_header_with_fallback`
- New database initialization
- **Tests**: round-trip, checksum verification, shadow fallback

## Phase C: B-Tree + Heap + Catalog

### C1: B-Tree Node (`btree/node.rs`)
- `BTreeNode` static methods for internal + leaf operations
- Cell parsing for primary keys (fixed 24-byte)
- Cell parsing for variable-length secondary keys
- Binary search within pages
- **Tests**: insert/read cells, binary search correctness

### C2: B-Tree Cursor (`btree/cursor.rs`)
- `BTreeCursor::new`, `seek`, `seek_first`, `seek_last`
- `next()` with sibling traversal
- `is_valid`, `key`, `cell_data`
- **Tests**: seek to exact key, seek to range, scan forward

### C3: B-Tree Insert (`btree/insert.rs`)
- `btree_insert` with path tracking
- Leaf insert (no split)
- Leaf split (allocate new page, redistribute, propagate)
- Root split (allocate new root)
- **Tests**: see test plan §T4

### C4: B-Tree Delete (`btree/delete.rs`)
- `btree_delete`
- Leaf delete (no merge)
- Redistribute from sibling
- Merge with sibling
- Root collapse
- **Tests**: delete with redistribution, merge, empty tree

### C5: B-Tree Scan (`btree/scan.rs`)
- `BTreeScan::new`, `next()`
- `BTreeReverseScan`
- Upper bound checking
- **Tests**: scan entire tree, scan range, reverse scan

### C6: External Heap (`heap.rs`)
- `ExternalHeap::store`, `read`, `delete`, `replace`
- Single-page and multi-page (overflow) paths
- `HeapFreeSpaceMap`
- **Tests**: see test plan §T5

### C7: Catalog (`catalog.rs`)
- `CatalogCache` with all lookup methods
- `CatalogManager` with create/drop operations
- `SharedCatalog` with ArcSwap
- Catalog B-tree key/value serialization
- **Tests**: create collection/index, lookup, drop, cache consistency

## Phase D: Checkpoint + Recovery + Engine

### D1: Checkpoint (`checkpoint.rs`)
- DWB write/read/clear
- `CheckpointManager::run_checkpoint`
- Checkpoint background task with timer
- **Tests**: see test plan §T6

### D2: Recovery (`recovery.rs`)
- `recover_dwb` (torn page repair)
- `replay_wal` (full WAL replay)
- `recover_database` (orchestration)
- `meta.json` read/write
- **Tests**: see test plan §T7

### D3: Engine (`engine.rs`)
- `StorageEngine::open`, `shutdown`
- Writer task loop
- Read/write operation routing
- **Tests**: see test plan §T8

## Estimated Sizes

| Phase | Files | ~LoC | Key complexity |
|-------|-------|------|----------------|
| A | 7 | 2500 | WAL serialization, key encoding |
| B | 4 | 1500 | Buffer pool concurrency, clock eviction |
| C | 7 | 3000 | B-tree split/merge, heap overflow |
| D | 3 | 1500 | Recovery orchestration, engine lifecycle |
| **Total** | **21** | **~8500** | |
