# Storage Layer Implementation Plan — Overview

## Scope

This plan covers the full storage engine described in DESIGN.md §2, including:

- Page format (slotted pages) and page types
- WAL (write-ahead log): segments, records, writer, reader, group commit
- Buffer pool with per-frame RwLock, clock eviction, positional I/O
- Free space management (free page list + heap free space map)
- B-tree (primary clustered + secondary indexes): insert, split, delete, merge, scan
- External heap for large documents + overflow pages
- Catalog B-tree (collections, indexes) + in-memory cache
- Double-write buffer, checkpoint, crash recovery
- File header + shadow copy
- Key encoding (order-preserving, self-delimiting)
- StorageEngine facade tying it all together

## Out of Scope (for this plan)

- Transaction layer (MVCC read/write sets, OCC validation, commit log) — separate plan
- Query engine (plan selection, post-filters) — separate plan
- Subscriptions and replication — separate plan
- Network/API layer — separate plan

## Module Layout

```
src/
  storage/
    mod.rs                 # Re-exports, StorageEngine
    types.rs               # PageId, FrameId, LSN, DocId, CollectionId, IndexId, etc.
    page.rs                # Page header, slotted page read/write, cell operations
    wal/
      mod.rs               # Re-exports
      segment.rs           # Segment file format, header, open/create
      record.rs            # WAL record types, serialization/deserialization
      writer.rs            # WalWriter (group commit, mpsc channel)
      reader.rs            # WalReader (forward scan, CRC verification)
    buffer_pool.rs         # BufferPool, FrameSlot, SharedPageGuard, ExclusivePageGuard
    free_list.rs           # FreePageList (linked list in page store)
    btree/
      mod.rs               # Re-exports
      node.rs              # BTreeNode abstraction over slotted pages
      cursor.rs            # BTreeCursor (seek, next, prev)
      insert.rs            # Insert + split
      delete.rs            # Delete + merge/redistribute
      scan.rs              # Range scan iterator
    heap.rs                # ExternalHeap (large doc storage) + overflow pages
    catalog.rs             # CatalogBTree + CatalogCache (in-memory)
    key_encoding.rs        # Order-preserving key encoding/decoding per §3.4
    checkpoint.rs          # Checkpoint + DWB (double-write buffer)
    recovery.rs            # Crash recovery (DWB repair + WAL replay)
    file_header.rs         # FileHeader (page 0) + shadow copy
    engine.rs              # StorageEngine — top-level facade
```

## Implementation Phases

### Phase A: Foundation (types, page format, WAL, key encoding)
1. `types.rs` — core type aliases and newtypes
2. `page.rs` — slotted page read/write
3. `key_encoding.rs` — order-preserving encoding
4. `wal/record.rs` — WAL record serialization
5. `wal/segment.rs` — segment file I/O
6. `wal/writer.rs` — group commit writer
7. `wal/reader.rs` — forward-scan reader

### Phase B: Buffer pool + free list + file header
1. `buffer_pool.rs` — frame management, guards, clock eviction
2. `free_list.rs` — free page linked list
3. `file_header.rs` — page 0 + shadow

### Phase C: B-tree + heap + catalog
1. `btree/node.rs` — node abstraction
2. `btree/cursor.rs` — seek/scan cursor
3. `btree/insert.rs` — insert with split
4. `btree/delete.rs` — delete with merge
5. `btree/scan.rs` — range scan iterator
6. `heap.rs` — external heap + overflow
7. `catalog.rs` — catalog B-tree + cache

### Phase D: Checkpoint + recovery + engine
1. `checkpoint.rs` — DWB + fuzzy checkpoint
2. `recovery.rs` — DWB repair + WAL replay
3. `engine.rs` — StorageEngine facade

## Dependencies (Rust crates)

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
parking_lot = "0.12"
crc32c = "0.6"                # hardware-accelerated CRC-32C
bson = "2"                     # BSON encoding
ulid = "1"                     # ULID generation
bytes = "1"                    # Buf/BufMut for binary encoding
thiserror = "2"                # Error types

[dev-dependencies]
tempfile = "3"
tokio-test = "0.4"
```
