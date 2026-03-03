# Storage Layer Implementation Plan

## Module Map

The storage layer is built bottom-up. Each module depends only on modules above it in this list:

```
src/
  storage/
    mod.rs              # Public re-exports
    types.rs            # Shared types (PageId, Lsn, FrameId, etc.)
    page.rs             # Page format: header, slotted page read/write
    wal/
      mod.rs            # Public WAL API
      record.rs         # WAL record types, serialization, deserialization
      segment.rs        # Segment file I/O (open, append, read, rollover)
      writer.rs         # Single-writer WAL writer with group commit
      reader.rs         # WAL reader (forward scan from LSN)
    buffer_pool.rs      # Buffer pool: frame management, clock eviction, pin/unpin
    dwb.rs              # Double-write buffer (checkpoint torn-write protection)
    freelist.rs         # Free page list management
    heap.rs             # External heap for large documents
    btree/
      mod.rs            # Public B-tree API
      node.rs           # Internal + leaf node operations (search, insert cell, split)
      cursor.rs         # B-tree cursor (seek, next, prev) for range scans
      ops.rs            # High-level operations (get, insert, delete, range_scan)
    catalog.rs          # Catalog B-tree: collection + index metadata
    checkpoint.rs       # Checkpoint orchestration (DWB + flush + WAL reclaim)
    recovery.rs         # Crash recovery (DWB repair + WAL replay)
    engine.rs           # StorageEngine: ties everything together
```

## Dependency Graph

```
types.rs ──────────────────────────────────────────────────────────────
   │
page.rs ───────────────────────────────────────────────────────────────
   │
   ├── wal/ (record.rs → segment.rs → writer.rs, reader.rs)
   │
   ├── buffer_pool.rs ─── (uses page.rs for checksums, types for PageId)
   │      │
   │      ├── freelist.rs (uses buffer_pool to read/write free pages)
   │      │
   │      ├── heap.rs (uses buffer_pool for heap page I/O)
   │      │
   │      ├── btree/ (uses buffer_pool for all page access)
   │      │     │
   │      │     └── catalog.rs (uses btree for catalog B-tree operations)
   │      │
   │      ├── dwb.rs (reads/writes data.dwb, restores torn pages)
   │      │
   │      └── checkpoint.rs (orchestrates dwb + buffer_pool flush + WAL reclaim)
   │
   └── recovery.rs (uses dwb, wal/reader, buffer_pool, btree, catalog)
         │
         └── engine.rs (owns all components, exposes StorageEngine)
```

## Implementation Order

1. **types.rs** — foundational types
2. **page.rs** — page format (read/write headers, slot directory, cells)
3. **wal/** — WAL subsystem (record → segment → writer + reader)
4. **buffer_pool.rs** — frame management with clock eviction
5. **freelist.rs** — free page tracking
6. **btree/** — B-tree implementation (node → cursor → ops)
7. **heap.rs** — external document storage
8. **catalog.rs** — catalog B-tree operations
9. **dwb.rs** — double-write buffer
10. **checkpoint.rs** — checkpoint orchestration
11. **recovery.rs** — crash recovery
12. **engine.rs** — top-level StorageEngine
