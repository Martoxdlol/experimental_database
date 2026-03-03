# 14 — Implementation Phases

The storage layer is built bottom-up in four phases. Each phase produces runnable, tested code.

---

## Phase A: Foundations (types + page + WAL)

**Goal**: a working WAL that can write, read, and verify records.

| Step | File | What | Tests |
|------|------|------|-------|
| A1 | `types.rs` | All shared types, constants, enums | Compile check |
| A2 | `page.rs` | `PageHeader` read/write, `SlottedPage` init/insert/remove/compact, checksum | Unit: insert cells, verify checksums, compact, page full |
| A3 | `wal/record.rs` | `WalRecord` enum, serialize/deserialize for all record types | Unit: round-trip each record type |
| A4 | `wal/segment.rs` | `WalSegment` create/open/append/read, `SegmentHeader` | Integration: create segment, append frames, read back |
| A5 | `wal/writer.rs` | `WalWriter` with group commit, segment rollover | Integration: concurrent writes, verify fsync ordering |
| A6 | `wal/reader.rs` | `WalReader` scan from LSN, segment discovery, reclaim | Integration: write N records, scan them, reclaim old segments |
| A7 | `key_encoding.rs` | All encode/decode functions, successor_prefix | Unit: round-trip each type, verify sort order with memcmp |

**Deliverable**: WAL writes and reads end-to-end. Key encoding is correct and order-preserving.

---

## Phase B: Buffer Pool + B-Tree

**Goal**: a working B-tree backed by the buffer pool, with insert/get/scan/delete.

| Step | File | What | Tests |
|------|------|------|-------|
| B1 | `buffer_pool.rs` | Frame management, fetch/pin/unpin, clock eviction (clean only), disk I/O | Unit with `MemPageIO`: pin/unpin, eviction, checksum verify |
| B2 | `freelist.rs` | Free page list push/pop | Unit: push/pop cycles |
| B3 | `btree/node.rs` | Leaf + internal node ops: search, insert_cell, split | Unit: insert cells into a single page, verify ordering, split |
| B4 | `btree/cursor.rs` | Seek + advance across leaf chain | Integration: insert N keys, seek to middle, scan forward/backward |
| B5 | `btree/ops.rs` | `BTree::get`, `insert`, `delete`, `range_scan` | Integration: insert 10K keys, get each, range scan, delete some |
| B6 | `heap.rs` | External heap store/read/delete, overflow chains | Integration: store small + large docs, read back, delete |

**Deliverable**: standalone B-tree that can insert, get, range scan, and handle large documents.

---

## Phase C: Catalog + Checkpoint + Recovery

**Goal**: a full storage engine that survives crashes.

| Step | File | What | Tests |
|------|------|------|-------|
| C1 | `catalog.rs` | Catalog B-tree, `CatalogCache`, create/drop collection/index | Integration: create collections, verify cache |
| C2 | `dwb.rs` | DWB write/read/truncate/recovery | Unit: write pages, read back, simulate torn write |
| C3 | `checkpoint.rs` | Full checkpoint protocol (DWB → scatter-write → WAL reclaim) | Integration: dirty some pages, checkpoint, verify clean |
| C4 | `recovery.rs` | DWB recovery + WAL replay | Integration: crash simulation (write WAL, don't checkpoint, recover) |

**Deliverable**: storage engine that creates collections, writes documents, checkpoints, and recovers from crashes.

---

## Phase D: Engine Integration

**Goal**: `StorageEngine` as the single entry point for all storage operations.

| Step | File | What | Tests |
|------|------|------|-------|
| D1 | `engine.rs` | `StorageEngine::open/create/shutdown` | Integration: create DB, reopen, verify state |
| D2 | `engine.rs` | `get_document`, `index_scan`, `apply_mutations` | Integration: insert docs via apply_mutations, query back |
| D3 | `engine.rs` | Background checkpoint task, early checkpoint trigger | Integration: fill buffer pool, verify early checkpoint fires |
| D4 | End-to-end | Full flow: create collection → insert → query → checkpoint → crash → recover → query | Crash recovery test |

**Deliverable**: complete storage layer ready for the transaction layer to build on top.

---

## Out of Scope (for later phases)

- Transaction layer (write set, read set, OCC, subscriptions)
- Query engine (query planning, post-filters)
- Replication
- Network protocol
- Authentication
- Vacuuming (partial — the storage ops exist, scheduling doesn't)
