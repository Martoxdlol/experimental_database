---
active: true
iteration: 1
session_id: 
max_iterations: 20
completion_promise: "DONE"
started_at: "2026-03-05T00:01:27Z"
---

Implement the storage engine code (Layer 2) in the storage/ crate based on docs/plan/storage/*.md.

The storage crate is at storage/ and is currently empty (just a main.rs placeholder). Convert it to a library crate (main.rs → lib.rs). All design docs with exact struct definitions, method signatures, algorithms, edge cases, and tests are in docs/plan/storage/01-s1-backend.md through 13-s13-engine-facade.md, plus 14-latch-protocol.md and 15-diagrams.md.

This is a generic storage engine library — NO domain knowledge (no DocId, Ts, Document, Filter, MVCC, or any Layer 3+ types).

IMPLEMENTATION PHASES (build order matters — do them in sequence):

Phase A — Foundations (S1 + S2, no deps between them):
  S1: backend.rs — PageStorage trait, WalStorage trait, MemoryPageStorage, MemoryWalStorage, FilePageStorage, FileWalStorage
  S2: page.rs — PageHeader (zerocopy LE wrappers), PageType enum, SlottedPage, SlottedPageRef, CRC-32C checksum

Phase B — Core Infrastructure (S3 + S4 + S5, depends on Phase A):
  S3: buffer_pool.rs — BufferPool with clock eviction, SharedPageGuard, ExclusivePageGuard, pin_count as AtomicU32 outside Frame RwLock
  S4: free_list.rs — FreeList (LIFO page allocation/deallocation, page 0 never freed)
  S5: wal.rs — WalWriter with group commit (tokio mpsc), WalReader, WalIterator, WAL record type constants (0x01-0x0A)

Phase C — Data Structures (S6 + S7, depends on Phase B):
  S6: btree.rs — B+ tree with memcmp key ordering, insert/get/delete/scan, split/merge, latch coupling (crab protocol)
  S7: heap.rs — Large value storage with overflow page chains

Phase D — Durability (S8 + S9 + S10, depends on Phase B+C):
  S8: dwb.rs — Double-write buffer for torn-write protection (file-backed only, no-op for in-memory)
  S9: checkpoint.rs — Flush dirty pages through DWB, write checkpoint WAL record
  S10: recovery.rs — DWB restore + WAL replay via WalRecordHandler trait callback

Phase E — Maintenance + Catalog (S11 + S12, depends on Phase C):
  S11: vacuum.rs — Page-level entry removal from B-trees
  S12: catalog_btree.rs — Key format for collection/index catalog entries, serialize/deserialize

Phase F — Facade (S13, depends on all):
  S13: engine.rs — StorageEngine struct composing all sub-layers, FileHeader (zerocopy) on page 0

CARGO DEPENDENCIES (add to storage/Cargo.toml):
  zerocopy = { version = "0.8", features = ["derive"] }
  crc32fast = "1"
  parking_lot = "0.12"
  tokio = { version = "1", features = ["sync", "rt", "time", "io-util"] }
  thiserror = "2"
  [dev-dependencies]
  tempfile = "3"
  tokio = { version = "1", features = ["full", "test-util"] }

PLEASE USE EFFICIENT CONCURRENCY MODEL, USE ASYNC AND TOKIO

CRITICAL DESIGN CONSTRAINTS:
1. All on-disk formats use little-endian. PageHeader and FileHeader use zerocopy LE wrapper types.
2. Latch hierarchy (strict): component Mutex (free_list/heap/file_header) → page_table RwLock → frame RwLock. Never reverse.
3. Multiple frame locks: always ascending page_id order.
4. B-tree uses latch coupling (crab protocol) for concurrent access.
5. pin_count is AtomicU32 OUTSIDE the Frame RwLock (both guard types can decrement on Drop).
6. No frame locks held across I/O or await points.
7. Page 0 is the FileHeader page — never freed, never evicted.
8. Recovery is internal to open() — no public recover() method.
9. In-memory backends skip DWB, checkpoint scatter-write, and recovery (all no-ops).

FOR EACH PHASE: read the corresponding docs/plan/storage/XX-*.md file(s) thoroughly BEFORE writing code. They contain the exact types, algorithms, and test cases. Write all tests listed in the plan. Make sure cargo test passes before moving to the next phase.
