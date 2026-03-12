# Storage Engine Implementation Plan — Overview

## Scope

Layer 2: Generic storage engine with **no domain knowledge**. Operates on bytes, pages, raw keys/values. No DocId, Ts, Document, Filter, MVCC, or any Layer 3+ types.

## Sub-Layer Organization (Bottom-Up Build Order)

| # | Sub-Layer | File | Dependencies | Testable Alone? |
|---|-----------|------|-------------|-----------------|
| S1 | Backend Traits | `backend.rs` | none | Yes |
| S2 | Page Format | `page.rs` | none | Yes |
| S3 | Buffer Pool | `buffer_pool.rs` | S1, S2 | Yes |
| S4 | Free List | `free_list.rs` | S3 | Yes |
| S5 | WAL | `wal.rs` | S1 | Yes |
| S6 | B+ Tree | `btree.rs` | S2, S3 | Yes |
| S7 | Heap | `heap.rs` | S2, S3 | Yes |
| S8 | Double-Write Buffer | `dwb.rs` | S1 | Yes |
| S9 | Checkpoint | `checkpoint.rs` | S3, S5, S8 | Yes (with mocks) |
| S10 | Recovery | `recovery.rs` | S1, S5, S8 | Yes |
| S11 | Vacuum | `vacuum.rs` | S6 | Yes (with B-tree) |
| S12 | Catalog B-Tree Schema | `catalog_btree.rs` | S6 | Yes |
| S13 | StorageEngine Facade | `engine.rs` | All | Integration |

## Implementation Phases

### Phase A: Foundations (S1 + S2) — No Dependencies
Build and fully test backend traits and page format independently.

### Phase B: Core Infrastructure (S3 + S4 + S5) — Depends on Phase A
Buffer pool needs S1+S2. Free list needs S3. WAL needs S1.

### Phase C: Data Structures (S6 + S7) — Depends on Phase B
B+ tree needs S2+S3. Heap needs S2+S3.

### Phase D: Durability (S8 + S9 + S10) — Depends on Phase B+C
DWB needs S1. Checkpoint needs S3+S5+S8. Recovery needs S1+S5+S8.

### Phase E: Maintenance + Catalog (S11 + S12) — Depends on Phase C
Vacuum needs S6. Catalog schema needs S6.

### Phase F: Facade (S13) — Depends on All
Compose everything into StorageEngine.

## File Map

```
storage/
  mod.rs              — re-exports
  backend.rs          — S1: PageStorage, WalStorage traits + impls
  page.rs             — S2: SlottedPage, PageHeader, PageType
  buffer_pool.rs      — S3: BufferPool, SharedPageGuard, ExclusivePageGuard
  free_list.rs        — S4: FreeList
  wal.rs              — S5: WalWriter, WalReader, WalRecord
  btree.rs            — S6: BTree, BTreeHandle, ScanStream
  heap.rs             — S7: Heap, HeapRef
  dwb.rs              — S8: DoubleWriteBuffer
  checkpoint.rs       — S9: Checkpoint
  recovery.rs         — S10: Recovery, WalRecordHandler
  vacuum.rs           — S11: VacuumTask
  catalog_btree.rs    — S12: catalog key/value format
  engine.rs           — S13: StorageEngine facade
```

## Serialization Strategy

All on-disk formats use **little-endian** byte order.

| Structure | Crate | Rationale |
|-----------|-------|-----------|
| `PageHeader` (32 bytes) | `zerocopy` | Zero-copy, read/written millions of times via buffer pool. `U32<LE>`, `U16<LE>`, `U64<LE>` wrapper types enforce LE at the type level. |
| `FileHeader` (page 0) | `zerocopy` | Same — lives in a page buffer, accessed through buffer pool. |
| WAL frame header (9 bytes) | Manual `from_le_bytes`/`to_le_bytes` | Simple 3-field header, hot path but trivial format. |
| WAL segment header (32 bytes) | Manual `from_le_bytes`/`to_le_bytes` | Written once per segment. |
| DWB header (16 bytes) | Manual `from_le_bytes`/`to_le_bytes` | Written once per checkpoint. |
| Catalog entries (variable) | Manual `from_le_bytes`/`to_le_bytes` | Variable-length format, not amenable to zero-copy. |
| B-tree cells (variable) | Manual `from_le_bytes`/`to_le_bytes` | Variable-length key/value pairs. |

**Dependencies**: `zerocopy` (with `derive` feature), `crc32fast`.

## Public Facade (NO Layer 3+ types)

```rust
// Lifecycle
async StorageEngine::open(path, config) → Result<Self>
async StorageEngine::open_in_memory(config) → Result<Self>
async StorageEngine::open_with_backend(page_storage, wal_storage, config) → Result<Self>
async StorageEngine::close() → Result<()>
StorageEngine::is_durable() → bool

// B-tree
async StorageEngine::create_btree() → Result<BTreeHandle>
StorageEngine::open_btree(root_page: PageId) → BTreeHandle
async BTreeHandle::get(key: &[u8]) → Result<Option<Vec<u8>>>
async BTreeHandle::insert(key: &[u8], value: &[u8]) → Result<()>
async BTreeHandle::delete(key: &[u8]) → Result<bool>
BTreeHandle::scan(lower, upper, direction) → ScanStream

// Heap
async StorageEngine::heap_store(data: &[u8]) → Result<HeapRef>
async StorageEngine::heap_load(href: HeapRef) → Result<Vec<u8>>
async StorageEngine::heap_free(href: HeapRef) → Result<()>

// WAL
async StorageEngine::append_wal(record_type: u8, payload: &[u8]) → Result<Lsn>
StorageEngine::read_wal_from(lsn: Lsn) → WalStream

// Maintenance
async StorageEngine::checkpoint() → Result<()>
// Note: recovery runs automatically inside open() — there is no public recover() method.
```

## Code Documentation Requirement

All source files in the storage engine **must** include documentation:

- Every public struct, enum, trait, and function must have a `///` doc comment explaining its purpose.
- Module-level `//!` doc comments at the top of each file explaining what the module does and its role in the storage engine.
- Non-obvious implementation details, invariants, and safety considerations must be documented with inline comments.
- Unsafe blocks (if any) must have `// SAFETY:` comments explaining why the usage is sound.

## Latch Protocol Summary

See [14-latch-protocol.md](14-latch-protocol.md) for full details.

1. **Hierarchy**: page_table RwLock → Frame RwLock (never reverse)
2. **Multiple frames**: always ascending page_id order
3. **No latches across I/O**: read into temp buffer, then copy under latch
4. **B-tree**: latch coupling (crab protocol) — hold parent until child is safe
5. **All frame locks**: `parking_lot::RwLock` for frame-level (synchronous), `tokio::sync::Mutex` for component-level
