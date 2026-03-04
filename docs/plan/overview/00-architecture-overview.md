# Architecture Overview

## Layer Diagram

```
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
  Layer 7: API & Protocol          ◄── OPTIONAL
  (frame format, messages, session, auth, transports)
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
  Layer 6: Replication             ◄── OPTIONAL
  (WAL streaming, promotion, 3-tier recovery, snapshot)
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
├─────────────────────────────────────────────────────────┤
│  Layer 5: Transaction Manager                           │
│  (timestamps, OCC, commit log, subscriptions, commit)   │
├─────────────────────────────────────────────────────────┤
│  Layer 4: Query Engine                                  │
│  (planning, range encoding, scan, post-filter, merge)   │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Document Store & Indexing                     │
│  (MVCC keys, version resolution, primary/secondary      │
│   index, key encoding, array indexing, index build)     │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Storage Engine                                │
│  (pages, buffer pool, B+ tree, WAL, heap, free list,    │
│   DWB, checkpoint, recovery, vacuum, catalog B-trees)   │
├─────────────────────────────────────────────────────────┤
│  Layer 1: Core Types & Encoding                         │
│  (DocId, Scalar, FieldPath, Filter, BSON/JSON, ULID)    │
├─────────────────────────────────────────────────────────┤
│  Integration: Database, SystemDatabase, CatalogCache    │
│  (instance lifecycle, config, in-memory dual-indexed    │
│   catalog by name AND id)                               │
└─────────────────────────────────────────────────────────┘
```

## Embedded vs Network Usage

The database is designed as an **embedded library first**. The `Database` struct (Integration layer) is the primary API boundary — it provides full functionality without any networking:

```rust
// Embedded usage — no server, no network, no Layer 6 or 7
let db = Database::open("./mydata", config).await?;
db.create_collection("users")?;

let tx = db.begin_mutation()?;
tx.insert("users", doc)?;
tx.commit().await?;
```

**Layer 7 (API & Protocol)** is an optional wrapper that exposes the `Database` API over TCP/TLS/WebSocket/QUIC. It adds framing, sessions, and auth but no business logic.

**Layer 6 (Replication)** is also optional — only needed for multi-node deployments. A single-node embedded database uses Layers 1–5 + Integration only.

## Critical Layer Boundary Rule

**Before placing a module in a layer, ask:** "Does this module know about domain concepts (documents, MVCC, timestamps, tombstones) or is it a generic data structure operating on bytes/pages?"

- Generic data structures (B-tree, slotted pages, buffer pool) → **Layer 2 (Storage Engine)**
- Domain-aware modules (key encoding, version resolution, document insert/get with MVCC) → **Layer 3+**

## Layer 2 Independence Test

Layer 2 (Storage Engine) MUST be usable as a standalone generic storage library with **no document/MVCC knowledge**. Its public facade:

```rust
// Lifecycle
StorageEngine::open(path, config) → Result<StorageEngine>
StorageEngine::close() → Result<()>

// B-tree management
StorageEngine::create_btree() → Result<BTreeHandle>        // allocates root page
StorageEngine::open_btree(root_page: PageId) → BTreeHandle // opens existing tree

// Raw byte key/value ops
BTreeHandle::get(key: &[u8]) → Result<Option<Vec<u8>>>
BTreeHandle::insert(key: &[u8], value: &[u8]) → Result<()>
BTreeHandle::delete(key: &[u8]) → Result<bool>
BTreeHandle::scan(lower, upper, direction) → ScanIterator   // range scan on bytes

// Large value storage
StorageEngine::heap_store(data: &[u8]) → Result<HeapRef>
StorageEngine::heap_load(href: HeapRef) → Result<Vec<u8>>
StorageEngine::heap_free(href: HeapRef) → Result<()>

// WAL
StorageEngine::append_wal(record_type: u8, payload: &[u8]) → Result<Lsn>
StorageEngine::read_wal_from(lsn: Lsn) → WalIterator

// Maintenance
StorageEngine::checkpoint() → Result<()>
StorageEngine::recover() → Result<()>  // DWB restore + WAL replay
```

**If any method requires importing a type from Layer 3+ (DocId, Ts, Document, Filter, etc.), the boundary is WRONG.**

## Module Map

| Layer | Package | Key Files |
|-------|---------|-----------|
| L1 | `core/` | `types.rs`, `encoding.rs`, `ulid.rs`, `filter.rs`, `field_path.rs` |
| L2 | `storage/` | `engine.rs`, `page.rs`, `buffer_pool.rs`, `btree.rs`, `wal.rs`, `heap.rs`, `free_list.rs`, `dwb.rs`, `checkpoint.rs`, `recovery.rs`, `vacuum.rs`, `catalog_btree.rs` |
| L3 | `docstore/` | `primary_index.rs`, `secondary_index.rs`, `key_encoding.rs`, `version_resolution.rs`, `array_indexing.rs`, `index_builder.rs` |
| L4 | `query/` | `planner.rs`, `range_encoder.rs`, `scan.rs`, `post_filter.rs`, `merge.rs` |
| L5 | `tx/` | `timestamp.rs`, `read_set.rs`, `write_set.rs`, `commit_log.rs`, `occ.rs`, `subscriptions.rs`, `commit.rs` |
| L6 | `replication/` | `primary_server.rs`, `replica_client.rs`, `promotion.rs`, `snapshot.rs`, `recovery_tiers.rs` |
| L7 | `api/` | `frame.rs`, `messages.rs`, `session.rs`, `auth.rs`, `transport.rs` |
| Integration | `lib.rs` | `database.rs`, `system_database.rs`, `catalog_cache.rs`, `config.rs` |

## Cross-Layer Data Flow

| From → To | Interface | Data Exchanged |
|-----------|-----------|---------------|
| L7 → Integration | `Database` | all client operations (begin, insert, query, commit, etc.) |
| L5 → L4 | `QueryEngine` | read set intervals from executed queries |
| L5 → L3 | `DocumentStore` | write set application at commit |
| L4 → L3 | `PrimaryIndex`, `SecondaryIndex` | scan/get with MVCC resolution |
| L3 → L2 | `StorageEngine`, `BTreeHandle` | raw byte key/value ops |
| L6 → L2 | `StorageEngine` | WAL streaming, checkpoint |
| L6 → L5 | `ReplicationHook` (implements trait) | L6 implements L5's replication trait |
| Integration → L2 | `StorageEngine` | lifecycle, catalog B-tree access |
| Integration → All | `CatalogCache` | collection/index metadata lookups |

## Dependency Direction

```
L7 ──→ Integration ──→ L5 → L4 → L3 → L2 → L1
           │               ↑
           │           L5 defines trait:
           │           ReplicationHook
           │               ▲
           └──→ L6 ────────┘ (implements trait)
               L6 also → L2 (WAL read/write)
```

- **L1–L5** form a strict bottom-up chain: each depends only on layers below
- **Integration** (`Database`, `CatalogCache`) composes L2–L5 into a usable unit
- **L7** (API/Protocol) depends on Integration — wraps `Database` with networking
- **L6** (Replication) implements the `ReplicationHook` trait defined in L5 — **no cycle**
- **Integration wires them together**: constructs `CommitCoordinator` with L6's `ReplicationHook` impl (or `NoReplication` for embedded single-node)

**Cycle avoidance via dependency inversion:**
- L5 does NOT depend on L6. It defines `trait ReplicationHook` with `replicate_and_wait()`
- L6 implements `ReplicationHook` (L6 → L5 trait, not L5 → L6)
- Integration injects L6's impl into L5 at construction time
- For embedded usage without replication: `NoReplication` (no-op impl) is injected instead
