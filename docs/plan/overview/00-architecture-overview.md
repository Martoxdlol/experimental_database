# Architecture Overview

## Layer Diagram

```
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
  Layer 8: API & Protocol          ◄── OPTIONAL
  (frame format, messages, session, auth, transports)
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
  Layer 7: Replication Transport   ◄── OPTIONAL
  (WAL streaming, promotion, 3-tier recovery, snapshot)
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
├─────────────────────────────────────────────────────────┤
│  Layer 6: Database Instance                             │
│  (lifecycle, collections, indexes, transactions,        │
│   catalog cache, replication hook trait, config)        │
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
└─────────────────────────────────────────────────────────┘
```

## Embedded-First Design

The database is designed as an **embedded library first**. The `Database` struct (Layer 6) is the primary API boundary — it provides full functionality without any networking:

```rust
// Embedded usage — no server, no network, no Layer 7 or 8
let db = Database::open("./mydata", config, None).await?;
db.create_collection("users")?;

let mut tx = db.begin_mutation()?;
let id = tx.insert("users", json!({"name": "Alice"}))?;
tx.commit(CommitOptions::default()).await?;

let tx = db.begin_readonly();
let results = tx.query("users", "_created_at", &[], None, None, Some(10))?;
db.close().await?;
```

**Layer 6 (Database)** is the core public API. Everything below (L1–L5) is internal machinery. Everything above (L7, L8) is optional.

**Layer 7 (Replication)** is optional — implements the `ReplicationHook` trait defined by L6. Only needed for multi-node deployments.

**Layer 8 (API & Protocol)** is optional — wraps `Database` with networking, framing, sessions, and auth. Only needed for remote access.

A single-node embedded database uses **Layers 1–6 only**.

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
| L6 | `database/` | `database.rs`, `catalog_cache.rs`, `system_database.rs`, `config.rs`, `replication_hook.rs` |
| L7 | `replication/` | `primary_server.rs`, `replica_client.rs`, `promotion.rs`, `snapshot.rs`, `recovery_tiers.rs` |
| L8 | `api/` | `frame.rs`, `messages.rs`, `session.rs`, `auth.rs`, `transport.rs` |

## Cross-Layer Data Flow

| From → To | Interface | Data Exchanged |
|-----------|-----------|---------------|
| L8 → L6 | `Database`, `SystemDatabase` | all client operations (begin, insert, query, commit, etc.) |
| L6 → L5 | `CommitHandle`, `TsAllocator` | commit requests, timestamp allocation |
| L6 → L3 | `PrimaryIndex`, `SecondaryIndex` | index handle management |
| L6 → L2 | `StorageEngine` | lifecycle, catalog B-tree access |
| L5 → L4 | `QueryEngine` | read set intervals from executed queries |
| L5 → L3 | `DocumentStore` | write set application at commit |
| L4 → L3 | `PrimaryIndex`, `SecondaryIndex` | scan/get with MVCC resolution |
| L3 → L2 | `StorageEngine`, `BTreeHandle` | raw byte key/value ops |
| L7 → L6 | `Database`, `ReplicationHook` (implements trait) | WAL streaming, replication |
| L7 → L2 | `StorageEngine` (via `Database::storage()`) | WAL read for streaming |

## Dependency Direction

```
L8 (API) ──→ L6 (Database) ──→ L5 → L4 → L3 → L2 → L1
                  │    ▲
                  │    │ implements ReplicationHook trait
                  │    │
L7 (Replication) ─┘    │
L7 also → L2 (WAL read/write)
```

- **L1–L5** form a strict bottom-up chain: each depends only on layers below
- **L6** (`Database`) composes L2–L5 into a usable embedded database. Defines the `ReplicationHook` trait.
- **L7** (Replication) implements the `ReplicationHook` trait from L6 — **no cycle**. Also accesses L2 for WAL streaming via `Database::storage()`.
- **L8** (API/Protocol) depends on L6 — wraps `Database` with networking, auth, sessions.

**Cycle avoidance via dependency inversion:**
- L6 defines `trait ReplicationHook` with `replicate_and_wait()`
- L7 implements `ReplicationHook` (L7 → L6 trait, not L6 → L7)
- L6 receives the hook at `Database::open()` construction time
- For embedded usage without replication: `NoReplication` (no-op impl in L6) is used by default
