# Implementation Phases

## Phase Ordering

The phases follow the layer dependency order: build from the bottom up.

```
Phase 1: Core Types + Storage Engine (Layers 1-2)
    │
Phase 2: Indexing (Layer 3)
    │
Phase 3: Query Engine (Layer 4)
    │
Phase 4: Transactions (Layer 5)
    │
Phase 5: Integration (database.rs, system.rs, catalog.rs)
    │
Phase 6: API / Protocol (Layer 7)
    │
Phase 7: Replication (Layer 6)
    │
Phase 8: Polish (integrity checks, optimization, benchmarks)
```

---

## Phase 1: Core Types + Storage Engine

**Goal:** Durable page store with WAL, buffer pool, and crash recovery.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `types.rs` | `DocId`, `CollectionId`, `IndexId`, `Ts`, `Lsn`, `PageId`, `Scalar`, `Filter`, `FieldPath` |
| `ulid.rs` | `UlidGenerator`, `encode_ulid()`, `decode_ulid()` |
| `encoding.rs` | `encode_document()`, `decode_document()`, `apply_patch()`, `extract_field()` |
| `storage/page.rs` | `SlottedPage`, `PageHeader`, cell operations |
| `storage/buffer_pool.rs` | `BufferPool`, `SharedPageGuard`, `ExclusivePageGuard` |
| `storage/wal.rs` | `WalWriter`, `WalReader`, `WalRecord`, all record types |
| `storage/heap.rs` | `HeapManager`, `HeapRef`, overflow page support |
| `storage/free_list.rs` | `FreeList` |
| `storage/dwb.rs` | `DoubleWriteBuffer` |
| `storage/checkpoint.rs` | `CheckpointManager` |
| `storage/recovery.rs` | `RecoveryManager::recover()` |

### Tests
- Page format: slot insert/delete, checksum, overflow
- Buffer pool: pin/unpin, eviction, concurrent shared access
- WAL: write/read records, segment rollover, CRC verification
- Checkpoint: dirty page flush, DWB write/restore
- Recovery: crash simulation (kill mid-checkpoint), WAL replay

---

## Phase 2: Indexing

**Goal:** B+ tree with order-preserving key encoding and MVCC version resolution.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `index/btree.rs` | `BTree`, `insert()`, `get()`, `delete()`, `scan_range()` |
| `index/key_encoding.rs` | `encode_scalar()`, `decode_scalar()`, `encode_index_key()`, `make_primary_key()` |
| `index/primary.rs` | `PrimaryIndex`, `get_at_ts()`, `insert_version()`, `scan_at_ts()` |
| `index/secondary.rs` | `SecondaryIndex`, `scan_range_at_ts()`, `apply_delta()` |
| `index/builder.rs` | `IndexBuilder::build_index()` |
| `catalog.rs` | `CatalogCache`, `CollectionMeta`, `IndexMeta`, catalog B-tree ops |

### Tests
- B+ tree: insert/delete, splits, range scan, ascending order
- Key encoding: round-trip all types, ordering correctness, compound keys
- Primary index: multi-version read, tombstone handling
- Secondary index: version resolution, stale entry skip
- Background index build: concurrent writes during build

---

## Phase 3: Query Engine

**Goal:** Query planning and execution with post-filters and read set generation.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `query/planner.rs` | `plan_query()`, `QueryPlan` enum |
| `query/range.rs` | `encode_range()`, `validate_range_exprs()` |
| `query/scan.rs` | `execute_query()`, `QueryResult` |
| `query/filter.rs` | `filter_matches()` |

### Tests
- Plan selection: primary get vs index scan vs table scan
- Range encoding: equality prefixes, range bounds, compound indexes
- Post-filter: all operators (eq, ne, gt, gte, lt, lte, in, and, or, not)
- Limit + tightening: interval narrows when limit reached
- Read-your-writes: write set merge with snapshot

---

## Phase 4: Transactions

**Goal:** OCC validation, subscriptions, and full commit protocol.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `tx/timestamp.rs` | `TsAllocator` |
| `tx/read_set.rs` | `ReadSet`, `ReadInterval`, interval merging |
| `tx/write_set.rs` | `WriteSet`, `compute_index_deltas()` |
| `tx/commit_log.rs` | `CommitLog`, `validate()` |
| `tx/subs.rs` | `SubscriptionRegistry`, `check_invalidation()` |
| `tx/mod.rs` | `TransactionManager`, `Transaction`, writer task |

### Tests
- OCC: conflict detection (old_key + new_key overlap)
- OCC: no false positive on non-overlapping intervals
- Subscription: invalidation notification with correct query_ids
- Subscription chain: re-registration on chain commit
- Write set: index delta computation
- Concurrent readers + single writer

---

## Phase 5: Integration

**Goal:** Wire everything together into `Database` and `SystemDatabase`.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `database.rs` | `Database::open/close()`, collection/index management, transaction flow |
| `system.rs` | `SystemDatabase`, multi-database management |
| `config.rs` | `ServerConfig`, `DatabaseConfig` |
| `storage/vacuum.rs` | `VacuumTask` |

### Tests
- Full lifecycle: create database → create collection → insert → query → commit
- Multi-database isolation
- Startup sequence: recovery + catalog rebuild
- Vacuum: old version cleanup
- Checkpoint + WAL reclamation

---

## Phase 6: API / Protocol

**Goal:** Client-facing transport with message framing, auth, and session management.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `protocol/framing.rs` | `FrameReader`, `FrameWriter`, auto-detect JSON/binary |
| `protocol/messages.rs` | `ClientMessage`, `ServerMessage`, `OkPayload` |
| `protocol/session.rs` | `Session`, message routing, pipelining |
| `protocol/auth.rs` | `validate_token()`, JWT support |
| `server.rs` | `Server::start()`, TCP/TLS/QUIC/WS listeners |

### Tests
- Frame parsing: JSON text mode, binary frame mode, mixed
- Message round-trip: all client/server message types
- Session: authentication flow, transaction ordering
- Pipelining: concurrent transactions on same connection
- WebSocket adaptation

---

## Phase 7: Replication

**Goal:** Primary-replica WAL streaming with synchronous confirmation.

### Deliverables

| Module | Key Types/Functions |
|---|---|
| `replication/primary.rs` | `PrimaryReplicationServer`, WAL broadcast |
| `replication/replica.rs` | `ReplicaClient`, WAL apply, reconnection |
| `replication/promotion.rs` | Transaction promotion protocol |
| `replication/snapshot.rs` | Full reconstruction (Tier 3) |

### Tests
- WAL streaming: primary → replica, records arrive in order
- Replica read: serves reads from local snapshot
- Promotion: write tx on replica → commit on primary
- Recovery tiers: Tier 1 (catch-up), Tier 2 (local + catch-up), Tier 3 (snapshot)
- Subscription invalidation on replica (via WAL stream)

---

## Phase 8: Polish

**Goal:** Production readiness.

### Deliverables

| Area | Work |
|---|---|
| `storage/recovery.rs` | `check_integrity()`, `repair()` |
| Performance | Buffer pool benchmarks, WAL throughput, B-tree fan-out tuning |
| Observability | Metrics (buffer pool hit rate, WAL fsync latency, commit throughput) |
| Compression | LZ4 frame compression for binary protocol |
| Configuration | Validation, defaults, documentation |
