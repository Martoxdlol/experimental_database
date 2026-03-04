# Implementation Phases

Bottom-up build order. Each phase produces a testable deliverable.

## Phase 1: Core Types & Encoding (Layer 1)

**Deliverables:**
- `core/types.rs` — DocId, CollectionId, IndexId, Ts, Scalar, TypeTag
- `core/field_path.rs` — FieldPath
- `core/filter.rs` — Filter AST, RangeExpr
- `core/encoding.rs` — BSON encode/decode, apply_patch, extract_scalar
- `core/ulid.rs` — ULID generation and Base32 encoding/decoding

**Tests:**
- Scalar ordering matches type ordering spec (section 1.6)
- ULID roundtrip encode/decode
- BSON encode/decode roundtrip
- Patch application (RFC 7396 merge-patch)
- Field extraction from nested documents
- Type tag ordering

**No dependencies on other layers.**

## Phase 2: Storage Engine Foundation (Layer 2 — Part 1)

**Deliverables:**
- `storage/page.rs` — SlottedPage read/write/checksum
- `storage/buffer_pool.rs` — BufferPool with clock eviction, SharedPageGuard, ExclusivePageGuard
- `storage/free_list.rs` — FreeList allocate/deallocate
- `storage/engine.rs` — StorageEngine skeleton (open, new_page, file header)

**Tests:**
- SlottedPage: insert slots, delete slots, compact, checksum verify
- BufferPool: fetch/pin/unpin, clock eviction, cache miss reads from disk
- FreeList: allocate/deallocate roundtrip
- FileHeader: read/write/checksum

**Layer 2 facade is usable at this point for page-level operations.**

## Phase 3: B-Tree & WAL (Layer 2 — Part 2)

**Deliverables:**
- `storage/btree.rs` — Generic B+ tree (insert, get, delete, scan, split, merge)
- `storage/wal.rs` — WalWriter (group commit), WalReader, segment management
- `storage/heap.rs` — Heap store/load/free with overflow chains
- `storage/dwb.rs` — DoubleWriteBuffer write/recover/truncate

**Tests:**
- B-tree: insert 10K random keys, get all, scan ranges, delete half, verify integrity
- B-tree: split correctness (leaf + internal), merge/redistribute
- B-tree: scan forward + backward with bounds
- WAL: write records, read back, verify CRC, segment rollover
- WAL: group commit (multiple concurrent appends -> single fsync)
- Heap: store + load small/medium/large blobs, overflow chain traversal
- DWB: write pages, simulate torn write, recover

**StorageEngine facade fully operational: create_btree, open_btree, BTreeHandle ops, heap, WAL.**

## Phase 4: Checkpoint & Recovery (Layer 2 — Part 3)

**Deliverables:**
- `storage/checkpoint.rs` — Full checkpoint protocol
- `storage/recovery.rs` — Crash recovery (DWB restore + WAL replay)
- `storage/vacuum.rs` — Page-level entry removal
- `storage/catalog_btree.rs` — Catalog B-tree key format, serialize/deserialize

**Tests:**
- Checkpoint: dirty pages flushed through DWB, WAL record written
- Recovery: crash after DWB write -> torn pages restored, WAL replayed
- Recovery: crash during WAL write -> partial record detected via CRC
- Vacuum: remove entries, pages freed
- Catalog B-tree: insert/get/scan collection and index entries

**Storage engine is crash-safe and self-recovering.**

## Phase 5: Document Store & Indexing (Layer 3)

**Deliverables:**
- `docstore/key_encoding.rs` — Order-preserving scalar encoding
- `docstore/primary_index.rs` — PrimaryIndex (insert_version, get_at_ts, scan)
- `docstore/secondary_index.rs` — SecondaryIndex (scan with version resolution)
- `docstore/version_resolution.rs` — MVCC version resolver
- `docstore/array_indexing.rs` — Array index entry expansion
- `docstore/index_builder.rs` — Background index build

**Tests:**
- Key encoding: roundtrip, ordering matches type ordering
- Primary: insert versions, get_at_ts sees correct version, tombstones hide doc
- Primary: inline vs external storage threshold
- Secondary: version resolution skips stale entries, verification against primary
- Array indexing: one entry per element, compound restriction
- Index builder: build on existing data matches manual insertion

**Document store is functional: MVCC reads and writes with version resolution.**

## Phase 6: Query Engine (Layer 4)

**Deliverables:**
- `query/planner.rs` — Query plan selection
- `query/range_encoder.rs` — Range expressions to byte intervals
- `query/scan.rs` — Scan execution pipeline
- `query/post_filter.rs` — Filter evaluation
- `query/merge.rs` — Read-your-writes merge

**Tests:**
- Range encoder: eq/gt/gte/lt/lte combinations, compound index ranges
- Range validation: reject invalid orderings
- Post-filter: eq, ne, gt, in, and, or, not operators
- Scan: index scan returns correct docs at read_ts
- Merge: write set overrides/additions/deletions visible in results
- Query result includes correct read set intervals

**Full query capability: plan, scan, filter, merge.**

## Phase 7: Transaction Manager (Layer 5)

**Deliverables:**
- `tx/timestamp.rs` — TsAllocator
- `tx/read_set.rs` — ReadSet with interval merging
- `tx/write_set.rs` — WriteSet with index delta computation
- `tx/commit_log.rs` — CommitLog
- `tx/occ.rs` — OCC validation
- `tx/subscriptions.rs` — SubscriptionRegistry
- `tx/commit.rs` — CommitCoordinator (single-writer loop), ReplicationHook trait

**Tests:**
- OCC: detect conflict when read interval overlaps concurrent write
- OCC: no false conflict when intervals don't overlap
- OCC: phantom detection (new key enters read interval)
- Subscriptions: register, invalidate on overlapping commit, collect query_ids
- Subscriptions: subscribe chain (invalidation -> new tx -> updated read set)
- Commit protocol: end-to-end commit with WAL + page store + commit log
- Read set: interval merging, limit-aware tightening

**Full ACID transactions with OCC and subscriptions.**

## Phase 8: Database Instance (Layer 6)

**Deliverables:**
- `database/database.rs` — Database struct (open, close, embedded API)
- `database/catalog_cache.rs` — CatalogCache (dual-indexed by name AND id)
- `database/system_database.rs` — SystemDatabase (database registry)
- `database/config.rs` — DatabaseConfig, TransactionConfig
- `database/replication_hook.rs` — ReplicationHook trait, NoReplication

**Tests:**
- CatalogCache: lookup by name and by id, add/remove collections and indexes
- Database: open with recovery, create collection, insert + query, close
- Database: create index, background build completes, query uses index
- Database: embedded usage end-to-end (no networking)
- Database: begin_readonly, begin_mutation, commit, rollback
- Database: ReplicationHook called during commit (mock implementation)
- SystemDatabase: create/drop/list databases
- End-to-end: multiple databases, each with collections and indexes
- Startup/shutdown: clean shutdown + restart preserves data

**System is usable as an embedded library. Primary milestone.**

## Phase 9: Replication (Layer 7) — Optional

**Deliverables:**
- `replication/primary_server.rs` — PrimaryReplicator (implements ReplicationHook)
- `replication/replica_client.rs` — ReplicaClient
- `replication/promotion.rs` — Transaction promotion
- `replication/snapshot.rs` — Full snapshot transfer
- `replication/recovery_tiers.rs` — Recovery tier selection

**Tests:**
- PrimaryReplicator implements ReplicationHook correctly
- WAL streaming: primary commit -> replica receives and applies
- Read isolation: replica reads at applied_ts
- Subscription: replica-local subscription invalidated on WAL receive
- Promotion: write on replica -> forwarded to primary -> committed
- Tier 1: replica disconnect -> reconnect -> incremental catch-up
- Tier 3: full snapshot transfer -> replica operational
- Reconnection: exponential backoff on connection failure

**Full distributed system: primary + replicas with synchronous replication.**

## Phase 10: API & Protocol (Layer 8) — Optional

**Deliverables:**
- `api/frame.rs` — Frame format (JSON text + binary auto-detect)
- `api/messages.rs` — All message types, parse/serialize
- `api/session.rs` — Session state machine
- `api/auth.rs` — JWT validation
- `api/transport.rs` — TCP/TLS/WebSocket listeners, ServerConfig

**Tests:**
- Frame: JSON text roundtrip, binary frame roundtrip, auto-detect
- Messages: parse all client message types, serialize all server message types
- Session: auth -> begin -> insert -> query -> commit lifecycle
- Session: pipelining (multiple messages before response)
- Session: subscription invalidation push
- Auth: valid JWT -> ok, expired -> error, wrong issuer -> error

**Server is accessible over the network.**

## Phase 11: Hardening & Polish

**Deliverables:**
- Integrity check (`check_integrity`) and auto-repair
- File header shadow copy
- Configurable resource limits enforcement
- Transaction timeout enforcement
- WAL retention policy enforcement
- Performance benchmarks
- Stress tests (concurrent readers + writer, crash-and-recover loops)

**Tests:**
- Integrity check: detects checksum failures, orphan pages, broken B-tree links
- Auto-repair: shadow header restore, orphan pages added to free list
- Limits: transaction timeout fires, read limit exceeded aborts tx
- Stress: 100 concurrent readers + 1 writer, verify no corruption after 1M ops
- Crash loop: kill process randomly during operation, recover, verify data integrity
