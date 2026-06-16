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
- Patch application with shallow replacement, explicit null storage, and
  `_meta.unset` removal
- Field extraction from nested documents
- `_meta.types` extraction for JSON bytes/id values
- Type tag ordering

**No dependencies on other layers.**

## Phase 2: Storage Engine Foundation (Layer 2 — Part 1)

**Deliverables:**
- `storage/backend.rs` — `PageStorage` and `WalStorage` traits, `FilePageStorage`, `MemoryPageStorage`, `FileWalStorage`, `MemoryWalStorage`
- `storage/page.rs` — SlottedPage read/write/checksum
- `storage/buffer_pool.rs` — BufferPool with clock eviction, SharedPageGuard, ExclusivePageGuard (takes `Arc<dyn PageStorage>`)
- `storage/free_list.rs` — FreeList allocate/deallocate
- `storage/engine.rs` — StorageEngine skeleton (open, open_in_memory, open_with_backend, file header)

**Tests:**
- PageStorage: FilePageStorage read/write roundtrip, MemoryPageStorage read/write roundtrip
- SlottedPage: insert slots, delete slots, compact, checksum verify
- BufferPool: fetch/pin/unpin, clock eviction, cache miss reads from backend
- BufferPool: works identically with file and memory backends
- FreeList: allocate/deallocate roundtrip
- FileHeader: read/write/checksum

**Layer 2 facade is usable at this point for page-level operations. Both file and memory backends work.**

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
- `query/access.rs` — Access method resolution
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
- Merge: pending array-index insert/replace rows use the same expanded
  secondary keys as committed index entries
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
- Database: embedded usage end-to-end (no networking), both file-backed and in-memory
- Database: open_in_memory creates ephemeral database, same API as file-backed
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

**Current implementation note:** `crates/replication/src/lib.rs` now contains
the first L7 foundation: cluster membership/quorum tracking, guarded topology
updates, replica applied-LSN tracking, a bounded framed peer protocol, an
async per-peer connection primitive, an injectable `WalSender`, a peer-backed
WAL sender, `PeerMesh` connection/routing ownership, and a `PrimaryReplicator`
that implements `ReplicationHook`. `PeerMeshRuntime` now starts TCP
listen/accept/connect tasks with initial outbound retry and heartbeat ticks.
Post-disconnect reconnect/replacement now removes stale peer handles and
reconnects lower-node outbound peers. L6 now exposes a durable replicated
TxCommit apply primitive that
persists received WAL and local `VisibleTs` records, reuses startup recovery
apply semantics, advances local visibility, and has file-backed durability
coverage. The server now wraps that method in a database-backed `WalApplier`
and covers primary commits flowing through real loopback TCP mesh traffic into
a file-backed replica database. Primary-source applied-LSN progress now lives
in the L2 file header and is exposed by L6; retained-WAL catch-up can now be
requested and is triggered when a replica attaches to a primary. A pure
recovery-tier selector and explicit retained-WAL gap signal now choose/report
snapshot reconstruction when Tier 1 catch-up is impossible. Opaque promotion
request/response transport over `PeerMesh` now forwards replica write payloads
to a primary-side handler, and L5/L6 now provide a versioned document
`ReadSet`/`WriteSet` promotion payload that the server primary handler commits
through the real database path. Quorum-checked snapshot chunk transport over
`PeerMesh` now streams begin/data/end frames into an injectable receiver sink
and falls back from unavailable retained-WAL catch-up to snapshot streaming
when a primary snapshot source is installed.
L2/L6 now export checkpointed snapshot images, restore them into fresh durable
database paths, and preserve restored WAL LSN ordering for future commits.
The server layer now bridges those snapshots into real mesh source/sink traffic.
The server config now starts replication meshes for configured per-database
entries after rejecting duplicate database names and mixed primary/replica role
sets before any managed-registry mutation, installs the primary replication
hook or replica WAL applier, advertises durable replica progress in startup
handshakes, registers fresh replicas after snapshot restore, installs the
restored database into the live WAL applier, owns mesh shutdown, and covers a
three-node topology where a
primary commits with one live replica while a second replica is offline, plus
an all-online three-node topology where the primary fans one commit out to both
replicas and records each replica's durable applied-LSN progress.
Fresh multi-database snapshot fallback coverage now restores two configured
databases independently, keeps their collection catalogs isolated, applies
subsequent live WAL to both, and proves the restored registry entries survive
managed-registry restart.
Existing multi-database snapshot replacement coverage now starts with two
stale local replica databases, replaces both from independent primary
snapshots, keeps their catalogs isolated, applies subsequent live WAL to both,
and proves both replacement registry entries survive managed-registry restart.
L8 replica document write-session commits now route to a configured
`PromotionClient` instead of committing locally, including Subscribe success
subscription registration and retry metadata propagation. Replica
collection/index management writes now promote primary-owned DDL intents and
return the primary's wire response. Primary handshakes advertise the live
retained-WAL floor from storage-backed catch-up sources, and replicas request a
snapshot immediately when that floor excludes their durable applied LSN.
Configured replicas without a usable local database quarantine corrupt local
state and force snapshot reconstruction on primary attach.
Configured replica wire sessions now also gate new readonly `Begin` requests
through the database's `PeerMesh` quorum state and return `quorum_lost` when a
replica lacks majority.
Mesh-backed promoted system database DDL now verifies not only create/drop
routing, but also primary-side `database_in_use` refusal propagation and
successful retry after the live primary handle is released. Direct
replica-session coverage also verifies primary-returned DDL errors such as
`database_exists` are forwarded unchanged and do not create replica-local
registry state.
Promotion-client coverage now also verifies a live local role transition from
replica to primary prevents later write promotion with a role-specific error
before contacting the primary handler.
Richer system-catalog recovery orchestration beyond current per-database and
multi-database snapshot registration/replacement, plus broader end-to-end
multi-node tests, remain pending.

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

**Current status:** The `exdb-wire` crate now implements the foundational
`frame`, `messages`, `auth`, and session-core deliverables for JSON text plus
binary JSON/BSON/Protobuf frames, JWT claim validation, hello/auth gating,
deduplication, strict unknown-field and `_meta` validation, ping, database
management, collection management, and index management, transaction lifecycle,
core CRUD/query data operations, plus a TCP transport connection/listener core.
Wire sessions now maintain a client-visible, connection-scoped transaction ID
namespace that starts at 1 independently of internal L6 transaction IDs, and
translate Subscribe continuation/retry transaction IDs at the protocol
boundary.
`get` and `query` responses now include
server-assigned query IDs, JSON document bodies honor `_meta.types` for bytes/id
storage metadata, JSON query predicates honor query-level `_meta.types` for
typed scalar disambiguation, Protobuf payloads use the shared schema in
`crates/wire/proto/exdb_wire.proto`, and the transport pushes subscription
invalidation notifications. Subscribe continuation/retry `new_tx` identifiers
are installed as active session transactions using that session-local
namespace. The server binary opens the
registry and starts the TCP listener from CLI/JSON config. The CLI now supports embedded database
commands, high-level JSON text commands against a TCP server, and raw JSON text
requests, plus interactive REPL sessions over the same command executor.
Background index builds now push `index_ready` notifications to authenticated
matching connections.
The TCP connection loop accepts pipelined input from clients, including
same-transaction message bursts, through a bounded per-connection request
queue, dispatches transaction-scoped messages through per-transaction workers
so different transaction IDs can make progress concurrently while preserving
same-transaction order, accounts for per-connection same-transaction scheduler
backlog with `server_busy` when the backlog is full, bounds per-connection
push-notification queues, and enforces a per-frame response write timeout for
slow clients.
Mixed JSON/BSON/Protobuf CRUD/readback lifecycle coverage now runs through the
direct stream harness plus real TCP, TLS-over-TCP, QUIC, plain WebSocket, and
secure WebSocket listeners. TCP, TLS-over-TCP, QUIC, plain WebSocket, and
secure WebSocket listener coverage also proves the same mixed encoding path
works after JWT authentication and rejects a binary request with
`auth_required` before authentication.
TCP, TLS-over-TCP, QUIC, plain WebSocket, and secure WebSocket listener
coverage also pipelines `authenticate` plus a readonly replica `begin` in one
burst, proving queued post-auth requests are released against the updated auth
state and fenced by the replica read gate.
TLS-over-TCP listeners now use configured certificate/key PEM material and run
the same stream protocol after handshake. Plain and secure WebSocket listeners
now carry text JSON messages without newline delimiters and binary messages
with the same 12-byte frame used by stream transports. QUIC now accepts TLS 1.3
connections, opens an initial bidirectional session stream for the protocol
hello, and accepts additional client-initiated bidirectional streams.
Replica sessions can be configured with a read-quorum gate; transport setup now
carries that gate into TCP, TLS, QUIC, WebSocket, and secure WebSocket sessions
so readonly begins fail with `quorum_lost` when a configured replica is
partitioned away from majority. TCP, TLS-over-TCP, QUIC, and plain and secure
WebSocket coverage now combines JWT auth, replica role, and a failing read gate
on real listeners, proving auth is enforced before the replica quorum check and
authenticated sessions still see `quorum_lost` for fenced reads across each
listener type.
Direct authenticated replica transport coverage now also uses a passing
read-quorum gate with a seeded managed database, mixes JSON, BSON, and Protobuf
frames in one session, verifies readonly get succeeds after auth, and verifies
a local write transaction is rejected as `readonly_node` without leaving active
transaction accounting behind.
Authenticated multi-client coverage now uses scoped user JWTs on two
connections sharing one registry, verifies allowlist-filtered
`list_databases`, and proves a committed write on one connection delivers a
server-pushed invalidation to a `notify` subscription on the other. Additional
multi-client mixed-workload coverage combines scoped auth, BSON subscriber
reads/commit, a pipelined Protobuf writer replace/commit, and a same-connection
BSON ping while preserving pushed invalidation framing, `msg_id = 0`, and the
subscriber's most recent binary encoding.
Multi-client resource-pressure coverage now also proves that saturating one
connection's asynchronous management scheduler with a blocked replica DDL
promotion does not prevent a second connection from receiving ordinary `ping`
responses, while the saturated connection receives connection-local
`server_busy` accounting.
Auth-expiry lifecycle coverage now also proves a server-initiated
`auth_expired` close rolls back an active uncommitted transaction before a
fresh authenticated connection can observe it.
Direct JSON transport and CLI server-mode coverage verify `list-databases`
preserves all per-database usage fields exposed by L8, checks the disk/page
accounting relationships returned to operators, and observes active transaction
count transitions through wire begin/rollback.
JSON-stream transport coverage now exercises a complete
database/collection/index management lifecycle through indexed query readback
and index/collection/database teardown. Broader concurrent per-category
scheduling and additional fairness/resource-accounting validation remain
pending.

**Deliverables:**
- `api/frame.rs` — Frame format (JSON text + binary auto-detect)
- `api/messages.rs` — All message types, parse/serialize
- `api/session.rs` — Session state machine
- `api/auth.rs` — JWT validation
- `api/transport.rs` — TCP/TLS/QUIC/WebSocket listeners, ServerConfig

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
- Workspace gates: `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace --doc`, `cargo test --workspace --all-targets`,
  and `git diff --check`
- Integrity check: detects checksum failures, orphan pages, broken B-tree links
- Auto-repair: shadow header restore, trailing data-file byte truncation,
  B-tree leaf sibling-chain rebuild, orphan pages added to free list, and a
  combined catalog-name/Ready-secondary/orphan-page repair pass that is
  idempotent on the next `repair_integrity` run
- Limits: transaction timeout fires, read limit exceeded aborts tx
- Stress: 100 concurrent readers + 1 writer smoke exists with indexed reads,
  concurrent commits, checkpointed integrity, and reopen verification;
  deterministic transaction-local query stress now models pending
  insert/delete/replace overlays against limited forward/backward compound-index
  queries before commit;
  deterministic randomized file-backed soak now mixes insert/replace/delete,
  secondary-index queries, checkpoints, crash/reopen cycles, model checks, and
  final full integrity; concurrent randomized file-backed soak now combines
  limited secondary-index readers, randomized insert/replace/delete writers,
  checkpoints during activity, repeated crash/reopen cycles, model checks, and
  final full integrity; the full 1M-operation soak remains a future extended
  target
- Crash loop: deterministic indexed and pseudo-random crash/recover loops with
  full startup integrity exist; deterministic process-abort coverage now
  verifies file-backed recovery after child-process termination without close;
  broader randomized process-kill coverage remains a future hardening target
