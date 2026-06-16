# Implementation Audit

This file tracks the gap between the written design/implementation plan and
the current codebase. A passing test suite is necessary evidence, but not
sufficient proof of completion for the full database specification.

Last audited from:

- `docs/DESIGN.md`
- `docs/plan/overview/11-implementation-phases.md`
- `docs/plan/database/00-overview.md`
- `docs/plan/database/10-durability.md`
- Current Rust crates and app entrypoints

## Summary

| Area | Status | Evidence |
|---|---|---|
| L1 core types | Mostly implemented | Core modules exist. Document bodies now encode to BSON at the shared encoding boundary while preserving the JSON-facing embedded API. JSON `_meta.types` now drives BSON bytes/id encoding and scalar extraction for indexed fields. The embedded Rust API also exposes native BSON document insert/get/query/replace methods plus BSON bytes/id helpers, so callers no longer need sidecar metadata to use non-JSON-native bytes/id values. A fully custom internal typed document value model remains future polish rather than a caller-facing gap. |
| L2 storage | Mostly implemented | Storage modules, WAL, DWB, checkpoint, recovery, B-tree, heap, catalog B-tree, storage integrity checking, shadow file header recovery, explicit file-header shadow repair, crash-tail zeroed-page reclamation during rollback recovery, replication-progress-aware WAL retention, optional size/age WAL retention caps, checkpointed page-store snapshot export/restore with WAL-base LSN preservation, and extensive tests exist. Hardening items such as general integrity repair and broader crash-loop coverage still need audit/implementation. |
| L3 docstore | Mostly implemented | Primary/secondary MVCC indexes, array indexing, index builder, and vacuum modules exist with tests. L6 startup rollback now removes unreplicated versions and frees external rollback bodies, with deeper fault-injection coverage still pending. |
| L4 query | Mostly implemented | Planner, range encoder, scan, post-filter, and write-set merge exist with tests. Range validation now enforces the DESIGN 7.7 rule that range bounds must target the next field after the equality prefix, post-filter operator coverage has been audited against `eq/ne/gt/gte/lt/lte/in/and/or/not`, write-set merge now handles replacements that move into or out of a scan range, and L6 integration coverage exercises array-expanded indexes plus filtered limited scans. |
| L5 transactions | Mostly implemented | OCC, read/write sets, subscriptions, commit coordinator, visible timestamp WAL, and replication hook exist. Public reads now allocate operation-level query IDs, and commit metadata includes primary-index writes so get-by-id read intervals can conflict/invalidate correctly. Secondary index entries for retained MVCC versions are now preserved until vacuum instead of being removed on replace/delete. L7 now has a quorum-aware network replication foundation, while higher-level recovery orchestration remains incomplete. |
| L6 embedded database | Partial | Embedded API, catalog, recovery, durability tests, subscriptions, and durable multi-database registry metadata exist. Startup rollback-vacuum now handles unreplicated document insert/replace/delete physical cleanup plus DDL create/drop collection/index recovery; configured document-size, disk quota, and transaction read/scan/work limits are enforced. Transaction query now merges pending inserts/deletes/replaces into secondary scans and records forward/backward limit boundaries for OCC/subscription precision. Disk/memory usage tracking exists. L6 now exposes durable replicated TxCommit apply and checkpointed snapshot export/restore primitives covered by file-backed durability tests; broader CPU/resource controls and streaming query optimization remain incomplete. |
| L7 replication | Partial foundation | `crates/replication` now implements cluster membership/quorum state, dynamic topology guards, replica progress tracking, a framed peer message codec, a live per-peer connection primitive, a `PeerMesh` TCP connection/routing owner with startup retry and post-disconnect reconnect/replacement, retained-WAL `RequestCatchup` streaming plus replica attach-triggered Tier 1 catch-up, explicit `CatchupUnavailable` gap signaling, direct Tier 3 snapshot request when the primary handshake proves retained WAL cannot cover local progress, catch-up-gap fallback to quorum-checked snapshot streaming when a primary snapshot source is installed, a recovery-tier selector, primary retained-WAL floor advertisement in handshakes, promotion request/response transport over the mesh, typed document promotion payloads for L5 `ReadSet`/`WriteSet`, L8 replica document-write routing through `PromotionClient`, configured replica readonly-begin fencing through mesh quorum, subscribe-mode promoted-write success subscription registration plus retry metadata propagation, collection/index/database DDL promotion through primary-owned management intents, a server primary promotion handler that submits decoded document payloads through the real database commit path and applies promoted system-database create/drop plus collection/index DDL through the shared registry, quorum-checked snapshot chunk transport with injectable sinks, a peer-backed WAL sender, a primary hold state, and a `PrimaryReplicator` that implements L5 `ReplicationHook`. The server layer now has database-backed WAL apply and snapshot source/sink bridges exercised through real TCP mesh traffic plus JSON-configured mesh startup, fresh-replica snapshot registration, existing-replica snapshot replacement, multi-database fresh snapshot registration with independent durable registry entries, configured three-node majority-quorum commit coverage with one replica offline, configured three-node fanout coverage with both replicas online and durable progress reflected per replica on the primary, and per-database configured replication entries with database-routed replica promotion and read fencing, L2/L6 persist generation and primary-source applied-LSN progress for replicas, L2/L6 can export and restore checkpointed snapshot images, configured startup uses durable file-header generation for existing primary/replica data and operator generation for fresh data, and primary replica progress plus optional size/age caps are wired into L2 checkpoint WAL retention; richer recovery policy/orchestration and broader multi-node tests remain incomplete. |
| L8 wire/API/server | Partial foundation | `exdb-wire` now implements protocol errors, JSON text/binary frame read/write, LZ4-compressed binary frame payloads, JSON/BSON/Protobuf payload encoding, typed client/server message parsing/serialization, strict unknown-field and `_meta` validation, JWT auth/authorization helpers with active token-expiration enforcement, session management for hello/auth gating, message deduplication, ping, database/collection/index management with inherited server default database config and per-database usage in list responses, transaction lifecycle plus core CRUD/query data operations with `query_id` in `get`/`query` responses, JSON `_meta.types` body metadata for bytes round-trips, JSON `_meta.types` range/filter scalar predicates, replica-role document write plus database/collection/index DDL promotion, replica readonly begin rejection with `quorum_lost` when a configured read gate reports no majority, TCP, TLS-over-TCP, QUIC, plain WebSocket, and secure WebSocket transport listeners, pipelined JSON request input through a bounded per-connection request queue, async management-message dispatch through bounded per-connection response queues, different-transaction concurrent execution with same-transaction ordering, per-transaction scheduler queue accounting, bounded per-connection push-notification queues, per-frame response write timeouts for slow-client backpressure, request/notification priority rotation to prevent push starvation, pushed invalidation notifications, pushed index-ready notifications, Subscribe-chain continuation transactions, Subscribe-mode conflict retry transactions, a runnable TCP/TLS/QUIC/ws/wss server binary, high-level JSON CLI server commands, interactive CLI REPL mode, and raw JSON text CLI server requests. Broader end-to-end multi-message protocol validation remains incomplete. |
| CLI | Mostly implemented | `apps/cli` now supports durable embedded single-database operations for collections, indexes, documents, query, integrity checks, interactive REPL mode for embedded and TCP server workflows, high-level translated TCP server commands for database/collection/index/document/query workflows, and raw JSON text `send-json` requests. |
| README accuracy | Mostly updated | README now describes L1-L6 as substantially implemented, calls out L7, L8 session/server, and CLI gaps, and its embedded quick-start commands now point at compile-checked `exdb` package examples that exercise the current transaction and subscription APIs. The `exdb` crate-level docs now include a runnable doctest for the current embedded transaction/query API, so `cargo test -p exdb --doc` provides real API-drift coverage instead of an empty doc-test pass. |
| Workspace quality gate | Improved | The workspace now passes `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `cargo test --workspace --doc`, `cargo test --workspace --all-targets`, and `git diff --check`. This is verification evidence for the current implementation, not proof that every design/plan item is complete. |

## High-Priority Gaps

### G1: Startup Rollback Vacuum

Design authority:

- `docs/plan/database/00-overview.md` startup step 4 requires rollback vacuum
  when committed timestamps exceed `visible_ts`.
- `docs/plan/database/10-durability.md` startup sequence step 7 requires scanning
  WAL for unreplicated commits and reversing their effects.
- `docs/plan/database/11-test-plan.md` lists rollback-vacuum tests #37-44.

Current evidence:

- `exdb_docstore::RollbackVacuum` exists.
- `Database::open` now rolls back replayed commits with `commit_ts > visible_ts`
  before rebuilding ready secondary indexes. Data rollback removes the exact
  unreplicated primary version, frees external heap bodies, removes newly added
  secondary keys for ready indexes, reclaims zeroed crash-tail pages into the
  free list, and checkpoints the repaired header/free-list/shadow state.
- Durability coverage includes unreplicated create/drop collection and
  create/drop index recovery after replication failure and crash.
- Durability coverage now includes unreplicated insert, replace, and delete
  recovery after replication failure and crash, with post-reopen integrity
  checks proving there are no dangling secondary entries or orphan heap pages.
- Durability coverage also exercises the narrower crash window where a
  `TxCommit` and page mutations have reached storage, replication is still
  pending, no `VisibleTs` record exists, and the process crashes. The test
  reopens twice to prove the rollback result remains stable across a second
  crash/reopen cycle.
- Startup recovery now has explicit fault-injection coverage for a crash after
  rollback cleanup and zeroed-page reclamation run, but before the repair
  checkpoint completes. The next reopen must replay the original WAL, rerun the
  rollback idempotently, hide the unreplicated document from primary and
  secondary-index reads, and pass integrity checks without orphan heap pages.
- Startup recovery also has fault-injection coverage for the same crash window
  with mixed unreplicated DDL: create collection, drop collection, create index,
  and drop index. Rollback now reclaims transient B-tree root pages allocated
  while replaying unreplicated create-collection/create-index records by
  rebuilding the free-list chain idempotently before checkpointing; the next
  reopen verifies restored catalog/index state and clean integrity.
- Building-index crash recovery coverage now also verifies selective cleanup:
  a previously Ready secondary index remains Ready and queryable after a crash
  while another index is still Building, and recovery leaves no Building indexes
  behind.
- Ready secondary-index crash recovery coverage now includes insert, delete,
  replace, compound-index, and array-expanded index entries after reopen,
  proving startup rebuild uses recovered primary versions for more than the
  simplest indexed insert path.
- `catalog_recovery.rs` now has direct handler-level coverage for TxCommit
  replay that creates a collection and inserts a document in the same record,
  data replay against a pre-existing recovered catalog, replace/delete version
  replay, drop-collection catalog replay, create-then-drop replay across
  separate transactions, idempotent duplicate create/insert/delete and
  `IndexReady` replay, Ready secondary-index rebuild from recovered primary
  versions, visible-timestamp monotonicity plus rollback trigger detection,
  informational `Vacuum`, `Checkpoint`, and `RollbackVacuum` record handling,
  skipped legacy/reserved standalone DDL record types (`0x03`-`0x06`),
  highest-commit timestamp tracking across multiple replayed records, and
  corrupt TxCommit payload rejection before recovery can proceed.
- `DESIGN.md` and `docs/plan/database/10-durability.md` now describe the
  implemented TxCommit WAL contract: current TxCommit payloads store primary
  document versions and catalog mutations, while Ready secondary indexes are
  rebuilt from recovered primary versions after WAL replay. Index deltas remain
  live commit-time data for secondary writes, OCC, and subscriptions, not
  persisted TxCommit bytes.
- `catalog_persistence.rs` now validates loaded catalog rows for missing
  index-owner collections and for each collection's required Ready
  `_created_at` index shape, and its isolated tests cover collection
  name-B-tree writes, create/drop index idempotency, scoped index-name cleanup,
  `datos_espanoles` collection-name round trip, and compound nested field-path
  round trip.
- Normal startup recovery now also reconciles page-allocation metadata after WAL
  replay even when no unreplicated rollback was required: zero-filled crash-tail
  pages are reclaimed, the free-list chain is rebuilt from pages physically
  stamped `Free` so checkpoint-era links to replay-allocated B-tree pages are
  dropped, and the durable file-header/shadow pair is refreshed before startup
  integrity checks run. Stress coverage repeatedly opens with full startup
  integrity, performs mixed indexed writes/replaces/deletes, alternates
  checkpointed and uncheckpointed crashes, verifies a model after every reopen,
  and finishes with a clean full integrity report.

L6 test-plan checklist evidence (`docs/plan/database/11-test-plan.md`
non-negotiable requirements):

| Requirement | Evidence |
|---|---|
| Every `apply_*` method in `CatalogPersistence` has idempotency coverage | `catalog_persistence.rs` tests `create_collection_idempotent`, `drop_collection_idempotent`, `create_index_idempotent`, `drop_index_idempotent`, and `apply_index_ready`. |
| Every implemented WAL record type has a replay test | `catalog_recovery.rs` covers `TxCommit`, `IndexReady`, `VisibleTs`, informational `Vacuum`/`Checkpoint`/`RollbackVacuum`, and skipped reserved legacy DDL records. |
| TxCommit replay verifies catalog-before-data ordering | `catalog_recovery::tests::create_collection_and_insert_replayed`. |
| CRUD insert/replace/patch/delete have crash-recovery tests | `durability.rs` tests `committed_data_survives_crash`, `replace_survives_crash`, `patch_survives_crash`, and `delete_survives_crash`. |
| DDL create/drop collection/index have crash-recovery tests | `durability.rs` tests `committed_collection_survives_crash`, `drop_collection_survives_crash`, `committed_index_survives_crash`, and `drop_index_survives_crash`. |
| Data survives checkpoint plus crash | `durability.rs` tests `data_before_checkpoint_survives_crash`, `data_after_checkpoint_survives_crash`, and `checkpoint_then_ddl_then_crash`. |
| Data survives crash without checkpoint | `durability.rs` test `committed_data_survives_crash`. |
| Uncommitted data is not recovered | `durability.rs` test `uncommitted_data_lost_on_crash`. |
| Building-index crash recovery is tested | `durability.rs` tests `building_index_dropped_on_crash`, `building_index_data_intact_after_crash`, and `ready_index_survives_crash_during_other_build`; `integration.rs` also covers `building_index_dropped_on_restart`. |
| Rollback-vacuum basic path is tested | Rollback-vacuum durability tests cover unreplicated insert/replace/delete and DDL rollback after failed replication and crash. |
| Double-crash during recovery is tested | `durability.rs` test `double_crash_recovery` plus rollback fault-injection tests for post-cleanup/pre-checkpoint crashes. |
| Phantom insert OCC is tested with and without `LimitBoundary` | `integration.rs` tests `query_full_range_conflicts_with_phantom_insert_without_limit_boundary`, `query_limit_boundary_allows_insert_beyond_cutoff`, and `query_limit_boundary_conflicts_with_insert_before_cutoff`. |
| Subscription invalidation fires correctly under concurrent writes | `integration.rs` tests `watch_subscription_limit_boundary_fires_for_insert_before_cutoff`, `subscribe_subscription_limit_boundary_produces_continuation`, and `watch_subscription_limit_boundary_filters_concurrent_writes` for separate writer tasks. |
| Active transaction count reaches zero after transactions complete | `integration.rs` test `database_usage_reports_storage_and_memory`. |
| Resource limits are enforced | `stress.rs` tests `max_read_intervals_reached`, `max_operations_reached_by_repeated_transaction_operations`, `max_operations_counts_secondary_scan_work`, `max_operations_counts_create_index_pending_write_validation`, `max_operations_counts_document_index_validation`, `max_operations_counts_drop_collection_index_cascade`, `max_operations_counts_committed_collection_list_rows`, `max_operations_counts_committed_index_list_rows`, `max_operations_counts_pending_catalog_resolution`, `max_operations_counts_pending_collection_duplicate_check`, `max_operations_counts_pending_index_duplicate_check`, `max_operations_counts_pending_collection_list_overlay`, `max_operations_counts_pending_index_list_overlay`, and `max_scanned_docs_reached`; point-get and byte-limit variants cover bypass prevention. |
| Transaction timeout is enforced | `stress.rs` tests `transaction_idle_timeout`, `reset_does_not_revive_idle_timed_out_transaction`, `transaction_max_lifetime`, and `transaction_commit_checks_timeout`. |
| Graceful shutdown waits for active transactions | `integration.rs` tests `close_waits_for_active_transactions_before_storage_shutdown` and `close_timeout_with_hung_transaction`. |

The numbered durability plan table also now has direct file-backed coverage
for previously implicit cases: `ddl_and_data_in_one_tx_rolled_back_on_crash`,
`cascade_drop_collection_indexes_survive_crash`, `empty_checkpoint_harmless`,
`catalog_name_btree_consistent_after_crash`,
`created_at_index_present_after_crash`, and
`index_state_preserved_after_crash`.
The test-plan wording for secondary-index crash recovery and rollback
idempotency now matches the implemented WAL contract: Ready secondary indexes
are rebuilt from recovered primary versions, and explicit `RollbackVacuum`
records are reserved future summaries rather than required for idempotency.

Remaining work:

- Add any additional fine-grained fault-injection points desired inside the
  individual data/index cleanup loops, beyond the current post-rollback
  pre-checkpoint crash windows for data and DDL rollback.

### G2: BSON Document Encoding

Design authority:

- `docs/DESIGN.md` section 1.5 requires BSON for internal storage and wire
  protocol.
- JSON is only an alternative debugging/human-readable wire format.

Current evidence:

- `crates/core/src/encoding.rs` encodes persisted documents as BSON and decodes
  them back to either the JSON-facing embedded API representation or native
  `bson::Document` values.
- JSON `_meta.types` is honored for document bodies: `bytes` values decode from
  base64 into BSON binary, `id` values encode as a private BSON binary subtype
  and round-trip as ULID strings with response `_meta.types`, and top-level
  `_meta` is stripped from persisted BSON/WAL bodies.
- L6 patch handling preserves the DESIGN.md 1.12 boundary: top-level `_meta`
  remains reserved for metadata, while nested user `_meta` fields are legal and
  `_meta.unset` can remove them through explicit nested paths.
- Scalar extraction reads `_meta.types`, so byte/id fields generate typed
  secondary index keys instead of falling back to base64/string keys.
- Commit WAL document bodies are serialized through the same BSON encoder.
- Durable format compatibility is now explicit: file headers reject unsupported
  data-file versions on open, current `TxCommit` WAL payloads use version 3,
  legacy version 2 JSON bodies are converted to BSON during replay, and future
  explicit WAL payload versions are rejected.
- `_created_at` is stored as a BSON datetime and exposed as integer
  milliseconds through the embedded API.
- Wire transport coverage now exercises binary BSON frames end to end through
  the TCP connection loop, L8 session, L6 transaction API, BSON-backed document
  persistence, commit, and readonly get response.
- L8 session coverage exercises JSON `_meta.types` bytes insert/commit/get
  round-tripping with response metadata.
- L8 query parsing now carries query-level `_meta.types` into range and
  post-filter predicate scalar parsing. Coverage includes bytes range
  predicates over a secondary index, bytes post-filter predicates, and parser
  coverage for bytes/id/int64/float64 hints.
- L6 embedded transactions now expose native BSON document methods:
  `insert_bson`, `replace_bson`, `get_bson`, and `query_bson`. Public helpers
  `bson_bytes`, `bson_doc_id`, and `DOC_ID_BINARY_SUBTYPE` let callers build
  native bytes/id values without JSON `_meta.types`. Integration coverage
  inserts and replaces BSON documents, round-trips native bytes/id fields,
  queries bytes and id secondary indexes using typed `Scalar` predicates, and
  verifies JSON compatibility metadata is still available.

Remaining work:

- Consider a fully custom internal typed document value model if the project
  later wants to eliminate the JSON-shaped transaction/write-set representation
  internally. The caller-facing embedded BSON API gap is closed.

### G3: L7 Replication

Design authority:

- `docs/plan/overview/11-implementation-phases.md` phase 9.
- `docs/DESIGN.md` sections 6.x.

Current evidence:

- `crates/replication/src/lib.rs` now defines `ClusterConfig`,
  `ClusterMembership`, `NodeStatus`, `NodeState`, `PeerMessage`,
  `PeerHandshake`, `PeerConnection`, `PeerMesh`, `WalApplier`, `WalSender`,
  `PeerWalSender`, and `PrimaryReplicator`.
- The L7 peer protocol now has deterministic binary payload encoding for
  handshakes, heartbeats, WAL records, WAL acknowledgements, catch-up requests,
  and snapshot begin/data/end messages, plus bounded 4-byte length-prefixed
  async read/write helpers.
- `PeerConnection` performs symmetric handshakes over an async stream, validates
  remote nodes against the cluster topology, updates membership on heartbeat and
  WAL ack messages, applies incoming WAL records through a `WalApplier` hook
  before sending `WalAck`, supports timeout-bounded WAL send/ack waits, and
  handles retained-WAL `RequestCatchup` by streaming filtered `TxCommit` WAL
  payloads back over the same peer connection.
- `PeerMesh` now triggers Tier 1 catch-up automatically when a replica attaches
  to a primary, requesting retained WAL from the replica's durable local
  applied LSN. Server coverage proves a replica can connect after the primary
  already has durable commits and become readable from retained primary WAL.
- `StorageWalCatchupSource` reads retained L2 WAL from a requested LSN and
  filters it to `TxCommit` payloads for replica apply; local replica
  `VisibleTs` records are still written by L6 after each applied commit.
- `PeerWalSender` adapts live peer connections to the existing `WalSender`
  interface, allowing `PrimaryReplicator` to replicate through framed peer
  messages in tests.
- `SnapshotSender` now streams `SnapshotBegin`, `SnapshotData`, and
  `SnapshotEnd` over an existing mesh connection after verifying the sender
  still has quorum. `PeerMesh` exposes an injectable `SnapshotSink`, so database
  reconstruction can be installed without opening side-channel sockets.
- L2 now exports a checkpointed `StorageSnapshot`, converts it to/from bounded
  chunks, restores it into a fresh durable path, and creates the restored WAL at
  the snapshot checkpoint LSN so future appends do not restart at zero.
- L6 wraps that storage primitive as `Database::export_snapshot` and
  `Database::restore_snapshot`; durability coverage restores a database with a
  Ready secondary index, queries it, commits new data on the restored copy, and
  verifies that data survives reopen.
- `apps/server` now provides `DatabaseSnapshotSource` and
  `DatabaseSnapshotSink`, bridging L6 snapshot export/restore into L7
  `SnapshotSource`/`SnapshotSink`. Server coverage drives a retained-WAL gap
  through automatic mesh fallback, streams a real database snapshot over
  loopback TCP mesh traffic, restores it into a fresh path, reads through a
  Ready secondary index, and verifies integrity.
- `PeerMesh` owns the current connection map, exposes lower-NodeId initiation
  decisions, attaches already-established streams from either side of the
  single-connection model, replaces stale connections for the same node,
  broadcasts local progress heartbeats, routes WAL records through
  `PeerWalSender`, and can be used directly to construct `PrimaryReplicator`.
- `PeerMeshRuntime` binds the local TCP listener, updates the local topology
  address from the bound socket, accepts inbound peer streams, starts outbound
  connection tasks only for peers with higher `NodeId`, retries initial outbound
  connection/accept timing with bounded backoff, runs periodic heartbeat
  broadcast plus timeout checks, removes stale disconnected connections, retries
  established outbound peer connections after disconnect, replaces reconnected
  peer handles, closes live peer connections on runtime shutdown, and supports
  graceful runtime shutdown.
- `PrimaryReplicator` implements L5 `ReplicationHook`, pre-checks hold/quorum
  state, sends WAL records through an injectable sender, records successful
  replica WAL acknowledgements, succeeds when acknowledgements plus self retain
  majority, and enters hold state when quorum or acknowledgement majority is
  lost.
- `ClusterMembership` tracks applied LSN/TS progress, counts Suspect nodes
  toward quorum while excluding them from active WAL sends, transitions peers
  through Online/Suspect/Down based on configured timeouts, supports guarded
  address updates/add/remove topology changes, and exposes `min_replica_lsn`.
- `ReplicationHook` now exposes an optional replication retention LSN.
  `PrimaryReplicator` reports the cluster's minimum replica progress through
  that hook, and the L5 replication runner publishes it into L2 after a commit
  reaches acknowledgement majority so checkpoints do not reclaim WAL needed by
  lagging replicas.
- L6 now exposes `Database::apply_replicated_wal`, which validates a received
  TxCommit payload, serializes replica applies, persists the TxCommit and
  local `VisibleTs` records, replays the payload through the startup recovery
  handler, rebuilds Ready secondary indexes for correctness, refreshes durable
  primary/shadow file-header metadata, and advances the in-memory visible
  timestamp and allocator monotonically.
- `apps/server` now provides `DatabaseWalApplier`, a server-side bridge from
  L7 `WalApplier` into the L6 durable apply primitive. Server test coverage
  opens a primary database with `PrimaryReplicator::from_mesh`, sends commits
  over real loopback TCP `PeerMesh` connections, applies them to a separate
  file-backed replica database before acknowledgement, verifies secondary index
  reads plus replica integrity, and separately exercises snapshot source/sink
  reconstruction over real mesh traffic.
- `apps/server` now accepts either a JSON `replication` object for the legacy
  single-database configuration or a `replication` array for multiple
  per-database mesh entries. It validates role/topology/chunk configuration,
  rejects duplicate database entries and mixed primary/replica role sets before
  starting runtimes or mutating the managed registry, creates managed databases
  with `PrimaryReplicator::from_mesh` on primaries, starts replicas with durable
  `replication_applied_lsn` in the local handshake, installs storage-backed
  catch-up and database-backed snapshot source/sink adapters, and owns mesh
  runtime shutdown alongside the wire server.
- Configured server coverage now starts a three-node topology with the primary
  and one replica online and the third node offline, proves the primary mesh has
  majority quorum, commits through the normal L6 database API, and verifies the
  online replica applies the live WAL record.
- Configured primary startup coverage now distinguishes fresh and existing
  local state: fresh primary databases persist the operator-configured
  generation, while `configured_existing_primary_uses_durable_generation`
  proves an existing database's file-header generation is used for the runtime
  and mesh instead of being overwritten by a mismatched configured generation.
- Configured server coverage now also starts two independent per-database mesh
  runtimes for one primary/replica process pair, commits to both managed
  databases through normal L6 handles, verifies each live WAL record reaches the
  matching replica database, and checks catalog isolation so app A does not
  receive app B collections and vice versa.
- Configured replica servers now install a database-routing transaction
  promoter across all configured replica runtimes. Document-write promotions
  dispatch by active transaction database name, DDL promotions route to the
  matching database promoter when the request names one, and unknown document
  write databases fail with a structured promotion error instead of falling
  through to the wrong mesh.
- Configured replica servers now also install a database-routing read gate
  across all configured replica runtimes. L8 readonly `Begin` requests on a
  replica call through that gate and return `quorum_lost` when the matching
  mesh lacks majority quorum. Wire coverage exercises session behavior and the
  public `ServerConfig` transport path; server coverage proves the gate tracks
  `PeerMesh` quorum changes.
- Fresh configured replicas now start without creating an empty database path,
  receive a snapshot when retained WAL is unavailable, register the restored
  path in `SystemDatabase`, install that database into the replica WAL applier,
  and continue applying live WAL from the primary after snapshot registration.
  Multi-database configured coverage now forces two fresh databases through
  snapshot fallback at the same time, verifies each restored catalog remains
  isolated from the other database, applies subsequent live WAL for both, and
  reopens the replica `SystemDatabase` to prove both registry entries and data
  survive restart.
- Existing configured replicas now use the same registering snapshot sink for
  Tier 3 fallback. The sink restores into a temporary path first, clears the
  replica WAL-applier handle, asks `SystemDatabase::drop_database` to close and
  remove the stale registered database only when no external database handles are
  live, renames the restored snapshot into place, registers and opens it, and
  installs the replacement handle for subsequent live WAL. Server coverage
  starts from a stale file-backed replica, forces a retained-WAL gap, verifies
  snapshot replacement through the registry, and then verifies a new primary
  commit is applied through live replication. Fresh snapshot registration and
  existing snapshot replacement coverage now also close and reopen the replica
  `SystemDatabase`, proving the restored registry entry and subsequent live WAL
  data survive a full managed-registry restart. Server snapshot sinks now treat
  a later `SnapshotBegin` as a fresh reconstruction attempt, discarding any
  abandoned partial stream left by a disconnected transfer; regression coverage
  verifies retry for both direct path restore and managed-registry install.
  Replacement failure coverage also holds an external database handle live,
  verifies `DatabaseInUse` aborts the install, and proves the previous
  WAL-applier handle plus old database contents remain in place.
- L7 unit coverage exercises quorum/timeouts, dynamic topology guards,
  peer message payload round-trips and malformed payload rejection, bounded
  async frame read/write, peer handshakes and heartbeat propagation, WAL
  send/apply/ack behavior, WAL ack timeouts, retained-WAL catch-up request
  streaming, storage-backed catch-up filtering, replica attach-triggered
  catch-up, catch-up-gap fallback to snapshot streaming, lower-NodeId mesh
  initiation decisions, mesh stream attachment, WAL routing, connection
  replacement, heartbeat broadcast, real loopback TCP mesh startup, delayed
  higher-node listener startup retry, established TCP connection
  reconnect/replacement, peer-backed primary replication, single-node
  replication, snapshot chunk streaming with quorum enforcement, promotion
  client refusal after a live local role transition from replica to primary,
  majority acknowledgement success, immediate hold-state on missing quorum, and
  hold-state when WAL acknowledgements cannot form majority.
- L6 durability coverage now captures TxCommit payloads from a primary
  replication hook, applies them to a separate file-backed replica database,
  verifies read visibility before and after reopen, and verifies replicated
  index definitions plus subsequent inserts are queryable through a Ready
  secondary index on the replica.
- L8 replica management sessions now promote collection/index DDL as primary
  intents rather than replica-local catalog mutation bytes. The L7 promotion
  response codec can carry an opaque primary response payload, the server
  primary handler executes create/drop collection/index requests through the
  normal L6 management transaction path, and tests cover primary-assigned
  index IDs returning to the replica.
- Replica management sessions now also promote database create/drop intents.
  The server primary promotion handler decodes those system DDL envelopes,
  parses create-database config with the same default-config merge used by
  normal sessions, applies them through `SystemDatabase`, routes promoted
  collection/index DDL by database name through the same registry, and returns
  the primary response payload to the originating replica. Server coverage
  creates a new promoted database and then promotes collection/index DDL against
  that newly created database. Configured server coverage now also drives
  create/drop database DDL from a replica through the live mesh-backed
  `PromotionClient`, proving system catalog DDL promotion is not only a
  handler-local path. That live mesh path now also verifies a promoted drop
  refused by the primary as `database_in_use` is returned to the replica
  without unregistering the database, and that the same promoted drop succeeds
  after the primary-side handle is released. Direct replica-session coverage
  also verifies primary-returned DDL errors such as `database_exists` are
  forwarded unchanged and do not create replica-local registry state.
- Replica document-write promotion now carries the wire transaction's database
  name in a server-level envelope around the existing L5 read/write-set payload.
  The primary promotion handler unwraps that envelope, resolves the named
  database through `SystemDatabase`, and commits the decoded payload through
  that database's normal promoted-transaction path, while still accepting
  legacy unwrapped payloads for the configured database. Wire-session coverage
  verifies replica commits pass the active transaction database to the promoter,
  and server coverage proves a handler configured for one database can route a
  wrapped promoted document write into another registry database without
  touching the default database.
- Primary handshakes now advertise the sender's oldest retained WAL LSN. The
  storage-backed catch-up source reports the current L2 retained floor, so a
  primary that advances retention after checkpoint advertises that live floor
  to newly connecting replicas. L7 and server tests cover payload round-trip,
  mesh-level observation, and configured TCP runtime propagation.
- Replica attach now uses the primary's advertised retained floor to avoid
  doomed retained-WAL requests. If the replica's durable applied LSN is older
  than the primary floor, it sends a `RequestSnapshot` immediately; the primary
  uses the existing quorum-checked snapshot stream or reports
  `CatchupUnavailable`. L7 coverage asserts that this path does not touch the
  catch-up source before snapshot transfer.
- Recovery-tier selection now treats a known generation change as replacement
  state rather than an ordinary reconnect. Local state from a previous
  generation is not used as an incremental catch-up base; WAL-only bootstrap is
  selected only when full history from LSN 0 is retained, otherwise Tier 3 full
  reconstruction is required.
- Configured replica startup now treats an absent or unusable local database as
  a Tier 3 reconstruction case. If an existing local replica database cannot be
  opened with the configured startup checks, the server quarantines the local
  directory for operator inspection, starts the mesh without a registered
  database handle, and forces `RequestSnapshot` on primary attach rather than
  attempting retained-WAL catch-up into a missing database. L7 mesh coverage
  verifies forced snapshot selection even when the primary advertises a
  retained-WAL floor of zero, and server coverage verifies corrupt local
  replica state is quarantined without registering a database. Configured
  replica startup now also probes the durable local file header and pending
  recovery state before opening an existing database. A cleanly closed existing
  replica still reports Tier 1 `IncrementalCatchup` from its durable
  `replication_applied_lsn`, while a crash-stopped existing replica with
  non-checkpoint WAL to replay reports Tier 2 `LocalRecoveryThenCatchup` and
  then recovers the uncheckpointed local catalog mutation during normal open.
  Configured three-node coverage now also exercises the normal all-online path:
  one primary commit is applied by both live replicas, each replica persists
  primary-source applied-LSN progress, and the primary's cluster view records
  each replica's durable progress independently. Existing multi-database
  replica coverage now also forces two stale local databases through independent
  snapshot replacement, verifies catalog isolation, applies subsequent live WAL
  to both restored handles, and reopens the managed registry to prove both
  replacement entries and post-replacement commits survive restart.

Required work:

- Finish richer recovery policy/orchestration beyond the current configured
  Tier 1, Tier 2 startup detection, and Tier 3 attach decisions, extend the
  current per-database and multi-database snapshot registration/replacement path
  into richer system-catalog reconstruction orchestration, and keep broadening
  end-to-end multi-node tests beyond current majority, all-online fanout,
  snapshot fallback, multi-database fresh/replacement snapshot fallback, partial
  snapshot retry, registry-restart, promoted system-DDL, and multi-database
  routing coverage.

### G4: L8 Wire/API/Server

Design authority:

- `docs/plan/overview/11-implementation-phases.md` phase 10.
- `docs/DESIGN.md` section 7.x.

Current evidence:

- `crates/wire` now has `error`, `frame`, and `messages` modules with tests for
  binary header validation, JSON text framing, binary framing, LZ4-compressed
  binary frame round-trips and decompressed-size limits, BSON client payload
  parsing, server response serialization, and payload/header consistency
  checks. Client message parsing now rejects `msg_id = 0`/JSON `"id":0`, so
  only server-initiated messages can use the reserved zero ID. Session and
  transport handling now enforce contiguous client message IDs starting at 1,
  reserve skipped IDs when rejecting them, and still suppress exact duplicate
  retries.
  Transport parse-error handling now also reserves any syntactically valid
  request ID before returning `invalid_message`, so a failed request cannot be
  retried as a different valid request with the same ID. Transport coverage
  includes a binary BSON CRUD round trip through database/collection creation,
  transaction begin/insert/commit, and readonly get. Message coverage includes
  every client message type and every server message type currently listed in
  `DESIGN.md` section 7.
- `crates/wire::auth` now validates JWTs with the configured algorithm/key,
  enforces `exp`, `nbf`, and configured issuer checks, maps roles/databases
  claims into authorization helpers, and covers valid, expired, not-before,
  wrong-issuer, database-scope, admin, and base64-secret cases in tests.
- `crates/wire::session` now sends hello metadata, enforces authentication
  before non-auth messages, schedules an `auth_expired` notification when an
  authenticated connection's JWT expires, also returns `auth_expired` if a
  message races the expiry timer, ignores duplicate nonzero message IDs,
  rejects and reserves skipped message IDs that violate the connection's
  contiguous ordering, tracks the current encoding, handles ping, wires
  database plus collection management requests into `SystemDatabase`/`Database`, and merges
  `create_database.config` overrides onto the server/session default
  `DatabaseConfig`. `list_databases` responses now include point-in-time
  per-database usage for the databases visible to the authenticated client.
- L8 session index management now parses top-level and nested field paths,
  auto-generates deterministic names when `name` is omitted, creates indexes,
  returns the created `index_id`, lists index metadata, drops indexes, rejects
  malformed field paths with `invalid_field_path`, and maps embedded
  `InvalidFieldPath` failures to the same protocol error code. Attempts to
  create or drop reserved system indexes now return `invalid_message` rather
  than leaking as `internal`.
- `crates/wire::session` now stores connection-scoped active transactions and
  maps `begin`, `commit`, `rollback`, `insert`, `get`, `replace`, `patch`,
  `delete`, and `query` messages into the embedded `Database`/`Transaction`
  API through a session-local transaction ID namespace instead of exposing
  internal L6 transaction IDs. It parses wire range/filter/order JSON, maps
  both syntactic and index-semantic range failures to `invalid_range`, maps
  transaction/data errors to protocol error codes, including oversized document
  writes as `doc_too_large` and lifecycle shutdown as `shutting_down`, keeps
  subscription handles alive after subscribing commits, includes server-assigned `query_id` fields
  in `get`/`query` responses, and rejects unknown transaction IDs. Session
  teardown now explicitly rolls back any still-active transactions, removes all
  subscriptions for the connection's session ID across live databases, and
  aborts notification tasks, matching the disconnect cleanup contract.
- `crates/wire::session` can now forward subscription invalidation events into
  server-initiated `invalidation` messages containing the subscribed
  transaction ID, affected query IDs, commit timestamp, and Subscribe-mode
  continuation fields when present. Subscribe continuation transactions and
  Subscribe-mode OCC conflict retry transactions are materialized into the
  session before their session-local `new_tx` identifiers are returned to the
  client, while replica-promotion retry metadata still preserves the
  primary-provided internal retry ID for local L6 materialization.
- L6 public read operations now allocate one query ID per `get`/`query` and use
  that same ID for collection/index catalog lookups plus data read intervals.
  Primary gets record a point interval on `PRIMARY_INDEX_SENTINEL`, and L5 commit
  metadata now includes primary-index key writes for OCC/subscription overlap
  checks.
- L5 commit application now keeps secondary-index entries for retained old MVCC
  primary versions during replace/delete; the old keys are still recorded in
  commit metadata for OCC/subscription overlap, and vacuum remains responsible
  for reclaiming old secondary entries.
- L6 transactions now own cloned internal handles rather than borrowing the
  `Database` facade, allowing a wire session to hold transactions between
  messages. The transaction drop path now decrements `active_tx_count`, and
  insert/replace preserve required `_id` and `_created_at` system fields.
- `crates/wire::transport` now provides `ServerConfig`, `ListenConfig`,
  `TlsConfig`, a per-connection loop that sends hello, reads JSON text or
  binary frames, dispatches through `Session`, mirrors response
  framing/encoding, multiplexes pushed invalidation and index-ready
  notifications as `msg_id = 0`, suppresses duplicate-message responses,
  returns structured invalid-message errors, plus TCP, TLS-over-TCP, plain
  WebSocket, and secure WebSocket listeners with cancellation.
- The stream and WebSocket connection loops accept client-pipelined input
  through explicit bounded per-connection parsed-request queues, dispatch
  management messages to asynchronous workers that return through the
  connection's serialized response writer, dispatch transaction-scoped
  messages to per-transaction workers so different transaction IDs can make
  progress concurrently while preserving same-transaction order, account for
  per-connection transaction scheduler backlog, per-transaction queued
  backlog, and active transaction worker fan-out with `server_busy` when those
  budgets are full, cap per-connection in-flight management workers with
  `server_busy` when that budget is full, and cap each
  response/notification frame write with a configurable timeout: tests now
  write multiple JSON messages before reading responses, including a
  begin-plus-transaction-message pipeline using the first session-local
  transaction ID, same-transaction insert/insert/commit bursts,
  different-transaction insert/commit bursts, a tiny-capacity request queue,
  and scheduler queue saturation, verify ordered correlated same-transaction
  responses, reject unauthenticated management before async dispatch, verify
  successful authentication releases already-queued transaction messages in
  order, reject already-queued pipelined messages after a failed authentication
  attempt until the connection successfully reauthenticates, deduplicate
  management message IDs, reserve invalid-message IDs for retry deduplication,
  reject excess per-transaction queued work, reject excess active transaction
  workers, reject excess in-flight async management work, close idle or active
  authenticated connections after an `auth_expired`
  response/notification, treat `tx_timeout` as a terminal active-transaction
  error in both direct session handling and scheduled connection workers, treat
  `read_limit_exceeded` as a terminal active-transaction error at the same
  layers, bound server-initiated invalidation/index-ready/auth-expiry
  notification queues and spawned task response queues by the same
  per-connection capacity so a stalled client cannot accumulate unbounded push
  events or completed async responses, prefer newly arrived client requests
  over queued notifications after completed responses, preserve queued
  same-transaction terminal error delivery under response-channel
  backpressure, close a slow client that stops draining writes, and validate a
  single-connection pipelined CRUD/query lifecycle across insert, replace,
  delete, commit visibility, deleted-document reads, secondary query readback,
  and rollback cleanup. Transport coverage now also drives a single
  JSON-stream connection through database create/list, collection create/list,
  index create/list, pushed `index_ready`, indexed query readback, index drop,
  collection drop, database drop, and final list verification.
  Protobuf binary frames now use the shared schema in
  `crates/wire/proto/exdb_wire.proto`, preserve exact integer scalar shapes,
  and mirror Protobuf responses through the transport. Mixed JSON/BSON/Protobuf
  transport coverage now proves one stream connection can freely alternate JSON
  text, binary BSON, and binary Protobuf requests while preserving
  request-correlated response framing, typed transaction IDs, committed BSON
  document persistence, and readback; the same mixed-encoding lifecycle now
  runs through real TCP, TLS-over-TCP, QUIC, plain WebSocket, and secure
  WebSocket listeners as well as the direct connection harness. TCP,
  TLS-over-TCP, QUIC, plain WebSocket, and secure WebSocket listener coverage
  now also combine JWT auth with mixed JSON/BSON/Protobuf framing: a binary
  BSON request is rejected as `auth_required` before authentication, then the
  authenticated session creates a database, writes BSON document data, commits
  via Protobuf, and reads the document back across alternating encodings.
  Separate binary BSON and Protobuf coverage proves responses and
  server-pushed invalidations on the same
  connection use the expected frame headers, `msg_id = 0` for pushed messages,
  and most-recent client encoding. Oversized binary frames now preserve the
  decoded request ID/type/encoding from the frame header for both declared-size
  overflow and compressed payloads whose decompressed body exceeds the limit,
  return a bounded `message_too_large` protocol error in the request's binary
  response encoding, reserve the message ID for retry deduplication, and close
  the connection without allocating or draining declared over-limit payloads.
  WebSocket transport coverage now also drives pipelined text-message and
  binary-frame CRUD/query lifecycles through the real listener, including
  begin-plus-transaction-message bursts, same-transaction insert ordering,
  committed readback, query IDs, secondary-index query readback, and rollback
  cleanup. Mixed auth/transport/replica-role coverage now starts real TCP,
  TLS-over-TCP, QUIC, and plain and secure WebSocket listeners with JWT auth
  enabled, `node_role = replica`, and a failing read gate; it verifies
  unauthenticated readonly begin is rejected as `auth_required`, then
  authenticates and verifies the same readonly begin is rejected as
  `quorum_lost` while authenticated ping still succeeds on each listener type.
  TCP, TLS-over-TCP, QUIC, plain WebSocket, and secure WebSocket listener
  coverage now also pipeline `authenticate` and readonly `begin` back-to-back,
  proving queued post-auth replica reads are released in order and fenced by
  the read-quorum gate as `quorum_lost` rather than being rejected with stale
  `auth_required` state. Direct authenticated replica transport coverage now
  also uses a passing read-quorum gate on a pre-seeded managed database, mixes
  JSON, BSON, and Protobuf frames in one session, proves the readonly get path
  succeeds after authentication, and proves a local replica write transaction
  is rejected as `readonly_node` without leaking an active transaction.
  Multi-client authenticated transport coverage now runs two scoped user JWT
  connections against the same managed registry, verifies `list_databases`
  respects the database allowlist, registers a `notify` subscription on one
  connection, commits a replacement on the other, and observes the
  server-pushed invalidation on the subscriber connection. Additional
  multi-client mixed-workload coverage now combines scoped JWT auth, a BSON
  subscriber read/commit path, a pipelined Protobuf writer replace/commit path,
  and a concurrent BSON ping on the subscriber, proving the pushed
  cross-connection invalidation remains request-correlated, uses `msg_id = 0`,
  and preserves the subscriber's most recent binary encoding while ordinary
  traffic continues on the same connection. Multi-client
  resource-pressure coverage now also saturates one connection's asynchronous
  management scheduler with a blocked replica DDL promotion and proves a second
  connection can still receive a normal `ping` response while the first
  connection receives connection-local `server_busy` accounting.
  Scheduler backpressure responses now
  include structured resource-accounting details: management pressure reports
  in-flight counts and limits, and transaction scheduler pressure reports the
  saturated budget, affected transaction ID, queued totals, per-transaction
  queue depth, and active-worker fan-out. The connection event loop now keeps
  completed responses as highest priority but rotates request/notification
  priority after each client request, so a ready stream of client messages
  cannot indefinitely starve server-initiated invalidation, index-ready, or
  auth-expiry notifications. Tests cover management accounting through the JSON
  transport path plus the transaction scheduler's total queue, per-transaction
  queue, active-worker pressure cases, and the request/notification priority
  rotation policy. Transport tests now also cover binary protocol frames over
  both plain and TLS WebSocket listeners. L7 promotion-client coverage now
  verifies a live local role transition from replica to primary prevents later
  write promotion with a role-specific error before contacting the primary
  handler. Additional end-to-end multi-message protocol validation remains to
  be implemented across broader concurrent auth, role-transition, and
  multi-client mixed-workload matrices.
- L6 databases now publish `IndexReadyEvent`s when the background index builder
  writes the `IndexReady` WAL record and transitions the cache to `Ready`.
  `SystemDatabase` fans those events into a registry-level channel, and L8
  sessions forward matching events to connections authenticated for the
  database.
- The L8 transport work also forced B-tree split helpers to stop carrying
  non-`Send` page guards across async allocation/rewrite boundaries, so server
  connection tasks can be spawned safely.
- The wire foundation follows `docs/DESIGN.md` section 7 where it conflicts
  with the older L8 plan sketch; collection/index management messages are
  parsed as management messages carrying `database`, not as transaction-scoped
  DDL.
- `apps/server` now opens a `SystemDatabase`, accepts `--config`,
  `--data-root`, `--listen-tcp`, `--max-message-size`,
  `--request-queue-capacity`, `--response-write-timeout-ms`, and
  `--check-config`, parses the JSON server config shape from `DESIGN.md`
  section 7.10 including `tls.cert_file`, `tls.key_file`,
  `listen.websocket`, `listen.websocket_tls`, and
  `default_database_config`. Top-level `transactions` config from the server
  config is now merged into the default database transaction limits before any
  explicit `default_database_config` overrides, matching the section 7.10
  table while preserving per-database override precedence. Final resolved
  server config validation now rejects TLS, QUIC, or secure WebSocket listeners
  that omit TLS certificate/key material, so `--check-config` cannot approve a
  secure transport configuration that would fail during startup. Validation
  also parses configured TLS certificate/key PEM for enabled secure listeners,
  so malformed TLS material is rejected by `--check-config` before listener
  startup. Enabled JWT auth config now validates HMAC secret presence and
  base64 decoding/non-emptiness plus RSA/EC public-key PEM parsing before
  server startup, so malformed auth key material is also rejected by
  `--check-config`. The server starts the TCP/TLS/QUIC/ws/wss wire server with
  Ctrl-C shutdown.
- `apps/cli` now provides a command-mode client for embedded database paths:
  collection/index/document CRUD, query using the same JSON range/filter shape
  as the wire protocol, and integrity checks. In TCP server mode it translates
  the same high-level commands into JSON protocol messages, including
  transaction begin/commit/rollback around document and query operations,
  database management commands, and the raw JSON `send-json` escape hatch. The
  `repl` mode reuses the same embedded/server command executor, supports
  `help`/`exit`/`quit`, shell-style quoting for JSON arguments, and JSON-shaped
  result/error output. `SystemDatabase` now persists registry metadata under
  `_system/registry.json`, but embedded CLI operations still intentionally
  target a single database path; multi-database management is exposed through
  server mode.
- L8 transport lifecycle coverage now proves the DESIGN 7.3.3 disconnect
  contract end to end: dropping a client with an active uncommitted transaction
  rolls the transaction back before a later connection can observe it, and
  connection-owned subscriptions are removed from the database registry when
  the session task exits. Auth-expiry lifecycle coverage now also proves a
  server-initiated `auth_expired` close rolls back an active uncommitted
  transaction before another authenticated connection can read the database.

Required work:

- Continue broadening end-to-end multi-message protocol validation beyond the
  current structured backpressure-accounting, mixed-encoding,
  priority-rotation, disconnect cleanup, management/index/drop lifecycle,
  sequential and pipelined authenticated replica WebSocket read-gate coverage,
  and pipelined CRUD/query lifecycle coverage.

### G5: Production Hardening

Design authority:

- `docs/plan/overview/11-implementation-phases.md` phase 11.
- `docs/DESIGN.md` section 2.13 and resource-control sections.

Current evidence:

- The repository now has a clean warning-deny Clippy gate across all workspace
  targets. The cleanup removed real issues such as async catalog mutation paths
  awaiting while holding a catalog lock, tightened test/runtime configuration
  construction, and brought database, wire, server, CLI, and studio targets
  under the same lint standard.
- Page checksums are now verified on buffer-pool cache misses for both shared
  and exclusive fetches, and direct buffer-pool flushes stamp checksums before
  writing.
- `StorageEngine::check_integrity` and `Database::check_integrity` provide
  integrity-check phases for file header validation, page scans,
  checksum/layout checks, durable data-file physical-size validation, and
  free-list walk/cycle detection.
- `StorageEngine::check_integrity` now strict-scans retained WAL frames from
  the oldest retained LSN to the logical WAL end, verifies WAL record CRCs, and
  reports truncated, zero-length, oversized, or checksum-mismatched frames
  instead of silently treating them as end-of-log. CLI JSON output exposes
  `wal_records_scanned` and `wal_bytes_scanned` integrity counters.
- `Recovery::needs_recovery` now applies the same complete-frame/CRC validation
  to the post-checkpoint WAL probe, so a truncated or checksum-bad WAL tail is
  not misclassified as a valid recovery workload. Focused recovery tests cover
  clean state, valid WAL, DWB-triggered recovery, truncated WAL tail, and
  CRC-bad WAL tail.
- `StorageEngine::check_integrity_with_btree_roots` and
  `Database::check_integrity` now validate supplied catalog/primary/secondary
  B-tree roots for local key ordering, cell encoding, child-page reachability,
  leaf sibling sanity, double allocation, and unreachable B-tree pages.
- `Database::check_integrity` now adds semantic primary/secondary index
  cross-reference validation for ready indexes: dangling secondary entries,
  non-empty secondary values, malformed primary/secondary MVCC keys, invalid
  primary cell bodies, and missing expected secondary entries are reported.
- `Database::check_integrity` now traces external primary bodies through heap
  slots and overflow chains, validates heap/overflow page shape and body length,
  and reports heap/overflow pages not reachable from any external primary body
  as orphan warnings.
- `Database::check_integrity` now validates catalog-level invariants including
  collection name-index consistency, stale collection-name entries, duplicate
  collection/index names, index ownership, reserved system index names, root
  page sanity, and the required Ready `_created_at` index shape on every
  collection.
- Catalog index-name B-tree entries are now keyed by `(collection_id, name)`,
  matching the in-memory catalog and API scope. New writes remove legacy
  globally keyed index-name entries, and full integrity checks report any
  remaining legacy or mismatched index-name entries.
- Durable storage now reserves the last page for a `FileHeaderShadow`, writes a
  matching shadow copy during initialization, checkpoint, and close, restores
  page 0 from the shadow when the primary header is corrupt, and reports shadow
  mismatches during integrity checks.
- `StorageEngine::repair_integrity` conservatively repairs durable
  file-header/shadow-header drift by rewriting both copies from the verified
  in-memory header, and now truncates extra trailing `data.db` bytes when the
  physical file is larger than the live page count. The root-aware storage
  repair pass also validates complete B-tree leaf sibling chains and rebuilds
  broken reachable leaf `right_sibling` pointers from a full root walk when the
  tree is otherwise structurally valid. `Database::repair_integrity` delegates
  to that root-aware storage pass before running semantic repairs, so catalog,
  primary, and secondary B-trees use the same sibling-chain repair path.
  `Database::repair_integrity` also performs a full semantic pre-check and
  rebuilds the catalog name-index B-tree from authoritative catalog ID entries
  when name-index corruption is detected, including stale collection-name
  entries, missing expected names, and legacy unscoped index-name entries. When
  active primary-index B-tree corruption is detected and retained WAL covers
  the complete history from LSN 0, it resets active primary roots, replays
  visible `TxCommit` document versions from retained WAL, and then rebuilds
  Ready secondary indexes from the reconstructed primaries. When Ready
  secondary-index corruption is detected, including semantic entry drift or
  valid-checksum structural B-tree damage on a Ready secondary root, it resets
  and rebuilds Ready secondary index trees from authoritative primary versions
  before checkpointing. It also reclaims warning-severity orphan
  B-tree, heap, and overflow pages into the free list and checkpoints the
  repaired free-list state. Integration coverage corrupts the catalog name
  index, repairs it, and
  verifies expected scoped entries are restored while stale/legacy entries are
  removed. File-backed coverage then crashes after the catalog name-index repair
  checkpoint, reopens with full startup integrity enabled, and verifies the
  repaired catalog remains clean and usable. Secondary-index coverage corrupts
  a Ready secondary index with both a missing required entry and a dangling
  entry, verifies the corruption is reported, repairs it, queries through the
  rebuilt index, and verifies clean post-repair integrity. Coverage now also
  corrupts a Ready secondary root page to a structurally invalid page type,
  verifies `check_integrity` reports the issue rather than aborting, repairs it,
  queries through the rebuilt index, and verifies the expected secondary key is
  restored. File-backed coverage now repeats both semantic and structural
  secondary-index rebuilds before a simulated crash/reopen with full startup
  integrity checking to prove the repair checkpoint is durable. Primary-index
  coverage corrupts an active primary root to a valid-checksum non-B-tree page,
  rebuilds it from fully retained WAL, verifies point reads and secondary-index
  queries, then repeats the repair on a file-backed database before a simulated
  crash/reopen with full startup integrity checking.
  Separate coverage creates orphan heap and B-tree pages, repairs them into the
  free list, verifies the orphan findings disappear, and now repeats the
  orphan-page repair on a file-backed database before a simulated crash/reopen
  with full startup integrity checking to prove the repair checkpoint is
  durable. Orphan-page reclamation is now explicitly deferred whenever the
  pre-repair report contains hard error-severity findings, so unrelated leaked
  pages are not reclaimed while primary or structural corruption still requires
  operator attention. Combined repair coverage now corrupts catalog-name
  metadata, a Ready secondary index, and orphan heap/B-tree pages in the same
  database, verifies all three repair categories are applied in one pass,
  confirms catalog and secondary queries remain usable, and then runs
  `repair_integrity` a second time to prove the repaired state is clean and
  idempotent. Free-list repair coverage now corrupts a file-backed
  free-list chain into a cycle, repairs it through
  `Database::repair_integrity`, crashes before close, and reopens with full
  startup integrity enabled to prove the rebuilt free-list/header/shadow state
  is durable. Durability coverage also appends an extra byte to `data.db`,
  verifies integrity reports the physical-size
  mismatch, repairs it through the database API, and reopens with full startup
  integrity enabled to prove existing documents remain readable.
- Durable single-node checkpoints now reclaim sealed WAL segments whose end LSN
  is covered by the checkpoint while always retaining the active segment.
- Replica WAL apply now persists the highest primary-source WAL LSN in the L2
  file header, keeps duplicate/older replays monotonic, exposes the value
  through L6, verifies it survives reopen, and the server mesh bridge test
  compares the primary cluster's recorded replica progress against the
  replica database's durable progress.
- Checkpoints now honor an optional replication retention LSN and optional
  size/age retention caps, while always retaining the active segment. L7
  primary progress now feeds real replica lag into this hook after
  acknowledgement majority; if configured caps force old WAL reclamation, a
  replica beyond the retained window can use the L7 catch-up-gap fallback path
  once a primary snapshot source and replica snapshot sink are installed.
- `SystemDatabase` now creates `_system/`, persists a versioned registry
  manifest in `_system/registry.json`, reloads active database entries on
  reopen, validates duplicate names/IDs, preserves database IDs/configs across
  reopen, persists restored-database registration, and keeps dropped entries
  removed after restart. The implementation currently uses a JSON manifest and
  eager database opens rather than the original internal `_system` database
  catalog design.
- `SystemDatabase` create/drop now uses durable lifecycle states: create
  persists `Creating` before opening and promotes to `Active` after success;
  drop persists `Dropping`, rejects live shared handles with `DatabaseInUse`,
  closes the owned handle, removes the data directory, and finally removes the
  registry entry. Startup cleanup removes `Creating`/`Dropping` directories and
  rewrites the manifest without those transitional entries.
- `DatabaseConfig::check_on_startup` and `check_on_startup_full` now run quick
  or full integrity checks during open after recovery and before background
  tasks start. Durability coverage proves quick startup integrity rejects both
  cold page checksum corruption and malformed durable data-file physical size.
- `DatabaseConfig::validate` now rejects unusable storage/transaction settings
  before database open, snapshot restore, or system-registry create persist any
  state. L8/server JSON config merging calls the same validation and returns
  `invalid_message`/configuration errors for invalid resource values such as a
  memory budget below page size, a WAL segment size that cannot hold the
  32-byte segment header plus payload, zero storage checkpoint controls, or
  zero transaction work budget. It also validates `external_threshold` against
  the primary B-tree inline cell budget, rejecting configurations that would
  force too-large document bodies into inline primary records and fail later
  during commit.
- General auto-repair is now limited to file-header metadata, including durable
  `page_count`/`free_list_head` refresh and primary/shadow reconciliation,
  trailing data-file byte truncation, free-list chain rebuild from physically
  `Free` pages when the free-list walk reports corruption, reachable B-tree
  leaf sibling-chain rebuilds, catalog name-index rebuilds, Ready
  secondary-index semantic and structural rebuilds, conservative primary-index
  rebuild from fully retained WAL, and orphan-page reclamation; broader WAL-redo
  page repair when the required log range has already been reclaimed remains
  open and belongs to snapshot/operator recovery.
- Database config max document size and transaction read/scan/work limits are
  now enforced by L6 transactions. `TransactionConfig::max_operations` bounds
  coarse CPU-style work units across public transaction operations, pending
  write-set merge work, scanned secondary-index rows, and pending write-set
  validation during create-index operations. Insert/replace paths also charge
  per-index document validation work, including pending indexes created earlier
  in the same transaction, and collection drops charge cascade-index metadata
  work. Catalog list operations charge committed result rows plus pending
  catalog-overlay work for in-transaction collection/index creates and drops.
  Collection resolution also charges pending catalog mutation scans before
  resolving read-your-writes DDL, and pending collection/index duplicate checks
  are bounded by the same operation budget. Direct point reads now charge
  scanned document and byte limits before document decode, so `get` cannot
  bypass the scan accounting enforced by query primary gets. Exceeding a
  read, scan, or work limit now moves the embedded transaction into a terminal
  aborted state so later operations, `reset`, and `commit` cannot continue the
  transaction after `read_limit_exceeded`. `reset` now also checks timeout
  before refreshing activity, so an already idle-expired transaction cannot be
  revived by resetting its read/write sets; `commit` also enforces transaction
  timeout before submitting to the commit coordinator.
- `Database::close` now follows the shutdown contract more tightly: it cancels
  background work, waits for active transactions with the existing timeout,
  joins the cancellable checkpoint and index-builder tasks, and only then runs
  the final storage close. Integration coverage proves close does not complete
  while a transaction is still active.
- The active-transaction close wait is now controlled by
  `DatabaseConfig::close_timeout` instead of a hard-coded duration. Validation
  rejects zero close timeouts, wire/server JSON database config parsing can set
  the value, legacy system-registry manifests missing the field deserialize
  with the default, and integration coverage proves an abandoned transaction
  does not block close beyond the configured timeout.
- `DatabaseConfig::max_disk_usage_bytes` now enforces a page-store/WAL disk
  quota during normal WAL appends and page allocation, and wire/server database
  config parsing can inherit or override that quota. Validation also rejects
  durable database quotas below the bootstrap footprint needed for the file
  header, shadow header, two catalog roots, and initial WAL segment header, so
  unusable quotas fail before database creation/open rather than midway through
  initialization. File-backed L6 durability coverage now drives the normal
  transaction commit path into a WAL quota failure and verifies the rejected
  document is not visible before or after reopen, with clean integrity.
- `StorageEngine::usage`, `Database::usage`, and
  `SystemDatabase::database_usage` expose per-database page-store bytes,
  retained WAL bytes, total disk bytes, configured memory budget, used buffer
  frames, active transaction count, page count, and page size. L8
  `list_databases` responses include these usage fields for each visible
  database, including active wire transactions observed through a real stream
  connection, and the CLI server `list-databases` command returns them
  unchanged. Direct JSON transport coverage and CLI server-mode coverage now
  both assert all usage fields are present and verify the accounting
  relationships `disk_usage_bytes = page_store_bytes + wal_retained_bytes` and
  `page_store_bytes = page_count * page_size`, including active transaction
  count transitions through begin/rollback at the wire layer.
- `exdb-wire::ServerConfig` and `exdb-server` now validate transport-level
  resource controls, rejecting zero `max_message_size`, zero
  `request_queue_capacity`, and zero `response_write_timeout_ms` before
  direct wire server start/connection handling, `--check-config`, or server
  startup can succeed.
- Coarse CPU/work limits now exist at the L6 transaction layer, and L8
  scheduler-level resource controls now bound per-connection request parsing,
  response delivery, notification delivery, async management fan-out,
  per-transaction backlog, and active transaction workers with structured
  `server_busy` accounting when those budgets are saturated. Remaining resource
  hardening is broader operational accounting and soak/fault coverage rather
  than a missing scheduler backpressure mechanism.
- Crash-loop coverage now includes repeated file-backed open/mutate/crash
  cycles with full startup integrity, secondary-index queries, replacements,
  deletes, checkpoints, and model verification after each recovery. Deterministic
  randomized file-backed soak coverage now drives
  insert/replace/delete/query/checkpoint/crash/reopen choices from a fixed
  pseudo-random seed, verifies point reads plus secondary-index bucket counts
  against a model after each recovery, and finishes with a full integrity
  check. Process-level crash coverage now includes
  `process_abort_file_backed_recovery_preserves_indexed_model`, which spawns a
  child test process for each phase, commits indexed mutations/checkpoints,
  fsyncs an external model marker, aborts the process without closing the
  database, and verifies recovery plus full integrity in the parent after every
  abort.
- Reader-heavy stress coverage now includes a file-backed
  `hundred_readers_one_writer_integrity_smoke` test with 100 concurrent
  readonly tasks performing secondary-index queries and seed document reads
  while one writer commits indexed inserts, followed by checkpointed integrity
  and reopen integrity checks.
- G5/G6 stress coverage now also includes
  `concurrent_randomized_file_backed_soak_survives_crash_recovery`, which runs
  concurrent limited secondary-index readers, randomized insert/replace/delete
  writers, checkpointing during activity, exact model verification, repeated
  crash/reopen cycles, and final full integrity. This closed two concrete
  pressure gaps: buffer-pool checkpoint clean-marking now uses a per-frame
  dirty generation so same-LSN post-snapshot writes stay dirty, and L6
  secondary queries fetch the exact primary version verified by the secondary
  entry so concurrent replacements cannot change the decoded body between
  verification and result production. The full 1M-operation or randomized
  process-kill soak remains outside the normal test suite.

Required work:

- Implement broader auto-repair, broader operational resource
  controls/accounting, larger extended randomized soak coverage, and broader
  randomized process-level crash/fault-injection tests.

### G6: Transaction Query Integration

Design authority:

- `docs/DESIGN.md` sections 5.4, 5.6.3, and 5.7.
- `docs/plan/database/05-b5-transaction.md` query flow.

Current evidence:

- L4 implements `merge_with_writes` and has direct unit tests for inserts,
  deletes, replacements that stay in range, replacements that move into/out of
  range, filters, sort order, limits, and pending rows on array-expanded
  secondary-index keys.
- L4 `merge_with_writes` now preserves streaming behavior for
  snapshot-only/delete-overlay limited scans, stopping once enough visible rows
  are returned instead of materializing the entire snapshot before truncation;
  direct tests count source polls for plain limits plus forward and backward
  delete-overlay limits.
- L5 implements `LimitBoundary` storage, merging, conflict checks, and
  subscription invalidation behavior.
- L6 transaction queries now build a `MergeView` from the current write set,
  merge pending inserts/deletes/replaces through L4, and compute full secondary
  keys for `LimitBoundary::Upper`/`Lower` when a nonzero limit is reached.
- Limited L6 transaction queries now merge pending write-set rows against the
  committed secondary stream incrementally and stop once enough ordered rows
  are known, instead of materializing the whole committed source range.
- Database integration tests cover query read-your-writes for pending insert,
  delete, and replace-across-range cases, plus forward and backward
  limit-boundary OCC behavior.
- Database integration tests also cover L6-produced limit boundaries through
  subscription invalidation for `Notify`, `Watch`, and `Subscribe`, including
  no invalidation beyond the cutoff, Watch persistence, and Subscribe chain
  continuation metadata.
- Database integration tests cover limited scans respecting tight
  `max_scanned_docs` limits, including a pending write-set row winning the first
  result. Coverage now also includes a larger mixed pending insert/delete/
  replace workload with a post-filtered limited secondary scan under
  `max_scanned_docs = 1`, proving the query can satisfy its limit from pending
  rows after one hidden committed row without scanning a long committed tail in
  both forward and backward order.
- Database integration tests cover the planned zero-limit edge case:
  `query_with_limit_zero_returns_empty_without_data_interval` proves
  `limit = Some(0)` returns an empty result with `max_scanned_docs = 0`, does
  not conflict with a later matching insert, and contrasts that with a nonzero
  limit that does hit `ReadLimitExceeded` under the same scan budget.
- Limited L6 secondary scans now use keyed committed scan entries and the same
  array-expanded secondary key generation used by committed index writes, so
  pending write-set rows that sort before the next committed row can satisfy a
  limit before fetching/filtering that committed document body. Regression
  coverage verifies this with a post-filtered pending row under
  `max_scanned_docs = 1` and pending array-index insert/replace rows under both
  limited and unlimited queries.
- Database integration tests now cover compound array-expanded secondary
  indexes end to end, post-filter-heavy limited scans that must skip several
  nonmatching rows before satisfying the limit, and mixed pending
  insert/delete/replace workloads merged with a filtered limited query.
- Database stress coverage now includes
  `pending_mixed_query_model_stress_matches_transaction_view`, a deterministic
  transaction-local model test that repeatedly mixes pending inserts, deletes,
  and replacements that move documents across compound `(bucket, rank)`
  secondary-index order and the `active` post-filter, then validates limited
  forward and backward queries before commit against the modeled pending view.
- Concurrent G5/G6 stress coverage now proves limited secondary scans remain
  predicate-correct while a writer concurrently replaces indexed documents
  across buckets: `concurrent_randomized_file_backed_soak_survives_crash_recovery`
  repeatedly queries limited bucket ranges during randomized writes, verifies
  all results against the indexed predicate, then crash/reopens and checks the
  exact external model.
- L6 now rejects unsupported compound-array index states before they can become
  durable catalog/build work: creating a compound index over an existing
  document with two array-valued indexed fields returns a transactional error,
  and inserts/replaces validate against both committed and pending index
  definitions in the same transaction.
- Embedded L6 index creation now validates index field-path definitions
  directly, rejecting empty index definitions, empty field paths, and empty path
  segments before any catalog mutation is buffered.
- `get()` now follows the design's read-your-writes shortcut for pending
  document mutations: after collection resolution, a self-written insert,
  replace, or delete returns from the write set without recording a primary
  index point-read interval. Regression coverage proves this avoids consuming
  an extra `max_intervals` slot and avoids registering a document subscription
  from a self-written `get()`.
- Catalog list operations now expose a transaction-local DDL view:
  `list_collections()` overlays pending creates and drops on top of the
  committed catalog cache, and `list_indexes()` overlays pending index creates
  and drops for the resolved collection. Integration coverage includes the
  planned `list_collections_includes_pending_creates` and
  `list_indexes_includes_pending_creates` regressions, plus pending-drop hiding
  and duplicate pending index-create rejection.
- `tx.reset()` now matches the design contract: it clears the read/write sets
  and query ID counter while preserving the transaction's original `begin_ts`
  snapshot. Regression coverage starts a transaction, commits a later document
  from another transaction, resets the first transaction, and verifies the
  later document remains invisible.

Required work:

- Keep broadening query execution coverage for larger mixed read/write
  workloads, streaming behavior, and stress/fault-injection combinations.

## Current Next Slice

The next implementation slice should continue from the audit rather than
assuming completion from the now-clean workspace gates. Candidate areas remain
**G5 production hardening** and **G3/G4 protocol layers**: replication recovery
orchestration, remaining repair work, operational resource accounting, broader
L6 query streaming stress coverage, and broader end-to-end protocol validation
are still open until each is reconciled against the design/plan.
