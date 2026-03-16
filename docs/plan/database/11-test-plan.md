# L6 Exhaustive Test Plan

## Testing Philosophy

**The database layer is where bugs become catastrophic.** A query bug returns wrong results. A durability bug silently loses committed data. An OCC bug allows non-serializable schedules. A catalog recovery bug orphans entire collections.

Every test in this plan exists because there is a specific failure mode it guards against. Tests are not optional padding — they are the *specification* of correct behavior. If a scenario is not tested, it is not guaranteed to work.

### Test Categories

| Category | Purpose | Approximate Count |
|----------|---------|-------------------|
| **Unit** | Individual module correctness (B1-B4, B7) | ~80 |
| **Transaction** | B5 transaction API correctness | ~60 |
| **Integration** | B6 full-stack with in-memory storage | ~40 |
| **Durability** | File-backed crash recovery, WAL replay, catalog persistence | ~70 |
| **Concurrency** | Multi-transaction conflicts, background tasks | ~30 |
| **Stress** | Load, limits, edge cases | ~20 |
| **Total** | | **~300** |

---

## B1: Config Tests (4 tests)

Already specified in `01-b1-config.md`. No changes needed.

| # | Test | Validates |
|---|------|-----------|
| 1 | `default_database_config` | All defaults match DESIGN.md values |
| 2 | `default_transaction_config` | All defaults match DESIGN.md 5.6.5, 5.6.6 |
| 3 | `custom_config` | Fields can be overridden and round-trip through clone |
| 4 | `external_threshold_default` | Defaults to page_size / 4 |

## B2: Catalog Cache Tests (16 tests)

Already specified in `02-b2-catalog-cache.md`. No changes needed.

## B3: Index Resolver Tests (5 tests)

Already specified in `03-b3-index-resolver.md`. No changes needed.

## B4: Catalog Tracker Tests (10 tests)

Already specified in `04-b4-catalog-tracker.md`. No changes needed.

## B5: Transaction Tests (50 → 65 tests)

The existing 50 tests from `05-b5-transaction.md` plus 15 additional durability-aware tests:

### Existing Tests (1-50)

As specified in `05-b5-transaction.md`.

### Additional Transaction Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 51 | `insert_meta_stripped_from_body` | `_meta` key removed before buffering in WriteSet | `_meta` must never reach the WAL — it's a client-side directive, not data |
| 52 | `insert_preserves_all_other_fields` | Non-system fields (`_id`, `_created_at`, `_meta`) pass through unchanged | Regression guard — stripping logic must not be greedy |
| 53 | `replace_overwrites_entire_body` | Replace with `{"x":1}` on doc `{"a":1,"b":2}` → only `{"x":1,"_id":...,"_created_at":...}` | Replace is full overwrite, not merge |
| 54 | `patch_null_stores_explicit_null` | `patch({"field": null})` stores null, not removes field | DESIGN.md 1.8 — null is a value, not deletion |
| 55 | `query_records_correct_index_id_in_interval` | ReadInterval uses the resolved IndexId, not a sentinel | Wrong index_id → OCC misses real conflicts |
| 56 | `commit_returns_correct_commit_ts` | `TransactionResult::Success::commit_ts` matches the allocated timestamp | Clients rely on commit_ts for ordering |
| 57 | `multiple_inserts_same_collection` | Insert 100 docs in one tx, all visible via query | Batch correctness |
| 58 | `insert_after_delete_same_doc` | Delete doc, insert new doc with same data → new DocId | Delete + insert is not an update |
| 59 | `query_empty_collection_returns_empty` | Query on collection with no docs → empty vec, not error | Must still record ReadInterval |
| 60 | `query_records_interval_even_on_empty_result` | ReadInterval recorded even when 0 rows returned | OCC must detect phantom inserts into empty ranges |
| 61 | `create_collection_name_validation` | Names with null bytes, empty string, very long names → error | Null bytes break catalog B-tree key encoding |
| 62 | `create_index_duplicate_fields` | `create_index("users", "idx", [age, age])` → error or allowed? | Define and test the behavior |
| 63 | `query_with_limit_zero` | `limit = Some(0)` → empty result, no ReadInterval | Edge case — is this valid? |
| 64 | `write_set_ordering_deterministic` | Multiple inserts to same collection → consistent ordering in MergeView | Non-deterministic ordering → non-deterministic LimitBoundary → OCC flakiness |
| 65 | `transaction_check_timeout_updates_last_activity` | Successful operation updates `last_activity` | Prevents false timeout on active transactions |

## B6: Database Integration Tests (26 → 55 tests)

### Existing Tests (1-26)

As specified in `06-b6-database.md`.

### Additional Integration Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 27 | `open_in_memory_then_insert_and_query` | Full CRUD cycle with in-memory storage | Baseline sanity for all other tests |
| 28 | `two_readers_one_writer_no_conflict` | Reader tx + writer tx on disjoint keys → both succeed | Basic MVCC isolation |
| 29 | `write_write_conflict_detected` | Two writers on same key → one gets Conflict | OCC fundamentals |
| 30 | `ddl_data_atomic_rollback` | Create collection + insert, then rollback → neither visible | Atomicity of mixed DDL+data tx |
| 31 | `subscription_watch_fires_on_relevant_write` | Watch on range [A,M], write in range → event fired | Subscription correctness |
| 32 | `subscription_watch_silent_on_irrelevant_write` | Watch on range [A,M], write in range [N,Z] → no event | False positive suppression |
| 33 | `subscription_notify_fires_once` | Notify mode fires exactly once, then next_event → None | One-shot semantics |
| 34 | `begin_after_shutdown_returns_error` | `close()` then `begin()` → ShuttingDown error | Lifecycle correctness |
| 35 | `checkpoint_reduces_recovery_time` | Insert 1000 docs, checkpoint, insert 10 more, simulate restart → only 10 replayed | Checkpoint effectiveness |
| 36 | `vacuum_removes_old_versions` | Insert, replace 5 times, trigger vacuum → old versions cleaned | MVCC garbage collection |
| 37 | `index_builder_completes` | Create index on populated collection → state transitions Building → Ready | Background build lifecycle |
| 38 | `query_building_index_returns_error` | Query on Building index → IndexNotReady | Prevents partial results |
| 39 | `concurrent_index_build_and_writes` | Create index, insert docs concurrently → all docs indexed | Builder + commit coordinator cooperation |
| 40 | `large_document_insert_and_retrieve` | Insert 1MB document → get returns identical bytes | External storage (heap) path |
| 41 | `document_at_size_limit` | Insert doc at exactly max_doc_size → succeeds | Boundary condition |
| 42 | `document_over_size_limit` | Insert doc at max_doc_size + 1 → DocTooLarge | Boundary condition |
| 43 | `100_collections_in_one_tx` | Create 100 collections in single tx → all visible after commit | Stress catalog mutations |
| 44 | `drop_collection_then_recreate_same_name` | Drop "users", create "users" in new tx → works, new CollectionId | Name reuse correctness |
| 45 | `query_created_at_ascending` | Insert A, B, C → query _created_at ASC → returns A, B, C | Auto-index ordering |
| 46 | `query_created_at_descending` | Insert A, B, C → query _created_at DESC → returns C, B, A | Reverse ordering |
| 47 | `list_collections_includes_pending_creates` | In-tx: create "foo", list_collections → includes "foo" | Read-your-writes for catalog |
| 48 | `list_indexes_includes_pending_creates` | In-tx: create index, list_indexes → includes new index | Read-your-writes for catalog |
| 49 | `three_way_occ_conflict` | TX1 reads K, TX2 writes K, TX3 writes K → only one succeeds | Multi-writer correctness |
| 50 | `snapshot_isolation_read_consistency` | TX1 reads, TX2 commits, TX1 reads again → same result | Snapshot doesn't shift mid-transaction |
| 51 | `reset_then_see_committed_changes` | TX1 reads (no data), TX2 inserts + commits, TX1.reset(), TX1 reads → still no data (same snapshot) | Reset preserves begin_ts |
| 52 | `active_tx_count_correct_after_panic` | Begin tx, panic in user code → Drop cleans up count | Leak prevention |
| 53 | `close_timeout_with_hung_transaction` | Begin tx, don't commit, call close with short timeout → close completes | Graceful shutdown with abandoned tx |
| 54 | `startup_drops_building_indexes` | Create index (Building state), crash, reopen → index gone, collection intact | Crash safety for index builds |
| 55 | `startup_validates_created_at_invariant` | Corrupt catalog: collection without _created_at → startup error | Integrity validation |

## B7: Subscription Handle Tests (11 tests)

Already specified in `07-b7-subscription.md`. No changes needed.

## B8: System Database Tests (12 tests)

Already specified in `08-b8-system-database.md`. No changes needed.

---

## Catalog Persistence Tests (20 tests)

These test the `catalog_persistence.rs` module in isolation, using a real storage engine but no WAL replay.

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 1 | `load_empty_catalog` | Empty B-trees → empty CatalogCache | Fresh database startup |
| 2 | `load_single_collection` | One collection in B-tree → correct CollectionMeta | Basic deserialization |
| 3 | `load_collection_with_indexes` | Collection + 3 indexes → all present in cache | Index-collection association |
| 4 | `load_sets_next_ids_correctly` | Collections with IDs 5,10,15 → next_collection_id = 16 | ID allocator initialization |
| 5 | `load_validates_created_at_exists` | Collection without _created_at → error | Invariant enforcement |
| 6 | `load_validates_index_references_collection` | Index references non-existent collection → error | Referential integrity |
| 7 | `load_handles_building_indexes` | Building index in B-tree → loaded with Building state | State preservation |
| 8 | `apply_create_collection_roundtrip` | Create → load → matches original | Write + read consistency |
| 9 | `apply_create_collection_idempotent` | Apply same create twice → no error, same state | WAL replay safety |
| 10 | `apply_drop_collection_removes_from_btrees` | Create → drop → scan B-trees → empty | Cleanup completeness |
| 11 | `apply_drop_collection_idempotent` | Drop non-existent → no error | WAL replay safety |
| 12 | `apply_drop_collection_cascades_indexes` | Collection with 3 indexes → drop → all 4 entries gone | Cascade correctness |
| 13 | `apply_create_index_roundtrip` | Create → load → matches original | Write + read consistency |
| 14 | `apply_create_index_idempotent` | Apply same create twice → no error | WAL replay safety |
| 15 | `apply_drop_index_roundtrip` | Create → drop → not in B-tree | Cleanup completeness |
| 16 | `apply_drop_index_idempotent` | Drop non-existent → no error | WAL replay safety |
| 17 | `apply_index_ready_transition` | Building → Ready in B-tree | State persistence |
| 18 | `load_unicode_collection_name` | Collection name "datos_espanoles" → roundtrip | Unicode correctness |
| 19 | `load_compound_index_fields` | Index on ["address.city", "address.zip"] → correct FieldPaths | Nested field path persistence |
| 20 | `apply_create_updates_name_btree` | Create collection → name B-tree has name→id mapping | Dual B-tree consistency |

## WAL Recovery Handler Tests (25 tests)

These test the `catalog_recovery.rs` module. Each test creates a storage engine, writes specific WAL records, then runs the handler and verifies the recovered state.

### Basic Replay Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 1 | `empty_wal_produces_empty_state` | No records → empty catalog, ts=0 | Baseline |
| 2 | `single_insert_replayed` | TxCommit with 1 insert → doc in primary B-tree | Basic data recovery |
| 3 | `single_create_collection_replayed` | TxCommit with CreateCollection → collection in catalog | Catalog recovery |
| 4 | `create_collection_and_insert_replayed` | TxCommit with CreateCollection + Insert → both recovered | Atomic DDL+data |
| 5 | `multiple_txcommits_replayed_in_order` | 3 TxCommits → all mutations applied, recovered_ts = highest | Sequential replay |
| 6 | `replace_replayed` | TxCommit with Replace → updated doc in B-tree | Update recovery |
| 7 | `delete_replayed` | TxCommit with Delete → tombstone in B-tree | Delete recovery |
| 8 | `index_delta_replayed` | TxCommit with index deltas → entries in secondary B-tree | Index recovery |
| 9 | `index_ready_replayed` | IndexReady record → index state = Ready | State transition recovery |
| 10 | `vacuum_replayed` | Vacuum record → old entries removed | Vacuum recovery |

### Ordering and Atomicity Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 11 | `catalog_before_data_within_txcommit` | CreateCollection + Insert in same record → both succeed | Order-dependent correctness |
| 12 | `data_without_catalog_uses_existing_collection` | Insert into pre-existing collection → succeeds | Common case (most txns don't do DDL) |
| 13 | `drop_collection_removes_catalog_and_data_accessible` | DropCollection replayed → collection gone from catalog | DDL recovery |
| 14 | `create_then_drop_same_collection_in_separate_txs` | TX1: create "users", TX2: drop "users" → catalog empty | Multi-tx DDL sequence |
| 15 | `create_index_then_insert_with_deltas` | TX1: CreateIndex, TX2: Insert + IndexDelta → both in B-tree | Index build + use sequence |

### Idempotency Tests (CRITICAL)

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 16 | `replay_same_insert_twice` | Apply TxCommit, apply again → same single doc in B-tree | Crash during replay |
| 17 | `replay_same_create_collection_twice` | Apply CreateCollection twice → single entry in catalog | Crash during replay |
| 18 | `replay_same_delete_twice` | Apply Delete twice → tombstone present, no error | Crash during replay |
| 19 | `replay_same_index_delta_twice` | Apply IndexDelta twice → single entry in secondary B-tree | Crash during replay |
| 20 | `replay_same_index_ready_twice` | Apply IndexReady twice → state = Ready, no error | Crash during replay |

### Timestamp and Visibility Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 21 | `recovered_ts_tracks_highest_commit_ts` | 3 commits at ts=10,20,30 → recovered_ts=30 | Correct timestamp recovery |
| 22 | `visible_ts_tracks_latest_visible_ts_record` | VisibleTs records at ts=10,20 → visible_ts=20 | Replication state recovery |
| 23 | `recovered_ts_gt_visible_ts_detected` | Commits at ts=30, visible_ts=20 → gap detected for rollback | Rollback vacuum trigger |

### Error Handling Tests

| # | Test | Validates | Why It Matters |
|---|------|-----------|----------------|
| 24 | `corrupt_txcommit_payload_returns_error` | Invalid payload bytes → handler returns error, recovery stops | Data corruption detection |
| 25 | `unknown_record_type_skipped` | Record type 0xFF → warning logged, replay continues | Forward compatibility |

## Durability Integration Tests (50 tests)

**These are the most important tests in the entire database.** Each test creates a file-backed database, performs operations, simulates a crash (by dropping the `Database` without calling `close()`), and reopens to verify durability.

### Test Infrastructure

```rust
/// Helper: create a file-backed database in a tempdir.
async fn open_test_db(dir: &Path) -> Database {
    Database::open(dir, DatabaseConfig::default(), None).await.unwrap()
}

/// Helper: simulate a crash by dropping the database without close().
/// This skips the final checkpoint, leaving uncommitted buffer pool state.
fn simulate_crash(db: Database) {
    // Don't call db.close() — just drop it.
    // The Drop impl should NOT do a final checkpoint (that would defeat the purpose).
    std::mem::drop(db);
}

/// Helper: reopen the database from the same directory.
async fn reopen_test_db(dir: &Path) -> Database {
    Database::open(dir, DatabaseConfig::default(), None).await.unwrap()
}
```

### Basic Durability Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 1 | `committed_data_survives_clean_close` | Insert, commit, close, reopen → data present | Baseline durability |
| 2 | `committed_data_survives_crash` | Insert, commit, crash, reopen → data present | WAL recovery |
| 3 | `uncommitted_data_lost_on_crash` | Insert (no commit), crash, reopen → data absent | Atomicity |
| 4 | `committed_collection_survives_crash` | Create collection, commit, crash, reopen → collection present | Catalog recovery |
| 5 | `uncommitted_collection_lost_on_crash` | Create collection (no commit), crash, reopen → collection absent | Catalog atomicity |
| 6 | `committed_index_survives_crash` | Create index, commit, crash, reopen → index present and queryable | Index catalog recovery |
| 7 | `replace_survives_crash` | Insert, commit, replace, commit, crash, reopen → replaced value | Update durability |
| 8 | `delete_survives_crash` | Insert, commit, delete, commit, crash, reopen → doc gone | Delete durability |
| 9 | `patch_survives_crash` | Insert, commit, patch, commit, crash, reopen → patched value | Patch durability |
| 10 | `multiple_commits_before_crash` | 10 sequential commits, crash, reopen → all 10 present | Multi-commit recovery |

### DDL + Data Atomicity Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 11 | `ddl_and_data_in_one_tx_survives_crash` | Create collection + insert in one tx, commit, crash, reopen → both present | Atomic DDL+data in WAL |
| 12 | `ddl_and_data_in_one_tx_rolled_back_on_crash` | Create collection + insert in one tx, NO commit, crash, reopen → neither present | Atomic rollback |
| 13 | `drop_collection_survives_crash` | Create, commit, drop, commit, crash, reopen → collection gone | Drop durability |
| 14 | `drop_index_survives_crash` | Create index, commit, drop index, commit, crash, reopen → index gone | Index drop durability |
| 15 | `create_and_drop_same_name_survives_crash` | Create "users", commit, drop "users", commit, create "users", commit, crash → "users" exists with new ID | Name reuse + crash |
| 16 | `cascade_drop_collection_indexes_survive_crash` | Create collection + 3 indexes, commit, drop collection, commit, crash → all gone | Cascade durability |

### Checkpoint Interaction Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 17 | `data_before_checkpoint_survives_crash` | Insert, commit, checkpoint, crash → data present | Post-checkpoint state |
| 18 | `data_after_checkpoint_survives_crash` | Checkpoint, insert, commit, crash → data present | WAL replay after checkpoint |
| 19 | `data_before_and_after_checkpoint` | Insert A, commit, checkpoint, insert B, commit, crash → A and B present | Mixed checkpoint/WAL |
| 20 | `multiple_checkpoints` | Insert, checkpoint, insert, checkpoint, insert, crash → all present | Multiple checkpoint cycles |
| 21 | `checkpoint_then_ddl_then_crash` | Checkpoint, create collection, commit, crash → collection present | DDL after checkpoint |
| 22 | `empty_checkpoint_harmless` | Checkpoint with no dirty pages, crash → clean recovery | No-op checkpoint |

### Catalog B-Tree Consistency Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 23 | `catalog_consistent_after_many_creates` | Create 50 collections, commit, crash, reopen → all 50 in catalog | Bulk catalog recovery |
| 24 | `catalog_consistent_after_interleaved_ddl` | Create A, create B, drop A, create C, commit, crash → B and C present | Interleaved DDL recovery |
| 25 | `catalog_id_allocator_correct_after_crash` | Create collections (IDs 1-5), crash, reopen, create another → ID >= 6 | ID monotonicity across crashes |
| 26 | `catalog_name_btree_consistent_after_crash` | Create "users", commit, crash, reopen → lookup by name works | Name B-tree recovery |
| 27 | `created_at_index_present_after_crash` | Create collection, commit, crash, reopen → _created_at index exists | Auto-index durability |
| 28 | `index_state_preserved_after_crash` | Create index (Building), mark Ready, commit, crash → Ready after reopen | State transition durability |

### Secondary Index Durability Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 29 | `secondary_index_entries_survive_crash` | Create index, insert doc (with indexed field), commit, crash → index query returns doc | Index delta recovery |
| 30 | `secondary_index_delete_survives_crash` | Insert, commit, delete, commit, crash → index query returns empty | Index delta removal recovery |
| 31 | `secondary_index_replace_survives_crash` | Insert age=20, commit, replace age=30, commit, crash → query age=30 returns doc | Index delta update recovery |
| 32 | `compound_index_survives_crash` | Create index on [city, zip], insert doc, commit, crash → compound query works | Multi-field index recovery |
| 33 | `array_index_entries_survive_crash` | Insert doc with tags=["a","b","c"], commit, crash → each tag queryable | Multi-entry expansion recovery |

### Building Index Crash Recovery Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 34 | `building_index_dropped_on_crash` | Create index on populated collection (Building state), crash before Ready → index gone | Crash during build |
| 35 | `building_index_data_intact_after_crash` | Same as above → collection data intact, only index gone | Build crash doesn't corrupt data |
| 36 | `ready_index_survives_crash_during_other_build` | Index A is Ready, index B is Building, crash → A survives, B dropped | Selective cleanup |

### Rollback Vacuum Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 37 | `no_rollback_when_visible_ts_equals_recovered_ts` | Normal operation, crash → no rollback needed | Common case |
| 38 | `rollback_vacuum_removes_unreplicated_insert` | Simulate: TxCommit at ts=10, no VisibleTs, crash, reopen → insert rolled back | Basic rollback |
| 39 | `rollback_vacuum_removes_unreplicated_replace` | Same with replace → original value restored | Replace rollback |
| 40 | `rollback_vacuum_removes_unreplicated_delete` | Same with delete → document reappears | Delete rollback |
| 41 | `rollback_vacuum_removes_unreplicated_collection` | CreateCollection committed but not visible → collection gone after reopen | DDL rollback |
| 42 | `rollback_vacuum_preserves_replicated_commits` | Commits at ts=5 (visible), ts=10 (not visible), crash → ts=5 data present, ts=10 data gone | Selective rollback |
| 43 | `rollback_vacuum_handles_multiple_unreplicated_txs` | 3 commits after last VisibleTs, crash → all 3 rolled back | Multi-tx rollback |
| 44 | `rollback_vacuum_writes_wal_record` | Rollback occurs, second crash, reopen → rollback not repeated | Rollback idempotency |

### Repeated Crash Tests

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 45 | `double_crash_recovery` | Insert, commit, crash, reopen (recovery runs), crash again, reopen → data present | Recovery is re-entrant |
| 46 | `triple_crash_recovery` | Same with 3 crashes | Extra paranoia |
| 47 | `crash_after_checkpoint_during_recovery` | Insert, commit, crash, reopen (recovery starts, checkpoint happens), crash again → data present | Checkpoint during recovery |

### Edge Cases

| # | Test | Scenario | Validates |
|---|------|----------|-----------|
| 48 | `empty_database_crash_recovery` | Open, crash immediately, reopen → clean empty state | Edge: nothing to recover |
| 49 | `very_large_transaction_survives_crash` | Insert 10,000 docs in one tx, commit, crash → all 10,000 present | Large WAL record |
| 50 | `interleaved_commits_and_checkpoints` | 100 commits interleaved with 5 checkpoints, crash at random point → all committed data present | Complex recovery sequence |

## Concurrency Tests (30 tests)

| # | Test | Validates |
|---|------|-----------|
| 1 | `two_readers_no_conflict` | Concurrent read-only txns never conflict |
| 2 | `reader_and_writer_no_conflict_disjoint_keys` | Reader and writer on different keys → both succeed |
| 3 | `reader_and_writer_conflict_same_range` | Reader reads range, writer inserts into range → reader's next commit would show stale data (but readonly commit is no-op) |
| 4 | `two_writers_same_key_one_conflicts` | Both write same doc → one gets Conflict |
| 5 | `two_writers_different_keys_no_conflict` | Writers on disjoint keys → both succeed |
| 6 | `phantom_insert_detected` | TX1 scans range [A,M] returning 5 rows, TX2 inserts "B" and commits → TX1 commit conflicts |
| 7 | `phantom_insert_outside_range_no_conflict` | TX1 scans [A,M], TX2 inserts "Z" → TX1 commit succeeds |
| 8 | `phantom_insert_with_limit_boundary` | TX1 scans [A,Z] limit=3 → last row "C", TX2 inserts "B" → TX1 conflicts (B < C = limit boundary) |
| 9 | `phantom_insert_beyond_limit_boundary` | TX1 scans [A,Z] limit=3 → last row "C", TX2 inserts "D" → TX1 succeeds (D > C) |
| 10 | `concurrent_ddl_same_name` | TX1 and TX2 both create "users" → one conflicts |
| 11 | `concurrent_ddl_different_names` | TX1 creates "users", TX2 creates "orders" → both succeed |
| 12 | `ddl_conflicts_with_data_read` | TX1 reads from "users", TX2 drops "users" → TX1 conflicts (catalog read interval) |
| 13 | `ten_concurrent_writers_disjoint` | 10 writers to different collections → all succeed |
| 14 | `ten_concurrent_writers_same_collection` | 10 writers inserting different docs → most succeed (no conflicts on different docs) |
| 15 | `writer_during_checkpoint` | Writer commits while checkpoint is running → both succeed |
| 16 | `multiple_commits_during_checkpoint` | 5 concurrent commits during checkpoint → all succeed, checkpoint covers pre-commit state |
| 17 | `subscription_fires_during_concurrent_writes` | Watch subscription active, concurrent writer commits → event received |
| 18 | `subscription_not_lost_during_concurrent_ddl` | Watch on "users", concurrent DDL on "orders" → no spurious event |
| 19 | `read_your_writes_during_concurrent_commit` | TX1 inserts, TX2 commits unrelated change → TX1 still sees its own writes |
| 20 | `100_concurrent_read_transactions` | 100 read-only txns all reading same data → all get consistent snapshots |
| 21 | `writer_blocked_by_catalog_read` | TX1 lists collections, TX2 creates collection → TX1's catalog read detects conflict |
| 22 | `list_indexes_conflict_with_create_index` | TX1 lists indexes, TX2 creates index → TX1 conflicts |
| 23 | `concurrent_vacuum_and_read` | Read tx active during vacuum → read sees consistent snapshot (vacuum only removes versions older than oldest active tx) |
| 24 | `concurrent_index_build_and_query` | Index Building, query on that index → IndexNotReady |
| 25 | `rapid_begin_commit_cycle` | 1000 begin/commit cycles in tight loop → no resource leak |
| 26 | `concurrent_close_and_begin` | Close called while begin in flight → one gets ShuttingDown error |
| 27 | `active_tx_count_converges_to_zero` | Spawn 50 txns, commit all, check count = 0 | Leak detection |
| 28 | `occ_retry_loop_converges` | Two conflicting writers in retry loop → both eventually succeed | Practical conflict resolution |
| 29 | `subscribe_mode_chain_under_concurrent_writes` | Subscribe tx, concurrent writes trigger chain → chain tx sees latest data | Subscribe semantics |
| 30 | `concurrent_create_and_use_index` | TX1 creates index + commits, TX2 immediately queries it → works or IndexNotReady (no crash) | Race between build and use |

## Stress Tests (20 tests)

| # | Test | Validates |
|---|------|-----------|
| 1 | `10000_documents_insert_and_query` | Bulk insert + full scan correctness |
| 2 | `1000_transactions_sequential` | Sequential commit throughput, no resource leak |
| 3 | `100_collections_create_and_query` | Catalog scalability |
| 4 | `50_indexes_on_one_collection` | Index count scalability |
| 5 | `deeply_nested_document` | 10 levels deep JSON → insert/get roundtrip |
| 6 | `max_size_document` | 16MB document → insert, get, replace, delete |
| 7 | `max_read_intervals_reached` | Query until max_intervals → ReadLimitExceeded |
| 8 | `max_scanned_docs_reached` | Scan until max_scanned_docs → ReadLimitExceeded |
| 9 | `transaction_idle_timeout` | Begin tx, sleep past idle_timeout, try operation → TransactionTimeout |
| 10 | `transaction_max_lifetime` | Begin tx, perform operations past max_lifetime → TransactionTimeout |
| 11 | `1000_docs_durability` | Insert 1000, checkpoint, crash, reopen → all 1000 present |
| 12 | `alternating_insert_delete_durability` | Insert 500, delete odd IDs, commit, crash → 250 present |
| 13 | `many_small_transactions_durability` | 1000 single-insert txns, crash, reopen → all 1000 present |
| 14 | `large_batch_in_one_tx_durability` | 5000 inserts in one tx, commit, crash → all 5000 present |
| 15 | `mixed_ddl_and_data_stress` | 100 txns: some create collections, some insert, some drop → consistent state after crash |
| 16 | `unicode_field_names_and_values` | Insert docs with Unicode keys/values → roundtrip correctly |
| 17 | `empty_document_insert` | Insert `{}` → get returns `{"_id": ..., "_created_at": ...}` |
| 18 | `null_field_values` | Insert `{"x": null}` → get returns `{"x": null, ...}` |
| 19 | `concurrent_stress_10_threads` | 10 threads, each doing 100 insert-commit cycles → no panics, no corruption |
| 20 | `checkpoint_under_sustained_write_load` | Continuous writes + periodic checkpoints → data consistent after crash |

---

## Test Implementation Notes

### Test Helper Utilities

```rust
/// Module: tests/helpers.rs

/// Create a tempdir-backed database with default config.
pub async fn test_db() -> (TempDir, Database) { ... }

/// Create an in-memory database with default config.
pub async fn test_db_in_memory() -> Database { ... }

/// Insert a simple test document into a collection.
pub async fn insert_test_doc(tx: &mut Transaction<'_>, collection: &str) -> DocId { ... }

/// Create a collection with a secondary index.
pub async fn setup_indexed_collection(db: &Database, name: &str, index_field: &str) { ... }

/// Simulate crash: drop database without close().
pub fn crash(db: Database) { std::mem::drop(db); }

/// Reopen database from same path.
pub async fn reopen(path: &Path) -> Database { ... }
```

### Durability Test Pattern

Every durability test follows the same 5-step pattern:

```rust
#[tokio::test]
async fn committed_data_survives_crash() {
    // 1. Setup: create temp directory
    let dir = tempdir().unwrap();

    // 2. Act: open db, perform operations, commit
    {
        let db = open_test_db(dir.path()).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        let id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        tx.commit().await.unwrap();
        // 3. Crash: drop without close
        // (db goes out of scope without close())
    }

    // 4. Recover: reopen
    let db = open_test_db(dir.path()).await;

    // 5. Verify: data present
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &id).await.unwrap();
    assert!(doc.is_some());
    assert_eq!(doc.unwrap()["name"], "Alice");

    db.close().await.unwrap();
}
```

### Crash Injection for Rollback Vacuum Tests

Testing rollback vacuum requires simulating a crash between the TxCommit WAL write and the VisibleTs WAL write. Two approaches:

1. **Manual WAL construction**: Write WAL records directly using `WalWriter`, without going through the commit coordinator. This gives precise control over which records exist.

2. **Custom ReplicationHook**: Implement a hook that delays or blocks the `visible_ts` advance. Then crash the database. On reopen, `recovered_ts > visible_ts`.

Approach 2 is preferred — it tests the actual commit path rather than constructing synthetic WAL state.

```rust
/// Replication hook that never advances visible_ts.
struct NeverVisibleHook;

impl ReplicationHook for NeverVisibleHook {
    async fn replicate(&self, _payload: &[u8]) -> Result<(), ReplicationError> {
        // Accept the payload but never advance visible_ts
        Ok(())
    }
}
```

### Test Organization

```
crates/database/
  tests/
    helpers.rs          — shared test utilities
    config.rs           — B1 tests
    catalog_cache.rs    — B2 tests
    index_resolver.rs   — B3 tests
    catalog_tracker.rs  — B4 tests
    transaction.rs      — B5 tests (in-memory, no crash)
    integration.rs      — B6 integration tests (in-memory)
    subscription.rs     — B7 tests
    system_database.rs  — B8 tests
    catalog_persistence.rs — Catalog persistence module tests
    recovery.rs         — WAL recovery handler tests
    durability.rs       — File-backed crash recovery tests (THE MOST IMPORTANT FILE)
    concurrency.rs      — Multi-transaction conflict tests
    stress.rs           — Load and limit tests
```

## Testing Checklist — Non-Negotiable Requirements

Before L6 is considered complete, ALL of the following must pass:

- [ ] Every `apply_*` method in CatalogPersistence has an idempotency test
- [ ] Every WAL record type has at least one replay test
- [ ] TxCommit replay tests verify catalog-before-data ordering
- [ ] Every CRUD operation (insert, replace, patch, delete) has a crash-recovery test
- [ ] Every DDL operation (create/drop collection/index) has a crash-recovery test
- [ ] At least one test verifies data survives checkpoint + crash
- [ ] At least one test verifies data survives crash without checkpoint
- [ ] At least one test verifies uncommitted data is NOT recovered
- [ ] Building index crash recovery is tested
- [ ] Rollback vacuum basic path is tested
- [ ] Double-crash (crash during recovery) is tested
- [ ] Phantom insert detection via OCC is tested (with and without LimitBoundary)
- [ ] Subscription invalidation fires correctly under concurrent writes
- [ ] Active transaction count reaches zero after all txns complete
- [ ] Resource limits (max_intervals, max_scanned_docs) are enforced
- [ ] Transaction timeout is enforced
- [ ] Graceful shutdown waits for active transactions

**A test that is not written is a bug waiting to happen.** A test that is written but not run in CI is decoration. Every test in this plan must be automated and run on every commit.
