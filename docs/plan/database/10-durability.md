# L6 Durability Design: WAL Recovery, Catalog Persistence, and Crash Safety

## Motivation

L6 is the first layer that must reason about **persistent state across restarts**. Layers 1-5 are stateless or runtime-only: L2 provides storage primitives, L3/L4/L5 operate on in-memory structures. L6 is where durability guarantees are assembled — it owns the startup sequence that rebuilds runtime state from durable storage, and the commit path that ensures mutations survive crashes.

**This is the hardest part of the database to get right.** A bug in query execution returns wrong results for one request. A bug in durability silently loses committed data. Every design decision here must be justified against specific crash scenarios, and every code path must be tested with simulated failures.

## Architecture: The Durability Stack

```
                    L6 Database (this document)
                    ┌─────────────────────────────┐
                    │ WalRecordHandler impl        │ ← Interprets WAL records during recovery
                    │ CatalogPersistence           │ ← Reads/writes catalog B-trees
                    │ Startup / Recovery sequence   │ ← Orchestrates DWB + WAL replay + catalog rebuild
                    │ Commit-time catalog side fx   │ ← B-tree allocation, catalog updates at commit
                    │ Rollback vacuum               │ ← Reverts un-replicated commits after crash
                    └──────────────┬──────────────┘
                                   │ uses
                    L2 Storage Engine
                    ┌──────────────┴──────────────┐
                    │ S5  WAL (append, read, replay)│
                    │ S8  DWB (torn write protection)│
                    │ S9  Checkpoint (flush to disk) │
                    │ S10 Recovery (DWB + WAL replay) │
                    │ S12 Catalog B-tree (schema)    │
                    │ S13 StorageEngine (orchestrator)│
                    └─────────────────────────────┘
```

**Key principle:** L2 provides *mechanisms* (WAL append, B-tree operations, DWB protection, recovery framework). L6 provides *policy* (what WAL records mean, when to checkpoint, how to rebuild catalog state, what to do with un-replicated commits).

## Module: Catalog Persistence (`catalog_persistence.rs`)

### Purpose

Bridges the in-memory `CatalogCache` (B2) with the durable catalog B-trees (S12). Handles:
1. **Startup**: Scanning catalog B-trees to populate `CatalogCache`
2. **Commit-time**: Writing catalog mutations to the catalog B-trees
3. **Recovery**: Applying catalog mutations from WAL replay

### Why a Separate Module

The B6 (`database.rs`) startup sequence says "Scan catalog B-tree → build CatalogCache" — but this is non-trivial logic involving deserialization of `CollectionEntry`/`IndexEntry` from S12 format, validation, ID allocator initialization, and B-tree handle creation. Mixing this into `database.rs` would make it unmanageably large. Similarly, commit-time catalog mutations involve multiple B-tree writes and cache updates that deserve encapsulation.

### Dependencies

- **S12 (`catalog_btree`)**: `CollectionEntry`, `IndexEntry`, `make_catalog_id_key`, `make_catalog_name_key`, `serialize_*`, `deserialize_*`
- **S6 (`BTreeHandle`)**: `get`, `insert`, `delete`, `scan`
- **B2 (`CatalogCache`)**: `add_collection`, `add_index`, `remove_collection`, `remove_index`, `set_index_state`
- **L3 (`PrimaryIndex`, `SecondaryIndex`)**: Handle creation for recovered indexes

### API

```rust
use exdb_storage::btree::BTreeHandle;
use exdb_storage::engine::StorageEngine;
use exdb_core::types::{CollectionId, IndexId};
use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

/// Catalog persistence — bridges CatalogCache with catalog B-trees.
pub struct CatalogPersistence;

impl CatalogPersistence {
    /// Load the entire catalog from the dual B-trees into a fresh CatalogCache.
    ///
    /// Called once during Database::open() after StorageEngine recovery.
    ///
    /// Steps:
    /// 1. Scan the by-ID B-tree with collection prefix (0x01)
    /// 2. Deserialize each CollectionEntry → CollectionMeta
    /// 3. Scan the by-ID B-tree with index prefix (0x02)
    /// 4. Deserialize each IndexEntry → IndexMeta
    /// 5. Populate CatalogCache with all entries
    /// 6. Set next_collection_id = max(collection_ids) + 1
    /// 7. Set next_index_id = max(index_ids) + 1
    /// 8. Validate invariants:
    ///    a. Every collection has a _created_at index
    ///    b. Every index references an existing collection
    ///    c. No duplicate names
    ///
    /// Returns the populated CatalogCache.
    pub async fn load_catalog(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
    ) -> Result<CatalogCache, DatabaseError>;

    /// Apply a CreateCollection mutation to the catalog B-trees.
    ///
    /// Called by the commit coordinator during step 4a.
    ///
    /// Steps:
    /// 1. Serialize CollectionEntry
    /// 2. Insert into by-ID B-tree (key = 0x01 || collection_id)
    /// 3. Insert into by-Name B-tree (key = 0x01 || name || 0x00, value = collection_id)
    /// 4. Update CatalogCache
    pub async fn apply_create_collection(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        collection_id: CollectionId,
        name: &str,
        primary_root_page: u32,
    ) -> Result<(), DatabaseError>;

    /// Apply a DropCollection mutation to the catalog B-trees.
    ///
    /// Steps:
    /// 1. Look up collection in cache to get name and all index IDs
    /// 2. Remove from by-ID B-tree
    /// 3. Remove from by-Name B-tree
    /// 4. For each index: remove from both B-trees
    /// 5. Update CatalogCache (cascades index removal)
    pub async fn apply_drop_collection(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        collection_id: CollectionId,
    ) -> Result<(), DatabaseError>;

    /// Apply a CreateIndex mutation to the catalog B-trees.
    pub async fn apply_create_index(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        meta: &IndexMeta,
    ) -> Result<(), DatabaseError>;

    /// Apply a DropIndex mutation to the catalog B-trees.
    pub async fn apply_drop_index(
        id_btree: &BTreeHandle,
        name_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        index_id: IndexId,
    ) -> Result<(), DatabaseError>;

    /// Apply an IndexReady state transition.
    pub async fn apply_index_ready(
        id_btree: &BTreeHandle,
        cache: &mut CatalogCache,
        index_id: IndexId,
    ) -> Result<(), DatabaseError>;
}
```

### Idempotency Requirement

All `apply_*` methods MUST be idempotent. During WAL replay, the same mutation may be applied multiple times (the page may have been flushed to disk before the crash, so the B-tree already contains the entry). The behavior:
- `apply_create_collection`: If collection_id already exists in B-tree, overwrite (same data).
- `apply_drop_collection`: If collection_id not in B-tree, no-op.
- `apply_create_index`: If index_id already exists, overwrite.
- `apply_drop_index`: If index_id not in B-tree, no-op.

This is critical for correctness. The B-tree `insert` operation is naturally idempotent (upsert semantics), and `delete` on a missing key is a no-op.

## Module: WAL Record Handler (`catalog_recovery.rs`)

### Purpose

Implements the `WalRecordHandler` trait (defined in S10) for L6. This is the callback that the storage engine invokes for each WAL record during crash recovery. It reconstructs the database's runtime state from the WAL.

### Why This Is Critical

The WAL is the **single source of truth** after a crash. Everything that was committed but not yet checkpointed exists only in WAL records. If the handler misinterprets or skips a record, committed data is silently lost. This module has **zero tolerance for bugs**.

### Dependencies

- **S10 (`Recovery`)**: `WalRecordHandler` trait
- **S5 (`WAL`)**: `WalRecord`, record type constants
- **L5 (`exdb-tx`)**: `CommitPayload` (deserialized from TxCommit WAL record)
- **Catalog Persistence**: `apply_create_collection`, `apply_create_index`, etc.
- **L3 (`exdb-docstore`)**: `PrimaryIndex`, `SecondaryIndex` for data mutation replay

### WAL Record Types Handled

| Record Type | Constant | Handler Action |
|-------------|----------|----------------|
| `TxCommit` | `0x01` | Deserialize payload → apply catalog mutations → apply data mutations → apply index deltas |
| `IndexReady` | `0x07` | Transition index Building → Ready in catalog |
| `Vacuum` | `0x08` | Remove old version entries (idempotent) |
| `VisibleTs` | `0x09` | Update visible_ts tracker |
| `RollbackVacuum` | `0x0A` | Remove entries from rolled-back commits |
| `Checkpoint` | `0x02` | Skip (informational, handled by S10) |

### TxCommit Replay — The Critical Path

A `TxCommit` WAL record contains (serialized by L5 T7):
1. `commit_ts: Ts`
2. `catalog_mutations: Vec<CatalogMutation>` — DDL operations
3. `data_mutations: Vec<(CollectionId, DocId, MutationOp, body)>` — document operations
4. `index_deltas: Vec<(IndexId, IndexKeyWrite)>` — secondary index entries

Replay order within a single TxCommit record is critical:

```
Step 1: Catalog mutations FIRST
        - CreateCollection → allocate B-tree pages, insert into catalog B-trees,
          update CatalogCache, create PrimaryIndex/SecondaryIndex handles
        - CreateIndex → allocate B-tree page, insert into catalog B-trees,
          update CatalogCache, create SecondaryIndex handle
        - DropCollection / DropIndex → remove from catalog B-trees and cache

Step 2: Data mutations SECOND (collections must exist from step 1)
        - For each (collection_id, doc_id, op, body):
          Insert/Replace/Delete into the collection's PrimaryIndex

Step 3: Index deltas THIRD (indexes must exist from step 1)
        - For each (index_id, key_write):
          Insert/Delete into the index's SecondaryIndex B-tree
```

**Why this order matters:** If a transaction creates a collection and inserts into it, the WAL record contains both the `CreateCollection` catalog mutation and the `Insert` data mutation. The data mutation references a `CollectionId` that only exists after the catalog mutation is applied. Reversing the order would cause a "collection not found" panic during replay.

### API

```rust
use exdb_storage::wal::WalRecord;
use exdb_storage::recovery::WalRecordHandler;

/// L6 WAL record handler for crash recovery.
///
/// Reconstructs catalog state, document data, and secondary indexes
/// from WAL records replayed after the last checkpoint.
///
/// # Invariants
///
/// - All operations are idempotent (safe to replay multiple times)
/// - Catalog mutations are applied before data mutations within each TxCommit
/// - The handler tracks the highest committed_ts and visible_ts seen
pub struct DatabaseRecoveryHandler {
    /// Storage engine for B-tree operations
    storage: Arc<StorageEngine>,
    /// Catalog B-trees (by-ID and by-Name)
    catalog_id_btree: BTreeHandle,
    catalog_name_btree: BTreeHandle,
    /// In-memory catalog being rebuilt
    catalog: CatalogCache,
    /// Primary index handles (built up during replay)
    primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
    /// Secondary index handles (built up during replay)
    secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
    /// Highest committed_ts seen in TxCommit records
    recovered_ts: Ts,
    /// Latest visible_ts seen in VisibleTs records
    visible_ts: Ts,
}

impl DatabaseRecoveryHandler {
    pub fn new(
        storage: Arc<StorageEngine>,
        catalog_id_btree: BTreeHandle,
        catalog_name_btree: BTreeHandle,
        catalog: CatalogCache,
        primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Self;

    /// Get the recovered state after replay completes.
    pub fn into_recovered_state(self) -> RecoveredState;
}

/// State recovered from WAL replay.
pub struct RecoveredState {
    pub catalog: CatalogCache,
    pub primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
    pub secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
    pub recovered_ts: Ts,
    pub visible_ts: Ts,
}

impl WalRecordHandler for DatabaseRecoveryHandler {
    fn handle_record(&mut self, record: &WalRecord) -> Result<()> {
        match record.record_type {
            WAL_RECORD_TX_COMMIT => self.replay_tx_commit(record),
            WAL_RECORD_INDEX_READY => self.replay_index_ready(record),
            WAL_RECORD_VACUUM => self.replay_vacuum(record),
            WAL_RECORD_VISIBLE_TS => self.replay_visible_ts(record),
            WAL_RECORD_ROLLBACK_VACUUM => self.replay_rollback_vacuum(record),
            WAL_RECORD_CHECKPOINT => Ok(()), // skip
            unknown => {
                tracing::warn!("unknown WAL record type 0x{:02x}, skipping", unknown);
                Ok(())
            }
        }
    }
}
```

### Internal Replay Methods

```rust
impl DatabaseRecoveryHandler {
    /// Replay a TxCommit record.
    ///
    /// CRITICAL: catalog mutations first, then data, then index deltas.
    fn replay_tx_commit(&mut self, record: &WalRecord) -> Result<()> {
        let payload = CommitPayload::deserialize(&record.payload)?;

        // Track highest committed_ts
        self.recovered_ts = self.recovered_ts.max(payload.commit_ts);

        // Step 1: Catalog mutations
        for mutation in &payload.catalog_mutations {
            self.apply_catalog_mutation(mutation).await?;
        }

        // Step 2: Data mutations
        for (collection_id, doc_id, op, body) in &payload.data_mutations {
            self.apply_data_mutation(*collection_id, doc_id, op, body).await?;
        }

        // Step 3: Index deltas
        for (index_id, key_write) in &payload.index_deltas {
            self.apply_index_delta(*index_id, key_write).await?;
        }

        Ok(())
    }

    /// Replay a VisibleTs record.
    fn replay_visible_ts(&mut self, record: &WalRecord) -> Result<()> {
        let ts = Ts::from_le_bytes(record.payload[..8].try_into()?);
        self.visible_ts = self.visible_ts.max(ts);
        Ok(())
    }

    /// Apply a single catalog mutation during replay.
    ///
    /// For CreateCollection: allocates a new B-tree (idempotent — if root page
    /// already exists, open it instead), inserts into catalog B-trees,
    /// creates PrimaryIndex handle.
    async fn apply_catalog_mutation(&mut self, mutation: &CatalogMutation) -> Result<()>;

    /// Apply a data mutation (insert/replace/delete) to a primary index.
    async fn apply_data_mutation(
        &mut self,
        collection_id: CollectionId,
        doc_id: &DocId,
        op: &MutationOp,
        body: &Option<Vec<u8>>,
    ) -> Result<()>;

    /// Apply an index delta (insert/delete key) to a secondary index.
    async fn apply_index_delta(
        &mut self,
        index_id: IndexId,
        key_write: &IndexKeyWrite,
    ) -> Result<()>;
}
```

## Rollback Vacuum

### Purpose

After a crash, `recovered_ts` (highest committed timestamp in WAL) may be greater than `visible_ts` (highest timestamp confirmed as replicated/visible). Commits in the range `(visible_ts, recovered_ts]` were committed locally but never confirmed as visible — they must be rolled back.

On single-node (NoReplication), `visible_ts` advances to `committed_ts` immediately in the commit path. A crash can only leave `recovered_ts > visible_ts` if the crash happened between writing the TxCommit WAL record and writing the VisibleTs WAL record. This window is tiny but real.

On replicated setups, the gap can be larger — commits that were never acknowledged by replicas.

### Algorithm

```
rollback_vacuum(recovered_ts, visible_ts, catalog, primary_indexes, secondary_indexes):
    if recovered_ts <= visible_ts:
        return  // nothing to roll back

    // Scan the commit log (WAL) for TxCommit records with ts in (visible_ts, recovered_ts]
    for record in wal.scan_backwards_from(recovered_ts):
        if record.commit_ts <= visible_ts:
            break

        payload = deserialize(record)

        // Reverse index deltas (remove secondary index entries)
        for (index_id, key_write) in payload.index_deltas.reversed():
            secondary_indexes[index_id].remove(key_write.key)

        // Reverse data mutations
        for (coll_id, doc_id, op, body) in payload.data_mutations.reversed():
            match op:
                Insert => primary_indexes[coll_id].delete(doc_id, record.commit_ts)
                Replace => primary_indexes[coll_id].delete(doc_id, record.commit_ts)
                    // Note: the previous version is still in the B-tree (MVCC)
                Delete => primary_indexes[coll_id].delete_tombstone(doc_id, record.commit_ts)

        // Reverse catalog mutations (in reverse order!)
        for mutation in payload.catalog_mutations.reversed():
            match mutation:
                CreateCollection(id) =>
                    catalog_persistence.apply_drop_collection(id)
                    // Also drop the B-tree pages (or just leave them — they'll be
                    // reclaimed by vacuum or are harmless orphans)
                DropCollection(id) =>
                    // Cannot easily un-drop. Log a warning. The collection data
                    // is still in the B-tree (MVCC tombstones). A more sophisticated
                    // approach would re-create the catalog entry.
                    // For now: panic on un-drop — this is a rare edge case that
                    // requires manual intervention.
                CreateIndex(id) =>
                    catalog_persistence.apply_drop_index(id)
                DropIndex(id) =>
                    // Same issue as DropCollection — cannot easily un-drop.
                    // Log warning.

    // Write a RollbackVacuum WAL record (so this doesn't repeat on next restart)
    wal.append(WAL_RECORD_ROLLBACK_VACUUM, &rollback_payload)

    // Update visible_ts to match (gap is now closed)
```

### Design Limitation: Un-dropping

Reversing a `DropCollection` or `DropIndex` that was committed but not replicated is complex — the catalog entry was removed, and recreating it requires knowing the original metadata. Two approaches:

1. **Store full metadata in the WAL record**: The TxCommit payload for a DropCollection should include the full `CollectionEntry` and all `IndexEntry` data, so rollback can reconstruct them. **This is the recommended approach.**
2. **Panic and require manual recovery**: Acceptable for v1 since un-replicated drops are extremely rare.

### Why Rollback Vacuum is Hard to Test

The rollback vacuum window requires:
1. A crash at a precise point in the commit path (after TxCommit WAL write, before VisibleTs WAL write)
2. Re-opening the database
3. Verifying that the un-replicated commit is not visible

This requires **deterministic crash injection** — see the testing section below.

## Crash Scenario Analysis

Every crash scenario must be analyzed for data safety. The scenarios are organized by *where in the commit/checkpoint path the crash occurs*.

### Scenario 1: Crash During Transaction (Before Commit)

**State**: Transaction has buffered writes in memory (WriteSet). No WAL record written.
**Recovery**: Nothing to recover. The transaction's writes are lost. This is correct — uncommitted transactions should not survive crashes.
**Risk**: None.

### Scenario 2: Crash During WAL Append (TxCommit)

**State**: The WAL writer is assembling a batch of records. The batch may be partially written.
**Recovery**: WAL reader stops at the first record that fails CRC verification. The partial TxCommit record is discarded. The transaction is effectively rolled back.
**Risk**: None — the WAL's CRC provides atomic record boundaries.

### Scenario 3: Crash After WAL Append, Before Catalog B-Tree Update

**State**: The TxCommit WAL record (including catalog mutations) is durably written. But the catalog B-tree pages in the buffer pool have not been updated yet.
**Recovery**: WAL replay applies the catalog mutations to the B-tree. Idempotent — if some pages were flushed, the B-tree insert is an upsert.
**Risk**: None, if the handler is correctly idempotent.

### Scenario 4: Crash After Catalog B-Tree Update, Before Data Mutation

**State**: Catalog B-trees updated, but document data not yet written to primary B-tree.
**Recovery**: WAL replay re-applies both catalog and data mutations. Catalog mutations are idempotent (upsert). Data mutations are idempotent.
**Risk**: None.

### Scenario 5: Crash During Checkpoint (DWB Phase)

**State**: DWB file contains dirty pages, but scatter-write to `data.db` is incomplete.
**Recovery**: DWB recovery restores torn pages. Then WAL replay from checkpoint_lsn re-applies any mutations.
**Risk**: None — this is exactly what the DWB is designed for.

### Scenario 6: Crash During Checkpoint (After Scatter-Write, Before Checkpoint WAL Record)

**State**: All dirty pages are flushed to `data.db` and fsynced. But the Checkpoint WAL record wasn't written, so `checkpoint_lsn` in the file header is still the *previous* checkpoint.
**Recovery**: WAL replay starts from the *old* checkpoint_lsn and replays records that were already flushed. This is safe because replay is idempotent. Extra work, but correct.
**Risk**: None, but wastes time replaying already-applied records.

### Scenario 7: Crash After VisibleTs WAL Record

**State**: Everything committed and visible. Clean state.
**Recovery**: WAL replay re-applies everything since last checkpoint. All idempotent.
**Risk**: None.

### Scenario 8: Crash Between TxCommit and VisibleTs (Replicated Setup)

**State**: TxCommit written, but VisibleTs not yet advanced. `recovered_ts > visible_ts`.
**Recovery**: Rollback vacuum removes the un-replicated commit.
**Risk**: **HIGH** — this is the most complex recovery path. See rollback vacuum section.

### Scenario 9: Crash During Index Building

**State**: Background index builder has partially populated a secondary B-tree.
**Recovery**: On startup, indexes in `Building` state are detected and dropped. The user must recreate the index. Partial B-tree pages are orphaned but harmless (reclaimed at next vacuum or ignored).
**Risk**: Low. The user loses the in-progress index build, not data.

### Scenario 10: Crash During Vacuum

**State**: Vacuum was removing old MVCC versions. Some entries removed, others not.
**Recovery**: WAL replay of the Vacuum record re-applies the removals. Idempotent — removing an already-removed entry is a no-op.
**Risk**: None.

### Scenario 11: Repeated Crashes During Recovery

**State**: The database crashes *while replaying the WAL* (e.g., out of memory, disk full).
**Recovery**: Recovery is fully re-entrant. The next startup replays from the same checkpoint_lsn. All operations are idempotent. No state corruption.
**Risk**: None, assuming idempotency is correctly implemented.

### Scenario 12: Crash After Creating Collection, Before _created_at Index

**Impossible** — `CreateCollection` and `CreateIndex(_created_at)` are both `CatalogMutation` entries in the same TxCommit WAL record. They are atomic. Either both are in the WAL (and both replayed) or neither is.

### Scenario 13: File Header Corruption

**State**: Page 0 (file header) is corrupted — `catalog_root_page`, `checkpoint_lsn`, etc. are unreadable.
**Recovery**: S13 maintains a shadow copy of the file header (DESIGN.md 2.13.4). If the primary header fails checksum, the shadow is used.
**Risk**: Low — dual-header scheme covers single-page corruption.

## File Header Fields Managed by L6

L6 is responsible for updating these file header fields (via `StorageEngine::update_file_header`):

| Field | Updated When | Value |
|-------|-------------|-------|
| `checkpoint_lsn` | After each checkpoint | LSN returned by `Checkpoint::run()` |
| `next_collection_id` | At startup (from max of catalog scan) and at commit (if DDL) | Monotonic counter |
| `next_index_id` | Same as above | Monotonic counter |
| `catalog_root_page` | At first collection creation (B-tree root may change on split) | Root page of by-ID catalog B-tree |
| `catalog_name_root_page` | Same | Root page of by-Name catalog B-tree |

**Important**: `next_collection_id` and `next_index_id` do NOT need to be updated on every commit. They can be recovered from `max(existing_ids) + 1` during catalog scan. Persisting them is an optimization to avoid the scan, not a correctness requirement. Gaps in IDs are harmless.

## Integration with B6 (Database) Startup Sequence — Revised

The startup sequence in `06-b6-database.md` is updated to show where each durability component fits:

```
Database::open(path, config, replication):

1.  Create data directory if needed

2.  StorageEngine::open(path, StorageConfig::from(config))
    → Acquires exclusive file lock (flock) — prevents concurrent access [D8]
    → Reads file header (page 0) for checkpoint_lsn, catalog root pages
    → DWB recovery (S8): restore torn pages
    → Returns (engine, file_header)

3.  CatalogPersistence::load_catalog(id_btree, name_btree)
    → Scans catalog B-trees from file header root pages
    → Returns initial CatalogCache + primary/secondary index handles
    → Drops indexes in Building state (incomplete builds from prior crash)
    → Validates _created_at invariant for every collection

4.  Create DatabaseRecoveryHandler with initial catalog + index handles

5.  Recovery::run(storage, wal, dwb_path, checkpoint_lsn, handler)
    → WAL replay from checkpoint_lsn
    → Handler applies TxCommit (catalog + data + index), IndexReady, Vacuum, VisibleTs
    → WAL payload version checked (V1 legacy vs V2 current) [D7]
    → Root page IDs from WAL used to open/initialize B-trees [D6]
    → Returns (end_lsn, RecoveredState)

6.  Extract RecoveredState: updated catalog, index handles, recovered_ts, visible_ts

7.  Rollback vacuum: if recovered_ts > visible_ts
    → Scan WAL backwards for un-replicated commits
    → Reverse their effects using full metadata from WAL record [D4]
    → Write RollbackVacuum WAL record

8.  Update file header: next_collection_id, next_index_id from catalog

9.  Create CommitCoordinator::new(recovered_ts, visible_ts, ...)
    → (coordinator, runner, handle)

10. Spawn coordinator on LocalSet (!Send)
11. Spawn runner on tokio::spawn (Send)
12. Spawn checkpoint_task
13. Spawn vacuum_task
14. Return Database
```

**Key change from original**: Steps 3-6 are now explicit — catalog loading and WAL replay are separate, named operations with clear ownership.

## Commit-Time Catalog Side Effects — Revised

The commit coordinator (L5 T7 step 4a) applies catalog mutations. L6 provides the implementation via `CatalogPersistence`:

```
CommitCoordinator step 4a (catalog mutations):

For each CatalogMutation in the committed WriteSet:

  CreateCollection { collection_id, name }:
    1. storage.create_btree() → primary_root_page
    2. storage.create_btree() → created_at_root_page
       (Note: _created_at index B-tree is created here, alongside the primary)
    3. CatalogPersistence::apply_create_collection(id_btree, name_btree, cache, ...)
    4. CatalogPersistence::apply_create_index(id_btree, name_btree, cache, _created_at_meta)
    5. Create PrimaryIndex handle → insert into primary_indexes map
    6. Create SecondaryIndex handle → insert into secondary_indexes map

  DropCollection { collection_id }:
    1. CatalogPersistence::apply_drop_collection(id_btree, name_btree, cache, collection_id)
    2. Remove PrimaryIndex handle from primary_indexes map
    3. Remove all SecondaryIndex handles for this collection from secondary_indexes map
    4. (B-tree pages are NOT freed here — vacuum reclaims them later)

  CreateIndex { index_id, collection_id, name, fields }:
    1. storage.create_btree() → root_page
    2. CatalogPersistence::apply_create_index(id_btree, name_btree, cache, meta)
    3. Create SecondaryIndex handle → insert into secondary_indexes map
    4. If not during recovery: signal index_builder_task to start building

  DropIndex { index_id }:
    1. CatalogPersistence::apply_drop_index(id_btree, name_btree, cache, index_id)
    2. Remove SecondaryIndex handle from secondary_indexes map
```

## Design Decisions

### D1: Catalog Mutations in TxCommit (Not Separate WAL Records)

Earlier designs had separate WAL record types for DDL (CreateCollection, DropCollection, etc.). The current design embeds catalog mutations inside the TxCommit record. This ensures atomicity — a transaction that creates a collection and inserts data has a single WAL record, so either all of it is recovered or none of it.

### D2: Catalog B-Trees Updated Through Buffer Pool (Not Direct I/O)

Catalog B-tree mutations go through the buffer pool, same as data B-trees. This means they are covered by the same checkpoint/DWB/WAL recovery infrastructure. No special persistence path for catalog data.

### D3: Building Indexes Are Dropped on Crash (Not Resumed)

Resuming a partial index build after a crash would require tracking the last-processed primary key. The complexity is not justified — index builds are infrequent, and restarting from scratch is simpler and more testable.

### D4: Rollback Vacuum Stores Full Metadata for Un-drops

**Implemented in L5 code.** The `CatalogMutation::DropCollection` includes `primary_root_page` and `dropped_indexes: Vec<DroppedIndexMeta>` (each with index_id, name, field_paths, root_page). `CatalogMutation::DropIndex` includes `field_paths` and `root_page`. This data is serialized into the WAL record, enabling rollback vacuum to reconstruct dropped entries.

### D5: Recovery Handler is Stateful (Not a Closure)

The `DatabaseRecoveryHandler` accumulates state (catalog, index handles, timestamps) during replay. This is cleaner than passing mutable references through a closure chain, and enables testing the handler independently.

### D6: Root Page IDs in WAL Records (Pre-allocation Before WAL Write)

**Implemented in L5 code.** The commit coordinator now follows a 3-phase step 3:

```
Step 3a: Pre-allocate B-tree pages (via CatalogMutationHandler::allocate_*())
         Fills in root page IDs on CatalogMutation structs.
Step 3b: Serialize WAL payload (includes allocated root page IDs).
Step 3c: WAL write + fsync.
```

If crash after 3a but before 3c: allocated pages are harmless orphans.
If crash after 3c: WAL replay uses the recorded page IDs to open/initialize B-trees.

This solves the critical problem where `create_btree()` during WAL replay could return different page IDs than the original commit, causing catalog inconsistency.

### D7: WAL Payload Version Field

**Implemented in L5 code.** The TxCommit payload now starts with a `version: u8` byte (currently version 2). The deserializer auto-detects V1 (legacy, no version byte) vs V2. Future format changes only require incrementing the version and adding a new deserialization branch.

### D8: Exclusive File Locking

**Implemented in L2 code.** `FilePageStorage::open()` and `::create()` acquire an exclusive `flock(LOCK_EX | LOCK_NB)` on the data file. This prevents two processes from opening the same database simultaneously, which would cause silent data corruption. The lock is released on `StorageEngine::close()` via `PageStorage::unlock()`.

### D9: WAL Segment Reclamation Coordination

WAL segment reclamation is an L2 responsibility. L6 provides two inputs:
- `checkpoint_lsn` (from the last checkpoint) — segments before this are candidates
- `min_required_lsn` (from L7 replication) — respects replica lag

L2 decides which segments to delete. L6 does not directly manage segments.

### D10: File Header Shadow Copy (Not Yet Implemented)

DESIGN.md 2.13.4 specifies a shadow copy of the file header at the last page of `data.db`. The `PageType::FileHeaderShadow = 0x07` enum variant is defined but the implementation is not written. Page 0 is protected by the DWB (same as all other pages during checkpoint), so torn writes on the header are already handled. The shadow copy is an additional defense-in-depth measure for future implementation.

### D11: Heap Orphan Cleanup During Replay

When a Replace mutation is replayed for a document that uses external heap storage, the old HeapRef may become orphaned if the replay allocates new heap pages. This is a space leak, not a data correctness issue. Orphaned heap pages are reclaimed by the next vacuum cycle. Acceptable for v1.
