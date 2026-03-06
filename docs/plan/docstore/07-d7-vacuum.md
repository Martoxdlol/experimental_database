# D7: Vacuum

## Purpose

Two distinct vacuum mechanisms:

1. **WAL-driven vacuum**: incrementally removes old document versions that are no longer visible to any reader. Candidates arrive from the commit path (proportional to write rate, not database size).

2. **Rollback vacuum**: removes entries from commits that were never replicated (`ts > visible_ts`). The only undo mechanism in the system.

Both operate on D3 (PrimaryIndex) and D4 (SecondaryIndex), using `btree().delete()` for key removal.

## Dependencies

- **D3 (Primary Index)**: `PrimaryIndex` (remove old primary entries via `btree().delete()`)
- **D4 (Secondary Index)**: `SecondaryIndex` (remove old secondary entries via `remove_entry()`)
- **D1 (Key Encoding)**: `make_primary_key`, `inv_ts`
- **L1 (Core Types)**: `DocId`, `Ts`, `CollectionId`, `IndexId`

No direct L2 dependency beyond what D3/D4 expose.

## Rust Types

```rust
use crate::core::types::{DocId, Ts, CollectionId, IndexId};
use crate::docstore::primary_index::PrimaryIndex;
use crate::docstore::secondary_index::SecondaryIndex;
use crate::docstore::key_encoding::{make_primary_key, inv_ts};
use std::collections::HashMap;
use std::sync::Arc;

// --- WAL-Driven Vacuum ---

/// A candidate for vacuum: an old version that has been superseded.
/// Created at commit time when a Replace or Delete mutation supersedes
/// a previous version.
pub struct VacuumCandidate {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    /// Timestamp of the old version being superseded.
    pub old_ts: Ts,
    /// Timestamp of the new version that supersedes the old one.
    /// Used for eligibility: the old version is safe to remove only when
    /// no reader can be pinned between old_ts and superseding_ts.
    pub superseding_ts: Ts,
    /// Secondary index keys that belonged to the old version.
    /// Each entry is (index_id, encoded_key).
    pub old_index_keys: Vec<(IndexId, Vec<u8>)>,
}

/// Manages the pending vacuum queue and executes vacuum passes.
pub struct VacuumCoordinator {
    /// Pending candidates not yet vacuumed.
    pending: Vec<VacuumCandidate>,
}

impl VacuumCoordinator {
    pub fn new() -> Self;

    /// Push a new vacuum candidate (called from commit path).
    pub fn push_candidate(&mut self, candidate: VacuumCandidate);

    /// Drain all candidates eligible for vacuum.
    ///
    /// `vacuum_safe_ts = min(oldest_active_read_ts, visible_ts)`
    ///
    /// A candidate is eligible if `candidate.superseding_ts <= vacuum_safe_ts`.
    /// This means the superseding version is visible to all current readers,
    /// so no reader can still need the old version.
    pub fn drain_eligible(&mut self, vacuum_safe_ts: Ts) -> Vec<VacuumCandidate>;

    /// Execute vacuum: remove old entries from primary and secondary indexes.
    /// Returns the number of entries removed.
    ///
    /// The caller (L5/L6) writes a Vacuum WAL record BEFORE calling this,
    /// so recovery can replay the removals.
    pub fn execute(
        &self,
        candidates: &[VacuumCandidate],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<u64>;

    /// Rebuild the pending queue from WAL replay.
    /// Called during recovery: each TxCommit with Replace/Delete generates
    /// candidates; each Vacuum record removes them.
    pub fn replay_commit(&mut self, candidate: VacuumCandidate);
    pub fn replay_vacuum(&mut self, collection_id: CollectionId, doc_id: &DocId, old_ts: Ts);

    /// Number of pending candidates.
    pub fn pending_count(&self) -> usize;
}

// --- Rollback Vacuum ---

/// Removes entries written by commits that were never replicated (ts > visible_ts).
/// This is the ONLY undo mechanism in the system.
///
/// NOTE: Rollback vacuum only handles B-tree entries (primary + secondary).
/// Catalog mutations (collection/index creation) from un-replicated commits
/// are rolled back by L5/L6, which has access to the Catalog.
pub struct RollbackVacuum;

impl RollbackVacuum {
    /// Live rollback: undo a single failed commit using its in-memory write set.
    /// Called when the primary loses quorum during replicate_and_wait().
    ///
    /// For each mutation in the write set:
    ///   - Delete the primary key doc_id || inv_ts(commit_ts)
    /// For each index delta with new_key:
    ///   - Delete the secondary key
    /// For each index delta with old_key (was a replace):
    ///   - The previous version is still in the B-tree, becomes "current" again
    ///
    /// O(write_set_size) -- no B-tree scan needed.
    pub fn rollback_commit(
        commit_ts: Ts,
        mutations: &[(CollectionId, DocId)],
        index_deltas: &[(IndexId, Option<Vec<u8>>)],  // (index_id, new_key to remove)
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<()>;

    /// Startup cleanup: undo all commits with ts > visible_ts using WAL.
    /// Called during recovery after WAL replay, if any commits exist
    /// beyond the visible_ts from the FileHeader.
    ///
    /// For each TxCommit WAL record where commit_ts > visible_ts:
    ///   - Delete primary key doc_id || inv_ts(commit_ts) for each mutation
    ///   - Delete new secondary keys from index deltas
    ///
    /// Returns the number of rolled-back commits.
    ///
    /// O(WAL_records_after_visible_ts) -- reads only the WAL tail.
    pub fn rollback_from_wal(
        visible_ts: Ts,
        wal_commits: &[WalCommitInfo],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<u64>;
}

/// Information extracted from a TxCommit WAL record for rollback.
/// Parsed by L5/L6 and passed to RollbackVacuum.
pub struct WalCommitInfo {
    pub commit_ts: Ts,
    pub mutations: Vec<(CollectionId, DocId)>,
    pub index_deltas: Vec<(IndexId, Option<Vec<u8>>)>,  // new_key to remove
}
```

## Implementation Details

### VacuumCoordinator::execute()

For each candidate:

1. **Remove primary entry**:
   ```
   let key = make_primary_key(&candidate.doc_id, candidate.old_ts);
   primary_indexes[&candidate.collection_id].btree().delete(&key)?;
   ```
   If the entry was external (heap-stored body), also free the heap ref. This requires reading the value first to check the external flag.

2. **Remove secondary entries**:
   ```
   for (index_id, encoded_key) in &candidate.old_index_keys {
       secondary_indexes[index_id].remove_entry(encoded_key)?;
   }
   ```

3. Count removed entries.

### External Body Cleanup

When vacuuming a primary entry that used external (heap) storage:

1. Read the B-tree value to check the external flag.
2. If external: parse the HeapRef, call `primary.heap().free(href)`.
3. Then delete the B-tree entry.

### drain_eligible()

A candidate is eligible if `candidate.superseding_ts <= vacuum_safe_ts`. The reasoning: the candidate was created when a newer version at `superseding_ts` was committed. If `vacuum_safe_ts >= superseding_ts`, then all readers can see the superseding version (or a later one), so no reader needs the old version.

```
let eligible: Vec<_> = self.pending
    .drain_filter(|c| c.superseding_ts <= vacuum_safe_ts)
    .collect();
```

Note: we check `superseding_ts`, not `old_ts`. Using `old_ts` would be wrong — a reader pinned between `old_ts` and `superseding_ts` still needs the old version because the superseding version isn't visible to that reader yet.

### RollbackVacuum::rollback_commit()

```
for (collection_id, doc_id) in mutations {
    let key = make_primary_key(doc_id, commit_ts);
    primary_indexes[collection_id].btree().delete(&key)?;
}
for (index_id, new_key) in index_deltas {
    if let Some(key) = new_key {
        secondary_indexes[index_id].remove_entry(key)?;
    }
}
```

### Recovery Queue Rebuild

On WAL replay:
1. Each `TxCommit` with Replace/Delete mutations -> `replay_commit()` pushes candidates.
2. Each `Vacuum` record -> `replay_vacuum()` removes matching candidates.
3. After replay, `pending` contains un-vacuumed candidates.

### Catalog Mutation Rollback

Rollback of catalog mutations (e.g., collection or index creation from an un-replicated commit) is NOT handled by this module. That responsibility belongs to L5/L6 (Transaction/Database layer), which has access to the Catalog and can revert metadata changes. D7 only handles B-tree entry removal.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| B-tree delete error | I/O failure during key removal | Propagate |
| Heap free error | I/O failure during heap cleanup | Propagate |
| Missing index | Vacuum candidate references dropped index | Skip (log warning) |

Vacuum operations are idempotent: deleting a key that doesn't exist is a no-op (`delete` returns false but doesn't error).

## Tests

1. **Push and drain**: push 3 candidates with superseding_ts 5, 10, 15. drain with vacuum_safe_ts=12. Verify 2 drained (superseding_ts 5 and 10) and 1 remaining.
2. **Execute removes primary entry**: insert version, execute vacuum on it, verify get_at_ts returns None.
3. **Execute removes secondary entries**: insert secondary entries, execute vacuum, verify entries gone.
4. **External body cleanup**: insert large doc (heap-stored), vacuum it, verify heap ref freed.
5. **Rollback single commit**: insert at ts=10, rollback ts=10, verify entry removed.
6. **Rollback preserves old version**: insert at ts=5, replace at ts=10, rollback ts=10 -- ts=5 version is still accessible.
7. **Rollback with index deltas**: insert with secondary entry, rollback removes secondary entry.
8. **Replay rebuild**: simulate WAL replay with commits and vacuums, verify pending queue matches expected state.
9. **Idempotent vacuum**: vacuum same candidate twice -- second time is a no-op.
10. **Empty candidates**: execute with empty list -> returns 0.
11. **drain_eligible with no eligible**: all candidates have superseding_ts > vacuum_safe_ts -> empty drain.
12. **Rollback from WAL**: simulate multiple commits after visible_ts, rollback_from_wal removes all of them.
13. **drain_eligible correctness**: candidate with old_ts=3, superseding_ts=8, vacuum_safe_ts=7 -> NOT eligible (superseding_ts > vacuum_safe_ts). Same candidate with vacuum_safe_ts=8 -> eligible.
