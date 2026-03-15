# T4: Commit Log

## Purpose

In-memory structure tracking recent commits for OCC validation and subscription invalidation. Indexes writes by `(collection, index)` for direct lookup against the read set. Pruned as old transactions complete.

## Dependencies

- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`, `DocId`, `Ts`

## Rust Types

```rust
use std::collections::BTreeMap;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};

/// Sentinel IndexId for the primary index.
/// Used for point-read OCC/subscriptions — get-by-id records a point
/// interval on this sentinel index.
pub const PRIMARY_INDEX_SENTINEL: IndexId = IndexId(0);

/// A single key write recorded in the commit log.
///
/// Stores both old and new keys so that OCC can detect both:
/// - Modifications to documents within the read set (old_key overlap)
/// - Phantoms entering the read set (new_key overlap)
pub struct IndexKeyWrite {
    pub doc_id: DocId,
    /// Encoded key that was removed. None for inserts.
    pub old_key: Option<Vec<u8>>,
    /// Encoded key that was added. None for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// Entry in the commit log tracking what a commit wrote.
///
/// Catalog DDL operations are represented as index key writes on the
/// reserved CATALOG_COLLECTIONS / CATALOG_INDEXES pseudo-collections.
/// No separate catalog_mutations field is needed — the same interval-based
/// conflict detection handles both data and catalog conflicts.
pub struct CommitLogEntry {
    pub commit_ts: Ts,
    /// Index key writes grouped by (collection, index).
    /// Includes both data writes AND catalog writes (on reserved CollectionIds).
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
}

/// In-memory commit log for OCC validation and subscription invalidation.
///
/// Entries are ordered by commit_ts (ascending). Supports efficient range
/// queries for the OCC danger window `(begin_ts, commit_ts)`.
pub struct CommitLog {
    entries: Vec<CommitLogEntry>,
}
```

## Implementation Details

### CommitLog methods

```rust
impl CommitLog {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Append a new commit entry. Must be called with monotonically
    /// increasing commit_ts values (enforced by single-writer).
    pub fn append(&mut self, entry: CommitLogEntry) {
        debug_assert!(
            self.entries.last().map_or(true, |e| e.commit_ts < entry.commit_ts),
            "commit log entries must be monotonically ordered"
        );
        self.entries.push(entry);
    }

    /// Get all entries with commit_ts in the open interval (begin_ts, commit_ts).
    ///
    /// These are the concurrent commits that could conflict with a
    /// transaction that started at begin_ts and is committing at commit_ts.
    /// Both endpoints are exclusive:
    /// - begin_ts: the transaction already saw this snapshot
    /// - commit_ts: this is our own commit, not a concurrent one
    pub fn entries_in_range(&self, begin_ts: Ts, commit_ts: Ts) -> &[CommitLogEntry] {
        // Binary search for first entry with commit_ts > begin_ts
        let start = self.entries.partition_point(|e| e.commit_ts <= begin_ts);
        // Binary search for first entry with commit_ts >= commit_ts
        let end = self.entries.partition_point(|e| e.commit_ts < commit_ts);
        &self.entries[start..end]
    }

    /// Prune entries no longer needed for OCC validation.
    ///
    /// Entries with commit_ts <= oldest_active_begin_ts can never be in
    /// any active transaction's danger window, so they are safe to remove.
    pub fn prune(&mut self, oldest_active_begin_ts: Ts) {
        let cutoff = self.entries.partition_point(|e| e.commit_ts <= oldest_active_begin_ts);
        self.entries.drain(..cutoff);
    }

    /// Remove a specific entry by commit_ts.
    ///
    /// Returns the removed entry, or None if not found.
    pub fn remove(&mut self, commit_ts: Ts) -> Option<CommitLogEntry> {
        let pos = self.entries.iter().position(|e| e.commit_ts == commit_ts)?;
        Some(self.entries.remove(pos))
    }

    /// Remove all entries with commit_ts > threshold.
    ///
    /// Used for rollback on replication failure: removes all commit log
    /// entries beyond visible_ts. This is critical because the writer may
    /// have validated subsequent commits against now-invalid entries.
    pub fn remove_after(&mut self, threshold: Ts) {
        let cutoff = self.entries.partition_point(|e| e.commit_ts <= threshold);
        self.entries.truncate(cutoff);
    }

    /// Number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
```

### Converting IndexDeltas to CommitLogEntry

This conversion happens in T7 (CommitCoordinator) at step 5, not in this module. The coordinator takes the `Vec<IndexDelta>` from `compute_index_deltas` and groups them:

```rust
// In T7 commit.rs:
fn build_commit_log_entry(
    commit_ts: Ts,
    index_deltas: &[IndexDelta],
) -> CommitLogEntry {
    let mut index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> = BTreeMap::new();
    for delta in index_deltas {
        index_writes
            .entry((delta.collection_id, delta.index_id))
            .or_default()
            .push(IndexKeyWrite {
                doc_id: delta.doc_id,
                old_key: delta.old_key.clone(),
                new_key: delta.new_key.clone(),
            });
    }
    CommitLogEntry { commit_ts, index_writes }
}
```

## Error Handling

No fallible operations. All methods are infallible. Debug assertions catch misuse (non-monotonic append).

## Tests

1. **new_is_empty**: `CommitLog::new()` — `is_empty()` is true, `len()` is 0.
2. **append_single**: Append one entry, `len()` is 1.
3. **entries_in_range_basic**: Append entries at ts 1, 2, 3, 4, 5. Range (1, 4) returns entries at ts 2, 3.
4. **entries_in_range_empty**: Range with no matching entries returns empty slice.
5. **entries_in_range_begin_exclusive**: Entry at exactly begin_ts is excluded.
6. **entries_in_range_commit_exclusive**: Entry at exactly commit_ts is excluded.
7. **entries_in_range_all**: Range (0, 100) with entries at 1–5 returns all.
8. **prune_removes_old**: Append at ts 1, 2, 3. Prune(2) removes entries at 1, 2.
9. **prune_empty_log**: Prune on empty log is a no-op.
10. **remove_existing**: Remove entry at specific ts, verify it's returned and log shrinks.
11. **remove_nonexistent**: Remove non-existent ts returns None.
12. **remove_after_basic**: Append at ts 1, 2, 3, 4, 5. `remove_after(3)` removes entries at 4, 5. `len()` == 3.
13. **remove_after_none**: `remove_after(10)` on log with entries 1–5 removes nothing.
14. **remove_after_all**: `remove_after(0)` removes all entries.
