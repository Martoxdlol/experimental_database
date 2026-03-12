# T4: Commit Log — Recent Commit Tracking

**File:** `crates/tx/src/commit_log.rs`
**Depends on:** L1 (`exdb-core::types::{CollectionId, IndexId, DocId, Ts}`)
**Depended on by:** T5 (`occ.rs`), T7 (`commit.rs`)

## Purpose

In-memory structure tracking recent commits. Used for two purposes:

1. **OCC validation** (T5) — a committing transaction's read set is checked against all commits in `(begin_ts, commit_ts)` to detect conflicts.
2. **Pruning** — entries older than the oldest active transaction can be removed, since no future validation will need them.

The commit log is NOT a durable structure. It exists only in memory and is populated from WAL replay on recovery (or built up as commits happen at runtime). Its contents are derived from the index deltas computed at commit time.

## Data Structures

```rust
use std::collections::BTreeMap;
use exdb_core::types::{CollectionId, IndexId, DocId, Ts};
use super::write_set::CatalogMutation;

/// A single commit's footprint in the key space.
#[derive(Debug, Clone)]
pub struct CommitLogEntry {
    /// The commit timestamp.
    pub commit_ts: Ts,
    /// Index key writes grouped by (collection, index).
    /// Used for interval overlap checks during OCC validation.
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    /// Catalog mutations from this commit (if any).
    /// Used for catalog OCC validation (CatalogRead vs CatalogMutation).
    pub catalog_mutations: Vec<CatalogMutation>,
}

/// A single key write within a commit, for one index.
#[derive(Debug, Clone)]
pub struct IndexKeyWrite {
    pub doc_id: DocId,
    /// Old encoded key. None for inserts.
    pub old_key: Option<Vec<u8>>,
    /// New encoded key. None for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// The commit log — ordered sequence of recent commits.
pub struct CommitLog {
    /// Entries ordered by commit_ts (ascending).
    /// Invariant: entries[i].commit_ts < entries[i+1].commit_ts
    entries: Vec<CommitLogEntry>,
}
```

## API

```rust
impl CommitLog {
    /// Create an empty commit log.
    pub fn new() -> Self;

    /// Append a new commit entry. Must have commit_ts > all existing entries.
    /// Called by CommitCoordinator at step 5 (LOG).
    pub fn append(&mut self, entry: CommitLogEntry);

    /// Get all entries with commit_ts in the open interval (begin_ts, commit_ts).
    /// Used by OCC validation to find concurrent commits.
    ///
    /// Returns a slice of the entries vec (efficient — no allocation).
    /// Uses binary search on both bounds for O(log N) lookup.
    pub fn entries_in_range(&self, begin_ts: Ts, commit_ts: Ts) -> &[CommitLogEntry];

    /// Remove all entries with commit_ts <= threshold.
    /// Called periodically with threshold = oldest_active_begin_ts.
    /// After pruning, no active transaction will validate against removed entries.
    pub fn prune(&mut self, threshold: Ts);

    /// Number of entries currently in the log.
    pub fn len(&self) -> usize;

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool;

    /// Remove a specific entry by commit_ts.
    /// Used for rollback on quorum loss (step 8f of commit protocol).
    pub fn remove(&mut self, commit_ts: Ts) -> Option<CommitLogEntry>;
}
```

## Implementation Notes

### Storage: Vec with Binary Search

The commit log is a `Vec<CommitLogEntry>` sorted by `commit_ts`. This is optimal because:

- **Append** is O(1) amortized (push to end, always increasing ts)
- **Range query** is O(log N) via `partition_point` for both bounds
- **Prune** is O(K) where K is the number of removed entries (`drain(..idx)`)
- **Memory** is compact — no tree node overhead

The expected size is small: entries are pruned once no active transaction needs them. With transaction timeouts of 5 minutes and commits happening at ~1K/sec, the log holds at most ~300K entries. Each entry is a few hundred bytes (index writes), so total memory is well under 100 MB even under heavy load.

### Pruning Strategy

Pruning is triggered by the commit coordinator periodically or after each commit:

```
oldest_active = min(all active transaction begin_ts values)
commit_log.prune(oldest_active)
```

The oldest active transaction timestamp is tracked by L6 (Database), which maintains a set of active transactions. L5 receives this value from L6 when pruning.

### Recovery

On database startup, the commit log starts empty. WAL replay does NOT populate it — there are no active transactions during recovery, so there's nothing to validate against. The commit log is only needed for runtime OCC validation.

However, after recovery, any commits that were in progress at crash time are already either:
- Fully committed (WAL record present → replayed → committed)
- Never committed (no WAL record → as if never happened)

So the commit log's emptiness on startup is correct.

## Tests

```
t4_append_and_len
    Append 3 entries. len() returns 3.

t4_entries_in_range_basic
    Append entries at ts 10, 20, 30, 40, 50.
    entries_in_range(15, 35) returns entries at 20 and 30.

t4_entries_in_range_exclusive_bounds
    Append entries at ts 10, 20, 30.
    entries_in_range(10, 30) returns only entry at 20 (exclusive both ends).

t4_entries_in_range_empty
    entries_in_range(5, 8) on entries at 10, 20, 30 returns empty slice.

t4_entries_in_range_all
    entries_in_range(0, 100) on entries at 10, 20, 30 returns all three.

t4_prune
    Append entries at ts 10, 20, 30, 40.
    prune(25) → len() is 2 (entries 30, 40 remain).

t4_prune_none
    prune(5) on entries at 10, 20 → nothing removed.

t4_prune_all
    prune(100) on entries at 10, 20 → empty.

t4_remove_specific
    Append entries at ts 10, 20, 30.
    remove(20) → returns the entry, len() is 2.
    remove(99) → returns None, len() is 2.
```
