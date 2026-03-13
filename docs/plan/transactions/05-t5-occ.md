# T5: OCC Validation — Conflict Detection

**File:** `crates/tx/src/occ.rs`
**Depends on:** T2 (`read_set.rs`), T4 (`commit_log.rs`)
**Depended on by:** T7 (`commit.rs`)

## Purpose

Validates a transaction's read set against concurrent commits to detect serialization conflicts. This is the core of Optimistic Concurrency Control (OCC): transactions proceed without locks, and conflicts are detected at commit time.

Two types of conflicts are checked:
1. **Index interval conflicts** — a concurrent commit wrote keys that overlap with the transaction's scanned intervals
2. **Catalog conflicts** — a concurrent commit performed DDL that conflicts with the transaction's catalog observations

## Data Structures

```rust
use exdb_core::types::{CollectionId, IndexId, Ts};
use super::read_set::{ReadSet, ReadInterval, CatalogRead, QueryId};
use super::commit_log::{CommitLog, CommitLogEntry};

/// Result of OCC validation.
#[derive(Debug)]
pub struct ConflictError {
    /// The commit_ts of the conflicting concurrent transaction.
    pub conflicting_ts: Ts,
    /// What kind of conflict was detected.
    pub kind: ConflictKind,
    /// The query_id(s) whose read intervals were overlapped.
    /// Sorted ascending. Used by subscription chain to determine
    /// which queries need re-execution.
    pub affected_query_ids: Vec<QueryId>,
}

#[derive(Debug)]
pub enum ConflictKind {
    /// A concurrent commit wrote keys overlapping this transaction's
    /// scanned index intervals.
    IndexInterval {
        collection_id: CollectionId,
        index_id: IndexId,
    },
    /// A concurrent commit performed DDL conflicting with this
    /// transaction's catalog observations.
    Catalog {
        description: String,
    },
}
```

## API

```rust
/// Validate a transaction's read set against the commit log.
///
/// Checks all commits in the open interval (begin_ts, commit_ts)
/// for conflicts with the read set's index intervals and catalog reads.
///
/// Returns Ok(()) if no conflicts, Err(ConflictError) with the first
/// detected conflict otherwise.
///
/// The algorithm short-circuits on the first conflict found. For
/// subscription invalidation (which needs ALL affected query_ids),
/// use the subscription registry's check_invalidation instead.
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError>;
```

## Algorithm

### Index Interval Validation

```
validate(read_set, commit_log, begin_ts, commit_ts):
    concurrent_entries = commit_log.entries_in_range(begin_ts, commit_ts)

    for each entry in concurrent_entries:
        // Check index interval conflicts
        for each (coll_id, idx_id), key_writes in entry.index_writes:
            // Look up read intervals for this (coll, idx)
            intervals = read_set.intervals.get((coll_id, idx_id))
            if intervals is None:
                continue  // no reads on this index

            for each key_write in key_writes:
                // Check old_key overlap (modification/deletion of observed data)
                if key_write.old_key is Some(k):
                    if let Some(qid) = key_overlaps_intervals(k, intervals):
                        return Err(ConflictError {
                            conflicting_ts: entry.commit_ts,
                            kind: IndexInterval { coll_id, idx_id },
                            affected_query_ids: vec![qid],
                        })

                // Check new_key overlap (phantom — new data enters observed range)
                if key_write.new_key is Some(k):
                    if let Some(qid) = key_overlaps_intervals(k, intervals):
                        return Err(ConflictError {
                            conflicting_ts: entry.commit_ts,
                            kind: IndexInterval { coll_id, idx_id },
                            affected_query_ids: vec![qid],
                        })

        // Check catalog conflicts
        if !entry.catalog_mutations.is_empty() && !read_set.catalog_reads.is_empty():
            validate_catalog(read_set.catalog_reads, entry.catalog_mutations)?

    return Ok(())
```

### Key Overlap Check

```rust
/// Check if an encoded key falls within any interval in a sorted group.
/// Returns the query_id of the first overlapping interval, or None.
///
/// Uses binary search: find the last interval whose lower <= key,
/// then check if key < upper. O(log I) per key.
fn key_overlaps_intervals(
    key: &[u8],
    intervals: &[ReadInterval],  // sorted by lower, non-overlapping
) -> Option<QueryId>;
```

**Binary search strategy:**

1. Find the rightmost interval where `interval.lower <= key` using `partition_point`
2. Check if `key` is within that interval's upper bound:
   - `upper = Unbounded` → always within
   - `upper = Excluded(bound)` → within if `key < bound`
3. If within → return `Some(interval.query_id)`
4. If not → return `None`

This works because intervals are sorted and non-overlapping: if the key doesn't fall in the interval whose lower is closest (from below), it can't fall in any other interval.

### Catalog Validation

```rust
/// Check if a set of catalog reads conflicts with a set of catalog mutations.
/// Returns true if any read-write conflict exists.
///
/// This is a PUBLIC function, shared with T6 (subscriptions.rs) for
/// subscription invalidation checks against catalog mutations.
pub fn catalog_conflicts(
    catalog_reads: &[CatalogRead],
    catalog_mutations: &[CatalogMutation],
) -> bool;

/// Wrapper that returns ConflictError on the first conflict found.
/// Used internally by validate().
fn validate_catalog(
    catalog_reads: &[CatalogRead],
    catalog_mutations: &[CatalogMutation],
    conflicting_ts: Ts,
) -> Result<(), ConflictError>;
```

**Conflict rules:**

| CatalogRead | Conflicts with CatalogMutation |
|-------------|-------------------------------|
| `CollectionByName("X")` | `CreateCollection { name: "X" }` or `DropCollection { name: "X" }` |
| `ListCollections` | Any `CreateCollection` or `DropCollection` |
| `IndexByName(coll, "Y")` | `CreateIndex { collection_id: coll, name: "Y" }` or `DropIndex { collection_id: coll, name: "Y" }` |
| `ListIndexes(coll)` | Any `CreateIndex { collection_id: coll }` or `DropIndex { collection_id: coll }` |

This is conservative: `ListCollections` conflicts with ANY collection DDL, even on collections the transaction never accessed. This is acceptable because DDL is rare.

## Edge Cases

### Empty Read Set
A transaction with no reads (write-only) has an empty read set. Validation trivially passes — there's nothing to conflict with. This is the common case for pure insert workloads.

### Empty Commit Log Range
If no commits occurred in `(begin_ts, commit_ts)`, `entries_in_range` returns an empty slice and validation passes immediately. This is the common case for low-contention workloads.

### Read-Only Transactions
Read-only transactions with `SubscriptionMode::None` don't go through OCC validation at all — they have no commit. Read-only transactions with subscriptions do "commit" (to register the subscription) but skip OCC since they made no writes.

### Multiple Conflicts
`validate()` returns on the first conflict found. This is sufficient for commit rejection. For subscription invalidation (which needs ALL affected query_ids), use `SubscriptionRegistry::check_invalidation` which performs a complete scan.

## Tests

```
t5_no_conflict_disjoint_intervals
    Read set: intervals on (coll_1, idx_1) covering [10, 20).
    Commit log: entry writing key 25 on (coll_1, idx_1).
    validate() → Ok.

t5_conflict_old_key_in_interval
    Read set: interval [10, 20) on (coll_1, idx_1).
    Commit log: entry with old_key=15 on (coll_1, idx_1).
    validate() → Err (modification of observed data).

t5_conflict_new_key_in_interval (phantom)
    Read set: interval [10, 20) on (coll_1, idx_1).
    Commit log: entry with new_key=15 on (coll_1, idx_1).
    validate() → Err (phantom).

t5_conflict_returns_query_id
    Read set: interval with query_id=3 covering [10, 20).
    Conflicting write at key 15.
    ConflictError.affected_query_ids == [3].

t5_no_conflict_different_index
    Read set: intervals on (coll_1, idx_1).
    Commit log: writes on (coll_1, idx_2).
    validate() → Ok.

t5_no_conflict_outside_ts_range
    Read set: intervals on (coll_1, idx_1).
    Commit log: entry at ts=5 writing into interval. begin_ts=10.
    validate(begin_ts=10, commit_ts=20) → Ok (entry at ts=5 is before begin_ts).

t5_unbounded_upper
    Read set: interval [10, Unbounded) on (coll_1, idx_1).
    Commit log: entry with new_key=9999.
    validate() → Err.

t5_catalog_conflict_collection_by_name
    CatalogRead::CollectionByName("users").
    Concurrent CatalogMutation::DropCollection { name: "users" }.
    validate() → Err(Catalog).

t5_catalog_conflict_list_collections
    CatalogRead::ListCollections.
    Concurrent CatalogMutation::CreateCollection { name: "orders" }.
    validate() → Err(Catalog).

t5_catalog_no_conflict_different_collection
    CatalogRead::CollectionByName("users").
    Concurrent CatalogMutation::CreateCollection { name: "orders" }.
    validate() → Ok.

t5_catalog_conflict_index_by_name
    CatalogRead::IndexByName(coll_1, "by_email").
    Concurrent CatalogMutation::DropIndex { collection_id: coll_1, name: "by_email" }.
    validate() → Err(Catalog).

t5_empty_read_set_always_passes
    Empty read set. Commit log has entries. validate() → Ok.

t5_multiple_concurrent_commits
    Two entries in commit log range. First doesn't conflict, second does.
    validate() → Err with second entry's ts.

t5_binary_search_correctness
    Add 100 non-overlapping intervals. Check key that falls in interval #73.
    Verify correct query_id returned.
```
