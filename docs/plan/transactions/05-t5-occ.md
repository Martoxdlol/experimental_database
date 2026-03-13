# T5: OCC Validation

## Purpose

Validates a transaction's read set against concurrent commits at commit time. This is the core conflict detection algorithm — if any key written by a concurrent commit falls within the transaction's effective read intervals, the transaction must abort.

Catalog conflicts (DDL) are handled uniformly through the same interval-based mechanism — catalog reads/writes are recorded as intervals on reserved pseudo-collections (CATALOG_COLLECTIONS, CATALOG_INDEXES). No separate catalog conflict detection is needed.

## Dependencies

- **T2 (`read_set.rs`)**: `ReadSet`, `ReadInterval`, `QueryId`
- **T4 (`commit_log.rs`)**: `CommitLog`, `CommitLogEntry`, `IndexKeyWrite`
- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`, `Ts`

## Rust Types

```rust
use exdb_core::types::{CollectionId, IndexId, Ts};
use crate::read_set::{ReadSet, QueryId};
use crate::commit_log::CommitLog;

/// The kind of conflict detected.
#[derive(Debug, Clone)]
pub enum ConflictKind {
    /// Interval overlap on an index (data or catalog).
    /// For catalog conflicts, collection_id will be CATALOG_COLLECTIONS
    /// or CATALOG_INDEXES — no separate variant needed.
    IndexInterval {
        collection_id: CollectionId,
        index_id: IndexId,
    },
}

/// OCC validation failure.
#[derive(Debug, Clone, thiserror::Error)]
#[error("OCC conflict at ts {conflicting_ts}: {kind:?}")]
pub struct ConflictError {
    /// The commit_ts of the conflicting concurrent commit.
    pub conflicting_ts: Ts,
    /// What kind of conflict.
    pub kind: ConflictKind,
    /// Query IDs whose intervals were overlapped (sorted ascending).
    /// Used by subscription notifications to identify which queries
    /// need re-execution.
    pub affected_query_ids: Vec<QueryId>,
}
```

## Implementation Details

### validate()

```rust
/// Validate read set against commit log.
///
/// Checks all commits in the danger window (begin_ts, commit_ts) for
/// interval overlaps. Handles both data and catalog conflicts uniformly —
/// catalog reads/writes appear as intervals on CATALOG_COLLECTIONS and
/// CATALOG_INDEXES pseudo-collections.
///
/// Returns Ok(()) if no conflicts, Err(ConflictError) with the FIRST
/// conflicting commit's details if a conflict is found.
///
/// **Critical correctness property**: uses `ReadInterval::contains_key`
/// which respects the effective interval (original bounds + LimitBoundary
/// tightening). Writes beyond the LIMIT cutoff do NOT cause conflicts.
///
/// **Does NOT call `extend_for_deltas`**: OCC validates the raw read set.
/// `extend_for_deltas` is only called at commit step 10a before
/// subscription registration.
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError>
```

**Algorithm** (per DESIGN.md 5.7):

```
for each entry in commit_log.entries_in_range(begin_ts, commit_ts):

    let mut affected_query_ids: BTreeSet<QueryId> = BTreeSet::new();
    let mut conflict_kind: Option<ConflictKind> = None;

    for ((coll_id, idx_id), key_writes) in &entry.index_writes:
        if let Some(intervals) = read_set.intervals.get(&(coll_id, idx_id)):
            for write in key_writes:
                for interval in intervals:
                    let hit =
                        write.old_key.as_ref().is_some_and(|k| interval.contains_key(k))
                     || write.new_key.as_ref().is_some_and(|k| interval.contains_key(k));
                    if hit:
                        affected_query_ids.insert(interval.query_id);
                        conflict_kind.get_or_insert(ConflictKind::IndexInterval {
                            collection_id: coll_id, index_id: idx_id,
                        });

    if !affected_query_ids.is_empty():
        return Err(ConflictError {
            conflicting_ts: entry.commit_ts,
            kind: conflict_kind.unwrap(),
            affected_query_ids: affected_query_ids.into_iter().collect(),
        })

// All checks passed
Ok(())
```

**On first conflicting commit**: collect ALL affected_query_ids from that commit (not just the first overlap), deduplicate and sort ascending, then return the error. This gives the subscription system complete information about which queries need re-execution.

### Performance Considerations

For typical workloads (a few dozen intervals, a few dozen concurrent key writes), the O(W × I) linear scan is efficient. If needed in the future, intervals within a group could be sorted by lower bound for binary search — O(W × log I) — but this is premature optimization.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| `ConflictError` | Written key overlaps a read interval (data or catalog) | Returned to caller (CommitCoordinator) |

## Tests

### Basic conflict detection

1. **no_conflict_disjoint**: Read interval [10, 20), concurrent write at key 25 → passes.
2. **conflict_old_key_in_interval**: Read interval [10, 20), concurrent write old_key = 15 → conflict.
3. **conflict_new_key_in_interval**: Read interval [10, 20), concurrent write new_key = 12 → conflict (phantom).
4. **both_keys_in_interval**: old_key = 11, new_key = 15 → conflict.
5. **old_key_in_new_key_out**: old_key = 15 (in range), new_key = 25 (out) → conflict.

### LimitBoundary interaction

6. **no_conflict_beyond_upper_limit**: Interval [10, 50) with Upper(20). Write at key 30 → passes (beyond limit).
7. **conflict_within_upper_limit**: Same interval. Write at key 15 → conflict.
8. **no_conflict_before_lower_limit**: Interval [10, 50) with Lower(30). Write at key 20 → passes.
9. **conflict_within_lower_limit**: Same interval. Write at key 35 → conflict.

### Boundary conditions

10. **begin_ts_exclusive**: Entry at exactly begin_ts is excluded from danger window.
11. **commit_ts_exclusive**: Entry at exactly commit_ts is excluded.
12. **empty_read_set**: Always passes (no intervals to conflict with).
13. **empty_commit_log**: Always passes (no concurrent commits).
14. **empty_write_set_still_validates**: Read-only tx with intervals still checks.

### Catalog conflicts (unified with data)

15. **catalog_name_point_read_vs_create**: Interval on (CATALOG_COLLECTIONS, NAME_IDX) covering "users". Concurrent write adds "users" key → conflict.
16. **catalog_name_point_read_no_conflict**: Same interval. Concurrent write adds "orders" key → passes.
17. **catalog_full_scan_vs_any_create**: Full-range interval on (CATALOG_COLLECTIONS, PRIMARY). Any catalog write → conflict.
18. **catalog_index_name_vs_create**: Interval on (CATALOG_INDEXES, NAME_IDX) covering (coll, "email"). Concurrent write adds matching key → conflict.
19. **catalog_index_list_vs_create_same_collection**: Range interval on CATALOG_INDEXES for coll prefix. Create index in same coll → conflict.
20. **catalog_index_list_vs_create_other_collection**: Same range. Create index in different coll → passes.

### Multiple conflicts

21. **affected_query_ids_collected**: Two intervals (Q1, Q3) overlap with same commit → affected_query_ids = [1, 3].
22. **first_conflicting_commit_returned**: Two concurrent commits conflict → error reports the first (lower ts).

### The K2/K3/K4 correctness example (from plan doc)

23. **interleaved_reads_writes_with_limits**: TX1 reads [K0,K99] limit=1 → K2 (Upper(K2)), deletes K2, reads again → K4 (Upper(K4)). TX2 inserts K3. OCC: K3 does not conflict with Q1's Upper(K2), but DOES conflict with Q2's Upper(K4).
