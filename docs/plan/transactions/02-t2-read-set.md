# T2: Read Set

## Purpose

Tracks which portions of the index key space a transaction has observed. Used for OCC validation at commit time (T5), subscription invalidation (T6), and read set carry-forward for subscription chains. This is the core correctness data structure — all conflict detection flows through `ReadInterval::contains_key`.

Catalog operations (DDL) are modeled as regular intervals on reserved pseudo-collections rather than a separate `CatalogRead` type. This eliminates special-casing in OCC and subscriptions — one code path handles both data and catalog conflicts.

## Dependencies

- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`
- **T3 (`write_set.rs`)**: `IndexDelta` (for `extend_for_deltas` — but only the type, not the module logic)

No L2/L3 dependency. Pure data structure with byte comparisons.

## Rust Types

```rust
use std::collections::BTreeMap;
use std::ops::Bound;
use exdb_core::types::{CollectionId, IndexId};

// ─── Reserved IDs for Catalog Pseudo-Collections ───
//
// Catalog operations (DDL) are modeled as regular read/write intervals
// on these reserved pseudo-collections. L6 provides a thin wrapper that
// encodes DDL operations into intervals on these IDs.
//
// This eliminates the need for a separate CatalogRead enum,
// catalog_conflicts() function, and catalog special-casing in OCC/subscriptions.

/// Reserved CollectionId for the "_collections" pseudo-collection.
/// Stores collection metadata. DDL like create/drop collection produces
/// intervals and deltas on this collection.
pub const CATALOG_COLLECTIONS: CollectionId = CollectionId(0);

/// Reserved CollectionId for the "_indexes" pseudo-collection.
/// Stores index metadata. DDL like create/drop index produces
/// intervals and deltas on this collection.
pub const CATALOG_INDEXES: CollectionId = CollectionId(1);

/// Reserved IndexId for the name lookup index on _collections.
/// Key encoding: encode_key_prefix(&[Scalar::String(name)]).
/// Used by L6 when resolving collection names.
pub const CATALOG_COLLECTIONS_NAME_IDX: IndexId = IndexId(1);

/// Reserved IndexId for the (collection_id, name) lookup index on _indexes.
/// Key encoding: encode_key_prefix(&[Scalar::Int64(coll_id), Scalar::String(name)]).
/// Used by L6 when resolving index names.
pub const CATALOG_INDEXES_NAME_IDX: IndexId = IndexId(2);

/// Identifies which query produced a read interval. Assigned sequentially.
/// Used for subscription invalidation notifications and carry-forward.
pub type QueryId = u32;

/// Indicates which side of the interval was tightened by a LIMIT clause.
///
/// When a query returns exactly `limit` results, the scan stopped at the
/// last result's sort key. The interval tightens beyond what the range
/// expressions alone would produce:
///
/// - ASC scan: upper bound tightens to Excluded(successor(K))
/// - DESC scan: lower bound tightens to Included(K)
///
/// Stored on the interval so that `apply_delta` can clear it when the
/// boundary document moves, restoring full original-bounds coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitBoundary {
    /// ASC scan stopped after returning the doc with sort key K.
    /// Effective upper = Excluded(successor(K)).
    /// Cleared when K's doc is deleted or its key moves outside the interval.
    Upper(Vec<u8>),

    /// DESC scan stopped after returning the doc with sort key K.
    /// Effective lower = Included(K).
    /// Cleared when K's doc is deleted or its key moves outside the interval.
    Lower(Vec<u8>),
}

/// A single read interval on an index.
///
/// Represents a contiguous range of encoded index keys that was scanned
/// during query execution. The `query_id` links the interval back to the
/// specific query operation for subscription notifications.
#[derive(Debug, Clone)]
pub struct ReadInterval {
    pub query_id: QueryId,
    /// Original range lower bound (before any LIMIT tightening).
    pub lower: Bound<Vec<u8>>,
    /// Original range upper bound (before any LIMIT tightening).
    pub upper: Bound<Vec<u8>>,
    /// LIMIT tightening. None = scan exhausted the range (full original coverage).
    pub limit_boundary: Option<LimitBoundary>,
}

/// All intervals for a transaction, grouped by (collection, index).
///
/// Catalog reads (DDL observations) are recorded as regular intervals on
/// the reserved CATALOG_COLLECTIONS / CATALOG_INDEXES pseudo-collections.
/// No separate CatalogRead type is needed.
pub struct ReadSet {
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    next_query_id: QueryId,
}
```

## Implementation Details

### ReadInterval::contains_key()

```rust
/// Check if `key` falls within the **effective** interval.
///
/// The effective interval is the original [lower, upper) range further
/// tightened by any LimitBoundary. A key must pass BOTH checks:
///
///   (1) Within original bounds:
///       key >= lower AND (upper == Unbounded OR key < excluded_upper)
///
///   (2) Within limit tightening (if any):
///       None       => true              (no tightening, full original range)
///       Upper(K)   => key <= K          (ASC: beyond K was never scanned)
///       Lower(K)   => key >= K          (DESC: before K was never scanned)
pub fn contains_key(&self, key: &[u8]) -> bool {
    // Step 1: Check original bounds
    let above_lower = match &self.lower {
        Bound::Included(lo) => key >= lo.as_slice(),
        Bound::Excluded(lo) => key > lo.as_slice(),
        Bound::Unbounded => true,
    };
    if !above_lower {
        return false;
    }

    let below_upper = match &self.upper {
        Bound::Included(hi) => key <= hi.as_slice(),
        Bound::Excluded(hi) => key < hi.as_slice(),
        Bound::Unbounded => true,
    };
    if !below_upper {
        return false;
    }

    // Step 2: Check limit tightening
    match &self.limit_boundary {
        None => true,
        Some(LimitBoundary::Upper(k)) => key <= k.as_slice(),
        Some(LimitBoundary::Lower(k)) => key >= k.as_slice(),
    }
}
```

This is the most critical function in the entire transaction system. Both OCC (T5) and subscription invalidation (T6) depend on it being correct.

### ReadInterval::apply_delta()

```rust
/// Process a write delta. If `old_key` falls within the effective interval
/// and `new_key` is None or outside the effective interval, clear
/// `limit_boundary` to restore full original-bounds coverage.
///
/// Called on each interval before subscription registration (commit step 10a)
/// to account for the transaction's own writes potentially moving the
/// boundary document.
///
/// Conservative: clearing the boundary only widens the interval, never
/// narrows it — so it may cause more future subscription invalidations,
/// but never misses one.
///
/// NOT called before OCC validation — OCC validates the raw read set.
pub fn apply_delta(&mut self, old_key: Option<&[u8]>, new_key: Option<&[u8]>) {
    if self.limit_boundary.is_none() {
        return; // Already full coverage, nothing to clear
    }

    // Check if old_key is within the effective interval
    let old_in = old_key.is_some_and(|k| self.contains_key(k));
    if !old_in {
        return; // Delta doesn't affect the boundary
    }

    // Check if new_key is outside the effective interval (or absent)
    let new_out = new_key.is_none() || new_key.is_some_and(|k| !self.contains_key(k));
    if new_out {
        // Boundary doc moved out — clear limit to restore full range
        self.limit_boundary = None;
    }
}
```

### ReadSet::new()

```rust
pub fn new() -> Self {
    Self {
        intervals: BTreeMap::new(),
        next_query_id: 0,
    }
}
```

### ReadSet::with_starting_query_id()

```rust
/// Create with a starting query_id offset.
/// Used for subscription chain carry-forward: the new transaction's
/// query_ids start where the carried read set left off.
pub fn with_starting_query_id(first_query_id: QueryId) -> Self {
    Self {
        intervals: BTreeMap::new(),
        next_query_id: first_query_id,
    }
}
```

### ReadSet::add_interval()

```rust
pub fn add_interval(
    &mut self,
    collection_id: CollectionId,
    index_id: IndexId,
    interval: ReadInterval,
) {
    self.intervals
        .entry((collection_id, index_id))
        .or_default()
        .push(interval);
}
```

### ReadSet::next_query_id() / peek_next_query_id() / set_next_query_id()

```rust
pub fn next_query_id(&mut self) -> QueryId {
    let id = self.next_query_id;
    self.next_query_id += 1;
    id
}

pub fn peek_next_query_id(&self) -> QueryId {
    self.next_query_id
}

pub fn set_next_query_id(&mut self, id: QueryId) {
    self.next_query_id = id;
}
```

### ReadSet::merge_overlapping()

```rust
/// Merge adjacent/overlapping intervals within each (collection, index) group.
///
/// Two intervals overlap if their effective ranges intersect. After merging:
/// - lower = min(both lowers)
/// - upper = max(both uppers)
/// - query_id = min(both)  [conservative — wider carried interval]
/// - limit_boundary: if either has None (full coverage), merged = None.
///   Otherwise take most extreme: max(Upper) for ASC, min(Lower) for DESC.
///   If different LimitBoundary variants, set to None (conservative).
pub fn merge_overlapping(&mut self);
```

Implementation: for each group, sort by lower bound, then linear scan merging overlapping consecutive intervals.

### ReadSet::split_before()

```rust
/// Extract intervals with query_id < threshold into a new ReadSet.
///
/// Used for subscription chain carry-forward. When invalidation fires
/// at Q_min, `split_before(Q_min)` extracts unaffected intervals.
///
/// Catalog intervals (on CATALOG_COLLECTIONS / CATALOG_INDEXES) are
/// treated the same as data intervals — they are carried if their
/// query_id < threshold, just like any other interval.
pub fn split_before(&self, threshold: QueryId) -> ReadSet {
    let mut carried = ReadSet::new();
    for (&(coll, idx), intervals) in &self.intervals {
        for interval in intervals {
            if interval.query_id < threshold {
                carried.add_interval(coll, idx, interval.clone());
            }
        }
    }
    carried
}
```

### ReadSet::merge_from()

```rust
/// Merge another ReadSet into this one (carried + new intervals).
pub fn merge_from(&mut self, other: &ReadSet) {
    for (&(coll, idx), intervals) in &other.intervals {
        for interval in intervals {
            self.add_interval(coll, idx, interval.clone());
        }
    }
}
```

### ReadSet::extend_for_deltas()

```rust
/// Apply a batch of index write deltas to all matching intervals.
///
/// For each delta, calls `ReadInterval::apply_delta` on every interval
/// whose (collection, index) matches. Used at commit step 10a to clear
/// stale `limit_boundary` values from the tx's own writes before the
/// read set is handed to the subscription registry.
///
/// The tx's own writes are NOT in the commit log range checked by OCC,
/// but they can still move boundary docs — this function handles that case.
pub fn extend_for_deltas(&mut self, deltas: &[IndexDelta]) {
    for delta in deltas {
        if let Some(intervals) = self.intervals.get_mut(&(delta.collection_id, delta.index_id)) {
            for interval in intervals.iter_mut() {
                interval.apply_delta(
                    delta.old_key.as_deref(),
                    delta.new_key.as_deref(),
                );
            }
        }
    }
}
```

### ReadSet::interval_count()

```rust
pub fn interval_count(&self) -> usize {
    self.intervals.values().map(|v| v.len()).sum()
}
```

## Error Handling

No fallible operations. All methods are infallible. Read set size limits (DESIGN.md 5.6.5) are enforced by L6, not here.

## Tests

### contains_key tests

1. **contains_key_included_range**: key within `[Included(lo), Excluded(hi))` returns true.
2. **contains_key_below_lower**: key below lower bound returns false.
3. **contains_key_above_upper**: key at or above excluded upper returns false.
4. **contains_key_unbounded**: `Unbounded` on both sides — all keys return true.
5. **contains_key_with_upper_limit**: key within original but beyond `LimitBoundary::Upper(K)` returns false.
6. **contains_key_at_upper_limit**: key equal to K with `Upper(K)` returns true (`key <= K`).
7. **contains_key_with_lower_limit**: key within original but before `LimitBoundary::Lower(K)` returns false.
8. **contains_key_at_lower_limit**: key equal to K with `Lower(K)` returns true (`key >= K`).
9. **contains_key_no_limit**: `limit_boundary = None` — all keys in original range return true.

### apply_delta tests

10. **apply_delta_clears_limit_on_delete**: old_key in effective range, new_key = None → limit cleared.
11. **apply_delta_clears_limit_on_move_out**: old_key in range, new_key outside range → limit cleared.
12. **apply_delta_no_clear_on_move_within**: old_key in range, new_key also in range → limit stays.
13. **apply_delta_no_clear_old_outside**: old_key outside effective range → no change.
14. **apply_delta_noop_when_no_limit**: limit_boundary already None → no change.

### ReadSet tests

15. **new_starts_at_zero**: `new()` → `peek_next_query_id()` == 0.
16. **with_starting_query_id**: starts at specified offset.
17. **add_interval_groups_by_collection_index**: two intervals on same (coll, idx) are in same group.
18. **split_before_basic**: intervals Q0, Q1, Q2 — `split_before(1)` returns only Q0.
19. **split_before_includes_catalog_intervals**: catalog interval with query_id < threshold is carried.
20. **merge_from_combines**: merge two ReadSets, verify all intervals present.
21. **merge_overlapping_adjacent**: two adjacent intervals merge into one.
22. **merge_overlapping_different_indexes**: intervals on different indexes do not merge.
23. **merge_overlapping_limit_boundary_conservative**: if one has None → merged is None. Both Upper → take max.
24. **extend_for_deltas_clears_matching**: delta on matching (coll, idx) clears stale limit.
25. **extend_for_deltas_ignores_unmatched**: delta on different (coll, idx) does not affect interval.
26. **interval_count**: counts across all groups.
