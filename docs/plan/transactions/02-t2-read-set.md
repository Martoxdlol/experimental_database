# T2: Read Set — Scanned Intervals + Carry-Forward

**File:** `crates/tx/src/read_set.rs`
**Depends on:** L1 (`exdb-core::types::{CollectionId, IndexId}`)
**Depended on by:** T5 (`occ.rs`), T6 (`subscriptions.rs`), T7 (`commit.rs`), L6 (Transaction)

## Purpose

Tracks which portions of the index key space a transaction has observed, including catalog reads. The read set serves three roles:

1. **OCC validation** — compared against concurrent commits at commit time (T5)
2. **Subscription watch predicate** — registered in the subscription registry after commit (T6)
3. **Carry-forward source** — when a subscription chain continues, unaffected intervals are carried to the new transaction

## Data Structures

```rust
use std::ops::Bound;
use exdb_core::types::{CollectionId, IndexId};

pub type QueryId = u32;

/// A single read interval on an index, tagged with the query that produced it.
#[derive(Clone, Debug)]
pub struct ReadInterval {
    /// Which query produced this interval.
    /// Multiple intervals can share the same query_id (e.g., a query that
    /// scans multiple index segments). Multiple query_ids can also share
    /// the same interval if queries overlap and intervals are merged —
    /// in that case, the interval stores the minimum query_id.
    pub query_id: QueryId,
    /// Inclusive lower bound (encoded key bytes).
    pub lower: Vec<u8>,
    /// Upper bound: Excluded(key) or Unbounded.
    pub upper: Bound<Vec<u8>>,
}

/// All intervals for a transaction, grouped by (collection, index).
#[derive(Clone, Debug, Default)]
pub struct ReadSet {
    /// Read intervals grouped by (collection_id, index_id).
    /// Within each group, intervals are sorted by `lower` and non-overlapping
    /// (maintained by `merge_overlapping`).
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    /// Catalog observations (see below).
    pub catalog_reads: Vec<CatalogRead>,
    /// Next query_id to assign. Starts at 0 for fresh transactions,
    /// or at `first_query_id` for carry-forward transactions.
    next_query_id: QueryId,
}

/// Records a catalog observation made during the transaction.
/// Used for OCC conflict detection against concurrent DDL.
#[derive(Clone, Debug)]
pub enum CatalogRead {
    /// Transaction resolved a collection name → CollectionId.
    /// Conflicts with: create or drop of collection with this name.
    CollectionByName(String),
    /// Transaction listed all collections.
    /// Conflicts with: any collection create or drop.
    ListCollections,
    /// Transaction resolved an index by name within a collection.
    /// Conflicts with: create or drop of index with this name in this collection.
    IndexByName(CollectionId, String),
    /// Transaction listed indexes for a collection.
    /// Conflicts with: any index create or drop in this collection.
    ListIndexes(CollectionId),
}
```

## API

```rust
impl ReadSet {
    /// Create an empty read set. Query IDs start at 0.
    pub fn new() -> Self;

    /// Create a read set with a starting query_id offset.
    /// Used for carry-forward: the new transaction's query_ids
    /// continue from where the carried intervals left off.
    pub fn with_starting_query_id(first_query_id: QueryId) -> Self;

    /// Allocate the next query_id. Called by L6 before executing each query.
    /// Returns the ID and advances the counter.
    pub fn next_query_id(&mut self) -> QueryId;

    /// Return the current next_query_id value without advancing.
    pub fn peek_next_query_id(&self) -> QueryId;

    /// Add a read interval for a (collection, index) pair.
    /// The interval is inserted in sorted position and adjacent/overlapping
    /// intervals are merged immediately.
    pub fn add_interval(
        &mut self,
        collection_id: CollectionId,
        index_id: IndexId,
        interval: ReadInterval,
    );

    /// Add a catalog read observation.
    pub fn add_catalog_read(&mut self, read: CatalogRead);

    /// Merge overlapping or adjacent intervals within each group.
    /// Called automatically by add_interval, but can also be called
    /// explicitly after bulk operations.
    pub fn merge_overlapping(&mut self);

    /// Total number of intervals across all groups.
    pub fn interval_count(&self) -> usize;

    /// Check if the read set is empty (no intervals, no catalog reads).
    pub fn is_empty(&self) -> bool;

    // ─── Carry-Forward ───

    /// Split the read set at a query_id threshold.
    /// Returns a new ReadSet containing only intervals with `query_id < threshold`.
    /// Intervals in the returned set retain their original query_ids.
    ///
    /// Used for subscription chain carry-forward: when invalidation fires
    /// at query_id = Q_min, this extracts the unaffected prefix.
    ///
    /// If an interval has query_id >= threshold, it is excluded entirely.
    /// (Intervals store the minimum query_id from merging, so a merged
    /// interval's query_id represents its earliest contributing query.)
    pub fn split_before(&self, threshold: QueryId) -> ReadSet;

    /// Merge another ReadSet into this one.
    /// Used to combine carried intervals with newly produced intervals
    /// when a chain transaction commits.
    ///
    /// Catalog reads from `other` are appended.
    /// Intervals from `other` are inserted and merged.
    /// The next_query_id is set to max(self, other).
    pub fn merge_from(&mut self, other: &ReadSet);
}
```

## Interval Merging Rules

Within a `(collection_id, index_id)` group, intervals are kept sorted by `lower` and non-overlapping:

1. **Overlap:** Two intervals overlap if one's lower is less than the other's upper.
2. **Adjacent:** Two intervals are adjacent if one's upper equals the other's lower.
3. **Merge:** When overlapping or adjacent, combine into a single interval:
   - `lower` = min of both lowers
   - `upper` = max of both uppers (Unbounded > Excluded(x) for all x)
   - `query_id` = min of both query_ids (preserves earliest contributing query)

Taking the minimum `query_id` when merging is important for carry-forward: if Q1 and Q3 overlap and are merged with `query_id=1`, and Q3 is invalidated, the merged interval will be carried forward (since `1 < 3`). This is conservative — it means the carried interval might be slightly wider than strictly necessary, but it's always correct. A wider interval can only cause more conflicts, never fewer.

## Split-Before Algorithm

```
split_before(threshold):
    result = ReadSet::with_starting_query_id(0)
    for each (key, intervals) in self.intervals:
        kept = intervals.filter(|i| i.query_id < threshold)
        if !kept.is_empty():
            result.intervals.insert(key, kept.clone())
    // Catalog reads with no query_id are always carried
    // (they represent transaction-level observations)
    result.catalog_reads = self.catalog_reads.clone()
    return result
```

**Note on catalog reads:** Catalog reads are always carried forward in full. They don't have individual query IDs because they represent structural observations (e.g., "collection X exists") that are relevant to all queries in the transaction.

## Limit Checks

The read set enforces configurable limits (checked by L6, not L5):

| Limit | Default | Check Point |
|-------|---------|-------------|
| `max_intervals` | 4,096 | After `add_interval` |
| `max_scanned_bytes` | 64 MB | Tracked by L4 scan, checked by L6 |
| `max_scanned_docs` | 100,000 | Tracked by L4 scan, checked by L6 |

L5 provides `interval_count()` for L6 to check against `max_intervals`.

## Tests

```
t2_add_and_count
    Add 3 intervals to different (coll, idx) pairs. Assert count = 3.

t2_merge_overlapping
    Add two overlapping intervals on same (coll, idx).
    Assert merged into one. Assert query_id = min of both.

t2_merge_adjacent
    Add [0, Excluded(10)) and [10, Excluded(20)) on same group.
    Assert merged into [0, Excluded(20)).

t2_no_merge_different_groups
    Add intervals on (coll_1, idx_1) and (coll_1, idx_2).
    Assert count = 2 (no merge across groups).

t2_query_id_incremental
    next_query_id() returns 0, 1, 2, ...

t2_with_starting_query_id
    with_starting_query_id(5) → next_query_id() returns 5, 6, 7, ...

t2_split_before_basic
    Add intervals with query_ids [0, 1, 2, 3, 4].
    split_before(3) → contains only intervals with query_id 0, 1, 2.

t2_split_before_preserves_groups
    Add intervals across multiple (coll, idx) groups with mixed query_ids.
    split_before(2) → correct filtering within each group.

t2_split_before_empty
    split_before(0) → empty ReadSet.

t2_split_before_all
    split_before(100) on read set with max query_id 4 → full copy.

t2_merge_from
    Create two ReadSets. merge_from combines intervals and catalog reads.
    Overlapping intervals in same group are merged.

t2_catalog_reads_carried
    Add catalog reads + intervals. split_before(2) carries all catalog reads.

t2_merge_query_id_min
    Two overlapping intervals with query_id 1 and 3.
    Merged interval has query_id 1.
```
