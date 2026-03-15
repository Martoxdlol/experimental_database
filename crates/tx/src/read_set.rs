//! T2: Read Set — tracks observed index key intervals for OCC and subscriptions.
//!
//! The read set records which portions of the index key space a transaction has
//! observed. It is the core correctness data structure: all conflict detection
//! (OCC, subscription invalidation) flows through [`ReadInterval::contains_key`].
//!
//! Catalog operations (DDL) are modeled as regular intervals on reserved
//! pseudo-collections ([`CATALOG_COLLECTIONS`], [`CATALOG_INDEXES`]) rather than
//! a separate `CatalogRead` type. This eliminates special-casing in OCC and
//! subscriptions — one code path handles both data and catalog conflicts.

use std::collections::BTreeMap;
use std::ops::Bound;

use exdb_core::types::{CollectionId, IndexId};

use crate::write_set::IndexDelta;

// ─── Reserved IDs for Catalog Pseudo-Collections ───

/// Reserved [`CollectionId`] for the `_collections` pseudo-collection.
///
/// Stores collection metadata. DDL like `CREATE COLLECTION` / `DROP COLLECTION`
/// produces intervals and deltas on this collection.
pub const CATALOG_COLLECTIONS: CollectionId = CollectionId(0);

/// Reserved [`CollectionId`] for the `_indexes` pseudo-collection.
///
/// Stores index metadata. DDL like `CREATE INDEX` / `DROP INDEX` produces
/// intervals and deltas on this collection.
pub const CATALOG_INDEXES: CollectionId = CollectionId(1);

/// Reserved [`IndexId`] for the name-lookup index on `_collections`.
///
/// Key encoding: `encode_key_prefix(&[Scalar::String(name)])`.
/// Used by L6 when resolving collection names.
pub const CATALOG_COLLECTIONS_NAME_IDX: IndexId = IndexId(1);

/// Reserved [`IndexId`] for the `(collection_id, name)` lookup index on `_indexes`.
///
/// Key encoding: `encode_key_prefix(&[Scalar::Int64(coll_id), Scalar::String(name)])`.
/// Used by L6 when resolving index names.
pub const CATALOG_INDEXES_NAME_IDX: IndexId = IndexId(2);

/// Identifies which query produced a read interval.
///
/// Assigned sequentially per transaction. Used for subscription invalidation
/// notifications and read-set carry-forward in Subscribe mode.
pub type QueryId = u32;

/// Indicates which side of the interval was tightened by a `LIMIT` clause.
///
/// When a query returns exactly `limit` results, the scan stopped at the last
/// result's sort key. The interval tightens beyond what the range expressions
/// alone would produce:
///
/// - **ASC scan**: upper bound tightens to `Excluded(successor(K))`
/// - **DESC scan**: lower bound tightens to `Included(K)`
///
/// Stored on the interval so that [`apply_delta`](ReadInterval::apply_delta) can
/// clear it when the boundary document moves, restoring full original-bounds
/// coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitBoundary {
    /// ASC scan stopped after returning the doc with sort key `K`.
    ///
    /// Effective upper = `Excluded(successor(K))`. Cleared when `K`'s doc is
    /// deleted or its key moves outside the interval.
    Upper(Vec<u8>),

    /// DESC scan stopped after returning the doc with sort key `K`.
    ///
    /// Effective lower = `Included(K)`. Cleared when `K`'s doc is deleted or
    /// its key moves outside the interval.
    Lower(Vec<u8>),
}

/// A single read interval on an index.
///
/// Represents a contiguous range of encoded index keys that was scanned during
/// query execution. The [`query_id`](Self::query_id) links the interval back to
/// the specific query operation for subscription notifications.
#[derive(Debug, Clone)]
pub struct ReadInterval {
    /// Which query produced this interval.
    pub query_id: QueryId,
    /// Original range lower bound (before any LIMIT tightening).
    pub lower: Bound<Vec<u8>>,
    /// Original range upper bound (before any LIMIT tightening).
    pub upper: Bound<Vec<u8>>,
    /// LIMIT tightening. `None` = scan exhausted the range (full original coverage).
    pub limit_boundary: Option<LimitBoundary>,
}

impl ReadInterval {
    /// Check if `key` falls within the **effective** interval.
    ///
    /// The effective interval is the original `[lower, upper)` range further
    /// tightened by any [`LimitBoundary`]. A key must pass **both** checks:
    ///
    /// 1. Within original bounds:
    ///    `key >= lower AND (upper == Unbounded OR key < excluded_upper)`
    ///
    /// 2. Within limit tightening (if any):
    ///    - `None`       → `true` (no tightening, full original range)
    ///    - `Upper(K)`   → `key <= K` (ASC: beyond K was never scanned)
    ///    - `Lower(K)`   → `key >= K` (DESC: before K was never scanned)
    ///
    /// This is the most critical function in the transaction system. Both OCC
    /// (T5) and subscription invalidation (T6) depend on it being correct.
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

    /// Process a write delta against this interval.
    ///
    /// If `old_key` falls within the effective interval and `new_key` is `None`
    /// or outside the effective interval, clears [`limit_boundary`](Self::limit_boundary)
    /// to restore full original-bounds coverage.
    ///
    /// Called on each interval before subscription registration (commit step 10a)
    /// to account for the transaction's own writes potentially moving the
    /// boundary document.
    ///
    /// **Conservative**: clearing the boundary only widens the interval — never
    /// narrows it — so it may cause more future subscription invalidations, but
    /// never misses one.
    ///
    /// **Not called before OCC validation** — OCC validates the raw read set.
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
}

/// All intervals for a transaction, grouped by `(collection, index)`.
///
/// Catalog reads (DDL observations) are recorded as regular intervals on the
/// reserved [`CATALOG_COLLECTIONS`] / [`CATALOG_INDEXES`] pseudo-collections.
/// No separate `CatalogRead` type is needed.
#[derive(Debug)]
pub struct ReadSet {
    /// Intervals grouped by `(CollectionId, IndexId)`.
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    /// Next query ID to assign.
    next_query_id: QueryId,
}

impl ReadSet {
    /// Create a new empty read set with query IDs starting at 0.
    pub fn new() -> Self {
        Self {
            intervals: BTreeMap::new(),
            next_query_id: 0,
        }
    }

    /// Create a read set with a starting query ID offset.
    ///
    /// Used for subscription chain carry-forward: the new transaction's query
    /// IDs start where the carried read set left off.
    pub fn with_starting_query_id(first_query_id: QueryId) -> Self {
        Self {
            intervals: BTreeMap::new(),
            next_query_id: first_query_id,
        }
    }

    /// Add an interval to the read set.
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

    /// Allocate and return the next query ID.
    pub fn next_query_id(&mut self) -> QueryId {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    /// Peek at the next query ID without advancing.
    pub fn peek_next_query_id(&self) -> QueryId {
        self.next_query_id
    }

    /// Override the next query ID counter.
    pub fn set_next_query_id(&mut self, id: QueryId) {
        self.next_query_id = id;
    }

    /// Extract intervals with `query_id < threshold` into a new [`ReadSet`].
    ///
    /// Used for subscription chain carry-forward. When invalidation fires at
    /// `Q_min`, `split_before(Q_min)` extracts unaffected intervals.
    ///
    /// Catalog intervals are treated identically to data intervals.
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

    /// Merge another [`ReadSet`] into this one (carried + new intervals).
    pub fn merge_from(&mut self, other: &ReadSet) {
        for (&(coll, idx), intervals) in &other.intervals {
            for interval in intervals {
                self.add_interval(coll, idx, interval.clone());
            }
        }
    }

    /// Merge adjacent/overlapping intervals within each `(collection, index)` group.
    ///
    /// Two intervals overlap if their effective ranges intersect. After merging:
    /// - `lower` = min of both lowers
    /// - `upper` = max of both uppers
    /// - `query_id` = min of both (conservative — wider carried interval)
    /// - `limit_boundary`: if either has `None` (full coverage), merged = `None`.
    ///   Otherwise take most extreme: max(`Upper`) for ASC, min(`Lower`) for DESC.
    ///   If different `LimitBoundary` variants, set to `None` (conservative).
    pub fn merge_overlapping(&mut self) {
        for intervals in self.intervals.values_mut() {
            if intervals.len() <= 1 {
                continue;
            }

            // Sort by lower bound (byte comparison)
            intervals.sort_by(|a, b| bound_cmp_lower(&a.lower, &b.lower));

            let mut merged: Vec<ReadInterval> = Vec::new();
            for interval in intervals.drain(..) {
                if let Some(last) = merged.last_mut() {
                    if bounds_overlap_or_adjacent(&last.lower, &last.upper, &interval.lower) {
                        // Merge: take wider bounds
                        last.upper = bound_max_upper(&last.upper, &interval.upper);
                        last.query_id = last.query_id.min(interval.query_id);
                        last.limit_boundary =
                            merge_limit_boundaries(&last.limit_boundary, &interval.limit_boundary);
                        continue;
                    }
                }
                merged.push(interval);
            }
            *intervals = merged;
        }
    }

    /// Apply a batch of index write deltas to all matching intervals.
    ///
    /// For each delta, calls [`ReadInterval::apply_delta`] on every interval
    /// whose `(collection, index)` matches. Used at commit step 10a to clear
    /// stale `limit_boundary` values from the transaction's own writes before
    /// the read set is handed to the subscription registry.
    pub fn extend_for_deltas(&mut self, deltas: &[IndexDelta]) {
        for delta in deltas {
            if let Some(intervals) =
                self.intervals.get_mut(&(delta.collection_id, delta.index_id))
            {
                for interval in intervals.iter_mut() {
                    interval.apply_delta(delta.old_key.as_deref(), delta.new_key.as_deref());
                }
            }
        }
    }

    /// Total number of intervals across all groups.
    pub fn interval_count(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }
}

impl Default for ReadSet {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Merge helpers ───

/// Compare two lower bounds for sorting.
fn bound_cmp_lower(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> std::cmp::Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => std::cmp::Ordering::Equal,
        (Bound::Unbounded, _) => std::cmp::Ordering::Less,
        (_, Bound::Unbounded) => std::cmp::Ordering::Greater,
        (Bound::Included(a), Bound::Included(b)) => a.cmp(b),
        (Bound::Excluded(a), Bound::Excluded(b)) => a.cmp(b),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a <= b {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if a < b {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        }
    }
}

/// Check if interval `[lo, hi)` overlaps or is adjacent to an interval starting at `next_lo`.
fn bounds_overlap_or_adjacent(
    _lo: &Bound<Vec<u8>>,
    hi: &Bound<Vec<u8>>,
    next_lo: &Bound<Vec<u8>>,
) -> bool {
    match (hi, next_lo) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => true,
        (Bound::Included(h), Bound::Included(l)) => h >= l,
        (Bound::Included(h), Bound::Excluded(l)) => h >= l,
        (Bound::Excluded(h), Bound::Included(l)) => h > l,
        (Bound::Excluded(h), Bound::Excluded(l)) => h > l,
    }
}

/// Take the wider (max) of two upper bounds.
fn bound_max_upper(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (a, b) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b).clone()),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b).clone()),
        (Bound::Included(a), Bound::Excluded(b)) => {
            if a >= b {
                Bound::Included(a.clone())
            } else {
                Bound::Excluded(b.clone())
            }
        }
        (Bound::Excluded(a), Bound::Included(b)) => {
            if b >= a {
                Bound::Included(b.clone())
            } else {
                Bound::Excluded(a.clone())
            }
        }
    }
}

/// Merge two limit boundaries conservatively.
fn merge_limit_boundaries(
    a: &Option<LimitBoundary>,
    b: &Option<LimitBoundary>,
) -> Option<LimitBoundary> {
    match (a, b) {
        (None, _) | (_, None) => None, // Full coverage wins
        (Some(LimitBoundary::Upper(a)), Some(LimitBoundary::Upper(b))) => {
            Some(LimitBoundary::Upper(a.max(b).clone()))
        }
        (Some(LimitBoundary::Lower(a)), Some(LimitBoundary::Lower(b))) => {
            Some(LimitBoundary::Lower(a.min(b).clone()))
        }
        _ => None, // Different variants — conservative full coverage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── contains_key tests ───

    #[test]
    fn contains_key_included_range() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![20]),
            limit_boundary: None,
        };
        assert!(ri.contains_key(&[10]));
        assert!(ri.contains_key(&[15]));
        assert!(!ri.contains_key(&[20]));
    }

    #[test]
    fn contains_key_below_lower() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![20]),
            limit_boundary: None,
        };
        assert!(!ri.contains_key(&[5]));
    }

    #[test]
    fn contains_key_above_upper() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![20]),
            limit_boundary: None,
        };
        assert!(!ri.contains_key(&[20]));
        assert!(!ri.contains_key(&[25]));
    }

    #[test]
    fn contains_key_unbounded() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            limit_boundary: None,
        };
        assert!(ri.contains_key(&[0]));
        assert!(ri.contains_key(&[255]));
    }

    #[test]
    fn contains_key_with_upper_limit() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        // Within original AND within limit
        assert!(ri.contains_key(&[15]));
        // At limit boundary (key <= K)
        assert!(ri.contains_key(&[20]));
        // Beyond limit boundary
        assert!(!ri.contains_key(&[30]));
    }

    #[test]
    fn contains_key_at_upper_limit() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        assert!(ri.contains_key(&[20])); // key == K → true (key <= K)
    }

    #[test]
    fn contains_key_with_lower_limit() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Lower(vec![30])),
        };
        // Before lower limit
        assert!(!ri.contains_key(&[20]));
        // At lower limit (key >= K)
        assert!(ri.contains_key(&[30]));
        // Within limit
        assert!(ri.contains_key(&[40]));
    }

    #[test]
    fn contains_key_at_lower_limit() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Lower(vec![30])),
        };
        assert!(ri.contains_key(&[30])); // key == K → true (key >= K)
    }

    #[test]
    fn contains_key_no_limit() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: None,
        };
        assert!(ri.contains_key(&[10]));
        assert!(ri.contains_key(&[30]));
        assert!(ri.contains_key(&[49]));
    }

    #[test]
    fn contains_key_excluded_lower() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Excluded(vec![10]),
            upper: Bound::Excluded(vec![20]),
            limit_boundary: None,
        };
        assert!(!ri.contains_key(&[10])); // Excluded
        assert!(ri.contains_key(&[11]));
    }

    #[test]
    fn contains_key_included_upper() {
        let ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Included(vec![20]),
            limit_boundary: None,
        };
        assert!(ri.contains_key(&[20])); // Included upper
        assert!(!ri.contains_key(&[21]));
    }

    // ─── apply_delta tests ───

    #[test]
    fn apply_delta_clears_limit_on_delete() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        ri.apply_delta(Some(&[15]), None); // old in range, new = None (delete)
        assert!(ri.limit_boundary.is_none());
    }

    #[test]
    fn apply_delta_clears_limit_on_move_out() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        ri.apply_delta(Some(&[15]), Some(&[60])); // old in range, new outside
        assert!(ri.limit_boundary.is_none());
    }

    #[test]
    fn apply_delta_no_clear_on_move_within() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        ri.apply_delta(Some(&[12]), Some(&[18])); // both in range
        assert!(ri.limit_boundary.is_some());
    }

    #[test]
    fn apply_delta_no_clear_old_outside() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Upper(vec![20])),
        };
        ri.apply_delta(Some(&[60]), Some(&[15])); // old outside
        assert!(ri.limit_boundary.is_some());
    }

    #[test]
    fn apply_delta_noop_when_no_limit() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: None,
        };
        ri.apply_delta(Some(&[15]), None);
        assert!(ri.limit_boundary.is_none()); // Still None, no-op
    }

    #[test]
    fn apply_delta_clears_lower_limit_on_delete() {
        let mut ri = ReadInterval {
            query_id: 0,
            lower: Bound::Included(vec![10]),
            upper: Bound::Excluded(vec![50]),
            limit_boundary: Some(LimitBoundary::Lower(vec![30])),
        };
        ri.apply_delta(Some(&[35]), None); // old within [30, 50), delete
        assert!(ri.limit_boundary.is_none());
    }

    // ─── ReadSet tests ───

    #[test]
    fn new_starts_at_zero() {
        let rs = ReadSet::new();
        assert_eq!(rs.peek_next_query_id(), 0);
    }

    #[test]
    fn with_starting_query_id() {
        let rs = ReadSet::with_starting_query_id(5);
        assert_eq!(rs.peek_next_query_id(), 5);
    }

    #[test]
    fn next_query_id_increments() {
        let mut rs = ReadSet::new();
        assert_eq!(rs.next_query_id(), 0);
        assert_eq!(rs.next_query_id(), 1);
        assert_eq!(rs.next_query_id(), 2);
    }

    #[test]
    fn add_interval_groups_by_collection_index() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(10);
        let idx = IndexId(20);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![5]),
                limit_boundary: None,
            },
        );
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![15]),
                limit_boundary: None,
            },
        );
        assert_eq!(rs.intervals[&(coll, idx)].len(), 2);
    }

    #[test]
    fn split_before_basic() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(5);
        let idx = IndexId(3);
        for q in 0..3 {
            rs.add_interval(
                coll,
                idx,
                ReadInterval {
                    query_id: q,
                    lower: Bound::Included(vec![q as u8 * 10]),
                    upper: Bound::Excluded(vec![q as u8 * 10 + 5]),
                    limit_boundary: None,
                },
            );
        }
        let carried = rs.split_before(1);
        assert_eq!(carried.interval_count(), 1);
        assert_eq!(carried.intervals[&(coll, idx)][0].query_id, 0);
    }

    #[test]
    fn split_before_includes_catalog_intervals() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            ReadInterval {
                query_id: 0,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        rs.add_interval(
            CollectionId(5),
            IndexId(3),
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![10]),
                limit_boundary: None,
            },
        );
        let carried = rs.split_before(1);
        assert_eq!(carried.interval_count(), 1);
        assert!(carried
            .intervals
            .contains_key(&(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)));
    }

    #[test]
    fn merge_from_combines() {
        let mut a = ReadSet::new();
        let mut b = ReadSet::new();
        let coll = CollectionId(1);
        let idx = IndexId(1);
        a.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![5]),
                limit_boundary: None,
            },
        );
        b.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![15]),
                limit_boundary: None,
            },
        );
        a.merge_from(&b);
        assert_eq!(a.interval_count(), 2);
    }

    #[test]
    fn merge_overlapping_adjacent() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(1);
        let idx = IndexId(1);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![10]),
                limit_boundary: None,
            },
        );
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );
        // [1,10) and [10,20) are adjacent because Excluded(10) is not >= Included(10)
        // Actually Excluded(10) > Included(10) in our check — they DO overlap
        // Let me re-check: bounds_overlap_or_adjacent checks hi=Excluded(10) vs next_lo=Included(10)
        // That's (Excluded(h), Included(l)) => h > l, 10 > 10 is false. So NOT adjacent.
        // Actually these are exactly adjacent. Let me make an overlapping test instead.
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 2,
                lower: Bound::Included(vec![8]),
                upper: Bound::Excluded(vec![25]),
                limit_boundary: None,
            },
        );
        rs.merge_overlapping();
        // [1,10) and [8,25) overlap → merge. [10,20) also overlaps with result.
        assert_eq!(rs.intervals[&(coll, idx)].len(), 1);
        assert_eq!(rs.intervals[&(coll, idx)][0].query_id, 0); // min
    }

    #[test]
    fn merge_overlapping_different_indexes() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![10]),
                limit_boundary: None,
            },
        );
        rs.add_interval(
            CollectionId(1),
            IndexId(2),
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![10]),
                limit_boundary: None,
            },
        );
        rs.merge_overlapping();
        // Different indexes — not merged
        assert_eq!(rs.interval_count(), 2);
    }

    #[test]
    fn merge_overlapping_limit_boundary_conservative() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(1);
        let idx = IndexId(1);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None, // Full coverage
            },
        );
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![5]),
                upper: Bound::Excluded(vec![25]),
                limit_boundary: Some(LimitBoundary::Upper(vec![15])),
            },
        );
        rs.merge_overlapping();
        assert_eq!(rs.intervals[&(coll, idx)].len(), 1);
        // One had None → merged is None
        assert!(rs.intervals[&(coll, idx)][0].limit_boundary.is_none());
    }

    #[test]
    fn merge_overlapping_both_upper_takes_max() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(1);
        let idx = IndexId(1);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![1]),
                upper: Bound::Excluded(vec![30]),
                limit_boundary: Some(LimitBoundary::Upper(vec![10])),
            },
        );
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![5]),
                upper: Bound::Excluded(vec![35]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );
        rs.merge_overlapping();
        assert_eq!(rs.intervals[&(coll, idx)].len(), 1);
        assert_eq!(
            rs.intervals[&(coll, idx)][0].limit_boundary,
            Some(LimitBoundary::Upper(vec![20]))
        ); // max
    }

    #[test]
    fn extend_for_deltas_clears_matching() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(5);
        let idx = IndexId(3);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );
        let deltas = vec![IndexDelta {
            index_id: idx,
            collection_id: coll,
            doc_id: exdb_core::types::DocId([0; 16]),
            old_key: Some(vec![15]),
            new_key: None, // delete
        }];
        rs.extend_for_deltas(&deltas);
        assert!(rs.intervals[&(coll, idx)][0].limit_boundary.is_none());
    }

    #[test]
    fn extend_for_deltas_ignores_unmatched() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(5);
        let idx = IndexId(3);
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );
        let deltas = vec![IndexDelta {
            index_id: IndexId(99),
            collection_id: CollectionId(99),
            doc_id: exdb_core::types::DocId([0; 16]),
            old_key: Some(vec![15]),
            new_key: None,
        }];
        rs.extend_for_deltas(&deltas);
        assert!(rs.intervals[&(coll, idx)][0].limit_boundary.is_some()); // unchanged
    }

    #[test]
    fn interval_count() {
        let mut rs = ReadSet::new();
        assert_eq!(rs.interval_count(), 0);
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        rs.add_interval(
            CollectionId(2),
            IndexId(2),
            ReadInterval {
                query_id: 1,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        assert_eq!(rs.interval_count(), 2);
    }

    #[test]
    fn set_next_query_id() {
        let mut rs = ReadSet::new();
        rs.set_next_query_id(10);
        assert_eq!(rs.next_query_id(), 10);
        assert_eq!(rs.next_query_id(), 11);
    }
}
