use std::collections::BTreeMap;
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

/// Records a catalog observation made during the transaction.
/// Used for OCC conflict detection against concurrent DDL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogRead {
    /// Transaction resolved a collection name → CollectionId.
    CollectionByName(String),
    /// Transaction listed all collections.
    ListCollections,
    /// Transaction resolved an index by name within a collection.
    IndexByName(CollectionId, String),
    /// Transaction listed indexes for a collection.
    ListIndexes(CollectionId),
}

/// All intervals for a transaction, grouped by (collection, index).
#[derive(Clone, Debug, Default)]
pub struct ReadSet {
    /// Read intervals grouped by (collection_id, index_id).
    /// Within each group, intervals are sorted by `lower` and non-overlapping.
    pub intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>,
    /// Catalog observations.
    pub catalog_reads: Vec<CatalogRead>,
    /// Next query_id to assign.
    next_query_id: QueryId,
}

impl ReadSet {
    /// Create an empty read set. Query IDs start at 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a read set with a starting query_id offset.
    /// Used for carry-forward: the new transaction's query_ids
    /// continue from where the carried intervals left off.
    pub fn with_starting_query_id(first_query_id: QueryId) -> Self {
        Self {
            next_query_id: first_query_id,
            ..Default::default()
        }
    }

    /// Allocate the next query_id. Called by L6 before executing each query.
    pub fn next_query_id(&mut self) -> QueryId {
        let id = self.next_query_id;
        self.next_query_id += 1;
        id
    }

    /// Return the current next_query_id value without advancing.
    pub fn peek_next_query_id(&self) -> QueryId {
        self.next_query_id
    }

    /// Explicitly set the next_query_id counter.
    /// Used by begin_chain_continuation (L6) after cloning a carried read set.
    pub fn set_next_query_id(&mut self, id: QueryId) {
        self.next_query_id = id;
    }

    /// Add a read interval for a (collection, index) pair.
    /// The interval is inserted in sorted position and adjacent/overlapping
    /// intervals are merged immediately.
    pub fn add_interval(
        &mut self,
        collection_id: CollectionId,
        index_id: IndexId,
        interval: ReadInterval,
    ) {
        let group = self.intervals.entry((collection_id, index_id)).or_default();
        // Insert in sorted position by lower bound
        let pos = group.partition_point(|existing| existing.lower <= interval.lower);
        group.insert(pos, interval);
        // Merge overlapping/adjacent intervals within this group
        merge_group(group);
    }

    /// Add a catalog read observation with deduplication/subsumption.
    pub fn add_catalog_read(&mut self, read: CatalogRead) {
        match &read {
            CatalogRead::ListCollections => {
                // Subsumes all CollectionByName reads
                self.catalog_reads
                    .retain(|r| !matches!(r, CatalogRead::CollectionByName(_)));
                if !self.catalog_reads.contains(&CatalogRead::ListCollections) {
                    self.catalog_reads.push(CatalogRead::ListCollections);
                }
            }
            CatalogRead::CollectionByName(_) => {
                // Subsumed by ListCollections
                if !self.catalog_reads.contains(&CatalogRead::ListCollections)
                    && !self.catalog_reads.contains(&read)
                {
                    self.catalog_reads.push(read);
                }
            }
            CatalogRead::ListIndexes(coll) => {
                let coll = *coll;
                // Subsumes all IndexByName reads for same collection
                self.catalog_reads.retain(|r| {
                    !matches!(r, CatalogRead::IndexByName(c, _) if *c == coll)
                });
                let already = self.catalog_reads.contains(&CatalogRead::ListIndexes(coll));
                if !already {
                    self.catalog_reads.push(CatalogRead::ListIndexes(coll));
                }
            }
            CatalogRead::IndexByName(coll, _) => {
                let coll = *coll;
                // Subsumed by ListIndexes(coll)
                if !self.catalog_reads.contains(&CatalogRead::ListIndexes(coll))
                    && !self.catalog_reads.contains(&read)
                {
                    self.catalog_reads.push(read);
                }
            }
        }
    }

    /// Merge overlapping or adjacent intervals within each group.
    /// Called automatically by add_interval.
    pub fn merge_overlapping(&mut self) {
        for intervals in self.intervals.values_mut() {
            merge_group(intervals);
        }
    }

    /// Total number of intervals across all groups.
    pub fn interval_count(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }

    /// Check if the read set is empty (no intervals, no catalog reads).
    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty() && self.catalog_reads.is_empty()
    }

    // ─── Carry-Forward ───

    /// Split the read set at a query_id threshold.
    /// Returns a new ReadSet containing only intervals with `query_id < threshold`.
    ///
    /// Catalog reads are carried only when threshold > 0. When threshold = 0 (a
    /// catalog mutation caused the invalidation), catalog reads are NOT carried —
    /// the schema may have changed, so the new transaction must re-read from scratch.
    pub fn split_before(&self, threshold: QueryId) -> ReadSet {
        let mut result = ReadSet::with_starting_query_id(0);
        for (key, intervals) in &self.intervals {
            let kept: Vec<_> = intervals
                .iter()
                .filter(|i| i.query_id < threshold)
                .cloned()
                .collect();
            if !kept.is_empty() {
                result.intervals.insert(*key, kept);
            }
        }
        if threshold > 0 {
            result.catalog_reads = self.catalog_reads.clone();
        }
        result
    }

    /// Merge another ReadSet into this one.
    /// Used to combine carried intervals with newly produced intervals
    /// when a chain transaction commits.
    pub fn merge_from(&mut self, other: &ReadSet) {
        for (key, other_intervals) in &other.intervals {
            let group = self.intervals.entry(*key).or_default();
            group.extend(other_intervals.iter().cloned());
            group.sort_by(|a, b| a.lower.cmp(&b.lower));
            merge_group(group);
        }
        for read in &other.catalog_reads {
            self.add_catalog_read(read.clone());
        }
        self.next_query_id = self.next_query_id.max(other.next_query_id);
    }
}

/// Merge overlapping or adjacent intervals in a sorted Vec<ReadInterval>.
/// Precondition: intervals sorted by lower (ascending).
fn merge_group(intervals: &mut Vec<ReadInterval>) {
    if intervals.len() <= 1 {
        return;
    }

    let mut merged: Vec<ReadInterval> = Vec::with_capacity(intervals.len());

    for interval in intervals.drain(..) {
        if let Some(last) = merged.last_mut() {
            if intervals_overlap_or_adjacent(last, &interval) {
                // Merge: take min lower, max upper, min query_id
                // lower is already correct (last.lower <= interval.lower since sorted)
                last.upper = max_bound(last.upper.clone(), interval.upper);
                last.query_id = last.query_id.min(interval.query_id);
                continue;
            }
        }
        merged.push(interval);
    }

    *intervals = merged;
}

/// Check if two sorted intervals overlap or are adjacent.
/// Precondition: a.lower <= b.lower (sorted order).
fn intervals_overlap_or_adjacent(a: &ReadInterval, b: &ReadInterval) -> bool {
    match &a.upper {
        Bound::Unbounded => true,
        Bound::Excluded(upper) => {
            // b starts at lower; overlaps if b.lower < upper (overlap) or b.lower == upper (adjacent)
            b.lower <= *upper
        }
        Bound::Included(_) => unreachable!("ReadInterval only uses Excluded or Unbounded"),
    }
}

/// Return the maximum of two upper bounds.
/// Unbounded > Excluded(x) for all x.
fn max_bound(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (&a, &b) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
        (Bound::Excluded(av), Bound::Excluded(bv)) => {
            if av >= bv {
                a
            } else {
                b
            }
        }
        _ => unreachable!("ReadInterval only uses Excluded or Unbounded"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::types::{CollectionId, IndexId};

    fn cid(n: u64) -> CollectionId { CollectionId(n) }
    fn iid(n: u64) -> IndexId { IndexId(n) }

    fn make_interval(query_id: QueryId, lower: u8, upper: Option<u8>) -> ReadInterval {
        ReadInterval {
            query_id,
            lower: vec![lower],
            upper: match upper {
                Some(u) => Bound::Excluded(vec![u]),
                None => Bound::Unbounded,
            },
        }
    }

    #[test]
    fn t2_add_and_count() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_interval(cid(1), iid(2), make_interval(1, 0, Some(10)));
        rs.add_interval(cid(2), iid(1), make_interval(2, 0, Some(10)));
        assert_eq!(rs.interval_count(), 3);
    }

    #[test]
    fn t2_merge_overlapping() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(3, 5, Some(15)));
        rs.add_interval(cid(1), iid(1), make_interval(1, 10, Some(20)));
        // [5,15) and [10,20) overlap → merged into [5,20) with query_id=min(3,1)=1
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].lower, vec![5]);
        assert_eq!(group[0].upper, Bound::Excluded(vec![20]));
        assert_eq!(group[0].query_id, 1);
    }

    #[test]
    fn t2_merge_adjacent() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_interval(cid(1), iid(1), make_interval(1, 10, Some(20)));
        // [0,10) and [10,20) are adjacent → merged into [0,20)
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].lower, vec![0]);
        assert_eq!(group[0].upper, Bound::Excluded(vec![20]));
    }

    #[test]
    fn t2_no_merge_different_groups() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_interval(cid(1), iid(2), make_interval(1, 5, Some(15)));
        // Different (coll, idx) groups — no merge
        assert_eq!(rs.interval_count(), 2);
    }

    #[test]
    fn t2_query_id_incremental() {
        let mut rs = ReadSet::new();
        assert_eq!(rs.next_query_id(), 0);
        assert_eq!(rs.next_query_id(), 1);
        assert_eq!(rs.next_query_id(), 2);
    }

    #[test]
    fn t2_with_starting_query_id() {
        let mut rs = ReadSet::with_starting_query_id(5);
        assert_eq!(rs.next_query_id(), 5);
        assert_eq!(rs.next_query_id(), 6);
        assert_eq!(rs.next_query_id(), 7);
    }

    #[test]
    fn t2_split_before_basic() {
        let mut rs = ReadSet::new();
        for qid in 0..5u32 {
            rs.add_interval(cid(1), iid(1), ReadInterval {
                query_id: qid,
                lower: vec![qid as u8 * 10],
                upper: Bound::Excluded(vec![qid as u8 * 10 + 5]),
            });
        }
        let split = rs.split_before(3);
        // Should have intervals with query_id 0, 1, 2 only
        let group = &split.intervals[&(CollectionId(1), IndexId(1))];
        assert!(group.iter().all(|i| i.query_id < 3));
        // query_id 3 and 4 should be absent
        assert!(group.iter().all(|i| i.query_id != 3 && i.query_id != 4));
    }

    #[test]
    fn t2_split_before_preserves_groups() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 0, lower: vec![0], upper: Bound::Excluded(vec![5]) });
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 3, lower: vec![10], upper: Bound::Excluded(vec![15]) });
        rs.add_interval(cid(2), iid(1), ReadInterval { query_id: 1, lower: vec![0], upper: Bound::Excluded(vec![5]) });
        rs.add_interval(cid(2), iid(1), ReadInterval { query_id: 4, lower: vec![20], upper: Bound::Excluded(vec![25]) });

        let split = rs.split_before(2);
        // (1,1): only query_id=0 survives
        let g11 = &split.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(g11.len(), 1);
        assert_eq!(g11[0].query_id, 0);
        // (2,1): only query_id=1 survives
        let g21 = &split.intervals[&(CollectionId(2), IndexId(1))];
        assert_eq!(g21.len(), 1);
        assert_eq!(g21[0].query_id, 1);
    }

    #[test]
    fn t2_split_before_empty() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(1, 0, Some(10)));
        rs.add_catalog_read(CatalogRead::ListCollections);
        let split = rs.split_before(0);
        assert!(split.is_empty());
        assert!(split.catalog_reads.is_empty());
    }

    #[test]
    fn t2_split_before_all() {
        let mut rs = ReadSet::new();
        for qid in 0..5u32 {
            rs.add_interval(cid(1), iid(1), ReadInterval {
                query_id: qid,
                lower: vec![qid as u8 * 10],
                upper: Bound::Excluded(vec![qid as u8 * 10 + 5]),
            });
        }
        let split = rs.split_before(100);
        assert_eq!(split.interval_count(), rs.interval_count());
    }

    #[test]
    fn t2_merge_from() {
        let mut rs1 = ReadSet::new();
        rs1.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs1.add_catalog_read(CatalogRead::CollectionByName("users".into()));

        let mut rs2 = ReadSet::with_starting_query_id(1);
        rs2.add_interval(cid(1), iid(1), make_interval(1, 5, Some(15))); // overlaps with rs1
        rs2.add_interval(cid(2), iid(1), make_interval(1, 0, Some(10)));
        rs2.add_catalog_read(CatalogRead::CollectionByName("orders".into()));

        rs1.merge_from(&rs2);

        // (1,1): [0,10) and [5,15) merged → [0,15), query_id=0
        let g = &rs1.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].upper, Bound::Excluded(vec![15]));
        // (2,1) added
        assert!(rs1.intervals.contains_key(&(CollectionId(2), IndexId(1))));
        // Both catalog reads present
        assert_eq!(rs1.catalog_reads.len(), 2);
    }

    #[test]
    fn t2_catalog_reads_carried() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_catalog_read(CatalogRead::ListCollections);
        let split = rs.split_before(2);
        assert!(!split.catalog_reads.is_empty());
        assert!(split.catalog_reads.contains(&CatalogRead::ListCollections));
    }

    #[test]
    fn t2_catalog_reads_not_carried_at_zero() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_catalog_read(CatalogRead::ListCollections);
        let split = rs.split_before(0);
        assert!(split.catalog_reads.is_empty());
        assert!(split.intervals.is_empty());
    }

    #[test]
    fn t2_catalog_read_deduplicate_exact() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        assert_eq!(rs.catalog_reads.len(), 1);
    }

    #[test]
    fn t2_catalog_read_deduplicate_subsumed_by_list() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::ListCollections);
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        assert_eq!(rs.catalog_reads.len(), 1);
        assert!(rs.catalog_reads.contains(&CatalogRead::ListCollections));
    }

    #[test]
    fn t2_catalog_read_deduplicate_retroactive_subsumption() {
        // Add specific read first, then list — list should subsume the specific one
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        rs.add_catalog_read(CatalogRead::ListCollections);
        assert_eq!(rs.catalog_reads.len(), 1);
        assert!(rs.catalog_reads.contains(&CatalogRead::ListCollections));
    }

    #[test]
    fn t2_catalog_read_deduplicate_index_subsumed() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::ListIndexes(CollectionId(1)));
        rs.add_catalog_read(CatalogRead::IndexByName(CollectionId(1), "by_email".into()));
        assert_eq!(rs.catalog_reads.len(), 1);
        assert!(rs.catalog_reads.contains(&CatalogRead::ListIndexes(CollectionId(1))));

        // Different collection — NOT subsumed
        rs.add_catalog_read(CatalogRead::IndexByName(CollectionId(2), "by_email".into()));
        assert_eq!(rs.catalog_reads.len(), 2);
    }

    #[test]
    fn t2_merge_query_id_min() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 3,
            lower: vec![10],
            upper: Bound::Excluded(vec![20]),
        });
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 1,
            lower: vec![15],
            upper: Bound::Excluded(vec![25]),
        });
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].query_id, 1);
    }

    #[test]
    fn t2_unbounded_upper_subsumes() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 0,
            lower: vec![0],
            upper: Bound::Unbounded,
        });
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 1,
            lower: vec![50],
            upper: Bound::Excluded(vec![100]),
        });
        // Unbounded upper should subsume the second interval
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].upper, Bound::Unbounded);
        assert_eq!(group[0].query_id, 0);
    }

    #[test]
    fn t2_is_empty_true() {
        let rs = ReadSet::new();
        assert!(rs.is_empty());
    }

    #[test]
    fn t2_is_empty_false_intervals() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        assert!(!rs.is_empty());
    }

    #[test]
    fn t2_is_empty_false_catalog() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::ListCollections);
        assert!(!rs.is_empty());
    }

    #[test]
    fn t2_peek_next_query_id() {
        let mut rs = ReadSet::new();
        assert_eq!(rs.peek_next_query_id(), 0);
        rs.next_query_id();
        assert_eq!(rs.peek_next_query_id(), 1);
        assert_eq!(rs.peek_next_query_id(), 1); // unchanged
    }

    #[test]
    fn t2_set_next_query_id() {
        let mut rs = ReadSet::new();
        rs.set_next_query_id(42);
        assert_eq!(rs.next_query_id(), 42);
    }

    #[test]
    fn t2_non_overlapping_stays_separate() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), make_interval(0, 0, Some(10)));
        rs.add_interval(cid(1), iid(1), make_interval(1, 20, Some(30)));
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 2);
    }

    #[test]
    fn t2_three_way_merge() {
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 2, lower: vec![20], upper: Bound::Excluded(vec![30]) });
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 0, lower: vec![0], upper: Bound::Excluded(vec![15]) });
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 1, lower: vec![10], upper: Bound::Excluded(vec![25]) });
        // All three overlap: [0,15), [10,25), [20,30) → [0,30), query_id=0
        let group = &rs.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].lower, vec![0]);
        assert_eq!(group[0].upper, Bound::Excluded(vec![30]));
        assert_eq!(group[0].query_id, 0);
    }
}
