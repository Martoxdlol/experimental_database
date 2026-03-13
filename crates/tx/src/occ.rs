use std::ops::Bound;
use exdb_core::types::{CollectionId, IndexId, Ts};
use crate::read_set::{ReadSet, ReadInterval, CatalogRead, QueryId};
use crate::commit_log::CommitLog;
use crate::write_set::CatalogMutation;

/// Result of OCC validation — details about the first conflict found.
#[derive(Debug)]
pub struct ConflictError {
    /// The commit_ts of the conflicting concurrent transaction.
    pub conflicting_ts: Ts,
    /// What kind of conflict was detected.
    pub kind: ConflictKind,
    /// The query_id(s) whose read intervals were overlapped.
    /// Sorted ascending.
    pub affected_query_ids: Vec<QueryId>,
}

#[derive(Debug)]
pub enum ConflictKind {
    /// A concurrent commit wrote keys overlapping this transaction's scanned intervals.
    IndexInterval {
        collection_id: CollectionId,
        index_id: IndexId,
    },
    /// A concurrent commit performed DDL conflicting with this transaction's catalog observations.
    Catalog {
        description: String,
    },
}

/// Validate a transaction's read set against the commit log.
///
/// Returns Ok(()) if no conflicts, Err(ConflictError) on the first detected conflict.
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError> {
    let concurrent = commit_log.entries_in_range(begin_ts, commit_ts);

    for entry in concurrent {
        // Check index interval conflicts
        for ((coll_id, idx_id), key_writes) in &entry.index_writes {
            let intervals = match read_set.intervals.get(&(*coll_id, *idx_id)) {
                Some(v) => v,
                None => continue,
            };

            for kw in key_writes {
                if let Some(old_key) = &kw.old_key {
                    if let Some(qid) = key_overlaps_intervals(old_key, intervals) {
                        return Err(ConflictError {
                            conflicting_ts: entry.commit_ts,
                            kind: ConflictKind::IndexInterval {
                                collection_id: *coll_id,
                                index_id: *idx_id,
                            },
                            affected_query_ids: vec![qid],
                        });
                    }
                }
                if let Some(new_key) = &kw.new_key {
                    if let Some(qid) = key_overlaps_intervals(new_key, intervals) {
                        return Err(ConflictError {
                            conflicting_ts: entry.commit_ts,
                            kind: ConflictKind::IndexInterval {
                                collection_id: *coll_id,
                                index_id: *idx_id,
                            },
                            affected_query_ids: vec![qid],
                        });
                    }
                }
            }
        }

        // Check catalog conflicts
        if !entry.catalog_mutations.is_empty() && !read_set.catalog_reads.is_empty() {
            validate_catalog(&read_set.catalog_reads, &entry.catalog_mutations, entry.commit_ts)?;
        }
    }

    Ok(())
}

/// Check if an encoded key falls within any interval in a sorted group.
/// Returns the query_id of the first overlapping interval, or None.
/// Uses binary search — O(log I) per key.
pub fn key_overlaps_intervals(key: &[u8], intervals: &[ReadInterval]) -> Option<QueryId> {
    if intervals.is_empty() {
        return None;
    }

    // Find the rightmost interval where lower <= key
    let idx = intervals.partition_point(|i| i.lower.as_slice() <= key);

    // idx is the first interval whose lower > key; check idx-1
    if idx == 0 {
        return None; // all intervals start after key
    }

    let candidate = &intervals[idx - 1];
    // key >= candidate.lower; check upper
    let within = match &candidate.upper {
        Bound::Unbounded => true,
        Bound::Excluded(upper) => key < upper.as_slice(),
        Bound::Included(_) => unreachable!("ReadInterval only uses Excluded or Unbounded"),
    };

    if within {
        Some(candidate.query_id)
    } else {
        None
    }
}

/// Check if a set of catalog reads conflicts with a set of catalog mutations.
/// Returns true if any read-write conflict exists.
/// Public so T6 (subscriptions.rs) can use it for subscription invalidation checks.
pub fn catalog_conflicts(
    catalog_reads: &[CatalogRead],
    catalog_mutations: &[CatalogMutation],
) -> bool {
    for read in catalog_reads {
        for mutation in catalog_mutations {
            if catalog_read_conflicts(read, mutation) {
                return true;
            }
        }
    }
    false
}

/// Internal helper: check if a single read/mutation pair conflicts.
fn catalog_read_conflicts(read: &CatalogRead, mutation: &CatalogMutation) -> bool {
    match (read, mutation) {
        (CatalogRead::CollectionByName(name), CatalogMutation::CreateCollection { name: n, .. }) => {
            name == n
        }
        (CatalogRead::CollectionByName(name), CatalogMutation::DropCollection { name: n, .. }) => {
            name == n
        }
        (CatalogRead::ListCollections, CatalogMutation::CreateCollection { .. }) => true,
        (CatalogRead::ListCollections, CatalogMutation::DropCollection { .. }) => true,
        (CatalogRead::IndexByName(coll, name), CatalogMutation::CreateIndex { collection_id, name: n, .. }) => {
            coll == collection_id && name == n
        }
        (CatalogRead::IndexByName(coll, name), CatalogMutation::DropIndex { collection_id, name: n, .. }) => {
            coll == collection_id && name == n
        }
        (CatalogRead::ListIndexes(coll), CatalogMutation::CreateIndex { collection_id, .. }) => {
            coll == collection_id
        }
        (CatalogRead::ListIndexes(coll), CatalogMutation::DropIndex { collection_id, .. }) => {
            coll == collection_id
        }
        _ => false,
    }
}

/// Wrapper that returns ConflictError on the first catalog conflict found.
fn validate_catalog(
    catalog_reads: &[CatalogRead],
    catalog_mutations: &[CatalogMutation],
    conflicting_ts: Ts,
) -> Result<(), ConflictError> {
    for read in catalog_reads {
        for mutation in catalog_mutations {
            if catalog_read_conflicts(read, mutation) {
                let description = format!("catalog read {:?} conflicts with {:?}", read, mutation);
                return Err(ConflictError {
                    conflicting_ts,
                    kind: ConflictKind::Catalog { description },
                    affected_query_ids: vec![0],
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::ops::Bound;
    use exdb_core::types::DocId;
    use crate::commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite};
    use crate::read_set::{ReadInterval, ReadSet, CatalogRead};

    fn make_read_set_with_interval(
        coll_id: u64,
        idx_id: u64,
        query_id: QueryId,
        lower: u8,
        upper: Option<u8>,
    ) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(coll_id), IndexId(idx_id), ReadInterval {
            query_id,
            lower: vec![lower],
            upper: match upper {
                Some(u) => Bound::Excluded(vec![u]),
                None => Bound::Unbounded,
            },
        });
        rs
    }

    fn make_commit_entry(
        commit_ts: Ts,
        coll_id: u64,
        idx_id: u64,
        old_key: Option<u8>,
        new_key: Option<u8>,
    ) -> CommitLogEntry {
        let kw = IndexKeyWrite {
            doc_id: DocId([0u8; 16]),
            old_key: old_key.map(|k| vec![k]),
            new_key: new_key.map(|k| vec![k]),
        };
        let mut index_writes = BTreeMap::new();
        index_writes.insert((CollectionId(coll_id), IndexId(idx_id)), vec![kw]);
        CommitLogEntry { commit_ts, index_writes, catalog_mutations: vec![] }
    }

    fn commit_log_with(entries: Vec<CommitLogEntry>) -> CommitLog {
        let mut log = CommitLog::new();
        for e in entries {
            log.append(e);
        }
        log
    }

    #[test]
    fn t5_no_conflict_disjoint_intervals() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, None, Some(25))]);
        assert!(validate(&rs, &log, 0, 20).is_ok());
    }

    #[test]
    fn t5_conflict_old_key_in_interval() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, Some(15), None)]);
        let result = validate(&rs, &log, 0, 20);
        assert!(result.is_err());
    }

    #[test]
    fn t5_conflict_new_key_in_interval_phantom() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, None, Some(15))]);
        assert!(validate(&rs, &log, 0, 20).is_err());
    }

    #[test]
    fn t5_conflict_returns_query_id() {
        let rs = make_read_set_with_interval(1, 1, 3, 10, Some(20));
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, None, Some(15))]);
        let err = validate(&rs, &log, 0, 20).unwrap_err();
        assert_eq!(err.affected_query_ids, vec![3]);
    }

    #[test]
    fn t5_no_conflict_different_index() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let log = commit_log_with(vec![make_commit_entry(15, 1, 2, None, Some(15))]);
        assert!(validate(&rs, &log, 0, 20).is_ok());
    }

    #[test]
    fn t5_no_conflict_outside_ts_range() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        // Entry at ts=5 is before begin_ts=10 → excluded
        let log = commit_log_with(vec![make_commit_entry(5, 1, 1, None, Some(15))]);
        assert!(validate(&rs, &log, 10, 20).is_ok());
    }

    #[test]
    fn t5_unbounded_upper() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, None); // [10, Unbounded)
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, None, Some(255))]);
        assert!(validate(&rs, &log, 0, 20).is_err());
    }

    #[test]
    fn t5_catalog_conflict_collection_by_name() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        let entry = CommitLogEntry {
            commit_ts: 10,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![CatalogMutation::DropCollection {
                collection_id: CollectionId(1),
                name: "users".into(),
            }],
        };
        let log = commit_log_with(vec![entry]);
        assert!(validate(&rs, &log, 0, 20).is_err());
    }

    #[test]
    fn t5_catalog_conflict_list_collections() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::ListCollections);
        let entry = CommitLogEntry {
            commit_ts: 10,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![CatalogMutation::CreateCollection {
                name: "orders".into(),
                provisional_id: CollectionId(99),
            }],
        };
        let log = commit_log_with(vec![entry]);
        assert!(validate(&rs, &log, 0, 20).is_err());
    }

    #[test]
    fn t5_catalog_no_conflict_different_collection() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        let entry = CommitLogEntry {
            commit_ts: 10,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![CatalogMutation::CreateCollection {
                name: "orders".into(),
                provisional_id: CollectionId(99),
            }],
        };
        let log = commit_log_with(vec![entry]);
        assert!(validate(&rs, &log, 0, 20).is_ok());
    }

    #[test]
    fn t5_catalog_conflict_index_by_name() {
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::IndexByName(CollectionId(1), "by_email".into()));
        let entry = CommitLogEntry {
            commit_ts: 10,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![CatalogMutation::DropIndex {
                collection_id: CollectionId(1),
                index_id: IndexId(10),
                name: "by_email".into(),
            }],
        };
        let log = commit_log_with(vec![entry]);
        assert!(validate(&rs, &log, 0, 20).is_err());
    }

    #[test]
    fn t5_empty_read_set_always_passes() {
        let rs = ReadSet::new();
        let log = commit_log_with(vec![make_commit_entry(15, 1, 1, Some(15), Some(15))]);
        assert!(validate(&rs, &log, 0, 20).is_ok());
    }

    #[test]
    fn t5_multiple_concurrent_commits() {
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        // First entry doesn't conflict (key=25 outside [10,20))
        // Second entry does conflict (key=15)
        let log = commit_log_with(vec![
            make_commit_entry(12, 1, 1, None, Some(25)),
            make_commit_entry(15, 1, 1, None, Some(15)),
        ]);
        let err = validate(&rs, &log, 0, 20).unwrap_err();
        assert_eq!(err.conflicting_ts, 15);
    }

    #[test]
    fn t5_binary_search_correctness() {
        let mut rs = ReadSet::new();
        // Add 100 non-overlapping intervals
        for i in 0..100u32 {
            rs.add_interval(CollectionId(1), IndexId(1), ReadInterval {
                query_id: i,
                lower: vec![(i * 3) as u8],
                upper: Bound::Excluded(vec![(i * 3 + 2) as u8]),
            });
        }

        // A key that falls in interval #73: lower=219, upper=221 → key=220
        let key = vec![220u8];
        let intervals = &rs.intervals[&(CollectionId(1), IndexId(1))];
        let result = key_overlaps_intervals(&key, intervals);
        assert!(result.is_some());
        // query_id should be 73
        assert_eq!(result.unwrap(), 73);
    }

    #[test]
    fn t5_key_at_lower_bound_inclusive() {
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval {
            query_id: 5,
            lower: vec![10],
            upper: Bound::Excluded(vec![20]),
        });
        let intervals = &rs.intervals[&(CollectionId(1), IndexId(1))];
        // Key exactly at lower bound
        assert_eq!(key_overlaps_intervals(&[10], intervals), Some(5));
    }

    #[test]
    fn t5_key_at_upper_bound_exclusive() {
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval {
            query_id: 5,
            lower: vec![10],
            upper: Bound::Excluded(vec![20]),
        });
        let intervals = &rs.intervals[&(CollectionId(1), IndexId(1))];
        // Key exactly at excluded upper bound — should NOT overlap
        assert!(key_overlaps_intervals(&[20], intervals).is_none());
    }

    #[test]
    fn t5_catalog_conflicts_list_indexes() {
        let reads = vec![CatalogRead::ListIndexes(CollectionId(1))];
        let mutations = vec![CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(5),
            name: "any".into(),
        }];
        assert!(catalog_conflicts(&reads, &mutations));
    }

    #[test]
    fn t5_catalog_no_conflict_different_collection_index() {
        let reads = vec![CatalogRead::ListIndexes(CollectionId(1))];
        let mutations = vec![CatalogMutation::DropIndex {
            collection_id: CollectionId(2), // different collection
            index_id: IndexId(5),
            name: "any".into(),
        }];
        assert!(!catalog_conflicts(&reads, &mutations));
    }
}
