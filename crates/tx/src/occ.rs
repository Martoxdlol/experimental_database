//! T5: Optimistic Concurrency Control (OCC) validation.
//!
//! Validates a transaction's [`ReadSet`] against concurrent commits in the
//! [`CommitLog`]. If any key written by a concurrent commit falls within the
//! transaction's effective read intervals, the transaction must abort.
//!
//! Catalog conflicts (DDL) are handled uniformly through the same interval-based
//! mechanism — catalog reads/writes appear as intervals on reserved
//! pseudo-collections. No separate catalog conflict detection is needed.
//!
//! **Critical correctness property**: uses [`ReadInterval::contains_key`] which
//! respects the effective interval (original bounds + [`LimitBoundary`]
//! tightening). Writes beyond the LIMIT cutoff do **not** cause conflicts.
//!
//! **Does NOT call `extend_for_deltas`**: OCC validates the raw read set.
//! `extend_for_deltas` is only called at commit step 10a before subscription
//! registration.

use std::collections::BTreeSet;

use exdb_core::types::{CollectionId, IndexId, Ts};

use crate::commit_log::CommitLog;
use crate::read_set::{QueryId, ReadSet};

/// The kind of conflict detected.
#[derive(Debug, Clone)]
pub enum ConflictKind {
    /// Interval overlap on an index (data or catalog).
    ///
    /// For catalog conflicts, `collection_id` will be
    /// [`CATALOG_COLLECTIONS`](crate::read_set::CATALOG_COLLECTIONS) or
    /// [`CATALOG_INDEXES`](crate::read_set::CATALOG_INDEXES).
    IndexInterval {
        /// Collection where the conflict occurred.
        collection_id: CollectionId,
        /// Index where the conflict occurred.
        index_id: IndexId,
    },
}

/// OCC validation failure.
#[derive(Debug, Clone, thiserror::Error)]
#[error("OCC conflict at ts {conflicting_ts}: {kind:?}")]
pub struct ConflictError {
    /// The `commit_ts` of the first conflicting concurrent commit.
    pub conflicting_ts: Ts,
    /// What kind of conflict.
    pub kind: ConflictKind,
    /// Query IDs whose intervals were overlapped (sorted ascending).
    ///
    /// Used by subscription notifications to identify which queries need
    /// re-execution.
    pub affected_query_ids: Vec<QueryId>,
}

/// Validate a transaction's read set against the commit log.
///
/// Checks all commits in the danger window `(begin_ts, commit_ts)` for interval
/// overlaps. Returns `Ok(())` if no conflicts, or `Err(ConflictError)` with the
/// **first** conflicting commit's details.
///
/// On the first conflicting commit, collects **all** affected query IDs from
/// that commit (not just the first overlap), deduplicates and sorts ascending.
pub fn validate(
    read_set: &ReadSet,
    commit_log: &CommitLog,
    begin_ts: Ts,
    commit_ts: Ts,
) -> Result<(), ConflictError> {
    for entry in commit_log.entries_in_range(begin_ts, commit_ts) {
        let mut affected_query_ids: BTreeSet<QueryId> = BTreeSet::new();
        let mut conflict_kind: Option<ConflictKind> = None;

        for (&(coll_id, idx_id), key_writes) in &entry.index_writes {
            if let Some(intervals) = read_set.intervals.get(&(coll_id, idx_id)) {
                for write in key_writes {
                    for interval in intervals {
                        let hit = write
                            .old_key
                            .as_ref()
                            .is_some_and(|k| interval.contains_key(k))
                            || write
                                .new_key
                                .as_ref()
                                .is_some_and(|k| interval.contains_key(k));
                        if hit {
                            affected_query_ids.insert(interval.query_id);
                            conflict_kind.get_or_insert(ConflictKind::IndexInterval {
                                collection_id: coll_id,
                                index_id: idx_id,
                            });
                        }
                    }
                }
            }
        }

        if !affected_query_ids.is_empty() {
            return Err(ConflictError {
                conflicting_ts: entry.commit_ts,
                kind: conflict_kind.unwrap(),
                affected_query_ids: affected_query_ids.into_iter().collect(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_log::{CommitLogEntry, IndexKeyWrite};
    use crate::read_set::{
        LimitBoundary, ReadInterval, CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX,
        CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX,
    };
    use std::ops::Bound;

    /// Build a commit log entry with one write on `(coll, idx)`.
    fn entry_with_write(
        ts: Ts,
        coll: CollectionId,
        idx: IndexId,
        old_key: Option<Vec<u8>>,
        new_key: Option<Vec<u8>>,
    ) -> CommitLogEntry {
        let mut index_writes = BTreeMap::new();
        index_writes
            .entry((coll, idx))
            .or_insert_with(Vec::new)
            .push(IndexKeyWrite {
                doc_id: exdb_core::types::DocId([0; 16]),
                old_key,
                new_key,
            });
        CommitLogEntry {
            commit_ts: ts,
            index_writes,
        }
    }

    use std::collections::BTreeMap;

    // ─── Basic conflict detection ───

    #[test]
    fn no_conflict_disjoint() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![25]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn conflict_old_key_in_interval() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            Some(vec![15]),
            None,
        ));

        let err = validate(&rs, &cl, 1, 10).unwrap_err();
        assert_eq!(err.conflicting_ts, 5);
        assert_eq!(err.affected_query_ids, vec![0]);
    }

    #[test]
    fn conflict_new_key_in_interval() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![12]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn both_keys_in_interval() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            Some(vec![11]),
            Some(vec![15]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn old_key_in_new_key_out() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            Some(vec![15]),
            Some(vec![25]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    // ─── LimitBoundary interaction ───

    #[test]
    fn no_conflict_beyond_upper_limit() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![30]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn conflict_within_upper_limit() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![15]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn no_conflict_before_lower_limit() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Lower(vec![30])),
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![20]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn conflict_within_lower_limit() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Lower(vec![30])),
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![35]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    // ─── Boundary conditions ───

    #[test]
    fn begin_ts_exclusive() {
        let mut rs = ReadSet::new();
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

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![1]),
        ));

        // begin_ts=5, commit_ts=10 → entry at 5 excluded
        assert!(validate(&rs, &cl, 5, 10).is_ok());
    }

    #[test]
    fn commit_ts_exclusive() {
        let mut rs = ReadSet::new();
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

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            10,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![1]),
        ));

        // begin_ts=1, commit_ts=10 → entry at 10 excluded
        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn empty_read_set() {
        let rs = ReadSet::new();
        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![1]),
        ));
        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn empty_commit_log() {
        let mut rs = ReadSet::new();
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
        let cl = CommitLog::new();
        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn empty_write_set_still_validates() {
        // Read-only tx with intervals still checks
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![15]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    // ─── Catalog conflicts (unified) ───

    #[test]
    fn catalog_name_point_read_vs_create() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(b"users".to_vec()),
                upper: Bound::Excluded(b"users\x01".to_vec()),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            None,
            Some(b"users".to_vec()),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn catalog_name_point_read_no_conflict() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(b"users".to_vec()),
                upper: Bound::Excluded(b"users\x01".to_vec()),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            None,
            Some(b"orders".to_vec()),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn catalog_full_scan_vs_any_create() {
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

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            None,
            Some(b"anything".to_vec()),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn catalog_index_name_vs_create() {
        let mut rs = ReadSet::new();
        rs.add_interval(
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(b"email".to_vec()),
                upper: Bound::Excluded(b"email\x01".to_vec()),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            5,
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            None,
            Some(b"email".to_vec()),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_err());
    }

    #[test]
    fn catalog_index_list_vs_create_other_collection() {
        // Range for coll_id=10 prefix
        let mut rs = ReadSet::new();
        rs.add_interval(
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![11]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        // Write for coll_id=20
        cl.append(entry_with_write(
            5,
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            None,
            Some(vec![20]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    // ─── Multiple conflicts ───

    #[test]
    fn affected_query_ids_collected() {
        let mut rs = ReadSet::new();
        let coll = CollectionId(1);
        let idx = IndexId(1);
        // Q1 covers [10, 20)
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
        // Q3 covers [15, 25)
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 3,
                lower: Bound::Included(vec![15]),
                upper: Bound::Excluded(vec![25]),
                limit_boundary: None,
            },
        );

        let mut cl = CommitLog::new();
        // Write at 17, overlaps both Q1 and Q3
        cl.append(entry_with_write(5, coll, idx, None, Some(vec![17])));

        let err = validate(&rs, &cl, 1, 10).unwrap_err();
        assert_eq!(err.affected_query_ids, vec![1, 3]);
    }

    #[test]
    fn first_conflicting_commit_returned() {
        let mut rs = ReadSet::new();
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

        let mut cl = CommitLog::new();
        cl.append(entry_with_write(
            3,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![1]),
        ));
        cl.append(entry_with_write(
            7,
            CollectionId(1),
            IndexId(1),
            None,
            Some(vec![2]),
        ));

        let err = validate(&rs, &cl, 1, 10).unwrap_err();
        assert_eq!(err.conflicting_ts, 3); // First conflict
    }

    #[test]
    fn interleaved_reads_writes_with_limits() {
        // The K2/K3/K4 correctness example from the plan
        let coll = CollectionId(1);
        let idx = IndexId(1);

        let mut rs = ReadSet::new();
        // Q1: [K0, K99] limit=1 → K2 (Upper(K2))
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 1,
                lower: Bound::Included(vec![0]),
                upper: Bound::Excluded(vec![99]),
                limit_boundary: Some(LimitBoundary::Upper(vec![2])),
            },
        );
        // Q2: [K0, K99] limit=1 → K4 (Upper(K4)) — after deleting K2
        rs.add_interval(
            coll,
            idx,
            ReadInterval {
                query_id: 2,
                lower: Bound::Included(vec![0]),
                upper: Bound::Excluded(vec![99]),
                limit_boundary: Some(LimitBoundary::Upper(vec![4])),
            },
        );

        let mut cl = CommitLog::new();
        // TX2 inserts K3
        cl.append(entry_with_write(5, coll, idx, None, Some(vec![3])));

        let result = validate(&rs, &cl, 1, 10);
        // K3 does NOT conflict with Q1's Upper(K2) since 3 > 2
        // But K3 DOES conflict with Q2's Upper(K4) since 3 <= 4
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.affected_query_ids, vec![2]); // Only Q2 affected
    }

    #[test]
    fn no_conflict_different_collection() {
        let mut rs = ReadSet::new();
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

        let mut cl = CommitLog::new();
        // Write to different collection
        cl.append(entry_with_write(
            5,
            CollectionId(2),
            IndexId(1),
            None,
            Some(vec![1]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }

    #[test]
    fn no_conflict_different_index() {
        let mut rs = ReadSet::new();
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

        let mut cl = CommitLog::new();
        // Write to different index
        cl.append(entry_with_write(
            5,
            CollectionId(1),
            IndexId(2),
            None,
            Some(vec![1]),
        ));

        assert!(validate(&rs, &cl, 1, 10).is_ok());
    }
}
