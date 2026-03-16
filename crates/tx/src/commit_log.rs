//! T4: In-memory commit log for OCC validation and subscription invalidation.
//!
//! Tracks recent commits indexed by `(collection, index)` for fast overlap
//! checks against a transaction's [`ReadSet`]. Entries are ordered by
//! `commit_ts` (ascending) and pruned as old transactions complete.

use std::collections::BTreeMap;

use exdb_core::types::{CollectionId, DocId, IndexId, Ts};

/// Sentinel [`IndexId`] for the primary index.
///
/// Get-by-id records a point interval on this sentinel so that OCC and
/// subscriptions can detect conflicts on specific document IDs.
pub const PRIMARY_INDEX_SENTINEL: IndexId = IndexId(0);

/// A single key write recorded in the commit log.
///
/// Stores both old and new keys so that OCC can detect:
/// - **Modifications** to documents within the read set (`old_key` overlap)
/// - **Phantoms** entering the read set (`new_key` overlap)
pub struct IndexKeyWrite {
    /// Which document was mutated.
    pub doc_id: DocId,
    /// Encoded key that was removed. `None` for inserts.
    pub old_key: Option<Vec<u8>>,
    /// Encoded key that was added. `None` for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// Entry in the commit log tracking what a single commit wrote.
///
/// Catalog DDL operations are represented as index key writes on the reserved
/// [`CATALOG_COLLECTIONS`](crate::read_set::CATALOG_COLLECTIONS) /
/// [`CATALOG_INDEXES`](crate::read_set::CATALOG_INDEXES) pseudo-collections.
pub struct CommitLogEntry {
    /// Timestamp of this commit.
    pub commit_ts: Ts,
    /// Index key writes grouped by `(collection, index)`.
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
}

/// In-memory commit log for OCC validation and subscription invalidation.
///
/// Entries are ordered by `commit_ts` (ascending). Supports efficient range
/// queries for the OCC danger window `(begin_ts, commit_ts)` via binary search.
pub struct CommitLog {
    entries: Vec<CommitLogEntry>,
}

impl CommitLog {
    /// Create an empty commit log.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Append a new commit entry.
    ///
    /// Must be called with monotonically increasing `commit_ts` values. This is
    /// naturally enforced by the single-writer commit coordinator.
    pub fn append(&mut self, entry: CommitLogEntry) {
        debug_assert!(
            self.entries
                .last()
                .is_none_or(|e| e.commit_ts < entry.commit_ts),
            "commit log entries must be monotonically ordered"
        );
        self.entries.push(entry);
    }

    /// Get all entries in the open interval `(begin_ts, commit_ts)`.
    ///
    /// These are the concurrent commits that could conflict with a transaction
    /// that started at `begin_ts` and is committing at `commit_ts`.
    ///
    /// Both endpoints are **exclusive**:
    /// - `begin_ts`: the transaction already saw this snapshot
    /// - `commit_ts`: this is our own commit, not a concurrent one
    pub fn entries_in_range(&self, begin_ts: Ts, commit_ts: Ts) -> &[CommitLogEntry] {
        let start = self.entries.partition_point(|e| e.commit_ts <= begin_ts);
        let end = self.entries.partition_point(|e| e.commit_ts < commit_ts);
        &self.entries[start..end]
    }

    /// Prune entries no longer needed for OCC validation.
    ///
    /// Entries with `commit_ts <= oldest_active_begin_ts` can never be in any
    /// active transaction's danger window, so they are safe to remove.
    pub fn prune(&mut self, oldest_active_begin_ts: Ts) {
        let cutoff = self
            .entries
            .partition_point(|e| e.commit_ts <= oldest_active_begin_ts);
        self.entries.drain(..cutoff);
    }

    /// Remove a specific entry by `commit_ts`.
    ///
    /// Returns the removed entry, or `None` if not found.
    pub fn remove(&mut self, commit_ts: Ts) -> Option<CommitLogEntry> {
        let pos = self.entries.iter().position(|e| e.commit_ts == commit_ts)?;
        Some(self.entries.remove(pos))
    }

    /// Remove all entries with `commit_ts > threshold`.
    ///
    /// Used for rollback on replication failure: removes all commit log entries
    /// beyond `visible_ts`. This is critical because the writer may have
    /// validated subsequent commits against now-invalid entries.
    pub fn remove_after(&mut self, threshold: Ts) {
        let cutoff = self
            .entries
            .partition_point(|e| e.commit_ts <= threshold);
        self.entries.truncate(cutoff);
    }

    /// Number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for CommitLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a simple commit log entry.
    fn make_entry(ts: Ts) -> CommitLogEntry {
        CommitLogEntry {
            commit_ts: ts,
            index_writes: BTreeMap::new(),
        }
    }

    /// Helper to create an entry with a single write.
    fn make_entry_with_write(
        ts: Ts,
        coll: CollectionId,
        idx: IndexId,
        doc_id: DocId,
        old_key: Option<Vec<u8>>,
        new_key: Option<Vec<u8>>,
    ) -> CommitLogEntry {
        let mut index_writes = BTreeMap::new();
        index_writes
            .entry((coll, idx))
            .or_insert_with(Vec::new)
            .push(IndexKeyWrite {
                doc_id,
                old_key,
                new_key,
            });
        CommitLogEntry {
            commit_ts: ts,
            index_writes,
        }
    }

    #[test]
    fn new_is_empty() {
        let cl = CommitLog::new();
        assert!(cl.is_empty());
        assert_eq!(cl.len(), 0);
    }

    #[test]
    fn append_single() {
        let mut cl = CommitLog::new();
        cl.append(make_entry(1));
        assert_eq!(cl.len(), 1);
        assert!(!cl.is_empty());
    }

    #[test]
    fn entries_in_range_basic() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        let result = cl.entries_in_range(1, 4);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].commit_ts, 2);
        assert_eq!(result[1].commit_ts, 3);
    }

    #[test]
    fn entries_in_range_empty() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        let result = cl.entries_in_range(5, 6);
        assert!(result.is_empty());
    }

    #[test]
    fn entries_in_range_begin_exclusive() {
        let mut cl = CommitLog::new();
        cl.append(make_entry(5));
        cl.append(make_entry(6));
        let result = cl.entries_in_range(5, 7);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].commit_ts, 6);
    }

    #[test]
    fn entries_in_range_commit_exclusive() {
        let mut cl = CommitLog::new();
        cl.append(make_entry(5));
        cl.append(make_entry(6));
        let result = cl.entries_in_range(4, 6);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].commit_ts, 5);
    }

    #[test]
    fn entries_in_range_all() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        let result = cl.entries_in_range(0, 100);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn prune_removes_old() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        cl.prune(2);
        assert_eq!(cl.len(), 3);
        let result = cl.entries_in_range(0, 100);
        assert_eq!(result[0].commit_ts, 3);
    }

    #[test]
    fn prune_empty_log() {
        let mut cl = CommitLog::new();
        cl.prune(100); // No-op on empty
        assert!(cl.is_empty());
    }

    #[test]
    fn remove_existing() {
        let mut cl = CommitLog::new();
        for ts in 1..=3 {
            cl.append(make_entry(ts));
        }
        let removed = cl.remove(2);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().commit_ts, 2);
        assert_eq!(cl.len(), 2);
    }

    #[test]
    fn remove_nonexistent() {
        let mut cl = CommitLog::new();
        cl.append(make_entry(1));
        assert!(cl.remove(99).is_none());
        assert_eq!(cl.len(), 1);
    }

    #[test]
    fn remove_after_basic() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        cl.remove_after(3);
        assert_eq!(cl.len(), 3);
        let result = cl.entries_in_range(0, 100);
        assert_eq!(result.last().unwrap().commit_ts, 3);
    }

    #[test]
    fn remove_after_none() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        cl.remove_after(10);
        assert_eq!(cl.len(), 5);
    }

    #[test]
    fn remove_after_all() {
        let mut cl = CommitLog::new();
        for ts in 1..=5 {
            cl.append(make_entry(ts));
        }
        cl.remove_after(0);
        assert!(cl.is_empty());
    }

    #[test]
    fn entries_with_writes() {
        let mut cl = CommitLog::new();
        cl.append(make_entry_with_write(
            1,
            CollectionId(1),
            IndexId(1),
            DocId([1; 16]),
            None,
            Some(vec![42]),
        ));
        let entries = cl.entries_in_range(0, 2);
        assert_eq!(entries.len(), 1);
        let writes = &entries[0].index_writes[&(CollectionId(1), IndexId(1))];
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].new_key, Some(vec![42]));
    }
}
