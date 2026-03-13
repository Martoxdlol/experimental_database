use std::collections::BTreeMap;
use exdb_core::types::{CollectionId, IndexId, DocId, Ts};
use crate::write_set::CatalogMutation;

/// A single key write within a commit, for one index.
#[derive(Debug, Clone)]
pub struct IndexKeyWrite {
    pub doc_id: DocId,
    /// Old encoded key prefix. None for inserts.
    pub old_key: Option<Vec<u8>>,
    /// New encoded key prefix. None for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// A single commit's footprint in the key space.
#[derive(Debug, Clone)]
pub struct CommitLogEntry {
    /// The commit timestamp.
    pub commit_ts: Ts,
    /// Index key writes grouped by (collection, index).
    pub index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    /// Catalog mutations from this commit (if any).
    pub catalog_mutations: Vec<CatalogMutation>,
}

/// The commit log — ordered sequence of recent commits.
pub struct CommitLog {
    /// Entries ordered by commit_ts (ascending).
    entries: Vec<CommitLogEntry>,
}

impl CommitLog {
    /// Create an empty commit log.
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Append a new commit entry. Must have commit_ts > all existing entries.
    pub fn append(&mut self, entry: CommitLogEntry) {
        debug_assert!(
            self.entries.last().map_or(true, |e| e.commit_ts < entry.commit_ts),
            "commit_ts must be strictly increasing"
        );
        self.entries.push(entry);
    }

    /// Get all entries with commit_ts in the open interval (begin_ts, commit_ts).
    /// Uses binary search — O(log N).
    pub fn entries_in_range(&self, begin_ts: Ts, commit_ts: Ts) -> &[CommitLogEntry] {
        // Find first index where commit_ts > begin_ts
        let start = self.entries.partition_point(|e| e.commit_ts <= begin_ts);
        // Find first index where commit_ts >= commit_ts (exclusive upper)
        let end = self.entries.partition_point(|e| e.commit_ts < commit_ts);
        &self.entries[start..end]
    }

    /// Remove all entries with commit_ts <= threshold.
    pub fn prune(&mut self, threshold: Ts) {
        let idx = self.entries.partition_point(|e| e.commit_ts <= threshold);
        self.entries.drain(..idx);
    }

    /// Number of entries currently in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove a specific entry by commit_ts.
    /// Used for rollback on quorum loss.
    pub fn remove(&mut self, commit_ts: Ts) -> Option<CommitLogEntry> {
        if let Ok(idx) = self.entries.binary_search_by_key(&commit_ts, |e| e.commit_ts) {
            Some(self.entries.remove(idx))
        } else {
            None
        }
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

    fn make_entry(commit_ts: Ts) -> CommitLogEntry {
        CommitLogEntry {
            commit_ts,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![],
        }
    }

    #[test]
    fn t4_append_and_len() {
        let mut log = CommitLog::new();
        log.append(make_entry(10));
        log.append(make_entry(20));
        log.append(make_entry(30));
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn t4_entries_in_range_basic() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30, 40, 50] {
            log.append(make_entry(ts));
        }
        let range = log.entries_in_range(15, 35);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].commit_ts, 20);
        assert_eq!(range[1].commit_ts, 30);
    }

    #[test]
    fn t4_entries_in_range_exclusive_bounds() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30] {
            log.append(make_entry(ts));
        }
        // begin_ts=10, commit_ts=30 → open interval (10, 30) → only ts=20
        let range = log.entries_in_range(10, 30);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].commit_ts, 20);
    }

    #[test]
    fn t4_entries_in_range_empty() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30] {
            log.append(make_entry(ts));
        }
        let range = log.entries_in_range(5, 8);
        assert!(range.is_empty());
    }

    #[test]
    fn t4_entries_in_range_all() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30] {
            log.append(make_entry(ts));
        }
        let range = log.entries_in_range(0, 100);
        assert_eq!(range.len(), 3);
    }

    #[test]
    fn t4_prune() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30, 40] {
            log.append(make_entry(ts));
        }
        log.prune(25);
        assert_eq!(log.len(), 2);
        assert_eq!(log.entries[0].commit_ts, 30);
        assert_eq!(log.entries[1].commit_ts, 40);
    }

    #[test]
    fn t4_prune_none() {
        let mut log = CommitLog::new();
        log.append(make_entry(10));
        log.append(make_entry(20));
        log.prune(5);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn t4_prune_all() {
        let mut log = CommitLog::new();
        log.append(make_entry(10));
        log.append(make_entry(20));
        log.prune(100);
        assert!(log.is_empty());
    }

    #[test]
    fn t4_remove_specific() {
        let mut log = CommitLog::new();
        for ts in [10, 20, 30] {
            log.append(make_entry(ts));
        }
        let removed = log.remove(20);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().commit_ts, 20);
        assert_eq!(log.len(), 2);
        // Non-existent
        assert!(log.remove(99).is_none());
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn t4_entries_in_range_exact_boundary_begin() {
        // begin_ts=20 should exclude the entry at 20
        let mut log = CommitLog::new();
        for ts in [10, 20, 30, 40] {
            log.append(make_entry(ts));
        }
        let range = log.entries_in_range(20, 40);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].commit_ts, 30);
    }

    #[test]
    fn t4_entries_in_range_single_entry() {
        let mut log = CommitLog::new();
        log.append(make_entry(15));
        assert_eq!(log.entries_in_range(10, 20).len(), 1);
        assert_eq!(log.entries_in_range(15, 20).len(), 0); // begin_ts=15 excludes 15
        assert_eq!(log.entries_in_range(10, 15).len(), 0); // commit_ts=15 excludes 15
    }

    #[test]
    fn t4_empty_log_operations() {
        let mut log = CommitLog::new();
        assert!(log.is_empty());
        assert_eq!(log.entries_in_range(0, 100).len(), 0);
        assert!(log.remove(5).is_none());
        log.prune(100); // no panic
    }
}
