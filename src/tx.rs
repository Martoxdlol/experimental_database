//! Transaction context: read sets, write sets, and OCC conflict detection.

use std::collections::HashMap;
use parking_lot::RwLock;

use crate::types::{CollectionId, DocId, IndexId, KeyRange, Ts};

// ── Read set ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PointRead {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub observed_ts: Option<Ts>, // None if document was not found
}

#[derive(Clone, Debug)]
pub struct IndexRangeRead {
    pub collection_id: CollectionId,
    pub index_id: IndexId,
    pub range: KeyRange,
    pub start_ts: Ts,
}

#[derive(Clone, Debug)]
pub struct CollectionScanRead {
    pub collection_id: CollectionId,
    pub start_ts: Ts,
}

#[derive(Clone, Debug, Default)]
pub struct ReadSet {
    pub point_reads: Vec<PointRead>,
    pub index_range_reads: Vec<IndexRangeRead>,
    pub collection_scans: Vec<CollectionScanRead>,
}

impl ReadSet {
    pub fn add_point_read(&mut self, collection_id: CollectionId, doc_id: DocId, observed_ts: Option<Ts>) {
        self.point_reads.push(PointRead { collection_id, doc_id, observed_ts });
    }

    pub fn add_collection_scan(&mut self, collection_id: CollectionId, start_ts: Ts) {
        // Deduplicate: only add if not already tracked
        if !self.collection_scans.iter().any(|s| s.collection_id == collection_id) {
            self.collection_scans.push(CollectionScanRead { collection_id, start_ts });
        }
    }

    pub fn add_index_range_read(
        &mut self,
        collection_id: CollectionId,
        index_id: IndexId,
        range: KeyRange,
        start_ts: Ts,
    ) {
        self.index_range_reads.push(IndexRangeRead { collection_id, index_id, range, start_ts });
    }
}

// ── Write set ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum DocWrite {
    Insert { json: Vec<u8> },
    Update { json: Vec<u8> }, // full new document after patch
    Delete,
}

#[derive(Clone, Debug, Default)]
pub struct WriteSet {
    pub docs: HashMap<(CollectionId, DocId), DocWrite>,
}

impl WriteSet {
    pub fn put(&mut self, collection_id: CollectionId, doc_id: DocId, write: DocWrite) {
        self.docs.insert((collection_id, doc_id), write);
    }

    pub fn get(&self, collection_id: CollectionId, doc_id: &DocId) -> Option<&DocWrite> {
        self.docs.get(&(collection_id, *doc_id))
    }

    pub fn iter_writes(&self) -> impl Iterator<Item = (&(CollectionId, DocId), &DocWrite)> {
        self.docs.iter()
    }
}

// ── WriteSummary (for conflict detection and subscription invalidation) ────────

#[derive(Clone, Debug)]
pub struct WriteSummary {
    pub commit_ts: Ts,
    pub doc_writes: Vec<(CollectionId, DocId)>,
}

// ── Committed transaction log ─────────────────────────────────────────────────

/// A log of recently committed transactions for OCC conflict detection.
pub struct CommitLog {
    entries: RwLock<Vec<WriteSummary>>,
}

impl CommitLog {
    pub fn new() -> Self {
        CommitLog { entries: RwLock::new(Vec::new()) }
    }

    pub fn append(&self, summary: WriteSummary) {
        let mut entries = self.entries.write();
        entries.push(summary);
        // Simple GC: keep only last 10000 entries
        // In production, use a proper sliding window or reference counting
        if entries.len() > 10_000 {
            let drain_to = entries.len() - 5_000;
            entries.drain(..drain_to);
        }
    }

    /// Get all summaries with commit_ts in (start_ts, end_ts].
    pub fn get_range(&self, start_ts: Ts, end_ts: Ts) -> Vec<WriteSummary> {
        let entries = self.entries.read();
        entries
            .iter()
            .filter(|e| e.commit_ts > start_ts && e.commit_ts <= end_ts)
            .cloned()
            .collect()
    }

    /// Check if a specific (collection_id, doc_id) pair was written in (start_ts, end_ts].
    pub fn was_written(
        &self,
        collection_id: CollectionId,
        doc_id: &DocId,
        start_ts: Ts,
        end_ts: Ts,
    ) -> bool {
        let entries = self.entries.read();
        entries.iter().any(|e| {
            e.commit_ts > start_ts
                && e.commit_ts <= end_ts
                && e.doc_writes.iter().any(|(cid, did)| *cid == collection_id && did == doc_id)
        })
    }

    /// Check if any document in a collection was written in (start_ts, end_ts].
    pub fn collection_had_write(&self, collection_id: CollectionId, start_ts: Ts, end_ts: Ts) -> bool {
        let entries = self.entries.read();
        entries.iter().any(|e| {
            e.commit_ts > start_ts
                && e.commit_ts <= end_ts
                && e.doc_writes.iter().any(|(cid, _)| *cid == collection_id)
        })
    }
}

// ── Conflict validation ───────────────────────────────────────────────────────

/// Validate a transaction's read set against the commit log.
/// Returns true if there's a conflict (transaction should be aborted).
pub fn has_conflict(
    read_set: &ReadSet,
    start_ts: Ts,
    commit_ts: Ts,
    commit_log: &CommitLog,
) -> bool {
    // Check point reads
    for pr in &read_set.point_reads {
        let was_written = commit_log.was_written(pr.collection_id, &pr.doc_id, start_ts, commit_ts);
        if was_written {
            // If we observed the doc (it existed), check if it changed
            // If we didn't observe it, check if it was created
            return true; // Conservative: any write to a read doc is a conflict
        }
    }

    // Check collection scans (very conservative: any write to the collection)
    for scan in &read_set.collection_scans {
        if commit_log.collection_had_write(scan.collection_id, start_ts, commit_ts) {
            return true;
        }
    }

    // Check index range reads (conservative: any write to the collection)
    for ir in &read_set.index_range_reads {
        if commit_log.collection_had_write(ir.collection_id, start_ts, commit_ts) {
            return true;
        }
    }

    false
}
