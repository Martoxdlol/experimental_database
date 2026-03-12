//! D2: MVCC version resolution.
//!
//! State machine that processes a stream of `(doc_id, ts)` entries from a B-tree
//! scan and determines which entries are visible at a given `read_ts`.
//! Direction-aware: handles both forward and backward scans.

use exdb_core::types::{DocId, Ts};
use exdb_storage::btree::ScanDirection;

/// Result of processing one entry through the resolver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// This entry is the latest visible version at read_ts for its doc_id.
    /// (Forward mode only — in backward mode, visibility is deferred.)
    Visible,
    /// This entry should be skipped.
    Skip,
    /// The PREVIOUS doc_id group's visible version is ready to emit.
    /// The current entry has already been absorbed into the new group.
    /// (Backward mode only.)
    EmitPrevious(DocId, Ts),
}

/// State machine for MVCC version resolution.
///
/// Forward: ascending doc_id, descending ts within group. Emits eagerly.
/// Backward: descending doc_id, ascending ts within group. Buffers and emits on group change.
pub struct VersionResolver {
    read_ts: Ts,
    direction: ScanDirection,
    current_doc_id: Option<DocId>,
    // Forward mode
    found_visible: bool,
    // Backward mode
    best_visible_ts: Option<Ts>,
}

impl VersionResolver {
    /// Create a new resolver pinned to the given read timestamp and direction.
    pub fn new(read_ts: Ts, direction: ScanDirection) -> Self {
        Self {
            read_ts,
            direction,
            current_doc_id: None,
            found_visible: false,
            best_visible_ts: None,
        }
    }

    /// Process the next `(doc_id, ts)` entry from a B-tree scan.
    pub fn process(&mut self, doc_id: &DocId, ts: Ts) -> Verdict {
        match self.direction {
            ScanDirection::Forward => self.process_forward(doc_id, ts),
            ScanDirection::Backward => self.process_backward(doc_id, ts),
        }
    }

    /// Signal end of stream. In backward mode, returns the final group's
    /// visible `(doc_id, ts)` if any. In forward mode, always returns None.
    pub fn finish(&mut self) -> Option<(DocId, Ts)> {
        match self.direction {
            ScanDirection::Forward => None,
            ScanDirection::Backward => {
                match (self.current_doc_id.take(), self.best_visible_ts.take()) {
                    (Some(id), Some(ts)) => Some((id, ts)),
                    _ => None,
                }
            }
        }
    }

    /// Check if a single version timestamp is visible at read_ts.
    pub fn is_visible(ts: Ts, read_ts: Ts) -> bool {
        ts <= read_ts
    }

    fn process_forward(&mut self, doc_id: &DocId, ts: Ts) -> Verdict {
        let same_doc = self.current_doc_id.as_ref() == Some(doc_id);

        if !same_doc {
            self.current_doc_id = Some(*doc_id);
            self.found_visible = false;
        }

        if self.found_visible {
            return Verdict::Skip;
        }

        if ts > self.read_ts {
            return Verdict::Skip;
        }

        self.found_visible = true;
        Verdict::Visible
    }

    fn process_backward(&mut self, doc_id: &DocId, ts: Ts) -> Verdict {
        let same_doc = self.current_doc_id.as_ref() == Some(doc_id);

        if same_doc {
            if ts <= self.read_ts {
                self.best_visible_ts = Some(ts);
            }
            return Verdict::Skip;
        }

        // New group — emit previous group's result if any
        let emit = match (self.current_doc_id.take(), self.best_visible_ts.take()) {
            (Some(prev_id), Some(prev_ts)) => Some(Verdict::EmitPrevious(prev_id, prev_ts)),
            _ => None,
        };

        // Start new group
        self.current_doc_id = Some(*doc_id);
        self.best_visible_ts = if ts <= self.read_ts { Some(ts) } else { None };

        emit.unwrap_or(Verdict::Skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    // ─── Forward Mode ───

    #[test]
    fn forward_single_visible() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 5), Verdict::Visible);
    }

    #[test]
    fn forward_single_invisible() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
    }

    #[test]
    fn forward_multiple_newest_visible() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 10), Verdict::Visible);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip);
    }

    #[test]
    fn forward_multiple_middle_visible() {
        let mut r = VersionResolver::new(12, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 10), Verdict::Visible);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
    }

    #[test]
    fn forward_multiple_oldest_visible() {
        let mut r = VersionResolver::new(7, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 10), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 5), Verdict::Visible);
    }

    #[test]
    fn forward_none_visible() {
        let mut r = VersionResolver::new(5, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 10), Verdict::Skip);
    }

    #[test]
    fn forward_multiple_docs() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 10), Verdict::Visible);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.process(&doc(2), 12), Verdict::Skip);
        assert_eq!(r.process(&doc(2), 8), Verdict::Visible);
    }

    #[test]
    fn forward_exact_ts() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 10), Verdict::Visible);
    }

    #[test]
    fn forward_finish_returns_none() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        r.process(&doc(1), 5);
        assert_eq!(r.finish(), None);
    }

    // ─── Backward Mode ───

    #[test]
    fn backward_single_visible() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 5)));
    }

    #[test]
    fn backward_single_invisible() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.finish(), None);
    }

    #[test]
    fn backward_multiple_ascending_ts() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 10), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 10)));
    }

    #[test]
    fn backward_latest_too_new() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 5)));
    }

    #[test]
    fn backward_all_too_new() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 11), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip);
        assert_eq!(r.finish(), None);
    }

    #[test]
    fn backward_two_docs() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(2), 3), Verdict::Skip);
        assert_eq!(r.process(&doc(2), 8), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 1), Verdict::EmitPrevious(doc(2), 8));
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 5)));
    }

    #[test]
    fn backward_emit_no_visible_previous() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(2), 15), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 5), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 5)));
    }

    // ─── Shared ───

    #[test]
    fn is_visible_helper() {
        assert!(VersionResolver::is_visible(5, 10));
        assert!(VersionResolver::is_visible(10, 10));
        assert!(!VersionResolver::is_visible(11, 10));
    }

    #[test]
    fn empty_stream() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.finish(), None);
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.finish(), None);
    }

    #[test]
    fn forward_many_versions() {
        let mut r = VersionResolver::new(50, ScanDirection::Forward);
        let id = doc(1);
        for ts in (1..=100).rev() {
            let v = r.process(&id, ts);
            if ts == 50 {
                assert_eq!(v, Verdict::Visible);
            } else {
                assert_eq!(v, Verdict::Skip);
            }
        }
    }

    #[test]
    fn backward_many_versions() {
        let mut r = VersionResolver::new(50, ScanDirection::Backward);
        let id = doc(1);
        for ts in 1..=100 {
            assert_eq!(r.process(&id, ts), Verdict::Skip);
        }
        assert_eq!(r.finish(), Some((id, 50)));
    }

    // ─── Boundary timestamp tests ───

    #[test]
    fn forward_read_ts_zero() {
        let mut r = VersionResolver::new(0, ScanDirection::Forward);
        // ts=0 is visible (ts <= read_ts)
        assert_eq!(r.process(&doc(1), 0), Verdict::Visible);
        // ts=1 is invisible
        assert_eq!(r.process(&doc(2), 1), Verdict::Skip);
    }

    #[test]
    fn forward_read_ts_max() {
        let mut r = VersionResolver::new(u64::MAX, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), u64::MAX), Verdict::Visible);
        assert_eq!(r.process(&doc(2), 0), Verdict::Visible);
    }

    #[test]
    fn backward_read_ts_zero() {
        let mut r = VersionResolver::new(0, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 0), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), 0)));
    }

    #[test]
    fn backward_read_ts_max() {
        let mut r = VersionResolver::new(u64::MAX, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip);
        assert_eq!(r.process(&doc(1), u64::MAX), Verdict::Skip);
        assert_eq!(r.finish(), Some((doc(1), u64::MAX)));
    }

    // ─── Multi-group backward tests ───

    #[test]
    fn backward_three_doc_groups() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);

        // Group doc(3): ts=2, ts=8
        assert_eq!(r.process(&doc(3), 2), Verdict::Skip);
        assert_eq!(r.process(&doc(3), 8), Verdict::Skip);

        // Group doc(2): ts=1, ts=5 — emits doc(3)@8
        assert_eq!(r.process(&doc(2), 1), Verdict::EmitPrevious(doc(3), 8));
        assert_eq!(r.process(&doc(2), 5), Verdict::Skip);

        // Group doc(1): ts=3 — emits doc(2)@5
        assert_eq!(r.process(&doc(1), 3), Verdict::EmitPrevious(doc(2), 5));

        // finish emits doc(1)@3
        assert_eq!(r.finish(), Some((doc(1), 3)));
    }

    #[test]
    fn backward_alternating_visible_invisible_groups() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);

        // Group doc(4): all invisible
        assert_eq!(r.process(&doc(4), 15), Verdict::Skip);

        // Group doc(3): visible
        assert_eq!(r.process(&doc(3), 5), Verdict::Skip); // no previous visible → Skip

        // Group doc(2): all invisible
        assert_eq!(r.process(&doc(2), 20), Verdict::EmitPrevious(doc(3), 5));

        // Group doc(1): visible
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip); // doc(2) had no visible

        // Finish: emits doc(1)@1
        assert_eq!(r.finish(), Some((doc(1), 1)));
    }

    #[test]
    fn forward_single_entry_per_doc() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        assert_eq!(r.process(&doc(1), 5), Verdict::Visible);
        assert_eq!(r.process(&doc(2), 7), Verdict::Visible);
        assert_eq!(r.process(&doc(3), 9), Verdict::Visible);
        assert_eq!(r.process(&doc(4), 11), Verdict::Skip);
    }

    #[test]
    fn backward_single_entry_per_doc() {
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(3), 9), Verdict::Skip);
        assert_eq!(r.process(&doc(2), 7), Verdict::EmitPrevious(doc(3), 9));
        assert_eq!(r.process(&doc(1), 5), Verdict::EmitPrevious(doc(2), 7));
        assert_eq!(r.finish(), Some((doc(1), 5)));
    }

    #[test]
    fn forward_ts_equals_read_ts_boundary() {
        let mut r = VersionResolver::new(10, ScanDirection::Forward);
        // Exactly at boundary: visible
        assert_eq!(r.process(&doc(1), 10), Verdict::Visible);
        // One above: invisible
        assert_eq!(r.process(&doc(2), 11), Verdict::Skip);
    }

    #[test]
    fn backward_keeps_latest_visible() {
        // In backward mode (ascending ts), the resolver should keep updating
        // best_visible_ts to the latest visible value
        let mut r = VersionResolver::new(10, ScanDirection::Backward);
        assert_eq!(r.process(&doc(1), 1), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 3), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 7), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 10), Verdict::Skip);
        assert_eq!(r.process(&doc(1), 15), Verdict::Skip); // too new
        // Best is ts=10
        assert_eq!(r.finish(), Some((doc(1), 10)));
    }
}
