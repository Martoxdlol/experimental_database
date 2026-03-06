# D2: Version Resolution

## Purpose

Encapsulates the shared MVCC version resolution algorithm used by both primary and secondary index scans. Given a stream of `(doc_id, ts)` pairs from a B-tree scan, resolves to the latest visible version per doc_id at a given `read_ts`. Direction-aware: handles both forward scans (descending ts within doc_id) and backward scans (ascending ts within doc_id).

## Dependencies

- **L1 (`core/types.rs`)**: `DocId`, `Ts`
- **L2 (`storage`)**: `ScanDirection` (enum only, no I/O dependency)

No L2 I/O dependency. Pure state machine logic.

## Rust Types

```rust
use crate::core::types::{DocId, Ts};
use crate::storage::ScanDirection;

/// State machine that processes a stream of (doc_id, ts) entries
/// and determines which entries are visible at a given read_ts.
///
/// Direction-aware:
/// - Forward (ascending doc_id, descending ts within group): the first
///   entry with ts <= read_ts is the visible version. Emit immediately.
/// - Backward (descending doc_id, ascending ts within group): entries
///   arrive oldest-first within a group. Buffer the best candidate and
///   emit when the group changes or the stream ends.
///
/// Usage (forward):
///   let mut resolver = VersionResolver::new(read_ts, Forward);
///   for (doc_id, ts) in btree_scan {
///       match resolver.process(&doc_id, ts) {
///           Verdict::Visible => { /* yield this entry */ }
///           Verdict::Skip => { /* skip */ }
///           Verdict::EmitPrevious(prev_id, prev_ts) => {
///               /* yield prev_id/prev_ts, then check current entry too */
///           }
///       }
///   }
///   if let Some((id, ts)) = resolver.finish() { /* yield final group */ }
pub struct VersionResolver {
    read_ts: Ts,
    direction: ScanDirection,
    current_doc_id: Option<DocId>,
    // Forward mode
    found_visible: bool,
    // Backward mode
    best_visible_ts: Option<Ts>,
}

/// Result of processing one entry through the resolver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// This entry is the latest visible version at read_ts for its doc_id.
    /// (Forward mode only — in backward mode, visibility is deferred.)
    Visible,
    /// This entry should be skipped.
    Skip,
    /// The PREVIOUS doc_id group's visible version is ready to emit.
    /// The contained (DocId, Ts) is the result for that group.
    /// The current entry has NOT been processed yet — the caller must
    /// decide what to do with it (typically it starts a new group and
    /// will be resolved in subsequent calls).
    /// (Backward mode only.)
    EmitPrevious(DocId, Ts),
}

impl VersionResolver {
    /// Create a new resolver pinned to the given read timestamp and direction.
    pub fn new(read_ts: Ts, direction: ScanDirection) -> Self;

    /// Process the next (doc_id, ts) entry from a B-tree scan.
    ///
    /// Forward: entries arrive in ascending doc_id, descending ts order.
    /// Backward: entries arrive in descending doc_id, ascending ts order.
    pub fn process(&mut self, doc_id: &DocId, ts: Ts) -> Verdict;

    /// Signal end of stream. In backward mode, returns the final group's
    /// visible (doc_id, ts) if any. In forward mode, always returns None
    /// (results are emitted eagerly).
    pub fn finish(&mut self) -> Option<(DocId, Ts)>;

    /// Check if a single version timestamp is visible at read_ts.
    /// Convenience method: ts <= read_ts.
    pub fn is_visible(ts: Ts, read_ts: Ts) -> bool;
}
```

## Implementation Details

### Forward Mode — process()

In forward scans, within a doc_id group, entries arrive in descending ts order (most recent first). The first entry with `ts <= read_ts` is the visible version.

```
fn process_forward(&mut self, doc_id: &DocId, ts: Ts) -> Verdict {
    let same_doc = match &self.current_doc_id {
        Some(current) => current == doc_id,
        None => false,
    };

    if !same_doc {
        // New doc_id group — reset
        self.current_doc_id = Some(*doc_id);
        self.found_visible = false;
    }

    if self.found_visible {
        return Verdict::Skip;
    }

    if ts > self.read_ts {
        return Verdict::Skip;  // too new
    }

    // First entry with ts <= read_ts → latest visible
    self.found_visible = true;
    Verdict::Visible
}
```

### Backward Mode — process()

In backward scans, within a doc_id group, entries arrive in ascending ts order (oldest first). We must buffer the latest visible candidate and emit when the group changes.

```
fn process_backward(&mut self, doc_id: &DocId, ts: Ts) -> Verdict {
    let same_doc = match &self.current_doc_id {
        Some(current) => current == doc_id,
        None => false,
    };

    if same_doc {
        // Same group — update candidate if visible
        if ts <= self.read_ts {
            self.best_visible_ts = Some(ts);  // overwrite: later ts is better
        }
        return Verdict::Skip;
    }

    // New group — emit previous group's result if any, then start new group
    let emit = match (self.current_doc_id.take(), self.best_visible_ts.take()) {
        (Some(prev_id), Some(prev_ts)) => Some(Verdict::EmitPrevious(prev_id, prev_ts)),
        _ => None,
    };

    // Start new group
    self.current_doc_id = Some(*doc_id);
    self.best_visible_ts = if ts <= self.read_ts { Some(ts) } else { None };

    emit.unwrap_or(Verdict::Skip)
}
```

**Important**: When `process()` returns `EmitPrevious(prev_id, prev_ts)`, the current entry has already been absorbed into the new group. The caller yields `(prev_id, prev_ts)` and then continues calling `process()` with the next entry — it does NOT need to re-feed the current entry.

### finish()

```
fn finish(&mut self) -> Option<(DocId, Ts)> {
    match self.direction {
        ScanDirection::Forward => None,  // forward emits eagerly
        ScanDirection::Backward => {
            match (self.current_doc_id.take(), self.best_visible_ts.take()) {
                (Some(id), Some(ts)) => Some((id, ts)),
                _ => None,
            }
        }
    }
}
```

### is_visible()

```
fn is_visible(ts: Ts, read_ts: Ts) -> bool {
    ts <= read_ts
}
```

## Key Invariants

1. **Input ordering**: The caller MUST feed entries in B-tree key order.
   - Forward: ascending doc_id, descending ts within each group.
   - Backward: descending doc_id, ascending ts within each group.
   This is naturally provided by B-tree scans on keys `doc_id || inv_ts`.

2. **One visible version per doc_id**: For any doc_id, at most one entry will be identified as visible.

3. **Tombstone awareness**: This module does NOT handle tombstones — it only determines visibility by timestamp. The caller (PrimaryIndex, SecondaryIndex) checks the tombstone flag on the visible version and decides whether to yield or skip.

4. **finish() is mandatory**: In backward mode, the last group's result is only available via `finish()`. Callers MUST call `finish()` after the scan loop.

## Error Handling

No errors. This is a pure state machine with no I/O or fallible operations.

## Tests

### Forward Mode

1. **Single version, visible**: one entry (doc_id=A, ts=5), read_ts=10 -> Visible.
2. **Single version, invisible**: one entry (doc_id=A, ts=15), read_ts=10 -> Skip.
3. **Multiple versions, newest visible**: entries [(A, 10), (A, 5), (A, 1)], read_ts=10 -> [Visible, Skip, Skip].
4. **Multiple versions, middle visible**: entries [(A, 15), (A, 10), (A, 5)], read_ts=12 -> [Skip, Visible, Skip].
5. **Multiple versions, oldest visible**: entries [(A, 15), (A, 10), (A, 5)], read_ts=7 -> [Skip, Skip, Visible].
6. **Multiple versions, none visible**: entries [(A, 15), (A, 10)], read_ts=5 -> [Skip, Skip].
7. **Multiple doc_ids**: entries [(A, 10), (A, 5), (B, 12), (B, 8)], read_ts=10 -> [Visible, Skip, Skip, Visible].
8. **Exact timestamp match**: entry (A, 10), read_ts=10 -> Visible.
9. **finish() returns None in forward mode**: after processing entries, finish() returns None.

### Backward Mode

10. **Single version, visible**: entry (A, 5), read_ts=10. process -> Skip. finish() -> Some((A, 5)).
11. **Single version, invisible**: entry (A, 15), read_ts=10. process -> Skip. finish() -> None.
12. **Multiple versions, ascending ts**: entries [(A, 1), (A, 5), (A, 10)], read_ts=10. All Skip. finish() -> Some((A, 10)).
13. **Multiple versions, latest too new**: entries [(A, 1), (A, 5), (A, 15)], read_ts=10. All Skip. finish() -> Some((A, 5)).
14. **Multiple versions, all too new**: entries [(A, 11), (A, 15)], read_ts=10. All Skip. finish() -> None.
15. **Two doc_ids**: entries [(B, 3), (B, 8), (A, 1), (A, 5)], read_ts=10. process(B,3) -> Skip, process(B,8) -> Skip, process(A,1) -> EmitPrevious(B, 8), process(A,5) -> Skip. finish() -> Some((A, 5)).
16. **EmitPrevious with no visible previous**: entries [(B, 15), (A, 5)], read_ts=10. process(B,15) -> Skip, process(A,5) -> Skip (no EmitPrevious because B had no visible). finish() -> Some((A, 5)).
17. **finish() is mandatory**: backward mode with one group; result only from finish().
18. **Many groups backward**: 10 doc_ids each with 3 versions, verify each group emits correctly.

### Shared

19. **is_visible helper**: is_visible(5, 10) = true, is_visible(10, 10) = true, is_visible(11, 10) = false.
20. **Empty stream**: no entries processed, finish() returns None.
21. **Single doc_id many versions forward**: 100 versions of same doc_id with ts 1..100, read_ts=50. Only ts=50 -> Visible.
22. **Single doc_id many versions backward**: 100 versions ts 1..100, read_ts=50. finish() -> Some((id, 50)).
