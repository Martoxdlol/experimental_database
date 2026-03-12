# D4: Secondary Index

## Purpose

Secondary index wrapper around a B-tree with MVCC-aware scanning. Handles version resolution and primary index verification (DESIGN.md section 3.5) to skip stale entries. Secondary index entries have the key format `type_tag || encoded_value || doc_id || inv_ts` with empty values.

## Dependencies

- **D1 (Key Encoding)**: `make_secondary_key`, `parse_secondary_key_suffix`, `inv_ts`
- **D2 (Version Resolution)**: `VersionResolver`, `Verdict`
- **D3 (Primary Index)**: `PrimaryIndex` (for verification of stale entries)
- **L2 (Storage Engine)**: `BTreeHandle`, `ScanStream`, `ScanDirection`
- **L1 (Core Types)**: `DocId`, `Ts`, `IndexId`

## Rust Types

```rust
use crate::storage::{BTreeHandle, ScanDirection, ScanStream};
use crate::core::types::{DocId, Ts};
use crate::docstore::key_encoding::{parse_secondary_key_suffix, inv_ts};
use crate::docstore::version_resolution::{VersionResolver, Verdict};
use crate::docstore::primary_index::PrimaryIndex;
use std::ops::Bound;
use std::sync::Arc;

/// A secondary index backed by a B-tree.
/// Keys: type_tag[1] || encoded_value[var] || doc_id[16] || inv_ts[8]
/// Values: empty (doc_id is in the key, body is in the primary index)
pub struct SecondaryIndex {
    btree: BTreeHandle,
    primary: Arc<PrimaryIndex>,
}

impl SecondaryIndex {
    /// Create a new SecondaryIndex.
    /// `btree`: the underlying B-tree for this index.
    /// `primary`: reference to the primary index for stale entry verification.
    pub fn new(btree: BTreeHandle, primary: Arc<PrimaryIndex>) -> Self;

    /// Insert a secondary index entry for a document version.
    /// `encoded_key`: the full secondary key (prefix + doc_id + inv_ts).
    /// Value is always empty.
    pub async fn insert_entry(&self, encoded_key: &[u8]) -> std::io::Result<()>;

    /// Remove a secondary index entry.
    pub async fn remove_entry(&self, encoded_key: &[u8]) -> std::io::Result<bool>;

    /// Scan with MVCC version resolution and primary index verification.
    ///
    /// For each (doc_id, inv_ts) in the scan:
    ///   1. Skip if version_ts > read_ts
    ///   2. Within same doc_id: take first (highest ts <= read_ts), skip rest
    ///   3. Verify against primary index: if the primary index shows a different
    ///      latest version ts for this doc_id, the secondary entry is stale — skip
    ///
    /// Yields verified (doc_id, version_ts) pairs.
    pub fn scan_at_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: Ts,
        direction: ScanDirection,
    ) -> SecondaryScanStream;

    /// Access the underlying B-tree (for vacuum).
    pub fn btree(&self) -> &BTreeHandle;
}

/// Stream yielding verified (doc_id, version_ts) pairs from a secondary index scan.
pub struct SecondaryScanStream {
    inner: ScanStream,
    resolver: VersionResolver,
    primary: Arc<PrimaryIndex>,
    read_ts: Ts,
}

impl Stream for SecondaryScanStream {
    type Item = Result<(DocId, Ts)>;
}
```

## Implementation Details

### insert_entry()

```rust
self.btree.insert(encoded_key, &[]).await  // empty value
```

### remove_entry()

```rust
self.btree.delete(encoded_key).await
```

### scan_at_ts()

Create a `SecondaryScanStream` wrapping the B-tree scan with a `VersionResolver`.

### SecondaryScanStream::poll_next()

The stream must handle all three `Verdict` variants. In forward mode, `EmitPrevious` never occurs. In backward mode, `Visible` never occurs.

```
// Helper: verify a (doc_id, ts) against the primary index
async fn verify(&self, doc_id: &DocId, ts: Ts) -> Result<Option<(DocId, Ts)>> {
    match self.primary.get_version_ts(doc_id, self.read_ts).await? {
        Some(primary_ts) if primary_ts == ts => Ok(Some((*doc_id, ts))),
        _ => Ok(None),  // stale or tombstoned
    }
}

// Conceptual async next (actual implementation uses poll_next + Pin)
async fn next(&mut self) -> Option<Result<(DocId, Ts)>> {
    loop {
        match self.inner.next().await {
            None => {
                // End of stream — check finish() for backward mode
                if let Some((doc_id, ts)) = self.resolver.finish() {
                    match self.verify(&doc_id, ts).await {
                        Ok(Some(pair)) => return Some(Ok(pair)),
                        Ok(None) => return None,  // stale final entry
                        Err(e) => return Some(Err(e)),
                    }
                }
                return None;
            }
            Some(Ok((key, _value))) => {
                let (doc_id, ts) = match parse_secondary_key_suffix(&key) {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                match self.resolver.process(&doc_id, ts) {
                    Verdict::Skip => continue,
                    Verdict::Visible => {
                        // Forward mode: verify immediately
                        match self.verify(&doc_id, ts).await {
                            Ok(Some(pair)) => return Some(Ok(pair)),
                            Ok(None) => continue,  // stale
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Verdict::EmitPrevious(prev_id, prev_ts) => {
                        // Backward mode: previous group resolved
                        match self.verify(&prev_id, prev_ts).await {
                            Ok(Some(pair)) => return Some(Ok(pair)),
                            Ok(None) => continue,  // stale, keep scanning
                            Err(e) => return Some(Err(e)),
                        }
                    }
                }
            }
            Some(Err(e)) => return Some(Err(e)),
        }
    }
}
```

### Why Verification is Needed

When a document is updated and a field value changes, the OLD secondary index entry remains in the B-tree (append-only model). Consider:

1. Doc A has `status = "active"` at ts=5 → secondary entry for "active" + (A, inv_ts(5))
2. Doc A is updated to `status = "inactive"` at ts=10 → new secondary entry for "inactive" + (A, inv_ts(10))
3. A scan on `status = "active"` at read_ts=15 finds the entry from step 1.
4. Without verification, it would incorrectly return doc A.
5. With verification: `primary.get_version_ts(A, read_ts=15)` returns `ts=10`, which != 5, so the entry is stale and skipped.

### Performance Considerations

- Verification requires a primary index lookup per visible secondary entry. This is O(log N) per entry.
- For recently written data (common case), the secondary entry IS the latest version, so verification confirms immediately.
- Stale entries are rare except for documents with frequent updates to indexed fields.
- Vacuum removes stale secondary entries over time, reducing verification misses.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| B-tree I/O error | L2 backend failure | Propagate |
| Key parse error | Corrupt secondary key | Propagate |
| Primary lookup error | Primary B-tree failure during verification | Propagate |

## Tests

1. **Insert and scan**: insert one entry, scan the full range, verify it's yielded.
2. **Insert multiple, scan range**: insert entries for values "a", "b", "c", scan ["b", "c"), verify only "b" returned.
3. **Version resolution in scan**: insert two entries for same doc_id with different ts. Scan at read_ts between them — only the older visible version yields.
4. **Stale entry skipped**: insert entry (value="x", doc_id=A, ts=5). Update primary to have ts=10 for doc_id=A. Scan secondary — entry is skipped because primary says ts=10 != 5.
5. **Multiple doc_ids**: insert entries for doc A and B. Scan, verify both yielded.
6. **Tombstoned doc**: insert secondary entry for doc_id=A at ts=5. Mark doc_id=A as tombstone at ts=10 in primary. Scan secondary — skipped because primary returns None.
7. **Remove entry**: insert entry, remove it, scan — not found.
8. **Empty scan**: scan with no entries → empty iterator.
9. **Scan direction**: verify forward and backward scans return entries in correct order.
10. **Compound key scan**: insert entries with compound keys, scan with prefix bounds, verify correct entries.
11. **Scan with exact bounds**: lower = Included(key), upper = Excluded(key+1), returns exactly one entry.
12. **Large number of entries**: insert 1000 entries, scan full range, verify all yielded with correct version resolution.
