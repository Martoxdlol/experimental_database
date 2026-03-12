# D6: Index Builder

## Purpose

Background index building: scans the primary B-tree at a snapshot timestamp and inserts entries into a `Building`-state secondary index. This enables creating indexes on existing collections without blocking writes (DESIGN.md section 3.7).

## Dependencies

- **D3 (Primary Index)**: `PrimaryIndex`, `PrimaryScanStream` (snapshot scan source)
- **D4 (Secondary Index)**: `SecondaryIndex` (target for entries)
- **D5 (Array Indexing)**: `compute_index_entries` (key generation per document)
- **D1 (Key Encoding)**: `make_secondary_key_from_prefix`
- **L1 (Core Types)**: `DocId`, `Ts`, `FieldPath`
- **L1 (Core Encoding)**: `decode_document` (BSON → JSON for field extraction)

## Rust Types

```rust
use crate::core::types::{DocId, Ts, FieldPath};
use crate::core::encoding::decode_document;
use crate::docstore::primary_index::PrimaryIndex;
use crate::docstore::secondary_index::SecondaryIndex;
use crate::docstore::array_indexing::compute_index_entries;
use crate::docstore::key_encoding::make_secondary_key_from_prefix;
use std::sync::Arc;

/// Builds a secondary index by scanning the primary index at a snapshot timestamp.
pub struct IndexBuilder {
    primary: Arc<PrimaryIndex>,
    secondary: Arc<SecondaryIndex>,
    field_paths: Vec<FieldPath>,
}

/// Progress report emitted periodically during build.
pub struct BuildProgress {
    pub docs_scanned: u64,
    pub entries_inserted: u64,
    pub elapsed_ms: u64,
}

impl IndexBuilder {
    pub fn new(
        primary: Arc<PrimaryIndex>,
        secondary: Arc<SecondaryIndex>,
        field_paths: Vec<FieldPath>,
    ) -> Self;

    /// Run the index build: scan primary at build_snapshot_ts,
    /// insert entries into the secondary index.
    ///
    /// This is designed to run as a background tokio task.
    /// The caller (L6) handles:
    ///   - Writing the IndexReady WAL record on success
    ///   - Updating the catalog state from Building → Ready
    ///   - Dropping the partial index on failure/crash
    ///
    /// Returns the number of entries inserted.
    pub async fn build(
        &self,
        build_snapshot_ts: Ts,
        progress_tx: Option<tokio::sync::watch::Sender<BuildProgress>>,
    ) -> Result<u64>;
}
```

## Implementation Details

### build()

```
1. Let stream = self.primary.scan_at_ts(build_snapshot_ts, Forward);
2. let mut entries_inserted: u64 = 0;
3. let mut docs_scanned: u64 = 0;

4. while let Some((doc_id, version_ts, body_bytes)) = stream.next().await {
       docs_scanned += 1;

       // Decode BSON body to JSON for field extraction
       let doc = decode_document(&body_bytes)?;

       // Compute index key prefixes (handles arrays)
       let key_prefixes = compute_index_entries(&doc, &self.field_paths)?;

       // Insert one entry per key prefix
       for prefix in key_prefixes {
           let full_key = make_secondary_key_from_prefix(&prefix, &doc_id, version_ts);
           self.secondary.insert_entry(&full_key).await?;
           entries_inserted += 1;
       }

       // Periodically report progress and yield to other tasks
       if docs_scanned % 1000 == 0 {
           if let Some(ref tx) = progress_tx {
               let _ = tx.send(BuildProgress { docs_scanned, entries_inserted, ... });
           }
           tokio::task::yield_now().await;
       }
   }

5. Return Ok(entries_inserted);
```

### Concurrent Writes During Build

The index builder runs at a snapshot timestamp (`build_snapshot_ts`). While it runs:

- New commits (with ts > build_snapshot_ts) also insert entries into the Building index in real-time. This is handled by the commit path in L5, not by this module.
- The builder sees a consistent snapshot and never races with the commit path — they write different keys (different ts values).

After the builder finishes, all documents visible at `build_snapshot_ts` have entries, and all commits after `build_snapshot_ts` have also been inserting entries. The index is complete.

### Crash Recovery

If the system crashes while an index is `Building`:
- On recovery, the partial index is dropped (catalog entry removed, B-tree pages freed).
- The build can optionally be restarted automatically.
- This is handled by L6 (Database startup), not by this module.

### Yielding

The builder yields to the tokio runtime every 1000 documents to avoid starving other tasks. This is especially important since B-tree operations hold frame latches briefly.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Primary scan error | B-tree I/O failure | Propagate (build fails) |
| Document decode error | Corrupt BSON body | Propagate (build fails) |
| Array constraint violation | Multiple arrays in compound index | Propagate (build fails — should have been caught at index creation) |
| Secondary insert error | B-tree I/O failure | Propagate (build fails) |

On any error, the build fails and the caller (L6) handles cleanup (drop partial index).

## Tests

1. **Build on empty collection**: build with no documents → 0 entries inserted, returns Ok.
2. **Build on single document**: one document with indexed field → 1 entry in secondary.
3. **Build on multiple documents**: 100 documents, verify secondary has 100 entries (single field index).
4. **Build with array field**: 10 documents each with 3-element array → 30 entries.
5. **Build with missing fields**: some documents missing the indexed field → entries with undefined key for those docs.
6. **Build with compound index**: compound index [a, b], verify entries encode both fields.
7. **Snapshot isolation**: insert doc at ts=5, insert another at ts=10. Build at snapshot_ts=7 → only first doc's entry.
8. **Progress reporting**: verify progress_tx receives updates during build.
9. **Large build**: 10,000 documents, verify all entries created correctly.
10. **Tombstoned documents**: insert doc, then tombstone it before build_snapshot_ts. Build should not include it (scanner skips tombstones).
