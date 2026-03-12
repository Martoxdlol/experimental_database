# Q4: Scan Execution

## Purpose

Executes an `AccessMethod` by driving Layer 3 async streams through the three-stage pipeline: Source → PostFilter → Terminal. Returns an async stream over matching documents. Computes read set intervals from access method bounds.

Corresponds to DESIGN.md sections 4.1, 4.2, 4.5.

## Dependencies

- **Q1 (`query/post_filter.rs`)**: `filter_matches`
- **Q3 (`query/access.rs`)**: `AccessMethod`
- **L3 (`docstore/primary_index.rs`)**: `PrimaryIndex`, `PrimaryScanner`
- **L3 (`docstore/secondary_index.rs`)**: `SecondaryIndex`, `SecondaryScanner`
- **L1 (`core/encoding.rs`)**: `decode_document`
- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`, `DocId`, `Ts`
- **L2 (`storage/btree.rs`)**: `ScanDirection`

## Rust Types

```rust
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_core::filter::Filter;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use crate::access::AccessMethod;
use crate::post_filter::filter_matches;
use std::collections::HashMap;
use std::ops::Bound;

/// A single result row from a query scan.
pub struct ScanRow {
    pub doc_id: DocId,
    pub version_ts: Ts,
    pub doc: serde_json::Value,
}

/// Read set interval computed from the plan bounds.
/// Available before iteration begins.
/// Convention: `lower` is always an inclusive bound (DESIGN.md 5.6.2).
/// L6 may tighten these bounds after iteration for limit-aware queries (5.6.3).
pub struct ReadIntervalInfo {
    pub collection_id: CollectionId,
    pub index_id: IndexId,
    pub lower: Vec<u8>,            // always Included (per DESIGN.md 5.6.2)
    pub upper: Bound<Vec<u8>>,     // Excluded or Unbounded
}

/// Scan statistics accumulated during iteration.
#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    pub scanned_docs: usize,
    pub scanned_bytes: usize,
    pub returned_docs: usize,
}

/// Execute a query plan against the indexes at the given read timestamp.
///
/// Returns a QueryScanStream that lazily produces documents through the
/// Source → PostFilter → Terminal pipeline, along with the ReadIntervalInfo.
pub async fn execute_scan<'a>(
    method: &'a AccessMethod,
    primary_index: &'a PrimaryIndex,
    secondary_indexes: &'a HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
) -> std::io::Result<(QueryScanStream<'a>, ReadIntervalInfo)>;

/// Async stream over query results.
pub struct QueryScanStream<'a> { /* internal state */ }

impl<'a> futures_core::Stream for QueryScanStream<'a> {
    type Item = std::io::Result<ScanRow>;
}
```

## Implementation Details

### execute_scan()

Constructs a `QueryScanStream` based on the plan variant:

```rust
pub async fn execute_scan<'a>(
    method: &'a AccessMethod,
    primary_index: &'a PrimaryIndex,
    secondary_indexes: &'a HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
) -> std::io::Result<(QueryScanStream<'a>, ReadIntervalInfo)> {
    match method {
        AccessMethod::PrimaryGet { collection_id, doc_id } => {
            // Point lookup — not a scan, but we wrap it as a single-element stream.
            let body = primary_index.get_at_ts(doc_id, read_ts).await?;
            let ts = primary_index.get_version_ts(doc_id, read_ts).await?;
            // ... construct single-row or empty stream + read interval
        }

        AccessMethod::IndexScan {
            collection_id, index_id, lower, upper,
            direction, post_filter, limit,
        } => {
            let sec_index = secondary_indexes.get(index_id)
                .ok_or_else(|| io::Error::other("index not found"))?;

            let stream = sec_index.scan_at_ts(
                bound_as_ref(lower),
                bound_as_ref(upper),
                read_ts,
                *direction,
            );

            // Wrap in QueryScanStream with post_filter + limit + primary fetch
        }

        AccessMethod::TableScan {
            collection_id, index_id, direction,
            post_filter, limit,
        } => {
            let sec_index = secondary_indexes.get(index_id)
                .ok_or_else(|| io::Error::other("_created_at index not found"))?;

            let stream = sec_index.scan_at_ts(
                Bound::Unbounded,
                Bound::Unbounded,
                read_ts,
                *direction,
            );

            // Wrap in QueryScanStream with post_filter + limit + primary fetch
        }
    }
}
```

### QueryScanStream internals

The stream wraps the L3 async stream and adds three stages:

```rust
enum ScanSource<'a> {
    /// Point lookup result (0 or 1 documents).
    Point(Option<ScanRow>),
    /// Secondary index scan — yields (doc_id, ts), needs primary fetch.
    Secondary {
        inner: SecondaryStream<'a>,
        primary: &'a PrimaryIndex,
    },
}

struct QueryScanStream<'a> {
    source: ScanSource<'a>,
    post_filter: Option<Filter>,
    limit: Option<usize>,
    stats: ScanStats,
}
```

### QueryScanStream::poll_next()

```
1. Check limit: if returned_docs >= limit, return Poll::Ready(None).

2. Pull next from source (async):
   a. Point: take the Option<ScanRow>, return it (single shot).
   b. Secondary:
      - Poll inner stream to get (doc_id, version_ts).
        Note: SecondaryStream yields (DocId, Ts) — the Ts is the version
        timestamp from primary verification. Use this directly for ScanRow.version_ts.
      - Fetch document body: primary.get_at_ts(doc_id, read_ts).await.
      - If get_at_ts returns None, skip (tombstone race — shouldn't happen
        normally but handle defensively).
      - Decode body: decode_document(body_bytes) → serde_json::Value.
      - Increment scanned_docs and scanned_bytes.

3. Apply post-filter: if post_filter is Some, call filter_matches(doc, filter).
   If no match, go back to step 2.

4. Increment returned_docs. Return Poll::Ready(Some(Ok(ScanRow { doc_id, version_ts, doc }))).
```

### Read interval computation

The read interval is computed from the plan bounds when the scanner is constructed:

```rust
fn compute_read_interval(plan: &AccessMethod) -> ReadIntervalInfo {
    match plan {
        AccessMethod::PrimaryGet { collection_id, doc_id } => {
            // Point interval: [doc_id || 0x00..00, successor(doc_id) || 0x00..00)
            // Per DESIGN.md section 5.6.2
            let mut lower = doc_id.as_bytes().to_vec();
            lower.extend_from_slice(&[0u8; 8]);
            // Use successor_key on the 16-byte doc_id to get the next doc_id prefix.
            let upper_prefix = successor_key(&doc_id.as_bytes().to_vec());
            let mut upper = upper_prefix;
            upper.extend_from_slice(&[0u8; 8]);
            ReadIntervalInfo {
                collection_id: *collection_id,
                index_id: PRIMARY_INDEX_ID, // convention: IndexId(0) for primary
                lower,
                upper: Bound::Excluded(upper),
            }
        }

        AccessMethod::IndexScan { collection_id, index_id, lower, upper, .. } => {
            ReadIntervalInfo {
                collection_id: *collection_id,
                index_id: *index_id,
                // lower is always Included in practice — encode_range only
                // produces Included or Unbounded for the lower bound.
                lower: match lower {
                    Bound::Included(v) => v.clone(),
                    Bound::Excluded(v) => v.clone(),
                    Bound::Unbounded => vec![0x00],
                },
                upper: match upper {
                    Bound::Excluded(v) => Bound::Excluded(v.clone()),
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(v) => Bound::Excluded(prefix_successor(v)),
                },
            }
        }

        AccessMethod::TableScan { collection_id, index_id, .. } => {
            // Full interval: [MIN, Unbounded) per DESIGN.md section 5.6.2
            ReadIntervalInfo {
                collection_id: *collection_id,
                index_id: *index_id,
                lower: vec![0x00],
                upper: Bound::Unbounded,
            }
        }
    }
}
```

### Query ID (DESIGN.md section 4.6)

Every read operation within a transaction is assigned an incremental `query_id: u32` (starting at 0). This ID is **not** assigned by L4 — it is assigned by L6 when recording the read set interval. L4's `ReadIntervalInfo` does not carry a `query_id`. L6 wraps it:

```rust
// In L6 (not L4):
struct ReadSetEntry {
    query_id: u32,
    interval: ReadIntervalInfo,
}
```

The `query_id` is used for subscription granularity (which queries were invalidated) and cache keying. L4 does not need to know about it.

### Read set size limits (DESIGN.md section 5.6.5)

The stream tracks `scanned_docs` and `scanned_bytes` via `ScanStats`. However, L4 does **not** enforce the read set size limits (`max_intervals`, `max_scanned_bytes`, `max_scanned_docs`). These limits are transaction-scoped (accumulated across multiple queries), so they are enforced by L6 which inspects stats after consuming the stream and aborts the transaction with `read_limit_exceeded` if any limit is exceeded.

### Limit-aware interval tightening

Per DESIGN.md section 5.6.3, when a query returns exactly `limit` results, the interval tightens to the last returned key. This is NOT done inside the stream — it is done by L6 after the stream is consumed, because it requires knowing the last returned document's index key encoding.

### Performance: avoiding unnecessary primary fetches

For `PrimaryGet`, the body is fetched directly — no secondary index involved.

For `IndexScan` / `TableScan`, each secondary stream hit requires an async primary fetch to get the document body (for post-filter evaluation and returning to the caller). This is inherent to the secondary index design — secondary entries contain only the key, not the body.

If there's no post-filter, we still need the body for the response. The primary fetch is always needed.

### Helper: bound_as_ref

```rust
fn bound_as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
```

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Index not found | index_id not in secondary_indexes map | Return io::Error |
| B-tree I/O error | L2/L3 backend failure during scan | Propagate through iterator |
| Document decode error | Corrupt BSON body | Propagate through iterator |
| Primary fetch miss | SecondaryScanner yielded a doc_id but primary returns None | Skip (defensive; log warning) |

## Tests

### Unit tests (with in-memory storage)

1. **PrimaryGet hit**: insert one doc, execute PrimaryGet, verify ScanRow returned.
2. **PrimaryGet miss**: execute PrimaryGet on non-existent doc_id, verify empty.
3. **PrimaryGet tombstone**: insert then delete, execute PrimaryGet, verify empty.
4. **IndexScan basic**: insert 3 docs with secondary index entries, scan full range, verify 3 results.
5. **IndexScan with range**: insert docs with values 1..5, scan [2, 4), verify 2 results (values 2 and 3).
6. **IndexScan with post-filter**: insert 5 docs, post-filter keeps 2, verify 2 results and scanned_docs=5.
7. **IndexScan with limit**: insert 10 docs, limit=3, verify exactly 3 results.
8. **IndexScan limit + filter**: insert 10 docs, filter keeps 5, limit=2, verify 2 results.
9. **IndexScan direction**: verify Forward returns ascending, Backward returns descending.
10. **TableScan**: insert 5 docs, table scan, verify all 5 returned.
11. **TableScan with filter**: insert 5 docs, filter keeps 2, verify 2 results.
12. **Read interval**: verify read_interval() matches expected bounds for each plan type.
13. **Stats**: verify scanned_docs, scanned_bytes, returned_docs are accurate.
14. **Empty collection**: scan on empty collection, verify empty result and stats=0.
15. **MVCC visibility**: insert doc at ts=5, scan at read_ts=3, verify not visible. Scan at read_ts=10, verify visible.
16. **Multiple versions**: insert at ts=5, update at ts=10. Scan at read_ts=7 sees v5. Scan at read_ts=15 sees v10.

### Integration tests

17. **Large scan**: insert 1000 docs, scan all, verify count and ordering.
18. **Compound index scan**: index on [A, B], range eq(A, 1), verify only A=1 docs returned.
19. **Stream laziness**: start stream, consume 2 of 100 results, drop stream. Verify no crash and that not all 100 docs were fetched (if measurable).
