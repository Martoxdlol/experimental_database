# Q5: Read-Your-Own-Writes Merge

## Purpose

Merges snapshot scan results with buffered in-transaction mutations to provide read-your-own-writes semantics within a mutation transaction. The merge layer sits between the scan executor (Q4) and the caller (L6), overlaying write-set changes onto the snapshot stream.

Corresponds to DESIGN.md section 5.4.

## Dependencies

- **Q1 (`query/post_filter.rs`)**: `filter_matches`
- **Q4 (`query/scan.rs`)**: `ScanRow`
- **L1 (`core/types.rs`)**: `DocId`, `CollectionId`
- **L1 (`core/field_path.rs`)**: `FieldPath`
- **L1 (`core/filter.rs`)**: `Filter`
- **L1 (`core/encoding.rs`)**: `extract_scalar`
- **L3 (`docstore/key_encoding.rs`)**: `encode_key_prefix`
- **L2 (`storage/btree.rs`)**: `ScanDirection`

**Critically, L4 does NOT import L5 types.** The caller (L6) decomposes the `WriteSet` into a `MergeView` — plain slices of inserts, deletes, and replaces — before calling `merge_with_writes`.

## Rust Types

```rust
use exdb_core::types::DocId;
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::encoding::extract_scalar;
use exdb_docstore::key_encoding::encode_key_prefix;
use exdb_storage::btree::ScanDirection;
use crate::scan::ScanRow;
use crate::post_filter::filter_matches;
use std::ops::Bound;

/// A view of buffered mutations for one collection.
/// Constructed by L6 from the WriteSet — L4 does not depend on L5.
pub struct MergeView<'a> {
    /// Documents inserted in this transaction that haven't been committed yet.
    pub inserts: &'a [(DocId, serde_json::Value)],
    /// Documents deleted in this transaction.
    pub deletes: &'a [DocId],
    /// Documents replaced in this transaction (new body).
    pub replaces: &'a [(DocId, serde_json::Value)],
}

/// Merge snapshot scan results with buffered mutations.
///
/// DESIGN.md section 5.4 rules:
/// - Get by ID: write set first, then snapshot (handled by L6, not here).
/// - Scan: merge results from snapshot with write set:
///   - Snapshot docs whose doc_id is in deletes: excluded.
///   - Snapshot docs whose doc_id is in replaces: replaced with new body,
///     then re-evaluated against the post-filter.
///   - Write-set inserts that fall within the scan range AND match the
///     post-filter: included in correct sort position.
///   - Result maintains index sort order.
///
/// Parameters:
/// - `snapshot`: lazy iterator from execute_scan (Q4).
/// - `merge_view`: decomposed write set for this collection.
/// - `sort_fields`: index field paths for sort key extraction.
/// - `range_lower`/`range_upper`: scan bounds (value prefix only).
///   Used to filter write-set inserts that fall within the range.
/// - `post_filter`: optional filter applied to write-set inserts and replaces.
/// - `direction`: sort direction (Forward = ascending, Backward = descending).
/// - `limit`: max results (applied after merge).
///
/// Returns a Vec because the merge requires buffering write-set inserts
/// to merge-sort with snapshot results. For transactions with small write sets
/// (the common case), this is efficient.
pub fn merge_with_writes<I>(
    snapshot: I,
    merge_view: &MergeView<'_>,
    sort_fields: &[FieldPath],
    range_lower: Bound<&[u8]>,
    range_upper: Bound<&[u8]>,
    post_filter: Option<&Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> std::io::Result<Vec<ScanRow>>
where
    I: Iterator<Item = std::io::Result<ScanRow>>;
```

## Implementation Details

### merge_with_writes()

```
1. Build a HashSet<DocId> from deletes for O(1) lookup.
2. Build a HashMap<DocId, &Value> from replaces for O(1) lookup.

3. Consume snapshot iterator, applying delete/replace overlay:
   for each ScanRow from snapshot:
     a. If doc_id is in deletes → skip.
     b. If doc_id is in replaces:
        - Replace doc body with the write-set version.
        - Re-evaluate post_filter against the new body.
        - If filter rejects → skip.
        - Otherwise, keep with the new body.
     c. Otherwise, keep as-is.
   Collect into Vec<ScanRow>.

4. Find write-set inserts that match the scan range:
   for each (doc_id, doc) in inserts:
     a. Extract index sort key: for each field in sort_fields,
        call extract_scalar(doc, field). Encode via encode_key_prefix.
     b. Check if encoded key is within [range_lower, range_upper):
        - key >= range_lower (or range_lower is Unbounded)
        - key < range_upper (or range_upper is Unbounded)
     c. If in range AND post_filter matches → include.

5. Merge the snapshot results and insert results by sort key:
   - Extract sort key for each row (snapshot rows already have correct
     ordering; insert rows need sort key extraction).
   - Merge-sort both lists maintaining direction order.
   - For Forward: ascending sort key order.
   - For Backward: descending sort key order.

6. Apply limit: truncate to limit results.

7. Return merged Vec<ScanRow>.
```

### Sort key extraction

To merge write-set inserts into the correct sort position, we need to extract the index sort key from each document:

```rust
fn extract_sort_key(doc: &serde_json::Value, sort_fields: &[FieldPath]) -> Vec<u8> {
    let scalars: Vec<Scalar> = sort_fields
        .iter()
        .map(|f| extract_scalar(doc, f).unwrap_or(Scalar::Undefined))
        .collect();
    encode_key_prefix(&scalars)
}
```

This produces the same byte prefix used by the secondary index, so sort order is consistent.

### Range check for inserts

```rust
fn key_in_range(key: &[u8], lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    let above_lower = match lower {
        Bound::Unbounded => true,
        Bound::Included(lb) => key >= lb,
        Bound::Excluded(lb) => key > lb,
    };
    let below_upper = match upper {
        Bound::Unbounded => true,
        Bound::Excluded(ub) => key < ub,
        Bound::Included(ub) => key <= ub,
    };
    above_lower && below_upper
}
```

### Why Vec and not Iterator

The merge returns a `Vec` rather than a lazy iterator because:
1. Write-set inserts need to be sorted into the snapshot stream. A true merge-sort iterator is possible but complex (two ordered sources merged on-the-fly).
2. Write sets are typically small (dozens to hundreds of entries). Materializing them is cheap.
3. The snapshot iterator is consumed once and can't be rewound.
4. L6 needs the full result for limit-aware interval tightening (DESIGN.md section 5.6.3).

If performance becomes an issue for large write sets, this can be changed to a merge iterator later.

### Get-by-ID (not handled here)

Per DESIGN.md section 5.4, get-by-ID checks the write set first, then falls through to the primary index. This is simpler and handled directly by L6:

```rust
// In L6 (not L4):
fn get(&self, collection: &str, doc_id: &DocId) -> Result<Option<Value>> {
    // Check write set first
    if let Some(entry) = self.write_set.get(collection_id, doc_id) {
        match entry.op {
            MutationOp::Delete => return Ok(None),
            MutationOp::Insert | MutationOp::Replace => return Ok(entry.body.clone()),
        }
    }
    // Fall through to snapshot
    primary_index.get_at_ts(doc_id, self.begin_ts)
}
```

L4's merge layer handles only scan operations.

### Duplicate handling

A document could appear in both the snapshot (at begin_ts) and the write set (as a replace or delete). The overlay logic in step 3 handles this: snapshot docs whose doc_id is in replaces/deletes are modified/removed. There's no double-counting.

An insert in the write set should NOT appear in the snapshot (it didn't exist at begin_ts). If it does (edge case: insert of a previously deleted doc), the replace path handles it.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Snapshot iteration error | L3 B-tree I/O failure | Propagate |
| Document decode error | Corrupt body in write set | Should not happen (write set has valid JSON) |

## Tests

### Unit tests

1. **No write set**: snapshot with 5 rows, empty MergeView, verify 5 rows unchanged.
2. **Delete overlay**: snapshot has docs A, B, C. Delete B. Verify A and C returned.
3. **Replace overlay**: snapshot has doc A with body `{"x": 1}`. Replace A with `{"x": 2}`. Verify new body.
4. **Replace with filter rejection**: snapshot has doc A matching filter. Replace A with body that fails filter. Verify A excluded.
5. **Replace with filter acceptance**: replace A with body that passes filter. Verify included.
6. **Insert within range**: snapshot empty. Insert doc with sort key in range. Verify included.
7. **Insert outside range**: insert doc with sort key outside range. Verify excluded.
8. **Insert with filter**: insert doc in range but fails filter. Verify excluded.
9. **Insert sort order (forward)**: snapshot has docs at sort keys 1, 3, 5. Insert doc at sort key 2. Verify order is 1, 2, 3, 5.
10. **Insert sort order (backward)**: same setup with Backward direction. Verify order is 5, 3, 2, 1.
11. **Delete + insert same doc_id**: delete A from snapshot, insert A with new body. Verify new body returned (insert takes precedence — handled by replace path).
12. **Limit**: snapshot has 10 rows, insert 5 matching rows, limit=3. Verify 3 results.
13. **All deleted**: snapshot has 3 rows, all deleted. Verify empty result.
14. **Multiple inserts sorted**: insert 3 docs, verify they merge into correct positions.
15. **Empty snapshot + inserts**: no snapshot results, 3 inserts in range. Verify 3 results in order.

### Integration tests (with in-memory storage)

16. **Full pipeline**: insert 10 docs via primary+secondary index. Start a "transaction" (simulated: write set + snapshot scan). Insert 2 more in write set, delete 1. Merge. Verify correct 11 docs in order.
17. **Compound index sort**: index on [A, B]. Snapshot has entries. Insert with A=1, B="z". Verify sort position relative to existing A=1 entries.
