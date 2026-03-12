# Q3: Planner

## Purpose

Selects the access method (primary get, index scan, table scan) and produces a `QueryPlan` struct consumed by the scan executor (Q4). The planner validates that the target index is ready, encodes range expressions into byte intervals, and attaches the post-filter and limit.

Corresponds to DESIGN.md section 4.5 steps 1-3.

## Dependencies

- **Q1 (`query/post_filter.rs`)**: `Filter` (passed through to plan)
- **Q2 (`query/range_encoder.rs`)**: `encode_range`, `validate_range`, `RangeError`
- **Q0 (`core/filter.rs`)**: `Filter`, `RangeExpr`
- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`, `DocId`
- **L1 (`core/field_path.rs`)**: `FieldPath`
- **L2 (`storage/btree.rs`)**: `ScanDirection`

No L3, L5, or L6 dependency. The planner does NOT access indexes or the catalog directly — it receives resolved metadata from the caller (L6).

## Rust Types

```rust
use exdb_core::types::{CollectionId, IndexId, DocId};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::{Filter, RangeExpr};
use exdb_storage::btree::ScanDirection;
use crate::range_encoder::{encode_range, RangeError};
use std::ops::Bound;

/// Index metadata provided by the caller (L6 catalog cache).
/// L4 does not depend on the catalog — the caller resolves this.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_id: IndexId,
    pub field_paths: Vec<FieldPath>,
    pub ready: bool, // false if index is still Building
}

/// Query plan produced by the planner.
#[derive(Debug)]
pub enum QueryPlan {
    /// Point lookup by document ID (DESIGN.md section 4.2.1).
    PrimaryGet {
        collection_id: CollectionId,
        doc_id: DocId,
    },

    /// Range scan on a secondary index (DESIGN.md section 4.2.2).
    IndexScan {
        collection_id: CollectionId,
        index_id: IndexId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },

    /// Full collection scan via _created_at index (DESIGN.md section 4.2.3).
    /// Semantically equivalent to IndexScan with Unbounded range.
    TableScan {
        collection_id: CollectionId,
        index_id: IndexId,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
}

/// Errors from query planning.
#[derive(Debug)]
pub enum PlanError {
    IndexNotReady,
    Range(RangeError),
}

/// Plan a query (DESIGN.md section 4.5).
///
/// The caller (L6) resolves:
/// - collection name → collection_id
/// - index name → IndexInfo (from catalog cache)
///
/// The planner:
/// 1. Checks index readiness.
/// 2. Validates and encodes range expressions.
/// 3. Determines if this is a table scan (empty range) or index scan.
/// 4. Returns the QueryPlan.
pub fn plan_query(
    collection_id: CollectionId,
    index: &IndexInfo,
    range: &[RangeExpr],
    filter: Option<Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<QueryPlan, PlanError>;
```

## Implementation Details

### plan_query()

```rust
pub fn plan_query(
    collection_id: CollectionId,
    index: &IndexInfo,
    range: &[RangeExpr],
    filter: Option<Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<QueryPlan, PlanError> {
    // 1. Check index readiness
    if !index.ready {
        return Err(PlanError::IndexNotReady);
    }

    // 2. Encode range expressions
    let (lower, upper) = encode_range(&index.field_paths, range)
        .map_err(PlanError::Range)?;

    // 3. Determine plan type
    if matches!((&lower, &upper), (Bound::Unbounded, Bound::Unbounded)) && range.is_empty() {
        // Empty range → table scan
        Ok(QueryPlan::TableScan {
            collection_id,
            index_id: index.index_id,
            direction,
            post_filter: filter,
            limit,
        })
    } else {
        Ok(QueryPlan::IndexScan {
            collection_id,
            index_id: index.index_id,
            lower,
            upper,
            direction,
            post_filter: filter,
            limit,
        })
    }
}
```

### PrimaryGet planning

`PrimaryGet` is not produced by `plan_query`. It is constructed directly by L6 when the user calls `tx.get(collection, doc_id)`. L6 builds the `PrimaryGet` variant and passes it to `execute_scan` (Q4), which handles it as a single-element iterator. The query planner handles only `query` operations — `get` bypasses it.

### Why TableScan is separate from IndexScan

Although `TableScan` is semantically an `IndexScan` with unbounded range, keeping it as a separate variant is useful because:
- The read set recording differs: table scan records `[MIN, Unbounded)` (DESIGN.md section 5.6.2).
- L6 can log/instrument table scans differently (they're the most expensive operation).
- The scan executor can skip bound encoding for table scans.

### Range vs post-filter split is done by the caller

Per DESIGN.md section 7.7, the API keeps `range` and `filter` as separate fields in the query message. The **user** (or L8 wire protocol parser) decides which predicates go into range expressions and which go into the post-filter. L4 does NOT auto-split — it receives pre-split `range: &[RangeExpr]` and `filter: Option<Filter>` and validates the range expressions. If the range is invalid, it returns `PlanError::Range(...)` and the caller can report it to the user.

This means for a query like "A between 1 and 10 AND B == 5" on index [A, B, C]:
- The user sends: `range: [gte(A, 1), lte(A, 10)]`, `filter: eq(B, 5)`
- L4 validates the range (valid) and attaches the filter as post-filter.
- If the user mistakenly puts `eq(B, 5)` in the range as `[gte(A,1), lte(A,10), eq(B,5)]`, L4 rejects with `EqAfterRange`.

### Note on `_created_at` vs `_id` built-in indexes

Per DESIGN.md section 1.11, every collection has two built-in indexes:
- `_id`: primary index (clustered B-tree, keyed by doc_id).
- `_created_at`: secondary index on the `_created_at` field.

A table scan uses the `_created_at` index. A query on the `_id` index with `eq(_id, some_id)` is planned as an `IndexScan` on the `_id` secondary index (NOT as a `PrimaryGet`). `PrimaryGet` is reserved for the `get` API which takes a raw doc_id, bypassing the query pipeline.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| IndexNotReady | Index is in Building state (section 3.7) | Return PlanError::IndexNotReady |
| Range validation failure | Invalid range expressions | Return PlanError::Range(RangeError) |

## Tests

1. **Index scan basic**: plan with `[eq(A, 1)]`, verify IndexScan with correct bounds.
2. **Index scan with filter**: plan with range + filter, verify post_filter is attached.
3. **Index scan with limit**: plan with limit=10, verify limit is attached.
4. **Table scan**: plan with empty range, verify TableScan variant.
5. **Table scan with filter**: empty range + filter, verify TableScan with post_filter.
6. **Table scan with limit**: empty range + limit, verify TableScan with limit.
7. **Direction**: plan with Forward and Backward, verify direction propagated.
8. **Index not ready**: plan with ready=false, verify PlanError::IndexNotReady.
9. **Invalid range**: plan with out-of-order fields, verify PlanError::Range.
10. **Range with both bounds**: `[gte(A, 5), lt(A, 10)]`, verify IndexScan with correct lower/upper.
11. **Compound range**: `[eq(A, 1), gt(B, "m")]` on `[A, B]`, verify bounds.
