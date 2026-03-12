# exdb-query — Layer 4: Query Engine

Translates user queries into Layer 3 (docstore) operations, evaluates post-filters,
executes scans lazily, and merges write-set mutations for read-your-writes semantics.

## Architecture

The query engine sits between the document store (L3) and the transaction layer (L5/L6):

```
L6 (Database) ─── calls ──→ L4 (Query Engine) ─── drives ──→ L3 (Docstore)
                                │
                                ├── resolve_access   → selects access method
                                ├── execute_scan     → drives L3 streams (async)
                                ├── filter_matches   → post-filter evaluation
                                ├── encode_range     → byte interval encoding
                                └── merge_with_writes → read-your-writes overlay (async)
```

**Depends on:** L1 (Core Types), L3 (Document Store).
**No knowledge of:** L5+ (Transactions, Database). Write-set data is received as
plain slices via `MergeView`, not as L5 types.

## Modules

| Module | Purpose |
|--------|---------|
| `post_filter` | Evaluate `Filter` expressions against JSON documents |
| `range_encoder` | Validate and encode `RangeExpr` into byte intervals |
| `access` | Resolve the access method (IndexScan, TableScan, PrimaryGet) |
| `scan` | Execute access methods via the Source → PostFilter → Terminal async stream pipeline |
| `merge` | Overlay write-set mutations onto snapshot scan stream (async) |

The `Filter` and `RangeExpr` AST types live in `exdb-core::filter` and are
re-exported from this crate for convenience.

## Query Execution Flow

```
1. Caller builds RangeExpr + Filter from the user query
2. resolve_access() validates the range and selects IndexScan or TableScan
3. execute_scan() creates a lazy ScanStream (async)
4. Stream::next().await drives the pipeline:
   Source (L3 SecondaryScanner stream) → PostFilter → Limit check → ScanRow
5. (Optional) merge_with_writes() overlays in-transaction mutations (async, takes Stream)
```

## Usage

### Resolving and executing a query

```rust
use exdb_query::{
    resolve_access, execute_scan, IndexInfo, AccessMethod, Filter, RangeExpr,
};
use exdb_core::types::{CollectionId, IndexId, Scalar};
use exdb_core::field_path::FieldPath;
use exdb_storage::btree::ScanDirection;
use std::collections::HashMap;

// Resolve index metadata (normally from L6 catalog)
let index = IndexInfo {
    index_id: IndexId(1),
    field_paths: vec![FieldPath::single("age")],
    ready: true,
};

// Range: age >= 18 AND age < 65
let range = vec![
    RangeExpr::Gte(FieldPath::single("age"), Scalar::Int64(18)),
    RangeExpr::Lt(FieldPath::single("age"), Scalar::Int64(65)),
];

// Post-filter: status == "active"
let filter = Some(Filter::Eq(
    FieldPath::single("status"),
    Scalar::String("active".into()),
));

// Resolve the access method
let method = resolve_access(
    CollectionId(1),
    &index,
    &range,
    filter,
    ScanDirection::Forward,
    Some(100), // limit
).expect("valid access method");

// Execute against indexes at a read timestamp
let read_ts = 42;
let secondary_indexes: HashMap<IndexId, _> = /* ... */;
let mut stream = execute_scan(
    &method,
    &primary_index,
    &secondary_indexes,
    read_ts,
).await.expect("scan started");

// Read interval is available immediately (before iterating)
let interval = stream.read_interval();

// Lazily consume results via async stream
use tokio_stream::StreamExt;
while let Some(result) = stream.next().await {
    let row = result.expect("no I/O error");
    println!("doc_id={:?} ts={} body={}", row.doc_id, row.version_ts, row.doc);
}
```

### Post-filter evaluation

```rust
use exdb_query::{filter_matches, compare_scalars, Filter};
use exdb_core::types::Scalar;
use exdb_core::field_path::FieldPath;
use serde_json::json;

let doc = json!({"name": "alice", "age": 30, "active": true});

// Simple equality
let f = Filter::Eq(FieldPath::single("name"), Scalar::String("alice".into()));
assert!(filter_matches(&doc, &f));

// Compound filter: age >= 18 AND active == true
let f = Filter::And(vec![
    Filter::Gte(FieldPath::single("age"), Scalar::Int64(18)),
    Filter::Eq(FieldPath::single("active"), Scalar::Boolean(true)),
]);
assert!(filter_matches(&doc, &f));

// Type-strict: int64(30) != float64(30.0)
assert_eq!(
    compare_scalars(&Scalar::Int64(30), &Scalar::Float64(30.0)),
    None, // cross-type → incomparable
);
```

### Range encoding

```rust
use exdb_query::{validate_range, encode_range, RangeExpr};
use exdb_core::types::Scalar;
use exdb_core::field_path::FieldPath;
use std::ops::Bound;

let index_fields = vec![
    FieldPath::single("country"),
    FieldPath::single("age"),
];

// eq(country, "US"), gte(age, 21)
let range = vec![
    RangeExpr::Eq(FieldPath::single("country"), Scalar::String("US".into())),
    RangeExpr::Gte(FieldPath::single("age"), Scalar::Int64(21)),
];

// Validate structure
let shape = validate_range(&index_fields, &range).unwrap();
assert_eq!(shape.eq_count, 1);        // one equality prefix
assert_eq!(shape.range_field, Some(1)); // range on field at position 1
assert!(shape.has_lower);              // gte provides lower bound

// Encode to byte interval
let (lower, upper) = encode_range(&index_fields, &range).unwrap();
// lower = Included(encode("US") || encode(21))
// upper = Excluded(successor_key(encode("US")))
```

### Read-your-writes merge

```rust
use exdb_query::{merge_with_writes, MergeView, ScanRow};
use exdb_core::types::DocId;
use exdb_core::field_path::FieldPath;
use exdb_storage::btree::ScanDirection;
use serde_json::json;
use std::ops::Bound;

// Snapshot results from execute_scan
let snapshot: Vec<std::io::Result<ScanRow>> = vec![/* ... */];

// Write-set mutations decomposed by L6
let inserts = vec![
    (DocId([0u8; 16]), json!({"x": 42})),
];
let deletes = vec![/* doc_ids to exclude */];
let replaces = vec![/* (doc_id, new_body) pairs */];

let merge_view = MergeView {
    inserts: &inserts,
    deletes: &deletes,
    replaces: &replaces,
};

let merged = merge_with_writes(
    tokio_stream::iter(snapshot),
    &merge_view,
    &[FieldPath::single("x")], // sort fields
    Bound::Unbounded,
    Bound::Unbounded,
    None,                        // no post-filter
    ScanDirection::Forward,
    None,                        // no limit
).await.unwrap();
```

## Key Design Decisions

1. **No L5 dependency** — The merge layer receives a `MergeView` (plain slices),
   not L5 `WriteSet`. This keeps the dependency chain strict: L4 → L3 → L2 → L1.

2. **Stream-based scan** — `execute_scan` returns a `ScanStream` implementing
   `Stream<Item = io::Result<ScanRow>>`. Documents are produced lazily through the
   Source → PostFilter → Limit pipeline.

3. **Read interval from access method bounds** — The read interval is computed from the
   `AccessMethod` bounds (not from scan results) and is available before iteration
   via `QueryScanner::read_interval()`. L6 applies limit-aware tightening after
   iteration completes.

4. **Table scan = `_created_at` index scan** — A table scan is an unbounded scan
   on the `_created_at` secondary index, not a primary index scan.

5. **Type-strict comparisons** — `int64(5) != float64(5.0)`. `compare_scalars`
   returns `None` for cross-type comparisons.

6. **Query ID assigned by L6** — `ReadIntervalInfo` does not carry a `query_id`.
   L6 assigns an incremental `u32` when recording the read set entry.

7. **Read set limits enforced by L6** — `ScanStats` tracks scanned docs/bytes,
   but L4 does not enforce limits. L6 checks after each scan and aborts the
   transaction if any threshold is exceeded.

## Range Expression Rules

Range expressions must follow index field order:

```
Index [A, B, C]:  valid patterns
  eq(A)                           → prefix scan on A
  eq(A), eq(B)                    → prefix scan on A,B
  eq(A), eq(B), eq(C)            → point scan
  eq(A), gte(B, v)               → range on B within A prefix
  eq(A), gte(B, lo), lt(B, hi)  → bounded range on B within A prefix
  gte(A, v)                       → range on first field

Invalid:
  eq(B)                            → skips A (FieldOutOfOrder)
  gte(A, v), eq(B, w)            → Eq after range (EqAfterRange)
  gt(A, 5), gte(A, 3)            → duplicate lower bound (DuplicateBound)
```

## Filter Expression Types

| Variant | Semantics |
|---------|-----------|
| `Eq(field, val)` | field == val (same type required) |
| `Ne(field, val)` | field != val (true if types differ) |
| `Gt/Gte/Lt/Lte` | ordered comparison (false if types differ) |
| `In(field, vals)` | field matches any value in the list |
| `And(filters)` | all must match (empty = true) |
| `Or(filters)` | any must match (empty = false) |
| `Not(filter)` | negation |

Missing fields extract as `Scalar::Undefined`. A missing field is not equal to `Null`.
