# Layer 4: Query Engine

**Layer purpose:** Query planning, range encoding, scan execution, post-filter evaluation, read-your-writes merge, and read set interval generation. Translates user queries into Layer 3 operations and produces read set intervals for Layer 5.

**Depends on:** Layer 1 (Core Types), Layer 3 (Document Store & Indexing).
**No knowledge of:** Layer 5+ (Transactions, Database). L5 types (`WriteSet`, `ReadSet`) are NOT imported — merge receives decomposed plain data from L5/L6.

## Prerequisite: `filter.rs` in L1 (Core Types)

Before implementing L4, add `filter.rs` to `exdb-core`. This module contains pure AST types with no logic:

```rust
use crate::field_path::FieldPath;
use crate::types::Scalar;

/// Post-filter expression AST (DESIGN.md sections 4.4, 7.7.2).
/// Evaluated against documents after index scan.
pub enum Filter {
    Eq(FieldPath, Scalar),
    Ne(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
    In(FieldPath, Vec<Scalar>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Index range expression (DESIGN.md sections 4.3, 7.7.1).
/// Defines a contiguous interval on an index's key space.
///
/// Validation rules (enforced by range_encoder):
/// 1. Predicates reference index fields in order.
/// 2. Zero or more Eq on leading fields (equality prefix).
/// 3. At most one lower bound (Gt/Gte) and one upper bound (Lt/Lte) on the next field.
/// 4. No predicates after a range bound.
pub enum RangeExpr {
    Eq(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
}
```

## Modules

### Q1: `post_filter.rs` — Filter Evaluation

**WHY HERE:** Evaluates arbitrary filter expressions against documents. Requires scalar extraction and comparison logic — domain-aware evaluation. No dependency on L3 scan types.

```rust
use exdb_core::types::Scalar;
use exdb_core::filter::Filter;
use exdb_core::encoding::extract_scalar;
use std::cmp::Ordering;

/// Evaluate a filter expression against a document (DESIGN.md section 4.4).
/// Type-strict: int64(5) != float64(5.0) per section 1.6.
pub fn filter_matches(doc: &serde_json::Value, filter: &Filter) -> bool;

/// Compare two scalars. Returns None if types differ (cross-type comparison
/// is not supported per DESIGN.md section 1.6).
pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Option<Ordering>;
```

### Q2: `range_encoder.rs` — Range Expression to Byte Interval

**WHY HERE:** Translates typed range expressions into byte intervals on the encoded key space. Requires knowledge of order-preserving key encoding (Layer 3) and index field definitions.

```rust
use exdb_core::filter::RangeExpr;
use exdb_core::field_path::FieldPath;
use exdb_docstore::key_encoding;
use std::ops::Bound;

/// Validate range expressions against index field order (DESIGN.md section 4.3).
///
/// Rules:
/// 1. Each RangeExpr must reference an index field by position (left to right).
/// 2. Eq predicates form a contiguous prefix.
/// 3. After the first non-Eq (Gt/Gte/Lt/Lte), only bounds on that same field are allowed.
/// 4. At most one lower bound and one upper bound per range field.
/// 5. No predicates on fields after the range field.
///
/// Returns the split point: (equality_count, has_lower, has_upper, range_field_index).
pub fn validate_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<RangeShape, RangeError>;

pub struct RangeShape {
    pub eq_count: usize,            // number of leading Eq predicates
    pub range_field: Option<usize>, // index of the range field (if any)
    pub has_lower: bool,
    pub has_upper: bool,
}

/// Encode validated range expressions into a contiguous byte interval
/// on the secondary index key space (DESIGN.md section 4.3).
///
/// The returned bounds are on the VALUE PREFIX only (no doc_id/inv_ts suffix).
/// The caller (scan.rs) passes these to SecondaryIndex::scan_at_ts which
/// handles MVCC resolution within the interval.
///
/// Encoding rules (see Q2 for full details):
/// - Equality prefix: encode all Eq values into a fixed prefix.
/// - Lower bound:
///   - No range field: Included(eq_prefix)
///   - Gte(v): Included(eq_prefix || encode_scalar(v))
///   - Gt(v):  Included(eq_prefix || successor_key(encode_scalar(v)))
/// - Upper bound:
///   - No range field: Excluded(successor_key(eq_prefix))
///   - Lt(v):  Excluded(eq_prefix || encode_scalar(v))
///   - Lte(v): Excluded(eq_prefix || successor_key(encode_scalar(v)))
///   - Range field with lower only: Excluded(successor_key(eq_prefix))
/// - Empty range: (Unbounded, Unbounded) — full index scan.
/// - Empty eq_prefix with no bound: Unbounded (not successor_key of empty).
pub fn encode_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), RangeError>;

pub enum RangeError {
    FieldNotInIndex { field: FieldPath },
    FieldOutOfOrder { field: FieldPath, expected_position: usize },
    EqAfterRange { field: FieldPath },
    DuplicateBound { field: FieldPath, bound_kind: &'static str },
}
```

### Q3: `access.rs` — Access Method Resolution

**WHY HERE:** Selects the access method (primary get, index scan, table scan) based on the query parameters. Requires knowledge of indexes and their states — domain-aware decision making.

```rust
use exdb_core::types::{CollectionId, IndexId, DocId};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::{Filter, RangeExpr};
use exdb_storage::btree::ScanDirection;
use std::ops::Bound;

/// Resolved index metadata needed by the access resolver (L6 catalog cache).
/// Provided by the catalog cache in L6. L4 does not depend on L6 —
/// the caller constructs this from catalog data before calling resolve_access().
pub struct IndexInfo {
    pub index_id: IndexId,
    pub field_paths: Vec<FieldPath>,
    pub ready: bool,
}

/// Resolved access method (DESIGN.md section 4.5).
pub enum AccessMethod {
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
    /// Equivalent to IndexScan with unbounded range on _created_at.
    TableScan {
        collection_id: CollectionId,
        index_id: IndexId,      // the _created_at index
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
}

/// Plan a query. The caller (L6) resolves collection name → CollectionId
/// and index name → IndexInfo before calling this.
pub fn resolve_access(
    collection_id: CollectionId,
    index: &IndexInfo,
    range: &[RangeExpr],
    filter: Option<Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<AccessMethod, AccessError>;

pub enum AccessError {
    IndexNotReady,
    Range(RangeError),
}
```

### Q4: `scan.rs` — Scan Execution

**WHY HERE:** Orchestrates the Source → PostFilter → Terminal pipeline. Drives Layer 3 iterators, applies post-filters, enforces limits, and computes read set intervals.

```rust
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_core::filter::Filter;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use std::ops::Bound;

/// Execute a query plan against the indexes at the given read timestamp.
///
/// Returns a stream that lazily produces documents. The stream:
/// 1. Drives the L3 scanner (PrimaryScanner or SecondaryScanner).
/// 2. Fetches document bodies from the primary index.
/// 3. Decodes JSON (via exdb_core::encoding::decode_document).
/// 4. Evaluates the post-filter (if any).
/// 5. Stops after `limit` matching documents (if set).
///
/// The read set interval is computed from the plan bounds (not from results)
/// and is available immediately via read_interval().
pub async fn execute_scan(
    plan: &AccessMethod,
    primary_index: &PrimaryIndex,
    secondary_indexes: &HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
) -> Result<QueryScanStream>;

/// Lazy stream over query results.
pub struct QueryScanStream { /* ... */ }

impl Stream for QueryScanStream {
    type Item = Result<ScanRow>;
}

/// A single result row from a query scan.
pub struct ScanRow {
    pub doc_id: DocId,
    pub version_ts: Ts,
    pub doc: serde_json::Value,
}

impl QueryScanStream {
    /// The read set interval for this scan, computed from the plan bounds.
    /// Available before iteration begins.
    pub fn read_interval(&self) -> ReadIntervalInfo;

    /// Stats accumulated during iteration.
    pub fn stats(&self) -> ScanStats;
}

pub struct ReadIntervalInfo {
    pub collection_id: CollectionId,
    pub index_id: IndexId,
    pub lower: Vec<u8>,            // always Included (convention per DESIGN.md 5.6.2)
    pub upper: Bound<Vec<u8>>,     // Excluded or Unbounded
}

pub struct ScanStats {
    pub scanned_docs: usize,
    pub scanned_bytes: usize,
    pub returned_docs: usize,
}
```

### Q5: `merge.rs` — Read-Your-Own-Writes Merge

**WHY HERE:** Merges snapshot scan results with buffered mutations to provide read-your-writes within a transaction (DESIGN.md section 5.4). L4 does NOT import L5 `WriteSet` — the caller decomposes the write set into a `MergeView` before calling merge.

```rust
use exdb_core::types::{DocId, CollectionId};
use exdb_core::filter::Filter;
use exdb_core::field_path::FieldPath;
use exdb_storage::btree::ScanDirection;
use std::ops::Bound;

/// A view of buffered mutations for one collection, provided by L5/L6.
/// L4 does not depend on L5 types.
pub struct MergeView<'a> {
    pub inserts: &'a [(DocId, serde_json::Value)],
    pub deletes: &'a [DocId],
    pub replaces: &'a [(DocId, serde_json::Value)],
}

/// Merge snapshot scan results with buffered mutations (DESIGN.md section 5.4).
///
/// Rules:
/// - Snapshot docs whose doc_id is in `deletes`: excluded.
/// - Snapshot docs whose doc_id is in `replaces`: replaced with new body.
///   The replacement is re-evaluated against the post-filter.
/// - Docs in `inserts` that match the range AND post-filter: included.
/// - Result is sorted by the index sort key (extracted via sort_fields).
///
/// `sort_fields`: the index field paths, used to extract sort keys from
/// write-set documents so they merge into the correct position.
/// `range_lower`/`range_upper`: the scan bounds (value prefix only),
/// used to check if write-set inserts fall within the scan range.
pub async fn merge_with_writes<S>(
    snapshot: S,
    merge_view: &MergeView<'_>,
    sort_fields: &[FieldPath],
    range_lower: Bound<&[u8]>,
    range_upper: Bound<&[u8]>,
    post_filter: Option<&Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> std::io::Result<Vec<ScanRow>>
where
    S: Stream<Item = std::io::Result<ScanRow>>;
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `resolve_access` | L6 (transaction query method) | Query planning |
| `execute_scan` | L6 (transaction query method) | Scan execution (returns stream, async) |
| `merge_with_writes` | L6 (read-your-writes in mutation txns) | Write set overlay |
| `filter_matches` | L4 internal (post-filter), L6 (subscription eval) | Filter evaluation |
| `compare_scalars` | L4 internal, L6 | Scalar comparison |
| `encode_range` | L4 internal (access resolver), L6 (read set construction) | Byte interval generation |
| `validate_range` | L4 internal (access resolver) | Range expression validation |
| `ReadIntervalInfo` | L5/L6 (read set recording) | OCC conflict surface |
