# Layer 4: Query Engine

**Layer purpose:** Query planning, range encoding, scan execution, post-filter evaluation, read-your-writes merge, and read set interval generation. Translates user queries into Layer 3 operations and produces read set intervals for Layer 5.

## Modules

### `planner.rs` — Query Planning

**WHY HERE:** Selects the access method (primary get, index scan, table scan) based on the query parameters. Requires knowledge of indexes and their states — domain-aware decision making.

```rust
use crate::core::types::{CollectionId, IndexId, DocId, Ts, FieldPath};
use crate::core::filter::{Filter, RangeExpr};

/// Query plan produced by the planner
pub enum QueryPlan {
    /// Point lookup by document ID
    PrimaryGet {
        collection_id: CollectionId,
        doc_id: DocId,
    },
    /// Range scan on a named index
    IndexScan {
        collection_id: CollectionId,
        index_id: IndexId,
        lower_bound: Vec<u8>,       // encoded byte interval
        upper_bound: Bound<Vec<u8>>,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
    /// Full collection scan via _created_at index
    TableScan {
        collection_id: CollectionId,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
}

/// Plan a query based on the specified index and range expressions
pub fn plan_query(
    catalog: &CatalogCache,
    collection: &str,
    index_name: &str,
    range: &[RangeExpr],
    filter: Option<&Filter>,
    order: ScanDirection,
    limit: Option<usize>,
) -> Result<QueryPlan>;
```

### `range_encoder.rs` — Range Expression to Byte Interval

**WHY HERE:** Translates typed range expressions into byte intervals on the encoded key space. Requires knowledge of order-preserving key encoding (Layer 3) and index field definitions.

```rust
use crate::core::filter::RangeExpr;
use crate::docstore::key_encoding;

/// Encode range expressions into a contiguous byte interval
/// Follows the rules from section 4.3
pub fn encode_range(
    field_paths: &[FieldPath],
    range: &[RangeExpr],
) -> Result<(Vec<u8>, Bound<Vec<u8>>)>;
// Returns (lower_bound, upper_bound)

/// Validate range expressions against index field order
pub fn validate_range(
    field_paths: &[FieldPath],
    range: &[RangeExpr],
) -> Result<()>;
```

### `scan.rs` — Scan Execution

**WHY HERE:** Orchestrates the Source → PostFilter → Terminal pipeline. Drives Layer 3 iterators and collects results.

```rust
use crate::docstore::{PrimaryIndex, SecondaryIndex};

/// Execute a query plan and return matching documents
pub fn execute_query(
    plan: &QueryPlan,
    primary_index: &PrimaryIndex,
    secondary_indexes: &HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
    write_set: Option<&WriteSet>,  // for read-your-writes
) -> Result<QueryResult>;

pub struct QueryResult {
    pub docs: Vec<serde_json::Value>,
    pub read_intervals: Vec<ReadInterval>,  // for read set
    pub scanned_docs: usize,
    pub scanned_bytes: usize,
}
```

### `post_filter.rs` — Filter Evaluation

**WHY HERE:** Evaluates arbitrary filter expressions against documents. Requires scalar extraction and comparison logic — domain-aware evaluation.

```rust
use crate::core::types::Scalar;
use crate::core::filter::Filter;
use crate::core::encoding::extract_scalar;

/// Evaluate a filter expression against a document
pub fn filter_matches(doc: &serde_json::Value, filter: &Filter) -> bool;

/// Compare two scalars (type-strict, per section 1.6)
pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Option<Ordering>;
```

### `merge.rs` — Read-Your-Own-Writes Merge

**WHY HERE:** Merges snapshot results with the in-memory write set to provide read-your-writes within a transaction. Requires knowledge of write set structure (domain-specific).

```rust
use crate::tx::WriteSet;

/// Merge snapshot scan results with write set for read-your-writes
/// - Documents inserted in write set that match the query: include
/// - Documents deleted in write set: exclude
/// - Documents modified in write set: use write set version
pub fn merge_with_write_set(
    snapshot_results: Vec<(DocId, serde_json::Value)>,
    write_set: &WriteSet,
    collection_id: CollectionId,
    filter: Option<&Filter>,
) -> Vec<(DocId, serde_json::Value)>;
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `plan_query` | L5 (transaction commit), L7 (query message handling) | Query planning |
| `execute_query` | L7 (query message handling) | Full query execution |
| `QueryResult.read_intervals` | L5 (read set recording) | OCC and subscriptions |
| `filter_matches` | L4 internal (post-filter), L5 (subscription eval) | Filter evaluation |
| `encode_range` | L5 (read set interval construction) | Byte interval generation |
