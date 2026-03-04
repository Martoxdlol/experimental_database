# Layer 4: Query Engine

## Purpose

Translates high-level query requests into index operations. Implements the three-stage pipeline: Source → Post-Filter → Terminal. Produces read set intervals for OCC/subscriptions.

## Sub-Modules

### `query/planner.rs` — Query Planning

```rust
pub enum QueryPlan {
    PrimaryGet {
        collection_id: CollectionId,
        doc_id: DocId,
    },
    IndexScan {
        collection_id: CollectionId,
        index_id: IndexId,
        lower_bound: Vec<u8>,          // Encoded key (inclusive)
        upper_bound: Bound<Vec<u8>>,   // Excluded or Unbounded
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
    TableScan {
        collection_id: CollectionId,
        post_filter: Option<Filter>,
        limit: Option<usize>,
        direction: ScanDirection,
    },
}

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

### `query/range.rs` — Index Range Expression Encoding

Converts `RangeExpr` predicates into byte interval bounds.

```rust
pub struct IndexRange {
    pub lower: Vec<u8>,
    pub upper: Bound<Vec<u8>>,
}

pub fn encode_range(
    index_fields: &[FieldPath],
    range_exprs: &[RangeExpr],
) -> Result<IndexRange>;

// Validates range expression ordering rules (§4.3)
pub fn validate_range_exprs(
    index_fields: &[FieldPath],
    range_exprs: &[RangeExpr],
) -> Result<()>;
```

### `query/scan.rs` — Scan Execution

```rust
pub struct QueryResult {
    pub query_id: QueryId,
    pub docs: Vec<Document>,
    pub read_intervals: Vec<ReadInterval>,  // For read set
}

pub fn execute_query(
    plan: &QueryPlan,
    read_ts: Ts,
    query_id: QueryId,
    primary: &PrimaryIndex,
    secondary_indexes: &HashMap<IndexId, SecondaryIndex>,
    write_set: Option<&WriteSet>,  // For read-your-writes
) -> Result<QueryResult>;
```

### `query/filter.rs` — Post-Filter Evaluation

```rust
pub fn filter_matches(doc: &Document, filter: &Filter) -> bool;

// Internal: extract field + compare
fn evaluate_comparison(doc: &Document, path: &FieldPath, op: CompareOp, value: &Scalar) -> bool;
```

## Interfaces

### Depends On (lower layers)

| Layer | Interface Used |
|---|---|
| Layer 3 | `PrimaryIndex::get_at_ts()`, `SecondaryIndex::scan_range_at_ts()`, `PrimaryIndex::scan_at_ts()` |
| Layer 2 | `CatalogCache` (resolve collection name → ID, index name → meta) |
| Layer 1 | `Filter`, `RangeExpr`, `Scalar`, `FieldPath`, `encode_scalar()` |

### Exposes To (higher layers)

| Consumer | Interface |
|---|---|
| Layer 5 (Transactions) | `plan_query()`, `execute_query()` — called within transaction context |
| Layer 7 (Protocol) | Indirectly via transaction sessions |

## Query Pipeline Diagram

```
  Client query message
         │
         ▼
  ┌──────────────┐
  │ plan_query() │  Resolve index, validate range, encode bounds
  └──────┬───────┘
         │ QueryPlan
         ▼
  ┌──────────────┐
  │ SOURCE       │  PrimaryGet / IndexScan / TableScan
  │              │  → stream of (doc_id, document) in key order
  └──────┬───────┘
         │ documents
         ▼
  ┌──────────────┐
  │ POST-FILTER  │  filter_matches(doc, filter)
  │ (optional)   │  → skip non-matching docs
  └──────┬───────┘
         │ filtered docs
         ▼
  ┌──────────────┐
  │ TERMINAL     │  collect / first / limit(N)
  │              │  → stop when limit reached
  └──────┬───────┘
         │
         ▼
  ┌──────────────────┐
  │ ReadInterval     │  Record scanned byte range
  │ (for read set)   │  Apply limit-aware tightening
  └──────────────────┘
         │
         ▼
     QueryResult { query_id, docs, read_intervals }
```

## Read-Your-Writes Integration

When executing within a write transaction, the scan merges results from the snapshot with the write set:

```
  Snapshot at read_ts   +   WriteSet
         │                      │
         ▼                      ▼
  ┌──────────────────────────────────┐
  │        Merge Strategy            │
  │  • Inserted docs: include       │
  │  • Deleted docs: exclude        │
  │  • Modified docs: use write set │
  └──────────────────────────────────┘
```
