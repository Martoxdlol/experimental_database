# Query Engine Implementation Plan — Overview

## Scope

Layer 4: Query Engine. Translates user queries into Layer 3 operations, evaluates post-filters, merges write-set mutations for read-your-writes, and produces read set intervals for Layer 5 OCC validation.

**Depends on:** Layer 1 (Core Types) and Layer 3 (Document Store & Indexing).
**No knowledge of:** Layer 5+ (Transactions, Database). L5 types like `WriteSet` and `ReadSet` are not imported. The merge layer receives decomposed plain data from L5/L6.

## Prerequisite: `filter.rs` in L1

Before implementing any L4 module, add `filter.rs` to `exdb-core` containing pure AST types: `Filter` (post-filter) and `RangeExpr` (index range). These are pure data definitions with no evaluation logic.

## Sub-Layer Organization (Bottom-Up Build Order)

| # | Sub-Layer | File | Dependencies | Testable Alone? |
|---|-----------|------|-------------|-----------------|
| Q0 | Filter AST (L1) | `core/filter.rs` | L1 (types, field_path) | Yes |
| Q1 | Post-Filter | `query/post_filter.rs` | Q0, L1 (encoding) | Yes |
| Q2 | Range Encoder | `query/range_encoder.rs` | Q0, L1, L3 (key_encoding) | Yes |
| Q3 | Planner | `query/planner.rs` | Q1, Q2 | Yes |
| Q4 | Scan Execution | `query/scan.rs` | Q1, Q2, Q3, L3 (PrimaryIndex, SecondaryIndex) | Yes (with L2 in-memory) |
| Q5 | Write-Set Merge | `query/merge.rs` | Q1, Q4 | Yes |

## Implementation Phases

### Phase A: Foundations (Q0 + Q1) — No L3 Dependencies
Add `Filter` and `RangeExpr` AST to L1. Build and fully test the post-filter evaluator. These are pure functions operating on JSON documents and scalars.

### Phase B: Range Encoding (Q2) — Depends on L3 key_encoding
Build and test the range encoder that converts `[RangeExpr]` into `(Bound<Vec<u8>>, Bound<Vec<u8>>)` byte intervals. Depends on L3 `encode_scalar`, `encode_key_prefix`, `successor_key`, `prefix_successor`.

### Phase C: Planner (Q3) — Depends on Phase B
Build the query planner that selects access method and produces a `QueryPlan`. Light module — mostly wiring.

### Phase D: Scan Execution + Merge (Q4 + Q5) — Depends on Phase C + L3
Build the scan executor that drives L3 iterators through the Source → PostFilter → Terminal pipeline. Then build the write-set merge layer. Both require L3 `PrimaryIndex` and `SecondaryIndex` for integration tests.

## File Map

```
core/
  filter.rs             — Q0: Filter and RangeExpr AST types

query/
  lib.rs                — re-exports
  post_filter.rs        — Q1: filter evaluation against documents
  range_encoder.rs      — Q2: range expressions → byte intervals
  planner.rs            — Q3: query planning
  scan.rs               — Q4: scan execution pipeline
  merge.rs              — Q5: read-your-writes merge
```

## Dependency Direction

```
Q5 (Merge) ──→ Q4, Q1
Q4 (Scan) ──→ Q3, Q1, L3
Q3 (Planner) ──→ Q2, Q1
Q2 (RangeEncoder) ──→ Q0, L3 (key_encoding only)
Q1 (PostFilter) ──→ Q0, L1 (encoding)
Q0 (Filter AST) ──→ L1 (types, field_path)
```

No circular dependencies. Each sub-layer depends only on sub-layers below it and on L1/L3.

## Public Facade

Layer 4 exposes individual functions and types composed by L6 (Database):

```rust
// Filter AST (from L1, re-exported for convenience)
pub use exdb_core::filter::{Filter, RangeExpr};

// Post-filter evaluation
pub use post_filter::{filter_matches, compare_scalars};

// Range encoding
pub use range_encoder::{encode_range, validate_range, RangeShape, RangeError};

// Query planning
pub use planner::{plan_query, QueryPlan, IndexInfo, PlanError};

// Scan execution
pub use scan::{execute_scan, QueryScanner, ScanRow, ReadIntervalInfo, ScanStats};

// Write-set merge
pub use merge::{merge_with_writes, MergeView};
```

## Timestamp Flow

All queries execute at a specific `read_ts: Ts` — the transaction's `begin_ts`. This timestamp flows as follows:

- **Q0-Q3**: No timestamp needed. Filter ASTs, post-filter evaluation, range encoding, and planning are timestamp-independent.
- **Q4 (`execute_scan`)**: Receives `read_ts` and passes it to `SecondaryIndex::scan_at_ts(lower, upper, read_ts, direction)` and `PrimaryIndex::get_at_ts(doc_id, read_ts)`. All MVCC visibility is resolved at this timestamp.
- **Q5 (`merge_with_writes`)**: No timestamp needed. The snapshot iterator already has `read_ts` baked in. Write-set documents are always visible (they're in-transaction mutations).

The `read_ts` is provided by L6 (which owns the transaction state). L4 has no concept of "current time" or timestamp generation.

## Key Design Decisions

1. **No L5 dependency**: L4 does not import `WriteSet`, `ReadSet`, or any L5 type. The merge layer receives a `MergeView` (plain slices of inserts/deletes/replaces) decomposed by L6 before calling into L4. This keeps the dependency chain strict: L4 → L3 → L2 → L1.

2. **Iterator-based scan**: `execute_scan` returns a `QueryScanner` implementing `Iterator`, not a `Vec`. Documents are produced lazily through the Source → PostFilter → Limit pipeline. This avoids materializing large result sets and matches the DESIGN.md description of the source as a "stream of documents."

3. **Read set interval from plan bounds**: The read interval is computed from the `QueryPlan` bounds, not from scan results. It is available before iteration begins via `QueryScanner::read_interval()`. Limit-aware tightening (DESIGN.md section 5.6.3) is applied after iteration completes by L6.

4. **Table scan = `_created_at` index scan**: Per DESIGN.md section 4.2.3, a table scan is an unbounded scan on the `_created_at` secondary index. It is NOT a primary index scan.

5. **Type-strict comparisons**: Per DESIGN.md section 1.6, `int64(5) != float64(5.0)`. `compare_scalars` returns `None` for cross-type comparisons. This applies to both post-filters and range expressions.

6. **Query ID assigned by L6, not L4**: Per DESIGN.md section 4.6, every read operation gets an incremental `query_id: u32`. L4's `ReadIntervalInfo` does not carry this — L6 assigns it when recording the read set entry. This keeps L4 stateless across queries.

7. **Read set size limits enforced by L6**: Per DESIGN.md section 5.6.5, limits on intervals, scanned bytes, and scanned docs are transaction-scoped. L4 tracks `ScanStats` but does not enforce limits — L6 checks after each scan and aborts the transaction if exceeded.
