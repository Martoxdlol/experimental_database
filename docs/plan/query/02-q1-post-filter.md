# Q1: Post-Filter Evaluation

## Purpose

Evaluates arbitrary `Filter` expressions against JSON documents. Used by the scan executor (Q4) to apply post-filters, and by L6 for subscription evaluation. Implements type-strict comparisons per DESIGN.md section 1.6.

Corresponds to DESIGN.md section 4.4.

## Dependencies

- **Q0 (`core/filter.rs`)**: `Filter`, `RangeExpr`
- **L1 (`core/types.rs`)**: `Scalar`
- **L1 (`core/encoding.rs`)**: `extract_scalar`
- **L1 (`core/field_path.rs`)**: `FieldPath`

No L2 or L3 dependency. Pure functions operating on JSON values and scalars.

## Rust Types

```rust
use exdb_core::filter::Filter;
use exdb_core::types::Scalar;
use exdb_core::encoding::extract_scalar;
use exdb_core::field_path::FieldPath;
use std::cmp::Ordering;

/// Evaluate a filter expression against a document (DESIGN.md section 4.4).
///
/// Extracts field values as scalars and compares type-strictly.
/// A missing field extracts as Scalar::Undefined.
pub fn filter_matches(doc: &serde_json::Value, filter: &Filter) -> bool;

/// Compare two scalars following DESIGN.md section 1.6 type ordering.
///
/// Returns None if types differ — cross-type comparison is not supported.
/// Within the same type:
///   - Int64: standard numeric ordering
///   - Float64: IEEE 754 with NaN sorting last
///   - Boolean: false < true
///   - String: lexicographic UTF-8 byte order
///   - Bytes: lexicographic byte order
///   - Null: equal to Null
///   - Undefined: equal to Undefined
///   - Id: compared by Crockford Base32 string (same as String)
pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Option<Ordering>;
```

## Implementation Details

### compare_scalars()

```rust
fn type_tag(s: &Scalar) -> u8 {
    match s {
        Scalar::Undefined => 0,
        Scalar::Null => 1,
        Scalar::Int64(_) => 2,
        Scalar::Float64(_) => 3,
        Scalar::Boolean(_) => 4,
        Scalar::String(_) => 5,
        Scalar::Bytes(_) => 6,
        Scalar::Id(_) => 5, // Id compares as String
    }
}

pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Option<Ordering> {
    if type_tag(a) != type_tag(b) {
        return None; // cross-type comparison not supported
    }
    match (a, b) {
        (Scalar::Undefined, Scalar::Undefined) => Some(Ordering::Equal),
        (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
        (Scalar::Int64(a), Scalar::Int64(b)) => Some(a.cmp(b)),
        (Scalar::Float64(a), Scalar::Float64(b)) => {
            // NaN sorts last within float64
            match (a.is_nan(), b.is_nan()) {
                (true, true) => Some(Ordering::Equal),
                (true, false) => Some(Ordering::Greater),
                (false, true) => Some(Ordering::Less),
                (false, false) => a.partial_cmp(b), // safe: no NaN
            }
        }
        (Scalar::Boolean(a), Scalar::Boolean(b)) => Some(a.cmp(b)),
        (Scalar::String(a), Scalar::String(b)) => Some(a.cmp(b)),
        (Scalar::Bytes(a), Scalar::Bytes(b)) => Some(a.cmp(b)),
        // Id vs Id: compare as Base32 strings
        (Scalar::Id(a), Scalar::Id(b)) => Some(a.0.cmp(&b.0)),
        // Id vs String or String vs Id: both have tag 5
        // extract_scalar returns Id for _id fields, but filter values
        // may be String. Compare via encoded Base32.
        (Scalar::Id(id), Scalar::String(s)) | (Scalar::String(s), Scalar::Id(id)) => {
            let id_str = exdb_core::ulid::encode_ulid(id);
            let cmp = id_str.cmp(s);
            // Preserve argument order
            if matches!(a, Scalar::Id(_)) {
                Some(cmp)
            } else {
                Some(cmp.reverse())
            }
        }
        _ => None,
    }
}
```

### filter_matches()

Recursive evaluation:

```rust
pub fn filter_matches(doc: &serde_json::Value, filter: &Filter) -> bool {
    match filter {
        Filter::Eq(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            compare_scalars(&extracted, val) == Some(Ordering::Equal)
        }
        Filter::Ne(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            compare_scalars(&extracted, val) != Some(Ordering::Equal)
        }
        Filter::Gt(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            compare_scalars(&extracted, val) == Some(Ordering::Greater)
        }
        Filter::Gte(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            matches!(compare_scalars(&extracted, val), Some(Ordering::Greater | Ordering::Equal))
        }
        Filter::Lt(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            compare_scalars(&extracted, val) == Some(Ordering::Less)
        }
        Filter::Lte(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            matches!(compare_scalars(&extracted, val), Some(Ordering::Less | Ordering::Equal))
        }
        Filter::In(path, vals) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            vals.iter().any(|v| compare_scalars(&extracted, v) == Some(Ordering::Equal))
        }
        Filter::And(filters) => filters.iter().all(|f| filter_matches(doc, f)),
        Filter::Or(filters) => filters.iter().any(|f| filter_matches(doc, f)),
        Filter::Not(f) => !filter_matches(doc, f),
    }
}

/// Extract a scalar from a document, returning Undefined if the field is absent.
fn extract_scalar_or_undefined(doc: &serde_json::Value, path: &FieldPath) -> Scalar {
    extract_scalar(doc, path).unwrap_or(Scalar::Undefined)
}
```

### Behavior on type mismatch

When `compare_scalars` returns `None` (types differ):
- `Eq` → false (different types are never equal)
- `Ne` → true (different types are never equal, so always not-equal)
- `Gt/Gte/Lt/Lte` → false (cross-type ordering is not defined)
- `In` → false for that element (tries each; match requires same type + equal)

This matches DESIGN.md section 1.6: "Equality and range queries on indexes must match on type."

### Behavior on missing fields

A missing field extracts as `Scalar::Undefined`. This means:
- `Eq("x", Null)` on a doc without field `x` → false (Undefined != Null)
- `Ne("x", Null)` on a doc without field `x` → true (Undefined != Null, different types)
- `Eq("x", Undefined)` — this would be unusual in practice but returns true for missing fields

This correctly distinguishes "field absent" from "field is null" per DESIGN.md section 1.4.

## Error Handling

`filter_matches` and `compare_scalars` are infallible — they always return a boolean or Option. Invalid filters (e.g., empty `And`) are handled gracefully:
- `And([])` → true (vacuous truth)
- `Or([])` → false (no match possible)

## Tests

1. **Eq match**: `{"name": "alice"}` matches `Eq("name", String("alice"))`.
2. **Eq mismatch**: `{"name": "bob"}` does not match `Eq("name", String("alice"))`.
3. **Eq type mismatch**: `{"x": 5}` (int64) does not match `Eq("x", Float64(5.0))`.
4. **Ne match**: `{"x": 1}` matches `Ne("x", Int64(2))`.
5. **Ne type mismatch**: `{"x": 5}` (int64) matches `Ne("x", String("5"))` (different types → not equal → true).
6. **Gt/Gte/Lt/Lte**: test boundary values for Int64, Float64, String.
7. **In match**: `{"status": "active"}` matches `In("status", [String("active"), String("archived")])`.
8. **In no match**: `{"status": "pending"}` does not match the above.
9. **And**: `{"x": 5, "y": 10}` matches `And([Gte("x", 5), Lt("y", 20)])`.
10. **Or**: `{"x": 1}` matches `Or([Eq("x", 1), Eq("x", 2)])`.
11. **Not**: `{"x": true}` matches `Not(Eq("x", false))`.
12. **Nested And/Or**: `And([Or([Eq("a", 1), Eq("a", 2)]), Eq("b", 3)])`.
13. **Missing field**: `{}` does not match `Eq("x", Null)`. Does match `Ne("x", Null)`.
14. **Nested field path**: `{"user": {"age": 25}}` matches `Gte(["user", "age"], Int64(18))`.
15. **Float64 NaN**: compare_scalars(NaN, NaN) → Equal. compare_scalars(NaN, 1.0) → Greater.
16. **Boolean ordering**: compare_scalars(false, true) → Less.
17. **Cross-type compare**: compare_scalars(Int64(5), Float64(5.0)) → None.
18. **Empty And**: `And([])` → true.
19. **Empty Or**: `Or([])` → false.
20. **Id vs String comparison**: `Eq("_id", String("...base32..."))` should match against extracted Id scalar.
21. **Deeply nested Not/And/Or**: verify recursive evaluation with 3+ levels of nesting.
