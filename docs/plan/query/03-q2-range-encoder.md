# Q2: Range Encoder

## Purpose

Validates index range expressions against an index's field order and encodes them into contiguous byte intervals on the secondary index key space. This is the bridge between the typed query predicates (from the API) and the raw byte bounds consumed by `SecondaryIndex::scan_at_ts`.

Corresponds to DESIGN.md sections 4.3, 4.5 step 2, and 5.6.2.

## Dependencies

- **Q0 (`core/filter.rs`)**: `RangeExpr`
- **L1 (`core/field_path.rs`)**: `FieldPath`
- **L1 (`core/types.rs`)**: `Scalar`
- **L3 (`docstore/key_encoding.rs`)**: `encode_scalar`, `encode_key_prefix`, `successor_key`, `prefix_successor`

No L2, L5, or L6 dependency.

## Rust Types

```rust
use exdb_core::filter::RangeExpr;
use exdb_core::field_path::FieldPath;
use exdb_core::types::Scalar;
use exdb_docstore::key_encoding;
use std::ops::Bound;

/// Result of validating range expressions against an index.
/// Describes the shape of the range: how many equality prefixes,
/// whether there is a range field, and which bounds are present.
#[derive(Debug, Clone)]
pub struct RangeShape {
    /// Number of leading Eq predicates.
    pub eq_count: usize,
    /// Index position of the range field (if any). Always == eq_count when present.
    pub range_field: Option<usize>,
    /// Whether a lower bound (Gt or Gte) is present on the range field.
    pub has_lower: bool,
    /// Whether an upper bound (Lt or Lte) is present on the range field.
    pub has_upper: bool,
}

/// Errors from range validation or encoding.
#[derive(Debug)]
pub enum RangeError {
    /// A field in a RangeExpr does not match any index field.
    FieldNotInIndex { field: FieldPath },
    /// A field was referenced out of index field order.
    FieldOutOfOrder { field: FieldPath, expected_position: usize },
    /// An Eq predicate appeared after a range predicate.
    EqAfterRange { field: FieldPath },
    /// Two lower bounds or two upper bounds on the same field.
    DuplicateBound { field: FieldPath, bound_kind: &'static str },
}

/// Validate range expressions against index field order (DESIGN.md section 4.3).
///
/// Rules enforced:
/// 1. Each RangeExpr references an index field, matched by FieldPath equality.
/// 2. Fields are consumed left-to-right in index order.
/// 3. Eq predicates must come first (contiguous prefix).
/// 4. After the first non-Eq, only bounds (Gt/Gte/Lt/Lte) on that same field.
/// 5. At most one lower bound and one upper bound per range field.
/// 6. No predicates on subsequent fields.
pub fn validate_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<RangeShape, RangeError>;

/// Encode validated range expressions into a byte interval on the index key space.
///
/// Returns `(lower_bound, upper_bound)` where bounds are on the VALUE PREFIX
/// (without doc_id/inv_ts suffix). The caller passes these to
/// SecondaryIndex::scan_at_ts.
///
/// Calls validate_range internally. If range is empty, returns
/// (Unbounded, Unbounded) for a full index scan.
pub fn encode_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), RangeError>;
```

## Implementation Details

### validate_range()

```
1. If range is empty, return RangeShape { eq_count: 0, range_field: None, ... }.

2. Walk through range expressions in order:
   a. For each expr, find its field in index_fields.
      - If not found → FieldNotInIndex error.
      - If found at position < current_position → FieldOutOfOrder.

   b. If expr is Eq:
      - If we already have a range field → EqAfterRange error.
      - Advance current_position to field_position + 1.
      - Increment eq_count.

   c. If expr is Gt/Gte (lower bound):
      - If range_field is already set AND it's on a different field → error.
      - If has_lower is already true → DuplicateBound.
      - Set range_field = field_position, has_lower = true.

   d. If expr is Lt/Lte (upper bound):
      - Same checks as lower bound, but for has_upper.
      - Set range_field = field_position, has_upper = true.

3. Return RangeShape.
```

### encode_range()

```
1. Call validate_range(). Return error if invalid.

2. If range is empty:
   return (Unbounded, Unbounded).

3. Collect Eq values in order. Encode eq_prefix = encode_key_prefix(eq_values).
   (eq_prefix may be empty if range starts with a non-Eq on the first field.)

4. If no range field (pure equality prefix):
   lower = Included(eq_prefix.clone())
   upper = Excluded(successor_key(&eq_prefix))
   return (lower, upper).

5. If range field exists:
   Find the lower bound expr (Gt or Gte) and upper bound expr (Lt or Lte).

   // ─── Lower bound ───
   match lower_expr {
       None => {
           if eq_prefix.is_empty() {
               lower = Unbounded;
           } else {
               lower = Included(eq_prefix.clone());
           }
       }
       Some(Gte(_, val)) => {
           let mut lb = eq_prefix.clone();
           lb.extend(encode_scalar(val));
           lower = Included(lb);
       }
       Some(Gt(_, val)) => {
           let mut lb = eq_prefix.clone();
           lb.extend(successor_key(&encode_scalar(val)));
           lower = Included(lb);
       }
   }

   // ─── Upper bound ───
   match upper_expr {
       None => {
           if eq_prefix.is_empty() {
               upper = Unbounded;
           } else {
               upper = Excluded(successor_key(&eq_prefix));
           }
       }
       Some(Lt(_, val)) => {
           let mut ub = eq_prefix.clone();
           ub.extend(encode_scalar(val));
           upper = Excluded(ub);
       }
       Some(Lte(_, val)) => {
           let mut ub = eq_prefix.clone();
           ub.extend(successor_key(&encode_scalar(val)));
           upper = Excluded(ub);
       }
   }

   return (lower, upper).
```

### Encoding examples

#### Single-field index `[A]`

**`eq(A, 1)`**:
```
eq_prefix = encode(Int64, 1)  // 9 bytes
lower = Included(eq_prefix)
upper = Excluded(successor_key(eq_prefix))
```

**`gt(A, 5)`**:
```
eq_prefix = [] (empty)
lower = Included(successor_key(encode(Int64, 5)))
upper = Unbounded
```

**`lt(A, 10)`**:
```
eq_prefix = [] (empty)
lower = Unbounded
upper = Excluded(encode(Int64, 10))
```

**`gte(A, 1), lte(A, 10)`** ("A between 1 and 10"):
```
eq_prefix = [] (empty)
lower = Included(encode(Int64, 1))
upper = Excluded(successor_key(encode(Int64, 10)))
```

**Empty range `[]`**:
```
lower = Unbounded
upper = Unbounded
```
Full index scan.

#### Two-field index `[A, B]`

**`eq(A, 1)`** — all B values within A=1:
```
eq_prefix = encode(1)
lower = Included(eq_prefix)
upper = Excluded(successor_key(eq_prefix))
```

**`eq(A, 1), eq(B, 2)`** — exact match on both fields:
```
eq_prefix = encode(1) || encode(2)
lower = Included(eq_prefix)
upper = Excluded(successor_key(eq_prefix))
```

**`eq(A, 1), gt(B, 3)`** — A=1, B > 3:
```
eq_prefix = encode(1)
lower = Included(encode(1) || successor_key(encode(3)))
upper = Excluded(successor_key(encode(1)))
```

**`eq(A, 1), gte(B, "hello"), lt(B, "world")`** — A=1, "hello" <= B < "world":
```
eq_prefix = encode(1)
lower = Included(encode(1) || encode("hello"))
upper = Excluded(encode(1) || encode("world"))
```

#### Three-field index `[A, B, C]`

**`eq(A, 1), eq(B, 10), eq(C, 20)`** — exact match all three:
```
eq_prefix = encode(1) || encode(10) || encode(20)
lower = Included(eq_prefix)
upper = Excluded(successor_key(eq_prefix))
```

**`eq(A, 1), eq(B, 2), gte(C, 5), lte(C, 20)`** — A=1, B=2, 5 <= C <= 20:
```
eq_prefix = encode(1) || encode(2)
lower = Included(encode(1) || encode(2) || encode(5))
upper = Excluded(encode(1) || encode(2) || successor_key(encode(20)))
```

**`eq(A, 1)`** — just A, all B and C values:
```
eq_prefix = encode(1)
lower = Included(encode(1))
upper = Excluded(successor_key(encode(1)))
```
Works because all keys starting with `encode(1)` are within the interval, regardless of B and C values.

#### Invalid combinations (rejected by validate_range)

| Range expressions | Error |
|------------------|-------|
| `[gte(A,1), lte(A,10), eq(B,5)]` | EqAfterRange — B==5 must be a post-filter |
| `[eq(A,1), eq(C,5)]` (skip B) | FieldOutOfOrder — C at position 2 before B at position 1 |
| `[eq(B,5)]` (skip A) | FieldOutOfOrder — B at position 1, expected A at position 0 |
| `[eq(A,1), gt(B,3), lt(C,10)]` | FieldOutOfOrder — C after range field B |
| `[gt(A,5), gte(A,3)]` | DuplicateBound — two lower bounds on A |

### Encoding exclusivity: Gt, Lt, Gte, Lte

The secondary key format is: `encode(A_value) || encode(B_value) || ... || doc_id[16] || inv_ts[8]`

The challenge is that range bounds are on the VALUE PREFIX (e.g., `encode(5)` = 9 bytes for Int64), but actual keys in the B-tree are longer (they have doc_id + inv_ts appended). This means:

- `Bound::Excluded(encode(5))` would exclude ONLY the exact 9-byte key `encode(5)`, but no actual secondary key is exactly 9 bytes — they all have the 24-byte suffix. So `Excluded(encode(5))` effectively excludes nothing.
- We must encode exclusivity into the key bytes using `successor_key`.

**`successor_key(encode(val))`** increments the encoded value to produce the smallest prefix that is strictly greater than ALL keys starting with `encode(val)`:

- For Int64: `successor_key(encode(5))` = `encode(6)` (increments last byte of 9-byte fixed encoding).
- For Float64: `successor_key(encode(5.0))` = encoding of `5.0 + ulp` (next representable float).
- For String: `successor_key(encode("hello"))` = `[0x05, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01]`. This is > `encode("hello") || any_suffix` because at the terminator position, `0x01 > 0x00` (the first byte of any doc_id suffix). And it's < `encode("hello\x01")` because at byte 6, `0x00 < 0x01`. So it correctly sits between "hello" keys and the next string.

**Final encoding rules:**

```
Gt(val):  lower = Included(eq_prefix || successor_key(encode_scalar(val)))
Gte(val): lower = Included(eq_prefix || encode_scalar(val))
Lt(val):  upper = Excluded(eq_prefix || encode_scalar(val))
Lte(val): upper = Excluded(eq_prefix || successor_key(encode_scalar(val)))
```

For `Lt(val)`: `Excluded(encode(val))` works correctly as an upper bound because all real keys with this value are `encode(val) || doc_id || inv_ts` which are > `encode(val)`, so they are correctly excluded. And any key with a lesser value has a shorter prefix that sorts before `encode(val)`.

For `Lte(val)`: we want to include all entries with value <= val. The secondary keys for value=val have the prefix `encode(val)` followed by doc_id/inv_ts bytes, making them longer and lexicographically greater. `successor_key(encode(val))` is the first prefix that belongs to a different (greater) value, so `Excluded(successor_key(encode(val)))` includes all val entries and excludes everything beyond.

**Special case — empty eq_prefix:**

When there is no equality prefix (range on the first field):
- Lower only (`gt(A, 5)`): lower = `Included(successor_key(encode(5)))`, upper = `Unbounded`.
- Upper only (`lt(A, 10)`): lower = `Unbounded`, upper = `Excluded(encode(10))`.
- Both (`gte(A, 5), lte(A, 10)`): lower = `Included(encode(5))`, upper = `Excluded(successor_key(encode(10)))`.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| FieldNotInIndex | RangeExpr references a field not in the index | Return RangeError |
| FieldOutOfOrder | Fields referenced out of index order | Return RangeError |
| EqAfterRange | Eq predicate after a Gt/Gte/Lt/Lte | Return RangeError |
| DuplicateBound | Two lower bounds or two upper bounds | Return RangeError |

## Tests

### validate_range tests

1. **Empty range**: valid, returns eq_count=0, range_field=None.
2. **Single Eq**: `[eq(A, 1)]` on index `[A, B]` → eq_count=1, no range field.
3. **Two Eq**: `[eq(A, 1), eq(B, 2)]` on `[A, B]` → eq_count=2.
4. **Eq + range**: `[eq(A, 1), gt(B, 5)]` → eq_count=1, range_field=Some(1), has_lower=true.
5. **Eq + both bounds**: `[eq(A, 1), gte(B, 5), lt(B, 10)]` → has_lower=true, has_upper=true.
6. **Range on first field**: `[gt(A, 5)]` → eq_count=0, range_field=Some(0).
7. **Both bounds no eq**: `[gte(A, 5), lte(A, 10)]` → valid.
8. **Error: field not in index**: `[eq(C, 1)]` on `[A, B]` → FieldNotInIndex.
9. **Error: field out of order**: `[eq(B, 1)]` on `[A, B]` (skips A) → FieldOutOfOrder.
10. **Error: eq after range**: `[gt(A, 5), eq(B, 1)]` → EqAfterRange.
11. **Error: duplicate lower**: `[gt(A, 5), gte(A, 3)]` → DuplicateBound.
12. **Error: duplicate upper**: `[lt(A, 10), lte(A, 8)]` → DuplicateBound.
13. **Single field index**: `[eq(A, 1)]` on `[A]` → valid.
14. **Three-field index**: `[eq(A, 1), eq(B, 2), lt(C, 10)]` on `[A, B, C]` → valid.

### encode_range tests

15. **Empty range**: returns `(Unbounded, Unbounded)`.
16. **eq(A, 1)** on `[A]`: lower = `Included(encode(1))`, upper = `Excluded(successor_key(encode(1)))`.
17. **eq(A, 1)** on `[A, B]`: same — covers all B values within A=1.
18. **eq(A, 1), eq(B, "x")** on `[A, B]`: lower = `Included(encode(1) || encode("x"))`, upper = `Excluded(successor_key(encode(1) || encode("x")))`.
19. **eq(A, 1), gte(B, "hello")**: lower includes encode("hello"), upper is successor of A=1 prefix.
20. **eq(A, 1), gte(B, "hello"), lt(B, "world")**: both bounds set.
21. **gt(A, 5)**: lower = `Included(successor_key(encode(5)))`, upper = `Unbounded`.
22. **lt(A, 10)**: lower = `Unbounded`, upper = `Excluded(encode(10))`.
23. **gte(A, 5), lte(A, 10)**: lower inclusive, upper inclusive (via successor_key).
24. **Verify byte ordering**: for each test above, generate sample secondary keys (with doc_id/inv_ts) and verify they fall inside/outside the bounds correctly.
25. **Roundtrip consistency**: encode_range then check that keys produced by make_secondary_key with matching values fall within the bounds, and keys with non-matching values fall outside.
