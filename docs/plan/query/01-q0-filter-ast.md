# Q0: Filter AST (in exdb-core)

## Purpose

Pure AST types for filter expressions and index range expressions. No evaluation logic. These types are defined in L1 (`exdb-core`) because they are used across L4 (query engine), L5 (subscriptions, OCC), L6 (database API), and L8 (wire protocol parsing).

Corresponds to DESIGN.md sections 4.3, 4.4, 7.7.1, 7.7.2.

## Dependencies

- **L1 (`core/field_path.rs`)**: `FieldPath`
- **L1 (`core/types.rs`)**: `Scalar`

No other dependencies. Pure data types.

## Rust Types

```rust
// In crates/core/src/filter.rs

use crate::field_path::FieldPath;
use crate::types::Scalar;

/// Post-filter expression AST (DESIGN.md sections 4.4, 7.7.2).
///
/// Evaluated against documents after index scan. Supports all comparison
/// and logical operators. Does NOT narrow the read set interval.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// field == value
    Eq(FieldPath, Scalar),
    /// field != value
    Ne(FieldPath, Scalar),
    /// field > value
    Gt(FieldPath, Scalar),
    /// field >= value
    Gte(FieldPath, Scalar),
    /// field < value
    Lt(FieldPath, Scalar),
    /// field <= value
    Lte(FieldPath, Scalar),
    /// field is one of the values
    In(FieldPath, Vec<Scalar>),
    /// All sub-filters must match
    And(Vec<Filter>),
    /// At least one sub-filter must match
    Or(Vec<Filter>),
    /// Negation
    Not(Box<Filter>),
}

/// Index range expression (DESIGN.md sections 4.3, 7.7.1).
///
/// Defines a contiguous interval on an index's key space.
/// Validation rules (enforced by L4 range_encoder):
/// 1. Predicates reference index fields in left-to-right order.
/// 2. Zero or more Eq on leading fields (equality prefix).
/// 3. At most one lower bound (Gt/Gte) and one upper bound (Lt/Lte)
///    on the next field after the equality prefix.
/// 4. No predicates on fields after the range field.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeExpr {
    Eq(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
}
```

## Implementation Details

### RangeExpr helpers

```rust
impl RangeExpr {
    /// Get the field path referenced by this expression.
    pub fn field_path(&self) -> &FieldPath {
        match self {
            RangeExpr::Eq(f, _)
            | RangeExpr::Gt(f, _)
            | RangeExpr::Gte(f, _)
            | RangeExpr::Lt(f, _)
            | RangeExpr::Lte(f, _) => f,
        }
    }

    /// Get the scalar value in this expression.
    pub fn value(&self) -> &Scalar {
        match self {
            RangeExpr::Eq(_, v)
            | RangeExpr::Gt(_, v)
            | RangeExpr::Gte(_, v)
            | RangeExpr::Lt(_, v)
            | RangeExpr::Lte(_, v) => v,
        }
    }

    /// Returns true if this is an Eq predicate.
    pub fn is_eq(&self) -> bool {
        matches!(self, RangeExpr::Eq(..))
    }

    /// Returns true if this is a lower bound (Gt or Gte).
    pub fn is_lower_bound(&self) -> bool {
        matches!(self, RangeExpr::Gt(..) | RangeExpr::Gte(..))
    }

    /// Returns true if this is an upper bound (Lt or Lte).
    pub fn is_upper_bound(&self) -> bool {
        matches!(self, RangeExpr::Lt(..) | RangeExpr::Lte(..))
    }
}
```

### Filter helpers

```rust
impl Filter {
    /// Get the field path for comparison filters (returns None for And/Or/Not).
    pub fn field_path(&self) -> Option<&FieldPath> {
        match self {
            Filter::Eq(f, _)
            | Filter::Ne(f, _)
            | Filter::Gt(f, _)
            | Filter::Gte(f, _)
            | Filter::Lt(f, _)
            | Filter::Lte(f, _)
            | Filter::In(f, _) => Some(f),
            Filter::And(_) | Filter::Or(_) | Filter::Not(_) => None,
        }
    }
}
```

## Integration with L1

Add `pub mod filter;` to `crates/core/src/lib.rs`.

The `Scalar` type must derive `Clone` and `PartialEq` (it likely already does). `FieldPath` must derive `Clone`, `PartialEq`, and `Debug`.

## Tests

1. **RangeExpr::field_path()**: create each variant, verify field_path() returns the correct path.
2. **RangeExpr::value()**: create each variant, verify value() returns the correct scalar.
3. **RangeExpr::is_eq()**: true for Eq, false for others.
4. **RangeExpr::is_lower_bound()**: true for Gt/Gte, false for others.
5. **RangeExpr::is_upper_bound()**: true for Lt/Lte, false for others.
6. **Filter construction**: build a nested `And([Eq(..), Or([Gt(..), Lt(..)])])`, verify structure via pattern matching.
7. **Filter::field_path()**: returns Some for comparison variants, None for logical.
8. **Clone and PartialEq**: clone a Filter and RangeExpr, verify equality.
