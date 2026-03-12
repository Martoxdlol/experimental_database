//! Range encoder (DESIGN.md section 4.3).
//!
//! Validates index range expressions against an index's field order and encodes
//! them into contiguous byte intervals on the secondary index key space.

use exdb_core::field_path::FieldPath;
use exdb_core::filter::RangeExpr;
use exdb_docstore::key_encoding::{encode_key_prefix, encode_scalar, successor_key};
use std::ops::Bound;

/// Result of validating range expressions against an index.
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
    FieldOutOfOrder {
        field: FieldPath,
        expected_position: usize,
    },
    /// An Eq predicate appeared after a range predicate.
    EqAfterRange { field: FieldPath },
    /// Two lower bounds or two upper bounds on the same field.
    DuplicateBound {
        field: FieldPath,
        bound_kind: &'static str,
    },
}

/// Validate range expressions against index field order.
///
/// Rules:
/// 1. Each RangeExpr references an index field, matched by FieldPath equality.
/// 2. Fields are consumed left-to-right in index order.
/// 3. Eq predicates must come first (contiguous prefix).
/// 4. After the first non-Eq, only bounds on that same field.
/// 5. At most one lower bound and one upper bound per range field.
/// 6. No predicates on subsequent fields.
pub fn validate_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<RangeShape, RangeError> {
    if range.is_empty() {
        return Ok(RangeShape {
            eq_count: 0,
            range_field: None,
            has_lower: false,
            has_upper: false,
        });
    }

    let mut eq_count = 0usize;
    let mut range_field: Option<usize> = None;
    let mut has_lower = false;
    let mut has_upper = false;
    let mut current_position = 0usize;

    for expr in range {
        let field = expr.field_path();
        let field_pos = index_fields
            .iter()
            .position(|f| f == field)
            .ok_or_else(|| RangeError::FieldNotInIndex {
                field: field.clone(),
            })?;

        if expr.is_eq() {
            if range_field.is_some() {
                return Err(RangeError::EqAfterRange {
                    field: field.clone(),
                });
            }
            if field_pos < current_position {
                return Err(RangeError::FieldOutOfOrder {
                    field: field.clone(),
                    expected_position: current_position,
                });
            }
            if field_pos > current_position {
                return Err(RangeError::FieldOutOfOrder {
                    field: field.clone(),
                    expected_position: current_position,
                });
            }
            current_position = field_pos + 1;
            eq_count += 1;
        } else if expr.is_lower_bound() {
            if let Some(rf) = range_field {
                if rf != field_pos {
                    return Err(RangeError::FieldOutOfOrder {
                        field: field.clone(),
                        expected_position: rf,
                    });
                }
            }
            if field_pos < current_position {
                return Err(RangeError::FieldOutOfOrder {
                    field: field.clone(),
                    expected_position: current_position,
                });
            }
            if has_lower {
                return Err(RangeError::DuplicateBound {
                    field: field.clone(),
                    bound_kind: "lower",
                });
            }
            range_field = Some(field_pos);
            has_lower = true;
        } else {
            // Upper bound
            if let Some(rf) = range_field {
                if rf != field_pos {
                    return Err(RangeError::FieldOutOfOrder {
                        field: field.clone(),
                        expected_position: rf,
                    });
                }
            }
            if field_pos < current_position {
                return Err(RangeError::FieldOutOfOrder {
                    field: field.clone(),
                    expected_position: current_position,
                });
            }
            if has_upper {
                return Err(RangeError::DuplicateBound {
                    field: field.clone(),
                    bound_kind: "upper",
                });
            }
            range_field = Some(field_pos);
            has_upper = true;
        }
    }

    Ok(RangeShape {
        eq_count,
        range_field,
        has_lower,
        has_upper,
    })
}

/// Encode validated range expressions into a byte interval on the index key space.
///
/// Returns `(lower_bound, upper_bound)` where bounds are on the VALUE PREFIX
/// (without doc_id/inv_ts suffix). Calls `validate_range` internally.
pub fn encode_range(
    index_fields: &[FieldPath],
    range: &[RangeExpr],
) -> Result<(Bound<Vec<u8>>, Bound<Vec<u8>>), RangeError> {
    let shape = validate_range(index_fields, range)?;

    if range.is_empty() {
        return Ok((Bound::Unbounded, Bound::Unbounded));
    }

    // Collect Eq values in order and encode the prefix
    let eq_values: Vec<_> = range
        .iter()
        .filter(|e| e.is_eq())
        .map(|e| e.value().clone())
        .collect();
    let eq_prefix = encode_key_prefix(&eq_values);

    if shape.range_field.is_none() {
        // Pure equality prefix
        let upper = successor_key(&eq_prefix);
        return Ok((Bound::Included(eq_prefix), Bound::Excluded(upper)));
    }

    // Find lower and upper bound expressions
    let lower_expr = range.iter().find(|e| e.is_lower_bound());
    let upper_expr = range.iter().find(|e| e.is_upper_bound());

    // Lower bound
    let lower = match lower_expr {
        None => {
            if eq_prefix.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Included(eq_prefix.clone())
            }
        }
        Some(expr) => {
            let val_bytes = encode_scalar(expr.value());
            let mut lb = eq_prefix.clone();
            match expr {
                RangeExpr::Gte(_, _) => {
                    lb.extend(&val_bytes);
                    Bound::Included(lb)
                }
                RangeExpr::Gt(_, _) => {
                    lb.extend(successor_key(&val_bytes));
                    Bound::Included(lb)
                }
                _ => unreachable!(),
            }
        }
    };

    // Upper bound
    let upper = match upper_expr {
        None => {
            if eq_prefix.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Excluded(successor_key(&eq_prefix))
            }
        }
        Some(expr) => {
            let val_bytes = encode_scalar(expr.value());
            let mut ub = eq_prefix.clone();
            match expr {
                RangeExpr::Lt(_, _) => {
                    ub.extend(&val_bytes);
                    Bound::Excluded(ub)
                }
                RangeExpr::Lte(_, _) => {
                    ub.extend(successor_key(&val_bytes));
                    Bound::Excluded(ub)
                }
                _ => unreachable!(),
            }
        }
    };

    Ok((lower, upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::Scalar;

    fn fp(name: &str) -> FieldPath {
        FieldPath::single(name)
    }

    fn fields(names: &[&str]) -> Vec<FieldPath> {
        names.iter().map(|n| fp(n)).collect()
    }

    // --- validate_range tests ---

    #[test]
    fn validate_empty_range() {
        let shape = validate_range(&fields(&["a", "b"]), &[]).unwrap();
        assert_eq!(shape.eq_count, 0);
        assert!(shape.range_field.is_none());
    }

    #[test]
    fn validate_single_eq() {
        let shape = validate_range(
            &fields(&["a", "b"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
        )
        .unwrap();
        assert_eq!(shape.eq_count, 1);
        assert!(shape.range_field.is_none());
    }

    #[test]
    fn validate_two_eq() {
        let shape = validate_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Eq(fp("b"), Scalar::Int64(2)),
            ],
        )
        .unwrap();
        assert_eq!(shape.eq_count, 2);
    }

    #[test]
    fn validate_eq_plus_range() {
        let shape = validate_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Gt(fp("b"), Scalar::Int64(5)),
            ],
        )
        .unwrap();
        assert_eq!(shape.eq_count, 1);
        assert_eq!(shape.range_field, Some(1));
        assert!(shape.has_lower);
        assert!(!shape.has_upper);
    }

    #[test]
    fn validate_eq_plus_both_bounds() {
        let shape = validate_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Gte(fp("b"), Scalar::Int64(5)),
                RangeExpr::Lt(fp("b"), Scalar::Int64(10)),
            ],
        )
        .unwrap();
        assert!(shape.has_lower);
        assert!(shape.has_upper);
    }

    #[test]
    fn validate_range_on_first_field() {
        let shape = validate_range(
            &fields(&["a"]),
            &[RangeExpr::Gt(fp("a"), Scalar::Int64(5))],
        )
        .unwrap();
        assert_eq!(shape.eq_count, 0);
        assert_eq!(shape.range_field, Some(0));
    }

    #[test]
    fn validate_both_bounds_no_eq() {
        let shape = validate_range(
            &fields(&["a"]),
            &[
                RangeExpr::Gte(fp("a"), Scalar::Int64(5)),
                RangeExpr::Lte(fp("a"), Scalar::Int64(10)),
            ],
        )
        .unwrap();
        assert!(shape.has_lower);
        assert!(shape.has_upper);
    }

    #[test]
    fn validate_error_field_not_in_index() {
        let result = validate_range(
            &fields(&["a", "b"]),
            &[RangeExpr::Eq(fp("c"), Scalar::Int64(1))],
        );
        assert!(matches!(result, Err(RangeError::FieldNotInIndex { .. })));
    }

    #[test]
    fn validate_error_field_out_of_order() {
        // Skip A, go directly to B
        let result = validate_range(
            &fields(&["a", "b"]),
            &[RangeExpr::Eq(fp("b"), Scalar::Int64(1))],
        );
        assert!(matches!(result, Err(RangeError::FieldOutOfOrder { .. })));
    }

    #[test]
    fn validate_error_eq_after_range() {
        let result = validate_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Gt(fp("a"), Scalar::Int64(5)),
                RangeExpr::Eq(fp("b"), Scalar::Int64(1)),
            ],
        );
        assert!(matches!(result, Err(RangeError::EqAfterRange { .. })));
    }

    #[test]
    fn validate_error_duplicate_lower() {
        let result = validate_range(
            &fields(&["a"]),
            &[
                RangeExpr::Gt(fp("a"), Scalar::Int64(5)),
                RangeExpr::Gte(fp("a"), Scalar::Int64(3)),
            ],
        );
        assert!(matches!(result, Err(RangeError::DuplicateBound { .. })));
    }

    #[test]
    fn validate_error_duplicate_upper() {
        let result = validate_range(
            &fields(&["a"]),
            &[
                RangeExpr::Lt(fp("a"), Scalar::Int64(10)),
                RangeExpr::Lte(fp("a"), Scalar::Int64(8)),
            ],
        );
        assert!(matches!(result, Err(RangeError::DuplicateBound { .. })));
    }

    #[test]
    fn validate_three_field_index() {
        let shape = validate_range(
            &fields(&["a", "b", "c"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Eq(fp("b"), Scalar::Int64(2)),
                RangeExpr::Lt(fp("c"), Scalar::Int64(10)),
            ],
        )
        .unwrap();
        assert_eq!(shape.eq_count, 2);
        assert_eq!(shape.range_field, Some(2));
    }

    // --- encode_range tests ---

    #[test]
    fn encode_empty_range() {
        let (lower, upper) = encode_range(&fields(&["a"]), &[]).unwrap();
        assert!(matches!(lower, Bound::Unbounded));
        assert!(matches!(upper, Bound::Unbounded));
    }

    #[test]
    fn encode_eq_single_field() {
        let (lower, upper) = encode_range(
            &fields(&["a"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
        )
        .unwrap();
        let encoded = encode_scalar(&Scalar::Int64(1));
        assert_eq!(lower, Bound::Included(encoded.clone()));
        assert_eq!(upper, Bound::Excluded(successor_key(&encoded)));
    }

    #[test]
    fn encode_eq_on_two_field_index() {
        // eq(A, 1) on [A, B] covers all B values
        let (lower, upper) = encode_range(
            &fields(&["a", "b"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
        )
        .unwrap();
        let prefix = encode_key_prefix(&[Scalar::Int64(1)]);
        assert_eq!(lower, Bound::Included(prefix.clone()));
        assert_eq!(upper, Bound::Excluded(successor_key(&prefix)));
    }

    #[test]
    fn encode_eq_both_fields() {
        let (lower, upper) = encode_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Eq(fp("b"), Scalar::String("x".into())),
            ],
        )
        .unwrap();
        let prefix = encode_key_prefix(&[Scalar::Int64(1), Scalar::String("x".into())]);
        assert_eq!(lower, Bound::Included(prefix.clone()));
        assert_eq!(upper, Bound::Excluded(successor_key(&prefix)));
    }

    #[test]
    fn encode_gt() {
        let (lower, upper) = encode_range(
            &fields(&["a"]),
            &[RangeExpr::Gt(fp("a"), Scalar::Int64(5))],
        )
        .unwrap();
        let encoded = encode_scalar(&Scalar::Int64(5));
        assert_eq!(lower, Bound::Included(successor_key(&encoded)));
        assert!(matches!(upper, Bound::Unbounded));
    }

    #[test]
    fn encode_lt() {
        let (lower, upper) = encode_range(
            &fields(&["a"]),
            &[RangeExpr::Lt(fp("a"), Scalar::Int64(10))],
        )
        .unwrap();
        let encoded = encode_scalar(&Scalar::Int64(10));
        assert!(matches!(lower, Bound::Unbounded));
        assert_eq!(upper, Bound::Excluded(encoded));
    }

    #[test]
    fn encode_gte_lte() {
        let (lower, upper) = encode_range(
            &fields(&["a"]),
            &[
                RangeExpr::Gte(fp("a"), Scalar::Int64(5)),
                RangeExpr::Lte(fp("a"), Scalar::Int64(10)),
            ],
        )
        .unwrap();
        let lower_enc = encode_scalar(&Scalar::Int64(5));
        let upper_enc = encode_scalar(&Scalar::Int64(10));
        assert_eq!(lower, Bound::Included(lower_enc));
        assert_eq!(upper, Bound::Excluded(successor_key(&upper_enc)));
    }

    #[test]
    fn encode_eq_plus_gte() {
        let (lower, upper) = encode_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Gte(fp("b"), Scalar::String("hello".into())),
            ],
        )
        .unwrap();
        let eq_prefix = encode_key_prefix(&[Scalar::Int64(1)]);
        let mut expected_lower = eq_prefix.clone();
        expected_lower.extend(encode_scalar(&Scalar::String("hello".into())));
        assert_eq!(lower, Bound::Included(expected_lower));
        assert_eq!(upper, Bound::Excluded(successor_key(&eq_prefix)));
    }

    #[test]
    fn encode_eq_plus_gte_lt() {
        let (lower, upper) = encode_range(
            &fields(&["a", "b"]),
            &[
                RangeExpr::Eq(fp("a"), Scalar::Int64(1)),
                RangeExpr::Gte(fp("b"), Scalar::String("hello".into())),
                RangeExpr::Lt(fp("b"), Scalar::String("world".into())),
            ],
        )
        .unwrap();
        let eq_prefix = encode_key_prefix(&[Scalar::Int64(1)]);
        let mut expected_lower = eq_prefix.clone();
        expected_lower.extend(encode_scalar(&Scalar::String("hello".into())));
        let mut expected_upper = eq_prefix;
        expected_upper.extend(encode_scalar(&Scalar::String("world".into())));
        assert_eq!(lower, Bound::Included(expected_lower));
        assert_eq!(upper, Bound::Excluded(expected_upper));
    }
}
