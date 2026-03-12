//! Post-filter evaluation (DESIGN.md section 4.4).
//!
//! Evaluates arbitrary `Filter` expressions against JSON documents.
//! Type-strict comparisons per DESIGN.md section 1.6.

use exdb_core::encoding::extract_scalar;
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::types::Scalar;
use std::cmp::Ordering;

/// Evaluate a filter expression against a document.
///
/// Extracts field values as scalars and compares type-strictly.
/// A missing field extracts as `Scalar::Undefined`.
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
            matches!(
                compare_scalars(&extracted, val),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }
        Filter::Lt(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            compare_scalars(&extracted, val) == Some(Ordering::Less)
        }
        Filter::Lte(path, val) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            matches!(
                compare_scalars(&extracted, val),
                Some(Ordering::Less | Ordering::Equal)
            )
        }
        Filter::In(path, vals) => {
            let extracted = extract_scalar_or_undefined(doc, path);
            vals.iter()
                .any(|v| compare_scalars(&extracted, v) == Some(Ordering::Equal))
        }
        Filter::And(filters) => filters.iter().all(|f| filter_matches(doc, f)),
        Filter::Or(filters) => filters.iter().any(|f| filter_matches(doc, f)),
        Filter::Not(f) => !filter_matches(doc, f),
    }
}

/// Compare two scalars following DESIGN.md section 1.6 type ordering.
///
/// Returns `None` if types differ — cross-type comparison is not supported.
pub fn compare_scalars(a: &Scalar, b: &Scalar) -> Option<Ordering> {
    if scalar_type_tag(a) != scalar_type_tag(b) {
        return None;
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
                (false, false) => a.partial_cmp(b),
            }
        }
        (Scalar::Boolean(a), Scalar::Boolean(b)) => Some(a.cmp(b)),
        (Scalar::String(a), Scalar::String(b)) => Some(a.cmp(b)),
        (Scalar::Bytes(a), Scalar::Bytes(b)) => Some(a.cmp(b)),
        (Scalar::Id(a), Scalar::Id(b)) => Some(a.0.cmp(&b.0)),
        // Id vs String or String vs Id: both have tag 5.
        // Compare via encoded Base32 representation.
        (Scalar::Id(id), Scalar::String(s)) => {
            let id_str = exdb_core::ulid::encode_ulid(id);
            Some(id_str.cmp(s))
        }
        (Scalar::String(s), Scalar::Id(id)) => {
            let id_str = exdb_core::ulid::encode_ulid(id);
            Some(s.as_str().cmp(id_str.as_str()))
        }
        _ => None,
    }
}

/// Type tag for same-type comparison grouping.
/// Id and String share tag 5 (Id compares as its Base32 string).
fn scalar_type_tag(s: &Scalar) -> u8 {
    match s {
        Scalar::Undefined => 0,
        Scalar::Null => 1,
        Scalar::Int64(_) => 2,
        Scalar::Float64(_) => 3,
        Scalar::Boolean(_) => 4,
        Scalar::String(_) => 5,
        Scalar::Bytes(_) => 6,
        Scalar::Id(_) => 5,
    }
}

fn extract_scalar_or_undefined(doc: &serde_json::Value, path: &FieldPath) -> Scalar {
    extract_scalar(doc, path).unwrap_or(Scalar::Undefined)
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::Scalar;
    use serde_json::json;

    fn fp(name: &str) -> FieldPath {
        FieldPath::single(name)
    }

    #[test]
    fn eq_match() {
        let doc = json!({"name": "alice"});
        assert!(filter_matches(
            &doc,
            &Filter::Eq(fp("name"), Scalar::String("alice".into()))
        ));
    }

    #[test]
    fn eq_mismatch() {
        let doc = json!({"name": "bob"});
        assert!(!filter_matches(
            &doc,
            &Filter::Eq(fp("name"), Scalar::String("alice".into()))
        ));
    }

    #[test]
    fn eq_type_mismatch() {
        let doc = json!({"x": 5});
        // int64(5) != float64(5.0) per DESIGN.md 1.6
        assert!(!filter_matches(
            &doc,
            &Filter::Eq(fp("x"), Scalar::Float64(5.0))
        ));
    }

    #[test]
    fn ne_match() {
        let doc = json!({"x": 1});
        assert!(filter_matches(
            &doc,
            &Filter::Ne(fp("x"), Scalar::Int64(2))
        ));
    }

    #[test]
    fn ne_type_mismatch_is_true() {
        let doc = json!({"x": 5});
        // Different types → not equal → true
        assert!(filter_matches(
            &doc,
            &Filter::Ne(fp("x"), Scalar::String("5".into()))
        ));
    }

    #[test]
    fn gt_lt_gte_lte() {
        let doc = json!({"x": 5});
        assert!(filter_matches(
            &doc,
            &Filter::Gt(fp("x"), Scalar::Int64(3))
        ));
        assert!(!filter_matches(
            &doc,
            &Filter::Gt(fp("x"), Scalar::Int64(5))
        ));
        assert!(filter_matches(
            &doc,
            &Filter::Gte(fp("x"), Scalar::Int64(5))
        ));
        assert!(filter_matches(
            &doc,
            &Filter::Lt(fp("x"), Scalar::Int64(10))
        ));
        assert!(!filter_matches(
            &doc,
            &Filter::Lt(fp("x"), Scalar::Int64(5))
        ));
        assert!(filter_matches(
            &doc,
            &Filter::Lte(fp("x"), Scalar::Int64(5))
        ));
    }

    #[test]
    fn in_match() {
        let doc = json!({"status": "active"});
        assert!(filter_matches(
            &doc,
            &Filter::In(
                fp("status"),
                vec![
                    Scalar::String("active".into()),
                    Scalar::String("archived".into()),
                ]
            )
        ));
    }

    #[test]
    fn in_no_match() {
        let doc = json!({"status": "pending"});
        assert!(!filter_matches(
            &doc,
            &Filter::In(
                fp("status"),
                vec![
                    Scalar::String("active".into()),
                    Scalar::String("archived".into()),
                ]
            )
        ));
    }

    #[test]
    fn and_filter() {
        let doc = json!({"x": 5, "y": 10});
        let f = Filter::And(vec![
            Filter::Gte(fp("x"), Scalar::Int64(5)),
            Filter::Lt(fp("y"), Scalar::Int64(20)),
        ]);
        assert!(filter_matches(&doc, &f));
    }

    #[test]
    fn or_filter() {
        let doc = json!({"x": 1});
        let f = Filter::Or(vec![
            Filter::Eq(fp("x"), Scalar::Int64(1)),
            Filter::Eq(fp("x"), Scalar::Int64(2)),
        ]);
        assert!(filter_matches(&doc, &f));
    }

    #[test]
    fn not_filter() {
        let doc = json!({"x": true});
        assert!(filter_matches(
            &doc,
            &Filter::Not(Box::new(Filter::Eq(fp("x"), Scalar::Boolean(false))))
        ));
    }

    #[test]
    fn nested_and_or() {
        let doc = json!({"a": 1, "b": 3});
        let f = Filter::And(vec![
            Filter::Or(vec![
                Filter::Eq(fp("a"), Scalar::Int64(1)),
                Filter::Eq(fp("a"), Scalar::Int64(2)),
            ]),
            Filter::Eq(fp("b"), Scalar::Int64(3)),
        ]);
        assert!(filter_matches(&doc, &f));
    }

    #[test]
    fn missing_field() {
        let doc = json!({});
        // Missing != Null
        assert!(!filter_matches(
            &doc,
            &Filter::Eq(fp("x"), Scalar::Null)
        ));
        // Missing != Null → true for Ne
        assert!(filter_matches(
            &doc,
            &Filter::Ne(fp("x"), Scalar::Null)
        ));
    }

    #[test]
    fn nested_field_path() {
        let doc = json!({"user": {"age": 25}});
        let path = FieldPath::new(vec!["user".into(), "age".into()]);
        assert!(filter_matches(
            &doc,
            &Filter::Gte(path, Scalar::Int64(18))
        ));
    }

    #[test]
    fn float64_nan() {
        assert_eq!(
            compare_scalars(&Scalar::Float64(f64::NAN), &Scalar::Float64(f64::NAN)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_scalars(&Scalar::Float64(f64::NAN), &Scalar::Float64(1.0)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn boolean_ordering() {
        assert_eq!(
            compare_scalars(&Scalar::Boolean(false), &Scalar::Boolean(true)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn cross_type_returns_none() {
        assert_eq!(
            compare_scalars(&Scalar::Int64(5), &Scalar::Float64(5.0)),
            None
        );
    }

    #[test]
    fn empty_and_is_true() {
        let doc = json!({});
        assert!(filter_matches(&doc, &Filter::And(vec![])));
    }

    #[test]
    fn empty_or_is_false() {
        let doc = json!({});
        assert!(!filter_matches(&doc, &Filter::Or(vec![])));
    }
}
