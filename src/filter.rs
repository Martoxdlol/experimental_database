//! Filter AST evaluation against JSON document payloads.

use serde_json::Value;
use crate::types::{FieldPath, Filter, JsonScalar};

/// Extract a value at the given field path from a JSON value.
pub fn extract_field<'a>(doc: &'a Value, path: &FieldPath) -> Option<&'a Value> {
    let mut current = doc;
    for part in &path.0 {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Convert a serde_json Value to a JsonScalar for comparison.
pub fn value_to_scalar(v: &Value) -> Option<JsonScalar> {
    match v {
        Value::Null => Some(JsonScalar::Null),
        Value::Bool(b) => Some(JsonScalar::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(JsonScalar::I64(i))
            } else if let Some(f) = n.as_f64() {
                Some(JsonScalar::F64(f))
            } else {
                None
            }
        }
        Value::String(s) => Some(JsonScalar::String(s.clone())),
        _ => None, // Arrays and objects are not scalars
    }
}

/// Compare two JsonScalar values. Returns None if they are incomparable types.
fn scalar_cmp(a: &JsonScalar, b: &JsonScalar) -> Option<std::cmp::Ordering> {
    use JsonScalar::*;
    match (a, b) {
        (Null, Null) => Some(std::cmp::Ordering::Equal),
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        (I64(x), I64(y)) => Some(x.cmp(y)),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (I64(x), F64(y)) => (*x as f64).partial_cmp(y),
        (F64(x), I64(y)) => x.partial_cmp(&(*y as f64)),
        (String(x), String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Evaluate a filter predicate against a parsed JSON document.
/// Returns true if the document matches the filter.
pub fn filter_matches(doc: &Value, filter: &Filter) -> bool {
    match filter {
        Filter::Eq(path, scalar) => {
            match extract_field(doc, path) {
                None => false,
                Some(v) => {
                    if let Some(doc_scalar) = value_to_scalar(v) {
                        scalar_cmp(&doc_scalar, scalar) == Some(std::cmp::Ordering::Equal)
                    } else {
                        false
                    }
                }
            }
        }
        Filter::Ne(path, scalar) => {
            match extract_field(doc, path) {
                None => true, // missing field != anything
                Some(v) => {
                    if let Some(doc_scalar) = value_to_scalar(v) {
                        scalar_cmp(&doc_scalar, scalar) != Some(std::cmp::Ordering::Equal)
                    } else {
                        true
                    }
                }
            }
        }
        Filter::Lt(path, scalar) => {
            match extract_field(doc, path).and_then(value_to_scalar) {
                None => false,
                Some(doc_scalar) => scalar_cmp(&doc_scalar, scalar) == Some(std::cmp::Ordering::Less),
            }
        }
        Filter::Lte(path, scalar) => {
            match extract_field(doc, path).and_then(value_to_scalar) {
                None => false,
                Some(doc_scalar) => {
                    matches!(
                        scalar_cmp(&doc_scalar, scalar),
                        Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                    )
                }
            }
        }
        Filter::Gt(path, scalar) => {
            match extract_field(doc, path).and_then(value_to_scalar) {
                None => false,
                Some(doc_scalar) => {
                    scalar_cmp(&doc_scalar, scalar) == Some(std::cmp::Ordering::Greater)
                }
            }
        }
        Filter::Gte(path, scalar) => {
            match extract_field(doc, path).and_then(value_to_scalar) {
                None => false,
                Some(doc_scalar) => {
                    matches!(
                        scalar_cmp(&doc_scalar, scalar),
                        Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                    )
                }
            }
        }
        Filter::In(path, scalars) => {
            match extract_field(doc, path).and_then(value_to_scalar) {
                None => false,
                Some(doc_scalar) => scalars
                    .iter()
                    .any(|s| scalar_cmp(&doc_scalar, s) == Some(std::cmp::Ordering::Equal)),
            }
        }
        Filter::And(filters) => filters.iter().all(|f| filter_matches(doc, f)),
        Filter::Or(filters) => filters.iter().any(|f| filter_matches(doc, f)),
        Filter::Not(inner) => !filter_matches(doc, inner),
    }
}

/// Parse a JSON document payload and evaluate filter.
pub fn matches_payload(json: &[u8], filter: &Filter) -> bool {
    match serde_json::from_slice::<Value>(json) {
        Ok(doc) => filter_matches(&doc, filter),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eq_filter() {
        let doc = json!({"name": "alice", "age": 30});
        assert!(filter_matches(&doc, &Filter::Eq(
            FieldPath::single("name"),
            JsonScalar::String("alice".to_string()),
        )));
        assert!(!filter_matches(&doc, &Filter::Eq(
            FieldPath::single("name"),
            JsonScalar::String("bob".to_string()),
        )));
    }

    #[test]
    fn test_nested_field() {
        let doc = json!({"user": {"age": 25}});
        assert!(filter_matches(&doc, &Filter::Eq(
            FieldPath::parse("user.age"),
            JsonScalar::I64(25),
        )));
    }

    #[test]
    fn test_and_or() {
        let doc = json!({"x": 5, "y": 10});
        assert!(filter_matches(&doc, &Filter::And(vec![
            Filter::Gte(FieldPath::single("x"), JsonScalar::I64(5)),
            Filter::Lte(FieldPath::single("y"), JsonScalar::I64(10)),
        ])));
        assert!(filter_matches(&doc, &Filter::Or(vec![
            Filter::Eq(FieldPath::single("x"), JsonScalar::I64(99)),
            Filter::Eq(FieldPath::single("y"), JsonScalar::I64(10)),
        ])));
    }

    #[test]
    fn test_in_filter() {
        let doc = json!({"status": "active"});
        assert!(filter_matches(&doc, &Filter::In(
            FieldPath::single("status"),
            vec![
                JsonScalar::String("active".to_string()),
                JsonScalar::String("pending".to_string()),
            ],
        )));
    }
}
