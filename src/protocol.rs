//! JSON protocol types for the WebSocket API.
//!
//! ## Filter format
//!
//! Filters are JSON objects with a single key (the operator):
//! ```json
//! {"eq":  ["field.path", value]}
//! {"ne":  ["field.path", value]}
//! {"lt":  ["field.path", value]}
//! {"lte": ["field.path", value]}
//! {"gt":  ["field.path", value]}
//! {"gte": ["field.path", value]}
//! {"in":  ["field.path", [v1, v2, ...]]}
//! {"and": [filter, ...]}
//! {"or":  [filter, ...]}
//! {"not": filter}
//! ```
//!
//! Scalar values are plain JSON: `null`, `true`/`false`, numbers, strings.
//!
//! ## DocId format
//! DocIds are encoded as 32 lowercase hex characters.

use anyhow::{anyhow, Result};
use serde_json::Value;

use crate::types::{DocId, FieldPath, Filter, JsonScalar};

// ── DocId encoding ────────────────────────────────────────────────────────────

pub fn encode_doc_id(id: &DocId) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

pub fn decode_doc_id(s: &str) -> Result<DocId> {
    if s.len() != 32 {
        return Err(anyhow!("invalid doc_id: expected 32 hex chars, got {}", s.len()));
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow!("invalid hex digit in doc_id at position {}", i * 2))?;
    }
    Ok(DocId::from_bytes(bytes))
}

// ── Scalar parsing ────────────────────────────────────────────────────────────

pub fn parse_scalar(v: &Value) -> Result<JsonScalar> {
    match v {
        Value::Null => Ok(JsonScalar::Null),
        Value::Bool(b) => Ok(JsonScalar::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(JsonScalar::I64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(JsonScalar::F64(f))
            } else {
                Err(anyhow!("number out of range"))
            }
        }
        Value::String(s) => Ok(JsonScalar::String(s.clone())),
        _ => Err(anyhow!("expected scalar (null/bool/number/string), got object or array")),
    }
}

pub fn scalar_to_json(s: &JsonScalar) -> Value {
    match s {
        JsonScalar::Null => Value::Null,
        JsonScalar::Bool(b) => Value::Bool(*b),
        JsonScalar::I64(i) => Value::Number((*i).into()),
        JsonScalar::F64(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        JsonScalar::String(s) => Value::String(s.clone()),
    }
}

// ── Filter parsing ────────────────────────────────────────────────────────────

/// Parse a filter from JSON. Returns `None` if the value is `null`.
pub fn parse_opt_filter(v: &Value) -> Result<Option<Filter>> {
    if v.is_null() {
        return Ok(None);
    }
    parse_filter(v).map(Some)
}

/// Parse a filter from a JSON object.
pub fn parse_filter(v: &Value) -> Result<Filter> {
    let obj = v.as_object().ok_or_else(|| anyhow!("filter must be a JSON object"))?;
    if obj.len() != 1 {
        return Err(anyhow!("filter object must have exactly one key (operator)"));
    }
    let (op, args) = obj.iter().next().unwrap();

    match op.as_str() {
        "eq" | "ne" | "lt" | "lte" | "gt" | "gte" => parse_comparison(op, args),
        "in" => parse_in(args),
        "and" => parse_logical_vec("and", args, Filter::And),
        "or" => parse_logical_vec("or", args, Filter::Or),
        "not" => Ok(Filter::Not(Box::new(parse_filter(args)?))),
        other => Err(anyhow!("unknown filter operator '{other}'")),
    }
}

fn parse_comparison(op: &str, args: &Value) -> Result<Filter> {
    let arr = args
        .as_array()
        .ok_or_else(|| anyhow!("'{op}' requires a [field, value] array"))?;
    if arr.len() != 2 {
        return Err(anyhow!("'{op}' requires exactly [field, value]"));
    }
    let field = arr[0]
        .as_str()
        .ok_or_else(|| anyhow!("field path must be a string"))?;
    let field = FieldPath::parse(field);
    let scalar = parse_scalar(&arr[1])?;

    Ok(match op {
        "eq" => Filter::Eq(field, scalar),
        "ne" => Filter::Ne(field, scalar),
        "lt" => Filter::Lt(field, scalar),
        "lte" => Filter::Lte(field, scalar),
        "gt" => Filter::Gt(field, scalar),
        "gte" => Filter::Gte(field, scalar),
        _ => unreachable!(),
    })
}

fn parse_in(args: &Value) -> Result<Filter> {
    let arr = args
        .as_array()
        .ok_or_else(|| anyhow!("'in' requires a [field, [values...]] array"))?;
    if arr.len() != 2 {
        return Err(anyhow!("'in' requires exactly [field, [values...]]"));
    }
    let field = arr[0]
        .as_str()
        .ok_or_else(|| anyhow!("field path must be a string"))?;
    let field = FieldPath::parse(field);
    let values = arr[1]
        .as_array()
        .ok_or_else(|| anyhow!("'in' values must be an array"))?;
    let scalars: Result<Vec<JsonScalar>> = values.iter().map(parse_scalar).collect();
    Ok(Filter::In(field, scalars?))
}

fn parse_logical_vec(
    op: &str,
    args: &Value,
    ctor: fn(Vec<Filter>) -> Filter,
) -> Result<Filter> {
    let arr = args
        .as_array()
        .ok_or_else(|| anyhow!("'{op}' requires an array of filters"))?;
    let filters: Result<Vec<Filter>> = arr.iter().map(parse_filter).collect();
    Ok(ctor(filters?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_id_roundtrip() {
        let id = DocId::new(12345678, 999);
        let hex = encode_doc_id(&id);
        assert_eq!(hex.len(), 32);
        let decoded = decode_doc_id(&hex).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn filter_eq() {
        let v = serde_json::json!({"eq": ["name", "Alice"]});
        let f = parse_filter(&v).unwrap();
        assert!(matches!(f, Filter::Eq(_, JsonScalar::String(_))));
    }

    #[test]
    fn filter_and() {
        let v = serde_json::json!({
            "and": [
                {"gt": ["age", 18]},
                {"eq": ["active", true]}
            ]
        });
        let f = parse_filter(&v).unwrap();
        assert!(matches!(f, Filter::And(_)));
    }

    #[test]
    fn filter_in() {
        let v = serde_json::json!({"in": ["status", ["active", "pending"]]});
        let f = parse_filter(&v).unwrap();
        assert!(matches!(f, Filter::In(_, _)));
    }
}
