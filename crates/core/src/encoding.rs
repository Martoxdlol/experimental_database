//! Document encoding utilities.
//!
//! Serialization of documents to/from JSON binary representation,
//! field extraction, and merge-patch application.

use crate::field_path::FieldPath;
use crate::types::Scalar;

/// Encode a `serde_json::Value` to JSON bytes.
///
/// For now we use compact JSON encoding. Future versions may use BSON.
pub fn encode_document(doc: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(doc).expect("valid JSON value should serialize")
}

/// Decode JSON bytes to `serde_json::Value`.
pub fn decode_document(data: &[u8]) -> Result<serde_json::Value, String> {
    serde_json::from_slice(data).map_err(|e| format!("decode error: {e}"))
}

/// Apply RFC 7396 merge-patch to a base document.
pub fn apply_patch(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let serde_json::Value::Object(patch_map) = patch {
        if !base.is_object() {
            *base = serde_json::Value::Object(serde_json::Map::new());
        }
        let base_map = base.as_object_mut().expect("checked is_object above");
        for (key, value) in patch_map {
            if value.is_null() {
                base_map.remove(key);
            } else if value.is_object() {
                let entry = base_map
                    .entry(key.clone())
                    .or_insert(serde_json::Value::Object(serde_json::Map::new()));
                apply_patch(entry, value);
            } else {
                base_map.insert(key.clone(), value.clone());
            }
        }
    } else {
        *base = patch.clone();
    }
}

/// Navigate into a JSON value by path segments, returning a reference.
fn navigate<'a>(doc: &'a serde_json::Value, segments: &[String]) -> Option<&'a serde_json::Value> {
    let mut current = doc;
    for segment in segments {
        current = current.get(segment.as_str())?;
    }
    Some(current)
}

/// Convert a JSON value to a Scalar.
fn json_to_scalar(val: &serde_json::Value) -> Scalar {
    match val {
        serde_json::Value::Null => Scalar::Null,
        serde_json::Value::Bool(b) => Scalar::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Scalar::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Scalar::Float64(f)
            } else {
                Scalar::Null
            }
        }
        serde_json::Value::String(s) => Scalar::String(s.clone()),
        // Arrays and objects don't map to scalars — treat as undefined
        _ => Scalar::Undefined,
    }
}

/// Extract a scalar value at a field path.
pub fn extract_scalar(doc: &serde_json::Value, path: &FieldPath) -> Option<Scalar> {
    let val = navigate(doc, path.segments())?;
    if val.is_array() || val.is_object() {
        return None;
    }
    Some(json_to_scalar(val))
}

/// Extract values at a field path (array-aware).
///
/// For non-array fields, returns a single-element vec.
/// For array fields, returns one scalar per array element.
/// For missing fields, returns an empty vec.
pub fn extract_scalars(doc: &serde_json::Value, path: &FieldPath) -> Vec<Scalar> {
    match navigate(doc, path.segments()) {
        None => vec![],
        Some(serde_json::Value::Array(arr)) => arr.iter().map(json_to_scalar).collect(),
        Some(val) => vec![json_to_scalar(val)],
    }
}
