//! Document encoding utilities.
//!
//! Serialization of documents to/from the persisted BSON representation,
//! field extraction, and merge-patch application.

use crate::field_path::FieldPath;
use crate::types::{DocId, Scalar};
use crate::ulid::{decode_ulid, encode_ulid};
use base64::Engine as _;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Document};
use std::io::Cursor;

/// Private BSON binary subtype used for exdb document-id values.
pub const DOC_ID_BINARY_SUBTYPE: BinarySubtype = BinarySubtype::UserDefined(0x80);

/// Build a BSON value for an exdb document identifier.
pub fn bson_doc_id(id: DocId) -> Bson {
    Bson::Binary(Binary {
        subtype: DOC_ID_BINARY_SUBTYPE,
        bytes: id.0.to_vec(),
    })
}

/// Build a BSON value for raw bytes.
pub fn bson_bytes(bytes: impl Into<Vec<u8>>) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: bytes.into(),
    })
}

/// Encode a JSON-facing document to persisted BSON bytes.
pub fn encode_document(doc: &serde_json::Value) -> Vec<u8> {
    try_encode_document(doc).expect("document should be a BSON-encodable object")
}

/// Encode a JSON-facing document to persisted BSON bytes.
///
/// exdb uses `serde_json::Value` as its embedded API document type, but storage
/// and WAL bodies are BSON. `_created_at` is exposed as integer milliseconds in
/// the API and stored as BSON datetime internally, matching DESIGN.md 1.11.
pub fn try_encode_document(doc: &serde_json::Value) -> Result<Vec<u8>, String> {
    let serde_json::Value::Object(map) = doc else {
        return Err("document root must be an object".into());
    };

    let type_hints = type_hints(map);
    let mut bson_doc = Document::new();
    for (key, value) in map {
        if key == "_meta" {
            continue;
        }
        let hint = type_hints.and_then(|types| types.get(key));
        let bson = if key == "_created_at" {
            match value.as_i64() {
                Some(ms) => Bson::DateTime(DateTime::from_millis(ms)),
                None => json_to_bson(value, hint)?,
            }
        } else {
            json_to_bson(value, hint)?
        };
        bson_doc.insert(key.clone(), bson);
    }

    bson::to_vec(&bson_doc).map_err(|e| format!("encode error: {e}"))
}

/// Encode a BSON document to persisted BSON bytes.
pub fn try_encode_bson_document(doc: &Document) -> Result<Vec<u8>, String> {
    let mut doc = doc.clone();
    doc.remove("_meta");
    bson::to_vec(&doc).map_err(|e| format!("encode error: {e}"))
}

/// Decode persisted BSON bytes to the JSON-facing document representation.
pub fn decode_document(data: &[u8]) -> Result<serde_json::Value, String> {
    let doc = decode_bson_document(data)?;
    bson_document_to_json(doc)
}

/// Decode persisted BSON bytes to a native BSON document.
pub fn decode_bson_document(data: &[u8]) -> Result<Document, String> {
    Document::from_reader(&mut Cursor::new(data)).map_err(|e| format!("decode error: {e}"))
}

/// Convert a JSON-facing document into a native BSON document.
pub fn json_document_to_bson(doc: &serde_json::Value) -> Result<Document, String> {
    let bytes = try_encode_document(doc)?;
    decode_bson_document(&bytes)
}

fn type_hints(map: &serde_json::Map<String, serde_json::Value>) -> Option<&serde_json::Value> {
    map.get("_meta")?.get("types")
}

fn json_to_bson(
    value: &serde_json::Value,
    type_hint: Option<&serde_json::Value>,
) -> Result<Bson, String> {
    if let Some(hint) = type_hint.and_then(serde_json::Value::as_str) {
        return json_to_bson_with_leaf_hint(value, hint);
    }

    match value {
        serde_json::Value::Null => Ok(Bson::Null),
        serde_json::Value::Bool(value) => Ok(Bson::Boolean(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Bson::Int64(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value)
                    .map(Bson::Int64)
                    .map_err(|_| format!("u64 value {value} exceeds BSON int64 range"))
            } else if let Some(value) = value.as_f64() {
                Ok(Bson::Double(value))
            } else {
                Err("unsupported JSON number".into())
            }
        }
        serde_json::Value::String(value) => Ok(Bson::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| json_to_bson(value, type_hint))
            .collect::<Result<Vec<_>, _>>()
            .map(Bson::Array),
        serde_json::Value::Object(values) => {
            let mut doc = Document::new();
            for (key, value) in values {
                let child_hint = type_hint.and_then(|hints| hints.get(key));
                doc.insert(key.clone(), json_to_bson(value, child_hint)?);
            }
            Ok(Bson::Document(doc))
        }
    }
}

fn json_to_bson_with_leaf_hint(value: &serde_json::Value, hint: &str) -> Result<Bson, String> {
    match hint {
        "bytes" => {
            let bytes = decode_base64_string(value, "bytes")?;
            Ok(Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes,
            }))
        }
        "id" => {
            let string = value
                .as_str()
                .ok_or_else(|| "_meta.types id values must be strings".to_string())?;
            let id = decode_ulid(string).map_err(|e| format!("invalid id type hint: {e}"))?;
            Ok(Bson::Binary(Binary {
                subtype: DOC_ID_BINARY_SUBTYPE,
                bytes: id.0.to_vec(),
            }))
        }
        "int64" => json_int64_to_bson(value),
        "float64" => value
            .as_f64()
            .map(Bson::Double)
            .ok_or_else(|| "_meta.types float64 values must be JSON numbers".to_string()),
        // Native JSON types are accepted as no-op hints for compatibility with
        // clients that annotate every field.
        "null" | "boolean" | "string" => json_to_bson(value, None),
        other => Err(format!("unsupported _meta.types hint: {other}")),
    }
}

fn json_int64_to_bson(value: &serde_json::Value) -> Result<Bson, String> {
    let Some(number) = value.as_number() else {
        return Err("_meta.types int64 values must be JSON numbers".to_string());
    };
    if let Some(value) = number.as_i64() {
        Ok(Bson::Int64(value))
    } else if let Some(value) = number.as_u64() {
        i64::try_from(value)
            .map(Bson::Int64)
            .map_err(|_| format!("u64 value {value} exceeds BSON int64 range"))
    } else {
        Err("_meta.types int64 values must be integral JSON numbers".to_string())
    }
}

fn decode_base64_string(value: &serde_json::Value, hint: &str) -> Result<Vec<u8>, String> {
    let string = value
        .as_str()
        .ok_or_else(|| format!("_meta.types {hint} values must be strings"))?;
    base64::engine::general_purpose::STANDARD
        .decode(string)
        .map_err(|e| format!("invalid base64 for _meta.types {hint}: {e}"))
}

/// Convert a native BSON document into the JSON-facing representation.
///
/// BSON bytes and exdb id binary values are represented as strings plus
/// `_meta.types` hints because JSON cannot carry those types natively.
pub fn bson_document_to_json(doc: Document) -> Result<serde_json::Value, String> {
    let (value, types) = bson_document_to_json_parts(doc)?;
    let serde_json::Value::Object(mut map) = value else {
        return Ok(value);
    };
    if let Some(types) = types {
        map.insert("_meta".to_string(), serde_json::json!({ "types": types }));
    }
    Ok(serde_json::Value::Object(map))
}

fn bson_document_to_json_parts(
    doc: Document,
) -> Result<(serde_json::Value, Option<serde_json::Value>), String> {
    let mut map = serde_json::Map::new();
    let mut types = serde_json::Map::new();
    for (key, value) in doc {
        let (value, type_hint) = bson_to_json(value)?;
        map.insert(key.clone(), value);
        if let Some(type_hint) = type_hint {
            types.insert(key, type_hint);
        }
    }
    Ok((
        serde_json::Value::Object(map),
        (!types.is_empty()).then_some(serde_json::Value::Object(types)),
    ))
}

fn bson_to_json(value: Bson) -> Result<(serde_json::Value, Option<serde_json::Value>), String> {
    match value {
        Bson::Double(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(|value| (value, None))
            .ok_or_else(|| format!("BSON double {value} cannot be represented as JSON")),
        Bson::String(value) => Ok((serde_json::Value::String(value), None)),
        Bson::Array(values) => {
            let mut json_values = Vec::with_capacity(values.len());
            let mut common_type: Option<serde_json::Value> = None;
            let mut saw_type = false;
            let mut mixed_types = false;

            for value in values {
                let (value, type_hint) = bson_to_json(value)?;
                json_values.push(value);
                if let Some(type_hint) = type_hint {
                    saw_type = true;
                    match &common_type {
                        Some(existing) if existing != &type_hint => mixed_types = true,
                        Some(_) => {}
                        None => common_type = Some(type_hint),
                    }
                } else if saw_type {
                    mixed_types = true;
                }
            }

            let type_hint = if saw_type && !mixed_types {
                common_type
            } else {
                None
            };
            Ok((serde_json::Value::Array(json_values), type_hint))
        }
        Bson::Document(doc) => bson_document_to_json_parts(doc),
        Bson::Boolean(value) => Ok((serde_json::Value::Bool(value), None)),
        Bson::Null => Ok((serde_json::Value::Null, None)),
        Bson::Int32(value) => Ok((serde_json::Value::Number(value.into()), None)),
        Bson::Int64(value) => Ok((serde_json::Value::Number(value.into()), None)),
        Bson::DateTime(value) => Ok((
            serde_json::Value::Number(value.timestamp_millis().into()),
            None,
        )),
        Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes,
        }) => Ok((
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            Some(serde_json::Value::String("bytes".to_string())),
        )),
        Bson::Binary(Binary {
            subtype: DOC_ID_BINARY_SUBTYPE,
            bytes,
        }) => {
            let id = doc_id_from_binary(bytes)?;
            Ok((
                serde_json::Value::String(encode_ulid(&id)),
                Some(serde_json::Value::String("id".to_string())),
            ))
        }
        other => Err(format!("unsupported BSON value in document: {other:?}")),
    }
}

fn doc_id_from_binary(bytes: Vec<u8>) -> Result<DocId, String> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!("id binary length must be 16 bytes, got {}", bytes.len())
    })?;
    Ok(DocId(bytes))
}

/// Apply exdb's top-level patch semantics to a base document.
///
/// Patch keys replace top-level fields and `null` is stored explicitly. Field
/// removal is handled by the database layer through `_meta.unset`.
pub fn apply_patch(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let serde_json::Value::Object(patch_map) = patch {
        if !base.is_object() {
            *base = serde_json::Value::Object(serde_json::Map::new());
        }
        let base_map = base.as_object_mut().expect("checked is_object above");
        for (key, value) in patch_map {
            if key != "_meta" {
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
fn type_hint_for_path<'a>(
    doc: &'a serde_json::Value,
    segments: &[String],
) -> Option<&'a serde_json::Value> {
    let mut current = doc.get("_meta")?.get("types")?;
    for segment in segments {
        current = current.get(segment.as_str())?;
    }
    Some(current)
}

fn json_to_scalar(val: &serde_json::Value, type_hint: Option<&serde_json::Value>) -> Scalar {
    if let Some(hint) = type_hint.and_then(serde_json::Value::as_str) {
        match hint {
            "bytes" => {
                return decode_base64_string(val, "bytes")
                    .map(Scalar::Bytes)
                    .unwrap_or(Scalar::Undefined);
            }
            "id" => {
                return val
                    .as_str()
                    .and_then(|value| decode_ulid(value).ok())
                    .map(Scalar::Id)
                    .unwrap_or(Scalar::Undefined);
            }
            "int64" => {
                return val
                    .as_number()
                    .and_then(serde_json::Number::as_i64)
                    .map(Scalar::Int64)
                    .unwrap_or(Scalar::Undefined);
            }
            "float64" => {
                return val
                    .as_f64()
                    .map(Scalar::Float64)
                    .unwrap_or(Scalar::Undefined);
            }
            "null" | "boolean" | "string" => {}
            _ => return Scalar::Undefined,
        }
    }

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
    Some(json_to_scalar(
        val,
        type_hint_for_path(doc, path.segments()),
    ))
}

/// Extract values at a field path (array-aware).
///
/// For non-array fields, returns a single-element vec.
/// For array fields, returns one scalar per array element.
/// For missing fields, returns an empty vec.
pub fn extract_scalars(doc: &serde_json::Value, path: &FieldPath) -> Vec<Scalar> {
    let type_hint = type_hint_for_path(doc, path.segments());
    match navigate(doc, path.segments()) {
        None => vec![],
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|value| json_to_scalar(value, type_hint))
            .collect(),
        Some(val) => vec![json_to_scalar(val, type_hint)],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bson_bytes, bson_doc_id, bson_document_to_json, decode_bson_document, decode_document,
        encode_document, extract_scalar, json_document_to_bson, try_encode_bson_document,
        try_encode_document,
    };
    use crate::field_path::FieldPath;
    use crate::types::{DocId, Scalar};
    use crate::ulid::encode_ulid;
    use bson::spec::BinarySubtype;
    use bson::{Bson, Document};
    use serde_json::json;

    #[test]
    fn bson_round_trip_preserves_json_document_shape() {
        let doc = json!({
            "name": "Ada",
            "age": 42_i64,
            "score": 98.5,
            "active": true,
            "tags": ["engineer", "math"],
            "nested": { "rank": 1_i64 },
            "_created_at": 1_700_000_000_123_i64
        });

        let bytes = encode_document(&doc);
        assert_ne!(bytes.first(), Some(&b'{'));

        let decoded = decode_document(&bytes).unwrap();
        assert_eq!(decoded, doc);
    }

    #[test]
    fn created_at_is_stored_as_bson_datetime() {
        let bytes = encode_document(&json!({
            "_created_at": 1_700_000_000_123_i64,
            "name": "Ada"
        }));

        let bson = Document::from_reader(&mut std::io::Cursor::new(bytes)).unwrap();
        assert!(matches!(bson.get("_created_at"), Some(Bson::DateTime(_))));
    }

    #[test]
    fn non_object_roots_are_rejected() {
        assert!(try_encode_document(&json!(["not", "a", "document"])).is_err());
    }

    #[test]
    fn meta_types_encode_bytes_and_strip_meta_from_bson() {
        let doc = json!({
            "name": "Ada",
            "avatar": "AQIDAA==",
            "_meta": {
                "types": {
                    "avatar": "bytes"
                },
                "debug": true
            }
        });

        let bytes = encode_document(&doc);
        let raw = Document::from_reader(&mut std::io::Cursor::new(bytes.clone())).unwrap();
        assert!(!raw.contains_key("_meta"));
        assert!(matches!(
            raw.get("avatar"),
            Some(Bson::Binary(binary))
                if binary.subtype == BinarySubtype::Generic
                    && binary.bytes.as_slice() == [1, 2, 3, 0]
        ));

        let decoded = decode_document(&bytes).unwrap();
        assert_eq!(decoded["avatar"], "AQIDAA==");
        assert_eq!(decoded["_meta"]["types"]["avatar"], "bytes");
    }

    #[test]
    fn meta_types_encode_id_as_private_binary_and_decode_with_hint() {
        let id = DocId([7; 16]);
        let encoded_id = encode_ulid(&id);
        let doc = json!({
            "owner": encoded_id,
            "_meta": {
                "types": {
                    "owner": "id"
                }
            }
        });

        let bytes = encode_document(&doc);
        let raw = Document::from_reader(&mut std::io::Cursor::new(bytes.clone())).unwrap();
        assert!(matches!(
            raw.get("owner"),
            Some(Bson::Binary(binary))
                if binary.subtype == BinarySubtype::UserDefined(0x80)
                    && binary.bytes.as_slice() == id.as_bytes()
        ));

        let decoded = decode_document(&bytes).unwrap();
        assert_eq!(decoded["owner"], encoded_id);
        assert_eq!(decoded["_meta"]["types"]["owner"], "id");
    }

    #[test]
    fn native_bson_document_converts_to_json_meta_types() {
        let id = DocId([3; 16]);
        let doc = bson::doc! {
            "avatar": bson_bytes(vec![1, 2, 3]),
            "owner": bson_doc_id(id),
            "nested": {
                "payload": bson_bytes(vec![4, 5])
            }
        };

        let json = bson_document_to_json(doc).unwrap();
        assert_eq!(json["avatar"], "AQID");
        assert_eq!(json["owner"], encode_ulid(&id));
        assert_eq!(json["nested"]["payload"], "BAU=");
        assert_eq!(json["_meta"]["types"]["avatar"], "bytes");
        assert_eq!(json["_meta"]["types"]["owner"], "id");
        assert_eq!(json["_meta"]["types"]["nested"]["payload"], "bytes");
    }

    #[test]
    fn json_document_to_bson_restores_native_typed_values() {
        let id = DocId([4; 16]);
        let json = json!({
            "avatar": "AQID",
            "owner": encode_ulid(&id),
            "_created_at": 1_700_000_000_123_i64,
            "_meta": {
                "types": {
                    "avatar": "bytes",
                    "owner": "id"
                }
            }
        });

        let doc = json_document_to_bson(&json).unwrap();
        assert!(matches!(
            doc.get("avatar"),
            Some(Bson::Binary(binary))
                if binary.subtype == BinarySubtype::Generic
                    && binary.bytes.as_slice() == [1, 2, 3]
        ));
        assert!(matches!(
            doc.get("owner"),
            Some(Bson::Binary(binary))
                if binary.subtype == BinarySubtype::UserDefined(0x80)
                    && binary.bytes.as_slice() == id.as_bytes()
        ));
        assert!(matches!(doc.get("_created_at"), Some(Bson::DateTime(_))));
    }

    #[test]
    fn try_encode_bson_document_strips_reserved_meta() {
        let doc = bson::doc! {
            "_meta": { "types": { "avatar": "bytes" } },
            "avatar": bson_bytes(vec![1])
        };

        let bytes = try_encode_bson_document(&doc).unwrap();
        let decoded = decode_bson_document(&bytes).unwrap();
        assert!(!decoded.contains_key("_meta"));
        assert!(matches!(
            decoded.get("avatar"),
            Some(Bson::Binary(binary))
                if binary.subtype == BinarySubtype::Generic && binary.bytes.as_slice() == [1]
        ));
    }

    #[test]
    fn extract_scalar_uses_meta_type_hints() {
        let id = DocId([9; 16]);
        let doc = json!({
            "avatar": "AAEC",
            "owner": encode_ulid(&id),
            "_meta": {
                "types": {
                    "avatar": "bytes",
                    "owner": "id"
                }
            }
        });

        assert_eq!(
            extract_scalar(&doc, &FieldPath::single("avatar")),
            Some(Scalar::Bytes(vec![0, 1, 2]))
        );
        assert_eq!(
            extract_scalar(&doc, &FieldPath::single("owner")),
            Some(Scalar::Id(id))
        );
    }
}
