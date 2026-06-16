//! Protobuf payload codec for binary wire frames.

use prost::Message;
use serde_json::{Map, Number, Value};

use crate::error::{Result, WireError};

#[derive(Clone, PartialEq, Message)]
struct ProtoObject {
    #[prost(message, repeated, tag = "1")]
    fields: Vec<ProtoField>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoField {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<ProtoValue>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoList {
    #[prost(message, repeated, tag = "1")]
    values: Vec<ProtoValue>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoValue {
    #[prost(oneof = "proto_value::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<proto_value::Kind>,
}

mod proto_value {
    use prost::Oneof;

    use super::{ProtoList, ProtoObject};

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(bool, tag = "1")]
        Bool(bool),
        #[prost(string, tag = "2")]
        String(String),
        #[prost(uint64, tag = "3")]
        U64(u64),
        #[prost(sint64, tag = "4")]
        I64(i64),
        #[prost(double, tag = "5")]
        F64(f64),
        #[prost(message, tag = "6")]
        Object(ProtoObject),
        #[prost(message, tag = "7")]
        List(ProtoList),
        #[prost(bool, tag = "8")]
        Null(bool),
    }
}

/// Encode a JSON object payload using the wire protocol Protobuf schema.
pub(crate) fn encode_object(object: Map<String, Value>) -> Result<Vec<u8>> {
    Ok(value_to_object(Value::Object(object))?.encode_to_vec())
}

/// Decode a wire protocol Protobuf object payload.
pub(crate) fn decode_object(payload: &[u8]) -> Result<Map<String, Value>> {
    let object = ProtoObject::decode(payload)
        .map_err(|err| WireError::Protobuf(format!("invalid protobuf payload: {err}")))?;
    proto_object_to_map(object)
}

fn value_to_object(value: Value) -> Result<ProtoObject> {
    match value {
        Value::Object(object) => {
            let mut fields = Vec::with_capacity(object.len());
            for (key, value) in object {
                fields.push(ProtoField {
                    key,
                    value: Some(value_to_proto(value)?),
                });
            }
            Ok(ProtoObject { fields })
        }
        _ => Err(WireError::InvalidMessage(
            "protobuf payload root must be an object".to_string(),
        )),
    }
}

fn value_to_proto(value: Value) -> Result<ProtoValue> {
    let kind = match value {
        Value::Null => proto_value::Kind::Null(true),
        Value::Bool(value) => proto_value::Kind::Bool(value),
        Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                proto_value::Kind::U64(value)
            } else if let Some(value) = value.as_i64() {
                proto_value::Kind::I64(value)
            } else if let Some(value) = value.as_f64() {
                proto_value::Kind::F64(value)
            } else {
                return Err(WireError::InvalidMessage(
                    "protobuf cannot encode non-finite JSON number".to_string(),
                ));
            }
        }
        Value::String(value) => proto_value::Kind::String(value),
        Value::Array(values) => proto_value::Kind::List(ProtoList {
            values: values
                .into_iter()
                .map(value_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        Value::Object(_) => proto_value::Kind::Object(value_to_object(value)?),
    };
    Ok(ProtoValue { kind: Some(kind) })
}

fn proto_object_to_map(object: ProtoObject) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    for field in object.fields {
        let value = field.value.ok_or_else(|| {
            WireError::Protobuf(format!("missing protobuf value for field '{}'", field.key))
        })?;
        map.insert(field.key, proto_to_value(value)?);
    }
    Ok(map)
}

fn proto_to_value(value: ProtoValue) -> Result<Value> {
    let kind = value
        .kind
        .ok_or_else(|| WireError::Protobuf("missing protobuf value kind".to_string()))?;
    match kind {
        proto_value::Kind::Null(_) => Ok(Value::Null),
        proto_value::Kind::Bool(value) => Ok(Value::Bool(value)),
        proto_value::Kind::String(value) => Ok(Value::String(value)),
        proto_value::Kind::U64(value) => Ok(Value::Number(Number::from(value))),
        proto_value::Kind::I64(value) => Ok(Value::Number(Number::from(value))),
        proto_value::Kind::F64(value) => Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| WireError::Protobuf("non-finite protobuf f64 value".to_string())),
        proto_value::Kind::Object(object) => Ok(Value::Object(proto_object_to_map(object)?)),
        proto_value::Kind::List(list) => {
            let mut values = Vec::with_capacity(list.values.len());
            for value in list.values {
                values.push(proto_to_value(value)?);
            }
            Ok(Value::Array(values))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn protobuf_object_round_trip_preserves_scalar_shapes() {
        let object = json!({
            "null": null,
            "bool": true,
            "string": "hello",
            "u64": u64::MAX,
            "i64": i64::MIN,
            "f64": 1.25,
            "list": [1, "two", false],
            "nested": {"x": 7}
        })
        .as_object()
        .unwrap()
        .clone();

        let encoded = encode_object(object.clone()).unwrap();
        let decoded = decode_object(&encoded).unwrap();

        assert_eq!(decoded, object);
    }
}
