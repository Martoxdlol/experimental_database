//! Client and server message schemas for the exdb wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Result, WireError};
use crate::frame::{Encoding, FrameType, RawFrame};

pub type TxId = u64;
pub type Ts = u64;
pub type IndexId = u64;

pub const MSG_AUTHENTICATE: u8 = 0x01;
pub const MSG_PING: u8 = 0x02;
pub const MSG_BEGIN: u8 = 0x10;
pub const MSG_COMMIT: u8 = 0x11;
pub const MSG_ROLLBACK: u8 = 0x12;
pub const MSG_INSERT: u8 = 0x20;
pub const MSG_GET: u8 = 0x21;
pub const MSG_REPLACE: u8 = 0x22;
pub const MSG_PATCH: u8 = 0x23;
pub const MSG_DELETE: u8 = 0x24;
pub const MSG_QUERY: u8 = 0x25;
pub const MSG_CREATE_DATABASE: u8 = 0x30;
pub const MSG_DROP_DATABASE: u8 = 0x31;
pub const MSG_LIST_DATABASES: u8 = 0x32;
pub const MSG_CREATE_COLLECTION: u8 = 0x33;
pub const MSG_DROP_COLLECTION: u8 = 0x34;
pub const MSG_LIST_COLLECTIONS: u8 = 0x35;
pub const MSG_CREATE_INDEX: u8 = 0x36;
pub const MSG_DROP_INDEX: u8 = 0x37;
pub const MSG_LIST_INDEXES: u8 = 0x38;

pub const MSG_HELLO: u8 = 0x80;
pub const MSG_OK: u8 = 0x81;
pub const MSG_ERROR: u8 = 0x82;
pub const MSG_INVALIDATION: u8 = 0x83;
pub const MSG_PONG: u8 = 0x84;
pub const MSG_INDEX_READY: u8 = 0x85;

/// Client to server messages.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    Authenticate {
        token: String,
    },
    Ping,
    Begin {
        database: String,
        readonly: bool,
        subscribe: bool,
        notify: bool,
    },
    Commit {
        tx: TxId,
    },
    Rollback {
        tx: TxId,
    },
    Insert {
        tx: TxId,
        collection: String,
        body: Value,
    },
    Get {
        tx: TxId,
        collection: String,
        doc_id: String,
    },
    Replace {
        tx: TxId,
        collection: String,
        doc_id: String,
        body: Value,
    },
    Patch {
        tx: TxId,
        collection: String,
        doc_id: String,
        body: Value,
    },
    Delete {
        tx: TxId,
        collection: String,
        doc_id: String,
    },
    Query {
        tx: TxId,
        collection: String,
        index: String,
        range: Vec<Value>,
        filter: Option<Value>,
        type_hints: Option<Value>,
        order: Option<String>,
        limit: Option<usize>,
    },
    CreateDatabase {
        name: String,
        config: Option<Value>,
    },
    DropDatabase {
        name: String,
    },
    ListDatabases,
    CreateCollection {
        database: String,
        name: String,
    },
    DropCollection {
        database: String,
        name: String,
    },
    ListCollections {
        database: String,
    },
    CreateIndex {
        database: String,
        collection: String,
        fields: Vec<Value>,
        name: Option<String>,
    },
    DropIndex {
        database: String,
        collection: String,
        name: String,
    },
    ListIndexes {
        database: String,
        collection: String,
    },
}

impl ClientMessage {
    pub fn is_management(&self) -> bool {
        matches!(
            self,
            ClientMessage::CreateDatabase { .. }
                | ClientMessage::DropDatabase { .. }
                | ClientMessage::ListDatabases
                | ClientMessage::CreateCollection { .. }
                | ClientMessage::DropCollection { .. }
                | ClientMessage::ListCollections { .. }
                | ClientMessage::CreateIndex { .. }
                | ClientMessage::DropIndex { .. }
                | ClientMessage::ListIndexes { .. }
        )
    }
}

/// Server to client messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello {
        version: String,
        encodings: Vec<String>,
        auth_required: bool,
        node_role: String,
        max_message_size: usize,
    },
    Ok {
        fields: Value,
    },
    Error {
        code: String,
        message: String,
        extra: Option<Value>,
    },
    Invalidation {
        tx: TxId,
        queries: Vec<u32>,
        commit_ts: Ts,
        new_tx: Option<TxId>,
        new_ts: Option<Ts>,
    },
    Pong,
    IndexReady {
        database: String,
        collection: String,
        index: String,
        index_id: IndexId,
    },
}

/// Parse a raw frame into a typed client message.
pub fn parse_client_message(frame: &RawFrame) -> Result<(u32, ClientMessage)> {
    let mut object = decode_payload_object(frame.encoding, &frame.payload)?;

    let (msg_id, msg_type) = match frame.frame_type {
        FrameType::JsonText => {
            let id = remove_required_u32(&mut object, "id")?;
            let type_name = remove_required_string(&mut object, "type")?;
            (id, client_type_name_to_byte(&type_name)?)
        }
        FrameType::Binary => {
            if let Some(id) = remove_optional_u32(&mut object, "id")?
                && id != frame.msg_id
            {
                return Err(WireError::InvalidMessage(format!(
                    "payload id {id} does not match frame id {}",
                    frame.msg_id
                )));
            }
            if let Some(type_name) = remove_optional_string(&mut object, "type")? {
                let payload_type = client_type_name_to_byte(&type_name)?;
                if payload_type != frame.msg_type {
                    return Err(WireError::InvalidMessage(format!(
                        "payload type {type_name} does not match frame type {:#04x}",
                        frame.msg_type
                    )));
                }
            }
            (frame.msg_id, frame.msg_type)
        }
    };
    validate_client_msg_id(msg_id)?;

    Ok((msg_id, parse_client_message_body(msg_type, object)?))
}

/// Serialize a server message into a raw frame.
pub fn serialize_server_message(
    msg: &ServerMessage,
    msg_id: u32,
    encoding: Encoding,
) -> Result<RawFrame> {
    let msg_type = server_message_type(msg);
    let mut object = server_message_object(msg);

    match encoding {
        Encoding::Json => {
            object.insert("id".to_string(), Value::from(msg_id));
            object.insert(
                "type".to_string(),
                Value::from(server_type_byte_to_name(msg_type)?),
            );
            let payload = serde_json::to_vec(&Value::Object(object))?;
            Ok(RawFrame::binary(msg_id, msg_type, Encoding::Json, payload))
        }
        Encoding::Bson => {
            let payload = bson::to_vec(&Value::Object(object))?;
            Ok(RawFrame::binary(msg_id, msg_type, Encoding::Bson, payload))
        }
        Encoding::Protobuf => {
            let payload = crate::protobuf::encode_object(object)?;
            Ok(RawFrame::binary(
                msg_id,
                msg_type,
                Encoding::Protobuf,
                payload,
            ))
        }
    }
}

/// Serialize a server message as JSON text mode.
pub fn serialize_server_message_json_text(msg: &ServerMessage, msg_id: u32) -> Result<RawFrame> {
    let msg_type = server_message_type(msg);
    let mut object = server_message_object(msg);
    if msg_id != 0 {
        object.insert("id".to_string(), Value::from(msg_id));
    }
    object.insert(
        "type".to_string(),
        Value::from(server_type_byte_to_name(msg_type)?),
    );
    Ok(RawFrame::json_text(serde_json::to_vec(&Value::Object(
        object,
    ))?))
}

fn parse_client_message_body(
    msg_type: u8,
    mut object: Map<String, Value>,
) -> Result<ClientMessage> {
    let message = match msg_type {
        MSG_AUTHENTICATE => ClientMessage::Authenticate {
            token: remove_required_string(&mut object, "token")?,
        },
        MSG_PING => ClientMessage::Ping,
        MSG_BEGIN => {
            let database = remove_required_string(&mut object, "database")?;
            let readonly = remove_optional_bool(&mut object, "readonly")?.unwrap_or(false);
            let subscribe = remove_optional_bool(&mut object, "subscribe")?.unwrap_or(false);
            let notify = remove_optional_bool(&mut object, "notify")?.unwrap_or(false);
            if subscribe && notify {
                return Err(WireError::InvalidMessage(
                    "begin cannot set both subscribe and notify".to_string(),
                ));
            }
            ClientMessage::Begin {
                database,
                readonly,
                subscribe,
                notify,
            }
        }
        MSG_COMMIT => ClientMessage::Commit {
            tx: remove_required_u64(&mut object, "tx")?,
        },
        MSG_ROLLBACK => ClientMessage::Rollback {
            tx: remove_required_u64(&mut object, "tx")?,
        },
        MSG_INSERT => ClientMessage::Insert {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            body: remove_required_object_value(&mut object, "body")?,
        },
        MSG_GET => ClientMessage::Get {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            doc_id: remove_required_string(&mut object, "doc_id")?,
        },
        MSG_REPLACE => ClientMessage::Replace {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            doc_id: remove_required_string(&mut object, "doc_id")?,
            body: remove_required_object_value(&mut object, "body")?,
        },
        MSG_PATCH => ClientMessage::Patch {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            doc_id: remove_required_string(&mut object, "doc_id")?,
            body: remove_required_object_value(&mut object, "body")?,
        },
        MSG_DELETE => ClientMessage::Delete {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            doc_id: remove_required_string(&mut object, "doc_id")?,
        },
        MSG_QUERY => ClientMessage::Query {
            tx: remove_required_u64(&mut object, "tx")?,
            collection: remove_required_string(&mut object, "collection")?,
            index: remove_required_string(&mut object, "index")?,
            range: remove_optional_array(&mut object, "range")?.unwrap_or_default(),
            filter: object.remove("filter"),
            type_hints: remove_optional_meta_types(&mut object)?,
            order: remove_optional_string(&mut object, "order")?,
            limit: remove_optional_usize(&mut object, "limit")?,
        },
        MSG_CREATE_DATABASE => ClientMessage::CreateDatabase {
            name: remove_required_string(&mut object, "name")?,
            config: object.remove("config"),
        },
        MSG_DROP_DATABASE => ClientMessage::DropDatabase {
            name: remove_required_string(&mut object, "name")?,
        },
        MSG_LIST_DATABASES => ClientMessage::ListDatabases,
        MSG_CREATE_COLLECTION => ClientMessage::CreateCollection {
            database: remove_required_string(&mut object, "database")?,
            name: remove_required_string(&mut object, "name")?,
        },
        MSG_DROP_COLLECTION => ClientMessage::DropCollection {
            database: remove_required_string(&mut object, "database")?,
            name: remove_required_string(&mut object, "name")?,
        },
        MSG_LIST_COLLECTIONS => ClientMessage::ListCollections {
            database: remove_required_string(&mut object, "database")?,
        },
        MSG_CREATE_INDEX => ClientMessage::CreateIndex {
            database: remove_required_string(&mut object, "database")?,
            collection: remove_required_string(&mut object, "collection")?,
            fields: remove_required_array(&mut object, "fields")?,
            name: remove_optional_string(&mut object, "name")?,
        },
        MSG_DROP_INDEX => ClientMessage::DropIndex {
            database: remove_required_string(&mut object, "database")?,
            collection: remove_required_string(&mut object, "collection")?,
            name: remove_required_string(&mut object, "name")?,
        },
        MSG_LIST_INDEXES => ClientMessage::ListIndexes {
            database: remove_required_string(&mut object, "database")?,
            collection: remove_required_string(&mut object, "collection")?,
        },
        other => return Err(WireError::UnsupportedMessageType(other)),
    };
    reject_unknown_fields(&object)?;
    Ok(message)
}

fn decode_payload_object(encoding: Encoding, payload: &[u8]) -> Result<Map<String, Value>> {
    let value = match encoding {
        Encoding::Json => serde_json::from_slice(payload)?,
        Encoding::Bson => bson::from_slice(payload)?,
        Encoding::Protobuf => return crate::protobuf::decode_object(payload),
    };

    match value {
        Value::Object(object) => Ok(object),
        _ => Err(WireError::InvalidMessage(
            "message payload must be an object".to_string(),
        )),
    }
}

fn server_message_type(msg: &ServerMessage) -> u8 {
    match msg {
        ServerMessage::Hello { .. } => MSG_HELLO,
        ServerMessage::Ok { .. } => MSG_OK,
        ServerMessage::Error { .. } => MSG_ERROR,
        ServerMessage::Invalidation { .. } => MSG_INVALIDATION,
        ServerMessage::Pong => MSG_PONG,
        ServerMessage::IndexReady { .. } => MSG_INDEX_READY,
    }
}

fn server_message_object(msg: &ServerMessage) -> Map<String, Value> {
    let mut object = Map::new();
    match msg {
        ServerMessage::Hello {
            version,
            encodings,
            auth_required,
            node_role,
            max_message_size,
        } => {
            object.insert("version".to_string(), Value::from(version.clone()));
            object.insert("encodings".to_string(), json_array_from_strings(encodings));
            object.insert("auth_required".to_string(), Value::from(*auth_required));
            object.insert("node_role".to_string(), Value::from(node_role.clone()));
            object.insert(
                "max_message_size".to_string(),
                Value::from(*max_message_size as u64),
            );
        }
        ServerMessage::Ok { fields } => {
            if let Value::Object(fields) = fields {
                object.extend(fields.clone());
            }
        }
        ServerMessage::Error {
            code,
            message,
            extra,
        } => {
            object.insert("code".to_string(), Value::from(code.clone()));
            object.insert("message".to_string(), Value::from(message.clone()));
            if let Some(Value::Object(extra)) = extra {
                object.extend(extra.clone());
            }
        }
        ServerMessage::Invalidation {
            tx,
            queries,
            commit_ts,
            new_tx,
            new_ts,
        } => {
            object.insert("tx".to_string(), Value::from(*tx));
            object.insert(
                "queries".to_string(),
                Value::Array(queries.iter().copied().map(Value::from).collect()),
            );
            object.insert("commit_ts".to_string(), Value::from(*commit_ts));
            if let Some(new_tx) = new_tx {
                object.insert("new_tx".to_string(), Value::from(*new_tx));
            }
            if let Some(new_ts) = new_ts {
                object.insert("new_ts".to_string(), Value::from(*new_ts));
            }
        }
        ServerMessage::Pong => {}
        ServerMessage::IndexReady {
            database,
            collection,
            index,
            index_id,
        } => {
            object.insert("database".to_string(), Value::from(database.clone()));
            object.insert("collection".to_string(), Value::from(collection.clone()));
            object.insert("index".to_string(), Value::from(index.clone()));
            object.insert("index_id".to_string(), Value::from(*index_id));
        }
    }
    object
}

fn json_array_from_strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::from).collect())
}

fn client_type_name_to_byte(name: &str) -> Result<u8> {
    match name {
        "authenticate" => Ok(MSG_AUTHENTICATE),
        "ping" => Ok(MSG_PING),
        "begin" => Ok(MSG_BEGIN),
        "commit" => Ok(MSG_COMMIT),
        "rollback" => Ok(MSG_ROLLBACK),
        "insert" => Ok(MSG_INSERT),
        "get" => Ok(MSG_GET),
        "replace" => Ok(MSG_REPLACE),
        "patch" => Ok(MSG_PATCH),
        "delete" => Ok(MSG_DELETE),
        "query" => Ok(MSG_QUERY),
        "create_database" => Ok(MSG_CREATE_DATABASE),
        "drop_database" => Ok(MSG_DROP_DATABASE),
        "list_databases" => Ok(MSG_LIST_DATABASES),
        "create_collection" => Ok(MSG_CREATE_COLLECTION),
        "drop_collection" => Ok(MSG_DROP_COLLECTION),
        "list_collections" => Ok(MSG_LIST_COLLECTIONS),
        "create_index" => Ok(MSG_CREATE_INDEX),
        "drop_index" => Ok(MSG_DROP_INDEX),
        "list_indexes" => Ok(MSG_LIST_INDEXES),
        other => Err(WireError::InvalidMessage(format!(
            "unknown client message type '{other}'"
        ))),
    }
}

fn server_type_byte_to_name(byte: u8) -> Result<&'static str> {
    match byte {
        MSG_HELLO => Ok("hello"),
        MSG_OK => Ok("ok"),
        MSG_ERROR => Ok("error"),
        MSG_INVALIDATION => Ok("invalidation"),
        MSG_PONG => Ok("pong"),
        MSG_INDEX_READY => Ok("index_ready"),
        other => Err(WireError::UnsupportedMessageType(other)),
    }
}

fn remove_required_string(object: &mut Map<String, Value>, field: &str) -> Result<String> {
    match object.remove(field) {
        Some(Value::String(value)) => Ok(value),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be a string"
        ))),
        None => Err(WireError::InvalidMessage(format!(
            "missing required field '{field}'"
        ))),
    }
}

fn remove_optional_string(object: &mut Map<String, Value>, field: &str) -> Result<Option<String>> {
    match object.remove(field) {
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be a string"
        ))),
    }
}

fn remove_required_u64(object: &mut Map<String, Value>, field: &str) -> Result<u64> {
    match object.remove(field).and_then(|value| value.as_u64()) {
        Some(value) => Ok(value),
        None => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an unsigned integer"
        ))),
    }
}

fn remove_required_u32(object: &mut Map<String, Value>, field: &str) -> Result<u32> {
    let value = remove_required_u64(object, field)?;
    u32::try_from(value)
        .map_err(|_| WireError::InvalidMessage(format!("field '{field}' is too large")))
}

fn remove_optional_u32(object: &mut Map<String, Value>, field: &str) -> Result<Option<u32>> {
    let Some(value) = object.remove(field) else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        return Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an unsigned integer"
        )));
    };
    Ok(Some(u32::try_from(value).map_err(|_| {
        WireError::InvalidMessage(format!("field '{field}' is too large"))
    })?))
}

fn remove_optional_usize(object: &mut Map<String, Value>, field: &str) -> Result<Option<usize>> {
    let Some(value) = object.remove(field) else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        return Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an unsigned integer"
        )));
    };
    Ok(Some(usize::try_from(value).map_err(|_| {
        WireError::InvalidMessage(format!("field '{field}' is too large"))
    })?))
}

fn validate_client_msg_id(msg_id: u32) -> Result<()> {
    if msg_id == 0 {
        return Err(WireError::InvalidMessage(
            "client message id must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn remove_optional_bool(object: &mut Map<String, Value>, field: &str) -> Result<Option<bool>> {
    match object.remove(field) {
        Some(Value::Bool(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be a boolean"
        ))),
    }
}

fn remove_required_object_value(object: &mut Map<String, Value>, field: &str) -> Result<Value> {
    match object.remove(field) {
        Some(value @ Value::Object(_)) => Ok(value),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an object"
        ))),
        None => Err(WireError::InvalidMessage(format!(
            "missing required field '{field}'"
        ))),
    }
}

fn remove_required_array(object: &mut Map<String, Value>, field: &str) -> Result<Vec<Value>> {
    match object.remove(field) {
        Some(Value::Array(value)) => Ok(value),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an array"
        ))),
        None => Err(WireError::InvalidMessage(format!(
            "missing required field '{field}'"
        ))),
    }
}

fn remove_optional_array(
    object: &mut Map<String, Value>,
    field: &str,
) -> Result<Option<Vec<Value>>> {
    match object.remove(field) {
        Some(Value::Array(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(WireError::InvalidMessage(format!(
            "field '{field}' must be an array"
        ))),
    }
}

fn remove_optional_meta_types(object: &mut Map<String, Value>) -> Result<Option<Value>> {
    let Some(meta) = object.remove("_meta") else {
        return Ok(None);
    };
    let Value::Object(mut meta) = meta else {
        return Err(WireError::InvalidMessage(
            "field '_meta' must be an object".to_string(),
        ));
    };
    let types = meta.remove("types");
    reject_unknown_meta_fields(&meta)?;
    Ok(types)
}

fn reject_unknown_fields(object: &Map<String, Value>) -> Result<()> {
    if object.is_empty() {
        return Ok(());
    }
    let mut fields = object.keys().cloned().collect::<Vec<_>>();
    fields.sort();
    Err(WireError::InvalidMessage(format!(
        "unknown field(s): {}",
        fields.join(", ")
    )))
}

fn reject_unknown_meta_fields(object: &Map<String, Value>) -> Result<()> {
    if object.is_empty() {
        return Ok(());
    }
    let mut fields = object.keys().cloned().collect::<Vec<_>>();
    fields.sort();
    Err(WireError::InvalidMessage(format!(
        "unknown _meta field(s): {}",
        fields.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_json_message(value: Value) -> (u32, ClientMessage) {
        let frame = RawFrame::json_text(serde_json::to_vec(&value).unwrap());
        parse_client_message(&frame).unwrap()
    }

    fn encode_protobuf_payload(value: Value) -> Vec<u8> {
        crate::protobuf::encode_object(value.as_object().unwrap().clone()).unwrap()
    }

    #[test]
    fn parse_json_text_ping() {
        let frame = RawFrame::json_text(br#"{"id":2,"type":"ping"}"#.to_vec());
        let (id, msg) = parse_client_message(&frame).unwrap();

        assert_eq!(id, 2);
        assert_eq!(msg, ClientMessage::Ping);
    }

    #[test]
    fn parse_all_client_message_types() {
        let cases = [
            (
                json!({"id":1,"type":"authenticate","token":"jwt"}),
                ClientMessage::Authenticate {
                    token: "jwt".to_string(),
                },
            ),
            (json!({"id":2,"type":"ping"}), ClientMessage::Ping),
            (
                json!({"id":3,"type":"begin","database":"app","readonly":true,"subscribe":true}),
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: true,
                    notify: false,
                },
            ),
            (
                json!({"id":4,"type":"commit","tx":7}),
                ClientMessage::Commit { tx: 7 },
            ),
            (
                json!({"id":5,"type":"rollback","tx":7}),
                ClientMessage::Rollback { tx: 7 },
            ),
            (
                json!({"id":6,"type":"insert","tx":7,"collection":"users","body":{"name":"Ada"}}),
                ClientMessage::Insert {
                    tx: 7,
                    collection: "users".to_string(),
                    body: json!({"name":"Ada"}),
                },
            ),
            (
                json!({"id":7,"type":"get","tx":7,"collection":"users","doc_id":"01"}),
                ClientMessage::Get {
                    tx: 7,
                    collection: "users".to_string(),
                    doc_id: "01".to_string(),
                },
            ),
            (
                json!({"id":8,"type":"replace","tx":7,"collection":"users","doc_id":"01","body":{"name":"Grace"}}),
                ClientMessage::Replace {
                    tx: 7,
                    collection: "users".to_string(),
                    doc_id: "01".to_string(),
                    body: json!({"name":"Grace"}),
                },
            ),
            (
                json!({"id":9,"type":"patch","tx":7,"collection":"users","doc_id":"01","body":{"email":"g@example.com"}}),
                ClientMessage::Patch {
                    tx: 7,
                    collection: "users".to_string(),
                    doc_id: "01".to_string(),
                    body: json!({"email":"g@example.com"}),
                },
            ),
            (
                json!({"id":10,"type":"delete","tx":7,"collection":"users","doc_id":"01"}),
                ClientMessage::Delete {
                    tx: 7,
                    collection: "users".to_string(),
                    doc_id: "01".to_string(),
                },
            ),
            (
                json!({"id":11,"type":"query","tx":7,"collection":"users","index":"by_age","range":[{"gte":["age",18]}],"filter":{"eq":["status","active"]},"order":"desc","limit":10}),
                ClientMessage::Query {
                    tx: 7,
                    collection: "users".to_string(),
                    index: "by_age".to_string(),
                    range: vec![json!({"gte":["age",18]})],
                    filter: Some(json!({"eq":["status","active"]})),
                    type_hints: None,
                    order: Some("desc".to_string()),
                    limit: Some(10),
                },
            ),
            (
                json!({"id":12,"type":"create_database","name":"analytics","config":{"page_size":8192}}),
                ClientMessage::CreateDatabase {
                    name: "analytics".to_string(),
                    config: Some(json!({"page_size":8192})),
                },
            ),
            (
                json!({"id":13,"type":"drop_database","name":"analytics"}),
                ClientMessage::DropDatabase {
                    name: "analytics".to_string(),
                },
            ),
            (
                json!({"id":14,"type":"list_databases"}),
                ClientMessage::ListDatabases,
            ),
            (
                json!({"id":15,"type":"create_collection","database":"app","name":"users"}),
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            ),
            (
                json!({"id":16,"type":"drop_collection","database":"app","name":"users"}),
                ClientMessage::DropCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            ),
            (
                json!({"id":17,"type":"list_collections","database":"app"}),
                ClientMessage::ListCollections {
                    database: "app".to_string(),
                },
            ),
            (
                json!({"id":18,"type":"create_index","database":"app","collection":"users","fields":["email"],"name":"idx_email"}),
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!("email")],
                    name: Some("idx_email".to_string()),
                },
            ),
            (
                json!({"id":19,"type":"drop_index","database":"app","collection":"users","name":"idx_email"}),
                ClientMessage::DropIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    name: "idx_email".to_string(),
                },
            ),
            (
                json!({"id":20,"type":"list_indexes","database":"app","collection":"users"}),
                ClientMessage::ListIndexes {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                },
            ),
        ];

        for (i, (input, expected)) in cases.into_iter().enumerate() {
            let (id, msg) = parse_json_message(input);
            assert_eq!(id, (i + 1) as u32);
            assert_eq!(msg, expected);
        }
    }

    #[test]
    fn begin_rejects_subscribe_and_notify_together() {
        let frame = RawFrame::json_text(
            br#"{"id":1,"type":"begin","database":"app","subscribe":true,"notify":true}"#.to_vec(),
        );

        assert!(matches!(
            parse_client_message(&frame),
            Err(WireError::InvalidMessage(_))
        ));
    }

    #[test]
    fn client_message_id_zero_is_rejected() {
        let json_frame = RawFrame::json_text(br#"{"id":0,"type":"ping"}"#.to_vec());
        let json_err = parse_client_message(&json_frame).unwrap_err();
        assert!(
            json_err
                .to_string()
                .contains("client message id must be greater than zero")
        );

        let binary_frame = RawFrame::binary(0, MSG_PING, Encoding::Json, br#"{}"#.to_vec());
        let binary_err = parse_client_message(&binary_frame).unwrap_err();
        assert!(
            binary_err
                .to_string()
                .contains("client message id must be greater than zero")
        );
    }

    #[test]
    fn parse_binary_bson_insert() {
        let payload = bson::to_vec(&json!({
            "tx": 9u64,
            "collection": "users",
            "body": {"name": "Alice"}
        }))
        .unwrap();
        let frame = RawFrame::binary(4, MSG_INSERT, Encoding::Bson, payload);

        let (id, msg) = parse_client_message(&frame).unwrap();

        assert_eq!(id, 4);
        assert_eq!(
            msg,
            ClientMessage::Insert {
                tx: 9,
                collection: "users".to_string(),
                body: json!({"name": "Alice"}),
            }
        );
    }

    #[test]
    fn parse_binary_protobuf_insert() {
        let payload = encode_protobuf_payload(json!({
            "tx": u64::MAX,
            "collection": "users",
            "body": {"name": "Alice", "rank": 9}
        }));
        let frame = RawFrame::binary(4, MSG_INSERT, Encoding::Protobuf, payload);

        let (id, msg) = parse_client_message(&frame).unwrap();

        assert_eq!(id, 4);
        assert_eq!(
            msg,
            ClientMessage::Insert {
                tx: u64::MAX,
                collection: "users".to_string(),
                body: json!({"name": "Alice", "rank": 9}),
            }
        );
    }

    #[test]
    fn parse_query_defaults_range() {
        let frame = RawFrame::json_text(
            br#"{"id":9,"type":"query","tx":1,"collection":"users","index":"_created_at"}"#
                .to_vec(),
        );
        let (_, msg) = parse_client_message(&frame).unwrap();

        assert_eq!(
            msg,
            ClientMessage::Query {
                tx: 1,
                collection: "users".to_string(),
                index: "_created_at".to_string(),
                range: Vec::new(),
                filter: None,
                type_hints: None,
                order: None,
                limit: None,
            }
        );
    }

    #[test]
    fn parse_query_preserves_meta_type_hints() {
        let frame = RawFrame::json_text(
            br#"{"id":9,"type":"query","tx":1,"collection":"users","index":"avatar_idx","range":[{"eq":["avatar","AQID"]}],"filter":{"eq":["avatar","AQID"]},"_meta":{"types":{"range":[{"eq":"bytes"}],"filter":{"eq":"bytes"}}}}"#
                .to_vec(),
        );
        let (_, msg) = parse_client_message(&frame).unwrap();

        match msg {
            ClientMessage::Query {
                range,
                filter,
                type_hints,
                ..
            } => {
                assert_eq!(range, vec![json!({"eq": ["avatar", "AQID"]})]);
                assert_eq!(filter, Some(json!({"eq": ["avatar", "AQID"]})));
                assert_eq!(
                    type_hints,
                    Some(json!({
                        "range": [{"eq": "bytes"}],
                        "filter": {"eq": "bytes"}
                    }))
                );
            }
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn binary_payload_type_must_match_header() {
        let frame = RawFrame::binary(
            1,
            MSG_PING,
            Encoding::Json,
            br#"{"type":"begin","database":"app"}"#.to_vec(),
        );

        assert!(matches!(
            parse_client_message(&frame),
            Err(WireError::InvalidMessage(_))
        ));
    }

    #[test]
    fn unknown_top_level_fields_are_rejected() {
        let frame = RawFrame::json_text(br#"{"id":1,"type":"ping","surprise":true}"#.to_vec());

        let err = parse_client_message(&frame).unwrap_err();
        assert!(err.to_string().contains("unknown field(s): surprise"));
    }

    #[test]
    fn binary_unknown_top_level_fields_are_rejected() {
        let frame = RawFrame::binary(
            1,
            MSG_PING,
            Encoding::Json,
            br#"{"surprise":true}"#.to_vec(),
        );

        let err = parse_client_message(&frame).unwrap_err();
        assert!(err.to_string().contains("unknown field(s): surprise"));
    }

    #[test]
    fn query_meta_must_be_an_object() {
        let frame = RawFrame::json_text(
            br#"{"id":1,"type":"query","tx":1,"collection":"users","index":"by_age","_meta":true}"#
                .to_vec(),
        );

        let err = parse_client_message(&frame).unwrap_err();
        assert!(err.to_string().contains("field '_meta' must be an object"));
    }

    #[test]
    fn query_meta_rejects_unknown_fields() {
        let frame = RawFrame::json_text(
            br#"{"id":1,"type":"query","tx":1,"collection":"users","index":"by_age","_meta":{"types":{},"debug":true}}"#
                .to_vec(),
        );

        let err = parse_client_message(&frame).unwrap_err();
        assert!(err.to_string().contains("unknown _meta field(s): debug"));
    }

    #[test]
    fn serialize_json_text_error_omits_zero_id() {
        let frame = serialize_server_message_json_text(
            &ServerMessage::Error {
                code: "invalid_message".to_string(),
                message: "bad request".to_string(),
                extra: Some(json!({"detail": "missing tx"})),
            },
            0,
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&frame.payload).unwrap();

        assert_eq!(value["type"], "error");
        assert_eq!(value["code"], "invalid_message");
        assert_eq!(value["detail"], "missing tx");
        assert!(value.get("id").is_none());
    }

    #[test]
    fn serialize_binary_bson_ok() {
        let frame = serialize_server_message(
            &ServerMessage::Ok {
                fields: json!({"commit_ts": 42u64}),
            },
            11,
            Encoding::Bson,
        )
        .unwrap();

        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, 11);
        assert_eq!(frame.msg_type, MSG_OK);
        assert_eq!(frame.encoding, Encoding::Bson);

        let payload: Value = bson::from_slice(&frame.payload).unwrap();
        assert_eq!(payload["commit_ts"], 42);
        assert!(payload.get("type").is_none());
    }

    #[test]
    fn serialize_binary_protobuf_ok() {
        let frame = serialize_server_message(
            &ServerMessage::Ok {
                fields: json!({"commit_ts": u64::MAX, "fraction": 1.25}),
            },
            11,
            Encoding::Protobuf,
        )
        .unwrap();

        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, 11);
        assert_eq!(frame.msg_type, MSG_OK);
        assert_eq!(frame.encoding, Encoding::Protobuf);

        let payload = crate::protobuf::decode_object(&frame.payload).unwrap();
        assert_eq!(payload["commit_ts"], u64::MAX);
        assert_eq!(payload["fraction"], 1.25);
        assert!(payload.get("type").is_none());
    }

    #[test]
    fn serialize_all_server_message_types_as_json_text() {
        let cases = [
            (
                ServerMessage::Hello {
                    version: "0.1.0".to_string(),
                    encodings: vec!["json".to_string(), "bson".to_string()],
                    auth_required: true,
                    node_role: "primary".to_string(),
                    max_message_size: 1024,
                },
                "hello",
            ),
            (
                ServerMessage::Ok {
                    fields: json!({"tx": 1}),
                },
                "ok",
            ),
            (
                ServerMessage::Error {
                    code: "conflict".to_string(),
                    message: "OCC conflict".to_string(),
                    extra: Some(json!({"new_tx": 2, "new_ts": 3})),
                },
                "error",
            ),
            (
                ServerMessage::Invalidation {
                    tx: 1,
                    queries: vec![0, 2],
                    commit_ts: 50,
                    new_tx: Some(3),
                    new_ts: Some(50),
                },
                "invalidation",
            ),
            (ServerMessage::Pong, "pong"),
            (
                ServerMessage::IndexReady {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    index: "idx_email".to_string(),
                    index_id: 5,
                },
                "index_ready",
            ),
        ];

        for (msg, expected_type) in cases {
            let frame = serialize_server_message_json_text(&msg, 99).unwrap();
            let value: Value = serde_json::from_slice(&frame.payload).unwrap();
            assert_eq!(value["type"], expected_type);
            assert_eq!(value["id"], 99);
        }
    }
}
