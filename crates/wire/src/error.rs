//! Error types for the wire protocol layer.

use std::io;

/// Result type used by the wire protocol layer.
pub type Result<T> = std::result::Result<T, WireError>;

/// Errors produced while framing, decoding, or encoding protocol messages.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("BSON serialization error: {0}")]
    BsonSer(#[from] bson::ser::Error),

    #[error("BSON deserialization error: {0}")]
    BsonDe(#[from] bson::de::Error),

    #[error("Protobuf error: {0}")]
    Protobuf(String),

    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),

    #[error("unsupported payload encoding {0:#04x}")]
    UnsupportedEncoding(u8),

    #[error("unsupported message type {0:#04x}")]
    UnsupportedMessageType(u8),

    #[error("unsupported frame flags {0:#04x}")]
    UnsupportedFlags(u8),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("message payload length {length} exceeds limit {limit}")]
    MessageTooLarge { length: usize, limit: usize },

    #[error("message payload length {length} exceeds limit {limit}")]
    BinaryMessageTooLarge {
        msg_id: u32,
        msg_type: u8,
        encoding: u8,
        length: usize,
        limit: usize,
    },

    #[error("transport write timed out after {timeout_ms} ms")]
    WriteTimeout { timeout_ms: u64 },

    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("QUIC error: {0}")]
    Quic(String),

    #[error("malformed frame: {0}")]
    MalformedFrame(&'static str),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("authentication required")]
    AuthRequired,

    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("forbidden: {0}")]
    Forbidden(String),
}
