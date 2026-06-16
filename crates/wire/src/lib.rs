//! exdb-wire — Layer 8: API & Protocol
//!
//! Network protocol layer: frame format, client/server messages,
//! session state machine, authentication, and transport (TCP/WS).

pub mod auth;
pub mod error;
pub mod frame;
pub mod messages;
mod protobuf;
pub mod session;
pub mod transport;

pub use auth::{
    AuthClaims, AuthConfig, JwtAlgorithm, Role, check_admin_access, check_database_access,
    validate_token,
};
pub use error::{Result, WireError};
pub use frame::{
    BINARY_HEADER_LEN, BinaryFrameHeader, DEFAULT_MAX_MESSAGE_SIZE, Encoding, FrameType,
    PROTOCOL_VERSION, RawFrame, read_frame, read_frame_with_limit, write_frame,
    write_frame_with_limit,
};
pub use messages::{
    ClientMessage, ServerMessage, parse_client_message, serialize_server_message,
    serialize_server_message_json_text,
};
pub use session::{
    DEFAULT_NODE_ROLE, DdlPromotionRequest, NODE_ROLE_PRIMARY, NODE_ROLE_REPLICA, ReplicaReadGate,
    Session, TransactionPromoter, TransactionPromotionOutcome, database_config_from_json,
};
pub use transport::{
    DEFAULT_REQUEST_QUEUE_CAPACITY, DEFAULT_RESPONSE_WRITE_TIMEOUT, ListenConfig, Server,
    ServerConfig, TlsConfig, handle_connection_with_session,
    handle_connection_with_session_and_queue,
};
