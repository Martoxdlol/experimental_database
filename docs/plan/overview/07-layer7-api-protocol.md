# Layer 7: API & Protocol

**Layer purpose:** Frame format with JSON text + binary auto-detect, message types, session state machine, JWT authentication, and transport listeners. This is the interface between clients and the database engine.

## Modules

### `frame.rs` — Wire Frame Format

**WHY HERE:** Protocol-level framing and encoding. Pure I/O format handling with no business logic.

```rust
/// Auto-detect frame type by first byte
pub enum FrameType {
    JsonText,  // first byte = 0x7B ('{')
    Binary,    // first byte = protocol version
}

/// Binary frame header (12 bytes)
pub struct BinaryFrameHeader {
    pub version: u8,      // 0x01
    pub flags: u8,        // bit 0 = LZ4 compressed
    pub encoding: Encoding,
    pub msg_type: u8,
    pub msg_id: u32,
    pub length: u32,
}

pub enum Encoding {
    Json     = 0x01,
    Bson     = 0x02,
    Protobuf = 0x03,
}

/// Read a frame from a stream transport (auto-detecting JSON vs binary)
pub async fn read_frame(reader: &mut impl AsyncRead) -> Result<RawFrame>;

/// Write a frame to a stream transport
pub async fn write_frame(writer: &mut impl AsyncWrite, frame: &RawFrame) -> Result<()>;

pub struct RawFrame {
    pub frame_type: FrameType,
    pub msg_id: u32,
    pub msg_type: u8,
    pub payload: Vec<u8>,
    pub encoding: Encoding,
}
```

### `messages.rs` — Message Types and Serialization

**WHY HERE:** Defines all client↔server message schemas and their serialization. Protocol-specific definitions.

```rust
/// Client → Server message types (0x01-0x3F)
pub enum ClientMessage {
    Authenticate { token: String },
    Ping,
    Begin { database: String, readonly: bool, subscribe: bool, notify: bool },
    Commit { tx: TxId },
    Rollback { tx: TxId },
    Insert { tx: TxId, collection: String, body: serde_json::Value },
    Get { tx: TxId, collection: String, doc_id: String },
    Replace { tx: TxId, collection: String, doc_id: String, body: serde_json::Value },
    Patch { tx: TxId, collection: String, doc_id: String, body: serde_json::Value },
    Delete { tx: TxId, collection: String, doc_id: String },
    Query { tx: TxId, collection: String, index: String,
            range: Vec<serde_json::Value>, filter: Option<serde_json::Value>,
            order: Option<String>, limit: Option<usize> },
    CreateDatabase { name: String, config: Option<serde_json::Value> },
    DropDatabase { name: String },
    ListDatabases,
    CreateCollection { database: String, name: String },
    DropCollection { database: String, name: String },
    ListCollections { database: String },
    CreateIndex { database: String, collection: String, fields: Vec<serde_json::Value>,
                  name: Option<String> },
    DropIndex { database: String, collection: String, name: String },
    ListIndexes { database: String, collection: String },
}

/// Server → Client message types (0x80-0xFF)
pub enum ServerMessage {
    Hello { version: String, encodings: Vec<String>, auth_required: bool,
            node_role: String, max_message_size: usize },
    Ok { fields: serde_json::Value },
    Error { code: String, message: String, extra: Option<serde_json::Value> },
    Invalidation { tx: TxId, queries: Vec<u32>, commit_ts: Ts,
                   new_tx: Option<TxId>, new_ts: Option<Ts> },
    Pong,
    IndexReady { database: String, collection: String, index: String, index_id: IndexId },
}

/// Parse a raw frame into a typed message
pub fn parse_client_message(frame: &RawFrame) -> Result<(u32, ClientMessage)>;

/// Serialize a server message into a raw frame
pub fn serialize_server_message(msg: &ServerMessage, msg_id: u32,
                                 encoding: Encoding) -> Result<RawFrame>;
```

### `session.rs` — Session State Machine

**WHY HERE:** Manages per-connection state: authentication status, active transactions, encoding mode. Processes messages in the correct order (auth blocks, same-tx serial, cross-tx concurrent).

```rust
pub struct Session {
    id: u64,
    authenticated: bool,
    auth_claims: Option<AuthClaims>,
    current_encoding: Encoding,
    active_transactions: HashMap<TxId, TransactionState>,
    database_handle: Option<Arc<Database>>,
}

struct TransactionState {
    begin_ts: Ts,
    read_set: ReadSet,
    write_set: WriteSet,
    readonly: bool,
    subscribe: bool,
    notify: bool,
    next_query_id: u32,
}

impl Session {
    pub fn new(id: u64) -> Self;

    /// Process a client message and produce a server response
    pub async fn handle_message(&mut self, msg: ClientMessage) -> ServerMessage;

    /// Push a server-initiated notification (invalidation, index_ready)
    pub fn push_notification(&self, msg: ServerMessage);
}
```

### `auth.rs` — JWT Authentication

**WHY HERE:** Token validation and claims extraction. Security layer at the protocol boundary.

```rust
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: JwtAlgorithm,
    pub secret: Option<Vec<u8>>,        // HMAC
    pub public_key: Option<Vec<u8>>,    // RSA/EC
    pub issuer: Option<String>,
}

pub struct AuthClaims {
    pub sub: Option<String>,
    pub databases: Option<Vec<String>>,  // None = all databases
    pub role: Role,
    pub exp: u64,
}

pub enum Role { Admin, User }

pub enum JwtAlgorithm { HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384 }

/// Validate a JWT token and extract claims
pub fn validate_token(token: &str, config: &AuthConfig) -> Result<AuthClaims>;

/// Check if claims authorize access to a specific database
pub fn check_database_access(claims: &AuthClaims, database: &str) -> bool;

/// Check if claims authorize management operations
pub fn check_admin_access(claims: &AuthClaims) -> bool;
```

### `transport.rs` — Transport Listeners

**WHY HERE:** Accepts connections over TCP/TLS/QUIC/WebSocket and spawns session tasks.

```rust
pub struct TransportConfig {
    pub tcp_addr: Option<SocketAddr>,
    pub tls_addr: Option<SocketAddr>,
    pub quic_addr: Option<SocketAddr>,
    pub ws_addr: Option<SocketAddr>,
    pub tls_config: Option<TlsConfig>,
    pub max_message_size: usize,
}

pub struct Server {
    config: TransportConfig,
    database_registry: Arc<DatabaseRegistry>,
    auth_config: AuthConfig,
}

impl Server {
    pub fn new(config: TransportConfig, registry: Arc<DatabaseRegistry>,
               auth_config: AuthConfig) -> Self;

    /// Start all configured transport listeners
    pub async fn start(&self, shutdown: CancellationToken) -> Result<()>;

    /// Handle a single connection (any transport)
    async fn handle_connection(&self, stream: impl AsyncRead + AsyncWrite) -> Result<()>;
}
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `Server::start` | Integration (main) | Start the server |
| `Session::handle_message` | Internal (transport) | Message processing |
| `ClientMessage`, `ServerMessage` | Internal (session) | Typed messages |

## Error Code Mapping

| Error Code | HTTP-like | Trigger |
|-----------|-----------|---------|
| `auth_required` | 401 | Not authenticated |
| `auth_failed` | 401 | Bad token |
| `forbidden` | 403 | Insufficient permissions |
| `unknown_database` | 404 | Database not found |
| `unknown_collection` | 404 | Collection not found |
| `conflict` | 409 | OCC validation failed |
| `invalid_message` | 400 | Malformed message |
| `internal` | 500 | Server error |
