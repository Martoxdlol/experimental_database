# Layer 8: API & Protocol (Optional)

**Layer purpose:** Expose the Database API over the network. Frame format with JSON text + binary auto-detect, message types, session state machine, JWT authentication, and transport listeners. This layer adds framing, sessions, and auth — but no business logic.

## Design Principles

1. **Thin wrapper**: Every operation maps directly to a `Database` or `SystemDatabase` method call. No business logic here.
2. **Database doesn't know about this layer**: L8 depends on L6 (Database), never the reverse.
3. **Optional**: The database is fully usable without this layer (embedded mode).

## Modules

### `frame.rs` — Wire Frame Format

**WHY HERE:** Protocol-level framing and encoding. Pure I/O format handling.

```rust
pub enum FrameType {
    JsonText,  // first byte = 0x7B ('{')
    Binary,    // first byte = protocol version
}

pub struct BinaryFrameHeader {
    pub version: u8,
    pub flags: u8,        // bit 0 = LZ4 compressed
    pub encoding: Encoding,
    pub msg_type: u8,
    pub msg_id: u32,
    pub length: u32,
}

pub enum Encoding { Json = 0x01, Bson = 0x02, Protobuf = 0x03 }

pub async fn read_frame(reader: &mut impl AsyncRead) -> Result<RawFrame>;
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

**WHY HERE:** Defines all client-server message schemas. Protocol-specific definitions.

```rust
pub enum ClientMessage {
    Authenticate { token: String },
    Ping,
    Begin { database: String, readonly: bool, subscribe: bool, notify: bool },
    Commit { tx: TxId },
    Rollback { tx: TxId },

    // --- Document operations (require active transaction) ---
    Insert { tx: TxId, collection: String, body: serde_json::Value },
    Get { tx: TxId, collection: String, doc_id: String },
    Replace { tx: TxId, collection: String, doc_id: String, body: serde_json::Value },
    Patch { tx: TxId, collection: String, doc_id: String, body: serde_json::Value },
    Delete { tx: TxId, collection: String, doc_id: String },
    Query { tx: TxId, collection: String, index: String,
            range: Vec<serde_json::Value>, filter: Option<serde_json::Value>,
            order: Option<String>, limit: Option<usize> },

    // --- Collection/index DDL (require active mutation transaction) ---
    // These are transactional: buffered in write set, applied at commit.
    CreateCollection { tx: TxId, name: String },
    DropCollection { tx: TxId, name: String },
    ListCollections { tx: TxId },
    CreateIndex { tx: TxId, collection: String, fields: Vec<serde_json::Value>,
                  name: Option<String> },
    DropIndex { tx: TxId, collection: String, name: String },
    ListIndexes { tx: TxId, collection: String },

    // --- Database management (not transactional — operates on SystemDatabase) ---
    CreateDatabase { name: String, config: Option<serde_json::Value> },
    DropDatabase { name: String },
    ListDatabases,
}

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

pub fn parse_client_message(frame: &RawFrame) -> Result<(u32, ClientMessage)>;
pub fn serialize_server_message(msg: &ServerMessage, msg_id: u32,
                                 encoding: Encoding) -> Result<RawFrame>;
```

### `session.rs` — Session State Machine

**WHY HERE:** Manages per-connection state. Maps protocol messages to Database API calls.

```rust
pub struct Session {
    id: u64,
    authenticated: bool,
    auth_claims: Option<AuthClaims>,
    current_encoding: Encoding,
    active_transactions: HashMap<TxId, TransactionHandle>,
    database_registry: Arc<SystemDatabase>,
}

/// Wraps ReadonlyTransaction or MutationTransaction
enum TransactionHandle {
    Readonly(ReadonlyTransaction),
    Mutation(MutationTransaction),
}

impl Session {
    pub fn new(id: u64, registry: Arc<SystemDatabase>) -> Self;

    /// Process a client message → Database API call → server response
    pub async fn handle_message(&mut self, msg: ClientMessage) -> ServerMessage;

    /// Push a server-initiated notification (invalidation, index_ready)
    pub fn push_notification(&self, msg: ServerMessage);
}
```

### `auth.rs` — JWT Authentication

**WHY HERE:** Token validation at the protocol boundary. The database itself has no auth concept.

```rust
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: JwtAlgorithm,
    pub secret: Option<Vec<u8>>,
    pub public_key: Option<Vec<u8>>,
    pub issuer: Option<String>,
}

pub struct AuthClaims {
    pub sub: Option<String>,
    pub databases: Option<Vec<String>>,
    pub role: Role,
    pub exp: u64,
}

pub enum Role { Admin, User }
pub enum JwtAlgorithm { HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384 }

pub fn validate_token(token: &str, config: &AuthConfig) -> Result<AuthClaims>;
pub fn check_database_access(claims: &AuthClaims, database: &str) -> bool;
pub fn check_admin_access(claims: &AuthClaims) -> bool;
```

### `transport.rs` — Transport Listeners

**WHY HERE:** Accepts connections over TCP/TLS/QUIC/WebSocket and spawns session tasks.

```rust
pub struct ServerConfig {
    pub listen: ListenConfig,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub max_message_size: usize,
    pub replication: Option<ReplicationConfig>,
}

pub struct ListenConfig {
    pub tcp: Option<SocketAddr>,
    pub tls: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

pub struct Server {
    config: ServerConfig,
    database_registry: Arc<SystemDatabase>,
}

impl Server {
    pub fn new(config: ServerConfig, registry: Arc<SystemDatabase>) -> Self;

    /// Start all configured transport listeners
    pub async fn start(&self, shutdown: CancellationToken) -> Result<()>;

    /// Handle a single connection (any transport)
    async fn handle_connection(&self, stream: impl AsyncRead + AsyncWrite) -> Result<()>;
}
```

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

## Interfaces Exposed

This is the top layer — it exposes nothing upward. It consumes:

| Consumed From | Interface | Purpose |
|---------------|-----------|---------|
| L6 (Database) | `Database::*` | All database operations |
| L6 (Database) | `SystemDatabase::*` | Multi-database management |
| L6 (Database) | `ReplicationHook` trait | May construct and inject L7 replicator |
| L6 (Database) | `SubscriptionRegistry` | Push invalidation notifications |
