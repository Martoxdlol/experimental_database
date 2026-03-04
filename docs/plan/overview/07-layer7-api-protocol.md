# Layer 7: API / Protocol

## Purpose

Client-facing transport, message framing, authentication, and session management. Translates wire protocol messages into transaction/query operations.

## Sub-Modules

### `protocol/framing.rs` — Frame Format

```rust
pub enum FrameMode {
    JsonText,   // First byte 0x7B ('{') → read line, parse JSON
    Binary,     // Otherwise → 12-byte header + payload
}

pub struct BinaryFrameHeader {
    pub version: u8,        // 0x01
    pub flags: u8,          // bit 0 = LZ4 compressed
    pub encoding: Encoding, // JSON(0x01) / BSON(0x02) / Protobuf(0x03)
    pub msg_type: u8,       // Message type code
    pub msg_id: u32,        // Client-assigned
    pub length: u32,        // Payload length
}

pub enum Encoding { Json = 0x01, Bson = 0x02, Protobuf = 0x03 }

pub trait FrameReader {
    async fn read_frame(&mut self) -> Result<RawMessage>;
}

pub trait FrameWriter {
    async fn write_frame(&mut self, msg: &ServerMessage, encoding: Encoding) -> Result<()>;
}

pub struct RawMessage {
    pub mode: FrameMode,
    pub encoding: Encoding,
    pub msg_type: u8,
    pub msg_id: u32,
    pub payload: Vec<u8>,
}
```

### `protocol/messages.rs` — Message Type Definitions

```rust
// Client → Server
pub enum ClientMessage {
    Authenticate { token: String },
    Ping,
    Begin { database: String, readonly: bool, subscribe: bool, notify: bool },
    Commit { tx: TxId },
    Rollback { tx: TxId },
    Insert { tx: TxId, collection: String, body: Document },
    Get { tx: TxId, collection: String, doc_id: DocId },
    Replace { tx: TxId, collection: String, doc_id: DocId, body: Document },
    Patch { tx: TxId, collection: String, doc_id: DocId, body: Document },
    Delete { tx: TxId, collection: String, doc_id: DocId },
    Query { tx: TxId, collection: String, index: String, range: Vec<RangeExpr>,
            filter: Option<Filter>, order: ScanDirection, limit: Option<usize> },
    CreateDatabase { name: String, config: Option<DatabaseConfig> },
    DropDatabase { name: String },
    ListDatabases,
    CreateCollection { database: String, name: String },
    DropCollection { database: String, name: String },
    ListCollections { database: String },
    CreateIndex { database: String, collection: String, fields: Vec<FieldPath>, name: Option<String> },
    DropIndex { database: String, collection: String, name: String },
    ListIndexes { database: String, collection: String },
}

// Server → Client
pub enum ServerMessage {
    Hello { version: String, encodings: Vec<String>, auth_required: bool, node_role: String, max_message_size: usize },
    Ok { msg_id: u32, payload: OkPayload },
    Error { msg_id: u32, code: ErrorCode, message: String, new_tx: Option<(TxId, Ts)> },
    Invalidation { tx: TxId, queries: Vec<QueryId>, commit_ts: Ts, new_tx: Option<(TxId, Ts)> },
    Pong { msg_id: u32 },
    IndexReady { database: String, collection: String, index: String, index_id: IndexId },
}

pub enum OkPayload {
    Empty,
    Tx(TxId),
    CommitTs(Ts),
    DocId(DocId),
    Doc { query_id: QueryId, doc: Option<Document> },
    Docs { query_id: QueryId, docs: Vec<Document> },
    Databases(Vec<DatabaseInfo>),
    Collections(Vec<CollectionInfo>),
    Indexes(Vec<IndexInfo>),
    IndexId(IndexId),
}
```

### `protocol/session.rs` — Connection State Machine

```rust
pub struct Session {
    pub authenticated: bool,
    pub current_encoding: Encoding,
    pub seen_msg_ids: HashSet<u32>,          // Deduplication
    pub transactions: HashMap<TxId, Transaction>,
    pub subscriptions: HashMap<SubscriptionId, TxId>,
    pub invalidation_tx: mpsc::Sender<ServerMessage>,  // Push notifications
}

impl Session {
    pub async fn handle_message(
        &mut self,
        msg: ClientMessage,
        system: &SystemDatabase,
    ) -> ServerMessage;

    // Process per category with ordering guarantees
    async fn handle_connection_message(&mut self, msg: ClientMessage) -> ServerMessage;
    async fn handle_transaction_message(&mut self, msg: ClientMessage) -> ServerMessage;
    async fn handle_management_message(&mut self, msg: ClientMessage) -> ServerMessage;
}
```

### `protocol/auth.rs` — JWT Authentication

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
    pub role: Role,   // Admin or User
    pub exp: u64,
}

pub fn validate_token(token: &str, config: &AuthConfig) -> Result<AuthClaims>;
pub fn check_database_access(claims: &AuthClaims, database: &str) -> Result<()>;
pub fn check_admin_access(claims: &AuthClaims) -> Result<()>;
```

### `server.rs` — Transport Listeners

```rust
pub struct Server {
    pub config: ServerConfig,
    pub system: Arc<SystemDatabase>,
}

impl Server {
    pub async fn start(config: ServerConfig) -> Result<Self>;

    // One listener per transport
    async fn listen_tcp(&self, addr: SocketAddr) -> Result<()>;
    async fn listen_tls(&self, addr: SocketAddr, tls_config: TlsConfig) -> Result<()>;
    async fn listen_quic(&self, addr: SocketAddr, tls_config: TlsConfig) -> Result<()>;
    async fn listen_websocket(&self, addr: SocketAddr) -> Result<()>;

    // Per-connection task
    async fn handle_connection(&self, stream: impl AsyncRead + AsyncWrite) -> Result<()>;
}
```

## Interfaces

### Depends On

| Layer | Interface |
|---|---|
| Layer 5 | `TransactionManager::begin/commit/rollback()` |
| Layer 4 | `plan_query()`, `execute_query()` (via transaction) |
| Layer 1 | `decode_ulid()`, `json_to_document()`, `Filter`/`RangeExpr` parsing |
| `system.rs` | `SystemDatabase::create_database()`, etc. |

### Exposes To

| Consumer | Interface |
|---|---|
| External clients | TCP/TLS/QUIC/WebSocket connections with JSON/BSON/Protobuf messages |
| `main.rs` | `Server::start()` |

## Connection Lifecycle Diagram

```
  Client                          Server
    │                               │
    │◀─── TCP connect ─────────────│
    │                               │
    │◀─── hello (JSON) ────────────│
    │     { version, auth_required, │
    │       node_role, encodings }  │
    │                               │
    │── authenticate ──────────────▶│
    │     { token: "eyJ..." }       │── validate JWT
    │                               │
    │◀─── ok ──────────────────────│
    │                               │
    │── begin ─────────────────────▶│
    │     { database: "myapp" }     │── TransactionManager.begin()
    │                               │
    │◀─── ok { tx: 1 } ───────────│
    │                               │
    │── insert ────────────────────▶│
    │── query ─────────────────────▶│  (pipelined)
    │── commit ────────────────────▶│
    │                               │
    │◀─── ok { doc_id } ──────────│
    │◀─── ok { query_id, docs } ──│
    │◀─── ok { commit_ts } ───────│
    │                               │
    │  ... subscription active ...  │
    │                               │
    │◀─── invalidation ───────────│  (server-initiated push)
    │     { tx:1, queries:[0,2],   │
    │       new_tx:2, new_ts:50 }  │
    │                               │
    │── query (re-execute) ────────▶│  (in new tx)
    │── commit ────────────────────▶│
    │                               │
```

## Message Processing Concurrency

```
  Connection's incoming message stream
          │
          ▼
  ┌───────────────────────┐
  │  Categorize by type   │
  └──┬──────┬──────┬──────┘
     │      │      │
     ▼      ▼      ▼
  ┌─────┐ ┌─────┐ ┌──────────┐
  │Conn │ │ Tx  │ │Management│
  │(seq)│ │msgs │ │  (async) │
  └─────┘ └──┬──┘ └──────────┘
             │
    Per-transaction ordering:
    ┌────────┴────────┐
    │ tx:1 messages   │ → sequential
    │ tx:2 messages   │ → sequential
    │ (different txs  │
    │  run concurrent)│
    └─────────────────┘
```
