# Layer 8: API & Protocol (Optional)

**Layer purpose:** Expose the Database API over the network. Frame format with JSON text + binary auto-detect, message types, session state machine, JWT authentication, and transport listeners. This layer adds framing, sessions, and auth — but no business logic.

## Design Principles

1. **Thin wrapper**: Every operation maps directly to a `Database` or `SystemDatabase` method call. No business logic here.
2. **Database doesn't know about this layer**: L8 depends on L6 (Database), never the reverse.
3. **Optional**: The database is fully usable without this layer (embedded mode).

## Modules

**Implementation note:** `exdb-wire` currently implements the protocol error,
frame, message-schema, auth, and session-core foundations. It supports JSON
text framing, binary JSON/BSON frames, first-byte frame detection,
protocol/header validation, strict unknown-field and `_meta` validation,
typed client-message parsing, server-message serialization, binary BSON CRUD
round trips through the transport/session/data
path, JWT validation, role/database authorization helpers, hello
metadata, auth-required and active-expiration enforcement, duplicate message ID
suppression, ping, database management, collection list/create/drop, index create/drop/list,
transaction begin/commit/rollback, and core
insert/get/replace/patch/delete/query data operations, including server-assigned
`query_id` fields in `get` and `query` responses. JSON document bodies honor
`_meta.types` for bytes/id storage metadata and bytes response metadata is
covered through session insert/commit/get tests. JSON query predicates carry
query-level `_meta.types` into range and post-filter scalar parsing for
bytes/id/int64/float64 disambiguation. The transport core now handles
per-connection request loops, pushed invalidation notifications, and a
cancellable TCP listener. Subscribe-mode continuation and OCC retry `new_tx`
identifiers are materialized as active session transactions. `apps/server` now
opens the database registry, parses CLI/JSON config including
`default_database_config`, and starts the TCP server.
`apps/cli` now supports embedded path operations, high-level JSON text commands
against a TCP server, interactive REPL sessions over the same command executor,
and raw JSON text requests to a TCP server. Background index builds now emit
pushed `index_ready` notifications through the same server-initiated transport
path as invalidations.
The TCP connection loop accepts pipelined input from clients, including
same-transaction message bursts, buffers parsed requests through an explicit
bounded per-connection queue, dispatches management messages asynchronously
through a per-connection response queue with an in-flight management budget,
dispatches transaction-scoped messages through per-transaction workers so
different transaction IDs can make progress concurrently, preserves
same-transaction response order, accounts for per-connection
same-transaction scheduler backlog, active transaction-worker fan-out, and
in-flight management work with `server_busy` when those budgets are full, and
uses bounded per-connection push-notification queues plus a configurable
per-frame response write timeout to close slow clients.
Transport coverage now drives one JSON-stream connection through database
create/list, collection create/list, index create/list, pushed `index_ready`,
indexed query readback, index drop, collection drop, database drop, and final
list verification.
Transport tests now also cover a mixed JSON/BSON/Protobuf connection,
including the same lifecycle through real TCP, TLS-over-TCP, QUIC, plain
WebSocket, and secure WebSocket listeners, where request-correlated binary
responses and server-pushed invalidations use the expected frame headers and
most-recent client encoding. TCP, TLS-over-TCP, QUIC, plain WebSocket, and
secure WebSocket listener coverage also combine JWT auth with mixed
JSON/BSON/Protobuf framing, including binary `auth_required` rejection before
authentication followed by authenticated mixed-encoding CRUD/readback.
TCP, TLS-over-TCP, QUIC, plain WebSocket, and secure WebSocket listener coverage
additionally pipeline `authenticate` and readonly replica `begin` back-to-back,
proving queued post-auth reads see the updated authenticated state and fail at
the read-quorum gate with `quorum_lost` rather than stale `auth_required`.
TLS-over-TCP listeners use configured PEM certificate/key material and then run
the same stream protocol after handshake.
Plain and secure WebSocket listeners accept text WebSocket messages as JSON
text frames without newline delimiters and binary WebSocket messages as the
same 12-byte binary frame used by stream transports; tests cover both text and
binary messages on plain and TLS WebSocket listeners. Plain WebSocket coverage
now also drives pipelined text-message and binary-frame CRUD/query transaction
lifecycles through the real listener. TCP, TLS-over-TCP, QUIC, and plain and
secure authenticated replica WebSocket coverage now verifies unauthenticated
readonly begin is rejected before dispatch, valid JWT authentication succeeds,
and the configured replica read gate still rejects readonly begin with
`quorum_lost` on each real listener type. TCP, TLS-over-TCP, QUIC, plain
WebSocket, and secure WebSocket listener coverage also pipelines
`authenticate` and readonly `begin` in one burst, proving queued post-auth
replica reads are released in order and fenced by the read-quorum gate rather
than rejected from stale unauthenticated state.
LZ4-compressed binary frames are decompressed on read and can be emitted on
write. Protobuf frames use `crates/wire/proto/exdb_wire.proto`, preserve exact
integer scalar shapes, and are mirrored by the transport like other binary
encodings. Replica-role sessions now report the configured node role in
`hello`, route document write transaction commits through an injectable
promotion client, promote collection/index management DDL through
primary-owned intents, and promote create/drop database management through the
same system-catalog DDL promotion path. Direct replica-session coverage verifies
primary-returned DDL errors such as `database_exists` are forwarded unchanged
and do not create replica-local registry state. Direct authenticated replica
transport coverage now also proves a passing read-quorum gate allows a readonly
mixed JSON/BSON/Protobuf session against seeded data while local writes without
a promotion backend are rejected as `readonly_node`. Authenticated multi-client
coverage now also uses scoped user JWTs on two connections, verifies database
allowlist visibility, and proves a write on one connection pushes invalidation
to a `notify` subscription registered on the other. Multi-client
resource-pressure coverage now also saturates one connection's asynchronous
management scheduler with a blocked replica DDL promotion while a second
connection continues to receive `ping` responses and the saturated connection
gets connection-local `server_busy` accounting. Request/notification priority
rotation is covered to prevent push starvation. Auth-expiry lifecycle coverage
now also proves a server-initiated `auth_expired` close rolls back an active
uncommitted transaction before a fresh authenticated connection can observe it.
L7 promotion-client coverage now also verifies a live local role transition
from replica to primary prevents later write promotion with a role-specific
error before contacting the primary handler.
Additional end-to-end protocol validation across more concurrent auth,
role-transition, and multi-client mixed-workload combinations remains pending.

Where this older plan sketch differs from `docs/DESIGN.md` section 7, the
implementation follows `DESIGN.md`. In particular, collection and index
management messages carry a `database` field and are parsed as management
messages, not as transaction-scoped DDL messages.

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

    // --- Collection/index management (not transaction-scoped in DESIGN.md §7) ---
    CreateCollection { database: String, name: String },
    DropCollection { database: String, name: String },
    ListCollections { database: String },
    CreateIndex { database: String, collection: String, fields: Vec<serde_json::Value>,
                  name: Option<String> },
    DropIndex { database: String, collection: String, name: String },
    ListIndexes { database: String, collection: String },

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
    pub async fn handle_message(&mut self, msg_id: u32, msg: ClientMessage) -> Option<ServerMessage>;

    /// Push a server-initiated notification (invalidation, index_ready)
    pub fn push_notification(&self, msg: ServerMessage);
}
```

Implemented as a session core for hello/auth/ping, duplicate message ID
suppression, database create/drop/list, collection create/drop/list, index
create/drop/list, transaction begin/commit/rollback, and core
insert/get/replace/patch/delete/query operations with data-response `query_id`
values. Invalidation and index-ready events are forwarded as server-initiated
notifications.
Subscribe continuation/retry transactions are installed before `new_tx` is
returned to clients. Client-pipelined input and async management dispatch are
accepted by the transport, and transaction-scoped messages for different
transaction IDs can execute concurrently while same-transaction messages remain
ordered. Push notifications share the bounded per-connection capacity instead
of accumulating in an unbounded queue. Spawned management/transaction response
delivery also uses a bounded per-connection channel, so a slow writer applies
backpressure to completed async work instead of allowing unbounded response
growth. When a transaction ends terminally while same-transaction messages are
queued, the scheduler delivers the resulting `unknown_transaction` responses
through a bounded ordered sender rather than dropping them under response-queue
pressure. Scheduler saturation responses now include structured accounting
fields for the tripped budget and current utilization, including management
in-flight counts and transaction scheduler queue/worker counters. Request and
notification priority rotation is covered by transport tests so ready client
requests cannot indefinitely starve server-initiated notifications. The same
WebSocket connection loop now has text and binary coverage for
begin-plus-transaction-message bursts, same-transaction insert ordering,
committed readback, query IDs, secondary-index query readback, and rollback
cleanup.
The same stream connection coverage now also includes a full management
lifecycle from database creation through collection/index creation, indexed
query readback, and index/collection/database teardown.

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

Implemented for HMAC, RSA, and EC verification keys through `jsonwebtoken`.
The current tests cover HMAC tokens, configured issuer checks, `exp`, `nbf`,
role defaults, admin authorization, database allowlists, and base64-encoded
HMAC secrets. Session and transport tests also cover active JWT expiration:
the session schedules a server-initiated `auth_expired` error when the token
expires, request preflight returns `auth_expired` if a message races that
timer, and the transport closes after sending the expiry response.

### `transport.rs` — Transport Listeners

**WHY HERE:** Accepts connections over TCP/TLS/QUIC/WebSocket and spawns session tasks.

```rust
pub struct ServerConfig {
    pub listen: ListenConfig,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub max_message_size: usize,
    pub request_queue_capacity: usize,
    pub response_write_timeout: Duration,
    pub default_database_config: DatabaseConfig,
}

pub struct ListenConfig {
    pub tcp: Option<SocketAddr>,
    pub tls: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
    pub websocket_tls: Option<SocketAddr>,
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

Implemented for JSON text and binary JSON/BSON connection loops,
LZ4-compressed binary frame payloads, plus cancellable TCP and TLS-over-TCP
listeners, a QUIC listener, a plain WebSocket listener, and a secure WebSocket
listener. Protobuf binary payloads are implemented with the shared schema in
`crates/wire/proto/exdb_wire.proto`.

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
