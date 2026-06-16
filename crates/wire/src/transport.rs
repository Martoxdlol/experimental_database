//! Transport listener and per-connection loop for the wire protocol.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{BufReader, ErrorKind};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use exdb::{DatabaseConfig, SystemDatabase};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use quinn::{
    Endpoint as QuicEndpoint, RecvStream as QuicRecvStream, SendStream as QuicSendStream,
    ServerConfig as QuicServerConfig, crypto::rustls::QuicServerConfig as QuicRustlsServerConfig,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, ReadHalf};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::auth::AuthConfig;
use crate::error::{Result, WireError};
use crate::frame::{
    DEFAULT_MAX_MESSAGE_SIZE, Encoding, FrameType, RawFrame, decode_binary_frame_with_limit,
    encode_binary_frame_with_limit,
};
use crate::messages::{
    ClientMessage, ServerMessage, parse_client_message, serialize_server_message,
    serialize_server_message_json_text,
};
use crate::session::{
    ActiveTransaction, DEFAULT_NODE_ROLE, ManagementContext, MessageIdAcceptance, ReplicaReadGate,
    Session, SessionNotification, TransactionExecutionContext, TransactionPromoter,
    TransactionTaskResult,
};

/// Default number of parsed client requests buffered per connection.
pub const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 1024;

/// Default maximum time allowed for one frame write to a client.
pub const DEFAULT_RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

const QUIC_ALPN: &[u8] = b"exdb/1";

enum PendingResponse {
    Plain {
        request: RawFrame,
        response: ServerMessage,
    },
    Management {
        request: RawFrame,
        response: ServerMessage,
    },
    Transaction {
        request: RawFrame,
        tx_id: u64,
        result: TransactionTaskResult,
    },
}

#[allow(clippy::large_enum_variant)]
enum ConnectionEvent {
    PendingResponse(Option<PendingResponse>),
    Request(Option<Result<RawFrame>>),
    Notification(Option<SessionNotification>),
}

async fn next_connection_event(
    prefer_notifications: bool,
    response_rx: &mut mpsc::Receiver<PendingResponse>,
    request_rx: &mut mpsc::Receiver<Result<RawFrame>>,
    notification_rx: &mut mpsc::Receiver<SessionNotification>,
) -> ConnectionEvent {
    if prefer_notifications {
        tokio::select! {
            biased;
            pending = response_rx.recv() => ConnectionEvent::PendingResponse(pending),
            notification = notification_rx.recv() => ConnectionEvent::Notification(notification),
            frame = request_rx.recv() => ConnectionEvent::Request(frame),
        }
    } else {
        tokio::select! {
            biased;
            pending = response_rx.recv() => ConnectionEvent::PendingResponse(pending),
            frame = request_rx.recv() => ConnectionEvent::Request(frame),
            notification = notification_rx.recv() => ConnectionEvent::Notification(notification),
        }
    }
}

/// Server transport configuration.
#[derive(Clone)]
pub struct ServerConfig {
    pub listen: ListenConfig,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub node_role: String,
    pub transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    pub replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
    pub max_message_size: usize,
    pub request_queue_capacity: usize,
    pub response_write_timeout: Duration,
    pub default_database_config: DatabaseConfig,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConfig")
            .field("listen", &self.listen)
            .field("tls", &self.tls)
            .field("auth", &self.auth)
            .field("node_role", &self.node_role)
            .field("transaction_promoter", &self.transaction_promoter.is_some())
            .field("replica_read_gate", &self.replica_read_gate.is_some())
            .field("max_message_size", &self.max_message_size)
            .field("request_queue_capacity", &self.request_queue_capacity)
            .field("response_write_timeout", &self.response_write_timeout)
            .field("default_database_config", &self.default_database_config)
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: ListenConfig::default(),
            tls: None,
            auth: AuthConfig::default(),
            node_role: DEFAULT_NODE_ROLE.to_string(),
            transaction_promoter: None,
            replica_read_gate: None,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            request_queue_capacity: DEFAULT_REQUEST_QUEUE_CAPACITY,
            response_write_timeout: DEFAULT_RESPONSE_WRITE_TIMEOUT,
            default_database_config: DatabaseConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Validate transport and default database resource controls before use.
    pub fn validate(&self) -> Result<()> {
        if self.max_message_size == 0 {
            return Err(WireError::InvalidMessage(
                "max_message_size must be greater than zero".to_string(),
            ));
        }
        if self.request_queue_capacity == 0 {
            return Err(WireError::InvalidMessage(
                "request_queue_capacity must be greater than zero".to_string(),
            ));
        }
        if self.response_write_timeout.is_zero() {
            return Err(WireError::InvalidMessage(
                "response_write_timeout_ms must be greater than zero".to_string(),
            ));
        }
        self.auth.validate()?;
        if self.tls.is_none() {
            if self.listen.tls.is_some() {
                return Err(WireError::InvalidMessage(
                    "TLS listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                ));
            }
            if self.listen.websocket_tls.is_some() {
                return Err(WireError::InvalidMessage(
                    "secure WebSocket listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                ));
            }
            if self.listen.quic.is_some() {
                return Err(WireError::InvalidMessage(
                    "QUIC listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                ));
            }
        }
        if let Some(tls) = &self.tls {
            tls.validate_for_listeners(&self.listen)?;
        }
        self.default_database_config
            .validate()
            .map_err(WireError::InvalidMessage)
    }
}

/// Network addresses for transport listeners.
#[derive(Debug, Clone, Default)]
pub struct ListenConfig {
    pub tcp: Option<SocketAddr>,
    pub tls: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
    pub websocket_tls: Option<SocketAddr>,
}

/// TLS certificate and private-key material for TLS-over-TCP listeners.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
}

impl TlsConfig {
    pub fn from_pem(cert_chain_pem: Vec<u8>, private_key_pem: Vec<u8>) -> Self {
        Self {
            cert_chain_pem,
            private_key_pem,
        }
    }

    fn validate_for_listeners(&self, listen: &ListenConfig) -> Result<()> {
        if listen.tls.is_some() || listen.websocket_tls.is_some() {
            self.acceptor()?;
        }
        if listen.quic.is_some() {
            self.quic_server_config()?;
        }
        Ok(())
    }

    fn acceptor(&self) -> Result<TlsAcceptor> {
        let mut cert_reader = BufReader::new(self.cert_chain_pem.as_slice());
        let cert_chain = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| WireError::TlsConfig(format!("invalid certificate PEM: {err}")))?;
        if cert_chain.is_empty() {
            return Err(WireError::TlsConfig(
                "certificate PEM did not contain any certificates".to_string(),
            ));
        }

        let mut key_reader = BufReader::new(self.private_key_pem.as_slice());
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|err| WireError::TlsConfig(format!("invalid private key PEM: {err}")))?
            .ok_or_else(|| {
                WireError::TlsConfig("private key PEM did not contain a key".to_string())
            })?;

        let config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|err| WireError::TlsConfig(err.to_string()))?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    fn quic_server_config(&self) -> Result<QuicServerConfig> {
        let mut cert_reader = BufReader::new(self.cert_chain_pem.as_slice());
        let cert_chain = rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|err| WireError::TlsConfig(format!("invalid certificate PEM: {err}")))?;
        if cert_chain.is_empty() {
            return Err(WireError::TlsConfig(
                "certificate PEM did not contain any certificates".to_string(),
            ));
        }

        let mut key_reader = BufReader::new(self.private_key_pem.as_slice());
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|err| WireError::TlsConfig(format!("invalid private key PEM: {err}")))?
            .ok_or_else(|| {
                WireError::TlsConfig("private key PEM did not contain a key".to_string())
            })?;

        let mut config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|err| WireError::TlsConfig(err.to_string()))?;
        config.alpn_protocols = vec![QUIC_ALPN.to_vec()];
        config.max_early_data_size = u32::MAX;
        let config = QuicRustlsServerConfig::try_from(config)
            .map_err(|err| WireError::TlsConfig(err.to_string()))?;
        Ok(QuicServerConfig::with_crypto(Arc::new(config)))
    }
}

/// Wire protocol server.
pub struct Server {
    config: ServerConfig,
    registry: Arc<SystemDatabase>,
    next_session_id: Arc<AtomicU64>,
}

impl Server {
    pub fn new(config: ServerConfig, registry: Arc<SystemDatabase>) -> Self {
        Self {
            config,
            registry,
            next_session_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Start configured listeners. TCP, TLS-over-TCP, QUIC, plain WebSocket,
    /// and TLS-over-TCP WebSocket are implemented.
    pub async fn start(&self, shutdown: CancellationToken) -> Result<()> {
        self.config.validate()?;

        let mut listeners = JoinSet::new();

        if let Some(addr) = self.config.listen.tcp {
            let listener = TcpListener::bind(addr).await?;
            let registry = Arc::clone(&self.registry);
            let auth = self.config.auth.clone();
            let max_message_size = self.config.max_message_size;
            let request_queue_capacity = self.config.request_queue_capacity;
            let response_write_timeout = self.config.response_write_timeout;
            let default_database_config = self.config.default_database_config.clone();
            let node_role = self.config.node_role.clone();
            let transaction_promoter = self.config.transaction_promoter.clone();
            let replica_read_gate = self.config.replica_read_gate.clone();
            let session_ids = Arc::clone(&self.next_session_id);
            listeners.spawn(async move {
                accept_tcp_loop(
                    listener,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                )
                .await
            });
        }

        if let Some(addr) = self.config.listen.tls {
            let tls = self.config.tls.as_ref().ok_or_else(|| {
                WireError::InvalidMessage(
                    "TLS listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                )
            })?;
            let acceptor = tls.acceptor()?;
            let listener = TcpListener::bind(addr).await?;
            let registry = Arc::clone(&self.registry);
            let auth = self.config.auth.clone();
            let max_message_size = self.config.max_message_size;
            let request_queue_capacity = self.config.request_queue_capacity;
            let response_write_timeout = self.config.response_write_timeout;
            let default_database_config = self.config.default_database_config.clone();
            let node_role = self.config.node_role.clone();
            let transaction_promoter = self.config.transaction_promoter.clone();
            let replica_read_gate = self.config.replica_read_gate.clone();
            let session_ids = Arc::clone(&self.next_session_id);
            listeners.spawn(async move {
                accept_tls_loop(
                    listener,
                    acceptor,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                )
                .await
            });
        }

        if let Some(addr) = self.config.listen.websocket {
            let listener = TcpListener::bind(addr).await?;
            let registry = Arc::clone(&self.registry);
            let auth = self.config.auth.clone();
            let max_message_size = self.config.max_message_size;
            let request_queue_capacity = self.config.request_queue_capacity;
            let response_write_timeout = self.config.response_write_timeout;
            let default_database_config = self.config.default_database_config.clone();
            let node_role = self.config.node_role.clone();
            let transaction_promoter = self.config.transaction_promoter.clone();
            let replica_read_gate = self.config.replica_read_gate.clone();
            let session_ids = Arc::clone(&self.next_session_id);
            listeners.spawn(async move {
                accept_websocket_loop(
                    listener,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                )
                .await
            });
        }

        if let Some(addr) = self.config.listen.websocket_tls {
            let tls = self.config.tls.as_ref().ok_or_else(|| {
                WireError::InvalidMessage(
                    "secure WebSocket listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                )
            })?;
            let acceptor = tls.acceptor()?;
            let listener = TcpListener::bind(addr).await?;
            let registry = Arc::clone(&self.registry);
            let auth = self.config.auth.clone();
            let max_message_size = self.config.max_message_size;
            let request_queue_capacity = self.config.request_queue_capacity;
            let response_write_timeout = self.config.response_write_timeout;
            let default_database_config = self.config.default_database_config.clone();
            let node_role = self.config.node_role.clone();
            let transaction_promoter = self.config.transaction_promoter.clone();
            let replica_read_gate = self.config.replica_read_gate.clone();
            let session_ids = Arc::clone(&self.next_session_id);
            listeners.spawn(async move {
                accept_secure_websocket_loop(
                    listener,
                    acceptor,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                )
                .await
            });
        }

        if let Some(addr) = self.config.listen.quic {
            let tls = self.config.tls.as_ref().ok_or_else(|| {
                WireError::InvalidMessage(
                    "QUIC listener requires tls.cert_file and tls.key_file configuration"
                        .to_string(),
                )
            })?;
            let endpoint = QuicEndpoint::server(tls.quic_server_config()?, addr)
                .map_err(|err| WireError::Quic(err.to_string()))?;
            let registry = Arc::clone(&self.registry);
            let auth = self.config.auth.clone();
            let max_message_size = self.config.max_message_size;
            let request_queue_capacity = self.config.request_queue_capacity;
            let response_write_timeout = self.config.response_write_timeout;
            let default_database_config = self.config.default_database_config.clone();
            let node_role = self.config.node_role.clone();
            let transaction_promoter = self.config.transaction_promoter.clone();
            let replica_read_gate = self.config.replica_read_gate.clone();
            let session_ids = Arc::clone(&self.next_session_id);
            listeners.spawn(async move {
                accept_quic_loop(
                    endpoint,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                )
                .await
            });
        }

        if listeners.is_empty() {
            return Err(WireError::InvalidMessage(
                "no transport listener configured".to_string(),
            ));
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                listeners.abort_all();
                Ok(())
            }
            result = listeners.join_next() => {
                match result {
                    Some(Ok(Ok(()))) | None => Ok(()),
                    Some(Ok(Err(err))) => Err(err),
                    Some(Err(err)) => Err(WireError::InvalidMessage(format!(
                        "transport listener task failed: {err}"
                    ))),
                }
            }
        }
    }

    /// Handle a single accepted stream.
    pub async fn handle_connection<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.config.validate()?;

        let mut session = Session::new(
            self.next_session_id(),
            Arc::clone(&self.registry),
            self.config.auth.clone(),
        );
        configure_session(
            &mut session,
            self.config.max_message_size,
            self.config.default_database_config.clone(),
            self.config.node_role.clone(),
            self.config.transaction_promoter.clone(),
            self.config.replica_read_gate.clone(),
        );
        handle_connection_with_session_and_queue(
            stream,
            &mut session,
            self.config.max_message_size,
            self.config.request_queue_capacity,
            self.config.response_write_timeout,
        )
        .await
    }

    fn next_session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::AcqRel)
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_tcp_loop(
    listener: TcpListener,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        spawn_connection_task(
            stream,
            Arc::clone(&session_ids),
            Arc::clone(&registry),
            auth.clone(),
            max_message_size,
            request_queue_capacity,
            response_write_timeout,
            default_database_config.clone(),
            node_role.clone(),
            transaction_promoter.clone(),
            replica_read_gate.clone(),
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_tls_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let session_ids = Arc::clone(&session_ids);
        let registry = Arc::clone(&registry);
        let auth = auth.clone();
        let default_database_config = default_database_config.clone();
        let node_role = node_role.clone();
        let transaction_promoter = transaction_promoter.clone();
        let replica_read_gate = replica_read_gate.clone();
        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    spawn_connection_task(
                        stream,
                        session_ids,
                        registry,
                        auth,
                        max_message_size,
                        request_queue_capacity,
                        response_write_timeout,
                        default_database_config,
                        node_role,
                        transaction_promoter,
                        replica_read_gate,
                    );
                }
                Err(err) => tracing_log_connection_error(WireError::Io(err)),
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_websocket_loop(
    listener: TcpListener,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let session_ids = Arc::clone(&session_ids);
        let registry = Arc::clone(&registry);
        let auth = auth.clone();
        let default_database_config = default_database_config.clone();
        let node_role = node_role.clone();
        let transaction_promoter = transaction_promoter.clone();
        let replica_read_gate = replica_read_gate.clone();
        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(stream) => {
                    let session_id = session_ids.fetch_add(1, Ordering::AcqRel);
                    let mut session = Session::new(session_id, registry, auth);
                    configure_session(
                        &mut session,
                        max_message_size,
                        default_database_config,
                        node_role,
                        transaction_promoter,
                        replica_read_gate,
                    );
                    if let Err(err) = handle_websocket_connection_with_session_and_queue(
                        stream,
                        &mut session,
                        max_message_size,
                        request_queue_capacity,
                        response_write_timeout,
                    )
                    .await
                    {
                        tracing_log_connection_error(err);
                    }
                }
                Err(err) => tracing_log_connection_error(WireError::WebSocket(err.to_string())),
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_secure_websocket_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let session_ids = Arc::clone(&session_ids);
        let registry = Arc::clone(&registry);
        let auth = auth.clone();
        let default_database_config = default_database_config.clone();
        let node_role = node_role.clone();
        let transaction_promoter = transaction_promoter.clone();
        let replica_read_gate = replica_read_gate.clone();
        tokio::spawn(async move {
            match acceptor.accept(stream).await {
                Ok(stream) => match accept_async(stream).await {
                    Ok(stream) => {
                        let session_id = session_ids.fetch_add(1, Ordering::AcqRel);
                        let mut session = Session::new(session_id, registry, auth);
                        configure_session(
                            &mut session,
                            max_message_size,
                            default_database_config,
                            node_role,
                            transaction_promoter,
                            replica_read_gate,
                        );
                        if let Err(err) = handle_websocket_connection_with_session_and_queue(
                            stream,
                            &mut session,
                            max_message_size,
                            request_queue_capacity,
                            response_write_timeout,
                        )
                        .await
                        {
                            tracing_log_connection_error(err);
                        }
                    }
                    Err(err) => tracing_log_connection_error(WireError::WebSocket(err.to_string())),
                },
                Err(err) => tracing_log_connection_error(WireError::Io(err)),
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_quic_loop(
    endpoint: QuicEndpoint,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) -> Result<()> {
    while let Some(incoming) = endpoint.accept().await {
        let session_ids = Arc::clone(&session_ids);
        let registry = Arc::clone(&registry);
        let auth = auth.clone();
        let default_database_config = default_database_config.clone();
        let node_role = node_role.clone();
        let transaction_promoter = transaction_promoter.clone();
        let replica_read_gate = replica_read_gate.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    let initial = connection.clone();
                    spawn_initial_quic_stream_task(
                        initial,
                        Arc::clone(&session_ids),
                        Arc::clone(&registry),
                        auth.clone(),
                        max_message_size,
                        request_queue_capacity,
                        response_write_timeout,
                        default_database_config.clone(),
                        node_role.clone(),
                        transaction_promoter.clone(),
                        replica_read_gate.clone(),
                    );

                    while let Ok((send, recv)) = connection.accept_bi().await {
                        let stream = QuicStream::new(recv, send);
                        spawn_connection_task(
                            stream,
                            Arc::clone(&session_ids),
                            Arc::clone(&registry),
                            auth.clone(),
                            max_message_size,
                            request_queue_capacity,
                            response_write_timeout,
                            default_database_config.clone(),
                            node_role.clone(),
                            transaction_promoter.clone(),
                            replica_read_gate.clone(),
                        );
                    }
                }
                Err(err) => tracing_log_connection_error(WireError::Quic(err.to_string())),
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_initial_quic_stream_task(
    connection: quinn::Connection,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) {
    tokio::spawn(async move {
        match connection.open_bi().await {
            Ok((send, recv)) => {
                let stream = QuicStream::new(recv, send);
                spawn_connection_task(
                    stream,
                    session_ids,
                    registry,
                    auth,
                    max_message_size,
                    request_queue_capacity,
                    response_write_timeout,
                    default_database_config,
                    node_role,
                    transaction_promoter,
                    replica_read_gate,
                );
            }
            Err(err) => tracing_log_connection_error(WireError::Quic(err.to_string())),
        }
    });
}

struct QuicStream {
    recv: QuicRecvStream,
    send: QuicSendStream,
}

impl QuicStream {
    fn new(recv: QuicRecvStream, send: QuicSendStream) -> Self {
        Self { recv, send }
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.send).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes)) => Poll::Ready(Ok(bytes)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_connection_task<S>(
    stream: S,
    session_ids: Arc<AtomicU64>,
    registry: Arc<SystemDatabase>,
    auth: AuthConfig,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let session_id = session_ids.fetch_add(1, Ordering::AcqRel);
    tokio::spawn(async move {
        let mut session = Session::new(session_id, registry, auth);
        configure_session(
            &mut session,
            max_message_size,
            default_database_config,
            node_role,
            transaction_promoter,
            replica_read_gate,
        );
        handle_connection_with_session_and_queue(
            stream,
            &mut session,
            max_message_size,
            request_queue_capacity,
            response_write_timeout,
        )
        .await
        .map_err(tracing_log_connection_error)
        .ok();
    });
}

fn configure_session(
    session: &mut Session,
    max_message_size: usize,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
) {
    session.set_max_message_size(max_message_size);
    session.set_default_database_config(default_database_config);
    session.set_node_role(node_role);
    session.set_transaction_promoter(transaction_promoter);
    session.set_replica_read_gate(replica_read_gate);
}

/// Handle a single WebSocket connection with a supplied session.
pub async fn handle_websocket_connection_with_session_and_queue<S>(
    stream: WebSocketStream<S>,
    session: &mut Session,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut writer, reader) = stream.split();
    let request_queue_capacity = bounded_queue_capacity(request_queue_capacity);
    let mut request_rx =
        spawn_websocket_request_reader(reader, max_message_size, request_queue_capacity);
    let (notification_tx, mut notification_rx) = mpsc::channel(request_queue_capacity);
    session.set_notification_sender(notification_tx);
    let (response_tx, mut response_rx) = bounded_response_channel(request_queue_capacity);
    let mut tx_scheduler = TransactionScheduler::new(request_queue_capacity);
    let mut management_in_flight = 0usize;
    let mut notification_frame_type = FrameType::JsonText;
    let mut notification_encoding = Encoding::Json;
    let mut prefer_notifications = false;

    let hello = serialize_server_message_json_text(&session.hello(), 0)?;
    write_websocket_frame_with_timeout(
        &mut writer,
        &hello,
        max_message_size,
        response_write_timeout,
    )
    .await?;

    loop {
        let event = next_connection_event(
            prefer_notifications,
            &mut response_rx,
            &mut request_rx,
            &mut notification_rx,
        )
        .await;

        match event {
            ConnectionEvent::PendingResponse(pending) => {
                let Some(pending) = pending else {
                    continue;
                };
                let (request, response) = match pending {
                    PendingResponse::Plain { request, response } => (request, response),
                    PendingResponse::Management { request, response } => {
                        management_in_flight = management_in_flight.saturating_sub(1);
                        (request, response)
                    }
                    PendingResponse::Transaction {
                        request,
                        tx_id,
                        result,
                    } => {
                        let response = session.apply_transaction_task_result(result);
                        tx_scheduler.schedule_next(session, response_tx.clone(), tx_id);
                        (request, response)
                    }
                };
                let close_after_response = response_closes_connection(&response);
                let response_frame = response_frame_for_request(&response, &request)?;
                write_websocket_frame_with_timeout(
                    &mut writer,
                    &response_frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
            ConnectionEvent::Request(frame) => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(err)) => {
                        if let Some((request, response)) = framed_read_error_response(session, &err)
                        {
                            let response_frame = response_frame_for_request(&response, &request)?;
                            write_websocket_frame_with_timeout(
                                &mut writer,
                                &response_frame,
                                max_message_size,
                                response_write_timeout,
                            )
                            .await?;
                            return Ok(());
                        }
                        return Err(err);
                    }
                    None => return Ok(()),
                };

                prefer_notifications = true;
                notification_frame_type = frame.frame_type;
                notification_encoding = frame.encoding;
                session.set_current_encoding(frame.encoding);
                let response = match parse_client_message(&frame) {
                    Ok((msg_id, msg)) => match session.accept_message_id(msg_id) {
                        MessageIdAcceptance::Accepted => {
                            if let Some(response) = session.preflight_response(&msg) {
                                Some(response)
                            } else if msg.is_management() {
                                if management_in_flight >= request_queue_capacity {
                                    Some(management_scheduler_full_response(
                                        management_in_flight,
                                        request_queue_capacity,
                                    ))
                                } else {
                                    management_in_flight += 1;
                                    spawn_management_response(
                                        response_tx.clone(),
                                        session.management_context(),
                                        frame,
                                        msg,
                                    );
                                    continue;
                                }
                            } else if Session::transaction_message_id(&msg).is_some() {
                                match tx_scheduler.dispatch_or_queue(
                                    session,
                                    response_tx.clone(),
                                    frame.clone(),
                                    msg,
                                ) {
                                    Some(response) => Some(response),
                                    None => continue,
                                }
                            } else {
                                Some(session.handle_accepted_message(msg).await)
                            }
                        }
                        MessageIdAcceptance::Duplicate => continue,
                        MessageIdAcceptance::Rejected(response) => Some(response),
                    },
                    Err(err) => invalid_message_response_for_parse_error(session, &frame, err),
                };
                let Some(response) = response else {
                    continue;
                };

                let close_after_response = response_closes_connection(&response);
                let response_frame = response_frame_for_request(&response, &frame)?;
                write_websocket_frame_with_timeout(
                    &mut writer,
                    &response_frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
            ConnectionEvent::Notification(notification) => {
                let Some(notification) = notification else {
                    continue;
                };
                prefer_notifications = false;
                let notification = session.apply_notification(notification);
                let close_after_response = response_closes_connection(&notification);
                let frame = notification_frame(
                    &notification,
                    notification_frame_type,
                    notification_encoding,
                )?;
                write_websocket_frame_with_timeout(
                    &mut writer,
                    &frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
        }
    }
}

async fn write_websocket_frame_with_timeout<S>(
    writer: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: &RawFrame,
    max_message_size: usize,
    response_write_timeout: Duration,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = websocket_message_for_frame(frame, max_message_size)?;
    match timeout(response_write_timeout, writer.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(WireError::WebSocket(err.to_string())),
        Err(_) => Err(WireError::WriteTimeout {
            timeout_ms: duration_millis_u64(response_write_timeout),
        }),
    }
}

fn websocket_message_for_frame(frame: &RawFrame, max_message_size: usize) -> Result<Message> {
    match frame.frame_type {
        FrameType::JsonText => {
            if frame.payload.len() > max_message_size {
                return Err(WireError::MessageTooLarge {
                    length: frame.payload.len(),
                    limit: max_message_size,
                });
            }
            let text = String::from_utf8(frame.payload.clone())
                .map_err(|_| WireError::MalformedFrame("JSON text frame is not UTF-8"))?;
            Ok(Message::Text(text.into()))
        }
        FrameType::Binary => Ok(Message::Binary(
            encode_binary_frame_with_limit(frame, max_message_size)?.into(),
        )),
    }
}

fn spawn_websocket_request_reader<S>(
    mut reader: SplitStream<WebSocketStream<S>>,
    max_message_size: usize,
    request_queue_capacity: usize,
) -> mpsc::Receiver<Result<RawFrame>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (request_tx, request_rx) = mpsc::channel(request_queue_capacity);
    tokio::spawn(async move {
        while let Some(message) = reader.next().await {
            let frame = match message {
                Ok(Message::Text(text)) => websocket_text_frame(text.as_str(), max_message_size),
                Ok(Message::Binary(bytes)) => {
                    decode_binary_frame_with_limit(&bytes, max_message_size)
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                Ok(_) => continue,
                Err(err) => Err(WireError::WebSocket(err.to_string())),
            };
            if request_tx.send(frame).await.is_err() {
                break;
            }
        }
    });
    request_rx
}

fn websocket_text_frame(text: &str, max_message_size: usize) -> Result<RawFrame> {
    if text.len() > max_message_size {
        return Err(WireError::MessageTooLarge {
            length: text.len(),
            limit: max_message_size,
        });
    }
    Ok(RawFrame::json_text(text.as_bytes().to_vec()))
}

/// Handle a single stream with a supplied session.
pub async fn handle_connection_with_session<S>(
    stream: S,
    session: &mut Session,
    max_message_size: usize,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    handle_connection_with_session_and_queue(
        stream,
        session,
        max_message_size,
        DEFAULT_REQUEST_QUEUE_CAPACITY,
        DEFAULT_RESPONSE_WRITE_TIMEOUT,
    )
    .await
}

/// Handle a single stream with a supplied session and bounded request queue.
pub async fn handle_connection_with_session_and_queue<S>(
    stream: S,
    session: &mut Session,
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let request_queue_capacity = bounded_queue_capacity(request_queue_capacity);
    let mut request_rx = spawn_request_reader(reader, max_message_size, request_queue_capacity);
    let (notification_tx, mut notification_rx) = mpsc::channel(request_queue_capacity);
    session.set_notification_sender(notification_tx);
    let (response_tx, mut response_rx) = bounded_response_channel(request_queue_capacity);
    let mut tx_scheduler = TransactionScheduler::new(request_queue_capacity);
    let mut management_in_flight = 0usize;
    let mut notification_frame_type = FrameType::JsonText;
    let mut notification_encoding = Encoding::Json;
    let mut prefer_notifications = false;

    let hello = serialize_server_message_json_text(&session.hello(), 0)?;
    write_frame_with_timeout(
        &mut writer,
        &hello,
        max_message_size,
        response_write_timeout,
    )
    .await?;

    loop {
        let event = next_connection_event(
            prefer_notifications,
            &mut response_rx,
            &mut request_rx,
            &mut notification_rx,
        )
        .await;

        match event {
            ConnectionEvent::PendingResponse(pending) => {
                let Some(pending) = pending else {
                    continue;
                };
                let (request, response) = match pending {
                    PendingResponse::Plain { request, response } => (request, response),
                    PendingResponse::Management { request, response } => {
                        management_in_flight = management_in_flight.saturating_sub(1);
                        (request, response)
                    }
                    PendingResponse::Transaction {
                        request,
                        tx_id,
                        result,
                    } => {
                        let response = session.apply_transaction_task_result(result);
                        tx_scheduler.schedule_next(session, response_tx.clone(), tx_id);
                        (request, response)
                    }
                };
                let close_after_response = response_closes_connection(&response);
                let response_frame = response_frame_for_request(&response, &request)?;
                write_frame_with_timeout(
                    &mut writer,
                    &response_frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
            ConnectionEvent::Request(frame) => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(WireError::Io(err))) if err.kind() == ErrorKind::UnexpectedEof => {
                        return Ok(());
                    }
                    Some(Err(err)) => {
                        if let Some((request, response)) = framed_read_error_response(session, &err)
                        {
                            let response_frame = response_frame_for_request(&response, &request)?;
                            write_frame_with_timeout(
                                &mut writer,
                                &response_frame,
                                max_message_size,
                                response_write_timeout,
                            )
                            .await?;
                            return Ok(());
                        }
                        return Err(err);
                    }
                    None => return Ok(()),
                };

                prefer_notifications = true;
                notification_frame_type = frame.frame_type;
                notification_encoding = frame.encoding;
                session.set_current_encoding(frame.encoding);
                let response = match parse_client_message(&frame) {
                    Ok((msg_id, msg)) => match session.accept_message_id(msg_id) {
                        MessageIdAcceptance::Accepted => {
                            if let Some(response) = session.preflight_response(&msg) {
                                Some(response)
                            } else if msg.is_management() {
                                if management_in_flight >= request_queue_capacity {
                                    Some(management_scheduler_full_response(
                                        management_in_flight,
                                        request_queue_capacity,
                                    ))
                                } else {
                                    management_in_flight += 1;
                                    spawn_management_response(
                                        response_tx.clone(),
                                        session.management_context(),
                                        frame,
                                        msg,
                                    );
                                    continue;
                                }
                            } else if Session::transaction_message_id(&msg).is_some() {
                                match tx_scheduler.dispatch_or_queue(
                                    session,
                                    response_tx.clone(),
                                    frame.clone(),
                                    msg,
                                ) {
                                    Some(response) => Some(response),
                                    None => continue,
                                }
                            } else {
                                Some(session.handle_accepted_message(msg).await)
                            }
                        }
                        MessageIdAcceptance::Duplicate => continue,
                        MessageIdAcceptance::Rejected(response) => Some(response),
                    },
                    Err(err) => invalid_message_response_for_parse_error(session, &frame, err),
                };

                let Some(response) = response else {
                    continue;
                };

                let close_after_response = response_closes_connection(&response);
                let response_frame = response_frame_for_request(&response, &frame)?;
                write_frame_with_timeout(
                    &mut writer,
                    &response_frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
            ConnectionEvent::Notification(notification) => {
                let Some(notification) = notification else {
                    continue;
                };
                prefer_notifications = false;
                let notification = session.apply_notification(notification);
                let close_after_response = response_closes_connection(&notification);
                let frame = notification_frame(
                    &notification,
                    notification_frame_type,
                    notification_encoding,
                )?;
                write_frame_with_timeout(
                    &mut writer,
                    &frame,
                    max_message_size,
                    response_write_timeout,
                )
                .await?;
                if close_after_response {
                    return Ok(());
                }
            }
        }
    }
}

async fn write_frame_with_timeout<W>(
    writer: &mut W,
    frame: &RawFrame,
    max_message_size: usize,
    response_write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match timeout(
        response_write_timeout,
        crate::frame::write_frame_with_limit(writer, frame, max_message_size),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(WireError::WriteTimeout {
            timeout_ms: duration_millis_u64(response_write_timeout),
        }),
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn spawn_request_reader<S>(
    mut reader: ReadHalf<S>,
    max_message_size: usize,
    request_queue_capacity: usize,
) -> mpsc::Receiver<Result<RawFrame>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (request_tx, request_rx) = mpsc::channel(request_queue_capacity);
    tokio::spawn(async move {
        loop {
            let frame = crate::frame::read_frame_with_limit(&mut reader, max_message_size).await;
            let terminal = matches!(
                &frame,
                Err(WireError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof
            );
            if request_tx.send(frame).await.is_err() || terminal {
                break;
            }
        }
    });
    request_rx
}

fn bounded_queue_capacity(request_queue_capacity: usize) -> usize {
    request_queue_capacity.max(1)
}

fn bounded_response_channel(
    request_queue_capacity: usize,
) -> (
    mpsc::Sender<PendingResponse>,
    mpsc::Receiver<PendingResponse>,
) {
    mpsc::channel(bounded_queue_capacity(request_queue_capacity))
}

struct TransactionScheduler {
    queued_by_tx: HashMap<u64, VecDeque<(RawFrame, ClientMessage)>>,
    queued_requests: usize,
    max_queued_requests: usize,
    max_queued_per_tx: usize,
    max_active_workers: usize,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionSchedulerPressure {
    TotalQueueFull { tx_id: u64, queued_for_tx: usize },
    TransactionQueueFull { tx_id: u64, queued_for_tx: usize },
    ActiveWorkersFull { tx_id: u64 },
}

impl TransactionScheduler {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            queued_by_tx: HashMap::new(),
            queued_requests: 0,
            max_queued_requests: capacity,
            max_queued_per_tx: capacity.div_ceil(2),
            max_active_workers: capacity,
        }
    }

    fn dispatch_or_queue(
        &mut self,
        session: &mut Session,
        response_tx: mpsc::Sender<PendingResponse>,
        request: RawFrame,
        msg: ClientMessage,
    ) -> Option<ServerMessage> {
        let Some(tx_id) = Session::transaction_message_id(&msg) else {
            return Some(ServerMessage::Error {
                code: "internal".to_string(),
                message: "transaction scheduler received non-transaction message".to_string(),
                extra: None,
            });
        };

        if let Some(queue) = self.queued_by_tx.get_mut(&tx_id) {
            let queued_for_tx = queue.len();
            if self.queued_requests >= self.max_queued_requests {
                return Some(self.transaction_scheduler_full_response(
                    TransactionSchedulerPressure::TotalQueueFull {
                        tx_id,
                        queued_for_tx,
                    },
                ));
            }
            if queued_for_tx >= self.max_queued_per_tx {
                return Some(self.transaction_scheduler_full_response(
                    TransactionSchedulerPressure::TransactionQueueFull {
                        tx_id,
                        queued_for_tx,
                    },
                ));
            }
            queue.push_back((request, msg));
            self.queued_requests += 1;
            return None;
        }

        if self.queued_by_tx.len() >= self.max_active_workers {
            return Some(self.transaction_scheduler_full_response(
                TransactionSchedulerPressure::ActiveWorkersFull { tx_id },
            ));
        }

        let Some(active) = session.take_active_transaction(tx_id) else {
            return Some(Session::unknown_transaction_response(tx_id));
        };

        self.queued_by_tx.insert(tx_id, VecDeque::new());
        spawn_transaction_response(
            response_tx,
            tx_id,
            request,
            msg,
            active,
            session.transaction_execution_context(),
        );
        None
    }

    fn schedule_next(
        &mut self,
        session: &mut Session,
        response_tx: mpsc::Sender<PendingResponse>,
        tx_id: u64,
    ) {
        let Some(queue) = self.queued_by_tx.get_mut(&tx_id) else {
            return;
        };

        if let Some((request, msg)) = queue.pop_front() {
            self.queued_requests = self.queued_requests.saturating_sub(1);
            let Some(active) = session.take_active_transaction(tx_id) else {
                let mut pending = vec![PendingResponse::Plain {
                    request,
                    response: Session::unknown_transaction_response(tx_id),
                }];
                while let Some((request, _msg)) = queue.pop_front() {
                    self.queued_requests = self.queued_requests.saturating_sub(1);
                    pending.push(PendingResponse::Plain {
                        request,
                        response: Session::unknown_transaction_response(tx_id),
                    });
                }
                self.queued_by_tx.remove(&tx_id);
                spawn_plain_responses(response_tx, pending);
                return;
            };
            spawn_transaction_response(
                response_tx,
                tx_id,
                request,
                msg,
                active,
                session.transaction_execution_context(),
            );
            return;
        }

        self.queued_by_tx.remove(&tx_id);
    }

    fn transaction_scheduler_full_response(
        &self,
        pressure: TransactionSchedulerPressure,
    ) -> ServerMessage {
        let (reason, tx_id, queued_for_tx) = match pressure {
            TransactionSchedulerPressure::TotalQueueFull {
                tx_id,
                queued_for_tx,
            } => ("total_queue_full", tx_id, Some(queued_for_tx)),
            TransactionSchedulerPressure::TransactionQueueFull {
                tx_id,
                queued_for_tx,
            } => ("transaction_queue_full", tx_id, Some(queued_for_tx)),
            TransactionSchedulerPressure::ActiveWorkersFull { tx_id } => {
                ("active_workers_full", tx_id, None)
            }
        };
        ServerMessage::Error {
            code: "server_busy".to_string(),
            message: "connection transaction scheduler is full".to_string(),
            extra: Some(serde_json::json!({
                "scope": "transaction_scheduler",
                "reason": reason,
                "tx": tx_id,
                "queued_requests": self.queued_requests,
                "max_queued_requests": self.max_queued_requests,
                "queued_for_tx": queued_for_tx,
                "max_queued_per_tx": self.max_queued_per_tx,
                "active_workers": self.queued_by_tx.len(),
                "max_active_workers": self.max_active_workers,
            })),
        }
    }
}

fn spawn_plain_responses(
    response_tx: mpsc::Sender<PendingResponse>,
    responses: Vec<PendingResponse>,
) {
    tokio::spawn(async move {
        for response in responses {
            if response_tx.send(response).await.is_err() {
                break;
            }
        }
    });
}

fn response_closes_connection(response: &ServerMessage) -> bool {
    matches!(
        response,
        ServerMessage::Error { code, .. } if code == "auth_expired"
    )
}

fn spawn_management_response(
    response_tx: mpsc::Sender<PendingResponse>,
    context: ManagementContext,
    request: RawFrame,
    msg: ClientMessage,
) {
    tokio::spawn(async move {
        let response = context
            .handle_message(msg)
            .await
            .unwrap_or_else(|| ServerMessage::Error {
                code: "internal".to_string(),
                message: "management scheduler received non-management message".to_string(),
                extra: None,
            });
        let _ = response_tx
            .send(PendingResponse::Management { request, response })
            .await;
    });
}

fn management_scheduler_full_response(in_flight: usize, capacity: usize) -> ServerMessage {
    ServerMessage::Error {
        code: "server_busy".to_string(),
        message: "connection management scheduler is full".to_string(),
        extra: Some(serde_json::json!({
            "scope": "management_scheduler",
            "reason": "in_flight_full",
            "in_flight": in_flight,
            "max_in_flight": capacity,
        })),
    }
}

fn framed_read_error_response(
    session: &mut Session,
    err: &WireError,
) -> Option<(RawFrame, ServerMessage)> {
    match err {
        WireError::BinaryMessageTooLarge {
            msg_id,
            msg_type,
            encoding,
            length,
            limit,
        } => {
            let encoding = Encoding::try_from(*encoding).ok()?;
            let request = RawFrame::binary(*msg_id, *msg_type, encoding, Vec::new());
            let response = match session.accept_message_id(*msg_id) {
                MessageIdAcceptance::Accepted => ServerMessage::Error {
                    code: "message_too_large".to_string(),
                    message: format!("message payload length {length} exceeds limit {limit}"),
                    extra: Some(serde_json::json!({
                        "length": length,
                        "limit": limit,
                    })),
                },
                MessageIdAcceptance::Duplicate => return None,
                MessageIdAcceptance::Rejected(response) => response,
            };
            Some((request, response))
        }
        _ => None,
    }
}

fn invalid_message_response_for_parse_error(
    session: &mut Session,
    request: &RawFrame,
    err: WireError,
) -> Option<ServerMessage> {
    if let Some(msg_id) = request_msg_id_for_parse_error(request) {
        match session.accept_message_id(msg_id) {
            MessageIdAcceptance::Accepted => {}
            MessageIdAcceptance::Duplicate => return None,
            MessageIdAcceptance::Rejected(response) => return Some(response),
        }
    }

    Some(ServerMessage::Error {
        code: "invalid_message".to_string(),
        message: err.to_string(),
        extra: None,
    })
}

fn request_msg_id_for_parse_error(request: &RawFrame) -> Option<u32> {
    match request.frame_type {
        FrameType::Binary => Some(request.msg_id),
        FrameType::JsonText => serde_json::from_slice::<serde_json::Value>(&request.payload)
            .ok()
            .and_then(|value| {
                value
                    .as_object()
                    .and_then(|object| object.get("id"))
                    .and_then(|id| id.as_u64())
            })
            .and_then(|id| u32::try_from(id).ok()),
    }
}

fn spawn_transaction_response(
    response_tx: mpsc::Sender<PendingResponse>,
    tx_id: u64,
    request: RawFrame,
    msg: ClientMessage,
    active: ActiveTransaction,
    context: TransactionExecutionContext,
) {
    tokio::spawn(async move {
        let result = Session::handle_transaction_task(active, msg, context, tx_id).await;
        let _ = response_tx
            .send(PendingResponse::Transaction {
                request,
                tx_id,
                result,
            })
            .await;
    });
}

fn response_frame_for_request(msg: &ServerMessage, request: &RawFrame) -> Result<RawFrame> {
    match request.frame_type {
        FrameType::JsonText => serialize_server_message_json_text(msg, json_text_msg_id(request)),
        FrameType::Binary => {
            let encoding = match request.encoding {
                Encoding::Json | Encoding::Bson | Encoding::Protobuf => request.encoding,
            };
            serialize_server_message(msg, request.msg_id, encoding)
        }
    }
}

fn notification_frame(
    msg: &ServerMessage,
    frame_type: FrameType,
    encoding: Encoding,
) -> Result<RawFrame> {
    match frame_type {
        FrameType::JsonText => serialize_server_message_json_text(msg, 0),
        FrameType::Binary => {
            let encoding = match encoding {
                Encoding::Json | Encoding::Bson | Encoding::Protobuf => encoding,
            };
            serialize_server_message(msg, 0, encoding)
        }
    }
}

fn json_text_msg_id(request: &RawFrame) -> u32 {
    serde_json::from_slice::<serde_json::Value>(&request.payload)
        .ok()
        .and_then(|value| value.get("id").and_then(|id| id.as_u64()))
        .and_then(|id| u32::try_from(id).ok())
        .unwrap_or(0)
}

fn tracing_log_connection_error(_err: WireError) {
    // Transport logging is intentionally tiny until the server binary wires in
    // tracing configuration.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionPromotionOutcome;
    use crate::auth::JwtAlgorithm;
    use crate::frame::{BinaryFrameHeader, PROTOCOL_VERSION, read_frame, write_frame};
    use crate::messages::{
        MSG_BEGIN, MSG_COMMIT, MSG_CREATE_COLLECTION, MSG_CREATE_DATABASE, MSG_ERROR, MSG_GET,
        MSG_INSERT, MSG_INVALIDATION, MSG_OK, MSG_PING, MSG_PONG, MSG_QUERY, MSG_REPLACE,
        MSG_ROLLBACK,
    };
    use jsonwebtoken::{EncodingKey, Header, encode, get_current_timestamp};
    use quinn::crypto::rustls::QuicClientConfig as QuicRustlsClientConfig;
    use serde::Serialize;
    use serde_json::json;
    use std::io::Write as _;
    use std::net::{TcpListener as StdTcpListener, UdpSocket as StdUdpSocket};
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::net::TcpStream;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use tokio_tungstenite::{client_async, connect_async};

    const TEST_CERT_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIDKDCCAhCgAwIBAgIURfbhnJLLFcNkk6wFRmYlnKcEA9kwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDYwOTA2MTAyOVoYDzIxMjYw
NTE2MDYxMDI5WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDQ9f47bcrK1aM/oqISe3wT7zgeho+MDI9vlrJs1/9j
nHUnJhM2Gmv6/woBLneEgs9pxzWlXHWKywWO0F/DIKjevJNRT4J0RogYnXFYlTIo
E5f1+xN+S6HW5XAnClAHPZkbv9lscIzxe9jppbSYPs0nWy5AWQ9T8wZTshw+YLDd
QZFph0CB7qpE+3NK0iWw6kZO/YUMkCI64G11yoiXliXiyOsBtMvcL+qMQse9aJdf
Bj0tX6mGUeyXPnLFYFphA7AlUdgTCwrVMkWm92dB/HMM1bKP9WvudHrY8pOCPue6
bS3GePcnMPAokRedxdaTbZ63RfKzr3g/M734GNrpeJbPAgMBAAGjcDBuMAwGA1Ud
EwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBoG
A1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUO3jMscbxEgUugmI3
QBV4geL0uLMwDQYJKoZIhvcNAQELBQADggEBAJGWCdsIPaW8Iu8n/NpU2b/ASzak
gwFOb1md52eGtRqQZ5aVcwSrzvtIBVSgmoMmiH/4cedZODf8F77MyKVSw1Pk64Gz
NvJrmW25tBvdQONPapkr+wLHoKE21usdNn30/n1k2xr3wez5Emk7OXiuHdZ/HKZa
isr0QflTXAtVRahE7bs4/+uOGZ6uT61gRglCcnQgZIDqllceRGv/wkET0ZIMeS6K
a/tyv1hhL2QlWFjJoYanKSeOl9BWQdAtwsQ/7j1yzjdX+Z1SjWlNLmS+qPJ3VbW9
hL7LHLZDxnwKCQpAqHW4SLKu3PRRto+czF6NJDDuBkj7qfk3nzrATyPCdso=
-----END CERTIFICATE-----
";

    const TEST_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDQ9f47bcrK1aM/
oqISe3wT7zgeho+MDI9vlrJs1/9jnHUnJhM2Gmv6/woBLneEgs9pxzWlXHWKywWO
0F/DIKjevJNRT4J0RogYnXFYlTIoE5f1+xN+S6HW5XAnClAHPZkbv9lscIzxe9jp
pbSYPs0nWy5AWQ9T8wZTshw+YLDdQZFph0CB7qpE+3NK0iWw6kZO/YUMkCI64G11
yoiXliXiyOsBtMvcL+qMQse9aJdfBj0tX6mGUeyXPnLFYFphA7AlUdgTCwrVMkWm
92dB/HMM1bKP9WvudHrY8pOCPue6bS3GePcnMPAokRedxdaTbZ63RfKzr3g/M734
GNrpeJbPAgMBAAECggEAEyRNrzz+BDmw1C3+QcotEhhqYCV8ddxasWKxIpZgz0uw
Ua9DuEOQW7RMQtJyYWGoRWzZxbWkENxLPItrElFmFs1Yg2zQqv0hD3wwu2gjaZdt
5rsizIF6KFjpPrJLpXYnleqrrKrcxmxNcQ3cbsvl+DJ6mKtv44VSMY2R18b8vK/P
7mO4m/Fg6i2DIzOjwhf61P/Erm/g2MNQRquiHsjP1ia7tkdy0MGvMuVMa2+i4nL1
BHkM9Yr1YHL/2qvePR965hKcvl+67gUNR9w1fsgfHdCdjZUX/OfeXIk9ZOTvUpFT
jHeN5RMmWV7xAG4M7O05rjWmep6EZArwoAgiWM6k+QKBgQD73zby0ooRr1K2OHgx
LQ4HQm5UL5REZhxzz1ZGeecCfE54gYIhjvzobtf0aQVYbRqrS+73aoF/1GITaZga
rAawGzJf2MgEHdjcppF3K5aBeLWkM6uHDIUBzb5+7XAcdeevFWbLYa6yawb089yf
ESLikGGNZZF6lWWpp4vpCJGUpwKBgQDUYrxQrzObT6/jkW71OuPca2PxxBQYpkIA
WHzLqRq35uQelMl8N+ZRo3DIKzby3k/NAac3VsUbXKdbFkm2kkS+ebzDACRlf42i
MB1jlUuS9Y1EcXQ7//CnyIHiIVzILQ/nuiutOfuf0M0hj34ADeUNEUet2UHqiTD1
YRWjZhgpmQKBgEj3j4mlKM6axhF4JonIjanYuvG0nvV5x4BdbpcYNa5oqKsMidCD
Bg2oHvP1gNDvOqypYN9YgY+wzYDmNGR1tbJLDrrpqHhY1TyHHNkzTXTQrX6WYXjM
TbQKEMfgrXvxRF9aid8du2iAzRElnKKQalTMkxZNeGnU4hDWMxgdnV89AoGAYR0w
vLgQIfFjHOywTuP5sx1L2w3snoTPYzcTAVE2zWZ/Ythn9SveXfAdLvpLquwqkzQx
UOWVRXitccIUPK5PTsV9obDb86lKiyHzHkjzAKTVTrmOC61xTlcpxMu6kDHCtxPy
ysfbne0tDy58z+uKk9oV65GPSX4n69vTuB6D5+kCgYEA4dnLObKyixUPXKlQF4hr
wwPtpPeE2eo7W4pHX8rY7Elrh3N/hKSL5qbrU5ZlkeFhzoNCC2zwdziGqJqT5dAr
uw+4eP75VPoDsxljqSDn344YhNCSCa1hp1R9yy9lO4X2ZG7cUYMTr3n6ors6yLqJ
rR1L8q3lzoY37ZfAklp7Uzs=
-----END PRIVATE KEY-----
";

    #[derive(Serialize)]
    struct TestClaims<'a> {
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        role: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        databases: Option<Vec<&'a str>>,
    }

    #[derive(Default)]
    struct BlockingPromoter {
        started: AtomicUsize,
        started_notify: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl BlockingPromoter {
        async fn wait_started(&self, expected: usize) {
            loop {
                if self.started.load(Ordering::Acquire) >= expected {
                    return;
                }
                self.started_notify.notified().await;
            }
        }

        fn release_all(&self) {
            self.release.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl TransactionPromoter for BlockingPromoter {
        async fn promote_transaction(
            &self,
            _database: &str,
            _begin_ts: exdb::Ts,
            _subscription: exdb::SubscriptionMode,
            _payload: Vec<u8>,
        ) -> std::result::Result<TransactionPromotionOutcome, String> {
            Ok(TransactionPromotionOutcome::Success { commit_ts: 1 })
        }

        async fn promote_ddl(
            &self,
            _request: crate::session::DdlPromotionRequest,
        ) -> std::result::Result<ServerMessage, String> {
            self.started.fetch_add(1, Ordering::AcqRel);
            self.started_notify.notify_waiters();
            self.release.notified().await;
            Ok(ServerMessage::Ok { fields: json!({}) })
        }
    }

    struct StaticReplicaReadGate(bool);

    impl ReplicaReadGate for StaticReplicaReadGate {
        fn has_read_quorum(&self, _database: &str) -> bool {
            self.0
        }
    }

    fn hmac_token(secret: &[u8], exp: u64) -> String {
        encode(
            &Header::new(jsonwebtoken::Algorithm::HS256),
            &TestClaims {
                exp,
                role: Some("admin"),
                databases: None,
            },
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    fn hmac_user_database_token(secret: &[u8], exp: u64, databases: Vec<&str>) -> String {
        encode(
            &Header::new(jsonwebtoken::Algorithm::HS256),
            &TestClaims {
                exp,
                role: Some("user"),
                databases: Some(databases),
            },
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    async fn registry() -> Arc<SystemDatabase> {
        static TEMP_DIRS: std::sync::Mutex<Vec<tempfile::TempDir>> =
            std::sync::Mutex::new(Vec::new());
        let tmp = tempdir().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        TEMP_DIRS.lock().unwrap().push(tmp);
        registry
    }

    fn assert_invalid_config_contains(config: ServerConfig, expected: &str) {
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected '{expected}' in '{err}'"
        );
    }

    #[test]
    fn server_config_validate_rejects_unusable_resource_limits() {
        let mut config = ServerConfig {
            max_message_size: 0,
            ..ServerConfig::default()
        };
        assert_invalid_config_contains(config.clone(), "max_message_size");

        config.max_message_size = DEFAULT_MAX_MESSAGE_SIZE;
        config.request_queue_capacity = 0;
        assert_invalid_config_contains(config.clone(), "request_queue_capacity");

        config.request_queue_capacity = DEFAULT_REQUEST_QUEUE_CAPACITY;
        config.response_write_timeout = Duration::ZERO;
        assert_invalid_config_contains(config, "response_write_timeout_ms");
    }

    #[test]
    fn server_config_validate_rejects_invalid_auth_key_material() {
        let config = ServerConfig {
            auth: AuthConfig {
                enabled: true,
                algorithm: JwtAlgorithm::HS256,
                secret: None,
                public_key: None,
                issuer: None,
            },
            ..ServerConfig::default()
        };
        assert_invalid_config_contains(config, "auth.jwt_secret");

        let config = ServerConfig {
            auth: AuthConfig::public_key(JwtAlgorithm::RS256, b"not a public key".to_vec()),
            ..ServerConfig::default()
        };
        assert_invalid_config_contains(config, "invalid RSA JWT public key");
    }

    #[test]
    fn server_config_validate_rejects_secure_listeners_without_tls_material() {
        let mut config = ServerConfig {
            listen: ListenConfig {
                tls: Some("127.0.0.1:0".parse().unwrap()),
                ..ListenConfig::default()
            },
            tls: None,
            ..ServerConfig::default()
        };
        assert_invalid_config_contains(config.clone(), "TLS listener requires");

        config.listen.tls = None;
        config.listen.websocket_tls = Some("127.0.0.1:0".parse().unwrap());
        assert_invalid_config_contains(config.clone(), "secure WebSocket listener requires");

        config.listen.websocket_tls = None;
        config.listen.quic = Some("127.0.0.1:0".parse().unwrap());
        assert_invalid_config_contains(config, "QUIC listener requires");
    }

    #[test]
    fn server_config_validate_rejects_malformed_tls_material() {
        let mut config = ServerConfig {
            listen: ListenConfig {
                tls: Some("127.0.0.1:0".parse().unwrap()),
                ..ListenConfig::default()
            },
            tls: Some(TlsConfig::from_pem(
                b"not a certificate".to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..ServerConfig::default()
        };
        assert_invalid_config_contains(config.clone(), "certificate PEM did not contain");

        config.tls = Some(TlsConfig::from_pem(
            TEST_CERT_PEM.to_vec(),
            b"not a private key".to_vec(),
        ));
        assert_invalid_config_contains(config, "private key PEM did not contain");
    }

    #[tokio::test]
    async fn server_start_rejects_invalid_config_before_listening() {
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some("127.0.0.1:0".parse().unwrap()),
                ..ListenConfig::default()
            },
            request_queue_capacity: 0,
            ..ServerConfig::default()
        };
        let server = Server::new(config, registry().await);

        let err = server.start(CancellationToken::new()).await.unwrap_err();
        assert!(err.to_string().contains("request_queue_capacity"));
    }

    #[tokio::test]
    async fn server_handle_connection_rejects_invalid_config() {
        let config = ServerConfig {
            response_write_timeout: Duration::ZERO,
            ..ServerConfig::default()
        };
        let server = Server::new(config, registry().await);
        let (_client, server_stream) = duplex(64);

        let err = server.handle_connection(server_stream).await.unwrap_err();
        assert!(err.to_string().contains("response_write_timeout_ms"));
    }

    fn json_payload(frame: &RawFrame) -> serde_json::Value {
        serde_json::from_slice(&frame.payload).unwrap()
    }

    fn bson_payload(frame: &RawFrame) -> serde_json::Value {
        bson::from_slice(&frame.payload).unwrap()
    }

    fn protobuf_payload(frame: &RawFrame) -> serde_json::Value {
        serde_json::Value::Object(crate::protobuf::decode_object(&frame.payload).unwrap())
    }

    fn assert_usage_accounting(usage: &serde_json::Value, expected_active_transactions: u64) {
        let disk_usage_bytes = usage["disk_usage_bytes"].as_u64().unwrap();
        let page_store_bytes = usage["page_store_bytes"].as_u64().unwrap();
        let wal_retained_bytes = usage["wal_retained_bytes"].as_u64().unwrap();
        let memory_budget_bytes = usage["memory_budget_bytes"].as_u64().unwrap();
        let buffer_pool_used_frames = usage["buffer_pool_used_frames"].as_u64().unwrap();
        let active_transactions = usage["active_transactions"].as_u64().unwrap();
        let page_count = usage["page_count"].as_u64().unwrap();
        let page_size = usage["page_size"].as_u64().unwrap();

        assert!(disk_usage_bytes > 0);
        assert_eq!(disk_usage_bytes, page_store_bytes + wal_retained_bytes);
        assert_eq!(page_store_bytes, page_count * page_size);
        assert!(memory_budget_bytes >= page_size);
        assert!(buffer_pool_used_frames > 0);
        assert_eq!(active_transactions, expected_active_transactions);
    }

    async fn send_json<S>(client: &mut S, value: serde_json::Value)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        write_frame(
            client,
            &RawFrame::json_text(serde_json::to_vec(&value).unwrap()),
        )
        .await
        .unwrap();
    }

    async fn recv_json<S>(client: &mut S) -> serde_json::Value
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        json_payload(&read_frame(client).await.unwrap())
    }

    async fn assert_auth_pipeline_reaches_replica_read_gate<S>(client: &mut S, token: String)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        for msg in [
            json!({"id":1,"type":"authenticate","token":token}),
            json!({"id":2,"type":"begin","database":"app","readonly":true}),
        ] {
            write_frame(
                client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let authenticated = read_frame(client).await.unwrap();
        assert_eq!(json_payload(&authenticated), json!({"id":1,"type":"ok"}));

        let fenced = read_frame(client).await.unwrap();
        assert_eq!(json_payload(&fenced)["id"], 2);
        assert_eq!(json_payload(&fenced)["type"], "error");
        assert_eq!(json_payload(&fenced)["code"], "quorum_lost");

        write_frame(
            client,
            &RawFrame::json_text(br#"{"id":3,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let pong = read_frame(client).await.unwrap();
        assert_eq!(json_payload(&pong), json!({"id":3,"type":"pong"}));
    }

    async fn send_ws_json<S>(client: &mut WebSocketStream<S>, value: serde_json::Value)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        client
            .send(Message::Text(serde_json::to_string(&value).unwrap().into()))
            .await
            .unwrap();
    }

    async fn recv_ws_json<S>(client: &mut WebSocketStream<S>) -> serde_json::Value
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match client.next().await.unwrap().unwrap() {
            Message::Text(text) => serde_json::from_str(&text).unwrap(),
            other => panic!("expected WebSocket text message, got {other:?}"),
        }
    }

    async fn assert_auth_websocket_pipeline_reaches_replica_read_gate<S>(
        client: &mut WebSocketStream<S>,
        token: String,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        for msg in [
            json!({"id":1,"type":"authenticate","token":token}),
            json!({"id":2,"type":"begin","database":"app","readonly":true}),
        ] {
            send_ws_json(client, msg).await;
        }

        assert_eq!(recv_ws_json(client).await, json!({"id":1,"type":"ok"}));

        let fenced = recv_ws_json(client).await;
        assert_eq!(fenced["id"], 2);
        assert_eq!(fenced["type"], "error");
        assert_eq!(fenced["code"], "quorum_lost");

        send_ws_json(client, json!({"id":3,"type":"ping"})).await;
        assert_eq!(recv_ws_json(client).await, json!({"id":3,"type":"pong"}));
    }

    async fn send_ws_binary<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
        encoding: Encoding,
        payload: Vec<u8>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let request = RawFrame::binary(msg_id, msg_type, encoding, payload);
        client
            .send(Message::Binary(
                encode_binary_frame_with_limit(&request, DEFAULT_MAX_MESSAGE_SIZE)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();
    }

    async fn recv_ws_binary<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
        encoding: Encoding,
    ) -> RawFrame
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let response = client.next().await.unwrap().unwrap();
        let Message::Binary(response) = response else {
            panic!("expected WebSocket binary response, got {response:?}");
        };
        let frame = decode_binary_frame_with_limit(&response, DEFAULT_MAX_MESSAGE_SIZE).unwrap();
        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, msg_id);
        assert_eq!(frame.msg_type, msg_type);
        assert_eq!(frame.encoding, encoding);
        frame
    }

    async fn send_ws_binary_json<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
        payload: serde_json::Value,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        send_ws_binary(
            client,
            msg_id,
            msg_type,
            Encoding::Json,
            serde_json::to_vec(&payload).unwrap(),
        )
        .await;
    }

    async fn recv_ws_binary_json<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
    ) -> serde_json::Value
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = recv_ws_binary(client, msg_id, msg_type, Encoding::Json).await;
        json_payload(&frame)
    }

    async fn send_ws_bson<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
        value: serde_json::Value,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        send_ws_binary(
            client,
            msg_id,
            msg_type,
            Encoding::Bson,
            bson::to_vec(&value).unwrap(),
        )
        .await;
    }

    async fn recv_ws_bson<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
    ) -> serde_json::Value
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = recv_ws_binary(client, msg_id, msg_type, Encoding::Bson).await;
        bson_payload(&frame)
    }

    async fn send_ws_protobuf<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
        value: serde_json::Value,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        send_ws_binary(
            client,
            msg_id,
            msg_type,
            Encoding::Protobuf,
            crate::protobuf::encode_object(value.as_object().unwrap().clone()).unwrap(),
        )
        .await;
    }

    async fn recv_ws_protobuf<S>(
        client: &mut WebSocketStream<S>,
        msg_id: u32,
        msg_type: u8,
    ) -> serde_json::Value
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = recv_ws_binary(client, msg_id, msg_type, Encoding::Protobuf).await;
        protobuf_payload(&frame)
    }

    async fn send_bson<S>(client: &mut S, msg_id: u32, msg_type: u8, value: serde_json::Value)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        write_frame(
            client,
            &RawFrame::binary(
                msg_id,
                msg_type,
                Encoding::Bson,
                bson::to_vec(&value).unwrap(),
            ),
        )
        .await
        .unwrap();
    }

    async fn recv_bson<S>(client: &mut S, msg_id: u32, msg_type: u8) -> serde_json::Value
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let frame = read_frame(client).await.unwrap();
        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, msg_id);
        assert_eq!(frame.msg_type, msg_type);
        assert_eq!(frame.encoding, Encoding::Bson);
        bson_payload(&frame)
    }

    async fn send_protobuf<S>(client: &mut S, msg_id: u32, msg_type: u8, value: serde_json::Value)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        write_frame(
            client,
            &RawFrame::binary(
                msg_id,
                msg_type,
                Encoding::Protobuf,
                crate::protobuf::encode_object(value.as_object().unwrap().clone()).unwrap(),
            ),
        )
        .await
        .unwrap();
    }

    async fn recv_protobuf<S>(client: &mut S, msg_id: u32, msg_type: u8) -> serde_json::Value
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let frame = read_frame(client).await.unwrap();
        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, msg_id);
        assert_eq!(frame.msg_type, msg_type);
        assert_eq!(frame.encoding, Encoding::Protobuf);
        protobuf_payload(&frame)
    }

    #[tokio::test]
    async fn connection_sends_hello_and_handles_json_ping() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(
            ServerConfig::default(),
            Arc::clone(&registry().await),
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(hello.frame_type, FrameType::JsonText);
        assert_eq!(json_payload(&hello)["type"], "hello");

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(response.frame_type, FrameType::JsonText);
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_applies_replica_read_gate_to_readonly_begin() {
        let (mut client, server_stream) = duplex(4096);
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let server = Arc::new(Server::new(
            ServerConfig {
                node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
                replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
                ..Default::default()
            },
            registry,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(
            &mut client,
            json!({
                "id": 1,
                "type": "begin",
                "database": "app",
                "readonly": true
            }),
        )
        .await;

        let response = recv_json(&mut client).await;
        assert_eq!(response["id"], 1);
        assert_eq!(response["type"], "error");
        assert_eq!(response["code"], "quorum_lost");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn unauthenticated_management_message_is_rejected_before_async_dispatch() {
        let (mut client, server_stream) = duplex(4096);
        let config = ServerConfig {
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;

        let response = recv_json(&mut client).await;
        assert_eq!(response["id"], 1);
        assert_eq!(response["type"], "error");
        assert_eq!(response["code"], "auth_required");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn duplicate_management_message_id_produces_no_second_response() {
        let (mut client, server_stream) = duplex(4096);
        let registry = registry().await;
        let server = Arc::new(Server::new(ServerConfig::default(), Arc::clone(&registry)));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"duplicate"}),
        )
        .await;

        let response = recv_json(&mut client).await;
        assert_eq!(response["id"], 1);
        assert_eq!(response["type"], "ok");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), recv_json(&mut client))
                .await
                .is_err()
        );

        let databases: Vec<String> = registry
            .list_databases()
            .into_iter()
            .map(|meta| meta.name)
            .collect();
        assert_eq!(databases, vec!["app".to_string()]);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authenticated_replica_quorum_session_allows_reads_and_rejects_local_writes() {
        let (mut client, server_stream) = duplex(65_536);
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let db = registry.get_database_by_name("app").unwrap();
        let mut seed = db
            .begin(exdb::TransactionOptions::default())
            .expect("seed transaction");
        seed.create_collection("users")
            .await
            .expect("seed collection");
        let doc_id = seed
            .insert("users", json!({"name": "Ada", "score": 42}))
            .await
            .expect("seed document");
        let doc_id = exdb::encode_ulid(&doc_id);
        assert!(matches!(
            seed.commit().await.expect("seed commit"),
            exdb::TransactionResult::Success { .. }
        ));

        let secret = b"secret";
        let server = Arc::new(Server::new(
            ServerConfig {
                auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
                node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
                replica_read_gate: Some(Arc::new(StaticReplicaReadGate(true))),
                ..Default::default()
            },
            registry,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let hello = recv_json(&mut client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);
        assert_eq!(hello["node_role"], "replica");

        send_bson(
            &mut client,
            1,
            MSG_BEGIN,
            json!({"database":"app","readonly":true}),
        )
        .await;
        let unauthenticated_read = recv_bson(&mut client, 1, MSG_ERROR).await;
        assert_eq!(unauthenticated_read["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        send_json(
            &mut client,
            json!({"id":2,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":2,"type":"ok"}));

        send_json(
            &mut client,
            json!({"id":3,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_bson(
            &mut client,
            4,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_bson(&mut client, 4, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["score"], 42);
        send_protobuf(&mut client, 5, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_protobuf(&mut client, 5, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_json(&mut client, json!({"id":6,"type":"begin","database":"app"})).await;
        let write_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_bson(
            &mut client,
            7,
            MSG_INSERT,
            json!({
                "tx": write_tx,
                "collection": "users",
                "body": {"name": "Replica Local Write"}
            }),
        )
        .await;
        assert!(
            recv_bson(&mut client, 7, MSG_OK).await["doc_id"]
                .as_str()
                .is_some()
        );
        send_protobuf(&mut client, 8, MSG_COMMIT, json!({"tx": write_tx})).await;
        let write_rejected = recv_protobuf(&mut client, 8, MSG_ERROR).await;
        assert_eq!(write_rejected["code"], "readonly_node");

        send_json(&mut client, json!({"id":9,"type":"list_databases"})).await;
        let databases = recv_json(&mut client).await;
        let app = databases["databases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|database| database["name"] == "app")
            .expect("app database should be listed");
        assert_usage_accounting(&app["usage"], 0);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authenticated_multi_client_subscription_receives_cross_connection_invalidation() {
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let db = registry.get_database_by_name("app").unwrap();
        let mut seed = db
            .begin(exdb::TransactionOptions::default())
            .expect("seed transaction");
        seed.create_collection("users")
            .await
            .expect("seed collection");
        let doc_id = seed
            .insert("users", json!({"name": "Ada", "score": 1}))
            .await
            .expect("seed document");
        let doc_id = exdb::encode_ulid(&doc_id);
        assert!(matches!(
            seed.commit().await.expect("seed commit"),
            exdb::TransactionResult::Success { .. }
        ));

        let secret = b"secret";
        let server = Arc::new(Server::new(
            ServerConfig {
                auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
                ..Default::default()
            },
            registry,
        ));
        let (mut subscriber, subscriber_stream) = duplex(65_536);
        let (mut writer, writer_stream) = duplex(65_536);
        let subscriber_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.handle_connection(subscriber_stream).await })
        };
        let writer_task =
            tokio::spawn(async move { server.handle_connection(writer_stream).await });

        assert_eq!(recv_json(&mut subscriber).await["type"], "hello");
        assert_eq!(recv_json(&mut writer).await["type"], "hello");

        let token = hmac_user_database_token(secret, get_current_timestamp() + 60, vec!["app"]);
        send_json(
            &mut subscriber,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(
            recv_json(&mut subscriber).await,
            json!({"id":1,"type":"ok"})
        );
        let token = hmac_user_database_token(secret, get_current_timestamp() + 60, vec!["app"]);
        send_json(
            &mut writer,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut writer).await, json!({"id":1,"type":"ok"}));

        send_json(&mut subscriber, json!({"id":2,"type":"list_databases"})).await;
        let visible = recv_json(&mut subscriber).await;
        let databases: Vec<_> = visible["databases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|database| database["name"].as_str().unwrap())
            .collect();
        assert_eq!(databases, vec!["app"]);

        send_json(
            &mut subscriber,
            json!({"id":3,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut subscriber).await["tx"].as_u64().unwrap();
        send_json(
            &mut subscriber,
            json!({"id":4,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut subscriber).await["query_id"], 0);
        send_json(&mut subscriber, json!({"id":5,"type":"commit","tx":sub_tx})).await;
        let subscribed = recv_json(&mut subscriber).await;
        assert_eq!(subscribed["type"], "ok");
        assert!(subscribed["subscription_id"].as_u64().is_some());

        send_json(&mut writer, json!({"id":2,"type":"begin","database":"app"})).await;
        let write_tx = recv_json(&mut writer).await["tx"].as_u64().unwrap();
        send_json(
            &mut writer,
            json!({"id":3,"type":"replace","tx":write_tx,"collection":"users","doc_id":doc_id,"body":{"name":"Ada Lovelace","score":2}}),
        )
        .await;
        assert_eq!(recv_json(&mut writer).await["type"], "ok");
        send_json(&mut writer, json!({"id":4,"type":"commit","tx":write_tx})).await;
        assert_eq!(recv_json(&mut writer).await["type"], "ok");

        let invalidation = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut subscriber))
            .await
            .expect("subscriber should receive cross-connection invalidation");
        assert_eq!(invalidation["type"], "invalidation");
        assert_eq!(invalidation["tx"], sub_tx);
        assert_eq!(invalidation["queries"], json!([0]));
        assert!(invalidation["commit_ts"].as_u64().unwrap() > 0);

        drop(subscriber);
        drop(writer);
        subscriber_task.await.unwrap().unwrap();
        writer_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn authenticated_multi_client_mixed_encoding_subscription_and_pipeline_coexist() {
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let db = registry.get_database_by_name("app").unwrap();
        let mut seed = db
            .begin(exdb::TransactionOptions::default())
            .expect("seed transaction");
        seed.create_collection("users")
            .await
            .expect("seed collection");
        let doc_id = seed
            .insert("users", json!({"name": "Ada", "score": 1}))
            .await
            .expect("seed document");
        let doc_id = exdb::encode_ulid(&doc_id);
        assert!(matches!(
            seed.commit().await.expect("seed commit"),
            exdb::TransactionResult::Success { .. }
        ));

        let secret = b"secret";
        let server = Arc::new(Server::new(
            ServerConfig {
                auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
                ..Default::default()
            },
            registry,
        ));
        let (mut subscriber, subscriber_stream) = duplex(65_536);
        let (mut writer, writer_stream) = duplex(65_536);
        let subscriber_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.handle_connection(subscriber_stream).await })
        };
        let writer_task =
            tokio::spawn(async move { server.handle_connection(writer_stream).await });

        assert_eq!(recv_json(&mut subscriber).await["type"], "hello");
        assert_eq!(recv_json(&mut writer).await["type"], "hello");

        let token = hmac_user_database_token(secret, get_current_timestamp() + 60, vec!["app"]);
        send_json(
            &mut subscriber,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(
            recv_json(&mut subscriber).await,
            json!({"id":1,"type":"ok"})
        );
        let token = hmac_user_database_token(secret, get_current_timestamp() + 60, vec!["app"]);
        send_json(
            &mut writer,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut writer).await, json!({"id":1,"type":"ok"}));

        send_json(
            &mut subscriber,
            json!({"id":2,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut subscriber).await["tx"].as_u64().unwrap();
        send_bson(
            &mut subscriber,
            3,
            MSG_GET,
            json!({
                "tx": sub_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_bson(&mut subscriber, 3, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["score"], 1);
        send_bson(&mut subscriber, 4, MSG_COMMIT, json!({"tx": sub_tx})).await;
        assert!(
            recv_bson(&mut subscriber, 4, MSG_OK).await["subscription_id"]
                .as_u64()
                .is_some()
        );

        send_protobuf(&mut writer, 2, MSG_BEGIN, json!({"database":"app"})).await;
        let writer_tx = recv_protobuf(&mut writer, 2, MSG_OK).await["tx"]
            .as_u64()
            .unwrap();
        for frame in [
            RawFrame::binary(
                3,
                MSG_REPLACE,
                Encoding::Protobuf,
                crate::protobuf::encode_object(
                    json!({
                        "tx": writer_tx,
                        "collection": "users",
                        "doc_id": doc_id,
                        "body": {"name": "Ada Lovelace", "score": 2}
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                )
                .unwrap(),
            ),
            RawFrame::binary(
                4,
                MSG_COMMIT,
                Encoding::Protobuf,
                crate::protobuf::encode_object(
                    json!({"tx": writer_tx}).as_object().unwrap().clone(),
                )
                .unwrap(),
            ),
        ] {
            write_frame(&mut writer, &frame).await.unwrap();
        }
        assert!(
            recv_protobuf(&mut writer, 3, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );
        assert!(
            recv_protobuf(&mut writer, 4, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_bson(&mut subscriber, 5, MSG_PING, json!({})).await;
        let mut saw_pong = false;
        let mut saw_invalidation = false;
        for _ in 0..2 {
            let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut subscriber))
                .await
                .expect("expected BSON ping response and pushed invalidation")
                .unwrap();
            assert_eq!(frame.frame_type, FrameType::Binary);
            assert_eq!(frame.encoding, Encoding::Bson);
            match (frame.msg_id, frame.msg_type) {
                (5, MSG_PONG) => {
                    let payload = bson_payload(&frame);
                    assert!(payload.as_object().unwrap().is_empty());
                    saw_pong = true;
                }
                (0, MSG_INVALIDATION) => {
                    let payload = bson_payload(&frame);
                    assert_eq!(payload["tx"], sub_tx);
                    assert_eq!(payload["queries"], json!([0]));
                    assert!(payload["commit_ts"].as_u64().unwrap() > 0);
                    saw_invalidation = true;
                }
                other => panic!("unexpected frame header {other:?}: {frame:?}"),
            }
        }
        assert!(saw_pong);
        assert!(saw_invalidation);

        drop(subscriber);
        drop(writer);
        subscriber_task.await.unwrap().unwrap();
        writer_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_mirrors_binary_json_response() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let _hello = read_frame(&mut client).await.unwrap();
        write_frame(
            &mut client,
            &RawFrame::binary(1, MSG_PING, Encoding::Json, b"{}".to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client).await.unwrap();

        assert_eq!(response.frame_type, FrameType::Binary);
        assert_eq!(response.msg_id, 1);
        assert_eq!(response.msg_type, MSG_PONG);
        assert_eq!(response.encoding, Encoding::Json);
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_mirrors_binary_protobuf_response() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(
            json_payload(&hello)["encodings"],
            json!(["json", "bson", "protobuf"])
        );

        send_protobuf(&mut client, 1, MSG_PING, json!({})).await;
        assert_eq!(recv_protobuf(&mut client, 1, MSG_PONG).await, json!({}));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_mixed_json_bson_and_protobuf_messages() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        run_mixed_json_bson_and_protobuf_lifecycle(&mut client).await;

        drop(client);
        task.await.unwrap().unwrap();
    }

    async fn run_mixed_json_bson_and_protobuf_lifecycle<S>(client: &mut S)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        assert_eq!(recv_json(client).await["type"], "hello");

        send_json(
            client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(client).await, json!({"id":1,"type":"ok"}));

        send_bson(
            client,
            2,
            MSG_CREATE_COLLECTION,
            json!({"database":"app","name":"users"}),
        )
        .await;
        assert!(
            recv_bson(client, 2, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_protobuf(client, 3, MSG_PING, json!({})).await;
        assert_eq!(recv_protobuf(client, 3, MSG_PONG).await, json!({}));

        send_json(client, json!({"id":4,"type":"begin","database":"app"})).await;
        let tx = recv_json(client).await["tx"].as_u64().unwrap();

        send_bson(
            client,
            5,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {"name": "Ada", "score": 42}
            }),
        )
        .await;
        let doc_id = recv_bson(client, 5, MSG_OK).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();

        send_protobuf(client, 6, MSG_COMMIT, json!({"tx": tx})).await;
        assert!(
            recv_protobuf(client, 6, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_json(
            client,
            json!({"id":7,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(client).await["tx"].as_u64().unwrap();

        send_bson(
            client,
            8,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_bson(client, 8, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["score"], 42);

        send_protobuf(client, 9, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_protobuf(client, 9, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    async fn run_authenticated_mixed_json_bson_and_protobuf_lifecycle<S>(
        client: &mut S,
        token: String,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let hello = recv_json(client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);

        send_bson(client, 1, MSG_PING, json!({})).await;
        let rejected = recv_bson(client, 1, MSG_ERROR).await;
        assert_eq!(rejected["code"], "auth_required");

        send_json(client, json!({"id":2,"type":"authenticate","token":token})).await;
        assert_eq!(recv_json(client).await, json!({"id":2,"type":"ok"}));

        send_protobuf(client, 3, MSG_CREATE_DATABASE, json!({"name":"app"})).await;
        assert!(
            recv_protobuf(client, 3, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_bson(
            client,
            4,
            MSG_CREATE_COLLECTION,
            json!({"database":"app","name":"users"}),
        )
        .await;
        assert!(
            recv_bson(client, 4, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_protobuf(client, 5, MSG_PING, json!({})).await;
        assert_eq!(recv_protobuf(client, 5, MSG_PONG).await, json!({}));

        send_json(client, json!({"id":6,"type":"begin","database":"app"})).await;
        let tx = recv_json(client).await["tx"].as_u64().unwrap();

        send_bson(
            client,
            7,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {"name": "Ada", "score": 99}
            }),
        )
        .await;
        let doc_id = recv_bson(client, 7, MSG_OK).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();

        send_protobuf(client, 8, MSG_COMMIT, json!({"tx": tx})).await;
        assert!(
            recv_protobuf(client, 8, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_json(
            client,
            json!({"id":9,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(client).await["tx"].as_u64().unwrap();

        send_bson(
            client,
            10,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_bson(client, 10, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["score"], 99);

        send_protobuf(client, 11, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_protobuf(client, 11, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    async fn run_websocket_mixed_json_bson_and_protobuf_lifecycle<S>(
        client: &mut WebSocketStream<S>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        assert_eq!(recv_ws_json(client).await["type"], "hello");

        send_ws_json(
            client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_ws_json(client).await, json!({"id":1,"type":"ok"}));

        send_ws_bson(
            client,
            2,
            MSG_CREATE_COLLECTION,
            json!({"database":"app","name":"users"}),
        )
        .await;
        assert!(
            recv_ws_bson(client, 2, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_ws_protobuf(client, 3, MSG_PING, json!({})).await;
        assert_eq!(recv_ws_protobuf(client, 3, MSG_PONG).await, json!({}));

        send_ws_json(client, json!({"id":4,"type":"begin","database":"app"})).await;
        let tx = recv_ws_json(client).await["tx"].as_u64().unwrap();

        send_ws_bson(
            client,
            5,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {"name": "Ada", "score": 42}
            }),
        )
        .await;
        let doc_id = recv_ws_bson(client, 5, MSG_OK).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();

        send_ws_protobuf(client, 6, MSG_COMMIT, json!({"tx": tx})).await;
        assert!(
            recv_ws_protobuf(client, 6, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_ws_json(
            client,
            json!({"id":7,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_ws_json(client).await["tx"].as_u64().unwrap();

        send_ws_bson(
            client,
            8,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_ws_bson(client, 8, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["score"], 42);

        send_ws_protobuf(client, 9, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_ws_protobuf(client, 9, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    async fn run_authenticated_websocket_mixed_json_bson_and_protobuf_lifecycle<S>(
        client: &mut WebSocketStream<S>,
        token: String,
    ) where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let hello = recv_ws_json(client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);

        send_ws_bson(client, 1, MSG_PING, json!({})).await;
        let rejected = recv_ws_bson(client, 1, MSG_ERROR).await;
        assert_eq!(rejected["code"], "auth_required");

        send_ws_json(client, json!({"id":2,"type":"authenticate","token":token})).await;
        assert_eq!(recv_ws_json(client).await, json!({"id":2,"type":"ok"}));

        send_ws_protobuf(client, 3, MSG_CREATE_DATABASE, json!({"name":"app"})).await;
        assert!(
            recv_ws_protobuf(client, 3, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_ws_bson(
            client,
            4,
            MSG_CREATE_COLLECTION,
            json!({"database":"app","name":"users"}),
        )
        .await;
        assert!(
            recv_ws_bson(client, 4, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_ws_protobuf(client, 5, MSG_PING, json!({})).await;
        assert_eq!(recv_ws_protobuf(client, 5, MSG_PONG).await, json!({}));

        send_ws_json(client, json!({"id":6,"type":"begin","database":"app"})).await;
        let tx = recv_ws_json(client).await["tx"].as_u64().unwrap();

        send_ws_bson(
            client,
            7,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {"name": "Ada", "score": 99}
            }),
        )
        .await;
        let doc_id = recv_ws_bson(client, 7, MSG_OK).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();

        send_ws_protobuf(client, 8, MSG_COMMIT, json!({"tx": tx})).await;
        assert!(
            recv_ws_protobuf(client, 8, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_ws_json(
            client,
            json!({"id":9,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_ws_json(client).await["tx"].as_u64().unwrap();

        send_ws_bson(
            client,
            10,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_ws_bson(client, 10, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["score"], 99);

        send_ws_protobuf(client, 11, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_ws_protobuf(client, 11, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[tokio::test]
    async fn binary_oversized_frame_returns_message_too_large_before_close() {
        let max_message_size = 1024usize;
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(
            ServerConfig {
                max_message_size,
                ..Default::default()
            },
            registry().await,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        let header = BinaryFrameHeader {
            version: PROTOCOL_VERSION,
            flags: 0,
            encoding: Encoding::Bson,
            msg_type: MSG_PING,
            msg_id: 1,
            length: (max_message_size + 1) as u32,
        };
        client.write_all(&header.encode()).await.unwrap();

        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(response.frame_type, FrameType::Binary);
        assert_eq!(response.msg_id, 1);
        assert_eq!(response.msg_type, MSG_ERROR);
        assert_eq!(response.encoding, Encoding::Bson);

        let payload = bson_payload(&response);
        assert_eq!(payload["code"], "message_too_large");
        assert_eq!(payload["length"], max_message_size + 1);
        assert_eq!(payload["limit"], max_message_size);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn binary_compressed_oversized_frame_returns_message_too_large_before_close() {
        let max_message_size = 1024usize;
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(
            ServerConfig {
                max_message_size,
                ..Default::default()
            },
            registry().await,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder
            .write_all(&vec![b'a'; max_message_size + 1])
            .unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() <= max_message_size);

        let header = BinaryFrameHeader {
            version: PROTOCOL_VERSION,
            flags: 0x01,
            encoding: Encoding::Bson,
            msg_type: MSG_PING,
            msg_id: 1,
            length: compressed.len() as u32,
        };
        client.write_all(&header.encode()).await.unwrap();
        client.write_all(&compressed).await.unwrap();

        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(response.frame_type, FrameType::Binary);
        assert_eq!(response.msg_id, 1);
        assert_eq!(response.msg_type, MSG_ERROR);
        assert_eq!(response.encoding, Encoding::Bson);

        let payload = bson_payload(&response);
        assert_eq!(payload["code"], "message_too_large");
        assert!(payload["length"].as_u64().unwrap() > max_message_size as u64);
        assert_eq!(payload["limit"], max_message_size);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_handles_binary_bson_crud_round_trip() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_bson(&mut client, 1, MSG_CREATE_DATABASE, json!({"name":"app"})).await;
        assert!(
            recv_bson(&mut client, 1, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_bson(
            &mut client,
            2,
            MSG_CREATE_COLLECTION,
            json!({"database":"app","name":"users"}),
        )
        .await;
        assert!(
            recv_bson(&mut client, 2, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_bson(&mut client, 3, MSG_BEGIN, json!({"database":"app"})).await;
        let tx = recv_bson(&mut client, 3, MSG_OK).await["tx"]
            .as_u64()
            .unwrap();

        send_bson(
            &mut client,
            4,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {
                    "name": "Ada",
                    "active": true,
                    "score": 42
                }
            }),
        )
        .await;
        let doc_id = recv_bson(&mut client, 4, MSG_OK).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();

        send_bson(&mut client, 5, MSG_COMMIT, json!({"tx": tx})).await;
        assert!(
            recv_bson(&mut client, 5, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        send_bson(
            &mut client,
            6,
            MSG_BEGIN,
            json!({"database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_bson(&mut client, 6, MSG_OK).await["tx"]
            .as_u64()
            .unwrap();

        send_bson(
            &mut client,
            7,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let fetched = recv_bson(&mut client, 7, MSG_OK).await;
        assert_eq!(fetched["query_id"], 0);
        assert_eq!(fetched["doc"]["name"], "Ada");
        assert_eq!(fetched["doc"]["active"], true);
        assert_eq!(fetched["doc"]["score"], 42);

        send_bson(&mut client, 8, MSG_COMMIT, json!({"tx": read_tx})).await;
        assert!(
            recv_bson(&mut client, 8, MSG_OK).await["commit_ts"]
                .as_u64()
                .unwrap()
                > 0
        );

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn binary_bson_request_sets_notification_encoding_for_pushed_invalidation() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["query_id"], 0);
        send_json(&mut client, json!({"id":8,"type":"commit","tx":sub_tx})).await;
        assert!(
            recv_json(&mut client).await["subscription_id"]
                .as_u64()
                .is_some()
        );

        send_bson(&mut client, 9, MSG_BEGIN, json!({"database":"app"})).await;
        let writer_tx = recv_bson(&mut client, 9, MSG_OK).await["tx"]
            .as_u64()
            .unwrap();
        send_bson(
            &mut client,
            10,
            MSG_REPLACE,
            json!({
                "tx": writer_tx,
                "collection": "users",
                "doc_id": doc_id,
                "body": {"name": "Ada Lovelace"}
            }),
        )
        .await;
        assert!(
            recv_bson(&mut client, 10, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_bson(&mut client, 11, MSG_COMMIT, json!({"tx": writer_tx})).await;
        let mut saw_commit = false;
        let mut saw_invalidation = false;
        for _ in 0..2 {
            let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut client))
                .await
                .expect("expected BSON commit response and pushed invalidation")
                .unwrap();
            assert_eq!(frame.frame_type, FrameType::Binary);
            assert_eq!(frame.encoding, Encoding::Bson);
            match (frame.msg_id, frame.msg_type) {
                (11, MSG_OK) => {
                    let payload = bson_payload(&frame);
                    assert!(payload["commit_ts"].as_u64().unwrap() > 0);
                    assert!(payload.get("type").is_none());
                    saw_commit = true;
                }
                (0, MSG_INVALIDATION) => {
                    let payload = bson_payload(&frame);
                    assert_eq!(payload["tx"], sub_tx);
                    assert_eq!(payload["queries"], json!([0]));
                    assert!(payload["commit_ts"].as_u64().unwrap() > 0);
                    assert!(payload.get("type").is_none());
                    saw_invalidation = true;
                }
                other => panic!("unexpected frame header {other:?}: {frame:?}"),
            }
        }
        assert!(saw_commit);
        assert!(saw_invalidation);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_pipelined_json_messages() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        for msg in [
            json!({"id":1,"type":"ping"}),
            json!({"id":2,"type":"ping"}),
            json!({"id":3,"type":"list_databases"}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        assert_eq!(recv_json(&mut client).await, json!({"id":1,"type":"pong"}));
        assert_eq!(recv_json(&mut client).await, json!({"id":2,"type":"pong"}));
        let list = recv_json(&mut client).await;
        assert_eq!(list["id"], 3);
        assert_eq!(list["type"], "ok");
        let databases = list["databases"].as_array().unwrap();
        assert!(databases.is_empty() || databases[0]["usage"].is_object());

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn list_databases_reports_active_wire_transactions() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":2,"type":"begin","database":"app"})).await;
        let begin = recv_json(&mut client).await;
        assert_eq!(begin["type"], "ok");
        let tx = begin["tx"].as_u64().unwrap();

        send_json(&mut client, json!({"id":3,"type":"list_databases"})).await;
        let list = recv_json(&mut client).await;
        assert_eq!(list["type"], "ok");
        let databases = list["databases"].as_array().unwrap();
        let app = databases
            .iter()
            .find(|database| database["name"] == "app")
            .expect("app database should be listed");
        assert_usage_accounting(&app["usage"], 1);

        send_json(&mut client, json!({"id":4,"type":"rollback","tx":tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":5,"type":"list_databases"})).await;
        let list = recv_json(&mut client).await;
        let app = list["databases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|database| database["name"] == "app")
            .expect("app database should still be listed");
        assert_usage_accounting(&app["usage"], 0);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_pipelined_json_messages_with_bounded_request_queue() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(
            ServerConfig {
                request_queue_capacity: 1,
                ..Default::default()
            },
            registry().await,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        for id in 1..=8 {
            write_frame(
                &mut client,
                &RawFrame::json_text(
                    serde_json::to_vec(&json!({"id": id, "type": "ping"})).unwrap(),
                ),
            )
            .await
            .unwrap();
        }

        for id in 1..=8 {
            assert_eq!(
                recv_json(&mut client).await,
                json!({"id": id, "type":"pong"})
            );
        }

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn binary_protobuf_request_sets_notification_encoding_for_pushed_invalidation() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["query_id"], 0);
        send_json(&mut client, json!({"id":8,"type":"commit","tx":sub_tx})).await;
        assert!(
            recv_json(&mut client).await["subscription_id"]
                .as_u64()
                .is_some()
        );

        send_protobuf(&mut client, 9, MSG_BEGIN, json!({"database":"app"})).await;
        let writer_tx = recv_protobuf(&mut client, 9, MSG_OK).await["tx"]
            .as_u64()
            .unwrap();
        send_protobuf(
            &mut client,
            10,
            MSG_REPLACE,
            json!({
                "tx": writer_tx,
                "collection": "users",
                "doc_id": doc_id,
                "body": {"name": "Ada Lovelace"}
            }),
        )
        .await;
        assert!(
            recv_protobuf(&mut client, 10, MSG_OK)
                .await
                .as_object()
                .unwrap()
                .is_empty()
        );

        send_protobuf(&mut client, 11, MSG_COMMIT, json!({"tx": writer_tx})).await;
        let mut saw_commit = false;
        let mut saw_invalidation = false;
        for _ in 0..2 {
            let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut client))
                .await
                .expect("expected Protobuf commit response and pushed invalidation")
                .unwrap();
            assert_eq!(frame.frame_type, FrameType::Binary);
            assert_eq!(frame.encoding, Encoding::Protobuf);
            match (frame.msg_id, frame.msg_type) {
                (11, MSG_OK) => {
                    let payload = protobuf_payload(&frame);
                    assert!(payload["commit_ts"].as_u64().unwrap() > 0);
                    assert!(payload.get("type").is_none());
                    saw_commit = true;
                }
                (0, MSG_INVALIDATION) => {
                    let payload = protobuf_payload(&frame);
                    assert_eq!(payload["tx"], sub_tx);
                    assert_eq!(payload["queries"], json!([0]));
                    assert!(payload["commit_ts"].as_u64().unwrap() > 0);
                    assert!(payload.get("type").is_none());
                    saw_invalidation = true;
                }
                other => panic!("unexpected frame header {other:?}: {frame:?}"),
            }
        }
        assert!(saw_commit);
        assert!(saw_invalidation);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[test]
    fn zero_request_queue_capacity_is_normalized_to_one() {
        assert_eq!(bounded_queue_capacity(0), 1);
        assert_eq!(bounded_queue_capacity(3), 3);
    }

    #[test]
    fn response_queue_is_bounded_by_connection_capacity() {
        let (response_tx, mut response_rx) = bounded_response_channel(1);
        let first = PendingResponse::Plain {
            request: RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
            response: ServerMessage::Pong,
        };
        let second = PendingResponse::Plain {
            request: RawFrame::json_text(br#"{"id":2,"type":"ping"}"#.to_vec()),
            response: ServerMessage::Pong,
        };

        assert!(response_tx.try_send(first).is_ok());
        assert!(response_tx.try_send(second).is_err());
        assert!(response_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn connection_event_rotation_prevents_notification_starvation() {
        let (response_tx, mut response_rx) = bounded_response_channel(4);
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let (notification_tx, mut notification_rx) = mpsc::channel(4);

        response_tx
            .send(PendingResponse::Plain {
                request: RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
                response: ServerMessage::Pong,
            })
            .await
            .unwrap();
        request_tx
            .send(Ok(RawFrame::json_text(
                br#"{"id":2,"type":"ping"}"#.to_vec(),
            )))
            .await
            .unwrap();
        notification_tx
            .send(SessionNotification::message(ServerMessage::Pong))
            .await
            .unwrap();

        assert!(matches!(
            next_connection_event(
                true,
                &mut response_rx,
                &mut request_rx,
                &mut notification_rx
            )
            .await,
            ConnectionEvent::PendingResponse(Some(_))
        ));

        assert!(matches!(
            next_connection_event(
                false,
                &mut response_rx,
                &mut request_rx,
                &mut notification_rx
            )
            .await,
            ConnectionEvent::Request(Some(Ok(_)))
        ));

        request_tx
            .send(Ok(RawFrame::json_text(
                br#"{"id":3,"type":"ping"}"#.to_vec(),
            )))
            .await
            .unwrap();
        assert!(matches!(
            next_connection_event(
                true,
                &mut response_rx,
                &mut request_rx,
                &mut notification_rx
            )
            .await,
            ConnectionEvent::Notification(Some(_))
        ));
    }

    fn assert_server_busy_extra(
        response: ServerMessage,
        scope: &str,
        reason: &str,
    ) -> serde_json::Value {
        match response {
            ServerMessage::Error { code, extra, .. } => {
                assert_eq!(code, "server_busy");
                let extra = extra.expect("server_busy should include resource accounting");
                assert_eq!(extra["scope"], scope);
                assert_eq!(extra["reason"], reason);
                extra
            }
            other => panic!("expected server_busy, got {other:?}"),
        }
    }

    #[test]
    fn management_scheduler_full_response_includes_resource_accounting() {
        let extra = assert_server_busy_extra(
            management_scheduler_full_response(3, 3),
            "management_scheduler",
            "in_flight_full",
        );

        assert_eq!(extra["in_flight"], 3);
        assert_eq!(extra["max_in_flight"], 3);
    }

    #[tokio::test]
    async fn transaction_scheduler_rejects_excess_same_tx_queue() {
        let mut scheduler = TransactionScheduler::new(1);
        scheduler.queued_by_tx.insert(42, VecDeque::new());
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        let (response_tx, mut response_rx) = mpsc::channel(1);

        let first = scheduler.dispatch_or_queue(
            &mut session,
            response_tx.clone(),
            RawFrame::json_text(br#"{"id":1,"type":"commit","tx":42}"#.to_vec()),
            ClientMessage::Commit { tx: 42 },
        );
        assert!(first.is_none());
        assert_eq!(scheduler.queued_requests, 1);

        let second = scheduler
            .dispatch_or_queue(
                &mut session,
                response_tx,
                RawFrame::json_text(br#"{"id":2,"type":"commit","tx":42}"#.to_vec()),
                ClientMessage::Commit { tx: 42 },
            )
            .unwrap();
        let extra = assert_server_busy_extra(second, "transaction_scheduler", "total_queue_full");
        assert_eq!(extra["tx"], 42);
        assert_eq!(extra["queued_requests"], 1);
        assert_eq!(extra["max_queued_requests"], 1);
        assert_eq!(extra["queued_for_tx"], 1);
        assert_eq!(extra["max_queued_per_tx"], 1);
        assert_eq!(extra["active_workers"], 1);
        assert_eq!(extra["max_active_workers"], 1);
        assert!(response_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn transaction_scheduler_caps_each_transaction_queue() {
        let mut scheduler = TransactionScheduler::new(4);
        assert_eq!(scheduler.max_queued_requests, 4);
        assert_eq!(scheduler.max_queued_per_tx, 2);
        scheduler.queued_by_tx.insert(42, VecDeque::new());
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        let (response_tx, mut response_rx) = mpsc::channel(4);

        for msg_id in 1..=2 {
            let response = scheduler.dispatch_or_queue(
                &mut session,
                response_tx.clone(),
                RawFrame::json_text(
                    format!(r#"{{"id":{msg_id},"type":"commit","tx":42}}"#).into_bytes(),
                ),
                ClientMessage::Commit { tx: 42 },
            );
            assert!(response.is_none());
        }
        assert_eq!(scheduler.queued_requests, 2);
        assert_eq!(scheduler.queued_by_tx.get(&42).unwrap().len(), 2);

        let response = scheduler
            .dispatch_or_queue(
                &mut session,
                response_tx,
                RawFrame::json_text(br#"{"id":3,"type":"commit","tx":42}"#.to_vec()),
                ClientMessage::Commit { tx: 42 },
            )
            .unwrap();
        let extra =
            assert_server_busy_extra(response, "transaction_scheduler", "transaction_queue_full");
        assert_eq!(extra["tx"], 42);
        assert_eq!(extra["queued_requests"], 2);
        assert_eq!(extra["max_queued_requests"], 4);
        assert_eq!(extra["queued_for_tx"], 2);
        assert_eq!(extra["max_queued_per_tx"], 2);
        assert_eq!(extra["active_workers"], 1);
        assert_eq!(extra["max_active_workers"], 4);
        assert_eq!(scheduler.queued_requests, 2);
        assert!(response_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn transaction_scheduler_rejects_excess_active_transaction_workers() {
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let mut session = Session::new(1, registry, AuthConfig::default());

        let ServerMessage::Ok { fields } = session
            .handle_accepted_message(ClientMessage::Begin {
                database: "app".to_string(),
                readonly: true,
                subscribe: false,
                notify: false,
            })
            .await
        else {
            panic!("expected first begin to succeed");
        };
        let tx_a = fields["tx"].as_u64().unwrap();

        let ServerMessage::Ok { fields } = session
            .handle_accepted_message(ClientMessage::Begin {
                database: "app".to_string(),
                readonly: true,
                subscribe: false,
                notify: false,
            })
            .await
        else {
            panic!("expected second begin to succeed");
        };
        let tx_b = fields["tx"].as_u64().unwrap();

        let mut scheduler = TransactionScheduler::new(1);
        let (response_tx, _response_rx) = mpsc::channel(1);
        let first = scheduler.dispatch_or_queue(
            &mut session,
            response_tx.clone(),
            RawFrame::json_text(format!(r#"{{"id":1,"type":"commit","tx":{tx_a}}}"#).into_bytes()),
            ClientMessage::Commit { tx: tx_a },
        );
        assert!(first.is_none());
        assert_eq!(scheduler.queued_by_tx.len(), 1);

        let second = scheduler
            .dispatch_or_queue(
                &mut session,
                response_tx,
                RawFrame::json_text(
                    format!(r#"{{"id":2,"type":"commit","tx":{tx_b}}}"#).into_bytes(),
                ),
                ClientMessage::Commit { tx: tx_b },
            )
            .unwrap();
        let extra =
            assert_server_busy_extra(second, "transaction_scheduler", "active_workers_full");
        assert_eq!(extra["tx"], tx_b);
        assert_eq!(extra["queued_requests"], 0);
        assert_eq!(extra["max_queued_requests"], 1);
        assert!(extra["queued_for_tx"].is_null());
        assert_eq!(extra["max_queued_per_tx"], 1);
        assert_eq!(extra["active_workers"], 1);
        assert_eq!(extra["max_active_workers"], 1);
    }

    #[tokio::test]
    async fn transaction_scheduler_delivers_terminal_unknown_tx_responses_under_backpressure() {
        let mut scheduler = TransactionScheduler::new(4);
        scheduler.queued_by_tx.insert(
            42,
            VecDeque::from([
                (
                    RawFrame::json_text(br#"{"id":1,"type":"commit","tx":42}"#.to_vec()),
                    ClientMessage::Commit { tx: 42 },
                ),
                (
                    RawFrame::json_text(br#"{"id":2,"type":"rollback","tx":42}"#.to_vec()),
                    ClientMessage::Rollback { tx: 42 },
                ),
            ]),
        );
        scheduler.queued_requests = 2;
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        let (response_tx, mut response_rx) = mpsc::channel(1);
        response_tx
            .send(PendingResponse::Plain {
                request: RawFrame::json_text(br#"{"id":99,"type":"ping"}"#.to_vec()),
                response: ServerMessage::Pong,
            })
            .await
            .unwrap();

        scheduler.schedule_next(&mut session, response_tx, 42);
        assert_eq!(scheduler.queued_requests, 0);
        assert!(!scheduler.queued_by_tx.contains_key(&42));

        let prefilled = response_rx.recv().await.unwrap();
        match prefilled {
            PendingResponse::Plain { request, response } => {
                assert_eq!(json_text_msg_id(&request), 99);
                assert!(matches!(response, ServerMessage::Pong));
            }
            _ => panic!("expected prefilled plain response"),
        }

        for expected_id in [1, 2] {
            let pending = tokio::time::timeout(Duration::from_secs(1), response_rx.recv())
                .await
                .expect("expected queued unknown-transaction response")
                .unwrap();
            match pending {
                PendingResponse::Plain { request, response } => {
                    assert_eq!(json_text_msg_id(&request), expected_id);
                    match response {
                        ServerMessage::Error { code, .. } => {
                            assert_eq!(code, "unknown_transaction")
                        }
                        other => panic!("expected unknown_transaction error, got {other:?}"),
                    }
                }
                _ => panic!("expected plain terminal response"),
            }
        }
    }

    #[tokio::test]
    async fn connection_write_timeout_closes_slow_client() {
        let (_client, server_stream) = duplex(1);
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        let err = handle_connection_with_session_and_queue(
            server_stream,
            &mut session,
            DEFAULT_MAX_MESSAGE_SIZE,
            DEFAULT_REQUEST_QUEUE_CAPACITY,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, WireError::WriteTimeout { timeout_ms: 10 }));
    }

    #[tokio::test]
    async fn connection_rejects_excess_in_flight_management_requests() {
        let (mut client, server_stream) = duplex(65_536);
        let promoter = Arc::new(BlockingPromoter::default());
        let transaction_promoter: Arc<dyn TransactionPromoter> = promoter.clone();
        let server = Arc::new(Server::new(
            ServerConfig {
                node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
                transaction_promoter: Some(transaction_promoter),
                request_queue_capacity: 1,
                ..Default::default()
            },
            registry().await,
        ));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        promoter.wait_started(1).await;

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"logs"}),
        )
        .await;
        let busy = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut client))
            .await
            .expect("expected management backpressure response");
        assert_eq!(busy["id"], 2);
        assert_eq!(busy["type"], "error");
        assert_eq!(busy["code"], "server_busy");
        assert_eq!(busy["scope"], "management_scheduler");
        assert_eq!(busy["reason"], "in_flight_full");
        assert_eq!(busy["in_flight"], 1);
        assert_eq!(busy["max_in_flight"], 1);
        assert_eq!(promoter.started.load(Ordering::Acquire), 1);

        promoter.release_all();
        let promoted = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut client))
            .await
            .expect("expected original management response");
        assert_eq!(promoted["id"], 1);
        assert_eq!(promoted["type"], "ok");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_pipelined_same_transaction_messages() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let tx = recv_json(&mut client).await["tx"].as_u64().unwrap();

        for msg in [
            json!({"id":4,"type":"insert","tx":tx,"collection":"users","body":{"name":"Ada"}}),
            json!({"id":5,"type":"insert","tx":tx,"collection":"users","body":{"name":"Grace"}}),
            json!({"id":6,"type":"commit","tx":tx}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let insert_a = recv_json(&mut client).await;
        assert_eq!(insert_a["id"], 4);
        assert_eq!(insert_a["type"], "ok");
        assert!(insert_a["doc_id"].as_str().is_some());

        let insert_b = recv_json(&mut client).await;
        assert_eq!(insert_b["id"], 5);
        assert_eq!(insert_b["type"], "ok");
        assert!(insert_b["doc_id"].as_str().is_some());

        let commit = recv_json(&mut client).await;
        assert_eq!(commit["id"], 6);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn timed_out_transaction_is_removed_from_connection_scheduler() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({
                "id": 1,
                "type": "create_database",
                "name": "app",
                "config": {
                    "transaction": {
                        "idle_timeout": "1ms"
                    }
                }
            }),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let begin = recv_json(&mut client).await;
        assert_eq!(begin["type"], "ok");
        let tx = begin["tx"].as_u64().unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let timed_out = recv_json(&mut client).await;
        assert_eq!(timed_out["id"], 4);
        assert_eq!(timed_out["type"], "error");
        assert_eq!(timed_out["code"], "tx_timeout");

        send_json(&mut client, json!({"id":5,"type":"rollback","tx":tx})).await;
        let rollback = recv_json(&mut client).await;
        assert_eq!(rollback["id"], 5);
        assert_eq!(rollback["type"], "error");
        assert_eq!(rollback["code"], "unknown_transaction");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn read_limit_exceeded_transaction_is_removed_from_connection_scheduler() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({
                "id": 1,
                "type": "create_database",
                "name": "app",
                "config": {
                    "transaction": {
                        "max_scanned_docs": 0
                    }
                }
            }),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"query","tx":tx,"collection":"users","index":"_created_at"}),
        )
        .await;
        let limit = recv_json(&mut client).await;
        assert_eq!(limit["id"], 7);
        assert_eq!(limit["type"], "error");
        assert_eq!(limit["code"], "read_limit_exceeded");

        send_json(&mut client, json!({"id":8,"type":"rollback","tx":tx})).await;
        let rollback = recv_json(&mut client).await;
        assert_eq!(rollback["id"], 8);
        assert_eq!(rollback["type"], "error");
        assert_eq!(rollback["code"], "unknown_transaction");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn multi_client_management_backpressure_is_connection_local() {
        let (mut saturated, saturated_stream) = duplex(65_536);
        let (mut peer, peer_stream) = duplex(65_536);
        let promoter = Arc::new(BlockingPromoter::default());
        let transaction_promoter: Arc<dyn TransactionPromoter> = promoter.clone();
        let server = Arc::new(Server::new(
            ServerConfig {
                node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
                transaction_promoter: Some(transaction_promoter),
                request_queue_capacity: 1,
                ..Default::default()
            },
            registry().await,
        ));
        let saturated_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.handle_connection(saturated_stream).await })
        };
        let peer_task = tokio::spawn(async move { server.handle_connection(peer_stream).await });

        assert_eq!(recv_json(&mut saturated).await["type"], "hello");
        assert_eq!(recv_json(&mut peer).await["type"], "hello");

        send_json(
            &mut saturated,
            json!({"id":1,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        promoter.wait_started(1).await;

        send_json(&mut peer, json!({"id":1,"type":"ping"})).await;
        let peer_pong = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut peer))
            .await
            .expect("peer connection should not wait for saturated connection");
        assert_eq!(peer_pong, json!({"id":1,"type":"pong"}));

        send_json(
            &mut saturated,
            json!({"id":2,"type":"create_collection","database":"app","name":"logs"}),
        )
        .await;
        let busy = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut saturated))
            .await
            .expect("expected saturated connection backpressure response");
        assert_eq!(busy["id"], 2);
        assert_eq!(busy["type"], "error");
        assert_eq!(busy["code"], "server_busy");
        assert_eq!(busy["scope"], "management_scheduler");
        assert_eq!(busy["reason"], "in_flight_full");
        assert_eq!(busy["in_flight"], 1);
        assert_eq!(busy["max_in_flight"], 1);
        assert_eq!(promoter.started.load(Ordering::Acquire), 1);

        promoter.release_all();
        let promoted = tokio::time::timeout(Duration::from_secs(1), recv_json(&mut saturated))
            .await
            .expect("expected original saturated management response");
        assert_eq!(promoted["id"], 1);
        assert_eq!(promoted["type"], "ok");

        drop(saturated);
        drop(peer);
        saturated_task.await.unwrap().unwrap();
        peer_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_begin_and_transaction_messages_in_one_pipeline() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        for msg in [
            json!({"id":3,"type":"begin","database":"app"}),
            json!({"id":4,"type":"insert","tx":1,"collection":"users","body":{"name":"Ada"}}),
            json!({"id":5,"type":"insert","tx":1,"collection":"users","body":{"name":"Grace"}}),
            json!({"id":6,"type":"commit","tx":1}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let begin = recv_json(&mut client).await;
        assert_eq!(begin["id"], 3);
        assert_eq!(begin["type"], "ok");
        assert_eq!(begin["tx"], 1);

        let insert_a = recv_json(&mut client).await;
        assert_eq!(insert_a["id"], 4);
        assert_eq!(insert_a["type"], "ok");
        assert!(insert_a["doc_id"].as_str().is_some());

        let insert_b = recv_json(&mut client).await;
        assert_eq!(insert_b["id"], 5);
        assert_eq!(insert_b["type"], "ok");
        assert!(insert_b["doc_id"].as_str().is_some());

        let commit = recv_json(&mut client).await;
        assert_eq!(commit["id"], 6);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_pipelines_crud_query_workflow_end_to_end() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":1,"type":"ok"}));
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":2,"type":"ok"}));

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let write_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();

        for msg in [
            json!({"id":4,"type":"insert","tx":write_tx,"collection":"users","body":{"name":"Ada","score":1}}),
            json!({"id":5,"type":"insert","tx":write_tx,"collection":"users","body":{"name":"Grace","score":2}}),
            json!({"id":6,"type":"commit","tx":write_tx}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let insert_a = recv_json(&mut client).await;
        assert_eq!(insert_a["id"], 4);
        assert_eq!(insert_a["type"], "ok");
        let doc_a = insert_a["doc_id"].as_str().unwrap().to_string();

        let insert_b = recv_json(&mut client).await;
        assert_eq!(insert_b["id"], 5);
        assert_eq!(insert_b["type"], "ok");
        let doc_b = insert_b["doc_id"].as_str().unwrap().to_string();

        let commit = recv_json(&mut client).await;
        assert_eq!(commit["id"], 6);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        send_json(&mut client, json!({"id":7,"type":"begin","database":"app"})).await;
        let update_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();

        for msg in [
            json!({"id":8,"type":"replace","tx":update_tx,"collection":"users","doc_id":doc_a.clone(),"body":{"name":"Ada Lovelace","score":99}}),
            json!({"id":9,"type":"delete","tx":update_tx,"collection":"users","doc_id":doc_b.clone()}),
            json!({"id":10,"type":"commit","tx":update_tx}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        for expected_id in [8, 9] {
            let response = recv_json(&mut client).await;
            assert_eq!(response["id"], expected_id);
            assert_eq!(response["type"], "ok");
        }
        let commit = recv_json(&mut client).await;
        assert_eq!(commit["id"], 10);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        send_json(
            &mut client,
            json!({"id":11,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();

        for msg in [
            json!({"id":12,"type":"get","tx":read_tx,"collection":"users","doc_id":doc_a}),
            json!({"id":13,"type":"get","tx":read_tx,"collection":"users","doc_id":doc_b}),
            json!({"id":14,"type":"query","tx":read_tx,"collection":"users","index":"_created_at","limit":10}),
            json!({"id":15,"type":"rollback","tx":read_tx}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let get_a = recv_json(&mut client).await;
        assert_eq!(get_a["id"], 12);
        assert_eq!(get_a["type"], "ok");
        assert_eq!(get_a["query_id"], 0);
        assert_eq!(get_a["doc"]["name"], "Ada Lovelace");
        assert_eq!(get_a["doc"]["score"], 99);

        let get_b = recv_json(&mut client).await;
        assert_eq!(get_b["id"], 13);
        assert_eq!(get_b["type"], "ok");
        assert_eq!(get_b["query_id"], 1);
        assert!(get_b["doc"].is_null());

        let query = recv_json(&mut client).await;
        assert_eq!(query["id"], 14);
        assert_eq!(query["type"], "ok");
        assert_eq!(query["query_id"], 2);
        let docs = query["docs"].as_array().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Ada Lovelace");
        assert_eq!(docs[0]["score"], 99);

        let rollback = recv_json(&mut client).await;
        assert_eq!(rollback, json!({"id":15,"type":"ok"}));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_runs_management_index_and_drop_lifecycle_end_to_end() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":1,"type":"ok"}));

        send_json(&mut client, json!({"id":2,"type":"list_databases"})).await;
        let list = recv_json(&mut client).await;
        assert_eq!(list["id"], 2);
        assert_eq!(list["type"], "ok");
        assert!(
            list["databases"]
                .as_array()
                .unwrap()
                .iter()
                .any(|database| database["name"] == "app"
                    && database["usage"]["active_transactions"] == 0)
        );

        send_json(
            &mut client,
            json!({"id":3,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":3,"type":"ok"}));

        send_json(
            &mut client,
            json!({"id":4,"type":"list_collections","database":"app"}),
        )
        .await;
        let collections = recv_json(&mut client).await;
        assert_eq!(collections["id"], 4);
        assert!(
            collections["collections"]
                .as_array()
                .unwrap()
                .iter()
                .any(|collection| collection["name"] == "users")
        );

        send_json(
            &mut client,
            json!({"id":5,"type":"create_index","database":"app","collection":"users","fields":["email"],"name":"email_idx"}),
        )
        .await;
        let create_index = recv_json(&mut client).await;
        assert_eq!(create_index["id"], 5);
        assert_eq!(create_index["type"], "ok");
        let index_id = create_index["index_id"].as_u64().unwrap();

        let ready = tokio::time::timeout(Duration::from_secs(5), recv_json(&mut client))
            .await
            .expect("expected pushed index_ready notification");
        assert_eq!(ready["type"], "index_ready");
        assert!(ready.get("id").is_none());
        assert_eq!(ready["database"], "app");
        assert_eq!(ready["collection"], "users");
        assert_eq!(ready["index"], "email_idx");
        assert_eq!(ready["index_id"], index_id);

        send_json(
            &mut client,
            json!({"id":6,"type":"list_indexes","database":"app","collection":"users"}),
        )
        .await;
        let indexes = recv_json(&mut client).await;
        assert_eq!(indexes["id"], 6);
        assert!(
            indexes["indexes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|index| index["name"] == "email_idx" && index["state"] == "ready")
        );

        send_json(&mut client, json!({"id":7,"type":"begin","database":"app"})).await;
        let write_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":8,"type":"insert","tx":write_tx,"collection":"users","body":{"name":"Ada","email":"ada@example.test"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":9,"type":"commit","tx":write_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":10,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({
                "id": 11,
                "type": "query",
                "tx": read_tx,
                "collection": "users",
                "index": "email_idx",
                "range": [{"eq": ["email", "ada@example.test"]}],
                "limit": 10
            }),
        )
        .await;
        let query = recv_json(&mut client).await;
        assert_eq!(query["id"], 11);
        assert_eq!(query["type"], "ok");
        let docs = query["docs"].as_array().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["_id"], doc_id);
        assert_eq!(docs[0]["email"], "ada@example.test");
        send_json(&mut client, json!({"id":12,"type":"rollback","tx":read_tx})).await;
        assert_eq!(recv_json(&mut client).await, json!({"id":12,"type":"ok"}));

        send_json(
            &mut client,
            json!({"id":13,"type":"drop_index","database":"app","collection":"users","name":"email_idx"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":13,"type":"ok"}));
        send_json(
            &mut client,
            json!({"id":14,"type":"list_indexes","database":"app","collection":"users"}),
        )
        .await;
        let indexes = recv_json(&mut client).await;
        assert_eq!(indexes["id"], 14);
        assert!(
            !indexes["indexes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|index| index["name"] == "email_idx")
        );

        send_json(
            &mut client,
            json!({"id":15,"type":"drop_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":15,"type":"ok"}));
        send_json(
            &mut client,
            json!({"id":16,"type":"list_collections","database":"app"}),
        )
        .await;
        let collections = recv_json(&mut client).await;
        assert_eq!(collections["id"], 16);
        assert!(
            !collections["collections"]
                .as_array()
                .unwrap()
                .iter()
                .any(|collection| collection["name"] == "users")
        );

        send_json(
            &mut client,
            json!({"id":17,"type":"drop_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":17,"type":"ok"}));
        send_json(&mut client, json!({"id":18,"type":"list_databases"})).await;
        let databases = recv_json(&mut client).await;
        assert_eq!(databases["id"], 18);
        assert!(
            !databases["databases"]
                .as_array()
                .unwrap()
                .iter()
                .any(|database| database["name"] == "app")
        );

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_accepts_pipelined_different_transaction_messages() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let tx_a = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(&mut client, json!({"id":4,"type":"begin","database":"app"})).await;
        let tx_b = recv_json(&mut client).await["tx"].as_u64().unwrap();

        for msg in [
            json!({"id":5,"type":"insert","tx":tx_a,"collection":"users","body":{"name":"Ada"}}),
            json!({"id":6,"type":"insert","tx":tx_b,"collection":"users","body":{"name":"Grace"}}),
            json!({"id":7,"type":"commit","tx":tx_a}),
            json!({"id":8,"type":"commit","tx":tx_b}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let mut doc_a = None;
        let mut doc_b = None;
        let mut committed = Vec::new();
        for _ in 0..4 {
            let response = recv_json(&mut client).await;
            assert_eq!(response["type"], "ok");
            match response["id"].as_u64().unwrap() {
                5 => doc_a = response["doc_id"].as_str().map(ToString::to_string),
                6 => doc_b = response["doc_id"].as_str().map(ToString::to_string),
                7 | 8 => {
                    assert!(response["commit_ts"].as_u64().unwrap() > 0);
                    committed.push(response["id"].as_u64().unwrap());
                }
                id => panic!("unexpected response id {id}: {response:?}"),
            }
        }
        assert!(doc_a.is_some());
        assert!(doc_b.is_some());
        committed.sort_unstable();
        assert_eq!(committed, vec![7, 8]);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn disconnect_rolls_back_active_transaction() {
        let registry = registry().await;
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), Arc::clone(&registry)));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":tx,"collection":"users","body":{"name":"Transient"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            registry.database_usage("app").unwrap().active_transactions,
            1
        );

        drop(client);
        task.await.unwrap().unwrap();
        assert_eq!(
            registry.database_usage("app").unwrap().active_transactions,
            0
        );

        let (mut reader, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut reader).await["type"], "hello");
        send_json(
            &mut reader,
            json!({"id":1,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(&mut reader).await["tx"].as_u64().unwrap();
        send_json(
            &mut reader,
            json!({"id":2,"type":"get","tx":read_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        let get = recv_json(&mut reader).await;
        assert_eq!(get["type"], "ok");
        assert!(get["doc"].is_null());

        send_json(&mut reader, json!({"id":3,"type":"rollback","tx":read_tx})).await;
        assert_eq!(recv_json(&mut reader).await, json!({"id":3,"type":"ok"}));
        drop(reader);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn disconnect_removes_registered_subscriptions() {
        let registry = registry().await;
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), Arc::clone(&registry)));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["query_id"], 0);
        send_json(&mut client, json!({"id":8,"type":"commit","tx":sub_tx})).await;
        let commit = recv_json(&mut client).await;
        assert_eq!(commit["type"], "ok");
        assert!(commit["subscription_id"].as_u64().is_some());

        let db = registry.get_database_by_name("app").unwrap();
        assert_eq!(db.subscriptions().read().subscription_count(), 1);

        drop(client);
        task.await.unwrap().unwrap();
        assert_eq!(db.subscriptions().read().subscription_count(), 0);
    }

    #[tokio::test]
    async fn duplicate_message_id_produces_no_second_response() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let _hello = read_frame(&mut client).await.unwrap();
        let ping = RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec());
        write_frame(&mut client, &ping).await.unwrap();
        assert_eq!(
            json_payload(&read_frame(&mut client).await.unwrap())["type"],
            "pong"
        );

        write_frame(&mut client, &ping).await.unwrap();
        let no_response = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            read_frame(&mut client),
        )
        .await;
        assert!(no_response.is_err());

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn skipped_message_id_returns_protocol_error_and_is_reserved() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(&mut client, json!({"id":1,"type":"ping"})).await;
        assert_eq!(recv_json(&mut client).await["type"], "pong");
        send_json(&mut client, json!({"id":3,"type":"ping"})).await;
        let response = recv_json(&mut client).await;
        assert_eq!(response["id"], 3);
        assert_eq!(response["type"], "error");
        assert_eq!(response["code"], "invalid_message");
        assert!(
            response["message"]
                .as_str()
                .unwrap()
                .contains("must be the next incrementing value")
        );
        assert!(response["message"].as_str().unwrap().contains("expected 2"));

        send_json(&mut client, json!({"id":2,"type":"ping"})).await;
        let pong = recv_json(&mut client).await;
        assert_eq!(pong["id"], 2);
        assert_eq!(pong["type"], "pong");

        send_json(&mut client, json!({"id":3,"type":"ping"})).await;
        let no_response =
            tokio::time::timeout(Duration::from_millis(100), recv_json(&mut client)).await;
        assert!(no_response.is_err());

        send_json(&mut client, json!({"id":4,"type":"ping"})).await;
        let pong = recv_json(&mut client).await;
        assert_eq!(pong["id"], 4);
        assert_eq!(pong["type"], "pong");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn invalid_message_id_is_reserved_for_retry_deduplication() {
        let (mut client, server_stream) = duplex(4096);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        send_json(&mut client, json!({"id":1,"type":"not_a_real_message"})).await;

        let invalid = recv_json(&mut client).await;
        assert_eq!(invalid["id"], 1);
        assert_eq!(invalid["type"], "error");
        assert_eq!(invalid["code"], "invalid_message");

        send_json(&mut client, json!({"id":1,"type":"ping"})).await;
        let no_response =
            tokio::time::timeout(Duration::from_millis(100), recv_json(&mut client)).await;
        assert!(no_response.is_err());

        send_json(&mut client, json!({"id":2,"type":"ping"})).await;
        let pong = recv_json(&mut client).await;
        assert_eq!(pong["id"], 2);
        assert_eq!(pong["type"], "pong");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_pushes_subscription_invalidation() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let hello = recv_json(&mut client).await;
        assert_eq!(hello["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true,"notify":true}),
        )
        .await;
        let sub_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        let get = recv_json(&mut client).await;
        assert_eq!(get["query_id"], 0);
        assert_eq!(get["doc"]["name"], "Ada");
        send_json(&mut client, json!({"id":8,"type":"commit","tx":sub_tx})).await;
        let subscribe_commit = recv_json(&mut client).await;
        assert_eq!(subscribe_commit["type"], "ok");
        assert!(subscribe_commit["subscription_id"].as_u64().is_some());

        send_json(&mut client, json!({"id":9,"type":"begin","database":"app"})).await;
        let writer_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":10,"type":"replace","tx":writer_tx,"collection":"users","doc_id":doc_id,"body":{"name":"Ada Lovelace"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(&mut client, json!({"id":11,"type":"commit","tx":writer_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        let invalidation =
            tokio::time::timeout(std::time::Duration::from_secs(1), recv_json(&mut client))
                .await
                .expect("expected pushed invalidation");
        assert_eq!(invalidation["type"], "invalidation");
        assert!(invalidation.get("id").is_none());
        assert_eq!(invalidation["tx"], sub_tx);
        assert_eq!(invalidation["queries"], json!([0]));
        assert!(invalidation["commit_ts"].as_u64().unwrap() > 0);
        assert!(invalidation.get("new_tx").is_none());
        assert!(invalidation.get("new_ts").is_none());

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_pushes_index_ready_notification() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":3,"type":"create_index","database":"app","collection":"users","fields":["email"],"name":"email_idx"}),
        )
        .await;
        let create = recv_json(&mut client).await;
        assert_eq!(create["type"], "ok");
        let index_id = create["index_id"].as_u64().unwrap();

        let ready = tokio::time::timeout(std::time::Duration::from_secs(5), recv_json(&mut client))
            .await
            .expect("expected pushed index_ready notification");
        assert_eq!(ready["type"], "index_ready");
        assert!(ready.get("id").is_none());
        assert_eq!(ready["database"], "app");
        assert_eq!(ready["collection"], "users");
        assert_eq!(ready["index"], "email_idx");
        assert_eq!(ready["index_id"], index_id);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_materializes_subscribe_continuation_tx() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","readonly":true,"subscribe":true}),
        )
        .await;
        let sub_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":sub_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["query_id"], 0);
        send_json(&mut client, json!({"id":8,"type":"commit","tx":sub_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":9,"type":"begin","database":"app"})).await;
        let writer_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":10,"type":"replace","tx":writer_tx,"collection":"users","doc_id":doc_id,"body":{"name":"Ada Lovelace"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(&mut client, json!({"id":11,"type":"commit","tx":writer_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        let invalidation =
            tokio::time::timeout(std::time::Duration::from_secs(1), recv_json(&mut client))
                .await
                .expect("expected pushed invalidation");
        assert_eq!(invalidation["type"], "invalidation");
        assert_eq!(invalidation["tx"], sub_tx);
        assert_eq!(invalidation["queries"], json!([0]));
        let new_tx = invalidation["new_tx"].as_u64().unwrap();
        assert_eq!(invalidation["new_ts"], invalidation["commit_ts"]);

        send_json(
            &mut client,
            json!({"id":12,"type":"get","tx":new_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        let continuation_get = recv_json(&mut client).await;
        assert_eq!(continuation_get["type"], "ok");
        assert_eq!(continuation_get["query_id"], 0);
        assert_eq!(continuation_get["doc"]["name"], "Ada Lovelace");

        send_json(&mut client, json!({"id":13,"type":"commit","tx":new_tx})).await;
        let continuation_commit = recv_json(&mut client).await;
        assert_eq!(continuation_commit["type"], "ok");
        assert!(continuation_commit["subscription_id"].as_u64().is_some());

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn connection_materializes_subscribe_conflict_retry_tx() {
        let (mut client, server_stream) = duplex(65_536);
        let server = Arc::new(Server::new(ServerConfig::default(), registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        send_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":3,"type":"begin","database":"app"})).await;
        let seed_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":4,"type":"insert","tx":seed_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        let doc_id = recv_json(&mut client).await["doc_id"]
            .as_str()
            .unwrap()
            .to_string();
        send_json(&mut client, json!({"id":5,"type":"commit","tx":seed_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(
            &mut client,
            json!({"id":6,"type":"begin","database":"app","subscribe":true}),
        )
        .await;
        let stale_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":7,"type":"get","tx":stale_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["doc"]["name"], "Ada");
        send_json(
            &mut client,
            json!({"id":8,"type":"replace","tx":stale_tx,"collection":"users","doc_id":doc_id,"body":{"name":"Stale Ada"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":9,"type":"begin","database":"app"})).await;
        let writer_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":10,"type":"replace","tx":writer_tx,"collection":"users","doc_id":doc_id,"body":{"name":"Fresh Ada"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        send_json(&mut client, json!({"id":11,"type":"commit","tx":writer_tx})).await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        send_json(&mut client, json!({"id":12,"type":"commit","tx":stale_tx})).await;
        let conflict = recv_json(&mut client).await;
        assert_eq!(conflict["type"], "error");
        assert_eq!(conflict["code"], "conflict");
        let retry_tx = conflict["new_tx"].as_u64().unwrap();
        assert!(conflict["new_ts"].as_u64().unwrap() > 0);

        send_json(
            &mut client,
            json!({"id":13,"type":"get","tx":retry_tx,"collection":"users","doc_id":doc_id}),
        )
        .await;
        let retry_get = recv_json(&mut client).await;
        assert_eq!(retry_get["type"], "ok");
        assert_eq!(retry_get["query_id"], 0);
        assert_eq!(retry_get["doc"]["name"], "Fresh Ada");

        send_json(
            &mut client,
            json!({"id":14,"type":"rollback","tx":retry_tx}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn auth_required_error_flows_through_transport() {
        let (mut client, server_stream) = duplex(4096);
        let config = ServerConfig {
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["auth_required"], true);

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&response)["code"], "auth_required");

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn failed_authentication_rejects_queued_pipeline_until_reauthenticated() {
        let (mut client, server_stream) = duplex(4096);
        let secret = b"secret";
        let config = ServerConfig {
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");

        for msg in [
            json!({"id":1,"type":"authenticate","token":"not-a-jwt"}),
            json!({"id":2,"type":"create_database","name":"app"}),
            json!({"id":3,"type":"ping"}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        let auth_failed = recv_json(&mut client).await;
        assert_eq!(auth_failed["id"], 1);
        assert_eq!(auth_failed["type"], "error");
        assert_eq!(auth_failed["code"], "auth_failed");

        let create_rejected = recv_json(&mut client).await;
        assert_eq!(create_rejected["id"], 2);
        assert_eq!(create_rejected["type"], "error");
        assert_eq!(create_rejected["code"], "auth_required");

        let ping_rejected = recv_json(&mut client).await;
        assert_eq!(ping_rejected["id"], 3);
        assert_eq!(ping_rejected["type"], "error");
        assert_eq!(ping_rejected["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        send_json(
            &mut client,
            json!({"id":4,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":4,"type":"ok"}));

        send_json(
            &mut client,
            json!({"id":5,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":5,"type":"ok"}));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn successful_authentication_releases_queued_pipeline_in_order() {
        let (mut client, server_stream) = duplex(65_536);
        let secret = b"secret";
        let config = ServerConfig {
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let db = registry.get_database_by_name("app").unwrap();
        let mut setup = db.begin(Default::default()).unwrap();
        setup.create_collection("users").await.unwrap();
        assert!(matches!(
            setup.commit().await.unwrap(),
            exdb::TransactionResult::Success { .. }
        ));

        let server = Arc::new(Server::new(config, registry));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        let token = hmac_token(secret, get_current_timestamp() + 60);
        for msg in [
            json!({"id":1,"type":"authenticate","token":token}),
            json!({"id":2,"type":"begin","database":"app"}),
            json!({"id":3,"type":"insert","tx":1,"collection":"users","body":{"name":"Ada"}}),
            json!({"id":4,"type":"commit","tx":1}),
        ] {
            write_frame(
                &mut client,
                &RawFrame::json_text(serde_json::to_vec(&msg).unwrap()),
            )
            .await
            .unwrap();
        }

        for id in 1..=2 {
            let response = recv_json(&mut client).await;
            assert_eq!(response["id"], id);
            assert_eq!(response["type"], "ok");
        }

        let insert = recv_json(&mut client).await;
        assert_eq!(insert["id"], 3);
        assert_eq!(insert["type"], "ok");
        assert!(insert["doc_id"].as_str().is_some());

        let commit = recv_json(&mut client).await;
        assert_eq!(commit["id"], 4);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn idle_authenticated_transport_closes_when_token_expires() {
        let (mut client, server_stream) = duplex(4096);
        let secret = b"secret";
        let config = ServerConfig {
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let task = tokio::spawn(async move { server.handle_connection(server_stream).await });

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        let token = hmac_token(secret, get_current_timestamp() + 1);
        send_json(
            &mut client,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");

        let expired = tokio::time::timeout(Duration::from_secs(5), recv_json(&mut client))
            .await
            .expect("expected server-initiated auth_expired notification");
        assert_eq!(expired["type"], "error");
        assert_eq!(expired["code"], "auth_expired");
        assert!(expired.get("id").is_none());

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn auth_expiry_rolls_back_active_transaction_before_close() {
        let registry = registry().await;
        let secret = b"secret";
        let server = Arc::new(Server::new(
            ServerConfig {
                auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
                ..Default::default()
            },
            Arc::clone(&registry),
        ));
        let (mut client, server_stream) = duplex(65_536);
        let task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.handle_connection(server_stream).await })
        };

        assert_eq!(recv_json(&mut client).await["type"], "hello");
        let token = hmac_token(secret, get_current_timestamp() + 2);
        send_json(
            &mut client,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":1,"type":"ok"}));

        send_json(
            &mut client,
            json!({"id":2,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":2,"type":"ok"}));
        send_json(
            &mut client,
            json!({"id":3,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await, json!({"id":3,"type":"ok"}));

        send_json(&mut client, json!({"id":4,"type":"begin","database":"app"})).await;
        let write_tx = recv_json(&mut client).await["tx"].as_u64().unwrap();
        send_json(
            &mut client,
            json!({"id":5,"type":"insert","tx":write_tx,"collection":"users","body":{"name":"Ada"}}),
        )
        .await;
        assert_eq!(recv_json(&mut client).await["type"], "ok");
        assert_eq!(
            registry.database_usage("app").unwrap().active_transactions,
            1
        );

        let expired = tokio::time::timeout(Duration::from_secs(5), recv_json(&mut client))
            .await
            .expect("expected server-initiated auth_expired notification");
        assert_eq!(expired["type"], "error");
        assert_eq!(expired["code"], "auth_expired");
        assert!(expired.get("id").is_none());
        task.await.unwrap().unwrap();
        assert_eq!(
            registry.database_usage("app").unwrap().active_transactions,
            0
        );

        let (mut reader, reader_stream) = duplex(65_536);
        let reader_task =
            tokio::spawn(async move { server.handle_connection(reader_stream).await });
        assert_eq!(recv_json(&mut reader).await["type"], "hello");
        let token = hmac_token(secret, get_current_timestamp() + 60);
        send_json(
            &mut reader,
            json!({"id":1,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_json(&mut reader).await, json!({"id":1,"type":"ok"}));
        send_json(
            &mut reader,
            json!({"id":2,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let read_tx = recv_json(&mut reader).await["tx"].as_u64().unwrap();
        send_json(
            &mut reader,
            json!({"id":3,"type":"query","tx":read_tx,"collection":"users","index":"_created_at","limit":10}),
        )
        .await;
        let query = recv_json(&mut reader).await;
        assert_eq!(query["type"], "ok");
        assert!(query["docs"].as_array().unwrap().is_empty());
        send_json(&mut reader, json!({"id":4,"type":"rollback","tx":read_tx})).await;
        assert_eq!(recv_json(&mut reader).await, json!({"id":4,"type":"ok"}));

        drop(reader);
        reader_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tcp_listener_accepts_connection_and_shutdown() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some(addr),
                ..Default::default()
            },
            max_message_size: 4096,
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_with_retry(addr).await;
        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["max_message_size"], 4096);

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tcp_listener_enforces_auth_before_replica_read_gate() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_with_retry(addr).await;
        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        write_frame(
            &mut client,
            &RawFrame::json_text(
                br#"{"id":1,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let rejected = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&rejected)["id"], 1);
        assert_eq!(json_payload(&rejected)["type"], "error");
        assert_eq!(json_payload(&rejected)["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        write_frame(
            &mut client,
            &RawFrame::json_text(
                serde_json::to_vec(&json!({
                    "id": 2,
                    "type": "authenticate",
                    "token": token
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let authenticated = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&authenticated), json!({"id":2,"type":"ok"}));

        write_frame(
            &mut client,
            &RawFrame::json_text(
                br#"{"id":3,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let fenced = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&fenced)["id"], 3);
        assert_eq!(json_payload(&fenced)["type"], "error");
        assert_eq!(json_payload(&fenced)["code"], "quorum_lost");

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":4,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let pong = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&pong), json!({"id":4,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tcp_listener_auth_pipeline_releases_replica_read_gate_check() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_with_retry(addr).await;
        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        assert_auth_pipeline_reaches_replica_read_gate(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tcp_listener_accepts_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_with_retry(addr).await;
        run_mixed_json_bson_and_protobuf_lifecycle(&mut client).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tcp_listener_accepts_authenticated_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_with_retry(addr).await;
        let token = hmac_token(secret, get_current_timestamp() + 60);
        run_authenticated_mixed_json_bson_and_protobuf_lifecycle(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_accepts_connection_and_handles_json_ping() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut client = connector.connect(server_name, tcp).await.unwrap();

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_accepts_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut client = connector.connect(server_name, tcp).await.unwrap();
        run_mixed_json_bson_and_protobuf_lifecycle(&mut client).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_accepts_authenticated_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut client = connector.connect(server_name, tcp).await.unwrap();
        let token = hmac_token(secret, get_current_timestamp() + 60);
        run_authenticated_mixed_json_bson_and_protobuf_lifecycle(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_enforces_auth_before_replica_read_gate() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut client = connector.connect(server_name, tcp).await.unwrap();

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        write_frame(
            &mut client,
            &RawFrame::json_text(
                br#"{"id":1,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let rejected = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&rejected)["id"], 1);
        assert_eq!(json_payload(&rejected)["type"], "error");
        assert_eq!(json_payload(&rejected)["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        write_frame(
            &mut client,
            &RawFrame::json_text(
                serde_json::to_vec(&json!({
                    "id": 2,
                    "type": "authenticate",
                    "token": token
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let authenticated = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&authenticated), json!({"id":2,"type":"ok"}));

        write_frame(
            &mut client,
            &RawFrame::json_text(
                br#"{"id":3,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let fenced = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&fenced)["id"], 3);
        assert_eq!(json_payload(&fenced)["type"], "error");
        assert_eq!(json_payload(&fenced)["code"], "quorum_lost");

        write_frame(
            &mut client,
            &RawFrame::json_text(br#"{"id":4,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let pong = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&pong), json!({"id":4,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tls_listener_auth_pipeline_releases_replica_read_gate_check() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut client = connector.connect(server_name, tcp).await.unwrap();

        let hello = read_frame(&mut client).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        assert_auth_pipeline_reaches_replica_read_gate(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_text_messages_without_newline() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        let hello = client.next().await.unwrap().unwrap();
        let Message::Text(hello) = hello else {
            panic!("expected text hello");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hello).unwrap()["type"],
            "hello"
        );

        client
            .send(Message::Text(r#"{"id":1,"type":"ping"}"#.into()))
            .await
            .unwrap();
        let response = client.next().await.unwrap().unwrap();
        let Message::Text(response) = response else {
            panic!("expected text response");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
            json!({"id":1,"type":"pong"})
        );

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_enforces_auth_before_replica_read_gate() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        let hello = recv_ws_json(&mut client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);
        assert_eq!(hello["node_role"], "replica");

        send_ws_json(
            &mut client,
            json!({"id":1,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let rejected = recv_ws_json(&mut client).await;
        assert_eq!(rejected["id"], 1);
        assert_eq!(rejected["type"], "error");
        assert_eq!(rejected["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        send_ws_json(
            &mut client,
            json!({"id":2,"type":"authenticate","token":token}),
        )
        .await;
        assert_eq!(recv_ws_json(&mut client).await, json!({"id":2,"type":"ok"}));

        send_ws_json(
            &mut client,
            json!({"id":3,"type":"begin","database":"app","readonly":true}),
        )
        .await;
        let fenced = recv_ws_json(&mut client).await;
        assert_eq!(fenced["id"], 3);
        assert_eq!(fenced["type"], "error");
        assert_eq!(fenced["code"], "quorum_lost");

        send_ws_json(&mut client, json!({"id":4,"type":"ping"})).await;
        assert_eq!(
            recv_ws_json(&mut client).await,
            json!({"id":4,"type":"pong"})
        );

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_auth_pipeline_releases_replica_read_gate_check() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        let hello = recv_ws_json(&mut client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);
        assert_eq!(hello["node_role"], "replica");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        assert_auth_websocket_pipeline_reaches_replica_read_gate(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_pipelined_crud_query_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        assert_eq!(recv_ws_json(&mut client).await["type"], "hello");

        send_ws_json(
            &mut client,
            json!({"id":1,"type":"create_database","name":"app"}),
        )
        .await;
        assert_eq!(recv_ws_json(&mut client).await, json!({"id":1,"type":"ok"}));

        send_ws_json(
            &mut client,
            json!({"id":2,"type":"create_collection","database":"app","name":"users"}),
        )
        .await;
        assert_eq!(recv_ws_json(&mut client).await, json!({"id":2,"type":"ok"}));

        for msg in [
            json!({"id":3,"type":"begin","database":"app"}),
            json!({"id":4,"type":"insert","tx":1,"collection":"users","body":{"name":"Ada","score":1}}),
            json!({"id":5,"type":"insert","tx":1,"collection":"users","body":{"name":"Grace","score":2}}),
            json!({"id":6,"type":"commit","tx":1}),
        ] {
            send_ws_json(&mut client, msg).await;
        }

        let begin = recv_ws_json(&mut client).await;
        assert_eq!(begin["id"], 3);
        assert_eq!(begin["type"], "ok");
        assert_eq!(begin["tx"], 1);

        let insert_a = recv_ws_json(&mut client).await;
        assert_eq!(insert_a["id"], 4);
        assert_eq!(insert_a["type"], "ok");
        let doc_a = insert_a["doc_id"].as_str().unwrap().to_string();

        let insert_b = recv_ws_json(&mut client).await;
        assert_eq!(insert_b["id"], 5);
        assert_eq!(insert_b["type"], "ok");
        let doc_b = insert_b["doc_id"].as_str().unwrap().to_string();

        let commit = recv_ws_json(&mut client).await;
        assert_eq!(commit["id"], 6);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        for msg in [
            json!({"id":7,"type":"begin","database":"app","readonly":true}),
            json!({"id":8,"type":"get","tx":2,"collection":"users","doc_id":doc_a}),
            json!({"id":9,"type":"query","tx":2,"collection":"users","index":"_created_at","limit":10}),
            json!({"id":10,"type":"rollback","tx":2}),
        ] {
            send_ws_json(&mut client, msg).await;
        }

        let read_begin = recv_ws_json(&mut client).await;
        assert_eq!(read_begin["id"], 7);
        assert_eq!(read_begin["type"], "ok");
        assert_eq!(read_begin["tx"], 2);

        let get = recv_ws_json(&mut client).await;
        assert_eq!(get["id"], 8);
        assert_eq!(get["type"], "ok");
        assert_eq!(get["query_id"], 0);
        assert_eq!(get["doc"]["name"], "Ada");
        assert_eq!(get["doc"]["score"], 1);

        let query = recv_ws_json(&mut client).await;
        assert_eq!(query["id"], 9);
        assert_eq!(query["type"], "ok");
        assert_eq!(query["query_id"], 1);
        let names: Vec<_> = query["docs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|doc| doc["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"Ada"));
        assert!(names.contains(&"Grace"));

        let rollback = recv_ws_json(&mut client).await;
        assert_eq!(rollback, json!({"id":10,"type":"ok"}));
        assert_ne!(doc_a, doc_b);

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_binary_frames() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        assert!(matches!(
            client.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));

        let request = RawFrame::binary(1, MSG_PING, Encoding::Json, b"{}".to_vec());
        client
            .send(Message::Binary(
                encode_binary_frame_with_limit(&request, DEFAULT_MAX_MESSAGE_SIZE)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let Message::Binary(response) = response else {
            panic!("expected binary response");
        };
        let frame = decode_binary_frame_with_limit(&response, DEFAULT_MAX_MESSAGE_SIZE).unwrap();
        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, 1);
        assert_eq!(frame.msg_type, MSG_PONG);
        assert_eq!(frame.encoding, Encoding::Json);
        assert_eq!(json_payload(&frame), json!({"id":1,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_binary_crud_query_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        assert!(matches!(
            client.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));

        send_ws_binary_json(&mut client, 1, MSG_CREATE_DATABASE, json!({"name": "app"})).await;
        assert_eq!(
            recv_ws_binary_json(&mut client, 1, MSG_OK).await,
            json!({"id": 1, "type": "ok"})
        );

        send_ws_binary_json(
            &mut client,
            2,
            MSG_CREATE_COLLECTION,
            json!({"database": "app", "name": "users"}),
        )
        .await;
        assert_eq!(
            recv_ws_binary_json(&mut client, 2, MSG_OK).await,
            json!({"id": 2, "type": "ok"})
        );

        send_ws_binary_json(&mut client, 3, MSG_BEGIN, json!({"database": "app"})).await;
        let begin = recv_ws_binary_json(&mut client, 3, MSG_OK).await;
        assert_eq!(begin["id"], 3);
        assert_eq!(begin["type"], "ok");
        let tx = begin["tx"].as_u64().unwrap();
        assert_eq!(tx, 1);

        send_ws_binary_json(
            &mut client,
            4,
            MSG_INSERT,
            json!({
                "tx": tx,
                "collection": "users",
                "body": {"name": "Ada", "score": 7}
            }),
        )
        .await;
        let insert = recv_ws_binary_json(&mut client, 4, MSG_OK).await;
        assert_eq!(insert["id"], 4);
        assert_eq!(insert["type"], "ok");
        let doc_id = insert["doc_id"].as_str().unwrap().to_string();

        send_ws_binary_json(&mut client, 5, MSG_COMMIT, json!({"tx": tx})).await;
        let commit = recv_ws_binary_json(&mut client, 5, MSG_OK).await;
        assert_eq!(commit["id"], 5);
        assert_eq!(commit["type"], "ok");
        assert!(commit["commit_ts"].as_u64().unwrap() > 0);

        send_ws_binary_json(
            &mut client,
            6,
            MSG_BEGIN,
            json!({"database": "app", "readonly": true}),
        )
        .await;
        let read_begin = recv_ws_binary_json(&mut client, 6, MSG_OK).await;
        assert_eq!(read_begin["id"], 6);
        assert_eq!(read_begin["type"], "ok");
        let read_tx = read_begin["tx"].as_u64().unwrap();
        assert_eq!(read_tx, 2);

        send_ws_binary_json(
            &mut client,
            7,
            MSG_GET,
            json!({
                "tx": read_tx,
                "collection": "users",
                "doc_id": doc_id
            }),
        )
        .await;
        let get = recv_ws_binary_json(&mut client, 7, MSG_OK).await;
        assert_eq!(get["id"], 7);
        assert_eq!(get["type"], "ok");
        assert_eq!(get["query_id"], 0);
        assert_eq!(get["doc"]["name"], "Ada");
        assert_eq!(get["doc"]["score"], 7);

        send_ws_binary_json(
            &mut client,
            8,
            MSG_QUERY,
            json!({
                "tx": read_tx,
                "collection": "users",
                "index": "_created_at",
                "limit": 10
            }),
        )
        .await;
        let query = recv_ws_binary_json(&mut client, 8, MSG_OK).await;
        assert_eq!(query["id"], 8);
        assert_eq!(query["type"], "ok");
        assert_eq!(query["query_id"], 1);
        assert_eq!(query["docs"].as_array().unwrap().len(), 1);
        assert_eq!(query["docs"][0]["name"], "Ada");

        send_ws_binary_json(&mut client, 9, MSG_ROLLBACK, json!({"tx": read_tx})).await;
        assert_eq!(
            recv_ws_binary_json(&mut client, 9, MSG_OK).await,
            json!({"id": 9, "type": "ok"})
        );

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        run_websocket_mixed_json_bson_and_protobuf_lifecycle(&mut client).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn websocket_listener_accepts_authenticated_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket: Some(addr),
                ..Default::default()
            },
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let mut client = connect_websocket_with_retry(addr).await;
        let token = hmac_token(secret, get_current_timestamp() + 60);
        run_authenticated_websocket_mixed_json_bson_and_protobuf_lifecycle(&mut client, token)
            .await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_accepts_text_messages() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        let hello = client.next().await.unwrap().unwrap();
        let Message::Text(hello) = hello else {
            panic!("expected text hello");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hello).unwrap()["type"],
            "hello"
        );

        client
            .send(Message::Text(r#"{"id":1,"type":"ping"}"#.into()))
            .await
            .unwrap();
        let response = client.next().await.unwrap().unwrap();
        let Message::Text(response) = response else {
            panic!("expected text response");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
            json!({"id":1,"type":"pong"})
        );

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_enforces_auth_before_replica_read_gate() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        let hello = client.next().await.unwrap().unwrap();
        let Message::Text(hello) = hello else {
            panic!("expected text hello");
        };
        let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);
        assert_eq!(hello["node_role"], "replica");

        client
            .send(Message::Text(
                r#"{"id":1,"type":"begin","database":"app","readonly":true}"#.into(),
            ))
            .await
            .unwrap();
        let rejected = client.next().await.unwrap().unwrap();
        let Message::Text(rejected) = rejected else {
            panic!("expected auth_required response");
        };
        let rejected: serde_json::Value = serde_json::from_str(&rejected).unwrap();
        assert_eq!(rejected["id"], 1);
        assert_eq!(rejected["type"], "error");
        assert_eq!(rejected["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        client
            .send(Message::Text(
                serde_json::to_string(&json!({
                    "id": 2,
                    "type": "authenticate",
                    "token": token
                }))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();
        let authenticated = client.next().await.unwrap().unwrap();
        let Message::Text(authenticated) = authenticated else {
            panic!("expected auth ok response");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&authenticated).unwrap(),
            json!({"id":2,"type":"ok"})
        );

        client
            .send(Message::Text(
                r#"{"id":3,"type":"begin","database":"app","readonly":true}"#.into(),
            ))
            .await
            .unwrap();
        let fenced = client.next().await.unwrap().unwrap();
        let Message::Text(fenced) = fenced else {
            panic!("expected quorum_lost response");
        };
        let fenced: serde_json::Value = serde_json::from_str(&fenced).unwrap();
        assert_eq!(fenced["id"], 3);
        assert_eq!(fenced["type"], "error");
        assert_eq!(fenced["code"], "quorum_lost");

        client
            .send(Message::Text(r#"{"id":4,"type":"ping"}"#.into()))
            .await
            .unwrap();
        let pong = client.next().await.unwrap().unwrap();
        let Message::Text(pong) = pong else {
            panic!("expected pong response");
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&pong).unwrap(),
            json!({"id":4,"type":"pong"})
        );

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_auth_pipeline_releases_replica_read_gate_check() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        let hello = recv_ws_json(&mut client).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["auth_required"], true);
        assert_eq!(hello["node_role"], "replica");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        assert_auth_websocket_pipeline_reaches_replica_read_gate(&mut client, token).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_accepts_binary_frames() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        assert!(matches!(
            client.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));

        let request = RawFrame::binary(1, MSG_PING, Encoding::Json, b"{}".to_vec());
        client
            .send(Message::Binary(
                encode_binary_frame_with_limit(&request, DEFAULT_MAX_MESSAGE_SIZE)
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap();
        let Message::Binary(response) = response else {
            panic!("expected binary response");
        };
        let frame = decode_binary_frame_with_limit(&response, DEFAULT_MAX_MESSAGE_SIZE).unwrap();
        assert_eq!(frame.frame_type, FrameType::Binary);
        assert_eq!(frame.msg_id, 1);
        assert_eq!(frame.msg_type, MSG_PONG);
        assert_eq!(frame.encoding, Encoding::Json);
        assert_eq!(json_payload(&frame), json!({"id":1,"type":"pong"}));

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_accepts_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        run_websocket_mixed_json_bson_and_protobuf_lifecycle(&mut client).await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn secure_websocket_listener_accepts_authenticated_mixed_json_bson_and_protobuf_lifecycle()
     {
        let probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                websocket_tls: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };

        let tcp = connect_with_retry(addr).await;
        let connector = TlsConnector::from(Arc::new(tls_client_config()));
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let (mut client, _) = client_async(format!("wss://localhost:{}/", addr.port()), tls)
            .await
            .unwrap();

        let token = hmac_token(secret, get_current_timestamp() + 60);
        run_authenticated_websocket_mixed_json_bson_and_protobuf_lifecycle(&mut client, token)
            .await;

        shutdown.cancel();
        drop(client);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_listener_opens_initial_stream_and_accepts_client_streams() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                quic: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server_task.is_finished(),
            "QUIC server task exited before client connection"
        );

        let mut client_endpoint = QuicEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quic_client_config());
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);

        let hello = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");

        write_frame(
            &mut stream,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let response = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        let (send, recv) = connection.open_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);
        write_frame(
            &mut stream,
            &RawFrame::json_text(br#"{"id":1,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();

        let hello = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        let response = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&response), json!({"id":1,"type":"pong"}));

        shutdown.cancel();
        connection.close(0u32.into(), b"done");
        client_endpoint.close(0u32.into(), b"done");
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_listener_accepts_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                quic: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server_task.is_finished(),
            "QUIC server task exited before client connection"
        );

        let mut client_endpoint = QuicEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quic_client_config());
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);
        run_mixed_json_bson_and_protobuf_lifecycle(&mut stream).await;

        shutdown.cancel();
        connection.close(0u32.into(), b"done");
        client_endpoint.close(0u32.into(), b"done");
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_listener_accepts_authenticated_mixed_json_bson_and_protobuf_lifecycle() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                quic: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry().await));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server_task.is_finished(),
            "QUIC server task exited before client connection"
        );

        let mut client_endpoint = QuicEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quic_client_config());
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);
        let token = hmac_token(secret, get_current_timestamp() + 60);
        run_authenticated_mixed_json_bson_and_protobuf_lifecycle(&mut stream, token).await;

        shutdown.cancel();
        connection.close(0u32.into(), b"done");
        client_endpoint.close(0u32.into(), b"done");
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_listener_enforces_auth_before_replica_read_gate() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                quic: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server_task.is_finished(),
            "QUIC server task exited before client connection"
        );

        let mut client_endpoint = QuicEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quic_client_config());
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);

        let hello = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        write_frame(
            &mut stream,
            &RawFrame::json_text(
                br#"{"id":1,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let rejected = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&rejected)["id"], 1);
        assert_eq!(json_payload(&rejected)["type"], "error");
        assert_eq!(json_payload(&rejected)["code"], "auth_required");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        write_frame(
            &mut stream,
            &RawFrame::json_text(
                serde_json::to_vec(&json!({
                    "id": 2,
                    "type": "authenticate",
                    "token": token
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let authenticated = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&authenticated), json!({"id":2,"type":"ok"}));

        write_frame(
            &mut stream,
            &RawFrame::json_text(
                br#"{"id":3,"type":"begin","database":"app","readonly":true}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
        let fenced = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&fenced)["id"], 3);
        assert_eq!(json_payload(&fenced)["type"], "error");
        assert_eq!(json_payload(&fenced)["code"], "quorum_lost");

        write_frame(
            &mut stream,
            &RawFrame::json_text(br#"{"id":4,"type":"ping"}"#.to_vec()),
        )
        .await
        .unwrap();
        let pong = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&pong), json!({"id":4,"type":"pong"}));

        shutdown.cancel();
        connection.close(0u32.into(), b"done");
        client_endpoint.close(0u32.into(), b"done");
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_listener_auth_pipeline_releases_replica_read_gate_check() {
        let probe = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let secret = b"secret";
        let registry = registry().await;
        registry
            .create_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let config = ServerConfig {
            listen: ListenConfig {
                tcp: None,
                quic: Some(addr),
                ..Default::default()
            },
            tls: Some(TlsConfig::from_pem(
                TEST_CERT_PEM.to_vec(),
                TEST_KEY_PEM.to_vec(),
            )),
            auth: AuthConfig::hmac(JwtAlgorithm::HS256, secret.to_vec()),
            node_role: crate::session::NODE_ROLE_REPLICA.to_string(),
            replica_read_gate: Some(Arc::new(StaticReplicaReadGate(false))),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, registry));
        let server_task = {
            let server = Arc::clone(&server);
            let shutdown = shutdown.clone();
            tokio::spawn(async move { server.start(shutdown).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !server_task.is_finished(),
            "QUIC server task exited before client connection"
        );

        let mut client_endpoint = QuicEndpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(quic_client_config());
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (send, recv) = connection.accept_bi().await.unwrap();
        let mut stream = QuicStream::new(recv, send);

        let hello = read_frame(&mut stream).await.unwrap();
        assert_eq!(json_payload(&hello)["type"], "hello");
        assert_eq!(json_payload(&hello)["auth_required"], true);
        assert_eq!(json_payload(&hello)["node_role"], "replica");

        let token = hmac_token(secret, get_current_timestamp() + 60);
        assert_auth_pipeline_reaches_replica_read_gate(&mut stream, token).await;

        shutdown.cancel();
        connection.close(0u32.into(), b"done");
        client_endpoint.close(0u32.into(), b"done");
        server_task.await.unwrap().unwrap();
    }

    fn tls_client_config() -> ClientConfig {
        let mut roots = RootCertStore::empty();
        let mut reader = BufReader::new(TEST_CERT_PEM);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        for cert in certs {
            roots.add(cert).unwrap();
        }

        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }

    fn quic_client_config() -> quinn::ClientConfig {
        let mut roots = RootCertStore::empty();
        let mut reader = BufReader::new(TEST_CERT_PEM);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        for cert in certs {
            roots.add(cert).unwrap();
        }
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![QUIC_ALPN.to_vec()];
        quinn::ClientConfig::new(Arc::new(QuicRustlsClientConfig::try_from(config).unwrap()))
    }

    async fn connect_with_retry(addr: SocketAddr) -> TcpStream {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match TcpStream::connect(addr).await {
                Ok(stream) => return stream,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if err.kind() == ErrorKind::ConnectionRefused {
                        continue;
                    }
                }
                Err(err) => panic!("failed to connect to test listener {addr}: {err}"),
            }
        }
    }

    async fn connect_websocket_with_retry(
        addr: SocketAddr,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
        let url = format!("ws://{addr}/");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match connect_async(&url).await {
                Ok((stream, _)) => return stream,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if matches!(
                        err,
                        tokio_tungstenite::tungstenite::Error::Io(ref io)
                            if io.kind() == ErrorKind::ConnectionRefused
                    ) {
                        continue;
                    }
                }
                Err(err) => panic!("failed to connect to test WebSocket listener {addr}: {err}"),
            }
        }
    }
}
