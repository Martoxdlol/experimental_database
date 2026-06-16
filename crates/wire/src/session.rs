//! Per-connection session state for the wire protocol.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use exdb::{
    ConflictRetry, Database, DatabaseConfig, DatabaseError, DatabaseMeta, DatabaseState,
    DatabaseUsage, FieldPath, Filter, IndexReadyEvent, RangeExpr, Scalar, ScanDirection,
    SubscriptionHandle, SubscriptionMode, SystemDatabase, Transaction, TransactionOptions,
    TransactionResult, decode_ulid, encode_ulid,
};
use serde_json::{Value, json};

use crate::auth::{
    AuthClaims, AuthConfig, check_admin_access, check_database_access, validate_token,
};
use crate::frame::{DEFAULT_MAX_MESSAGE_SIZE, Encoding};
use crate::messages::{ClientMessage, ServerMessage};
use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Server-initiated notification plus any session state it materializes.
pub struct SessionNotification {
    message: ServerMessage,
    continuation: Option<(u64, ActiveTransaction)>,
}

impl SessionNotification {
    #[cfg(test)]
    pub(crate) fn message(message: ServerMessage) -> Self {
        Self {
            message,
            continuation: None,
        }
    }
}

/// Result from a scheduled transaction operation.
pub(crate) struct TransactionTaskResult {
    pub response: ServerMessage,
    updates: Vec<SessionUpdate>,
}

struct PendingSubscription {
    wire_tx_id: u64,
    handle: SubscriptionHandle,
    db: Option<Arc<Database>>,
    database_name: Option<String>,
    opts: Option<TransactionOptions>,
}

#[derive(Clone)]
pub(crate) struct TransactionExecutionContext {
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    next_wire_tx_id: Arc<AtomicU64>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum SessionUpdate {
    ActiveTransaction {
        wire_tx_id: u64,
        active: ActiveTransaction,
    },
    Subscription {
        wire_tx_id: u64,
        handle: SubscriptionHandle,
        db: Arc<Database>,
        database_name: String,
        opts: TransactionOptions,
    },
}

/// Session-local decision for a client message ID.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MessageIdAcceptance {
    Accepted,
    Duplicate,
    Rejected(ServerMessage),
}

/// Outcome returned by a replica write promotion backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionPromotionOutcome {
    Success {
        commit_ts: exdb::Ts,
    },
    Conflict {
        message: String,
        extra: Option<Value>,
    },
}

/// Management DDL operation that a replica can promote to the primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlPromotionRequest {
    CreateDatabase {
        name: String,
        config: Option<Value>,
    },
    DropDatabase {
        name: String,
    },
    CreateCollection {
        database: String,
        name: String,
    },
    DropCollection {
        database: String,
        name: String,
    },
    CreateIndex {
        database: String,
        collection: String,
        fields: Vec<Vec<String>>,
        name: Option<String>,
    },
    DropIndex {
        database: String,
        collection: String,
        name: String,
    },
}

/// Bridge used by replica L8 sessions to forward write commits to the primary.
#[async_trait]
pub trait TransactionPromoter: Send + Sync {
    async fn promote_transaction(
        &self,
        database: &str,
        begin_ts: exdb::Ts,
        subscription: SubscriptionMode,
        payload: Vec<u8>,
    ) -> std::result::Result<TransactionPromotionOutcome, String>;

    async fn promote_ddl(
        &self,
        request: DdlPromotionRequest,
    ) -> std::result::Result<ServerMessage, String>;
}

/// Quorum gate used by configured replica sessions before starting new reads.
pub trait ReplicaReadGate: Send + Sync {
    fn has_read_quorum(&self, database: &str) -> bool;
}

/// Cloned session state needed to process management requests asynchronously.
#[derive(Clone)]
pub struct ManagementContext {
    auth_claims: Option<AuthClaims>,
    registry: Arc<SystemDatabase>,
    default_database_config: DatabaseConfig,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
}

pub const NODE_ROLE_PRIMARY: &str = "primary";
pub const NODE_ROLE_REPLICA: &str = "replica";
pub const DEFAULT_NODE_ROLE: &str = NODE_ROLE_PRIMARY;

/// Stateful handler for one client connection.
pub struct Session {
    id: u64,
    auth_config: AuthConfig,
    authenticated: bool,
    auth_claims: Option<AuthClaims>,
    current_encoding: Encoding,
    seen_message_ids: HashSet<u32>,
    highest_message_id: u32,
    active_transactions: HashMap<u64, ActiveTransaction>,
    subscriptions: Vec<PendingSubscription>,
    next_wire_tx_id: Arc<AtomicU64>,
    notification_tx: Option<mpsc::Sender<SessionNotification>>,
    notification_tasks: Vec<JoinHandle<()>>,
    auth_expiry_task: Option<JoinHandle<()>>,
    index_ready_listener_started: bool,
    registry: Arc<SystemDatabase>,
    server_version: String,
    node_role: String,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    replica_read_gate: Option<Arc<dyn ReplicaReadGate>>,
    max_message_size: usize,
    default_database_config: DatabaseConfig,
}

impl Session {
    /// Create a new session with default server metadata.
    pub fn new(id: u64, registry: Arc<SystemDatabase>, auth_config: AuthConfig) -> Self {
        let auth_claims = if auth_config.enabled {
            None
        } else {
            Some(AuthClaims::unauthenticated_admin())
        };
        Self {
            id,
            auth_config,
            authenticated: auth_claims.is_some(),
            auth_claims,
            current_encoding: Encoding::Json,
            seen_message_ids: HashSet::new(),
            highest_message_id: 0,
            active_transactions: HashMap::new(),
            subscriptions: Vec::new(),
            next_wire_tx_id: Arc::new(AtomicU64::new(1)),
            notification_tx: None,
            notification_tasks: Vec::new(),
            auth_expiry_task: None,
            index_ready_listener_started: false,
            registry,
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            node_role: DEFAULT_NODE_ROLE.to_string(),
            transaction_promoter: None,
            replica_read_gate: None,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            default_database_config: DatabaseConfig::default(),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn auth_claims(&self) -> Option<&AuthClaims> {
        self.auth_claims.as_ref()
    }

    pub fn current_encoding(&self) -> Encoding {
        self.current_encoding
    }

    pub fn set_current_encoding(&mut self, encoding: Encoding) {
        self.current_encoding = encoding;
    }

    pub fn set_node_role(&mut self, role: impl Into<String>) {
        self.node_role = role.into();
    }

    pub fn set_transaction_promoter(&mut self, promoter: Option<Arc<dyn TransactionPromoter>>) {
        self.transaction_promoter = promoter;
    }

    pub fn set_replica_read_gate(&mut self, gate: Option<Arc<dyn ReplicaReadGate>>) {
        self.replica_read_gate = gate;
    }

    /// Validate and mark a client message ID as seen.
    pub(crate) fn accept_message_id(&mut self, msg_id: u32) -> MessageIdAcceptance {
        if msg_id == 0 {
            return MessageIdAcceptance::Rejected(error(
                "invalid_message",
                "client message id must be greater than zero",
            ));
        }
        if self.seen_message_ids.contains(&msg_id) {
            return MessageIdAcceptance::Duplicate;
        }
        let expected = self.highest_message_id.saturating_add(1);
        if msg_id != expected {
            self.seen_message_ids.insert(msg_id);
            return MessageIdAcceptance::Rejected(error(
                "invalid_message",
                format!(
                    "client message id must be the next incrementing value; received {msg_id}, expected {expected}"
                ),
            ));
        }

        self.seen_message_ids.insert(msg_id);
        self.highest_message_id = msg_id;
        while self
            .highest_message_id
            .checked_add(1)
            .is_some_and(|next| self.seen_message_ids.contains(&next))
        {
            self.highest_message_id += 1;
        }
        MessageIdAcceptance::Accepted
    }

    /// Return a pre-dispatch auth response, or `None` when processing may
    /// continue.
    pub fn preflight_response(&mut self, msg: &ClientMessage) -> Option<ServerMessage> {
        if self.auth_config.enabled
            && self.authenticated
            && !matches!(msg, ClientMessage::Authenticate { .. })
            && self
                .auth_claims
                .as_ref()
                .is_some_and(|claims| claims.exp <= current_unix_secs())
        {
            self.authenticated = false;
            self.auth_claims = None;
            return Some(error("auth_expired", "authentication token expired"));
        }

        if self.auth_config.enabled
            && !self.authenticated
            && !matches!(msg, ClientMessage::Authenticate { .. })
        {
            Some(error("auth_required", "authentication required"))
        } else {
            None
        }
    }

    pub fn set_max_message_size(&mut self, max_message_size: usize) {
        self.max_message_size = max_message_size;
    }

    pub fn set_default_database_config(&mut self, config: DatabaseConfig) {
        self.default_database_config = config;
    }

    /// Configure a channel for server-initiated notifications.
    pub fn set_notification_sender(&mut self, tx: mpsc::Sender<SessionNotification>) {
        self.notification_tx = Some(tx);
        self.ensure_index_ready_listener();
        let subscriptions = std::mem::take(&mut self.subscriptions);
        for subscription in subscriptions {
            self.register_subscription(
                subscription.wire_tx_id,
                subscription.handle,
                subscription.db,
                subscription.database_name,
                subscription.opts,
            );
        }
    }

    /// Install state carried by a server-initiated notification.
    pub fn apply_notification(&mut self, notification: SessionNotification) -> ServerMessage {
        if let Some((wire_tx_id, active)) = notification.continuation {
            self.active_transactions.insert(wire_tx_id, active);
        }
        if response_code(&notification.message).is_some_and(|code| code == "auth_expired") {
            self.authenticated = false;
            self.auth_claims = None;
        }
        notification.message
    }

    /// Server hello sent immediately after transport connection.
    pub fn hello(&self) -> ServerMessage {
        ServerMessage::Hello {
            version: self.server_version.clone(),
            encodings: vec![
                "json".to_string(),
                "bson".to_string(),
                "protobuf".to_string(),
            ],
            auth_required: self.auth_config.enabled,
            node_role: self.node_role.clone(),
            max_message_size: self.max_message_size,
        }
    }

    /// Clone the session state needed by asynchronous management workers.
    pub fn management_context(&self) -> ManagementContext {
        ManagementContext {
            auth_claims: self.auth_claims.clone(),
            registry: Arc::clone(&self.registry),
            default_database_config: self.default_database_config.clone(),
            node_role: self.node_role.clone(),
            transaction_promoter: self.transaction_promoter.clone(),
        }
    }

    /// Process one parsed client message.
    ///
    /// Duplicate nonzero message IDs are ignored per the wire specification.
    pub async fn handle_message(
        &mut self,
        msg_id: u32,
        msg: ClientMessage,
    ) -> Option<ServerMessage> {
        match self.accept_message_id(msg_id) {
            MessageIdAcceptance::Accepted => {}
            MessageIdAcceptance::Duplicate => return None,
            MessageIdAcceptance::Rejected(response) => return Some(response),
        }

        Some(self.handle_accepted_message(msg).await)
    }

    /// Process a message whose ID has already passed deduplication.
    pub async fn handle_accepted_message(&mut self, msg: ClientMessage) -> ServerMessage {
        if let Some(response) = self.preflight_response(&msg) {
            return response;
        }
        if is_replica_role(&self.node_role) && is_management_write(&msg) {
            return self.handle_promoted_management_write(msg).await;
        }

        match msg {
            ClientMessage::Authenticate { token } => self.authenticate(&token),
            ClientMessage::Ping => ServerMessage::Pong,
            ClientMessage::CreateDatabase { name, config } => {
                self.handle_create_database(&name, config).await
            }
            ClientMessage::DropDatabase { name } => self.handle_drop_database(&name).await,
            ClientMessage::ListDatabases => self.handle_list_databases(),
            ClientMessage::CreateCollection { database, name } => {
                self.handle_create_collection(&database, &name).await
            }
            ClientMessage::DropCollection { database, name } => {
                self.handle_drop_collection(&database, &name).await
            }
            ClientMessage::ListCollections { database } => self.handle_list_collections(&database),
            ClientMessage::CreateIndex {
                database,
                collection,
                fields,
                name,
            } => {
                self.handle_create_index(&database, &collection, fields, name)
                    .await
            }
            ClientMessage::DropIndex {
                database,
                collection,
                name,
            } => self.handle_drop_index(&database, &collection, &name).await,
            ClientMessage::ListIndexes {
                database,
                collection,
            } => self.handle_list_indexes(&database, &collection),
            ClientMessage::Begin {
                database,
                readonly,
                subscribe,
                notify,
            } => {
                self.handle_begin(&database, readonly, subscribe, notify)
                    .await
            }
            ClientMessage::Commit { tx } => self.handle_commit(tx).await,
            ClientMessage::Rollback { tx } => self.handle_rollback(tx),
            ClientMessage::Insert {
                tx,
                collection,
                body,
            } => self.handle_insert(tx, &collection, body).await,
            ClientMessage::Get {
                tx,
                collection,
                doc_id,
            } => self.handle_get(tx, &collection, &doc_id).await,
            ClientMessage::Replace {
                tx,
                collection,
                doc_id,
                body,
            } => self.handle_replace(tx, &collection, &doc_id, body).await,
            ClientMessage::Patch {
                tx,
                collection,
                doc_id,
                body,
            } => self.handle_patch(tx, &collection, &doc_id, body).await,
            ClientMessage::Delete {
                tx,
                collection,
                doc_id,
            } => self.handle_delete(tx, &collection, &doc_id).await,
            ClientMessage::Query {
                tx,
                collection,
                index,
                range,
                filter,
                type_hints,
                order,
                limit,
            } => {
                self.handle_query(
                    tx,
                    &collection,
                    &index,
                    range,
                    filter,
                    type_hints,
                    order,
                    limit,
                )
                .await
            }
        }
    }

    /// Return the transaction identifier targeted by a transaction-scoped
    /// message, or `None` for non-transaction messages.
    pub fn transaction_message_id(msg: &ClientMessage) -> Option<u64> {
        match msg {
            ClientMessage::Commit { tx }
            | ClientMessage::Rollback { tx }
            | ClientMessage::Insert { tx, .. }
            | ClientMessage::Get { tx, .. }
            | ClientMessage::Replace { tx, .. }
            | ClientMessage::Patch { tx, .. }
            | ClientMessage::Delete { tx, .. }
            | ClientMessage::Query { tx, .. } => Some(*tx),
            _ => None,
        }
    }

    /// Remove an active transaction so a transport scheduler can process one
    /// operation without holding the whole session mutable across the await.
    pub(crate) fn take_active_transaction(&mut self, tx_id: u64) -> Option<ActiveTransaction> {
        self.active_transactions.remove(&tx_id)
    }

    pub(crate) fn transaction_execution_context(&self) -> TransactionExecutionContext {
        TransactionExecutionContext {
            node_role: self.node_role.clone(),
            transaction_promoter: self.transaction_promoter.clone(),
            next_wire_tx_id: Arc::clone(&self.next_wire_tx_id),
        }
    }

    pub(crate) fn unknown_transaction_response(tx_id: u64) -> ServerMessage {
        error(
            "unknown_transaction",
            format!("unknown transaction: {tx_id}"),
        )
    }

    pub(crate) fn apply_transaction_task_result(
        &mut self,
        result: TransactionTaskResult,
    ) -> ServerMessage {
        for update in result.updates {
            self.apply_session_update(update);
        }
        result.response
    }

    fn apply_session_update(&mut self, update: SessionUpdate) {
        match update {
            SessionUpdate::ActiveTransaction { wire_tx_id, active } => {
                self.active_transactions.insert(wire_tx_id, active);
            }
            SessionUpdate::Subscription {
                wire_tx_id,
                handle,
                db,
                database_name,
                opts,
            } => {
                self.register_subscription(
                    wire_tx_id,
                    handle,
                    Some(db),
                    Some(database_name),
                    Some(opts),
                );
            }
        }
    }

    pub(crate) async fn handle_transaction_task(
        mut active: ActiveTransaction,
        msg: ClientMessage,
        context: TransactionExecutionContext,
        wire_tx_id: u64,
    ) -> TransactionTaskResult {
        let mut updates = Vec::new();
        let response = match msg {
            ClientMessage::Commit { .. } => {
                return handle_active_commit(active, context, wire_tx_id).await;
            }
            ClientMessage::Rollback { .. } => {
                active.tx.rollback();
                ok(json!({}))
            }
            ClientMessage::Insert {
                collection, body, ..
            } => match active.tx.insert(&collection, body).await {
                Ok(doc_id) => {
                    updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                    ok(json!({ "doc_id": encode_ulid(&doc_id) }))
                }
                Err(err) => transaction_task_database_error(&mut updates, wire_tx_id, active, err),
            },
            ClientMessage::Get {
                collection, doc_id, ..
            } => {
                let doc_id = match decode_ulid(&doc_id) {
                    Ok(doc_id) => doc_id,
                    Err(err) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_message", format!("invalid doc_id: {err}")),
                            updates,
                        };
                    }
                };
                match active.tx.get_with_query_id(&collection, &doc_id).await {
                    Ok((query_id, doc)) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        ok(json!({ "query_id": query_id, "doc": doc }))
                    }
                    Err(err) => {
                        transaction_task_database_error(&mut updates, wire_tx_id, active, err)
                    }
                }
            }
            ClientMessage::Replace {
                collection,
                doc_id,
                body,
                ..
            } => {
                let doc_id = match decode_ulid(&doc_id) {
                    Ok(doc_id) => doc_id,
                    Err(err) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_message", format!("invalid doc_id: {err}")),
                            updates,
                        };
                    }
                };
                match active.tx.replace(&collection, &doc_id, body).await {
                    Ok(()) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        ok(json!({}))
                    }
                    Err(err) => {
                        transaction_task_database_error(&mut updates, wire_tx_id, active, err)
                    }
                }
            }
            ClientMessage::Patch {
                collection,
                doc_id,
                body,
                ..
            } => {
                let doc_id = match decode_ulid(&doc_id) {
                    Ok(doc_id) => doc_id,
                    Err(err) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_message", format!("invalid doc_id: {err}")),
                            updates,
                        };
                    }
                };
                match active.tx.patch(&collection, &doc_id, body).await {
                    Ok(()) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        ok(json!({}))
                    }
                    Err(err) => {
                        transaction_task_database_error(&mut updates, wire_tx_id, active, err)
                    }
                }
            }
            ClientMessage::Delete {
                collection, doc_id, ..
            } => {
                let doc_id = match decode_ulid(&doc_id) {
                    Ok(doc_id) => doc_id,
                    Err(err) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_message", format!("invalid doc_id: {err}")),
                            updates,
                        };
                    }
                };
                match active.tx.delete(&collection, &doc_id).await {
                    Ok(()) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        ok(json!({}))
                    }
                    Err(err) => {
                        transaction_task_database_error(&mut updates, wire_tx_id, active, err)
                    }
                }
            }
            ClientMessage::Query {
                collection,
                index,
                range,
                filter,
                type_hints,
                order,
                limit,
                ..
            } => {
                let range_hints = type_hints.as_ref().and_then(|hints| hints.get("range"));
                let filter_hints = type_hints.as_ref().and_then(|hints| hints.get("filter"));
                let range = match parse_range_exprs(range, range_hints) {
                    Ok(range) => range,
                    Err(message) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_range", message),
                            updates,
                        };
                    }
                };
                let filter = match filter
                    .map(|filter| parse_filter(filter, filter_hints))
                    .transpose()
                {
                    Ok(filter) => filter,
                    Err(message) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_filter", message),
                            updates,
                        };
                    }
                };
                let direction = match parse_order(order.as_deref()) {
                    Ok(direction) => direction,
                    Err(message) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        return TransactionTaskResult {
                            response: error("invalid_message", message),
                            updates,
                        };
                    }
                };
                match active
                    .tx
                    .query_with_query_id(&collection, &index, &range, filter, direction, limit)
                    .await
                {
                    Ok((query_id, docs)) => {
                        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
                        ok(json!({ "query_id": query_id, "docs": docs }))
                    }
                    Err(err) => {
                        transaction_task_database_error(&mut updates, wire_tx_id, active, err)
                    }
                }
            }
            _ => error(
                "internal",
                "transaction scheduler received non-transaction message",
            ),
        };
        TransactionTaskResult { response, updates }
    }

    fn authenticate(&mut self, token: &str) -> ServerMessage {
        match validate_token(token, &self.auth_config) {
            Ok(claims) => {
                self.authenticated = true;
                self.auth_claims = Some(claims);
                self.schedule_auth_expiry_notification();
                self.ensure_index_ready_listener();
                ok(json!({}))
            }
            Err(err) => error("auth_failed", err.to_string()),
        }
    }

    async fn handle_create_database(&self, name: &str, config: Option<Value>) -> ServerMessage {
        if !self.is_admin() {
            return error("forbidden", "admin role required");
        }

        let config = match database_config_from_json(config, &self.default_database_config) {
            Ok(config) => config,
            Err(message) => return error("invalid_message", message),
        };

        match self.registry.create_database(name, config).await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_database(&self, name: &str) -> ServerMessage {
        if !self.is_admin() {
            return error("forbidden", "admin role required");
        }

        match self.registry.drop_database(name).await {
            Ok(()) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_promoted_management_write(&self, msg: ClientMessage) -> ServerMessage {
        let Some(promoter) = self.transaction_promoter.clone() else {
            return readonly_node_response("management writes on replicas require DDL promotion");
        };
        let request = match self.ddl_promotion_request(msg) {
            Ok(request) => request,
            Err(response) => return response,
        };
        match promoter.promote_ddl(request).await {
            Ok(response) => response,
            Err(message) => readonly_node_response(format!("DDL promotion failed: {message}")),
        }
    }

    fn ddl_promotion_request(
        &self,
        msg: ClientMessage,
    ) -> std::result::Result<DdlPromotionRequest, ServerMessage> {
        match msg {
            ClientMessage::CreateDatabase { name, config } => {
                if !self.is_admin() {
                    return Err(error("forbidden", "admin role required"));
                }
                Ok(DdlPromotionRequest::CreateDatabase { name, config })
            }
            ClientMessage::DropDatabase { name } => {
                if !self.is_admin() {
                    return Err(error("forbidden", "admin role required"));
                }
                Ok(DdlPromotionRequest::DropDatabase { name })
            }
            ClientMessage::CreateCollection { database, name } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::CreateCollection { database, name })
            }
            ClientMessage::DropCollection { database, name } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::DropCollection { database, name })
            }
            ClientMessage::CreateIndex {
                database,
                collection,
                fields,
                name,
            } => {
                self.ensure_database_access(&database)?;
                let fields = parse_field_paths(fields)
                    .map_err(|message| error("invalid_field_path", message))?
                    .into_iter()
                    .map(|field| field.segments().to_vec())
                    .collect();
                Ok(DdlPromotionRequest::CreateIndex {
                    database,
                    collection,
                    fields,
                    name,
                })
            }
            ClientMessage::DropIndex {
                database,
                collection,
                name,
            } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::DropIndex {
                    database,
                    collection,
                    name,
                })
            }
            _ => Err(error(
                "invalid_message",
                "message is not a promotable DDL management write",
            )),
        }
    }

    fn handle_list_databases(&self) -> ServerMessage {
        let Some(claims) = self.auth_claims.as_ref() else {
            return error("auth_required", "authentication required");
        };

        match list_database_metadata_json(&self.registry, claims) {
            Ok(databases) => ok(json!({ "databases": databases })),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_create_collection(&self, database: &str, name: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.create_collection(name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_collection(&self, database: &str, name: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.drop_collection(name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    fn handle_list_collections(&self, database: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let collections: Vec<Value> = db
            .list_collections()
            .into_iter()
            .map(|meta| {
                json!({
                    "id": meta.collection_id.0,
                    "name": meta.name,
                    "doc_count": meta.doc_count,
                })
            })
            .collect();
        ok(json!({ "collections": collections }))
    }

    async fn handle_create_index(
        &self,
        database: &str,
        collection: &str,
        fields: Vec<Value>,
        name: Option<String>,
    ) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let fields = match parse_field_paths(fields) {
            Ok(fields) => fields,
            Err(message) => return error("invalid_field_path", message),
        };
        let name = name.unwrap_or_else(|| default_index_name(&fields));

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.create_index(collection, &name, fields).await {
            return database_error_response(err);
        }
        if let Err(err) = tx.commit().await {
            return database_error_response(err);
        }

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        match tx.list_indexes(collection) {
            Ok(indexes) => {
                let index = indexes.into_iter().find(|meta| meta.name == name);
                match index {
                    Some(index) => ok(json!({ "index_id": index.index_id.0 })),
                    None => error("internal", "created index was not visible after commit"),
                }
            }
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_index(
        &self,
        database: &str,
        collection: &str,
        name: &str,
    ) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.drop_index(collection, name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    fn handle_list_indexes(&self, database: &str, collection: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        match tx.list_indexes(collection) {
            Ok(indexes) => {
                let indexes: Vec<Value> = indexes
                    .into_iter()
                    .map(|meta| {
                        json!({
                            "id": meta.index_id.0,
                            "name": meta.name,
                            "fields": meta.field_paths.iter().map(field_path_json).collect::<Vec<_>>(),
                            "state": format!("{:?}", meta.state).to_lowercase(),
                        })
                    })
                    .collect();
                ok(json!({ "indexes": indexes }))
            }
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_begin(
        &mut self,
        database: &str,
        readonly: bool,
        subscribe: bool,
        notify: bool,
    ) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }
        if readonly
            && is_replica_role(&self.node_role)
            && self
                .replica_read_gate
                .as_ref()
                .is_some_and(|gate| !gate.has_read_quorum(database))
        {
            return error(
                "quorum_lost",
                format!("replica lacks read quorum for database: {database}"),
            );
        }
        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let subscription = if subscribe {
            SubscriptionMode::Subscribe
        } else if notify {
            SubscriptionMode::Notify
        } else {
            SubscriptionMode::None
        };

        let opts = TransactionOptions {
            readonly,
            subscription,
            session_id: self.id,
        };
        let tx = match db.begin(opts.clone()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        let tx_id = self.next_wire_tx_id.fetch_add(1, Ordering::Relaxed);
        let begin_ts = tx.begin_ts();
        self.active_transactions.insert(
            tx_id,
            ActiveTransaction {
                tx,
                db,
                database_name: database.to_string(),
                opts,
            },
        );
        ok(json!({ "tx": tx_id, "begin_ts": begin_ts }))
    }

    async fn handle_commit(&mut self, tx_id: u64) -> ServerMessage {
        let Some(active) = self.active_transactions.remove(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };

        let result =
            handle_active_commit(active, self.transaction_execution_context(), tx_id).await;
        self.apply_transaction_task_result(result)
    }

    fn handle_rollback(&mut self, tx_id: u64) -> ServerMessage {
        let Some(active) = self.active_transactions.remove(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        active.tx.rollback();
        ok(json!({}))
    }

    async fn handle_insert(&mut self, tx_id: u64, collection: &str, body: Value) -> ServerMessage {
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active.tx.insert(collection, body).await {
            Ok(doc_id) => ok(json!({ "doc_id": encode_ulid(&doc_id) })),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    async fn handle_get(&mut self, tx_id: u64, collection: &str, doc_id: &str) -> ServerMessage {
        let doc_id = match decode_ulid(doc_id) {
            Ok(doc_id) => doc_id,
            Err(err) => return error("invalid_message", format!("invalid doc_id: {err}")),
        };
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active.tx.get_with_query_id(collection, &doc_id).await {
            Ok((query_id, doc)) => ok(json!({ "query_id": query_id, "doc": doc })),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    async fn handle_replace(
        &mut self,
        tx_id: u64,
        collection: &str,
        doc_id: &str,
        body: Value,
    ) -> ServerMessage {
        let doc_id = match decode_ulid(doc_id) {
            Ok(doc_id) => doc_id,
            Err(err) => return error("invalid_message", format!("invalid doc_id: {err}")),
        };
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active.tx.replace(collection, &doc_id, body).await {
            Ok(()) => ok(json!({})),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    async fn handle_patch(
        &mut self,
        tx_id: u64,
        collection: &str,
        doc_id: &str,
        body: Value,
    ) -> ServerMessage {
        let doc_id = match decode_ulid(doc_id) {
            Ok(doc_id) => doc_id,
            Err(err) => return error("invalid_message", format!("invalid doc_id: {err}")),
        };
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active.tx.patch(collection, &doc_id, body).await {
            Ok(()) => ok(json!({})),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    async fn handle_delete(&mut self, tx_id: u64, collection: &str, doc_id: &str) -> ServerMessage {
        let doc_id = match decode_ulid(doc_id) {
            Ok(doc_id) => doc_id,
            Err(err) => return error("invalid_message", format!("invalid doc_id: {err}")),
        };
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active.tx.delete(collection, &doc_id).await {
            Ok(()) => ok(json!({})),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_query(
        &mut self,
        tx_id: u64,
        collection: &str,
        index: &str,
        range: Vec<Value>,
        filter: Option<Value>,
        type_hints: Option<Value>,
        order: Option<String>,
        limit: Option<usize>,
    ) -> ServerMessage {
        let range_hints = type_hints.as_ref().and_then(|hints| hints.get("range"));
        let filter_hints = type_hints.as_ref().and_then(|hints| hints.get("filter"));
        let range = match parse_range_exprs(range, range_hints) {
            Ok(range) => range,
            Err(message) => return error("invalid_range", message),
        };
        let filter = match filter
            .map(|filter| parse_filter(filter, filter_hints))
            .transpose()
        {
            Ok(filter) => filter,
            Err(message) => return error("invalid_filter", message),
        };
        let direction = match parse_order(order.as_deref()) {
            Ok(direction) => direction,
            Err(message) => return error("invalid_message", message),
        };
        let Some(active) = self.active_transactions.get_mut(&tx_id) else {
            return error(
                "unknown_transaction",
                format!("unknown transaction: {tx_id}"),
            );
        };
        match active
            .tx
            .query_with_query_id(collection, index, &range, filter, direction, limit)
            .await
        {
            Ok((query_id, docs)) => ok(json!({ "query_id": query_id, "docs": docs })),
            Err(err) => self.database_error_for_transaction(tx_id, err),
        }
    }

    fn database_error_for_transaction(&mut self, tx_id: u64, err: DatabaseError) -> ServerMessage {
        let drop_active = transaction_error_drops_active(&err);
        let response = database_error_response(err);
        if drop_active {
            self.active_transactions.remove(&tx_id);
        }
        response
    }

    fn is_admin(&self) -> bool {
        self.auth_claims.as_ref().is_some_and(check_admin_access)
    }

    fn ensure_database_access(&self, database: &str) -> std::result::Result<(), ServerMessage> {
        let Some(claims) = self.auth_claims.as_ref() else {
            return Err(error("auth_required", "authentication required"));
        };
        if check_database_access(claims, database) {
            Ok(())
        } else {
            Err(error(
                "forbidden",
                format!("database access denied: {database}"),
            ))
        }
    }

    fn register_subscription(
        &mut self,
        wire_tx_id: u64,
        handle: SubscriptionHandle,
        db: Option<Arc<Database>>,
        database_name: Option<String>,
        opts: Option<TransactionOptions>,
    ) {
        let Some(tx) = self.notification_tx.clone() else {
            self.subscriptions.push(PendingSubscription {
                wire_tx_id,
                handle,
                db,
                database_name,
                opts,
            });
            return;
        };

        self.notification_tasks
            .push(tokio::spawn(forward_subscription_events(
                handle,
                wire_tx_id,
                tx,
                db,
                database_name,
                opts,
                Arc::clone(&self.next_wire_tx_id),
            )));
    }

    fn ensure_index_ready_listener(&mut self) {
        if self.index_ready_listener_started {
            return;
        }
        let Some(tx) = self.notification_tx.clone() else {
            return;
        };
        let Some(claims) = self.auth_claims.clone() else {
            return;
        };

        self.index_ready_listener_started = true;
        let rx = self.registry.subscribe_index_ready();
        self.notification_tasks
            .push(tokio::spawn(forward_index_ready_events(rx, tx, claims)));
    }

    fn schedule_auth_expiry_notification(&mut self) {
        if let Some(task) = self.auth_expiry_task.take() {
            task.abort();
        }
        if !self.auth_config.enabled {
            return;
        }
        let Some(tx) = self.notification_tx.clone() else {
            return;
        };
        let Some(exp) = self.auth_claims.as_ref().map(|claims| claims.exp) else {
            return;
        };

        self.auth_expiry_task = Some(tokio::spawn(async move {
            let now = current_unix_secs();
            if exp > now {
                tokio::time::sleep(Duration::from_secs(exp - now)).await;
            }
            let _ = tx
                .send(SessionNotification {
                    message: error("auth_expired", "authentication token expired"),
                    continuation: None,
                })
                .await;
        }));
    }

    fn remove_session_subscriptions(&self) {
        for database in self.registry.list_databases() {
            if let Some(db) = self.registry.get_database_by_name(&database.name) {
                db.subscriptions().write().remove_session(self.id);
            }
        }
    }
}

fn transaction_task_database_error(
    updates: &mut Vec<SessionUpdate>,
    wire_tx_id: u64,
    active: ActiveTransaction,
    err: DatabaseError,
) -> ServerMessage {
    if !transaction_error_drops_active(&err) {
        updates.push(SessionUpdate::ActiveTransaction { wire_tx_id, active });
    }
    database_error_response(err)
}

fn transaction_error_drops_active(err: &DatabaseError) -> bool {
    matches!(
        err,
        DatabaseError::ReadLimitExceeded(_) | DatabaseError::TransactionTimeout
    )
}

async fn handle_active_commit(
    active: ActiveTransaction,
    context: TransactionExecutionContext,
    wire_tx_id: u64,
) -> TransactionTaskResult {
    if is_replica_role(&context.node_role) && !active.opts.readonly {
        return handle_promoted_commit(active, context, wire_tx_id).await;
    }

    let mut updates = Vec::new();
    let response = match active.tx.commit().await {
        Ok(TransactionResult::Success {
            commit_ts,
            subscription_handle,
        }) => {
            let subscription_id = subscription_handle.as_ref().map(|handle| handle.id());
            if let Some(handle) = subscription_handle {
                updates.push(SessionUpdate::Subscription {
                    wire_tx_id,
                    handle,
                    db: Arc::clone(&active.db),
                    database_name: active.database_name.clone(),
                    opts: active.opts.clone(),
                });
            }
            let mut fields = json!({ "commit_ts": commit_ts });
            if let (Some(id), Value::Object(map)) = (subscription_id, &mut fields) {
                map.insert("subscription_id".to_string(), Value::from(id));
            }
            ok(fields)
        }
        Ok(TransactionResult::Conflict { error: err, retry }) => {
            let extra = match retry {
                Some(retry) => {
                    let new_tx = context.next_wire_tx_id.fetch_add(1, Ordering::Relaxed);
                    let new_ts = retry.new_ts;
                    match active.db.begin_conflict_retry(retry, active.opts.clone()) {
                        Ok(tx) => {
                            updates.push(SessionUpdate::ActiveTransaction {
                                wire_tx_id: new_tx,
                                active: ActiveTransaction {
                                    tx,
                                    db: Arc::clone(&active.db),
                                    database_name: active.database_name.clone(),
                                    opts: active.opts.clone(),
                                },
                            });
                            Some(json!({
                                "new_tx": new_tx,
                                "new_ts": new_ts,
                            }))
                        }
                        Err(err) => {
                            return TransactionTaskResult {
                                response: database_error_response(err),
                                updates,
                            };
                        }
                    }
                }
                None => None,
            };
            ServerMessage::Error {
                code: "conflict".to_string(),
                message: err.to_string(),
                extra,
            }
        }
        Ok(TransactionResult::QuorumLost) => error("quorum_lost", "replication quorum lost"),
        Err(err) => database_error_response(err),
    };
    TransactionTaskResult { response, updates }
}

async fn handle_promoted_commit(
    active: ActiveTransaction,
    context: TransactionExecutionContext,
    wire_tx_id: u64,
) -> TransactionTaskResult {
    let mut updates = Vec::new();
    let ActiveTransaction {
        tx,
        db,
        database_name,
        opts,
    } = active;

    let Some(promoter) = context.transaction_promoter else {
        return TransactionTaskResult {
            response: readonly_node_response("write transaction on replica requires promotion"),
            updates,
        };
    };

    let promotion = match tx.into_promotion_payload_parts() {
        Ok(promotion) => promotion,
        Err(err) => {
            return TransactionTaskResult {
                response: database_error_response(err),
                updates,
            };
        }
    };

    let response = match promoter
        .promote_transaction(
            &database_name,
            promotion.begin_ts,
            opts.subscription,
            promotion.payload,
        )
        .await
    {
        Ok(TransactionPromotionOutcome::Success { commit_ts }) => {
            let subscription_handle = if opts.subscription != SubscriptionMode::None {
                match db
                    .register_promoted_subscription(
                        promotion.tx_id,
                        commit_ts,
                        promotion.read_set,
                        &promotion.write_set,
                        opts.clone(),
                    )
                    .await
                {
                    Ok(handle) => handle,
                    Err(err) => {
                        return TransactionTaskResult {
                            response: database_error_response(err),
                            updates,
                        };
                    }
                }
            } else {
                None
            };
            let subscription_id = subscription_handle.as_ref().map(|handle| handle.id());
            if let Some(handle) = subscription_handle {
                updates.push(SessionUpdate::Subscription {
                    wire_tx_id,
                    handle,
                    db: Arc::clone(&db),
                    database_name: database_name.clone(),
                    opts: opts.clone(),
                });
            }
            let mut fields = json!({ "commit_ts": commit_ts });
            if let (Some(id), Value::Object(map)) = (subscription_id, &mut fields) {
                map.insert("subscription_id".to_string(), Value::from(id));
            }
            ok(fields)
        }
        Ok(TransactionPromotionOutcome::Conflict { message, extra }) => {
            let extra = if opts.subscription == SubscriptionMode::Subscribe {
                if let Some((internal_new_tx, new_ts)) = promoted_retry_fields(extra.as_ref()) {
                    let wire_new_tx = context.next_wire_tx_id.fetch_add(1, Ordering::Relaxed);
                    match db.begin_conflict_retry(
                        ConflictRetry {
                            new_tx_id: internal_new_tx,
                            new_ts,
                        },
                        opts.clone(),
                    ) {
                        Ok(tx) => {
                            updates.push(SessionUpdate::ActiveTransaction {
                                wire_tx_id: wire_new_tx,
                                active: ActiveTransaction {
                                    tx,
                                    db: Arc::clone(&db),
                                    database_name: database_name.clone(),
                                    opts: opts.clone(),
                                },
                            });
                            Some(json!({
                                "new_tx": wire_new_tx,
                                "new_ts": new_ts,
                            }))
                        }
                        Err(err) => {
                            return TransactionTaskResult {
                                response: database_error_response(err),
                                updates,
                            };
                        }
                    }
                } else {
                    extra
                }
            } else {
                extra
            };
            ServerMessage::Error {
                code: "conflict".to_string(),
                message,
                extra,
            }
        }
        Err(message) => readonly_node_response(format!("transaction promotion failed: {message}")),
    };

    TransactionTaskResult { response, updates }
}

fn promoted_retry_fields(extra: Option<&Value>) -> Option<(u64, u64)> {
    let extra = extra?.as_object()?;
    let new_tx = extra.get("new_tx")?.as_u64()?;
    let new_ts = extra.get("new_ts")?.as_u64()?;
    Some((new_tx, new_ts))
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl ManagementContext {
    /// Process a management message outside the main connection session.
    ///
    /// Returns `None` for non-management messages so callers can route those
    /// back through `Session`.
    pub async fn handle_message(&self, msg: ClientMessage) -> Option<ServerMessage> {
        if is_replica_role(&self.node_role) && is_management_write(&msg) {
            return Some(self.handle_promoted_management_write(msg).await);
        }

        let response = match msg {
            ClientMessage::CreateDatabase { name, config } => {
                self.handle_create_database(&name, config).await
            }
            ClientMessage::DropDatabase { name } => self.handle_drop_database(&name).await,
            ClientMessage::ListDatabases => self.handle_list_databases(),
            ClientMessage::CreateCollection { database, name } => {
                self.handle_create_collection(&database, &name).await
            }
            ClientMessage::DropCollection { database, name } => {
                self.handle_drop_collection(&database, &name).await
            }
            ClientMessage::ListCollections { database } => self.handle_list_collections(&database),
            ClientMessage::CreateIndex {
                database,
                collection,
                fields,
                name,
            } => {
                self.handle_create_index(&database, &collection, fields, name)
                    .await
            }
            ClientMessage::DropIndex {
                database,
                collection,
                name,
            } => self.handle_drop_index(&database, &collection, &name).await,
            ClientMessage::ListIndexes {
                database,
                collection,
            } => self.handle_list_indexes(&database, &collection),
            ClientMessage::Authenticate { .. }
            | ClientMessage::Ping
            | ClientMessage::Begin { .. }
            | ClientMessage::Commit { .. }
            | ClientMessage::Rollback { .. }
            | ClientMessage::Insert { .. }
            | ClientMessage::Get { .. }
            | ClientMessage::Replace { .. }
            | ClientMessage::Patch { .. }
            | ClientMessage::Delete { .. }
            | ClientMessage::Query { .. } => return None,
        };
        Some(response)
    }

    fn is_admin(&self) -> bool {
        self.auth_claims.as_ref().is_some_and(check_admin_access)
    }

    fn ensure_database_access(&self, database: &str) -> std::result::Result<(), ServerMessage> {
        let Some(claims) = self.auth_claims.as_ref() else {
            return Err(error("auth_required", "authentication required"));
        };
        if check_database_access(claims, database) {
            Ok(())
        } else {
            Err(error(
                "forbidden",
                format!("database access denied: {database}"),
            ))
        }
    }

    async fn handle_create_database(&self, name: &str, config: Option<Value>) -> ServerMessage {
        if !self.is_admin() {
            return error("forbidden", "admin role required");
        }

        let config = match database_config_from_json(config, &self.default_database_config) {
            Ok(config) => config,
            Err(message) => return error("invalid_message", message),
        };

        match self.registry.create_database(name, config).await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_database(&self, name: &str) -> ServerMessage {
        if !self.is_admin() {
            return error("forbidden", "admin role required");
        }

        match self.registry.drop_database(name).await {
            Ok(()) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_promoted_management_write(&self, msg: ClientMessage) -> ServerMessage {
        let Some(promoter) = self.transaction_promoter.clone() else {
            return readonly_node_response("management writes on replicas require DDL promotion");
        };
        let request = match self.ddl_promotion_request(msg) {
            Ok(request) => request,
            Err(response) => return response,
        };
        match promoter.promote_ddl(request).await {
            Ok(response) => response,
            Err(message) => readonly_node_response(format!("DDL promotion failed: {message}")),
        }
    }

    fn ddl_promotion_request(
        &self,
        msg: ClientMessage,
    ) -> std::result::Result<DdlPromotionRequest, ServerMessage> {
        match msg {
            ClientMessage::CreateDatabase { name, config } => {
                if !self.is_admin() {
                    return Err(error("forbidden", "admin role required"));
                }
                Ok(DdlPromotionRequest::CreateDatabase { name, config })
            }
            ClientMessage::DropDatabase { name } => {
                if !self.is_admin() {
                    return Err(error("forbidden", "admin role required"));
                }
                Ok(DdlPromotionRequest::DropDatabase { name })
            }
            ClientMessage::CreateCollection { database, name } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::CreateCollection { database, name })
            }
            ClientMessage::DropCollection { database, name } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::DropCollection { database, name })
            }
            ClientMessage::CreateIndex {
                database,
                collection,
                fields,
                name,
            } => {
                self.ensure_database_access(&database)?;
                let fields = parse_field_paths(fields)
                    .map_err(|message| error("invalid_field_path", message))?
                    .into_iter()
                    .map(|field| field.segments().to_vec())
                    .collect();
                Ok(DdlPromotionRequest::CreateIndex {
                    database,
                    collection,
                    fields,
                    name,
                })
            }
            ClientMessage::DropIndex {
                database,
                collection,
                name,
            } => {
                self.ensure_database_access(&database)?;
                Ok(DdlPromotionRequest::DropIndex {
                    database,
                    collection,
                    name,
                })
            }
            _ => Err(error(
                "invalid_message",
                "message is not a promotable DDL management write",
            )),
        }
    }

    fn handle_list_databases(&self) -> ServerMessage {
        let Some(claims) = self.auth_claims.as_ref() else {
            return error("auth_required", "authentication required");
        };

        match list_database_metadata_json(&self.registry, claims) {
            Ok(databases) => ok(json!({ "databases": databases })),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_create_collection(&self, database: &str, name: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.create_collection(name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_collection(&self, database: &str, name: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.drop_collection(name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    fn handle_list_collections(&self, database: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let collections: Vec<Value> = db
            .list_collections()
            .into_iter()
            .map(|meta| {
                json!({
                    "id": meta.collection_id.0,
                    "name": meta.name,
                    "doc_count": meta.doc_count,
                })
            })
            .collect();
        ok(json!({ "collections": collections }))
    }

    async fn handle_create_index(
        &self,
        database: &str,
        collection: &str,
        fields: Vec<Value>,
        name: Option<String>,
    ) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let fields = match parse_field_paths(fields) {
            Ok(fields) => fields,
            Err(message) => return error("invalid_field_path", message),
        };
        let name = name.unwrap_or_else(|| default_index_name(&fields));

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.create_index(collection, &name, fields).await {
            return database_error_response(err);
        }
        if let Err(err) = tx.commit().await {
            return database_error_response(err);
        }

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        match tx.list_indexes(collection) {
            Ok(indexes) => {
                let index = indexes.into_iter().find(|meta| meta.name == name);
                match index {
                    Some(index) => ok(json!({ "index_id": index.index_id.0 })),
                    None => error("internal", "created index was not visible after commit"),
                }
            }
            Err(err) => database_error_response(err),
        }
    }

    async fn handle_drop_index(
        &self,
        database: &str,
        collection: &str,
        name: &str,
    ) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        if let Err(err) = tx.drop_index(collection, name).await {
            return database_error_response(err);
        }
        match tx.commit().await {
            Ok(_) => ok(json!({})),
            Err(err) => database_error_response(err),
        }
    }

    fn handle_list_indexes(&self, database: &str, collection: &str) -> ServerMessage {
        if let Err(response) = self.ensure_database_access(database) {
            return response;
        }

        let Some(db) = self.registry.get_database_by_name(database) else {
            return error(
                "unknown_database",
                format!("database not found: {database}"),
            );
        };

        let mut tx = match db.begin(Default::default()) {
            Ok(tx) => tx,
            Err(err) => return database_error_response(err),
        };
        match tx.list_indexes(collection) {
            Ok(indexes) => {
                let indexes: Vec<Value> = indexes
                    .into_iter()
                    .map(|meta| {
                        json!({
                            "id": meta.index_id.0,
                            "name": meta.name,
                            "fields": meta.field_paths.iter().map(field_path_json).collect::<Vec<_>>(),
                            "state": format!("{:?}", meta.state).to_lowercase(),
                        })
                    })
                    .collect();
                ok(json!({ "indexes": indexes }))
            }
            Err(err) => database_error_response(err),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        for (_, active) in self.active_transactions.drain() {
            active.tx.rollback();
        }
        self.remove_session_subscriptions();
        if let Some(task) = &self.auth_expiry_task {
            task.abort();
        }
        for task in &self.notification_tasks {
            task.abort();
        }
    }
}

async fn forward_subscription_events(
    mut handle: SubscriptionHandle,
    subscribed_wire_tx: u64,
    tx: mpsc::Sender<SessionNotification>,
    db: Option<Arc<Database>>,
    database_name: Option<String>,
    opts: Option<TransactionOptions>,
    next_wire_tx_id: Arc<AtomicU64>,
) {
    while let Some(event) = handle.next_event().await {
        let mut continuation_tx = None;
        let mut new_tx = None;
        let mut new_ts = None;

        if let Some(continuation) = event.continuation {
            let wire_new_tx = next_wire_tx_id.fetch_add(1, Ordering::Relaxed);
            new_tx = Some(wire_new_tx);
            new_ts = Some(continuation.new_ts);
            if let (Some(db), Some(database_name), Some(opts)) = (&db, &database_name, &opts) {
                match db.begin_subscription_continuation(continuation, opts.clone()) {
                    Ok(tx) => {
                        continuation_tx = Some((
                            wire_new_tx,
                            ActiveTransaction {
                                tx,
                                db: Arc::clone(db),
                                database_name: database_name.clone(),
                                opts: opts.clone(),
                            },
                        ));
                    }
                    Err(err) => {
                        let _ = tx
                            .send(SessionNotification {
                                message: database_error_response(err),
                                continuation: None,
                            })
                            .await;
                        break;
                    }
                }
            }
        }

        let msg = ServerMessage::Invalidation {
            tx: subscribed_wire_tx,
            queries: event.affected_query_ids,
            commit_ts: event.commit_ts,
            new_tx,
            new_ts,
        };
        if tx
            .send(SessionNotification {
                message: msg,
                continuation: continuation_tx,
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn forward_index_ready_events(
    mut rx: broadcast::Receiver<IndexReadyEvent>,
    tx: mpsc::Sender<SessionNotification>,
    claims: AuthClaims,
) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                if !check_database_access(&claims, &event.database) {
                    continue;
                }
                let message = ServerMessage::IndexReady {
                    database: event.database,
                    collection: event.collection,
                    index: event.index,
                    index_id: event.index_id.0,
                };
                if tx
                    .send(SessionNotification {
                        message,
                        continuation: None,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

pub(crate) struct ActiveTransaction {
    tx: Transaction,
    db: Arc<Database>,
    database_name: String,
    opts: TransactionOptions,
}

/// Merge a JSON database config override onto a base `DatabaseConfig`.
pub fn database_config_from_json(
    value: Option<Value>,
    base: &DatabaseConfig,
) -> std::result::Result<DatabaseConfig, String> {
    let Some(value) = value else {
        return Ok(base.clone());
    };
    let Value::Object(object) = value else {
        return Err("config must be an object".to_string());
    };

    let mut config = base.clone();
    for (key, value) in object {
        match key.as_str() {
            "page_size" => config.page_size = parse_usize_config(&key, value)?,
            "memory_budget" => config.memory_budget = parse_usize_config(&key, value)?,
            "max_disk_usage_bytes" => {
                config.max_disk_usage_bytes = parse_optional_u64_config(&key, value)?
            }
            "max_doc_size" => config.max_doc_size = parse_usize_config(&key, value)?,
            "external_threshold" => config.external_threshold = parse_usize_config(&key, value)?,
            "wal_segment_size" => config.wal_segment_size = parse_usize_config(&key, value)?,
            "wal_retention_max_size" => {
                config.wal_retention_max_size = parse_optional_u64_config(&key, value)?
            }
            "wal_retention_max_age" => {
                config.wal_retention_max_age = parse_optional_duration_config(&key, value)?
            }
            "checkpoint_wal_threshold" => {
                config.checkpoint_wal_threshold = parse_usize_config(&key, value)?
            }
            "checkpoint_interval" => {
                config.checkpoint_interval = parse_duration_config(&key, value)?
            }
            "vacuum_interval" => config.vacuum_interval = parse_duration_config(&key, value)?,
            "close_timeout" => config.close_timeout = parse_duration_config(&key, value)?,
            "check_on_startup" => config.check_on_startup = parse_bool_config(&key, value)?,
            "check_on_startup_full" => {
                config.check_on_startup_full = parse_bool_config(&key, value)?
            }
            "transaction" | "transactions" => {
                apply_transaction_config(&mut config.transaction, value)?
            }
            "tx_idle_timeout" => {
                config.transaction.idle_timeout = parse_duration_config(&key, value)?
            }
            "tx_max_lifetime" => {
                config.transaction.max_lifetime = parse_duration_config(&key, value)?
            }
            other => return Err(format!("unsupported database config field '{other}'")),
        }
    }
    config.validate()?;
    Ok(config)
}

fn apply_transaction_config(
    config: &mut exdb::TransactionConfig,
    value: Value,
) -> std::result::Result<(), String> {
    let Value::Object(object) = value else {
        return Err("config.transaction must be an object".to_string());
    };
    for (key, value) in object {
        match key.as_str() {
            "idle_timeout" => config.idle_timeout = parse_duration_config(&key, value)?,
            "max_lifetime" => config.max_lifetime = parse_duration_config(&key, value)?,
            "max_intervals" => config.max_intervals = parse_usize_config(&key, value)?,
            "max_operations" => config.max_operations = parse_usize_config(&key, value)?,
            "max_scanned_bytes" => config.max_scanned_bytes = parse_usize_config(&key, value)?,
            "max_scanned_docs" => config.max_scanned_docs = parse_usize_config(&key, value)?,
            other => return Err(format!("unsupported transaction config field '{other}'")),
        }
    }
    Ok(())
}

fn parse_optional_u64_config(
    field: &str,
    value: Value,
) -> std::result::Result<Option<u64>, String> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| format!("config.{field} must be an unsigned integer or null"))
}

fn parse_optional_duration_config(
    field: &str,
    value: Value,
) -> std::result::Result<Option<Duration>, String> {
    if value.is_null() {
        return Ok(None);
    }
    parse_duration_config(field, value).map(Some)
}

fn parse_usize_config(field: &str, value: Value) -> std::result::Result<usize, String> {
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| format!("config.{field} must be an unsigned integer"))
}

fn parse_bool_config(field: &str, value: Value) -> std::result::Result<bool, String> {
    value
        .as_bool()
        .ok_or_else(|| format!("config.{field} must be a boolean"))
}

fn parse_duration_config(field: &str, value: Value) -> std::result::Result<Duration, String> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .map(Duration::from_millis)
            .ok_or_else(|| format!("config.{field} must be a duration string or milliseconds")),
        Value::String(value) => {
            parse_duration_string(&value).map_err(|message| format!("config.{field} {message}"))
        }
        _ => Err(format!(
            "config.{field} must be a duration string or milliseconds"
        )),
    }
}

fn parse_duration_string(value: &str) -> std::result::Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration cannot be empty".to_string());
    }

    let suffix_start = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(suffix_start);
    if number.is_empty() {
        return Err("duration must start with a number".to_string());
    }
    let amount = number
        .parse::<u64>()
        .map_err(|_| "duration amount is too large".to_string())?;
    match suffix {
        "" | "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| "duration is too large".to_string()),
        "h" => amount
            .checked_mul(60 * 60)
            .map(Duration::from_secs)
            .ok_or_else(|| "duration is too large".to_string()),
        _ => Err("duration suffix must be ms, s, m, or h".to_string()),
    }
}

fn ok(fields: Value) -> ServerMessage {
    ServerMessage::Ok { fields }
}

fn response_code(msg: &ServerMessage) -> Option<&str> {
    match msg {
        ServerMessage::Error { code, .. } => Some(code.as_str()),
        _ => None,
    }
}

fn error(code: impl Into<String>, message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error {
        code: code.into(),
        message: message.into(),
        extra: None,
    }
}

fn readonly_node_response(message: impl Into<String>) -> ServerMessage {
    error("readonly_node", message)
}

fn is_replica_role(role: &str) -> bool {
    role == NODE_ROLE_REPLICA
}

fn is_management_write(msg: &ClientMessage) -> bool {
    matches!(
        msg,
        ClientMessage::CreateDatabase { .. }
            | ClientMessage::DropDatabase { .. }
            | ClientMessage::CreateCollection { .. }
            | ClientMessage::DropCollection { .. }
            | ClientMessage::CreateIndex { .. }
            | ClientMessage::DropIndex { .. }
    )
}

fn database_error_response(err: DatabaseError) -> ServerMessage {
    if err.is_invalid_range() {
        return error("invalid_range", err.to_string());
    }

    let code = match &err {
        DatabaseError::DatabaseNotFound(_) => "unknown_database",
        DatabaseError::DatabaseAlreadyExists(_) => "database_exists",
        DatabaseError::DatabaseInUse(_) => "database_in_use",
        DatabaseError::CollectionNotFound(_) => "unknown_collection",
        DatabaseError::CollectionAlreadyExists(_) => "collection_exists",
        DatabaseError::IndexNotFound { .. } => "unknown_index",
        DatabaseError::IndexAlreadyExists { .. } => "index_exists",
        DatabaseError::IndexNotReady(_) => "index_not_ready",
        DatabaseError::SystemIndex(_) => "invalid_message",
        DatabaseError::DocNotFound => "doc_not_found",
        DatabaseError::DocTooLarge { .. } => "doc_too_large",
        DatabaseError::ReadonlyWrite => "readonly_tx",
        DatabaseError::ReadLimitExceeded(_) => "read_limit_exceeded",
        DatabaseError::TransactionTimeout => "tx_timeout",
        DatabaseError::ShuttingDown => "shutting_down",
        DatabaseError::QuorumLost => "quorum_lost",
        DatabaseError::InvalidConfig(_) => "invalid_message",
        DatabaseError::InvalidFieldPath(_) => "invalid_field_path",
        DatabaseError::InvalidName(_) | DatabaseError::ReservedName(_) => "invalid_message",
        DatabaseError::IntegrityCheckFailed { .. } => "database_corrupt",
        _ => "internal",
    };
    error(code, err.to_string())
}

fn list_database_metadata_json(
    registry: &SystemDatabase,
    claims: &AuthClaims,
) -> std::result::Result<Vec<Value>, DatabaseError> {
    registry
        .list_databases()
        .into_iter()
        .filter(|meta| check_database_access(claims, &meta.name))
        .map(|meta| {
            let usage = registry.database_usage(&meta.name)?;
            Ok(database_meta_json(meta, usage))
        })
        .collect()
}

fn database_meta_json(meta: DatabaseMeta, usage: DatabaseUsage) -> Value {
    json!({
        "id": meta.database_id,
        "name": meta.name,
        "path": meta.path,
        "created_at": meta.created_at,
        "state": database_state_str(meta.state),
        "usage": database_usage_json(usage),
    })
}

fn database_usage_json(usage: DatabaseUsage) -> Value {
    json!({
        "disk_usage_bytes": usage.disk_usage_bytes,
        "page_store_bytes": usage.page_store_bytes,
        "wal_retained_bytes": usage.wal_retained_bytes,
        "memory_budget_bytes": usage.memory_budget_bytes,
        "buffer_pool_used_frames": usage.buffer_pool_used_frames,
        "active_transactions": usage.active_transactions,
        "page_count": usage.page_count,
        "page_size": usage.page_size,
    })
}

fn database_state_str(state: DatabaseState) -> &'static str {
    match state {
        DatabaseState::Active => "active",
        DatabaseState::Creating => "creating",
        DatabaseState::Dropping => "dropping",
    }
}

fn field_path_json(path: &FieldPath) -> Value {
    if path.segments().len() == 1 {
        Value::from(path.segments()[0].clone())
    } else {
        Value::Array(path.segments().iter().cloned().map(Value::from).collect())
    }
}

fn parse_field_paths(values: Vec<Value>) -> std::result::Result<Vec<FieldPath>, String> {
    if values.is_empty() {
        return Err("fields must contain at least one field path".to_string());
    }

    values
        .into_iter()
        .map(parse_field_path)
        .collect::<std::result::Result<Vec<_>, _>>()
}

fn parse_field_path(value: Value) -> std::result::Result<FieldPath, String> {
    match value {
        Value::String(segment) => {
            if segment.is_empty() {
                return Err("field path segment cannot be empty".to_string());
            }
            Ok(FieldPath::single(&segment))
        }
        Value::Array(segments) => {
            if segments.is_empty() {
                return Err("nested field path cannot be empty".to_string());
            }
            let mut parsed = Vec::with_capacity(segments.len());
            for segment in segments {
                let Value::String(segment) = segment else {
                    return Err("nested field path segments must be strings".to_string());
                };
                if segment.is_empty() {
                    return Err("field path segment cannot be empty".to_string());
                }
                parsed.push(segment);
            }
            Ok(FieldPath::new(parsed))
        }
        _ => Err("field path must be a string or array of strings".to_string()),
    }
}

fn default_index_name(fields: &[FieldPath]) -> String {
    let encoded = fields
        .iter()
        .map(|field| field.segments().join("_"))
        .collect::<Vec<_>>()
        .join("__");
    format!("idx_{encoded}")
}

fn parse_range_exprs(
    values: Vec<Value>,
    type_hints: Option<&Value>,
) -> std::result::Result<Vec<RangeExpr>, String> {
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| parse_range_expr(value, array_hint(type_hints, index)))
        .collect::<std::result::Result<Vec<_>, _>>()
}

fn parse_range_expr(
    value: Value,
    type_hint: Option<&Value>,
) -> std::result::Result<RangeExpr, String> {
    let (op, args) = parse_single_operator_object(value, "range expression")?;
    let scalar_hint = operator_hint(type_hint, &op);
    let (field, scalar) = parse_field_scalar_args(args, &op, scalar_hint)?;
    match op.as_str() {
        "eq" => Ok(RangeExpr::Eq(field, scalar)),
        "gt" => Ok(RangeExpr::Gt(field, scalar)),
        "gte" => Ok(RangeExpr::Gte(field, scalar)),
        "lt" => Ok(RangeExpr::Lt(field, scalar)),
        "lte" => Ok(RangeExpr::Lte(field, scalar)),
        _ => Err(format!("unsupported range operator '{op}'")),
    }
}

fn parse_filter(value: Value, type_hint: Option<&Value>) -> std::result::Result<Filter, String> {
    let (op, args) = parse_single_operator_object(value, "filter")?;
    match op.as_str() {
        "eq" | "ne" | "gt" | "gte" | "lt" | "lte" => {
            let scalar_hint = operator_hint(type_hint, &op);
            let (field, scalar) = parse_field_scalar_args(args, &op, scalar_hint)?;
            match op.as_str() {
                "eq" => Ok(Filter::Eq(field, scalar)),
                "ne" => Ok(Filter::Ne(field, scalar)),
                "gt" => Ok(Filter::Gt(field, scalar)),
                "gte" => Ok(Filter::Gte(field, scalar)),
                "lt" => Ok(Filter::Lt(field, scalar)),
                "lte" => Ok(Filter::Lte(field, scalar)),
                _ => unreachable!(),
            }
        }
        "in" => {
            let Value::Array(mut args) = args else {
                return Err("filter operator 'in' expects [field, values]".to_string());
            };
            if args.len() != 2 {
                return Err("filter operator 'in' expects [field, values]".to_string());
            }
            let values = args.pop().unwrap();
            let field = parse_field_path(args.pop().unwrap())?;
            let Value::Array(values) = values else {
                return Err("filter operator 'in' values must be an array".to_string());
            };
            let value_hints = operator_hint(type_hint, "in");
            let values = values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    parse_scalar_with_hint(value, in_value_hint(value_hints, index))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Filter::In(field, values))
        }
        "and" | "or" => {
            let Value::Array(values) = args else {
                return Err(format!("filter operator '{op}' expects an array"));
            };
            let filters = values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    parse_filter(value, array_hint(operator_hint(type_hint, &op), index))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if op == "and" {
                Ok(Filter::And(filters))
            } else {
                Ok(Filter::Or(filters))
            }
        }
        "not" => Ok(Filter::Not(Box::new(parse_filter(
            args,
            operator_hint(type_hint, "not"),
        )?))),
        _ => Err(format!("unsupported filter operator '{op}'")),
    }
}

fn parse_single_operator_object(
    value: Value,
    context: &str,
) -> std::result::Result<(String, Value), String> {
    let Value::Object(object) = value else {
        return Err(format!("{context} must be an object"));
    };
    if object.len() != 1 {
        return Err(format!("{context} must contain exactly one operator"));
    }
    Ok(object.into_iter().next().unwrap())
}

fn parse_field_scalar_args(
    value: Value,
    op: &str,
    type_hint: Option<&Value>,
) -> std::result::Result<(FieldPath, Scalar), String> {
    let Value::Array(mut args) = value else {
        return Err(format!("operator '{op}' expects [field, value]"));
    };
    if args.len() != 2 {
        return Err(format!("operator '{op}' expects [field, value]"));
    }
    let scalar = parse_scalar_with_hint(args.pop().unwrap(), type_hint)?;
    let field = parse_field_path(args.pop().unwrap())?;
    Ok((field, scalar))
}

fn parse_scalar(value: Value) -> std::result::Result<Scalar, String> {
    parse_scalar_with_hint(value, None)
}

fn parse_scalar_with_hint(
    value: Value,
    type_hint: Option<&Value>,
) -> std::result::Result<Scalar, String> {
    if let Some(hint) = type_hint.and_then(Value::as_str) {
        return parse_typed_scalar(value, hint);
    }

    match value {
        Value::Null => Ok(Scalar::Null),
        Value::Bool(value) => Ok(Scalar::Boolean(value)),
        Value::String(value) => Ok(Scalar::String(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Scalar::Int64(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value)
                    .map(Scalar::Int64)
                    .map_err(|_| "unsigned integer scalar exceeds int64 range".to_string())
            } else if let Some(value) = value.as_f64() {
                Ok(Scalar::Float64(value))
            } else {
                Err("unsupported numeric scalar".to_string())
            }
        }
        Value::Array(_) | Value::Object(_) => {
            Err("array and object values are not valid scalar predicates".to_string())
        }
    }
}

fn parse_typed_scalar(value: Value, hint: &str) -> std::result::Result<Scalar, String> {
    match hint {
        "bytes" => {
            let Value::String(value) = value else {
                return Err("_meta.types bytes predicate values must be strings".to_string());
            };
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .map(Scalar::Bytes)
                .map_err(|err| format!("invalid base64 bytes predicate: {err}"))
        }
        "id" => {
            let Value::String(value) = value else {
                return Err("_meta.types id predicate values must be strings".to_string());
            };
            decode_ulid(&value).map(Scalar::Id)
        }
        "int64" => parse_int64_scalar(value),
        "float64" => match value {
            Value::Number(value) => value
                .as_f64()
                .map(Scalar::Float64)
                .ok_or_else(|| "_meta.types float64 predicate values must be numbers".to_string()),
            _ => Err("_meta.types float64 predicate values must be numbers".to_string()),
        },
        "null" | "boolean" | "string" => parse_scalar(value),
        other => Err(format!("unsupported _meta.types predicate hint '{other}'")),
    }
}

fn parse_int64_scalar(value: Value) -> std::result::Result<Scalar, String> {
    let Value::Number(value) = value else {
        return Err("_meta.types int64 predicate values must be numbers".to_string());
    };
    if let Some(value) = value.as_i64() {
        Ok(Scalar::Int64(value))
    } else if let Some(value) = value.as_u64() {
        i64::try_from(value)
            .map(Scalar::Int64)
            .map_err(|_| "unsigned integer scalar exceeds int64 range".to_string())
    } else {
        Err("_meta.types int64 predicate values must be integral numbers".to_string())
    }
}

fn operator_hint<'a>(type_hint: Option<&'a Value>, op: &str) -> Option<&'a Value> {
    match type_hint {
        Some(Value::Object(map)) => map.get(op),
        Some(Value::String(_)) | Some(Value::Array(_)) => type_hint,
        _ => None,
    }
}

fn array_hint(type_hint: Option<&Value>, index: usize) -> Option<&Value> {
    match type_hint {
        Some(Value::Array(values)) => values.get(index),
        Some(Value::Object(_)) | Some(Value::String(_)) => type_hint,
        _ => None,
    }
}

fn in_value_hint(type_hint: Option<&Value>, index: usize) -> Option<&Value> {
    match type_hint {
        Some(Value::Array(values)) => values.get(index),
        Some(Value::String(_)) => type_hint,
        _ => None,
    }
}

fn parse_order(order: Option<&str>) -> std::result::Result<Option<ScanDirection>, String> {
    match order {
        None | Some("asc") => Ok(Some(ScanDirection::Forward)),
        Some("desc") => Ok(Some(ScanDirection::Backward)),
        Some(other) => Err(format!("unsupported query order '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{JwtAlgorithm, Role};
    use std::sync::Mutex;
    use tempfile::tempdir;

    struct RecordingPromoter {
        seen: Mutex<Vec<(String, exdb::Ts, Vec<u8>)>>,
        response: Mutex<std::result::Result<TransactionPromotionOutcome, String>>,
        ddl_seen: Mutex<Vec<DdlPromotionRequest>>,
        ddl_response: Mutex<std::result::Result<ServerMessage, String>>,
    }

    impl RecordingPromoter {
        fn success(commit_ts: exdb::Ts) -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                response: Mutex::new(Ok(TransactionPromotionOutcome::Success { commit_ts })),
                ddl_seen: Mutex::new(Vec::new()),
                ddl_response: Mutex::new(Ok(ok(json!({})))),
            }
        }

        fn conflict_with_retry(new_tx: u64, new_ts: u64) -> Self {
            Self {
                seen: Mutex::new(Vec::new()),
                response: Mutex::new(Ok(TransactionPromotionOutcome::Conflict {
                    message: "remote OCC conflict".to_string(),
                    extra: Some(json!({
                        "new_tx": new_tx,
                        "new_ts": new_ts,
                    })),
                })),
                ddl_seen: Mutex::new(Vec::new()),
                ddl_response: Mutex::new(Ok(ok(json!({})))),
            }
        }

        fn with_ddl_response(self, response: ServerMessage) -> Self {
            *self.ddl_response.lock().unwrap() = Ok(response);
            self
        }
    }

    #[async_trait::async_trait]
    impl TransactionPromoter for RecordingPromoter {
        async fn promote_transaction(
            &self,
            database: &str,
            begin_ts: exdb::Ts,
            _subscription: SubscriptionMode,
            payload: Vec<u8>,
        ) -> std::result::Result<TransactionPromotionOutcome, String> {
            self.seen
                .lock()
                .unwrap()
                .push((database.to_string(), begin_ts, payload));
            self.response.lock().unwrap().clone()
        }

        async fn promote_ddl(
            &self,
            request: DdlPromotionRequest,
        ) -> std::result::Result<ServerMessage, String> {
            self.ddl_seen.lock().unwrap().push(request);
            self.ddl_response.lock().unwrap().clone()
        }
    }

    struct StaticReplicaReadGate(bool);

    impl ReplicaReadGate for StaticReplicaReadGate {
        fn has_read_quorum(&self, _database: &str) -> bool {
            self.0
        }
    }

    async fn registry() -> Arc<SystemDatabase> {
        static TEMP_DIRS: std::sync::Mutex<Vec<tempfile::TempDir>> =
            std::sync::Mutex::new(Vec::new());
        let tmp = tempdir().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        TEMP_DIRS.lock().unwrap().push(tmp);
        registry
    }

    fn error_code(msg: &ServerMessage) -> &str {
        match msg {
            ServerMessage::Error { code, .. } => code,
            other => panic!("expected error, got {other:?}"),
        }
    }

    fn ok_fields(msg: &ServerMessage) -> &Value {
        match msg {
            ServerMessage::Ok { fields } => fields,
            other => panic!("expected ok, got {other:?}"),
        }
    }

    async fn create_app_users(session: &mut Session) {
        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();
    }

    async fn wait_for_session_index_ready(
        session: &mut Session,
        collection: &str,
        index: &str,
        first_msg_id: u32,
    ) -> u32 {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut msg_id = first_msg_id;
        loop {
            let list = session
                .handle_message(
                    msg_id,
                    ClientMessage::ListIndexes {
                        database: "app".to_string(),
                        collection: collection.to_string(),
                    },
                )
                .await
                .unwrap();
            msg_id += 1;
            let indexes = ok_fields(&list)["indexes"].as_array().unwrap();
            if indexes
                .iter()
                .any(|meta| meta["name"] == index && meta["state"] == "ready")
            {
                return msg_id;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("index {collection}.{index} did not become ready");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    #[test]
    fn query_meta_type_hints_parse_all_typed_scalar_kinds() {
        let id = "00000000000000000000000000";
        let range_hints = json!([{"eq": "id"}]);
        let range =
            parse_range_exprs(vec![json!({"eq": ["owner", id]})], Some(&range_hints)).unwrap();
        assert_eq!(
            range,
            vec![RangeExpr::Eq(
                FieldPath::single("owner"),
                Scalar::Id(decode_ulid(id).unwrap())
            )]
        );

        let filter_hints = json!({
            "and": [
                {"eq": "bytes"},
                {"gte": "int64"},
                {"lt": "float64"}
            ]
        });
        let filter = parse_filter(
            json!({
                "and": [
                    {"eq": ["blob", "AAEC"]},
                    {"gte": ["count", 42]},
                    {"lt": ["score", 42]}
                ]
            }),
            Some(&filter_hints),
        )
        .unwrap();
        assert_eq!(
            filter,
            Filter::And(vec![
                Filter::Eq(FieldPath::single("blob"), Scalar::Bytes(vec![0, 1, 2])),
                Filter::Gte(FieldPath::single("count"), Scalar::Int64(42)),
                Filter::Lt(FieldPath::single("score"), Scalar::Float64(42.0)),
            ])
        );
    }

    #[tokio::test]
    async fn hello_reflects_auth_config() {
        let session = Session::new(
            7,
            registry().await,
            AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
        );

        assert_eq!(session.id(), 7);
        assert_eq!(
            session.hello(),
            ServerMessage::Hello {
                version: env!("CARGO_PKG_VERSION").to_string(),
                encodings: vec![
                    "json".to_string(),
                    "bson".to_string(),
                    "protobuf".to_string(),
                ],
                auth_required: true,
                node_role: "primary".to_string(),
                max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            }
        );
    }

    #[tokio::test]
    async fn auth_required_before_any_non_auth_message() {
        let mut session = Session::new(
            1,
            registry().await,
            AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
        );

        let response = session
            .handle_message(1, ClientMessage::Ping)
            .await
            .unwrap();

        assert_eq!(error_code(&response), "auth_required");
        assert!(!session.is_authenticated());
    }

    #[tokio::test]
    async fn disabled_auth_allows_ping() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        let response = session
            .handle_message(1, ClientMessage::Ping)
            .await
            .unwrap();

        assert_eq!(response, ServerMessage::Pong);
        assert!(session.is_authenticated());
        assert_eq!(session.auth_claims().unwrap().role, Role::Admin);
    }

    #[tokio::test]
    async fn expired_authenticated_session_returns_auth_expired() {
        let mut session = Session::new(
            1,
            registry().await,
            AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
        );
        session.authenticated = true;
        session.auth_claims = Some(AuthClaims {
            sub: Some("svc".to_string()),
            databases: None,
            role: Role::Admin,
            exp: current_unix_secs().saturating_sub(1),
            issuer: None,
        });

        let response = session
            .handle_message(1, ClientMessage::Ping)
            .await
            .unwrap();

        assert_eq!(error_code(&response), "auth_expired");
        assert!(!session.is_authenticated());

        let response = session
            .handle_message(2, ClientMessage::Ping)
            .await
            .unwrap();
        assert_eq!(error_code(&response), "auth_required");
    }

    #[tokio::test]
    async fn bad_token_returns_auth_failed() {
        let mut session = Session::new(
            1,
            registry().await,
            AuthConfig::hmac(JwtAlgorithm::HS256, b"secret".to_vec()),
        );

        let response = session
            .handle_message(
                1,
                ClientMessage::Authenticate {
                    token: "not-a-token".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "auth_failed");
        assert!(!session.is_authenticated());
    }

    #[tokio::test]
    async fn duplicate_message_id_is_ignored() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        assert!(
            session
                .handle_message(1, ClientMessage::Ping)
                .await
                .is_some()
        );
        assert!(
            session
                .handle_message(1, ClientMessage::Ping)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn skipped_message_id_is_rejected_and_reserved() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        assert_eq!(
            session
                .handle_message(1, ClientMessage::Ping)
                .await
                .unwrap(),
            ServerMessage::Pong
        );
        let response = session
            .handle_message(3, ClientMessage::Ping)
            .await
            .expect("skipped id should receive a protocol error");

        assert_eq!(error_code(&response), "invalid_message");
        match response {
            ServerMessage::Error { message, .. } => {
                assert!(message.contains("must be the next incrementing value"));
                assert!(message.contains("expected 2"));
            }
            other => panic!("expected error response, got {other:?}"),
        }
        assert_eq!(
            session
                .handle_message(2, ClientMessage::Ping)
                .await
                .unwrap(),
            ServerMessage::Pong
        );
        assert!(
            session
                .handle_message(3, ClientMessage::Ping)
                .await
                .is_none()
        );
        assert_eq!(
            session
                .handle_message(4, ClientMessage::Ping)
                .await
                .unwrap(),
            ServerMessage::Pong
        );
    }

    #[tokio::test]
    async fn admin_can_create_list_and_drop_database() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        let create = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({"page_size": 4096})),
                },
            )
            .await
            .unwrap();
        assert!(matches!(create, ServerMessage::Ok { .. }));

        let list = session
            .handle_message(2, ClientMessage::ListDatabases)
            .await
            .unwrap();
        let databases = ok_fields(&list)["databases"].as_array().unwrap();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0]["name"], "app");
        assert!(databases[0]["usage"]["disk_usage_bytes"].as_u64().unwrap() > 0);
        assert_eq!(databases[0]["usage"]["page_size"], 4096);
        assert_eq!(databases[0]["usage"]["active_transactions"], 0);

        let drop = session
            .handle_message(
                3,
                ClientMessage::DropDatabase {
                    name: "app".to_string(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(drop, ServerMessage::Ok { .. }));
    }

    #[tokio::test]
    async fn create_database_uses_session_default_database_config() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        let default_config = DatabaseConfig {
            max_doc_size: 4096,
            max_disk_usage_bytes: Some(64 * 1024 * 1024),
            memory_budget: 16 * 1024 * 1024,
            wal_retention_max_size: Some(32 * 1024 * 1024),
            wal_retention_max_age: Some(Duration::from_secs(60)),
            close_timeout: Duration::from_secs(3),
            transaction: exdb::TransactionConfig {
                idle_timeout: Duration::from_secs(7),
                max_scanned_docs: 123,
                max_operations: 321,
                ..Default::default()
            },
            ..Default::default()
        };
        session.set_default_database_config(default_config);

        let create = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();
        assert!(matches!(create, ServerMessage::Ok { .. }));

        let database = registry
            .list_databases()
            .into_iter()
            .find(|meta| meta.name == "app")
            .unwrap();
        assert_eq!(database.config.max_doc_size, 4096);
        assert_eq!(database.config.max_disk_usage_bytes, Some(64 * 1024 * 1024));
        assert_eq!(database.config.memory_budget, 16 * 1024 * 1024);
        assert_eq!(database.config.close_timeout, Duration::from_secs(3));
        assert_eq!(
            database.config.transaction.idle_timeout,
            Duration::from_secs(7)
        );
        assert_eq!(database.config.transaction.max_scanned_docs, 123);
        assert_eq!(database.config.transaction.max_operations, 321);
    }

    #[tokio::test]
    async fn create_database_merges_request_config_over_session_default() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        let default_config = DatabaseConfig {
            max_doc_size: 4096,
            max_disk_usage_bytes: Some(64 * 1024 * 1024),
            memory_budget: 16 * 1024 * 1024,
            close_timeout: Duration::from_secs(3),
            transaction: exdb::TransactionConfig {
                idle_timeout: Duration::from_secs(7),
                max_lifetime: Duration::from_secs(99),
                max_scanned_docs: 123,
                max_operations: 321,
                ..Default::default()
            },
            ..Default::default()
        };
        session.set_default_database_config(default_config);

        let create = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({
                        "max_doc_size": 8192,
                        "max_disk_usage_bytes": 128 * 1024 * 1024u64,
                        "wal_retention_max_size": 16 * 1024 * 1024u64,
                        "wal_retention_max_age": "5s",
                        "close_timeout": "4s",
                        "transaction": {
                            "idle_timeout": "2s",
                            "max_operations": 456,
                            "max_scanned_docs": 456
                        }
                    })),
                },
            )
            .await
            .unwrap();
        assert!(matches!(create, ServerMessage::Ok { .. }));

        let database = registry
            .list_databases()
            .into_iter()
            .find(|meta| meta.name == "app")
            .unwrap();
        assert_eq!(database.config.max_doc_size, 8192);
        assert_eq!(
            database.config.max_disk_usage_bytes,
            Some(128 * 1024 * 1024)
        );
        assert_eq!(
            database.config.wal_retention_max_size,
            Some(16 * 1024 * 1024)
        );
        assert_eq!(
            database.config.wal_retention_max_age,
            Some(Duration::from_secs(5))
        );
        assert_eq!(database.config.memory_budget, 16 * 1024 * 1024);
        assert_eq!(database.config.close_timeout, Duration::from_secs(4));
        assert_eq!(
            database.config.transaction.idle_timeout,
            Duration::from_secs(2)
        );
        assert_eq!(
            database.config.transaction.max_lifetime,
            Duration::from_secs(99)
        );
        assert_eq!(database.config.transaction.max_scanned_docs, 456);
        assert_eq!(database.config.transaction.max_operations, 456);
    }

    #[test]
    fn database_config_from_json_rejects_unknown_transaction_fields() {
        let err = database_config_from_json(
            Some(json!({"transaction": {"unknown": 1}})),
            &DatabaseConfig::default(),
        )
        .unwrap_err();

        assert!(err.contains("unsupported transaction config field 'unknown'"));
    }

    #[test]
    fn database_config_from_json_rejects_invalid_resource_values() {
        let err = database_config_from_json(
            Some(json!({"page_size": 4096, "memory_budget": 1024})),
            &DatabaseConfig::default(),
        )
        .unwrap_err();

        assert!(err.contains("memory_budget must be at least page_size"));
    }

    #[test]
    fn database_config_from_json_rejects_invalid_external_threshold() {
        let err = database_config_from_json(
            Some(json!({"page_size": 4096, "external_threshold": 4096})),
            &DatabaseConfig::default(),
        )
        .unwrap_err();

        assert!(err.contains("external_threshold 4096 exceeds maximum inline primary body size"));
    }

    #[test]
    fn database_config_from_json_rejects_bootstrap_disk_quota() {
        let err = database_config_from_json(
            Some(json!({"page_size": 4096, "max_disk_usage_bytes": 4096 * 4 + 31})),
            &DatabaseConfig::default(),
        )
        .unwrap_err();

        assert!(err.contains(
            "max_disk_usage_bytes 16415 is smaller than minimum durable database bootstrap usage 16416"
        ));
    }

    #[test]
    fn database_config_from_json_rejects_wal_segment_header_only_size() {
        let err = database_config_from_json(
            Some(json!({"wal_segment_size": 32})),
            &DatabaseConfig::default(),
        )
        .unwrap_err();

        assert!(
            err.contains("wal_segment_size 32 must be greater than WAL segment header size 32")
        );
    }

    #[tokio::test]
    async fn create_database_rejects_invalid_config_without_creating_entry() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());

        let response = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "bad".to_string(),
                    config: Some(json!({"transaction": {"max_operations": 0}})),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "invalid_message");
        assert!(registry.get_database_by_name("bad").is_none());
    }

    #[tokio::test]
    async fn collection_management_uses_database_access() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();

        let create = session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(create, ServerMessage::Ok { .. }));

        let list = session
            .handle_message(
                3,
                ClientMessage::ListCollections {
                    database: "app".to_string(),
                },
            )
            .await
            .unwrap();
        let collections = ok_fields(&list)["collections"].as_array().unwrap();
        assert!(collections.iter().any(|coll| coll["name"] == "users"));
    }

    #[tokio::test]
    async fn index_management_creates_lists_and_drops_indexes() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();

        let create = session
            .handle_message(
                3,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!("status"), json!(["address", "city"])],
                    name: None,
                },
            )
            .await
            .unwrap();
        assert!(ok_fields(&create)["index_id"].as_u64().unwrap() > 0);

        let list = session
            .handle_message(
                4,
                ClientMessage::ListIndexes {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                },
            )
            .await
            .unwrap();
        let indexes = ok_fields(&list)["indexes"].as_array().unwrap();
        let created = indexes
            .iter()
            .find(|index| index["name"] == "idx_status__address_city")
            .unwrap();
        assert_eq!(created["fields"], json!(["status", ["address", "city"]]));

        let drop = session
            .handle_message(
                5,
                ClientMessage::DropIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    name: "idx_status__address_city".to_string(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(drop, ServerMessage::Ok { .. }));

        let create_system = session
            .handle_message(
                6,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!("email")],
                    name: Some("_reserved".to_string()),
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&create_system), "invalid_message");

        let drop_system = session
            .handle_message(
                7,
                ClientMessage::DropIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    name: "_created_at".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&drop_system), "invalid_message");
    }

    #[tokio::test]
    async fn create_index_rejects_invalid_field_path() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();

        let response = session
            .handle_message(
                3,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!(["address", 42])],
                    name: Some("bad".to_string()),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "invalid_field_path");

        let response = session
            .handle_message(
                4,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![],
                    name: Some("empty".to_string()),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "invalid_field_path");
    }

    #[test]
    fn database_error_response_maps_special_database_errors() {
        let response = database_error_response(DatabaseError::InvalidFieldPath(
            "field path segment cannot be empty".to_string(),
        ));

        assert_eq!(error_code(&response), "invalid_field_path");

        let response = database_error_response(DatabaseError::ShuttingDown);
        assert_eq!(error_code(&response), "shutting_down");
    }

    #[tokio::test]
    async fn transaction_insert_commit_and_read_back() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada", "_meta": {"types": {"name": "string"}}}),
                },
            )
            .await
            .unwrap();
        let doc_id = ok_fields(&insert)["doc_id"].as_str().unwrap().to_string();

        let commit = session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert!(ok_fields(&commit)["commit_ts"].as_u64().unwrap() > 0);

        let read_begin = session
            .handle_message(
                6,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let read_tx = ok_fields(&read_begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                7,
                ClientMessage::Get {
                    tx: read_tx,
                    collection: "users".to_string(),
                    doc_id,
                },
            )
            .await
            .unwrap();
        let get_fields = ok_fields(&get);
        assert_eq!(get_fields["query_id"], 0);
        let doc = &get_fields["doc"];
        assert_eq!(doc["name"], "Ada");
        assert!(doc.get("_meta").is_none());
        assert!(doc["_id"].as_str().is_some());
        assert!(doc["_created_at"].as_i64().is_some());
    }

    #[tokio::test]
    async fn replica_write_commit_promotes_payload_instead_of_committing_locally() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        create_app_users(&mut session).await;
        let database = registry.get_database_by_name("app").unwrap();
        let promoter = Arc::new(RecordingPromoter::success(777));
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Replica Ada"}),
                },
            )
            .await
            .unwrap();
        let doc_id = decode_ulid(ok_fields(&insert)["doc_id"].as_str().unwrap()).unwrap();

        let commit = session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert_eq!(ok_fields(&commit)["commit_ts"].as_u64(), Some(777));

        {
            let seen = promoter.seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].0, "app");
            assert_eq!(seen[0].1, ok_fields(&begin)["begin_ts"].as_u64().unwrap());
            assert!(!seen[0].2.is_empty());
        }

        let mut read = database.begin(TransactionOptions::readonly()).unwrap();
        let local_doc = read.get("users", &doc_id).await.unwrap();
        read.rollback();
        assert!(local_doc.is_none(), "replica session committed locally");
    }

    #[tokio::test]
    async fn replica_create_collection_promotes_management_ddl() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();
        let promoter = Arc::new(RecordingPromoter::success(1));
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let response = session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "events".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(ok_fields(&response), &json!({}));
        assert_eq!(
            promoter.ddl_seen.lock().unwrap().as_slice(),
            &[DdlPromotionRequest::CreateCollection {
                database: "app".to_string(),
                name: "events".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn replica_database_management_promotes_system_ddl() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        let promoter = Arc::new(RecordingPromoter::success(1));
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let create = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({ "max_doc_size": 8192 })),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&create), &json!({}));

        let drop = session
            .handle_message(
                2,
                ClientMessage::DropDatabase {
                    name: "app".to_string(),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&drop), &json!({}));

        assert_eq!(
            promoter.ddl_seen.lock().unwrap().as_slice(),
            &[
                DdlPromotionRequest::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({ "max_doc_size": 8192 })),
                },
                DdlPromotionRequest::DropDatabase {
                    name: "app".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn replica_database_management_returns_primary_ddl_error_without_local_side_effect() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        let promoter = Arc::new(
            RecordingPromoter::success(1)
                .with_ddl_response(error("database_exists", "primary database already exists")),
        );
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let response = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({ "max_doc_size": 8192 })),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "database_exists");
        assert!(
            registry.get_database_by_name("app").is_none(),
            "replica must not apply promoted system DDL locally on primary error"
        );
        assert_eq!(
            promoter.ddl_seen.lock().unwrap().as_slice(),
            &[DdlPromotionRequest::CreateDatabase {
                name: "app".to_string(),
                config: Some(json!({ "max_doc_size": 8192 })),
            }]
        );
    }

    #[tokio::test]
    async fn replica_create_index_promotes_fields_and_returns_primary_response() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        create_app_users(&mut session).await;
        let promoter = Arc::new(
            RecordingPromoter::success(1).with_ddl_response(ok(json!({ "index_id": 42 }))),
        );
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let response = session
            .handle_message(
                3,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!("email"), json!(["profile", "age"])],
                    name: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(ok_fields(&response)["index_id"], 42);
        assert_eq!(
            promoter.ddl_seen.lock().unwrap().as_slice(),
            &[DdlPromotionRequest::CreateIndex {
                database: "app".to_string(),
                collection: "users".to_string(),
                fields: vec![
                    vec!["email".to_string()],
                    vec!["profile".to_string(), "age".to_string()]
                ],
                name: None,
            }]
        );

        let bad = session
            .handle_message(
                4,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!(["profile", 42])],
                    name: Some("bad".to_string()),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&bad), "invalid_field_path");
        assert_eq!(
            promoter.ddl_seen.lock().unwrap().as_slice(),
            &[DdlPromotionRequest::CreateIndex {
                database: "app".to_string(),
                collection: "users".to_string(),
                fields: vec![
                    vec!["email".to_string()],
                    vec!["profile".to_string(), "age".to_string()]
                ],
                name: None,
            }]
        );
    }

    #[tokio::test]
    async fn replica_subscribe_write_promotion_registers_local_subscription() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        create_app_users(&mut session).await;
        let database = registry.get_database_by_name("app").unwrap();
        let mut seed = database.begin(TransactionOptions::default()).unwrap();
        let doc_id = seed.insert("users", json!({"name": "Ada"})).await.unwrap();
        assert!(matches!(
            seed.commit().await.unwrap(),
            TransactionResult::Success { .. }
        ));

        let promoter = Arc::new(RecordingPromoter::success(888));
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter.clone()));

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: true,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                4,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&get)["query_id"], 0);
        let replace = session
            .handle_message(
                5,
                ClientMessage::Replace {
                    tx,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                    body: json!({"name": "Promoted Ada"}),
                },
            )
            .await
            .unwrap();
        assert!(matches!(replace, ServerMessage::Ok { .. }));

        let commit = session
            .handle_message(6, ClientMessage::Commit { tx })
            .await
            .unwrap();
        let fields = ok_fields(&commit);
        assert_eq!(fields["commit_ts"].as_u64(), Some(888));
        assert!(fields["subscription_id"].as_u64().is_some());
        assert_eq!(session.subscriptions.len(), 1);
        let seen = promoter.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "app");
    }

    #[tokio::test]
    async fn drop_removes_session_subscriptions_with_notification_sender() {
        let registry = registry().await;
        let mut session = Session::new(77, Arc::clone(&registry), AuthConfig::default());
        let (notification_tx, _notification_rx) = mpsc::channel(8);
        session.set_notification_sender(notification_tx);
        create_app_users(&mut session).await;

        let database = registry.get_database_by_name("app").unwrap();
        let mut seed = database.begin(TransactionOptions::default()).unwrap();
        let doc_id = seed.insert("users", json!({"name": "Ada"})).await.unwrap();
        assert!(matches!(
            seed.commit().await.unwrap(),
            TransactionResult::Success { .. }
        ));

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: true,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                4,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&get)["query_id"], 0);

        let commit = session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert!(ok_fields(&commit)["subscription_id"].as_u64().is_some());
        assert_eq!(database.subscriptions().read().subscription_count(), 1);

        drop(session);
        assert_eq!(database.subscriptions().read().subscription_count(), 0);
    }

    #[tokio::test]
    async fn replica_subscribe_promotion_conflict_materializes_retry_transaction() {
        let registry = registry().await;
        let mut session = Session::new(1, Arc::clone(&registry), AuthConfig::default());
        create_app_users(&mut session).await;
        let database = registry.get_database_by_name("app").unwrap();
        let mut seed = database.begin(TransactionOptions::default()).unwrap();
        let doc_id = seed.insert("users", json!({"name": "Ada"})).await.unwrap();
        let commit_ts = match seed.commit().await.unwrap() {
            TransactionResult::Success { commit_ts, .. } => commit_ts,
            _ => panic!("seed commit failed"),
        };

        let promoter = Arc::new(RecordingPromoter::conflict_with_retry(4242, commit_ts));
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_transaction_promoter(Some(promoter));

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: true,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                4,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&get)["doc"]["name"], "Ada");
        let replace = session
            .handle_message(
                5,
                ClientMessage::Replace {
                    tx,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                    body: json!({"name": "Conflicting Ada"}),
                },
            )
            .await
            .unwrap();
        assert!(matches!(replace, ServerMessage::Ok { .. }));

        let conflict = session
            .handle_message(6, ClientMessage::Commit { tx })
            .await
            .unwrap();
        match conflict {
            ServerMessage::Error { code, extra, .. } => {
                assert_eq!(code, "conflict");
                assert_eq!(extra.as_ref().unwrap()["new_tx"], 2);
                assert_eq!(extra.as_ref().unwrap()["new_ts"], commit_ts);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(
            session.active_transactions.get(&2).unwrap().tx.tx_id(),
            4242
        );

        let retry_get = session
            .handle_message(
                7,
                ClientMessage::Get {
                    tx: 2,
                    collection: "users".to_string(),
                    doc_id: encode_ulid(&doc_id),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&retry_get)["doc"]["name"], "Ada");
    }

    #[tokio::test]
    async fn replica_write_commit_without_promoter_returns_readonly_node() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;
        session.set_node_role(NODE_ROLE_REPLICA);

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Replica Ada"}),
                },
            )
            .await
            .unwrap();
        assert!(matches!(insert, ServerMessage::Ok { .. }));

        let commit = session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert_eq!(error_code(&commit), "readonly_node");
    }

    #[tokio::test]
    async fn replica_read_begin_without_quorum_returns_quorum_lost() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_replica_read_gate(Some(Arc::new(StaticReplicaReadGate(false))));

        let response = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "quorum_lost");
    }

    #[tokio::test]
    async fn replica_read_begin_with_quorum_succeeds() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;
        session.set_node_role(NODE_ROLE_REPLICA);
        session.set_replica_read_gate(Some(Arc::new(StaticReplicaReadGate(true))));

        let response = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();

        assert!(matches!(response, ServerMessage::Ok { .. }));
    }

    #[tokio::test]
    async fn replica_management_write_is_rejected_without_ddl_promoter() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        session.set_node_role(NODE_ROLE_REPLICA);

        let response = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "readonly_node");
    }

    #[tokio::test]
    async fn transaction_json_meta_types_round_trip_bytes_metadata() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({
                        "name": "Ada",
                        "avatar": "AQID",
                        "_meta": {
                            "types": {
                                "avatar": "bytes"
                            }
                        }
                    }),
                },
            )
            .await
            .unwrap();
        let doc_id = ok_fields(&insert)["doc_id"].as_str().unwrap().to_string();

        let commit = session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert!(ok_fields(&commit)["commit_ts"].as_u64().unwrap() > 0);

        let read_begin = session
            .handle_message(
                6,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let read_tx = ok_fields(&read_begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                7,
                ClientMessage::Get {
                    tx: read_tx,
                    collection: "users".to_string(),
                    doc_id,
                },
            )
            .await
            .unwrap();
        let doc = &ok_fields(&get)["doc"];
        assert_eq!(doc["avatar"], "AQID");
        assert_eq!(doc["_meta"]["types"]["avatar"], "bytes");
    }

    #[tokio::test]
    async fn transaction_query_parses_range_filter_order_and_limit() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        for (id, name) in [(4, "Ada"), (5, "Bob")] {
            session
                .handle_message(
                    id,
                    ClientMessage::Insert {
                        tx,
                        collection: "users".to_string(),
                        body: json!({"name": name, "active": true}),
                    },
                )
                .await
                .unwrap();
        }
        session
            .handle_message(6, ClientMessage::Commit { tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                7,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let query = session
            .handle_message(
                8,
                ClientMessage::Query {
                    tx,
                    collection: "users".to_string(),
                    index: "_created_at".to_string(),
                    range: vec![],
                    filter: Some(json!({"eq": ["name", "Bob"]})),
                    type_hints: None,
                    order: Some("desc".to_string()),
                    limit: Some(1),
                },
            )
            .await
            .unwrap();
        let query_fields = ok_fields(&query);
        assert_eq!(query_fields["query_id"], 0);
        let docs = query_fields["docs"].as_array().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Bob");
    }

    #[tokio::test]
    async fn transaction_query_uses_meta_types_for_range_and_filter_scalars() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let create_index = session
            .handle_message(
                3,
                ClientMessage::CreateIndex {
                    database: "app".to_string(),
                    collection: "users".to_string(),
                    fields: vec![json!("avatar")],
                    name: Some("avatar_idx".to_string()),
                },
            )
            .await
            .unwrap();
        assert!(ok_fields(&create_index)["index_id"].as_u64().unwrap() > 0);
        let next_msg_id =
            wait_for_session_index_ready(&mut session, "users", "avatar_idx", 4).await;

        let begin = session
            .handle_message(
                next_msg_id,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        session
            .handle_message(
                next_msg_id + 1,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({
                        "name": "Typed",
                        "avatar": "AQID",
                        "_meta": {"types": {"avatar": "bytes"}}
                    }),
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                next_msg_id + 2,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "String", "avatar": "AQID"}),
                },
            )
            .await
            .unwrap();
        let commit = session
            .handle_message(next_msg_id + 3, ClientMessage::Commit { tx })
            .await
            .unwrap();
        assert!(ok_fields(&commit)["commit_ts"].as_u64().unwrap() > 0);

        let begin = session
            .handle_message(
                next_msg_id + 4,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let read_tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        let range_query = session
            .handle_message(
                next_msg_id + 5,
                ClientMessage::Query {
                    tx: read_tx,
                    collection: "users".to_string(),
                    index: "avatar_idx".to_string(),
                    range: vec![json!({"eq": ["avatar", "AQID"]})],
                    filter: None,
                    type_hints: Some(json!({"range": [{"eq": "bytes"}]})),
                    order: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        let docs = ok_fields(&range_query)["docs"].as_array().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Typed");
        assert_eq!(docs[0]["_meta"]["types"]["avatar"], "bytes");

        let filter_query = session
            .handle_message(
                next_msg_id + 6,
                ClientMessage::Query {
                    tx: read_tx,
                    collection: "users".to_string(),
                    index: "_created_at".to_string(),
                    range: vec![],
                    filter: Some(json!({"eq": ["avatar", "AQID"]})),
                    type_hints: Some(json!({"filter": {"eq": "bytes"}})),
                    order: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        let docs = ok_fields(&filter_query)["docs"].as_array().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Typed");
    }

    #[tokio::test]
    async fn transaction_replace_patch_and_rollback() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada", "old": true}),
                },
            )
            .await
            .unwrap();
        let doc_id = ok_fields(&insert)["doc_id"].as_str().unwrap().to_string();
        session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                6,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        assert!(matches!(
            session
                .handle_message(
                    7,
                    ClientMessage::Replace {
                        tx,
                        collection: "users".to_string(),
                        doc_id: doc_id.clone(),
                        body: json!({"name": "Ada Lovelace", "old": true}),
                    },
                )
                .await
                .unwrap(),
            ServerMessage::Ok { .. }
        ));
        assert!(matches!(
            session
                .handle_message(
                    8,
                    ClientMessage::Patch {
                        tx,
                        collection: "users".to_string(),
                        doc_id: doc_id.clone(),
                        body: json!({"old": null, "title": "Countess"}),
                    },
                )
                .await
                .unwrap(),
            ServerMessage::Ok { .. }
        ));
        let get = session
            .handle_message(
                9,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id: doc_id.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&get)["doc"]["title"], "Countess");
        session
            .handle_message(10, ClientMessage::Rollback { tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                11,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                12,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(ok_fields(&get)["doc"]["name"], "Ada");
        assert!(ok_fields(&get)["doc"].get("title").is_none());
    }

    #[tokio::test]
    async fn transaction_delete_commits_tombstone() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let insert = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada"}),
                },
            )
            .await
            .unwrap();
        let doc_id = ok_fields(&insert)["doc_id"].as_str().unwrap().to_string();
        session
            .handle_message(5, ClientMessage::Commit { tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                6,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        session
            .handle_message(
                7,
                ClientMessage::Delete {
                    tx,
                    collection: "users".to_string(),
                    doc_id: doc_id.clone(),
                },
            )
            .await
            .unwrap();
        session
            .handle_message(8, ClientMessage::Commit { tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                9,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let get = session
            .handle_message(
                10,
                ClientMessage::Get {
                    tx,
                    collection: "users".to_string(),
                    doc_id,
                },
            )
            .await
            .unwrap();
        assert!(ok_fields(&get)["doc"].is_null());
    }

    #[tokio::test]
    async fn transaction_errors_are_mapped() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        create_app_users(&mut session).await;

        let unknown = session
            .handle_message(3, ClientMessage::Commit { tx: 999 })
            .await
            .unwrap();
        assert_eq!(error_code(&unknown), "unknown_transaction");

        let begin = session
            .handle_message(
                4,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        let readonly = session
            .handle_message(
                5,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada"}),
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&readonly), "readonly_tx");

        let bad_query = session
            .handle_message(
                6,
                ClientMessage::Query {
                    tx,
                    collection: "users".to_string(),
                    index: "_created_at".to_string(),
                    range: vec![json!({"eq": ["name"]})],
                    filter: None,
                    type_hints: None,
                    order: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&bad_query), "invalid_range");

        let semantic_bad_query = session
            .handle_message(
                7,
                ClientMessage::Query {
                    tx,
                    collection: "users".to_string(),
                    index: "_created_at".to_string(),
                    range: vec![json!({"eq": ["name", "Ada"]})],
                    filter: None,
                    type_hints: None,
                    order: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&semantic_bad_query), "invalid_range");
    }

    #[tokio::test]
    async fn oversized_document_returns_doc_too_large() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());

        let create = session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({ "max_doc_size": 128 })),
                },
            )
            .await
            .unwrap();
        assert!(matches!(create, ServerMessage::Ok { .. }));
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        let response = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({ "name": "x".repeat(512) }),
                },
            )
            .await
            .unwrap();

        assert_eq!(error_code(&response), "doc_too_large");
    }

    #[tokio::test]
    async fn transaction_timeout_aborts_active_session_transaction() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({
                        "transaction": {
                            "idle_timeout": "1ms"
                        }
                    })),
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();

        let begin = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let timed_out = session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada"}),
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&timed_out), "tx_timeout");
        assert!(!session.active_transactions.contains_key(&tx));

        let rollback = session
            .handle_message(5, ClientMessage::Rollback { tx })
            .await
            .unwrap();
        assert_eq!(error_code(&rollback), "unknown_transaction");
    }

    #[tokio::test]
    async fn read_limit_exceeded_aborts_active_session_transaction() {
        let mut session = Session::new(1, registry().await, AuthConfig::default());
        session
            .handle_message(
                1,
                ClientMessage::CreateDatabase {
                    name: "app".to_string(),
                    config: Some(json!({
                        "transaction": {
                            "max_scanned_docs": 0
                        }
                    })),
                },
            )
            .await
            .unwrap();
        session
            .handle_message(
                2,
                ClientMessage::CreateCollection {
                    database: "app".to_string(),
                    name: "users".to_string(),
                },
            )
            .await
            .unwrap();

        let seed = session
            .handle_message(
                3,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: false,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let seed_tx = ok_fields(&seed)["tx"].as_u64().unwrap();
        session
            .handle_message(
                4,
                ClientMessage::Insert {
                    tx: seed_tx,
                    collection: "users".to_string(),
                    body: json!({"name": "Ada"}),
                },
            )
            .await
            .unwrap();
        session
            .handle_message(5, ClientMessage::Commit { tx: seed_tx })
            .await
            .unwrap();

        let begin = session
            .handle_message(
                6,
                ClientMessage::Begin {
                    database: "app".to_string(),
                    readonly: true,
                    subscribe: false,
                    notify: false,
                },
            )
            .await
            .unwrap();
        let tx = ok_fields(&begin)["tx"].as_u64().unwrap();

        let query = session
            .handle_message(
                7,
                ClientMessage::Query {
                    tx,
                    collection: "users".to_string(),
                    index: "_created_at".to_string(),
                    range: vec![],
                    filter: None,
                    type_hints: None,
                    order: None,
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(error_code(&query), "read_limit_exceeded");
        assert!(!session.active_transactions.contains_key(&tx));

        let rollback = session
            .handle_message(8, ClientMessage::Rollback { tx })
            .await
            .unwrap();
        assert_eq!(error_code(&rollback), "unknown_transaction");
    }
}
