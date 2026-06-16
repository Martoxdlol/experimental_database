//! exdb-server — Database server binary.
//!
//! Opens a multi-database registry and exposes it through the L8 wire protocol.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use exdb::{
    Database, DatabaseConfig, DatabaseError, FieldPath, StorageSnapshot, SubscriptionMode,
    SystemDatabase, TransactionOptions,
};
use exdb_replication::{
    ClusterConfig, ClusterMembership, LocalState, NodeId, NodeRole, PeerMesh, PeerMeshRuntime,
    PrimaryReplicator, PromotionClient, PromotionHandler, PromotionOutcome, PromotionRetry,
    RecoveryTier, ReplicationError, SnapshotSink, SnapshotSource, StorageWalCatchupSource,
    WalApplier, select_recovery_tier,
};
use exdb_wire::{
    AuthConfig, DEFAULT_NODE_ROLE, DdlPromotionRequest, JwtAlgorithm, ListenConfig,
    ReplicaReadGate, Server, ServerConfig, ServerMessage, TlsConfig, TransactionPromoter,
    TransactionPromotionOutcome, database_config_from_json,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const DEFAULT_DATA_ROOT: &str = "./data";
const DEFAULT_TCP_ADDR: &str = "0.0.0.0:5200";
const DEFAULT_REPLICATION_DATABASE: &str = "default";
const DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const DDL_PROMOTION_MAGIC: &[u8] = b"EXDB_DDL_PROMOTION_V1\0";
const TX_PROMOTION_MAGIC: &[u8] = b"EXDB_TX_PROMOTION_V1\0";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum DdlPromotionEnvelope {
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

fn encode_ddl_promotion(request: &DdlPromotionRequest) -> Result<Vec<u8>, String> {
    let envelope = match request {
        DdlPromotionRequest::CreateDatabase { name, config } => {
            DdlPromotionEnvelope::CreateDatabase {
                name: name.clone(),
                config: config.clone(),
            }
        }
        DdlPromotionRequest::DropDatabase { name } => {
            DdlPromotionEnvelope::DropDatabase { name: name.clone() }
        }
        DdlPromotionRequest::CreateCollection { database, name } => {
            DdlPromotionEnvelope::CreateCollection {
                database: database.clone(),
                name: name.clone(),
            }
        }
        DdlPromotionRequest::DropCollection { database, name } => {
            DdlPromotionEnvelope::DropCollection {
                database: database.clone(),
                name: name.clone(),
            }
        }
        DdlPromotionRequest::CreateIndex {
            database,
            collection,
            fields,
            name,
        } => DdlPromotionEnvelope::CreateIndex {
            database: database.clone(),
            collection: collection.clone(),
            fields: fields.clone(),
            name: name.clone(),
        },
        DdlPromotionRequest::DropIndex {
            database,
            collection,
            name,
        } => DdlPromotionEnvelope::DropIndex {
            database: database.clone(),
            collection: collection.clone(),
            name: name.clone(),
        },
    };

    let mut out = Vec::from(DDL_PROMOTION_MAGIC);
    out.extend(
        serde_json::to_vec(&envelope).map_err(|err| format!("encode DDL promotion: {err}"))?,
    );
    Ok(out)
}

fn decode_ddl_promotion(payload: &[u8]) -> Result<Option<DdlPromotionEnvelope>, String> {
    let Some(json_payload) = payload.strip_prefix(DDL_PROMOTION_MAGIC) else {
        return Ok(None);
    };
    serde_json::from_slice(json_payload)
        .map(Some)
        .map_err(|err| format!("decode DDL promotion: {err}"))
}

fn decode_ddl_response(payload: &[u8]) -> Result<ServerMessage, String> {
    serde_json::from_slice(payload).map_err(|err| format!("decode DDL promotion response: {err}"))
}

fn encode_transaction_promotion(database: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
    let database = database.as_bytes();
    let database_len = u32::try_from(database.len())
        .map_err(|_| "database name is too large for transaction promotion".to_string())?;
    let mut out = Vec::with_capacity(
        TX_PROMOTION_MAGIC.len() + std::mem::size_of::<u32>() + database.len() + payload.len(),
    );
    out.extend_from_slice(TX_PROMOTION_MAGIC);
    out.extend_from_slice(&database_len.to_le_bytes());
    out.extend_from_slice(database);
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_transaction_promotion(payload: &[u8]) -> Result<Option<(String, &[u8])>, String> {
    let Some(payload) = payload.strip_prefix(TX_PROMOTION_MAGIC) else {
        return Ok(None);
    };
    if payload.len() < std::mem::size_of::<u32>() {
        return Err("decode transaction promotion: truncated database length".to_string());
    }
    let database_len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    let after_len = &payload[4..];
    if after_len.len() < database_len {
        return Err("decode transaction promotion: truncated database name".to_string());
    }
    let database = std::str::from_utf8(&after_len[..database_len])
        .map_err(|err| format!("decode transaction promotion database name: {err}"))?
        .to_string();
    Ok(Some((database, &after_len[database_len..])))
}

fn field_paths_from_segments(fields: Vec<Vec<String>>) -> Result<Vec<FieldPath>, String> {
    if fields.is_empty() {
        return Err("fields must contain at least one field path".to_string());
    }

    fields
        .into_iter()
        .map(|segments| {
            if segments.is_empty() {
                return Err("nested field path cannot be empty".to_string());
            }
            if segments.iter().any(|segment| segment.is_empty()) {
                return Err("field path segment cannot be empty".to_string());
            }
            Ok(FieldPath::new(segments))
        })
        .collect()
}

fn default_index_name(fields: &[FieldPath]) -> String {
    let encoded = fields
        .iter()
        .map(|field| field.segments().join("_"))
        .collect::<Vec<_>>()
        .join("__");
    format!("idx_{encoded}")
}

fn wire_ok(fields: serde_json::Value) -> ServerMessage {
    ServerMessage::Ok { fields }
}

fn wire_error(code: impl Into<String>, message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error {
        code: code.into(),
        message: message.into(),
        extra: None,
    }
}

fn database_error_response(err: DatabaseError) -> ServerMessage {
    let code = match &err {
        DatabaseError::DatabaseNotFound(_) => "unknown_database",
        DatabaseError::DatabaseAlreadyExists(_) => "database_exists",
        DatabaseError::DatabaseInUse(_) => "database_in_use",
        DatabaseError::CollectionNotFound(_) => "unknown_collection",
        DatabaseError::CollectionAlreadyExists(_) => "collection_exists",
        DatabaseError::IndexNotFound { .. } => "unknown_index",
        DatabaseError::IndexAlreadyExists { .. } => "index_exists",
        DatabaseError::IndexNotReady(_) => "index_not_ready",
        DatabaseError::DocNotFound => "doc_not_found",
        DatabaseError::ReadonlyWrite => "readonly_tx",
        DatabaseError::ReadLimitExceeded(_) => "read_limit_exceeded",
        DatabaseError::TransactionTimeout => "tx_timeout",
        DatabaseError::QuorumLost => "quorum_lost",
        DatabaseError::InvalidConfig(_) => "invalid_message",
        DatabaseError::InvalidName(_) | DatabaseError::ReservedName(_) => "invalid_message",
        DatabaseError::IntegrityCheckFailed { .. } => "database_corrupt",
        _ => "internal",
    };
    wire_error(code, err.to_string())
}

struct NoopWalApplier;

#[async_trait::async_trait]
impl WalApplier for NoopWalApplier {
    async fn apply_wal_record(
        &self,
        _peer_id: NodeId,
        lsn: u64,
        _record: &[u8],
    ) -> Result<(u64, exdb::Ts), ReplicationError> {
        Ok((lsn, 0))
    }
}

/// Server-side bridge from L7 peer WAL frames into one L6 database.
///
/// The server owns this adapter because it is the first layer that can depend
/// on both `exdb` and `exdb-replication` without creating a crate cycle.
struct DatabaseWalApplier {
    database: Arc<Database>,
}

impl DatabaseWalApplier {
    fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

#[async_trait::async_trait]
impl WalApplier for DatabaseWalApplier {
    async fn apply_wal_record(
        &self,
        peer_id: NodeId,
        lsn: u64,
        record: &[u8],
    ) -> Result<(u64, exdb::Ts), ReplicationError> {
        let applied_ts = self
            .database
            .apply_replicated_wal(lsn, record)
            .await
            .map_err(|err| ReplicationError::ApplyFailed {
                node_id: peer_id,
                message: err.to_string(),
            })?;
        Ok((lsn, applied_ts))
    }
}

/// Server-side bridge from L7 promotion frames into the primary L6 commit path.
struct DatabasePromotionHandler {
    database: Arc<Database>,
    registry: Arc<SystemDatabase>,
    default_database_config: DatabaseConfig,
}

impl DatabasePromotionHandler {
    fn new(
        database: Arc<Database>,
        registry: Arc<SystemDatabase>,
        default_database_config: DatabaseConfig,
    ) -> Self {
        Self {
            database,
            registry,
            default_database_config,
        }
    }

    async fn handle_ddl_promotion(&self, request: DdlPromotionEnvelope) -> ServerMessage {
        match request {
            DdlPromotionEnvelope::CreateDatabase { name, config } => {
                let config = match database_config_from_json(config, &self.default_database_config)
                {
                    Ok(config) => config,
                    Err(message) => return wire_error("invalid_message", message),
                };
                match self.registry.create_database(&name, config).await {
                    Ok(_) => wire_ok(json!({})),
                    Err(err) => database_error_response(err),
                }
            }
            DdlPromotionEnvelope::DropDatabase { name } => {
                match self.registry.drop_database(&name).await {
                    Ok(()) => wire_ok(json!({})),
                    Err(err) => database_error_response(err),
                }
            }
            DdlPromotionEnvelope::CreateCollection { database, name } => {
                let database = match self.database_for_ddl(&database) {
                    Ok(database) => database,
                    Err(response) => return response,
                };
                let mut tx = match database.begin(TransactionOptions::default()) {
                    Ok(tx) => tx,
                    Err(err) => return database_error_response(err),
                };
                if let Err(err) = tx.create_collection(&name).await {
                    return database_error_response(err);
                }
                match tx.commit().await {
                    Ok(_) => wire_ok(json!({})),
                    Err(err) => database_error_response(err),
                }
            }
            DdlPromotionEnvelope::DropCollection { database, name } => {
                let database = match self.database_for_ddl(&database) {
                    Ok(database) => database,
                    Err(response) => return response,
                };
                let mut tx = match database.begin(TransactionOptions::default()) {
                    Ok(tx) => tx,
                    Err(err) => return database_error_response(err),
                };
                if let Err(err) = tx.drop_collection(&name).await {
                    return database_error_response(err);
                }
                match tx.commit().await {
                    Ok(_) => wire_ok(json!({})),
                    Err(err) => database_error_response(err),
                }
            }
            DdlPromotionEnvelope::CreateIndex {
                database,
                collection,
                fields,
                name,
            } => {
                let database = match self.database_for_ddl(&database) {
                    Ok(database) => database,
                    Err(response) => return response,
                };
                let fields = match field_paths_from_segments(fields) {
                    Ok(fields) => fields,
                    Err(message) => return wire_error("invalid_message", message),
                };
                let name = name.unwrap_or_else(|| default_index_name(&fields));

                let mut tx = match database.begin(TransactionOptions::default()) {
                    Ok(tx) => tx,
                    Err(err) => return database_error_response(err),
                };
                if let Err(err) = tx.create_index(&collection, &name, fields).await {
                    return database_error_response(err);
                }
                if let Err(err) = tx.commit().await {
                    return database_error_response(err);
                }

                let mut tx = match database.begin(TransactionOptions::readonly()) {
                    Ok(tx) => tx,
                    Err(err) => return database_error_response(err),
                };
                let response = match tx.list_indexes(&collection) {
                    Ok(indexes) => indexes
                        .into_iter()
                        .find(|meta| meta.name == name)
                        .map(|meta| wire_ok(json!({ "index_id": meta.index_id.0 })))
                        .unwrap_or_else(|| {
                            wire_error("internal", "created index was not visible after commit")
                        }),
                    Err(err) => database_error_response(err),
                };
                tx.rollback();
                response
            }
            DdlPromotionEnvelope::DropIndex {
                database,
                collection,
                name,
            } => {
                let database = match self.database_for_ddl(&database) {
                    Ok(database) => database,
                    Err(response) => return response,
                };
                let mut tx = match database.begin(TransactionOptions::default()) {
                    Ok(tx) => tx,
                    Err(err) => return database_error_response(err),
                };
                if let Err(err) = tx.drop_index(&collection, &name).await {
                    return database_error_response(err);
                }
                match tx.commit().await {
                    Ok(_) => wire_ok(json!({})),
                    Err(err) => database_error_response(err),
                }
            }
        }
    }

    fn database_for_ddl(&self, name: &str) -> Result<Arc<Database>, ServerMessage> {
        self.registry
            .get_database_by_name(name)
            .ok_or_else(|| wire_error("unknown_database", format!("database not found: {name}")))
    }
}

#[async_trait::async_trait]
impl PromotionHandler for DatabasePromotionHandler {
    async fn handle_promotion(
        &self,
        peer_id: NodeId,
        begin_ts: exdb::Ts,
        subscription: SubscriptionMode,
        payload: &[u8],
    ) -> Result<PromotionOutcome, ReplicationError> {
        if let Some(request) =
            decode_ddl_promotion(payload).map_err(|message| ReplicationError::PromotionFailed {
                node_id: peer_id,
                message,
            })?
        {
            let response = self.handle_ddl_promotion(request).await;
            let payload =
                serde_json::to_vec(&response).map_err(|err| ReplicationError::PromotionFailed {
                    node_id: peer_id,
                    message: err.to_string(),
                })?;
            return Ok(PromotionOutcome::Response { payload });
        }

        let (database, payload) =
            match decode_transaction_promotion(payload).map_err(|message| {
                ReplicationError::PromotionFailed {
                    node_id: peer_id,
                    message,
                }
            })? {
                Some((database_name, payload)) => (
                    self.registry
                        .get_database_by_name(&database_name)
                        .ok_or_else(|| ReplicationError::PromotionFailed {
                            node_id: peer_id,
                            message: format!("database not found: {database_name}"),
                        })?,
                    payload,
                ),
                None => (Arc::clone(&self.database), payload),
            };

        match database
            .commit_promoted_transaction_with_subscription(begin_ts, payload, subscription)
            .await
            .map_err(|err| ReplicationError::PromotionFailed {
                node_id: peer_id,
                message: err.to_string(),
            })? {
            exdb::TransactionResult::Success { commit_ts, .. } => {
                Ok(PromotionOutcome::Success { commit_ts })
            }
            exdb::TransactionResult::Conflict { error, retry } => Ok(PromotionOutcome::Conflict {
                error: error.to_string().into_bytes(),
                retry: retry.map(|retry| PromotionRetry {
                    new_tx: retry.new_tx_id,
                    new_ts: retry.new_ts,
                }),
            }),
            exdb::TransactionResult::QuorumLost => Err(ReplicationError::PromotionFailed {
                node_id: peer_id,
                message: "primary replication quorum lost during promoted commit".to_string(),
            }),
        }
    }
}

/// L8 adapter that promotes replica wire-session commits through the L7 mesh.
struct ReplicaTransactionPromoter {
    client: PromotionClient,
}

impl ReplicaTransactionPromoter {
    fn new(mesh: Arc<PeerMesh>) -> Self {
        Self {
            client: PromotionClient::new(mesh),
        }
    }
}

#[async_trait::async_trait]
impl TransactionPromoter for ReplicaTransactionPromoter {
    async fn promote_transaction(
        &self,
        database: &str,
        begin_ts: exdb::Ts,
        subscription: SubscriptionMode,
        payload: Vec<u8>,
    ) -> Result<TransactionPromotionOutcome, String> {
        let payload = encode_transaction_promotion(database, &payload)?;
        match self.client.promote(begin_ts, subscription, &payload).await {
            Ok(PromotionOutcome::Success { commit_ts }) => {
                Ok(TransactionPromotionOutcome::Success { commit_ts })
            }
            Ok(PromotionOutcome::Conflict { error, retry }) => {
                Ok(TransactionPromotionOutcome::Conflict {
                    message: String::from_utf8_lossy(&error).into_owned(),
                    extra: retry.map(|retry| {
                        serde_json::json!({
                            "new_tx": retry.new_tx,
                            "new_ts": retry.new_ts,
                        })
                    }),
                })
            }
            Ok(PromotionOutcome::Response { .. }) => {
                Err("primary returned a DDL response to transaction promotion".to_string())
            }
            Err(err) => Err(err.to_string()),
        }
    }

    async fn promote_ddl(&self, request: DdlPromotionRequest) -> Result<ServerMessage, String> {
        let payload = encode_ddl_promotion(&request)?;
        match self
            .client
            .promote(0, SubscriptionMode::None, &payload)
            .await
        {
            Ok(PromotionOutcome::Response { payload }) => decode_ddl_response(&payload),
            Ok(PromotionOutcome::Success { .. }) => Ok(ServerMessage::Ok { fields: json!({}) }),
            Ok(PromotionOutcome::Conflict { error, retry }) => {
                let extra = retry.map(|retry| {
                    json!({
                        "new_tx": retry.new_tx,
                        "new_ts": retry.new_ts,
                    })
                });
                Ok(ServerMessage::Error {
                    code: "conflict".to_string(),
                    message: String::from_utf8_lossy(&error).into_owned(),
                    extra,
                })
            }
            Err(err) => Err(err.to_string()),
        }
    }
}

#[derive(Default)]
struct ReplicaDatabaseHandle {
    database: StdMutex<Option<Arc<Database>>>,
}

impl ReplicaDatabaseHandle {
    fn new(database: Option<Arc<Database>>) -> Self {
        Self {
            database: StdMutex::new(database),
        }
    }

    fn install(&self, database: Arc<Database>) {
        *self
            .database
            .lock()
            .expect("replica database mutex poisoned") = Some(database);
    }

    fn clear(&self) {
        *self
            .database
            .lock()
            .expect("replica database mutex poisoned") = None;
    }

    fn database(&self) -> Option<Arc<Database>> {
        self.database
            .lock()
            .expect("replica database mutex poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl WalApplier for ReplicaDatabaseHandle {
    async fn apply_wal_record(
        &self,
        peer_id: NodeId,
        lsn: u64,
        record: &[u8],
    ) -> Result<(u64, exdb::Ts), ReplicationError> {
        let database = self
            .database()
            .ok_or_else(|| ReplicationError::ApplyFailed {
                node_id: peer_id,
                message: "replica database has not been restored".to_string(),
            })?;
        DatabaseWalApplier::new(database)
            .apply_wal_record(peer_id, lsn, record)
            .await
    }
}

/// Server-side source adapter from an L6 database to L7 snapshot chunks.
///
/// The server owns this adapter for the same reason it owns
/// [`DatabaseWalApplier`]: this is the layer that can depend on both the
/// database facade and the replication mesh without introducing a crate cycle.
struct DatabaseSnapshotSource {
    database: Arc<Database>,
    max_chunk_len: usize,
}

impl DatabaseSnapshotSource {
    fn new(database: Arc<Database>, max_chunk_len: usize) -> Self {
        Self {
            database,
            max_chunk_len,
        }
    }

    async fn chunks_for_peer(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError> {
        let snapshot = self.database.export_snapshot().await.map_err(|err| {
            ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            }
        })?;
        snapshot
            .into_chunks(self.max_chunk_len)
            .map_err(|err| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            })
    }
}

#[async_trait::async_trait]
impl SnapshotSource for DatabaseSnapshotSource {
    async fn snapshot_chunks(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.chunks_for_peer(peer_id).await
    }
}

/// Server-side sink adapter from L7 snapshot chunks into an L6 database path.
#[cfg(test)]
struct DatabaseSnapshotSink {
    target_path: PathBuf,
    config: DatabaseConfig,
    chunks: StdMutex<Option<Vec<Vec<u8>>>>,
}

#[cfg(test)]
impl DatabaseSnapshotSink {
    fn new(target_path: PathBuf, config: DatabaseConfig) -> Self {
        Self {
            target_path,
            config,
            chunks: StdMutex::new(None),
        }
    }

    fn snapshot_chunks(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.chunks
            .lock()
            .expect("snapshot sink mutex poisoned")
            .take()
            .ok_or_else(|| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: "snapshot stream was not started".to_string(),
            })
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl SnapshotSink for DatabaseSnapshotSink {
    async fn begin_snapshot(&self, _peer_id: NodeId) -> Result<(), ReplicationError> {
        let mut chunks = self.chunks.lock().expect("snapshot sink mutex poisoned");
        // A previous stream can be abandoned if the peer disconnects before
        // SnapshotEnd. Treat a new begin as a fresh reconstruction attempt.
        *chunks = Some(Vec::new());
        Ok(())
    }

    async fn apply_snapshot_chunk(
        &self,
        peer_id: NodeId,
        chunk: &[u8],
    ) -> Result<(), ReplicationError> {
        let mut chunks = self.chunks.lock().expect("snapshot sink mutex poisoned");
        let chunks = chunks
            .as_mut()
            .ok_or_else(|| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: "snapshot chunk received before begin".to_string(),
            })?;
        chunks.push(chunk.to_vec());
        Ok(())
    }

    async fn end_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
        let chunks = self.snapshot_chunks(peer_id)?;
        let snapshot = StorageSnapshot::from_chunks(chunks).map_err(|err| {
            ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            }
        })?;
        Database::restore_snapshot(&self.target_path, self.config.clone(), snapshot)
            .await
            .map_err(|err| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            })?;
        Ok(())
    }
}

struct RegisteringDatabaseSnapshotSink {
    registry: Arc<SystemDatabase>,
    database_name: String,
    target_path: PathBuf,
    config: DatabaseConfig,
    database_handle: Arc<ReplicaDatabaseHandle>,
    chunks: StdMutex<Option<Vec<Vec<u8>>>>,
}

impl RegisteringDatabaseSnapshotSink {
    fn new(
        registry: Arc<SystemDatabase>,
        database_name: String,
        target_path: PathBuf,
        config: DatabaseConfig,
        database_handle: Arc<ReplicaDatabaseHandle>,
    ) -> Self {
        Self {
            registry,
            database_name,
            target_path,
            config,
            database_handle,
            chunks: StdMutex::new(None),
        }
    }

    fn snapshot_chunks(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError> {
        self.chunks
            .lock()
            .expect("snapshot sink mutex poisoned")
            .take()
            .ok_or_else(|| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: "snapshot stream was not started".to_string(),
            })
    }

    fn temporary_restore_path(&self, peer_id: NodeId) -> PathBuf {
        let parent = self.target_path.parent().unwrap_or_else(|| Path::new("."));
        let database_dir = self
            .target_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("database");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        parent.join(format!(
            ".{database_dir}.snapshot-replace-{peer_id}-{nonce}"
        ))
    }

    fn cleanup_temporary_restore(
        &self,
        peer_id: NodeId,
        temp_path: &Path,
    ) -> Result<(), ReplicationError> {
        if !temp_path.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(temp_path).map_err(|err| ReplicationError::SnapshotFailed {
            node_id: peer_id,
            message: format!(
                "failed to remove temporary snapshot path {}: {err}",
                temp_path.display()
            ),
        })
    }

    fn restore_previous_handle(&self) {
        if let Some(database) = self.registry.get_database_by_name(&self.database_name) {
            self.database_handle.install(database);
        }
    }

    async fn replace_registered_database(
        &self,
        peer_id: NodeId,
        snapshot: StorageSnapshot,
    ) -> Result<Arc<Database>, ReplicationError> {
        let temp_path = self.temporary_restore_path(peer_id);
        self.cleanup_temporary_restore(peer_id, &temp_path)?;

        Database::restore_snapshot(&temp_path, self.config.clone(), snapshot)
            .await
            .map_err(|err| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            })?;

        self.database_handle.clear();
        match self.registry.drop_database(&self.database_name).await {
            Ok(()) | Err(DatabaseError::DatabaseNotFound(_)) => {}
            Err(err) => {
                self.restore_previous_handle();
                self.cleanup_temporary_restore(peer_id, &temp_path)?;
                return Err(ReplicationError::SnapshotFailed {
                    node_id: peer_id,
                    message: err.to_string(),
                });
            }
        }

        if self.target_path.exists() {
            self.cleanup_temporary_restore(peer_id, &temp_path)?;
            return Err(ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: format!(
                    "snapshot target path {} exists without a registered database",
                    self.target_path.display()
                ),
            });
        }
        std::fs::rename(&temp_path, &self.target_path).map_err(|err| {
            ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: format!(
                    "failed to install snapshot at {}: {err}",
                    self.target_path.display()
                ),
            }
        })?;

        self.registry
            .register_existing_database(&self.database_name, self.config.clone())
            .await
            .map_err(|err| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            })
    }
}

#[async_trait::async_trait]
impl SnapshotSink for RegisteringDatabaseSnapshotSink {
    async fn begin_snapshot(&self, _peer_id: NodeId) -> Result<(), ReplicationError> {
        let mut chunks = self.chunks.lock().expect("snapshot sink mutex poisoned");
        // A previous stream can be abandoned if the peer disconnects before
        // SnapshotEnd. Treat a new begin as a fresh reconstruction attempt.
        *chunks = Some(Vec::new());
        Ok(())
    }

    async fn apply_snapshot_chunk(
        &self,
        peer_id: NodeId,
        chunk: &[u8],
    ) -> Result<(), ReplicationError> {
        let mut chunks = self.chunks.lock().expect("snapshot sink mutex poisoned");
        let chunks = chunks
            .as_mut()
            .ok_or_else(|| ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: "snapshot chunk received before begin".to_string(),
            })?;
        chunks.push(chunk.to_vec());
        Ok(())
    }

    async fn end_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
        let chunks = self.snapshot_chunks(peer_id)?;
        let snapshot = StorageSnapshot::from_chunks(chunks).map_err(|err| {
            ReplicationError::SnapshotFailed {
                node_id: peer_id,
                message: err.to_string(),
            }
        })?;
        let database = self.replace_registered_database(peer_id, snapshot).await?;
        self.database_handle.install(database);
        Ok(())
    }
}

#[allow(dead_code)]
struct ServerReplicationRuntime {
    database_name: String,
    runtime: PeerMeshRuntime,
    node_role: NodeRole,
    startup_generation: u64,
    startup_recovery_tier: Option<RecoveryTier>,
    startup_quarantined_path: Option<PathBuf>,
    transaction_promoter: Option<Arc<dyn TransactionPromoter>>,
    _mesh: Arc<PeerMesh>,
    _database: Option<Arc<Database>>,
    _replica_database_handle: Option<Arc<ReplicaDatabaseHandle>>,
}

impl ServerReplicationRuntime {
    async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}

struct DatabaseRoutingTransactionPromoter {
    by_database: HashMap<String, Arc<dyn TransactionPromoter>>,
    default_promoter: Arc<dyn TransactionPromoter>,
}

impl DatabaseRoutingTransactionPromoter {
    fn new(
        by_database: HashMap<String, Arc<dyn TransactionPromoter>>,
        default_promoter: Arc<dyn TransactionPromoter>,
    ) -> Self {
        Self {
            by_database,
            default_promoter,
        }
    }

    fn promoter_for_database(&self, database: &str) -> Option<Arc<dyn TransactionPromoter>> {
        self.by_database.get(database).cloned()
    }

    fn promoter_for_ddl(&self, request: &DdlPromotionRequest) -> Arc<dyn TransactionPromoter> {
        let database = match request {
            DdlPromotionRequest::CreateDatabase { .. } => None,
            DdlPromotionRequest::DropDatabase { name } => Some(name.as_str()),
            DdlPromotionRequest::CreateCollection { database, .. }
            | DdlPromotionRequest::DropCollection { database, .. }
            | DdlPromotionRequest::CreateIndex { database, .. }
            | DdlPromotionRequest::DropIndex { database, .. } => Some(database.as_str()),
        };
        database
            .and_then(|database| self.promoter_for_database(database))
            .unwrap_or_else(|| Arc::clone(&self.default_promoter))
    }
}

#[async_trait::async_trait]
impl TransactionPromoter for DatabaseRoutingTransactionPromoter {
    async fn promote_transaction(
        &self,
        database: &str,
        begin_ts: exdb::Ts,
        subscription: SubscriptionMode,
        payload: Vec<u8>,
    ) -> Result<TransactionPromotionOutcome, String> {
        let promoter = self
            .promoter_for_database(database)
            .ok_or_else(|| format!("database is not configured for replication: {database}"))?;
        promoter
            .promote_transaction(database, begin_ts, subscription, payload)
            .await
    }

    async fn promote_ddl(&self, request: DdlPromotionRequest) -> Result<ServerMessage, String> {
        self.promoter_for_ddl(&request).promote_ddl(request).await
    }
}

struct DatabaseRoutingReplicaReadGate {
    by_database: HashMap<String, Arc<PeerMesh>>,
}

impl DatabaseRoutingReplicaReadGate {
    fn new(by_database: HashMap<String, Arc<PeerMesh>>) -> Self {
        Self { by_database }
    }
}

impl ReplicaReadGate for DatabaseRoutingReplicaReadGate {
    fn has_read_quorum(&self, database: &str) -> bool {
        self.by_database
            .get(database)
            .is_some_and(|mesh| mesh.cluster().has_quorum())
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(env::args().skip(1)).await {
        eprintln!("exdb-server: {err}");
        std::process::exit(1);
    }
}

async fn run(args: impl IntoIterator<Item = String>) -> Result<(), ServerCliError> {
    let cli = CliOptions::parse_with_env(args)?;
    if cli.help {
        print_help();
        return Ok(());
    }

    let mut resolved = ResolvedConfig::load(&cli)?;
    if cli.check_config {
        println!(
            "exdb-server: configuration ok (data_root={}, tcp={})",
            resolved.data_root.display(),
            resolved.tcp_addr
        );
        return Ok(());
    }

    fs::create_dir_all(&resolved.data_root)?;
    let registry = Arc::new(SystemDatabase::open(&resolved.data_root).await?);
    let replication_runtimes = start_configured_replications(
        Arc::clone(&registry),
        &resolved.data_root,
        resolved.server.default_database_config.clone(),
        resolved.replication.clone(),
    )
    .await?;
    if let Some(role) = configured_replication_role(&replication_runtimes)? {
        resolved.server.node_role = node_role_name(role).to_string();
        resolved.server.transaction_promoter =
            configured_transaction_promoter(&replication_runtimes)?;
        resolved.server.replica_read_gate = configured_replica_read_gate(&replication_runtimes)?;
    }

    let server = Server::new(resolved.server, registry);
    let shutdown = CancellationToken::new();

    println!(
        "exdb-server: listening on {} (data_root={})",
        resolved.tcp_addr,
        resolved.data_root.display()
    );

    let result = tokio::select! {
        result = server.start(shutdown.clone()) => result.map_err(ServerCliError::Wire),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            shutdown.cancel();
            Ok(())
        }
    };

    for runtime in replication_runtimes {
        runtime.shutdown().await;
    }

    result
}

fn quarantine_replica_database_dir(database_path: &Path) -> Result<PathBuf, ServerCliError> {
    if !database_path.exists() {
        return Ok(database_path.to_path_buf());
    }

    let parent = database_path.parent().unwrap_or_else(|| Path::new("."));
    let database_dir = database_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("database");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    for attempt in 0..100 {
        let quarantine_path =
            parent.join(format!(".{database_dir}.corrupt-replica-{nonce}-{attempt}"));
        match fs::rename(database_path, &quarantine_path) {
            Ok(()) => return Ok(quarantine_path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(ServerCliError::Io(err)),
        }
    }

    Err(ServerCliError::Config(format!(
        "failed to choose quarantine path for replica database {}",
        database_path.display()
    )))
}

async fn start_configured_replication(
    registry: Arc<SystemDatabase>,
    data_root: &Path,
    database_config: DatabaseConfig,
    config: ReplicationConfig,
) -> Result<ServerReplicationRuntime, ServerCliError> {
    let cluster = Arc::new(ClusterMembership::new(ClusterConfig::new(
        config.self_id,
        config.topology.clone(),
    ))?);
    let database_path = data_root.join(&config.database);
    let data_file_exists = database_path.join("data.db").exists();

    match config.role {
        NodeRole::Primary => {
            let startup_generation = if data_file_exists {
                Database::read_generation_at_path(&database_path, database_config.clone()).await?
            } else {
                config.generation
            };
            let mesh = Arc::new(PeerMesh::new(
                Arc::clone(&cluster),
                NodeRole::Primary,
                startup_generation,
                0,
                0,
                Arc::new(NoopWalApplier),
            ));
            if data_file_exists {
                registry
                    .register_existing_database_with_replication(
                        &config.database,
                        database_config.clone(),
                        Some(Box::new(PrimaryReplicator::from_mesh(Arc::clone(&mesh)))),
                    )
                    .await?;
            } else {
                registry
                    .create_database_with_replication(
                        &config.database,
                        database_config.clone(),
                        Some(Box::new(PrimaryReplicator::from_mesh(Arc::clone(&mesh)))),
                    )
                    .await?;
            }
            let database = registry
                .get_database_by_name(&config.database)
                .ok_or_else(|| exdb::DatabaseError::DatabaseNotFound(config.database.clone()))?;
            if !data_file_exists {
                database.set_generation(startup_generation).await?;
            }
            mesh.set_oldest_retained_lsn(database.storage().oldest_retained_wal_lsn().unwrap_or(0));
            mesh.set_catchup_source(Arc::new(StorageWalCatchupSource::new(Arc::clone(
                database.storage(),
            ))));
            mesh.set_snapshot_source(Arc::new(DatabaseSnapshotSource::new(
                Arc::clone(&database),
                config.snapshot_chunk_bytes,
            )));
            mesh.set_promotion_handler(Arc::new(DatabasePromotionHandler::new(
                Arc::clone(&database),
                Arc::clone(&registry),
                database_config.clone(),
            )));
            let runtime = mesh.start_tcp().await?;
            Ok(ServerReplicationRuntime {
                database_name: config.database,
                runtime,
                node_role: NodeRole::Primary,
                startup_generation,
                startup_recovery_tier: None,
                startup_quarantined_path: None,
                transaction_promoter: None,
                _mesh: mesh,
                _database: Some(database),
                _replica_database_handle: None,
            })
        }
        NodeRole::Replica => {
            let mut startup_quarantined_path = None;
            let durable_probe = if data_file_exists {
                match Database::probe_durable_open_state_at_path(
                    &database_path,
                    database_config.clone(),
                )
                .await
                {
                    Ok(probe) => probe,
                    Err(_err) => {
                        startup_quarantined_path =
                            Some(quarantine_replica_database_dir(&database_path)?);
                        None
                    }
                }
            } else {
                None
            };
            let existing_database = if data_file_exists && startup_quarantined_path.is_none() {
                match registry
                    .register_existing_database(&config.database, database_config.clone())
                    .await
                {
                    Ok(database) => Some(database),
                    Err(_err) => {
                        startup_quarantined_path =
                            Some(quarantine_replica_database_dir(&database_path)?);
                        None
                    }
                }
            } else {
                None
            };
            let startup_generation = match &existing_database {
                Some(database) => database.generation().await,
                None => config.generation,
            };
            let startup_applied_lsn = match &existing_database {
                Some(database) => database.replication_applied_lsn().await,
                None => 0,
            };
            let startup_recovery_tier = if existing_database.is_some() {
                let wal_intact = durable_probe
                    .as_ref()
                    .map(|probe| !probe.recovery_needed)
                    .unwrap_or(true);
                Some(select_recovery_tier(
                    &LocalState {
                        data_file_exists,
                        data_file_valid: true,
                        wal_intact,
                        applied_lsn: Some(startup_applied_lsn),
                    },
                    0,
                    startup_generation,
                    Some(startup_generation),
                ))
            } else {
                Some(RecoveryTier::FullReconstruction)
            };
            let database_handle = Arc::new(ReplicaDatabaseHandle::new(existing_database.clone()));
            let mesh = Arc::new(PeerMesh::new(
                Arc::clone(&cluster),
                NodeRole::Replica,
                startup_generation,
                startup_applied_lsn,
                0,
                database_handle.clone(),
            ));
            mesh.set_force_snapshot_on_primary_attach(existing_database.is_none());
            mesh.set_snapshot_sink(Arc::new(RegisteringDatabaseSnapshotSink::new(
                Arc::clone(&registry),
                config.database.clone(),
                database_path,
                database_config,
                Arc::clone(&database_handle),
            )));
            let runtime = mesh.start_tcp().await?;
            let transaction_promoter: Arc<dyn TransactionPromoter> =
                Arc::new(ReplicaTransactionPromoter::new(Arc::clone(&mesh)));
            Ok(ServerReplicationRuntime {
                database_name: config.database,
                runtime,
                node_role: NodeRole::Replica,
                startup_generation,
                startup_recovery_tier,
                startup_quarantined_path,
                transaction_promoter: Some(transaction_promoter),
                _mesh: mesh,
                _database: None,
                _replica_database_handle: Some(database_handle),
            })
        }
    }
}

async fn start_configured_replications(
    registry: Arc<SystemDatabase>,
    data_root: &Path,
    database_config: DatabaseConfig,
    configs: Vec<ReplicationConfig>,
) -> Result<Vec<ServerReplicationRuntime>, ServerCliError> {
    let mut seen_databases = std::collections::HashSet::new();
    for config in &configs {
        if !seen_databases.insert(config.database.clone()) {
            return Err(ServerCliError::Config(format!(
                "replication database configured more than once: {}",
                config.database
            )));
        }
    }
    configured_replication_config_role(&configs)?;

    let mut runtimes = Vec::with_capacity(configs.len());
    for config in configs {
        runtimes.push(
            start_configured_replication(
                Arc::clone(&registry),
                data_root,
                database_config.clone(),
                config,
            )
            .await?,
        );
    }
    Ok(runtimes)
}

fn configured_replication_config_role(
    configs: &[ReplicationConfig],
) -> Result<Option<NodeRole>, ServerCliError> {
    let Some(first) = configs.first() else {
        return Ok(None);
    };
    let role = first.role;
    if configs.iter().any(|config| config.role != role) {
        return Err(ServerCliError::Config(
            "all configured replication databases must use the same role".to_string(),
        ));
    }
    Ok(Some(role))
}

fn configured_replication_role(
    runtimes: &[ServerReplicationRuntime],
) -> Result<Option<NodeRole>, ServerCliError> {
    let Some(first) = runtimes.first() else {
        return Ok(None);
    };
    let role = first.node_role;
    if runtimes.iter().any(|runtime| runtime.node_role != role) {
        return Err(ServerCliError::Config(
            "all configured replication databases must use the same role".to_string(),
        ));
    }
    Ok(Some(role))
}

fn configured_transaction_promoter(
    runtimes: &[ServerReplicationRuntime],
) -> Result<Option<Arc<dyn TransactionPromoter>>, ServerCliError> {
    if configured_replication_role(runtimes)? != Some(NodeRole::Replica) {
        return Ok(None);
    }

    let mut by_database = HashMap::new();
    let mut default_promoter = None;
    for runtime in runtimes {
        let promoter = runtime.transaction_promoter.clone().ok_or_else(|| {
            ServerCliError::Config(format!(
                "replica database {} did not install a transaction promoter",
                runtime.database_name
            ))
        })?;
        default_promoter.get_or_insert_with(|| Arc::clone(&promoter));
        by_database.insert(runtime.database_name.clone(), promoter);
    }

    let Some(default_promoter) = default_promoter else {
        return Ok(None);
    };
    Ok(Some(Arc::new(DatabaseRoutingTransactionPromoter::new(
        by_database,
        default_promoter,
    ))))
}

fn configured_replica_read_gate(
    runtimes: &[ServerReplicationRuntime],
) -> Result<Option<Arc<dyn ReplicaReadGate>>, ServerCliError> {
    if configured_replication_role(runtimes)? != Some(NodeRole::Replica) {
        return Ok(None);
    }

    let mut by_database = HashMap::new();
    for runtime in runtimes {
        by_database.insert(runtime.database_name.clone(), Arc::clone(&runtime._mesh));
    }
    if by_database.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(DatabaseRoutingReplicaReadGate::new(
            by_database,
        ))))
    }
}

fn print_help() {
    println!(
        "exdb-server {}\n\
\n\
Usage:\n\
  exdb-server [OPTIONS]\n\
\n\
Options:\n\
  --config <PATH>            JSON config file (DESIGN.md section 7.10)\n\
  --data-root <PATH>         Root directory for databases [default: ./data]\n\
  --listen-tcp <ADDR>        TCP listen address [default: 0.0.0.0:5200]\n\
  --max-message-size <N>     Maximum wire message size in bytes\n\
  --request-queue-capacity <N>\n\
                             Parsed request buffer per connection\n\
  --response-write-timeout-ms <N>\n\
                             Maximum time to spend writing one response frame\n\
  --check-config             Validate config and exit without listening\n\
  -h, --help                 Show this help\n\
\n\
Environment:\n\
  EXDB_CONFIG, EXDB_DATA_ROOT, EXDB_LISTEN_TCP, EXDB_MAX_MESSAGE_SIZE,\n\
  EXDB_REQUEST_QUEUE_CAPACITY, EXDB_RESPONSE_WRITE_TIMEOUT_MS\n",
        env!("CARGO_PKG_VERSION")
    );
}

#[derive(Debug, Default)]
struct CliOptions {
    config_path: Option<PathBuf>,
    data_root: Option<PathBuf>,
    listen_tcp: Option<SocketAddr>,
    max_message_size: Option<usize>,
    request_queue_capacity: Option<usize>,
    response_write_timeout_ms: Option<u64>,
    check_config: bool,
    help: bool,
}

impl CliOptions {
    fn parse_with_env(args: impl IntoIterator<Item = String>) -> Result<Self, ServerCliError> {
        Self::parse_with_base(Self::from_env()?, args)
    }

    #[cfg(test)]
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, ServerCliError> {
        Self::parse_with_base(Self::default(), args)
    }

    fn parse_with_base(
        mut opts: Self,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, ServerCliError> {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-h" | "--help" => opts.help = true,
                "--check-config" => opts.check_config = true,
                "--config" => opts.config_path = Some(next_path(&mut args, "--config")?),
                "--data-root" => opts.data_root = Some(next_path(&mut args, "--data-root")?),
                "--listen-tcp" => {
                    opts.listen_tcp = Some(next_addr(&mut args, "--listen-tcp")?);
                }
                "--max-message-size" => {
                    opts.max_message_size = Some(next_usize(&mut args, "--max-message-size")?);
                }
                "--request-queue-capacity" => {
                    opts.request_queue_capacity =
                        Some(next_usize(&mut args, "--request-queue-capacity")?);
                }
                "--response-write-timeout-ms" => {
                    opts.response_write_timeout_ms =
                        Some(next_u64(&mut args, "--response-write-timeout-ms")?);
                }
                other => {
                    return Err(ServerCliError::Config(format!("unknown argument: {other}")));
                }
            }
        }
        Ok(opts)
    }

    fn from_env() -> Result<Self, ServerCliError> {
        Ok(Self {
            config_path: env::var_os("EXDB_CONFIG").map(PathBuf::from),
            data_root: env::var_os("EXDB_DATA_ROOT").map(PathBuf::from),
            listen_tcp: parse_optional_addr("EXDB_LISTEN_TCP", env::var("EXDB_LISTEN_TCP").ok())?,
            max_message_size: parse_optional_usize(
                "EXDB_MAX_MESSAGE_SIZE",
                env::var("EXDB_MAX_MESSAGE_SIZE").ok(),
            )?,
            request_queue_capacity: parse_optional_usize(
                "EXDB_REQUEST_QUEUE_CAPACITY",
                env::var("EXDB_REQUEST_QUEUE_CAPACITY").ok(),
            )?,
            response_write_timeout_ms: parse_optional_u64(
                "EXDB_RESPONSE_WRITE_TIMEOUT_MS",
                env::var("EXDB_RESPONSE_WRITE_TIMEOUT_MS").ok(),
            )?,
            check_config: false,
            help: false,
        })
    }
}

#[derive(Debug)]
struct ResolvedConfig {
    data_root: PathBuf,
    tcp_addr: SocketAddr,
    server: ServerConfig,
    replication: Vec<ReplicationConfig>,
}

impl ResolvedConfig {
    fn load(cli: &CliOptions) -> Result<Self, ServerCliError> {
        let file = match &cli.config_path {
            Some(path) => Some(FileConfig::read(path)?),
            None => None,
        };

        let data_root = cli
            .data_root
            .clone()
            .or_else(|| file.as_ref().and_then(|config| config.data_root.clone()))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_ROOT));

        let tcp_addr = cli
            .listen_tcp
            .or_else(|| file.as_ref().and_then(|config| config.listen.as_ref()?.tcp))
            .unwrap_or(DEFAULT_TCP_ADDR.parse().expect("valid default TCP address"));

        let max_message_size = cli
            .max_message_size
            .or_else(|| file.as_ref().and_then(|config| config.max_message_size))
            .unwrap_or(exdb_wire::DEFAULT_MAX_MESSAGE_SIZE);

        let request_queue_capacity = cli
            .request_queue_capacity
            .or_else(|| {
                file.as_ref()
                    .and_then(|config| config.request_queue_capacity)
            })
            .unwrap_or(exdb_wire::DEFAULT_REQUEST_QUEUE_CAPACITY);

        let response_write_timeout = Duration::from_millis(
            cli.response_write_timeout_ms
                .or_else(|| {
                    file.as_ref()
                        .and_then(|config| config.response_write_timeout_ms)
                })
                .unwrap_or_else(|| {
                    u64::try_from(exdb_wire::DEFAULT_RESPONSE_WRITE_TIMEOUT.as_millis())
                        .unwrap_or(u64::MAX)
                }),
        );

        let mut default_database_config = DatabaseConfig::default();
        if let Some(transactions) = file.as_ref().and_then(|config| config.transactions.clone()) {
            default_database_config = database_config_from_json(
                Some(json!({ "transaction": transactions })),
                &default_database_config,
            )
            .map_err(ServerCliError::Config)?;
        }
        if let Some(value) = file
            .as_ref()
            .and_then(|config| config.default_database_config.clone())
        {
            default_database_config =
                database_config_from_json(Some(value), &default_database_config)
                    .map_err(ServerCliError::Config)?;
        }
        validate_server_config(
            max_message_size,
            request_queue_capacity,
            response_write_timeout,
            &default_database_config,
        )?;

        let auth = match file.as_ref().and_then(|config| config.auth.as_ref()) {
            Some(auth) => auth.to_auth_config()?,
            None => AuthConfig::default(),
        };

        let tls = match file.as_ref().and_then(|config| config.tls.as_ref()) {
            Some(tls) => Some(tls.to_tls_config()?),
            None => None,
        };

        let listen = ListenConfig {
            tcp: Some(tcp_addr),
            tls: file.as_ref().and_then(|config| config.listen.as_ref()?.tls),
            quic: file
                .as_ref()
                .and_then(|config| config.listen.as_ref()?.quic),
            websocket: file
                .as_ref()
                .and_then(|config| config.listen.as_ref()?.websocket),
            websocket_tls: file
                .as_ref()
                .and_then(|config| config.listen.as_ref()?.websocket_tls),
        };
        let replication = file
            .as_ref()
            .and_then(|config| config.replication.as_ref())
            .map(FileReplicationConfigs::to_replication_configs)
            .transpose()?
            .unwrap_or_default();

        let server = ServerConfig {
            listen,
            tls,
            auth,
            node_role: "primary".to_string(),
            transaction_promoter: None,
            replica_read_gate: None,
            max_message_size,
            request_queue_capacity,
            response_write_timeout,
            default_database_config,
        };
        server
            .validate()
            .map_err(|err| ServerCliError::Config(err.to_string()))?;

        Ok(Self {
            data_root,
            tcp_addr,
            server,
            replication,
        })
    }
}

fn validate_server_config(
    max_message_size: usize,
    request_queue_capacity: usize,
    response_write_timeout: Duration,
    default_database_config: &DatabaseConfig,
) -> Result<(), ServerCliError> {
    ServerConfig {
        listen: ListenConfig::default(),
        tls: None,
        auth: AuthConfig::default(),
        node_role: DEFAULT_NODE_ROLE.to_string(),
        transaction_promoter: None,
        replica_read_gate: None,
        max_message_size,
        request_queue_capacity,
        response_write_timeout,
        default_database_config: default_database_config.clone(),
    }
    .validate()
    .map_err(|err| ServerCliError::Config(err.to_string()))
}

#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    #[serde(default)]
    listen: Option<FileListenConfig>,
    #[serde(default)]
    tls: Option<FileTlsConfig>,
    #[serde(default)]
    auth: Option<FileAuthConfig>,
    #[serde(default)]
    data_root: Option<PathBuf>,
    #[serde(default)]
    max_message_size: Option<usize>,
    #[serde(default)]
    request_queue_capacity: Option<usize>,
    #[serde(default)]
    response_write_timeout_ms: Option<u64>,
    #[serde(default)]
    transactions: Option<serde_json::Value>,
    #[serde(default)]
    default_database_config: Option<serde_json::Value>,
    #[serde(default)]
    replication: Option<FileReplicationConfigs>,
}

#[derive(Debug, Clone)]
struct ReplicationConfig {
    database: String,
    self_id: NodeId,
    role: NodeRole,
    generation: u64,
    topology: HashMap<NodeId, SocketAddr>,
    snapshot_chunk_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct FileReplicationConfig {
    #[serde(default = "default_replication_database")]
    database: String,
    self_id: NodeId,
    role: String,
    #[serde(default = "default_generation")]
    generation: u64,
    topology: HashMap<NodeId, SocketAddr>,
    #[serde(default = "default_replication_snapshot_chunk_bytes")]
    snapshot_chunk_bytes: usize,
}

#[derive(Debug)]
enum FileReplicationConfigs {
    Single(FileReplicationConfig),
    Many(Vec<FileReplicationConfig>),
}

impl<'de> Deserialize<'de> for FileReplicationConfigs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if value.is_array() {
            let configs = Vec::<FileReplicationConfig>::deserialize(value)
                .map_err(serde::de::Error::custom)?;
            Ok(FileReplicationConfigs::Many(configs))
        } else {
            let config =
                FileReplicationConfig::deserialize(value).map_err(serde::de::Error::custom)?;
            Ok(FileReplicationConfigs::Single(config))
        }
    }
}

impl FileReplicationConfigs {
    fn to_replication_configs(&self) -> Result<Vec<ReplicationConfig>, ServerCliError> {
        match self {
            FileReplicationConfigs::Single(config) => Ok(vec![config.to_replication_config()?]),
            FileReplicationConfigs::Many(configs) => configs
                .iter()
                .map(FileReplicationConfig::to_replication_config)
                .collect(),
        }
    }
}

impl FileReplicationConfig {
    fn to_replication_config(&self) -> Result<ReplicationConfig, ServerCliError> {
        let role = parse_node_role(&self.role)?;
        if !self.topology.contains_key(&self.self_id) {
            return Err(ServerCliError::Config(format!(
                "replication topology must include self_id {}",
                self.self_id
            )));
        }
        if self.snapshot_chunk_bytes == 0 {
            return Err(ServerCliError::Config(
                "replication.snapshot_chunk_bytes must be greater than zero".to_string(),
            ));
        }
        Ok(ReplicationConfig {
            database: self.database.clone(),
            self_id: self.self_id,
            role,
            generation: self.generation,
            topology: self.topology.clone(),
            snapshot_chunk_bytes: self.snapshot_chunk_bytes,
        })
    }
}

fn default_replication_database() -> String {
    DEFAULT_REPLICATION_DATABASE.to_string()
}

fn default_generation() -> u64 {
    1
}

fn default_replication_snapshot_chunk_bytes() -> usize {
    DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES
}

impl FileConfig {
    fn read(path: &Path) -> Result<Self, ServerCliError> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(ServerCliError::Json)
    }
}

#[derive(Debug, Deserialize, Default)]
struct FileListenConfig {
    #[serde(default)]
    tcp: Option<SocketAddr>,
    #[serde(default)]
    tls: Option<SocketAddr>,
    #[serde(default)]
    quic: Option<SocketAddr>,
    #[serde(default)]
    websocket: Option<SocketAddr>,
    #[serde(default)]
    websocket_tls: Option<SocketAddr>,
}

#[derive(Debug, Deserialize)]
struct FileTlsConfig {
    cert_file: PathBuf,
    key_file: PathBuf,
}

impl FileTlsConfig {
    fn to_tls_config(&self) -> Result<TlsConfig, ServerCliError> {
        Ok(TlsConfig::from_pem(
            fs::read(&self.cert_file)?,
            fs::read(&self.key_file)?,
        ))
    }
}

#[derive(Debug, Deserialize, Default)]
struct FileAuthConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_jwt_algorithm")]
    jwt_algorithm: String,
    #[serde(default)]
    jwt_secret: Option<String>,
    #[serde(default)]
    jwt_public_key_file: Option<PathBuf>,
    #[serde(default)]
    jwt_issuer: Option<String>,
}

impl FileAuthConfig {
    fn to_auth_config(&self) -> Result<AuthConfig, ServerCliError> {
        if !self.enabled {
            return Ok(AuthConfig::default());
        }

        let algorithm = parse_jwt_algorithm(&self.jwt_algorithm)?;
        let mut auth = if matches!(
            algorithm,
            JwtAlgorithm::HS256 | JwtAlgorithm::HS384 | JwtAlgorithm::HS512
        ) {
            let secret = self.jwt_secret.as_deref().ok_or_else(|| {
                ServerCliError::Config(
                    "auth.jwt_secret is required for HMAC JWT algorithms".to_string(),
                )
            })?;
            AuthConfig::hmac_base64(algorithm, secret).map_err(wire_config_error)?
        } else {
            let path = self.jwt_public_key_file.as_ref().ok_or_else(|| {
                ServerCliError::Config(
                    "auth.jwt_public_key_file is required for RSA/EC JWT algorithms".to_string(),
                )
            })?;
            AuthConfig::public_key(algorithm, fs::read(path)?)
        };

        if let Some(issuer) = &self.jwt_issuer {
            auth = auth.with_issuer(issuer.clone());
        }
        Ok(auth)
    }
}

fn wire_config_error(err: exdb_wire::WireError) -> ServerCliError {
    match err {
        exdb_wire::WireError::InvalidMessage(message) => ServerCliError::Config(message),
        other => ServerCliError::Config(other.to_string()),
    }
}

fn default_jwt_algorithm() -> String {
    "HS256".to_string()
}

fn parse_jwt_algorithm(value: &str) -> Result<JwtAlgorithm, ServerCliError> {
    match value {
        "HS256" => Ok(JwtAlgorithm::HS256),
        "HS384" => Ok(JwtAlgorithm::HS384),
        "HS512" => Ok(JwtAlgorithm::HS512),
        "RS256" => Ok(JwtAlgorithm::RS256),
        "RS384" => Ok(JwtAlgorithm::RS384),
        "RS512" => Ok(JwtAlgorithm::RS512),
        "ES256" => Ok(JwtAlgorithm::ES256),
        "ES384" => Ok(JwtAlgorithm::ES384),
        other => Err(ServerCliError::Config(format!(
            "unsupported jwt_algorithm: {other}"
        ))),
    }
}

fn parse_node_role(value: &str) -> Result<NodeRole, ServerCliError> {
    match value {
        "primary" => Ok(NodeRole::Primary),
        "replica" => Ok(NodeRole::Replica),
        other => Err(ServerCliError::Config(format!(
            "unsupported replication role: {other}"
        ))),
    }
}

fn node_role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Primary => "primary",
        NodeRole::Replica => "replica",
    }
}

fn next_path(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<PathBuf, ServerCliError> {
    args.next()
        .map(PathBuf::from)
        .ok_or_else(|| ServerCliError::Config(format!("{flag} requires a value")))
}

fn next_addr(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<SocketAddr, ServerCliError> {
    let value = args
        .next()
        .ok_or_else(|| ServerCliError::Config(format!("{flag} requires a value")))?;
    value
        .parse()
        .map_err(|e| ServerCliError::Config(format!("{flag} expects socket address: {e}")))
}

fn next_usize(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<usize, ServerCliError> {
    let value = args
        .next()
        .ok_or_else(|| ServerCliError::Config(format!("{flag} requires a value")))?;
    value
        .parse()
        .map_err(|e| ServerCliError::Config(format!("{flag} expects integer: {e}")))
}

fn next_u64(
    args: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<u64, ServerCliError> {
    let value = args
        .next()
        .ok_or_else(|| ServerCliError::Config(format!("{flag} requires a value")))?;
    value
        .parse()
        .map_err(|e| ServerCliError::Config(format!("{flag} expects integer: {e}")))
}

fn parse_optional_addr(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<SocketAddr>, ServerCliError> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|e| ServerCliError::Config(format!("{name} expects socket address: {e}")))
        })
        .transpose()
}

fn parse_optional_usize(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<usize>, ServerCliError> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|e| ServerCliError::Config(format!("{name} expects integer: {e}")))
        })
        .transpose()
}

fn parse_optional_u64(
    name: &'static str,
    value: Option<String>,
) -> Result<Option<u64>, ServerCliError> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|e| ServerCliError::Config(format!("{name} expects integer: {e}")))
        })
        .transpose()
}

#[derive(Debug)]
enum ServerCliError {
    Config(String),
    Database(exdb::DatabaseError),
    Io(std::io::Error),
    Json(serde_json::Error),
    Replication(ReplicationError),
    Wire(exdb_wire::WireError),
}

impl fmt::Display for ServerCliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerCliError::Config(message) => f.write_str(message),
            ServerCliError::Database(err) => write!(f, "{err}"),
            ServerCliError::Io(err) => write!(f, "{err}"),
            ServerCliError::Json(err) => write!(f, "{err}"),
            ServerCliError::Replication(err) => write!(f, "{err}"),
            ServerCliError::Wire(err) => write!(f, "{err}"),
        }
    }
}

impl Error for ServerCliError {}

impl From<exdb::DatabaseError> for ServerCliError {
    fn from(value: exdb::DatabaseError) -> Self {
        ServerCliError::Database(value)
    }
}

impl From<std::io::Error> for ServerCliError {
    fn from(value: std::io::Error) -> Self {
        ServerCliError::Io(value)
    }
}

impl From<exdb_wire::WireError> for ServerCliError {
    fn from(value: exdb_wire::WireError) -> Self {
        ServerCliError::Wire(value)
    }
}

impl From<ReplicationError> for ServerCliError {
    fn from(value: ReplicationError) -> Self {
        ServerCliError::Replication(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb::{FieldPath, RangeExpr, Scalar, TransactionOptions, TransactionResult};
    use exdb_replication::{
        ClusterConfig, ClusterMembership, NodeRole, PeerMesh, PrimaryReplicator,
        StorageWalCatchupSource, WalCatchupSource,
    };
    use std::collections::HashMap;
    use std::net::TcpListener as StdTcpListener;
    use tokio::net::TcpListener;
    use tokio::time::sleep;

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

    struct NoopWalApplier;

    #[async_trait::async_trait]
    impl WalApplier for NoopWalApplier {
        async fn apply_wal_record(
            &self,
            _peer_id: NodeId,
            lsn: u64,
            _record: &[u8],
        ) -> Result<(u64, exdb::Ts), ReplicationError> {
            Ok((lsn, 0))
        }
    }

    struct UnavailableCatchupSource {
        oldest_retained_lsn: u64,
    }

    #[async_trait::async_trait]
    impl WalCatchupSource for UnavailableCatchupSource {
        async fn records_from(
            &self,
            from_lsn: u64,
        ) -> Result<Vec<(u64, Vec<u8>)>, ReplicationError> {
            Err(ReplicationError::CatchupUnavailable {
                requested_lsn: from_lsn,
                oldest_retained_lsn: self.oldest_retained_lsn,
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedPromotion {
        database: String,
        begin_ts: exdb::Ts,
        subscription: SubscriptionMode,
        payload: Vec<u8>,
    }

    struct RecordingTransactionPromoter {
        label: &'static str,
        transactions: StdMutex<Vec<RecordedPromotion>>,
        ddl: StdMutex<Vec<DdlPromotionRequest>>,
    }

    impl RecordingTransactionPromoter {
        fn new(label: &'static str) -> Self {
            Self {
                label,
                transactions: StdMutex::new(Vec::new()),
                ddl: StdMutex::new(Vec::new()),
            }
        }

        fn transactions(&self) -> Vec<RecordedPromotion> {
            self.transactions.lock().unwrap().clone()
        }

        fn ddl(&self) -> Vec<DdlPromotionRequest> {
            self.ddl.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl TransactionPromoter for RecordingTransactionPromoter {
        async fn promote_transaction(
            &self,
            database: &str,
            begin_ts: exdb::Ts,
            subscription: SubscriptionMode,
            payload: Vec<u8>,
        ) -> Result<TransactionPromotionOutcome, String> {
            self.transactions.lock().unwrap().push(RecordedPromotion {
                database: database.to_string(),
                begin_ts,
                subscription,
                payload,
            });
            Ok(TransactionPromotionOutcome::Success { commit_ts: 42 })
        }

        async fn promote_ddl(&self, request: DdlPromotionRequest) -> Result<ServerMessage, String> {
            self.ddl.lock().unwrap().push(request);
            Ok(ServerMessage::Ok {
                fields: json!({ "promoter": self.label }),
            })
        }
    }

    fn assert_success(result: TransactionResult) -> exdb::Ts {
        match result {
            TransactionResult::Success { commit_ts, .. } => commit_ts,
            TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
            TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
        }
    }

    fn bound_listener() -> (TcpListener, SocketAddr) {
        let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        (TcpListener::from_std(std_listener).unwrap(), addr)
    }

    async fn promotion_handler_fixture() -> (tempfile::TempDir, Arc<SystemDatabase>, Arc<Database>)
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        registry
            .create_database("default", DatabaseConfig::default())
            .await
            .unwrap();
        let db = registry.get_database_by_name("default").unwrap();
        (tmp, registry, db)
    }

    async fn eventually(mut condition: impl FnMut() -> bool) {
        for _ in 0..100 {
            if condition() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(condition());
    }

    async fn snapshot_chunks_with_document(path: &Path, name: &str) -> (Vec<Vec<u8>>, exdb::DocId) {
        let db = Database::open(path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", json!({ "name": name, "snapshot": true }))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let chunks = db
            .export_snapshot()
            .await
            .unwrap()
            .into_chunks(1024)
            .unwrap();
        db.close().await.unwrap();
        (chunks, doc_id)
    }

    #[tokio::test]
    async fn database_snapshot_sink_restarts_after_abandoned_partial_stream() {
        let tmp = tempfile::TempDir::new().unwrap();
        let source_path = tmp.path().join("source");
        let target_path = tmp.path().join("target");
        let (chunks, doc_id) = snapshot_chunks_with_document(&source_path, "Ada").await;

        let sink = DatabaseSnapshotSink::new(target_path.clone(), DatabaseConfig::default());
        sink.begin_snapshot(1).await.unwrap();
        sink.apply_snapshot_chunk(1, &chunks[0]).await.unwrap();

        sink.begin_snapshot(1).await.unwrap();
        for chunk in &chunks {
            sink.apply_snapshot_chunk(1, chunk).await.unwrap();
        }
        sink.end_snapshot(1).await.unwrap();

        let restored = Database::open(&target_path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = restored.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &doc_id).await.unwrap().unwrap()["name"],
            "Ada"
        );
        tx.rollback();
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn registering_snapshot_sink_restarts_after_abandoned_partial_stream() {
        let tmp = tempfile::TempDir::new().unwrap();
        let source_path = tmp.path().join("source");
        let replica_root = tmp.path().join("replica");
        let target_path = replica_root.join("app");
        let (chunks, doc_id) = snapshot_chunks_with_document(&source_path, "Grace").await;

        let registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let database_handle = Arc::new(ReplicaDatabaseHandle::default());
        let sink = RegisteringDatabaseSnapshotSink::new(
            Arc::clone(&registry),
            "app".to_string(),
            target_path,
            DatabaseConfig::default(),
            Arc::clone(&database_handle),
        );

        sink.begin_snapshot(1).await.unwrap();
        sink.apply_snapshot_chunk(1, &chunks[0]).await.unwrap();

        sink.begin_snapshot(1).await.unwrap();
        for chunk in &chunks {
            sink.apply_snapshot_chunk(1, chunk).await.unwrap();
        }
        sink.end_snapshot(1).await.unwrap();

        let restored = registry
            .get_database_by_name("app")
            .expect("snapshot should register the restored database");
        assert!(database_handle.database().is_some());
        let mut tx = restored.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &doc_id).await.unwrap().unwrap()["name"],
            "Grace"
        );
        tx.rollback();
        drop(restored);
        database_handle.clear();
        drop(sink);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn registering_snapshot_sink_preserves_live_database_on_replacement_failure() {
        let tmp = tempfile::TempDir::new().unwrap();
        let source_path = tmp.path().join("source");
        let replica_root = tmp.path().join("replica");
        let target_path = replica_root.join("app");
        let (chunks, new_doc_id) = snapshot_chunks_with_document(&source_path, "new").await;

        let existing = Database::open(&target_path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = existing.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        let mut tx = existing.begin(TransactionOptions::default()).unwrap();
        let old_doc_id = tx
            .insert("users", json!({"name": "old", "snapshot": false}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        existing.close().await.unwrap();

        let registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let old_database = registry
            .register_existing_database("app", DatabaseConfig::default())
            .await
            .unwrap();
        let database_handle = Arc::new(ReplicaDatabaseHandle::new(Some(Arc::clone(&old_database))));
        let sink = RegisteringDatabaseSnapshotSink::new(
            Arc::clone(&registry),
            "app".to_string(),
            target_path,
            DatabaseConfig::default(),
            Arc::clone(&database_handle),
        );

        sink.begin_snapshot(1).await.unwrap();
        for chunk in &chunks {
            sink.apply_snapshot_chunk(1, chunk).await.unwrap();
        }
        let err = sink.end_snapshot(1).await.unwrap_err();
        assert!(
            err.to_string().contains("database is in use"),
            "expected DatabaseInUse replacement failure, got {err}"
        );

        let restored_handle = database_handle
            .database()
            .expect("previous handle should be restored after replacement failure");
        assert!(Arc::ptr_eq(&old_database, &restored_handle));
        let mut tx = restored_handle
            .begin(TransactionOptions::readonly())
            .unwrap();
        assert_eq!(
            tx.get("users", &old_doc_id).await.unwrap().unwrap()["name"],
            "old"
        );
        assert!(
            tx.get("users", &new_doc_id).await.unwrap().is_none(),
            "failed snapshot replacement must not install new snapshot data"
        );
        tx.rollback();

        drop(restored_handle);
        drop(old_database);
        database_handle.clear();
        drop(sink);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[test]
    fn cli_flags_override_defaults() {
        let opts = CliOptions::parse([
            "--data-root".to_string(),
            "tmp-data".to_string(),
            "--listen-tcp".to_string(),
            "127.0.0.1:9000".to_string(),
            "--max-message-size".to_string(),
            "1234".to_string(),
            "--request-queue-capacity".to_string(),
            "7".to_string(),
            "--response-write-timeout-ms".to_string(),
            "250".to_string(),
        ])
        .unwrap();
        let resolved = ResolvedConfig::load(&opts).unwrap();

        assert_eq!(resolved.data_root, PathBuf::from("tmp-data"));
        assert_eq!(resolved.tcp_addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(resolved.server.max_message_size, 1234);
        assert_eq!(resolved.server.request_queue_capacity, 7);
        assert_eq!(
            resolved.server.response_write_timeout,
            Duration::from_millis(250)
        );
        assert!(!resolved.server.auth.enabled);
    }

    #[test]
    fn server_config_rejects_unusable_transport_limits() {
        let opts = CliOptions::parse(["--max-message-size".to_string(), "0".to_string()]).unwrap();
        let err = ResolvedConfig::load(&opts).unwrap_err();
        assert!(err.to_string().contains("max_message_size"));

        let opts =
            CliOptions::parse(["--request-queue-capacity".to_string(), "0".to_string()]).unwrap();
        let err = ResolvedConfig::load(&opts).unwrap_err();
        assert!(err.to_string().contains("request_queue_capacity"));

        let opts = CliOptions::parse(["--response-write-timeout-ms".to_string(), "0".to_string()])
            .unwrap();
        let err = ResolvedConfig::load(&opts).unwrap_err();
        assert!(err.to_string().contains("response_write_timeout_ms"));
    }

    #[test]
    fn server_config_rejects_invalid_default_database_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.json");
        fs::write(
            &config_path,
            r#"{
                "default_database_config": {
                    "page_size": 4096,
                    "memory_budget": 1024
                }
            }"#,
        )
        .unwrap();

        let opts = CliOptions::parse([
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let err = ResolvedConfig::load(&opts).unwrap_err();
        assert!(
            err.to_string()
                .contains("memory_budget must be at least page_size")
        );
    }

    #[test]
    fn json_config_rejects_secure_listeners_without_tls_material() {
        let cases = [
            (
                r#"{ "listen": { "tls": "127.0.0.1:5210" } }"#,
                "TLS listener requires",
            ),
            (
                r#"{ "listen": { "quic": "127.0.0.1:5210" } }"#,
                "QUIC listener requires",
            ),
            (
                r#"{ "listen": { "websocket_tls": "127.0.0.1:5212" } }"#,
                "secure WebSocket listener requires",
            ),
        ];

        for (body, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let config_path = dir.path().join("server.json");
            fs::write(&config_path, body).unwrap();

            let opts = CliOptions::parse([
                "--config".to_string(),
                config_path.to_string_lossy().to_string(),
            ])
            .unwrap();
            let err = ResolvedConfig::load(&opts).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected '{expected}' in '{err}'"
            );
        }
    }

    #[test]
    fn json_config_rejects_malformed_tls_material() {
        let cases = [
            (
                "not a certificate",
                TEST_KEY_PEM,
                "certificate PEM did not contain",
            ),
            (
                std::str::from_utf8(TEST_CERT_PEM).unwrap(),
                b"not a private key".as_slice(),
                "private key PEM did not contain",
            ),
        ];

        for (cert, key, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let config_path = dir.path().join("server.json");
            let cert_path = dir.path().join("cert.pem");
            let key_path = dir.path().join("key.pem");
            fs::write(&cert_path, cert).unwrap();
            fs::write(&key_path, key).unwrap();
            fs::write(
                &config_path,
                format!(
                    r#"{{
                    "listen": {{ "tls": "127.0.0.1:5210" }},
                    "tls": {{
                        "cert_file": "{}",
                        "key_file": "{}"
                    }}
                }}"#,
                    cert_path.display(),
                    key_path.display()
                ),
            )
            .unwrap();

            let opts = CliOptions::parse([
                "--config".to_string(),
                config_path.to_string_lossy().to_string(),
            ])
            .unwrap();
            let err = ResolvedConfig::load(&opts).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected '{expected}' in '{err}'"
            );
        }
    }

    #[test]
    fn json_config_builds_auth_and_listen_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.json");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        fs::write(&cert_path, TEST_CERT_PEM).unwrap();
        fs::write(&key_path, TEST_KEY_PEM).unwrap();
        fs::write(
            &config_path,
            format!(
                r#"{{
                "listen": {{ "tcp": "127.0.0.1:5209", "tls": "127.0.0.1:5210", "quic": "127.0.0.1:5210", "websocket": "127.0.0.1:5211", "websocket_tls": "127.0.0.1:5212" }},
                "tls": {{
                    "cert_file": "{}",
                    "key_file": "{}"
                }},
                "auth": {{
                    "enabled": true,
                    "jwt_algorithm": "HS256",
                    "jwt_secret": "c2VjcmV0",
                    "jwt_issuer": "issuer"
                }},
                "data_root": "configured-data",
                "max_message_size": 4096,
                "request_queue_capacity": 9,
                "response_write_timeout_ms": 750,
                "transactions": {{
                    "idle_timeout": "9s",
                    "max_lifetime": "10m",
                    "max_intervals": 55,
                    "max_scanned_bytes": 66
                }},
                "default_database_config": {{
                    "page_size": 4096,
                    "memory_budget": 123456,
                    "max_disk_usage_bytes": 987654,
                    "wal_retention_max_size": 12345,
                    "wal_retention_max_age": "6s",
                    "max_doc_size": 2048,
                    "checkpoint_interval": "2s",
                    "close_timeout": "5s",
                    "transaction": {{
                        "idle_timeout": "3s",
                        "max_lifetime": "4m",
                        "max_operations": 34,
                        "max_scanned_docs": 12
                    }}
                }}
            }}"#,
                cert_path.display(),
                key_path.display()
            ),
        )
        .unwrap();

        let opts = CliOptions::parse([
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let resolved = ResolvedConfig::load(&opts).unwrap();

        assert_eq!(resolved.data_root, PathBuf::from("configured-data"));
        assert_eq!(resolved.tcp_addr, "127.0.0.1:5209".parse().unwrap());
        assert_eq!(
            resolved.server.listen.tls,
            Some("127.0.0.1:5210".parse().unwrap())
        );
        assert_eq!(
            resolved.server.listen.quic,
            Some("127.0.0.1:5210".parse().unwrap())
        );
        assert_eq!(
            resolved.server.listen.websocket,
            Some("127.0.0.1:5211".parse().unwrap())
        );
        assert_eq!(
            resolved.server.listen.websocket_tls,
            Some("127.0.0.1:5212".parse().unwrap())
        );
        let tls = resolved.server.tls.as_ref().unwrap();
        assert_eq!(tls.cert_chain_pem, TEST_CERT_PEM);
        assert_eq!(tls.private_key_pem, TEST_KEY_PEM);
        assert_eq!(resolved.server.max_message_size, 4096);
        assert_eq!(resolved.server.request_queue_capacity, 9);
        assert_eq!(
            resolved.server.response_write_timeout,
            Duration::from_millis(750)
        );
        assert_eq!(resolved.server.default_database_config.page_size, 4096);
        assert_eq!(
            resolved.server.default_database_config.memory_budget,
            123456
        );
        assert_eq!(
            resolved.server.default_database_config.max_disk_usage_bytes,
            Some(987654)
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .wal_retention_max_size,
            Some(12345)
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .wal_retention_max_age,
            Some(Duration::from_secs(6))
        );
        assert_eq!(resolved.server.default_database_config.max_doc_size, 2048);
        assert_eq!(
            resolved.server.default_database_config.checkpoint_interval,
            Duration::from_secs(2)
        );
        assert_eq!(
            resolved.server.default_database_config.close_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .idle_timeout,
            Duration::from_secs(3)
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .max_lifetime,
            Duration::from_secs(4 * 60)
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .max_intervals,
            55
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .max_operations,
            34
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .max_scanned_bytes,
            66
        );
        assert_eq!(
            resolved
                .server
                .default_database_config
                .transaction
                .max_scanned_docs,
            12
        );
        assert!(resolved.replication.is_empty());
        assert!(resolved.server.auth.enabled);
        assert_eq!(resolved.server.auth.issuer.as_deref(), Some("issuer"));
    }

    #[test]
    fn json_config_builds_replication_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.json");
        fs::write(
            &config_path,
            r#"{
                "listen": { "tcp": "127.0.0.1:5209" },
                "replication": {
                    "database": "app",
                    "self_id": 1,
                    "role": "primary",
                    "generation": 7,
                    "snapshot_chunk_bytes": 2048,
                    "topology": {
                        "1": "127.0.0.1:5301",
                        "2": "127.0.0.1:5302"
                    }
                }
            }"#,
        )
        .unwrap();

        let opts = CliOptions::parse([
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let resolved = ResolvedConfig::load(&opts).unwrap();
        assert_eq!(resolved.replication.len(), 1);
        let replication = &resolved.replication[0];

        assert_eq!(replication.database, "app");
        assert_eq!(replication.self_id, 1);
        assert_eq!(replication.role, NodeRole::Primary);
        assert_eq!(replication.generation, 7);
        assert_eq!(replication.snapshot_chunk_bytes, 2048);
        assert_eq!(replication.topology.len(), 2);
        assert_eq!(
            replication.topology.get(&2).copied(),
            Some("127.0.0.1:5302".parse().unwrap())
        );
    }

    #[test]
    fn json_config_builds_multiple_replication_configs() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.json");
        fs::write(
            &config_path,
            r#"{
                "listen": { "tcp": "127.0.0.1:5209" },
                "replication": [
                    {
                        "database": "app_a",
                        "self_id": 1,
                        "role": "replica",
                        "generation": 7,
                        "topology": {
                            "1": "127.0.0.1:5301"
                        }
                    },
                    {
                        "database": "app_b",
                        "self_id": 2,
                        "role": "replica",
                        "generation": 8,
                        "snapshot_chunk_bytes": 4096,
                        "topology": {
                            "2": "127.0.0.1:5302"
                        }
                    }
                ]
            }"#,
        )
        .unwrap();

        let opts = CliOptions::parse([
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let resolved = ResolvedConfig::load(&opts).unwrap();

        assert_eq!(resolved.replication.len(), 2);
        assert_eq!(resolved.replication[0].database, "app_a");
        assert_eq!(resolved.replication[0].self_id, 1);
        assert_eq!(resolved.replication[0].role, NodeRole::Replica);
        assert_eq!(resolved.replication[0].generation, 7);
        assert_eq!(
            resolved.replication[0].snapshot_chunk_bytes,
            DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES
        );
        assert_eq!(resolved.replication[1].database, "app_b");
        assert_eq!(resolved.replication[1].self_id, 2);
        assert_eq!(resolved.replication[1].role, NodeRole::Replica);
        assert_eq!(resolved.replication[1].generation, 8);
        assert_eq!(resolved.replication[1].snapshot_chunk_bytes, 4096);
    }

    #[test]
    fn replication_config_requires_self_in_topology() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("server.json");
        fs::write(
            &config_path,
            r#"{
                "replication": {
                    "self_id": 3,
                    "role": "replica",
                    "topology": {
                        "1": "127.0.0.1:5301",
                        "2": "127.0.0.1:5302"
                    }
                }
            }"#,
        )
        .unwrap();

        let opts = CliOptions::parse([
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let err = ResolvedConfig::load(&opts).unwrap_err();
        assert!(err.to_string().contains("topology must include self_id 3"));
    }

    #[tokio::test]
    async fn database_routing_transaction_promoter_routes_by_database() {
        let app_a = Arc::new(RecordingTransactionPromoter::new("app_a"));
        let app_b = Arc::new(RecordingTransactionPromoter::new("app_b"));
        let app_a_promoter: Arc<dyn TransactionPromoter> = app_a.clone();
        let app_b_promoter: Arc<dyn TransactionPromoter> = app_b.clone();
        let default_promoter = Arc::clone(&app_a_promoter);
        let mut by_database = HashMap::new();
        by_database.insert("app_a".to_string(), app_a_promoter);
        by_database.insert("app_b".to_string(), app_b_promoter);
        let router = DatabaseRoutingTransactionPromoter::new(by_database, default_promoter);

        let outcome = router
            .promote_transaction(
                "app_b",
                10,
                SubscriptionMode::Subscribe,
                b"opaque commit".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            TransactionPromotionOutcome::Success { commit_ts: 42 }
        );
        assert!(app_a.transactions().is_empty());
        assert_eq!(
            app_b.transactions(),
            vec![RecordedPromotion {
                database: "app_b".to_string(),
                begin_ts: 10,
                subscription: SubscriptionMode::Subscribe,
                payload: b"opaque commit".to_vec(),
            }]
        );

        let err = router
            .promote_transaction("missing", 11, SubscriptionMode::None, Vec::new())
            .await
            .unwrap_err();
        assert!(err.contains("database is not configured for replication: missing"));

        let response = router
            .promote_ddl(DdlPromotionRequest::CreateCollection {
                database: "app_b".to_string(),
                name: "items".to_string(),
            })
            .await
            .unwrap();
        match response {
            ServerMessage::Ok { fields } => {
                assert_eq!(fields["promoter"], "app_b");
            }
            other => panic!("unexpected DDL promotion response: {other:?}"),
        }
        assert!(app_a.ddl().is_empty());
        assert_eq!(
            app_b.ddl(),
            vec![DdlPromotionRequest::CreateCollection {
                database: "app_b".to_string(),
                name: "items".to_string(),
            }]
        );
    }

    #[test]
    fn database_routing_replica_read_gate_tracks_mesh_quorum() {
        let topology = HashMap::from([
            (1, SocketAddr::from(([127, 0, 0, 1], 5201))),
            (2, SocketAddr::from(([127, 0, 0, 1], 5202))),
            (3, SocketAddr::from(([127, 0, 0, 1], 5203))),
        ]);
        let cluster = Arc::new(ClusterMembership::new(ClusterConfig::new(2, topology)).unwrap());
        let mesh = Arc::new(PeerMesh::new(
            Arc::clone(&cluster),
            NodeRole::Replica,
            1,
            0,
            0,
            Arc::new(NoopWalApplier),
        ));
        let gate = DatabaseRoutingReplicaReadGate::new(HashMap::from([(
            "app".to_string(),
            Arc::clone(&mesh),
        )]));

        assert!(!gate.has_read_quorum("app"));
        assert!(!gate.has_read_quorum("missing"));

        mesh.cluster().record_heartbeat(1, 0, 0);

        assert!(gate.has_read_quorum("app"));
        assert!(!gate.has_read_quorum("missing"));
    }

    #[tokio::test]
    async fn configured_replications_reject_duplicate_database_names_before_start() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let configs = vec![
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 1,
                topology: HashMap::new(),
                snapshot_chunk_bytes: DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES,
            },
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Primary,
                generation: 1,
                topology: HashMap::new(),
                snapshot_chunk_bytes: DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES,
            },
        ];

        let err = match start_configured_replications(
            registry,
            tmp.path(),
            DatabaseConfig::default(),
            configs,
        )
        .await
        {
            Ok(_) => panic!("duplicate replication databases should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("replication database configured more than once: app")
        );
    }

    #[tokio::test]
    async fn configured_replications_reject_mixed_roles_before_start() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let configs = vec![
            ReplicationConfig {
                database: "app_primary".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 1,
                topology: HashMap::new(),
                snapshot_chunk_bytes: DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES,
            },
            ReplicationConfig {
                database: "app_replica".to_string(),
                self_id: 1,
                role: NodeRole::Replica,
                generation: 1,
                topology: HashMap::new(),
                snapshot_chunk_bytes: DEFAULT_REPLICATION_SNAPSHOT_CHUNK_BYTES,
            },
        ];

        let err = match start_configured_replications(
            Arc::clone(&registry),
            tmp.path(),
            DatabaseConfig::default(),
            configs,
        )
        .await
        {
            Ok(_) => panic!("mixed replication roles should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("all configured replication databases must use the same role")
        );
        assert!(
            registry.list_databases().is_empty(),
            "mixed-role config should fail before creating managed databases"
        );
    }

    #[test]
    fn invalid_algorithm_is_rejected() {
        let err = parse_jwt_algorithm("NONE").unwrap_err();
        assert!(err.to_string().contains("unsupported jwt_algorithm"));
    }

    #[test]
    fn json_config_rejects_invalid_hmac_jwt_secret_material() {
        let cases = [
            (
                r#"{ "auth": { "enabled": true, "jwt_algorithm": "HS256" } }"#,
                "auth.jwt_secret is required",
            ),
            (
                r#"{ "auth": { "enabled": true, "jwt_algorithm": "HS256", "jwt_secret": "not base64!" } }"#,
                "invalid base64 JWT secret",
            ),
            (
                r#"{ "auth": { "enabled": true, "jwt_algorithm": "HS256", "jwt_secret": "" } }"#,
                "auth.jwt_secret must not be empty",
            ),
        ];

        for (body, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let config_path = dir.path().join("server.json");
            fs::write(&config_path, body).unwrap();

            let opts = CliOptions::parse([
                "--config".to_string(),
                config_path.to_string_lossy().to_string(),
            ])
            .unwrap();
            let err = ResolvedConfig::load(&opts).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected '{expected}' in '{err}'"
            );
        }
    }

    #[test]
    fn json_config_rejects_invalid_jwt_public_key_material() {
        let cases = [
            ("RS256", "invalid RSA JWT public key"),
            ("ES256", "invalid EC JWT public key"),
        ];

        for (algorithm, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let config_path = dir.path().join("server.json");
            let key_path = dir.path().join("jwt-public.pem");
            fs::write(&key_path, "not a public key").unwrap();
            fs::write(
                &config_path,
                format!(
                    r#"{{
                    "auth": {{
                        "enabled": true,
                        "jwt_algorithm": "{algorithm}",
                        "jwt_public_key_file": "{}"
                    }}
                }}"#,
                    key_path.display()
                ),
            )
            .unwrap();

            let opts = CliOptions::parse([
                "--config".to_string(),
                config_path.to_string_lossy().to_string(),
            ])
            .unwrap();
            let err = ResolvedConfig::load(&opts).unwrap_err();
            assert!(
                err.to_string().contains(expected),
                "expected '{expected}' in '{err}'"
            );
        }
    }

    #[tokio::test]
    async fn configured_primary_replication_starts_managed_database_with_hook() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let (_listener, addr) = bound_listener();
        drop(_listener);
        let runtime = start_configured_replication(
            Arc::clone(&registry),
            tmp.path(),
            DatabaseConfig::default(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 3,
                topology: HashMap::from([(1, addr)]),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(runtime.node_role, NodeRole::Primary);
        assert_eq!(runtime.startup_generation, 3);
        assert!(runtime.startup_recovery_tier.is_none());
        assert_eq!(runtime._mesh.generation(), 3);
        assert!(runtime.transaction_promoter.is_none());

        let db = registry.get_database_by_name("app").unwrap();
        assert_eq!(db.generation().await, 3);
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        runtime.shutdown().await;
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_existing_primary_uses_durable_generation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let (_listener, addr) = bound_listener();
        drop(_listener);

        {
            let db = Database::open(tmp.path().join("app"), DatabaseConfig::default(), None)
                .await
                .unwrap();
            db.set_generation(42).await.unwrap();
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());
            db.close().await.unwrap();
        }

        let runtime = start_configured_replication(
            Arc::clone(&registry),
            tmp.path(),
            DatabaseConfig::default(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 99,
                topology: HashMap::from([(1, addr)]),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(runtime.node_role, NodeRole::Primary);
        assert_eq!(runtime.startup_generation, 42);
        assert_eq!(runtime._mesh.generation(), 42);

        let db = registry.get_database_by_name("app").unwrap();
        assert_eq!(db.generation().await, 42);
        assert!(db.list_collections().iter().any(|c| c.name == "users"));

        runtime.shutdown().await;
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_existing_replica_uses_durable_generation_and_applied_lsn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let (_listener, addr) = bound_listener();
        drop(_listener);
        {
            let db = Database::open(tmp.path().join("app"), DatabaseConfig::default(), None)
                .await
                .unwrap();
            db.set_generation(42).await.unwrap();
            db.storage()
                .update_file_header(|fh| {
                    fh.replication_applied_lsn.set(777);
                })
                .await
                .unwrap();
            db.storage().sync_file_header().await.unwrap();
            db.close().await.unwrap();
        }

        let runtime = start_configured_replication(
            Arc::clone(&registry),
            tmp.path(),
            DatabaseConfig::default(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 99,
                topology: HashMap::from([(2, addr)]),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(runtime.node_role, NodeRole::Replica);
        assert_eq!(runtime.startup_generation, 42);
        assert_eq!(
            runtime.startup_recovery_tier,
            Some(RecoveryTier::IncrementalCatchup { from_lsn: 777 })
        );
        assert_eq!(runtime._mesh.generation(), 42);
        assert_eq!(runtime._mesh.applied_lsn(), 777);

        let db = registry.get_database_by_name("app").unwrap();
        assert_eq!(db.generation().await, 42);
        assert_eq!(db.replication_applied_lsn().await, 777);

        runtime.shutdown().await;
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_existing_replica_reports_local_recovery_then_catchup_after_crash() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let (_listener, addr) = bound_listener();
        drop(_listener);

        let db = Database::open(tmp.path().join("app"), DatabaseConfig::default(), None)
            .await
            .unwrap();
        db.set_generation(42).await.unwrap();
        db.storage()
            .update_file_header(|fh| {
                fh.replication_applied_lsn.set(777);
            })
            .await
            .unwrap();
        db.storage().sync_file_header().await.unwrap();

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;

        let runtime = start_configured_replication(
            Arc::clone(&registry),
            tmp.path(),
            DatabaseConfig::default(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 99,
                topology: HashMap::from([(2, addr)]),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(runtime.node_role, NodeRole::Replica);
        assert_eq!(runtime.startup_generation, 42);
        assert_eq!(
            runtime.startup_recovery_tier,
            Some(RecoveryTier::LocalRecoveryThenCatchup)
        );
        assert_eq!(runtime._mesh.generation(), 42);
        assert_eq!(runtime._mesh.applied_lsn(), 777);

        let db = registry.get_database_by_name("app").unwrap();
        assert!(
            db.list_collections()
                .iter()
                .any(|collection| collection.name == "users"),
            "local WAL recovery should replay the uncheckpointed collection create"
        );
        assert_eq!(db.replication_applied_lsn().await, 777);

        runtime.shutdown().await;
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_replica_quarantines_unusable_local_database_for_snapshot_recovery() {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(SystemDatabase::open(tmp.path()).await.unwrap());
        let (_listener, addr) = bound_listener();
        drop(_listener);

        let corrupt_path = tmp.path().join("app");
        {
            let db = Database::open(&corrupt_path, DatabaseConfig::default(), None)
                .await
                .unwrap();
            db.close().await.unwrap();
        }
        let mut data_file = std::fs::OpenOptions::new()
            .append(true)
            .open(corrupt_path.join("data.db"))
            .unwrap();
        use std::io::Write as _;
        data_file.write_all(&[0xA5]).unwrap();

        let database_config = DatabaseConfig {
            check_on_startup: true,
            ..Default::default()
        };
        let runtime = start_configured_replication(
            Arc::clone(&registry),
            tmp.path(),
            database_config,
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 99,
                topology: HashMap::from([(2, addr)]),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(runtime.node_role, NodeRole::Replica);
        assert_eq!(runtime.startup_generation, 99);
        assert_eq!(
            runtime.startup_recovery_tier,
            Some(RecoveryTier::FullReconstruction)
        );
        let quarantined = runtime
            .startup_quarantined_path
            .as_ref()
            .expect("corrupt replica database should be quarantined");
        assert!(quarantined.join("data.db").exists());
        assert!(!corrupt_path.exists());
        assert!(registry.get_database_by_name("app").is_none());

        runtime.shutdown().await;
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_three_node_replication_commits_with_one_replica_offline() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        let (listener3, addr3) = bound_listener();
        drop(listener1);
        drop(listener2);
        drop(listener3);
        let topology = HashMap::from([(1, addr1), (2, addr2), (3, addr3)]);
        let database_config = DatabaseConfig::default();

        {
            let primary = Database::open(primary_root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            primary.set_generation(3).await.unwrap();
            let mut tx = primary.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());
            primary.close().await.unwrap();
        }
        {
            let replica = Database::open(replica_root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            replica.set_generation(3).await.unwrap();
            let mut tx = replica.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());
            replica.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtime = start_configured_replication(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 3,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let primary_db = primary_registry.get_database_by_name("app").unwrap();

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtime = start_configured_replication(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 3,
                topology,
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let replica_db = replica_registry.get_database_by_name("app").unwrap();

        eventually(|| {
            primary_runtime
                ._mesh
                .connection(2)
                .is_some_and(|connection| connection.is_connected())
                && primary_runtime._mesh.cluster().has_quorum()
        })
        .await;
        assert!(
            primary_runtime._mesh.connection(3).is_none(),
            "node 3 is intentionally offline for this majority-quorum test"
        );

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", serde_json::json!({"name": "Majority Ada"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated = false;
        for _ in 0..100 {
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            replicated = tx
                .get("users", &doc_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Majority Ada"))
                .unwrap_or(false);
            tx.rollback();
            if replicated {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            replicated,
            "primary should commit with one live replica in a three-node topology"
        );

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_db);
        drop(replica_db);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_three_node_replication_applies_to_both_online_replicas() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica2_root = tmp.path().join("replica2");
        let replica3_root = tmp.path().join("replica3");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        let (listener3, addr3) = bound_listener();
        drop(listener1);
        drop(listener2);
        drop(listener3);
        let topology = HashMap::from([(1, addr1), (2, addr2), (3, addr3)]);
        let database_config = DatabaseConfig::default();

        for root in [&primary_root, &replica2_root, &replica3_root] {
            let db = Database::open(root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            db.set_generation(4).await.unwrap();
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());
            db.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtime = start_configured_replication(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 4,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let primary_db = primary_registry.get_database_by_name("app").unwrap();

        let replica2_registry = Arc::new(SystemDatabase::open(&replica2_root).await.unwrap());
        let replica2_runtime = start_configured_replication(
            Arc::clone(&replica2_registry),
            &replica2_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 4,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let replica2_db = replica2_registry.get_database_by_name("app").unwrap();

        let replica3_registry = Arc::new(SystemDatabase::open(&replica3_root).await.unwrap());
        let replica3_runtime = start_configured_replication(
            Arc::clone(&replica3_registry),
            &replica3_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 3,
                role: NodeRole::Replica,
                generation: 4,
                topology,
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let replica3_db = replica3_registry.get_database_by_name("app").unwrap();

        eventually(|| {
            [2, 3].into_iter().all(|peer| {
                primary_runtime
                    ._mesh
                    .connection(peer)
                    .is_some_and(|connection| connection.is_connected())
            }) && primary_runtime._mesh.cluster().has_quorum()
        })
        .await;

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", serde_json::json!({"name": "Fully Replicated Ada"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        for (node_id, replica_db) in [(2, &replica2_db), (3, &replica3_db)] {
            let mut replicated = false;
            for _ in 0..100 {
                let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
                replicated = tx
                    .get("users", &doc_id)
                    .await
                    .map(|doc| doc.is_some_and(|doc| doc["name"] == "Fully Replicated Ada"))
                    .unwrap_or(false);
                tx.rollback();
                if replicated {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
            assert!(
                replicated,
                "replica node {node_id} should apply primary WAL"
            );

            let applied_lsn = replica_db.replication_applied_lsn().await;
            assert!(
                applied_lsn > 0,
                "replica node {node_id} should persist source WAL progress"
            );
            assert_eq!(
                primary_runtime
                    ._mesh
                    .cluster()
                    .status(node_id)
                    .unwrap()
                    .applied_lsn,
                applied_lsn,
                "primary should track durable progress for replica node {node_id}"
            );
        }

        primary_runtime.shutdown().await;
        replica2_runtime.shutdown().await;
        replica3_runtime.shutdown().await;
        drop(primary_db);
        drop(replica2_db);
        drop(replica3_db);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica2_registry = Arc::try_unwrap(replica2_registry).ok().unwrap();
        replica2_registry.close().await.unwrap();
        let replica3_registry = Arc::try_unwrap(replica3_registry).ok().unwrap();
        replica3_registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_multi_database_replication_routes_each_database_independently() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener_a_primary, addr_a_primary) = bound_listener();
        let (listener_a_replica, addr_a_replica) = bound_listener();
        let (listener_b_primary, addr_b_primary) = bound_listener();
        let (listener_b_replica, addr_b_replica) = bound_listener();
        drop(listener_a_primary);
        drop(listener_a_replica);
        drop(listener_b_primary);
        drop(listener_b_replica);

        let database_config = DatabaseConfig::default();
        for root in [&primary_root, &replica_root] {
            for (database, collection) in [("app_a", "users"), ("app_b", "orders")] {
                let db = Database::open(root.join(database), database_config.clone(), None)
                    .await
                    .unwrap();
                db.set_generation(7).await.unwrap();
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.create_collection(collection).await.unwrap();
                assert_success(tx.commit().await.unwrap());
                db.close().await.unwrap();
            }
        }

        let topology_a = HashMap::from([(1, addr_a_primary), (2, addr_a_replica)]);
        let topology_b = HashMap::from([(1, addr_b_primary), (2, addr_b_replica)]);
        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtimes = start_configured_replications(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 7,
                    topology: topology_a.clone(),
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 7,
                    topology: topology_b.clone(),
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();
        let primary_a = primary_registry.get_database_by_name("app_a").unwrap();
        let primary_b = primary_registry.get_database_by_name("app_b").unwrap();

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtimes = start_configured_replications(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 7,
                    topology: topology_a,
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 7,
                    topology: topology_b,
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();
        let replica_a = replica_registry.get_database_by_name("app_a").unwrap();
        let replica_b = replica_registry.get_database_by_name("app_b").unwrap();

        eventually(|| {
            primary_runtimes.iter().all(|runtime| {
                runtime
                    ._mesh
                    .connection(2)
                    .is_some_and(|connection| connection.is_connected())
                    && runtime._mesh.cluster().has_quorum()
            })
        })
        .await;

        let mut tx = primary_a.begin(TransactionOptions::default()).unwrap();
        let user_id = tx
            .insert("users", serde_json::json!({"name": "Ada", "tenant": "a"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_b.begin(TransactionOptions::default()).unwrap();
        let order_id = tx
            .insert(
                "orders",
                serde_json::json!({"number": "B-42", "tenant": "b"}),
            )
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated_a = false;
        let mut replicated_b = false;
        for _ in 0..100 {
            let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
            replicated_a = tx
                .get("users", &user_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Ada"))
                .unwrap_or(false);
            tx.rollback();

            let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
            replicated_b = tx
                .get("orders", &order_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["number"] == "B-42"))
                .unwrap_or(false);
            tx.rollback();

            if replicated_a && replicated_b {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(replicated_a, "app_a WAL should replicate to app_a replica");
        assert!(replicated_b, "app_b WAL should replicate to app_b replica");

        let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "orders"),
            "app_a replica should not receive app_b catalog state"
        );
        tx.rollback();
        let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "users"),
            "app_b replica should not receive app_a catalog state"
        );
        tx.rollback();

        for runtime in primary_runtimes {
            runtime.shutdown().await;
        }
        for runtime in replica_runtimes {
            runtime.shutdown().await;
        }
        drop(primary_a);
        drop(primary_b);
        drop(replica_a);
        drop(replica_b);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_fresh_replica_registers_multiple_snapshots_independently() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener_a_primary, addr_a_primary) = bound_listener();
        let (listener_a_replica, addr_a_replica) = bound_listener();
        let (listener_b_primary, addr_b_primary) = bound_listener();
        let (listener_b_replica, addr_b_replica) = bound_listener();
        drop(listener_a_primary);
        drop(listener_a_replica);
        drop(listener_b_primary);
        drop(listener_b_replica);

        let topology_a = HashMap::from([(1, addr_a_primary), (2, addr_a_replica)]);
        let topology_b = HashMap::from([(1, addr_b_primary), (2, addr_b_replica)]);
        let database_config = DatabaseConfig {
            wal_segment_size: 256,
            wal_retention_max_size: Some(512),
            ..Default::default()
        };

        let mut setup_ids = HashMap::new();
        for (database, collection, doc) in [
            (
                "app_a",
                "users",
                serde_json::json!({"name": "Ada", "tenant": "a"}),
            ),
            (
                "app_b",
                "orders",
                serde_json::json!({"number": "B-42", "tenant": "b"}),
            ),
        ] {
            let setup = Database::open(primary_root.join(database), database_config.clone(), None)
                .await
                .unwrap();
            setup.set_generation(9).await.unwrap();
            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(collection).await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            let doc_id = tx.insert(collection, doc).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            setup_ids.insert(database, doc_id);

            for i in 0..20 {
                let mut tx = setup.begin(TransactionOptions::default()).unwrap();
                tx.insert(
                    collection,
                    serde_json::json!({
                        "filler": i,
                        "payload": "x".repeat(512)
                    }),
                )
                .await
                .unwrap();
                assert_success(tx.commit().await.unwrap());
            }
            setup.storage().checkpoint().await.unwrap();
            assert!(
                setup.storage().oldest_retained_wal_lsn().unwrap_or(0) > 0,
                "{database} setup must force retained-WAL catch-up gap"
            );
            setup.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtimes = start_configured_replications(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 9,
                    topology: topology_a.clone(),
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 9,
                    topology: topology_b.clone(),
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();
        let primary_a = primary_registry.get_database_by_name("app_a").unwrap();
        let primary_b = primary_registry.get_database_by_name("app_b").unwrap();

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtimes = start_configured_replications(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 1,
                    topology: topology_a,
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 1,
                    topology: topology_b,
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();

        eventually(|| {
            replica_registry.get_database_by_name("app_a").is_some()
                && replica_registry.get_database_by_name("app_b").is_some()
        })
        .await;
        let replica_a = replica_registry.get_database_by_name("app_a").unwrap();
        let replica_b = replica_registry.get_database_by_name("app_b").unwrap();

        let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", setup_ids.get("app_a").unwrap())
                .await
                .unwrap()
                .unwrap()["name"],
            "Ada"
        );
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "orders"),
            "app_a snapshot must not import app_b catalog state"
        );
        tx.rollback();

        let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("orders", setup_ids.get("app_b").unwrap())
                .await
                .unwrap()
                .unwrap()["number"],
            "B-42"
        );
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "users"),
            "app_b snapshot must not import app_a catalog state"
        );
        tx.rollback();

        let mut tx = primary_a.begin(TransactionOptions::default()).unwrap();
        let grace_id = tx
            .insert("users", serde_json::json!({"name": "Grace", "tenant": "a"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_b.begin(TransactionOptions::default()).unwrap();
        let invoice_id = tx
            .insert(
                "orders",
                serde_json::json!({"number": "B-43", "tenant": "b"}),
            )
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated_a = false;
        let mut replicated_b = false;
        for _ in 0..100 {
            let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
            replicated_a = tx
                .get("users", &grace_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Grace"))
                .unwrap_or(false);
            tx.rollback();

            let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
            replicated_b = tx
                .get("orders", &invoice_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["number"] == "B-43"))
                .unwrap_or(false);
            tx.rollback();

            if replicated_a && replicated_b {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            replicated_a,
            "app_a should keep applying live WAL after snapshot registration"
        );
        assert!(
            replicated_b,
            "app_b should keep applying live WAL after snapshot registration"
        );

        for runtime in primary_runtimes {
            runtime.shutdown().await;
        }
        for runtime in replica_runtimes {
            runtime.shutdown().await;
        }
        drop(primary_a);
        drop(primary_b);
        drop(replica_a);
        drop(replica_b);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();

        let reopened_replica = SystemDatabase::open(&replica_root).await.unwrap();
        let replica_a = reopened_replica
            .get_database_by_name("app_a")
            .expect("app_a snapshot registry entry should survive restart");
        let replica_b = reopened_replica
            .get_database_by_name("app_b")
            .expect("app_b snapshot registry entry should survive restart");

        let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", setup_ids.get("app_a").unwrap())
                .await
                .unwrap()
                .unwrap()["name"],
            "Ada"
        );
        assert_eq!(
            tx.get("users", &grace_id).await.unwrap().unwrap()["name"],
            "Grace"
        );
        tx.rollback();

        let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("orders", setup_ids.get("app_b").unwrap())
                .await
                .unwrap()
                .unwrap()["number"],
            "B-42"
        );
        assert_eq!(
            tx.get("orders", &invoice_id).await.unwrap().unwrap()["number"],
            "B-43"
        );
        tx.rollback();
        drop(replica_a);
        drop(replica_b);
        reopened_replica.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_existing_replica_replaces_multiple_snapshots_independently() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener_a_primary, addr_a_primary) = bound_listener();
        let (listener_a_replica, addr_a_replica) = bound_listener();
        let (listener_b_primary, addr_b_primary) = bound_listener();
        let (listener_b_replica, addr_b_replica) = bound_listener();
        drop(listener_a_primary);
        drop(listener_a_replica);
        drop(listener_b_primary);
        drop(listener_b_replica);

        let topology_a = HashMap::from([(1, addr_a_primary), (2, addr_a_replica)]);
        let topology_b = HashMap::from([(1, addr_b_primary), (2, addr_b_replica)]);
        let database_config = DatabaseConfig {
            wal_segment_size: 256,
            wal_retention_max_size: Some(512),
            ..Default::default()
        };

        let mut setup_ids = HashMap::new();
        for (database, collection, doc) in [
            (
                "app_a",
                "users",
                serde_json::json!({"name": "Ada", "tenant": "a"}),
            ),
            (
                "app_b",
                "orders",
                serde_json::json!({"number": "B-42", "tenant": "b"}),
            ),
        ] {
            let setup = Database::open(primary_root.join(database), database_config.clone(), None)
                .await
                .unwrap();
            setup.set_generation(13).await.unwrap();
            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(collection).await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            let doc_id = tx.insert(collection, doc).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            setup_ids.insert(database, doc_id);

            for i in 0..20 {
                let mut tx = setup.begin(TransactionOptions::default()).unwrap();
                tx.insert(
                    collection,
                    serde_json::json!({
                        "filler": i,
                        "payload": "x".repeat(512)
                    }),
                )
                .await
                .unwrap();
                assert_success(tx.commit().await.unwrap());
            }
            setup.storage().checkpoint().await.unwrap();
            assert!(
                setup.storage().oldest_retained_wal_lsn().unwrap_or(0) > 0,
                "{database} setup must force retained-WAL catch-up gap"
            );
            setup.close().await.unwrap();
        }

        let mut stale_ids = HashMap::new();
        for (database, collection) in [("app_a", "users"), ("app_b", "orders")] {
            let stale = Database::open(replica_root.join(database), database_config.clone(), None)
                .await
                .unwrap();
            stale.set_generation(13).await.unwrap();
            let mut tx = stale.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(collection).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            let mut tx = stale.begin(TransactionOptions::default()).unwrap();
            let stale_id = tx
                .insert(collection, serde_json::json!({"stale": database}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());
            stale_ids.insert(database, stale_id);
            stale.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtimes = start_configured_replications(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 13,
                    topology: topology_a.clone(),
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 1,
                    role: NodeRole::Primary,
                    generation: 13,
                    topology: topology_b.clone(),
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();
        let primary_a = primary_registry.get_database_by_name("app_a").unwrap();
        let primary_b = primary_registry.get_database_by_name("app_b").unwrap();
        for (runtime, database) in primary_runtimes.iter().zip([&primary_a, &primary_b]) {
            for _ in 0..8 {
                database
                    .storage()
                    .append_wal(0x01, &vec![0xAB; 512])
                    .await
                    .unwrap();
            }
            database.storage().checkpoint().await.unwrap();
            assert!(
                runtime._mesh.oldest_retained_lsn() > 0,
                "{} primary handshake should advertise a retained-WAL floor",
                runtime.database_name
            );
        }

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtimes = start_configured_replications(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            vec![
                ReplicationConfig {
                    database: "app_a".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 1,
                    topology: topology_a,
                    snapshot_chunk_bytes: 1024,
                },
                ReplicationConfig {
                    database: "app_b".to_string(),
                    self_id: 2,
                    role: NodeRole::Replica,
                    generation: 1,
                    topology: topology_b,
                    snapshot_chunk_bytes: 1024,
                },
            ],
        )
        .await
        .unwrap();
        let mut both_replica_meshes_attached = false;
        for _ in 0..300 {
            both_replica_meshes_attached = replica_runtimes.iter().all(|runtime| {
                runtime
                    ._mesh
                    .connection(1)
                    .is_some_and(|connection| connection.peer_oldest_retained_lsn() > 0)
            });
            if both_replica_meshes_attached {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            both_replica_meshes_attached,
            "both replica meshes should attach to their primary with retained-WAL floors: {:?}",
            replica_runtimes
                .iter()
                .map(|runtime| {
                    (
                        runtime.database_name.as_str(),
                        runtime._mesh.connection(1).map(|connection| {
                            (
                                connection.is_connected(),
                                connection.peer_oldest_retained_lsn(),
                            )
                        }),
                    )
                })
                .collect::<Vec<_>>()
        );

        let mut restored_a = false;
        let mut restored_b = false;
        for _ in 0..100 {
            if let Some(replica) = replica_registry.get_database_by_name("app_a") {
                let mut tx = replica.begin(TransactionOptions::readonly()).unwrap();
                restored_a = tx
                    .get("users", setup_ids.get("app_a").unwrap())
                    .await
                    .map(|doc| doc.is_some_and(|doc| doc["name"] == "Ada"))
                    .unwrap_or(false);
                tx.rollback();
            }
            if let Some(replica) = replica_registry.get_database_by_name("app_b") {
                let mut tx = replica.begin(TransactionOptions::readonly()).unwrap();
                restored_b = tx
                    .get("orders", setup_ids.get("app_b").unwrap())
                    .await
                    .map(|doc| doc.is_some_and(|doc| doc["number"] == "B-42"))
                    .unwrap_or(false);
                tx.rollback();
            }
            if restored_a && restored_b {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            restored_a,
            "app_a existing replica should restore primary snapshot"
        );
        assert!(
            restored_b,
            "app_b existing replica should restore primary snapshot"
        );

        let replica_a = replica_registry.get_database_by_name("app_a").unwrap();
        let replica_b = replica_registry.get_database_by_name("app_b").unwrap();

        let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", setup_ids.get("app_a").unwrap())
                .await
                .unwrap()
                .unwrap()["name"],
            "Ada"
        );
        assert!(
            tx.get("users", stale_ids.get("app_a").unwrap())
                .await
                .unwrap()
                .is_none(),
            "app_a replacement snapshot should remove stale local rows"
        );
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "orders"),
            "app_a replacement snapshot must not import app_b catalog state"
        );
        tx.rollback();

        let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("orders", setup_ids.get("app_b").unwrap())
                .await
                .unwrap()
                .unwrap()["number"],
            "B-42"
        );
        assert!(
            tx.get("orders", stale_ids.get("app_b").unwrap())
                .await
                .unwrap()
                .is_none(),
            "app_b replacement snapshot should remove stale local rows"
        );
        assert!(
            !tx.list_collections()
                .unwrap()
                .iter()
                .any(|collection| collection.name == "users"),
            "app_b replacement snapshot must not import app_a catalog state"
        );
        tx.rollback();

        let mut tx = primary_a.begin(TransactionOptions::default()).unwrap();
        let grace_id = tx
            .insert("users", serde_json::json!({"name": "Grace", "tenant": "a"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_b.begin(TransactionOptions::default()).unwrap();
        let invoice_id = tx
            .insert(
                "orders",
                serde_json::json!({"number": "B-43", "tenant": "b"}),
            )
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated_a = false;
        let mut replicated_b = false;
        for _ in 0..100 {
            let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
            replicated_a = tx
                .get("users", &grace_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Grace"))
                .unwrap_or(false);
            tx.rollback();

            let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
            replicated_b = tx
                .get("orders", &invoice_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["number"] == "B-43"))
                .unwrap_or(false);
            tx.rollback();

            if replicated_a && replicated_b {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            replicated_a,
            "app_a should keep applying live WAL after snapshot replacement"
        );
        assert!(
            replicated_b,
            "app_b should keep applying live WAL after snapshot replacement"
        );

        for runtime in primary_runtimes {
            runtime.shutdown().await;
        }
        for runtime in replica_runtimes {
            runtime.shutdown().await;
        }
        drop(primary_a);
        drop(primary_b);
        drop(replica_a);
        drop(replica_b);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();

        let reopened_replica = SystemDatabase::open(&replica_root).await.unwrap();
        let replica_a = reopened_replica
            .get_database_by_name("app_a")
            .expect("app_a replacement registry entry should survive restart");
        let replica_b = reopened_replica
            .get_database_by_name("app_b")
            .expect("app_b replacement registry entry should survive restart");

        let mut tx = replica_a.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", setup_ids.get("app_a").unwrap())
                .await
                .unwrap()
                .unwrap()["name"],
            "Ada"
        );
        assert_eq!(
            tx.get("users", &grace_id).await.unwrap().unwrap()["name"],
            "Grace"
        );
        tx.rollback();

        let mut tx = replica_b.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("orders", setup_ids.get("app_b").unwrap())
                .await
                .unwrap()
                .unwrap()["number"],
            "B-42"
        );
        assert_eq!(
            tx.get("orders", &invoice_id).await.unwrap().unwrap()["number"],
            "B-43"
        );
        tx.rollback();
        drop(replica_a);
        drop(replica_b);
        reopened_replica.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_replica_promotes_system_database_ddl_over_mesh() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        drop(listener1);
        drop(listener2);
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let database_config = DatabaseConfig::default();

        for root in [&primary_root, &replica_root] {
            let db = Database::open(root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            db.set_generation(11).await.unwrap();
            db.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtime = start_configured_replication(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 11,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtime = start_configured_replication(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config,
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 11,
                topology,
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let promoter = replica_runtime
            .transaction_promoter
            .as_ref()
            .expect("configured replica should install a transaction promoter");

        eventually(|| {
            replica_runtime
                ._mesh
                .connection(1)
                .is_some_and(|connection| connection.is_connected())
                && replica_runtime._mesh.cluster().has_quorum()
        })
        .await;

        let response = promoter
            .promote_ddl(DdlPromotionRequest::CreateDatabase {
                name: "tenant".to_string(),
                config: Some(json!({
                    "max_doc_size": 8192,
                    "transaction": {
                        "max_operations": 321
                    }
                })),
            })
            .await
            .unwrap();
        assert!(matches!(response, ServerMessage::Ok { .. }));

        let tenant = primary_registry
            .list_databases()
            .into_iter()
            .find(|database| database.name == "tenant")
            .expect("primary should create promoted tenant database");
        assert_eq!(tenant.config.max_doc_size, 8192);
        assert_eq!(tenant.config.transaction.max_operations, 321);
        assert!(replica_registry.get_database_by_name("tenant").is_none());

        let held_tenant = primary_registry
            .get_database_by_name("tenant")
            .expect("promoted tenant database should be open on primary");
        let response = promoter
            .promote_ddl(DdlPromotionRequest::DropDatabase {
                name: "tenant".to_string(),
            })
            .await
            .unwrap();
        match response {
            ServerMessage::Error { code, .. } => assert_eq!(code, "database_in_use"),
            other => panic!("expected database_in_use response, got {other:?}"),
        }
        assert!(
            primary_registry.get_database_by_name("tenant").is_some(),
            "failed promoted drop must leave tenant database registered"
        );
        drop(held_tenant);

        let response = promoter
            .promote_ddl(DdlPromotionRequest::DropDatabase {
                name: "tenant".to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(response, ServerMessage::Ok { .. }));
        assert!(primary_registry.get_database_by_name("tenant").is_none());

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_promotion_handler_commits_encoded_payload() {
        let (_tmp, registry, db) = promotion_handler_fixture().await;
        let mut setup = db.begin(TransactionOptions::default()).unwrap();
        setup.create_collection("users").await.unwrap();
        assert_success(setup.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", serde_json::json!({"name": "Ada"}))
            .await
            .unwrap();
        let (begin_ts, payload) = tx.into_promotion_payload().unwrap();

        let handler = DatabasePromotionHandler::new(
            Arc::clone(&db),
            Arc::clone(&registry),
            DatabaseConfig::default(),
        );
        let outcome = handler
            .handle_promotion(2, begin_ts, SubscriptionMode::None, &payload)
            .await
            .unwrap();
        assert!(matches!(outcome, PromotionOutcome::Success { .. }));

        let mut read = db.begin(TransactionOptions::readonly()).unwrap();
        let doc = read.get("users", &doc_id).await.unwrap().unwrap();
        read.rollback();
        assert_eq!(doc["name"], "Ada");

        drop(handler);
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_promotion_handler_routes_wrapped_payload_by_database_name() {
        let (_tmp, registry, default_db) = promotion_handler_fixture().await;
        registry
            .create_database("tenant_b", DatabaseConfig::default())
            .await
            .unwrap();
        let tenant_b = registry.get_database_by_name("tenant_b").unwrap();

        let mut setup = tenant_b.begin(TransactionOptions::default()).unwrap();
        setup.create_collection("users").await.unwrap();
        assert_success(setup.commit().await.unwrap());

        let mut tx = tenant_b.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", serde_json::json!({"name": "Tenant B Ada"}))
            .await
            .unwrap();
        let (begin_ts, payload) = tx.into_promotion_payload().unwrap();
        let payload = encode_transaction_promotion("tenant_b", &payload).unwrap();

        let handler = DatabasePromotionHandler::new(
            Arc::clone(&default_db),
            Arc::clone(&registry),
            DatabaseConfig::default(),
        );
        let outcome = handler
            .handle_promotion(2, begin_ts, SubscriptionMode::None, &payload)
            .await
            .unwrap();
        assert!(matches!(outcome, PromotionOutcome::Success { .. }));

        let mut read = tenant_b.begin(TransactionOptions::readonly()).unwrap();
        let doc = read.get("users", &doc_id).await.unwrap().unwrap();
        read.rollback();
        assert_eq!(doc["name"], "Tenant B Ada");

        let mut default_read = default_db.begin(TransactionOptions::readonly()).unwrap();
        assert!(default_read.list_collections().unwrap().is_empty());
        default_read.rollback();

        drop(handler);
        drop(tenant_b);
        drop(default_db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_promotion_handler_returns_subscribe_retry_metadata() {
        let (_tmp, registry, db) = promotion_handler_fixture().await;
        let mut setup = db.begin(TransactionOptions::default()).unwrap();
        setup.create_collection("users").await.unwrap();
        assert_success(setup.commit().await.unwrap());

        let mut seed = db.begin(TransactionOptions::default()).unwrap();
        let doc_id = seed
            .insert("users", serde_json::json!({"name": "Ada"}))
            .await
            .unwrap();
        assert_success(seed.commit().await.unwrap());

        let mut promoted = db.begin(TransactionOptions::default()).unwrap();
        let original = promoted.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(original["name"], "Ada");
        promoted
            .replace("users", &doc_id, serde_json::json!({"name": "Replica Ada"}))
            .await
            .unwrap();
        let (begin_ts, payload) = promoted.into_promotion_payload().unwrap();

        let mut writer = db.begin(TransactionOptions::default()).unwrap();
        writer
            .replace("users", &doc_id, serde_json::json!({"name": "Primary Ada"}))
            .await
            .unwrap();
        assert_success(writer.commit().await.unwrap());

        let handler = DatabasePromotionHandler::new(
            Arc::clone(&db),
            Arc::clone(&registry),
            DatabaseConfig::default(),
        );
        let outcome = handler
            .handle_promotion(2, begin_ts, SubscriptionMode::Subscribe, &payload)
            .await
            .unwrap();
        match outcome {
            PromotionOutcome::Conflict {
                retry: Some(retry), ..
            } => {
                assert!(retry.new_tx > 0);
                assert!(retry.new_ts > 0);
            }
            other => panic!("expected retryable promotion conflict, got {other:?}"),
        }

        drop(handler);
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_promotion_handler_applies_management_ddl_and_returns_response() {
        let (_tmp, registry, db) = promotion_handler_fixture().await;
        let handler = DatabasePromotionHandler::new(
            Arc::clone(&db),
            Arc::clone(&registry),
            DatabaseConfig::default(),
        );

        let create_collection = encode_ddl_promotion(&DdlPromotionRequest::CreateCollection {
            database: "default".to_string(),
            name: "users".to_string(),
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &create_collection)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                assert!(matches!(
                    decode_ddl_response(&payload).unwrap(),
                    ServerMessage::Ok { .. }
                ));
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }
        assert!(
            db.list_collections()
                .iter()
                .any(|meta| meta.name == "users")
        );

        let create_index = encode_ddl_promotion(&DdlPromotionRequest::CreateIndex {
            database: "default".to_string(),
            collection: "users".to_string(),
            fields: vec![vec!["email".to_string()]],
            name: None,
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &create_index)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                match decode_ddl_response(&payload).unwrap() {
                    ServerMessage::Ok { fields } => {
                        assert!(fields["index_id"].as_u64().unwrap() > 0);
                    }
                    other => panic!("expected ok DDL response, got {other:?}"),
                }
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }

        drop(handler);
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_promotion_handler_applies_system_database_ddl() {
        let (_tmp, registry, db) = promotion_handler_fixture().await;
        let default_config = DatabaseConfig {
            max_doc_size: 4096,
            memory_budget: 16 * 1024 * 1024,
            ..Default::default()
        };
        let handler =
            DatabasePromotionHandler::new(Arc::clone(&db), Arc::clone(&registry), default_config);

        let create_database = encode_ddl_promotion(&DdlPromotionRequest::CreateDatabase {
            name: "tenant".to_string(),
            config: Some(json!({
                "max_doc_size": 8192,
                "transaction": {
                    "max_operations": 123
                }
            })),
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &create_database)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                assert!(matches!(
                    decode_ddl_response(&payload).unwrap(),
                    ServerMessage::Ok { .. }
                ));
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }

        let tenant = registry
            .list_databases()
            .into_iter()
            .find(|database| database.name == "tenant")
            .unwrap();
        assert_eq!(tenant.config.max_doc_size, 8192);
        assert_eq!(tenant.config.memory_budget, 16 * 1024 * 1024);
        assert_eq!(tenant.config.transaction.max_operations, 123);

        let create_collection = encode_ddl_promotion(&DdlPromotionRequest::CreateCollection {
            database: "tenant".to_string(),
            name: "users".to_string(),
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &create_collection)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                assert!(matches!(
                    decode_ddl_response(&payload).unwrap(),
                    ServerMessage::Ok { .. }
                ));
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }

        let tenant_db = registry.get_database_by_name("tenant").unwrap();
        assert!(
            tenant_db
                .list_collections()
                .iter()
                .any(|collection| collection.name == "users"),
            "promoted collection DDL should apply to the newly created database"
        );

        let create_index = encode_ddl_promotion(&DdlPromotionRequest::CreateIndex {
            database: "tenant".to_string(),
            collection: "users".to_string(),
            fields: vec![vec!["email".to_string()]],
            name: Some("email_idx".to_string()),
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &create_index)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                match decode_ddl_response(&payload).unwrap() {
                    ServerMessage::Ok { fields } => {
                        assert!(fields["index_id"].as_u64().unwrap() > 0);
                    }
                    other => panic!("expected ok DDL response, got {other:?}"),
                }
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }

        let mut tx = tenant_db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        tx.rollback();
        assert!(indexes.iter().any(|index| index.name == "email_idx"));
        drop(tenant_db);

        let drop_database = encode_ddl_promotion(&DdlPromotionRequest::DropDatabase {
            name: "tenant".to_string(),
        })
        .unwrap();
        let response = handler
            .handle_promotion(2, 0, SubscriptionMode::None, &drop_database)
            .await
            .unwrap();
        match response {
            PromotionOutcome::Response { payload } => {
                assert!(matches!(
                    decode_ddl_response(&payload).unwrap(),
                    ServerMessage::Ok { .. }
                ));
            }
            other => panic!("expected DDL response outcome, got {other:?}"),
        }
        assert!(registry.get_database_by_name("tenant").is_none());

        drop(handler);
        drop(db);
        let registry = Arc::try_unwrap(registry).ok().unwrap();
        registry.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_fresh_replica_registers_snapshot_then_applies_live_wal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        drop(listener1);
        drop(listener2);
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let database_config = DatabaseConfig {
            wal_segment_size: 256,
            wal_retention_max_size: Some(512),
            ..Default::default()
        };

        let ada_id;
        {
            let setup = Database::open(primary_root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            ada_id = tx
                .insert("users", serde_json::json!({"name": "Ada", "age": 37}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());

            for i in 0..20 {
                let mut tx = setup.begin(TransactionOptions::default()).unwrap();
                tx.insert(
                    "users",
                    serde_json::json!({
                        "name": format!("filler-{i}"),
                        "payload": "x".repeat(512)
                    }),
                )
                .await
                .unwrap();
                assert_success(tx.commit().await.unwrap());
            }
            setup.storage().checkpoint().await.unwrap();
            assert!(
                setup.storage().oldest_retained_wal_lsn().unwrap_or(0) > 0,
                "test setup must force retained-WAL catch-up gap"
            );
            setup.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtime = start_configured_replication(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 3,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let primary_db = primary_registry.get_database_by_name("app").unwrap();
        for _ in 0..8 {
            primary_db
                .storage()
                .append_wal(0x01, &vec![0xAB; 512])
                .await
                .unwrap();
        }
        primary_db.storage().checkpoint().await.unwrap();
        assert!(
            primary_db.storage().oldest_retained_wal_lsn().unwrap_or(0) > 0,
            "configured primary should expose retained-WAL gap"
        );
        let advertised_retained_lsn = primary_runtime._mesh.oldest_retained_lsn();
        assert!(
            advertised_retained_lsn > 0,
            "primary handshake should advertise the retained-WAL floor"
        );

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtime = start_configured_replication(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 1,
                topology,
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();

        assert_eq!(replica_runtime.node_role, NodeRole::Replica);
        assert!(replica_runtime.transaction_promoter.is_some());
        eventually(|| replica_runtime._mesh.connection(1).is_some()).await;
        assert_eq!(
            replica_runtime
                ._mesh
                .connection(1)
                .unwrap()
                .peer_oldest_retained_lsn(),
            advertised_retained_lsn
        );

        eventually(|| replica_registry.get_database_by_name("app").is_some()).await;
        let replica_db = replica_registry.get_database_by_name("app").unwrap();
        let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &ada_id).await.unwrap().unwrap()["name"],
            "Ada"
        );
        tx.rollback();

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        let grace_id = tx
            .insert("users", serde_json::json!({"name": "Grace", "age": 42}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated = false;
        for _ in 0..100 {
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            replicated = tx
                .get("users", &grace_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Grace"))
                .unwrap_or(false);
            tx.rollback();
            if replicated {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            replicated,
            "fresh replica should apply live WAL after snapshot"
        );

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_db);
        drop(replica_db);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();

        let reopened_replica = SystemDatabase::open(&replica_root).await.unwrap();
        let replica_db = reopened_replica
            .get_database_by_name("app")
            .expect("snapshot-restored replica database should persist in registry");
        let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &ada_id).await.unwrap().unwrap()["name"],
            "Ada"
        );
        assert_eq!(
            tx.get("users", &grace_id).await.unwrap().unwrap()["name"],
            "Grace"
        );
        tx.rollback();
        drop(replica_db);
        reopened_replica.close().await.unwrap();
    }

    #[tokio::test]
    async fn configured_existing_replica_replaces_snapshot_then_applies_live_wal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_root = tmp.path().join("primary");
        let replica_root = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        drop(listener1);
        drop(listener2);
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let database_config = DatabaseConfig {
            wal_segment_size: 256,
            wal_retention_max_size: Some(512),
            ..Default::default()
        };

        let ada_id;
        {
            let setup = Database::open(primary_root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = setup.begin(TransactionOptions::default()).unwrap();
            ada_id = tx
                .insert("users", serde_json::json!({"name": "Ada", "age": 37}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());

            for i in 0..20 {
                let mut tx = setup.begin(TransactionOptions::default()).unwrap();
                tx.insert(
                    "users",
                    serde_json::json!({
                        "name": format!("filler-{i}"),
                        "payload": "x".repeat(512)
                    }),
                )
                .await
                .unwrap();
                assert_success(tx.commit().await.unwrap());
            }
            setup.storage().checkpoint().await.unwrap();
            assert!(
                setup.storage().oldest_retained_wal_lsn().unwrap_or(0) > 0,
                "test setup must force retained-WAL catch-up gap"
            );
            setup.close().await.unwrap();
        }

        {
            let stale = Database::open(replica_root.join("app"), database_config.clone(), None)
                .await
                .unwrap();
            stale.set_generation(3).await.unwrap();
            let mut tx = stale.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());
            let mut tx = stale.begin(TransactionOptions::default()).unwrap();
            tx.insert("users", serde_json::json!({"name": "Stale"}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());
            stale.close().await.unwrap();
        }

        let primary_registry = Arc::new(SystemDatabase::open(&primary_root).await.unwrap());
        let primary_runtime = start_configured_replication(
            Arc::clone(&primary_registry),
            &primary_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 1,
                role: NodeRole::Primary,
                generation: 3,
                topology: topology.clone(),
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let primary_db = primary_registry.get_database_by_name("app").unwrap();
        for _ in 0..8 {
            primary_db
                .storage()
                .append_wal(0x01, &vec![0xAB; 512])
                .await
                .unwrap();
        }
        primary_db.storage().checkpoint().await.unwrap();
        let advertised_retained_lsn = primary_runtime._mesh.oldest_retained_lsn();
        assert!(
            advertised_retained_lsn > 0,
            "primary handshake should advertise the retained-WAL floor"
        );

        let replica_registry = Arc::new(SystemDatabase::open(&replica_root).await.unwrap());
        let replica_runtime = start_configured_replication(
            Arc::clone(&replica_registry),
            &replica_root,
            database_config.clone(),
            ReplicationConfig {
                database: "app".to_string(),
                self_id: 2,
                role: NodeRole::Replica,
                generation: 1,
                topology,
                snapshot_chunk_bytes: 1024,
            },
        )
        .await
        .unwrap();
        let initial_replica_database_id = replica_registry
            .list_databases()
            .into_iter()
            .find(|database| database.name == "app")
            .expect("existing replica database is registered at startup")
            .database_id;

        assert_eq!(replica_runtime.node_role, NodeRole::Replica);
        eventually(|| replica_runtime._mesh.connection(1).is_some()).await;
        assert_eq!(
            replica_runtime
                ._mesh
                .connection(1)
                .unwrap()
                .peer_oldest_retained_lsn(),
            advertised_retained_lsn
        );

        eventually(|| {
            replica_registry
                .list_databases()
                .into_iter()
                .any(|database| {
                    database.name == "app" && database.database_id != initial_replica_database_id
                })
        })
        .await;
        let mut restored = false;
        for _ in 0..100 {
            let replica_db = replica_registry.get_database_by_name("app").unwrap();
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            restored = tx
                .get("users", &ada_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Ada"))
                .unwrap_or(false);
            tx.rollback();
            if restored {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(restored, "existing replica should restore primary snapshot");
        let replica_db = replica_registry.get_database_by_name("app").unwrap();
        let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &ada_id).await.unwrap().unwrap()["name"],
            "Ada"
        );
        tx.rollback();

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        let grace_id = tx
            .insert("users", serde_json::json!({"name": "Grace", "age": 42}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut replicated = false;
        for _ in 0..100 {
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            replicated = tx
                .get("users", &grace_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Grace"))
                .unwrap_or(false);
            tx.rollback();
            if replicated {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(
            replicated,
            "existing replica should apply live WAL after snapshot replacement"
        );

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_db);
        drop(replica_db);
        let primary_registry = Arc::try_unwrap(primary_registry).ok().unwrap();
        primary_registry.close().await.unwrap();
        let replica_registry = Arc::try_unwrap(replica_registry).ok().unwrap();
        replica_registry.close().await.unwrap();

        let reopened_replica = SystemDatabase::open(&replica_root).await.unwrap();
        let replica_db = reopened_replica
            .get_database_by_name("app")
            .expect("snapshot-replaced replica database should persist in registry");
        let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
        assert_eq!(
            tx.get("users", &ada_id).await.unwrap().unwrap()["name"],
            "Ada"
        );
        assert_eq!(
            tx.get("users", &grace_id).await.unwrap().unwrap()["name"],
            "Grace"
        );
        tx.rollback();
        drop(replica_db);
        reopened_replica.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_wal_applier_applies_primary_commits_over_peer_mesh() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_path = tmp.path().join("primary");
        let replica_path = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(1, topology.clone())).unwrap());
        let replica_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(2, topology)).unwrap());

        let replica_db = Arc::new(
            Database::open(&replica_path, DatabaseConfig::default(), None)
                .await
                .unwrap(),
        );
        let replica_applier = Arc::new(DatabaseWalApplier::new(Arc::clone(&replica_db)));
        let primary_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&primary_cluster),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(NoopWalApplier),
        ));
        let replica_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&replica_cluster),
            NodeRole::Replica,
            20,
            0,
            0,
            replica_applier,
        ));

        let primary_db = Database::open(
            &primary_path,
            DatabaseConfig::default(),
            Some(Box::new(PrimaryReplicator::from_mesh(Arc::clone(
                &primary_mesh,
            )))),
        )
        .await
        .unwrap();
        primary_mesh.set_catchup_source(Arc::new(StorageWalCatchupSource::new(Arc::clone(
            primary_db.storage(),
        ))));

        let replica_runtime = replica_mesh
            .start_tcp_with_listener(listener2)
            .await
            .unwrap();
        let primary_runtime = primary_mesh
            .start_tcp_with_listener(listener1)
            .await
            .unwrap();
        eventually(|| {
            primary_mesh
                .connection(2)
                .is_some_and(|conn| conn.is_connected())
                && replica_mesh
                    .connection(1)
                    .is_some_and(|conn| conn.is_connected())
        })
        .await;

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        eventually(|| {
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            let ready = tx
                .list_indexes("users")
                .map(|indexes| {
                    indexes.iter().any(|index| {
                        index.name == "age_idx" && index.state == exdb::IndexState::Ready
                    })
                })
                .unwrap_or(false);
            tx.rollback();
            ready
        })
        .await;

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.insert("users", serde_json::json!({"name": "Ada", "age": 37}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
        let docs = tx
            .query(
                "users",
                "age_idx",
                &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(37))],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Ada");
        tx.rollback();
        assert!(
            !replica_db.check_integrity().await.unwrap().has_errors(),
            "replica should remain clean after mesh-applied commits"
        );
        let replica_applied_lsn = replica_db.replication_applied_lsn().await;
        assert!(replica_applied_lsn > 0);
        assert_eq!(
            primary_cluster.status(2).unwrap().applied_lsn,
            replica_applied_lsn
        );

        primary_db.close().await.unwrap();
        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_mesh);
        drop(replica_mesh);
        let replica_db = match Arc::try_unwrap(replica_db) {
            Ok(database) => database,
            Err(_) => panic!("test holds sole replica db ref"),
        };
        replica_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn replica_catches_up_from_retained_primary_wal_on_mesh_attach() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_path = tmp.path().join("primary");
        let replica_path = tmp.path().join("replica");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(1, topology.clone())).unwrap());
        let replica_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(2, topology)).unwrap());

        let primary_db = Arc::new(
            Database::open(&primary_path, DatabaseConfig::default(), None)
                .await
                .unwrap(),
        );
        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        let doc_id = tx
            .insert("users", serde_json::json!({"name": "Grace"}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let replica_db = Arc::new(
            Database::open(&replica_path, DatabaseConfig::default(), None)
                .await
                .unwrap(),
        );
        let primary_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&primary_cluster),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(NoopWalApplier),
        ));
        primary_mesh.set_catchup_source(Arc::new(StorageWalCatchupSource::new(Arc::clone(
            primary_db.storage(),
        ))));
        let replica_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&replica_cluster),
            NodeRole::Replica,
            20,
            replica_db.replication_applied_lsn().await,
            0,
            Arc::new(DatabaseWalApplier::new(Arc::clone(&replica_db))),
        ));

        let replica_runtime = replica_mesh
            .start_tcp_with_listener(listener2)
            .await
            .unwrap();
        let primary_runtime = primary_mesh
            .start_tcp_with_listener(listener1)
            .await
            .unwrap();
        eventually(|| {
            primary_mesh
                .connection(2)
                .is_some_and(|conn| conn.is_connected())
                && replica_mesh
                    .connection(1)
                    .is_some_and(|conn| conn.is_connected())
        })
        .await;

        let mut caught_up = false;
        for _ in 0..100 {
            let mut tx = replica_db.begin(TransactionOptions::readonly()).unwrap();
            caught_up = tx
                .get("users", &doc_id)
                .await
                .map(|doc| doc.is_some_and(|doc| doc["name"] == "Grace"))
                .unwrap_or(false);
            tx.rollback();
            if caught_up {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(caught_up, "replica should catch up retained primary WAL");

        assert!(
            replica_db.replication_applied_lsn().await > 0,
            "catch-up should advance durable replica source progress"
        );
        assert!(
            !replica_db.check_integrity().await.unwrap().has_errors(),
            "catch-up replica should remain clean"
        );

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_mesh);
        drop(replica_mesh);
        let primary_db = match Arc::try_unwrap(primary_db) {
            Ok(database) => database,
            Err(_) => panic!("test holds sole primary db ref"),
        };
        primary_db.close().await.unwrap();
        let replica_db = match Arc::try_unwrap(replica_db) {
            Ok(database) => database,
            Err(_) => panic!("test holds sole replica db ref"),
        };
        replica_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_snapshot_source_and_sink_restore_over_peer_mesh() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_path = tmp.path().join("primary");
        let restored_path = tmp.path().join("restored");
        let (listener1, addr1) = bound_listener();
        let (listener2, addr2) = bound_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(1, topology.clone())).unwrap());
        let replica_cluster =
            Arc::new(ClusterMembership::new(ClusterConfig::new(2, topology)).unwrap());

        let primary_db = Arc::new(
            Database::open(&primary_path, DatabaseConfig::default(), None)
                .await
                .unwrap(),
        );
        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        eventually(|| {
            let mut tx = primary_db.begin(TransactionOptions::readonly()).unwrap();
            let ready = tx
                .list_indexes("users")
                .map(|indexes| {
                    indexes.iter().any(|index| {
                        index.name == "age_idx" && index.state == exdb::IndexState::Ready
                    })
                })
                .unwrap_or(false);
            tx.rollback();
            ready
        })
        .await;

        let mut tx = primary_db.begin(TransactionOptions::default()).unwrap();
        tx.insert("users", serde_json::json!({"name": "Ada", "age": 37}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let primary_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&primary_cluster),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(NoopWalApplier),
        ));
        primary_mesh.set_catchup_source(Arc::new(UnavailableCatchupSource {
            oldest_retained_lsn: 1,
        }));
        primary_mesh.set_snapshot_source(Arc::new(DatabaseSnapshotSource::new(
            Arc::clone(&primary_db),
            1024,
        )));
        let snapshot_sink = Arc::new(DatabaseSnapshotSink::new(
            restored_path.clone(),
            DatabaseConfig::default(),
        ));
        let replica_mesh = Arc::new(PeerMesh::new(
            Arc::clone(&replica_cluster),
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(NoopWalApplier),
        ));
        replica_mesh.set_snapshot_sink(snapshot_sink);

        let replica_runtime = replica_mesh
            .start_tcp_with_listener(listener2)
            .await
            .unwrap();
        let primary_runtime = primary_mesh
            .start_tcp_with_listener(listener1)
            .await
            .unwrap();
        eventually(|| {
            primary_mesh
                .connection(2)
                .is_some_and(|conn| conn.is_connected())
                && replica_mesh
                    .connection(1)
                    .is_some_and(|conn| conn.is_connected())
        })
        .await;

        eventually(|| restored_path.join("wal/segment-000001.wal").exists()).await;

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
        drop(primary_mesh);
        drop(replica_mesh);
        let primary_db = match Arc::try_unwrap(primary_db) {
            Ok(database) => database,
            Err(_) => panic!("test holds sole primary db ref"),
        };
        primary_db.close().await.unwrap();

        let restored = Database::open(&restored_path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = restored.begin(TransactionOptions::readonly()).unwrap();
        let docs = tx
            .query(
                "users",
                "age_idx",
                &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(37))],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Ada");
        tx.rollback();
        assert!(
            !restored.check_integrity().await.unwrap().has_errors(),
            "snapshot-restored database should pass integrity checks"
        );
        restored.close().await.unwrap();
    }
}
