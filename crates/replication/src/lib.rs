//! exdb-replication — Layer 7: Replication Transport
//!
//! This crate provides the first production-facing L7 building blocks:
//! cluster membership/quorum tracking and a primary-side [`ReplicationHook`]
//! implementation. The network peer mesh, replica apply loop, snapshot transfer,
//! and promotion protocol build on these primitives.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use exdb_core::types::{Ts, TxId};
use exdb_storage::engine::StorageEngine;
use exdb_storage::wal::{Lsn, WAL_RECORD_TX_COMMIT};
use exdb_tx::{ReplicationHook, SubscriptionMode};
use futures_util::StreamExt;
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// Default maximum payload size for one framed peer message.
///
/// This caps the frame payload, excluding the 4-byte length prefix.
pub const DEFAULT_MAX_PEER_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Cluster node identifier.
pub type NodeId = u64;

/// Monotonic node generation, incremented when a node is replaced with fresh
/// local state.
pub type Generation = u64;

/// Role advertised in replication handshakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica,
}

impl NodeRole {
    fn to_wire(self) -> u8 {
        match self {
            NodeRole::Primary => 1,
            NodeRole::Replica => 2,
        }
    }

    fn from_wire(value: u8) -> Result<Self, ReplicationError> {
        match value {
            1 => Ok(NodeRole::Primary),
            2 => Ok(NodeRole::Replica),
            _ => Err(ReplicationError::InvalidNodeRole(value)),
        }
    }
}

/// Message exchanged on the single bidirectional peer connection between two
/// cluster nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    Handshake {
        node_id: NodeId,
        generation: Generation,
        role: NodeRole,
        applied_lsn: Lsn,
        applied_ts: Ts,
        oldest_retained_lsn: Lsn,
    },
    Heartbeat {
        node_id: NodeId,
        applied_lsn: Lsn,
        applied_ts: Ts,
    },
    WalRecord {
        lsn: Lsn,
        data: Vec<u8>,
    },
    WalAck {
        lsn: Lsn,
    },
    RequestCatchup {
        from_lsn: Lsn,
    },
    RequestSnapshot {
        from_lsn: Lsn,
    },
    CatchupUnavailable {
        requested_lsn: Lsn,
        oldest_retained_lsn: Lsn,
    },
    PromotionRequest {
        request_id: u64,
        begin_ts: Ts,
        subscription: SubscriptionMode,
        payload: Vec<u8>,
    },
    PromotionResponse {
        request_id: u64,
        outcome: PromotionOutcome,
    },
    SnapshotBegin,
    SnapshotData {
        chunk: Vec<u8>,
    },
    SnapshotEnd,
}

/// Local or remote handshake state for a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerHandshake {
    pub node_id: NodeId,
    pub generation: Generation,
    pub role: NodeRole,
    pub applied_lsn: Lsn,
    pub applied_ts: Ts,
    pub oldest_retained_lsn: Lsn,
}

impl PeerHandshake {
    fn into_message(self) -> PeerMessage {
        PeerMessage::Handshake {
            node_id: self.node_id,
            generation: self.generation,
            role: self.role,
            applied_lsn: self.applied_lsn,
            applied_ts: self.applied_ts,
            oldest_retained_lsn: self.oldest_retained_lsn,
        }
    }

    fn from_message(message: PeerMessage) -> Result<Self, ReplicationError> {
        match message {
            PeerMessage::Handshake {
                node_id,
                generation,
                role,
                applied_lsn,
                applied_ts,
                oldest_retained_lsn,
            } => Ok(Self {
                node_id,
                generation,
                role,
                applied_lsn,
                applied_ts,
                oldest_retained_lsn,
            }),
            other => Err(ReplicationError::ExpectedHandshake {
                received: peer_message_name(&other),
            }),
        }
    }
}

/// Local durable state used to select a replica recovery strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalState {
    pub data_file_exists: bool,
    pub data_file_valid: bool,
    pub wal_intact: bool,
    pub applied_lsn: Option<Lsn>,
}

/// Recovery strategy chosen for a connecting or restarting replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTier {
    /// Tier 1: primary WAL is retained from this LSN, so catch-up can stream.
    IncrementalCatchup { from_lsn: Lsn },
    /// Tier 2: local storage can recover first, then use retained primary WAL.
    LocalRecoveryThenCatchup,
    /// Tier 3: retained WAL is insufficient or local state cannot be trusted.
    FullReconstruction,
}

/// Result of a transaction promotion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionOutcome {
    Success {
        commit_ts: Ts,
    },
    Response {
        payload: Vec<u8>,
    },
    Conflict {
        error: Vec<u8>,
        retry: Option<PromotionRetry>,
    },
}

/// Subscribe-mode retry metadata returned by a primary after promotion conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionRetry {
    pub new_tx: TxId,
    pub new_ts: Ts,
}

/// Select the recovery tier from local state, generation continuity, and the
/// primary's oldest retained WAL LSN.
pub fn select_recovery_tier(
    local_state: &LocalState,
    primary_oldest_retained_lsn: Lsn,
    peer_generation: Generation,
    previous_generation: Option<Generation>,
) -> RecoveryTier {
    let from_lsn = local_state.applied_lsn.unwrap_or(0);
    let retained_wal_covers_local_progress = from_lsn >= primary_oldest_retained_lsn;
    let same_generation = previous_generation == Some(peer_generation);
    let generation_changed = previous_generation.is_some() && !same_generation;

    if generation_changed {
        return if from_lsn == 0 && primary_oldest_retained_lsn == 0 {
            RecoveryTier::IncrementalCatchup { from_lsn }
        } else {
            RecoveryTier::FullReconstruction
        };
    }

    if !local_state.data_file_exists || !local_state.data_file_valid {
        return if from_lsn == 0 && primary_oldest_retained_lsn == 0 {
            RecoveryTier::IncrementalCatchup { from_lsn }
        } else {
            RecoveryTier::FullReconstruction
        };
    }

    if !retained_wal_covers_local_progress {
        return RecoveryTier::FullReconstruction;
    }

    if same_generation && !local_state.wal_intact {
        RecoveryTier::LocalRecoveryThenCatchup
    } else {
        RecoveryTier::IncrementalCatchup { from_lsn }
    }
}

/// L7 cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub self_id: NodeId,
    pub topology: HashMap<NodeId, SocketAddr>,
    pub heartbeat_interval: Duration,
    pub suspect_timeout: Duration,
    pub down_timeout: Duration,
    pub primary_ack_timeout: Duration,
    pub replica_fence_timeout: Duration,
    pub wal_retention_duration: Duration,
    pub wal_retention_size: usize,
}

impl ClusterConfig {
    /// Build a config with design defaults.
    pub fn new(self_id: NodeId, topology: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            self_id,
            topology,
            heartbeat_interval: Duration::from_millis(500),
            suspect_timeout: Duration::from_secs(3),
            down_timeout: Duration::from_secs(10),
            primary_ack_timeout: Duration::from_secs(5),
            replica_fence_timeout: Duration::from_secs(2),
            wal_retention_duration: Duration::from_secs(24 * 60 * 60),
            wal_retention_size: 1024 * 1024 * 1024,
        }
    }
}

/// Node liveness state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Online,
    Suspect,
    Down,
}

/// Point-in-time status for a node in the replication topology.
#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: NodeState,
    pub last_heartbeat: Instant,
    pub applied_lsn: Lsn,
    pub applied_ts: Ts,
}

/// Errors returned by replication coordination.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReplicationError {
    #[error("self node {0} is missing from replication topology")]
    MissingSelf(NodeId),
    #[error("node {0} already exists in replication topology")]
    NodeAlreadyExists(NodeId),
    #[error("node {0} not found in replication topology")]
    NodeNotFound(NodeId),
    #[error("cannot remove self node {0} from replication topology")]
    CannotRemoveSelf(NodeId),
    #[error("cannot remove node {node_id}: state is {state:?}, expected Down")]
    NodeNotDown { node_id: NodeId, state: NodeState },
    #[error("cannot remove node {0}: remaining topology would not have quorum")]
    RemovalWouldBreakQuorum(NodeId),
    #[error("replication primary is holding")]
    Holding,
    #[error("replication quorum is unavailable")]
    QuorumUnavailable,
    #[error("cannot promote transaction while local node role is {role:?}")]
    PromotionRoleMismatch { role: NodeRole },
    #[error("replication failed to reach majority: {acked_with_self} of {cluster_size}")]
    AckQuorumFailed {
        acked_with_self: usize,
        cluster_size: usize,
    },
    #[error("replication sender failed for node {node_id}: {message}")]
    SenderFailed { node_id: NodeId, message: String },
    #[error("replica WAL apply failed for peer {node_id}: {message}")]
    ApplyFailed { node_id: NodeId, message: String },
    #[error("replica promotion failed for peer {node_id}: {message}")]
    PromotionFailed { node_id: NodeId, message: String },
    #[error("replica has no connected primary for promotion")]
    NoPrimaryConnection,
    #[error("timed out waiting for promotion response from peer {node_id} request {request_id}")]
    PromotionTimedOut { node_id: NodeId, request_id: u64 },
    #[error("snapshot transfer failed for peer {node_id}: {message}")]
    SnapshotFailed { node_id: NodeId, message: String },
    #[error("replication catch-up failed: {message}")]
    CatchupFailed { message: String },
    #[error(
        "replication catch-up unavailable: requested LSN {requested_lsn} is older than oldest retained LSN {oldest_retained_lsn}"
    )]
    CatchupUnavailable {
        requested_lsn: Lsn,
        oldest_retained_lsn: Lsn,
    },
    #[error("invalid peer node role tag {0}")]
    InvalidNodeRole(u8),
    #[error("invalid peer message tag {0}")]
    InvalidPeerMessageTag(u8),
    #[error("peer message payload is truncated")]
    TruncatedPeerMessage,
    #[error("peer message has {remaining} trailing bytes")]
    TrailingPeerMessageBytes { remaining: usize },
    #[error("peer message payload length {len} exceeds limit {max}")]
    PeerMessageTooLarge { len: usize, max: usize },
    #[error("peer I/O failed: {message}")]
    PeerIo { message: String },
    #[error("expected peer handshake, received {received}")]
    ExpectedHandshake { received: &'static str },
    #[error(
        "local handshake node {handshake_node_id} does not match configured self node {self_id}"
    )]
    LocalNodeMismatch {
        self_id: NodeId,
        handshake_node_id: NodeId,
    },
    #[error("peer {0} is not in replication topology")]
    UnknownPeer(NodeId),
    #[error("peer {0} identified as self")]
    PeerIsSelf(NodeId),
    #[error("peer {0} disconnected")]
    PeerDisconnected(NodeId),
    #[error("timed out waiting for WAL ack from peer {node_id} at lsn {lsn}")]
    WalAckTimedOut { node_id: NodeId, lsn: Lsn },
    #[error("unexpected peer message from node {node_id}: {message}")]
    UnexpectedPeerMessage {
        node_id: NodeId,
        message: &'static str,
    },
    #[error("node {self_id} must not initiate peer connection to lower node {peer_id}")]
    InitiationRuleViolation { self_id: NodeId, peer_id: NodeId },
}

impl ReplicationError {
    fn into_hook_error(self) -> String {
        self.to_string()
    }
}

fn peer_message_name(message: &PeerMessage) -> &'static str {
    match message {
        PeerMessage::Handshake { .. } => "handshake",
        PeerMessage::Heartbeat { .. } => "heartbeat",
        PeerMessage::WalRecord { .. } => "wal_record",
        PeerMessage::WalAck { .. } => "wal_ack",
        PeerMessage::RequestCatchup { .. } => "request_catchup",
        PeerMessage::RequestSnapshot { .. } => "request_snapshot",
        PeerMessage::CatchupUnavailable { .. } => "catchup_unavailable",
        PeerMessage::PromotionRequest { .. } => "promotion_request",
        PeerMessage::PromotionResponse { .. } => "promotion_response",
        PeerMessage::SnapshotBegin => "snapshot_begin",
        PeerMessage::SnapshotData { .. } => "snapshot_data",
        PeerMessage::SnapshotEnd => "snapshot_end",
    }
}

/// Encode a peer message payload.
///
/// The returned bytes do not include the 4-byte frame length prefix used by
/// [`write_peer_message_with_limit`].
pub fn encode_peer_message(message: &PeerMessage) -> Result<Vec<u8>, ReplicationError> {
    let mut out = Vec::new();
    match message {
        PeerMessage::Handshake {
            node_id,
            generation,
            role,
            applied_lsn,
            applied_ts,
            oldest_retained_lsn,
        } => {
            out.push(1);
            put_u64(&mut out, *node_id);
            put_u64(&mut out, *generation);
            out.push(role.to_wire());
            put_u64(&mut out, *applied_lsn);
            put_u64(&mut out, *applied_ts);
            put_u64(&mut out, *oldest_retained_lsn);
        }
        PeerMessage::Heartbeat {
            node_id,
            applied_lsn,
            applied_ts,
        } => {
            out.push(2);
            put_u64(&mut out, *node_id);
            put_u64(&mut out, *applied_lsn);
            put_u64(&mut out, *applied_ts);
        }
        PeerMessage::WalRecord { lsn, data } => {
            out.push(3);
            put_u64(&mut out, *lsn);
            put_bytes(&mut out, data)?;
        }
        PeerMessage::WalAck { lsn } => {
            out.push(4);
            put_u64(&mut out, *lsn);
        }
        PeerMessage::RequestCatchup { from_lsn } => {
            out.push(5);
            put_u64(&mut out, *from_lsn);
        }
        PeerMessage::RequestSnapshot { from_lsn } => {
            out.push(12);
            put_u64(&mut out, *from_lsn);
        }
        PeerMessage::CatchupUnavailable {
            requested_lsn,
            oldest_retained_lsn,
        } => {
            out.push(9);
            put_u64(&mut out, *requested_lsn);
            put_u64(&mut out, *oldest_retained_lsn);
        }
        PeerMessage::PromotionRequest {
            request_id,
            begin_ts,
            subscription,
            payload,
        } => {
            out.push(10);
            put_u64(&mut out, *request_id);
            put_u64(&mut out, *begin_ts);
            put_subscription_mode(&mut out, *subscription);
            put_bytes(&mut out, payload)?;
        }
        PeerMessage::PromotionResponse {
            request_id,
            outcome,
        } => {
            out.push(11);
            put_u64(&mut out, *request_id);
            encode_promotion_outcome(&mut out, outcome)?;
        }
        PeerMessage::SnapshotBegin => {
            out.push(6);
        }
        PeerMessage::SnapshotData { chunk } => {
            out.push(7);
            put_bytes(&mut out, chunk)?;
        }
        PeerMessage::SnapshotEnd => {
            out.push(8);
        }
    }
    Ok(out)
}

/// Decode a peer message payload.
///
/// `payload` is the exact frame body without the 4-byte length prefix.
pub fn decode_peer_message(payload: &[u8]) -> Result<PeerMessage, ReplicationError> {
    let mut cursor = PeerPayloadCursor::new(payload);
    let tag = cursor.take_u8()?;
    let message = match tag {
        1 => PeerMessage::Handshake {
            node_id: cursor.take_u64()?,
            generation: cursor.take_u64()?,
            role: NodeRole::from_wire(cursor.take_u8()?)?,
            applied_lsn: cursor.take_u64()?,
            applied_ts: cursor.take_u64()?,
            oldest_retained_lsn: cursor.take_u64()?,
        },
        2 => PeerMessage::Heartbeat {
            node_id: cursor.take_u64()?,
            applied_lsn: cursor.take_u64()?,
            applied_ts: cursor.take_u64()?,
        },
        3 => PeerMessage::WalRecord {
            lsn: cursor.take_u64()?,
            data: cursor.take_bytes()?.to_vec(),
        },
        4 => PeerMessage::WalAck {
            lsn: cursor.take_u64()?,
        },
        5 => PeerMessage::RequestCatchup {
            from_lsn: cursor.take_u64()?,
        },
        12 => PeerMessage::RequestSnapshot {
            from_lsn: cursor.take_u64()?,
        },
        9 => PeerMessage::CatchupUnavailable {
            requested_lsn: cursor.take_u64()?,
            oldest_retained_lsn: cursor.take_u64()?,
        },
        10 => PeerMessage::PromotionRequest {
            request_id: cursor.take_u64()?,
            begin_ts: cursor.take_u64()?,
            subscription: decode_subscription_mode(cursor.take_u8()?)?,
            payload: cursor.take_bytes()?.to_vec(),
        },
        11 => PeerMessage::PromotionResponse {
            request_id: cursor.take_u64()?,
            outcome: decode_promotion_outcome(&mut cursor)?,
        },
        6 => PeerMessage::SnapshotBegin,
        7 => PeerMessage::SnapshotData {
            chunk: cursor.take_bytes()?.to_vec(),
        },
        8 => PeerMessage::SnapshotEnd,
        _ => return Err(ReplicationError::InvalidPeerMessageTag(tag)),
    };
    cursor.finish()?;
    Ok(message)
}

/// Write one length-prefixed peer message.
pub async fn write_peer_message_with_limit<W>(
    writer: &mut W,
    message: &PeerMessage,
    max_payload_len: usize,
) -> Result<(), ReplicationError>
where
    W: AsyncWrite + Unpin,
{
    let payload = encode_peer_message(message)?;
    if payload.len() > max_payload_len {
        return Err(ReplicationError::PeerMessageTooLarge {
            len: payload.len(),
            max: max_payload_len,
        });
    }
    let frame_len =
        u32::try_from(payload.len()).map_err(|_| ReplicationError::PeerMessageTooLarge {
            len: payload.len(),
            max: u32::MAX as usize,
        })?;
    writer
        .write_all(&frame_len.to_be_bytes())
        .await
        .map_err(peer_io)?;
    writer.write_all(&payload).await.map_err(peer_io)?;
    writer.flush().await.map_err(peer_io)?;
    Ok(())
}

/// Read one length-prefixed peer message.
pub async fn read_peer_message_with_limit<R>(
    reader: &mut R,
    max_payload_len: usize,
) -> Result<PeerMessage, ReplicationError>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.map_err(peer_io)?;
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    if payload_len > max_payload_len {
        return Err(ReplicationError::PeerMessageTooLarge {
            len: payload_len,
            max: max_payload_len,
        });
    }

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await.map_err(peer_io)?;
    decode_peer_message(&payload)
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ReplicationError> {
    let len = u32::try_from(bytes.len()).map_err(|_| ReplicationError::PeerMessageTooLarge {
        len: bytes.len(),
        max: u32::MAX as usize,
    })?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_promotion_outcome(
    out: &mut Vec<u8>,
    outcome: &PromotionOutcome,
) -> Result<(), ReplicationError> {
    match outcome {
        PromotionOutcome::Success { commit_ts } => {
            out.push(1);
            put_u64(out, *commit_ts);
        }
        PromotionOutcome::Response { payload } => {
            out.push(3);
            put_bytes(out, payload)?;
        }
        PromotionOutcome::Conflict { error, retry } => {
            out.push(2);
            put_bytes(out, error)?;
            match retry {
                Some(retry) => {
                    out.push(1);
                    put_u64(out, retry.new_tx);
                    put_u64(out, retry.new_ts);
                }
                None => out.push(0),
            }
        }
    }
    Ok(())
}

fn decode_promotion_outcome(
    cursor: &mut PeerPayloadCursor<'_>,
) -> Result<PromotionOutcome, ReplicationError> {
    match cursor.take_u8()? {
        1 => Ok(PromotionOutcome::Success {
            commit_ts: cursor.take_u64()?,
        }),
        2 => {
            let error = cursor.take_bytes()?.to_vec();
            let retry = match cursor.take_u8()? {
                0 => None,
                1 => Some(PromotionRetry {
                    new_tx: cursor.take_u64()?,
                    new_ts: cursor.take_u64()?,
                }),
                tag => return Err(ReplicationError::InvalidPeerMessageTag(tag)),
            };
            Ok(PromotionOutcome::Conflict { error, retry })
        }
        3 => Ok(PromotionOutcome::Response {
            payload: cursor.take_bytes()?.to_vec(),
        }),
        tag => Err(ReplicationError::InvalidPeerMessageTag(tag)),
    }
}

fn put_subscription_mode(out: &mut Vec<u8>, mode: SubscriptionMode) {
    out.push(match mode {
        SubscriptionMode::None => 0,
        SubscriptionMode::Notify => 1,
        SubscriptionMode::Subscribe => 2,
        SubscriptionMode::Watch => 3,
    });
}

fn decode_subscription_mode(value: u8) -> Result<SubscriptionMode, ReplicationError> {
    match value {
        0 => Ok(SubscriptionMode::None),
        1 => Ok(SubscriptionMode::Notify),
        2 => Ok(SubscriptionMode::Subscribe),
        3 => Ok(SubscriptionMode::Watch),
        tag => Err(ReplicationError::InvalidPeerMessageTag(tag)),
    }
}

fn peer_io(err: std::io::Error) -> ReplicationError {
    ReplicationError::PeerIo {
        message: err.to_string(),
    }
}

struct PeerPayloadCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PeerPayloadCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn finish(&self) -> Result<(), ReplicationError> {
        let remaining = self.payload.len().saturating_sub(self.offset);
        if remaining == 0 {
            Ok(())
        } else {
            Err(ReplicationError::TrailingPeerMessageBytes { remaining })
        }
    }

    fn take_u8(&mut self) -> Result<u8, ReplicationError> {
        if self.offset >= self.payload.len() {
            return Err(ReplicationError::TruncatedPeerMessage);
        }
        let value = self.payload[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn take_u64(&mut self) -> Result<u64, ReplicationError> {
        let bytes = self.take_exact(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn take_bytes(&mut self) -> Result<&'a [u8], ReplicationError> {
        let len_bytes = self.take_exact(4)?;
        let len = u32::from_be_bytes(len_bytes.try_into().unwrap()) as usize;
        self.take_exact(len)
    }

    fn take_exact(&mut self, len: usize) -> Result<&'a [u8], ReplicationError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ReplicationError::TruncatedPeerMessage)?;
        if end > self.payload.len() {
            return Err(ReplicationError::TruncatedPeerMessage);
        }
        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

/// Cluster topology plus liveness/progress state.
pub struct ClusterMembership {
    config: RwLock<ClusterConfig>,
    nodes: RwLock<HashMap<NodeId, NodeStatus>>,
}

impl ClusterMembership {
    /// Create membership with every peer initially down except self.
    pub fn new(config: ClusterConfig) -> Result<Self, ReplicationError> {
        if !config.topology.contains_key(&config.self_id) {
            return Err(ReplicationError::MissingSelf(config.self_id));
        }

        let now = Instant::now();
        let nodes = config
            .topology
            .iter()
            .map(|(node_id, addr)| {
                (
                    *node_id,
                    NodeStatus {
                        node_id: *node_id,
                        addr: *addr,
                        state: if *node_id == config.self_id {
                            NodeState::Online
                        } else {
                            NodeState::Down
                        },
                        last_heartbeat: now,
                        applied_lsn: 0,
                        applied_ts: 0,
                    },
                )
            })
            .collect();

        Ok(Self {
            config: RwLock::new(config),
            nodes: RwLock::new(nodes),
        })
    }

    pub fn config(&self) -> ClusterConfig {
        self.config.read().clone()
    }

    pub fn cluster_size(&self) -> usize {
        self.nodes.read().len()
    }

    pub fn self_id(&self) -> NodeId {
        self.config.read().self_id
    }

    /// Number of nodes not marked Down. Suspect nodes count toward quorum per
    /// the L7 timeout design to avoid flapping on short network blips.
    pub fn online_count(&self) -> usize {
        self.nodes
            .read()
            .values()
            .filter(|status| status.state != NodeState::Down)
            .count()
    }

    pub fn has_quorum(&self) -> bool {
        has_majority(self.online_count(), self.cluster_size())
    }

    /// Online replicas, excluding self and Suspect peers.
    pub fn online_replicas(&self) -> Vec<NodeId> {
        let self_id = self.self_id();
        self.nodes
            .read()
            .values()
            .filter(|status| status.node_id != self_id && status.state == NodeState::Online)
            .map(|status| status.node_id)
            .collect()
    }

    pub fn status(&self, node_id: NodeId) -> Option<NodeStatus> {
        self.nodes.read().get(&node_id).cloned()
    }

    pub fn record_heartbeat(&self, node_id: NodeId, applied_lsn: Lsn, applied_ts: Ts) {
        if let Some(status) = self.nodes.write().get_mut(&node_id) {
            status.state = NodeState::Online;
            status.last_heartbeat = Instant::now();
            status.applied_lsn = status.applied_lsn.max(applied_lsn);
            status.applied_ts = status.applied_ts.max(applied_ts);
        }
    }

    pub fn record_wal_ack(&self, node_id: NodeId, applied_lsn: Lsn) {
        if let Some(status) = self.nodes.write().get_mut(&node_id) {
            status.applied_lsn = status.applied_lsn.max(applied_lsn);
            if status.state != NodeState::Down {
                status.state = NodeState::Online;
            }
        }
    }

    pub fn check_timeouts(&self) {
        let config = self.config.read().clone();
        let now = Instant::now();
        for status in self.nodes.write().values_mut() {
            if status.node_id == config.self_id {
                continue;
            }
            let elapsed = now.duration_since(status.last_heartbeat);
            status.state = if elapsed >= config.down_timeout {
                NodeState::Down
            } else if elapsed >= config.suspect_timeout {
                NodeState::Suspect
            } else {
                status.state
            };
        }
    }

    pub fn update_peer_addr(
        &self,
        node_id: NodeId,
        new_addr: SocketAddr,
    ) -> Result<(), ReplicationError> {
        self.config
            .write()
            .topology
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))
            .map(|addr| *addr = new_addr)?;
        self.nodes
            .write()
            .get_mut(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))
            .map(|status| status.addr = new_addr)
    }

    pub fn add_node(&self, node_id: NodeId, addr: SocketAddr) -> Result<(), ReplicationError> {
        let mut config = self.config.write();
        if config.topology.contains_key(&node_id) {
            return Err(ReplicationError::NodeAlreadyExists(node_id));
        }
        config.topology.insert(node_id, addr);
        self.nodes.write().insert(
            node_id,
            NodeStatus {
                node_id,
                addr,
                state: NodeState::Down,
                last_heartbeat: Instant::now(),
                applied_lsn: 0,
                applied_ts: 0,
            },
        );
        Ok(())
    }

    pub fn remove_node(&self, node_id: NodeId) -> Result<(), ReplicationError> {
        let self_id = self.self_id();
        if node_id == self_id {
            return Err(ReplicationError::CannotRemoveSelf(node_id));
        }

        let nodes = self.nodes.read();
        let status = nodes
            .get(&node_id)
            .ok_or(ReplicationError::NodeNotFound(node_id))?;
        if status.state != NodeState::Down {
            return Err(ReplicationError::NodeNotDown {
                node_id,
                state: status.state,
            });
        }

        let remaining_cluster_size = nodes.len().saturating_sub(1);
        let remaining_online = nodes
            .values()
            .filter(|status| status.node_id != node_id && status.state != NodeState::Down)
            .count();
        drop(nodes);

        if !has_majority(remaining_online, remaining_cluster_size) {
            return Err(ReplicationError::RemovalWouldBreakQuorum(node_id));
        }

        self.config.write().topology.remove(&node_id);
        self.nodes.write().remove(&node_id);
        Ok(())
    }

    pub fn min_replica_lsn(&self) -> Option<Lsn> {
        let self_id = self.self_id();
        self.nodes
            .read()
            .values()
            .filter(|status| status.node_id != self_id)
            .map(|status| status.applied_lsn)
            .min()
    }
}

/// Applies WAL records received from a peer before the connection sends
/// `WalAck`.
#[async_trait]
pub trait WalApplier: Send + Sync {
    async fn apply_wal_record(
        &self,
        peer_id: NodeId,
        lsn: Lsn,
        record: &[u8],
    ) -> Result<(Lsn, Ts), ReplicationError>;
}

/// Source of retained primary WAL records for Tier 1 catch-up.
#[async_trait]
pub trait WalCatchupSource: Send + Sync {
    fn oldest_retained_lsn(&self) -> Option<Lsn> {
        None
    }

    async fn records_from(&self, from_lsn: Lsn) -> Result<Vec<(Lsn, Vec<u8>)>, ReplicationError>;
}

/// Empty catch-up source used by replicas and tests that do not exercise
/// retained-WAL catch-up.
pub struct NoWalCatchupSource;

#[async_trait]
impl WalCatchupSource for NoWalCatchupSource {
    async fn records_from(&self, _from_lsn: Lsn) -> Result<Vec<(Lsn, Vec<u8>)>, ReplicationError> {
        Ok(Vec::new())
    }
}

/// Storage-backed retained WAL source for primary catch-up.
///
/// Only `TxCommit` payloads are streamed. Replica apply writes its own local
/// `VisibleTs` records after each replicated commit, matching the live WAL path.
pub struct StorageWalCatchupSource {
    storage: Arc<StorageEngine>,
}

impl StorageWalCatchupSource {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl WalCatchupSource for StorageWalCatchupSource {
    fn oldest_retained_lsn(&self) -> Option<Lsn> {
        self.storage.oldest_retained_wal_lsn()
    }

    async fn records_from(&self, from_lsn: Lsn) -> Result<Vec<(Lsn, Vec<u8>)>, ReplicationError> {
        if let Some(oldest_retained_lsn) = self.storage.oldest_retained_wal_lsn()
            && from_lsn < oldest_retained_lsn
        {
            return Err(ReplicationError::CatchupUnavailable {
                requested_lsn: from_lsn,
                oldest_retained_lsn,
            });
        }

        let mut stream = self.storage.read_wal_from(from_lsn);
        let mut records = Vec::new();
        while let Some(record) = stream.next().await {
            let record = record.map_err(|err| ReplicationError::CatchupFailed {
                message: err.to_string(),
            })?;
            if record.record_type == WAL_RECORD_TX_COMMIT {
                records.push((record.lsn, record.payload));
            }
        }
        Ok(records)
    }
}

/// Handles write promotion requests received by a primary.
#[async_trait]
pub trait PromotionHandler: Send + Sync {
    async fn handle_promotion(
        &self,
        peer_id: NodeId,
        begin_ts: Ts,
        subscription: SubscriptionMode,
        payload: &[u8],
    ) -> Result<PromotionOutcome, ReplicationError>;
}

/// Default promotion handler used until a primary installs L6/L8 commit logic.
pub struct NoPromotionHandler;

#[async_trait]
impl PromotionHandler for NoPromotionHandler {
    async fn handle_promotion(
        &self,
        peer_id: NodeId,
        _begin_ts: Ts,
        _subscription: SubscriptionMode,
        _payload: &[u8],
    ) -> Result<PromotionOutcome, ReplicationError> {
        Err(ReplicationError::PromotionFailed {
            node_id: peer_id,
            message: "promotion handler is not installed".to_string(),
        })
    }
}

/// Receives a snapshot stream from a healthy peer.
#[async_trait]
pub trait SnapshotSink: Send + Sync {
    async fn begin_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError>;

    async fn apply_snapshot_chunk(
        &self,
        peer_id: NodeId,
        chunk: &[u8],
    ) -> Result<(), ReplicationError>;

    async fn end_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError>;
}

/// Produces a consistent snapshot stream for Tier 3 replica reconstruction.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    async fn snapshot_chunks(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError>;
}

/// Default snapshot sink used until database reconstruction logic is installed.
pub struct NoSnapshotSink;

#[async_trait]
impl SnapshotSink for NoSnapshotSink {
    async fn begin_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
        Err(ReplicationError::SnapshotFailed {
            node_id: peer_id,
            message: "snapshot sink is not installed".to_string(),
        })
    }

    async fn apply_snapshot_chunk(
        &self,
        peer_id: NodeId,
        _chunk: &[u8],
    ) -> Result<(), ReplicationError> {
        Err(ReplicationError::SnapshotFailed {
            node_id: peer_id,
            message: "snapshot sink is not installed".to_string(),
        })
    }

    async fn end_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
        Err(ReplicationError::SnapshotFailed {
            node_id: peer_id,
            message: "snapshot sink is not installed".to_string(),
        })
    }
}

type AckWaiter = oneshot::Sender<Result<(), ReplicationError>>;
type PendingAcks = Arc<Mutex<HashMap<Lsn, Vec<AckWaiter>>>>;
type PromotionWaiter = oneshot::Sender<Result<PromotionOutcome, ReplicationError>>;
type PendingPromotions = Arc<Mutex<HashMap<u64, PromotionWaiter>>>;

/// A live bidirectional connection to one replication peer.
///
/// The connection owns independent reader/writer tasks. Received heartbeats and
/// WAL acknowledgements update [`ClusterMembership`]; received WAL records are
/// applied through [`WalApplier`] before an acknowledgement is sent.
#[derive(Clone)]
pub struct PeerConnection {
    inner: Arc<PeerConnectionInner>,
}

struct PeerConnectionInner {
    local_node_id: NodeId,
    peer: PeerHandshake,
    outbound: mpsc::Sender<PeerMessage>,
    pending_acks: PendingAcks,
    pending_promotions: PendingPromotions,
    next_promotion_request_id: AtomicU64,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    connected: AtomicBool,
    shutdown_tx: watch::Sender<bool>,
}

impl PeerConnection {
    /// Establish a framed peer connection on an already-open stream.
    ///
    /// This performs the symmetric handshake used by the peer mesh: write the
    /// local handshake, read the remote handshake, validate it against the
    /// cluster topology, then spawn background read/write loops.
    pub async fn connect_stream<S>(
        stream: S,
        cluster: Arc<ClusterMembership>,
        local: PeerHandshake,
        wal_applier: Arc<dyn WalApplier>,
        max_payload_len: usize,
    ) -> Result<Self, ReplicationError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::connect_stream_with_catchup(
            stream,
            cluster,
            local,
            wal_applier,
            Arc::new(NoWalCatchupSource),
            Arc::new(NoPromotionHandler),
            Arc::new(NoSnapshotSink),
            None,
            max_payload_len,
        )
        .await
    }

    /// Establish a peer connection with a retained-WAL catch-up source.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_stream_with_catchup<S>(
        mut stream: S,
        cluster: Arc<ClusterMembership>,
        local: PeerHandshake,
        wal_applier: Arc<dyn WalApplier>,
        catchup_source: Arc<dyn WalCatchupSource>,
        promotion_handler: Arc<dyn PromotionHandler>,
        snapshot_sink: Arc<dyn SnapshotSink>,
        snapshot_source: Option<Arc<dyn SnapshotSource>>,
        max_payload_len: usize,
    ) -> Result<Self, ReplicationError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let self_id = cluster.self_id();
        if local.node_id != self_id {
            return Err(ReplicationError::LocalNodeMismatch {
                self_id,
                handshake_node_id: local.node_id,
            });
        }

        write_peer_message_with_limit(&mut stream, &local.into_message(), max_payload_len).await?;
        let peer = PeerHandshake::from_message(
            read_peer_message_with_limit(&mut stream, max_payload_len).await?,
        )?;

        if peer.node_id == self_id {
            return Err(ReplicationError::PeerIsSelf(peer.node_id));
        }
        if cluster.status(peer.node_id).is_none() {
            return Err(ReplicationError::UnknownPeer(peer.node_id));
        }

        cluster.record_heartbeat(peer.node_id, peer.applied_lsn, peer.applied_ts);

        let (reader, writer) = tokio::io::split(stream);
        let (outbound, outbound_rx) = mpsc::channel(256);
        let pending_acks = Arc::new(Mutex::new(HashMap::new()));
        let pending_promotions = Arc::new(Mutex::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection = Self {
            inner: Arc::new(PeerConnectionInner {
                local_node_id: local.node_id,
                peer,
                outbound: outbound.clone(),
                pending_acks: pending_acks.clone(),
                pending_promotions: pending_promotions.clone(),
                next_promotion_request_id: AtomicU64::new(1),
                applied_lsn: AtomicU64::new(peer.applied_lsn),
                applied_ts: AtomicU64::new(peer.applied_ts),
                connected: AtomicBool::new(true),
                shutdown_tx,
            }),
        };

        spawn_peer_writer(
            peer.node_id,
            writer,
            outbound_rx,
            connection.clone(),
            shutdown_rx.clone(),
            max_payload_len,
        );
        spawn_peer_reader(
            peer.node_id,
            reader,
            outbound,
            cluster,
            wal_applier,
            catchup_source,
            promotion_handler,
            snapshot_sink,
            snapshot_source,
            connection.clone(),
            shutdown_rx,
            max_payload_len,
        );

        Ok(connection)
    }

    pub fn peer_id(&self) -> NodeId {
        self.inner.peer.node_id
    }

    pub fn peer_generation(&self) -> Generation {
        self.inner.peer.generation
    }

    pub fn peer_role(&self) -> NodeRole {
        self.inner.peer.role
    }

    pub fn peer_oldest_retained_lsn(&self) -> Lsn {
        self.inner.peer.oldest_retained_lsn
    }

    pub fn applied_lsn(&self) -> Lsn {
        self.inner.applied_lsn.load(Ordering::Acquire)
    }

    pub fn applied_ts(&self) -> Ts {
        self.inner.applied_ts.load(Ordering::Acquire)
    }

    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.disconnect();
        let _ = self.inner.shutdown_tx.send(true);
    }

    pub async fn send_heartbeat(
        &self,
        applied_lsn: Lsn,
        applied_ts: Ts,
    ) -> Result<(), ReplicationError> {
        self.send(PeerMessage::Heartbeat {
            node_id: self.inner.local_node_id,
            applied_lsn,
            applied_ts,
        })
        .await
    }

    pub async fn send(&self, message: PeerMessage) -> Result<(), ReplicationError> {
        if !self.is_connected() {
            return Err(ReplicationError::PeerDisconnected(self.peer_id()));
        }
        self.inner.outbound.send(message).await.map_err(|_| {
            self.disconnect();
            ReplicationError::PeerDisconnected(self.peer_id())
        })
    }

    pub async fn request_catchup(&self, from_lsn: Lsn) -> Result<(), ReplicationError> {
        self.send(PeerMessage::RequestCatchup { from_lsn }).await
    }

    pub async fn request_snapshot(&self, from_lsn: Lsn) -> Result<(), ReplicationError> {
        self.send(PeerMessage::RequestSnapshot { from_lsn }).await
    }

    pub async fn send_snapshot<I>(&self, chunks: I) -> Result<(), ReplicationError>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        self.send(PeerMessage::SnapshotBegin).await?;
        for chunk in chunks {
            self.send(PeerMessage::SnapshotData { chunk }).await?;
        }
        self.send(PeerMessage::SnapshotEnd).await
    }

    pub async fn promote_and_wait(
        &self,
        begin_ts: Ts,
        subscription: SubscriptionMode,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<PromotionOutcome, ReplicationError> {
        let request_id = self
            .inner
            .next_promotion_request_id
            .fetch_add(1, Ordering::AcqRel);
        let (tx, rx) = oneshot::channel();
        self.inner.pending_promotions.lock().insert(request_id, tx);

        if let Err(err) = self
            .send(PeerMessage::PromotionRequest {
                request_id,
                begin_ts,
                subscription,
                payload: payload.to_vec(),
            })
            .await
        {
            self.drop_promotion_waiter(request_id);
            return Err(err);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ReplicationError::PeerDisconnected(self.peer_id())),
            Err(_) => {
                self.drop_promotion_waiter(request_id);
                Err(ReplicationError::PromotionTimedOut {
                    node_id: self.peer_id(),
                    request_id,
                })
            }
        }
    }

    pub async fn send_wal_record_and_wait(
        &self,
        lsn: Lsn,
        record: &[u8],
        ack_timeout: Duration,
    ) -> Result<(), ReplicationError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending_acks
            .lock()
            .entry(lsn)
            .or_default()
            .push(tx);

        if let Err(err) = self
            .send(PeerMessage::WalRecord {
                lsn,
                data: record.to_vec(),
            })
            .await
        {
            self.drop_closed_ack_waiters(lsn);
            return Err(err);
        }

        match tokio::time::timeout(ack_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ReplicationError::PeerDisconnected(self.peer_id())),
            Err(_) => {
                self.drop_closed_ack_waiters(lsn);
                Err(ReplicationError::WalAckTimedOut {
                    node_id: self.peer_id(),
                    lsn,
                })
            }
        }
    }

    fn record_peer_progress(&self, applied_lsn: Lsn, applied_ts: Ts) {
        self.inner
            .applied_lsn
            .fetch_max(applied_lsn, Ordering::AcqRel);
        self.inner
            .applied_ts
            .fetch_max(applied_ts, Ordering::AcqRel);
    }

    fn drop_closed_ack_waiters(&self, lsn: Lsn) {
        let mut pending = self.inner.pending_acks.lock();
        if let Some(waiters) = pending.get_mut(&lsn) {
            waiters.retain(|waiter| !waiter.is_closed());
            if waiters.is_empty() {
                pending.remove(&lsn);
            }
        }
    }

    fn drop_promotion_waiter(&self, request_id: u64) {
        self.inner.pending_promotions.lock().remove(&request_id);
    }

    fn disconnect(&self) {
        if self.inner.connected.swap(false, Ordering::AcqRel) {
            drain_pending_acks(&self.inner.pending_acks, self.peer_id());
            drain_pending_promotions(&self.inner.pending_promotions, self.peer_id());
        }
    }
}

fn spawn_peer_writer<W>(
    _peer_id: NodeId,
    mut writer: W,
    mut outbound_rx: mpsc::Receiver<PeerMessage>,
    connection: PeerConnection,
    mut shutdown_rx: watch::Receiver<bool>,
    max_payload_len: usize,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                message = outbound_rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    if write_peer_message_with_limit(&mut writer, &message, max_payload_len)
                        .await
                        .is_err()
                    {
                        connection.disconnect();
                        break;
                    }
                }
            }
        }
        connection.disconnect();
    });
}

#[allow(clippy::too_many_arguments)]
fn spawn_peer_reader<R>(
    peer_id: NodeId,
    mut reader: R,
    outbound: mpsc::Sender<PeerMessage>,
    cluster: Arc<ClusterMembership>,
    wal_applier: Arc<dyn WalApplier>,
    catchup_source: Arc<dyn WalCatchupSource>,
    promotion_handler: Arc<dyn PromotionHandler>,
    snapshot_sink: Arc<dyn SnapshotSink>,
    snapshot_source: Option<Arc<dyn SnapshotSource>>,
    connection: PeerConnection,
    mut shutdown_rx: watch::Receiver<bool>,
    max_payload_len: usize,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let message = tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
                message = read_peer_message_with_limit(&mut reader, max_payload_len) => {
                    match message {
                        Ok(message) => message,
                        Err(_) => {
                            connection.disconnect();
                            break;
                        }
                    }
                }
            };

            let result = handle_peer_message(
                peer_id,
                message,
                &outbound,
                &cluster,
                wal_applier.as_ref(),
                catchup_source.as_ref(),
                promotion_handler.as_ref(),
                snapshot_sink.as_ref(),
                snapshot_source.as_deref(),
                &connection,
            )
            .await;
            if result.is_err() {
                connection.disconnect();
                break;
            }
        }
        connection.disconnect();
    });
}

#[allow(clippy::too_many_arguments)]
async fn handle_peer_message(
    peer_id: NodeId,
    message: PeerMessage,
    outbound: &mpsc::Sender<PeerMessage>,
    cluster: &ClusterMembership,
    wal_applier: &dyn WalApplier,
    catchup_source: &dyn WalCatchupSource,
    promotion_handler: &dyn PromotionHandler,
    snapshot_sink: &dyn SnapshotSink,
    snapshot_source: Option<&dyn SnapshotSource>,
    connection: &PeerConnection,
) -> Result<(), ReplicationError> {
    match message {
        PeerMessage::Heartbeat {
            node_id,
            applied_lsn,
            applied_ts,
        } => {
            if node_id != peer_id {
                return Err(ReplicationError::UnexpectedPeerMessage {
                    node_id: peer_id,
                    message: "heartbeat_from_wrong_node",
                });
            }
            cluster.record_heartbeat(node_id, applied_lsn, applied_ts);
            connection.record_peer_progress(applied_lsn, applied_ts);
            Ok(())
        }
        PeerMessage::WalAck { lsn } => {
            cluster.record_wal_ack(peer_id, lsn);
            connection.record_peer_progress(lsn, 0);
            if let Some(waiters) = connection.inner.pending_acks.lock().remove(&lsn) {
                for waiter in waiters {
                    let _ = waiter.send(Ok(()));
                }
            }
            Ok(())
        }
        PeerMessage::WalRecord { lsn, data } => {
            let (applied_lsn, applied_ts) =
                wal_applier.apply_wal_record(peer_id, lsn, &data).await?;
            connection.record_peer_progress(applied_lsn, applied_ts);
            outbound
                .send(PeerMessage::WalAck { lsn })
                .await
                .map_err(|_| ReplicationError::PeerDisconnected(peer_id))
        }
        PeerMessage::Handshake { .. } => Err(ReplicationError::UnexpectedPeerMessage {
            node_id: peer_id,
            message: "handshake_after_connect",
        }),
        PeerMessage::RequestCatchup { from_lsn } => {
            let records = match catchup_source.records_from(from_lsn).await {
                Ok(records) => records,
                Err(ReplicationError::CatchupUnavailable {
                    requested_lsn,
                    oldest_retained_lsn,
                }) => {
                    stream_snapshot_or_report_unavailable(
                        peer_id,
                        requested_lsn,
                        oldest_retained_lsn,
                        outbound,
                        cluster,
                        snapshot_source,
                    )
                    .await?;
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            for (lsn, data) in records {
                outbound
                    .send(PeerMessage::WalRecord { lsn, data })
                    .await
                    .map_err(|_| ReplicationError::PeerDisconnected(peer_id))?;
            }
            Ok(())
        }
        PeerMessage::RequestSnapshot { from_lsn } => {
            let oldest_retained_lsn = catchup_source.oldest_retained_lsn().unwrap_or(0);
            stream_snapshot_or_report_unavailable(
                peer_id,
                from_lsn,
                oldest_retained_lsn,
                outbound,
                cluster,
                snapshot_source,
            )
            .await
        }
        PeerMessage::CatchupUnavailable {
            requested_lsn: _,
            oldest_retained_lsn: _,
        } => Ok(()),
        PeerMessage::PromotionRequest {
            request_id,
            begin_ts,
            subscription,
            payload,
        } => {
            let outcome = promotion_handler
                .handle_promotion(peer_id, begin_ts, subscription, &payload)
                .await?;
            outbound
                .send(PeerMessage::PromotionResponse {
                    request_id,
                    outcome,
                })
                .await
                .map_err(|_| ReplicationError::PeerDisconnected(peer_id))
        }
        PeerMessage::PromotionResponse {
            request_id,
            outcome,
        } => {
            if let Some(waiter) = connection
                .inner
                .pending_promotions
                .lock()
                .remove(&request_id)
            {
                let _ = waiter.send(Ok(outcome));
            }
            Ok(())
        }
        PeerMessage::SnapshotBegin => snapshot_sink.begin_snapshot(peer_id).await,
        PeerMessage::SnapshotData { chunk } => {
            snapshot_sink.apply_snapshot_chunk(peer_id, &chunk).await
        }
        PeerMessage::SnapshotEnd => snapshot_sink.end_snapshot(peer_id).await,
    }
}

async fn stream_snapshot_or_report_unavailable(
    peer_id: NodeId,
    requested_lsn: Lsn,
    oldest_retained_lsn: Lsn,
    outbound: &mpsc::Sender<PeerMessage>,
    cluster: &ClusterMembership,
    snapshot_source: Option<&dyn SnapshotSource>,
) -> Result<(), ReplicationError> {
    if let Some(snapshot_source) = snapshot_source
        && cluster.has_quorum()
    {
        let chunks = snapshot_source.snapshot_chunks(peer_id).await?;
        outbound
            .send(PeerMessage::SnapshotBegin)
            .await
            .map_err(|_| ReplicationError::PeerDisconnected(peer_id))?;
        for chunk in chunks {
            outbound
                .send(PeerMessage::SnapshotData { chunk })
                .await
                .map_err(|_| ReplicationError::PeerDisconnected(peer_id))?;
        }
        outbound
            .send(PeerMessage::SnapshotEnd)
            .await
            .map_err(|_| ReplicationError::PeerDisconnected(peer_id))?;
        return Ok(());
    }

    outbound
        .send(PeerMessage::CatchupUnavailable {
            requested_lsn,
            oldest_retained_lsn,
        })
        .await
        .map_err(|_| ReplicationError::PeerDisconnected(peer_id))
}

fn drain_pending_acks(pending_acks: &PendingAcks, peer_id: NodeId) {
    for waiters in pending_acks.lock().drain().map(|(_, waiters)| waiters) {
        for waiter in waiters {
            let _ = waiter.send(Err(ReplicationError::PeerDisconnected(peer_id)));
        }
    }
}

fn drain_pending_promotions(pending_promotions: &PendingPromotions, peer_id: NodeId) {
    for waiter in pending_promotions.lock().drain().map(|(_, waiter)| waiter) {
        let _ = waiter.send(Err(ReplicationError::PeerDisconnected(peer_id)));
    }
}

/// Owns the live single-connection-per-peer replication mesh.
///
/// This is the connection registry and routing layer. TCP listener/reconnect
/// loops, catch-up, snapshots, and promotion will build on this owner rather
/// than opening side-channel sockets.
pub struct PeerMesh {
    cluster: Arc<ClusterMembership>,
    self_role: RwLock<NodeRole>,
    generation: Generation,
    applied_lsn: AtomicU64,
    applied_ts: AtomicU64,
    oldest_retained_lsn: AtomicU64,
    force_snapshot_on_primary_attach: AtomicBool,
    wal_applier: Arc<dyn WalApplier>,
    catchup_source: RwLock<Arc<dyn WalCatchupSource>>,
    promotion_handler: RwLock<Arc<dyn PromotionHandler>>,
    snapshot_sink: RwLock<Arc<dyn SnapshotSink>>,
    snapshot_source: RwLock<Option<Arc<dyn SnapshotSource>>>,
    sender: Arc<PeerWalSender>,
    max_payload_len: usize,
}

/// Running TCP lifecycle for a [`PeerMesh`].
///
/// Dropping the handle aborts background tasks. Call [`PeerMeshRuntime::shutdown`]
/// when a graceful stop is preferred.
pub struct PeerMeshRuntime {
    mesh: Arc<PeerMesh>,
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    local_addr: SocketAddr,
}

impl PeerMeshRuntime {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        self.mesh.close_all_connections();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

impl Drop for PeerMeshRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.mesh.close_all_connections();
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl PeerMesh {
    pub fn new(
        cluster: Arc<ClusterMembership>,
        role: NodeRole,
        generation: Generation,
        applied_lsn: Lsn,
        applied_ts: Ts,
        wal_applier: Arc<dyn WalApplier>,
    ) -> Self {
        let config = cluster.config();
        Self {
            cluster,
            self_role: RwLock::new(role),
            generation,
            applied_lsn: AtomicU64::new(applied_lsn),
            applied_ts: AtomicU64::new(applied_ts),
            oldest_retained_lsn: AtomicU64::new(0),
            force_snapshot_on_primary_attach: AtomicBool::new(false),
            wal_applier,
            catchup_source: RwLock::new(Arc::new(NoWalCatchupSource)),
            promotion_handler: RwLock::new(Arc::new(NoPromotionHandler)),
            snapshot_sink: RwLock::new(Arc::new(NoSnapshotSink)),
            snapshot_source: RwLock::new(None),
            sender: Arc::new(PeerWalSender::new(config.primary_ack_timeout)),
            max_payload_len: DEFAULT_MAX_PEER_MESSAGE_SIZE,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_limits(
        cluster: Arc<ClusterMembership>,
        role: NodeRole,
        generation: Generation,
        applied_lsn: Lsn,
        applied_ts: Ts,
        wal_applier: Arc<dyn WalApplier>,
        max_payload_len: usize,
        ack_timeout: Duration,
    ) -> Self {
        Self {
            cluster,
            self_role: RwLock::new(role),
            generation,
            applied_lsn: AtomicU64::new(applied_lsn),
            applied_ts: AtomicU64::new(applied_ts),
            oldest_retained_lsn: AtomicU64::new(0),
            force_snapshot_on_primary_attach: AtomicBool::new(false),
            wal_applier,
            catchup_source: RwLock::new(Arc::new(NoWalCatchupSource)),
            promotion_handler: RwLock::new(Arc::new(NoPromotionHandler)),
            snapshot_sink: RwLock::new(Arc::new(NoSnapshotSink)),
            snapshot_source: RwLock::new(None),
            sender: Arc::new(PeerWalSender::new(ack_timeout)),
            max_payload_len,
        }
    }

    pub fn cluster(&self) -> &Arc<ClusterMembership> {
        &self.cluster
    }

    pub fn role(&self) -> NodeRole {
        *self.self_role.read()
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn applied_lsn(&self) -> Lsn {
        self.applied_lsn.load(Ordering::Acquire)
    }

    pub fn applied_ts(&self) -> Ts {
        self.applied_ts.load(Ordering::Acquire)
    }

    pub fn oldest_retained_lsn(&self) -> Lsn {
        self.catchup_source
            .read()
            .oldest_retained_lsn()
            .unwrap_or_else(|| self.oldest_retained_lsn.load(Ordering::Acquire))
    }

    pub fn set_oldest_retained_lsn(&self, oldest_retained_lsn: Lsn) {
        self.oldest_retained_lsn
            .store(oldest_retained_lsn, Ordering::Release);
    }

    pub fn set_force_snapshot_on_primary_attach(&self, force: bool) {
        self.force_snapshot_on_primary_attach
            .store(force, Ordering::Release);
    }

    pub fn set_role(&self, role: NodeRole) {
        *self.self_role.write() = role;
    }

    pub fn set_catchup_source(&self, catchup_source: Arc<dyn WalCatchupSource>) {
        *self.catchup_source.write() = catchup_source;
    }

    pub fn set_promotion_handler(&self, promotion_handler: Arc<dyn PromotionHandler>) {
        *self.promotion_handler.write() = promotion_handler;
    }

    pub fn set_snapshot_sink(&self, snapshot_sink: Arc<dyn SnapshotSink>) {
        *self.snapshot_sink.write() = snapshot_sink;
    }

    pub fn set_snapshot_source(&self, snapshot_source: Arc<dyn SnapshotSource>) {
        *self.snapshot_source.write() = Some(snapshot_source);
    }

    pub fn wal_sender(&self) -> Arc<PeerWalSender> {
        self.sender.clone()
    }

    pub fn connection(&self, node_id: NodeId) -> Option<PeerConnection> {
        self.sender.get(node_id)
    }

    pub fn connected_peers(&self) -> Vec<NodeId> {
        self.sender.remove_disconnected();
        self.sender.peer_ids()
    }

    pub fn primary_connection(&self) -> Option<PeerConnection> {
        self.sender.remove_disconnected();
        self.sender
            .connections()
            .into_iter()
            .find(|connection| connection.peer_role() == NodeRole::Primary)
    }

    pub fn set_local_progress(&self, applied_lsn: Lsn, applied_ts: Ts) {
        self.applied_lsn.fetch_max(applied_lsn, Ordering::AcqRel);
        self.applied_ts.fetch_max(applied_ts, Ordering::AcqRel);
    }

    /// Start the autonomous TCP lifecycle for this mesh.
    ///
    /// The mesh binds to its configured local topology address, accepts inbound
    /// peer streams, initiates outbound streams only when `self_id < peer_id`,
    /// retries initial outbound connection failures with backoff, and broadcasts
    /// heartbeats on the configured interval.
    pub async fn start_tcp(self: &Arc<Self>) -> Result<PeerMeshRuntime, ReplicationError> {
        let self_id = self.cluster.self_id();
        let addr = self
            .cluster
            .status(self_id)
            .ok_or(ReplicationError::MissingSelf(self_id))?
            .addr;
        let listener = TcpListener::bind(addr).await.map_err(peer_io)?;
        self.start_tcp_with_listener(listener).await
    }

    /// Start the TCP lifecycle using an already-bound listener.
    ///
    /// This is useful when callers need deterministic port selection before the
    /// cluster topology is built.
    pub async fn start_tcp_with_listener(
        self: &Arc<Self>,
        listener: TcpListener,
    ) -> Result<PeerMeshRuntime, ReplicationError> {
        let local_addr = listener.local_addr().map_err(peer_io)?;
        self.cluster
            .update_peer_addr(self.cluster.self_id(), local_addr)?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::new();
        tasks.push(spawn_mesh_accept_loop(
            self.clone(),
            listener,
            shutdown_rx.clone(),
        ));

        let peer_ids: Vec<_> = self
            .cluster
            .config()
            .topology
            .keys()
            .copied()
            .filter(|node_id| *node_id != self.cluster.self_id())
            .collect();
        for peer_id in peer_ids {
            if self.should_initiate_connection(peer_id)? {
                tasks.push(spawn_mesh_connect_loop(
                    self.clone(),
                    peer_id,
                    shutdown_rx.clone(),
                ));
            }
        }

        tasks.push(spawn_mesh_heartbeat_loop(self.clone(), shutdown_rx));

        Ok(PeerMeshRuntime {
            mesh: self.clone(),
            shutdown_tx,
            tasks,
            local_addr,
        })
    }

    pub fn should_initiate_connection(&self, peer_id: NodeId) -> Result<bool, ReplicationError> {
        let self_id = self.cluster.self_id();
        if peer_id == self_id {
            return Err(ReplicationError::PeerIsSelf(peer_id));
        }
        if self.cluster.status(peer_id).is_none() {
            return Err(ReplicationError::UnknownPeer(peer_id));
        }
        Ok(self_id < peer_id)
    }

    /// Attach an already established stream to this mesh.
    ///
    /// This method is used by both sides of the lower-node-initiated connection
    /// model: the lower node calls it after connecting, and the higher node calls
    /// it after accepting.
    pub async fn attach_stream<S>(&self, stream: S) -> Result<PeerConnection, ReplicationError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let catchup_source = { self.catchup_source.read().clone() };
        let promotion_handler = { self.promotion_handler.read().clone() };
        let snapshot_sink = { self.snapshot_sink.read().clone() };
        let snapshot_source = { self.snapshot_source.read().clone() };
        let connection = PeerConnection::connect_stream_with_catchup(
            stream,
            self.cluster.clone(),
            self.local_handshake(),
            self.wal_applier.clone(),
            catchup_source,
            promotion_handler,
            snapshot_sink,
            snapshot_source,
            self.max_payload_len,
        )
        .await?;
        self.sender.insert(connection.clone());
        self.request_catchup_if_replica_connected_to_primary(&connection)
            .await?;
        Ok(connection)
    }

    async fn request_catchup_if_replica_connected_to_primary(
        &self,
        connection: &PeerConnection,
    ) -> Result<(), ReplicationError> {
        if self.role() == NodeRole::Replica && connection.peer_role() == NodeRole::Primary {
            let from_lsn = self.applied_lsn.load(Ordering::Acquire);
            let oldest_retained_lsn = connection.peer_oldest_retained_lsn();
            if self
                .force_snapshot_on_primary_attach
                .load(Ordering::Acquire)
                || (oldest_retained_lsn > 0 && from_lsn < oldest_retained_lsn)
            {
                connection.request_snapshot(from_lsn).await?;
            } else {
                connection.request_catchup(from_lsn).await?;
            }
        }
        Ok(())
    }

    /// Connect to a peer over TCP, enforcing the lower-NodeId initiation rule.
    pub async fn connect_tcp(&self, peer_id: NodeId) -> Result<PeerConnection, ReplicationError> {
        if !self.should_initiate_connection(peer_id)? {
            return Err(ReplicationError::InitiationRuleViolation {
                self_id: self.cluster.self_id(),
                peer_id,
            });
        }
        let addr = self
            .cluster
            .status(peer_id)
            .ok_or(ReplicationError::UnknownPeer(peer_id))?
            .addr;
        let stream = TcpStream::connect(addr).await.map_err(peer_io)?;
        self.attach_stream(stream).await
    }

    pub async fn replicate_and_wait(
        &self,
        node_id: NodeId,
        lsn: Lsn,
        record: &[u8],
    ) -> Result<(), ReplicationError> {
        self.sender.send_wal_record(node_id, lsn, record).await
    }

    pub async fn broadcast_heartbeat(&self) -> Result<(), ReplicationError> {
        self.sender.remove_disconnected();
        let applied_lsn = self.applied_lsn.load(Ordering::Acquire);
        let applied_ts = self.applied_ts.load(Ordering::Acquire);
        let connections: Vec<_> = self
            .connected_peers()
            .into_iter()
            .filter_map(|node_id| self.connection(node_id))
            .collect();

        for connection in connections {
            connection.send_heartbeat(applied_lsn, applied_ts).await?;
        }
        Ok(())
    }

    pub fn close_all_connections(&self) {
        self.sender.close_all();
    }

    fn local_handshake(&self) -> PeerHandshake {
        PeerHandshake {
            node_id: self.cluster.self_id(),
            generation: self.generation,
            role: self.role(),
            applied_lsn: self.applied_lsn.load(Ordering::Acquire),
            applied_ts: self.applied_ts.load(Ordering::Acquire),
            oldest_retained_lsn: self.oldest_retained_lsn(),
        }
    }
}

fn spawn_mesh_accept_loop(
    mesh: Arc<PeerMesh>,
    listener: TcpListener,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let Ok((stream, _addr)) = accepted else {
                        continue;
                    };
                    let mesh = mesh.clone();
                    tokio::spawn(async move {
                        let _ = mesh.attach_stream(stream).await;
                    });
                }
            }
        }
    })
}

fn spawn_mesh_connect_loop(
    mesh: Arc<PeerMesh>,
    peer_id: NodeId,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(25);
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            if let Some(connection) = mesh.connection(peer_id) {
                if connection.is_connected() {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(mesh.cluster.config().heartbeat_interval) => {}
                    }
                    continue;
                }
                mesh.sender.remove_if_disconnected(peer_id);
            }

            match mesh.connect_tcp(peer_id).await {
                Ok(_) => {
                    backoff = Duration::from_millis(25);
                }
                Err(_) => {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_millis(500));
                }
            }
        }
    })
}

fn spawn_mesh_heartbeat_loop(
    mesh: Arc<PeerMesh>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = mesh.cluster.config().heartbeat_interval;
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = sleep(interval) => {
                    let _ = mesh.broadcast_heartbeat().await;
                    mesh.cluster.check_timeouts();
                }
            }
        }
    })
}

/// WAL sender backed by live peer connections.
pub struct PeerWalSender {
    peers: RwLock<HashMap<NodeId, PeerConnection>>,
    ack_timeout: Duration,
}

impl PeerWalSender {
    pub fn new(ack_timeout: Duration) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            ack_timeout,
        }
    }

    pub fn insert(&self, connection: PeerConnection) {
        self.peers.write().insert(connection.peer_id(), connection);
    }

    pub fn remove(&self, node_id: NodeId) -> Option<PeerConnection> {
        self.peers.write().remove(&node_id)
    }

    pub fn remove_if_disconnected(&self, node_id: NodeId) -> Option<PeerConnection> {
        let mut peers = self.peers.write();
        if peers
            .get(&node_id)
            .is_some_and(|connection| !connection.is_connected())
        {
            peers.remove(&node_id)
        } else {
            None
        }
    }

    pub fn remove_disconnected(&self) {
        self.peers
            .write()
            .retain(|_, connection| connection.is_connected());
    }

    pub fn close_all(&self) {
        let connections: Vec<_> = self.peers.write().drain().map(|(_, conn)| conn).collect();
        for connection in connections {
            connection.close();
        }
    }

    pub fn connections(&self) -> Vec<PeerConnection> {
        self.peers.read().values().cloned().collect()
    }

    pub fn get(&self, node_id: NodeId) -> Option<PeerConnection> {
        self.peers.read().get(&node_id).cloned()
    }

    pub fn peer_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<_> = self.peers.read().keys().copied().collect();
        ids.sort_unstable();
        ids
    }
}

#[async_trait]
impl WalSender for PeerWalSender {
    async fn send_wal_record(
        &self,
        node_id: NodeId,
        lsn: Lsn,
        record: &[u8],
    ) -> Result<(), ReplicationError> {
        let connection = self
            .get(node_id)
            .ok_or_else(|| ReplicationError::SenderFailed {
                node_id,
                message: "peer connection not found".to_string(),
            })?;
        connection
            .send_wal_record_and_wait(lsn, record, self.ack_timeout)
            .await
    }
}

/// Quorum-checked snapshot stream sender over the peer mesh.
pub struct SnapshotSender {
    mesh: Arc<PeerMesh>,
}

impl SnapshotSender {
    pub fn new(mesh: Arc<PeerMesh>) -> Self {
        Self { mesh }
    }

    pub async fn send_snapshot<I>(
        &self,
        target_node: NodeId,
        chunks: I,
    ) -> Result<(), ReplicationError>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        if !self.mesh.cluster().has_quorum() {
            return Err(ReplicationError::QuorumUnavailable);
        }
        let connection =
            self.mesh
                .connection(target_node)
                .ok_or_else(|| ReplicationError::SenderFailed {
                    node_id: target_node,
                    message: "peer connection not found".to_string(),
                })?;
        connection.send_snapshot(chunks).await
    }
}

/// Client used by replicas to forward write transactions to the primary.
pub struct PromotionClient {
    mesh: Arc<PeerMesh>,
    timeout: Duration,
}

impl PromotionClient {
    pub fn new(mesh: Arc<PeerMesh>) -> Self {
        let timeout = mesh.cluster().config().primary_ack_timeout;
        Self { mesh, timeout }
    }

    pub fn with_timeout(mesh: Arc<PeerMesh>, timeout: Duration) -> Self {
        Self { mesh, timeout }
    }

    pub async fn promote(
        &self,
        begin_ts: Ts,
        subscription: SubscriptionMode,
        payload: &[u8],
    ) -> Result<PromotionOutcome, ReplicationError> {
        let role = self.mesh.role();
        if role != NodeRole::Replica {
            return Err(ReplicationError::PromotionRoleMismatch { role });
        }
        if !self.mesh.cluster().has_quorum() {
            return Err(ReplicationError::QuorumUnavailable);
        }
        let primary = self
            .mesh
            .primary_connection()
            .ok_or(ReplicationError::NoPrimaryConnection)?;
        primary
            .promote_and_wait(begin_ts, subscription, payload, self.timeout)
            .await
    }
}

/// Abstract primary-to-replica WAL sender.
///
/// A future peer mesh implementation will send `WalRecord` frames and complete
/// only after a durable `WalAck`. Tests and embedders can inject deterministic
/// senders today.
#[async_trait]
pub trait WalSender: Send + Sync {
    async fn send_wal_record(
        &self,
        node_id: NodeId,
        lsn: Lsn,
        record: &[u8],
    ) -> Result<(), ReplicationError>;
}

/// Primary-side replication hook.
pub struct PrimaryReplicator {
    cluster: Arc<ClusterMembership>,
    sender: Arc<dyn WalSender>,
    hold_state: AtomicBool,
}

impl PrimaryReplicator {
    pub fn new(cluster: Arc<ClusterMembership>, sender: Arc<dyn WalSender>) -> Self {
        Self {
            cluster,
            sender,
            hold_state: AtomicBool::new(false),
        }
    }

    pub fn from_mesh(mesh: Arc<PeerMesh>) -> Self {
        Self::new(mesh.cluster().clone(), mesh.wal_sender())
    }

    pub fn cluster(&self) -> &Arc<ClusterMembership> {
        &self.cluster
    }

    pub fn enter_hold_state(&self) {
        self.hold_state.store(true, Ordering::Release);
    }

    pub fn clear_hold_state(&self) {
        self.hold_state.store(false, Ordering::Release);
    }

    pub fn min_replica_lsn(&self) -> Option<Lsn> {
        self.cluster.min_replica_lsn()
    }

    async fn replicate_inner(&self, lsn: Lsn, record: &[u8]) -> Result<(), ReplicationError> {
        if self.is_holding() {
            return Err(ReplicationError::Holding);
        }
        if !self.cluster.has_quorum() {
            self.enter_hold_state();
            return Err(ReplicationError::QuorumUnavailable);
        }

        let replicas = self.cluster.online_replicas();
        let mut acked_with_self = 1usize;
        for node_id in replicas {
            match self.sender.send_wal_record(node_id, lsn, record).await {
                Ok(()) => {
                    self.cluster.record_wal_ack(node_id, lsn);
                    acked_with_self += 1;
                }
                Err(err) => {
                    tracing_sender_error(node_id, &err);
                }
            }
        }

        let cluster_size = self.cluster.cluster_size();
        if has_majority(acked_with_self, cluster_size) {
            Ok(())
        } else {
            self.enter_hold_state();
            Err(ReplicationError::AckQuorumFailed {
                acked_with_self,
                cluster_size,
            })
        }
    }
}

#[async_trait]
impl ReplicationHook for PrimaryReplicator {
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<(), String> {
        self.replicate_inner(lsn, record)
            .await
            .map_err(ReplicationError::into_hook_error)
    }

    fn replication_retention_lsn(&self) -> Option<Lsn> {
        self.min_replica_lsn()
    }

    fn has_quorum(&self) -> bool {
        self.cluster.has_quorum()
    }

    fn is_holding(&self) -> bool {
        self.hold_state.load(Ordering::Acquire)
    }
}

fn has_majority(count: usize, cluster_size: usize) -> bool {
    cluster_size > 0 && count > cluster_size / 2
}

fn tracing_sender_error(_node_id: NodeId, _err: &ReplicationError) {
    // Keep this helper separate so a future tracing dependency decision is
    // localized. The primary hook currently treats individual send failures as
    // best-effort until quorum is evaluated.
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::StorageConfig;
    use exdb_storage::recovery::NoOpHandler;
    use exdb_storage::wal::WAL_RECORD_VISIBLE_TS;
    use parking_lot::Mutex;
    use std::net::TcpListener as StdTcpListener;
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::time::sleep;

    #[derive(Default)]
    struct RecordingSender {
        failures: Mutex<Vec<NodeId>>,
        sent: Mutex<Vec<(NodeId, Lsn, Vec<u8>)>>,
    }

    #[async_trait]
    impl WalSender for RecordingSender {
        async fn send_wal_record(
            &self,
            node_id: NodeId,
            lsn: Lsn,
            record: &[u8],
        ) -> Result<(), ReplicationError> {
            if self.failures.lock().contains(&node_id) {
                return Err(ReplicationError::SenderFailed {
                    node_id,
                    message: "injected failure".to_string(),
                });
            }
            self.sent.lock().push((node_id, lsn, record.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingApplier {
        applied: Mutex<Vec<(NodeId, Lsn, Vec<u8>)>>,
    }

    #[async_trait]
    impl WalApplier for RecordingApplier {
        async fn apply_wal_record(
            &self,
            peer_id: NodeId,
            lsn: Lsn,
            record: &[u8],
        ) -> Result<(Lsn, Ts), ReplicationError> {
            self.applied.lock().push((peer_id, lsn, record.to_vec()));
            Ok((lsn, lsn + 1000))
        }
    }

    struct DelayedApplier {
        delay: Duration,
    }

    #[async_trait]
    impl WalApplier for DelayedApplier {
        async fn apply_wal_record(
            &self,
            _peer_id: NodeId,
            lsn: Lsn,
            _record: &[u8],
        ) -> Result<(Lsn, Ts), ReplicationError> {
            sleep(self.delay).await;
            Ok((lsn, lsn + 1000))
        }
    }

    struct RecordingCatchupSource {
        requested: Mutex<Vec<Lsn>>,
        records: Vec<(Lsn, Vec<u8>)>,
    }

    #[async_trait]
    impl WalCatchupSource for RecordingCatchupSource {
        async fn records_from(
            &self,
            from_lsn: Lsn,
        ) -> Result<Vec<(Lsn, Vec<u8>)>, ReplicationError> {
            self.requested.lock().push(from_lsn);
            Ok(self
                .records
                .iter()
                .filter(|(lsn, _)| *lsn >= from_lsn)
                .cloned()
                .collect())
        }
    }

    struct UnavailableCatchupSource {
        oldest_retained_lsn: Lsn,
    }

    #[async_trait]
    impl WalCatchupSource for UnavailableCatchupSource {
        async fn records_from(
            &self,
            from_lsn: Lsn,
        ) -> Result<Vec<(Lsn, Vec<u8>)>, ReplicationError> {
            Err(ReplicationError::CatchupUnavailable {
                requested_lsn: from_lsn,
                oldest_retained_lsn: self.oldest_retained_lsn,
            })
        }
    }

    type RecordedPromotion = (NodeId, Ts, SubscriptionMode, Vec<u8>);

    struct RecordingPromotionHandler {
        seen: Mutex<Vec<RecordedPromotion>>,
        outcome: PromotionOutcome,
    }

    #[async_trait]
    impl PromotionHandler for RecordingPromotionHandler {
        async fn handle_promotion(
            &self,
            peer_id: NodeId,
            begin_ts: Ts,
            subscription: SubscriptionMode,
            payload: &[u8],
        ) -> Result<PromotionOutcome, ReplicationError> {
            self.seen
                .lock()
                .push((peer_id, begin_ts, subscription, payload.to_vec()));
            Ok(self.outcome.clone())
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum SnapshotEvent {
        Begin(NodeId),
        Chunk(NodeId, Vec<u8>),
        End(NodeId),
    }

    #[derive(Default)]
    struct RecordingSnapshotSink {
        events: Mutex<Vec<SnapshotEvent>>,
    }

    #[async_trait]
    impl SnapshotSink for RecordingSnapshotSink {
        async fn begin_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
            self.events.lock().push(SnapshotEvent::Begin(peer_id));
            Ok(())
        }

        async fn apply_snapshot_chunk(
            &self,
            peer_id: NodeId,
            chunk: &[u8],
        ) -> Result<(), ReplicationError> {
            self.events
                .lock()
                .push(SnapshotEvent::Chunk(peer_id, chunk.to_vec()));
            Ok(())
        }

        async fn end_snapshot(&self, peer_id: NodeId) -> Result<(), ReplicationError> {
            self.events.lock().push(SnapshotEvent::End(peer_id));
            Ok(())
        }
    }

    struct RecordingSnapshotSource {
        requested: Mutex<Vec<NodeId>>,
        chunks: Vec<Vec<u8>>,
    }

    #[async_trait]
    impl SnapshotSource for RecordingSnapshotSource {
        async fn snapshot_chunks(&self, peer_id: NodeId) -> Result<Vec<Vec<u8>>, ReplicationError> {
            self.requested.lock().push(peer_id);
            Ok(self.chunks.clone())
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn cluster(ids: &[NodeId]) -> Arc<ClusterMembership> {
        cluster_for(1, ids)
    }

    fn cluster_for(self_id: NodeId, ids: &[NodeId]) -> Arc<ClusterMembership> {
        let topology = ids
            .iter()
            .map(|id| (*id, addr(5000 + *id as u16)))
            .collect();
        Arc::new(ClusterMembership::new(ClusterConfig::new(self_id, topology)).unwrap())
    }

    fn cluster_with_topology(
        self_id: NodeId,
        topology: HashMap<NodeId, SocketAddr>,
    ) -> Arc<ClusterMembership> {
        let mut config = ClusterConfig::new(self_id, topology);
        config.heartbeat_interval = Duration::from_millis(20);
        config.suspect_timeout = Duration::from_millis(200);
        config.down_timeout = Duration::from_millis(500);
        config.primary_ack_timeout = Duration::from_secs(1);
        Arc::new(ClusterMembership::new(config).unwrap())
    }

    fn bound_tokio_listener() -> (TcpListener, SocketAddr) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        (TcpListener::from_std(listener).unwrap(), addr)
    }

    fn handshake(
        node_id: NodeId,
        generation: Generation,
        role: NodeRole,
        applied_lsn: Lsn,
        applied_ts: Ts,
    ) -> PeerHandshake {
        PeerHandshake {
            node_id,
            generation,
            role,
            applied_lsn,
            applied_ts,
            oldest_retained_lsn: 0,
        }
    }

    async fn eventually(mut condition: impl FnMut() -> bool) {
        for _ in 0..50 {
            if condition() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(condition());
    }

    async fn connect_peer_pair(
        primary_cluster: Arc<ClusterMembership>,
        replica_cluster: Arc<ClusterMembership>,
        primary_applier: Arc<dyn WalApplier>,
        replica_applier: Arc<dyn WalApplier>,
    ) -> (PeerConnection, PeerConnection) {
        let (primary_stream, replica_stream) = duplex(4096);
        let primary = PeerConnection::connect_stream(
            primary_stream,
            primary_cluster,
            handshake(1, 10, NodeRole::Primary, 7, 70),
            primary_applier,
            DEFAULT_MAX_PEER_MESSAGE_SIZE,
        );
        let replica = PeerConnection::connect_stream(
            replica_stream,
            replica_cluster,
            handshake(2, 20, NodeRole::Replica, 5, 50),
            replica_applier,
            DEFAULT_MAX_PEER_MESSAGE_SIZE,
        );

        tokio::try_join!(primary, replica).unwrap()
    }

    async fn connect_peer_pair_with_primary_catchup(
        primary_cluster: Arc<ClusterMembership>,
        replica_cluster: Arc<ClusterMembership>,
        primary_applier: Arc<dyn WalApplier>,
        replica_applier: Arc<dyn WalApplier>,
        catchup_source: Arc<dyn WalCatchupSource>,
    ) -> (PeerConnection, PeerConnection) {
        let (primary_stream, replica_stream) = duplex(8192);
        let primary = PeerConnection::connect_stream_with_catchup(
            primary_stream,
            primary_cluster,
            handshake(1, 10, NodeRole::Primary, 7, 70),
            primary_applier,
            catchup_source,
            Arc::new(NoPromotionHandler),
            Arc::new(NoSnapshotSink),
            None,
            DEFAULT_MAX_PEER_MESSAGE_SIZE,
        );
        let replica = PeerConnection::connect_stream(
            replica_stream,
            replica_cluster,
            handshake(2, 20, NodeRole::Replica, 5, 50),
            replica_applier,
            DEFAULT_MAX_PEER_MESSAGE_SIZE,
        );

        tokio::try_join!(primary, replica).unwrap()
    }

    fn mesh_for(
        cluster: Arc<ClusterMembership>,
        role: NodeRole,
        generation: Generation,
        applied_lsn: Lsn,
        applied_ts: Ts,
        applier: Arc<dyn WalApplier>,
    ) -> Arc<PeerMesh> {
        Arc::new(PeerMesh::with_limits(
            cluster,
            role,
            generation,
            applied_lsn,
            applied_ts,
            applier,
            DEFAULT_MAX_PEER_MESSAGE_SIZE,
            Duration::from_secs(1),
        ))
    }

    async fn attach_mesh_pair(
        primary_mesh: Arc<PeerMesh>,
        replica_mesh: Arc<PeerMesh>,
    ) -> (PeerConnection, PeerConnection) {
        let (primary_stream, replica_stream) = duplex(4096);
        let primary = primary_mesh.attach_stream(primary_stream);
        let replica = replica_mesh.attach_stream(replica_stream);
        tokio::try_join!(primary, replica).unwrap()
    }

    #[test]
    fn recovery_tier_selects_incremental_when_retained_wal_covers_progress() {
        let local = LocalState {
            data_file_exists: true,
            data_file_valid: true,
            wal_intact: true,
            applied_lsn: Some(120),
        };

        assert_eq!(
            select_recovery_tier(&local, 100, 7, Some(7)),
            RecoveryTier::IncrementalCatchup { from_lsn: 120 }
        );
    }

    #[test]
    fn recovery_tier_selects_local_recovery_before_catchup_for_same_generation_wal_repair() {
        let local = LocalState {
            data_file_exists: true,
            data_file_valid: true,
            wal_intact: false,
            applied_lsn: Some(120),
        };

        assert_eq!(
            select_recovery_tier(&local, 100, 7, Some(7)),
            RecoveryTier::LocalRecoveryThenCatchup
        );
    }

    #[test]
    fn recovery_tier_selects_full_reconstruction_when_required_wal_was_reclaimed() {
        let local = LocalState {
            data_file_exists: true,
            data_file_valid: true,
            wal_intact: true,
            applied_lsn: Some(80),
        };

        assert_eq!(
            select_recovery_tier(&local, 100, 7, Some(7)),
            RecoveryTier::FullReconstruction
        );
    }

    #[test]
    fn recovery_tier_bootstraps_fresh_node_from_wal_only_when_full_history_is_retained() {
        let local = LocalState {
            data_file_exists: false,
            data_file_valid: false,
            wal_intact: false,
            applied_lsn: None,
        };

        assert_eq!(
            select_recovery_tier(&local, 0, 1, None),
            RecoveryTier::IncrementalCatchup { from_lsn: 0 }
        );
        assert_eq!(
            select_recovery_tier(&local, 40, 1, None),
            RecoveryTier::FullReconstruction
        );
    }

    #[test]
    fn recovery_tier_treats_generation_change_as_replacement_state() {
        let local = LocalState {
            data_file_exists: true,
            data_file_valid: true,
            wal_intact: true,
            applied_lsn: Some(120),
        };

        assert_eq!(
            select_recovery_tier(&local, 0, 8, Some(7)),
            RecoveryTier::FullReconstruction
        );
        assert_eq!(
            select_recovery_tier(
                &LocalState {
                    applied_lsn: None,
                    ..local
                },
                0,
                8,
                Some(7),
            ),
            RecoveryTier::IncrementalCatchup { from_lsn: 0 }
        );
    }

    #[test]
    fn peer_messages_round_trip_payload_codec() {
        let messages = vec![
            PeerMessage::Handshake {
                node_id: 7,
                generation: 3,
                role: NodeRole::Primary,
                applied_lsn: 99,
                applied_ts: 101,
                oldest_retained_lsn: 80,
            },
            PeerMessage::Handshake {
                node_id: 8,
                generation: 4,
                role: NodeRole::Replica,
                applied_lsn: 100,
                applied_ts: 102,
                oldest_retained_lsn: 0,
            },
            PeerMessage::Heartbeat {
                node_id: 7,
                applied_lsn: 42,
                applied_ts: 43,
            },
            PeerMessage::WalRecord {
                lsn: 44,
                data: b"wal bytes".to_vec(),
            },
            PeerMessage::WalAck { lsn: 44 },
            PeerMessage::RequestCatchup { from_lsn: 12 },
            PeerMessage::RequestSnapshot { from_lsn: 13 },
            PeerMessage::CatchupUnavailable {
                requested_lsn: 12,
                oldest_retained_lsn: 40,
            },
            PeerMessage::PromotionRequest {
                request_id: 77,
                begin_ts: 123,
                subscription: SubscriptionMode::Subscribe,
                payload: b"encoded write set".to_vec(),
            },
            PeerMessage::PromotionResponse {
                request_id: 77,
                outcome: PromotionOutcome::Success { commit_ts: 130 },
            },
            PeerMessage::PromotionResponse {
                request_id: 79,
                outcome: PromotionOutcome::Response {
                    payload: b"opaque response".to_vec(),
                },
            },
            PeerMessage::PromotionResponse {
                request_id: 78,
                outcome: PromotionOutcome::Conflict {
                    error: b"conflict payload".to_vec(),
                    retry: Some(PromotionRetry {
                        new_tx: 900,
                        new_ts: 901,
                    }),
                },
            },
            PeerMessage::SnapshotBegin,
            PeerMessage::SnapshotData {
                chunk: b"snapshot chunk".to_vec(),
            },
            PeerMessage::SnapshotEnd,
        ];

        for message in messages {
            let encoded = encode_peer_message(&message).unwrap();
            assert_eq!(decode_peer_message(&encoded).unwrap(), message);
        }
    }

    #[test]
    fn peer_message_decoder_rejects_malformed_payloads() {
        assert!(matches!(
            decode_peer_message(&[0xff]).unwrap_err(),
            ReplicationError::InvalidPeerMessageTag(0xff)
        ));
        assert!(matches!(
            decode_peer_message(&[2, 0, 0]).unwrap_err(),
            ReplicationError::TruncatedPeerMessage
        ));
        assert!(matches!(
            decode_peer_message(&[6, 0]).unwrap_err(),
            ReplicationError::TrailingPeerMessageBytes { remaining: 1 }
        ));

        let mut invalid_role = vec![1];
        put_u64(&mut invalid_role, 1);
        put_u64(&mut invalid_role, 1);
        invalid_role.push(9);
        put_u64(&mut invalid_role, 1);
        put_u64(&mut invalid_role, 1);
        assert!(matches!(
            decode_peer_message(&invalid_role).unwrap_err(),
            ReplicationError::InvalidNodeRole(9)
        ));
    }

    #[tokio::test]
    async fn peer_frame_read_write_round_trips_over_async_io() {
        let (mut writer, mut reader) = duplex(1024);
        let message = PeerMessage::WalRecord {
            lsn: 77,
            data: b"durable frame".to_vec(),
        };
        let write_message = message.clone();

        let write_task = tokio::spawn(async move {
            write_peer_message_with_limit(
                &mut writer,
                &write_message,
                DEFAULT_MAX_PEER_MESSAGE_SIZE,
            )
            .await
            .unwrap();
        });

        let decoded = read_peer_message_with_limit(&mut reader, DEFAULT_MAX_PEER_MESSAGE_SIZE)
            .await
            .unwrap();
        write_task.await.unwrap();

        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn peer_frame_enforces_max_payload_size_on_write_and_read() {
        let (mut writer, _reader) = duplex(1024);
        let message = PeerMessage::SnapshotData { chunk: vec![1; 16] };
        let err = write_peer_message_with_limit(&mut writer, &message, 20)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ReplicationError::PeerMessageTooLarge { len: 21, max: 20 }
        ));

        let (mut raw_writer, mut raw_reader) = duplex(16);
        raw_writer.write_all(&(10u32.to_be_bytes())).await.unwrap();
        let err = read_peer_message_with_limit(&mut raw_reader, 9)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ReplicationError::PeerMessageTooLarge { len: 10, max: 9 }
        ));
    }

    #[tokio::test]
    async fn peer_connection_handshakes_and_tracks_heartbeats() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let (primary_conn, replica_conn) = connect_peer_pair(
            primary_cluster.clone(),
            replica_cluster.clone(),
            Arc::new(RecordingApplier::default()),
            Arc::new(RecordingApplier::default()),
        )
        .await;

        assert_eq!(primary_conn.peer_id(), 2);
        assert_eq!(primary_conn.peer_generation(), 20);
        assert_eq!(primary_conn.peer_role(), NodeRole::Replica);
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 5);
        assert_eq!(replica_cluster.status(1).unwrap().applied_lsn, 7);

        replica_conn.send_heartbeat(11, 110).await.unwrap();
        eventually(|| primary_cluster.status(2).unwrap().applied_lsn == 11).await;
        assert_eq!(primary_cluster.status(2).unwrap().applied_ts, 110);
    }

    #[tokio::test]
    async fn peer_connection_sends_wal_and_waits_for_durable_ack() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let replica_applier = Arc::new(RecordingApplier::default());
        let (primary_conn, _replica_conn) = connect_peer_pair(
            primary_cluster.clone(),
            replica_cluster,
            Arc::new(RecordingApplier::default()),
            replica_applier.clone(),
        )
        .await;

        primary_conn
            .send_wal_record_and_wait(55, b"wal frame", Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 55, b"wal frame".to_vec())]
        );
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 55);
    }

    #[tokio::test]
    async fn request_catchup_streams_retained_wal_records() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let catchup_source = Arc::new(RecordingCatchupSource {
            requested: Mutex::new(Vec::new()),
            records: vec![
                (9, b"too-old".to_vec()),
                (12, b"first".to_vec()),
                (22, b"second".to_vec()),
            ],
        });
        let replica_applier = Arc::new(RecordingApplier::default());
        let (_primary_conn, replica_conn) = connect_peer_pair_with_primary_catchup(
            primary_cluster.clone(),
            replica_cluster,
            Arc::new(RecordingApplier::default()),
            replica_applier.clone(),
            catchup_source.clone(),
        )
        .await;

        replica_conn.request_catchup(10).await.unwrap();
        eventually(|| replica_applier.applied.lock().len() == 2).await;
        eventually(|| primary_cluster.status(2).unwrap().applied_lsn == 22).await;

        assert_eq!(catchup_source.requested.lock().as_slice(), &[10]);
        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 12, b"first".to_vec()), (1, 22, b"second".to_vec())]
        );
    }

    #[tokio::test]
    async fn replica_mesh_requests_catchup_from_primary_on_attach() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let catchup_source = Arc::new(RecordingCatchupSource {
            requested: Mutex::new(Vec::new()),
            records: vec![
                (12, b"first".to_vec()),
                (22, b"second".to_vec()),
                (40, b"later".to_vec()),
            ],
        });
        let replica_applier = Arc::new(RecordingApplier::default());
        let primary_mesh = mesh_for(
            primary_cluster.clone(),
            NodeRole::Primary,
            10,
            100,
            1000,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_oldest_retained_lsn(12);
        primary_mesh.set_catchup_source(catchup_source.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            22,
            220,
            replica_applier.clone(),
        );

        let (_primary_conn, replica_conn) = attach_mesh_pair(primary_mesh, replica_mesh).await;
        assert_eq!(replica_conn.peer_oldest_retained_lsn(), 12);
        eventually(|| replica_applier.applied.lock().len() == 2).await;
        eventually(|| primary_cluster.status(2).unwrap().applied_lsn == 40).await;

        assert_eq!(catchup_source.requested.lock().as_slice(), &[22]);
        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 22, b"second".to_vec()), (1, 40, b"later".to_vec())]
        );
    }

    #[tokio::test]
    async fn replica_mesh_requests_snapshot_immediately_when_handshake_retention_has_gap() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let catchup_source = Arc::new(RecordingCatchupSource {
            requested: Mutex::new(Vec::new()),
            records: vec![(88, b"not requested".to_vec())],
        });
        let snapshot_source = Arc::new(RecordingSnapshotSource {
            requested: Mutex::new(Vec::new()),
            chunks: vec![b"snapshot header".to_vec(), b"snapshot pages".to_vec()],
        });
        let replica_sink = Arc::new(RecordingSnapshotSink::default());
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            200,
            2000,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_oldest_retained_lsn(88);
        primary_mesh.set_catchup_source(catchup_source.clone());
        primary_mesh.set_snapshot_source(snapshot_source.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            44,
            440,
            Arc::new(RecordingApplier::default()),
        );
        replica_mesh.set_snapshot_sink(replica_sink.clone());

        let (primary_conn, replica_conn) = attach_mesh_pair(primary_mesh, replica_mesh).await;
        eventually(|| replica_sink.events.lock().len() == 4).await;

        assert!(primary_conn.is_connected());
        assert!(replica_conn.is_connected());
        assert_eq!(replica_conn.peer_oldest_retained_lsn(), 88);
        assert!(
            catchup_source.requested.lock().is_empty(),
            "replica should not request retained WAL that the primary already advertised as reclaimed"
        );
        assert_eq!(snapshot_source.requested.lock().as_slice(), &[2]);
        assert_eq!(
            replica_sink.events.lock().as_slice(),
            &[
                SnapshotEvent::Begin(1),
                SnapshotEvent::Chunk(1, b"snapshot header".to_vec()),
                SnapshotEvent::Chunk(1, b"snapshot pages".to_vec()),
                SnapshotEvent::End(1),
            ]
        );
    }

    #[tokio::test]
    async fn replica_mesh_can_force_snapshot_on_primary_attach() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let catchup_source = Arc::new(RecordingCatchupSource {
            requested: Mutex::new(Vec::new()),
            records: vec![(0, b"full history retained".to_vec())],
        });
        let snapshot_source = Arc::new(RecordingSnapshotSource {
            requested: Mutex::new(Vec::new()),
            chunks: vec![b"snapshot header".to_vec()],
        });
        let replica_sink = Arc::new(RecordingSnapshotSink::default());
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            200,
            2000,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_oldest_retained_lsn(0);
        primary_mesh.set_catchup_source(catchup_source.clone());
        primary_mesh.set_snapshot_source(snapshot_source.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        replica_mesh.set_snapshot_sink(replica_sink.clone());
        replica_mesh.set_force_snapshot_on_primary_attach(true);

        let (_primary_conn, replica_conn) = attach_mesh_pair(primary_mesh, replica_mesh).await;
        eventually(|| replica_sink.events.lock().len() == 3).await;

        assert!(replica_conn.is_connected());
        assert!(
            catchup_source.requested.lock().is_empty(),
            "forced reconstruction should not attempt retained-WAL catch-up first"
        );
        assert_eq!(snapshot_source.requested.lock().as_slice(), &[2]);
        assert_eq!(
            replica_sink.events.lock().as_slice(),
            &[
                SnapshotEvent::Begin(1),
                SnapshotEvent::Chunk(1, b"snapshot header".to_vec()),
                SnapshotEvent::End(1),
            ]
        );
    }

    #[tokio::test]
    async fn replica_mesh_falls_back_to_snapshot_when_catchup_wal_has_gap() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let snapshot_source = Arc::new(RecordingSnapshotSource {
            requested: Mutex::new(Vec::new()),
            chunks: vec![b"snapshot header".to_vec(), b"snapshot pages".to_vec()],
        });
        let replica_sink = Arc::new(RecordingSnapshotSink::default());
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            200,
            2000,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_catchup_source(Arc::new(UnavailableCatchupSource {
            oldest_retained_lsn: 88,
        }));
        primary_mesh.set_snapshot_source(snapshot_source.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            44,
            440,
            Arc::new(RecordingApplier::default()),
        );
        replica_mesh.set_snapshot_sink(replica_sink.clone());

        let (primary_conn, replica_conn) = attach_mesh_pair(primary_mesh, replica_mesh).await;
        eventually(|| replica_sink.events.lock().len() == 4).await;

        assert!(primary_conn.is_connected());
        assert!(replica_conn.is_connected());
        assert_eq!(snapshot_source.requested.lock().as_slice(), &[2]);
        assert_eq!(
            replica_sink.events.lock().as_slice(),
            &[
                SnapshotEvent::Begin(1),
                SnapshotEvent::Chunk(1, b"snapshot header".to_vec()),
                SnapshotEvent::Chunk(1, b"snapshot pages".to_vec()),
                SnapshotEvent::End(1),
            ]
        );
    }

    #[tokio::test]
    async fn promotion_client_forwards_replica_write_payload_to_primary_over_mesh() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let handler = Arc::new(RecordingPromotionHandler {
            seen: Mutex::new(Vec::new()),
            outcome: PromotionOutcome::Success { commit_ts: 55 },
        });
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_promotion_handler(handler.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        attach_mesh_pair(primary_mesh, replica_mesh.clone()).await;

        let client = PromotionClient::with_timeout(replica_mesh, Duration::from_secs(1));
        let outcome = client
            .promote(42, SubscriptionMode::Subscribe, b"opaque read-write-set")
            .await
            .unwrap();

        assert_eq!(outcome, PromotionOutcome::Success { commit_ts: 55 });
        assert_eq!(
            handler.seen.lock().as_slice(),
            &[(
                2,
                42,
                SubscriptionMode::Subscribe,
                b"opaque read-write-set".to_vec()
            )]
        );
    }

    #[tokio::test]
    async fn promotion_client_rejects_when_replica_lacks_quorum() {
        let replica_cluster = cluster_for(2, &[1, 2]);
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let client = PromotionClient::with_timeout(replica_mesh, Duration::from_millis(10));

        assert_eq!(
            client
                .promote(42, SubscriptionMode::None, b"payload")
                .await
                .unwrap_err(),
            ReplicationError::QuorumUnavailable
        );
    }

    #[tokio::test]
    async fn promotion_client_rejects_after_local_role_transition_to_primary() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let handler = Arc::new(RecordingPromotionHandler {
            seen: Mutex::new(Vec::new()),
            outcome: PromotionOutcome::Success { commit_ts: 55 },
        });
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        primary_mesh.set_promotion_handler(handler.clone());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        attach_mesh_pair(primary_mesh, replica_mesh.clone()).await;

        let client = PromotionClient::with_timeout(replica_mesh.clone(), Duration::from_secs(1));
        assert_eq!(
            client
                .promote(42, SubscriptionMode::None, b"before transition")
                .await
                .unwrap(),
            PromotionOutcome::Success { commit_ts: 55 }
        );

        replica_mesh.set_role(NodeRole::Primary);
        assert_eq!(
            client
                .promote(43, SubscriptionMode::None, b"after transition")
                .await
                .unwrap_err(),
            ReplicationError::PromotionRoleMismatch {
                role: NodeRole::Primary
            }
        );
        assert_eq!(handler.seen.lock().len(), 1);
    }

    #[tokio::test]
    async fn promotion_client_rejects_when_no_primary_connection_is_available() {
        let replica_cluster = cluster_for(2, &[1, 2, 3]);
        replica_cluster.record_heartbeat(3, 0, 0);
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let client = PromotionClient::with_timeout(replica_mesh, Duration::from_millis(10));

        assert_eq!(
            client
                .promote(42, SubscriptionMode::None, b"payload")
                .await
                .unwrap_err(),
            ReplicationError::NoPrimaryConnection
        );
    }

    #[tokio::test]
    async fn snapshot_sender_streams_chunks_over_peer_mesh() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let replica_sink = Arc::new(RecordingSnapshotSink::default());
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        replica_mesh.set_snapshot_sink(replica_sink.clone());
        attach_mesh_pair(primary_mesh.clone(), replica_mesh).await;

        SnapshotSender::new(primary_mesh)
            .send_snapshot(
                2,
                vec![b"page image one".to_vec(), b"page image two".to_vec()],
            )
            .await
            .unwrap();

        eventually(|| replica_sink.events.lock().len() == 4).await;
        assert_eq!(
            replica_sink.events.lock().as_slice(),
            &[
                SnapshotEvent::Begin(1),
                SnapshotEvent::Chunk(1, b"page image one".to_vec()),
                SnapshotEvent::Chunk(1, b"page image two".to_vec()),
                SnapshotEvent::End(1),
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_sender_rejects_when_cluster_lacks_quorum() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );

        assert_eq!(
            SnapshotSender::new(primary_mesh)
                .send_snapshot(2, vec![b"stale snapshot".to_vec()])
                .await
                .unwrap_err(),
            ReplicationError::QuorumUnavailable
        );
    }

    #[tokio::test]
    async fn storage_wal_catchup_source_streams_only_tx_commit_records() {
        let storage = Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        );
        let first_lsn = storage
            .append_wal(WAL_RECORD_TX_COMMIT, b"first")
            .await
            .unwrap();
        storage
            .append_wal(WAL_RECORD_VISIBLE_TS, &123u64.to_le_bytes())
            .await
            .unwrap();
        let second_lsn = storage
            .append_wal(WAL_RECORD_TX_COMMIT, b"second")
            .await
            .unwrap();

        let source = StorageWalCatchupSource::new(storage);
        assert_eq!(
            source.records_from(0).await.unwrap(),
            vec![
                (first_lsn, b"first".to_vec()),
                (second_lsn, b"second".to_vec())
            ]
        );
        assert_eq!(
            source.records_from(second_lsn).await.unwrap(),
            vec![(second_lsn, b"second".to_vec())]
        );
    }

    #[tokio::test]
    async fn storage_wal_catchup_source_reports_gap_when_requested_lsn_was_reclaimed() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut handler = NoOpHandler;
        let storage = Arc::new(
            StorageEngine::open(
                tmp.path(),
                StorageConfig {
                    page_size: 4096,
                    memory_budget: 4096 * 64,
                    wal_segment_size: 256,
                    wal_retention_max_size: Some(512),
                    ..Default::default()
                },
                &mut handler,
            )
            .await
            .unwrap(),
        );

        let first_lsn = storage
            .append_wal(WAL_RECORD_TX_COMMIT, b"first")
            .await
            .unwrap();
        let payload = vec![0xAB; 96];
        for _ in 0..16 {
            storage
                .append_wal(WAL_RECORD_TX_COMMIT, &payload)
                .await
                .unwrap();
        }
        storage.set_replication_retention_lsn(Some(first_lsn));
        storage.checkpoint().await.unwrap();
        let oldest_retained_lsn = storage.oldest_retained_wal_lsn().unwrap();
        assert!(
            oldest_retained_lsn > first_lsn,
            "test setup should reclaim the first WAL segment"
        );

        let source = StorageWalCatchupSource::new(Arc::clone(&storage));
        assert_eq!(
            source.records_from(first_lsn).await.unwrap_err(),
            ReplicationError::CatchupUnavailable {
                requested_lsn: first_lsn,
                oldest_retained_lsn,
            }
        );

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn request_catchup_sends_catchup_unavailable_when_retained_wal_has_gap() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let (primary_conn, _replica_conn) = connect_peer_pair(
            primary_cluster.clone(),
            replica_cluster,
            Arc::new(RecordingApplier::default()),
            Arc::new(RecordingApplier::default()),
        )
        .await;
        let (outbound, mut outbound_rx) = mpsc::channel(4);
        let source = UnavailableCatchupSource {
            oldest_retained_lsn: 88,
        };
        let applier = RecordingApplier::default();

        handle_peer_message(
            2,
            PeerMessage::RequestCatchup { from_lsn: 44 },
            &outbound,
            primary_cluster.as_ref(),
            &applier,
            &source,
            &NoPromotionHandler,
            &NoSnapshotSink,
            None,
            &primary_conn,
        )
        .await
        .unwrap();

        assert_eq!(
            outbound_rx.recv().await.unwrap(),
            PeerMessage::CatchupUnavailable {
                requested_lsn: 44,
                oldest_retained_lsn: 88,
            }
        );
    }

    #[tokio::test]
    async fn peer_connection_times_out_waiting_for_wal_ack() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let (primary_conn, _replica_conn) = connect_peer_pair(
            primary_cluster,
            replica_cluster,
            Arc::new(RecordingApplier::default()),
            Arc::new(DelayedApplier {
                delay: Duration::from_millis(100),
            }),
        )
        .await;

        let err = primary_conn
            .send_wal_record_and_wait(56, b"slow wal frame", Duration::from_millis(5))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ReplicationError::WalAckTimedOut {
                node_id: 2,
                lsn: 56
            }
        ));
    }

    #[tokio::test]
    async fn primary_replicator_can_use_peer_backed_wal_sender() {
        let primary_cluster = cluster_for(1, &[1, 2, 3]);
        let replica_cluster = cluster_for(2, &[1, 2, 3]);
        primary_cluster.record_heartbeat(3, 0, 0);
        let replica_applier = Arc::new(RecordingApplier::default());
        let (primary_conn, _replica_conn) = connect_peer_pair(
            primary_cluster.clone(),
            replica_cluster,
            Arc::new(RecordingApplier::default()),
            replica_applier.clone(),
        )
        .await;

        let sender = Arc::new(PeerWalSender::new(Duration::from_secs(1)));
        sender.insert(primary_conn);
        let replicator = PrimaryReplicator::new(primary_cluster.clone(), sender);

        replicator
            .replicate_and_wait(88, b"replicated wal")
            .await
            .unwrap();

        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 88, b"replicated wal".to_vec())]
        );
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 88);
    }

    #[test]
    fn peer_mesh_enforces_lower_node_initiation_semantics() {
        let low_mesh = mesh_for(
            cluster_for(1, &[1, 2]),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let high_mesh = mesh_for(
            cluster_for(2, &[1, 2]),
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );

        assert!(low_mesh.should_initiate_connection(2).unwrap());
        assert!(!high_mesh.should_initiate_connection(1).unwrap());
        assert!(matches!(
            low_mesh.should_initiate_connection(1).unwrap_err(),
            ReplicationError::PeerIsSelf(1)
        ));
        assert!(matches!(
            low_mesh.should_initiate_connection(99).unwrap_err(),
            ReplicationError::UnknownPeer(99)
        ));
    }

    #[tokio::test]
    async fn peer_mesh_attaches_streams_and_routes_wal() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let replica_applier = Arc::new(RecordingApplier::default());
        let primary_mesh = mesh_for(
            primary_cluster.clone(),
            NodeRole::Primary,
            10,
            7,
            70,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            5,
            50,
            replica_applier.clone(),
        );

        let (primary_conn, _replica_conn) =
            attach_mesh_pair(primary_mesh.clone(), replica_mesh).await;

        assert_eq!(primary_mesh.connected_peers(), vec![2]);
        assert_eq!(primary_mesh.connection(2).unwrap().peer_generation(), 20);
        assert_eq!(primary_conn.peer_id(), 2);

        primary_mesh
            .replicate_and_wait(2, 91, b"mesh wal")
            .await
            .unwrap();
        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 91, b"mesh wal".to_vec())]
        );
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 91);
    }

    #[tokio::test]
    async fn peer_mesh_replaces_connection_for_same_node() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );

        let first_replica_mesh = mesh_for(
            cluster_for(2, &[1, 2]),
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        attach_mesh_pair(primary_mesh.clone(), first_replica_mesh).await;
        assert_eq!(primary_mesh.connection(2).unwrap().peer_generation(), 20);

        let replacement_replica_mesh = mesh_for(
            cluster_for(2, &[1, 2]),
            NodeRole::Replica,
            21,
            100,
            200,
            Arc::new(RecordingApplier::default()),
        );
        attach_mesh_pair(primary_mesh.clone(), replacement_replica_mesh).await;

        assert_eq!(primary_mesh.connected_peers(), vec![2]);
        let replacement = primary_mesh.connection(2).unwrap();
        assert_eq!(replacement.peer_generation(), 21);
        assert_eq!(replacement.applied_lsn(), 100);
        assert_eq!(replacement.applied_ts(), 200);
    }

    #[tokio::test]
    async fn primary_replicator_can_be_constructed_from_peer_mesh() {
        let primary_cluster = cluster_for(1, &[1, 2, 3]);
        let replica_cluster = cluster_for(2, &[1, 2, 3]);
        primary_cluster.record_heartbeat(3, 0, 0);
        let replica_applier = Arc::new(RecordingApplier::default());
        let primary_mesh = mesh_for(
            primary_cluster.clone(),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            replica_applier.clone(),
        );
        attach_mesh_pair(primary_mesh.clone(), replica_mesh).await;

        let replicator = PrimaryReplicator::from_mesh(primary_mesh);
        replicator
            .replicate_and_wait(92, b"mesh-backed primary")
            .await
            .unwrap();

        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 92, b"mesh-backed primary".to_vec())]
        );
    }

    #[tokio::test]
    async fn peer_mesh_broadcasts_local_heartbeat_to_connected_peers() {
        let primary_cluster = cluster_for(1, &[1, 2]);
        let replica_cluster = cluster_for(2, &[1, 2]);
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            7,
            70,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster.clone(),
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        attach_mesh_pair(primary_mesh.clone(), replica_mesh).await;

        primary_mesh.set_local_progress(123, 456);
        primary_mesh.broadcast_heartbeat().await.unwrap();

        eventually(|| replica_cluster.status(1).unwrap().applied_lsn == 123).await;
        assert_eq!(replica_cluster.status(1).unwrap().applied_ts, 456);
    }

    #[tokio::test]
    async fn peer_mesh_tcp_runtime_connects_lower_to_higher_and_routes_wal() {
        let (listener1, addr1) = bound_tokio_listener();
        let (listener2, addr2) = bound_tokio_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster = cluster_with_topology(1, topology.clone());
        let replica_cluster = cluster_with_topology(2, topology);
        let replica_applier = Arc::new(RecordingApplier::default());
        let primary_mesh = mesh_for(
            primary_cluster.clone(),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            replica_applier.clone(),
        );

        let replica_runtime = replica_mesh
            .start_tcp_with_listener(listener2)
            .await
            .unwrap();
        let primary_runtime = primary_mesh
            .start_tcp_with_listener(listener1)
            .await
            .unwrap();

        eventually(|| primary_mesh.connection(2).is_some() && replica_mesh.connection(1).is_some())
            .await;

        primary_mesh
            .replicate_and_wait(2, 600, b"tcp mesh wal")
            .await
            .unwrap();
        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 600, b"tcp mesh wal".to_vec())]
        );
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 600);

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
    }

    #[tokio::test]
    async fn peer_mesh_tcp_runtime_retries_until_higher_node_listens() {
        let (listener1, addr1) = bound_tokio_listener();
        let (listener2, addr2) = bound_tokio_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster = cluster_with_topology(1, topology.clone());
        let replica_cluster = cluster_with_topology(2, topology);
        let primary_mesh = mesh_for(
            primary_cluster,
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );

        let primary_runtime = primary_mesh
            .start_tcp_with_listener(listener1)
            .await
            .unwrap();
        sleep(Duration::from_millis(75)).await;
        assert!(primary_mesh.connection(2).is_none());

        let replica_runtime = replica_mesh
            .start_tcp_with_listener(listener2)
            .await
            .unwrap();
        eventually(|| primary_mesh.connection(2).is_some() && replica_mesh.connection(1).is_some())
            .await;

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
    }

    #[tokio::test]
    async fn peer_mesh_tcp_runtime_reconnects_after_established_connection_closes() {
        let (listener1, addr1) = bound_tokio_listener();
        let (listener2, addr2) = bound_tokio_listener();
        let topology = HashMap::from([(1, addr1), (2, addr2)]);
        let primary_cluster = cluster_with_topology(1, topology.clone());
        let replica_cluster = cluster_with_topology(2, topology);
        let replica_applier = Arc::new(RecordingApplier::default());
        let primary_mesh = mesh_for(
            primary_cluster.clone(),
            NodeRole::Primary,
            10,
            0,
            0,
            Arc::new(RecordingApplier::default()),
        );
        let replica_mesh = mesh_for(
            replica_cluster,
            NodeRole::Replica,
            20,
            0,
            0,
            replica_applier.clone(),
        );

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

        let first_connection = primary_mesh.connection(2).unwrap();
        first_connection.close();
        eventually(|| {
            primary_mesh
                .connection(2)
                .is_some_and(|conn| conn.is_connected())
                && replica_mesh
                    .connection(1)
                    .is_some_and(|conn| conn.is_connected())
        })
        .await;

        primary_mesh
            .replicate_and_wait(2, 700, b"wal after reconnect")
            .await
            .unwrap();
        assert_eq!(
            replica_applier.applied.lock().as_slice(),
            &[(1, 700, b"wal after reconnect".to_vec())]
        );
        assert_eq!(primary_cluster.status(2).unwrap().applied_lsn, 700);

        primary_runtime.shutdown().await;
        replica_runtime.shutdown().await;
    }

    #[test]
    fn cluster_membership_tracks_quorum_and_timeouts() {
        let cluster = cluster(&[1, 2, 3]);
        assert_eq!(cluster.cluster_size(), 3);
        assert!(!cluster.has_quorum());

        cluster.record_heartbeat(2, 10, 7);
        assert!(cluster.has_quorum());
        assert_eq!(cluster.online_replicas(), vec![2]);

        {
            let mut nodes = cluster.nodes.write();
            nodes.get_mut(&2).unwrap().last_heartbeat = Instant::now() - Duration::from_secs(4);
        }
        cluster.check_timeouts();
        assert_eq!(cluster.status(2).unwrap().state, NodeState::Suspect);
        assert!(cluster.has_quorum(), "suspect nodes still count for quorum");
        assert!(cluster.online_replicas().is_empty());

        {
            let mut nodes = cluster.nodes.write();
            nodes.get_mut(&2).unwrap().last_heartbeat = Instant::now() - Duration::from_secs(11);
        }
        cluster.check_timeouts();
        assert_eq!(cluster.status(2).unwrap().state, NodeState::Down);
        assert!(!cluster.has_quorum());
    }

    #[test]
    fn topology_updates_are_guarded_by_quorum_rules() {
        let cluster = cluster(&[1, 2, 3]);
        cluster.record_heartbeat(2, 1, 0);

        let err = cluster.remove_node(2).unwrap_err();
        assert!(matches!(err, ReplicationError::NodeNotDown { .. }));

        cluster.add_node(4, addr(5004)).unwrap();
        assert_eq!(cluster.cluster_size(), 4);
        cluster.update_peer_addr(4, addr(6004)).unwrap();
        assert_eq!(cluster.status(4).unwrap().addr, addr(6004));

        cluster.remove_node(4).unwrap();
        assert_eq!(cluster.cluster_size(), 3);
    }

    #[tokio::test]
    async fn primary_replicator_succeeds_with_majority_ack() {
        let cluster = cluster(&[1, 2, 3]);
        cluster.record_heartbeat(2, 0, 0);
        cluster.record_heartbeat(3, 0, 0);
        let sender = Arc::new(RecordingSender::default());
        sender.failures.lock().push(3);
        let replicator = PrimaryReplicator::new(cluster.clone(), sender.clone());

        replicator
            .replicate_and_wait(42, b"wal-record")
            .await
            .unwrap();

        assert!(!replicator.is_holding());
        assert_eq!(cluster.status(2).unwrap().applied_lsn, 42);
        assert_eq!(replicator.min_replica_lsn(), Some(0));
        assert_eq!(replicator.replication_retention_lsn(), Some(0));
        assert_eq!(
            sender.sent.lock().as_slice(),
            &[(2, 42, b"wal-record".to_vec())]
        );
    }

    #[tokio::test]
    async fn primary_replicator_enters_hold_without_quorum() {
        let cluster = cluster(&[1, 2, 3]);
        let sender = Arc::new(RecordingSender::default());
        let replicator = PrimaryReplicator::new(cluster, sender);

        let err = replicator
            .replicate_and_wait(42, b"wal-record")
            .await
            .unwrap_err();

        assert!(err.contains("quorum"));
        assert!(replicator.is_holding());
    }

    #[tokio::test]
    async fn primary_replicator_enters_hold_when_acks_lose_majority() {
        let cluster = cluster(&[1, 2, 3]);
        cluster.record_heartbeat(2, 0, 0);
        cluster.record_heartbeat(3, 0, 0);
        let sender = Arc::new(RecordingSender::default());
        sender.failures.lock().extend([2, 3]);
        let replicator = PrimaryReplicator::new(cluster, sender);

        let err = replicator
            .replicate_and_wait(42, b"wal-record")
            .await
            .unwrap_err();

        assert!(err.contains("failed to reach majority"));
        assert!(replicator.is_holding());
    }

    #[tokio::test]
    async fn single_node_primary_replicates_without_sender_calls() {
        let cluster = cluster(&[1]);
        let sender = Arc::new(RecordingSender::default());
        let replicator = PrimaryReplicator::new(cluster, sender.clone());

        replicator.replicate_and_wait(1, b"local").await.unwrap();

        assert!(sender.sent.lock().is_empty());
        assert_eq!(replicator.min_replica_lsn(), None);
        assert_eq!(replicator.replication_retention_lsn(), None);
    }
}
