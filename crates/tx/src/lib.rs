//! exdb-tx — Layer 5: Transactions
//!
//! Timestamp allocation, read/write set management, OCC validation via commit
//! log, subscription registry with three modes (Notify / Watch / Subscribe),
//! read-set carry-forward for subscription chains, and the two-task commit
//! architecture (writer task + replication task).
//!
//! # Architecture
//!
//! ```text
//! T7 (Commit) ──→ T1, T2, T3, T4, T5, T6, L2, L3
//! T6 (Subscriptions) ──→ T2, T3, T4, T5
//! T5 (OCC) ──→ T2, T3, T4
//! T4 (CommitLog) ──→ T3
//! T3 (WriteSet) ──→ L1, L3
//! T2 (ReadSet) ──→ L1
//! T1 (Timestamp) ──→ L1
//! ```
//!
//! # Design Authority
//!
//! DESIGN.md sections 5.1–5.12, `docs/plan/transactions/`.
//!
//! # Layer Boundaries
//!
//! - **Depends on:** L1 (Core Types), L2 (Storage Engine), L3 (Document Store).
//! - **No knowledge of:** L6+ (Database, Replication, Wire Protocol).
//! - L5 defines [`IndexResolver`] trait that L6 implements.
//! - L6 bridges L4 (Query) and L5 (Transactions).
//!
//! # Two-Task Commit Architecture
//!
//! - **Writer task** ([`CommitCoordinator`]): Steps 1–5 (OCC → ts → WAL →
//!   mutations → commit log). `!Send` — holds page guards across `.await`.
//! - **Replication task** ([`ReplicationRunner`]): Steps 6–11 (replicate →
//!   `visible_ts` → subscriptions → respond). `Send`.
//! - **[`CommitHandle`]**: `Send + Clone` — distributed to application code.

pub mod commit;
pub mod commit_log;
pub mod occ;
pub mod promotion;
pub mod read_set;
pub mod subscriptions;
pub mod timestamp;
pub mod write_set;

// ─── Public Facade ───

// T1: Timestamp
pub use timestamp::TsAllocator;

// T2: Read set
pub use read_set::{
    CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX, CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX,
    LimitBoundary, QueryId, ReadInterval, ReadSet,
};

// T3: Write set
pub use write_set::{
    CatalogMutation, CatalogMutationHandler, DroppedIndexMeta, IndexDelta, IndexInfo,
    IndexResolver, MutationEntry, MutationOp, NoOpCatalogHandler, WriteSet, compute_index_deltas,
};

// T4: Commit log
pub use commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite, PRIMARY_INDEX_SENTINEL};

// T5: OCC validation
pub use occ::{ConflictError, ConflictKind, validate};

// Transaction promotion payloads
pub use promotion::{deserialize_promotion_payload, serialize_promotion_payload};

// T6: Subscriptions
pub use subscriptions::{
    ChainContinuation, InvalidationEvent, SubscriptionId, SubscriptionInterval, SubscriptionMeta,
    SubscriptionMode, SubscriptionRegistry,
};

// T7: Commit protocol
pub use commit::{
    CommitCoordinator, CommitError, CommitHandle, CommitRequest, CommitResult, ConflictRetry,
    NoReplication, ReplicationHook, ReplicationRunner, WAL_PAYLOAD_VERSION,
    deserialize_wal_payload,
};
