//! Experimental embedded JSON document store with MVCC, WAL, indexes, and subscriptions.

pub mod types;
pub mod storage;
pub mod catalog;
pub mod mvcc;
pub mod tx;
pub mod doc;
pub mod filter;
pub mod index;
pub mod query;
pub mod subs;
pub mod db;
pub mod protocol;
pub mod server;

// Re-export the primary public API
pub use db::{Database, DbConfig, MutationSession, QuerySession};
pub use types::{
    DocId, CollectionId, IndexId, Ts, FieldPath, JsonScalar,
    Filter, IndexSpec, IndexState, PatchOp, QueryType, QueryOptions,
    MutationCommitOutcome, QueryCommitOutcome, InvalidationEvent,
    WalFsyncPolicy, ReplicationConfig,
};
pub use subs::Subscription;
