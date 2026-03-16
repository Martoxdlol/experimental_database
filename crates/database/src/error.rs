//! Database error types covering all L6 failure modes.

use exdb_query::RangeError;

/// Unified error type for all database operations.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    /// Storage I/O error.
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),

    /// Collection not found.
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    /// Collection already exists.
    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    /// Collection was dropped in this transaction.
    #[error("collection was dropped in this transaction")]
    CollectionDropped,

    /// Index not found.
    #[error("index not found: {collection}.{index}")]
    IndexNotFound { collection: String, index: String },

    /// Index already exists.
    #[error("index already exists: {collection}.{index}")]
    IndexAlreadyExists { collection: String, index: String },

    /// Index not ready (still building).
    #[error("index not ready: {0}")]
    IndexNotReady(String),

    /// Cannot drop a system index.
    #[error("cannot drop system index: {0}")]
    SystemIndex(String),

    /// Document not found (for replace/patch/delete).
    #[error("document not found")]
    DocNotFound,

    /// Document exceeds maximum size.
    #[error("document too large: {size} bytes (max: {max})")]
    DocTooLarge { size: usize, max: usize },

    /// Attempted write operation in a read-only transaction.
    #[error("cannot write in a read-only transaction")]
    ReadonlyWrite,

    /// Read limit exceeded.
    #[error("read limit exceeded: {0}")]
    ReadLimitExceeded(String),

    /// Transaction has exceeded its lifetime or idle timeout.
    #[error("transaction timeout")]
    TransactionTimeout,

    /// Database is shutting down.
    #[error("database is shutting down")]
    ShuttingDown,

    /// Range expression error.
    #[error("range error: {0:?}")]
    Range(RangeError),

    /// Access method error.
    #[error("access error: {0:?}")]
    Access(exdb_query::AccessError),

    /// Invalid field path.
    #[error("invalid field path: {0}")]
    InvalidFieldPath(String),

    /// Commit-level error.
    #[error("commit error: {0}")]
    Commit(String),

    /// Replication quorum lost.
    #[error("replication quorum lost")]
    QuorumLost,

    /// Named database not found (SystemDatabase).
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// Named database already exists (SystemDatabase).
    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    /// Reserved name (SystemDatabase).
    #[error("reserved name: {0}")]
    ReservedName(String),

    /// Invalid name.
    #[error("invalid name: {0}")]
    InvalidName(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, DatabaseError>;
