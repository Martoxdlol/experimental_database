//! Shared error types for the database layer (L6).

/// Top-level database error.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    // ── Storage / I/O ──
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),

    // ── Collection ──
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("collection dropped in this transaction")]
    CollectionDropped,

    // ── Index ──
    #[error("index not found: {collection}.{index}")]
    IndexNotFound { collection: String, index: String },

    #[error("index already exists: {collection}.{index}")]
    IndexAlreadyExists { collection: String, index: String },

    #[error("index not ready (still building): {0}")]
    IndexNotReady(String),

    #[error("cannot drop system index: {0}")]
    SystemIndex(String),

    // ── Document ──
    #[error("document not found")]
    DocNotFound,

    #[error("document too large: {size} bytes (max {max})")]
    DocTooLarge { size: usize, max: usize },

    // ── Transaction ──
    #[error("readonly transaction cannot write")]
    ReadonlyWrite,

    #[error("read limit exceeded: {0}")]
    ReadLimitExceeded(String),

    #[error("transaction timeout")]
    TransactionTimeout,

    #[error("database is shutting down")]
    ShuttingDown,

    // ── Query ──
    #[error("range error: {0}")]
    Range(String),

    #[error("invalid field path: {0}")]
    InvalidFieldPath(String),

    // ── Commit ──
    #[error("commit error: {0}")]
    Commit(String),

    #[error("replication quorum lost")]
    QuorumLost,

    // ── Database Management ──
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    #[error("reserved name: {0}")]
    ReservedName(String),

    #[error("invalid name: {0}")]
    InvalidName(String),
}

/// Alias for Result<T, DatabaseError>.
pub type Result<T> = std::result::Result<T, DatabaseError>;
