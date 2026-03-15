# Error Types

## Purpose

Shared error types for the database layer. A single `DatabaseError` enum covers all failure modes across B1–B8, mapped to appropriate API error codes by L8.

## Rust Types

```rust
use exdb_core::types::{CollectionId, IndexId, DocId};
use exdb_query::RangeError;

/// Top-level database error.
///
/// Covers all failure modes in L6. L8 maps these to wire-protocol error codes.
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
    Range(#[from] RangeError),

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
```

## Error-to-API-Code Mapping (for L8)

| DatabaseError | API Error Code | HTTP Status |
|---------------|---------------|-------------|
| `CollectionNotFound` | `collection_not_found` | 404 |
| `CollectionAlreadyExists` | `collection_already_exists` | 409 |
| `IndexNotFound` | `index_not_found` | 404 |
| `IndexAlreadyExists` | `index_already_exists` | 409 |
| `IndexNotReady` | `index_not_ready` | 409 |
| `DocNotFound` | `doc_not_found` | 404 |
| `DocTooLarge` | `doc_too_large` | 413 |
| `ReadonlyWrite` | `readonly_tx` | 400 |
| `ReadLimitExceeded` | `read_limit_exceeded` | 400 |
| `TransactionTimeout` | `tx_timeout` | 408 |
| `Range` | `invalid_range` | 400 |
| `QuorumLost` | `quorum_lost` | 503 |
| `ShuttingDown` | `shutting_down` | 503 |
| `Storage` | `internal_error` | 500 |
