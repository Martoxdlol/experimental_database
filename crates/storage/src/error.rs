//! Central error types for the storage engine.

use std::io;

/// Typed error enum for storage engine operations.
///
/// All public APIs continue to return `io::Result<T>`. This enum provides
/// semantic precision internally — callers that need to distinguish error
/// kinds can downcast via `io::Error::get_ref()` / `io::Error::into_inner()`.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Page header buffer is too small to contain a valid header.
    #[error("corrupt page header: buffer is {size} bytes, need at least {required}")]
    CorruptPageHeader { size: usize, required: usize },

    /// Binary data is truncated at the given offset.
    #[error("data truncated at offset {offset}: need {needed} bytes, have {available}")]
    DataTruncated {
        offset: usize,
        needed: usize,
        available: usize,
    },

    /// The WAL writer background task has shut down.
    #[error("WAL writer shut down")]
    WalShutDown,

    /// A configuration value is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// On-disk data does not match expected format.
    #[error("data corruption: {0}")]
    Corruption(String),

    /// An internal invariant was violated (indicates a bug).
    #[error("internal invariant violated: {0}")]
    InternalBug(String),

    /// Passthrough for genuine I/O errors.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<StorageError> for io::Error {
    fn from(e: StorageError) -> io::Error {
        match e {
            StorageError::Io(io_err) => io_err,
            other => io::Error::other(other),
        }
    }
}
