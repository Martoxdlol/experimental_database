use std::io;

use crate::pager::types::PageId;

#[derive(Debug, thiserror::Error)]
pub enum PagerError {
    #[error("Page {0} is out of bounds")]
    OutOfBounds(PageId),

    #[error("Pager is at maximum capacity")]
    CapacityReached,

    #[error("Page {0} is already free or invalid")]
    InvalidPage(PageId),

    #[error("Page {0} failed checksum validation")]
    Corrupted(PageId),

    /// Passthrough for genuine I/O errors.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<PagerError> for io::Error {
    fn from(e: PagerError) -> io::Error {
        match e {
            PagerError::Io(io_err) => io_err,
            other => io::Error::other(other),
        }
    }
}
