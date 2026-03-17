use std::io;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("WAL record at LSN {0} is corrupted")]
    Corrupted(super::types::Lsn),

    #[error("WAL is full")]
    Full,
}
