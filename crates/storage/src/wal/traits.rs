use async_trait::async_trait;

use crate::wal::{error::WalError, types::Lsn};

/// Append-only write path for WAL records.
#[async_trait]
pub trait WalWriter: Send + Sync {
    /// Append a record and return its LSN.
    async fn append(&self, record: &[u8]) -> Result<Lsn, WalError>;

    /// Flush buffered records to durable storage.
    async fn sync(&self) -> Result<(), WalError>;
}

/// Read path for WAL recovery and replication.
#[async_trait]
pub trait WalReader: Send + Sync {
    /// Read records starting from the given LSN.
    async fn read_from(
        &self,
        lsn: Lsn,
    ) -> Result<Vec<(Lsn, Vec<u8>)>, WalError>;
}
