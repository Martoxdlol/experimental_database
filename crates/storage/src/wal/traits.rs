use async_trait::async_trait;

use crate::wal::{error::WalError, types::Lsn};

/// Append-only write path for WAL records.
#[async_trait]
pub trait WalStorage: Send + Sync {
    /// Appends a record to the WAL and returns its LSN.
    async fn append(&self, record: &[u8]) -> Result<Lsn, WalError>;

    /// Reads a record from the WAL by its LSN.
    async fn read(&self, lsn: Lsn) -> Result<Vec<u8>, WalError>;

    /// Flushes all buffered WAL records to durable storage to ensure they are safely persisted.
    ///
    /// This can be used when adding multiple entries at the same time in the same operation.
    /// Having a explicit flush allows the caller to control when to actually persist WAL records
    /// avoid unnecessary flushes when multiple records are appended in a batch.
    ///
    /// The implementation internally can also batch different batches to reduce even
    /// further the actual amount of flushes to disk.
    ///
    /// Not calling flush doesn't guarantee that records are not persisted,
    /// but it allows the implementation to optimize flushes.
    async fn flush(&self) -> Result<(), WalError>;

    /// Truncate old WAL records up to the given LSN, which is typically the LSN of the last checkpoint record.
    async fn truncate(&self, up_to: Lsn) -> Result<(), WalError>;

    /// Returns a Stream of WAL records starting from the given LSN, which can be used for recovery or replication.
    async fn scan_from(&self, from: Lsn) -> Result<Box<dyn WalScan>, WalError>;

    /// Returns a Stream of all WAL records starting from the beginning of the log.
    async fn scan(&self) -> Result<Box<dyn WalScan>, WalError>;
}

#[async_trait]
pub trait WalScan: Send {
    /// Advance to the next record. Returns `None` when the log is exhausted.
    async fn next(&mut self) -> Option<Result<(Lsn, Vec<u8>), WalError>>;
}
