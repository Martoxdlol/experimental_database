//! B1: Database and transaction configuration.

use std::time::Duration;

/// Database-level configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Page size in bytes. Default: 8192.
    pub page_size: usize,
    /// Memory budget for the buffer pool. Default: 256 MB.
    pub memory_budget: usize,
    /// Maximum document size in bytes. Default: 16 MB.
    pub max_doc_size: usize,
    /// Threshold for external (heap) storage. Default: page_size / 4.
    pub external_threshold: usize,
    /// WAL segment size in bytes. Default: 64 MB.
    pub wal_segment_size: usize,
    /// WAL bytes before triggering auto-checkpoint. Default: 64 MB.
    pub checkpoint_wal_threshold: usize,
    /// Interval between auto-checkpoint checks. Default: 60s.
    pub checkpoint_interval: Duration,
    /// Interval between vacuum runs. Default: 300s.
    pub vacuum_interval: Duration,
    /// Transaction-level limits.
    pub transaction: TransactionConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        let page_size = 8192;
        DatabaseConfig {
            page_size,
            memory_budget: 256 * 1024 * 1024,
            max_doc_size: 16 * 1024 * 1024,
            external_threshold: page_size / 4,
            wal_segment_size: 64 * 1024 * 1024,
            checkpoint_wal_threshold: 64 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(60),
            vacuum_interval: Duration::from_secs(300),
            transaction: TransactionConfig::default(),
        }
    }
}

impl DatabaseConfig {
    /// Convert to a `StorageConfig`.
    pub(crate) fn to_storage_config(&self) -> exdb_storage::engine::StorageConfig {
        exdb_storage::engine::StorageConfig {
            page_size: self.page_size,
            memory_budget: self.memory_budget,
            wal_segment_size: self.wal_segment_size,
            checkpoint_wal_threshold: self.checkpoint_wal_threshold,
            checkpoint_interval: self.checkpoint_interval,
        }
    }
}

/// Transaction-level resource limits.
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum idle time before a transaction is aborted. Default: 30s.
    pub idle_timeout: Duration,
    /// Maximum total lifetime for a transaction. Default: 300s.
    pub max_lifetime: Duration,
    /// Maximum number of read-set intervals per transaction. Default: 4096.
    pub max_intervals: usize,
    /// Maximum bytes scanned per query. Default: 64 MB.
    pub max_scanned_bytes: usize,
    /// Maximum documents scanned per query. Default: 100_000.
    pub max_scanned_docs: usize,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        TransactionConfig {
            idle_timeout: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(300),
            max_intervals: 4096,
            max_scanned_bytes: 64 * 1024 * 1024,
            max_scanned_docs: 100_000,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_database_config() {
        let c = DatabaseConfig::default();
        assert_eq!(c.page_size, 8192);
        assert_eq!(c.memory_budget, 256 * 1024 * 1024);
        assert_eq!(c.max_doc_size, 16 * 1024 * 1024);
        assert_eq!(c.external_threshold, 2048);
        assert_eq!(c.checkpoint_interval, Duration::from_secs(60));
        assert_eq!(c.vacuum_interval, Duration::from_secs(300));
    }

    #[test]
    fn default_transaction_config() {
        let c = TransactionConfig::default();
        assert_eq!(c.idle_timeout, Duration::from_secs(30));
        assert_eq!(c.max_lifetime, Duration::from_secs(300));
        assert_eq!(c.max_intervals, 4096);
        assert_eq!(c.max_scanned_bytes, 64 * 1024 * 1024);
        assert_eq!(c.max_scanned_docs, 100_000);
    }

    #[test]
    fn custom_config() {
        let c = DatabaseConfig {
            page_size: 4096,
            memory_budget: 128 * 1024 * 1024,
            external_threshold: 1024,
            ..Default::default()
        };
        assert_eq!(c.page_size, 4096);
        assert_eq!(c.memory_budget, 128 * 1024 * 1024);
        assert_eq!(c.external_threshold, 1024);
        // Unchanged fields
        assert_eq!(c.max_doc_size, 16 * 1024 * 1024);
    }

    #[test]
    fn to_storage_config() {
        let c = DatabaseConfig::default();
        let sc = c.to_storage_config();
        assert_eq!(sc.page_size, c.page_size);
        assert_eq!(sc.memory_budget, c.memory_budget);
        assert_eq!(sc.wal_segment_size, c.wal_segment_size);
    }
}
