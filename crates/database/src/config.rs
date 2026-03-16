//! B1: Database and transaction configuration with sensible defaults.

use std::time::Duration;

/// Database-level configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Page size in bytes. Default: 8192.
    pub page_size: usize,
    /// Total memory budget for the buffer pool. Default: 256 MB.
    pub memory_budget: usize,
    /// Maximum document size in bytes. Default: 16 MB.
    pub max_doc_size: usize,
    /// Documents larger than this are stored on heap. Default: page_size / 4.
    pub external_threshold: usize,
    /// WAL segment size in bytes. Default: 64 MB.
    pub wal_segment_size: usize,
    /// WAL bytes since last checkpoint before triggering a new one. Default: 64 MB.
    pub checkpoint_wal_threshold: usize,
    /// Time between periodic checkpoint attempts. Default: 60 seconds.
    pub checkpoint_interval: Duration,
    /// Vacuum interval. Default: 300 seconds.
    pub vacuum_interval: Duration,
    /// Transaction configuration.
    pub transaction: TransactionConfig,
}

/// Transaction-scoped limits.
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum idle time. Default: 30 seconds.
    pub idle_timeout: Duration,
    /// Maximum total lifetime. Default: 5 minutes.
    pub max_lifetime: Duration,
    /// Maximum number of read intervals. Default: 4,096.
    pub max_intervals: usize,
    /// Maximum total bytes scanned. Default: 64 MB.
    pub max_scanned_bytes: usize,
    /// Maximum documents scanned. Default: 100,000.
    pub max_scanned_docs: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: 8192,
            memory_budget: 256 * 1024 * 1024,
            max_doc_size: 16 * 1024 * 1024,
            external_threshold: 8192 / 4,
            wal_segment_size: 64 * 1024 * 1024,
            checkpoint_wal_threshold: 64 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(60),
            vacuum_interval: Duration::from_secs(300),
            transaction: TransactionConfig::default(),
        }
    }
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(300),
            max_intervals: 4096,
            max_scanned_bytes: 64 * 1024 * 1024,
            max_scanned_docs: 100_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_database_config() {
        let cfg = DatabaseConfig::default();
        assert_eq!(cfg.page_size, 8192);
        assert_eq!(cfg.memory_budget, 256 * 1024 * 1024);
        assert_eq!(cfg.max_doc_size, 16 * 1024 * 1024);
        assert_eq!(cfg.external_threshold, 8192 / 4);
        assert_eq!(cfg.wal_segment_size, 64 * 1024 * 1024);
        assert_eq!(cfg.checkpoint_wal_threshold, 64 * 1024 * 1024);
        assert_eq!(cfg.checkpoint_interval, Duration::from_secs(60));
        assert_eq!(cfg.vacuum_interval, Duration::from_secs(300));
    }

    #[test]
    fn default_transaction_config() {
        let cfg = TransactionConfig::default();
        assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
        assert_eq!(cfg.max_lifetime, Duration::from_secs(300));
        assert_eq!(cfg.max_intervals, 4096);
        assert_eq!(cfg.max_scanned_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.max_scanned_docs, 100_000);
    }

    #[test]
    fn custom_config() {
        let cfg = DatabaseConfig {
            page_size: 4096,
            memory_budget: 128 * 1024 * 1024,
            ..Default::default()
        };
        let cloned = cfg.clone();
        assert_eq!(cloned.page_size, 4096);
        assert_eq!(cloned.memory_budget, 128 * 1024 * 1024);
        // Other defaults preserved
        assert_eq!(cloned.max_doc_size, 16 * 1024 * 1024);
    }

    #[test]
    fn external_threshold_default() {
        let cfg = DatabaseConfig::default();
        assert_eq!(cfg.external_threshold, cfg.page_size / 4);
    }
}
