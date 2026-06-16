//! B1: Database and transaction configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};

const PAGE_HEADER_SIZE: usize = 32;
const SLOT_ENTRY_SIZE: usize = 4;
const BTREE_KEY_LEN_SIZE: usize = 2;
const PRIMARY_KEY_SIZE: usize = 24;
const PRIMARY_INLINE_VALUE_HEADER_SIZE: usize = 5;
const DURABLE_BOOTSTRAP_PAGE_COUNT: u64 = 4;
const WAL_SEGMENT_HEADER_SIZE: u64 = 32;

fn default_close_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Database-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Page size in bytes. Default: 8192.
    pub page_size: usize,
    /// Memory budget for the buffer pool. Default: 256 MB.
    pub memory_budget: usize,
    /// Optional maximum retained disk usage in bytes. Default: unlimited.
    pub max_disk_usage_bytes: Option<u64>,
    /// Maximum document size in bytes. Default: 16 MB.
    pub max_doc_size: usize,
    /// Threshold for external (heap) storage. Default: page_size / 4.
    pub external_threshold: usize,
    /// WAL segment size in bytes. Default: 64 MB.
    pub wal_segment_size: usize,
    /// Optional maximum retained WAL bytes for replication catch-up.
    ///
    /// When exceeded, checkpoint reclamation may discard old replica catch-up
    /// WAL and force slow replicas onto snapshot reconstruction.
    pub wal_retention_max_size: Option<u64>,
    /// Optional maximum age for sealed WAL segments retained for replication.
    pub wal_retention_max_age: Option<Duration>,
    /// WAL bytes before triggering auto-checkpoint. Default: 64 MB.
    pub checkpoint_wal_threshold: usize,
    /// Interval between auto-checkpoint checks. Default: 60s.
    pub checkpoint_interval: Duration,
    /// Interval between vacuum runs. Default: 300s.
    pub vacuum_interval: Duration,
    /// Maximum time close waits for active transactions before proceeding.
    /// Default: 10s.
    #[serde(default = "default_close_timeout")]
    pub close_timeout: Duration,
    /// Run a quick storage integrity check on database open. Default: false.
    pub check_on_startup: bool,
    /// Run a full storage/catalog B-tree integrity check on database open.
    /// Default: false.
    pub check_on_startup_full: bool,
    /// Transaction-level limits.
    pub transaction: TransactionConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        let page_size = 8192;
        DatabaseConfig {
            page_size,
            memory_budget: 256 * 1024 * 1024,
            max_disk_usage_bytes: None,
            max_doc_size: 16 * 1024 * 1024,
            external_threshold: page_size / 4,
            wal_segment_size: 64 * 1024 * 1024,
            wal_retention_max_size: None,
            wal_retention_max_age: None,
            checkpoint_wal_threshold: 64 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(60),
            vacuum_interval: Duration::from_secs(300),
            close_timeout: default_close_timeout(),
            check_on_startup: false,
            check_on_startup_full: false,
            transaction: TransactionConfig::default(),
        }
    }
}

impl DatabaseConfig {
    /// Validate database and transaction configuration before opening or
    /// persisting a database entry.
    pub fn validate(&self) -> Result<(), String> {
        self.to_storage_config()
            .validate()
            .map_err(|err| err.to_string())?;
        if self.max_doc_size == 0 {
            return Err("max_doc_size must be greater than zero".to_string());
        }
        if self.external_threshold == 0 {
            return Err("external_threshold must be greater than zero".to_string());
        }
        let max_inline_body = self.max_inline_primary_body_size()?;
        if self.external_threshold > max_inline_body {
            return Err(format!(
                "external_threshold {} exceeds maximum inline primary body size {} for page_size {}",
                self.external_threshold, max_inline_body, self.page_size
            ));
        }
        if let Some(limit) = self.max_disk_usage_bytes {
            let min_bootstrap_usage = self.min_durable_bootstrap_disk_usage()?;
            if limit < min_bootstrap_usage {
                return Err(format!(
                    "max_disk_usage_bytes {} is smaller than minimum durable database bootstrap usage {} for page_size {}",
                    limit, min_bootstrap_usage, self.page_size
                ));
            }
        }
        if self.wal_segment_size == 0 {
            return Err("wal_segment_size must be greater than zero".to_string());
        }
        if self.checkpoint_wal_threshold == 0 {
            return Err("checkpoint_wal_threshold must be greater than zero".to_string());
        }
        if self.checkpoint_interval.is_zero() {
            return Err("checkpoint_interval must be greater than zero".to_string());
        }
        if self.vacuum_interval.is_zero() {
            return Err("vacuum_interval must be greater than zero".to_string());
        }
        if self.close_timeout.is_zero() {
            return Err("close_timeout must be greater than zero".to_string());
        }
        self.transaction.validate()
    }

    fn max_inline_primary_body_size(&self) -> Result<usize, String> {
        self.page_size
            .checked_sub(PAGE_HEADER_SIZE)
            .and_then(|remaining| remaining.checked_sub(SLOT_ENTRY_SIZE))
            .and_then(|remaining| remaining.checked_sub(BTREE_KEY_LEN_SIZE))
            .and_then(|remaining| remaining.checked_sub(PRIMARY_KEY_SIZE))
            .and_then(|remaining| remaining.checked_sub(PRIMARY_INLINE_VALUE_HEADER_SIZE))
            .filter(|max| *max > 0)
            .ok_or_else(|| {
                format!(
                    "page_size {} leaves no room for inline primary document bodies",
                    self.page_size
                )
            })
    }

    fn min_durable_bootstrap_disk_usage(&self) -> Result<u64, String> {
        let page_size = u64::try_from(self.page_size)
            .map_err(|_| format!("page_size {} exceeds u64", self.page_size))?;
        DURABLE_BOOTSTRAP_PAGE_COUNT
            .checked_mul(page_size)
            .and_then(|bytes| bytes.checked_add(WAL_SEGMENT_HEADER_SIZE))
            .ok_or_else(|| {
                format!(
                    "minimum durable database bootstrap usage overflows for page_size {}",
                    self.page_size
                )
            })
    }

    /// Convert to a `StorageConfig`.
    pub(crate) fn to_storage_config(&self) -> exdb_storage::engine::StorageConfig {
        exdb_storage::engine::StorageConfig {
            page_size: self.page_size,
            memory_budget: self.memory_budget,
            max_disk_usage_bytes: self.max_disk_usage_bytes,
            wal_segment_size: self.wal_segment_size,
            wal_retention_max_size: self.wal_retention_max_size,
            wal_retention_max_age: self.wal_retention_max_age,
            checkpoint_wal_threshold: self.checkpoint_wal_threshold,
            checkpoint_interval: self.checkpoint_interval,
        }
    }
}

/// Transaction-level resource limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionConfig {
    /// Maximum idle time before a transaction is aborted. Default: 30s.
    pub idle_timeout: Duration,
    /// Maximum total lifetime for a transaction. Default: 300s.
    pub max_lifetime: Duration,
    /// Maximum number of read-set intervals per transaction. Default: 4096.
    pub max_intervals: usize,
    /// Maximum coarse work units per transaction. Default: 100,000.
    ///
    /// Public transaction operations and scanned index rows each consume at
    /// least one unit, bounding CPU-heavy transactions independently of wall
    /// clock time.
    pub max_operations: usize,
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
            max_operations: 100_000,
            max_scanned_bytes: 64 * 1024 * 1024,
            max_scanned_docs: 100_000,
        }
    }
}

impl TransactionConfig {
    /// Validate transaction timeout and accounting limits.
    pub fn validate(&self) -> Result<(), String> {
        if self.idle_timeout.is_zero() {
            return Err("transaction.idle_timeout must be greater than zero".to_string());
        }
        if self.max_lifetime.is_zero() {
            return Err("transaction.max_lifetime must be greater than zero".to_string());
        }
        if self.max_intervals == 0 {
            return Err("transaction.max_intervals must be greater than zero".to_string());
        }
        if self.max_operations == 0 {
            return Err("transaction.max_operations must be greater than zero".to_string());
        }
        Ok(())
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
        assert_eq!(c.max_disk_usage_bytes, None);
        assert_eq!(c.max_doc_size, 16 * 1024 * 1024);
        assert_eq!(c.external_threshold, 2048);
        assert_eq!(c.wal_retention_max_size, None);
        assert_eq!(c.wal_retention_max_age, None);
        assert_eq!(c.checkpoint_interval, Duration::from_secs(60));
        assert_eq!(c.vacuum_interval, Duration::from_secs(300));
        assert_eq!(c.close_timeout, Duration::from_secs(10));
        assert!(!c.check_on_startup);
        assert!(!c.check_on_startup_full);
    }

    #[test]
    fn database_config_deserializes_legacy_manifest_without_close_timeout() {
        let json = serde_json::json!({
            "page_size": 8192,
            "memory_budget": 268435456usize,
            "max_disk_usage_bytes": null,
            "max_doc_size": 16777216usize,
            "external_threshold": 2048usize,
            "wal_segment_size": 67108864usize,
            "wal_retention_max_size": null,
            "wal_retention_max_age": null,
            "checkpoint_wal_threshold": 67108864usize,
            "checkpoint_interval": { "secs": 60, "nanos": 0 },
            "vacuum_interval": { "secs": 300, "nanos": 0 },
            "check_on_startup": false,
            "check_on_startup_full": false,
            "transaction": {
                "idle_timeout": { "secs": 30, "nanos": 0 },
                "max_lifetime": { "secs": 300, "nanos": 0 },
                "max_intervals": 4096usize,
                "max_operations": 100000usize,
                "max_scanned_bytes": 67108864usize,
                "max_scanned_docs": 100000usize
            }
        });

        let config: DatabaseConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.close_timeout, Duration::from_secs(10));
    }

    #[test]
    fn default_transaction_config() {
        let c = TransactionConfig::default();
        assert_eq!(c.idle_timeout, Duration::from_secs(30));
        assert_eq!(c.max_lifetime, Duration::from_secs(300));
        assert_eq!(c.max_intervals, 4096);
        assert_eq!(c.max_operations, 100_000);
        assert_eq!(c.max_scanned_bytes, 64 * 1024 * 1024);
        assert_eq!(c.max_scanned_docs, 100_000);
    }

    #[test]
    fn custom_config() {
        let c = DatabaseConfig {
            page_size: 4096,
            memory_budget: 128 * 1024 * 1024,
            max_disk_usage_bytes: Some(512 * 1024 * 1024),
            external_threshold: 1024,
            ..Default::default()
        };
        assert_eq!(c.page_size, 4096);
        assert_eq!(c.memory_budget, 128 * 1024 * 1024);
        assert_eq!(c.max_disk_usage_bytes, Some(512 * 1024 * 1024));
        assert_eq!(c.external_threshold, 1024);
        // Unchanged fields
        assert_eq!(c.max_doc_size, 16 * 1024 * 1024);
    }

    #[test]
    fn validate_rejects_unusable_database_config() {
        let mut config = DatabaseConfig::default();
        config.memory_budget = config.page_size - 1;
        let err = config.validate().unwrap_err();
        assert!(err.contains("memory_budget must be at least page_size"));

        let config = DatabaseConfig {
            max_doc_size: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("max_doc_size must be greater than zero"));

        let config = DatabaseConfig {
            checkpoint_interval: Duration::ZERO,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("checkpoint_interval must be greater than zero"));

        let config = DatabaseConfig {
            close_timeout: Duration::ZERO,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("close_timeout must be greater than zero"));

        let config = DatabaseConfig {
            wal_segment_size: WAL_SEGMENT_HEADER_SIZE as usize,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            err.contains("wal_segment_size 32 must be greater than WAL segment header size 32")
        );

        let mut config = DatabaseConfig {
            page_size: 4096,
            max_disk_usage_bytes: Some(4096 * 4 + 31),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            err.contains("max_disk_usage_bytes 16415 is smaller than minimum durable database bootstrap usage 16416")
        );

        config.max_disk_usage_bytes = Some(4096 * 4 + 32);
        config.validate().unwrap();

        let mut config = DatabaseConfig {
            page_size: 4096,
            external_threshold: 4096,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("external_threshold 4096 exceeds maximum inline primary body size"));

        config.external_threshold = 4096 - 32 - 4 - 2 - 24 - 5;
        config.validate().unwrap();

        let config = DatabaseConfig {
            page_size: 64,
            external_threshold: 1,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("leaves no room for inline primary document bodies"));
    }

    #[test]
    fn validate_rejects_unusable_transaction_config() {
        let config = TransactionConfig {
            max_operations: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("transaction.max_operations must be greater than zero"));

        let config = TransactionConfig {
            max_scanned_docs: 0,
            max_scanned_bytes: 0,
            ..Default::default()
        };
        config.validate().unwrap();
    }

    #[test]
    fn to_storage_config() {
        let c = DatabaseConfig::default();
        let sc = c.to_storage_config();
        assert_eq!(sc.page_size, c.page_size);
        assert_eq!(sc.memory_budget, c.memory_budget);
        assert_eq!(sc.max_disk_usage_bytes, c.max_disk_usage_bytes);
        assert_eq!(sc.wal_segment_size, c.wal_segment_size);
        assert_eq!(sc.wal_retention_max_size, c.wal_retention_max_size);
        assert_eq!(sc.wal_retention_max_age, c.wal_retention_max_age);
    }
}
