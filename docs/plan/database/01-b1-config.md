# B1: Configuration

## Purpose

Database and transaction configuration with sensible defaults. Pure data types — no I/O, no dependencies beyond L1 types.

## Dependencies

- **L1 (`exdb-core`)**: `Ts` type (for timeout representations)

## Rust Types

```rust
use std::time::Duration;

/// Database-level configuration.
///
/// Controls storage, memory, and document size limits. Passed to
/// `Database::open()` and `Database::open_in_memory()`.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Page size in bytes. Must match the storage engine.
    /// Default: 8192.
    pub page_size: usize,

    /// Total memory budget for the buffer pool.
    /// Default: 256 MB.
    pub memory_budget: usize,

    /// Optional maximum retained disk usage in bytes.
    /// Includes page-store bytes plus retained WAL bytes during normal writes.
    /// Default: None (unlimited).
    pub max_disk_usage_bytes: Option<u64>,

    /// Maximum document size in bytes (binary-encoded).
    /// Documents exceeding this are rejected at insert/replace/patch.
    /// Default: 16 MB (DESIGN.md 1.5).
    pub max_doc_size: usize,

    /// Documents larger than this are stored externally on the heap
    /// rather than inline in the B-tree leaf cell.
    /// Default: page_size / 4.
    pub external_threshold: usize,

    /// WAL segment size in bytes.
    /// Default: 64 MB.
    pub wal_segment_size: usize,

    /// Number of WAL bytes since last checkpoint before triggering a new one.
    /// Default: 64 MB.
    pub checkpoint_wal_threshold: usize,

    /// Time between periodic checkpoint attempts.
    /// Default: 60 seconds.
    pub checkpoint_interval: Duration,

    /// Vacuum interval.
    /// Default: 300 seconds.
    pub vacuum_interval: Duration,

    /// Transaction configuration.
    pub transaction: TransactionConfig,
}

/// Transaction-scoped limits.
///
/// Prevents runaway transactions from consuming unbounded resources
/// (DESIGN.md 5.6.5, 5.6.6).
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum time a transaction can remain open without client activity.
    /// Default: 30 seconds.
    pub idle_timeout: Duration,

    /// Maximum total lifetime of a transaction.
    /// Default: 5 minutes.
    pub max_lifetime: Duration,

    /// Maximum number of read intervals across all indexes.
    /// Default: 4,096.
    pub max_intervals: usize,

    /// Maximum coarse work units across public transaction operations and
    /// scanned index rows.
    /// Default: 100,000.
    pub max_operations: usize,

    /// Maximum total bytes read from index + primary B-trees.
    /// Default: 64 MB.
    pub max_scanned_bytes: usize,

    /// Maximum documents scanned (including filtered out).
    /// Default: 100,000.
    pub max_scanned_docs: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: 8192,
            memory_budget: 256 * 1024 * 1024,
            max_disk_usage_bytes: None,
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
            max_operations: 100_000,
            max_scanned_bytes: 64 * 1024 * 1024,
            max_scanned_docs: 100_000,
        }
    }
}
```

## Validation

`DatabaseConfig::validate()` rejects unusable storage settings before a
database is opened, restored from snapshot, or persisted in the system
registry. Validation covers the storage engine constraints (`page_size`,
`memory_budget`, `max_disk_usage_bytes`, WAL retention size) plus database
level limits such as nonzero document size, external-storage threshold, WAL
segment size, checkpoint threshold, checkpoint interval, and vacuum interval.

`TransactionConfig::validate()` rejects zero timeout/work-accounting values
for `idle_timeout`, `max_lifetime`, `max_intervals`, and `max_operations`.
`max_operations` also accounts for bounded internal work such as secondary
scan rows, pending write-set merge rows, pending create-index validation, and
per-index document validation during writes, plus cascade-index metadata work
during collection drops, committed catalog result rows, and pending
catalog-overlay work during list operations, collection resolution, and DDL
duplicate checks.
Zero `max_scanned_bytes` and `max_scanned_docs` remain valid as restrictive
limits that allow a deployment to forbid scans.

L8 and `exdb-server` call the same validation when parsing JSON
`default_database_config` or create-database overrides, so invalid resource
settings fail as configuration/client errors before partial database metadata
is persisted.

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `default_database_config` | All defaults match DESIGN.md values |
| 2 | `default_transaction_config` | All defaults match DESIGN.md 5.6.5, 5.6.6 |
| 3 | `custom_config` | Fields can be overridden and round-trip through clone |
| 4 | `external_threshold_default` | Defaults to page_size / 4 |
