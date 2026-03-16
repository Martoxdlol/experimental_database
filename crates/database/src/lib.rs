//! exdb — Layer 6: Database Facade
//!
//! Unified transaction API, catalog management, background tasks, and
//! multi-database support. This is the primary embedded API for applications.
//!
//! # Architecture
//!
//! ```text
//! B8 (SystemDatabase) ──→ B6
//! B7 (Subscription)   ──→ L5
//! B6 (Database)       ──→ B1..B5, B9, B10, L2..L5
//! B5 (Transaction)    ──→ B1..B4, L3..L5
//! B4 (CatalogTracker) ──→ L5 (ReadSet)
//! B3 (IndexResolver)  ──→ B2, L5
//! B2 (CatalogCache)   ──→ L1
//! B1 (Config)
//! B9 (CatalogPersistence)  ──→ B2, L2
//! B10 (CatalogRecovery)    ──→ B2, B9, L2, L3, L5
//! ```
//!
//! # Layer Boundaries
//!
//! - **Depends on:** L1 (Core), L2 (Storage), L3 (DocStore), L4 (Query), L5 (Tx).
//! - **No knowledge of:** L7 (Replication), L8 (Wire Protocol).
//! - L5 defines `IndexResolver` and `CatalogMutationHandler` traits; L6 implements them.

pub mod config;
pub mod error;
pub mod catalog_cache;
pub mod index_resolver;
pub mod catalog_tracker;
pub mod catalog_persistence;
pub mod catalog_mutation_handler;
pub mod catalog_recovery;
pub mod subscription;
pub mod transaction;
pub mod database;
pub mod system_database;

// ─── Public Facade ───

// B1: Config
pub use config::{DatabaseConfig, TransactionConfig};

// Error types
pub use error::{DatabaseError, Result};

// B2: Catalog types
pub use catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

// B5: Transaction
pub use transaction::{Transaction, TransactionOptions, TransactionResult};

// B6: Database
pub use database::Database;

// B7: Subscription
pub use subscription::SubscriptionHandle;

// B8: SystemDatabase
pub use system_database::{DatabaseId, DatabaseMeta, DatabaseState, SystemDatabase};

// Re-export commonly used types from lower layers
pub use exdb_core::field_path::FieldPath;
pub use exdb_core::filter::{Filter, RangeExpr};
pub use exdb_core::types::{CollectionId, DocId, IndexId, Scalar, Ts};
pub use exdb_core::ulid::{decode_ulid, encode_ulid};
pub use exdb_storage::btree::ScanDirection;
pub use exdb_tx::{
    CommitHandle, ReplicationHook, NoReplication, SubscriptionMode,
    InvalidationEvent, ConflictError, ConflictRetry,
};
