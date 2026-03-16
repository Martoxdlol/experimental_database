//! exdb — Layer 6: Database Instance
//!
//! Primary public API for the embedded database. Composes all lower
//! layers into a usable `Database` struct with collection management,
//! transaction sessions, and catalog caching.
//!
//! This is the crate that downstream users depend on.

pub mod config;
pub mod catalog_cache;
pub mod index_resolver;
pub mod catalog_tracker;
pub mod catalog_recovery;
pub mod catalog_mutation_handler;
pub mod catalog_persistence;
pub mod transaction;
pub mod database;
pub mod subscription;
pub mod error;

// ── Public Facade ──

// Config
pub use config::{DatabaseConfig, TransactionConfig};

// Catalog
pub use catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

// Transaction API
pub use transaction::{Transaction, TransactionOptions, TransactionResult};

// Database lifecycle
pub use database::Database;

// Subscription
pub use subscription::SubscriptionHandle;

// Errors
pub use error::{DatabaseError, Result};

// Re-exports from lower layers (convenience)
pub use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
pub use exdb_core::field_path::FieldPath;
pub use exdb_core::filter::{Filter, RangeExpr};
pub use exdb_tx::{
    SubscriptionMode, InvalidationEvent, ChainContinuation,
    ConflictRetry, ReplicationHook, NoReplication,
};
pub use exdb_storage::btree::ScanDirection;
