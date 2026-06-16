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
//!
//! # Example
//!
//! ```rust
//! use exdb::{Database, DatabaseConfig, TransactionOptions, TransactionResult};
//! use serde_json::json;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> exdb::Result<()> {
//! let db = Database::open_in_memory(DatabaseConfig::default(), None).await?;
//!
//! let mut schema = db.begin(TransactionOptions::default())?;
//! schema.create_collection("users").await?;
//! assert!(matches!(
//!     schema.commit().await?,
//!     TransactionResult::Success { .. }
//! ));
//!
//! let mut write = db.begin(TransactionOptions::default())?;
//! write.insert("users", json!({"name": "Ada", "age": 37})).await?;
//! assert!(matches!(
//!     write.commit().await?,
//!     TransactionResult::Success { .. }
//! ));
//!
//! let mut read = db.begin(TransactionOptions::readonly())?;
//! let users = read
//!     .query("users", "_created_at", &[], None, None, None)
//!     .await?;
//! assert_eq!(users.len(), 1);
//! read.rollback();
//!
//! db.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog_cache;
pub mod catalog_mutation_handler;
pub mod catalog_persistence;
pub mod catalog_recovery;
pub mod catalog_tracker;
pub mod config;
pub mod database;
pub mod error;
pub mod index_resolver;
pub mod subscription;
pub mod system_database;
pub mod transaction;

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
pub use database::{Database, DatabaseDurableOpenProbe, DatabaseUsage, IndexReadyEvent};

// B7: Subscription
pub use subscription::SubscriptionHandle;

// B8: SystemDatabase
pub use system_database::{DatabaseId, DatabaseMeta, DatabaseState, SystemDatabase};

// Native BSON embedded API helpers
pub use bson::{Bson, Document as BsonDocument};
pub use exdb_core::encoding::{DOC_ID_BINARY_SUBTYPE, bson_bytes, bson_doc_id};

// Re-export commonly used types from lower layers
pub use exdb_core::field_path::FieldPath;
pub use exdb_core::filter::{Filter, RangeExpr};
pub use exdb_core::types::{CollectionId, DocId, IndexId, Scalar, Ts};
pub use exdb_core::ulid::{decode_ulid, encode_ulid};
pub use exdb_storage::btree::ScanDirection;
pub use exdb_storage::engine::{
    IntegrityRepair, IntegrityRepairReport, IntegrityReport, StorageSnapshot,
};
pub use exdb_tx::{
    CommitHandle, ConflictError, ConflictRetry, InvalidationEvent, NoReplication, ReplicationHook,
    SubscriptionMode,
};
