# B8: System Database

## Purpose

Multi-database registry. Manages multiple `Database` instances, providing create/drop/list/get operations. Uses an internal `Database` instance (`_system/`) for durable storage of the registry.

Required for the server (L8) but optional for embedded use. A simple embedded application can use `Database::open()` directly without `SystemDatabase`.

## Dependencies

- **B6 (`database.rs`)**: `Database`, `DatabaseConfig`
- **L1 (`exdb-core`)**: `CollectionId` (used as `DatabaseId`)

## Rust Types

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;
use crate::database::Database;
use crate::config::DatabaseConfig;
use crate::error::DatabaseError;

/// Unique identifier for a database.
pub type DatabaseId = u64;

/// Metadata for a managed database.
#[derive(Debug, Clone)]
pub struct DatabaseMeta {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub created_at: u64,
    pub config: DatabaseConfig,
    pub state: DatabaseState,
}

/// Database lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseState {
    /// Database is active and accepting connections.
    Active,
    /// Database is being created (initial setup in progress).
    Creating,
    /// Database is being dropped (cleanup in progress).
    Dropping,
}

/// Multi-database registry.
///
/// Manages the lifecycle of multiple `Database` instances. Uses an internal
/// `Database` instance (`_system/`) for durable storage of database metadata.
///
/// # Thread Safety
///
/// `SystemDatabase` is `Send + Sync`. Database lookups are lock-free after
/// initial load. Create/drop operations are serialized internally.
pub struct SystemDatabase {
    /// Data root directory.
    data_root: PathBuf,

    /// The internal system database (stores registry metadata).
    inner: Database,

    /// In-memory registry (bidirectional).
    databases_by_name: RwLock<HashMap<String, DatabaseId>>,
    databases_by_id: RwLock<HashMap<DatabaseId, DatabaseMeta>>,

    /// Open database handles.
    open_databases: RwLock<HashMap<DatabaseId, Arc<Database>>>,

    /// Next database ID.
    next_id: std::sync::atomic::AtomicU64,
}
```

## Public API

```rust
impl SystemDatabase {
    /// Open the system database at the given data root.
    ///
    /// Creates `_system/` if it doesn't exist. Loads the registry from
    /// the system database's `_databases` collection.
    pub async fn open(data_root: impl AsRef<Path>) -> Result<Self, DatabaseError>;

    /// Close the system database and all managed databases.
    pub async fn close(self) -> Result<(), DatabaseError>;

    // ── Lookup ──

    /// Get a database by name. Returns None if not found or not Active.
    pub fn get_database_by_name(&self, name: &str) -> Option<Arc<Database>>;

    /// Get a database by ID. Returns None if not found or not Active.
    pub fn get_database_by_id(&self, id: DatabaseId) -> Option<Arc<Database>>;

    /// List all databases (includes non-Active states for admin use).
    pub fn list_databases(&self) -> Vec<DatabaseMeta>;

    // ── Lifecycle ──

    /// Create a new database.
    ///
    /// 1. Validate name (no reserved names, no path traversal).
    /// 2. Insert metadata into system database.
    /// 3. Create directory + open Database instance.
    /// 4. Update in-memory registry.
    pub async fn create_database(
        &self,
        name: &str,
        config: DatabaseConfig,
    ) -> Result<DatabaseId, DatabaseError>;

    /// Drop a database.
    ///
    /// 1. Mark as Dropping in system database.
    /// 2. Close the Database instance.
    /// 3. Remove data directory.
    /// 4. Remove metadata from system database.
    /// 5. Update in-memory registry.
    pub async fn drop_database(&self, name: &str) -> Result<(), DatabaseError>;

    /// Get or open a database by name.
    ///
    /// If the database exists in the registry but is not currently open,
    /// opens it and caches the handle.
    pub async fn open_database(&self, name: &str) -> Result<Arc<Database>, DatabaseError>;
}
```

## Reserved Names

- `_system` — the internal system database
- Names starting with `_` are reserved for internal use
- Names containing `/`, `\`, `..`, or null bytes are rejected

## Implementation Notes

- The `_system/` database stores a `_databases` collection with one document per managed database.
- On startup, `SystemDatabase::open` scans the `_databases` collection to populate the in-memory registry but does NOT eagerly open all databases. Databases are opened on first access.
- `Arc<Database>` is used for the open database handles so they can be shared across sessions in L8.
- The `Dropping` state is used to handle crash-during-drop: on startup, if a database is in `Dropping` state, the cleanup is resumed.

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `open_creates_system_dir` | System database created on first open |
| 2 | `create_and_list_databases` | Created database appears in list |
| 3 | `get_database_by_name` | Lookup returns correct database |
| 4 | `get_database_by_id` | ID lookup returns correct database |
| 5 | `drop_database_removes_from_list` | Dropped database not in list |
| 6 | `create_duplicate_name_errors` | Same name twice → error |
| 7 | `reserved_name_rejected` | Names starting with `_` rejected |
| 8 | `path_traversal_rejected` | Names with `..` rejected |
| 9 | `close_closes_all_databases` | All databases closed on system close |
| 10 | `lazy_open_on_first_access` | Database opened only when accessed |
| 11 | `reopen_after_close` | Close + reopen preserves databases |
| 12 | `drop_during_creating_state` | Handles crash-during-create cleanup |
