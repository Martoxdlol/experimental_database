# B8: System Database

## Purpose

Multi-database registry. Manages multiple `Database` instances, providing create/drop/list/get operations. The design target is an internal `Database` instance (`_system/`) for durable storage of the registry; the current implementation persists a versioned registry manifest at `_system/registry.json`.

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
/// Manages the lifecycle of multiple `Database` instances. The implementation
/// persists database metadata in `_system/registry.json`.
///
/// # Thread Safety
///
/// `SystemDatabase` is `Send + Sync`. Database lookups are lock-free after
/// initial load. Create/drop operations are serialized internally.
pub struct SystemDatabase {
    /// Data root directory.
    data_root: PathBuf,

    /// Durable registry manifest path.
    registry_path: PathBuf,

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
    /// Creates `_system/` if it doesn't exist. Loads the registry from the
    /// versioned registry manifest and eagerly opens active databases.
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
    /// 2. Create directory + open Database instance.
    /// 3. Update in-memory registry.
    /// 4. Persist registry manifest.
    pub async fn create_database(
        &self,
        name: &str,
        config: DatabaseConfig,
    ) -> Result<DatabaseId, DatabaseError>;

    /// Drop a database.
    ///
    /// 1. Persist Dropping state in the registry manifest.
    /// 2. Close the Database instance if no shared handles remain.
    /// 3. Remove the data directory.
    /// 4. Remove metadata from the registry manifest.
    pub async fn drop_database(&self, name: &str) -> Result<(), DatabaseError>;

    /// Register and open an existing restored database path under the data root.
    pub async fn register_existing_database(
        &self,
        name: &str,
        config: DatabaseConfig,
    ) -> Result<Arc<Database>, DatabaseError>;

    /// Register and open an existing restored database path under the data root
    /// with an optional replication hook.
    pub async fn register_existing_database_with_replication(
        &self,
        name: &str,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Arc<Database>, DatabaseError>;
}
```

## Reserved Names

- `_system` — the internal system database
- Names starting with `_` are reserved for internal use
- Names containing `/`, `\`, `..`, or null bytes are rejected

## Implementation Notes

- The current `_system/` directory stores `registry.json`, a versioned manifest with one entry per managed database.
- On startup, `SystemDatabase::open` reads `registry.json`, validates duplicate IDs/names, cleans up `Creating` and `Dropping` transitional entries, rewrites the manifest without those entries, and eagerly opens active databases so synchronous lookup APIs can return ready handles.
- `Arc<Database>` is used for the open database handles so they can be shared across sessions in L8.
- Create persists `Creating` before opening a new database and promotes the entry to `Active` after success. It rejects unregistered pre-existing paths; restored or pre-existing paths must use explicit registration. Drop persists `Dropping`, rejects the operation with `DatabaseInUse` when external handles still exist, closes the owned handle, removes the data directory, and then removes the registry entry.
- The original internal `_system` database and `_databases` collection design remains a possible future replacement if registry metadata needs transactional catalog semantics.

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
| 10 | `open_eagerly_loads_active_databases` | Active registry entries are opened during system startup |
| 11 | `reopen_after_close` | Close + reopen preserves databases |
| 12 | `drop_during_creating_state` | Handles crash-during-create cleanup |
| 13 | `reopen_after_close_preserves_database_metadata_and_config` | Manifest persists ID, name, and config |
| 14 | `register_existing_database_persists_metadata` | Restored database registration survives reopen |
| 15 | `drop_database_persists_removal_after_reopen` | Dropped registry entries stay removed |
| 16 | `drop_database_removes_directory_and_allows_clean_recreate` | Drop removes data directory and clean recreate works |
| 17 | `drop_database_refuses_live_shared_handle` | Drop refuses live external database handles |
| 18 | `open_resumes_transitional_registry_cleanup` | Startup cleans `Creating`/`Dropping` crash states |
| 19 | `create_rejects_unregistered_existing_path` | Existing paths must be registered explicitly |
