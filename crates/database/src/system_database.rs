//! B8: Multi-database registry (SystemDatabase).
//!
//! Manages multiple named databases under a shared data root.
//! Persists durable registry metadata under `_system/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::config::DatabaseConfig;
use crate::database::{Database, DatabaseUsage, IndexReadyEvent};
use crate::error::{DatabaseError, Result};
use exdb_tx::ReplicationHook;

/// Unique identifier for a managed database.
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

/// Lifecycle state of a managed database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseState {
    Active,
    Creating,
    Dropping,
}

const SYSTEM_DIR: &str = "_system";
const REGISTRY_FILE: &str = "registry.json";
const REGISTRY_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegistryManifest {
    version: u32,
    next_id: DatabaseId,
    databases: Vec<PersistedDatabaseMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedDatabaseMeta {
    database_id: DatabaseId,
    name: String,
    path: String,
    created_at: u64,
    config: DatabaseConfig,
    state: DatabaseState,
}

impl From<DatabaseMeta> for PersistedDatabaseMeta {
    fn from(meta: DatabaseMeta) -> Self {
        Self {
            database_id: meta.database_id,
            name: meta.name,
            path: meta.path,
            created_at: meta.created_at,
            config: meta.config,
            state: meta.state,
        }
    }
}

impl From<PersistedDatabaseMeta> for DatabaseMeta {
    fn from(meta: PersistedDatabaseMeta) -> Self {
        Self {
            database_id: meta.database_id,
            name: meta.name,
            path: meta.path,
            created_at: meta.created_at,
            config: meta.config,
            state: meta.state,
        }
    }
}

/// Multi-database registry with durable metadata in a system database.
pub struct SystemDatabase {
    data_root: PathBuf,
    registry_path: PathBuf,
    databases_by_name: RwLock<HashMap<String, DatabaseId>>,
    databases_by_id: RwLock<HashMap<DatabaseId, DatabaseMeta>>,
    open_databases: RwLock<HashMap<DatabaseId, Arc<Database>>>,
    next_id: AtomicU64,
    index_ready_tx: broadcast::Sender<IndexReadyEvent>,
}

impl SystemDatabase {
    fn load_manifest(path: &Path) -> Result<Option<RegistryManifest>> {
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(path)?;
        let manifest: RegistryManifest = serde_json::from_slice(&bytes).map_err(|err| {
            DatabaseError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
        })?;
        if manifest.version != REGISTRY_VERSION {
            return Err(DatabaseError::Storage(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported system registry version {}", manifest.version),
            )));
        }
        Ok(Some(manifest))
    }

    async fn load_registry(&self, manifest: RegistryManifest) -> Result<()> {
        let mut max_id = 0;
        let mut cleaned_transitional_entries = false;
        for persisted in manifest.databases {
            let meta: DatabaseMeta = persisted.into();
            Self::validate_name(&meta.name)?;
            if self
                .databases_by_name
                .read()
                .contains_key(meta.name.as_str())
            {
                return Err(DatabaseError::Storage(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("duplicate database name in registry: {}", meta.name),
                )));
            }
            if self.databases_by_id.read().contains_key(&meta.database_id) {
                return Err(DatabaseError::Storage(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("duplicate database id in registry: {}", meta.database_id),
                )));
            }

            max_id = max_id.max(meta.database_id);
            if matches!(
                meta.state,
                DatabaseState::Creating | DatabaseState::Dropping
            ) {
                Self::remove_database_dir(Path::new(&meta.path))?;
                cleaned_transitional_entries = true;
                continue;
            }

            let db_path = PathBuf::from(&meta.path);
            if !db_path.join("data.db").exists() {
                return Err(DatabaseError::DatabaseNotFound(meta.name));
            }
            let db = Arc::new(
                Database::open_managed(
                    &db_path,
                    meta.name.clone(),
                    meta.config.clone(),
                    None,
                    self.index_ready_tx.clone(),
                )
                .await?,
            );

            self.databases_by_name
                .write()
                .insert(meta.name.clone(), meta.database_id);
            self.databases_by_id
                .write()
                .insert(meta.database_id, meta.clone());
            self.open_databases
                .write()
                .insert(meta.database_id, Arc::clone(&db));
        }
        self.next_id.fetch_max(max_id + 1, Ordering::AcqRel);
        if cleaned_transitional_entries {
            self.persist_registry()?;
        }
        Ok(())
    }

    fn persist_registry(&self) -> Result<()> {
        let mut databases: Vec<PersistedDatabaseMeta> = self
            .databases_by_id
            .read()
            .values()
            .cloned()
            .map(PersistedDatabaseMeta::from)
            .collect();
        databases.sort_by_key(|meta| meta.database_id);
        let manifest = RegistryManifest {
            version: REGISTRY_VERSION,
            next_id: self.next_id.load(Ordering::Acquire),
            databases,
        };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|err| {
            DatabaseError::Storage(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
        })?;
        let tmp_path = self.registry_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, bytes)?;
        std::fs::rename(&tmp_path, &self.registry_path)?;
        Ok(())
    }

    fn remove_database_dir(path: &Path) -> Result<()> {
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    /// Open or create the system database at the given root directory.
    pub async fn open(data_root: impl AsRef<Path>) -> Result<Self> {
        let data_root = data_root.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_root)?;
        let system_dir = data_root.join(SYSTEM_DIR);
        std::fs::create_dir_all(&system_dir)?;
        let registry_path = system_dir.join(REGISTRY_FILE);

        let (index_ready_tx, _) = broadcast::channel(1024);
        let manifest = Self::load_manifest(&registry_path)?;
        let databases_by_name = RwLock::new(HashMap::new());
        let databases_by_id = RwLock::new(HashMap::new());
        let open_databases = RwLock::new(HashMap::new());
        let next_id = AtomicU64::new(manifest.as_ref().map_or(1, |manifest| manifest.next_id));

        let registry = SystemDatabase {
            data_root,
            registry_path,
            databases_by_name,
            databases_by_id,
            open_databases,
            next_id,
            index_ready_tx,
        };
        if let Some(manifest) = manifest {
            registry.load_registry(manifest).await?;
        }
        Ok(registry)
    }

    /// Close the system database and all managed databases.
    pub async fn close(self) -> Result<()> {
        let open: Vec<Arc<Database>> = {
            let mut map = self.open_databases.write();
            map.drain().map(|(_, db)| db).collect()
        };
        for db in open {
            if let Ok(db) = Arc::try_unwrap(db) {
                db.close().await?;
            }
        }
        Ok(())
    }

    /// Get an open database by name.
    pub fn get_database_by_name(&self, name: &str) -> Option<Arc<Database>> {
        let id = self.databases_by_name.read().get(name).copied()?;
        self.open_databases.read().get(&id).cloned()
    }

    /// Get an open database by ID.
    pub fn get_database_by_id(&self, id: DatabaseId) -> Option<Arc<Database>> {
        self.open_databases.read().get(&id).cloned()
    }

    /// List all known databases.
    pub fn list_databases(&self) -> Vec<DatabaseMeta> {
        self.databases_by_id.read().values().cloned().collect()
    }

    /// Return point-in-time usage for an open database by name.
    pub fn database_usage(&self, name: &str) -> Result<DatabaseUsage> {
        let db = self
            .get_database_by_name(name)
            .ok_or_else(|| DatabaseError::DatabaseNotFound(name.to_string()))?;
        Ok(db.usage())
    }

    /// Subscribe to index-ready events from all managed databases.
    pub fn subscribe_index_ready(&self) -> broadcast::Receiver<IndexReadyEvent> {
        self.index_ready_tx.subscribe()
    }

    /// Create a new named database.
    pub async fn create_database(&self, name: &str, config: DatabaseConfig) -> Result<DatabaseId> {
        self.create_database_with_replication(name, config, None)
            .await
    }

    /// Create a new named database with an optional replication hook.
    pub async fn create_database_with_replication(
        &self,
        name: &str,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<DatabaseId> {
        Self::validate_name(name)?;
        config.validate().map_err(DatabaseError::InvalidConfig)?;

        if self.databases_by_name.read().contains_key(name) {
            return Err(DatabaseError::DatabaseAlreadyExists(name.to_string()));
        }

        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        let db_path = self.data_root.join(name);
        if db_path.exists() {
            return Err(DatabaseError::DatabaseAlreadyExists(name.to_string()));
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta = DatabaseMeta {
            database_id: id,
            name: name.to_string(),
            path: db_path.to_string_lossy().to_string(),
            created_at: now_ms,
            config: config.clone(),
            state: DatabaseState::Creating,
        };

        self.databases_by_name.write().insert(name.to_string(), id);
        self.databases_by_id.write().insert(id, meta.clone());
        if let Err(err) = self.persist_registry() {
            self.databases_by_name.write().remove(name);
            self.databases_by_id.write().remove(&id);
            return Err(err);
        }

        let db = match Database::open_managed(
            &db_path,
            name.to_string(),
            config,
            replication,
            self.index_ready_tx.clone(),
        )
        .await
        {
            Ok(db) => Arc::new(db),
            Err(err) => {
                self.databases_by_name.write().remove(name);
                self.databases_by_id.write().remove(&id);
                let _ = self.persist_registry();
                let _ = Self::remove_database_dir(&db_path);
                return Err(err);
            }
        };

        let mut active_meta = meta;
        active_meta.state = DatabaseState::Active;
        self.databases_by_id.write().insert(id, active_meta);
        self.open_databases.write().insert(id, Arc::clone(&db));
        if let Err(err) = self.persist_registry() {
            self.open_databases.write().remove(&id);
            self.databases_by_name.write().remove(name);
            self.databases_by_id.write().remove(&id);
            if let Ok(db) = Arc::try_unwrap(db) {
                let _ = db.close().await;
            }
            let _ = Self::remove_database_dir(&db_path);
            return Err(err);
        }

        Ok(id)
    }

    fn restore_open_database(&self, id: DatabaseId, db: Arc<Database>) {
        self.open_databases.write().insert(id, db);
    }

    fn set_database_state(&self, id: DatabaseId, state: DatabaseState) -> Result<DatabaseMeta> {
        let mut databases = self.databases_by_id.write();
        let meta = databases
            .get_mut(&id)
            .ok_or_else(|| DatabaseError::DatabaseNotFound(id.to_string()))?;
        meta.state = state;
        Ok(meta.clone())
    }

    fn remove_registry_entry(&self, name: &str, id: DatabaseId) {
        self.databases_by_name.write().remove(name);
        self.databases_by_id.write().remove(&id);
    }

    fn restore_registry_entry(&self, meta: DatabaseMeta) {
        self.databases_by_name
            .write()
            .insert(meta.name.clone(), meta.database_id);
        self.databases_by_id.write().insert(meta.database_id, meta);
    }

    async fn close_unshared_database(db: Arc<Database>) -> Result<()> {
        let db = Arc::try_unwrap(db)
            .map_err(|db| DatabaseError::DatabaseInUse(db.name().to_string()))?;
        db.close().await
    }

    /// Register and open an existing restored database path.
    pub async fn register_existing_database(
        &self,
        name: &str,
        config: DatabaseConfig,
    ) -> Result<Arc<Database>> {
        self.register_existing_database_with_replication(name, config, None)
            .await
    }

    /// Register and open an existing restored database path with an optional replication hook.
    pub async fn register_existing_database_with_replication(
        &self,
        name: &str,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Arc<Database>> {
        Self::validate_name(name)?;

        if self.databases_by_name.read().contains_key(name) {
            return Err(DatabaseError::DatabaseAlreadyExists(name.to_string()));
        }

        let db_path = self.data_root.join(name);
        if !db_path.join("data.db").exists() {
            return Err(DatabaseError::DatabaseNotFound(name.to_string()));
        }

        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta = DatabaseMeta {
            database_id: id,
            name: name.to_string(),
            path: db_path.to_string_lossy().to_string(),
            created_at: now_ms,
            config: config.clone(),
            state: DatabaseState::Active,
        };

        let db = Arc::new(
            Database::open_managed(
                &db_path,
                name.to_string(),
                config,
                replication,
                self.index_ready_tx.clone(),
            )
            .await?,
        );

        self.databases_by_name.write().insert(name.to_string(), id);
        self.databases_by_id.write().insert(id, meta);
        self.open_databases.write().insert(id, Arc::clone(&db));
        self.persist_registry()?;

        Ok(db)
    }

    /// Drop a named database.
    pub async fn drop_database(&self, name: &str) -> Result<()> {
        let id = self
            .databases_by_name
            .read()
            .get(name)
            .copied()
            .ok_or_else(|| DatabaseError::DatabaseNotFound(name.to_string()))?;

        let db = self.open_databases.write().remove(&id);
        if let Some(db) = &db
            && Arc::strong_count(db) > 1
        {
            self.restore_open_database(id, Arc::clone(db));
            return Err(DatabaseError::DatabaseInUse(name.to_string()));
        }

        let original_meta = self.set_database_state(id, DatabaseState::Dropping)?;
        if let Err(err) = self.persist_registry() {
            self.set_database_state(id, DatabaseState::Active)?;
            if let Some(db) = db {
                self.restore_open_database(id, db);
            }
            return Err(err);
        }

        if let Some(db) = db {
            Self::close_unshared_database(db).await?;
        }
        Self::remove_database_dir(Path::new(&original_meta.path))?;

        self.remove_registry_entry(name, id);
        if let Err(err) = self.persist_registry() {
            self.restore_registry_entry(DatabaseMeta {
                state: DatabaseState::Dropping,
                ..original_meta
            });
            return Err(err);
        }

        Ok(())
    }

    /// Validate a database name.
    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DatabaseError::InvalidName(
                "name cannot be empty".to_string(),
            ));
        }
        if name.starts_with('_') {
            return Err(DatabaseError::ReservedName(name.to_string()));
        }
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(DatabaseError::InvalidName(
                "name cannot contain path separators or '..'".to_string(),
            ));
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
    use crate::config::TransactionConfig;

    #[tokio::test]
    async fn open_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.list_databases().is_empty());
        assert!(tmp.path().join(SYSTEM_DIR).is_dir());
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn create_and_get_database() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        let id = sys
            .create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();

        assert!(sys.get_database_by_name("mydb").is_some());
        assert!(sys.get_database_by_id(id).is_some());
        assert_eq!(sys.list_databases().len(), 1);

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn reopen_after_close_preserves_database_metadata_and_config() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = DatabaseConfig {
            max_doc_size: 4096,
            transaction: TransactionConfig {
                max_operations: 123,
                ..Default::default()
            },
            ..Default::default()
        };

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let app_id = sys.create_database("app", config.clone()).await.unwrap();
        assert!(sys.get_database_by_name("app").is_some());
        sys.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let databases = sys.list_databases();
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].database_id, app_id);
        assert_eq!(databases[0].name, "app");
        assert_eq!(databases[0].config.max_doc_size, 4096);
        assert_eq!(databases[0].config.transaction.max_operations, 123);
        assert!(sys.get_database_by_name("app").is_some());
        assert!(sys.get_database_by_id(app_id).is_some());

        let logs_id = sys
            .create_database("logs", DatabaseConfig::default())
            .await
            .unwrap();
        assert!(logs_id > app_id);
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn register_existing_database_opens_restored_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("restored");
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        db.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let db = sys
            .register_existing_database("restored", DatabaseConfig::default())
            .await
            .unwrap();

        assert!(sys.get_database_by_name("restored").is_some());
        assert_eq!(db.name(), "restored");
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn register_existing_database_persists_metadata() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("restored");
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        db.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let db = sys
            .register_existing_database("restored", DatabaseConfig::default())
            .await
            .unwrap();
        drop(db);
        sys.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.get_database_by_name("restored").is_some());
        assert_eq!(sys.list_databases().len(), 1);
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn create_duplicate_fails() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();

        let result = sys.create_database("mydb", DatabaseConfig::default()).await;
        assert!(matches!(
            result,
            Err(DatabaseError::DatabaseAlreadyExists(_))
        ));

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn create_rejects_invalid_config_without_registry_entry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let config = DatabaseConfig {
            memory_budget: 1,
            ..Default::default()
        };

        let result = sys.create_database("bad", config).await;

        assert!(matches!(result, Err(DatabaseError::InvalidConfig(_))));
        assert!(sys.get_database_by_name("bad").is_none());
        assert!(sys.list_databases().is_empty());

        sys.close().await.unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.get_database_by_name("bad").is_none());
        assert!(sys.list_databases().is_empty());
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn create_rejects_unregistered_existing_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("restored");
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        db.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        let result = sys
            .create_database("restored", DatabaseConfig::default())
            .await;
        assert!(matches!(
            result,
            Err(DatabaseError::DatabaseAlreadyExists(_))
        ));
        assert!(sys.get_database_by_name("restored").is_none());

        let db = sys
            .register_existing_database("restored", DatabaseConfig::default())
            .await
            .unwrap();
        assert_eq!(db.name(), "restored");
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn database_usage_by_name() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();

        let usage = sys.database_usage("mydb").unwrap();
        assert!(usage.disk_usage_bytes > 0);
        assert!(usage.memory_budget_bytes > 0);

        let missing = sys.database_usage("missing");
        assert!(matches!(missing, Err(DatabaseError::DatabaseNotFound(_))));

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_database() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();
        sys.drop_database("mydb").await.unwrap();

        assert!(sys.get_database_by_name("mydb").is_none());
        assert!(sys.list_databases().is_empty());

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_database_persists_removal_after_reopen() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("keep", DatabaseConfig::default())
            .await
            .unwrap();
        sys.create_database("dropme", DatabaseConfig::default())
            .await
            .unwrap();
        sys.drop_database("dropme").await.unwrap();
        sys.close().await.unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.get_database_by_name("keep").is_some());
        assert!(sys.get_database_by_name("dropme").is_none());
        let names: Vec<String> = sys
            .list_databases()
            .into_iter()
            .map(|meta| meta.name)
            .collect();
        assert_eq!(names, vec!["keep".to_string()]);
        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_database_removes_directory_and_allows_clean_recreate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        let first_id = sys
            .create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();
        let db_path = tmp.path().join("mydb");
        assert!(db_path.join("data.db").exists());

        sys.drop_database("mydb").await.unwrap();
        assert!(!db_path.exists());
        assert!(sys.get_database_by_name("mydb").is_none());

        let second_id = sys
            .create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();
        assert!(second_id > first_id);
        assert!(db_path.join("data.db").exists());

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_database_refuses_live_shared_handle() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();
        let db = sys.get_database_by_name("mydb").unwrap();
        let db_path = tmp.path().join("mydb");

        let result = sys.drop_database("mydb").await;
        assert!(matches!(result, Err(DatabaseError::DatabaseInUse(_))));
        assert!(db_path.join("data.db").exists());
        assert!(sys.get_database_by_name("mydb").is_some());
        assert_eq!(sys.list_databases().len(), 1);

        drop(db);
        sys.drop_database("mydb").await.unwrap();
        assert!(!db_path.exists());

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn open_resumes_transitional_registry_cleanup() {
        let tmp = tempfile::TempDir::new().unwrap();
        let system_dir = tmp.path().join(SYSTEM_DIR);
        std::fs::create_dir_all(&system_dir).unwrap();

        let creating_path = tmp.path().join("creating");
        std::fs::create_dir_all(&creating_path).unwrap();

        let dropping_path = tmp.path().join("dropping");
        let dropping_db = Database::open(&dropping_path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        dropping_db.close().await.unwrap();

        let manifest = RegistryManifest {
            version: REGISTRY_VERSION,
            next_id: 3,
            databases: vec![
                PersistedDatabaseMeta {
                    database_id: 1,
                    name: "creating".to_string(),
                    path: creating_path.to_string_lossy().to_string(),
                    created_at: 1,
                    config: DatabaseConfig::default(),
                    state: DatabaseState::Creating,
                },
                PersistedDatabaseMeta {
                    database_id: 2,
                    name: "dropping".to_string(),
                    path: dropping_path.to_string_lossy().to_string(),
                    created_at: 2,
                    config: DatabaseConfig::default(),
                    state: DatabaseState::Dropping,
                },
            ],
        };
        std::fs::write(
            system_dir.join(REGISTRY_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.list_databases().is_empty());
        assert!(!creating_path.exists());
        assert!(!dropping_path.exists());

        let manifest = SystemDatabase::load_manifest(&system_dir.join(REGISTRY_FILE))
            .unwrap()
            .unwrap();
        assert!(manifest.databases.is_empty());
        assert_eq!(manifest.next_id, 3);

        sys.close().await.unwrap();
    }

    #[tokio::test]
    async fn drop_nonexistent_fails() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        let result = sys.drop_database("nope").await;
        assert!(matches!(result, Err(DatabaseError::DatabaseNotFound(_))));

        sys.close().await.unwrap();
    }

    #[test]
    fn validate_name_reserved() {
        assert!(matches!(
            SystemDatabase::validate_name("_system"),
            Err(DatabaseError::ReservedName(_))
        ));
    }

    #[test]
    fn validate_name_empty() {
        assert!(matches!(
            SystemDatabase::validate_name(""),
            Err(DatabaseError::InvalidName(_))
        ));
    }

    #[test]
    fn validate_name_path_traversal() {
        assert!(matches!(
            SystemDatabase::validate_name("../evil"),
            Err(DatabaseError::InvalidName(_))
        ));
        assert!(matches!(
            SystemDatabase::validate_name("a/b"),
            Err(DatabaseError::InvalidName(_))
        ));
    }

    #[test]
    fn validate_name_ok() {
        assert!(SystemDatabase::validate_name("mydb").is_ok());
        assert!(SystemDatabase::validate_name("test123").is_ok());
    }

    #[tokio::test]
    async fn multiple_databases() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        for i in 0..3 {
            sys.create_database(&format!("db{i}"), DatabaseConfig::default())
                .await
                .unwrap();
        }
        assert_eq!(sys.list_databases().len(), 3);

        sys.close().await.unwrap();
    }
}
