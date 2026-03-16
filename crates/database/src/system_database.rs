//! B8: Multi-database registry (SystemDatabase).
//!
//! Manages multiple named databases under a shared data root.
//! Uses an internal `_system/` database for durable metadata.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::config::DatabaseConfig;
use crate::database::Database;
use crate::error::{DatabaseError, Result};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseState {
    Active,
    Creating,
    Dropping,
}

/// Multi-database registry with durable metadata in a system database.
pub struct SystemDatabase {
    data_root: PathBuf,
    databases_by_name: RwLock<HashMap<String, DatabaseId>>,
    databases_by_id: RwLock<HashMap<DatabaseId, DatabaseMeta>>,
    open_databases: RwLock<HashMap<DatabaseId, Arc<Database>>>,
    next_id: AtomicU64,
}

impl SystemDatabase {
    /// Open or create the system database at the given root directory.
    pub async fn open(data_root: impl AsRef<Path>) -> Result<Self> {
        let data_root = data_root.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_root)?;

        Ok(SystemDatabase {
            data_root,
            databases_by_name: RwLock::new(HashMap::new()),
            databases_by_id: RwLock::new(HashMap::new()),
            open_databases: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        })
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

    /// Create a new named database.
    pub async fn create_database(
        &self,
        name: &str,
        config: DatabaseConfig,
    ) -> Result<DatabaseId> {
        Self::validate_name(name)?;

        if self.databases_by_name.read().contains_key(name) {
            return Err(DatabaseError::DatabaseAlreadyExists(name.to_string()));
        }

        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        let db_path = self.data_root.join(name);

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

        // Open the database
        let db = Database::open(&db_path, config, None).await?;
        let db = Arc::new(db);

        // Register
        self.databases_by_name.write().insert(name.to_string(), id);
        self.databases_by_id.write().insert(id, meta);
        self.open_databases.write().insert(id, db);

        Ok(id)
    }

    /// Drop a named database.
    pub async fn drop_database(&self, name: &str) -> Result<()> {
        let id = self
            .databases_by_name
            .read()
            .get(name)
            .copied()
            .ok_or_else(|| DatabaseError::DatabaseNotFound(name.to_string()))?;

        // Remove and close
        let db = self.open_databases.write().remove(&id);
        self.databases_by_name.write().remove(name);
        self.databases_by_id.write().remove(&id);

        if let Some(db) = db {
            if let Ok(db) = Arc::try_unwrap(db) {
                db.close().await?;
            }
        }

        Ok(())
    }

    /// Validate a database name.
    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(DatabaseError::InvalidName("name cannot be empty".to_string()));
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

    #[tokio::test]
    async fn open_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();
        assert!(sys.list_databases().is_empty());
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
    async fn create_duplicate_fails() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sys = SystemDatabase::open(tmp.path()).await.unwrap();

        sys.create_database("mydb", DatabaseConfig::default())
            .await
            .unwrap();

        let result = sys
            .create_database("mydb", DatabaseConfig::default())
            .await;
        assert!(matches!(
            result,
            Err(DatabaseError::DatabaseAlreadyExists(_))
        ));

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
