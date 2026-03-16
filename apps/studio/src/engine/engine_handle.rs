//! Unified engine handle — opens a Database (L6) and derives a StorageHandle
//! for low-level internals access.

use std::path::Path;
use std::sync::Arc;

use exdb::{Database, DatabaseConfig};

use super::l6_handle::L6Handle;
use super::storage_handle::StorageHandle;

/// Combined handle that provides both L6 (high-level) and L2-L4 (low-level) access.
pub struct EngineHandle {
    pub l6: L6Handle,
    pub storage: Arc<StorageHandle>,
    pub path: String,
}

#[allow(dead_code)]
impl EngineHandle {
    /// Open a database via L6 and derive a storage handle for internals.
    pub async fn open(path: &Path) -> Result<Self, String> {
        let config = DatabaseConfig::default();
        let page_size = config.page_size;
        let db = Database::open(path, config, None)
            .await
            .map_err(|e| format!("Failed to open database: {e}"))?;
        let db = Arc::new(db);

        let storage = Arc::new(StorageHandle::from_engine(
            Arc::clone(db.storage()),
            &path.display().to_string(),
            page_size,
        ));
        let l6 = L6Handle::new(Arc::clone(&db));

        Ok(EngineHandle {
            l6,
            storage,
            path: path.display().to_string(),
        })
    }

    /// Close the database gracefully.
    pub async fn close(self) -> Result<(), String> {
        let db = Arc::try_unwrap(self.l6.database().clone())
            .map_err(|_| "Cannot close: other references to database exist".to_string())?;
        db.close().await.map_err(|e| format!("Close failed: {e}"))
    }
}
