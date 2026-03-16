//! B6: Database — the core integration point.
//!
//! Owns all components (storage engine, indexes, catalog cache, commit system)
//! and provides the lifecycle API plus the transaction entry point.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use exdb_core::types::{CollectionId, IndexId, Ts};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::{StorageConfig, StorageEngine};
use exdb_tx::{
    CommitCoordinator, CommitHandle, NoReplication, ReplicationHook,
    SubscriptionRegistry,
};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use crate::catalog_cache::{CatalogCache, CollectionMeta};
use crate::catalog_mutation_handler::CatalogMutationHandlerImpl;
use crate::catalog_recovery::{apply_recovered_catalog, CatalogRecoveryHandler};
use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::index_resolver::IndexResolverImpl;
use crate::transaction::{Transaction, TransactionContext, TransactionOptions};

/// A database instance — the primary public API.
///
/// Fully functional without networking. Composes L1–L5 into a single
/// operational unit with collection management, transactions, and
/// background maintenance.
///
/// # Thread Safety
///
/// `Database` is `Send + Sync`. Multiple tasks can call `begin()` concurrently.
pub struct Database {
    name: String,
    config: DatabaseConfig,

    // Shared context for transactions
    tx_context: TransactionContext,

    // Layer 2: Storage
    storage: Arc<StorageEngine>,

    // Layer 5: Transactions
    commit_handle: CommitHandle,

    // State
    active_tx_count: Arc<AtomicU64>,
    shutdown: CancellationToken,
}

impl Database {
    /// Open a file-backed database at the given path.
    ///
    /// Performs the full startup sequence:
    /// 1. `StorageEngine::open` (DWB restore + WAL replay)
    /// 2. Recover catalog from WAL replay
    /// 3. Open primary + secondary B-tree handles
    /// 4. Create `CommitCoordinator` + `ReplicationRunner` + `CommitHandle`
    /// 5. Spawn background tasks
    ///
    /// If the path doesn't exist, a new empty database is created.
    pub async fn open(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let storage_config = StorageConfig {
            page_size: config.page_size,
            memory_budget: config.memory_budget,
            wal_segment_size: config.wal_segment_size,
            checkpoint_wal_threshold: config.checkpoint_wal_threshold,
            checkpoint_interval: config.checkpoint_interval,
        };

        // WAL replay handler collects catalog mutations and tracks timestamps
        let mut handler = CatalogRecoveryHandler::new();

        let storage = Arc::new(
            StorageEngine::open(path, storage_config, &mut handler)
                .await
                .map_err(DatabaseError::Storage)?,
        );

        // Read file header for persisted catalog state
        let file_header = storage.file_header().await;
        let recovered_ts = handler.recovered_ts.max(file_header.visible_ts.get());
        let visible_ts = handler.visible_ts.max(file_header.visible_ts.get());

        // Rebuild catalog from recovered WAL mutations
        let mut catalog = CatalogCache::new();
        let mut primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>> = HashMap::new();
        let mut secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>> = HashMap::new();

        apply_recovered_catalog(
            &mut catalog,
            &handler.catalog_mutations,
            &storage,
            &mut primary_indexes,
            &mut secondary_indexes,
        )
        .await
        .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;

        // Derive database name from path
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("default")
            .to_string();

        Self::init_with_state(
            name,
            config,
            storage,
            catalog,
            primary_indexes,
            secondary_indexes,
            recovered_ts,
            visible_ts,
            replication,
        )
        .await
    }

    /// Open an ephemeral in-memory database.
    ///
    /// No files, no durability. Useful for testing, caching, temporary data.
    pub async fn open_in_memory(
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let storage_config = StorageConfig {
            page_size: config.page_size,
            memory_budget: config.memory_budget,
            ..StorageConfig::default()
        };

        let storage = Arc::new(
            StorageEngine::open_in_memory(storage_config)
                .await
                .map_err(DatabaseError::Storage)?,
        );

        Self::init_with_state(
            "default".to_string(),
            config,
            storage,
            CatalogCache::new(),
            HashMap::new(),
            HashMap::new(),
            0, // initial_ts
            0, // visible_ts
            replication,
        )
        .await
    }

    /// Internal initialization with pre-built catalog state.
    async fn init_with_state(
        name: String,
        config: DatabaseConfig,
        storage: Arc<StorageEngine>,
        catalog: CatalogCache,
        primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
        initial_ts: Ts,
        visible_ts: Ts,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let catalog = Arc::new(RwLock::new(catalog));
        let primary_indexes = Arc::new(RwLock::new(primary_indexes));
        let secondary_indexes = Arc::new(RwLock::new(secondary_indexes));

        let index_resolver = Arc::new(IndexResolverImpl::new(Arc::clone(&catalog)));

        let catalog_handler = Arc::new(CatalogMutationHandlerImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&primary_indexes),
            Arc::clone(&secondary_indexes),
        ));

        let replication_hook: Box<dyn ReplicationHook> =
            replication.unwrap_or_else(|| Box::new(NoReplication));

        let (mut coordinator, mut runner, commit_handle) = CommitCoordinator::new(
            initial_ts,
            visible_ts,
            Arc::clone(&storage),
            Arc::clone(&primary_indexes),
            Arc::clone(&secondary_indexes),
            replication_hook,
            index_resolver,
            catalog_handler,
            256,
            256,
        );

        let shutdown = CancellationToken::new();

        // Spawn the commit coordinator on a LocalSet (!Send)
        let shutdown_coord = shutdown.clone();
        tokio::task::spawn_local(async move {
            tokio::select! {
                _ = coordinator.run() => {}
                _ = shutdown_coord.cancelled() => {}
            }
        });

        // Spawn the replication runner
        let shutdown_repl = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = runner.run() => {}
                _ = shutdown_repl.cancelled() => {}
            }
        });

        let tx_context = TransactionContext {
            catalog: Arc::clone(&catalog),
            primary_indexes: Arc::clone(&primary_indexes),
            secondary_indexes: Arc::clone(&secondary_indexes),
            commit_handle: commit_handle.clone(),
            tx_config: config.transaction.clone(),
            max_doc_size: config.max_doc_size,
        };

        Ok(Self {
            name,
            config,
            tx_context,
            storage,
            commit_handle,
            active_tx_count: Arc::new(AtomicU64::new(0)),
            shutdown,
        })
    }

    /// Begin a transaction.
    ///
    /// - `begin_ts` is set to `visible_ts` (latest committed + replicated timestamp).
    /// - `tx_id` is allocated atomically.
    /// - `wall_clock_ms` is captured for `_created_at`.
    pub fn begin(&self, opts: TransactionOptions) -> Result<Transaction<'_>> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        let tx_id = self.commit_handle.allocate_tx_id();
        let begin_ts = self.commit_handle.visible_ts();
        let wall_clock_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.active_tx_count.fetch_add(1, Ordering::Relaxed);

        Ok(Transaction::new(
            &self.tx_context,
            tx_id,
            opts,
            begin_ts,
            wall_clock_ms,
        ))
    }

    /// Close the database.
    ///
    /// 1. Signal shutdown to all background tasks
    /// 2. Wait for active transactions (with timeout)
    /// 3. Close storage engine (flush + checkpoint)
    pub async fn close(self) -> Result<()> {
        self.shutdown.cancel();

        // Wait for active transactions (with timeout)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);
        while self.active_tx_count.load(Ordering::Relaxed) > 0 {
            if start.elapsed() > timeout {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Close storage engine (includes final checkpoint for durable storage)
        self.storage.close().await.map_err(DatabaseError::Storage)?;

        Ok(())
    }

    // ── Read-Only Accessors ──

    /// List all collections (convenience, outside transactions).
    ///
    /// Does NOT record catalog reads for OCC. For transactional consistency,
    /// use `Transaction::list_collections()`.
    pub fn list_collections(&self) -> Vec<CollectionMeta> {
        self.tx_context
            .catalog
            .read()
            .list_collections()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Get a collection by name (convenience, outside transactions).
    pub fn get_collection(&self, name: &str) -> Option<CollectionMeta> {
        self.tx_context.catalog.read().get_collection_by_name(name).cloned()
    }

    /// Get the subscription registry (for L7/L8 push notifications).
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>> {
        self.commit_handle.subscriptions()
    }

    /// Get the storage engine (for L7 WAL access).
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Get the commit handle (for advanced L7/L8 use).
    pub fn commit_handle(&self) -> &CommitHandle {
        &self.commit_handle
    }

    /// Get the database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Check if the database is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    // ── Internal accessors for Transaction ──

    #[allow(dead_code)]
    pub(crate) fn catalog(&self) -> &Arc<RwLock<CatalogCache>> {
        &self.tx_context.catalog
    }

    #[allow(dead_code)]
    pub(crate) fn primary_indexes(
        &self,
    ) -> &Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>> {
        &self.tx_context.primary_indexes
    }

    #[allow(dead_code)]
    pub(crate) fn secondary_indexes(
        &self,
    ) -> &Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>> {
        &self.tx_context.secondary_indexes
    }

    #[allow(dead_code)]
    pub(crate) fn transaction_config(&self) -> &crate::config::TransactionConfig {
        &self.tx_context.tx_config
    }
}
