//! B6: Database facade — the main entry point for embedded usage.
//!
//! Owns the storage engine, catalog, indexes, commit handle, and background
//! tasks. Provides `begin()` to create transactions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use exdb_core::types::{CollectionId, IndexId};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::StorageEngine;
use exdb_tx::{
    CommitCoordinator, CommitHandle, NoReplication, ReplicationHook,
    SubscriptionRegistry,
};
use parking_lot::RwLock;
use tokio_util::task::LocalPoolHandle;

use crate::catalog_cache::CatalogCache;
use crate::catalog_mutation_handler::CatalogMutationHandlerImpl;
use crate::catalog_recovery::DatabaseRecoveryHandler;
use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::index_resolver::IndexResolverImpl;
use crate::transaction::{Transaction, TransactionOptions};

/// The main database struct — owns all state and background tasks.
pub struct Database {
    name: String,
    config: DatabaseConfig,
    #[allow(dead_code)]
    path: Option<PathBuf>,

    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    commit_handle: CommitHandle,
    catalog: Arc<RwLock<CatalogCache>>,
    active_tx_count: Arc<AtomicU64>,
    shutdown: tokio_util::sync::CancellationToken,

    // Background task handles
    _coordinator_pool: LocalPoolHandle,
    _runner_handle: Option<tokio::task::JoinHandle<()>>,
    _checkpoint_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Database {
    /// Open a file-backed database at the given path.
    pub async fn open(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let storage_config = config.to_storage_config();

        // Create storage engine with recovery handler
        let storage = Arc::new(
            StorageEngine::open(&path, storage_config, &mut exdb_storage::recovery::NoOpHandler)
                .await?,
        );

        // Run L6 recovery
        let recovery_handler =
            DatabaseRecoveryHandler::new(Arc::clone(&storage)).await?;

        // WAL replay (records after checkpoint are already replayed by StorageEngine::open,
        // but we need to re-open and replay for L6 state. For file-backed, the WAL was already
        // replayed by the storage engine — DatabaseRecoveryHandler picks up from catalog B-trees.)

        let state = recovery_handler.into_state();

        Self::from_recovered_state(
            "default".to_string(),
            config,
            Some(path),
            storage,
            state.catalog,
            state.primary_indexes,
            state.secondary_indexes,
            state.recovered_ts,
            state.visible_ts,
            replication,
        )
        .await
    }

    /// Open an in-memory database (ephemeral, for testing).
    pub async fn open_in_memory(
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let storage_config = config.to_storage_config();
        let storage = Arc::new(
            StorageEngine::open_in_memory(storage_config).await?,
        );

        let fh = storage.file_header().await;
        let catalog = CatalogCache::new(
            fh.next_collection_id.get(),
            fh.next_index_id.get(),
        );

        Self::from_recovered_state(
            "default".to_string(),
            config,
            None,
            storage,
            catalog,
            HashMap::new(),
            HashMap::new(),
            0,
            0,
            replication,
        )
        .await
    }

    /// Build a Database from recovered state (shared between open and open_in_memory).
    async fn from_recovered_state(
        name: String,
        config: DatabaseConfig,
        path: Option<PathBuf>,
        storage: Arc<StorageEngine>,
        catalog: CatalogCache,
        primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
        recovered_ts: u64,
        visible_ts: u64,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        let catalog = Arc::new(RwLock::new(catalog));
        let primary_indexes = Arc::new(RwLock::new(primary_indexes));
        let secondary_indexes = Arc::new(RwLock::new(secondary_indexes));

        // Create index resolver
        let index_resolver = Arc::new(IndexResolverImpl::new(Arc::clone(&catalog)));

        // Create catalog mutation handler
        let fh = storage.file_header().await;
        let catalog_id_btree = storage.open_btree(fh.catalog_root_page.get());
        let catalog_name_btree = storage.open_btree(fh.catalog_name_root_page.get());
        let catalog_handler = Arc::new(CatalogMutationHandlerImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&primary_indexes),
            Arc::clone(&secondary_indexes),
            catalog_id_btree,
            catalog_name_btree,
        ));

        // Create commit coordinator
        let replication_hook: Box<dyn ReplicationHook> =
            replication.unwrap_or_else(|| Box::new(NoReplication));

        let (mut coordinator, mut runner, commit_handle) = CommitCoordinator::new(
            recovered_ts,
            visible_ts,
            Arc::clone(&storage),
            Arc::clone(&primary_indexes),
            Arc::clone(&secondary_indexes),
            replication_hook,
            index_resolver,
            catalog_handler,
            256,  // channel_size
            256,  // replication_queue_size
        );

        // Spawn coordinator on LocalSet (CommitCoordinator is !Send)
        let coordinator_pool = LocalPoolHandle::new(1);
        coordinator_pool.spawn_pinned(move || async move {
            coordinator.run().await;
        });

        // Spawn replication runner
        let runner_handle = tokio::spawn(async move {
            runner.run().await;
        });

        // Spawn checkpoint task
        let checkpoint_storage = Arc::clone(&storage);
        let checkpoint_interval = config.checkpoint_interval;
        let shutdown = tokio_util::sync::CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let checkpoint_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(checkpoint_interval) => {
                        if let Err(e) = checkpoint_storage.checkpoint().await {
                            tracing::error!("checkpoint failed: {e}");
                        }
                    }
                    _ = shutdown_clone.cancelled() => break,
                }
            }
        });

        Ok(Database {
            name,
            config,
            path,
            storage,
            primary_indexes,
            secondary_indexes,
            commit_handle,
            catalog,
            active_tx_count: Arc::new(AtomicU64::new(0)),
            shutdown,
            _coordinator_pool: coordinator_pool,
            _runner_handle: Some(runner_handle),
            _checkpoint_handle: Some(checkpoint_handle),
        })
    }

    /// Close the database gracefully.
    pub async fn close(self) -> Result<()> {
        // Signal shutdown
        self.shutdown.cancel();

        // Wait for active transactions to finish (with timeout)
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while self.active_tx_count.load(Ordering::Acquire) > 0 {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!("close timeout: {} active transactions remaining",
                    self.active_tx_count.load(Ordering::Acquire));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Final checkpoint + close storage
        self.storage.close().await?;

        Ok(())
    }

    // ─── Transaction Entry Point ───

    /// Begin a new transaction.
    pub fn begin(&self, opts: TransactionOptions) -> Result<Transaction<'_>> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        let tx_id = self.commit_handle.allocate_tx_id();
        let begin_ts = self.commit_handle.visible_ts();

        self.active_tx_count.fetch_add(1, Ordering::AcqRel);

        Ok(Transaction::new(
            &self.commit_handle,
            &self.catalog,
            &self.primary_indexes,
            &self.secondary_indexes,
            &self.config.transaction,
            tx_id,
            begin_ts,
            opts,
        ))
    }

    // ─── Accessors ───

    /// Name of this database.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a snapshot of all collections.
    pub fn list_collections(&self) -> Vec<crate::catalog_cache::CollectionMeta> {
        self.catalog.read().list_collections()
    }

    /// Get a collection by name.
    pub fn get_collection(
        &self,
        name: &str,
    ) -> Option<crate::catalog_cache::CollectionMeta> {
        self.catalog.read().get_collection_by_name(name).cloned()
    }

    /// Access the subscription registry.
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>> {
        self.commit_handle.subscriptions()
    }

    /// Access the storage engine (for advanced operations).
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Access the commit handle.
    pub fn commit_handle(&self) -> &CommitHandle {
        &self.commit_handle
    }

    /// Database config.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }
}
