//! B6: Database facade — the main entry point for embedded usage.
//!
//! Owns the storage engine, catalog, indexes, commit handle, and background
//! tasks. Provides `begin()` to create transactions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use exdb_core::types::{CollectionId, IndexId};
use exdb_docstore::{IndexBuilder, PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::StorageEngine;
use exdb_storage::wal::WAL_RECORD_INDEX_READY;
use exdb_tx::{
    CommitCoordinator, CommitHandle, NoReplication, ReplicationHook,
    SubscriptionRegistry,
};
use parking_lot::RwLock;
use tokio_util::task::LocalPoolHandle;

use crate::catalog_cache::{CatalogCache, IndexMeta, IndexState};
use crate::catalog_mutation_handler::CatalogMutationHandlerImpl;
use crate::catalog_persistence::CatalogPersistence;
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
    _index_builder_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Database {
    /// Open a file-backed database at the given path.
    ///
    /// Performs two-phase recovery:
    /// 1. L2 physical recovery (DWB torn page restoration) via `StorageEngine::open`
    /// 2. L6 logical recovery: replay WAL records from `checkpoint_lsn` to rebuild
    ///    catalog, primary/secondary indexes, and timestamp state.
    pub async fn open(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        use tokio_stream::StreamExt;

        let path = path.as_ref().to_path_buf();
        let storage_config = config.to_storage_config();

        // Phase 1: L2 physical recovery (DWB, page-level redo).
        // We pass NoOpHandler because L6 recovery is done separately below
        // (DatabaseRecoveryHandler is !Send and can't implement WalRecordHandler).
        let storage = Arc::new(
            StorageEngine::open(&path, storage_config, &mut exdb_storage::recovery::NoOpHandler)
                .await?,
        );

        // Phase 2: L6 logical recovery.
        // Load initial state from catalog B-trees (checkpointed state).
        let mut recovery_handler =
            DatabaseRecoveryHandler::new(Arc::clone(&storage)).await?;

        // Replay WAL records from checkpoint_lsn to rebuild any mutations
        // that were committed after the last checkpoint but before the crash.
        let checkpoint_lsn = storage.file_header().await.checkpoint_lsn.get();
        let mut wal_stream = storage.read_wal_from(checkpoint_lsn);
        while let Some(result) = wal_stream.next().await {
            let record = result?;
            recovery_handler.handle_record(&record).await?;
        }

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
        mut catalog: CatalogCache,
        primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>>,
        mut secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>>,
        recovered_ts: u64,
        visible_ts: u64,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        // ── Drop Building indexes from prior crash (D3) ──
        // Building indexes are incomplete — drop them so users can recreate.
        // Partial B-tree pages are orphaned but harmless.
        let building = catalog.building_indexes();
        if !building.is_empty() {
            let fh = storage.file_header().await;
            let id_btree = storage.open_btree(fh.catalog_root_page.get());
            let name_btree = storage.open_btree(fh.catalog_name_root_page.get());
            for idx in &building {
                tracing::info!(
                    "dropping incomplete Building index {:?} ({}) on collection {:?}",
                    idx.index_id, idx.name, idx.collection_id,
                );
                secondary_indexes.remove(&idx.index_id);
                CatalogPersistence::apply_drop_index(
                    &id_btree,
                    &name_btree,
                    &mut catalog,
                    idx.index_id,
                )
                .await?;
            }
        }

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
            256,
            256,
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

        // Spawn index builder task on LocalSet (B-tree ops hold parking_lot
        // guards across .await, same as CommitCoordinator)
        let builder_storage = Arc::clone(&storage);
        let builder_catalog = Arc::clone(&catalog);
        let builder_primaries = Arc::clone(&primary_indexes);
        let builder_secondaries = Arc::clone(&secondary_indexes);
        let builder_commit = commit_handle.clone();
        let builder_shutdown = shutdown.clone();
        let builder_handle = coordinator_pool.spawn_pinned(move || {
            Self::index_builder_loop(
                builder_storage,
                builder_catalog,
                builder_primaries,
                builder_secondaries,
                builder_commit,
                builder_shutdown,
            )
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
            _index_builder_handle: Some(builder_handle),
        })
    }

    /// Background index builder loop.
    ///
    /// Polls for indexes in `Building` state and builds them by scanning the
    /// primary index. On completion, writes `WAL_RECORD_INDEX_READY` and
    /// transitions the index to `Ready`.
    ///
    /// Runs on a `LocalSet` because B-tree operations hold parking_lot guards
    /// across `.await` points (same as CommitCoordinator).
    async fn index_builder_loop(
        storage: Arc<StorageEngine>,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        commit_handle: CommitHandle,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        loop {
                // Poll every 100ms for new Building indexes
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    _ = shutdown.cancelled() => break,
                }

                // Find all Building indexes
                let building: Vec<IndexMeta> = catalog.read().building_indexes();
                if building.is_empty() {
                    continue;
                }

                for idx_meta in building {
                    if shutdown.is_cancelled() {
                        break;
                    }

                    // Get handles — clone Arcs, drop guards immediately
                    let primary = primary_indexes
                        .read()
                        .get(&idx_meta.collection_id)
                        .cloned();
                    let secondary = secondary_indexes
                        .read()
                        .get(&idx_meta.index_id)
                        .cloned();

                    let (primary, secondary) = match (primary, secondary) {
                        (Some(p), Some(s)) => (p, s),
                        _ => {
                            tracing::warn!(
                                "index builder: missing handles for {:?}, skipping",
                                idx_meta.index_id,
                            );
                            continue;
                        }
                    };

                    // Snapshot timestamp: use current visible_ts
                    let build_ts = commit_handle.visible_ts();

                    tracing::info!(
                        "building index {:?} ({}) on collection {:?} at ts={}",
                        idx_meta.index_id, idx_meta.name,
                        idx_meta.collection_id, build_ts,
                    );

                    let builder = IndexBuilder::new(
                        primary,
                        secondary,
                        idx_meta.field_paths.clone(),
                    );

                    // Use progress channel to trigger periodic checkpoints
                    // during long builds, flushing dirty pages so the buffer
                    // pool doesn't fill up.
                    let (progress_tx, mut progress_rx) =
                        tokio::sync::watch::channel(exdb_docstore::index_builder::BuildProgress {
                            docs_scanned: 0,
                            entries_inserted: 0,
                            elapsed_ms: 0,
                        });
                    let checkpoint_storage = Arc::clone(&storage);
                    let checkpoint_shutdown = shutdown.clone();
                    let checkpoint_task = tokio::task::spawn_local(async move {
                        while progress_rx.changed().await.is_ok() {
                            if checkpoint_shutdown.is_cancelled() {
                                break;
                            }
                            // Checkpoint every progress report (every 1000 docs)
                            // to flush dirty secondary index pages.
                            if let Err(e) = checkpoint_storage.checkpoint().await {
                                tracing::warn!("index build checkpoint failed: {e}");
                            }
                        }
                    });

                    match builder.build(build_ts, Some(progress_tx)).await {
                        Ok(entries) => {
                            checkpoint_task.abort();
                            tracing::info!(
                                "index {:?} ({}) built: {} entries",
                                idx_meta.index_id, idx_meta.name, entries,
                            );

                            // Write WAL_RECORD_INDEX_READY
                            let payload = idx_meta.index_id.0.to_le_bytes();
                            if let Err(e) = storage
                                .append_wal(WAL_RECORD_INDEX_READY, &payload)
                                .await
                            {
                                tracing::error!(
                                    "failed to write INDEX_READY WAL for {:?}: {e}",
                                    idx_meta.index_id,
                                );
                                continue;
                            }

                            // Transition Building → Ready.
                            // 1. Write to durable B-tree (async, no guard held)
                            let fh = storage.file_header().await;
                            let id_btree = storage.open_btree(fh.catalog_root_page.get());
                            if let Err(e) = CatalogPersistence::apply_index_ready_btree(
                                &id_btree,
                                &idx_meta,
                            )
                            .await
                            {
                                tracing::error!(
                                    "failed to mark index {:?} as Ready in B-tree: {e}",
                                    idx_meta.index_id,
                                );
                                continue;
                            }
                            // 2. Update in-memory cache (sync, guard dropped immediately)
                            catalog.write().set_index_state(
                                idx_meta.index_id,
                                IndexState::Ready,
                            );
                        }
                        Err(e) => {
                            checkpoint_task.abort();
                            tracing::error!(
                                "index build failed for {:?} ({}): {e}",
                                idx_meta.index_id, idx_meta.name,
                            );
                        }
                    }
                }
            }
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

    /// Simulate a crash: cancel background tasks, release the file lock,
    /// and drop without final checkpoint.
    ///
    /// This is intended for durability tests that need to verify recovery
    /// after an unclean shutdown.
    pub async fn crash(self) {
        // Signal all background tasks to stop
        self.shutdown.cancel();
        // Give tasks a moment to observe cancellation and exit
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        // Release the file lock WITHOUT doing a final checkpoint.
        // This allows reopening the database in the same process.
        self.storage.unlock();
        // Drop everything — no final checkpoint
    }
}
