//! B6: Database facade — the main entry point for embedded usage.
//!
//! Owns the storage engine, catalog, indexes, commit handle, and background
//! tasks. Provides `begin()` to create transactions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use exdb_core::encoding::decode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId, Ts};
use exdb_docstore::{
    CellFlags, IndexBuilder, PrimaryIndex, SecondaryIndex, compute_index_entries, make_primary_key,
    make_secondary_key_from_prefix, parse_primary_key, parse_secondary_key_suffix,
};
use exdb_storage::btree::ScanDirection;
use exdb_storage::catalog_btree::{self, CatalogEntityType};
use exdb_storage::engine::{
    DurableOpenProbe as StorageDurableOpenProbe, IntegrityBTreeRoot, IntegrityRepair,
    IntegrityRepairReport, IntegrityReport, IntegritySeverity, IntegrityStats, StorageEngine,
    StorageSnapshot, StorageUsage,
};
use exdb_storage::heap::HeapRef;
use exdb_storage::page::{PageType, SlottedPage, SlottedPageRef};
use exdb_storage::wal::{
    WAL_RECORD_INDEX_READY, WAL_RECORD_TX_COMMIT, WAL_RECORD_VISIBLE_TS, WalRecord,
};
use exdb_tx::{
    CommitCoordinator, CommitHandle, CommitRequest, CommitResult, NoReplication, ReplicationHook,
    SubscriptionMode, SubscriptionRegistry, WriteSet, compute_index_deltas,
    deserialize_promotion_payload, deserialize_wal_payload,
};
use parking_lot::RwLock;
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc};
use tokio_util::task::LocalPoolHandle;

use crate::catalog_cache::{CatalogCache, IndexMeta, IndexState};
use crate::catalog_mutation_handler::CatalogMutationHandlerImpl;
use crate::catalog_persistence::CatalogPersistence;
use crate::catalog_recovery::DatabaseRecoveryHandler;
use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::index_resolver::IndexResolverImpl;
use crate::subscription::SubscriptionHandle;
use crate::transaction::{Transaction, TransactionOptions, TransactionResult};

const PAGE_HEADER_SIZE: usize = 32;
const HEAP_SLOT_HEADER_SIZE: usize = 9;
const HEAP_HAS_OVERFLOW: u8 = 0x01;
const OVERFLOW_DATA_LEN_SIZE: usize = 4;
const MAX_OVERFLOW_CHAIN: usize = 2000;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryOpenFault {
    None,
    AfterRollbackBeforeRepairCheckpoint,
}

/// Event emitted when a background index build transitions to Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReadyEvent {
    pub database: String,
    pub collection: String,
    pub index: String,
    pub index_id: IndexId,
}

/// Point-in-time per-database resource usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseUsage {
    pub disk_usage_bytes: u64,
    pub page_store_bytes: u64,
    pub wal_retained_bytes: u64,
    pub memory_budget_bytes: usize,
    pub buffer_pool_used_frames: usize,
    pub active_transactions: u64,
    pub page_count: u64,
    pub page_size: usize,
}

/// Durable file-backed database state observed before a full open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseDurableOpenProbe {
    pub generation: u64,
    pub replication_applied_lsn: u64,
    pub checkpoint_lsn: u64,
    pub recovery_needed: bool,
}

impl From<StorageDurableOpenProbe> for DatabaseDurableOpenProbe {
    fn from(value: StorageDurableOpenProbe) -> Self {
        Self {
            generation: value.generation,
            replication_applied_lsn: value.replication_applied_lsn,
            checkpoint_lsn: value.checkpoint_lsn,
            recovery_needed: value.recovery_needed,
        }
    }
}

impl DatabaseUsage {
    fn from_storage(usage: StorageUsage, active_transactions: u64) -> Self {
        Self {
            disk_usage_bytes: usage.disk_usage_bytes,
            page_store_bytes: usage.page_store_bytes,
            wal_retained_bytes: usage.wal_retained_bytes,
            memory_budget_bytes: usage.memory_budget_bytes,
            buffer_pool_used_frames: usage.buffer_pool_used_frames,
            active_transactions,
            page_count: usage.page_count,
            page_size: usage.page_size,
        }
    }
}

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
    replicated_apply_lock: Arc<AsyncMutex<()>>,
    active_tx_count: Arc<AtomicU64>,
    index_ready_tx: broadcast::Sender<IndexReadyEvent>,
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
        Self::open_with_name(path, "default", config, replication, None).await
    }

    /// Read only the durable file-header generation for an existing database path.
    pub async fn read_generation_at_path(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<u64> {
        config.validate().map_err(DatabaseError::InvalidConfig)?;
        let storage = StorageEngine::open(
            path.as_ref(),
            config.to_storage_config(),
            &mut exdb_storage::recovery::NoOpHandler,
        )
        .await?;
        let generation = storage.file_header().await.generation.get();
        storage.close().await?;
        Ok(generation)
    }

    /// Probe durable replication/recovery state for an existing database path.
    pub async fn probe_durable_open_state_at_path(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
    ) -> Result<Option<DatabaseDurableOpenProbe>> {
        config.validate().map_err(DatabaseError::InvalidConfig)?;
        StorageEngine::probe_existing_durable(path.as_ref(), config.to_storage_config())
            .await
            .map(|probe| probe.map(Into::into))
            .map_err(DatabaseError::Storage)
    }

    pub(crate) async fn open_managed(
        path: impl AsRef<Path>,
        name: impl Into<String>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
        index_ready_tx: broadcast::Sender<IndexReadyEvent>,
    ) -> Result<Self> {
        Self::open_with_name(path, name, config, replication, Some(index_ready_tx)).await
    }

    async fn open_with_name(
        path: impl AsRef<Path>,
        name: impl Into<String>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
        index_ready_tx: Option<broadcast::Sender<IndexReadyEvent>>,
    ) -> Result<Self> {
        Self::open_with_name_with_recovery_fault(
            path,
            name,
            config,
            replication,
            index_ready_tx,
            RecoveryOpenFault::None,
        )
        .await
    }

    async fn open_with_name_with_recovery_fault(
        path: impl AsRef<Path>,
        name: impl Into<String>,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
        index_ready_tx: Option<broadcast::Sender<IndexReadyEvent>>,
        recovery_fault: RecoveryOpenFault,
    ) -> Result<Self> {
        use tokio_stream::StreamExt;

        let path = path.as_ref().to_path_buf();
        let name = name.into();
        config.validate().map_err(DatabaseError::InvalidConfig)?;
        let storage_config = config.to_storage_config();

        // Phase 1: L2 physical recovery (DWB, page-level redo).
        // We pass NoOpHandler because L6 recovery is done separately below
        // (DatabaseRecoveryHandler is !Send and can't implement WalRecordHandler).
        let storage = Arc::new(
            StorageEngine::open(
                &path,
                storage_config,
                &mut exdb_storage::recovery::NoOpHandler,
            )
            .await?,
        );

        // Phase 2: L6 logical recovery.
        // Load initial state from catalog B-trees (checkpointed state).
        let mut recovery_handler = DatabaseRecoveryHandler::new(Arc::clone(&storage)).await?;

        // Replay WAL records from checkpoint_lsn to rebuild any mutations
        // that were committed after the last checkpoint but before the crash.
        let checkpoint_lsn = storage.file_header().await.checkpoint_lsn.get();
        let mut wal_stream = storage.read_wal_from(checkpoint_lsn);
        while let Some(result) = wal_stream.next().await {
            let record = result?;
            recovery_handler.handle_record(&record).await?;
        }
        let rolled_back_unreplicated = recovery_handler.rollback_unreplicated_commits().await?;
        recovery_handler.rebuild_ready_indexes().await?;
        if rolled_back_unreplicated {
            storage.reclaim_zeroed_pages_for_recovery().await?;
            storage
                .rebuild_free_list_from_existing_free_pages_for_recovery()
                .await?;
            if recovery_fault == RecoveryOpenFault::AfterRollbackBeforeRepairCheckpoint {
                storage.unlock();
                return Err(DatabaseError::Storage(std::io::Error::other(
                    "injected recovery crash after rollback before repair checkpoint",
                )));
            }
            storage.checkpoint().await?;
        } else {
            storage.reclaim_zeroed_pages_for_recovery().await?;
            storage
                .rebuild_free_list_from_existing_free_pages_for_recovery()
                .await?;
            storage.sync_file_header().await?;
        }

        let state = recovery_handler.into_state();

        Self::from_recovered_state(
            name,
            config,
            Some(path),
            storage,
            state.catalog,
            state.primary_indexes,
            state.secondary_indexes,
            state.recovered_ts,
            state.visible_ts,
            replication,
            index_ready_tx,
        )
        .await
    }

    /// Open an in-memory database (ephemeral, for testing).
    pub async fn open_in_memory(
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self> {
        config.validate().map_err(DatabaseError::InvalidConfig)?;
        let storage_config = config.to_storage_config();
        let storage = Arc::new(StorageEngine::open_in_memory(storage_config).await?);

        let fh = storage.file_header().await;
        let catalog = CatalogCache::new(fh.next_collection_id.get(), fh.next_index_id.get());

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
            None,
        )
        .await
    }

    /// Build a Database from recovered state (shared between open and open_in_memory).
    #[allow(clippy::too_many_arguments)]
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
        index_ready_tx: Option<broadcast::Sender<IndexReadyEvent>>,
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
                    idx.index_id,
                    idx.name,
                    idx.collection_id,
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

        Self::run_startup_integrity_checks(&config, &storage, &catalog).await?;

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
        let (local_index_ready_tx, _) = broadcast::channel(1024);
        let index_ready_tx = index_ready_tx.unwrap_or(local_index_ready_tx);
        let builder_database_name = name.clone();
        let builder_index_ready_tx = index_ready_tx.clone();
        let builder_handle = coordinator_pool.spawn_pinned(move || {
            Self::index_builder_loop(
                builder_storage,
                builder_catalog,
                builder_primaries,
                builder_secondaries,
                builder_commit,
                builder_database_name,
                builder_index_ready_tx,
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
            replicated_apply_lock: Arc::new(AsyncMutex::new(())),
            active_tx_count: Arc::new(AtomicU64::new(0)),
            index_ready_tx,
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
    #[allow(clippy::too_many_arguments)]
    async fn index_builder_loop(
        storage: Arc<StorageEngine>,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        commit_handle: CommitHandle,
        database_name: String,
        index_ready_tx: broadcast::Sender<IndexReadyEvent>,
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
                let primary = primary_indexes.read().get(&idx_meta.collection_id).cloned();
                let secondary = secondary_indexes.read().get(&idx_meta.index_id).cloned();

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
                    idx_meta.index_id,
                    idx_meta.name,
                    idx_meta.collection_id,
                    build_ts,
                );

                let builder = IndexBuilder::new(primary, secondary, idx_meta.field_paths.clone());

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
                            idx_meta.index_id,
                            idx_meta.name,
                            entries,
                        );

                        // Write WAL_RECORD_INDEX_READY
                        let payload = idx_meta.index_id.0.to_le_bytes();
                        if let Err(e) = storage.append_wal(WAL_RECORD_INDEX_READY, &payload).await {
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
                        if let Err(e) =
                            CatalogPersistence::apply_index_ready_btree(&id_btree, &idx_meta).await
                        {
                            tracing::error!(
                                "failed to mark index {:?} as Ready in B-tree: {e}",
                                idx_meta.index_id,
                            );
                            continue;
                        }
                        // 2. Update in-memory cache (sync, guard dropped immediately)
                        let collection_name = {
                            let mut cache = catalog.write();
                            cache.set_index_state(idx_meta.index_id, IndexState::Ready);
                            cache
                                .get_collection(idx_meta.collection_id)
                                .map(|collection| collection.name.clone())
                        };
                        if let Some(collection_name) = collection_name {
                            let _ = index_ready_tx.send(IndexReadyEvent {
                                database: database_name.clone(),
                                collection: collection_name,
                                index: idx_meta.name.clone(),
                                index_id: idx_meta.index_id,
                            });
                        }
                    }
                    Err(e) => {
                        checkpoint_task.abort();
                        tracing::error!(
                            "index build failed for {:?} ({}): {e}",
                            idx_meta.index_id,
                            idx_meta.name,
                        );
                    }
                }
            }
        }
    }

    /// Close the database gracefully.
    pub async fn close(mut self) -> Result<()> {
        // Signal shutdown
        self.shutdown.cancel();

        // Wait for active transactions to finish (with timeout)
        let deadline = tokio::time::Instant::now() + self.config.close_timeout;
        while self.active_tx_count.load(Ordering::Acquire) > 0 {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    "close timeout: {} active transactions remaining",
                    self.active_tx_count.load(Ordering::Acquire)
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        Self::await_background_task("checkpoint", self._checkpoint_handle.take()).await;
        Self::await_background_task("index builder", self._index_builder_handle.take()).await;

        // Final checkpoint + close storage
        self.storage.close().await?;

        Ok(())
    }

    async fn await_background_task(
        name: &'static str,
        handle: Option<tokio::task::JoinHandle<()>>,
    ) {
        let Some(handle) = handle else {
            return;
        };
        match handle.await {
            Ok(()) => {}
            Err(err) if err.is_cancelled() => {}
            Err(err) => tracing::warn!("background {name} task ended with error: {err}"),
        }
    }

    /// Run a storage integrity check for maintenance/diagnostics.
    pub async fn check_integrity(&self) -> Result<IntegrityReport> {
        let fh = self.storage.file_header().await;
        let (roots, collections, indexes, ready_indexes_by_collection) = {
            let catalog = self.catalog.read();
            let collections = catalog.list_collections();
            let indexes = catalog.list_all_indexes();
            let ready_indexes_by_collection = collections
                .iter()
                .map(|collection| {
                    (
                        collection.collection_id,
                        catalog.ready_indexes(collection.collection_id),
                    )
                })
                .collect::<HashMap<_, _>>();
            (
                Self::integrity_roots(
                    fh.catalog_root_page.get(),
                    fh.catalog_name_root_page.get(),
                    &catalog,
                ),
                collections,
                indexes,
                ready_indexes_by_collection,
            )
        };

        let mut report = self
            .storage
            .check_integrity_with_btree_roots(&roots)
            .await?;
        Self::check_catalog_semantic_integrity(
            &self.storage,
            fh.catalog_name_root_page.get(),
            &collections,
            &indexes,
            &mut report,
        )
        .await?;
        Self::check_index_semantic_integrity(
            &self.storage,
            &collections,
            &ready_indexes_by_collection,
            &mut report,
        )
        .await?;

        Ok(report)
    }

    /// Return point-in-time resource usage for this database.
    pub fn usage(&self) -> DatabaseUsage {
        DatabaseUsage::from_storage(
            self.storage.usage(),
            self.active_tx_count.load(Ordering::Acquire),
        )
    }

    /// Export a checkpointed storage snapshot for full replica reconstruction.
    pub async fn export_snapshot(&self) -> Result<StorageSnapshot> {
        Ok(self.storage.export_snapshot().await?)
    }

    /// Restore a database snapshot into a fresh durable database path.
    pub async fn restore_snapshot(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        snapshot: StorageSnapshot,
    ) -> Result<()> {
        config.validate().map_err(DatabaseError::InvalidConfig)?;
        Ok(
            StorageEngine::restore_snapshot(path.as_ref(), config.to_storage_config(), snapshot)
                .await?,
        )
    }

    /// Highest primary-source WAL LSN durably applied by this database as a replica.
    pub async fn replication_applied_lsn(&self) -> u64 {
        self.storage.replication_applied_lsn().await
    }

    /// Cluster generation durably recorded in the database file header.
    pub async fn generation(&self) -> u64 {
        self.storage.file_header().await.generation.get()
    }

    /// Persist this database's cluster generation in the file-header pair.
    pub async fn set_generation(&self, generation: u64) -> Result<()> {
        self.storage
            .update_file_header(|fh| {
                fh.generation.set(generation);
            })
            .await?;
        self.storage.sync_file_header().await?;
        Ok(())
    }

    /// Durably apply a TxCommit WAL payload received from a primary.
    ///
    /// This is the L6 primitive used by replication transports: the received
    /// primary commit is first persisted to the replica WAL, then interpreted
    /// through the same recovery handler used at startup, then a local
    /// `VisibleTs` record is persisted before the in-memory read fence advances.
    /// The apply path is serialized and runs on the database local pool because
    /// B-tree mutation code can hold page guards across `.await` points.
    pub async fn apply_replicated_wal(&self, source_lsn: u64, payload: &[u8]) -> Result<u64> {
        let (_, commit_ts, _, _) = deserialize_wal_payload(payload)
            .map_err(|err| DatabaseError::Commit(format!("invalid replicated WAL: {err}")))?;

        let _guard = self.replicated_apply_lock.lock().await;
        if commit_ts <= self.commit_handle.visible_ts() {
            self.storage
                .update_file_header(|fh| {
                    let applied_lsn = fh.replication_applied_lsn.get().max(source_lsn);
                    fh.replication_applied_lsn.set(applied_lsn);
                })
                .await?;
            self.storage.sync_file_header().await?;
            return Ok(commit_ts);
        }

        let payload = payload.to_vec();
        let storage = Arc::clone(&self.storage);
        let apply = self._coordinator_pool.spawn_pinned(move || async move {
            let local_commit_lsn = storage.append_wal(WAL_RECORD_TX_COMMIT, &payload).await?;
            let mut handler = DatabaseRecoveryHandler::new(Arc::clone(&storage)).await?;
            handler
                .handle_record(&WalRecord {
                    lsn: source_lsn.max(local_commit_lsn),
                    record_type: WAL_RECORD_TX_COMMIT,
                    payload,
                })
                .await?;

            let visible_payload = commit_ts.to_le_bytes().to_vec();
            let local_visible_lsn = storage
                .append_wal(WAL_RECORD_VISIBLE_TS, &visible_payload)
                .await?;
            handler
                .handle_record(&WalRecord {
                    lsn: local_visible_lsn,
                    record_type: WAL_RECORD_VISIBLE_TS,
                    payload: visible_payload,
                })
                .await?;

            handler.rebuild_ready_indexes().await?;
            storage
                .update_file_header(|fh| {
                    let visible_ts = fh.visible_ts.get().max(commit_ts);
                    fh.visible_ts.set(visible_ts);
                    let applied_lsn = fh.replication_applied_lsn.get().max(source_lsn);
                    fh.replication_applied_lsn.set(applied_lsn);
                })
                .await?;
            storage.sync_file_header().await?;

            Ok::<_, std::io::Error>(handler.into_state())
        });

        let state = apply
            .await
            .map_err(|err| DatabaseError::Commit(format!("replica apply task failed: {err}")))??;
        *self.catalog.write() = state.catalog;
        *self.primary_indexes.write() = state.primary_indexes;
        *self.secondary_indexes.write() = state.secondary_indexes;
        self.commit_handle.advance_visible_ts(commit_ts);

        Ok(commit_ts)
    }

    /// Run a conservative repair pass for maintenance.
    ///
    /// This repairs the durable file-header shadow pair, can rebuild the
    /// catalog name index from authoritative catalog ID records, and can rebuild
    /// Ready secondary indexes from authoritative primary-index versions when
    /// the semantic integrity pass detects missing or dangling secondary
    /// entries.
    pub async fn repair_integrity(&self) -> Result<IntegrityRepairReport> {
        let fh = self.storage.file_header().await;
        let roots = {
            let catalog = self.catalog.read();
            Self::integrity_roots(
                fh.catalog_root_page.get(),
                fh.catalog_name_root_page.get(),
                &catalog,
            )
        };
        let before = self.check_integrity().await?;
        let mut report = self
            .storage
            .repair_integrity_with_btree_roots(&roots)
            .await?;

        if Self::has_catalog_name_integrity_issue(&before) {
            let (removed, written) = self.rebuild_catalog_name_index_for_repair().await?;
            report.repairs.push(IntegrityRepair {
                page_id: None,
                message: format!(
                    "rebuilt catalog name index from authoritative catalog entries ({removed} old entries removed, {written} entries written)"
                ),
            });
            self.storage.checkpoint().await?;
        }

        let primary_index_issue = Self::has_primary_index_integrity_issue(&before);
        let primary_rebuilt = if primary_index_issue {
            let rebuilt = self
                .rebuild_primary_indexes_from_retained_wal_for_repair()
                .await?;
            if rebuilt > 0 {
                report.repairs.push(IntegrityRepair {
                    page_id: None,
                    message: format!(
                        "rebuilt {rebuilt} primary index tree(s) from fully retained WAL"
                    ),
                });
                self.storage.checkpoint().await?;
            }
            rebuilt
        } else {
            0
        };

        if Self::has_secondary_index_integrity_issue(&before)
            || (primary_index_issue && primary_rebuilt > 0)
        {
            let rebuilt = self.rebuild_ready_secondary_indexes_for_repair().await?;
            if rebuilt > 0 {
                report.repairs.push(IntegrityRepair {
                    page_id: None,
                    message: format!(
                        "rebuilt {rebuilt} ready secondary index tree(s) from primary versions"
                    ),
                });
                self.storage.checkpoint().await?;
            }
        }

        let post_rebuild = self.check_integrity().await?;
        let reclaimed = if post_rebuild.has_errors() {
            0
        } else {
            self.reclaim_orphan_pages_for_repair(&post_rebuild).await?
        };
        if reclaimed > 0 {
            report.repairs.push(IntegrityRepair {
                page_id: None,
                message: format!("reclaimed {reclaimed} orphan page(s) into the free list"),
            });
            self.storage.checkpoint().await?;
        }

        report.remaining_issues = self.check_integrity().await?.issues;
        Ok(report)
    }

    fn has_catalog_name_integrity_issue(report: &IntegrityReport) -> bool {
        report.issues.iter().any(|issue| {
            let message = issue.message.as_str();
            issue.severity == IntegritySeverity::Error
                && (message.contains("catalog name index")
                    || message.contains("name catalog")
                    || message.contains("catalog collection-name entry")
                    || message.contains("catalog index-name entry"))
        })
    }

    fn has_primary_index_integrity_issue(report: &IntegrityReport) -> bool {
        report.issues.iter().any(|issue| {
            let message = issue.message.as_str();
            issue.severity == IntegritySeverity::Error
                && ((message.starts_with("B-tree 'collection:") && message.contains(":primary'"))
                    || message.contains("failed to scan primary entry")
                    || message.contains("failed to lookup referenced primary version"))
        })
    }

    fn has_secondary_index_integrity_issue(report: &IntegrityReport) -> bool {
        report.issues.iter().any(|issue| {
            let message = issue.message.as_str();
            issue.severity == IntegritySeverity::Error
                && (message.contains("missing secondary entry")
                    || message.contains("secondary entry has non-empty value")
                    || message.contains("malformed secondary key")
                    || message.contains("references missing primary version")
                    || message.contains("references tombstoned primary version")
                    || (message.starts_with("B-tree 'collection:") && message.contains(":index:")))
        })
    }

    async fn rebuild_catalog_name_index_for_repair(&self) -> Result<(usize, usize)> {
        use std::ops::Bound;
        use tokio_stream::StreamExt;

        let fh = self.storage.file_header().await;
        let name_btree = self.storage.open_btree(fh.catalog_name_root_page.get());
        let mut old_keys = Vec::new();
        let scan = name_btree.scan(
            Bound::Included(catalog_btree::collection_name_scan_prefix().as_slice()),
            Bound::Excluded([CatalogEntityType::Index as u8 + 1].as_slice()),
            ScanDirection::Forward,
        );
        tokio::pin!(scan);

        while let Some(item) = scan.next().await {
            let (key, _) = item?;
            old_keys.push(key);
        }

        let removed = old_keys.len();
        for key in old_keys {
            name_btree.delete(&key).await?;
        }

        let (collections, indexes) = {
            let catalog = self.catalog.read();
            (catalog.list_collections(), catalog.list_all_indexes())
        };

        let mut written = 0usize;
        for collection in collections {
            let key = catalog_btree::make_catalog_name_key(
                CatalogEntityType::Collection,
                &collection.name,
            );
            let value = catalog_btree::serialize_name_value(collection.collection_id.0);
            name_btree.insert(&key, &value).await?;
            written += 1;
        }
        for index in indexes {
            let key =
                catalog_btree::make_catalog_index_name_key(index.collection_id.0, &index.name);
            let value = catalog_btree::serialize_name_value(index.index_id.0);
            name_btree.insert(&key, &value).await?;
            written += 1;
        }

        Ok((removed, written))
    }

    async fn rebuild_primary_indexes_from_retained_wal_for_repair(&self) -> Result<usize> {
        use tokio_stream::StreamExt;

        if self.storage.oldest_retained_wal_lsn() != Some(0) {
            return Ok(0);
        }

        let collections = self.catalog.read().list_collections();
        if collections.is_empty() {
            return Ok(0);
        }

        let primary_indexes = self.primary_indexes.read().clone();
        for collection in &collections {
            Self::reset_btree_root_for_repair(
                &self.storage,
                collection.primary_root_page,
                PageType::BTreeLeaf,
            )
            .await?;
        }

        let visible_ts = self.storage.file_header().await.visible_ts.get();
        let mut wal = self.storage.read_wal_from(0);
        while let Some(item) = wal.next().await {
            let record = item?;
            if record.record_type != WAL_RECORD_TX_COMMIT {
                continue;
            }

            let (_version, commit_ts, mutations, _catalog_bytes) =
                deserialize_wal_payload(&record.payload).map_err(|err| {
                    DatabaseError::Commit(format!(
                        "primary WAL repair failed to deserialize TxCommit at LSN {}: {err}",
                        record.lsn
                    ))
                })?;
            if commit_ts > visible_ts {
                continue;
            }

            for (collection_id, doc_id, op_tag, body) in mutations {
                let Some(primary) = primary_indexes.get(&collection_id) else {
                    continue;
                };

                match op_tag {
                    0x01 | 0x02 => {
                        if let Some(body) = body {
                            primary
                                .insert_version(&doc_id, commit_ts, Some(&body))
                                .await?;
                        }
                    }
                    0x03 => {
                        primary.insert_version(&doc_id, commit_ts, None).await?;
                    }
                    _ => {}
                }
            }
        }

        Ok(collections.len())
    }

    async fn rebuild_ready_secondary_indexes_for_repair(&self) -> Result<usize> {
        use tokio_stream::StreamExt;

        let collections = self.catalog.read().list_collections();
        let ready_indexes_by_collection = {
            let catalog = self.catalog.read();
            collections
                .iter()
                .map(|collection| {
                    (
                        collection.collection_id,
                        catalog.ready_indexes(collection.collection_id),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        let mut rebuilt = 0usize;
        for collection in collections {
            let primary = self.storage.open_btree(collection.primary_root_page);
            let ready_indexes = ready_indexes_by_collection
                .get(&collection.collection_id)
                .cloned()
                .unwrap_or_default();

            for index in ready_indexes {
                let secondary = self.storage.open_btree(index.root_page);
                Self::reset_btree_root_for_repair(
                    &self.storage,
                    index.root_page,
                    PageType::BTreeLeaf,
                )
                .await?;

                let mut primary_scan = primary.scan(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Unbounded,
                    ScanDirection::Forward,
                );
                while let Some(item) = primary_scan.next().await {
                    let (primary_key, value) = item?;
                    let (doc_id, ts) = parse_primary_key(&primary_key)
                        .map_err(|err| DatabaseError::Commit(format!("repair failed: {err}")))?;
                    if value.is_empty() {
                        return Err(DatabaseError::Commit(
                            "repair failed: primary value is empty".to_string(),
                        ));
                    }
                    if value
                        .first()
                        .copied()
                        .is_some_and(|flags| CellFlags::from_byte(flags).tombstone)
                    {
                        continue;
                    }

                    let Some(body) =
                        Self::load_primary_body_for_repair(&self.storage, &collection.name, &value)
                            .await?
                    else {
                        continue;
                    };
                    let doc = decode_document(&body)
                        .map_err(|err| DatabaseError::Commit(format!("repair failed: {err}")))?;
                    let prefixes = compute_index_entries(&doc, &index.field_paths)
                        .map_err(|err| DatabaseError::Commit(format!("repair failed: {err}")))?;
                    for prefix in prefixes {
                        let key = make_secondary_key_from_prefix(&prefix, &doc_id, ts);
                        secondary.insert(&key, &[]).await?;
                    }
                }

                rebuilt += 1;
            }
        }

        Ok(rebuilt)
    }

    async fn reset_btree_root_for_repair(
        storage: &StorageEngine,
        root_page: u32,
        page_type: PageType,
    ) -> Result<()> {
        let mut guard = storage
            .buffer_pool()
            .fetch_page_exclusive(root_page)
            .await?;
        SlottedPage::init(guard.data_mut(), root_page, page_type).stamp_checksum();
        Ok(())
    }

    async fn reclaim_orphan_pages_for_repair(&self, report: &IntegrityReport) -> Result<usize> {
        let orphan_pages = Self::repairable_orphan_pages(report);
        if orphan_pages.is_empty() {
            return Ok(0);
        }

        let mut reclaimed = 0usize;
        let mut free_list = self.storage.free_list().lock().await;
        for page_id in orphan_pages {
            free_list.deallocate(page_id).await?;
            reclaimed += 1;
        }
        drop(free_list);

        self.storage.sync_file_header().await?;
        Ok(reclaimed)
    }

    fn repairable_orphan_pages(report: &IntegrityReport) -> BTreeSet<u32> {
        report
            .issues
            .iter()
            .filter_map(|issue| {
                let page_id = issue.page_id?;
                let message = issue.message.as_str();
                let is_repairable_orphan = issue.severity == IntegritySeverity::Warning
                    && (message.contains("B-tree page is not reachable")
                        || message.contains("heap page is not reachable")
                        || message.contains("overflow page is not reachable"));
                (page_id != 0 && is_repairable_orphan).then_some(page_id)
            })
            .collect()
    }

    async fn load_primary_body_for_repair(
        storage: &StorageEngine,
        collection_name: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut scratch_report = IntegrityReport {
            issues: Vec::new(),
            stats: IntegrityStats {
                page_count: 0,
                pages_scanned: 0,
                free_pages: 0,
                btree_pages: 0,
                heap_pages: 0,
                overflow_pages: 0,
                orphan_btree_pages: 0,
                orphan_heap_pages: 0,
                double_allocated_pages: 0,
                wal_records_scanned: 0,
                wal_bytes_scanned: 0,
                page_type_counts: BTreeMap::new(),
            },
        };
        let mut reachable_heap_pages = BTreeSet::new();

        Ok(Self::load_primary_body(
            storage,
            value,
            &mut scratch_report,
            collection_name,
            &mut reachable_heap_pages,
        )
        .await)
    }

    async fn run_startup_integrity_checks(
        config: &DatabaseConfig,
        storage: &StorageEngine,
        catalog: &CatalogCache,
    ) -> Result<()> {
        if config.check_on_startup_full {
            let fh = storage.file_header().await;
            let collections = catalog.list_collections();
            let indexes = catalog.list_all_indexes();
            let ready_indexes_by_collection = collections
                .iter()
                .map(|collection| {
                    (
                        collection.collection_id,
                        catalog.ready_indexes(collection.collection_id),
                    )
                })
                .collect::<HashMap<_, _>>();
            let roots = Self::integrity_roots(
                fh.catalog_root_page.get(),
                fh.catalog_name_root_page.get(),
                catalog,
            );
            let mut report = storage.check_integrity_with_btree_roots(&roots).await?;
            Self::check_catalog_semantic_integrity(
                storage,
                fh.catalog_name_root_page.get(),
                &collections,
                &indexes,
                &mut report,
            )
            .await?;
            Self::check_index_semantic_integrity(
                storage,
                &collections,
                &ready_indexes_by_collection,
                &mut report,
            )
            .await?;
            Self::ensure_integrity_ok("startup full", report)?;
        } else if config.check_on_startup {
            let report = storage.check_integrity().await?;
            Self::ensure_integrity_ok("startup quick", report)?;
        }

        Ok(())
    }

    fn integrity_roots(
        catalog_root_page: u32,
        catalog_name_root_page: u32,
        catalog: &CatalogCache,
    ) -> Vec<IntegrityBTreeRoot> {
        let mut roots = vec![
            IntegrityBTreeRoot {
                name: "catalog_by_id".to_string(),
                root_page: catalog_root_page,
            },
            IntegrityBTreeRoot {
                name: "catalog_by_name".to_string(),
                root_page: catalog_name_root_page,
            },
        ];

        for collection in catalog.list_collections() {
            roots.push(IntegrityBTreeRoot {
                name: format!("collection:{}:primary", collection.name),
                root_page: collection.primary_root_page,
            });

            for index in catalog.list_indexes(collection.collection_id) {
                roots.push(IntegrityBTreeRoot {
                    name: format!("collection:{}:index:{}", collection.name, index.name),
                    root_page: index.root_page,
                });
            }
        }

        roots
    }

    async fn check_catalog_semantic_integrity(
        storage: &StorageEngine,
        catalog_name_root_page: u32,
        collections: &[crate::catalog_cache::CollectionMeta],
        indexes: &[IndexMeta],
        report: &mut IntegrityReport,
    ) -> Result<()> {
        use std::ops::Bound;
        use tokio_stream::StreamExt;

        let mut collections_by_id = BTreeMap::new();
        let mut collection_ids_by_name: BTreeMap<String, CollectionId> = BTreeMap::new();
        for collection in collections {
            if collection.collection_id.0 == 0 {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("collection '{}' has reserved id 0", collection.name),
                );
            }
            if collection.name.is_empty() {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("collection {:?} has empty name", collection.collection_id),
                );
            }
            if collection.primary_root_page == 0 {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("collection '{}' has primary root page 0", collection.name),
                );
            }

            if let Some(previous_id) =
                collection_ids_by_name.insert(collection.name.clone(), collection.collection_id)
            {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "duplicate collection name '{}' for ids {:?} and {:?}",
                        collection.name, previous_id, collection.collection_id
                    ),
                );
            }
            collections_by_id.insert(collection.collection_id, collection.clone());
        }

        let name_btree = storage.open_btree(catalog_name_root_page);
        for collection in collections {
            let key = catalog_btree::make_catalog_name_key(
                CatalogEntityType::Collection,
                &collection.name,
            );
            match name_btree.get(&key).await? {
                Some(value) => match catalog_btree::deserialize_name_value(&value) {
                    Ok(id) if id == collection.collection_id.0 => {}
                    Ok(id) => Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "collection '{}' name catalog maps to id {}, expected {}",
                            collection.name, id, collection.collection_id.0
                        ),
                    ),
                    Err(err) => Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "collection '{}' name catalog value is malformed: {err}",
                            collection.name
                        ),
                    ),
                },
                None => Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' is missing from the catalog name index",
                        collection.name
                    ),
                ),
            }
        }

        let mut name_scan = name_btree.scan(
            Bound::Included(catalog_btree::collection_name_scan_prefix().as_slice()),
            Bound::Excluded(catalog_btree::index_name_scan_prefix().as_slice()),
            ScanDirection::Forward,
        );
        while let Some(item) = name_scan.next().await {
            let (key, value) = match item {
                Ok(item) => item,
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!("failed to scan catalog collection-name entry: {err}"),
                    );
                    continue;
                }
            };
            let Some(name) =
                Self::parse_catalog_name_key(&key, CatalogEntityType::Collection, report)
            else {
                continue;
            };
            let id = match catalog_btree::deserialize_name_value(&value) {
                Ok(id) => CollectionId(id),
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "catalog collection-name entry '{name}' has malformed value: {err}"
                        ),
                    );
                    continue;
                }
            };
            match collections_by_id.get(&id) {
                Some(collection) if collection.name == name => {}
                Some(collection) => Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "catalog collection-name entry '{name}' points to collection '{}' ({:?})",
                        collection.name, collection.collection_id
                    ),
                ),
                None => Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "catalog collection-name entry '{name}' points to missing id {:?}",
                        id
                    ),
                ),
            }
        }

        let mut indexes_by_collection: BTreeMap<CollectionId, Vec<&IndexMeta>> = BTreeMap::new();
        let mut index_ids = BTreeSet::new();
        let mut indexes_by_id = BTreeMap::new();
        for index in indexes {
            if !index_ids.insert(index.index_id) {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("duplicate index id {:?}", index.index_id),
                );
            }
            if !collections_by_id.contains_key(&index.collection_id) {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "index '{}' ({:?}) references missing collection {:?}",
                        index.name, index.index_id, index.collection_id
                    ),
                );
            }
            if index.name.is_empty() {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("index {:?} has empty name", index.index_id),
                );
            }
            if index.root_page == 0 {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "index '{}' ({:?}) has root page 0",
                        index.name, index.index_id
                    ),
                );
            }
            if index.name.starts_with('_') && index.name != "_created_at" {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "index '{}' ({:?}) uses reserved system index name",
                        index.name, index.index_id
                    ),
                );
            }
            indexes_by_id.insert(index.index_id, index);
            indexes_by_collection
                .entry(index.collection_id)
                .or_default()
                .push(index);
        }

        let expected_created_at_path = vec![FieldPath::single("_created_at")];
        for collection in collections {
            let indexes = indexes_by_collection
                .get(&collection.collection_id)
                .cloned()
                .unwrap_or_default();
            let mut names = BTreeMap::new();
            let mut created_at_indexes = Vec::new();

            for index in indexes {
                let key =
                    catalog_btree::make_catalog_index_name_key(index.collection_id.0, &index.name);
                match name_btree.get(&key).await? {
                    Some(value) => match catalog_btree::deserialize_name_value(&value) {
                        Ok(id) if id == index.index_id.0 => {}
                        Ok(id) => Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' index '{}' name catalog maps to id {}, expected {}",
                                collection.name, index.name, id, index.index_id.0
                            ),
                        ),
                        Err(err) => Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' index '{}' name catalog value is malformed: {err}",
                                collection.name, index.name
                            ),
                        ),
                    },
                    None => Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "collection '{}' index '{}' is missing from the catalog name index",
                            collection.name, index.name
                        ),
                    ),
                }

                if let Some(previous_id) = names.insert(index.name.clone(), index.index_id) {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "collection '{}' has duplicate index name '{}' for ids {:?} and {:?}",
                            collection.name, index.name, previous_id, index.index_id
                        ),
                    );
                }
                if index.name == "_created_at" {
                    created_at_indexes.push(index);
                }
            }

            match created_at_indexes.as_slice() {
                [] => Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' is missing required _created_at index",
                        collection.name
                    ),
                ),
                [index] => {
                    if index.state != IndexState::Ready {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' _created_at index is {:?}, expected Ready",
                                collection.name, index.state
                            ),
                        );
                    }
                    if index.field_paths != expected_created_at_path {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' _created_at index has field paths {:?}",
                                collection.name, index.field_paths
                            ),
                        );
                    }
                }
                multiple => Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' has {} _created_at indexes",
                        collection.name,
                        multiple.len()
                    ),
                ),
            }
        }

        let mut index_name_scan = name_btree.scan(
            Bound::Included(catalog_btree::index_name_scan_prefix().as_slice()),
            Bound::Excluded([CatalogEntityType::Index as u8 + 1].as_slice()),
            ScanDirection::Forward,
        );
        while let Some(item) = index_name_scan.next().await {
            let (key, value) = match item {
                Ok(item) => item,
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!("failed to scan catalog index-name entry: {err}"),
                    );
                    continue;
                }
            };
            let id = match catalog_btree::deserialize_name_value(&value) {
                Ok(id) => IndexId(id),
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!("catalog index-name entry has malformed value: {err}"),
                    );
                    continue;
                }
            };
            match indexes_by_id.get(&id) {
                Some(index) => {
                    let expected = catalog_btree::make_catalog_index_name_key(
                        index.collection_id.0,
                        &index.name,
                    );
                    if key == expected {
                        continue;
                    }
                    let legacy =
                        catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &index.name);
                    if key == legacy {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "catalog index-name entry for '{}' ({:?}) uses legacy unscoped key",
                                index.name, index.index_id
                            ),
                        );
                    } else {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "catalog index-name entry for {:?} does not match collection-scoped key",
                                index.index_id
                            ),
                        );
                    }
                }
                None => Self::push_integrity_error(
                    report,
                    None,
                    format!("catalog index-name entry points to missing id {:?}", id),
                ),
            }
        }

        Ok(())
    }

    fn parse_catalog_name_key(
        key: &[u8],
        expected_type: CatalogEntityType,
        report: &mut IntegrityReport,
    ) -> Option<String> {
        if key.len() < 2 {
            Self::push_integrity_error(
                report,
                None,
                format!("catalog name key is too short: {} bytes", key.len()),
            );
            return None;
        }
        if key[0] != expected_type as u8 {
            Self::push_integrity_error(
                report,
                None,
                format!(
                    "catalog name key has type byte {:#04x}, expected {:#04x}",
                    key[0], expected_type as u8
                ),
            );
            return None;
        }
        if key.last().copied() != Some(0) {
            Self::push_integrity_error(report, None, "catalog name key is missing terminator");
            return None;
        }
        match std::str::from_utf8(&key[1..key.len() - 1]) {
            Ok(name) => Some(name.to_string()),
            Err(err) => {
                Self::push_integrity_error(
                    report,
                    None,
                    format!("catalog name key is not valid UTF-8: {err}"),
                );
                None
            }
        }
    }

    async fn check_index_semantic_integrity(
        storage: &StorageEngine,
        collections: &[crate::catalog_cache::CollectionMeta],
        ready_indexes_by_collection: &HashMap<CollectionId, Vec<IndexMeta>>,
        report: &mut IntegrityReport,
    ) -> Result<()> {
        use tokio_stream::StreamExt;

        let mut reachable_heap_pages = BTreeSet::new();

        for collection in collections {
            let primary = storage.open_btree(collection.primary_root_page);
            let ready_indexes = ready_indexes_by_collection
                .get(&collection.collection_id)
                .cloned()
                .unwrap_or_default();

            for index in &ready_indexes {
                let secondary = storage.open_btree(index.root_page);
                let mut secondary_scan = secondary.scan(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Unbounded,
                    ScanDirection::Forward,
                );

                while let Some(item) = secondary_scan.next().await {
                    let (key, value) = match item {
                        Ok(item) => item,
                        Err(err) => {
                            Self::push_integrity_error(
                                report,
                                None,
                                format!(
                                    "collection '{}' index '{}' failed to scan secondary entry: {err}",
                                    collection.name, index.name
                                ),
                            );
                            continue;
                        }
                    };

                    if !value.is_empty() {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' index '{}' secondary entry has non-empty value",
                                collection.name, index.name
                            ),
                        );
                    }

                    let (doc_id, ts) = match parse_secondary_key_suffix(&key) {
                        Ok(parsed) => parsed,
                        Err(err) => {
                            Self::push_integrity_error(
                                report,
                                None,
                                format!(
                                    "collection '{}' index '{}' has malformed secondary key: {err}",
                                    collection.name, index.name
                                ),
                            );
                            continue;
                        }
                    };

                    let primary_key = make_primary_key(&doc_id, ts);
                    match primary.get(&primary_key).await {
                        Ok(Some(primary_value)) => {
                            if let Some(flags) = Self::primary_cell_flags(
                                &primary_value,
                                report,
                                &format!(
                                    "collection '{}' primary version referenced by index '{}'",
                                    collection.name, index.name
                                ),
                            ) && flags.tombstone
                            {
                                Self::push_integrity_error(
                                    report,
                                    None,
                                    format!(
                                        "collection '{}' index '{}' references tombstoned primary version",
                                        collection.name, index.name
                                    ),
                                );
                            }
                        }
                        Ok(None) => {
                            Self::push_integrity_error(
                                report,
                                None,
                                format!(
                                    "collection '{}' index '{}' references missing primary version",
                                    collection.name, index.name
                                ),
                            );
                        }
                        Err(err) => {
                            Self::push_integrity_error(
                                report,
                                Some(collection.primary_root_page),
                                format!(
                                    "collection '{}' index '{}' failed to lookup referenced primary version: {err}",
                                    collection.name, index.name
                                ),
                            );
                        }
                    }
                }
            }

            let mut primary_scan = primary.scan(
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                ScanDirection::Forward,
            );

            while let Some(item) = primary_scan.next().await {
                let (key, value) = match item {
                    Ok(item) => item,
                    Err(err) => {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' failed to scan primary entry: {err}",
                                collection.name
                            ),
                        );
                        continue;
                    }
                };

                if key.len() != 24 {
                    Self::push_integrity_error(
                        report,
                        None,
                        format!(
                            "collection '{}' primary key has length {}, expected 24",
                            collection.name,
                            key.len()
                        ),
                    );
                    continue;
                }

                let (doc_id, ts) = match parse_primary_key(&key) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' has malformed primary key: {err}",
                                collection.name
                            ),
                        );
                        continue;
                    }
                };

                let Some(flags) = Self::primary_cell_flags(
                    &value,
                    report,
                    &format!("collection '{}' primary version", collection.name),
                ) else {
                    continue;
                };

                if flags.tombstone {
                    if value.len() != 1 {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' tombstone primary value has length {}, expected 1",
                                collection.name,
                                value.len()
                            ),
                        );
                    }
                    continue;
                }

                let Some(body) = Self::load_primary_body(
                    storage,
                    &value,
                    report,
                    &collection.name,
                    &mut reachable_heap_pages,
                )
                .await
                else {
                    continue;
                };
                let doc = match decode_document(&body) {
                    Ok(doc) => doc,
                    Err(err) => {
                        Self::push_integrity_error(
                            report,
                            None,
                            format!(
                                "collection '{}' primary document body is invalid BSON: {err}",
                                collection.name
                            ),
                        );
                        continue;
                    }
                };

                for index in &ready_indexes {
                    let secondary = storage.open_btree(index.root_page);
                    let prefixes = match compute_index_entries(&doc, &index.field_paths) {
                        Ok(prefixes) => prefixes,
                        Err(err) => {
                            Self::push_integrity_error(
                                report,
                                None,
                                format!(
                                    "collection '{}' index '{}' cannot compute expected secondary keys: {err}",
                                    collection.name, index.name
                                ),
                            );
                            continue;
                        }
                    };

                    for prefix in prefixes {
                        let expected = make_secondary_key_from_prefix(&prefix, &doc_id, ts);
                        match secondary.get(&expected).await {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                Self::push_integrity_error(
                                    report,
                                    None,
                                    format!(
                                        "collection '{}' index '{}' is missing secondary entry for primary version",
                                        collection.name, index.name
                                    ),
                                );
                            }
                            Err(err) => {
                                Self::push_integrity_error(
                                    report,
                                    Some(index.root_page),
                                    format!(
                                        "collection '{}' index '{}' failed to lookup expected secondary entry: {err}",
                                        collection.name, index.name
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }

        Self::check_heap_page_reachability(storage, &reachable_heap_pages, report).await?;

        Ok(())
    }

    fn primary_cell_flags(
        value: &[u8],
        report: &mut IntegrityReport,
        context: &str,
    ) -> Option<CellFlags> {
        let Some(&flag_byte) = value.first() else {
            Self::push_integrity_error(report, None, format!("{context} has empty primary value"));
            return None;
        };

        if flag_byte & !0x03 != 0 {
            Self::push_integrity_error(
                report,
                None,
                format!("{context} has reserved primary cell flag bits set: {flag_byte:#04x}"),
            );
        }

        Some(CellFlags::from_byte(flag_byte))
    }

    async fn load_primary_body(
        storage: &StorageEngine,
        value: &[u8],
        report: &mut IntegrityReport,
        collection_name: &str,
        reachable_heap_pages: &mut BTreeSet<u32>,
    ) -> Option<Vec<u8>> {
        let flags = CellFlags::from_byte(value[0]);
        if value.len() < 5 {
            Self::push_integrity_error(
                report,
                None,
                format!(
                    "collection '{}' primary value is too short for body header",
                    collection_name
                ),
            );
            return None;
        }

        let body_len =
            u32::from_le_bytes(value[1..5].try_into().expect("bounds checked above")) as usize;
        if flags.external {
            if value.len() != 11 {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' external primary value has length {}, expected 11",
                        collection_name,
                        value.len()
                    ),
                );
                return None;
            }
            let href_bytes: [u8; 6] = value[5..11].try_into().expect("bounds checked above");
            let href = HeapRef::from_bytes(&href_bytes);
            let body = Self::trace_external_body(
                storage,
                href,
                collection_name,
                report,
                reachable_heap_pages,
            )
            .await?;
            if body.len() != body_len {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' external primary body length {} does not match declared {}",
                        collection_name,
                        body.len(),
                        body_len
                    ),
                );
                return None;
            }
            Some(body)
        } else {
            if value.len() != 5 + body_len {
                Self::push_integrity_error(
                    report,
                    None,
                    format!(
                        "collection '{}' inline primary value has length {}, expected {}",
                        collection_name,
                        value.len(),
                        5 + body_len
                    ),
                );
                return None;
            }
            Some(value[5..].to_vec())
        }
    }

    async fn trace_external_body(
        storage: &StorageEngine,
        href: HeapRef,
        collection_name: &str,
        report: &mut IntegrityReport,
        reachable_heap_pages: &mut BTreeSet<u32>,
    ) -> Option<Vec<u8>> {
        let (flags, total_length, first_overflow, mut body) =
            Self::read_heap_slot(storage, href, collection_name, report).await?;

        reachable_heap_pages.insert(href.page_id);

        if flags & !HEAP_HAS_OVERFLOW != 0 {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' heap slot has reserved flag bits set: {flags:#04x}",
                    collection_name
                ),
            );
        }

        if flags & HEAP_HAS_OVERFLOW == 0 {
            if first_overflow != 0 {
                Self::push_integrity_error(
                    report,
                    Some(href.page_id),
                    format!(
                        "collection '{}' heap slot has overflow pointer without overflow flag",
                        collection_name
                    ),
                );
                return None;
            }
        } else if first_overflow == 0 {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' heap slot has overflow flag without first overflow page",
                    collection_name
                ),
            );
            return None;
        }

        let mut next_page = if flags & HEAP_HAS_OVERFLOW != 0 {
            first_overflow
        } else {
            0
        };
        let mut chain_seen = BTreeSet::new();
        let mut chain_count = 0usize;

        while next_page != 0 {
            chain_count += 1;
            if chain_count > MAX_OVERFLOW_CHAIN {
                Self::push_integrity_error(
                    report,
                    Some(next_page),
                    format!(
                        "collection '{}' overflow chain exceeds maximum length",
                        collection_name
                    ),
                );
                return None;
            }
            if !chain_seen.insert(next_page) {
                Self::push_integrity_error(
                    report,
                    Some(next_page),
                    format!(
                        "collection '{}' overflow chain contains a cycle",
                        collection_name
                    ),
                );
                return None;
            }
            reachable_heap_pages.insert(next_page);

            let (following_page, chunk) =
                Self::read_overflow_page(storage, next_page, collection_name, report).await?;
            body.extend_from_slice(&chunk);
            next_page = following_page;
        }

        if body.len() != total_length {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' heap body length {} does not match heap slot total_length {}",
                    collection_name,
                    body.len(),
                    total_length
                ),
            );
            return None;
        }

        Some(body)
    }

    async fn read_heap_slot(
        storage: &StorageEngine,
        href: HeapRef,
        collection_name: &str,
        report: &mut IntegrityReport,
    ) -> Option<(u8, usize, u32, Vec<u8>)> {
        let guard = match storage.buffer_pool().fetch_page_shared(href.page_id).await {
            Ok(guard) => guard,
            Err(err) => {
                Self::push_integrity_error(
                    report,
                    Some(href.page_id),
                    format!(
                        "collection '{}' failed to read heap page {}: {err}",
                        collection_name, href.page_id
                    ),
                );
                return None;
            }
        };
        let page = match SlottedPageRef::from_buf(guard.data()) {
            Ok(page) => page,
            Err(err) => {
                Self::push_integrity_error(
                    report,
                    Some(href.page_id),
                    format!(
                        "collection '{}' heap page wrapper is invalid: {err}",
                        collection_name
                    ),
                );
                return None;
            }
        };

        if page.try_page_type() != Some(PageType::Heap) {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' external body points to page type {:?}, expected Heap",
                    collection_name,
                    page.try_page_type()
                ),
            );
            return None;
        }

        if href.slot_id >= page.num_slots() {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' external body heap slot {} is outside num_slots {}",
                    collection_name,
                    href.slot_id,
                    page.num_slots()
                ),
            );
            return None;
        }

        let slot_data = page.slot_data(href.slot_id);
        if slot_data.is_empty() {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' external body heap slot {} is deleted",
                    collection_name, href.slot_id
                ),
            );
            return None;
        }
        if slot_data.len() < HEAP_SLOT_HEADER_SIZE {
            Self::push_integrity_error(
                report,
                Some(href.page_id),
                format!(
                    "collection '{}' heap slot length {} is shorter than heap header {}",
                    collection_name,
                    slot_data.len(),
                    HEAP_SLOT_HEADER_SIZE
                ),
            );
            return None;
        }

        let flags = slot_data[0];
        let total_length =
            u32::from_le_bytes(slot_data[1..5].try_into().expect("bounds checked above")) as usize;
        let overflow_page =
            u32::from_le_bytes(slot_data[5..9].try_into().expect("bounds checked above"));
        let first_chunk = slot_data[HEAP_SLOT_HEADER_SIZE..].to_vec();

        Some((flags, total_length, overflow_page, first_chunk))
    }

    async fn read_overflow_page(
        storage: &StorageEngine,
        page_id: u32,
        collection_name: &str,
        report: &mut IntegrityReport,
    ) -> Option<(u32, Vec<u8>)> {
        let guard = match storage.buffer_pool().fetch_page_shared(page_id).await {
            Ok(guard) => guard,
            Err(err) => {
                Self::push_integrity_error(
                    report,
                    Some(page_id),
                    format!(
                        "collection '{}' failed to read overflow page {}: {err}",
                        collection_name, page_id
                    ),
                );
                return None;
            }
        };
        let page = match SlottedPageRef::from_buf(guard.data()) {
            Ok(page) => page,
            Err(err) => {
                Self::push_integrity_error(
                    report,
                    Some(page_id),
                    format!(
                        "collection '{}' overflow page wrapper is invalid: {err}",
                        collection_name
                    ),
                );
                return None;
            }
        };

        if page.try_page_type() != Some(PageType::Overflow) {
            Self::push_integrity_error(
                report,
                Some(page_id),
                format!(
                    "collection '{}' overflow chain points to page type {:?}, expected Overflow",
                    collection_name,
                    page.try_page_type()
                ),
            );
            return None;
        }

        let buf = guard.data();
        let data_len_start = PAGE_HEADER_SIZE;
        let data_start = data_len_start + OVERFLOW_DATA_LEN_SIZE;
        if buf.len() < data_start {
            Self::push_integrity_error(
                report,
                Some(page_id),
                format!(
                    "collection '{}' overflow page is too short for data length header",
                    collection_name
                ),
            );
            return None;
        }
        let data_len = u32::from_le_bytes(
            buf[data_len_start..data_start]
                .try_into()
                .expect("bounds checked above"),
        ) as usize;
        let data_end = data_start + data_len;
        if data_end > buf.len() {
            Self::push_integrity_error(
                report,
                Some(page_id),
                format!(
                    "collection '{}' overflow page data_length {} exceeds page bounds",
                    collection_name, data_len
                ),
            );
            return None;
        }

        Some((page.prev_or_ptr(), buf[data_start..data_end].to_vec()))
    }

    async fn check_heap_page_reachability(
        storage: &StorageEngine,
        reachable_heap_pages: &BTreeSet<u32>,
        report: &mut IntegrityReport,
    ) -> Result<()> {
        let page_count = storage.buffer_pool().page_storage().page_count();

        for page_id_u64 in 1..page_count {
            let Ok(page_id) = u32::try_from(page_id_u64) else {
                continue;
            };
            let guard = match storage.buffer_pool().fetch_page_shared(page_id).await {
                Ok(guard) => guard,
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        Some(page_id),
                        format!("failed to read page during heap reachability scan: {err}"),
                    );
                    continue;
                }
            };
            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(page) => page,
                Err(err) => {
                    Self::push_integrity_error(
                        report,
                        Some(page_id),
                        format!("invalid page wrapper during heap reachability scan: {err}"),
                    );
                    continue;
                }
            };

            match page.try_page_type() {
                Some(PageType::Heap) => {
                    report.stats.heap_pages += 1;
                    if !reachable_heap_pages.contains(&page_id) {
                        report.stats.orphan_heap_pages += 1;
                        Self::push_integrity_warning(
                            report,
                            Some(page_id),
                            "heap page is not reachable from any external primary body",
                        );
                    }
                }
                Some(PageType::Overflow) => {
                    report.stats.overflow_pages += 1;
                    if !reachable_heap_pages.contains(&page_id) {
                        report.stats.orphan_heap_pages += 1;
                        Self::push_integrity_warning(
                            report,
                            Some(page_id),
                            "overflow page is not reachable from any external primary body",
                        );
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn push_integrity_error(
        report: &mut IntegrityReport,
        page_id: Option<u32>,
        message: impl Into<String>,
    ) {
        report.issues.push(exdb_storage::engine::IntegrityIssue {
            severity: IntegritySeverity::Error,
            page_id,
            message: message.into(),
        });
    }

    fn push_integrity_warning(
        report: &mut IntegrityReport,
        page_id: Option<u32>,
        message: impl Into<String>,
    ) {
        report.issues.push(exdb_storage::engine::IntegrityIssue {
            severity: IntegritySeverity::Warning,
            page_id,
            message: message.into(),
        });
    }

    fn ensure_integrity_ok(phase: &str, report: IntegrityReport) -> Result<()> {
        let errors = report
            .issues
            .iter()
            .filter(|issue| issue.severity == IntegritySeverity::Error)
            .count();
        if errors == 0 {
            return Ok(());
        }

        Err(DatabaseError::IntegrityCheckFailed {
            phase: phase.to_string(),
            errors,
            issues: report.issues.len(),
        })
    }

    // ─── Transaction Entry Point ───

    /// Begin a new transaction.
    pub fn begin(&self, opts: TransactionOptions) -> Result<Transaction> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        let tx_id = self.commit_handle.allocate_tx_id();
        let begin_ts = self.commit_handle.visible_ts();

        self.active_tx_count.fetch_add(1, Ordering::AcqRel);

        Ok(Transaction::new(
            self.commit_handle.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.primary_indexes),
            Arc::clone(&self.secondary_indexes),
            self.config.transaction.clone(),
            self.config.max_doc_size,
            Arc::clone(&self.active_tx_count),
            tx_id,
            begin_ts,
            opts,
        ))
    }

    /// Commit a transaction promoted from a replica.
    ///
    /// `begin_ts` is the snapshot timestamp observed on the replica. `payload`
    /// must be produced by [`Transaction::into_promotion_payload`]. The decoded
    /// read/write sets are submitted through the normal primary commit
    /// coordinator, so OCC validation, WAL durability, replication quorum, and
    /// visibility advancement all remain on the canonical path.
    pub async fn commit_promoted_transaction(
        &self,
        begin_ts: Ts,
        payload: &[u8],
    ) -> Result<TransactionResult> {
        self.commit_promoted_transaction_with_subscription(
            begin_ts,
            payload,
            SubscriptionMode::None,
        )
        .await
    }

    /// Commit a transaction promoted from a replica with its requested
    /// subscription mode.
    ///
    /// The primary remains the commit/OCC authority. If the mode is
    /// [`SubscriptionMode::Subscribe`], OCC conflicts can return retry metadata;
    /// any primary-side success subscription handle is dropped by the caller-side
    /// promotion handler and therefore not retained on the primary.
    pub async fn commit_promoted_transaction_with_subscription(
        &self,
        begin_ts: Ts,
        payload: &[u8],
        subscription: SubscriptionMode,
    ) -> Result<TransactionResult> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        let (read_set, write_set) =
            deserialize_promotion_payload(payload).map_err(DatabaseError::Commit)?;
        let request = CommitRequest {
            tx_id: self.commit_handle.allocate_tx_id(),
            begin_ts,
            read_set,
            write_set,
            subscription,
            session_id: 0,
        };

        match self.commit_handle.commit(request).await {
            CommitResult::Success { commit_ts, .. } => Ok(TransactionResult::Success {
                commit_ts,
                subscription_handle: None,
            }),
            CommitResult::Conflict { error, retry } => {
                Ok(TransactionResult::Conflict { error, retry })
            }
            CommitResult::QuorumLost => Ok(TransactionResult::QuorumLost),
        }
    }

    /// Register a local subscription after a write transaction was promoted to
    /// and committed by the primary.
    pub async fn register_promoted_subscription(
        &self,
        tx_id: exdb_core::types::TxId,
        commit_ts: Ts,
        mut read_set: exdb_tx::ReadSet,
        write_set: &WriteSet,
        opts: TransactionOptions,
    ) -> Result<Option<SubscriptionHandle>> {
        if opts.subscription == SubscriptionMode::None {
            return Ok(None);
        }

        let index_resolver = IndexResolverImpl::new(Arc::clone(&self.catalog));
        let primary_indexes = self.primary_indexes.read().clone();
        let index_deltas =
            compute_index_deltas(write_set, &index_resolver, &primary_indexes, commit_ts).await?;
        read_set.extend_for_deltas(&index_deltas);

        let (event_tx, event_rx) = mpsc::channel(64);
        let id = self.subscriptions().write().register(
            opts.subscription,
            opts.session_id,
            tx_id,
            commit_ts,
            read_set,
            event_tx,
        );
        Ok(Some(SubscriptionHandle::new(
            id,
            tx_id,
            Arc::clone(self.subscriptions()),
            event_rx,
        )))
    }

    /// Begin a transaction from a Subscribe-mode chain continuation.
    pub fn begin_subscription_continuation(
        &self,
        continuation: exdb_tx::ChainContinuation,
        opts: TransactionOptions,
    ) -> Result<Transaction> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        let mut read_set = continuation.carried_read_set;
        read_set.set_next_query_id(continuation.first_query_id);

        self.active_tx_count.fetch_add(1, Ordering::AcqRel);

        Ok(Transaction::new_with_read_set(
            self.commit_handle.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.primary_indexes),
            Arc::clone(&self.secondary_indexes),
            self.config.transaction.clone(),
            self.config.max_doc_size,
            Arc::clone(&self.active_tx_count),
            continuation.new_tx_id,
            continuation.new_ts,
            opts,
            read_set,
        ))
    }

    /// Begin a transaction returned by a Subscribe-mode OCC conflict retry.
    pub fn begin_conflict_retry(
        &self,
        retry: exdb_tx::ConflictRetry,
        opts: TransactionOptions,
    ) -> Result<Transaction> {
        if self.shutdown.is_cancelled() {
            return Err(DatabaseError::ShuttingDown);
        }

        self.active_tx_count.fetch_add(1, Ordering::AcqRel);

        Ok(Transaction::new(
            self.commit_handle.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.primary_indexes),
            Arc::clone(&self.secondary_indexes),
            self.config.transaction.clone(),
            self.config.max_doc_size,
            Arc::clone(&self.active_tx_count),
            retry.new_tx_id,
            retry.new_ts,
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
    pub fn get_collection(&self, name: &str) -> Option<crate::catalog_cache::CollectionMeta> {
        self.catalog.read().get_collection_by_name(name).cloned()
    }

    /// Access the subscription registry.
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>> {
        self.commit_handle.subscriptions()
    }

    /// Subscribe to background index-ready events for this database.
    pub fn subscribe_index_ready(&self) -> broadcast::Receiver<IndexReadyEvent> {
        self.index_ready_tx.subscribe()
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

#[cfg(test)]
mod tests {
    use super::*;

    use exdb_core::filter::RangeExpr;
    use exdb_core::types::Scalar;
    use serde_json::json;

    struct FailingReplication;

    #[async_trait::async_trait]
    impl ReplicationHook for FailingReplication {
        async fn replicate_and_wait(
            &self,
            _lsn: exdb_storage::wal::Lsn,
            _record: &[u8],
        ) -> std::result::Result<(), String> {
            Err("test replication failure".to_string())
        }
    }

    fn assert_success(result: TransactionResult) -> Ts {
        match result {
            TransactionResult::Success { commit_ts, .. } => commit_ts,
            TransactionResult::Conflict { error, .. } => {
                panic!("unexpected conflict: {error:?}")
            }
            TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
        }
    }

    fn assert_quorum_lost(result: TransactionResult) {
        match result {
            TransactionResult::QuorumLost => {}
            TransactionResult::Success { commit_ts, .. } => {
                panic!("expected quorum lost, got success at {commit_ts}")
            }
            TransactionResult::Conflict { error, .. } => {
                panic!("expected quorum lost, got conflict: {error:?}")
            }
        }
    }

    async fn wait_index_ready(db: &Database, collection: &str, index_name: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            let indexes = tx.list_indexes(collection).unwrap();
            tx.rollback();
            if indexes
                .iter()
                .any(|index| index.name == index_name && index.state == IndexState::Ready)
            {
                return;
            }

            if tokio::time::Instant::now() >= deadline {
                panic!("index '{index_name}' did not become Ready within 10 seconds");
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn assert_clean_integrity(db: &Database) {
        let report = db.check_integrity().await.unwrap();
        assert!(
            !report.has_errors(),
            "database integrity check should not report errors: {:?}",
            report.issues
        );
        assert_eq!(
            report.stats.orphan_heap_pages, 0,
            "rollback recovery should not leave orphan heap/overflow pages: {:?}",
            report.issues
        );
    }

    #[tokio::test]
    async fn startup_rollback_is_idempotent_after_crash_before_repair_checkpoint() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("db");
        let doc_id;

        {
            let db = Database::open(&path, DatabaseConfig::default(), None)
                .await
                .unwrap();

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());

            wait_index_ready(&db, "users", "age_idx").await;
            db.close().await.unwrap();
        }

        {
            let db = Database::open(
                &path,
                DatabaseConfig::default(),
                Some(Box::new(FailingReplication)),
            )
            .await
            .unwrap();

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            doc_id = tx
                .insert(
                    "users",
                    json!({
                        "name": "Pending",
                        "age": 42,
                        "payload": "x".repeat(12_000),
                    }),
                )
                .await
                .unwrap();
            assert_quorum_lost(tx.commit().await.unwrap());
            db.crash().await;
        }

        let injected = Database::open_with_name_with_recovery_fault(
            &path,
            "default",
            DatabaseConfig::default(),
            None,
            None,
            RecoveryOpenFault::AfterRollbackBeforeRepairCheckpoint,
        )
        .await;
        let err = match injected {
            Ok(_) => panic!("expected injected recovery failure"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("injected recovery crash after rollback"),
            "expected injected recovery failure, got {err}"
        );

        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        assert!(tx.get("users", &doc_id).await.unwrap().is_none());
        let results = tx
            .query(
                "users",
                "age_idx",
                &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(42))],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(results.is_empty());
        tx.rollback();
        assert_clean_integrity(&db).await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn startup_rollback_ddl_is_idempotent_after_crash_before_repair_checkpoint() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("db");

        {
            let db = Database::open(&path, DatabaseConfig::default(), None)
                .await
                .unwrap();

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("users").await.unwrap();
            tx.create_collection("temp").await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());

            wait_index_ready(&db, "users", "age_idx").await;
            db.close().await.unwrap();
        }

        {
            let db = Database::open(
                &path,
                DatabaseConfig::default(),
                Some(Box::new(FailingReplication)),
            )
            .await
            .unwrap();

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("events").await.unwrap();
            tx.drop_collection("temp").await.unwrap();
            tx.drop_index("users", "age_idx").await.unwrap();
            tx.create_index("users", "name_idx", vec![FieldPath::single("name")])
                .await
                .unwrap();
            assert_quorum_lost(tx.commit().await.unwrap());
            db.crash().await;
        }

        let injected = Database::open_with_name_with_recovery_fault(
            &path,
            "default",
            DatabaseConfig::default(),
            None,
            None,
            RecoveryOpenFault::AfterRollbackBeforeRepairCheckpoint,
        )
        .await;
        let err = match injected {
            Ok(_) => panic!("expected injected recovery failure"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("injected recovery crash after rollback"),
            "expected injected recovery failure, got {err}"
        );

        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let collection_names = db
            .list_collections()
            .into_iter()
            .map(|collection| collection.name)
            .collect::<Vec<_>>();
        assert!(collection_names.contains(&"users".to_string()));
        assert!(collection_names.contains(&"temp".to_string()));
        assert!(!collection_names.contains(&"events".to_string()));

        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        tx.rollback();
        assert!(
            indexes
                .iter()
                .any(|index| index.name == "age_idx" && index.state == IndexState::Ready),
            "replicated ready index should survive rollback: {indexes:?}"
        );
        assert!(
            indexes.iter().all(|index| index.name != "name_idx"),
            "unreplicated index create should be rolled back: {indexes:?}"
        );
        assert_clean_integrity(&db).await;
        db.close().await.unwrap();
    }
}
