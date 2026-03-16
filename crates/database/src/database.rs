//! B6: Database — the core integration point.
//!
//! Owns all components (storage engine, indexes, catalog cache, commit system)
//! and provides the lifecycle API plus the transaction entry point.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId, Ts};
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_storage::engine::{StorageConfig, StorageEngine};
use exdb_tx::{
    CommitCoordinator, CommitHandle, NoReplication, ReplicationHook,
    SubscriptionRegistry,
};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use exdb_storage::engine::BTreeHandle;

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};
use crate::catalog_mutation_handler::CatalogMutationHandlerImpl;
use crate::catalog_persistence::{self, CatalogEntry};
use crate::catalog_recovery::CatalogRecoveryHandler;
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

    // Persistent catalog B-tree handle (shared with CatalogMutationHandler).
    // None for in-memory databases.
    catalog_btree: Option<Arc<BTreeHandle>>,

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

        // WAL replay handler tracks timestamps for recovery
        let mut handler = CatalogRecoveryHandler::new();

        let storage = Arc::new(
            StorageEngine::open(path, storage_config, &mut handler)
                .await
                .map_err(DatabaseError::Storage)?,
        );

        // Read file header for persisted state
        let file_header = storage.file_header().await;
        let recovered_ts = handler.recovered_ts.max(file_header.visible_ts.get());
        let visible_ts = handler.visible_ts.max(file_header.visible_ts.get());

        // Open the persistent catalog B-tree
        let catalog_root = file_header.catalog_root_page.get();
        let catalog_btree = storage.open_btree(catalog_root);

        // Rebuild catalog from the persistent B-tree (survives checkpoints)
        let mut catalog = CatalogCache::new();
        let mut primary_indexes: HashMap<CollectionId, Arc<PrimaryIndex>> = HashMap::new();
        let mut secondary_indexes: HashMap<IndexId, Arc<SecondaryIndex>> = HashMap::new();

        // Initialize ID counters from FileHeader
        let next_coll_id = file_header.next_collection_id.get().max(10);
        let next_idx_id = file_header.next_index_id.get().max(10);
        catalog.set_next_ids(next_coll_id, next_idx_id);

        // Phase 1: Scan catalog B-tree for persisted entries (pre-checkpoint state)
        let entries = catalog_persistence::scan_catalog(&catalog_btree)
            .await
            .map_err(DatabaseError::Storage)?;

        // First pass: collections (so primary indexes exist for secondary index creation)
        for entry in &entries {
            if let CatalogEntry::Collection { collection_id, name, primary_root_page } = entry {
                eprintln!("[open] coll {:?} '{}' primary_root_page={}", collection_id, name, primary_root_page);
                let btree = storage.open_btree(*primary_root_page);
                let primary = Arc::new(PrimaryIndex::new(
                    btree,
                    Arc::clone(&storage),
                    config.external_threshold,
                ));
                primary_indexes.insert(*collection_id, Arc::clone(&primary));
                catalog.add_collection(CollectionMeta {
                    collection_id: *collection_id,
                    name: name.clone(),
                    primary_root_page: *primary_root_page,
                    doc_count: 0,
                });
            }
        }

        // Second pass: indexes
        for entry in &entries {
            if let CatalogEntry::Index { index_id, collection_id, name, field_paths, btree_root_page } = entry {
                let btree = storage.open_btree(*btree_root_page);
                if let Some(primary) = primary_indexes.get(collection_id) {
                    let secondary = Arc::new(SecondaryIndex::new(btree, Arc::clone(primary)));
                    secondary_indexes.insert(*index_id, secondary);
                }
                catalog.add_index(IndexMeta {
                    index_id: *index_id,
                    collection_id: *collection_id,
                    name: name.clone(),
                    field_paths: field_paths.clone(),
                    root_page: *btree_root_page,
                    state: IndexState::Ready,
                });
            }
        }

        // Phase 2: Apply WAL-replayed mutations (post-checkpoint changes)
        // These are catalog mutations from commits that happened after the last checkpoint.
        for (_ts, mutations) in &handler.catalog_mutations {
            for mutation in mutations {
                Self::apply_wal_catalog_mutation(
                    mutation,
                    &storage,
                    &config,
                    &mut catalog,
                    &mut primary_indexes,
                    &mut secondary_indexes,
                    &catalog_btree,
                ).await.map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;
            }
        }

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
            Some(catalog_btree),
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
            None, // no persistent catalog for in-memory
        )
        .await
    }

    /// Internal initialization with pre-built catalog state.
    #[allow(clippy::too_many_arguments)]
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
        catalog_btree: Option<BTreeHandle>,
    ) -> Result<Self> {
        let catalog = Arc::new(RwLock::new(catalog));
        let primary_indexes = Arc::new(RwLock::new(primary_indexes));
        let secondary_indexes = Arc::new(RwLock::new(secondary_indexes));

        let index_resolver = Arc::new(IndexResolverImpl::new(Arc::clone(&catalog)));

        // Share a single catalog B-tree handle between the mutation handler and Database.
        let catalog_btree = catalog_btree.map(Arc::new);

        let catalog_handler = Arc::new(CatalogMutationHandlerImpl::new(
            Arc::clone(&storage),
            Arc::clone(&catalog),
            Arc::clone(&primary_indexes),
            Arc::clone(&secondary_indexes),
            catalog_btree.clone(),
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
            catalog_btree: catalog_btree.clone(),
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
        // Persist final visible_ts before shutdown (runner may not have updated FileHeader yet)
        let final_visible_ts = self.commit_handle.visible_ts();
        if final_visible_ts > 0 {
            self.storage.update_file_header(|fh| {
                let current = fh.visible_ts.get();
                if final_visible_ts > current {
                    fh.visible_ts.set(final_visible_ts);
                }
            }).await.map_err(DatabaseError::Storage)?;
        }

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

        // Update catalog B-tree with current root pages before checkpoint.
        // B-tree root pages can change due to splits during operation.
        eprintln!("[close] catalog_btree.is_some() = {}", self.catalog_btree.is_some());
        if let Some(ref cat_btree) = self.catalog_btree {
            eprintln!("[close] calling persist_current_root_pages, cat_btree root={}", cat_btree.root_page());
            self.persist_current_root_pages(cat_btree).await?;
            eprintln!("[close] done persisting");
        }

        // Close storage engine (includes final checkpoint for durable storage)
        self.storage.close().await.map_err(DatabaseError::Storage)?;

        Ok(())
    }

    /// Write the current root pages of all primary + secondary B-trees
    /// into the catalog B-tree so they survive reopen.
    async fn persist_current_root_pages(&self, cat_btree: &BTreeHandle) -> Result<()> {
        // Snapshot all the data we need under locks, then release before awaiting.
        let collection_updates: Vec<(CollectionId, String, u32)> = {
            let catalog = self.tx_context.catalog.read();
            let primaries = self.tx_context.primary_indexes.read();
            eprintln!("[persist] catalog has {} collections, primaries has {} entries",
                catalog.collection_count(), primaries.len());
            primaries.iter().filter_map(|(coll_id, primary)| {
                let root = primary.btree().root_page();
                catalog.get_collection_by_id(*coll_id)
                    .map(|meta| (*coll_id, meta.name.clone(), root))
            }).collect()
        };

        let index_updates: Vec<(IndexId, CollectionId, String, Vec<FieldPath>, u32)> = {
            let catalog = self.tx_context.catalog.read();
            let secondaries = self.tx_context.secondary_indexes.read();
            secondaries.iter().filter_map(|(idx_id, secondary)| {
                let root = secondary.btree().root_page();
                catalog.get_index_by_id(*idx_id)
                    .map(|meta| (*idx_id, meta.collection_id, meta.name.clone(),
                                 meta.field_paths.clone(), root))
            }).collect()
        };

        // Now do the async writes without holding any locks
        for (coll_id, name, root) in &collection_updates {
            eprintln!("[persist_root_pages] coll {:?} '{}' root={}", coll_id, name, root);
            catalog_persistence::write_collection(cat_btree, *coll_id, name, *root)
                .await.map_err(DatabaseError::Storage)?;
        }

        for (idx_id, coll_id, name, fps, root) in &index_updates {
            catalog_persistence::write_index(cat_btree, *idx_id, *coll_id, name, fps, *root)
                .await.map_err(DatabaseError::Storage)?;
        }

        // The catalog B-tree's own root page may have changed due to splits.
        // Update the FileHeader to point to the current root.
        let cat_root = cat_btree.root_page();
        self.storage.update_file_header(|fh| {
            fh.catalog_root_page.set(cat_root);
        }).await.map_err(DatabaseError::Storage)?;

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

    // ── Recovery helpers ──

    /// Apply a single WAL-replayed catalog mutation during Database::open.
    ///
    /// This handles post-checkpoint mutations that aren't yet in the catalog B-tree.
    /// The catalog B-tree is also updated so it stays consistent for the next checkpoint.
    async fn apply_wal_catalog_mutation(
        mutation: &crate::catalog_recovery::RecoveredCatalogMutation,
        storage: &Arc<StorageEngine>,
        config: &DatabaseConfig,
        catalog: &mut CatalogCache,
        primary_indexes: &mut HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &mut HashMap<IndexId, Arc<SecondaryIndex>>,
        catalog_btree: &BTreeHandle,
    ) -> std::result::Result<(), String> {
        use crate::catalog_recovery::RecoveredCatalogMutation;

        match mutation {
            RecoveredCatalogMutation::CreateCollection { name, provisional_id } => {
                if catalog.get_collection_by_id(*provisional_id).is_some() {
                    return Ok(()); // Already loaded from catalog B-tree
                }
                let btree = storage.create_btree().await.map_err(|e| e.to_string())?;
                let root_page = btree.root_page();
                let primary = Arc::new(PrimaryIndex::new(
                    btree,
                    Arc::clone(storage),
                    config.external_threshold,
                ));
                primary_indexes.insert(*provisional_id, Arc::clone(&primary));
                catalog.add_collection(CollectionMeta {
                    collection_id: *provisional_id,
                    name: name.clone(),
                    primary_root_page: root_page,
                    doc_count: 0,
                });
                // Persist to catalog B-tree
                catalog_persistence::write_collection(
                    catalog_btree, *provisional_id, name, root_page,
                ).await.map_err(|e| e.to_string())?;
            }
            RecoveredCatalogMutation::DropCollection { collection_id, .. } => {
                let indexes: Vec<IndexId> = catalog
                    .list_indexes(*collection_id)
                    .iter()
                    .map(|m| m.index_id)
                    .collect();
                catalog.remove_collection(*collection_id);
                primary_indexes.remove(collection_id);
                for idx_id in &indexes {
                    secondary_indexes.remove(idx_id);
                    catalog_persistence::remove_index(catalog_btree, *idx_id)
                        .await.map_err(|e| e.to_string())?;
                }
                catalog_persistence::remove_collection(catalog_btree, *collection_id)
                    .await.map_err(|e| e.to_string())?;
            }
            RecoveredCatalogMutation::CreateIndex { collection_id, name, field_paths, provisional_id } => {
                if catalog.get_index_by_id(*provisional_id).is_some() {
                    return Ok(()); // Already loaded from catalog B-tree
                }
                let btree = storage.create_btree().await.map_err(|e| e.to_string())?;
                let root_page = btree.root_page();
                if let Some(primary) = primary_indexes.get(collection_id) {
                    let secondary = Arc::new(SecondaryIndex::new(btree, Arc::clone(primary)));
                    secondary_indexes.insert(*provisional_id, secondary);
                }
                catalog.add_index(IndexMeta {
                    index_id: *provisional_id,
                    collection_id: *collection_id,
                    name: name.clone(),
                    field_paths: field_paths.clone(),
                    root_page,
                    state: IndexState::Ready,
                });
                catalog_persistence::write_index(
                    catalog_btree, *provisional_id, *collection_id, name, field_paths, root_page,
                ).await.map_err(|e| e.to_string())?;
            }
            RecoveredCatalogMutation::DropIndex { index_id, .. } => {
                catalog.remove_index(*index_id);
                secondary_indexes.remove(index_id);
                catalog_persistence::remove_index(catalog_btree, *index_id)
                    .await.map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }
}
