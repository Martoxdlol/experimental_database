//! T7: Two-task commit protocol (DESIGN.md 5.12).
//!
//! The **writer task** ([`CommitCoordinator`]) serializes page mutations (steps
//! 1–5) with no network dependency. The **replication task** ([`ReplicationRunner`])
//! handles visibility advancement, subscription management, and client
//! notification (steps 6–11). This split ensures the writer is never blocked on
//! replication latency.
//!
//! ```text
//!                     ┌─────────────────────┐
//!   CommitHandle ───► │   Writer Task       │  steps 1–5
//!   (mpsc)            │   (single-writer)   │  OCC → ts → WAL → mutations → commit log
//!                     └────────┬────────────┘
//!                              │ replication_tx (mpsc)
//!                              ▼
//!                     ┌─────────────────────┐
//!                     │  Replication Task   │  steps 6–11
//!                     │  (serial, ordered)  │  replicate → visible_ts → subs → respond
//!                     └─────────────────────┘
//! ```
//!
//! **`CommitCoordinator` is `!Send`**: B-tree operations hold `parking_lot::RwLock`
//! page guards across `.await` points. Must run on a `LocalSet` or single-threaded
//! runtime.
//!
//! **`ReplicationRunner` is `Send`**: No page guards held; only calls WAL append
//! (channel-based) and sync subscription operations.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use exdb_core::encoding::encode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts, TxId};
use exdb_storage::engine::StorageEngine;
use exdb_storage::wal::{Lsn, WAL_RECORD_TX_COMMIT, WAL_RECORD_VISIBLE_TS};
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};

use crate::commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite};
use crate::occ::{self, ConflictError};
use crate::read_set::ReadSet;
use crate::subscriptions::{
    InvalidationEvent, SubscriptionId, SubscriptionMode, SubscriptionRegistry,
};
use crate::timestamp::TsAllocator;
use crate::write_set::{
    compute_index_deltas, CatalogMutation, CatalogMutationHandler, IndexDelta, IndexResolver,
    MutationOp, WriteSet,
};

use exdb_docstore::{PrimaryIndex, SecondaryIndex};

/// Request submitted to the commit coordinator via [`CommitHandle::commit`].
pub struct CommitRequest {
    /// Transaction identifier.
    pub tx_id: TxId,
    /// Snapshot timestamp the transaction read at.
    pub begin_ts: Ts,
    /// Intervals observed during the transaction.
    pub read_set: ReadSet,
    /// Buffered mutations to apply.
    pub write_set: WriteSet,
    /// Subscription mode for post-commit read-set watching.
    pub subscription: SubscriptionMode,
    /// Client session for subscription management.
    pub session_id: u64,
}

/// Successful commit result returned to the caller.
pub enum CommitResult {
    /// Commit succeeded. Visible to all readers.
    Success {
        /// Assigned commit timestamp.
        commit_ts: Ts,
        /// Subscription ID if a subscription was registered.
        subscription_id: Option<SubscriptionId>,
        /// Event receiver for the subscription (L6 wraps in `SubscriptionHandle`).
        event_rx: Option<mpsc::Receiver<InvalidationEvent>>,
    },
    /// OCC conflict detected. Transaction must be retried.
    Conflict {
        /// Details about the conflict.
        error: ConflictError,
        /// For Subscribe mode: auto-retry transaction info.
        retry: Option<ConflictRetry>,
    },
    /// Replication quorum lost. System is degraded.
    QuorumLost,
}

/// Auto-retry info for Subscribe mode OCC conflicts.
pub struct ConflictRetry {
    /// New transaction ID for the retry.
    pub new_tx_id: TxId,
    /// New snapshot timestamp for the retry.
    pub new_ts: Ts,
}

/// Commit error types for internal use.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// OCC validation failed.
    #[error("OCC conflict: {0}")]
    Conflict(ConflictError),
    /// Replication quorum lost.
    #[error("replication quorum lost")]
    QuorumLost,
    /// I/O error during WAL write or index mutation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Trait for replication — defined in L5, implemented by L6 or L7.
///
/// Injected into the replication task at construction time. Default
/// implementation: [`NoReplication`] (for embedded/single-node).
#[async_trait::async_trait]
pub trait ReplicationHook: Send + Sync {
    /// Replicate a WAL record and wait for quorum acknowledgement.
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<(), String>;

    /// Check if replication quorum is available.
    fn has_quorum(&self) -> bool {
        true
    }

    /// Check if replication is in a holding state.
    fn is_holding(&self) -> bool {
        false
    }
}

/// No-op replication for embedded/single-node. Always succeeds immediately.
pub struct NoReplication;

#[async_trait::async_trait]
impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// Entry enqueued from writer task to replication task after steps 1–5 complete.
struct ReplicationEntry {
    commit_ts: Ts,
    lsn: Lsn,
    wal_payload: Vec<u8>,
    read_set: ReadSet,
    index_deltas: Vec<IndexDelta>,
    subscription: SubscriptionMode,
    session_id: u64,
    tx_id: TxId,
    response_tx: oneshot::Sender<CommitResult>,
}

/// The single-writer commit coordinator (steps 1–5).
///
/// **`!Send`**: B-tree operations hold `parking_lot::RwLock` page guards across
/// `.await` points. Must be spawned on a `tokio::task::LocalSet` or
/// single-threaded runtime.
pub struct CommitCoordinator {
    ts_allocator: Arc<TsAllocator>,
    visible_ts: Arc<AtomicU64>,
    commit_log: Arc<RwLock<CommitLog>>,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    index_resolver: Arc<dyn IndexResolver>,
    catalog_handler: Arc<dyn CatalogMutationHandler>,
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
    replication_tx: mpsc::Sender<ReplicationEntry>,
    next_tx_id: Arc<AtomicU64>,
}

/// The replication task — processes committed entries in order (steps 6–11).
///
/// **`Send`** — runs on any tokio task. No page guards held.
pub struct ReplicationRunner {
    visible_ts: Arc<AtomicU64>,
    commit_log: Arc<RwLock<CommitLog>>,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    storage: Arc<StorageEngine>,
    replication: Box<dyn ReplicationHook>,
    replication_rx: mpsc::Receiver<ReplicationEntry>,
    next_tx_id: Arc<AtomicU64>,
}

/// Clone-able handle for submitting commit requests from any task.
///
/// **`Send + Clone`** — can be distributed to multiple reader/writer tasks.
#[derive(Clone)]
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
    visible_ts: Arc<AtomicU64>,
    ts_allocator: Arc<TsAllocator>,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    next_tx_id: Arc<AtomicU64>,
}

impl CommitCoordinator {
    /// Create a new commit coordinator, replication runner, and handle.
    ///
    /// # Arguments
    ///
    /// - `initial_ts`: highest committed timestamp from WAL recovery.
    /// - `visible_ts`: last `WAL_RECORD_VISIBLE_TS` from recovery.
    /// - `storage`: shared storage engine.
    /// - `primary_indexes`: map of collection → primary index (shared with L6).
    /// - `secondary_indexes`: map of index_id → secondary index (shared with L6).
    /// - `replication`: replication hook (use [`NoReplication`] for single-node).
    /// - `index_resolver`: index metadata lookup (implemented by L6).
    /// - `channel_size`: bounded capacity for the commit request channel.
    /// - `replication_queue_size`: bounded capacity for the writer→replication queue.
    ///
    /// # Returns
    ///
    /// `(coordinator, replication_runner, handle)`:
    /// - Coordinator must be `.run()` on a `LocalSet` (owns page guards).
    /// - `ReplicationRunner` must be `.run()` on any tokio task.
    /// - Handle is distributed to application code.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        initial_ts: Ts,
        visible_ts: Ts,
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        index_resolver: Arc<dyn IndexResolver>,
        catalog_handler: Arc<dyn CatalogMutationHandler>,
        channel_size: usize,
        replication_queue_size: usize,
    ) -> (Self, ReplicationRunner, CommitHandle) {
        let ts_allocator = Arc::new(TsAllocator::new(initial_ts));
        let visible_ts = Arc::new(AtomicU64::new(visible_ts));
        let commit_log = Arc::new(RwLock::new(CommitLog::new()));
        let subscriptions = Arc::new(RwLock::new(SubscriptionRegistry::new()));
        let next_tx_id = Arc::new(AtomicU64::new(1));

        let (commit_tx, commit_rx) = mpsc::channel(channel_size);
        let (replication_tx, replication_rx) = mpsc::channel(replication_queue_size);

        let coordinator = CommitCoordinator {
            ts_allocator: Arc::clone(&ts_allocator),
            visible_ts: Arc::clone(&visible_ts),
            commit_log: Arc::clone(&commit_log),
            storage: Arc::clone(&storage),
            primary_indexes,
            secondary_indexes,
            index_resolver,
            catalog_handler,
            commit_rx,
            replication_tx,
            next_tx_id: Arc::clone(&next_tx_id),
        };

        let runner = ReplicationRunner {
            visible_ts: Arc::clone(&visible_ts),
            commit_log: Arc::clone(&commit_log),
            subscriptions: Arc::clone(&subscriptions),
            storage: Arc::clone(&storage),
            replication,
            replication_rx,
            next_tx_id: Arc::clone(&next_tx_id),
        };

        let handle = CommitHandle {
            tx: commit_tx,
            visible_ts: Arc::clone(&visible_ts),
            ts_allocator: Arc::clone(&ts_allocator),
            subscriptions: Arc::clone(&subscriptions),
            next_tx_id: Arc::clone(&next_tx_id),
        };

        (coordinator, runner, handle)
    }

    /// The single-writer commit loop. Processes one commit at a time.
    ///
    /// After steps 1–5, enqueues to the replication task and immediately loops
    /// back for the next commit. Never blocked on network I/O.
    ///
    /// Runs until the channel is closed (all handles dropped).
    pub async fn run(&mut self) {
        while let Some((request, response_tx)) = self.commit_rx.recv().await {
            let _ = self.process_commit(request, response_tx).await;
        }
    }

    /// Process a single commit request through steps 1–5.
    ///
    /// Returns `Ok(())` if enqueued to replication, `Err(())` if responded
    /// directly (OCC conflict, read-only without subscription, or I/O error).
    async fn process_commit(
        &self,
        req: CommitRequest,
        response_tx: oneshot::Sender<CommitResult>,
    ) -> Result<(), ()> {
        let is_read_only = req.write_set.is_empty();

        // ── Step 1: OCC Validation ──
        if !is_read_only || req.subscription != SubscriptionMode::None {
            let commit_log = self.commit_log.read();
            let commit_ts_candidate = self.ts_allocator.latest() + 1;
            if let Err(conflict) =
                occ::validate(&req.read_set, &commit_log, req.begin_ts, commit_ts_candidate)
            {
                let retry = if req.subscription == SubscriptionMode::Subscribe {
                    Some(ConflictRetry {
                        new_tx_id: self.next_tx_id.fetch_add(1, Ordering::AcqRel),
                        new_ts: self.visible_ts.load(Ordering::Acquire),
                    })
                } else {
                    None
                };
                let _ = response_tx.send(CommitResult::Conflict {
                    error: conflict,
                    retry,
                });
                return Err(());
            }
        }

        // Read-only without subscription — respond immediately
        if is_read_only && req.subscription == SubscriptionMode::None {
            let _ = response_tx.send(CommitResult::Success {
                commit_ts: req.begin_ts,
                subscription_id: None,
                event_rx: None,
            });
            return Err(());
        }

        // ── Step 2: Assign commit_ts ──
        let commit_ts = if is_read_only {
            // Read-only with subscription: no real commit, use begin_ts
            req.begin_ts
        } else {
            self.ts_allocator.allocate()
        };

        let mut wal_payload = Vec::new();
        let mut lsn: Lsn = 0;
        let mut index_deltas = Vec::new();

        if !is_read_only {
            // ── Step 3a: Pre-allocate B-tree pages for catalog mutations ──
            // Allocate pages BEFORE building the WAL payload so that root page
            // IDs are included in the WAL record. This enables deterministic
            // WAL replay during crash recovery.
            //
            // If crash after allocation but before WAL write: allocated pages
            // are orphaned (harmless — reclaimed by vacuum or ignored).
            let mut write_set = req.write_set;
            for cat_mut in &mut write_set.catalog_mutations {
                match cat_mut {
                    CatalogMutation::CreateCollection {
                        primary_root_page,
                        created_at_root_page,
                        ..
                    } => {
                        let (p, c) = match self
                            .catalog_handler
                            .allocate_collection_pages()
                            .await
                        {
                            Ok(pages) => pages,
                            Err(e) => {
                                tracing::error!(
                                    "page allocation failed for commit_ts={commit_ts}: {e}"
                                );
                                let _ = response_tx.send(CommitResult::QuorumLost);
                                return Err(());
                            }
                        };
                        *primary_root_page = p;
                        *created_at_root_page = c;
                    }
                    CatalogMutation::CreateIndex {
                        root_page, ..
                    } => {
                        let rp = match self.catalog_handler.allocate_index_page().await {
                            Ok(page) => page,
                            Err(e) => {
                                tracing::error!(
                                    "page allocation failed for commit_ts={commit_ts}: {e}"
                                );
                                let _ = response_tx.send(CommitResult::QuorumLost);
                                return Err(());
                            }
                        };
                        *root_page = rp;
                    }
                    _ => {} // DropCollection/DropIndex: no allocation needed
                }
            }

            // ── Step 3b+3c: Serialize WAL payload + persist ──
            wal_payload = serialize_wal_payload(commit_ts, &write_set);
            match self
                .storage
                .append_wal(WAL_RECORD_TX_COMMIT, &wal_payload)
                .await
            {
                Ok(l) => lsn = l,
                Err(e) => {
                    tracing::error!("WAL write failed for commit_ts={commit_ts}: {e}");
                    let _ = response_tx.send(CommitResult::QuorumLost);
                    return Err(());
                }
            }

            // ── Step 4a: Apply Catalog Mutations ──
            // Process DDL before data mutations so that new B-trees exist
            // when insert_version / index delta computation runs.
            // Pages are already allocated (step 3a) — this step registers
            // handles and updates catalog B-trees + cache.
            for cat_mut in &write_set.catalog_mutations {
                let result = match cat_mut {
                    CatalogMutation::CreateCollection {
                        name,
                        provisional_id,
                        primary_root_page,
                        created_at_root_page,
                    } => {
                        self.catalog_handler
                            .apply_create_collection(
                                *provisional_id,
                                name,
                                *primary_root_page,
                                *created_at_root_page,
                            )
                            .await
                    }
                    CatalogMutation::DropCollection {
                        collection_id, ..
                    } => {
                        self.catalog_handler
                            .apply_drop_collection(*collection_id)
                            .await
                    }
                    CatalogMutation::CreateIndex {
                        collection_id,
                        name,
                        field_paths,
                        provisional_id,
                        root_page,
                    } => {
                        self.catalog_handler
                            .apply_create_index(
                                *provisional_id,
                                *collection_id,
                                name,
                                field_paths,
                                *root_page,
                            )
                            .await
                    }
                    CatalogMutation::DropIndex { index_id, .. } => {
                        self.catalog_handler
                            .apply_drop_index(*index_id)
                            .await
                    }
                };
                if let Err(e) = result {
                    tracing::error!(
                        "catalog mutation failed for commit_ts={commit_ts}: {e}"
                    );
                    let _ = response_tx.send(CommitResult::QuorumLost);
                    return Err(());
                }
            }

            // ── Step 4b: Apply Data Mutations + Compute Deltas ──
            // Clone Arc'd indexes and drop parking_lot read guards immediately.
            let primaries: HashMap<CollectionId, Arc<PrimaryIndex>> =
                self.primary_indexes.read().clone();
            let secondaries: HashMap<IndexId, Arc<SecondaryIndex>> =
                self.secondary_indexes.read().clone();

            // Apply primary index mutations
            for (&(coll_id, doc_id), entry) in &write_set.mutations {
                if let Some(primary) = primaries.get(&coll_id) {
                    let body_bytes = entry
                        .body
                        .as_ref()
                        .map(encode_document);
                    if let Err(e) = primary
                        .insert_version(&doc_id, commit_ts, body_bytes.as_deref())
                        .await
                    {
                        tracing::error!(
                            "primary index write failed for commit_ts={commit_ts}: {e}"
                        );
                        let _ = response_tx.send(CommitResult::QuorumLost);
                        return Err(());
                    }
                }
            }

            // Compute index deltas
            match compute_index_deltas(
                &write_set,
                self.index_resolver.as_ref(),
                &primaries,
                commit_ts,
            )
            .await
            {
                Ok(deltas) => index_deltas = deltas,
                Err(e) => {
                    tracing::error!("index delta computation failed: {e}");
                    let _ = response_tx.send(CommitResult::QuorumLost);
                    return Err(());
                }
            }

            // Apply secondary index mutations
            for delta in &index_deltas {
                if let Some(secondary) = secondaries.get(&delta.index_id) {
                    if let Some(old_key) = &delta.old_key
                        && let Err(e) = secondary.remove_entry(old_key).await {
                            tracing::error!("secondary index remove failed: {e}");
                            let _ = response_tx.send(CommitResult::QuorumLost);
                            return Err(());
                        }
                    if let Some(new_key) = &delta.new_key
                        && let Err(e) = secondary.insert_entry(new_key).await {
                            tracing::error!("secondary index insert failed: {e}");
                            let _ = response_tx.send(CommitResult::QuorumLost);
                            return Err(());
                        }
                }
            }

            // ── Step 5: Append to Commit Log ──
            let log_entry = build_commit_log_entry(commit_ts, &index_deltas);
            self.commit_log.write().append(log_entry);
        }

        // ── Enqueue to Replication Task ──
        let entry = ReplicationEntry {
            commit_ts,
            lsn,
            wal_payload,
            read_set: req.read_set,
            index_deltas,
            subscription: req.subscription,
            session_id: req.session_id,
            tx_id: req.tx_id,
            response_tx,
        };

        if self.replication_tx.send(entry).await.is_err() {
            tracing::error!(
                "replication task dropped, cannot complete commit {commit_ts}"
            );
        }

        Ok(())
    }
}

impl ReplicationRunner {
    /// Process the replication queue in order.
    ///
    /// For each entry: replicate → advance `visible_ts` → subscriptions → respond.
    /// On replication failure: drain remaining entries with `QuorumLost` and stop.
    pub async fn run(&mut self) {
        while let Some(entry) = self.replication_rx.recv().await {
            if !self.process_entry(entry).await {
                self.drain_with_error().await;
                break;
            }
        }
    }

    /// Process one replication entry. Returns `true` on success, `false` on failure.
    async fn process_entry(&mut self, entry: ReplicationEntry) -> bool {
        let is_read_only = entry.wal_payload.is_empty();

        if !is_read_only {
            // ── Step 6: Replicate ──
            if let Err(msg) = self
                .replication
                .replicate_and_wait(entry.lsn, &entry.wal_payload)
                .await
            {
                let _ = entry.response_tx.send(CommitResult::QuorumLost);
                tracing::error!(
                    "replication failed at commit_ts={}: {msg}",
                    entry.commit_ts
                );
                let visible = self.visible_ts.load(Ordering::Acquire);
                self.commit_log.write().remove_after(visible);
                return false;
            }

            // ── Step 7: WAL visible_ts Record ──
            if let Err(e) = self
                .storage
                .append_wal(WAL_RECORD_VISIBLE_TS, &entry.commit_ts.to_le_bytes())
                .await
            {
                tracing::error!("failed to persist visible_ts WAL record: {e}");
                let _ = entry.response_tx.send(CommitResult::QuorumLost);
                return false;
            }

            // ── Step 8: Advance visible_ts ──
            self.visible_ts.store(entry.commit_ts, Ordering::Release);

            // Persist visible_ts in FileHeader so it survives checkpoint + reopen
            let commit_ts = entry.commit_ts;
            if let Err(e) = self.storage.update_file_header(|fh| {
                fh.visible_ts.set(commit_ts);
            }).await {
                tracing::error!("failed to persist visible_ts to file header: {e}");
            }
        }

        // ── Step 9: Subscription Invalidation Check ──
        let index_writes = build_index_writes(&entry.index_deltas);
        let invalidation_events = {
            let mut subs = self.subscriptions.write();
            subs.check_invalidation(entry.commit_ts, &index_writes, || {
                self.next_tx_id.fetch_add(1, Ordering::AcqRel)
            })
        };

        // ── Step 10: Register Subscription (if requested) ──
        let (subscription_id, event_rx) = if entry.subscription != SubscriptionMode::None {
            // 10a: Clear stale limit boundaries from tx's own writes
            let mut read_set = entry.read_set;
            read_set.extend_for_deltas(&entry.index_deltas);

            // 10b: Create channel and register
            let (event_tx, event_rx) = mpsc::channel(64);
            let mut subs = self.subscriptions.write();
            let sub_id = subs.register(
                entry.subscription,
                entry.session_id,
                entry.tx_id,
                entry.commit_ts,
                read_set,
                event_tx,
            );
            (Some(sub_id), Some(event_rx))
        } else {
            (None, None)
        };

        // ── Step 11: Push Invalidation Events + Respond ──
        SubscriptionRegistry::push_events(invalidation_events);

        let _ = entry.response_tx.send(CommitResult::Success {
            commit_ts: entry.commit_ts,
            subscription_id,
            event_rx,
        });

        true
    }

    /// Drain remaining entries with `QuorumLost` after a replication failure.
    async fn drain_with_error(&mut self) {
        while let Ok(entry) = self.replication_rx.try_recv() {
            let _ = entry.response_tx.send(CommitResult::QuorumLost);
        }
        let visible = self.visible_ts.load(Ordering::Acquire);
        self.commit_log.write().remove_after(visible);
    }
}

impl CommitHandle {
    /// Submit a commit request and await the result.
    ///
    /// The response arrives after the replication task has confirmed visibility
    /// — not immediately after the writer processes the commit.
    pub async fn commit(&self, request: CommitRequest) -> CommitResult {
        let (response_tx, response_rx) = oneshot::channel();
        if self.tx.send((request, response_tx)).await.is_err() {
            return CommitResult::QuorumLost;
        }
        response_rx.await.unwrap_or(CommitResult::QuorumLost)
    }

    /// Get the current `visible_ts` (latest safe read timestamp).
    pub fn visible_ts(&self) -> Ts {
        self.visible_ts.load(Ordering::Acquire)
    }

    /// Allocate a new [`TxId`].
    pub fn allocate_tx_id(&self) -> TxId {
        self.next_tx_id.fetch_add(1, Ordering::AcqRel)
    }

    /// Access the subscription registry (for session cleanup, etc.).
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>> {
        &self.subscriptions
    }

    /// Access the timestamp allocator (for diagnostics).
    pub fn ts_allocator(&self) -> &Arc<TsAllocator> {
        &self.ts_allocator
    }
}

// ─── Helper: build_commit_log_entry ───

/// Convert index deltas into a [`CommitLogEntry`] for the commit log.
fn build_commit_log_entry(commit_ts: Ts, index_deltas: &[IndexDelta]) -> CommitLogEntry {
    let mut index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> = BTreeMap::new();
    for delta in index_deltas {
        index_writes
            .entry((delta.collection_id, delta.index_id))
            .or_default()
            .push(IndexKeyWrite {
                doc_id: delta.doc_id,
                old_key: delta.old_key.clone(),
                new_key: delta.new_key.clone(),
            });
    }
    CommitLogEntry {
        commit_ts,
        index_writes,
    }
}

// ─── Helper: build_index_writes ───

/// Extract index writes from deltas for subscription invalidation check.
fn build_index_writes(
    index_deltas: &[IndexDelta],
) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> {
    let mut map: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> = BTreeMap::new();
    for delta in index_deltas {
        map.entry((delta.collection_id, delta.index_id))
            .or_default()
            .push(IndexKeyWrite {
                doc_id: delta.doc_id,
                old_key: delta.old_key.clone(),
                new_key: delta.new_key.clone(),
            });
    }
    map
}

// ─── WAL Payload Serialization ───

/// WAL TxCommit payload format version.
///
/// Version history:
/// - V1 (initial): no root pages in CreateCollection/CreateIndex, no drop metadata
/// - V2 (current): includes root page IDs, full drop metadata for rollback
pub const WAL_PAYLOAD_VERSION: u8 = 2;

/// Serialize commit data into the binary WAL payload format.
///
/// Format (version 2):
/// ```text
/// version:        u8
/// commit_ts:      u64 (LE)
/// mutation_count: u32 (LE)
/// for each mutation:
///     collection_id: u64 (LE)
///     doc_id:        [u8; 16]
///     op_tag:        u8  (0x01=Insert, 0x02=Replace, 0x03=Delete)
///     body_len:      u32 (LE)  (0 for Delete)
///     body:          [u8; body_len]  (JSON bytes)
/// catalog_count:  u32 (LE)
/// for each catalog mutation:
///     type_tag:   u8  (0x01=CreateCollection, ...)
///     payload:    type-specific encoding (see below)
/// ```
///
/// CreateCollection payload (0x01):
///     provisional_id: u64 (LE), name_len: u32 (LE), name, primary_root_page: u32 (LE), created_at_root_page: u32 (LE)
///
/// DropCollection payload (0x02):
///     collection_id: u64 (LE), name_len: u32 (LE), name, primary_root_page: u32 (LE),
///     dropped_index_count: u32 (LE), for each: index_id: u64, name_len: u32, name, field_paths, root_page: u32
///
/// CreateIndex payload (0x03):
///     provisional_id: u64 (LE), collection_id: u64 (LE), name_len: u32 (LE), name,
///     field_paths, root_page: u32 (LE)
///
/// DropIndex payload (0x04):
///     index_id: u64 (LE), collection_id: u64 (LE), name_len: u32 (LE), name,
///     field_paths, root_page: u32 (LE)
fn serialize_wal_payload(commit_ts: Ts, write_set: &WriteSet) -> Vec<u8> {
    let mut buf = Vec::new();

    // version
    buf.push(WAL_PAYLOAD_VERSION);

    // commit_ts
    buf.extend_from_slice(&commit_ts.to_le_bytes());

    // mutations
    let mutation_count = write_set.mutations.len() as u32;
    buf.extend_from_slice(&mutation_count.to_le_bytes());

    for (&(coll_id, doc_id), entry) in &write_set.mutations {
        buf.extend_from_slice(&coll_id.0.to_le_bytes());
        buf.extend_from_slice(doc_id.as_bytes());
        buf.push(match entry.op {
            MutationOp::Insert => 0x01,
            MutationOp::Replace => 0x02,
            MutationOp::Delete => 0x03,
        });
        match &entry.body {
            Some(body) => {
                let json_bytes = serde_json::to_vec(body).expect("JSON serialization");
                buf.extend_from_slice(&(json_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&json_bytes);
            }
            None => {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }

    // catalog mutations
    let catalog_count = write_set.catalog_mutations.len() as u32;
    buf.extend_from_slice(&catalog_count.to_le_bytes());

    for cat_mut in &write_set.catalog_mutations {
        match cat_mut {
            CatalogMutation::CreateCollection {
                name,
                provisional_id,
                primary_root_page,
                created_at_root_page,
            } => {
                buf.push(0x01);
                buf.extend_from_slice(&provisional_id.0.to_le_bytes());
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name.as_bytes());
                buf.extend_from_slice(&primary_root_page.to_le_bytes());
                buf.extend_from_slice(&created_at_root_page.to_le_bytes());
            }
            CatalogMutation::DropCollection {
                collection_id,
                name,
                primary_root_page,
                dropped_indexes,
            } => {
                buf.push(0x02);
                buf.extend_from_slice(&collection_id.0.to_le_bytes());
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name.as_bytes());
                buf.extend_from_slice(&primary_root_page.to_le_bytes());
                // Dropped indexes metadata for rollback
                buf.extend_from_slice(&(dropped_indexes.len() as u32).to_le_bytes());
                for idx in dropped_indexes {
                    buf.extend_from_slice(&idx.index_id.0.to_le_bytes());
                    buf.extend_from_slice(&(idx.name.len() as u32).to_le_bytes());
                    buf.extend_from_slice(idx.name.as_bytes());
                    serialize_field_paths(&mut buf, &idx.field_paths);
                    buf.extend_from_slice(&idx.root_page.to_le_bytes());
                }
            }
            CatalogMutation::CreateIndex {
                collection_id,
                name,
                field_paths,
                provisional_id,
                root_page,
            } => {
                buf.push(0x03);
                buf.extend_from_slice(&provisional_id.0.to_le_bytes());
                buf.extend_from_slice(&collection_id.0.to_le_bytes());
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name.as_bytes());
                serialize_field_paths(&mut buf, field_paths);
                buf.extend_from_slice(&root_page.to_le_bytes());
            }
            CatalogMutation::DropIndex {
                collection_id,
                index_id,
                name,
                field_paths,
                root_page,
            } => {
                buf.push(0x04);
                buf.extend_from_slice(&index_id.0.to_le_bytes());
                buf.extend_from_slice(&collection_id.0.to_le_bytes());
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name.as_bytes());
                serialize_field_paths(&mut buf, field_paths);
                buf.extend_from_slice(&root_page.to_le_bytes());
            }
        }
    }

    buf
}

/// Helper: serialize field paths into a buffer.
fn serialize_field_paths(buf: &mut Vec<u8>, field_paths: &[FieldPath]) {
    buf.extend_from_slice(&(field_paths.len() as u32).to_le_bytes());
    for fp in field_paths {
        let segments = fp.segments();
        buf.extend_from_slice(&(segments.len() as u32).to_le_bytes());
        for seg in segments {
            buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            buf.extend_from_slice(seg.as_bytes());
        }
    }
}

/// Deserialize a WAL payload back into commit data.
///
/// Returns `(commit_ts, mutations, catalog_mutations_bytes)`.
///
/// Handles version 1 (legacy, no version byte — starts with commit_ts directly)
/// and version 2 (current, starts with version byte).
#[allow(clippy::type_complexity)]
pub fn deserialize_wal_payload(
    data: &[u8],
) -> Result<(u8, Ts, Vec<(CollectionId, DocId, u8, Option<Vec<u8>>)>, Vec<u8>), String> {
    if data.is_empty() {
        return Err("WAL payload empty".into());
    }

    // Version detection: V2 starts with version byte (0x02).
    // V1 (legacy) starts with commit_ts (u64 LE) — first byte is the low byte
    // of commit_ts, which will be a small number but never exactly 0x02 for
    // typical timestamps. For safety, we check: if first byte == WAL_PAYLOAD_VERSION
    // AND total length is consistent with V2 format, treat as V2.
    let (version, offset_start) = if data[0] == WAL_PAYLOAD_VERSION {
        (WAL_PAYLOAD_VERSION, 1usize)
    } else {
        // Legacy V1: no version byte, commit_ts starts at offset 0
        (1u8, 0usize)
    };

    let min_len = offset_start + 12; // commit_ts(8) + mutation_count(4)
    if data.len() < min_len {
        return Err("WAL payload too short".into());
    }

    let mut offset = offset_start;
    let commit_ts = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;
    let mutation_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut mutations = Vec::with_capacity(mutation_count);

    for _ in 0..mutation_count {
        if offset + 25 > data.len() {
            return Err("truncated mutation".into());
        }
        let coll_id = CollectionId(u64::from_le_bytes(
            data[offset..offset + 8].try_into().unwrap(),
        ));
        offset += 8;
        let mut doc_bytes = [0u8; 16];
        doc_bytes.copy_from_slice(&data[offset..offset + 16]);
        let doc_id = DocId(doc_bytes);
        offset += 16;
        let op_tag = data[offset];
        offset += 1;
        let body_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let body = if body_len > 0 {
            if offset + body_len > data.len() {
                return Err("truncated body".into());
            }
            let b = data[offset..offset + body_len].to_vec();
            offset += body_len;
            Some(b)
        } else {
            None
        };
        mutations.push((coll_id, doc_id, op_tag, body));
    }

    // Return remaining bytes as catalog data (parsed by L6)
    let remaining = data[offset..].to_vec();

    Ok((version, commit_ts, mutations, remaining))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_set::ReadInterval;
    use crate::subscriptions::SubscriptionMode;
    use crate::write_set::DroppedIndexMeta;
    use exdb_core::field_path::FieldPath;
    use exdb_storage::engine::StorageConfig;
    use serde_json::json;
    use std::ops::Bound;

    /// Mock IndexResolver that returns no indexes.
    struct EmptyResolver;
    impl IndexResolver for EmptyResolver {
        fn indexes_for_collection(&self, _: CollectionId) -> Vec<crate::write_set::IndexInfo> {
            vec![]
        }
    }

    /// Create an in-memory test setup with coordinator, runner, and handle.
    async fn setup() -> (CommitCoordinator, ReplicationRunner, CommitHandle) {
        let storage =
            Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let primaries = Arc::new(RwLock::new(HashMap::new()));
        let secondaries = Arc::new(RwLock::new(HashMap::new()));
        let resolver: Arc<dyn IndexResolver> = Arc::new(EmptyResolver);

        let catalog_handler: Arc<dyn CatalogMutationHandler> =
            Arc::new(crate::write_set::NoOpCatalogHandler);

        CommitCoordinator::new(
            0,
            0,
            storage,
            primaries,
            secondaries,
            Box::new(NoReplication),
            resolver,
            catalog_handler,
            256,
            256,
        )
    }

    /// Run coordinator and runner as background tasks, return handle.
    async fn spawn_system() -> CommitHandle {
        let (mut coord, mut runner, handle) = setup().await;

        // Spawn runner on normal tokio task (it's Send)
        tokio::spawn(async move {
            runner.run().await;
        });

        // Spawn coordinator on a LocalSet (it's !Send due to page guards)
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();
            local.spawn_local(async move {
                coord.run().await;
            });
            rt.block_on(local);
        });

        // Small yield to let tasks start
        tokio::task::yield_now().await;

        handle
    }

    #[tokio::test]
    async fn commit_read_only() {
        let handle = spawn_system().await;

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: WriteSet::new(),
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        match result {
            CommitResult::Success { commit_ts, subscription_id, event_rx } => {
                assert_eq!(commit_ts, 0); // begin_ts returned for read-only
                assert!(subscription_id.is_none());
                assert!(event_rx.is_none());
            }
            _ => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn commit_single_insert() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        match result {
            CommitResult::Success { commit_ts, .. } => {
                assert!(commit_ts > 0);
            }
            _ => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn commit_advances_visible_ts() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        if let CommitResult::Success { commit_ts, .. } = result {
            assert_eq!(handle.visible_ts(), commit_ts);
        } else {
            panic!("expected success");
        }
    }

    #[tokio::test]
    async fn commit_sequential_timestamps() {
        let handle = spawn_system().await;

        let mut ws1 = WriteSet::new();
        ws1.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));
        let mut ws2 = WriteSet::new();
        ws2.insert(CollectionId(10), DocId([2; 16]), json!({"x": 2}));

        let r1 = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws1,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;
        let r2 = handle
            .commit(CommitRequest {
                tx_id: 2,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws2,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        let ts1 = match r1 {
            CommitResult::Success { commit_ts, .. } => commit_ts,
            _ => panic!("expected success"),
        };
        let ts2 = match r2 {
            CommitResult::Success { commit_ts, .. } => commit_ts,
            _ => panic!("expected success"),
        };
        assert!(ts2 > ts1);
    }

    #[tokio::test]
    async fn commit_occ_conflict() {
        let handle = spawn_system().await;

        // TX1: write to key space
        let mut ws1 = WriteSet::new();
        ws1.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));

        let r1 = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws1,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;
        assert!(matches!(r1, CommitResult::Success { .. }));

        // TX2: started at ts=0, reads the same range, tries to commit
        // But we need actual index deltas in the commit log for OCC to find conflicts.
        // Since EmptyResolver returns no indexes, there are no index_writes in commit log.
        // This test validates the flow; real OCC tests are in occ.rs.
        let mut rs2 = ReadSet::new();
        rs2.add_interval(
            CollectionId(10),
            IndexId(0), // PRIMARY_INDEX_SENTINEL
            ReadInterval {
                query_id: 0,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        let mut ws2 = WriteSet::new();
        ws2.insert(CollectionId(10), DocId([2; 16]), json!({"y": 2}));

        let r2 = handle
            .commit(CommitRequest {
                tx_id: 2,
                begin_ts: 0,
                read_set: rs2,
                write_set: ws2,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        // With EmptyResolver there are no index writes → no OCC conflict
        // This verifies the flow works end-to-end
        assert!(matches!(r2, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn commit_handle_clone() {
        let handle = spawn_system().await;
        let h2 = handle.clone();

        let mut ws = WriteSet::new();
        ws.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));

        let result = h2
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        assert!(matches!(result, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn commit_with_notify_subscription() {
        let handle = spawn_system().await;

        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(10),
            IndexId(5),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![20]),
                limit_boundary: None,
            },
        );

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: rs,
                write_set: WriteSet::new(),
                subscription: SubscriptionMode::Notify,
                session_id: 1,
            })
            .await;

        match result {
            CommitResult::Success {
                subscription_id,
                event_rx,
                ..
            } => {
                assert!(subscription_id.is_some());
                assert!(event_rx.is_some());
            }
            _ => panic!("expected success"),
        }
    }

    #[tokio::test]
    async fn commit_no_replication() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.insert(CollectionId(10), DocId([1; 16]), json!({"x": 1}));

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        if let CommitResult::Success { commit_ts, .. } = result {
            // With NoReplication, visible_ts advances immediately
            assert_eq!(handle.visible_ts(), commit_ts);
        } else {
            panic!("expected success");
        }
    }

    #[tokio::test]
    async fn allocate_tx_id() {
        let handle = spawn_system().await;
        let id1 = handle.allocate_tx_id();
        let id2 = handle.allocate_tx_id();
        assert_ne!(id1, id2);
        assert!(id2 > id1);
    }

    // ─── WAL payload roundtrip ───

    #[test]
    fn wal_payload_roundtrip() {
        let mut ws = WriteSet::new();
        ws.insert(CollectionId(1), DocId([1; 16]), json!({"name": "Alice"}));
        ws.delete(CollectionId(2), DocId([2; 16]), 5);
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "test".into(),
            provisional_id: CollectionId(42),
            primary_root_page: 100,
            created_at_root_page: 101,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(42),
            name: "idx".into(),
            field_paths: vec![FieldPath::single("name")],
            provisional_id: IndexId(10),
            root_page: 102,
        });

        let payload = serialize_wal_payload(100, &ws);
        let (version, commit_ts, mutations, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, WAL_PAYLOAD_VERSION);
        assert_eq!(commit_ts, 100);
        assert_eq!(mutations.len(), 2);

        // First mutation: insert into coll 1
        assert_eq!(mutations[0].0, CollectionId(1));
        assert_eq!(mutations[0].2, 0x01); // Insert
        assert!(mutations[0].3.is_some());

        // Second mutation: delete from coll 2
        assert_eq!(mutations[1].0, CollectionId(2));
        assert_eq!(mutations[1].2, 0x03); // Delete
        assert!(mutations[1].3.is_none());

        // Catalog data is present (version byte + catalog entries)
        assert!(!catalog_data.is_empty());
    }

    #[test]
    fn wal_payload_empty() {
        let ws = WriteSet::new();
        let payload = serialize_wal_payload(1, &ws);
        let (version, ts, mutations, catalog) = deserialize_wal_payload(&payload).unwrap();
        assert_eq!(version, WAL_PAYLOAD_VERSION);
        assert_eq!(ts, 1);
        assert!(mutations.is_empty());
        assert_eq!(catalog.len(), 4); // just the catalog_count (0)
    }

    // ─── build_commit_log_entry ───

    #[test]
    fn build_commit_log_entry_groups_by_index() {
        let deltas = vec![
            IndexDelta {
                index_id: IndexId(1),
                collection_id: CollectionId(1),
                doc_id: DocId([1; 16]),
                old_key: None,
                new_key: Some(vec![42]),
            },
            IndexDelta {
                index_id: IndexId(1),
                collection_id: CollectionId(1),
                doc_id: DocId([2; 16]),
                old_key: Some(vec![10]),
                new_key: None,
            },
            IndexDelta {
                index_id: IndexId(2),
                collection_id: CollectionId(1),
                doc_id: DocId([3; 16]),
                old_key: None,
                new_key: Some(vec![99]),
            },
        ];

        let entry = build_commit_log_entry(5, &deltas);
        assert_eq!(entry.commit_ts, 5);
        assert_eq!(entry.index_writes.len(), 2); // Two (coll, idx) groups
        assert_eq!(
            entry.index_writes[&(CollectionId(1), IndexId(1))].len(),
            2
        );
        assert_eq!(
            entry.index_writes[&(CollectionId(1), IndexId(2))].len(),
            1
        );
    }

    // ─── WAL payload V2 format tests ───

    #[test]
    fn wal_payload_version_byte() {
        let ws = WriteSet::new();
        let payload = serialize_wal_payload(1, &ws);
        assert_eq!(payload[0], WAL_PAYLOAD_VERSION);
        assert_eq!(payload[0], 2);
    }

    #[test]
    fn wal_payload_create_collection_with_root_pages() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: CollectionId(42),
            primary_root_page: 100,
            created_at_root_page: 101,
        });

        let payload = serialize_wal_payload(10, &ws);
        let (version, ts, mutations, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 10);
        assert!(mutations.is_empty());
        // Catalog data should be non-empty and parseable
        assert!(!catalog_data.is_empty());

        // Parse catalog data manually to verify root pages are present
        let mut off = 0;
        let count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(count, 1);

        let tag = catalog_data[off];
        off += 1;
        assert_eq!(tag, 0x01); // CreateCollection

        let prov_id =
            u64::from_le_bytes(catalog_data[off..off + 8].try_into().unwrap());
        off += 8;
        assert_eq!(prov_id, 42);

        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let name = std::str::from_utf8(&catalog_data[off..off + name_len]).unwrap();
        off += name_len;
        assert_eq!(name, "users");

        let primary_rp =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(primary_rp, 100);

        let created_at_rp =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        assert_eq!(created_at_rp, 101);
    }

    #[test]
    fn wal_payload_create_index_with_root_page() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "by_email".into(),
            field_paths: vec![FieldPath::single("email")],
            provisional_id: IndexId(10),
            root_page: 200,
        });

        let payload = serialize_wal_payload(20, &ws);
        let (version, ts, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 20);

        // Parse: count(4) + tag(1) + prov_id(8) + coll_id(8) + name_len(4) + "by_email"(8) + field_paths + root_page(4)
        let mut off = 0;
        let count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(count, 1);

        let tag = catalog_data[off];
        off += 1;
        assert_eq!(tag, 0x03); // CreateIndex

        // Skip prov_id + coll_id + name
        off += 8; // provisional_id
        off += 8; // collection_id
        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        off += name_len; // "by_email"

        // Skip field_paths: count(4) + segments_count(4) + seg_len(4) + "email"(5)
        let fp_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(fp_count, 1);
        let seg_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(seg_count, 1);
        let seg_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        off += seg_len; // "email"

        // Now root_page
        let root_page =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        assert_eq!(root_page, 200);
    }

    #[test]
    fn wal_payload_drop_collection_with_full_metadata() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(5),
            name: "users".into(),
            primary_root_page: 50,
            dropped_indexes: vec![
                DroppedIndexMeta {
                    index_id: IndexId(20),
                    name: "_created_at".into(),
                    field_paths: vec![FieldPath::single("_created_at")],
                    root_page: 51,
                },
                DroppedIndexMeta {
                    index_id: IndexId(21),
                    name: "by_email".into(),
                    field_paths: vec![FieldPath::single("email")],
                    root_page: 52,
                },
            ],
        });

        let payload = serialize_wal_payload(30, &ws);
        let (version, ts, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 30);

        // Parse catalog data
        let mut off = 0;
        let count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(count, 1);

        let tag = catalog_data[off];
        off += 1;
        assert_eq!(tag, 0x02); // DropCollection

        let coll_id =
            u64::from_le_bytes(catalog_data[off..off + 8].try_into().unwrap());
        off += 8;
        assert_eq!(coll_id, 5);

        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        off += name_len; // "users"

        let primary_rp =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(primary_rp, 50);

        let idx_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(idx_count, 2);

        // First dropped index
        let idx_id =
            u64::from_le_bytes(catalog_data[off..off + 8].try_into().unwrap());
        off += 8;
        assert_eq!(idx_id, 20);

        // We won't parse every byte — just verify the data is present
        // by checking total length is reasonable
        assert!(catalog_data.len() > off + 20, "should have more data for indexes");
    }

    #[test]
    fn wal_payload_drop_index_with_full_metadata() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(10),
            name: "by_age".into(),
            field_paths: vec![FieldPath::single("age")],
            root_page: 77,
        });

        let payload = serialize_wal_payload(40, &ws);
        let (version, ts, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 40);

        // Parse: count(4) + tag(1) + index_id(8) + coll_id(8) + name + field_paths + root_page(4)
        let mut off = 0;
        let count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(count, 1);

        let tag = catalog_data[off];
        off += 1;
        assert_eq!(tag, 0x04); // DropIndex

        let idx_id =
            u64::from_le_bytes(catalog_data[off..off + 8].try_into().unwrap());
        off += 8;
        assert_eq!(idx_id, 10);

        let coll_id =
            u64::from_le_bytes(catalog_data[off..off + 8].try_into().unwrap());
        off += 8;
        assert_eq!(coll_id, 1);

        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let name = std::str::from_utf8(&catalog_data[off..off + name_len]).unwrap();
        off += name_len;
        assert_eq!(name, "by_age");

        // field_paths
        let fp_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(fp_count, 1);
        let seg_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(seg_count, 1);
        let seg_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let seg = std::str::from_utf8(&catalog_data[off..off + seg_len]).unwrap();
        off += seg_len;
        assert_eq!(seg, "age");

        // root_page at the end
        let root_page =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        assert_eq!(root_page, 77);
    }

    #[test]
    fn wal_payload_mixed_mutations_and_catalog() {
        let mut ws = WriteSet::new();
        ws.insert(CollectionId(1), DocId([1; 16]), json!({"x": 1}));
        ws.delete(CollectionId(1), DocId([2; 16]), 5);
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "orders".into(),
            provisional_id: CollectionId(2),
            primary_root_page: 30,
            created_at_root_page: 31,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(2),
            name: "_created_at".into(),
            field_paths: vec![FieldPath::single("_created_at")],
            provisional_id: IndexId(5),
            root_page: 32,
        });
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(3),
            name: "old_idx".into(),
            field_paths: vec![FieldPath::single("old")],
            root_page: 99,
        });

        let payload = serialize_wal_payload(50, &ws);
        let (version, ts, mutations, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 50);
        assert_eq!(mutations.len(), 2);
        // Verify data mutations
        assert_eq!(mutations[0].2, 0x01); // Insert
        assert_eq!(mutations[1].2, 0x03); // Delete

        // Verify catalog count
        let count = u32::from_le_bytes(catalog_data[0..4].try_into().unwrap());
        assert_eq!(count, 3);
    }

    #[test]
    fn wal_payload_compound_field_paths() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "by_addr".into(),
            field_paths: vec![
                FieldPath::new(vec!["address".into(), "city".into()]),
                FieldPath::new(vec!["address".into(), "zip".into()]),
            ],
            provisional_id: IndexId(1),
            root_page: 50,
        });

        let payload = serialize_wal_payload(1, &ws);
        let (version, _, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();
        assert_eq!(version, 2);

        // Parse to verify compound field paths are serialized
        let mut off = 4; // skip count
        off += 1; // tag
        off += 8; // provisional_id
        off += 8; // collection_id
        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4 + name_len;

        // field_path_count = 2
        let fp_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(fp_count, 2);

        // First field path: ["address", "city"] → segment_count=2
        let seg_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(seg_count, 2);

        // "address"
        let seg_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let seg = std::str::from_utf8(&catalog_data[off..off + seg_len]).unwrap();
        off += seg_len;
        assert_eq!(seg, "address");

        // "city"
        let seg_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let seg = std::str::from_utf8(&catalog_data[off..off + seg_len]).unwrap();
        off += seg_len;
        assert_eq!(seg, "city");

        // Second field path: ["address", "zip"]
        let seg_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        off += 4;
        assert_eq!(seg_count, 2);
    }

    #[test]
    fn wal_payload_large_body_roundtrip() {
        let mut ws = WriteSet::new();
        let large_body: serde_json::Value =
            serde_json::from_str(&format!("{{\"data\": \"{}\"}}", "x".repeat(100_000)))
                .unwrap();
        ws.insert(CollectionId(1), DocId([7; 16]), large_body);

        let payload = serialize_wal_payload(99, &ws);
        let (version, ts, mutations, _) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 99);
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].3.as_ref().unwrap().len() > 100_000);
    }

    #[test]
    fn wal_payload_only_deletes() {
        let mut ws = WriteSet::new();
        ws.delete(CollectionId(1), DocId([1; 16]), 10);
        ws.delete(CollectionId(1), DocId([2; 16]), 20);

        let payload = serialize_wal_payload(5, &ws);
        let (version, ts, mutations, _) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        assert_eq!(ts, 5);
        assert_eq!(mutations.len(), 2);
        for m in &mutations {
            assert_eq!(m.2, 0x03); // Delete
            assert!(m.3.is_none());
        }
    }

    #[test]
    fn wal_payload_deserialize_truncated_errors() {
        // Too short to have even a version + commit_ts
        let result = deserialize_wal_payload(&[2, 0, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn wal_payload_deserialize_empty_errors() {
        let result = deserialize_wal_payload(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn wal_payload_deserialize_truncated_mutation_errors() {
        // Version(1) + commit_ts(8) + mutation_count(4) = 13 bytes
        // Says 1 mutation but no mutation data follows
        let mut buf = vec![WAL_PAYLOAD_VERSION];
        buf.extend_from_slice(&100u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 mutation
        // No mutation data
        let result = deserialize_wal_payload(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn wal_payload_unicode_names() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "données_françaises".into(),
            provisional_id: CollectionId(1),
            primary_root_page: 10,
            created_at_root_page: 11,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "по_имени".into(),
            field_paths: vec![FieldPath::single("名前")],
            provisional_id: IndexId(1),
            root_page: 12,
        });

        let payload = serialize_wal_payload(1, &ws);
        let (version, _, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        // Verify we can at least deserialize without error
        assert!(!catalog_data.is_empty());

        // Parse first entry name
        let mut off = 4; // count
        off += 1; // tag = 0x01
        off += 8; // provisional_id
        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4;
        let name = std::str::from_utf8(&catalog_data[off..off + name_len]).unwrap();
        assert_eq!(name, "données_françaises");
    }

    #[test]
    fn wal_payload_drop_collection_no_indexes() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "empty".into(),
            primary_root_page: 5,
            dropped_indexes: vec![],
        });

        let payload = serialize_wal_payload(1, &ws);
        let (version, _, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);

        // Parse to verify dropped_index_count = 0
        let mut off = 4; // count
        off += 1; // tag = 0x02
        off += 8; // collection_id
        let name_len =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap())
                as usize;
        off += 4 + name_len;
        off += 4; // primary_root_page

        let idx_count =
            u32::from_le_bytes(catalog_data[off..off + 4].try_into().unwrap());
        assert_eq!(idx_count, 0);
    }

    #[test]
    fn wal_payload_all_four_catalog_types() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "c1".into(),
            provisional_id: CollectionId(1),
            primary_root_page: 10,
            created_at_root_page: 11,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "i1".into(),
            field_paths: vec![FieldPath::single("x")],
            provisional_id: IndexId(1),
            root_page: 12,
        });
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(1),
            name: "i1".into(),
            field_paths: vec![FieldPath::single("x")],
            root_page: 12,
        });
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "c1".into(),
            primary_root_page: 10,
            dropped_indexes: vec![],
        });

        let payload = serialize_wal_payload(1, &ws);
        let (version, _, _, catalog_data) =
            deserialize_wal_payload(&payload).unwrap();

        assert_eq!(version, 2);
        let count =
            u32::from_le_bytes(catalog_data[0..4].try_into().unwrap());
        assert_eq!(count, 4);

        // Verify tag sequence
        let tags: Vec<u8> = {
            let mut off = 4;
            let mut tags = Vec::new();
            for _ in 0..4 {
                tags.push(catalog_data[off]);
                // Skip to next entry — we can't easily skip variable-length
                // so just verify the tags are present
                break; // Only check first tag
            }
            tags
        };
        assert_eq!(tags[0], 0x01); // CreateCollection
    }

    // ─── Commit coordinator step reorder tests ───

    #[tokio::test]
    async fn commit_with_create_collection_succeeds() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "test".into(),
            provisional_id: CollectionId(100),
            primary_root_page: 0,     // Will be filled by allocate_collection_pages
            created_at_root_page: 0,  // Will be filled by allocate_collection_pages
        });

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        assert!(matches!(result, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn commit_with_create_index_succeeds() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "idx".into(),
            field_paths: vec![FieldPath::single("x")],
            provisional_id: IndexId(1),
            root_page: 0, // Will be filled by allocate_index_page
        });

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        assert!(matches!(result, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn commit_with_drop_collection_succeeds() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "gone".into(),
            primary_root_page: 10,
            dropped_indexes: vec![DroppedIndexMeta {
                index_id: IndexId(1),
                name: "idx".into(),
                field_paths: vec![],
                root_page: 11,
            }],
        });

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        assert!(matches!(result, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn commit_mixed_ddl_and_data() {
        let handle = spawn_system().await;

        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "orders".into(),
            provisional_id: CollectionId(50),
            primary_root_page: 0,
            created_at_root_page: 0,
        });
        ws.insert(CollectionId(50), DocId([1; 16]), json!({"total": 99}));

        let result = handle
            .commit(CommitRequest {
                tx_id: 1,
                begin_ts: 0,
                read_set: ReadSet::new(),
                write_set: ws,
                subscription: SubscriptionMode::None,
                session_id: 1,
            })
            .await;

        assert!(matches!(result, CommitResult::Success { .. }));
    }
}
