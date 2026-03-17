//! B5: Unified transaction API.
//!
//! `Transaction<'db>` borrows the `Database` for its lifetime. Provides
//! read, write, DDL, and lifecycle methods.
//!
//! # Send safety
//!
//! All `parking_lot::RwLock` guards are dropped before any `.await` point,
//! so the resulting futures are `Send`. This follows the same pattern used
//! by `CommitCoordinator` in L5: clone `Arc`-wrapped handles out of the
//! map, drop the guard, then await on the cloned handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use exdb_core::encoding::{apply_patch, decode_document};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts, TxId};
use exdb_core::ulid::generate_ulid;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use exdb_query::{
    resolve_access, AccessMethod, IndexInfo, RangeExpr,
};
use exdb_storage::btree::ScanDirection;
use exdb_tx::{
    CatalogMutation, CommitHandle, CommitRequest, CommitResult, DroppedIndexMeta,
    MutationOp, ReadInterval, ReadSet, SubscriptionMode, WriteSet,
};
use parking_lot::RwLock;
use serde_json::Value;
use std::ops::Bound;
use tokio_stream::StreamExt;

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};
use crate::catalog_tracker::CatalogTracker;
use crate::config::TransactionConfig;
use crate::error::{DatabaseError, Result};
use crate::subscription::SubscriptionHandle;

/// Transaction options.
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    pub readonly: bool,
    pub subscription: SubscriptionMode,
    pub session_id: u64,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        TransactionOptions {
            readonly: false,
            subscription: SubscriptionMode::None,
            session_id: 0,
        }
    }
}

impl TransactionOptions {
    /// Create read-only transaction options.
    pub fn readonly() -> Self {
        TransactionOptions {
            readonly: true,
            ..Default::default()
        }
    }
}

/// Result of committing a transaction.
pub enum TransactionResult {
    /// Commit succeeded.
    Success {
        commit_ts: Ts,
        subscription_handle: Option<SubscriptionHandle>,
    },
    /// OCC conflict detected.
    Conflict {
        error: exdb_tx::ConflictError,
        retry: Option<exdb_tx::ConflictRetry>,
    },
    /// Replication quorum lost.
    QuorumLost,
}

/// A transaction that borrows the Database for its lifetime.
pub struct Transaction<'db> {
    // Database references
    commit_handle: &'db CommitHandle,
    catalog: &'db Arc<RwLock<CatalogCache>>,
    primary_indexes: &'db Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: &'db Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    config: &'db TransactionConfig,

    // Transaction state
    tx_id: TxId,
    opts: TransactionOptions,
    begin_ts: Ts,
    wall_clock_ms: u64,
    read_set: ReadSet,
    write_set: WriteSet,
    committed: bool,
    created_at: Instant,
    last_activity: Instant,
}

impl<'db> Transaction<'db> {
    /// Create a new transaction (called by Database::begin).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        commit_handle: &'db CommitHandle,
        catalog: &'db Arc<RwLock<CatalogCache>>,
        primary_indexes: &'db Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: &'db Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        config: &'db TransactionConfig,
        tx_id: TxId,
        begin_ts: Ts,
        opts: TransactionOptions,
    ) -> Self {
        let now = Instant::now();
        let wall_clock_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Transaction {
            commit_handle,
            catalog,
            primary_indexes,
            secondary_indexes,
            config,
            tx_id,
            opts,
            begin_ts,
            wall_clock_ms,
            read_set: ReadSet::new(),
            write_set: WriteSet::new(),
            committed: false,
            created_at: now,
            last_activity: now,
        }
    }

    // ─── Timeout Check ───

    fn check_timeout(&self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.created_at) > self.config.max_lifetime {
            return Err(DatabaseError::TransactionTimeout);
        }
        if now.duration_since(self.last_activity) > self.config.idle_timeout {
            return Err(DatabaseError::TransactionTimeout);
        }
        Ok(())
    }

    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    // ─── Lock helpers ───
    // Clone Arc handles out of the RwLock-guarded maps so the parking_lot
    // guard is dropped before any .await. This keeps futures Send.

    /// Get a cloned Arc<PrimaryIndex> for a collection.
    /// The RwLock guard is dropped before returning.
    fn get_primary(&self, coll_id: CollectionId, collection: &str) -> Result<Arc<PrimaryIndex>> {
        self.primary_indexes
            .read()
            .get(&coll_id)
            .cloned()
            .ok_or_else(|| DatabaseError::CollectionNotFound(collection.to_string()))
    }

    /// Clone the primary index map. Guard is dropped before returning.
    fn clone_primaries(&self) -> HashMap<CollectionId, Arc<PrimaryIndex>> {
        self.primary_indexes.read().clone()
    }

    /// Clone the secondary index map. Guard is dropped before returning.
    fn clone_secondaries(&self) -> HashMap<IndexId, Arc<SecondaryIndex>> {
        self.secondary_indexes.read().clone()
    }

    // ─── Collection Resolution ───

    /// Resolve a collection name to its ID and metadata.
    fn resolve_collection(&mut self, name: &str) -> Result<(CollectionId, CollectionMeta)> {
        let qid = self.read_set.next_query_id();
        CatalogTracker::record_collection_name_lookup(&mut self.read_set, qid, name);

        // Check if created in this tx
        if let Some(cid) = self.write_set.resolve_pending_collection(name) {
            let meta = CollectionMeta {
                collection_id: cid,
                name: name.to_string(),
                primary_root_page: 0,
                doc_count: 0,
            };
            return Ok((cid, meta));
        }

        // Look up in catalog, check if dropped
        let cache = self.catalog.read();
        if let Some(meta) = cache.get_collection_by_name(name) {
            if self.write_set.is_collection_dropped(meta.collection_id) {
                return Err(DatabaseError::CollectionDropped);
            }
            Ok((meta.collection_id, meta.clone()))
        } else {
            Err(DatabaseError::CollectionNotFound(name.to_string()))
        }
        // guard dropped here — before any .await in callers
    }

    // ─── Read Operations ───

    /// Get a single document by ID.
    pub async fn get(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<Option<Value>> {
        self.check_timeout()?;
        self.touch();

        let (coll_id, _meta) = self.resolve_collection(collection)?;

        // Check write set first (read-your-writes)
        if let Some(entry) = self.write_set.get(coll_id, doc_id) {
            return match entry.op {
                MutationOp::Delete => Ok(None),
                MutationOp::Insert | MutationOp::Replace => {
                    Ok(entry.body.clone())
                }
            };
        }

        // Clone Arc<PrimaryIndex> out of the map — drops guard before .await
        let primary = self.get_primary(coll_id, collection)?;

        let body = primary.get_at_ts(doc_id, self.begin_ts).await.map_err(DatabaseError::Storage)?;

        match body {
            Some(bytes) => {
                let doc = decode_document(&bytes)
                    .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Query documents using an index.
    pub async fn query(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        self.check_timeout()?;
        self.touch();

        let (coll_id, _meta) = self.resolve_collection(collection)?;
        let direction = direction.unwrap_or(ScanDirection::Forward);

        // Resolve index — guard scoped to this block, dropped before .await
        let qid = self.read_set.next_query_id();
        CatalogTracker::record_index_name_lookup(
            &mut self.read_set,
            qid,
            coll_id,
            index,
        );

        let index_info = {
            let cache = self.catalog.read();
            let idx = cache
                .get_index_by_name(coll_id, index)
                .ok_or_else(|| DatabaseError::IndexNotFound {
                    collection: collection.to_string(),
                    index: index.to_string(),
                })?;
            if idx.state != IndexState::Ready {
                return Err(DatabaseError::IndexNotReady(index.to_string()));
            }
            IndexInfo {
                index_id: idx.index_id,
                field_paths: idx.field_paths.clone(),
                ready: true,
            }
        }; // guard dropped here

        // Resolve access method
        let access = resolve_access(coll_id, &index_info, range, filter, direction, limit)
            .map_err(DatabaseError::Access)?;

        // Clone Arc maps — guards dropped immediately
        let primaries = self.clone_primaries();
        let sec_arcs = self.clone_secondaries();

        let primary = primaries
            .get(&coll_id)
            .ok_or_else(|| DatabaseError::CollectionNotFound(collection.to_string()))?;

        // No guards held past this point — all .await below are Send-safe
        let results = match &access {
            AccessMethod::PrimaryGet { doc_id, .. } => {
                let body = primary
                    .get_at_ts(doc_id, self.begin_ts)
                    .await
                    .map_err(DatabaseError::Storage)?;
                match body {
                    Some(bytes) => {
                        let doc = decode_document(&bytes).map_err(|e| {
                            DatabaseError::Commit(format!("decode error: {e}"))
                        })?;
                        vec![doc]
                    }
                    None => vec![],
                }
            }
            AccessMethod::IndexScan {
                index_id: scan_idx_id,
                lower,
                upper,
                post_filter,
                limit: scan_limit,
                direction: scan_dir,
                ..
            } => {
                self.execute_secondary_scan(
                    coll_id,
                    *scan_idx_id,
                    lower,
                    upper,
                    post_filter.as_ref(),
                    *scan_limit,
                    *scan_dir,
                    qid,
                    &primaries,
                    &sec_arcs,
                )
                .await?
            }
            AccessMethod::TableScan {
                index_id: scan_idx_id,
                post_filter,
                limit: scan_limit,
                direction: scan_dir,
                ..
            } => {
                self.execute_secondary_scan(
                    coll_id,
                    *scan_idx_id,
                    &Bound::Unbounded,
                    &Bound::Unbounded,
                    post_filter.as_ref(),
                    *scan_limit,
                    *scan_dir,
                    qid,
                    &primaries,
                    &sec_arcs,
                )
                .await?
            }
        };

        Ok(results)
    }

    /// Execute a secondary index scan.
    ///
    /// Receives pre-cloned Arc maps — no lock guards held during execution.
    #[allow(clippy::too_many_arguments)]
    async fn execute_secondary_scan(
        &mut self,
        coll_id: CollectionId,
        index_id: IndexId,
        lower: &Bound<Vec<u8>>,
        upper: &Bound<Vec<u8>>,
        post_filter: Option<&Filter>,
        limit: Option<usize>,
        direction: ScanDirection,
        qid: exdb_tx::QueryId,
        primaries: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        sec_arcs: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<Vec<Value>> {
        let secondary = sec_arcs
            .get(&index_id)
            .ok_or_else(|| DatabaseError::IndexNotFound {
                collection: format!("{:?}", coll_id),
                index: format!("{:?}", index_id),
            })?;
        let primary = primaries
            .get(&coll_id)
            .ok_or_else(|| DatabaseError::CollectionNotFound(format!("{:?}", coll_id)))?;

        // Record read interval
        let lower_bytes = match lower {
            Bound::Included(v) => v.clone(),
            Bound::Excluded(v) => v.clone(),
            Bound::Unbounded => vec![],
        };
        self.read_set.add_interval(
            coll_id,
            index_id,
            ReadInterval {
                query_id: qid,
                lower: Bound::Included(lower_bytes),
                upper: upper.clone(),
                limit_boundary: None,
            },
        );

        // Scan secondary index
        let lower_ref = match lower {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper_ref = match upper {
            Bound::Included(v) => Bound::Included(v.as_slice()),
            Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let stream = secondary.scan_at_ts(lower_ref, upper_ref, self.begin_ts, direction);
        tokio::pin!(stream);

        let mut results = Vec::new();
        while let Some(entry) = stream.next().await {
            let (doc_id, _version_ts) = entry.map_err(DatabaseError::Storage)?;

            // Fetch document body from primary
            let body = primary
                .get_at_ts(&doc_id, self.begin_ts)
                .await
                .map_err(DatabaseError::Storage)?;

            if let Some(bytes) = body {
                let doc = decode_document(&bytes)
                    .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;

                // Apply post-filter
                if let Some(filter) = post_filter {
                    if !exdb_query::filter_matches(&doc, filter) {
                        continue;
                    }
                }

                results.push(doc);

                if let Some(lim) = limit {
                    if results.len() >= lim {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// List all collections.
    pub fn list_collections(&mut self) -> Result<Vec<CollectionMeta>> {
        self.check_timeout()?;
        self.touch();

        let qid = self.read_set.next_query_id();
        CatalogTracker::record_list_collections(&mut self.read_set, qid);

        let cache = self.catalog.read();
        Ok(cache.list_collections())
    }

    /// List all indexes for a collection.
    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<IndexMeta>> {
        self.check_timeout()?;
        self.touch();

        let (coll_id, _) = self.resolve_collection(collection)?;
        let qid = self.read_set.next_query_id();
        CatalogTracker::record_list_indexes(&mut self.read_set, qid, coll_id);

        let cache = self.catalog.read();
        Ok(cache.list_indexes(coll_id))
    }

    // ─── Write Operations ───

    /// Insert a new document. Returns the generated DocId.
    pub async fn insert(
        &mut self,
        collection: &str,
        mut body: Value,
    ) -> Result<DocId> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Generate doc ID
        let doc_id = generate_ulid();

        // Set _created_at
        if let Value::Object(ref mut map) = body {
            map.insert(
                "_created_at".to_string(),
                Value::Number(serde_json::Number::from(self.wall_clock_ms)),
            );
            // Remove _meta if present
            map.remove("_meta");
        }

        // Check size
        let encoded = serde_json::to_vec(&body)
            .map_err(|e| DatabaseError::Commit(format!("JSON error: {e}")))?;
        if encoded.len() > 16 * 1024 * 1024 {
            return Err(DatabaseError::DocTooLarge {
                size: encoded.len(),
                max: 16 * 1024 * 1024,
            });
        }

        self.write_set.insert(coll_id, doc_id, body);

        Ok(doc_id)
    }

    /// Replace an existing document.
    pub async fn replace(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        mut body: Value,
    ) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Verify document exists
        let existing = self.get(collection, doc_id).await?;
        if existing.is_none() {
            return Err(DatabaseError::DocNotFound);
        }

        // Remove _meta
        if let Value::Object(ref mut map) = body {
            map.remove("_meta");
        }

        // Clone Arc out, drop guard before .await
        let primary = self.get_primary(coll_id, collection)?;
        let prev_ts = primary
            .get_version_ts(doc_id, self.begin_ts)
            .await
            .map_err(DatabaseError::Storage)?
            .unwrap_or(0);

        self.write_set.replace(coll_id, *doc_id, body, prev_ts);

        Ok(())
    }

    /// Patch an existing document (RFC 7396 merge-patch).
    pub async fn patch(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        patch: Value,
    ) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        // Get current document
        let existing = self.get(collection, doc_id).await?;
        let mut doc = existing.ok_or(DatabaseError::DocNotFound)?;

        // Apply merge-patch
        apply_patch(&mut doc, &patch);

        // Replace with patched version
        self.replace(collection, doc_id, doc).await
    }

    /// Delete a document.
    pub async fn delete(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Verify document exists
        let existing = self.get(collection, doc_id).await?;
        if existing.is_none() {
            return Err(DatabaseError::DocNotFound);
        }

        // Clone Arc out, drop guard before .await
        let primary = self.get_primary(coll_id, collection)?;
        let prev_ts = primary
            .get_version_ts(doc_id, self.begin_ts)
            .await
            .map_err(DatabaseError::Storage)?
            .unwrap_or(0);

        self.write_set.delete(coll_id, *doc_id, prev_ts);

        Ok(())
    }

    // ─── DDL Operations ───

    /// Create a new collection.
    pub async fn create_collection(&mut self, name: &str) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        // Validate name
        if name.is_empty() || name.starts_with('_') {
            return Err(DatabaseError::InvalidName(name.to_string()));
        }

        // Record a read interval on the catalog name index so that OCC
        // detects concurrent creates of the same collection name.
        let qid = self.read_set.next_query_id();
        CatalogTracker::record_collection_name_lookup(&mut self.read_set, qid, name);

        // Check if already exists — guard scoped to block
        {
            let cache = self.catalog.read();
            if cache.has_collection(name) {
                return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
            }
        }

        // Check write set for pending creation
        if self.write_set.resolve_pending_collection(name).is_some() {
            return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
        }

        let provisional_id = self.catalog.read().allocate_collection_id();

        self.write_set.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: name.to_string(),
            provisional_id,
            primary_root_page: 0,
            created_at_root_page: 0,
        });

        Ok(())
    }

    /// Drop a collection.
    pub async fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, meta) = self.resolve_collection(name)?;

        // Gather indexes — guard scoped to block
        let indexes: Vec<IndexMeta> = {
            let cache = self.catalog.read();
            cache.list_indexes(coll_id)
        };

        let dropped_indexes: Vec<DroppedIndexMeta> = indexes
            .iter()
            .map(|idx| DroppedIndexMeta {
                index_id: idx.index_id,
                name: idx.name.clone(),
                field_paths: idx.field_paths.clone(),
                root_page: idx.root_page,
            })
            .collect();

        self.write_set.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: coll_id,
            name: name.to_string(),
            primary_root_page: meta.primary_root_page,
            dropped_indexes,
        });

        Ok(())
    }

    /// Create a secondary index on a collection.
    pub async fn create_index(
        &mut self,
        collection: &str,
        name: &str,
        fields: Vec<FieldPath>,
    ) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        if name.starts_with('_') {
            return Err(DatabaseError::SystemIndex(name.to_string()));
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Check if already exists — guard scoped to block
        {
            let cache = self.catalog.read();
            if cache.get_index_by_name(coll_id, name).is_some() {
                return Err(DatabaseError::IndexAlreadyExists {
                    collection: collection.to_string(),
                    index: name.to_string(),
                });
            }
        }

        let provisional_id = self.catalog.read().allocate_index_id();

        self.write_set.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: coll_id,
            name: name.to_string(),
            field_paths: fields,
            provisional_id,
            root_page: 0,
        });

        Ok(())
    }

    /// Drop a secondary index.
    pub async fn drop_index(
        &mut self,
        collection: &str,
        name: &str,
    ) -> Result<()> {
        self.check_timeout()?;
        self.touch();

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        if name == "_created_at" || name == "_id" {
            return Err(DatabaseError::SystemIndex(name.to_string()));
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Clone out of guard — guard scoped to block
        let idx = {
            let cache = self.catalog.read();
            cache
                .get_index_by_name(coll_id, name)
                .cloned()
                .ok_or_else(|| DatabaseError::IndexNotFound {
                    collection: collection.to_string(),
                    index: name.to_string(),
                })?
        };

        self.write_set.add_catalog_mutation(CatalogMutation::DropIndex {
            index_id: idx.index_id,
            collection_id: coll_id,
            name: name.to_string(),
            field_paths: idx.field_paths.clone(),
            root_page: idx.root_page,
        });

        Ok(())
    }

    // ─── Lifecycle ───

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<TransactionResult> {
        self.committed = true;

        let request = CommitRequest {
            tx_id: self.tx_id,
            begin_ts: self.begin_ts,
            read_set: std::mem::replace(&mut self.read_set, ReadSet::new()),
            write_set: std::mem::replace(&mut self.write_set, WriteSet::new()),
            subscription: self.opts.subscription,
            session_id: self.opts.session_id,
        };

        let result = self.commit_handle.commit(request).await;

        match result {
            CommitResult::Success {
                commit_ts,
                subscription_id,
                event_rx,
            } => {
                let sub_handle = match (subscription_id, event_rx) {
                    (Some(id), Some(rx)) => Some(SubscriptionHandle::new(
                        id,
                        Arc::clone(self.commit_handle.subscriptions()),
                        rx,
                    )),
                    _ => None,
                };
                Ok(TransactionResult::Success {
                    commit_ts,
                    subscription_handle: sub_handle,
                })
            }
            CommitResult::Conflict { error, retry } => {
                Ok(TransactionResult::Conflict { error, retry })
            }
            CommitResult::QuorumLost => Ok(TransactionResult::QuorumLost),
        }
    }

    /// Rollback the transaction (no-op: just drops state).
    pub fn rollback(mut self) {
        self.committed = true;
    }

    /// Reset the transaction for reuse with a new snapshot.
    pub fn reset(&mut self) {
        self.read_set = ReadSet::new();
        self.write_set = WriteSet::new();
        self.begin_ts = self.commit_handle.visible_ts();
        self.last_activity = Instant::now();
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            tracing::debug!("transaction {} dropped without commit/rollback", self.tx_id);
        }
    }
}
