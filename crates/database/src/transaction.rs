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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bson::Document as BsonDocument;
use exdb_core::encoding::{
    apply_patch, bson_document_to_json, decode_document, extract_scalar, json_document_to_bson,
    try_encode_document,
};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::types::{CollectionId, DocId, IndexId, Scalar, Ts, TxId};
use exdb_core::ulid::{encode_ulid, generate_ulid};
use exdb_docstore::{
    PrimaryIndex, SecondaryIndex, compute_index_entries, encode_key_prefix, make_primary_key,
    make_secondary_key_from_prefix,
};
use exdb_query::{
    AccessMethod, IndexInfo, MergeView, RangeExpr, ScanRow, merge_with_writes, resolve_access,
};
use exdb_storage::btree::ScanDirection;
use exdb_tx::{
    CatalogMutation, CommitHandle, CommitRequest, CommitResult, DroppedIndexMeta, LimitBoundary,
    MutationOp, PRIMARY_INDEX_SENTINEL, QueryId, ReadInterval, ReadSet, SubscriptionMode, WriteSet,
    serialize_promotion_payload,
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

type PendingDocRows = Vec<(DocId, Value)>;
type PendingDeletes = Vec<DocId>;

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

impl std::fmt::Debug for TransactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionResult::Success {
                commit_ts,
                subscription_handle,
            } => f
                .debug_struct("Success")
                .field("commit_ts", commit_ts)
                .field("subscription_handle", &subscription_handle.is_some())
                .finish(),
            TransactionResult::Conflict { error, retry } => f
                .debug_struct("Conflict")
                .field("error", error)
                .field("retry", &retry.is_some())
                .finish(),
            TransactionResult::QuorumLost => f.write_str("QuorumLost"),
        }
    }
}

/// Concrete transaction state forwarded from a replica to a primary.
pub struct PromotionPayload {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub payload: Vec<u8>,
}

/// A transaction owns cloned database handles so it can be used by embedded
/// callers directly or held across network session messages.
pub struct Transaction {
    // Database references
    commit_handle: CommitHandle,
    catalog: Arc<RwLock<CatalogCache>>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    config: TransactionConfig,
    max_doc_size: usize,
    active_tx_count: Arc<AtomicU64>,

    // Transaction state
    tx_id: TxId,
    opts: TransactionOptions,
    begin_ts: Ts,
    wall_clock_ms: u64,
    read_set: ReadSet,
    write_set: WriteSet,
    operations_used: usize,
    abort_reason: Option<TransactionAbortReason>,
    committed: bool,
    created_at: Instant,
    last_activity: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionAbortReason {
    ReadLimitExceeded,
    Timeout,
}

impl TransactionAbortReason {
    fn error(self) -> DatabaseError {
        match self {
            TransactionAbortReason::ReadLimitExceeded => DatabaseError::ReadLimitExceeded(
                "transaction was aborted after exceeding a read/resource limit".to_string(),
            ),
            TransactionAbortReason::Timeout => DatabaseError::TransactionTimeout,
        }
    }
}

impl Transaction {
    /// Create a new transaction (called by Database::begin).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        commit_handle: CommitHandle,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        config: TransactionConfig,
        max_doc_size: usize,
        active_tx_count: Arc<AtomicU64>,
        tx_id: TxId,
        begin_ts: Ts,
        opts: TransactionOptions,
    ) -> Self {
        Self::new_with_read_set(
            commit_handle,
            catalog,
            primary_indexes,
            secondary_indexes,
            config,
            max_doc_size,
            active_tx_count,
            tx_id,
            begin_ts,
            opts,
            ReadSet::new(),
        )
    }

    /// Create a transaction with an existing carried read set.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_read_set(
        commit_handle: CommitHandle,
        catalog: Arc<RwLock<CatalogCache>>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        config: TransactionConfig,
        max_doc_size: usize,
        active_tx_count: Arc<AtomicU64>,
        tx_id: TxId,
        begin_ts: Ts,
        opts: TransactionOptions,
        read_set: ReadSet,
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
            max_doc_size,
            active_tx_count,
            tx_id,
            opts,
            begin_ts,
            wall_clock_ms,
            read_set,
            write_set: WriteSet::new(),
            operations_used: 0,
            abort_reason: None,
            committed: false,
            created_at: now,
            last_activity: now,
        }
    }

    // ─── Timeout Check ───

    fn check_not_aborted(&self) -> Result<()> {
        if let Some(reason) = self.abort_reason {
            return Err(reason.error());
        }
        Ok(())
    }

    fn check_timeout(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.created_at) > self.config.max_lifetime
            || now.duration_since(self.last_activity) > self.config.idle_timeout
        {
            self.abort_reason = Some(TransactionAbortReason::Timeout);
            return Err(DatabaseError::TransactionTimeout);
        }
        Ok(())
    }

    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    fn charge_operations(&mut self, amount: usize, context: &str) -> Result<()> {
        self.operations_used = self.operations_used.saturating_add(amount);
        if self.operations_used > self.config.max_operations {
            return self.abort_read_limit(format!(
                "transaction operation limit exceeded during {context}: {} > {}",
                self.operations_used, self.config.max_operations
            ));
        }
        Ok(())
    }

    fn enter_operation(&mut self, context: &str) -> Result<()> {
        self.check_not_aborted()?;
        self.check_timeout()?;
        self.touch();
        self.charge_operations(1, context)
    }

    fn validate_doc_size(&self, body: &Value) -> Result<()> {
        let encoded = try_encode_document(body)
            .map_err(|e| DatabaseError::Commit(format!("BSON encode error: {e}")))?;
        if encoded.len() > self.max_doc_size {
            return Err(DatabaseError::DocTooLarge {
                size: encoded.len(),
                max: self.max_doc_size,
            });
        }
        Ok(())
    }

    fn check_read_limits(&mut self) -> Result<()> {
        let intervals = self.read_set.interval_count();
        if intervals > self.config.max_intervals {
            return self.abort_read_limit(format!(
                "read interval limit exceeded: {intervals} > {}",
                self.config.max_intervals
            ));
        }
        Ok(())
    }

    fn check_scan_limits(&mut self, docs_scanned: usize, bytes_scanned: usize) -> Result<()> {
        if docs_scanned > self.config.max_scanned_docs {
            return self.abort_read_limit(format!(
                "scanned document limit exceeded: {docs_scanned} > {}",
                self.config.max_scanned_docs
            ));
        }
        if bytes_scanned > self.config.max_scanned_bytes {
            return self.abort_read_limit(format!(
                "scanned byte limit exceeded: {bytes_scanned} > {}",
                self.config.max_scanned_bytes
            ));
        }
        Ok(())
    }

    fn abort_read_limit<T>(&mut self, message: String) -> Result<T> {
        self.abort_reason = Some(TransactionAbortReason::ReadLimitExceeded);
        Err(DatabaseError::ReadLimitExceeded(message))
    }

    /// Server-assigned transaction identifier.
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Snapshot timestamp pinned by this transaction.
    pub fn begin_ts(&self) -> Ts {
        self.begin_ts
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
    fn resolve_collection_with_query_id(
        &mut self,
        name: &str,
        qid: QueryId,
    ) -> Result<(CollectionId, CollectionMeta)> {
        CatalogTracker::record_collection_name_lookup(&mut self.read_set, qid, name);
        self.check_read_limits()?;
        self.charge_operations(
            self.write_set.catalog_mutations.len(),
            "collection pending catalog resolution",
        )?;

        // Check if created in this tx
        if let Some(cid) = self.write_set.resolve_pending_collection(name) {
            if self.write_set.is_collection_dropped(cid) {
                return Err(DatabaseError::CollectionDropped);
            }
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

    /// Resolve a collection name using a fresh internal query ID.
    fn resolve_collection(&mut self, name: &str) -> Result<(CollectionId, CollectionMeta)> {
        let qid = self.read_set.next_query_id();
        self.resolve_collection_with_query_id(name, qid)
    }

    // ─── Read Operations ───

    /// Get a single document by ID.
    pub async fn get(&mut self, collection: &str, doc_id: &DocId) -> Result<Option<Value>> {
        self.get_with_query_id(collection, doc_id)
            .await
            .map(|(_, doc)| doc)
    }

    /// Get a single document by ID as a native BSON document.
    pub async fn get_bson(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<Option<BsonDocument>> {
        self.get_bson_with_query_id(collection, doc_id)
            .await
            .map(|(_, doc)| doc)
    }

    /// Get a single BSON document by ID and return the read-set query ID.
    pub async fn get_bson_with_query_id(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<(QueryId, Option<BsonDocument>)> {
        let (qid, doc) = self.get_with_query_id(collection, doc_id).await?;
        Ok((
            qid,
            doc.map(|doc| {
                json_document_to_bson(&doc)
                    .map_err(|e| DatabaseError::Commit(format!("BSON encode error: {e}")))
            })
            .transpose()?,
        ))
    }

    /// Get a single document by ID and return the read-set query ID.
    pub async fn get_with_query_id(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<(QueryId, Option<Value>)> {
        self.enter_operation("get")?;

        let qid = self.read_set.next_query_id();
        let (coll_id, _meta) = self.resolve_collection_with_query_id(collection, qid)?;

        // Check write set first (read-your-writes)
        if let Some(entry) = self.write_set.get(coll_id, doc_id) {
            return match entry.op {
                MutationOp::Delete => Ok((qid, None)),
                MutationOp::Insert | MutationOp::Replace => Ok((qid, entry.body.clone())),
            };
        }

        self.record_primary_get(coll_id, doc_id, qid)?;

        // Clone Arc<PrimaryIndex> out of the map — drops guard before .await
        let primary = self.get_primary(coll_id, collection)?;

        let body = primary
            .get_at_ts(doc_id, self.begin_ts)
            .await
            .map_err(DatabaseError::Storage)?;

        match body {
            Some(bytes) => {
                self.check_scan_limits(1, bytes.len())?;
                let doc = decode_document(&bytes)
                    .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;
                Ok((qid, Some(doc)))
            }
            None => Ok((qid, None)),
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
        self.query_with_query_id(collection, index, range, filter, direction, limit)
            .await
            .map(|(_, docs)| docs)
    }

    /// Query documents using an index and return native BSON documents.
    pub async fn query_bson(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<Vec<BsonDocument>> {
        self.query_bson_with_query_id(collection, index, range, filter, direction, limit)
            .await
            .map(|(_, docs)| docs)
    }

    /// Query native BSON documents using an index and return the read-set query ID.
    pub async fn query_bson_with_query_id(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<(QueryId, Vec<BsonDocument>)> {
        let (qid, docs) = self
            .query_with_query_id(collection, index, range, filter, direction, limit)
            .await?;
        let docs = docs
            .into_iter()
            .map(|doc| {
                json_document_to_bson(&doc)
                    .map_err(|e| DatabaseError::Commit(format!("BSON encode error: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((qid, docs))
    }

    /// Query documents using an index and return the read-set query ID.
    pub async fn query_with_query_id(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<(QueryId, Vec<Value>)> {
        self.enter_operation("query")?;

        let qid = self.read_set.next_query_id();
        let (coll_id, _meta) = self.resolve_collection_with_query_id(collection, qid)?;
        let direction = direction.unwrap_or(ScanDirection::Forward);

        // Resolve index — guard scoped to this block, dropped before .await
        CatalogTracker::record_index_name_lookup(&mut self.read_set, qid, coll_id, index);
        self.check_read_limits()?;

        let index_info = {
            let cache = self.catalog.read();
            let idx = cache.get_index_by_name(coll_id, index).ok_or_else(|| {
                DatabaseError::IndexNotFound {
                    collection: collection.to_string(),
                    index: index.to_string(),
                }
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
                self.record_primary_get(coll_id, doc_id, qid)?;
                let body = primary
                    .get_at_ts(doc_id, self.begin_ts)
                    .await
                    .map_err(DatabaseError::Storage)?;
                match body {
                    Some(bytes) => {
                        self.check_scan_limits(1, bytes.len())?;
                        let doc = decode_document(&bytes)
                            .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;
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
                    &index_info.field_paths,
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
                    &index_info.field_paths,
                    &primaries,
                    &sec_arcs,
                )
                .await?
            }
        };

        Ok((qid, results))
    }

    fn record_primary_get(
        &mut self,
        coll_id: CollectionId,
        doc_id: &DocId,
        qid: QueryId,
    ) -> Result<()> {
        self.read_set.add_interval(
            coll_id,
            PRIMARY_INDEX_SENTINEL,
            ReadInterval {
                query_id: qid,
                lower: Bound::Included(make_primary_key(doc_id, u64::MAX).to_vec()),
                upper: Bound::Included(make_primary_key(doc_id, 0).to_vec()),
                limit_boundary: None,
            },
        );
        self.check_read_limits()
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
        index_fields: &[FieldPath],
        primaries: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        sec_arcs: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<Vec<Value>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }

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

        let stream = secondary.scan_entries_at_ts(lower_ref, upper_ref, self.begin_ts, direction);
        tokio::pin!(stream);

        let (inserts, deletes, replaces) = self.merge_view_parts(coll_id);
        self.charge_operations(
            inserts.len() + deletes.len() + replaces.len(),
            "pending write-set merge",
        )?;
        let lower_ref = bound_vec_as_ref(lower);
        let upper_ref = bound_vec_as_ref(upper);
        let pending_rows = keyed_pending_rows(
            &inserts,
            &replaces,
            index_fields,
            lower_ref,
            upper_ref,
            post_filter,
            direction,
        )
        .map_err(|e| DatabaseError::Commit(format!("index entry error: {e}")))?;
        let hidden_doc_ids = hidden_snapshot_doc_ids(&deletes, &replaces);
        let mut docs_scanned = 0usize;
        let mut bytes_scanned = 0usize;
        let mut merged_rows = Vec::new();

        if let Some(limit) = limit {
            let mut pending_index = 0usize;

            'scan: while let Some(entry) = stream.next().await {
                let entry = entry.map_err(DatabaseError::Storage)?;
                self.charge_operations(1, "secondary scan")?;

                while pending_index < pending_rows.len()
                    && pending_precedes_snapshot(
                        &pending_rows[pending_index].0,
                        &entry.key_prefix,
                        direction,
                    )
                {
                    merged_rows.push(pending_rows[pending_index].1.clone());
                    pending_index += 1;
                    if merged_rows.len() >= limit {
                        break 'scan;
                    }
                }

                docs_scanned += 1;
                self.check_scan_limits(docs_scanned, bytes_scanned)?;

                if hidden_doc_ids.contains(&entry.doc_id) {
                    continue;
                }

                let Some(bytes) = primary
                    .get_version(&entry.doc_id, entry.version_ts)
                    .await
                    .map_err(DatabaseError::Storage)?
                else {
                    continue;
                };

                bytes_scanned += bytes.len();
                self.check_scan_limits(docs_scanned, bytes_scanned)?;
                let doc = decode_document(&bytes)
                    .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;

                if let Some(filter) = post_filter
                    && !exdb_query::filter_matches(&doc, filter)
                {
                    continue;
                }

                let snapshot_key = extract_sort_key(&doc, index_fields);
                while pending_index < pending_rows.len()
                    && pending_precedes_snapshot(
                        &pending_rows[pending_index].0,
                        &snapshot_key,
                        direction,
                    )
                {
                    merged_rows.push(pending_rows[pending_index].1.clone());
                    pending_index += 1;
                    if merged_rows.len() >= limit {
                        break 'scan;
                    }
                }

                merged_rows.push(ScanRow {
                    doc_id: entry.doc_id,
                    version_ts: entry.version_ts,
                    doc,
                });
                if merged_rows.len() >= limit {
                    break;
                }
            }

            while merged_rows.len() < limit && pending_index < pending_rows.len() {
                merged_rows.push(pending_rows[pending_index].1.clone());
                pending_index += 1;
            }
        } else {
            let mut snapshot_rows = Vec::new();
            while let Some(entry) = stream.next().await {
                let entry = entry.map_err(DatabaseError::Storage)?;
                docs_scanned += 1;
                self.charge_operations(1, "secondary scan")?;
                self.check_scan_limits(docs_scanned, bytes_scanned)?;

                if hidden_doc_ids.contains(&entry.doc_id) {
                    continue;
                }

                // Fetch the exact primary version verified by the secondary
                // scan so concurrent replacements cannot change the body
                // between verification and decode.
                let body = primary
                    .get_version(&entry.doc_id, entry.version_ts)
                    .await
                    .map_err(DatabaseError::Storage)?;

                if let Some(bytes) = body {
                    bytes_scanned += bytes.len();
                    self.check_scan_limits(docs_scanned, bytes_scanned)?;
                    let doc = decode_document(&bytes)
                        .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;

                    // Apply post-filter
                    if let Some(filter) = post_filter
                        && !exdb_query::filter_matches(&doc, filter)
                    {
                        continue;
                    }

                    snapshot_rows.push(ScanRow {
                        doc_id: entry.doc_id,
                        version_ts: entry.version_ts,
                        doc,
                    });
                }
            }

            let merge_view = MergeView {
                inserts: &inserts,
                deletes: &deletes,
                replaces: &replaces,
            };
            merged_rows = merge_with_writes(
                tokio_stream::iter(snapshot_rows.into_iter().map(Ok)),
                &merge_view,
                index_fields,
                lower_ref,
                upper_ref,
                post_filter,
                direction,
                limit,
            )
            .await
            .map_err(DatabaseError::Storage)?;
        }

        let limit_boundary = self.compute_limit_boundary(
            &merged_rows,
            index_fields,
            lower,
            upper,
            direction,
            limit,
        )?;

        self.read_set.add_interval(
            coll_id,
            index_id,
            ReadInterval {
                query_id: qid,
                lower: Bound::Included(lower_bytes),
                upper: upper.clone(),
                limit_boundary,
            },
        );
        self.check_read_limits()?;

        Ok(merged_rows.into_iter().map(|row| row.doc).collect())
    }

    fn merge_view_parts(
        &self,
        coll_id: CollectionId,
    ) -> (PendingDocRows, PendingDeletes, PendingDocRows) {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        let mut replaces = Vec::new();

        for (doc_id, entry) in self.write_set.mutations_for_collection(coll_id) {
            match entry.op {
                MutationOp::Insert => {
                    if let Some(body) = &entry.body {
                        inserts.push((*doc_id, body.clone()));
                    }
                }
                MutationOp::Replace => {
                    if let Some(body) = &entry.body {
                        replaces.push((*doc_id, body.clone()));
                    }
                }
                MutationOp::Delete => deletes.push(*doc_id),
            }
        }

        (inserts, deletes, replaces)
    }

    fn compute_limit_boundary(
        &self,
        rows: &[ScanRow],
        index_fields: &[FieldPath],
        lower: &Bound<Vec<u8>>,
        upper: &Bound<Vec<u8>>,
        direction: ScanDirection,
        limit: Option<usize>,
    ) -> Result<Option<LimitBoundary>> {
        let Some(limit) = limit else {
            return Ok(None);
        };
        if limit == 0 || rows.len() < limit {
            return Ok(None);
        }
        let Some(last_row) = rows.last() else {
            return Ok(None);
        };

        let prefixes = compute_index_entries(&last_row.doc, index_fields)
            .map_err(|e| DatabaseError::Commit(format!("index entry error: {e}")))?;
        let mut keys: Vec<Vec<u8>> = prefixes
            .iter()
            .map(|prefix| {
                make_secondary_key_from_prefix(prefix, &last_row.doc_id, last_row.version_ts)
            })
            .filter(|key| key_in_original_bounds(key, lower, upper))
            .collect();
        if keys.is_empty() {
            return Ok(None);
        }
        keys.sort();
        let key = match direction {
            ScanDirection::Forward => keys.remove(0),
            ScanDirection::Backward => keys.pop().unwrap(),
        };
        Ok(Some(match direction {
            ScanDirection::Forward => LimitBoundary::Upper(key),
            ScanDirection::Backward => LimitBoundary::Lower(key),
        }))
    }

    /// List all collections.
    pub fn list_collections(&mut self) -> Result<Vec<CollectionMeta>> {
        self.enter_operation("list_collections")?;

        let qid = self.read_set.next_query_id();
        CatalogTracker::record_list_collections(&mut self.read_set, qid);
        self.check_read_limits()?;

        let collections = {
            let cache = self.catalog.read();
            cache.list_collections()
        };
        self.charge_operations(collections.len(), "list_collections committed catalog rows")?;
        self.apply_pending_collection_mutations(collections)
    }

    fn apply_pending_collection_mutations(
        &mut self,
        mut collections: Vec<CollectionMeta>,
    ) -> Result<Vec<CollectionMeta>> {
        self.charge_operations(
            self.write_set.catalog_mutations.len(),
            "list_collections pending catalog overlay",
        )?;

        let mut dropped = HashSet::new();

        for mutation in &self.write_set.catalog_mutations {
            if let CatalogMutation::DropCollection { collection_id, .. } = mutation {
                dropped.insert(*collection_id);
            }
        }

        collections.retain(|collection| !dropped.contains(&collection.collection_id));

        for mutation in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateCollection {
                name,
                provisional_id,
                primary_root_page,
                ..
            } = mutation
            {
                if dropped.contains(provisional_id) {
                    continue;
                }

                if collections
                    .iter()
                    .any(|collection| collection.collection_id == *provisional_id)
                {
                    continue;
                }

                collections.push(CollectionMeta {
                    collection_id: *provisional_id,
                    name: name.clone(),
                    primary_root_page: *primary_root_page,
                    doc_count: 0,
                });
            }
        }

        Ok(collections)
    }

    /// List all indexes for a collection.
    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<IndexMeta>> {
        self.enter_operation("list_indexes")?;

        let (coll_id, _) = self.resolve_collection(collection)?;
        let qid = self.read_set.next_query_id();
        CatalogTracker::record_list_indexes(&mut self.read_set, qid, coll_id);
        self.check_read_limits()?;

        let indexes = {
            let cache = self.catalog.read();
            cache.list_indexes(coll_id)
        };
        self.charge_operations(indexes.len(), "list_indexes committed catalog rows")?;
        self.apply_pending_index_mutations(coll_id, indexes)
    }

    fn apply_pending_index_mutations(
        &mut self,
        collection_id: CollectionId,
        mut indexes: Vec<IndexMeta>,
    ) -> Result<Vec<IndexMeta>> {
        self.charge_operations(
            self.write_set.catalog_mutations.len(),
            "list_indexes pending catalog overlay",
        )?;

        for mutation in &self.write_set.catalog_mutations {
            match mutation {
                CatalogMutation::CreateIndex {
                    collection_id: idx_collection_id,
                    name,
                    field_paths,
                    provisional_id,
                    root_page,
                } if *idx_collection_id == collection_id => {
                    if indexes.iter().any(|index| {
                        index.index_id == *provisional_id
                            || (index.collection_id == collection_id && index.name == *name)
                    }) {
                        continue;
                    }

                    indexes.push(IndexMeta {
                        index_id: *provisional_id,
                        collection_id,
                        name: name.clone(),
                        field_paths: field_paths.clone(),
                        root_page: *root_page,
                        state: IndexState::Building,
                    });
                }
                CatalogMutation::DropIndex {
                    collection_id: idx_collection_id,
                    index_id,
                    name,
                    ..
                } if *idx_collection_id == collection_id => {
                    indexes.retain(|index| index.index_id != *index_id && index.name != *name);
                }
                CatalogMutation::DropCollection {
                    collection_id: dropped_collection_id,
                    ..
                } if *dropped_collection_id == collection_id => {
                    indexes.clear();
                }
                _ => {}
            }
        }

        Ok(indexes)
    }

    // ─── Write Operations ───

    /// Insert a new document. Returns the generated DocId.
    pub async fn insert(&mut self, collection: &str, mut body: Value) -> Result<DocId> {
        self.enter_operation("insert")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Generate doc ID
        let doc_id = generate_ulid();

        // Set _created_at
        if let Value::Object(ref mut map) = body {
            map.insert("_id".to_string(), Value::String(encode_ulid(&doc_id)));
            map.insert(
                "_created_at".to_string(),
                Value::Number(serde_json::Number::from(self.wall_clock_ms)),
            );
            retain_only_meta_types(map);
        }

        self.validate_doc_size(&body)?;
        self.validate_document_for_active_indexes(coll_id, collection, &body)?;

        self.write_set.insert(coll_id, doc_id, body);

        Ok(doc_id)
    }

    /// Insert a native BSON document. Returns the generated DocId.
    pub async fn insert_bson(&mut self, collection: &str, body: BsonDocument) -> Result<DocId> {
        let body = bson_document_to_json(body)
            .map_err(|e| DatabaseError::Commit(format!("BSON decode error: {e}")))?;
        self.insert(collection, body).await
    }

    /// Replace an existing document.
    pub async fn replace(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        mut body: Value,
    ) -> Result<()> {
        self.enter_operation("replace")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        // Verify document exists
        let existing = self.get(collection, doc_id).await?;
        let existing = existing.ok_or(DatabaseError::DocNotFound)?;

        if let Value::Object(ref mut map) = body {
            retain_only_meta_types(map);
            if let Some(id) = existing.get("_id") {
                map.insert("_id".to_string(), id.clone());
            }
            if let Some(created_at) = existing.get("_created_at") {
                map.insert("_created_at".to_string(), created_at.clone());
            }
        }
        self.validate_doc_size(&body)?;
        self.validate_document_for_active_indexes(coll_id, collection, &body)?;

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

    /// Replace an existing document with a native BSON document.
    pub async fn replace_bson(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        body: BsonDocument,
    ) -> Result<()> {
        let body = bson_document_to_json(body)
            .map_err(|e| DatabaseError::Commit(format!("BSON decode error: {e}")))?;
        self.replace(collection, doc_id, body).await
    }

    /// Patch an existing document (RFC 7396 merge-patch).
    pub async fn patch(&mut self, collection: &str, doc_id: &DocId, patch: Value) -> Result<()> {
        self.enter_operation("patch")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        // Get current document
        let existing = self.get(collection, doc_id).await?;
        let mut doc = existing.ok_or(DatabaseError::DocNotFound)?;

        let unset_paths = meta_unset_paths(&patch)?;

        // Apply top-level patch fields. `_meta` is interpreted as wire
        // metadata and is not applied as a user document field.
        apply_patch(&mut doc, &patch);
        merge_patch_meta_types(&mut doc, &patch);
        apply_unset_paths(&mut doc, &unset_paths);

        // Replace with patched version
        self.replace(collection, doc_id, doc).await
    }

    /// Delete a document.
    pub async fn delete(&mut self, collection: &str, doc_id: &DocId) -> Result<()> {
        self.enter_operation("delete")?;

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
        self.enter_operation("create_collection")?;

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
        self.check_read_limits()?;

        // Check if already exists — guard scoped to block
        {
            let cache = self.catalog.read();
            if cache.has_collection(name) {
                return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
            }
        }

        // Check write set for pending creation
        self.charge_operations(
            self.write_set.catalog_mutations.len(),
            "create_collection pending duplicate check",
        )?;
        if self.write_set.resolve_pending_collection(name).is_some() {
            return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
        }

        let provisional_id = self.catalog.read().allocate_collection_id();

        self.write_set
            .add_catalog_mutation(CatalogMutation::CreateCollection {
                name: name.to_string(),
                provisional_id,
                primary_root_page: 0,
                created_at_root_page: 0,
            });

        Ok(())
    }

    /// Drop a collection.
    pub async fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.enter_operation("drop_collection")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        let (coll_id, meta) = self.resolve_collection(name)?;

        // Gather indexes — guard scoped to block
        let indexes: Vec<IndexMeta> = {
            let cache = self.catalog.read();
            cache.list_indexes(coll_id)
        };
        self.charge_operations(indexes.len(), "drop_collection index cascade")?;

        let dropped_indexes: Vec<DroppedIndexMeta> = indexes
            .iter()
            .map(|idx| DroppedIndexMeta {
                index_id: idx.index_id,
                name: idx.name.clone(),
                field_paths: idx.field_paths.clone(),
                root_page: idx.root_page,
            })
            .collect();

        self.write_set
            .add_catalog_mutation(CatalogMutation::DropCollection {
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
        self.enter_operation("create_index")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        if name.starts_with('_') {
            return Err(DatabaseError::SystemIndex(name.to_string()));
        }

        validate_index_field_paths(&fields)?;

        let (coll_id, _) = self.resolve_collection(collection)?;

        let qid = self.read_set.next_query_id();
        CatalogTracker::record_index_name_lookup(&mut self.read_set, qid, coll_id, name);
        self.check_read_limits()?;

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

        self.charge_operations(
            self.write_set.catalog_mutations.len(),
            "create_index pending duplicate check",
        )?;
        if self.write_set.catalog_mutations.iter().any(|mutation| {
            matches!(
                mutation,
                CatalogMutation::CreateIndex {
                    collection_id: pending_collection_id,
                    name: pending_name,
                    ..
                } if *pending_collection_id == coll_id && pending_name == name
            )
        }) {
            return Err(DatabaseError::IndexAlreadyExists {
                collection: collection.to_string(),
                index: name.to_string(),
            });
        }

        self.validate_collection_for_new_index(collection, coll_id, &fields)
            .await?;

        let provisional_id = self.catalog.read().allocate_index_id();

        self.write_set
            .add_catalog_mutation(CatalogMutation::CreateIndex {
                collection_id: coll_id,
                name: name.to_string(),
                field_paths: fields,
                provisional_id,
                root_page: 0,
            });

        Ok(())
    }

    fn validate_document_for_active_indexes(
        &mut self,
        coll_id: CollectionId,
        collection: &str,
        doc: &Value,
    ) -> Result<()> {
        for (index_name, field_paths) in self.active_index_fields_for_collection(coll_id) {
            self.charge_operations(1, "document index validation")?;
            compute_index_entries(doc, &field_paths).map_err(|err| {
                DatabaseError::Commit(format!(
                    "document violates index '{collection}.{index_name}' array constraint: {err}"
                ))
            })?;
        }
        Ok(())
    }

    fn active_index_fields_for_collection(
        &self,
        coll_id: CollectionId,
    ) -> Vec<(String, Vec<FieldPath>)> {
        let mut indexes: Vec<(String, Vec<FieldPath>)> = {
            let cache = self.catalog.read();
            cache
                .list_indexes(coll_id)
                .into_iter()
                .filter(|index| !self.index_dropped_in_transaction(index))
                .map(|index| (index.name, index.field_paths))
                .collect()
        };

        for mutation in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateIndex {
                collection_id,
                name,
                field_paths,
                ..
            } = mutation
                && *collection_id == coll_id
            {
                indexes.push((name.clone(), field_paths.clone()));
            }
        }

        indexes
    }

    fn index_dropped_in_transaction(&self, index: &IndexMeta) -> bool {
        self.write_set
            .catalog_mutations
            .iter()
            .any(|mutation| match mutation {
                CatalogMutation::DropCollection { collection_id, .. } => {
                    *collection_id == index.collection_id
                }
                CatalogMutation::DropIndex {
                    index_id,
                    collection_id,
                    name,
                    ..
                } => {
                    *index_id == index.index_id
                        || (*collection_id == index.collection_id && name == &index.name)
                }
                _ => false,
            })
    }

    async fn validate_collection_for_new_index(
        &mut self,
        collection: &str,
        coll_id: CollectionId,
        field_paths: &[FieldPath],
    ) -> Result<()> {
        let (inserts, deletes, replaces) = self.merge_view_parts(coll_id);
        let hidden_doc_ids = hidden_snapshot_doc_ids(&deletes, &replaces);

        let primary = self.primary_indexes.read().get(&coll_id).cloned();
        let mut docs_scanned = 0usize;
        let mut bytes_scanned = 0usize;
        if let Some(primary) = primary {
            let mut stream = primary.scan_at_ts(self.begin_ts, ScanDirection::Forward);
            while let Some(item) = stream.next().await {
                let (doc_id, _version_ts, body) = item.map_err(DatabaseError::Storage)?;
                docs_scanned += 1;
                bytes_scanned += body.len();
                self.charge_operations(1, "create_index validation")?;
                self.check_scan_limits(docs_scanned, bytes_scanned)?;

                if hidden_doc_ids.contains(&doc_id) {
                    continue;
                }

                let doc = decode_document(&body)
                    .map_err(|e| DatabaseError::Commit(format!("decode error: {e}")))?;
                compute_index_entries(&doc, field_paths).map_err(|err| {
                    DatabaseError::Commit(format!(
                        "index '{collection}' violates array constraint for existing document {}: {err}",
                        encode_ulid(&doc_id)
                    ))
                })?;
            }
        }

        for (doc_id, doc) in inserts.iter().chain(replaces.iter()) {
            self.charge_operations(1, "create_index pending validation")?;
            compute_index_entries(doc, field_paths).map_err(|err| {
                DatabaseError::Commit(format!(
                    "index '{collection}' violates array constraint for pending document {}: {err}",
                    encode_ulid(doc_id)
                ))
            })?;
        }

        Ok(())
    }

    /// Drop a secondary index.
    pub async fn drop_index(&mut self, collection: &str, name: &str) -> Result<()> {
        self.enter_operation("drop_index")?;

        if self.opts.readonly {
            return Err(DatabaseError::ReadonlyWrite);
        }

        if name == "_created_at" || name == "_id" {
            return Err(DatabaseError::SystemIndex(name.to_string()));
        }

        let (coll_id, _) = self.resolve_collection(collection)?;

        let qid = self.read_set.next_query_id();
        CatalogTracker::record_index_name_lookup(&mut self.read_set, qid, coll_id, name);
        self.check_read_limits()?;

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

        self.write_set
            .add_catalog_mutation(CatalogMutation::DropIndex {
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
        self.enter_operation("commit")?;
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
                        self.tx_id,
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

    /// Consume this transaction into a typed replica-promotion payload.
    ///
    /// The returned timestamp is the snapshot timestamp that must accompany the
    /// payload in the L7 promotion request. The primary decodes the payload and
    /// submits the concrete read/write sets through its normal commit path.
    pub fn into_promotion_payload(self) -> Result<(Ts, Vec<u8>)> {
        let promotion = self.into_promotion_payload_parts()?;
        Ok((promotion.begin_ts, promotion.payload))
    }

    /// Consume this transaction into a typed replica-promotion payload plus the
    /// local read/write sets needed for replica-side subscription registration.
    pub fn into_promotion_payload_parts(mut self) -> Result<PromotionPayload> {
        self.enter_operation("promotion")?;
        self.committed = true;

        let payload = serialize_promotion_payload(&self.read_set, &self.write_set)
            .map_err(DatabaseError::Commit)?;
        Ok(PromotionPayload {
            tx_id: self.tx_id,
            begin_ts: self.begin_ts,
            read_set: std::mem::replace(&mut self.read_set, ReadSet::new()),
            write_set: std::mem::replace(&mut self.write_set, WriteSet::new()),
            payload,
        })
    }

    /// Rollback the transaction (no-op: just drops state).
    pub fn rollback(mut self) {
        self.committed = true;
    }

    /// Reset the transaction for reuse on the same snapshot.
    pub fn reset(&mut self) {
        if self.abort_reason.is_none() {
            let _ = self.check_timeout();
        }
        self.read_set = ReadSet::new();
        self.write_set = WriteSet::new();
        self.operations_used = 0;
        if self.abort_reason.is_none() {
            self.last_activity = Instant::now();
        }
    }
}

fn bound_vec_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(value) => Bound::Included(value.as_slice()),
        Bound::Excluded(value) => Bound::Excluded(value.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn key_in_original_bounds(key: &[u8], lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    let above_lower = match lower {
        Bound::Included(value) => key >= value.as_slice(),
        Bound::Excluded(value) => key > value.as_slice(),
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(value) => key <= value.as_slice(),
        Bound::Excluded(value) => key < value.as_slice(),
        Bound::Unbounded => true,
    };
    above_lower && below_upper
}

fn hidden_snapshot_doc_ids(deletes: &[DocId], replaces: &[(DocId, Value)]) -> HashSet<DocId> {
    deletes
        .iter()
        .copied()
        .chain(replaces.iter().map(|(doc_id, _)| *doc_id))
        .collect()
}

fn retain_only_meta_types(map: &mut serde_json::Map<String, Value>) {
    let Some(types) = map.get("_meta").and_then(|meta| meta.get("types")).cloned() else {
        map.remove("_meta");
        return;
    };

    map.insert("_meta".to_string(), serde_json::json!({ "types": types }));
}

fn validate_index_field_paths(fields: &[FieldPath]) -> Result<()> {
    if fields.is_empty() {
        return Err(DatabaseError::InvalidFieldPath(
            "index must contain at least one field path".to_string(),
        ));
    }

    for field in fields {
        if field.segments().is_empty() {
            return Err(DatabaseError::InvalidFieldPath(
                "field path cannot be empty".to_string(),
            ));
        }
        for segment in field.segments() {
            if segment.is_empty() {
                return Err(DatabaseError::InvalidFieldPath(
                    "field path segment cannot be empty".to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn meta_unset_paths(patch: &Value) -> Result<Vec<Vec<String>>> {
    let Some(unset) = patch.get("_meta").and_then(|meta| meta.get("unset")) else {
        return Ok(Vec::new());
    };

    let Value::Array(paths) = unset else {
        return Err(DatabaseError::Commit(
            "_meta.unset must be an array of field paths".to_string(),
        ));
    };

    paths
        .iter()
        .map(meta_unset_path)
        .collect::<Result<Vec<_>>>()
}

fn meta_unset_path(value: &Value) -> Result<Vec<String>> {
    match value {
        Value::String(segment) if !segment.is_empty() && segment != "_meta" => {
            Ok(vec![segment.clone()])
        }
        Value::Array(segments) => {
            let mut parsed = Vec::with_capacity(segments.len());
            for segment in segments {
                let Some(segment) = segment.as_str() else {
                    return Err(DatabaseError::Commit(
                        "_meta.unset path arrays must contain strings".to_string(),
                    ));
                };
                if segment.is_empty() || (parsed.is_empty() && segment == "_meta") {
                    return Err(DatabaseError::Commit(
                        "_meta.unset path segments cannot be empty or start with _meta".to_string(),
                    ));
                }
                parsed.push(segment.to_string());
            }
            if parsed.is_empty() {
                Err(DatabaseError::Commit(
                    "_meta.unset path arrays cannot be empty".to_string(),
                ))
            } else {
                Ok(parsed)
            }
        }
        _ => Err(DatabaseError::Commit(
            "_meta.unset entries must be strings or string arrays".to_string(),
        )),
    }
}

fn merge_patch_meta_types(doc: &mut Value, patch: &Value) {
    let Some(types) = patch
        .get("_meta")
        .and_then(|meta| meta.get("types"))
        .cloned()
    else {
        return;
    };
    let Value::Object(map) = doc else {
        return;
    };
    let meta = map
        .entry("_meta".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !meta.is_object() {
        *meta = serde_json::json!({});
    }
    let meta_map = meta.as_object_mut().expect("checked is_object above");
    meta_map.insert("types".to_string(), types);
}

fn apply_unset_paths(doc: &mut Value, paths: &[Vec<String>]) {
    for path in paths {
        remove_path(doc, path);
        remove_path_from_meta_types(doc, path);
    }
}

fn remove_path(doc: &mut Value, path: &[String]) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut current = doc;
    for segment in parents {
        let Some(next) = current.get_mut(segment) else {
            return;
        };
        current = next;
    }
    if let Value::Object(map) = current {
        map.remove(last);
    }
}

fn remove_path_from_meta_types(doc: &mut Value, path: &[String]) {
    let Some(types) = doc.get_mut("_meta").and_then(|meta| meta.get_mut("types")) else {
        return;
    };
    remove_path(types, path);
}

fn keyed_pending_rows(
    inserts: &[(DocId, Value)],
    replaces: &[(DocId, Value)],
    index_fields: &[FieldPath],
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    post_filter: Option<&Filter>,
    direction: ScanDirection,
) -> std::result::Result<Vec<(Vec<u8>, ScanRow)>, String> {
    let mut rows = Vec::new();
    for (doc_id, doc) in inserts.iter().chain(replaces.iter()) {
        if let Some(filter) = post_filter
            && !exdb_query::filter_matches(doc, filter)
        {
            continue;
        }

        for key in compute_index_entries(doc, index_fields)? {
            if !sort_key_in_range(&key, lower, upper) {
                continue;
            }
            rows.push((
                key,
                ScanRow {
                    doc_id: *doc_id,
                    version_ts: 0,
                    doc: doc.clone(),
                },
            ));
        }
    }

    match direction {
        ScanDirection::Forward => rows.sort_by(|a, b| a.0.cmp(&b.0)),
        ScanDirection::Backward => rows.sort_by(|a, b| b.0.cmp(&a.0)),
    }

    Ok(rows)
}

fn pending_precedes_snapshot(
    pending_key: &[u8],
    snapshot_key: &[u8],
    direction: ScanDirection,
) -> bool {
    match direction {
        ScanDirection::Forward => pending_key < snapshot_key,
        ScanDirection::Backward => pending_key > snapshot_key,
    }
}

fn extract_sort_key(doc: &Value, index_fields: &[FieldPath]) -> Vec<u8> {
    let scalars: Vec<Scalar> = index_fields
        .iter()
        .map(|field| extract_scalar(doc, field).unwrap_or(Scalar::Undefined))
        .collect();
    encode_key_prefix(&scalars)
}

fn sort_key_in_range(key: &[u8], lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    let above_lower = match lower {
        Bound::Included(value) => key >= value,
        Bound::Excluded(value) => key > value,
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(value) => key <= value,
        Bound::Excluded(value) => key < value,
        Bound::Unbounded => true,
    };
    above_lower && below_upper
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.active_tx_count.fetch_sub(1, Ordering::AcqRel);
        if !self.committed {
            tracing::debug!("transaction {} dropped without commit/rollback", self.tx_id);
        }
    }
}
