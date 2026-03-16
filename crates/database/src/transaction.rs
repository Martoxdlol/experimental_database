//! B5: Unified Transaction type.
//!
//! Composes L4 (query execution) and L5 (read/write sets, commit) into a
//! coherent transaction API with read-your-writes, catalog name resolution,
//! and transactional DDL.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Instant;

use exdb_core::encoding::{decode_document, encode_document, extract_scalar};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::filter::RangeExpr;
use exdb_core::types::{CollectionId, DocId, IndexId, Scalar, Ts, TxId};
use exdb_core::ulid::{encode_ulid, generate_ulid};
use exdb_docstore::{
    encode_key_prefix, make_secondary_key_from_prefix, PrimaryIndex, SecondaryIndex,
};
use exdb_query::{merge_with_writes, resolve_access, AccessMethod, MergeView, ScanRow};
use exdb_query::{IndexInfo as QueryIndexInfo, ReadIntervalInfo};
use exdb_storage::btree::ScanDirection;
use exdb_tx::read_set::{LimitBoundary, QueryId, ReadInterval, ReadSet};
use exdb_tx::{
    CatalogMutation, CommitHandle, CommitRequest, CommitResult, MutationOp, SubscriptionMode,
    WriteSet,
};
use parking_lot::RwLock;
use tokio_stream::StreamExt;

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};
use crate::catalog_tracker::CatalogTracker;
use crate::config::TransactionConfig;
use crate::error::{DatabaseError, Result};
use crate::subscription::SubscriptionHandle;

/// Options for beginning a transaction.
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    /// If true, write operations return `ReadonlyWrite` error.
    pub readonly: bool,
    /// Subscription mode — controls post-commit read set behavior.
    pub subscription: SubscriptionMode,
    /// Session ID for subscription association.
    pub session_id: u64,
}

impl TransactionOptions {
    pub fn readonly() -> Self {
        Self {
            readonly: true,
            subscription: SubscriptionMode::None,
            session_id: 0,
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            readonly: false,
            subscription: SubscriptionMode::None,
            session_id: 0,
        }
    }
}

/// Result of a committed transaction.
pub enum TransactionResult {
    Success {
        commit_ts: Ts,
        subscription_handle: Option<SubscriptionHandle>,
    },
    Conflict {
        error: exdb_tx::ConflictError,
        retry: Option<exdb_tx::ConflictRetry>,
    },
    QuorumLost,
}

/// Shared database state that a transaction needs access to.
pub(crate) struct TransactionContext {
    pub catalog: Arc<RwLock<CatalogCache>>,
    pub primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    pub secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    pub commit_handle: CommitHandle,
    pub tx_config: TransactionConfig,
    pub max_doc_size: usize,
}

/// Unified transaction — handles both reads and writes.
pub struct Transaction<'db> {
    ctx: &'db TransactionContext,
    tx_id: TxId,
    opts: TransactionOptions,
    begin_ts: Ts,
    wall_clock_ms: u64,
    read_set: ReadSet,
    write_set: WriteSet,
    committed: bool,
    created_at: Instant,
    last_activity: Instant,
    dropped_collections: Vec<CollectionId>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(
        ctx: &'db TransactionContext,
        tx_id: TxId,
        opts: TransactionOptions,
        begin_ts: Ts,
        wall_clock_ms: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            ctx,
            tx_id,
            opts,
            begin_ts,
            wall_clock_ms,
            read_set: ReadSet::new(),
            write_set: WriteSet::new(),
            committed: false,
            created_at: now,
            last_activity: now,
            dropped_collections: Vec::new(),
        }
    }

    // ── Introspection ──

    pub fn tx_id(&self) -> TxId { self.tx_id }
    pub fn begin_ts(&self) -> Ts { self.begin_ts }
    pub fn is_readonly(&self) -> bool { self.opts.readonly }
    pub fn subscription_mode(&self) -> SubscriptionMode { self.opts.subscription }

    // ── Read Operations ──

    /// Get a document by ID.
    pub async fn get(
        &mut self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<Option<serde_json::Value>> {
        self.check_timeout()?;
        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;

        // Check write set first (read-your-writes)
        if let Some(entry) = self.write_set.get(collection_id, doc_id) {
            return match entry.op {
                MutationOp::Delete => Ok(None),
                MutationOp::Insert | MutationOp::Replace => Ok(entry.body.clone()),
            };
        }

        // Fall through to primary index I/O
        // Clone the Arc to release the lock before awaiting
        let primary = {
            let primary_indexes = self.ctx.primary_indexes.read();
            primary_indexes.get(&collection_id).cloned()
        };

        let ri = make_point_read_interval(collection_id, doc_id);

        let primary = match primary {
            Some(p) => p,
            None => {
                // Collection only in write set (pending create) — doc doesn't exist
                let interval = ReadInterval {
                    query_id,
                    lower: Bound::Included(ri.lower.clone()),
                    upper: ri.upper.clone(),
                    limit_boundary: None,
                };
                self.read_set.add_interval(ri.collection_id, ri.index_id, interval);
                return Ok(None);
            }
        };

        let body = primary.get_at_ts(doc_id, self.begin_ts)
            .await
            .map_err(DatabaseError::Storage)?;

        let doc_opt = match body {
            Some(bytes) => {
                Some(decode_document(&bytes)
                    .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?)
            }
            None => None,
        };

        let read_interval_info = ri;

        // Record read interval
        let interval = ReadInterval {
            query_id,
            lower: Bound::Included(read_interval_info.lower),
            upper: read_interval_info.upper,
            limit_boundary: None,
        };
        self.read_set.add_interval(
            read_interval_info.collection_id,
            read_interval_info.index_id,
            interval,
        );

        Ok(doc_opt)
    }

    /// Query documents using an index.
    pub async fn query(
        &mut self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<&Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        self.check_timeout()?;
        let query_id = self.read_set.next_query_id();
        let direction = direction.unwrap_or(ScanDirection::Forward);

        let collection_id = self.resolve_collection(collection, query_id)?;
        let index_meta = self.resolve_index(collection, collection_id, index, query_id)?;

        if index_meta.state != IndexState::Ready {
            return Err(DatabaseError::IndexNotReady(index.to_string()));
        }

        let query_index_info = QueryIndexInfo {
            index_id: index_meta.index_id,
            field_paths: index_meta.field_paths.clone(),
            ready: true,
        };

        let method = resolve_access(
            collection_id,
            &query_index_info,
            range,
            filter.cloned(),
            direction,
            limit,
        )
        .map_err(|e| match e {
            exdb_query::AccessError::IndexNotReady => {
                DatabaseError::IndexNotReady(index.to_string())
            }
            exdb_query::AccessError::Range(r) => DatabaseError::Range(format!("{r:?}")),
        })?;

        // Execute the scan and collect all results while holding locks
        let (snapshot_rows, read_interval_info) = self.execute_scan_collect(&method, collection, index).await?;

        // Merge with writes if read-write transaction
        let results = if !self.opts.readonly && !self.write_set.is_empty() {
            let (inserts, deletes, replaces) = self.decompose_write_set(collection_id);

            let merge_view = MergeView {
                inserts: &inserts,
                deletes: &deletes,
                replaces: &replaces,
            };

            let (range_lower, range_upper) = match &method {
                AccessMethod::IndexScan { lower, upper, .. } => {
                    (bound_as_ref(lower), bound_as_ref(upper))
                }
                _ => (Bound::Unbounded, Bound::Unbounded),
            };

            // Create a stream from collected rows
            let stream = tokio_stream::iter(
                snapshot_rows.into_iter().map(Ok::<_, std::io::Error>)
            );

            merge_with_writes(
                stream,
                &merge_view,
                &index_meta.field_paths,
                range_lower,
                range_upper,
                filter,
                direction,
                limit,
            )
            .await
            .map_err(DatabaseError::Storage)?
        } else {
            // Apply limit on collected rows
            let mut rows = snapshot_rows;
            if let Some(lim) = limit {
                rows.truncate(lim);
            }
            rows
        };

        // Check read limits
        self.check_read_limits()?;

        // Compute LimitBoundary
        let limit_boundary = if let Some(lim) = limit {
            if results.len() == lim && !results.is_empty() {
                let last_row = results.last().unwrap();
                Some(compute_limit_boundary(last_row, &index_meta, direction))
            } else {
                None
            }
        } else {
            None
        };

        // Record read interval
        let interval = ReadInterval {
            query_id,
            lower: Bound::Included(read_interval_info.lower),
            upper: read_interval_info.upper,
            limit_boundary,
        };
        self.read_set.add_interval(
            read_interval_info.collection_id,
            read_interval_info.index_id,
            interval,
        );

        Ok(results.into_iter().map(|r| r.doc).collect())
    }

    /// Execute scan and collect all results, releasing index locks afterward.
    async fn execute_scan_collect(
        &self,
        method: &AccessMethod,
        collection_name: &str,
        index_name: &str,
    ) -> Result<(Vec<ScanRow>, ReadIntervalInfo)> {
        let collection_id = match method {
            AccessMethod::PrimaryGet { collection_id, .. }
            | AccessMethod::IndexScan { collection_id, .. }
            | AccessMethod::TableScan { collection_id, .. } => *collection_id,
        };

        // Clone Arcs out of locks before any .await
        let primary = {
            let guard = self.ctx.primary_indexes.read();
            match guard.get(&collection_id) {
                Some(p) => Arc::clone(p),
                None => {
                    let ri = compute_read_interval_for_method(method);
                    return Ok((Vec::new(), ri));
                }
            }
        };

        let sec_index = match method {
            AccessMethod::IndexScan { index_id, .. }
            | AccessMethod::TableScan { index_id, .. } => {
                let guard = self.ctx.secondary_indexes.read();
                Some(guard.get(index_id).cloned().ok_or_else(|| {
                    DatabaseError::IndexNotFound {
                        collection: collection_name.to_string(),
                        index: index_name.to_string(),
                    }
                })?)
            }
            _ => None,
        };

        let ri = compute_read_interval_for_method(method);

        let mut rows = Vec::new();

        match method {
            AccessMethod::PrimaryGet { doc_id, .. } => {
                let body = primary.get_at_ts(doc_id, self.begin_ts)
                    .await
                    .map_err(DatabaseError::Storage)?;
                if let Some(bytes) = body {
                    let version_ts = primary.get_version_ts(doc_id, self.begin_ts)
                        .await
                        .map_err(DatabaseError::Storage)?;
                    if let Some(ts) = version_ts {
                        let doc = decode_document(&bytes)
                            .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;
                        rows.push(ScanRow { doc_id: *doc_id, version_ts: ts, doc });
                    }
                }
            }
            AccessMethod::IndexScan { lower, upper, post_filter, limit, direction, .. } => {
                let sec = sec_index.as_ref().unwrap();
                let mut scanner = sec.scan_at_ts(
                    bound_as_ref(lower),
                    bound_as_ref(upper),
                    self.begin_ts,
                    *direction,
                );
                let mut returned = 0usize;
                while let Some(result) = scanner.next().await {
                    if let Some(lim) = limit
                        && returned >= *lim { break; }
                    let (doc_id, version_ts) = result.map_err(DatabaseError::Storage)?;
                    let body_bytes = match primary.get_at_ts(&doc_id, self.begin_ts).await
                        .map_err(DatabaseError::Storage)? {
                        Some(b) => b,
                        None => continue,
                    };
                    let doc = decode_document(&body_bytes)
                        .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;
                    if let Some(f) = post_filter
                        && !exdb_query::filter_matches(&doc, f) { continue; }
                    returned += 1;
                    rows.push(ScanRow { doc_id, version_ts, doc });
                }
            }
            AccessMethod::TableScan { direction, post_filter, limit, .. } => {
                let sec = sec_index.as_ref().unwrap();
                let mut scanner = sec.scan_at_ts(
                    Bound::Unbounded,
                    Bound::Unbounded,
                    self.begin_ts,
                    *direction,
                );
                let mut returned = 0usize;
                while let Some(result) = scanner.next().await {
                    if let Some(lim) = limit
                        && returned >= *lim { break; }
                    let (doc_id, version_ts) = result.map_err(DatabaseError::Storage)?;
                    let body_bytes = match primary.get_at_ts(&doc_id, self.begin_ts).await
                        .map_err(DatabaseError::Storage)? {
                        Some(b) => b,
                        None => continue,
                    };
                    let doc = decode_document(&body_bytes)
                        .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;
                    if let Some(f) = post_filter
                        && !exdb_query::filter_matches(&doc, f) { continue; }
                    returned += 1;
                    rows.push(ScanRow { doc_id, version_ts, doc });
                }
            }
        }

        Ok((rows, ri))
    }

    /// List all collections (with catalog read tracking).
    pub fn list_collections(&mut self) -> Result<Vec<CollectionMeta>> {
        self.check_timeout()?;
        let query_id = self.read_set.next_query_id();
        CatalogTracker::record_list_collections(&mut self.read_set, query_id);

        let catalog = self.ctx.catalog.read();
        let mut result: Vec<CollectionMeta> = catalog.list_collections().into_iter().cloned().collect();
        drop(catalog);

        for m in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateCollection { name, provisional_id } = m {
                result.push(CollectionMeta {
                    collection_id: *provisional_id,
                    name: name.clone(),
                    primary_root_page: 0,
                    doc_count: 0,
                });
            }
        }
        result.retain(|c| !self.write_set.is_collection_dropped(c.collection_id));
        Ok(result)
    }

    /// List all indexes for a collection (with catalog read tracking).
    pub fn list_indexes(&mut self, collection: &str) -> Result<Vec<IndexMeta>> {
        self.check_timeout()?;
        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;
        CatalogTracker::record_list_indexes(&mut self.read_set, query_id, collection_id);

        let catalog = self.ctx.catalog.read();
        let mut result: Vec<IndexMeta> = catalog.list_indexes(collection_id).into_iter().cloned().collect();
        drop(catalog);

        for m in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateIndex { collection_id: cid, name, field_paths, provisional_id } = m
                && *cid == collection_id {
                    result.push(IndexMeta {
                        index_id: *provisional_id,
                        collection_id: *cid,
                        name: name.clone(),
                        field_paths: field_paths.clone(),
                        root_page: 0,
                        state: IndexState::Ready,
                    });
                }
        }
        Ok(result)
    }

    // ── Write Operations ──

    /// Insert a document. Returns the auto-generated DocId.
    pub async fn insert(
        &mut self,
        collection: &str,
        mut body: serde_json::Value,
    ) -> Result<DocId> {
        self.require_writable()?;
        self.check_timeout()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;

        let doc_id = generate_ulid();
        let id_str = encode_ulid(&doc_id);

        if let Some(obj) = body.as_object_mut() {
            obj.insert("_id".to_string(), serde_json::Value::String(id_str));
            obj.insert(
                "_created_at".to_string(),
                serde_json::Value::Number(serde_json::Number::from(self.wall_clock_ms)),
            );
            obj.remove("_meta");
        }

        let encoded = encode_document(&body);
        if encoded.len() > self.ctx.max_doc_size {
            return Err(DatabaseError::DocTooLarge {
                size: encoded.len(),
                max: self.ctx.max_doc_size,
            });
        }

        self.write_set.insert(collection_id, doc_id, body);
        Ok(doc_id)
    }

    /// Replace a document entirely.
    pub async fn replace(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        mut body: serde_json::Value,
    ) -> Result<()> {
        self.require_writable()?;
        self.check_timeout()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;
        let (current_doc, version_ts) = self.read_current(collection_id, doc_id).await?;

        if let Some(obj) = body.as_object_mut() {
            if let Some(id) = current_doc.get("_id") {
                obj.insert("_id".to_string(), id.clone());
            }
            if let Some(created) = current_doc.get("_created_at") {
                obj.insert("_created_at".to_string(), created.clone());
            }
            obj.remove("_meta");
        }

        let encoded = encode_document(&body);
        if encoded.len() > self.ctx.max_doc_size {
            return Err(DatabaseError::DocTooLarge {
                size: encoded.len(),
                max: self.ctx.max_doc_size,
            });
        }

        self.write_set.replace(collection_id, *doc_id, body, version_ts);
        Ok(())
    }

    /// Patch a document (shallow merge).
    pub async fn patch(
        &mut self,
        collection: &str,
        doc_id: &DocId,
        patch: serde_json::Value,
    ) -> Result<()> {
        self.require_writable()?;
        self.check_timeout()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;
        let (mut current_doc, version_ts) = self.read_current(collection_id, doc_id).await?;

        // Extract _meta.unset before stripping
        let unset_fields: Vec<Vec<String>> = patch
            .get("_meta")
            .and_then(|m| m.get("unset"))
            .and_then(|u| u.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        if let Some(s) = v.as_str() {
                            Some(vec![s.to_string()])
                        } else { v.as_array().map(|arr| arr.iter().filter_map(|s| s.as_str().map(|s| s.to_string())).collect()) }
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Shallow merge
        if let Some(patch_obj) = patch.as_object()
            && let Some(doc_obj) = current_doc.as_object_mut() {
                for (key, value) in patch_obj {
                    if key == "_meta" { continue; }
                    doc_obj.insert(key.clone(), value.clone());
                }
            }

        // Apply _meta.unset
        for path in &unset_fields {
            if path.is_empty() { continue; }
            if path.len() == 1 {
                if let Some(obj) = current_doc.as_object_mut() {
                    obj.remove(&path[0]);
                }
            } else {
                // Navigate to parent and remove the last key
                let parent_path = &path[..path.len() - 1];
                let last_key = &path[path.len() - 1];
                // Use a helper to navigate and remove nested field
                remove_nested_field(&mut current_doc, parent_path, last_key);
            }
        }

        if let Some(obj) = current_doc.as_object_mut() {
            obj.remove("_meta");
        }

        let encoded = encode_document(&current_doc);
        if encoded.len() > self.ctx.max_doc_size {
            return Err(DatabaseError::DocTooLarge {
                size: encoded.len(),
                max: self.ctx.max_doc_size,
            });
        }

        self.write_set.replace(collection_id, *doc_id, current_doc, version_ts);
        Ok(())
    }

    /// Delete a document.
    pub async fn delete(&mut self, collection: &str, doc_id: &DocId) -> Result<()> {
        self.require_writable()?;
        self.check_timeout()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;
        let (_doc, version_ts) = self.read_current(collection_id, doc_id).await?;
        self.write_set.delete(collection_id, *doc_id, version_ts);
        Ok(())
    }

    // ── DDL Operations ──

    pub fn create_collection(&mut self, name: &str) -> Result<()> {
        self.require_writable()?;

        let catalog = self.ctx.catalog.read();
        if catalog.get_collection_by_name(name).is_some() {
            return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
        }
        let collection_id = catalog.next_collection_id();
        let created_at_index_id = catalog.next_index_id();
        drop(catalog);

        if self.write_set.resolve_pending_collection(name).is_some() {
            return Err(DatabaseError::CollectionAlreadyExists(name.to_string()));
        }

        self.write_set.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: name.to_string(),
            provisional_id: collection_id,
        });
        self.write_set.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id,
            name: "_created_at".to_string(),
            field_paths: vec![FieldPath::single("_created_at")],
            provisional_id: created_at_index_id,
        });

        Ok(())
    }

    pub fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.require_writable()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(name, query_id)?;

        let catalog = self.ctx.catalog.read();
        let indexes: Vec<IndexMeta> = catalog.list_indexes(collection_id).into_iter().cloned().collect();
        drop(catalog);

        for idx in &indexes {
            self.write_set.add_catalog_mutation(CatalogMutation::DropIndex {
                collection_id,
                index_id: idx.index_id,
                name: idx.name.clone(),
            });
        }

        self.write_set.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id,
            name: name.to_string(),
        });
        self.dropped_collections.push(collection_id);
        Ok(())
    }

    pub fn create_index(&mut self, collection: &str, name: &str, fields: Vec<FieldPath>) -> Result<()> {
        self.require_writable()?;

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;

        let catalog = self.ctx.catalog.read();
        if catalog.get_index_by_name(collection_id, name).is_some() {
            return Err(DatabaseError::IndexAlreadyExists {
                collection: collection.to_string(),
                index: name.to_string(),
            });
        }
        let index_id = catalog.next_index_id();
        drop(catalog);

        for m in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateIndex { collection_id: cid, name: n, .. } = m
                && *cid == collection_id && n == name {
                    return Err(DatabaseError::IndexAlreadyExists {
                        collection: collection.to_string(),
                        index: name.to_string(),
                    });
                }
        }

        self.write_set.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id,
            name: name.to_string(),
            field_paths: fields,
            provisional_id: index_id,
        });
        Ok(())
    }

    pub fn drop_index(&mut self, collection: &str, name: &str) -> Result<()> {
        self.require_writable()?;

        if name == "_created_at" || name == "_id" {
            return Err(DatabaseError::SystemIndex(name.to_string()));
        }

        let query_id = self.read_set.next_query_id();
        let collection_id = self.resolve_collection(collection, query_id)?;

        let catalog = self.ctx.catalog.read();
        let index_meta = catalog.get_index_by_name(collection_id, name).ok_or_else(|| {
            DatabaseError::IndexNotFound {
                collection: collection.to_string(),
                index: name.to_string(),
            }
        })?;
        let index_id = index_meta.index_id;
        drop(catalog);

        self.write_set.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id,
            index_id,
            name: name.to_string(),
        });
        Ok(())
    }

    // ── Lifecycle ──

    pub async fn commit(mut self) -> Result<TransactionResult> {
        self.committed = true;

        if self.opts.readonly && self.opts.subscription == SubscriptionMode::None {
            return Ok(TransactionResult::Success {
                commit_ts: self.begin_ts,
                subscription_handle: None,
            });
        }

        // Take ownership of read_set and write_set out of self via mem::take
        let read_set = std::mem::take(&mut self.read_set);
        let write_set = std::mem::take(&mut self.write_set);

        if self.opts.readonly {
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            let registry = self.ctx.commit_handle.subscriptions();
            let id = registry.write().register(
                self.opts.subscription,
                self.opts.session_id,
                self.tx_id,
                self.begin_ts,
                read_set,
                tx,
            );
            let handle = SubscriptionHandle::new(id, Arc::clone(registry), rx);
            return Ok(TransactionResult::Success {
                commit_ts: self.begin_ts,
                subscription_handle: Some(handle),
            });
        }

        let request = CommitRequest {
            tx_id: self.tx_id,
            begin_ts: self.begin_ts,
            read_set,
            write_set,
            subscription: self.opts.subscription,
            session_id: self.opts.session_id,
        };

        let result = self.ctx.commit_handle.commit(request).await;

        match result {
            CommitResult::Success { commit_ts, subscription_id, event_rx } => {
                let handle = match (subscription_id, event_rx) {
                    (Some(id), Some(rx)) => {
                        let registry = self.ctx.commit_handle.subscriptions();
                        Some(SubscriptionHandle::new(id, Arc::clone(registry), rx))
                    }
                    _ => None,
                };
                Ok(TransactionResult::Success { commit_ts, subscription_handle: handle })
            }
            CommitResult::Conflict { error, retry } => {
                Ok(TransactionResult::Conflict { error, retry })
            }
            CommitResult::QuorumLost => Ok(TransactionResult::QuorumLost),
        }
    }

    pub fn rollback(mut self) {
        self.committed = true;
    }

    pub fn reset(&mut self) {
        self.read_set = ReadSet::new();
        self.write_set = WriteSet::new();
        self.dropped_collections.clear();
    }

    // ── Internal Helpers ──

    fn resolve_collection(&mut self, name: &str, query_id: QueryId) -> Result<CollectionId> {
        CatalogTracker::record_collection_name_lookup(&mut self.read_set, query_id, name);

        if let Some(id) = self.write_set.resolve_pending_collection(name) {
            if self.write_set.is_collection_dropped(id) {
                return Err(DatabaseError::CollectionDropped);
            }
            return Ok(id);
        }

        let catalog = self.ctx.catalog.read();
        let meta = catalog.get_collection_by_name(name)
            .ok_or_else(|| DatabaseError::CollectionNotFound(name.to_string()))?;
        let id = meta.collection_id;
        drop(catalog);

        if self.write_set.is_collection_dropped(id) {
            return Err(DatabaseError::CollectionDropped);
        }

        Ok(id)
    }

    fn resolve_index(
        &mut self,
        collection_name: &str,
        collection_id: CollectionId,
        name: &str,
        query_id: QueryId,
    ) -> Result<IndexMeta> {
        CatalogTracker::record_index_name_lookup(&mut self.read_set, query_id, collection_id, name);

        for m in &self.write_set.catalog_mutations {
            if let CatalogMutation::CreateIndex { collection_id: cid, name: n, field_paths, provisional_id } = m
                && *cid == collection_id && n == name {
                    return Ok(IndexMeta {
                        index_id: *provisional_id,
                        collection_id: *cid,
                        name: n.clone(),
                        field_paths: field_paths.clone(),
                        root_page: 0,
                        state: IndexState::Ready,
                    });
                }
        }

        let catalog = self.ctx.catalog.read();
        catalog.get_index_by_name(collection_id, name).cloned().ok_or_else(|| {
            DatabaseError::IndexNotFound {
                collection: collection_name.to_string(),
                index: name.to_string(),
            }
        })
    }

    async fn read_current(&self, collection_id: CollectionId, doc_id: &DocId) -> Result<(serde_json::Value, Ts)> {
        if let Some(entry) = self.write_set.get(collection_id, doc_id) {
            return match &entry.op {
                MutationOp::Delete => Err(DatabaseError::DocNotFound),
                MutationOp::Insert | MutationOp::Replace => {
                    let body = entry.body.clone().ok_or(DatabaseError::DocNotFound)?;
                    let ts = entry.previous_ts.unwrap_or(self.begin_ts);
                    Ok((body, ts))
                }
            };
        }

        let primary = {
            let guard = self.ctx.primary_indexes.read();
            match guard.get(&collection_id) {
                Some(p) => Arc::clone(p),
                None => return Err(DatabaseError::DocNotFound),
            }
        };

        let body_bytes = primary.get_at_ts(doc_id, self.begin_ts).await
            .map_err(DatabaseError::Storage)?
            .ok_or(DatabaseError::DocNotFound)?;

        let version_ts = primary.get_version_ts(doc_id, self.begin_ts).await
            .map_err(DatabaseError::Storage)?
            .ok_or(DatabaseError::DocNotFound)?;

        let doc = decode_document(&body_bytes)
            .map_err(|e| DatabaseError::Storage(std::io::Error::other(e)))?;

        Ok((doc, version_ts))
    }

    fn decompose_write_set(&self, collection_id: CollectionId) -> MergeComponents
    {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        let mut replaces = Vec::new();

        for (doc_id, entry) in self.write_set.mutations_for_collection(collection_id) {
            match &entry.op {
                MutationOp::Insert => {
                    if let Some(body) = &entry.body { inserts.push((*doc_id, body.clone())); }
                }
                MutationOp::Delete => { deletes.push(*doc_id); }
                MutationOp::Replace => {
                    if let Some(body) = &entry.body { replaces.push((*doc_id, body.clone())); }
                }
            }
        }
        (inserts, deletes, replaces)
    }

    fn require_writable(&self) -> Result<()> {
        if self.opts.readonly { Err(DatabaseError::ReadonlyWrite) } else { Ok(()) }
    }

    fn check_timeout(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.created_at) > self.ctx.tx_config.max_lifetime {
            return Err(DatabaseError::TransactionTimeout);
        }
        if now.duration_since(self.last_activity) > self.ctx.tx_config.idle_timeout {
            return Err(DatabaseError::TransactionTimeout);
        }
        self.last_activity = now;
        Ok(())
    }

    fn check_read_limits(&self) -> Result<()> {
        let count = self.read_set.interval_count();
        if count > self.ctx.tx_config.max_intervals {
            return Err(DatabaseError::ReadLimitExceeded(format!(
                "intervals: {count} > {}", self.ctx.tx_config.max_intervals
            )));
        }
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Implicit rollback if not committed
    }
}

// ── Types ──

/// Decomposed write set for merge: (inserts, deletes, replaces).
type MergeComponents = (
    Vec<(DocId, serde_json::Value)>,
    Vec<DocId>,
    Vec<(DocId, serde_json::Value)>,
);

// ── Helper Functions ──

fn make_point_read_interval(collection_id: CollectionId, doc_id: &DocId) -> ReadIntervalInfo {
    let mut lower = doc_id.as_bytes().to_vec();
    lower.extend_from_slice(&[0u8; 8]);
    let upper_prefix = exdb_docstore::successor_key(doc_id.as_bytes().as_ref());
    let mut upper = upper_prefix;
    upper.extend_from_slice(&[0u8; 8]);
    ReadIntervalInfo {
        collection_id,
        index_id: IndexId(0),
        lower,
        upper: Bound::Excluded(upper),
    }
}

fn remove_nested_field(doc: &mut serde_json::Value, parent_path: &[String], last_key: &str) {
    let mut current = doc;
    for segment in parent_path {
        match current {
            serde_json::Value::Object(map) => {
                match map.get_mut(segment) {
                    Some(v) => current = v,
                    None => return,
                }
            }
            _ => return,
        }
    }
    if let Some(obj) = current.as_object_mut() {
        obj.remove(last_key);
    }
}

fn bound_as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn compute_read_interval_for_method(method: &AccessMethod) -> ReadIntervalInfo {
    match method {
        AccessMethod::PrimaryGet { collection_id, doc_id } => {
            let mut lower = doc_id.as_bytes().to_vec();
            lower.extend_from_slice(&[0u8; 8]);
            let upper_prefix = exdb_docstore::successor_key(doc_id.as_bytes().as_ref());
            let mut upper = upper_prefix;
            upper.extend_from_slice(&[0u8; 8]);
            ReadIntervalInfo {
                collection_id: *collection_id,
                index_id: IndexId(0),
                lower,
                upper: Bound::Excluded(upper),
            }
        }
        AccessMethod::IndexScan { collection_id, index_id, lower, upper, .. } => ReadIntervalInfo {
            collection_id: *collection_id,
            index_id: *index_id,
            lower: match lower {
                Bound::Included(v) | Bound::Excluded(v) => v.clone(),
                Bound::Unbounded => vec![0x00],
            },
            upper: match upper {
                Bound::Excluded(v) => Bound::Excluded(v.clone()),
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(v) => Bound::Excluded(exdb_docstore::prefix_successor(v)),
            },
        },
        AccessMethod::TableScan { collection_id, index_id, .. } => ReadIntervalInfo {
            collection_id: *collection_id,
            index_id: *index_id,
            lower: vec![0x00],
            upper: Bound::Unbounded,
        },
    }
}

fn compute_limit_boundary(last_row: &ScanRow, index_meta: &IndexMeta, direction: ScanDirection) -> LimitBoundary {
    let scalars: Vec<Scalar> = index_meta.field_paths.iter()
        .map(|f| extract_scalar(&last_row.doc, f).unwrap_or(Scalar::Undefined))
        .collect();
    let prefix = encode_key_prefix(&scalars);
    let full_key = make_secondary_key_from_prefix(&prefix, &last_row.doc_id, last_row.version_ts);

    match direction {
        ScanDirection::Forward => LimitBoundary::Upper(full_key),
        ScanDirection::Backward => LimitBoundary::Lower(full_key),
    }
}
