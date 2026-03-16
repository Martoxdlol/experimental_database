//! T3: Write Set — buffers document and catalog mutations until commit.
//!
//! Also defines the [`IndexResolver`] trait (implemented by L6) and
//! [`compute_index_deltas`] for transforming the write set into index deltas
//! at commit time.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use exdb_core::encoding::decode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_docstore::{compute_index_entries, make_secondary_key_from_prefix, PrimaryIndex};

/// Mutation operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOp {
    /// New document (no previous version).
    Insert,
    /// Update existing document (replaces previous version).
    Replace,
    /// Remove document (creates a tombstone).
    Delete,
}

/// A single buffered mutation entry.
#[derive(Debug, Clone)]
pub struct MutationEntry {
    /// What kind of mutation.
    pub op: MutationOp,
    /// Resolved document body. `None` for [`Delete`](MutationOp::Delete).
    pub body: Option<serde_json::Value>,
    /// Timestamp of the version being replaced. `None` for [`Insert`](MutationOp::Insert).
    pub previous_ts: Option<Ts>,
}

/// Catalog DDL operations buffered in the write set.
///
/// Applied atomically with document mutations at commit time.
#[derive(Debug, Clone)]
pub enum CatalogMutation {
    /// Create a new collection.
    CreateCollection {
        /// Collection name.
        name: String,
        /// Allocated eagerly from atomic counter in L6.
        provisional_id: CollectionId,
    },
    /// Drop an existing collection.
    DropCollection {
        /// ID of the collection to drop.
        collection_id: CollectionId,
        /// Collection name (for WAL record).
        name: String,
    },
    /// Create a new secondary index.
    CreateIndex {
        /// Collection this index belongs to.
        collection_id: CollectionId,
        /// Index name.
        name: String,
        /// Fields to index.
        field_paths: Vec<FieldPath>,
        /// Allocated eagerly from atomic counter in L6.
        provisional_id: IndexId,
    },
    /// Drop an existing secondary index.
    DropIndex {
        /// Collection this index belongs to.
        collection_id: CollectionId,
        /// ID of the index to drop.
        index_id: IndexId,
        /// Index name (for WAL record).
        name: String,
    },
}

/// Index delta computed at commit time (DESIGN.md 5.5.1).
///
/// Records the old and new encoded secondary key for a single document mutation
/// on a single index. Used by OCC (T5) and subscriptions (T6) for
/// conflict/invalidation detection, and by the commit protocol (T7) for applying
/// secondary index mutations.
#[derive(Debug, Clone)]
pub struct IndexDelta {
    /// Which index was affected.
    pub index_id: IndexId,
    /// Which collection.
    pub collection_id: CollectionId,
    /// Which document.
    pub doc_id: DocId,
    /// Encoded key that was removed. `None` for inserts.
    pub old_key: Option<Vec<u8>>,
    /// Encoded key that was added. `None` for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// Minimal index metadata needed for delta computation.
pub struct IndexInfo {
    /// Index identifier.
    pub index_id: IndexId,
    /// Collection this index belongs to.
    pub collection_id: CollectionId,
    /// Fields covered by this index.
    pub field_paths: Vec<FieldPath>,
}

/// Trait for looking up index metadata during commit.
///
/// Defined in L5, implemented by L6 (wrapping `CatalogCache`). Avoids L5
/// depending on L6's `CatalogCache` type.
pub trait IndexResolver: Send + Sync {
    /// Return all secondary indexes for `collection_id`.
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo>;
}

/// Trait for handling catalog DDL mutations during commit.
///
/// Defined in L5, implemented by L6. Called by the `CommitCoordinator`
/// during step 4a (after WAL persist, before data mutations) to create
/// B-trees, update the catalog cache, and register index handles.
///
/// Methods are `async` because B-tree creation requires I/O.
///
/// The coordinator calls these in the order they appear in
/// `WriteSet::catalog_mutations`, which is the order L6 buffered them.
/// This means `CreateCollection` is processed before `CreateIndex` for
/// the `_created_at` auto-index, so the primary index exists when the
/// secondary index is created.
#[async_trait::async_trait]
pub trait CatalogMutationHandler: Send + Sync {
    /// Handle a CreateCollection mutation.
    ///
    /// Must: create primary B-tree, register PrimaryIndex handle, update catalog.
    async fn handle_create_collection(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> std::io::Result<()>;

    /// Handle a DropCollection mutation.
    ///
    /// Must: remove PrimaryIndex + all SecondaryIndex handles, update catalog.
    async fn handle_drop_collection(
        &self,
        collection_id: CollectionId,
    ) -> std::io::Result<()>;

    /// Handle a CreateIndex mutation.
    ///
    /// Must: create secondary B-tree, register SecondaryIndex handle, update catalog.
    async fn handle_create_index(
        &self,
        index_id: IndexId,
        collection_id: CollectionId,
        name: &str,
        field_paths: &[FieldPath],
    ) -> std::io::Result<()>;

    /// Handle a DropIndex mutation.
    ///
    /// Must: remove SecondaryIndex handle, update catalog.
    async fn handle_drop_index(
        &self,
        index_id: IndexId,
    ) -> std::io::Result<()>;
}

/// No-op catalog mutation handler (for tests without catalog support).
pub struct NoOpCatalogHandler;

#[async_trait::async_trait]
impl CatalogMutationHandler for NoOpCatalogHandler {
    async fn handle_create_collection(&self, _: CollectionId, _: &str) -> std::io::Result<()> {
        Ok(())
    }
    async fn handle_drop_collection(&self, _: CollectionId) -> std::io::Result<()> {
        Ok(())
    }
    async fn handle_create_index(
        &self, _: IndexId, _: CollectionId, _: &str, _: &[FieldPath],
    ) -> std::io::Result<()> {
        Ok(())
    }
    async fn handle_drop_index(&self, _: IndexId) -> std::io::Result<()> {
        Ok(())
    }
}

/// All buffered mutations for a transaction.
pub struct WriteSet {
    /// Document mutations keyed by `(collection_id, doc_id)`.
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
    /// Catalog DDL mutations, applied in order.
    pub catalog_mutations: Vec<CatalogMutation>,
}

impl WriteSet {
    /// Create a new empty write set.
    pub fn new() -> Self {
        Self {
            mutations: BTreeMap::new(),
            catalog_mutations: Vec::new(),
        }
    }

    /// Buffer an insert mutation.
    pub fn insert(
        &mut self,
        collection_id: CollectionId,
        doc_id: DocId,
        body: serde_json::Value,
    ) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry {
                op: MutationOp::Insert,
                body: Some(body),
                previous_ts: None,
            },
        );
    }

    /// Buffer a replace mutation.
    pub fn replace(
        &mut self,
        collection_id: CollectionId,
        doc_id: DocId,
        body: serde_json::Value,
        previous_ts: Ts,
    ) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry {
                op: MutationOp::Replace,
                body: Some(body),
                previous_ts: Some(previous_ts),
            },
        );
    }

    /// Buffer a delete mutation.
    pub fn delete(&mut self, collection_id: CollectionId, doc_id: DocId, previous_ts: Ts) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry {
                op: MutationOp::Delete,
                body: None,
                previous_ts: Some(previous_ts),
            },
        );
    }

    /// Look up a mutation for read-your-writes.
    pub fn get(&self, collection_id: CollectionId, doc_id: &DocId) -> Option<&MutationEntry> {
        self.mutations.get(&(collection_id, *doc_id))
    }

    /// Add a catalog DDL mutation.
    pub fn add_catalog_mutation(&mut self, mutation: CatalogMutation) {
        self.catalog_mutations.push(mutation);
    }

    /// Resolve a pending collection name to its provisional [`CollectionId`].
    pub fn resolve_pending_collection(&self, name: &str) -> Option<CollectionId> {
        self.catalog_mutations.iter().find_map(|m| match m {
            CatalogMutation::CreateCollection {
                name: n,
                provisional_id,
            } if n == name => Some(*provisional_id),
            _ => None,
        })
    }

    /// Check if a collection is marked for drop in this transaction.
    pub fn is_collection_dropped(&self, collection_id: CollectionId) -> bool {
        self.catalog_mutations.iter().any(|m| {
            matches!(
                m,
                CatalogMutation::DropCollection { collection_id: id, .. } if *id == collection_id
            )
        })
    }

    /// `true` when both `mutations` and `catalog_mutations` are empty.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.catalog_mutations.is_empty()
    }

    /// Iterate mutations for a specific collection.
    pub fn mutations_for_collection(
        &self,
        collection_id: CollectionId,
    ) -> impl Iterator<Item = (&DocId, &MutationEntry)> + '_ {
        self.mutations
            .range((collection_id, DocId([0u8; 16]))..=(collection_id, DocId([0xFF; 16])))
            .map(|((_, doc_id), entry)| (doc_id, entry))
    }
}

impl Default for WriteSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute index deltas for all mutations in the write set.
///
/// For each `(collection_id, doc_id)` mutation:
/// - **Insert**: `old_key = None`, `new_key` computed from new body
/// - **Delete**: `old_key` computed from old body (via [`PrimaryIndex::get_at_ts`]), `new_key = None`
/// - **Replace**: `old_key` from old body, `new_key` from new body
///
/// Async because reading old documents requires `PrimaryIndex::get_at_ts`.
///
/// **Timestamp in delta keys**: `old_key` uses `previous_ts` (the version being
/// replaced), `new_key` uses `commit_ts`.
pub async fn compute_index_deltas(
    write_set: &WriteSet,
    index_resolver: &dyn IndexResolver,
    primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
    commit_ts: Ts,
) -> std::io::Result<Vec<IndexDelta>> {
    let mut deltas = Vec::new();

    for (&(collection_id, doc_id), entry) in &write_set.mutations {
        let indexes = index_resolver.indexes_for_collection(collection_id);
        if indexes.is_empty() {
            continue;
        }

        // Read old document for Replace/Delete
        let old_doc: Option<serde_json::Value> = match entry.op {
            MutationOp::Insert => None,
            MutationOp::Replace | MutationOp::Delete => {
                let previous_ts = entry.previous_ts.expect("Replace/Delete must have previous_ts");
                if let Some(primary) = primary_indexes.get(&collection_id) {
                    match primary.get_at_ts(&doc_id, previous_ts).await? {
                        Some(bytes) => Some(decode_document(&bytes).map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                        })?),
                        None => None,
                    }
                } else {
                    None
                }
            }
        };

        let new_doc: Option<&serde_json::Value> = entry.body.as_ref();
        let previous_ts = entry.previous_ts.unwrap_or(0);

        for index_info in &indexes {
            let old_prefixes = old_doc
                .as_ref()
                .map(|doc| compute_index_entries(doc, &index_info.field_paths))
                .transpose()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                .unwrap_or_default();

            let new_prefixes = new_doc
                .map(|doc| compute_index_entries(doc, &index_info.field_paths))
                .transpose()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                .unwrap_or_default();

            // Emit removals for old keys
            for prefix in &old_prefixes {
                deltas.push(IndexDelta {
                    index_id: index_info.index_id,
                    collection_id,
                    doc_id,
                    old_key: Some(make_secondary_key_from_prefix(
                        prefix,
                        &doc_id,
                        previous_ts,
                    )),
                    new_key: None,
                });
            }

            // Emit additions for new keys
            for prefix in &new_prefixes {
                deltas.push(IndexDelta {
                    index_id: index_info.index_id,
                    collection_id,
                    doc_id,
                    old_key: None,
                    new_key: Some(make_secondary_key_from_prefix(prefix, &doc_id, commit_ts)),
                });
            }
        }
    }

    Ok(deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn insert_and_get() {
        let mut ws = WriteSet::new();
        let coll = CollectionId(1);
        let doc = DocId([1; 16]);
        ws.insert(coll, doc, json!({"name": "Alice"}));
        let entry = ws.get(coll, &doc).unwrap();
        assert_eq!(entry.op, MutationOp::Insert);
        assert!(entry.body.is_some());
        assert!(entry.previous_ts.is_none());
    }

    #[test]
    fn replace_preserves_previous_ts() {
        let mut ws = WriteSet::new();
        let coll = CollectionId(1);
        let doc = DocId([1; 16]);
        ws.replace(coll, doc, json!({"name": "Bob"}), 42);
        let entry = ws.get(coll, &doc).unwrap();
        assert_eq!(entry.op, MutationOp::Replace);
        assert_eq!(entry.previous_ts, Some(42));
    }

    #[test]
    fn delete_records_previous_ts() {
        let mut ws = WriteSet::new();
        let coll = CollectionId(1);
        let doc = DocId([1; 16]);
        ws.delete(coll, doc, 10);
        let entry = ws.get(coll, &doc).unwrap();
        assert_eq!(entry.op, MutationOp::Delete);
        assert!(entry.body.is_none());
        assert_eq!(entry.previous_ts, Some(10));
    }

    #[test]
    fn get_missing() {
        let ws = WriteSet::new();
        assert!(ws.get(CollectionId(1), &DocId([1; 16])).is_none());
    }

    #[test]
    fn is_empty_initially() {
        let ws = WriteSet::new();
        assert!(ws.is_empty());
    }

    #[test]
    fn is_empty_after_insert() {
        let mut ws = WriteSet::new();
        ws.insert(CollectionId(1), DocId([1; 16]), json!({}));
        assert!(!ws.is_empty());
    }

    #[test]
    fn is_empty_with_catalog_only() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "test".into(),
            provisional_id: CollectionId(100),
        });
        assert!(!ws.is_empty());
    }

    #[test]
    fn mutations_for_collection() {
        let mut ws = WriteSet::new();
        ws.insert(CollectionId(1), DocId([1; 16]), json!({"a": 1}));
        ws.insert(CollectionId(1), DocId([2; 16]), json!({"a": 2}));
        ws.insert(CollectionId(2), DocId([3; 16]), json!({"a": 3}));

        let coll1: Vec<_> = ws.mutations_for_collection(CollectionId(1)).collect();
        assert_eq!(coll1.len(), 2);

        let coll2: Vec<_> = ws.mutations_for_collection(CollectionId(2)).collect();
        assert_eq!(coll2.len(), 1);

        let coll3: Vec<_> = ws.mutations_for_collection(CollectionId(99)).collect();
        assert_eq!(coll3.len(), 0);
    }

    #[test]
    fn catalog_mutation_create_collection() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: CollectionId(42),
        });
        assert_eq!(
            ws.resolve_pending_collection("users"),
            Some(CollectionId(42))
        );
    }

    #[test]
    fn catalog_mutation_drop_collection() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(5),
            name: "users".into(),
        });
        assert!(ws.is_collection_dropped(CollectionId(5)));
        assert!(!ws.is_collection_dropped(CollectionId(6)));
    }

    #[test]
    fn resolve_pending_collection_not_found() {
        let ws = WriteSet::new();
        assert!(ws.resolve_pending_collection("nope").is_none());
    }

    #[test]
    fn overwrite_mutation() {
        let mut ws = WriteSet::new();
        let coll = CollectionId(1);
        let doc = DocId([1; 16]);
        ws.insert(coll, doc, json!({"v": 1}));
        ws.replace(coll, doc, json!({"v": 2}), 5);
        let entry = ws.get(coll, &doc).unwrap();
        assert_eq!(entry.op, MutationOp::Replace);
    }
}
