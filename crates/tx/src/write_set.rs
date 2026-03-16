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
///
/// # Root page IDs
///
/// `CreateCollection` and `CreateIndex` include `root_page` fields that are
/// populated during commit step 3a (pre-allocation), **before** the WAL record
/// is written. This ensures the WAL record contains the exact page IDs that
/// were allocated, enabling deterministic WAL replay during crash recovery.
///
/// # Drop metadata for rollback
///
/// `DropCollection` and `DropIndex` include full original metadata (root pages,
/// field paths, etc.) so that rollback vacuum can reconstruct dropped entries
/// if un-replicated commits need to be reversed after a crash.
#[derive(Debug, Clone)]
pub enum CatalogMutation {
    /// Create a new collection.
    CreateCollection {
        /// Collection name.
        name: String,
        /// Allocated eagerly from atomic counter in L6.
        provisional_id: CollectionId,
        /// Root page of the primary B-tree (allocated at commit step 3a).
        /// Set to 0 when buffered in the transaction; filled in by the commit
        /// coordinator before WAL serialization.
        primary_root_page: u32,
        /// Root page of the `_created_at` secondary index B-tree.
        /// Same lifecycle as `primary_root_page`.
        created_at_root_page: u32,
    },
    /// Drop an existing collection.
    DropCollection {
        /// ID of the collection to drop.
        collection_id: CollectionId,
        /// Collection name (for WAL record + rollback).
        name: String,
        /// Root page of the primary B-tree (for rollback reconstruction).
        primary_root_page: u32,
        /// All indexes that will be cascade-dropped (for rollback reconstruction).
        /// Populated by L6 when buffering the drop, from the CatalogCache.
        dropped_indexes: Vec<DroppedIndexMeta>,
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
        /// Root page of the secondary B-tree (allocated at commit step 3a).
        /// Set to 0 when buffered; filled in by commit coordinator.
        root_page: u32,
    },
    /// Drop an existing secondary index.
    DropIndex {
        /// Collection this index belongs to.
        collection_id: CollectionId,
        /// ID of the index to drop.
        index_id: IndexId,
        /// Index name (for WAL record + rollback).
        name: String,
        /// Fields that were indexed (for rollback reconstruction).
        field_paths: Vec<FieldPath>,
        /// Root page of the secondary B-tree (for rollback reconstruction).
        root_page: u32,
    },
}

/// Metadata for an index being cascade-dropped with its collection.
///
/// Stored in `DropCollection` so rollback vacuum can reconstruct the indexes.
#[derive(Debug, Clone)]
pub struct DroppedIndexMeta {
    /// Index identifier.
    pub index_id: IndexId,
    /// Index name.
    pub name: String,
    /// Fields that were indexed.
    pub field_paths: Vec<FieldPath>,
    /// Root page of the secondary B-tree.
    pub root_page: u32,
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
/// Defined in L5, implemented by L6. The commit coordinator uses this trait
/// in two phases:
///
/// 1. **Step 3a (pre-allocation)**: `allocate_*` methods create B-tree pages
///    and return root page IDs. These IDs are stored in `CatalogMutation`
///    fields before the WAL record is serialized (step 3b).
///
/// 2. **Step 4a (apply)**: `apply_*` methods update catalog B-trees, the
///    in-memory CatalogCache, and register index handles. Called after the
///    WAL record is durably written.
///
/// Methods are `async` because B-tree creation/mutation requires I/O.
///
/// The coordinator calls these in the order they appear in
/// `WriteSet::catalog_mutations`, which is the order L6 buffered them.
/// This means `CreateCollection` is processed before `CreateIndex` for
/// the `_created_at` auto-index, so the primary index exists when the
/// secondary index is created.
#[async_trait::async_trait(?Send)]
pub trait CatalogMutationHandler: Send + Sync {
    /// Pre-allocate B-tree pages for a new collection.
    ///
    /// Creates the primary B-tree and `_created_at` secondary B-tree pages.
    /// Returns `(primary_root_page, created_at_root_page)`.
    ///
    /// Called at step 3a, **before** the WAL record is written.
    /// If crash occurs after this but before WAL write, the allocated pages
    /// are harmless orphans (reclaimed by vacuum or ignored).
    async fn allocate_collection_pages(&self) -> std::io::Result<(u32, u32)>;

    /// Pre-allocate a B-tree page for a new secondary index.
    ///
    /// Returns the `root_page` for the new secondary B-tree.
    ///
    /// Called at step 3a, **before** the WAL record is written.
    async fn allocate_index_page(&self) -> std::io::Result<u32>;

    /// Apply a CreateCollection mutation (step 4a, after WAL persist).
    ///
    /// Must: register PrimaryIndex + SecondaryIndex handles, update catalog
    /// B-trees and CatalogCache. The root pages are already allocated.
    async fn apply_create_collection(
        &self,
        collection_id: CollectionId,
        name: &str,
        primary_root_page: u32,
        created_at_root_page: u32,
    ) -> std::io::Result<()>;

    /// Apply a DropCollection mutation (step 4a, after WAL persist).
    ///
    /// Must: remove PrimaryIndex + all SecondaryIndex handles, update catalog.
    async fn apply_drop_collection(
        &self,
        collection_id: CollectionId,
    ) -> std::io::Result<()>;

    /// Apply a CreateIndex mutation (step 4a, after WAL persist).
    ///
    /// Must: register SecondaryIndex handle, update catalog B-trees and cache.
    /// The root page is already allocated.
    async fn apply_create_index(
        &self,
        index_id: IndexId,
        collection_id: CollectionId,
        name: &str,
        field_paths: &[FieldPath],
        root_page: u32,
    ) -> std::io::Result<()>;

    /// Apply a DropIndex mutation (step 4a, after WAL persist).
    ///
    /// Must: remove SecondaryIndex handle, update catalog.
    async fn apply_drop_index(
        &self,
        index_id: IndexId,
    ) -> std::io::Result<()>;
}

/// No-op catalog mutation handler (for tests without catalog support).
pub struct NoOpCatalogHandler;

#[async_trait::async_trait(?Send)]
impl CatalogMutationHandler for NoOpCatalogHandler {
    async fn allocate_collection_pages(&self) -> std::io::Result<(u32, u32)> {
        Ok((0, 0))
    }
    async fn allocate_index_page(&self) -> std::io::Result<u32> {
        Ok(0)
    }
    async fn apply_create_collection(
        &self, _: CollectionId, _: &str, _: u32, _: u32,
    ) -> std::io::Result<()> {
        Ok(())
    }
    async fn apply_drop_collection(&self, _: CollectionId) -> std::io::Result<()> {
        Ok(())
    }
    async fn apply_create_index(
        &self, _: IndexId, _: CollectionId, _: &str, _: &[FieldPath], _: u32,
    ) -> std::io::Result<()> {
        Ok(())
    }
    async fn apply_drop_index(&self, _: IndexId) -> std::io::Result<()> {
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
                ..
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
            primary_root_page: 0,
            created_at_root_page: 0,
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
            primary_root_page: 0,
            created_at_root_page: 0,
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
            primary_root_page: 0,
            dropped_indexes: vec![],
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

    // ─── CatalogMutation field tests ───

    #[test]
    fn create_collection_has_root_pages() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: CollectionId(1),
            primary_root_page: 10,
            created_at_root_page: 11,
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::CreateCollection {
                primary_root_page,
                created_at_root_page,
                ..
            } => {
                assert_eq!(*primary_root_page, 10);
                assert_eq!(*created_at_root_page, 11);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn create_collection_root_pages_default_zero() {
        // When buffered in a transaction, root pages start at 0.
        // They are filled in by the commit coordinator at step 3a.
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "orders".into(),
            provisional_id: CollectionId(5),
            primary_root_page: 0,
            created_at_root_page: 0,
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::CreateCollection {
                primary_root_page,
                created_at_root_page,
                ..
            } => {
                assert_eq!(*primary_root_page, 0);
                assert_eq!(*created_at_root_page, 0);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn create_index_has_root_page() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "by_email".into(),
            field_paths: vec![FieldPath::single("email")],
            provisional_id: IndexId(10),
            root_page: 42,
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::CreateIndex { root_page, .. } => {
                assert_eq!(*root_page, 42);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn drop_collection_has_full_metadata() {
        let dropped_idx = DroppedIndexMeta {
            index_id: IndexId(20),
            name: "_created_at".into(),
            field_paths: vec![FieldPath::single("_created_at")],
            root_page: 15,
        };
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "users".into(),
            primary_root_page: 10,
            dropped_indexes: vec![dropped_idx],
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::DropCollection {
                primary_root_page,
                dropped_indexes,
                ..
            } => {
                assert_eq!(*primary_root_page, 10);
                assert_eq!(dropped_indexes.len(), 1);
                assert_eq!(dropped_indexes[0].index_id, IndexId(20));
                assert_eq!(dropped_indexes[0].name, "_created_at");
                assert_eq!(dropped_indexes[0].root_page, 15);
                assert_eq!(dropped_indexes[0].field_paths.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn drop_collection_with_multiple_indexes() {
        let idx1 = DroppedIndexMeta {
            index_id: IndexId(20),
            name: "_created_at".into(),
            field_paths: vec![FieldPath::single("_created_at")],
            root_page: 15,
        };
        let idx2 = DroppedIndexMeta {
            index_id: IndexId(21),
            name: "by_email".into(),
            field_paths: vec![FieldPath::single("email")],
            root_page: 16,
        };
        let idx3 = DroppedIndexMeta {
            index_id: IndexId(22),
            name: "by_city_zip".into(),
            field_paths: vec![
                FieldPath::new(vec!["address".into(), "city".into()]),
                FieldPath::new(vec!["address".into(), "zip".into()]),
            ],
            root_page: 17,
        };
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "users".into(),
            primary_root_page: 10,
            dropped_indexes: vec![idx1, idx2, idx3],
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::DropCollection {
                dropped_indexes, ..
            } => {
                assert_eq!(dropped_indexes.len(), 3);
                assert_eq!(dropped_indexes[2].name, "by_city_zip");
                assert_eq!(dropped_indexes[2].field_paths.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn drop_collection_empty_indexes() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "empty_coll".into(),
            primary_root_page: 5,
            dropped_indexes: vec![],
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::DropCollection {
                dropped_indexes, ..
            } => {
                assert!(dropped_indexes.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn drop_index_has_full_metadata() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(10),
            name: "by_age".into(),
            field_paths: vec![FieldPath::single("age")],
            root_page: 30,
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::DropIndex {
                field_paths,
                root_page,
                ..
            } => {
                assert_eq!(*root_page, 30);
                assert_eq!(field_paths.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn drop_index_compound_fields() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: CollectionId(1),
            index_id: IndexId(11),
            name: "by_city_zip".into(),
            field_paths: vec![
                FieldPath::new(vec!["addr".into(), "city".into()]),
                FieldPath::new(vec!["addr".into(), "zip".into()]),
            ],
            root_page: 31,
        });
        match &ws.catalog_mutations[0] {
            CatalogMutation::DropIndex { field_paths, .. } => {
                assert_eq!(field_paths.len(), 2);
                assert_eq!(field_paths[0].segments(), &["addr", "city"]);
                assert_eq!(field_paths[1].segments(), &["addr", "zip"]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn resolve_pending_collection_ignores_root_pages() {
        // resolve_pending_collection should work regardless of root page values
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "test".into(),
            provisional_id: CollectionId(99),
            primary_root_page: 100,
            created_at_root_page: 101,
        });
        assert_eq!(
            ws.resolve_pending_collection("test"),
            Some(CollectionId(99))
        );
    }

    #[test]
    fn is_collection_dropped_with_metadata() {
        // is_collection_dropped should work with the new metadata fields
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: CollectionId(7),
            name: "test".into(),
            primary_root_page: 50,
            dropped_indexes: vec![DroppedIndexMeta {
                index_id: IndexId(1),
                name: "idx".into(),
                field_paths: vec![],
                root_page: 51,
            }],
        });
        assert!(ws.is_collection_dropped(CollectionId(7)));
        assert!(!ws.is_collection_dropped(CollectionId(8)));
    }

    #[test]
    fn dropped_index_meta_clone() {
        let meta = DroppedIndexMeta {
            index_id: IndexId(5),
            name: "by_name".into(),
            field_paths: vec![FieldPath::single("name")],
            root_page: 20,
        };
        let cloned = meta.clone();
        assert_eq!(cloned.index_id, IndexId(5));
        assert_eq!(cloned.name, "by_name");
        assert_eq!(cloned.root_page, 20);
    }

    #[test]
    fn catalog_mutation_clone_preserves_all_fields() {
        let original = CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: CollectionId(42),
            primary_root_page: 10,
            created_at_root_page: 11,
        };
        let cloned = original.clone();
        match cloned {
            CatalogMutation::CreateCollection {
                name,
                provisional_id,
                primary_root_page,
                created_at_root_page,
            } => {
                assert_eq!(name, "users");
                assert_eq!(provisional_id, CollectionId(42));
                assert_eq!(primary_root_page, 10);
                assert_eq!(created_at_root_page, 11);
            }
            _ => panic!("wrong variant after clone"),
        }
    }

    #[test]
    fn multiple_catalog_mutations_ordering() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: CollectionId(1),
            primary_root_page: 10,
            created_at_root_page: 11,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "_created_at".into(),
            field_paths: vec![FieldPath::single("_created_at")],
            provisional_id: IndexId(1),
            root_page: 12,
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: CollectionId(1),
            name: "by_email".into(),
            field_paths: vec![FieldPath::single("email")],
            provisional_id: IndexId(2),
            root_page: 13,
        });
        // Verify ordering is preserved (important for replay)
        assert_eq!(ws.catalog_mutations.len(), 3);
        assert!(matches!(
            &ws.catalog_mutations[0],
            CatalogMutation::CreateCollection { .. }
        ));
        assert!(matches!(
            &ws.catalog_mutations[1],
            CatalogMutation::CreateIndex { name, .. } if name == "_created_at"
        ));
        assert!(matches!(
            &ws.catalog_mutations[2],
            CatalogMutation::CreateIndex { name, .. } if name == "by_email"
        ));
    }
}
