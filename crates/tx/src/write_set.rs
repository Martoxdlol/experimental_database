use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_core::field_path::FieldPath;
use serde_json::Value;

/// All buffered mutations for a transaction.
#[derive(Debug, Default)]
pub struct WriteSet {
    /// Document mutations, keyed by (collection, doc_id).
    /// BTreeMap ensures deterministic iteration order at commit time.
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
    /// Catalog DDL operations, applied in order at commit time.
    pub catalog_mutations: Vec<CatalogMutation>,
}

/// A single document mutation.
#[derive(Debug, Clone)]
pub struct MutationEntry {
    /// The type of mutation.
    pub op: MutationOp,
    /// The resolved document body. None for Delete.
    pub body: Option<Value>,
    /// The timestamp of the version being replaced.
    /// None for Insert (no previous version).
    /// Some(ts) for Replace/Delete (needed for conflict detection
    /// and for reading the old document's index keys at commit time).
    pub previous_ts: Option<Ts>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOp {
    Insert,
    Replace,
    Delete,
}

/// Catalog DDL operations buffered in the write set.
#[derive(Debug, Clone)]
pub enum CatalogMutation {
    CreateCollection {
        name: String,
        provisional_id: CollectionId,
    },
    DropCollection {
        collection_id: CollectionId,
        name: String,
    },
    CreateIndex {
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        provisional_id: IndexId,
    },
    DropIndex {
        collection_id: CollectionId,
        index_id: IndexId,
        name: String,
    },
}

/// Index delta computed at commit time.
/// One delta per (index, doc_id) pair per mutation.
#[derive(Debug, Clone)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    /// Old encoded key prefix. None for inserts (no previous entry).
    pub old_key: Option<Vec<u8>>,
    /// New encoded key prefix. None for deletes (entry removed).
    pub new_key: Option<Vec<u8>>,
}

/// Metadata about a single index, sufficient for delta computation.
pub struct IndexInfo {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub field_paths: Vec<FieldPath>,
}

/// Trait for looking up index metadata during commit.
/// Defined in L5, implemented by L6 (via CatalogCache).
pub trait IndexResolver: Send + Sync {
    /// List all secondary indexes on a collection.
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo>;
}

impl WriteSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer an insert mutation.
    /// `body` must have `_id` and `_created_at` already set by L6.
    pub fn insert(&mut self, collection_id: CollectionId, doc_id: DocId, body: Value) {
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
        body: Value,
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

    /// Look up a buffered mutation by (collection, doc_id).
    /// Used for read-your-writes within the transaction.
    pub fn get(&self, collection_id: CollectionId, doc_id: &DocId) -> Option<&MutationEntry> {
        self.mutations.get(&(collection_id, *doc_id))
    }

    /// Add a catalog DDL operation.
    pub fn add_catalog_mutation(&mut self, mutation: CatalogMutation) {
        self.catalog_mutations.push(mutation);
    }

    /// Resolve a collection name within pending catalog mutations.
    pub fn resolve_pending_collection(&self, name: &str) -> Option<CollectionId> {
        for m in &self.catalog_mutations {
            if let CatalogMutation::CreateCollection { name: n, provisional_id } = m {
                if n == name {
                    return Some(*provisional_id);
                }
            }
        }
        None
    }

    /// Check if a collection is being dropped in this transaction.
    pub fn is_collection_dropped(&self, collection_id: CollectionId) -> bool {
        self.catalog_mutations.iter().any(|m| {
            matches!(m, CatalogMutation::DropCollection { collection_id: cid, .. } if *cid == collection_id)
        })
    }

    /// Check if the write set is empty (no mutations, no catalog ops).
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.catalog_mutations.is_empty()
    }

    /// Number of document mutations.
    pub fn mutation_count(&self) -> usize {
        self.mutations.len()
    }

    /// Iterate over mutations for a specific collection.
    pub fn mutations_for_collection(
        &self,
        collection_id: CollectionId,
    ) -> impl Iterator<Item = (&DocId, &MutationEntry)> {
        let range_start = (collection_id, DocId([0u8; 16]));
        let range_end = (collection_id, DocId([0xff; 16]));
        self.mutations
            .range(range_start..=range_end)
            .map(|((_, doc_id), entry)| (doc_id, entry))
    }
}

/// Compute index deltas for all mutations in the write set.
///
/// For each mutation:
/// 1. Look up all indexes on the mutation's collection (via IndexResolver).
/// 2. If previous_ts is set: read old document via PrimaryIndex, extract old index keys.
/// 3. If body is set (not delete): extract new index keys.
/// 4. Emit IndexDelta for each index.
pub async fn compute_index_deltas(
    write_set: &WriteSet,
    index_resolver: &dyn IndexResolver,
    primary_indexes: &HashMap<CollectionId, Arc<exdb_docstore::PrimaryIndex>>,
) -> anyhow::Result<Vec<IndexDelta>> {
    let mut deltas = Vec::new();

    for ((collection_id, doc_id), entry) in &write_set.mutations {
        let indexes = index_resolver.indexes_for_collection(*collection_id);

        for index in &indexes {
            // Old keys (if replacing/deleting)
            let old_keys = if let Some(prev_ts) = entry.previous_ts {
                if let Some(primary) = primary_indexes.get(collection_id) {
                    if let Some(body_bytes) = primary.get_at_ts(doc_id, prev_ts).await? {
                        let old_doc: Value = serde_json::from_slice(&body_bytes)?;
                        exdb_docstore::compute_index_entries(&old_doc, &index.field_paths)
                            .unwrap_or_default()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

            // New keys (if inserting/replacing)
            let new_keys = if let Some(body) = &entry.body {
                exdb_docstore::compute_index_entries(body, &index.field_paths)
                    .unwrap_or_default()
            } else {
                vec![]
            };

            // Emit deltas — pair up old and new keys
            // For simple (non-array) indexes: one old_key, one new_key → one delta
            // For array indexes: multiple old_keys and/or new_keys → multiple deltas
            let max_len = old_keys.len().max(new_keys.len()).max(1);
            for i in 0..max_len {
                let old_key = old_keys.get(i).cloned();
                let new_key = new_keys.get(i).cloned();
                if old_key.is_some() || new_key.is_some() {
                    deltas.push(IndexDelta {
                        index_id: index.index_id,
                        collection_id: *collection_id,
                        doc_id: *doc_id,
                        old_key,
                        new_key,
                    });
                }
            }
        }
    }

    Ok(deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use exdb_core::types::DocId;
    use exdb_storage::engine::StorageConfig;

    fn cid(n: u64) -> CollectionId { CollectionId(n) }
    fn iid(n: u64) -> IndexId { IndexId(n) }
    fn make_doc_id(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    #[test]
    fn t3_insert_and_get() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(1);
        ws.insert(cid(1), doc_id, serde_json::json!({"_id": "1", "name": "Alice"}));
        let entry = ws.get(cid(1), &doc_id).unwrap();
        assert_eq!(entry.op, MutationOp::Insert);
        assert!(entry.body.is_some());
        assert!(entry.previous_ts.is_none());
    }

    #[test]
    fn t3_replace_tracks_previous_ts() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(2);
        ws.replace(cid(1), doc_id, serde_json::json!({"name": "Bob"}), 42);
        let entry = ws.get(cid(1), &doc_id).unwrap();
        assert_eq!(entry.op, MutationOp::Replace);
        assert_eq!(entry.previous_ts, Some(42));
    }

    #[test]
    fn t3_delete_no_body() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(3);
        ws.delete(cid(1), doc_id, 10);
        let entry = ws.get(cid(1), &doc_id).unwrap();
        assert_eq!(entry.op, MutationOp::Delete);
        assert!(entry.body.is_none());
        assert_eq!(entry.previous_ts, Some(10));
    }

    #[test]
    fn t3_overwrite_in_same_tx() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(4);
        ws.insert(cid(1), doc_id, serde_json::json!({"name": "Alice"}));
        ws.replace(cid(1), doc_id, serde_json::json!({"name": "Alice Updated"}), 1);
        assert_eq!(ws.mutation_count(), 1);
        let entry = ws.get(cid(1), &doc_id).unwrap();
        assert_eq!(entry.op, MutationOp::Replace);
    }

    #[test]
    fn t3_mutations_for_collection() {
        let mut ws = WriteSet::new();
        ws.insert(cid(1), make_doc_id(1), serde_json::json!({}));
        ws.insert(cid(1), make_doc_id(2), serde_json::json!({}));
        ws.insert(cid(2), make_doc_id(3), serde_json::json!({}));
        let coll1: Vec<_> = ws.mutations_for_collection(cid(1)).collect();
        assert_eq!(coll1.len(), 2);
        let coll2: Vec<_> = ws.mutations_for_collection(cid(2)).collect();
        assert_eq!(coll2.len(), 1);
    }

    #[test]
    fn t3_resolve_pending_collection() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: cid(42),
        });
        assert_eq!(ws.resolve_pending_collection("users"), Some(cid(42)));
        assert_eq!(ws.resolve_pending_collection("orders"), None);
    }

    #[test]
    fn t3_is_collection_dropped() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::DropCollection {
            collection_id: cid(1),
            name: "old".into(),
        });
        assert!(ws.is_collection_dropped(cid(1)));
        assert!(!ws.is_collection_dropped(cid(2)));
    }

    #[test]
    fn t3_catalog_mutations_ordered() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: cid(1),
        });
        ws.add_catalog_mutation(CatalogMutation::CreateIndex {
            collection_id: cid(1),
            name: "by_email".into(),
            field_paths: vec![],
            provisional_id: iid(10),
        });
        assert_eq!(ws.catalog_mutations.len(), 2);
        assert!(matches!(ws.catalog_mutations[0], CatalogMutation::CreateCollection { .. }));
        assert!(matches!(ws.catalog_mutations[1], CatalogMutation::CreateIndex { .. }));
    }

    #[test]
    fn t3_is_empty() {
        let ws = WriteSet::new();
        assert!(ws.is_empty());
    }

    #[test]
    fn t3_not_empty_with_mutations() {
        let mut ws = WriteSet::new();
        ws.insert(cid(1), make_doc_id(1), serde_json::json!({}));
        assert!(!ws.is_empty());
    }

    #[test]
    fn t3_not_empty_with_catalog_only() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "x".into(),
            provisional_id: cid(1),
        });
        assert!(!ws.is_empty());
    }

    // ── Compute index delta tests use in-memory storage ──

    struct MockIndexResolver {
        indexes: Vec<(IndexId, CollectionId, Vec<FieldPath>)>,
    }

    impl IndexResolver for MockIndexResolver {
        fn indexes_for_collection(&self, _: CollectionId) -> Vec<IndexInfo> {
            self.indexes.iter().map(|(idx_id, coll_id, fps)| IndexInfo {
                index_id: *idx_id,
                collection_id: *coll_id,
                field_paths: fps.clone(),
            }).collect()
        }
    }

    async fn make_primary_with_doc(
        doc_id: DocId,
        body: Value,
        ts: Ts,
    ) -> (Arc<exdb_docstore::PrimaryIndex>, Arc<exdb_storage::engine::StorageEngine>) {
        let engine = Arc::new(exdb_storage::engine::StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(exdb_docstore::PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        let body_bytes = serde_json::to_vec(&body).unwrap();
        primary.insert_version(&doc_id, ts, Some(&body_bytes)).await.unwrap();
        (primary, engine)
    }

    #[tokio::test]
    async fn t3_compute_index_deltas_insert() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(1);
        ws.insert(cid(1), doc_id, serde_json::json!({"age": 30}));

        let resolver = MockIndexResolver {
            indexes: vec![(iid(10), cid(1), vec![FieldPath::single("age")])],
        };

        let engine = Arc::new(exdb_storage::engine::StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(exdb_docstore::PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        let primaries = HashMap::from([(cid(1), primary)]);

        let deltas = compute_index_deltas(&ws, &resolver, &primaries).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].old_key.is_none());
        assert!(deltas[0].new_key.is_some());
    }

    #[tokio::test]
    async fn t3_compute_index_deltas_delete() {
        let doc_id = make_doc_id(2);
        let body = serde_json::json!({"age": 25});
        let (primary, engine) = make_primary_with_doc(doc_id, body, 5).await;

        let mut ws = WriteSet::new();
        ws.delete(cid(1), doc_id, 5);

        let resolver = MockIndexResolver {
            indexes: vec![(iid(10), cid(1), vec![FieldPath::single("age")])],
        };
        let primaries = HashMap::from([(cid(1), primary)]);
        let _ = engine; // keep alive

        let deltas = compute_index_deltas(&ws, &resolver, &primaries).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].old_key.is_some());
        assert!(deltas[0].new_key.is_none());
    }

    #[tokio::test]
    async fn t3_compute_index_deltas_replace() {
        let doc_id = make_doc_id(3);
        let old_body = serde_json::json!({"score": 10});
        let (primary, engine) = make_primary_with_doc(doc_id, old_body, 5).await;

        let mut ws = WriteSet::new();
        ws.replace(cid(1), doc_id, serde_json::json!({"score": 20}), 5);

        let resolver = MockIndexResolver {
            indexes: vec![(iid(10), cid(1), vec![FieldPath::single("score")])],
        };
        let primaries = HashMap::from([(cid(1), primary)]);
        let _ = engine;

        let deltas = compute_index_deltas(&ws, &resolver, &primaries).await.unwrap();
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].old_key.is_some());
        assert!(deltas[0].new_key.is_some());
    }

    #[tokio::test]
    async fn t3_compute_index_deltas_array_field() {
        let mut ws = WriteSet::new();
        let doc_id = make_doc_id(4);
        ws.insert(cid(1), doc_id, serde_json::json!({"tags": [1, 2, 3]}));

        let resolver = MockIndexResolver {
            indexes: vec![(iid(10), cid(1), vec![FieldPath::single("tags")])],
        };
        let engine = Arc::new(exdb_storage::engine::StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(exdb_docstore::PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        let primaries = HashMap::from([(cid(1), primary)]);

        let deltas = compute_index_deltas(&ws, &resolver, &primaries).await.unwrap();
        // 3 elements → 3 deltas
        assert_eq!(deltas.len(), 3);
        assert!(deltas.iter().all(|d| d.old_key.is_none() && d.new_key.is_some()));
    }

    #[tokio::test]
    async fn t3_compute_index_deltas_no_indexes() {
        let mut ws = WriteSet::new();
        ws.insert(cid(1), make_doc_id(5), serde_json::json!({"x": 1}));

        let resolver = MockIndexResolver { indexes: vec![] };
        let engine = Arc::new(exdb_storage::engine::StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(exdb_docstore::PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        let primaries = HashMap::from([(cid(1), primary)]);

        let deltas = compute_index_deltas(&ws, &resolver, &primaries).await.unwrap();
        assert!(deltas.is_empty());
    }
}
