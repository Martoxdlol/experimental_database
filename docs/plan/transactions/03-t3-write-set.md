# T3: Write Set

## Purpose

Buffers document mutations and catalog mutations until commit. Also defines the `IndexResolver` trait used by the commit coordinator to look up index metadata without depending on L6's `CatalogCache` type, and `compute_index_deltas` for transforming the write set into index deltas at commit time.

## Dependencies

- **L1 (`core/types.rs`)**: `CollectionId`, `DocId`, `IndexId`, `Ts`
- **L1 (`core/field_path.rs`)**: `FieldPath`
- **L1 (`core/encoding.rs`)**: `encode_document`, `decode_document`
- **L3 (`primary_index.rs`)**: `PrimaryIndex::get_at_ts` (read old doc for delta computation)
- **L3 (`array_indexing.rs`)**: `compute_index_entries` (compute secondary key prefixes)
- **L3 (`key_encoding.rs`)**: `make_secondary_key_from_prefix`, `make_primary_key`, `inv_ts`

## Rust Types

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_core::field_path::FieldPath;
use exdb_docstore::PrimaryIndex;

/// Mutation operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOp {
    Insert,
    Replace,
    Delete,
}

/// A single buffered mutation entry.
#[derive(Debug, Clone)]
pub struct MutationEntry {
    pub op: MutationOp,
    /// Resolved document body. None for Delete.
    pub body: Option<serde_json::Value>,
    /// Version being replaced. None for Insert (no previous version).
    pub previous_ts: Option<Ts>,
}

/// Catalog DDL operations buffered in the write set.
/// Applied atomically with document mutations at commit time.
#[derive(Debug, Clone)]
pub enum CatalogMutation {
    CreateCollection {
        name: String,
        /// Allocated eagerly from atomic counter in L6.
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
        /// Allocated eagerly from atomic counter in L6.
        provisional_id: IndexId,
    },
    DropIndex {
        collection_id: CollectionId,
        index_id: IndexId,
        name: String,
    },
}

/// Index delta computed at commit time (DESIGN.md 5.5.1).
///
/// Records the old and new encoded secondary key for a single document
/// mutation on a single index. Used by OCC (T5) and subscriptions (T6)
/// for conflict/invalidation detection.
#[derive(Debug, Clone)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    /// Encoded key that was removed. None for inserts.
    pub old_key: Option<Vec<u8>>,
    /// Encoded key that was added. None for deletes.
    pub new_key: Option<Vec<u8>>,
}

/// Minimal index metadata needed for delta computation.
pub struct IndexInfo {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub field_paths: Vec<FieldPath>,
}

/// Trait for looking up index metadata during commit.
///
/// Defined in L5, implemented by L6 (wrapping CatalogCache).
/// Avoids L5 depending on L6's CatalogCache type.
pub trait IndexResolver: Send + Sync {
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo>;
}

/// All buffered mutations for a transaction.
pub struct WriteSet {
    pub mutations: BTreeMap<(CollectionId, DocId), MutationEntry>,
    pub catalog_mutations: Vec<CatalogMutation>,
}
```

## Implementation Details

### WriteSet methods

```rust
impl WriteSet {
    pub fn new() -> Self {
        Self {
            mutations: BTreeMap::new(),
            catalog_mutations: Vec::new(),
        }
    }

    pub fn insert(&mut self, collection_id: CollectionId, doc_id: DocId,
                  body: serde_json::Value) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry { op: MutationOp::Insert, body: Some(body), previous_ts: None },
        );
    }

    pub fn replace(&mut self, collection_id: CollectionId, doc_id: DocId,
                   body: serde_json::Value, previous_ts: Ts) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry { op: MutationOp::Replace, body: Some(body), previous_ts: Some(previous_ts) },
        );
    }

    pub fn delete(&mut self, collection_id: CollectionId, doc_id: DocId,
                  previous_ts: Ts) {
        self.mutations.insert(
            (collection_id, doc_id),
            MutationEntry { op: MutationOp::Delete, body: None, previous_ts: Some(previous_ts) },
        );
    }

    /// Look up a mutation for read-your-writes.
    pub fn get(&self, collection_id: CollectionId, doc_id: &DocId) -> Option<&MutationEntry> {
        self.mutations.get(&(collection_id, *doc_id))
    }

    pub fn add_catalog_mutation(&mut self, mutation: CatalogMutation) {
        self.catalog_mutations.push(mutation);
    }

    /// Resolve a pending collection name to its provisional CollectionId.
    pub fn resolve_pending_collection(&self, name: &str) -> Option<CollectionId> {
        self.catalog_mutations.iter().find_map(|m| match m {
            CatalogMutation::CreateCollection { name: n, provisional_id } if n == name => {
                Some(*provisional_id)
            }
            _ => None,
        })
    }

    /// Check if a collection is marked for drop in this transaction.
    pub fn is_collection_dropped(&self, collection_id: CollectionId) -> bool {
        self.catalog_mutations.iter().any(|m| matches!(m,
            CatalogMutation::DropCollection { collection_id: id, .. } if *id == collection_id
        ))
    }

    /// True when BOTH mutations and catalog_mutations are empty.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.catalog_mutations.is_empty()
    }

    /// Iterate mutations for a specific collection.
    pub fn mutations_for_collection(&self, collection_id: CollectionId)
        -> impl Iterator<Item = (&DocId, &MutationEntry)> + '_
    {
        self.mutations
            .range((collection_id, DocId::MIN)..=(collection_id, DocId::MAX))
            .map(|((_, doc_id), entry)| (doc_id, entry))
    }
}
```

Note: `DocId::MIN` and `DocId::MAX` — DocId is a `[u8; 16]` wrapper, so MIN = `[0u8; 16]` and MAX = `[0xFF; 16]`. If these constants don't exist on DocId, use range filtering via `filter()` instead.

### compute_index_deltas()

```rust
/// Compute index deltas for all mutations in the write set.
///
/// For each (collection_id, doc_id) mutation:
///   - Insert: old_key = None, new_key = computed from new body
///   - Delete: old_key = computed from old body (read via PrimaryIndex), new_key = None
///   - Replace: old_key from old body, new_key from new body
///
/// For each index on the collection (via IndexResolver):
///   - compute_index_entries(doc, field_paths) → Vec<key_prefix>
///   - make_secondary_key_from_prefix(prefix, doc_id, commit_ts) → full key
///
/// Async because reading old documents for Replace/Delete requires
/// PrimaryIndex::get_at_ts (async I/O).
pub async fn compute_index_deltas(
    write_set: &WriteSet,
    index_resolver: &dyn IndexResolver,
    primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
    commit_ts: Ts,
) -> std::io::Result<Vec<IndexDelta>>
```

**Algorithm:**

```
for each (collection_id, doc_id), entry in write_set.mutations:
    let indexes = index_resolver.indexes_for_collection(collection_id)

    // Compute old keys (for Replace/Delete)
    let old_doc: Option<Value> = match entry.op {
        Insert => None,
        Replace | Delete => {
            let previous_ts = entry.previous_ts.unwrap();
            let primary = primary_indexes[&collection_id];
            let raw = primary.get_at_ts(&doc_id, previous_ts).await?;
            raw.map(|bytes| decode_document(&bytes))
        }
    };

    // Compute new keys (for Insert/Replace)
    let new_doc: Option<&Value> = entry.body.as_ref();

    for index_info in &indexes:
        let old_prefixes = old_doc.as_ref()
            .map(|doc| compute_index_entries(doc, &index_info.field_paths))
            .unwrap_or_default();
        let new_prefixes = new_doc
            .map(|doc| compute_index_entries(doc, &index_info.field_paths))
            .unwrap_or_default();

        // For simple case (single old key, single new key):
        let old_key = old_prefixes.first()
            .map(|p| make_secondary_key_from_prefix(p, &doc_id, commit_ts));
        let new_key = new_prefixes.first()
            .map(|p| make_secondary_key_from_prefix(p, &doc_id, commit_ts));

        // For array fields: produce one delta per (old_prefix, new_prefix) pair
        // Simple version: emit all old keys as removals, all new keys as additions
        for prefix in &old_prefixes:
            deltas.push(IndexDelta {
                index_id: index_info.index_id,
                collection_id,
                doc_id,
                old_key: Some(make_secondary_key_from_prefix(prefix, &doc_id, commit_ts)),
                new_key: None,
            });
        for prefix in &new_prefixes:
            deltas.push(IndexDelta {
                index_id: index_info.index_id,
                collection_id,
                doc_id,
                old_key: None,
                new_key: Some(make_secondary_key_from_prefix(prefix, &doc_id, commit_ts)),
            });
```

**Note on timestamps in delta keys:** The `commit_ts` in the delta key is used for the `inv_ts` suffix. For `old_key`, we use the `previous_ts` (the version being replaced), not `commit_ts`. For `new_key`, we use `commit_ts`. This ensures the delta keys match the actual B-tree entries.

**Correction:** Old keys use `previous_ts` for the inv_ts suffix:
```rust
old_key: Some(make_secondary_key_from_prefix(prefix, &doc_id, previous_ts))
new_key: Some(make_secondary_key_from_prefix(prefix, &doc_id, commit_ts))
```

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| I/O error | PrimaryIndex::get_at_ts fails | Propagated from compute_index_deltas |
| Array index error | Multiple array fields in compound index | compute_index_entries returns Err — propagate as io::Error |

## Tests

1. **insert_and_get**: Insert a document, get returns it.
2. **replace_preserves_previous_ts**: Replace records previous_ts.
3. **delete_records_previous_ts**: Delete records previous_ts, body is None.
4. **get_missing**: Get non-existent mutation returns None.
5. **is_empty_initially**: New WriteSet is empty.
6. **is_empty_after_insert**: Not empty after insert.
7. **mutations_for_collection**: Returns only mutations for the specified collection.
8. **catalog_mutation_create_collection**: Add and resolve pending collection.
9. **catalog_mutation_drop_collection**: is_collection_dropped returns true.
10. **resolve_pending_collection_not_found**: Returns None for unknown name.
11. **compute_index_deltas_insert**: No old_key, has new_key.
12. **compute_index_deltas_delete**: Has old_key, no new_key.
13. **compute_index_deltas_replace**: Both old_key and new_key present.
14. **compute_index_deltas_no_indexes**: Empty result when collection has no indexes.
