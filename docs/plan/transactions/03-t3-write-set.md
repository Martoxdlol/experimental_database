# T3: Write Set — Buffered Mutations + Index Delta Computation

**File:** `crates/tx/src/write_set.rs`
**Depends on:** L1 (`exdb-core`), L3 (`exdb-docstore::compute_index_entries`, `PrimaryIndex`)
**Depended on by:** T7 (`commit.rs`), L4 (`merge.rs` via `MergeView`), L6 (Transaction)
**Defines trait:** `IndexResolver` (implemented by L6's CatalogCache wrapper)

## Purpose

Buffers all document mutations and catalog (DDL) mutations within a transaction until commit. At commit time, computes index deltas for conflict detection and subscription invalidation.

The write set is the transaction's "pending changes" — nothing is visible to other transactions until commit materializes these changes into the page store.

## Data Structures

```rust
use std::collections::BTreeMap;
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
    /// For Insert: full document with _id and _created_at set.
    /// For Replace: full new document body.
    /// For Patch: fully resolved body (patch applied eagerly).
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
/// Applied atomically with document mutations at commit time.
#[derive(Debug, Clone)]
pub enum CatalogMutation {
    CreateCollection {
        name: String,
        /// Allocated eagerly from atomic counter. On abort, simply
        /// never committed (harmless gap in ID sequence).
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
        /// Allocated eagerly, same as collection IDs.
        provisional_id: IndexId,
    },
    DropIndex {
        collection_id: CollectionId,
        index_id: IndexId,
        name: String,
    },
}

/// Index delta computed at commit time (section 5.5.1 of DESIGN.md).
/// One delta per (index, doc_id) pair per mutation.
#[derive(Debug, Clone)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    /// Old encoded key. None for inserts (no previous entry).
    pub old_key: Option<Vec<u8>>,
    /// New encoded key. None for deletes (entry removed).
    pub new_key: Option<Vec<u8>>,
}
```

## API

```rust
impl WriteSet {
    pub fn new() -> Self;

    /// Buffer an insert mutation.
    /// `body` must have `_id` and `_created_at` already set by L6.
    pub fn insert(
        &mut self,
        collection_id: CollectionId,
        doc_id: DocId,
        body: Value,
    );

    /// Buffer a replace mutation.
    /// `body` is the complete new document body.
    /// `previous_ts` is the version being replaced (read during get/query).
    pub fn replace(
        &mut self,
        collection_id: CollectionId,
        doc_id: DocId,
        body: Value,
        previous_ts: Ts,
    );

    /// Buffer a delete mutation.
    /// `previous_ts` is the version being deleted.
    pub fn delete(
        &mut self,
        collection_id: CollectionId,
        doc_id: DocId,
        previous_ts: Ts,
    );

    /// Look up a buffered mutation by (collection, doc_id).
    /// Used for read-your-writes within the transaction.
    pub fn get(
        &self,
        collection_id: CollectionId,
        doc_id: &DocId,
    ) -> Option<&MutationEntry>;

    /// Add a catalog DDL operation.
    pub fn add_catalog_mutation(&mut self, mutation: CatalogMutation);

    /// Resolve a collection name within pending catalog mutations.
    /// Returns the provisional CollectionId if a CreateCollection
    /// is buffered for this name, or None.
    /// Used by L6 for name resolution within the transaction.
    pub fn resolve_pending_collection(&self, name: &str) -> Option<CollectionId>;

    /// Check if a collection is being dropped in this transaction.
    pub fn is_collection_dropped(&self, collection_id: CollectionId) -> bool;

    /// Check if the write set is empty (no mutations, no catalog ops).
    pub fn is_empty(&self) -> bool;

    /// Number of document mutations.
    pub fn mutation_count(&self) -> usize;

    /// Iterate over mutations for a specific collection.
    /// Used by L4 merge to build MergeView.
    pub fn mutations_for_collection(
        &self,
        collection_id: CollectionId,
    ) -> impl Iterator<Item = (&DocId, &MutationEntry)>;
}
```

## Index Resolver Trait

The commit coordinator needs to look up which indexes exist on a collection to compute index deltas. To avoid L5 depending on L6's `CatalogCache` type, L5 defines a trait that L6 implements:

```rust
use exdb_core::field_path::FieldPath;

/// Metadata about a single index, sufficient for delta computation.
pub struct IndexInfo {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub field_paths: Vec<FieldPath>,
}

/// Trait for looking up index metadata during commit.
/// Defined in L5, implemented by L6 (via CatalogCache).
pub trait IndexResolver: Send + Sync {
    /// List all indexes on a collection (including the primary).
    fn indexes_for_collection(&self, collection_id: CollectionId) -> Vec<IndexInfo>;
}
```

## Index Delta Computation

At commit time, the `CommitCoordinator` calls `compute_index_deltas` to transform write set mutations into index deltas. This is a separate async function (not a method on WriteSet) because it needs access to the index resolver and primary indexes for reading old documents:

```rust
/// Compute index deltas for all mutations in the write set.
///
/// For each mutation:
/// 1. Look up all indexes on the mutation's collection (via IndexResolver).
/// 2. If previous_ts is set: read old document via PrimaryIndex, extract old index keys.
/// 3. If body is set (not delete): extract new index keys.
/// 4. Emit IndexDelta for each index.
///
/// For array-indexed fields, one delta per array element.
///
/// This function is async because reading old documents (step 2) requires
/// PrimaryIndex::get_at_ts which is an async I/O operation.
pub async fn compute_index_deltas(
    write_set: &WriteSet,
    index_resolver: &dyn IndexResolver,
    primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
) -> Result<Vec<IndexDelta>>;
```

**Algorithm:**

```
for each (collection_id, doc_id), entry in write_set.mutations:
    indexes = index_resolver.indexes_for_collection(collection_id)
    for each index in indexes:
        // Old keys (if replacing/deleting)
        old_keys = []
        if entry.previous_ts is Some(ts):
            old_doc = primary_indexes[collection_id].get_at_ts(doc_id, ts).await?
            old_keys = compute_index_entries(old_doc, &index.field_paths)

        // New keys (if inserting/replacing)
        new_keys = []
        if entry.body is Some(body):
            new_keys = compute_index_entries(body, &index.field_paths)

        // Emit deltas
        // For simple (non-array) indexes: one old_key, one new_key
        // For array indexes: multiple old_keys and/or new_keys
        emit IndexDelta { index_id, collection_id, doc_id, old_key, new_key }
```

**Note:** `compute_index_deltas` is called within the commit coordinator's single-writer context, so primary index reads are safe (no concurrent modifications during this phase).

**Note:** `compute_index_entries` is from L3 (`exdb_docstore::compute_index_entries`). It computes encoded key prefixes; the caller appends `doc_id || inv_ts` to form the full secondary key via `make_secondary_key_from_prefix`.

## Read-Your-Writes Support

L6's transaction query method uses WriteSet to implement read-your-writes. L4's `merge.rs` receives a decomposed `MergeView` (not the full WriteSet) to avoid L4 depending on L5:

```rust
// In L6, before calling L4 merge:
let merge_view = MergeView {
    inserts: write_set.mutations_for_collection(coll_id)
        .filter(|(_, e)| e.op == MutationOp::Insert)
        .map(|(id, e)| (*id, e.body.clone().unwrap()))
        .collect(),
    deletes: write_set.mutations_for_collection(coll_id)
        .filter(|(_, e)| e.op == MutationOp::Delete)
        .map(|(id, _)| *id)
        .collect(),
    replaces: write_set.mutations_for_collection(coll_id)
        .filter(|(_, e)| e.op == MutationOp::Replace)
        .map(|(id, e)| (*id, e.body.clone().unwrap()))
        .collect(),
};
```

## Tests

```
t3_insert_and_get
    Insert a document. get() returns it.

t3_replace_tracks_previous_ts
    Replace a document with previous_ts=42. get() returns Replace entry with previous_ts=42.

t3_delete_no_body
    Delete a document. get() returns Delete entry with body=None.

t3_overwrite_in_same_tx
    Insert doc_id=X, then replace doc_id=X in same tx. Only one entry exists.

t3_mutations_for_collection
    Insert into collection A and B. mutations_for_collection(A) returns only A's entries.

t3_resolve_pending_collection
    add_catalog_mutation(CreateCollection { name: "users", ... }).
    resolve_pending_collection("users") returns Some(id).
    resolve_pending_collection("orders") returns None.

t3_is_collection_dropped
    add_catalog_mutation(DropCollection { collection_id: 1, ... }).
    is_collection_dropped(1) returns true.
    is_collection_dropped(2) returns false.

t3_catalog_mutations_ordered
    Add CreateCollection then CreateIndex. catalog_mutations preserves order.

t3_compute_index_deltas_insert
    Insert a document. Mock IndexResolver returns one index.
    Verify delta has old_key=None, new_key=Some(...).

t3_compute_index_deltas_delete
    Delete a document with previous_ts. Mock IndexResolver + in-memory PrimaryIndex.
    Verify delta has old_key=Some(...), new_key=None.

t3_compute_index_deltas_replace
    Replace a document. Verify delta has both old_key and new_key.

t3_compute_index_deltas_array_field
    Insert document with array field [1, 2, 3] on an array index.
    Verify 3 deltas emitted (one per element).

t3_compute_index_deltas_no_indexes
    Insert a document into a collection with no secondary indexes.
    IndexResolver returns empty. No deltas emitted.
```
