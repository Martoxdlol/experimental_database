# exdb-docstore

Layer 3 of the exdb database. Adds MVCC document semantics on top of raw B-tree storage.

This crate takes the raw B-trees and heap from `exdb-storage` (which knows nothing about
documents, timestamps, or versions) and turns them into a versioned document store with
secondary indexes. It is the bridge between "bytes in a B-tree" and "documents with history".

## What this crate does

Every document mutation (insert, update, delete) creates a new **version** keyed by
`(doc_id, timestamp)`. Old versions are kept until vacuumed. Reading at any timestamp
returns the document as it existed at that point in time — this is the foundation of
snapshot isolation.

The crate provides:

- **Primary index** — a clustered B-tree where each document lives, keyed by `doc_id || inverted_timestamp`
- **Secondary indexes** — separate B-trees that map field values back to document IDs, with stale-entry detection
- **Key encoding** — order-preserving byte encoding so B-tree memcmp ordering matches logical value ordering
- **Version resolution** — a state machine that filters a B-tree scan down to "the one visible version per document"
- **Array indexing** — expanding a single document into multiple index entries when a field is an array
- **Index builder** — background population of a new secondary index by scanning the primary
- **Vacuum** — removing old versions that no reader can ever see again

## What this crate does NOT do

It is a dumb versioned key-value store. It does not enforce:

- "Document must exist before you can update it" — that is the transaction layer's job (L5)
- Query planning or filter evaluation — that is the query engine (L4)
- Conflict detection between concurrent writers — that is OCC in L5
- Catalog management (which collections/indexes exist) — that is L6

## How it fits in the stack

```
L6  Database        ← composes everything, manages catalog
L5  Transactions    ← OCC, read/write sets, commit protocol
L4  Query Engine    ← plans and executes queries
L3  Document Store  ← this crate
L2  Storage Engine  ← raw B-trees, heap, WAL, buffer pool
L1  Core Types      ← DocId, Scalar, FieldPath, encoding
```

L3 depends on L1 and L2. It exposes building blocks that L4, L5, and L6 compose.

## Usage

### Setting up

Every index needs a `StorageEngine` (from L2) to allocate B-trees and heap storage:

```rust
use std::sync::Arc;
use exdb_storage::engine::{StorageEngine, StorageConfig};
use exdb_docstore::PrimaryIndex;

// In-memory engine (for tests or ephemeral databases)
let engine = Arc::new(
    StorageEngine::open_in_memory(StorageConfig::default()).unwrap()
);

// Create a primary index for a collection
let btree = engine.create_btree().unwrap();
let primary = PrimaryIndex::new(
    btree,
    engine.clone(),
    4096,  // external_threshold: docs larger than this go to the heap
);
```

### Inserting and reading documents

Documents are opaque byte slices at this layer. Higher layers encode JSON/BSON into bytes
before passing them here.

```rust
use exdb_core::types::DocId;

let doc_id = DocId([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

// Insert a document at timestamp 1
primary.insert_version(&doc_id, 1, Some(b"{\"name\": \"Alice\"}")).unwrap();

// Update it at timestamp 5
primary.insert_version(&doc_id, 5, Some(b"{\"name\": \"Bob\"}")).unwrap();

// Read at different points in time
let v = primary.get_at_ts(&doc_id, 3).unwrap().unwrap();
assert_eq!(v, b"{\"name\": \"Alice\"}");  // sees ts=1 version

let v = primary.get_at_ts(&doc_id, 10).unwrap().unwrap();
assert_eq!(v, b"{\"name\": \"Bob\"}");    // sees ts=5 version

// Before any version exists → None
assert!(primary.get_at_ts(&doc_id, 0).unwrap().is_none());
```

### Deleting documents (tombstones)

A delete is just an insert with `body = None`. The tombstone is a version like any other —
readers at timestamps before the delete still see the document.

```rust
// Delete at timestamp 10
primary.insert_version(&doc_id, 10, None).unwrap();

// After the delete → None
assert!(primary.get_at_ts(&doc_id, 15).unwrap().is_none());

// Before the delete → still visible
assert!(primary.get_at_ts(&doc_id, 7).unwrap().is_some());
```

### Scanning all visible documents

`scan_at_ts` returns an iterator over all visible, non-tombstoned documents at a given
timestamp. The iterator handles version resolution internally — if a document has 10
versions, only the one visible at `read_ts` is yielded.

```rust
use exdb_storage::btree::ScanDirection;

for result in primary.scan_at_ts(100, ScanDirection::Forward) {
    let (doc_id, version_ts, body) = result.unwrap();
    // doc_id:     which document
    // version_ts: the timestamp of the visible version
    // body:       the document bytes
}
```

### Secondary indexes

A secondary index maps field values to document IDs. It is a separate B-tree where:
- **Keys** encode the field value + doc_id + timestamp
- **Values** are empty (the document body lives in the primary index)

```rust
use exdb_docstore::{SecondaryIndex, make_secondary_key, encode_key_prefix, successor_key};
use exdb_core::types::Scalar;
use std::ops::Bound;

let primary = Arc::new(primary);  // secondary needs a shared reference
let sec_btree = engine.create_btree().unwrap();
let secondary = SecondaryIndex::new(sec_btree, primary.clone());

// When committing a document with name="Alice", insert an index entry:
let key = make_secondary_key(
    &[Scalar::String("Alice".into())],
    &doc_id,
    5,  // the commit timestamp
);
secondary.insert_entry(&key).unwrap();

// Scan for all documents where name="Alice":
let prefix = encode_key_prefix(&[Scalar::String("Alice".into())]);
let upper = successor_key(&prefix);

let results: Vec<_> = secondary
    .scan_at_ts(
        Bound::Included(prefix.as_slice()),
        Bound::Excluded(upper.as_slice()),
        100,  // read_ts
        ScanDirection::Forward,
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

// Each result is (doc_id, version_ts) — fetch the body from the primary index
for (doc_id, version_ts) in &results {
    let body = primary.get_at_ts(doc_id, *version_ts).unwrap();
}
```

**Stale entry detection**: when a document's field value changes, the old secondary entry
is not immediately removed (append-only). Instead, `scan_at_ts` verifies each candidate
against the primary index: "is this really the latest version of this doc?" If the primary
says the latest version has a different timestamp, the secondary entry is skipped.

### Array indexing

When a field contains an array, one index entry is created per element:

```rust
use exdb_docstore::compute_index_entries;
use exdb_core::field_path::FieldPath;

let doc = serde_json::json!({"tags": ["rust", "database", "mvcc"]});
let prefixes = compute_index_entries(&doc, &[FieldPath::single("tags")]).unwrap();
assert_eq!(prefixes.len(), 3);  // one entry per tag

// Compound indexes with one array field are supported:
let doc = serde_json::json!({"status": "active", "tags": ["rust", "db"]});
let prefixes = compute_index_entries(
    &doc,
    &[FieldPath::single("status"), FieldPath::single("tags")],
).unwrap();
assert_eq!(prefixes.len(), 2);  // ("active","rust") and ("active","db")

// Two array fields in a compound index → error
let doc = serde_json::json!({"a": [1, 2], "b": [3, 4]});
assert!(compute_index_entries(
    &doc,
    &[FieldPath::single("a"), FieldPath::single("b")],
).is_err());
```

### Building an index in the background

When a new secondary index is created on a collection that already has data, the
`IndexBuilder` scans the primary index at a snapshot timestamp and populates the
secondary index. It yields to the tokio runtime periodically to avoid starving other tasks.

```rust
use exdb_docstore::IndexBuilder;

let builder = IndexBuilder::new(
    primary.clone(),
    Arc::new(secondary),
    vec![FieldPath::single("name")],
);

let entries_inserted = builder.build(
    100,   // snapshot timestamp — only sees documents committed at ts <= 100
    None,  // optional progress channel
).await.unwrap();
```

### Vacuum

Old versions accumulate over time. The `VacuumCoordinator` tracks which versions have
been superseded and removes them when it is safe (no active reader can still need them).

```rust
use exdb_docstore::VacuumCoordinator;
use exdb_docstore::vacuum::VacuumCandidate;
use exdb_core::types::CollectionId;

let mut vacuum = VacuumCoordinator::new();

// After a commit that replaces doc_id at ts=10 (old version was ts=5):
vacuum.push_candidate(VacuumCandidate {
    collection_id: CollectionId(1),
    doc_id,
    old_ts: 5,
    superseding_ts: 10,
    old_index_keys: vec![],  // secondary keys of the old version
});

// Later, when no reader is pinned below ts=10:
let eligible = vacuum.drain_eligible(10);
// Execute the removals against the actual indexes
// vacuum.execute(&eligible, &primary_indexes, &secondary_indexes).unwrap();
```

**Rollback vacuum** is separate: it removes entries from commits that failed replication.
This is the only undo mechanism in the system.

## Key encoding details

All keys are byte-comparable via `memcmp`. The encoding preserves type ordering:

| Type | Tag | Encoding |
|------|-----|----------|
| Undefined (missing field) | `0x00` | tag only |
| Null | `0x01` | tag only |
| Int64 | `0x02` | tag + big-endian with sign bit flipped |
| Float64 | `0x03` | tag + IEEE 754 BE with sign flip (negatives flip all bits) |
| Boolean | `0x04` | tag + `0x00`/`0x01` |
| String | `0x05` | tag + UTF-8 with null-byte escaping + `0x00 0x00` terminator |
| Bytes | `0x06` | tag + escaped bytes + `0x00 0x00` terminator |

**Primary key**: `doc_id[16] || inv_ts[8]` where `inv_ts = u64::MAX - ts`.
The inversion means newer timestamps sort first within a doc_id group, so a forward
scan finds the latest version first.

**Secondary key**: `encoded_value[var] || doc_id[16] || inv_ts[8]` with empty value.
All information is in the key — the B-tree value is always `&[]`.

## On-disk cell format

Each primary index B-tree entry stores a cell value:

```text
Tombstone:  [flags: 0x01]                              (1 byte)
Inline:     [flags: 0x00] [body_len: u32 LE] [body]    (5 + N bytes)
External:   [flags: 0x02] [body_len: u32 LE] [HeapRef]  (11 bytes, body in heap)
```

Documents smaller than `external_threshold` (default: page_size / 2) are stored inline
in the B-tree leaf. Larger documents are stored in the heap, with a 6-byte `HeapRef`
(page_id + slot_id) in the leaf.

## Module map

```
docstore/
  key_encoding.rs       ← encode_scalar, make_primary_key, make_secondary_key, ...
  version_resolution.rs ← VersionResolver state machine
  primary_index.rs      ← PrimaryIndex, PrimaryScanner
  secondary_index.rs    ← SecondaryIndex, SecondaryScanner
  array_indexing.rs     ← compute_index_entries
  index_builder.rs      ← IndexBuilder (async)
  vacuum.rs             ← VacuumCoordinator, RollbackVacuum
```

Dependencies flow strictly downward: vacuum → secondary/primary → version_resolution/key_encoding.
No circular dependencies.

## Tests

```
cargo test -p exdb-docstore
```

105 tests covering key encoding roundtrips and ordering, version resolution in both
directions, MVCC reads at various timestamps, tombstone semantics, inline vs heap storage,
secondary index verification, array expansion, background index building, and vacuum
eligibility/execution.
