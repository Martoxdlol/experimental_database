# Layer 3: Indexing

## Purpose

B+ tree operations, order-preserving key encoding, MVCC version resolution in indexes. Sits between the raw page store (Layer 2) and the query engine (Layer 4).

## Sub-Modules

### `index/btree.rs` — Generic B+ Tree

Generic B+ tree operating on slotted pages via the buffer pool.

```rust
pub struct BTree {
    root_page: AtomicU32,  // Root can change on splits
    buffer_pool: Arc<BufferPool>,
}

impl BTree {
    pub fn new(root_page: PageId, buffer_pool: Arc<BufferPool>) -> Self;

    // Point operations
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<bool>;

    // Range scan (returns iterator over leaf cells)
    pub fn scan_range(
        &self,
        lower: &[u8],
        upper: Bound<&[u8]>,
        direction: ScanDirection,
    ) -> Result<BTreeScanIter>;

    // Tree maintenance
    pub fn root_page(&self) -> PageId;
}

pub enum ScanDirection { Forward, Backward }

pub struct BTreeScanIter<'a> {
    // Walks leaf pages via right_sibling links
    // Yields (key: &[u8], value: &[u8]) pairs
}
```

### `index/key_encoding.rs` — Order-Preserving Key Encoding

Encodes `Scalar` values into byte-comparable keys per DESIGN.md §3.4.

```rust
// Type tags (ascending byte values matching type ordering)
pub const TAG_UNDEFINED: u8 = 0x00;
pub const TAG_NULL: u8     = 0x01;
pub const TAG_INT64: u8    = 0x02;
pub const TAG_FLOAT64: u8  = 0x03;
pub const TAG_BOOLEAN: u8  = 0x04;
pub const TAG_STRING: u8   = 0x05;
pub const TAG_BYTES: u8    = 0x06;
pub const TAG_ARRAY: u8    = 0x07;

pub fn encode_scalar(scalar: &Scalar) -> Vec<u8>;
pub fn decode_scalar(bytes: &[u8]) -> Result<(Scalar, usize)>;  // returns (value, bytes_consumed)

// Compound key construction
pub fn encode_index_key(
    values: &[Scalar],
    doc_id: DocId,
    inv_ts: u64,
) -> Vec<u8>;

pub fn decode_index_key_suffix(key: &[u8]) -> (DocId, Ts);  // Extract doc_id + ts from key tail

// Primary key construction
pub fn make_primary_key(doc_id: DocId, inv_ts: u64) -> [u8; 24];

// Range bound helpers
pub fn successor_prefix(prefix: &[u8]) -> Vec<u8>;
```

### `index/primary.rs` — Primary (Clustered) Index

```rust
pub struct PrimaryIndex {
    btree: BTree,
    external_threshold: usize,
    heap: Arc<HeapManager>,
}

impl PrimaryIndex {
    // Read the latest visible version of a document
    pub fn get_at_ts(&self, doc_id: DocId, read_ts: Ts) -> Result<Option<Document>>;

    // Insert a new document version
    pub fn insert_version(
        &self,
        doc_id: DocId,
        commit_ts: Ts,
        body: &[u8],  // BSON-encoded
    ) -> Result<()>;

    // Insert a tombstone (delete marker)
    pub fn insert_tombstone(&self, doc_id: DocId, commit_ts: Ts) -> Result<()>;

    // Remove an old version (vacuum)
    pub fn remove_version(&self, doc_id: DocId, ts: Ts) -> Result<()>;

    // Scan all documents visible at read_ts (for table scan / index build)
    pub fn scan_at_ts(&self, read_ts: Ts) -> Result<PrimaryScanIter>;
}
```

### `index/secondary.rs` — Secondary Index Operations

```rust
pub struct SecondaryIndex {
    btree: BTree,
    index_meta: IndexMeta,  // field_paths, index_id, etc.
}

impl SecondaryIndex {
    // Scan a range with MVCC version resolution
    pub fn scan_range_at_ts(
        &self,
        lower: &[u8],
        upper: Bound<&[u8]>,
        read_ts: Ts,
        direction: ScanDirection,
        primary: &PrimaryIndex,  // For version verification
    ) -> Result<SecondaryIndexScanIter>;

    // Insert/remove index entries (used by writer at commit)
    pub fn insert_entry(&self, encoded_key: &[u8]) -> Result<()>;
    pub fn remove_entry(&self, encoded_key: &[u8]) -> Result<()>;

    // Apply an IndexDelta (from WAL commit record)
    pub fn apply_delta(&self, delta: &WalIndexDelta, commit_ts: Ts) -> Result<()>;
}
```

### `index/builder.rs` — Background Index Building

```rust
pub struct IndexBuilder { /* ... */ }

impl IndexBuilder {
    pub async fn build_index(
        &self,
        collection_id: CollectionId,
        index_meta: &IndexMeta,
        snapshot_ts: Ts,
        primary: &PrimaryIndex,
        writer_channel: mpsc::Sender<WriterRequest>,
    ) -> Result<()>;
}
```

## Interfaces Exposed to Higher Layers

| Consumer | Interface |
|---|---|
| Layer 4 (Query) | `PrimaryIndex::get_at_ts()`, `SecondaryIndex::scan_range_at_ts()`, `PrimaryIndex::scan_at_ts()` |
| Layer 5 (Transactions) | `PrimaryIndex::insert_version/tombstone()`, `SecondaryIndex::apply_delta()`, `encode_index_key()` |
| Layer 2 (Catalog) | `BTree` (for catalog B-tree operations) |

## Key Data Flow: Secondary Index Scan with Version Resolution

```
                   ┌──────────────┐
                   │ SecondaryIndex│
                   │  scan_range   │
                   └──────┬───────┘
                          │
                    ┌─────▼──────┐
                    │ B+ Tree    │
                    │ leaf scan  │
                    └─────┬──────┘
                          │ for each (encoded_key, doc_id, inv_ts):
                          │
              ┌───────────▼────────────┐
              │ MVCC version resolution │
              │ skip if ts > read_ts    │
              │ deduplicate per doc_id  │
              └───────────┬────────────┘
                          │
              ┌───────────▼────────────┐
              │ Verify against primary │
              │ (check for stale entry)│
              └───────────┬────────────┘
                          │
                    ┌─────▼──────┐
                    │ Yield doc  │
                    └────────────┘
```
