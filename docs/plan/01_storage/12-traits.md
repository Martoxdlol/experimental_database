# 12 — Traits and Interfaces

Key abstractions that allow testing, modularity, and future extensibility.

## Core Traits

### PageIO — abstraction over disk I/O

```rust
/// Abstraction over page-level disk I/O. Allows testing with in-memory
/// storage and production use with real files.
#[async_trait::async_trait]
pub trait PageIO: Send + Sync {
    /// Read a page from the store into `buf`. Returns the number of bytes read.
    async fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<usize, StorageError>;

    /// Write a page to the store from `buf`.
    async fn write_page(&self, page_id: PageId, buf: &[u8]) -> Result<(), StorageError>;

    /// Sync all pending writes to durable storage.
    async fn sync(&self) -> Result<(), StorageError>;

    /// Extend the store by one page. Returns the new page count.
    async fn extend(&self) -> Result<u64, StorageError>;

    /// Current number of pages in the store.
    async fn page_count(&self) -> Result<u64, StorageError>;
}

/// File-backed implementation.
pub struct FilePageIO { /* tokio::fs::File */ }

/// In-memory implementation (for tests).
pub struct MemPageIO { /* Vec<Vec<u8>> */ }
```

### WalSink — abstraction over WAL writing

```rust
/// Abstraction over where WAL records go. Primary writes to disk,
/// replicas receive from the network.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    /// Write a WAL record. Returns the assigned LSN once durable.
    async fn write(&self, record: &WalRecord) -> Result<Lsn, WalError>;

    /// Current LSN position.
    fn current_lsn(&self) -> Lsn;
}
```

### KeyEncoder — index key encoding

```rust
/// Encodes field values into order-preserving byte keys (DESIGN §3.4).
pub trait KeyEncoder {
    /// Encode a single scalar value with its type tag.
    fn encode_value(&self, value: &ScalarValue) -> Vec<u8>;

    /// Encode a compound key from multiple field values.
    fn encode_compound(&self, values: &[ScalarValue]) -> Vec<u8>;

    /// Decode the first value from an encoded key buffer.
    /// Returns (value, bytes_consumed).
    fn decode_value(&self, buf: &[u8]) -> Result<(ScalarValue, usize), StorageError>;
}

/// The scalar values that can appear in index keys.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Undefined,
    Null,
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
}
```

### DocumentStore — high-level document operations

```rust
/// The interface that the transaction layer calls into the storage layer.
/// This is the primary integration boundary.
#[async_trait::async_trait]
pub trait DocumentStore: Send + Sync {
    /// Get a document by ID at a read timestamp.
    async fn get(
        &self,
        collection_id: CollectionId,
        doc_id: DocId,
        read_ts: Timestamp,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    /// Scan an index within bounds, returning visible documents.
    async fn scan(
        &self,
        index_id: IndexId,
        lower: &[u8],
        upper: Option<&[u8]>,
        direction: Direction,
        read_ts: Timestamp,
        limit: Option<u32>,
    ) -> Result<ScanResult, StorageError>;

    /// Apply committed mutations to the page store.
    async fn apply(
        &self,
        commit_ts: Timestamp,
        mutations: &[Mutation],
        index_deltas: &[IndexDelta],
        lsn: Lsn,
    ) -> Result<(), StorageError>;

    /// Write a WAL record.
    async fn write_wal(&self, record: &WalRecord) -> Result<Lsn, StorageError>;
}

#[derive(Debug)]
pub struct ScanResult {
    pub documents: Vec<DocumentResult>,
    /// The actual byte range scanned (for read set recording).
    pub scanned_lower: Vec<u8>,
    pub scanned_upper: Option<Vec<u8>>,
    /// The key of the last returned document (for limit tightening).
    pub last_key: Option<Vec<u8>>,
}
```

### CatalogOps — catalog operations

```rust
/// Operations on database metadata. Called by management message handlers.
#[async_trait::async_trait]
pub trait CatalogOps: Send + Sync {
    async fn create_collection(&self, name: &str) -> Result<CollectionMeta, StorageError>;
    async fn drop_collection(&self, name: &str) -> Result<(), StorageError>;
    fn get_collection(&self, name: &str) -> Option<CollectionMeta>;
    fn list_collections(&self) -> Vec<CollectionMeta>;

    async fn create_index(
        &self,
        collection: &str,
        name: &str,
        fields: Vec<FieldPath>,
    ) -> Result<IndexMeta, StorageError>;
    async fn drop_index(&self, collection: &str, name: &str) -> Result<(), StorageError>;
    fn list_indexes(&self, collection: &str) -> Vec<IndexMeta>;
}
```

## Why These Traits

| Trait | Purpose |
|-------|---------|
| `PageIO` | Decouple buffer pool from filesystem. Enables in-memory tests. |
| `WalSink` | Same WAL record type used for local writes and replica ingestion. |
| `KeyEncoder` | Isolate the encoding logic from B-tree operations. Makes it testable independently. |
| `DocumentStore` | The boundary between storage and transaction layers. The transaction layer only calls this. |
| `CatalogOps` | The boundary between storage and API/management layer. |

## Testing Strategy

Each layer is testable in isolation:
- **Page format**: unit tests on `SlottedPage` with in-memory buffers.
- **WAL**: unit tests on record serialization; integration tests with `tempfile`.
- **Buffer pool**: tests with `MemPageIO`.
- **B-tree**: tests with `MemPageIO`-backed buffer pool.
- **Checkpoint/recovery**: integration tests with `tempfile` directories.
- **Engine**: end-to-end tests with `tempfile` database directories.
