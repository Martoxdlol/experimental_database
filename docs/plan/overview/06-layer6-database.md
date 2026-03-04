# Layer 6: Database Instance

**Layer purpose:** Compose layers 1–5 into a usable embedded database. Owns lifecycle (open/close), collection and index management, transaction API, catalog cache, and defines the replication callback trait. This is the primary public API — fully functional without networking.

## Design Principles

1. **Embedded-first**: `Database` is a Rust struct you instantiate directly. No server, no network, no protocol.
2. **Replication via callback**: The database defines a `ReplicationHook` trait. It calls the hook at the right moment during commit (step 7-8). The database does NOT know how replication messages are transported — that's the implementor's concern.
3. **No upward dependencies**: Database knows nothing about API, networking, auth, sessions, or transport. Higher layers (L7, L8) depend on Database, never the reverse.

## Modules

### `database.rs` — Per-Database Instance

**WHY HERE:** Orchestrates storage engine, document store, query engine, and transaction manager into a single operational unit. The primary API boundary for all database operations.

```rust
pub struct Database {
    pub name: String,
    pub config: DatabaseConfig,

    // Layer 2
    storage: Arc<StorageEngine>,

    // Layer 3
    primary_indexes: RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>,
    secondary_indexes: RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>,

    // Layer 5
    commit_handle: CommitHandle,
    ts_allocator: TsAllocator,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,

    // Catalog (this layer)
    catalog: Arc<RwLock<CatalogCache>>,

    // State
    is_replica: bool,
    shutdown: CancellationToken,
}

impl Database {
    /// Open a database from disk (recovery + catalog load)
    /// Accepts an optional ReplicationHook; defaults to NoReplication
    pub async fn open(
        path: &Path,
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self>;

    /// Open an ephemeral in-memory database (no files, no durability)
    /// Useful for testing, temporary data, caching layers
    pub async fn open_in_memory(
        config: DatabaseConfig,
        replication: Option<Box<dyn ReplicationHook>>,
    ) -> Result<Self>;

    /// Close the database (flush, checkpoint, shutdown tasks)
    pub async fn close(&mut self) -> Result<()>;

    // --- Transaction API ---

    /// Begin a read-only transaction (snapshot at current visible_ts —
    /// the latest replicated/committed timestamp, not ts_allocator.latest())
    pub fn begin_readonly(&self) -> ReadonlyTransaction;

    /// Begin a read-write transaction
    pub fn begin_mutation(&self) -> Result<MutationTransaction>;

    // --- Collection Management ---

    pub fn create_collection(&self, name: &str) -> Result<CollectionId>;
    pub fn drop_collection(&self, name: &str) -> Result<()>;
    pub fn list_collections(&self) -> Vec<CollectionMeta>;
    pub fn get_collection_by_name(&self, name: &str) -> Option<CollectionMeta>;
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<CollectionMeta>;

    // --- Index Management ---

    pub fn create_index(&self, collection: &str, name: &str,
                        fields: Vec<FieldPath>) -> Result<IndexId>;
    pub fn drop_index(&self, collection: &str, name: &str) -> Result<()>;
    pub fn list_indexes(&self, collection: &str) -> Vec<IndexMeta>;
    pub fn get_index_by_name(&self, collection_id: CollectionId,
                              name: &str) -> Option<IndexMeta>;
    pub fn get_index_by_id(&self, id: IndexId) -> Option<IndexMeta>;

    // --- Subscription Access (for higher layers) ---

    /// Get the subscription registry (used by L7/L8 for push notifications)
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>>;

    /// Get the storage engine (used by replication for WAL access)
    pub fn storage(&self) -> &Arc<StorageEngine>;

    // --- Internal ---

    fn start_background_tasks(&self);
    fn start_checkpoint_task(&self);
    fn start_vacuum_task(&self);
}
```

### Transaction API (within `database.rs`)

```rust
/// Read-only transaction — snapshot isolation.
/// `read_ts` is set to `visible_ts` (the latest replicated/committed timestamp),
/// ensuring readers only see data that has been fully committed and replicated.
pub struct ReadonlyTransaction<'db> {
    db: &'db Database,
    read_ts: Ts,
    read_set: ReadSet,
}

impl<'db> ReadonlyTransaction<'db> {
    pub fn get(&mut self, collection: &str, doc_id: &DocId) -> Result<Option<Document>>;
    pub fn query(&mut self, collection: &str, index: &str,
                 range: &[RangeExpr], filter: Option<&Filter>,
                 order: Option<ScanDirection>, limit: Option<usize>)
        -> Result<Vec<Document>>;
    pub fn rollback(self);  // explicit discard (also happens on drop)
}

/// Read-write transaction — OCC with write buffering
pub struct MutationTransaction<'db> {
    db: &'db Database,
    begin_ts: Ts,
    read_set: ReadSet,
    write_set: WriteSet,
}

impl<'db> MutationTransaction<'db> {
    pub fn insert(&mut self, collection: &str, body: serde_json::Value) -> Result<DocId>;
    pub fn replace(&mut self, collection: &str, doc_id: &DocId,
                   body: serde_json::Value) -> Result<()>;
    pub fn patch(&mut self, collection: &str, doc_id: &DocId,
                 patch: serde_json::Value) -> Result<()>;
    pub fn delete(&mut self, collection: &str, doc_id: &DocId) -> Result<()>;
    pub fn get(&mut self, collection: &str, doc_id: &DocId) -> Result<Option<Document>>;
    pub fn query(&mut self, collection: &str, index: &str,
                 range: &[RangeExpr], filter: Option<&Filter>,
                 order: Option<ScanDirection>, limit: Option<usize>)
        -> Result<Vec<Document>>;

    /// Commit the transaction (OCC validate + persist + materialize)
    pub async fn commit(self, options: CommitOptions) -> CommitResult;

    pub fn rollback(self);  // explicit discard
}

pub struct CommitOptions {
    pub subscribe: bool,    // register read set as subscription
    pub notify: bool,       // one-shot invalidation notification
}
```

### `replication_hook.rs` — Replication Callback Trait

**WHY HERE:** The database knows it needs to replicate committed WAL records. It defines WHAT to replicate (the trait), but not HOW (the implementation). This is the dependency inversion point that lets replication live in a higher layer.

```rust
/// Trait for replication — defined in L6, implemented by L7 or any external code.
/// The database calls this during commit (step 7-8) after WAL persist.
/// The implementor decides how to transport the data (TCP, QUIC, etc.).
pub trait ReplicationHook: Send + Sync {
    /// Replicate a committed WAL record and wait for acknowledgment.
    /// Called by CommitCoordinator after WAL fsync (step 7-8).
    /// `lsn` — the log sequence number of the committed record
    /// `record` — the raw WAL record bytes
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;
}

/// No-op implementation for embedded/single-node usage (default)
pub struct NoReplication;

impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> {
        Ok(())
    }
}
```

### `catalog_cache.rs` — In-Memory Dual-Indexed Catalog

**WHY HERE:** Provides O(1) lookups by BOTH name and id for collections and indexes. Derived from the durable catalog B-tree in Layer 2. All query interfaces support both access patterns.

```rust
/// Note: When a collection is created, a `_created_at` secondary index is
/// automatically created and stored as a regular IndexMeta entry (not as a
/// special field on CollectionMeta).
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: PageId,
    pub doc_count: u64,
}

pub struct IndexMeta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
    pub root_page: PageId,
    pub state: IndexState,
}

pub enum IndexState { Building, Ready, Dropping }

pub struct CatalogCache {
    // Collection lookup (bidirectional)
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,

    // Index lookup (bidirectional)
    indexes: HashMap<IndexId, IndexMeta>,
    index_name_to_id: HashMap<(CollectionId, String), IndexId>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,

    // ID allocators
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

impl CatalogCache {
    /// Build from catalog B-tree scan on startup
    pub fn from_btree(btree: &BTreeHandle) -> Result<Self>;

    // --- By Name ---
    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta>;
    pub fn get_index_by_name(&self, collection_id: CollectionId,
                              name: &str) -> Option<&IndexMeta>;

    // --- By ID ---
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<&CollectionMeta>;
    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexMeta>;

    // --- Listing ---
    pub fn list_collections(&self) -> Vec<&CollectionMeta>;
    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;

    // --- Mutation (synchronized with catalog B-tree writes) ---
    pub fn add_collection(&mut self, meta: CollectionMeta);
    pub fn remove_collection(&mut self, id: CollectionId);
    pub fn add_index(&mut self, meta: IndexMeta);
    pub fn remove_index(&mut self, id: IndexId);
    pub fn set_index_state(&mut self, id: IndexId, state: IndexState);

    // --- ID allocation ---
    pub fn next_collection_id(&self) -> CollectionId;
    pub fn next_index_id(&self) -> IndexId;
}
```

### `system_database.rs` — Database Registry

**WHY HERE:** Manages multiple database instances. Itself uses a Database instance internally for durable storage of the registry.

```rust
pub struct DatabaseMeta {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub created_at: u64,
    pub config: DatabaseConfig,
    pub state: DatabaseState,
}

pub enum DatabaseState { Active, Creating, Dropping }

pub struct SystemDatabase {
    inner: Database,  // the _system/ database instance

    // In-memory registry (bidirectional)
    databases_by_name: RwLock<HashMap<String, DatabaseId>>,
    databases_by_id: RwLock<HashMap<DatabaseId, DatabaseMeta>>,

    // Open database handles
    open_databases: RwLock<HashMap<DatabaseId, Arc<Database>>>,
}

impl SystemDatabase {
    pub async fn open(data_root: &Path) -> Result<Self>;
    pub async fn close(&mut self) -> Result<()>;

    // --- Lookup ---
    pub fn get_database_by_name(&self, name: &str) -> Option<Arc<Database>>;
    pub fn get_database_by_id(&self, id: DatabaseId) -> Option<Arc<Database>>;
    pub fn list_databases(&self) -> Vec<DatabaseMeta>;

    // --- Lifecycle ---
    pub async fn create_database(&self, name: &str, config: DatabaseConfig)
        -> Result<DatabaseId>;
    pub async fn drop_database(&self, name: &str) -> Result<()>;
}
```

### `config.rs` — Database Configuration

**WHY HERE:** Configuration for the database layer. Does NOT include server/network/auth config (those belong in L8).

```rust
pub struct DatabaseConfig {
    pub page_size: usize,        // default 8192
    pub memory_budget: usize,    // default 256MB
    pub max_doc_size: usize,     // default 16MB
    pub external_threshold: usize,
    pub wal_segment_size: usize,
    pub checkpoint_wal_threshold: usize,
    pub checkpoint_interval: Duration,
}

pub struct TransactionConfig {
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub max_intervals: usize,
    pub max_scanned_bytes: usize,
    pub max_scanned_docs: usize,
}
```

## Embedded Usage Examples

```rust
// File-backed (durable) — data persists across restarts
let db = Database::open("./mydata", DatabaseConfig::default(), None).await?;

db.create_collection("users")?;

let mut tx = db.begin_mutation()?;
let id = tx.insert("users", json!({"name": "Alice", "age": 30}))?;
let doc = tx.get("users", &id)?;
tx.commit(CommitOptions::default()).await?;

let tx = db.begin_readonly();
let results = tx.query("users", "_created_at", &[], None, None, Some(10))?;

db.close().await?;
```

```rust
// In-memory (ephemeral) — no files, no I/O, data lost on close
let db = Database::open_in_memory(DatabaseConfig::default(), None).await?;

db.create_collection("cache")?;
let mut tx = db.begin_mutation()?;
tx.insert("cache", json!({"key": "session_123", "data": "..."}))?;
tx.commit(CommitOptions::default()).await?;
// Same API — everything works, just no disk persistence
```

## Startup Sequence (Embedded)

```
1. StorageEngine::open() — includes DWB restore + WAL replay internally
2. Scan catalog B-tree → build CatalogCache
3. Open primary + secondary index B-tree handles
4. Create CommitCoordinator with ReplicationHook (or NoReplication)
5. Start CommitCoordinator task
6. Start checkpoint background task
7. Start vacuum background task
8. Ready ✓
```

## Shutdown Sequence

```
1. Signal shutdown to all background tasks
2. Wait for active transactions to complete (with timeout)
3. Stop background tasks (vacuum, checkpoint, index builds)
4. Final checkpoint
5. Close storage engine (flush buffer pool)
6. Done
```

## Catalog Consistency Invariant

The in-memory `CatalogCache` is always consistent with the catalog B-tree. Both are updated atomically within the same commit path:

1. WAL record written and fsynced
2. Catalog B-tree updated via buffer pool
3. In-memory cache updated

If crash between steps 2 and 3: WAL replay reconstructs the B-tree, cache rebuilt on startup.

## Dual B-Tree Catalog (Layer 2 Storage)

Each database's `data.db` contains TWO catalog B-trees:

| B-Tree | Key Format | Purpose |
|--------|-----------|---------|
| **By-ID (primary)** | `entity_type[1] \|\| entity_id[8]` | Primary lookup by collection_id or index_id |
| **By-Name (secondary)** | `entity_type[1] \|\| name_bytes[var] \|\| 0x00` | Name → ID lookup for create/drop/query |

The file header (page 0) stores the root pages of both catalog B-trees.

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `Database::open/close` | L7 (replication), L8 (server) | Lifecycle |
| `Database::begin_readonly/begin_mutation` | L8 (session) | Transaction access |
| `Database::create/drop_collection` | L8 (session) | Schema management |
| `Database::create/drop_index` | L8 (session) | Index management |
| `Database::subscriptions()` | L8 (push notifications) | Subscription access |
| `Database::storage()` | L7 (WAL streaming) | Storage access for replication |
| `ReplicationHook` trait | L7 (implements) | Replication callback |
| `SystemDatabase` | L8 (server) | Multi-database management |
| `CommitResult` | L8 (session response) | Success/conflict reporting |
| `InvalidationEvent` | L8 (push to client) | Subscription invalidation |
