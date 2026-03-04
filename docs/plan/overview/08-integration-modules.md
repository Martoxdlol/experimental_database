# Integration Modules

**Purpose:** Coordinate across all layers without adding business logic. Database instance lifecycle, system database, in-memory catalog cache (dual-indexed by name AND id), and configuration.

## Modules

### `database.rs` — Per-Database Instance

**WHY HERE:** Brings together storage engine, document store, query engine, and transaction manager into a single operational unit. Orchestration, not logic.

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

    // Integration
    catalog: Arc<RwLock<CatalogCache>>,

    // State
    is_replica: bool,
    shutdown: CancellationToken,
}

impl Database {
    /// Open a database from disk (recovery + catalog load)
    pub async fn open(path: &Path, config: DatabaseConfig) -> Result<Self>;

    /// Close the database (flush, checkpoint, shutdown tasks)
    pub async fn close(&mut self) -> Result<()>;

    // --- Transaction API ---
    pub fn begin_readonly(&self) -> ReadonlyTransaction;
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

    // --- Internal ---
    fn start_background_tasks(&self);
    fn start_checkpoint_task(&self);
    fn start_vacuum_task(&self);
}
```

### `catalog_cache.rs` — In-Memory Dual-Indexed Catalog

**WHY HERE:** Provides O(1) lookups by BOTH name and id for collections, indexes, and databases. Derived from the durable catalog B-tree in Layer 2. All query interfaces support both access patterns.

```rust
pub struct CollectionMeta {
    pub collection_id: CollectionId,
    pub name: String,
    pub primary_root_page: PageId,
    pub created_at_root_page: PageId,
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

    // ID allocators (derived from file header)
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
    pub fn all_collections(&self) -> Vec<&CollectionMeta>;

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

### `system_database.rs` — System Database (Database Registry)

**WHY HERE:** Manages the `_system/` database which is itself a database instance tracking all user databases. Dual-indexed by name AND id.

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
}

impl SystemDatabase {
    pub async fn open(data_root: &Path) -> Result<Self>;
    pub async fn close(&mut self) -> Result<()>;

    // --- By Name ---
    pub fn get_database_by_name(&self, name: &str) -> Option<DatabaseMeta>;

    // --- By ID ---
    pub fn get_database_by_id(&self, id: DatabaseId) -> Option<DatabaseMeta>;

    // --- Listing ---
    pub fn list_databases(&self) -> Vec<DatabaseMeta>;

    // --- Lifecycle ---
    pub async fn create_database(&self, name: &str, config: DatabaseConfig)
        -> Result<DatabaseId>;
    pub async fn drop_database(&self, name: &str) -> Result<()>;
}
```

### `config.rs` — Configuration

**WHY HERE:** Defines all configuration structures used across layers.

```rust
pub struct ServerConfig {
    pub listen: ListenConfig,
    pub tls: Option<TlsConfig>,
    pub auth: AuthConfig,
    pub data_root: PathBuf,
    pub max_message_size: usize,
    pub default_database_config: DatabaseConfig,
    pub replication: ReplicationConfig,
    pub transactions: TransactionConfig,
}

pub struct ListenConfig {
    pub tcp: Option<SocketAddr>,
    pub tls: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

pub struct DatabaseConfig {
    pub page_size: usize,        // default 8192
    pub memory_budget: usize,    // default 256MB
    pub max_doc_size: usize,     // default 16MB
    pub external_threshold: usize,
    pub wal_segment_size: usize,
    pub checkpoint_wal_threshold: usize,
    pub checkpoint_interval: Duration,
}

pub struct ReplicationConfig {
    pub mode: ReplicationMode,
    pub wal_retention_max_size: usize,
    pub wal_retention_max_age: Duration,
}

pub struct TransactionConfig {
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub max_intervals: usize,
    pub max_scanned_bytes: usize,
    pub max_scanned_docs: usize,
}
```

## Startup Sequence

```
1. Load ServerConfig from config file
2. Open _system/ database (SystemDatabase::open)
   a. StorageEngine::recover() — DWB restore + WAL replay
   b. Scan catalog B-tree → populate database registry
3. For each Active database in registry:
   a. Database::open(path, config)
      i.   StorageEngine::recover()
      ii.  Scan catalog B-tree → build CatalogCache
      iii. Open primary indexes and secondary indexes
      iv.  Start CommitCoordinator task
      v.   Start checkpoint background task
      vi.  Start vacuum background task
4. If replica: start ReplicaClient
5. If primary: start PrimaryReplicationServer
6. Start Server (transport listeners)
7. Accept connections
```

## Shutdown Sequence

```
1. Stop accepting new connections
2. Signal shutdown to all sessions
3. Wait for active transactions to complete (with timeout)
4. For each database:
   a. Stop background tasks (vacuum, checkpoint, index builds)
   b. Final checkpoint
   c. Close storage engine
5. Close _system/ database
6. Exit
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

The file header (page 0) stores the root page of the by-ID B-tree. The by-name B-tree root is stored as a special catalog entry in the by-ID B-tree.

Both B-trees are updated atomically during catalog mutations (create/drop collection/index). On startup, both are scanned to populate the bidirectional `CatalogCache`.
