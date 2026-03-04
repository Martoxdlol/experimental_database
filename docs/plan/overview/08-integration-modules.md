# Integration Modules

## Purpose

These modules tie the layers together into cohesive database instances.

## `database.rs` — Per-Database Instance

A `Database` encapsulates all per-database state: storage engine, indexes, transactions, and background tasks.

```rust
pub struct Database {
    pub config: DatabaseConfig,
    pub db_dir: PathBuf,

    // Layer 2: Storage
    pub buffer_pool: Arc<BufferPool>,
    pub wal_writer: Arc<WalWriter>,
    pub checkpoint_mgr: CheckpointManager,
    pub free_list: Mutex<FreeList>,
    pub heap_mgr: Mutex<HeapManager>,

    // Layer 2/3: Catalog
    pub catalog: RwLock<CatalogCache>,

    // Layer 3: Indexes (per collection)
    pub primary_indexes: RwLock<HashMap<CollectionId, PrimaryIndex>>,
    pub secondary_indexes: RwLock<HashMap<IndexId, SecondaryIndex>>,

    // Layer 5: Transactions
    pub tx_manager: TransactionManager,

    // Background tasks
    pub is_replica: bool,
    pub shutdown: CancellationToken,
}

impl Database {
    pub async fn open(config: DatabaseConfig, db_dir: PathBuf) -> Result<Self>;
    pub async fn close(&self) -> Result<()>;

    // === Collection management ===
    pub async fn create_collection(&self, name: &str) -> Result<CollectionId>;
    pub async fn drop_collection_by_name(&self, name: &str) -> Result<()>;
    pub async fn drop_collection_by_id(&self, id: CollectionId) -> Result<()>;
    pub fn get_collection_by_name(&self, name: &str) -> Option<CollectionMeta>;
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<CollectionMeta>;
    pub fn list_collections(&self) -> Vec<CollectionMeta>;

    // === Index management ===
    pub async fn create_index(&self, collection: &str, fields: Vec<FieldPath>, name: Option<String>) -> Result<IndexId>;
    pub async fn drop_index_by_name(&self, collection: &str, name: &str) -> Result<()>;
    pub async fn drop_index_by_id(&self, id: IndexId) -> Result<()>;
    pub fn get_index_by_name(&self, collection: &str, name: &str) -> Option<IndexMeta>;
    pub fn get_index_by_id(&self, id: IndexId) -> Option<IndexMeta>;
    pub fn list_indexes(&self, collection: &str) -> Vec<IndexMeta>;

    // === Transaction operations (delegate to tx_manager) ===
    pub fn begin_transaction(&self, readonly: bool, subscribe: bool, notify: bool) -> Transaction;
    pub async fn commit_transaction(&self, tx: Transaction) -> CommitResult;

    // === Query operations (delegate to query engine) ===
    pub fn execute_get(&self, tx: &mut Transaction, collection: &str, doc_id: DocId) -> Result<Option<Document>>;
    pub fn execute_query(&self, tx: &mut Transaction, collection: &str, plan: &QueryPlan) -> Result<QueryResult>;
}
```

## `catalog.rs` — Catalog B-Trees + In-Memory Cache

### Durable Storage: Two B-Trees

The catalog uses **two B-trees** in `data.db` for dual-indexed lookup:

```
Catalog B-Tree by ID (primary):
  Key:   entity_type[1] || entity_id[8]     (big-endian)
  Value: serialized CollectionEntry or IndexEntry

Catalog B-Tree by Name (secondary):
  Key:   entity_type[1] || name_bytes[var]   (0x00-terminated)
  Value: entity_id[8]                        (pointer to primary)
```

Both B-trees are updated atomically in the same WAL commit. The name B-tree enforces uniqueness — insert fails if the key already exists.

For the system catalog (`_system/`), the same dual-index pattern applies:
- By ID: `0x01 || database_id[8]`
- By Name: `0x01 || name_bytes`

### In-Memory Cache

```rust
pub struct CatalogCache {
    // === Collection lookups (bidirectional) ===
    name_to_collection: HashMap<String, CollectionId>,
    collections: HashMap<CollectionId, CollectionMeta>,

    // === Index lookups (bidirectional) ===
    indexes: HashMap<IndexId, IndexMeta>,
    index_name_to_id: HashMap<(CollectionId, String), IndexId>,
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>,

    // === ID allocators ===
    next_collection_id: AtomicU64,
    next_index_id: AtomicU64,
}

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

impl CatalogCache {
    // --- Rebuild from durable B-trees on startup ---
    pub fn rebuild_from_btrees(
        id_btree: &BTree,
        name_btree: &BTree,
    ) -> Result<Self>;

    // --- Collection queries ---
    pub fn get_collection_by_name(&self, name: &str) -> Option<&CollectionMeta>;
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<&CollectionMeta>;
    pub fn list_collections(&self) -> Vec<&CollectionMeta>;

    // --- Index queries ---
    pub fn get_index_by_name(&self, collection_id: CollectionId, name: &str) -> Option<&IndexMeta>;
    pub fn get_index_by_id(&self, id: IndexId) -> Option<&IndexMeta>;
    pub fn list_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;
    pub fn list_ready_indexes(&self, collection_id: CollectionId) -> Vec<&IndexMeta>;

    // --- Mutations (called by writer task after WAL commit) ---
    pub fn insert_collection(&mut self, meta: CollectionMeta) -> Result<()>;
    pub fn remove_collection(&mut self, id: CollectionId) -> Result<()>;
    pub fn insert_index(&mut self, meta: IndexMeta) -> Result<()>;
    pub fn remove_index(&mut self, id: IndexId) -> Result<()>;
    pub fn mark_index_ready(&mut self, id: IndexId) -> Result<()>;
}
```

## `system.rs` — System Database + Multi-DB Management

```rust
pub struct SystemDatabase {
    // The _system database instance (holds the database registry catalog)
    pub system_db: Database,

    // All open user databases — dual-indexed
    pub databases_by_name: RwLock<HashMap<String, Arc<Database>>>,
    pub databases_by_id: RwLock<HashMap<DatabaseId, Arc<Database>>>,

    pub config: ServerConfig,
}

impl SystemDatabase {
    pub async fn open(data_root: PathBuf, config: ServerConfig) -> Result<Self>;
    pub async fn close(&self) -> Result<()>;

    // === Database lifecycle ===
    pub async fn create_database(&self, name: &str, config: Option<DatabaseConfig>) -> Result<DatabaseId>;
    pub async fn drop_database_by_name(&self, name: &str) -> Result<()>;
    pub async fn drop_database_by_id(&self, id: DatabaseId) -> Result<()>;

    // === Database queries ===
    pub fn get_database_by_name(&self, name: &str) -> Result<Arc<Database>>;
    pub fn get_database_by_id(&self, id: DatabaseId) -> Result<Arc<Database>>;
    pub fn list_databases(&self) -> Vec<DatabaseInfo>;
}
```

## `config.rs` — Configuration Types

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

pub struct DatabaseConfig {
    pub page_size: usize,          // default: 8192
    pub memory_budget: usize,      // default: 256 MB
    pub max_doc_size: usize,       // default: 16 MB
    pub external_threshold: usize, // default: page_size / 2
    pub checkpoint_wal_threshold: u64,   // default: 64 MB
    pub checkpoint_time_threshold: Duration, // default: 5 min
}
```

## Startup Sequence Diagram

```
  main.rs
    │
    ├── Read config.json → ServerConfig
    │
    ├── SystemDatabase::open(data_root)
    │   │
    │   ├── Open _system/ database
    │   │   ├── DWB recovery (if needed)
    │   │   ├── WAL replay from checkpoint_lsn
    │   │   └── Rebuild catalog cache (database registry)
    │   │
    │   └── For each Active database in registry:
    │       ├── Database::open(db_dir)
    │       │   ├── DWB recovery
    │       │   ├── WAL replay
    │       │   ├── Rebuild catalog cache (collections + indexes)
    │       │   ├── Initialize buffer pool
    │       │   ├── Initialize TransactionManager
    │       │   └── Start background tasks:
    │       │       ├── Checkpoint task
    │       │       ├── Vacuum task
    │       │       └── Index build tasks (for Building indexes)
    │       └── Store Arc<Database> in databases map
    │
    ├── Server::start(config, system_db)
    │   ├── Spawn TCP listener
    │   ├── Spawn TLS listener (if configured)
    │   ├── Spawn QUIC listener (if configured)
    │   └── Spawn WebSocket listener (if configured)
    │
    └── If replica:
        └── ReplicaClient::start(primary_addr)
            └── Connect → receive WAL stream → apply
```

## Shutdown Sequence

```
  Shutdown signal (SIGTERM / ctrl-c)
    │
    ├── Stop accepting new connections
    ├── Close existing connections (rollback open transactions)
    ├── If replica: disconnect from primary
    ├── If primary: wait for final replication acks
    │
    └── For each database:
        ├── Stop background tasks (vacuum, index build)
        ├── Final checkpoint (flush all dirty pages)
        ├── Close WAL (sync final segment)
        └── Close data file
```
