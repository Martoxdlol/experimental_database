# Layer 6: Database Instance

**Layer purpose:** Compose layers 1–5 into a usable embedded database. Owns lifecycle (open/close), collection and index management, transaction API, catalog cache, and **implements** the L5-defined trait interfaces (`ReplicationHook`, `CatalogMutator`, `IndexResolver`). This is the primary public API — fully functional without networking.

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

    /// Begin a transaction with the given options.
    /// The single entry point replaces the previous begin_readonly / begin_mutation split.
    /// read_ts is set to visible_ts (latest replicated/committed timestamp).
    /// Returns error if readonly=false and replication hook reports no quorum or hold state.
    pub fn begin(&self, opts: TransactionOptions) -> Result<Transaction>;

    // Convenience aliases:
    // pub fn begin_readonly(&self) -> Transaction
    //     { self.begin(TransactionOptions { readonly: true, ..Default::default() })? }
    // pub fn begin_mutation(&self) -> Result<Transaction>
    //     { self.begin(TransactionOptions::default())? }

    // --- Read-Only Catalog Access (outside transactions) ---
    // These are convenience methods that read the current committed catalog state.
    // They do NOT record catalog reads for OCC — use the transaction methods
    // when catalog consistency with other operations is required.

    pub fn list_collections(&self) -> Vec<CollectionMeta>;
    pub fn get_collection_by_name(&self, name: &str) -> Option<CollectionMeta>;
    pub fn get_collection_by_id(&self, id: CollectionId) -> Option<CollectionMeta>;
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

### Transaction Options

```rust
/// Options for beginning a transaction.
/// Replaces the previous begin_readonly() / begin_mutation() split.
pub struct TransactionOptions {
    /// If true, write operations (insert, replace, patch, delete, DDL)
    /// return an error. No OCC validation at commit. No write set allocated.
    pub readonly: bool,
    /// Subscription mode — controls what happens to the read set after commit.
    /// See L5 SubscriptionMode for full semantics.
    pub subscription: SubscriptionMode,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            readonly: false,
            subscription: SubscriptionMode::None,
        }
    }
}
```

### Unified Transaction API (within `database.rs`)

```rust
/// Unified transaction — handles both reads and writes.
/// Whether writes are allowed is controlled by `opts.readonly`.
///
/// `begin_ts` is set to `visible_ts` (the latest replicated/committed timestamp),
/// ensuring the snapshot only includes fully committed and replicated data.
///
/// For Subscribe-mode chain continuations, the transaction may be initialized
/// with a carried read set and a non-zero starting query_id.
pub struct Transaction<'db> {
    db: &'db Database,
    tx_id: TxId,
    opts: TransactionOptions,
    begin_ts: Ts,
    read_set: ReadSet,
    write_set: WriteSet,  // empty if readonly (no allocation)
}

impl<'db> Transaction<'db> {
    // --- Read Operations (always available) ---

    pub fn get(&mut self, collection: &str, doc_id: &DocId) -> Result<Option<Document>>;
    pub fn query(&mut self, collection: &str, index: &str,
                 range: &[RangeExpr], filter: Option<&Filter>,
                 order: Option<ScanDirection>, limit: Option<usize>)
        -> Result<Vec<Document>>;

    // --- Write Operations (error if readonly) ---

    pub fn insert(&mut self, collection: &str, body: serde_json::Value) -> Result<DocId>;
    pub fn replace(&mut self, collection: &str, doc_id: &DocId,
                   body: serde_json::Value) -> Result<()>;
    pub fn patch(&mut self, collection: &str, doc_id: &DocId,
                 patch: serde_json::Value) -> Result<()>;
    pub fn delete(&mut self, collection: &str, doc_id: &DocId) -> Result<()>;

    // --- Collection Management (DDL, error if readonly) ---

    /// Create a collection. Returns a provisional CollectionId that can be
    /// used immediately for inserts within this same transaction.
    pub fn create_collection(&mut self, name: &str) -> Result<CollectionId>;
    pub fn drop_collection(&mut self, name: &str) -> Result<()>;

    // --- Index Management (DDL, error if readonly) ---

    pub fn create_index(&mut self, collection: &str, name: &str,
                        fields: Vec<FieldPath>) -> Result<IndexId>;
    pub fn drop_index(&mut self, collection: &str, name: &str) -> Result<()>;

    // --- Catalog reads (recorded in read set for OCC / subscriptions) ---
    pub fn list_collections(&mut self) -> Vec<CollectionMeta>;
    pub fn list_indexes(&mut self, collection: &str) -> Vec<IndexMeta>;

    // --- Lifecycle ---

    /// Commit the transaction.
    /// - Readonly + subscription: registers read set, no OCC.
    ///   Returns SubscriptionHandle in CommitResult.
    /// - Readonly + None: no-op (read set discarded).
    /// - Read-write: OCC validate + WAL + materialize + register subscription.
    pub async fn commit(self) -> CommitResult;

    /// Explicit rollback. Discards all buffered mutations and read set.
    /// No subscription is registered. Equivalent to drop without commit.
    pub fn rollback(self);

    /// Reset transaction state: clears read + write sets, resets query_id
    /// counter to 0. Keeps the same begin_ts (same snapshot).
    /// Useful for retry loops within the same snapshot or discarding
    /// partial work without closing the transaction.
    pub fn reset(&mut self);

    // --- Introspection ---

    pub fn tx_id(&self) -> TxId;
    pub fn begin_ts(&self) -> Ts;
    pub fn is_readonly(&self) -> bool;
    pub fn subscription_mode(&self) -> SubscriptionMode;
}

/// Drop = implicit rollback. No subscription registered.
/// The transaction is removed from the active transaction set.
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Remove from Database's active transaction set.
        // No commit, no subscription. Resources released.
    }
}
```

### SubscriptionHandle

Returned from `CommitResult::Success` when subscription mode is not `None`. L6 constructs it from `event_rx` in the commit result. Provides the event stream and RAII cleanup.

```rust
pub struct SubscriptionHandle {
    id: SubscriptionId,
    registry: Arc<RwLock<SubscriptionRegistry>>,
    /// Receiver for invalidation events.
    /// For write commits: sourced from CommitResult::Success::event_rx.
    /// For read-only commits: L6 creates the mpsc channel directly
    ///   before calling registry.register(), keeps the receiver here.
    events: mpsc::Receiver<InvalidationEvent>,
}

impl SubscriptionHandle {
    /// The subscription's unique ID.
    pub fn id(&self) -> SubscriptionId;

    /// Wait for the next invalidation event.
    /// Returns None if the subscription was removed (e.g., Notify mode
    /// after firing, or server shutdown).
    pub async fn next_event(&mut self) -> Option<InvalidationEvent>;

    /// Explicitly end the subscription and remove it from the registry.
    /// After this call, no further events will be received.
    /// Also happens automatically on drop.
    pub fn unsubscribe(self);
}

/// Drop = automatic unsubscribe. Removes from registry.
impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        // registry.remove(self.id)
    }
}
```

### Chain Continuation Transaction

When a `Subscribe`-mode subscription is invalidated, a new `Transaction` is auto-started internally by the commit coordinator. L6 constructs it with the carried read set:

```rust
impl Database {
    /// Create a chain continuation transaction (internal, not public API).
    /// Called when Subscribe invalidation fires.
    fn begin_chain_continuation(
        &self,
        continuation: &ChainContinuation,
        opts: TransactionOptions,
    ) -> Transaction {
        // Pre-load the carried read set, then set the query_id counter
        // to first_query_id so new queries continue numbering from there.
        let mut read_set = continuation.carried_read_set.clone();
        // Adjust the starting query_id so new queries assigned in this
        // transaction don't collide with already-carried query IDs.
        // carried_read_set was produced by split_before(first_query_id),
        // so all its intervals have query_id < first_query_id.
        // We start the new transaction's counter at first_query_id.
        // ReadSet::with_starting_query_id is used implicitly by ensuring
        // next_query_id is set correctly via the internal counter.
        read_set.set_next_query_id(continuation.first_query_id);
        Transaction {
            db: self,
            tx_id: continuation.new_tx_id,
            opts,
            begin_ts: continuation.new_ts,
            read_set,
            write_set: WriteSet::new(),
        }
    }
}
```

### `replication_hook.rs` — Replication and Catalog Trait Implementations

**WHY HERE:** L5 defines `ReplicationHook`, `CatalogMutator`, and `IndexResolver` traits (dependency inversion — L5 defines the interface, L6 provides implementations). L6's `replication_hook.rs` houses these implementations and the `NoReplication` default.

```rust
// ReplicationHook is defined in L5 (exdb_tx::commit::ReplicationHook).
// L5 also provides NoReplication. L6 re-exports it for convenience.
pub use exdb_tx::commit::{ReplicationHook, NoReplication};

// CatalogMutator is defined in L5 (exdb_tx::commit::CatalogMutator).
// L6 implements it, wrapping CatalogCache + catalog B-trees.
//
// apply():  update catalog B-tree page → update CatalogCache in-memory
// rollback(): reverse the mutation (remove created entry / restore dropped entry)
pub struct CatalogMutatorImpl {
    catalog: Arc<RwLock<CatalogCache>>,
    storage: Arc<StorageEngine>,
}
impl exdb_tx::commit::CatalogMutator for CatalogMutatorImpl { ... }

// IndexResolver is defined in L5 (exdb_tx::write_set::IndexResolver).
// L6 implements it, reading from CatalogCache.
pub struct IndexResolverImpl {
    catalog: Arc<RwLock<CatalogCache>>,
}
impl exdb_tx::write_set::IndexResolver for IndexResolverImpl { ... }
```

**Dependency inversion summary:**

| Trait | Defined in | Implemented in |
|-------|-----------|---------------|
| `ReplicationHook` | L5 (`commit.rs`) | L7 (real replication), L5 (`NoReplication` default) |
| `CatalogMutator` | L5 (`commit.rs`) | L6 (`CatalogMutatorImpl`) |
| `IndexResolver` | L5 (`write_set.rs`) | L6 (`IndexResolverImpl`) |

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

// Create collection + insert documents atomically in one transaction
let mut tx = db.begin(TransactionOptions::default())?;  // read-write, no subscription
tx.create_collection("users")?;
let id = tx.insert("users", json!({"name": "Alice", "age": 30}))?;
let doc = tx.get("users", &id)?;
tx.commit().await?;

// Read-only transaction
let mut tx = db.begin(TransactionOptions { readonly: true, ..Default::default() })?;
let results = tx.query("users", "_created_at", &[], None, None, Some(10))?;
tx.rollback();  // or just drop

db.close().await?;
```

```rust
// Subscribe mode — live query subscription chain
let mut tx = db.begin(TransactionOptions {
    readonly: true,
    subscription: SubscriptionMode::Subscribe,
})?;
let users = tx.query("users", "_created_at", &[], None, None, Some(50))?;  // query_id=0
let orders = tx.query("orders", "by_status", &[eq("status", "active")], None, None, None)?;  // query_id=1
let result = tx.commit().await?;
// result.subscription_id is Some(...)
// When a future commit invalidates query_id=1:
//   → InvalidationEvent with affected_query_ids=[1], continuation with:
//     carried_read_set (query_id=0's intervals), first_query_id=1
//   → Client re-executes query_id=1 in the new transaction
```

```rust
// Watch mode — persistent notification without auto-transaction
let mut tx = db.begin(TransactionOptions {
    readonly: true,
    subscription: SubscriptionMode::Watch,
})?;
let users = tx.query("users", "_created_at", &[], None, None, Some(50))?;
let result = tx.commit().await?;
// Every time a commit touches the watched interval:
//   → InvalidationEvent with affected_query_ids (no continuation)
//   → Client decides when/how to refresh
```

```rust
// DDL and data in separate transactions
let mut tx = db.begin(TransactionOptions::default())?;
tx.create_collection("orders")?;
tx.create_index("orders", "by_customer", vec![FieldPath::single("customer_id")])?;
tx.commit().await?;

let mut tx = db.begin(TransactionOptions::default())?;
tx.insert("orders", json!({"customer_id": "c1", "total": 99.99}))?;
tx.commit().await?;
```

## Startup Sequence (Embedded)

```
1. StorageEngine::open() — includes DWB restore + WAL replay internally
   (WAL replay handles TxCommit records that include catalog mutations:
    allocate pages, update catalog B-tree — same as runtime commit step 4a)
2. Scan catalog B-tree → build CatalogCache
3. Open primary + secondary index B-tree handles
4. Rollback vacuum: if committed ts > visible_ts, clean up un-replicated entries via WAL scan
   (includes reverting catalog mutations from rolled-back commits)
5. Create CommitCoordinator with ReplicationHook (or NoReplication)
6. Start CommitCoordinator task
7. Start checkpoint background task
8. Start vacuum background task
9. Ready ✓
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

Note: Hold state is preserved across restart via `visible_ts` in the FileHeader. If the database shuts down while in hold state (quorum lost during commit), the persisted `visible_ts` will be behind the committed ts. On next startup, the rollback vacuum step (step 4) detects this gap and cleans up the un-replicated entries.

## Transactional DDL

Collection and index management (create, drop) are transactional operations performed within a `MutationTransaction`. This means:

1. **Atomicity**: DDL and data mutations in the same transaction are applied or rolled back together. You can create a collection and insert documents into it in a single atomic commit.
2. **Isolation**: Concurrent transactions that read the catalog (e.g., resolving a collection name for an insert) are validated against DDL commits via OCC. A transaction that read "users" exists will conflict if another transaction drops "users" concurrently.
3. **Serialization**: All DDL flows through the `CommitCoordinator`'s single-writer loop, so catalog updates never race with each other or with data mutations.

### Provisional IDs

When `tx.create_collection("users")` is called, a `CollectionId` is allocated eagerly from the atomic counter and returned immediately. This allows the transaction to insert documents into the new collection before committing. On abort, the ID is simply never committed — a harmless gap in the ID sequence.

Within the transaction, collection name resolution checks pending `CatalogMutation::CreateCollection` entries in the write set first, then falls back to the committed `CatalogCache`.

### Catalog Read Tracking

When a transaction resolves a collection or index name (e.g., `tx.insert("users", ...)` looks up "users" → CollectionId), a `CatalogRead::CollectionByName("users")` is recorded in the read set. At OCC validation, this is checked against concurrent commits' `CatalogMutation` entries:

- `CatalogRead::CollectionByName("X")` conflicts with `CreateCollection{name: "X"}` or `DropCollection{name: "X"}` in any concurrent commit.
- `CatalogRead::ListCollections` conflicts with **any** collection create or drop in any concurrent commit.
- Index reads follow the same pattern scoped to a collection.

This is conservative but correct, and DDL is rare enough that false conflicts are negligible.

## Catalog Consistency Invariant

The in-memory `CatalogCache` is always consistent with the catalog B-tree. Both are updated atomically within the commit coordinator's apply step (step 4a):

1. WAL record written and fsynced (includes catalog mutations)
2. Catalog B-tree updated via buffer pool
3. In-memory cache updated
4. Data mutations applied (step 4b)

Since all catalog updates go through the single-writer commit coordinator, the CatalogCache RwLock only needs to serialize reads against the commit coordinator's writes — no concurrent DDL writers exist.

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
| `Database::begin(TransactionOptions)` | L8 (session) | Unified transaction entry point |
| `Transaction::insert/replace/patch/delete` | L8 (session) | Document mutations |
| `Transaction::create/drop_collection` | L8 (session) | Transactional schema management |
| `Transaction::create/drop_index` | L8 (session) | Transactional index management |
| `Transaction::commit()` | L8 (session) | Commit with subscription mode from options |
| `TransactionOptions` | L8 (session) | Transaction configuration |
| `Database::subscriptions()` | L8 (push notifications) | Subscription access |
| `Database::storage()` | L7 (WAL streaming) | Storage access for replication |
| `ReplicationHook` trait | L7 (implements) | Replication callback |
| `SystemDatabase` | L8 (server) | Multi-database management |
| `CommitResult`, `ConflictRetry` | L8 (session response) | Success/conflict/retry reporting |
| `InvalidationEvent`, `ChainContinuation` | L8 (push to client) | Subscription invalidation + chain |
