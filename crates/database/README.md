# exdb — Layer 6: Database Facade

The primary embedded API for the experimental database. Provides a unified
transaction interface, catalog management, background maintenance tasks, and
multi-database support.

## Architecture

```text
B8 (SystemDatabase)         Multi-database registry
B7 (SubscriptionHandle)     RAII subscription wrapper
B6 (Database)               Main facade: lifecycle, transactions, background tasks
B5 (Transaction)            Unified read/write/DDL API
B4 (CatalogTracker)         OCC intervals for catalog reads
B3 (IndexResolver)          L5 IndexResolver trait implementation
B2 (CatalogCache)           In-memory dual-indexed catalog
B1 (Config)                 DatabaseConfig, TransactionConfig
B9 (CatalogPersistence)     Durable catalog B-tree bridge
B10 (CatalogRecovery)       WAL replay for crash recovery
```

## Quick Start

```rust
use exdb::{Database, DatabaseConfig, TransactionOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> exdb::Result<()> {
    let db = Database::open_in_memory(DatabaseConfig::default(), None).await?;

    let mut tx = db.begin(TransactionOptions::default())?;
    tx.create_collection("users").await?;
    let doc_id = tx.insert("users", json!({
        "name": "Alice",
        "email": "alice@example.com"
    })).await?;
    tx.commit().await?;

    let mut tx = db.begin(TransactionOptions::readonly())?;
    let doc = tx.get("users", &doc_id).await?;
    println!("Found: {:?}", doc);
    tx.rollback();

    db.close().await?;
    Ok(())
}
```

## File-Backed Storage

```rust
let db = Database::open("./data/mydb", DatabaseConfig::default(), None).await?;
// ... use the database ...
db.close().await?;
```

Data persists across restarts. WAL + checkpoint ensures durability. Crash
recovery replays uncommitted WAL records on next open.

## Async & Multi-Threaded Usage

`Database` is `Send + Sync`. Share it via `Arc` and use it from any tokio task:

```rust
use std::sync::Arc;

let db = Arc::new(Database::open_in_memory(config, None).await?);

let db2 = Arc::clone(&db);
tokio::spawn(async move {
    let mut tx = db2.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();
}).await?;
```

`Transaction` futures are `Send` — all internal lock guards are dropped before
`.await` points, following the same pattern as the L5 `CommitCoordinator`.

## Transaction API

### Read Operations
- `get(collection, doc_id)` — point lookup by document ID
- `query(collection, index, range, filter, direction, limit)` — index scan with filtering
- `list_collections()` — list all collections
- `list_indexes(collection)` — list indexes for a collection

### Write Operations
- `insert(collection, body)` — insert new document, returns generated DocId
- `replace(collection, doc_id, body)` — replace entire document
- `patch(collection, doc_id, patch)` — RFC 7396 merge-patch
- `delete(collection, doc_id)` — delete a document

### DDL Operations
- `create_collection(name)` — create a new collection (with auto `_created_at` index)
- `drop_collection(name)` — drop a collection and all its indexes
- `create_index(collection, name, fields)` — create a secondary index
- `drop_index(collection, name)` — drop a secondary index

### Lifecycle
- `commit()` — commit the transaction (OCC validation)
- `rollback()` — discard all changes
- `reset()` — clear state and get a fresh snapshot

## Multi-Database (SystemDatabase)

```rust
use exdb::{SystemDatabase, DatabaseConfig};

let sys = SystemDatabase::open("./data").await?;
sys.create_database("mydb", DatabaseConfig::default()).await?;

let db = sys.get_database_by_name("mydb").unwrap();
// ... use db ...

sys.close().await?;
```

## Concurrency Model

- **MVCC**: Each transaction reads a consistent snapshot at `begin_ts`
- **OCC**: Optimistic concurrency control validates read sets at commit time
- **Single-writer**: CommitCoordinator serializes commits (no lock contention)
- **Two-task commit**: Writer task (steps 1-5) + Replication task (steps 6-11)
- **Send-safe**: `Database` is `Send + Sync`, transaction futures are `Send`

## Background Tasks

- **Checkpoint**: Periodic dirty page flush + WAL checkpoint record
- **Vacuum**: Removes old MVCC versions once safe
- **Index Builder**: Builds newly-created indexes in the background

## Layer Dependencies

```text
L6 (exdb) -> L5 (exdb-tx) -> L4 (exdb-query) -> L3 (exdb-docstore) -> L2 (exdb-storage) -> L1 (exdb-core)
```

L6 implements L5's `IndexResolver` and `CatalogMutationHandler` traits,
bridging the transaction engine with the catalog and storage layers.

## Testing

```bash
cargo test -p exdb
```

98 tests across unit tests (config, catalog cache, index resolver,
catalog tracker, catalog persistence, catalog recovery, subscriptions,
system database) and integration tests (lifecycle, DDL, CRUD, read-your-writes,
readonly enforcement, snapshot isolation, concurrent readers/writers across
tokio tasks, cross-collection isolation, file-backed durability).
