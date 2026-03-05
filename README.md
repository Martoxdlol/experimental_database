# exdb

An embedded JSON document database written in Rust.

MVCC transactions, B+ tree indexes, write-ahead logging, live query subscriptions, and optional replication — all in a single library you can embed in any Rust application, or run as a standalone server.

## Architecture

The system is split into 8 layers, each in its own crate. Layers 1–6 form the embedded database. Layers 7–8 are optional and add networking.

```
┌─────────────────────────────────────────────────┐
│  L8  exdb-wire        Protocol, sessions, auth  │  optional
│  L7  exdb-replication WAL streaming replicas    │  optional
├─────────────────────────────────────────────────┤
│  L6  exdb             Database (public API)     │  ◄── import this
│  L5  exdb-tx          Transactions & OCC        │
│  L4  exdb-query       Query planning & scans    │
│  L3  exdb-docstore    MVCC documents & indexes  │
│  L2  exdb-storage     Pages, B+ tree, WAL, heap │
│  L1  exdb-core        Types & encoding          │
└─────────────────────────────────────────────────┘
```

Dependencies flow strictly downward. No cycles.

## Repository Layout

```
crates/
  core/           L1  Pure types, encoding, filters (no I/O)
  storage/        L2  Generic storage engine (B+ tree, WAL, buffer pool)
  docstore/       L3  MVCC document store and secondary indexing
  query/          L4  Query planner and execution
  tx/             L5  Transaction manager, OCC, subscriptions
  database/       L6  Database instance — the main public API
  replication/    L7  WAL streaming replication (optional)
  wire/           L8  Network protocol and sessions (optional)

apps/
  server/         Standalone database server (TCP/WebSocket)
  cli/            Interactive REPL shell
  studio/         Web UI for browsing and managing databases

examples/
  embedded.rs     Embed the database in a Rust application
  replication.rs  Primary + replica setup
  subscriptions.rs  Live query subscriptions

docs/
  DESIGN.md           Full technical specification
  ARCHITECTURE.md     Monorepo structure and crate details
  plan/overview/      Layer-by-layer architecture docs
  plan/storage/       Storage engine design docs
```

## Quick Start

```bash
# Build everything
cargo build --workspace

# Run all tests
cargo test --workspace

# Test a single crate
cargo test -p exdb-storage

# Run the server
cargo run -p exdb-server

# Run the studio UI
cargo run -p exdb-studio
```

### Embedded Usage

Add `exdb` to your `Cargo.toml`:

```toml
[dependencies]
exdb = { path = "crates/database" }
```

```rust
use exdb::Database;

let db = Database::open("./mydata", Default::default()).await?;
db.create_collection("users")?;

let mut tx = db.begin_mutation()?;
tx.insert("users", serde_json::json!({"name": "Alice", "age": 30}))?;
tx.commit().await?;

let tx = db.begin_readonly();
let results = tx.query("users", &[])?;
db.close().await?;
```

## Crate Overview

| Crate | Layer | Description |
|---|---|---|
| `exdb-core` | L1 | `DocId`, `Scalar`, `Filter`, `FieldPath`, encoding, ULID |
| `exdb-storage` | L2 | Slotted pages, buffer pool (clock eviction), B+ tree, WAL (group commit), heap overflow, double-write buffer, checkpoint, crash recovery, vacuum |
| `exdb-docstore` | L3 | MVCC key layout (`doc_id \|\| inv_ts`), version resolution, primary/secondary indexes, background index builds |
| `exdb-query` | L4 | Query planning (primary get / index scan / table scan), range encoding, post-filtering, read-your-writes merge |
| `exdb-tx` | L5 | Timestamp allocation, read/write sets, OCC conflict detection, commit coordination, subscription invalidation |
| `exdb` | L6 | `Database` struct, catalog cache, collection/index management, mutation/readonly sessions |
| `exdb-replication` | L7 | Primary server, replica client, WAL streaming, snapshot transfer, promotion |
| `exdb-wire` | L8 | Frame format, message types, session state machine, JWT auth, TCP/WebSocket transport |

## Current Status

The **storage engine** (L2) is implemented with ~12k lines of Rust and 170 tests covering B+ tree operations, buffer pool, WAL, checkpoints, recovery, and vacuum.

Layers 1 and 3–8 are stubbed out and ready for implementation following the bottom-up build order defined in the design docs.

## Design Documents

- [Full specification](docs/DESIGN.md) — complete technical design
- [Architecture](docs/ARCHITECTURE.md) — monorepo structure and crate boundaries
- [Layer overviews](docs/plan/overview/) — per-layer architecture docs
- [Storage design](docs/plan/storage/) — detailed storage engine specs
