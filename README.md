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
  server/         Standalone database server (TCP/TLS/QUIC/WebSocket)
  cli/            Embedded and TCP JSON command-line client
  studio/         Web UI for browsing and managing databases

crates/database/examples/
  embedded.rs       Embed the database in a Rust application
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

# Run compile-checked embedded examples
cargo run -p exdb --example embedded
cargo run -p exdb --example subscriptions

# Run the TCP server
cargo run -p exdb-server -- --data-root ./data --listen-tcp 127.0.0.1:5200 --request-queue-capacity 1024

# Use the embedded CLI
cargo run -p exdb-cli -- --data-path ./data/app list-collections

# Use high-level CLI commands against the TCP server
cargo run -p exdb-cli -- --connect 127.0.0.1:5200 create-database app
cargo run -p exdb-cli -- --connect 127.0.0.1:5200 --database app list-collections

# Open an interactive CLI session
cargo run -p exdb-cli -- --connect 127.0.0.1:5200 --database app repl

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
use exdb::{Database, DatabaseConfig, TransactionOptions, TransactionResult};
use serde_json::json;

let db = Database::open("./mydata", DatabaseConfig::default(), None).await?;

let mut schema = db.begin(TransactionOptions::default())?;
schema.create_collection("users").await?;
assert!(matches!(
    schema.commit().await?,
    TransactionResult::Success { .. }
));

let mut write = db.begin(TransactionOptions::default())?;
write.insert("users", json!({"name": "Alice", "age": 30})).await?;
assert!(matches!(
    write.commit().await?,
    TransactionResult::Success { .. }
));

let mut read = db.begin(TransactionOptions::readonly())?;
let results = read
    .query("users", "_created_at", &[], None, None, None)
    .await?;
read.rollback();
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
| `exdb` | L6 | `Database` struct, catalog cache, collection/index management, mutation/readonly transactions, subscriptions, snapshot export/restore, resource-limit enforcement, integrity check/repair, usage reporting, and compile-checked embedded examples |
| `exdb-replication` | L7 | Cluster membership/quorum tracking, framed peer protocol, live per-peer connections, `PeerMesh` TCP startup/reconnect/routing, retained-WAL catch-up streaming with replica attach trigger and gap signaling, recovery-tier selection, primary retained-WAL floor handshake advertisement, direct snapshot request when the handshake proves retained WAL cannot cover local progress, catch-up-gap fallback to quorum-checked snapshot streaming, promotion transport with typed document read/write payloads and Subscribe retry metadata, L8 replica document-write routing, collection/index/database DDL promotion intents, and a server primary commit/management handler, quorum-checked snapshot chunk transport with injectable sinks, a primary `ReplicationHook` foundation, durable generation plus replica source-LSN progress storage, replica-progress plus optional size/age WAL retention wiring, server database-backed WAL apply and snapshot source/sink bridges, JSON-configured per-database server mesh startup, fresh/existing replica snapshot registration/replacement, and multi-database fresh snapshot registration with durable registry entries; richer recovery orchestration and broader remote multi-node E2E still pending |
| `exdb-wire` | L8 | Frame format, JSON/BSON/Protobuf message schemas, JWT auth helpers with active expiration enforcement, session core with CRUD/query responses carrying query IDs, JSON `_meta.types` document-body and query-predicate metadata, inherited server default database config, database/collection/index management, replica document writes plus DDL promotion, pushed invalidations, index-ready notifications, Subscribe continuations/retries, TCP/TLS/QUIC/WebSocket transport core with bounded request queues, async management dispatch, same-transaction ordering with different-transaction concurrency, request/notification priority rotation, and response write timeouts, a runnable TCP/TLS/QUIC/ws/wss server binary, and JSON CLI support for embedded, TCP server, and REPL commands |

## Current Status

Layers 1–6 are substantially implemented for embedded use: BSON-backed core encoding with JSON `_meta.types` bytes/id document metadata, native BSON embedded document APIs, storage, MVCC document indexes, query planning/execution, OCC transactions, catalog persistence/recovery, subscriptions, startup recovery, checkpointed snapshot export/restore, integrity checks and repairs, usage reporting, compile-checked embedded examples, and durability coverage.

Layer 7 replication has quorum-aware cluster membership, a bounded framed peer protocol, live per-peer connections, `PeerMesh` TCP startup/reconnect/routing, peer-backed WAL sends, retained-WAL catch-up streaming with replica attach trigger and explicit gap signaling, recovery-tier selection, primary retained-WAL floor handshake advertisement, direct snapshot request when the handshake proves retained WAL cannot cover local progress, automatic catch-up-gap fallback to snapshot streaming when a primary snapshot source is installed, promotion transport with typed document `ReadSet`/`WriteSet` payloads committed through the primary database path, L8 replica document-write routing to the mesh `PromotionClient` including Subscribe success/retry handling, collection/index/database DDL promotion intents executed on the primary, quorum-checked snapshot chunk transport with injectable sinks, a primary hook foundation, durable database-backed TxCommit apply for replicas, persisted replica generation and primary-LSN progress, replica-progress plus optional size/age WAL retention wiring, server WAL apply plus snapshot source/sink bridges covered by real TCP mesh traffic, JSON-configured per-database server mesh startup, fresh/existing replica snapshot registration/replacement, multi-database fresh snapshot registration with durable registry entries, and configured multi-database routing/read fencing. Richer recovery orchestration and broader remote multi-node E2E remain pending. Layer 8 has a wire frame/message/auth/session/TCP/TLS/QUIC/WebSocket server foundation with JSON/BSON/Protobuf payloads, JWT active-expiration enforcement, core transaction/data operations, query IDs on read responses, JSON `_meta.types` document-body and query-predicate metadata, inherited server default database config, replica-role document write plus database/collection/index DDL promotion, pushed invalidations, pushed index-ready notifications, materialized Subscribe continuations/retries, bounded per-connection request queues, async management dispatch, different-transaction concurrent execution with same-transaction ordering, scheduler resource accounting, request/notification priority rotation, response write timeouts for slow clients, end-to-end management/index/drop lifecycle coverage, a runnable TCP/TLS/QUIC/ws/wss server, and a CLI for embedded database operations, high-level TCP server commands, interactive REPL sessions, and raw JSON text server requests. Larger mixed protocol validation and production hardening remain pending.

## Design Documents

- [Full specification](docs/DESIGN.md) — complete technical design
- [Architecture](docs/ARCHITECTURE.md) — monorepo structure and crate boundaries
- [Layer overviews](docs/plan/overview/) — per-layer architecture docs
- [Storage design](docs/plan/storage/) — detailed storage engine specs
