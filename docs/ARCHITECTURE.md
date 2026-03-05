# Monorepo Architecture

Cargo workspace with one crate per architectural layer and an `apps/` directory for binaries and UI.

## Workspace Layout

```
experimental_database/
├── Cargo.toml                     # Workspace root
├── docs/                          # Design docs (unchanged)
│
├── crates/                        # Library crates (one per layer)
│   ├── core/                      # L1 — Core Types & Encoding
│   ├── storage/                   # L2 — Storage Engine (existing)
│   ├── docstore/                  # L3 — Document Store & Indexing
│   ├── query/                     # L4 — Query Engine
│   ├── tx/                        # L5 — Transaction Manager
│   ├── database/                  # L6 — Database Instance (public API)
│   ├── replication/               # L7 — Replication Transport
│   └── wire/                      # L8 — API & Protocol (framing, sessions, auth)
│
├── apps/                          # Binary crates
│   ├── server/                    # DB server binary (TCP/WS, wraps L6+L7+L8)
│   ├── cli/                       # CLI shell (REPL for the database)
│   └── studio/                    # Web UI (Rust backend + frontend assets)
│
└── examples/                      # Standalone example programs
    ├── embedded.rs                # Embed the DB in a Rust app
    ├── replication.rs             # Primary + replica setup
    └── subscriptions.rs           # Live query subscriptions
```

## Dependency Graph

Strict bottom-up. No cycles.

```
apps/server ──┐
apps/cli ─────┤
apps/studio ──┤
              ▼
         ┌─────────┐
         │ wire     │  L8  (optional)
         │ (api)    │
         └────┬─────┘
              │
         ┌────┴──────────────┐
         │                   │
    ┌────▼─────┐    ┌───────▼────────┐
    │replication│ L7 │   database     │ L6  ◄── primary public API
    │(optional) │    │                │
    └────┬──────┘    └──┬──┬──┬──┬───┘
         │             │  │  │  │
         │        ┌────┘  │  │  └────┐
         │        ▼       ▼  ▼       ▼
         │     ┌────┐  ┌────┐  ┌─────────┐
         │     │ tx │  │query│  │docstore │
         │     │ L5 │  │ L4 │  │  L3     │
         │     └──┬─┘  └──┬─┘  └───┬─────┘
         │        │       │        │
         │        └───┬───┘        │
         │            │    ┌───────┘
         │            ▼    ▼
         │        ┌──────────┐
         └───────►│ storage  │ L2
                  └────┬─────┘
                       │
                  ┌────▼─────┐
                  │  core    │ L1
                  └──────────┘
```

## Crate Details

### `crates/core` — Layer 1

Pure types and encoding. Zero I/O. No async.

```
crates/core/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── types.rs          # DocId, CollectionId, IndexId, Ts, Scalar, TypeTag
    ├── field_path.rs     # FieldPath parsing and traversal
    ├── filter.rs         # Filter AST, RangeExpr
    ├── encoding.rs       # Order-preserving scalar encoding, BSON helpers
    ├── ulid.rs           # ULID generation + Base32
    └── patch.rs          # RFC 7396 JSON merge-patch
```

**Dependencies**: `serde`, `serde_json` only.
**Crate name**: `exdb-core`

---

### `crates/storage` — Layer 2

Generic storage engine. Operates on `&[u8]` keys/values. No document or MVCC knowledge.

```
crates/storage/                    # (existing code, move into crates/)
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── backend.rs        # PageStorage & WalStorage traits + File/Memory impl
    ├── page.rs           # SlottedPage, PageHeader, CRC-32C
    ├── buffer_pool.rs    # Clock eviction, RAII guards
    ├── free_list.rs      # LIFO page allocator
    ├── wal.rs            # Write-ahead log, group commit
    ├── btree.rs          # B+ tree (insert, get, delete, scan)
    ├── heap.rs           # Large value overflow chains
    ├── dwb.rs            # Double-write buffer
    ├── checkpoint.rs     # Flush dirty pages
    ├── recovery.rs       # DWB restore + WAL replay
    ├── vacuum.rs         # Batch entry removal
    ├── catalog_btree.rs  # Collection/index catalog
    ├── posting.rs        # Posting list codec
    ├── engine.rs         # StorageEngine facade
    ├── error.rs
    └── util.rs
```

**Dependencies**: `zerocopy`, `crc32fast`, `parking_lot`, `tokio`, `thiserror`
**Crate name**: `exdb-storage`

---

### `crates/docstore` — Layer 3

MVCC-aware document storage and indexing on top of the raw storage engine.

```
crates/docstore/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── key_encoding.rs         # Scalar → order-preserving bytes
    ├── primary_index.rs        # PrimaryIndex (doc_id || inv_ts → value)
    ├── secondary_index.rs      # SecondaryIndex (encoded_val || doc_id || inv_ts)
    ├── version_resolution.rs   # MVCC version resolver (scan, skip tombstones)
    ├── array_indexing.rs       # Array index entry expansion
    └── index_builder.rs        # Background index build (snapshot + catch-up)
```

**Dependencies**: `exdb-core`, `exdb-storage`
**Crate name**: `exdb-docstore`

---

### `crates/query` — Layer 4

Query planning and execution.

```
crates/query/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── planner.rs        # Plan selection (PrimaryGet / IndexScan / TableScan)
    ├── range_encoder.rs  # Filter → byte-range intervals
    ├── scan.rs           # Scan execution pipeline
    ├── post_filter.rs    # In-memory filter evaluation
    └── merge.rs          # Read-your-writes merge
```

**Dependencies**: `exdb-core`, `exdb-storage`, `exdb-docstore`
**Crate name**: `exdb-query`

---

### `crates/tx` — Layer 5

Transaction lifecycle, OCC, and subscriptions.

```
crates/tx/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── timestamp.rs      # TsAllocator (monotonic)
    ├── read_set.rs       # ReadSet with interval merging
    ├── write_set.rs      # WriteSet + index delta computation
    ├── commit_log.rs     # CommitLog (recent writes for OCC check)
    ├── occ.rs            # OCC validation
    ├── subscriptions.rs  # SubscriptionRegistry, invalidation
    └── commit.rs         # CommitCoordinator (single-writer)
```

**Dependencies**: `exdb-core`, `exdb-storage`, `exdb-docstore`, `exdb-query`
**Crate name**: `exdb-tx`

---

### `crates/database` — Layer 6

**Primary public API.** This is what users import for embedded usage.

```
crates/database/
├── Cargo.toml
└── src/
    ├── lib.rs               # Re-exports Database, Config, sessions
    ├── database.rs          # Database (open, close, begin_*, create_collection, etc.)
    ├── catalog_cache.rs     # In-memory catalog (dual-indexed by name & id)
    ├── system_database.rs   # Multi-database registry
    ├── config.rs            # DatabaseConfig, TransactionConfig
    └── replication_hook.rs  # trait ReplicationHook + NoReplication default
```

**Dependencies**: `exdb-core`, `exdb-storage`, `exdb-docstore`, `exdb-query`, `exdb-tx`
**Crate name**: `exdb` (the "main" crate users depend on)

---

### `crates/replication` — Layer 7 (optional)

```
crates/replication/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── primary_server.rs   # PrimaryReplicator (implements ReplicationHook)
    ├── replica_client.rs   # ReplicaClient (WAL consumer)
    ├── promotion.rs        # Write promotion (replica → primary)
    ├── snapshot.rs         # Full snapshot transfer
    └── recovery_tiers.rs   # Tier selection (incremental vs snapshot)
```

**Dependencies**: `exdb` (L6), `exdb-storage` (WAL access), `tokio`
**Crate name**: `exdb-replication`

---

### `crates/wire` — Layer 8 (optional)

Network protocol, sessions, auth. Named `wire` to avoid confusion with `api` (which could mean the Rust API).

```
crates/wire/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── frame.rs          # Frame format (JSON text + binary auto-detect)
    ├── messages.rs       # Client/server message types, parse/serialize
    ├── session.rs        # Session state machine
    ├── auth.rs           # JWT / token validation
    └── transport.rs      # TCP/TLS/WebSocket listener setup
```

**Dependencies**: `exdb` (L6), `tokio`, `tokio-tungstenite`, `jsonwebtoken`
**Crate name**: `exdb-wire`

---

## Apps

### `apps/server`

Database server binary. Combines L6 + L7 + L8 into a runnable process.

```
apps/server/
├── Cargo.toml
└── src/
    └── main.rs           # CLI args, config loading, start server
```

**Dependencies**: `exdb`, `exdb-wire`, `exdb-replication`, `clap`, `tracing`
**Binary**: `exdb-server`

---

### `apps/cli`

Interactive REPL / command-line shell for the database.

```
apps/cli/
├── Cargo.toml
└── src/
    └── main.rs           # REPL loop, connect to server or open embedded
```

**Dependencies**: `exdb`, `exdb-wire`, `clap`, `rustyline`
**Binary**: `exdb-cli`

---

### `apps/studio`

Web-based UI for browsing and managing the database. Rust backend serves a SPA frontend.

```
apps/studio/
├── Cargo.toml
├── src/
│   └── main.rs           # Axum server, serves API + static assets
├── frontend/             # SPA (React/TypeScript, built separately)
│   ├── package.json
│   ├── src/
│   │   ├── App.tsx
│   │   ├── components/   # Collection browser, doc viewer, query builder, etc.
│   │   └── ...
│   └── dist/             # Built assets (embedded into binary via include_dir)
└── build.rs              # Optional: trigger frontend build
```

**Dependencies**: `exdb`, `exdb-wire`, `axum`, `tower-http` (serve static), `include_dir`
**Binary**: `exdb-studio`

---

## Root Cargo.toml

```toml
[workspace]
resolver = "2"
members = [
    "crates/core",
    "crates/storage",
    "crates/docstore",
    "crates/query",
    "crates/tx",
    "crates/database",
    "crates/replication",
    "crates/wire",
    "apps/server",
    "apps/cli",
    "apps/studio",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
license = "MIT"

[workspace.dependencies]
# Internal crates
exdb-core       = { path = "crates/core" }
exdb-storage    = { path = "crates/storage" }
exdb-docstore   = { path = "crates/docstore" }
exdb-query      = { path = "crates/query" }
exdb-tx         = { path = "crates/tx" }
exdb            = { path = "crates/database" }
exdb-replication = { path = "crates/replication" }
exdb-wire       = { path = "crates/wire" }

# Shared external deps
tokio         = { version = "1", features = ["full"] }
serde         = { version = "1", features = ["derive"] }
serde_json    = "1"
thiserror     = "2"
tracing       = "0.1"
parking_lot   = "0.12"
zerocopy      = { version = "0.8", features = ["derive"] }
crc32fast     = "1"
```

## Build & Test

```bash
# Build everything
cargo build --workspace

# Test a single layer
cargo test -p exdb-storage

# Test all
cargo test --workspace

# Run the server
cargo run -p exdb-server

# Run the CLI
cargo run -p exdb-cli

# Run studio
cargo run -p exdb-studio
```

## Migration Path (from current state)

1. Create `crates/` and `apps/` directories
2. Move `storage/` → `crates/storage/`, rename package to `exdb-storage`
3. Create `crates/core/` — extract pure types (currently some live in storage)
4. Stub out remaining crates with `lib.rs` and correct dependency declarations
5. Build bottom-up: core → storage → docstore → query → tx → database
6. Wire and replication crates come last
7. Apps are built once `database` (L6) is functional

## Design Principles

- **Each crate compiles independently** — `cargo test -p exdb-storage` must work with no L3+ types
- **`exdb` (L6) is the user-facing crate** — downstream apps depend on `exdb`, not internal crates
- **Internal crates are `pub` but not part of the stability contract** — only `exdb` has a stable API
- **Feature flags for optional layers** — `exdb` can re-export replication/wire behind features
- **Shared workspace deps** — version pinned once in root `Cargo.toml`, used everywhere
