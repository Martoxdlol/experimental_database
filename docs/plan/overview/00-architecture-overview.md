# Architecture Overview

## System Layers

The database is organized into 7 distinct layers, from bottom (disk) to top (client-facing API). Each layer depends only on the layers below it.

```
┌─────────────────────────────────────────────────────────┐
│  Layer 7: API / Protocol                                │
│  (message framing, JSON/BSON/Protobuf, auth, sessions)  │
├─────────────────────────────────────────────────────────┤
│  Layer 6: Distributed / Replication                     │
│  (WAL streaming, promotion, replica recovery)           │
├─────────────────────────────────────────────────────────┤
│  Layer 5: Transaction Manager                           │
│  (OCC, read/write sets, subscriptions, commit protocol) │
├─────────────────────────────────────────────────────────┤
│  Layer 4: Query Engine                                  │
│  (query planning, index scan, post-filter, terminals)   │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Indexing                                      │
│  (B+ tree ops, key encoding, version resolution,        │
│   background index build, array indexing)               │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Storage Engine                                │
│  (WAL, buffer pool, page store, catalog, checkpoint,    │
│   free space, heap, double-write buffer, vacuum)        │
├─────────────────────────────────────────────────────────┤
│  Layer 1: Core Types & Encoding                         │
│  (DocId/ULID, FieldPath, Filter, BSON encoding,         │
│   type ordering, scalar types)                          │
└─────────────────────────────────────────────────────────┘
```

## Module Map

```
src/
├── types.rs          ── Layer 1: Core types
├── encoding.rs       ── Layer 1: BSON/JSON encoding, type hints
├── ulid.rs           ── Layer 1: ULID generation + Crockford Base32
│
├── storage/
│   ├── mod.rs        ── Layer 2: StorageEngine facade
│   ├── page.rs       ── Layer 2: Page format (slotted pages)
│   ├── buffer_pool.rs── Layer 2: Buffer pool + frame management
│   ├── wal.rs        ── Layer 2: WAL writer, segments, records
│   ├── heap.rs       ── Layer 2: External heap + overflow pages
│   ├── free_list.rs  ── Layer 2: Free page list management
│   ├── dwb.rs        ── Layer 2: Double-write buffer
│   ├── checkpoint.rs ── Layer 2: Checkpoint protocol
│   ├── recovery.rs   ── Layer 2: Crash recovery + integrity check
│   └── vacuum.rs     ── Layer 2: Version cleanup
│
├── catalog.rs        ── Layer 2/3: Catalog B-tree + in-memory cache
│
├── index/
│   ├── mod.rs        ── Layer 3: Index facade
│   ├── btree.rs      ── Layer 3: B+ tree implementation
│   ├── key_encoding.rs─ Layer 3: Order-preserving key encoding
│   ├── primary.rs    ── Layer 3: Primary (clustered) index ops
│   ├── secondary.rs  ── Layer 3: Secondary index ops + version resolution
│   └── builder.rs    ── Layer 3: Background index building
│
├── query/
│   ├── mod.rs        ── Layer 4: Query pipeline facade
│   ├── planner.rs    ── Layer 4: Query planning (source selection)
│   ├── scan.rs       ── Layer 4: Index scan execution
│   ├── filter.rs     ── Layer 4: Post-filter evaluation
│   └── range.rs      ── Layer 4: Index range expression encoding
│
├── tx/
│   ├── mod.rs        ── Layer 5: Transaction manager
│   ├── read_set.rs   ── Layer 5: Read set intervals
│   ├── write_set.rs  ── Layer 5: Write set buffer
│   ├── commit_log.rs ── Layer 5: Commit log + OCC validation
│   ├── timestamp.rs  ── Layer 5: Monotonic timestamp allocator
│   └── subs.rs       ── Layer 5: Subscription registry + invalidation
│
├── replication/
│   ├── mod.rs        ── Layer 6: Replication facade
│   ├── primary.rs    ── Layer 6: WAL streaming server
│   ├── replica.rs    ── Layer 6: Replica client + catch-up
│   ├── promotion.rs  ── Layer 6: Transaction promotion protocol
│   └── snapshot.rs   ── Layer 6: Full reconstruction (Tier 3)
│
├── protocol/
│   ├── mod.rs        ── Layer 7: Protocol facade
│   ├── framing.rs    ── Layer 7: Frame format (JSON text + binary)
│   ├── messages.rs   ── Layer 7: Message type definitions
│   ├── session.rs    ── Layer 7: Connection state machine
│   └── auth.rs       ── Layer 7: JWT authentication
│
├── server.rs         ── Layer 7: Transport listeners (TCP/TLS/QUIC/WS)
├── database.rs       ── Integration: Database instance (ties layers 2-5)
├── system.rs         ── Integration: System database + multi-DB management
├── config.rs         ── Configuration types
├── lib.rs            ── Public API re-exports
└── main.rs           ── Binary entry point
```

## Cross-Layer Data Flow Summary

| Flow | Path (layers traversed) |
|------|------------------------|
| Point read (get by ID) | 7 → 5 → 4 → 3 → 2 |
| Index scan query | 7 → 5 → 4 → 3 → 2 |
| Insert document | 7 → 5 → (4 for read-your-writes) → 3 → 2 |
| Commit transaction | 7 → 5 → 2(WAL) → 3(index update) → 2(page store) → 5(invalidation) → 6(replication) |
| Subscription invalidation | 2(WAL receive) → 3(index apply) → 5(subscription check) → 7(push notification) |
| Crash recovery | 2(DWB restore) → 2(WAL replay) → 3(index rebuild) → catalog rebuild |
