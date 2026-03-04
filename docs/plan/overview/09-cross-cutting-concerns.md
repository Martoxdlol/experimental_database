# Cross-Cutting Concerns

## Concurrency Model

### Latch Hierarchy (Deadlock Prevention)

```
  Level 1: Writer Lock (tokio::sync::Mutex)
      │     Serializes commits + checkpoint snapshot
      │
  Level 2: Page Table RwLock (parking_lot::RwLock)
      │     Brief: lookup/insert page mapping only
      │
  Level 3: Frame RwLock (parking_lot::RwLock, per-frame)
            Hold for page access duration
            Multiple frames: acquire in ascending page_id order
```

**Rules:**
- Never acquire a higher-level lock while holding a lower-level lock
- Frame locks are synchronous (never held across `.await`)
- Writer lock is async (held during WAL fsync via group commit)
- Page table lock is never held while acquiring frame locks or doing I/O

### Single Writer + Concurrent Readers

```
  ┌───────────────────────────────────────────────────────┐
  │                     Buffer Pool                       │
  │                                                       │
  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐  │
  │  │Frame│ │Frame│ │Frame│ │Frame│ │Frame│ │Frame│  │
  │  │ RwL │ │ RwL │ │ RwL │ │ RwL │ │ RwL │ │ RwL │  │
  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘  │
  │     │       │       │       │       │       │      │
  │  ┌──┴──┐ ┌──┴──┐ ┌──┴──┐ ┌──┴──┐ ┌──┴──┐ ┌──┴──┐  │
  │  │  S  │ │  S  │ │  X  │ │  S  │ │  S  │ │  S  │  │
  │  │Read1│ │Read2│ │Write│ │Read3│ │Read4│ │Read5│  │
  │  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘  │
  │                                                       │
  │  S = SharedPageGuard (multiple concurrent)            │
  │  X = ExclusivePageGuard (one writer, blocks S on same)│
  └───────────────────────────────────────────────────────┘
```

## Error Handling Strategy

| Layer | Error Type | Handling |
|---|---|---|
| Layer 1 | Encoding errors | `Result<T, EncodingError>` — propagate up |
| Layer 2 | I/O errors, corruption | `Result<T, StorageError>` — may trigger recovery |
| Layer 3 | B-tree errors | `Result<T, IndexError>` — propagate up |
| Layer 4 | Invalid query, scan errors | `Result<T, QueryError>` — return to client |
| Layer 5 | OCC conflict, timeout | `CommitResult::Conflict` — return to client |
| Layer 6 | Network errors | Retry with backoff; Tier escalation |
| Layer 7 | Protocol errors, auth | `ServerMessage::Error` — return to client |

All errors use `anyhow::Result` for internal propagation with error context. Public API errors use typed enums mapping to error codes (§7.9).

## Checksum Strategy

| What | Algorithm | When Computed | When Verified |
|---|---|---|---|
| Page data | CRC-32C | On page flush to disk | On every page read from disk (cache miss) |
| WAL records | CRC-32C | On WAL write | On WAL replay, replication receive, integrity check |
| File header | CRC-32C | On checkpoint | On database open |
| DWB header | CRC-32C | On DWB write | On DWB recovery |

CRC-32C chosen for hardware acceleration (SSE 4.2 / ARM CRC).

## Background Task Coordination

```
  ┌─────────────────────────────────────┐
  │         Writer Task (single)        │
  │                                     │
  │  Receives via bounded channel:      │
  │  • Client commit requests           │
  │  • Vacuum mutations                 │◀─── VacuumTask
  │  • Index build inserts              │◀─── IndexBuilder
  │  • Catalog mutations                │
  └───────────────┬─────────────────────┘
                  │
          ┌───────┴──────────┐
          │ WAL Writer Task  │  (group commit)
          └───────┬──────────┘
                  │
          ┌───────┴──────────┐
          │ Checkpoint Task  │  (periodic)
          │ • snapshot dirty │
          │   frames under   │
          │   writer lock    │
          │ • DWB + scatter  │
          │   without lock   │
          └──────────────────┘
```

## Resource Limits (Per Database)

| Resource | Config Key | Default | Enforcement Point |
|---|---|---|---|
| Buffer pool memory | `memory_budget` | 256 MB | `BufferPool::new()` |
| Max document size | `max_doc_size` | 16 MB | `Session::handle_insert/replace/patch()` |
| Max message size | `max_message_size` | 16 MB | `FrameReader::read_frame()` |
| Read set intervals | `max_intervals` | 4,096 | `ReadSet::add_interval()` |
| Scanned bytes | `max_scanned_bytes` | 64 MB | `execute_query()` |
| Scanned documents | `max_scanned_docs` | 100,000 | `execute_query()` |
| Transaction idle timeout | `tx_idle_timeout` | 30s | Session timeout task |
| Transaction max lifetime | `tx_max_lifetime` | 5 min | Session timeout task |
| WAL retention size | `wal_retention_max_size` | 1 GB | Checkpoint reclamation |
| WAL retention age | `wal_retention_max_age` | 24h | Checkpoint reclamation |

## Dependency Graph (Crates)

```
tokio          ── async runtime
serde / serde_json ── JSON serialization
bson           ── BSON encoding/decoding
anyhow         ── error handling
parking_lot    ── synchronous RwLock/Mutex (for frame locks)
crc32fast      ── CRC-32C (hardware-accelerated)
ulid           ── ULID generation (or custom impl)
jsonwebtoken   ── JWT validation
rustls / quinn ── TLS / QUIC transport
tokio-tungstenite ── WebSocket
lz4_flex       ── LZ4 compression (binary frame flag)
tempfile       ── dev dependency (tests)
```
