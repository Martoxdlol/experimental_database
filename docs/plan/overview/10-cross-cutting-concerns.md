# Cross-Cutting Concerns

## Latch Hierarchy (Deadlock Prevention)

All locks are acquired in this strict order. Never acquire a higher-numbered lock while holding a lower-numbered lock in reverse.

```
1. Writer lock         (tokio::sync::Mutex — outermost, async)
2. Page table RwLock   (parking_lot::RwLock — brief, lookup/insert)
3. Frame RwLock        (parking_lot::RwLock — per-frame, page access)
```

**Rules:**
- Multiple frame locks: acquire in ascending `page_id` order
- Page table lock is never held while acquiring a frame lock or performing I/O
- Frame locks are synchronous and never held across `.await` points
- Writer lock is always outermost — no synchronous latch held while awaiting it

## Error Handling Strategy

| Layer | Error Type | Strategy |
|-------|-----------|----------|
| L1 | `EncodingError` | Return Result, let caller decide |
| L2 | `StorageError` | I/O errors, checksum failures, page corruption |
| L3 | `DocStoreError` | Wraps StorageError + domain errors (doc not found, tombstone) |
| L4 | `QueryError` | Invalid range, index not ready, limit exceeded |
| L5 | `TxError` | Conflict, timeout, read limit exceeded |
| L6 | `DatabaseError` | Wraps lower-layer errors, adds lifecycle errors (not open, replica readonly) |
| L7 | `ReplicationError` | Connection failures, WAL gaps, snapshot failures |
| L8 | `ProtocolError` → error codes | Maps internal errors to wire error codes |

All errors use `anyhow::Result` internally. L8 maps to structured error codes at the protocol boundary.

## Checksum Strategy

| Data | Algorithm | When Computed | When Verified |
|------|-----------|---------------|---------------|
| Page data | CRC-32C | On flush to disk | Every buffer pool cache miss |
| WAL records | CRC-32C | On write | Every read (recovery, replication) |
| File header | CRC-32C | On checkpoint | On database open |
| DWB header | CRC-32C | On checkpoint | On recovery |

CRC-32C is hardware-accelerated via SSE 4.2 / ARM CRC instructions. ~1% CPU overhead.

## Background Task Coordination

| Task | Runs As | Modifies Pages? | Coordination |
|------|---------|-----------------|--------------|
| CommitCoordinator | Dedicated tokio task | Yes (via ExclusivePageGuard) | Receives requests via mpsc channel |
| WAL Writer | Dedicated tokio task | No (file append only) | Group commit via mpsc channel |
| Checkpoint | Background tokio task | Yes (DWB + scatter-write) | Brief writer lock for snapshot |
| Vacuum | Background tokio task | Yes (removes entries) | Submits through writer channel |
| Index Build | Background tokio task | Yes (inserts entries) | Submits through writer channel |
| Replication Server (L7) | Dedicated tokio task | No (read-only WAL streaming) | Notified via broadcast channel |
| Replica Client (L7) | Dedicated tokio task | Yes (applies WAL records) | Submits through writer channel |

**All page modifications funnel through the single writer** (except checkpoint's DWB/scatter-write, which is safe due to LSN-based mark-clean checking).

## Resource Limits

| Resource | Scope | Default | Configurable |
|----------|-------|---------|-------------|
| Buffer pool memory | Per database | 256 MB | `memory_budget` |
| Page size | Per database | 8 KB | `page_size` (immutable after creation) |
| Max document size | Per database | 16 MB | `max_doc_size` |
| WAL segment size | Per database | 64 MB | `wal_segment_size` |
| Max message size | Server-wide (L8) | 16 MB | `max_message_size` |
| Transaction idle timeout | Per database | 30s | `tx_idle_timeout` |
| Transaction max lifetime | Per database | 5 min | `tx_max_lifetime` |
| Read set max intervals | Per database | 4,096 | `max_intervals` |
| Read set max scanned bytes | Per database | 64 MB | `max_scanned_bytes` |
| Read set max scanned docs | Per database | 100,000 | `max_scanned_docs` |
| WAL retention for replication | Per database | 1 GB | `wal_retention_max_size` |
| WAL retention max age | Per database | 24h | `wal_retention_max_age` |

## Dependency Graph (Crate-Level)

```
                +----------+
                |  tokio    |  async runtime
                +-----+----+
                      |
  +-------------------+-------------------+
  |                   |                   |
  v                   v                   v
parking_lot       crc32fast            bson
(sync locks)      (checksums)       (encoding)
                      |
                      v
                serde_json
               (JSON support)
```

External dependencies:
- `tokio` — async runtime with full features
- `parking_lot` — synchronous RwLock (no-await, low overhead)
- `crc32fast` — hardware-accelerated CRC-32C
- `bson` — BSON encoding/decoding
- `serde_json` — JSON encoding/decoding
- `anyhow` — error handling
- `jsonwebtoken` — JWT validation (L8 only)

## No-Steal, Force Policy

The buffer pool follows a **no-steal, force** policy:
- **No-steal**: dirty pages from uncommitted transactions are never in the buffer pool (mutations are buffered in the write set until commit)
- **Force**: all mutations are written to WAL before commit returns (WAL fsync)

This means recovery only needs **redo** (replay committed WAL records). No undo phase is ever needed. This simplifies crash recovery significantly.
