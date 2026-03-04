# Storage Engine

A WAL-first, page-oriented storage engine for an embedded document database, written in Rust.

## Features

- **B+ tree indexes** with insert, delete, cursor-based iteration, and range scans
- **Write-ahead log** with group commit and multi-segment rotation
- **Buffer pool** with clock eviction, per-frame RwLock, and positional I/O (`pread`/`pwrite`)
- **Crash recovery** via double-write buffer (torn write protection) + WAL replay
- **External heap** for large documents with overflow page chains
- **Order-preserving key encoding** — all comparisons use `memcmp`
- **Single-writer architecture** — mutations are serialized through an async writer task; reads are lock-free

## Architecture

```
                    ┌─────────────────────────┐
                    │     StorageEngine        │
                    │  (open / shutdown / API) │
                    └────┬──────────┬──────────┘
                         │          │
              ┌──────────▼──┐  ┌────▼─────────────┐
              │ Writer Task │  │ Readers (any task)│
              │ (sequential)│  │ (concurrent)      │
              └──┬──────┬───┘  └────┬──────────────┘
                 │      │           │
        ┌────────▼─┐ ┌──▼──────────▼──┐
        │   WAL    │ │  Buffer Pool    │
        │ (append) │ │ (shared pages)  │
        └────┬─────┘ └───────┬────────┘
             │               │
        ┌────▼─────┐   ┌─────▼────┐
        │wal/*.wal │   │ data.db  │
        └──────────┘   └──────────┘
```

### Write path

1. Client submits a `CommitRequest` via mpsc channel
2. Writer task serializes the WAL record and appends it (group commit with fsync)
3. Writer applies mutations to B-tree pages in the buffer pool
4. Writer publishes updated catalog snapshot (lock-free via ArcSwap)
5. Response is sent back to the client

### Read path

1. Client loads the catalog snapshot (lock-free `ArcSwap` read)
2. Client fetches pages from the buffer pool (shared lock per frame)
3. On cache miss, the page is read from `data.db` via positional I/O

### Recovery

On startup:

1. **DWB repair** — if `data.dwb` exists, verify each page's CRC in `data.db`; restore torn pages from the DWB copy
2. **WAL replay** — from the checkpoint LSN in `meta.json`, replay all committed WAL records forward

## On-disk layout

```
<db_dir>/
  data.db         Page store (8 KiB pages by default)
  data.dwb        Double-write buffer (temporary, cleared after checkpoint)
  meta.json       Checkpoint LSN and database metadata
  wal/
    wal_000001_00000000.wal
    wal_000002_00000150.wal
    ...
```

## Page format

Every page uses a 32-byte slotted page header:

```
Offset  Size  Field
  0       4   page_id (u32 LE)
  4       1   page_type
  5       1   flags
  6       2   num_slots (u16 LE)
  8       2   free_space_start (u16 LE)
 10       2   free_space_end (u16 LE)
 12       4   prev_or_ptr (u32 LE) — sibling pointer / free list next / leftmost child
 16       4   reserved
 20       4   checksum (CRC-32C)
 24       8   lsn (u64 LE)
```

The slot directory follows the header (4 bytes per slot: offset + length). Cell data grows backward from the end of the page.

## Key encoding

Primary keys are 24 bytes: `doc_id[16] || inverted_timestamp[8]` (inverted = `u64::MAX - ts`, so the newest version sorts first).

Secondary index keys use type-tagged, order-preserving encoding:

| Type    | Tag  | Encoding |
|---------|------|----------|
| undefined | 0x00 | tag only |
| null    | 0x01 | tag only |
| int64   | 0x02 | XOR-flip for signed ordering |
| float64 | 0x03 | IEEE 754 total-order transform |
| boolean | 0x04 | `false=0x00`, `true=0x01` |
| string  | 0x05 | null-byte escaped, double-null terminated |
| bytes   | 0x06 | null-byte escaped, double-null terminated |
| array   | 0x07 | recursive element encoding |

## Usage

```rust
use storage::engine::{StorageEngine, DatabaseConfig};
use storage::catalog::CollectionConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a database
    let engine = StorageEngine::open(
        "my_db".as_ref(),
        DatabaseConfig::default(),
    ).await?;

    // Create a collection
    let col = engine.create_collection(
        "users",
        CollectionConfig::default(),
    ).await?;

    // Create a secondary index
    let idx = engine.create_index(
        col.collection_id,
        "email_idx",
        vec![vec!["email".into()]],
    ).await?;

    // Mark index ready after background build
    engine.mark_index_ready(idx.index_id).await?;

    // Read the catalog (lock-free)
    let catalog = engine.catalog();
    let users = catalog.get_collection_by_name("users").unwrap();
    println!("Collection: {} (root page: {:?})", users.name, users.primary_root_page);

    // Trigger a checkpoint
    engine.checkpoint().await?;

    // Graceful shutdown
    engine.shutdown().await?;
    Ok(())
}
```

## Configuration

```rust
DatabaseConfig {
    page_size: 8192,                    // bytes per page
    memory_budget: 256 * 1024 * 1024,   // 256 MiB buffer pool
    max_doc_size: 16 * 1024 * 1024,     // 16 MiB max document
    external_threshold: 4096,            // spill to heap above this size
    wal_config: WalConfig {
        target_segment_size: 64 * 1024 * 1024,  // 64 MiB per WAL segment
        channel_capacity: 1024,
    },
    checkpoint_config: CheckpointConfig {
        wal_size_threshold: 64 * 1024 * 1024,
        interval: Duration::from_secs(300),
    },
}
```

## Module overview

| Module | Description |
|--------|-------------|
| `types` | Core newtypes (`PageId`, `Lsn`, `DocId`, `Timestamp`) and constants |
| `page` | Slotted page format — variable-length cells with CRC-32C checksums |
| `buffer_pool` | Page cache with clock eviction and `pread`/`pwrite` I/O |
| `wal` | WAL segments, record serialization, async group-commit writer, reader |
| `btree` | B+ tree insert (with split), delete, cursor, range scan |
| `heap` | External heap for large documents, overflow page chains |
| `key_encoding` | Order-preserving scalar encoding (`memcmp`-comparable) |
| `catalog` | In-memory collection/index metadata with ArcSwap publishing |
| `free_list` | Free page linked list (intrusive, in page headers) |
| `file_header` | Page 0 header + shadow copy for crash safety |
| `checkpoint` | Fuzzy checkpoint with double-write buffer |
| `recovery` | Crash recovery: DWB repair + WAL replay |
| `engine` | `StorageEngine` facade — open, DDL, DML, shutdown |

## Testing

```bash
cargo test -p storage
```

98 tests cover all modules: page operations, buffer pool eviction, B-tree splits, WAL round-trips, checkpoint/recovery, and end-to-end engine operations including reopen-after-crash.
