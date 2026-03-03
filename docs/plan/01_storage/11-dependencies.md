# 11 — Dependencies and Cargo Setup

## Cargo.toml

```toml
[package]
name = "exdb"
version = "0.1.0"
edition = "2024"

[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# Serialization
bson = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Checksums
crc32fast = "1"

# Concurrency
parking_lot = "0.12"

# Error handling
thiserror = "2"
anyhow = "1"

# ID generation
ulid = "1"

# Bitflags
bitflags = "2"

# Logging
tracing = "0.1"

[dev-dependencies]
tempfile = "3"
tokio-test = "0.4"
```

## Justification

| Crate | Why |
|-------|-----|
| `tokio` | Async runtime — all I/O is async (file, network). `features = ["full"]` for fs, sync, rt-multi-thread, time, io-util. |
| `bson` | BSON encoding/decoding for document bodies. Wire format for storage and replication. |
| `serde` / `serde_json` | JSON parsing for config files, JSON wire format, meta.json. |
| `crc32fast` | Hardware-accelerated CRC-32C for page checksums and WAL record checksums. |
| `parking_lot` | Faster Mutex/RwLock than std. Used for frame-level locking in buffer pool and catalog cache. |
| `thiserror` | Derive Error for structured error types. |
| `anyhow` | Top-level error handling in the binary entry point. |
| `ulid` | ULID generation for document IDs. |
| `bitflags` | Bitflag types for CellFlags, page flags. |
| `tracing` | Structured logging throughout the storage engine. |

## What We Don't Need Yet

- **Network**: no axum, hyper, or quinn — the storage engine is an embedded library.
- **Protobuf**: deferred to later phase (wire format).
- **JWT**: deferred (auth is above the storage layer).
- **LZ4**: deferred (compression is a future optimization).
