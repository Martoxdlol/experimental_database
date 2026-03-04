//! Write-ahead log (WAL).
//!
//! The WAL guarantees durability: every committed transaction is persisted
//! to a WAL segment file before it becomes visible. The WAL uses a
//! multi-segment design with configurable segment size (default 64 MiB).
//!
//! ## Frame format
//!
//! Each WAL frame is 9 bytes of header followed by a variable-length payload:
//!
//! ```text
//! [payload_len: u32 LE] [crc32c: u32 LE] [record_type: u8] [payload ...]
//! ```
//!
//! ## Components
//!
//! - [`record`]: WAL record types and serialization (TxCommit, Checkpoint, DDL ops)
//! - [`segment`]: Individual WAL segment files with headers and append/read operations
//! - [`writer`]: Async group-commit writer (batches via mpsc channel, fsync per batch)
//! - [`reader`]: Forward-scanning reader for recovery and replication

pub mod record;
pub mod segment;
pub mod writer;
pub mod reader;

pub use record::*;
pub use segment::*;
pub use writer::*;
pub use reader::*;
