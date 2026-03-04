//! # Storage Engine
//!
//! A WAL-first, page-oriented storage engine for an embedded document database.
//!
//! ## Architecture
//!
//! The engine uses a **single-writer** model: all mutations are serialized through an
//! async writer task, while reads can proceed concurrently from any thread via the
//! buffer pool's per-frame `RwLock`s.
//!
//! ### On-disk layout
//!
//! ```text
//! <db_dir>/
//!   data.db         Page store (slotted pages, 8 KiB default)
//!   data.dwb        Double-write buffer (torn-write protection)
//!   meta.json       Checkpoint LSN and database metadata
//!   wal/
//!     wal_000001_00000000.wal
//!     wal_000002_00000150.wal
//!     ...
//! ```
//!
//! ### Data flow
//!
//! 1. **Write path**: mutation → WAL append (group commit) → apply to B-tree pages in buffer pool
//! 2. **Read path**: fetch page from buffer pool (cache hit) or disk (cache miss)
//! 3. **Checkpoint**: snapshot dirty pages → DWB → scatter-write to data.db → fsync → clear DWB
//! 4. **Recovery**: repair torn pages from DWB → replay WAL from last checkpoint LSN
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`types`] | Core newtypes (`PageId`, `Lsn`, `DocId`, etc.) and constants |
//! | [`page`] | Slotted page format with variable-length cells |
//! | [`buffer_pool`] | Page cache with clock eviction and positional I/O |
//! | [`wal`] | Write-ahead log: segments, records, async writer, reader |
//! | [`btree`] | B+ tree: insert/delete/cursor/range-scan |
//! | [`heap`] | External heap for large documents (overflow pages) |
//! | [`key_encoding`] | Order-preserving, self-delimiting scalar encoding |
//! | [`catalog`] | In-memory collection and index metadata registry |
//! | [`free_list`] | Reusable page allocator (intrusive linked list) |
//! | [`file_header`] | Database file header with shadow copy for crash safety |
//! | [`checkpoint`] | Fuzzy checkpoint with double-write buffer |
//! | [`recovery`] | Crash recovery: DWB repair + WAL replay |
//! | [`engine`] | Top-level `StorageEngine` facade |

pub mod types;
pub mod page;
pub mod key_encoding;
pub mod wal;
pub mod buffer_pool;
pub mod free_list;
pub mod file_header;
pub mod btree;
pub mod heap;
pub mod catalog;
pub mod checkpoint;
pub mod recovery;
pub mod engine;
