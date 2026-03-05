//! Generic storage engine library (Layer 2).
//!
//! This crate provides a page-oriented storage engine with B+ trees, heap storage,
//! write-ahead logging, and crash recovery. It operates on raw bytes and pages with
//! no domain-specific knowledge (no DocId, Ts, Document, Filter, MVCC, etc.).

// SlottedPage/SlottedPageRef are borrowed views without Drop impls.
// Explicit drop() calls document when page access ends before guard release.
#![allow(clippy::drop_non_drop)]

pub mod error;
pub mod util;

pub mod backend;
pub mod page;
pub mod buffer_pool;
pub mod free_list;
pub mod wal;
pub mod btree;
pub mod heap;
pub mod dwb;
pub mod checkpoint;
pub mod recovery;
pub mod vacuum;
pub mod catalog_btree;
pub mod posting;
pub mod engine;
