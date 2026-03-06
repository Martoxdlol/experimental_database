//! exdb-core — Layer 1: Core Types & Encoding
//!
//! Pure types and encoding utilities shared across all layers.
//! No I/O, no async, no domain logic.

pub mod types;
pub mod field_path;
pub mod encoding;
pub mod ulid;
