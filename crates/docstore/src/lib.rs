//! exdb-docstore — Layer 3: Document Store & Indexing
//!
//! MVCC-aware document storage and secondary indexing built on top of
//! the raw storage engine. Handles key encoding, version resolution,
//! and background index builds.

pub mod key_encoding;
pub mod version_resolution;
pub mod primary_index;
pub mod secondary_index;
pub mod array_indexing;
pub mod index_builder;
pub mod vacuum;

// ─── Public Facade ───

// Key encoding (used by L4 range encoding, L5 read set intervals)
pub use key_encoding::{
    decode_scalar, encode_key_prefix, encode_scalar, inv_ts, make_primary_key,
    make_secondary_key, make_secondary_key_from_prefix, parse_primary_key,
    parse_secondary_key_suffix, prefix_successor, successor_key,
};

// Primary index (used by L4 scan/get, L5 commit)
pub use primary_index::{CellFlags, PrimaryIndex, PrimaryScanStream};

// Secondary index (used by L4 index scan, L5 commit)
pub use secondary_index::{SecondaryIndex, SecondaryScanStream};

// Version resolution (used internally by D3/D4)
pub use version_resolution::VersionResolver;

// Array indexing (used by L5 index delta computation)
pub use array_indexing::compute_index_entries;

// Index builder (used by L6 background task)
pub use index_builder::IndexBuilder;

// Vacuum (used by L5 commit coordinator, L6 startup)
pub use vacuum::{RollbackVacuum, VacuumCoordinator};
