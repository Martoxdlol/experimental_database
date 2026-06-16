//! exdb-query — Layer 4: Query Engine
//!
//! Access method resolution, range encoding, scan execution, post-filtering,
//! and read-your-writes merge.

pub mod access;
pub mod merge;
pub mod post_filter;
pub mod range_encoder;
pub mod scan;

// ─── Public Facade ───

// Filter AST (from L1, re-exported for convenience)
pub use exdb_core::filter::{Filter, RangeExpr};

// Post-filter evaluation
pub use post_filter::{compare_scalars, filter_matches};

// Range encoding
pub use range_encoder::{RangeError, RangeShape, encode_range, validate_range};

// Access method resolution
pub use access::{AccessError, AccessMethod, IndexInfo, resolve_access};

// Scan execution
pub use scan::{QueryScanStream, ReadIntervalInfo, ScanRow, ScanStats, execute_scan};

// Write-set merge
pub use merge::{MergeView, merge_with_writes};
