//! B+ tree index implementation.
//!
//! Provides a disk-backed B+ tree with:
//! - **Insert with split**: leaf and internal node splits propagate upward
//! - **Delete**: key removal from leaf nodes
//! - **Cursor**: positioned iteration (seek, seek_first, advance)
//! - **Range scan**: bounded iteration over `[lower, upper)` key ranges
//!
//! All operations work directly on slotted pages via the buffer pool.
//! Keys are compared with `memcmp` — the key encoding module ensures
//! this produces the correct logical ordering.

pub mod node;
pub mod cursor;
pub mod insert;
pub mod delete;
pub mod scan;

pub use node::*;
pub use cursor::*;
pub use insert::*;
pub use delete::*;
pub use scan::*;
