//! Replication is currently exercised by the `exdb-replication` and
//! `exdb-server` test suites rather than a runnable package example.
//!
//! Start with:
//!
//! ```bash
//! cargo test -p exdb-replication --all-targets
//! cargo test -p exdb-server configured_three_node_replication_applies_to_both_online_replicas
//! ```

fn main() {
    println!("replication examples are covered by exdb-replication and exdb-server tests");
}
