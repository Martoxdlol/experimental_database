//! exdb-replication — Layer 7: Replication Transport
//!
//! Optional WAL streaming replication. Implements the `ReplicationHook`
//! trait defined by L6 to enable primary/replica topologies with
//! synchronous replication, promotion, and snapshot transfer.
