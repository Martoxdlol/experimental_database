//! exdb — Layer 6: Database Instance
//!
//! Primary public API for the embedded database. Composes all lower
//! layers into a usable `Database` struct with collection management,
//! transaction sessions, and catalog caching.
//!
//! This is the crate that downstream users depend on.
