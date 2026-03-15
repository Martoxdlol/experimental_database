//! T1: Monotonic timestamp allocator for MVCC.
//!
//! Every transaction gets a unique, globally-ordered timestamp. Both `begin_ts`
//! (read snapshot) and `commit_ts` (write version) are drawn from the same
//! sequence to ensure total ordering.
//!
//! Thread-safe via atomic operations. Shared between [`CommitCoordinator`]
//! (which allocates `commit_ts`) and [`CommitHandle`] (which reads `visible_ts`).
//!
//! **Note:** `visible_ts` (the latest timestamp safe for new readers) lives on
//! [`CommitCoordinator`], not here. [`TsAllocator::latest`] tracks the highest
//! *allocated* timestamp, which may be ahead of what has been replicated.

use std::sync::atomic::{AtomicU64, Ordering};

use exdb_core::types::Ts;

/// Monotonic timestamp allocator.
///
/// Provides a lock-free, thread-safe source of strictly increasing timestamps
/// for the MVCC concurrency control layer. At 1 billion allocations per second,
/// overflow would take ~584 years — no overflow handling is needed.
pub struct TsAllocator {
    /// The *next* timestamp to be handed out. Invariant: `next >= 1`.
    next: AtomicU64,
}

impl TsAllocator {
    /// Create a new allocator seeded with `initial`.
    ///
    /// After construction, [`latest()`](Self::latest) returns `initial` and the
    /// first [`allocate()`](Self::allocate) call returns `initial + 1`.
    pub fn new(initial: Ts) -> Self {
        Self {
            next: AtomicU64::new(initial + 1),
        }
    }

    /// The highest timestamp that has been allocated so far.
    ///
    /// This is **not** the same as `visible_ts` — it may be ahead of
    /// replication. Use `CommitHandle::visible_ts()` for the safe read fence.
    pub fn latest(&self) -> Ts {
        self.next.load(Ordering::Acquire) - 1
    }

    /// Allocate the next timestamp. Strictly monotonically increasing.
    ///
    /// Uses `AcqRel` ordering so that:
    /// - **Acquire**: we see all writes from prior allocators.
    /// - **Release**: subsequent [`latest()`](Self::latest) calls see our allocation.
    pub fn allocate(&self) -> Ts {
        self.next.fetch_add(1, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn new_initial() {
        let alloc = TsAllocator::new(42);
        assert_eq!(alloc.latest(), 42);
    }

    #[test]
    fn allocate_returns_next() {
        let alloc = TsAllocator::new(0);
        assert_eq!(alloc.allocate(), 1);
        assert_eq!(alloc.allocate(), 2);
        assert_eq!(alloc.allocate(), 3);
    }

    #[test]
    fn latest_tracks_allocations() {
        let alloc = TsAllocator::new(0);
        alloc.allocate();
        alloc.allocate();
        alloc.allocate();
        assert_eq!(alloc.latest(), 3);
    }

    #[test]
    fn allocate_from_nonzero_initial() {
        let alloc = TsAllocator::new(100);
        assert_eq!(alloc.allocate(), 101);
        assert_eq!(alloc.latest(), 101);
    }

    #[tokio::test]
    async fn concurrent_no_duplicates() {
        let alloc = Arc::new(TsAllocator::new(0));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let a = Arc::clone(&alloc);
            handles.push(tokio::spawn(async move {
                let mut vals = Vec::with_capacity(100);
                for _ in 0..100 {
                    vals.push(a.allocate());
                }
                vals
            }));
        }
        let mut all = HashSet::new();
        for h in handles {
            for v in h.await.unwrap() {
                assert!(all.insert(v), "duplicate timestamp: {v}");
            }
        }
        assert_eq!(all.len(), 10_000);
    }

    #[tokio::test]
    async fn concurrent_monotonic() {
        let alloc = Arc::new(TsAllocator::new(0));
        let mut handles = Vec::new();
        for _ in 0..50 {
            let a = Arc::clone(&alloc);
            handles.push(tokio::spawn(async move {
                let mut prev = 0;
                for _ in 0..200 {
                    let ts = a.allocate();
                    assert!(ts > prev, "non-monotonic: {prev} -> {ts}");
                    prev = ts;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
