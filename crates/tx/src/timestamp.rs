use std::sync::atomic::{AtomicU64, Ordering};
use exdb_core::types::Ts;

/// Monotonic timestamp allocator.
///
/// Provides globally-ordered timestamps for transactions. Each transaction gets a
/// `begin_ts` (read snapshot) and each commit gets a `commit_ts` (write point).
/// Timestamps are strictly monotonic — no two transactions share a timestamp.
pub struct TsAllocator {
    /// The highest allocated timestamp.
    current: AtomicU64,
}

impl TsAllocator {
    /// Create a new allocator starting at the given timestamp.
    /// `initial` is typically the highest commit_ts found during WAL replay,
    /// or 0 for a fresh database.
    pub fn new(initial: Ts) -> Self {
        Self {
            current: AtomicU64::new(initial),
        }
    }

    /// Return the latest allocated timestamp without advancing.
    /// Used by readers to know the current state, NOT for assigning timestamps.
    pub fn latest(&self) -> Ts {
        self.current.load(Ordering::SeqCst)
    }

    /// Allocate the next timestamp. Returns the new value.
    /// Called by CommitCoordinator during commit (step 2).
    pub fn allocate(&self) -> Ts {
        self.current.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t1_allocate_sequential() {
        let alloc = TsAllocator::new(0);
        assert_eq!(alloc.allocate(), 1);
        assert_eq!(alloc.allocate(), 2);
        assert_eq!(alloc.allocate(), 3);
    }

    #[test]
    fn t1_allocate_from_recovered() {
        let alloc = TsAllocator::new(100);
        assert_eq!(alloc.allocate(), 101);
        assert_eq!(alloc.latest(), 101);
    }

    #[test]
    fn t1_latest_does_not_advance() {
        let alloc = TsAllocator::new(5);
        assert_eq!(alloc.latest(), 5);
        assert_eq!(alloc.latest(), 5);
    }

    #[tokio::test]
    async fn t1_concurrent_allocate() {
        use std::sync::Arc;
        use std::collections::HashSet;

        let alloc = Arc::new(TsAllocator::new(0));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let a = Arc::clone(&alloc);
            handles.push(tokio::spawn(async move {
                let mut ts = Vec::new();
                for _ in 0..100 {
                    ts.push(a.allocate());
                }
                ts
            }));
        }

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.await.unwrap());
        }

        assert_eq!(all.len(), 1000);
        let unique: HashSet<_> = all.iter().collect();
        assert_eq!(unique.len(), 1000);
        assert!(all.iter().all(|&ts| ts >= 1 && ts <= 1000));
    }

    #[test]
    fn t1_allocate_peek_symmetry() {
        let alloc = TsAllocator::new(0);
        let before = alloc.latest();
        let allocated = alloc.allocate();
        assert_eq!(allocated, before + 1);
        assert_eq!(alloc.latest(), allocated);
    }

    #[test]
    fn t1_initial_zero_allocates_one() {
        let alloc = TsAllocator::new(0);
        assert_eq!(alloc.latest(), 0);
        assert_eq!(alloc.allocate(), 1);
    }
}
