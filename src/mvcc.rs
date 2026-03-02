//! MVCC: timestamp allocation and snapshot visibility.

use std::sync::atomic::{AtomicU64, Ordering};
use crate::types::Ts;

/// Monotonically increasing timestamp allocator.
pub struct TsAllocator {
    current: AtomicU64,
}

impl TsAllocator {
    pub fn new(initial: Ts) -> Self {
        TsAllocator { current: AtomicU64::new(initial) }
    }

    /// Allocate the next timestamp. Thread-safe.
    pub fn next(&self) -> Ts {
        self.current.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current committed timestamp (last allocated - 1).
    pub fn current_ts(&self) -> Ts {
        self.current.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// Advance the allocator to be at least `min_ts + 1`.
    pub fn advance_to_at_least(&self, min_ts: Ts) {
        let mut cur = self.current.load(Ordering::SeqCst);
        while cur <= min_ts {
            match self.current.compare_exchange(cur, min_ts + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}
