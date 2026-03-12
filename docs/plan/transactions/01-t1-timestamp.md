# T1: Monotonic Timestamp Allocator

**File:** `crates/tx/src/timestamp.rs`
**Depends on:** L1 (`exdb-core::types::Ts`)
**Depended on by:** T7 (`commit.rs`)

## Purpose

Provides globally-ordered timestamps for transactions. Each transaction gets a `begin_ts` (read snapshot) and each commit gets a `commit_ts` (write point). Timestamps are strictly monotonic — no two transactions share a timestamp.

## Data Structures

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use exdb_core::types::Ts;

pub struct TsAllocator {
    /// The highest allocated timestamp.
    /// Starts at the recovered value from WAL replay (or 0 for fresh databases).
    current: AtomicU64,
}
```

## API

```rust
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
    /// Ordering: SeqCst to ensure all threads see timestamps in order.
    pub fn allocate(&self) -> Ts {
        self.current.fetch_add(1, Ordering::SeqCst) + 1
    }
}
```

## Design Notes

### `visible_ts` Is NOT Here

`visible_ts` (the latest timestamp safe for new readers) lives on `CommitCoordinator`, not on `TsAllocator`. This is because:

- `TsAllocator.latest()` tracks the highest *allocated* timestamp, which may be ahead of what has been replicated
- `visible_ts` only advances after replication confirms AND the WAL records this advancement
- New read-only transactions use `visible_ts`, not `latest()`

### Ordering

`SeqCst` is used for both `load` and `fetch_add`. This is conservative but correct. The timestamp allocator is not a hot path — it's called once per commit by the single writer. The cost of `SeqCst` vs `Relaxed` is negligible here, and correctness is critical.

### Recovery

On database open, `TsAllocator::new(recovered_ts)` is called where `recovered_ts` is the highest `commit_ts` found during WAL replay. This ensures no timestamp is reused after a crash.

### Overflow

`u64` provides 2^64 timestamps. At 1 million commits per second, this lasts ~584,000 years. No overflow handling needed.

## Tests

```
t1_allocate_sequential
    new(0) → allocate() returns 1, 2, 3, ...

t1_allocate_from_recovered
    new(100) → allocate() returns 101, latest() returns 101

t1_latest_does_not_advance
    new(5) → latest() returns 5, latest() returns 5 (unchanged)

t1_concurrent_allocate
    Spawn 10 tasks, each calling allocate() 100 times.
    Collect all 1000 timestamps. Assert all unique, all in [1..=1000].
```
