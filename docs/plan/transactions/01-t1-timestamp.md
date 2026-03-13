# T1: Timestamp Allocator

## Purpose

Monotonic timestamp allocator for MVCC. Every transaction gets a unique, globally-ordered timestamp. Central coordination point — both `begin_ts` (read snapshot) and `commit_ts` (write version) are drawn from the same sequence.

## Dependencies

- **L1 (`core/types.rs`)**: `Ts` (alias for `u64`)

No other dependencies. Pure atomic counter.

## Rust Types

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use exdb_core::types::Ts;

/// Monotonic timestamp allocator.
///
/// Thread-safe via atomic operations. Shared between CommitCoordinator
/// (which allocates commit_ts) and CommitHandle (which allocates begin_ts
/// via visible_ts). Both draw from the same sequence to ensure total ordering.
///
/// NOTE: `visible_ts` (the latest timestamp safe for new readers) lives on
/// CommitCoordinator, not here. `TsAllocator.latest()` tracks the highest
/// *allocated* timestamp, which may be ahead of what has been replicated.
pub struct TsAllocator {
    next: AtomicU64,
}

impl TsAllocator {
    /// Create with an initial timestamp. `latest()` will return `initial`.
    /// The next `allocate()` call returns `initial + 1`.
    pub fn new(initial: Ts) -> Self;

    /// The highest timestamp that has been allocated.
    /// This is NOT the same as visible_ts — it may be ahead of replication.
    pub fn latest(&self) -> Ts;

    /// Allocate the next timestamp. Monotonically increasing.
    /// Returns the allocated value.
    pub fn allocate(&self) -> Ts;
}
```

## Implementation Details

### new()

```rust
pub fn new(initial: Ts) -> Self {
    Self {
        next: AtomicU64::new(initial + 1),
    }
}
```

### latest()

```rust
pub fn latest(&self) -> Ts {
    self.next.load(Ordering::Acquire) - 1
}
```

Uses `Acquire` ordering to ensure subsequent reads of data at this timestamp see all writes that happened before the timestamp was published.

### allocate()

```rust
pub fn allocate(&self) -> Ts {
    self.next.fetch_add(1, Ordering::AcqRel)
}
```

`AcqRel` ensures:
- **Acquire**: we see all writes from prior allocators.
- **Release**: subsequent loads (via `latest()`) see our allocation.

Returns the value that was in `next` before the increment — so if `next` was 5, `allocate()` returns 5 and `next` becomes 6.

## Error Handling

No errors. Timestamp overflow at `u64::MAX` is not handled — at 1 billion transactions per second, this would take ~584 years.

## Tests

1. **new_initial**: `TsAllocator::new(42)` — `latest()` returns 42.
2. **allocate_returns_next**: `new(0)` then `allocate()` returns 1, next `allocate()` returns 2.
3. **latest_tracks_allocations**: After 3 allocations from `new(0)`, `latest()` returns 3.
4. **concurrent_no_duplicates**: Spawn 100 tasks, each calling `allocate()` 100 times. Collect all values into a `HashSet` — size must equal 10,000. Verifies no duplicates under contention.
5. **concurrent_monotonic**: Each task verifies its sequence of allocated values is strictly increasing.
