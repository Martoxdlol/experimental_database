```rust
/// Semantic bound for an interval endpoint.
/// Uses std::ops::Bound — reuses existing ecosystem type.
/// Lower: Included or Unbounded (Excluded is legal but never produced by encode_range).
/// Upper: Excluded or Unbounded (Included is legal but encode_range always converts to Excluded).

/// Indicates that a LIMIT stopped the scan before the range was exhausted,
/// and which side of the interval was tightened.
pub enum LimitBoundary {
    /// ASC scan: stopped after returning a doc with sort key K.
    /// Effective upper = Excluded(successor(K)).
    /// If K's doc is deleted or moved out: clear → restore original upper.
    Upper(Vec<u8>),

    /// DESC scan: stopped after returning a doc with sort key K.
    /// Effective lower = Included(K).
    /// If K's doc is deleted or moved out: clear → restore original lower.
    Lower(Vec<u8>),
}

pub struct ReadInterval {
    /// Which query produced this interval (minimum query_id after merge).
    pub query_id: QueryId,

    /// Original range lower bound (before any LIMIT tightening).
    pub lower: Bound<Vec<u8>>,

    /// Original range upper bound (before any LIMIT tightening).
    pub upper: Bound<Vec<u8>>,

    /// LIMIT tightening. None = scan exhausted index within bounds (full coverage).
    pub limit_boundary: Option<LimitBoundary>,
}

impl ReadInterval {
    /// Check if `key` falls within the effective interval.
    /// Considers both original bounds AND any LIMIT tightening.
    pub fn contains_key(&self, key: &[u8]) -> bool { ... }

    /// Process a write delta: if `old_key` is in the effective interval and
    /// `new_key` is None or outside the effective interval, clear `limit_boundary`
    /// to restore full original-bounds coverage.
    pub fn apply_delta(&mut self, old_key: Option<&[u8]>, new_key: Option<&[u8]>) { ... }
}
```


Considerations:

In a transactions when reading, we must merge read set with our own writes using apply delta.

When checking OCC we can use contains_key with each written entries between start and end ts when committing.

After checking OCC, if subscribe, we need to ensure all read intervals consider the writes. Because we may have staled read set because of writes after the original read.

The read set for subscription invalidation might not be the same used for OCC.

