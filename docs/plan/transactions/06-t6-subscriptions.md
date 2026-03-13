# T6: Subscriptions

## Purpose

Manages persistent read set watches for reactive invalidation on commit. Indexes intervals by `(collection, index)` for fast overlap checks. Supports three subscription modes with different lifecycles: Notify (one-shot), Watch (persistent, manual refresh), Subscribe (persistent with automatic chain continuation).

## Dependencies

- **T2 (`read_set.rs`)**: `ReadSet`, `ReadInterval`, `QueryId`, `LimitBoundary`
- **T3 (`write_set.rs`)**: `IndexDelta`
- **T4 (`commit_log.rs`)**: `CommitLogEntry`, `IndexKeyWrite`
- **L1 (`core/types.rs`)**: `CollectionId`, `IndexId`, `Ts`
- **tokio**: `mpsc` (event delivery channels)

## Rust Types

```rust
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use exdb_core::types::{CollectionId, IndexId, Ts};
use tokio::sync::mpsc;
use crate::read_set::{ReadSet, ReadInterval, QueryId};
use crate::commit_log::{CommitLogEntry, IndexKeyWrite};

/// Unique subscription identifier.
pub type SubscriptionId = u64;

/// Transaction identifier (for chain continuation).
pub type TxId = u64;

/// Subscription mode — controls post-commit read set behavior.
/// Mutually exclusive: exactly one mode per transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    /// No subscription. Read set discarded after commit.
    None,
    /// One-shot: fire once on first invalidation, then remove.
    Notify,
    /// Persistent: fire on every invalidation, subscription persists.
    /// No new transaction is auto-started. Client manages re-queries.
    Watch,
    /// Persistent with chain continuation: fire, auto-start new tx,
    /// carry forward unaffected read set intervals.
    Subscribe,
}

/// An interval stored in the subscription registry index.
///
/// Embeds ReadInterval (including limit_boundary) so that invalidation
/// checks use `contains_key` and respect LIMIT tightening, matching
/// OCC semantics exactly.
pub struct SubscriptionInterval {
    pub subscription_id: SubscriptionId,
    pub interval: ReadInterval,
}

/// Metadata for a registered subscription.
pub struct SubscriptionMeta {
    pub mode: SubscriptionMode,
    pub session_id: u64,
    pub tx_id: TxId,
    pub read_ts: Ts,
    /// Full read set (for split_before on chain continuation).
    pub read_set: ReadSet,
    /// Channel sender for pushing InvalidationEvents to the client.
    pub event_tx: mpsc::Sender<InvalidationEvent>,
}

/// Invalidation event — one per affected subscription per commit.
///
/// Contains all invalidated query_ids (sorted ascending) so the client
/// knows which queries need re-execution. A single commit that overlaps
/// N queries in the same subscription produces exactly one event with
/// N query IDs — never N separate events.
#[derive(Debug)]
pub struct InvalidationEvent {
    pub subscription_id: SubscriptionId,
    /// Sorted ascending. Used by client to determine re-execution scope.
    pub affected_query_ids: Vec<QueryId>,
    /// The commit_ts that triggered the invalidation.
    pub commit_ts: Ts,
    /// Chain continuation (present only for Subscribe mode).
    pub continuation: Option<ChainContinuation>,
}

/// Chain continuation for Subscribe mode.
///
/// When a subscription is invalidated, this struct packages the
/// auto-started new transaction and the carried-forward read set.
#[derive(Debug)]
pub struct ChainContinuation {
    /// New transaction auto-started at the invalidation timestamp.
    pub new_tx_id: TxId,
    /// Read snapshot for the new transaction (= invalidating commit_ts).
    pub new_ts: Ts,
    /// Read set intervals from queries BEFORE the first invalidated query.
    /// Carried forward because they were provably unaffected.
    pub carried_read_set: ReadSet,
    /// The first query_id that needs re-execution (= min(affected_query_ids)).
    pub first_query_id: QueryId,
}

/// The subscription registry — indexes intervals for fast invalidation lookup.
pub struct SubscriptionRegistry {
    /// Grouped by (collection, index) for range overlap checks.
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    /// Metadata per subscription.
    subscriptions: HashMap<SubscriptionId, SubscriptionMeta>,
    /// Session → subscription IDs for bulk cleanup on disconnect.
    session_index: HashMap<u64, Vec<SubscriptionId>>,
    /// Monotonic ID allocator.
    next_id: SubscriptionId,
}
```

## Implementation Details

### SubscriptionRegistry::new()

```rust
pub fn new() -> Self {
    Self {
        index: HashMap::new(),
        subscriptions: HashMap::new(),
        session_index: HashMap::new(),
        next_id: 1,
    }
}
```

### register()

```rust
/// Register a read set as a subscription.
///
/// `read_set` must already have had `extend_for_deltas` applied (commit
/// step 10a) so that any stale `limit_boundary` from the tx's own writes
/// is cleared.
///
/// `event_tx`: sender for pushing InvalidationEvents to the client.
/// L6 wraps the receiver in a SubscriptionHandle.
pub fn register(
    &mut self,
    mode: SubscriptionMode,
    session_id: u64,
    tx_id: TxId,
    read_ts: Ts,
    read_set: ReadSet,
    event_tx: mpsc::Sender<InvalidationEvent>,
) -> SubscriptionId {
    let id = self.next_id;
    self.next_id += 1;

    // Index all intervals for fast overlap lookup
    for (&(coll, idx), intervals) in &read_set.intervals {
        let sub_intervals = self.index.entry((coll, idx)).or_default();
        for interval in intervals {
            sub_intervals.push(SubscriptionInterval {
                subscription_id: id,
                interval: interval.clone(),
            });
        }
    }

    // Store metadata
    self.subscriptions.insert(id, SubscriptionMeta {
        mode,
        session_id,
        tx_id,
        read_ts,
        read_set,
        event_tx,
    });

    // Update session index
    self.session_index.entry(session_id).or_default().push(id);

    id
}
```

### remove()

```rust
/// Remove a subscription from the registry.
///
/// Cleans up the interval index, metadata, and session index.
pub fn remove(&mut self, id: SubscriptionId) {
    // Remove from interval index
    for intervals in self.index.values_mut() {
        intervals.retain(|si| si.subscription_id != id);
    }
    // Remove empty groups
    self.index.retain(|_, v| !v.is_empty());

    // Remove metadata
    if let Some(meta) = self.subscriptions.remove(&id) {
        // Remove from session index
        if let Some(session_subs) = self.session_index.get_mut(&meta.session_id) {
            session_subs.retain(|&sid| sid != id);
            if session_subs.is_empty() {
                self.session_index.remove(&meta.session_id);
            }
        }
    }
}
```

### check_invalidation()

```rust
/// Check all subscriptions against a new commit's writes.
///
/// Uses `ReadInterval::contains_key` for overlap, respecting effective
/// intervals (original bounds + LimitBoundary). Handles both data and
/// catalog conflicts uniformly — catalog writes appear as index_writes
/// on reserved CATALOG_COLLECTIONS / CATALOG_INDEXES pseudo-collections.
///
/// For Subscribe mode, computes ChainContinuation with carried read set.
/// For Notify mode, marks for removal after event delivery.
///
/// `allocate_tx_id`: closure to allocate new TxIds for Subscribe mode
/// chain continuations.
///
/// Returns one InvalidationEvent per affected subscription.
pub fn check_invalidation(
    &mut self,
    commit_ts: Ts,
    index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
    mut allocate_tx_id: impl FnMut() -> TxId,
) -> Vec<InvalidationEvent>
```

**Algorithm:**

```
// Step 1: Collect affected (subscription_id, query_id) pairs
//
// Catalog conflicts are handled uniformly: catalog writes appear as
// index_writes on CATALOG_COLLECTIONS / CATALOG_INDEXES, and catalog
// reads appear as intervals on the same reserved collections. The same
// contains_key check handles both.
let mut affected: HashMap<SubscriptionId, BTreeSet<QueryId>> = HashMap::new();

for ((coll, idx), key_writes) in index_writes:
    if let Some(sub_intervals) = self.index.get(&(coll, idx)):
        for write in key_writes:
            for sub_interval in sub_intervals:
                let hit = write.old_key.as_ref().is_some_and(|k| sub_interval.interval.contains_key(k))
                       || write.new_key.as_ref().is_some_and(|k| sub_interval.interval.contains_key(k));
                if hit:
                    affected.entry(sub_interval.subscription_id)
                        .or_default()
                        .insert(sub_interval.interval.query_id);

// Step 2: Build events
let mut events = Vec::new();
let mut to_remove = Vec::new();

for (sub_id, query_ids) in affected:
    let meta = &self.subscriptions[&sub_id];
    let affected_ids: Vec<QueryId> = query_ids.into_iter().collect();  // already sorted (BTreeSet)
    let q_min = affected_ids[0];

    let continuation = match meta.mode {
        SubscriptionMode::Subscribe => {
            let carried = meta.read_set.split_before(q_min);
            Some(ChainContinuation {
                new_tx_id: allocate_tx_id(),
                new_ts: commit_ts,
                carried_read_set: carried,
                first_query_id: q_min,
            })
        }
        _ => None,
    };

    events.push(InvalidationEvent {
        subscription_id: sub_id,
        affected_query_ids: affected_ids,
        commit_ts,
        continuation,
    });

    match meta.mode {
        SubscriptionMode::Notify => to_remove.push(sub_id),
        SubscriptionMode::Subscribe => to_remove.push(sub_id),  // re-registered on chain commit
        SubscriptionMode::Watch => {},  // persists
        SubscriptionMode::None => unreachable!(),
    }

// Step 3: Clean up Notify/Subscribe subscriptions
for id in to_remove:
    self.remove(id);

events
```

### update_read_set()

```rust
/// Update a subscription's read set (for chain commit or Watch refresh).
///
/// Replaces the existing intervals in the index and metadata with
/// the new read set's intervals.
pub fn update_read_set(&mut self, id: SubscriptionId, new_read_set: ReadSet) {
    // Remove old intervals from index
    for intervals in self.index.values_mut() {
        intervals.retain(|si| si.subscription_id != id);
    }

    // Add new intervals
    for (&(coll, idx), intervals) in &new_read_set.intervals {
        let sub_intervals = self.index.entry((coll, idx)).or_default();
        for interval in intervals {
            sub_intervals.push(SubscriptionInterval {
                subscription_id: id,
                interval: interval.clone(),
            });
        }
    }

    // Update metadata
    if let Some(meta) = self.subscriptions.get_mut(&id) {
        meta.read_set = new_read_set;
    }
}
```

### remove_session()

```rust
/// Remove all subscriptions for a session (on disconnect).
pub fn remove_session(&mut self, session_id: u64) {
    if let Some(sub_ids) = self.session_index.remove(&session_id) {
        for id in sub_ids {
            self.remove(id);
        }
    }
}
```

### push_events()

```rust
/// Push invalidation events to subscriber channels.
///
/// Called by CommitCoordinator (T7 step 11) after check_invalidation.
/// Uses try_send; full channels or closed receivers are silently dropped.
pub fn push_events(&self, events: Vec<InvalidationEvent>) {
    for event in events {
        if let Some(meta) = self.subscriptions.get(&event.subscription_id) {
            let _ = meta.event_tx.try_send(event);
        }
        // For Notify/Subscribe: subscription already removed in check_invalidation,
        // so this may not find the meta. In that case, the event was already
        // captured and will be delivered via the stored event_tx.
    }
}
```

**Note on push_events:** After `check_invalidation`, Notify and Subscribe subscriptions have been removed from `self.subscriptions`. The events carry the subscription_id, but the meta is gone. Solution: `check_invalidation` returns the events along with cloned `event_tx` senders, OR push_events is called BEFORE removal. The cleaner approach is to have `check_invalidation` return `Vec<(InvalidationEvent, mpsc::Sender<InvalidationEvent>)>`:

```rust
pub fn push_events(events: Vec<(InvalidationEvent, mpsc::Sender<InvalidationEvent>)>) {
    for (event, tx) in events {
        let _ = tx.try_send(event);
    }
}
```

This avoids the lookup-after-removal problem.

## Error Handling

No fallible operations. Channel send failures (full or closed) are silently dropped — the client has disconnected or is not consuming events.

## Tests

### Registration and removal

1. **register_assigns_id**: Two registrations get different IDs.
2. **remove_cleans_up**: After remove, subscription is gone from index and metadata.
3. **remove_session_clears_all**: Register 3 subscriptions for same session, remove_session, all gone.

### Invalidation — no overlap

4. **no_invalidation_disjoint**: Subscription on [10, 20), commit writes at 25 → no events.
5. **no_invalidation_beyond_limit**: Subscription on [10, 50) with Upper(20), write at 30 → no events.

### Invalidation — Notify mode

6. **notify_fires_once**: Write overlaps → event produced. Subscription removed.
7. **notify_no_second_fire**: After removal, second write does not produce event.

### Invalidation — Watch mode

8. **watch_fires_repeatedly**: First write → event. Second write → event again. Subscription persists.
9. **watch_same_read_set**: Read set unchanged between invalidations.

### Invalidation — Subscribe mode

10. **subscribe_chain_continuation**: Write overlaps Q1 (of Q0, Q1, Q2) → event has `ChainContinuation` with `carried_read_set` containing Q0's intervals, `first_query_id = 1`.
11. **subscribe_catalog_conflict_via_interval**: Write on CATALOG_COLLECTIONS overlaps catalog interval at Q0 → `first_query_id = 0`, empty carried read set.
12. **subscribe_removed_after_fire**: Subscription removed from registry (re-registered on chain commit via update_read_set).

### update_read_set

13. **update_replaces_intervals**: After update, old intervals no longer trigger invalidation, new ones do.

### Edge cases

14. **multiple_subscriptions_one_commit**: Two subscriptions, both overlap → two events.
15. **multiple_queries_one_event**: Subscription with Q0, Q1, Q2 — write overlaps Q0 and Q2 → single event with `affected_query_ids = [0, 2]`.
16. **limit_boundary_respected_in_invalidation**: Key in original range but outside effective interval → no invalidation.
