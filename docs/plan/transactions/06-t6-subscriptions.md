# T6: Subscription Registry — Notify / Watch / Subscribe + Carry-Forward

**File:** `crates/tx/src/subscriptions.rs`
**Depends on:** T2 (`read_set.rs`), L1 types
**Depended on by:** T7 (`commit.rs`), L6 (Database), L8 (push notifications)

## Purpose

Manages persistent read set watches for reactive query invalidation. When a commit writes keys that overlap with a subscription's intervals, the subscription fires an invalidation event.

Three modes with different lifecycles:
- **Notify** — one-shot: fire once, remove
- **Watch** — persistent: fire on every invalidation, keep watching with same read set
- **Subscribe** — persistent with chain: fire, auto-start new transaction, carry forward unaffected read set

## Data Structures

```rust
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use exdb_core::types::{CollectionId, IndexId, Ts};
use super::read_set::{ReadSet, ReadInterval, QueryId};

pub type SubscriptionId = u64;
pub type TxId = u64;

/// Subscription mode — determines behavior on invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    /// No subscription. Never stored in the registry.
    None,
    /// One-shot notification. Fire once, then remove.
    Notify,
    /// Persistent watch. Fire on every invalidation.
    /// Read set stays fixed. Client manages re-queries.
    Watch,
    /// Persistent subscription with chain continuation.
    /// On invalidation: auto-start new tx, carry forward
    /// unaffected read set, notify client.
    Subscribe,
}

/// A single interval within the subscription registry,
/// indexed by (collection, index) for fast overlap checks.
#[derive(Debug, Clone)]
pub struct SubscriptionInterval {
    pub subscription_id: SubscriptionId,
    pub query_id: QueryId,
    pub lower: Vec<u8>,
    pub upper: Bound<Vec<u8>>,
}

/// Metadata about a subscription (not indexed for overlap checks).
#[derive(Debug)]
pub struct SubscriptionMeta {
    pub mode: SubscriptionMode,
    /// Which client session owns this subscription.
    /// Used for routing invalidation events to the right client.
    pub session_id: u64,
    /// The transaction that created this subscription.
    pub tx_id: TxId,
    /// The read_ts of the transaction that created this subscription.
    /// Used for chain continuation: the new transaction reads at
    /// the invalidating commit_ts, not at this value.
    pub read_ts: Ts,
    /// The full read set (needed for split_before on chain continuation).
    pub read_set: ReadSet,
}

/// Invalidation event produced when a commit overlaps with a subscription.
#[derive(Debug)]
pub struct InvalidationEvent {
    pub subscription_id: SubscriptionId,
    /// The query IDs whose intervals were overlapped, sorted ascending.
    /// A single commit produces exactly ONE event per subscription,
    /// containing ALL affected query IDs.
    pub affected_query_ids: Vec<QueryId>,
    /// The commit_ts that caused the invalidation.
    pub commit_ts: Ts,
    /// Chain continuation info (present only for Subscribe mode).
    pub continuation: Option<ChainContinuation>,
}

/// Chain continuation for Subscribe mode.
/// Contains everything the client needs to continue the subscription chain.
#[derive(Debug)]
pub struct ChainContinuation {
    /// ID of the new transaction auto-started for this chain link.
    pub new_tx_id: TxId,
    /// Timestamp of the new transaction (= invalidating commit_ts).
    /// The client reads at this timestamp to see the changed data.
    pub new_ts: Ts,
    /// Read set intervals from queries BEFORE the first invalidated query.
    /// These are carried forward because they were provably unaffected
    /// by any commit between the old read_ts and new_ts.
    pub carried_read_set: ReadSet,
    /// The first query_id that needs re-execution.
    /// = min(affected_query_ids).
    /// The new transaction's next_query_id counter starts here.
    pub first_query_id: QueryId,
}

/// The subscription registry.
pub struct SubscriptionRegistry {
    /// Intervals indexed by (collection, index) for fast overlap checks.
    /// Within each group, intervals are sorted by `lower` for binary search.
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    /// Subscription metadata by ID.
    subscriptions: HashMap<SubscriptionId, SubscriptionMeta>,
    /// Next subscription ID counter.
    next_id: AtomicU64,
}
```

## API

```rust
impl SubscriptionRegistry {
    pub fn new() -> Self;

    /// Register a committed transaction's read set as a subscription.
    /// Called after successful commit (or readonly commit with subscription).
    ///
    /// `mode` must not be `None` (caller should not register None subscriptions).
    ///
    /// Returns the SubscriptionId assigned to this subscription.
    pub fn register(
        &mut self,
        mode: SubscriptionMode,
        session_id: u64,
        tx_id: TxId,
        read_ts: Ts,
        read_set: ReadSet,
    ) -> SubscriptionId;

    /// Remove a subscription explicitly.
    /// Called on client disconnect, explicit unsubscribe, or after Notify fires.
    pub fn remove(&mut self, id: SubscriptionId);

    /// Check all subscriptions against a new commit's index key writes.
    ///
    /// For each subscription whose intervals overlap with any key write:
    /// - Collect all affected query_ids (deduplicated, sorted ascending)
    /// - For Subscribe mode: compute ChainContinuation with carried read set
    /// - For Notify mode: mark for removal after this call
    ///
    /// Returns all InvalidationEvents. The caller (CommitCoordinator)
    /// is responsible for:
    /// - Routing events to client sessions
    /// - Starting new transactions for Subscribe continuations
    /// - Removing Notify subscriptions
    ///
    /// `commit_ts`: the timestamp of the commit being checked.
    /// `index_writes`: the commit's key writes, grouped by (collection, index).
    /// `catalog_mutations`: the commit's DDL ops (for catalog subscription checks).
    /// `allocate_tx`: callback to allocate a new TxId for Subscribe continuations.
    pub fn check_invalidation(
        &mut self,
        commit_ts: Ts,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
        catalog_mutations: &[CatalogMutation],
        allocate_tx: impl FnMut() -> TxId,
    ) -> Vec<InvalidationEvent>;

    /// Update a subscription's read set.
    /// Called when a chain transaction (Subscribe mode) commits.
    /// The new read set replaces the old one entirely.
    ///
    /// Also called explicitly by Watch-mode clients that want to
    /// refresh their subscription after re-querying.
    pub fn update_read_set(
        &mut self,
        id: SubscriptionId,
        new_read_set: ReadSet,
    );

    /// Remove all subscriptions for a session.
    /// Called on client disconnect.
    pub fn remove_session(&mut self, session_id: u64);

    /// Number of active subscriptions.
    pub fn len(&self) -> usize;
}
```

## Check Invalidation Algorithm

```
check_invalidation(commit_ts, index_writes, catalog_mutations, allocate_tx):
    // Phase 1: Find all (subscription_id, query_id) overlaps
    affected: HashMap<SubscriptionId, BTreeSet<QueryId>> = {}

    for each (coll_id, idx_id), key_writes in index_writes:
        sub_intervals = self.index.get((coll_id, idx_id))
        if sub_intervals is None:
            continue

        for each key_write in key_writes:
            for each sub_interval in sub_intervals:
                if overlaps(key_write.old_key, sub_interval)
                   or overlaps(key_write.new_key, sub_interval):
                    affected
                        .entry(sub_interval.subscription_id)
                        .or_default()
                        .insert(sub_interval.query_id)

    // Phase 1b: Check catalog subscriptions
    // (Catalog reads in subscriptions conflict with catalog mutations
    //  in the same way as OCC validation)
    if !catalog_mutations.is_empty():
        for each (sub_id, meta) in self.subscriptions:
            if catalog_conflicts(meta.read_set.catalog_reads, catalog_mutations):
                // Catalog conflicts affect all queries (query_id = 0)
                affected.entry(sub_id).or_default().insert(0)

    // Phase 2: Build events
    events = []
    notify_removals = []

    for each (sub_id, query_ids) in affected:
        meta = self.subscriptions[sub_id]
        sorted_qids = query_ids.into_sorted_vec()

        continuation = None
        if meta.mode == Subscribe:
            first_qid = sorted_qids[0]
            carried = meta.read_set.split_before(first_qid)
            new_tx_id = allocate_tx()
            continuation = Some(ChainContinuation {
                new_tx_id,
                new_ts: commit_ts,
                carried_read_set: carried,
                first_query_id: first_qid,
            })

        events.push(InvalidationEvent {
            subscription_id: sub_id,
            affected_query_ids: sorted_qids,
            commit_ts,
            continuation,
        })

        if meta.mode == Notify:
            notify_removals.push(sub_id)

    // Phase 3: Remove one-shot subscriptions
    for sub_id in notify_removals:
        self.remove(sub_id)

    return events
```

## Carry-Forward Correctness

When Subscribe mode fires with `affected_query_ids = [Q_min, ...]`:

1. `split_before(Q_min)` extracts intervals with `query_id < Q_min`
2. These intervals were NOT overlapped by the invalidating commit (otherwise their query_id would be in the affected set)
3. No commit between the subscription's `read_ts` and `commit_ts` overlapped these intervals either (the subscription would have fired earlier — subscriptions are checked against EVERY commit)
4. Therefore, data in these intervals is identical at `read_ts` and `commit_ts`
5. Carrying them forward is equivalent to re-executing those queries — same results, same intervals

**Edge case — merged intervals:** If Q1 and Q3 were merged into a single interval with `query_id = min(1, 3) = 1`, and Q3 is invalidated (`affected = [3]`), then `split_before(3)` includes the merged interval (since its `query_id = 1 < 3`). This is conservative: the carried interval is wider than strictly needed for Q1, but it's correct — it can only cause more future conflicts, never fewer.

**Edge case — catalog reads:** Catalog reads have no query_id. They're always carried forward in full via `split_before`. A catalog conflict (e.g., collection dropped) affects `query_id = 0`, which means `first_query_id = 0` and `split_before(0)` returns an empty read set. The entire transaction must be re-executed. This is correct: if the catalog changed, all queries might return different results.

## Watch Mode Details

Watch mode subscriptions persist across multiple invalidations:

1. Commit A writes into Watch subscription's interval → fire event
2. Watch subscription remains in registry with same intervals
3. Commit B writes into same interval → fire event again
4. Client explicitly calls `update_read_set()` or `remove()` when done

The read set is NOT automatically updated. This means:
- The subscription keeps watching the original key ranges
- Even if the underlying data has changed, the subscription still fires on subsequent overlapping writes
- The client decides when (if ever) to refresh

This is intentional: Watch mode is for clients that want to know about changes but manage their own refresh logic. The subscription is a "dirty flag" — it signals "something changed in your area" without doing the work of refreshing.

## Internal Index Management

When registering or updating a subscription, the registry maintains the `index` HashMap:

```
register(mode, session_id, tx_id, read_ts, read_set):
    id = next_id.fetch_add(1)
    // Index intervals by (coll, idx)
    for each (coll_id, idx_id), intervals in read_set.intervals:
        for each interval in intervals:
            self.index
                .entry((coll_id, idx_id))
                .or_default()
                .push(SubscriptionInterval {
                    subscription_id: id,
                    query_id: interval.query_id,
                    lower: interval.lower.clone(),
                    upper: interval.upper.clone(),
                })
        // Keep sorted by lower for binary search during overlap checks
        sort_by_lower(self.index[(coll_id, idx_id)])
    // Store metadata
    self.subscriptions.insert(id, SubscriptionMeta { mode, session_id, tx_id, read_ts, read_set })
    return id
```

When removing:
```
remove(id):
    self.subscriptions.remove(id)
    // Remove all intervals for this subscription from the index
    for each (key, intervals) in self.index:
        intervals.retain(|i| i.subscription_id != id)
        if intervals.is_empty():
            remove key from index
```

When updating read set (`update_read_set`):
```
update_read_set(id, new_read_set):
    // Remove old intervals
    // (same as remove, but don't delete metadata)
    for each (key, intervals) in self.index:
        intervals.retain(|i| i.subscription_id != id)

    // Insert new intervals
    for each (coll_id, idx_id), intervals in new_read_set.intervals:
        for each interval in intervals:
            self.index.entry((coll_id, idx_id)).or_default().push(...)
        sort(...)

    // Update metadata
    self.subscriptions[id].read_set = new_read_set
```

## Tests

```
t6_register_and_len
    Register a subscription. len() returns 1.

t6_register_returns_unique_ids
    Register 3 subscriptions. All IDs are different.

t6_remove
    Register, then remove. len() returns 0.

t6_check_invalidation_overlap
    Register subscription with interval [10, 20) on (coll_1, idx_1).
    Check against commit writing new_key=15 on (coll_1, idx_1).
    Returns 1 event with correct subscription_id and query_id.

t6_check_invalidation_no_overlap
    Register subscription with interval [10, 20).
    Check against commit writing key=25.
    Returns empty events.

t6_check_invalidation_multiple_query_ids
    Register subscription with intervals query_id=0:[10,20), query_id=1:[30,40).
    Commit writes key=15 and key=35.
    Returns 1 event with affected_query_ids=[0, 1].

t6_check_invalidation_one_event_per_subscription
    Register subscription with 3 intervals. Commit overlaps all 3.
    Returns exactly 1 event (not 3).

t6_notify_mode_removed_after_fire
    Register Notify subscription. Trigger invalidation.
    len() returns 0 (removed after fire).

t6_watch_mode_persists_after_fire
    Register Watch subscription. Trigger invalidation.
    len() returns 1 (still active).

t6_watch_mode_fires_again
    Register Watch subscription. Trigger invalidation twice.
    Both return events.

t6_subscribe_mode_chain_continuation
    Register Subscribe subscription with intervals:
      query_id=0: [0, 10)
      query_id=1: [10, 20)
      query_id=2: [20, 30)
    Trigger invalidation overlapping query_id=1.
    Event has continuation with:
      carried_read_set containing only query_id=0's interval
      first_query_id = 1

t6_subscribe_carry_forward_correctness
    Register Subscribe with query_ids [0, 1, 2, 3, 4].
    Invalidate query_ids [2, 4].
    Carried read set contains query_ids [0, 1] only.
    first_query_id = 2.

t6_subscribe_catalog_conflict_carries_nothing
    Register Subscribe with catalog reads + intervals.
    Concurrent DDL conflicts with catalog read.
    affected_query_ids includes 0, first_query_id = 0.
    carried_read_set is empty (split_before(0) → empty).

t6_update_read_set
    Register subscription. Update with new read set.
    Old intervals no longer trigger. New intervals do.

t6_remove_session
    Register 3 subscriptions for session_id=42, 1 for session_id=99.
    remove_session(42) → len() returns 1.

t6_check_invalidation_old_key
    Commit with old_key overlapping subscription interval.
    Returns event (modification of observed data).

t6_check_invalidation_both_keys
    Commit with old_key=15 (in interval) and new_key=25 (not in interval).
    Returns event (old_key overlap is sufficient).

t6_check_invalidation_catalog_mutations
    Register subscription with CatalogRead::CollectionByName("users").
    Commit includes CatalogMutation::DropCollection { name: "users" }.
    Returns event.
```
