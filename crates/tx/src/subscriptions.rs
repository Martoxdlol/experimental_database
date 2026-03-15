//! T6: Subscription Registry — reactive invalidation on commit.
//!
//! Manages persistent read-set watches indexed by `(collection, index)` for fast
//! overlap checks. Supports three subscription modes with different lifecycles:
//!
//! - **Notify**: one-shot — fires once on first invalidation, then removed.
//! - **Watch**: persistent — fires on every invalidation, subscription persists.
//! - **Subscribe**: persistent with chain continuation — fires, auto-starts new
//!   transaction, carries forward unaffected read-set intervals.
//!
//! Uses [`ReadInterval::contains_key`] for overlap checks, matching OCC
//! semantics exactly (including LIMIT boundary tightening).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use exdb_core::types::{CollectionId, IndexId, Ts, TxId};
use tokio::sync::mpsc;

use crate::commit_log::IndexKeyWrite;
use crate::read_set::{QueryId, ReadInterval, ReadSet};

/// Unique subscription identifier.
pub type SubscriptionId = u64;

/// Subscription mode — controls post-commit read-set behavior.
///
/// Mutually exclusive: exactly one mode per transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    /// No subscription. Read set discarded after commit.
    None,
    /// One-shot: fire once on first invalidation, then remove.
    Notify,
    /// Persistent: fire on every invalidation, subscription persists.
    /// Client manages re-queries manually.
    Watch,
    /// Persistent with chain continuation: fire, auto-start new transaction,
    /// carry forward unaffected read-set intervals.
    Subscribe,
}

/// An interval stored in the subscription registry index.
///
/// Embeds [`ReadInterval`] (including `limit_boundary`) so that invalidation
/// checks use [`contains_key`](ReadInterval::contains_key) and respect LIMIT
/// tightening, matching OCC semantics exactly.
pub struct SubscriptionInterval {
    /// Which subscription owns this interval.
    pub subscription_id: SubscriptionId,
    /// The read interval with original bounds and limit boundary.
    pub interval: ReadInterval,
}

/// Metadata for a registered subscription.
pub struct SubscriptionMeta {
    /// Subscription mode (Notify/Watch/Subscribe).
    pub mode: SubscriptionMode,
    /// Client session (for bulk cleanup on disconnect).
    pub session_id: u64,
    /// Transaction that produced this subscription.
    pub tx_id: TxId,
    /// Read timestamp of the subscription's transaction.
    pub read_ts: Ts,
    /// Full read set (for `split_before` on chain continuation).
    pub read_set: ReadSet,
    /// Channel sender for pushing [`InvalidationEvent`]s to the client.
    pub event_tx: mpsc::Sender<InvalidationEvent>,
}

/// Invalidation event — one per affected subscription per commit.
///
/// Contains all invalidated query IDs (sorted ascending) so the client knows
/// which queries need re-execution. A single commit that overlaps N queries in
/// the same subscription produces exactly **one** event with N query IDs.
#[derive(Debug)]
pub struct InvalidationEvent {
    /// Which subscription was invalidated.
    pub subscription_id: SubscriptionId,
    /// Sorted ascending. Used by client to determine re-execution scope.
    pub affected_query_ids: Vec<QueryId>,
    /// The `commit_ts` that triggered the invalidation.
    pub commit_ts: Ts,
    /// Chain continuation (present only for Subscribe mode).
    pub continuation: Option<ChainContinuation>,
}

/// Chain continuation for Subscribe mode.
///
/// When a subscription is invalidated, this packages the auto-started new
/// transaction and the carried-forward read set.
#[derive(Debug)]
pub struct ChainContinuation {
    /// New transaction auto-started at the invalidation timestamp.
    pub new_tx_id: TxId,
    /// Read snapshot for the new transaction (= invalidating `commit_ts`).
    pub new_ts: Ts,
    /// Read-set intervals from queries **before** the first invalidated query.
    /// Carried forward because they were provably unaffected.
    pub carried_read_set: ReadSet,
    /// The first `query_id` that needs re-execution (= `min(affected_query_ids)`).
    pub first_query_id: QueryId,
}

/// The subscription registry — indexes intervals for fast invalidation lookup.
///
/// All methods are called by the **replication task** (T7 steps 9–11), except
/// [`remove_session`](Self::remove_session) which may be called from any task
/// via a shared reference.
pub struct SubscriptionRegistry {
    /// Intervals grouped by `(collection, index)` for range overlap checks.
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    /// Metadata per subscription.
    subscriptions: HashMap<SubscriptionId, SubscriptionMeta>,
    /// Session → subscription IDs for bulk cleanup on disconnect.
    session_index: HashMap<u64, Vec<SubscriptionId>>,
    /// Monotonic ID allocator.
    next_id: SubscriptionId,
}

impl SubscriptionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            subscriptions: HashMap::new(),
            session_index: HashMap::new(),
            next_id: 1,
        }
    }

    /// Register a read set as a subscription.
    ///
    /// `read_set` must already have had [`extend_for_deltas`](ReadSet::extend_for_deltas)
    /// applied (commit step 10a) so that any stale `limit_boundary` from the
    /// transaction's own writes is cleared.
    ///
    /// Returns the assigned [`SubscriptionId`].
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
        self.subscriptions.insert(
            id,
            SubscriptionMeta {
                mode,
                session_id,
                tx_id,
                read_ts,
                read_set,
                event_tx,
            },
        );

        // Update session index
        self.session_index.entry(session_id).or_default().push(id);

        id
    }

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

    /// Check all subscriptions against a new commit's writes.
    ///
    /// Uses [`ReadInterval::contains_key`] for overlap, respecting effective
    /// intervals. Returns one [`InvalidationEvent`] per affected subscription,
    /// paired with the cloned `event_tx` for delivery.
    ///
    /// For **Subscribe** mode, computes [`ChainContinuation`] with carried read set.
    /// For **Notify** mode, marks for removal after event delivery.
    ///
    /// `allocate_tx_id`: closure to allocate new `TxId`s for Subscribe mode.
    pub fn check_invalidation(
        &mut self,
        commit_ts: Ts,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
        mut allocate_tx_id: impl FnMut() -> TxId,
    ) -> Vec<(InvalidationEvent, mpsc::Sender<InvalidationEvent>)> {
        // Step 1: Collect affected (subscription_id, query_id) pairs
        let mut affected: HashMap<SubscriptionId, BTreeSet<QueryId>> = HashMap::new();

        for (&(coll, idx), key_writes) in index_writes {
            if let Some(sub_intervals) = self.index.get(&(coll, idx)) {
                for write in key_writes {
                    for sub_interval in sub_intervals {
                        let hit = write
                            .old_key
                            .as_ref()
                            .is_some_and(|k| sub_interval.interval.contains_key(k))
                            || write
                                .new_key
                                .as_ref()
                                .is_some_and(|k| sub_interval.interval.contains_key(k));
                        if hit {
                            affected
                                .entry(sub_interval.subscription_id)
                                .or_default()
                                .insert(sub_interval.interval.query_id);
                        }
                    }
                }
            }
        }

        // Step 2: Build events
        let mut events = Vec::new();
        let mut to_remove = Vec::new();

        for (sub_id, query_ids) in affected {
            let meta = match self.subscriptions.get(&sub_id) {
                Some(m) => m,
                None => continue,
            };
            let affected_ids: Vec<QueryId> = query_ids.into_iter().collect();
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

            let event = InvalidationEvent {
                subscription_id: sub_id,
                affected_query_ids: affected_ids,
                commit_ts,
                continuation,
            };
            let tx = meta.event_tx.clone();

            events.push((event, tx));

            match meta.mode {
                SubscriptionMode::Notify | SubscriptionMode::Subscribe => {
                    to_remove.push(sub_id);
                }
                SubscriptionMode::Watch => {} // Persists
                SubscriptionMode::None => {}  // Should not happen
            }
        }

        // Step 3: Clean up Notify/Subscribe subscriptions
        for id in to_remove {
            self.remove(id);
        }

        events
    }

    /// Update a subscription's read set (for chain commit or Watch refresh).
    ///
    /// Replaces the existing intervals in the index with the new read set's
    /// intervals.
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

    /// Remove all subscriptions for a session (on disconnect).
    pub fn remove_session(&mut self, session_id: u64) {
        if let Some(sub_ids) = self.session_index.remove(&session_id) {
            for id in sub_ids {
                // Inline removal to avoid double-lookup on session_index
                for intervals in self.index.values_mut() {
                    intervals.retain(|si| si.subscription_id != id);
                }
                self.index.retain(|_, v| !v.is_empty());
                self.subscriptions.remove(&id);
            }
        }
    }

    /// Push invalidation events to subscriber channels.
    ///
    /// Uses `try_send`; full channels or closed receivers are silently dropped.
    pub fn push_events(events: Vec<(InvalidationEvent, mpsc::Sender<InvalidationEvent>)>) {
        for (event, tx) in events {
            let _ = tx.try_send(event);
        }
    }

    /// Number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_set::LimitBoundary;
    use std::ops::Bound;

    fn make_interval(query_id: QueryId, lo: u8, hi: u8) -> ReadInterval {
        ReadInterval {
            query_id,
            lower: Bound::Included(vec![lo]),
            upper: Bound::Excluded(vec![hi]),
            limit_boundary: None,
        }
    }

    fn make_read_set(coll: CollectionId, idx: IndexId, intervals: Vec<ReadInterval>) -> ReadSet {
        let mut rs = ReadSet::new();
        for interval in intervals {
            rs.add_interval(coll, idx, interval);
        }
        rs
    }

    fn make_writes(
        coll: CollectionId,
        idx: IndexId,
        old_key: Option<Vec<u8>>,
        new_key: Option<Vec<u8>>,
    ) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> {
        let mut map = BTreeMap::new();
        map.entry((coll, idx))
            .or_insert_with(Vec::new)
            .push(IndexKeyWrite {
                doc_id: exdb_core::types::DocId([0; 16]),
                old_key,
                new_key,
            });
        map
    }

    // ─── Registration and removal ───

    #[test]
    fn register_assigns_id() {
        let mut reg = SubscriptionRegistry::new();
        let (tx1, _rx1) = mpsc::channel(8);
        let (tx2, _rx2) = mpsc::channel(8);
        let rs1 = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        let rs2 = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 30, 40)]);
        let id1 = reg.register(SubscriptionMode::Watch, 1, 1, 1, rs1, tx1);
        let id2 = reg.register(SubscriptionMode::Watch, 1, 2, 2, rs2, tx2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn remove_cleans_up() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        let id = reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);
        reg.remove(id);
        assert_eq!(reg.subscription_count(), 0);
        assert!(reg.index.is_empty());
    }

    #[test]
    fn remove_session_clears_all() {
        let mut reg = SubscriptionRegistry::new();
        for i in 0..3 {
            let (tx, _rx) = mpsc::channel(8);
            let rs = make_read_set(
                CollectionId(1),
                IndexId(1),
                vec![make_interval(i as u32, i * 10, i * 10 + 5)],
            );
            reg.register(SubscriptionMode::Watch, 42, i as u64, 1, rs, tx);
        }
        assert_eq!(reg.subscription_count(), 3);
        reg.remove_session(42);
        assert_eq!(reg.subscription_count(), 0);
    }

    // ─── No invalidation ───

    #[test]
    fn no_invalidation_disjoint() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![25]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert!(events.is_empty());
    }

    #[test]
    fn no_invalidation_beyond_limit() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![30]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert!(events.is_empty());
    }

    // ─── Notify mode ───

    #[test]
    fn notify_fires_once() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        let id = reg.register(SubscriptionMode::Notify, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert_eq!(events.len(), 1);
        SubscriptionRegistry::push_events(events);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.subscription_id, id);
        assert_eq!(event.affected_query_ids, vec![0]);
        assert!(event.continuation.is_none());

        // Subscription removed after fire
        assert_eq!(reg.subscription_count(), 0);
    }

    #[test]
    fn notify_no_second_fire() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        reg.register(SubscriptionMode::Notify, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events1 = reg.check_invalidation(5, &writes, || 100);
        assert_eq!(events1.len(), 1);
        SubscriptionRegistry::push_events(events1);

        // Second write — no more events
        let events2 = reg.check_invalidation(6, &writes, || 101);
        assert!(events2.is_empty());
    }

    // ─── Watch mode ───

    #[test]
    fn watch_fires_repeatedly() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));

        let events1 = reg.check_invalidation(5, &writes, || 100);
        assert_eq!(events1.len(), 1);
        SubscriptionRegistry::push_events(events1);
        assert!(rx.try_recv().is_ok());

        let events2 = reg.check_invalidation(6, &writes, || 101);
        assert_eq!(events2.len(), 1);
        SubscriptionRegistry::push_events(events2);
        assert!(rx.try_recv().is_ok());

        // Subscription still registered
        assert_eq!(reg.subscription_count(), 1);
    }

    // ─── Subscribe mode ───

    #[test]
    fn subscribe_chain_continuation() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let rs = make_read_set(
            CollectionId(1),
            IndexId(1),
            vec![
                make_interval(0, 10, 20),
                make_interval(1, 30, 40),
                make_interval(2, 50, 60),
            ],
        );
        reg.register(SubscriptionMode::Subscribe, 1, 1, 1, rs, tx);

        // Write overlaps Q1 (key 35 is in [30, 40))
        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![35]));
        let events = reg.check_invalidation(10, &writes, || 42);
        assert_eq!(events.len(), 1);
        SubscriptionRegistry::push_events(events);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.affected_query_ids, vec![1]);

        let cont = event.continuation.unwrap();
        assert_eq!(cont.new_tx_id, 42);
        assert_eq!(cont.new_ts, 10);
        assert_eq!(cont.first_query_id, 1);
        // Carried read set has Q0 only (query_id < 1)
        assert_eq!(cont.carried_read_set.interval_count(), 1);
    }

    #[test]
    fn subscribe_removed_after_fire() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        reg.register(SubscriptionMode::Subscribe, 1, 1, 1, rs, tx);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events = reg.check_invalidation(5, &writes, || 100);
        SubscriptionRegistry::push_events(events);

        assert_eq!(reg.subscription_count(), 0);
    }

    // ─── update_read_set ───

    #[test]
    fn update_replaces_intervals() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        let id = reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        // Update to cover [50, 60) instead
        let new_rs = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 50, 60)]);
        reg.update_read_set(id, new_rs);

        // Old interval no longer triggers
        let writes_old = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events = reg.check_invalidation(5, &writes_old, || 100);
        assert!(events.is_empty());

        // New interval triggers
        let writes_new = make_writes(CollectionId(1), IndexId(1), None, Some(vec![55]));
        let events = reg.check_invalidation(6, &writes_new, || 101);
        assert_eq!(events.len(), 1);
    }

    // ─── Edge cases ───

    #[test]
    fn multiple_subscriptions_one_commit() {
        let mut reg = SubscriptionRegistry::new();
        let (tx1, _rx1) = mpsc::channel(8);
        let (tx2, _rx2) = mpsc::channel(8);
        let rs1 = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        let rs2 = make_read_set(CollectionId(1), IndexId(1), vec![make_interval(0, 10, 20)]);
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs1, tx1);
        reg.register(SubscriptionMode::Watch, 2, 2, 2, rs2, tx2);

        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn multiple_queries_one_event() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let rs = make_read_set(
            CollectionId(1),
            IndexId(1),
            vec![
                make_interval(0, 10, 20),
                make_interval(1, 30, 40),
                make_interval(2, 10, 20), // Overlaps same range as Q0
            ],
        );
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        // Write at 15 overlaps Q0 and Q2
        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![15]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert_eq!(events.len(), 1); // One event per subscription
        SubscriptionRegistry::push_events(events);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.affected_query_ids, vec![0, 2]);
    }

    #[test]
    fn limit_boundary_respected_in_invalidation() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = mpsc::channel(8);
        let mut rs = ReadSet::new();
        rs.add_interval(
            CollectionId(1),
            IndexId(1),
            ReadInterval {
                query_id: 0,
                lower: Bound::Included(vec![10]),
                upper: Bound::Excluded(vec![50]),
                limit_boundary: Some(LimitBoundary::Upper(vec![20])),
            },
        );
        reg.register(SubscriptionMode::Watch, 1, 1, 1, rs, tx);

        // Write at 30 — in original range but beyond limit
        let writes = make_writes(CollectionId(1), IndexId(1), None, Some(vec![30]));
        let events = reg.check_invalidation(5, &writes, || 100);
        assert!(events.is_empty());
    }

    #[test]
    fn subscribe_catalog_conflict_via_interval() {
        use crate::read_set::{CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX};

        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = mpsc::channel(8);
        let rs = make_read_set(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            vec![ReadInterval {
                query_id: 0,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            }],
        );
        reg.register(SubscriptionMode::Subscribe, 1, 1, 1, rs, tx);

        let writes = make_writes(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            None,
            Some(b"new_coll".to_vec()),
        );
        let events = reg.check_invalidation(5, &writes, || 42);
        assert_eq!(events.len(), 1);
        SubscriptionRegistry::push_events(events);

        let event = rx.try_recv().unwrap();
        let cont = event.continuation.unwrap();
        assert_eq!(cont.first_query_id, 0);
        assert_eq!(cont.carried_read_set.interval_count(), 0); // Nothing before Q0
    }
}
