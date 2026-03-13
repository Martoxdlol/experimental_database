use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use exdb_core::types::{CollectionId, IndexId, Ts};
use tokio::sync::mpsc;
use crate::read_set::{ReadSet, QueryId};
use crate::write_set::CatalogMutation;
use crate::commit_log::IndexKeyWrite;
use crate::occ;

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
    /// On invalidation: auto-start new tx, carry forward unaffected read set.
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
pub struct SubscriptionMeta {
    pub mode: SubscriptionMode,
    pub session_id: u64,
    pub tx_id: TxId,
    pub read_ts: Ts,
    pub read_set: ReadSet,
    pub event_tx: mpsc::Sender<InvalidationEvent>,
}

/// Invalidation event produced when a commit overlaps with a subscription.
#[derive(Debug)]
pub struct InvalidationEvent {
    pub subscription_id: SubscriptionId,
    /// The query IDs whose intervals were overlapped, sorted ascending.
    pub affected_query_ids: Vec<QueryId>,
    /// The commit_ts that caused the invalidation.
    pub commit_ts: Ts,
    /// Chain continuation info (present only for Subscribe mode).
    pub continuation: Option<ChainContinuation>,
}

/// Chain continuation for Subscribe mode.
#[derive(Debug)]
pub struct ChainContinuation {
    pub new_tx_id: TxId,
    pub new_ts: Ts,
    pub carried_read_set: ReadSet,
    pub first_query_id: QueryId,
}

/// The subscription registry.
pub struct SubscriptionRegistry {
    /// Intervals indexed by (collection, index) for fast overlap checks.
    /// Within each group, intervals are sorted by lower for overlap checks.
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>,
    /// Subscription metadata by ID.
    subscriptions: HashMap<SubscriptionId, SubscriptionMeta>,
    /// Next subscription ID counter.
    next_id: AtomicU64,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            subscriptions: HashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Register a committed transaction's read set as a subscription.
    pub fn register(
        &mut self,
        mode: SubscriptionMode,
        session_id: u64,
        tx_id: TxId,
        read_ts: Ts,
        read_set: ReadSet,
        event_tx: mpsc::Sender<InvalidationEvent>,
    ) -> SubscriptionId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.index_intervals(id, &read_set);
        self.subscriptions.insert(id, SubscriptionMeta {
            mode,
            session_id,
            tx_id,
            read_ts,
            read_set,
            event_tx,
        });
        id
    }

    /// Remove a subscription explicitly.
    pub fn remove(&mut self, id: SubscriptionId) {
        self.subscriptions.remove(&id);
        self.remove_intervals_for(id);
    }

    /// Check all subscriptions against a new commit's index key writes.
    ///
    /// Returns all InvalidationEvents (does NOT push them to channels).
    pub fn check_invalidation(
        &mut self,
        commit_ts: Ts,
        index_writes: &BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>,
        catalog_mutations: &[CatalogMutation],
        mut allocate_tx: impl FnMut() -> TxId,
    ) -> Vec<InvalidationEvent> {
        // Phase 1: find all (subscription_id, query_id) overlaps from index writes
        let mut affected: HashMap<SubscriptionId, BTreeSet<QueryId>> = HashMap::new();

        for ((coll_id, idx_id), key_writes) in index_writes {
            let sub_intervals = match self.index.get(&(*coll_id, *idx_id)) {
                Some(v) => v,
                None => continue,
            };

            for kw in key_writes {
                for sub_interval in sub_intervals {
                    let overlaps_old = kw.old_key.as_deref()
                        .map(|k| key_overlaps_sub_interval(k, sub_interval))
                        .unwrap_or(false);
                    let overlaps_new = kw.new_key.as_deref()
                        .map(|k| key_overlaps_sub_interval(k, sub_interval))
                        .unwrap_or(false);

                    if overlaps_old || overlaps_new {
                        affected
                            .entry(sub_interval.subscription_id)
                            .or_default()
                            .insert(sub_interval.query_id);
                    }
                }
            }
        }

        // Phase 1b: check catalog subscriptions
        if !catalog_mutations.is_empty() {
            let sub_ids: Vec<SubscriptionId> = self.subscriptions.keys().copied().collect();
            for sub_id in sub_ids {
                if let Some(meta) = self.subscriptions.get(&sub_id) {
                    if occ::catalog_conflicts(&meta.read_set.catalog_reads, catalog_mutations) {
                        // Catalog conflicts affect all queries (query_id = 0)
                        affected.entry(sub_id).or_default().insert(0);
                    }
                }
            }
        }

        // Phase 2: build events
        let mut events = Vec::new();
        let mut notify_removals = Vec::new();

        for (sub_id, query_ids) in affected {
            let meta = match self.subscriptions.get(&sub_id) {
                Some(m) => m,
                None => continue,
            };

            let sorted_qids: Vec<QueryId> = query_ids.into_iter().collect(); // BTreeSet iterates sorted

            let continuation = if meta.mode == SubscriptionMode::Subscribe {
                let first_qid = sorted_qids[0];
                let carried = meta.read_set.split_before(first_qid);
                let new_tx_id = allocate_tx();
                Some(ChainContinuation {
                    new_tx_id,
                    new_ts: commit_ts,
                    carried_read_set: carried,
                    first_query_id: first_qid,
                })
            } else {
                None
            };

            events.push(InvalidationEvent {
                subscription_id: sub_id,
                affected_query_ids: sorted_qids,
                commit_ts,
                continuation,
            });

            if meta.mode == SubscriptionMode::Notify {
                notify_removals.push(sub_id);
            }
        }

        // Phase 3: remove one-shot subscriptions
        for sub_id in notify_removals {
            self.remove(sub_id);
        }

        events
    }

    /// Update a subscription's read set.
    pub fn update_read_set(&mut self, id: SubscriptionId, new_read_set: ReadSet) {
        // Remove old intervals for this subscription
        self.remove_intervals_for(id);

        // Insert new intervals
        self.index_intervals(id, &new_read_set);

        // Update metadata
        if let Some(meta) = self.subscriptions.get_mut(&id) {
            meta.read_set = new_read_set;
        }
    }

    /// Remove all subscriptions for a session.
    pub fn remove_session(&mut self, session_id: u64) {
        let to_remove: Vec<SubscriptionId> = self.subscriptions
            .iter()
            .filter(|(_, m)| m.session_id == session_id)
            .map(|(id, _)| *id)
            .collect();
        for id in to_remove {
            self.remove(id);
        }
    }

    /// Push invalidation events to subscriber channels.
    /// Uses try_send — non-blocking. Events for removed/full subscriptions are dropped.
    pub fn push_events(&self, events: Vec<InvalidationEvent>) {
        for event in events {
            if let Some(meta) = self.subscriptions.get(&event.subscription_id) {
                // Ignore send error: channel full or receiver dropped
                let _ = meta.event_tx.try_send(event);
            }
        }
    }

    /// Number of active subscriptions.
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    // ─── Internal helpers ───

    fn index_intervals(&mut self, id: SubscriptionId, read_set: &ReadSet) {
        for ((coll_id, idx_id), intervals) in &read_set.intervals {
            let group = self.index.entry((*coll_id, *idx_id)).or_default();
            for interval in intervals {
                group.push(SubscriptionInterval {
                    subscription_id: id,
                    query_id: interval.query_id,
                    lower: interval.lower.clone(),
                    upper: interval.upper.clone(),
                });
            }
            // Keep sorted by lower for binary search
            group.sort_by(|a, b| a.lower.cmp(&b.lower));
        }
    }

    fn remove_intervals_for(&mut self, id: SubscriptionId) {
        let mut empty_keys = Vec::new();
        for (key, intervals) in &mut self.index {
            intervals.retain(|i| i.subscription_id != id);
            if intervals.is_empty() {
                empty_keys.push(*key);
            }
        }
        for key in empty_keys {
            self.index.remove(&key);
        }
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a key overlaps with a SubscriptionInterval.
fn key_overlaps_sub_interval(key: &[u8], interval: &SubscriptionInterval) -> bool {
    if key < interval.lower.as_slice() {
        return false;
    }
    match &interval.upper {
        Bound::Unbounded => true,
        Bound::Excluded(upper) => key < upper.as_slice(),
        Bound::Included(_) => unreachable!("only Excluded or Unbounded"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;
    use exdb_core::types::DocId;
    use crate::read_set::{ReadInterval, ReadSet, CatalogRead};

    fn make_event_channel(capacity: usize) -> (mpsc::Sender<InvalidationEvent>, mpsc::Receiver<InvalidationEvent>) {
        mpsc::channel(capacity)
    }

    fn make_read_set_with_interval(
        coll_id: u64,
        idx_id: u64,
        query_id: QueryId,
        lower: u8,
        upper: Option<u8>,
    ) -> ReadSet {
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(coll_id), IndexId(idx_id), ReadInterval {
            query_id,
            lower: vec![lower],
            upper: match upper {
                Some(u) => Bound::Excluded(vec![u]),
                None => Bound::Unbounded,
            },
        });
        rs
    }

    fn make_index_writes(
        coll_id: u64,
        idx_id: u64,
        old_key: Option<u8>,
        new_key: Option<u8>,
    ) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> {
        let mut map = BTreeMap::new();
        map.insert((CollectionId(coll_id), IndexId(idx_id)), vec![IndexKeyWrite {
            doc_id: DocId([0u8; 16]),
            old_key: old_key.map(|k| vec![k]),
            new_key: new_key.map(|k| vec![k]),
        }]);
        map
    }

    #[test]
    fn t6_register_and_len() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        reg.register(SubscriptionMode::Notify, 1, 1, 0, ReadSet::new(), tx);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn t6_register_returns_unique_ids() {
        let mut reg = SubscriptionRegistry::new();
        let ids: Vec<_> = (0..3).map(|_| {
            let (tx, _rx) = make_event_channel(8);
            reg.register(SubscriptionMode::Notify, 1, 1, 0, ReadSet::new(), tx)
        }).collect();
        assert_eq!(ids[0], ids[1] - 1);
        assert_eq!(ids[1], ids[2] - 1);
        // All unique
        let set: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn t6_remove() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let id = reg.register(SubscriptionMode::Notify, 1, 1, 0, ReadSet::new(), tx);
        reg.remove(id);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn t6_check_invalidation_overlap() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let sub_id = reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(15)); // key=15 in [10,20)
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subscription_id, sub_id);
        assert_eq!(events[0].affected_query_ids, vec![0]);
    }

    #[test]
    fn t6_check_invalidation_no_overlap() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(25)); // key=25 outside [10,20)
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert!(events.is_empty());
    }

    #[test]
    fn t6_check_invalidation_multiple_query_ids() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 0, lower: vec![10], upper: Bound::Excluded(vec![20]) });
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 1, lower: vec![30], upper: Bound::Excluded(vec![40]) });
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let mut writes = BTreeMap::new();
        writes.insert((CollectionId(1), IndexId(1)), vec![
            IndexKeyWrite { doc_id: DocId([0u8; 16]), old_key: None, new_key: Some(vec![15]) },
            IndexKeyWrite { doc_id: DocId([1u8; 16]), old_key: None, new_key: Some(vec![35]) },
        ]);
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(events.len(), 1);
        let mut qids = events[0].affected_query_ids.clone();
        qids.sort();
        assert_eq!(qids, vec![0, 1]);
    }

    #[test]
    fn t6_check_invalidation_one_event_per_subscription() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        for i in 0..3u8 {
            rs.add_interval(CollectionId(1), IndexId(1), ReadInterval {
                query_id: i as u32,
                lower: vec![i * 10],
                upper: Bound::Excluded(vec![i * 10 + 5]),
            });
        }
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let mut writes = BTreeMap::new();
        writes.insert((CollectionId(1), IndexId(1)), vec![
            IndexKeyWrite { doc_id: DocId([0u8; 16]), old_key: None, new_key: Some(vec![2]) },
            IndexKeyWrite { doc_id: DocId([1u8; 16]), old_key: None, new_key: Some(vec![12]) },
            IndexKeyWrite { doc_id: DocId([2u8; 16]), old_key: None, new_key: Some(vec![22]) },
        ]);
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        // One event for the subscription, not 3
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn t6_notify_mode_removed_after_fire() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Notify, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(15));
        reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn t6_watch_mode_persists_after_fire() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(15));
        reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn t6_watch_mode_fires_again() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(15));
        let e1 = reg.check_invalidation(10, &writes, &[], || 99);
        let e2 = reg.check_invalidation(11, &writes, &[], || 99);
        assert_eq!(e1.len(), 1);
        assert_eq!(e2.len(), 1);
    }

    #[test]
    fn t6_subscribe_mode_chain_continuation() {
        // Use non-adjacent intervals to prevent merging by add_interval.
        // Gaps between ranges: [0,5), [10,15), [20,25) — no adjacency, no merges.
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 0, lower: vec![0],  upper: Bound::Excluded(vec![5])  });
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 1, lower: vec![10], upper: Bound::Excluded(vec![15]) });
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 2, lower: vec![20], upper: Bound::Excluded(vec![25]) });
        reg.register(SubscriptionMode::Subscribe, 1, 1, 5, rs, tx);

        // Key=12 hits query_id=1 (interval [10, 15))
        let writes = make_index_writes(1, 1, None, Some(12));
        let events = reg.check_invalidation(10, &writes, &[], || 42);
        assert_eq!(events.len(), 1);
        let cont = events[0].continuation.as_ref().unwrap();
        assert_eq!(cont.first_query_id, 1);
        assert_eq!(cont.new_tx_id, 42);
        // Carried read set should contain only query_id=0 (split_before(1))
        let carried_count = cont.carried_read_set.interval_count();
        assert_eq!(carried_count, 1);
        let carried_group = &cont.carried_read_set.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(carried_group[0].query_id, 0);
    }

    #[test]
    fn t6_subscribe_carry_forward_correctness() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        for qid in 0..5u32 {
            rs.add_interval(CollectionId(1), IndexId(1), ReadInterval {
                query_id: qid,
                lower: vec![qid as u8 * 10],
                upper: Bound::Excluded(vec![qid as u8 * 10 + 5]),
            });
        }
        reg.register(SubscriptionMode::Subscribe, 1, 1, 5, rs, tx);

        // Invalidate query_ids 2 and 4
        let mut writes = BTreeMap::new();
        writes.insert((CollectionId(1), IndexId(1)), vec![
            IndexKeyWrite { doc_id: DocId([0u8; 16]), old_key: None, new_key: Some(vec![22]) }, // hits qid=2
            IndexKeyWrite { doc_id: DocId([1u8; 16]), old_key: None, new_key: Some(vec![42]) }, // hits qid=4
        ]);
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(events.len(), 1);
        let cont = events[0].continuation.as_ref().unwrap();
        assert_eq!(cont.first_query_id, 2);
        // Carried: qid 0 and 1 only
        let carried = &cont.carried_read_set.intervals[&(CollectionId(1), IndexId(1))];
        assert_eq!(carried.len(), 2);
        assert!(carried.iter().all(|i| i.query_id < 2));
    }

    #[test]
    fn t6_subscribe_catalog_conflict_carries_nothing() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        rs.add_interval(CollectionId(1), IndexId(1), ReadInterval { query_id: 1, lower: vec![0], upper: Bound::Excluded(vec![10]) });
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        reg.register(SubscriptionMode::Subscribe, 1, 1, 5, rs, tx);

        // Catalog mutation that conflicts
        let catalog_muts = vec![CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "users".into(),
        }];
        let events = reg.check_invalidation(10, &BTreeMap::new(), &catalog_muts, || 99);
        assert_eq!(events.len(), 1);
        let cont = events[0].continuation.as_ref().unwrap();
        assert_eq!(cont.first_query_id, 0);
        // split_before(0) → empty
        assert!(cont.carried_read_set.is_empty());
    }

    #[test]
    fn t6_update_read_set() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let sub_id = reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        // Old interval [10,20) fires on key=15
        let writes = make_index_writes(1, 1, None, Some(15));
        let e1 = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(e1.len(), 1);

        // Update with new interval [50,60)
        let new_rs = make_read_set_with_interval(1, 1, 0, 50, Some(60));
        reg.update_read_set(sub_id, new_rs);

        // Old interval should NOT trigger
        let e2 = reg.check_invalidation(11, &writes, &[], || 99);
        assert!(e2.is_empty());

        // New interval [50,60) triggers on key=55
        let writes2 = make_index_writes(1, 1, None, Some(55));
        let e3 = reg.check_invalidation(12, &writes2, &[], || 99);
        assert_eq!(e3.len(), 1);
    }

    #[test]
    fn t6_remove_session() {
        let mut reg = SubscriptionRegistry::new();
        for _ in 0..3 {
            let (tx, _rx) = make_event_channel(8);
            reg.register(SubscriptionMode::Watch, 42, 1, 0, ReadSet::new(), tx);
        }
        let (tx, _rx) = make_event_channel(8);
        reg.register(SubscriptionMode::Watch, 99, 1, 0, ReadSet::new(), tx);

        reg.remove_session(42);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn t6_check_invalidation_old_key() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, Some(15), None); // old_key=15 in interval
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn t6_check_invalidation_both_keys() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        // old_key=15 in interval, new_key=25 outside
        let writes = make_index_writes(1, 1, Some(15), Some(25));
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn t6_check_invalidation_catalog_mutations() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let mut rs = ReadSet::new();
        rs.add_catalog_read(CatalogRead::CollectionByName("users".into()));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let catalog_muts = vec![CatalogMutation::DropCollection {
            collection_id: CollectionId(1),
            name: "users".into(),
        }];
        let events = reg.check_invalidation(10, &BTreeMap::new(), &catalog_muts, || 99);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn t6_push_events() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let sub_id = reg.register(SubscriptionMode::Watch, 1, 1, 5, rs, tx);

        let event = InvalidationEvent {
            subscription_id: sub_id,
            affected_query_ids: vec![0],
            commit_ts: 10,
            continuation: None,
        };
        reg.push_events(vec![event]);
        let received = rx.try_recv();
        assert!(received.is_ok());
        assert_eq!(received.unwrap().subscription_id, sub_id);
    }

    #[test]
    fn t6_push_events_unknown_subscription() {
        let reg = SubscriptionRegistry::new();
        let event = InvalidationEvent {
            subscription_id: 9999,
            affected_query_ids: vec![0],
            commit_ts: 10,
            continuation: None,
        };
        // Should not panic
        reg.push_events(vec![event]);
    }

    #[test]
    fn t6_push_events_channel_full() {
        let mut reg = SubscriptionRegistry::new();
        let (tx, mut _rx) = make_event_channel(1); // capacity 1
        let sub_id = reg.register(SubscriptionMode::Watch, 1, 1, 5, ReadSet::new(), tx);

        let make_event = |sub_id| InvalidationEvent {
            subscription_id: sub_id,
            affected_query_ids: vec![0],
            commit_ts: 10,
            continuation: None,
        };

        // Fill channel
        reg.push_events(vec![make_event(sub_id)]);
        // Second push should NOT block or panic (try_send drops)
        reg.push_events(vec![make_event(sub_id)]);
        // Still one item in channel
        assert!(_rx.try_recv().is_ok());
    }

    #[test]
    fn t6_multiple_subscriptions_same_interval() {
        // Two subscriptions watching the same key range
        let mut reg = SubscriptionRegistry::new();
        let (tx1, _rx1) = make_event_channel(8);
        let (tx2, _rx2) = make_event_channel(8);
        let rs1 = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        let rs2 = make_read_set_with_interval(1, 1, 0, 10, Some(20));
        reg.register(SubscriptionMode::Watch, 1, 1, 5, rs1, tx1);
        reg.register(SubscriptionMode::Watch, 2, 2, 5, rs2, tx2);

        let writes = make_index_writes(1, 1, None, Some(15));
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        // Both subscriptions should fire
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn t6_subscribe_mode_no_continuation_no_overlap() {
        // Subscribe mode subscription that doesn't overlap — no event
        let mut reg = SubscriptionRegistry::new();
        let (tx, _rx) = make_event_channel(8);
        let rs = make_read_set_with_interval(1, 1, 0, 50, Some(60));
        reg.register(SubscriptionMode::Subscribe, 1, 1, 5, rs, tx);

        let writes = make_index_writes(1, 1, None, Some(15)); // key=15, interval is [50,60)
        let events = reg.check_invalidation(10, &writes, &[], || 99);
        assert!(events.is_empty());
    }
}
