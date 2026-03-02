//! Subscription registry and invalidation.

use std::collections::HashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::tx::{ReadSet, WriteSummary};
use crate::types::{CollectionId, DocId, InvalidationEvent, SubId};

// ── Subscription ──────────────────────────────────────────────────────────────

/// A live subscription. The user holds this and receives invalidation events.
pub struct Subscription {
    pub id: SubId,
    pub rx: mpsc::Receiver<InvalidationEvent>,
}

// ── SubscriptionEntry ─────────────────────────────────────────────────────────

struct SubscriptionEntry {
    read_set: ReadSet,
    tx: mpsc::Sender<InvalidationEvent>,
    active: bool,
}

// ── SubscriptionRegistry ──────────────────────────────────────────────────────

/// Central registry for all subscriptions.
pub struct SubscriptionRegistry {
    inner: RwLock<RegistryInner>,
    next_id: std::sync::atomic::AtomicU64,
}

struct RegistryInner {
    subs: HashMap<SubId, SubscriptionEntry>,
    /// collection_id -> subscription IDs that scanned the collection
    scan_index: HashMap<CollectionId, Vec<SubId>>,
    /// (collection_id, doc_id) -> subscription IDs that point-read this doc
    point_index: HashMap<(CollectionId, DocId), Vec<SubId>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        SubscriptionRegistry {
            inner: RwLock::new(RegistryInner {
                subs: HashMap::new(),
                scan_index: HashMap::new(),
                point_index: HashMap::new(),
            }),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Register a new subscription and return a Subscription handle.
    pub fn register(&self, read_set: ReadSet) -> Subscription {
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(128);

        let mut inner = self.inner.write();

        // Build indexes
        for scan in &read_set.collection_scans {
            inner.scan_index.entry(scan.collection_id).or_default().push(id);
        }
        for pr in &read_set.point_reads {
            inner
                .point_index
                .entry((pr.collection_id, pr.doc_id))
                .or_default()
                .push(id);
        }

        inner.subs.insert(id, SubscriptionEntry { read_set, tx, active: true });

        Subscription { id, rx }
    }

    /// Invalidate subscriptions affected by a committed write summary.
    pub fn invalidate(&self, summary: &WriteSummary) {
        let mut to_invalidate: Vec<SubId> = Vec::new();

        {
            let inner = self.inner.read();

            // Check scan-based subscriptions
            let affected_collections: std::collections::HashSet<CollectionId> =
                summary.doc_writes.iter().map(|(cid, _)| *cid).collect();

            for coll_id in &affected_collections {
                if let Some(sub_ids) = inner.scan_index.get(coll_id) {
                    to_invalidate.extend_from_slice(sub_ids);
                }
            }

            // Check point-read subscriptions
            for (coll_id, doc_id) in &summary.doc_writes {
                if let Some(sub_ids) = inner.point_index.get(&(*coll_id, *doc_id)) {
                    to_invalidate.extend_from_slice(sub_ids);
                }
            }
        }

        // Deduplicate and send events
        to_invalidate.sort_unstable();
        to_invalidate.dedup();

        for sub_id in to_invalidate {
            self.send_invalidation(sub_id, summary.commit_ts);
        }
    }

    fn send_invalidation(&self, sub_id: SubId, commit_ts: crate::types::Ts) {
        let inner = self.inner.read();
        if let Some(entry) = inner.subs.get(&sub_id) {
            if entry.active {
                let event = InvalidationEvent {
                    subscription_id: sub_id,
                    invalidated_at_ts: commit_ts,
                };
                // Non-blocking send; if the channel is full, the user missed an event
                // (they'll get it on the next invalidation or can re-poll)
                let _ = entry.tx.try_send(event);
            }
        }
    }

    /// Remove a subscription (cleanup when user drops it).
    pub fn unregister(&self, sub_id: SubId) {
        let mut inner = self.inner.write();
        let Some(entry) = inner.subs.remove(&sub_id) else { return };

        // Clean up indexes
        for scan in &entry.read_set.collection_scans {
            if let Some(ids) = inner.scan_index.get_mut(&scan.collection_id) {
                ids.retain(|&id| id != sub_id);
            }
        }
        for pr in &entry.read_set.point_reads {
            if let Some(ids) = inner.point_index.get_mut(&(pr.collection_id, pr.doc_id)) {
                ids.retain(|&id| id != sub_id);
            }
        }
    }
}
