//! B7: Subscription handle (RAII wrapper).
//!
//! Wraps a subscription registration in the L5 `SubscriptionRegistry` with
//! automatic cleanup on Drop.

use std::sync::Arc;

use exdb_tx::{InvalidationEvent, ReadSet, SubscriptionId, SubscriptionRegistry};
use parking_lot::RwLock;
use tokio::sync::mpsc;

/// RAII handle to a subscription. Automatically unsubscribes on drop.
pub struct SubscriptionHandle {
    id: SubscriptionId,
    registry: Arc<RwLock<SubscriptionRegistry>>,
    events: Option<mpsc::Receiver<InvalidationEvent>>,
}

impl SubscriptionHandle {
    /// Create a new subscription handle.
    pub(crate) fn new(
        id: SubscriptionId,
        registry: Arc<RwLock<SubscriptionRegistry>>,
        events: mpsc::Receiver<InvalidationEvent>,
    ) -> Self {
        SubscriptionHandle {
            id,
            registry,
            events: Some(events),
        }
    }

    /// Get the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Wait for the next invalidation event.
    /// Returns `None` if the subscription has been removed or the channel closed.
    pub async fn next_event(&mut self) -> Option<InvalidationEvent> {
        self.events.as_mut()?.recv().await
    }

    /// Update the read set for this subscription (Watch mode re-subscription).
    pub fn update_read_set(&self, new_read_set: ReadSet) {
        let mut registry = self.registry.write();
        registry.update_read_set(self.id, new_read_set);
    }

    /// Explicitly unsubscribe and consume the handle.
    pub fn unsubscribe(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        self.events.take(); // drop receiver
        self.registry.write().remove(self.id);
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.cleanup();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_tx::{ReadInterval, ReadSet, SubscriptionMode, SubscriptionRegistry};
    use std::ops::Bound;

    fn make_registry() -> Arc<RwLock<SubscriptionRegistry>> {
        Arc::new(RwLock::new(SubscriptionRegistry::new()))
    }

    fn register_sub(
        registry: &Arc<RwLock<SubscriptionRegistry>>,
    ) -> (SubscriptionId, mpsc::Receiver<InvalidationEvent>) {
        let (tx, rx) = mpsc::channel(16);
        let mut read_set = ReadSet::new();
        let qid = read_set.next_query_id();
        read_set.add_interval(
            exdb_core::types::CollectionId(1),
            exdb_core::types::IndexId(0),
            ReadInterval {
                query_id: qid,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        let id = registry.write().register(
            SubscriptionMode::Watch,
            0,
            1,
            1,
            read_set,
            tx,
        );
        (id, rx)
    }

    #[test]
    fn handle_id() {
        let registry = make_registry();
        let (id, rx) = register_sub(&registry);
        let handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);
        assert_eq!(handle.id(), id);
    }

    #[test]
    fn drop_cleans_up() {
        let registry = make_registry();
        let (id, rx) = register_sub(&registry);
        {
            let _handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);
            // Verify subscription exists
            assert!(registry.read().subscription_count() > 0);
        }
        // After drop, subscription should be removed
        assert_eq!(registry.read().subscription_count(), 0);
    }

    #[test]
    fn explicit_unsubscribe() {
        let registry = make_registry();
        let (id, rx) = register_sub(&registry);
        let handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);
        handle.unsubscribe();
        assert_eq!(registry.read().subscription_count(), 0);
    }

    #[tokio::test]
    async fn next_event_returns_none_after_unsubscribe() {
        let registry = make_registry();
        let (id, rx) = register_sub(&registry);
        let mut handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);

        // Drop the sender side by removing from registry (simulates cleanup)
        registry.write().remove(id);

        // Channel closed → None
        let event = handle.next_event().await;
        assert!(event.is_none());
    }

    #[test]
    fn update_read_set() {
        let registry = make_registry();
        let (id, rx) = register_sub(&registry);
        let handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);

        // Should not panic
        let new_rs = ReadSet::new();
        handle.update_read_set(new_rs);
    }

    #[tokio::test]
    async fn next_event_receives_event() {
        let registry = make_registry();
        let (tx, rx) = mpsc::channel(16);
        let mut read_set = ReadSet::new();
        let qid = read_set.next_query_id();
        read_set.add_interval(
            exdb_core::types::CollectionId(1),
            exdb_core::types::IndexId(0),
            ReadInterval {
                query_id: qid,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
        let id = registry.write().register(
            SubscriptionMode::Notify,
            0,
            1,
            1,
            read_set,
            tx.clone(),
        );

        let mut handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);

        // Send an event via the channel
        let event = InvalidationEvent {
            subscription_id: id,
            affected_query_ids: vec![qid],
            commit_ts: 42,
            continuation: None,
        };
        tx.send(event).await.unwrap();

        let received = handle.next_event().await.unwrap();
        assert_eq!(received.commit_ts, 42);
    }

    #[test]
    fn multiple_handles_independent() {
        let registry = make_registry();
        let (id1, rx1) = register_sub(&registry);
        let (id2, rx2) = register_sub(&registry);

        let handle1 = SubscriptionHandle::new(id1, Arc::clone(&registry), rx1);
        let _handle2 = SubscriptionHandle::new(id2, Arc::clone(&registry), rx2);

        handle1.unsubscribe();
        // handle2 still alive
        assert_eq!(registry.read().subscription_count(), 1);
    }
}
