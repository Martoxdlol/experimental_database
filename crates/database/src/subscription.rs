//! B7: Subscription Handle — RAII wrapper around a subscription.

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use exdb_tx::{InvalidationEvent, SubscriptionId, SubscriptionRegistry};

/// RAII handle for a subscription.
///
/// Provides an async event stream for invalidation notifications and
/// automatic cleanup on drop.
pub struct SubscriptionHandle {
    id: SubscriptionId,
    registry: Arc<RwLock<SubscriptionRegistry>>,
    events: Option<mpsc::Receiver<InvalidationEvent>>,
}

impl SubscriptionHandle {
    pub(crate) fn new(
        id: SubscriptionId,
        registry: Arc<RwLock<SubscriptionRegistry>>,
        events: mpsc::Receiver<InvalidationEvent>,
    ) -> Self {
        Self {
            id,
            registry,
            events: Some(events),
        }
    }

    /// The subscription's unique ID.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Wait for the next invalidation event.
    /// Returns `None` when the subscription is removed.
    pub async fn next_event(&mut self) -> Option<InvalidationEvent> {
        self.events.as_mut()?.recv().await
    }

    /// Explicitly end the subscription and remove it from the registry.
    pub fn unsubscribe(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        self.events.take();
        self.registry.write().remove(self.id);
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if self.events.is_some() {
            self.cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_tx::{ReadSet, SubscriptionMode};

    fn make_registry_and_handle() -> (Arc<RwLock<SubscriptionRegistry>>, SubscriptionHandle) {
        let registry = Arc::new(RwLock::new(SubscriptionRegistry::new()));
        let (tx, rx) = mpsc::channel(16);
        let id = registry.write().register(
            SubscriptionMode::Watch,
            0,
            1,
            1,
            ReadSet::new(),
            tx,
        );
        let handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);
        (registry, handle)
    }

    #[tokio::test]
    async fn next_event_receives_event() {
        let registry = Arc::new(RwLock::new(SubscriptionRegistry::new()));
        let (tx, rx) = mpsc::channel(16);
        let id = registry.write().register(
            SubscriptionMode::Watch,
            0,
            1,
            1,
            ReadSet::new(),
            tx.clone(),
        );
        let mut handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);

        tx.send(InvalidationEvent {
            subscription_id: id,
            affected_query_ids: vec![0],
            commit_ts: 10,
            continuation: None,
        })
        .await
        .unwrap();

        let event = handle.next_event().await;
        assert!(event.is_some());
        assert_eq!(event.unwrap().commit_ts, 10);
    }

    #[tokio::test]
    async fn next_event_returns_none_on_close() {
        let registry = Arc::new(RwLock::new(SubscriptionRegistry::new()));
        let (tx, rx) = mpsc::channel(16);
        let id = registry.write().register(
            SubscriptionMode::Watch,
            0,
            1,
            1,
            ReadSet::new(),
            tx,
        );
        let mut handle = SubscriptionHandle::new(id, Arc::clone(&registry), rx);

        // Drop the sender
        // The sender was moved into register, so we need to drop the channel
        // Actually, we already gave `tx` to register. Let's just drop handle's events
        // by dropping the registry's copy
        registry.write().remove(id);

        // The sender inside the registry is now dropped, recv returns None
        let event = handle.next_event().await;
        assert!(event.is_none());
    }

    #[test]
    fn unsubscribe_removes_from_registry() {
        let (registry, handle) = make_registry_and_handle();
        let id = handle.id();
        assert_eq!(registry.read().subscription_count(), 1);
        handle.unsubscribe();
        assert_eq!(registry.read().subscription_count(), 0);
        // Verify removed
        let _ = id;
    }

    #[test]
    fn drop_removes_from_registry() {
        let (registry, handle) = make_registry_and_handle();
        assert_eq!(registry.read().subscription_count(), 1);
        drop(handle);
        assert_eq!(registry.read().subscription_count(), 0);
    }

    #[test]
    fn double_cleanup_safe() {
        let (registry, mut handle) = make_registry_and_handle();
        handle.cleanup();
        // Second cleanup is a no-op (events already taken)
        handle.cleanup();
        assert_eq!(registry.read().subscription_count(), 0);
    }

    #[test]
    fn id_accessible() {
        let (_registry, handle) = make_registry_and_handle();
        let _ = handle.id(); // Should not panic
    }
}
