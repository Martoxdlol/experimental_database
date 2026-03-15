# B7: Subscription Handle

## Purpose

RAII wrapper around a subscription in the `SubscriptionRegistry`. Provides a typed event stream and automatic cleanup on drop. Returned from `Transaction::commit()` when the subscription mode is not `None`.

## Dependencies

- **L5 (`exdb-tx`)**: `SubscriptionId`, `SubscriptionRegistry`, `InvalidationEvent`
- **tokio**: `sync::mpsc`

## Rust Types

```rust
use std::sync::Arc;
use parking_lot::RwLock;
use exdb_tx::{SubscriptionId, SubscriptionRegistry, InvalidationEvent};
use tokio::sync::mpsc;

/// RAII handle for a subscription.
///
/// Provides an async event stream for invalidation notifications and
/// automatic cleanup on drop. Created by `Transaction::commit()` when
/// `TransactionOptions.subscription != None`.
///
/// # Lifecycle
///
/// - **`next_event()`**: await the next invalidation. Returns `None` when
///   the subscription is removed (Notify mode after firing, or explicit
///   unsubscribe, or server shutdown).
/// - **`unsubscribe()`**: explicitly end the subscription. Equivalent to drop.
/// - **`drop`**: automatic unsubscribe. Removes from registry.
///
/// # Thread Safety
///
/// `SubscriptionHandle` is `Send` — it can be moved to another task for
/// long-running event processing.
pub struct SubscriptionHandle {
    id: SubscriptionId,
    registry: Arc<RwLock<SubscriptionRegistry>>,
    /// Receiver for invalidation events.
    ///
    /// For write commits: created by the ReplicationRunner (step 10b),
    /// receiver passed through CommitResult.
    ///
    /// For read-only commits: L6 creates the mpsc channel directly
    /// before calling registry.register(), keeps the receiver here.
    events: Option<mpsc::Receiver<InvalidationEvent>>,
}

impl SubscriptionHandle {
    /// Create a new subscription handle.
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
    ///
    /// Returns `None` when:
    /// - The subscription was removed (Notify mode after firing once)
    /// - `unsubscribe()` was called
    /// - The database is shutting down
    /// - The channel sender was dropped (internal cleanup)
    pub async fn next_event(&mut self) -> Option<InvalidationEvent> {
        self.events.as_mut()?.recv().await
    }

    /// Update the subscription's read set (DESIGN.md 5.9.2 — Watch mode).
    ///
    /// After receiving an invalidation event, a Watch-mode client can
    /// re-execute queries and update the subscription's watched intervals.
    /// This replaces the old read set entirely — the subscription will now
    /// fire on writes overlapping the new intervals.
    ///
    /// No-op for Notify mode (subscription is already removed after firing).
    /// For Subscribe mode, the chain commit handles this automatically.
    pub fn update_read_set(&self, new_read_set: exdb_tx::ReadSet) {
        self.registry.write().update_read_set(self.id, new_read_set);
    }

    /// Explicitly end the subscription and remove it from the registry.
    ///
    /// After this call, `next_event()` returns `None` immediately.
    /// Also happens automatically on drop.
    pub fn unsubscribe(mut self) {
        self.cleanup();
    }

    fn cleanup(&mut self) {
        self.events.take(); // drop receiver → sender gets error on try_send
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
```

## Implementation Notes

- The `events` field is `Option` so `cleanup()` can be called idempotently (both from `unsubscribe()` and `Drop`).
- Dropping the receiver causes the sender's `try_send` to return `Err(SendError)`, which the `SubscriptionRegistry::push_events` silently ignores. This means there's no dangling sender after unsubscribe.
- For Notify mode, the registry removes the subscription after firing once. The sender is dropped, causing `next_event()` to return `None`. The `Drop` impl then calls `remove()` which is a no-op (already removed).

## Tests

| # | Test | Validates |
|---|------|-----------|
| 1 | `next_event_receives_event` | Event pushed by registry is received |
| 2 | `next_event_returns_none_on_close` | Sender dropped → None |
| 3 | `unsubscribe_removes_from_registry` | Subscription removed after unsubscribe |
| 4 | `drop_removes_from_registry` | Subscription removed on drop |
| 5 | `double_cleanup_safe` | unsubscribe + drop doesn't panic |
| 6 | `id_accessible` | id() returns correct SubscriptionId |
| 7 | `send_after_handle_closed` | Unsubscribe, sender error handled gracefully |
| 8 | `update_read_set_changes_watched_intervals` | After update, new writes trigger event |
| 9 | `notify_mode_one_shot` | Fires once, then next_event returns None |
| 10 | `watch_mode_persists_across_events` | Multiple invalidations received |
| 11 | `subscribe_mode_chain_continuation` | Event carries ChainContinuation |
