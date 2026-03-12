# Layer 5: Transaction Manager — Implementation Plan

**Crate:** `exdb-tx`
**Depends on:** L1 (`exdb-core`), L2 (`exdb-storage`), L3 (`exdb-docstore`)
**Depended on by:** L6 (`exdb` / Database facade)

## Purpose

Layer 5 is the concurrency and consistency layer. It owns:

- Monotonic timestamp allocation
- Read set tracking (scanned intervals + catalog observations)
- Write set buffering (document mutations + DDL)
- OCC conflict detection via commit log
- Subscription registry with three modes (Notify / Watch / Subscribe)
- Read set carry-forward for subscription chains
- The single-writer commit protocol

L5 does **not** own the user-facing transaction API (`Transaction` struct, `begin()`, collection name resolution). That belongs to L6. L5 provides the machinery that L6 orchestrates.

## Key Design Decisions

### Unified Subscription Mode Enum

Previous design had two separate booleans (`notify`, `subscribe`). This is replaced by a single enum to make the modes explicit and mutually exclusive:

```rust
pub enum SubscriptionMode {
    /// No subscription. Read set discarded after commit.
    None,
    /// One-shot: fire once on first invalidation, then remove subscription.
    /// Notification includes affected query_ids and commit_ts.
    /// No new transaction is started.
    Notify,
    /// Persistent watch: fire on every invalidation, subscription persists.
    /// Notification includes affected query_ids and commit_ts.
    /// No new transaction is started. Read set is NOT updated.
    /// Client manages their own re-queries and can explicitly update
    /// the subscription's read set via update_read_set().
    Watch,
    /// Persistent subscription with chain continuation.
    /// On invalidation: auto-start new transaction at invalidation ts,
    /// carry forward unaffected read set intervals (Option A).
    /// Notification includes affected query_ids, commit_ts, and
    /// ChainContinuation with carried read set + new tx.
    Subscribe,
}
```

**Behavior matrix:**

| Scenario | `None` | `Notify` | `Watch` | `Subscribe` |
|----------|--------|----------|---------|-------------|
| After commit | Read set discarded | Read set → subscription (one-shot) | Read set → subscription (persistent) | Read set → subscription (persistent) |
| On invalidation | — | Notify + remove | Notify (keep subscription) | Notify + new tx + carry read set |
| On OCC conflict | Error returned | Error returned | Error returned | Error + new write tx for retry |
| Read set update | — | — | Manual (`update_read_set`) | Automatic on chain commit |

### Read Set Carry-Forward (Option A)

When a `Subscribe`-mode subscription is invalidated by a commit at `ts=T`, and the affected query IDs are `[Q_min, ...]`:

1. A new transaction is started at `read_ts = T`
2. Read intervals with `query_id < Q_min` are copied verbatim to the new transaction
3. The new transaction's `next_query_id` counter starts at `Q_min`
4. The client re-executes queries from `Q_min` onward
5. On commit, the merged read set (carried + new) replaces the subscription

**Correctness proof:** If intervals for Q0..Q_{min-1} were NOT in `affected_query_ids`, then by definition no commit between `old_ts` and `T` wrote keys overlapping those intervals. The data in those ranges is identical at `old_ts` and `T`. Therefore carrying those intervals forward is equivalent to re-executing the same queries — the results and intervals would be identical.

**Important invariant:** Carried intervals use byte-range bounds on the encoded key space. These bounds are timestamp-independent (they describe a region of the index, not a point in time). So carried intervals from `old_ts` remain valid conflict detectors at `T`.

### Watch Mode — Persistent Notification Without Auto-Transaction

Watch mode fills the gap between one-shot Notify and full Subscribe:

- **Notify:** "Tell me once if anything changes" → fire, remove
- **Watch:** "Keep telling me when things change" → fire, keep watching
- **Subscribe:** "Keep telling me and help me refresh" → fire, new tx, carry read set

Watch mode is useful for:
- UI dashboards that poll on their own schedule but want push invalidation
- Cache layers that evict entries on invalidation but refill lazily
- Clients that batch re-queries across multiple invalidation events

In Watch mode, the subscription's read set stays fixed after registration. The client can explicitly call `update_read_set()` if they re-query and want the subscription to reflect the new data.

## Module Map

| # | File | Struct / Function | Purpose |
|---|------|-------------------|---------|
| T1 | `timestamp.rs` | `TsAllocator` | Monotonic timestamp allocation |
| T2 | `read_set.rs` | `ReadSet`, `ReadInterval`, `CatalogRead` | Scanned interval tracking + carry-forward |
| T3 | `write_set.rs` | `WriteSet`, `MutationEntry`, `CatalogMutation`, `IndexDelta` | Buffered mutations + delta computation |
| T4 | `commit_log.rs` | `CommitLog`, `CommitLogEntry`, `IndexKeyWrite` | Recent commit tracking for OCC |
| T5 | `occ.rs` | `validate()`, `ConflictError` | Read set vs commit log conflict detection |
| T6 | `subscriptions.rs` | `SubscriptionRegistry`, `InvalidationEvent`, `ChainContinuation` | Subscription lifecycle + invalidation |
| T7 | `commit.rs` | `CommitCoordinator`, `CommitHandle`, `CommitRequest`, `CommitResult` | Single-writer commit protocol |

## Dependency Graph (Within L5)

```
T7 (commit.rs)
 ├── T5 (occ.rs)
 │    ├── T2 (read_set.rs)
 │    └── T4 (commit_log.rs)
 ├── T6 (subscriptions.rs)
 │    └── T2 (read_set.rs)
 ├── T3 (write_set.rs)
 ├── T1 (timestamp.rs)
 └── L2 (StorageEngine — WAL)
     L3 (PrimaryIndex, SecondaryIndex — materialize)
```

## Build Order

Strictly bottom-up, each step produces a testable unit:

1. **T1** `timestamp.rs` — no L5 dependencies, pure atomic counter
2. **T2** `read_set.rs` — depends only on L1 types
3. **T3** `write_set.rs` — depends on L1 types, L3 for index delta computation
4. **T4** `commit_log.rs` — depends on L1 types
5. **T5** `occ.rs` — depends on T2 + T4
6. **T6** `subscriptions.rs` — depends on T2
7. **T7** `commit.rs` — depends on all above + L2 + L3

## Types Exported to L6

| Type | Used By L6 For |
|------|---------------|
| `SubscriptionMode` | `TransactionOptions` |
| `ReadSet`, `ReadInterval`, `CatalogRead` | Transaction read tracking |
| `WriteSet`, `MutationEntry`, `CatalogMutation` | Transaction write buffering |
| `CommitHandle` | Submitting commit requests |
| `CommitRequest`, `CommitResult` | Commit protocol |
| `TsAllocator` | Timestamp management |
| `SubscriptionRegistry` | Subscription access for L7/L8 |
| `InvalidationEvent`, `ChainContinuation` | Push notifications |
| `ConflictError`, `ConflictKind` | Error reporting |
| `SubscriptionId`, `QueryId` | Subscription identifiers |

## Crate Dependencies

```toml
[dependencies]
exdb-core = { path = "../core" }
exdb-storage = { path = "../storage" }
exdb-docstore = { path = "../docstore" }
tokio = { version = "1", features = ["sync", "macros", "rt"] }
serde_json = "1"
anyhow = "1"
```

## Test Strategy

Each module has unit tests for its isolated logic. Integration tests in `tests/` combine multiple modules:

- **T5 + T4:** OCC validation against populated commit log
- **T6 + T2:** Subscription invalidation with carry-forward
- **T7 end-to-end:** Full commit protocol with in-memory storage engine

All tests use `StorageEngine::open_in_memory()` via `#[tokio::test]`.
