/**
 * @module subscription
 * Live subscription handle returned after committing a subscribed query session.
 */

import type { InvalidationEvent } from "./types.ts";

// ── Types ─────────────────────────────────────────────────────────────────────

/** Callback invoked each time the server pushes an invalidation for this subscription. */
export type InvalidateCallback = (event: InvalidationEvent) => void;

// ── Subscription ──────────────────────────────────────────────────────────────

/**
 * A live subscription that receives push notifications whenever a committed
 * write intersects the query's read set.
 *
 * Obtain a subscription by committing a {@link QuerySession} created with
 * `{ subscribe: true }`:
 *
 * ```ts
 * const q = db.query({ subscribe: true });
 * await q.find("orders", { eq: ["status", "pending"] });
 * const outcome = await q.commit();
 *
 * if (outcome.subscribed) {
 *   outcome.subscription.onInvalidate(async (event) => {
 *     console.log(`Orders changed at ts=${event.ts} — re-fetching…`);
 *     await refetch();
 *   });
 * }
 * ```
 *
 * ### Lifecycle
 * 1. Created when a subscribed {@link QuerySession} commits successfully.
 * 2. Each subsequent commit that touches data read during the query pushes an
 *    {@link InvalidationEvent} over the WebSocket connection.
 * 3. Call {@link Subscription.cancel} to stop receiving events.
 */
export class Subscription {
  /** Server-assigned subscription identifier. */
  readonly id: number;

  private readonly _callbacks: InvalidateCallback[] = [];
  private _cancelled = false;

  /** @internal */
  constructor(id: number, private readonly _unregister: (id: number) => void) {
    this.id = id;
  }

  // ── Callback registration ─────────────────────────────────────────────────

  /**
   * Register a callback to invoke on each invalidation event.
   *
   * - Multiple callbacks can be registered; they are called in registration order.
   * - Calling this after {@link Subscription.cancel} is a no-op.
   * - Returns `this` for fluent chaining.
   *
   * @example
   * ```ts
   * sub
   *   .onInvalidate(() => setLoading(true))
   *   .onInvalidate(event => refetch(event.ts));
   * ```
   */
  onInvalidate(cb: InvalidateCallback): this {
    if (!this._cancelled) this._callbacks.push(cb);
    return this;
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Cancel this subscription.
   *
   * After cancellation:
   * - No further callbacks will fire.
   * - The subscription is removed from the client's dispatch table.
   *
   * Safe to call multiple times — subsequent calls are no-ops.
   */
  cancel(): void {
    if (this._cancelled) return;
    this._cancelled = true;
    this._unregister(this.id);
  }

  /**
   * Whether this subscription has been cancelled.
   * `true` immediately after a call to {@link Subscription.cancel}.
   */
  get cancelled(): boolean {
    return this._cancelled;
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  /** @internal Called by DbClient when an invalidation arrives for this sub. */
  _dispatch(event: InvalidationEvent): void {
    if (this._cancelled) return;
    for (const cb of this._callbacks) cb(event);
  }
}
