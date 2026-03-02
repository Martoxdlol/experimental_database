/**
 * @packageDocumentation
 * **experimental-db-client** — TypeScript/JavaScript client for experimental-db.
 *
 * A lightweight, MVCC-based JSON document store with WebSocket API, OCC
 * transactions, and live query subscriptions.
 *
 * ---
 *
 * ### Quick start
 * ```ts
 * import { DbClient } from "experimental-db-client";
 *
 * const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
 *
 * await db.createCollection("users");
 *
 * // Write
 * const tx = db.mutation();
 * const id = await tx.insert("users", { name: "Alice", age: 30 });
 * await tx.commit();
 *
 * // Read
 * const q = db.query();
 * const users = await q.find("users", { gt: ["age", 18] });
 * await q.commit();
 *
 * // Live subscription
 * const q2 = db.query({ subscribe: true });
 * await q2.find("users", null);
 * const { subscribed, subscription } = await q2.commit();
 * if (subscribed) {
 *   subscription.onInvalidate(() => console.log("users changed!"));
 * }
 *
 * db.close();
 * ```
 */

// ── Values ────────────────────────────────────────────────────────────────────

export { DbClient }        from "./src/client.ts";
export { MutationSession } from "./src/mutation.ts";
export { QuerySession }    from "./src/query.ts";
export { Subscription }    from "./src/subscription.ts";
export { DbError }         from "./src/errors.ts";

// ── Types ─────────────────────────────────────────────────────────────────────

export type { ConnectOptions, QueryOptions }        from "./src/client.ts";
export type { MutationOutcome, Filter, Scalar, InvalidationEvent } from "./src/types.ts";
export type { QueryOutcome }                        from "./src/query.ts";
export type { InvalidateCallback }                  from "./src/subscription.ts";
