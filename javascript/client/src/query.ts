/**
 * @module query
 * Read-only snapshot session with optional live subscription.
 */

import type {
  Sender,
  TxResponse,
  DocResponse,
  DocsResponse,
  DocIdsResponse,
  CommitQueryResponse,
} from "./_wire.ts";
import type { Filter } from "./types.ts";
import { Subscription } from "./subscription.ts";

// ── QueryOutcome ──────────────────────────────────────────────────────────────

/**
 * Result of committing a {@link QuerySession}.
 *
 * | Variant | Description |
 * |---------|-------------|
 * | `{ subscribed: false }` | Plain snapshot read; no live updates. |
 * | `{ subscribed: false, invalidatedDuringRun: true }` | Data changed while the query was executing — the snapshot may be stale. Re-run the query. |
 * | `{ subscribed: true, subscription }` | Live subscription activated. Use `subscription.onInvalidate()` to react to future writes. |
 */
export type QueryOutcome =
  | { subscribed: false }
  | { subscribed: false; invalidatedDuringRun: true }
  | { subscribed: true; subscription: Subscription };

// ── QuerySession ──────────────────────────────────────────────────────────────

/**
 * A read-only snapshot transaction with optional live-subscription support.
 *
 * All reads within a session observe a **consistent point-in-time snapshot**.
 * Concurrent writes are invisible until the session ends.
 *
 * Create one with {@link DbClient.query}. The underlying server transaction is
 * opened lazily on the first read — no explicit `begin()` is needed.
 *
 * ---
 *
 * ### Plain reads
 * ```ts
 * const q = db.query();
 * const user    = await q.get("users", userId);
 * const friends = await q.find("users", { eq: ["friendOf", userId] });
 * await q.commit();
 * ```
 *
 * ### Live subscription
 * ```ts
 * const q = db.query({ subscribe: true });
 * await q.find("notifications", { eq: ["userId", myId] });
 * const outcome = await q.commit();
 *
 * if (outcome.subscribed) {
 *   outcome.subscription.onInvalidate(() => refetch());
 * }
 * ```
 *
 * ### Index-accelerated queries
 * The server automatically uses secondary indexes (created via
 * {@link DbClient.createIndex}) when they are available for the filtered field.
 * No client-side hint is required.
 */
export class QuerySession {
  private _txId: string | null = null;

  /** @internal */
  constructor(
    private readonly _send: Sender,
    private readonly _subscribe: boolean,
    private readonly _registerSub: (sub: Subscription) => void,
    private readonly _unregisterSub: (id: number) => void,
  ) {}

  // ── Lazy bootstrap ────────────────────────────────────────────────────────

  private async _ensureTx(): Promise<string> {
    if (this._txId !== null) return this._txId;
    const r = (await this._send({
      type: "begin_query",
      subscribe: this._subscribe,
    })) as TxResponse;
    this._txId = r.tx;
    return this._txId;
  }

  // ── Reads ─────────────────────────────────────────────────────────────────

  /**
   * Fetch a single document by ID from the snapshot.
   *
   * @param collection - Collection to read from.
   * @param docId      - 32-char hex document ID.
   * @returns The document at snapshot time, or `null` if not found / deleted.
   */
  async get(collection: string, docId: string): Promise<Record<string, unknown> | null> {
    const tx = await this._ensureTx();
    const r = (await this._send({
      type: "query_get",
      tx,
      collection,
      doc_id: docId,
    })) as DocResponse;
    return r.doc;
  }

  /**
   * Retrieve all documents matching a filter from the snapshot.
   *
   * The query planner automatically selects secondary indexes for range and
   * equality predicates when an index exists for the filtered field.
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to return all documents.
   * @param limit      - Maximum number of documents to return.
   * @returns Array of matching documents. Order is not guaranteed.
   *
   * @example
   * ```ts
   * const active = await q.find("products", {
   *   and: [{ eq: ["inStock", true] }, { gt: ["price", 0] }],
   * });
   * ```
   */
  async find(
    collection: string,
    filter: Filter | null = null,
    limit?: number,
  ): Promise<Record<string, unknown>[]> {
    const tx = await this._ensureTx();
    const r = (await this._send({
      type: "query_find",
      tx,
      collection,
      filter: filter ?? null,
      limit: limit ?? null,
    })) as DocsResponse;
    return r.docs;
  }

  /**
   * Like {@link QuerySession.find} but returns only document IDs.
   *
   * More efficient when you only need IDs — avoids transmitting full document
   * payloads over the wire.
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to match all documents.
   * @param limit      - Maximum number of IDs to return.
   * @returns Array of matching document IDs.
   *
   * @example
   * ```ts
   * const ids = await q.findIds("events", { gt: ["ts", since] });
   * ```
   */
  async findIds(
    collection: string,
    filter: Filter | null = null,
    limit?: number,
  ): Promise<string[]> {
    const tx = await this._ensureTx();
    const r = (await this._send({
      type: "query_find_ids",
      tx,
      collection,
      filter: filter ?? null,
      limit: limit ?? null,
    })) as DocIdsResponse;
    return r.doc_ids;
  }

  // ── Commit ────────────────────────────────────────────────────────────────

  /**
   * End the query session and optionally activate a live subscription.
   *
   * @returns A {@link QueryOutcome} discriminated union.
   * @throws {@link DbError} on server errors.
   *
   * @example
   * ```ts
   * const outcome = await q.commit();
   * if (!outcome.subscribed) return;
   * outcome.subscription.onInvalidate(() => console.log("stale!"));
   * ```
   */
  async commit(): Promise<QueryOutcome> {
    const tx = await this._ensureTx();
    const r = (await this._send({ type: "commit_query", tx })) as CommitQueryResponse;
    this._txId = null;

    if (r.invalidated_during_run) return { subscribed: false, invalidatedDuringRun: true };
    if (!r.subscribed) return { subscribed: false };

    const sub = new Subscription(r.sub_id!, (id) => this._unregisterSub(id));
    this._registerSub(sub);
    return { subscribed: true, subscription: sub };
  }
}
