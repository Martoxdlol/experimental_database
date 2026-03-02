/**
 * @module client
 * Core WebSocket connection and main `DbClient` class.
 */

import { DbError } from "./errors.ts";
import { MutationSession } from "./mutation.ts";
import { QuerySession } from "./query.ts";
import type { Subscription } from "./subscription.ts";
import type { InvalidationEvent } from "./types.ts";

// ── Internal types ────────────────────────────────────────────────────────────

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject:  (err: Error) => void;
}

// ── Options ───────────────────────────────────────────────────────────────────

/**
 * Options for {@link DbClient.connect}.
 */
export interface ConnectOptions {
  /**
   * Milliseconds to wait for the WebSocket handshake before rejecting.
   * Default: no timeout.
   */
  timeoutMs?: number;
}

/**
 * Options for {@link DbClient.query}.
 */
export interface QueryOptions {
  /**
   * When `true`, committing the session registers a live subscription and
   * returns a {@link Subscription} in the {@link QueryOutcome}.
   * Default: `false`.
   */
  subscribe?: boolean;
}

// ── DbClient ──────────────────────────────────────────────────────────────────

/**
 * The main entry point for interacting with an experimental-db server.
 *
 * `DbClient` owns the WebSocket connection, serialises outgoing commands with
 * auto-incremented `req_id`s, correlates responses, and routes server-push
 * invalidation events to the correct {@link Subscription}.
 *
 * Uses the **native `WebSocket` API** — available in all modern browsers and
 * Node.js / Bun runtimes. No external dependencies required.
 *
 * ---
 *
 * ### Connecting
 * ```ts
 * import { DbClient } from "experimental-db-client";
 *
 * const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
 * ```
 *
 * ### Collections
 * ```ts
 * await db.createCollection("users");
 * await db.deleteCollection("archive");
 * ```
 *
 * ### Writing data
 * ```ts
 * const tx = db.mutation();
 * const id = await tx.insert("users", { name: "Alice", age: 30 });
 * await tx.patch("users", id, { verified: true });
 * const { result } = await tx.commit();
 * ```
 *
 * ### Reading data
 * ```ts
 * const q = db.query();
 * const users = await q.find("users", { eq: ["verified", true] });
 * await q.commit();
 * ```
 *
 * ### Live subscriptions
 * ```ts
 * const q = db.query({ subscribe: true });
 * await q.find("events", null);
 * const { subscribed, subscription } = await q.commit();
 * if (subscribed) subscription.onInvalidate(() => refetch());
 * ```
 *
 * ### Closing
 * ```ts
 * db.close();
 * ```
 */
export class DbClient {
  private readonly _ws: WebSocket;
  private _nextReqId = 1;
  private readonly _pending = new Map<number, PendingRequest>();
  private readonly _subs    = new Map<number, Subscription>();

  private constructor(ws: WebSocket) {
    this._ws = ws;
    ws.addEventListener("message", (event) => this._onMessage(event));
    ws.addEventListener("close",   ()      => this._onClose());
  }

  // ── Factory ───────────────────────────────────────────────────────────────

  /**
   * Open a WebSocket connection to the database server and return a ready client.
   *
   * Resolves once the WebSocket handshake completes. If {@link ConnectOptions.timeoutMs}
   * is set, the promise rejects when the timeout expires before the connection opens.
   *
   * @param url  - WebSocket URL, e.g. `"ws://127.0.0.1:3000/ws"`.
   * @param opts - Optional {@link ConnectOptions}.
   *
   * @example
   * ```ts
   * const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
   * ```
   *
   * @example With timeout
   * ```ts
   * const db = await DbClient.connect("ws://127.0.0.1:3000/ws", { timeoutMs: 5_000 });
   * ```
   */
  static connect(url: string, opts: ConnectOptions = {}): Promise<DbClient> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      const client = new DbClient(ws);

      let timer: ReturnType<typeof setTimeout> | undefined;
      if (opts.timeoutMs != null) {
        timer = setTimeout(() => {
          ws.close();
          reject(new Error(`Connection to ${url} timed out after ${opts.timeoutMs}ms`));
        }, opts.timeoutMs);
      }

      ws.addEventListener("open", () => {
        clearTimeout(timer);
        resolve(client);
      });

      ws.addEventListener("error", (e) => {
        clearTimeout(timer);
        reject(new Error(`WebSocket error: ${String(e)}`));
      });
    });
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Close the underlying WebSocket connection.
   *
   * All pending requests are immediately rejected and all subscriptions stop
   * receiving events.
   */
  close(): void {
    this._ws.close();
  }

  // ── Internal message handling ─────────────────────────────────────────────

  private _onMessage(event: MessageEvent): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(event.data as string) as Record<string, unknown>;
    } catch {
      console.warn("[DbClient] received non-JSON message:", event.data);
      return;
    }

    // Server-push invalidation event (no req_id)
    if (msg["type"] === "invalidation") {
      const ev = msg as unknown as InvalidationEvent;
      this._subs.get(ev.sub_id)?._dispatch(ev);
      return;
    }

    // Response to a pending request
    const reqId = msg["req_id"] as number | undefined;
    if (reqId == null) return;

    const pending = this._pending.get(reqId);
    if (pending == null) return;
    this._pending.delete(reqId);

    if (msg["ok"] === true) {
      pending.resolve(msg);
    } else {
      const error = (msg["error"] as string | undefined) ?? "unknown server error";
      pending.reject(new DbError(error, reqId));
    }
  }

  private _onClose(): void {
    const err = new Error("WebSocket connection closed");
    for (const { reject } of this._pending.values()) reject(err);
    this._pending.clear();
  }

  // ── RPC core ──────────────────────────────────────────────────────────────

  /** @internal Serialise a command, assign a req_id, and await the response. */
  _send(payload: Record<string, unknown>): Promise<unknown> {
    const reqId = this._nextReqId++;
    return new Promise<unknown>((resolve, reject) => {
      this._pending.set(reqId, { resolve, reject });
      this._ws.send(JSON.stringify({ ...payload, req_id: reqId }));
    });
  }

  /** @internal Register a subscription so push events are routed to it. */
  _registerSub(sub: Subscription): void {
    this._subs.set(sub.id, sub);
  }

  /** @internal Remove a subscription from the dispatch table. */
  _unregisterSub(id: number): void {
    this._subs.delete(id);
  }

  // ── Collection management ─────────────────────────────────────────────────

  /**
   * Create a new collection.
   *
   * @param name - Collection name. Must be unique within the database.
   * @throws {@link DbError} if a collection with that name already exists.
   *
   * @example
   * ```ts
   * await db.createCollection("products");
   * ```
   */
  async createCollection(name: string): Promise<void> {
    await this._send({ type: "create_collection", name });
  }

  /**
   * Delete a collection and permanently remove all its documents.
   *
   * @param name - Collection name to remove.
   * @throws {@link DbError} if the collection does not exist.
   */
  async deleteCollection(name: string): Promise<void> {
    await this._send({ type: "delete_collection", name });
  }

  // ── Index management ──────────────────────────────────────────────────────

  /**
   * Create a secondary index on a document field.
   *
   * Index building runs in the background. Poll {@link DbClient.indexState}
   * to check when it reaches `"Ready"`. Once ready, range and equality filters
   * on the indexed field automatically use it — no hint required.
   *
   * @param collection - Collection to index.
   * @param field      - Dot-separated field path (e.g. `"user.age"`).
   * @param unique     - Enforce uniqueness across all documents. Default: `false`.
   * @returns The server-assigned numeric index ID.
   *
   * @example
   * ```ts
   * const indexId = await db.createIndex("orders", "createdAt");
   * // Poll until ready
   * while (!(await db.indexState("orders", indexId)).startsWith("Ready")) {
   *   await Bun.sleep(50);
   * }
   * ```
   */
  async createIndex(collection: string, field: string, unique = false): Promise<number> {
    const r = (await this._send({
      type: "create_index",
      collection,
      field,
      unique,
    })) as { index_id: number };
    return r.index_id;
  }

  /**
   * Return the current build state of a secondary index.
   *
   * Possible values: `"Building { … }"`, `"Ready { … }"`, `"Failed { … }"`.
   *
   * @param collection - Collection the index belongs to.
   * @param indexId    - Index ID returned by {@link DbClient.createIndex}.
   */
  async indexState(collection: string, indexId: number): Promise<string> {
    const r = (await this._send({
      type: "index_state",
      collection,
      index_id: indexId,
    })) as { state: string };
    return r.state;
  }

  // ── Session factories ─────────────────────────────────────────────────────

  /**
   * Start a new write transaction.
   *
   * The server-side transaction opens lazily on the first operation.
   *
   * @example
   * ```ts
   * const tx = db.mutation();
   * const id = await tx.insert("tasks", { title: "Buy milk", done: false });
   * const { result } = await tx.commit();
   * ```
   */
  mutation(): MutationSession {
    return new MutationSession((payload) => this._send(payload));
  }

  /**
   * Start a new read-only snapshot session.
   *
   * Pass `{ subscribe: true }` to register a live subscription after commit.
   *
   * @param opts - {@link QueryOptions}.
   *
   * @example
   * ```ts
   * const q = db.query();
   * const logs = await q.find("logs", { gt: ["ts", Date.now() - 3_600_000] });
   * await q.commit();
   * ```
   */
  query(opts: QueryOptions = {}): QuerySession {
    return new QuerySession(
      (payload) => this._send(payload),
      opts.subscribe ?? false,
      (sub) => this._registerSub(sub),
      (id)  => this._unregisterSub(id),
    );
  }
}
