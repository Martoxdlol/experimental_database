/**
 * @file client.ts
 * @description Fully-typed TypeScript client for the experimental-database WebSocket API.
 *
 * ## Quick start
 * ```ts
 * const db = await DbClient.connect("ws://127.0.0.1:3000/ws");
 *
 * await db.createCollection("users");
 *
 * // Insert documents
 * const tx = db.mutation();
 * const id = await tx.insert("users", { name: "Alice", age: 30 });
 * await tx.commit();
 *
 * // Query
 * const q = db.query();
 * const docs = await q.find("users", { gt: ["age", 25] });
 * await q.commit();
 *
 * // Subscribe to live updates
 * const q2 = db.query({ subscribe: true });
 * await q2.find("users", null);
 * const sub = await q2.commit();   // returns a Subscription
 * sub.onInvalidate(() => console.log("data changed!"));
 *
 * db.close();
 * ```
 *
 * ## Transport
 * Uses the native `WebSocket` API available in all modern browsers and Node.js 21+.
 *
 * ## Protocol overview
 * Every outgoing message carries a `req_id` (auto-incremented number).
 * The server echoes it back so responses can be matched to pending requests.
 * Subscription invalidation events are pushed without a `req_id`:
 * `{ "type": "invalidation", "sub_id": number, "ts": number }`.
 */

// ---------------------------------------------------------------------------
// Filter type
// ---------------------------------------------------------------------------

/** A scalar value used in filter predicates. */
export type Scalar = null | boolean | number | string;

/**
 * Filter expression for querying documents.
 *
 * Filters are plain JSON objects with a single key identifying the operator.
 * Field paths use dot notation for nested access (e.g. `"address.city"`).
 *
 * @example
 * // Equality
 * { eq: ["status", "active"] }
 *
 * // Range
 * { gt: ["age", 18] }
 * { lte: ["price", 100] }
 *
 * // Membership
 * { in: ["role", ["admin", "moderator"]] }
 *
 * // Logical
 * { and: [{ eq: ["active", true] }, { gt: ["score", 50] }] }
 * { or:  [{ eq: ["type", "a"] },   { eq: ["type", "b"] }] }
 * { not: { eq: ["deleted", true] } }
 */
export type Filter =
  | { eq:  [string, Scalar] }
  | { ne:  [string, Scalar] }
  | { lt:  [string, Scalar] }
  | { lte: [string, Scalar] }
  | { gt:  [string, Scalar] }
  | { gte: [string, Scalar] }
  | { in:  [string, Scalar[]] }
  | { and: Filter[] }
  | { or:  Filter[] }
  | { not: Filter };

// ---------------------------------------------------------------------------
// Server message shapes
// ---------------------------------------------------------------------------

/** Every response from the server carries these base fields. */
interface BaseResponse {
  req_id: number;
  ok: boolean;
  error?: string;
}

interface OkResponse extends BaseResponse { ok: true }

interface DocIdResponse    extends OkResponse { doc_id: string }
interface DocResponse      extends OkResponse { doc: Record<string, unknown> | null }
interface DocsResponse     extends OkResponse { docs: Record<string, unknown>[] }
interface DocIdsResponse   extends OkResponse { doc_ids: string[] }
interface TxResponse       extends OkResponse { tx: string }
interface IndexIdResponse  extends OkResponse { index_id: number }
interface IndexStateResp   extends OkResponse { state: string }

interface CommitMutationResponse extends OkResponse {
  result: "committed" | "conflict";
  commit_ts?: number;
}

interface CommitQueryResponse extends OkResponse {
  subscribed: boolean;
  sub_id?: number;
  invalidated_during_run?: boolean;
}

/** Server-pushed invalidation event (no req_id). */
export interface InvalidationEvent {
  type: "invalidation";
  sub_id: number;
  /** The commit timestamp that caused the invalidation. */
  ts: number;
}

// ---------------------------------------------------------------------------
// Outcome types (public API)
// ---------------------------------------------------------------------------

/** Outcome of committing a mutation transaction. */
export type MutationOutcome =
  | { result: "committed"; commitTs: number }
  | { result: "conflict" };

/** Outcome of committing a subscribed query. */
export type QueryOutcome =
  | { subscribed: false }
  | { subscribed: false; invalidatedDuringRun: true }
  | { subscribed: true; subscription: Subscription };

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

type InvalidateCallback = (event: InvalidationEvent) => void;

/**
 * A live subscription that fires when the server invalidates the query's read set.
 *
 * After receiving an invalidation you should re-run the query to get fresh data.
 * Call {@link Subscription.cancel} to stop receiving events.
 *
 * @example
 * const sub = await q.commit();   // QueryOutcome with subscribed=true
 * if (sub.subscribed) {
 *   sub.subscription.onInvalidate(event => {
 *     console.log(`Query stale at ts=${event.ts}, re-fetching…`);
 *   });
 * }
 */
export class Subscription {
  /** Server-assigned subscription identifier. */
  readonly id: number;

  private _callbacks: InvalidateCallback[] = [];
  private _cancelled = false;

  constructor(id: number, private readonly _client: DbClient) {
    this.id = id;
  }

  /**
   * Register a callback invoked whenever the server pushes an invalidation event.
   * Multiple callbacks can be registered; they are called in registration order.
   */
  onInvalidate(cb: InvalidateCallback): this {
    this._callbacks.push(cb);
    return this;
  }

  /** @internal Called by DbClient when an invalidation arrives. */
  _dispatch(event: InvalidationEvent): void {
    if (this._cancelled) return;
    for (const cb of this._callbacks) cb(event);
  }

  /**
   * Unregister this subscription.
   * After calling this, no further callbacks will fire.
   */
  cancel(): void {
    this._cancelled = true;
    this._client._unregisterSub(this.id);
  }
}

// ---------------------------------------------------------------------------
// Core client
// ---------------------------------------------------------------------------

interface PendingRequest {
  resolve: (value: BaseResponse) => void;
  reject:  (err: Error) => void;
}

/**
 * Low-level WebSocket client for the experimental database.
 *
 * Manages the connection, request/response correlation, and subscription dispatch.
 * Use the higher-level {@link MutationSession} and {@link QuerySession} wrappers
 * returned by {@link DbClient.mutation} and {@link DbClient.query}.
 */
export class DbClient {
  private _ws: WebSocket;
  private _nextReqId = 1;
  private _pending = new Map<number, PendingRequest>();
  private _subs    = new Map<number, Subscription>();
  private _ready: Promise<void>;

  private constructor(ws: WebSocket) {
    this._ws = ws;

    this._ready = new Promise((resolve, reject) => {
      ws.addEventListener("open",  () => resolve());
      ws.addEventListener("error", (e) => reject(new Error(`WebSocket error: ${String(e)}`)));
    });

    ws.addEventListener("message", (event) => this._onMessage(event));
    ws.addEventListener("close", () => this._onClose());
  }

  /**
   * Open a WebSocket connection to the database server.
   *
   * @param url - WebSocket URL, e.g. `"ws://127.0.0.1:3000/ws"`
   * @throws If the connection cannot be established.
   */
  static async connect(url: string): Promise<DbClient> {
    const ws = new WebSocket(url);
    const client = new DbClient(ws);
    await client._ready;
    return client;
  }

  /** Close the WebSocket connection and reject all pending requests. */
  close(): void {
    this._ws.close();
  }

  // ── Internal message handling ──────────────────────────────────────────────

  private _onMessage(event: MessageEvent): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(event.data as string) as Record<string, unknown>;
    } catch {
      console.warn("[DbClient] received non-JSON message:", event.data);
      return;
    }

    // Server-pushed invalidation event
    if (msg["type"] === "invalidation") {
      const ev = msg as unknown as InvalidationEvent;
      this._subs.get(ev.sub_id)?._dispatch(ev);
      return;
    }

    // Response to a pending request
    const reqId = msg["req_id"] as number | undefined;
    if (reqId == null) return;

    const pending = this._pending.get(reqId);
    if (!pending) return;
    this._pending.delete(reqId);

    const resp = msg as unknown as BaseResponse;
    if (resp.ok) {
      pending.resolve(resp);
    } else {
      pending.reject(new DbError(resp.error ?? "unknown server error", reqId));
    }
  }

  private _onClose(): void {
    const err = new Error("WebSocket closed");
    for (const { reject } of this._pending.values()) reject(err);
    this._pending.clear();
  }

  // ── RPC send ──────────────────────────────────────────────────────────────

  /** @internal Send a command and await its response. */
  async _send<T extends BaseResponse>(payload: Record<string, unknown>): Promise<T> {
    const reqId = this._nextReqId++;
    const message = JSON.stringify({ ...payload, req_id: reqId });

    return new Promise<T>((resolve, reject) => {
      this._pending.set(reqId, {
        resolve: (r) => resolve(r as T),
        reject,
      });
      this._ws.send(message);
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
   * Create a new collection. Throws if a collection with that name already exists.
   *
   * @param name - Collection name (must be unique within the database).
   */
  async createCollection(name: string): Promise<void> {
    await this._send({ type: "create_collection", name });
  }

  /**
   * Delete a collection and all documents it contains. Throws if it does not exist.
   *
   * @param name - Collection name to remove.
   */
  async deleteCollection(name: string): Promise<void> {
    await this._send({ type: "delete_collection", name });
  }

  // ── Index management ──────────────────────────────────────────────────────

  /**
   * Create a secondary index on a field within a collection.
   *
   * Index building happens asynchronously in the background. Use
   * {@link DbClient.indexState} to poll readiness. Queries automatically use
   * the index once it reaches `Ready` state.
   *
   * @param collection - Target collection name.
   * @param field      - Dot-separated field path (e.g. `"user.age"`).
   * @param unique     - Whether the index should enforce uniqueness (default: false).
   * @returns The server-assigned index ID.
   */
  async createIndex(collection: string, field: string, unique = false): Promise<number> {
    const r = await this._send<IndexIdResponse>({
      type: "create_index",
      collection,
      field,
      unique,
    });
    return r.index_id;
  }

  /**
   * Return the current state of an index as a human-readable string.
   *
   * Possible values: `"Building { … }"`, `"Ready { … }"`, `"Failed { … }"`.
   *
   * @param collection - Collection the index belongs to.
   * @param indexId    - Index ID returned by {@link DbClient.createIndex}.
   */
  async indexState(collection: string, indexId: number): Promise<string> {
    const r = await this._send<IndexStateResp>({
      type: "index_state",
      collection,
      index_id: indexId,
    });
    return r.state;
  }

  // ── Session factories ─────────────────────────────────────────────────────

  /**
   * Create a new mutation session.
   *
   * The session is **not** started on the server until the first operation is
   * performed (lazy `begin_mutation`). Commit it with {@link MutationSession.commit}.
   *
   * @example
   * const tx = db.mutation();
   * const id = await tx.insert("orders", { item: "Widget", qty: 3 });
   * await tx.patch("orders", id, { qty: 5 });
   * const outcome = await tx.commit();
   * if (outcome.result === "conflict") { /* retry *\/ }
   */
  mutation(): MutationSession {
    return new MutationSession(this);
  }

  /**
   * Create a new query session.
   *
   * @param opts.subscribe - If `true`, committing the session registers a live
   *   subscription; the returned {@link QueryOutcome} will contain a
   *   {@link Subscription} you can listen on.
   * @param opts.limit     - Default result limit applied to all `find` calls.
   *
   * @example
   * const q = db.query();
   * const users = await q.find("users", { gt: ["age", 18] });
   * await q.commit();
   */
  query(opts: { subscribe?: boolean } = {}): QuerySession {
    return new QuerySession(this, opts.subscribe ?? false);
  }
}

// ---------------------------------------------------------------------------
// DbError
// ---------------------------------------------------------------------------

/**
 * Thrown when the server returns `{ "ok": false }`.
 */
export class DbError extends Error {
  /** The `req_id` of the failed request. */
  readonly reqId: number;

  constructor(message: string, reqId: number) {
    super(message);
    this.name = "DbError";
    this.reqId = reqId;
  }
}

// ---------------------------------------------------------------------------
// MutationSession
// ---------------------------------------------------------------------------

/**
 * A write transaction. Operations are buffered on the server until
 * {@link MutationSession.commit} is called.
 *
 * The session is serializable: if another transaction modified any document
 * you read between your `start_ts` and commit, the server returns `"conflict"`.
 * In that case simply retry the whole mutation.
 *
 * **Read-your-own-writes** are supported: a `get` call will return a document
 * inserted or patched earlier in the same session.
 *
 * @example
 * const tx = db.mutation();
 *
 * // Read-your-own-writes
 * const id = await tx.insert("items", { name: "Foo" });
 * const doc = await tx.get("items", id);  // sees the insert above
 *
 * await tx.patch("items", id, { name: "Bar" });
 * await tx.delete("items", id);
 *
 * const outcome = await tx.commit();
 */
export class MutationSession {
  private _tx: string | null = null;

  /** @internal */
  constructor(private readonly _client: DbClient) {}

  // ── Lazy session start ────────────────────────────────────────────────────

  private async _ensureTx(): Promise<string> {
    if (this._tx) return this._tx;
    const r = await this._client._send<TxResponse>({ type: "begin_mutation" });
    this._tx = r.tx;
    return this._tx;
  }

  // ── Reads ─────────────────────────────────────────────────────────────────

  /**
   * Read a single document by ID, respecting writes already made in this session.
   *
   * @param collection - Collection to read from.
   * @param docId      - Document ID (hex string from a previous insert).
   * @returns The document object, or `null` if it does not exist.
   */
  async get(
    collection: string,
    docId: string
  ): Promise<Record<string, unknown> | null> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocResponse>({
      type: "tx_get",
      tx,
      collection,
      doc_id: docId,
    });
    return r.doc;
  }

  /**
   * Find document IDs matching a filter within this transaction.
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to match all documents.
   * @param limit      - Maximum number of IDs to return.
   * @returns Array of matching document IDs.
   */
  async findIds(
    collection: string,
    filter: Filter | null = null,
    limit?: number
  ): Promise<string[]> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocIdsResponse>({
      type: "tx_find_ids",
      tx,
      collection,
      filter: filter ?? null,
      limit: limit ?? null,
    });
    return r.doc_ids;
  }

  // ── Writes ────────────────────────────────────────────────────────────────

  /**
   * Insert a new document. The server generates a unique ID.
   *
   * @param collection - Target collection.
   * @param doc        - Plain object to store as JSON.
   * @returns The generated document ID (32-char hex string).
   */
  async insert(
    collection: string,
    doc: Record<string, unknown>
  ): Promise<string> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocIdResponse>({
      type: "insert",
      tx,
      collection,
      doc,
    });
    return r.doc_id;
  }

  /**
   * Apply a [merge-patch (RFC 7396)](https://www.rfc-editor.org/rfc/rfc7396) to a document.
   *
   * Top-level keys in `patch` are merged into the stored document.
   * Set a key to `null` in the patch to remove it.
   *
   * @param collection - Collection containing the document.
   * @param docId      - ID of the document to update.
   * @param patch      - Partial document to merge in.
   *
   * @example
   * await tx.patch("users", id, { age: 31, nickname: null });
   * // → age is updated, nickname key is removed
   */
  async patch(
    collection: string,
    docId: string,
    patch: Record<string, unknown>
  ): Promise<void> {
    const tx = await this._ensureTx();
    await this._client._send({
      type: "patch",
      tx,
      collection,
      doc_id: docId,
      patch,
    });
  }

  /**
   * Mark a document as deleted. The deletion is visible to other sessions
   * only after this transaction commits successfully.
   *
   * @param collection - Collection containing the document.
   * @param docId      - ID of the document to delete.
   */
  async delete(collection: string, docId: string): Promise<void> {
    const tx = await this._ensureTx();
    await this._client._send({
      type: "delete_doc",
      tx,
      collection,
      doc_id: docId,
    });
  }

  // ── Commit ────────────────────────────────────────────────────────────────

  /**
   * Commit the transaction.
   *
   * The server validates the read set for conflicts. If another transaction
   * wrote to a document you read between your snapshot and now, the commit
   * returns `{ result: "conflict" }` and **no changes are applied** — you must
   * retry from scratch.
   *
   * @returns `{ result: "committed", commitTs: number }` on success,
   *          or `{ result: "conflict" }` if the transaction conflicted.
   *
   * @throws {@link DbError} on network or server errors (distinct from conflicts).
   */
  async commit(): Promise<MutationOutcome> {
    const tx = await this._ensureTx();
    const r = await this._client._send<CommitMutationResponse>({
      type: "commit_mutation",
      tx,
    });
    this._tx = null;
    if (r.result === "committed") {
      return { result: "committed", commitTs: r.commit_ts! };
    }
    return { result: "conflict" };
  }
}

// ---------------------------------------------------------------------------
// QuerySession
// ---------------------------------------------------------------------------

/**
 * A read-only snapshot transaction with optional live-subscription support.
 *
 * All reads within a session observe a consistent snapshot taken at
 * `begin_query` time — concurrent writes are invisible until the session ends.
 *
 * If constructed with `subscribe: true`, committing registers a
 * {@link Subscription} that fires whenever the queried data changes.
 *
 * @example
 * // Plain read
 * const q = db.query();
 * const admins = await q.find("users", { eq: ["role", "admin"] });
 * await q.commit();
 *
 * // Live subscription
 * const q2 = db.query({ subscribe: true });
 * await q2.findIds("orders", { eq: ["status", "pending"] });
 * const outcome = await q2.commit();
 * if (outcome.subscribed) {
 *   outcome.subscription.onInvalidate(() => refetch());
 * }
 */
export class QuerySession {
  private _tx: string | null = null;

  /** @internal */
  constructor(
    private readonly _client: DbClient,
    private readonly _subscribe: boolean
  ) {}

  // ── Lazy session start ────────────────────────────────────────────────────

  private async _ensureTx(): Promise<string> {
    if (this._tx) return this._tx;
    const r = await this._client._send<TxResponse>({
      type: "begin_query",
      subscribe: this._subscribe,
    });
    this._tx = r.tx;
    return this._tx;
  }

  // ── Reads ─────────────────────────────────────────────────────────────────

  /**
   * Fetch a single document by ID from the snapshot.
   *
   * @param collection - Collection to read from.
   * @param docId      - Document ID (32-char hex string).
   * @returns The document, or `null` if deleted/not found at snapshot time.
   */
  async get(
    collection: string,
    docId: string
  ): Promise<Record<string, unknown> | null> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocResponse>({
      type: "query_get",
      tx,
      collection,
      doc_id: docId,
    });
    return r.doc;
  }

  /**
   * Find all documents matching a filter and return their full payloads.
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to return all documents.
   * @returns Array of matching document objects.
   *
   * @example
   * const docs = await q.find("products", {
   *   and: [{ eq: ["in_stock", true] }, { gt: ["price", 10] }]
   * });
   */
  async find(
    collection: string,
    filter: Filter | null = null
  ): Promise<Record<string, unknown>[]> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocsResponse>({
      type: "query_find",
      tx,
      collection,
      filter: filter ?? null,
    });
    return r.docs;
  }

  /**
   * Like {@link QuerySession.find} but returns only document IDs.
   * Cheaper when you only need IDs (avoids transmitting full payloads).
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to match all.
   * @returns Array of matching document IDs.
   */
  async findIds(
    collection: string,
    filter: Filter | null = null
  ): Promise<string[]> {
    const tx = await this._ensureTx();
    const r = await this._client._send<DocIdsResponse>({
      type: "query_find_ids",
      tx,
      collection,
      filter: filter ?? null,
    });
    return r.doc_ids;
  }

  // ── Commit ────────────────────────────────────────────────────────────────

  /**
   * Commit the query session.
   *
   * - If the session was **not** subscribed: returns `{ subscribed: false }`.
   * - If the data changed **while the query was running** (rare): returns
   *   `{ subscribed: false, invalidatedDuringRun: true }` — re-run the query.
   * - If subscribed: returns `{ subscribed: true, subscription }`.
   *   Call {@link Subscription.onInvalidate} to react to future changes.
   *
   * @throws {@link DbError} on server errors.
   */
  async commit(): Promise<QueryOutcome> {
    const tx = await this._ensureTx();
    const r = await this._client._send<CommitQueryResponse>({
      type: "commit_query",
      tx,
    });
    this._tx = null;

    if (r.invalidated_during_run) {
      return { subscribed: false, invalidatedDuringRun: true };
    }
    if (!r.subscribed) {
      return { subscribed: false };
    }
    const sub = new Subscription(r.sub_id!, this._client);
    this._client._registerSub(sub);
    return { subscribed: true, subscription: sub };
  }
}

