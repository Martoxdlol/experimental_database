/**
 * Self-contained WebSocket client for experimental-db studio.
 * No external dependencies — uses the native WebSocket API.
 */

// ── Types ─────────────────────────────────────────────────────────────────────

export type Scalar = string | number | boolean | null;

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

export interface IndexInfo {
  index_id: number;
  field: string;
  unique: boolean;
  state: string;
}

export interface DocRow {
  _id: string;
  doc: Record<string, unknown>;
}

export type MutationResult = "committed" | "conflict";

// ── Internal types ────────────────────────────────────────────────────────────

interface Pending {
  resolve: (v: unknown) => void;
  reject:  (e: Error)   => void;
}

// ── StudioClient ──────────────────────────────────────────────────────────────

export class StudioClient {
  private readonly _ws: WebSocket;
  private _nextId = 1;
  private readonly _pending = new Map<number, Pending>();
  private readonly _subs    = new Map<number, (ts: number) => void>();
  private _closed = false;

  private constructor(ws: WebSocket) {
    this._ws = ws;
    ws.addEventListener("message", (e) => this._onMessage(e));
    ws.addEventListener("close",   ()  => this._onClose());
  }

  // ── Factory ───────────────────────────────────────────────────────────────

  static connect(url: string, timeoutMs?: number): Promise<StudioClient> {
    return new Promise((resolve, reject) => {
      let ws: WebSocket;
      try {
        ws = new WebSocket(url);
      } catch (e) {
        reject(new Error(`Failed to create WebSocket: ${String(e)}`));
        return;
      }
      const client = new StudioClient(ws);

      let timer: ReturnType<typeof setTimeout> | undefined;
      if (timeoutMs != null) {
        timer = setTimeout(() => {
          ws.close();
          reject(new Error(`Connection timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }

      ws.addEventListener("open", () => {
        clearTimeout(timer);
        resolve(client);
      });
      ws.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new Error("WebSocket connection failed"));
      });
    });
  }

  get connected(): boolean {
    return !this._closed && this._ws.readyState === WebSocket.OPEN;
  }

  close(): void {
    this._closed = true;
    this._ws.close();
  }

  // ── Message handling ──────────────────────────────────────────────────────

  private _onMessage(event: MessageEvent): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(event.data as string) as Record<string, unknown>;
    } catch {
      return;
    }

    if (msg["type"] === "invalidation") {
      const subId = msg["sub_id"] as number;
      const ts    = msg["ts"]     as number;
      this._subs.get(subId)?.(ts);
      return;
    }

    const id = msg["req_id"] as number | undefined;
    if (id == null) return;

    const p = this._pending.get(id);
    if (!p) return;
    this._pending.delete(id);

    if (msg["ok"] === true) {
      p.resolve(msg);
    } else {
      p.reject(new Error((msg["error"] as string | undefined) ?? "server error"));
    }
  }

  private _onClose(): void {
    this._closed = true;
    const err = new Error("WebSocket closed");
    for (const p of this._pending.values()) p.reject(err);
    this._pending.clear();
  }

  // ── RPC core ──────────────────────────────────────────────────────────────

  private _send(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const id = this._nextId++;
    return new Promise((resolve, reject) => {
      this._pending.set(id, {
        resolve: (v) => resolve(v as Record<string, unknown>),
        reject,
      });
      this._ws.send(JSON.stringify({ ...payload, req_id: id }));
    });
  }

  // ── Collections ───────────────────────────────────────────────────────────

  async listCollections(): Promise<string[]> {
    const r = await this._send({ type: "list_collections" });
    return r["collections"] as string[];
  }

  async createCollection(name: string): Promise<void> {
    await this._send({ type: "create_collection", name });
  }

  async deleteCollection(name: string): Promise<void> {
    await this._send({ type: "delete_collection", name });
  }

  // ── Indexes ───────────────────────────────────────────────────────────────

  async listIndexes(collection: string): Promise<IndexInfo[]> {
    const r = await this._send({ type: "list_indexes", collection });
    return r["indexes"] as IndexInfo[];
  }

  async createIndex(collection: string, field: string, unique = false): Promise<number> {
    const r = await this._send({ type: "create_index", collection, field, unique });
    return r["index_id"] as number;
  }

  async indexState(collection: string, indexId: number): Promise<string> {
    const r = await this._send({ type: "index_state", collection, index_id: indexId });
    return r["state"] as string;
  }

  // ── Document queries ──────────────────────────────────────────────────────

  async findDocs(
    collection: string,
    filter: Filter | null = null,
    limit = 500,
  ): Promise<DocRow[]> {
    const txR = await this._send({ type: "begin_query", subscribe: false });
    const tx  = txR["tx"] as string;
    const r   = await this._send({
      type: "query_find_with_ids",
      tx,
      collection,
      filter: filter ?? null,
      limit,
    });
    await this._send({ type: "commit_query", tx });
    return r["results"] as DocRow[];
  }

  async getDoc(collection: string, docId: string): Promise<Record<string, unknown> | null> {
    const txR = await this._send({ type: "begin_query", subscribe: false });
    const tx  = txR["tx"] as string;
    const r   = await this._send({ type: "query_get", tx, collection, doc_id: docId });
    await this._send({ type: "commit_query", tx });
    const doc = r["doc"];
    return doc == null ? null : doc as Record<string, unknown>;
  }

  // ── Document mutations ────────────────────────────────────────────────────

  async insertDoc(
    collection: string,
    doc: Record<string, unknown>,
  ): Promise<{ id: string; result: MutationResult }> {
    const txR   = await this._send({ type: "begin_mutation" });
    const tx    = txR["tx"] as string;
    const insR  = await this._send({ type: "insert", tx, collection, doc });
    const docId = insR["doc_id"] as string;
    const comR  = await this._send({ type: "commit_mutation", tx });
    return { id: docId, result: comR["result"] as MutationResult };
  }

  async patchDoc(
    collection: string,
    docId: string,
    patch: Record<string, unknown>,
  ): Promise<MutationResult> {
    const txR  = await this._send({ type: "begin_mutation" });
    const tx   = txR["tx"] as string;
    await this._send({ type: "patch", tx, collection, doc_id: docId, patch });
    const comR = await this._send({ type: "commit_mutation", tx });
    return comR["result"] as MutationResult;
  }

  async deleteDoc(collection: string, docId: string): Promise<MutationResult> {
    const txR  = await this._send({ type: "begin_mutation" });
    const tx   = txR["tx"] as string;
    await this._send({ type: "delete_doc", tx, collection, doc_id: docId });
    const comR = await this._send({ type: "commit_mutation", tx });
    return comR["result"] as MutationResult;
  }

  // ── Live subscriptions ────────────────────────────────────────────────────

  /**
   * Subscribe to changes in a collection.
   * Returns a cancel function. Call it to stop receiving events.
   */
  async subscribe(
    collection: string,
    filter: Filter | null,
    onInvalidate: (ts: number) => void,
  ): Promise<{ subId: number; cancel: () => void }> {
    const txR = await this._send({ type: "begin_query", subscribe: true });
    const tx  = txR["tx"] as string;
    await this._send({
      type: "query_find_ids",
      tx,
      collection,
      filter: filter ?? null,
    });
    const comR = await this._send({ type: "commit_query", tx });
    const subId = comR["sub_id"] as number;
    this._subs.set(subId, onInvalidate);
    return {
      subId,
      cancel: () => this._subs.delete(subId),
    };
  }
}
