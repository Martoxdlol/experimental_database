/**
 * @module mutation
 * Write transaction — accumulates inserts, patches, and deletes until committed.
 */

import type {
  Sender,
  TxResponse,
  DocIdResponse,
  DocResponse,
  DocIdsResponse,
  CommitMutationResponse,
} from "./_wire.ts";
import type { Filter, MutationOutcome } from "./types.ts";

/**
 * A write transaction. Inserts, patches, and deletes are buffered server-side
 * until {@link MutationSession.commit} is called.
 *
 * Create one with {@link DbClient.mutation}. The underlying server transaction
 * is opened **lazily** on the first operation — there is no explicit `begin()`.
 *
 * ---
 *
 * ### Isolation & conflicts
 *
 * The server uses **optimistic concurrency control (OCC)**. At commit time it
 * checks whether any document you _read_ was concurrently modified. If so, the
 * commit returns `{ result: "conflict" }` and **no changes are applied**.
 * Simply retry the entire mutation:
 *
 * ```ts
 * while (true) {
 *   const tx = db.mutation();
 *   const doc = await tx.get("counters", id);
 *   await tx.patch("counters", id, { n: ((doc?.n as number) ?? 0) + 1 });
 *   if ((await tx.commit()).result === "committed") break;
 * }
 * ```
 *
 * ### Read-your-own-writes
 *
 * {@link MutationSession.get} and {@link MutationSession.findIds} reflect
 * writes already made within the same session — you can insert a document and
 * immediately read it back in the same transaction.
 *
 * @example Basic insert and commit
 * ```ts
 * const tx = db.mutation();
 * const id = await tx.insert("users", { name: "Alice", age: 30 });
 * const outcome = await tx.commit();
 * if (outcome.result === "committed") {
 *   console.log("saved at ts", outcome.commitTs);
 * }
 * ```
 */
export class MutationSession {
  private _txId: string | null = null;

  /** @internal */
  constructor(private readonly _send: Sender) {}

  // ── Lazy bootstrap ────────────────────────────────────────────────────────

  private async _ensureTx(): Promise<string> {
    if (this._txId !== null) return this._txId;
    const r = (await this._send({ type: "begin_mutation" })) as TxResponse;
    this._txId = r.tx;
    return this._txId;
  }

  // ── Reads (read-your-own-writes) ──────────────────────────────────────────

  /**
   * Fetch a document by ID, reflecting any writes made earlier in this session.
   *
   * @param collection - Collection to read from.
   * @param docId      - 32-char hex document ID.
   * @returns The document payload, or `null` if not found (or deleted in this session).
   *
   * @example
   * ```ts
   * const id = await tx.insert("items", { qty: 5 });
   * const doc = await tx.get("items", id);  // → { qty: 5 }
   * ```
   */
  async get(collection: string, docId: string): Promise<Record<string, unknown> | null> {
    const tx = await this._ensureTx();
    const r = (await this._send({ type: "tx_get", tx, collection, doc_id: docId })) as DocResponse;
    return r.doc;
  }

  /**
   * Find document IDs matching a filter, respecting writes made in this session.
   *
   * @param collection - Collection to scan.
   * @param filter     - Filter expression, or `null` to match all documents.
   * @param limit      - Maximum number of IDs to return.
   * @returns Array of matching document IDs.
   */
  async findIds(
    collection: string,
    filter: Filter | null = null,
    limit?: number,
  ): Promise<string[]> {
    const tx = await this._ensureTx();
    const r = (await this._send({
      type: "tx_find_ids",
      tx,
      collection,
      filter: filter ?? null,
      limit: limit ?? null,
    })) as DocIdsResponse;
    return r.doc_ids;
  }

  // ── Writes ────────────────────────────────────────────────────────────────

  /**
   * Insert a new document. The server generates a unique 128-bit ID.
   *
   * @param collection - Target collection (must already exist).
   * @param doc        - Document payload — any JSON-serialisable object.
   * @returns The generated document ID as a 32-character lowercase hex string.
   *
   * @example
   * ```ts
   * const userId = await tx.insert("users", {
   *   name: "Alice",
   *   email: "alice@example.com",
   *   createdAt: Date.now(),
   * });
   * ```
   */
  async insert(collection: string, doc: Record<string, unknown>): Promise<string> {
    const tx = await this._ensureTx();
    const r = (await this._send({ type: "insert", tx, collection, doc })) as DocIdResponse;
    return r.doc_id;
  }

  /**
   * Apply an [RFC 7396 merge-patch](https://www.rfc-editor.org/rfc/rfc7396) to a document.
   *
   * Top-level keys in `patch` are merged into the stored document.
   * Set a key to **`null`** to remove it.
   *
   * @param collection - Collection containing the document.
   * @param docId      - ID of the document to update.
   * @param patch      - Partial object to merge in.
   *
   * @example
   * ```ts
   * // Increment age, remove the one-time token
   * await tx.patch("users", id, { age: 31, verifyToken: null });
   * ```
   */
  async patch(
    collection: string,
    docId: string,
    patch: Record<string, unknown>,
  ): Promise<void> {
    const tx = await this._ensureTx();
    await this._send({ type: "patch", tx, collection, doc_id: docId, patch });
  }

  /**
   * Mark a document as deleted.
   *
   * The deletion is only visible to other sessions after this transaction
   * commits successfully.
   *
   * @param collection - Collection containing the document.
   * @param docId      - ID of the document to delete.
   */
  async delete(collection: string, docId: string): Promise<void> {
    const tx = await this._ensureTx();
    await this._send({ type: "delete_doc", tx, collection, doc_id: docId });
  }

  // ── Commit ────────────────────────────────────────────────────────────────

  /**
   * Commit the transaction.
   *
   * The server validates the read set against concurrent commits. On conflict
   * **no changes are applied** — the caller should retry from scratch.
   *
   * @returns `{ result: "committed", commitTs }` on success,
   *          or `{ result: "conflict" }` on an OCC conflict.
   * @throws {@link DbError} on network errors or server-side faults.
   *
   * @example Retry loop
   * ```ts
   * while (true) {
   *   const tx = db.mutation();
   *   // … perform reads and writes …
   *   const { result } = await tx.commit();
   *   if (result === "committed") break;
   * }
   * ```
   */
  async commit(): Promise<MutationOutcome> {
    const tx = await this._ensureTx();
    const r = (await this._send({ type: "commit_mutation", tx })) as CommitMutationResponse;
    this._txId = null;
    if (r.result === "committed") {
      return { result: "committed", commitTs: r.commit_ts! };
    }
    return { result: "conflict" };
  }
}
