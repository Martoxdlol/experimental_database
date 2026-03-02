/**
 * @module errors
 * Error types thrown by the experimental-db client.
 */

/**
 * Thrown when the server responds with `{ ok: false, error: "…" }`.
 *
 * Distinct from conflicts ({@link MutationOutcome}) which are normal
 * application-level outcomes — `DbError` signals a server-side fault or a
 * programming mistake (e.g. writing to a collection that does not exist).
 *
 * @example
 * ```ts
 * try {
 *   await db.createCollection("users");
 * } catch (err) {
 *   if (err instanceof DbError) {
 *     console.error(`Server rejected req #${err.reqId}: ${err.message}`);
 *   }
 * }
 * ```
 */
export class DbError extends Error {
  /**
   * The `req_id` echoed by the server in its error response.
   * Useful for correlating a client-side error with server logs.
   */
  readonly reqId: number;

  constructor(message: string, reqId: number) {
    super(message);
    this.name = "DbError";
    this.reqId = reqId;
  }
}
