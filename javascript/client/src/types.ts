/**
 * @module types
 * Core public types for the experimental-db client.
 */

// ── Primitives ────────────────────────────────────────────────────────────────

/**
 * A primitive scalar value used in filter predicates.
 * Maps directly to JSON `null`, `boolean`, `number`, or `string`.
 */
export type Scalar = null | boolean | number | string;

// ── Filter ────────────────────────────────────────────────────────────────────

/**
 * A composable filter expression tree for querying documents.
 *
 * Each variant is a single-key object whose key names the operator.
 * Field paths support **dot notation** for nested access (e.g. `"address.city"`).
 *
 * ---
 *
 * **Comparison operators** — field value vs. scalar:
 * | Operator | Meaning |
 * |----------|---------|
 * | `eq`     | equal |
 * | `ne`     | not equal |
 * | `lt`     | less than |
 * | `lte`    | less than or equal |
 * | `gt`     | greater than |
 * | `gte`    | greater than or equal |
 * | `in`     | value is in a set of scalars |
 *
 * **Logical operators:**
 * | Operator | Meaning |
 * |----------|---------|
 * | `and`    | all child filters must match |
 * | `or`     | at least one child filter must match |
 * | `not`    | the child filter must not match |
 *
 * ---
 *
 * @example Equality
 * ```ts
 * { eq: ["status", "active"] }
 * ```
 *
 * @example Range — price between 10 and 100
 * ```ts
 * { and: [{ gte: ["price", 10] }, { lte: ["price", 100] }] }
 * ```
 *
 * @example Membership
 * ```ts
 * { in: ["role", ["admin", "moderator"]] }
 * ```
 *
 * @example Nested logical
 * ```ts
 * {
 *   and: [
 *     { eq: ["active", true] },
 *     { or: [{ gt: ["score", 90] }, { eq: ["tier", "gold"] }] },
 *   ]
 * }
 * ```
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

// ── Events ────────────────────────────────────────────────────────────────────

/**
 * Server-pushed event indicating a subscribed query's read set has been
 * invalidated by a concurrent commit.
 *
 * Received via {@link Subscription.onInvalidate}. After receiving this event
 * you should re-run the original query in a fresh {@link QuerySession} to
 * obtain up-to-date data.
 */
export interface InvalidationEvent {
  /** Always `"invalidation"` — discriminator field. */
  readonly type: "invalidation";
  /** Server-assigned subscription ID (matches the value in {@link QueryOutcome}). */
  readonly sub_id: number;
  /** Logical commit timestamp of the write that triggered this invalidation. */
  readonly ts: number;
}

// ── Outcomes ──────────────────────────────────────────────────────────────────

/**
 * Result of committing a {@link MutationSession}.
 *
 * | Variant | Description |
 * |---------|-------------|
 * | `{ result: "committed", commitTs }` | All writes are durable. `commitTs` is the logical timestamp assigned to this commit. |
 * | `{ result: "conflict" }` | A concurrent transaction wrote to a document you read; **no changes were applied**. Retry the entire mutation. |
 *
 * @example
 * ```ts
 * const outcome = await tx.commit();
 * if (outcome.result === "conflict") {
 *   // retry...
 * }
 * ```
 */
export type MutationOutcome =
  | { result: "committed"; commitTs: number }
  | { result: "conflict" };
