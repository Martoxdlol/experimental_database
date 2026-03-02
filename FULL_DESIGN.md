# Design document: Basic JSON document-store DB with MVCC, B-Tree indexes, serializable transactions, and subscriptions

This document describes a practical architecture for a small-but-real JSON document database that supports:

- Collections of JSON documents
- Auto-generated document IDs
- `insert/get/patch/delete` by ID
- Secondary B-Tree indexes on document fields (+ primary index on ID)
- Queries via index scans or table scans with filters (`eq, ne, lt, lte, gt, gte`, `AND/OR`)
- MVCC snapshots with per-query/per-mutation timestamps
- Serializable “mutations” (multi-read + multi-write transactions)
- Subscriptions that track a read set and automatically invalidate and rerun on commits that conflict

It’s written as if you already have a working **page store** (paged file + buffer pool + WAL + recovery), but it also calls out what must change/extend in that lower layer to support these features correctly.

---

## 1. Terminology and goals

### Core objects
- **Collection**: named container of documents.
- **Document**: JSON value (object is typical, but you can allow any JSON).
- **DocumentId**: unique ID auto-generated on insert (recommended: 128-bit).
- **Version/Timestamp**: a monotonically increasing commit timestamp `ts` assigned at commit. Each transaction/query has a `start_ts` snapshot.
- **MVCC**: Multi-Version Concurrency Control: multiple committed versions of a document can exist.

### Transaction types
- **Query transaction**: read-only transaction. Can run multiple queries and contributes to a read set (for serializability + subscription invalidation).
- **Mutation**: read-write transaction. Has read set + write set; must commit serializably; must read its own writes.

### Guarantees you want
1. **Snapshot reads**: A transaction starting at `start_ts` reads the state as of that timestamp (plus its own writes).
2. **Serializable mutations**: Committed mutations appear as if executed one-at-a-time in some order.
3. **Subscriptions**: A subscription reruns when any committed mutation could change its result.

---

## 2. High-level architecture (layered)

Think in layers; each layer has a small responsibility.

### 2.1 Storage engine (low-level)
- **Page store**: read/write fixed-size pages; buffer pool cache; WAL.
- **Page allocator**: allocate/free pages, free lists.
- **Record store / blob store**: store variable-sized data (JSON bytes, index entries, version records).

### 2.2 Data model + access methods
- **Document version store (MVCC)**: append versions, find visible version.
- **Primary index**: `doc_id -> current version chain head`.
- **Secondary indexes**: `field -> doc_id` (MVCC-aware).

### 2.3 Transaction layer
- `TxnManager` assigns timestamps and tracks active transactions.
- Serializable validation at commit using read/write sets (details below).

### 2.4 Query execution layer
- Query planner chooses index scan vs table scan
- Filter evaluation
- Produces both results and a **read set** describing what was observed (for serializability + subscription invalidation).

### 2.5 Subscription engine
- Stores (query + parameters + read set)
- On each commit, checks if commit’s write set intersects subscription’s read set; if yes, rerun query.

---

## 3. On-disk data structures

A “real DB” usually separates “logical” concepts from their physical layout. Here’s a minimal set that works.

### 3.1 JSON storage
Store JSON as bytes (e.g., UTF-8 JSON text, or a compact binary format like CBOR/MessagePack). You can start with UTF-8 JSON for simplicity.

- **Blob store**: append-only value log of bytes
  - `BlobId` = (page_id, slot_id) or an absolute offset + length
  - Supports `put(bytes) -> BlobId`, `get(BlobId) -> bytes`

### 3.2 Document version records (MVCC)
Each document has a chain of versions.

A **DocVersion** record contains:
- `doc_id: DocumentId`
- `begin_ts: u64` (commit ts when this version became visible)
- `end_ts: u64` (commit ts when this version stopped being visible; `INF` if current)
- `prev: Option<DocVersionId>` (older version)
- `blob: BlobId` (JSON bytes)
- optional: `deleted: bool` (or represent deletion as a tombstone version)

Storage strategy:
- Append DocVersion records to a **version store** (heap / append-only record log).
- `DocVersionId` is a pointer to the stored record.

### 3.3 Primary mapping: doc_id → head version
You need a fast way to find a doc’s version chain.
- Primary B-Tree index (or hash index) mapping:
  - `doc_id -> head_version_id`

When you insert/patch/delete:
- You append a new DocVersion record and update the primary index head pointer.

### 3.4 Secondary indexes (MVCC-aware)
This is the most important design detail for correctness with snapshot reads + predicates.

If you index a field `f`, the index must handle:
- Updates where `f` changes
- Snapshot queries at `start_ts` that must see the index state as-of `start_ts`
- Preventing phantoms for serializable transactions

A practical minimal approach: **versioned index entries**.

Each secondary index entry stores:
- `index_key` (encoded value of field, plus tie-breakers)
- `doc_id`
- `begin_ts`
- `end_ts`

Meaning: “for timestamps in `[begin_ts, end_ts)`, this document belonged to this index key”.

On update:
- Insert a new index entry for the new field value with `begin_ts = commit_ts, end_ts = INF`
- Find the previous index entry for that doc+field and set `end_ts = commit_ts`

To make “find previous index entry” feasible, you typically store in the DocVersion record the old indexed values or store an “index maintenance record”. Minimal approach:
- At commit time, read the previous visible version, extract old field value, and update the corresponding old index entry.

Index key encoding recommendation:
- `index_key = (field_value_encoded, doc_id)` so duplicates are unique and range scans are stable.

---

## 4. Timestamps, snapshots, and visibility rules

### 4.1 Timestamp source
Maintain a durable monotonic counter:
- `next_commit_ts: u64`

Transactions:
- `start_ts` assigned at `begin_txn()` as the current latest committed timestamp.

Commits:
- assign a new `commit_ts = next_commit_ts++`

### 4.2 Visibility of document versions
Given a transaction snapshot `start_ts`:

A version is visible if:
- `begin_ts <= start_ts < end_ts`
- plus: the transaction should see its own writes, even though they have no committed `begin_ts` yet.

### 4.3 “Read your writes”
Within a mutation:
- Reads first consult the transaction’s local write set.
- If key is in write set, return that value/version (including deletions).
- Otherwise read from storage at `start_ts`.

---

## 5. Queries and filters

### 5.1 Filter model
Support an expression tree:
- Comparisons: `= != < <= > >=`
- Boolean ops: `AND`, `OR`
- (Optional later: `NOT`, `IN`, `prefix`, etc.)

A query is:
- `collection`
- optional `index_hint` / chosen index
- filter expression
- projection (optional, can be full doc initially)
- sort/limit (optional; can postpone)

### 5.2 Two execution strategies
#### A) Index scan + filters
If you have an index on field `f` and filter includes `f` constraints:
- Compute index range(s) from filter
- Scan the secondary index for entries whose `[begin_ts, end_ts)` contains `start_ts`
- For each candidate doc_id, fetch document version visible at `start_ts` and apply remaining filters

#### B) Table scan + filters
Scan all doc_ids in the collection:
- read visible version at `start_ts`
- apply filters

Table scan is slower but always works.

### 5.3 Read set production (critical for serializable + subscriptions)
Every query must produce a **read set** describing what it depended on.

At minimum, track:
- Point reads by doc_id: `ReadDoc(doc_id, observed_version_begin_ts)`
- Index range reads: `ReadIndexRange(index_name, lower_bound, upper_bound, start_ts)`
- Collection scan reads: `ReadCollectionScan(collection_id, start_ts)` (coarse but correct)

If you don’t track predicate/range reads, you cannot reliably prevent phantoms, and subscriptions will miss invalidations.

---

## 6. Mutations, write sets, and commit protocol

### 6.1 Write set contents
The write set must be explicit enough to:
- Apply changes at commit
- Validate conflicts
- Invalidate subscriptions efficiently

For each written document:
- `WriteDoc { doc_id, op: Insert|Update|Delete, new_blob, indexed_field_values }`

Also record:
- which secondary indexes are affected and what keys are inserted/removed.

### 6.2 Commit algorithm (optimistic, serializable via validation)
A practical minimal design is **Optimistic Concurrency Control (OCC)** with predicate validation:

**During the mutation**:
- Reads are at `start_ts` and recorded in read set.
- Writes are buffered (not visible globally).

**At commit**:
1. Acquire a commit timestamp `commit_ts` (or reserve one after validation; either is fine if you’re careful).
2. **Validate** that no conflicting committed transaction committed in `(start_ts, commit_ts]`.

Validation rules (document-store oriented):
- **Write-write conflicts**:
  - If any doc_id in your write set has a committed version with `begin_ts > start_ts`, conflict.
- **Read-write conflicts (serializable / phantom prevention)**:
  - For each `ReadDoc(doc_id, ...)`: if doc has a new committed version with `begin_ts > start_ts`, conflict.
  - For each `ReadIndexRange(...)`: if any committed write inserted/removed an index entry whose key falls in the range and whose `begin_ts` is in `(start_ts, commit_ts]`, conflict.
  - For `ReadCollectionScan`: any committed insert/delete/update in the collection in that window conflicts (coarse).

3. If validation passes:
   - Append new DocVersion records with `begin_ts = commit_ts`
   - Set old version’s `end_ts = commit_ts` (or logically mark ended)
   - Update primary index head pointer
   - Update secondary indexes (insert new entries, end old entries)
   - Write `COMMIT(txn_id, commit_ts, write_set_summary)` to WAL and fsync (durability point)

If validation fails:
- Abort: discard buffered writes (no global changes should have been made).

This gives serializability **if and only if** your read set is precise enough to capture predicates (especially for ORs and scans). Coarse reads (like collection-scan markers) are safe but reduce concurrency.

---

## 7. Indexing strategy details

### 7.1 Defining indexes
An index definition includes:
- `collection_id`
- `index_name`
- `field_path` (e.g., `"user.age"` path resolution rules)
- `type` (number/string/bool/nullable ordering)
- collation rules (for strings)
- uniqueness (optional; can add later)

### 7.2 Field extraction
For each document version at commit:
- Extract indexed field values deterministically.
- If field missing: either do not index, or index a special `NULL` token (choose one; both are valid behaviors).

### 7.3 Maintaining versioned index entries
On commit for a document update:
- Read old visible version at `start_ts` (or the current head at commit time if you serialize commits) to find old indexed values.
- For each index:
  - if old_value != new_value:
    - end old index entry: set `end_ts = commit_ts`
    - insert new index entry with `begin_ts = commit_ts, end_ts = INF`

To “end” an old entry, you need to find it. Options:
- (Recommended) Store, in the DocVersion record, the extracted indexed values so you can reconstruct keys.
- Or add a backpointer: index entry includes a pointer to the version record; still need to locate entry to update end_ts.

---

## 8. Subscriptions: invalidation and rerun

A subscription stores:
- Query definition + parameters
- The `start_ts` at which it last ran
- The query’s **read set** (ranges, point reads, scan markers)
- Last result (optional cache)

On each commit, you have:
- `commit_ts`
- A **write set summary** (doc_ids changed, index keys added/removed, collections touched)

Invalidation check:
- If commit write set intersects subscription read set, invalidate
- Then rerun query at new snapshot (e.g., latest committed ts) and update subscription read set

Intersection logic examples:
- Subscription read: `ReadDoc(doc_id=X)` intersects commit write doc_id `X`
- Subscription read: `ReadIndexRange(users.age, [20..30])` intersects commit index insert/remove where key is 25
- Subscription read: `ReadCollectionScan(users)` intersects any write in `users`

This is essentially the same machinery as serializable validation—subscriptions are “long-lived read-only transactions” that rerun when their read set is invalidated.

---

## 9. Concurrency model (pragmatic baseline)

You can build this incrementally. A workable first version:

- **Single writer** (one committing mutation at a time)
- Multiple concurrent readers (queries/subscriptions) using snapshots

Why: it simplifies index maintenance and commit ordering.

Even with single-writer commits, you still need validation against commits between `start_ts` and your `commit_ts` (which can be handled by comparing against a commit log).

Later:
- Allow concurrent writers by adding:
  - latches for B-Tree pages
  - per-doc locks for commit
  - careful index entry updates

---

## 10. WAL, durability, and recovery (what must be true)

Your existing “page patch WAL” can work, but once you introduce transactions and MVCC, you need these additional invariants:

### 10.1 Transaction status must be recoverable
After crash, you must know which commits happened.

Minimum:
- WAL contains `COMMIT` records that are fsynced before acknowledging commit.

Recovery must:
- Rebuild committed transaction list and apply redo accordingly.

### 10.2 Redo must be idempotent
All data/index changes must be replayable safely.
Techniques:
- Use page LSNs
- Include record IDs and write operations that can be “applied if absent”

### 10.3 Vacuum / GC is required
Old versions and ended index entries accumulate.
You need:
- Track oldest active `start_ts` among running queries/subscriptions.
- Safe to delete versions where `end_ts < oldest_active_ts`.
- Same for index entries.

---

## 11. Handling filter complexity (AND/OR) with indexes

### 11.1 Planning
Given a filter tree:
- Choose an index that covers a selective predicate if possible.
- For `AND`: intersect candidate sets (or do one index scan then filter)
- For `OR`: union scans from multiple indexes (or scan superset then filter)

### 11.2 Read set for complex predicates
Your read set must reflect what you *logically read*:
- If you do multiple index scans and union them, record multiple `ReadIndexRange` entries.
- If you fall back to table scan, record `ReadCollectionScan`.

For serializable validation and subscription invalidation, it’s acceptable (and often easier) to over-approximate:
- If you can’t express the predicate precisely as ranges, record a coarse scan marker.

---

## 12. Document ID generation

Requirements:
- unique, fast to generate
- ideally roughly ordered to avoid B-Tree hotspotting

Good choices:
- **UUIDv7**-style (time-ordered)
- **ULID**
- Random 128-bit (works but may cause more page splits)

Store doc_id as 16 bytes, compare lexicographically.

---

## 13. Minimal public API (suggested)

### Database / collections
- `create_collection(name)`
- `drop_collection(name)`
- `create_index(collection, index_name, field_path)`
- `drop_index(...)`

### Documents
- `insert(collection, json) -> doc_id`
- `get(collection, doc_id) -> Option<json>`
- `patch(collection, doc_id, patch_ops) -> bool` (or returns updated doc)
- `delete(collection, doc_id) -> bool`

### Transactions
- `begin_query_txn() -> Txn`
- `begin_mutation() -> Txn`
- `txn.query(collection, filter, options) -> Vec<doc>`
- `txn.insert/patch/delete/get(...)`
- `txn.commit() -> Result<commit_ts, Conflict>`
- `txn.abort()`

### Subscriptions
- `subscribe(query) -> SubscriptionHandle`
- `poll(handle) -> current result`
- Or push-based callbacks (harder; polling is simpler)

---

## 14. What to implement first (incremental plan)

1. **Collections + primary index + MVCC versions**
   - insert/get by id
   - patch/delete by id
   - snapshot reads by `start_ts`
2. **Transactions with OCC validation (doc-level only)**
   - point reads recorded
   - write-write conflicts
3. **Secondary index on one field**
   - equality queries
   - read sets include `ReadIndexEq`
4. **Range queries + proper `ReadIndexRange`**
5. **Serializable phantom protection**
   - validate range reads against writes
6. **Subscriptions**
   - reuse same read set intersection logic
7. **Vacuum**
   - required for long-running subscriptions

---

## 15. Important tradeoffs and “gotchas” (so you don’t get stuck)

1. **MVCC + secondary indexes is where correctness lives.**
   If you don’t version index entries (or otherwise make them snapshot-correct), you will get wrong query results at snapshots and/or you’ll miss conflicts and invalidations.

2. **Serializable snapshot isolation is non-trivial.**
   The approach described here is OCC-style serializable validation using read sets and predicate/range tracking. It can be correct, but only if you faithfully record read predicates at execution time.

3. **Table scans force coarse conflicts.**
   If you record `ReadCollectionScan`, then any write in that collection invalidates it, reducing concurrency. That’s acceptable early.

4. **Subscriptions are just long-lived reads.**
   They will prevent vacuum from removing old versions unless you design subscriptions to advance their snapshot (usually you rerun at latest and update the read set).

---

## 16. Clarifying questions (affects exact formats and algorithms)

To turn this design into a concrete file/page format and a step-by-step implementation plan, I need these decisions:

1. Do you require **arbitrary historical queries** (query at an old timestamp), or only “current snapshot” queries for active transactions?
2. Is your first target **single-writer** (one committing mutation at a time), or do you want concurrent writers immediately?
3. What JSON patch format do you want (RFC 6902 JSON Patch, RFC 7396 Merge Patch, or custom)?
4. Which indexed field types must be supported first (string, number, bool, null, arrays)?

If you answer those, I can produce a more concrete follow-up design with:
- exact record layouts for DocVersion and index entries
- B-Tree key encodings for JSON types
- the precise read-set/write-set structures
- commit validation pseudocode
- subscription invalidation algorithm and complexity considerations