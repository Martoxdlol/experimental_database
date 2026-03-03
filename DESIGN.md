# Technical Requirements Specification: Distributed JSON Document Store

---

## 1. Core Data Model

### 1.1 Databases

- A database is an isolated group of collections.
- Authentication and authorization are scoped to a specific database.
- Transactions (both read-only and mutation) are serializable (SI + OCC via the transaction log) and scoped to a single database. A transaction can read/write across multiple collections within the same database.
- All operations except management operations (e.g. create/drop/list database) are scoped to a database.
- Databases are fully isolated from each other:
  - **Resource controls**: configurable limits for disk space, memory, and CPU usage.
  - **Usage tracking**: the system must track and expose resource consumption per database.

### 1.2 Collections

- A collection is a named namespace for documents within a database.
- Individual document operations (query, insert, patch, delete, get) are scoped to a collection.
- Documents within a collection are schema-less.

### 1.3 Document Identity

- Documents are identified by a 128-bit ULID (Universally Unique Lexicographically Sortable Identifier).
- API representation: 26-character lowercase Crockford's Base32 string.
- ULIDs are automatically generated on insert.
- Decoding is case-insensitive; encoding always produces lowercase.

### 1.4 Supported Types

Documents are schema-less but the system recognizes the following value types:

| Type | JSON Representation | Notes |
|------|---------------------|-------|
| `id` | string | Document identifier, ULID format |
| `string` | string | UTF-8 text |
| `null` | null | Explicit null value |
| `int64` | number (no decimal) | 64-bit signed integer |
| `float64` | number (with decimal) | 64-bit IEEE 754 floating point |
| `boolean` | boolean | true / false |
| `bytes` | string (base64) | Binary data; base64 in JSON, native in BSON |
| `array` | array | Ordered list of values |
| `object` | object | Nested key-value structure |

- A missing field (undefined) is distinct from an explicit null.

### 1.5 Document Encoding

- Internal storage and wire protocol use **BSON** (Binary JSON) for binary encoding.
  - BSON natively discriminates int32/int64 vs double (float64).
  - BSON has native binary data subtype support (no base64 overhead).
  - BSON has a native datetime type (used for `_created_at` and timestamps).
  - Rust: `bson` crate. JavaScript: `bson` (official MongoDB package).
- JSON is supported as an alternative wire format for debugging and human-readable tooling, with the caveat that bytes must be base64-encoded and int64/float64 distinction relies on the presence of a decimal point.
- Maximum document size: **16 MB** binary-encoded (configurable per database).

### 1.6 Type Ordering

Total ordering for index comparisons and sorting:

```
undefined (no value)
  < null
  < int64 (signed, ascending)
  < float64 (IEEE 754, ascending; NaN sorts last within type)
  < boolean (false < true)
  < string (lexicographic, UTF-8 byte order)
  < bytes (lexicographic byte order)
  < array (element-wise comparison)
  < object (not comparable, not indexable)
```

- `int64` and `float64` are **distinct types** with separate ordering positions. Cross-type numeric comparison is not supported.
- Equality and range queries on indexes must match on type: an `int64` value of `5` is not equal to a `float64` value of `5.0` in index operations.

### 1.7 Field Paths

- A field path identifies a (possibly nested) field within a document.
- Internal representation: an ordered array of string segments. E.g. `["user", "id"]` for nested field `user.id`.
- API supports two forms:
  - Simple string for top-level fields: `"user_id"`
  - Array of strings for nested fields: `["user", "id"]`
- Multi-field (compound) indexes are supported. Specified as an array of field paths:
  - Example: `[["user", "email"], "enabled"]` — compound index on nested `user.email` and top-level `enabled`.

### 1.8 Operations

- **Insert**: create a new document with an auto-generated ULID.
- **Replace**: full document replacement (entire document body is overwritten).
- **Patch**: partial update with shallow merge semantics. Only top-level fields present in the request are replaced; omitted fields are left unchanged. Setting a field to null stores an explicit null. To remove a field entirely, list it in `_meta.unset` (see 1.12).
- **Delete**: logical deletion via a tombstone version.
- **Get**: retrieve a document by ID.

### 1.9 Multi-Versioning (MVCC)

- Every document write (insert, replace, patch, delete) creates a new version identified by a unique timestamp.
- Reads are pinned to a specific timestamp and can only access data committed prior to that timestamp.
- Deletes create a tombstone version; reads after the tombstone see "not found."
- Old versions are eligible for cleanup by the vacuuming process.

### 1.10 Array Indexing

- Indexes can index array fields: one index entry is created per element in the array.
- **Restriction**: a compound index may contain at most one array-typed field. Multiple array fields in a single compound index are not supported.

### 1.11 System Metadata

- `_id` and `_created_at` are stored as regular top-level fields on every document (not inside `_meta`).
- `_id`: the document's ULID. Set on insert, immutable. Automatically indexed on every collection (primary index).
- `_created_at`: automatically set on document creation (timestamp). Automatically indexed on every collection.
- No automatic `_updated_at` field.
- These two default indexes (`_id`, `_created_at`) are always present and cannot be dropped.

### 1.12 Wire Format and the `_meta` Convention

- The field name `_meta` is reserved at the top level of all documents. It is never persisted as part of the document — it is stripped on ingest and used exclusively for wire-format metadata.
- Users cannot store a field named `_meta` at the document root. Nested objects may contain `_meta` fields freely.

#### 1.12.1 `_meta.unset` — Field Removal in Patches

- Patch operations use shallow merge semantics: top-level fields in the request replace the corresponding fields in the stored document. Null means "set to null" (not remove).
- To remove fields entirely, the client provides `_meta.unset`: an array of field paths to delete.
- Field paths use the same notation as everywhere else (string for top-level, array for nested).
- Example — set email to null, remove `old_field` and nested `address.zip`:

```json
{
  "email": null,
  "_meta": { "unset": ["old_field", ["address", "zip"]] }
}
```

#### 1.12.2 `_meta.types` — Type Hints for JSON Wire Format

- When using the JSON wire format, certain types are ambiguous: int64 vs float64 (both JSON numbers), bytes and id vs string (all JSON strings).
- The client may include `_meta.types` to disambiguate. The types object mirrors the document structure: leaf values are type name strings (`"id"`, `"int64"`, `"bytes"`), nested objects are containers.
- Example — annotate `user_id` as an id, `avatar` as bytes, `count` as int64:

```json
{
  "user_id": "01h5kz3x7d8c9v2npqrstuvwxy",
  "avatar": "iVBORw0KGgo=",
  "count": 42,
  "_meta": {
    "types": {
      "user_id": "id",
      "avatar": "bytes",
      "count": "int64"
    }
  }
}
```

- For nested fields, the types object mirrors the nesting:

```json
{ "_meta": { "types": { "profile": { "avatar": "bytes" } } } }
```

- When using BSON wire format, `_meta.types` is unnecessary since BSON carries native type information. The server ignores `_meta.types` in BSON messages.
- Unannotated JSON values use default inference: numbers without decimal → int64, numbers with decimal → float64, strings → string.

#### 1.12.3 `_meta` in Responses

- The server may include `_meta` in response documents to carry wire-format metadata if needed. System fields (`_id`, `_created_at`) are regular top-level document fields, not carried in `_meta`. Further details in the API specification.

---

## 2. Storage Engine

### 2.1 Architecture Overview

The storage engine follows a **WAL-first, clustered B-tree** architecture:

- **Write-Ahead Log (WAL)**: append-only, durable log of all committed transactions. The WAL is the source of truth — all state can be reconstructed by replaying it from the beginning.
- **Page Store**: a single data file containing fixed-size pages organized as B-trees. Stores both documents (in the clustered primary B-tree) and secondary indexes (in separate B-trees). The page store is a materialized acceleration structure derived from the WAL.
- **Buffer Pool**: in-memory LRU cache of pages. All page reads and writes go through the buffer pool. Supports datasets larger than available RAM by evicting cold pages to disk.

**Write path** (transaction commit):

1. Validate (OCC) — see section 5.7.
2. Assign `commit_ts`.
3. Serialize and append WAL record. **fsync**.
4. Apply mutations to pages in the buffer pool (primary B-tree + secondary indexes). Pages are marked dirty.
5. Update commit log and latest committed timestamp.
6. Return success to the client.

**Read path**:

1. Pin to `read_ts` (latest committed timestamp at transaction begin).
2. Query planner selects access method (primary get, index scan, or table scan).
3. Traverse B-tree pages via the buffer pool. Buffer pool fetches from disk on cache miss.
4. For MVCC: skip versions with `ts > read_ts`, take the first version with `ts ≤ read_ts`.

### 2.2 File Layout

A database is stored as a directory:

```
<database_dir>/
  wal/
    segment-000001.wal    # WAL segments (~64 MB each)
    segment-000002.wal
    ...
  data.db                 # Page store (primary B-trees + secondary index B-trees)
  meta.json               # Database config, checkpoint LSN, page size
```

- **WAL segments**: fixed-size append-only files. New segment on rollover. Old segments reclaimed after checkpoint.
- **data.db**: the page store. Array of fixed-size pages. Page 0 is reserved for the file header (page count, free list head, root page IDs for each B-tree).
- **meta.json**: database-level metadata. Written atomically (write to temp, fsync, rename).

### 2.3 Page Format (Slotted Pages)

All pages use a **slotted page** layout. Fixed page size (default **8 KB**, configurable at database creation, immutable after).

```
┌──────────────────────────────────────────┐
│ Page Header (32 bytes)                   │
│   page_id:          u32                  │
│   page_type:        u8                   │
│     (BTreeInternal | BTreeLeaf           │
│      | Overflow | Free)                  │
│   flags:            u8                   │
│   num_slots:        u16                  │
│   free_space_start: u16                  │
│   free_space_end:   u16                  │
│   right_sibling:    u32  (leaf chains)   │
│   checksum:         u32                  │
│   lsn:              u64  (last WAL LSN)  │
├──────────────────────────────────────────┤
│ Slot Directory (grows forward →)         │
│   [offset: u16, length: u16] × num_slots │
├──────────────────────────────────────────┤
│              Free Space                  │
├──────────────────────────────────────────┤
│ Cell Data (grows backward ←)             │
│   variable-length records                │
└──────────────────────────────────────────┘
```

- **Slot directory** grows downward from the header. Each slot is 4 bytes (offset + length).
- **Cell data** grows upward from the end of the page.
- Page is full when `free_space_start ≥ free_space_end`.
- **LSN** (Log Sequence Number): WAL position of the last modification to this page. Used during crash recovery to determine which WAL records still need to be replayed.
- **right_sibling**: used in B-tree leaf pages to form a linked list for efficient range scans.

### 2.4 Primary Store (Clustered B-Tree)

Document versions are stored directly in the leaf pages of the **primary B-tree** (clustered index). There is no separate heap — the B-tree IS the document store.

**Primary key**: `doc_id[16] || inv_ts[8]`

- `doc_id`: 16-byte ULID.
- `inv_ts`: `u64::MAX - commit_ts`. Inverted so the most recent version sorts first within a given doc_id.

**Leaf cell format**:

```
┌────────────────────────────────────┐
│ Key: doc_id[16] || inv_ts[8]       │  24 bytes
├────────────────────────────────────┤
│ flags: u8                          │  (tombstone, overflow)
│ body_length: u32                   │
│ body: [u8; body_length]            │  BSON-encoded document
└────────────────────────────────────┘
```

- **Tombstone** documents: `flags` has tombstone bit set, no body.
- **Overflow**: if the document body exceeds ~75% of usable page space (~6 KB for 8 KB pages), the cell stores the first portion inline and the remainder in linked overflow pages (see 2.5).

**Internal node cell format**:

```
┌────────────────────────────────────┐
│ Key: doc_id[16] || inv_ts[8]       │  24 bytes
│ child_page_id: u32                 │  4 bytes
└────────────────────────────────────┘
```

**Benefits of clustered storage**:

- **Point lookup**: single B-tree traversal lands on the document data. No heap indirection.
- **Version resolution**: seek to `(doc_id, inv_ts_for_read_ts)` — the first matching entry is the correct version. All versions of a document are physically adjacent.
- **Insert performance**: ULIDs are time-ordered, so recent inserts append to the rightmost leaf pages (sequential, cache-friendly).

**One B-tree per collection**: each collection has its own primary B-tree, with its root page ID stored in the catalog.

### 2.5 Overflow Pages

Documents larger than the inline threshold (~75% of usable page space) use **overflow pages**.

```
┌─────────────────────────────────────┐
│ Overflow Page Header                │
│   page_id:       u32               │
│   page_type:     Overflow           │
│   next_overflow: u32 (0 = last)     │
│   data_length:   u16               │
├─────────────────────────────────────┤
│ Overflow Data                       │
│   [u8; data_length]                 │
└─────────────────────────────────────┘
```

- The B-tree leaf cell stores a portion of the body inline + a pointer to the first overflow page.
- Overflow pages form a singly-linked list.
- Maximum document size (16 MB) requires at most ~2,000 overflow pages at 8 KB page size.

### 2.6 Free Space Management

**Free page list**: a linked list of free pages in the data file. The file header (page 0) stores the head of the free list. Each free page stores a pointer to the next free page.

- When a page is deallocated (e.g., B-tree merge, vacuum): add to free list.
- When a new page is needed: pop from free list, or extend the data file.

No per-page free space map is needed since document storage is managed by B-tree insert/split mechanics (not heap-style tuple insertion).

### 2.7 Buffer Pool

The buffer pool manages a fixed pool of in-memory page frames, providing the interface between B-trees and disk I/O.

**Structure**:

- `page_table: HashMap<PageId, FrameId>` — maps on-disk page IDs to in-memory frames.
- `frames: Vec<Frame>` — fixed-size array of page-sized buffers.
- Each frame: `{ data: [u8; PAGE_SIZE], pin_count: u32, dirty: bool, ref_bit: bool }`.

**Operations**:

- `fetch_page(page_id) → PinnedPage`: if cached, pin and return. Otherwise, evict a frame, read page from disk, pin, return.
- `new_page() → PinnedPage`: allocate a page (free list or file extension), pin a frame, return.
- `unpin(page_id, dirty: bool)`: decrement pin count. Set dirty flag if modified.
- `flush_page(page_id)`: write dirty page to disk, clear dirty flag.

**Eviction policy**: **Clock algorithm** (approximation of LRU with lower overhead). Only unpinned frames are evictable. Dirty pages are flushed to disk before eviction.

**Memory budget**: configurable per database. Frame count = `memory_budget / page_size`. Default: 256 MB → 32,768 frames at 8 KB pages.

**Pin contract**: callers must pin pages for the duration of access and unpin promptly. A page cannot be evicted while pinned.

### 2.8 Write-Ahead Log (WAL)

The WAL guarantees durability. Every committed transaction is recorded in the WAL before its effects become visible in the page store.

**Record format**:

```
┌──────────────────────────────────────┐
│ length:      u32  (little-endian)    │
│ crc32:       u32                     │
│ record_type: u8                      │
│ payload:     [u8; length]            │
└──────────────────────────────────────┘
```

**Record types**:

| Type | Payload |
|------|---------|
| `TxCommit` | `tx_id: u64`, `commit_ts: u64`, `mutations: Vec<Mutation>` |
| `Checkpoint` | `checkpoint_lsn: u64` |
| `CreateCollection` | `collection_id: u64`, `name: String` |
| `DropCollection` | `collection_id: u64` |
| `CreateIndex` | `index_id: u64`, `collection_id: u64`, `field_paths: Vec<FieldPath>` |
| `DropIndex` | `index_id: u64` |

**Mutation** (within a TxCommit record):

```
collection_id: u64
doc_id:        u128  (ULID)
op_type:       u8    (Insert | Replace | Delete)
body:          Option<Vec<u8>>  (BSON; absent for Delete)
```

Patch operations are resolved to full document bodies before writing to the WAL. The WAL stores the final document state, not the delta. This simplifies replay.

**Write protocol**:

1. Serialize the WAL record.
2. Append to the current WAL segment.
3. **fsync** to guarantee durability.
4. Return the LSN (byte offset in the WAL stream).

**Group commit**: multiple concurrent transactions batch their WAL records into a single fsync call, amortizing the disk flush cost. A brief batching window (~1 ms) collects pending commits and flushes them together.

**Segment rollover**: when a segment reaches ~64 MB, close it and start a new one. Old segments are retained until a checkpoint covers them.

### 2.9 Checkpoint

Checkpointing flushes dirty buffer pool pages to the data file, allowing old WAL segments to be reclaimed.

**Fuzzy checkpoint** (non-blocking):

1. Record `checkpoint_lsn` = current WAL position.
2. Iterate all dirty frames in the buffer pool.
3. Write each dirty page to the data file. Clear dirty flag.
4. fsync the data file.
5. Write a `Checkpoint` WAL record.
6. Update `meta.json` with the new checkpoint LSN.
7. Delete WAL segments fully before `checkpoint_lsn`.

**Trigger conditions** (whichever comes first):

- WAL size exceeds threshold (default: 64 MB).
- Time since last checkpoint exceeds threshold (default: 5 minutes).
- Graceful shutdown.

### 2.10 Crash Recovery

On startup after a crash:

1. Read `meta.json` → last `checkpoint_lsn`.
2. Open `data.db` — all pages are at least as recent as the checkpoint.
3. Open WAL, scan forward from `checkpoint_lsn`.
4. For each committed `TxCommit` record: **redo** the mutations — insert document versions into the primary B-tree and update secondary indexes via the buffer pool.
5. Skip uncommitted transactions (they were never applied to the page store).
6. Optionally checkpoint immediately to shrink the recovery window.

**No undo phase needed**: mutations are buffered in the in-memory write set and only applied to the page store after WAL commit. The buffer pool never contains dirty pages from uncommitted transactions (no-steal policy). Only redo of committed transactions is required.

### 2.11 Vacuuming

Old document versions are retained for MVCC readers but eventually need cleanup.

**Reclaimability**: a document version is reclaimable when no active transaction or subscription can read it. Specifically: for a given `(collection, doc_id)`, all versions older than the most recent version visible at `oldest_active_read_ts` are reclaimable.

**Process**:

1. Compute `oldest_active_read_ts` from all active transactions and subscriptions.
2. Iterate the primary B-tree (incrementally, one collection at a time to avoid long pauses).
3. For each document with multiple versions: identify reclaimable versions (all versions older than the most recent version before `oldest_active_read_ts`).
4. Remove reclaimable cells from primary B-tree leaf pages.
5. Remove corresponding entries from all secondary indexes.
6. Reclaim overflow pages.
7. Record vacuum operations as WAL records for crash recovery.

**Scheduling**: background tokio task, runs periodically or when space pressure is detected. Yields to active transactions to avoid contention.

---

## 3. Indexing

### 3.1 Index Types

Every collection has two mandatory indexes that cannot be dropped:

| Index | Key | Purpose |
|-------|-----|---------|
| Primary | `doc_id[16] \|\| inv_ts[8]` | Document storage (clustered) and point lookups by ID |
| Created-at | `created_at_ts[8] \|\| doc_id[16] \|\| inv_ts[8]` | Time-ordered queries |

Additional **secondary indexes** can be created on arbitrary field paths (simple or compound).

### 3.2 B+ Tree Structure

All indexes are **B+ trees**: internal nodes contain keys and child pointers; leaf nodes contain keys, values, and sibling pointers for range scans.

**Internal nodes**:

```
[child₀, key₁, child₁, key₂, child₂, ..., keyₙ, childₙ]
```

Where `child_i` points to the subtree with keys in range `[key_i, key_{i+1})`.

**Leaf nodes**:

```
[key₁, val₁, key₂, val₂, ..., keyₙ, valₙ] → right_sibling
```

Leaf pages are linked via `right_sibling` for efficient forward range scans.

**Fan-out**: determined by page size and key/value sizes. With 8 KB pages:

- Primary B-tree internal nodes: ~250 entries (28-byte key+pointer per cell).
- Secondary index internal nodes: ~100–500 entries depending on key size.
- A 3-level primary B-tree can address ~15 million document versions.

### 3.3 Versioned Index Keys

All index keys embed `doc_id[16]` and `inv_ts[8]` (inverted timestamp = `u64::MAX - commit_ts`) to support MVCC. Within a given `(key_prefix, doc_id)` group, the most recent version sorts first.

**Primary index key** (24 bytes, clustered):

```
doc_id[16] || inv_ts[8]
```

Value: document body (BSON), inline or with overflow pointer (see 2.4).

**Secondary index key** (variable length):

```
type_tag[1] || encoded_value[var] || doc_id[16] || inv_ts[8]
```

Value: empty. The `doc_id` embedded in the key is sufficient for primary index lookup.

**Compound secondary index key**:

```
type_tag₁[1] || value₁[var] || type_tag₂[1] || value₂[var] || ... || doc_id[16] || inv_ts[8]
```

Each `type_tag || value` pair is self-delimiting (see 3.4), allowing compound keys to be concatenated and compared with a single memcmp.

### 3.4 Order-Preserving Key Encoding

Index keys must be **byte-comparable** (memcmp-ordered) and preserve the type ordering from section 1.6.

**Type tags** (ascending byte values):

```
0x00 = undefined (field absent)
0x01 = null
0x02 = int64
0x03 = float64
0x04 = boolean
0x05 = string
0x06 = bytes
0x07 = array
```

Since `int64` and `float64` are distinct types with no cross-type comparison, each gets its own tag and independent encoding. An `int64` value always sorts before any `float64` value.

**Value encoding per type**:

| Type | Encoding | Size |
|------|----------|------|
| undefined | type tag only | 1 byte |
| null | type tag only | 1 byte |
| int64 | Big-endian 8 bytes with sign bit flipped (XOR high byte with `0x80`) | 9 bytes |
| float64 | IEEE 754 big-endian; positive: flip sign bit; negative: flip all bits | 9 bytes |
| boolean | `0x00` = false, `0x01` = true | 2 bytes |
| string | UTF-8 bytes, `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` | variable |
| bytes | Raw bytes, `0x00` escaped as `0x00 0xFF`, terminated by `0x00 0x00` | variable |
| array | Element-wise: each element as `type_tag \|\| encoded_value`, terminated by `0x00 0x00` | variable |

The encoding is **self-delimiting**: a decoder can determine where one encoded value ends and the next begins. This is essential for compound keys where multiple encoded values are concatenated.

### 3.5 Secondary Index Reads (Version Resolution)

Because each document version generates its own index entries, secondary index scans must resolve which version is visible at the reader's `read_ts`.

**Algorithm**:

1. Seek into the secondary index at the scan's lower bound.
2. Scan forward, collecting entries that match the query predicate.
3. For each `(doc_id, inv_ts)` pair:
   a. Compute `version_ts = u64::MAX - inv_ts`.
   b. **Skip** if `version_ts > read_ts` (version created after our snapshot).
   c. Within consecutive entries for the same `doc_id`: take the **first** (highest `version_ts ≤ read_ts`), skip the rest.
4. **Verify**: look up the primary index for this `doc_id` at `read_ts`. Confirm the latest visible version's timestamp matches the secondary index entry's timestamp. If a newer version exists (with different field values), the secondary entry is stale — skip it.

**Why verification is needed**: when a document is updated and a field value changes, the old index entry remains (append-only model). A reader might find a stale entry for the old value. Verification against the primary index confirms the entry reflects the actual current version.

**Optimization**: for recently written data (the common case), the secondary index entry's version IS the latest version, so verification confirms immediately. The overhead is meaningful only for documents with frequent updates to indexed fields.

### 3.6 Array Indexing

When a field contains an array, one index entry is created per element:

- Document `{ tags: ["a", "b", "c"] }` → 3 secondary index entries with values `"a"`, `"b"`, `"c"`, each tagged with the same `(doc_id, inv_ts)`.

**Restriction**: a compound index may include at most one array-typed field. This prevents combinatorial explosion (N × M entries for two arrays of size N and M).

### 3.7 Background Index Building

Creating an index on an existing collection with data requires backfilling without blocking writes.

**Protocol**:

1. Register the index in the catalog as `Building`. Queries cannot use it yet.
2. Record the current timestamp as `build_snapshot_ts`.
3. Spawn a background tokio task:
   a. Scan the primary B-tree at `build_snapshot_ts`.
   b. For each visible document: extract field values, insert secondary index entries.
4. Concurrently: all new commits (after `build_snapshot_ts`) also insert entries into the `Building` index in real-time.
5. When the background scan completes: mark the index as `Ready` in the catalog.
6. The index is now available for query planning.

**Crash during build**: if the system crashes while an index is `Building`, drop the partial index on recovery and restart the build (or leave for manual retry).

### 3.8 Index Vacuuming

When document versions are vacuumed (section 2.11), their secondary index entries must also be removed:

- For each vacuumed `(doc_id, version_ts)`: scan all secondary indexes on that collection and remove entries with matching `(doc_id, inv_ts)`.
- Primary B-tree entries for vacuumed versions are removed as part of the primary vacuum pass.
- Reclaimed B-tree pages are added to the free page list.

---

## 4. Query Engine

- **Access Patterns**: index-based lookups and full table scans.
- **Operators**: `eq`, `lt`, `gt`, `gte`, `lte`, `ne`, `AND`, `OR`, `IN`, `NOT`.
- **Constraints**: support for `limit` on all query types.
- **Subscriptions**: live query tracking with automated invalidation and notification when the underlying read set changes via committed writes.

---

## 5. Transactions and Concurrency

### 5.1 Transaction Types

| Type | Reads | Writes | OCC Validation |
|------|-------|--------|----------------|
| Read-only (query) | Yes, at pinned `read_ts` | No | No |
| Read-write (mutation) | Yes, at pinned `begin_ts` + own writes | Yes, buffered in write set | Yes, at commit |

Both types are scoped to a single database and may span multiple collections within that database.

### 5.2 Timestamps

A **monotonic timestamp allocator** (`AtomicU64`) assigns all timestamps.

| Timestamp | Meaning |
|-----------|---------|
| `begin_ts` | Latest committed timestamp when the transaction starts. Defines the read snapshot. |
| `commit_ts` | Assigned at commit time. All mutations in the transaction are tagged with this value. |
| `_created_at` | Set to `commit_ts` on document insert. Immutable on subsequent replace/patch/delete. |

All mutations within a transaction share the same `commit_ts`. The transaction is atomic — it appears to occur at a single logical instant.

### 5.3 Transaction Lifecycle

**Read-only (query)**:

1. `begin()` → acquire `read_ts` = latest committed timestamp.
2. Execute reads. All reads see the consistent snapshot at `read_ts`.
3. `commit()` → release resources. No validation needed.

**Read-write (mutation)**:

1. `begin()` → acquire `begin_ts` = latest committed timestamp.
2. Execute reads (recorded in read set) and writes (buffered in write set).
3. `commit()`:
   a. Acquire the next `commit_ts` (atomic increment).
   b. Validate the read set against the commit log (see 5.7).
   c. If valid: write WAL record → fsync → apply to page store → update commit log → advance latest committed timestamp.
   d. If conflict: abort → return error to client. Client may retry with a new transaction.

### 5.4 Read-Your-Own-Writes

Within a read-write transaction, reads check the **write set first**, then fall through to the snapshot at `begin_ts`.

- **Get by ID**: if `(collection, doc_id)` exists in the write set, return that version. Otherwise, read from the primary B-tree at `begin_ts`.
- **Index scan / table scan**: merge results from the snapshot with the write set:
  - Include documents inserted in the write set that match the query filter/range.
  - Exclude documents deleted in the write set.
  - For documents modified in the write set: use the write set version instead of the snapshot version.

### 5.5 Write Set

The write set buffers all mutations until commit.

```
WriteSet {
    mutations: BTreeMap<(CollectionId, DocId), MutationEntry>
}

MutationEntry {
    op:          Insert | Replace | Delete
    body:        Option<Document>   // Resolved BSON (post-merge for patches). None for Delete.
    previous_ts: Option<u64>        // Version being replaced. None for Insert.
}
```

- **Insert**: generate ULID, store full document body (with `_id` and `_created_at` set).
- **Replace**: store full new document body. `_id` and `_created_at` preserved from original.
- **Patch**: read current document (from write set or snapshot), apply shallow merge, store resolved body.
- **Delete**: store tombstone marker. Record `previous_ts` for conflict detection.

Patch operations are resolved eagerly: the write set always contains the final document body, never a delta. The WAL also stores resolved bodies (see 2.8).

### 5.6 Read Set

The read set records every read performed during a transaction. Used for OCC validation at commit time and as the foundation for subscription invalidation.

```
ReadSet {
    entries: Vec<ReadSetEntry>
}

ReadSetEntry:

    Get {
        collection_id: CollectionId
        doc_id:        DocId
    }

    IndexScan {
        collection_id:  CollectionId
        index_id:       IndexId
        lower_bound:    Option<EncodedKey>
        upper_bound:    Option<EncodedKey>
        filter:         Option<Filter>
        limit:          Option<u64>
        result_doc_ids: Vec<DocId>
    }

    TableScan {
        collection_id:  CollectionId
        filter:         Option<Filter>
        limit:          Option<u64>
        result_doc_ids: Vec<DocId>
    }
```

Every query operation appends an entry. This captures both the **query parameters** (what was asked) and the **result set** (what was returned). Both are needed: query parameters define the conflict range, result set enables precise invalidation.

### 5.7 OCC Validation (Conflict Detection)

At commit time, the read set is validated against all transactions that committed in the interval `(begin_ts, commit_ts)`.

**Commit log** — in-memory structure tracking recent commits:

```
CommitLog {
    entries: Vec<CommitLogEntry>    // ordered by commit_ts
}

CommitLogEntry {
    commit_ts: u64
    writes:    Vec<WriteRecord>
}

WriteRecord {
    collection_id: CollectionId
    doc_id:        DocId
    index_deltas:  Vec<IndexDelta>
}

IndexDelta {
    index_id: IndexId
    old_key:  Option<EncodedKey>    // None for inserts
    new_key:  Option<EncodedKey>    // None for deletes
}
```

The `index_deltas` capture the old and new index key values for each write. This enables precise phantom detection without re-reading documents.

**Validation rules** — for each `ReadSetEntry`, check all `CommitLogEntry` in `(begin_ts, commit_ts)`:

| Read Set Entry | Conflict Condition |
|----------------|-------------------|
| `Get(col, doc_id)` | Any concurrent write to the same `(col, doc_id)`. |
| `IndexScan(col, idx, bounds, ...)` | Any concurrent write where: **(a)** `doc_id` is in `result_doc_ids` (direct hit), OR **(b)** an `IndexDelta` for this `index_id` has `old_key` or `new_key` within `[lower_bound, upper_bound]` (phantom — a document entered or left the scan range). |
| `TableScan(col, filter, ...)` | Any concurrent write to the same `collection_id`. **Optimization**: if a `filter` is present, only conflict if the written document (before or after the write) satisfies the filter. |

**Limit-aware phantom detection**:

- A query with `limit: N` that returned **fewer** than N results scanned the entire matching range — the effective bounds are the full scan range.
- A query with `limit: N` that returned **exactly** N results: the effective upper bound tightens to the index key of the Nth (last) result. Any insert/update within `[lower_bound, key_of_Nth_result]` is a phantom conflict, since it could displace a result.

**Commit log pruning**: entries with `commit_ts ≤ oldest_active_begin_ts` can be removed — no active transaction will validate against them.

### 5.8 Subscription Invalidation

Subscriptions are persistent read sets that receive push notifications when affected by new commits.

A subscription is created by executing a query with `subscribe: true`. The query's read set entry becomes the subscription's **watch predicate**.

**On every commit**, check all active subscriptions against the committed write set using the same conflict detection rules from 5.7:

| Subscription Type | Invalidation Trigger |
|-------------------|---------------------|
| Point (Get) | Committed write to the watched `(collection, doc_id)`. |
| Index scan | Committed write with an `IndexDelta` in the watched range, or to a document in the result set. |
| Table scan | Any committed write to the watched collection (with filter optimization). |

**Subscription registry** — indexed for fast lookup:

```
SubscriptionRegistry {
    // O(1) lookup for point watches
    point_index: HashMap<(CollectionId, DocId), Vec<SubscriptionId>>

    // Grouped by (collection, index) for range overlap checks
    scan_index:  HashMap<(CollectionId, Option<IndexId>), Vec<SubscriptionId>>
}
```

When a commit writes to `(collection, doc_id)`:

1. Check `point_index[(collection, doc_id)]` for direct matches.
2. For each scan subscription on this collection: check if the write's `IndexDelta` key falls within the subscription's bounds, or if `doc_id` is in the subscription's `result_doc_ids`.
3. Fire invalidation notification for all matched subscriptions.

### 5.9 Concurrency Model

- **Async runtime**: all I/O and computation runs on the tokio async runtime.
- **Buffer pool locking**: page-level read/write latches (short-lived, never held across await points).
- **Write serialization**: WAL append + `commit_ts` assignment is a critical section, serialized via a mutex or single-writer channel. This is the serialization point for all writes.
- **Read concurrency**: fully concurrent. Multiple readers at different `read_ts` values traverse B-trees simultaneously without coordination.
- **OCC advantage**: readers never block writers, writers never block readers. Conflicts are detected only at commit time.

---

## 6. Distributed Architecture

- **Replication**: single Primary node for all write operations with multiple Read Replicas.
- **Consistency**: strong consistency across nodes via replication logs.

---

## 7. API Definition

- **Collection Management**: `create_collection(name)`, `delete_collection(name)`, `create_index(field)`.
- **Query Interface**: `start_query(query_type, query_id, subscribe: bool)`, `read_data()`, `commit_query()`.
- **Mutation Interface**: `start_mutation()`, `read_data()`, `write_data()`, `commit_mutation()`. Automated rollback requires explicit user retry.

---

## 8. Technical Stack and Delivery

- **Language**: Rust.
- **Async Runtime**: tokio.
- **Architecture**: modular design where every component is a discrete module.
- **Deployment**: embedded library instantiated directly within Rust application code.
