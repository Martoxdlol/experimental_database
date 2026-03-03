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

The data root contains a **system database** (`_system/`) and one directory per user database. Every database — including `_system` — uses the same internal layout (WAL + page store + metadata).

```
<data_root>/
  _system/                        # System database (database registry)
    wal/
      segment-000001.wal
    data.db                       # Catalog B-tree: database registry entries
    meta.json
  myapp/                          # User database "myapp"
    wal/
      segment-000001.wal
      segment-000002.wal
    data.db                       # Catalog B-tree + collection B-trees + index B-trees
    meta.json
  analytics/                      # User database "analytics"
    wal/
      segment-000001.wal
    data.db
    meta.json
```

**Per-database directory layout**:

```
<database_dir>/
  wal/
    segment-000001.wal    # WAL segments (~64 MB each)
    segment-000002.wal
    ...
  data.db                 # Page store (catalog B-tree + primary B-trees + secondary index B-trees)
  meta.json               # Database config, checkpoint LSN, page size
```

- **WAL segments**: fixed-size append-only files. New segment on rollover. Old segments reclaimed after checkpoint.
- **data.db**: the page store. Array of fixed-size pages. Page 0 is the file header (see 2.12.3). The catalog B-tree stores collection and index metadata. Each collection has its own primary B-tree and secondary index B-trees.
- **meta.json**: database-level metadata. Written atomically (write to temp, fsync, rename).

**Isolation**: each database has its own WAL, page store, buffer pool, and checkpoint cycle. Databases are fully independent — creating, dropping, backing up, or restoring one database does not affect any other. Per-database resource limits (disk, memory) are enforced at the directory level.

### 2.3 Page Format (Slotted Pages)

All pages use a **slotted page** layout. Fixed page size (default **8 KB**, configurable at database creation, immutable after).

```
┌──────────────────────────────────────────┐
│ Page Header (32 bytes)                   │
│   page_id:          u32                  │
│   page_type:        u8                   │
│     (BTreeInternal | BTreeLeaf           │
│      | Heap | Overflow | Free)           │
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

**Leaf cell format** (inline mode — small documents):

```
┌────────────────────────────────────┐
│ Key: doc_id[16] || inv_ts[8]       │  24 bytes
├────────────────────────────────────┤
│ flags: u8                          │  (tombstone, external)
│ body_length: u32                   │
│ body: [u8; body_length]            │  BSON-encoded document
└────────────────────────────────────┘
```

**Leaf cell format** (external mode — large documents):

```
┌────────────────────────────────────┐
│ Key: doc_id[16] || inv_ts[8]       │  24 bytes
├────────────────────────────────────┤
│ flags: u8                          │  (tombstone, external=1)
│ body_length: u32                   │  total body size
│ heap_page_id: u32                  │  first heap page
│ heap_slot_id: u16                  │  slot within heap page
└────────────────────────────────────┘
```

**Inline vs External storage**: documents are stored in one of two modes based on their BSON-encoded body size:

| Mode | Condition | Leaf Cell Contains |
|------|-----------|-------------------|
| Inline | `body_size ≤ EXTERNAL_THRESHOLD` | Full document body (BSON) |
| External | `body_size > EXTERNAL_THRESHOLD` | `HeapRef` pointer to external heap (see 2.5) |

`EXTERNAL_THRESHOLD` is configurable per database (default: half of page size, e.g., **4 KB** for 8 KB pages). Tuning: lower threshold → better B-tree fan-out and cache utilization; higher threshold → fewer heap lookups for medium-sized documents.

- **Tombstone** documents: `flags` has tombstone bit set, no body (always inline, minimal size).

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

### 2.5 External Heap

Documents whose BSON-encoded body exceeds `EXTERNAL_THRESHOLD` are stored in the **external heap** — a pool of heap pages in the page store dedicated to large document bodies.

**Heap pages** use the standard slotted page layout (section 2.3) with `page_type = Heap`. Each slot holds one document body (or the first chunk of a multi-page document).

**Single-page documents** (`body_size ≤ usable page space`): stored as a single slot in a heap page. The B-tree leaf cell's `HeapRef` points directly to `(heap_page_id, heap_slot_id)`.

**Multi-page documents** (`body_size > usable page space`): the first heap page slot contains the initial chunk of the body plus a pointer to the first **overflow page**. Overflow pages form a singly-linked list:

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

Maximum document size (16 MB) requires at most ~2,000 overflow pages at 8 KB page size.

**Benefits of external heap**:

- **B-tree compactness**: leaf pages maintain high fan-out (hundreds of entries per page) regardless of document size. A collection mixing 100-byte and 1 MB documents has the same B-tree shape.
- **Cache efficiency**: buffer pool pages aren't dominated by a few large documents. More document keys fit per cached page.
- **Scan performance**: index scans that evaluate predicates before fetching full document bodies avoid loading large external documents unnecessarily. Only documents that pass all filters trigger a heap fetch.
- **Predictable splits**: B-tree splits are fast and predictable since leaf cells are small (just key + pointer for external docs).

### 2.6 Free Space Management

**Free page list**: a linked list of completely free pages in the data file. The file header (page 0) stores the head. Each free page stores a next pointer.

- When a page is deallocated (e.g., B-tree merge, vacuum): add to free list.
- When a new page is needed: pop from free list, or extend the data file.

**Heap free space map**: the external heap requires tracking partially-filled heap pages. An in-memory map of `(page_id → free_bytes)` identifies candidate pages for new external document inserts. This map is rebuilt from heap pages on startup and maintained incrementally during operation.

B-tree pages do not need a free space map — their space is managed by B-tree insert/split mechanics.

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

### 2.12 Catalog

The catalog tracks all metadata about databases, collections, and indexes. It is organized in two layers:

- **System catalog** (database registry): lives in the `_system/` database. Tracks which user databases exist, their paths, and configuration.
- **Per-database catalog**: lives inside each database's `data.db` as a **catalog B-tree**. Tracks collections and indexes within that database.

Both layers use the same B-tree and page store implementation as data storage — the catalog is not a special case. At runtime, catalog metadata is cached in an **in-memory HashMap** for O(1) lookups. The catalog B-tree is the durable source of truth; the in-memory cache is derived from it.

#### 2.12.1 System Catalog (Database Registry)

The `_system/` directory is itself a database instance, opened first on startup. Its catalog B-tree stores **database registry entries** — one entry per user database.

**Database registry entry**:

| Field | Type | Description |
|-------|------|-------------|
| `database_id` | `u64` | Unique identifier, monotonically assigned |
| `name` | `String` | Database name (unique, used as directory name) |
| `path` | `String` | Relative path from data root to database directory |
| `created_at` | `u64` | Timestamp of creation |
| `config` | `DatabaseConfig` | Page size, memory budget, max doc size, resource limits |
| `state` | `u8` | `Active`, `Dropping`, `Creating` |

**Catalog B-tree key** (in `_system`):

```
entity_type[1] || entity_id[8]
```

Where `entity_type = 0x01` (Database) and `entity_id` is the `database_id` as big-endian u64.

**Catalog B-tree value**: the database registry entry fields above, serialized as a compact binary format.

**Name lookup**: a secondary structure (in-memory `HashMap<String, u64>` mapping name → database_id) provides O(1) name-based lookup. Populated from the catalog B-tree on startup.

**WAL record types** (system catalog):

| Type | Payload |
|------|---------|
| `CreateDatabase` | `database_id: u64`, `name: String`, `path: String`, `config: DatabaseConfig` |
| `DropDatabase` | `database_id: u64` |

**Lifecycle**:

1. `create_database(name, config)`:
   - Assign `database_id` (monotonic).
   - Write `CreateDatabase` WAL record to `_system` WAL.
   - Insert entry into `_system` catalog B-tree via buffer pool.
   - Create the database directory and initialize an empty `data.db` + `meta.json`.
   - Update in-memory cache.

2. `drop_database(name)`:
   - Mark entry as `Dropping` in catalog B-tree.
   - Write `DropDatabase` WAL record.
   - Close the database (flush, checkpoint).
   - Remove the database directory.
   - Remove entry from catalog B-tree and in-memory cache.

**Startup sequence**:

1. Open `_system/` database (WAL replay, checkpoint recovery).
2. Read `_system` catalog B-tree → populate in-memory database registry.
3. For each `Active` database: open it (see per-database startup below).

#### 2.12.2 Per-Database Catalog (Collections and Indexes)

Within each database's `data.db`, a **catalog B-tree** stores metadata for all collections and indexes. The root page ID of the catalog B-tree is stored in the file header (page 0 — see 2.12.3).

**Catalog entry types**:

```
entity_type:
  0x01 = Collection
  0x02 = Index
```

**Catalog B-tree key**:

```
entity_type[1] || entity_id[8]
```

Where `entity_id` is `collection_id` or `index_id` as big-endian u64.

**Collection entry value**:

| Field | Type | Description |
|-------|------|-------------|
| `collection_id` | `u64` | Unique identifier within this database |
| `name` | `String` | Collection name (unique within database) |
| `primary_root_page` | `u32` | Root page of the primary B-tree (clustered) |
| `created_at_root_page` | `u32` | Root page of the `_created_at` index B-tree |
| `doc_count` | `u64` | Approximate document count (updated lazily) |
| `config` | `CollectionConfig` | Max document size, other collection-level settings |

**Index entry value**:

| Field | Type | Description |
|-------|------|-------------|
| `index_id` | `u64` | Unique identifier within this database |
| `collection_id` | `u64` | Parent collection |
| `name` | `String` | Index name (unique within collection) |
| `field_paths` | `Vec<FieldPath>` | Indexed field paths (single or compound) |
| `root_page` | `u32` | Root page of the secondary index B-tree |
| `state` | `u8` | `Building` (0x01), `Ready` (0x02), `Dropping` (0x03) |
| `unique` | `bool` | Whether the index enforces uniqueness |

**WAL record types** (per-database catalog — these already exist in section 2.8):

| Type | Payload |
|------|---------|
| `CreateCollection` | `collection_id: u64`, `name: String` |
| `DropCollection` | `collection_id: u64` |
| `CreateIndex` | `index_id: u64`, `collection_id: u64`, `field_paths: Vec<FieldPath>` |
| `DropIndex` | `index_id: u64` |

**Lifecycle** (create collection):

1. Assign `collection_id` (monotonic within database).
2. Allocate two new pages: one for the primary B-tree root (empty leaf), one for the `_created_at` index root (empty leaf).
3. Write `CreateCollection` WAL record.
4. Insert collection entry into the catalog B-tree via the buffer pool.
5. Update in-memory cache.

**Lifecycle** (create index):

1. Assign `index_id` (monotonic within database).
2. Allocate a new page for the secondary index B-tree root (empty leaf).
3. Write `CreateIndex` WAL record.
4. Insert index entry into the catalog B-tree with `state = Building`.
5. Update in-memory cache.
6. Begin background index build (see section 3.7).
7. On completion: update catalog entry to `state = Ready`.

**Lifecycle** (drop collection):

1. Write `DropCollection` WAL record.
2. Remove collection entry and all associated index entries from catalog B-tree.
3. Reclaim all pages belonging to the collection's B-trees (primary + secondary indexes) via the free page list.
4. Update in-memory cache.

**Per-database startup sequence**:

1. Read page 0 (file header) → `catalog_root_page_id`.
2. Walk the catalog B-tree (typically 2–3 levels for thousands of collections).
3. Populate in-memory cache:
   - `name_to_collection: HashMap<String, CollectionId>`
   - `collections: HashMap<CollectionId, CollectionMeta>`
   - `indexes: HashMap<IndexId, IndexMeta>`
   - `collection_indexes: HashMap<CollectionId, Vec<IndexId>>`
4. For any index with `state = Building`: drop partial index, optionally restart build.

#### 2.12.3 File Header (Page 0)

Page 0 of every `data.db` file is reserved as the **file header**. It uses a fixed layout (not the standard slotted page format):

```
┌──────────────────────────────────────────┐
│ File Header (Page 0)                     │
│   magic:              u32  (0x45584442)  │  "EXDB"
│   version:            u32               │  File format version
│   page_size:          u32               │  Page size in bytes (e.g. 8192)
│   page_count:         u64               │  Total pages in data file
│   free_list_head:     u32               │  First free page (0 = none)
│   catalog_root_page:  u32               │  Root page of catalog B-tree
│   next_collection_id: u64               │  Monotonic ID allocator
│   next_index_id:      u64               │  Monotonic ID allocator
│   checkpoint_lsn:     u64               │  LSN of last checkpoint
│   created_at:         u64               │  Database creation timestamp
│   reserved:           [u8; ...]         │  Remainder of page (zeroed)
└──────────────────────────────────────────┘
```

The file header is updated during checkpointing (page count, free list head, checkpoint LSN) and during catalog mutations (catalog root page, ID allocators). Updates go through the buffer pool like any other page — the header page is pinned, modified, marked dirty, and flushed during checkpoint.

#### 2.12.4 In-Memory Catalog Cache

At runtime, all catalog lookups go through an **in-memory cache** — never through the B-tree directly. The cache provides O(1) access for the hot path (every insert, get, query must resolve collection name → metadata).

**Cache structure** (per database):

```
CatalogCache {
    // Collection lookup
    name_to_collection: HashMap<String, CollectionId>
    collections:        HashMap<CollectionId, CollectionMeta>

    // Index lookup
    indexes:            HashMap<IndexId, IndexMeta>
    collection_indexes: HashMap<CollectionId, Vec<IndexId>>

    // ID allocators
    next_collection_id: AtomicU64
    next_index_id:      AtomicU64
}
```

**Cache invariant**: the in-memory cache is always consistent with the catalog B-tree. Both are updated atomically within the same commit path:

1. WAL record written and fsynced.
2. Catalog B-tree updated via buffer pool.
3. In-memory cache updated.

If the process crashes between steps 2 and 3, WAL replay on recovery reconstructs the catalog B-tree, and the cache is rebuilt from it during startup.

**Cache population**: on database open, the catalog B-tree is scanned once (a few dozen pages for even very large catalogs) to populate the HashMap. At ~128 bytes per entry, 100,000 collections fit in ~1,600 pages (~13 MB) — the top levels of the catalog B-tree will always be in the buffer pool.

#### 2.12.5 Catalog Scalability

The catalog B-tree scales identically to data B-trees:

| Collections | Catalog B-tree depth | Catalog pages | Startup scan time |
|-------------|---------------------|---------------|-------------------|
| 100 | 1 (single leaf) | 1–2 | < 1 ms |
| 10,000 | 2 | ~20 | < 1 ms |
| 100,000 | 2–3 | ~200 | ~2 ms |
| 1,000,000 | 3 | ~2,000 | ~20 ms |

Schema mutations (create/drop collection or index) are O(log N) in the number of catalog entries — a single B-tree insert or delete. The in-memory cache update is O(1).

The system catalog (`_system`) scales the same way for the database registry. At 10,000+ databases, the B-tree is 2–3 levels deep; startup reads a few dozen pages.

### 2.13 Integrity and Error Recovery

The storage engine must detect corruption, tolerate torn writes, and provide tools for diagnosis and repair. The design philosophy: the **WAL is the source of truth** for recent changes, the **page store is a materialized cache** that can be reconstructed, and **replicas are the ultimate backup** when local recovery is insufficient.

#### 2.13.1 Checksum Verification

**Page checksums**: the `checksum` field in every page header (section 2.3) covers the entire page contents (header fields excluding the checksum itself, slot directory, and cell data). Algorithm: CRC-32C (hardware-accelerated on modern CPUs via SSE 4.2 / ARM CRC instructions).

**Verification policy**: checksums are verified **on every page read from disk** — i.e., every buffer pool cache miss in `fetch_page()`. This catches:

- Silent bit flips in storage (bit rot).
- Torn writes from previous crashes.
- Filesystem or controller bugs.

Cost: ~1% CPU overhead (CRC-32C is hardware-accelerated). This is the right trade-off — corruption that goes undetected compounds over time and becomes unrecoverable.

**Checksum computation**: computed and written whenever a page is flushed to disk (buffer pool `flush_page()` and checkpoint). The checksum is the last field written to the page buffer before the disk write.

**WAL record checksums**: the existing CRC-32 per WAL record (section 2.8) is verified on every read during:

- Crash recovery (WAL replay from checkpoint).
- Replication (replica applying received WAL records).
- Integrity check (full WAL scan).

A CRC mismatch in a WAL record during replay terminates replay at that point — all committed data before the corruption is safe.

**File header checksums**: page 0 (file header, section 2.12.3) includes its own CRC-32C covering all header fields. Verified on database open.

#### 2.13.2 Torn Write Protection

A power failure or crash during a page write to `data.db` can leave a page half-written (torn). The storage engine handles this without a double-write buffer:

**Principle**: the page store is a materialized acceleration structure derived from the WAL. A torn page is simply a page that missed its latest update — the correct state is recoverable from the WAL.

**Detection**: on startup, every page read from disk during recovery is checksum-verified. A torn page will fail its checksum.

**Recovery**: when a checksum failure is detected during normal crash recovery (section 2.10):

1. The page is known to be corrupt.
2. WAL replay will overwrite it with the correct state (since the WAL record for that page's latest modification is still available — it hasn't been checkpointed yet, or the checkpoint itself was interrupted).
3. After recovery, the page is correct in the buffer pool and will be flushed cleanly on the next checkpoint.

**Why this works**: the checkpoint protocol (section 2.9) only deletes WAL segments **after** all dirty pages have been successfully flushed and fsynced. Sequence:

1. Flush all dirty pages → fsync `data.db`.
2. Write `Checkpoint` WAL record with `checkpoint_lsn`.
3. Update `meta.json` with `checkpoint_lsn`.
4. **Only then**: delete WAL segments before `checkpoint_lsn`.

If a crash occurs during step 1 (some pages torn), the WAL segments are still intact. On recovery, the torn pages are detected by checksum and repaired by WAL redo. If a crash occurs during step 4 (WAL deletion), extra WAL segments remain — harmless, they'll be cleaned up next checkpoint.

**Critical invariant**: a WAL segment is never deleted until every page modification it contains has been durably flushed to `data.db` and verified. This is the torn write safety guarantee.

#### 2.13.3 WAL Segment Lifecycle and Reclamation

WAL segments accumulate as transactions commit and are reclaimed after checkpoints. Understanding the lifecycle is critical for recovery guarantees:

**Segment lifecycle**:

```
Created         → Active (receiving appends)
Active          → Sealed (reached ~64 MB, rolled over to new segment)
Sealed          → Reclaimable (all contained records covered by a successful checkpoint)
Reclaimable     → Deleted (reclaimed during checkpoint cleanup)
```

**Reclamation rules**:

- A segment is reclaimable only if its **highest LSN < checkpoint_lsn** — meaning every record in it has been fully materialized in `data.db` and fsynced.
- Reclamation happens at the end of the checkpoint protocol (step 7 in section 2.9), after `meta.json` is updated.
- The active segment (currently being appended to) is never reclaimed.

**What the WAL retains at any given time**:

- All records from the last checkpoint onward (guaranteed).
- The active segment and any sealed-but-not-yet-checkpointed segments.
- After a long-running database with regular checkpoints: typically 1–2 segments (~64–128 MB).

**What the WAL does NOT retain**:

- Records from before the last successful checkpoint. These are gone — the page store is the only copy.
- A full history of all transactions since database creation. The WAL is not an event log; it's a recovery mechanism.

**Implication for repair**: if both a page in `data.db` AND the corresponding WAL records have been lost (e.g., page corrupted after its WAL segment was reclaimed), the data on that page is **unrecoverable from local state alone**. This is where the file header shadow copy (2.13.4) and replica reconstruction (2.13.6) come in.

#### 2.13.4 File Header Redundancy (Shadow Copy)

The file header (page 0) is a single point of failure — it contains the catalog root page pointer, ID allocators, free list head, and checkpoint LSN. After WAL segments have been reclaimed, page 0 cannot be reconstructed from the WAL alone.

**Shadow header**: a copy of the file header is maintained at a **fixed location at the end of the data file** — specifically, the last page of `data.db` (page `page_count - 1`). This page has `page_type = FileHeaderShadow` and contains an identical copy of all file header fields.

**Update protocol**: whenever the file header (page 0) is flushed to disk during checkpoint, the shadow copy is also written and fsynced. The sequence:

1. Write page 0 to disk.
2. Write shadow copy to the last page.
3. fsync.

Both copies include independent checksums.

**Recovery from header corruption**:

1. On database open, read page 0 and verify checksum.
2. If checksum fails: read the shadow copy from the last page of the file (file size / page size - 1).
3. If shadow checksum is valid: restore page 0 from the shadow copy. Log a warning.
4. If both are corrupt: the database cannot be opened from local state. Use replica reconstruction (2.13.6).

**Cost**: one extra page write per checkpoint. Negligible.

#### 2.13.5 Integrity Check and Repair

The system provides an explicit **integrity check** command that performs a full structural verification of the database. This can be run on demand (maintenance), on startup (optional, configurable), or after a suspected issue.

**Integrity check** (`check_integrity(database)`):

Phase 1 — File-level checks:

1. Verify file header (page 0) checksum. Verify shadow header matches.
2. Verify `meta.json` is readable and consistent with file header (`checkpoint_lsn`, `page_size`).
3. Verify file size is consistent with `page_count * page_size`.

Phase 2 — Page-level checks:

4. Scan every page in the data file sequentially.
5. Verify checksum of each page.
6. Verify page header fields are within valid ranges (`page_type` is known, `num_slots` fits within page, `free_space_start ≤ free_space_end`, etc.).
7. Collect: set of all page IDs seen, by type.

Phase 3 — Free list check:

8. Walk the free list from `free_list_head` to completion.
9. Verify: no cycles (track visited pages), all pages in the list have `page_type = Free`, all page IDs are within bounds.
10. Record the set of free pages.

Phase 4 — B-tree structural checks (catalog, primary, secondary):

11. Walk the catalog B-tree from `catalog_root_page`. Verify:
    - Keys are in sorted order within each page.
    - Internal node child pointers reference valid pages with correct `page_type`.
    - Leaf `right_sibling` pointers form a valid chain (no cycles, ascending keys across siblings).
    - All referenced pages are accounted for.
12. For each collection in the catalog: walk the primary B-tree and every secondary index B-tree with the same structural checks.

Phase 5 — Cross-reference checks:

13. **Orphan detection**: every page in `data.db` should be either (a) in a B-tree (reachable from some root), (b) in the free list, (c) page 0 (header), or (d) last page (shadow header). Any page not accounted for is an orphan — likely a leaked page from a crash during a B-tree split or merge.
14. **Double-allocation detection**: no page should appear in more than one B-tree or in both a B-tree and the free list.
15. **Secondary index consistency** (optional, expensive): for a sample of secondary index entries, verify the corresponding primary B-tree entry exists and the indexed field value matches.

**Output**: a report listing all issues found, categorized by severity:

| Severity | Examples |
|----------|---------|
| **Error** | Checksum failure, B-tree structural corruption, double-allocated page |
| **Warning** | Orphaned pages (space leak, not data loss), shadow header mismatch |
| **Info** | Statistics (page counts by type, free space ratio, B-tree depths) |

**Auto-repair** (`repair(database)`):

For issues that can be safely corrected without data loss:

| Issue | Auto-repair action |
|-------|-------------------|
| Shadow header mismatch | Rewrite shadow from primary header (or vice versa if primary is corrupt) |
| Orphaned pages | Add to free list |
| Corrupted page with WAL coverage | Redo from WAL (if segment still available) |
| Corrupted page without WAL coverage | Mark page as damaged; if it's a secondary index page, drop and rebuild the index; if it's a primary B-tree page, data loss — report and skip |
| B-tree sibling chain broken | Rebuild sibling pointers from a full tree walk |
| Index with `state = Building` | Drop partial index and restart build |

**Repair limitations**: auto-repair cannot recover data that is both corrupted in the page store and no longer covered by WAL segments. For this case, replica reconstruction (2.13.6) is the recovery path.

**Configuration**:

- `check_on_startup: bool` (default: `false`) — run a quick integrity check (phases 1–3 only) on database open. Full check is too expensive for routine startups.
- `check_on_startup_full: bool` (default: `false`) — run the full integrity check on startup. Use after suspected corruption.

#### 2.13.6 Replica Reconstruction

When local recovery is insufficient (e.g., catastrophic disk failure, unrecoverable corruption beyond WAL coverage), a database instance can be **fully reconstructed from a replica**.

**Prerequisite**: at least one replica (or the primary) has a healthy copy of the database.

**Reconstruction protocol**:

1. **Initiate**: the recovering node contacts a healthy source node (primary or replica) and requests a full database snapshot.

2. **Source-side snapshot**:
   a. The source begins a read-only transaction at its current `applied_ts` to get a consistent snapshot.
   b. The source streams the **entire `data.db`** page-by-page to the recovering node. Pages are sent with their checksums.
   c. Concurrently, new WAL records that arrive at the source after the snapshot `applied_ts` are buffered.

3. **Transfer**:
   a. Recovering node writes received pages to a new `data.db`, verifying each checksum on receipt.
   b. After all pages are received: fsync `data.db`.
   c. Source sends all buffered WAL records from `snapshot_ts` onward.
   d. Recovering node writes these to its local WAL directory.

4. **Finalize**:
   a. Recovering node writes `meta.json` with the snapshot's `checkpoint_lsn`.
   b. Recovering node opens the database normally — WAL replay applies any records after the snapshot.
   c. Recovering node connects to the primary for ongoing WAL streaming.

**Incremental catch-up vs full reconstruction**:

| Scenario | Recovery method |
|----------|----------------|
| Replica briefly offline, WAL records available on primary | Incremental: stream missing WAL records (section 6.7) |
| Replica WAL gap too large (primary already reclaimed those segments) | Full reconstruction from snapshot |
| Corrupted `data.db`, local WAL intact | Local repair: re-checkpoint from WAL if enough history; otherwise full reconstruction |
| Total data loss (disk failure) | Full reconstruction |

**Reconstruction of `_system` database**: the system catalog can also be reconstructed from a replica. The recovering node requests both the `_system` snapshot and the list of database directories, then reconstructs each database individually.

**Online reconstruction**: the source node continues serving reads and writes during the snapshot transfer. The consistent snapshot guarantees the recovering node gets a valid point-in-time copy. WAL records generated during the transfer are forwarded afterward.

**Bandwidth optimization**: for large databases, the snapshot transfer can be compressed (LZ4 frame compression on the page stream). Pages that are entirely zeroed (free pages) can be sent as a marker rather than full page data.

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

### 4.1 Access Patterns

- **Primary get**: point lookup by document ID via the primary B-tree.
- **Index scan**: range scan on a secondary index with optional filter and limit.
- **Table scan**: full collection scan with optional filter and limit.

### 4.2 Operators

`eq`, `lt`, `gt`, `gte`, `lte`, `ne`, `AND`, `OR`, `IN`, `NOT`.

### 4.3 Query Tagging

Every read operation within a transaction is assigned an incremental **query ID** (`u32`, starting at 0). This ID is stored in the read set entry and serves two purposes:

- **Subscription granularity**: when a subscription is invalidated, the notification includes the specific query IDs affected, so the client knows which queries to re-execute.
- **Cache keying**: query results can be cached and invalidated at the individual query level.

### 4.4 Constraints

- `limit` supported on all query types.
- Limit interacts with read sets and conflict detection (see 5.7).

---

## 5. Transactions and Concurrency

### 5.1 Transaction Types and Options

Transactions are scoped to a single database and may span multiple collections. They are parameterized with:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `readonly` | bool | false | Read-only transactions cannot write data. No OCC validation at commit. |
| `notify` | bool | false | One-shot: notify the client when any query in the read set is invalidated by a future commit. Fires once, then the subscription is removed. |
| `subscribe` | bool | false | Persistent: on invalidation, report which query IDs were affected and automatically begin a new transaction at the latest committed timestamp, forming a **subscription chain** (see 5.8). |

`notify` and `subscribe` are mutually exclusive.

**Behavior matrix**:

| Scenario | `subscribe: false` | `subscribe: true` |
|----------|--------------------|--------------------|
| Read-only commit | Read set discarded. | Read set registered as subscription. On invalidation: notify with affected query IDs + new `tx_id`. |
| Write commit (success) | Read set discarded. | Read set registered as subscription. Chain continues reactively. |
| Write commit (OCC conflict) | Error returned. Client retries manually. | Error returned + new **write** transaction automatically started at current timestamp for retry. |

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

1. `begin(readonly: true)` → acquire `read_ts` = latest committed timestamp.
2. Execute reads. Each read is assigned an incremental `query_id` (see 4.3). All reads see the consistent snapshot at `read_ts`.
3. `commit()` → if `subscribe` or `notify`: register read set in subscription registry. Release resources.

**Read-write (mutation)**:

1. `begin()` → acquire `begin_ts` = latest committed timestamp.
2. Execute reads (recorded in read set with `query_id`s) and writes (buffered in write set).
3. `commit()`:
   a. Acquire the next `commit_ts` (atomic increment).
   b. Validate the read set against the commit log (see 5.7).
   c. If valid: persist → invalidate → replicate → notify (see 5.11 for full protocol). If `subscribe`: register read set as subscription.
   d. If conflict and `subscribe: true`: start a new write transaction at current timestamp, notify client of conflict + new `tx_id`.
   e. If conflict and `subscribe: false`: return error to client.

**Subscription chain** (when `subscribe: true`):

```
T1 commit (ts=10, queries: [Q0, Q1, Q2])
  → read set registered as subscription
  → future commit at ts=15 invalidates Q1
    → client notified: { invalidated: [1], new_tx: { id: "...", ts: 15 } }
    → T2 begins (ts=15)
      → client re-executes Q1 (or all queries) in T2
      → T2 commit → subscription updated with new read set
      → future commit at ts=22 invalidates Q0, Q2
        → T3 begins (ts=22)
          → ...
```

Each link in the chain:
1. Current transaction commits → read set becomes the subscription's watch predicate.
2. A future commit invalidates part of the read set (detected via conflict rules in 5.7).
3. Subscription manager creates a new transaction at the latest committed timestamp.
4. Client is notified with the list of invalidated `query_id`s and the new transaction's ID/timestamp.
5. Client re-executes the affected queries (or all queries) within the new transaction.
6. New transaction commits → subscription's read set is updated.
7. Repeat.

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
        query_id:      u32              // incremental per transaction (see 4.3)
        collection_id: CollectionId
        doc_id:        DocId
    }

    IndexScan {
        query_id:       u32
        collection_id:  CollectionId
        index_id:       IndexId
        lower_bound:    Option<EncodedKey>
        upper_bound:    Option<EncodedKey>
        filter:         Option<Filter>
        limit:          Option<u64>
        result_doc_ids: Vec<DocId>
    }

    TableScan {
        query_id:       u32
        collection_id:  CollectionId
        filter:         Option<Filter>
        limit:          Option<u64>
        result_doc_ids: Vec<DocId>
    }
```

Every query operation appends an entry with a unique `query_id`. This captures the **query parameters** (what was asked), the **result set** (what was returned), and the **query identity** (which query to re-execute on invalidation). Query parameters define the conflict range; result set enables precise invalidation; query ID enables granular subscription notifications.

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

### 5.8 Transaction Subscriptions

Subscriptions operate at the **transaction level**: a subscription watches the entire read set of a committed transaction, not an individual query.

**Registration**: when a transaction with `subscribe: true` or `notify: true` commits, its full read set (with `query_id`s) is stored in the subscription registry.

**Invalidation**: on every new commit, check all active subscriptions against the committed write set using the conflict detection rules from 5.7. For each affected subscription, collect the `query_id`s of conflicting read set entries.

| Subscription Mode | On Invalidation |
|-------------------|----------------|
| `notify` | Send one-shot notification with affected `query_id`s. Remove subscription. |
| `subscribe` | Send notification with affected `query_id`s + new transaction `(tx_id, ts)`. Subscription persists — updated when the chain transaction commits its new read set. |

**Subscription registry** — indexed for fast invalidation lookup:

```
SubscriptionRegistry {
    // O(1) lookup for point watches
    point_index: HashMap<(CollectionId, DocId), Vec<(SubscriptionId, QueryId)>>

    // Grouped by (collection, index) for range overlap checks
    scan_index:  HashMap<(CollectionId, Option<IndexId>), Vec<(SubscriptionId, QueryId)>>
}
```

**Invalidation walk** — when a commit writes to `(collection, doc_id)`:

1. Check `point_index[(collection, doc_id)]` → collect `(subscription_id, query_id)` pairs.
2. For each scan subscription on this collection: check if the write's `IndexDelta` key falls within the subscription's bounds, or if `doc_id` is in the subscription's `result_doc_ids` → collect affected `(subscription_id, query_id)` pairs.
3. Group by `subscription_id`.
4. For each affected subscription: fire notification with the set of invalidated `query_id`s. For `subscribe` mode: begin a new transaction and include its `tx_id` in the notification.

**Subscription update on chain commit**: when a chain transaction (the new transaction from step 4) commits, its read set replaces the subscription's previous read set. The registry indexes are updated accordingly.

### 5.9 Query Result Caching

The subscription mechanism naturally enables **query result caching**: the server (or client) can cache the full result of a query alongside its read set. On subsequent identical queries, the cached result is returned immediately if no intervening commit has invalidated the read set.

**Cache invalidation** uses the same conflict detection logic as subscriptions (5.8): when a commit's write set overlaps with a cached query's read set, the cached entry is evicted. This provides exact invalidation — no stale reads, no false positives for non-overlapping writes.

**Cache lifecycle**: cached results are associated with the `read_ts` at which they were computed. A cache hit is valid if and only if no commit in `(read_ts, current_ts)` conflicts with the read set. This check is equivalent to the OCC validation in 5.7.

### 5.10 Concurrency Model

- **Async runtime**: all I/O and computation runs on the tokio async runtime.
- **Buffer pool locking**: page-level read/write latches (short-lived, never held across await points).
- **Write serialization**: WAL append + `commit_ts` assignment is a critical section, serialized via a single-writer committer (mutex or channel). This is the serialization point for all writes.
- **Read concurrency**: fully concurrent. Multiple readers at different `read_ts` values traverse B-trees simultaneously without coordination.
- **OCC advantage**: readers never block writers, writers never block readers. Conflicts are detected only at commit time.

### 5.11 Commit Protocol and Ordering Guarantees

The commit protocol enforces a strict ordering of effects. When a client receives commit confirmation, it is guaranteed that:

1. Data is durably persisted.
2. All subscriptions and caches on **all nodes** have been invalidated.
3. All replicas have applied the changes.
4. Any new transaction on any node will see the committed data.

**Commit sequence** (on the primary):

```
1. OCC validation                           [VALIDATE]
2. Assign commit_ts                         [TIMESTAMP]
3. Write WAL record + fsync                 [PERSIST]
4. Apply mutations to page store            [MATERIALIZE]
5. Update commit log                        [LOG]
6. Invalidate local subscriptions/caches    [INVALIDATE]
7. Replicate to all replicas (see 6.2)      [REPLICATE]
   Each replica: apply WAL → update page
   store → invalidate local subscriptions
8. Wait for replica acknowledgements        [SYNC]
9. Advance latest_committed_ts              [VISIBLE]
10. Notify the client                       [RESPOND]
```

**Critical ordering**: `latest_committed_ts` (step 9) is only advanced after all replicas confirm (step 8). This ensures that no new transaction on any node can begin at a timestamp for which some replica hasn't yet applied the data.

**Monotonic visibility**: a query at timestamp T on any node is guaranteed to see all commits with `commit_ts ≤ T`. It is impossible for a newer timestamp to return older data than a query at an earlier timestamp.

**Write-to-read latency**: the window between steps 3 and 9 is a brief period where the data is persisted but not yet visible to new transactions. This is intentional — visibility is deferred until all nodes are consistent. During this window, the committing client has not yet been notified, so no external observer can expect to see the data.

---

## 6. Distributed Architecture

### 6.1 Topology

Single **Primary** with multiple **Read Replicas**. All nodes can accept client connections.

| Role | Writes | Reads | Subscriptions |
|------|--------|-------|---------------|
| Primary | Yes (commits locally) | Yes | Yes (local) |
| Replica | No (promotes to primary) | Yes (local snapshot) | Yes (local, invalidated via WAL stream) |

### 6.2 WAL Streaming

The primary continuously streams committed WAL records to all replicas:

1. Primary commits a transaction (WAL + page store + local invalidation).
2. Primary sends the WAL record to all replicas via persistent TCP connections.
3. Each replica:
   a. Applies the WAL record to its local page store and indexes.
   b. Invalidates local subscriptions/caches affected by the commit.
   c. Sends acknowledgement to primary.
4. Primary waits for acknowledgements (see 6.5), then advances `latest_committed_ts`.

### 6.3 Transaction Execution on Replicas

**Read-only transactions**: execute entirely on the local replica.

- Reads are served from the replica's page store at timestamps the replica has already applied.
- No primary contact needed.
- Subscriptions are registered locally — invalidated when the replica receives WAL records from the primary.

**Write transactions**: execute locally, commit via the primary.

- Reads from the local replica's snapshot at `begin_ts`.
- Writes buffered in the local write set.
- At commit time: **promote** to primary (see 6.4).

### 6.4 Transaction Promotion

When a write transaction on a replica reaches commit:

1. Originating replica sends to primary: `{ begin_ts, read_set, write_set }`.
2. Primary assigns `commit_ts` and validates OCC against its commit log.
3. If valid: primary executes the full commit protocol (5.11 steps 3–9).
4. Primary responds to originating replica: `{ commit_ts }` on success, or `{ conflict }` on OCC failure.
5. Originating replica notifies the client (success or error, following `subscribe` semantics from 5.1).

If OCC fails and `subscribe: true`: the originating replica starts a new write transaction at the current timestamp and notifies the client with the new `tx_id`.

### 6.5 Replication Consistency

**Default: strict synchronous replication** — the primary waits for **all** replicas to acknowledge before advancing the committed timestamp.

**Configurable**: can be relaxed to **Primary + 1** (at least one replica confirms; remaining catch up asynchronously). This trades some consistency for lower write latency.

| Mode | Guarantees | Trade-off |
|------|-----------|-----------|
| Strict (all replicas) | Any read on any node immediately sees committed data. | Write latency = max(replica round-trips). |
| Primary + 1 | Committed data is on at least 2 nodes. Lagging replicas may serve slightly stale reads. | Lower write latency. |

In both modes, the committing client is only notified after the required acknowledgements are received.

### 6.6 Monotonic Reads

Each replica tracks its `applied_ts` — the highest WAL timestamp it has fully applied and made visible locally.

**Guarantee**: a replica never serves a read at a timestamp it hasn't fully applied. This is enforced by:

- Transactions on a replica can only begin at timestamps `≤ applied_ts`.
- If a client requests a read at a timestamp the replica hasn't reached, the replica either waits until caught up or returns an error.

This ensures **monotonic reads**: a query at timestamp T always sees a complete, consistent snapshot of all commits up to T. No "time travel" — a newer timestamp always reflects a superset of an older timestamp's data.

### 6.7 Replica Failure and Recovery

**Recovery tiers** — from cheapest to most expensive, the system attempts recovery in this order:

**Tier 1 — Incremental catch-up** (seconds):

- **Condition**: replica was briefly offline; the primary still has the WAL segments covering the gap.
- **Process**: replica reconnects, sends its `applied_ts`. Primary streams missing WAL records. Replica applies them in order, then resumes normal replication.
- **Availability**: replica is read-only during catch-up (serves reads at its current `applied_ts`). Becomes fully current once catch-up completes.

**Tier 2 — Local crash recovery + catch-up** (seconds to minutes):

- **Condition**: replica process crashed or was killed. Local disk is intact.
- **Process**: on restart, the replica recovers from its local checkpoint + WAL (section 2.10). Then reconnects to the primary for Tier 1 incremental catch-up.
- **Availability**: unavailable during local recovery, then read-only during catch-up.

**Tier 3 — Full reconstruction from snapshot** (minutes to hours, depending on data size):

- **Condition**: local data is unrecoverable — disk failure, corruption beyond WAL/repair coverage, or WAL gap too large (primary has already reclaimed the needed segments).
- **Process**: full database reconstruction from a healthy node (section 2.13.6):
  1. Recovering node requests a consistent snapshot from the primary (or another replica).
  2. Source streams `data.db` page-by-page at a consistent `applied_ts`.
  3. Source then streams WAL records from `applied_ts` onward.
  4. Recovering node writes the snapshot, replays buffered WAL, and connects for ongoing replication.
- **Availability**: unavailable until snapshot transfer + replay completes.

**Tier selection**: the recovering node determines which tier applies:

| Local state | Primary WAL available | Recovery tier |
|-------------|----------------------|---------------|
| Local WAL intact, `applied_ts` known | Gap covered by primary WAL | Tier 1 |
| Local disk intact, needs crash recovery | Gap covered after local recovery | Tier 2 |
| Local disk intact, needs crash recovery | Gap NOT covered (primary reclaimed segments) | Tier 3 |
| Local disk failed / corrupted beyond repair | N/A | Tier 3 |

**New node provisioning**: adding a new replica to the cluster follows the same Tier 3 protocol — it is functionally identical to reconstructing a failed node. The new node has no local state and receives a full snapshot.

**Primary failure**: out of scope for initial design. Future: leader election among replicas.

### 6.8 WAL Retention for Replication

The primary must retain WAL segments long enough for replicas to catch up. Without this, a slow replica forces Tier 3 reconstruction even for brief outages.

**Retention policy**: the primary retains WAL segments beyond the checkpoint horizon if any replica's `applied_ts` falls within those segments.

```
retention_lsn = min(checkpoint_lsn, min(replica.applied_ts for all replicas))
```

WAL segments are only deleted when `segment.max_lsn < retention_lsn`.

**Configurable bounds**:

- `wal_retention_max_size`: maximum total WAL size to retain for replication (default: 1 GB). If exceeded, the oldest segments are reclaimed even if a replica still needs them — that replica will need Tier 3 reconstruction.
- `wal_retention_max_age`: maximum age of retained WAL segments (default: 24 hours). Same forced-reclamation behavior beyond this limit.

These bounds prevent a disconnected replica from causing unbounded WAL growth on the primary.

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
