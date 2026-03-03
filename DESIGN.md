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
- **LSN** (Log Sequence Number): global byte offset in the WAL stream (see 2.8.1) of the last modification to this page. Used during crash recovery to determine which WAL records still need to be replayed.
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

The WAL guarantees durability. Every committed transaction is recorded in the WAL before its effects become visible in the page store. The WAL is the source of truth — all state can be reconstructed by replaying it from the last checkpoint.

#### 2.8.1 Log Sequence Number (LSN)

An **LSN** is a `u64` representing a byte offset in the logical WAL stream.

- LSNs are monotonically increasing and never reused.
- The first record in a new database starts at LSN `0`.
- Every WAL record is uniquely identified by its LSN.
- Given a record at LSN `L` with total size `S` bytes: the next record's LSN is `L + S`.

LSNs appear in:

| Location | Purpose |
|----------|---------|
| Page header `lsn` field (2.3) | LSN of the WAL record that last modified this page |
| `Checkpoint` record and `meta.json` | LSN up to which all data is materialized in the page store |
| Replication protocol (6.2) | Replicas track `applied_lsn` for incremental catch-up |
| Buffer pool dirty tracking (2.7) | Write-ahead guarantee: page flush requires WAL flush up to page LSN |

**Mapping LSN to physical location**: given a set of WAL segments ordered by `base_lsn`, find the segment with the largest `base_lsn ≤ L`. The record is at file offset `32 + (L - base_lsn)` within that segment file.

#### 2.8.2 Segment File Format

WAL data is split into **segment files** in the database's `wal/` directory.

**Naming**: `segment-{N:06}.wal` where `N` is a 1-based sequential number, zero-padded to 6 digits.

**Segment header** (32 bytes, at file offset 0):

```
┌──────────────────────────────────────────┐
│ magic:          u32  (0x57414C00)        │  "WAL\0"
│ version:        u16  (1)                 │  format version
│ _reserved:      u16                      │
│ segment_id:     u32                      │  matches filename number
│ base_lsn:       u64                      │  LSN of first record in this segment
│ created_at_ms:  u64                      │  wall-clock ms since Unix epoch
└──────────────────────────────────────────┘
```

**Physical layout**:

```
[Segment Header: 32 bytes]
[WAL Record 0]
[WAL Record 1]
...
[WAL Record N]
```

Records are packed contiguously after the header with no padding between records.

**LSN ↔ file offset**: a record at LSN `L` in a segment with `base_lsn = B` is at file offset `32 + (L - B)`.

**Pre-allocation**: segment files may be pre-allocated to the target size (~64 MB) on creation to reduce filesystem fragmentation. Unused space at the tail is zero-filled and ignored by readers (a `payload_len` of `0` signals end-of-data).

#### 2.8.3 Record Format

Every WAL record has a fixed 9-byte frame header followed by a variable-length payload:

```
┌──────────────────────────────────────────┐
│ payload_len:  u32 LE                     │  byte length of payload
│ crc32c:       u32 LE                     │  CRC-32C(record_type || payload)
│ record_type:  u8                         │
│ payload:      [u8; payload_len]          │
└──────────────────────────────────────────┘

Total record size = 9 + payload_len
```

- **payload_len**: byte count of `payload` only (excludes the 9-byte frame header and `record_type`).
- **crc32c**: CRC-32C computed over `record_type` (1 byte) concatenated with `payload` (`payload_len` bytes). Hardware-accelerated via SSE 4.2 / ARM CRC instructions.
- **record_type**: identifies the record kind (see 2.8.5).

**LSN assignment**: the LSN of a record is the byte offset of its `payload_len` field in the logical WAL stream. The next record's LSN is `current_lsn + 9 + payload_len`.

**Verification on read**: read `payload_len` and `crc32c`, then read `record_type || payload`, compute CRC-32C, compare. Mismatch → corrupt record; terminate replay at this point (section 2.13.1).

**End-of-data detection**: a `payload_len` of `0` signals end-of-data in the segment (from pre-allocation zero-fill or clean shutdown). Readers stop scanning the current segment and advance to the next if it exists.

#### 2.8.4 Encoding Conventions

All WAL record payloads use a compact binary encoding. Multi-byte integers are **little-endian**.

**Fixed-width types**:

| Notation | Encoding | Size |
|----------|----------|------|
| `u8` | 1 byte | 1 |
| `u16` | 2 bytes LE | 2 |
| `u32` | 4 bytes LE | 4 |
| `u64` | 8 bytes LE | 8 |
| `u128` | 16 bytes LE | 16 |
| `bool` | `u8`: `0x00` = false, `0x01` = true | 1 |

**Variable-length types**:

| Notation | Encoding | Overhead |
|----------|----------|----------|
| `str` | `u16 len` + `[u8; len]` (UTF-8, not null-terminated) | 2 + len |
| `blob` | `u32 len` + `[u8; len]` | 4 + len |
| `key` | `u16 len` + `[u8; len]` (encoded index key) | 2 + len |

**Composite types** used in record payloads:

| Type | Encoding |
|------|----------|
| `FieldPath` | `u8 segment_count` + `segment_count × str` |
| `Option<T>` | `u8 tag` (`0` = None, `1` = Some) + `T` if tag = 1 |
| `Array<T>` | `u32 count` + `count × T` |

#### 2.8.5 Record Types

| Code | Name | Scope | Description |
|------|------|-------|-------------|
| `0x01` | `TxCommit` | per-database | Transaction commit with mutations and index deltas |
| `0x02` | `Checkpoint` | per-database | Marks a successful checkpoint |
| `0x03` | `CreateCollection` | per-database | New collection created |
| `0x04` | `DropCollection` | per-database | Collection dropped |
| `0x05` | `CreateIndex` | per-database | New secondary index created |
| `0x06` | `DropIndex` | per-database | Index dropped |
| `0x07` | `IndexReady` | per-database | Index build completed (`Building` → `Ready`) |
| `0x08` | `Vacuum` | per-database | Old document versions removed |
| `0x10` | `CreateDatabase` | system | New database created (`_system` WAL only) |
| `0x11` | `DropDatabase` | system | Database dropped (`_system` WAL only) |

Types `0x10`–`0x11` are only written to the `_system` database WAL. Types `0x01`–`0x08` are written to per-database WALs. Codes `0x09`–`0x0F` and `0x12`–`0xFF` are reserved for future use.

---

**`TxCommit` (0x01)**

```
tx_id:                u64
commit_ts:            u64
mutation_count:       u32
mutations:            Mutation[mutation_count]
index_delta_count:    u32
index_deltas:         IndexDelta[index_delta_count]
```

**Mutation**:

```
collection_id:  u64
doc_id:         u128    (ULID)
op_type:        u8      (0x01 = Insert, 0x02 = Replace, 0x03 = Delete)
body_len:       u32     (0 for Delete)
body:           [u8; body_len]   (BSON-encoded document; empty for Delete)
```

Patch operations are resolved to full document bodies before writing. The WAL stores the final document state, never the delta. This simplifies replay — each mutation is self-contained.

**IndexDelta**:

```
index_id:       u64
collection_id:  u64
doc_id:         u128
has_old_key:    u8      (0 = no, 1 = yes)
[if has_old_key = 1]:
  old_key_len:  u16
  old_key:      [u8; old_key_len]
has_new_key:    u8      (0 = no, 1 = yes)
[if has_new_key = 1]:
  new_key_len:  u16
  new_key:      [u8; new_key_len]
```

- **Insert**: `has_old_key = 0`, `has_new_key = 1`.
- **Delete**: `has_old_key = 1`, `has_new_key = 0`.
- **Update** (value changed): both present.
- **Update** (indexed value unchanged): both present, keys are identical. Emitted for correctness; can be deduplicated by the reader.

Array-indexed fields produce one `IndexDelta` per array element affected. A single mutation on a document with a 5-element array index produces up to 10 deltas (5 old + 5 new).

Index deltas are computed at commit time from the write set (see 5.5.1) and stored in the WAL to enable:
- **Fast replay**: page store and secondary indexes can be updated directly from the deltas without recomputing keys from document bodies.
- **Replication**: replicas apply index updates without needing to resolve index definitions.
- **Subscription invalidation**: replicas use the encoded keys for interval overlap checks (see 5.8).

---

**`Checkpoint` (0x02)**

```
checkpoint_lsn:  u64
```

Marks that all WAL records with `LSN < checkpoint_lsn` have been fully materialized in the page store and fsynced. WAL segments fully before this LSN become eligible for reclamation (see 2.8.8, 2.9).

---

**`CreateCollection` (0x03)**

```
collection_id:  u64
name_len:       u16
name:           [u8; name_len]   (UTF-8)
```

---

**`DropCollection` (0x04)**

```
collection_id:  u64
```

---

**`CreateIndex` (0x05)**

```
index_id:         u64
collection_id:    u64
name_len:         u16
name:             [u8; name_len]   (UTF-8)
unique:           u8   (0 = non-unique, 1 = unique)
field_count:      u8
field_paths:      FieldPath[field_count]
```

Where each `FieldPath`:

```
segment_count:  u8
segments:       (u16 len + [u8; len])[segment_count]
```

Example: field path `["user", "email"]` → `segment_count = 2`, segments = `[{4, "user"}, {5, "email"}]`.

---

**`DropIndex` (0x06)**

```
index_id:  u64
```

---

**`IndexReady` (0x07)**

```
index_id:  u64
```

Written when background index building (section 3.7) completes successfully. On WAL replay, transitions the index state from `Building` to `Ready` in the catalog.

---

**`Vacuum` (0x08)**

```
collection_id:    u64
entry_count:      u32
entries:          VacuumEntry[entry_count]
```

**VacuumEntry**:

```
doc_id:             u128
removed_ts:         u64     (timestamp of the version being removed)
index_key_count:    u16
index_keys:         VacuumIndexKey[index_key_count]
```

**VacuumIndexKey**:

```
index_id:   u64
key_len:    u16
key:        [u8; key_len]
```

On replay: for each entry, remove the primary B-tree cell keyed by `doc_id || inv_ts(removed_ts)` and all listed secondary index key entries. The operation is idempotent — if a cell or key is already absent, it is a no-op.

---

**`CreateDatabase` (0x10)** — system WAL only

```
database_id:      u64
name_len:         u16
name:             [u8; name_len]   (UTF-8)
path_len:         u16
path:             [u8; path_len]   (UTF-8, relative from data root)
config_len:       u32
config:           [u8; config_len] (BSON-encoded DatabaseConfig)
```

---

**`DropDatabase` (0x11)** — system WAL only

```
database_id:  u64
```

---

#### 2.8.6 Write Protocol and Group Commit

WAL writes are serialized through a **single-writer committer** (see 5.10). This is the serialization point for all writes in the database.

**Single transaction commit**:

1. Serialize the `TxCommit` WAL record into a byte buffer (frame header + payload).
2. Enqueue the buffer into the WAL writer's **write queue** (bounded mpsc channel).
3. Block on a per-transaction **oneshot channel** for the LSN assignment.

**Group commit** — the WAL writer task amortizes fsync cost across concurrent transactions:

1. **Drain**: the WAL writer blocks until at least one record is available, then greedily drains all pending records from the queue.
2. **Write**: all collected records are appended to the current segment file in a single `writev()` or sequential write. Each record's LSN is assigned as the current write position in the logical stream.
3. **Flush**: a single **fsync** covers the entire batch.
4. **Notify**: the writer sends each transaction its assigned LSN via the per-transaction oneshot channel.

**Batching behavior**: the writer drains the queue greedily with no artificial delay. If multiple records are already queued when the writer wakes, they are all flushed together. Under high concurrency, batches naturally form. Under low concurrency, single-record batches are common (one fsync per transaction).

**Throughput**: group commit transforms O(N) fsyncs into O(1) per batch. On NVMe storage with concurrent load, typical batches contain 10–100+ transactions per fsync.

**Non-transaction records** (`CreateCollection`, `CreateIndex`, etc.) follow the same write path. They are enqueued, batched, and fsynced identically to `TxCommit` records.

#### 2.8.7 Segment Rollover

When a segment file exceeds the target size (~64 MB) after completing a record write:

1. The current segment is sealed (no more appends).
2. A new segment file is created: `segment_id` incremented, `base_lsn = previous_base_lsn + (previous_file_size - 32)`.
3. The 32-byte segment header is written and fsynced.
4. Subsequent records are appended to the new segment.

A record is never split across segments. If a record would cross the boundary, it is written entirely to the current segment, which may slightly exceed 64 MB. The rollover check happens after each write (or batch write).

**Active segment**: at most one segment is active (being appended to) at any time. Previous segments are sealed and immutable. The active segment is the only file with an open write file descriptor.

#### 2.8.8 Segment Reclamation

WAL segments are reclaimed after checkpoints (see 2.9) and subject to replication retention (see 6.8).

A segment is reclaimable when:
- Its highest LSN is below the `checkpoint_lsn` (all records materialized in the page store).
- Its highest LSN is below the `retention_lsn` (all replicas have applied the records, or retention bounds are exceeded).

The active segment is never reclaimed. See section 2.13.3 for the full segment lifecycle.

#### 2.8.9 WAL and Replication

WAL records are the unit of replication. The primary streams raw WAL record bytes (9-byte frame header + payload, exactly as stored on disk) to replicas over TCP (see 6.2). This means:

- The WAL record format **is** the replication wire format — no re-serialization or transcoding.
- Replicas verify CRC-32C on received records before applying them.
- The `TxCommit` record's embedded `IndexDelta` entries allow replicas to update secondary indexes directly, without recomputing keys from document bodies or resolving index definitions.
- Replicas write received records to their local WAL via the same write path (2.8.6) before applying to the page store, preserving local durability.
- Replicas track their position as `applied_lsn` — the LSN after the last fully applied record.

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
3. Open WAL, locate the segment containing `checkpoint_lsn` (see 2.8.1), scan forward.
4. For each WAL record (verified by CRC-32C per 2.8.3), replay by type:
   - `TxCommit`: redo mutations — insert document versions into the primary B-tree; apply `IndexDelta` entries to secondary indexes via the buffer pool.
   - `CreateCollection` / `DropCollection`: update the catalog B-tree and in-memory cache.
   - `CreateIndex` / `DropIndex`: update the catalog B-tree and in-memory cache.
   - `IndexReady`: transition index state from `Building` to `Ready` in the catalog.
   - `Vacuum`: remove listed document versions and index keys (idempotent).
   - `Checkpoint`: no-op during replay (informational only).
5. Optionally checkpoint immediately to shrink the recovery window.

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
7. Write `Vacuum` WAL records (2.8.5) for crash recovery. Each record includes the removed document versions and their secondary index keys for idempotent replay.

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

**WAL record types** (system catalog — see section 2.8.5 for full binary layouts):

| Type | Code | Key Payload Fields |
|------|------|--------------------|
| `CreateDatabase` | `0x10` | `database_id`, `name`, `path`, `config` (BSON) |
| `DropDatabase` | `0x11` | `database_id` |

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

**WAL record types** (per-database catalog — see section 2.8.5 for full binary layouts):

| Type | Code | Key Payload Fields |
|------|------|--------------------|
| `CreateCollection` | `0x03` | `collection_id`, `name` |
| `DropCollection` | `0x04` | `collection_id` |
| `CreateIndex` | `0x05` | `index_id`, `collection_id`, `name`, `unique`, `field_paths` |
| `DropIndex` | `0x06` | `index_id` |
| `IndexReady` | `0x07` | `index_id` |

**Lifecycle** (create collection):

1. Assign `collection_id` (monotonic within database).
2. Allocate two new pages: one for the primary B-tree root (empty leaf), one for the `_created_at` index root (empty leaf).
3. Write `CreateCollection` WAL record (2.8.5).
4. Insert collection entry into the catalog B-tree via the buffer pool.
5. Update in-memory cache.

**Lifecycle** (create index):

1. Assign `index_id` (monotonic within database).
2. Allocate a new page for the secondary index B-tree root (empty leaf).
3. Write `CreateIndex` WAL record (2.8.5).
4. Insert index entry into the catalog B-tree with `state = Building`.
5. Update in-memory cache.
6. Begin background index build (see section 3.7).
7. On completion: write `IndexReady` WAL record (2.8.5), update catalog entry to `state = Ready`.

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

**WAL record checksums**: the CRC-32C per WAL record (section 2.8.3) is verified on every read during:

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

### 4.1 Query Pipeline

Every query follows a three-stage pipeline:

```
Source  →  Post-Filter  →  Terminal
```

1. **Source**: produces a stream of documents in index key order. One of:
   - **Primary get**: point lookup by document ID.
   - **Index scan**: range scan on a named index using index range expressions.
   - **Table scan**: full collection scan via the `_created_at` index (equivalent to an index scan with unbounded range).

2. **Post-filter** (optional): arbitrary filter expression evaluated against each document from the source. Documents that don't match are skipped. The post-filter does **not** narrow the index scan interval — it only reduces the result set.

3. **Terminal**: controls how many documents to return.
   - **collect**: return all matching documents.
   - **first**: return the first matching document (equivalent to `limit: 1`).
   - **limit(N)**: return at most N matching documents.

The source determines the read set interval. The post-filter and terminal affect which documents are returned, but the **scanned range** (not the returned documents) defines the conflict surface for OCC and subscriptions.

### 4.2 Query Sources

#### 4.2.1 Primary Get

Point lookup by document ID. Traverses the primary B-tree to `doc_id || inv_ts` and returns the first version visible at `read_ts`.

**Read set**: records a **point interval** on the primary index covering all versions of that document: `[doc_id || 0x00...00, doc_id+1 || 0x00...00)`.

**Cost**: O(log N) — one B-tree traversal.

#### 4.2.2 Index Scan

Range scan on a named secondary index (or a built-in index: `_id`, `_created_at`). The client specifies the index and provides **index range expressions** (section 4.3) that define the scan interval.

**Procedure**:

1. Encode the range expressions into a contiguous byte interval `[lower_bound, upper_bound)` on the encoded index key space (section 3.4).
2. Seek to `lower_bound` in the secondary index B-tree.
3. Scan forward (or backward for `order: "desc"`) along the leaf page chain.
4. For each entry: perform MVCC version resolution (section 3.5). Skip stale entries.
5. For each visible document: if a post-filter is present, fetch the document body from the primary B-tree and evaluate the filter. Skip non-matching documents.
6. Yield matching documents until the terminal condition (limit reached or scan exhausted).
7. Record the **scanned interval** in the read set (see 5.6).

**Read set**: records the byte interval `[lower_bound, upper_bound)` on the specified index. If a limit was applied and exactly N results were returned, the upper bound tightens to the key of the last returned document (see 5.6.3 for details).

**Cost**: O(log N + K) where K is the number of index entries scanned (including those filtered out by post-filter or MVCC resolution).

#### 4.2.3 Table Scan

Full collection scan. Equivalent to an index scan on the `_created_at` index with an unbounded range. Used when the client explicitly scans the entire collection.

**Read set**: records the **full interval** `[MIN, MAX)` on the `_created_at` index. This means any write to the collection will conflict — table scans have the widest conflict surface.

### 4.3 Index Range Expressions

Index range expressions specify a contiguous interval on a compound index's key space. They follow a strict rule: **zero or more equality prefixes in index field order, then optionally one range bound (lower, upper, or both) on the next field**.

For a compound index on fields `[A, B, C]`:

| Expression | Valid | Interval |
|------------|-------|----------|
| `eq(A, 1)` | yes | All entries where A=1 |
| `eq(A, 1), eq(B, "x")` | yes | All entries where A=1 and B="x" |
| `eq(A, 1), eq(B, "x"), gt(C, 100)` | yes | A=1, B="x", C>100 |
| `eq(A, 1), gte(B, "m"), lt(B, "z")` | yes | A=1, "m"≤B<"z" |
| `gt(A, 5)` | yes | A>5 (no equality prefix, range on first field) |
| `gt(B, "m")` | no | Skips A — cannot produce contiguous interval |
| `eq(A, 1), gt(B, "m"), eq(C, 5)` | no | Equality after range — not contiguous |

**Why this constraint**: each valid range expression maps to exactly one contiguous byte interval on the encoded key space. The order-preserving key encoding (section 3.4) means `eq(A, 1)` fixes a prefix, and subsequent bounds narrow within that prefix. Skipping a field or placing equality after a range would create disjoint intervals, breaking the single-interval guarantee.

**Available operators**:

| Operator | Meaning | Position |
|----------|---------|----------|
| `eq(field, value)` | Equality | Zero or more, in index field order |
| `gt(field, value)` | Exclusive lower bound | After all `eq`s, on the next field |
| `gte(field, value)` | Inclusive lower bound | After all `eq`s, on the next field |
| `lt(field, value)` | Exclusive upper bound | After all `eq`s, on the next field |
| `lte(field, value)` | Inclusive upper bound | After all `eq`s, on the next field |

A range field can have both a lower and upper bound: `gte(B, 10), lt(B, 20)`.

**Special case**: no range expressions at all (empty or omitted) scans the entire index — equivalent to a table scan on that index.

**Encoding to byte interval**:

Given index `[A, B]` and range `eq(A, 1), gte(B, "hello")`:

```
lower_bound = encode(type_tag(int64), 1) || encode(type_tag(string), "hello")
upper_bound = encode(type_tag(int64), 1) || successor_prefix
```

Where `successor_prefix` is one byte past the end of the `A=1` prefix, covering all possible B values within A=1. The `doc_id || inv_ts` suffix in every index key is handled by MVCC resolution, not by the range bounds.

### 4.4 Post-Filters

Post-filters are arbitrary filter expressions evaluated against each document after it is read from the source. They support all comparison and logical operators:

**Comparison operators**:

| Operator | Description |
|----------|-------------|
| `eq(field, value)` | Equal |
| `ne(field, value)` | Not equal |
| `gt(field, value)` | Greater than |
| `gte(field, value)` | Greater than or equal |
| `lt(field, value)` | Less than |
| `lte(field, value)` | Less than or equal |
| `in(field, [values])` | Value is in the set |

**Logical operators**:

| Operator | Description |
|----------|-------------|
| `and([filters])` | All must match |
| `or([filters])` | At least one must match |
| `not(filter)` | Negation |

**Interaction with read sets**: post-filters do **not** narrow the read set interval. The full scanned byte range from the source is recorded regardless of how many documents the post-filter rejects. This is by design — a future write that matches the post-filter could enter the scanned range, and the read set must capture this phantom possibility.

**Performance implication**: post-filters cause more documents to be scanned (and more I/O) than equivalent index range expressions. A query with `eq("status", "active")` as a post-filter on a `_created_at` table scan reads every document in the collection. The same predicate as an index range expression on a `[status]` index reads only the "active" entries. The read set is also wider: the table scan conflicts with any write to the collection, while the index scan only conflicts with writes that affect the `status = "active"` key range.

### 4.5 Query Execution

Given a `query` message with `index`, `range`, optional `filter`, `order`, and `limit`:

1. **Resolve index**: look up the named index in the catalog. Verify it exists and is `Ready` (section 3.7). If `Building`, return error `index_not_ready`.
2. **Encode range**: translate index range expressions into a byte interval `[lower_bound, upper_bound)` using the order-preserving key encoding (section 3.4). Validate that predicates follow the index field order rule (section 4.3).
3. **Choose scan direction**: `order: "asc"` scans forward from `lower_bound`; `order: "desc"` scans backward from `upper_bound`.
4. **Execute scan**: traverse the index B-tree (section 4.2.2). For each visible document:
   a. If the source is a secondary index, fetch the document body from the primary B-tree (needed for post-filter evaluation and to return the document).
   b. Evaluate post-filter. Skip if no match.
   c. Add to result set. Check limit.
5. **Record read set**: compute the scanned interval and record it (section 5.6). Apply limit-aware tightening if applicable (section 5.6.3).
6. **Return**: results in index key order, with server-assigned `query_id`.

### 4.6 Query Tagging

Every read operation within a transaction is assigned an incremental **query ID** (`u32`, starting at 0). This ID is stored alongside the read set interval entry and serves two purposes:

- **Subscription granularity**: when a subscription is invalidated, a single notification includes **all** affected query IDs (sorted ascending), so the client knows exactly which queries to re-execute. A commit that overlaps multiple queries in the same subscription produces one notification containing every affected query ID — never multiple separate notifications.
- **Cache keying**: query results can be cached and invalidated at the individual query level.

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
2. Execute reads. Each read is assigned an incremental `query_id` (see 4.6). All reads see the consistent snapshot at `read_ts`.
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

Patch operations are resolved eagerly: the write set always contains the final document body, never a delta. The WAL also stores resolved bodies (see 2.8.5, `TxCommit` Mutation format).

#### 5.5.1 Index Delta Computation

At commit time, the write set is transformed into **index deltas** for each mutation. These deltas are stored in the commit log (section 5.7) and used for conflict detection against concurrent read sets and active subscriptions.

For each `(collection_id, doc_id)` in the write set:

1. **Old document**: if `previous_ts` is set, read the previous document version at that timestamp. For every index on the collection, extract the indexed field values and encode into an index key → `old_key`. For array-indexed fields, one key per array element.
2. **New document**: if the operation is not a delete, extract indexed field values from the new body and encode → `new_key`.
3. **Delta**: for each index, emit an `IndexDelta`:

```
IndexDelta {
    index_id: IndexId
    old_key:  Option<EncodedKey>    // None for inserts (no previous entry)
    new_key:  Option<EncodedKey>    // None for deletes (entry removed)
}
```

When the indexed field value did not change (`old_key == new_key`), the delta is still recorded — it is needed for conflict detection (a concurrent read set interval may cover this key). The delta is omitted only when neither the old nor the new document has a value for the indexed field (both would encode as the `undefined` type tag).

For **array-indexed fields**, a single document may produce multiple old keys and multiple new keys. Each combination is a separate delta entry.

### 5.6 Read Set

The read set records which portions of the index key space a transaction has observed. It is used for OCC validation at commit time (section 5.7) and as the watch predicate for subscription invalidation (section 5.8).

#### 5.6.1 Structure

```
ReadSet {
    intervals: BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>
}

ReadInterval {
    query_id:  u32                  // which query produced this interval (see 4.6)
    lower:     EncodedKey           // inclusive lower bound (byte string)
    upper:     Bound<EncodedKey>    // Excluded(key) or Unbounded
}

Bound<T>:
    Excluded(T)
    Unbounded
```

The read set is a collection of byte-range intervals, grouped by `(collection, index)`. Each interval represents a contiguous range of encoded index keys that was scanned during query execution. The `query_id` links the interval back to the specific query operation for subscription notifications.

**Invariant**: intervals within the same `(collection, index)` group are sorted by `lower` bound. Overlapping or adjacent intervals are merged. This keeps the interval count bounded and makes conflict checking efficient.

#### 5.6.2 How Queries Produce Intervals

Each query type records a specific interval pattern:

**Primary get** (`get` by document ID):

Records a point interval on the **primary index** covering all versions of the document:

```
lower = doc_id || 0x00..00    (16 bytes doc_id + 8 zero bytes)
upper = Excluded(successor(doc_id) || 0x00..00)
```

Where `successor(doc_id)` is the next 16-byte value after `doc_id`. This captures any change to the document — insert, replace, patch, or delete — because all versions of a document share the same `doc_id` prefix in the primary B-tree.

**Index scan** (with range expressions):

Records the byte interval derived from the index range expressions (section 4.3).

For range `eq(A, 1), gte(B, "hello")` on index `[A, B]`:

```
lower = encode(int64, 1) || encode(string, "hello")
upper = Excluded(encode(int64, 1) || successor_prefix)
```

Where `successor_prefix` is one past the end of the `A=1` prefix, covering all B values within A=1.

For range `eq(A, 1), gte(B, "hello"), lt(B, "world")`:

```
lower = encode(int64, 1) || encode(string, "hello")
upper = Excluded(encode(int64, 1) || encode(string, "world"))
```

**Table scan** (unbounded scan on `_created_at` index):

Records the full interval:

```
lower = 0x00  (minimum possible key)
upper = Unbounded
```

This is the widest possible interval — any write to the collection will overlap with it.

#### 5.6.3 Limit-Aware Interval Tightening

When a query has a `limit`, the scanned interval may be tighter than the range expressions suggest:

- **Returned fewer than `limit` results**: the scan exhausted the entire range. The interval is the full range from the range expressions — no tightening.
- **Returned exactly `limit` results**: the scan stopped at the last returned document's index key. The interval tightens:
  - For `order: "asc"`: the `upper` bound tightens to `Excluded(last_key + 1)`. The scan never looked beyond the last result.
  - For `order: "desc"`: the `lower` bound tightens to the key of the last result.

**Example**: index scan on `[status]` with `eq("status", "active"), limit: 10, order: "asc"`.

- The range expression produces interval `[encode("active"), Excluded(successor("active")))`.
- 10 results returned, last one at key `encode("active") || doc_id_10 || inv_ts_10`:
  - Tightened interval: `[encode("active"), Excluded(encode("active") || doc_id_10 || inv_ts_10 + 1))`.
  - A new document inserted into the `active` range **after** `doc_id_10` does NOT conflict — it would appear after the limit cutoff.
  - A new document **before** `doc_id_10` DOES conflict — it could displace a result.

This tightening is critical for high-throughput workloads: a paginated query reading the first 50 results of a large range only conflicts with writes to the first 50 entries' key range, not the entire range.

#### 5.6.4 Post-Filters and Read Set Precision

Post-filters do **not** narrow the read set interval. The full scanned range from the source is recorded, regardless of how many documents the post-filter rejects.

**Why**: a post-filter like `ne("deleted", true)` rejects some documents within the scanned range. But a future write could change a document's `deleted` field from `true` to `false`, causing it to enter the result set. If the read set only covered the returned documents, this phantom would go undetected.

The correct way to narrow the read set is to use index range expressions, not post-filters. For the example above: create an index on `[deleted, ...]` and use `eq("deleted", false)` as a range expression.

#### 5.6.5 Read Set Size Limits

To prevent unbounded read set growth in long-running transactions:

| Limit | Default | Description |
|-------|---------|-------------|
| `max_intervals` | 4,096 | Maximum number of intervals across all indexes |
| `max_scanned_bytes` | 64 MB | Maximum total bytes read from index + primary B-trees |
| `max_scanned_docs` | 100,000 | Maximum documents scanned (including those filtered out) |

Exceeding any limit aborts the transaction with error code `read_limit_exceeded`.

### 5.7 OCC Validation (Conflict Detection)

At commit time, the read set is validated against all transactions that committed in the interval `(begin_ts, commit_ts)`.

**Commit log** — in-memory structure tracking recent commits:

```
CommitLog {
    entries: Vec<CommitLogEntry>    // ordered by commit_ts
}

CommitLogEntry {
    commit_ts:    u64
    index_writes: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>>
}

IndexKeyWrite {
    doc_id:  DocId
    old_key: Option<EncodedKey>    // None for inserts
    new_key: Option<EncodedKey>    // None for deletes
}
```

The commit log indexes writes by `(collection, index)` for direct lookup against the read set. Each entry stores the old and new encoded index keys for every affected index (computed per section 5.5.1).

**Validation algorithm**:

For each `(collection_id, index_id)` group in the read set:

1. Gather all `IndexKeyWrite` entries from commits in `(begin_ts, commit_ts)` for this `(collection, index)`.
2. For each `IndexKeyWrite`:
   - If `old_key` is `Some(k)` and `k` falls within any `ReadInterval` in this group → **conflict**. A document that was in the scan range has been modified or deleted.
   - If `new_key` is `Some(k)` and `k` falls within any `ReadInterval` in this group → **conflict** (phantom). A document has entered the scan range via insert or update.
3. If no key overlaps are found across all groups → validation passes.

**Key overlap check**: a key `k` falls within `ReadInterval { lower, upper }` if:

```
k >= lower AND (upper == Unbounded OR k < excluded_upper)
```

This is a byte comparison on encoded keys (memcmp). Since intervals within a group are sorted and non-overlapping, the check uses binary search — O(log I) per key, where I is the number of intervals in the group.

**Why both old and new keys matter**:

- **`old_key` overlap**: a document that was _inside_ the scan range has been modified or deleted. The transaction may have read it, and the result is now stale.
- **`new_key` overlap**: a document has _entered_ the scan range (phantom). The transaction didn't see it, but if re-executed, the query would return a different result set.

Checking both keys is essential. A document updated from `status="active"` to `status="archived"` produces `old_key` in the `active` range and `new_key` in the `archived` range. Both a reader of "active" documents and a reader of "archived" documents are affected.

**Total validation cost**: O(W × log I) where W is the total number of index key writes across concurrent commits and I is the maximum number of intervals per group. In practice, both are small — a typical transaction has a few dozen intervals and concurrent commits touch a few dozen keys.

**Commit log pruning**: entries with `commit_ts ≤ oldest_active_begin_ts` can be removed — no active transaction will validate against them.

### 5.8 Transaction Subscriptions

Subscriptions operate at the **transaction level**: a subscription watches the entire read set of a committed transaction, not an individual query.

**Registration**: when a transaction with `subscribe: true` or `notify: true` commits, its full read set (the `BTreeMap<(CollectionId, IndexId), Vec<ReadInterval>>` from section 5.6) is stored in the subscription registry.

**Invalidation**: on every new commit, check all active subscriptions against the committed index key writes using the same overlap algorithm as OCC validation (section 5.7). For each affected subscription, collect the `query_id`s of overlapping intervals.

| Subscription Mode | On Invalidation |
|-------------------|----------------|
| `notify` | Send one-shot notification with affected `query_id`s. Remove subscription. |
| `subscribe` | Send notification with affected `query_id`s + new transaction `(tx_id, ts)`. Subscription persists — updated when the chain transaction commits its new read set. |

**Subscription registry** — indexed for fast invalidation lookup:

```
SubscriptionRegistry {
    // Grouped by (collection, index) for range overlap checks
    index: HashMap<(CollectionId, IndexId), Vec<SubscriptionInterval>>
}

SubscriptionInterval {
    subscription_id: SubscriptionId
    query_id:        QueryId
    lower:           EncodedKey
    upper:           Bound<EncodedKey>
}
```

The registry uses the same `(collection, index)` grouping as the read set and commit log. This enables direct key overlap checks without type-switching.

**Invalidation walk** — when a new commit produces `IndexKeyWrite` entries (section 5.7):

1. For each `(collection_id, index_id)` group in the commit's index writes:
   a. Look up `SubscriptionInterval` entries for the same `(collection, index)`.
   b. For each `IndexKeyWrite`: check if `old_key` or `new_key` falls within any subscription interval (same overlap check as OCC — binary search on sorted intervals).
   c. Collect affected `(subscription_id, query_id)` pairs.
2. Group by `subscription_id`. Within each group, deduplicate and sort `query_id`s in ascending order.
3. For each affected subscription: fire **one** notification containing **all** invalidated `query_id`s (ascending). A single commit that overlaps N queries in the same subscription produces exactly one notification with N query IDs — never N separate notifications. For `subscribe` mode: begin a new transaction and include its `tx_id` in the notification.

**Subscription update on chain commit**: when a chain transaction (the new transaction from step 3) commits, its read set replaces the subscription's previous read set in the registry. Old intervals are removed, new intervals are inserted.

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

**Retention policy**: the primary retains WAL segments beyond the checkpoint horizon if any replica's `applied_lsn` (see 2.8.1) falls within those segments. Each replica tracks both `applied_lsn` (WAL byte offset, for replication catch-up and segment retention) and `applied_ts` (committed timestamp, for read visibility — see 6.6).

```
retention_lsn = min(checkpoint_lsn, min(replica.applied_lsn for all replicas))
```

WAL segments are only deleted when `segment.max_lsn < retention_lsn`.

**Configurable bounds**:

- `wal_retention_max_size`: maximum total WAL size to retain for replication (default: 1 GB). If exceeded, the oldest segments are reclaimed even if a replica still needs them — that replica will need Tier 3 reconstruction.
- `wal_retention_max_age`: maximum age of retained WAL segments (default: 24 hours). Same forced-reclamation behavior beyond this limit.

These bounds prevent a disconnected replica from causing unbounded WAL growth on the primary.

---

## 7. API Definition

The API is message-based. Clients connect over a transport, exchange messages with the server, and can pipeline freely — the client never needs to wait for a response before sending the next message.

### 7.1 Transport Layers

All transports carry the same frame format and message semantics. The choice of transport affects only connection establishment and encryption.

| Transport | Encryption | Notes |
|-----------|-----------|-------|
| TCP | None | Development and trusted networks |
| TLS | TLS 1.3 (over TCP) | Production default |
| QUIC | TLS 1.3 (built-in) | Lower latency, connection migration, native multiplexing |
| WebSocket | Optional (ws:// or wss://) | Browser clients, HTTP infrastructure compatibility |

### 7.2 Frame Format

The protocol supports two framing modes, **auto-detected per message** by inspecting the first byte on the wire.

#### 7.2.1 First-Byte Detection (Stream Transports)

On stream transports (TCP, TLS, QUIC), the server reads the first byte of each message:

- **`0x7B`** (`{`): **JSON text mode** — read until `\n` (ignoring `\r` before `\n`), parse the entire line as a JSON object.
- **Any other value**: **Binary frame mode** — interpret as the first byte of a 12-byte binary header, then read `length` bytes of payload.

This works because the binary frame's first byte is a protocol version (starting at `0x01`), which will never be `0x7B` (that would require 123 protocol revisions).

Clients can freely mix modes on the same connection, per-message.

**Server encoding selection**:

- **Responses**: the server mirrors the framing mode and encoding of the request. A JSON text request gets a JSON text response. A binary BSON request gets a binary BSON response.
- **Server-initiated messages** (invalidation): the connection tracks a **current encoding**, initialized to JSON text mode and updated every time the client sends a message. The server uses whatever mode and encoding the client last used. A client that speaks binary BSON gets BSON invalidations; a client that telnets in with JSON gets JSON invalidations.

#### 7.2.2 JSON Text Mode

A single JSON object terminated by `\n`. All message metadata — including message ID and type — are JSON fields:

```json
{"id":1,"type":"authenticate","token":"eyJhbGciOiJIUzI1NiJ9..."}\n
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Client-assigned message ID (incremental, starting at 1) |
| `type` | string | Message type name (see 7.5) |
| ... | | Additional fields per message type |

Maximum line length: **16 MB** (configurable). This is the "zero-dependency" mode — usable with `telnet`, `netcat`, or any language with JSON support and no additional libraries.

#### 7.2.3 Binary Frame Mode

```
┌──────────────────────────────────────┐
│ version:   u8                        │  Protocol version (0x01)
│ flags:     u8                        │  Bit flags
│ encoding:  u8                        │  Payload encoding
│ msg_type:  u8                        │  Message type (see 7.5)
│ msg_id:    u32 LE                    │  Message ID
│ length:    u32 LE                    │  Payload length in bytes
├──────────────────────────────────────┤
│ payload:   [u8; length]              │
└──────────────────────────────────────┘
```

12-byte fixed header + variable-length payload.

**`version`** (u8): protocol version. Starts at `0x01`. The server rejects frames with an unsupported version.

**`flags`** (u8):

| Bit | Meaning |
|-----|---------|
| 0 | Payload is LZ4-compressed |
| 1–7 | Reserved (must be 0) |

**`encoding`** (u8) — how the payload is serialized:

| Value | Encoding | Notes |
|-------|----------|-------|
| 0x01 | JSON | JSON inside binary framing (structured header without BSON/Protobuf dependency) |
| 0x02 | BSON | Native type discrimination (int64 vs float64, binary, datetime). No `_meta.types` needed. |
| 0x03 | Protobuf | Compact, schema-driven. Requires shared `.proto` definitions. |

The `encoding` byte is per-frame. A client using BSON can send an individual message as JSON (encoding `0x01`) without any negotiation. The server responds using the same encoding as the request.

**`msg_type`** (u8): determines the payload schema. Values listed in section 7.5.

**`msg_id`** (u32 LE): client-assigned incremental message ID. Server responses echo this value for correlation.

**`length`** (u32 LE): payload size in bytes. Maximum: **16 MB** (configurable).

#### 7.2.4 WebSocket Adaptation

WebSocket provides its own message framing, so the protocol adapts:

- **Text WebSocket message**: JSON text mode. No newline needed — the WebSocket message boundary serves as the delimiter.
- **Binary WebSocket message**: binary frame (same 12-byte header + payload). The `length` field is present for header uniformity but the WebSocket frame boundary is authoritative.

### 7.3 Connection Lifecycle

#### 7.3.1 Server Hello

Immediately after connection establishment, the server sends a **hello** message in JSON text mode:

```json
{"type":"hello","version":"1.0.0","encodings":["json","bson","protobuf"],"auth_required":true,"node_role":"primary","max_message_size":16777216}\n
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"hello"` |
| `version` | string | Server version (semver) |
| `encodings` | string[] | Supported payload encodings for binary frames |
| `auth_required` | bool | Whether authentication is required before other operations |
| `node_role` | string | `"primary"` or `"replica"` |
| `max_message_size` | integer | Maximum accepted message size in bytes |

The hello is always JSON text mode — even clients using binary frames can parse one JSON line.

#### 7.3.2 Authentication

If `auth_required` is `true`, the client **must** send an `authenticate` message before any other operation. Messages received before successful authentication (other than `authenticate`) are rejected with error code `auth_required`.

See section 7.8 for authentication details.

#### 7.3.3 Graceful Disconnect

Either side may close the connection at any time:

- **TCP/TLS**: close the socket.
- **QUIC**: send a CONNECTION_CLOSE frame.
- **WebSocket**: send a close frame.

On disconnect: open transactions are rolled back, active subscriptions are removed, server-side resources for the connection are freed.

### 7.4 Message Processing Model

#### 7.4.1 Message IDs and Deduplication

Every client → server message carries a message ID (`msg_id` in binary, `"id"` in JSON):

- Client-assigned, incrementing integer, starting at 1, scoped to the connection.
- The server **ignores** messages with a `msg_id` it has already seen on this connection (deduplication for retry safety).

Server → client responses echo the client's message ID for correlation. Server-initiated messages (hello, invalidation) use `msg_id = 0` (binary) or omit the `"id"` field (JSON).

#### 7.4.2 Ordering and Concurrency

The client can send messages without waiting for responses. The server processes them according to their category:

**Connection-level messages** (`authenticate`): processed strictly in order. **Block** processing of all subsequent messages until complete. Incoming messages during processing are queued internally.

**Transaction messages** (`begin`, `commit`, `rollback`, `insert`, `get`, `replace`, `patch`, `delete`, `query`): messages targeting the **same transaction** are processed in the order received. Messages targeting **different transactions** may execute concurrently — no ordering guarantee across transactions.

**Management messages** (`create_database`, `create_collection`, etc.): processed asynchronously. No ordering guarantee relative to other messages.

**Pipelining example**:

```
→ {"id":1,"type":"authenticate","token":"..."}       ← blocks
→ {"id":2,"type":"begin","database":"myapp"}          ← queued until auth completes
→ {"id":3,"type":"insert","tx":1,"collection":"users","body":{"name":"Alice"}}
→ {"id":4,"type":"insert","tx":1,"collection":"users","body":{"name":"Bob"}}
→ {"id":5,"type":"commit","tx":1}
← {"id":1,"type":"ok"}                               ← auth succeeded
← {"id":2,"type":"ok","tx":1}                        ← transaction started
← {"id":3,"type":"ok","doc_id":"01j..."}              ← insert 1 done
← {"id":4,"type":"ok","doc_id":"01j..."}              ← insert 2 done
← {"id":5,"type":"ok","commit_ts":42}                 ← committed
```

Messages 2–5 are queued while authentication completes. After auth succeeds, `begin` executes, then messages 3–5 execute in order (same transaction). If auth fails, messages 2–5 are rejected with `auth_required`.

### 7.5 Message Types

#### 7.5.1 Client → Server

Ranges: `0x01–0x0F` connection, `0x10–0x1F` transaction control, `0x20–0x2F` data operations, `0x30–0x3F` management.

| Byte | JSON `type` | Category | Description |
|------|-------------|----------|-------------|
| `0x01` | `"authenticate"` | Connection | Authenticate with JWT |
| `0x02` | `"ping"` | Connection | Keepalive ping |
| `0x10` | `"begin"` | Transaction | Start a transaction |
| `0x11` | `"commit"` | Transaction | Commit a transaction |
| `0x12` | `"rollback"` | Transaction | Abort a transaction |
| `0x20` | `"insert"` | Data | Insert a new document |
| `0x21` | `"get"` | Data | Get document by ID |
| `0x22` | `"replace"` | Data | Full document replacement |
| `0x23` | `"patch"` | Data | Partial update (RFC 7396 merge-patch) |
| `0x24` | `"delete"` | Data | Delete a document |
| `0x25` | `"query"` | Data | Query with filter |
| `0x30` | `"create_database"` | Management | Create a database |
| `0x31` | `"drop_database"` | Management | Drop a database |
| `0x32` | `"list_databases"` | Management | List all databases |
| `0x33` | `"create_collection"` | Management | Create a collection |
| `0x34` | `"drop_collection"` | Management | Drop a collection |
| `0x35` | `"list_collections"` | Management | List collections in a database |
| `0x36` | `"create_index"` | Management | Create a secondary index |
| `0x37` | `"drop_index"` | Management | Drop a secondary index |
| `0x38` | `"list_indexes"` | Management | List indexes in a collection |

#### 7.5.2 Server → Client

Range: `0x80–0xFF`.

| Byte | JSON `type` | Description |
|------|-------------|-------------|
| `0x80` | `"hello"` | Connection greeting (first message, always JSON text mode) |
| `0x81` | `"ok"` | Success response. Payload varies per request type. |
| `0x82` | `"error"` | Error response. Always contains `code` and `message`. |
| `0x83` | `"invalidation"` | Subscription notification (server-initiated, `msg_id = 0`) |
| `0x84` | `"pong"` | Keepalive response to `ping` |

### 7.6 Message Payloads

All payloads are shown in JSON. Binary encodings (BSON, Protobuf) carry equivalent fields.

#### 7.6.1 Connection Messages

**`authenticate`** — authenticate with the server.

```json
{"id":1, "type":"authenticate", "token":"eyJhbGciOiJIUzI1NiJ9..."}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `token` | string | yes | JWT token |

Response: `ok` on success, `error` with code `auth_failed` on failure.

---

**`ping`** — keepalive. No payload fields.

```json
{"id":2, "type":"ping"}
```

Response: `pong`.

#### 7.6.2 Transaction Control

**`begin`** — start a new transaction.

```json
{"id":3, "type":"begin", "database":"myapp", "readonly":false, "subscribe":false, "notify":false}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `database` | string | yes | | Target database name |
| `readonly` | bool | no | `false` | Read-only transaction (no OCC validation at commit) |
| `subscribe` | bool | no | `false` | Persistent subscription: on invalidation, send affected query IDs + auto-begin new transaction (see 5.8) |
| `notify` | bool | no | `false` | One-shot notification on invalidation. Mutually exclusive with `subscribe`. |

Response: `ok` with `tx` field.

```json
{"id":3, "type":"ok", "tx":1}
```

`tx` is the server-assigned transaction ID (u64), used in all subsequent messages targeting this transaction. Transaction IDs are scoped to the connection.

---

**`commit`** — commit a transaction.

```json
{"id":10, "type":"commit", "tx":1}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID from `begin` response |

Response: `ok` with `commit_ts` on success, `error` with code `conflict` on OCC failure.

```json
{"id":10, "type":"ok", "commit_ts":42}
```

On OCC conflict with `subscribe: true`: the server automatically begins a new write transaction and responds with:

```json
{"id":10, "type":"error", "code":"conflict", "message":"OCC conflict", "new_tx":2, "new_ts":43}
```

---

**`rollback`** — abort a transaction. Discards the write set and read set.

```json
{"id":11, "type":"rollback", "tx":1}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |

Response: `ok`.

#### 7.6.3 Data Operations

All data operations require an active transaction (`tx` field). The collection is specified per-message. The database is implied by the transaction.

**`insert`** — insert a new document. The server generates a ULID.

```json
{"id":4, "type":"insert", "tx":1, "collection":"users", "body":{"name":"Alice","email":"a@b.com"}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |
| `collection` | string | yes | Collection name |
| `body` | object | yes | Document body. May include `_meta` (see 1.12). |

Response: `ok` with generated document ID.

```json
{"id":4, "type":"ok", "doc_id":"01h5kz3x7d8c9v2npqrstuvwxy"}
```

---

**`get`** — retrieve a document by ID.

```json
{"id":5, "type":"get", "tx":1, "collection":"users", "doc_id":"01h5kz3x7d8c9v2npqrstuvwxy"}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |
| `collection` | string | yes | Collection name |
| `doc_id` | string | yes | Document ID (ULID) |

Response: `ok` with document (or `null` if not found). Includes the server-assigned `query_id` for subscription tracking (see 4.6).

```json
{"id":5, "type":"ok", "query_id":0, "doc":{"_id":"01h5kz3x7d...","_created_at":40,"name":"Alice"}}
```

```json
{"id":5, "type":"ok", "query_id":0, "doc":null}
```

---

**`replace`** — full document replacement. `_id` and `_created_at` are preserved.

```json
{"id":6, "type":"replace", "tx":1, "collection":"users", "doc_id":"01h5kz3x7d...", "body":{"name":"Alice Smith","email":"alice@new.com"}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |
| `collection` | string | yes | Collection name |
| `doc_id` | string | yes | Document ID |
| `body` | object | yes | Full new document body |

Response: `ok`.

---

**`patch`** — partial update with shallow merge (RFC 7396). Only top-level fields in `body` are replaced; omitted fields are unchanged. Use `_meta.unset` to remove fields (see 1.12.1).

```json
{"id":7, "type":"patch", "tx":1, "collection":"users", "doc_id":"01h5kz3x7d...", "body":{"email":"new@email.com","_meta":{"unset":["old_field"]}}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |
| `collection` | string | yes | Collection name |
| `doc_id` | string | yes | Document ID |
| `body` | object | yes | Fields to merge. `_meta.unset` for removals. |

Response: `ok`.

---

**`delete`** — delete a document (creates a tombstone version).

```json
{"id":8, "type":"delete", "tx":1, "collection":"users", "doc_id":"01h5kz3x7d..."}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tx` | integer | yes | Transaction ID |
| `collection` | string | yes | Collection name |
| `doc_id` | string | yes | Document ID |

Response: `ok`.

---

**`query`** — query documents using an index scan with optional post-filter.

```json
{"id":9, "type":"query", "tx":1, "collection":"users", "index":"by_status", "range":[{"eq":["status","active"]}], "filter":{"gte":["age",18]}, "order":"asc", "limit":10}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `tx` | integer | yes | | Transaction ID |
| `collection` | string | yes | | Collection name |
| `index` | string | yes | | Index name. Use `"_id"` for primary key order, `"_created_at"` for creation-time order. |
| `range` | array | no | (unbounded) | Index range expressions (see 7.7.1). Array of predicates defining the scan interval. |
| `filter` | object | no | (match all) | Post-filter expression (see 7.7.2). Applied after index scan; does not narrow the read set. |
| `order` | string | no | `"asc"` | `"asc"` or `"desc"`. Scan direction along the index key order. |
| `limit` | integer | no | (no limit) | Maximum number of documents to return |

The `index` field is always required — the client explicitly selects which index to scan (see 4.3). To scan the full collection, use `"index": "_created_at"` with no `range`.

Results are returned in index key order (ascending or descending per `order`). There is no arbitrary sort — to sort by a different field, create an index on that field.

Response: `ok` with array of matching documents and server-assigned `query_id`.

```json
{"id":9, "type":"ok", "query_id":1, "docs":[{"_id":"01h5...","name":"Alice"},{"_id":"01h6...","name":"Bob"}]}
```

**Examples**:

```json
// All active users, newest first, page of 20
{"id":9, "type":"query", "tx":1, "collection":"users",
 "index":"by_status_created_at",
 "range":[{"eq":["status","active"]}],
 "order":"desc", "limit":20}

// Orders in a price range, with post-filter on region
{"id":10, "type":"query", "tx":1, "collection":"orders",
 "index":"by_total",
 "range":[{"gte":["total",100]}, {"lt":["total",500]}],
 "filter":{"eq":["region","eu"]},
 "order":"asc"}

// Full collection scan (all documents by creation time)
{"id":11, "type":"query", "tx":1, "collection":"logs",
 "index":"_created_at", "order":"desc", "limit":100}
```

#### 7.6.4 Database Management

These operations are **not** scoped to a transaction. They execute as atomic operations against the system catalog.

**`create_database`**:

```json
{"id":20, "type":"create_database", "name":"analytics", "config":{"page_size":8192,"memory_budget":268435456}}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Database name (used as directory name) |
| `config` | object | no | `DatabaseConfig` overrides (page_size, memory_budget, max_doc_size, resource limits) |

Response: `ok`. Error `database_exists` if name is taken.

---

**`drop_database`**:

```json
{"id":21, "type":"drop_database", "name":"analytics"}
```

Response: `ok`. Error `unknown_database` if not found.

---

**`list_databases`**:

```json
{"id":22, "type":"list_databases"}
```

Response: `ok` with array of database metadata.

```json
{"id":22, "type":"ok", "databases":[{"name":"myapp","state":"active","created_at":100},{"name":"analytics","state":"active","created_at":200}]}
```

#### 7.6.5 Collection Management

**`create_collection`**:

```json
{"id":30, "type":"create_collection", "database":"myapp", "name":"users"}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `database` | string | yes | Target database |
| `name` | string | yes | Collection name (unique within database) |

Response: `ok`. Error `collection_exists` if name is taken.

---

**`drop_collection`**:

```json
{"id":31, "type":"drop_collection", "database":"myapp", "name":"users"}
```

Response: `ok`. Error `unknown_collection` if not found.

---

**`list_collections`**:

```json
{"id":32, "type":"list_collections", "database":"myapp"}
```

Response: `ok` with array of collection metadata.

```json
{"id":32, "type":"ok", "collections":[{"name":"users","doc_count":1500},{"name":"orders","doc_count":42000}]}
```

#### 7.6.6 Index Management

**`create_index`**:

```json
{"id":40, "type":"create_index", "database":"myapp", "collection":"users", "fields":["email"], "unique":true, "name":"idx_email"}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `database` | string | yes | | Target database |
| `collection` | string | yes | | Target collection |
| `fields` | array | yes | | Field paths to index. Single field: `["email"]`. Compound: `["status", ["address","city"]]`. |
| `unique` | bool | no | `false` | Enforce uniqueness |
| `name` | string | no | auto-generated | Index name |

Response: `ok` with `index_id`. The index is created in `Building` state (see 3.7) and becomes available for queries once background build completes.

```json
{"id":40, "type":"ok", "index_id":5}
```

---

**`drop_index`**:

```json
{"id":41, "type":"drop_index", "database":"myapp", "collection":"users", "name":"idx_email"}
```

Response: `ok`. Error `unknown_index` if not found.

---

**`list_indexes`**:

```json
{"id":42, "type":"list_indexes", "database":"myapp", "collection":"users"}
```

Response: `ok` with array of index metadata.

```json
{"id":42, "type":"ok", "indexes":[{"name":"_id","fields":["_id"],"state":"ready","unique":true},{"name":"_created_at","fields":["_created_at"],"state":"ready","unique":false},{"name":"idx_email","fields":["email"],"state":"building","unique":true}]}
```

#### 7.6.7 Server Responses

All server responses use one of two types:

**`ok`** — success. The payload includes additional fields depending on the request:

| Request | Additional `ok` fields |
|---------|----------------------|
| `authenticate` | (none) |
| `begin` | `tx` |
| `commit` | `commit_ts` |
| `rollback` | (none) |
| `insert` | `doc_id` |
| `get` | `query_id`, `doc` (object or null) |
| `replace`, `patch`, `delete` | (none) |
| `query` | `query_id`, `docs` (array) |
| `create_database` | (none) |
| `drop_database` | (none) |
| `list_databases` | `databases` (array) |
| `create_collection` | (none) |
| `drop_collection` | (none) |
| `list_collections` | `collections` (array) |
| `create_index` | `index_id` |
| `drop_index` | (none) |
| `list_indexes` | `indexes` (array) |

**`error`** — failure:

```json
{"id":5, "type":"error", "code":"unknown_collection", "message":"collection 'users' does not exist in database 'myapp'"}
```

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | Machine-readable error code (see 7.9) |
| `message` | string | Human-readable description |

On OCC conflict with `subscribe: true`, the error includes additional fields:

```json
{"id":10, "type":"error", "code":"conflict", "message":"OCC conflict", "new_tx":2, "new_ts":43}
```

#### 7.6.8 Server Notifications

**`invalidation`** — pushed to the client when a subscription's read set is invalidated by a new commit. Server-initiated (`msg_id = 0`, no `"id"` in JSON).

```json
{"type":"invalidation", "tx":1, "queries":[0,2], "commit_ts":50}
```

| Field | Type | Description |
|-------|------|-------------|
| `tx` | integer | The transaction whose read set was invalidated |
| `queries` | integer[] | All `query_id`s invalidated by this commit, sorted ascending. A single commit that affects multiple queries in the subscription produces one message with every affected ID — never separate messages per query. |
| `commit_ts` | integer | Timestamp of the commit that caused invalidation |

For `subscribe` mode, the notification also includes a new transaction:

```json
{"type":"invalidation", "tx":1, "queries":[0,2], "commit_ts":50, "new_tx":3, "new_ts":50}
```

| Field | Type | Description |
|-------|------|-------------|
| `new_tx` | integer | New transaction ID (already started at `new_ts`) |
| `new_ts` | integer | Timestamp of the new transaction |

The client re-executes the affected queries within the new transaction, then commits to continue the subscription chain (see 5.8).

For `notify` mode, the notification has no `new_tx`/`new_ts` — it is a one-shot notification and the subscription is removed.

### 7.7 Filter Expressions

The query message uses two distinct filter syntaxes: **index range expressions** (the `range` field) and **post-filter expressions** (the `filter` field). Both use JSON objects with field paths in the standard notation (string for top-level, array of strings for nested).

#### 7.7.1 Index Range Expressions

Index range expressions define the scan interval on an index's key space (section 4.3). They are provided as the `range` array in a `query` message. Each element is a single predicate object.

**Available operators**:

```json
{"eq":  ["field_path", value]}
{"gt":  ["field_path", value]}
{"gte": ["field_path", value]}
{"lt":  ["field_path", value]}
{"lte": ["field_path", value]}
```

**Rules** (enforced by the server — violation returns `invalid_range`):

1. Predicates must reference index fields **in order**. For index `[A, B, C]`, the `range` array must address A first, then B, then C.
2. Zero or more `eq` predicates on leading fields (equality prefix).
3. At most one lower bound (`gt` or `gte`) and one upper bound (`lt` or `lte`) on the next field after the equality prefix.
4. No predicates after a range bound — fields after the range field are unconstrained.
5. `ne`, `in`, `and`, `or`, `not` are **not** available as range operators — they cannot produce contiguous intervals. Use them as post-filters instead.

**Examples**:

```json
// Compound index on [status, created_at]
// Equality prefix on status, range on created_at
[{"eq": ["status", "active"]}, {"gte": ["_created_at", 1000]}, {"lt": ["_created_at", 2000]}]

// Single equality — scans all entries where status = "active"
[{"eq": ["status", "active"]}]

// Range on first field — no equality prefix
[{"gt": ["price", 100]}]

// Nested field path
[{"eq": [["address", "country"], "DE"]}]

// Empty array or omitted — unbounded scan (full index)
[]
```

#### 7.7.2 Post-Filter Expressions

Post-filter expressions are arbitrary predicates evaluated against each document after it is read from the index scan. They are provided as the `filter` object in a `query` message. Post-filters support all comparison and logical operators but do **not** narrow the read set interval (section 5.6.4).

**Comparison operators** — compare a field against a value:

```json
{"eq":  ["field_path", value]}
{"ne":  ["field_path", value]}
{"gt":  ["field_path", value]}
{"gte": ["field_path", value]}
{"lt":  ["field_path", value]}
{"lte": ["field_path", value]}
{"in":  ["field_path", [value1, value2, ...]]}
```

Nested field example: `{"eq": [["address","city"], "Berlin"]}`.

**Logical operators** — combine filters:

```json
{"and": [filter, filter, ...]}
{"or":  [filter, filter, ...]}
{"not": filter}
```

**Examples**:

```json
// Simple equality
{"eq": ["status", "active"]}

// Conjunction
{"and": [
  {"gte": ["age", 18]},
  {"ne": ["deleted", true]}
]}

// Disjunction with nesting
{"or": [
  {"eq": ["role", "admin"]},
  {"and": [
    {"eq": ["role", "editor"]},
    {"in": ["department", ["engineering", "design"]]}
  ]}
]}

// Negation
{"not": {"eq": ["archived", true]}}
```

#### 7.7.3 Type Matching

Type matching follows section 1.6: comparisons are type-strict. An `int64` value of `5` does not match a `float64` value of `5.0`. When using JSON text mode, `_meta.types` (section 1.12.2) can disambiguate numeric types in filter values.

This applies to both index range expressions and post-filter expressions.

### 7.8 Authentication

Authentication uses **JWT** (JSON Web Tokens). The server validates tokens using parameters from the server configuration file.

**Authentication flow**:

1. Client connects. Server sends `hello` with `auth_required: true`.
2. Client sends `authenticate` with the JWT token.
3. Server validates: signature, expiration (`exp`), not-before (`nbf`), issuer (`iss`) if configured.
4. On success: `ok`. The connection is authenticated for the lifetime of the connection.
5. On failure: `error` with code `auth_failed`. The client may retry with a different token.

**JWT claims used by the server**:

| Claim | Required | Description |
|-------|----------|-------------|
| `exp` | yes | Expiration time. Connection is terminated when the token expires. |
| `sub` | no | Subject (user/service identifier). Logged for auditing. |
| `iss` | no | Issuer. Validated against configured `jwt_issuer` if set. |
| `databases` | no | Array of database names the token grants access to. If absent, access to all databases. |
| `role` | no | `"admin"` or `"user"`. Admins can create/drop databases. Default: `"user"`. |

### 7.9 Error Codes

| Code | Description |
|------|-------------|
| `auth_required` | Authentication needed but not provided |
| `auth_failed` | Invalid or expired credentials |
| `auth_expired` | Token expired during an active connection |
| `forbidden` | Authenticated but not authorized for this operation |
| `unknown_database` | Database does not exist |
| `unknown_collection` | Collection does not exist |
| `unknown_index` | Index does not exist |
| `unknown_transaction` | Transaction ID not found or already completed |
| `database_exists` | Database name already taken |
| `collection_exists` | Collection name already taken |
| `database_corrupt` | Database is in corrupt state (see 2.13.2) |
| `doc_not_found` | Document ID not found at the transaction's read timestamp |
| `conflict` | OCC validation failed at commit (see 5.7) |
| `readonly_tx` | Write operation attempted on a read-only transaction |
| `readonly_node` | Write operation on a replica (transaction promotion failed) |
| `invalid_message` | Malformed, unparseable, or unknown message type |
| `message_too_large` | Message exceeds `max_message_size` |
| `invalid_filter` | Post-filter expression is syntactically invalid |
| `invalid_range` | Index range expression violates ordering rules (section 4.3) |
| `index_not_ready` | Index is in `Building` state and not yet available for queries |
| `read_limit_exceeded` | Transaction exceeded read set size limits (section 5.6.5) |
| `internal` | Unexpected server error |

### 7.10 Server Configuration

The server reads a JSON configuration file on startup. All fields are optional with sensible defaults.

```json
{
  "listen": {
    "tcp":       "0.0.0.0:5200",
    "tls":       "0.0.0.0:5201",
    "quic":      "0.0.0.0:5201",
    "websocket": "0.0.0.0:5202"
  },
  "tls": {
    "cert_file": "/path/to/cert.pem",
    "key_file":  "/path/to/key.pem"
  },
  "auth": {
    "enabled": true,
    "jwt_algorithm": "HS256",
    "jwt_secret": "base64-encoded-secret",
    "jwt_public_key_file": "/path/to/key.pem",
    "jwt_issuer": "my-auth-server"
  },
  "data_root": "/var/lib/exdb/data",
  "max_message_size": 16777216,
  "default_database_config": {
    "page_size": 8192,
    "memory_budget": 268435456,
    "max_doc_size": 16777216
  },
  "replication": {
    "mode": "strict",
    "wal_retention_max_size": 1073741824,
    "wal_retention_max_age": "24h"
  }
}
```

| Section | Key | Type | Default | Description |
|---------|-----|------|---------|-------------|
| `listen` | `tcp` | string | `"0.0.0.0:5200"` | TCP listen address |
| `listen` | `tls` | string | (disabled) | TLS listen address |
| `listen` | `quic` | string | (disabled) | QUIC listen address (shares TLS config) |
| `listen` | `websocket` | string | (disabled) | WebSocket listen address |
| `tls` | `cert_file` | string | | TLS certificate (PEM). Required if TLS or QUIC enabled. |
| `tls` | `key_file` | string | | TLS private key (PEM). |
| `auth` | `enabled` | bool | `false` | Require authentication |
| `auth` | `jwt_algorithm` | string | `"HS256"` | JWT algorithm: `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `ES256`, `ES384` |
| `auth` | `jwt_secret` | string | | Shared secret for HMAC algorithms (base64-encoded) |
| `auth` | `jwt_public_key_file` | string | | Public key file for RSA/EC algorithms (PEM) |
| `auth` | `jwt_issuer` | string | (no check) | Expected `iss` claim. Reject tokens with different issuer. |
| `data_root` | | string | `"./data"` | Root directory for all database storage |
| `max_message_size` | | integer | `16777216` | Maximum message size in bytes (16 MB) |
| `default_database_config` | | object | | Defaults for new databases (see `DatabaseConfig` in 2.12.1) |
| `replication` | `mode` | string | `"strict"` | `"strict"` (all replicas) or `"primary_plus_one"` |
| `replication` | `wal_retention_max_size` | integer | `1073741824` | Max WAL retention for replication (1 GB) |
| `replication` | `wal_retention_max_age` | string | `"24h"` | Max age of retained WAL segments |

**Symmetric (HMAC) vs asymmetric (RSA/EC) JWT**: for single-server deployments, HMAC (`HS256`) with a shared secret is simplest. For multi-service architectures where an external auth server issues tokens, asymmetric algorithms (`RS256`, `ES256`) allow the database server to verify tokens without knowing the signing key.

---

## 8. Technical Stack and Delivery

- **Language**: Rust.
- **Async Runtime**: tokio.
- **Architecture**: modular design where every component is a discrete module.
- **Deployment**: embedded library instantiated directly within Rust application code.
