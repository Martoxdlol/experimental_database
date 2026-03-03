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
  < number (int64 and float64, compared numerically)
  < boolean (false < true)
  < string (lexicographic, UTF-8 byte order)
  < bytes (lexicographic byte order)
  < array (element-wise comparison)
  < object (not comparable, not indexable)
```

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

- **Source of Truth**: an append-only Transaction Log serves as the authoritative source of truth for all data and state changes.
- **Architecture**: strongly consistent storage using an in-memory cache and the Transaction Log.
- **Performance**: low-latency writes to memory and the log.
- **Buffer Management**: page-based storage system with an LRU cache for frequently accessed pages. Support for datasets exceeding available RAM.
- **Scalability**: optimized to handle a very large quantity of collections.
- **Vacuuming**: background process to reclaim space by purging old document versions from the Transaction Log that are no longer required by any active transaction or snapshot.

---

## 3. Indexing

- **Purpose**: versioned indexes are used to accelerate lookups, avoiding the need to scan the Transaction Log for every query.
- **Structure**: B-Tree indexes for document IDs and arbitrary document fields.
- **Version Awareness**: indexes must store both the field value and the document version/timestamp.
- **Management**: support for creating indexes on existing data via background population (backfilling) without locking the database.
- **Index Vacuuming**: periodic removal of stale entries from indexes corresponding to document versions purged during the Transaction Log vacuuming process.

---

## 4. Query Engine

- **Access Patterns**: index-based lookups and full table scans.
- **Operators**: `eq`, `lt`, `gt`, `gte`, `lte`, `ne`, `AND`, `OR`, `IN`, `NOT`.
- **Constraints**: support for `limit` on all query types.
- **Subscriptions**: live query tracking with automated invalidation and notification when the underlying read set changes via committed writes.

---

## 5. Transactions and Concurrency

- **Isolation Level**: Strict Serializability.
- **Mechanism**: transactions track Read Sets and Write Sets. Optimistic Concurrency Control (OCC) is used for validation at commit time.
- **Read-your-own-writes**: transactions can read data modified within their own write set before committing.
- **Concurrency**: multi-threaded execution using the tokio asynchronous task scheduler.

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
