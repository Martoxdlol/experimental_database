# Layer 1: Core Types & Encoding

## Purpose

Provides foundational types, encoding, and ordering used by every other layer. No dependencies on higher layers.

## Modules

### `types.rs` — Core Type Definitions

All identifiers and domain types used across the system.

```rust
// === Identifiers ===
pub struct DocId(pub u128);          // 128-bit ULID
pub struct CollectionId(pub u64);    // Monotonic within a database
pub struct IndexId(pub u64);         // Monotonic within a database
pub struct DatabaseId(pub u64);      // Monotonic (system catalog)
pub struct TxId(pub u64);           // Transaction identifier
pub type Ts = u64;                   // Monotonic logical timestamp
pub type Lsn = u64;                 // WAL byte offset (Log Sequence Number)
pub type PageId = u32;              // Page identifier in data.db
pub type FrameId = u32;             // Buffer pool frame index
pub type QueryId = u32;             // Per-transaction query counter
pub type SubscriptionId = u64;      // Global subscription identifier

// === Field Paths ===
pub struct FieldPath(pub Vec<String>);  // ["user", "email"] for nested

// === Scalar Values (for index keys and filter evaluation) ===
pub enum Scalar {
    Undefined,
    Null,
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
}

// === Filter Expressions (used by query engine + protocol) ===
pub enum Filter {
    Eq(FieldPath, Scalar),
    Ne(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
    In(FieldPath, Vec<Scalar>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

// === Index Range Expressions ===
pub enum RangeExpr {
    Eq(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
}

// === Page Types ===
pub enum PageType {
    BTreeInternal = 0x01,
    BTreeLeaf = 0x02,
    Heap = 0x03,
    Overflow = 0x04,
    Free = 0x05,
    FileHeaderShadow = 0x06,
}

// === Document Body ===
pub type Document = bson::Document;  // BSON document (via bson crate)
```

### `ulid.rs` — ULID Generation

```rust
pub trait UlidGenerator {
    fn generate(&self) -> DocId;
}

// Crockford Base32 encode/decode
pub fn encode_ulid(id: DocId) -> String;          // 26-char lowercase
pub fn decode_ulid(s: &str) -> Result<DocId>;     // case-insensitive
```

### `encoding.rs` — Document Encoding

```rust
// BSON ↔ bytes
pub fn encode_document(doc: &Document) -> Vec<u8>;
pub fn decode_document(bytes: &[u8]) -> Result<Document>;

// JSON wire format with _meta.types support
pub fn json_to_document(json: &serde_json::Value) -> Result<Document>;
pub fn document_to_json(doc: &Document) -> serde_json::Value;

// Field extraction from documents
pub fn extract_field(doc: &Document, path: &FieldPath) -> Option<Scalar>;
pub fn extract_field_value(doc: &Document, path: &FieldPath) -> Option<bson::Bson>;

// Patch application (RFC 7396 merge-patch + _meta.unset)
pub fn apply_patch(base: &Document, patch: &Document) -> Document;
```

## Interfaces Exposed to Higher Layers

| Consumer Layer | Types/Functions Used |
|---|---|
| Layer 2 (Storage) | `DocId`, `CollectionId`, `IndexId`, `Lsn`, `PageId`, `PageType`, `Document`, `Ts` |
| Layer 3 (Indexing) | `Scalar`, `FieldPath`, `extract_field()`, `DocId`, `Ts` |
| Layer 4 (Query) | `Filter`, `RangeExpr`, `FieldPath`, `Scalar`, `QueryId` |
| Layer 5 (Transactions) | `TxId`, `Ts`, `DocId`, `CollectionId`, `IndexId` |
| Layer 6 (Replication) | `Lsn`, `Ts`, `DatabaseId` |
| Layer 7 (Protocol) | `Filter`, `RangeExpr`, `json_to_document()`, `encode_ulid()`/`decode_ulid()` |

## Type Ordering

The `Scalar` enum implements `Ord` following the total ordering from DESIGN.md §1.6:

```
Undefined < Null < Int64 < Float64 < Boolean < String < Bytes
```

Within each type: natural ordering. Int64 and Float64 are **distinct** — no cross-type numeric comparison.
