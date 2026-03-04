# Layer 1: Core Types & Encoding

**Layer purpose:** Foundational types and encoding used by all higher layers. No I/O, no state, no dependencies on other layers. Pure data definitions and transformations.

## Modules

### `types.rs` — Core Identifiers and Value Types

**WHY HERE:** These are domain-agnostic identifiers and value types with no behavior beyond representation.

```rust
/// 128-bit ULID document identifier
pub struct DocId([u8; 16]);

/// Monotonic collection identifier
pub struct CollectionId(u64);

/// Monotonic index identifier
pub struct IndexId(u64);

/// Monotonic database identifier
pub struct DatabaseId(u64);

/// Logical timestamp for MVCC
pub type Ts = u64;

/// Transaction identifier
pub type TxId = u64;

/// Scalar values for index comparisons and filter evaluation
pub enum Scalar {
    Null,
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
    Id(DocId),
}

/// Type ordering tag (section 1.6)
pub enum TypeTag {
    Undefined = 0x00,
    Null      = 0x01,
    Int64     = 0x02,
    Float64   = 0x03,
    Boolean   = 0x04,
    String    = 0x05,
    Bytes     = 0x06,
    Array     = 0x07,
}
```

### `field_path.rs` — Field Path Representation

**WHY HERE:** A pure data structure representing nested document field paths, used throughout query/index/filter layers.

```rust
/// Identifies a (possibly nested) field within a document
pub struct FieldPath {
    segments: Vec<String>,
}

impl FieldPath {
    pub fn new(segments: Vec<String>) -> Self;
    pub fn single(name: &str) -> Self;
    pub fn segments(&self) -> &[String];
}
```

### `filter.rs` — Filter Expression AST

**WHY HERE:** Pure AST representation of filter expressions, used by query engine and subscriptions. No evaluation logic here.

```rust
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

/// Index range expression (section 4.3)
pub enum RangeExpr {
    Eq(FieldPath, Scalar),
    Gt(FieldPath, Scalar),
    Gte(FieldPath, Scalar),
    Lt(FieldPath, Scalar),
    Lte(FieldPath, Scalar),
}
```

### `encoding.rs` — Document Encoding (BSON/JSON)

**WHY HERE:** Serialization/deserialization of documents to/from binary and text formats. Pure encoding with no storage concerns.

```rust
/// Encode a serde_json::Value to BSON bytes
pub fn encode_document(doc: &serde_json::Value) -> Result<Vec<u8>>;

/// Decode BSON bytes to serde_json::Value
pub fn decode_document(bson: &[u8]) -> Result<serde_json::Value>;

/// Apply RFC 7396 merge-patch
pub fn apply_patch(base: &mut serde_json::Value, patch: &serde_json::Value);

/// Apply _meta.unset field removal
pub fn apply_unset(doc: &mut serde_json::Value, unset: &[FieldPath]);

/// Extract a scalar value at a field path
pub fn extract_scalar(doc: &serde_json::Value, path: &FieldPath) -> Option<Scalar>;

/// Extract values at a field path (array-aware: returns multiple for arrays)
pub fn extract_scalars(doc: &serde_json::Value, path: &FieldPath) -> Vec<Scalar>;

/// Strip _meta from document root, return parsed meta
pub fn strip_meta(doc: &mut serde_json::Value) -> Option<MetaInfo>;

/// Parse _meta.types for JSON wire format type hints
pub fn apply_type_hints(doc: &mut serde_json::Value, types: &serde_json::Value);
```

### `ulid.rs` — ULID Generation

**WHY HERE:** Pure ID generation with no dependencies on storage or domain logic.

```rust
/// Generate a new ULID with current timestamp
pub fn generate_ulid() -> DocId;

/// Encode DocId as 26-char Crockford Base32
pub fn encode_ulid(id: &DocId) -> String;

/// Decode Crockford Base32 string to DocId (case-insensitive)
pub fn decode_ulid(s: &str) -> Result<DocId>;
```

## Interfaces Exposed to Higher Layers

| Interface | Used By |
|-----------|---------|
| `DocId`, `CollectionId`, `IndexId`, `Ts`, `TxId` | All layers |
| `Scalar`, `TypeTag` | L3 (key encoding), L4 (filter eval) |
| `FieldPath` | L3, L4, L5, L7 |
| `Filter`, `RangeExpr` | L4 (query planning), L5 (subscriptions), L7 (message parsing) |
| `encode_document`, `decode_document` | L3 (doc store), L7 (wire protocol) |
| `apply_patch`, `extract_scalar` | L3 (doc mutations), L4 (post-filter eval) |
| `generate_ulid`, `encode_ulid`, `decode_ulid` | L3 (insert), L7 (API) |
