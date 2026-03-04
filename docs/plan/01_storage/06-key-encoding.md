# 06 — Order-Preserving Key Encoding

Implements DESIGN.md §3.4. Self-delimiting, memcmp-comparable encoding.

## File: `src/storage/key_encoding.rs`

### Type Tags (ascending byte values per §1.6)

```rust
pub mod type_tag {
    pub const UNDEFINED: u8 = 0x00;
    pub const NULL: u8      = 0x01;
    pub const INT64: u8     = 0x02;
    pub const FLOAT64: u8   = 0x03;
    pub const BOOLEAN: u8   = 0x04;
    pub const STRING: u8    = 0x05;
    pub const BYTES: u8     = 0x06;
    pub const ARRAY: u8     = 0x07;
}
```

### Scalar Value for Encoding

```rust
/// A typed scalar value that can be encoded into an index key.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Undefined,
    Null,
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<ScalarValue>),
}
```

### Encoding Functions

```rust
/// Encode a single ScalarValue into the buffer.
/// Self-delimiting: decoder can determine where value ends.
pub fn encode_scalar(buf: &mut Vec<u8>, value: &ScalarValue);

/// Encode a compound key: multiple scalars concatenated.
/// Each scalar is self-delimiting via type_tag + encoding.
pub fn encode_compound_key(values: &[ScalarValue]) -> EncodedKey;

/// Encode a primary key: doc_id[16] || inv_ts[8].
pub fn encode_primary_key(doc_id: DocId, commit_ts: Timestamp) -> EncodedKey;

/// Encode a secondary index key:
/// type_tag₁ || value₁ || ... || doc_id[16] || inv_ts[8].
pub fn encode_secondary_key(
    values: &[ScalarValue],
    doc_id: DocId,
    commit_ts: Timestamp,
) -> EncodedKey;

/// Encode a created_at index key:
/// encode(int64, created_at_ts) || doc_id[16] || inv_ts[8].
pub fn encode_created_at_key(
    created_at: i64,
    doc_id: DocId,
    commit_ts: Timestamp,
) -> EncodedKey;
```

### Per-Type Encoding Details

```rust
/// Encoding implementations (all self-delimiting):

fn encode_undefined(buf: &mut Vec<u8>) {
    buf.push(type_tag::UNDEFINED);
    // tag only, no data
}

fn encode_null(buf: &mut Vec<u8>) {
    buf.push(type_tag::NULL);
    // tag only, no data
}

fn encode_int64(buf: &mut Vec<u8>, v: i64) {
    buf.push(type_tag::INT64);
    // Big-endian with sign bit flipped: XOR high byte with 0x80.
    let be = v.to_be_bytes();
    let mut encoded = be;
    encoded[0] ^= 0x80;
    buf.extend_from_slice(&encoded);
    // Total: 9 bytes (1 tag + 8 value)
}

fn encode_float64(buf: &mut Vec<u8>, v: f64) {
    buf.push(type_tag::FLOAT64);
    // 1. Canonicalize NaN → 0x7FF8000000000000 (quiet NaN, positive).
    let canonical = if v.is_nan() {
        f64::from_bits(0x7FF8000000000000)
    } else {
        v
    };
    // 2. IEEE 754 big-endian.
    let mut bits = canonical.to_bits().to_be_bytes();
    // 3. If positive (sign bit 0): flip sign bit.
    //    If negative (sign bit 1): flip ALL bits.
    if bits[0] & 0x80 == 0 {
        bits[0] ^= 0x80;
    } else {
        for b in &mut bits {
            *b ^= 0xFF;
        }
    }
    buf.extend_from_slice(&bits);
    // Total: 9 bytes
}

fn encode_boolean(buf: &mut Vec<u8>, v: bool) {
    buf.push(type_tag::BOOLEAN);
    buf.push(if v { 0x01 } else { 0x00 });
    // Total: 2 bytes
}

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(type_tag::STRING);
    // UTF-8 bytes with 0x00 escaped as 0x00 0xFF, terminated by 0x00 0x00.
    encode_escaped_bytes(buf, s.as_bytes());
    buf.extend_from_slice(&[0x00, 0x00]); // terminator
}

fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.push(type_tag::BYTES);
    encode_escaped_bytes(buf, b);
    buf.extend_from_slice(&[0x00, 0x00]); // terminator
}

fn encode_array(buf: &mut Vec<u8>, elements: &[ScalarValue]) {
    buf.push(type_tag::ARRAY);
    for elem in elements {
        encode_scalar(buf, elem); // each element is self-delimiting
    }
    buf.extend_from_slice(&[0x00, 0x00]); // terminator
}

/// Escape 0x00 bytes as 0x00 0xFF for string/bytes encoding.
fn encode_escaped_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    for &b in data {
        buf.push(b);
        if b == 0x00 {
            buf.push(0xFF); // escape: 0x00 → 0x00 0xFF
        }
    }
}
```

### Decoding Functions

```rust
/// Decode a single ScalarValue from the cursor, advancing it.
pub fn decode_scalar(cursor: &mut &[u8]) -> Result<ScalarValue, KeyDecodingError>;

/// Decode a primary key: returns (doc_id, commit_ts).
pub fn decode_primary_key(key: &EncodedKey) -> Result<(DocId, Timestamp)>;

/// Decode a secondary key's suffix: extract (doc_id, commit_ts) from the last 24 bytes.
pub fn decode_secondary_key_suffix(key: &EncodedKey) -> Result<(DocId, Timestamp)>;

/// Decode individual value types (mirror of encode).
fn decode_int64(cursor: &mut &[u8]) -> Result<i64>;
fn decode_float64(cursor: &mut &[u8]) -> Result<f64>;
fn decode_boolean(cursor: &mut &[u8]) -> Result<bool>;
fn decode_string(cursor: &mut &[u8]) -> Result<String>;
fn decode_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>>;
fn decode_array(cursor: &mut &[u8]) -> Result<Vec<ScalarValue>>;
fn decode_escaped_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>>;
```

### Key Prefix / Successor Utilities

```rust
/// Compute the "successor prefix" of a key prefix.
/// Used for range scan upper bounds: all keys with the prefix are < successor.
pub fn successor_prefix(prefix: &[u8]) -> Vec<u8>;

/// Compute successor of a full key (key + 1 in byte order).
pub fn successor_key(key: &EncodedKey) -> EncodedKey;

/// Minimum possible key (0x00).
pub fn min_key() -> EncodedKey;

/// Maximum possible key sentinel.
pub fn max_key() -> EncodedKey;
```

### Extracting Field Values from BSON Documents

```rust
/// Extract a scalar value from a BSON document at the given field path.
/// Returns ScalarValue::Undefined if the field is missing.
pub fn extract_field_value(doc: &bson::Document, path: &FieldPath) -> ScalarValue;
```

### Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum KeyDecodingError {
    #[error("unexpected end of key data")]
    UnexpectedEnd,

    #[error("unknown type tag: {0:#04x}")]
    UnknownTypeTag(u8),

    #[error("invalid UTF-8 in string key")]
    InvalidUtf8,

    #[error("unterminated escaped sequence")]
    UnterminatedEscape,
}
```

### Correctness Properties

1. **Order-preserving**: `encode(a) < encode(b)` iff `a < b` per §1.6 type ordering.
2. **Self-delimiting**: decoder can find value boundaries without external length info.
3. **Memcmp-comparable**: `EncodedKey` derives `Ord` via `Vec<u8>` byte comparison.
4. **NaN canonical**: all NaN bit patterns map to single canonical form, sorts last in float64.
5. **Cross-type ordering**: type tags ensure int64 < float64 < boolean < string < bytes.
