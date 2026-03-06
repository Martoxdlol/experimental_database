# D1: Key Encoding

## Purpose

Order-preserving key encoding that converts domain-typed scalars into byte-comparable keys for B-tree storage. Preserves the type ordering from DESIGN.md section 1.6. Also constructs primary and secondary index keys from (doc_id, ts) components.

## Dependencies

- **L1 (`core/types.rs`)**: `Scalar`, `TypeTag`, `DocId`, `Ts`

No L2 dependency. Pure encoding functions.

## Rust Types

```rust
use crate::core::types::{Scalar, TypeTag, DocId, Ts};

/// Encode a scalar value into an order-preserving byte representation.
/// The encoding is self-delimiting: a decoder can determine where one
/// encoded value ends and the next begins.
///
/// Encoding per type (DESIGN.md section 3.4):
///   Undefined  -> [0x00]
///   Null       -> [0x01]
///   Int64      -> [0x02] || big-endian 8 bytes with sign bit flipped
///   Float64    -> [0x03] || canonicalize NaN, then IEEE 754 BE with sign flip
///   Boolean    -> [0x04] || 0x00 (false) or 0x01 (true)
///   String     -> [0x05] || UTF-8 with 0x00->0x00,0xFF escaping, terminated 0x00,0x00
///   Bytes      -> [0x06] || raw bytes with 0x00->0x00,0xFF escaping, terminated 0x00,0x00
///   Id         -> [0x05] || Crockford Base32 with same escaping (sorts as string)
///
/// NOTE: Id encodes with tag 0x05 (same as String). After decode, the Id variant
/// is lost and returns as String. This is intentional for index ordering purposes.
pub fn encode_scalar(scalar: &Scalar) -> Vec<u8>;

/// Decode an order-preserving byte representation back to a scalar.
/// Returns (value, bytes_consumed) so compound keys can decode sequentially.
/// NOTE: tag 0x05 always decodes as Scalar::String (Id is not recoverable).
pub fn decode_scalar(data: &[u8]) -> Result<(Scalar, usize)>;

/// Construct primary index key: doc_id[16] || inv_ts[8]
/// This is the clustered key for the primary B-tree.
pub fn make_primary_key(doc_id: &DocId, ts: Ts) -> [u8; 24];

/// Parse a primary key back into (DocId, Ts) components.
/// ts is the original timestamp (inv_ts is reversed).
pub fn parse_primary_key(key: &[u8]) -> Result<(DocId, Ts)>;

/// Construct secondary index key from typed scalars:
///   type_tag1[1] || value1[var] || ... || doc_id[16] || inv_ts[8]
/// For single-field indexes, `values` has one element.
/// For compound indexes, values are concatenated in field order.
pub fn make_secondary_key(values: &[Scalar], doc_id: &DocId, ts: Ts) -> Vec<u8>;

/// Construct secondary index key from an already-encoded prefix.
/// Appends doc_id[16] || inv_ts[8] to the prefix bytes.
/// Used by IndexBuilder (D6) where the prefix is pre-computed by compute_index_entries.
pub fn make_secondary_key_from_prefix(prefix: &[u8], doc_id: &DocId, ts: Ts) -> Vec<u8>;

/// Parse the suffix of a secondary key to extract doc_id and ts.
/// The suffix is always the last 24 bytes: doc_id[16] || inv_ts[8].
pub fn parse_secondary_key_suffix(key: &[u8]) -> Result<(DocId, Ts)>;

/// Compute inverted timestamp for descending sort within doc_id group.
/// inv_ts = u64::MAX - ts
pub fn inv_ts(ts: Ts) -> u64;

/// Append a 0x00 byte to create a key that is just past the original
/// in byte ordering. Used for prefix-based exclusive upper bounds
/// (e.g., all keys starting with a given prefix).
pub fn prefix_successor(key: &[u8]) -> Vec<u8>;

/// Increment the key to produce the next key at the same length.
/// Increments the last byte; on overflow (0xFF), truncates and increments
/// the previous byte, etc. If all bytes are 0xFF, appends 0x00.
/// Used for point exclusive upper bounds.
pub fn successor_key(key: &[u8]) -> Vec<u8>;

/// Encode the prefix portion of a secondary key (without doc_id/inv_ts suffix).
/// Used by range encoding in L4 to construct scan bounds.
pub fn encode_key_prefix(values: &[Scalar]) -> Vec<u8>;
```

## Implementation Details

### encode_scalar()

Dispatches on `Scalar` variant:

1. **Undefined**: emit `[0x00]`.
2. **Null**: emit `[0x01]`.
3. **Int64(v)**:
   - Emit `[0x02]`.
   - Convert `v` to big-endian 8 bytes.
   - XOR the high byte with `0x80` (flip sign bit).
   - This maps `i64::MIN -> 0x00..00`, `0 -> 0x80..00`, `i64::MAX -> 0xFF..FF`.
4. **Float64(v)**:
   - Emit `[0x03]`.
   - Canonicalize NaN: map all NaN bit patterns to `0x7FF8000000000000`.
   - Convert to big-endian 8 bytes.
   - If positive (sign bit = 0): flip sign bit (XOR high byte with `0x80`).
   - If negative (sign bit = 1): flip ALL bits (XOR all bytes with `0xFF`).
   - Result: negative values sort before positive, NaN sorts last within float64.
5. **Boolean(v)**: emit `[0x04, if v { 0x01 } else { 0x00 }]`.
6. **String(s)**:
   - Emit `[0x05]`.
   - For each byte in UTF-8: if byte == 0x00, emit `0x00, 0xFF`; else emit byte.
   - Emit terminator `0x00, 0x00`.
7. **Bytes(b)**:
   - Emit `[0x06]`.
   - Same escape/terminator as string.
8. **Id(doc_id)**:
   - Emit `[0x05]` (ids sort as strings in Crockford Base32).
   - Encode the 26-char Base32 representation with same escape/terminator.
   - NOTE: decode will return Scalar::String, not Scalar::Id. This is acceptable
     because index comparisons only care about byte ordering, not type identity.

### decode_scalar()

1. Read type tag byte.
2. Dispatch on tag:
   - `0x00`: return `(Scalar::Undefined, 1)`.
   - `0x01`: return `(Scalar::Null, 1)`.
   - `0x02`: read 8 bytes, XOR high byte with `0x80`, convert from BE to i64.
   - `0x03`: read 8 bytes, reverse the sign encoding, convert to f64.
   - `0x04`: read 1 byte, return Boolean.
   - `0x05`: scan until `0x00, 0x00` terminator, unescape `0x00, 0xFF -> 0x00`. Return String.
   - `0x06`: scan until `0x00, 0x00` terminator, unescape. Return Bytes.

### make_primary_key()

```
let mut key = [0u8; 24];
key[0..16].copy_from_slice(doc_id.as_bytes());
key[16..24].copy_from_slice(&inv_ts(ts).to_be_bytes());
key
```

### make_secondary_key()

```
let mut key = encode_key_prefix(values);
key.extend_from_slice(doc_id.as_bytes());
key.extend_from_slice(&inv_ts(ts).to_be_bytes());
key
```

### make_secondary_key_from_prefix()

```
let mut key = Vec::with_capacity(prefix.len() + 24);
key.extend_from_slice(prefix);
key.extend_from_slice(doc_id.as_bytes());
key.extend_from_slice(&inv_ts(ts).to_be_bytes());
key
```

### prefix_successor()

```
let mut result = key.to_vec();
result.push(0x00);
result
```

### successor_key()

Increment the last byte. On overflow (0xFF), truncate and increment the previous byte, etc. If all bytes are 0xFF, append 0x00 (this extends the key).

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Invalid type tag | Unknown tag byte during decode | Return error |
| Truncated data | Not enough bytes for fixed-size type | Return error |
| Missing terminator | String/bytes without 0x00,0x00 | Return error |
| Key too short | Primary key < 24 bytes | Return error |

## Tests

1. **Undefined roundtrip**: encode Undefined, decode, verify identity.
2. **Null roundtrip**: encode Null, decode, verify identity.
3. **Undefined < Null**: encode(Undefined) < encode(Null) via memcmp.
4. **Int64 ordering**: encode several i64 values (MIN, -1, 0, 1, MAX), verify byte ordering matches numeric ordering via memcmp.
5. **Int64 roundtrip**: encode then decode, verify identity.
6. **Float64 ordering**: encode (-inf, -1.0, -0.0, 0.0, 1.0, inf, NaN), verify memcmp order.
7. **Float64 roundtrip**: encode then decode, verify identity (including -0.0 -> 0.0 normalization).
8. **Float64 NaN canonicalization**: encode signaling NaN, quiet NaN, negative NaN -- all produce same bytes.
9. **Boolean ordering**: encode(false) < encode(true) via memcmp.
10. **String ordering**: encode("", "a", "aa", "ab", "b"), verify memcmp order matches lexicographic.
11. **String with null bytes**: encode "a\x00b", decode back, verify roundtrip.
12. **String terminator**: encoded string ends with 0x00,0x00.
13. **Bytes ordering**: same as string tests but with Bytes variant.
14. **Cross-type ordering**: encode one value of each type, verify memcmp order matches type tag order (undefined < null < int64 < float64 < boolean < string < bytes).
15. **Id encodes as string tag**: encode Id, verify first byte is 0x05. Decode returns String variant.
16. **Primary key roundtrip**: make_primary_key then parse_primary_key, verify doc_id and ts.
17. **Primary key ordering**: two keys with same doc_id but different ts -- newer ts (higher) sorts first (due to inv_ts).
18. **Primary key ordering across doc_ids**: different doc_ids sort by doc_id bytes.
19. **Secondary key roundtrip**: make_secondary_key then parse_secondary_key_suffix, verify doc_id and ts.
20. **Secondary key ordering**: keys with same value but different doc_ids sort by doc_id.
21. **Compound secondary key**: encode [Int64(1), String("hello")], verify prefix is concatenated encoded scalars.
22. **make_secondary_key_from_prefix**: verify output matches make_secondary_key for same inputs.
23. **prefix_successor**: prefix_successor([0x01, 0x02]) > [0x01, 0x02] and < [0x01, 0x03] via memcmp.
24. **successor_key**: successor_key([0x01, 0x02]) == [0x01, 0x03].
25. **successor_key all 0xFF**: successor_key([0xFF, 0xFF]) handles overflow.
26. **encode_key_prefix**: verify output matches concatenated encode_scalar calls.
27. **decode_scalar consumed bytes**: verify bytes_consumed is correct for each type, enabling sequential compound key decoding.
