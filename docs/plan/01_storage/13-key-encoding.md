# 13 — Key Encoding (`storage/key_encoding.rs`)

Implements the order-preserving key encoding from DESIGN §3.4. Sits alongside the B-tree module but used by the engine for encoding/decoding index keys.

## Type Tags

```rust
pub const TAG_UNDEFINED: u8 = 0x00;
pub const TAG_NULL: u8      = 0x01;
pub const TAG_INT64: u8     = 0x02;
pub const TAG_FLOAT64: u8   = 0x03;
pub const TAG_BOOLEAN: u8   = 0x04;
pub const TAG_STRING: u8    = 0x05;
pub const TAG_BYTES: u8     = 0x06;
pub const TAG_ARRAY: u8     = 0x07;
```

## Encoding Functions

```rust
/// Encode a scalar value into an order-preserving byte sequence.
pub fn encode_scalar(value: &ScalarValue, buf: &mut Vec<u8>) {
    match value {
        ScalarValue::Undefined => buf.push(TAG_UNDEFINED),
        ScalarValue::Null => buf.push(TAG_NULL),
        ScalarValue::Int64(v) => {
            buf.push(TAG_INT64);
            // Big-endian with sign bit flipped
            let mut bytes = v.to_be_bytes();
            bytes[0] ^= 0x80;
            buf.extend_from_slice(&bytes);
        }
        ScalarValue::Float64(v) => {
            buf.push(TAG_FLOAT64);
            // NaN canonicalization: all NaN → 0x7FF8000000000000
            let bits = if v.is_nan() {
                0x7FF8_0000_0000_0000u64
            } else {
                v.to_bits()
            };
            // IEEE 754 encoding:
            // positive: flip sign bit
            // negative: flip all bits
            let encoded = if bits & (1u64 << 63) == 0 {
                bits ^ (1u64 << 63)       // positive: flip sign bit
            } else {
                !bits                      // negative: flip all bits
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
        ScalarValue::Boolean(v) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *v { 0x01 } else { 0x00 });
        }
        ScalarValue::String(s) => {
            buf.push(TAG_STRING);
            encode_bytes_escaped(s.as_bytes(), buf);
        }
        ScalarValue::Bytes(b) => {
            buf.push(TAG_BYTES);
            encode_bytes_escaped(b, buf);
        }
    }
}

/// Escaped byte encoding: 0x00 → 0x00 0xFF, terminated by 0x00 0x00.
fn encode_bytes_escaped(data: &[u8], buf: &mut Vec<u8>) {
    for &byte in data {
        if byte == 0x00 {
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(byte);
        }
    }
    buf.push(0x00);
    buf.push(0x00); // terminator
}

/// Encode a compound key: multiple scalar values concatenated.
pub fn encode_compound_key(values: &[ScalarValue]) -> Vec<u8> {
    let mut buf = Vec::new();
    for value in values {
        encode_scalar(value, &mut buf);
    }
    buf
}

/// Encode a primary B-tree key: doc_id[16] || inv_ts[8].
pub fn encode_primary_key(doc_id: DocId, ts: Timestamp) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(&doc_id.0.to_be_bytes());
    let inv_ts = u64::MAX - ts.0;
    key[16..24].copy_from_slice(&inv_ts.to_be_bytes());
    key
}

/// Encode a secondary index key: encoded_values || doc_id[16] || inv_ts[8].
pub fn encode_secondary_key(
    values: &[ScalarValue],
    doc_id: DocId,
    ts: Timestamp,
) -> Vec<u8> {
    let mut buf = encode_compound_key(values);
    buf.extend_from_slice(&doc_id.0.to_be_bytes());
    let inv_ts = u64::MAX - ts.0;
    buf.extend_from_slice(&inv_ts.to_be_bytes());
    buf
}

/// Extract (doc_id, inv_ts) from the last 24 bytes of any index key.
pub fn extract_key_suffix(key: &[u8]) -> (DocId, InvTs) {
    let len = key.len();
    let doc_id = DocId(u128::from_be_bytes(key[len-24..len-8].try_into().unwrap()));
    let inv_ts = InvTs(u64::from_be_bytes(key[len-8..len].try_into().unwrap()));
    (doc_id, inv_ts)
}
```

## Decoding Functions

```rust
/// Decode a scalar value from the start of an encoded buffer.
/// Returns (value, bytes_consumed).
pub fn decode_scalar(buf: &[u8]) -> Result<(ScalarValue, usize), StorageError> {
    if buf.is_empty() {
        return Err(StorageError::Deserialize("empty buffer".into()));
    }
    match buf[0] {
        TAG_UNDEFINED => Ok((ScalarValue::Undefined, 1)),
        TAG_NULL => Ok((ScalarValue::Null, 1)),
        TAG_INT64 => {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&buf[1..9]);
            bytes[0] ^= 0x80;
            Ok((ScalarValue::Int64(i64::from_be_bytes(bytes)), 9))
        }
        TAG_FLOAT64 => {
            let encoded = u64::from_be_bytes(buf[1..9].try_into().unwrap());
            let bits = if encoded & (1u64 << 63) != 0 {
                encoded ^ (1u64 << 63)  // was positive
            } else {
                !encoded                 // was negative
            };
            Ok((ScalarValue::Float64(f64::from_bits(bits)), 9))
        }
        TAG_BOOLEAN => {
            Ok((ScalarValue::Boolean(buf[1] != 0), 2))
        }
        TAG_STRING => {
            let (bytes, consumed) = decode_bytes_escaped(&buf[1..])?;
            Ok((ScalarValue::String(String::from_utf8(bytes)?), 1 + consumed))
        }
        TAG_BYTES => {
            let (bytes, consumed) = decode_bytes_escaped(&buf[1..])?;
            Ok((ScalarValue::Bytes(bytes), 1 + consumed))
        }
        other => Err(StorageError::Deserialize(format!("unknown type tag: {other:#04x}"))),
    }
}

/// Decode escaped bytes: read until 0x00 0x00 terminator.
fn decode_bytes_escaped(buf: &[u8]) -> Result<(Vec<u8>, usize), StorageError> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == 0x00 {
            if i + 1 < buf.len() && buf[i + 1] == 0x00 {
                return Ok((result, i + 2)); // terminator
            } else if i + 1 < buf.len() && buf[i + 1] == 0xFF {
                result.push(0x00); // escaped zero
                i += 2;
            } else {
                return Err(StorageError::Deserialize("invalid escape".into()));
            }
        } else {
            result.push(buf[i]);
            i += 1;
        }
    }
    Err(StorageError::Deserialize("unterminated string/bytes".into()))
}
```

## Successor Key Helpers

```rust
/// Compute the successor prefix for a given encoded key prefix.
/// Used for upper bounds in range scans: successor_prefix is one byte
/// past the end of all keys sharing the prefix.
pub fn successor_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut result = prefix.to_vec();
    // Increment the last byte, handling overflow
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            return result;
        }
        result.pop(); // carry: remove trailing 0xFF
    }
    // All 0xFF — return a single byte past max
    vec![0xFF; prefix.len() + 1]
}
```
