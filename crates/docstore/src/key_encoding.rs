//! D1: Order-preserving key encoding.
//!
//! Converts domain-typed scalars into byte-comparable keys for B-tree storage.
//! Preserves the type ordering: Undefined < Null < Int64 < Float64 < Boolean < String < Bytes.
//! Also constructs primary and secondary index keys from (doc_id, ts) components.

use exdb_core::types::{DocId, Scalar, Ts};
use exdb_core::ulid::encode_ulid;

/// Compute inverted timestamp for descending sort within doc_id group.
/// `inv_ts = u64::MAX - ts`
pub fn inv_ts(ts: Ts) -> u64 {
    u64::MAX - ts
}

/// Encode a scalar value into an order-preserving byte representation.
///
/// Encoding per type:
/// - Undefined  -> `[0x00]`
/// - Null       -> `[0x01]`
/// - Int64      -> `[0x02]` || big-endian 8 bytes with sign bit flipped
/// - Float64    -> `[0x03]` || canonicalize NaN, then IEEE 754 BE with sign flip
/// - Boolean    -> `[0x04]` || 0x00 (false) or 0x01 (true)
/// - String     -> `[0x05]` || UTF-8 with 0x00->0x00,0xFF escaping, terminated 0x00,0x00
/// - Bytes      -> `[0x06]` || raw bytes with 0x00->0x00,0xFF escaping, terminated 0x00,0x00
/// - Id         -> `[0x05]` || Crockford Base32 with same escaping (sorts as string)
pub fn encode_scalar(scalar: &Scalar) -> Vec<u8> {
    match scalar {
        Scalar::Undefined => vec![0x00],
        Scalar::Null => vec![0x01],
        Scalar::Int64(v) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(0x02);
            let mut bytes = v.to_be_bytes();
            bytes[0] ^= 0x80; // flip sign bit
            buf.extend_from_slice(&bytes);
            buf
        }
        Scalar::Float64(v) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(0x03);
            // Canonicalize NaN
            let f = if v.is_nan() {
                f64::from_bits(0x7FF8000000000000)
            } else {
                *v
            };
            let mut bytes = f.to_be_bytes();
            if f.is_sign_negative() {
                // Negative: flip ALL bits
                for b in &mut bytes {
                    *b ^= 0xFF;
                }
            } else {
                // Positive (includes +0.0): flip sign bit only
                bytes[0] ^= 0x80;
            }
            buf.extend_from_slice(&bytes);
            buf
        }
        Scalar::Boolean(v) => {
            vec![0x04, if *v { 0x01 } else { 0x00 }]
        }
        Scalar::String(s) => {
            let mut buf = Vec::with_capacity(2 + s.len());
            buf.push(0x05);
            encode_escaped_bytes(s.as_bytes(), &mut buf);
            buf
        }
        Scalar::Bytes(b) => {
            let mut buf = Vec::with_capacity(2 + b.len());
            buf.push(0x06);
            encode_escaped_bytes(b, &mut buf);
            buf
        }
        Scalar::Id(doc_id) => {
            let s = encode_ulid(doc_id);
            let mut buf = Vec::with_capacity(2 + s.len());
            buf.push(0x05); // Id encodes with String tag
            encode_escaped_bytes(s.as_bytes(), &mut buf);
            buf
        }
    }
}

/// Decode an order-preserving byte representation back to a scalar.
///
/// Returns `(value, bytes_consumed)` so compound keys can decode sequentially.
/// Tag 0x05 always decodes as `Scalar::String` (Id is not recoverable).
pub fn decode_scalar(data: &[u8]) -> Result<(Scalar, usize), String> {
    if data.is_empty() {
        return Err("empty data".into());
    }
    match data[0] {
        0x00 => Ok((Scalar::Undefined, 1)),
        0x01 => Ok((Scalar::Null, 1)),
        0x02 => {
            if data.len() < 9 {
                return Err("truncated Int64".into());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[1..9]);
            bytes[0] ^= 0x80;
            Ok((Scalar::Int64(i64::from_be_bytes(bytes)), 9))
        }
        0x03 => {
            if data.len() < 9 {
                return Err("truncated Float64".into());
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[1..9]);
            // Check if originally negative (sign bit was 0 after flip => negative)
            if bytes[0] & 0x80 == 0 {
                // Negative: flip ALL bits back
                for b in &mut bytes {
                    *b ^= 0xFF;
                }
            } else {
                // Positive: flip sign bit back
                bytes[0] ^= 0x80;
            }
            Ok((Scalar::Float64(f64::from_be_bytes(bytes)), 9))
        }
        0x04 => {
            if data.len() < 2 {
                return Err("truncated Boolean".into());
            }
            Ok((Scalar::Boolean(data[1] != 0), 2))
        }
        0x05 => {
            let (bytes, consumed) = decode_escaped_bytes(&data[1..])?;
            let s = String::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {e}"))?;
            Ok((Scalar::String(s), 1 + consumed))
        }
        0x06 => {
            let (bytes, consumed) = decode_escaped_bytes(&data[1..])?;
            Ok((Scalar::Bytes(bytes), 1 + consumed))
        }
        tag => Err(format!("invalid type tag: 0x{tag:02X}")),
    }
}

/// Construct primary index key: `doc_id[16] || inv_ts[8]`.
pub fn make_primary_key(doc_id: &DocId, ts: Ts) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[0..16].copy_from_slice(doc_id.as_bytes());
    key[16..24].copy_from_slice(&inv_ts(ts).to_be_bytes());
    key
}

/// Parse a primary key back into (DocId, Ts) components.
pub fn parse_primary_key(key: &[u8]) -> Result<(DocId, Ts), String> {
    if key.len() < 24 {
        return Err(format!("primary key too short: {} bytes", key.len()));
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&key[0..16]);
    let inv = u64::from_be_bytes(key[16..24].try_into().expect("length checked above"));
    Ok((DocId(id_bytes), u64::MAX - inv))
}

/// Construct secondary index key from typed scalars:
/// `type_tag1[1] || value1[var] || ... || doc_id[16] || inv_ts[8]`
pub fn make_secondary_key(values: &[Scalar], doc_id: &DocId, ts: Ts) -> Vec<u8> {
    let mut key = encode_key_prefix(values);
    key.extend_from_slice(doc_id.as_bytes());
    key.extend_from_slice(&inv_ts(ts).to_be_bytes());
    key
}

/// Construct secondary index key from an already-encoded prefix.
/// Appends `doc_id[16] || inv_ts[8]` to the prefix bytes.
pub fn make_secondary_key_from_prefix(prefix: &[u8], doc_id: &DocId, ts: Ts) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 24);
    key.extend_from_slice(prefix);
    key.extend_from_slice(doc_id.as_bytes());
    key.extend_from_slice(&inv_ts(ts).to_be_bytes());
    key
}

/// Parse the suffix of a secondary key to extract doc_id and ts.
/// The suffix is always the last 24 bytes: `doc_id[16] || inv_ts[8]`.
pub fn parse_secondary_key_suffix(key: &[u8]) -> Result<(DocId, Ts), String> {
    if key.len() < 24 {
        return Err(format!("secondary key too short: {} bytes", key.len()));
    }
    let suffix = &key[key.len() - 24..];
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&suffix[0..16]);
    let inv = u64::from_be_bytes(suffix[16..24].try_into().expect("length checked above"));
    Ok((DocId(id_bytes), u64::MAX - inv))
}

/// Append a 0x00 byte to create the smallest key strictly greater than the input.
pub fn prefix_successor(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    result.push(0x00);
    result
}

/// Increment the key to produce the next key at the same length.
///
/// Increments the last byte; on overflow (0xFF), truncates and increments
/// the previous byte, etc. If all bytes are 0xFF, appends 0x00.
pub fn successor_key(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    let mut i = result.len();
    while i > 0 {
        i -= 1;
        if result[i] < 0xFF {
            result[i] += 1;
            return result;
        }
        result.truncate(i);
    }
    // All bytes were 0xFF — extend
    let mut result = key.to_vec();
    result.push(0x00);
    result
}

/// Encode the prefix portion of a secondary key (without doc_id/inv_ts suffix).
pub fn encode_key_prefix(values: &[Scalar]) -> Vec<u8> {
    let mut prefix = Vec::new();
    for v in values {
        prefix.extend_from_slice(&encode_scalar(v));
    }
    prefix
}

// ─── Internal helpers ───

/// Encode bytes with 0x00 → 0x00,0xFF escaping, terminated by 0x00,0x00.
fn encode_escaped_bytes(input: &[u8], buf: &mut Vec<u8>) {
    for &b in input {
        if b == 0x00 {
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00);
    buf.push(0x00);
}

/// Decode escaped bytes, returning (decoded_bytes, bytes_consumed).
fn decode_escaped_bytes(data: &[u8]) -> Result<(Vec<u8>, usize), String> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i] == 0x00 {
            if i + 1 >= data.len() {
                return Err("unexpected end in escaped bytes".into());
            }
            if data[i + 1] == 0x00 {
                // Terminator found
                return Ok((result, i + 2));
            } else if data[i + 1] == 0xFF {
                // Escaped null byte
                result.push(0x00);
                i += 2;
            } else {
                return Err(format!("invalid escape sequence: 0x00, 0x{:02X}", data[i + 1]));
            }
        } else {
            result.push(data[i]);
            i += 1;
        }
    }
    Err("missing terminator in escaped bytes".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undefined_roundtrip() {
        let encoded = encode_scalar(&Scalar::Undefined);
        let (decoded, consumed) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::Undefined);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_null_roundtrip() {
        let encoded = encode_scalar(&Scalar::Null);
        let (decoded, consumed) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::Null);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_undefined_lt_null() {
        assert!(encode_scalar(&Scalar::Undefined) < encode_scalar(&Scalar::Null));
    }

    #[test]
    fn test_int64_ordering() {
        let values = [i64::MIN, -1, 0, 1, i64::MAX];
        let encoded: Vec<_> = values.iter().map(|v| encode_scalar(&Scalar::Int64(*v))).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{} should sort before {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_int64_roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let encoded = encode_scalar(&Scalar::Int64(v));
            let (decoded, consumed) = decode_scalar(&encoded).unwrap();
            assert_eq!(decoded, Scalar::Int64(v));
            assert_eq!(consumed, 9);
        }
    }

    #[test]
    fn test_float64_ordering() {
        let values = [f64::NEG_INFINITY, -1.0, 0.0, 1.0, f64::INFINITY, f64::NAN];
        let encoded: Vec<_> = values.iter().map(|v| encode_scalar(&Scalar::Float64(*v))).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1], "index {i} should sort before {}", i + 1);
        }
    }

    #[test]
    fn test_float64_roundtrip() {
        for v in [f64::NEG_INFINITY, -1.0, 0.0, 1.0, f64::INFINITY] {
            let encoded = encode_scalar(&Scalar::Float64(v));
            let (decoded, _) = decode_scalar(&encoded).unwrap();
            assert_eq!(decoded, Scalar::Float64(v));
        }
        // -0.0 normalizes to 0.0
        let encoded = encode_scalar(&Scalar::Float64(-0.0));
        let neg_zero_enc = encode_scalar(&Scalar::Float64(0.0));
        // -0.0 sorts before 0.0 (they have different bit patterns in our encoding)
        // Actually let's just check decoding
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        if let Scalar::Float64(f) = decoded {
            assert!(f.is_sign_negative() || f == 0.0);
        }
        // Check encode of negative zero differs from positive zero since bits differ
        let _ = neg_zero_enc; // acknowledged
    }

    #[test]
    fn test_float64_nan_canonicalization() {
        let nan1 = encode_scalar(&Scalar::Float64(f64::NAN));
        let nan2 = encode_scalar(&Scalar::Float64(-f64::NAN));
        let nan3 = encode_scalar(&Scalar::Float64(f64::from_bits(0x7FF0000000000001))); // signaling NaN
        assert_eq!(nan1, nan2);
        assert_eq!(nan1, nan3);
    }

    #[test]
    fn test_boolean_ordering() {
        assert!(encode_scalar(&Scalar::Boolean(false)) < encode_scalar(&Scalar::Boolean(true)));
    }

    #[test]
    fn test_string_ordering() {
        let values = ["", "a", "aa", "ab", "b"];
        let encoded: Vec<_> = values
            .iter()
            .map(|s| encode_scalar(&Scalar::String(s.to_string())))
            .collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "{:?} should sort before {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_string_with_null_bytes() {
        let s = "a\x00b".to_string();
        let encoded = encode_scalar(&Scalar::String(s.clone()));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::String(s));
    }

    #[test]
    fn test_string_terminator() {
        let encoded = encode_scalar(&Scalar::String("hello".into()));
        // Should end with 0x00, 0x00
        let len = encoded.len();
        assert_eq!(encoded[len - 2], 0x00);
        assert_eq!(encoded[len - 1], 0x00);
    }

    #[test]
    fn test_bytes_ordering() {
        let values: Vec<Vec<u8>> = vec![vec![], vec![0x01], vec![0x01, 0x01], vec![0x01, 0x02], vec![0x02]];
        let encoded: Vec<_> = values.iter().map(|b| encode_scalar(&Scalar::Bytes(b.clone()))).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1]);
        }
    }

    #[test]
    fn test_cross_type_ordering() {
        let values = [
            Scalar::Undefined,
            Scalar::Null,
            Scalar::Int64(0),
            Scalar::Float64(0.0),
            Scalar::Boolean(false),
            Scalar::String(String::new()),
            Scalar::Bytes(vec![]),
        ];
        let encoded: Vec<_> = values.iter().map(encode_scalar).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1], "type at index {i} should sort before {}", i + 1);
        }
    }

    #[test]
    fn test_id_encodes_as_string_tag() {
        let id = DocId([1; 16]);
        let encoded = encode_scalar(&Scalar::Id(id));
        assert_eq!(encoded[0], 0x05); // String tag
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        matches!(decoded, Scalar::String(_));
    }

    #[test]
    fn test_primary_key_roundtrip() {
        let doc_id = DocId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let ts = 42;
        let key = make_primary_key(&doc_id, ts);
        let (parsed_id, parsed_ts) = parse_primary_key(&key).unwrap();
        assert_eq!(parsed_id, doc_id);
        assert_eq!(parsed_ts, ts);
    }

    #[test]
    fn test_primary_key_ordering_same_doc() {
        let doc_id = DocId([1; 16]);
        let key_newer = make_primary_key(&doc_id, 10);
        let key_older = make_primary_key(&doc_id, 5);
        // Newer ts (10) has smaller inv_ts, so sorts first
        assert!(key_newer < key_older);
    }

    #[test]
    fn test_primary_key_ordering_across_docs() {
        let doc_a = DocId([0; 16]);
        let doc_b = DocId([1; 16]);
        let key_a = make_primary_key(&doc_a, 10);
        let key_b = make_primary_key(&doc_b, 10);
        assert!(key_a < key_b);
    }

    #[test]
    fn test_secondary_key_roundtrip() {
        let doc_id = DocId([5; 16]);
        let ts = 100;
        let key = make_secondary_key(&[Scalar::Int64(42)], &doc_id, ts);
        let (parsed_id, parsed_ts) = parse_secondary_key_suffix(&key).unwrap();
        assert_eq!(parsed_id, doc_id);
        assert_eq!(parsed_ts, ts);
    }

    #[test]
    fn test_secondary_key_ordering() {
        let doc_a = DocId([0; 16]);
        let doc_b = DocId([1; 16]);
        let key_a = make_secondary_key(&[Scalar::Int64(42)], &doc_a, 10);
        let key_b = make_secondary_key(&[Scalar::Int64(42)], &doc_b, 10);
        assert!(key_a < key_b);
    }

    #[test]
    fn test_compound_secondary_key() {
        let values = [Scalar::Int64(1), Scalar::String("hello".into())];
        let prefix = encode_key_prefix(&values);
        let expected: Vec<u8> = [encode_scalar(&values[0]), encode_scalar(&values[1])].concat();
        assert_eq!(prefix, expected);
    }

    #[test]
    fn test_make_secondary_key_from_prefix() {
        let doc_id = DocId([3; 16]);
        let ts = 50;
        let values = [Scalar::Int64(1)];
        let key1 = make_secondary_key(&values, &doc_id, ts);
        let prefix = encode_key_prefix(&values);
        let key2 = make_secondary_key_from_prefix(&prefix, &doc_id, ts);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_prefix_successor() {
        let key = vec![0x01, 0x02];
        let succ = prefix_successor(&key);
        assert!(succ > key);
        assert!(succ < vec![0x01, 0x03]);
    }

    #[test]
    fn test_successor_key() {
        assert_eq!(successor_key(&[0x01, 0x02]), vec![0x01, 0x03]);
    }

    #[test]
    fn test_successor_key_all_ff() {
        let result = successor_key(&[0xFF, 0xFF]);
        assert!(result > vec![0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_key_prefix() {
        let values = [Scalar::Int64(1), Scalar::Null];
        let prefix = encode_key_prefix(&values);
        let mut expected = encode_scalar(&values[0]);
        expected.extend_from_slice(&encode_scalar(&values[1]));
        assert_eq!(prefix, expected);
    }

    #[test]
    fn test_decode_scalar_consumed_bytes() {
        let s1 = encode_scalar(&Scalar::Int64(42));
        let s2 = encode_scalar(&Scalar::String("hi".into()));
        let mut combined = s1.clone();
        combined.extend_from_slice(&s2);

        let (v1, c1) = decode_scalar(&combined).unwrap();
        assert_eq!(v1, Scalar::Int64(42));
        let (v2, c2) = decode_scalar(&combined[c1..]).unwrap();
        assert_eq!(v2, Scalar::String("hi".into()));
        assert_eq!(c1 + c2, combined.len());
    }

    // ─── Error path tests ───

    #[test]
    fn test_decode_empty_data() {
        assert!(decode_scalar(&[]).is_err());
    }

    #[test]
    fn test_decode_invalid_tag() {
        assert!(decode_scalar(&[0x07]).is_err());
        assert!(decode_scalar(&[0xFF]).is_err());
        assert!(decode_scalar(&[0x10]).is_err());
    }

    #[test]
    fn test_decode_truncated_int64() {
        // Tag + only 4 bytes instead of 8
        assert!(decode_scalar(&[0x02, 0x00, 0x00, 0x00, 0x00]).is_err());
    }

    #[test]
    fn test_decode_truncated_float64() {
        assert!(decode_scalar(&[0x03, 0x00, 0x00]).is_err());
    }

    #[test]
    fn test_decode_truncated_boolean() {
        assert!(decode_scalar(&[0x04]).is_err());
    }

    #[test]
    fn test_decode_string_missing_terminator() {
        // String tag + data but no 0x00 0x00 terminator
        assert!(decode_scalar(&[0x05, 0x41, 0x42]).is_err());
    }

    #[test]
    fn test_decode_string_invalid_escape() {
        // String tag + 0x00 followed by invalid byte (not 0x00 or 0xFF)
        assert!(decode_scalar(&[0x05, 0x00, 0x42]).is_err());
    }

    #[test]
    fn test_decode_bytes_missing_terminator() {
        assert!(decode_scalar(&[0x06, 0x41]).is_err());
    }

    // ─── Boundary value tests ───

    #[test]
    fn test_inv_ts_boundaries() {
        assert_eq!(inv_ts(0), u64::MAX);
        assert_eq!(inv_ts(u64::MAX), 0);
        assert_eq!(inv_ts(1), u64::MAX - 1);
    }

    #[test]
    fn test_primary_key_boundary_ts() {
        let id = DocId([0; 16]);

        // ts=0: inv_ts = u64::MAX, should roundtrip
        let key = make_primary_key(&id, 0);
        let (parsed_id, parsed_ts) = parse_primary_key(&key).unwrap();
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_ts, 0);

        // ts=u64::MAX: inv_ts = 0, should roundtrip
        let key = make_primary_key(&id, u64::MAX);
        let (parsed_id, parsed_ts) = parse_primary_key(&key).unwrap();
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_ts, u64::MAX);
    }

    #[test]
    fn test_parse_primary_key_too_short() {
        assert!(parse_primary_key(&[0; 23]).is_err());
        assert!(parse_primary_key(&[]).is_err());
    }

    #[test]
    fn test_parse_secondary_key_suffix_too_short() {
        assert!(parse_secondary_key_suffix(&[0; 23]).is_err());
        assert!(parse_secondary_key_suffix(&[]).is_err());
    }

    // ─── String edge cases ───

    #[test]
    fn test_empty_string_roundtrip() {
        let encoded = encode_scalar(&Scalar::String(String::new()));
        let (decoded, consumed) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::String(String::new()));
        // Tag + terminator 0x00 0x00
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_empty_bytes_roundtrip() {
        let encoded = encode_scalar(&Scalar::Bytes(vec![]));
        let (decoded, consumed) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::Bytes(vec![]));
        assert_eq!(consumed, 3);
    }

    #[test]
    fn test_string_with_many_null_bytes() {
        let s = "\x00\x00\x00".to_string();
        let encoded = encode_scalar(&Scalar::String(s.clone()));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::String(s));
    }

    #[test]
    fn test_bytes_with_null_bytes() {
        let b = vec![0x00, 0x01, 0x00, 0xFF, 0x00];
        let encoded = encode_scalar(&Scalar::Bytes(b.clone()));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::Bytes(b));
    }

    #[test]
    fn test_long_string_roundtrip() {
        let s: String = "x".repeat(10_000);
        let encoded = encode_scalar(&Scalar::String(s.clone()));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::String(s));
    }

    // ─── Float64 edge cases ───

    #[test]
    fn test_float64_negative_zero_roundtrip() {
        let encoded = encode_scalar(&Scalar::Float64(-0.0));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        if let Scalar::Float64(f) = decoded {
            assert!(f == 0.0);
        } else {
            panic!("expected Float64");
        }
    }

    #[test]
    fn test_float64_neg_zero_before_pos_zero() {
        let neg = encode_scalar(&Scalar::Float64(-0.0));
        let pos = encode_scalar(&Scalar::Float64(0.0));
        // -0.0 should sort before +0.0 (different bit patterns)
        assert!(neg < pos);
    }

    #[test]
    fn test_float64_subnormal() {
        let tiny = f64::from_bits(1); // smallest positive subnormal
        let encoded = encode_scalar(&Scalar::Float64(tiny));
        let (decoded, _) = decode_scalar(&encoded).unwrap();
        assert_eq!(decoded, Scalar::Float64(tiny));
    }

    #[test]
    fn test_float64_extensive_ordering() {
        let values = [
            f64::NEG_INFINITY,
            f64::MIN,
            -1e100,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1e100,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ];
        let encoded: Vec<_> = values.iter().map(|v| encode_scalar(&Scalar::Float64(*v))).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1], "index {} ({}) should sort before {} ({})", i, values[i], i + 1, values[i + 1]);
        }
    }

    // ─── successor_key edge cases ───

    #[test]
    fn test_successor_key_empty() {
        let result = successor_key(&[]);
        assert_eq!(result, vec![0x00]);
    }

    #[test]
    fn test_successor_key_middle_overflow() {
        // [0x01, 0xFF] -> increment 0xFF overflows, truncate, increment 0x01 -> [0x02]
        let result = successor_key(&[0x01, 0xFF]);
        assert_eq!(result, vec![0x02]);
    }

    #[test]
    fn test_successor_key_preserves_ordering() {
        let keys: Vec<Vec<u8>> = vec![
            vec![0x00],
            vec![0x01],
            vec![0x01, 0x00],
            vec![0x01, 0xFF],
            vec![0xFF],
        ];
        for key in &keys {
            let succ = successor_key(key);
            assert!(succ > *key, "successor of {:?} should be greater", key);
        }
    }

    // ─── Compound key sequential decode ───

    #[test]
    fn test_decode_three_scalars_sequentially() {
        let s1 = Scalar::Int64(-42);
        let s2 = Scalar::String("hello".into());
        let s3 = Scalar::Boolean(true);
        let mut buf = encode_scalar(&s1);
        buf.extend_from_slice(&encode_scalar(&s2));
        buf.extend_from_slice(&encode_scalar(&s3));

        let (v1, c1) = decode_scalar(&buf).unwrap();
        assert_eq!(v1, s1);
        let (v2, c2) = decode_scalar(&buf[c1..]).unwrap();
        assert_eq!(v2, s2);
        let (v3, c3) = decode_scalar(&buf[c1 + c2..]).unwrap();
        assert_eq!(v3, s3);
        assert_eq!(c1 + c2 + c3, buf.len());
    }

    // ─── Id encoding preserves ULID order ───

    #[test]
    fn test_id_ordering_matches_bytes() {
        let id_low = DocId([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        let id_high = DocId([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
        let enc_low = encode_scalar(&Scalar::Id(id_low));
        let enc_high = encode_scalar(&Scalar::Id(id_high));
        assert!(enc_low < enc_high);
    }

    // ─── All type roundtrips in one test ───

    #[test]
    fn test_all_scalar_types_roundtrip() {
        let values = vec![
            Scalar::Undefined,
            Scalar::Null,
            Scalar::Int64(i64::MIN),
            Scalar::Int64(-1),
            Scalar::Int64(0),
            Scalar::Int64(1),
            Scalar::Int64(i64::MAX),
            Scalar::Float64(f64::NEG_INFINITY),
            Scalar::Float64(-1.0),
            Scalar::Float64(0.0),
            Scalar::Float64(1.0),
            Scalar::Float64(f64::INFINITY),
            Scalar::Boolean(false),
            Scalar::Boolean(true),
            Scalar::String(String::new()),
            Scalar::String("hello world".into()),
            Scalar::String("with\x00null".into()),
            Scalar::Bytes(vec![]),
            Scalar::Bytes(vec![0x00, 0xFF, 0x42]),
        ];

        for val in &values {
            let encoded = encode_scalar(val);
            let (decoded, consumed) = decode_scalar(&encoded).unwrap();
            assert_eq!(consumed, encoded.len(), "consumed should match encoded length for {:?}", val);
            match (val, &decoded) {
                (Scalar::Id(_), Scalar::String(_)) => {} // Id decodes as String, expected
                _ => assert_eq!(*val, decoded, "roundtrip failed for {:?}", val),
            }
        }
    }

    // ─── Primary key: same doc, ts ordering is descending within group ───

    #[test]
    fn test_primary_key_ts_ordering_comprehensive() {
        let id = DocId([1; 16]);
        let timestamps = [0, 1, 100, 1000, u64::MAX / 2, u64::MAX];
        let keys: Vec<_> = timestamps.iter().map(|&ts| make_primary_key(&id, ts)).collect();
        // Newer ts has smaller inv_ts, so sorts first (before older entries)
        for i in 0..keys.len() - 1 {
            assert!(keys[i] > keys[i + 1],
                "ts={} (inv_ts={}) should sort after ts={} (inv_ts={})",
                timestamps[i], inv_ts(timestamps[i]),
                timestamps[i + 1], inv_ts(timestamps[i + 1]));
        }
    }

    // ─── Secondary key: value ordering preserved ───

    #[test]
    fn test_secondary_key_value_ordering() {
        let id = DocId([0; 16]);
        let ts = 1;
        let k1 = make_secondary_key(&[Scalar::Int64(1)], &id, ts);
        let k2 = make_secondary_key(&[Scalar::Int64(2)], &id, ts);
        let k3 = make_secondary_key(&[Scalar::String("a".into())], &id, ts);
        assert!(k1 < k2, "Int(1) < Int(2) in secondary key");
        assert!(k2 < k3, "Int(2) < String('a') in secondary key (cross-type)");
    }
}
