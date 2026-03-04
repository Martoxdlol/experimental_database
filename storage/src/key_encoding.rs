use crate::types::*;

pub mod type_tag {
    pub const UNDEFINED: u8 = 0x00;
    pub const NULL: u8 = 0x01;
    pub const INT64: u8 = 0x02;
    pub const FLOAT64: u8 = 0x03;
    pub const BOOLEAN: u8 = 0x04;
    pub const STRING: u8 = 0x05;
    pub const BYTES: u8 = 0x06;
    pub const ARRAY: u8 = 0x07;
}

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

/// Encode a single ScalarValue into the buffer.
pub fn encode_scalar(buf: &mut Vec<u8>, value: &ScalarValue) {
    match value {
        ScalarValue::Undefined => buf.push(type_tag::UNDEFINED),
        ScalarValue::Null => buf.push(type_tag::NULL),
        ScalarValue::Int64(v) => encode_int64(buf, *v),
        ScalarValue::Float64(v) => encode_float64(buf, *v),
        ScalarValue::Boolean(v) => encode_boolean(buf, *v),
        ScalarValue::String(s) => encode_string(buf, s),
        ScalarValue::Bytes(b) => encode_bytes(buf, b),
        ScalarValue::Array(elems) => encode_array(buf, elems),
    }
}

/// Encode a compound key: multiple scalars concatenated.
pub fn encode_compound_key(values: &[ScalarValue]) -> EncodedKey {
    let mut buf = Vec::new();
    for v in values {
        encode_scalar(&mut buf, v);
    }
    EncodedKey(buf)
}

/// Encode a primary key: doc_id[16] || inv_ts[8].
pub fn encode_primary_key(doc_id: DocId, commit_ts: Timestamp) -> EncodedKey {
    let mut buf = Vec::with_capacity(PRIMARY_KEY_SIZE);
    buf.extend_from_slice(&doc_id.to_bytes());
    buf.extend_from_slice(&commit_ts.inverted().to_be_bytes());
    EncodedKey(buf)
}

/// Encode a secondary index key: encoded_values || doc_id[16] || inv_ts[8].
pub fn encode_secondary_key(
    values: &[ScalarValue],
    doc_id: DocId,
    commit_ts: Timestamp,
) -> EncodedKey {
    let mut buf = Vec::new();
    for v in values {
        encode_scalar(&mut buf, v);
    }
    buf.extend_from_slice(&doc_id.to_bytes());
    buf.extend_from_slice(&commit_ts.inverted().to_be_bytes());
    EncodedKey(buf)
}

/// Encode a created_at index key.
pub fn encode_created_at_key(
    created_at: i64,
    doc_id: DocId,
    commit_ts: Timestamp,
) -> EncodedKey {
    let mut buf = Vec::new();
    encode_int64(&mut buf, created_at);
    buf.extend_from_slice(&doc_id.to_bytes());
    buf.extend_from_slice(&commit_ts.inverted().to_be_bytes());
    EncodedKey(buf)
}

fn encode_int64(buf: &mut Vec<u8>, v: i64) {
    buf.push(type_tag::INT64);
    let mut be = v.to_be_bytes();
    be[0] ^= 0x80; // flip sign bit for unsigned sort order
    buf.extend_from_slice(&be);
}

fn encode_float64(buf: &mut Vec<u8>, v: f64) {
    buf.push(type_tag::FLOAT64);
    let canonical = if v.is_nan() {
        f64::from_bits(0x7FF8_0000_0000_0000)
    } else {
        v
    };
    let mut bits = canonical.to_bits().to_be_bytes();
    if bits[0] & 0x80 == 0 {
        bits[0] ^= 0x80; // positive: flip sign bit
    } else {
        for b in &mut bits {
            *b ^= 0xFF; // negative: flip all bits
        }
    }
    buf.extend_from_slice(&bits);
}

fn encode_boolean(buf: &mut Vec<u8>, v: bool) {
    buf.push(type_tag::BOOLEAN);
    buf.push(if v { 0x01 } else { 0x00 });
}

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(type_tag::STRING);
    encode_escaped_bytes(buf, s.as_bytes());
    buf.extend_from_slice(&[0x00, 0x00]);
}

fn encode_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.push(type_tag::BYTES);
    encode_escaped_bytes(buf, b);
    buf.extend_from_slice(&[0x00, 0x00]);
}

fn encode_array(buf: &mut Vec<u8>, elements: &[ScalarValue]) {
    buf.push(type_tag::ARRAY);
    for elem in elements {
        encode_scalar(buf, elem);
    }
    buf.extend_from_slice(&[0x00, 0x00]);
}

fn encode_escaped_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    for &b in data {
        buf.push(b);
        if b == 0x00 {
            buf.push(0xFF); // escape
        }
    }
}

// --- Decoding ---

/// Decode a single ScalarValue from the cursor, advancing it.
pub fn decode_scalar(cursor: &mut &[u8]) -> Result<ScalarValue, KeyDecodingError> {
    if cursor.is_empty() {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    match tag {
        type_tag::UNDEFINED => Ok(ScalarValue::Undefined),
        type_tag::NULL => Ok(ScalarValue::Null),
        type_tag::INT64 => decode_int64(cursor).map(ScalarValue::Int64),
        type_tag::FLOAT64 => decode_float64(cursor).map(ScalarValue::Float64),
        type_tag::BOOLEAN => decode_boolean(cursor).map(ScalarValue::Boolean),
        type_tag::STRING => decode_string(cursor).map(ScalarValue::String),
        type_tag::BYTES => decode_bytes_val(cursor).map(ScalarValue::Bytes),
        type_tag::ARRAY => decode_array(cursor).map(ScalarValue::Array),
        _ => Err(KeyDecodingError::UnknownTypeTag(tag)),
    }
}

/// Decode a primary key: returns (doc_id, commit_ts).
pub fn decode_primary_key(key: &EncodedKey) -> Result<(DocId, Timestamp), KeyDecodingError> {
    if key.0.len() < PRIMARY_KEY_SIZE {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let doc_id = DocId::from_bytes(key.0[..16].try_into().unwrap());
    let inv_ts = u64::from_be_bytes(key.0[16..24].try_into().unwrap());
    let ts = Timestamp(u64::MAX - inv_ts);
    Ok((doc_id, ts))
}

/// Decode a secondary key's suffix: extract (doc_id, commit_ts) from the last 24 bytes.
pub fn decode_secondary_key_suffix(key: &EncodedKey) -> Result<(DocId, Timestamp), KeyDecodingError> {
    if key.0.len() < PRIMARY_KEY_SIZE {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let suffix_start = key.0.len() - PRIMARY_KEY_SIZE;
    let doc_id = DocId::from_bytes(key.0[suffix_start..suffix_start + 16].try_into().unwrap());
    let inv_ts = u64::from_be_bytes(key.0[suffix_start + 16..].try_into().unwrap());
    let ts = Timestamp(u64::MAX - inv_ts);
    Ok((doc_id, ts))
}

fn decode_int64(cursor: &mut &[u8]) -> Result<i64, KeyDecodingError> {
    if cursor.len() < 8 {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let mut be: [u8; 8] = cursor[..8].try_into().unwrap();
    *cursor = &cursor[8..];
    be[0] ^= 0x80;
    Ok(i64::from_be_bytes(be))
}

fn decode_float64(cursor: &mut &[u8]) -> Result<f64, KeyDecodingError> {
    if cursor.len() < 8 {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let mut bits: [u8; 8] = cursor[..8].try_into().unwrap();
    *cursor = &cursor[8..];
    if bits[0] & 0x80 != 0 {
        bits[0] ^= 0x80; // was positive
    } else {
        for b in &mut bits {
            *b ^= 0xFF; // was negative
        }
    }
    Ok(f64::from_be_bytes(bits))
}

fn decode_boolean(cursor: &mut &[u8]) -> Result<bool, KeyDecodingError> {
    if cursor.is_empty() {
        return Err(KeyDecodingError::UnexpectedEnd);
    }
    let v = cursor[0] != 0;
    *cursor = &cursor[1..];
    Ok(v)
}

fn decode_string(cursor: &mut &[u8]) -> Result<String, KeyDecodingError> {
    let bytes = decode_escaped_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|_| KeyDecodingError::InvalidUtf8)
}

fn decode_bytes_val(cursor: &mut &[u8]) -> Result<Vec<u8>, KeyDecodingError> {
    decode_escaped_bytes(cursor)
}

fn decode_array(cursor: &mut &[u8]) -> Result<Vec<ScalarValue>, KeyDecodingError> {
    let mut elements = Vec::new();
    loop {
        // Check for terminator 0x00 0x00.
        if cursor.len() >= 2 && cursor[0] == 0x00 && cursor[1] == 0x00 {
            *cursor = &cursor[2..];
            return Ok(elements);
        }
        if cursor.is_empty() {
            return Err(KeyDecodingError::UnterminatedEscape);
        }
        elements.push(decode_scalar(cursor)?);
    }
}

fn decode_escaped_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>, KeyDecodingError> {
    let mut result = Vec::new();
    loop {
        if cursor.is_empty() {
            return Err(KeyDecodingError::UnterminatedEscape);
        }
        let b = cursor[0];
        if b == 0x00 {
            if cursor.len() < 2 {
                return Err(KeyDecodingError::UnterminatedEscape);
            }
            if cursor[1] == 0x00 {
                // Terminator.
                *cursor = &cursor[2..];
                return Ok(result);
            } else if cursor[1] == 0xFF {
                // Escaped null byte.
                result.push(0x00);
                *cursor = &cursor[2..];
            } else {
                return Err(KeyDecodingError::UnterminatedEscape);
            }
        } else {
            result.push(b);
            *cursor = &cursor[1..];
        }
    }
}

/// Compute the successor prefix for range scan upper bounds.
pub fn successor_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut result = prefix.to_vec();
    // Increment the last non-0xFF byte and truncate.
    while let Some(&last) = result.last() {
        if last < 0xFF {
            *result.last_mut().unwrap() += 1;
            return result;
        }
        result.pop();
    }
    // All 0xFF — return a sentinel that's larger than anything.
    vec![0xFF; prefix.len() + 1]
}

/// Successor of a full key.
pub fn successor_key(key: &EncodedKey) -> EncodedKey {
    let mut result = key.0.clone();
    result.push(0x00);
    EncodedKey(result)
}

/// Minimum possible key.
pub fn min_key() -> EncodedKey {
    EncodedKey(vec![])
}

/// Maximum possible key sentinel.
pub fn max_key() -> EncodedKey {
    EncodedKey(vec![0xFF; 32])
}

/// Extract a scalar value from a BSON document at the given field path.
pub fn extract_field_value(doc: &bson::Document, path: &FieldPath) -> ScalarValue {
    if path.is_empty() {
        return ScalarValue::Undefined;
    }

    let mut current: &bson::Bson = match doc.get(&path[0]) {
        Some(v) => v,
        None => return ScalarValue::Undefined,
    };

    for segment in &path[1..] {
        match current {
            bson::Bson::Document(inner) => match inner.get(segment) {
                Some(v) => current = v,
                None => return ScalarValue::Undefined,
            },
            _ => return ScalarValue::Undefined,
        }
    }

    bson_to_scalar(current)
}

fn bson_to_scalar(val: &bson::Bson) -> ScalarValue {
    match val {
        bson::Bson::Null => ScalarValue::Null,
        bson::Bson::Boolean(b) => ScalarValue::Boolean(*b),
        bson::Bson::Int32(i) => ScalarValue::Int64(*i as i64),
        bson::Bson::Int64(i) => ScalarValue::Int64(*i),
        bson::Bson::Double(f) => ScalarValue::Float64(*f),
        bson::Bson::String(s) => ScalarValue::String(s.clone()),
        bson::Bson::Binary(b) => ScalarValue::Bytes(b.bytes.clone()),
        bson::Bson::Array(arr) => {
            ScalarValue::Array(arr.iter().map(bson_to_scalar).collect())
        }
        _ => ScalarValue::Undefined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int64_sort_order() {
        let values = [i64::MIN, -1, 0, 1, i64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut buf = Vec::new();
                encode_scalar(&mut buf, &ScalarValue::Int64(*v));
                buf
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1], "{:?} should be < {:?}", w[0], w[1]);
        }
    }

    #[test]
    fn test_float64_sort_order() {
        let values = [
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
        ];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut buf = Vec::new();
                encode_scalar(&mut buf, &ScalarValue::Float64(*v));
                buf
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] <= w[1], "{:?} should be <= {:?}", w[0], w[1]);
        }
    }

    #[test]
    fn test_float64_nan_canonicalization() {
        let nan1 = f64::from_bits(0x7FF0_0000_0000_0001);
        let nan2 = f64::from_bits(0x7FF8_0000_0000_0000);
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_scalar(&mut buf1, &ScalarValue::Float64(nan1));
        encode_scalar(&mut buf2, &ScalarValue::Float64(nan2));
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn test_boolean_sort_order() {
        let mut f = Vec::new();
        let mut t = Vec::new();
        encode_scalar(&mut f, &ScalarValue::Boolean(false));
        encode_scalar(&mut t, &ScalarValue::Boolean(true));
        assert!(f < t);
    }

    #[test]
    fn test_string_sort_order() {
        let strings = ["", "a", "aa", "b"];
        let encoded: Vec<Vec<u8>> = strings
            .iter()
            .map(|s| {
                let mut buf = Vec::new();
                encode_scalar(&mut buf, &ScalarValue::String(s.to_string()));
                buf
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn test_string_with_null_bytes() {
        let s = "a\x00b";
        let mut buf = Vec::new();
        encode_scalar(&mut buf, &ScalarValue::String(s.to_string()));
        let mut cursor: &[u8] = &buf;
        let decoded = decode_scalar(&mut cursor).unwrap();
        assert_eq!(decoded, ScalarValue::String(s.to_string()));
    }

    #[test]
    fn test_bytes_encoding() {
        let data = vec![0x00, 0xFF, 0x00];
        let mut buf = Vec::new();
        encode_scalar(&mut buf, &ScalarValue::Bytes(data.clone()));
        let mut cursor: &[u8] = &buf;
        let decoded = decode_scalar(&mut cursor).unwrap();
        assert_eq!(decoded, ScalarValue::Bytes(data));
    }

    #[test]
    fn test_cross_type_ordering() {
        let values = [
            ScalarValue::Null,
            ScalarValue::Int64(0),
            ScalarValue::Float64(0.0),
            ScalarValue::Boolean(false),
            ScalarValue::String("a".to_string()),
            ScalarValue::Bytes(vec![0]),
        ];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut buf = Vec::new();
                encode_scalar(&mut buf, v);
                buf
            })
            .collect();
        for w in encoded.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn test_primary_key_roundtrip() {
        let doc_id = DocId(0x0123456789ABCDEF_FEDCBA9876543210);
        let ts = Timestamp(42);
        let key = encode_primary_key(doc_id, ts);
        let (d, t) = decode_primary_key(&key).unwrap();
        assert_eq!(d, doc_id);
        assert_eq!(t, ts);
    }

    #[test]
    fn test_primary_key_sort_newer_first() {
        let doc_id = DocId(1);
        let k1 = encode_primary_key(doc_id, Timestamp(20));
        let k2 = encode_primary_key(doc_id, Timestamp(10));
        assert!(k1 < k2, "newer (ts=20) should sort before older (ts=10)");
    }

    #[test]
    fn test_compound_key() {
        let values = vec![ScalarValue::Int64(1), ScalarValue::String("hello".into())];
        let key = encode_compound_key(&values);
        let mut cursor: &[u8] = &key.0;
        let v1 = decode_scalar(&mut cursor).unwrap();
        let v2 = decode_scalar(&mut cursor).unwrap();
        assert_eq!(v1, ScalarValue::Int64(1));
        assert_eq!(v2, ScalarValue::String("hello".into()));
    }

    #[test]
    fn test_compound_key_prefix_ordering() {
        let k1 = encode_compound_key(&[ScalarValue::Int64(1), ScalarValue::String("a".into())]);
        let k2 = encode_compound_key(&[ScalarValue::Int64(1), ScalarValue::String("b".into())]);
        let k3 = encode_compound_key(&[ScalarValue::Int64(2), ScalarValue::String("a".into())]);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn test_secondary_key_suffix() {
        let doc_id = DocId(999);
        let ts = Timestamp(50);
        let key = encode_secondary_key(&[ScalarValue::String("active".into())], doc_id, ts);
        let (d, t) = decode_secondary_key_suffix(&key).unwrap();
        assert_eq!(d, doc_id);
        assert_eq!(t, ts);
    }

    #[test]
    fn test_undefined_encoding() {
        let mut buf = Vec::new();
        encode_scalar(&mut buf, &ScalarValue::Undefined);
        assert_eq!(buf, vec![0x00]);

        let mut undef = Vec::new();
        let mut null_buf = Vec::new();
        let mut min_int = Vec::new();
        encode_scalar(&mut undef, &ScalarValue::Undefined);
        encode_scalar(&mut null_buf, &ScalarValue::Null);
        encode_scalar(&mut min_int, &ScalarValue::Int64(i64::MIN));
        assert!(undef < null_buf);
        assert!(null_buf < min_int);
    }

    #[test]
    fn test_array_encoding() {
        let arr = ScalarValue::Array(vec![ScalarValue::Int64(1), ScalarValue::String("a".into())]);
        let mut buf = Vec::new();
        encode_scalar(&mut buf, &arr);
        let mut cursor: &[u8] = &buf;
        let decoded = decode_scalar(&mut cursor).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn test_extract_field_value_bson() {
        let doc = bson::doc! {
            "user": { "email": "a@b.com" },
            "count": 42_i64,
        };
        let email = extract_field_value(&doc, &vec!["user".into(), "email".into()]);
        assert_eq!(email, ScalarValue::String("a@b.com".into()));
        let missing = extract_field_value(&doc, &vec!["missing".into()]);
        assert_eq!(missing, ScalarValue::Undefined);
    }

    #[test]
    fn test_successor_prefix() {
        let prefix = vec![0x01, 0x02, 0x03];
        let succ = successor_prefix(&prefix);
        assert_eq!(succ, vec![0x01, 0x02, 0x04]);

        let prefix_ff = vec![0x01, 0xFF, 0xFF];
        let succ_ff = successor_prefix(&prefix_ff);
        assert_eq!(succ_ff, vec![0x02]);
    }
}
