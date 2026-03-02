//! Secondary index key encoding and index management.
//!
//! Secondary index key format: `type_tag(1) || encoded_value || doc_id(16) || inv_ts(8)`
//!
//! Type tags (total order):
//! 0x00 = Null
//! 0x01 = Bool(false)
//! 0x02 = Bool(true)
//! 0x03 = I64
//! 0x04 = F64
//! 0x05 = String

use crate::types::{DocId, JsonScalar, KeyRange, Ts};

const TAG_NULL: u8 = 0x00;
const TAG_BOOL_FALSE: u8 = 0x01;
const TAG_BOOL_TRUE: u8 = 0x02;
const TAG_I64: u8 = 0x03;
const TAG_F64: u8 = 0x04;
const TAG_STRING: u8 = 0x05;

/// Encode a JsonScalar into a sortable byte sequence.
pub fn encode_scalar(scalar: &JsonScalar) -> Vec<u8> {
    match scalar {
        JsonScalar::Null => vec![TAG_NULL],
        JsonScalar::Bool(false) => vec![TAG_BOOL_FALSE],
        JsonScalar::Bool(true) => vec![TAG_BOOL_TRUE],
        JsonScalar::I64(v) => {
            // Signed-to-ordered: flip the sign bit
            let ordered = ((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes();
            let mut out = vec![TAG_I64];
            out.extend_from_slice(&ordered);
            out
        }
        JsonScalar::F64(v) => {
            let bits = v.to_bits();
            // IEEE 754 sortable transform:
            // If sign bit is set (negative), invert all bits; otherwise flip only sign bit
            let ordered = if bits >> 63 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            let mut out = vec![TAG_F64];
            out.extend_from_slice(&ordered.to_be_bytes());
            out
        }
        JsonScalar::String(s) => {
            // Length-prefixed to avoid issues with null bytes in strings
            let mut out = vec![TAG_STRING];
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
            out
        }
    }
}

/// Build a full secondary index key.
pub fn make_index_key(scalar: &JsonScalar, doc_id: &DocId, ts: Ts) -> Vec<u8> {
    let encoded_val = encode_scalar(scalar);
    let inv_ts = u64::MAX - ts;
    let mut key = Vec::with_capacity(encoded_val.len() + 16 + 8);
    key.extend_from_slice(&encoded_val);
    key.extend_from_slice(doc_id.as_bytes());
    key.extend_from_slice(&inv_ts.to_be_bytes());
    key
}

/// Build a prefix key for scanning all index entries for a specific scalar value.
pub fn make_index_value_prefix(scalar: &JsonScalar) -> Vec<u8> {
    encode_scalar(scalar)
}

/// Build a range for an Eq scan on an index.
pub fn index_eq_range(scalar: &JsonScalar) -> KeyRange {
    let prefix = encode_scalar(scalar);
    let mut end = prefix.clone();
    // Increment last byte to create exclusive upper bound
    increment_bytes(&mut end);
    KeyRange { start: prefix, end }
}

/// Build a range for scanning all entries with scalar value >= start_scalar.
pub fn index_range_gte(start_scalar: &JsonScalar) -> KeyRange {
    let start = encode_scalar(start_scalar);
    KeyRange { start, end: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff] }
}

/// Build a range for scanning all entries with scalar value > start_scalar.
/// We achieve this by appending 0xFF to go past all doc_ids with this value.
pub fn index_range_gt(start_scalar: &JsonScalar) -> KeyRange {
    let mut start = encode_scalar(start_scalar);
    // Go past all doc_ids and timestamps for this value
    start.extend_from_slice(&[0xFF; 24]); // doc_id(16) + inv_ts(8)
    KeyRange { start, end: vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff] }
}

/// Build a range for scanning all entries with scalar value <= end_scalar.
pub fn index_range_lte(end_scalar: &JsonScalar) -> KeyRange {
    let mut end = encode_scalar(end_scalar);
    // Include all doc_ids and timestamps for this value (use max inv_ts = 0)
    end.extend_from_slice(&[0xFF; 16]); // past all doc_ids
    end.extend_from_slice(&[0xFF; 8]);  // past all timestamps
    KeyRange { start: vec![], end }
}

/// Build a range for scanning all entries with scalar value < end_scalar.
pub fn index_range_lt(end_scalar: &JsonScalar) -> KeyRange {
    let end = encode_scalar(end_scalar);
    KeyRange { start: vec![], end }
}

/// Decode (doc_id, ts) from the trailing 24 bytes of a secondary index key.
pub fn decode_index_key_suffix(key: &[u8]) -> Option<(DocId, Ts)> {
    if key.len() < 24 {
        return None;
    }
    let suffix = &key[key.len() - 24..];
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&suffix[..16]);
    let inv_ts = u64::from_be_bytes(suffix[16..24].try_into().ok()?);
    let ts = u64::MAX - inv_ts;
    Some((DocId::from_bytes(id_bytes), ts))
}

/// Increment a byte slice to create the next lexicographic value.
fn increment_bytes(bytes: &mut Vec<u8>) {
    for byte in bytes.iter_mut().rev() {
        if *byte < 0xFF {
            *byte += 1;
            return;
        }
        *byte = 0x00;
    }
    // Overflow: push a new byte
    bytes.push(0x00);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_ordering() {
        // Null < Bool(false) < Bool(true) < I64(-∞) < I64(0) < I64(∞) < F64 < String
        let null = encode_scalar(&JsonScalar::Null);
        let bool_f = encode_scalar(&JsonScalar::Bool(false));
        let bool_t = encode_scalar(&JsonScalar::Bool(true));
        let neg = encode_scalar(&JsonScalar::I64(-1));
        let zero = encode_scalar(&JsonScalar::I64(0));
        let pos = encode_scalar(&JsonScalar::I64(1));
        let s = encode_scalar(&JsonScalar::String("hello".to_string()));

        assert!(null < bool_f);
        assert!(bool_f < bool_t);
        assert!(bool_t < neg); // I64 tag 0x03 > Bool tag 0x02
        assert!(neg < zero);
        assert!(zero < pos);
        assert!(pos < s); // String tag 0x05 > I64 tag 0x03
    }

    #[test]
    fn test_i64_ordering() {
        let cases = [-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX];
        let mut encoded: Vec<Vec<u8>> = cases.iter().map(|&v| encode_scalar(&JsonScalar::I64(v))).collect();
        let mut sorted = cases.to_vec();
        sorted.sort();
        encoded.sort();
        let decoded: Vec<i64> = encoded.iter().map(|e| {
            let bits = u64::from_be_bytes(e[1..9].try_into().unwrap());
            (bits ^ 0x8000_0000_0000_0000) as i64
        }).collect();
        assert_eq!(decoded, sorted);
    }
}
