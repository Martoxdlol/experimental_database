//! ULID generation and Crockford Base32 encoding.

use crate::types::DocId;

/// Crockford Base32 alphabet (lowercase).
const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Decode table for Crockford Base32 (maps ASCII byte -> 5-bit value, 0xFF = invalid).
const DECODE_TABLE: [u8; 128] = {
    let mut table = [0xFFu8; 128];
    // digits
    table[b'0' as usize] = 0;
    table[b'1' as usize] = 1;
    table[b'2' as usize] = 2;
    table[b'3' as usize] = 3;
    table[b'4' as usize] = 4;
    table[b'5' as usize] = 5;
    table[b'6' as usize] = 6;
    table[b'7' as usize] = 7;
    table[b'8' as usize] = 8;
    table[b'9' as usize] = 9;
    // lowercase letters
    table[b'a' as usize] = 10;
    table[b'b' as usize] = 11;
    table[b'c' as usize] = 12;
    table[b'd' as usize] = 13;
    table[b'e' as usize] = 14;
    table[b'f' as usize] = 15;
    table[b'g' as usize] = 16;
    table[b'h' as usize] = 17;
    table[b'j' as usize] = 18;
    table[b'k' as usize] = 19;
    table[b'm' as usize] = 20;
    table[b'n' as usize] = 21;
    table[b'p' as usize] = 22;
    table[b'q' as usize] = 23;
    table[b'r' as usize] = 24;
    table[b's' as usize] = 25;
    table[b't' as usize] = 26;
    table[b'v' as usize] = 27;
    table[b'w' as usize] = 28;
    table[b'x' as usize] = 29;
    table[b'y' as usize] = 30;
    table[b'z' as usize] = 31;
    // uppercase letters (same values)
    table[b'A' as usize] = 10;
    table[b'B' as usize] = 11;
    table[b'C' as usize] = 12;
    table[b'D' as usize] = 13;
    table[b'E' as usize] = 14;
    table[b'F' as usize] = 15;
    table[b'G' as usize] = 16;
    table[b'H' as usize] = 17;
    table[b'J' as usize] = 18;
    table[b'K' as usize] = 19;
    table[b'M' as usize] = 20;
    table[b'N' as usize] = 21;
    table[b'P' as usize] = 22;
    table[b'Q' as usize] = 23;
    table[b'R' as usize] = 24;
    table[b'S' as usize] = 25;
    table[b'T' as usize] = 26;
    table[b'V' as usize] = 27;
    table[b'W' as usize] = 28;
    table[b'X' as usize] = 29;
    table[b'Y' as usize] = 30;
    table[b'Z' as usize] = 31;
    // Common substitutions
    table[b'O' as usize] = 0; // O -> 0
    table[b'o' as usize] = 0;
    table[b'I' as usize] = 1; // I -> 1
    table[b'i' as usize] = 1;
    table[b'L' as usize] = 1; // L -> 1
    table[b'l' as usize] = 1;
    table
};

/// Encode DocId as 26-char Crockford Base32 string.
pub fn encode_ulid(id: &DocId) -> String {
    // ULID is 128 bits = 26 base32 characters (first char uses 2 bits, rest use 5)
    let bytes = id.as_bytes();
    // Combine into a u128
    let val = u128::from_be_bytes(*bytes);

    let mut result = [0u8; 26];
    // First character encodes top 2 bits
    result[0] = CROCKFORD[(val >> 125) as usize & 0x1F];
    for (i, byte) in result.iter_mut().enumerate().skip(1) {
        let shift = 125 - (i * 5);
        *byte = CROCKFORD[(val >> shift) as usize & 0x1F];
    }
    String::from_utf8(result.to_vec()).unwrap()
}

/// Decode Crockford Base32 string to DocId (case-insensitive).
pub fn decode_ulid(s: &str) -> Result<DocId, String> {
    if s.len() != 26 {
        return Err(format!("ULID must be 26 characters, got {}", s.len()));
    }
    let mut val: u128 = 0;
    for (i, byte) in s.bytes().enumerate() {
        if byte >= 128 {
            return Err(format!("invalid character at position {i}"));
        }
        let bits = DECODE_TABLE[byte as usize];
        if bits == 0xFF {
            return Err(format!("invalid character '{}' at position {i}", byte as char));
        }
        if i == 0 {
            // First char only uses 2 bits for a 128-bit ULID
            val = (bits as u128) & 0x07;
        } else {
            val = (val << 5) | (bits as u128);
        }
    }
    Ok(DocId(val.to_be_bytes()))
}

/// Generate a new ULID with current timestamp and random component.
pub fn generate_ulid() -> DocId {
    use std::time::SystemTime;

    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // ULID: 48-bit timestamp (ms) || 80-bit random
    let timestamp_bits = (millis as u128) << 80;

    // Simple random: use std to get some entropy
    let random: u128 = {
        let mut buf = [0u8; 10];
        // Use a simple source of randomness
        let addr = &buf as *const _ as u64;
        let seed = millis.wrapping_mul(6364136223846793005).wrapping_add(addr);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (seed.wrapping_mul((i as u64).wrapping_add(1)) >> (i * 3)) as u8;
        }
        let mut val: u128 = 0;
        for &b in &buf {
            val = (val << 8) | b as u128;
        }
        val
    };

    let ulid = timestamp_bits | (random & ((1u128 << 80) - 1));
    DocId(ulid.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ulid_roundtrip() {
        let id = generate_ulid();
        let encoded = encode_ulid(&id);
        assert_eq!(encoded.len(), 26);
        let decoded = decode_ulid(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_decode_case_insensitive() {
        let id = DocId([0; 16]);
        let encoded = encode_ulid(&id);
        let upper = encoded.to_uppercase();
        let decoded = decode_ulid(&upper).unwrap();
        assert_eq!(id, decoded);
    }
}
