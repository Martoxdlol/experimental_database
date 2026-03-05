//! Posting list codec for inverted indexes.
//!
//! Provides a sorted list of (key, value) entries used by GIN and full-text search
//! indexes. Keys are typically `doc_id[16] || inv_ts[8]` (24 bytes), values are
//! optional payload (e.g. term positions for phrase queries).
//!
//! The codec is domain-agnostic — callers define key/value semantics.

use std::io;

/// A single posting entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostingEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// A sorted list of posting entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingList {
    pub entries: Vec<PostingEntry>,
}

impl PostingList {
    /// Create an empty posting list.
    pub fn new() -> Self {
        PostingList {
            entries: Vec::new(),
        }
    }

    /// Create from entries (must already be sorted by key).
    pub fn from_sorted(entries: Vec<PostingEntry>) -> Self {
        PostingList { entries }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check if a key exists via binary search.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.entries
            .binary_search_by(|e| e.key.as_slice().cmp(key))
            .is_ok()
    }

    /// Encode to bytes.
    ///
    /// Wire format: `entry_count(u32 LE) || [key_len(u16 LE) || key || value_len(u16 LE) || value]...`
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in &self.entries {
            buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&entry.key);
            buf.extend_from_slice(&(entry.value.len() as u16).to_le_bytes());
            buf.extend_from_slice(&entry.value);
        }
        buf
    }

    /// Compute encoded size without allocating.
    pub fn encoded_size(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(|e| 2 + e.key.len() + 2 + e.value.len())
            .sum::<usize>()
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < 4 {
            return Err(crate::error::StorageError::Corruption(
                "posting list too short for header".into(),
            )
            .into());
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut offset = 4;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            if offset + 2 > data.len() {
                return Err(crate::error::StorageError::Corruption(
                    "posting list truncated at key_len".into(),
                )
                .into());
            }
            let key_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if offset + key_len > data.len() {
                return Err(crate::error::StorageError::Corruption(
                    "posting list truncated at key".into(),
                )
                .into());
            }
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset + 2 > data.len() {
                return Err(crate::error::StorageError::Corruption(
                    "posting list truncated at value_len".into(),
                )
                .into());
            }
            let value_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if offset + value_len > data.len() {
                return Err(crate::error::StorageError::Corruption(
                    "posting list truncated at value".into(),
                )
                .into());
            }
            let value = data[offset..offset + value_len].to_vec();
            offset += value_len;

            entries.push(PostingEntry { key, value });
        }

        Ok(PostingList { entries })
    }

    /// Merge two sorted lists. On duplicate keys, `other` wins.
    pub fn union(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::with_capacity(self.entries.len() + other.entries.len());
        let (mut i, mut j) = (0, 0);

        while i < self.entries.len() && j < other.entries.len() {
            match self.entries[i].key.cmp(&other.entries[j].key) {
                std::cmp::Ordering::Less => {
                    result.push(self.entries[i].clone());
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    result.push(other.entries[j].clone());
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    // other wins on conflict
                    result.push(other.entries[j].clone());
                    i += 1;
                    j += 1;
                }
            }
        }
        result.extend_from_slice(&self.entries[i..]);
        result.extend_from_slice(&other.entries[j..]);

        PostingList { entries: result }
    }

    /// Intersect by key. Values come from `self`.
    pub fn intersect(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::new();
        let (mut i, mut j) = (0, 0);

        while i < self.entries.len() && j < other.entries.len() {
            match self.entries[i].key.cmp(&other.entries[j].key) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    result.push(self.entries[i].clone());
                    i += 1;
                    j += 1;
                }
            }
        }

        PostingList { entries: result }
    }

    /// Remove entries whose keys appear in `other`.
    pub fn difference(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::new();
        let (mut i, mut j) = (0, 0);

        while i < self.entries.len() {
            if j < other.entries.len() {
                match self.entries[i].key.cmp(&other.entries[j].key) {
                    std::cmp::Ordering::Less => {
                        result.push(self.entries[i].clone());
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        j += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        i += 1;
                        j += 1;
                    }
                }
            } else {
                result.push(self.entries[i].clone());
                i += 1;
            }
        }

        PostingList { entries: result }
    }

    /// Apply pending inserts and deletes to produce a new list.
    /// Equivalent to `self.difference(deletes).union(inserts)`.
    pub fn merge_pending(&self, inserts: &PostingList, deletes: &PostingList) -> PostingList {
        self.difference(deletes).union(inserts)
    }
}

impl Default for PostingList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &[u8], value: &[u8]) -> PostingEntry {
        PostingEntry {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn list(entries: Vec<PostingEntry>) -> PostingList {
        PostingList { entries }
    }

    // ─── Codec tests ───

    #[test]
    fn empty_list_encode_decode() {
        let pl = PostingList::new();
        let data = pl.encode();
        assert_eq!(data.len(), 4); // just the count
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded, pl);
        assert!(decoded.is_empty());
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn single_entry_roundtrip() {
        let pl = list(vec![entry(b"hello", b"world")]);
        let data = pl.encode();
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded, pl);
        assert_eq!(decoded.len(), 1);
    }

    #[test]
    fn multi_entry_roundtrip() {
        let entries: Vec<PostingEntry> = (0u8..10)
            .map(|i| entry(&[i], &[i * 2]))
            .collect();
        let pl = list(entries);
        let data = pl.encode();
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded, pl);
        assert_eq!(decoded.len(), 10);
    }

    #[test]
    fn encoded_size_matches() {
        let pl = list(vec![
            entry(b"abc", b"12"),
            entry(b"def", b""),
        ]);
        assert_eq!(pl.encoded_size(), pl.encode().len());
    }

    #[test]
    fn decode_truncated_header() {
        assert!(PostingList::decode(&[0x01, 0x00]).is_err());
        assert!(PostingList::decode(&[]).is_err());
    }

    #[test]
    fn decode_truncated_entry() {
        // Count says 1 entry but no data follows.
        let data = 1u32.to_le_bytes();
        assert!(PostingList::decode(&data).is_err());

        // key_len present but key truncated.
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&5u16.to_le_bytes()); // key_len = 5
        data.extend_from_slice(&[0u8; 3]); // only 3 bytes
        assert!(PostingList::decode(&data).is_err());
    }

    // ─── contains_key ───

    #[test]
    fn contains_key_found() {
        let pl = list(vec![
            entry(&[1], b""),
            entry(&[3], b""),
            entry(&[5], b""),
        ]);
        assert!(pl.contains_key(&[1]));
        assert!(pl.contains_key(&[3]));
        assert!(pl.contains_key(&[5]));
    }

    #[test]
    fn contains_key_missing() {
        let pl = list(vec![
            entry(&[1], b""),
            entry(&[3], b""),
            entry(&[5], b""),
        ]);
        assert!(!pl.contains_key(&[0]));
        assert!(!pl.contains_key(&[2]));
        assert!(!pl.contains_key(&[6]));
    }

    // ─── Union ───

    #[test]
    fn union_disjoint() {
        let a = list(vec![entry(&[1], b"a"), entry(&[3], b"c")]);
        let b = list(vec![entry(&[2], b"b"), entry(&[4], b"d")]);
        let result = a.union(&b);
        assert_eq!(
            result,
            list(vec![
                entry(&[1], b"a"),
                entry(&[2], b"b"),
                entry(&[3], b"c"),
                entry(&[4], b"d"),
            ])
        );
    }

    #[test]
    fn union_overlapping() {
        let a = list(vec![entry(&[1], b"old"), entry(&[2], b"old")]);
        let b = list(vec![entry(&[2], b"new"), entry(&[3], b"new")]);
        let result = a.union(&b);
        assert_eq!(
            result,
            list(vec![
                entry(&[1], b"old"),
                entry(&[2], b"new"), // b wins
                entry(&[3], b"new"),
            ])
        );
    }

    #[test]
    fn union_empty() {
        let a = list(vec![entry(&[1], b"x")]);
        let b = PostingList::new();
        assert_eq!(a.union(&b), a);
        assert_eq!(b.union(&a), a);
    }

    // ─── Intersect ───

    #[test]
    fn intersect_basic() {
        let a = list(vec![
            entry(&[1], b"a"),
            entry(&[2], b"a"),
            entry(&[3], b"a"),
        ]);
        let b = list(vec![
            entry(&[2], b"b"),
            entry(&[3], b"b"),
            entry(&[4], b"b"),
        ]);
        let result = a.intersect(&b);
        // Values from a.
        assert_eq!(
            result,
            list(vec![entry(&[2], b"a"), entry(&[3], b"a")])
        );
    }

    #[test]
    fn intersect_disjoint() {
        let a = list(vec![entry(&[1], b"")]);
        let b = list(vec![entry(&[2], b"")]);
        assert!(a.intersect(&b).is_empty());
    }

    #[test]
    fn intersect_identical() {
        let a = list(vec![entry(&[1], b"x"), entry(&[2], b"y")]);
        let result = a.intersect(&a);
        assert_eq!(result, a);
    }

    // ─── Difference ───

    #[test]
    fn difference_basic() {
        let a = list(vec![
            entry(&[1], b""),
            entry(&[2], b""),
            entry(&[3], b""),
        ]);
        let b = list(vec![entry(&[2], b"")]);
        let result = a.difference(&b);
        assert_eq!(
            result,
            list(vec![entry(&[1], b""), entry(&[3], b"")])
        );
    }

    #[test]
    fn difference_all() {
        let a = list(vec![entry(&[1], b""), entry(&[2], b"")]);
        let result = a.difference(&a);
        assert!(result.is_empty());
    }

    #[test]
    fn difference_none() {
        let a = list(vec![entry(&[1], b""), entry(&[2], b"")]);
        let b = list(vec![entry(&[5], b"")]);
        assert_eq!(a.difference(&b), a);
    }

    // ─── merge_pending ───

    #[test]
    fn merge_pending_test() {
        let base = list(vec![
            entry(&[1], b"v1"),
            entry(&[2], b"v2"),
            entry(&[3], b"v3"),
        ]);
        let inserts = list(vec![
            entry(&[2], b"v2_new"), // update existing
            entry(&[4], b"v4"),
        ]);
        let deletes = list(vec![entry(&[1], b"")]); // remove key 1

        let result = base.merge_pending(&inserts, &deletes);
        assert_eq!(
            result,
            list(vec![
                entry(&[2], b"v2_new"),
                entry(&[3], b"v3"),
                entry(&[4], b"v4"),
            ])
        );
    }

    // ─── Large list ───

    #[test]
    fn large_list_roundtrip() {
        let entries: Vec<PostingEntry> = (0u32..10_000)
            .map(|i| {
                let key = i.to_be_bytes().to_vec(); // 4 bytes, sorted
                let value = vec![(i & 0xFF) as u8];
                PostingEntry { key, value }
            })
            .collect();
        let pl = list(entries);
        let data = pl.encode();
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded.len(), 10_000);
        assert_eq!(decoded, pl);
    }

    // ─── Variable length ───

    #[test]
    fn variable_length_keys() {
        let pl = list(vec![
            entry(&[1], b""),
            entry(&[1, 2], b""),
            entry(&[1, 2, 3], b""),
            entry(&[2], b""),
        ]);
        let data = pl.encode();
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded, pl);
    }

    #[test]
    fn variable_length_values() {
        let pl = list(vec![
            entry(&[1], b""),
            entry(&[2], b"short"),
            entry(&[3], &vec![0xAB; 100]),
        ]);
        let data = pl.encode();
        let decoded = PostingList::decode(&data).unwrap();
        assert_eq!(decoded, pl);
    }
}
