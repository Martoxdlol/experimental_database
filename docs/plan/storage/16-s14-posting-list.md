# S14: Posting List Codec

## Purpose

Reusable sorted-list codec for inverted indexes (GIN and full-text search). Each posting list is a sorted sequence of (key, value) entries stored as a B-tree value or heap blob. The codec is domain-agnostic — callers define key and value semantics.

## Dependencies

- None (pure codec, no B-tree or heap dependency)

## Rust Types

```rust
/// A single posting entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostingEntry {
    pub key: Vec<u8>,    // typically doc_id[16] || inv_ts[8]
    pub value: Vec<u8>,  // optional payload (e.g. term positions)
}

/// A sorted list of posting entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingList {
    pub entries: Vec<PostingEntry>,
}
```

## Wire Format

```
entry_count:     u32 LE    (4 bytes)
For each entry:
  key_len:       u16 LE    (2 bytes)
  key:           [u8; key_len]
  value_len:     u16 LE    (2 bytes)
  value:         [u8; value_len]
```

Entries must be sorted by key (memcmp order). Maximum key/value size: 65535 bytes each.

## API

```rust
impl PostingList {
    pub fn new() -> Self;
    pub fn from_sorted(entries: Vec<PostingEntry>) -> Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn contains_key(&self, key: &[u8]) -> bool;  // binary search
    pub fn encode(&self) -> Vec<u8>;
    pub fn encoded_size(&self) -> usize;
    pub fn decode(data: &[u8]) -> io::Result<Self>;
    pub fn union(&self, other: &PostingList) -> PostingList;      // merge, other wins on dup
    pub fn intersect(&self, other: &PostingList) -> PostingList;  // values from self
    pub fn difference(&self, other: &PostingList) -> PostingList;  // remove keys in other
    pub fn merge_pending(&self, inserts: &PostingList, deletes: &PostingList) -> PostingList;
}
```

## Usage Patterns

### GIN Index

Each term maps to a posting list in the posting B-tree:
- **Key**: term bytes (caller-defined encoding)
- **Value**: `PostingList` encoded bytes (inline in B-tree value if small, heap blob if large)
- **Posting entry key**: `doc_id[16] || inv_ts[8]`
- **Posting entry value**: empty

### Full-Text Search

Same as GIN, but posting entry values store term positions for phrase queries:
- **Posting entry value**: `position_count(u16 LE) || [position(u32 LE)...]`

### Pending Buffer

Fast-write GIN indexes use a pending inserts B-tree that accumulates entries before merging into the main posting tree. Each pending entry is a single posting list keyed by term.

## Tests

1. `empty_list_encode_decode` — empty roundtrip
2. `single_entry_roundtrip` — one entry
3. `multi_entry_roundtrip` — 10 entries
4. `encoded_size_matches` — encoded_size() == encode().len()
5. `decode_truncated_header` — error on <4 bytes
6. `decode_truncated_entry` — error on partial entry
7. `union_disjoint` — two non-overlapping lists
8. `union_overlapping` — overlapping keys, other wins
9. `union_empty` — one empty, one non-empty
10. `intersect_basic` — some matching keys
11. `intersect_disjoint` — no overlap → empty
12. `intersect_identical` — same list → same result
13. `difference_basic` — remove some keys
14. `difference_all` — remove all → empty
15. `difference_none` — nothing to remove → unchanged
16. `merge_pending` — inserts + deletes applied correctly
17. `contains_key_found` / `contains_key_missing`
18. `large_list_roundtrip` — 10,000 entries
19. `variable_length_keys` — different key sizes
20. `variable_length_values` — different value sizes
