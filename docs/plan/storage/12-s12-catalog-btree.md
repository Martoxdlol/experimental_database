# S12: Catalog B-Tree Schema

## Purpose

Define the key format and serialization for catalog entries stored in the catalog B-tree. The catalog B-tree is a regular B-tree (S6) with a well-known key schema for collection and index entries. Two B-trees: one keyed by ID (primary), one keyed by name (secondary).

**No MVCC, no domain awareness.** Just defines how to encode/decode catalog entries into bytes for B-tree storage.

## Dependencies

- **S6 (B-Tree)**: uses BTree for storage (but this module only defines the schema, not the tree itself)

## Rust Types

```rust
use crate::storage::backend::PageId;

/// Catalog entity types.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEntityType {
    Collection = 0x01,
    Index      = 0x02,
}

/// Index states.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogIndexState {
    Building = 0x01,
    Ready    = 0x02,
    Dropping = 0x03,
}

// ─── Collection Entry ───

/// Collection metadata stored in the catalog B-tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionEntry {
    pub collection_id: u64,
    pub name: String,
    pub primary_root_page: PageId,
    pub doc_count: u64,
}

// ─── Index Entry ───

/// Index metadata stored in the catalog B-tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub index_id: u64,
    pub collection_id: u64,
    pub name: String,
    /// Field paths as raw string segments (not FieldPath from L1).
    pub field_paths: Vec<Vec<String>>,
    pub root_page: PageId,
    pub state: CatalogIndexState,
}

// ─── Key Construction ───

/// Build a primary catalog key: entity_type[1] || entity_id[8] (big-endian).
/// Total: 9 bytes.
pub fn make_catalog_id_key(entity_type: CatalogEntityType, entity_id: u64) -> [u8; 9];

/// Build a secondary catalog key for name lookup:
/// entity_type[1] || name_bytes[var] || 0x00 (null terminator).
pub fn make_catalog_name_key(entity_type: CatalogEntityType, name: &str) -> Vec<u8>;

// ─── Serialization ───

/// Serialize a CollectionEntry to bytes (for B-tree value).
pub fn serialize_collection(entry: &CollectionEntry) -> Vec<u8>;

/// Deserialize a CollectionEntry from bytes.
pub fn deserialize_collection(data: &[u8]) -> Result<CollectionEntry>;

/// Serialize an IndexEntry to bytes (for B-tree value).
pub fn serialize_index(entry: &IndexEntry) -> Vec<u8>;

/// Deserialize an IndexEntry from bytes.
pub fn deserialize_index(data: &[u8]) -> Result<IndexEntry>;

/// Serialize a name → entity_id mapping (for the name B-tree value).
/// Just the entity_id as u64 big-endian (8 bytes).
pub fn serialize_name_value(entity_id: u64) -> [u8; 8];
pub fn deserialize_name_value(data: &[u8]) -> u64;

// ─── Scan Helpers ───
// Same entity_type prefix as ID tree — these are used in separate B-trees

/// Scan prefix for all collections in the ID B-tree.
pub fn collection_id_scan_prefix() -> [u8; 1];  // [0x01]

/// Scan prefix for all indexes in the ID B-tree.
pub fn index_id_scan_prefix() -> [u8; 1];  // [0x02]

/// Scan prefix for all collection names in the name B-tree.
pub fn collection_name_scan_prefix() -> [u8; 1];  // [0x01]

/// Scan prefix for all index names in the name B-tree.
pub fn index_name_scan_prefix() -> [u8; 1];  // [0x02]
```

## Serialization Format

### CollectionEntry Value

```
collection_id:         u64 LE    (8 bytes)
name_len:              u16 LE    (2 bytes)
name:                  [u8; name_len]
primary_root_page:     u32 LE    (4 bytes)
doc_count:             u64 LE    (8 bytes)
```

### IndexEntry Value

```
index_id:              u64 LE    (8 bytes)
collection_id:         u64 LE    (8 bytes)
name_len:              u16 LE    (2 bytes)
name:                  [u8; name_len]
field_count:           u8        (1 byte)
  For each field_path:
    segment_count:     u8        (1 byte)
    For each segment:
      seg_len:         u16 LE    (2 bytes)
      segment:         [u8; seg_len]
root_page:             u32 LE    (4 bytes)
state:                 u8        (1 byte)
```

### Key Format

**By-ID (primary) B-tree**:
```
Key: entity_type[1] || entity_id[8]
     0x01 || collection_id (big-endian u64)  — for collections
     0x02 || index_id (big-endian u64)       — for indexes
```

Big-endian for entity_id so memcmp ordering matches numeric ordering.

**By-Name (secondary) B-tree**:
```
Key: entity_type[1] || name_bytes[var] || 0x00
     0x01 || "users" || 0x00   — for collection named "users"
     0x02 || "email_idx" || 0x00 — for index named "email_idx"

Value: entity_id as u64 big-endian (8 bytes)
```

The null terminator ensures that "abc" sorts before "abcd" (correct string ordering with memcmp). No collection/index name may contain a null byte.

### Dual B-Tree Design

Each database has TWO catalog B-trees:

1. **By-ID B-tree**: primary lookup. Key = entity_type + entity_id. Value = full serialized entry.
2. **By-Name B-tree**: secondary lookup for name → id. Key = entity_type + name + 0x00. Value = entity_id.

The file header (page 0) stores the root pages of both B-trees: `catalog_root_page` for the by-ID B-tree and `catalog_name_root_page` for the by-Name B-tree (see S13 FileHeader).

### Startup Flow

1. Read file header → `catalog_root_page` (by-ID B-tree root) and `catalog_name_root_page` (by-Name B-tree root).
2. Open by-ID B-tree.
3. Open by-Name B-tree.
4. Scan both trees to populate in-memory CatalogCache (Layer 6).

### Create Collection Flow

1. `make_catalog_id_key(Collection, collection_id)` → key.
2. `serialize_collection(entry)` → value.
3. `id_btree.insert(key, value)`.
4. `make_catalog_name_key(Collection, name)` → name_key.
5. `serialize_name_value(collection_id)` → name_value.
6. `name_btree.insert(name_key, name_value)`.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| Deserialization error | Truncated or corrupt data | Return error |
| Name contains null byte | Invalid collection/index name | Return error (check before insert) |
| Unknown entity type | Corrupt catalog | Return error |
| Unknown index state | Corrupt catalog | Return error |

## Tests

1. **Collection key roundtrip**: make_catalog_id_key → parse back → same entity_type and id.
2. **Name key ordering**: Verify memcmp ordering matches string ordering for collection names.
3. **Collection serialize/deserialize**: Roundtrip a CollectionEntry through serialize + deserialize.
4. **Index serialize/deserialize**: Roundtrip an IndexEntry with compound field paths.
5. **Name key with null terminator**: Verify "abc" < "abcd" via memcmp on encoded keys.
6. **Cross-entity ordering**: Collection keys sort before index keys (0x01 < 0x02).
7. **Dual B-tree insert + lookup**: Insert a collection into both ID and Name B-trees. Look up by ID → full entry. Look up by name → get ID.
8. **Scan all collections**: Insert 5 collections + 3 indexes. Scan with collection prefix → get exactly 5 entries.
9. **Scan all indexes**: Same setup, scan with index prefix → get exactly 3 entries.
10. **Empty fields**: IndexEntry with empty field_paths → serializes/deserializes correctly.
11. **Unicode names**: Collection name with Unicode characters → roundtrip correctly.
12. **Name value roundtrip**: serialize_name_value / deserialize_name_value.
