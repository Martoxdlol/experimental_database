//! Catalog B-tree key format and serialization.
//!
//! Defines the key schema and entry serialization for catalog entries stored
//! in B-trees. Two B-trees form the catalog: one keyed by ID (primary), one
//! keyed by name (secondary).
//!
//! No MVCC, no domain awareness. Just defines how to encode/decode catalog
//! entries into bytes for B-tree storage.

use crate::backend::PageId;
use std::io;

// ─── Entity Types ───

/// Catalog entity types.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEntityType {
    Collection = 0x01,
    Index = 0x02,
}

impl CatalogEntityType {
    pub fn from_u8(val: u8) -> io::Result<Self> {
        match val {
            0x01 => Ok(CatalogEntityType::Collection),
            0x02 => Ok(CatalogEntityType::Index),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown catalog entity type: 0x{:02x}", val),
            )),
        }
    }
}

// ─── Index States ───

/// Index states.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogIndexState {
    Building = 0x01,
    Ready = 0x02,
    Dropping = 0x03,
}

impl CatalogIndexState {
    fn from_u8(val: u8) -> io::Result<Self> {
        match val {
            0x01 => Ok(CatalogIndexState::Building),
            0x02 => Ok(CatalogIndexState::Ready),
            0x03 => Ok(CatalogIndexState::Dropping),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown catalog index state: 0x{:02x}", val),
            )),
        }
    }
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
pub fn make_catalog_id_key(entity_type: CatalogEntityType, entity_id: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = entity_type as u8;
    key[1..9].copy_from_slice(&entity_id.to_be_bytes());
    key
}

/// Build a secondary catalog key for name lookup:
/// entity_type[1] || name_bytes[var] || 0x00 (null terminator).
pub fn make_catalog_name_key(entity_type: CatalogEntityType, name: &str) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let mut key = Vec::with_capacity(1 + name_bytes.len() + 1);
    key.push(entity_type as u8);
    key.extend_from_slice(name_bytes);
    key.push(0x00);
    key
}

// ─── Serialization ───

/// Serialize a CollectionEntry to bytes (for B-tree value).
///
/// Format: collection_id(u64 LE) || name_len(u16 LE) || name || primary_root_page(u32 LE) || doc_count(u64 LE)
pub fn serialize_collection(entry: &CollectionEntry) -> Vec<u8> {
    let name_bytes = entry.name.as_bytes();
    let name_len = name_bytes.len() as u16;
    let total = 8 + 2 + name_bytes.len() + 4 + 8;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&entry.collection_id.to_le_bytes());
    buf.extend_from_slice(&name_len.to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&entry.primary_root_page.to_le_bytes());
    buf.extend_from_slice(&entry.doc_count.to_le_bytes());
    buf
}

/// Deserialize a CollectionEntry from bytes.
pub fn deserialize_collection(data: &[u8]) -> io::Result<CollectionEntry> {
    // Minimum size: 8 (collection_id) + 2 (name_len) + 0 (name) + 4 (root) + 8 (doc_count) = 22
    if data.len() < 22 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "collection entry too short",
        ));
    }

    let mut offset = 0;

    let collection_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if data.len() < offset + name_len + 4 + 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "collection entry truncated at name",
        ));
    }

    let name = String::from_utf8(data[offset..offset + name_len].to_vec()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "collection name is not valid UTF-8",
        )
    })?;
    offset += name_len;

    let primary_root_page =
        u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let doc_count = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

    Ok(CollectionEntry {
        collection_id,
        name,
        primary_root_page,
        doc_count,
    })
}

/// Serialize an IndexEntry to bytes (for B-tree value).
///
/// Format: index_id(u64 LE) || collection_id(u64 LE) || name_len(u16 LE) || name
///       || field_count(u8) || [segment_count(u8) || [seg_len(u16 LE) || segment]...]
///       || root_page(u32 LE) || state(u8)
pub fn serialize_index(entry: &IndexEntry) -> Vec<u8> {
    let name_bytes = entry.name.as_bytes();
    let name_len = name_bytes.len() as u16;

    let mut buf = Vec::new();
    buf.extend_from_slice(&entry.index_id.to_le_bytes());
    buf.extend_from_slice(&entry.collection_id.to_le_bytes());
    buf.extend_from_slice(&name_len.to_le_bytes());
    buf.extend_from_slice(name_bytes);

    buf.push(entry.field_paths.len() as u8);
    for field_path in &entry.field_paths {
        buf.push(field_path.len() as u8);
        for segment in field_path {
            let seg_bytes = segment.as_bytes();
            buf.extend_from_slice(&(seg_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(seg_bytes);
        }
    }

    buf.extend_from_slice(&entry.root_page.to_le_bytes());
    buf.push(entry.state as u8);

    buf
}

/// Deserialize an IndexEntry from bytes.
pub fn deserialize_index(data: &[u8]) -> io::Result<IndexEntry> {
    // Minimum: 8 + 8 + 2 + 0 + 1 + 4 + 1 = 24
    if data.len() < 24 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index entry too short",
        ));
    }

    let mut offset = 0;

    let index_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let collection_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if data.len() < offset + name_len + 1 + 4 + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index entry truncated at name",
        ));
    }

    let name = String::from_utf8(data[offset..offset + name_len].to_vec()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "index name is not valid UTF-8",
        )
    })?;
    offset += name_len;

    if offset >= data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index entry truncated at field_count",
        ));
    }

    let field_count = data[offset] as usize;
    offset += 1;

    let mut field_paths = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        if offset >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index entry truncated at segment_count",
            ));
        }
        let segment_count = data[offset] as usize;
        offset += 1;

        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            if offset + 2 > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index entry truncated at seg_len",
                ));
            }
            let seg_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;

            if offset + seg_len > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index entry truncated at segment data",
                ));
            }
            let segment =
                String::from_utf8(data[offset..offset + seg_len].to_vec()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "index field path segment is not valid UTF-8",
                    )
                })?;
            offset += seg_len;
            segments.push(segment);
        }
        field_paths.push(segments);
    }

    if offset + 4 + 1 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index entry truncated at root_page/state",
        ));
    }

    let root_page = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
    offset += 4;

    let state = CatalogIndexState::from_u8(data[offset])?;

    Ok(IndexEntry {
        index_id,
        collection_id,
        name,
        field_paths,
        root_page,
        state,
    })
}

/// Serialize a name -> entity_id mapping (for the name B-tree value).
/// Just the entity_id as u64 big-endian (8 bytes).
pub fn serialize_name_value(entity_id: u64) -> [u8; 8] {
    entity_id.to_be_bytes()
}

/// Deserialize a name -> entity_id mapping from bytes.
pub fn deserialize_name_value(data: &[u8]) -> u64 {
    u64::from_be_bytes(data[0..8].try_into().unwrap())
}

// ─── Scan Helpers ───

/// Scan prefix for all collections in the ID B-tree.
pub fn collection_id_scan_prefix() -> [u8; 1] {
    [0x01]
}

/// Scan prefix for all indexes in the ID B-tree.
pub fn index_id_scan_prefix() -> [u8; 1] {
    [0x02]
}

/// Scan prefix for all collection names in the name B-tree.
pub fn collection_name_scan_prefix() -> [u8; 1] {
    [0x01]
}

/// Scan prefix for all index names in the name B-tree.
pub fn index_name_scan_prefix() -> [u8; 1] {
    [0x02]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, PageStorage};
    use crate::btree::{BTree, ScanDirection};
    use crate::buffer_pool::{BufferPool, BufferPoolConfig};
    use crate::free_list::FreeList;
    use std::ops::Bound;
    use std::sync::Arc;

    const PAGE_SIZE: usize = 4096;

    fn setup() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(256).unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 128,
            },
            storage,
        ));
        let free_list = FreeList::new(0, pool.clone());
        (pool, free_list)
    }

    // ─── Test 1: Collection key roundtrip ───
    // make_catalog_id_key -> parse back -> same entity_type and id.
    #[test]
    fn collection_key_roundtrip() {
        let entity_type = CatalogEntityType::Collection;
        let entity_id = 42u64;
        let key = make_catalog_id_key(entity_type, entity_id);

        assert_eq!(key.len(), 9);
        // Parse back.
        let parsed_type = CatalogEntityType::from_u8(key[0]).unwrap();
        let parsed_id = u64::from_be_bytes(key[1..9].try_into().unwrap());

        assert_eq!(parsed_type, entity_type);
        assert_eq!(parsed_id, entity_id);
    }

    // ─── Test 2: Name key ordering ───
    // Verify memcmp ordering matches string ordering for collection names.
    #[test]
    fn name_key_ordering() {
        let names = ["alpha", "bravo", "charlie", "delta"];
        let mut keys: Vec<Vec<u8>> = names
            .iter()
            .map(|n| make_catalog_name_key(CatalogEntityType::Collection, n))
            .collect();

        // Keys should already be in sorted order by memcmp.
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);

        // Reverse order should differ.
        keys.reverse();
        assert_ne!(keys, sorted_keys);
    }

    // ─── Test 3: Collection serialize/deserialize ───
    // Roundtrip a CollectionEntry through serialize + deserialize.
    #[test]
    fn collection_serialize_deserialize() {
        let entry = CollectionEntry {
            collection_id: 12345,
            name: "my_collection".to_string(),
            primary_root_page: 99,
            doc_count: 1000,
        };

        let data = serialize_collection(&entry);
        let decoded = deserialize_collection(&data).unwrap();

        assert_eq!(decoded, entry);
    }

    // ─── Test 4: Index serialize/deserialize ───
    // Roundtrip an IndexEntry with compound field paths.
    #[test]
    fn index_serialize_deserialize() {
        let entry = IndexEntry {
            index_id: 77,
            collection_id: 12345,
            name: "user_email_idx".to_string(),
            field_paths: vec![
                vec!["user".to_string(), "email".to_string()],
                vec!["profile".to_string(), "name".to_string(), "first".to_string()],
            ],
            root_page: 200,
            state: CatalogIndexState::Ready,
        };

        let data = serialize_index(&entry);
        let decoded = deserialize_index(&data).unwrap();

        assert_eq!(decoded, entry);
    }

    // ─── Test 5: Name key with null terminator ───
    // Verify "abc" < "abcd" via memcmp on encoded keys.
    #[test]
    fn name_key_null_terminator_ordering() {
        let key_abc = make_catalog_name_key(CatalogEntityType::Collection, "abc");
        let key_abcd = make_catalog_name_key(CatalogEntityType::Collection, "abcd");

        // "abc\0" should sort before "abcd\0" since '\0' < 'd'.
        assert!(key_abc < key_abcd);
    }

    // ─── Test 6: Cross-entity ordering ───
    // Collection keys sort before index keys (0x01 < 0x02).
    #[test]
    fn cross_entity_ordering() {
        let col_key = make_catalog_id_key(CatalogEntityType::Collection, 999);
        let idx_key = make_catalog_id_key(CatalogEntityType::Index, 1);

        assert!(col_key < idx_key, "collection keys (0x01) should sort before index keys (0x02)");
    }

    // ─── Test 7: Dual B-tree insert + lookup ───
    // Insert a collection into both ID and Name B-trees. Look up by ID -> full entry.
    // Look up by name -> get ID.
    #[test]
    fn dual_btree_insert_and_lookup() {
        let (pool, mut fl) = setup();

        let id_btree = BTree::create(pool.clone(), &mut fl).unwrap();
        let name_btree = BTree::create(pool.clone(), &mut fl).unwrap();

        let entry = CollectionEntry {
            collection_id: 42,
            name: "users".to_string(),
            primary_root_page: 10,
            doc_count: 500,
        };

        // Insert into ID B-tree.
        let id_key = make_catalog_id_key(CatalogEntityType::Collection, entry.collection_id);
        let id_value = serialize_collection(&entry);
        id_btree.insert(&id_key, &id_value, &mut fl).unwrap();

        // Insert into Name B-tree.
        let name_key = make_catalog_name_key(CatalogEntityType::Collection, &entry.name);
        let name_value = serialize_name_value(entry.collection_id);
        name_btree.insert(&name_key, &name_value, &mut fl).unwrap();

        // Look up by ID.
        let found_value = id_btree.get(&id_key).unwrap().expect("should find by ID");
        let found_entry = deserialize_collection(&found_value).unwrap();
        assert_eq!(found_entry, entry);

        // Look up by name.
        let found_name_value = name_btree
            .get(&name_key)
            .unwrap()
            .expect("should find by name");
        let found_id = deserialize_name_value(&found_name_value);
        assert_eq!(found_id, entry.collection_id);
    }

    // ─── Test 8: Scan all collections ───
    // Insert 5 collections + 3 indexes. Scan with collection prefix -> get exactly 5 entries.
    #[test]
    fn scan_all_collections() {
        let (pool, mut fl) = setup();
        let id_btree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Insert 5 collections.
        for i in 1u64..=5 {
            let entry = CollectionEntry {
                collection_id: i,
                name: format!("col_{}", i),
                primary_root_page: (i * 10) as PageId,
                doc_count: i * 100,
            };
            let key = make_catalog_id_key(CatalogEntityType::Collection, i);
            let value = serialize_collection(&entry);
            id_btree.insert(&key, &value, &mut fl).unwrap();
        }

        // Insert 3 indexes.
        for i in 1u64..=3 {
            let entry = IndexEntry {
                index_id: i,
                collection_id: 1,
                name: format!("idx_{}", i),
                field_paths: vec![vec!["field".to_string()]],
                root_page: (i * 20) as PageId,
                state: CatalogIndexState::Ready,
            };
            let key = make_catalog_id_key(CatalogEntityType::Index, i);
            let value = serialize_index(&entry);
            id_btree.insert(&key, &value, &mut fl).unwrap();
        }

        // Scan with collection prefix.
        let prefix = collection_id_scan_prefix();
        // Collection keys are 0x01 || id[8]. We scan from [0x01] to [0x02) (exclusive).
        let lower = Bound::Included(prefix.as_slice());
        let upper_prefix = index_id_scan_prefix();
        let upper = Bound::Excluded(upper_prefix.as_slice());

        let results: Vec<(Vec<u8>, Vec<u8>)> = id_btree
            .scan(lower, upper, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 5);

        // Verify all entries are collections.
        for (key, value) in &results {
            assert_eq!(key[0], CatalogEntityType::Collection as u8);
            let entry = deserialize_collection(value).unwrap();
            assert!(entry.name.starts_with("col_"));
        }
    }

    // ─── Test 9: Scan all indexes ───
    // Same setup, scan with index prefix -> get exactly 3 entries.
    #[test]
    fn scan_all_indexes() {
        let (pool, mut fl) = setup();
        let id_btree = BTree::create(pool.clone(), &mut fl).unwrap();

        // Insert 5 collections.
        for i in 1u64..=5 {
            let entry = CollectionEntry {
                collection_id: i,
                name: format!("col_{}", i),
                primary_root_page: (i * 10) as PageId,
                doc_count: i * 100,
            };
            let key = make_catalog_id_key(CatalogEntityType::Collection, i);
            let value = serialize_collection(&entry);
            id_btree.insert(&key, &value, &mut fl).unwrap();
        }

        // Insert 3 indexes.
        for i in 1u64..=3 {
            let entry = IndexEntry {
                index_id: i,
                collection_id: 1,
                name: format!("idx_{}", i),
                field_paths: vec![vec!["field".to_string()]],
                root_page: (i * 20) as PageId,
                state: CatalogIndexState::Ready,
            };
            let key = make_catalog_id_key(CatalogEntityType::Index, i);
            let value = serialize_index(&entry);
            id_btree.insert(&key, &value, &mut fl).unwrap();
        }

        // Scan with index prefix. Index keys start with 0x02.
        // We scan from [0x02] to [0x03) (exclusive).
        let prefix = index_id_scan_prefix();
        let lower = Bound::Included(prefix.as_slice());
        let upper_byte: [u8; 1] = [0x03];
        let upper = Bound::Excluded(upper_byte.as_slice());

        let results: Vec<(Vec<u8>, Vec<u8>)> = id_btree
            .scan(lower, upper, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(results.len(), 3);

        // Verify all entries are indexes.
        for (key, value) in &results {
            assert_eq!(key[0], CatalogEntityType::Index as u8);
            let entry = deserialize_index(value).unwrap();
            assert!(entry.name.starts_with("idx_"));
        }
    }

    // ─── Test 10: Empty fields ───
    // IndexEntry with empty field_paths -> serializes/deserializes correctly.
    #[test]
    fn empty_fields() {
        let entry = IndexEntry {
            index_id: 1,
            collection_id: 2,
            name: "empty_idx".to_string(),
            field_paths: vec![],
            root_page: 50,
            state: CatalogIndexState::Building,
        };

        let data = serialize_index(&entry);
        let decoded = deserialize_index(&data).unwrap();
        assert_eq!(decoded, entry);
    }

    // ─── Test 11: Unicode names ───
    // Collection name with Unicode characters -> roundtrip correctly.
    #[test]
    fn unicode_names() {
        let entry = CollectionEntry {
            collection_id: 999,
            name: "usuarios_\u{00e9}\u{00e8}\u{00ea}".to_string(),
            primary_root_page: 42,
            doc_count: 0,
        };

        let data = serialize_collection(&entry);
        let decoded = deserialize_collection(&data).unwrap();
        assert_eq!(decoded, entry);

        // Also test name key with unicode.
        let key = make_catalog_name_key(CatalogEntityType::Collection, &entry.name);
        assert_eq!(key[0], CatalogEntityType::Collection as u8);
        assert_eq!(*key.last().unwrap(), 0x00); // null terminator
        // Name bytes should be in the middle.
        let name_bytes = &key[1..key.len() - 1];
        assert_eq!(name_bytes, entry.name.as_bytes());
    }

    // ─── Test 12: Name value roundtrip ───
    // serialize_name_value / deserialize_name_value.
    #[test]
    fn name_value_roundtrip() {
        for id in [0u64, 1, 42, u64::MAX, 0xDEAD_BEEF_CAFE_BABEu64] {
            let encoded = serialize_name_value(id);
            let decoded = deserialize_name_value(&encoded);
            assert_eq!(decoded, id);
        }
    }
}
