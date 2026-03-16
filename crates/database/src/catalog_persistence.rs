//! Persistent catalog storage using B-trees.
//!
//! Collections and indexes are stored in the catalog B-tree (rooted at
//! `FileHeader::catalog_root_page`). This ensures catalog state survives
//! checkpoint + reopen cycles.
//!
//! ## Key format (catalog ID B-tree)
//!
//! - Collection: `[0x01][collection_id as u64 BE]`
//! - Index:      `[0x02][index_id as u64 BE]`
//!
//! ## Value format
//!
//! Collection: `[name_len:u32 LE][name bytes][primary_root_page:u32 LE]`
//! Index:      `[collection_id:u64 LE][btree_root_page:u32 LE][name_len:u32 LE][name bytes][field_paths...]`
//!   where field_paths = `[count:u32 LE]{ [seg_count:u32 LE]{ [seg_len:u32 LE][seg bytes] } }`

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId};
use exdb_storage::engine::BTreeHandle;
use std::io;

const TAG_COLLECTION: u8 = 0x01;
const TAG_INDEX: u8 = 0x02;

/// A recovered catalog entry from the B-tree.
#[derive(Debug)]
pub(crate) enum CatalogEntry {
    Collection {
        collection_id: CollectionId,
        name: String,
        primary_root_page: u32,
    },
    Index {
        index_id: IndexId,
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        btree_root_page: u32,
    },
}

// ── Key construction ──

fn collection_key(id: CollectionId) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = TAG_COLLECTION;
    key[1..9].copy_from_slice(&id.0.to_be_bytes());
    key
}

fn index_key(id: IndexId) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = TAG_INDEX;
    key[1..9].copy_from_slice(&id.0.to_be_bytes());
    key
}

// ── Value serialization ──

fn serialize_collection(name: &str, primary_root_page: u32) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let mut buf = Vec::with_capacity(4 + name_bytes.len() + 4);
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&primary_root_page.to_le_bytes());
    buf
}

fn deserialize_collection(id: CollectionId, data: &[u8]) -> io::Result<CatalogEntry> {
    if data.len() < 8 {
        return Err(io::Error::other("catalog collection entry too short"));
    }
    let name_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + name_len + 4 {
        return Err(io::Error::other("catalog collection entry truncated"));
    }
    let name = std::str::from_utf8(&data[4..4 + name_len])
        .map_err(|e| io::Error::other(format!("invalid collection name: {e}")))?
        .to_string();
    let primary_root_page =
        u32::from_le_bytes(data[4 + name_len..4 + name_len + 4].try_into().unwrap());
    Ok(CatalogEntry::Collection {
        collection_id: id,
        name,
        primary_root_page,
    })
}

fn serialize_index(
    collection_id: CollectionId,
    name: &str,
    field_paths: &[FieldPath],
    btree_root_page: u32,
) -> Vec<u8> {
    let name_bytes = name.as_bytes();
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&collection_id.0.to_le_bytes());
    buf.extend_from_slice(&btree_root_page.to_le_bytes());
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&(field_paths.len() as u32).to_le_bytes());
    for fp in field_paths {
        let segs = fp.segments();
        buf.extend_from_slice(&(segs.len() as u32).to_le_bytes());
        for seg in segs {
            let seg_bytes = seg.as_bytes();
            buf.extend_from_slice(&(seg_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(seg_bytes);
        }
    }
    buf
}

fn deserialize_index(id: IndexId, data: &[u8]) -> io::Result<CatalogEntry> {
    let err = |msg: &str| io::Error::other(format!("catalog index entry: {msg}"));
    if data.len() < 20 {
        return Err(err("too short"));
    }
    let mut pos = 0;

    let collection_id = CollectionId(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
    pos += 8;

    let btree_root_page = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if data.len() < pos + name_len + 4 {
        return Err(err("truncated at name"));
    }
    let name = std::str::from_utf8(&data[pos..pos + name_len])
        .map_err(|e| io::Error::other(format!("invalid index name: {e}")))?
        .to_string();
    pos += name_len;

    let fp_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut field_paths = Vec::with_capacity(fp_count);
    for _ in 0..fp_count {
        if data.len() < pos + 4 {
            return Err(err("truncated at field_path segment count"));
        }
        let seg_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let mut segments = Vec::with_capacity(seg_count);
        for _ in 0..seg_count {
            if data.len() < pos + 4 {
                return Err(err("truncated at segment length"));
            }
            let seg_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if data.len() < pos + seg_len {
                return Err(err("truncated at segment data"));
            }
            let seg = std::str::from_utf8(&data[pos..pos + seg_len])
                .map_err(|e| io::Error::other(format!("invalid segment: {e}")))?
                .to_string();
            pos += seg_len;
            segments.push(seg);
        }
        field_paths.push(FieldPath::new(segments));
    }

    Ok(CatalogEntry::Index {
        index_id: id,
        collection_id,
        name,
        field_paths,
        btree_root_page,
    })
}

// ── Public write operations ──

/// Write a collection entry to the catalog B-tree.
pub(crate) async fn write_collection(
    btree: &BTreeHandle,
    collection_id: CollectionId,
    name: &str,
    primary_root_page: u32,
) -> io::Result<()> {
    let key = collection_key(collection_id);
    let value = serialize_collection(name, primary_root_page);
    btree.insert(&key, &value).await
}

/// Write an index entry to the catalog B-tree.
pub(crate) async fn write_index(
    btree: &BTreeHandle,
    index_id: IndexId,
    collection_id: CollectionId,
    name: &str,
    field_paths: &[FieldPath],
    btree_root_page: u32,
) -> io::Result<()> {
    let key = index_key(index_id);
    let value = serialize_index(collection_id, name, field_paths, btree_root_page);
    btree.insert(&key, &value).await
}

/// Remove a collection entry from the catalog B-tree.
pub(crate) async fn remove_collection(
    btree: &BTreeHandle,
    collection_id: CollectionId,
) -> io::Result<()> {
    let key = collection_key(collection_id);
    btree.delete(&key).await?;
    Ok(())
}

/// Remove an index entry from the catalog B-tree.
pub(crate) async fn remove_index(btree: &BTreeHandle, index_id: IndexId) -> io::Result<()> {
    let key = index_key(index_id);
    btree.delete(&key).await?;
    Ok(())
}

// ── Public read operations ──

/// Scan the entire catalog B-tree and return all entries.
pub(crate) async fn scan_catalog(btree: &BTreeHandle) -> io::Result<Vec<CatalogEntry>> {
    use std::ops::Bound;
    use tokio_stream::StreamExt;

    let mut entries = Vec::new();
    let stream = btree.scan(Bound::Unbounded, Bound::Unbounded, exdb_storage::btree::ScanDirection::Forward);
    tokio::pin!(stream);

    while let Some(result) = stream.next().await {
        let (key, value): (Vec<u8>, Vec<u8>) = result?;
        if key.is_empty() {
            continue;
        }
        match key[0] {
            TAG_COLLECTION => {
                if key.len() != 9 {
                    continue;
                }
                let id = CollectionId(u64::from_be_bytes(key[1..9].try_into().unwrap()));
                entries.push(deserialize_collection(id, &value)?);
            }
            TAG_INDEX => {
                if key.len() != 9 {
                    continue;
                }
                let id = IndexId(u64::from_be_bytes(key[1..9].try_into().unwrap()));
                entries.push(deserialize_index(id, &value)?);
            }
            _ => {
                // Unknown tag — skip
            }
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    async fn make_btree() -> (std::sync::Arc<StorageEngine>, BTreeHandle) {
        let storage = std::sync::Arc::new(
            StorageEngine::open_in_memory(StorageConfig::default())
                .await
                .unwrap(),
        );
        let btree = storage.create_btree().await.unwrap();
        (storage, btree)
    }

    #[tokio::test]
    async fn roundtrip_collection() {
        let (_storage, btree) = make_btree().await;

        write_collection(&btree, CollectionId(10), "users", 42).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Collection { collection_id, name, primary_root_page } => {
                assert_eq!(*collection_id, CollectionId(10));
                assert_eq!(name, "users");
                assert_eq!(*primary_root_page, 42);
            }
            _ => panic!("expected Collection"),
        }
    }

    #[tokio::test]
    async fn roundtrip_index() {
        let (_storage, btree) = make_btree().await;

        let fps = vec![
            FieldPath::single("email"),
        ];
        write_index(&btree, IndexId(20), CollectionId(10), "by_email", &fps, 99).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Index { index_id, collection_id, name, field_paths, btree_root_page } => {
                assert_eq!(*index_id, IndexId(20));
                assert_eq!(*collection_id, CollectionId(10));
                assert_eq!(name, "by_email");
                assert_eq!(field_paths.len(), 1);
                assert_eq!(field_paths[0].segments(), &["email"]);
                assert_eq!(*btree_root_page, 99);
            }
            _ => panic!("expected Index"),
        }
    }

    #[tokio::test]
    async fn roundtrip_compound_index() {
        let (_storage, btree) = make_btree().await;

        let fps = vec![
            FieldPath::new(vec!["address".to_string(), "city".to_string()]),
            FieldPath::single("age"),
        ];
        write_index(&btree, IndexId(30), CollectionId(10), "by_city_age", &fps, 55).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Index { field_paths, .. } => {
                assert_eq!(field_paths.len(), 2);
                assert_eq!(field_paths[0].segments(), &["address", "city"]);
                assert_eq!(field_paths[1].segments(), &["age"]);
            }
            _ => panic!("expected Index"),
        }
    }

    #[tokio::test]
    async fn multiple_entries_sorted() {
        let (_storage, btree) = make_btree().await;

        // Collections come before indexes (TAG_COLLECTION < TAG_INDEX)
        write_collection(&btree, CollectionId(10), "users", 1).await.unwrap();
        write_collection(&btree, CollectionId(11), "orders", 2).await.unwrap();
        write_index(&btree, IndexId(20), CollectionId(10), "_created_at", &[FieldPath::single("_created_at")], 3).await.unwrap();
        write_index(&btree, IndexId(21), CollectionId(10), "by_email", &[FieldPath::single("email")], 4).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 4);

        // First two should be collections
        assert!(matches!(&entries[0], CatalogEntry::Collection { .. }));
        assert!(matches!(&entries[1], CatalogEntry::Collection { .. }));
        // Last two should be indexes
        assert!(matches!(&entries[2], CatalogEntry::Index { .. }));
        assert!(matches!(&entries[3], CatalogEntry::Index { .. }));
    }

    #[tokio::test]
    async fn remove_collection_entry() {
        let (_storage, btree) = make_btree().await;

        write_collection(&btree, CollectionId(10), "users", 1).await.unwrap();
        write_collection(&btree, CollectionId(11), "orders", 2).await.unwrap();

        remove_collection(&btree, CollectionId(10)).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Collection { name, .. } => assert_eq!(name, "orders"),
            _ => panic!("expected Collection"),
        }
    }

    #[tokio::test]
    async fn remove_index_entry() {
        let (_storage, btree) = make_btree().await;

        write_index(&btree, IndexId(20), CollectionId(10), "by_email", &[FieldPath::single("email")], 3).await.unwrap();
        write_index(&btree, IndexId(21), CollectionId(10), "by_age", &[FieldPath::single("age")], 4).await.unwrap();

        remove_index(&btree, IndexId(20)).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Index { name, .. } => assert_eq!(name, "by_age"),
            _ => panic!("expected Index"),
        }
    }

    #[tokio::test]
    async fn empty_catalog_scan() {
        let (_storage, btree) = make_btree().await;
        let entries = scan_catalog(&btree).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn overwrite_collection_entry() {
        let (_storage, btree) = make_btree().await;

        write_collection(&btree, CollectionId(10), "users", 1).await.unwrap();
        // Overwrite with new root page
        write_collection(&btree, CollectionId(10), "users", 99).await.unwrap();

        let entries = scan_catalog(&btree).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            CatalogEntry::Collection { primary_root_page, .. } => {
                assert_eq!(*primary_root_page, 99);
            }
            _ => panic!("expected Collection"),
        }
    }
}
