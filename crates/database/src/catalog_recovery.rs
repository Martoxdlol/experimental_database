//! Catalog recovery — WAL replay handler and catalog data deserialization.
//!
//! During `Database::open()`, the storage engine replays WAL records.
//! This module provides the handler that reconstructs the catalog cache
//! and tracks the highest committed timestamp for restart.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, IndexId, Ts};
use exdb_docstore::PrimaryIndex;
use exdb_storage::engine::StorageEngine;
use exdb_storage::recovery::WalRecordHandler;
use exdb_storage::wal::{WalRecord, WAL_RECORD_TX_COMMIT, WAL_RECORD_VISIBLE_TS};

use crate::catalog_cache::{CatalogCache, CollectionMeta, IndexMeta, IndexState};

/// Parsed catalog mutation from WAL replay.
#[derive(Debug, Clone)]
pub(crate) enum RecoveredCatalogMutation {
    CreateCollection {
        name: String,
        provisional_id: CollectionId,
    },
    DropCollection {
        collection_id: CollectionId,
        #[allow(dead_code)]
        name: String,
    },
    CreateIndex {
        collection_id: CollectionId,
        name: String,
        field_paths: Vec<FieldPath>,
        provisional_id: IndexId,
    },
    DropIndex {
        #[allow(dead_code)]
        collection_id: CollectionId,
        index_id: IndexId,
        #[allow(dead_code)]
        name: String,
    },
}

/// WAL replay handler for database recovery.
///
/// Tracks the highest committed timestamp and collects catalog mutations
/// that need to be applied to rebuild the catalog cache.
pub(crate) struct CatalogRecoveryHandler {
    /// Highest commit_ts seen during replay.
    pub recovered_ts: Ts,
    /// Last visible_ts from WAL_RECORD_VISIBLE_TS records.
    pub visible_ts: Ts,
    /// All catalog mutations in WAL order (for rebuilding catalog).
    pub catalog_mutations: Vec<(Ts, Vec<RecoveredCatalogMutation>)>,
}

impl CatalogRecoveryHandler {
    pub fn new() -> Self {
        Self {
            recovered_ts: 0,
            visible_ts: 0,
            catalog_mutations: Vec::new(),
        }
    }
}

#[async_trait]
impl WalRecordHandler for CatalogRecoveryHandler {
    async fn handle_record(&mut self, record: &WalRecord) -> std::io::Result<()> {
        match record.record_type {
            WAL_RECORD_TX_COMMIT => {
                let (commit_ts, _mutations, catalog_data) =
                    exdb_tx::deserialize_wal_payload(&record.payload).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                    })?;

                if commit_ts > self.recovered_ts {
                    self.recovered_ts = commit_ts;
                }

                // Parse catalog mutations
                let cat_muts = deserialize_catalog_data(&catalog_data).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;

                if !cat_muts.is_empty() {
                    self.catalog_mutations.push((commit_ts, cat_muts));
                }
            }
            WAL_RECORD_VISIBLE_TS => {
                if record.payload.len() >= 8 {
                    let ts = u64::from_le_bytes(
                        record.payload[0..8].try_into().unwrap(),
                    );
                    if ts > self.visible_ts {
                        self.visible_ts = ts;
                    }
                }
            }
            _ => {
                // Other record types (checkpoint, vacuum, etc.) — skip
            }
        }
        Ok(())
    }
}

/// Parse the catalog portion of a WAL payload.
pub(crate) fn deserialize_catalog_data(
    data: &[u8],
) -> Result<Vec<RecoveredCatalogMutation>, String> {
    if data.len() < 4 {
        return Ok(Vec::new());
    }

    let catalog_count =
        u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    let mut result = Vec::with_capacity(catalog_count);

    for _ in 0..catalog_count {
        if offset >= data.len() {
            return Err("truncated catalog data".into());
        }
        let type_tag = data[offset];
        offset += 1;

        match type_tag {
            0x01 => {
                // CreateCollection
                if offset + 12 > data.len() {
                    return Err("truncated CreateCollection".into());
                }
                let provisional_id =
                    CollectionId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let name_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + name_len > data.len() {
                    return Err("truncated CreateCollection name".into());
                }
                let name =
                    String::from_utf8(data[offset..offset + name_len].to_vec())
                        .map_err(|e| format!("invalid UTF-8 in collection name: {e}"))?;
                offset += name_len;
                result.push(RecoveredCatalogMutation::CreateCollection {
                    name,
                    provisional_id,
                });
            }
            0x02 => {
                // DropCollection
                if offset + 12 > data.len() {
                    return Err("truncated DropCollection".into());
                }
                let collection_id =
                    CollectionId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let name_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + name_len > data.len() {
                    return Err("truncated DropCollection name".into());
                }
                let name =
                    String::from_utf8(data[offset..offset + name_len].to_vec())
                        .map_err(|e| format!("invalid UTF-8: {e}"))?;
                offset += name_len;
                result.push(RecoveredCatalogMutation::DropCollection {
                    collection_id,
                    name,
                });
            }
            0x03 => {
                // CreateIndex
                if offset + 20 > data.len() {
                    return Err("truncated CreateIndex".into());
                }
                let provisional_id =
                    IndexId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let collection_id =
                    CollectionId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let name_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + name_len > data.len() {
                    return Err("truncated CreateIndex name".into());
                }
                let name =
                    String::from_utf8(data[offset..offset + name_len].to_vec())
                        .map_err(|e| format!("invalid UTF-8: {e}"))?;
                offset += name_len;

                if offset + 4 > data.len() {
                    return Err("truncated field_paths count".into());
                }
                let field_paths_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                let mut field_paths = Vec::with_capacity(field_paths_len);
                for _ in 0..field_paths_len {
                    if offset + 4 > data.len() {
                        return Err("truncated segments count".into());
                    }
                    let segments_len = u32::from_le_bytes(
                        data[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    offset += 4;

                    let mut segments = Vec::with_capacity(segments_len);
                    for _ in 0..segments_len {
                        if offset + 4 > data.len() {
                            return Err("truncated segment len".into());
                        }
                        let seg_len = u32::from_le_bytes(
                            data[offset..offset + 4].try_into().unwrap(),
                        ) as usize;
                        offset += 4;
                        if offset + seg_len > data.len() {
                            return Err("truncated segment data".into());
                        }
                        let seg =
                            String::from_utf8(data[offset..offset + seg_len].to_vec())
                                .map_err(|e| format!("invalid UTF-8: {e}"))?;
                        offset += seg_len;
                        segments.push(seg);
                    }
                    field_paths.push(FieldPath::new(segments));
                }

                result.push(RecoveredCatalogMutation::CreateIndex {
                    collection_id,
                    name,
                    field_paths,
                    provisional_id,
                });
            }
            0x04 => {
                // DropIndex
                if offset + 20 > data.len() {
                    return Err("truncated DropIndex".into());
                }
                let index_id =
                    IndexId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let collection_id =
                    CollectionId(u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()));
                offset += 8;
                let name_len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + name_len > data.len() {
                    return Err("truncated DropIndex name".into());
                }
                let name =
                    String::from_utf8(data[offset..offset + name_len].to_vec())
                        .map_err(|e| format!("invalid UTF-8: {e}"))?;
                offset += name_len;
                result.push(RecoveredCatalogMutation::DropIndex {
                    collection_id,
                    index_id,
                    name,
                });
            }
            other => {
                return Err(format!("unknown catalog mutation type tag: {other:#x}"));
            }
        }
    }

    Ok(result)
}

/// Apply recovered catalog mutations to rebuild the catalog cache.
///
/// Also allocates B-trees for any collections/indexes that were created
/// during WAL replay (since those B-trees already exist on disk, we
/// use `open_btree` with root page 0 as a placeholder — in practice,
/// the B-tree data is already in the buffer pool from WAL replay).
pub(crate) async fn apply_recovered_catalog(
    catalog: &mut CatalogCache,
    mutations: &[(Ts, Vec<RecoveredCatalogMutation>)],
    storage: &Arc<StorageEngine>,
    primary_indexes: &mut HashMap<CollectionId, Arc<PrimaryIndex>>,
    secondary_indexes: &mut HashMap<IndexId, Arc<exdb_docstore::SecondaryIndex>>,
) -> Result<(), String> {
    for (_commit_ts, cat_muts) in mutations {
        for m in cat_muts {
            match m {
                RecoveredCatalogMutation::CreateCollection {
                    name,
                    provisional_id,
                } => {
                    // The B-tree was created during the original commit.
                    // During WAL replay, the primary index mutations were replayed,
                    // so the B-tree pages are already in the buffer pool.
                    // We need to create a new B-tree handle for this collection.
                    let btree = storage.create_btree().await.map_err(|e| e.to_string())?;
                    let root_page = btree.root_page();
                    let primary = Arc::new(PrimaryIndex::new(
                        btree,
                        Arc::clone(storage),
                        storage.config().page_size / 4,
                    ));
                    primary_indexes.insert(*provisional_id, Arc::clone(&primary));

                    if catalog.get_collection_by_id(*provisional_id).is_none() {
                        catalog.add_collection(CollectionMeta {
                            collection_id: *provisional_id,
                            name: name.clone(),
                            primary_root_page: root_page,
                            doc_count: 0,
                        });
                    }
                }
                RecoveredCatalogMutation::DropCollection {
                    collection_id,
                    ..
                } => {
                    catalog.remove_collection(*collection_id);
                    primary_indexes.remove(collection_id);
                    // Secondary indexes for this collection are cascaded by catalog.remove_collection
                }
                RecoveredCatalogMutation::CreateIndex {
                    collection_id,
                    name,
                    field_paths,
                    provisional_id,
                } => {
                    let btree = storage.create_btree().await.map_err(|e| e.to_string())?;
                    let root_page = btree.root_page();

                    if let Some(primary) = primary_indexes.get(collection_id) {
                        let secondary = Arc::new(exdb_docstore::SecondaryIndex::new(
                            btree,
                            Arc::clone(primary),
                        ));
                        secondary_indexes.insert(*provisional_id, secondary);
                    }

                    if catalog.get_index_by_id(*provisional_id).is_none() {
                        catalog.add_index(IndexMeta {
                            index_id: *provisional_id,
                            collection_id: *collection_id,
                            name: name.clone(),
                            field_paths: field_paths.clone(),
                            root_page,
                            state: IndexState::Ready,
                        });
                    }
                }
                RecoveredCatalogMutation::DropIndex {
                    index_id,
                    ..
                } => {
                    catalog.remove_index(*index_id);
                    secondary_indexes.remove(index_id);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_empty_catalog_data() {
        // Just 4 bytes: count = 0
        let data = 0u32.to_le_bytes();
        let result = deserialize_catalog_data(&data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn deserialize_too_short_returns_empty() {
        let result = deserialize_catalog_data(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn roundtrip_create_collection() {
        // Simulate what serialize_wal_payload writes for catalog mutations
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // catalog_count = 1
        buf.push(0x01); // CreateCollection
        buf.extend_from_slice(&42u64.to_le_bytes()); // provisional_id
        let name = "users";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            RecoveredCatalogMutation::CreateCollection {
                name,
                provisional_id,
            } => {
                assert_eq!(name, "users");
                assert_eq!(*provisional_id, CollectionId(42));
            }
            _ => panic!("expected CreateCollection"),
        }
    }

    #[test]
    fn roundtrip_drop_collection() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x02); // DropCollection
        buf.extend_from_slice(&42u64.to_le_bytes()); // collection_id
        let name = "users";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            RecoveredCatalogMutation::DropCollection {
                collection_id,
                name,
            } => {
                assert_eq!(*collection_id, CollectionId(42));
                assert_eq!(name, "users");
            }
            _ => panic!("expected DropCollection"),
        }
    }

    #[test]
    fn roundtrip_create_index() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x03); // CreateIndex
        buf.extend_from_slice(&100u64.to_le_bytes()); // provisional_id
        buf.extend_from_slice(&42u64.to_le_bytes()); // collection_id
        let name = "by_email";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        // field_paths: 1 path with 1 segment "email"
        buf.extend_from_slice(&1u32.to_le_bytes()); // field_paths_len
        buf.extend_from_slice(&1u32.to_le_bytes()); // segments_len
        let seg = "email";
        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(seg.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            RecoveredCatalogMutation::CreateIndex {
                collection_id,
                name,
                field_paths,
                provisional_id,
            } => {
                assert_eq!(*provisional_id, IndexId(100));
                assert_eq!(*collection_id, CollectionId(42));
                assert_eq!(name, "by_email");
                assert_eq!(field_paths.len(), 1);
                assert_eq!(field_paths[0].segments(), &["email"]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn roundtrip_drop_index() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x04); // DropIndex
        buf.extend_from_slice(&100u64.to_le_bytes()); // index_id
        buf.extend_from_slice(&42u64.to_le_bytes()); // collection_id
        let name = "by_email";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        assert_eq!(result.len(), 1);
        match &result[0] {
            RecoveredCatalogMutation::DropIndex {
                index_id,
                collection_id,
                name,
            } => {
                assert_eq!(*index_id, IndexId(100));
                assert_eq!(*collection_id, CollectionId(42));
                assert_eq!(name, "by_email");
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn roundtrip_multiple_mutations() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&2u32.to_le_bytes()); // 2 mutations

        // 1. CreateCollection
        buf.push(0x01);
        buf.extend_from_slice(&10u64.to_le_bytes());
        let name = "users";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());

        // 2. CreateIndex for _created_at
        buf.push(0x03);
        buf.extend_from_slice(&20u64.to_le_bytes()); // index id
        buf.extend_from_slice(&10u64.to_le_bytes()); // collection id
        let idx_name = "_created_at";
        buf.extend_from_slice(&(idx_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(idx_name.as_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 field path
        buf.extend_from_slice(&1u32.to_le_bytes()); // 1 segment
        let seg = "_created_at";
        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(seg.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        assert_eq!(result.len(), 2);
        assert!(matches!(
            &result[0],
            RecoveredCatalogMutation::CreateCollection { .. }
        ));
        assert!(matches!(
            &result[1],
            RecoveredCatalogMutation::CreateIndex { .. }
        ));
    }

    #[test]
    fn unknown_type_tag_errors() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0xFF); // Unknown type
        let result = deserialize_catalog_data(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn create_index_with_compound_field_paths() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0x03);
        buf.extend_from_slice(&50u64.to_le_bytes()); // index id
        buf.extend_from_slice(&10u64.to_le_bytes()); // collection id
        let name = "by_addr";
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        // 2 field paths: ["address", "city"] and ["zip"]
        buf.extend_from_slice(&2u32.to_le_bytes());
        // First: 2 segments
        buf.extend_from_slice(&2u32.to_le_bytes());
        for seg in ["address", "city"] {
            buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
            buf.extend_from_slice(seg.as_bytes());
        }
        // Second: 1 segment
        buf.extend_from_slice(&1u32.to_le_bytes());
        let seg = "zip";
        buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
        buf.extend_from_slice(seg.as_bytes());

        let result = deserialize_catalog_data(&buf).unwrap();
        match &result[0] {
            RecoveredCatalogMutation::CreateIndex { field_paths, .. } => {
                assert_eq!(field_paths.len(), 2);
                assert_eq!(field_paths[0].segments(), &["address", "city"]);
                assert_eq!(field_paths[1].segments(), &["zip"]);
            }
            _ => panic!("expected CreateIndex"),
        }
    }
}
