//! Comprehensive integration tests for the storage engine.
//!
//! These tests exercise the StorageEngine facade end-to-end, covering:
//! - Full lifecycle (create, insert, checkpoint, close, reopen, verify)
//! - Crash recovery (WAL replay without prior checkpoint)
//! - Multiple engine isolation
//! - Catalog B-tree persistence
//! - Heap through engine
//! - Vacuum through engine
//! - Large-scale inserts
//! - Custom backends

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use exdb_storage::backend::{MemoryPageStorage, MemoryWalStorage, PageStorage, WalStorage};
use exdb_storage::btree::ScanDirection;
use exdb_storage::catalog_btree::{
    self, CatalogEntityType, CatalogIndexState, CollectionEntry, IndexEntry, IndexType,
};
use exdb_storage::engine::{StorageConfig, StorageEngine};
use exdb_storage::heap::HeapRef;
use exdb_storage::recovery::{NoOpHandler, WalRecordHandler};
use exdb_storage::vacuum::VacuumEntry;
use exdb_storage::wal::WalRecord;

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

fn small_config() -> StorageConfig {
    StorageConfig {
        page_size: 4096,
        memory_budget: 4096 * 256,
        ..Default::default()
    }
}

/// A WalRecordHandler that collects all records for verification.
struct CollectingHandler {
    records: Vec<(u8, Vec<u8>)>,
}

impl CollectingHandler {
    fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }
}

impl WalRecordHandler for CollectingHandler {
    fn handle_record(&mut self, record: &WalRecord) -> std::io::Result<()> {
        self.records
            .push((record.record_type, record.payload.clone()));
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 10: Full lifecycle — 1000 entries
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_full_lifecycle_1000_entries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("lifecycle_db");

    let root_page;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        let handle = engine.create_btree().unwrap();

        // Insert 1000 entries.
        for i in 0u32..1000 {
            let key = format!("key-{:06}", i);
            let value = format!("value-{:06}", i);
            handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Save root page AFTER inserts (root may change due to splits).
        root_page = handle.root_page();

        // Checkpoint to flush everything.
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify all 1000 entries.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        let handle = engine.open_btree(root_page);
        for i in 0u32..1000 {
            let key = format!("key-{:06}", i);
            let expected = format!("value-{:06}", i);
            let value = handle.get(key.as_bytes()).unwrap();
            assert_eq!(value, Some(expected.into_bytes()), "missing key {}", key);
        }

        // Verify scan returns all 1000 entries in order.
        let entries: Vec<_> = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 1000);

        // Verify ordering.
        for window in entries.windows(2) {
            assert!(window[0].0 < window[1].0, "scan results not sorted");
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 11: In-memory lifecycle
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_in_memory_lifecycle() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    assert!(!engine.is_durable());

    // Create multiple B-trees and insert data.
    let bt1 = engine.create_btree().unwrap();
    let bt2 = engine.create_btree().unwrap();

    for i in 0u32..100 {
        let key = format!("k{:04}", i);
        bt1.insert(key.as_bytes(), b"from-tree1").unwrap();
        bt2.insert(key.as_bytes(), b"from-tree2").unwrap();
    }

    // Verify isolation.
    for i in 0u32..100 {
        let key = format!("k{:04}", i);
        assert_eq!(
            bt1.get(key.as_bytes()).unwrap(),
            Some(b"from-tree1".to_vec())
        );
        assert_eq!(
            bt2.get(key.as_bytes()).unwrap(),
            Some(b"from-tree2".to_vec())
        );
    }

    // Heap operations.
    let large_data = vec![0xABu8; 8000]; // larger than a page
    let href = engine.heap_store(&large_data).unwrap();
    let loaded = engine.heap_load(href).unwrap();
    assert_eq!(loaded, large_data);

    // WAL operations.
    let lsn1 = engine.append_wal(0x03, b"payload1").await.unwrap();
    let lsn2 = engine.append_wal(0x04, b"payload2").await.unwrap();
    assert!(lsn2 > lsn1);

    // Checkpoint (no-op for in-memory, but should not error).
    engine.checkpoint().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Test 12: Custom backend
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_custom_backend() {
    let page_storage: Arc<dyn PageStorage> = Arc::new(MemoryPageStorage::new(4096));
    let wal_storage: Arc<dyn WalStorage> = Arc::new(MemoryWalStorage::new());

    let engine = StorageEngine::open_with_backend(
        page_storage,
        wal_storage,
        small_config(),
        None, // no handler for fresh database
    )
    .unwrap();

    assert!(!engine.is_durable()); // memory backends are not durable

    let handle = engine.create_btree().unwrap();
    handle.insert(b"custom", b"backend").unwrap();
    assert_eq!(handle.get(b"custom").unwrap(), Some(b"backend".to_vec()));
}

// ═══════════════════════════════════════════════════════════════════════
// Test 13: Crash recovery end-to-end
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_crash_recovery_end_to_end() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("crash_db");

    // Phase 1: Open, checkpoint once (so file structure is valid), then append
    // more WAL records AFTER the checkpoint, and simulate crash.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        // First checkpoint to establish a valid on-disk state.
        engine.checkpoint().await.unwrap();

        // Now append WAL records AFTER the checkpoint.
        // These should be replayed on recovery since they're after checkpoint_lsn.
        engine.append_wal(0x03, b"create-col-1").await.unwrap();
        engine.append_wal(0x01, b"commit-tx-1").await.unwrap();

        // Simulate crash: shutdown WAL writer (so records are flushed to storage)
        // but do NOT close/checkpoint — the file header retains the OLD checkpoint_lsn.
        engine.wal_writer().shutdown().await;
        // Write file header so page 0 is valid on reopen (with old checkpoint_lsn).
        engine.buffer_pool().flush_page(0).unwrap();
        engine.buffer_pool().page_storage().sync().unwrap();
        // Engine is dropped here without final checkpoint.
    }

    // Phase 2: Reopen with a collecting handler to verify WAL replay.
    {
        let mut handler = CollectingHandler::new();
        let engine = StorageEngine::open(&path, small_config(), &mut handler).unwrap();

        // The handler should have seen the post-checkpoint WAL records replayed.
        assert!(
            !handler.records.is_empty(),
            "WAL records should be replayed on recovery"
        );

        // Verify we got the expected record types.
        let types: Vec<u8> = handler.records.iter().map(|(t, _)| *t).collect();
        assert!(
            types.contains(&0x03),
            "should replay create-collection record"
        );
        assert!(types.contains(&0x01), "should replay commit record");

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 14: Multiple engines isolation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multiple_engines_isolation() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path1 = tmp.path().join("db1");
    let path2 = tmp.path().join("db2");

    let engine1 = StorageEngine::open(&path1, small_config(), &mut NoOpHandler).unwrap();
    let engine2 = StorageEngine::open(&path2, small_config(), &mut NoOpHandler).unwrap();

    let bt1 = engine1.create_btree().unwrap();
    let bt2 = engine2.create_btree().unwrap();

    bt1.insert(b"only-in-1", b"val1").unwrap();
    bt2.insert(b"only-in-2", b"val2").unwrap();

    // Verify data does not leak across engines.
    assert_eq!(bt1.get(b"only-in-1").unwrap(), Some(b"val1".to_vec()));
    assert_eq!(bt1.get(b"only-in-2").unwrap(), None);

    assert_eq!(bt2.get(b"only-in-2").unwrap(), Some(b"val2".to_vec()));
    assert_eq!(bt2.get(b"only-in-1").unwrap(), None);

    engine1.close().await.unwrap();
    engine2.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Test 15: Catalog B-tree persistence
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_catalog_btree_persistence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("catalog_db");

    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let fh = engine.file_header();
        let catalog_root = fh.catalog_root_page.get();
        let name_root = fh.catalog_name_root_page.get();

        let catalog = engine.open_btree(catalog_root);
        let name_idx = engine.open_btree(name_root);

        // Insert collection entries.
        let col1 = CollectionEntry {
            collection_id: 1,
            name: "users".to_string(),
            primary_root_page: 100,
            doc_count: 42,
        };
        let col2 = CollectionEntry {
            collection_id: 2,
            name: "orders".to_string(),
            primary_root_page: 200,
            doc_count: 7,
        };

        for col in &[&col1, &col2] {
            let key = catalog_btree::make_catalog_id_key(
                CatalogEntityType::Collection,
                col.collection_id,
            );
            let val = catalog_btree::serialize_collection(col);
            catalog.insert(&key, &val).unwrap();

            let name_key =
                catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, &col.name);
            let name_val = catalog_btree::serialize_name_value(col.collection_id);
            name_idx.insert(&name_key, &name_val).unwrap();
        }

        // Insert index entry.
        let idx1 = IndexEntry {
            index_id: 10,
            collection_id: 1,
            name: "users_email".to_string(),
            field_paths: vec![vec!["email".to_string()]],
            root_page: 300,
            state: CatalogIndexState::Ready,
            index_type: IndexType::BTree,
            aux_root_pages: vec![],
            config: vec![],
        };
        let idx_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx1.index_id);
        let idx_val = catalog_btree::serialize_index(&idx1);
        catalog.insert(&idx_key, &idx_val).unwrap();

        let idx_name_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &idx1.name);
        let idx_name_val = catalog_btree::serialize_name_value(idx1.index_id);
        name_idx.insert(&idx_name_key, &idx_name_val).unwrap();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify catalog entries.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let fh = engine.file_header();
        let catalog = engine.open_btree(fh.catalog_root_page.get());
        let name_idx = engine.open_btree(fh.catalog_name_root_page.get());

        // Scan all collections by ID prefix.
        let prefix = catalog_btree::collection_id_scan_prefix();
        let entries: Vec<_> = catalog
            .scan(
                Bound::Included(&prefix[..]),
                Bound::Unbounded,
                ScanDirection::Forward,
            )
            .take_while(|r| {
                r.as_ref()
                    .map(|(k, _)| k.first() == Some(&prefix[0]))
                    .unwrap_or(true)
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entries.len(), 2, "should have 2 collection entries");

        let col1 = catalog_btree::deserialize_collection(&entries[0].1).unwrap();
        assert_eq!(col1.name, "users");
        assert_eq!(col1.doc_count, 42);

        let col2 = catalog_btree::deserialize_collection(&entries[1].1).unwrap();
        assert_eq!(col2.name, "orders");
        assert_eq!(col2.doc_count, 7);

        // Verify name index lookup.
        let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
        let name_val = name_idx.get(&name_key).unwrap().unwrap();
        let resolved_id = catalog_btree::deserialize_name_value(&name_val).unwrap();
        assert_eq!(resolved_id, 1);

        // Verify index entry.
        let idx_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, 10);
        let idx_val = catalog.get(&idx_key).unwrap().unwrap();
        let idx = catalog_btree::deserialize_index(&idx_val).unwrap();
        assert_eq!(idx.name, "users_email");
        assert_eq!(idx.state, CatalogIndexState::Ready);
        assert_eq!(idx.field_paths, vec![vec!["email".to_string()]]);

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 16: Heap through engine — large blobs + free
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_heap_large_blobs_and_free() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();

    // Store several blobs of varying sizes.
    let sizes = [100, 1000, 4000, 8000, 16000, 50000];
    let mut refs = Vec::new();

    for &size in &sizes {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let href = engine.heap_store(&data).unwrap();
        refs.push((href, data));
    }

    // Verify all blobs can be loaded correctly.
    for (href, expected) in &refs {
        let loaded = engine.heap_load(*href).unwrap();
        assert_eq!(loaded.len(), expected.len(), "blob size mismatch");
        assert_eq!(&loaded, expected, "blob content mismatch");
    }

    // Free the first few blobs.
    for (href, _) in &refs[..3] {
        engine.heap_free(*href).unwrap();
    }

    // Remaining blobs should still be accessible.
    for (href, expected) in &refs[3..] {
        let loaded = engine.heap_load(*href).unwrap();
        assert_eq!(&loaded, expected);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 17: Vacuum through engine
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vacuum_through_engine() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();

    let handle = engine.create_btree().unwrap();
    let root = handle.root_page();

    // Insert entries.
    for i in 0u32..50 {
        let key = format!("vac-{:04}", i);
        let value = format!("val-{:04}", i);
        handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Vacuum some entries.
    let entries: Vec<VacuumEntry> = (0u32..10)
        .map(|i| VacuumEntry {
            btree_root: root,
            key: format!("vac-{:04}", i).into_bytes(),
        })
        .collect();

    let removed = engine.vacuum(&entries).unwrap();
    assert_eq!(removed, 10);

    // Verify vacuumed entries are gone.
    for i in 0u32..10 {
        let key = format!("vac-{:04}", i);
        assert_eq!(handle.get(key.as_bytes()).unwrap(), None);
    }

    // Verify remaining entries still exist.
    for i in 10u32..50 {
        let key = format!("vac-{:04}", i);
        let expected = format!("val-{:04}", i);
        assert_eq!(
            handle.get(key.as_bytes()).unwrap(),
            Some(expected.into_bytes())
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 18: B-tree scan directions + bounded ranges
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_btree_scan_directions_and_bounds() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    // Insert ordered keys.
    for i in 0u32..100 {
        let key = format!("scan-{:04}", i);
        handle
            .insert(key.as_bytes(), format!("{}", i).as_bytes())
            .unwrap();
    }

    // Forward bounded scan [scan-0020, scan-0030).
    let entries: Vec<_> = handle
        .scan(
            Bound::Included(b"scan-0020"),
            Bound::Excluded(b"scan-0030"),
            ScanDirection::Forward,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 10);
    assert_eq!(entries[0].0, b"scan-0020");
    assert_eq!(entries[9].0, b"scan-0029");

    // Backward unbounded scan — verify descending order.
    let entries: Vec<_> = handle
        .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Backward)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 100);
    assert_eq!(entries[0].0, b"scan-0099"); // largest key first
    assert_eq!(entries[99].0, b"scan-0000"); // smallest key last

    // Backward bounded scan.
    let entries: Vec<_> = handle
        .scan(
            Bound::Included(b"scan-0050"),
            Bound::Included(b"scan-0055"),
            ScanDirection::Backward,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 6);
    assert_eq!(entries[0].0, b"scan-0055");
    assert_eq!(entries[5].0, b"scan-0050");
}

// ═══════════════════════════════════════════════════════════════════════
// Test 19: B-tree delete + re-insert
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_btree_delete_and_reinsert() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    // Insert 200 entries.
    for i in 0u32..200 {
        let key = format!("del-{:04}", i);
        handle.insert(key.as_bytes(), b"original").unwrap();
    }

    // Delete even-numbered entries.
    for i in (0u32..200).step_by(2) {
        let key = format!("del-{:04}", i);
        let deleted = handle.delete(key.as_bytes()).unwrap();
        assert!(deleted, "key {} should have been deleted", key);
    }

    // Verify: even = gone, odd = present.
    for i in 0u32..200 {
        let key = format!("del-{:04}", i);
        let value = handle.get(key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert_eq!(value, None, "key {} should be deleted", key);
        } else {
            assert_eq!(
                value,
                Some(b"original".to_vec()),
                "key {} should exist",
                key
            );
        }
    }

    // Re-insert some deleted keys with new values.
    for i in (0u32..50).step_by(2) {
        let key = format!("del-{:04}", i);
        handle.insert(key.as_bytes(), b"reinserted").unwrap();
    }

    // Verify re-inserted values.
    for i in (0u32..50).step_by(2) {
        let key = format!("del-{:04}", i);
        assert_eq!(
            handle.get(key.as_bytes()).unwrap(),
            Some(b"reinserted".to_vec())
        );
    }

    // Total scan should show 100 odd + 25 re-inserted = 125 entries.
    let count = handle
        .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
        .count();
    assert_eq!(count, 125);
}

// ═══════════════════════════════════════════════════════════════════════
// Test 20: WAL multiple records + sequential read
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_wal_multiple_records_and_read() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();

    let mut lsns = Vec::new();
    for i in 0u32..20 {
        let payload = format!("record-{}", i);
        let lsn = engine
            .append_wal(0x01 + (i % 10) as u8, payload.as_bytes())
            .await
            .unwrap();
        lsns.push(lsn);
    }

    // Verify all LSNs are monotonically increasing.
    for window in lsns.windows(2) {
        assert!(
            window[1] > window[0],
            "LSNs must be monotonically increasing"
        );
    }

    // Read from the beginning and verify all records.
    let records: Vec<_> = engine
        .read_wal_from(0)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(records.len(), 20);

    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.lsn, lsns[i]);
        let expected_type = 0x01 + (i % 10) as u8;
        assert_eq!(record.record_type, expected_type);
        let expected_payload = format!("record-{}", i);
        assert_eq!(record.payload, expected_payload.as_bytes());
    }

    // Read from a mid-point.
    let mid_records: Vec<_> = engine
        .read_wal_from(lsns[10])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(mid_records.len(), 10);
    assert_eq!(mid_records[0].lsn, lsns[10]);
}

// ═══════════════════════════════════════════════════════════════════════
// Test 21: Checkpoint + recovery round-trip
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_checkpoint_recovery_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("roundtrip_db");

    let root1;
    let root2;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        // Create two btrees with data.
        let bt1 = engine.create_btree().unwrap();
        let bt2 = engine.create_btree().unwrap();

        for i in 0u32..100 {
            bt1.insert(
                format!("bt1-{:04}", i).as_bytes(),
                format!("v1-{}", i).as_bytes(),
            )
            .unwrap();
            bt2.insert(
                format!("bt2-{:04}", i).as_bytes(),
                format!("v2-{}", i).as_bytes(),
            )
            .unwrap();
        }

        // First checkpoint.
        engine.checkpoint().await.unwrap();

        // Insert more data after checkpoint.
        for i in 100u32..150 {
            bt1.insert(
                format!("bt1-{:04}", i).as_bytes(),
                format!("v1-{}", i).as_bytes(),
            )
            .unwrap();
        }

        // Save root pages AFTER all inserts (roots may change due to splits).
        root1 = bt1.root_page();
        root2 = bt2.root_page();

        // Append WAL records for the post-checkpoint data.
        engine.append_wal(0x01, b"post-checkpoint").await.unwrap();

        // Second checkpoint.
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify all data (pre- and post-first-checkpoint).
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        let bt1 = engine.open_btree(root1);
        let bt2 = engine.open_btree(root2);

        // All 150 entries in bt1.
        for i in 0u32..150 {
            let key = format!("bt1-{:04}", i);
            assert!(
                bt1.get(key.as_bytes()).unwrap().is_some(),
                "bt1 missing key {}",
                key
            );
        }

        // All 100 entries in bt2.
        for i in 0u32..100 {
            let key = format!("bt2-{:04}", i);
            assert!(
                bt2.get(key.as_bytes()).unwrap().is_some(),
                "bt2 missing key {}",
                key
            );
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 22: Update existing keys (overwrite)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_btree_update_existing_keys() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    // Insert.
    for i in 0u32..50 {
        let key = format!("upd-{:04}", i);
        handle.insert(key.as_bytes(), b"version-1").unwrap();
    }

    // Overwrite with new values.
    for i in 0u32..50 {
        let key = format!("upd-{:04}", i);
        handle.insert(key.as_bytes(), b"version-2").unwrap();
    }

    // Verify all have the updated value.
    for i in 0u32..50 {
        let key = format!("upd-{:04}", i);
        assert_eq!(
            handle.get(key.as_bytes()).unwrap(),
            Some(b"version-2".to_vec())
        );
    }

    // Total count should still be 50 (not 100).
    let count = handle
        .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
        .count();
    assert_eq!(count, 50);
}

// ═══════════════════════════════════════════════════════════════════════
// Test 23: File header persistence across open/close
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_file_header_persistence() {
    use zerocopy::byteorder::U64;

    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("header_db");

    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        engine
            .update_file_header(|fh| {
                fh.visible_ts = U64::new(99999);
                fh.next_collection_id = U64::new(42);
                fh.next_index_id = U64::new(7);
                fh.generation = U64::new(3);
            })
            .unwrap();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let fh = engine.file_header();

        assert_eq!(fh.visible_ts.get(), 99999);
        assert_eq!(fh.next_collection_id.get(), 42);
        assert_eq!(fh.next_index_id.get(), 7);
        assert_eq!(fh.generation.get(), 3);

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 24: B-tree handles large keys and values
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_btree_large_keys_and_values() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    // Insert entries with varying key/value sizes.
    let test_cases = vec![
        (vec![b'K'; 10], vec![b'V'; 10]),
        (vec![b'K'; 100], vec![b'V'; 100]),
        (vec![b'K'; 500], vec![b'V'; 500]),
        (vec![b'K'; 1000], vec![b'V'; 1000]),
    ];

    for (key, value) in &test_cases {
        handle.insert(key, value).unwrap();
    }

    for (key, expected) in &test_cases {
        let result = handle.get(key).unwrap();
        assert_eq!(result.as_deref(), Some(expected.as_slice()));
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 25: Heap store + btree reference pattern
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_heap_btree_reference_pattern() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    // Common pattern: store large values in heap, reference from B-tree.
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for i in 0u32..20 {
        let key = format!("doc-{:04}", i);
        let large_value: Vec<u8> = (0..5000).map(|j| ((i + j) % 256) as u8).collect();

        // Store in heap, put heap ref in btree.
        let href = engine.heap_store(&large_value).unwrap();
        handle.insert(key.as_bytes(), &href.to_bytes()).unwrap();

        expected.insert(key.into_bytes(), large_value);
    }

    // Verify: read heap ref from btree, load from heap.
    for (key, expected_value) in &expected {
        let href_bytes = handle.get(key).unwrap().unwrap();
        let href = exdb_storage::heap::HeapRef::from_bytes(href_bytes[..6].try_into().unwrap());
        let loaded = engine.heap_load(href).unwrap();
        assert_eq!(&loaded, expected_value);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 26: Reopen preserves B-tree structure after many operations
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_reopen_preserves_complex_btree() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("complex_db");

    let root;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();

        // Insert 500 entries.
        for i in 0u32..500 {
            let key = format!("cplx-{:06}", i);
            let value = format!("data-{}", i);
            handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Delete every 3rd entry.
        for i in (0u32..500).step_by(3) {
            let key = format!("cplx-{:06}", i);
            handle.delete(key.as_bytes()).unwrap();
        }

        // Update entries where i % 5 == 0 and not deleted (i % 3 != 0).
        for i in (0u32..500).filter(|i| i % 3 != 0 && i % 5 == 0) {
            let key = format!("cplx-{:06}", i);
            handle.insert(key.as_bytes(), b"updated").unwrap();
        }

        // Save root page AFTER all modifications (root may change due to splits).
        root = handle.root_page();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root);

        for i in 0u32..500 {
            let key = format!("cplx-{:06}", i);
            let value = handle.get(key.as_bytes()).unwrap();

            if i % 3 == 0 {
                // Deleted.
                assert_eq!(value, None, "key {} should be deleted", key);
            } else if i % 5 == 0 {
                // Updated (and not deleted since i%3!=0).
                assert_eq!(
                    value,
                    Some(b"updated".to_vec()),
                    "key {} should be updated",
                    key
                );
            } else {
                // Original.
                let expected = format!("data-{}", i);
                assert_eq!(
                    value,
                    Some(expected.into_bytes()),
                    "key {} should have original value",
                    key
                );
            }
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 27: Empty B-tree scan
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_empty_btree_scan() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    let entries: Vec<_> = handle
        .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(entries.is_empty());

    let entries: Vec<_> = handle
        .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Backward)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(entries.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════
// Test 28: Delete nonexistent key returns false
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_delete_nonexistent() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();

    handle.insert(b"exists", b"yes").unwrap();
    assert!(!handle.delete(b"does-not-exist").unwrap());
    assert!(handle.delete(b"exists").unwrap());
    assert!(!handle.delete(b"exists").unwrap()); // already deleted
}

// ═══════════════════════════════════════════════════════════════════════
// Test 29: Vacuum idempotent (double vacuum same keys)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vacuum_idempotent() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();
    let handle = engine.create_btree().unwrap();
    let root = handle.root_page();

    for i in 0u32..20 {
        handle
            .insert(format!("idem-{}", i).as_bytes(), b"val")
            .unwrap();
    }

    let entries: Vec<VacuumEntry> = (0u32..10)
        .map(|i| VacuumEntry {
            btree_root: root,
            key: format!("idem-{}", i).into_bytes(),
        })
        .collect();

    let removed1 = engine.vacuum(&entries).unwrap();
    assert_eq!(removed1, 10);

    // Second vacuum of same entries should remove 0.
    let removed2 = engine.vacuum(&entries).unwrap();
    assert_eq!(removed2, 0);
}

// ═══════════════════════════════════════════════════════════════════════
// Test 30: Stress test — many B-trees
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_many_btrees() {
    let engine = StorageEngine::open_in_memory(small_config()).unwrap();

    let mut handles = Vec::new();
    for _ in 0..20 {
        handles.push(engine.create_btree().unwrap());
    }

    // Insert different data into each tree.
    for (t, handle) in handles.iter().enumerate() {
        for i in 0u32..50 {
            let key = format!("t{}-k{}", t, i);
            let value = format!("t{}-v{}", t, i);
            handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Verify each tree has exactly its own data.
    for (t, handle) in handles.iter().enumerate() {
        let count = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .count();
        assert_eq!(count, 50, "tree {} should have 50 entries", t);

        let key = format!("t{}-k{}", t, 25);
        let expected = format!("t{}-v{}", t, 25);
        assert_eq!(
            handle.get(key.as_bytes()).unwrap(),
            Some(expected.into_bytes())
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 31: No checkpoint = data loss
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_no_checkpoint_data_lost() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("no_ckpt_db");

    let root_page;
    // Phase 1: create btree, checkpoint so root page exists on disk.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();
        root_page = handle.root_page();
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Phase 2: reopen, insert data, do NOT checkpoint — simulate crash.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root_page);

        for i in 0u32..50 {
            let key = format!("ephemeral-{:04}", i);
            handle.insert(key.as_bytes(), b"gone").unwrap();
        }

        // Deliberately do NOT checkpoint. Just drop — simulates crash.
        // The WAL may have records but checkpoint_lsn still points to
        // the old checkpoint, and pages were never flushed to disk.
    }

    // Phase 3: reopen — data written after last checkpoint should not
    // be in the btree pages (they were never flushed to disk).
    {
        let mut handler = CollectingHandler::new();
        let engine = StorageEngine::open(&path, small_config(), &mut handler).unwrap();

        let handle = engine.open_btree(root_page);
        let entries: Vec<_> = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            entries.len(),
            0,
            "data inserted without checkpoint should not survive reopen"
        );

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 32: Multiple open/write/checkpoint/close cycles
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multiple_reopen_cycles() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("multi_cycle_db");

    let mut root;
    // Cycle 1: create btree, insert 0..100
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();

        for i in 0u32..100 {
            let key = format!("cycle-{:06}", i);
            handle.insert(key.as_bytes(), b"v1").unwrap();
        }
        root = handle.root_page();
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Cycle 2: reopen, insert 100..200, update some from cycle 1
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root);

        for i in 100u32..200 {
            let key = format!("cycle-{:06}", i);
            handle.insert(key.as_bytes(), b"v2").unwrap();
        }
        // Update first 10 keys from cycle 1.
        for i in 0u32..10 {
            let key = format!("cycle-{:06}", i);
            handle.insert(key.as_bytes(), b"updated").unwrap();
        }
        // Root may have changed due to splits.
        root = handle.root_page();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Cycle 3: reopen, delete some, insert 200..250
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root);

        // Delete 50..60.
        for i in 50u32..60 {
            let key = format!("cycle-{:06}", i);
            handle.delete(key.as_bytes()).unwrap();
        }
        for i in 200u32..250 {
            let key = format!("cycle-{:06}", i);
            handle.insert(key.as_bytes(), b"v3").unwrap();
        }
        // Root may have changed due to splits.
        root = handle.root_page();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Final verify: reopen and check all data.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root);

        // 0..10 should be "updated".
        for i in 0u32..10 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(b"updated".to_vec()),
                "key {} should be updated",
                key
            );
        }
        // 10..50 should be "v1".
        for i in 10u32..50 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(b"v1".to_vec()),
                "key {} should be v1",
                key
            );
        }
        // 50..60 should be deleted.
        for i in 50u32..60 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                None,
                "key {} should be deleted",
                key
            );
        }
        // 60..100 should be "v1".
        for i in 60u32..100 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(b"v1".to_vec()),
                "key {} should be v1",
                key
            );
        }
        // 100..200 should be "v2".
        for i in 100u32..200 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(b"v2".to_vec()),
                "key {} should be v2",
                key
            );
        }
        // 200..250 should be "v3".
        for i in 200u32..250 {
            let key = format!("cycle-{:06}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(b"v3".to_vec()),
                "key {} should be v3",
                key
            );
        }

        // Total: 10 + 40 + 0 + 40 + 100 + 50 = 240.
        let count = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .count();
        assert_eq!(count, 240);

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 33: Heap persistence across file-backed reopen
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_heap_persistence_file_backed() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("heap_persist_db");

    let mut refs_and_data: Vec<([u8; 6], Vec<u8>)> = Vec::new();
    let btree_root;

    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();

        // Store blobs of varying sizes: small, medium, and large (overflow).
        let sizes = [50, 500, 2000, 8000];
        for (idx, &size) in sizes.iter().enumerate() {
            let data: Vec<u8> = (0..size).map(|j| ((idx + j) % 256) as u8).collect();
            let href = engine.heap_store(&data).unwrap();
            let href_bytes = href.to_bytes();

            // Store the heap ref in the btree so we can find it after reopen.
            let key = format!("blob-{}", idx);
            handle.insert(key.as_bytes(), &href_bytes).unwrap();

            refs_and_data.push((href_bytes, data));
        }

        btree_root = handle.root_page();
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify all blobs.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(btree_root);

        for (idx, (expected_href_bytes, expected_data)) in refs_and_data.iter().enumerate() {
            let key = format!("blob-{}", idx);
            let stored_href_bytes = handle.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                &stored_href_bytes[..6],
                expected_href_bytes,
                "heap ref mismatch for blob {}",
                idx
            );

            let href = HeapRef::from_bytes(stored_href_bytes[..6].try_into().unwrap());
            let loaded = engine.heap_load(href).unwrap();
            assert_eq!(
                loaded.len(),
                expected_data.len(),
                "blob {} size mismatch",
                idx
            );
            assert_eq!(&loaded, expected_data, "blob {} content mismatch", idx);
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 34: Page size mismatch on reopen
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_page_size_mismatch_on_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("mismatch_db");

    // Create DB with page_size = 4096.
    {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler).unwrap();
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen with page_size = 8192 — should fail.
    {
        let config = StorageConfig {
            page_size: 8192,
            memory_budget: 8192 * 64,
            ..Default::default()
        };
        let result = StorageEngine::open(&path, config, &mut NoOpHandler);
        assert!(result.is_err(), "opening with wrong page_size should fail");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 35: Free list persistence across reopen
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_free_list_persistence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("freelist_db");

    let freed_pages;
    let page_count_before_close;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        // Allocate several pages via btree inserts to grow the file.
        let handle = engine.create_btree().unwrap();
        for i in 0u32..100 {
            let key = format!("fl-{:06}", i);
            handle.insert(key.as_bytes(), b"data").unwrap();
        }

        // Manually deallocate some pages through the free list.
        // First, allocate extra pages that we'll then free.
        let mut pages_to_free = Vec::new();
        {
            let mut fl = engine.free_list().lock();
            for _ in 0..5 {
                let page_id = fl.allocate().unwrap();
                pages_to_free.push(page_id);
            }
            // Now free them back.
            for &page_id in &pages_to_free {
                fl.deallocate(page_id).unwrap();
            }
        }

        // The free list should now have 5 pages.
        let free_count = engine.free_list().lock().count().unwrap();
        assert_eq!(free_count, 5, "free list should have 5 freed pages");
        freed_pages = free_count;

        page_count_before_close = engine.buffer_pool().page_storage().page_count();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify free list was restored.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();

        // Free list head should be non-zero (restored from file header).
        let free_head = engine.free_list().lock().head();
        assert!(free_head > 0, "free list head should be restored on reopen");

        let free_count = engine.free_list().lock().count().unwrap();
        assert_eq!(
            free_count, freed_pages,
            "free list page count should survive reopen"
        );

        // Allocate from free list — should reuse freed pages, not extend file.
        {
            let mut fl = engine.free_list().lock();
            let reused = fl.allocate().unwrap();
            assert!(
                (reused as u64) < page_count_before_close,
                "allocated page {} should come from free list (total pages={})",
                reused,
                page_count_before_close
            );
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 36: Truncated WAL frame recovery
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_truncated_wal_frame_recovery() {
    use exdb_storage::recovery::{Recovery, RecoveryMode};

    let page_storage = MemoryPageStorage::new(4096);
    let wal_storage = MemoryWalStorage::new();

    fn encode_wal_frame(record_type: u8, payload: &[u8]) -> Vec<u8> {
        let payload_len = payload.len() as u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type]);
        hasher.update(payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(9 + payload.len());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.push(record_type);
        frame.extend_from_slice(payload);
        frame
    }

    // Write 3 valid records.
    for i in 0..3u32 {
        let payload = format!("valid-{}", i);
        let frame = encode_wal_frame(0x01, payload.as_bytes());
        wal_storage.append(&frame).unwrap();
    }

    // Write a truncated frame: valid header claiming 100 bytes of payload,
    // but only write 10 bytes of actual payload data.
    {
        let payload_len = 100u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[0x01]);
        hasher.update(&vec![0xAB; 100]);
        let crc = hasher.finalize();

        let mut partial_frame = Vec::new();
        partial_frame.extend_from_slice(&payload_len.to_le_bytes());
        partial_frame.extend_from_slice(&crc.to_le_bytes());
        partial_frame.push(0x01);
        partial_frame.extend_from_slice(&[0xAB; 10]); // Only 10 of 100 bytes
        wal_storage.append(&partial_frame).unwrap();
    }

    // Write another valid record after the truncated one.
    let good_after = encode_wal_frame(0x01, b"after-truncated");
    wal_storage.append(&good_after).unwrap();

    let mut handler = CollectingHandler::new();
    let (_end_lsn, _stats) = Recovery::run(
        &page_storage,
        &wal_storage,
        None,
        0,
        4096,
        &mut handler,
        RecoveryMode::Strict,
    )
    .unwrap();

    // Only the 3 valid records before the truncation should be replayed.
    assert_eq!(
        handler.records.len(),
        3,
        "should stop at truncated frame, got {} records",
        handler.records.len()
    );
    for (i, (rt, payload)) in handler.records.iter().enumerate() {
        assert_eq!(*rt, 0x01);
        let expected = format!("valid-{}", i);
        assert_eq!(payload, expected.as_bytes());
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 37: Multiple crash/recovery cycles
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multiple_crash_recovery_cycles() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("multi_crash_db");

    let root;

    // Session 1: create, insert, checkpoint, then insert more WITHOUT checkpoint → crash.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();

        for i in 0u32..50 {
            let key = format!("s1-{:04}", i);
            handle.insert(key.as_bytes(), b"batch1").unwrap();
        }
        root = handle.root_page();
        engine.checkpoint().await.unwrap();

        // Post-checkpoint WAL record.
        engine.append_wal(0x03, b"post-ckpt-1").await.unwrap();

        // Simulate crash.
        engine.wal_writer().shutdown().await;
        engine.buffer_pool().flush_page(0).unwrap();
        engine.buffer_pool().page_storage().sync().unwrap();
    }

    // Recovery 1.
    {
        let mut handler = CollectingHandler::new();
        let engine = StorageEngine::open(&path, small_config(), &mut handler).unwrap();

        assert!(
            !handler.records.is_empty(),
            "WAL records should be replayed after crash 1"
        );

        // Batch 1 (checkpointed) should be present.
        let handle = engine.open_btree(root);
        for i in 0u32..50 {
            let key = format!("s1-{:04}", i);
            assert!(
                handle.get(key.as_bytes()).unwrap().is_some(),
                "checkpointed key {} should survive crash",
                key
            );
        }

        // Session 2: write more, checkpoint, crash again.
        for i in 0u32..30 {
            let key = format!("s2-{:04}", i);
            handle.insert(key.as_bytes(), b"batch2").unwrap();
        }
        engine.checkpoint().await.unwrap();

        engine.append_wal(0x01, b"post-ckpt-2").await.unwrap();

        // Simulate crash 2.
        engine.wal_writer().shutdown().await;
        engine.buffer_pool().flush_page(0).unwrap();
        engine.buffer_pool().page_storage().sync().unwrap();
    }

    // Recovery 2.
    {
        let mut handler = CollectingHandler::new();
        let engine = StorageEngine::open(&path, small_config(), &mut handler).unwrap();

        assert!(
            !handler.records.is_empty(),
            "WAL records should be replayed after crash 2"
        );

        let handle = engine.open_btree(root);

        // Batch 1 still present.
        for i in 0u32..50 {
            let key = format!("s1-{:04}", i);
            assert!(
                handle.get(key.as_bytes()).unwrap().is_some(),
                "batch 1 key {} should survive two crashes",
                key
            );
        }
        // Batch 2 (checkpointed in session 2) should be present.
        for i in 0u32..30 {
            let key = format!("s2-{:04}", i);
            assert!(
                handle.get(key.as_bytes()).unwrap().is_some(),
                "batch 2 key {} should survive crash 2",
                key
            );
        }

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 38: Corrupt file header on reopen
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_corrupt_file_header_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("corrupt_hdr_db");

    // Create a valid database.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Corrupt the data file's page 0 (overwrite magic bytes).
    {
        use std::io::Write;
        let data_path = path.join("data.db");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        // Page header is 32 bytes, file header magic is at offset 32..36.
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(32)).unwrap();
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        file.sync_data().unwrap();
    }

    // Reopen should fail with a clear error.
    let result = StorageEngine::open(&path, small_config(), &mut NoOpHandler);
    assert!(
        result.is_err(),
        "corrupt file header should cause open to fail"
    );
    let err = result.err().expect("should be an error");
    assert!(
        err.to_string().contains("magic") || err.to_string().contains("mismatch"),
        "error should mention magic/mismatch: {}",
        err
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 39: Empty database reopen
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_empty_database_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("empty_db");

    let catalog_root;
    let catalog_name_root;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let fh = engine.file_header();
        catalog_root = fh.catalog_root_page.get();
        catalog_name_root = fh.catalog_name_root_page.get();

        // No writes — just checkpoint and close.
        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify empty state.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let fh = engine.file_header();

        assert_eq!(fh.catalog_root_page.get(), catalog_root);
        assert_eq!(fh.catalog_name_root_page.get(), catalog_name_root);

        // Catalog btrees should be empty.
        let catalog = engine.open_btree(catalog_root);
        let entries: Vec<_> = catalog
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            entries.len(),
            0,
            "empty database should have no catalog entries"
        );

        // Should be able to create and use btrees normally.
        let handle = engine.create_btree().unwrap();
        handle.insert(b"first-key", b"first-value").unwrap();
        assert_eq!(
            handle.get(b"first-key").unwrap(),
            Some(b"first-value".to_vec())
        );

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 40: Vacuum + reopen persistence
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vacuum_persistence_file_backed() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("vacuum_persist_db");

    let root;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();

        for i in 0u32..100 {
            let key = format!("vp-{:04}", i);
            let value = format!("val-{}", i);
            handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
        root = handle.root_page();

        // Vacuum entries 0..40.
        let entries: Vec<VacuumEntry> = (0u32..40)
            .map(|i| VacuumEntry {
                btree_root: root,
                key: format!("vp-{:04}", i).into_bytes(),
            })
            .collect();
        let removed = engine.vacuum(&entries).unwrap();
        assert_eq!(removed, 40);

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen and verify vacuum results persisted.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(root);

        for i in 0u32..40 {
            let key = format!("vp-{:04}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                None,
                "vacuumed key {} should be gone after reopen",
                key
            );
        }
        for i in 40u32..100 {
            let key = format!("vp-{:04}", i);
            let expected = format!("val-{}", i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(expected.into_bytes()),
                "surviving key {} should persist",
                key
            );
        }

        let count = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .count();
        assert_eq!(count, 60);

        engine.close().await.unwrap();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 41: Crash mid-checkpoint (DWB written, partial scatter-write)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_crash_mid_checkpoint_dwb_recovery() {
    use exdb_storage::page::{PageType, SlottedPage, SlottedPageRef};

    let tmp = tempfile::TempDir::new().unwrap();
    let dwb_path = tmp.path().join("test.dwb");
    let page_size = 4096;

    let page_storage = Arc::new(MemoryPageStorage::new(page_size));
    page_storage.extend(10).unwrap();

    let mut original_pages: Vec<Vec<u8>> = Vec::new();
    for i in 0..10u32 {
        let mut buf = vec![0u8; page_size];
        let mut page = SlottedPage::init(&mut buf, i, PageType::Heap);
        let data = format!("original-page-{}", i);
        page.insert_slot(data.as_bytes()).unwrap();
        page.stamp_checksum();
        page_storage.write_page(i, &buf).unwrap();
        original_pages.push(buf);
    }

    // Build updated pages (as if checkpoint were writing them).
    let updated_pages: Vec<Vec<u8>> = (0..10u32)
        .map(|i| {
            let mut buf = vec![0u8; page_size];
            let mut page = SlottedPage::init(&mut buf, i, PageType::Heap);
            let data = format!("updated-page-{}", i);
            page.insert_slot(data.as_bytes()).unwrap();
            page.stamp_checksum();
            buf
        })
        .collect();

    // Write DWB file with all 10 updated pages.
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dwb_path)
            .unwrap();

        let mut header = [0u8; 16];
        header[0..4].copy_from_slice(&0x44574200u32.to_le_bytes());
        header[4..6].copy_from_slice(&1u16.to_le_bytes());
        header[6..8].copy_from_slice(&(page_size as u16).to_le_bytes());
        header[8..12].copy_from_slice(&10u32.to_le_bytes());
        let checksum = crc32fast::hash(&header[0..12]);
        header[12..16].copy_from_slice(&checksum.to_le_bytes());
        file.write_all(&header).unwrap();

        for i in 0..10u32 {
            file.write_all(&i.to_le_bytes()).unwrap();
            file.write_all(&updated_pages[i as usize]).unwrap();
        }
        file.sync_data().unwrap();
    }

    // Partial scatter-write: only pages 0..5 were written before crash.
    for i in 0..5u32 {
        page_storage
            .write_page(i, &updated_pages[i as usize])
            .unwrap();
    }
    // Corrupt pages 5..8 (torn writes).
    for i in 5..8u32 {
        let corrupt = vec![0xFFu8; page_size];
        page_storage.write_page(i, &corrupt).unwrap();
    }
    // Pages 8..10 still have valid original data.

    // Run DWB recovery.
    let dwb = exdb_storage::dwb::DoubleWriteBuffer::new(
        &dwb_path,
        page_storage.clone() as Arc<dyn PageStorage>,
        page_size,
    );
    let restored = dwb.recover().unwrap();

    assert!(
        restored >= 3,
        "at least 3 torn pages should be restored, got {}",
        restored
    );

    // All 10 pages should have valid checksums after recovery.
    for i in 0..10u32 {
        let mut buf = vec![0u8; page_size];
        page_storage.read_page(i, &mut buf).unwrap();
        let page_ref = SlottedPageRef::from_buf(&buf).unwrap();
        assert!(
            page_ref.verify_checksum(),
            "page {} should have valid checksum after DWB recovery",
            i
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 42: Buffer pool under extreme pressure
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_buffer_pool_extreme_pressure() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("pressure_db");

    // Very small buffer pool: only 8 frames.
    let config = StorageConfig {
        page_size: 4096,
        memory_budget: 4096 * 8,
        ..Default::default()
    };

    let engine = StorageEngine::open(&path, config, &mut NoOpHandler).unwrap();

    // Create 3 btrees — forces heavy eviction with only 8 frames.
    let mut handles = Vec::new();
    for _ in 0..3 {
        handles.push(engine.create_btree().unwrap());
    }

    for (t, handle) in handles.iter().enumerate() {
        for i in 0u32..100 {
            let key = format!("t{}-k{:04}", t, i);
            let value = format!("t{}-v{:04}", t, i);
            handle.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Verify all data despite heavy eviction.
    for (t, handle) in handles.iter().enumerate() {
        for i in 0u32..100 {
            let key = format!("t{}-k{:04}", t, i);
            let expected = format!("t{}-v{:04}", t, i);
            assert_eq!(
                handle.get(key.as_bytes()).unwrap(),
                Some(expected.into_bytes()),
                "tree {} key {} mismatch under pressure",
                t,
                key
            );
        }
        let count = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .count();
        assert_eq!(count, 100, "tree {} should have 100 entries", t);
    }

    // Checkpoint and reopen with normal buffer to verify persistence.
    let roots: Vec<_> = handles.iter().map(|h| h.root_page()).collect();
    engine.checkpoint().await.unwrap();
    engine.close().await.unwrap();
    drop(handles);

    let engine2 = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
    for (t, &root) in roots.iter().enumerate() {
        let handle = engine2.open_btree(root);
        let count = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .count();
        assert_eq!(
            count, 100,
            "tree {} should have 100 entries after reopen",
            t
        );
    }
    engine2.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Test 43: Double close
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_double_close() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("double_close_db");

    let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
    let handle = engine.create_btree().unwrap();
    let root = handle.root_page();
    handle.insert(b"key", b"value").unwrap();

    // First close.
    engine.close().await.unwrap();

    // Second close should not panic.
    let result = engine.close().await;
    // Acceptable for second close to succeed or fail gracefully —
    // the important thing is no panic or data corruption.
    drop(result);

    // Reopen and verify data is intact.
    let engine2 = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
    let handle2 = engine2.open_btree(root);
    assert_eq!(
        handle2.get(b"key").unwrap(),
        Some(b"value".to_vec()),
        "data should survive double close"
    );
    engine2.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Test 35: Fresh database reopen without checkpoint
// ═══════════════════════════════════════════════════════════════════════

/// Create a fresh database and drop it without checkpoint or close.
/// Reopen should succeed — init_new_database flushes all pages.
#[tokio::test]
async fn test_reopen_fresh_db_without_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("fresh_no_ckpt");

    {
        let _engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        // Drop without checkpoint or close.
    }

    let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
    // Verify the catalog roots are valid by creating a btree (touches the free list + catalog).
    let handle = engine.create_btree().unwrap();
    handle.insert(b"k", b"v").unwrap();
    assert_eq!(handle.get(b"k").unwrap(), Some(b"v".to_vec()));
    engine.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Test 36: Heap store works after reopen (empty free space map)
// ═══════════════════════════════════════════════════════════════════════

/// After reopen the heap free space map is empty. Verify that store()
/// still works (allocates new pages) and that old blobs are still loadable.
#[tokio::test]
async fn test_heap_store_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("heap_reopen_db");

    let btree_root;
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.create_btree().unwrap();
        btree_root = handle.root_page();

        // Store a blob, persist its ref in a btree.
        let data = vec![0xAAu8; 500];
        let href = engine.heap_store(&data).unwrap();
        handle.insert(b"blob1", &href.to_bytes()).unwrap();

        engine.checkpoint().await.unwrap();
        engine.close().await.unwrap();
    }

    // Reopen — free space map is empty.
    {
        let engine = StorageEngine::open(&path, small_config(), &mut NoOpHandler).unwrap();
        let handle = engine.open_btree(btree_root);

        // Old blob is still loadable.
        let href_bytes = handle.get(b"blob1").unwrap().unwrap();
        let href = HeapRef::from_bytes(href_bytes[..6].try_into().unwrap());
        assert_eq!(engine.heap_load(href).unwrap(), vec![0xAAu8; 500]);

        // New store works despite empty free space map.
        let data2 = vec![0xBBu8; 300];
        let href2 = engine.heap_store(&data2).unwrap();
        assert_eq!(engine.heap_load(href2).unwrap(), data2);

        engine.close().await.unwrap();
    }
}
