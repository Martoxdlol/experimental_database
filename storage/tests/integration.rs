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

use storage::backend::{MemoryPageStorage, MemoryWalStorage, PageStorage, WalStorage};
use storage::btree::ScanDirection;
use storage::catalog_btree::{
    self, CatalogEntityType, CatalogIndexState, CollectionEntry, IndexEntry, IndexType,
};
use storage::engine::{StorageConfig, StorageEngine};
use storage::recovery::{NoOpHandler, WalRecordHandler};
use storage::vacuum::VacuumEntry;
use storage::wal::WalRecord;

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
            assert_eq!(
                value,
                Some(expected.into_bytes()),
                "missing key {}",
                key
            );
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
    assert_eq!(
        handle.get(b"custom").unwrap(),
        Some(b"backend".to_vec())
    );
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
        let idx_key =
            catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx1.index_id);
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
        let name_key =
            catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
        let name_val = name_idx.get(&name_key).unwrap().unwrap();
        let resolved_id = catalog_btree::deserialize_name_value(&name_val);
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
        assert!(window[1] > window[0], "LSNs must be monotonically increasing");
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
        let href = storage::heap::HeapRef::from_bytes(href_bytes[..6].try_into().unwrap());
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
