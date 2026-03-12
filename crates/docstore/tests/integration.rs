//! Integration tests for exdb-docstore — cross-module workflows.

use exdb_core::encoding::encode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::{CollectionId, DocId, IndexId, Scalar};
use exdb_docstore::key_encoding::{
    encode_key_prefix, make_secondary_key, make_secondary_key_from_prefix, successor_key,
};
use exdb_docstore::{IndexBuilder, PrimaryIndex, SecondaryIndex, VacuumCoordinator};
use exdb_docstore::vacuum::VacuumCandidate;
use exdb_storage::btree::ScanDirection;
use exdb_storage::engine::{StorageConfig, StorageEngine};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use tokio_stream::StreamExt;

async fn engine() -> Arc<StorageEngine> {
    Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap())
}

fn doc_id(n: u8) -> DocId {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    DocId(bytes)
}

async fn make_primary(engine: &Arc<StorageEngine>) -> Arc<PrimaryIndex> {
    let btree = engine.create_btree().await.unwrap();
    Arc::new(PrimaryIndex::new(btree, engine.clone(), 4096))
}

async fn make_secondary(engine: &Arc<StorageEngine>, primary: &Arc<PrimaryIndex>) -> Arc<SecondaryIndex> {
    let btree = engine.create_btree().await.unwrap();
    Arc::new(SecondaryIndex::new(btree, primary.clone()))
}

/// Collect a stream into a Vec of Results, then unwrap.
async fn collect_stream<S, T, E>(mut stream: S) -> Result<Vec<T>, E>
where
    S: futures_core::Stream<Item = Result<T, E>> + Unpin,
{
    let mut results = Vec::new();
    while let Some(item) = stream.next().await {
        results.push(item?);
    }
    Ok(results)
}

// ─── Full write → build index → query workflow ───

#[tokio::test]
async fn write_build_query_flow() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    // Insert 5 documents
    for i in 0..5u8 {
        let doc = serde_json::json!({"name": format!("user_{i}"), "age": (20 + i) as i64});
        let body = encode_document(&doc);
        primary.insert_version(&doc_id(i), 1, Some(&body)).await.unwrap();
    }

    // Build secondary index on "name"
    let builder = IndexBuilder::new(primary.clone(), secondary.clone(), vec![FieldPath::single("name")]);
    let count = builder.build(10, None).await.unwrap();
    assert_eq!(count, 5);

    // Query by exact name
    let prefix = encode_key_prefix(&[Scalar::String("user_2".into())]);
    let upper = successor_key(&prefix);
    let results = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, doc_id(2));

    // Verify body through primary
    let body = primary.get_at_ts(&doc_id(2), 10).await.unwrap().unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["name"], "user_2");
}

// ─── MVCC multi-version time-travel ───

#[tokio::test]
async fn mvcc_time_travel() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let id = doc_id(1);

    primary.insert_version(&id, 1, Some(b"{\"v\":1}")).await.unwrap();
    primary.insert_version(&id, 5, Some(b"{\"v\":5}")).await.unwrap();
    primary.insert_version(&id, 10, Some(b"{\"v\":10}")).await.unwrap();

    // Before any version
    assert!(primary.get_at_ts(&id, 0).await.unwrap().is_none());

    // At each version
    assert_eq!(primary.get_at_ts(&id, 1).await.unwrap().unwrap(), b"{\"v\":1}");
    assert_eq!(primary.get_at_ts(&id, 3).await.unwrap().unwrap(), b"{\"v\":1}");
    assert_eq!(primary.get_at_ts(&id, 5).await.unwrap().unwrap(), b"{\"v\":5}");
    assert_eq!(primary.get_at_ts(&id, 7).await.unwrap().unwrap(), b"{\"v\":5}");
    assert_eq!(primary.get_at_ts(&id, 10).await.unwrap().unwrap(), b"{\"v\":10}");
    assert_eq!(primary.get_at_ts(&id, u64::MAX).await.unwrap().unwrap(), b"{\"v\":10}");
}

// ─── Multi-document scan at various timestamps ───

#[tokio::test]
async fn scan_at_various_timestamps() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;

    // doc 1: created at ts=1, updated at ts=5
    primary.insert_version(&doc_id(1), 1, Some(b"d1v1")).await.unwrap();
    primary.insert_version(&doc_id(1), 5, Some(b"d1v5")).await.unwrap();

    // doc 2: created at ts=3
    primary.insert_version(&doc_id(2), 3, Some(b"d2v3")).await.unwrap();

    // doc 3: created at ts=2, deleted at ts=4
    primary.insert_version(&doc_id(3), 2, Some(b"d3v2")).await.unwrap();
    primary.insert_version(&doc_id(3), 4, None).await.unwrap();

    // At ts=0: nothing visible
    let r = collect_stream(primary.scan_at_ts(0, ScanDirection::Forward)).await.unwrap();
    assert_eq!(r.len(), 0);

    // At ts=1: only doc 1
    let r = collect_stream(primary.scan_at_ts(1, ScanDirection::Forward)).await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, doc_id(1));
    assert_eq!(r[0].2, b"d1v1");

    // At ts=3: doc 1 (v1), doc 2, doc 3
    let r = collect_stream(primary.scan_at_ts(3, ScanDirection::Forward)).await.unwrap();
    assert_eq!(r.len(), 3);

    // At ts=4: doc 1 (v1), doc 2, doc 3 deleted
    let r = collect_stream(primary.scan_at_ts(4, ScanDirection::Forward)).await.unwrap();
    assert_eq!(r.len(), 2);

    // At ts=10: doc 1 (v5), doc 2
    let r = collect_stream(primary.scan_at_ts(10, ScanDirection::Forward)).await.unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].2, b"d1v5");
}

// ─── Backward scan integration ───

#[tokio::test]
async fn backward_scan_ordering() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;

    for i in 0..5u8 {
        let mut id = [0u8; 16];
        id[15] = i;
        primary.insert_version(&DocId(id), 1, Some(&[i])).await.unwrap();
    }

    let forward = collect_stream(primary.scan_at_ts(10, ScanDirection::Forward)).await.unwrap();
    let backward = collect_stream(primary.scan_at_ts(10, ScanDirection::Backward)).await.unwrap();

    assert_eq!(forward.len(), 5);
    assert_eq!(backward.len(), 5);

    // Backward should be reverse of forward
    for (i, fwd) in forward.iter().enumerate() {
        let bwd = &backward[backward.len() - 1 - i];
        assert_eq!(fwd.0, bwd.0);
        assert_eq!(fwd.2, bwd.2);
    }
}

// ─── Backward scan with MVCC ───

#[tokio::test]
async fn backward_scan_mvcc() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;

    // doc 1: two versions
    primary.insert_version(&doc_id(1), 1, Some(b"v1")).await.unwrap();
    primary.insert_version(&doc_id(1), 5, Some(b"v5")).await.unwrap();

    // doc 2: one version
    primary.insert_version(&doc_id(2), 3, Some(b"d2")).await.unwrap();

    // At ts=4: should see doc1@v1, doc2@v3
    let results = collect_stream(primary.scan_at_ts(4, ScanDirection::Backward)).await.unwrap();
    assert_eq!(results.len(), 2);
    // Backward: doc2 first, then doc1
    assert_eq!(results[0].0, doc_id(2));
    assert_eq!(results[1].0, doc_id(1));
    assert_eq!(results[1].2, b"v1");
}

// ─── Insert, update value, vacuum old version, verify ───

#[tokio::test]
async fn insert_update_vacuum_cycle() {
    let eng = engine().await;
    let primary_btree = eng.create_btree().await.unwrap();
    let primary = Arc::new(PrimaryIndex::new(primary_btree, eng.clone(), 4096));
    let sec_btree = eng.create_btree().await.unwrap();
    let secondary = Arc::new(SecondaryIndex::new(sec_btree, primary.clone()));

    let d = doc_id(1);

    // Insert v1 with secondary index entry
    let doc_v1 = serde_json::json!({"name": "Alice"});
    let body_v1 = encode_document(&doc_v1);
    primary.insert_version(&d, 1, Some(&body_v1)).await.unwrap();
    let sec_key_v1 = make_secondary_key(&[Scalar::String("Alice".into())], &d, 1);
    secondary.insert_entry(&sec_key_v1).await.unwrap();

    // Update to v2 with new secondary entry
    let doc_v2 = serde_json::json!({"name": "Bob"});
    let body_v2 = encode_document(&doc_v2);
    primary.insert_version(&d, 5, Some(&body_v2)).await.unwrap();
    let sec_key_v2 = make_secondary_key(&[Scalar::String("Bob".into())], &d, 5);
    secondary.insert_entry(&sec_key_v2).await.unwrap();

    // Before vacuum: both versions exist in primary
    assert!(primary.get_at_ts(&d, 1).await.unwrap().is_some());

    // Vacuum old version
    let mut primaries = HashMap::new();
    primaries.insert(CollectionId(1), primary.clone());
    let mut secondaries = HashMap::new();
    secondaries.insert(IndexId(1), secondary.clone());

    let mut vc = VacuumCoordinator::new();
    vc.push_candidate(VacuumCandidate {
        collection_id: CollectionId(1),
        doc_id: d,
        old_ts: 1,
        superseding_ts: 5,
        old_index_keys: vec![(IndexId(1), sec_key_v1.clone())],
    });

    let eligible = vc.drain_eligible(10);
    let removed = vc.execute(&eligible, &primaries, &secondaries).await.unwrap();
    assert!(removed >= 2); // primary + secondary entry

    // After vacuum: v1 gone, v2 still accessible
    assert!(primary.get_at_ts(&d, 3).await.unwrap().is_none()); // v1 vacuumed, nothing before v5
    assert_eq!(primary.get_at_ts(&d, 10).await.unwrap().unwrap(), body_v2);

    // Secondary scan for "Alice" returns nothing
    let alice_prefix = encode_key_prefix(&[Scalar::String("Alice".into())]);
    let alice_upper = successor_key(&alice_prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&alice_prefix), Bound::Excluded(&alice_upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert!(r.is_empty());

    // Secondary scan for "Bob" works
    let bob_prefix = encode_key_prefix(&[Scalar::String("Bob".into())]);
    let bob_upper = successor_key(&bob_prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&bob_prefix), Bound::Excluded(&bob_upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, d);
}

// ─── Index build then update: stale entry detection ───

#[tokio::test]
async fn index_build_then_update_stale_detection() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    let d = doc_id(1);
    let doc_v1 = serde_json::json!({"status": "active"});
    primary.insert_version(&d, 1, Some(&encode_document(&doc_v1))).await.unwrap();

    // Build index at ts=5
    let builder = IndexBuilder::new(primary.clone(), secondary.clone(), vec![FieldPath::single("status")]);
    let count = builder.build(5, None).await.unwrap();
    assert_eq!(count, 1);

    // Update document at ts=10 (secondary index now has stale entry for ts=1)
    let doc_v2 = serde_json::json!({"status": "inactive"});
    primary.insert_version(&d, 10, Some(&encode_document(&doc_v2))).await.unwrap();
    // Insert new secondary entry at ts=10
    let new_key = make_secondary_key(&[Scalar::String("inactive".into())], &d, 10);
    secondary.insert_entry(&new_key).await.unwrap();

    // Query for "active" at ts=15: stale entry should be filtered by primary verification
    let prefix = encode_key_prefix(&[Scalar::String("active".into())]);
    let upper = successor_key(&prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 15, ScanDirection::Forward))
        .await.unwrap();
    assert!(r.is_empty(), "stale entry should be filtered out");

    // Query for "inactive" at ts=15: should find it
    let prefix = encode_key_prefix(&[Scalar::String("inactive".into())]);
    let upper = successor_key(&prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 15, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
}

// ─── Tombstone document not returned by secondary scan ───

#[tokio::test]
async fn secondary_scan_after_delete() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    let d = doc_id(1);
    primary.insert_version(&d, 1, Some(b"{\"x\":1}")).await.unwrap();
    let key = make_secondary_key(&[Scalar::Int64(1)], &d, 1);
    secondary.insert_entry(&key).await.unwrap();

    // Delete at ts=5
    primary.insert_version(&d, 5, None).await.unwrap();

    // At ts=3: document still visible
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 3, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);

    // At ts=10: document deleted
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward))
        .await.unwrap();
    assert!(r.is_empty());
}

// ─── Compound secondary index with arrays ───

#[tokio::test]
async fn compound_index_with_array_build_and_query() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    // Document with array field + scalar field
    let doc = serde_json::json!({"category": "tech", "tags": ["rust", "db", "mvcc"]});
    let body = encode_document(&doc);
    let d = doc_id(1);
    primary.insert_version(&d, 1, Some(&body)).await.unwrap();

    // Build compound index on [category, tags]
    let paths = vec![FieldPath::single("category"), FieldPath::single("tags")];
    let builder = IndexBuilder::new(primary.clone(), secondary.clone(), paths);
    let count = builder.build(10, None).await.unwrap();
    assert_eq!(count, 3); // 3 array elements → 3 entries

    // Query for category="tech", tags="rust"
    let prefix = encode_key_prefix(&[Scalar::String("tech".into()), Scalar::String("rust".into())]);
    let upper = successor_key(&prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, d);
}

// ─── Many documents: build, scan, verify count ───

#[tokio::test]
async fn many_documents_index_build() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    for i in 0..200u32 {
        let mut id = [0u8; 16];
        id[12..16].copy_from_slice(&i.to_be_bytes());
        let doc = serde_json::json!({"val": i as i64});
        primary.insert_version(&DocId(id), 1, Some(&encode_document(&doc))).await.unwrap();
    }

    let builder = IndexBuilder::new(primary.clone(), secondary.clone(), vec![FieldPath::single("val")]);
    let count = builder.build(10, None).await.unwrap();
    assert_eq!(count, 200);

    // Full scan should return all 200
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 200);
}

// ─── Rollback then re-query ───

#[tokio::test]
async fn rollback_restores_previous_state() {
    let eng = engine().await;
    let primary_btree = eng.create_btree().await.unwrap();
    let primary = Arc::new(PrimaryIndex::new(primary_btree, eng.clone(), 4096));
    let sec_btree = eng.create_btree().await.unwrap();
    let secondary = Arc::new(SecondaryIndex::new(sec_btree, primary.clone()));

    let d = doc_id(1);
    primary.insert_version(&d, 1, Some(b"original")).await.unwrap();
    let sec_key_v1 = make_secondary_key(&[Scalar::String("orig".into())], &d, 1);
    secondary.insert_entry(&sec_key_v1).await.unwrap();

    // "Commit" at ts=5 (simulating a failed replication)
    primary.insert_version(&d, 5, Some(b"updated")).await.unwrap();
    let sec_key_v2 = make_secondary_key(&[Scalar::String("upd".into())], &d, 5);
    secondary.insert_entry(&sec_key_v2).await.unwrap();

    // Rollback ts=5
    let mut primaries = HashMap::new();
    primaries.insert(CollectionId(1), primary.clone());
    let mut secondaries = HashMap::new();
    secondaries.insert(IndexId(1), secondary.clone());

    exdb_docstore::vacuum::RollbackVacuum::rollback_commit(
        5,
        &[(CollectionId(1), d)],
        &[(IndexId(1), Some(sec_key_v2))],
        &primaries,
        &secondaries,
    ).await.unwrap();

    // Original version visible again at ts=10
    assert_eq!(primary.get_at_ts(&d, 10).await.unwrap().unwrap(), b"original");

    // Secondary scan for "orig" works
    let prefix = encode_key_prefix(&[Scalar::String("orig".into())]);
    let upper = successor_key(&prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
}

// ─── External storage through full pipeline ───

#[tokio::test]
async fn external_storage_scan() {
    let eng = engine().await;
    let btree = eng.create_btree().await.unwrap();
    // Threshold of 10 to force external storage
    let primary = Arc::new(PrimaryIndex::new(btree, eng.clone(), 10));

    let big_body = vec![0xABu8; 500];
    for i in 0..5u8 {
        let mut id = [0u8; 16];
        id[15] = i;
        primary.insert_version(&DocId(id), 1, Some(&big_body)).await.unwrap();
    }

    // Scan should load all bodies from heap
    let results = collect_stream(primary.scan_at_ts(10, ScanDirection::Forward)).await.unwrap();
    assert_eq!(results.len(), 5);
    for (_, _, body) in &results {
        assert_eq!(body.len(), 500);
        assert!(body.iter().all(|&b| b == 0xAB));
    }
}

// ─── External storage backward scan ───

#[tokio::test]
async fn external_storage_backward_scan() {
    let eng = engine().await;
    let btree = eng.create_btree().await.unwrap();
    let primary = Arc::new(PrimaryIndex::new(btree, eng.clone(), 10));

    for i in 0..3u8 {
        let mut id = [0u8; 16];
        id[15] = i;
        let body = vec![i; 100];
        primary.insert_version(&DocId(id), 1, Some(&body)).await.unwrap();
    }

    let results = collect_stream(primary.scan_at_ts(10, ScanDirection::Backward)).await.unwrap();
    assert_eq!(results.len(), 3);
    // Backward: doc_id(2) first, then 1, then 0
    assert_eq!(results[0].0, doc_id(2));
    assert_eq!(results[2].0, doc_id(0));
}

// ─── Empty body document ───

#[tokio::test]
async fn empty_body_document() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let d = doc_id(1);

    primary.insert_version(&d, 1, Some(b"")).await.unwrap();
    let body = primary.get_at_ts(&d, 5).await.unwrap().unwrap();
    assert!(body.is_empty());

    // Also works in scan
    let results = collect_stream(primary.scan_at_ts(5, ScanDirection::Forward)).await.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].2.is_empty());
}

// ─── Secondary index: same value, different doc_ids ───

#[tokio::test]
async fn secondary_same_value_multiple_docs() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    for i in 0..5u8 {
        let d = doc_id(i);
        primary.insert_version(&d, 1, Some(b"body")).await.unwrap();
        // All share the same secondary key value
        let key = make_secondary_key(&[Scalar::String("shared".into())], &d, 1);
        secondary.insert_entry(&key).await.unwrap();
    }

    let prefix = encode_key_prefix(&[Scalar::String("shared".into())]);
    let upper = successor_key(&prefix);
    let results = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(results.len(), 5);
}

// ─── Secondary index backward scan ───

#[tokio::test]
async fn secondary_backward_scan() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    for i in 0..3u8 {
        let d = doc_id(i);
        primary.insert_version(&d, 1, Some(b"body")).await.unwrap();
        let key = make_secondary_key(&[Scalar::Int64(i as i64)], &d, 1);
        secondary.insert_entry(&key).await.unwrap();
    }

    let forward = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward))
        .await.unwrap();
    let backward = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Backward))
        .await.unwrap();

    assert_eq!(forward.len(), 3);
    assert_eq!(backward.len(), 3);

    // Backward is reverse of forward
    for (i, fwd) in forward.iter().enumerate() {
        let bwd = &backward[backward.len() - 1 - i];
        assert_eq!(fwd.0, bwd.0);
    }
}

// ─── make_secondary_key_from_prefix matches make_secondary_key ───

#[tokio::test]
async fn secondary_key_from_prefix_consistency() {
    let d = doc_id(42);
    let values = [
        Scalar::Int64(100),
        Scalar::String("test".into()),
        Scalar::Boolean(true),
    ];
    let key_direct = make_secondary_key(&values, &d, 50);
    let prefix = encode_key_prefix(&values);
    let key_from_prefix = make_secondary_key_from_prefix(&prefix, &d, 50);
    assert_eq!(key_direct, key_from_prefix);
}

// ─── Multiple collections vacuum ───

#[tokio::test]
async fn vacuum_multiple_collections() {
    let eng = engine().await;

    // Two collections
    let primary1_btree = eng.create_btree().await.unwrap();
    let primary1 = Arc::new(PrimaryIndex::new(primary1_btree, eng.clone(), 4096));
    let primary2_btree = eng.create_btree().await.unwrap();
    let primary2 = Arc::new(PrimaryIndex::new(primary2_btree, eng.clone(), 4096));

    let d1 = doc_id(1);
    let d2 = doc_id(2);

    primary1.insert_version(&d1, 1, Some(b"old1")).await.unwrap();
    primary1.insert_version(&d1, 5, Some(b"new1")).await.unwrap();
    primary2.insert_version(&d2, 2, Some(b"old2")).await.unwrap();
    primary2.insert_version(&d2, 6, Some(b"new2")).await.unwrap();

    let mut primaries = HashMap::new();
    primaries.insert(CollectionId(1), primary1.clone());
    primaries.insert(CollectionId(2), primary2.clone());
    let secondaries = HashMap::new();

    let vc = VacuumCoordinator::new();
    let candidates = vec![
        VacuumCandidate {
            collection_id: CollectionId(1),
            doc_id: d1,
            old_ts: 1,
            superseding_ts: 5,
            old_index_keys: vec![],
        },
        VacuumCandidate {
            collection_id: CollectionId(2),
            doc_id: d2,
            old_ts: 2,
            superseding_ts: 6,
            old_index_keys: vec![],
        },
    ];

    let removed = vc.execute(&candidates, &primaries, &secondaries).await.unwrap();
    assert_eq!(removed, 2);

    // Old versions gone, new versions still there
    assert!(primary1.get_at_ts(&d1, 3).await.unwrap().is_none());
    assert_eq!(primary1.get_at_ts(&d1, 10).await.unwrap().unwrap(), b"new1");
    assert!(primary2.get_at_ts(&d2, 4).await.unwrap().is_none());
    assert_eq!(primary2.get_at_ts(&d2, 10).await.unwrap().unwrap(), b"new2");
}

// ─── Delete-then-reinsert through secondary index ───

#[tokio::test]
async fn delete_reinsert_secondary() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;
    let d = doc_id(1);

    // Insert
    primary.insert_version(&d, 1, Some(b"v1")).await.unwrap();
    let k1 = make_secondary_key(&[Scalar::String("val".into())], &d, 1);
    secondary.insert_entry(&k1).await.unwrap();

    // Delete
    primary.insert_version(&d, 5, None).await.unwrap();

    // Reinsert
    primary.insert_version(&d, 10, Some(b"v10")).await.unwrap();
    let k10 = make_secondary_key(&[Scalar::String("val".into())], &d, 10);
    secondary.insert_entry(&k10).await.unwrap();

    // At ts=3: alive
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 3, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);

    // At ts=7: deleted
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 7, ScanDirection::Forward))
        .await.unwrap();
    assert!(r.is_empty());

    // At ts=15: alive again
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 15, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].1, 10);
}

// ─── Index build with MVCC: multiple versions, only latest visible ───

#[tokio::test]
async fn index_build_resolves_mvcc() {
    let eng = engine().await;
    let primary = make_primary(&eng).await;
    let secondary = make_secondary(&eng, &primary).await;

    let d = doc_id(1);
    // Three versions of the same doc with different names
    primary.insert_version(&d, 1, Some(&encode_document(&serde_json::json!({"name": "v1"})))).await.unwrap();
    primary.insert_version(&d, 5, Some(&encode_document(&serde_json::json!({"name": "v5"})))).await.unwrap();
    primary.insert_version(&d, 10, Some(&encode_document(&serde_json::json!({"name": "v10"})))).await.unwrap();

    // Build at ts=7: should only index v5
    let builder = IndexBuilder::new(primary.clone(), secondary.clone(), vec![FieldPath::single("name")]);
    let count = builder.build(7, None).await.unwrap();
    assert_eq!(count, 1);

    // Verify the entry is for "v5"
    let prefix = encode_key_prefix(&[Scalar::String("v5".into())]);
    let upper = successor_key(&prefix);
    let r = collect_stream(secondary
        .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 7, ScanDirection::Forward))
        .await.unwrap();
    assert_eq!(r.len(), 1);
}
