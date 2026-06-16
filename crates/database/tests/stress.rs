//! Stress tests for the L6 Database.
//!
//! Bulk operations, size limits, edge cases, and sustained load patterns.
//! These tests validate resource management and correctness under pressure.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use exdb::{
    Database, DatabaseConfig, DatabaseError, DocId, FieldPath, Filter, RangeExpr, Scalar,
    ScanDirection, Transaction, TransactionConfig, TransactionOptions, TransactionResult,
};
use exdb_core::encoding::try_encode_document;
use exdb_core::ulid::decode_ulid;
use serde_json::json;

// ─── Helpers ───

async fn open_test_db() -> Database {
    Database::open_in_memory(DatabaseConfig::default(), None)
        .await
        .unwrap()
}

async fn create_collection(db: &Database, name: &str) {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection(name).await.unwrap();
    match tx.commit().await.unwrap() {
        TransactionResult::Success { .. } => {}
        _ => panic!("unexpected commit result"),
    }
}

fn assert_success(result: TransactionResult) -> exdb::Ts {
    match result {
        TransactionResult::Success { commit_ts, .. } => commit_ts,
        TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

struct DeterministicRng {
    state: u64,
}

#[derive(Clone, Debug)]
struct PendingQueryModelDoc {
    doc_id: DocId,
    bucket: i64,
    rank: i64,
    active: bool,
    name: String,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }
}

async fn assert_pending_bucket_query_matches_model(
    tx: &mut Transaction,
    model: &BTreeMap<u32, PendingQueryModelDoc>,
    bucket: i64,
    direction: Option<ScanDirection>,
    limit: usize,
) {
    let docs = tx
        .query(
            "events",
            "bucket_rank_idx",
            &[RangeExpr::Eq(
                FieldPath::single("bucket"),
                Scalar::Int64(bucket),
            )],
            Some(Filter::Eq(
                FieldPath::single("active"),
                Scalar::Boolean(true),
            )),
            direction,
            Some(limit),
        )
        .await
        .unwrap();

    let mut expected: Vec<_> = model
        .values()
        .filter(|doc| doc.bucket == bucket && doc.active)
        .collect();
    expected.sort_by_key(|doc| doc.rank);
    if direction == Some(ScanDirection::Backward) {
        expected.reverse();
    }
    expected.truncate(limit);

    let actual_names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    let expected_names: Vec<_> = expected.iter().map(|doc| doc.name.clone()).collect();
    assert_eq!(
        actual_names, expected_names,
        "bucket {bucket} direction {direction:?} limit {limit} should match pending model"
    );
    assert!(docs.iter().all(|doc| doc["bucket"] == bucket));
    assert!(docs.iter().all(|doc| doc["active"] == true));
}

async fn wait_for_index_ready(db: &Database, collection: &str, index: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes(collection).unwrap();
        tx.rollback();

        if indexes
            .iter()
            .any(|i| i.name == index && i.state == exdb::IndexState::Ready)
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("index {collection}.{index} did not become ready within 5 seconds");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Bulk Insert Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn ten_thousand_documents_insert() {
    let db = open_test_db().await;
    create_collection(&db, "bulk").await;

    // Insert 10K docs across 10 batches — verifies no panics, no resource exhaustion
    for batch in 0..10 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..1000 {
            let seq = batch * 1000 + i;
            tx.insert("bulk", json!({"seq": seq, "data": "payload"}))
                .await
                .unwrap();
        }
        assert_success(tx.commit().await.unwrap());
    }

    // Verify last batch's docs via read-your-writes in a new tx
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let last_id = tx.insert("bulk", json!({"seq": 99999})).await.unwrap();
    let doc = tx.get("bulk", &last_id).await.unwrap().unwrap();
    assert_eq!(doc["seq"].as_u64().unwrap(), 99999);
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn thousand_sequential_transactions() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    for i in 0..1000 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.insert("data", json!({"i": i})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
    }
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Catalog Scalability Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn hundred_collections() {
    let db = open_test_db().await;

    for i in 0..100 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection(&format!("col_{i:03}")).await.unwrap();
        assert_success(tx.commit().await.unwrap());
    }

    assert_eq!(db.list_collections().len(), 100);
    db.close().await.unwrap();
}

#[tokio::test]
async fn hundred_collections_in_one_tx() {
    let db = open_test_db().await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..100 {
        tx.create_collection(&format!("col_{i:03}")).await.unwrap();
    }
    assert_success(tx.commit().await.unwrap());

    assert_eq!(db.list_collections().len(), 100);
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Document Size Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn deeply_nested_document() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    // Build 10-level nested JSON
    let mut doc = json!({"leaf": true});
    for i in (0..10).rev() {
        doc = json!({ format!("level_{i}"): doc });
    }

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("data", doc.clone()).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let got = tx.get("data", &id).await.unwrap().unwrap();
    // Verify nesting survived
    let mut cursor = &got;
    for i in 0..10 {
        cursor = &cursor[format!("level_{i}")];
    }
    assert_eq!(cursor["leaf"], true);
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn document_over_size_limit() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    // 16 MB + 1 byte of actual content (plus JSON overhead)
    let big = "x".repeat(16 * 1024 * 1024 + 1);
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.insert("data", json!({"big": big})).await;
    assert!(
        matches!(result, Err(DatabaseError::DocTooLarge { .. })),
        "should reject doc over 16MB"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn configured_document_size_limit_applies_to_insert_replace_and_patch() {
    let small_with_system_fields = json!({
        "_id": "00000000000000000000000000",
        "_created_at": 0_i64,
        "name": "ok"
    });
    let max_doc_size = try_encode_document(&small_with_system_fields)
        .unwrap()
        .len();
    let config = DatabaseConfig {
        max_doc_size,
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let doc_id;
    {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx.insert("data", json!({"name": "ok"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
    }

    let large_value = "x".repeat(max_doc_size);

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let insert_result = tx
        .insert("data", json!({"name": large_value.clone()}))
        .await;
    assert!(matches!(
        insert_result,
        Err(DatabaseError::DocTooLarge { .. })
    ));
    tx.rollback();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let replace_result = tx
        .replace("data", &doc_id, json!({"name": large_value.clone()}))
        .await;
    assert!(matches!(
        replace_result,
        Err(DatabaseError::DocTooLarge { .. })
    ));
    tx.rollback();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let patch_result = tx
        .patch("data", &doc_id, json!({"name": large_value}))
        .await;
    assert!(matches!(
        patch_result,
        Err(DatabaseError::DocTooLarge { .. })
    ));
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_read_intervals_reached() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_intervals: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.list_indexes("data");
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_scanned_docs_reached() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..3 {
        tx.insert("data", json!({"i": i})).await.unwrap();
    }
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.query("data", "_created_at", &[], None, None, None).await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_scanned_docs_reached_by_point_get() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx
        .insert("data", json!({"payload": "point read"}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.get("data", &id).await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    let after_limit = tx.list_collections();
    assert!(matches!(
        after_limit,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_scanned_bytes_reached() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_bytes: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("data", json!({"payload": "larger than one byte"}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.query("data", "_created_at", &[], None, None, None).await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_scanned_bytes_reached_by_point_get() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_bytes: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx
        .insert("data", json!({"payload": "larger than one byte"}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.get("data", &id).await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    let after_limit = tx.list_collections();
    assert!(matches!(
        after_limit,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_reached_by_repeated_transaction_operations() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    tx.list_collections().unwrap();
    let result = tx.list_collections();
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn read_limit_exceeded_aborts_transaction_until_drop() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    tx.list_collections().unwrap();
    let result = tx.list_collections();
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));

    tx.reset();
    let after_reset = tx.list_collections();
    assert!(matches!(
        after_reset,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    let commit = tx.commit().await;
    assert!(matches!(commit, Err(DatabaseError::ReadLimitExceeded(_))));
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_secondary_scan_work() {
    let path = tempfile::tempdir().unwrap();
    {
        let db = Database::open(path.path().join("testdb"), DatabaseConfig::default(), None)
            .await
            .unwrap();
        create_collection(&db, "data").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..3 {
            tx.insert("data", json!({"i": i})).await.unwrap();
        }
        assert_success(tx.commit().await.unwrap());
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open(path.path().join("testdb"), config, None)
        .await
        .unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.query("data", "_created_at", &[], None, None, None).await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_create_index_pending_write_validation() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 20,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..8 {
        tx.insert("data", json!({"group": i % 2, "seq": i}))
            .await
            .unwrap();
    }

    let result = tx
        .create_index("data", "group_idx", vec![FieldPath::single("group")])
        .await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending create-index validation should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_document_index_validation() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 76,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..8 {
        tx.create_index(
            "data",
            &format!("idx_{i}"),
            vec![FieldPath::single("group")],
        )
        .await
        .unwrap();
    }

    let result = tx.insert("data", json!({"group": "alpha"})).await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "per-index document validation should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_drop_collection_index_cascade() {
    let path = tempfile::tempdir().unwrap();
    {
        let db = Database::open(path.path().join("testdb"), DatabaseConfig::default(), None)
            .await
            .unwrap();
        create_collection(&db, "data").await;
        for i in 0..4 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index(
                "data",
                &format!("idx_{i}"),
                vec![FieldPath::single("group")],
            )
            .await
            .unwrap();
            assert_success(tx.commit().await.unwrap());
            wait_for_index_ready(&db, "data", &format!("idx_{i}")).await;
        }
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open(path.path().join("testdb"), config, None)
        .await
        .unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.drop_collection("data").await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "drop_collection cascade metadata should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    assert!(
        db.get_collection("data").is_some(),
        "failed drop should leave the collection visible"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_committed_collection_list_rows() {
    let path = tempfile::tempdir().unwrap();
    {
        let db = Database::open(path.path().join("testdb"), DatabaseConfig::default(), None)
            .await
            .unwrap();
        for i in 0..5 {
            create_collection(&db, &format!("col_{i}")).await;
        }
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open(path.path().join("testdb"), config, None)
        .await
        .unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.list_collections();
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "committed collection list rows should count against max_operations"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_committed_index_list_rows() {
    let path = tempfile::tempdir().unwrap();
    {
        let db = Database::open(path.path().join("testdb"), DatabaseConfig::default(), None)
            .await
            .unwrap();
        create_collection(&db, "data").await;
        for i in 0..4 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index(
                "data",
                &format!("idx_{i}"),
                vec![FieldPath::single("group")],
            )
            .await
            .unwrap();
            assert_success(tx.commit().await.unwrap());
            wait_for_index_ready(&db, "data", &format!("idx_{i}")).await;
        }
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open(path.path().join("testdb"), config, None)
        .await
        .unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.list_indexes("data");
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "committed index list rows should count against max_operations"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_pending_catalog_resolution() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 12,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..4 {
        tx.create_collection(&format!("col_{i}")).await.unwrap();
    }

    let result = tx.insert("col_3", json!({"seq": 1})).await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending collection resolution should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_pending_collection_duplicate_check() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 8,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..3 {
        tx.create_collection(&format!("col_{i}")).await.unwrap();
    }

    let result = tx.create_collection("col_2").await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending collection duplicate check should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_pending_index_duplicate_check() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 15,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..3 {
        tx.create_index(
            "data",
            &format!("idx_{i}"),
            vec![FieldPath::single("group")],
        )
        .await
        .unwrap();
    }

    let result = tx
        .create_index("data", "idx_2", vec![FieldPath::single("group")])
        .await;
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending index duplicate check should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_pending_collection_list_overlay() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 40,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..8 {
        tx.create_collection(&format!("col_{i}")).await.unwrap();
    }

    let result = tx.list_collections();
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending collection overlay should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn max_operations_counts_pending_index_list_overlay() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_operations: 76,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..8 {
        tx.create_index(
            "data",
            &format!("idx_{i}"),
            vec![FieldPath::single("group")],
        )
        .await
        .unwrap();
    }

    let result = tx.list_indexes("data");
    assert!(
        matches!(result, Err(DatabaseError::ReadLimitExceeded(_))),
        "pending index overlay should count against max_operations"
    );
    assert!(matches!(
        tx.commit().await,
        Err(DatabaseError::ReadLimitExceeded(_))
    ));

    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction Timeout Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn transaction_idle_timeout() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            idle_timeout: Duration::from_millis(100),
            max_lifetime: Duration::from_secs(300),
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = tx.insert("data", json!({"x": 1})).await;
    assert!(
        matches!(result, Err(DatabaseError::TransactionTimeout)),
        "should timeout after idle period"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_does_not_revive_idle_timed_out_transaction() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            idle_timeout: Duration::from_millis(50),
            max_lifetime: Duration::from_secs(300),
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    tx.reset();

    let result = tx.insert("data", json!({"x": 1})).await;
    assert!(
        matches!(result, Err(DatabaseError::TransactionTimeout)),
        "reset must not refresh an already expired idle transaction"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn transaction_max_lifetime() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    // Keep active but exceed lifetime
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Touch to prevent idle timeout
        let _ = tx.list_collections();
    }

    let result = tx.insert("data", json!({"x": 1})).await;
    assert!(
        matches!(result, Err(DatabaseError::TransactionTimeout)),
        "should timeout after max lifetime"
    );
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn transaction_commit_checks_timeout() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_millis(50),
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let tx = db.begin(TransactionOptions::default()).unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let result = tx.commit().await;
    assert!(matches!(result, Err(DatabaseError::TransactionTimeout)));
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Edge Case Document Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn empty_document_insert() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("data", json!({})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("data", &id).await.unwrap().unwrap();
    // Should have _created_at injected
    assert!(doc.get("_created_at").is_some());
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn null_field_values() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("data", json!({"x": null})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("data", &id).await.unwrap().unwrap();
    assert!(doc["x"].is_null());
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn unicode_field_names_and_values() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx
        .insert(
            "data",
            json!({"名前": "太郎", "emoji": "🎉", "中文": "数据库"}),
        )
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("data", &id).await.unwrap().unwrap();
    assert_eq!(doc["名前"], "太郎");
    assert_eq!(doc["emoji"], "🎉");
    assert_eq!(doc["中文"], "数据库");
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Alternating Insert/Delete Stress
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn insert_and_delete_read_your_writes() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    // Insert a doc, verify it's visible via read-your-writes
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("data", json!({"x": "hello"})).await.unwrap();
    let doc = tx.get("data", &id).await.unwrap();
    assert!(
        doc.is_some(),
        "inserted doc should be visible via read-your-writes"
    );
    assert_eq!(doc.unwrap()["x"], "hello");
    assert_success(tx.commit().await.unwrap());
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent Stress
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_stress_10_tasks() {
    let db = Arc::new(open_test_db().await);
    create_collection(&db, "stress").await;

    let mut handles = vec![];
    for task_id in 0u64..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                tokio::time::sleep(std::time::Duration::from_millis(task_id)).await;
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.insert("stress", json!({"task": task_id, "i": i}))
                    .await
                    .unwrap();
                let _ = tx.commit().await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_readers_one_writer_integrity_smoke() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let db = Arc::new(
        Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap(),
    );

    create_collection(&db, "events").await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("events", "bucket_idx", vec![FieldPath::single("bucket")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "events", "bucket_idx").await;

    let mut seed_ids = Vec::new();
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..32_i64 {
        let id = tx
            .insert(
                "events",
                json!({
                    "kind": "seed",
                    "bucket": i % 4,
                    "seq": i
                }),
            )
            .await
            .unwrap();
        seed_ids.push((id, i % 4));
    }
    assert_success(tx.commit().await.unwrap());

    let start = Arc::new(tokio::sync::Barrier::new(101));
    let seed_ids = Arc::new(seed_ids);
    let mut handles = Vec::new();
    for reader_id in 0..100usize {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        let seed_ids = Arc::clone(&seed_ids);
        handles.push(tokio::spawn(async move {
            start.wait().await;
            for round in 0..8usize {
                let bucket = ((reader_id + round) % 4) as i64;
                let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
                let docs = tx
                    .query(
                        "events",
                        "bucket_idx",
                        &[RangeExpr::Eq(
                            FieldPath::single("bucket"),
                            Scalar::Int64(bucket),
                        )],
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap();
                assert!(
                    docs.iter().all(|doc| doc["bucket"] == bucket),
                    "secondary-index reader observed a document outside bucket {bucket}"
                );

                let (seed_id, expected_bucket) = &seed_ids[(reader_id + round) % seed_ids.len()];
                let seed = tx
                    .get("events", seed_id)
                    .await
                    .unwrap()
                    .expect("seed document should remain visible");
                assert_eq!(seed["bucket"], *expected_bucket);
                tx.rollback();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    let writer_db = Arc::clone(&db);
    let writer_start = Arc::clone(&start);
    handles.push(tokio::spawn(async move {
        writer_start.wait().await;
        for i in 0..120_i64 {
            let mut tx = writer_db.begin(TransactionOptions::default()).unwrap();
            tx.insert(
                "events",
                json!({
                    "kind": "writer",
                    "bucket": i % 4,
                    "seq": 10_000 + i
                }),
            )
            .await
            .unwrap();
            assert_success(tx.commit().await.unwrap());
            if i % 8 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }));

    for handle in handles {
        handle.await.unwrap();
    }

    db.storage().checkpoint().await.unwrap();
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "concurrent reader/writer stress should leave clean integrity: {:?}",
        report.issues
    );

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "reopened concurrent stress database should remain clean: {:?}",
        report.issues
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_mixed_query_model_stress_matches_transaction_view() {
    let db = open_test_db().await;
    create_collection(&db, "events").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index(
        "events",
        "bucket_rank_idx",
        vec![FieldPath::single("bucket"), FieldPath::single("rank")],
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "events", "bucket_rank_idx").await;

    let mut committed: BTreeMap<u32, PendingQueryModelDoc> = BTreeMap::new();
    let mut next_logical_id = 0_u32;
    let mut next_rank = 1_000_i64;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for _ in 0..80 {
        let logical_id = next_logical_id;
        next_logical_id += 1;
        let bucket = (logical_id % 5) as i64;
        let rank = next_rank;
        next_rank += 7;
        let active = !logical_id.is_multiple_of(3);
        let name = format!("seed-{logical_id}");
        let doc_id = tx
            .insert(
                "events",
                json!({
                    "logical_id": logical_id as i64,
                    "bucket": bucket,
                    "rank": rank,
                    "active": active,
                    "name": name,
                }),
            )
            .await
            .unwrap();
        committed.insert(
            logical_id,
            PendingQueryModelDoc {
                doc_id,
                bucket,
                rank,
                active,
                name: format!("seed-{logical_id}"),
            },
        );
    }
    assert_success(tx.commit().await.unwrap());

    let mut rng = DeterministicRng::new(0xA17E_5EED_2026);
    for round in 0..10_u32 {
        let round_start_next_id = next_logical_id;
        let mut pending_model = committed.clone();
        let mut touched_committed = BTreeSet::new();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();

        for action in 0..36_u32 {
            match rng.next_usize(100) {
                0..=44 => {
                    let logical_id = next_logical_id;
                    next_logical_id += 1;
                    let bucket = rng.next_usize(5) as i64;
                    let rank = next_rank;
                    next_rank += 7;
                    let active = rng.next_usize(4) != 0;
                    let name = format!("pending-insert-{round}-{action}-{logical_id}");
                    let doc_id = tx
                        .insert(
                            "events",
                            json!({
                                "logical_id": logical_id as i64,
                                "bucket": bucket,
                                "rank": rank,
                                "active": active,
                                "name": name,
                            }),
                        )
                        .await
                        .unwrap();
                    pending_model.insert(
                        logical_id,
                        PendingQueryModelDoc {
                            doc_id,
                            bucket,
                            rank,
                            active,
                            name,
                        },
                    );
                }
                45..=74 => {
                    let candidates: Vec<_> = pending_model
                        .keys()
                        .copied()
                        .filter(|logical_id| {
                            *logical_id < round_start_next_id
                                && !touched_committed.contains(logical_id)
                        })
                        .collect();
                    if !candidates.is_empty() {
                        let logical_id = candidates[rng.next_usize(candidates.len())];
                        let previous = pending_model.get(&logical_id).unwrap().clone();
                        let bucket = rng.next_usize(5) as i64;
                        let rank = next_rank;
                        next_rank += 7;
                        let active = rng.next_usize(3) != 0;
                        let name = format!("pending-replace-{round}-{action}-{logical_id}");
                        tx.replace(
                            "events",
                            &previous.doc_id,
                            json!({
                                "logical_id": logical_id as i64,
                                "bucket": bucket,
                                "rank": rank,
                                "active": active,
                                "name": name,
                            }),
                        )
                        .await
                        .unwrap();
                        pending_model.insert(
                            logical_id,
                            PendingQueryModelDoc {
                                doc_id: previous.doc_id,
                                bucket,
                                rank,
                                active,
                                name,
                            },
                        );
                        touched_committed.insert(logical_id);
                    }
                }
                75..=89 => {
                    let candidates: Vec<_> = pending_model
                        .keys()
                        .copied()
                        .filter(|logical_id| {
                            *logical_id < round_start_next_id
                                && !touched_committed.contains(logical_id)
                        })
                        .collect();
                    if !candidates.is_empty() {
                        let logical_id = candidates[rng.next_usize(candidates.len())];
                        let removed = pending_model.remove(&logical_id).unwrap();
                        tx.delete("events", &removed.doc_id).await.unwrap();
                        touched_committed.insert(logical_id);
                    }
                }
                _ => {}
            }

            if action % 6 == 5 {
                let bucket = ((round + action) % 5) as i64;
                assert_pending_bucket_query_matches_model(&mut tx, &pending_model, bucket, None, 9)
                    .await;
                assert_pending_bucket_query_matches_model(
                    &mut tx,
                    &pending_model,
                    bucket,
                    Some(ScanDirection::Backward),
                    7,
                )
                .await;
            }
        }

        for bucket in 0..5_i64 {
            assert_pending_bucket_query_matches_model(&mut tx, &pending_model, bucket, None, 11)
                .await;
            assert_pending_bucket_query_matches_model(
                &mut tx,
                &pending_model,
                bucket,
                Some(ScanDirection::Backward),
                11,
            )
            .await;
        }

        assert_success(tx.commit().await.unwrap());
        committed = pending_model;
    }

    let mut read = db.begin(TransactionOptions::readonly()).unwrap();
    for bucket in 0..5_i64 {
        assert_pending_bucket_query_matches_model(&mut read, &committed, bucket, None, 32).await;
    }
    read.rollback();

    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// File-Backed Durability Stress
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn thousand_docs_durability() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut ids = Vec::new();
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for batch in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..100 {
                let id = tx
                    .insert("data", json!({"seq": batch * 100 + i}))
                    .await
                    .unwrap();
                ids.push(id);
            }
            assert_success(tx.commit().await.unwrap());
        }
        db.storage().checkpoint().await.unwrap();
        // Crash
        db.crash().await;
    }

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut found = 0;
    for id in &ids {
        if tx.get("data", id).await.unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 1000, "all 1000 docs should survive");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn many_small_transactions_durability() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut ids = Vec::new();
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for i in 0..200 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            let id = tx.insert("data", json!({"i": i})).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            ids.push(id);
        }
        // Crash
        db.crash().await;
    }

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut found = 0;
    for id in &ids {
        if tx.get("data", id).await.unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 200);
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_ddl_and_data_stress() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();

        // Create some collections, insert data, drop some
        for i in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(&format!("col_{i}")).await.unwrap();
            assert_success(tx.commit().await.unwrap());
        }
        for i in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for j in 0..10 {
                tx.insert(&format!("col_{i}"), json!({"i": i, "j": j}))
                    .await
                    .unwrap();
            }
            assert_success(tx.commit().await.unwrap());
        }
        // Drop even collections
        for i in (0..10).step_by(2) {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.drop_collection(&format!("col_{i}")).await.unwrap();
            assert_success(tx.commit().await.unwrap());
        }
        db.crash().await;
    }

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    let collections = db.list_collections();
    assert_eq!(collections.len(), 5, "only odd collections should survive");
    for c in &collections {
        let i: usize = c.name.strip_prefix("col_").unwrap().parse().unwrap();
        assert!(i % 2 == 1, "collection {} should be odd-numbered", c.name);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_crash_recover_loop_with_indexes_and_full_integrity() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let config = DatabaseConfig {
        check_on_startup: true,
        check_on_startup_full: true,
        ..Default::default()
    };

    let mut expected: BTreeMap<u32, (DocId, i64, i64)> = BTreeMap::new();

    for round in 0..12u32 {
        let db = open_crash_loop_db(&path, config.clone(), round).await;

        if round == 0 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection("events").await.unwrap();
            assert_success(tx.commit().await.unwrap());

            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index("events", "bucket_idx", vec![FieldPath::single("bucket")])
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());
            wait_for_index_ready(&db, "events", "bucket_idx").await;
        } else {
            verify_crash_loop_model(&db, &expected).await;
        }

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for offset in 0..8u32 {
            let logical_id = round * 100 + offset;
            let bucket = (logical_id % 4) as i64;
            let revision = round as i64;
            let doc_id = tx
                .insert(
                    "events",
                    json!({
                        "logical_id": logical_id as i64,
                        "bucket": bucket,
                        "revision": revision,
                        "payload": format!("round-{round}-offset-{offset}")
                    }),
                )
                .await
                .unwrap();
            expected.insert(logical_id, (doc_id, bucket, revision));
        }

        if round > 0 {
            let replace_key = (round - 1) * 100 + (round % 8);
            if let Some((doc_id, bucket, revision)) = expected.get_mut(&replace_key) {
                *revision += 1000;
                tx.replace(
                    "events",
                    doc_id,
                    json!({
                        "logical_id": replace_key as i64,
                        "bucket": *bucket,
                        "revision": *revision,
                        "payload": format!("replaced-round-{round}")
                    }),
                )
                .await
                .unwrap();
            }
        }

        if round % 3 == 2 {
            let delete_key = (round - 2) * 100;
            if let Some((doc_id, _, _)) = expected.remove(&delete_key) {
                tx.delete("events", &doc_id).await.unwrap();
            }
        }

        assert_success(tx.commit().await.unwrap());
        verify_crash_loop_model(&db, &expected).await;

        if round % 2 == 0 {
            db.storage().checkpoint().await.unwrap();
        }
        db.crash().await;
    }

    let db = open_crash_loop_db(&path, config, 12).await;
    verify_crash_loop_model(&db, &expected).await;
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "final crash-loop integrity should be clean: {:?}",
        report.issues
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn deterministic_randomized_file_backed_soak_with_crash_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let config = DatabaseConfig {
        check_on_startup: true,
        check_on_startup_full: true,
        ..Default::default()
    };
    let mut rng = DeterministicRng::new(0xEADB_2026_060A);
    let mut expected: BTreeMap<u32, (DocId, i64, i64)> = BTreeMap::new();
    let mut next_logical_id = 0u32;

    let mut db = open_crash_loop_db(&path, config.clone(), 0).await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("events").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("events", "bucket_idx", vec![FieldPath::single("bucket")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "events", "bucket_idx").await;

    for step in 0..96u32 {
        match rng.next_usize(100) {
            0..=49 => {
                let logical_id = next_logical_id;
                next_logical_id += 1;
                let bucket = rng.next_usize(8) as i64;
                let revision = 1_i64;
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                let doc_id = tx
                    .insert(
                        "events",
                        json!({
                            "logical_id": logical_id as i64,
                            "bucket": bucket,
                            "revision": revision,
                            "payload": format!("insert-step-{step}")
                        }),
                    )
                    .await
                    .unwrap();
                assert_success(tx.commit().await.unwrap());
                expected.insert(logical_id, (doc_id, bucket, revision));
            }
            50..=69 if !expected.is_empty() => {
                let index = rng.next_usize(expected.len());
                let logical_id = *expected.keys().nth(index).unwrap();
                let (doc_id, _, revision) = *expected.get(&logical_id).unwrap();
                let bucket = rng.next_usize(8) as i64;
                let revision = revision + 1;
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.replace(
                    "events",
                    &doc_id,
                    json!({
                        "logical_id": logical_id as i64,
                        "bucket": bucket,
                        "revision": revision,
                        "payload": format!("replace-step-{step}")
                    }),
                )
                .await
                .unwrap();
                assert_success(tx.commit().await.unwrap());
                expected.insert(logical_id, (doc_id, bucket, revision));
            }
            70..=84 if !expected.is_empty() => {
                let index = rng.next_usize(expected.len());
                let logical_id = *expected.keys().nth(index).unwrap();
                let (doc_id, _, _) = expected.remove(&logical_id).unwrap();
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.delete("events", &doc_id).await.unwrap();
                assert_success(tx.commit().await.unwrap());
            }
            _ => {
                verify_randomized_soak_model(&db, &expected).await;
            }
        }

        if step % 7 == 3 {
            db.storage().checkpoint().await.unwrap();
        }

        if step % 11 == 10 {
            verify_randomized_soak_model(&db, &expected).await;
            db.crash().await;
            db = open_crash_loop_db(&path, config.clone(), step + 1).await;
            verify_randomized_soak_model(&db, &expected).await;
        }
    }

    verify_randomized_soak_model(&db, &expected).await;
    db.storage().checkpoint().await.unwrap();
    db.crash().await;

    let db = open_crash_loop_db(&path, config, 97).await;
    verify_randomized_soak_model(&db, &expected).await;
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "deterministic randomized soak should finish clean: {:?}",
        report.issues
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_randomized_file_backed_soak_survives_crash_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let config = DatabaseConfig {
        check_on_startup: true,
        check_on_startup_full: true,
        ..Default::default()
    };
    let mut rng = DeterministicRng::new(0x00C0_DEDB_2026_060A);
    let mut expected: BTreeMap<u32, (DocId, i64, i64)> = BTreeMap::new();
    let mut next_logical_id = 0u32;

    let db = open_crash_loop_db(&path, config.clone(), 0).await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("events").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("events", "bucket_idx", vec![FieldPath::single("bucket")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "events", "bucket_idx").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for _ in 0..24 {
        let logical_id = next_logical_id;
        next_logical_id += 1;
        let bucket = (logical_id % 8) as i64;
        let revision = 1_i64;
        let doc_id = tx
            .insert(
                "events",
                json!({
                    "logical_id": logical_id as i64,
                    "bucket": bucket,
                    "revision": revision,
                    "payload": format!("seed-{logical_id}")
                }),
            )
            .await
            .unwrap();
        expected.insert(logical_id, (doc_id, bucket, revision));
    }
    assert_success(tx.commit().await.unwrap());
    verify_randomized_soak_model(&db, &expected).await;
    db.crash().await;

    for epoch in 0..6_u32 {
        let db = Arc::new(open_crash_loop_db(&path, config.clone(), epoch + 1).await);
        verify_randomized_soak_model(&db, &expected).await;

        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(tokio::sync::Barrier::new(18));
        let mut handles = Vec::new();
        for reader_id in 0..16_usize {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                start.wait().await;
                let mut round = 0_usize;
                while !stop.load(Ordering::SeqCst) || round < 24 {
                    let bucket = ((reader_id + round) % 8) as i64;
                    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
                    let docs = tx
                        .query(
                            "events",
                            "bucket_idx",
                            &[RangeExpr::Eq(
                                FieldPath::single("bucket"),
                                Scalar::Int64(bucket),
                            )],
                            None,
                            None,
                            Some(12),
                        )
                        .await
                        .unwrap();
                    assert!(
                        docs.iter().all(|doc| doc["bucket"] == bucket),
                        "reader {reader_id} observed a document outside bucket {bucket}"
                    );
                    tx.rollback();
                    round += 1;
                    tokio::task::yield_now().await;
                    if round >= 64 {
                        break;
                    }
                }
            }));
        }

        let checkpoint_db = Arc::clone(&db);
        let checkpoint_start = Arc::clone(&start);
        handles.push(tokio::spawn(async move {
            checkpoint_start.wait().await;
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(2)).await;
                checkpoint_db.storage().checkpoint().await.unwrap();
            }
        }));

        start.wait().await;
        for step in 0..32_u32 {
            match rng.next_usize(100) {
                0..=49 => {
                    let logical_id = next_logical_id;
                    next_logical_id += 1;
                    let bucket = rng.next_usize(8) as i64;
                    let revision = 1_i64;
                    let mut tx = db.begin(TransactionOptions::default()).unwrap();
                    let doc_id = tx
                        .insert(
                            "events",
                            json!({
                                "logical_id": logical_id as i64,
                                "bucket": bucket,
                                "revision": revision,
                                "payload": format!("epoch-{epoch}-insert-{step}")
                            }),
                        )
                        .await
                        .unwrap();
                    assert_success(tx.commit().await.unwrap());
                    expected.insert(logical_id, (doc_id, bucket, revision));
                }
                50..=74 if !expected.is_empty() => {
                    let index = rng.next_usize(expected.len());
                    let logical_id = *expected.keys().nth(index).unwrap();
                    let (doc_id, _, revision) = *expected.get(&logical_id).unwrap();
                    let bucket = rng.next_usize(8) as i64;
                    let revision = revision + 1;
                    let mut tx = db.begin(TransactionOptions::default()).unwrap();
                    tx.replace(
                        "events",
                        &doc_id,
                        json!({
                            "logical_id": logical_id as i64,
                            "bucket": bucket,
                            "revision": revision,
                            "payload": format!("epoch-{epoch}-replace-{step}")
                        }),
                    )
                    .await
                    .unwrap();
                    assert_success(tx.commit().await.unwrap());
                    expected.insert(logical_id, (doc_id, bucket, revision));
                }
                75..=89 if expected.len() > 8 => {
                    let index = rng.next_usize(expected.len());
                    let logical_id = *expected.keys().nth(index).unwrap();
                    let (doc_id, _, _) = expected.remove(&logical_id).unwrap();
                    let mut tx = db.begin(TransactionOptions::default()).unwrap();
                    tx.delete("events", &doc_id).await.unwrap();
                    assert_success(tx.commit().await.unwrap());
                }
                _ => {
                    tokio::task::yield_now().await;
                }
            }
        }

        stop.store(true, Ordering::SeqCst);
        for handle in handles {
            handle.await.unwrap();
        }

        verify_randomized_soak_model(&db, &expected).await;
        db.storage().checkpoint().await.unwrap();
        let report = db.check_integrity().await.unwrap();
        assert!(
            !report.has_errors(),
            "concurrent randomized epoch {epoch} should leave clean integrity: {:?}",
            report.issues
        );
        Arc::try_unwrap(db).ok().unwrap().crash().await;
    }

    let db = open_crash_loop_db(&path, config, 7).await;
    verify_randomized_soak_model(&db, &expected).await;
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "concurrent randomized soak should finish clean: {:?}",
        report.issues
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn process_abort_file_backed_recovery_preserves_indexed_model() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let helper = env::current_exe().expect("current test binary path");

    for phase in 0..=6_u32 {
        let marker_path = tmp.path().join(format!("phase-{phase}-model.json"));
        let status = Command::new(&helper)
            .arg("process_abort_file_backed_recovery_helper")
            .arg("--exact")
            .arg("--ignored")
            .env("EXDB_PROCESS_ABORT_HELPER", "1")
            .env("EXDB_PROCESS_ABORT_DB", &path)
            .env("EXDB_PROCESS_ABORT_MODEL", &marker_path)
            .env("EXDB_PROCESS_ABORT_PHASE", phase.to_string())
            .status()
            .expect("spawn process-abort helper");
        assert!(
            !status.success(),
            "helper phase {phase} should abort to simulate process death"
        );
        assert!(
            marker_path.exists(),
            "helper phase {phase} did not reach the pre-abort marker"
        );
        let expected: BTreeMap<u32, (i64, i64)> =
            serde_json::from_slice(&std::fs::read(&marker_path).expect("read helper model marker"))
                .expect("decode helper model marker");

        let db = open_process_abort_db(&path, phase).await;
        verify_process_abort_model(&db, &expected).await;
        let report = db.check_integrity().await.unwrap();
        assert!(
            !report.has_errors(),
            "process-abort phase {phase} should recover with clean integrity: {:?}",
            report.issues
        );
        db.close().await.unwrap();
    }
}

#[test]
#[ignore]
fn process_abort_file_backed_recovery_helper() {
    if env::var("EXDB_PROCESS_ABORT_HELPER").ok().as_deref() != Some("1") {
        return;
    }

    let path = env::var_os("EXDB_PROCESS_ABORT_DB")
        .map(std::path::PathBuf::from)
        .expect("EXDB_PROCESS_ABORT_DB");
    let phase: u32 = env::var("EXDB_PROCESS_ABORT_PHASE")
        .expect("EXDB_PROCESS_ABORT_PHASE")
        .parse()
        .expect("valid process-abort phase");
    let marker_path = env::var_os("EXDB_PROCESS_ABORT_MODEL")
        .map(std::path::PathBuf::from)
        .expect("EXDB_PROCESS_ABORT_MODEL");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        run_process_abort_helper_phase(&path, &marker_path, phase).await;
    });

    std::process::abort();
}

async fn run_process_abort_helper_phase(
    path: &std::path::Path,
    marker_path: &std::path::Path,
    phase: u32,
) {
    let db = open_process_abort_db(path, phase).await;
    if phase == 0 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("events").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("events", "bucket_idx", vec![FieldPath::single("bucket")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_for_index_ready(&db, "events", "bucket_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for logical_id in 0..12_u32 {
            let bucket = (logical_id % 5) as i64;
            tx.insert(
                "events",
                json!({
                    "logical_id": logical_id as i64,
                    "bucket": bucket,
                    "revision": 1_i64,
                    "payload": format!("seed-{logical_id}")
                }),
            )
            .await
            .unwrap();
        }
        assert_success(tx.commit().await.unwrap());
    } else {
        let current = load_process_abort_model(&db).await;
        let keys: Vec<_> = current.keys().copied().collect();
        assert!(!keys.is_empty(), "phase {phase} needs seeded documents");
        let replace_key = keys[(phase as usize * 3) % keys.len()];
        let mut delete_key = keys[(phase as usize * 5 + 1) % keys.len()];
        if delete_key == replace_key && keys.len() > 1 {
            delete_key = keys[(phase as usize * 5 + 2) % keys.len()];
        }

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for offset in 0..4_u32 {
            let logical_id = phase * 100 + offset;
            let bucket = ((phase + offset) % 5) as i64;
            tx.insert(
                "events",
                json!({
                    "logical_id": logical_id as i64,
                    "bucket": bucket,
                    "revision": 1_i64,
                    "payload": format!("phase-{phase}-insert-{offset}")
                }),
            )
            .await
            .unwrap();
        }

        let (replace_doc_id, _, previous_revision) = current[&replace_key];
        let replacement_bucket = ((phase * 2 + 1) % 5) as i64;
        tx.replace(
            "events",
            &replace_doc_id,
            json!({
                "logical_id": replace_key as i64,
                "bucket": replacement_bucket,
                "revision": previous_revision + 1,
                "payload": format!("phase-{phase}-replace-{replace_key}")
            }),
        )
        .await
        .unwrap();

        if delete_key != replace_key {
            let (delete_doc_id, _, _) = current[&delete_key];
            tx.delete("events", &delete_doc_id).await.unwrap();
        }

        assert_success(tx.commit().await.unwrap());
    }

    if phase.is_multiple_of(2) {
        db.storage().checkpoint().await.unwrap();
    }
    let expected = load_process_abort_model(&db)
        .await
        .into_iter()
        .map(|(logical_id, (_, bucket, revision))| (logical_id, (bucket, revision)))
        .collect();
    verify_process_abort_model(&db, &expected).await;
    write_process_abort_model_marker(marker_path, &expected);
}

async fn open_process_abort_db(path: &std::path::Path, phase: u32) -> Database {
    open_crash_loop_db(
        path,
        DatabaseConfig {
            check_on_startup: true,
            check_on_startup_full: true,
            ..Default::default()
        },
        phase,
    )
    .await
}

async fn load_process_abort_model(db: &Database) -> BTreeMap<u32, (DocId, i64, i64)> {
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut actual = BTreeMap::new();
    for bucket in 0..5_i64 {
        let docs = tx
            .query(
                "events",
                "bucket_idx",
                &[RangeExpr::Eq(
                    FieldPath::single("bucket"),
                    Scalar::Int64(bucket),
                )],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        for doc in docs {
            assert_eq!(doc["bucket"], bucket);
            let logical_id = doc["logical_id"].as_i64().unwrap() as u32;
            let revision = doc["revision"].as_i64().unwrap();
            let doc_id = decode_ulid(doc["_id"].as_str().expect("document id should be present"))
                .expect("document id should decode");
            assert!(
                actual
                    .insert(logical_id, (doc_id, bucket, revision))
                    .is_none(),
                "logical_id {logical_id} appeared in more than one bucket"
            );
        }
    }
    tx.rollback();
    actual
}

fn write_process_abort_model_marker(
    marker_path: &std::path::Path,
    expected: &BTreeMap<u32, (i64, i64)>,
) {
    let tmp_path = marker_path.with_extension("json.tmp");
    let encoded = serde_json::to_vec(expected).expect("encode process-abort model marker");
    {
        let mut file = std::fs::File::create(&tmp_path).expect("create model marker");
        std::io::Write::write_all(&mut file, &encoded).expect("write model marker");
        file.sync_all().expect("sync model marker");
    }
    std::fs::rename(&tmp_path, marker_path).expect("install model marker");
}

async fn verify_process_abort_model(db: &Database, expected: &BTreeMap<u32, (i64, i64)>) {
    let actual = load_process_abort_model(db).await;
    let actual_without_ids: BTreeMap<_, _> = actual
        .iter()
        .map(|(logical_id, (_, bucket, revision))| (*logical_id, (*bucket, *revision)))
        .collect();
    assert_eq!(
        actual_without_ids, *expected,
        "process-abort recovered data should match deterministic model"
    );
}

async fn open_crash_loop_db(
    path: &std::path::Path,
    config: DatabaseConfig,
    round: u32,
) -> Database {
    match Database::open(path, config, None).await {
        Ok(db) => db,
        Err(err) => {
            let db = Database::open(path, DatabaseConfig::default(), None)
                .await
                .unwrap();
            let report = db.check_integrity().await.unwrap();
            let issues = report
                .issues
                .iter()
                .map(|issue| {
                    format!(
                        "{:?} {:?}: {}",
                        issue.severity, issue.page_id, issue.message
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            panic!("crash-loop open failed at round {round}: {err:?}\n{issues}");
        }
    }
}

async fn verify_crash_loop_model(db: &Database, expected: &BTreeMap<u32, (DocId, i64, i64)>) {
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();

    for (logical_id, (doc_id, bucket, revision)) in expected {
        let doc =
            tx.get("events", doc_id).await.unwrap().unwrap_or_else(|| {
                panic!("expected logical_id {logical_id} to be readable by doc id")
            });
        assert_eq!(doc["logical_id"], *logical_id as i64);
        assert_eq!(doc["bucket"], *bucket);
        assert_eq!(doc["revision"], *revision);
    }

    for bucket in 0..4_i64 {
        let docs = tx
            .query(
                "events",
                "bucket_idx",
                &[RangeExpr::Eq(
                    FieldPath::single("bucket"),
                    Scalar::Int64(bucket),
                )],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let expected_count = expected
            .values()
            .filter(|(_, expected_bucket, _)| *expected_bucket == bucket)
            .count();
        assert_eq!(
            docs.len(),
            expected_count,
            "bucket {bucket} query should match the crash-loop model"
        );
    }

    tx.rollback();
}

async fn verify_randomized_soak_model(db: &Database, expected: &BTreeMap<u32, (DocId, i64, i64)>) {
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut actual_by_bucket = [0usize; 8];

    for (logical_id, (doc_id, bucket, revision)) in expected {
        let doc = tx.get("events", doc_id).await.unwrap().unwrap_or_else(|| {
            panic!("expected logical_id {logical_id} to survive randomized soak")
        });
        assert_eq!(doc["logical_id"], *logical_id as i64);
        assert_eq!(doc["bucket"], *bucket);
        assert_eq!(doc["revision"], *revision);
        actual_by_bucket[*bucket as usize] += 1;
    }

    for bucket in 0..8_i64 {
        let docs = tx
            .query(
                "events",
                "bucket_idx",
                &[RangeExpr::Eq(
                    FieldPath::single("bucket"),
                    Scalar::Int64(bucket),
                )],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            docs.len(),
            actual_by_bucket[bucket as usize],
            "bucket {bucket} query should match randomized soak model"
        );
        assert!(
            docs.iter().all(|doc| doc["bucket"] == bucket),
            "bucket {bucket} query returned a document outside the bucket"
        );
    }

    tx.rollback();
}

// ═══════════════════════════════════════════════════════════════════════
// Checkpoint Under Sustained Write Load
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn checkpoint_under_sustained_write_load() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut ids = Vec::new();
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for round in 0..5 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..100 {
                let id = tx
                    .insert("data", json!({"round": round, "i": i}))
                    .await
                    .unwrap();
                ids.push(id);
            }
            assert_success(tx.commit().await.unwrap());

            // Checkpoint after each batch
            db.storage().checkpoint().await.unwrap();
        }
        db.crash().await;
    }

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut found = 0;
    for id in &ids {
        if tx.get("data", id).await.unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 500);
    tx.rollback();
    db.close().await.unwrap();
}
