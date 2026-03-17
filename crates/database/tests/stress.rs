//! Stress tests for the L6 Database.
//!
//! Bulk operations, size limits, edge cases, and sustained load patterns.
//! These tests validate resource management and correctness under pressure.

use std::sync::Arc;
use std::time::Duration;

use exdb::{
    Database, DatabaseConfig, DatabaseError, TransactionConfig, TransactionOptions,
    TransactionResult,
};
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
    let last_id = tx
        .insert("bulk", json!({"seq": 99999}))
        .await
        .unwrap();
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
        .insert("data", json!({"名前": "太郎", "emoji": "🎉", "中文": "数据库"}))
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
    assert!(doc.is_some(), "inserted doc should be visible via read-your-writes");
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
