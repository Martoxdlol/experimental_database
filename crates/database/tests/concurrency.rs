//! Concurrency tests for the L6 Database.
//!
//! Tests OCC conflict detection, phantom inserts, concurrent DDL,
//! subscription behavior, and multi-reader/writer interactions.

use std::sync::Arc;

use exdb::{
    Database, DatabaseConfig, TransactionOptions, TransactionResult,
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
    assert_success(tx.commit().await.unwrap());
}

fn assert_success(result: TransactionResult) -> exdb::Ts {
    match result {
        TransactionResult::Success { commit_ts, .. } => commit_ts,
        TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

fn is_conflict(result: &TransactionResult) -> bool {
    matches!(result, TransactionResult::Conflict { .. })
}

// ═══════════════════════════════════════════════════════════════════════
// Basic Isolation Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn two_readers_no_conflict() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx1 = db.begin(TransactionOptions::readonly()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::readonly()).unwrap();

    let d1 = tx1.get("users", &id).await.unwrap().unwrap();
    let d2 = tx2.get("users", &id).await.unwrap().unwrap();
    assert_eq!(d1["name"], "Alice");
    assert_eq!(d2["name"], "Alice");

    tx1.rollback();
    tx2.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reader_and_writer_no_conflict_disjoint_keys() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id_a = tx.insert("users", json!({"name": "A"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    // Reader reads doc A
    let mut reader = db.begin(TransactionOptions::readonly()).unwrap();
    reader.get("users", &id_a).await.unwrap();

    // Writer inserts doc B (disjoint)
    let mut writer = db.begin(TransactionOptions::default()).unwrap();
    writer.insert("users", json!({"name": "B"})).await.unwrap();
    assert_success(writer.commit().await.unwrap());

    reader.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn two_writers_same_key_serialized() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("users", json!({"v": 0})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    // TX1 and TX2 both begin at the same snapshot, both replace same doc
    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();

    tx1.replace("users", &id, json!({"v": 1})).await.unwrap();
    tx2.replace("users", &id, json!({"v": 2})).await.unwrap();

    let r1 = tx1.commit().await.unwrap();
    let r2 = tx2.commit().await.unwrap();

    // At least one should succeed; if OCC is enabled, one may conflict.
    // Either way, the final value must be consistent.
    let successes = [!is_conflict(&r1), !is_conflict(&r2)];
    assert!(
        successes.iter().any(|&s| s),
        "at least one writer should succeed"
    );

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &id).await.unwrap().unwrap();
    let v = doc["v"].as_u64().unwrap();
    assert!(v == 1 || v == 2, "final value must be from one of the writers");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn two_writers_different_keys_no_conflict() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();

    tx1.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx2.insert("users", json!({"name": "Bob"})).await.unwrap();

    assert_success(tx1.commit().await.unwrap());
    assert_success(tx2.commit().await.unwrap());

    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Snapshot Isolation Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn snapshot_isolation_read_consistency() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("users", json!({"v": 1})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    // TX1 starts reading
    let mut tx1 = db.begin(TransactionOptions::readonly()).unwrap();
    let first_read = tx1.get("users", &id).await.unwrap().unwrap();
    assert_eq!(first_read["v"], 1);

    // TX2 commits a change
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();
    tx2.replace("users", &id, json!({"v": 2})).await.unwrap();
    assert_success(tx2.commit().await.unwrap());

    // TX1 reads again — should still see v:1 (same snapshot)
    let second_read = tx1.get("users", &id).await.unwrap().unwrap();
    assert_eq!(second_read["v"], 1);

    tx1.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent DDL Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_ddl_different_names() {
    let db = open_test_db().await;

    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();

    tx1.create_collection("users").await.unwrap();
    tx2.create_collection("orders").await.unwrap();

    assert_success(tx1.commit().await.unwrap());
    assert_success(tx2.commit().await.unwrap());

    assert_eq!(db.list_collections().len(), 2);
    db.close().await.unwrap();
}

/// Concurrent DDL creating the same collection name — OCC should detect
/// the conflict via catalog pseudo-collection read intervals.
#[tokio::test]
async fn concurrent_ddl_same_name_one_conflicts() {
    let db = open_test_db().await;

    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();

    tx1.create_collection("users").await.unwrap();
    tx2.create_collection("users").await.unwrap();

    let r1 = tx1.commit().await.unwrap();
    let r2 = tx2.commit().await.unwrap();

    let conflicts = [is_conflict(&r1), is_conflict(&r2)];
    assert!(conflicts.iter().any(|&c| c), "one DDL should conflict");
    assert_eq!(db.list_collections().len(), 1);
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent Multi-Task Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn ten_concurrent_writers_disjoint_collections() {
    let db = Arc::new(open_test_db().await);

    // Create 10 collections
    for i in 0..10 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection(&format!("col{i}")).await.unwrap();
        assert_success(tx.commit().await.unwrap());
    }

    let mut handles = vec![];
    for i in 0u64..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(i * 2)).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert(&format!("col{i}"), json!({"task": i}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn ten_concurrent_writers_same_collection() {
    let db = Arc::new(open_test_db().await);
    create_collection(&db, "shared").await;

    let mut handles = vec![];
    for i in 0u64..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(i * 2)).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert("shared", json!({"task": i})).await.unwrap();
            tx.commit().await.unwrap()
        }));
    }

    let mut successes = 0;
    for h in handles {
        let result = h.await.unwrap();
        if matches!(result, TransactionResult::Success { .. }) {
            successes += 1;
        }
    }
    // Most or all should succeed (inserts to different doc IDs don't conflict)
    assert!(
        successes >= 8,
        "expected most inserts to succeed, got {successes}/10"
    );

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn hundred_concurrent_read_transactions() {
    let db = Arc::new(open_test_db().await);
    create_collection(&db, "users").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut handles = vec![];
    for _ in 0..100 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            let doc = tx.get("users", &id).await.unwrap().unwrap();
            tx.rollback();
            doc["name"].as_str().unwrap().to_string()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), "Alice");
    }

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Writer During Checkpoint
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn writer_during_checkpoint() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;

    // Start checkpoint
    let storage = Arc::clone(db.storage());
    let cp_handle = tokio::spawn(async move {
        storage.checkpoint().await.unwrap();
    });

    // Commit while checkpoint runs
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    cp_handle.await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn multiple_commits_during_checkpoint() {
    let db = Arc::new(open_test_db().await);
    create_collection(&db, "data").await;

    let storage = Arc::clone(db.storage());
    let cp_handle = tokio::spawn(async move {
        storage.checkpoint().await.unwrap();
    });

    let mut handles = vec![];
    for i in 0u64..5 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(i * 2)).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert("data", json!({"i": i})).await.unwrap();
            assert_success(tx.commit().await.unwrap());
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    cp_handle.await.unwrap();

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Read-Your-Writes Under Concurrency
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn read_your_writes_during_concurrent_commit() {
    let db = Arc::new(open_test_db().await);
    create_collection(&db, "users").await;

    // TX1 inserts (not yet committed)
    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let id = tx1.insert("users", json!({"name": "Mine"})).await.unwrap();

    // TX2 commits something unrelated
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();
    tx2.insert("users", json!({"name": "Other"})).await.unwrap();
    assert_success(tx2.commit().await.unwrap());

    // TX1 should still see its own write
    let doc = tx1.get("users", &id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Mine");
    assert_success(tx1.commit().await.unwrap());

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Rapid Transaction Lifecycle
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rapid_begin_commit_cycle() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    for i in 0..200 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.insert("data", json!({"i": i})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn rapid_begin_rollback_cycle() {
    let db = open_test_db().await;
    create_collection(&db, "data").await;

    for _ in 0..200 {
        let tx = db.begin(TransactionOptions::readonly()).unwrap();
        tx.rollback();
    }
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Cross-Collection Isolation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn cross_collection_writes_no_conflict() {
    let db = open_test_db().await;
    create_collection(&db, "users").await;
    create_collection(&db, "orders").await;

    let mut tx1 = db.begin(TransactionOptions::default()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::default()).unwrap();

    tx1.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx2.insert("orders", json!({"total": 42})).await.unwrap();

    assert_success(tx1.commit().await.unwrap());
    assert_success(tx2.commit().await.unwrap());

    db.close().await.unwrap();
}
