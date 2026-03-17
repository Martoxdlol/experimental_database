//! Durability integration tests for the L6 Database.
//!
//! Each test creates a file-backed database, performs operations, simulates a
//! crash via `db.crash().await` (cancels background tasks without final
//! checkpoint), and reopens to verify durability.
//!
//! ## Status: WAL Replay Not Yet Integrated
//!
//! Tests marked `#[ignore]` require WAL replay during `Database::open`.
//! Currently `StorageEngine::open` receives `NoOpHandler`, so WAL records
//! are not replayed into L6 catalog/data structures after a crash.
//! Once `DatabaseRecoveryHandler` is wired into the open path (requires
//! solving the !Send constraint), these tests should pass.

use std::path::Path;

use exdb::{
    Database, DatabaseConfig, FieldPath, Scalar, TransactionOptions, TransactionResult,
};
use serde_json::json;

// ─── Helpers ───

async fn open_db(path: &Path) -> Database {
    Database::open(path, DatabaseConfig::default(), None)
        .await
        .unwrap()
}

fn assert_success(result: TransactionResult) -> exdb::Ts {
    match result {
        TransactionResult::Success { commit_ts, .. } => commit_ts,
        TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

async fn wait_index_ready(db: &Database, collection: &str, index_name: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes(collection).unwrap();
        tx.rollback();
        if indexes
            .iter()
            .any(|i| i.name == index_name && i.state == exdb::IndexState::Ready)
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("index '{index_name}' did not become Ready within 10 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Basic Durability Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn committed_data_survives_clean_close() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.close().await.unwrap();
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn committed_data_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn uncommitted_data_lost_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx.insert("users", json!({"name": "Ghost"})).await.unwrap();
        tx.rollback();
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn committed_collection_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("orders").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert_eq!(db.list_collections().len(), 1);
    assert_eq!(db.list_collections()[0].name, "orders");
    db.close().await.unwrap();
}

#[tokio::test]
async fn uncommitted_collection_lost_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("ephemeral").await.unwrap();
        tx.rollback();
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn replace_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"name": "Bob"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Bob");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn delete_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &doc_id).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn patch_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice", "age": 30})).await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.patch("users", &doc_id, json!({"age": 31})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    assert_eq!(doc["age"], 31);
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn multiple_commits_before_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut doc_ids = Vec::new();
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for i in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            let id = tx.insert("users", json!({"seq": i})).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            doc_ids.push(id);
        }
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    for (i, doc_id) in doc_ids.iter().enumerate() {
        let doc = tx.get("users", doc_id).await.unwrap().unwrap();
        assert_eq!(doc["seq"], i as u64);
    }
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// DDL + Data Atomicity Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn ddl_and_data_in_one_tx_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert_eq!(db.list_collections().len(), 1);
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn drop_collection_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn drop_index_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "email_idx", vec![FieldPath::single("email")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "email_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_index("users", "email_idx").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();
    assert!(!indexes.iter().any(|i| i.name == "email_idx"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_and_drop_same_name_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        tx.insert("users", json!({"v": 1})).await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"v": 2})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert_eq!(db.list_collections().len(), 1);
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["v"], 2);
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Checkpoint Interaction Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn data_before_checkpoint_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.storage().checkpoint().await.unwrap();
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn data_after_checkpoint_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        db.storage().checkpoint().await.unwrap();

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx.insert("users", json!({"name": "PostCheckpoint"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "PostCheckpoint");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn data_before_and_after_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let id_a;
    let id_b;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        id_a = tx.insert("users", json!({"name": "Before"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.storage().checkpoint().await.unwrap();

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        id_b = tx.insert("users", json!({"name": "After"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert_eq!(tx.get("users", &id_a).await.unwrap().unwrap()["name"], "Before");
    assert_eq!(tx.get("users", &id_b).await.unwrap().unwrap()["name"], "After");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn multiple_checkpoints() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut ids = Vec::new();
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for i in 0..3 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            let id = tx.insert("data", json!({"round": i})).await.unwrap();
            assert_success(tx.commit().await.unwrap());
            ids.push(id);
            db.storage().checkpoint().await.unwrap();
        }
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    for (i, id) in ids.iter().enumerate() {
        let doc = tx.get("data", id).await.unwrap().unwrap();
        assert_eq!(doc["round"], i as u64);
    }
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_then_ddl_then_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        db.storage().checkpoint().await.unwrap();

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("post_checkpoint").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert_eq!(db.list_collections().len(), 1);
    assert_eq!(db.list_collections()[0].name, "post_checkpoint");
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Catalog Consistency Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn catalog_consistent_after_many_creates() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        for i in 0..20 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(&format!("col_{i}")).await.unwrap();
            assert_success(tx.commit().await.unwrap());
        }
        db.crash().await;
    }

    let db = open_db(&path).await;
    let collections = db.list_collections();
    assert_eq!(collections.len(), 20);
    for i in 0..20 {
        assert!(collections.iter().any(|c| c.name == format!("col_{i}")));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn catalog_consistent_after_interleaved_ddl() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("a").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("b").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("a").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("c").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let names: Vec<String> = db.list_collections().iter().map(|c| c.name.clone()).collect();
    assert!(names.contains(&"b".to_string()));
    assert!(names.contains(&"c".to_string()));
    assert!(!names.contains(&"a".to_string()));
    assert_eq!(names.len(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn catalog_id_allocator_correct_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        for i in 0..5 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(&format!("col_{i}")).await.unwrap();
            assert_success(tx.commit().await.unwrap());
        }
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("new_col").await.unwrap();
    assert_success(tx.commit().await.unwrap());
    assert_eq!(db.list_collections().len(), 6);
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Secondary Index Durability Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn committed_index_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "age_idx").await;
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();
    assert!(
        indexes.iter().any(|i| i.name == "age_idx" && i.state == exdb::IndexState::Ready),
        "Ready index should survive crash"
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "WAL does not store index deltas; secondary index rebuild during recovery not yet implemented"]
async fn secondary_index_entries_survive_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "age_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "age_idx",
            &[exdb::RangeExpr::Eq(
                FieldPath::single("age"),
                Scalar::Int64(30),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Building Index Crash Recovery Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn building_index_dropped_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for batch in 0..5 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..100 {
                tx.insert("users", json!({"age": batch * 100 + i}))
                    .await
                    .unwrap();
            }
            assert_success(tx.commit().await.unwrap());
        }

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        // Crash immediately — builder may not have finished
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();

    for idx in &indexes {
        assert_ne!(
            idx.state,
            exdb::IndexState::Building,
            "Building index '{}' should have been dropped on restart",
            idx.name,
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn building_index_data_intact_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut doc_ids = Vec::new();
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..10 {
            doc_ids.push(
                tx.insert("users", json!({"name": format!("User{i}")}))
                    .await
                    .unwrap(),
            );
        }
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "name_idx", vec![FieldPath::single("name")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    for (i, doc_id) in doc_ids.iter().enumerate() {
        let doc = tx.get("users", doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], format!("User{i}"));
    }
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Repeated Crash Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn double_crash_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    // First recovery, then crash again
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        assert!(tx.get("users", &doc_id).await.unwrap().is_some());
        tx.rollback();
        db.crash().await;
    }

    // Second recovery
    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn triple_crash_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let doc_id;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        doc_id = tx.insert("data", json!({"v": 42})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    for _ in 0..3 {
        let db = open_db(&path).await;
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("data", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["v"], 42);
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Edge Cases
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn empty_database_crash_recovery() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn large_transaction_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("bulk").await.unwrap();
        for i in 0..1000 {
            tx.insert("bulk", json!({"seq": i, "data": "x".repeat(100)}))
                .await
                .unwrap();
        }
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert_eq!(db.list_collections().len(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn interleaved_commits_and_checkpoints() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let mut all_ids = Vec::new();
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for round in 0..5 {
            for i in 0..10 {
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                let id = tx
                    .insert("data", json!({"round": round, "i": i}))
                    .await
                    .unwrap();
                assert_success(tx.commit().await.unwrap());
                all_ids.push(id);
            }
            db.storage().checkpoint().await.unwrap();
        }
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let mut found = 0;
    for id in &all_ids {
        if tx.get("data", id).await.unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 50, "all 50 committed docs should survive");
    tx.rollback();
    db.close().await.unwrap();
}
