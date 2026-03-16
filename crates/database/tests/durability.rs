//! Durability and crash-recovery tests for the Database.
//!
//! These tests use file-backed storage to verify that committed data
//! survives close+reopen cycles (WAL replay + catalog recovery).

use exdb::{
    Database, DatabaseConfig, DatabaseError, DocId, FieldPath,
    ScanDirection, TransactionOptions, TransactionResult,
};
use serde_json::json;
use std::path::{Path, PathBuf};

/// Helper: open a file-backed database at the given path on a LocalSet.
async fn open_db(path: &Path) -> Database {
    Database::open(path, DatabaseConfig::default(), None)
        .await
        .expect("open should succeed")
}

/// Helper: open DB, create a collection, commit, close.
async fn create_collection_and_close(path: &Path, name: &str) {
    let db = open_db(path).await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection(name).unwrap();
    let result = tx.commit().await.unwrap();
    assert!(matches!(result, TransactionResult::Success { .. }));
    db.close().await.unwrap();
}

/// Helper: insert a doc into a committed collection (across separate tx).
async fn insert_doc(db: &Database, collection: &str, body: serde_json::Value) -> DocId {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert(collection, body).await.unwrap();
    let result = tx.commit().await.unwrap();
    assert!(matches!(result, TransactionResult::Success { .. }));
    doc_id
}

// ── Basic Open/Close ──

#[tokio::test]
async fn reopen_empty_database() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Open + close
        let db = open_db(&path).await;
        db.close().await.unwrap();

        // Reopen
        let db = open_db(&path).await;
        assert_eq!(db.name(), "testdb");
        assert!(db.list_collections().is_empty());
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn reopen_preserves_name() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("my_cool_db");

        let db = open_db(&path).await;
        assert_eq!(db.name(), "my_cool_db");
        db.close().await.unwrap();

        let db = open_db(&path).await;
        assert_eq!(db.name(), "my_cool_db");
        db.close().await.unwrap();
    }).await;
}

// ── Collection DDL Recovery ──

#[tokio::test]
async fn collection_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        create_collection_and_close(&path, "users").await;

        // Reopen and verify collection exists
        let db = open_db(&path).await;
        let collections = db.list_collections();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].name, "users");
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn multiple_collections_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create multiple collections in one transaction
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_collection("orders").unwrap();
        tx.create_collection("products").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Reopen and verify all exist
        let db = open_db(&path).await;
        let collections = db.list_collections();
        let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"products"));
        assert_eq!(collections.len(), 3);
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn collection_created_in_separate_txns_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create each collection in a separate committed transaction
        let db = open_db(&path).await;
        for name in &["users", "orders", "products"] {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(name).unwrap();
            tx.commit().await.unwrap();
        }
        db.close().await.unwrap();

        // Reopen
        let db = open_db(&path).await;
        assert_eq!(db.list_collections().len(), 3);
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn rolled_back_collection_not_recovered() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        // Committed collection
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        // Rolled-back collection
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("ghosts").unwrap();
        tx.rollback();

        db.close().await.unwrap();

        // Only "users" should exist
        let db = open_db(&path).await;
        let names: Vec<String> = db.list_collections().into_iter().map(|c| c.name).collect();
        assert_eq!(names, vec!["users"]);
        db.close().await.unwrap();
    }).await;
}

// ── Index DDL Recovery ──

#[tokio::test]
async fn created_at_index_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        create_collection_and_close(&path, "users").await;

        // Reopen and verify _created_at index exists
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|i| i.name == "_created_at"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn custom_index_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create collection + custom index
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_email", vec![FieldPath::single("email")]).unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Reopen and verify
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"_created_at"));
        assert!(names.contains(&"by_email"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn compound_index_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_city_age", vec![
            FieldPath::single("city"),
            FieldPath::single("age"),
        ]).unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|i| i.name == "by_city_age"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn multiple_indexes_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_email", vec![FieldPath::single("email")]).unwrap();
        tx.create_index("users", "by_age", vec![FieldPath::single("age")]).unwrap();
        tx.create_index("users", "by_name", vec![FieldPath::single("name")]).unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        // _created_at + 3 custom
        assert_eq!(indexes.len(), 4);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Document Insert Recovery ──

#[tokio::test]
async fn single_doc_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create collection + insert doc
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice", "age": 30})).await;
        db.close().await.unwrap();

        // Reopen and verify
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap();
        assert!(doc.is_some(), "document should survive reopen");
        let doc = doc.unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);
        assert!(doc.get("_id").is_some());
        assert!(doc.get("_created_at").is_some());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn multiple_docs_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let id1 = insert_doc(&db, "users", json!({"name": "Alice"})).await;
        let id2 = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        let id3 = insert_doc(&db, "users", json!({"name": "Charlie"})).await;
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert_eq!(tx.get("users", &id1).await.unwrap().unwrap()["name"], "Alice");
        assert_eq!(tx.get("users", &id2).await.unwrap().unwrap()["name"], "Bob");
        assert_eq!(tx.get("users", &id3).await.unwrap().unwrap()["name"], "Charlie");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn many_docs_in_one_tx_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("items").unwrap();
        tx.commit().await.unwrap();

        // Insert 50 docs in one transaction
        let mut ids = Vec::new();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..50 {
            let id = tx.insert("items", json!({"index": i, "value": format!("item_{}", i)})).await.unwrap();
            ids.push(id);
        }
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Reopen and verify all 50
        let db = open_db(&path).await;
        eprintln!("visible_ts after reopen: {}", db.commit_handle().visible_ts());
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        eprintln!("begin_ts: {}", tx.begin_ts());

        // First, just dump what we get for each id
        let mut mismatches = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let doc = tx.get("items", id).await.unwrap();
            match doc {
                None => eprintln!("  id[{}] = {:?} → NONE", i, &id.0[..4]),
                Some(ref d) => {
                    let got = d["index"].as_u64().unwrap_or(9999);
                    if got != i as u64 {
                        mismatches.push((i, got));
                    }
                }
            }
        }
        eprintln!("total mismatches: {}/{}", mismatches.len(), ids.len());
        for (i, got) in &mismatches[..mismatches.len().min(10)] {
            eprintln!("  ids[{}] → index={}", i, got);
        }

        // Now do the real assertion
        for (i, id) in ids.iter().enumerate() {
            let doc = tx.get("items", id).await.unwrap()
                .unwrap_or_else(|| panic!("doc {} should exist", i));
            assert_eq!(doc["index"], i as u64, "mismatch at i={}", i);
        }
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn batch_inserts_across_transactions_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("logs").unwrap();
        tx.commit().await.unwrap();

        // 10 transactions, each with 5 inserts
        let mut all_ids = Vec::new();
        for batch in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..5 {
                let id = tx.insert("logs", json!({
                    "batch": batch,
                    "seq": i,
                    "message": format!("batch_{}_seq_{}", batch, i),
                })).await.unwrap();
                all_ids.push(id);
            }
            tx.commit().await.unwrap();
        }
        db.close().await.unwrap();

        // Verify all 50 docs
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for id in &all_ids {
            assert!(tx.get("logs", id).await.unwrap().is_some());
        }
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Complex Document Recovery ──

#[tokio::test]
async fn complex_document_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").unwrap();
        tx.commit().await.unwrap();

        let complex = json!({
            "name": "Alice",
            "age": 30,
            "active": true,
            "score": null,
            "tags": ["admin", "user", "moderator"],
            "address": {
                "city": "NYC",
                "zip": "10001",
                "coords": [40.7128, -74.0060]
            },
            "history": [
                {"date": "2024-01-01", "action": "signup"},
                {"date": "2024-06-15", "action": "upgrade"},
            ],
            "metadata": {},
            "count": 999999999,
            "ratio": 3.14159,
            "negative": -42,
        });

        let doc_id = insert_doc(&db, "data", complex).await;
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("data", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);
        assert_eq!(doc["active"], true);
        assert!(doc["score"].is_null());
        assert_eq!(doc["tags"][1], "user");
        assert_eq!(doc["address"]["city"], "NYC");
        assert_eq!(doc["address"]["coords"][0], 40.7128);
        assert_eq!(doc["history"][0]["action"], "signup");
        assert_eq!(doc["count"], 999999999);
        assert!((doc["ratio"].as_f64().unwrap() - 3.14159).abs() < 1e-10);
        assert_eq!(doc["negative"], -42);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn empty_object_doc_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "data", json!({})).await;
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("data", &doc_id).await.unwrap().unwrap();
        // Should have _id and _created_at even though body was empty
        assert!(doc.get("_id").is_some());
        assert!(doc.get("_created_at").is_some());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Replace/Patch/Delete Recovery ──

#[tokio::test]
async fn replace_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice", "age": 30})).await;

        // Replace in a new tx
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"name": "Bob", "age": 25})).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Verify replacement persisted
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Bob");
        assert_eq!(doc["age"], 25);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn patch_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({
            "name": "Alice", "age": 30, "city": "NYC"
        })).await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.patch("users", &doc_id, json!({"age": 31, "email": "alice@test.com"})).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 31);
        assert_eq!(doc["city"], "NYC");
        assert_eq!(doc["email"], "alice@test.com");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn delete_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let id1 = insert_doc(&db, "users", json!({"name": "Alice"})).await;
        let id2 = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        let id3 = insert_doc(&db, "users", json!({"name": "Charlie"})).await;

        // Delete the middle one
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &id2).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Verify
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &id1).await.unwrap().is_some());
        assert!(tx.get("users", &id2).await.unwrap().is_none(), "deleted doc should be gone");
        assert!(tx.get("users", &id3).await.unwrap().is_some());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn replace_then_delete_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Replace
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"name": "Bob"})).await.unwrap();
        tx.commit().await.unwrap();

        // Then delete
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &doc_id).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Should be gone
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &doc_id).await.unwrap().is_none());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Drop Collection Recovery ──

#[tokio::test]
async fn drop_collection_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create two collections
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_collection("orders").unwrap();
        tx.commit().await.unwrap();

        // Drop one
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Only "orders" should survive
        let db = open_db(&path).await;
        let names: Vec<String> = db.list_collections().into_iter().map(|c| c.name).collect();
        assert_eq!(names, vec!["orders"]);
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn drop_collection_removes_docs_on_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        insert_doc(&db, "users", json!({"name": "Alice"})).await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Collection should not exist
        let db = open_db(&path).await;
        assert!(db.list_collections().is_empty());
        // Trying to insert should fail
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let err = tx.insert("users", json!({"name": "Bob"})).await.unwrap_err();
        assert!(matches!(err, DatabaseError::CollectionNotFound(_)));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Drop Index Recovery ──

#[tokio::test]
async fn drop_index_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_email", vec![FieldPath::single("email")]).unwrap();
        tx.create_index("users", "by_age", vec![FieldPath::single("age")]).unwrap();
        tx.commit().await.unwrap();

        // Drop one index
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_index("users", "by_email").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // by_email should be gone, by_age and _created_at should remain
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
        assert!(!names.contains(&"by_email"));
        assert!(names.contains(&"by_age"));
        assert!(names.contains(&"_created_at"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Cross-Collection Recovery ──

#[tokio::test]
async fn cross_collection_docs_survive_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_collection("orders").unwrap();
        tx.commit().await.unwrap();

        let user_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;
        let order_id = insert_doc(&db, "orders", json!({"total": 99.99})).await;
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert_eq!(tx.get("users", &user_id).await.unwrap().unwrap()["name"], "Alice");
        assert_eq!(tx.get("orders", &order_id).await.unwrap().unwrap()["total"], 99.99);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn mixed_ddl_and_data_across_transactions() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;

        // Tx 1: Create users collection
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        // Tx 2: Insert a user
        let user1 = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Tx 3: Create orders + index
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("orders").unwrap();
        tx.create_index("orders", "by_total", vec![FieldPath::single("total")]).unwrap();
        tx.commit().await.unwrap();

        // Tx 4: Insert more users and an order
        let user2 = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        let order1 = insert_doc(&db, "orders", json!({"total": 50.0})).await;

        // Tx 5: Delete first user
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &user1).await.unwrap();
        tx.commit().await.unwrap();

        db.close().await.unwrap();

        // Verify everything
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();

        // user1 deleted, user2 alive
        assert!(tx.get("users", &user1).await.unwrap().is_none());
        assert_eq!(tx.get("users", &user2).await.unwrap().unwrap()["name"], "Bob");
        assert_eq!(tx.get("orders", &order1).await.unwrap().unwrap()["total"], 50.0);

        // Collections and indexes
        assert_eq!(db.list_collections().len(), 2);
        let indexes = tx.list_indexes("orders").unwrap();
        assert!(indexes.iter().any(|i| i.name == "by_total"));

        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Repeated Open/Close Cycles ──

#[tokio::test]
async fn many_open_close_cycles_with_data() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let mut all_ids: Vec<DocId> = Vec::new();

        // First open: create collection
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("data").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // 5 cycles: open, insert 3 docs, close
        for cycle in 0..5 {
            let db = open_db(&path).await;
            for i in 0..3 {
                let id = insert_doc(&db, "data", json!({
                    "cycle": cycle,
                    "item": i,
                })).await;
                all_ids.push(id);
            }
            db.close().await.unwrap();
        }

        // Final open: verify all 15 docs
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for (idx, id) in all_ids.iter().enumerate() {
            let doc = tx.get("data", id).await.unwrap()
                .unwrap_or_else(|| panic!("doc {} from cycle {} should exist", idx % 3, idx / 3));
            assert_eq!(doc["cycle"], (idx / 3) as u64);
            assert_eq!(doc["item"], (idx % 3) as u64);
        }
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn open_close_open_close_empty() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        for _ in 0..5 {
            let db = open_db(&path).await;
            assert!(db.list_collections().is_empty());
            db.close().await.unwrap();
        }
    }).await;
}

// ── Interleaved DDL and Data Recovery ──

#[tokio::test]
async fn create_drop_recreate_collection_across_reopens() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create collection + data
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();
        insert_doc(&db, "users", json!({"name": "Alice"})).await;
        db.close().await.unwrap();

        // Drop the collection
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Recreate the collection
        let db = open_db(&path).await;
        assert!(db.list_collections().is_empty());
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        // Insert new data
        let new_id = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        db.close().await.unwrap();

        // Verify new data exists
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &new_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Bob");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Transaction Isolation After Reopen ──

#[tokio::test]
async fn new_transactions_see_committed_data_after_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let id1 = insert_doc(&db, "users", json!({"name": "Alice"})).await;
        let id2 = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        db.close().await.unwrap();

        let db = open_db(&path).await;

        // Readonly tx should see committed data
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        assert!(tx.get("users", &id1).await.unwrap().is_some());
        assert!(tx.get("users", &id2).await.unwrap().is_some());
        tx.rollback();

        // Write tx should also see committed data
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &id1).await.unwrap().is_some());
        // And be able to modify it
        tx.replace("users", &id1, json!({"name": "Alice Updated"})).await.unwrap();
        tx.commit().await.unwrap();

        db.close().await.unwrap();

        // Verify modification persisted
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert_eq!(tx.get("users", &id1).await.unwrap().unwrap()["name"], "Alice Updated");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Rollback Not Persisted ──

#[tokio::test]
async fn rolled_back_docs_not_persisted() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        // Committed insert
        let committed_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Rolled-back insert
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let rolled_back_id = tx.insert("users", json!({"name": "Ghost"})).await.unwrap();
        tx.rollback();

        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &committed_id).await.unwrap().is_some());
        assert!(tx.get("users", &rolled_back_id).await.unwrap().is_none());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn rolled_back_delete_not_persisted() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Attempt delete, then rollback
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &doc_id).await.unwrap();
        tx.rollback();

        db.close().await.unwrap();

        // Doc should still exist
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &doc_id).await.unwrap().is_some());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn rolled_back_replace_not_persisted() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Attempt replace, then rollback
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"name": "NOT_ALICE"})).await.unwrap();
        tx.rollback();

        db.close().await.unwrap();

        // Should still be Alice
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert_eq!(tx.get("users", &doc_id).await.unwrap().unwrap()["name"], "Alice");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── DDL + Data in Same Transaction Recovery ──

#[tokio::test]
async fn create_collection_and_insert_in_same_tx_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        assert_eq!(db.list_collections().len(), 1);
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn create_collection_index_and_insert_in_same_tx() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_email", vec![FieldPath::single("email")]).unwrap();
        let doc_id = tx.insert("users", json!({
            "name": "Alice",
            "email": "alice@test.com"
        })).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["email"], "alice@test.com");

        let indexes = tx.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|i| i.name == "by_email"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Stress / Edge Cases ──

#[tokio::test]
async fn many_collections_with_indexes_and_docs() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;

        // Create 10 collections, each with a custom index
        for i in 0..10 {
            let name = format!("coll_{}", i);
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(&name).unwrap();
            tx.create_index(&name, "by_val", vec![FieldPath::single("val")]).unwrap();
            tx.commit().await.unwrap();
        }

        // Insert 3 docs into each
        let mut all_ids: Vec<(String, DocId)> = Vec::new();
        for i in 0..10 {
            let name = format!("coll_{}", i);
            for j in 0..3 {
                let id = insert_doc(&db, &name, json!({"val": i * 10 + j})).await;
                all_ids.push((name.clone(), id));
            }
        }
        db.close().await.unwrap();

        // Verify everything
        let db = open_db(&path).await;
        assert_eq!(db.list_collections().len(), 10);

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for (coll, id) in &all_ids {
            assert!(tx.get(coll, id).await.unwrap().is_some(),
                "doc in {} should exist", coll);
        }

        // Verify indexes
        for i in 0..10 {
            let name = format!("coll_{}", i);
            let indexes = tx.list_indexes(&name).unwrap();
            assert!(indexes.iter().any(|idx| idx.name == "by_val"),
                "coll_{} should have by_val index", i);
        }

        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn insert_replace_patch_delete_sequence_across_reopens() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        // Create collection
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        // Insert 4 docs
        let id_a = insert_doc(&db, "users", json!({"name": "A", "val": 1})).await;
        let id_b = insert_doc(&db, "users", json!({"name": "B", "val": 2})).await;
        let id_c = insert_doc(&db, "users", json!({"name": "C", "val": 3})).await;
        let id_d = insert_doc(&db, "users", json!({"name": "D", "val": 4})).await;
        db.close().await.unwrap();

        // Reopen: replace A, patch B, delete C, leave D
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &id_a, json!({"name": "A_replaced", "val": 10})).await.unwrap();
        tx.patch("users", &id_b, json!({"val": 20})).await.unwrap();
        tx.delete("users", &id_c).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Verify final state
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();

        let a = tx.get("users", &id_a).await.unwrap().unwrap();
        assert_eq!(a["name"], "A_replaced");
        assert_eq!(a["val"], 10);

        let b = tx.get("users", &id_b).await.unwrap().unwrap();
        assert_eq!(b["name"], "B");
        assert_eq!(b["val"], 20);

        assert!(tx.get("users", &id_c).await.unwrap().is_none());

        let d = tx.get("users", &id_d).await.unwrap().unwrap();
        assert_eq!(d["name"], "D");
        assert_eq!(d["val"], 4);

        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── _id and _created_at Preservation ──

#[tokio::test]
async fn id_and_created_at_stable_across_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();

        let doc_id = insert_doc(&db, "users", json!({"name": "Alice"})).await;

        // Read the original _id and _created_at
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let original = tx.get("users", &doc_id).await.unwrap().unwrap();
        let original_str_id = original["_id"].as_str().unwrap().to_string();
        let original_created_at = original["_created_at"].as_u64().unwrap();
        tx.rollback();
        db.close().await.unwrap();

        // Reopen and verify they're the same
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let recovered = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(recovered["_id"].as_str().unwrap(), original_str_id);
        assert_eq!(recovered["_created_at"].as_u64().unwrap(), original_created_at);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Write After Reopen ──

#[tokio::test]
async fn can_write_after_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();
        let id1 = insert_doc(&db, "users", json!({"name": "Alice"})).await;
        db.close().await.unwrap();

        // Reopen and write more
        let db = open_db(&path).await;
        let id2 = insert_doc(&db, "users", json!({"name": "Bob"})).await;
        db.close().await.unwrap();

        // Verify both
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        assert!(tx.get("users", &id1).await.unwrap().is_some());
        assert!(tx.get("users", &id2).await.unwrap().is_some());
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn can_create_new_collection_after_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        create_collection_and_close(&path, "users").await;

        // Add a new collection after reopen
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("orders").unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Both should exist
        let db = open_db(&path).await;
        assert_eq!(db.list_collections().len(), 2);
        db.close().await.unwrap();
    }).await;
}

#[tokio::test]
async fn can_add_index_after_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        create_collection_and_close(&path, "users").await;

        // Add index after reopen
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "by_age", vec![FieldPath::single("age")]).unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        // Verify
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        assert!(indexes.iter().any(|i| i.name == "by_age"));
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Multiple Replace on Same Doc Across Reopens ──

#[tokio::test]
async fn multiple_replaces_across_reopens() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("counter").unwrap();
        tx.commit().await.unwrap();
        let doc_id = insert_doc(&db, "counter", json!({"value": 0})).await;
        db.close().await.unwrap();

        // Increment across 5 reopens
        for i in 1..=5 {
            let db = open_db(&path).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.replace("counter", &doc_id, json!({"value": i})).await.unwrap();
            tx.commit().await.unwrap();
            db.close().await.unwrap();
        }

        // Final value should be 5
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let doc = tx.get("counter", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["value"], 5);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Nested Field Path Index Recovery ──

#[tokio::test]
async fn nested_field_path_index_survives_reopen() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.create_index("users", "by_city", vec![
            FieldPath::new(vec!["address".to_string(), "city".to_string()]),
        ]).unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        let city_idx = indexes.iter().find(|i| i.name == "by_city").unwrap();
        assert_eq!(city_idx.field_paths.len(), 1);
        assert_eq!(city_idx.field_paths[0].segments(), &["address", "city"]);
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── Concurrent Transactions Before Close ──

#[tokio::test]
async fn multiple_committed_txns_before_close() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("log").unwrap();
        tx.commit().await.unwrap();

        // Many quick transactions
        let mut ids = Vec::new();
        for i in 0..20 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            let id = tx.insert("log", json!({"seq": i})).await.unwrap();
            tx.commit().await.unwrap();
            ids.push(id);
        }
        db.close().await.unwrap();

        // All should survive
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for (i, id) in ids.iter().enumerate() {
            let doc = tx.get("log", id).await.unwrap()
                .unwrap_or_else(|| panic!("seq {} should exist", i));
            assert_eq!(doc["seq"], i as u64);
        }
        tx.rollback();
        db.close().await.unwrap();
    }).await;
}

// ── File structure sanity check ──

#[tokio::test]
async fn database_files_exist_after_open() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        db.close().await.unwrap();

        assert!(path.exists());
        assert!(path.join("data.db").exists());
        assert!(path.join("wal").exists());
    }).await;
}

#[tokio::test]
async fn database_files_persist_across_reopens() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("testdb");

        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").unwrap();
        tx.commit().await.unwrap();
        insert_doc(&db, "users", json!({"name": "Alice"})).await;
        db.close().await.unwrap();

        // Files should still exist
        assert!(path.join("data.db").exists());

        // Reopen should work
        let db = open_db(&path).await;
        assert_eq!(db.list_collections().len(), 1);
        db.close().await.unwrap();
    }).await;
}
