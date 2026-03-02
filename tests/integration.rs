//! Integration tests for the experimental database.

use experimental_database::{
    Database, DbConfig, Filter, FieldPath, JsonScalar, MutationCommitOutcome,
    PatchOp, QueryCommitOutcome, QueryOptions, QueryType,
};
use serde_json::json;
use tempfile::TempDir;

async fn open_test_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let cfg = DbConfig::new(dir.path());
    let db = Database::open(cfg).await.unwrap();
    (db, dir)
}

#[tokio::test]
async fn test_create_and_delete_collection() {
    let (db, _dir) = open_test_db().await;

    db.create_collection("users").await.unwrap();
    // Creating same collection again should fail
    assert!(db.create_collection("users").await.is_err());

    db.delete_collection("users").await.unwrap();
    // Deleting non-existent collection should fail
    assert!(db.delete_collection("users").await.is_err());
}

#[tokio::test]
async fn test_insert_and_get() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    // Insert a document
    let mut mutation = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"name": "Alice", "age": 30})).unwrap();
    let doc_id = mutation.insert("docs", json.clone()).await.unwrap();

    // Read-your-own-writes: should be visible within the same mutation
    let retrieved = mutation.get("docs", doc_id).await.unwrap();
    assert!(retrieved.is_some());
    let val: serde_json::Value = serde_json::from_slice(&retrieved.unwrap()).unwrap();
    assert_eq!(val["name"], "Alice");

    let outcome = mutation.commit().await.unwrap();
    assert!(matches!(outcome, MutationCommitOutcome::Committed { .. }));

    // Now read in a query session
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let retrieved = query.get("docs", doc_id).await.unwrap();
    assert!(retrieved.is_some());
    let val: serde_json::Value = serde_json::from_slice(&retrieved.unwrap()).unwrap();
    assert_eq!(val["name"], "Alice");
    assert_eq!(val["age"], 30);
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_patch_document() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    // Insert
    let mut mutation = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"name": "Alice", "age": 30})).unwrap();
    let doc_id = mutation.insert("docs", json).await.unwrap();
    mutation.commit().await.unwrap();

    // Patch
    let mut mutation = db.start_mutation().await.unwrap();
    mutation.patch("docs", doc_id, PatchOp::MergePatch(json!({"age": 31}))).await.unwrap();
    mutation.commit().await.unwrap();

    // Verify
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let retrieved = query.get("docs", doc_id).await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_slice(&retrieved).unwrap();
    assert_eq!(val["name"], "Alice");
    assert_eq!(val["age"], 31);
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_delete_document() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    // Insert
    let mut mutation = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"x": 1})).unwrap();
    let doc_id = mutation.insert("docs", json).await.unwrap();
    mutation.commit().await.unwrap();

    // Delete
    let mut mutation = db.start_mutation().await.unwrap();
    mutation.delete("docs", doc_id).await.unwrap();
    mutation.commit().await.unwrap();

    // Should be gone
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let retrieved = query.get("docs", doc_id).await.unwrap();
    assert!(retrieved.is_none());
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_find_with_filter() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("users").await.unwrap();

    // Insert multiple users
    let mut mutation = db.start_mutation().await.unwrap();
    for (name, age) in [("Alice", 30), ("Bob", 25), ("Charlie", 35)] {
        let json = serde_json::to_vec(&json!({"name": name, "age": age})).unwrap();
        mutation.insert("users", json).await.unwrap();
    }
    mutation.commit().await.unwrap();

    // Find users with age > 28
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let docs = query.find("users", Some(Filter::Gt(
        FieldPath::single("age"),
        JsonScalar::I64(28),
    ))).await.unwrap();
    assert_eq!(docs.len(), 2); // Alice (30) and Charlie (35)
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_find_ids() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("items").await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    let mut inserted_ids = Vec::new();
    for i in 0..5 {
        let json = serde_json::to_vec(&json!({"i": i})).unwrap();
        inserted_ids.push(mutation.insert("items", json).await.unwrap());
    }
    mutation.commit().await.unwrap();

    let mut query = db.start_query(QueryType::FindIds, 1, QueryOptions::default()).await.unwrap();
    let ids = query.find_ids("items", None).await.unwrap();
    assert_eq!(ids.len(), 5);
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_snapshot_isolation() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    // Insert initial document
    let mut mutation = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"v": 1})).unwrap();
    let doc_id = mutation.insert("docs", json).await.unwrap();
    mutation.commit().await.unwrap();

    // Start a query session (takes snapshot)
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let snap_ts = query.start_ts;

    // Update the document after the snapshot
    let mut mutation = db.start_mutation().await.unwrap();
    mutation.patch("docs", doc_id, PatchOp::MergePatch(json!({"v": 2}))).await.unwrap();
    mutation.commit().await.unwrap();

    // The query session should still see v=1 (snapshot isolation)
    let retrieved = query.get("docs", doc_id).await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_slice(&retrieved).unwrap();
    assert_eq!(val["v"], 1, "query session should see snapshot at ts={snap_ts}");
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_conflict_detection() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    // Insert initial document
    let mut m = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"v": 1})).unwrap();
    let doc_id = m.insert("docs", json).await.unwrap();
    m.commit().await.unwrap();

    // Start two concurrent mutations, both reading the same doc
    let mut m1 = db.start_mutation().await.unwrap();
    let mut m2 = db.start_mutation().await.unwrap();

    let _v1 = m1.get("docs", doc_id).await.unwrap();
    let _v2 = m2.get("docs", doc_id).await.unwrap();

    m1.patch("docs", doc_id, PatchOp::MergePatch(json!({"v": 2}))).await.unwrap();
    m2.patch("docs", doc_id, PatchOp::MergePatch(json!({"v": 3}))).await.unwrap();

    // m1 commits first
    let r1 = m1.commit().await.unwrap();
    assert!(matches!(r1, MutationCommitOutcome::Committed { .. }));

    // m2 should conflict (it read doc_id at a snapshot before m1 committed)
    let r2 = m2.commit().await.unwrap();
    assert!(matches!(r2, MutationCommitOutcome::Conflict));
}

#[tokio::test]
async fn test_subscription_invalidation() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("events").await.unwrap();

    // Set up a subscription
    let mut query = db.start_query(
        QueryType::Find,
        1,
        QueryOptions { subscribe: true, limit: None },
    ).await.unwrap();
    let _docs = query.find("events", None).await.unwrap();
    let outcome = query.commit().await.unwrap();

    let mut subscription = match outcome {
        QueryCommitOutcome::Subscribed { subscription } => subscription,
        _ => panic!("expected subscription"),
    };

    // Insert a document – should trigger invalidation
    let mut mutation = db.start_mutation().await.unwrap();
    let json = serde_json::to_vec(&json!({"event": "created"})).unwrap();
    mutation.insert("events", json).await.unwrap();
    mutation.commit().await.unwrap();

    // Should receive an invalidation event
    let event = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        subscription.rx.recv(),
    ).await.unwrap().expect("expected invalidation event");

    assert_eq!(event.subscription_id, subscription.id);
}

#[tokio::test]
async fn test_limit() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("items").await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    for i in 0..20 {
        let json = serde_json::to_vec(&json!({"i": i})).unwrap();
        mutation.insert("items", json).await.unwrap();
    }
    mutation.commit().await.unwrap();

    let mut query = db.start_query(
        QueryType::Find,
        1,
        QueryOptions { subscribe: false, limit: Some(5) },
    ).await.unwrap();
    let docs = query.find("items", None).await.unwrap();
    assert_eq!(docs.len(), 5);
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_in_filter() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("users").await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    for (name, status) in [("Alice", "active"), ("Bob", "inactive"), ("Charlie", "pending")] {
        let json = serde_json::to_vec(&json!({"name": name, "status": status})).unwrap();
        mutation.insert("users", json).await.unwrap();
    }
    mutation.commit().await.unwrap();

    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let docs = query.find("users", Some(Filter::In(
        FieldPath::single("status"),
        vec![
            JsonScalar::String("active".to_string()),
            JsonScalar::String("pending".to_string()),
        ],
    ))).await.unwrap();
    assert_eq!(docs.len(), 2);
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_and_filter() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("products").await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    for (name, price, in_stock) in [
        ("Apple", 1, true),
        ("Banana", 2, false),
        ("Cherry", 5, true),
        ("Date", 10, true),
    ] {
        let json = serde_json::to_vec(&json!({
            "name": name,
            "price": price,
            "in_stock": in_stock,
        })).unwrap();
        mutation.insert("products", json).await.unwrap();
    }
    mutation.commit().await.unwrap();

    // Find products that are in stock AND cost > 1
    let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
    let docs = query.find("products", Some(Filter::And(vec![
        Filter::Eq(FieldPath::single("in_stock"), JsonScalar::Bool(true)),
        Filter::Gt(FieldPath::single("price"), JsonScalar::I64(1)),
    ]))).await.unwrap();
    assert_eq!(docs.len(), 2); // Cherry and Date
    query.commit().await.unwrap();
}

#[tokio::test]
async fn test_mutation_find_ids() {
    let (db, _dir) = open_test_db().await;
    db.create_collection("docs").await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    for i in 0..5 {
        let json = serde_json::to_vec(&json!({"x": i})).unwrap();
        mutation.insert("docs", json).await.unwrap();
    }
    mutation.commit().await.unwrap();

    let mut mutation = db.start_mutation().await.unwrap();
    let ids = mutation.find_ids("docs", None, Some(10)).await.unwrap();
    assert_eq!(ids.len(), 5);
    mutation.commit().await.unwrap();
}

#[tokio::test]
async fn test_wal_recovery() {
    let dir = TempDir::new().unwrap();
    let doc_id;

    // Write data
    {
        let cfg = DbConfig::new(dir.path());
        let db = Database::open(cfg).await.unwrap();
        db.create_collection("items").await.unwrap();

        let mut mutation = db.start_mutation().await.unwrap();
        let json = serde_json::to_vec(&json!({"recovered": true})).unwrap();
        doc_id = mutation.insert("items", json).await.unwrap();
        mutation.commit().await.unwrap();
        db.shutdown().await.unwrap();
    }

    // Reopen and verify data is still there
    {
        let cfg = DbConfig::new(dir.path());
        let db = Database::open(cfg).await.unwrap();

        let mut query = db.start_query(QueryType::Find, 1, QueryOptions::default()).await.unwrap();
        let retrieved = query.get("items", doc_id).await.unwrap();
        assert!(retrieved.is_some(), "document should survive WAL recovery");
        let val: serde_json::Value = serde_json::from_slice(&retrieved.unwrap()).unwrap();
        assert_eq!(val["recovered"], true);
        query.commit().await.unwrap();
    }
}
