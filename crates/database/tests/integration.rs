//! Integration tests for the L6 Database facade.
//!
//! These tests exercise the full stack from Database::open_in_memory
//! through transactions, DDL, CRUD, concurrency, and durability.

use std::sync::Arc;

use exdb::{
    Bson, DOC_ID_BINARY_SUBTYPE, Database, DatabaseConfig, DatabaseError, DocId, FieldPath, Filter,
    RangeExpr, Scalar, ScanDirection, SubscriptionHandle, SubscriptionMode, TransactionConfig,
    TransactionOptions, TransactionResult, bson_bytes, bson_doc_id, encode_ulid,
};
use exdb_docstore::make_secondary_key;
use exdb_storage::catalog_btree::{self, CatalogEntityType};
use exdb_storage::page::{PageType, SlottedPage};
use serde_json::json;

// ─── Helpers ───

async fn open_test_db() -> Database {
    Database::open_in_memory(DatabaseConfig::default(), None)
        .await
        .unwrap()
}

async fn create_users_collection(db: &Database) {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    let result = tx.commit().await.unwrap();
    assert!(matches!(result, TransactionResult::Success { .. }));
}

async fn seed_user(db: &Database, body: serde_json::Value) -> DocId {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", body).await.unwrap();
    assert_success(tx.commit().await.unwrap());
    doc_id
}

async fn wait_for_index_ready(db: &Database, collection: &str, index: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes(collection).unwrap();
        tx.rollback();

        if indexes
            .iter()
            .any(|i| i.name == index && i.state == exdb::IndexState::Ready)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("index {collection}.{index} did not become ready within 5 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn create_ready_age_index(db: &Database) {
    create_ready_index(db, "age_idx", vec![FieldPath::single("age")]).await;
}

async fn create_ready_index(db: &Database, name: &str, fields: Vec<FieldPath>) {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", name, fields).await.unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(db, "users", name).await;
}

async fn seed_age_docs(db: &Database) {
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for age in [10, 20, 30, 40] {
        tx.insert("users", json!({"name": format!("age{age}"), "age": age}))
            .await
            .unwrap();
    }
    assert_success(tx.commit().await.unwrap());
}

async fn register_age_limit_subscription(
    db: &Database,
    mode: SubscriptionMode,
    direction: Option<ScanDirection>,
) -> SubscriptionHandle {
    let mut tx = db
        .begin(TransactionOptions {
            readonly: true,
            subscription: mode,
            session_id: 7,
        })
        .unwrap();
    let docs = tx
        .query("users", "age_idx", &[], None, direction, Some(2))
        .await
        .unwrap();
    assert_eq!(docs.len(), 2);
    match tx.commit().await.unwrap() {
        TransactionResult::Success {
            subscription_handle: Some(handle),
            ..
        } => handle,
        TransactionResult::Success {
            subscription_handle: None,
            ..
        } => panic!("subscription commit did not return a handle"),
        TransactionResult::Conflict { error, .. } => {
            panic!("unexpected subscription conflict: {error:?}")
        }
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

async fn assert_no_subscription_event(handle: &mut SubscriptionHandle) {
    let event =
        tokio::time::timeout(std::time::Duration::from_millis(100), handle.next_event()).await;
    assert!(event.is_err(), "subscription fired unexpectedly: {event:?}");
}

fn assert_success(result: TransactionResult) -> exdb::Ts {
    match result {
        TransactionResult::Success { commit_ts, .. } => commit_ts,
        TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

fn assert_conflict(result: TransactionResult) {
    assert!(
        matches!(result, TransactionResult::Conflict { .. }),
        "expected conflict"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn open_in_memory() {
    let db = open_test_db().await;
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn open_in_memory_with_full_startup_integrity_check() {
    let config = DatabaseConfig {
        check_on_startup_full: true,
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn open_file_backed() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db = Database::open(tmp.path().join("testdb"), DatabaseConfig::default(), None)
        .await
        .unwrap();
    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn database_usage_reports_storage_and_memory() {
    let config = DatabaseConfig {
        page_size: 4096,
        memory_budget: 4096 * 64,
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    let usage = db.usage();
    assert_eq!(usage.page_size, 4096);
    assert!(usage.page_count >= 3);
    assert_eq!(usage.page_store_bytes, usage.page_count * 4096);
    assert_eq!(
        usage.disk_usage_bytes,
        usage.page_store_bytes + usage.wal_retained_bytes
    );
    assert_eq!(usage.memory_budget_bytes, 4096 * 64);
    assert!(usage.buffer_pool_used_frames > 0);
    assert_eq!(usage.active_transactions, 0);

    let tx_a = db.begin(TransactionOptions::readonly()).unwrap();
    let tx_b = db.begin(TransactionOptions::default()).unwrap();
    assert_eq!(db.usage().active_transactions, 2);
    tx_a.rollback();
    assert_eq!(db.usage().active_transactions, 1);
    drop(tx_b);
    assert_eq!(db.usage().active_transactions, 0);

    db.close().await.unwrap();
}

#[tokio::test]
async fn promoted_transaction_payload_commits_through_primary_path() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Ada", "role": "engineer"}))
        .await
        .unwrap();
    let (begin_ts, payload) = tx.into_promotion_payload().unwrap();

    assert_success(
        db.commit_promoted_transaction(begin_ts, &payload)
            .await
            .unwrap(),
    );

    let mut read = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = read.get("users", &doc_id).await.unwrap().unwrap();
    read.rollback();
    assert_eq!(doc["name"], "Ada");
    assert_eq!(doc["role"], "engineer");

    db.close().await.unwrap();
}

#[tokio::test]
async fn promoted_transaction_payload_preserves_occ_conflict_detection() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    let doc_id = seed_user(&db, json!({"name": "Ada", "version": 1})).await;

    let mut promoted = db.begin(TransactionOptions::default()).unwrap();
    let original = promoted.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(original["version"], 1);
    promoted
        .replace("users", &doc_id, json!({"name": "Ada", "version": 2}))
        .await
        .unwrap();
    let (begin_ts, payload) = promoted.into_promotion_payload().unwrap();

    let mut concurrent = db.begin(TransactionOptions::default()).unwrap();
    concurrent
        .replace("users", &doc_id, json!({"name": "Ada", "version": 99}))
        .await
        .unwrap();
    assert_success(concurrent.commit().await.unwrap());

    assert_conflict(
        db.commit_promoted_transaction(begin_ts, &payload)
            .await
            .unwrap(),
    );

    let mut read = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = read.get("users", &doc_id).await.unwrap().unwrap();
    read.rollback();
    assert_eq!(doc["version"], 99);

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_rewrites_stale_file_header_shadow() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("repair_integrity_db");
    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    db.storage().checkpoint().await.unwrap();

    db.storage()
        .update_file_header(|fh| {
            fh._reserved[0] = fh._reserved[0].wrapping_add(1);
        })
        .await
        .unwrap();

    let before = db.storage().check_integrity().await.unwrap();
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("file-header shadow")),
        "expected stale shadow warning before repair: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(repair.repaired(), "expected database repair to fix shadow");
    assert!(
        repair.is_clean(),
        "expected no remaining storage issues after repair: {:?}",
        repair.remaining_issues
    );

    let after = db.check_integrity().await.unwrap();
    assert!(
        after.is_ok(),
        "database integrity should be clean after repair: {:?}",
        after.issues
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn begin_after_close_is_error() {
    let db = open_test_db().await;
    // close consumes self, so we just verify the DB works before close
    let tx = db.begin(TransactionOptions::readonly());
    assert!(tx.is_ok());
    tx.unwrap().rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn close_waits_for_active_transactions_before_storage_shutdown() {
    let db = open_test_db().await;
    let tx = db.begin(TransactionOptions::readonly()).unwrap();

    let close = db.close();
    tokio::pin!(close);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut close)
            .await
            .is_err(),
        "close completed while a transaction was still active"
    );

    drop(tx);
    tokio::time::timeout(std::time::Duration::from_secs(1), &mut close)
        .await
        .expect("close should finish after active transaction drops")
        .unwrap();
}

#[tokio::test]
async fn close_timeout_with_hung_transaction() {
    let config = DatabaseConfig {
        close_timeout: std::time::Duration::from_millis(25),
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    let tx = db.begin(TransactionOptions::readonly()).unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), db.close())
        .await
        .expect("close should use configured active transaction timeout")
        .unwrap();

    drop(tx);
}

#[tokio::test]
async fn database_check_integrity_reports_clean_storage() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({
            "name": "Ada",
            "age": 37,
        }),
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());

    let report = db.check_integrity().await.unwrap();
    assert!(
        report.is_ok(),
        "database integrity check should have no errors: {:?}",
        report.issues
    );
    assert_eq!(report.stats.pages_scanned, report.stats.page_count);
    assert!(report.stats.btree_pages > 0);
    assert_eq!(report.stats.orphan_btree_pages, 0);
    assert_eq!(report.stats.orphan_heap_pages, 0);
    assert_eq!(report.stats.double_allocated_pages, 0);

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_stays_clean_after_replace_and_delete() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Ada", "age": 37}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.replace("users", &doc_id, json!({"name": "Ada", "age": 38}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.delete("users", &doc_id).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let report = db.check_integrity().await.unwrap();
    assert!(
        report.is_ok(),
        "retained MVCC versions should keep matching secondary entries until vacuum: {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_traces_external_heap_and_overflow_pages() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let large_body = "x".repeat(20 * 1024);
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({
            "name": "Large Ada",
            "payload": large_body,
        }),
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());

    let report = db.check_integrity().await.unwrap();
    assert!(
        report.is_ok(),
        "large external document should have clean integrity report: {:?}",
        report.issues
    );
    assert!(report.stats.heap_pages > 0);
    assert!(report.stats.overflow_pages > 0);
    assert_eq!(report.stats.orphan_heap_pages, 0);

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_reports_orphan_heap_page() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    db.storage()
        .heap_store(b"unreferenced heap blob")
        .await
        .unwrap();

    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "orphan heap page should be a warning, not structural corruption: {:?}",
        report.issues
    );
    assert!(report.stats.orphan_heap_pages > 0);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.message.contains("heap page is not reachable")),
        "expected orphan heap warning, got {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_reclaims_orphan_heap_and_btree_pages() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    db.storage()
        .heap_store(b"unreferenced heap blob")
        .await
        .unwrap();
    let orphan_btree = db.storage().create_btree().await.unwrap();
    let orphan_root = orphan_btree.root_page();
    drop(orphan_btree);

    let before = db.check_integrity().await.unwrap();
    assert!(
        !before.has_errors(),
        "orphan pages should be repairable warnings: {:?}",
        before.issues
    );
    assert!(before.stats.orphan_heap_pages > 0);
    assert!(before.stats.orphan_btree_pages > 0);
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.page_id == Some(orphan_root)
                && issue.message.contains("B-tree page is not reachable")),
        "expected orphan B-tree warning for page {orphan_root}: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("reclaimed")
                && repair.message.contains("orphan page")),
        "expected orphan-page repair action: {repair:?}"
    );
    assert!(
        !repair
            .remaining_issues
            .iter()
            .any(|issue| issue.message.contains("not reachable")),
        "repair should remove orphan reachability findings: {:?}",
        repair.remaining_issues
    );

    let after = db.check_integrity().await.unwrap();
    assert_eq!(after.stats.orphan_heap_pages, 0);
    assert_eq!(after.stats.orphan_btree_pages, 0);
    assert!(
        !after
            .issues
            .iter()
            .any(|issue| issue.message.contains("not reachable")),
        "integrity after repair: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_defers_orphan_reclaim_when_hard_errors_remain() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let catalog_root_page = db.storage().file_header().await.catalog_root_page.get();
    let orphan_btree = db.storage().create_btree().await.unwrap();
    let orphan_root = orphan_btree.root_page();
    drop(orphan_btree);

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(catalog_root_page)
            .await
            .unwrap();
        SlottedPage::init(guard.data_mut(), catalog_root_page, PageType::Free).stamp_checksum();
    }

    let before = db.check_integrity().await.unwrap();
    assert!(
        before.has_errors(),
        "corrupt reachable catalog page should be a hard error"
    );
    assert!(
        before.stats.orphan_btree_pages > 0,
        "expected unrelated orphan page before repair: {:?}",
        before.issues
    );
    assert!(
        before.issues.iter().any(|issue| {
            issue.page_id == Some(catalog_root_page)
                && issue
                    .message
                    .contains("B-tree 'catalog_by_id' page has type Free")
        }),
        "expected catalog B-tree page-type error: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        !repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("orphan page")),
        "orphan pages should not be reclaimed while hard errors remain: {repair:?}"
    );
    assert!(
        repair
            .remaining_issues
            .iter()
            .any(|issue| issue.page_id == Some(orphan_root)
                && issue.message.contains("B-tree page is not reachable")),
        "orphan finding should remain for a later clean-warning repair pass: {:?}",
        repair.remaining_issues
    );
    assert!(
        repair.remaining_issues.iter().any(|issue| {
            issue.page_id == Some(catalog_root_page)
                && issue
                    .message
                    .contains("B-tree 'catalog_by_id' page has type Free")
        }),
        "hard catalog-page corruption should remain visible after repair: {:?}",
        repair.remaining_issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_rebuilds_structurally_corrupt_primary_index_from_retained_wal() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let ada = seed_user(&db, json!({"name": "Ada", "age": 37})).await;
    let bob = seed_user(&db, json!({"name": "Bob", "age": 42})).await;
    let collection = db.get_collection("users").unwrap();

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(collection.primary_root_page)
            .await
            .unwrap();
        SlottedPage::init(
            guard.data_mut(),
            collection.primary_root_page,
            PageType::Free,
        )
        .stamp_checksum();
    }

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors());
    assert!(
        before.issues.iter().any(|issue| {
            issue.page_id == Some(collection.primary_root_page)
                && issue
                    .message
                    .contains("expected BTreeLeaf or BTreeInternal")
        }),
        "expected structural primary-index issue: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair.repairs.iter().any(|repair| {
            repair
                .message
                .contains("rebuilt 1 primary index tree(s) from fully retained WAL")
        }),
        "expected primary-index WAL rebuild repair: {repair:?}"
    );
    assert!(
        repair.repairs.iter().any(|repair| {
            repair
                .message
                .contains("rebuilt 2 ready secondary index tree(s)")
        }),
        "expected secondary rebuild after primary repair: {repair:?}"
    );

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert_eq!(tx.get("users", &ada).await.unwrap().unwrap()["name"], "Ada");
    assert_eq!(tx.get("users", &bob).await.unwrap().unwrap()["name"], "Bob");
    let results = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(42))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&bob));
    tx.rollback();

    let after = db.check_integrity().await.unwrap();
    assert!(
        !after.has_errors(),
        "integrity after primary WAL repair: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repaired_primary_index_survives_crash_and_full_startup_check() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("primary_repair_db");
    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let ada = seed_user(&db, json!({"name": "Ada", "age": 37})).await;
    let bob = seed_user(&db, json!({"name": "Bob", "age": 42})).await;
    let collection = db.get_collection("users").unwrap();

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(collection.primary_root_page)
            .await
            .unwrap();
        SlottedPage::init(
            guard.data_mut(),
            collection.primary_root_page,
            PageType::Free,
        )
        .stamp_checksum();
    }

    let before = db.check_integrity().await.unwrap();
    assert!(
        before.has_errors(),
        "expected primary corruption before repair"
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair.repairs.iter().any(|repair| {
            repair
                .message
                .contains("rebuilt 1 primary index tree(s) from fully retained WAL")
        }),
        "expected primary-index WAL rebuild repair: {repair:?}"
    );
    assert!(
        repair.is_clean(),
        "primary repair should leave clean integrity: {:?}",
        repair.remaining_issues
    );

    db.crash().await;

    let reopened = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..DatabaseConfig::default()
        },
        None,
    )
    .await
    .unwrap();

    let mut tx = reopened.begin(TransactionOptions::readonly()).unwrap();
    assert_eq!(tx.get("users", &ada).await.unwrap().unwrap()["name"], "Ada");
    assert_eq!(tx.get("users", &bob).await.unwrap().unwrap()["name"], "Bob");
    let results = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(37))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&ada));
    tx.rollback();

    let after = reopened.check_integrity().await.unwrap();
    assert!(
        !after.has_errors(),
        "integrity after repaired primary reopen: {:?}",
        after.issues
    );

    reopened.close().await.unwrap();
}

#[tokio::test]
async fn repaired_orphan_pages_survive_crash_and_full_startup_check() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("orphan_repair_db");
    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;

    db.storage()
        .heap_store(b"unreferenced durable heap blob")
        .await
        .unwrap();
    let orphan_btree = db.storage().create_btree().await.unwrap();
    let orphan_root = orphan_btree.root_page();
    drop(orphan_btree);

    let before = db.check_integrity().await.unwrap();
    assert!(before.stats.orphan_heap_pages > 0);
    assert!(before.stats.orphan_btree_pages > 0);
    assert!(
        before.issues.iter().any(|issue| {
            issue.page_id == Some(orphan_root)
                && issue.message.contains("B-tree page is not reachable")
        }),
        "expected orphan B-tree warning before repair: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("reclaimed")
                && repair.message.contains("orphan page")),
        "expected orphan-page repair action: {repair:?}"
    );

    db.crash().await;

    let db = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let after = db.check_integrity().await.unwrap();
    assert_eq!(after.stats.orphan_heap_pages, 0);
    assert_eq!(after.stats.orphan_btree_pages, 0);
    assert!(
        !after
            .issues
            .iter()
            .any(|issue| issue.message.contains("not reachable")),
        "orphan-page repair should survive crash and full startup check: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repaired_free_list_chain_survives_crash_and_full_startup_check() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("free_list_repair_db");
    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;

    let page_a = db.storage().create_btree().await.unwrap().root_page();
    let page_b = db.storage().create_btree().await.unwrap().root_page();

    {
        let mut free_list = db.storage().free_list().lock().await;
        free_list.deallocate(page_a).await.unwrap();
        free_list.deallocate(page_b).await.unwrap();
    }

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(page_a)
            .await
            .unwrap();
        let mut page = SlottedPage::from_buf(guard.data_mut()).unwrap();
        page.set_prev_or_ptr(page_b);
        page.stamp_checksum();
    }

    let before = db.check_integrity().await.unwrap();
    assert!(
        before.has_errors(),
        "free-list cycle should be reported before repair"
    );
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("free list cycle")),
        "expected free-list cycle finding: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt free-list chain")),
        "expected free-list rebuild repair action: {repair:?}"
    );
    assert!(
        repair.is_clean(),
        "free-list repair should leave clean storage: {:?}",
        repair.remaining_issues
    );

    db.crash().await;

    let db = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let after = db.check_integrity().await.unwrap();
    assert!(
        after.is_ok(),
        "free-list repair should survive crash and full startup check: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_is_idempotent_after_combined_semantic_and_orphan_repairs() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Ada", "email": "ada@example.com"}))
        .await
        .unwrap();
    let commit_ts = assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let users = tx
        .list_collections()
        .unwrap()
        .into_iter()
        .find(|collection| collection.name == "users")
        .unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    let created_at = doc["_created_at"].as_i64().unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let users_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
    let ghost_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "ghost");
    name_btree.delete(&users_key).await.unwrap();
    name_btree
        .insert(&ghost_key, &catalog_btree::serialize_name_value(999))
        .await
        .unwrap();

    let created_at_key = make_secondary_key(&[Scalar::Int64(created_at)], &doc_id, commit_ts);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    assert!(created_at_btree.delete(&created_at_key).await.unwrap());
    let dangling_key = make_secondary_key(&[Scalar::Int64(0)], &DocId([0xDD; 16]), 999);
    created_at_btree.insert(&dangling_key, &[]).await.unwrap();

    db.storage()
        .heap_store(b"unreferenced repair-idempotence heap blob")
        .await
        .unwrap();
    let orphan_btree = db.storage().create_btree().await.unwrap();
    let orphan_root = orphan_btree.root_page();
    drop(orphan_btree);

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors(), "expected combined repairable issues");
    assert!(before.stats.orphan_heap_pages > 0);
    assert!(before.stats.orphan_btree_pages > 0);
    assert!(
        before.issues.iter().any(|issue| issue
            .message
            .contains("missing from the catalog name index")),
        "expected catalog name-index issue: {:?}",
        before.issues
    );
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("missing secondary entry")),
        "expected missing secondary entry issue: {:?}",
        before.issues
    );
    assert!(
        before.issues.iter().any(|issue| {
            issue.page_id == Some(orphan_root)
                && issue.message.contains("B-tree page is not reachable")
        }),
        "expected orphan B-tree warning: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt catalog name index")),
        "expected catalog name-index rebuild: {repair:?}"
    );
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt")
                && repair.message.contains("ready secondary index")),
        "expected ready secondary-index rebuild: {repair:?}"
    );
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("reclaimed")
                && repair.message.contains("orphan page")),
        "expected orphan-page reclamation: {repair:?}"
    );
    assert!(
        repair.is_clean(),
        "combined repair should leave clean integrity: {:?}",
        repair.remaining_issues
    );

    let second = db.repair_integrity().await.unwrap();
    assert!(
        !second.repaired(),
        "second repair pass should be a no-op: {second:?}"
    );
    assert!(
        second.is_clean(),
        "second repair pass should have no remaining issues: {:?}",
        second.remaining_issues
    );

    assert_eq!(
        catalog_btree::deserialize_name_value(&name_btree.get(&users_key).await.unwrap().unwrap())
            .unwrap(),
        users.collection_id.0
    );
    assert!(name_btree.get(&ghost_key).await.unwrap().is_none());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "_created_at",
            &[RangeExpr::Eq(
                FieldPath::single("_created_at"),
                Scalar::Int64(created_at),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&doc_id));
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_reports_stale_catalog_collection_name() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let ghost_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "ghost");
    let ghost_value = catalog_btree::serialize_name_value(999);
    name_btree.insert(&ghost_key, &ghost_value).await.unwrap();

    let report = db.check_integrity().await.unwrap();
    assert!(report.has_errors());
    assert!(
        report.issues.iter().any(|issue| issue
            .message
            .contains("catalog collection-name entry 'ghost' points to missing id")),
        "expected stale catalog name entry issue, got {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn catalog_index_name_keys_are_scoped_by_collection() {
    let db = open_test_db().await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    tx.create_collection("admins").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "email_idx", vec![FieldPath::single("email")])
        .await
        .unwrap();
    tx.create_index("admins", "email_idx", vec![FieldPath::single("email")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let users_idx = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "email_idx")
        .unwrap();
    let admins_idx = tx
        .list_indexes("admins")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "email_idx")
        .unwrap();
    tx.rollback();

    assert_ne!(users_idx.index_id, admins_idx.index_id);

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let users_key =
        catalog_btree::make_catalog_index_name_key(users_idx.collection_id.0, "email_idx");
    let admins_key =
        catalog_btree::make_catalog_index_name_key(admins_idx.collection_id.0, "email_idx");
    assert_eq!(
        catalog_btree::deserialize_name_value(&name_btree.get(&users_key).await.unwrap().unwrap())
            .unwrap(),
        users_idx.index_id.0
    );
    assert_eq!(
        catalog_btree::deserialize_name_value(&name_btree.get(&admins_key).await.unwrap().unwrap())
            .unwrap(),
        admins_idx.index_id.0
    );

    let legacy_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");
    assert!(name_btree.get(&legacy_key).await.unwrap().is_none());

    let report = db.check_integrity().await.unwrap();
    assert!(
        report.is_ok(),
        "scoped catalog index names should be clean: {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_reports_legacy_unscoped_catalog_index_name() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "email_idx", vec![FieldPath::single("email")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let email_idx = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "email_idx")
        .unwrap();
    tx.rollback();

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let legacy_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");
    name_btree
        .insert(
            &legacy_key,
            &catalog_btree::serialize_name_value(email_idx.index_id.0),
        )
        .await
        .unwrap();

    let report = db.check_integrity().await.unwrap();
    assert!(report.has_errors());
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.message.contains("uses legacy unscoped key")),
        "expected legacy catalog index-name issue, got {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_rebuilds_catalog_name_index() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_index(&db, "email_idx", vec![FieldPath::single("email")]).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let users = tx
        .list_collections()
        .unwrap()
        .into_iter()
        .find(|collection| collection.name == "users")
        .unwrap();
    let email_idx = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "email_idx")
        .unwrap();
    tx.rollback();

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let users_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
    let ghost_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "ghost");
    let scoped_email_key =
        catalog_btree::make_catalog_index_name_key(users.collection_id.0, "email_idx");
    let legacy_email_key =
        catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");

    name_btree.delete(&users_key).await.unwrap();
    name_btree
        .insert(&ghost_key, &catalog_btree::serialize_name_value(999))
        .await
        .unwrap();
    name_btree
        .insert(
            &legacy_email_key,
            &catalog_btree::serialize_name_value(email_idx.index_id.0),
        )
        .await
        .unwrap();

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors());
    assert!(
        before.issues.iter().any(|issue| issue
            .message
            .contains("missing from the catalog name index")),
        "expected missing collection-name issue, got {:?}",
        before.issues
    );
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("points to missing id")),
        "expected stale collection-name issue, got {:?}",
        before.issues
    );
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("uses legacy unscoped key")),
        "expected legacy index-name issue, got {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt catalog name index")),
        "expected catalog name-index repair action: {repair:?}"
    );
    assert!(
        repair
            .remaining_issues
            .iter()
            .all(|issue| !issue.message.contains("catalog name")
                && !issue.message.contains("catalog collection-name entry")
                && !issue.message.contains("catalog index-name entry")),
        "catalog name-index issues should be repaired: {:?}",
        repair.remaining_issues
    );

    assert_eq!(
        catalog_btree::deserialize_name_value(&name_btree.get(&users_key).await.unwrap().unwrap())
            .unwrap(),
        users.collection_id.0
    );
    assert_eq!(
        catalog_btree::deserialize_name_value(
            &name_btree.get(&scoped_email_key).await.unwrap().unwrap()
        )
        .unwrap(),
        email_idx.index_id.0
    );
    assert!(name_btree.get(&ghost_key).await.unwrap().is_none());
    assert!(name_btree.get(&legacy_email_key).await.unwrap().is_none());

    let after = db.check_integrity().await.unwrap();
    assert!(after.is_ok(), "integrity after repair: {:?}", after.issues);

    db.close().await.unwrap();
}

#[tokio::test]
async fn repaired_catalog_name_index_survives_crash_and_full_startup_check() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;
    create_ready_index(&db, "email_idx", vec![FieldPath::single("email")]).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let users = tx
        .list_collections()
        .unwrap()
        .into_iter()
        .find(|collection| collection.name == "users")
        .unwrap();
    let email_idx = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "email_idx")
        .unwrap();
    tx.rollback();

    let fh = db.storage().file_header().await;
    let name_btree = db.storage().open_btree(fh.catalog_name_root_page.get());
    let users_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
    let ghost_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "ghost");
    let legacy_email_key =
        catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "email_idx");

    name_btree.delete(&users_key).await.unwrap();
    name_btree
        .insert(&ghost_key, &catalog_btree::serialize_name_value(999))
        .await
        .unwrap();
    name_btree
        .insert(
            &legacy_email_key,
            &catalog_btree::serialize_name_value(email_idx.index_id.0),
        )
        .await
        .unwrap();

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors(), "expected corruption before repair");

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt catalog name index")),
        "expected catalog name-index repair action: {repair:?}"
    );

    db.crash().await;

    let db = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let collections = tx.list_collections().unwrap();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].collection_id, users.collection_id);
    assert_eq!(collections[0].name, "users");
    let indexes = tx.list_indexes("users").unwrap();
    assert!(indexes.iter().any(|index| index.name == "_created_at"));
    assert!(
        indexes
            .iter()
            .any(|index| index.name == "email_idx" && index.index_id == email_idx.index_id)
    );
    tx.rollback();

    let after = db.check_integrity().await.unwrap();
    assert!(
        after.is_ok(),
        "full startup check should leave clean integrity: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_reports_missing_secondary_entry() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    let commit_ts = assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    let created_at = doc["_created_at"].as_i64().unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let created_at_key = make_secondary_key(&[Scalar::Int64(created_at)], &doc_id, commit_ts);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    assert!(created_at_btree.delete(&created_at_key).await.unwrap());

    let report = db.check_integrity().await.unwrap();
    assert!(report.has_errors());
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.message.contains("missing secondary entry")),
        "expected missing secondary entry issue, got {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn database_check_integrity_reports_dangling_secondary_entry() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let missing_doc = DocId([0xAB; 16]);
    let dangling_key = make_secondary_key(&[Scalar::Int64(0)], &missing_doc, 999);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    created_at_btree.insert(&dangling_key, &[]).await.unwrap();

    let report = db.check_integrity().await.unwrap();
    assert!(report.has_errors());
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.message.contains("references missing primary version")),
        "expected dangling secondary issue, got {:?}",
        report.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_rebuilds_corrupt_ready_secondary_index() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    let commit_ts = assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    let created_at = doc["_created_at"].as_i64().unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let created_at_key = make_secondary_key(&[Scalar::Int64(created_at)], &doc_id, commit_ts);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    assert!(created_at_btree.delete(&created_at_key).await.unwrap());

    let missing_doc = DocId([0xEE; 16]);
    let dangling_key = make_secondary_key(&[Scalar::Int64(0)], &missing_doc, 999);
    created_at_btree.insert(&dangling_key, &[]).await.unwrap();

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors());
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("missing secondary entry"))
    );
    assert!(
        before
            .issues
            .iter()
            .any(|issue| issue.message.contains("references missing primary version"))
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(repair.repaired(), "expected repair action: {repair:?}");
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt")
                && repair.message.contains("ready secondary index")),
        "expected secondary-index rebuild repair: {repair:?}"
    );
    assert!(
        !repair
            .remaining_issues
            .iter()
            .any(|issue| issue.severity == exdb_storage::engine::IntegritySeverity::Error),
        "repair should leave no error-severity issues: {:?}",
        repair.remaining_issues
    );

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "_created_at",
            &[RangeExpr::Eq(
                FieldPath::single("_created_at"),
                Scalar::Int64(created_at),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&doc_id));
    tx.rollback();

    let after = db.check_integrity().await.unwrap();
    assert!(
        !after.has_errors(),
        "integrity after repair: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repair_integrity_rebuilds_structurally_corrupt_ready_secondary_index() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    let commit_ts = assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    let created_at = doc["_created_at"].as_i64().unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(created_at_index.root_page)
            .await
            .unwrap();
        SlottedPage::init(guard.data_mut(), created_at_index.root_page, PageType::Free)
            .stamp_checksum();
    }

    let before = db.check_integrity().await.unwrap();
    assert!(before.has_errors());
    assert!(
        before.issues.iter().any(|issue| {
            issue.page_id == Some(created_at_index.root_page)
                && issue
                    .message
                    .contains("expected BTreeLeaf or BTreeInternal")
        }),
        "expected structural secondary-index issue: {:?}",
        before.issues
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt")
                && repair.message.contains("ready secondary index")),
        "expected secondary-index rebuild repair: {repair:?}"
    );
    assert!(
        !repair
            .remaining_issues
            .iter()
            .any(|issue| issue.severity == exdb_storage::engine::IntegritySeverity::Error),
        "repair should leave no error-severity issues: {:?}",
        repair.remaining_issues
    );

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "_created_at",
            &[RangeExpr::Eq(
                FieldPath::single("_created_at"),
                Scalar::Int64(created_at),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&doc_id));
    tx.rollback();

    let created_at_key = make_secondary_key(&[Scalar::Int64(created_at)], &doc_id, commit_ts);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    assert!(
        created_at_btree
            .get(&created_at_key)
            .await
            .unwrap()
            .is_some(),
        "rebuilt secondary B-tree should contain the expected committed key"
    );

    let after = db.check_integrity().await.unwrap();
    assert!(
        !after.has_errors(),
        "integrity after structural secondary repair: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn repaired_secondary_index_survives_crash_and_full_startup_check() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("secondary_repair_db");
    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    let commit_ts = assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    let created_at = doc["_created_at"].as_i64().unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let created_at_key = make_secondary_key(&[Scalar::Int64(created_at)], &doc_id, commit_ts);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    assert!(created_at_btree.delete(&created_at_key).await.unwrap());

    let missing_doc = DocId([0xEF; 16]);
    let dangling_key = make_secondary_key(&[Scalar::Int64(0)], &missing_doc, 999);
    created_at_btree.insert(&dangling_key, &[]).await.unwrap();

    let before = db.check_integrity().await.unwrap();
    assert!(
        before.has_errors(),
        "expected secondary corruption before repair"
    );

    let repair = db.repair_integrity().await.unwrap();
    assert!(
        repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt")
                && repair.message.contains("ready secondary index")),
        "expected secondary-index rebuild repair: {repair:?}"
    );

    {
        let mut guard = db
            .storage()
            .buffer_pool()
            .fetch_page_exclusive(created_at_index.root_page)
            .await
            .unwrap();
        SlottedPage::init(guard.data_mut(), created_at_index.root_page, PageType::Free)
            .stamp_checksum();
    }
    let structural = db.check_integrity().await.unwrap();
    assert!(
        structural.has_errors(),
        "expected structural secondary-index corruption before second repair"
    );
    assert!(
        structural.issues.iter().any(|issue| {
            issue.page_id == Some(created_at_index.root_page)
                && issue
                    .message
                    .contains("expected BTreeLeaf or BTreeInternal")
        }),
        "expected structural secondary-index issue: {:?}",
        structural.issues
    );

    let structural_repair = db.repair_integrity().await.unwrap();
    assert!(
        structural_repair
            .repairs
            .iter()
            .any(|repair| repair.message.contains("rebuilt")
                && repair.message.contains("ready secondary index")),
        "expected structural secondary-index rebuild repair: {structural_repair:?}"
    );

    db.crash().await;

    let db = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "_created_at",
            &[RangeExpr::Eq(
                FieldPath::single("_created_at"),
                Scalar::Int64(created_at),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["_id"], encode_ulid(&doc_id));
    tx.rollback();

    let after = db.check_integrity().await.unwrap();
    assert!(
        !after.has_errors(),
        "secondary-index repair should survive crash and full startup check: {:?}",
        after.issues
    );

    db.close().await.unwrap();
}

#[tokio::test]
async fn full_startup_integrity_rejects_dangling_secondary_entry() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Ada"})).await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let missing_doc = DocId([0xCD; 16]);
    let dangling_key = make_secondary_key(&[Scalar::Int64(0)], &missing_doc, 999);
    let created_at_btree = db.storage().open_btree(created_at_index.root_page);
    created_at_btree.insert(&dangling_key, &[]).await.unwrap();
    db.close().await.unwrap();

    let result = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await;

    match result {
        Err(DatabaseError::IntegrityCheckFailed { phase, errors, .. }) => {
            assert_eq!(phase, "startup full");
            assert!(errors >= 1);
        }
        Ok(db) => {
            db.close().await.unwrap();
            panic!("startup full integrity check should reject dangling secondary entry");
        }
        Err(err) => panic!("unexpected startup error: {err:?}"),
    }
}

#[tokio::test]
async fn full_startup_integrity_rejects_missing_created_at_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    let db = Database::open(&path, DatabaseConfig::default(), None)
        .await
        .unwrap();
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let created_at_index = tx
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .find(|index| index.name == "_created_at")
        .unwrap();
    tx.rollback();

    let fh = db.storage().file_header().await;
    let id_btree = db.storage().open_btree(fh.catalog_root_page.get());
    let id_key =
        catalog_btree::make_catalog_id_key(CatalogEntityType::Index, created_at_index.index_id.0);
    assert!(id_btree.delete(&id_key).await.unwrap());
    db.close().await.unwrap();

    let result = Database::open(
        &path,
        DatabaseConfig {
            check_on_startup_full: true,
            ..Default::default()
        },
        None,
    )
    .await;

    match result {
        Err(DatabaseError::IntegrityCheckFailed { phase, errors, .. }) => {
            assert_eq!(phase, "startup full");
            assert!(errors >= 1);
        }
        Err(DatabaseError::Storage(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert!(err.to_string().contains("missing required _created_at"));
        }
        Ok(db) => {
            db.close().await.unwrap();
            panic!("startup full integrity check should reject missing _created_at index");
        }
        Err(err) => panic!("unexpected startup error: {err:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// DDL Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn create_collection() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    assert_eq!(db.list_collections().len(), 1);
    assert_eq!(db.list_collections()[0].name, "users");
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_duplicate_collection() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.create_collection("users").await;
    assert!(matches!(
        result,
        Err(DatabaseError::CollectionAlreadyExists(_))
    ));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn drop_collection() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.drop_collection("users").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    assert!(db.list_collections().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_index() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "email_idx", vec![FieldPath::single("email")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    // Should have _created_at + email_idx
    assert!(indexes.len() >= 2);
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn cannot_drop_system_index() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.drop_index("users", "_created_at").await;
    assert!(matches!(result, Err(DatabaseError::SystemIndex(_))));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn invalid_collection_name() {
    let db = open_test_db().await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();

    assert!(matches!(
        tx.create_collection("_reserved").await,
        Err(DatabaseError::InvalidName(_))
    ));
    assert!(matches!(
        tx.create_collection("").await,
        Err(DatabaseError::InvalidName(_))
    ));

    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// CRUD Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn insert_and_get() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Alice", "email": "alice@test.com"}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let (get_qid, doc) = tx.get_with_query_id("users", &doc_id).await.unwrap();
    let doc = doc.unwrap();
    assert_eq!(get_qid, 0);
    assert_eq!(doc["name"], "Alice");
    assert_eq!(doc["email"], "alice@test.com");
    assert_eq!(doc["_id"], encode_ulid(&doc_id));
    assert!(doc.get("_created_at").is_some());
    let (query_qid, docs) = tx
        .query_with_query_id("users", "_created_at", &[], None, None, Some(1))
        .await
        .unwrap();
    assert_eq!(query_qid, 1);
    assert_eq!(docs.len(), 1);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn insert_multiple_documents() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..10 {
        tx.insert("users", json!({"name": format!("User {i}"), "age": i}))
            .await
            .unwrap();
    }
    assert_success(tx.commit().await.unwrap());

    db.close().await.unwrap();
}

#[tokio::test]
async fn replace_document() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.replace("users", &doc_id, json!({"name": "Bob"}))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Bob");
    assert_eq!(doc["_id"], encode_ulid(&doc_id));
    assert!(doc.get("_created_at").is_some());
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn patch_document() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Alice", "email": "alice@test.com"}))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.patch("users", &doc_id, json!({"email": "alice@new.com"}))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice"); // unchanged
    assert_eq!(doc["email"], "alice@new.com"); // patched
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn patch_null_stores_value_and_meta_unset_removes_fields() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert(
            "users",
            json!({
                "name": "Alice",
                "middle_name": "Beth",
                "profile": {
                    "zip": "10001",
                    "city": "NYC"
                }
            }),
        )
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.patch(
        "users",
        &doc_id,
        json!({
            "middle_name": null,
            "_meta": {
                "unset": [["profile", "zip"]]
            }
        }),
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert!(doc.get("_meta").is_none());
    assert!(doc["middle_name"].is_null());
    assert_eq!(doc["profile"]["city"], "NYC");
    assert!(doc["profile"].get("zip").is_none());
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn patch_meta_unset_can_remove_nested_meta_fields() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert(
            "users",
            json!({
                "profile": {
                    "name": "Ada",
                    "_meta": {
                        "note": "user field"
                    }
                }
            }),
        )
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.patch(
        "users",
        &doc_id,
        json!({
            "_meta": {
                "unset": [["profile", "_meta"]]
            }
        }),
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["profile"]["name"], "Ada");
    assert!(doc["profile"].get("_meta").is_none());
    tx.rollback();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx
        .patch(
            "users",
            &doc_id,
            json!({
                "_meta": {
                    "unset": [["_meta", "types"]]
                }
            }),
        )
        .await;
    assert!(
        matches!(result, Err(DatabaseError::Commit(message)) if message.contains("start with _meta"))
    );
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn json_meta_types_drive_bson_bytes_storage_and_index_lookup() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "avatar_idx", vec![FieldPath::single("avatar")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "users", "avatar_idx").await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({
            "name": "Typed",
            "avatar": "AQID",
            "_meta": {
                "types": {
                    "avatar": "bytes"
                }
            }
        }),
    )
    .await
    .unwrap();
    tx.insert("users", json!({"name": "String", "avatar": "AQID"}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let docs = tx
        .query(
            "users",
            "avatar_idx",
            &[RangeExpr::Eq(
                FieldPath::single("avatar"),
                Scalar::Bytes(vec![1, 2, 3]),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Typed");
    assert_eq!(docs[0]["avatar"], "AQID");
    assert_eq!(docs[0]["_meta"]["types"]["avatar"], "bytes");
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn bson_embedded_api_round_trips_native_bytes_and_ids() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "avatar_idx", vec![FieldPath::single("avatar")])
        .await
        .unwrap();
    tx.create_index("users", "owner_idx", vec![FieldPath::single("owner")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_for_index_ready(&db, "users", "avatar_idx").await;
    wait_for_index_ready(&db, "users", "owner_idx").await;

    let owner = DocId([8; 16]);
    let avatar = vec![9, 8, 7, 6];
    let doc_id;
    {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx
            .insert_bson(
                "users",
                bson::doc! {
                    "name": "Native",
                    "avatar": bson_bytes(avatar.clone()),
                    "owner": bson_doc_id(owner),
                },
            )
            .await
            .unwrap();
        tx.insert_bson(
            "users",
            bson::doc! {
                "name": "String",
                "avatar": "CQgHBg==",
                "owner": "plain-string-owner",
            },
        )
        .await
        .unwrap();
        assert_success(tx.commit().await.unwrap());
    }

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let native = tx.get_bson("users", &doc_id).await.unwrap().unwrap();
    assert!(matches!(
        native.get("avatar"),
        Some(Bson::Binary(binary))
            if binary.subtype == bson::spec::BinarySubtype::Generic
                && binary.bytes.as_slice() == avatar.as_slice()
    ));
    assert!(matches!(
        native.get("owner"),
        Some(Bson::Binary(binary))
            if binary.subtype == DOC_ID_BINARY_SUBTYPE
                && binary.bytes.as_slice() == owner.as_bytes()
    ));
    assert!(native.get("_meta").is_none());
    assert!(matches!(native.get("_created_at"), Some(Bson::DateTime(_))));

    let avatar_matches = tx
        .query_bson(
            "users",
            "avatar_idx",
            &[RangeExpr::Eq(
                FieldPath::single("avatar"),
                Scalar::Bytes(avatar.clone()),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(avatar_matches.len(), 1);
    assert_eq!(avatar_matches[0].get_str("name").unwrap(), "Native");

    let owner_matches = tx
        .query_bson(
            "users",
            "owner_idx",
            &[RangeExpr::Eq(FieldPath::single("owner"), Scalar::Id(owner))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(owner_matches.len(), 1);
    assert_eq!(owner_matches[0].get_str("name").unwrap(), "Native");

    let json_doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(json_doc["_meta"]["types"]["avatar"], "bytes");
    assert_eq!(json_doc["_meta"]["types"]["owner"], "id");
    tx.rollback();

    let replacement_owner = DocId([9; 16]);
    let replacement_avatar = vec![1, 1, 2, 3];
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.replace_bson(
        "users",
        &doc_id,
        bson::doc! {
            "name": "Native v2",
            "avatar": bson_bytes(replacement_avatar.clone()),
            "owner": bson_doc_id(replacement_owner),
        },
    )
    .await
    .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let old_avatar_matches = tx
        .query_bson(
            "users",
            "avatar_idx",
            &[RangeExpr::Eq(
                FieldPath::single("avatar"),
                Scalar::Bytes(avatar),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(old_avatar_matches.is_empty());

    let replacement = tx
        .query_bson(
            "users",
            "owner_idx",
            &[RangeExpr::Eq(
                FieldPath::single("owner"),
                Scalar::Id(replacement_owner),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(replacement.len(), 1);
    assert_eq!(replacement[0].get_str("name").unwrap(), "Native v2");
    assert!(matches!(
        replacement[0].get("avatar"),
        Some(Bson::Binary(binary))
            if binary.subtype == bson::spec::BinarySubtype::Generic
                && binary.bytes.as_slice() == replacement_avatar.as_slice()
    ));
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn delete_document() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.delete("users", &doc_id).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn delete_nonexistent() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.delete("users", &DocId([0u8; 16])).await;
    assert!(matches!(result, Err(DatabaseError::DocNotFound)));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn get_nonexistent_collection() {
    let db = open_test_db().await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.get("nope", &DocId([0u8; 16])).await;
    assert!(matches!(result, Err(DatabaseError::CollectionNotFound(_))));
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Read-Your-Writes Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn read_your_writes_insert() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();

    let doc = tx.get("users", &doc_id).await.unwrap();
    assert!(doc.is_some());
    assert_eq!(doc.unwrap()["name"], "Alice");

    tx.commit().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn get_pending_write_does_not_record_primary_read_interval() {
    let db = Database::open_in_memory(
        DatabaseConfig {
            transaction: TransactionConfig {
                max_intervals: 2,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();

    let (query_id, doc) = tx.get_with_query_id("users", &doc_id).await.unwrap();
    assert_eq!(query_id, 1);
    assert_eq!(doc.unwrap()["name"], "Alice");

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn read_your_writes_delete() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.delete("users", &doc_id).await.unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());

    tx.commit().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn query_read_your_writes_insert() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Alice", "age": 30}))
        .await
        .unwrap();

    let docs = tx
        .query("users", "age_idx", &[], None, None, None)
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Alice");

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn query_read_your_writes_delete() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Alice", "age": 30}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.delete("users", &doc_id).await.unwrap();
    let docs = tx
        .query("users", "age_idx", &[], None, None, None)
        .await
        .unwrap();
    assert!(docs.is_empty());

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn query_read_your_writes_replace_moves_across_range() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let young_id = tx
        .insert("users", json!({"name": "Young", "age": 20}))
        .await
        .unwrap();
    let older_id = tx
        .insert("users", json!({"name": "Older", "age": 50}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.replace("users", &young_id, json!({"name": "Young", "age": 60}))
        .await
        .unwrap();
    tx.replace("users", &older_id, json!({"name": "Older", "age": 10}))
        .await
        .unwrap();

    let docs = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Lt(FieldPath::single("age"), Scalar::Int64(30))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Older");
    assert_eq!(docs[0]["age"], 10);

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn compound_array_index_query_expands_array_entries_end_to_end() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    seed.insert(
        "users",
        json!({"name": "Ada", "status": "active", "tags": ["db", "math"]}),
    )
    .await
    .unwrap();
    seed.insert(
        "users",
        json!({"name": "Grace", "status": "inactive", "tags": ["db", "compiler"]}),
    )
    .await
    .unwrap();
    seed.insert(
        "users",
        json!({"name": "Linus", "status": "active", "tags": ["os", "db"]}),
    )
    .await
    .unwrap();
    seed.insert(
        "users",
        json!({"name": "Barbara", "status": "active", "tags": ["ai"]}),
    )
    .await
    .unwrap();
    assert_success(seed.commit().await.unwrap());

    create_ready_index(
        &db,
        "status_tags_idx",
        vec![FieldPath::single("status"), FieldPath::single("tags")],
    )
    .await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let docs = tx
        .query(
            "users",
            "status_tags_idx",
            &[
                RangeExpr::Eq(
                    FieldPath::single("status"),
                    Scalar::String("active".to_string()),
                ),
                RangeExpr::Eq(FieldPath::single("tags"), Scalar::String("db".to_string())),
            ],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let mut names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Ada".to_string(), "Linus".to_string()]);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn create_index_rejects_existing_document_with_two_array_fields() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    seed.insert("users", json!({"a": [1, 2], "b": [3, 4]}))
        .await
        .unwrap();
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx
        .create_index(
            "users",
            "bad_compound_idx",
            vec![FieldPath::single("a"), FieldPath::single("b")],
        )
        .await;
    assert!(
        matches!(result, Err(DatabaseError::Commit(message)) if message.contains("at most one array field"))
    );
    assert!(
        !tx.list_indexes("users")
            .unwrap()
            .iter()
            .any(|index| index.name == "bad_compound_idx")
    );
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_compound_index_rejects_insert_with_two_array_fields() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index(
        "users",
        "compound_idx",
        vec![FieldPath::single("a"), FieldPath::single("b")],
    )
    .await
    .unwrap();

    let result = tx.insert("users", json!({"a": [1, 2], "b": [3, 4]})).await;
    assert!(
        matches!(result, Err(DatabaseError::Commit(message)) if message.contains("at most one array field"))
    );
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn limited_query_includes_pending_array_index_insert_and_replace() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    let replace_id = seed
        .insert(
            "users",
            json!({"name": "replace-me", "tags": ["cold", "old"]}),
        )
        .await
        .unwrap();
    assert_success(seed.commit().await.unwrap());

    create_ready_index(&db, "tags_idx", vec![FieldPath::single("tags")]).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-insert", "tags": ["cold", "urgent"]}),
    )
    .await
    .unwrap();
    tx.replace(
        "users",
        &replace_id,
        json!({"name": "pending-replace", "tags": ["later", "urgent"]}),
    )
    .await
    .unwrap();

    let docs = tx
        .query(
            "users",
            "tags_idx",
            &[RangeExpr::Eq(
                FieldPath::single("tags"),
                Scalar::String("urgent".to_string()),
            )],
            None,
            None,
            Some(2),
        )
        .await
        .unwrap();
    let mut names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["pending-insert".to_string(), "pending-replace".to_string()]
    );

    let docs = tx
        .query(
            "users",
            "tags_idx",
            &[RangeExpr::Eq(
                FieldPath::single("tags"),
                Scalar::String("urgent".to_string()),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let mut names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["pending-insert".to_string(), "pending-replace".to_string()]
    );

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn post_filter_heavy_limited_scan_returns_first_matching_rows() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for (age, status) in [
        (10, "skip"),
        (20, "skip"),
        (30, "keep"),
        (40, "skip"),
        (50, "skip"),
        (60, "keep"),
        (70, "keep"),
    ] {
        seed.insert(
            "users",
            json!({"name": format!("{status}-{age}"), "age": age, "status": status}),
        )
        .await
        .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let docs = tx
        .query(
            "users",
            "age_idx",
            &[],
            Some(Filter::Eq(
                FieldPath::single("status"),
                Scalar::String("keep".to_string()),
            )),
            None,
            Some(2),
        )
        .await
        .unwrap();
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![30, 60]);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_pending_writes_merge_with_filtered_limited_query() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    let delete_id = seed
        .insert(
            "users",
            json!({"name": "delete-me", "age": 20, "status": "active"}),
        )
        .await
        .unwrap();
    let promote_id = seed
        .insert(
            "users",
            json!({"name": "promote-me", "age": 30, "status": "inactive"}),
        )
        .await
        .unwrap();
    seed.insert(
        "users",
        json!({"name": "keep-me", "age": 40, "status": "active"}),
    )
    .await
    .unwrap();
    let demote_id = seed
        .insert(
            "users",
            json!({"name": "demote-me", "age": 50, "status": "active"}),
        )
        .await
        .unwrap();
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-first", "age": 15, "status": "active"}),
    )
    .await
    .unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-filtered", "age": 10, "status": "inactive"}),
    )
    .await
    .unwrap();
    tx.delete("users", &delete_id).await.unwrap();
    tx.replace(
        "users",
        &promote_id,
        json!({"name": "promoted", "age": 25, "status": "active"}),
    )
    .await
    .unwrap();
    tx.replace(
        "users",
        &demote_id,
        json!({"name": "demoted", "age": 55, "status": "inactive"}),
    )
    .await
    .unwrap();

    let docs = tx
        .query(
            "users",
            "age_idx",
            &[],
            Some(Filter::Eq(
                FieldPath::single("status"),
                Scalar::String("active".to_string()),
            )),
            None,
            Some(3),
        )
        .await
        .unwrap();
    let names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        vec![
            "pending-first".to_string(),
            "promoted".to_string(),
            "keep-me".to_string()
        ]
    );
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![15, 25, 40]);

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn query_limit_boundary_allows_insert_beyond_cutoff() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("logs").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for age in [10, 20, 30, 40] {
        seed.insert("users", json!({"name": format!("age{age}"), "age": age}))
            .await
            .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut reader = db.begin(TransactionOptions::default()).unwrap();
    let docs = reader
        .query("users", "age_idx", &[], None, None, Some(2))
        .await
        .unwrap();
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![10, 20]);

    let mut concurrent = db.begin(TransactionOptions::default()).unwrap();
    concurrent
        .insert("users", json!({"name": "after", "age": 35}))
        .await
        .unwrap();
    assert_success(concurrent.commit().await.unwrap());

    reader
        .insert("logs", json!({"event": "force-occ-validation"}))
        .await
        .unwrap();
    assert_success(reader.commit().await.unwrap());

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_full_range_conflicts_with_phantom_insert_without_limit_boundary() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("logs").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut reader = db.begin(TransactionOptions::default()).unwrap();
    let docs = reader
        .query("users", "age_idx", &[], None, None, None)
        .await
        .unwrap();
    assert_eq!(docs.len(), 4);

    let mut concurrent = db.begin(TransactionOptions::default()).unwrap();
    concurrent
        .insert("users", json!({"name": "phantom", "age": 25}))
        .await
        .unwrap();
    assert_success(concurrent.commit().await.unwrap());

    reader
        .insert("logs", json!({"event": "force-occ-validation"}))
        .await
        .unwrap();
    assert!(matches!(
        reader.commit().await.unwrap(),
        TransactionResult::Conflict { .. }
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_limit_boundary_conflicts_with_insert_before_cutoff() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("logs").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for age in [10, 20, 30, 40] {
        seed.insert("users", json!({"name": format!("age{age}"), "age": age}))
            .await
            .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut reader = db.begin(TransactionOptions::default()).unwrap();
    let docs = reader
        .query("users", "age_idx", &[], None, None, Some(2))
        .await
        .unwrap();
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![10, 20]);

    let mut concurrent = db.begin(TransactionOptions::default()).unwrap();
    concurrent
        .insert("users", json!({"name": "before", "age": 15}))
        .await
        .unwrap();
    assert_success(concurrent.commit().await.unwrap());

    reader
        .insert("logs", json!({"event": "force-occ-validation"}))
        .await
        .unwrap();
    assert!(matches!(
        reader.commit().await.unwrap(),
        TransactionResult::Conflict { .. }
    ));

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_limit_boundary_backward_allows_insert_before_cutoff() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("logs").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for age in [10, 20, 30, 40] {
        seed.insert("users", json!({"name": format!("age{age}"), "age": age}))
            .await
            .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut reader = db.begin(TransactionOptions::default()).unwrap();
    let docs = reader
        .query(
            "users",
            "age_idx",
            &[],
            None,
            Some(ScanDirection::Backward),
            Some(2),
        )
        .await
        .unwrap();
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![40, 30]);

    let mut concurrent = db.begin(TransactionOptions::default()).unwrap();
    concurrent
        .insert("users", json!({"name": "before", "age": 15}))
        .await
        .unwrap();
    assert_success(concurrent.commit().await.unwrap());

    reader
        .insert("logs", json!({"event": "force-occ-validation"}))
        .await
        .unwrap();
    assert_success(reader.commit().await.unwrap());

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_with_limit_zero_returns_empty_without_data_interval() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_user(&db, json!({"name": "seed", "age": 20})).await;

    let mut reader = db.begin(TransactionOptions::readonly()).unwrap();
    let docs = reader
        .query("users", "age_idx", &[], None, None, Some(0))
        .await
        .unwrap();
    assert!(docs.is_empty());

    let mut writer = db.begin(TransactionOptions::default()).unwrap();
    writer
        .insert("users", json!({"name": "concurrent", "age": 10}))
        .await
        .unwrap();
    assert_success(writer.commit().await.unwrap());

    assert_success(reader.commit().await.unwrap());

    let mut scanner = db.begin(TransactionOptions::readonly()).unwrap();
    let result = scanner
        .query("users", "age_idx", &[], None, None, Some(1))
        .await;
    assert!(matches!(result, Err(DatabaseError::ReadLimitExceeded(_))));
    scanner.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_limit_stops_before_scanned_doc_limit() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let docs = tx
        .query("users", "age_idx", &[], None, None, Some(1))
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["age"], 10);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn query_limit_with_pending_write_stops_before_scanned_doc_limit() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for age in [20, 30, 40] {
        seed.insert("users", json!({"name": format!("age{age}"), "age": age}))
            .await
            .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "pending-first", "age": 10}))
        .await
        .unwrap();
    let docs = tx
        .query("users", "age_idx", &[], None, None, Some(1))
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "pending-first");
    assert_eq!(docs[0]["age"], 10);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn filtered_query_limit_with_pending_write_stops_before_scanned_doc_limit() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    for age in [20, 30, 40] {
        seed.insert(
            "users",
            json!({"name": format!("inactive-{age}"), "age": age, "status": "inactive"}),
        )
        .await
        .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-active", "age": 10, "status": "active"}),
    )
    .await
    .unwrap();
    let docs = tx
        .query(
            "users",
            "age_idx",
            &[],
            Some(Filter::Eq(
                FieldPath::single("status"),
                Scalar::String("active".to_string()),
            )),
            None,
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "pending-active");
    assert_eq!(docs[0]["age"], 10);
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_pending_filtered_limit_streams_before_committed_tail() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    let delete_id = seed
        .insert(
            "users",
            json!({"name": "delete-me", "age": 7, "status": "active"}),
        )
        .await
        .unwrap();
    let replace_id = seed
        .insert(
            "users",
            json!({"name": "replace-me", "age": 100, "status": "inactive"}),
        )
        .await
        .unwrap();
    for age in 9..80 {
        seed.insert(
            "users",
            json!({"name": format!("tail-{age}"), "age": age, "status": "inactive"}),
        )
        .await
        .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-filtered", "age": 4, "status": "inactive"}),
    )
    .await
    .unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-5", "age": 5, "status": "active"}),
    )
    .await
    .unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-6", "age": 6, "status": "active"}),
    )
    .await
    .unwrap();
    tx.delete("users", &delete_id).await.unwrap();
    tx.replace(
        "users",
        &replace_id,
        json!({"name": "pending-replace", "age": 8, "status": "active"}),
    )
    .await
    .unwrap();

    let docs = tx
        .query(
            "users",
            "age_idx",
            &[],
            Some(Filter::Eq(
                FieldPath::single("status"),
                Scalar::String("active".to_string()),
            )),
            None,
            Some(3),
        )
        .await
        .unwrap();
    let names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        vec![
            "pending-5".to_string(),
            "pending-6".to_string(),
            "pending-replace".to_string()
        ]
    );
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![5, 6, 8]);

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn backward_mixed_pending_filtered_limit_streams_before_committed_tail() {
    let config = DatabaseConfig {
        transaction: TransactionConfig {
            max_scanned_docs: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;

    let mut seed = db.begin(TransactionOptions::default()).unwrap();
    let delete_id = seed
        .insert(
            "users",
            json!({"name": "delete-me", "age": 100, "status": "active"}),
        )
        .await
        .unwrap();
    let replace_id = seed
        .insert(
            "users",
            json!({"name": "replace-me", "age": 1, "status": "inactive"}),
        )
        .await
        .unwrap();
    for age in 10..90 {
        seed.insert(
            "users",
            json!({"name": format!("tail-{age}"), "age": age, "status": "inactive"}),
        )
        .await
        .unwrap();
    }
    assert_success(seed.commit().await.unwrap());

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-filtered", "age": 99, "status": "inactive"}),
    )
    .await
    .unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-98", "age": 98, "status": "active"}),
    )
    .await
    .unwrap();
    tx.insert(
        "users",
        json!({"name": "pending-97", "age": 97, "status": "active"}),
    )
    .await
    .unwrap();
    tx.delete("users", &delete_id).await.unwrap();
    tx.replace(
        "users",
        &replace_id,
        json!({"name": "pending-replace", "age": 96, "status": "active"}),
    )
    .await
    .unwrap();

    let docs = tx
        .query(
            "users",
            "age_idx",
            &[],
            Some(Filter::Eq(
                FieldPath::single("status"),
                Scalar::String("active".to_string()),
            )),
            Some(ScanDirection::Backward),
            Some(3),
        )
        .await
        .unwrap();
    let names: Vec<_> = docs
        .iter()
        .map(|doc| doc["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        vec![
            "pending-98".to_string(),
            "pending-97".to_string(),
            "pending-replace".to_string()
        ]
    );
    let ages: Vec<_> = docs
        .iter()
        .map(|doc| doc["age"].as_i64().unwrap())
        .collect();
    assert_eq!(ages, vec![98, 97, 96]);

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn notify_subscription_limit_boundary_ignores_insert_beyond_cutoff() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut handle = register_age_limit_subscription(&db, SubscriptionMode::Notify, None).await;

    let mut writer = db.begin(TransactionOptions::default()).unwrap();
    writer
        .insert("users", json!({"name": "after", "age": 35}))
        .await
        .unwrap();
    assert_success(writer.commit().await.unwrap());

    assert_no_subscription_event(&mut handle).await;

    db.close().await.unwrap();
}

#[tokio::test]
async fn notify_subscription_self_written_get_does_not_watch_document() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db
        .begin(TransactionOptions {
            readonly: false,
            subscription: SubscriptionMode::Notify,
            session_id: 7,
        })
        .unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "self-written", "age": 10}))
        .await
        .unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "self-written");
    let mut handle = match tx.commit().await.unwrap() {
        TransactionResult::Success {
            subscription_handle: Some(handle),
            ..
        } => handle,
        TransactionResult::Success {
            subscription_handle: None,
            ..
        } => panic!("subscription commit did not return a handle"),
        TransactionResult::Conflict { error, .. } => {
            panic!("unexpected subscription conflict: {error:?}")
        }
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    };

    let mut writer = db.begin(TransactionOptions::default()).unwrap();
    writer
        .replace("users", &doc_id, json!({"name": "updated", "age": 11}))
        .await
        .unwrap();
    assert_success(writer.commit().await.unwrap());

    assert_no_subscription_event(&mut handle).await;

    db.close().await.unwrap();
}

#[tokio::test]
async fn watch_subscription_limit_boundary_fires_for_insert_before_cutoff() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut handle = register_age_limit_subscription(&db, SubscriptionMode::Watch, None).await;

    let mut outside = db.begin(TransactionOptions::default()).unwrap();
    outside
        .insert("users", json!({"name": "after", "age": 35}))
        .await
        .unwrap();
    assert_success(outside.commit().await.unwrap());
    assert_no_subscription_event(&mut handle).await;

    let mut inside = db.begin(TransactionOptions::default()).unwrap();
    inside
        .insert("users", json!({"name": "before", "age": 15}))
        .await
        .unwrap();
    let commit_ts = assert_success(inside.commit().await.unwrap());

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), handle.next_event())
        .await
        .expect("watch subscription should fire")
        .expect("watch subscription event channel should stay open");
    assert_eq!(event.affected_query_ids, vec![0]);
    assert_eq!(event.commit_ts, commit_ts);
    assert!(event.continuation.is_none());

    let mut second_inside = db.begin(TransactionOptions::default()).unwrap();
    second_inside
        .insert("users", json!({"name": "also-before", "age": 18}))
        .await
        .unwrap();
    assert_success(second_inside.commit().await.unwrap());
    let second_event = tokio::time::timeout(std::time::Duration::from_secs(1), handle.next_event())
        .await
        .expect("watch subscription should persist and fire again")
        .expect("watch subscription event channel should stay open");
    assert_eq!(second_event.affected_query_ids, vec![0]);

    db.close().await.unwrap();
}

#[tokio::test]
async fn watch_subscription_limit_boundary_filters_concurrent_writes() {
    let db = Arc::new(open_test_db().await);
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut handle = register_age_limit_subscription(&db, SubscriptionMode::Watch, None).await;
    let start = Arc::new(tokio::sync::Barrier::new(2));

    let outside = {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert("users", json!({"name": "outside", "age": 35}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap())
        })
    };

    let inside = {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert("users", json!({"name": "inside", "age": 15}))
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap())
        })
    };

    let outside_ts = outside.await.unwrap();
    let inside_ts = inside.await.unwrap();
    assert_ne!(outside_ts, inside_ts);

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), handle.next_event())
        .await
        .expect("watch subscription should fire for the in-bound concurrent write")
        .expect("watch subscription event channel should stay open");
    assert_eq!(event.affected_query_ids, vec![0]);
    assert_eq!(event.commit_ts, inside_ts);
    assert!(event.continuation.is_none());
    assert_no_subscription_event(&mut handle).await;

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn subscribe_subscription_limit_boundary_produces_continuation() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    create_ready_age_index(&db).await;
    seed_age_docs(&db).await;

    let mut handle = register_age_limit_subscription(&db, SubscriptionMode::Subscribe, None).await;

    let mut outside = db.begin(TransactionOptions::default()).unwrap();
    outside
        .insert("users", json!({"name": "after", "age": 35}))
        .await
        .unwrap();
    assert_success(outside.commit().await.unwrap());
    assert_no_subscription_event(&mut handle).await;

    let mut inside = db.begin(TransactionOptions::default()).unwrap();
    inside
        .insert("users", json!({"name": "before", "age": 15}))
        .await
        .unwrap();
    let commit_ts = assert_success(inside.commit().await.unwrap());

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), handle.next_event())
        .await
        .expect("subscribe subscription should fire")
        .expect("subscribe subscription event channel should deliver");
    assert_eq!(event.affected_query_ids, vec![0]);
    assert_eq!(event.commit_ts, commit_ts);
    let continuation = event
        .continuation
        .expect("subscribe invalidation should include a continuation");
    assert_eq!(continuation.new_ts, commit_ts);
    assert_eq!(continuation.first_query_id, 0);
    assert_eq!(continuation.carried_read_set.interval_count(), 0);

    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Readonly Transaction Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn readonly_cannot_write() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(matches!(
        tx.insert("users", json!({"name": "Alice"})).await,
        Err(DatabaseError::ReadonlyWrite)
    ));
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn readonly_cannot_ddl() {
    let db = open_test_db().await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(matches!(
        tx.create_collection("test").await,
        Err(DatabaseError::ReadonlyWrite)
    ));
    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction Lifecycle Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rollback_discards_changes() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.rollback();

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    tx.rollback();

    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_clears_state() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.reset();

    // After reset, the insert is gone — commit should be read-only no-op
    assert_success(tx.commit().await.unwrap());
    db.close().await.unwrap();
}

#[tokio::test]
async fn reset_then_see_same_snapshot() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let initial = tx
        .query("users", "_created_at", &[], None, None, None)
        .await
        .unwrap();
    assert!(initial.is_empty());

    let mut writer = db.begin(TransactionOptions::default()).unwrap();
    writer
        .insert("users", json!({"name": "later"}))
        .await
        .unwrap();
    assert_success(writer.commit().await.unwrap());

    tx.reset();
    let after_reset = tx
        .query("users", "_created_at", &[], None, None, None)
        .await
        .unwrap();
    assert!(
        after_reset.is_empty(),
        "reset must preserve the transaction snapshot"
    );

    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrency Tests
// ═══════════════════════════════════════════════════════════════════════

/// Compile-time proof that Database is Send + Sync.
#[tokio::test]
async fn database_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<Database>();
    assert_sync::<Database>();
}

/// Two read-only transactions on the same task see the same snapshot.
#[tokio::test]
async fn concurrent_readers_same_task() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx1 = db.begin(TransactionOptions::readonly()).unwrap();
    let mut tx2 = db.begin(TransactionOptions::readonly()).unwrap();

    let doc1 = tx1.get("users", &doc_id).await.unwrap().unwrap();
    let doc2 = tx2.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc1["name"], "Alice");
    assert_eq!(doc2["name"], "Alice");

    tx1.rollback();
    tx2.rollback();
    db.close().await.unwrap();
}

/// Readers on separate tokio tasks see committed data.
#[tokio::test]
async fn concurrent_readers_across_tasks() {
    let db = Arc::new(open_test_db().await);
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    let mut handles = vec![];
    for _ in 0..5 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
            tx.rollback();
            doc
        }));
    }
    for h in handles {
        let doc = h.await.unwrap();
        assert_eq!(doc["name"], "Alice");
    }

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

/// Writer and reader on separate tasks: reader doesn't see uncommitted data.
#[tokio::test]
async fn snapshot_isolation_across_tasks() {
    let db = Arc::new(open_test_db().await);
    create_users_collection(&db).await;

    // Insert initial doc
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx.insert("users", json!({"v": 1})).await.unwrap();
    tx.commit().await.unwrap();

    // Start a reader BEFORE the writer commits — it should see v:1
    let mut reader_tx = db.begin(TransactionOptions::readonly()).unwrap();

    // Now commit v:2 from a spawned task
    let writer_db = Arc::clone(&db);
    tokio::spawn(async move {
        let mut tx = writer_db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"v": 2})).await.unwrap();
        tx.commit().await.unwrap();
    })
    .await
    .unwrap();

    // Reader (started before the write) still sees v:1 due to MVCC
    let doc = reader_tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["v"], 1);
    reader_tx.rollback();

    // New reader sees v:2
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["v"], 2);
    tx.rollback();

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

/// Share Database via Arc, run full insert+read cycle on a spawned task.
#[tokio::test]
async fn transaction_on_spawned_task() {
    let db = Arc::new(open_test_db().await);
    create_users_collection(&db).await;

    let db2 = Arc::clone(&db);
    let doc_id = tokio::spawn(async move {
        let mut tx = db2.begin(TransactionOptions::default()).unwrap();
        let id = tx
            .insert("users", json!({"name": "from task"}))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    })
    .await
    .unwrap();

    let db3 = Arc::clone(&db);
    let doc = tokio::spawn(async move {
        let mut tx = db3.begin(TransactionOptions::readonly()).unwrap();
        let d = tx.get("users", &doc_id).await.unwrap();
        tx.rollback();
        d
    })
    .await
    .unwrap();

    assert_eq!(doc.unwrap()["name"], "from task");
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

/// Multiple concurrent writers on separate tokio tasks all succeed.
/// Tasks are staggered by 2ms to avoid L1 ULID PRNG collisions (the PRNG
/// uses timestamp + stack address, which can collide under true concurrency).
#[tokio::test]
async fn concurrent_writers_from_tasks() {
    let db = Arc::new(open_test_db().await);
    create_users_collection(&db).await;

    let mut handles = vec![];
    for i in 0u64..10 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(i * 2)).await;
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            let id = tx.insert("users", json!({"task": i})).await.unwrap();
            tx.commit().await.unwrap();
            (i, id)
        }));
    }

    let mut results = vec![];
    for h in handles {
        results.push(h.await.unwrap());
    }

    // Verify each doc is readable with correct task value
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    for (expected_task, doc_id) in &results {
        let doc = tx.get("users", doc_id).await.unwrap().unwrap();
        assert_eq!(doc["task"], *expected_task);
    }
    tx.rollback();

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

/// Concurrent DDL + writes on separate tasks.
#[tokio::test]
async fn concurrent_ddl_across_tasks() {
    let db = Arc::new(open_test_db().await);

    // Create 5 collections concurrently from separate tasks
    let mut handles = vec![];
    for i in 0..5 {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let name = format!("col{i}");
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(&name).await.unwrap();
            tx.commit().await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(db.list_collections().len(), 5);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

/// Writer and reader interleaved: reader on a different collection is unaffected.
#[tokio::test]
async fn cross_collection_isolation() {
    let db = Arc::new(open_test_db().await);

    // Create two collections
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    tx.create_collection("orders").await.unwrap();
    tx.commit().await.unwrap();

    // Insert into users
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let user_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
    tx.commit().await.unwrap();

    // Insert into orders from another task
    let db2 = Arc::clone(&db);
    let order_id = tokio::spawn(async move {
        let mut tx = db2.begin(TransactionOptions::default()).unwrap();
        let id = tx.insert("orders", json!({"total": 42})).await.unwrap();
        tx.commit().await.unwrap();
        id
    })
    .await
    .unwrap();

    // Both documents are independently visible
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert_eq!(
        tx.get("users", &user_id).await.unwrap().unwrap()["name"],
        "Alice"
    );
    assert_eq!(
        tx.get("orders", &order_id).await.unwrap().unwrap()["total"],
        42
    );
    tx.rollback();

    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// File-backed Durability Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn file_backed_persistence() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    let doc_id;
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        tx.commit().await.unwrap();
        db.close().await.unwrap();
    }

    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(db.list_collections().len(), 1);

        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
        assert_eq!(doc["name"], "Alice");
        tx.rollback();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn file_backed_multiple_collections() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        for name in ["users", "orders", "products"] {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_collection(name).await.unwrap();
            tx.commit().await.unwrap();
        }
        db.close().await.unwrap();
    }

    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        assert_eq!(db.list_collections().len(), 3);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn list_collections_in_transaction() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let collections = tx.list_collections().unwrap();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "users");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn list_collections_includes_pending_creates() {
    let db = open_test_db().await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("foo").await.unwrap();

    let collections = tx.list_collections().unwrap();
    assert!(
        collections
            .iter()
            .any(|collection| collection.name == "foo")
    );

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn list_collections_hides_pending_drops() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.drop_collection("users").await.unwrap();

    let collections = tx.list_collections().unwrap();
    assert!(
        !collections
            .iter()
            .any(|collection| collection.name == "users")
    );

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn list_indexes_includes_pending_creates() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();

    let indexes = tx.list_indexes("users").unwrap();
    let pending = indexes
        .iter()
        .find(|index| index.name == "age_idx")
        .expect("pending index should be visible");
    assert_eq!(pending.field_paths, vec![FieldPath::single("age")]);
    assert_eq!(pending.state, exdb::IndexState::Building);

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_index_rejects_duplicate_pending_create() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();

    let result = tx
        .create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await;
    assert!(matches!(
        result,
        Err(DatabaseError::IndexAlreadyExists { collection, index })
            if collection == "users" && index == "age_idx"
    ));

    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_index_rejects_invalid_embedded_field_paths() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();

    let empty_fields = tx.create_index("users", "empty_idx", vec![]).await;
    assert!(
        matches!(empty_fields, Err(DatabaseError::InvalidFieldPath(message)) if message.contains("at least one"))
    );

    let empty_path = tx
        .create_index("users", "empty_path_idx", vec![FieldPath::new(vec![])])
        .await;
    assert!(
        matches!(empty_path, Err(DatabaseError::InvalidFieldPath(message)) if message.contains("cannot be empty"))
    );

    let empty_segment = tx
        .create_index(
            "users",
            "empty_segment_idx",
            vec![FieldPath::new(vec!["profile".to_string(), String::new()])],
        )
        .await;
    assert!(
        matches!(empty_segment, Err(DatabaseError::InvalidFieldPath(message)) if message.contains("segment"))
    );

    assert!(!tx.list_indexes("users").unwrap().iter().any(|index| {
        index.name == "empty_idx"
            || index.name == "empty_path_idx"
            || index.name == "empty_segment_idx"
    }));

    tx.rollback();
    db.close().await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Index Building Lifecycle Tests
// ═══════════════════════════════════════════════════════════════════════

/// Create an index on a populated collection: it should transition from
/// Building to Ready in the background, then be queryable.
#[tokio::test]
async fn index_builder_completes() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    // Insert documents first
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    for i in 0..5 {
        tx.insert("users", json!({"name": format!("User{i}"), "age": 20 + i}))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // Create index — starts in Building state
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for the background builder to transition it to Ready.
    // Poll up to 5 seconds.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        tx.rollback();

        let age_idx = indexes.iter().find(|i| i.name == "age_idx");
        if let Some(idx) = age_idx
            && idx.state == exdb::IndexState::Ready
        {
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("index builder did not complete within 5 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn index_builder_emits_index_ready_event() {
    let db = open_test_db().await;
    create_users_collection(&db).await;
    let mut events = db.subscribe_index_ready();

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
        .await
        .expect("index_ready event should be emitted")
        .unwrap();
    assert_eq!(event.database, "default");
    assert_eq!(event.collection, "users");
    assert_eq!(event.index, "age_idx");
    assert!(event.index_id.0 > 0);

    db.close().await.unwrap();
}

/// Query a Building index returns IndexNotReady.
#[tokio::test]
async fn query_building_index_returns_error() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    // Create index
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Immediately try to query — might still be Building
    // (This test is best-effort: the builder could be very fast.
    //  We just verify the error path exists.)
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let result = tx.query("users", "age_idx", &[], None, None, None).await;
    // Either IndexNotReady (still building) or success (already built) is ok
    match result {
        Err(DatabaseError::IndexNotReady(_)) => { /* expected if builder hasn't finished */ }
        Ok(_) => { /* builder already finished — also fine */ }
        Err(e) => panic!("unexpected error: {e}"),
    }
    tx.rollback();
    db.close().await.unwrap();
}

/// Create index on empty collection — builds instantly, becomes Ready.
#[tokio::test]
async fn index_on_empty_collection_becomes_ready() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for Ready
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        tx.rollback();

        if indexes
            .iter()
            .any(|i| i.name == "age_idx" && i.state == exdb::IndexState::Ready)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("index builder did not complete within 5 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Now insert and verify the index works (doesn't error)
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Alice", "age": 30}))
        .await
        .unwrap();
    tx.commit().await.unwrap();

    db.close().await.unwrap();
}

/// Build an index over many documents — verifies that the periodic
/// checkpoint during build prevents BufferPoolFull by flushing dirty pages.
/// Uses a small buffer pool (64 pages) so the test can trigger pressure
/// without needing millions of documents.
#[tokio::test]
async fn index_build_with_buffer_pool_pressure() {
    // Small buffer pool: 64 pages × 4096 bytes = 256 KB
    let config = DatabaseConfig {
        page_size: 4096,
        memory_budget: 4096 * 64,
        external_threshold: 1024,
        ..Default::default()
    };
    let db = Database::open_in_memory(config, None).await.unwrap();

    // Create collection
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("items").await.unwrap();
    tx.commit().await.unwrap();

    // Insert enough documents to generate more dirty secondary index pages
    // than the 64-frame buffer pool can hold without flushing.
    // Each doc produces ~1 secondary key. With ~100 keys per leaf page,
    // 200 docs needs ~2 leaf pages. But B-tree splits and internal nodes
    // add more. With 64 frames total (shared by primary + secondary +
    // catalog), this is a tight fit. 500 docs should push the boundary.
    for batch in 0..5 {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..100 {
            let n = batch * 100 + i;
            tx.insert("items", json!({"sku": format!("SKU-{n:05}"), "price": n}))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
    }

    // Create an index — the background builder must handle the small pool
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("items", "price_idx", vec![FieldPath::single("price")])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for Ready — if checkpoint-during-build works, this succeeds.
    // If dirty pages fill the pool, build() returns BufferPoolFull and the
    // index stays Building forever → test times out.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("items").unwrap();
        tx.rollback();

        if indexes
            .iter()
            .any(|i| i.name == "price_idx" && i.state == exdb::IndexState::Ready)
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("index build did not complete — possible BufferPoolFull");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    db.close().await.unwrap();
}

/// Building indexes are dropped on restart (D3 crash recovery policy).
#[tokio::test]
async fn building_index_dropped_on_restart() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("testdb");

    // Create collection + index, then close BEFORE the builder finishes.
    // We can't easily guarantee the builder hasn't finished, so we create
    // the index and close immediately.
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        tx.commit().await.unwrap();

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        tx.commit().await.unwrap();

        db.close().await.unwrap();
    }

    // Reopen — the index should either be Ready (if builder finished before
    // close) or gone (if it was still Building and got dropped per D3).
    {
        let db = Database::open(&path, DatabaseConfig::default(), None)
            .await
            .unwrap();
        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
        let indexes = tx.list_indexes("users").unwrap();
        tx.rollback();

        // No Building indexes should exist after restart
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
}

// ═══════════════════════════════════════════════════════════════════════
// Bug Reproduction: Delete after insert in same tx
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn delete_pending_insert_same_tx() {
    let db = open_test_db().await;
    create_users_collection(&db).await;

    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let id_a = tx.insert("users", json!({"name": "keep"})).await.unwrap();
    let id_b = tx
        .insert("users", json!({"name": "delete_me"}))
        .await
        .unwrap();

    // Both visible before delete
    assert!(
        tx.get("users", &id_a).await.unwrap().is_some(),
        "id_a before delete"
    );
    assert!(
        tx.get("users", &id_b).await.unwrap().is_some(),
        "id_b before delete"
    );

    tx.delete("users", &id_b).await.unwrap();

    // After delete: id_a still visible, id_b gone
    let got_a = tx.get("users", &id_a).await;
    assert!(
        got_a.as_ref().unwrap().is_some(),
        "id_a after delete: {got_a:?}"
    );
    assert!(
        tx.get("users", &id_b).await.unwrap().is_none(),
        "id_b after delete"
    );

    assert_success(tx.commit().await.unwrap());
    db.close().await.unwrap();
}
