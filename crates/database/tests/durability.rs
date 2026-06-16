//! Durability integration tests for the L6 Database.
//!
//! Each test creates a file-backed database, performs operations, simulates a
//! crash via `db.crash().await` (cancels background tasks without final
//! checkpoint), and reopens to verify durability.
//!
//! WAL replay is integrated into `Database::open`: physical L2 recovery runs
//! first, then L6 replays committed catalog/data records and reconciles Ready
//! secondary indexes from recovered primary data.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use exdb::{
    Database, DatabaseConfig, DatabaseError, FieldPath, RangeExpr, ReplicationHook, Scalar,
    TransactionOptions, TransactionResult,
};
use serde_json::json;
use tokio::sync::Notify;

// ─── Helpers ───

async fn open_db(path: &Path) -> Database {
    Database::open(path, DatabaseConfig::default(), None)
        .await
        .unwrap()
}

#[tokio::test]
async fn file_header_generation_persists_and_can_be_probed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");

    let db = open_db(&path).await;
    assert_eq!(db.generation().await, 1);
    db.set_generation(17).await.unwrap();
    db.close().await.unwrap();

    assert_eq!(
        Database::read_generation_at_path(&path, DatabaseConfig::default())
            .await
            .unwrap(),
        17
    );

    let reopened = open_db(&path).await;
    assert_eq!(reopened.generation().await, 17);
    reopened.close().await.unwrap();
}

async fn open_db_with_failing_replication(path: &Path) -> Database {
    Database::open(
        path,
        DatabaseConfig::default(),
        Some(Box::new(FailingReplication)),
    )
    .await
    .unwrap()
}

struct FailingReplication;

#[async_trait::async_trait]
impl ReplicationHook for FailingReplication {
    async fn replicate_and_wait(
        &self,
        _lsn: exdb_storage::wal::Lsn,
        _record: &[u8],
    ) -> Result<(), String> {
        Err("test replication failure".to_string())
    }
}

#[derive(Clone, Default)]
struct CapturingReplication {
    records: Arc<Mutex<CapturedReplicationRecords>>,
}

type CapturedReplicationRecords = Vec<(exdb_storage::wal::Lsn, Vec<u8>)>;

impl CapturingReplication {
    fn records(&self) -> Vec<(exdb_storage::wal::Lsn, Vec<u8>)> {
        self.records.lock().unwrap().clone()
    }

    fn new_records_from(&self, start: usize) -> Vec<(exdb_storage::wal::Lsn, Vec<u8>)> {
        self.records.lock().unwrap()[start..].to_vec()
    }
}

#[async_trait::async_trait]
impl ReplicationHook for CapturingReplication {
    async fn replicate_and_wait(
        &self,
        lsn: exdb_storage::wal::Lsn,
        record: &[u8],
    ) -> Result<(), String> {
        self.records.lock().unwrap().push((lsn, record.to_vec()));
        Ok(())
    }
}

#[derive(Clone)]
struct BlockingFailingReplication {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}

impl BlockingFailingReplication {
    fn new() -> Self {
        Self {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn wait_until_called(&self) {
        loop {
            if self.calls.load(Ordering::Acquire) > 0 {
                return;
            }
            self.entered.notified().await;
        }
    }

    fn release_with_failure(&self) {
        self.release.notify_waiters();
    }
}

#[async_trait::async_trait]
impl ReplicationHook for BlockingFailingReplication {
    async fn replicate_and_wait(
        &self,
        _lsn: exdb_storage::wal::Lsn,
        _record: &[u8],
    ) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_waiters();
        self.release.notified().await;
        Err("test replication failure after crash".to_string())
    }
}

fn assert_success(result: TransactionResult) -> exdb::Ts {
    match result {
        TransactionResult::Success { commit_ts, .. } => commit_ts,
        TransactionResult::Conflict { error, .. } => panic!("unexpected conflict: {error:?}"),
        TransactionResult::QuorumLost => panic!("unexpected quorum lost"),
    }
}

fn assert_quorum_lost(result: TransactionResult) {
    match result {
        TransactionResult::QuorumLost => {}
        TransactionResult::Success { commit_ts, .. } => {
            panic!("expected quorum lost, got success at {commit_ts}")
        }
        TransactionResult::Conflict { error, .. } => {
            panic!("expected quorum lost, got conflict: {error:?}")
        }
    }
}

async fn assert_clean_integrity(db: &Database) {
    let report = db.check_integrity().await.unwrap();
    assert!(
        !report.has_errors(),
        "database integrity check should not report errors: {:?}",
        report.issues
    );
    assert_eq!(
        report.stats.orphan_heap_pages, 0,
        "rollback vacuum should not leave orphan heap/overflow pages: {:?}",
        report.issues
    );
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

async fn apply_replicated_records(db: &Database, records: Vec<(exdb_storage::wal::Lsn, Vec<u8>)>) {
    for (lsn, payload) in records {
        db.apply_replicated_wal(lsn, &payload).await.unwrap();
    }
}

#[tokio::test]
async fn replicated_wal_apply_is_visible_and_durable_after_reopen() {
    let tmp = tempfile::TempDir::new().unwrap();
    let primary_path = tmp.path().join("primary");
    let replica_path = tmp.path().join("replica");
    let capture = CapturingReplication::default();

    let primary = Database::open(
        &primary_path,
        DatabaseConfig::default(),
        Some(Box::new(capture.clone())),
    )
    .await
    .unwrap();
    let replica = open_db(&replica_path).await;

    let mut tx = primary.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = primary.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert("users", json!({"name": "Ada", "age": 37}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let records = capture.records();
    let max_source_lsn = records.iter().map(|(lsn, _)| *lsn).max().unwrap();
    apply_replicated_records(&replica, records.clone()).await;
    assert_eq!(replica.replication_applied_lsn().await, max_source_lsn);
    replica
        .apply_replicated_wal(records[0].0, &records[0].1)
        .await
        .unwrap();
    assert_eq!(replica.replication_applied_lsn().await, max_source_lsn);

    let mut tx = replica.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Ada");
    assert_eq!(doc["age"], 37);
    tx.rollback();

    assert_clean_integrity(&replica).await;
    primary.close().await.unwrap();
    replica.close().await.unwrap();

    let reopened = open_db(&replica_path).await;
    assert_eq!(reopened.replication_applied_lsn().await, max_source_lsn);
    let mut tx = reopened.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Ada");
    assert_eq!(doc["age"], 37);
    tx.rollback();
    assert_clean_integrity(&reopened).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn replicated_wal_apply_updates_ready_secondary_indexes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let primary_path = tmp.path().join("primary");
    let replica_path = tmp.path().join("replica");
    let capture = CapturingReplication::default();

    let primary = Database::open(
        &primary_path,
        DatabaseConfig::default(),
        Some(Box::new(capture.clone())),
    )
    .await
    .unwrap();
    let replica = open_db(&replica_path).await;
    let mut applied = 0;

    let mut tx = primary.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    assert_success(tx.commit().await.unwrap());
    let records = capture.new_records_from(applied);
    applied += records.len();
    apply_replicated_records(&replica, records).await;

    let mut tx = primary.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    let records = capture.new_records_from(applied);
    applied += records.len();
    apply_replicated_records(&replica, records).await;
    wait_index_ready(&replica, "users", "age_idx").await;

    let mut tx = primary.begin(TransactionOptions::default()).unwrap();
    tx.insert("users", json!({"name": "Grace", "age": 42}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    let records = capture.new_records_from(applied);
    apply_replicated_records(&replica, records).await;

    let mut tx = replica.begin(TransactionOptions::readonly()).unwrap();
    let docs = tx
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
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Grace");
    tx.rollback();

    assert_clean_integrity(&replica).await;
    primary.close().await.unwrap();
    replica.close().await.unwrap();
}

#[tokio::test]
async fn database_snapshot_restore_reconstructs_data_indexes_and_future_commits() {
    let tmp = tempfile::TempDir::new().unwrap();
    let source_path = tmp.path().join("source");
    let restored_path = tmp.path().join("restored");

    let source = open_db(&source_path).await;
    let mut tx = source.begin(TransactionOptions::default()).unwrap();
    tx.create_collection("users").await.unwrap();
    assert_success(tx.commit().await.unwrap());

    let mut tx = source.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    wait_index_ready(&source, "users", "age_idx").await;

    let mut tx = source.begin(TransactionOptions::default()).unwrap();
    let ada_id = tx
        .insert("users", json!({"name": "Ada", "age": 37}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());

    let snapshot = source.export_snapshot().await.unwrap();
    Database::restore_snapshot(&restored_path, DatabaseConfig::default(), snapshot)
        .await
        .unwrap();

    let restored = open_db(&restored_path).await;
    let mut tx = restored.begin(TransactionOptions::readonly()).unwrap();
    let ada = tx.get("users", &ada_id).await.unwrap().unwrap();
    assert_eq!(ada["name"], "Ada");
    let docs = tx
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
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "Ada");
    tx.rollback();

    let mut tx = restored.begin(TransactionOptions::default()).unwrap();
    let grace_id = tx
        .insert("users", json!({"name": "Grace", "age": 42}))
        .await
        .unwrap();
    assert_success(tx.commit().await.unwrap());
    assert_clean_integrity(&source).await;
    assert_clean_integrity(&restored).await;
    source.close().await.unwrap();
    restored.close().await.unwrap();

    let reopened = open_db(&restored_path).await;
    let mut tx = reopened.begin(TransactionOptions::readonly()).unwrap();
    let grace = tx.get("users", &grace_id).await.unwrap().unwrap();
    assert_eq!(grace["name"], "Grace");
    tx.rollback();
    assert_clean_integrity(&reopened).await;
    reopened.close().await.unwrap();
}

fn corrupt_page(path: &Path, page_size: usize, page_id: u32) {
    let data_path = path.join("data.db");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&data_path)
        .unwrap();
    file.seek(SeekFrom::Start(
        page_id as u64 * page_size as u64 + page_size as u64 - 1,
    ))
    .unwrap();
    file.write_all(&[0xA5]).unwrap();
    file.sync_data().unwrap();
}

fn append_trailing_data_file_byte(path: &Path) {
    let data_path = path.join("data.db");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&data_path)
        .unwrap();
    let len = file.metadata().unwrap().len();
    file.seek(SeekFrom::Start(len)).unwrap();
    file.write_all(&[0xA5]).unwrap();
    file.sync_data().unwrap();
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
async fn startup_integrity_check_rejects_corrupt_cold_page() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let page_size = DatabaseConfig::default().page_size;
    let primary_root_page;

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        primary_root_page = db.get_collection("users").unwrap().primary_root_page;
        db.close().await.unwrap();
    }

    corrupt_page(&path, page_size, primary_root_page);

    let config = DatabaseConfig {
        check_on_startup: true,
        ..Default::default()
    };
    let err = Database::open(&path, config, None).await.err().unwrap();
    assert!(
        matches!(
            err,
            DatabaseError::IntegrityCheckFailed {
                ref phase,
                errors: 1..,
                ..
            } if phase == "startup quick"
        ),
        "expected startup integrity failure, got {err:?}",
    );
}

#[tokio::test]
async fn startup_integrity_check_rejects_data_file_size_mismatch() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        tx.insert("users", json!({"name": "Alice"})).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.close().await.unwrap();
    }

    append_trailing_data_file_byte(&path);

    let config = DatabaseConfig {
        check_on_startup: true,
        ..Default::default()
    };
    let err = Database::open(&path, config, None).await.err().unwrap();
    match err {
        DatabaseError::IntegrityCheckFailed {
            phase,
            errors,
            issues,
            ..
        } => {
            assert_eq!(phase, "startup quick");
            assert!(errors >= 1);
            assert!(issues >= errors);
        }
        other => panic!("expected startup integrity failure, got {other:?}"),
    }
}

#[tokio::test]
async fn repair_integrity_truncates_data_file_size_mismatch_and_survives_reopen() {
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

    append_trailing_data_file_byte(&path);

    {
        let db = open_db(&path).await;
        let before = db.check_integrity().await.unwrap();
        assert!(
            before
                .issues
                .iter()
                .any(|issue| issue.message.contains("data file size")),
            "expected data-file size issue before repair: {:?}",
            before.issues
        );

        let repair = db.repair_integrity().await.unwrap();
        assert!(
            repair.repairs.iter().any(|repair| repair
                .message
                .contains("truncated 1 trailing data-file byte")),
            "expected trailing-byte truncation repair: {:?}",
            repair.repairs
        );
        assert!(
            repair.is_clean(),
            "expected clean repair result, remaining issues: {:?}",
            repair.remaining_issues
        );
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        check_on_startup: true,
        check_on_startup_full: true,
        ..Default::default()
    };
    let reopened = Database::open(&path, config, None).await.unwrap();
    let mut tx = reopened.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    tx.rollback();
    assert_clean_integrity(&reopened).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn disk_quota_rejects_commit_wal_without_visibility() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    let quota;
    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.storage().checkpoint().await.unwrap();
        quota = db.usage().disk_usage_bytes + 128;
        db.close().await.unwrap();
    }

    let config = DatabaseConfig {
        max_disk_usage_bytes: Some(quota),
        ..Default::default()
    };
    let db = Database::open(&path, config, None).await.unwrap();
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let doc_id = tx
        .insert(
            "users",
            json!({
                "name": "Too Large For Remaining WAL Quota",
                "payload": "x".repeat(2048),
            }),
        )
        .await
        .unwrap();
    assert_quorum_lost(tx.commit().await.unwrap());

    let mut read = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(
        read.get("users", &doc_id).await.unwrap().is_none(),
        "quota-rejected commit must not become visible"
    );
    read.rollback();
    db.close().await.unwrap();

    let reopened = open_db(&path).await;
    let mut read = reopened.begin(TransactionOptions::readonly()).unwrap();
    assert!(
        read.get("users", &doc_id).await.unwrap().is_none(),
        "quota-rejected commit must not be recovered after reopen"
    );
    read.rollback();
    assert_clean_integrity(&reopened).await;
    reopened.close().await.unwrap();
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
async fn unreplicated_collection_lost_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("unreplicated").await.unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(
        db.list_collections().is_empty(),
        "collection from unreplicated commit must be rolled back during recovery"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_drop_collection_preserves_collection_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").await.unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let names: Vec<_> = db.list_collections().into_iter().map(|c| c.name).collect();
    assert_eq!(names, vec!["users"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_create_index_lost_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();
    assert!(
        indexes.iter().all(|idx| idx.name != "age_idx"),
        "index from unreplicated commit must be rolled back during recovery"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_drop_index_preserves_index_on_crash() {
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
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_index("users", "age_idx").await.unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();
    assert!(
        indexes
            .iter()
            .any(|idx| idx.name == "age_idx" && idx.state == exdb::IndexState::Ready),
        "index from visible commit must survive unreplicated drop rollback"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_insert_removed_physically_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let doc_id;

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
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx
            .insert(
                "users",
                json!({
                    "name": "Ghost",
                    "age": 99,
                    "payload": "x".repeat(12_000),
                }),
            )
            .await
            .unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    let results = tx
        .query(
            "users",
            "age_idx",
            &[exdb::RangeExpr::Eq(
                FieldPath::single("age"),
                Scalar::Int64(99),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    tx.rollback();
    assert_clean_integrity(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_replication_insert_rolls_back_after_crash_before_visible_ts() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let hook = BlockingFailingReplication::new();
    let doc_id;

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
        db.close().await.unwrap();
    }

    {
        let db = Database::open(
            &path,
            DatabaseConfig::default(),
            Some(Box::new(hook.clone())),
        )
        .await
        .unwrap();
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        doc_id = tx
            .insert(
                "users",
                json!({
                    "name": "Pending",
                    "age": 77,
                    "payload": "p".repeat(12_000),
                }),
            )
            .await
            .unwrap();

        let commit_task = tokio::spawn(async move { tx.commit().await });
        hook.wait_until_called().await;
        db.crash().await;
        hook.release_with_failure();
        let result = commit_task.await.unwrap().unwrap();
        assert_quorum_lost(result);
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    let results = tx
        .query(
            "users",
            "age_idx",
            &[exdb::RangeExpr::Eq(
                FieldPath::single("age"),
                Scalar::Int64(77),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    tx.rollback();
    assert_clean_integrity(&db).await;
    db.crash().await;

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    assert!(tx.get("users", &doc_id).await.unwrap().is_none());
    tx.rollback();
    assert_clean_integrity(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_replace_removed_physically_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let doc_id;

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx
            .insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "age_idx").await;
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace(
            "users",
            &doc_id,
            json!({
                "name": "Bob",
                "age": 31,
                "payload": "y".repeat(12_000),
            }),
        )
        .await
        .unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
    assert_eq!(doc["age"], 30);
    let results = tx
        .query(
            "users",
            "age_idx",
            &[exdb::RangeExpr::Eq(
                FieldPath::single("age"),
                Scalar::Int64(31),
            )],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    tx.rollback();
    assert_clean_integrity(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn unreplicated_delete_removed_physically_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");
    let doc_id;

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        doc_id = tx
            .insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "age_idx").await;
        db.close().await.unwrap();
    }

    {
        let db = open_db_with_failing_replication(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &doc_id).await.unwrap();
        assert_quorum_lost(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let doc = tx.get("users", &doc_id).await.unwrap().unwrap();
    assert_eq!(doc["name"], "Alice");
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
    tx.rollback();
    assert_clean_integrity(&db).await;
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
        tx.replace("users", &doc_id, json!({"name": "Bob"}))
            .await
            .unwrap();
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
        doc_id = tx
            .insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.patch("users", &doc_id, json!({"age": 31}))
            .await
            .unwrap();
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
async fn ddl_and_data_in_one_tx_rolled_back_on_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        tx.insert("users", json!({"name": "Uncommitted"}))
            .await
            .unwrap();
        drop(tx);
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    assert_clean_integrity(&db).await;
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

#[tokio::test]
async fn cascade_drop_collection_indexes_survive_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        for (name, field) in [
            ("email_idx", "email"),
            ("age_idx", "age"),
            ("city_idx", "city"),
        ] {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.create_index("users", name, vec![FieldPath::single(field)])
                .await
                .unwrap();
            assert_success(tx.commit().await.unwrap());
            wait_index_ready(&db, "users", name).await;
        }

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.drop_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    assert_clean_integrity(&db).await;
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
        doc_id = tx
            .insert("users", json!({"name": "PostCheckpoint"}))
            .await
            .unwrap();
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
    assert_eq!(
        tx.get("users", &id_a).await.unwrap().unwrap()["name"],
        "Before"
    );
    assert_eq!(
        tx.get("users", &id_b).await.unwrap().unwrap()["name"],
        "After"
    );
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

#[tokio::test]
async fn empty_checkpoint_harmless() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        db.storage().checkpoint().await.unwrap();
        db.crash().await;
    }

    let db = open_db(&path).await;
    assert!(db.list_collections().is_empty());
    assert_clean_integrity(&db).await;
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
    let names: Vec<String> = db
        .list_collections()
        .iter()
        .map(|c| c.name.clone())
        .collect();
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

#[tokio::test]
async fn catalog_name_btree_consistent_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    let result = tx.create_collection("users").await;
    assert!(matches!(
        result,
        Err(DatabaseError::CollectionAlreadyExists(name)) if name == "users"
    ));
    tx.rollback();
    assert_eq!(db.list_collections()[0].name, "users");
    assert_clean_integrity(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn created_at_index_present_after_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    tx.rollback();
    assert!(
        indexes.iter().any(|index| {
            index.name == "_created_at"
                && index.field_paths == vec![FieldPath::single("_created_at")]
                && index.state == exdb::IndexState::Ready
        }),
        "_created_at index should be Ready after crash recovery: {indexes:?}"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn index_state_preserved_after_crash() {
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
        indexes
            .iter()
            .any(|index| index.name == "age_idx" && index.state == exdb::IndexState::Ready),
        "Ready index state should be preserved after crash: {indexes:?}"
    );
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
        indexes
            .iter()
            .any(|i| i.name == "age_idx" && i.state == exdb::IndexState::Ready),
        "Ready index should survive crash"
    );
    db.close().await.unwrap();
}

#[tokio::test]
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

#[tokio::test]
async fn secondary_index_delete_survives_crash() {
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
        let doc_id = tx
            .insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.delete("users", &doc_id).await.unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let results = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(30))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn secondary_index_replace_survives_crash() {
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
        let doc_id = tx
            .insert("users", json!({"name": "Alice", "age": 20}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.replace("users", &doc_id, json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let old_results = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(20))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(old_results.is_empty());

    let new_results = tx
        .query(
            "users",
            "age_idx",
            &[RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(30))],
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(new_results.len(), 1);
    assert_eq!(new_results[0]["name"], "Alice");
    tx.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn compound_index_survives_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index(
            "users",
            "city_zip_idx",
            vec![FieldPath::single("city"), FieldPath::single("zip")],
        )
        .await
        .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "city_zip_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.insert(
            "users",
            json!({"name": "Alice", "city": "NYC", "zip": "10001"}),
        )
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
            "city_zip_idx",
            &[
                RangeExpr::Eq(FieldPath::single("city"), Scalar::String("NYC".to_string())),
                RangeExpr::Eq(
                    FieldPath::single("zip"),
                    Scalar::String("10001".to_string()),
                ),
            ],
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

#[tokio::test]
async fn array_index_entries_survive_crash() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "tags_idx", vec![FieldPath::single("tags")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "tags_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.insert("users", json!({"name": "Alice", "tags": ["a", "b", "c"]}))
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    for tag in ["a", "b", "c"] {
        let results = tx
            .query(
                "users",
                "tags_idx",
                &[RangeExpr::Eq(
                    FieldPath::single("tags"),
                    Scalar::String(tag.to_string()),
                )],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "tag {tag} should be queryable");
        assert_eq!(results[0]["name"], "Alice");
    }
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

#[tokio::test]
async fn ready_index_survives_crash_during_other_build() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("db");

    {
        let db = open_db(&path).await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_collection("users").await.unwrap();
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for age in 0..100 {
            tx.insert(
                "users",
                json!({
                    "age": age,
                    "name": format!("User{age}"),
                }),
            )
            .await
            .unwrap();
        }
        assert_success(tx.commit().await.unwrap());

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());
        wait_index_ready(&db, "users", "age_idx").await;

        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        tx.create_index("users", "name_idx", vec![FieldPath::single("name")])
            .await
            .unwrap();
        assert_success(tx.commit().await.unwrap());

        // Crash before the polling index-builder loop can make the second
        // index Ready. Recovery must drop only the incomplete index.
        db.crash().await;
    }

    let db = open_db(&path).await;
    let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
    let indexes = tx.list_indexes("users").unwrap();
    assert!(
        indexes
            .iter()
            .any(|idx| idx.name == "age_idx" && idx.state == exdb::IndexState::Ready),
        "previously Ready index should survive selective Building-index cleanup: {indexes:?}",
    );
    assert!(
        indexes
            .iter()
            .all(|idx| idx.state != exdb::IndexState::Building),
        "recovery should not leave Building indexes after restart: {indexes:?}",
    );

    let docs = tx
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
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], "User42");
    tx.rollback();

    assert_clean_integrity(&db).await;
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
