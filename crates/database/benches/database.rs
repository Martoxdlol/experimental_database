//! Benchmarks for the L6 Database layer.
//!
//! Measures insert throughput, read throughput, query performance,
//! transaction lifecycle overhead, and concurrent access patterns.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde_json::json;
use std::sync::Arc;
use tokio::runtime::Runtime;

use exdb::{
    Database, DatabaseConfig, FieldPath, Scalar, TransactionOptions,
};

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn setup_db() -> Database {
    Database::open_in_memory(DatabaseConfig::default(), None)
        .await
        .unwrap()
}

async fn setup_db_with_collection(name: &str) -> Database {
    let db = setup_db().await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_collection(name).await.unwrap();
    tx.commit().await.unwrap();
    db
}

async fn setup_db_with_indexed_collection() -> Database {
    let db = setup_db_with_collection("users").await;
    let mut tx = db.begin(TransactionOptions::default()).unwrap();
    tx.create_index("users", "age_idx", vec![FieldPath::single("age")])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for index to be ready
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
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
            panic!("index did not become ready");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    db
}

// ═══════════════════════════════════════════════════════════════════════
// Insert Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_single_insert(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(setup_db_with_collection("users"));

    c.bench_function("insert_single_doc", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            tx.insert("users", json!({"name": "Alice", "age": 30, "email": "alice@test.com"}))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        });
    });

    rt.block_on(db.close()).unwrap();
}

fn bench_batch_insert(c: &mut Criterion) {
    let rt = rt();

    let mut group = c.benchmark_group("batch_insert");
    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("{batch_size}_docs"), |b| {
            b.iter_batched(
                || rt.block_on(setup_db_with_collection("data")),
                |db| {
                    rt.block_on(async {
                        let mut tx = db.begin(TransactionOptions::default()).unwrap();
                        for i in 0..batch_size {
                            tx.insert("data", json!({"seq": i, "payload": "x".repeat(100)}))
                                .await
                                .unwrap();
                        }
                        tx.commit().await.unwrap();
                        db.close().await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ═══════════════════════════════════════════════════════════════════════
// Read Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_point_read(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(async {
        let db = setup_db_with_collection("users").await;
        // Pre-populate
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..1000 {
            tx.insert("users", json!({"name": format!("User{i}"), "age": i % 100}))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        db
    });

    // Get a known doc ID
    let doc_id = rt.block_on(async {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let id = tx.insert("users", json!({"name": "target"})).await.unwrap();
        tx.commit().await.unwrap();
        id
    });

    c.bench_function("point_read", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            tx.get("users", &doc_id).await.unwrap();
            tx.rollback();
        });
    });

    rt.block_on(db.close()).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Index Query Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_index_point_query(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(async {
        let db = setup_db_with_indexed_collection().await;
        // Pre-populate with indexed data
        for batch in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..100 {
                tx.insert("users", json!({"name": format!("User{}", batch * 100 + i), "age": i}))
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        }
        db
    });

    c.bench_function("index_point_query", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            tx.query(
                "users",
                "age_idx",
                &[exdb::RangeExpr::Eq(FieldPath::single("age"), Scalar::Int64(42))],
                None,
                None,
                None,
            )
            .await
            .unwrap();
            tx.rollback();
        });
    });

    rt.block_on(db.close()).unwrap();
}

fn bench_index_range_query(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(async {
        let db = setup_db_with_indexed_collection().await;
        for batch in 0..10 {
            let mut tx = db.begin(TransactionOptions::default()).unwrap();
            for i in 0..100 {
                tx.insert("users", json!({"name": format!("User{}", batch * 100 + i), "age": i}))
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        }
        db
    });

    c.bench_function("index_range_query_10pct", |b| {
        b.to_async(&rt).iter(|| async {
            let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
            tx.query(
                "users",
                "age_idx",
                &[
                    exdb::RangeExpr::Gte(FieldPath::single("age"), Scalar::Int64(40)),
                    exdb::RangeExpr::Lt(FieldPath::single("age"), Scalar::Int64(50)),
                ],
                None,
                None,
                None,
            )
            .await
            .unwrap();
            tx.rollback();
        });
    });

    rt.block_on(db.close()).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction Lifecycle Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_transaction_overhead(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(setup_db());

    c.bench_function("begin_rollback_cycle", |b| {
        b.to_async(&rt).iter(|| async {
            let tx = db.begin(TransactionOptions::readonly()).unwrap();
            tx.rollback();
        });
    });

    rt.block_on(db.close()).unwrap();
}

fn bench_readonly_commit(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(setup_db_with_collection("data"));

    c.bench_function("readonly_begin_commit", |b| {
        b.to_async(&rt).iter(|| async {
            let tx = db.begin(TransactionOptions::default()).unwrap();
            tx.commit().await.unwrap();
        });
    });

    rt.block_on(db.close()).unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// DDL Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_create_collection(c: &mut Criterion) {
    let rt = rt();

    c.bench_function("create_collection", |b| {
        let counter = std::sync::atomic::AtomicU64::new(0);
        b.iter_batched(
            || rt.block_on(setup_db()),
            |db| {
                let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    let mut tx = db.begin(TransactionOptions::default()).unwrap();
                    tx.create_collection(&format!("col_{i}")).await.unwrap();
                    tx.commit().await.unwrap();
                    db.close().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent Access Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = rt();
    let db = Arc::new(rt.block_on(async {
        let db = setup_db_with_collection("users").await;
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        for i in 0..100 {
            tx.insert("users", json!({"name": format!("User{i}")}))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        db
    }));

    let doc_id = rt.block_on(async {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let id = tx.insert("users", json!({"name": "target"})).await.unwrap();
        tx.commit().await.unwrap();
        id
    });

    c.bench_function("concurrent_reads_4_tasks", |b| {
        b.to_async(&rt).iter(|| {
            let db = Arc::clone(&db);
            async move {
                let mut handles = Vec::new();
                for _ in 0..4 {
                    let db = Arc::clone(&db);
                    handles.push(tokio::spawn(async move {
                        let mut tx = db.begin(TransactionOptions::readonly()).unwrap();
                        tx.get("users", &doc_id).await.unwrap();
                        tx.rollback();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            }
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// Replace / Patch Benchmarks
// ═══════════════════════════════════════════════════════════════════════

fn bench_replace(c: &mut Criterion) {
    let rt = rt();
    let db = Arc::new(rt.block_on(setup_db_with_collection("users")));
    let doc_id = rt.block_on(async {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let id = tx
            .insert("users", json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    });

    c.bench_function("replace_doc", |b| {
        b.to_async(&rt).iter(|| {
            let db = Arc::clone(&db);
            async move {
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.replace("users", &doc_id, json!({"name": "Alice", "age": 99}))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        });
    });
}

fn bench_patch(c: &mut Criterion) {
    let rt = rt();
    let db = Arc::new(rt.block_on(setup_db_with_collection("users")));
    let doc_id = rt.block_on(async {
        let mut tx = db.begin(TransactionOptions::default()).unwrap();
        let id = tx
            .insert("users", json!({"name": "Alice", "age": 30, "email": "a@b.com"}))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    });

    c.bench_function("patch_doc", |b| {
        b.to_async(&rt).iter(|| {
            let db = Arc::clone(&db);
            async move {
                let mut tx = db.begin(TransactionOptions::default()).unwrap();
                tx.patch("users", &doc_id, json!({"age": 42}))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        });
    });
}

// ═══════════════════════════════════════════════════════════════════════
// Groups
// ═══════════════════════════════════════════════════════════════════════

criterion_group!(
    writes,
    bench_single_insert,
    bench_batch_insert,
    bench_replace,
    bench_patch,
);

criterion_group!(
    reads,
    bench_point_read,
    bench_index_point_query,
    bench_index_range_query,
    bench_concurrent_reads,
);

criterion_group!(
    lifecycle,
    bench_transaction_overhead,
    bench_readonly_commit,
    bench_create_collection,
);

criterion_main!(writes, reads, lifecycle);
