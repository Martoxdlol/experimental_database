//! Benchmarks for the experimental database.
//!
//! Run with: cargo bench
//! Results are written to target/criterion/.
//!
//! Scenarios:
//!   insert/sequential        – one doc per transaction, single-threaded baseline
//!   insert/batch_{N}         – N docs per transaction (amortises commit overhead)
//!   read/point               – point lookups from a 10 k-doc collection
//!   scan/full_{N}            – full table scan on collections of 100/1k/10k docs
//!   scan/full_scan_eq        – Eq-filter scan WITHOUT an index (table scan)
//!   scan/index_scan_eq       – same Eq-filter scan WITH a secondary index
//!   concurrent/independent_writes_{N} – N tasks doing independent inserts (no OCC conflicts)
//!   concurrent/contended_writes_{N}   – N tasks all read-modify-writing the same doc (high OCC conflict rate)
//!   concurrent/mixed_{label} – mix of reader and writer tasks running in parallel

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use experimental_database::{
    Database, DbConfig, FieldPath, Filter, IndexSpec, IndexState, JsonScalar, PatchOp,
    QueryOptions, QueryType,
};
use tokio::runtime::Runtime;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn open_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(DbConfig::new(dir.path())).await.unwrap();
    db.create_collection("col").await.unwrap();
    (db, dir)
}

fn sample_doc(n: u64) -> Vec<u8> {
    format!(r#"{{"id":{n},"name":"user_{n}","score":{n},"active":true}}"#).into_bytes()
}

/// Insert `count` docs in batches of 500 to keep individual write sets small.
async fn bulk_insert(db: &Database, count: usize) {
    const CHUNK: usize = 500;
    let mut offset = 0;
    while offset < count {
        let end = (offset + CHUNK).min(count);
        let mut tx = db.start_mutation().await.unwrap();
        for i in offset..end {
            tx.insert("col", sample_doc(i as u64)).await.unwrap();
        }
        tx.commit().await.unwrap();
        offset = end;
    }
}

// ─── 1. Sequential single-insert transactions ─────────────────────────────────
//
// Measures the full round-trip cost of one mutation session:
//   start_mutation → insert → commit (WAL append, MVCC ts alloc, BTreeMap insert).

fn bench_insert_sequential(c: &mut Criterion) {
    let rt = rt();
    let (db, _dir) = rt.block_on(open_db());

    c.bench_function("insert/sequential", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut tx = db.start_mutation().await.unwrap();
                tx.insert("col", sample_doc(1)).await.unwrap();
                tx.commit().await.unwrap();
            })
        });
    });
}

// ─── 2. Batch inserts (N docs per tx) ─────────────────────────────────────────
//
// Shows how amortising the per-commit overhead (WAL Commit record, OCC validate,
// ts allocation) improves throughput as batch size grows.

fn bench_insert_batch(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("insert/batch");

    for batch_size in [10usize, 100, 1000] {
        let (db, _dir) = rt.block_on(open_db());
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &sz| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut tx = db.start_mutation().await.unwrap();
                        for i in 0..sz as u64 {
                            tx.insert("col", sample_doc(i)).await.unwrap();
                        }
                        tx.commit().await.unwrap();
                    })
                });
            },
        );
    }
    group.finish();
}

// ─── 3. Point reads ───────────────────────────────────────────────────────────
//
// Measures a single get() against a 10 k-doc collection.
// Cost: catalog lookup + BTreeMap range scan (primary key prefix).

fn bench_read_point(c: &mut Criterion) {
    let rt = rt();
    let (db, _dir) = rt.block_on(async {
        let (db, dir) = open_db().await;
        bulk_insert(&db, 10_000).await;
        (db, dir)
    });

    let ids = rt.block_on(async {
        let mut q = db
            .start_query(QueryType::Find, 0, QueryOptions::default())
            .await
            .unwrap();
        q.find_ids("col", None).await.unwrap()
    });
    let n = ids.len();

    c.bench_function("read/point", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let id = ids[i % n];
            i += 1;
            rt.block_on(async {
                let mut q = db
                    .start_query(QueryType::Find, 0, QueryOptions::default())
                    .await
                    .unwrap();
                black_box(q.get("col", id).await.unwrap())
            })
        });
    });
}

// ─── 4. Full table scan at various collection sizes ───────────────────────────
//
// Documents how scan throughput (docs/s) changes with collection size.
// Reveals the O(n) cost of iterating the BTreeMap and applying MVCC visibility.

fn bench_scan_full(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("scan/full");
    group.sample_size(20);

    for size in [100usize, 1_000, 10_000] {
        let (db, _dir) = rt.block_on(async {
            let (db, dir) = open_db().await;
            bulk_insert(&db, size).await;
            (db, dir)
        });

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let mut q = db
                        .start_query(QueryType::Find, 0, QueryOptions::default())
                        .await
                        .unwrap();
                    black_box(q.find("col", None).await.unwrap())
                })
            });
        });
    }
    group.finish();
}

// ─── 5. Index scan vs full scan for an Eq filter ─────────────────────────────
//
// Both benchmarks run the same Filter::Eq query on a 10 k-doc collection.
// One DB has no index (forces a table scan + post-filter); the other has a
// secondary B-tree index on "score" that the planner will prefer (IndexScan).

fn bench_scan_indexed_vs_full(c: &mut Criterion) {
    let rt = rt();

    // Collection without an index → planner chooses TableScan
    let (db_no_idx, _dir1) = rt.block_on(async {
        let (db, dir) = open_db().await;
        bulk_insert(&db, 10_000).await;
        (db, dir)
    });

    // Collection with an index on "score" → planner chooses IndexScan
    let (db_idx, _dir2) = rt.block_on(async {
        let (db, dir) = open_db().await;
        bulk_insert(&db, 10_000).await;

        let idx_id = db
            .create_index("col", IndexSpec { field: FieldPath::single("score"), unique: false })
            .await
            .unwrap();

        // Wait for the background index build to finish.
        loop {
            match db.index_state("col", idx_id).await.unwrap() {
                IndexState::Ready { .. } => break,
                IndexState::Building { .. } => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                IndexState::Failed { message } => panic!("index build failed: {message}"),
            }
        }
        (db, dir)
    });

    let filter = Filter::Eq(FieldPath::single("score"), JsonScalar::I64(5000));
    let mut group = c.benchmark_group("scan");
    group.sample_size(20);

    group.bench_function("full_scan_eq", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut q = db_no_idx
                    .start_query(QueryType::Find, 0, QueryOptions::default())
                    .await
                    .unwrap();
                black_box(q.find("col", Some(filter.clone())).await.unwrap())
            })
        });
    });

    group.bench_function("index_scan_eq", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut q = db_idx
                    .start_query(QueryType::Find, 0, QueryOptions::default())
                    .await
                    .unwrap();
                black_box(q.find("col", Some(filter.clone())).await.unwrap())
            })
        });
    });

    group.finish();
}

// ─── 6. Concurrent independent writes ────────────────────────────────────────
//
// N Tokio tasks each open their own mutation session and insert a unique doc.
// No two tasks touch the same document, so OCC conflicts cannot occur.
// This shows how well the database scales with concurrency (WAL mpsc channel,
// parking_lot locks on the BTreeMap, Arc refcounting).

fn bench_concurrent_independent(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("concurrent/independent_writes");

    for num_tasks in [4usize, 8, 16, 32] {
        let (db, _dir) = rt.block_on(open_db());
        group.throughput(Throughput::Elements(num_tasks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
            |b, &n| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(n);
                        for i in 0..n {
                            let db = db.clone();
                            handles.push(tokio::spawn(async move {
                                let mut tx = db.start_mutation().await.unwrap();
                                tx.insert("col", sample_doc(i as u64)).await.unwrap();
                                tx.commit().await.unwrap();
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

// ─── 7. Concurrent contended writes – high OCC conflict rate ─────────────────
//
// N tasks all read and then patch the SAME document concurrently.  The first
// task to commit wins; every subsequent committer will detect a conflict
// (its read-set entry for the shared doc was invalidated by the winner).
//
// This benchmark stresses the CommitLog / OCC path and shows:
//  • how the conflict-detection overhead scales with N
//  • that conflicting tasks still terminate quickly (no locking, no blocking)

fn bench_concurrent_contended(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("concurrent/contended_writes");

    for num_tasks in [2usize, 4, 8, 16] {
        // Insert one shared document that all tasks will fight over.
        let (db, _dir, shared_id) = rt.block_on(async {
            let (db, dir) = open_db().await;
            let mut tx = db.start_mutation().await.unwrap();
            let id = tx.insert("col", sample_doc(0)).await.unwrap();
            tx.commit().await.unwrap();
            (db, dir, id)
        });

        group.throughput(Throughput::Elements(num_tasks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
            |b, &n| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(n);
                        for _ in 0..n {
                            let db = db.clone();
                            handles.push(tokio::spawn(async move {
                                let mut tx = db.start_mutation().await.unwrap();
                                // Reading the doc adds it to the read-set, which is
                                // what triggers OCC conflict detection at commit time.
                                let _ = tx.get("col", shared_id).await.unwrap();
                                tx.patch(
                                    "col",
                                    shared_id,
                                    PatchOp::MergePatch(serde_json::json!({"v": 1})),
                                )
                                .await
                                .unwrap();
                                // Returns Committed or Conflict – either way we don't panic.
                                tx.commit().await.unwrap()
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

// ─── 8. Mixed read / write workload ──────────────────────────────────────────
//
// Runs reader tasks and writer tasks concurrently.  Each iteration spawns
// `readers` tasks doing a point-get and `writers` tasks doing a fresh insert.
// Three mixes are benchmarked: read-heavy, balanced, and write-heavy.

fn bench_concurrent_mixed(c: &mut Criterion) {
    let rt = rt();
    let (db, _dir) = rt.block_on(async {
        let (db, dir) = open_db().await;
        bulk_insert(&db, 1_000).await;
        (db, dir)
    });

    // Collect doc IDs once; share them across all reader tasks via Arc.
    let ids: Arc<Vec<_>> = Arc::new(rt.block_on(async {
        let mut q = db
            .start_query(QueryType::Find, 0, QueryOptions::default())
            .await
            .unwrap();
        q.find_ids("col", None).await.unwrap()
    }));

    let mut group = c.benchmark_group("concurrent/mixed");

    for (label, readers, writers) in [("r8_w2", 8usize, 2usize), ("r4_w4", 4, 4), ("r2_w8", 2, 8)]
    {
        let total = readers + writers;
        group.throughput(Throughput::Elements(total as u64));
        group.bench_function(label, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(total);

                    // Reader tasks: point-get a document then commit (no subscribe).
                    for i in 0..readers {
                        let db = db.clone();
                        let ids = ids.clone();
                        handles.push(tokio::spawn(async move {
                            let id = ids[i % ids.len()];
                            let mut q = db
                                .start_query(QueryType::Find, 0, QueryOptions::default())
                                .await
                                .unwrap();
                            black_box(q.get("col", id).await.unwrap());
                            q.commit().await.unwrap();
                        }));
                    }

                    // Writer tasks: insert a new document.
                    for _ in 0..writers {
                        let db = db.clone();
                        handles.push(tokio::spawn(async move {
                            let mut tx = db.start_mutation().await.unwrap();
                            tx.insert("col", sample_doc(99999)).await.unwrap();
                            tx.commit().await.unwrap();
                        }));
                    }

                    for h in handles {
                        h.await.unwrap();
                    }
                })
            });
        });
    }
    group.finish();
}

// ─── Registration ─────────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_insert_batch,
    bench_read_point,
    bench_scan_full,
    bench_scan_indexed_vs_full,
    bench_concurrent_independent,
    bench_concurrent_contended,
    bench_concurrent_mixed,
);
criterion_main!(benches);
