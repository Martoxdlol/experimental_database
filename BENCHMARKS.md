# Benchmark Results

**Machine**: MacBook Air M4 — 24 GB RAM, 512 GB SSD
**Profile**: `cargo bench` (release, optimised)
**Date**: 2026-03-02

---

## Insert throughput

| Benchmark | Time | Docs/s |
|---|---|---|
| `insert/sequential` | 28.5 µs | ~35 k/s |
| `insert/batch/10` | 99.3 µs | ~101 k/s (9.9 µs/doc) |
| `insert/batch/100` | 519 µs | ~193 k/s (5.2 µs/doc) |
| `insert/batch/1000` | 3.65 ms | ~274 k/s (3.7 µs/doc) |

Batching 1000 docs into one transaction is **7.8× faster** than one-per-tx. The sequential cost is dominated by per-commit overhead: allocating a monotonic timestamp, building and appending the WAL `Begin + Commit` records to the async mpsc channel, and running OCC validation — all paid once per batch instead of once per doc.

The flat per-doc cost of ~3–5 µs across batch sizes is the actual BTreeMap insert + WAL `PutDoc` record cost.

---

## Read throughput

| Benchmark | Time | Throughput |
|---|---|---|
| `read/point` | 1.1 µs | ~909 k reads/s |

A point `get()` is extremely cheap: catalog HashMap lookup → `get_collection` → one BTreeMap range scan for the key prefix. No locking on the happy path, no allocation.

---

## Scan scaling

| Collection size | Time | Scan rate |
|---|---|---|
| 100 docs | 16.4 µs | ~6.1 M docs/s |
| 1 000 docs | 151.5 µs | ~6.6 M docs/s |
| 10 000 docs | 1.47 ms | ~6.8 M docs/s |

Perfectly linear — ~150 ns per document. Cost is the MVCC visibility check per BTreeMap entry (decode key, compare `inv_ts` against `start_ts`, decode value bytes).

---

## Index scan vs full scan (Eq filter, 10 k docs)

| Strategy | Time | vs full scan |
|---|---|---|
| `full_scan_eq` (table scan + post-filter) | 15.0 ms | 1× |
| `index_scan_eq` (B-tree index, direct lookup) | 4.4 µs | **3 400×** faster |

The full scan with an Eq filter is **10× slower than the raw full scan** (15 ms vs 1.47 ms). The extra cost is deserialising the JSON of every document to extract the target field — `serde_json` parse overhead adds up fast across 10 k docs.

The index scan is nearly as fast as a single point read (4.4 µs vs 1.1 µs) because it jumps straight to the matching entries in the secondary B-tree.

---

## Concurrent independent writes

No two tasks touch the same document; OCC conflicts cannot occur.

| Tasks | Time | Total tx/s | Per-task time |
|---|---|---|---|
| 4 | 108.5 µs | ~37 k/s | 27.1 µs |
| 8 | 139.6 µs | ~57 k/s | 17.5 µs |
| 16 | 282.7 µs | ~57 k/s | 17.7 µs |
| 32 | 492.3 µs | ~65 k/s | 15.4 µs |

With 8+ concurrent tasks throughput is roughly **1.6–1.9× better** than sequential. The parallelism is real but limited — the WAL channel (async mpsc) and the `parking_lot::RwLock` on each collection's BTreeMap both serialise at some point. After 8–16 tasks the throughput plateaus.

---

## Concurrent contended writes (OCC conflicts)

All N tasks read and then patch the same document. Only the first committer wins; the rest abort via OCC conflict detection.

| Tasks | Time | vs independent (same N) |
|---|---|---|
| 2 | 92.5 µs | — |
| 4 | 147.2 µs | +36% |
| 8 | 195.8 µs | +40% |
| 16 | 317.0 µs | +12% |

At N=16 only 1 out of 16 tasks commits, yet the overhead is only **+12% over the no-conflict case**. This is the key property of OCC: conflicting transactions fail cheaply — no locks held, no blocking, just a CommitLog scan and a WAL `Abort` record. The gap narrows at high N because the aborting tasks become negligible compared to the wall-clock time dominated by the single winning commit.

---

## Mixed read / write

| Mix | Time | Total ops |
|---|---|---|
| r8_w2 (read-heavy) | 71.7 µs | 10 |
| r4_w4 (balanced) | 116.8 µs | 8 |
| r2_w8 (write-heavy) | 166.1 µs | 10 |

Reads and writes run concurrently without blocking each other (MVCC snapshot isolation: readers never wait for writers). The r8_w2 mix is fast because 8 reads complete in ~1 µs each while only 2 write paths hit the WAL. The r2_w8 time (166 µs) is close to `independent_writes/8` (140 µs) — the 2 readers are essentially free passengers.

---

## Summary

| Finding | Detail |
|---|---|
| Batch writes | batch-1000 is 7.8× faster than single-insert; fixed per-tx cost dominates for small writes |
| Reads are nearly free | 1.1 µs point read, well below any network RTT |
| Index or suffer | missing index turns a 4 µs query into a 15 ms one (3 400×) |
| OCC conflict cost is low | contended vs independent overhead is only 12–40%, not catastrophic |
| Write concurrency saturates ~8–16 tasks | WAL mpsc channel + storage lock are the bottleneck, not CPU |
| Scan rate is ~6.8 M docs/s | ~150 ns/doc, linear in collection size |
