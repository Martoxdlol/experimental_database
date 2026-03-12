# Async Migration Audit

Exhaustive inventory of all synchronous primitives, blocking I/O, and patterns that need async counterparts across the entire codebase.

---

## Executive Summary

The codebase is **synchronous-first** with a thin async layer around WAL writes and checkpoint. Making the database fully async requires changes at every layer, bottoming out at L2's buffer pool and file I/O. The main blockers are:

1. **`PageStorage` and `WalStorage` are sync traits** (`backend.rs:26, 56`) — these define the I/O contract for the entire engine. All 4 implementations (File + Memory × Page + WAL) are sync. Converting these to async traits is the foundational change.
2. **Buffer pool page guards** (`SharedPageGuard`, `ExclusivePageGuard`) hold `parking_lot::RwLock` guards — these are `!Send` and cannot cross `.await` points.
3. **`Iterator` trait is synchronous** — `ScanIterator`, `WalIterator`, `PrimaryScanner`, `SecondaryScanner`, and `QueryScanner` all implement `Iterator`, which has no async equivalent in std.
4. **All L2 B-tree and heap operations are sync** — every read/write/scan goes through `BufferPool::fetch_page_*()` which does blocking file I/O on cache miss.
5. **Sync-in-async anti-pattern** — WAL `process_batch()` calls blocking `storage.append()` + `storage.sync()` inside a `tokio::spawn` task (`wal.rs:183-238`).
6. **Mixed sync/async API surface** — `StorageEngine` has 3 async methods (`close`, `append_wal`, `checkpoint`) and ~15 sync methods. This needs a unified approach.

---

## Layer 1: Core Types (`exdb-core`) — NO CHANGES NEEDED

All modules are **pure functions and data types**. No I/O, no locks, no sync primitives.

| File | Status | Notes |
|------|--------|-------|
| `types.rs` | Pure | Enums/structs only |
| `field_path.rs` | Pure | `Vec<String>` wrapper |
| `encoding.rs` | Pure | JSON encode/decode, scalar extraction |
| `ulid.rs` | Pure* | `SystemTime::now()` — not blocking I/O |
| `filter.rs` | Pure | AST types only |

---

## Layer 2: Storage Engine (`exdb-storage`) — MOST WORK HERE

### Sync I/O Trait Definitions (CRITICAL — the I/O contract)

These two traits define the I/O boundary for the entire storage engine. Every page read, page write, WAL append, and fsync flows through them. Converting these to async traits is the single most impactful change.

**`PageStorage`** (`backend.rs:26-51`)
```rust
pub trait PageStorage: Send + Sync {
    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()>;
    fn write_page(&self, page_id: PageId, buf: &[u8]) -> io::Result<()>;
    fn sync(&self) -> io::Result<()>;
    fn page_count(&self) -> u64;
    fn extend(&self, new_count: u64) -> io::Result<()>;
    fn page_size(&self) -> usize;
    fn is_durable(&self) -> bool;
}
```
- Implementations: `FilePageStorage` (`backend.rs:108`), `MemoryPageStorage` (`backend.rs:569`)
- Every buffer pool cache miss calls `read_page()` → blocking `pread`
- Every flush calls `write_page()` → blocking `pwrite`

**`WalStorage`** (`backend.rs:56-85`)
```rust
pub trait WalStorage: Send + Sync {
    fn append(&self, data: &[u8]) -> io::Result<u64>;
    fn sync(&self) -> io::Result<()>;
    fn read_from(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn truncate_before(&self, offset: u64) -> io::Result<()>;
    fn oldest_lsn(&self) -> Option<u64>;
    fn total_size(&self) -> u64;
}
```
- Implementations: `FileWalStorage` (`backend.rs:233`), `MemoryWalStorage` (`backend.rs:643`)
- Called from `process_batch()` inside `tokio::spawn` — sync I/O on async runtime thread

### Synchronous Locks

| Type | Location | Protected Resource | Hot Path? |
|------|----------|-------------------|-----------|
| `parking_lot::Mutex` | `engine.rs:270` | `Arc<Mutex<FreeList>>` — free page allocator | **Yes** — every B-tree insert/delete |
| `parking_lot::Mutex` | `engine.rs:273` | `Mutex<Heap>` — heap free space map | **Yes** — blob store/free |
| `parking_lot::Mutex` | `engine.rs:278` | `Mutex<FileHeader>` — database metadata | No — init/checkpoint |
| `parking_lot::Mutex` | `backend.rs:239` | `Mutex<FileWalInner>` — active WAL segment | **Yes** — all WAL appends |
| `parking_lot::RwLock` | `buffer_pool.rs:103` | `RwLock<HashMap<PageId, FrameId>>` — page table | **Yes** — every page fetch |
| `parking_lot::RwLock` | `buffer_pool.rs:87` | `RwLock<FrameData>` (per frame) — page data + dirty flag | **Yes** — every page access |
| `parking_lot::RwLock` | `backend.rs:571` | `RwLock<Vec<Vec<u8>>>` — MemoryPageStorage | Test only |
| `parking_lot::RwLock` | `backend.rs:645` | `RwLock<MemoryWalInner>` — MemoryWalStorage | Test only |

### Lock Guards Returned from Functions (CRITICAL — `!Send`)

**`SharedPageGuard<'a>`** (`buffer_pool.rs:517-545`)
```rust
pub struct SharedPageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockReadGuard<'a, FrameData>,  // !Send
}
```
- Returned from: `fetch_page_shared()` (line 145), `new_page()` (line 304)
- Cannot cross `.await` points
- Drop impl: releases lock, decrements pin_count atomically

**`ExclusivePageGuard<'a>`** (`buffer_pool.rs:547-593`)
```rust
pub struct ExclusivePageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
    guard: parking_lot::RwLockWriteGuard<'a, FrameData>,  // !Send
    modified: bool,
}
```
- Returned from: `fetch_page_exclusive()` (line 229), `new_page()` (line 304)
- Drop impl: sets dirty flag if modified, releases lock, decrements pin_count

### Blocking File I/O

**Hot Path (every read/write):**

| Operation | File | Line | Syscall | Notes |
|-----------|------|------|---------|-------|
| Page read | `backend.rs` | 151 | `pread` (via `read_at`) | Concurrent, no lock |
| Page write | `backend.rs` | 164 | `pwrite` (via `write_at`) | Concurrent, no lock |
| Page sync | `backend.rs` | 169 | `fsync` (via `sync_data`) | Blocks until durable |
| WAL append | `backend.rs` | 432 | `write_all` (buffered) | Inside Mutex |
| WAL sync | `backend.rs` | 451 | `fsync` (via `sync_data`) | Per batch (group commit) |

**Cold Path (startup, checkpoint, recovery):**

| Operation | File | Line | Syscall | Notes |
|-----------|------|------|---------|-------|
| File open | `backend.rs` | 114, 127 | `open` | Database init |
| File extend | `backend.rs` | 182-183 | `ftruncate` + `fsync` | Page allocation |
| WAL dir scan | `backend.rs` | 248 | `readdir` | Startup |
| WAL segment create | `backend.rs` | 313-318 | `open` + `write` + `fsync` | Rollover |
| WAL segment delete | `backend.rs` | 537 | `unlink` | Retention |
| DWB write | `dwb.rs` | 119-144 | `open` + `write_all` + `fsync` | Checkpoint |
| DWB scatter-write | `dwb.rs` | 148 | `pwrite` per page | Checkpoint |
| DWB storage sync | `dwb.rs` | 152 | `fsync` | Checkpoint |
| DWB truncate | `dwb.rs` | 155-158 | `ftruncate` + `fsync` | Checkpoint |
| Recovery DWB | `recovery.rs` | 335-417 | `open` + `read` + `pwrite` + `fsync` | Startup |
| Recovery WAL | `recovery.rs` | 154-254 | `pread` per frame | Startup |
| Engine dir create | `engine.rs` | 300-302 | `mkdir` | First open |
| Page 0 flush | `engine.rs` | 800 | `flush_page` → `pwrite` | Shutdown |
| Final sync | `engine.rs` | 471 | `fsync` | Shutdown |

### Atomic Operations (Non-blocking, keep as-is)

| Atomic | File | Line | Purpose |
|--------|------|------|---------|
| `AtomicU32` (pin_count) | `buffer_pool.rs` | 91 | Frame pin counting |
| `AtomicBool` (ref_bit) | `buffer_pool.rs` | 95 | Clock eviction algorithm |
| `AtomicU32` (clock_hand) | `buffer_pool.rs` | 107 | Eviction position |
| `AtomicU64` (page_count) | `backend.rs` | 108 | File page count |
| `AtomicU32` (root_page) | `btree.rs` | 209 | B-tree root page pointer |
| `AtomicU64` (current_lsn) | `wal.rs` | 135 | WAL sequence number |

### L2 Vacuum Module (`vacuum.rs`)

**`VacuumTask::remove_entries()`** (`vacuum.rs:35`) — Blocking B-tree deletions:
- Opens B-trees via `BTree::open()` (line 57) — buffer pool fetch
- Calls `btree.delete()` per key (line 59) — blocking I/O per deletion
- Takes `&mut FreeList` directly (not behind Mutex) — caller must hold the lock
- Cold path, but blocks if called from async context

### Sync Iterators (blocking `next()`)

**`ScanIterator`** (`btree.rs:928+`)
```rust
pub struct ScanIterator {
    buffer_pool: Arc<BufferPool>,  // Arc, not guard
    current_page: Option<PageId>,
    // ...
}
```
- Each `next()` call: acquires page guard → reads → releases guard
- Does NOT hold guard across `next()` calls — safe pattern
- But `next()` itself does blocking I/O (buffer pool fetch)

**`WalIterator`** (`wal.rs:382-394`)
```rust
pub struct WalIterator {
    storage: Arc<dyn WalStorage>,
    current_lsn: Lsn,
}
```
- `next()` calls `storage.read_from()` (line 400, 428) — blocking I/O per iteration step
- Used in recovery (`recovery.rs`) and `StorageEngine::read_wal_from()` (line 534)
- Cold path (startup/recovery/replication), but still blocking on async runtime if called from async context

### Sync-in-Async Anti-patterns

**1. WAL `process_batch()`** (`wal.rs:183-238`) — The WAL background task (`tokio::spawn`) calls `process_batch()` which is a **sync function doing blocking I/O on the tokio runtime thread**:
```rust
// Inside tokio::spawn task:
let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    Self::process_batch(&storage, batch, &current_lsn);  // SYNC!
}));
```
- Line 234: `storage.append(&buf)` — blocking WAL write (can be `write_all` + buffered I/O)
- Line 238: `storage.sync()` — blocking `fsync`
- These block the tokio worker thread during every WAL batch

**2. `Checkpoint::run()`** (`checkpoint.rs:63`) — Declared `async fn` but performs blocking I/O before the only true `.await`:
- Line 68: `self.buffer_pool.dirty_pages()` — acquires RwLock on page table + per-frame RwLocks (sync)
- Line 78: `dwb.write_pages(&pages_for_dwb)?` — blocking file I/O (`pwrite` + `fsync` via DWB)
- Line 83: `self.buffer_pool.mark_clean(...)` — acquires per-frame RwLock (sync)
- Only line 89: `self.wal_writer.append(...).await` is truly async
- Net effect: most of the checkpoint work blocks the async task before the single `.await`

### Mixed Sync/Async API: `StorageEngine`

`StorageEngine` has an inconsistent API surface:
- **Async** (3 methods): `close()` (line 450), `append_wal()` (line 529), `checkpoint()` (line 541)
- **Sync** (~15 methods): `get/insert/delete/scan` (lines 224-241), `heap_store/load/free` (lines 507-520), `create_btree/open_btree` (lines 484-495), `vacuum` (line 554), etc.
- **Leaks internal Mutex**: `free_list()` (line 598) returns `&Arc<Mutex<FreeList>>` — callers (Studio) lock it directly

### Recovery Module (entirely sync)

**`Recovery::run()`** (`recovery.rs:98`) — The crash recovery coordinator is fully synchronous, doing extensive blocking I/O:
- Takes `&dyn PageStorage` + `&dyn WalStorage` (sync traits)
- `dwb_recover()` (line 319): opens DWB file, reads pages, calls `page_storage.read_page()` + `write_page()` + `sync()` — all blocking
- `replay_wal()` (line 154): loops calling `wal_storage.read_from()` — blocking per-frame reads
- `scan_forward_for_valid_frame()` (line 258): byte-by-byte WAL scan — blocking reads in tight loop

**`WalRecordHandler` trait** (`recovery.rs:63`) — sync callback for WAL replay:
```rust
pub trait WalRecordHandler {
    fn handle_record(&mut self, record: &WalRecord) -> io::Result<()>;
}
```
- Higher layers implement this for domain-specific recovery
- Would need `async fn handle_record()` if recovery becomes async
- Cold path (startup only), but blocks the async runtime if called from async context

**`Heap` not Arc-wrapped** (`engine.rs:273`) — `heap: Mutex<Heap>` (no `Arc`), unlike `free_list: Arc<Mutex<FreeList>>`. This limits sharing across async tasks. Would need `Arc<Mutex<Heap>>` or `Arc<tokio::sync::Mutex<Heap>>` for async.

### `eprintln!` in production code

Two production `eprintln!` calls that bypass any logging/tracing infrastructure:
- `wal.rs:188` — WAL writer panic recovery message
- `recovery.rs:420` — DWB recovery corrupt page warning

In async context, structured logging (`tracing`) is needed for correlating log entries across concurrent tasks. These should migrate to `tracing::warn!`/`tracing::error!`.

### Background Tasks

| Task | File | Line | Runtime |
|------|------|------|---------|
| WAL writer | `wal.rs` | 152 | `tokio::spawn` — async (but calls sync I/O inside) |
| No other threads | — | — | — |

### Channels

| Channel | File | Line | Type |
|---------|------|------|------|
| WAL request | `wal.rs` | 133 | `tokio::sync::mpsc` (async, bounded 1024) |
| WAL response | `wal.rs` | 91, 97 | `tokio::sync::oneshot` (async) |

---

## Layer 3: Document Store (`exdb-docstore`) — ZERO OWN LOCKS

The docstore has **no locks of its own**. All synchronization is inherited from L2 calls. The issue is that every public method calls blocking L2 operations.

### Synchronous L2 Calls (Hot Path)

| Method | File | Line | L2 Call | Blocking? |
|--------|------|------|---------|-----------|
| `PrimaryIndex::insert_version()` | `primary_index.rs` | 97 | `engine.heap_store()` | Yes |
| `PrimaryIndex::insert_version()` | `primary_index.rs` | 110 | `btree.insert()` | Yes |
| `PrimaryIndex::get_at_ts()` | `primary_index.rs` | 119-123 | `btree.scan()` | Yes |
| `PrimaryIndex::get_version_ts()` | `primary_index.rs` | 150-154 | `btree.scan()` | Yes |
| `PrimaryIndex::scan_at_ts()` | `primary_index.rs` | 177 | `btree.scan()` | Yes |
| `PrimaryIndex::load_body()` | `primary_index.rs` | 207 | `engine.heap_load()` | Yes |
| `SecondaryIndex::insert_entry()` | `secondary_index.rs` | 34 | `btree.insert()` | Yes |
| `SecondaryIndex::remove_entry()` | `secondary_index.rs` | 39 | `btree.delete()` | Yes |
| `SecondaryIndex::scan_at_ts()` | `secondary_index.rs` | 52 | `btree.scan()` | Yes |

### Synchronous L2 Calls (Cold Path)

| Method | File | Line | L2 Call |
|--------|------|------|---------|
| `VacuumCoordinator::execute()` | `vacuum.rs` | 83, 88, 92, 100 | `btree.get/delete`, `heap_free`, `remove_entry` |
| `RollbackVacuum::rollback_commit()` | `vacuum.rs` | 159, 166 | `btree.delete`, `remove_entry` |
| `IndexBuilder::build()` | `index_builder.rs` | 55, 71 | `scan_at_ts`, `insert_entry` |

### Iterators/Scanners (CRITICAL — `!Send`, blocking `next()`)

**`PrimaryScanner<'a>`** (`primary_index.rs:216-222`)
```rust
pub struct PrimaryScanner<'a> {
    inner: ScanIterator,         // L2 B-tree iterator
    resolver: VersionResolver,
    engine: Arc<StorageEngine>,
    btree: &'a BTreeHandle,      // Borrowed — makes struct !Send
    finished: bool,
}
```
- `next()` calls: `btree.get()`, `engine.heap_load()` — blocking I/O
- **`!Send`** due to `&'a BTreeHandle`

**`SecondaryScanner`** (`secondary_index.rs:69-75`)
```rust
pub struct SecondaryScanner {
    inner: ScanIterator,
    resolver: VersionResolver,
    primary: Arc<PrimaryIndex>,
    read_ts: Ts,
    finished: bool,
}
```
- `next()` calls: `primary.get_version_ts()` → blocking I/O
- Technically `Send` (all Arc/owned), but `next()` blocks

### Channels

| Channel | File | Line | Type |
|---------|------|------|------|
| `tokio::sync::watch::Sender<BuildProgress>` | `index_builder.rs` | 52 | Progress reporting during index build |

### Async Function Wrapping Sync I/O (Anti-pattern)

**`IndexBuilder::build()`** (`index_builder.rs:55`)
- Declared `async fn` but iterates `PrimaryScanner` in a loop with blocking I/O
- `tokio::task::yield_now().await` at line 83 does not help — the sync I/O blocks before yield

---

## Layer 4: Query Engine (`exdb-query`) — ITERATOR TRAIT BLOCKER

### Pure Modules (No Changes Needed)

| Module | Status |
|--------|--------|
| `post_filter.rs` | Pure — no I/O |
| `range_encoder.rs` | Pure — no I/O |
| `access.rs` | Pure — no I/O |

### Modules with Sync I/O

**`scan.rs`** — The main blocker

| Item | Line | Issue |
|------|------|-------|
| `execute_scan()` returns `std::io::Result<QueryScanner>` | 56 | Sync error type (fine for async too) |
| `QueryScanner` implements `Iterator` | 185 | **Sync trait — cannot `.await` in `next()`** |
| `primary_index.get_at_ts()` inside `next()` | 216 | Blocking I/O per iteration step |
| `sec_index.scan_at_ts()` | 106, 135 | Returns sync `SecondaryScanner` |

**`merge.rs`** — Consumes sync iterators

| Item | Line | Issue |
|------|------|-------|
| `merge_with_writes()` takes `I: Iterator<Item = io::Result<ScanRow>>` | 38 | Bound to sync `Iterator` |
| Collects all results with `item?` in loop | 46-47 | Blocking consumption |

---

## Applications

### Studio (`apps/studio/`)

| Pattern | File | Line | Notes |
|---------|------|------|-------|
| `DbHandle::open()` | `db_handle.rs` | 106 | Sync — wrapped in `spawn_blocking` by caller |
| `DbHandle::close()` | `db_handle.rs` | 118 | Async (delegates to `engine.close().await`) |
| `FreeList.lock()` | `db_handle.rs` | 432, 437 | `parking_lot::Mutex` — blocking |
| All B-tree/heap/scan ops | `db_handle.rs` | 156-932 | Sync — ~80 methods returning `io::Result` |
| `spawn_blocking` in UI modules | `components/toolbar.rs`, `modules/storage/*.rs`, `modules/docstore/*.rs` | Various | Wraps all sync `DbHandle` calls |

### Server/CLI (`apps/server/`, `apps/cli/`)

Empty stubs — not yet implemented.

---

## Migration Plan: What Must Change

### Tier 1: Foundation (L2 — must change first)

| Component | Current | Async Replacement | Difficulty |
|-----------|---------|-------------------|------------|
| **`PageStorage` trait** | 7 sync methods (`backend.rs:26`) | `async fn` or `async-trait` | **Hard** — 2 impls + all callers |
| **`WalStorage` trait** | 6 sync methods (`backend.rs:56`) | `async fn` or `async-trait` | **Hard** — 2 impls + all callers |
| `BufferPool::fetch_page_shared()` | Returns `SharedPageGuard` with `RwLockReadGuard` | `tokio::sync::RwLock` or redesign with page handle | **Hard** — ripples everywhere |
| `BufferPool::fetch_page_exclusive()` | Returns `ExclusivePageGuard` with `RwLockWriteGuard` | Same as above | **Hard** |
| `BTreeHandle::get/insert/delete` | Sync, holds page guards | Async methods | **Hard** — guard lifetime management |
| `BTreeHandle::scan()` | Returns `ScanIterator` (sync `Iterator`) | Return `AsyncIterator` / `Stream` | **Hard** |
| `StorageEngine` API | 3 async + ~15 sync methods | Unify: all async | **Hard** — largest public API change |
| `StorageEngine::heap_store/load/free` | Sync | Async methods | **Medium** |
| WAL `process_batch()` | Sync I/O inside `tokio::spawn` | Use async `WalStorage` methods | **Medium** — depends on async WalStorage |
| `Checkpoint::run()` | Async fn calling sync DWB + buffer pool ops | Await async DWB + buffer pool methods | **Medium** — depends on async PageStorage |
| `WalIterator` | Sync `Iterator` with blocking reads | `Stream` | **Medium** — cold path |
| `Recovery::run()` | Fully sync, blocking file + WAL I/O | Async fn, async `WalRecordHandler` trait | **Medium** — cold path, but blocks async runtime |
| `FreeList` Mutex | `parking_lot::Mutex` | `tokio::sync::Mutex` | **Easy** |
| `Heap` Mutex | `parking_lot::Mutex` (not Arc-wrapped) | `Arc<tokio::sync::Mutex>` | **Easy** |
| `FileHeader` Mutex | `parking_lot::Mutex` | `tokio::sync::Mutex` | **Easy** |
| `FileWalInner` Mutex | `parking_lot::Mutex` | `tokio::sync::Mutex` | **Easy** |
| `free_list()` accessor | Leaks `&Arc<Mutex<FreeList>>` | Encapsulate behind async API | **Easy** |

### Tier 2: Document Store (L3 — follows L2)

| Component | Change |
|-----------|--------|
| `PrimaryIndex::get_at_ts()` | Async — awaits B-tree scan |
| `PrimaryIndex::insert_version()` | Async — awaits heap_store + btree insert |
| `PrimaryIndex::scan_at_ts()` | Return async `Stream` instead of `PrimaryScanner` |
| `PrimaryScanner` | Replace `Iterator` with `Stream` |
| `SecondaryIndex::scan_at_ts()` | Return async `Stream` |
| `SecondaryScanner` | Replace `Iterator` with `Stream` |
| `SecondaryIndex::insert_entry/remove_entry` | Async |
| `VacuumCoordinator::execute()` | Async |
| `IndexBuilder::build()` | Already async but needs truly async L2 underneath |

### Tier 1.5: L2 Vacuum (follows L2 foundation)

| Component | Change |
|-----------|--------|
| `VacuumTask::remove_entries()` | Async — awaits btree deletes |

### Tier 3: Query Engine (L4 — follows L3)

| Component | Change |
|-----------|--------|
| `execute_scan()` | Async — awaits L3 scanner creation |
| `QueryScanner` | Replace `Iterator` with `Stream` |
| `merge_with_writes()` | Accept `Stream` instead of `Iterator` |

### Tier 4: Applications

| Component | Change |
|-----------|--------|
| `DbHandle` methods | Convert from sync to async (remove `spawn_blocking` wrappers) |
| Studio UI modules | Call async methods directly instead of `spawn_blocking` |

---

## Key Design Decisions

### 1. Buffer Pool Lock Strategy

**Option A: `tokio::sync::RwLock`**
- Page guards become `Send` — can cross `.await`
- But: tokio RwLock is ~10x slower than parking_lot for uncontended access
- Hot path regression for single-threaded workloads

**Option B: Keep `parking_lot`, wrap in `spawn_blocking`**
- No lock changes needed
- But: every page access crosses thread boundary (overhead)
- Doesn't scale — thread pool becomes bottleneck

**Option C: Redesign page access with owned handles**
- `fetch_page()` returns owned `PageRef` that pins the frame
- No lock guard in the returned type — lock is held only during copy/pin
- Best for async but largest code change

### 2. Iterator → Stream

Replace all `Iterator<Item = io::Result<T>>` with:
```rust
use futures::Stream;
use std::pin::Pin;

type ScanStream<T> = Pin<Box<dyn Stream<Item = io::Result<T>> + Send>>;
```

Or use `async_stream` crate for ergonomic async iterators.

### 3. Storage Trait Async Strategy

The `PageStorage` and `WalStorage` traits are the I/O boundary. Three approaches:

**Option A: Native `async fn` in trait (Rust 1.75+)**
```rust
pub trait PageStorage: Send + Sync {
    async fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()>;
    // ...
}
```
- Clean, no external crate needed
- But: `async fn` in traits does not auto-impl `Send` for the returned future unless using `#[trait_variant::make(Send)]` or `-> impl Future + Send`

**Option B: `async-trait` crate**
- Erases to `Pin<Box<dyn Future>>` — heap allocation per call
- Proven pattern, easy migration
- Performance cost on hot path (every page fetch)

**Option C: Manual `-> impl Future` with `Send` bound**
- No boxing, best performance
- Verbose but zero-overhead

### 4. File I/O Strategy

**Option A: `tokio::fs`** — wraps sync I/O in `spawn_blocking` internally
**Option B: `io_uring`** (Linux) — true async I/O, best throughput
**Option C: Keep `pread/pwrite` in `spawn_blocking`** — simplest migration

---

## Inventory Totals

| Category | Count |
|----------|-------|
| **Sync I/O trait definitions** | **2** (`PageStorage`, `WalStorage`) — 4 implementations |
| Sync Mutexes (production) | 4 |
| Sync RwLocks (production) | 2 + N (per buffer pool frame) |
| Lock guards returned from functions | 2 types (`SharedPageGuard`, `ExclusivePageGuard`) |
| Blocking file I/O call sites | 27+ (hot path: 5, cold path: 22+) |
| `fsync` call sites | 11 |
| Sync `Iterator` impls needing `Stream` | **5** (`ScanIterator`, `WalIterator`, `PrimaryScanner`, `SecondaryScanner`, `QueryScanner`) |
| Sync-in-async anti-patterns | 3 (`process_batch` in WAL, `Checkpoint::run`, `IndexBuilder::build`) |
| Sync callback traits needing async | 1 (`WalRecordHandler`) |
| Mixed sync/async API surfaces | 1 (`StorageEngine`: 3 async + ~15 sync methods) |
| Fully sync cold-path modules | 1 (`Recovery`: all blocking I/O) |
| Leaked internal sync primitives | 1 (`engine.rs:598` exposes `&Arc<Mutex<FreeList>>`) |
| Non-Arc sync primitives | 1 (`Heap` Mutex not Arc-wrapped — limits async sharing) |
| Sync L2 calls from L3 | 20 call sites (12 hot-path, 8 cold-path) |
| Sync L3 calls from L4 | 6 call sites |
| Functions returning `std::io::Result` | 80+ across all layers |
| Production `eprintln!` calls | 2 (`wal.rs:188`, `recovery.rs:420`) — need `tracing` |
| Background async tasks | 1 (WAL writer) |
| `spawn_blocking` usage | 16 call sites in Studio app |
