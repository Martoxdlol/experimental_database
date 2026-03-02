# Design Document: Embedded Rust JSON Document Store (MVCC, B-Tree Indexes, WAL, Subscriptions, Replication)

## 1. Objectives

Build an embedded database library in Rust (Tokio async) that provides:

- JSON document store with **many collections**
- CRUD-like operations:
  - `insert` (auto-generated `DocId`)
  - `get|patch|delete` by `DocId`
- MVCC with versioned documents:
  - each write creates a new version at a unique `commit_ts`
  - each query/mutation reads from a fixed `start_ts` snapshot
- B-tree indexes:
  - always index `id`
  - optional secondary indexes on JSON fields
  - indexes are MVCC-aware (include version info)
- Query engine:
  - index scan + filters + limit
  - table scan + filters + limit
  - filters: `eq, lt, gt, gte, lte, ne, AND, OR, IN, NOT`
- Serializable transactions:
  - query transactions: multiple reads/queries, contribute to read set
  - mutation transactions: multiple reads+writes, have read set + write set, read-own-writes
  - commit must detect conflicts with transactions committed after `start_ts`
- Subscriptions:
  - subscribed query registers read set
  - on commit of other writes: invalidate subscription when write set intersects read set
- Storage engine:
  - strong local consistency using **WAL + page cache**
  - commit is fast: write to memory + WAL, flush policy configurable
  - background page flushing and checkpointing
  - cannot assume full collection fits in memory
- Concurrency:
  - efficient multi-threading
- Replication:
  - single primary, multiple read replicas
  - replicas apply WAL stream and serve reads

---

## 2. Core Concepts and Types (Rust-Level Definitions)

### 2.1 Identifiers
```rust
pub type CollectionId = u64;
pub type IndexId = u64;
pub type TxId = u64;
pub type Ts = u64; // commit timestamp, monotonic on primary

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocId([u8; 16]); // fixed 16 bytes
```

**DocId generation (primary only):**
- Recommended: `DocId = (8 bytes: commit_ts or wall-clock micros) + (8 bytes: atomic counter)` to preserve locality.
- Alternative: random 128-bit.
- Must be unique on the primary; replicas never generate ids.

### 2.2 JSON Value Representation
Internally, store payload as bytes and parse only when needed for filtering/indexing.
- Payload storage: `Vec<u8>` containing UTF-8 JSON.
- For index keys and filter comparisons, decode only the indexed fields:
  - `serde_json::Value` is acceptable for MVP; later optimize with a faster parser.

### 2.3 Field Paths
```rust
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<String>); // e.g. ["user", "age"]
```
Parsing from `"user.age"` is done at API boundary.

---

## 3. Public Embedded API (Concrete Session Model)

### 3.1 Database Handle
```rust
pub struct Database {
  // shared internal state, background tasks, handles
}

pub struct DbConfig {
  pub data_dir: std::path::PathBuf,
  pub wal_dir: std::path::PathBuf,
  pub page_size: usize,           // e.g. 16 * 1024
  pub cache_capacity_bytes: usize,
  pub wal_fsync: WalFsyncPolicy,  // Always | GroupCommit { interval_ms } | Never (tests)
  pub replication: ReplicationConfig,
}

pub enum WalFsyncPolicy {
  Always,
  GroupCommit { interval_ms: u64, max_batch_bytes: usize },
  Never,
}
```

Open/close:
```rust
impl Database {
  pub async fn open(cfg: DbConfig) -> anyhow::Result<Self>;
  pub async fn shutdown(self) -> anyhow::Result<()>;
}
```

### 3.2 Collection Management
```rust
impl Database {
  pub async fn create_collection(&self, name: &str) -> anyhow::Result<()>;
  pub async fn delete_collection(&self, name: &str) -> anyhow::Result<()>;
}
```

### 3.3 Index Management (Online Build)
```rust
pub struct IndexSpec {
  pub field: FieldPath,
  pub unique: bool, // optional MVP; can be false-only initially
}

pub enum IndexState {
  Building { started_at_ts: Ts },
  Ready { ready_at_ts: Ts },
  Failed { message: String },
}

impl Database {
  pub async fn create_index(
    &self,
    collection: &str,
    spec: IndexSpec,
  ) -> anyhow::Result<IndexId>;

  pub async fn index_state(
    &self,
    collection: &str,
    index_id: IndexId,
  ) -> anyhow::Result<IndexState>;
}
```

Index build is performed by a background worker (Tokio task) without locking the whole DB (details in §9).

### 3.4 Queries and Subscriptions
```rust
pub enum QueryType {
  Find,     // returns documents
  FindIds,  // returns doc ids
}

pub struct QueryOptions {
  pub subscribe: bool,
  pub limit: Option<usize>,
}

pub struct QuerySession {
  start_ts: Ts,
  // accumulates read set
}

pub struct Subscription {
  pub id: u64,
  pub rx: tokio::sync::mpsc::Receiver<InvalidationEvent>,
}

pub struct InvalidationEvent {
  pub subscription_id: u64,
  pub invalidated_at_ts: Ts,
}
```

Filter AST:
```rust
pub enum JsonScalar {
  Null,
  Bool(bool),
  I64(i64),
  F64(f64),
  String(String),
}

pub enum Filter {
  Eq(FieldPath, JsonScalar),
  Ne(FieldPath, JsonScalar),
  Lt(FieldPath, JsonScalar),
  Lte(FieldPath, JsonScalar),
  Gt(FieldPath, JsonScalar),
  Gte(FieldPath, JsonScalar),
  In(FieldPath, Vec<JsonScalar>),
  And(Vec<Filter>),
  Or(Vec<Filter>),
  Not(Box<Filter>),
}
```

Query execution:
```rust
impl Database {
  pub async fn start_query(
    &self,
    query_type: QueryType,
    query_id: u64,
    opts: QueryOptions,
  ) -> anyhow::Result<QuerySession>;
}

impl QuerySession {
  pub async fn get(
    &mut self,
    collection: &str,
    id: DocId,
  ) -> anyhow::Result<Option<Vec<u8>>>;

  pub async fn find(
    &mut self,
    collection: &str,
    filter: Option<Filter>,
  ) -> anyhow::Result<Vec<Vec<u8>>>;

  pub async fn find_ids(
    &mut self,
    collection: &str,
    filter: Option<Filter>,
  ) -> anyhow::Result<Vec<DocId>>;

  pub async fn commit(
    self,
  ) -> anyhow::Result<QueryCommitOutcome>;
}

pub enum QueryCommitOutcome {
  NotSubscribed,
  Subscribed { subscription: Subscription },
  InvalidatedDuringRun, // indicates user should re-run query
}
```

### 3.5 Mutation Transactions
```rust
pub struct MutationSession {
  start_ts: Ts,
  // write set overlay + read set
}

pub enum PatchOp {
  // MVP: merge-patch style; can add RFC6902 later
  MergePatch(serde_json::Value),
}

impl Database {
  pub async fn start_mutation(&self) -> anyhow::Result<MutationSession>;
}

impl MutationSession {
  // reads (respect write overlay)
  pub async fn get(
    &mut self,
    collection: &str,
    id: DocId,
  ) -> anyhow::Result<Option<Vec<u8>>>;

  pub async fn find_ids(
    &mut self,
    collection: &str,
    filter: Option<Filter>,
    limit: Option<usize>,
  ) -> anyhow::Result<Vec<DocId>>;

  // writes
  pub async fn insert(
    &mut self,
    collection: &str,
    json: Vec<u8>,
  ) -> anyhow::Result<DocId>;

  pub async fn patch(
    &mut self,
    collection: &str,
    id: DocId,
    op: PatchOp,
  ) -> anyhow::Result<()>;

  pub async fn delete(
    &mut self,
    collection: &str,
    id: DocId,
  ) -> anyhow::Result<()>;

  pub async fn commit(self) -> anyhow::Result<MutationCommitOutcome>;
}

pub enum MutationCommitOutcome {
  Committed { commit_ts: Ts },
  Conflict, // user must retry explicitly
}
```

---

## 4. Storage Model (MVCC + Indexes)

### 4.1 MVCC Record Layout

Each logical document has versions. A version is defined by `(collection_id, doc_id, ts)`.

**Primary “document versions” B-tree**
- Key: `(doc_id, ts_desc)`
- Value: `DocValue { payload_ptr, is_tombstone }`

To support “latest version <= start_ts” efficiently, store ts in descending order within each doc_id. Concretely:

- B-tree key bytes: `doc_id(16) || inv_ts(8)`
- Where `inv_ts = u64::MAX - ts` so that larger `ts` sorts earlier.

**Visibility rule at snapshot `start_ts`:**
- Seek to key prefix `doc_id` with `inv_ts >= u64::MAX - start_ts`
- First entry for that `doc_id` with `ts <= start_ts` is the visible version.

Payload storage:
- Either inline in leaf (simple, larger leaf nodes)
- Or stored in a value log (“blob file”) and referenced via `payload_ptr`:
  - MVP: inline is acceptable for simplicity
  - If documents are large, value log reduces B-tree churn

### 4.2 Secondary Index B-trees (MVCC-aware)

For an index on field `f`:

- Key: `encoded(field_value) || doc_id(16) || inv_ts(8)`
- Value: empty or small pointer

Secondary index scan yields candidate `(doc_id, ts)` pairs. Correctness requires verifying snapshot visibility:
- After reading an index entry, do a primary lookup at `start_ts` for that `doc_id` and confirm:
  - visible version exists
  - visible version’s `ts` equals the index entry’s `ts`
  - visible version matches the filter (full evaluation)

This prevents returning older versions when a newer visible version exists.

### 4.3 Encoding JSON scalar values for index keys
Define a total order:

Type tags (1 byte):
- `0x00 = Null`
- `0x01 = Bool(false)`
- `0x02 = Bool(true)`
- `0x03 = I64`
- `0x04 = F64`
- `0x05 = String`

Encoding:
- `Null`: `[0x00]`
- `Bool`: `[0x01]` or `[0x02]`
- `I64`: `[0x03] || i64_to_be_ordered(v)`
  - Use signed-to-ordered transform: `((v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()`
- `F64`: `[0x04] || f64_to_be_ordered(v)`
  - Use IEEE sortable transform (sign-bit flip for positives, bitwise invert for negatives)
- `String`: `[0x05] || u32_be_len || bytes`
  - Or null-terminated; length-prefixed is safer.

Missing/non-scalar field handling (MVP):
- Not indexed if missing or not scalar (array/object).
- Filter semantics for missing: comparisons evaluate to false except `Ne` (define explicitly in filter evaluator).

---

## 5. Query Engine

### 5.1 Planner

Inputs:
- `collection_id`
- `filter: Option<Filter>`
- `limit`

Outputs:
- `Plan::PrimaryGet(doc_id)`
- `Plan::IndexScan { index_id, range, post_filter }`
- `Plan::TableScan { post_filter }`

Sargable predicate extraction (MVP):
- If filter contains `Eq(field, value)` on an indexed field → point/range on index.
- If `In(field, [...])` on indexed field → multiple point ranges merged.
- If range comparisons on indexed field (`Gt/Gte/Lt/Lte`) → a range.
- Otherwise table scan.

### 5.2 Executors

**PrimaryGet**
- direct primary lookup at `start_ts`

**IndexScan**
- iterate index entries in range
- for each candidate doc_id:
  - do primary lookup at `start_ts`
  - evaluate full filter on visible doc JSON
  - collect until `limit`

**TableScan**
- iterate primary B-tree by doc_id groups
- for each doc_id, pick visible version at `start_ts`
- evaluate filter
- collect until `limit`

Performance note:
- TableScan should avoid parsing JSON when filter is `None`.
- For filter evaluation, parse only once per candidate.

---

## 6. Transaction System (Serializable + MVCC)

### 6.1 Transaction state
```rust
pub struct TxContext {
  pub tx_id: TxId,
  pub start_ts: Ts,
  pub read_set: ReadSet,
  pub write_set: WriteSet,
}

pub struct WriteSet {
  // per collection/doc logical changes
  pub docs: std::collections::HashMap<(CollectionId, DocId), DocWrite>,
}

pub enum DocWrite {
  Insert { json: Vec<u8> },
  Update { json: Vec<u8> },     // full new document after patch applied
  Delete,
}
```

### 6.2 Read-your-own-writes
Read path inside a mutation:
1. Check `write_set.docs[(collection_id, doc_id)]`:
   - `Insert/Update` → return that JSON
   - `Delete` → None
2. Otherwise read from storage snapshot at `start_ts`.

### 6.3 Read set definition (for conflict detection + subscriptions)

A practical, implementable read set for MVP:
```rust
pub struct ReadSet {
  pub point_reads: Vec<PointRead>,
  pub index_range_reads: Vec<IndexRangeRead>,
  pub collection_scans: Vec<CollectionScanRead>,
}

pub struct PointRead {
  pub collection_id: CollectionId,
  pub doc_id: DocId,
  pub observed_ts: Option<Ts>, // None if not found
}

pub struct IndexRangeRead {
  pub collection_id: CollectionId,
  pub index_id: IndexId,
  pub range: KeyRange,
  pub start_ts: Ts,
}

pub struct CollectionScanRead {
  pub collection_id: CollectionId,
  pub start_ts: Ts,
}
```

Key range:
```rust
pub struct KeyRange {
  pub start: Vec<u8>, // inclusive
  pub end: Vec<u8>,   // exclusive
}
```

When to record:
- `get(id)` records `PointRead { observed_ts: visible_ts_or_none }`
- Index scan records `IndexRangeRead` for the scanned range(s)
- Table scan records `CollectionScanRead` (coarse but correct)

### 6.4 Write set summary (for validation + subscription invalidation)
At commit, derive:
```rust
pub struct WriteSummary {
  pub commit_ts: Ts,
  pub doc_writes: Vec<(CollectionId, DocId)>,
  pub index_key_writes: Vec<(CollectionId, IndexId, Vec<u8>)>, // optional MVP+
}
```

MVP invalidation can be done using only `doc_writes` + “collection touched” marker:
- Invalidate collection scans on any write in that collection
- Invalidate point reads when doc_id is written
- For index-range subscriptions, either:
  - MVP: invalidate on any write to that collection (coarser), or
  - MVP+: include `index_key_writes` for the indexes involved in committed writes

### 6.5 Serializable validation (OCC)

Commit process for a mutation:
1. Acquire `commit_ts` from primary allocator.
2. Validate that no conflicting commits occurred between `start_ts` and `commit_ts`.
3. If ok → append WAL commit batch → apply to in-memory pages → publish commit → return success.
4. If conflict → return `Conflict` (no partial changes applied).

**Validation rules (MVP, correct but conservative):**
- For each `PointRead`:
  - If observed existed at `observed_ts`:
    - conflict if the current visible version at `commit_ts-1` is not the same `observed_ts`
  - If observed not found:
    - conflict if any version for that doc_id was committed in `(start_ts, commit_ts]`
- For each `CollectionScanRead`:
  - conflict if the collection had any committed write in `(start_ts, commit_ts]`

This makes table scans serializable by treating them as reading the whole collection (higher abort rate).

**IndexRangeRead validation (MVP+):**
- conflict if any committed write inserted/deleted an index key within that range in `(start_ts, commit_ts]`

Implementing this efficiently requires a “conflict index” (§7.3).

Query transactions are serializable under the same rules, but instead of applying writes they only register subscriptions and/or return “invalidated during run”.

---

## 7. Subscriptions

### 7.1 Subscription registration
On `QuerySession::commit()` with `subscribe=true`:
- store:
  - query specification (optional, for user convenience; DB does not need to re-run itself)
  - `read_set`
  - `registered_at_ts` (the commit horizon at time of registration)
  - a channel sender

### 7.2 Invalidation logic
On each committed mutation:
- compute `WriteSummary`
- find affected subscriptions and send `InvalidationEvent`

Intersection rules (MVP):
- If subscription has `CollectionScanRead(collection_id)` and mutation writes any doc in that collection → invalidate
- If subscription has `PointRead(collection_id, doc_id)` and mutation writes that doc_id → invalidate

Intersection rules (MVP+):
- If subscription has `IndexRangeRead` and mutation’s `index_key_writes` contains keys in the range → invalidate

### 7.3 Data structures
```rust
pub struct SubscriptionRegistry {
  // collection_id -> subscriptions that did a collection scan
  pub scan_subs: dashmap::DashMap<CollectionId, Vec<SubId>>,

  // (collection_id, doc_id) -> subscriptions that point-read
  pub point_subs: dashmap::DashMap<(CollectionId, DocId), Vec<SubId>>,

  // optional: index_id -> subscriptions with key ranges
  pub index_range_subs: dashmap::DashMap<(CollectionId, IndexId), Vec<(KeyRange, SubId)>>,

  pub subs: dashmap::DashMap<SubId, SubscriptionEntry>,
}
```

---

## 8. Storage Engine (WAL + Page Cache + B-Tree)

### 8.1 WAL records (replication-friendly)
Use a binary WAL format with a framed record structure:
- `len(u32) | crc32(u32) | record_type(u8) | payload(bytes...)`

Record types:
```rust
pub enum WalRecord {
  Begin { tx_id: TxId, start_ts: Ts },
  PutDoc {
    tx_id: TxId,
    collection_id: CollectionId,
    doc_id: DocId,
    json: Vec<u8>,
  },
  DeleteDoc {
    tx_id: TxId,
    collection_id: CollectionId,
    doc_id: DocId,
  },
  Commit {
    tx_id: TxId,
    commit_ts: Ts,
  },
  Abort { tx_id: TxId },
  CatalogUpdate { /* create/drop collection, index metadata */ },
  Checkpoint { up_to_lsn: u64 },
}
```

On replay:
- apply committed txs only (Begin + ops + Commit)
- ignore aborted/incomplete txs

### 8.2 Commit pipeline
Implement a single WAL writer task to ensure ordered append and efficient group commit:

- Sessions submit `CommitBatch { records, reply_chan }` into an `mpsc` channel.
- WAL writer appends, fsyncs depending on policy, then replies.
- After WAL is durable, apply changes to in-memory B-trees and publish commit.

If you want “memory updated before WAL durable” for latency, you must handle crash consistency carefully. The simplest strong design is:
- append+fsync WAL first
- then apply to in-memory and mark dirty pages
- acknowledge

### 8.3 Page cache (buffer pool)
- Fixed page size, e.g. 16KB.
- `PageId = (FileId, page_no)`
- Cache uses `HashMap<PageId, Arc<PageFrame>>` with sharding.
- Each `PageFrame` has:
  - `RwLock<PageData>`
  - dirty flag + page_lsn
  - pin count

Eviction:
- Clock or LRU-ish approximation.
- Never evict pinned pages.

Flusher task:
- periodically scans dirty pages and writes to disk
- writes are done in `spawn_blocking`

### 8.4 B-tree module interfaces
```rust
pub trait BTree {
  type Key;
  type Value;

  fn get(&self, key: &Self::Key) -> anyhow::Result<Option<Self::Value>>;
  fn insert(&self, key: Self::Key, value: Self::Value) -> anyhow::Result<()>;
  fn delete(&self, key: &Self::Key) -> anyhow::Result<()>;

  fn cursor(&self, start: Option<&Self::Key>) -> anyhow::Result<BTreeCursor<Self::Key, Self::Value>>;
}
```

Internally, implement:
- page format for internal/leaf nodes
- split/merge
- latch coupling:
  - read: shared locks down the tree
  - write: exclusive locks during modifications

For MVP, a standard B-tree with careful locking is fine; optimize later if contention is high.

---

## 9. Online Index Build (No Global DB Lock)

### 9.1 Index state machine
Catalog entry:
```rust
pub struct IndexMeta {
  pub index_id: IndexId,
  pub collection_id: CollectionId,
  pub spec: IndexSpec,
  pub state: IndexState,
}
```

### 9.2 Build algorithm (snapshot + catch-up)
1. Write catalog `IndexState::Building { started_at_ts = current_committed_ts }` to WAL and apply.
2. Builder obtains `build_ts = started_at_ts`.
3. Snapshot scan at `build_ts`:
   - iterate all doc_ids in collection
   - for each doc_id, find visible version at `build_ts`
   - extract field value; if scalar, insert secondary index entry `(value, doc_id, ts)`
4. Catch-up:
   - replay WAL from the LSN/ts corresponding to `build_ts` forward, applying index updates for documents written after `build_ts`
5. Mark index `Ready { ready_at_ts = current_committed_ts }` in catalog.

During build:
- queries should not use the index until `Ready`.
- writes proceed normally; catch-up ensures completeness.

---

## 10. Replication (Single Primary, Read Replicas)

### 10.1 Primary responsibilities
- Assign `commit_ts`
- Persist WAL
- Stream WAL records to replicas

A simple replication interface:
- primary runs a `ReplicationServer` task that tails WAL segments and sends frames over TCP (or user-provided transport).
- Because you requested “embedded instantiation”, replication can be optional and enabled via config.

### 10.2 Replica responsibilities
- Receive WAL frames
- Append to local WAL
- Apply records in order to local storage engine
- Expose read-only Database handle with `applied_ts`

Replica reads:
- start query at `start_ts = applied_ts` (or earlier if requested)
- never accept mutations

### 10.3 Consistency
- primary: read-after-write consistency for committed transactions
- replicas: eventually consistent; can offer:
  - `wait_until_ts(ts)` to block until applied

---

## 11. Concurrency and Tokio Integration

### 11.1 Threading model
- Tokio runtime handles async API and coordination.
- Blocking IO (disk) runs via `tokio::task::spawn_blocking`.
- CPU-heavy scans can use `spawn_blocking` or rayon-style parallelism; keep it optional.

### 11.2 Locks and contention avoidance
- Catalog: `RwLock`
- Collection handles: cached in a concurrent map with per-collection lock
- B-tree pages: per-page latches
- WAL writer: single task serializes appends
- Timestamp allocator: `AtomicU64`

### 11.3 Large number of collections
- Lazy open B-tree files
- File handle cache with LRU eviction
- Catalog stores mapping name -> CollectionId and file ids

---

## 12. Module Layout (Each Component as a Rust Module)

Suggested crate structure:

- `src/lib.rs`
- `src/db/mod.rs` (Database wiring, config, startup/shutdown)
- `src/api/mod.rs` (public sessions/types if you want separation)
- `src/catalog/mod.rs` (collections, index metadata, catalog WAL updates)
- `src/doc/mod.rs` (JSON parsing, field extraction, patch application)
- `src/filter/mod.rs` (Filter AST, evaluator)
- `src/query/mod.rs` (planner, executors, result shaping)
- `src/mvcc/mod.rs` (Ts management, visibility)
- `src/tx/mod.rs` (TxContext, read/write sets, validation)
- `src/index/mod.rs` (index specs, key encoding, index manager, builder)
- `src/storage/mod.rs`
  - `src/storage/wal/mod.rs`
  - `src/storage/page/mod.rs`
  - `src/storage/buffer_pool/mod.rs`
  - `src/storage/btree/mod.rs`
  - `src/storage/checkpoint/mod.rs`
- `src/subs/mod.rs` (subscription registry + invalidation)
- `src/replication/mod.rs` (primary streamer + replica applier)
- `src/error/mod.rs` (error enums)

---

## 13. Minimal Correctness Guarantees (Explicit)

- Snapshot reads: a query/mutation reads a consistent snapshot at `start_ts`.
- Read-your-own-writes: within a mutation, reads reflect its buffered writes.
- Serializable commits:
  - mutation commit validates its reads against intervening commits
  - on conflict, mutation aborts and returns `Conflict`
- Subscriptions:
  - read sets are registered
  - invalidation is triggered when a relevant write commits
  - user re-runs queries manually after notification

---

## 14. Implementation Milestones (Concrete Build Order)

1. **WAL + recovery**
   - append, fsync policy, replay committed txs
2. **Single B-tree + page cache**
   - get/insert/delete, cursor scan
3. **Catalog**
   - create/delete collection
4. **Primary MVCC store**
   - insert/get/delete with version keys `(doc_id, inv_ts)`
5. **Transactions (mutation)**
   - start_ts snapshot, write overlay, commit with conservative validation (point reads + collection scan)
6. **Query engine**
   - table scan + filters + limit
7. **Secondary indexes**
   - create index, use for `Eq/In/range`, still verify via primary lookup
8. **Online index build**
   - snapshot build + WAL catch-up, index ready state
9. **Subscriptions**
   - register read sets, invalidate on commit
10. **Replication**
   - WAL streaming and replica apply; read-only mode

This plan produces a working system early, then improves precision/performance (index-range conflicts, better invalidation granularity) without changing the public API.

---

If you want the next step, specify one design choice that affects many modules: **patch semantics** (RFC 6902 JSON Patch vs merge-patch vs “set/unset” ops). Once that’s fixed, the WAL record payloads and the write-set representation can be finalized without churn.