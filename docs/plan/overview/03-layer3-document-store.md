# Layer 3: Document Store & Indexing

**Layer purpose:** Adds document/MVCC semantics on top of Layer 2 storage primitives. This is where domain concepts (DocId, timestamps, tombstones, version resolution) enter the system. The primary B-tree IS the document store — it is both storage and primary index (clustered). This layer uses Layer 2 `BTreeHandle` but adds all document-aware logic.

## Modules

### `key_encoding.rs` — Order-Preserving Key Encoding

**WHY HERE:** Converts domain-typed scalars into byte-comparable keys for B-tree storage. Requires knowledge of the type ordering (section 1.6) and encoding rules (section 3.4) — these are document/domain concepts that Layer 2 doesn't need.

```rust
use crate::core::types::{Scalar, TypeTag, DocId, Ts};

/// Encode a scalar value into an order-preserving byte representation
pub fn encode_scalar(scalar: &Scalar) -> Vec<u8>;

/// Decode an order-preserving byte representation back to a scalar
pub fn decode_scalar(data: &[u8]) -> Result<(Scalar, usize)>;  // returns (value, bytes_consumed)

/// Construct primary index key: doc_id[16] || inv_ts[8]
pub fn make_primary_key(doc_id: &DocId, ts: Ts) -> [u8; 24];

/// Parse a primary key back into components
pub fn parse_primary_key(key: &[u8]) -> Result<(DocId, Ts)>;

/// Construct secondary index key from typed scalars
pub fn make_secondary_key(values: &[Scalar], doc_id: &DocId, ts: Ts) -> Vec<u8>;

/// Construct secondary index key from pre-encoded prefix (used by IndexBuilder)
pub fn make_secondary_key_from_prefix(prefix: &[u8], doc_id: &DocId, ts: Ts) -> Vec<u8>;

/// Parse secondary key suffix to extract doc_id and ts
pub fn parse_secondary_key_suffix(key: &[u8]) -> Result<(DocId, Ts)>;

/// Encode prefix portion of secondary key (without doc_id/inv_ts suffix)
pub fn encode_key_prefix(values: &[Scalar]) -> Vec<u8>;

/// Compute inverted timestamp for descending sort within doc_id group
pub fn inv_ts(ts: Ts) -> u64 { u64::MAX - ts }

/// Smallest key strictly greater than input (append 0x00). For exclusive lower bounds.
pub fn prefix_successor(key: &[u8]) -> Vec<u8>;

/// Next key at same length (increment last byte). For exclusive upper bounds in prefix scans.
pub fn successor_key(key: &[u8]) -> Vec<u8>;
```

### `primary_index.rs` — Document Store (Clustered Primary B-Tree)

**WHY HERE:** Adds MVCC version semantics on top of the raw B-tree. Constructs `doc_id||inv_ts` keys, handles tombstones, makes inline-vs-heap decisions. This is where "insert a document" becomes "insert a versioned entry in a B-tree".

```rust
use crate::storage::BTreeHandle;
use crate::storage::HeapRef;
use crate::core::types::{DocId, Ts};

pub struct PrimaryIndex {
    btree: BTreeHandle,
    heap: Arc<Heap>,
    external_threshold: usize,
}

/// Cell flags stored in the B-tree value
pub struct CellFlags {
    pub tombstone: bool,
    pub external: bool,
}

impl PrimaryIndex {
    pub fn new(btree: BTreeHandle, heap: Arc<Heap>, external_threshold: usize) -> Self;

    /// Insert a new document version (used at commit time)
    /// Constructs key = doc_id || inv_ts(commit_ts)
    /// Value = flags[1] || body_len[4] || body (inline) or heap_ref (external)
    pub async fn insert_version(&self, doc_id: &DocId, commit_ts: Ts,
                          body: Option<&[u8]>) -> Result<()>;
    // body=None means tombstone (delete)

    /// Get the latest visible version of a document at read_ts
    /// Seeks to doc_id || inv_ts(read_ts), takes first entry with ts <= read_ts
    /// Returns None if tombstone or doc doesn't exist
    pub async fn get_at_ts(&self, doc_id: &DocId, read_ts: Ts) -> Result<Option<Vec<u8>>>;

    /// Get the latest visible version's timestamp (for verification)
    pub async fn get_version_ts(&self, doc_id: &DocId, read_ts: Ts) -> Result<Option<Ts>>;

    /// Scan all visible document versions at read_ts
    pub fn scan_at_ts(&self, read_ts: Ts, direction: ScanDirection) -> PrimaryScanner;

    /// Internal: decide inline vs external storage
    fn store_body(&self, body: &[u8]) -> Result<StoredBody>;
    fn load_body(&self, value: &[u8]) -> Result<Vec<u8>>;
}

enum StoredBody {
    Inline(Vec<u8>),
    External(HeapRef),
}

/// Stream over visible documents in primary index
pub struct PrimaryScanner { /* ... */ }
impl Stream for PrimaryScanner {
    type Item = Result<(DocId, Ts, Vec<u8>)>;  // (doc_id, version_ts, body)
}
```

### `secondary_index.rs` — Secondary Index with Version Resolution

**WHY HERE:** Adds MVCC-aware scanning to secondary index B-trees. Must resolve versions and skip stale entries — requires domain knowledge of timestamps and doc_ids.

```rust
use crate::storage::BTreeHandle;

pub struct SecondaryIndex {
    btree: BTreeHandle,
    primary: Arc<PrimaryIndex>,
}

impl SecondaryIndex {
    pub fn new(btree: BTreeHandle, primary: Arc<PrimaryIndex>) -> Self;

    /// Insert a secondary index entry for a document version.
    /// encoded_key is the full key (prefix + doc_id + inv_ts). Value is empty.
    pub async fn insert_entry(&self, encoded_key: &[u8]) -> Result<()>;

    /// Remove a secondary index entry.
    pub async fn remove_entry(&self, encoded_key: &[u8]) -> Result<bool>;

    /// Scan with version resolution (section 3.5)
    /// For each (doc_id, inv_ts):
    ///   - Skip if version_ts > read_ts
    ///   - Within same doc_id: take first (highest ts <= read_ts), skip rest
    ///   - Verify against primary index (skip stale entries)
    pub fn scan_at_ts(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>,
                      read_ts: Ts, direction: ScanDirection) -> SecondaryScanner;
}

/// Stream yielding verified (doc_id, version_ts) pairs from secondary index
pub struct SecondaryScanner { /* ... */ }
impl Stream for SecondaryScanner {
    type Item = Result<(DocId, Ts)>;
}
```

### `version_resolution.rs` — MVCC Version Resolution Logic

**WHY HERE:** Encapsulates the shared MVCC resolution algorithm used by both primary and secondary scans. Domain logic that requires understanding of timestamps and tombstones.

```rust
/// Given a stream of (doc_id, ts) pairs from a B-tree scan,
/// resolve to the latest visible version per doc_id at read_ts.
/// Direction-aware: handles both forward (descending ts) and
/// backward (ascending ts) entry ordering within doc_id groups.
pub struct VersionResolver {
    read_ts: Ts,
    direction: ScanDirection,
    current_doc_id: Option<DocId>,
    found_visible: bool,       // forward mode
    best_visible_ts: Option<Ts>, // backward mode
}

impl VersionResolver {
    pub fn new(read_ts: Ts, direction: ScanDirection) -> Self;

    /// Process next entry. Returns verdict indicating what the caller should do.
    pub fn process(&mut self, doc_id: &DocId, ts: Ts) -> Verdict;

    /// Call after the last entry. In backward mode, returns the final group's
    /// visible (doc_id, ts) if any.
    pub fn finish(&mut self) -> Option<(DocId, Ts)>;

    /// Check if a version is visible at read_ts
    pub fn is_visible(ts: Ts, read_ts: Ts) -> bool { ts <= read_ts }
}

pub enum Verdict {
    Visible,                       // yield this entry (forward mode)
    Skip,                          // skip this entry
    EmitPrevious(DocId, Ts),       // yield the previous doc_id group's result (backward mode)
}
```

### `array_indexing.rs` — Array Index Entry Expansion

**WHY HERE:** Handles the domain-specific rule that array fields produce one index entry per element. Requires knowledge of document structure and scalar extraction.

```rust
use crate::core::types::{Scalar, FieldPath, DocId, Ts};
use crate::core::encoding::extract_scalars;

/// For a document, compute all secondary index entries for a given index definition
/// Handles single fields, compound fields, and array expansion
/// Returns: Vec of (encoded_key_prefix) — caller appends doc_id || inv_ts
pub fn compute_index_entries(
    doc: &serde_json::Value,
    field_paths: &[FieldPath],
) -> Result<Vec<Vec<u8>>>;
// Validates: at most one array field in compound index
```

### `index_builder.rs` — Background Index Building

**WHY HERE:** Scans the primary B-tree at a snapshot timestamp and inserts entries into a Building secondary index. Requires MVCC snapshot semantics.

```rust
pub struct IndexBuilder {
    primary: Arc<PrimaryIndex>,
    secondary: Arc<SecondaryIndex>,
    field_paths: Vec<FieldPath>,
}

impl IndexBuilder {
    pub fn new(primary: Arc<PrimaryIndex>, secondary: Arc<SecondaryIndex>,
               field_paths: Vec<FieldPath>) -> Self;

    /// Run background build: scan primary at build_snapshot_ts,
    /// insert entries into secondary index.
    /// Returns the number of entries inserted.
    pub async fn build(&self, build_snapshot_ts: Ts) -> Result<u64>;
}
```

## Vacuum Strategy: WAL-Driven Candidate Tracking

Instead of periodically scanning the entire primary B-tree to find reclaimable old versions (O(total entries)), use the **write path as a source of vacuum candidates**:

1. **On each commit**: when a `TxCommit` contains a Replace or Delete mutation, the *previous* version at `(doc_id, old_ts)` becomes a future vacuum candidate. Push it (along with its old secondary index keys from the IndexDelta) into a **pending vacuum queue**.
2. **Periodically**: compute the vacuum-safe threshold:
   ```
   vacuum_safe_ts = min(oldest_active_read_ts, visible_ts)
   ```
   where `oldest_active_read_ts` comes from L5 (TxManager) and `visible_ts` is the latest replicated timestamp (see Layer 5, Visibility Fence). `visible_ts` is the latest replicated timestamp. Un-replicated commits (`ts > visible_ts`) may have replaced versions that readers at `visible_ts` still need. Using `min()` ensures we never vacuum versions that are still the "latest visible" at either the oldest active reader's snapshot or the visibility fence.
3. **Drain eligible entries**: any pending entry where `superseding_ts <= vacuum_safe_ts` is safe to remove. This means the superseding version is visible to all current readers, so no reader can still need the old version.
4. **Execute**: write a `Vacuum` WAL record (0x08) with the entries, then call `btree().delete()` on each primary and secondary key to perform the actual B-tree key deletions.

**Advantages over B-tree scan**:
- Work proportional to write rate, not database size — a 100 GB database with 1 write/sec does minimal vacuum work
- Candidates arrive incrementally from the commit path — no separate scan phase
- Lower latency to reclaim old versions

**Queue persistence**: the pending queue is rebuilt on startup by replaying the WAL from the last checkpoint. Each `TxCommit` with Replace/Delete re-populates candidates; each `Vacuum` record removes them. Alternatively, the queue can be a simple in-memory structure (rebuilt from WAL on recovery).

**Fallback**: a full B-tree scan can still be used as a safety net (e.g., on startup or periodically at low frequency) to catch any candidates missed by the incremental approach.

### Rollback Vacuum (Un-replicated Commit Cleanup)

This is a specialized vacuum that removes entries written by commits that were never replicated (ts > visible_ts). It is the ONLY undo mechanism in the system.

**When triggered:**
- **Live rollback**: When the primary loses quorum during `replicate_and_wait()`, the current transaction's write set is still in memory. The commit coordinator can immediately undo it by deleting the exact keys that were just materialized (step 4 of the commit protocol). No WAL scan needed — the write set + index deltas provide exact keys.
- **Startup cleanup**: On recovery, if any commits exist with `ts > visible_ts` (from the FileHeader), they must be removed. This uses the WAL to identify exactly what to undo.

**WAL-driven startup cleanup (no full scan):**
1. After WAL replay completes, check: does the latest committed ts in memory exceed `visible_ts` from FileHeader?
2. If yes, scan WAL records where `commit_ts > visible_ts`:
   - Each `TxCommit` record contains the collection_id, doc_id, commit_ts, and index deltas
   - For each: delete primary key `doc_id || inv_ts(commit_ts)` from the primary B-tree
   - For each index delta with `new_key`: delete secondary key from the secondary B-tree
   - For each index delta with `old_key = None` (was an insert, no previous version): nothing else needed
   - For each index delta with `old_key = Some(...)` (was a replace/delete): the previous version is still in the B-tree and becomes "current" again — no action needed
3. Write a `WAL_RECORD_ROLLBACK_VACUUM` record listing the rolled-back transactions
4. The pending vacuum queue should also discard any candidates from rolled-back commits

**Efficiency:**
- Live rollback: O(write_set_size) — deletes exactly the keys from the failed commit, using in-memory write set
- Startup cleanup: O(WAL_records_after_visible_ts) — reads only the WAL tail, no B-tree scan
- Both are proportional to the amount of un-replicated work, not database size

**Correctness:**
- Un-replicated data was NEVER visible to any reader (visible_ts fence prevented it)
- Removing it is semantically equivalent to the commit never happening
- After rollback vacuum, the database state is identical to what replicas see

```rust
/// Unit struct — all methods are stateless, indexes passed as parameters.
/// This avoids L3 depending on L5 types (WriteSet, IndexDelta).
pub struct RollbackVacuum;

impl RollbackVacuum {
    /// Live rollback: undo a single failed commit.
    /// L5 decomposes its WriteSet into plain (CollectionId, DocId) tuples
    /// and IndexDelta into (IndexId, Option<Vec<u8>>) before calling this.
    pub async fn rollback_commit(
        commit_ts: Ts,
        mutations: &[(CollectionId, DocId)],
        index_deltas: &[(IndexId, Option<Vec<u8>>)],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<()>;

    /// Startup cleanup: undo all commits with ts > visible_ts.
    /// L5/L6 parses WAL records into WalCommitInfo before calling this.
    pub async fn rollback_from_wal(
        visible_ts: Ts,
        wal_commits: &[WalCommitInfo],
        primary_indexes: &HashMap<CollectionId, Arc<PrimaryIndex>>,
        secondary_indexes: &HashMap<IndexId, Arc<SecondaryIndex>>,
    ) -> Result<u64>;
}
```

## Interfaces Exposed to Higher Layers

| Interface | Used By | Purpose |
|-----------|---------|---------|
| `PrimaryIndex::insert_version` | L5 (commit) | Apply mutations |
| `PrimaryIndex::get_at_ts` | L4 (point read) | Document retrieval |
| `PrimaryIndex::scan_at_ts` | L4 (table scan) | Full collection iteration |
| `SecondaryIndex::scan_at_ts` | L4 (index scan) | Range queries with MVCC |
| `SecondaryIndex::insert_entry/remove_entry` | L5 (commit, vacuum) | Index maintenance |
| `make_primary_key`, `make_secondary_key` | L5 (read set, conflict detection) | Key encoding for intervals |
| `compute_index_entries` | L5 (index delta computation) | Index key generation |
| `IndexBuilder::build` | Integration (background task) | Index backfill |
| `key_encoding::encode_scalar` | L4 (range encoder) | Query range to byte interval |
| `RollbackVacuum::rollback_commit` | L5 (commit coordinator) | Live rollback of failed commit |
| `RollbackVacuum::rollback_from_wal` | L6 (startup/recovery) | Startup cleanup of un-replicated commits |
