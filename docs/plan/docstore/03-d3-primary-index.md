# D3: Primary Index

## Purpose

Document store built on top of the clustered primary B-tree. Adds MVCC version semantics: constructs `doc_id || inv_ts` keys, handles tombstones, makes inline-vs-heap storage decisions. This is where "insert a document" becomes "insert a versioned entry in a B-tree".

## Dependencies

- **D1 (Key Encoding)**: `make_primary_key`, `parse_primary_key`, `inv_ts`
- **D2 (Version Resolution)**: `VersionResolver`, `Verdict`
- **L2 (Storage Engine)**: `BTreeHandle`, `ScanStream`, `ScanDirection`, `Heap`, `HeapRef`
- **L1 (Core Types)**: `DocId`, `Ts`

## Rust Types

```rust
use crate::storage::{BTreeHandle, Heap, HeapRef, ScanDirection, ScanStream};
use crate::core::types::{DocId, Ts};
use crate::docstore::key_encoding::{make_primary_key, parse_primary_key, inv_ts};
use crate::docstore::version_resolution::{VersionResolver, Verdict};
use std::sync::Arc;

/// Cell flags stored in the first byte of B-tree values.
/// bit 0: tombstone (1 = deleted)
/// bit 1: external (1 = body stored in heap)
#[derive(Debug, Clone, Copy)]
pub struct CellFlags {
    pub tombstone: bool,
    pub external: bool,
}

impl CellFlags {
    pub fn to_byte(self) -> u8;
    pub fn from_byte(b: u8) -> Self;
}

/// The clustered primary B-tree wrapper with MVCC semantics.
pub struct PrimaryIndex {
    btree: BTreeHandle,
    heap: Arc<Heap>,
    external_threshold: usize,
}

impl PrimaryIndex {
    /// Create a new PrimaryIndex wrapping a B-tree and heap.
    /// `external_threshold`: body sizes above this are stored in the heap.
    /// Default: page_size / 2 (e.g., 4096 for 8KB pages).
    pub fn new(btree: BTreeHandle, heap: Arc<Heap>, external_threshold: usize) -> Self;

    /// Insert a new document version at commit_ts.
    /// key = doc_id || inv_ts(commit_ts)
    /// value = flags[1] || body_len[4] || body (inline) or heap_ref (external)
    /// body = None means tombstone (delete).
    pub async fn insert_version(
        &self,
        doc_id: &DocId,
        commit_ts: Ts,
        body: Option<&[u8]>,
    ) -> std::io::Result<()>;

    /// Get the latest visible version of a document at read_ts.
    /// Seeks to doc_id || inv_ts(read_ts), takes first entry with ts <= read_ts.
    /// Returns None if tombstone or doc doesn't exist.
    pub async fn get_at_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Vec<u8>>>;

    /// Get the latest visible version's timestamp (for secondary index verification).
    /// Returns None if doc doesn't exist or is a tombstone at read_ts.
    pub async fn get_version_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Ts>>;

    /// Scan all visible documents at read_ts.
    /// Returns a stream yielding (doc_id, version_ts, body) for each visible
    /// non-tombstone document.
    pub fn scan_at_ts(&self, read_ts: Ts, direction: ScanDirection) -> PrimaryScanStream;

    /// Access the underlying B-tree handle (for vacuum and index builder).
    pub fn btree(&self) -> &BTreeHandle;

    /// Access the heap (for vacuum cleanup of external bodies).
    pub fn heap(&self) -> &Arc<Heap>;
}

/// Stream over visible documents in the primary index.
pub struct PrimaryScanStream {
    inner: ScanStream,
    resolver: VersionResolver,
    heap: Arc<Heap>,
}

impl Stream for PrimaryScanStream {
    /// (doc_id, version_ts, body_bytes)
    type Item = Result<(DocId, Ts, Vec<u8>)>;
}
```

## Internal Types

```rust
/// How a document body is stored in the B-tree value.
enum StoredBody {
    Inline(Vec<u8>),
    External(HeapRef),
}
```

## Cell Value Format

The B-tree value for each entry follows this layout:

```
┌────────────────────────────────┐
│ flags: u8                      │  bit 0: tombstone, bit 1: external
├────────────────────────────────┤
│ [if not tombstone]:            │
│   body_len: u32 LE             │  total body size (original, uncompressed)
│   [if inline]:                 │
│     body: [u8; body_len]       │  BSON-encoded document
│   [if external]:               │
│     heap_page_id: u32 LE       │
│     heap_slot_id: u16 LE       │
└────────────────────────────────┘
```

- **Tombstone**: flags byte only (1 byte total). No body_len, no body.
- **Inline**: `1 + 4 + body_len` bytes.
- **External**: `1 + 4 + 6 = 11` bytes (body stored in heap).

## Implementation Details

### insert_version()

1. Construct key: `make_primary_key(doc_id, commit_ts)`.
2. If `body` is None (tombstone):
   - value = `[CellFlags { tombstone: true, external: false }.to_byte()]`
3. If `body` is Some(data):
   - If `data.len() <= external_threshold`:
     - value = `[flags_byte, body_len_le_bytes..., data...]`
   - Else (external):
     - Store in heap: `let href = self.heap.store(data).await?`
     - value = `[flags_byte, body_len_le_bytes..., href.page_id_le, href.slot_id_le]`
4. Call `self.btree.insert(&key, &value).await`.

### get_at_ts()

1. Construct seek key: `make_primary_key(doc_id, read_ts)`.
   - Due to inv_ts, this sorts just before the first version with `ts <= read_ts`.
2. Start a scan from `Included(seek_key)` forward.
3. For each entry:
   - Parse key → `(entry_doc_id, entry_ts)`.
   - If `entry_doc_id != doc_id`: return None (doc doesn't exist at read_ts).
   - If `entry_ts > read_ts`: continue (version too new — shouldn't happen with correct seek, but defensive).
   - Parse flags from value.
   - If tombstone: return None.
   - Load body (inline or external via heap).
   - Return `Some(body)`.

**Optimization**: Since the seek key is `doc_id || inv_ts(read_ts)`, and inv_ts is `u64::MAX - ts`, seeking to this key positions us at or just after the first version with `ts <= read_ts`. The first matching entry with the same doc_id is our answer.

### get_version_ts()

Same as `get_at_ts()` but returns `Some(ts)` instead of the body. Used by secondary index verification (DESIGN.md section 3.5 step 4).

### scan_at_ts()

1. Create a B-tree scan with `Unbounded` bounds (or bounded for specific doc_id ranges).
2. Wrap in `PrimaryScanStream` with a `VersionResolver` pinned at `read_ts`.
3. The stream's `poll_next()` implementation handles all three verdict variants:

   **Forward mode** (`Visible` / `Skip`):
   - Read next entry from inner B-tree scan.
   - Parse key -> `(doc_id, ts)`.
   - Feed to `resolver.process(doc_id, ts)`.
   - If `Skip`: continue.
   - If `Visible`: parse flags. If tombstone: continue. Else: load body, yield `(doc_id, ts, body)`.

   **Backward mode** (`EmitPrevious` / `Skip` / `finish()`):
   - Read next entry, parse key, feed to `resolver.process()`.
   - If `Skip`: continue.
   - If `EmitPrevious(prev_id, prev_ts)`: look up the entry at `(prev_id, prev_ts)` via a B-tree point read. Parse flags. If tombstone: continue. Else: load body, yield `(prev_id, prev_ts, body)`.
   - On end of stream: call `resolver.finish()`. If `Some((id, ts))`: same point-read + tombstone check + yield logic.

   Note: in backward mode, the resolver only gives us `(doc_id, ts)` — we need to re-read the B-tree value at `make_primary_key(doc_id, ts)` to get the body. This is an extra point lookup per yielded document, but backward scans are uncommon and typically bounded by LIMIT.

### load_body() (internal)

1. Parse flags byte from value[0].
2. If tombstone: error (caller should have checked).
3. Read body_len from value[1..5].
4. If external flag set:
   - Read heap_page_id from value[5..9], heap_slot_id from value[9..11].
   - `self.heap.load(HeapRef { page_id, slot_id }).await`
5. Else (inline):
   - Copy `value[5..5+body_len]`.

## Document Existence Semantics

Layer 3 is a **dumb versioned key-value store**. It does NOT enforce document-level invariants like "document must exist before replace" or "cannot delete a non-existent document". It simply stores versioned entries keyed by `(doc_id, ts)`.

**Who enforces existence?** Layer 5/6 (Transactions / Database):
- **Replace of non-existent doc**: L5 calls `get_at_ts(doc_id, read_ts)` before commit. If None, the mutation is rejected with a "document not found" error.
- **Delete of non-existent doc**: same check — L5 verifies the doc exists at the transaction's read_ts before writing a tombstone.
- **Re-deletion** (deleting an already-deleted doc): L5 sees None from `get_at_ts` (tombstone returns None) and rejects.
- **Duplicate insert**: ULIDs are unique by construction, so two inserts with the same doc_id are astronomically unlikely. If it somehow happened, L5's OCC conflict detection would catch the concurrent write to the same doc_id.
- **Insert over tombstone**: This is valid — it creates a new version after the tombstone. L5 treats this as an insert of a "new" document (since `get_at_ts` returns None for tombstones).

**Why this split?** L3 has no concept of transactions, read sets, or write sets. Existence checks require reading at a snapshot timestamp and coordinating with the commit protocol — that's L5's domain. L3 just provides the building block: `get_at_ts` tells L5 what's visible.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| B-tree I/O error | L2 backend failure | Propagate |
| Heap load error | Corrupt or missing heap page | Propagate |
| Invalid cell format | Corrupt value bytes | Return error |
| Key parse error | Corrupt or truncated key | Return error |

## Tests

1. **Insert and get**: insert a document at ts=1, get_at_ts(read_ts=1) returns it.
2. **Get at future ts**: insert at ts=1, get_at_ts(read_ts=10) returns it.
3. **Get before insert**: insert at ts=5, get_at_ts(read_ts=3) returns None.
4. **Multiple versions**: insert at ts=1, insert at ts=5, insert at ts=10.
   - get_at_ts(read_ts=3) returns ts=1 version.
   - get_at_ts(read_ts=7) returns ts=5 version.
   - get_at_ts(read_ts=15) returns ts=10 version.
5. **Tombstone**: insert at ts=1, delete (tombstone) at ts=5.
   - get_at_ts(read_ts=3) returns doc.
   - get_at_ts(read_ts=7) returns None.
6. **Tombstone then reinsert**: insert ts=1, delete ts=5, insert ts=10.
   - get_at_ts(read_ts=7) returns None.
   - get_at_ts(read_ts=12) returns ts=10 version.
7. **get_version_ts**: insert at ts=5, get_version_ts(read_ts=10) returns Some(5).
8. **get_version_ts tombstone**: insert ts=1, delete ts=5, get_version_ts(read_ts=7) returns None.
9. **Inline storage**: insert small document (< threshold), verify body retrieved correctly.
10. **External storage**: insert large document (> threshold), verify body retrieved correctly from heap.
11. **Scan all visible**: insert 3 docs at various ts, scan_at_ts returns all visible non-tombstone docs.
12. **Scan skips tombstones**: insert doc, delete it, scan doesn't yield it.
13. **Scan with multiple versions**: insert doc at ts=1 and ts=5, scan at read_ts=10 yields only ts=5 version.
14. **Scan direction**: verify forward and backward scans return correct doc order.
15. **CellFlags roundtrip**: verify to_byte and from_byte are inverses for all flag combinations.
16. **Multiple documents**: insert 100 documents, verify each retrievable by get_at_ts.
17. **Empty index**: get_at_ts on empty index returns None. scan_at_ts on empty yields nothing.
18. **Non-existent doc_id**: get_at_ts with doc_id not in index returns None.
