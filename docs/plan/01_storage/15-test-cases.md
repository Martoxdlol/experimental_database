# 15 — Comprehensive Test Cases

Organized by module. Each test includes: name, setup, action, assertion.

---

## T1: Slotted Page (`page.rs`)

### T1.1 — Init and header
- **Setup**: 8 KB buffer.
- **Action**: `SlottedPageMut::init(PageId(1), BTreeLeaf)`.
- **Assert**: header fields correct, num_slots = 0, free_space = page_size - HEADER_SIZE.

### T1.2 — Insert single cell
- **Setup**: Init page.
- **Action**: `insert_cell(0, b"hello")`.
- **Assert**: num_slots = 1, cell_data(0) = b"hello", free_space reduced by 5 + SLOT_ENTRY_SIZE.

### T1.3 — Insert multiple cells in order
- **Setup**: Init page.
- **Action**: insert cells at slots 0, 1, 2 with data "aaa", "bbb", "ccc".
- **Assert**: cell_data(0)="aaa", cell_data(1)="bbb", cell_data(2)="ccc".

### T1.4 — Insert cell at middle position
- **Setup**: Page with 3 cells.
- **Action**: `insert_cell(1, b"xxx")`.
- **Assert**: slot 0 unchanged, slot 1 = "xxx", slot 2 = old slot 1, slot 3 = old slot 2.

### T1.5 — Delete cell
- **Setup**: Page with 3 cells.
- **Action**: `delete_cell(1)`.
- **Assert**: num_slots = 2, slot 0 unchanged, slot 1 = old slot 2.

### T1.6 — Page full detection
- **Setup**: Init page.
- **Action**: Insert cells until `insert_cell` returns `Err(PageFull)`.
- **Assert**: error contains correct needed/available bytes.

### T1.7 — Compaction recovers fragmented space
- **Setup**: Insert 10 cells, delete cells 2, 5, 8 (creates gaps).
- **Action**: `compact()`, then insert a cell that previously wouldn't fit.
- **Assert**: insert succeeds after compaction.

### T1.8 — Checksum round-trip
- **Setup**: Page with cells.
- **Action**: `compute_checksum()`, then `verify_checksum()`.
- **Assert**: returns true.

### T1.9 — Checksum detects corruption
- **Setup**: Page with valid checksum.
- **Action**: Flip one byte in cell data.
- **Assert**: `verify_checksum()` returns false.

### T1.10 — Update cell (same size)
- **Setup**: Page with cell "hello".
- **Action**: `update_cell(0, b"world")`.
- **Assert**: cell_data(0) = "world", free_space unchanged.

### T1.11 — Update cell (larger)
- **Setup**: Page with small cell.
- **Action**: `update_cell(0, &large_data)`.
- **Assert**: cell_data(0) = large_data, free_space reduced.

### T1.12 — prev_or_ptr read/write
- **Setup**: Init page.
- **Action**: `set_prev_or_ptr(42)`.
- **Assert**: `header().prev_or_ptr == 42`.

### T1.13 — LSN read/write
- **Setup**: Init page.
- **Action**: `set_lsn(Lsn(12345))`.
- **Assert**: `header().lsn == Lsn(12345)`.

---

## T2: Key Encoding (`key_encoding.rs`)

### T2.1 — Int64 sort order
- **Action**: Encode i64::MIN, -1, 0, 1, i64::MAX.
- **Assert**: encoded bytes are in ascending order (memcmp).

### T2.2 — Float64 sort order
- **Action**: Encode -f64::INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY, f64::NAN.
- **Assert**: encoded bytes ascending. NaN sorts last.

### T2.3 — Float64 NaN canonicalization
- **Action**: Encode multiple NaN bit patterns.
- **Assert**: all produce identical encoded bytes.

### T2.4 — Boolean sort order
- **Assert**: encode(false) < encode(true).

### T2.5 — String sort order
- **Assert**: encode("") < encode("a") < encode("aa") < encode("b").

### T2.6 — String with null bytes
- **Action**: Encode "a\x00b".
- **Assert**: round-trip decode produces "a\x00b". Encodes as "a" 0x00 0xFF "b" 0x00 0x00.

### T2.7 — Bytes encoding
- **Action**: Encode &[0x00, 0xFF, 0x00].
- **Assert**: round-trip decode matches. Null bytes escaped.

### T2.8 — Cross-type ordering
- **Action**: Encode null, int64(0), float64(0.0), false, "a", bytes([0]).
- **Assert**: encoded bytes in ascending order matching §1.6.

### T2.9 — Primary key encode/decode
- **Action**: encode_primary_key(doc_id, ts), then decode_primary_key.
- **Assert**: round-trip matches.

### T2.10 — Primary key sort order
- **Action**: Same doc_id, ts=10 and ts=20.
- **Assert**: encode(doc_id, ts=20) < encode(doc_id, ts=10) (inverted ts → newer first).

### T2.11 — Compound key encoding
- **Action**: encode_compound_key(&[Int64(1), String("hello")]).
- **Assert**: round-trip decode matches both values.

### T2.12 — Compound key prefix ordering
- **Assert**: encode([Int64(1), String("a")]) < encode([Int64(1), String("b")]) < encode([Int64(2), String("a")]).

### T2.13 — Secondary key with doc_id suffix
- **Action**: encode_secondary_key(&[String("active")], doc_id, ts).
- **Assert**: decode_secondary_key_suffix extracts correct doc_id and ts.

### T2.14 — successor_prefix
- **Action**: prefix = encode_scalar(Int64(5)). succ = successor_prefix(&prefix).
- **Assert**: encode([Int64(5), String("zzz")]) < succ. encode([Int64(6), anything]) >= succ.

### T2.15 — Undefined encoding
- **Assert**: encode(Undefined) produces single byte 0x00.
- **Assert**: Undefined < Null < Int64(i64::MIN).

### T2.16 — Array encoding
- **Action**: Encode Array([Int64(1), String("a")]).
- **Assert**: round-trip decode matches. Tag 0x07, elements, terminator 0x00 0x00.

### T2.17 — extract_field_value from BSON
- **Setup**: BSON doc `{ "user": { "email": "a@b.com" }, "count": 42 }`.
- **Assert**: extract(["user", "email"]) = String("a@b.com").
- **Assert**: extract(["missing"]) = Undefined.

---

## T3: Buffer Pool (`buffer_pool.rs`)

### T3.1 — Fetch page shared (cache miss → disk read)
- **Setup**: MemPageIO with one pre-written page.
- **Action**: `fetch_page_shared(PageId(1))`.
- **Assert**: guard.data() matches the pre-written page.

### T3.2 — Fetch page shared (cache hit)
- **Action**: fetch_page_shared twice.
- **Assert**: second fetch doesn't read from disk (MemPageIO read count = 1).

### T3.3 — Multiple shared guards on same page
- **Action**: fetch_page_shared twice without dropping first guard.
- **Assert**: both guards are valid, data matches.

### T3.4 — Exclusive guard blocks shared
- **Action**: fetch_page_exclusive(PageId(1)). In another thread, try fetch_page_shared(PageId(1)).
- **Assert**: shared blocks until exclusive is dropped.

### T3.5 — Exclusive guard marks dirty
- **Action**: fetch_page_exclusive, call data_mut(), drop guard.
- **Assert**: frame is marked dirty.

### T3.6 — Clock eviction of clean frame
- **Setup**: BufferPool with 2 frames.
- **Action**: Fetch pages 1, 2 (fills pool). Drop guards. Fetch page 3.
- **Assert**: page 3 loaded, one of pages 1/2 evicted (clean, unpinned).

### T3.7 — Dirty frame NOT evicted
- **Setup**: BufferPool with 2 frames.
- **Action**: Fetch page 1 exclusive, modify, drop (dirty). Fetch page 2. Fetch page 3.
- **Assert**: page 2 is evicted (clean), not page 1 (dirty).

### T3.8 — Pinned frame NOT evicted
- **Setup**: BufferPool with 2 frames.
- **Action**: Hold shared guard on page 1. Fetch page 2. Fetch page 3.
- **Assert**: page 2 evicted, not page 1 (pinned).

### T3.9 — new_page allocates from free list
- **Setup**: Free list with PageId(5).
- **Action**: `new_page(BTreeLeaf, &mut free_list)`.
- **Assert**: returned guard's page_id = PageId(5).

### T3.10 — new_page extends file when free list empty
- **Setup**: Empty free list, data file with 3 pages.
- **Action**: `new_page(BTreeLeaf, &mut free_list)`.
- **Assert**: returned guard's page_id = PageId(3), file extended.

### T3.11 — snapshot_dirty_frames returns correct set
- **Setup**: Fetch pages 1, 2, 3 exclusive, modify 1 and 3 only.
- **Action**: `snapshot_dirty_frames()`.
- **Assert**: returns pages 1 and 3 (not 2).

### T3.12 — mark_clean_if_lsn matches
- **Setup**: Dirty frame with LSN 100.
- **Action**: `mark_clean_if_lsn(page_id, Lsn(100))`.
- **Assert**: frame marked clean.

### T3.13 — mark_clean_if_lsn mismatches (concurrent modification)
- **Setup**: Dirty frame with LSN 100. Modify again → LSN 200.
- **Action**: `mark_clean_if_lsn(page_id, Lsn(100))`.
- **Assert**: frame remains dirty (LSN changed).

### T3.14 — Concurrent readers on different pages
- **Setup**: BufferPool with multiple pages.
- **Action**: Spawn N tasks, each fetching a different page shared, reading data.
- **Assert**: all complete without deadlock or error.

---

## T4: B-Tree (`btree/`)

### T4.1 — Insert into empty tree
- **Setup**: Single empty leaf page as root.
- **Action**: `btree_insert(pool, root, cell_data, Fixed(24), ...)`.
- **Assert**: root unchanged, leaf has 1 cell.

### T4.2 — Insert preserves sort order
- **Action**: Insert 100 random keys.
- **Assert**: BTreeScan from min to max yields keys in ascending order.

### T4.3 — Leaf split
- **Setup**: Insert keys until leaf is full.
- **Action**: Insert one more key.
- **Assert**: root changed to internal node with 2 children. Both children are leaves with correct key distribution.

### T4.4 — Multiple splits (3-level tree)
- **Action**: Insert ~1000 entries (enough for 3 levels with 8 KB pages).
- **Assert**: tree has depth 3. All keys retrievable via cursor.seek.

### T4.5 — Root split creates new root
- **Action**: Force root (internal) to split.
- **Assert**: new root is internal with 2 children. Old root is now a child.

### T4.6 — Cursor seek exact match
- **Setup**: Tree with keys [10, 20, 30, 40, 50].
- **Action**: cursor.seek(30).
- **Assert**: cursor.key() == 30.

### T4.7 — Cursor seek non-existent (finds successor)
- **Setup**: Tree with keys [10, 20, 30, 40, 50].
- **Action**: cursor.seek(25).
- **Assert**: cursor.key() == 30.

### T4.8 — Cursor seek beyond end
- **Setup**: Tree with keys [10, 20, 30].
- **Action**: cursor.seek(40).
- **Assert**: cursor.is_valid() == false.

### T4.9 — Cursor next across leaf boundary
- **Setup**: Tree with 2 leaf pages.
- **Action**: Position cursor at last cell of first leaf, call next().
- **Assert**: cursor now points to first cell of second leaf.

### T4.10 — Scan range
- **Setup**: Tree with keys [10, 20, 30, 40, 50].
- **Action**: BTreeScan [20, Excluded(40)).
- **Assert**: yields keys 20, 30.

### T4.11 — Scan entire tree
- **Action**: BTreeScan [min_key, Unbounded).
- **Assert**: yields all keys in order.

### T4.12 — Scan empty range
- **Action**: BTreeScan [25, Excluded(25)).
- **Assert**: yields nothing.

### T4.13 — Delete from leaf (no underflow)
- **Setup**: Tree with keys [10, 20, 30, 40, 50].
- **Action**: btree_delete(30).
- **Assert**: scan yields [10, 20, 40, 50].

### T4.14 — Delete causing redistribution
- **Setup**: Tree with minimum-occupancy leaf.
- **Action**: Delete one key.
- **Assert**: sibling lends a key. Both leaves valid.

### T4.15 — Delete causing merge
- **Setup**: Two minimum-occupancy sibling leaves.
- **Action**: Delete from one.
- **Assert**: leaves merge into one. Parent updated.

### T4.16 — Delete all keys
- **Action**: Insert 100 keys, then delete all 100.
- **Assert**: tree is a single empty leaf. Scan yields nothing.

### T4.17 — Duplicate key handling
- **Action**: Insert two cells with the same key prefix but different inv_ts suffix.
- **Assert**: Both cells present. Cursor scan sees both in inv_ts order.

### T4.18 — Large tree stress test
- **Action**: Insert 10,000 random keys. Verify via full scan. Delete 5,000. Verify again. Delete rest. Verify empty.
- **Assert**: all invariants hold at each step.

---

## T5: External Heap (`heap.rs`)

### T5.1 — Store and read (single page)
- **Action**: Store a 5 KB document (> external_threshold 4 KB, < page usable).
- **Assert**: read back matches original bytes.

### T5.2 — Store and read (multi-page with overflow)
- **Action**: Store a 20 KB document.
- **Assert**: read back matches. Overflow pages allocated.

### T5.3 — Delete frees overflow pages
- **Setup**: Store a 20 KB document.
- **Action**: delete(heap_ref).
- **Assert**: overflow pages returned to free list.

### T5.4 — Replace (different size)
- **Setup**: Store 5 KB doc.
- **Action**: replace with 20 KB doc.
- **Assert**: old slot freed, new doc readable, new HeapRef different.

### T5.5 — Free space map best-fit
- **Setup**: Two heap pages with 2 KB and 3 KB free.
- **Action**: find_page(2500).
- **Assert**: returns the 3 KB page (smallest sufficient).

### T5.6 — Free space map rebuild
- **Setup**: Several heap pages with various occupancy.
- **Action**: rebuild_free_space_map.
- **Assert**: map matches actual page free space.

### T5.7 — Maximum document size (16 MB)
- **Action**: Store a 16 MB document.
- **Assert**: stored successfully with many overflow pages. Read back matches.

---

## T6: Checkpoint (`checkpoint.rs`)

### T6.1 — DWB write and read round-trip
- **Action**: write_dwb with 3 dirty pages, then read_dwb.
- **Assert**: all pages and page_ids match.

### T6.2 — Full checkpoint cycle
- **Setup**: Engine with dirty pages.
- **Action**: run_checkpoint.
- **Assert**: dirty pages flushed. DWB cleared. Checkpoint WAL record written. meta.json updated.

### T6.3 — Checkpoint marks frames clean
- **Setup**: 3 dirty frames.
- **Action**: run_checkpoint.
- **Assert**: all 3 frames marked clean (LSNs unchanged).

### T6.4 — Concurrent modification during checkpoint
- **Setup**: Dirty frame with LSN 100.
- **Action**: Start checkpoint (snapshots LSN 100). Concurrently modify the frame → LSN 200.
- **Assert**: mark_clean_if_lsn skips the frame (LSN mismatch). Frame stays dirty.

### T6.5 — Old WAL segments reclaimed
- **Setup**: 3 WAL segments. Checkpoint covers first 2.
- **Action**: run_checkpoint.
- **Assert**: first 2 segments deleted. Third retained.

---

## T7: Recovery (`recovery.rs`)

### T7.1 — DWB recovery repairs torn page
- **Setup**: Write DWB with page P. Corrupt page P in data.db (flip bytes).
- **Action**: recover_dwb.
- **Assert**: page P restored from DWB. DWB truncated.

### T7.2 — DWB recovery skips clean pages
- **Setup**: Write DWB with page P. data.db page P has valid checksum.
- **Action**: recover_dwb.
- **Assert**: page P unchanged. DWB truncated.

### T7.3 — DWB recovery with empty DWB
- **Setup**: Empty DWB file.
- **Action**: recover_dwb.
- **Assert**: no-op, no error.

### T7.4 — WAL replay of TxCommit
- **Setup**: Checkpoint at LSN 0. WAL has a TxCommit inserting doc_id=X.
- **Action**: replay_wal.
- **Assert**: primary B-tree contains the document.

### T7.5 — WAL replay of CreateCollection
- **Setup**: Checkpoint with empty catalog. WAL has CreateCollection "users".
- **Action**: replay_wal.
- **Assert**: catalog contains "users" with allocated root pages.

### T7.6 — WAL replay of IndexReady
- **Setup**: Catalog has index in Building state. WAL has IndexReady.
- **Action**: replay_wal.
- **Assert**: index state = Ready.

### T7.7 — WAL replay of Vacuum (idempotent)
- **Setup**: Vacuum record referencing doc versions. Run replay twice.
- **Assert**: no error on second replay (idempotent).

### T7.8 — Full recovery cycle
- **Setup**: Create engine, insert 100 docs, checkpoint, insert 50 more, "crash" (drop without shutdown).
- **Action**: Re-open engine (triggers recovery).
- **Assert**: all 150 docs present.

### T7.9 — Recovery with torn write + WAL
- **Setup**: After checkpoint, insert docs. Simulate torn page write.
- **Action**: Re-open engine.
- **Assert**: DWB repairs torn page. WAL replays post-checkpoint records. All data intact.

---

## T8: StorageEngine integration (`engine.rs`)

### T8.1 — Create and open database
- **Action**: StorageEngine::open on empty dir.
- **Assert**: data.db, wal/ created. File header valid.

### T8.2 — Re-open existing database
- **Setup**: Create engine, insert docs, shutdown.
- **Action**: Open again.
- **Assert**: docs present (recovered from checkpoint + WAL).

### T8.3 — Create collection through engine
- **Action**: engine.create_collection("users", default_config).
- **Assert**: catalog contains "users". Primary + _created_at roots allocated.

### T8.4 — Insert document through engine
- **Action**: Create collection, commit TxCommit with one insert.
- **Assert**: WAL record written. Document retrievable via B-tree scan.

### T8.5 — Concurrent reads during write
- **Setup**: Engine with populated collection.
- **Action**: Spawn reader tasks scanning the collection. Simultaneously commit an insert.
- **Assert**: readers complete without error. Writer completes.

### T8.6 — Graceful shutdown + re-open
- **Action**: Insert docs, shutdown(), open().
- **Assert**: all docs present after re-open.

### T8.7 — Checkpoint triggered by WAL size
- **Setup**: Set wal_size_threshold to 1 KB.
- **Action**: Insert enough data to exceed threshold.
- **Assert**: checkpoint runs automatically. Old WAL segments reclaimed.

### T8.8 — Create and query secondary index
- **Action**: Create index on field "status". Insert docs with status "active"/"inactive".
- Commit index delta entries.
- **Assert**: secondary index B-tree contains correct entries.

### T8.9 — Drop collection reclaims pages
- **Setup**: Collection with data.
- **Action**: drop_collection.
- **Assert**: all pages returned to free list. Catalog entry removed.

### T8.10 — Writer serialization
- **Action**: Submit 100 concurrent commit requests.
- **Assert**: all complete with unique LSNs. No data corruption.

---

## T9: WAL (`wal/`)

### T9.1 — Record serialization round-trip (all types)
- **Action**: For each WalRecordType, serialize then deserialize.
- **Assert**: all fields match.

### T9.2 — CRC mismatch detection
- **Action**: Serialize a record, flip one byte in payload, deserialize.
- **Assert**: returns WalError::CrcMismatch.

### T9.3 — Segment rollover
- **Setup**: Target size = 1 KB.
- **Action**: Write records until rollover.
- **Assert**: new segment created. Reader scans across both.

### T9.4 — Group commit batching
- **Setup**: 10 concurrent write requests.
- **Action**: Submit all, then check segment.
- **Assert**: all records present. fsync called fewer than 10 times (batched).

### T9.5 — End-of-data detection (pre-allocated zeros)
- **Setup**: Pre-allocated segment with zeros after last record.
- **Action**: WalReader::next().
- **Assert**: returns None at zero fill (payload_len = 0).

### T9.6 — TxCommit with mutations and index deltas
- **Action**: Serialize TxCommit with 3 mutations and 5 index deltas.
- **Assert**: round-trip preserves all fields.

---

## T10: File Header (`file_header.rs`)

### T10.1 — New database initialization
- **Action**: FileHeader::new_database(8192, PageId(1)).
- **Assert**: magic, version, page_size correct. checkpoint_lsn = 0.

### T10.2 — Serialize/deserialize round-trip
- **Action**: serialize, then deserialize.
- **Assert**: all fields match. Checksum valid.

### T10.3 — Shadow header fallback
- **Setup**: Corrupt page 0 (bad checksum). Valid shadow at last page.
- **Action**: read_file_header_with_fallback.
- **Assert**: returns header from shadow. Page 0 repaired.

### T10.4 — Both headers corrupt
- **Setup**: Corrupt both page 0 and shadow.
- **Action**: read_file_header_with_fallback.
- **Assert**: returns Err(BothCorrupt).
