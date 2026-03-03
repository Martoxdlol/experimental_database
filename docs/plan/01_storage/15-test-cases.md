# 15 — Test Cases

Every test case needed to ensure correctness of the storage layer. Organized by module, matching the implementation order. Each test has a unique ID for tracking.

---

## T1 — Page Format (`page.rs`)

### T1.1 Header Serialization

| ID | Test | Description |
|----|------|-------------|
| T1.1.1 | `header_round_trip` | Write a PageHeader to a buffer, read it back. All fields must match exactly. |
| T1.1.2 | `header_all_page_types` | Round-trip a header for each PageType variant (FileHeader, BTreeInternal, BTreeLeaf, Heap, Overflow, Free, FileHeaderShadow). |
| T1.1.3 | `header_boundary_values` | Round-trip with `page_id = u32::MAX`, `lsn = u64::MAX`, `num_slots = u16::MAX`, etc. |

### T1.2 Slotted Page — Init & Basic Operations

| ID | Test | Description |
|----|------|-------------|
| T1.2.1 | `init_empty_page` | Init a page, verify: `num_slots = 0`, `free_space_start = PAGE_HEADER_SIZE`, `free_space_end = page_size`, page_type set correctly. |
| T1.2.2 | `insert_single_cell` | Insert one cell. Verify `num_slots = 1`, cell data readable via `cell(0)`, `free_space` decreased by `SLOT_ENTRY_SIZE + cell_len`. |
| T1.2.3 | `insert_multiple_cells` | Insert 10 cells of varying sizes. Verify all are readable and in correct slot positions. |
| T1.2.4 | `insert_at_beginning` | Insert at index 0 on a page with existing cells. Verify subsequent slot entries shifted correctly. |
| T1.2.5 | `insert_at_middle` | Insert at index 3 on a page with 5 cells. Verify ordering. |
| T1.2.6 | `free_space_tracking` | Insert cells one by one, verify `free_space()` decreases correctly each time (by `4 + cell_len`). |

### T1.3 Slotted Page — Page Full

| ID | Test | Description |
|----|------|-------------|
| T1.3.1 | `insert_page_full` | Fill a page to capacity, then attempt one more insert. Must return `PageFullError`. |
| T1.3.2 | `insert_exact_fit` | Insert a cell that uses exactly all remaining free space. Must succeed. |
| T1.3.3 | `insert_one_byte_over` | Insert a cell 1 byte larger than remaining space. Must return `PageFullError`. |
| T1.3.4 | `update_cell_larger_page_full` | Update a cell with larger data when there's no room. Must return `PageFullError`. |

### T1.4 Slotted Page — Remove & Compact

| ID | Test | Description |
|----|------|-------------|
| T1.4.1 | `remove_single_cell` | Remove cell at index 0, verify `num_slots` decremented, slot marked as tombstone. |
| T1.4.2 | `remove_middle_cell` | Remove from the middle of 5 cells. Verify remaining cells still accessible. |
| T1.4.3 | `remove_then_compact` | Remove 3 cells out of 10, then compact. Verify: all remaining cells intact, `free_space` increased. |
| T1.4.4 | `compact_no_gaps` | Compact a page with no removed cells. Verify no data corruption and same free space. |
| T1.4.5 | `remove_all_cells` | Remove every cell from a page. Verify `num_slots = 0` and free space equals initial capacity. |
| T1.4.6 | `insert_after_remove_no_compact` | Remove a cell, then insert a new (smaller) one. Without compacting, the gap from removal is NOT reused (free_space_end stays). |
| T1.4.7 | `insert_after_compact` | Remove cells, compact, then insert. The reclaimed space is usable. |

### T1.5 Slotted Page — Update

| ID | Test | Description |
|----|------|-------------|
| T1.5.1 | `update_cell_same_size` | Update a cell with data of the same length. Must succeed, data must match. |
| T1.5.2 | `update_cell_smaller` | Update with smaller data. Must succeed. |
| T1.5.3 | `update_cell_larger` | Update with larger data (but space available). Must succeed. |

### T1.6 Checksum

| ID | Test | Description |
|----|------|-------------|
| T1.6.1 | `checksum_valid` | Write cells, compute checksum, verify. Must pass. |
| T1.6.2 | `checksum_detects_bit_flip` | Compute checksum, flip one bit in cell data, verify. Must fail. |
| T1.6.3 | `checksum_detects_header_corruption` | Flip a bit in the page header (not the checksum field), verify. Must fail. |
| T1.6.4 | `checksum_stable` | Compute checksum twice on the same page. Must be identical. |
| T1.6.5 | `update_checksum_then_verify` | Call `update_checksum()`, then `verify_checksum()`. Must pass. |

---

## T2 — Key Encoding (`key_encoding.rs`)

### T2.1 Scalar Round-Trip

| ID | Test | Description |
|----|------|-------------|
| T2.1.1 | `roundtrip_undefined` | Encode Undefined, decode. Must match. |
| T2.1.2 | `roundtrip_null` | Encode Null, decode. Must match. |
| T2.1.3 | `roundtrip_int64` | Encode/decode `0`, `1`, `-1`, `i64::MIN`, `i64::MAX`. All must round-trip. |
| T2.1.4 | `roundtrip_float64` | Encode/decode `0.0`, `-0.0`, `1.5`, `-1.5`, `f64::MIN`, `f64::MAX`, `f64::INFINITY`, `f64::NEG_INFINITY`. |
| T2.1.5 | `roundtrip_float64_nan` | Encode any NaN variant, decode. Must produce canonical NaN (quiet, positive). |
| T2.1.6 | `roundtrip_boolean` | Encode/decode `true` and `false`. |
| T2.1.7 | `roundtrip_string_empty` | Encode `""`, decode. Must match. |
| T2.1.8 | `roundtrip_string_simple` | Encode `"hello"`, decode. Must match. |
| T2.1.9 | `roundtrip_string_with_nulls` | Encode a string containing `\0` bytes. Must round-trip via escaping. |
| T2.1.10 | `roundtrip_string_unicode` | Encode a string with multi-byte UTF-8 chars (`"日本語"`). Must round-trip. |
| T2.1.11 | `roundtrip_bytes_empty` | Encode empty byte slice. Must round-trip. |
| T2.1.12 | `roundtrip_bytes_with_nulls` | Encode bytes containing `\x00`. Must round-trip via escaping. |
| T2.1.13 | `roundtrip_bytes_all_zeros` | Encode `[0x00; 100]`. Must round-trip. |

### T2.2 Sort Order

| ID | Test | Description |
|----|------|-------------|
| T2.2.1 | `type_order` | Encode one value of each type. Sort encoded bytes with memcmp. Must follow: undefined < null < int64 < float64 < boolean < string < bytes. |
| T2.2.2 | `int64_sort_order` | Encode `i64::MIN`, `-100`, `-1`, `0`, `1`, `100`, `i64::MAX`. Memcmp sort must match numeric order. |
| T2.2.3 | `float64_sort_order` | Encode `NEG_INFINITY`, `-1.0`, `-0.0`, `0.0`, `1.0`, `INFINITY`, `NaN`. Memcmp sort must match: neg_inf < -1.0 < -0.0 = 0.0 < 1.0 < inf < NaN. |
| T2.2.4 | `float64_negative_zero_equals_positive_zero` | Encode `-0.0` and `0.0`. Encoded bytes must be identical. |
| T2.2.5 | `float64_nan_sorts_last` | All NaN patterns (signaling, quiet, positive, negative) must encode identically and sort after INFINITY. |
| T2.2.6 | `boolean_sort_order` | `false` < `true` in memcmp. |
| T2.2.7 | `string_sort_order` | Encode `""`, `"a"`, `"ab"`, `"b"`, `"z"`. Memcmp must match lexicographic UTF-8 order. |
| T2.2.8 | `string_with_null_sort_order` | `"a\x00b"` < `"a\x01"` — null escaping must preserve ordering. |
| T2.2.9 | `bytes_sort_order` | Same as string sort order but with byte slices. |

### T2.3 Compound Keys

| ID | Test | Description |
|----|------|-------------|
| T2.3.1 | `compound_two_fields` | Encode `[Int64(1), String("a")]`, decode both values. Must match. |
| T2.3.2 | `compound_sort_first_field` | `[Int64(1), String("z")]` < `[Int64(2), String("a")]`. First field dominates. |
| T2.3.3 | `compound_sort_second_field` | `[Int64(1), String("a")]` < `[Int64(1), String("b")]`. Same first field, second decides. |
| T2.3.4 | `compound_three_fields` | Encode/decode three fields. All must round-trip. |

### T2.4 Primary & Secondary Keys

| ID | Test | Description |
|----|------|-------------|
| T2.4.1 | `primary_key_format` | Encode primary key, verify it's exactly 24 bytes: `doc_id[16] \|\| inv_ts[8]`. |
| T2.4.2 | `primary_key_same_doc_newer_first` | Two primary keys with same doc_id but different timestamps. Higher timestamp (lower inv_ts) sorts first. |
| T2.4.3 | `primary_key_different_docs` | Two different doc_ids. Sort order matches doc_id byte comparison. |
| T2.4.4 | `secondary_key_extract_suffix` | Encode a secondary key, extract doc_id and inv_ts from the last 24 bytes. Must match originals. |
| T2.4.5 | `secondary_key_sort_by_value_then_doc` | Two secondary keys with same value but different doc_ids. Sort by doc_id within value group. |

### T2.5 Successor Prefix

| ID | Test | Description |
|----|------|-------------|
| T2.5.1 | `successor_normal` | `successor_prefix(&[0x01, 0x02])` → `[0x01, 0x03]`. |
| T2.5.2 | `successor_trailing_ff` | `successor_prefix(&[0x01, 0xFF])` → `[0x02]`. |
| T2.5.3 | `successor_all_ff` | `successor_prefix(&[0xFF, 0xFF])` → `[0xFF, 0xFF, 0xFF]` (or similar overflow representation). |
| T2.5.4 | `successor_empty` | `successor_prefix(&[])` — edge case, define expected behavior. |

---

## T3 — WAL Records (`wal/record.rs`)

### T3.1 Record Serialization Round-Trip

| ID | Test | Description |
|----|------|-------------|
| T3.1.1 | `roundtrip_tx_commit_insert` | TxCommit with one Insert mutation, no index deltas. |
| T3.1.2 | `roundtrip_tx_commit_replace` | TxCommit with one Replace mutation. |
| T3.1.3 | `roundtrip_tx_commit_delete` | TxCommit with one Delete mutation (empty body). |
| T3.1.4 | `roundtrip_tx_commit_mixed` | TxCommit with multiple mutations (insert + replace + delete) and multiple index deltas. |
| T3.1.5 | `roundtrip_tx_commit_index_delta_insert` | IndexDelta with old_key=None, new_key=Some. |
| T3.1.6 | `roundtrip_tx_commit_index_delta_delete` | IndexDelta with old_key=Some, new_key=None. |
| T3.1.7 | `roundtrip_tx_commit_index_delta_update` | IndexDelta with both keys present. |
| T3.1.8 | `roundtrip_checkpoint` | Checkpoint record with a specific LSN. |
| T3.1.9 | `roundtrip_create_collection` | CreateCollection with a name. |
| T3.1.10 | `roundtrip_drop_collection` | DropCollection. |
| T3.1.11 | `roundtrip_create_index` | CreateIndex with compound field paths. |
| T3.1.12 | `roundtrip_drop_index` | DropIndex. |
| T3.1.13 | `roundtrip_index_ready` | IndexReady. |
| T3.1.14 | `roundtrip_vacuum` | Vacuum with multiple entries, each with multiple index keys. |
| T3.1.15 | `roundtrip_create_database` | CreateDatabase with name, path, and BSON config. |
| T3.1.16 | `roundtrip_drop_database` | DropDatabase. |
| T3.1.17 | `roundtrip_tx_commit_large_body` | TxCommit with a mutation body of 1 MB (stress test). |
| T3.1.18 | `roundtrip_tx_commit_empty` | TxCommit with zero mutations and zero index deltas. |

### T3.2 WAL Frame

| ID | Test | Description |
|----|------|-------------|
| T3.2.1 | `frame_round_trip` | Serialize a WalRecord to a frame, deserialize. Must match. |
| T3.2.2 | `frame_crc_valid` | Serialize a frame, verify CRC-32C matches on deserialization. |
| T3.2.3 | `frame_crc_detects_corruption` | Serialize a frame, flip one payload byte, deserialize. Must return `CrcMismatch`. |
| T3.2.4 | `frame_crc_detects_type_corruption` | Flip the record_type byte. CRC must fail. |
| T3.2.5 | `frame_unknown_record_type` | Attempt to deserialize a frame with record_type = 0xFF. Must return `InvalidRecordType`. |
| T3.2.6 | `frame_truncated` | Attempt to deserialize a truncated frame (fewer bytes than payload_len). Must return `UnexpectedEof` or similar. |

---

## T4 — WAL Segment (`wal/segment.rs`)

| ID | Test | Description |
|----|------|-------------|
| T4.1 | `create_new_segment` | Create a segment file. Verify header fields (magic, version, segment_id, base_lsn). |
| T4.2 | `open_existing_segment` | Create, close, reopen. Header must match. |
| T4.3 | `append_and_read_back` | Append 10 frames, read each back at the correct offset. Must match. |
| T4.4 | `current_lsn_advances` | After each append, `current_lsn()` must advance by `9 + payload_len`. |
| T4.5 | `should_rollover` | Append data until segment exceeds `WAL_TARGET_SEGMENT_SIZE`. `should_rollover()` must return true. |
| T4.6 | `should_not_rollover_when_small` | Segment below target size. `should_rollover()` must return false. |
| T4.7 | `corrupt_segment_header` | Open a segment with corrupted magic bytes. Must return `CorruptSegmentHeader`. |
| T4.8 | `pre_allocation` | After creation, file size should be ~64 MB (pre-allocated). Unused space is zero-filled. |

---

## T5 — WAL Writer (`wal/writer.rs`)

| ID | Test | Description |
|----|------|-------------|
| T5.1 | `write_single_record` | Write one record. Must return a valid LSN. Record must be readable via WalReader. |
| T5.2 | `write_multiple_sequential` | Write 100 records sequentially. All LSNs must be strictly increasing. |
| T5.3 | `write_concurrent` | Spawn 10 tasks, each writing 100 records concurrently. All 1000 records must be readable. No LSN duplicates. |
| T5.4 | `group_commit_batching` | Submit many records near-simultaneously. Verify they were fsynced in fewer batches than records (check by counting fsync calls or measuring time). |
| T5.5 | `segment_rollover` | Write enough data to trigger a rollover. Verify a new segment file is created. Both segments are readable. |
| T5.6 | `write_batch` | Use `write_batch()` with 5 records. All 5 LSNs returned. All readable. |
| T5.7 | `shutdown_flushes` | Write records, call `shutdown()`. Reopen WAL reader and verify all records are present. |
| T5.8 | `write_after_shutdown` | After shutdown, `write()` must return an error (channel closed). |

---

## T6 — WAL Reader (`wal/reader.rs`)

| ID | Test | Description |
|----|------|-------------|
| T6.1 | `scan_from_beginning` | Write N records, scan from LSN 0. Must yield all N records in order. |
| T6.2 | `scan_from_middle` | Write N records, scan from the 50th record's LSN. Must yield records 50..N. |
| T6.3 | `scan_across_segments` | Write enough to fill 3 segments. Scan from beginning to end. All records present, in order. |
| T6.4 | `scan_stops_on_eof` | Scan past the last record. Iterator must return None (not error). |
| T6.5 | `scan_stops_on_corruption` | Write records, corrupt one in the middle (flip bytes). Scan must yield all records before the corruption, then stop. |
| T6.6 | `segment_discovery` | Create 5 segments. `open()` must discover all 5, sorted by base_lsn. |
| T6.7 | `segment_for_lsn` | Given 3 segments with known base_lsns, `segment_for_lsn()` must return the correct segment for various LSNs. |
| T6.8 | `reclaim_segments` | Write to 3 segments. Reclaim with `reclaim_lsn` in the middle of segment 2. Segment 1 must be deleted. Segments 2 and 3 must remain. |
| T6.9 | `reclaim_no_segments` | Reclaim with `reclaim_lsn = 0`. Nothing should be deleted. |
| T6.10 | `reclaim_all_but_active` | Reclaim with very high LSN. All sealed segments deleted, active segment remains. |

---

## T7 — Buffer Pool (`buffer_pool.rs`)

### T7.1 Basic Operations

| ID | Test | Description |
|----|------|-------------|
| T7.1.1 | `fetch_page_reads_from_disk` | Write a page to disk directly. Fetch via buffer pool. Data must match. |
| T7.1.2 | `fetch_page_cache_hit` | Fetch same page twice. Second fetch must not read from disk (hit in cache). |
| T7.1.3 | `pin_count_increments` | Fetch same page twice (two PinnedPage guards). Both must be valid simultaneously. |
| T7.1.4 | `unpin_on_drop` | Fetch a page, drop the PinnedPage. The frame must be unpinned (evictable). |
| T7.1.5 | `new_page_allocates` | Call `new_page()`. Returns a valid PinnedPage with the correct page_type. |
| T7.1.6 | `new_page_extends_file` | With empty free list, `new_page()` extends the data file. New page_id = previous page_count. |

### T7.2 Dirty Tracking

| ID | Test | Description |
|----|------|-------------|
| T7.2.1 | `data_mut_marks_dirty` | Fetch a page, call `data_mut()`. Frame must be marked dirty. |
| T7.2.2 | `mark_dirty_explicit` | Fetch a page, call `mark_dirty()`. Frame must be dirty. |
| T7.2.3 | `dirty_pages_returns_all` | Dirty 5 frames. `dirty_pages()` must return all 5. |
| T7.2.4 | `mark_clean` | Dirty a frame, then `mark_clean()`. Must no longer appear in `dirty_pages()`. |

### T7.3 Eviction

| ID | Test | Description |
|----|------|-------------|
| T7.3.1 | `evict_clean_frame` | Fill all frames, unpin all (clean). Fetch a new page. One frame must be evicted (clock). |
| T7.3.2 | `no_evict_dirty` | Fill all frames, mark all dirty, unpin all. Fetch a new page. Must return `BufferPoolFull` (or trigger early checkpoint). |
| T7.3.3 | `no_evict_pinned` | Fill all frames, keep all pinned. Fetch a new page. Must fail (all pinned). |
| T7.3.4 | `clock_eviction_skips_referenced` | Fill frames, access some recently (ref_bit=1). Eviction should prefer unreferenced frames. |
| T7.3.5 | `eviction_writes_nothing_to_disk` | Evict a clean frame. Verify no disk write occurred (clean frames are just discarded). |

### T7.4 Checksum Verification

| ID | Test | Description |
|----|------|-------------|
| T7.4.1 | `fetch_valid_page` | Write a page with valid checksum to disk. Fetch must succeed. |
| T7.4.2 | `fetch_corrupt_page` | Write a page with invalid checksum to disk. Fetch must return `CorruptPage`. |

### T7.5 Raw I/O

| ID | Test | Description |
|----|------|-------------|
| T7.5.1 | `read_page_raw` | Write a page to disk. `read_page_raw()` must return exact bytes (bypasses cache). |
| T7.5.2 | `write_page_raw` | Write arbitrary bytes via `write_page_raw()`. `read_page_raw()` returns them. |

---

## T8 — Free List (`freelist.rs`)

| ID | Test | Description |
|----|------|-------------|
| T8.1 | `push_and_pop` | Push page 5, pop. Must return page 5. |
| T8.2 | `fifo_or_lifo_order` | Push pages 1, 2, 3. Pop three times. Verify order (should be LIFO: 3, 2, 1). |
| T8.3 | `pop_empty` | Pop from an empty list. Must return None. |
| T8.4 | `push_multiple_pop_all` | Push 100 pages, pop all. Must return all 100, none missing. |
| T8.5 | `head_tracks_correctly` | Push page 10. `head()` must be `Some(PageId(10))`. Pop. `head()` must be None (or previous head). |
| T8.6 | `persist_through_pages` | Push pages, verify the linked list is written to page data (readable by buffer pool). Reopen and reconstruct — same list. |

---

## T9 — B-Tree Node (`btree/node.rs`)

### T9.1 Leaf Node

| ID | Test | Description |
|----|------|-------------|
| T9.1.1 | `leaf_insert_single` | Insert one cell into an empty leaf. `cell_count() = 1`. Cell data readable. |
| T9.1.2 | `leaf_insert_sorted` | Insert cells with keys `[c, a, b]`. After insertion, `search_leaf("a")` must return `Found(0)`. |
| T9.1.3 | `leaf_search_found` | Insert keys `[a, b, c]`. `search_leaf("b")` → `Found(1)`. |
| T9.1.4 | `leaf_search_not_found` | Search for key between existing keys. Must return `NotFound` with correct insertion point. |
| T9.1.5 | `leaf_search_empty` | Search on empty leaf. Must return `NotFound(0)`. |
| T9.1.6 | `leaf_remove` | Insert 3 cells, remove middle one. Remaining 2 cells must be correct. |
| T9.1.7 | `leaf_right_sibling` | Set right sibling, read it back. Must match. |
| T9.1.8 | `leaf_right_sibling_none` | Default right sibling must be None (page_id = 0). |
| T9.1.9 | `leaf_split` | Fill a leaf to capacity, split. Left half and right half must together contain all original cells. Median key must be correct. |
| T9.1.10 | `leaf_split_preserves_order` | After split, all keys in left < all keys in right. |
| T9.1.11 | `leaf_split_sets_sibling` | After split, left's right_sibling must point to the right page. |

### T9.2 Internal Node

| ID | Test | Description |
|----|------|-------------|
| T9.2.1 | `internal_leftmost_child` | Set and read leftmost child. Must match. |
| T9.2.2 | `internal_insert_cell` | Insert one key + child pointer. `cell_count() = 1`. |
| T9.2.3 | `internal_search` | Insert keys `[10, 20, 30]` with children. Search for `15` must descend to the child between 10 and 20. |
| T9.2.4 | `internal_search_before_first` | Search for key < all keys. Must return leftmost_child. |
| T9.2.5 | `internal_search_after_last` | Search for key > all keys. Must return the last child. |
| T9.2.6 | `internal_split` | Fill an internal node, split. Promoted key must be the median. Left and right must partition correctly. |
| T9.2.7 | `internal_remove` | Remove a cell from an internal node. Verify remaining cells correct. |

### T9.3 Primary Leaf Cell Parsing

| ID | Test | Description |
|----|------|-------------|
| T9.3.1 | `read_primary_leaf_inline` | Write an inline primary cell, read it back. All fields must match (doc_id, inv_ts, body). |
| T9.3.2 | `read_primary_leaf_external` | Write an external primary cell (EXTERNAL flag set), read it back. Must return `CellBody::External` with correct HeapRef. |
| T9.3.3 | `read_primary_leaf_tombstone` | Write a tombstone cell (TOMBSTONE flag set). Must return `CellBody::Tombstone`. |

### T9.4 Secondary Leaf Cell Parsing

| ID | Test | Description |
|----|------|-------------|
| T9.4.1 | `read_secondary_leaf_cell` | Write a secondary index cell, read it back. Key, doc_id, inv_ts must match. |

---

## T10 — B-Tree Cursor (`btree/cursor.rs`)

| ID | Test | Description |
|----|------|-------------|
| T10.1 | `seek_exact` | Insert keys [10, 20, 30]. Seek to 20. Current cell must have key 20. |
| T10.2 | `seek_between` | Seek to 15. Must position at 20 (first >= 15) in forward mode. |
| T10.3 | `seek_before_all` | Seek to 5 (below minimum). Must position at 10 (first key). |
| T10.4 | `seek_after_all` | Seek to 35 (above maximum). Cursor must be exhausted immediately. |
| T10.5 | `seek_start_forward` | `seek_start(Forward)`. Must be at the first cell. |
| T10.6 | `seek_start_backward` | `seek_start(Backward)`. Must be at the last cell. |
| T10.7 | `advance_forward` | Position at first key, advance to end. Must visit all keys in order. |
| T10.8 | `advance_backward` | Position at last key, advance backward. Must visit all keys in reverse order. |
| T10.9 | `advance_across_pages` | Insert enough keys to span 3+ leaf pages. Forward scan must follow right_sibling links seamlessly. |
| T10.10 | `advance_exhausted` | Advance past the last key. `is_exhausted()` must return true. Further `advance()` returns false. |
| T10.11 | `empty_tree` | Seek on an empty B-tree. Cursor must be immediately exhausted. |

---

## T11 — B-Tree Ops (`btree/ops.rs`)

### T11.1 Get

| ID | Test | Description |
|----|------|-------------|
| T11.1.1 | `get_existing_key` | Insert key, get it. Must return the correct value. |
| T11.1.2 | `get_missing_key` | Get a key that was never inserted. Must return None. |
| T11.1.3 | `get_after_delete` | Insert, delete, get. Must return None. |

### T11.2 Insert

| ID | Test | Description |
|----|------|-------------|
| T11.2.1 | `insert_single` | Insert one key. Tree must contain it. |
| T11.2.2 | `insert_ascending_order` | Insert 1000 keys in ascending order. All must be retrievable. |
| T11.2.3 | `insert_descending_order` | Insert 1000 keys in descending order. All must be retrievable. |
| T11.2.4 | `insert_random_order` | Insert 10000 keys in random order. All must be retrievable. |
| T11.2.5 | `insert_causes_leaf_split` | Insert enough keys to split a leaf page. Root must become an internal node. Both children must have correct keys. |
| T11.2.6 | `insert_causes_multi_level_split` | Insert enough to produce a 3-level tree. All keys retrievable. |
| T11.2.7 | `insert_causes_root_split` | Insert until the root (internal) splits. New root created with 2 children. |
| T11.2.8 | `insert_duplicate_key` | Insert the same key twice (with different values). Both must be stored (MVCC — keys include inv_ts, so they differ). |
| T11.2.9 | `insert_returns_new_root` | When a root split occurs, `insert()` must return the new root PageId. |

### T11.3 Delete

| ID | Test | Description |
|----|------|-------------|
| T11.3.1 | `delete_existing` | Insert, delete. `get()` must return None. |
| T11.3.2 | `delete_nonexistent` | Delete a key that doesn't exist. Must not panic (no-op or error). |
| T11.3.3 | `delete_all_keys` | Insert 100 keys, delete all. Tree must be empty. |
| T11.3.4 | `delete_interleaved_with_inserts` | Insert 100, delete 50, insert 50 more. All remaining 100 must be correct. |

### T11.4 Range Scan

| ID | Test | Description |
|----|------|-------------|
| T11.4.1 | `range_scan_full` | Insert 100 keys. Range scan from min to max. Must return all 100 in order. |
| T11.4.2 | `range_scan_partial` | Scan from key 30 to key 70 (forward). Must return exactly the keys in that range. |
| T11.4.3 | `range_scan_backward` | Scan backward from key 70. Must return keys in descending order. |
| T11.4.4 | `range_scan_empty_range` | Scan a range that contains no keys. Must return nothing. |
| T11.4.5 | `range_scan_single_key` | Range scan that matches exactly one key. Must return it. |
| T11.4.6 | `range_scan_after_splits` | Insert enough for multi-level tree, then range scan. Results must be correct across page boundaries. |

### T11.5 Count

| ID | Test | Description |
|----|------|-------------|
| T11.5.1 | `count_empty` | Empty tree. Must return 0. |
| T11.5.2 | `count_after_inserts` | Insert 500 keys. Count must return 500. |

---

## T12 — External Heap (`heap.rs`)

| ID | Test | Description |
|----|------|-------------|
| T12.1 | `store_and_read_small` | Store a 100-byte document. Read it back. Must match. |
| T12.2 | `store_and_read_page_sized` | Store a document that fills exactly one heap page. Read it back. |
| T12.3 | `store_and_read_multi_page` | Store a 20 KB document (requires overflow pages at 8 KB page size). Read it back. Must match exactly. |
| T12.4 | `store_and_read_large` | Store a 1 MB document. Read back. Must match (many overflow pages). |
| T12.5 | `delete_single_page` | Store, then delete. HeapRef must be freed. Slot must be reclaimable. |
| T12.6 | `delete_multi_page` | Store a multi-page document, delete. All overflow pages must be freed. |
| T12.7 | `replace_same_size` | Store, replace with same-size body. HeapRef may be reused. Data must match. |
| T12.8 | `replace_smaller` | Replace with a smaller body. Must succeed. |
| T12.9 | `replace_larger_fits` | Replace with a slightly larger body that still fits in the same slot. |
| T12.10 | `replace_larger_needs_new` | Replace with a much larger body. Old slot freed, new slot allocated. |
| T12.11 | `free_space_map_find` | After storing a few documents, `find_page()` must find pages with sufficient free space. |
| T12.12 | `multiple_docs_per_heap_page` | Store multiple small documents. They should share heap pages. |

---

## T13 — Catalog (`catalog.rs`)

### T13.1 Collections

| ID | Test | Description |
|----|------|-------------|
| T13.1.1 | `create_collection` | Create a collection. Must be retrievable by name. Must have allocated primary + _created_at root pages. |
| T13.1.2 | `create_duplicate_name` | Create two collections with the same name. Second must return an error. |
| T13.1.3 | `drop_collection` | Create, then drop. Lookup by name must return None. |
| T13.1.4 | `drop_nonexistent` | Drop a collection that doesn't exist. Must return error. |
| T13.1.5 | `list_collections` | Create 5 collections. `list_collections()` must return all 5. |
| T13.1.6 | `list_empty` | No collections. `list_collections()` must return empty vec. |
| T13.1.7 | `update_primary_root` | Update primary root page. Subsequent lookup must reflect the new root. |
| T13.1.8 | `update_doc_count` | Increment doc count by 10. Must be reflected in collection metadata. |
| T13.1.9 | `collection_id_monotonic` | Create 3 collections. IDs must be strictly increasing. |

### T13.2 Indexes

| ID | Test | Description |
|----|------|-------------|
| T13.2.1 | `create_index` | Create an index on a collection. Must be in `Building` state. |
| T13.2.2 | `create_index_compound` | Create with field_paths `[["user", "email"], "enabled"]`. Round-trip must match. |
| T13.2.3 | `mark_index_ready` | Create, mark ready. State must transition to `Ready`. |
| T13.2.4 | `drop_index` | Create, drop. Must not appear in `list_indexes()`. |
| T13.2.5 | `list_indexes` | Create 3 indexes on a collection. `list_indexes()` returns all 3. |
| T13.2.6 | `list_indexes_other_collection` | Indexes on collection A must not appear when listing collection B's indexes. |
| T13.2.7 | `index_id_monotonic` | Create 3 indexes. IDs must be strictly increasing. |

### T13.3 Catalog Persistence

| ID | Test | Description |
|----|------|-------------|
| T13.3.1 | `catalog_load` | Create collections and indexes, then `load()` from B-tree. Cache must match original. |
| T13.3.2 | `catalog_survives_reopen` | Create collections, close engine, reopen. All collections and indexes must be present. |
| T13.3.3 | `catalog_serialization_round_trip` | Serialize and deserialize `CollectionMeta` and `IndexMeta`. All fields must match. |

---

## T14 — Double-Write Buffer (`dwb.rs`)

| ID | Test | Description |
|----|------|-------------|
| T14.1 | `write_and_read_back` | Write 10 pages to DWB. Read all entries. Must match exactly. |
| T14.2 | `header_fields` | After writing, verify DWB header: magic, version, page_size, page_count, checksum. |
| T14.3 | `truncate` | Write pages, truncate. `needs_recovery()` must return false. |
| T14.4 | `needs_recovery_empty` | No DWB file exists. Must return false. |
| T14.5 | `needs_recovery_non_empty` | Write pages (don't truncate). Must return true. |
| T14.6 | `recovery_no_torn_writes` | Write pages to DWB, also write them correctly to data.db. Recovery should detect all pages are valid and not overwrite. |
| T14.7 | `recovery_torn_write` | Write pages to DWB. In data.db, corrupt one page (bad checksum). Recovery must restore the torn page from DWB. Verify data.db page is now correct. |
| T14.8 | `recovery_multiple_torn` | Corrupt 3 out of 10 pages in data.db. Recovery must restore exactly 3. Verify all 10 are correct after. |
| T14.9 | `recovery_idempotent` | Run recovery twice. Second run should detect all pages are valid and make no changes. |

---

## T15 — Checkpoint (`checkpoint.rs`)

| ID | Test | Description |
|----|------|-------------|
| T15.1 | `checkpoint_flushes_dirty` | Dirty 5 frames. Checkpoint. All 5 must be clean. Data must be on disk. |
| T15.2 | `checkpoint_writes_wal_record` | After checkpoint, WAL must contain a Checkpoint record with the correct LSN. |
| T15.3 | `checkpoint_reclaims_segments` | Write enough WAL to fill 3 segments. Checkpoint. Old segments (below checkpoint_lsn) must be deleted. |
| T15.4 | `checkpoint_respects_replica_lsn` | Primary has 3 segments. Replica at LSN in segment 2. `reclaim_lsn = min(checkpoint_lsn, replica_lsn)`. Segment 1 may be reclaimed, segment 2 must be retained. |
| T15.5 | `checkpoint_dwb_sequence` | Verify that DWB is written and fsynced BEFORE data.db writes. (Inspect DWB existence during checkpoint if possible, or verify invariant via crash test.) |
| T15.6 | `checkpoint_truncates_dwb` | After successful checkpoint, DWB file must be empty/absent. |
| T15.7 | `checkpoint_updates_file_header` | After checkpoint, file header (page 0) must have updated `checkpoint_lsn`. |
| T15.8 | `checkpoint_no_dirty_pages` | Checkpoint with no dirty frames. Should be a no-op (no DWB write, no data.db writes). |

---

## T16 — Crash Recovery (`recovery.rs`)

### T16.1 WAL Replay

| ID | Test | Description |
|----|------|-------------|
| T16.1.1 | `replay_tx_commit_insert` | Write a TxCommit(Insert) to WAL but don't apply to pages. Recover. Document must be in the primary B-tree. |
| T16.1.2 | `replay_tx_commit_replace` | Same for Replace. New version must be in the tree. |
| T16.1.3 | `replay_tx_commit_delete` | Same for Delete. Tombstone must be in the tree. |
| T16.1.4 | `replay_index_deltas` | TxCommit with index deltas. After replay, secondary index entries must exist. |
| T16.1.5 | `replay_create_collection` | WAL contains CreateCollection. After replay, catalog must contain the collection. |
| T16.1.6 | `replay_drop_collection` | Create, then drop in WAL. After replay, collection must be gone. |
| T16.1.7 | `replay_create_index` | After replay, catalog must contain the index in Building state. |
| T16.1.8 | `replay_index_ready` | CreateIndex + IndexReady in WAL. After replay, index must be Ready. |
| T16.1.9 | `replay_vacuum` | Vacuum record for a specific version. After replay, that version must be removed. Second replay is a no-op (idempotent). |
| T16.1.10 | `replay_checkpoint_noop` | Checkpoint record in WAL. Replay must not crash or alter state. |
| T16.1.11 | `replay_multiple_transactions` | 10 TxCommit records. All must be replayed. All documents and indexes must be correct. |
| T16.1.12 | `replay_idempotent` | Replay the same WAL twice. Result must be identical (page LSN check skips already-applied records). |

### T16.2 DWB Recovery

| ID | Test | Description |
|----|------|-------------|
| T16.2.1 | `dwb_recovery_before_replay` | Leave a non-empty DWB (simulating crash during checkpoint). Recovery must repair torn pages BEFORE WAL replay. |
| T16.2.2 | `dwb_recovery_not_needed` | Clean DWB (empty). Recovery skips DWB step. WAL replay proceeds normally. |

### T16.3 End-to-End Crash Simulation

| ID | Test | Description |
|----|------|-------------|
| T16.3.1 | `crash_after_wal_write` | Write WAL records for inserts. Don't checkpoint or apply to pages. Kill process. Recover. All inserts must be present. |
| T16.3.2 | `crash_during_checkpoint` | Start checkpoint, write DWB, write some pages to data.db, corrupt one page (simulate torn write). Kill. Recover. All pages must be correct. Documents must be present. |
| T16.3.3 | `crash_after_checkpoint` | Complete checkpoint, then write more WAL records. Kill. Recover. Checkpoint data intact, WAL records after checkpoint replayed. |
| T16.3.4 | `repeated_crash_recovery` | Crash and recover 3 times in succession (each time adding more WAL records). Final state must be consistent. |

---

## T17 — File Header (`engine.rs`)

| ID | Test | Description |
|----|------|-------------|
| T17.1 | `file_header_round_trip` | Write and read a FileHeader. All fields must match. |
| T17.2 | `file_header_verify_valid` | Write correct header with checksum. `verify()` must pass. |
| T17.3 | `file_header_verify_corrupt` | Corrupt a field. `verify()` must fail. |
| T17.4 | `file_header_bad_magic` | Write header with wrong magic. `verify()` must fail. |
| T17.5 | `shadow_header_matches` | After checkpoint, shadow header (last page) must match page 0 header. |
| T17.6 | `shadow_header_recovery` | Corrupt page 0 header. Recovery must restore from shadow. |

---

## T18 — Storage Engine Integration (`engine.rs`)

### T18.1 Lifecycle

| ID | Test | Description |
|----|------|-------------|
| T18.1.1 | `create_new_database` | `create()` a new database. Directory structure must exist: `wal/`, `data.db`, `meta.json`. |
| T18.1.2 | `open_existing_database` | Create, shutdown, `open()`. Must succeed. Catalog must be intact. |
| T18.1.3 | `open_nonexistent` | `open()` on a missing directory. Must return error. |
| T18.1.4 | `shutdown_checkpoints` | Insert documents, shutdown. Reopen. Documents must be present (checkpoint on shutdown). |

### T18.2 Document Operations

| ID | Test | Description |
|----|------|-------------|
| T18.2.1 | `apply_insert_and_get` | Apply an Insert mutation. `get_document()` must return the document. |
| T18.2.2 | `apply_replace_and_get` | Insert, then Replace. Get must return the new version. |
| T18.2.3 | `apply_delete_and_get` | Insert, then Delete. Get must return None. |
| T18.2.4 | `mvcc_version_resolution` | Insert at ts=10, Replace at ts=20. Get at ts=15 must return the ts=10 version. Get at ts=25 must return the ts=20 version. |
| T18.2.5 | `mvcc_tombstone_invisible` | Insert at ts=10, Delete at ts=20. Get at ts=15 must return the document. Get at ts=25 must return None. |
| T18.2.6 | `mvcc_not_yet_visible` | Insert at ts=10. Get at ts=5 must return None (not yet visible). |

### T18.3 Index Scan

| ID | Test | Description |
|----|------|-------------|
| T18.3.1 | `index_scan_forward` | Insert 50 documents. Scan secondary index forward. Must return docs in ascending key order. |
| T18.3.2 | `index_scan_backward` | Same, backward. Descending key order. |
| T18.3.3 | `index_scan_with_bounds` | Scan with lower and upper bounds. Only docs in range returned. |
| T18.3.4 | `index_scan_mvcc` | Insert doc at ts=10, update at ts=20. Scan at ts=15 must see ts=10 version only. |
| T18.3.5 | `index_scan_skips_stale_entries` | Insert at ts=10 with field="a", replace at ts=20 with field="b". Scan for field="a" at ts=25 must NOT return the doc (stale secondary entry). |
| T18.3.6 | `index_scan_tombstone` | Insert, then delete. Scan must not return the deleted document. |
| T18.3.7 | `index_scan_deduplication` | Multiple versions of same doc. Scan must return each doc_id at most once. |

### T18.4 Multiple Collections

| ID | Test | Description |
|----|------|-------------|
| T18.4.1 | `multiple_collections_isolated` | Insert into collection A and B. Documents in A must not appear in B. |
| T18.4.2 | `drop_collection_cleans_up` | Create collection, insert docs, drop. Catalog must not contain it. Pages should be freed. |

### T18.5 WAL Integration

| ID | Test | Description |
|----|------|-------------|
| T18.5.1 | `write_wal_returns_lsn` | `write_wal()` returns an LSN. Must be valid and increasing. |
| T18.5.2 | `wal_and_apply_consistency` | Write WAL record then apply mutations. Reopen (crash recovery). State must be consistent. |

### T18.6 Checkpoint Integration

| ID | Test | Description |
|----|------|-------------|
| T18.6.1 | `explicit_checkpoint` | Insert docs, explicit checkpoint. All dirty pages flushed. WAL reclaimed appropriately. |
| T18.6.2 | `early_checkpoint_on_full_pool` | Fill the buffer pool with dirty pages. The engine must trigger an early checkpoint to free frames. |

---

## T19 — End-to-End Scenarios

These tests exercise the full storage layer as a black box.

| ID | Test | Description |
|----|------|-------------|
| T19.1 | `full_lifecycle` | Create DB → create collection → create index → insert 1000 docs → query by index → checkpoint → close → reopen → query again → all docs present. |
| T19.2 | `crash_recovery_full` | Create DB → insert 500 docs → checkpoint → insert 500 more (no checkpoint) → simulate crash → recover → all 1000 docs present. |
| T19.3 | `crash_during_checkpoint_full` | Insert docs → start checkpoint → corrupt one data.db page (simulate torn write) → recover → all docs present (DWB restores torn page). |
| T19.4 | `large_dataset` | Insert 100,000 documents across 10 collections. Verify all retrievable. Checkpoint. Reopen. Verify again. |
| T19.5 | `large_documents` | Insert documents of sizes: 100B, 1KB, 10KB, 100KB, 1MB, 16MB. All must be storable and retrievable. |
| T19.6 | `many_collections` | Create 1000 collections. Verify catalog lists all. Drop 500. Verify remaining 500. |
| T19.7 | `many_indexes` | Create 50 indexes on a single collection. Insert docs, verify all indexes updated. |
| T19.8 | `concurrent_reads` | Spawn 100 async tasks reading different documents simultaneously. No panics, correct results. |
| T19.9 | `index_scan_across_versions` | Insert doc, update 10 times. Scan at each version timestamp. Each scan must see the correct version. |
| T19.10 | `wal_segment_rollover_under_load` | Insert enough to trigger multiple WAL segment rollovers. All data must survive checkpoint + recovery. |

---

## Test Infrastructure

### Shared Helpers

```rust
/// Create a temporary database directory with tempfile.
fn temp_db_dir() -> tempfile::TempDir;

/// Create a StorageEngine in a temp directory with default config.
async fn test_engine() -> (StorageEngine, tempfile::TempDir);

/// Create a BufferPool backed by MemPageIO (no disk I/O).
fn mem_buffer_pool(page_size: u32, frame_count: u32) -> BufferPool;

/// Generate N random documents as BSON bytes.
fn random_docs(n: usize, size_range: Range<usize>) -> Vec<Vec<u8>>;

/// Generate a sequence of DocIds for testing.
fn sequential_doc_ids(n: usize) -> Vec<DocId>;
```

### Test Categories

| Category | Count | Runner |
|----------|-------|--------|
| Unit (pure logic, no I/O) | ~80 | `cargo test` |
| Integration (with tempfile I/O) | ~70 | `cargo test` (tempfile cleanup) |
| End-to-end (full engine) | ~10 | `cargo test` (longer timeout) |
| **Total** | **~160** | |
