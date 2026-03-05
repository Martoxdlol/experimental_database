I've now read through the entire storage implementation. Here's my analysis of subtle bugs, edge cases, and incorrect assumptions, ordered by severity.

  ---
  Critical

  1. Page checksums are never updated after modification — DWB recovery is broken

  This is the most serious finding. SlottedPage::init() stamps a valid CRC-32C, but no code path stamps the checksum after modifying a page. B-tree inserts, heap writes, file header updates — none
   re-stamp the checksum.

  This breaks the entire DWB torn-write protection mechanism. During DWB::recover() (dwb.rs:232-235):

  let dwb_ref = SlottedPageRef::from_buf(dwb_page_data);
  if !dwb_ref.verify_checksum() {
      continue; // ← always skips, because checksum is stale
  }

  Since the DWB copy itself has a stale checksum (it was snapshotted from the buffer pool where no stamping occurred), verify_checksum() fails and the page is skipped. The same check on the data
  file page also fails. Result: DWB cannot detect or repair torn writes on any modified page.

  The same issue exists in recovery.rs:272-273 for the same reason.

  Fix: Stamp checksums during checkpoint, before snapshotting dirty pages — either in BufferPool::dirty_pages() or in the checkpoint coordinator before calling dwb.write_pages().

  2. Buffer pool fast-path TOCTOU race

  In buffer_pool.rs:134-150 (fetch_page_shared fast path):

  let pt = self.page_table.read();
  if let Some(&frame_id) = pt.get(&page_id) {
      let slot = &self.frames[frame_id as usize];
      slot.pin_count.fetch_add(1, Ordering::AcqRel);  // ← atomic, no lock
      slot.ref_bit.store(true, Ordering::Release);
      let guard = slot.lock.read();                     // ← blocks until write lock released
      return Ok(SharedPageGuard { ... });
  }

  Between the page_table read and the pin_count increment, another thread can evict this frame via find_victim. The eviction re-checks pin_count under the frame write lock (buffer_pool.rs:436),
  but the timing window is:

  1. Thread A: reads page_table → frame 5 maps to page 10
  2. Thread B: find_victim → frame 5 has pin_count=0 → acquires write lock → re-checks pin_count (still 0) → evicts frame 5
  3. Thread A: increments pin_count (too late) → waits for read lock
  4. Thread B: drops write lock, installs page 20 in frame 5
  5. Thread A: acquires read lock → reads page 20 thinking it's page 10

  Or worse, between step 2 and step 4, thread A gets the read lock on a frame with page_id = None, causing page_id() to panic.

  Fix: After acquiring the frame lock in the fast path, re-validate that guard.page_id == Some(page_id). If not, decrement pin_count and fall through to the slow path.

  ---
  Important

  3. PageType::from_u8 panics on invalid values (page.rs:62-72)

  fn from_u8(v: u8) -> Self {
      match v {
          0x01 => ...,
          ...
          _ => panic!("invalid page type: {:#04x}", v),
      }
  }

  Any page corruption that changes the type byte crashes the process. This was already hit in the previous session (zeroed page → panic on 0x00). A corrupted page anywhere in the data file turns a
   recoverable error into an unrecoverable panic.

  Fix: Return Result<Self, io::Error> instead of panicking. Propagate the error through callers.

  4. WAL payload_len == 0 is an end-of-log sentinel (wal.rs:408-410)

  if payload_len == 0 {
      return None;
  }

  This means a WAL record with an empty payload is impossible — it would be misinterpreted as end-of-log. Currently no record type uses empty payloads, but this is a silent contract that's easy to
   violate when adding new record types.

  5. update_slot loses data on PageFullError after compaction (page.rs:371-389)

  When the new data is larger and compaction is attempted:
  self.write_slot_entry(slot, SlotEntry { offset: entry.offset, length: 0 }); // marks old as tombstone
  self.compact(); // overwrites old cell space
  if contiguous < needed {
      return Err(PageFullError); // old data is gone!
  }

  The comment acknowledges it: "the old data is gone (compaction may have moved things)." The B-tree's split handler must handle this, but there's no way for the caller to recover the old value.

  6. Page size implicitly limited to 65535 bytes but never validated

  Page header uses U16 for free_space_start and free_space_end (page.rs:96-98). Slot offsets and lengths are u16. This means max page size is 65535 bytes. But StorageConfig::page_size is usize
  with no upper-bound check. Using a page size > 65535 would cause silent truncation and corruption.

  Similarly, DwbHeader::page_size is u16 (dwb.rs:41), silently truncating larger page sizes.

  7. FilePageStorage::read_page doesn't handle short reads (backend.rs:154)

  self.file.read_at(buf, offset)?;

  pread can return fewer bytes than requested (POSIX allows short reads even on regular files). The code doesn't check the return value or retry. In practice this rarely happens on local
  filesystems, but it's technically incorrect and could matter on networked filesystems.

  ---
  Minor

  8. FileWalStorage::open doesn't detect gaps between segments

  Segments are sorted by base_lsn and write_offset is computed from the last segment only (backend.rs:298). If a middle segment file is deleted or missing, reads across the gap would silently
  return 0 bytes, and the missing data would never be detected.

  9. Heap::rebuild_free_space_map reads every page through the buffer pool (heap.rs:416-431)

  On a large database, this evicts all useful cached pages at startup, filling the pool with heap pages that may not be needed soon. Could be optimized by reading page headers directly from the
  backend.

  10. File header page_count can drift from actual file size

  FilePageStorage tracks page_count in an AtomicU64 and also in FileHeader::page_count. After a crash during extend() (between set_len and the page_count.store), the file could be larger than
  page_count indicates. On reopen, FilePageStorage::open recomputes from file size, but FileHeader::page_count might be stale.

  11. FileWalStorage::read_from opens a new file handle per read (backend.rs:517)

  Every read_from call opens a fresh File::open for each segment it reads from. This is correct but slow for the WAL replay path which reads sequentially through many records.

  ---
  Want me to fix any of these? The checksum stamping issue (#1) and the buffer pool race (#2) are the ones I'd prioritize.