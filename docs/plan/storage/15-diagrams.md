# Internal Flow Diagrams

## 1. Page Read Path (Buffer Pool Miss)

```
fetch_page_shared(page_id=42)
         │
         ▼
┌─────────────────┐
│ page_table      │
│  .read()        │──── Found frame_id=7 ───► Acquire frame[7].read()
│  lookup(42)     │                           Set ref_bit = true
└────────┬────────┘                           pin_count += 1
         │ NOT FOUND                          Return SharedPageGuard
         ▼
┌─────────────────┐
│ Release         │  ← No lock held during I/O!
│ page_table      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ PageStorage     │
│  .read_page(42) │  ← Disk read into temp buffer
│  → temp_buf     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ find_victim()   │  ← Clock scan for evictable frame
│ → frame_id=3   │     Skip: pinned, dirty, ref_bit=1
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ page_table      │
│  .write()       │  ← Brief exclusive lock
│  insert(42→3)   │
│  remove old     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ frame[3].write()│  ← Copy temp_buf → frame data
│ page_id = 42    │
│ pin_count = 1   │
│ dirty = false   │
│ ref_bit = true  │
└────────┬────────┘
         │
         ▼
  Downgrade to read lock
  Return SharedPageGuard(frame=3)
```

## 2. Page Write Path

```
fetch_page_exclusive(page_id=42)
         │
         ▼
   (same as read path but acquire frame.write())
         │
         ▼
┌─────────────────────────┐
│ ExclusivePageGuard      │
│   .data_mut() → &mut    │  ← Caller modifies page
│   modified = true       │
└────────┬────────────────┘
         │
         ▼ (on Drop)
┌─────────────────────────┐
│ if modified:            │
│   frame.dirty = true    │
│ pin_count -= 1          │
│ Release write lock      │
└────────┬────────────────┘
         │
         ▼ (later, during Checkpoint)
┌─────────────────────────┐
│ Checkpoint::run()       │
│ dirty_pages() snapshot  │──► DWB.write_pages(dirty)
│ mark_clean()            │──► DWB writes → scatter-write → sync
└─────────────────────────┘
```

## 3. B-Tree Insert With Split

```
insert(key=M, value=...)
         │
         ▼
┌──────────────────────────────────┐
│ Traverse from root to leaf       │
│ (latch coupling: shared locks)   │
│                                  │
│ root(internal) → child → leaf    │
└────────────────┬─────────────────┘
                 │
                 ▼
┌──────────────────────────────────┐
│ Acquire leaf EXCLUSIVE           │
│ Try insert into leaf             │
│                                  │
│ ┌────── Has space? ─────┐       │
│ │ YES                   │ NO    │
│ │ Insert at sorted      │       │
│ │ position. Done.       │       │
│ └───────────────────────┘       │
└────────────────┬────────────────┘
                 │ NO (leaf full)
                 ▼
┌──────────────────────────────────────────────────┐
│ SPLIT LEAF                                        │
│                                                   │
│ Before:  leaf[A B C D E F G H]  (full)           │
│          right_sibling = X                        │
│                                                   │
│ 1. Allocate new_page from free_list              │
│ 2. Split after D; E becomes first key of new leaf;│
│    promote E to parent                            │
│ 3. Move upper half to new_page:                  │
│                                                   │
│    old_leaf[A B C D]     new_leaf[E F G H]       │
│    right_sib = new_leaf  right_sib = X           │
│                                                   │
│ 4. Insert M into appropriate leaf                │
│ 5. Promote median_key=E to parent                │
└────────────────┬─────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────┐
│ INSERT INTO PARENT                                │
│                                                   │
│ Acquire parent EXCLUSIVE                          │
│ Insert (median_key=E, child=new_leaf) into parent│
│                                                   │
│ ┌────── Parent has space? ─────┐                 │
│ │ YES → Done                   │ NO → Split      │
│ └──────────────────────────────┘ parent too      │
│                                   (recurse)      │
└────────────────┬─────────────────────────────────┘
                 │ (if root splits)
                 ▼
┌──────────────────────────────────────────────────┐
│ ROOT SPLIT                                        │
│                                                   │
│ 1. Old root becomes regular internal node        │
│ 2. Allocate new_root_page from free_list         │
│ 3. new_root: leftmost_child = old_root           │
│              slot[0] = (median, new_sibling)     │
│ 4. Update self.root_page = new_root_page         │
│ 5. Tree height increased by 1                    │
└──────────────────────────────────────────────────┘
```

## 4. B-Tree Scan (Forward)

```
scan(lower=Included("C"), upper=Excluded("G"), Forward)
         │
         ▼
┌──────────────────────────────────┐
│ 1. Traverse from root to leaf    │
│    containing lower bound "C"    │
│    (shared locks, latch coupling)│
└────────────────┬─────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────┐
│ 2. Binary search leaf for first key >= "C"       │
│                                                   │
│    Leaf page 5: [A, B, C, D]  right_sib=page 8  │
│                       ^                           │
│                    start here (slot 2)            │
└────────────────┬─────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────┐
│ 3. Yield entries while key < "G"                 │
│                                                   │
│    Yield (C, val_c)  ✓                           │
│    Yield (D, val_d)  ✓                           │
│    End of page 5 → follow right_sibling          │
│                                                   │
│    Fetch page 8 (shared):                        │
│    Leaf page 8: [E, F, G, H]  right_sib=page 12 │
│                                                   │
│    Yield (E, val_e)  ✓                           │
│    Yield (F, val_f)  ✓                           │
│    Key G >= upper bound "G" (exclusive) → STOP   │
└──────────────────────────────────────────────────┘

Result: [(C, val_c), (D, val_d), (E, val_e), (F, val_f)]
```

## 5. WAL Write Path (Group Commit)

```
Thread A                Thread B              WAL Writer Task
────────                ────────              ───────────────
append(0x01, data_a)    append(0x01, data_b)
     │                       │
     ▼                       ▼
  encode frame            encode frame
  header + CRC            header + CRC
     │                       │
     ▼                       ▼
  tx.send(req_a)          tx.send(req_b)
     │                       │
     ▼                       ▼
  await oneshot_a         await oneshot_b
                                              ┌──────────────────┐
                                              │ rx.recv()        │
                                              │ → req_a          │
                                              │ rx.try_recv()    │
                                              │ → req_b          │
                                              │ rx.try_recv()    │
                                              │ → None (empty)   │
                                              └────────┬─────────┘
                                                       │
                                              ┌────────▼─────────┐
                                              │ Batch buffer:    │
                                              │ [frame_a bytes]  │
                                              │ [frame_b bytes]  │
                                              └────────┬─────────┘
                                                       │
                                              ┌────────▼─────────┐
                                              │ WalStorage       │
                                              │  .append(batch)  │
                                              └────────┬─────────┘
                                                       │
                                              ┌────────▼─────────┐
                                              │ WalStorage       │
                                              │  .sync()         │
                                              │  ← SINGLE fsync! │
                                              └────────┬─────────┘
                                                       │
                                              ┌────────▼─────────┐
                                              │ Notify:          │
                                              │ oneshot_a ← LSN_a│
                                              │ oneshot_b ← LSN_b│
                                              └──────────────────┘
  ◄── LSN_a               ◄── LSN_b
```

## 6. Checkpoint Flow

```
Checkpoint::run()
     │
     ▼
┌─────────────────────────────────┐
│ checkpoint_lsn =                │
│   wal_writer.current_lsn()     │
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ dirty = buffer_pool             │
│   .dirty_pages()                │ ← Copy data from each dirty frame
│                                 │   (no I/O, just memory copy)
│ Returns: [(pg3, data3, lsn3),  │
│           (pg7, data7, lsn7),  │
│           (pg12, data12, lsn12)]│
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐    ┌─────────────────┐
│ DWB File (data.dwb)             │    │ data.db          │
│                                 │    │                  │
│ Write header + entries  ────────┤    │                  │
│ fsync DWB              ────────┤    │                  │
│                                 │    │                  │
│ Scatter-write:                  │    │                  │
│   pg3  ──────────────────────────────► offset 3*pgsize │
│   pg7  ──────────────────────────────► offset 7*pgsize │
│   pg12 ──────────────────────────────► offset 12*pgsize│
│                                 │    │                  │
│                                 │    │ fsync data.db    │
│ Truncate DWB           ────────┤    │                  │
│ fsync DWB              ────────┤    │                  │
└─────────────────────────────────┘    └─────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ mark_clean(pg3, lsn3)          │
│ mark_clean(pg7, lsn7)          │  ← Only if LSN hasn't changed
│ mark_clean(pg12, lsn12)        │
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ WAL.append(Checkpoint,          │
│   checkpoint_lsn)               │
└─────────────────────────────────┘
```

## 7. Recovery Flow

```
Recovery::run()
     │
     ▼
┌─────────────────────────────────┐
│ 1. checkpoint_lsn from meta.json│
│    (or page 0 file header)      │
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ 2. DWB Recovery                 │
│                                 │
│ If data.dwb exists & non-empty: │
│   Read DWB header               │
│   For each entry:               │
│     Read page from data.db      │
│     If checksum INVALID:        │
│       ──► Restore from DWB      │ ← Fixes torn pages
│     If checksum VALID:          │
│       ──► Skip                  │
│   sync data.db                  │
│   Truncate DWB                  │
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ 3. WAL Replay                   │
│                                 │
│ Open WAL at checkpoint_lsn      │
│ For each record:                │
│   ┌─ Checkpoint? ──► Skip      │
│   └─ Other?                     │
│       ──► handler.handle(rec)   │ ← Higher layer interprets
│                                 │
│ Stop at end-of-log or corrupt   │
└─────────────┬───────────────────┘
              │
              ▼
┌─────────────────────────────────┐
│ 4. Return end_lsn               │
│    (WAL writer starts here)     │
└─────────────────────────────────┘
```

## 8. Free List Allocate

```
free_list.allocate()
         │
         ▼
┌──────────────────────────────────┐
│ head == 0?                       │
├──── YES ─────────────────────────┤
│                                  │
│  ┌───────────────────────┐       │
│  │ page_storage.extend() │       │
│  │ new page_id =         │       │
│  │   old page_count      │       │
│  │ Return new page_id    │       │
│  └───────────────────────┘       │
│                                  │
├──── NO (head != 0) ─────────────┤
│                                  │
│  ┌───────────────────────┐       │
│  │ Fetch head page       │       │
│  │ (exclusive)           │       │
│  │                       │       │
│  │ Read next_free from   │       │
│  │ header.prev_or_ptr    │       │
│  │                       │       │
│  │ old_head = self.head  │       │
│  │ self.head = next_free │       │
│  │                       │       │
│  │ Return old_head       │       │
│  └───────────────────────┘       │
└──────────────────────────────────┘
```

## 9. Heap Overflow Chain

```
SlottedPage (page 20)
┌──────────────────────────────────────────────────────────┐
│ PageHeader                                                │
│  page_type = SLOTTED                                     │
├──────────────────────────────────────────────────────────┤
│ Slot Array:                                               │
│  slot[0] ──────────────┐                                 │
│  slot[1]               │                                 │
│  ...                   │                                 │
├────────────────────────┼─────────────────────────────────┤
│ Cell area:             ▼                                  │
│  ┌─────────────────────────────────────────────┐         │
│  │ HeapRef metadata                             │         │
│  │  total_len: 12000                            │         │
│  │  stored_inline: 3800 bytes (first chunk)     │         │
│  │  next_page: 44  ─────────────────────────────┼────┐   │
│  │                                              │    │   │
│  │ [===== first 3800 bytes of blob data ======] │    │   │
│  └─────────────────────────────────────────────┘    │   │
└──────────────────────────────────────────────────────┘   │
                                                            │
       ┌────────────────────────────────────────────────────┘
       │
       ▼
Overflow Page (page 44)
┌──────────────────────────────────────────────────────────┐
│ PageHeader                                                │
│  page_type = OVERFLOW                                    │
│  next_page: 51  ─────────────────────────────────────┐   │
├──────────────────────────────────────────────────────┤   │
│ data_length: u32                                      │   │
├──────────────────────────────────────────────────────┤   │
│                                                       │   │
│ [========== 4076 bytes of blob data =============]   │   │
│                                                       │   │
└──────────────────────────────────────────────────────┘   │
                                                            │
       ┌────────────────────────────────────────────────────┘
       │
       ▼
Overflow Page (page 51)
┌──────────────────────────────────────────────────────────┐
│ PageHeader                                                │
│  page_type = OVERFLOW                                    │
│  next_page: 0  ← END OF CHAIN                           │
├──────────────────────────────────────────────────────────┤
│ data_length: u32                                          │
├──────────────────────────────────────────────────────────┤
│                                                           │
│ [========== remaining 4124 bytes of blob data ========]  │
│                                                           │
└──────────────────────────────────────────────────────────┘


Load path (read large blob):

  heap_store.load(page=20, slot=0)
           │
           ▼
  ┌─────────────────────────────┐
  │ Read slot[0] cell from      │
  │ slotted page 20             │
  │ → HeapRef { total_len=12000 │
  │   inline chunk, next=44 }   │
  └────────────┬────────────────┘
               │
               ▼
  ┌─────────────────────────────┐
  │ buf = Vec::with_capacity    │
  │         (12000)             │
  │ buf.extend(inline chunk)    │  ← 3800 bytes
  └────────────┬────────────────┘
               │
               ▼
  ┌─────────────────────────────┐
  │ current_page = 44           │
  │ while current_page != 0:    │
  │   fetch page (shared)       │
  │   buf.extend(data chunk)    │  ← +4076 bytes (page 44)
  │   current_page = next_page  │    +4124 bytes (page 51)
  └────────────┬────────────────┘
               │
               ▼
  ┌─────────────────────────────┐
  │ assert buf.len() == 12000   │
  │ Return buf                  │
  └─────────────────────────────┘
```

## 10. Free List Deallocate

```
LIFO push: deallocate page 7

BEFORE:
                ┌──────────┐     ┌──────────┐
  head ──────►  │ Page 5   │────►│ Page 3   │────► 0 (empty)
                │prev_or_  │     │prev_or_  │
                │ptr = 3   │     │ptr = 0   │
                └──────────┘     └──────────┘

STEP 1: Write page 7's prev_or_ptr = 5 (old head)

                ┌──────────┐     ┌──────────┐     ┌──────────┐
  head ──────►  │ Page 5   │────►│ Page 3   │────► 0
                │prev_or_  │     │prev_or_  │
                │ptr = 3   │     │ptr = 0   │
                └──────────┘     └──────────┘
                     ▲
                     │
                ┌──────────┐
                │ Page 7   │
                │prev_or_  │
                │ptr = 5   │
                └──────────┘

STEP 2: Update head = 7

                ┌──────────┐     ┌──────────┐     ┌──────────┐
  head ──────►  │ Page 7   │────►│ Page 5   │────►│ Page 3   │────► 0
                │prev_or_  │     │prev_or_  │     │prev_or_  │
                │ptr = 5   │     │ptr = 3   │     │ptr = 0   │
                └──────────┘     └──────────┘     └──────────┘

AFTER:
  head → page 7 → page 5 → page 3 → 0 (empty)


Code path:

  free_list.deallocate(page_id=7)
           │
           ▼
  ┌──────────────────────────────────┐
  │ Fetch page 7 (exclusive)         │
  │ Write header:                    │
  │   page_type = FREE               │
  │   prev_or_ptr = self.head  (= 5) │
  └─────────────┬────────────────────┘
                │
                ▼
  ┌──────────────────────────────────┐
  │ self.head = 7                    │  ← New head of free list
  └─────────────┬────────────────────┘
                │
                ▼
  ┌──────────────────────────────────┐
  │ Done. Page 7 is now at the      │
  │ front of the free list.         │
  └──────────────────────────────────┘
```
