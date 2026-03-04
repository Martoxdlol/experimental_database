# Latch Protocol

## Purpose

Define the locking hierarchy and rules to prevent deadlocks in the storage engine. All frame locks use `parking_lot::RwLock` (synchronous, not async).

## Latch Hierarchy (Strict Ordering)

```
Level 1 (outermost):  page_table: RwLock<HashMap<PageId, FrameId>>
Level 2:              frame[i].lock: RwLock<FrameData>
Level 3 (innermost):  free_list: Mutex<FreeList>
                      heap: Mutex<Heap>
                      file_header: Mutex<FileHeader>
```

**Rule 1**: Never acquire a higher-level lock while holding a lower-level lock.
- Never acquire page_table while holding a frame lock.
- Never acquire a frame lock while holding free_list/heap/file_header mutex.

**Rule 2**: page_table lock is held briefly — never during I/O.
- Acquire page_table.read() → lookup frame_id → release.
- Acquire page_table.write() → insert/remove mapping → release.
- NEVER hold page_table while reading from disk or waiting on I/O.

## Multiple Frame Locks

**Rule 3**: When acquiring locks on multiple frames simultaneously, always acquire in **ascending page_id order**.

This prevents A-B / B-A deadlocks between concurrent operations.

Example (B-tree split needs parent + child):
```
// CORRECT:
if parent_page_id < child_page_id {
    let parent = pool.fetch_page_exclusive(parent_page_id)?;
    let child = pool.fetch_page_exclusive(child_page_id)?;
} else {
    let child = pool.fetch_page_exclusive(child_page_id)?;
    let parent = pool.fetch_page_exclusive(parent_page_id)?;
}

// WRONG: always parent first regardless of page_id
```

## No Latches Across I/O

**Rule 4**: Never hold a frame RwLock while performing disk I/O or awaiting an async operation.

The pattern for loading a page on cache miss:

```rust
// 1. Check cache (brief read lock on page_table)
let frame_id = {
    let pt = self.page_table.read();
    pt.get(&page_id).copied()
};

if let Some(frame_id) = frame_id {
    // Cache hit — acquire frame lock directly
    return Ok(SharedPageGuard::new(&self.frames[frame_id]));
}

// 2. Read from disk — NO LOCKS HELD
let mut temp_buf = vec![0u8; self.page_size];
self.page_storage.read_page(page_id, &mut temp_buf)?;

// 3. Find/evict frame and install (brief write lock on page_table)
let frame_id = self.find_victim()?;
{
    let mut pt = self.page_table.write();
    // Double-check another thread didn't load it
    if let Some(&existing) = pt.get(&page_id) {
        // Race: someone else loaded it. Use theirs.
        return Ok(SharedPageGuard::new(&self.frames[existing]));
    }
    pt.insert(page_id, frame_id);
}

// 4. Copy data into frame (frame write lock)
{
    let mut frame = self.frames[frame_id].lock.write();
    frame.data.copy_from_slice(&temp_buf);
    frame.page_id = Some(page_id);
    frame.pin_count = 1;
    frame.dirty = false;
    frame.ref_bit = true;
}

// 5. Downgrade to read lock for SharedPageGuard
Ok(SharedPageGuard::new(&self.frames[frame_id]))
```

## B-Tree Latch Coupling (Crab Protocol)

For B-tree traversal, we use **latch coupling** (also called "crab protocol"):

### Read Traversal (get, scan)

```
1. Acquire SHARED lock on root page
2. While current is internal node:
   a. Find child page_id for the search key
   b. Acquire SHARED lock on child page
   c. Release SHARED lock on current page (parent)
   d. current = child
3. At leaf: search for key, read data
4. Release SHARED lock on leaf
```

At most ONE page is latched at a time during reads.

### Write Traversal (insert)

```
1. Acquire EXCLUSIVE lock on root page
2. If root is "safe" (won't split): release root, re-acquire as shared,
   then proceed like a read traversal until the leaf, but acquire leaf exclusive.

   Optimistic approach:
   a. Traverse from root acquiring SHARED locks (like a read)
   b. At leaf, acquire EXCLUSIVE lock
   c. If leaf has space → insert, done
   d. If leaf needs split → restart with pessimistic approach

   Pessimistic approach (after split detected):
   a. Acquire EXCLUSIVE on root
   b. At each level:
      - If current node is "safe" → release all ancestor exclusive locks
      - Acquire EXCLUSIVE on child
      - Move down
   c. At leaf: perform split, propagate up through held exclusive locks
```

**Safe node**: a node that has enough free space that an insert won't cause a split. If we know the node is safe, we can release all ancestor latches because even if we split below, the split won't propagate past this safe node.

### Delete Traversal

Similar to insert but "safe" means the node has enough entries that a merge won't be triggered (if we implement merging — for v1 we skip merging, so delete traversal is like a read traversal with exclusive lock on the leaf).

## Single-Writer Simplification

Since the database uses a **single-writer model** (Section 5.10 of DESIGN.md):

- Only ONE write operation executes at a time.
- Multiple read operations can execute concurrently.
- Writers and readers can execute concurrently on DIFFERENT pages.

This means:
- No writer-writer deadlocks possible.
- The crab protocol is mostly about writer-reader coordination.
- The writer can take a simpler approach: hold exclusive lock on the path from root to leaf, without worrying about concurrent writers.

## Summary of Rules

| # | Rule | Reason |
|---|------|--------|
| 1 | page_table before frame locks | Hierarchy prevents deadlock |
| 2 | page_table held briefly, never during I/O | Prevents blocking all page access |
| 3 | Multiple frames: ascending page_id order | Prevents A-B/B-A deadlock |
| 4 | No frame locks across I/O or await | Prevents async deadlocks |
| 5 | B-tree: latch coupling (one or two pages at a time) | Minimizes lock duration |
| 6 | All frame locks: parking_lot::RwLock (sync) | Compatible with single-threaded async runtime |
