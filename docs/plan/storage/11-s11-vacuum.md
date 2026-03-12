# S11: Vacuum

## Purpose

Remove entries from B-trees and reclaim freed pages. Higher layers determine WHICH entries to remove (e.g., old MVCC versions). This layer just performs the removal on raw keys.

## Dependencies

- **S6 (B-Tree)**: BTree.delete() for entry removal
- **S4 (Free List)**: page deallocation when B-tree pages become empty

## Rust Types

```rust
use crate::storage::backend::PageId;
use crate::storage::btree::BTree;
use crate::storage::free_list::FreeList;
use std::sync::Arc;

/// A single entry to vacuum from a B-tree.
pub struct VacuumEntry {
    /// Root page of the B-tree to remove from.
    pub btree_root: PageId,
    /// Key to delete.
    pub key: Vec<u8>,
}

/// Vacuum coordinator.
pub struct VacuumTask {
    buffer_pool: Arc<BufferPool>,
}

impl VacuumTask {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self;

    /// Remove a batch of entries from their respective B-trees.
    /// Returns the number of entries actually removed (some may not exist = idempotent).
    pub async fn remove_entries(
        &self,
        entries: &[VacuumEntry],
        free_list: &mut FreeList,
    ) -> Result<usize>;
}
```

## Implementation Details

### remove_entries()

For each `VacuumEntry`:

1. Open the B-tree at `btree_root`.
2. Call `btree.delete(&entry.key, free_list)`.
3. If delete returns `true`: increment removed count.
4. If delete returns `false`: key was already removed (idempotent, not an error).

### Batch Optimization

Group entries by `btree_root` to avoid repeatedly opening the same B-tree:

```rust
// Group by btree_root
let mut by_tree: HashMap<PageId, Vec<&[u8]>> = HashMap::new();
for entry in entries {
    by_tree.entry(entry.btree_root).or_default().push(&entry.key);
}

// Process each tree
for (root, keys) in by_tree {
    let btree = BTree::open(root, buffer_pool.clone());
    for key in keys {
        if btree.delete(key, free_list).await? {
            removed += 1;
        }
    }
}
```

### Page Reclamation

When `btree.delete()` causes a leaf page to become empty:
- The B-tree's merge/redistribute logic handles removing the page from the tree.
- The page is returned to the free list.
- For v1 (no merge): empty leaf pages remain in the tree. They're harmless but waste space. A future compaction pass could reclaim them.

### Integration with Higher Layers

The vacuum flow from the perspective of the full system:

1. **Layer 5 (Tx Manager)**: Determines `oldest_active_read_ts`.
2. **Layer 3 (DocStore)**: Scans primary B-tree, identifies reclaimable versions, computes keys to delete (both primary and secondary index keys).
3. **Layer 3**: Writes `Vacuum` WAL record with the entries.
4. **Layer 2 (this)**: `VacuumTask.remove_entries()` executes the actual deletions.

The vacuum task itself is generic — it doesn't know about documents, versions, or indexes. It just deletes keys from B-trees.

## Error Handling

| Error | Cause | Handling |
|-------|-------|----------|
| B-tree I/O error | Page read/write failure | Propagate |
| Key not found | Already vacuumed or never existed | Not an error (idempotent) |

## Tests

1. **Remove existing entries**: Insert 100 entries into a B-tree. Vacuum 50. Verify only 50 remain.
2. **Idempotent removal**: Remove an entry, then try to remove it again. No error.
3. **Remove from multiple trees**: Create 3 B-trees. Insert entries in each. Vacuum entries across all trees.
4. **Remove all entries**: Insert entries, vacuum all. B-tree should be empty (scan returns nothing).
5. **Batch grouping**: Vacuum entries from same tree — verify they're processed together efficiently.
6. **Empty batch**: Call remove_entries with empty slice → returns 0, no error.
