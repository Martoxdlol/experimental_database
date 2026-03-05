//! Vacuum: remove entries from B-trees and reclaim freed pages.
//!
//! Higher layers determine WHICH entries to remove (e.g., old MVCC versions).
//! This module just performs the removal on raw keys. It is generic and has no
//! knowledge of documents, versions, or indexes.

use crate::backend::PageId;
use crate::btree::BTree;
use crate::buffer_pool::BufferPool;
use crate::free_list::FreeList;
use std::collections::HashMap;
use std::io;
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
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        Self { buffer_pool }
    }

    /// Remove a batch of entries from their respective B-trees.
    /// Returns the number of entries actually removed (some may not exist = idempotent).
    pub fn remove_entries(
        &self,
        entries: &[VacuumEntry],
        free_list: &mut FreeList,
    ) -> io::Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        // Group entries by btree_root to avoid repeatedly opening the same B-tree.
        let mut by_tree: HashMap<PageId, Vec<&[u8]>> = HashMap::new();
        for entry in entries {
            by_tree
                .entry(entry.btree_root)
                .or_default()
                .push(&entry.key);
        }

        let mut removed = 0usize;

        // Process each tree.
        for (root, keys) in by_tree {
            let btree = BTree::open(root, self.buffer_pool.clone());
            for key in keys {
                if btree.delete(key, free_list)? {
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{MemoryPageStorage, PageStorage};
    use crate::buffer_pool::{BufferPool, BufferPoolConfig};
    use std::ops::Bound;

    const PAGE_SIZE: usize = 4096;

    fn setup() -> (Arc<BufferPool>, FreeList) {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        // Pre-allocate enough pages for test trees.
        storage.extend(256).unwrap();
        let pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size: PAGE_SIZE,
                frame_count: 128,
            },
            storage,
        ));
        let free_list = FreeList::new(0, pool.clone());
        (pool, free_list)
    }

    /// Helper: collect all (key, value) pairs from a B-tree via a full forward scan.
    fn scan_all(btree: &BTree) -> Vec<(Vec<u8>, Vec<u8>)> {
        use crate::btree::ScanDirection;
        btree
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect::<io::Result<Vec<_>>>()
            .unwrap()
    }

    // ─── Test 1: Remove existing entries ───
    // Insert 100 entries into a B-tree. Vacuum 50. Verify only 50 remain.
    #[test]
    fn remove_existing_entries() {
        let (pool, mut fl) = setup();
        let btree = BTree::create(pool.clone(), &mut fl).unwrap();
        let root = btree.root_page();

        // Insert 100 entries.
        for i in 0u32..100 {
            let key = i.to_be_bytes().to_vec();
            let value = format!("val-{}", i).into_bytes();
            btree.insert(&key, &value, &mut fl).unwrap();
        }

        // Vacuum the first 50 entries.
        let entries: Vec<VacuumEntry> = (0u32..50)
            .map(|i| VacuumEntry {
                btree_root: root,
                key: i.to_be_bytes().to_vec(),
            })
            .collect();

        let task = VacuumTask::new(pool.clone());
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 50);

        // Verify only 50 remain (keys 50..100).
        let remaining = scan_all(&btree);
        assert_eq!(remaining.len(), 50);
        for (i, (key, _value)) in remaining.iter().enumerate() {
            let expected_key = (50u32 + i as u32).to_be_bytes().to_vec();
            assert_eq!(key, &expected_key);
        }
    }

    // ─── Test 2: Idempotent removal ───
    // Remove an entry, then try to remove it again. No error.
    #[test]
    fn idempotent_removal() {
        let (pool, mut fl) = setup();
        let btree = BTree::create(pool.clone(), &mut fl).unwrap();
        let root = btree.root_page();

        let key = b"hello".to_vec();
        btree.insert(&key, b"world", &mut fl).unwrap();

        let entries = vec![VacuumEntry {
            btree_root: root,
            key: key.clone(),
        }];

        let task = VacuumTask::new(pool.clone());

        // First removal: key exists.
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 1);

        // Second removal: key already gone, should not error.
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 0);
    }

    // ─── Test 3: Remove from multiple trees ───
    // Create 3 B-trees. Insert entries in each. Vacuum entries across all trees.
    #[test]
    fn remove_from_multiple_trees() {
        let (pool, mut fl) = setup();

        let btree1 = BTree::create(pool.clone(), &mut fl).unwrap();
        let btree2 = BTree::create(pool.clone(), &mut fl).unwrap();
        let btree3 = BTree::create(pool.clone(), &mut fl).unwrap();

        // Insert 10 entries into each tree.
        for i in 0u32..10 {
            let key = i.to_be_bytes().to_vec();
            btree1.insert(&key, b"t1", &mut fl).unwrap();
            btree2.insert(&key, b"t2", &mut fl).unwrap();
            btree3.insert(&key, b"t3", &mut fl).unwrap();
        }

        // Vacuum entries 0..5 from tree1, 3..7 from tree2, 8..10 from tree3.
        let mut entries = Vec::new();
        for i in 0u32..5 {
            entries.push(VacuumEntry {
                btree_root: btree1.root_page(),
                key: i.to_be_bytes().to_vec(),
            });
        }
        for i in 3u32..7 {
            entries.push(VacuumEntry {
                btree_root: btree2.root_page(),
                key: i.to_be_bytes().to_vec(),
            });
        }
        for i in 8u32..10 {
            entries.push(VacuumEntry {
                btree_root: btree3.root_page(),
                key: i.to_be_bytes().to_vec(),
            });
        }

        let task = VacuumTask::new(pool.clone());
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 5 + 4 + 2);

        // Verify remaining counts.
        assert_eq!(scan_all(&btree1).len(), 5); // 10 - 5
        assert_eq!(scan_all(&btree2).len(), 6); // 10 - 4
        assert_eq!(scan_all(&btree3).len(), 8); // 10 - 2
    }

    // ─── Test 4: Remove all entries ───
    // Insert entries, vacuum all. B-tree should be empty (scan returns nothing).
    #[test]
    fn remove_all_entries() {
        let (pool, mut fl) = setup();
        let btree = BTree::create(pool.clone(), &mut fl).unwrap();
        let root = btree.root_page();

        for i in 0u32..20 {
            let key = i.to_be_bytes().to_vec();
            btree.insert(&key, b"data", &mut fl).unwrap();
        }

        let entries: Vec<VacuumEntry> = (0u32..20)
            .map(|i| VacuumEntry {
                btree_root: root,
                key: i.to_be_bytes().to_vec(),
            })
            .collect();

        let task = VacuumTask::new(pool.clone());
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 20);

        let remaining = scan_all(&btree);
        assert_eq!(remaining.len(), 0);
    }

    // ─── Test 5: Batch grouping ───
    // Vacuum entries from same tree — verify they're processed together efficiently.
    // We verify by inserting entries interleaved from two trees and checking
    // the final state is correct (correct grouping means correct behavior).
    #[test]
    fn batch_grouping() {
        let (pool, mut fl) = setup();
        let btree_a = BTree::create(pool.clone(), &mut fl).unwrap();
        let btree_b = BTree::create(pool.clone(), &mut fl).unwrap();

        for i in 0u32..10 {
            btree_a
                .insert(&i.to_be_bytes(), b"a", &mut fl)
                .unwrap();
            btree_b
                .insert(&i.to_be_bytes(), b"b", &mut fl)
                .unwrap();
        }

        // Interleave entries from both trees.
        let mut entries = Vec::new();
        for i in 0u32..5 {
            entries.push(VacuumEntry {
                btree_root: btree_a.root_page(),
                key: i.to_be_bytes().to_vec(),
            });
            entries.push(VacuumEntry {
                btree_root: btree_b.root_page(),
                key: (i + 5).to_be_bytes().to_vec(),
            });
        }

        let task = VacuumTask::new(pool.clone());
        let removed = task.remove_entries(&entries, &mut fl).unwrap();
        assert_eq!(removed, 10);

        // btree_a should have keys 5..10, btree_b should have keys 0..5.
        let a_remaining = scan_all(&btree_a);
        assert_eq!(a_remaining.len(), 5);
        for (i, (key, _)) in a_remaining.iter().enumerate() {
            assert_eq!(key, &(5u32 + i as u32).to_be_bytes().to_vec());
        }

        let b_remaining = scan_all(&btree_b);
        assert_eq!(b_remaining.len(), 5);
        for (i, (key, _)) in b_remaining.iter().enumerate() {
            assert_eq!(key, &(i as u32).to_be_bytes().to_vec());
        }
    }

    // ─── Test 6: Empty batch ───
    // Call remove_entries with empty slice -> returns 0, no error.
    #[test]
    fn empty_batch() {
        let (pool, mut fl) = setup();
        let task = VacuumTask::new(pool.clone());
        let removed = task.remove_entries(&[], &mut fl).unwrap();
        assert_eq!(removed, 0);
    }
}
