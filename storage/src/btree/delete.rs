use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError};
use crate::free_list::FreePageList;
use super::node::{BTreeNode, KeySizeHint};

/// Delete a cell from a B-tree by key.
/// Returns the (possibly new) root page ID and whether the key was found.
pub fn btree_delete(
    pool: &BufferPool,
    root_page_id: PageId,
    key: &[u8],
    key_hint: KeySizeHint,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<(PageId, bool), BufferPoolError> {
    // Descend to the leaf.
    let mut path: Vec<PageId> = Vec::new();
    let mut current_page_id = root_page_id;

    loop {
        let guard = pool.fetch_page_shared(current_page_id)?;
        let page = guard.as_slotted_page();
        let header = page.header();

        match header.page_type {
            PageType::BTreeInternal => {
                path.push(current_page_id);
                let child = BTreeNode::find_child(&page, key);
                drop(guard);
                current_page_id = child;
            }
            PageType::BTreeLeaf => {
                drop(guard);
                break;
            }
            _ => return Ok((root_page_id, false)),
        }
    }

    let leaf_page_id = current_page_id;
    let mut guard = pool.fetch_page_exclusive(leaf_page_id)?;

    // Find the key.
    let slot = {
        let sp = guard.as_slotted_page();
        match BTreeNode::leaf_search(&sp, key, key_hint) {
            Ok(s) => s,
            Err(_) => return Ok((root_page_id, false)), // not found
        }
    };

    // Delete the cell.
    {
        let mut sp = guard.as_slotted_page_mut();
        sp.delete_cell(slot);
        sp.compact();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    // For simplicity, we skip merge/redistribute for now.
    // A full production implementation would check for underfull pages
    // and merge or redistribute with siblings.

    // Check if root became empty (single-child internal).
    drop(guard);
    let root_guard = pool.fetch_page_shared(root_page_id)?;
    let root_page = root_guard.as_slotted_page();
    if root_page.header().page_type == PageType::BTreeInternal && root_page.num_slots() == 0 {
        let child = BTreeNode::leftmost_child(&root_page);
        drop(root_guard);
        free_list.push(root_page_id, pool)?;
        return Ok((child, true));
    }

    Ok((root_page_id, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::MemPageIO;
    use crate::page::SlottedPageMut;
    use crate::btree::insert::btree_insert;
    use crate::btree::cursor::BTreeCursor;

    fn make_pool(pages: usize, frames: u32) -> BufferPool {
        let io = Box::new(MemPageIO::new(DEFAULT_PAGE_SIZE, pages));
        BufferPool::new(io, DEFAULT_PAGE_SIZE, frames)
    }

    fn make_cell(doc_num: u8) -> (Vec<u8>, Vec<u8>) {
        let doc_id = DocId(doc_num as u128);
        let key = crate::key_encoding::encode_primary_key(doc_id, Timestamp(1));
        let key_bytes = key.0.clone();
        let mut cell = key.0;
        cell.push(0);
        cell.extend_from_slice(&4u32.to_le_bytes());
        cell.extend_from_slice(b"body");
        (key_bytes, cell)
    }

    #[test]
    fn test_delete_from_leaf() {
        let pool = make_pool(2, 8);
        {
            let io = pool.io();
            let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
            let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
            sp.init(PageId(1), PageType::BTreeLeaf);
            sp.compute_checksum();
            io.write_page(PageId(1), &buf).unwrap();
        }

        let mut fl = FreePageList::new(None);
        let mut root = PageId(1);

        // Insert 5 cells.
        let mut keys = Vec::new();
        for i in 0..5u8 {
            let (key, cell) = make_cell(i * 10);
            keys.push(key);
            root = btree_insert(&pool, root, &cell, KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0)).unwrap();
        }

        // Delete cell 2 (key index 2).
        let (new_root, found) = btree_delete(&pool, root, &keys[2], KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(1)).unwrap();
        assert!(found);

        // Scan and verify 4 remaining.
        let mut cursor = BTreeCursor::new(&pool, new_root, KeySizeHint::Fixed(PRIMARY_KEY_SIZE));
        cursor.seek_first().unwrap();
        let mut count = 0;
        while cursor.is_valid() {
            count += 1;
            cursor.advance().unwrap();
        }
        assert_eq!(count, 4);
    }

    #[test]
    fn test_delete_nonexistent() {
        let pool = make_pool(2, 8);
        {
            let io = pool.io();
            let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
            let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
            sp.init(PageId(1), PageType::BTreeLeaf);
            sp.compute_checksum();
            io.write_page(PageId(1), &buf).unwrap();
        }

        let mut fl = FreePageList::new(None);
        let (_, cell) = make_cell(1);
        let root = btree_insert(&pool, PageId(1), &cell, KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0)).unwrap();

        let nonexistent = vec![0xFFu8; PRIMARY_KEY_SIZE];
        let (_, found) = btree_delete(&pool, root, &nonexistent, KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(1)).unwrap();
        assert!(!found);
    }
}
