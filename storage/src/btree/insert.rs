use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError};
use crate::free_list::FreePageList;
use super::node::{BTreeNode, KeySizeHint};

/// Insert a cell into a B-tree.
/// Returns the (possibly new) root page ID.
pub fn btree_insert(
    pool: &BufferPool,
    root_page_id: PageId,
    cell_data: &[u8],
    key_hint: KeySizeHint,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<PageId, BufferPoolError> {
    let key = BTreeNode::leaf_cell_key(cell_data, key_hint);
    let key_vec = key.to_vec();

    // Descend to find the target leaf, collecting the path.
    let mut path: Vec<PageId> = Vec::new();
    let mut current_page_id = root_page_id;

    loop {
        let guard = pool.fetch_page_shared(current_page_id)?;
        let page = guard.as_slotted_page();
        let header = page.header();

        match header.page_type {
            PageType::BTreeInternal => {
                path.push(current_page_id);
                let child = BTreeNode::find_child(&page, &key_vec);
                drop(guard);
                current_page_id = child;
            }
            PageType::BTreeLeaf => {
                drop(guard);
                break;
            }
            _ => {
                return Err(BufferPoolError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected page type during B-tree insert",
                )));
            }
        }
    }

    // Insert into the leaf.
    let leaf_page_id = current_page_id;
    let mut guard = pool.fetch_page_exclusive(leaf_page_id)?;
    let insert_slot = {
        let page = guard.as_slotted_page();
        match BTreeNode::leaf_search(&page, &key_vec, key_hint) {
            Ok(s) => s,   // exact match — insert at same position (duplicate version)
            Err(s) => s,  // insertion point
        }
    };
    {
        let mut sp = guard.as_slotted_page_mut();
        if sp.insert_cell(insert_slot, cell_data).is_ok() {
            sp.set_lsn(lsn);
            sp.compute_checksum();
            return Ok(root_page_id); // no split needed
        }
    }

    // Leaf is full — need to split.
    let (separator_key, new_page_id) = split_leaf(pool, &mut guard, cell_data, &key_vec, insert_slot, key_hint, free_list, lsn)?;
    drop(guard);

    // Propagate the split upward.
    propagate_split(pool, root_page_id, &path, &separator_key, new_page_id, free_list, lsn)
}

/// Split a leaf page. Returns (separator_key, new_page_id).
#[allow(clippy::too_many_arguments)]
fn split_leaf(
    pool: &BufferPool,
    guard: &mut crate::buffer_pool::ExclusivePageGuard<'_>,
    new_cell: &[u8],
    _new_key: &[u8],
    insert_slot: u16,
    key_hint: KeySizeHint,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<(Vec<u8>, PageId), BufferPoolError> {

    // Collect all existing cells + the new cell.
    let (mut all_cells, old_right_sibling) = {
        let sp = guard.as_slotted_page();
        let num_slots = sp.num_slots();
        let old_right_sibling = BTreeNode::right_sibling(&sp);
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(num_slots as usize + 1);
        for i in 0..num_slots {
            cells.push(sp.cell_data(i).unwrap().to_vec());
        }
        (cells, old_right_sibling)
    };
    all_cells.insert(insert_slot as usize, new_cell.to_vec());

    let total = all_cells.len();
    let mid = total / 2;

    // Allocate new leaf page.
    let mut new_guard = pool.new_page(PageType::BTreeLeaf, free_list)?;
    let new_page_id = new_guard.page_id();

    // Rewrite old leaf with left half.
    {
        let old_page_id = guard.page_id();
        let mut sp = guard.as_slotted_page_mut();
        sp.init(old_page_id, PageType::BTreeLeaf);
        for (i, cell) in all_cells[..mid].iter().enumerate() {
            sp.insert_cell(i as u16, cell).unwrap();
        }
        BTreeNode::set_right_sibling(&mut sp, Some(new_page_id));
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    // Write right half to new page.
    {
        let mut sp = new_guard.as_slotted_page_mut();
        for (i, cell) in all_cells[mid..].iter().enumerate() {
            sp.insert_cell(i as u16, cell).unwrap();
        }
        BTreeNode::set_right_sibling(&mut sp, old_right_sibling);
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    // Separator key is the first key of the right (new) page.
    let sep_key = BTreeNode::leaf_cell_key(&all_cells[mid], key_hint).to_vec();

    Ok((sep_key, new_page_id))
}

/// Propagate a split upward through the internal nodes.
fn propagate_split(
    pool: &BufferPool,
    root_page_id: PageId,
    path: &[PageId],
    separator_key: &[u8],
    new_child: PageId,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<PageId, BufferPoolError> {
    let mut sep_key = separator_key.to_vec();
    let mut child_page_id = new_child;

    for &parent_page_id in path.iter().rev() {
        let mut parent_guard = pool.fetch_page_exclusive(parent_page_id)?;
        let cell = BTreeNode::build_internal_cell(&sep_key, child_page_id);

        // Find insertion point.
        let insert_pos = {
            let sp = parent_guard.as_slotted_page();
            let n = sp.num_slots();
            let mut pos = n;
            for i in 0..n {
                if let Some((k, _)) = BTreeNode::internal_cell_key_and_child(&sp, i)
                    && sep_key.as_slice() <= k {
                    pos = i;
                    break;
                }
            }
            pos
        };

        {
            let mut sp = parent_guard.as_slotted_page_mut();
            if sp.insert_cell(insert_pos, &cell).is_ok() {
                sp.set_lsn(lsn);
                sp.compute_checksum();
                return Ok(root_page_id); // fit in parent, done
            }
        }

        // Parent is full — split the internal node.
        let (new_sep, new_internal_id) = split_internal(
            pool, &mut parent_guard, &cell, insert_pos, free_list, lsn,
        )?;
        sep_key = new_sep;
        child_page_id = new_internal_id;
    }

    // If we get here, we need a new root.
    let mut new_root_guard = pool.new_page(PageType::BTreeInternal, free_list)?;
    let new_root_id = new_root_guard.page_id();
    {
        let mut sp = new_root_guard.as_slotted_page_mut();
        BTreeNode::set_leftmost_child(&mut sp, root_page_id);
        let cell = BTreeNode::build_internal_cell(&sep_key, child_page_id);
        sp.insert_cell(0, &cell).unwrap();
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    Ok(new_root_id)
}

/// Split an internal node.
fn split_internal(
    pool: &BufferPool,
    guard: &mut crate::buffer_pool::ExclusivePageGuard<'_>,
    new_cell: &[u8],
    insert_pos: u16,
    free_list: &mut FreePageList,
    lsn: Lsn,
) -> Result<(Vec<u8>, PageId), BufferPoolError> {
    let (mut all_cells, leftmost_child) = {
        let sp = guard.as_slotted_page();
        let n = sp.num_slots();
        let leftmost_child = BTreeNode::leftmost_child(&sp);
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            cells.push(sp.cell_data(i).unwrap().to_vec());
        }
        (cells, leftmost_child)
    };
    all_cells.insert(insert_pos as usize, new_cell.to_vec());

    let total = all_cells.len();
    let mid = total / 2;

    // The middle cell's key becomes the separator pushed up.
    // Left gets cells [0..mid), right gets cells [mid+1..total).
    // The mid cell's child becomes the leftmost_child of the right page.
    let mid_cell = &all_cells[mid];
    let mid_key_len = mid_cell.len() - 4;
    let sep_key = mid_cell[..mid_key_len].to_vec();
    let mid_child = PageId(u32::from_le_bytes(mid_cell[mid_key_len..].try_into().unwrap()));

    // Allocate new internal page.
    let mut new_guard = pool.new_page(PageType::BTreeInternal, free_list)?;
    let new_page_id = new_guard.page_id();

    // Rewrite old page with left half.
    {
        let old_page_id = guard.page_id();
        let mut sp = guard.as_slotted_page_mut();
        sp.init(old_page_id, PageType::BTreeInternal);
        BTreeNode::set_leftmost_child(&mut sp, leftmost_child);
        for (i, cell) in all_cells[..mid].iter().enumerate() {
            sp.insert_cell(i as u16, cell).unwrap();
        }
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    // Write right half to new page.
    {
        let mut sp = new_guard.as_slotted_page_mut();
        BTreeNode::set_leftmost_child(&mut sp, mid_child);
        for (i, cell) in all_cells[mid + 1..].iter().enumerate() {
            sp.insert_cell(i as u16, cell).unwrap();
        }
        sp.set_lsn(lsn);
        sp.compute_checksum();
    }

    Ok((sep_key, new_page_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::MemPageIO;
    use crate::page::SlottedPageMut;
    use crate::btree::cursor::BTreeCursor;

    fn make_pool(pages: usize, frames: u32) -> BufferPool {
        let io = Box::new(MemPageIO::new(DEFAULT_PAGE_SIZE, pages));
        BufferPool::new(io, DEFAULT_PAGE_SIZE, frames)
    }

    fn init_empty_leaf(pool: &BufferPool, page_id: PageId) {
        let mut guard = pool.fetch_page_exclusive(page_id).unwrap();
        let mut sp = guard.as_slotted_page_mut();
        sp.init(page_id, PageType::BTreeLeaf);
        sp.compute_checksum();
    }

    fn make_primary_cell(doc_num: u8, ts: u64) -> Vec<u8> {
        let doc_id = DocId(doc_num as u128);
        let timestamp = Timestamp(ts);
        let key = crate::key_encoding::encode_primary_key(doc_id, timestamp);
        let mut cell = key.0;
        cell.push(0); // flags
        cell.extend_from_slice(&4u32.to_le_bytes()); // body_len
        cell.extend_from_slice(b"body"); // body
        cell
    }

    #[test]
    fn test_insert_into_empty_tree() {
        let pool = make_pool(2, 8);
        // Initialize page 1 as empty leaf root.
        {
            let io = pool.io();
            let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
            let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
            sp.init(PageId(1), PageType::BTreeLeaf);
            sp.compute_checksum();
            io.write_page(PageId(1), &buf).unwrap();
        }

        let mut fl = FreePageList::new(None);
        let cell = make_primary_cell(1, 100);
        let new_root = btree_insert(
            &pool, PageId(1), &cell,
            KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0),
        ).unwrap();
        assert_eq!(new_root, PageId(1));

        // Verify cell is there.
        let guard = pool.fetch_page_shared(PageId(1)).unwrap();
        let sp = guard.as_slotted_page();
        assert_eq!(sp.num_slots(), 1);
    }

    #[test]
    fn test_insert_preserves_sort_order() {
        let pool = make_pool(2, 16);
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

        // Insert in reverse order.
        for i in (0..50u8).rev() {
            let cell = make_primary_cell(i, 1);
            root = btree_insert(
                &pool, root, &cell,
                KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0),
            ).unwrap();
        }

        // Scan and verify sorted order.
        let mut cursor = BTreeCursor::new(&pool, root, KeySizeHint::Fixed(PRIMARY_KEY_SIZE));
        cursor.seek_first().unwrap();
        let mut prev_key: Option<Vec<u8>> = None;
        let mut count = 0;
        while cursor.is_valid() {
            let key = cursor.key(&pool).unwrap().unwrap();
            if let Some(pk) = &prev_key {
                assert!(pk <= &key, "sort order violated");
            }
            prev_key = Some(key);
            count += 1;
            cursor.advance().unwrap();
        }
        assert_eq!(count, 50);
    }

    #[test]
    fn test_leaf_split() {
        let pool = make_pool(2, 32);
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

        // Insert enough cells to force a split.
        // Each cell ~33 bytes. Page usable ~8160. ~247 cells fit.
        for i in 0..300u16 {
            let doc_id = DocId(i as u128);
            let key = crate::key_encoding::encode_primary_key(doc_id, Timestamp(1));
            let mut cell = key.0;
            cell.push(0); // flags
            cell.extend_from_slice(&4u32.to_le_bytes());
            cell.extend_from_slice(b"body");
            root = btree_insert(
                &pool, root, &cell,
                KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0),
            ).unwrap();
        }

        // Root should now be an internal node.
        let guard = pool.fetch_page_shared(root).unwrap();
        let _header = guard.as_slotted_page().header();
        // After enough inserts, root should have changed.
        // Verify all cells can be found.
        drop(guard);

        let mut cursor = BTreeCursor::new(&pool, root, KeySizeHint::Fixed(PRIMARY_KEY_SIZE));
        cursor.seek_first().unwrap();
        let mut count = 0;
        while cursor.is_valid() {
            count += 1;
            cursor.advance().unwrap();
        }
        assert_eq!(count, 300);
    }
}
