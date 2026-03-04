use crate::types::*;
use crate::page::{SlottedPage, SlottedPageMut};

/// Hint for how to parse key length from a cell.
#[derive(Debug, Clone, Copy)]
pub enum KeySizeHint {
    /// Fixed-size key (e.g., primary key: 24 bytes).
    Fixed(usize),
    /// Variable-size key — the entire cell is the key (for secondary indexes).
    VariableWithSuffix,
}

/// B-tree node operations over slotted pages.
pub struct BTreeNode;

impl BTreeNode {
    // --- Internal node operations ---

    /// Read the leftmost child from prev_or_ptr field.
    pub fn leftmost_child(page: &SlottedPage) -> PageId {
        PageId(page.header().prev_or_ptr)
    }

    /// Read a cell from an internal node: returns (key_bytes, child_page_id).
    pub fn internal_cell_key_and_child<'a>(page: &'a SlottedPage<'a>, slot: u16) -> Option<(&'a [u8], PageId)> {
        let cell = page.cell_data(slot)?;
        if cell.len() < 4 {
            return None;
        }
        let key_len = cell.len() - 4;
        let key = &cell[..key_len];
        let child = u32::from_le_bytes(cell[key_len..key_len + 4].try_into().unwrap());
        Some((key, PageId(child)))
    }

    /// Build an internal cell: key || child_page_id.
    pub fn build_internal_cell(key: &[u8], child: PageId) -> Vec<u8> {
        let mut cell = Vec::with_capacity(key.len() + 4);
        cell.extend_from_slice(key);
        cell.extend_from_slice(&child.0.to_le_bytes());
        cell
    }

    /// Find the child page for a given key (binary search on internal node).
    pub fn find_child(page: &SlottedPage, key: &[u8]) -> PageId {
        let n = page.num_slots();
        if n == 0 {
            return Self::leftmost_child(page);
        }

        // Binary search: find the first cell key > search key.
        let mut lo = 0u16;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if let Some((cell_key, _)) = Self::internal_cell_key_and_child(page, mid) {
                if key >= cell_key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            } else {
                break;
            }
        }

        // If lo == 0, key < all cells → leftmost child.
        // If lo == i, key >= cell[i-1] → child from cell[i-1].
        if lo == 0 {
            Self::leftmost_child(page)
        } else {
            let (_, child) = Self::internal_cell_key_and_child(page, lo - 1).unwrap();
            child
        }
    }

    pub fn set_leftmost_child(page: &mut SlottedPageMut, child: PageId) {
        page.set_prev_or_ptr(child.0);
    }

    // --- Leaf node operations ---

    /// Extract the key from a leaf cell.
    pub fn leaf_cell_key(cell: &[u8], key_hint: KeySizeHint) -> &[u8] {
        match key_hint {
            KeySizeHint::Fixed(size) => {
                if cell.len() >= size {
                    &cell[..size]
                } else {
                    cell
                }
            }
            KeySizeHint::VariableWithSuffix => {
                // For secondary index: the entire cell IS the key.
                cell
            }
        }
    }

    /// Binary search for a key in a leaf page.
    /// Returns Ok(slot) for exact match, Err(slot) for insertion point.
    pub fn leaf_search(
        page: &SlottedPage,
        key: &[u8],
        key_hint: KeySizeHint,
    ) -> Result<u16, u16> {
        let n = page.num_slots();
        let mut lo = 0u16;
        let mut hi = n;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if let Some(cell) = page.cell_data(mid) {
                let cell_key = Self::leaf_cell_key(cell, key_hint);
                match cell_key.cmp(key) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Equal => return Ok(mid),
                    std::cmp::Ordering::Greater => hi = mid,
                }
            } else {
                break;
            }
        }
        Err(lo)
    }

    /// Right sibling page ID (from prev_or_ptr in leaf).
    pub fn right_sibling(page: &SlottedPage) -> Option<PageId> {
        let ptr = page.header().prev_or_ptr;
        if ptr == 0 { None } else { Some(PageId(ptr)) }
    }

    /// Set the right sibling pointer.
    pub fn set_right_sibling(page: &mut SlottedPageMut, sibling: Option<PageId>) {
        page.set_prev_or_ptr(sibling.map(|p| p.0).unwrap_or(0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_cell_build_and_parse() {
        let key = &[1u8, 2, 3, 4, 5, 6, 7, 8];
        let cell = BTreeNode::build_internal_cell(key, PageId(42));
        assert_eq!(cell.len(), 12); // 8 key + 4 child

        let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
        let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
        sp.init(PageId(1), PageType::BTreeInternal);
        sp.insert_cell(0, &cell).unwrap();

        let ro = SlottedPage::new(&buf, DEFAULT_PAGE_SIZE);
        let (k, c) = BTreeNode::internal_cell_key_and_child(&ro, 0).unwrap();
        assert_eq!(k, key);
        assert_eq!(c, PageId(42));
    }

    #[test]
    fn test_leaf_search() {
        let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
        let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
        sp.init(PageId(1), PageType::BTreeLeaf);

        // Insert keys in order: 24-byte primary keys.
        for i in 0u8..5 {
            let mut key = vec![0u8; 24];
            key[0] = i * 10;
            // Add some value bytes after the key.
            let mut cell = key.clone();
            cell.extend_from_slice(&[0xFF; 4]); // flags + body_len placeholder
            sp.insert_cell(i as u16, &cell).unwrap();
        }

        let ro = SlottedPage::new(&buf, DEFAULT_PAGE_SIZE);

        // Search for existing key.
        let mut target = vec![0u8; 24];
        target[0] = 20;
        assert_eq!(BTreeNode::leaf_search(&ro, &target, KeySizeHint::Fixed(24)), Ok(2));

        // Search for non-existing key.
        target[0] = 15;
        assert_eq!(BTreeNode::leaf_search(&ro, &target, KeySizeHint::Fixed(24)), Err(2));
    }

    #[test]
    fn test_find_child() {
        let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
        let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
        sp.init(PageId(1), PageType::BTreeInternal);
        BTreeNode::set_leftmost_child(&mut sp, PageId(10));

        // Keys: [20] -> child 11, [40] -> child 12
        let cell1 = BTreeNode::build_internal_cell(&[20u8], PageId(11));
        let cell2 = BTreeNode::build_internal_cell(&[40u8], PageId(12));
        sp.insert_cell(0, &cell1).unwrap();
        sp.insert_cell(1, &cell2).unwrap();

        let ro = SlottedPage::new(&buf, DEFAULT_PAGE_SIZE);

        // Key < 20 → leftmost (10).
        assert_eq!(BTreeNode::find_child(&ro, &[10u8]), PageId(10));
        // Key == 20 → child 11.
        assert_eq!(BTreeNode::find_child(&ro, &[20u8]), PageId(11));
        // Key between 20 and 40 → child 11.
        assert_eq!(BTreeNode::find_child(&ro, &[30u8]), PageId(11));
        // Key >= 40 → child 12.
        assert_eq!(BTreeNode::find_child(&ro, &[40u8]), PageId(12));
        assert_eq!(BTreeNode::find_child(&ro, &[50u8]), PageId(12));
    }
}
