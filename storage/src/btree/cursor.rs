use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError};
use super::node::{BTreeNode, KeySizeHint};

/// A positioned cursor into a B-tree.
pub struct BTreeCursor<'a> {
    pool: &'a BufferPool,
    root_page_id: PageId,
    current_leaf_page_id: Option<PageId>,
    current_slot: u16,
    key_hint: KeySizeHint,
    valid: bool,
}

impl<'a> BTreeCursor<'a> {
    pub fn new(pool: &'a BufferPool, root_page_id: PageId, key_hint: KeySizeHint) -> Self {
        Self {
            pool,
            root_page_id,
            current_leaf_page_id: None,
            current_slot: 0,
            key_hint,
            valid: false,
        }
    }

    /// Seek to the first key >= target.
    pub fn seek(&mut self, target: &[u8]) -> Result<(), BufferPoolError> {
        let mut page_id = self.root_page_id;

        loop {
            let guard = self.pool.fetch_page_shared(page_id)?;
            let page = guard.as_slotted_page();
            let header = page.header();

            match header.page_type {
                PageType::BTreeInternal => {
                    let child = BTreeNode::find_child(&page, target);
                    drop(guard);
                    page_id = child;
                }
                PageType::BTreeLeaf => {
                    let slot = match BTreeNode::leaf_search(&page, target, self.key_hint) {
                        Ok(s) => s,
                        Err(s) => s,
                    };
                    let num_slots = page.num_slots();
                    drop(guard);

                    if slot < num_slots {
                        self.current_leaf_page_id = Some(page_id);
                        self.current_slot = slot;
                        self.valid = true;
                    } else {
                        // Check right sibling.
                        let guard = self.pool.fetch_page_shared(page_id)?;
                        let page = guard.as_slotted_page();
                        let sibling = BTreeNode::right_sibling(&page);
                        drop(guard);

                        if let Some(sib_id) = sibling {
                            self.current_leaf_page_id = Some(sib_id);
                            self.current_slot = 0;
                            // Verify sibling has data.
                            let sib_guard = self.pool.fetch_page_shared(sib_id)?;
                            let sib_page = sib_guard.as_slotted_page();
                            self.valid = sib_page.num_slots() > 0;
                        } else {
                            self.valid = false;
                        }
                    }
                    return Ok(());
                }
                _ => {
                    self.valid = false;
                    return Ok(());
                }
            }
        }
    }

    /// Seek to the first entry.
    pub fn seek_first(&mut self) -> Result<(), BufferPoolError> {
        let mut page_id = self.root_page_id;

        loop {
            let guard = self.pool.fetch_page_shared(page_id)?;
            let page = guard.as_slotted_page();
            let header = page.header();

            match header.page_type {
                PageType::BTreeInternal => {
                    let child = BTreeNode::leftmost_child(&page);
                    drop(guard);
                    page_id = child;
                }
                PageType::BTreeLeaf => {
                    let num_slots = page.num_slots();
                    drop(guard);
                    self.current_leaf_page_id = Some(page_id);
                    self.current_slot = 0;
                    self.valid = num_slots > 0;
                    return Ok(());
                }
                _ => {
                    self.valid = false;
                    return Ok(());
                }
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Read the current cell's key bytes.
    pub fn key(&self, pool: &BufferPool) -> Result<Option<Vec<u8>>, BufferPoolError> {
        if !self.valid {
            return Ok(None);
        }
        let pid = self.current_leaf_page_id.unwrap();
        let guard = pool.fetch_page_shared(pid)?;
        let page = guard.as_slotted_page();
        let cell = page.cell_data(self.current_slot);
        Ok(cell.map(|c| BTreeNode::leaf_cell_key(c, self.key_hint).to_vec()))
    }

    /// Read the current cell's full data.
    pub fn cell_data(&self, pool: &BufferPool) -> Result<Option<Vec<u8>>, BufferPoolError> {
        if !self.valid {
            return Ok(None);
        }
        let pid = self.current_leaf_page_id.unwrap();
        let guard = pool.fetch_page_shared(pid)?;
        let page = guard.as_slotted_page();
        Ok(page.cell_data(self.current_slot).map(|c| c.to_vec()))
    }

    /// Advance to the next cell.
    pub fn advance(&mut self) -> Result<(), BufferPoolError> {
        if !self.valid {
            return Ok(());
        }

        let pid = self.current_leaf_page_id.unwrap();
        let guard = self.pool.fetch_page_shared(pid)?;
        let page = guard.as_slotted_page();
        let num_slots = page.num_slots();

        if self.current_slot + 1 < num_slots {
            self.current_slot += 1;
            return Ok(());
        }

        // End of leaf — follow right sibling.
        let sibling = BTreeNode::right_sibling(&page);
        drop(guard);

        if let Some(sib_id) = sibling {
            let sib_guard = self.pool.fetch_page_shared(sib_id)?;
            let sib_page = sib_guard.as_slotted_page();
            if sib_page.num_slots() > 0 {
                self.current_leaf_page_id = Some(sib_id);
                self.current_slot = 0;
            } else {
                self.valid = false;
            }
        } else {
            self.valid = false;
        }

        Ok(())
    }
}
