use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError};
use super::node::KeySizeHint;
use super::cursor::BTreeCursor;

/// Scan bound for range scans.
#[derive(Debug, Clone)]
pub enum ScanBound {
    Excluded(Vec<u8>),
    Unbounded,
}

/// Iterator over a B-tree range [lower, upper).
pub struct BTreeScan<'a> {
    cursor: BTreeCursor<'a>,
    upper_bound: ScanBound,
    pool: &'a BufferPool,
    #[allow(dead_code)]
    key_hint: KeySizeHint,
    exhausted: bool,
}

impl<'a> BTreeScan<'a> {
    pub fn new(
        pool: &'a BufferPool,
        root_page_id: PageId,
        lower: &[u8],
        upper: ScanBound,
        key_hint: KeySizeHint,
    ) -> Result<Self, BufferPoolError> {
        let mut cursor = BTreeCursor::new(pool, root_page_id, key_hint);
        if lower.is_empty() {
            cursor.seek_first()?;
        } else {
            cursor.seek(lower)?;
        }
        Ok(Self {
            cursor,
            upper_bound: upper,
            pool,
            key_hint,
            exhausted: false,
        })
    }

    /// Get the next cell in the scan.
    /// Returns (key, cell_data).
    #[allow(clippy::type_complexity)]
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BufferPoolError> {
        if self.exhausted || !self.cursor.is_valid() {
            return Ok(None);
        }

        let key = match self.cursor.key(self.pool)? {
            Some(k) => k,
            None => {
                self.exhausted = true;
                return Ok(None);
            }
        };

        // Check upper bound.
        match &self.upper_bound {
            ScanBound::Excluded(bound) => {
                if key.as_slice() >= bound.as_slice() {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
            ScanBound::Unbounded => {}
        }

        let cell = self.cursor.cell_data(self.pool)?.unwrap();
        self.cursor.advance()?;

        Ok(Some((key, cell)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::MemPageIO;
    use crate::page::SlottedPageMut;
    use crate::free_list::FreePageList;
    use crate::btree::insert::btree_insert;

    fn make_pool(pages: usize, frames: u32) -> BufferPool {
        let io = Box::new(MemPageIO::new(DEFAULT_PAGE_SIZE, pages));
        BufferPool::new(io, DEFAULT_PAGE_SIZE, frames)
    }

    fn make_cell(doc_num: u8) -> Vec<u8> {
        let doc_id = DocId(doc_num as u128);
        let key = crate::key_encoding::encode_primary_key(doc_id, Timestamp(1));
        let mut cell = key.0;
        cell.push(0);
        cell.extend_from_slice(&4u32.to_le_bytes());
        cell.extend_from_slice(b"body");
        cell
    }

    #[test]
    fn test_scan_range() {
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

        // Insert keys 0..5
        for i in 0..5u8 {
            let cell = make_cell(i);
            root = btree_insert(&pool, root, &cell, KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0)).unwrap();
        }

        // Full scan.
        let mut scan = BTreeScan::new(&pool, root, &[], ScanBound::Unbounded, KeySizeHint::Fixed(PRIMARY_KEY_SIZE)).unwrap();
        let mut count = 0;
        while scan.next_entry().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn test_scan_bounded() {
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

        for i in 0..10u8 {
            let cell = make_cell(i);
            root = btree_insert(&pool, root, &cell, KeySizeHint::Fixed(PRIMARY_KEY_SIZE), &mut fl, Lsn(0)).unwrap();
        }

        // Scan from key(3) to key(7) exclusive.
        let lower = crate::key_encoding::encode_primary_key(DocId(3), Timestamp(1));
        let upper = crate::key_encoding::encode_primary_key(DocId(7), Timestamp(1));
        let mut scan = BTreeScan::new(
            &pool, root, &lower.0,
            ScanBound::Excluded(upper.0),
            KeySizeHint::Fixed(PRIMARY_KEY_SIZE),
        ).unwrap();

        let mut count = 0;
        while scan.next_entry().unwrap().is_some() {
            count += 1;
        }
        // doc_ids 3, 4, 5, 6 → 4 entries
        assert_eq!(count, 4);
    }
}
