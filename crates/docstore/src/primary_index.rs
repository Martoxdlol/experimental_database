//! D3: Primary Index — clustered B-tree wrapper with MVCC semantics.
//!
//! Adds document/version semantics on top of the raw B-tree: constructs
//! `doc_id || inv_ts` keys, handles tombstones, makes inline-vs-heap decisions.

use crate::key_encoding::{make_primary_key, parse_primary_key};
use crate::version_resolution::{Verdict, VersionResolver};
use exdb_core::types::{DocId, Ts};
use exdb_storage::btree::{ScanDirection, ScanIterator};
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_storage::heap::HeapRef;
use std::ops::Bound;
use std::sync::Arc;

/// Cell flags stored in the first byte of B-tree values.
///
/// bit 0: tombstone (1 = deleted)
/// bit 1: external (1 = body stored in heap)
#[derive(Debug, Clone, Copy)]
pub struct CellFlags {
    pub tombstone: bool,
    pub external: bool,
}

impl CellFlags {
    /// Encode to a single byte.
    pub fn to_byte(self) -> u8 {
        let mut b = 0u8;
        if self.tombstone {
            b |= 0x01;
        }
        if self.external {
            b |= 0x02;
        }
        b
    }

    /// Decode from a single byte.
    pub fn from_byte(b: u8) -> Self {
        Self {
            tombstone: b & 0x01 != 0,
            external: b & 0x02 != 0,
        }
    }
}

/// The clustered primary B-tree wrapper with MVCC semantics.
pub struct PrimaryIndex {
    btree: BTreeHandle,
    engine: Arc<StorageEngine>,
    external_threshold: usize,
}

impl PrimaryIndex {
    /// Create a new PrimaryIndex wrapping a B-tree.
    /// `external_threshold`: body sizes above this are stored in the heap.
    pub fn new(btree: BTreeHandle, engine: Arc<StorageEngine>, external_threshold: usize) -> Self {
        Self {
            btree,
            engine,
            external_threshold,
        }
    }

    /// Insert a new document version at commit_ts.
    ///
    /// `body = None` means tombstone (delete).
    pub fn insert_version(
        &self,
        doc_id: &DocId,
        commit_ts: Ts,
        body: Option<&[u8]>,
    ) -> std::io::Result<()> {
        let key = make_primary_key(doc_id, commit_ts);
        let value = match body {
            None => {
                vec![CellFlags {
                    tombstone: true,
                    external: false,
                }
                .to_byte()]
            }
            Some(data) => {
                if data.len() <= self.external_threshold {
                    // Inline
                    let flags = CellFlags {
                        tombstone: false,
                        external: false,
                    };
                    let mut val = Vec::with_capacity(1 + 4 + data.len());
                    val.push(flags.to_byte());
                    val.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    val.extend_from_slice(data);
                    val
                } else {
                    // External (heap)
                    let href = self.engine.heap_store(data)?;
                    let flags = CellFlags {
                        tombstone: false,
                        external: true,
                    };
                    let mut val = Vec::with_capacity(1 + 4 + 6);
                    val.push(flags.to_byte());
                    val.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    val.extend_from_slice(&href.to_bytes());
                    val
                }
            }
        };
        self.btree.insert(&key, &value)
    }

    /// Get the latest visible version of a document at read_ts.
    ///
    /// Returns None if the document doesn't exist or is a tombstone.
    pub fn get_at_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Vec<u8>>> {
        let seek_key = make_primary_key(doc_id, read_ts);
        let upper_key = make_primary_key(doc_id, 0); // inv_ts(0) = u64::MAX, sorts last
        let iter = self.btree.scan(
            Bound::Included(seek_key.as_slice()),
            Bound::Included(upper_key.as_slice()),
            ScanDirection::Forward,
        );

        for result in iter {
            let (key, value) = result?;
            let (entry_doc_id, entry_ts) = parse_primary_key(&key)
                .map_err(std::io::Error::other)?;
            if entry_doc_id != *doc_id {
                return Ok(None);
            }
            if entry_ts > read_ts {
                continue;
            }
            let flags = CellFlags::from_byte(value[0]);
            if flags.tombstone {
                return Ok(None);
            }
            return self.load_body(&value).map(Some);
        }
        Ok(None)
    }

    /// Get the latest visible version's timestamp.
    ///
    /// Returns None if doc doesn't exist or is a tombstone at read_ts.
    pub fn get_version_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Ts>> {
        let seek_key = make_primary_key(doc_id, read_ts);
        let upper_key = make_primary_key(doc_id, 0);
        let iter = self.btree.scan(
            Bound::Included(seek_key.as_slice()),
            Bound::Included(upper_key.as_slice()),
            ScanDirection::Forward,
        );

        for result in iter {
            let (key, value) = result?;
            let (entry_doc_id, entry_ts) = parse_primary_key(&key)
                .map_err(std::io::Error::other)?;
            if entry_doc_id != *doc_id {
                return Ok(None);
            }
            if entry_ts > read_ts {
                continue;
            }
            let flags = CellFlags::from_byte(value[0]);
            if flags.tombstone {
                return Ok(None);
            }
            return Ok(Some(entry_ts));
        }
        Ok(None)
    }

    /// Scan all visible documents at read_ts.
    pub fn scan_at_ts(&self, read_ts: Ts, direction: ScanDirection) -> PrimaryScanner<'_> {
        let iter = self.btree.scan(Bound::Unbounded, Bound::Unbounded, direction);
        PrimaryScanner {
            inner: iter,
            resolver: VersionResolver::new(read_ts, direction),
            engine: self.engine.clone(),
            btree: &self.btree,
            finished: false,
        }
    }

    /// Access the underlying B-tree handle.
    pub fn btree(&self) -> &BTreeHandle {
        &self.btree
    }

    /// Access the storage engine (for heap operations).
    pub fn engine(&self) -> &Arc<StorageEngine> {
        &self.engine
    }

    /// Load the document body from a B-tree cell value.
    fn load_body(&self, value: &[u8]) -> std::io::Result<Vec<u8>> {
        let flags = CellFlags::from_byte(value[0]);
        debug_assert!(!flags.tombstone, "load_body called on tombstone");

        let body_len = u32::from_le_bytes(value[1..5].try_into().unwrap()) as usize;

        if flags.external {
            let href_bytes: [u8; 6] = value[5..11].try_into().unwrap();
            let href = HeapRef::from_bytes(&href_bytes);
            self.engine.heap_load(href)
        } else {
            Ok(value[5..5 + body_len].to_vec())
        }
    }

}

/// Iterator over visible documents in the primary index.
pub struct PrimaryScanner<'a> {
    inner: ScanIterator,
    resolver: VersionResolver,
    engine: Arc<StorageEngine>,
    btree: &'a BTreeHandle,
    finished: bool,
}

impl Iterator for PrimaryScanner<'_> {
    /// (doc_id, version_ts, body_bytes)
    type Item = std::io::Result<(DocId, Ts, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }

            match self.inner.next() {
                None => {
                    self.finished = true;
                    // Check finish() for backward mode
                    if let Some((doc_id, ts)) = self.resolver.finish() {
                        match self.load_and_check(&doc_id, ts) {
                            Ok(Some(item)) => return Some(Ok(item)),
                            Ok(None) => return None,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    return None;
                }
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok((key, value))) => {
                    let (doc_id, ts) = match parse_primary_key(&key) {
                        Ok(v) => v,
                        Err(e) => return Some(Err(std::io::Error::other(e))),
                    };

                    match self.resolver.process(&doc_id, ts) {
                        Verdict::Skip => continue,
                        Verdict::Visible => {
                            // Forward mode: use value directly
                            let flags = CellFlags::from_byte(value[0]);
                            if flags.tombstone {
                                continue;
                            }
                            let body_len =
                                u32::from_le_bytes(value[1..5].try_into().unwrap()) as usize;
                            let body = if flags.external {
                                let href_bytes: [u8; 6] = value[5..11].try_into().unwrap();
                                let href = HeapRef::from_bytes(&href_bytes);
                                match self.engine.heap_load(href) {
                                    Ok(b) => b,
                                    Err(e) => return Some(Err(e)),
                                }
                            } else {
                                value[5..5 + body_len].to_vec()
                            };
                            return Some(Ok((doc_id, ts, body)));
                        }
                        Verdict::EmitPrevious(prev_id, prev_ts) => {
                            // Backward mode: look up previous group's entry
                            match self.load_and_check(&prev_id, prev_ts) {
                                Ok(Some(item)) => return Some(Ok(item)),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                    }
                }
            }
        }
    }
}

impl PrimaryScanner<'_> {
    fn load_and_check(&self, doc_id: &DocId, ts: Ts) -> std::io::Result<Option<(DocId, Ts, Vec<u8>)>> {
        let key = make_primary_key(doc_id, ts);
        match self.btree.get(&key)? {
            None => Ok(None),
            Some(value) => {
                let flags = CellFlags::from_byte(value[0]);
                if flags.tombstone {
                    return Ok(None);
                }
                let body_len = u32::from_le_bytes(value[1..5].try_into().unwrap()) as usize;
                let body = if flags.external {
                    let href_bytes: [u8; 6] = value[5..11].try_into().unwrap();
                    let href = HeapRef::from_bytes(&href_bytes);
                    self.engine.heap_load(href)?
                } else {
                    value[5..5 + body_len].to_vec()
                };
                Ok(Some((*doc_id, ts, body)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    fn setup() -> (Arc<StorageEngine>, PrimaryIndex) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).unwrap());
        let btree = engine.create_btree().unwrap();
        let pi = PrimaryIndex::new(btree, engine.clone(), 4096);
        (engine, pi)
    }

    #[tokio::test]
    async fn insert_and_get() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).unwrap();
        let body = pi.get_at_ts(&doc_id, 1).unwrap().unwrap();
        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn get_at_future_ts() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).unwrap();
        let body = pi.get_at_ts(&doc_id, 10).unwrap().unwrap();
        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn get_before_insert() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 5, Some(b"hello")).unwrap();
        assert!(pi.get_at_ts(&doc_id, 3).unwrap().is_none());
    }

    #[tokio::test]
    async fn multiple_versions() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).unwrap();
        pi.insert_version(&doc_id, 5, Some(b"v5")).unwrap();
        pi.insert_version(&doc_id, 10, Some(b"v10")).unwrap();

        assert_eq!(pi.get_at_ts(&doc_id, 3).unwrap().unwrap(), b"v1");
        assert_eq!(pi.get_at_ts(&doc_id, 7).unwrap().unwrap(), b"v5");
        assert_eq!(pi.get_at_ts(&doc_id, 15).unwrap().unwrap(), b"v10");
    }

    #[tokio::test]
    async fn tombstone() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).unwrap();
        pi.insert_version(&doc_id, 5, None).unwrap(); // delete

        assert_eq!(pi.get_at_ts(&doc_id, 3).unwrap().unwrap(), b"hello");
        assert!(pi.get_at_ts(&doc_id, 7).unwrap().is_none());
    }

    #[tokio::test]
    async fn tombstone_then_reinsert() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).unwrap();
        pi.insert_version(&doc_id, 5, None).unwrap();
        pi.insert_version(&doc_id, 10, Some(b"v10")).unwrap();

        assert!(pi.get_at_ts(&doc_id, 7).unwrap().is_none());
        assert_eq!(pi.get_at_ts(&doc_id, 12).unwrap().unwrap(), b"v10");
    }

    #[tokio::test]
    async fn get_version_ts() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 5, Some(b"hello")).unwrap();
        assert_eq!(pi.get_version_ts(&doc_id, 10).unwrap(), Some(5));
    }

    #[tokio::test]
    async fn get_version_ts_tombstone() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).unwrap();
        pi.insert_version(&doc_id, 5, None).unwrap();
        assert!(pi.get_version_ts(&doc_id, 7).unwrap().is_none());
    }

    #[tokio::test]
    async fn scan_all_visible() {
        let (_engine, pi) = setup();
        for i in 0..3u8 {
            let mut id = [0u8; 16];
            id[15] = i;
            pi.insert_version(&DocId(id), 1, Some(&[i])).unwrap();
        }
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward).collect();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
        }
    }

    #[tokio::test]
    async fn scan_skips_tombstones() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).unwrap();
        pi.insert_version(&doc_id, 5, None).unwrap();
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward).collect();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn scan_multiple_versions() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).unwrap();
        pi.insert_version(&doc_id, 5, Some(b"v5")).unwrap();
        let results: Vec<_> = pi
            .scan_at_ts(10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 5); // version ts
        assert_eq!(results[0].2, b"v5");
    }

    #[tokio::test]
    async fn cell_flags_roundtrip() {
        for tombstone in [false, true] {
            for external in [false, true] {
                let f = CellFlags {
                    tombstone,
                    external,
                };
                let decoded = CellFlags::from_byte(f.to_byte());
                assert_eq!(decoded.tombstone, tombstone);
                assert_eq!(decoded.external, external);
            }
        }
    }

    #[tokio::test]
    async fn empty_index() {
        let (_engine, pi) = setup();
        let doc_id = DocId([1; 16]);
        assert!(pi.get_at_ts(&doc_id, 10).unwrap().is_none());
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward).collect();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn non_existent_doc() {
        let (_engine, pi) = setup();
        let doc_a = DocId([1; 16]);
        let doc_b = DocId([2; 16]);
        pi.insert_version(&doc_a, 1, Some(b"hello")).unwrap();
        assert!(pi.get_at_ts(&doc_b, 10).unwrap().is_none());
    }

    #[tokio::test]
    async fn external_storage() {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).unwrap());
        let btree = engine.create_btree().unwrap();
        // Very low threshold to force external storage
        let pi = PrimaryIndex::new(btree, engine.clone(), 10);
        let doc_id = DocId([1; 16]);
        let big_body = vec![0xAB; 100];
        pi.insert_version(&doc_id, 1, Some(&big_body)).unwrap();
        let loaded = pi.get_at_ts(&doc_id, 1).unwrap().unwrap();
        assert_eq!(loaded, big_body);
    }

    #[tokio::test]
    async fn many_documents() {
        let (_engine, pi) = setup();
        for i in 0..100u32 {
            let mut id = [0u8; 16];
            id[12..16].copy_from_slice(&i.to_be_bytes());
            pi.insert_version(&DocId(id), 1, Some(&i.to_le_bytes()))
                .unwrap();
        }
        for i in 0..100u32 {
            let mut id = [0u8; 16];
            id[12..16].copy_from_slice(&i.to_be_bytes());
            let body = pi.get_at_ts(&DocId(id), 10).unwrap().unwrap();
            assert_eq!(body, i.to_le_bytes());
        }
    }
}
