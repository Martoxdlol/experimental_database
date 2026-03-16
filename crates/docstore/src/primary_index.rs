//! D3: Primary Index — clustered B-tree wrapper with MVCC semantics.
//!
//! Adds document/version semantics on top of the raw B-tree: constructs
//! `doc_id || inv_ts` keys, handles tombstones, makes inline-vs-heap decisions.

use crate::key_encoding::{make_primary_key, parse_primary_key};
use crate::version_resolution::{Verdict, VersionResolver};
use exdb_core::types::{DocId, Ts};
use exdb_storage::btree::ScanDirection;
use exdb_storage::engine::{BTreeHandle, StorageEngine};
use exdb_storage::heap::HeapRef;
use futures_core::Stream;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::StreamExt;

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

/// Stream type for primary index scans.
pub type PrimaryScanStream<'a> = Pin<Box<dyn Stream<Item = std::io::Result<(DocId, Ts, Vec<u8>)>> + Send + 'a>>;

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
    pub async fn insert_version(
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
                    let href = self.engine.heap_store(data).await?;
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
        self.btree.insert(&key, &value).await
    }

    /// Get the latest visible version of a document at read_ts.
    ///
    /// Returns None if the document doesn't exist or is a tombstone.
    pub async fn get_at_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Vec<u8>>> {
        let seek_key = make_primary_key(doc_id, read_ts);
        let upper_key = make_primary_key(doc_id, 0); // inv_ts(0) = u64::MAX, sorts last
        let mut iter = self.btree.scan(
            Bound::Included(seek_key.as_slice()),
            Bound::Included(upper_key.as_slice()),
            ScanDirection::Forward,
        );

        while let Some(result) = iter.next().await {
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
            return self.load_body(&value).await.map(Some);
        }
        Ok(None)
    }

    /// Get the latest visible version's timestamp.
    ///
    /// Returns None if doc doesn't exist or is a tombstone at read_ts.
    pub async fn get_version_ts(&self, doc_id: &DocId, read_ts: Ts) -> std::io::Result<Option<Ts>> {
        let seek_key = make_primary_key(doc_id, read_ts);
        let upper_key = make_primary_key(doc_id, 0);
        let mut iter = self.btree.scan(
            Bound::Included(seek_key.as_slice()),
            Bound::Included(upper_key.as_slice()),
            ScanDirection::Forward,
        );

        while let Some(result) = iter.next().await {
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
    pub fn scan_at_ts(&self, read_ts: Ts, direction: ScanDirection) -> PrimaryScanStream<'_> {
        let engine = self.engine.clone();
        let btree = &self.btree;
        Box::pin(async_stream::try_stream! {
            let mut inner = btree.scan(Bound::Unbounded, Bound::Unbounded, direction);
            let mut resolver = VersionResolver::new(read_ts, direction);

            while let Some(result) = inner.next().await {
                let (key, value) = result?;
                let (doc_id, ts) = parse_primary_key(&key)
                    .map_err(std::io::Error::other)?;

                match resolver.process(&doc_id, ts) {
                    Verdict::Skip => continue,
                    Verdict::Visible => {
                        let flags = CellFlags::from_byte(value[0]);
                        if flags.tombstone {
                            continue;
                        }
                        let body = load_body_from_value(&engine, &value).await?;
                        yield (doc_id, ts, body);
                    }
                    Verdict::EmitPrevious(prev_id, prev_ts) => {
                        match load_and_check(btree, &engine, &prev_id, prev_ts).await? {
                            Some(item) => yield item,
                            None => continue,
                        }
                    }
                }
            }

            // Check finish() for backward mode
            #[allow(clippy::collapsible_if)]
            if let Some((doc_id, ts)) = resolver.finish() {
                if let Some(item) = load_and_check(btree, &engine, &doc_id, ts).await? {
                    yield item;
                }
            }
        })
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
    async fn load_body(&self, value: &[u8]) -> std::io::Result<Vec<u8>> {
        load_body_from_value(&self.engine, value).await
    }

}

/// Extract the body from a cell value. Returns Ok(body_bytes) for inline,
/// or the HeapRef for external storage. This is a sync helper to avoid
/// holding !Send types in async generators.
fn decode_cell_body(value: &[u8]) -> std::io::Result<Result<Vec<u8>, HeapRef>> {
    if value.len() < 5 {
        return Err(std::io::Error::other("cell value too short for header"));
    }
    let flags = CellFlags::from_byte(value[0]);
    debug_assert!(!flags.tombstone, "decode_cell_body called on tombstone");

    let body_len = u32::from_le_bytes(value[1..5].try_into().expect("bounds checked above")) as usize;

    if flags.external {
        if value.len() < 11 {
            return Err(std::io::Error::other("cell value too short for external ref"));
        }
        let href_bytes: [u8; 6] = value[5..11].try_into().expect("bounds checked above");
        let href = HeapRef::from_bytes(&href_bytes);
        Ok(Err(href)) // Err variant = external, needs heap_load
    } else {
        if value.len() < 5 + body_len {
            return Err(std::io::Error::other("cell value too short for inline body"));
        }
        Ok(Ok(value[5..5 + body_len].to_vec())) // Ok variant = inline
    }
}

/// Load body from a cell value — async version for use outside streams.
async fn load_body_from_value(engine: &StorageEngine, value: &[u8]) -> std::io::Result<Vec<u8>> {
    match decode_cell_body(value)? {
        Ok(inline) => Ok(inline),
        Err(href) => engine.heap_load(href).await,
    }
}

/// Resolve a body from decoded cell, loading from heap if external.
async fn resolve_body(engine: &StorageEngine, value: &[u8]) -> std::io::Result<Vec<u8>> {
    match decode_cell_body(value)? {
        Ok(inline) => Ok(inline),
        Err(href) => engine.heap_load(href).await,
    }
}

/// Free async function for backward-mode load-and-check, usable inside streams.
async fn load_and_check(
    btree: &BTreeHandle,
    engine: &StorageEngine,
    doc_id: &DocId,
    ts: Ts,
) -> std::io::Result<Option<(DocId, Ts, Vec<u8>)>> {
    let key = make_primary_key(doc_id, ts);
    match btree.get(&key).await? {
        None => Ok(None),
        Some(value) => {
            let flags = CellFlags::from_byte(value[0]);
            if flags.tombstone {
                return Ok(None);
            }
            let body = resolve_body(engine, &value).await?;
            Ok(Some((*doc_id, ts, body)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    async fn setup() -> (Arc<StorageEngine>, PrimaryIndex) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let pi = PrimaryIndex::new(btree, engine.clone(), 4096);
        (engine, pi)
    }

    #[tokio::test]
    async fn insert_and_get() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).await.unwrap();
        let body = pi.get_at_ts(&doc_id, 1).await.unwrap().unwrap();
        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn get_at_future_ts() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).await.unwrap();
        let body = pi.get_at_ts(&doc_id, 10).await.unwrap().unwrap();
        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn get_before_insert() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 5, Some(b"hello")).await.unwrap();
        assert!(pi.get_at_ts(&doc_id, 3).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn multiple_versions() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&doc_id, 5, Some(b"v5")).await.unwrap();
        pi.insert_version(&doc_id, 10, Some(b"v10")).await.unwrap();

        assert_eq!(pi.get_at_ts(&doc_id, 3).await.unwrap().unwrap(), b"v1");
        assert_eq!(pi.get_at_ts(&doc_id, 7).await.unwrap().unwrap(), b"v5");
        assert_eq!(pi.get_at_ts(&doc_id, 15).await.unwrap().unwrap(), b"v10");
    }

    #[tokio::test]
    async fn tombstone() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).await.unwrap();
        pi.insert_version(&doc_id, 5, None).await.unwrap(); // delete

        assert_eq!(pi.get_at_ts(&doc_id, 3).await.unwrap().unwrap(), b"hello");
        assert!(pi.get_at_ts(&doc_id, 7).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tombstone_then_reinsert() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&doc_id, 5, None).await.unwrap();
        pi.insert_version(&doc_id, 10, Some(b"v10")).await.unwrap();

        assert!(pi.get_at_ts(&doc_id, 7).await.unwrap().is_none());
        assert_eq!(pi.get_at_ts(&doc_id, 12).await.unwrap().unwrap(), b"v10");
    }

    #[tokio::test]
    async fn get_version_ts() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 5, Some(b"hello")).await.unwrap();
        assert_eq!(pi.get_version_ts(&doc_id, 10).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn get_version_ts_tombstone() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).await.unwrap();
        pi.insert_version(&doc_id, 5, None).await.unwrap();
        assert!(pi.get_version_ts(&doc_id, 7).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scan_all_visible() {
        let (_engine, pi) = setup().await;
        for i in 0..3u8 {
            let mut id = [0u8; 16];
            id[15] = i;
            pi.insert_version(&DocId(id), 1, Some(&[i])).await.unwrap();
        }
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
        }
    }

    #[tokio::test]
    async fn scan_skips_tombstones() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"hello")).await.unwrap();
        pi.insert_version(&doc_id, 5, None).await.unwrap();
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn scan_multiple_versions() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&doc_id, 5, Some(b"v5")).await.unwrap();
        let results: Vec<_> = pi
            .scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
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
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        assert!(pi.get_at_ts(&doc_id, 10).await.unwrap().is_none());
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn non_existent_doc() {
        let (_engine, pi) = setup().await;
        let doc_a = DocId([1; 16]);
        let doc_b = DocId([2; 16]);
        pi.insert_version(&doc_a, 1, Some(b"hello")).await.unwrap();
        assert!(pi.get_at_ts(&doc_b, 10).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn external_storage() {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        // Very low threshold to force external storage
        let pi = PrimaryIndex::new(btree, engine.clone(), 10);
        let doc_id = DocId([1; 16]);
        let big_body = vec![0xAB; 100];
        pi.insert_version(&doc_id, 1, Some(&big_body)).await.unwrap();
        let loaded = pi.get_at_ts(&doc_id, 1).await.unwrap().unwrap();
        assert_eq!(loaded, big_body);
    }

    #[tokio::test]
    async fn many_documents() {
        let (_engine, pi) = setup().await;
        for i in 0..100u32 {
            let mut id = [0u8; 16];
            id[12..16].copy_from_slice(&i.to_be_bytes());
            pi.insert_version(&DocId(id), 1, Some(&i.to_le_bytes()))
                .await.unwrap();
        }
        for i in 0..100u32 {
            let mut id = [0u8; 16];
            id[12..16].copy_from_slice(&i.to_be_bytes());
            let body = pi.get_at_ts(&DocId(id), 10).await.unwrap().unwrap();
            assert_eq!(body, i.to_le_bytes());
        }
    }

    // ─── Backward scan tests ───

    #[tokio::test]
    async fn backward_scan_basic() {
        let (_engine, pi) = setup().await;
        for i in 0..3u8 {
            let mut id = [0u8; 16];
            id[15] = i;
            pi.insert_version(&DocId(id), 1, Some(&[i])).await.unwrap();
        }
        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Backward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 3);
        // Backward: highest doc_id first
        assert_eq!(results[0].0 .0[15], 2);
        assert_eq!(results[2].0 .0[15], 0);
    }

    #[tokio::test]
    async fn backward_scan_with_versions() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&doc_id, 5, Some(b"v5")).await.unwrap();
        pi.insert_version(&doc_id, 10, Some(b"v10")).await.unwrap();

        // At ts=7: should see v5
        let results: Vec<_> = pi.scan_at_ts(7, ScanDirection::Backward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 5);
        assert_eq!(results[0].2, b"v5");
    }

    #[tokio::test]
    async fn backward_scan_skips_tombstones() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 1, Some(b"hello")).await.unwrap();
        pi.insert_version(&id, 5, None).await.unwrap();

        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Backward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert!(results.is_empty());
    }

    // ─── get_at_ts boundary tests ───

    #[tokio::test]
    async fn get_at_ts_zero() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 0, Some(b"at_zero")).await.unwrap();
        assert_eq!(pi.get_at_ts(&doc_id, 0).await.unwrap().unwrap(), b"at_zero");
    }

    #[tokio::test]
    async fn get_at_ts_max() {
        let (_engine, pi) = setup().await;
        let doc_id = DocId([1; 16]);
        pi.insert_version(&doc_id, 5, Some(b"data")).await.unwrap();
        assert_eq!(pi.get_at_ts(&doc_id, u64::MAX).await.unwrap().unwrap(), b"data");
    }

    // ─── Multiple tombstones ───

    #[tokio::test]
    async fn multiple_tombstones() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&id, 3, None).await.unwrap();
        pi.insert_version(&id, 5, Some(b"v5")).await.unwrap();
        pi.insert_version(&id, 7, None).await.unwrap();
        pi.insert_version(&id, 9, Some(b"v9")).await.unwrap();

        assert_eq!(pi.get_at_ts(&id, 2).await.unwrap().unwrap(), b"v1");
        assert!(pi.get_at_ts(&id, 4).await.unwrap().is_none());
        assert_eq!(pi.get_at_ts(&id, 6).await.unwrap().unwrap(), b"v5");
        assert!(pi.get_at_ts(&id, 8).await.unwrap().is_none());
        assert_eq!(pi.get_at_ts(&id, 10).await.unwrap().unwrap(), b"v9");
    }

    // ─── Scan with various read_ts ───

    #[tokio::test]
    async fn scan_time_travel() {
        let (_engine, pi) = setup().await;

        // doc A: created at ts=1
        let doc_a = DocId([0; 16]);
        pi.insert_version(&doc_a, 1, Some(b"a")).await.unwrap();

        // doc B: created at ts=5
        let mut id_b = [0u8; 16];
        id_b[15] = 1;
        let doc_b = DocId(id_b);
        pi.insert_version(&doc_b, 5, Some(b"b")).await.unwrap();

        // ts=0: nothing
        let r: Vec<_> = pi.scan_at_ts(0, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 0);

        // ts=3: only A
        let r: Vec<_> = pi.scan_at_ts(3, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 1);

        // ts=10: both
        let r: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 2);
    }

    // ─── External storage in scan ───

    #[tokio::test]
    async fn external_storage_in_scan() {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let pi = PrimaryIndex::new(btree, engine.clone(), 10); // low threshold

        let big = vec![0xCD; 200];
        for i in 0..3u8 {
            let mut id = [0u8; 16];
            id[15] = i;
            pi.insert_version(&DocId(id), 1, Some(&big)).await.unwrap();
        }

        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(r.2, big);
        }
    }

    // ─── CellFlags: reserved bits preserved ───

    #[tokio::test]
    async fn cell_flags_reserved_bits() {
        // Higher bits don't affect tombstone/external
        let f = CellFlags::from_byte(0b11111100);
        assert!(!f.tombstone);
        assert!(!f.external);

        let f = CellFlags::from_byte(0b11111111);
        assert!(f.tombstone);
        assert!(f.external);
    }

    // ─── get_version_ts boundary ───

    #[tokio::test]
    async fn get_version_ts_before_any_version() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 5, Some(b"data")).await.unwrap();
        assert!(pi.get_version_ts(&id, 3).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_version_ts_exact_match() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 5, Some(b"data")).await.unwrap();
        assert_eq!(pi.get_version_ts(&id, 5).await.unwrap(), Some(5));
    }

    #[tokio::test]
    async fn get_version_ts_multiple_versions() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 1, Some(b"v1")).await.unwrap();
        pi.insert_version(&id, 5, Some(b"v5")).await.unwrap();
        pi.insert_version(&id, 10, Some(b"v10")).await.unwrap();

        assert_eq!(pi.get_version_ts(&id, 3).await.unwrap(), Some(1));
        assert_eq!(pi.get_version_ts(&id, 7).await.unwrap(), Some(5));
        assert_eq!(pi.get_version_ts(&id, 10).await.unwrap(), Some(10));
        assert_eq!(pi.get_version_ts(&id, 100).await.unwrap(), Some(10));
    }

    // ─── Empty body (zero-length document) ───

    #[tokio::test]
    async fn empty_body_insert_get() {
        let (_engine, pi) = setup().await;
        let id = DocId([1; 16]);
        pi.insert_version(&id, 1, Some(b"")).await.unwrap();
        let body = pi.get_at_ts(&id, 5).await.unwrap().unwrap();
        assert!(body.is_empty());
    }

    // ─── Scan ordering: doc_ids are sorted ───

    #[tokio::test]
    async fn scan_doc_ordering() {
        let (_engine, pi) = setup().await;
        let ids: Vec<DocId> = (0..20u8).map(|i| {
            let mut id = [0u8; 16];
            id[15] = i;
            DocId(id)
        }).collect();

        // Insert in reverse order
        for id in ids.iter().rev() {
            pi.insert_version(id, 1, Some(&[id.0[15]])).await.unwrap();
        }

        let results: Vec<_> = pi.scan_at_ts(10, ScanDirection::Forward)
            .collect::<Vec<_>>().await
            .into_iter()
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 20);
        // Should come out in sorted order regardless of insert order
        for (i, (doc_id, _, _)) in results.iter().enumerate() {
            assert_eq!(doc_id.0[15], i as u8);
        }
    }
}
