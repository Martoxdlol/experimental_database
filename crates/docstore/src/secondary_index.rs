//! D4: Secondary Index — MVCC-aware secondary B-tree with primary verification.
//!
//! Secondary index entries have the key format:
//! `type_tag || encoded_value || doc_id || inv_ts` with empty values.
//! Scans use version resolution and verify against the primary index to skip
//! stale entries.

use crate::key_encoding::parse_secondary_key_suffix;
use crate::primary_index::PrimaryIndex;
use crate::version_resolution::{Verdict, VersionResolver};
use exdb_core::types::{DocId, Ts};
use exdb_storage::btree::{ScanDirection, ScanIterator};
use exdb_storage::engine::BTreeHandle;
use std::ops::Bound;
use std::sync::Arc;

/// A secondary index backed by a B-tree.
///
/// Keys: `type_tag[1] || encoded_value[var] || doc_id[16] || inv_ts[8]`
/// Values: empty (body is in the primary index).
pub struct SecondaryIndex {
    btree: BTreeHandle,
    primary: Arc<PrimaryIndex>,
}

impl SecondaryIndex {
    /// Create a new SecondaryIndex.
    pub fn new(btree: BTreeHandle, primary: Arc<PrimaryIndex>) -> Self {
        Self { btree, primary }
    }

    /// Insert a secondary index entry. Value is always empty.
    pub fn insert_entry(&self, encoded_key: &[u8]) -> std::io::Result<()> {
        self.btree.insert(encoded_key, &[])
    }

    /// Remove a secondary index entry.
    pub fn remove_entry(&self, encoded_key: &[u8]) -> std::io::Result<bool> {
        self.btree.delete(encoded_key)
    }

    /// Scan with MVCC version resolution and primary index verification.
    ///
    /// Yields verified `(doc_id, version_ts)` pairs.
    pub fn scan_at_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: Ts,
        direction: ScanDirection,
    ) -> SecondaryScanner {
        let iter = self.btree.scan(lower, upper, direction);
        SecondaryScanner {
            inner: iter,
            resolver: VersionResolver::new(read_ts, direction),
            primary: self.primary.clone(),
            read_ts,
            finished: false,
        }
    }

    /// Access the underlying B-tree.
    pub fn btree(&self) -> &BTreeHandle {
        &self.btree
    }
}

/// Iterator yielding verified `(doc_id, version_ts)` pairs from a secondary index scan.
pub struct SecondaryScanner {
    inner: ScanIterator,
    resolver: VersionResolver,
    primary: Arc<PrimaryIndex>,
    read_ts: Ts,
    finished: bool,
}

impl SecondaryScanner {
    fn verify(&self, doc_id: &DocId, ts: Ts) -> std::io::Result<Option<(DocId, Ts)>> {
        match self.primary.get_version_ts(doc_id, self.read_ts)? {
            Some(primary_ts) if primary_ts == ts => Ok(Some((*doc_id, ts))),
            _ => Ok(None), // stale or tombstoned
        }
    }
}

impl Iterator for SecondaryScanner {
    type Item = std::io::Result<(DocId, Ts)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }

            match self.inner.next() {
                None => {
                    self.finished = true;
                    if let Some((doc_id, ts)) = self.resolver.finish() {
                        match self.verify(&doc_id, ts) {
                            Ok(Some(pair)) => return Some(Ok(pair)),
                            Ok(None) => return None,
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    return None;
                }
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok((key, _value))) => {
                    let (doc_id, ts) = match parse_secondary_key_suffix(&key) {
                        Ok(v) => v,
                        Err(e) => return Some(Err(std::io::Error::other(e))),
                    };

                    match self.resolver.process(&doc_id, ts) {
                        Verdict::Skip => continue,
                        Verdict::Visible => match self.verify(&doc_id, ts) {
                            Ok(Some(pair)) => return Some(Ok(pair)),
                            Ok(None) => continue,
                            Err(e) => return Some(Err(e)),
                        },
                        Verdict::EmitPrevious(prev_id, prev_ts) => {
                            match self.verify(&prev_id, prev_ts) {
                                Ok(Some(pair)) => return Some(Ok(pair)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_encoding::{make_secondary_key, encode_key_prefix, successor_key};
    use exdb_core::types::Scalar;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    fn setup() -> (Arc<StorageEngine>, Arc<PrimaryIndex>, SecondaryIndex) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).unwrap());
        let primary_btree = engine.create_btree().unwrap();
        let primary = Arc::new(PrimaryIndex::new(primary_btree, engine.clone(), 4096));
        let sec_btree = engine.create_btree().unwrap();
        let sec = SecondaryIndex::new(sec_btree, primary.clone());
        (engine, primary, sec)
    }

    fn doc(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    #[tokio::test]
    async fn insert_and_scan() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"body")).unwrap();
        let key = make_secondary_key(&[Scalar::String("hello".into())], &d, 5);
        sec.insert_entry(&key).unwrap();

        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (d, 5));
    }

    #[tokio::test]
    async fn scan_range() {
        let (_engine, primary, sec) = setup();
        for (i, val) in ["a", "b", "c"].iter().enumerate() {
            let d = doc(i as u8);
            primary.insert_version(&d, 5, Some(b"body")).unwrap();
            let key = make_secondary_key(&[Scalar::String(val.to_string())], &d, 5);
            sec.insert_entry(&key).unwrap();
        }

        let lower = encode_key_prefix(&[Scalar::String("b".into())]);
        let upper = successor_key(&encode_key_prefix(&[Scalar::String("b".into())]));
        let results: Vec<_> = sec
            .scan_at_ts(
                Bound::Included(&lower),
                Bound::Excluded(&upper),
                10,
                ScanDirection::Forward,
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, doc(1));
    }

    #[tokio::test]
    async fn version_resolution_in_scan() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"v5")).unwrap();
        primary.insert_version(&d, 10, Some(b"v10")).unwrap();

        let key5 = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        let key10 = make_secondary_key(&[Scalar::String("x".into())], &d, 10);
        sec.insert_entry(&key5).unwrap();
        sec.insert_entry(&key10).unwrap();

        // At read_ts=7, only ts=5 is visible, and primary confirms ts=5 is latest
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 7, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (d, 5));
    }

    #[tokio::test]
    async fn stale_entry_skipped() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        // Primary has version at ts=10 (newer)
        primary.insert_version(&d, 5, Some(b"v5")).unwrap();
        primary.insert_version(&d, 10, Some(b"v10")).unwrap();

        // Secondary only has entry at ts=5 (stale)
        let key5 = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        sec.insert_entry(&key5).unwrap();

        // At read_ts=15, version resolution picks ts=5, but primary says latest is ts=10 != 5 → stale
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 15, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn tombstoned_doc() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"body")).unwrap();
        primary.insert_version(&d, 10, None).unwrap(); // tombstone

        let key = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        sec.insert_entry(&key).unwrap();

        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 15, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn remove_entry() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"body")).unwrap();
        let key = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        sec.insert_entry(&key).unwrap();
        sec.remove_entry(&key).unwrap();

        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn empty_scan() {
        let (_engine, _primary, sec) = setup();
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn multiple_doc_ids() {
        let (_engine, primary, sec) = setup();
        for i in 0..3u8 {
            let d = doc(i);
            primary.insert_version(&d, 5, Some(b"body")).unwrap();
            let key = make_secondary_key(&[Scalar::Int64(i as i64)], &d, 5);
            sec.insert_entry(&key).unwrap();
        }

        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    // ─── Backward scan ───

    #[tokio::test]
    async fn backward_scan() {
        let (_engine, primary, sec) = setup();
        for i in 0..3u8 {
            let d = doc(i);
            primary.insert_version(&d, 5, Some(b"body")).unwrap();
            let key = make_secondary_key(&[Scalar::Int64(i as i64)], &d, 5);
            sec.insert_entry(&key).unwrap();
        }

        let forward: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        let backward: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Backward)
            .collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(forward.len(), 3);
        assert_eq!(backward.len(), 3);
        // Backward reverses forward
        for i in 0..3 {
            assert_eq!(forward[i].0, backward[2 - i].0);
        }
    }

    // ─── Compound key scan ───

    #[tokio::test]
    async fn compound_key_scan() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"body")).unwrap();
        let key = make_secondary_key(
            &[Scalar::String("hello".into()), Scalar::Int64(42)],
            &d, 5,
        );
        sec.insert_entry(&key).unwrap();

        let prefix = encode_key_prefix(&[Scalar::String("hello".into()), Scalar::Int64(42)]);
        let upper = successor_key(&prefix);
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 1);
    }

    // ─── Same value, multiple docs ───

    #[tokio::test]
    async fn same_value_multiple_docs() {
        let (_engine, primary, sec) = setup();
        for i in 0..10u8 {
            let d = doc(i);
            primary.insert_version(&d, 5, Some(b"body")).unwrap();
            let key = make_secondary_key(&[Scalar::String("shared".into())], &d, 5);
            sec.insert_entry(&key).unwrap();
        }

        let prefix = encode_key_prefix(&[Scalar::String("shared".into())]);
        let upper = successor_key(&prefix);
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Included(&prefix), Bound::Excluded(&upper), 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 10);
    }

    // ─── All entries stale ───

    #[tokio::test]
    async fn all_entries_stale() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        // Primary has newer version
        primary.insert_version(&d, 5, Some(b"v5")).unwrap();
        primary.insert_version(&d, 10, Some(b"v10")).unwrap();

        // Secondary only has old entry
        let key = make_secondary_key(&[Scalar::String("old".into())], &d, 5);
        sec.insert_entry(&key).unwrap();

        // At ts=15: secondary sees ts=5 but primary latest is ts=10 → stale
        let results: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 15, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert!(results.is_empty());
    }

    // ─── Version resolution across time ───

    #[tokio::test]
    async fn scan_at_different_timestamps() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 5, Some(b"v5")).unwrap();
        primary.insert_version(&d, 10, Some(b"v10")).unwrap();

        let key5 = make_secondary_key(&[Scalar::String("x".into())], &d, 5);
        let key10 = make_secondary_key(&[Scalar::String("x".into())], &d, 10);
        sec.insert_entry(&key5).unwrap();
        sec.insert_entry(&key10).unwrap();

        // At ts=7: only ts=5 visible, matches primary
        let r: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 7, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].1, 5);

        // At ts=15: ts=10 visible, matches primary
        let r: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 15, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].1, 10);
    }

    // ─── Inclusive/Exclusive bound combinations ───

    #[tokio::test]
    async fn scan_bound_variations() {
        let (_engine, primary, sec) = setup();
        for i in 0..5u8 {
            let d = doc(i);
            primary.insert_version(&d, 1, Some(b"body")).unwrap();
            let key = make_secondary_key(&[Scalar::Int64(i as i64)], &d, 1);
            sec.insert_entry(&key).unwrap();
        }

        // Unbounded, Unbounded → all 5
        let r: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 5);

        // Exact range for value=2
        let lower = encode_key_prefix(&[Scalar::Int64(2)]);
        let upper = successor_key(&lower);
        let r: Vec<_> = sec
            .scan_at_ts(Bound::Included(&lower), Bound::Excluded(&upper), 10, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, doc(2));
    }

    // ─── Read_ts before any entry ───

    #[tokio::test]
    async fn scan_read_ts_before_entries() {
        let (_engine, primary, sec) = setup();
        let d = doc(1);
        primary.insert_version(&d, 10, Some(b"body")).unwrap();
        let key = make_secondary_key(&[Scalar::Int64(1)], &d, 10);
        sec.insert_entry(&key).unwrap();

        // read_ts=5: entry at ts=10 invisible
        let r: Vec<_> = sec
            .scan_at_ts(Bound::Unbounded, Bound::Unbounded, 5, ScanDirection::Forward)
            .collect::<Result<Vec<_>, _>>().unwrap();
        assert!(r.is_empty());
    }
}
