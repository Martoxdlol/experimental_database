//! In-memory storage engine backed by BTreeMaps.
//!
//! Primary store key: `doc_id[16] || inv_ts[8]` where `inv_ts = u64::MAX - ts`
//! This ensures that for a given doc_id, the most recent version sorts first.
//!
//! Secondary index key: `encoded_value || doc_id[16] || inv_ts[8]`

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use parking_lot::RwLock;

use crate::types::{CollectionId, DocId, IndexId, Ts};

// ── Key helpers ───────────────────────────────────────────────────────────────

/// Encode (doc_id, ts) into a primary store key.
/// inv_ts = u64::MAX - ts, so higher ts sorts first within a doc_id.
pub fn make_primary_key(doc_id: &DocId, ts: Ts) -> Vec<u8> {
    let inv_ts = u64::MAX - ts;
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(doc_id.as_bytes());
    key.extend_from_slice(&inv_ts.to_be_bytes());
    key
}

/// Decode (doc_id, ts) from a primary store key.
pub fn decode_primary_key(key: &[u8]) -> Option<(DocId, Ts)> {
    if key.len() != 24 {
        return None;
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&key[..16]);
    let inv_ts = u64::from_be_bytes(key[16..24].try_into().ok()?);
    let ts = u64::MAX - inv_ts;
    Some((DocId::from_bytes(id_bytes), ts))
}

/// Create a key prefix for all versions of a given doc_id.
pub fn doc_prefix(doc_id: &DocId) -> Vec<u8> {
    doc_id.as_bytes().to_vec()
}

// ── DocValue ──────────────────────────────────────────────────────────────────

/// The value stored in the primary B-tree for a document version.
#[derive(Clone, Debug)]
pub struct DocValue {
    pub payload: Option<Vec<u8>>, // None = tombstone
}

impl DocValue {
    pub fn alive(json: Vec<u8>) -> Self {
        DocValue { payload: Some(json) }
    }
    pub fn tombstone() -> Self {
        DocValue { payload: None }
    }
    pub fn is_tombstone(&self) -> bool {
        self.payload.is_none()
    }
}

// Encode/decode DocValue to raw bytes stored in the BTreeMap.
// Format: `is_tombstone(u8) [payload_len(u32) payload...]`
pub fn encode_doc_value(v: &DocValue) -> Vec<u8> {
    match &v.payload {
        None => vec![0u8],
        Some(data) => {
            let mut out = Vec::with_capacity(1 + 4 + data.len());
            out.push(1u8);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(data);
            out
        }
    }
}

pub fn decode_doc_value(raw: &[u8]) -> Option<DocValue> {
    if raw.is_empty() {
        return None;
    }
    match raw[0] {
        0 => Some(DocValue::tombstone()),
        1 => {
            if raw.len() < 5 {
                return None;
            }
            let len = u32::from_le_bytes(raw[1..5].try_into().ok()?) as usize;
            if raw.len() < 5 + len {
                return None;
            }
            Some(DocValue::alive(raw[5..5 + len].to_vec()))
        }
        _ => None,
    }
}

// ── CollectionStore ───────────────────────────────────────────────────────────

/// Per-collection in-memory B-tree storage.
pub struct CollectionStore {
    /// Primary MVCC store: key = doc_id[16] || inv_ts[8], value = encoded DocValue
    pub primary: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,

    /// Secondary index stores: IndexId -> sorted map of encoded index keys -> ()
    pub indexes: RwLock<HashMap<IndexId, Arc<RwLock<BTreeMap<Vec<u8>, ()>>>>>,
}

impl CollectionStore {
    pub fn new() -> Self {
        CollectionStore {
            primary: RwLock::new(BTreeMap::new()),
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Add a secondary index store.
    pub fn add_index(&self, index_id: IndexId) {
        let mut indexes = self.indexes.write();
        indexes.entry(index_id).or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
    }

    /// Remove a secondary index store.
    pub fn remove_index(&self, index_id: IndexId) {
        let mut indexes = self.indexes.write();
        indexes.remove(&index_id);
    }

    /// Get the index store for an index, if it exists.
    pub fn get_index_store(&self, index_id: IndexId) -> Option<Arc<RwLock<BTreeMap<Vec<u8>, ()>>>> {
        let indexes = self.indexes.read();
        indexes.get(&index_id).cloned()
    }

    /// Put a document version into the primary store.
    pub fn put_version(&self, doc_id: &DocId, ts: Ts, value: DocValue) {
        let key = make_primary_key(doc_id, ts);
        let encoded = encode_doc_value(&value);
        let mut primary = self.primary.write();
        primary.insert(key, encoded);
    }

    /// Find the visible version of a document at a given snapshot timestamp.
    /// Returns (ts, DocValue) of the most recent version with ts <= start_ts,
    /// or None if no such version exists.
    pub fn get_visible(&self, doc_id: &DocId, start_ts: Ts) -> Option<(Ts, DocValue)> {
        let primary = self.primary.read();

        // Seek to the first key >= doc_id || inv_ts_for(start_ts+1)
        // Since inv_ts = u64::MAX - ts, we want keys with ts <= start_ts,
        // i.e. inv_ts >= u64::MAX - start_ts
        let seek_key = make_primary_key(doc_id, start_ts);
        let prefix = doc_prefix(doc_id);

        // Iterate from seek_key forward (remember: higher ts = lower inv_ts = earlier in tree)
        // We want entries with key[..16] == doc_id AND key ts <= start_ts
        for (key, raw_val) in primary.range(seek_key..) {
            if !key.starts_with(&prefix) {
                break; // Past this doc_id
            }
            let (found_doc_id, found_ts) = decode_primary_key(key)?;
            if found_doc_id != *doc_id {
                break;
            }
            if found_ts <= start_ts {
                if let Some(val) = decode_doc_value(raw_val) {
                    return Some((found_ts, val));
                }
            }
        }
        None
    }

    /// Scan all doc_ids visible at start_ts. Yields (doc_id, ts, DocValue).
    /// Each doc_id appears at most once (the visible version).
    pub fn scan_visible<F>(&self, start_ts: Ts, mut callback: F)
    where
        F: FnMut(DocId, Ts, DocValue),
    {
        let primary = self.primary.read();
        let mut last_doc_id: Option<DocId> = None;

        for (key, raw_val) in primary.iter() {
            if key.len() != 24 {
                continue;
            }
            let Some((doc_id, ts)) = decode_primary_key(key) else { continue };

            // Skip doc_ids we've already processed (we only want the first visible version)
            if let Some(ref last) = last_doc_id {
                if *last == doc_id {
                    continue;
                }
            }

            if ts > start_ts {
                // This version is too new – but there might be an older one
                continue;
            }

            if let Some(val) = decode_doc_value(raw_val) {
                last_doc_id = Some(doc_id);
                callback(doc_id, ts, val);
            }
        }
    }

    /// Scan visible docs more efficiently: iterate in doc_id order, and for each
    /// doc_id find the best version at start_ts.
    pub fn scan_all_docs_at<F>(&self, start_ts: Ts, mut callback: F)
    where
        F: FnMut(DocId, Ts, DocValue),
    {
        let primary = self.primary.read();
        let mut prev_doc: Option<DocId> = None;
        let mut best: Option<(DocId, Ts, DocValue)> = None;

        // Keys are sorted: (doc_id asc, inv_ts asc) = (doc_id asc, ts desc)
        // So for each doc_id, the first key with ts <= start_ts is the visible one.
        for (key, raw_val) in primary.iter() {
            let Some((doc_id, ts)) = decode_primary_key(key) else { continue };

            let new_doc = match &prev_doc {
                None => true,
                Some(prev) => *prev != doc_id,
            };

            if new_doc {
                // Emit the best version of the previous doc_id
                if let Some((d, t, v)) = best.take() {
                    callback(d, t, v);
                }
                prev_doc = Some(doc_id);
            }

            // If we haven't found a visible version yet for this doc_id
            if best.is_none() || best.as_ref().map_or(true, |(d, _, _)| *d != doc_id) {
                if ts <= start_ts {
                    if let Some(val) = decode_doc_value(raw_val) {
                        best = Some((doc_id, ts, val));
                    }
                }
            }
        }

        // Emit the last doc
        if let Some((d, t, v)) = best {
            callback(d, t, v);
        }
    }

    /// Check if any version of a doc_id was committed after start_ts (exclusive) and
    /// up to end_ts (inclusive). Used for conflict detection.
    pub fn any_version_in_range(&self, doc_id: &DocId, after_ts: Ts, up_to_ts: Ts) -> bool {
        let primary = self.primary.read();
        let prefix = doc_prefix(doc_id);

        // We want ts in (after_ts, up_to_ts]
        // inv_ts = u64::MAX - ts
        // ts > after_ts means inv_ts < u64::MAX - after_ts
        // ts <= up_to_ts means inv_ts >= u64::MAX - up_to_ts

        let start_key = make_primary_key(doc_id, up_to_ts); // inv_ts = u64::MAX - up_to_ts (larger inv_ts first in btree = lower ts)
        // Actually we want ts in (after_ts, up_to_ts], so:
        // ts >= after_ts + 1 and ts <= up_to_ts
        // inv_ts <= u64::MAX - (after_ts + 1) and inv_ts >= u64::MAX - up_to_ts
        // In btree order (ascending inv_ts), we want: key >= make_primary_key(doc_id, up_to_ts)
        // and key <= make_primary_key(doc_id, after_ts + 1)
        let end_key = make_primary_key(doc_id, after_ts.saturating_add(1));

        for (key, _) in primary.range(start_key..=end_key) {
            if !key.starts_with(&prefix) {
                break;
            }
            let Some((found_doc_id, found_ts)) = decode_primary_key(key) else { continue };
            if found_doc_id != *doc_id {
                break;
            }
            if found_ts > after_ts && found_ts <= up_to_ts {
                return true;
            }
        }
        false
    }

    /// Get the maximum timestamp of any write to this collection. Returns 0 if empty.
    pub fn max_ts(&self) -> Ts {
        let primary = self.primary.read();
        let mut max = 0u64;
        for key in primary.keys() {
            if let Some((_, ts)) = decode_primary_key(key) {
                if ts > max {
                    max = ts;
                }
            }
        }
        max
    }
}

// ── StorageEngine ─────────────────────────────────────────────────────────────

/// Central storage engine managing all collection stores.
pub struct StorageEngine {
    collections: parking_lot::RwLock<HashMap<CollectionId, Arc<CollectionStore>>>,
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            collections: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn create_collection(&self, id: CollectionId) {
        let mut cols = self.collections.write();
        cols.entry(id).or_insert_with(|| Arc::new(CollectionStore::new()));
    }

    pub fn drop_collection(&self, id: CollectionId) {
        let mut cols = self.collections.write();
        cols.remove(&id);
    }

    pub fn get_collection(&self, id: CollectionId) -> Option<Arc<CollectionStore>> {
        let cols = self.collections.read();
        cols.get(&id).cloned()
    }

    pub fn collection_ids(&self) -> Vec<CollectionId> {
        let cols = self.collections.read();
        cols.keys().copied().collect()
    }

    /// Apply a put (insert or update) to the engine.
    pub fn apply_put(&self, collection_id: CollectionId, doc_id: &DocId, ts: Ts, json: Vec<u8>) {
        let col = {
            let cols = self.collections.read();
            cols.get(&collection_id).cloned()
        };
        if let Some(col) = col {
            col.put_version(doc_id, ts, DocValue::alive(json));
        }
    }

    /// Apply a delete (tombstone) to the engine.
    pub fn apply_delete(&self, collection_id: CollectionId, doc_id: &DocId, ts: Ts) {
        let col = {
            let cols = self.collections.read();
            cols.get(&collection_id).cloned()
        };
        if let Some(col) = col {
            col.put_version(doc_id, ts, DocValue::tombstone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_key_roundtrip() {
        let doc_id = DocId::new(12345, 67890);
        for ts in [0u64, 1, 100, u64::MAX / 2, u64::MAX - 1] {
            let key = make_primary_key(&doc_id, ts);
            let (decoded_doc_id, decoded_ts) = decode_primary_key(&key).unwrap();
            assert_eq!(decoded_doc_id, doc_id);
            assert_eq!(decoded_ts, ts);
        }
    }

    #[test]
    fn test_mvcc_ordering() {
        // Later timestamps should sort BEFORE earlier ones in the BTree
        // (because inv_ts = u64::MAX - ts)
        let doc_id = DocId::new(1, 1);
        let key_ts10 = make_primary_key(&doc_id, 10);
        let key_ts5 = make_primary_key(&doc_id, 5);
        // inv_ts for ts=10 is smaller than inv_ts for ts=5
        // So key_ts10 < key_ts5 in lexicographic order
        assert!(key_ts10 < key_ts5, "newer ts should sort before older ts");
    }

    #[test]
    fn test_collection_store_get_visible() {
        let store = CollectionStore::new();
        let doc_id = DocId::new(1, 1);

        store.put_version(&doc_id, 10, DocValue::alive(b"v10".to_vec()));
        store.put_version(&doc_id, 20, DocValue::alive(b"v20".to_vec()));
        store.put_version(&doc_id, 30, DocValue::tombstone());

        // At ts=10, see v10
        let visible = store.get_visible(&doc_id, 10).unwrap();
        assert_eq!(visible.0, 10);
        assert_eq!(visible.1.payload.unwrap(), b"v10");

        // At ts=15, see v10 (last version <= 15)
        let visible = store.get_visible(&doc_id, 15).unwrap();
        assert_eq!(visible.0, 10);

        // At ts=20, see v20
        let visible = store.get_visible(&doc_id, 20).unwrap();
        assert_eq!(visible.0, 20);
        assert_eq!(visible.1.payload.unwrap(), b"v20");

        // At ts=30, see tombstone
        let visible = store.get_visible(&doc_id, 30).unwrap();
        assert!(visible.1.is_tombstone());

        // At ts=5, nothing
        assert!(store.get_visible(&doc_id, 5).is_none());
    }
}
