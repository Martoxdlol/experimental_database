//! B4: Catalog read tracking for unified OCC.
//!
//! Encodes catalog reads (collection lookups, index lookups, list operations)
//! as intervals on pseudo-collections so that OCC validation can detect
//! catalog-level conflicts without any special-casing.

use exdb_core::types::CollectionId;
use exdb_docstore::successor_key;
use exdb_tx::{
    QueryId, ReadInterval, ReadSet, CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX,
    CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX,
};
use std::ops::Bound;

/// Stateless utility for recording catalog reads into a transaction's read set.
pub struct CatalogTracker;

impl CatalogTracker {
    /// Record a collection name lookup (e.g., resolving "users" to CollectionId).
    pub fn record_collection_name_lookup(
        read_set: &mut ReadSet,
        query_id: QueryId,
        name: &str,
    ) {
        let key = Self::encode_collection_name_key(name);
        let upper = successor_key(&key);
        read_set.add_interval(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            ReadInterval {
                query_id,
                lower: Bound::Included(key),
                upper: Bound::Excluded(upper),
                limit_boundary: None,
            },
        );
    }

    /// Record a list-all-collections operation.
    pub fn record_list_collections(read_set: &mut ReadSet, query_id: QueryId) {
        read_set.add_interval(
            CATALOG_COLLECTIONS,
            CATALOG_COLLECTIONS_NAME_IDX,
            ReadInterval {
                query_id,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
    }

    /// Record an index name lookup within a collection.
    pub fn record_index_name_lookup(
        read_set: &mut ReadSet,
        query_id: QueryId,
        collection_id: CollectionId,
        name: &str,
    ) {
        let key = Self::encode_index_name_key(collection_id, name);
        let upper = successor_key(&key);
        read_set.add_interval(
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            ReadInterval {
                query_id,
                lower: Bound::Included(key),
                upper: Bound::Excluded(upper),
                limit_boundary: None,
            },
        );
    }

    /// Record a list-all-indexes-for-collection operation.
    pub fn record_list_indexes(
        read_set: &mut ReadSet,
        query_id: QueryId,
        collection_id: CollectionId,
    ) {
        let prefix = Self::encode_index_collection_prefix(collection_id);
        let upper = successor_key(&prefix);
        read_set.add_interval(
            CATALOG_INDEXES,
            CATALOG_INDEXES_NAME_IDX,
            ReadInterval {
                query_id,
                lower: Bound::Included(prefix),
                upper: Bound::Excluded(upper),
                limit_boundary: None,
            },
        );
    }

    // ─── Key Encoding ───

    /// Encode a collection name as a catalog key.
    pub fn encode_collection_name_key(name: &str) -> Vec<u8> {
        name.as_bytes().to_vec()
    }

    /// Encode an index name within a collection as a catalog key.
    pub fn encode_index_name_key(collection_id: CollectionId, name: &str) -> Vec<u8> {
        let mut key = collection_id.0.to_be_bytes().to_vec();
        key.extend_from_slice(name.as_bytes());
        key
    }

    /// Encode a prefix for all indexes in a collection.
    fn encode_index_collection_prefix(collection_id: CollectionId) -> Vec<u8> {
        collection_id.0.to_be_bytes().to_vec()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_name_lookup_adds_interval() {
        let mut rs = ReadSet::new();
        let qid = rs.next_query_id();
        CatalogTracker::record_collection_name_lookup(&mut rs, qid, "users");
        assert_eq!(rs.interval_count(), 1);
    }

    #[test]
    fn list_collections_adds_interval() {
        let mut rs = ReadSet::new();
        let qid = rs.next_query_id();
        CatalogTracker::record_list_collections(&mut rs, qid);
        assert_eq!(rs.interval_count(), 1);
    }

    #[test]
    fn index_name_lookup_adds_interval() {
        let mut rs = ReadSet::new();
        let qid = rs.next_query_id();
        CatalogTracker::record_index_name_lookup(&mut rs, qid, CollectionId(1), "email_idx");
        assert_eq!(rs.interval_count(), 1);
    }

    #[test]
    fn list_indexes_adds_interval() {
        let mut rs = ReadSet::new();
        let qid = rs.next_query_id();
        CatalogTracker::record_list_indexes(&mut rs, qid, CollectionId(42));
        assert_eq!(rs.interval_count(), 1);
    }

    #[test]
    fn encode_collection_name_key_deterministic() {
        let k1 = CatalogTracker::encode_collection_name_key("users");
        let k2 = CatalogTracker::encode_collection_name_key("users");
        assert_eq!(k1, k2);
    }

    #[test]
    fn encode_index_name_key_includes_collection() {
        let k1 = CatalogTracker::encode_index_name_key(CollectionId(1), "idx");
        let k2 = CatalogTracker::encode_index_name_key(CollectionId(2), "idx");
        assert_ne!(k1, k2);
    }

    #[test]
    fn name_keys_ordered() {
        let ka = CatalogTracker::encode_collection_name_key("aaa");
        let kb = CatalogTracker::encode_collection_name_key("bbb");
        assert!(ka < kb);
    }

    #[test]
    fn multiple_lookups() {
        let mut rs = ReadSet::new();
        let q1 = rs.next_query_id();
        let q2 = rs.next_query_id();
        CatalogTracker::record_collection_name_lookup(&mut rs, q1, "users");
        CatalogTracker::record_collection_name_lookup(&mut rs, q2, "orders");
        assert_eq!(rs.interval_count(), 2);
    }

    #[test]
    fn different_collections_different_indexes() {
        let mut rs = ReadSet::new();
        let q1 = rs.next_query_id();
        let q2 = rs.next_query_id();
        CatalogTracker::record_index_name_lookup(&mut rs, q1, CollectionId(1), "idx_a");
        CatalogTracker::record_index_name_lookup(&mut rs, q2, CollectionId(2), "idx_b");
        assert_eq!(rs.interval_count(), 2);
    }

    #[test]
    fn list_and_lookup_coexist() {
        let mut rs = ReadSet::new();
        let q1 = rs.next_query_id();
        let q2 = rs.next_query_id();
        CatalogTracker::record_list_collections(&mut rs, q1);
        CatalogTracker::record_collection_name_lookup(&mut rs, q2, "users");
        assert_eq!(rs.interval_count(), 2);
    }
}
