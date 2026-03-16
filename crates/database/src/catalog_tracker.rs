//! B4: Catalog Tracker — encodes catalog reads/writes as intervals on pseudo-collections.
//!
//! Translates catalog operations into byte-interval representation used by
//! OCC validation and subscription invalidation (DESIGN.md 5.6.7).

use std::ops::Bound;

use exdb_core::types::{CollectionId, Scalar};
use exdb_docstore::{encode_key_prefix, successor_key};
use exdb_tx::read_set::{
    QueryId, ReadInterval, ReadSet, CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX,
    CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX,
};
use exdb_tx::PRIMARY_INDEX_SENTINEL;

/// Catalog read/write tracking.
///
/// Stateless — all methods are associated functions on this struct for namespacing.
pub struct CatalogTracker;

impl CatalogTracker {
    /// Record a catalog read for resolving a collection name.
    ///
    /// Point interval on `(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)`.
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

    /// Record a catalog read for listing all collections.
    ///
    /// Full-range interval on `(CATALOG_COLLECTIONS, PRIMARY_INDEX_SENTINEL)`.
    pub fn record_list_collections(read_set: &mut ReadSet, query_id: QueryId) {
        read_set.add_interval(
            CATALOG_COLLECTIONS,
            PRIMARY_INDEX_SENTINEL,
            ReadInterval {
                query_id,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                limit_boundary: None,
            },
        );
    }

    /// Record a catalog read for resolving an index name within a collection.
    ///
    /// Point interval on `(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)`.
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

    /// Record a catalog read for listing all indexes of a collection.
    ///
    /// Prefix-range interval on `(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)`.
    pub fn record_list_indexes(
        read_set: &mut ReadSet,
        query_id: QueryId,
        collection_id: CollectionId,
    ) {
        let prefix = encode_key_prefix(&[Scalar::Int64(collection_id.0 as i64)]);
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

    /// Encode a collection name into a catalog key for conflict detection.
    pub fn encode_collection_name_key(name: &str) -> Vec<u8> {
        encode_key_prefix(&[Scalar::String(name.to_string())])
    }

    /// Encode a (collection_id, index_name) pair into a catalog key.
    pub fn encode_index_name_key(collection_id: CollectionId, name: &str) -> Vec<u8> {
        encode_key_prefix(&[
            Scalar::Int64(collection_id.0 as i64),
            Scalar::String(name.to_string()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_name_lookup_point_interval() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_collection_name_lookup(&mut rs, 0, "users");
        let intervals = &rs.intervals[&(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)];
        assert_eq!(intervals.len(), 1);
        let ri = &intervals[0];
        assert!(matches!(&ri.lower, Bound::Included(_)));
        assert!(matches!(&ri.upper, Bound::Excluded(_)));
        assert!(ri.limit_boundary.is_none());
    }

    #[test]
    fn list_collections_full_range() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_list_collections(&mut rs, 0);
        let intervals = &rs.intervals[&(CATALOG_COLLECTIONS, PRIMARY_INDEX_SENTINEL)];
        assert_eq!(intervals.len(), 1);
        let ri = &intervals[0];
        assert!(matches!(&ri.lower, Bound::Unbounded));
        assert!(matches!(&ri.upper, Bound::Unbounded));
    }

    #[test]
    fn index_name_lookup_point_interval() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_index_name_lookup(
            &mut rs,
            0,
            CollectionId(10),
            "by_email",
        );
        let intervals = &rs.intervals[&(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)];
        assert_eq!(intervals.len(), 1);
        let ri = &intervals[0];
        assert!(matches!(&ri.lower, Bound::Included(_)));
        assert!(matches!(&ri.upper, Bound::Excluded(_)));
    }

    #[test]
    fn list_indexes_prefix_range() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_list_indexes(&mut rs, 0, CollectionId(10));
        let intervals = &rs.intervals[&(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)];
        assert_eq!(intervals.len(), 1);
        let ri = &intervals[0];
        assert!(matches!(&ri.lower, Bound::Included(_)));
        assert!(matches!(&ri.upper, Bound::Excluded(_)));
    }

    #[test]
    fn query_id_preserved() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_collection_name_lookup(&mut rs, 42, "test");
        let intervals = &rs.intervals[&(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)];
        assert_eq!(intervals[0].query_id, 42);
    }

    #[test]
    fn encode_collection_name_key_roundtrip() {
        let k1 = CatalogTracker::encode_collection_name_key("users");
        let k2 = CatalogTracker::encode_collection_name_key("users");
        assert_eq!(k1, k2);
    }

    #[test]
    fn encode_index_name_key_roundtrip() {
        let k1 = CatalogTracker::encode_index_name_key(CollectionId(10), "by_email");
        let k2 = CatalogTracker::encode_index_name_key(CollectionId(10), "by_email");
        assert_eq!(k1, k2);
    }

    #[test]
    fn point_interval_detects_exact_match() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_collection_name_lookup(&mut rs, 0, "users");
        let intervals = &rs.intervals[&(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)];
        let ri = &intervals[0];
        let key = CatalogTracker::encode_collection_name_key("users");
        assert!(ri.contains_key(&key));
    }

    #[test]
    fn point_interval_rejects_different_name() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_collection_name_lookup(&mut rs, 0, "users");
        let intervals = &rs.intervals[&(CATALOG_COLLECTIONS, CATALOG_COLLECTIONS_NAME_IDX)];
        let ri = &intervals[0];
        let key = CatalogTracker::encode_collection_name_key("orders");
        assert!(!ri.contains_key(&key));
    }

    #[test]
    fn prefix_range_covers_all_names_in_collection() {
        let mut rs = ReadSet::new();
        CatalogTracker::record_list_indexes(&mut rs, 0, CollectionId(10));
        let intervals = &rs.intervals[&(CATALOG_INDEXES, CATALOG_INDEXES_NAME_IDX)];
        let ri = &intervals[0];
        // Any index name under collection 10 should match
        let k1 = CatalogTracker::encode_index_name_key(CollectionId(10), "alpha");
        let k2 = CatalogTracker::encode_index_name_key(CollectionId(10), "zeta");
        assert!(ri.contains_key(&k1));
        assert!(ri.contains_key(&k2));
        // Index under a different collection should not match
        let k3 = CatalogTracker::encode_index_name_key(CollectionId(11), "alpha");
        assert!(!ri.contains_key(&k3));
    }
}
