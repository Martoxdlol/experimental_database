//! Access method resolution (DESIGN.md section 4.5).
//!
//! Selects the access method (primary get, index scan, table scan) by validating
//! the index, encoding the range, and producing an `AccessMethod` consumed by
//! the scan executor.

use crate::range_encoder::{encode_range, RangeError};
use exdb_core::field_path::FieldPath;
use exdb_core::filter::{Filter, RangeExpr};
use exdb_core::types::{CollectionId, DocId, IndexId};
use exdb_storage::btree::ScanDirection;
use std::ops::Bound;

/// Index metadata provided by the caller (L6 catalog cache).
/// L4 does not depend on the catalog — the caller resolves this.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub index_id: IndexId,
    pub field_paths: Vec<FieldPath>,
    pub ready: bool,
}

/// Resolved access method for query execution.
#[derive(Debug)]
pub enum AccessMethod {
    /// Point lookup by document ID (DESIGN.md section 4.2.1).
    /// Constructed by L6 directly, not by resolve_access.
    PrimaryGet {
        collection_id: CollectionId,
        doc_id: DocId,
    },

    /// Range scan on a secondary index (DESIGN.md section 4.2.2).
    IndexScan {
        collection_id: CollectionId,
        index_id: IndexId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },

    /// Full collection scan via _created_at index (DESIGN.md section 4.2.3).
    TableScan {
        collection_id: CollectionId,
        index_id: IndexId,
        direction: ScanDirection,
        post_filter: Option<Filter>,
        limit: Option<usize>,
    },
}

/// Errors from access method resolution.
#[derive(Debug)]
pub enum AccessError {
    IndexNotReady,
    Range(RangeError),
}

/// Resolve the access method for a query (DESIGN.md section 4.5).
///
/// The caller (L6) resolves collection name → collection_id and
/// index name → IndexInfo before calling this.
pub fn resolve_access(
    collection_id: CollectionId,
    index: &IndexInfo,
    range: &[RangeExpr],
    filter: Option<Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> Result<AccessMethod, AccessError> {
    if !index.ready {
        return Err(AccessError::IndexNotReady);
    }

    let (lower, upper) = encode_range(&index.field_paths, range).map_err(AccessError::Range)?;

    if matches!((&lower, &upper), (Bound::Unbounded, Bound::Unbounded)) && range.is_empty() {
        Ok(AccessMethod::TableScan {
            collection_id,
            index_id: index.index_id,
            direction,
            post_filter: filter,
            limit,
        })
    } else {
        Ok(AccessMethod::IndexScan {
            collection_id,
            index_id: index.index_id,
            lower,
            upper,
            direction,
            post_filter: filter,
            limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::Scalar;

    fn fp(name: &str) -> FieldPath {
        FieldPath::single(name)
    }

    fn test_index(fields: &[&str]) -> IndexInfo {
        IndexInfo {
            index_id: IndexId(1),
            field_paths: fields.iter().map(|n| fp(n)).collect(),
            ready: true,
        }
    }

    #[test]
    fn index_scan_basic() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a", "b"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert!(matches!(method, AccessMethod::IndexScan { .. }));
    }

    #[test]
    fn index_scan_with_filter() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
            Some(Filter::Eq(fp("b"), Scalar::Int64(2))),
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        match method {
            AccessMethod::IndexScan { post_filter, .. } => {
                assert!(post_filter.is_some());
            }
            _ => panic!("expected IndexScan"),
        }
    }

    #[test]
    fn index_scan_with_limit() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
            None,
            ScanDirection::Forward,
            Some(10),
        )
        .unwrap();
        match method {
            AccessMethod::IndexScan { limit, .. } => assert_eq!(limit, Some(10)),
            _ => panic!("expected IndexScan"),
        }
    }

    #[test]
    fn table_scan_empty_range() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a"]),
            &[],
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert!(matches!(method, AccessMethod::TableScan { .. }));
    }

    #[test]
    fn table_scan_with_filter() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a"]),
            &[],
            Some(Filter::Eq(fp("x"), Scalar::Int64(5))),
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        match method {
            AccessMethod::TableScan { post_filter, .. } => assert!(post_filter.is_some()),
            _ => panic!("expected TableScan"),
        }
    }

    #[test]
    fn direction_propagated() {
        let method = resolve_access(
            CollectionId(1),
            &test_index(&["a"]),
            &[RangeExpr::Eq(fp("a"), Scalar::Int64(1))],
            None,
            ScanDirection::Backward,
            None,
        )
        .unwrap();
        match method {
            AccessMethod::IndexScan { direction, .. } => {
                assert_eq!(direction, ScanDirection::Backward)
            }
            _ => panic!("expected IndexScan"),
        }
    }

    #[test]
    fn index_not_ready() {
        let mut idx = test_index(&["a"]);
        idx.ready = false;
        let result = resolve_access(
            CollectionId(1),
            &idx,
            &[],
            None,
            ScanDirection::Forward,
            None,
        );
        assert!(matches!(result, Err(AccessError::IndexNotReady)));
    }

    #[test]
    fn invalid_range_error() {
        let result = resolve_access(
            CollectionId(1),
            &test_index(&["a", "b"]),
            &[RangeExpr::Eq(fp("b"), Scalar::Int64(1))], // skips A
            None,
            ScanDirection::Forward,
            None,
        );
        assert!(matches!(result, Err(AccessError::Range(_))));
    }
}
