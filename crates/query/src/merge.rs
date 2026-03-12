//! Read-your-own-writes merge (DESIGN.md section 5.4).
//!
//! Merges snapshot scan results with buffered in-transaction mutations.
//! L4 does NOT import L5 types — the caller decomposes the write set
//! into a `MergeView` before calling `merge_with_writes`.

use crate::post_filter::filter_matches;
use crate::scan::ScanRow;
use exdb_core::encoding::extract_scalar;
use exdb_core::field_path::FieldPath;
use exdb_core::filter::Filter;
use exdb_core::types::{DocId, Scalar};
use exdb_docstore::key_encoding::encode_key_prefix;
use exdb_storage::btree::ScanDirection;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

/// A view of buffered mutations for one collection.
/// Constructed by L6 from the WriteSet — L4 does not depend on L5.
pub struct MergeView<'a> {
    pub inserts: &'a [(DocId, serde_json::Value)],
    pub deletes: &'a [DocId],
    pub replaces: &'a [(DocId, serde_json::Value)],
}

/// Merge snapshot scan results with buffered mutations.
pub fn merge_with_writes<I>(
    snapshot: I,
    merge_view: &MergeView<'_>,
    sort_fields: &[FieldPath],
    range_lower: Bound<&[u8]>,
    range_upper: Bound<&[u8]>,
    post_filter: Option<&Filter>,
    direction: ScanDirection,
    limit: Option<usize>,
) -> std::io::Result<Vec<ScanRow>>
where
    I: Iterator<Item = std::io::Result<ScanRow>>,
{
    let delete_set: HashSet<DocId> = merge_view.deletes.iter().copied().collect();
    let replace_map: HashMap<DocId, &serde_json::Value> =
        merge_view.replaces.iter().map(|(id, v)| (*id, v)).collect();

    // Step 1: Consume snapshot with delete/replace overlay
    let mut snapshot_rows = Vec::new();
    for item in snapshot {
        let row = item?;
        if delete_set.contains(&row.doc_id) {
            continue;
        }
        if let Some(new_body) = replace_map.get(&row.doc_id) {
            let doc = (*new_body).clone();
            if let Some(f) = post_filter {
                if !filter_matches(&doc, f) {
                    continue;
                }
            }
            snapshot_rows.push(ScanRow {
                doc_id: row.doc_id,
                version_ts: row.version_ts,
                doc,
            });
        } else {
            snapshot_rows.push(row);
        }
    }

    // Step 2: Find inserts within range that match filter
    let mut insert_rows = Vec::new();
    for (doc_id, doc) in merge_view.inserts {
        let sort_key = extract_sort_key(doc, sort_fields);
        if !key_in_range(&sort_key, range_lower, range_upper) {
            continue;
        }
        if let Some(f) = post_filter {
            if !filter_matches(doc, f) {
                continue;
            }
        }
        insert_rows.push((*doc_id, doc.clone(), sort_key));
    }

    // Step 3: Merge by sort key
    // Extract sort keys for snapshot rows too
    let mut keyed_snapshot: Vec<(Vec<u8>, ScanRow)> = snapshot_rows
        .into_iter()
        .map(|row| {
            let key = extract_sort_key(&row.doc, sort_fields);
            (key, row)
        })
        .collect();

    let mut keyed_inserts: Vec<(Vec<u8>, ScanRow)> = insert_rows
        .into_iter()
        .map(|(doc_id, doc, sort_key)| {
            (
                sort_key,
                ScanRow {
                    doc_id,
                    version_ts: 0, // write-set inserts don't have a committed ts yet
                    doc,
                },
            )
        })
        .collect();

    // Sort both lists by sort key
    match direction {
        ScanDirection::Forward => {
            keyed_snapshot.sort_by(|a, b| a.0.cmp(&b.0));
            keyed_inserts.sort_by(|a, b| a.0.cmp(&b.0));
        }
        ScanDirection::Backward => {
            keyed_snapshot.sort_by(|a, b| b.0.cmp(&a.0));
            keyed_inserts.sort_by(|a, b| b.0.cmp(&a.0));
        }
    }

    // Merge-sort the two sorted lists
    let mut result = merge_sorted(keyed_snapshot, keyed_inserts, direction);

    // Step 4: Apply limit
    if let Some(limit) = limit {
        result.truncate(limit);
    }

    Ok(result)
}

fn merge_sorted(
    mut a: Vec<(Vec<u8>, ScanRow)>,
    mut b: Vec<(Vec<u8>, ScanRow)>,
    direction: ScanDirection,
) -> Vec<ScanRow> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut ai = a.drain(..);
    let mut bi = b.drain(..);
    let mut a_next = ai.next();
    let mut b_next = bi.next();

    loop {
        match (a_next.take(), b_next.take()) {
            (Some(a_item), Some(b_item)) => {
                let ord = match direction {
                    ScanDirection::Forward => a_item.0.cmp(&b_item.0),
                    ScanDirection::Backward => b_item.0.cmp(&a_item.0),
                };
                if ord != std::cmp::Ordering::Greater {
                    result.push(a_item.1);
                    a_next = ai.next();
                    b_next = Some(b_item);
                } else {
                    result.push(b_item.1);
                    b_next = bi.next();
                    a_next = Some(a_item);
                }
            }
            (Some(a_item), None) => {
                result.push(a_item.1);
                result.extend(ai.map(|(_, r)| r));
                break;
            }
            (None, Some(b_item)) => {
                result.push(b_item.1);
                result.extend(bi.map(|(_, r)| r));
                break;
            }
            (None, None) => break,
        }
    }

    result
}

fn extract_sort_key(doc: &serde_json::Value, sort_fields: &[FieldPath]) -> Vec<u8> {
    let scalars: Vec<Scalar> = sort_fields
        .iter()
        .map(|f| extract_scalar(doc, f).unwrap_or(Scalar::Undefined))
        .collect();
    encode_key_prefix(&scalars)
}

fn key_in_range(key: &[u8], lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    let above_lower = match lower {
        Bound::Unbounded => true,
        Bound::Included(lb) => key >= lb,
        Bound::Excluded(lb) => key > lb,
    };
    let below_upper = match upper {
        Bound::Unbounded => true,
        Bound::Excluded(ub) => key < ub,
        Bound::Included(ub) => key <= ub,
    };
    above_lower && below_upper
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::field_path::FieldPath;
    use exdb_core::types::DocId;
    use serde_json::json;

    fn fp(name: &str) -> FieldPath {
        FieldPath::single(name)
    }

    fn make_doc_id(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    fn make_row(n: u8, doc: serde_json::Value) -> ScanRow {
        ScanRow {
            doc_id: make_doc_id(n),
            version_ts: 10,
            doc,
        }
    }

    fn empty_merge_view() -> MergeView<'static> {
        MergeView {
            inserts: &[],
            deletes: &[],
            replaces: &[],
        }
    }

    #[test]
    fn no_write_set() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![
            Ok(make_row(1, json!({"x": 1}))),
            Ok(make_row(2, json!({"x": 2}))),
            Ok(make_row(3, json!({"x": 3}))),
        ];
        let result = merge_with_writes(
            rows.into_iter(),
            &empty_merge_view(),
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn delete_overlay() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![
            Ok(make_row(1, json!({"x": 1}))),
            Ok(make_row(2, json!({"x": 2}))),
            Ok(make_row(3, json!({"x": 3}))),
        ];
        let deletes = [make_doc_id(2)];
        let mv = MergeView {
            inserts: &[],
            deletes: &deletes,
            replaces: &[],
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].doc_id, make_doc_id(1));
        assert_eq!(result[1].doc_id, make_doc_id(3));
    }

    #[test]
    fn replace_overlay() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![Ok(make_row(1, json!({"x": 1})))];
        let replaces = [(make_doc_id(1), json!({"x": 99}))];
        let mv = MergeView {
            inserts: &[],
            deletes: &[],
            replaces: &replaces,
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].doc["x"], 99);
    }

    #[test]
    fn replace_with_filter_rejection() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![Ok(make_row(1, json!({"x": 5})))];
        let replaces = [(make_doc_id(1), json!({"x": 1}))];
        let mv = MergeView {
            inserts: &[],
            deletes: &[],
            replaces: &replaces,
        };
        let filter = Filter::Gte(fp("x"), Scalar::Int64(3));
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            Some(&filter),
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn insert_within_range() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![];
        let inserts = [(make_doc_id(10), json!({"x": 5}))];
        let mv = MergeView {
            inserts: &inserts,
            deletes: &[],
            replaces: &[],
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn insert_sort_order_forward() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![
            Ok(make_row(1, json!({"x": 1}))),
            Ok(make_row(3, json!({"x": 3}))),
            Ok(make_row(5, json!({"x": 5}))),
        ];
        let inserts = [(make_doc_id(2), json!({"x": 2}))];
        let mv = MergeView {
            inserts: &inserts,
            deletes: &[],
            replaces: &[],
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 4);
        let xs: Vec<i64> = result
            .iter()
            .map(|r| r.doc["x"].as_i64().unwrap())
            .collect();
        assert_eq!(xs, vec![1, 2, 3, 5]);
    }

    #[test]
    fn insert_sort_order_backward() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![
            Ok(make_row(5, json!({"x": 5}))),
            Ok(make_row(3, json!({"x": 3}))),
            Ok(make_row(1, json!({"x": 1}))),
        ];
        let inserts = [(make_doc_id(2), json!({"x": 2}))];
        let mv = MergeView {
            inserts: &inserts,
            deletes: &[],
            replaces: &[],
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Backward,
            None,
        )
        .unwrap();
        let xs: Vec<i64> = result
            .iter()
            .map(|r| r.doc["x"].as_i64().unwrap())
            .collect();
        assert_eq!(xs, vec![5, 3, 2, 1]);
    }

    #[test]
    fn limit_applied() {
        let rows: Vec<std::io::Result<ScanRow>> = (1..=5)
            .map(|i| Ok(make_row(i, json!({"x": i}))))
            .collect();
        let result = merge_with_writes(
            rows.into_iter(),
            &empty_merge_view(),
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            Some(3),
        )
        .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn all_deleted() {
        let rows: Vec<std::io::Result<ScanRow>> = vec![
            Ok(make_row(1, json!({"x": 1}))),
            Ok(make_row(2, json!({"x": 2}))),
        ];
        let deletes = [make_doc_id(1), make_doc_id(2)];
        let mv = MergeView {
            inserts: &[],
            deletes: &deletes,
            replaces: &[],
        };
        let result = merge_with_writes(
            rows.into_iter(),
            &mv,
            &[fp("x")],
            Bound::Unbounded,
            Bound::Unbounded,
            None,
            ScanDirection::Forward,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 0);
    }
}
