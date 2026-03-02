//! Query planner and executors.

use anyhow::Result;

use crate::catalog::CollectionMeta;
use crate::filter::filter_matches;
use crate::index::{decode_index_key_suffix, index_eq_range, index_range_gt, index_range_gte, index_range_lt, index_range_lte};
use crate::storage::engine::CollectionStore;
use crate::tx::ReadSet;
use crate::types::{CollectionId, DocId, Filter, IndexId, KeyRange, Ts};

// ── Query plan ────────────────────────────────────────────────────────────────

pub enum QueryPlan {
    /// Direct lookup of a known doc_id
    PrimaryGet(DocId),
    /// Scan a secondary index in a key range, then verify via primary
    IndexScan {
        index_id: IndexId,
        range: KeyRange,
        post_filter: Option<Filter>,
    },
    /// Full table scan with optional filter
    TableScan {
        post_filter: Option<Filter>,
    },
}

// ── Planner ───────────────────────────────────────────────────────────────────

/// Determine the best plan for a filter against a collection.
pub fn plan_query(collection_meta: &CollectionMeta, filter: Option<&Filter>) -> QueryPlan {
    let Some(filter) = filter else {
        return QueryPlan::TableScan { post_filter: None };
    };

    // Try to find an index-sargable predicate
    if let Some((index_id, range)) = extract_index_plan(collection_meta, filter) {
        return QueryPlan::IndexScan {
            index_id,
            range,
            post_filter: Some(filter.clone()),
        };
    }

    QueryPlan::TableScan { post_filter: Some(filter.clone()) }
}

/// Try to extract an index scan plan from the filter.
/// Returns (index_id, key_range) if an applicable index exists.
fn extract_index_plan(
    collection_meta: &CollectionMeta,
    filter: &Filter,
) -> Option<(IndexId, KeyRange)> {
    match filter {
        Filter::Eq(field, scalar) => {
            let idx = collection_meta.find_ready_index_for_field(field)?;
            Some((idx.index_id, index_eq_range(scalar)))
        }
        Filter::In(field, scalars) => {
            // For In, we'd need multiple ranges. For simplicity, fall back to table scan
            // if there are many values; use first range as a hint.
            let idx = collection_meta.find_ready_index_for_field(field)?;
            if scalars.is_empty() {
                return None;
            }
            // Use a range from min to max scalar in the In list
            // This is a conservative range that will be post-filtered
            let min_scalar = scalars.iter().min_by(|a, b| {
                let ea = crate::index::encode_scalar(a);
                let eb = crate::index::encode_scalar(b);
                ea.cmp(&eb)
            })?;
            let max_scalar = scalars.iter().max_by(|a, b| {
                let ea = crate::index::encode_scalar(a);
                let eb = crate::index::encode_scalar(b);
                ea.cmp(&eb)
            })?;
            let mut end = crate::index::encode_scalar(max_scalar);
            // Go past all doc_ids for the max value
            end.extend_from_slice(&[0xFF; 24]);
            Some((idx.index_id, KeyRange {
                start: crate::index::encode_scalar(min_scalar),
                end,
            }))
        }
        Filter::Gt(field, scalar) => {
            let idx = collection_meta.find_ready_index_for_field(field)?;
            Some((idx.index_id, index_range_gt(scalar)))
        }
        Filter::Gte(field, scalar) => {
            let idx = collection_meta.find_ready_index_for_field(field)?;
            Some((idx.index_id, index_range_gte(scalar)))
        }
        Filter::Lt(field, scalar) => {
            let idx = collection_meta.find_ready_index_for_field(field)?;
            Some((idx.index_id, index_range_lt(scalar)))
        }
        Filter::Lte(field, scalar) => {
            let idx = collection_meta.find_ready_index_for_field(field)?;
            Some((idx.index_id, index_range_lte(scalar)))
        }
        Filter::And(filters) => {
            // Try to use an index for the first sargable predicate
            for f in filters {
                if let Some(plan) = extract_index_plan(collection_meta, f) {
                    return Some(plan);
                }
            }
            None
        }
        _ => None,
    }
}

// ── Executors ─────────────────────────────────────────────────────────────────

/// Result of a query execution.
pub struct QueryResult {
    pub docs: Vec<(DocId, Vec<u8>)>, // (doc_id, json_bytes)
}

/// Execute a query plan against a collection store at a given snapshot.
pub fn execute_query(
    plan: QueryPlan,
    store: &CollectionStore,
    start_ts: Ts,
    limit: Option<usize>,
    read_set: &mut ReadSet,
    collection_id: CollectionId,
) -> Result<QueryResult> {
    let mut docs = Vec::new();
    let max = limit.unwrap_or(usize::MAX);

    match plan {
        QueryPlan::PrimaryGet(doc_id) => {
            if let Some((ts, val)) = store.get_visible(&doc_id, start_ts) {
                read_set.add_point_read(collection_id, doc_id, Some(ts));
                if !val.is_tombstone() {
                    if let Some(json) = val.payload {
                        docs.push((doc_id, json));
                    }
                }
            } else {
                read_set.add_point_read(collection_id, doc_id, None);
            }
        }

        QueryPlan::IndexScan { index_id, range, post_filter } => {
            let index_store = store.get_index_store(index_id);
            if let Some(index_store) = index_store {
                let index = index_store.read();

                // Record the index range read
                read_set.add_index_range_read(collection_id, index_id, range.clone(), start_ts);

                // Iterate index entries in range
                let range_iter = index.range(range.start.clone()..range.end.clone());
                for (key, _) in range_iter {
                    if docs.len() >= max {
                        break;
                    }
                    let Some((doc_id, _index_ts)) = decode_index_key_suffix(key) else { continue };

                    // Primary lookup at start_ts
                    if let Some((vis_ts, val)) = store.get_visible(&doc_id, start_ts) {
                        if val.is_tombstone() {
                            continue;
                        }
                        if let Some(json) = val.payload {
                            // Apply post-filter
                            if let Some(ref f) = post_filter {
                                if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&json) {
                                    if !filter_matches(&parsed, f) {
                                        continue;
                                    }
                                }
                            }
                            read_set.add_point_read(collection_id, doc_id, Some(vis_ts));
                            docs.push((doc_id, json));
                        }
                    }
                }
            } else {
                // Index not available – fall back to table scan
                return execute_query(
                    QueryPlan::TableScan { post_filter },
                    store,
                    start_ts,
                    limit,
                    read_set,
                    collection_id,
                );
            }
        }

        QueryPlan::TableScan { post_filter } => {
            read_set.add_collection_scan(collection_id, start_ts);

            store.scan_all_docs_at(start_ts, |doc_id, _ts, val| {
                if docs.len() >= max {
                    return;
                }
                if val.is_tombstone() {
                    return;
                }
                if let Some(json) = val.payload {
                    if let Some(ref f) = post_filter {
                        match serde_json::from_slice::<serde_json::Value>(&json) {
                            Ok(parsed) => {
                                if !filter_matches(&parsed, f) {
                                    return;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    docs.push((doc_id, json));
                }
            });
        }
    }

    Ok(QueryResult { docs })
}
