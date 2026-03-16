//! Scan execution (DESIGN.md sections 4.1, 4.2, 4.5).
//!
//! Executes an `AccessMethod` by driving L3 streams through the three-stage
//! pipeline: Source → PostFilter → Terminal.

use crate::access::AccessMethod;
use crate::post_filter::filter_matches;
use exdb_core::encoding::decode_document;
use exdb_core::filter::Filter;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_docstore::key_encoding::successor_key;
use exdb_docstore::{PrimaryIndex, SecondaryIndex};
use futures_core::Stream;
use std::collections::HashMap;
use std::ops::Bound;
use std::pin::Pin;

/// A single result row from a query scan.
#[derive(Debug)]
pub struct ScanRow {
    pub doc_id: DocId,
    pub version_ts: Ts,
    pub doc: serde_json::Value,
}

/// Read set interval computed from the plan bounds.
/// Available before iteration begins.
#[derive(Debug, Clone)]
pub struct ReadIntervalInfo {
    pub collection_id: CollectionId,
    pub index_id: IndexId,
    /// Always Included (convention per DESIGN.md 5.6.2).
    pub lower: Vec<u8>,
    /// Excluded or Unbounded.
    pub upper: Bound<Vec<u8>>,
}

/// Scan statistics accumulated during iteration.
#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    pub scanned_docs: usize,
    pub scanned_bytes: usize,
    pub returned_docs: usize,
}

/// Convention: IndexId(0) for the primary index.
const PRIMARY_INDEX_ID: IndexId = IndexId(0);

/// Stream type for query scans.
pub type QueryScanStream<'a> = Pin<Box<dyn Stream<Item = std::io::Result<ScanRow>> + Send + 'a>>;

/// Execute an access method against the indexes at the given read timestamp.
///
/// Returns a `QueryScanStream` that lazily produces documents through the
/// Source → PostFilter → Terminal pipeline.
pub async fn execute_scan<'a>(
    method: &'a AccessMethod,
    primary_index: &'a PrimaryIndex,
    secondary_indexes: &'a HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
) -> std::io::Result<(QueryScanStream<'a>, ReadIntervalInfo)> {
    let read_interval = compute_read_interval(method);

    match method {
        AccessMethod::PrimaryGet {
            doc_id,
            ..
        } => {
            let body = primary_index.get_at_ts(doc_id, read_ts).await?;
            let version_ts = primary_index.get_version_ts(doc_id, read_ts).await?;
            let row = match (body, version_ts) {
                (Some(body_bytes), Some(ts)) => {
                    let doc = decode_document(&body_bytes)
                        .map_err(std::io::Error::other)?;
                    Some(ScanRow {
                        doc_id: *doc_id,
                        version_ts: ts,
                        doc,
                    })
                }
                _ => None,
            };
            let stream: QueryScanStream<'a> = Box::pin(async_stream::try_stream! {
                if let Some(r) = row {
                    yield r;
                }
            });
            Ok((stream, read_interval))
        }

        AccessMethod::IndexScan {
            index_id,
            lower,
            upper,
            post_filter,
            limit,
            ..
        } => {
            let sec_index = secondary_indexes
                .get(index_id)
                .ok_or_else(|| std::io::Error::other("index not found"))?;

            let direction = match method {
                AccessMethod::IndexScan { direction, .. } => *direction,
                _ => unreachable!(),
            };

            let post_filter = post_filter.clone();
            let limit = *limit;

            let stream: QueryScanStream<'a> = Box::pin(make_secondary_scan_stream(
                sec_index,
                bound_as_ref(lower),
                bound_as_ref(upper),
                read_ts,
                direction,
                primary_index,
                post_filter,
                limit,
            ));
            Ok((stream, read_interval))
        }

        AccessMethod::TableScan {
            index_id,
            direction,
            post_filter,
            limit,
            ..
        } => {
            let sec_index = secondary_indexes
                .get(index_id)
                .ok_or_else(|| std::io::Error::other("_created_at index not found"))?;

            let post_filter = post_filter.clone();
            let limit = *limit;

            let stream: QueryScanStream<'a> = Box::pin(make_secondary_scan_stream(
                sec_index,
                Bound::Unbounded,
                Bound::Unbounded,
                read_ts,
                *direction,
                primary_index,
                post_filter,
                limit,
            ));
            Ok((stream, read_interval))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn make_secondary_scan_stream<'a>(
    sec_index: &'a SecondaryIndex,
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
    read_ts: Ts,
    direction: exdb_storage::btree::ScanDirection,
    primary_index: &'a PrimaryIndex,
    post_filter: Option<Filter>,
    limit: Option<usize>,
) -> impl Stream<Item = std::io::Result<ScanRow>> + Send + 'a {
    async_stream::try_stream! {
        use tokio_stream::StreamExt;

        let mut scanner = sec_index.scan_at_ts(lower, upper, read_ts, direction);
        let mut returned = 0usize;

        while let Some(result) = scanner.next().await {
            if let Some(limit) = limit
                && returned >= limit {
                    break;
                }

            let (doc_id, version_ts) = result?;

            // Fetch document body from primary index
            let body_bytes = match primary_index.get_at_ts(&doc_id, read_ts).await? {
                Some(b) => b,
                None => continue, // tombstone race — skip defensively
            };

            let doc = decode_document(&body_bytes)
                .map_err(std::io::Error::other)?;

            // Apply post-filter
            if let Some(ref filter) = post_filter
                && !filter_matches(&doc, filter) {
                    continue;
                }

            returned += 1;
            yield ScanRow {
                doc_id,
                version_ts,
                doc,
            };
        }
    }
}

fn bound_as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn compute_read_interval(method: &AccessMethod) -> ReadIntervalInfo {
    match method {
        AccessMethod::PrimaryGet {
            collection_id,
            doc_id,
        } => {
            // Point interval: [doc_id || 0x00..00, successor(doc_id) || 0x00..00)
            let mut lower = doc_id.as_bytes().to_vec();
            lower.extend_from_slice(&[0u8; 8]);
            let upper_prefix = successor_key(doc_id.as_bytes().as_ref());
            let mut upper = upper_prefix;
            upper.extend_from_slice(&[0u8; 8]);
            ReadIntervalInfo {
                collection_id: *collection_id,
                index_id: PRIMARY_INDEX_ID,
                lower,
                upper: Bound::Excluded(upper),
            }
        }

        AccessMethod::IndexScan {
            collection_id,
            index_id,
            lower,
            upper,
            ..
        } => ReadIntervalInfo {
            collection_id: *collection_id,
            index_id: *index_id,
            lower: match lower {
                Bound::Included(v) => v.clone(),
                Bound::Excluded(v) => v.clone(),
                Bound::Unbounded => vec![0x00],
            },
            upper: match upper {
                Bound::Excluded(v) => Bound::Excluded(v.clone()),
                Bound::Unbounded => Bound::Unbounded,
                Bound::Included(v) => {
                    Bound::Excluded(exdb_docstore::prefix_successor(v))
                }
            },
        },

        AccessMethod::TableScan {
            collection_id,
            index_id,
            ..
        } => ReadIntervalInfo {
            collection_id: *collection_id,
            index_id: *index_id,
            lower: vec![0x00],
            upper: Bound::Unbounded,
        },
    }
}
