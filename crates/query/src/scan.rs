//! Scan execution (DESIGN.md sections 4.1, 4.2, 4.5).
//!
//! Executes an `AccessMethod` by driving L3 iterators through the three-stage
//! pipeline: Source → PostFilter → Terminal.

use crate::access::AccessMethod;
use crate::post_filter::filter_matches;
use exdb_core::encoding::decode_document;
use exdb_core::filter::Filter;
use exdb_core::types::{CollectionId, DocId, IndexId, Ts};
use exdb_docstore::key_encoding::successor_key;
use exdb_docstore::{PrimaryIndex, SecondaryIndex, SecondaryScanner};
use std::collections::HashMap;
use std::ops::Bound;

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

/// Execute an access method against the indexes at the given read timestamp.
///
/// Returns a `QueryScanner` that lazily produces documents through the
/// Source → PostFilter → Terminal pipeline.
pub fn execute_scan<'a>(
    method: &AccessMethod,
    primary_index: &'a PrimaryIndex,
    secondary_indexes: &'a HashMap<IndexId, SecondaryIndex>,
    read_ts: Ts,
) -> std::io::Result<QueryScanner<'a>> {
    let read_interval = compute_read_interval(method);

    match method {
        AccessMethod::PrimaryGet {
            doc_id,
            ..
        } => {
            let body = primary_index.get_at_ts(doc_id, read_ts)?;
            let version_ts = primary_index.get_version_ts(doc_id, read_ts)?;
            let row = match (body, version_ts) {
                (Some(body_bytes), Some(ts)) => {
                    let doc = decode_document(&body_bytes)
                        .map_err(|e| std::io::Error::other(e))?;
                    Some(ScanRow {
                        doc_id: *doc_id,
                        version_ts: ts,
                        doc,
                    })
                }
                _ => None,
            };
            Ok(QueryScanner {
                source: ScanSource::Point(row),
                post_filter: None,
                limit: None,
                read_interval,
                stats: ScanStats::default(),
                primary_index,
                read_ts,
            })
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

            let scanner = sec_index.scan_at_ts(
                bound_as_ref(lower),
                bound_as_ref(upper),
                read_ts,
                direction,
            );

            Ok(QueryScanner {
                source: ScanSource::Secondary(scanner),
                post_filter: post_filter.clone(),
                limit: *limit,
                read_interval,
                stats: ScanStats::default(),
                primary_index,
                read_ts,
            })
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

            let scanner = sec_index.scan_at_ts(
                Bound::Unbounded,
                Bound::Unbounded,
                read_ts,
                *direction,
            );

            Ok(QueryScanner {
                source: ScanSource::Secondary(scanner),
                post_filter: post_filter.clone(),
                limit: *limit,
                read_interval,
                stats: ScanStats::default(),
                primary_index,
                read_ts,
            })
        }
    }
}

enum ScanSource {
    /// Point lookup result (0 or 1 documents).
    Point(Option<ScanRow>),
    /// Secondary index scan — yields (doc_id, ts), needs primary fetch.
    Secondary(SecondaryScanner),
}

/// Lazy iterator over query results.
pub struct QueryScanner<'a> {
    source: ScanSource,
    post_filter: Option<Filter>,
    limit: Option<usize>,
    read_interval: ReadIntervalInfo,
    stats: ScanStats,
    primary_index: &'a PrimaryIndex,
    read_ts: Ts,
}

impl<'a> QueryScanner<'a> {
    /// The read set interval for this scan.
    pub fn read_interval(&self) -> &ReadIntervalInfo {
        &self.read_interval
    }

    /// Accumulated scan statistics.
    pub fn stats(&self) -> &ScanStats {
        &self.stats
    }
}

impl Iterator for QueryScanner<'_> {
    type Item = std::io::Result<ScanRow>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check limit
        if let Some(limit) = self.limit {
            if self.stats.returned_docs >= limit {
                return None;
            }
        }

        match &mut self.source {
            ScanSource::Point(row) => {
                let row = row.take()?;
                self.stats.returned_docs += 1;
                Some(Ok(row))
            }
            ScanSource::Secondary(scanner) => loop {
                // Check limit again inside loop
                if let Some(limit) = self.limit {
                    if self.stats.returned_docs >= limit {
                        return None;
                    }
                }

                let (doc_id, version_ts) = match scanner.next()? {
                    Ok(pair) => pair,
                    Err(e) => return Some(Err(e)),
                };

                // Fetch document body from primary index
                let body_bytes = match self.primary_index.get_at_ts(&doc_id, self.read_ts) {
                    Ok(Some(b)) => b,
                    Ok(None) => continue, // tombstone race — skip defensively
                    Err(e) => return Some(Err(e)),
                };

                self.stats.scanned_docs += 1;
                self.stats.scanned_bytes += body_bytes.len();

                let doc = match decode_document(&body_bytes) {
                    Ok(d) => d,
                    Err(e) => return Some(Err(std::io::Error::other(e))),
                };

                // Apply post-filter
                if let Some(ref filter) = self.post_filter {
                    if !filter_matches(&doc, filter) {
                        continue;
                    }
                }

                self.stats.returned_docs += 1;
                return Some(Ok(ScanRow {
                    doc_id,
                    version_ts,
                    doc,
                }));
            },
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
            let upper_prefix = successor_key(&doc_id.as_bytes().to_vec());
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
