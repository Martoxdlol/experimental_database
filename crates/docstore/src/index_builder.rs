//! D6: Index Builder — background index building.
//!
//! Scans the primary B-tree at a snapshot timestamp and inserts entries into
//! a `Building`-state secondary index.

use crate::array_indexing::compute_index_entries;
use crate::key_encoding::make_secondary_key_from_prefix;
use crate::primary_index::PrimaryIndex;
use crate::secondary_index::SecondaryIndex;
use exdb_core::encoding::decode_document;
use exdb_core::field_path::FieldPath;
use exdb_core::types::Ts;
use exdb_storage::btree::ScanDirection;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Progress report emitted periodically during build.
#[derive(Debug, Clone)]
pub struct BuildProgress {
    pub docs_scanned: u64,
    pub entries_inserted: u64,
    pub elapsed_ms: u64,
}

/// Builds a secondary index by scanning the primary index at a snapshot timestamp.
pub struct IndexBuilder {
    primary: Arc<PrimaryIndex>,
    secondary: Arc<SecondaryIndex>,
    field_paths: Vec<FieldPath>,
}

impl IndexBuilder {
    /// Create a new IndexBuilder.
    pub fn new(
        primary: Arc<PrimaryIndex>,
        secondary: Arc<SecondaryIndex>,
        field_paths: Vec<FieldPath>,
    ) -> Self {
        Self {
            primary,
            secondary,
            field_paths,
        }
    }

    /// Run the index build: scan primary at `build_snapshot_ts`,
    /// insert entries into the secondary index.
    ///
    /// Returns the number of entries inserted.
    pub async fn build(
        &self,
        build_snapshot_ts: Ts,
        progress_tx: Option<tokio::sync::watch::Sender<BuildProgress>>,
    ) -> Result<u64, std::io::Error> {
        let start = std::time::Instant::now();
        let mut scanner = self.primary.scan_at_ts(build_snapshot_ts, ScanDirection::Forward);
        let mut entries_inserted: u64 = 0;
        let mut docs_scanned: u64 = 0;

        while let Some(result) = scanner.next().await {
            let (doc_id, version_ts, body_bytes) = result?;
            docs_scanned += 1;

            let doc = decode_document(&body_bytes)
                .map_err(|e| std::io::Error::other(format!("document decode error: {e}")))?;

            let key_prefixes = compute_index_entries(&doc, &self.field_paths)
                .map_err(std::io::Error::other)?;

            for prefix in key_prefixes {
                let full_key = make_secondary_key_from_prefix(&prefix, &doc_id, version_ts);
                self.secondary.insert_entry(&full_key).await?;
                entries_inserted += 1;
            }

            if docs_scanned.is_multiple_of(1000) {
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(BuildProgress {
                        docs_scanned,
                        entries_inserted,
                        elapsed_ms: start.elapsed().as_millis() as u64,
                    });
                }
                tokio::task::yield_now().await;
            }
        }

        Ok(entries_inserted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exdb_core::encoding::encode_document;
    use exdb_core::types::DocId;
    use exdb_storage::engine::{StorageConfig, StorageEngine};

    async fn setup() -> (Arc<StorageEngine>, Arc<PrimaryIndex>, Arc<SecondaryIndex>) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let primary_btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(PrimaryIndex::new(primary_btree, engine.clone(), 4096));
        let sec_btree = engine.create_btree().await.unwrap();
        let sec = Arc::new(SecondaryIndex::new(sec_btree, primary.clone()));
        (engine, primary, sec)
    }

    fn doc_id(n: u8) -> DocId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        DocId(bytes)
    }

    #[tokio::test]
    async fn build_empty_collection() {
        let (_engine, primary, secondary) = setup().await;
        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(10, None).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn build_single_doc() {
        let (_engine, primary, secondary) = setup().await;
        let doc = serde_json::json!({"name": "Alice"});
        let body = encode_document(&doc);
        primary
            .insert_version(&doc_id(1), 5, Some(&body))
            .await.unwrap();

        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(10, None).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn build_multiple_docs() {
        let (_engine, primary, secondary) = setup().await;
        for i in 0..10u8 {
            let doc = serde_json::json!({"name": format!("user_{i}")});
            let body = encode_document(&doc);
            primary
                .insert_version(&doc_id(i), 5, Some(&body))
                .await.unwrap();
        }

        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(10, None).await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn build_with_array() {
        let (_engine, primary, secondary) = setup().await;
        let doc = serde_json::json!({"tags": ["a", "b", "c"]});
        let body = encode_document(&doc);
        primary
            .insert_version(&doc_id(1), 5, Some(&body))
            .await.unwrap();

        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("tags")]);
        let count = builder.build(10, None).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn build_snapshot_isolation() {
        let (_engine, primary, secondary) = setup().await;
        let doc1 = serde_json::json!({"name": "Alice"});
        let doc2 = serde_json::json!({"name": "Bob"});
        primary
            .insert_version(&doc_id(1), 5, Some(&encode_document(&doc1)))
            .await.unwrap();
        primary
            .insert_version(&doc_id(2), 10, Some(&encode_document(&doc2)))
            .await.unwrap();

        // Build at snapshot_ts=7, should only see doc1
        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(7, None).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn build_skips_tombstones() {
        let (_engine, primary, secondary) = setup().await;
        let doc = serde_json::json!({"name": "Alice"});
        primary
            .insert_version(&doc_id(1), 5, Some(&encode_document(&doc)))
            .await.unwrap();
        primary.insert_version(&doc_id(1), 8, None).await.unwrap(); // tombstone

        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(10, None).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn build_with_progress() {
        let (_engine, primary, secondary) = setup().await;
        for i in 0..5u8 {
            let doc = serde_json::json!({"name": format!("user_{i}")});
            primary
                .insert_version(&doc_id(i), 5, Some(&encode_document(&doc)))
                .await.unwrap();
        }

        let (tx, mut rx) = tokio::sync::watch::channel(BuildProgress {
            docs_scanned: 0,
            entries_inserted: 0,
            elapsed_ms: 0,
        });

        let builder =
            IndexBuilder::new(primary, secondary, vec![FieldPath::single("name")]);
        let count = builder.build(10, Some(tx)).await.unwrap();
        assert_eq!(count, 5);
        // rx should have the final state or at least initial
        let progress = rx.borrow_and_update().clone();
        // With only 5 docs, we won't hit the 1000-doc progress boundary,
        // so it stays at default
        let _ = progress;
    }
}
