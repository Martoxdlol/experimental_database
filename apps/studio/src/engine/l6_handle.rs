//! L6 Database handle for the studio — wraps the Database struct
//! and provides one-shot transaction convenience methods.

use std::sync::Arc;

use exdb::catalog_cache::{CollectionMeta, IndexMeta};
use exdb::{
    Database, DatabaseConfig, DatabaseError, DocId, FieldPath, Filter,
    RangeExpr, ScanDirection, TransactionOptions, TransactionResult,
};
use serde_json::Value;

#[allow(dead_code)]
/// High-level database handle for the studio UI.
pub struct L6Handle {
    db: Arc<Database>,
}

#[allow(dead_code)]
impl L6Handle {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    // ─── Catalog Reads (no transaction needed) ───

    pub fn list_collections(&self) -> Vec<CollectionMeta> {
        self.db.list_collections()
    }

    pub fn get_collection(&self, name: &str) -> Option<CollectionMeta> {
        self.db.get_collection(name)
    }

    pub fn config(&self) -> &DatabaseConfig {
        self.db.config()
    }

    pub fn name(&self) -> &str {
        self.db.name()
    }

    // ─── Catalog Reads (via transaction for index listing) ───

    pub async fn list_indexes(&self, collection: &str) -> Result<Vec<IndexMeta>, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::readonly())?;
        let indexes = tx.list_indexes(collection)?;
        tx.rollback();
        Ok(indexes)
    }

    // ─── Document Reads ───

    pub async fn get_doc(
        &self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<Option<Value>, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::readonly())?;
        let doc = tx.get(collection, doc_id).await?;
        tx.rollback();
        Ok(doc)
    }

    pub async fn query_docs(
        &self,
        collection: &str,
        index: &str,
        range: &[RangeExpr],
        filter: Option<Filter>,
        direction: Option<ScanDirection>,
        limit: Option<usize>,
    ) -> Result<Vec<Value>, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::readonly())?;
        let docs = tx.query(collection, index, range, filter, direction, limit).await?;
        tx.rollback();
        Ok(docs)
    }

    /// Scan all documents in a collection using the _created_at index.
    pub async fn scan_collection(
        &self,
        collection: &str,
        direction: ScanDirection,
        limit: usize,
    ) -> Result<Vec<Value>, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::readonly())?;
        let docs = tx
            .query(collection, "_created_at", &[], None, Some(direction), Some(limit))
            .await?;
        tx.rollback();
        Ok(docs)
    }

    // ─── Document Writes ───

    pub async fn insert_doc(
        &self,
        collection: &str,
        body: Value,
    ) -> Result<(DocId, TransactionResult), DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        let doc_id = tx.insert(collection, body).await?;
        let result = tx.commit().await?;
        Ok((doc_id, result))
    }

    pub async fn replace_doc(
        &self,
        collection: &str,
        doc_id: &DocId,
        body: Value,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.replace(collection, doc_id, body).await?;
        tx.commit().await
    }

    pub async fn patch_doc(
        &self,
        collection: &str,
        doc_id: &DocId,
        patch: Value,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.patch(collection, doc_id, patch).await?;
        tx.commit().await
    }

    pub async fn delete_doc(
        &self,
        collection: &str,
        doc_id: &DocId,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.delete(collection, doc_id).await?;
        tx.commit().await
    }

    // ─── DDL ───

    pub async fn create_collection(
        &self,
        name: &str,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.create_collection(name).await?;
        tx.commit().await
    }

    pub async fn drop_collection(
        &self,
        name: &str,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.drop_collection(name).await?;
        tx.commit().await
    }

    pub async fn create_index(
        &self,
        collection: &str,
        name: &str,
        fields: Vec<FieldPath>,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.create_index(collection, name, fields).await?;
        tx.commit().await
    }

    pub async fn drop_index(
        &self,
        collection: &str,
        name: &str,
    ) -> Result<TransactionResult, DatabaseError> {
        let mut tx = self.db.begin(TransactionOptions::default())?;
        tx.drop_index(collection, name).await?;
        tx.commit().await
    }
}
