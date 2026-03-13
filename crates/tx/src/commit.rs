use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot, RwLock};
use async_trait::async_trait;
use anyhow::Result;

use exdb_core::types::{CollectionId, IndexId, DocId, Ts};
use exdb_storage::engine::StorageEngine;
use exdb_storage::wal::Lsn;
use exdb_docstore::{PrimaryIndex, SecondaryIndex, make_secondary_key_from_prefix};

use crate::timestamp::TsAllocator;
use crate::read_set::ReadSet;
use crate::write_set::{WriteSet, CatalogMutation, IndexDelta, IndexResolver, MutationOp};
use crate::commit_log::{CommitLog, CommitLogEntry, IndexKeyWrite};
use crate::occ;
use crate::subscriptions::{
    SubscriptionRegistry, SubscriptionMode, SubscriptionId,
    InvalidationEvent, TxId,
};

// ─── WAL record type tags ───
const WAL_RECORD_TX_COMMIT: u8 = 0x01;
const WAL_RECORD_VISIBLE_TS: u8 = 0x09;

// ─── Mutation op bytes in WAL ───
const OP_INSERT: u8 = 0x01;
const OP_REPLACE: u8 = 0x02;
const OP_DELETE: u8 = 0x03;

// ─── Catalog mutation tags in WAL ───
const CAT_CREATE_COLLECTION: u8 = 0x01;
const CAT_DROP_COLLECTION: u8 = 0x02;
const CAT_CREATE_INDEX: u8 = 0x03;
const CAT_DROP_INDEX: u8 = 0x04;

/// Trait for replication — defined in L5, implemented by L6 or L7.
#[async_trait]
pub trait ReplicationHook: Send + Sync {
    /// Replicate a committed WAL record and wait for acknowledgment.
    async fn replicate_and_wait(&self, lsn: Lsn, record: &[u8]) -> Result<()>;

    /// Whether the cluster currently has quorum.
    fn has_quorum(&self) -> bool { true }

    /// Whether the replicator is in hold state.
    fn is_holding(&self) -> bool { false }
}

/// No-op implementation for embedded/single-node usage.
pub struct NoReplication;

#[async_trait]
impl ReplicationHook for NoReplication {
    async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// Trait for applying catalog mutations to the in-memory catalog.
/// Defined in L5, implemented by L6 (which owns CatalogCache + catalog B-trees).
#[async_trait]
pub trait CatalogMutator: Send + Sync {
    /// Apply a catalog mutation (create/drop collection/index).
    async fn apply(&self, mutation: &CatalogMutation, commit_ts: Ts) -> Result<()>;

    /// Rollback a catalog mutation (reverse of apply).
    async fn rollback(&self, mutation: &CatalogMutation) -> Result<()>;
}

/// Request submitted to the commit coordinator.
pub struct CommitRequest {
    pub tx_id: TxId,
    pub begin_ts: Ts,
    pub read_set: ReadSet,
    pub write_set: WriteSet,
    pub subscription: SubscriptionMode,
    pub session_id: u64,
}

/// Result of a commit attempt.
#[derive(Debug)]
pub enum CommitResult {
    Success {
        commit_ts: Ts,
        subscription_id: Option<SubscriptionId>,
        event_rx: Option<mpsc::Receiver<InvalidationEvent>>,
    },
    Conflict {
        error: occ::ConflictError,
        retry: Option<ConflictRetry>,
    },
    QuorumLost,
}

/// Auto-retry info for Subscribe mode on OCC conflict.
#[derive(Debug)]
pub struct ConflictRetry {
    pub new_tx_id: TxId,
    pub new_ts: Ts,
}

/// The commit coordinator — runs as a dedicated tokio task.
pub struct CommitCoordinator {
    ts_allocator: Arc<TsAllocator>,
    visible_ts: Arc<AtomicU64>,
    commit_log: CommitLog,
    subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    storage: Arc<StorageEngine>,
    primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
    secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
    replication: Box<dyn ReplicationHook>,
    catalog_mutator: Arc<dyn CatalogMutator>,
    index_resolver: Arc<dyn IndexResolver>,
    commit_rx: mpsc::Receiver<(CommitRequest, oneshot::Sender<CommitResult>)>,
    next_tx_id: Arc<AtomicU64>,
}

/// Handle for submitting commit requests from any task.
/// Cheaply cloneable.
#[derive(Clone)]
pub struct CommitHandle {
    tx: mpsc::Sender<(CommitRequest, oneshot::Sender<CommitResult>)>,
    visible_ts: Arc<AtomicU64>,
    #[allow(dead_code)] // reserved for future allocate_read_ts use
    ts_allocator: Arc<TsAllocator>,
    pub subscriptions: Arc<RwLock<SubscriptionRegistry>>,
    next_tx_id: Arc<AtomicU64>,
}

impl CommitHandle {
    /// Submit a commit request and wait for the result.
    pub async fn commit(&self, request: CommitRequest) -> CommitResult {
        let (response_tx, response_rx) = oneshot::channel();
        // If the coordinator is gone, return QuorumLost
        if self.tx.send((request, response_tx)).await.is_err() {
            return CommitResult::QuorumLost;
        }
        response_rx.await.unwrap_or(CommitResult::QuorumLost)
    }

    /// Get the current visible_ts (for read-only transaction begin_ts).
    pub fn visible_ts(&self) -> Ts {
        self.visible_ts.load(Ordering::SeqCst)
    }

    /// Allocate a new transaction ID.
    pub fn allocate_tx_id(&self) -> TxId {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Get the subscriptions registry.
    pub fn subscriptions(&self) -> &Arc<RwLock<SubscriptionRegistry>> {
        &self.subscriptions
    }
}

impl CommitCoordinator {
    /// Create a new commit coordinator.
    pub fn new(
        initial_ts: Ts,
        visible_ts_initial: Ts,
        storage: Arc<StorageEngine>,
        primary_indexes: Arc<RwLock<HashMap<CollectionId, Arc<PrimaryIndex>>>>,
        secondary_indexes: Arc<RwLock<HashMap<IndexId, Arc<SecondaryIndex>>>>,
        replication: Box<dyn ReplicationHook>,
        catalog_mutator: Arc<dyn CatalogMutator>,
        index_resolver: Arc<dyn IndexResolver>,
        channel_size: usize,
    ) -> (Self, CommitHandle) {
        let ts_allocator = Arc::new(TsAllocator::new(initial_ts));
        let visible_ts = Arc::new(AtomicU64::new(visible_ts_initial));
        let subscriptions = Arc::new(RwLock::new(SubscriptionRegistry::new()));
        let next_tx_id = Arc::new(AtomicU64::new(0));
        let (commit_tx, commit_rx) = mpsc::channel(channel_size);

        let handle = CommitHandle {
            tx: commit_tx,
            visible_ts: Arc::clone(&visible_ts),
            ts_allocator: Arc::clone(&ts_allocator),
            subscriptions: Arc::clone(&subscriptions),
            next_tx_id: Arc::clone(&next_tx_id),
        };

        let coordinator = CommitCoordinator {
            ts_allocator,
            visible_ts,
            commit_log: CommitLog::new(),
            subscriptions,
            storage,
            primary_indexes,
            secondary_indexes,
            replication,
            catalog_mutator,
            index_resolver,
            commit_rx,
            next_tx_id,
        };

        (coordinator, handle)
    }

    /// Run the commit loop. Processes requests until the channel closes.
    pub async fn run(&mut self) {
        while let Some((request, response_tx)) = self.commit_rx.recv().await {
            self.process_commit(request, response_tx).await;
        }
    }

    async fn process_commit(
        &mut self,
        request: CommitRequest,
        response_tx: oneshot::Sender<CommitResult>,
    ) {
        // ── Step 1: VALIDATE ──
        if !request.write_set.is_empty() {
            let commit_ts_candidate = self.ts_allocator.latest() + 1;
            match occ::validate(
                &request.read_set,
                &self.commit_log,
                request.begin_ts,
                commit_ts_candidate,
            ) {
                Err(conflict) => {
                    let retry = if request.subscription == SubscriptionMode::Subscribe {
                        let new_tx_id =
                            self.next_tx_id.fetch_add(1, Ordering::SeqCst) + 1;
                        let new_ts = self.visible_ts.load(Ordering::SeqCst);
                        Some(ConflictRetry { new_tx_id, new_ts })
                    } else {
                        None
                    };
                    let _ = response_tx
                        .send(CommitResult::Conflict { error: conflict, retry });
                    return;
                }
                Ok(()) => {}
            }
        }

        // ── Step 2: TIMESTAMP ──
        let commit_ts = self.ts_allocator.allocate();

        // ── Step 3: PERSIST (WAL) ──
        let wal_record = serialize_commit_record(&request.write_set, commit_ts);
        let lsn = match self
            .storage
            .append_wal(WAL_RECORD_TX_COMMIT, &wal_record)
            .await
        {
            Ok(l) => l,
            Err(e) => {
                // WAL failure is catastrophic; report QuorumLost to signal unrecoverable error
                tracing::error!("WAL append failed: {e}");
                let _ = response_tx.send(CommitResult::QuorumLost);
                return;
            }
        };

        // ── Step 4a: MATERIALIZE CATALOG ──
        for mutation in &request.write_set.catalog_mutations {
            if let Err(e) = self.catalog_mutator.apply(mutation, commit_ts).await {
                tracing::error!("catalog mutation failed: {e}");
                let _ = response_tx.send(CommitResult::QuorumLost);
                return;
            }
        }

        // ── Step 4b: MATERIALIZE DATA ──
        let index_deltas = match crate::write_set::compute_index_deltas(
            &request.write_set,
            self.index_resolver.as_ref(),
            &*self.primary_indexes.read().await,
        )
        .await
        {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("compute_index_deltas failed: {e}");
                let _ = response_tx.send(CommitResult::QuorumLost);
                return;
            }
        };

        // Apply to primary indexes
        {
            let primaries = self.primary_indexes.read().await;
            for ((collection_id, doc_id), entry) in &request.write_set.mutations {
                if let Some(primary) = primaries.get(collection_id) {
                    let body_bytes = entry
                        .body
                        .as_ref()
                        .map(serde_json::to_vec)
                        .transpose()
                        .unwrap_or(None);
                    if let Err(e) = primary
                        .insert_version(doc_id, commit_ts, body_bytes.as_deref())
                        .await
                    {
                        tracing::error!("primary insert_version failed: {e}");
                        let _ = response_tx.send(CommitResult::QuorumLost);
                        return;
                    }
                }
            }
        }

        // Apply to secondary indexes
        {
            let secondaries = self.secondary_indexes.read().await;
            for delta in &index_deltas {
                // Old entries: NOT removed immediately (MVCC / vacuum handles this)
                if let Some(new_key) = &delta.new_key {
                    if let Some(secondary) = secondaries.get(&delta.index_id) {
                        let full_key =
                            make_secondary_key_from_prefix(new_key, &delta.doc_id, commit_ts);
                        if let Err(e) = secondary.insert_entry(&full_key).await {
                            tracing::error!("secondary insert_entry failed: {e}");
                            let _ = response_tx.send(CommitResult::QuorumLost);
                            return;
                        }
                    }
                }
            }
        }

        // ── Step 5: LOG ──
        let index_writes = index_deltas_to_key_writes(&index_deltas);
        let commit_catalog_mutations = request.write_set.catalog_mutations.clone();
        self.commit_log.append(CommitLogEntry {
            commit_ts,
            index_writes: index_writes.clone(),
            catalog_mutations: commit_catalog_mutations.clone(),
        });

        // ── Step 6: CONCURRENT (replication + invalidation check) ──
        let next_tx_id_arc = Arc::clone(&self.next_tx_id);
        let allocate_tx = move || next_tx_id_arc.fetch_add(1, Ordering::SeqCst) + 1;

        let (replication_result, invalidation_events) = tokio::join!(
            // 6a: replicate
            self.replication.replicate_and_wait(lsn, &wal_record),
            // 6b: subscription invalidation check
            async {
                let mut subs = self.subscriptions.write().await;
                subs.check_invalidation(
                    commit_ts,
                    &index_writes,
                    &commit_catalog_mutations,
                    allocate_tx,
                )
            }
        );

        // ── Step 7: AWAIT REPLICATION ──
        if let Err(e) = replication_result {
            tracing::warn!("replication failed, rolling back: {e}");

            // 7f-a: tombstone inserts to cancel new versions
            {
                let primaries = self.primary_indexes.read().await;
                for ((collection_id, doc_id), entry) in &request.write_set.mutations {
                    if let Some(primary) = primaries.get(collection_id) {
                        match entry.op {
                            MutationOp::Insert | MutationOp::Replace => {
                                // Insert tombstone to logically cancel
                                let _ = primary
                                    .insert_version(doc_id, commit_ts, None)
                                    .await;
                            }
                            MutationOp::Delete => {
                                // Tombstone already inserted; no further action
                            }
                        }
                    }
                }
            }

            // 7f-b: remove secondary index entries we just added
            {
                let secondaries = self.secondary_indexes.read().await;
                for delta in &index_deltas {
                    if let Some(new_key) = &delta.new_key {
                        if let Some(secondary) = secondaries.get(&delta.index_id) {
                            let full_key = make_secondary_key_from_prefix(
                                new_key,
                                &delta.doc_id,
                                commit_ts,
                            );
                            let _ = secondary.remove_entry(&full_key).await;
                        }
                    }
                }
            }

            // 7f-c: rollback catalog mutations (reverse order)
            for mutation in request.write_set.catalog_mutations.iter().rev() {
                let _ = self.catalog_mutator.rollback(mutation).await;
            }

            // 8f: remove from commit log
            self.commit_log.remove(commit_ts);

            // 9f: discard invalidation events
            drop(invalidation_events);

            // 10f: respond
            let _ = response_tx.send(CommitResult::QuorumLost);
            return;
        }

        // ═══ SUCCESS PATH ═══

        // ── Step 8: PERSIST VISIBLE_TS ──
        let _ = self
            .storage
            .append_wal(WAL_RECORD_VISIBLE_TS, &commit_ts.to_be_bytes())
            .await;

        // ── Step 9: ADVANCE visible_ts ──
        self.visible_ts.store(commit_ts, Ordering::SeqCst);

        // ── Step 10: REGISTER SUBSCRIPTION + RESPOND ──
        let (subscription_id, event_rx) = if request.subscription != SubscriptionMode::None {
            let (event_tx, evt_rx) = mpsc::channel(64);
            let mut subs = self.subscriptions.write().await;
            let id = subs.register(
                request.subscription,
                request.session_id,
                request.tx_id,
                commit_ts,
                request.read_set,
                event_tx,
            );
            (Some(id), Some(evt_rx))
        } else {
            (None, None)
        };

        let _ = response_tx.send(CommitResult::Success {
            commit_ts,
            subscription_id,
            event_rx,
        });

        // ── Step 11: PUSH INVALIDATION (fire-and-forget) ──
        let subs = self.subscriptions.read().await;
        subs.push_events(invalidation_events);
    }
}

// ─── Helper functions ───

/// Serialize a commit's write set into a WAL record payload.
pub fn serialize_commit_record(write_set: &WriteSet, commit_ts: Ts) -> Vec<u8> {
    let mut buf = Vec::new();

    // commit_ts[8]
    buf.extend_from_slice(&commit_ts.to_be_bytes());

    // mutation_count[4]
    let mutation_count = write_set.mutations.len() as u32;
    buf.extend_from_slice(&mutation_count.to_be_bytes());

    // mutations
    for ((collection_id, doc_id), entry) in &write_set.mutations {
        buf.extend_from_slice(&collection_id.0.to_be_bytes()); // collection_id[8]
        buf.extend_from_slice(&doc_id.0); // doc_id[16]
        let op_byte = match entry.op {
            MutationOp::Insert => OP_INSERT,
            MutationOp::Replace => OP_REPLACE,
            MutationOp::Delete => OP_DELETE,
        };
        buf.push(op_byte); // op[1]
        if let Some(body) = &entry.body {
            let body_bytes = serde_json::to_vec(body).unwrap_or_default();
            buf.extend_from_slice(&(body_bytes.len() as u32).to_be_bytes()); // body_len[4]
            buf.extend_from_slice(&body_bytes);
        } else {
            buf.extend_from_slice(&0u32.to_be_bytes()); // body_len=0
        }
    }

    // catalog_mutation_count[4]
    let cat_count = write_set.catalog_mutations.len() as u32;
    buf.extend_from_slice(&cat_count.to_be_bytes());

    // catalog mutations
    for mutation in &write_set.catalog_mutations {
        serialize_catalog_mutation(mutation, &mut buf);
    }

    buf
}

fn serialize_catalog_mutation(mutation: &CatalogMutation, buf: &mut Vec<u8>) {
    match mutation {
        CatalogMutation::CreateCollection { name, provisional_id } => {
            let payload = {
                let mut p = Vec::new();
                p.extend_from_slice(&provisional_id.0.to_be_bytes());
                let name_bytes = name.as_bytes();
                p.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                p.extend_from_slice(name_bytes);
                p
            };
            buf.push(CAT_CREATE_COLLECTION);
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
        }
        CatalogMutation::DropCollection { collection_id, name } => {
            let payload = {
                let mut p = Vec::new();
                p.extend_from_slice(&collection_id.0.to_be_bytes());
                let name_bytes = name.as_bytes();
                p.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                p.extend_from_slice(name_bytes);
                p
            };
            buf.push(CAT_DROP_COLLECTION);
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
        }
        CatalogMutation::CreateIndex { collection_id, name, field_paths: _, provisional_id } => {
            // Simplified: serialize collection_id + index_id + name (field_paths not needed for replay)
            let payload = {
                let mut p = Vec::new();
                p.extend_from_slice(&collection_id.0.to_be_bytes());
                p.extend_from_slice(&provisional_id.0.to_be_bytes());
                let name_bytes = name.as_bytes();
                p.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                p.extend_from_slice(name_bytes);
                p
            };
            buf.push(CAT_CREATE_INDEX);
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
        }
        CatalogMutation::DropIndex { collection_id, index_id, name } => {
            let payload = {
                let mut p = Vec::new();
                p.extend_from_slice(&collection_id.0.to_be_bytes());
                p.extend_from_slice(&index_id.0.to_be_bytes());
                let name_bytes = name.as_bytes();
                p.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
                p.extend_from_slice(name_bytes);
                p
            };
            buf.push(CAT_DROP_INDEX);
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
        }
    }
}

/// Deserialize a WAL_RECORD_TX_COMMIT payload for recovery.
pub fn deserialize_commit_record(
    payload: &[u8],
) -> Result<(Ts, Vec<(CollectionId, DocId, MutationOp, Option<Vec<u8>>)>, Vec<CatalogMutation>)> {
    let mut pos = 0;

    let commit_ts = read_u64(payload, &mut pos)?;
    let mutation_count = read_u32(payload, &mut pos)?;

    let mut mutations = Vec::new();
    for _ in 0..mutation_count {
        let coll_id = CollectionId(read_u64(payload, &mut pos)?);
        let doc_id = read_doc_id(payload, &mut pos)?;
        let op_byte = read_u8(payload, &mut pos)?;
        let op = match op_byte {
            OP_INSERT => MutationOp::Insert,
            OP_REPLACE => MutationOp::Replace,
            OP_DELETE => MutationOp::Delete,
            _ => anyhow::bail!("unknown op byte {op_byte}"),
        };
        let body_len = read_u32(payload, &mut pos)? as usize;
        let body = if body_len > 0 {
            let bytes = payload.get(pos..pos + body_len)
                .ok_or_else(|| anyhow::anyhow!("body out of bounds"))?;
            pos += body_len;
            Some(bytes.to_vec())
        } else {
            None
        };
        mutations.push((coll_id, doc_id, op, body));
    }

    let cat_count = read_u32(payload, &mut pos)?;
    let mut catalog_mutations = Vec::new();
    for _ in 0..cat_count {
        let tag = read_u8(payload, &mut pos)?;
        let payload_len = read_u32(payload, &mut pos)? as usize;
        let cat_payload = payload.get(pos..pos + payload_len)
            .ok_or_else(|| anyhow::anyhow!("catalog payload out of bounds"))?;
        pos += payload_len;
        let mut p = 0;
        let mutation = match tag {
            CAT_CREATE_COLLECTION => {
                let provisional_id = CollectionId(read_u64_from(cat_payload, &mut p)?);
                let name = read_string(cat_payload, &mut p)?;
                CatalogMutation::CreateCollection { name, provisional_id }
            }
            CAT_DROP_COLLECTION => {
                let collection_id = CollectionId(read_u64_from(cat_payload, &mut p)?);
                let name = read_string(cat_payload, &mut p)?;
                CatalogMutation::DropCollection { collection_id, name }
            }
            CAT_CREATE_INDEX => {
                let collection_id = CollectionId(read_u64_from(cat_payload, &mut p)?);
                let provisional_id = IndexId(read_u64_from(cat_payload, &mut p)?);
                let name = read_string(cat_payload, &mut p)?;
                CatalogMutation::CreateIndex {
                    collection_id,
                    name,
                    field_paths: vec![], // field_paths not stored in WAL
                    provisional_id,
                }
            }
            CAT_DROP_INDEX => {
                let collection_id = CollectionId(read_u64_from(cat_payload, &mut p)?);
                let index_id = IndexId(read_u64_from(cat_payload, &mut p)?);
                let name = read_string(cat_payload, &mut p)?;
                CatalogMutation::DropIndex { collection_id, index_id, name }
            }
            _ => anyhow::bail!("unknown catalog mutation tag {tag}"),
        };
        catalog_mutations.push(mutation);
    }

    Ok((commit_ts, mutations, catalog_mutations))
}

// ─── Binary parsing helpers ───
fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8> {
    let v = *buf.get(*pos).ok_or_else(|| anyhow::anyhow!("EOF at pos {}", *pos))?;
    *pos += 1;
    Ok(v)
}
fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes = buf.get(*pos..*pos + 4).ok_or_else(|| anyhow::anyhow!("EOF"))?;
    *pos += 4;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}
fn read_u64(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes = buf.get(*pos..*pos + 8).ok_or_else(|| anyhow::anyhow!("EOF"))?;
    *pos += 8;
    Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
}
fn read_u64_from(buf: &[u8], pos: &mut usize) -> Result<u64> {
    read_u64(buf, pos)
}
fn read_doc_id(buf: &[u8], pos: &mut usize) -> Result<DocId> {
    let bytes = buf.get(*pos..*pos + 16).ok_or_else(|| anyhow::anyhow!("EOF"))?;
    *pos += 16;
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(DocId(arr))
}
fn read_string(buf: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_u32(buf, pos)? as usize;
    let bytes = buf.get(*pos..*pos + len).ok_or_else(|| anyhow::anyhow!("EOF"))?;
    *pos += len;
    Ok(String::from_utf8(bytes.to_vec())?)
}

/// Convert IndexDeltas into IndexKeyWrites for the commit log.
pub fn index_deltas_to_key_writes(
    deltas: &[IndexDelta],
) -> BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> {
    let mut map: BTreeMap<(CollectionId, IndexId), Vec<IndexKeyWrite>> = BTreeMap::new();
    for delta in deltas {
        map.entry((delta.collection_id, delta.index_id))
            .or_default()
            .push(IndexKeyWrite {
                doc_id: delta.doc_id,
                old_key: delta.old_key.clone(),
                new_key: delta.new_key.clone(),
            });
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use exdb_core::types::{CollectionId, IndexId};
    use exdb_core::field_path::FieldPath;
    use exdb_storage::engine::StorageConfig;
    use crate::read_set::{ReadInterval, ReadSet};
    use crate::write_set::{WriteSet, IndexInfo};
    use crate::subscriptions::SubscriptionMode;
    use std::ops::Bound;

    // ─── Mock implementations ───

    struct AlwaysOkReplication;
    #[async_trait]
    impl ReplicationHook for AlwaysOkReplication {
        async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    struct FailReplication;
    #[async_trait]
    impl ReplicationHook for FailReplication {
        async fn replicate_and_wait(&self, _lsn: Lsn, _record: &[u8]) -> Result<()> {
            Err(anyhow::anyhow!("quorum lost"))
        }
    }

    struct NoopCatalogMutator;
    #[async_trait]
    impl CatalogMutator for NoopCatalogMutator {
        async fn apply(&self, _: &CatalogMutation, _: Ts) -> Result<()> { Ok(()) }
        async fn rollback(&self, _: &CatalogMutation) -> Result<()> { Ok(()) }
    }

    struct EmptyIndexResolver;
    impl IndexResolver for EmptyIndexResolver {
        fn indexes_for_collection(&self, _: CollectionId) -> Vec<IndexInfo> { vec![] }
    }

    struct SingleFieldIndexResolver {
        collection_id: CollectionId,
        index_id: IndexId,
        field: String,
    }
    impl IndexResolver for SingleFieldIndexResolver {
        fn indexes_for_collection(&self, coll: CollectionId) -> Vec<IndexInfo> {
            if coll == self.collection_id {
                vec![IndexInfo {
                    index_id: self.index_id,
                    collection_id: coll,
                    field_paths: vec![FieldPath::single(&self.field)],
                }]
            } else {
                vec![]
            }
        }
    }

    fn cid(n: u64) -> CollectionId { CollectionId(n) }
    fn iid(n: u64) -> IndexId { IndexId(n) }
    fn doc(n: u8) -> DocId { DocId({let mut b = [0u8; 16]; b[15] = n; b}) }

    async fn make_coordinator(
        replication: Box<dyn ReplicationHook>,
        index_resolver: Arc<dyn IndexResolver>,
    ) -> (CommitCoordinator, CommitHandle, Arc<StorageEngine>) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let primaries = Arc::new(RwLock::new(HashMap::new()));
        let secondaries = Arc::new(RwLock::new(HashMap::new()));
        let catalog_mutator = Arc::new(NoopCatalogMutator) as Arc<dyn CatalogMutator>;
        let (coord, handle) = CommitCoordinator::new(
            0, 0,
            Arc::clone(&engine),
            primaries,
            secondaries,
            replication,
            catalog_mutator,
            index_resolver,
            256,
        );
        (coord, handle, engine)
    }

    #[allow(dead_code)]
    async fn make_coordinator_with_primary(
        collection_id: CollectionId,
        index_resolver: Arc<dyn IndexResolver>,
    ) -> (CommitCoordinator, CommitHandle, Arc<StorageEngine>, Arc<PrimaryIndex>) {
        let engine = Arc::new(StorageEngine::open_in_memory(StorageConfig::default()).await.unwrap());
        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(PrimaryIndex::new(btree, Arc::clone(&engine), 256));

        let primaries = Arc::new(RwLock::new(HashMap::from([(collection_id, Arc::clone(&primary))])));
        let secondaries = Arc::new(RwLock::new(HashMap::new()));
        let catalog_mutator = Arc::new(NoopCatalogMutator) as Arc<dyn CatalogMutator>;
        let replication = Box::new(AlwaysOkReplication) as Box<dyn ReplicationHook>;
        let (coord, handle) = CommitCoordinator::new(
            0, 0,
            Arc::clone(&engine),
            primaries,
            secondaries,
            replication,
            catalog_mutator,
            index_resolver,
            256,
        );
        (coord, handle, engine, primary)
    }

    fn make_commit_request(
        tx_id: TxId,
        begin_ts: Ts,
        write_set: WriteSet,
    ) -> CommitRequest {
        CommitRequest {
            tx_id,
            begin_ts,
            read_set: ReadSet::new(),
            write_set,
            subscription: SubscriptionMode::None,
            session_id: 0,
        }
    }

    async fn run_commit(
        coord: &mut CommitCoordinator,
        request: CommitRequest,
    ) -> CommitResult {
        let (response_tx, response_rx) = oneshot::channel();
        coord.process_commit(request, response_tx).await;
        response_rx.await.unwrap()
    }

    // ─── Tests ───

    #[tokio::test]
    async fn t7_commit_success_basic() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({"name": "Alice"}));

        let request = make_commit_request(1, 0, ws);
        let result = run_commit(&mut coord, request).await;
        match result {
            CommitResult::Success { commit_ts, .. } => assert!(commit_ts > 0),
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_commit_occ_conflict() {
        // Simulate a prior commit (ts=1) that wrote key=15 on (cid(1), iid(1)).
        // Then a new commit B that began before ts=1 and reads [10,20) on that index
        // should be detected as conflicting.
        let (mut coord, _, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Advance the ts allocator to ts=1 to simulate a prior committed transaction.
        coord.ts_allocator.allocate(); // now ts_allocator.latest() == 1

        // Manually inject a commit log entry as if commit A (ts=1) wrote key=15
        // on (cid(1), iid(1)).
        let mut index_writes = BTreeMap::new();
        index_writes.insert((cid(1), iid(1)), vec![IndexKeyWrite {
            doc_id: doc(1),
            old_key: None,
            new_key: Some(vec![15]),
        }]);
        coord.commit_log.append(CommitLogEntry {
            commit_ts: 1,
            index_writes,
            catalog_mutations: vec![],
        });

        // Commit B: begins at ts=0 (before commit A), reads interval [10,20) on same index.
        // Key 15 falls inside [10,20) → conflict.
        let mut rs_b = ReadSet::new();
        rs_b.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 0,
            lower: vec![10],
            upper: Bound::Excluded(vec![20]),
        });
        let mut ws_b = WriteSet::new();
        ws_b.insert(cid(2), doc(2), serde_json::json!({"val": 99}));
        let req_b = CommitRequest {
            tx_id: 2,
            begin_ts: 0, // started before A
            read_set: rs_b,
            write_set: ws_b,
            subscription: SubscriptionMode::None,
            session_id: 0,
        };
        let result_b = run_commit(&mut coord, req_b).await;
        assert!(matches!(result_b, CommitResult::Conflict { .. }));
    }

    #[tokio::test]
    async fn t7_commit_occ_no_conflict() {
        let (mut coord, _, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Two commits with non-overlapping read/write sets
        let mut ws1 = WriteSet::new();
        ws1.insert(cid(1), doc(1), serde_json::json!({}));
        let r1 = run_commit(&mut coord, make_commit_request(1, 0, ws1)).await;
        assert!(matches!(r1, CommitResult::Success { .. }));

        let mut ws2 = WriteSet::new();
        ws2.insert(cid(2), doc(2), serde_json::json!({}));
        let r2 = run_commit(&mut coord, make_commit_request(2, 0, ws2)).await;
        assert!(matches!(r2, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn t7_commit_subscribe_conflict_retry() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Inject a commit log entry to force conflict
        let mut index_writes = BTreeMap::new();
        index_writes.insert((cid(1), iid(1)), vec![IndexKeyWrite {
            doc_id: doc(1),
            old_key: None,
            new_key: Some(vec![15]),
        }]);
        coord.commit_log.append(CommitLogEntry {
            commit_ts: 1,
            index_writes,
            catalog_mutations: vec![],
        });
        coord.ts_allocator.allocate(); // advance ts past 1

        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 0,
            lower: vec![10],
            upper: Bound::Excluded(vec![20]),
        });
        let mut ws = WriteSet::new();
        ws.insert(cid(2), doc(2), serde_json::json!({}));

        let req = CommitRequest {
            tx_id: 99,
            begin_ts: 0,
            read_set: rs,
            write_set: ws,
            subscription: SubscriptionMode::Subscribe,
            session_id: 1,
        };
        let result = run_commit(&mut coord, req).await;
        match result {
            CommitResult::Conflict { retry: Some(retry), .. } => {
                assert!(retry.new_tx_id > 0);
            }
            other => panic!("expected Conflict with retry, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_commit_subscribe_none_conflict_no_retry() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut index_writes = BTreeMap::new();
        index_writes.insert((cid(1), iid(1)), vec![IndexKeyWrite {
            doc_id: doc(1), old_key: None, new_key: Some(vec![15]),
        }]);
        coord.commit_log.append(CommitLogEntry { commit_ts: 1, index_writes, catalog_mutations: vec![] });
        coord.ts_allocator.allocate();

        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval { query_id: 0, lower: vec![10], upper: Bound::Excluded(vec![20]) });
        let mut ws = WriteSet::new();
        ws.insert(cid(2), doc(2), serde_json::json!({}));
        let req = CommitRequest {
            tx_id: 99, begin_ts: 0, read_set: rs, write_set: ws,
            subscription: SubscriptionMode::None, session_id: 0,
        };
        let result = run_commit(&mut coord, req).await;
        match result {
            CommitResult::Conflict { retry: None, .. } => {}
            other => panic!("expected Conflict with no retry, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_commit_registers_subscription() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({}));
        let req = CommitRequest {
            tx_id: 1, begin_ts: 0, read_set: ReadSet::new(), write_set: ws,
            subscription: SubscriptionMode::Notify, session_id: 0,
        };
        let result = run_commit(&mut coord, req).await;
        match result {
            CommitResult::Success { subscription_id: Some(_), event_rx: Some(_), .. } => {}
            other => panic!("expected Success with subscription, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_commit_no_subscription_mode_none() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({}));
        let req = make_commit_request(1, 0, ws);
        let result = run_commit(&mut coord, req).await;
        match result {
            CommitResult::Success { subscription_id: None, event_rx: None, .. } => {}
            other => panic!("expected Success with no subscription, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_commit_visible_ts_advances() {
        let (mut coord, handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        assert_eq!(handle.visible_ts(), 0);

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({}));
        let result = run_commit(&mut coord, make_commit_request(1, 0, ws)).await;
        let commit_ts = match result {
            CommitResult::Success { commit_ts, .. } => commit_ts,
            _ => panic!(),
        };
        assert_eq!(handle.visible_ts(), commit_ts);
    }

    #[tokio::test]
    async fn t7_commit_visible_ts_not_advanced_until_replication() {
        // Use a replication hook that fails immediately — visible_ts should NOT advance
        let (mut coord, handle, engine) =
            make_coordinator(Box::new(FailReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let before_ts = handle.visible_ts();
        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({}));
        let result = run_commit(&mut coord, make_commit_request(1, 0, ws)).await;
        assert!(matches!(result, CommitResult::QuorumLost));
        // visible_ts should NOT have advanced
        assert_eq!(handle.visible_ts(), before_ts);
    }

    #[tokio::test]
    async fn t7_commit_invalidation_fires() {
        let (mut coord, handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Register a Watch subscription watching (cid(1), iid(1)) with Unbounded range.
        // Index keys are order-preserving encoded scalars; using Unbounded ensures any
        // write to this index triggers the subscription regardless of the encoding.
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 0,
            lower: vec![],
            upper: Bound::Unbounded,
        });
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let sub_id = handle
            .subscriptions()
            .write()
            .await
            .register(SubscriptionMode::Watch, 0, 0, 0, rs, event_tx);

        // Do a commit that writes val=15 on (cid(1), iid(1))
        // For this we need an index resolver that returns iid(1) for cid(1)
        let resolver = Arc::new(SingleFieldIndexResolver {
            collection_id: cid(1),
            index_id: iid(1),
            field: "val".to_string(),
        }) as Arc<dyn IndexResolver>;

        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        coord.primary_indexes.write().await.insert(cid(1), Arc::clone(&primary));

        let btree2 = engine.create_btree().await.unwrap();
        let secondary = Arc::new(SecondaryIndex::new(btree2, Arc::clone(&primary)));
        coord.secondary_indexes.write().await.insert(iid(1), secondary);

        coord.index_resolver = resolver;

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({"val": 15}));
        let result = run_commit(&mut coord, make_commit_request(1, 0, ws)).await;
        assert!(matches!(result, CommitResult::Success { .. }));

        // The subscription should have received an event
        let event = event_rx.try_recv();
        assert!(event.is_ok(), "expected invalidation event, got none");
        assert_eq!(event.unwrap().subscription_id, sub_id);
    }

    #[tokio::test]
    async fn t7_commit_ordering() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut prev_ts = 0u64;
        for i in 0..5u8 {
            let mut ws = WriteSet::new();
            ws.insert(cid(1), doc(i), serde_json::json!({}));
            let result = run_commit(&mut coord, make_commit_request(i as u64, 0, ws)).await;
            match result {
                CommitResult::Success { commit_ts, .. } => {
                    assert!(commit_ts > prev_ts, "commit_ts not strictly increasing");
                    prev_ts = commit_ts;
                }
                _ => panic!("commit {i} failed"),
            }
        }
    }

    #[tokio::test]
    async fn t7_commit_sequential_processing() {
        // Verify that 3 sequential commits each get a strictly increasing, distinct timestamp.
        // The coordinator is !Send (storage uses parking_lot internally), so we drive it
        // directly rather than spawning it in a background task.
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut commit_timestamps = Vec::new();
        for i in 0..3u8 {
            let mut ws = WriteSet::new();
            ws.insert(cid(1), doc(i), serde_json::json!({}));
            match run_commit(&mut coord, make_commit_request(i as u64, 0, ws)).await {
                CommitResult::Success { commit_ts, .. } => commit_timestamps.push(commit_ts),
                other => panic!("unexpected: {:?}", other),
            }
        }

        // All timestamps must be strictly increasing and distinct
        let unique: std::collections::HashSet<_> = commit_timestamps.iter().collect();
        assert_eq!(unique.len(), 3);
        for w in commit_timestamps.windows(2) {
            assert!(w[0] < w[1], "timestamps not strictly increasing");
        }
    }

    #[tokio::test]
    async fn t7_commit_quorum_lost_rollback() {
        let (mut coord, handle, engine) =
            make_coordinator(Box::new(FailReplication), Arc::new(EmptyIndexResolver)).await;

        let btree = engine.create_btree().await.unwrap();
        let primary = Arc::new(PrimaryIndex::new(btree, Arc::clone(&engine), 256));
        coord.primary_indexes.write().await.insert(cid(1), Arc::clone(&primary));

        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({"x": 1}));
        let result = run_commit(&mut coord, make_commit_request(1, 0, ws)).await;

        assert!(matches!(result, CommitResult::QuorumLost));
        // visible_ts should NOT have advanced
        assert_eq!(handle.visible_ts(), 0);
        // Commit log should have the entry removed (rollback step 8f)
        assert!(coord.commit_log.is_empty());
    }

    #[tokio::test]
    async fn t7_commit_catalog_only() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: cid(1),
        });
        assert!(!ws.is_empty()); // catalog-only is NOT empty

        let result = run_commit(&mut coord, make_commit_request(1, 0, ws)).await;
        assert!(matches!(result, CommitResult::Success { .. }));
    }

    #[tokio::test]
    async fn t7_commit_catalog_occ_conflict() {
        let (mut coord, _handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Inject a commit log entry with a catalog mutation for "users"
        coord.commit_log.append(CommitLogEntry {
            commit_ts: 1,
            index_writes: BTreeMap::new(),
            catalog_mutations: vec![CatalogMutation::CreateCollection {
                name: "users".into(),
                provisional_id: cid(5),
            }],
        });
        coord.ts_allocator.allocate(); // push past ts=1

        // Transaction A reads CollectionByName("users"), then tries to commit
        let mut rs = ReadSet::new();
        rs.add_catalog_read(crate::read_set::CatalogRead::CollectionByName("users".into()));
        let mut ws = WriteSet::new();
        ws.insert(cid(2), doc(1), serde_json::json!({}));
        let req = CommitRequest {
            tx_id: 99, begin_ts: 0, read_set: rs, write_set: ws,
            subscription: SubscriptionMode::None, session_id: 0,
        };
        let result = run_commit(&mut coord, req).await;
        match result {
            CommitResult::Conflict { error, .. } => {
                assert!(matches!(error.kind, occ::ConflictKind::Catalog { .. }));
            }
            other => panic!("expected Catalog conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn t7_shutdown_graceful() {
        let (mut coord, handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Drop the handle — the channel closes immediately
        drop(handle);

        // run() should exit cleanly since the channel is already closed
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coord.run(),
        )
        .await
        .expect("run() did not exit within timeout");
    }

    #[tokio::test]
    async fn t7_event_channel_full_no_block() {
        let (_coord, handle, engine) =
            make_coordinator(Box::new(AlwaysOkReplication), Arc::new(EmptyIndexResolver)).await;
        let _ = engine;

        // Register subscription with small channel capacity (1) watching (cid(1), iid(1))
        let mut rs = ReadSet::new();
        rs.add_interval(cid(1), iid(1), ReadInterval {
            query_id: 0, lower: vec![10], upper: Bound::Excluded(vec![20]),
        });
        let (event_tx, _rx) = mpsc::channel(1); // capacity 1
        handle.subscriptions().write().await.register(
            SubscriptionMode::Watch, 0, 0, 0, rs, event_tx,
        );

        // Inject index writes to trigger invalidation on both commits
        let make_writes = || {
            let mut iw = BTreeMap::new();
            iw.insert((cid(1), iid(1)), vec![IndexKeyWrite {
                doc_id: doc(1), old_key: None, new_key: Some(vec![15]),
            }]);
            iw
        };

        // Manually call check_invalidation twice to simulate rapid commits
        {
            let mut subs = handle.subscriptions().write().await;
            let _ = subs.check_invalidation(1, &make_writes(), &[], || 99);
            // First event fills the channel; second should not block
            subs.push_events(vec![crate::subscriptions::InvalidationEvent {
                subscription_id: 1,
                affected_query_ids: vec![0],
                commit_ts: 1,
                continuation: None,
            }]);
            // Second push — channel is full, try_send should drop
            subs.push_events(vec![crate::subscriptions::InvalidationEvent {
                subscription_id: 1,
                affected_query_ids: vec![0],
                commit_ts: 2,
                continuation: None,
            }]);
        }
        // No panic, no block — test passes
    }

    // ─── WAL serialization round-trip ───

    #[test]
    fn t7_wal_serialize_deserialize_roundtrip() {
        let mut ws = WriteSet::new();
        ws.insert(cid(1), doc(1), serde_json::json!({"name": "Alice", "age": 30}));
        ws.replace(cid(1), doc(2), serde_json::json!({"name": "Bob"}), 5);
        ws.delete(cid(2), doc(3), 10);

        let commit_ts = 42u64;
        let payload = serialize_commit_record(&ws, commit_ts);
        let (ts, mutations, catalog_mutations) = deserialize_commit_record(&payload).unwrap();

        assert_eq!(ts, commit_ts);
        assert_eq!(mutations.len(), 3);
        assert!(catalog_mutations.is_empty());
    }

    #[test]
    fn t7_wal_serialize_catalog_roundtrip() {
        let mut ws = WriteSet::new();
        ws.add_catalog_mutation(CatalogMutation::CreateCollection {
            name: "users".into(),
            provisional_id: cid(42),
        });
        ws.add_catalog_mutation(CatalogMutation::DropIndex {
            collection_id: cid(1),
            index_id: iid(5),
            name: "by_email".into(),
        });

        let payload = serialize_commit_record(&ws, 100);
        let (ts, mutations, catalog_mutations) = deserialize_commit_record(&payload).unwrap();

        assert_eq!(ts, 100);
        assert!(mutations.is_empty());
        assert_eq!(catalog_mutations.len(), 2);
        assert!(matches!(catalog_mutations[0], CatalogMutation::CreateCollection { name: ref n, .. } if n == "users"));
    }

    #[test]
    fn t7_index_deltas_to_key_writes() {
        let deltas = vec![
            IndexDelta { index_id: iid(1), collection_id: cid(1), doc_id: doc(1), old_key: None, new_key: Some(vec![10]) },
            IndexDelta { index_id: iid(1), collection_id: cid(1), doc_id: doc(2), old_key: Some(vec![5]), new_key: Some(vec![15]) },
            IndexDelta { index_id: iid(2), collection_id: cid(1), doc_id: doc(1), old_key: None, new_key: Some(vec![20]) },
        ];
        let writes = index_deltas_to_key_writes(&deltas);
        assert_eq!(writes.len(), 2); // two distinct (coll, idx) pairs
        assert_eq!(writes[&(cid(1), iid(1))].len(), 2);
        assert_eq!(writes[&(cid(1), iid(2))].len(), 1);
    }

    #[test]
    fn t7_no_replication_is_no_op() {
        // NoReplication should not block and always return Ok
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let nr = NoReplication;
            assert!(nr.replicate_and_wait(0, &[]).await.is_ok());
            assert!(nr.has_quorum());
            assert!(!nr.is_holding());
        });
    }
}
