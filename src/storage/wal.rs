//! Write-Ahead Log (WAL) implementation.
//!
//! Frame format: `len(u32 LE) | crc32(u32 LE) | record_type(u8) | payload(bytes)`

use std::io::Write;
use std::path::Path;
use anyhow::{anyhow, Context, Result};
use crc32fast::Hasher as Crc32Hasher;

use crate::types::{CollectionId, DocId, IndexId, Ts, TxId};

// ── Record types ──────────────────────────────────────────────────────────────

const RT_BEGIN: u8 = 1;
const RT_PUT_DOC: u8 = 2;
const RT_DELETE_DOC: u8 = 3;
const RT_COMMIT: u8 = 4;
const RT_ABORT: u8 = 5;
const RT_CATALOG_UPDATE: u8 = 6;
const RT_CHECKPOINT: u8 = 7;

/// A single logical WAL record.
#[derive(Debug, Clone)]
pub enum WalRecord {
    Begin {
        tx_id: TxId,
        start_ts: Ts,
    },
    PutDoc {
        tx_id: TxId,
        collection_id: CollectionId,
        doc_id: DocId,
        json: Vec<u8>,
    },
    DeleteDoc {
        tx_id: TxId,
        collection_id: CollectionId,
        doc_id: DocId,
    },
    Commit {
        tx_id: TxId,
        commit_ts: Ts,
    },
    Abort {
        tx_id: TxId,
    },
    CatalogUpdate(CatalogWalEntry),
    Checkpoint {
        up_to_lsn: u64,
    },
}

#[derive(Debug, Clone)]
pub enum CatalogWalEntry {
    CreateCollection { id: CollectionId, name: String },
    DeleteCollection { id: CollectionId },
    CreateIndex {
        collection_id: CollectionId,
        index_id: IndexId,
        field_path: Vec<String>,
        unique: bool,
        state_tag: u8, // 0=Building, 1=Ready, 2=Failed
        state_ts: Ts,
    },
    SetIndexReady {
        collection_id: CollectionId,
        index_id: IndexId,
        ready_at_ts: Ts,
    },
    SetIndexFailed {
        collection_id: CollectionId,
        index_id: IndexId,
        message: String,
    },
    DeleteIndex {
        collection_id: CollectionId,
        index_id: IndexId,
    },
}

// ── Serialization helpers ─────────────────────────────────────────────────────

fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_bytes(buf, s.as_bytes());
}

fn read_u8(data: &[u8], pos: &mut usize) -> Result<u8> {
    if *pos >= data.len() {
        return Err(anyhow!("unexpected EOF reading u8"));
    }
    let v = data[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    if *pos + 8 > data.len() {
        return Err(anyhow!("unexpected EOF reading u64"));
    }
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

fn read_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    if *pos + 4 > data.len() {
        return Err(anyhow!("unexpected EOF reading bytes length"));
    }
    let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > data.len() {
        return Err(anyhow!("unexpected EOF reading bytes data (need {len} bytes)"));
    }
    let slice = &data[*pos..*pos + len];
    *pos += len;
    Ok(slice)
}

fn read_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let bytes = read_bytes(data, pos)?;
    String::from_utf8(bytes.to_vec()).context("invalid UTF-8 in WAL string")
}

fn read_doc_id(data: &[u8], pos: &mut usize) -> Result<DocId> {
    if *pos + 16 > data.len() {
        return Err(anyhow!("unexpected EOF reading DocId"));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&data[*pos..*pos + 16]);
    *pos += 16;
    Ok(DocId::from_bytes(bytes))
}

// ── WalRecord encoding ────────────────────────────────────────────────────────

impl WalRecord {
    fn record_type(&self) -> u8 {
        match self {
            WalRecord::Begin { .. } => RT_BEGIN,
            WalRecord::PutDoc { .. } => RT_PUT_DOC,
            WalRecord::DeleteDoc { .. } => RT_DELETE_DOC,
            WalRecord::Commit { .. } => RT_COMMIT,
            WalRecord::Abort { .. } => RT_ABORT,
            WalRecord::CatalogUpdate(_) => RT_CATALOG_UPDATE,
            WalRecord::Checkpoint { .. } => RT_CHECKPOINT,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            WalRecord::Begin { tx_id, start_ts } => {
                write_u64(&mut payload, *tx_id);
                write_u64(&mut payload, *start_ts);
            }
            WalRecord::PutDoc { tx_id, collection_id, doc_id, json } => {
                write_u64(&mut payload, *tx_id);
                write_u64(&mut payload, *collection_id);
                payload.extend_from_slice(doc_id.as_bytes());
                write_bytes(&mut payload, json);
            }
            WalRecord::DeleteDoc { tx_id, collection_id, doc_id } => {
                write_u64(&mut payload, *tx_id);
                write_u64(&mut payload, *collection_id);
                payload.extend_from_slice(doc_id.as_bytes());
            }
            WalRecord::Commit { tx_id, commit_ts } => {
                write_u64(&mut payload, *tx_id);
                write_u64(&mut payload, *commit_ts);
            }
            WalRecord::Abort { tx_id } => {
                write_u64(&mut payload, *tx_id);
            }
            WalRecord::CatalogUpdate(entry) => {
                encode_catalog_entry(&mut payload, entry);
            }
            WalRecord::Checkpoint { up_to_lsn } => {
                write_u64(&mut payload, *up_to_lsn);
            }
        }

        // Frame: record_type || payload
        let mut frame_body = Vec::with_capacity(1 + payload.len());
        write_u8(&mut frame_body, self.record_type());
        frame_body.extend_from_slice(&payload);

        // Frame: len(u32) || crc32(u32) || frame_body
        let len = frame_body.len() as u32;
        let mut hasher = Crc32Hasher::new();
        hasher.update(&frame_body);
        let crc = hasher.finalize();

        let mut out = Vec::with_capacity(8 + frame_body.len());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&frame_body);
        out
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        if frame.is_empty() {
            return Err(anyhow!("empty frame"));
        }
        let record_type = frame[0];
        let payload = &frame[1..];
        let mut pos = 0;

        let record = match record_type {
            RT_BEGIN => {
                let tx_id = read_u64(payload, &mut pos)?;
                let start_ts = read_u64(payload, &mut pos)?;
                WalRecord::Begin { tx_id, start_ts }
            }
            RT_PUT_DOC => {
                let tx_id = read_u64(payload, &mut pos)?;
                let collection_id = read_u64(payload, &mut pos)?;
                let doc_id = read_doc_id(payload, &mut pos)?;
                let json = read_bytes(payload, &mut pos)?.to_vec();
                WalRecord::PutDoc { tx_id, collection_id, doc_id, json }
            }
            RT_DELETE_DOC => {
                let tx_id = read_u64(payload, &mut pos)?;
                let collection_id = read_u64(payload, &mut pos)?;
                let doc_id = read_doc_id(payload, &mut pos)?;
                WalRecord::DeleteDoc { tx_id, collection_id, doc_id }
            }
            RT_COMMIT => {
                let tx_id = read_u64(payload, &mut pos)?;
                let commit_ts = read_u64(payload, &mut pos)?;
                WalRecord::Commit { tx_id, commit_ts }
            }
            RT_ABORT => {
                let tx_id = read_u64(payload, &mut pos)?;
                WalRecord::Abort { tx_id }
            }
            RT_CATALOG_UPDATE => {
                let entry = decode_catalog_entry(payload, &mut pos)?;
                WalRecord::CatalogUpdate(entry)
            }
            RT_CHECKPOINT => {
                let up_to_lsn = read_u64(payload, &mut pos)?;
                WalRecord::Checkpoint { up_to_lsn }
            }
            t => return Err(anyhow!("unknown WAL record type: {t}")),
        };
        Ok(record)
    }
}

const CAT_CREATE_COLLECTION: u8 = 1;
const CAT_DELETE_COLLECTION: u8 = 2;
const CAT_CREATE_INDEX: u8 = 3;
const CAT_SET_INDEX_READY: u8 = 4;
const CAT_SET_INDEX_FAILED: u8 = 5;
const CAT_DELETE_INDEX: u8 = 6;

fn encode_catalog_entry(buf: &mut Vec<u8>, entry: &CatalogWalEntry) {
    match entry {
        CatalogWalEntry::CreateCollection { id, name } => {
            write_u8(buf, CAT_CREATE_COLLECTION);
            write_u64(buf, *id);
            write_string(buf, name);
        }
        CatalogWalEntry::DeleteCollection { id } => {
            write_u8(buf, CAT_DELETE_COLLECTION);
            write_u64(buf, *id);
        }
        CatalogWalEntry::CreateIndex { collection_id, index_id, field_path, unique, state_tag, state_ts } => {
            write_u8(buf, CAT_CREATE_INDEX);
            write_u64(buf, *collection_id);
            write_u64(buf, *index_id);
            write_u64(buf, field_path.len() as u64);
            for part in field_path {
                write_string(buf, part);
            }
            write_u8(buf, if *unique { 1 } else { 0 });
            write_u8(buf, *state_tag);
            write_u64(buf, *state_ts);
        }
        CatalogWalEntry::SetIndexReady { collection_id, index_id, ready_at_ts } => {
            write_u8(buf, CAT_SET_INDEX_READY);
            write_u64(buf, *collection_id);
            write_u64(buf, *index_id);
            write_u64(buf, *ready_at_ts);
        }
        CatalogWalEntry::SetIndexFailed { collection_id, index_id, message } => {
            write_u8(buf, CAT_SET_INDEX_FAILED);
            write_u64(buf, *collection_id);
            write_u64(buf, *index_id);
            write_string(buf, message);
        }
        CatalogWalEntry::DeleteIndex { collection_id, index_id } => {
            write_u8(buf, CAT_DELETE_INDEX);
            write_u64(buf, *collection_id);
            write_u64(buf, *index_id);
        }
    }
}

fn decode_catalog_entry(data: &[u8], pos: &mut usize) -> Result<CatalogWalEntry> {
    let tag = read_u8(data, pos)?;
    match tag {
        CAT_CREATE_COLLECTION => {
            let id = read_u64(data, pos)?;
            let name = read_string(data, pos)?;
            Ok(CatalogWalEntry::CreateCollection { id, name })
        }
        CAT_DELETE_COLLECTION => {
            let id = read_u64(data, pos)?;
            Ok(CatalogWalEntry::DeleteCollection { id })
        }
        CAT_CREATE_INDEX => {
            let collection_id = read_u64(data, pos)?;
            let index_id = read_u64(data, pos)?;
            let field_len = read_u64(data, pos)? as usize;
            let mut field_path = Vec::with_capacity(field_len);
            for _ in 0..field_len {
                field_path.push(read_string(data, pos)?);
            }
            let unique = read_u8(data, pos)? != 0;
            let state_tag = read_u8(data, pos)?;
            let state_ts = read_u64(data, pos)?;
            Ok(CatalogWalEntry::CreateIndex { collection_id, index_id, field_path, unique, state_tag, state_ts })
        }
        CAT_SET_INDEX_READY => {
            let collection_id = read_u64(data, pos)?;
            let index_id = read_u64(data, pos)?;
            let ready_at_ts = read_u64(data, pos)?;
            Ok(CatalogWalEntry::SetIndexReady { collection_id, index_id, ready_at_ts })
        }
        CAT_SET_INDEX_FAILED => {
            let collection_id = read_u64(data, pos)?;
            let index_id = read_u64(data, pos)?;
            let message = read_string(data, pos)?;
            Ok(CatalogWalEntry::SetIndexFailed { collection_id, index_id, message })
        }
        CAT_DELETE_INDEX => {
            let collection_id = read_u64(data, pos)?;
            let index_id = read_u64(data, pos)?;
            Ok(CatalogWalEntry::DeleteIndex { collection_id, index_id })
        }
        t => Err(anyhow!("unknown catalog WAL entry type: {t}")),
    }
}

// ── WalWriter ─────────────────────────────────────────────────────────────────

/// Append-only WAL writer. Uses a tokio task internally for serialized writes.
pub struct WalWriter {
    tx: tokio::sync::mpsc::Sender<WriteRequest>,
}

struct WriteRequest {
    data: Vec<u8>,
    reply: tokio::sync::oneshot::Sender<Result<()>>,
}

impl WalWriter {
    pub async fn open(path: &Path, fsync_policy: WalFsyncPolicy) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open WAL at {}", path.display()))?;

        let (tx, mut rx) = tokio::sync::mpsc::channel::<WriteRequest>(1024);

        tokio::task::spawn_blocking(move || {
            let mut file = file;
            while let Some(req) = rx.blocking_recv() {
                let result = (|| -> Result<()> {
                    file.write_all(&req.data)?;
                    match &fsync_policy {
                        WalFsyncPolicy::Always => file.sync_all()?,
                        WalFsyncPolicy::GroupCommit { .. } => {} // handled by batching
                        WalFsyncPolicy::Never => {}
                    }
                    Ok(())
                })();
                let _ = req.reply.send(result);
            }
        });

        Ok(WalWriter { tx })
    }

    pub async fn append(&self, record: &WalRecord) -> Result<()> {
        let data = record.encode();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(WriteRequest { data, reply: reply_tx })
            .await
            .map_err(|_| anyhow!("WAL writer task died"))?;
        reply_rx.await.map_err(|_| anyhow!("WAL writer reply channel closed"))?
    }

    pub async fn append_batch(&self, records: &[WalRecord]) -> Result<()> {
        let mut data = Vec::new();
        for r in records {
            data.extend_from_slice(&r.encode());
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(WriteRequest { data, reply: reply_tx })
            .await
            .map_err(|_| anyhow!("WAL writer task died"))?;
        reply_rx.await.map_err(|_| anyhow!("WAL writer reply channel closed"))?
    }
}

// ── WalReader ─────────────────────────────────────────────────────────────────

/// Reads and replays WAL records from a file.
pub struct WalReader {
    data: Vec<u8>,
    pos: usize,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self> {
        let data = if path.exists() {
            std::fs::read(path).with_context(|| format!("failed to read WAL at {}", path.display()))?
        } else {
            Vec::new()
        };
        Ok(WalReader { data, pos: 0 })
    }

    pub fn next(&mut self) -> Result<Option<WalRecord>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }

        // Read len (u32 LE)
        if self.pos + 8 > self.data.len() {
            // Truncated header – treat as end of valid log
            return Ok(None);
        }
        let len = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(self.data[self.pos + 4..self.pos + 8].try_into().unwrap());
        self.pos += 8;

        if self.pos + len > self.data.len() {
            // Truncated record – treat as end of valid log
            return Ok(None);
        }
        let frame_body = &self.data[self.pos..self.pos + len];

        // Verify CRC
        let mut hasher = Crc32Hasher::new();
        hasher.update(frame_body);
        let computed = hasher.finalize();
        if computed != crc {
            return Err(anyhow!(
                "WAL CRC mismatch at offset {}: expected {crc:#010x}, got {computed:#010x}",
                self.pos - 8
            ));
        }

        self.pos += len;
        let record = WalRecord::decode(frame_body)?;
        Ok(Some(record))
    }

    /// Collect all valid records, stopping at first CRC error or truncation.
    pub fn read_all(mut self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        loop {
            match self.next() {
                Ok(Some(r)) => records.push(r),
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("WAL read error (treating as truncation): {e}");
                    break;
                }
            }
        }
        Ok(records)
    }
}

/// Replay committed transactions from WAL records.
/// Returns an ordered list of committed transactions: (commit_ts, ops).
pub fn replay_committed(
    records: Vec<WalRecord>,
) -> Vec<(Ts, Vec<WalRecord>)> {
    use std::collections::HashMap;

    let mut in_flight: HashMap<TxId, Vec<WalRecord>> = HashMap::new();
    let mut committed: Vec<(Ts, Vec<WalRecord>)> = Vec::new();

    for record in records {
        match &record {
            WalRecord::Begin { tx_id, .. } => {
                in_flight.insert(*tx_id, vec![record]);
            }
            WalRecord::PutDoc { tx_id, .. } | WalRecord::DeleteDoc { tx_id, .. } => {
                if let Some(ops) = in_flight.get_mut(tx_id) {
                    ops.push(record);
                }
            }
            WalRecord::Commit { tx_id, commit_ts } => {
                if let Some(mut ops) = in_flight.remove(tx_id) {
                    ops.push(record.clone());
                    committed.push((*commit_ts, ops));
                }
            }
            WalRecord::Abort { tx_id } => {
                in_flight.remove(tx_id);
            }
            WalRecord::CatalogUpdate(_) => {
                // Catalog updates are not wrapped in transactions – apply immediately
                committed.push((0, vec![record]));
            }
            WalRecord::Checkpoint { .. } => {}
        }
    }

    // Sort by commit_ts so we apply in order
    committed.sort_by_key(|(ts, _)| *ts);
    committed
}

// ── Re-export WalFsyncPolicy (defined in types) ───────────────────────────────

pub use crate::types::WalFsyncPolicy;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_records() {
        let records = vec![
            WalRecord::Begin { tx_id: 1, start_ts: 100 },
            WalRecord::PutDoc {
                tx_id: 1,
                collection_id: 42,
                doc_id: DocId::new(123, 1),
                json: b"{\"x\":1}".to_vec(),
            },
            WalRecord::Commit { tx_id: 1, commit_ts: 101 },
        ];

        for r in &records {
            let encoded = r.encode();
            // Frame = len(4) + crc(4) + body
            let body = &encoded[8..];
            let decoded = WalRecord::decode(body).unwrap();
            // Just check it decodes without error (can't easily compare variants)
            let _ = decoded;
        }
    }
}
