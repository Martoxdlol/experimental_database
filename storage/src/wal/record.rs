use crate::types::*;

/// Deserialized WAL record with its LSN.
#[derive(Debug)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: WalRecordType,
    pub payload: WalPayload,
}

/// Typed WAL record payloads.
#[derive(Debug)]
pub enum WalPayload {
    TxCommit(TxCommitRecord),
    Checkpoint(CheckpointRecord),
    CreateCollection(CreateCollectionRecord),
    DropCollection(DropCollectionRecord),
    CreateIndex(CreateIndexRecord),
    DropIndex(DropIndexRecord),
    IndexReady(IndexReadyRecord),
    Vacuum(VacuumRecord),
    CreateDatabase(CreateDatabaseRecord),
    DropDatabase(DropDatabaseRecord),
}

#[derive(Debug)]
pub struct TxCommitRecord {
    pub tx_id: TxId,
    pub commit_ts: Timestamp,
    pub mutations: Vec<Mutation>,
    pub index_deltas: Vec<IndexDelta>,
}

#[derive(Debug)]
pub struct Mutation {
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub op_type: OpType,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct IndexDelta {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub doc_id: DocId,
    pub old_key: Option<EncodedKey>,
    pub new_key: Option<EncodedKey>,
}

#[derive(Debug)]
pub struct CheckpointRecord {
    pub checkpoint_lsn: Lsn,
}

#[derive(Debug)]
pub struct CreateCollectionRecord {
    pub collection_id: CollectionId,
    pub name: String,
}

#[derive(Debug)]
pub struct DropCollectionRecord {
    pub collection_id: CollectionId,
}

#[derive(Debug)]
pub struct CreateIndexRecord {
    pub index_id: IndexId,
    pub collection_id: CollectionId,
    pub name: String,
    pub field_paths: Vec<FieldPath>,
}

#[derive(Debug)]
pub struct DropIndexRecord {
    pub index_id: IndexId,
}

#[derive(Debug)]
pub struct IndexReadyRecord {
    pub index_id: IndexId,
}

#[derive(Debug)]
pub struct VacuumRecord {
    pub collection_id: CollectionId,
    pub entries: Vec<VacuumEntry>,
}

#[derive(Debug)]
pub struct VacuumEntry {
    pub doc_id: DocId,
    pub removed_ts: Timestamp,
    pub index_keys: Vec<VacuumIndexKey>,
}

#[derive(Debug)]
pub struct VacuumIndexKey {
    pub index_id: IndexId,
    pub key: EncodedKey,
}

#[derive(Debug)]
pub struct CreateDatabaseRecord {
    pub database_id: DatabaseId,
    pub name: String,
    pub path: String,
    pub config: Vec<u8>,
}

#[derive(Debug)]
pub struct DropDatabaseRecord {
    pub database_id: DatabaseId,
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC mismatch at LSN {lsn:?}: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { lsn: Lsn, expected: u32, computed: u32 },

    #[error("invalid record type: {0:#04x}")]
    InvalidRecordType(u8),

    #[error("truncated record at LSN {0:?}")]
    TruncatedRecord(Lsn),

    #[error("invalid segment header: {0}")]
    InvalidSegmentHeader(String),

    #[error("payload too large: {0} bytes")]
    PayloadTooLarge(u32),

    #[error("encoding error: {0}")]
    Encoding(String),
}

// --- Binary encoding helpers ---

pub mod encoding {
    use super::*;

    pub fn write_u8(buf: &mut Vec<u8>, v: u8) {
        buf.push(v);
    }

    pub fn write_u16(buf: &mut Vec<u8>, v: u16) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u128(buf: &mut Vec<u8>, v: u128) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_str(buf: &mut Vec<u8>, s: &str) {
        write_u16(buf, s.len() as u16);
        buf.extend_from_slice(s.as_bytes());
    }

    pub fn write_blob(buf: &mut Vec<u8>, b: &[u8]) {
        write_u32(buf, b.len() as u32);
        buf.extend_from_slice(b);
    }

    pub fn write_key(buf: &mut Vec<u8>, k: &EncodedKey) {
        write_u16(buf, k.0.len() as u16);
        buf.extend_from_slice(&k.0);
    }

    pub fn write_field_path(buf: &mut Vec<u8>, fp: &FieldPath) {
        write_u16(buf, fp.len() as u16);
        for seg in fp {
            write_str(buf, seg);
        }
    }

    pub fn read_u8(cursor: &mut &[u8]) -> Result<u8, WalError> {
        if cursor.is_empty() {
            return Err(WalError::Encoding("unexpected end reading u8".into()));
        }
        let v = cursor[0];
        *cursor = &cursor[1..];
        Ok(v)
    }

    pub fn read_u16(cursor: &mut &[u8]) -> Result<u16, WalError> {
        if cursor.len() < 2 {
            return Err(WalError::Encoding("unexpected end reading u16".into()));
        }
        let v = u16::from_le_bytes([cursor[0], cursor[1]]);
        *cursor = &cursor[2..];
        Ok(v)
    }

    pub fn read_u32(cursor: &mut &[u8]) -> Result<u32, WalError> {
        if cursor.len() < 4 {
            return Err(WalError::Encoding("unexpected end reading u32".into()));
        }
        let v = u32::from_le_bytes(cursor[..4].try_into().unwrap());
        *cursor = &cursor[4..];
        Ok(v)
    }

    pub fn read_u64(cursor: &mut &[u8]) -> Result<u64, WalError> {
        if cursor.len() < 8 {
            return Err(WalError::Encoding("unexpected end reading u64".into()));
        }
        let v = u64::from_le_bytes(cursor[..8].try_into().unwrap());
        *cursor = &cursor[8..];
        Ok(v)
    }

    pub fn read_u128(cursor: &mut &[u8]) -> Result<u128, WalError> {
        if cursor.len() < 16 {
            return Err(WalError::Encoding("unexpected end reading u128".into()));
        }
        let v = u128::from_le_bytes(cursor[..16].try_into().unwrap());
        *cursor = &cursor[16..];
        Ok(v)
    }

    pub fn read_str(cursor: &mut &[u8]) -> Result<String, WalError> {
        let len = read_u16(cursor)? as usize;
        if cursor.len() < len {
            return Err(WalError::Encoding("unexpected end reading string".into()));
        }
        let s = String::from_utf8(cursor[..len].to_vec())
            .map_err(|e| WalError::Encoding(format!("invalid utf8: {e}")))?;
        *cursor = &cursor[len..];
        Ok(s)
    }

    pub fn read_blob(cursor: &mut &[u8]) -> Result<Vec<u8>, WalError> {
        let len = read_u32(cursor)? as usize;
        if cursor.len() < len {
            return Err(WalError::Encoding("unexpected end reading blob".into()));
        }
        let b = cursor[..len].to_vec();
        *cursor = &cursor[len..];
        Ok(b)
    }

    pub fn read_key(cursor: &mut &[u8]) -> Result<EncodedKey, WalError> {
        let len = read_u16(cursor)? as usize;
        if cursor.len() < len {
            return Err(WalError::Encoding("unexpected end reading key".into()));
        }
        let k = EncodedKey(cursor[..len].to_vec());
        *cursor = &cursor[len..];
        Ok(k)
    }

    pub fn read_field_path(cursor: &mut &[u8]) -> Result<FieldPath, WalError> {
        let len = read_u16(cursor)? as usize;
        let mut fp = Vec::with_capacity(len);
        for _ in 0..len {
            fp.push(read_str(cursor)?);
        }
        Ok(fp)
    }
}

// --- Serialization ---

impl WalRecord {
    /// Serialize into frame bytes: [payload_len: u32][crc32c: u32][record_type: u8][payload].
    pub fn serialize_frame(record_type: WalRecordType, payload: &[u8]) -> Vec<u8> {
        let payload_len = payload.len() as u32;
        let mut type_and_payload = Vec::with_capacity(1 + payload.len());
        type_and_payload.push(record_type as u8);
        type_and_payload.extend_from_slice(payload);
        let crc = crc32c::crc32c(&type_and_payload);

        let mut frame = Vec::with_capacity(WAL_FRAME_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.push(record_type as u8);
        frame.extend_from_slice(payload);
        frame
    }
}

/// Serialize a TxCommit record payload.
pub fn serialize_tx_commit(rec: &TxCommitRecord) -> Vec<u8> {
    use encoding::*;
    let mut buf = Vec::new();
    write_u64(&mut buf, rec.tx_id.0);
    write_u64(&mut buf, rec.commit_ts.0);
    // Mutations
    write_u32(&mut buf, rec.mutations.len() as u32);
    for m in &rec.mutations {
        write_u64(&mut buf, m.collection_id.0);
        write_u128(&mut buf, m.doc_id.0);
        write_u8(&mut buf, m.op_type as u8);
        write_blob(&mut buf, &m.body);
    }
    // Index deltas
    write_u32(&mut buf, rec.index_deltas.len() as u32);
    for d in &rec.index_deltas {
        write_u64(&mut buf, d.index_id.0);
        write_u64(&mut buf, d.collection_id.0);
        write_u128(&mut buf, d.doc_id.0);
        // old_key
        match &d.old_key {
            Some(k) => { write_u8(&mut buf, 1); write_key(&mut buf, k); }
            None => write_u8(&mut buf, 0),
        }
        // new_key
        match &d.new_key {
            Some(k) => { write_u8(&mut buf, 1); write_key(&mut buf, k); }
            None => write_u8(&mut buf, 0),
        }
    }
    buf
}

pub fn deserialize_tx_commit(cursor: &mut &[u8]) -> Result<TxCommitRecord, WalError> {
    use encoding::*;
    let tx_id = TxId(read_u64(cursor)?);
    let commit_ts = Timestamp(read_u64(cursor)?);
    let num_mutations = read_u32(cursor)? as usize;
    let mut mutations = Vec::with_capacity(num_mutations);
    for _ in 0..num_mutations {
        let collection_id = CollectionId(read_u64(cursor)?);
        let doc_id = DocId(read_u128(cursor)?);
        let op_type = OpType::from_u8(read_u8(cursor)?)
            .ok_or_else(|| WalError::Encoding("invalid op type".into()))?;
        let body = read_blob(cursor)?;
        mutations.push(Mutation { collection_id, doc_id, op_type, body });
    }
    let num_deltas = read_u32(cursor)? as usize;
    let mut index_deltas = Vec::with_capacity(num_deltas);
    for _ in 0..num_deltas {
        let index_id = IndexId(read_u64(cursor)?);
        let collection_id = CollectionId(read_u64(cursor)?);
        let doc_id = DocId(read_u128(cursor)?);
        let old_key = if read_u8(cursor)? == 1 { Some(read_key(cursor)?) } else { None };
        let new_key = if read_u8(cursor)? == 1 { Some(read_key(cursor)?) } else { None };
        index_deltas.push(IndexDelta { index_id, collection_id, doc_id, old_key, new_key });
    }
    Ok(TxCommitRecord { tx_id, commit_ts, mutations, index_deltas })
}

pub fn serialize_checkpoint(rec: &CheckpointRecord) -> Vec<u8> {
    rec.checkpoint_lsn.0.to_le_bytes().to_vec()
}

pub fn deserialize_checkpoint(cursor: &mut &[u8]) -> Result<CheckpointRecord, WalError> {
    let lsn = Lsn(encoding::read_u64(cursor)?);
    Ok(CheckpointRecord { checkpoint_lsn: lsn })
}

pub fn serialize_create_collection(rec: &CreateCollectionRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    encoding::write_u64(&mut buf, rec.collection_id.0);
    encoding::write_str(&mut buf, &rec.name);
    buf
}

pub fn deserialize_create_collection(cursor: &mut &[u8]) -> Result<CreateCollectionRecord, WalError> {
    let id = CollectionId(encoding::read_u64(cursor)?);
    let name = encoding::read_str(cursor)?;
    Ok(CreateCollectionRecord { collection_id: id, name })
}

pub fn serialize_drop_collection(rec: &DropCollectionRecord) -> Vec<u8> {
    rec.collection_id.0.to_le_bytes().to_vec()
}

pub fn deserialize_drop_collection(cursor: &mut &[u8]) -> Result<DropCollectionRecord, WalError> {
    let id = CollectionId(encoding::read_u64(cursor)?);
    Ok(DropCollectionRecord { collection_id: id })
}

pub fn serialize_create_index(rec: &CreateIndexRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    encoding::write_u64(&mut buf, rec.index_id.0);
    encoding::write_u64(&mut buf, rec.collection_id.0);
    encoding::write_str(&mut buf, &rec.name);
    encoding::write_u16(&mut buf, rec.field_paths.len() as u16);
    for fp in &rec.field_paths {
        encoding::write_field_path(&mut buf, fp);
    }
    buf
}

pub fn deserialize_create_index(cursor: &mut &[u8]) -> Result<CreateIndexRecord, WalError> {
    let index_id = IndexId(encoding::read_u64(cursor)?);
    let collection_id = CollectionId(encoding::read_u64(cursor)?);
    let name = encoding::read_str(cursor)?;
    let num_paths = encoding::read_u16(cursor)? as usize;
    let mut field_paths = Vec::with_capacity(num_paths);
    for _ in 0..num_paths {
        field_paths.push(encoding::read_field_path(cursor)?);
    }
    Ok(CreateIndexRecord { index_id, collection_id, name, field_paths })
}

pub fn serialize_drop_index(rec: &DropIndexRecord) -> Vec<u8> {
    rec.index_id.0.to_le_bytes().to_vec()
}

pub fn deserialize_drop_index(cursor: &mut &[u8]) -> Result<DropIndexRecord, WalError> {
    let id = IndexId(encoding::read_u64(cursor)?);
    Ok(DropIndexRecord { index_id: id })
}

pub fn serialize_index_ready(rec: &IndexReadyRecord) -> Vec<u8> {
    rec.index_id.0.to_le_bytes().to_vec()
}

pub fn deserialize_index_ready(cursor: &mut &[u8]) -> Result<IndexReadyRecord, WalError> {
    let id = IndexId(encoding::read_u64(cursor)?);
    Ok(IndexReadyRecord { index_id: id })
}

pub fn serialize_vacuum(rec: &VacuumRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    encoding::write_u64(&mut buf, rec.collection_id.0);
    encoding::write_u32(&mut buf, rec.entries.len() as u32);
    for entry in &rec.entries {
        encoding::write_u128(&mut buf, entry.doc_id.0);
        encoding::write_u64(&mut buf, entry.removed_ts.0);
        encoding::write_u32(&mut buf, entry.index_keys.len() as u32);
        for ik in &entry.index_keys {
            encoding::write_u64(&mut buf, ik.index_id.0);
            encoding::write_key(&mut buf, &ik.key);
        }
    }
    buf
}

pub fn deserialize_vacuum(cursor: &mut &[u8]) -> Result<VacuumRecord, WalError> {
    let collection_id = CollectionId(encoding::read_u64(cursor)?);
    let num_entries = encoding::read_u32(cursor)? as usize;
    let mut entries = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        let doc_id = DocId(encoding::read_u128(cursor)?);
        let removed_ts = Timestamp(encoding::read_u64(cursor)?);
        let num_keys = encoding::read_u32(cursor)? as usize;
        let mut index_keys = Vec::with_capacity(num_keys);
        for _ in 0..num_keys {
            let index_id = IndexId(encoding::read_u64(cursor)?);
            let key = encoding::read_key(cursor)?;
            index_keys.push(VacuumIndexKey { index_id, key });
        }
        entries.push(VacuumEntry { doc_id, removed_ts, index_keys });
    }
    Ok(VacuumRecord { collection_id, entries })
}

pub fn serialize_create_database(rec: &CreateDatabaseRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    encoding::write_u64(&mut buf, rec.database_id.0);
    encoding::write_str(&mut buf, &rec.name);
    encoding::write_str(&mut buf, &rec.path);
    encoding::write_blob(&mut buf, &rec.config);
    buf
}

pub fn deserialize_create_database(cursor: &mut &[u8]) -> Result<CreateDatabaseRecord, WalError> {
    let database_id = DatabaseId(encoding::read_u64(cursor)?);
    let name = encoding::read_str(cursor)?;
    let path = encoding::read_str(cursor)?;
    let config = encoding::read_blob(cursor)?;
    Ok(CreateDatabaseRecord { database_id, name, path, config })
}

pub fn serialize_drop_database(rec: &DropDatabaseRecord) -> Vec<u8> {
    rec.database_id.0.to_le_bytes().to_vec()
}

pub fn deserialize_drop_database(cursor: &mut &[u8]) -> Result<DropDatabaseRecord, WalError> {
    let id = DatabaseId(encoding::read_u64(cursor)?);
    Ok(DropDatabaseRecord { database_id: id })
}

/// Serialize any WalPayload into a frame.
pub fn serialize_payload(record_type: WalRecordType, payload: &WalPayload) -> Vec<u8> {
    let payload_bytes = match payload {
        WalPayload::TxCommit(r) => serialize_tx_commit(r),
        WalPayload::Checkpoint(r) => serialize_checkpoint(r),
        WalPayload::CreateCollection(r) => serialize_create_collection(r),
        WalPayload::DropCollection(r) => serialize_drop_collection(r),
        WalPayload::CreateIndex(r) => serialize_create_index(r),
        WalPayload::DropIndex(r) => serialize_drop_index(r),
        WalPayload::IndexReady(r) => serialize_index_ready(r),
        WalPayload::Vacuum(r) => serialize_vacuum(r),
        WalPayload::CreateDatabase(r) => serialize_create_database(r),
        WalPayload::DropDatabase(r) => serialize_drop_database(r),
    };
    WalRecord::serialize_frame(record_type, &payload_bytes)
}

/// Deserialize a WAL record from frame bytes (after the 9-byte frame header has been parsed).
pub fn deserialize_payload(
    record_type: WalRecordType,
    payload: &[u8],
) -> Result<WalPayload, WalError> {
    let mut cursor: &[u8] = payload;
    match record_type {
        WalRecordType::TxCommit => Ok(WalPayload::TxCommit(deserialize_tx_commit(&mut cursor)?)),
        WalRecordType::Checkpoint => Ok(WalPayload::Checkpoint(deserialize_checkpoint(&mut cursor)?)),
        WalRecordType::CreateCollection => Ok(WalPayload::CreateCollection(deserialize_create_collection(&mut cursor)?)),
        WalRecordType::DropCollection => Ok(WalPayload::DropCollection(deserialize_drop_collection(&mut cursor)?)),
        WalRecordType::CreateIndex => Ok(WalPayload::CreateIndex(deserialize_create_index(&mut cursor)?)),
        WalRecordType::DropIndex => Ok(WalPayload::DropIndex(deserialize_drop_index(&mut cursor)?)),
        WalRecordType::IndexReady => Ok(WalPayload::IndexReady(deserialize_index_ready(&mut cursor)?)),
        WalRecordType::Vacuum => Ok(WalPayload::Vacuum(deserialize_vacuum(&mut cursor)?)),
        WalRecordType::CreateDatabase => Ok(WalPayload::CreateDatabase(deserialize_create_database(&mut cursor)?)),
        WalRecordType::DropDatabase => Ok(WalPayload::DropDatabase(deserialize_drop_database(&mut cursor)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_frame(record_type: WalRecordType, payload: &WalPayload) -> WalPayload {
        let frame = serialize_payload(record_type, payload);
        // Parse frame header
        let payload_len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(frame[4..8].try_into().unwrap());
        let rt_byte = frame[8];
        let payload_data = &frame[9..9 + payload_len];
        // Verify CRC
        let mut crc_data = vec![rt_byte];
        crc_data.extend_from_slice(payload_data);
        assert_eq!(crc32c::crc32c(&crc_data), stored_crc);
        // Deserialize
        let rt = WalRecordType::from_u8(rt_byte).unwrap();
        deserialize_payload(rt, payload_data).unwrap()
    }

    #[test]
    fn test_tx_commit_roundtrip() {
        let rec = TxCommitRecord {
            tx_id: TxId(42),
            commit_ts: Timestamp(100),
            mutations: vec![
                Mutation {
                    collection_id: CollectionId(1),
                    doc_id: DocId(999),
                    op_type: OpType::Insert,
                    body: vec![1, 2, 3, 4],
                },
            ],
            index_deltas: vec![
                IndexDelta {
                    index_id: IndexId(10),
                    collection_id: CollectionId(1),
                    doc_id: DocId(999),
                    old_key: None,
                    new_key: Some(EncodedKey(vec![5, 6, 7])),
                },
            ],
        };
        let payload = WalPayload::TxCommit(rec);
        let result = roundtrip_frame(WalRecordType::TxCommit, &payload);
        if let WalPayload::TxCommit(r) = result {
            assert_eq!(r.tx_id, TxId(42));
            assert_eq!(r.commit_ts, Timestamp(100));
            assert_eq!(r.mutations.len(), 1);
            assert_eq!(r.mutations[0].body, vec![1, 2, 3, 4]);
            assert_eq!(r.index_deltas.len(), 1);
            assert!(r.index_deltas[0].old_key.is_none());
            assert_eq!(r.index_deltas[0].new_key.as_ref().unwrap().0, vec![5, 6, 7]);
        } else {
            panic!("wrong payload type");
        }
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let payload = WalPayload::Checkpoint(CheckpointRecord { checkpoint_lsn: Lsn(12345) });
        let result = roundtrip_frame(WalRecordType::Checkpoint, &payload);
        if let WalPayload::Checkpoint(r) = result {
            assert_eq!(r.checkpoint_lsn, Lsn(12345));
        } else {
            panic!("wrong payload type");
        }
    }

    #[test]
    fn test_create_collection_roundtrip() {
        let payload = WalPayload::CreateCollection(CreateCollectionRecord {
            collection_id: CollectionId(7),
            name: "users".into(),
        });
        let result = roundtrip_frame(WalRecordType::CreateCollection, &payload);
        if let WalPayload::CreateCollection(r) = result {
            assert_eq!(r.collection_id, CollectionId(7));
            assert_eq!(r.name, "users");
        } else {
            panic!("wrong payload type");
        }
    }

    #[test]
    fn test_create_index_roundtrip() {
        let payload = WalPayload::CreateIndex(CreateIndexRecord {
            index_id: IndexId(3),
            collection_id: CollectionId(1),
            name: "email_idx".into(),
            field_paths: vec![vec!["user".into(), "email".into()]],
        });
        let result = roundtrip_frame(WalRecordType::CreateIndex, &payload);
        if let WalPayload::CreateIndex(r) = result {
            assert_eq!(r.index_id, IndexId(3));
            assert_eq!(r.name, "email_idx");
            assert_eq!(r.field_paths, vec![vec!["user".to_string(), "email".to_string()]]);
        } else {
            panic!("wrong payload type");
        }
    }

    #[test]
    fn test_vacuum_roundtrip() {
        let payload = WalPayload::Vacuum(VacuumRecord {
            collection_id: CollectionId(1),
            entries: vec![VacuumEntry {
                doc_id: DocId(42),
                removed_ts: Timestamp(10),
                index_keys: vec![VacuumIndexKey {
                    index_id: IndexId(2),
                    key: EncodedKey(vec![1, 2, 3]),
                }],
            }],
        });
        let result = roundtrip_frame(WalRecordType::Vacuum, &payload);
        if let WalPayload::Vacuum(r) = result {
            assert_eq!(r.collection_id, CollectionId(1));
            assert_eq!(r.entries.len(), 1);
            assert_eq!(r.entries[0].doc_id, DocId(42));
        } else {
            panic!("wrong payload type");
        }
    }
}
