//! Typed transaction promotion payloads.
//!
//! A replica forwards write intent to the primary as the transaction snapshot
//! plus encoded L5 read/write sets. The primary decodes this payload and submits
//! it through the normal commit coordinator, preserving OCC and replication
//! semantics.

use std::ops::Bound;

use exdb_core::encoding::{decode_document, encode_document};
use exdb_core::types::{CollectionId, DocId, IndexId};

use crate::read_set::{LimitBoundary, ReadInterval, ReadSet};
use crate::write_set::{MutationOp, WriteSet};

const PROMOTION_PAYLOAD_VERSION: u8 = 1;

/// Encode a transaction's concrete L5 read/write sets for replica promotion.
pub fn serialize_promotion_payload(
    read_set: &ReadSet,
    write_set: &WriteSet,
) -> Result<Vec<u8>, String> {
    if !write_set.catalog_mutations.is_empty() {
        return Err("catalog promotion payloads are not supported yet".to_string());
    }

    let mut buf = Vec::new();
    buf.push(PROMOTION_PAYLOAD_VERSION);

    buf.extend_from_slice(&read_set.peek_next_query_id().to_le_bytes());
    buf.extend_from_slice(&(read_set.intervals.len() as u32).to_le_bytes());
    for (&(collection_id, index_id), intervals) in &read_set.intervals {
        buf.extend_from_slice(&collection_id.0.to_le_bytes());
        buf.extend_from_slice(&index_id.0.to_le_bytes());
        buf.extend_from_slice(&(intervals.len() as u32).to_le_bytes());
        for interval in intervals {
            buf.extend_from_slice(&interval.query_id.to_le_bytes());
            encode_bound(&mut buf, &interval.lower);
            encode_bound(&mut buf, &interval.upper);
            encode_limit_boundary(&mut buf, &interval.limit_boundary);
        }
    }

    buf.extend_from_slice(&(write_set.mutations.len() as u32).to_le_bytes());
    for (&(collection_id, doc_id), entry) in &write_set.mutations {
        buf.extend_from_slice(&collection_id.0.to_le_bytes());
        buf.extend_from_slice(doc_id.as_bytes());
        buf.push(match entry.op {
            MutationOp::Insert => 1,
            MutationOp::Replace => 2,
            MutationOp::Delete => 3,
        });
        buf.extend_from_slice(&entry.previous_ts.unwrap_or(0).to_le_bytes());
        match &entry.body {
            Some(body) => {
                let body = encode_document(body);
                buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
                buf.extend_from_slice(&body);
            }
            None => buf.extend_from_slice(&0u32.to_le_bytes()),
        }
    }

    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved catalog mutation count
    Ok(buf)
}

/// Decode a replica promotion payload into concrete L5 read/write sets.
pub fn deserialize_promotion_payload(data: &[u8]) -> Result<(ReadSet, WriteSet), String> {
    let mut reader = Reader::new(data);
    let version = reader.u8()?;
    if version != PROMOTION_PAYLOAD_VERSION {
        return Err(format!(
            "unsupported promotion payload version: current {}, got {}",
            PROMOTION_PAYLOAD_VERSION, version
        ));
    }

    let next_query_id = reader.u32()?;
    let mut read_set = ReadSet::new();
    read_set.set_next_query_id(next_query_id);

    let read_group_count = reader.u32()? as usize;
    for _ in 0..read_group_count {
        let collection_id = CollectionId(reader.u64()?);
        let index_id = IndexId(reader.u64()?);
        let interval_count = reader.u32()? as usize;
        for _ in 0..interval_count {
            let interval = ReadInterval {
                query_id: reader.u32()?,
                lower: reader.bound()?,
                upper: reader.bound()?,
                limit_boundary: reader.limit_boundary()?,
            };
            read_set.add_interval(collection_id, index_id, interval);
        }
    }

    let mut write_set = WriteSet::new();
    let mutation_count = reader.u32()? as usize;
    for _ in 0..mutation_count {
        let collection_id = CollectionId(reader.u64()?);
        let doc_id = DocId(reader.bytes_16()?);
        let op = reader.u8()?;
        let previous_ts = reader.u64()?;
        let body_len = reader.u32()? as usize;
        let body = if body_len == 0 {
            None
        } else {
            Some(
                decode_document(reader.bytes(body_len)?)
                    .map_err(|err| format!("invalid promotion document body: {err}"))?,
            )
        };

        match op {
            1 => {
                let body = body.ok_or("insert promotion mutation missing body")?;
                write_set.insert(collection_id, doc_id, body);
            }
            2 => {
                let body = body.ok_or("replace promotion mutation missing body")?;
                if previous_ts == 0 {
                    return Err("replace promotion mutation missing previous_ts".to_string());
                }
                write_set.replace(collection_id, doc_id, body, previous_ts);
            }
            3 => {
                if body.is_some() {
                    return Err("delete promotion mutation unexpectedly included body".to_string());
                }
                if previous_ts == 0 {
                    return Err("delete promotion mutation missing previous_ts".to_string());
                }
                write_set.delete(collection_id, doc_id, previous_ts);
            }
            other => return Err(format!("invalid promotion mutation op: {other}")),
        }
    }

    let catalog_count = reader.u32()? as usize;
    if catalog_count != 0 {
        return Err("catalog promotion payloads are not supported yet".to_string());
    }
    reader.finish()?;

    Ok((read_set, write_set))
}

fn encode_bound(buf: &mut Vec<u8>, bound: &Bound<Vec<u8>>) {
    match bound {
        Bound::Unbounded => buf.push(0),
        Bound::Included(value) => {
            buf.push(1);
            encode_bytes(buf, value);
        }
        Bound::Excluded(value) => {
            buf.push(2);
            encode_bytes(buf, value);
        }
    }
}

fn encode_limit_boundary(buf: &mut Vec<u8>, boundary: &Option<LimitBoundary>) {
    match boundary {
        None => buf.push(0),
        Some(LimitBoundary::Upper(value)) => {
            buf.push(1);
            encode_bytes(buf, value);
        }
        Some(LimitBoundary::Lower(value)) => {
            buf.push(2);
            encode_bytes(buf, value);
        }
    }
}

fn encode_bytes(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
}

struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.data.len() {
            Ok(())
        } else {
            Err("promotion payload has trailing bytes".to_string())
        }
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        let bytes: [u8; 4] = self.bytes(4)?.try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let bytes: [u8; 8] = self.bytes(8)?.try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }

    fn bytes_16(&mut self) -> Result<[u8; 16], String> {
        Ok(self.bytes(16)?.try_into().unwrap())
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or("promotion payload offset overflow")?;
        if end > self.data.len() {
            return Err("promotion payload truncated".to_string());
        }
        let bytes = &self.data[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn vec_bytes(&mut self) -> Result<Vec<u8>, String> {
        let len = self.u32()? as usize;
        Ok(self.bytes(len)?.to_vec())
    }

    fn bound(&mut self) -> Result<Bound<Vec<u8>>, String> {
        match self.u8()? {
            0 => Ok(Bound::Unbounded),
            1 => Ok(Bound::Included(self.vec_bytes()?)),
            2 => Ok(Bound::Excluded(self.vec_bytes()?)),
            other => Err(format!("invalid promotion bound tag: {other}")),
        }
    }

    fn limit_boundary(&mut self) -> Result<Option<LimitBoundary>, String> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(LimitBoundary::Upper(self.vec_bytes()?))),
            2 => Ok(Some(LimitBoundary::Lower(self.vec_bytes()?))),
            other => Err(format!("invalid promotion limit boundary tag: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn promotion_payload_roundtrips_read_and_write_sets() {
        let mut read_set = ReadSet::new();
        read_set.set_next_query_id(7);
        read_set.add_interval(
            CollectionId(2),
            IndexId(3),
            ReadInterval {
                query_id: 4,
                lower: Bound::Included(vec![1, 2]),
                upper: Bound::Excluded(vec![9]),
                limit_boundary: Some(LimitBoundary::Upper(vec![5])),
            },
        );

        let mut write_set = WriteSet::new();
        let doc_id = DocId([42; 16]);
        write_set.replace(CollectionId(2), doc_id, json!({ "name": "Ada" }), 11);

        let encoded = serialize_promotion_payload(&read_set, &write_set).unwrap();
        let (decoded_read_set, decoded_write_set) =
            deserialize_promotion_payload(&encoded).unwrap();

        assert_eq!(decoded_read_set.peek_next_query_id(), 7);
        assert_eq!(decoded_read_set.interval_count(), 1);
        let interval = &decoded_read_set.intervals[&(CollectionId(2), IndexId(3))][0];
        assert_eq!(interval.query_id, 4);
        assert_eq!(interval.lower, Bound::Included(vec![1, 2]));
        assert_eq!(interval.upper, Bound::Excluded(vec![9]));
        assert_eq!(interval.limit_boundary, Some(LimitBoundary::Upper(vec![5])));

        let entry = decoded_write_set.get(CollectionId(2), &doc_id).unwrap();
        assert_eq!(entry.op, MutationOp::Replace);
        assert_eq!(entry.previous_ts, Some(11));
        assert_eq!(entry.body.as_ref().unwrap(), &json!({ "name": "Ada" }));
    }

    #[test]
    fn promotion_payload_rejects_catalog_mutations_until_ddl_ids_are_primary_owned() {
        let read_set = ReadSet::new();
        let mut write_set = WriteSet::new();
        write_set.add_catalog_mutation(crate::write_set::CatalogMutation::CreateCollection {
            name: "users".to_string(),
            provisional_id: CollectionId(9),
            primary_root_page: 0,
            created_at_root_page: 0,
        });

        let err = serialize_promotion_payload(&read_set, &write_set).unwrap_err();
        assert!(err.contains("catalog promotion payloads"));
    }
}
