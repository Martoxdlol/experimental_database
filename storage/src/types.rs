//! Core type definitions and constants for the storage engine.
//!
//! All fundamental newtypes (`PageId`, `Lsn`, `DocId`, `Timestamp`, etc.),
//! enum discriminants (`PageType`, `WalRecordType`, `OpType`), and
//! format constants (`PAGE_HEADER_SIZE`, `PRIMARY_KEY_SIZE`, etc.) live here.

use std::fmt;

/// Page identifier — index into the data file. Page 0 = file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u32);

impl PageId {
    pub const INVALID: PageId = PageId(0);
    pub const FILE_HEADER: PageId = PageId(0);
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

/// Frame identifier — index into the buffer pool's frame array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub u32);

/// Log Sequence Number — byte offset in the logical WAL stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);
    pub const INVALID: Lsn = Lsn(u64::MAX);

    pub fn advance(self, bytes: u64) -> Lsn {
        Lsn(self.0 + bytes)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({})", self.0)
    }
}

/// Monotonic timestamp used for MVCC ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn inverted(self) -> u64 {
        u64::MAX - self.0
    }
}

/// Collection identifier within a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionId(pub u64);

/// Index identifier within a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(pub u64);

/// Database identifier in the system catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatabaseId(pub u64);

/// Document identifier — 128-bit ULID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocId(pub u128);

impl DocId {
    pub fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        DocId(u128::from_be_bytes(bytes))
    }
}

/// Transaction identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxId(pub u64);

/// Page type discriminant stored in page headers.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    BTreeInternal = 0x01,
    BTreeLeaf = 0x02,
    Heap = 0x03,
    Overflow = 0x04,
    Free = 0x05,
    FileHeader = 0x06,
    FileHeaderShadow = 0x07,
}

impl PageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::BTreeInternal),
            0x02 => Some(Self::BTreeLeaf),
            0x03 => Some(Self::Heap),
            0x04 => Some(Self::Overflow),
            0x05 => Some(Self::Free),
            0x06 => Some(Self::FileHeader),
            0x07 => Some(Self::FileHeaderShadow),
            _ => None,
        }
    }
}

/// WAL record type discriminant.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    TxCommit = 0x01,
    Checkpoint = 0x02,
    CreateCollection = 0x03,
    DropCollection = 0x04,
    CreateIndex = 0x05,
    DropIndex = 0x06,
    IndexReady = 0x07,
    Vacuum = 0x08,
    CreateDatabase = 0x10,
    DropDatabase = 0x11,
}

impl WalRecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::TxCommit),
            0x02 => Some(Self::Checkpoint),
            0x03 => Some(Self::CreateCollection),
            0x04 => Some(Self::DropCollection),
            0x05 => Some(Self::CreateIndex),
            0x06 => Some(Self::DropIndex),
            0x07 => Some(Self::IndexReady),
            0x08 => Some(Self::Vacuum),
            0x10 => Some(Self::CreateDatabase),
            0x11 => Some(Self::DropDatabase),
            _ => None,
        }
    }
}

/// Leaf cell flags.
pub mod cell_flags {
    pub const TOMBSTONE: u8 = 0x01;
    pub const EXTERNAL: u8 = 0x02;
}

/// Index state in the catalog.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Building = 0x01,
    Ready = 0x02,
    Dropping = 0x03,
}

/// Database state in the system catalog.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseState {
    Active = 0x01,
    Creating = 0x02,
    Dropping = 0x03,
}

/// Operation type for WAL mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Insert = 0x01,
    Replace = 0x02,
    Delete = 0x03,
}

impl OpType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Insert),
            0x02 => Some(Self::Replace),
            0x03 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Default page size.
pub const DEFAULT_PAGE_SIZE: u32 = 8192;

/// Page header size in bytes.
pub const PAGE_HEADER_SIZE: usize = 32;

/// Slot directory entry size (offset: u16 + length: u16).
pub const SLOT_ENTRY_SIZE: usize = 4;

/// WAL record frame header size.
pub const WAL_FRAME_HEADER_SIZE: usize = 9;

/// WAL segment header size.
pub const WAL_SEGMENT_HEADER_SIZE: usize = 32;

/// WAL segment magic.
pub const WAL_MAGIC: u32 = 0x57414C00;

/// Data file magic.
pub const DATA_FILE_MAGIC: u32 = 0x45584442;

/// DWB file magic.
pub const DWB_MAGIC: u32 = 0x44574200;

/// Primary key size: doc_id[16] + inv_ts[8].
pub const PRIMARY_KEY_SIZE: usize = 24;

/// Reference to a document stored in the external heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

/// A field path: ordered array of string segments.
pub type FieldPath = Vec<String>;

/// Encoded index key — opaque byte string, memcmp-comparable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedKey(pub Vec<u8>);

impl AsRef<[u8]> for EncodedKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id_ordering() {
        assert!(PageId(1) < PageId(2));
        assert_eq!(PageId(5), PageId(5));
    }

    #[test]
    fn test_lsn_advance() {
        let lsn = Lsn(100);
        assert_eq!(lsn.advance(50), Lsn(150));
    }

    #[test]
    fn test_timestamp_inverted() {
        let ts = Timestamp(10);
        assert_eq!(ts.inverted(), u64::MAX - 10);
    }

    #[test]
    fn test_doc_id_bytes_roundtrip() {
        let id = DocId(0x0123456789ABCDEF_FEDCBA9876543210);
        let bytes = id.to_bytes();
        assert_eq!(DocId::from_bytes(bytes), id);
    }

    #[test]
    fn test_page_type_from_u8() {
        assert_eq!(PageType::from_u8(0x01), Some(PageType::BTreeInternal));
        assert_eq!(PageType::from_u8(0x02), Some(PageType::BTreeLeaf));
        assert_eq!(PageType::from_u8(0xFF), None);
    }

    #[test]
    fn test_encoded_key_ordering() {
        let k1 = EncodedKey(vec![0x01, 0x02]);
        let k2 = EncodedKey(vec![0x01, 0x03]);
        assert!(k1 < k2);
    }
}
