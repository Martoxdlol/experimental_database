# 01 — Core Types

All fundamental type aliases and newtypes used across the storage layer.

## File: `src/storage/types.rs`

```rust
use std::fmt;

/// Page identifier — index into the data file. Page 0 = file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u32);

impl PageId {
    pub const INVALID: PageId = PageId(0); // page 0 is file header, never a data page
    pub const FILE_HEADER: PageId = PageId(0);
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
    BTreeLeaf     = 0x02,
    Heap          = 0x03,
    Overflow      = 0x04,
    Free          = 0x05,
    FileHeader    = 0x06,
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
    TxCommit          = 0x01,
    Checkpoint        = 0x02,
    CreateCollection  = 0x03,
    DropCollection    = 0x04,
    CreateIndex       = 0x05,
    DropIndex         = 0x06,
    IndexReady        = 0x07,
    Vacuum            = 0x08,
    CreateDatabase    = 0x10,
    DropDatabase      = 0x11,
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
    pub const EXTERNAL:  u8 = 0x02;
}

/// Index state in the catalog.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Building = 0x01,
    Ready    = 0x02,
    Dropping = 0x03,
}

/// Database state in the system catalog.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseState {
    Active   = 0x01,
    Creating = 0x02,
    Dropping = 0x03,
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

/// Reference to a document stored in the external heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

/// A field path: ordered array of string segments (e.g. ["user", "email"]).
pub type FieldPath = Vec<String>;

/// Encoded index key — opaque byte string, memcmp-comparable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EncodedKey(pub Vec<u8>);
```

## Design Notes

- Newtypes prevent accidental mixing (e.g., `PageId` vs `FrameId`).
- `PageId(0)` is always the file header — it's valid but never used as a data page.
- `Lsn` is a byte offset, not a record count. `advance(bytes)` moves forward.
- `DocId` is stored big-endian in keys for correct sort order.
- `EncodedKey` wraps `Vec<u8>` and derives `Ord` — byte comparison IS the sort order.
