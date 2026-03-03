# 01 — Shared Types (`storage/types.rs`)

All foundational types used across the storage layer. No dependencies on other storage modules.

```rust
/// Page identifier — index into data.db (page 0 = file header).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u32);

/// Frame identifier — index into the buffer pool frame array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub u32);

/// Log sequence number — byte offset in the logical WAL stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

/// Monotonic transaction timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

/// Inverted timestamp for descending sort: u64::MAX - ts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct InvTs(pub u64);

impl InvTs {
    pub fn from_ts(ts: Timestamp) -> Self {
        Self(u64::MAX - ts.0)
    }
    pub fn to_ts(self) -> Timestamp {
        Timestamp(u64::MAX - self.0)
    }
}

/// Collection identifier (unique within a database).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionId(pub u64);

/// Index identifier (unique within a database).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexId(pub u64);

/// Database identifier (unique across the system).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub u64);

/// 128-bit ULID document identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocId(pub u128);

/// Transaction identifier (unique within a connection).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId(pub u64);

// ── Page types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    FileHeader       = 0x00,
    BTreeInternal    = 0x01,
    BTreeLeaf        = 0x02,
    Heap             = 0x03,
    Overflow         = 0x04,
    Free             = 0x05,
    FileHeaderShadow = 0x06,
}

// ── Constants ───────────────────────────────────────────────────────

pub const PAGE_HEADER_SIZE: usize = 32;
pub const SLOT_ENTRY_SIZE: usize = 4; // offset: u16 + length: u16
pub const DEFAULT_PAGE_SIZE: u32 = 8192;

// WAL
pub const WAL_SEGMENT_HEADER_SIZE: usize = 32;
pub const WAL_RECORD_HEADER_SIZE: usize = 9; // payload_len(4) + crc32c(4) + record_type(1)
pub const WAL_SEGMENT_MAGIC: u32 = 0x57414C00;
pub const WAL_TARGET_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB

// DWB
pub const DWB_HEADER_SIZE: usize = 16;
pub const DWB_MAGIC: u32 = 0x44574200;

// File header (page 0)
pub const FILE_HEADER_MAGIC: u32 = 0x45584442; // "EXDB"

// ── Leaf cell flags ─────────────────────────────────────────────────

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CellFlags: u8 {
        const TOMBSTONE = 0b0000_0001;
        const EXTERNAL  = 0b0000_0010;
    }
}

// ── Heap reference ──────────────────────────────────────────────────

/// Pointer to a document body stored in the external heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapRef {
    pub page_id: PageId,
    pub slot_id: u16,
}

// ── WAL record type codes ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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

// ── Field path ──────────────────────────────────────────────────────

/// A path identifying a (possibly nested) field within a document.
/// E.g. ["user", "email"] for `user.email`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<String>);
```

## Dependencies

```toml
[dependencies]
bitflags = "2"
```

Only `bitflags` is needed for `CellFlags`. Everything else is plain Rust.
