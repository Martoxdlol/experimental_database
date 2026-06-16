//! StorageEngine facade composing all sub-layers.
//!
//! This is the public API of Layer 2. All access from higher layers goes through
//! this module. Composes backends, buffer pool, free list, WAL, B-trees, heap,
//! checkpoint, recovery, and vacuum into a single coherent interface.

use crate::backend::{
    MemoryPageStorage, MemoryWalStorage, PageId, PageStorage, WAL_SEGMENT_HEADER_SIZE, WalStorage,
};
use crate::btree::{BTree, ScanDirection, ScanStream};
use crate::buffer_pool::{BufferPool, BufferPoolConfig};
use crate::checkpoint::Checkpoint;
use crate::dwb::DoubleWriteBuffer;
use crate::free_list::FreeList;
use crate::heap::{Heap, HeapRef};
use crate::page::{PageType, SlottedPage, SlottedPageRef};
use crate::recovery::{Recovery, RecoveryMode, WalRecordHandler};
use crate::vacuum::{VacuumEntry, VacuumTask};
use crate::wal::{
    Lsn, MAX_WAL_RECORD_SIZE, WAL_FRAME_HEADER_SIZE, WalConfig, WalReader, WalStream, WalWriter,
    compute_crc as compute_wal_crc,
};

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64 as AtomicLsn, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

use zerocopy::byteorder::{LittleEndian, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ─── Constants ───

/// Size of the page header in bytes (mirrors page.rs PAGE_HEADER_SIZE).
const PAGE_HEADER_SIZE: usize = 32;

/// Size of a slotted-page directory entry (offset:u16 + length:u16).
const SLOT_ENTRY_SIZE: usize = 4;

/// Magic number for the file header: "EXDB" in little-endian.
const FILE_HEADER_MAGIC: u32 = 0x45584442;

/// Current file format version.
const FILE_HEADER_VERSION: u32 = 1;

/// Size of the FileHeader struct in bytes.
const FILE_HEADER_SIZE: usize = 92;

/// Sentinel for "no replication WAL retention bound is currently installed".
const NO_REPLICATION_RETENTION_LSN: u64 = u64::MAX;

// Compile-time assertion that FileHeader is exactly 84 bytes.
const _: () = assert!(std::mem::size_of::<FileHeader>() == FILE_HEADER_SIZE);

// ─── StorageConfig ───

/// Storage engine configuration.
pub struct StorageConfig {
    /// Page size in bytes. Default: 8192.
    pub page_size: usize,
    /// Memory budget for the buffer pool in bytes. Default: 256 MB.
    /// Determines frame count as `memory_budget / page_size`.
    pub memory_budget: usize,
    /// Optional maximum retained disk/page-storage bytes for this database.
    /// Defaults to unlimited.
    pub max_disk_usage_bytes: Option<u64>,
    /// WAL segment size in bytes. Default: 64 MB.
    pub wal_segment_size: usize,
    /// Optional cap for retained WAL bytes after checkpoint reclamation.
    ///
    /// When set, checkpoint may reclaim WAL needed by a lagging replica once
    /// retained WAL exceeds this cap. That replica must recover from a full
    /// snapshot instead of Tier 1 WAL catch-up.
    pub wal_retention_max_size: Option<u64>,
    /// Optional maximum age for sealed WAL segments after checkpoint
    /// reclamation.
    ///
    /// The active segment is never reclaimed by this bound.
    pub wal_retention_max_age: Option<Duration>,
    /// Bytes of WAL written before triggering an auto-checkpoint.
    pub checkpoint_wal_threshold: usize,
    /// Time between auto-checkpoint checks.
    pub checkpoint_interval: Duration,
}

/// Severity of an integrity-check finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegritySeverity {
    /// Structural corruption or metadata inconsistency that requires repair.
    Error,
    /// Suspicious state that may be valid but should be investigated.
    Warning,
    /// Informational observation.
    Info,
}

/// A single integrity-check finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityIssue {
    pub severity: IntegritySeverity,
    pub page_id: Option<PageId>,
    pub message: String,
}

/// Aggregate counters collected during an integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityStats {
    pub page_count: u64,
    pub pages_scanned: u64,
    pub free_pages: u64,
    pub btree_pages: u64,
    pub heap_pages: u64,
    pub overflow_pages: u64,
    pub orphan_btree_pages: u64,
    pub orphan_heap_pages: u64,
    pub double_allocated_pages: u64,
    pub wal_records_scanned: u64,
    pub wal_bytes_scanned: u64,
    pub page_type_counts: BTreeMap<String, u64>,
}

/// A named B-tree root to include in full integrity checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityBTreeRoot {
    pub name: String,
    pub root_page: PageId,
}

/// Result of a storage integrity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityReport {
    pub issues: Vec<IntegrityIssue>,
    pub stats: IntegrityStats,
}

/// Point-in-time storage resource usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUsage {
    pub page_count: u64,
    pub page_size: usize,
    pub page_store_bytes: u64,
    pub wal_retained_bytes: u64,
    pub disk_usage_bytes: u64,
    pub memory_budget_bytes: usize,
    pub buffer_pool_used_frames: usize,
}

/// Durable state observed before opening an existing file-backed database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableOpenProbe {
    pub generation: u64,
    pub replication_applied_lsn: Lsn,
    pub checkpoint_lsn: Lsn,
    pub recovery_needed: bool,
}

/// A checkpointed page-store image suitable for full replica reconstruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageSnapshot {
    pub page_size: usize,
    pub page_count: u64,
    pub checkpoint_lsn: Lsn,
    pages: Vec<u8>,
}

impl StorageSnapshot {
    pub fn new(
        page_size: usize,
        page_count: u64,
        checkpoint_lsn: Lsn,
        pages: Vec<u8>,
    ) -> io::Result<Self> {
        let expected_len = snapshot_byte_len(page_size, page_count)?;
        if pages.len() != expected_len {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "snapshot page image length {} does not match page_size {} * page_count {} ({})",
                pages.len(),
                page_size,
                page_count,
                expected_len
            ))
            .into());
        }
        Ok(Self {
            page_size,
            page_count,
            checkpoint_lsn,
            pages,
        })
    }

    pub fn pages(&self) -> &[u8] {
        &self.pages
    }

    pub fn into_pages(self) -> Vec<u8> {
        self.pages
    }

    pub fn into_chunks(self, max_chunk_len: usize) -> io::Result<Vec<Vec<u8>>> {
        if max_chunk_len == 0 {
            return Err(crate::error::StorageError::InvalidConfig(
                "snapshot chunk size must be greater than zero".into(),
            )
            .into());
        }

        let mut chunks = Vec::new();
        chunks.push(encode_snapshot_header(
            self.page_size,
            self.page_count,
            self.checkpoint_lsn,
        )?);
        chunks.extend(self.pages.chunks(max_chunk_len).map(Vec::from));
        Ok(chunks)
    }

    pub fn from_chunks<I>(chunks: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut iter = chunks.into_iter();
        let header = iter.next().ok_or_else(|| {
            io::Error::from(crate::error::StorageError::Corruption(
                "snapshot stream is missing header chunk".into(),
            ))
        })?;
        let (page_size, page_count, checkpoint_lsn) = decode_snapshot_header(&header)?;
        let expected_len = snapshot_byte_len(page_size, page_count)?;
        let mut pages = Vec::with_capacity(expected_len);
        for chunk in iter {
            pages.extend_from_slice(&chunk);
            if pages.len() > expected_len {
                return Err(crate::error::StorageError::Corruption(format!(
                    "snapshot stream has {} page bytes, expected {}",
                    pages.len(),
                    expected_len
                ))
                .into());
            }
        }
        Self::new(page_size, page_count, checkpoint_lsn, pages)
    }
}

/// A single repair performed by [`StorageEngine::repair_integrity`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrityRepair {
    pub page_id: Option<PageId>,
    pub message: String,
}

/// Result of an integrity repair pass.
///
/// Repair is intentionally conservative. The current pass only fixes metadata
/// that has an authoritative duplicate in durable storage, then reports any
/// remaining findings for operator follow-up.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntegrityRepairReport {
    pub repairs: Vec<IntegrityRepair>,
    pub remaining_issues: Vec<IntegrityIssue>,
}

#[derive(Debug, Clone)]
struct BTreeLeafSummary {
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    right_sibling: PageId,
}

impl IntegrityReport {
    /// True when no error-severity findings were produced.
    pub fn is_ok(&self) -> bool {
        !self.has_errors()
    }

    /// True when the report contains at least one corruption-level finding.
    pub fn has_errors(&self) -> bool {
        self.issues
            .iter()
            .any(|issue| issue.severity == IntegritySeverity::Error)
    }

    fn push_error(&mut self, page_id: Option<PageId>, message: impl Into<String>) {
        self.issues.push(IntegrityIssue {
            severity: IntegritySeverity::Error,
            page_id,
            message: message.into(),
        });
    }

    fn push_warning(&mut self, page_id: Option<PageId>, message: impl Into<String>) {
        self.issues.push(IntegrityIssue {
            severity: IntegritySeverity::Warning,
            page_id,
            message: message.into(),
        });
    }
}

impl IntegrityRepairReport {
    /// True when at least one on-disk repair was applied.
    pub fn repaired(&self) -> bool {
        !self.repairs.is_empty()
    }

    /// True when the post-repair integrity pass has no remaining issues.
    pub fn is_clean(&self) -> bool {
        self.remaining_issues.is_empty()
    }
}

impl StorageConfig {
    /// Validate the configuration, returning an error on invalid values.
    pub fn validate(&self) -> io::Result<()> {
        // Page header uses u16 for offsets/lengths, so max page size is 65535.
        // DwbHeader::page_size is also u16.
        if self.page_size > u16::MAX as usize {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_size {} exceeds maximum of {} (u16 offset limit)",
                self.page_size,
                u16::MAX
            ))
            .into());
        }
        if self.page_size < 64 {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "page_size {} is too small (minimum 64)",
                self.page_size
            ))
            .into());
        }
        if self.memory_budget < self.page_size {
            return Err(crate::error::StorageError::InvalidConfig(
                "memory_budget must be at least page_size".into(),
            )
            .into());
        }
        if self.wal_segment_size <= WAL_SEGMENT_HEADER_SIZE as usize {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "wal_segment_size {} must be greater than WAL segment header size {}",
                self.wal_segment_size, WAL_SEGMENT_HEADER_SIZE
            ))
            .into());
        }
        if self.checkpoint_wal_threshold == 0 {
            return Err(crate::error::StorageError::InvalidConfig(
                "checkpoint_wal_threshold must be greater than zero".into(),
            )
            .into());
        }
        if self.checkpoint_interval.is_zero() {
            return Err(crate::error::StorageError::InvalidConfig(
                "checkpoint_interval must be greater than zero".into(),
            )
            .into());
        }
        if let Some(limit) = self.max_disk_usage_bytes
            && limit < self.page_size as u64
        {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "max_disk_usage_bytes {} is smaller than page_size {}",
                limit, self.page_size
            ))
            .into());
        }
        if let Some(limit) = self.wal_retention_max_size
            && limit == 0
        {
            return Err(crate::error::StorageError::InvalidConfig(
                "wal_retention_max_size must be greater than zero".into(),
            )
            .into());
        }
        Ok(())
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            page_size: 8192,
            memory_budget: 256 * 1024 * 1024,
            max_disk_usage_bytes: None,
            wal_segment_size: 64 * 1024 * 1024,
            wal_retention_max_size: None,
            wal_retention_max_age: None,
            checkpoint_wal_threshold: 64 * 1024 * 1024,
            checkpoint_interval: Duration::from_secs(300),
        }
    }
}

// ─── FileHeader ───

/// File header stored in page 0 of the data file.
///
/// Uses `zerocopy` with LE wrapper types for zero-copy read/write.
/// Stored at offset `PAGE_HEADER_SIZE` (32) within page 0.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Debug)]
#[repr(C)]
pub struct FileHeader {
    /// Magic number: 0x45584442 ("EXDB").
    pub magic: U32<LittleEndian>,
    /// Format version (1).
    pub version: U32<LittleEndian>,
    /// Page size in bytes.
    pub page_size: U32<LittleEndian>,
    /// Total number of pages in the data file.
    pub page_count: U64<LittleEndian>,
    /// Head of the free page list (PageId). 0 = empty.
    pub free_list_head: U32<LittleEndian>,
    /// Root page of the catalog by-ID B-tree.
    pub catalog_root_page: U32<LittleEndian>,
    /// Root page of the catalog by-name B-tree.
    pub catalog_name_root_page: U32<LittleEndian>,
    /// Reserved for future use.
    pub _reserved: [u8; 4],
    /// Next collection ID to allocate.
    pub next_collection_id: U64<LittleEndian>,
    /// Next index ID to allocate.
    pub next_index_id: U64<LittleEndian>,
    /// LSN of the last completed checkpoint.
    pub checkpoint_lsn: U64<LittleEndian>,
    /// Latest visible timestamp for MVCC.
    pub visible_ts: U64<LittleEndian>,
    /// Highest primary-source WAL LSN durably applied by a replica.
    pub replication_applied_lsn: U64<LittleEndian>,
    /// Cluster generation counter.
    pub generation: U64<LittleEndian>,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: U64<LittleEndian>,
}

impl FileHeader {
    /// Create a new FileHeader with default values for a fresh database.
    fn new(page_size: usize) -> Self {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        FileHeader {
            magic: U32::new(FILE_HEADER_MAGIC),
            version: U32::new(FILE_HEADER_VERSION),
            page_size: U32::new(page_size as u32),
            page_count: U64::new(0),
            free_list_head: U32::new(0),
            catalog_root_page: U32::new(0),
            catalog_name_root_page: U32::new(0),
            _reserved: [0u8; 4],
            next_collection_id: U64::new(1),
            next_index_id: U64::new(1),
            checkpoint_lsn: U64::new(0),
            visible_ts: U64::new(0),
            replication_applied_lsn: U64::new(0),
            generation: U64::new(1),
            created_at: U64::new(now_millis),
        }
    }

    /// Verify the magic number and version.
    pub fn verify(&self) -> io::Result<()> {
        if self.magic.get() != FILE_HEADER_MAGIC {
            return Err(crate::error::StorageError::Corruption(format!(
                "file header magic mismatch: expected 0x{:08X}, got 0x{:08X}",
                FILE_HEADER_MAGIC,
                self.magic.get()
            ))
            .into());
        }
        if self.version.get() != FILE_HEADER_VERSION {
            return Err(crate::error::StorageError::Corruption(format!(
                "file header version mismatch: expected {}, got {}",
                FILE_HEADER_VERSION,
                self.version.get()
            ))
            .into());
        }
        Ok(())
    }
}

/// Read a FileHeader from a page buffer at offset PAGE_HEADER_SIZE.
fn read_file_header(buf: &[u8]) -> io::Result<FileHeader> {
    let start = PAGE_HEADER_SIZE;
    let end = start + FILE_HEADER_SIZE;
    if buf.len() < end {
        return Err(crate::error::StorageError::Corruption(
            "page 0 buffer too small for FileHeader".into(),
        )
        .into());
    }
    FileHeader::read_from_bytes(&buf[start..end]).map_err(|e| {
        io::Error::from(crate::error::StorageError::Corruption(format!(
            "failed to read FileHeader: {:?}",
            e
        )))
    })
}

/// Write a FileHeader into a page buffer at offset PAGE_HEADER_SIZE.
fn write_file_header(buf: &mut [u8], header: &FileHeader) {
    let start = PAGE_HEADER_SIZE;
    let end = start + FILE_HEADER_SIZE;
    buf[start..end].copy_from_slice(header.as_bytes());
    if let Ok(mut page) = SlottedPage::from_buf(buf) {
        page.stamp_checksum();
    }
}

fn page_id_from_u64(page_id: u64) -> io::Result<PageId> {
    PageId::try_from(page_id).map_err(|_| {
        crate::error::StorageError::InternalBug(format!("page id {} exceeds PageId range", page_id))
            .into()
    })
}

const STORAGE_SNAPSHOT_MAGIC: &[u8; 8] = b"EXDBSNAP";
const STORAGE_SNAPSHOT_VERSION: u32 = 1;
const STORAGE_SNAPSHOT_HEADER_LEN: usize = 32;

fn snapshot_byte_len(page_size: usize, page_count: u64) -> io::Result<usize> {
    let page_count = usize::try_from(page_count).map_err(|_| {
        crate::error::StorageError::InvalidConfig(format!(
            "snapshot page_count {page_count} exceeds platform usize"
        ))
    })?;
    page_size.checked_mul(page_count).ok_or_else(|| {
        crate::error::StorageError::InvalidConfig("snapshot page image length overflow".to_string())
            .into()
    })
}

fn encode_snapshot_header(
    page_size: usize,
    page_count: u64,
    checkpoint_lsn: Lsn,
) -> io::Result<Vec<u8>> {
    let page_size = u32::try_from(page_size).map_err(|_| {
        crate::error::StorageError::InvalidConfig(format!(
            "snapshot page_size {page_size} exceeds u32"
        ))
    })?;
    let mut out = Vec::with_capacity(STORAGE_SNAPSHOT_HEADER_LEN);
    out.extend_from_slice(STORAGE_SNAPSHOT_MAGIC);
    out.extend_from_slice(&STORAGE_SNAPSHOT_VERSION.to_le_bytes());
    out.extend_from_slice(&page_size.to_le_bytes());
    out.extend_from_slice(&page_count.to_le_bytes());
    out.extend_from_slice(&checkpoint_lsn.to_le_bytes());
    Ok(out)
}

fn decode_snapshot_header(header: &[u8]) -> io::Result<(usize, u64, Lsn)> {
    if header.len() != STORAGE_SNAPSHOT_HEADER_LEN {
        return Err(crate::error::StorageError::Corruption(format!(
            "snapshot header length {} does not match expected {}",
            header.len(),
            STORAGE_SNAPSHOT_HEADER_LEN
        ))
        .into());
    }
    if &header[..8] != STORAGE_SNAPSHOT_MAGIC {
        return Err(crate::error::StorageError::Corruption(
            "snapshot header magic mismatch".into(),
        )
        .into());
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if version != STORAGE_SNAPSHOT_VERSION {
        return Err(crate::error::StorageError::Corruption(format!(
            "unsupported snapshot version {version}"
        ))
        .into());
    }
    let page_size = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    let page_count = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let checkpoint_lsn = u64::from_le_bytes(header[24..32].try_into().unwrap());
    Ok((page_size, page_count, checkpoint_lsn))
}

fn build_file_header_page(
    page_id: PageId,
    page_type: PageType,
    page_size: usize,
    header: &FileHeader,
) -> Vec<u8> {
    let mut buf = vec![0u8; page_size];
    SlottedPage::init(&mut buf, page_id, page_type);
    write_file_header(&mut buf, header);
    buf
}

fn read_verified_file_header_page(
    buf: &[u8],
    expected_page_id: PageId,
    expected_page_type: PageType,
) -> io::Result<FileHeader> {
    let page = SlottedPageRef::from_buf(buf)?;
    if page.page_id() != expected_page_id {
        return Err(crate::error::StorageError::Corruption(format!(
            "file header page id mismatch: expected {}, got {}",
            expected_page_id,
            page.page_id()
        ))
        .into());
    }
    if page.try_page_type() != Some(expected_page_type) {
        return Err(crate::error::StorageError::Corruption(format!(
            "file header page type mismatch: expected {:?}, got {:?}",
            expected_page_type,
            page.try_page_type()
        ))
        .into());
    }
    if !page.verify_checksum() {
        return Err(crate::error::StorageError::Corruption(
            "file header page checksum mismatch".into(),
        )
        .into());
    }

    let header = read_file_header(buf)?;
    header.verify()?;
    Ok(header)
}

async fn read_verified_file_header_from_storage(
    page_storage: &dyn PageStorage,
    page_id: PageId,
    page_type: PageType,
    page_size: usize,
) -> io::Result<FileHeader> {
    let mut buf = vec![0u8; page_size];
    page_storage.read_page(page_id, &mut buf).await?;
    read_verified_file_header_page(&buf, page_id, page_type)
}

async fn write_file_header_page_to_storage(
    page_storage: &dyn PageStorage,
    page_id: PageId,
    page_type: PageType,
    page_size: usize,
    header: &FileHeader,
) -> io::Result<()> {
    let buf = build_file_header_page(page_id, page_type, page_size, header);
    page_storage.write_page(page_id, &buf).await
}

async fn read_primary_or_shadow_file_header(
    page_storage: &dyn PageStorage,
    page_size: usize,
) -> io::Result<FileHeader> {
    match read_verified_file_header_from_storage(page_storage, 0, PageType::FileHeader, page_size)
        .await
    {
        Ok(header) => Ok(header),
        Err(primary_err) => {
            let page_count = page_storage.page_count();
            if page_count < 2 {
                return Err(primary_err);
            }

            let shadow_page_id = page_id_from_u64(page_count - 1)?;
            let shadow = read_verified_file_header_from_storage(
                page_storage,
                shadow_page_id,
                PageType::FileHeaderShadow,
                page_size,
            )
            .await
            .map_err(|shadow_err| {
                crate::error::StorageError::Corruption(format!(
                    "primary file header is invalid ({primary_err}); shadow header is invalid ({shadow_err})"
                ))
            })?;

            write_file_header_page_to_storage(
                page_storage,
                0,
                PageType::FileHeader,
                page_size,
                &shadow,
            )
            .await?;
            page_storage.sync().await?;
            Ok(shadow)
        }
    }
}

async fn ensure_shadow_file_header(
    page_storage: &dyn PageStorage,
    page_size: usize,
    header: &FileHeader,
) -> io::Result<FileHeader> {
    if !page_storage.is_durable() {
        return Ok(header.clone());
    }

    let page_count = page_storage.page_count();
    if page_count >= 2 {
        let last_page_id = page_id_from_u64(page_count - 1)?;
        if let Ok(shadow) = read_verified_file_header_from_storage(
            page_storage,
            last_page_id,
            PageType::FileHeaderShadow,
            page_size,
        )
        .await
            && shadow.as_bytes() == header.as_bytes()
        {
            return Ok(header.clone());
        }
    }

    let new_page_count = page_storage.page_count() + 1;
    page_storage.extend(new_page_count).await?;
    let shadow_page_id = page_id_from_u64(new_page_count - 1)?;

    let mut repaired = header.clone();
    repaired.page_count = U64::new(new_page_count);
    write_file_header_page_to_storage(page_storage, 0, PageType::FileHeader, page_size, &repaired)
        .await?;
    write_file_header_page_to_storage(
        page_storage,
        shadow_page_id,
        PageType::FileHeaderShadow,
        page_size,
        &repaired,
    )
    .await?;
    page_storage.sync().await?;

    Ok(repaired)
}

async fn truncate_zeroed_tail_pages_to_header(
    page_storage: &dyn PageStorage,
    page_size: usize,
    header: &FileHeader,
) -> io::Result<bool> {
    if !page_storage.is_durable() {
        return Ok(false);
    }

    let header_page_count = header.page_count.get();
    if header_page_count < 2 {
        return Ok(false);
    }

    let live_page_count = page_storage.page_count();
    if live_page_count <= header_page_count {
        return Ok(false);
    }

    for page_id_u64 in header_page_count..live_page_count {
        let page_id = page_id_from_u64(page_id_u64)?;
        let mut page = vec![0u8; page_size];
        page_storage.read_page(page_id, &mut page).await?;
        if page.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
    }

    let Some(size) = header_page_count.checked_mul(page_size as u64) else {
        return Err(crate::error::StorageError::Corruption(format!(
            "file header page_count {} overflows physical byte length for page_size {}",
            header_page_count, page_size
        ))
        .into());
    };
    page_storage.truncate_to_size_bytes(size).await?;
    page_storage.sync().await?;
    Ok(true)
}

// ─── BTreeHandle ───

/// A handle to a B-tree, bound to the specific components it needs.
///
/// Holds references to the free list and buffer pool directly, avoiding
/// an `Arc<StorageEngine>` cycle.
pub struct BTreeHandle {
    btree: BTree,
    free_list: Arc<Mutex<FreeList>>,
    #[allow(dead_code)]
    buffer_pool: Arc<BufferPool>,
}

impl BTreeHandle {
    /// Point lookup. Returns value bytes if found.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.btree.get(key).await
    }

    /// Insert a key-value pair.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut free_list = self.free_list.lock().await;
        self.btree.insert(key, value, &mut free_list).await
    }

    /// Delete a key. Returns true if the key existed.
    pub async fn delete(&self, key: &[u8]) -> io::Result<bool> {
        let mut free_list = self.free_list.lock().await;
        self.btree.delete(key, &mut free_list).await
    }

    /// Range scan.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        direction: ScanDirection,
    ) -> ScanStream<'_> {
        self.btree.scan(lower, upper, direction)
    }

    /// Root page of the underlying B-tree.
    pub fn root_page(&self) -> PageId {
        self.btree.root_page()
    }
}

// ─── StorageEngine ───

/// The main storage engine facade.
///
/// Composes all sub-layers (backends, buffer pool, free list, WAL, heap,
/// checkpoint, recovery, vacuum) into a single coherent API.
pub struct StorageEngine {
    // Backends
    page_storage: Arc<dyn PageStorage>,
    #[allow(dead_code)]
    wal_storage: Arc<dyn WalStorage>,

    // Sub-components
    buffer_pool: Arc<BufferPool>,
    free_list: Arc<Mutex<FreeList>>,
    wal_writer: Arc<WalWriter>,
    wal_reader: WalReader,
    heap: Mutex<Heap>,
    checkpoint: Checkpoint,
    vacuum_task: VacuumTask,

    // Metadata
    file_header: Mutex<FileHeader>,
    config: StorageConfig,
    is_durable: bool,
    replication_retention_lsn: AtomicLsn,
    #[allow(dead_code)]
    path: Option<PathBuf>,
}

impl StorageEngine {
    // ─── Lifecycle ───

    /// Open a file-backed storage engine (durable).
    ///
    /// Creates directories if needed, opens or creates the data file and WAL,
    /// and runs recovery if an existing database is detected.
    pub async fn open(
        path: &Path,
        config: StorageConfig,
        handler: &mut dyn WalRecordHandler,
    ) -> io::Result<Self> {
        use crate::backend::{FilePageStorage, FileWalStorage};

        // Ensure directories exist.
        std::fs::create_dir_all(path)?;
        let wal_dir = path.join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let data_path = path.join("data.db");
        let dwb_path = path.join("data.dwb");

        // Open or create page storage.
        let page_storage: Arc<dyn PageStorage> = if data_path.exists() {
            Arc::new(FilePageStorage::open_with_max_size(
                &data_path,
                config.page_size,
                config.max_disk_usage_bytes,
            )?)
        } else {
            Arc::new(FilePageStorage::create_with_max_size(
                &data_path,
                config.page_size,
                config.max_disk_usage_bytes,
            )?)
        };

        // Open or create WAL storage.
        let wal_storage: Arc<dyn WalStorage> = if wal_dir.join("segment-000001.wal").exists() {
            Arc::new(FileWalStorage::open(&wal_dir, config.wal_segment_size)?)
        } else {
            Arc::new(FileWalStorage::create(&wal_dir, config.wal_segment_size)?)
        };

        let is_new = page_storage.page_count() == 0;

        if is_new {
            // Initialize new database.
            Self::init_new_database(
                page_storage.clone(),
                wal_storage.clone(),
                config,
                true,
                Some(dwb_path),
                Some(path.to_path_buf()),
            )
            .await
        } else {
            // Existing database: read file header, run recovery.
            let mut file_header =
                read_primary_or_shadow_file_header(page_storage.as_ref(), config.page_size).await?;

            // Validate page_size matches the stored value.
            let stored_page_size = file_header.page_size.get() as usize;
            if stored_page_size != config.page_size {
                return Err(crate::error::StorageError::Corruption(format!(
                    "page size mismatch: file has {}, config has {}",
                    stored_page_size, config.page_size
                ))
                .into());
            }

            truncate_zeroed_tail_pages_to_header(
                page_storage.as_ref(),
                config.page_size,
                &file_header,
            )
            .await?;

            let checkpoint_lsn = file_header.checkpoint_lsn.get();

            // Run recovery.
            let (_end_lsn, _stats) = Recovery::run(
                page_storage.as_ref(),
                wal_storage.as_ref(),
                Some(dwb_path.as_path()),
                checkpoint_lsn,
                config.page_size,
                handler,
                RecoveryMode::Strict,
            )
            .await?;

            file_header =
                ensure_shadow_file_header(page_storage.as_ref(), config.page_size, &file_header)
                    .await?;

            // Build components.
            Self::build_from_existing(
                page_storage,
                wal_storage,
                file_header,
                config,
                true,
                Some(dwb_path),
                Some(path.to_path_buf()),
            )
        }
    }

    /// Probe durable header and recovery state for an existing file-backed database.
    ///
    /// This mirrors the file-backed open path enough for higher layers to make
    /// startup orchestration decisions before constructing a full engine. It
    /// does not replay WAL or restore DWB contents; normal `open` still owns
    /// actual recovery.
    pub async fn probe_existing_durable(
        path: &Path,
        config: StorageConfig,
    ) -> io::Result<Option<DurableOpenProbe>> {
        use crate::backend::{FilePageStorage, FileWalStorage};

        config.validate()?;

        let data_path = path.join("data.db");
        if !data_path.exists() {
            return Ok(None);
        }

        let page_storage = FilePageStorage::open_with_max_size(
            &data_path,
            config.page_size,
            config.max_disk_usage_bytes,
        )?;
        let file_header =
            read_primary_or_shadow_file_header(&page_storage, config.page_size).await?;

        let stored_page_size = file_header.page_size.get() as usize;
        if stored_page_size != config.page_size {
            return Err(crate::error::StorageError::Corruption(format!(
                "page size mismatch: file has {}, config has {}",
                stored_page_size, config.page_size
            ))
            .into());
        }

        let wal_dir = path.join("wal");
        let wal_storage = if wal_dir.join("segment-000001.wal").exists() {
            FileWalStorage::open(&wal_dir, config.wal_segment_size)?
        } else {
            FileWalStorage::create(&wal_dir, config.wal_segment_size)?
        };

        let checkpoint_lsn = file_header.checkpoint_lsn.get();
        let dwb_path = path.join("data.dwb");
        let dwb_recovery_needed = tokio::task::spawn_blocking(move || -> io::Result<bool> {
            if dwb_path.exists() {
                let metadata = std::fs::metadata(&dwb_path)?;
                Ok(metadata.len() > 0)
            } else {
                Ok(false)
            }
        })
        .await
        .map_err(|e| io::Error::other(format!("spawn_blocking join error: {e}")))??;

        use tokio_stream::StreamExt;
        let wal_reader = WalReader::new(Arc::new(wal_storage));
        let mut wal_stream = wal_reader.read_from(checkpoint_lsn);
        let mut wal_recovery_needed = false;
        while let Some(record) = wal_stream.next().await {
            let record = record?;
            if record.record_type != crate::wal::WAL_RECORD_CHECKPOINT {
                wal_recovery_needed = true;
                break;
            }
        }

        Ok(Some(DurableOpenProbe {
            generation: file_header.generation.get(),
            replication_applied_lsn: file_header.replication_applied_lsn.get(),
            checkpoint_lsn,
            recovery_needed: dwb_recovery_needed || wal_recovery_needed,
        }))
    }

    /// Open an ephemeral in-memory storage engine.
    pub async fn open_in_memory(config: StorageConfig) -> io::Result<Self> {
        let page_storage: Arc<dyn PageStorage> = Arc::new(MemoryPageStorage::new_with_max_size(
            config.page_size,
            config.max_disk_usage_bytes,
        ));
        let wal_storage: Arc<dyn WalStorage> = Arc::new(MemoryWalStorage::new());

        Self::init_new_database(page_storage, wal_storage, config, false, None, None).await
    }

    /// Open with custom backends.
    ///
    /// If the backend is durable and `handler` is Some, runs recovery.
    /// If the backend is durable and `handler` is None, recovery is skipped.
    /// If the backend is not durable, `handler` is ignored.
    pub async fn open_with_backend(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
        handler: Option<&mut dyn WalRecordHandler>,
    ) -> io::Result<Self> {
        config.validate()?;
        let is_durable = page_storage.is_durable();
        let is_new = page_storage.page_count() == 0;

        if is_new {
            Self::init_new_database(page_storage, wal_storage, config, is_durable, None, None).await
        } else {
            // Existing database.
            let mut file_header =
                read_primary_or_shadow_file_header(page_storage.as_ref(), config.page_size).await?;

            if is_durable {
                truncate_zeroed_tail_pages_to_header(
                    page_storage.as_ref(),
                    config.page_size,
                    &file_header,
                )
                .await?;
            }

            // Run recovery if durable and handler provided.
            if is_durable && let Some(h) = handler {
                let checkpoint_lsn = file_header.checkpoint_lsn.get();
                let (_end_lsn, _stats) = Recovery::run(
                    page_storage.as_ref(),
                    wal_storage.as_ref(),
                    None,
                    checkpoint_lsn,
                    config.page_size,
                    h,
                    RecoveryMode::Strict,
                )
                .await?;
            }

            file_header =
                ensure_shadow_file_header(page_storage.as_ref(), config.page_size, &file_header)
                    .await?;

            Self::build_from_existing(
                page_storage,
                wal_storage,
                file_header,
                config,
                is_durable,
                None,
                None,
            )
        }
    }

    /// Close the engine: final checkpoint if durable, shutdown WAL writer.
    pub async fn close(&self) -> io::Result<()> {
        // Run final checkpoint if durable.
        if self.is_durable {
            self.checkpoint().await?;
        }

        // Update file header with current free list head and page count
        // before writing to disk.
        {
            let free_list_head = self.free_list.lock().await.head();
            let page_count = self.page_storage.page_count();
            self.update_file_header(|fh| {
                fh.free_list_head = U32::new(free_list_head);
                fh.page_count = U64::new(page_count);
            })
            .await?;
        }

        // Write final file header to page 0.
        self.write_file_header_to_page0().await?;

        // Sync page storage.
        self.page_storage.sync().await?;

        // Release exclusive file lock so the database can be reopened.
        self.page_storage.unlock();

        Ok(())
    }

    /// Release the exclusive file lock without closing the engine.
    ///
    /// Used by crash simulation in tests: releases the lock so the database
    /// can be reopened in the same process, without performing a final
    /// checkpoint (which would defeat the purpose of crash testing).
    pub fn unlock(&self) {
        self.page_storage.unlock();
    }

    /// Whether this engine uses durable storage.
    pub fn is_durable(&self) -> bool {
        self.is_durable
    }

    // ─── B-Tree Management ───

    /// Create a new B-tree with an empty root. Returns a handle.
    pub async fn create_btree(&self) -> io::Result<BTreeHandle> {
        let mut free_list = self.free_list.lock().await;
        let btree = BTree::create(self.buffer_pool.clone(), &mut free_list).await?;
        Ok(BTreeHandle {
            btree,
            free_list: self.free_list.clone(),
            buffer_pool: self.buffer_pool.clone(),
        })
    }

    /// Open an existing B-tree by root page.
    pub fn open_btree(&self, root_page: PageId) -> BTreeHandle {
        let btree = BTree::open(root_page, self.buffer_pool.clone());
        BTreeHandle {
            btree,
            free_list: self.free_list.clone(),
            buffer_pool: self.buffer_pool.clone(),
        }
    }

    // ─── Heap ───

    /// Store a blob in the heap. Returns a reference for later retrieval.
    pub async fn heap_store(&self, data: &[u8]) -> io::Result<HeapRef> {
        let mut heap = self.heap.lock().await;
        let mut free_list = self.free_list.lock().await;
        heap.store(data, &mut free_list).await
    }

    /// Load a blob from the heap.
    pub async fn heap_load(&self, href: HeapRef) -> io::Result<Vec<u8>> {
        let heap = self.heap.lock().await;
        heap.load(href).await
    }

    /// Free a blob from the heap.
    pub async fn heap_free(&self, href: HeapRef) -> io::Result<()> {
        let mut heap = self.heap.lock().await;
        let mut free_list = self.free_list.lock().await;
        heap.free(href, &mut free_list).await
    }

    /// Reclaim zero-filled pages left by crash-time file extension.
    ///
    /// Durable allocation keeps the last page reserved for the file-header
    /// shadow. A crash after extending the file but before initializing every
    /// allocated page can leave all-zero tail pages. They are not referenced by
    /// committed state after WAL recovery, so startup rollback can safely push
    /// those zeroed pages onto the free list before checkpointing the repaired
    /// header. Nonzero malformed pages are deliberately left untouched for
    /// integrity checking to report.
    pub async fn reclaim_zeroed_pages_for_recovery(&self) -> io::Result<u64> {
        let page_count = self.page_storage.page_count();
        if page_count <= 2 {
            return Ok(0);
        }

        let page_size = self.page_storage.page_size();
        let mut reclaimed = 0u64;
        let mut free_list = self.free_list.lock().await;
        let mut head = free_list.head();

        for page_id_u64 in 1..page_count - 1 {
            let page_id = page_id_from_u64(page_id_u64)?;
            let mut existing = vec![0u8; page_size];
            self.page_storage.read_page(page_id, &mut existing).await?;
            if !existing.iter().all(|byte| *byte == 0) {
                continue;
            }

            let mut free_page = vec![0u8; page_size];
            {
                let mut page = SlottedPage::init(&mut free_page, page_id, PageType::Free);
                page.set_prev_or_ptr(head);
                page.stamp_checksum();
            }
            self.page_storage.write_page(page_id, &free_page).await?;
            head = page_id;
            reclaimed += 1;
        }

        if reclaimed > 0 {
            free_list.set_head_for_recovery(head);
        }

        Ok(reclaimed)
    }

    /// Rebuild the recovery free-list head from pages physically stamped Free.
    ///
    /// WAL replay can reallocate pages that were still present in the durable
    /// checkpoint's free-list chain. This drops those stale links before the
    /// refreshed file header is written.
    pub async fn rebuild_free_list_from_existing_free_pages_for_recovery(
        &self,
    ) -> io::Result<usize> {
        let mut free_list = self.free_list.lock().await;
        free_list.rebuild_from_existing_free_pages().await
    }

    // ─── WAL ───

    /// Append a WAL record. Returns the assigned LSN after fsync.
    pub async fn append_wal(&self, record_type: u8, payload: &[u8]) -> io::Result<Lsn> {
        self.check_disk_quota_for_wal_append(payload.len())?;
        self.wal_writer.append(record_type, payload).await
    }

    /// Read WAL records starting from a given LSN.
    pub fn read_wal_from(&self, lsn: Lsn) -> WalStream {
        self.wal_reader.read_from(lsn)
    }

    /// Return the oldest currently retained WAL LSN, if any WAL is retained.
    pub fn oldest_retained_wal_lsn(&self) -> Option<Lsn> {
        self.wal_storage.oldest_lsn()
    }

    /// Return point-in-time resource usage for this storage engine.
    pub fn usage(&self) -> StorageUsage {
        let page_count = self.page_storage.page_count();
        let page_size = self.page_storage.page_size();
        let page_store_bytes = page_count.saturating_mul(page_size as u64);
        let wal_retained_bytes = self.wal_storage.retained_size();
        StorageUsage {
            page_count,
            page_size,
            page_store_bytes,
            wal_retained_bytes,
            disk_usage_bytes: page_store_bytes.saturating_add(wal_retained_bytes),
            memory_budget_bytes: self.config.memory_budget,
            buffer_pool_used_frames: self.buffer_pool.used_frames(),
        }
    }

    /// Export a checkpointed page-store snapshot for full reconstruction.
    ///
    /// The snapshot contains the complete page store after a checkpoint and the
    /// checkpoint LSN. Restoring it creates a fresh WAL whose first future append
    /// starts at that LSN.
    pub async fn export_snapshot(&self) -> io::Result<StorageSnapshot> {
        self.checkpoint().await?;
        self.page_storage.sync().await?;

        let file_header = self.file_header().await;
        let page_size = self.page_storage.page_size();
        let page_count = self.page_storage.page_count();
        let mut pages = Vec::with_capacity(snapshot_byte_len(page_size, page_count)?);
        let mut page = vec![0u8; page_size];

        for page_id_u64 in 0..page_count {
            let page_id = page_id_from_u64(page_id_u64)?;
            self.page_storage.read_page(page_id, &mut page).await?;
            pages.extend_from_slice(&page);
        }

        StorageSnapshot::new(
            page_size,
            page_count,
            file_header.checkpoint_lsn.get(),
            pages,
        )
    }

    /// Restore a checkpointed page-store snapshot into a fresh durable path.
    ///
    /// The target directory must not already contain a database. Callers can
    /// open the restored path normally after this returns.
    pub async fn restore_snapshot(
        path: &Path,
        config: StorageConfig,
        snapshot: StorageSnapshot,
    ) -> io::Result<()> {
        use crate::backend::{FilePageStorage, FileWalStorage};

        config.validate()?;
        if snapshot.page_size != config.page_size {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "snapshot page_size {} does not match config page_size {}",
                snapshot.page_size, config.page_size
            ))
            .into());
        }
        if snapshot.page_count == 0 {
            return Err(crate::error::StorageError::Corruption(
                "snapshot must contain at least a file-header page".into(),
            )
            .into());
        }

        let data_path = path.join("data.db");
        let wal_dir = path.join("wal");
        let dwb_path = path.join("data.dwb");
        if data_path.exists() || wal_dir.exists() || dwb_path.exists() {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "cannot restore snapshot into non-empty database path {}",
                path.display()
            ))
            .into());
        }

        std::fs::create_dir_all(path)?;
        let page_storage = FilePageStorage::create_with_max_size(
            &data_path,
            config.page_size,
            config.max_disk_usage_bytes,
        )?;
        page_storage.extend(snapshot.page_count).await?;
        for (page_id, page) in snapshot.pages().chunks(config.page_size).enumerate() {
            let page_id = page_id_from_u64(page_id as u64)?;
            page_storage.write_page(page_id, page).await?;
        }
        page_storage.sync().await?;
        page_storage.unlock();

        FileWalStorage::create_with_base_lsn(
            &wal_dir,
            config.wal_segment_size,
            snapshot.checkpoint_lsn,
        )?;

        Ok(())
    }

    fn check_disk_quota_for_wal_append(&self, payload_len: usize) -> io::Result<()> {
        let Some(limit) = self.config.max_disk_usage_bytes else {
            return Ok(());
        };
        let frame_len = WAL_FRAME_HEADER_SIZE
            .checked_add(payload_len)
            .ok_or_else(|| {
                crate::error::StorageError::InvalidConfig(
                    "WAL frame size overflow while checking disk usage limit".into(),
                )
            })? as u64;
        let projected = self.usage().disk_usage_bytes.saturating_add(frame_len);
        if projected > limit {
            return Err(crate::error::StorageError::InvalidConfig(format!(
                "disk usage limit exceeded: WAL append would grow retained usage to {} bytes (limit {})",
                projected, limit
            ))
            .into());
        }
        Ok(())
    }

    // ─── Maintenance ───

    /// Run a checkpoint. Flushes dirty pages and writes a checkpoint WAL record.
    pub async fn checkpoint(&self) -> io::Result<()> {
        // Pre-stamp the checkpoint LSN into the file header *before* flushing
        // so that page 0 is included in the dirty-page snapshot and gets
        // written + marked clean in the same pass.
        let checkpoint_lsn = self.checkpoint.wal_lsn();
        let page_count = self.page_storage.page_count();
        let free_list_head = self.free_list.lock().await.head();
        self.update_file_header(|fh| {
            fh.checkpoint_lsn = U64::new(checkpoint_lsn);
            fh.page_count = U64::new(page_count);
            fh.free_list_head = U32::new(free_list_head);
        })
        .await?;
        self.checkpoint.run().await?;
        self.write_file_header_shadow().await?;
        if self.is_durable {
            self.wal_storage
                .truncate_before(self.wal_reclamation_lsn(checkpoint_lsn))
                .await?;
        }
        Ok(())
    }

    /// Set the oldest WAL LSN still required by replication.
    ///
    /// During checkpoint, durable WAL reclamation uses
    /// `min(checkpoint_lsn, replication_retention_lsn)` so a lagging replica can
    /// catch up from retained WAL. Passing `None` restores single-node
    /// retention behavior.
    pub fn set_replication_retention_lsn(&self, min_required_lsn: Option<Lsn>) {
        self.replication_retention_lsn.store(
            min_required_lsn.unwrap_or(NO_REPLICATION_RETENTION_LSN),
            Ordering::Release,
        );
    }

    /// Return the current replication WAL-retention lower bound, if any.
    pub fn replication_retention_lsn(&self) -> Option<Lsn> {
        let lsn = self.replication_retention_lsn.load(Ordering::Acquire);
        if lsn == NO_REPLICATION_RETENTION_LSN {
            None
        } else {
            Some(lsn)
        }
    }

    fn wal_reclamation_lsn(&self, checkpoint_lsn: Lsn) -> Lsn {
        let progress_bound = self
            .replication_retention_lsn()
            .map(|replication_lsn| checkpoint_lsn.min(replication_lsn))
            .unwrap_or(checkpoint_lsn);
        self.retention_limit_reclamation_lsn(progress_bound)
    }

    fn retention_limit_reclamation_lsn(&self, progress_bound: Lsn) -> Lsn {
        let mut reclamation_lsn = progress_bound;

        if let Some(max_size) = self.config.wal_retention_max_size {
            let retained_size = self.wal_storage.retained_size();
            if retained_size > max_size {
                let size_bound = self.wal_storage.size().saturating_sub(max_size);
                reclamation_lsn = reclamation_lsn.max(size_bound);
            }
        }

        if let Some(max_age) = self.config.wal_retention_max_age
            && let Some(age_bound) = self.wal_storage.reclamation_lsn_for_retention_age(max_age)
        {
            reclamation_lsn = reclamation_lsn.max(age_bound);
        }

        reclamation_lsn
    }

    /// Vacuum entries from B-trees. Returns the number of entries removed.
    pub async fn vacuum(&self, entries: &[VacuumEntry]) -> io::Result<usize> {
        let mut free_list = self.free_list.lock().await;
        self.vacuum_task
            .remove_entries(entries, &mut free_list)
            .await
    }

    /// Run a structural integrity check over storage pages and the free list.
    ///
    /// This is the quick/medium integrity pass from the design: it validates the
    /// file header, page headers, page checksums, slot-directory bounds, and free
    /// list reachability. Full B-tree graph validation and repair are deliberately
    /// separate future phases.
    pub async fn check_integrity(&self) -> io::Result<IntegrityReport> {
        self.check_integrity_with_btree_roots(&[]).await
    }

    /// Run a conservative repair pass over integrity findings.
    ///
    /// This repairs only conditions that can be corrected from authoritative
    /// local metadata without data loss. If page 0 is stale relative to the
    /// in-memory header, or the trailing shadow is stale, corrupt, or missing,
    /// the current verified header is flushed to both copies and the storage
    /// file is synced. If a durable page store has extra trailing bytes beyond
    /// the live page count, those surplus bytes are truncated. Broader page,
    /// free-list, B-tree, heap, and catalog repair remains a higher-level/manual
    /// operation.
    pub async fn repair_integrity(&self) -> io::Result<IntegrityRepairReport> {
        let before = self.check_integrity().await?;
        let mut repairs = Vec::new();

        self.repair_physical_integrity(&before, &mut repairs)
            .await?;

        let after = self.check_integrity().await?;
        Ok(IntegrityRepairReport {
            repairs,
            remaining_issues: after.issues,
        })
    }

    /// Run a conservative repair pass over integrity findings that require
    /// caller-supplied B-tree roots.
    ///
    /// In addition to the quick physical repairs performed by
    /// [`repair_integrity`], this can rebuild broken B-tree leaf sibling chains
    /// from the set of leaves reachable from each supplied root. It only updates
    /// sibling pointers when the B-tree's structural pages and key ranges are
    /// otherwise valid.
    pub async fn repair_integrity_with_btree_roots(
        &self,
        btree_roots: &[IntegrityBTreeRoot],
    ) -> io::Result<IntegrityRepairReport> {
        let before = self.check_integrity_with_btree_roots(btree_roots).await?;
        let mut repairs = Vec::new();

        self.repair_physical_integrity(&before, &mut repairs)
            .await?;
        self.repair_btree_leaf_sibling_chains(btree_roots, &before, &mut repairs)
            .await?;

        let after = self.check_integrity_with_btree_roots(btree_roots).await?;
        Ok(IntegrityRepairReport {
            repairs,
            remaining_issues: after.issues,
        })
    }

    async fn repair_physical_integrity(
        &self,
        before: &IntegrityReport,
        repairs: &mut Vec<IntegrityRepair>,
    ) -> io::Result<()> {
        let repaired_header_metadata = self.is_durable
            && self
                .repair_file_header_metadata_mismatch(before, repairs)
                .await?;

        if self.is_durable
            && !repaired_header_metadata
            && Self::has_file_header_shadow_issue(before)
        {
            let fh = self.file_header().await;
            fh.verify()?;
            self.write_file_header_to_page0().await?;
            repairs.push(IntegrityRepair {
                page_id: Some(0),
                message: "rewrote durable file header and trailing shadow from verified header"
                    .into(),
            });
        }

        if Self::has_free_list_integrity_issue(before) {
            self.repair_free_list_integrity(repairs).await?;
        }

        if self.is_durable {
            self.repair_trailing_data_file_bytes(repairs).await?;
        }

        Ok(())
    }

    async fn repair_file_header_metadata_mismatch(
        &self,
        before: &IntegrityReport,
        repairs: &mut Vec<IntegrityRepair>,
    ) -> io::Result<bool> {
        if !Self::has_file_header_metadata_mismatch(before) {
            return Ok(false);
        }

        self.sync_file_header().await?;
        repairs.push(IntegrityRepair {
            page_id: Some(0),
            message:
                "refreshed durable file header page_count/free_list_head metadata and trailing shadow"
                    .into(),
        });
        Ok(true)
    }

    fn has_file_header_metadata_mismatch(report: &IntegrityReport) -> bool {
        report.issues.iter().any(|issue| {
            let message = issue.message.as_str();
            message.contains("file header page_count")
                || message.contains("file header free_list_head")
        })
    }

    async fn repair_free_list_integrity(
        &self,
        repairs: &mut Vec<IntegrityRepair>,
    ) -> io::Result<()> {
        let rebuilt = {
            let mut free_list = self.free_list.lock().await;
            free_list.rebuild_from_existing_free_pages().await?
        };
        self.sync_file_header().await?;
        self.checkpoint().await?;
        repairs.push(IntegrityRepair {
            page_id: None,
            message: format!("rebuilt free-list chain from {rebuilt} physically Free page(s)"),
        });
        Ok(())
    }

    fn has_free_list_integrity_issue(report: &IntegrityReport) -> bool {
        report.issues.iter().any(|issue| {
            let message = issue.message.as_str();
            message.contains("free list page")
                || message.contains("free list cycle")
                || message.contains("invalid free list page wrapper")
        })
    }

    /// Run integrity checks and validate the named B-tree root pages.
    ///
    /// The caller supplies higher-level roots (catalog, primary, secondary)
    /// because the storage layer does not own catalog semantics.
    pub async fn check_integrity_with_btree_roots(
        &self,
        btree_roots: &[IntegrityBTreeRoot],
    ) -> io::Result<IntegrityReport> {
        // Bring dirty in-memory pages to the same checksum-stamped state that
        // checkpoint uses before copying pages to durable storage.
        let _ = self.buffer_pool.dirty_pages();

        let page_count = self.page_storage.page_count();
        let page_size = self.page_storage.page_size();
        let file_header = self.file_header().await;
        let current_free_head = self.free_list.lock().await.head();

        let mut report = IntegrityReport {
            issues: Vec::new(),
            stats: IntegrityStats {
                page_count,
                pages_scanned: 0,
                free_pages: 0,
                btree_pages: 0,
                heap_pages: 0,
                overflow_pages: 0,
                orphan_btree_pages: 0,
                orphan_heap_pages: 0,
                double_allocated_pages: 0,
                wal_records_scanned: 0,
                wal_bytes_scanned: 0,
                page_type_counts: BTreeMap::new(),
            },
        };

        if let Err(err) = file_header.verify() {
            report.push_error(None, format!("file header verification failed: {err}"));
        }

        if file_header.page_size.get() as usize != page_size {
            report.push_error(
                Some(0),
                format!(
                    "file header page_size {} does not match storage page_size {}",
                    file_header.page_size.get(),
                    page_size,
                ),
            );
        }

        if file_header.page_count.get() != page_count {
            report.push_warning(
                Some(0),
                format!(
                    "file header page_count {} does not match live storage page_count {}",
                    file_header.page_count.get(),
                    page_count,
                ),
            );
        }

        if file_header.free_list_head.get() != current_free_head {
            report.push_warning(
                Some(0),
                format!(
                    "file header free_list_head {} does not match live free_list head {}",
                    file_header.free_list_head.get(),
                    current_free_head,
                ),
            );
        }

        if self.is_durable {
            self.check_data_file_size_integrity(page_count, page_size, &mut report)?;
            self.check_shadow_header_integrity(&file_header, page_count, &mut report)
                .await?;
        }

        for page_id_u64 in 0..page_count {
            let page_id = match PageId::try_from(page_id_u64) {
                Ok(page_id) => page_id,
                Err(_) => {
                    report.push_error(None, format!("page id {page_id_u64} exceeds PageId range"));
                    continue;
                }
            };

            let guard = match self.buffer_pool.fetch_page_shared(page_id).await {
                Ok(guard) => guard,
                Err(err) => {
                    report.push_error(Some(page_id), format!("failed to read page: {err}"));
                    continue;
                }
            };
            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(page) => page,
                Err(err) => {
                    report.push_error(Some(page_id), format!("invalid page wrapper: {err}"));
                    continue;
                }
            };

            report.stats.pages_scanned += 1;
            Self::check_page_integrity(page_id, page, page_size, &mut report);
        }

        let free_pages = self
            .check_free_list_integrity(current_free_head, page_count, &mut report)
            .await?;

        if !btree_roots.is_empty() {
            self.check_btree_roots_integrity(btree_roots, page_count, &free_pages, &mut report)
                .await?;
        }

        self.check_wal_integrity(&mut report).await?;

        Ok(report)
    }

    fn check_data_file_size_integrity(
        &self,
        page_count: u64,
        page_size: usize,
        report: &mut IntegrityReport,
    ) -> io::Result<()> {
        let Some(actual_size) = self.page_storage.physical_size_bytes()? else {
            return Ok(());
        };
        let Some(expected_size) = page_count.checked_mul(page_size as u64) else {
            report.push_error(
                None,
                format!(
                    "data file expected size overflows u64 for page_count {page_count} and page_size {page_size}"
                ),
            );
            return Ok(());
        };

        if actual_size != expected_size {
            report.push_error(
                None,
                format!(
                    "data file size {actual_size} does not match live page_count {page_count} * page_size {page_size} ({expected_size})"
                ),
            );
        }

        Ok(())
    }

    async fn check_wal_integrity(&self, report: &mut IntegrityReport) -> io::Result<()> {
        let end_lsn = self.wal_writer.current_lsn();
        let Some(mut current_lsn) = self.wal_storage.oldest_lsn() else {
            if end_lsn != 0 {
                report.push_error(
                    None,
                    format!(
                        "WAL has logical end LSN {end_lsn} but no retained segment is available"
                    ),
                );
            }
            return Ok(());
        };

        if current_lsn > end_lsn {
            report.push_error(
                None,
                format!(
                    "oldest retained WAL LSN {current_lsn} is beyond logical WAL end {end_lsn}"
                ),
            );
            return Ok(());
        }

        while current_lsn < end_lsn {
            let mut header = [0u8; WAL_FRAME_HEADER_SIZE];
            let header_len = self.wal_storage.read_from(current_lsn, &mut header).await?;
            if header_len < WAL_FRAME_HEADER_SIZE {
                report.push_error(
                    None,
                    format!(
                        "truncated WAL frame header at LSN {current_lsn}: read {header_len} of {WAL_FRAME_HEADER_SIZE} bytes before logical end {end_lsn}"
                    ),
                );
                break;
            }

            let payload_len =
                u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let stored_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            let record_type = header[8];

            if payload_len == 0 {
                report.push_error(
                    None,
                    format!("zero-length WAL record before logical end at LSN {current_lsn}"),
                );
                break;
            }

            if payload_len > MAX_WAL_RECORD_SIZE {
                report.push_error(
                    None,
                    format!(
                        "WAL record payload length {payload_len} at LSN {current_lsn} exceeds maximum {MAX_WAL_RECORD_SIZE}"
                    ),
                );
                break;
            }

            let mut payload = vec![0u8; payload_len];
            let payload_read = self
                .wal_storage
                .read_from(
                    current_lsn + WAL_FRAME_HEADER_SIZE as u64,
                    payload.as_mut_slice(),
                )
                .await?;
            if payload_read < payload_len {
                report.push_error(
                    None,
                    format!(
                        "truncated WAL payload at LSN {current_lsn}: read {payload_read} of {payload_len} bytes before logical end {end_lsn}"
                    ),
                );
                break;
            }

            let computed_crc = compute_wal_crc(record_type, &payload);
            if computed_crc != stored_crc {
                report.push_error(
                    None,
                    format!(
                        "WAL record CRC mismatch at LSN {current_lsn}: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
                    ),
                );
                break;
            }

            let frame_len = (WAL_FRAME_HEADER_SIZE + payload_len) as u64;
            report.stats.wal_records_scanned += 1;
            report.stats.wal_bytes_scanned += frame_len;
            current_lsn += frame_len;
        }

        Ok(())
    }

    fn has_file_header_shadow_issue(report: &IntegrityReport) -> bool {
        report
            .issues
            .iter()
            .any(|issue| issue.message.contains("file-header shadow"))
    }

    async fn repair_trailing_data_file_bytes(
        &self,
        repairs: &mut Vec<IntegrityRepair>,
    ) -> io::Result<()> {
        let page_count = self.page_storage.page_count();
        let page_size = self.page_storage.page_size();
        let Some(actual_size) = self.page_storage.physical_size_bytes()? else {
            return Ok(());
        };
        let Some(expected_size) = page_count.checked_mul(page_size as u64) else {
            return Ok(());
        };

        if actual_size <= expected_size {
            return Ok(());
        }

        self.page_storage
            .truncate_to_size_bytes(expected_size)
            .await?;
        repairs.push(IntegrityRepair {
            page_id: None,
            message: format!(
                "truncated {trailing_bytes} trailing data-file byte(s) beyond live page_count {page_count}",
                trailing_bytes = actual_size - expected_size,
            ),
        });
        Ok(())
    }

    async fn repair_btree_leaf_sibling_chains(
        &self,
        btree_roots: &[IntegrityBTreeRoot],
        before: &IntegrityReport,
        repairs: &mut Vec<IntegrityRepair>,
    ) -> io::Result<()> {
        let mut repaired_pages = 0usize;
        let page_count = self.page_storage.page_count();

        for root in btree_roots {
            if !Self::has_repairable_btree_leaf_sibling_issue(before, &root.name)
                || Self::has_non_sibling_btree_error(before, &root.name)
            {
                continue;
            }

            let mut scratch = Self::empty_integrity_report(page_count);
            let (_, leaves) = self
                .collect_single_btree_integrity(root, page_count, &mut scratch)
                .await?;
            if scratch.has_errors() {
                continue;
            }

            let Some(ordered_leaf_ids) = Self::ordered_btree_leaf_ids_by_key(&leaves) else {
                continue;
            };
            for (idx, page_id) in ordered_leaf_ids.iter().enumerate() {
                let expected = ordered_leaf_ids.get(idx + 1).copied().unwrap_or(0);
                let actual = leaves
                    .get(page_id)
                    .map(|leaf| leaf.right_sibling)
                    .unwrap_or(0);
                if actual == expected {
                    continue;
                }

                let mut guard = self.buffer_pool.fetch_page_exclusive(*page_id).await?;
                let mut page = SlottedPage::from_buf(guard.data_mut())?;
                if page.try_page_type() != Some(PageType::BTreeLeaf) {
                    continue;
                }
                page.set_prev_or_ptr(expected);
                page.stamp_checksum();
                repaired_pages += 1;
                repairs.push(IntegrityRepair {
                    page_id: Some(*page_id),
                    message: format!(
                        "rewrote B-tree '{}' leaf right_sibling from {actual} to {expected}",
                        root.name,
                    ),
                });
            }
        }

        if repaired_pages > 0 {
            self.checkpoint().await?;
        }

        Ok(())
    }

    fn has_repairable_btree_leaf_sibling_issue(report: &IntegrityReport, tree_name: &str) -> bool {
        report.issues.iter().any(|issue| {
            issue.severity == IntegritySeverity::Error
                && Self::is_btree_issue_for_tree(issue, tree_name)
                && Self::is_btree_leaf_sibling_issue(issue)
        })
    }

    fn has_non_sibling_btree_error(report: &IntegrityReport, tree_name: &str) -> bool {
        report.issues.iter().any(|issue| {
            issue.severity == IntegritySeverity::Error
                && Self::is_btree_issue_for_tree(issue, tree_name)
                && !Self::is_btree_leaf_sibling_issue(issue)
        })
    }

    fn is_btree_issue_for_tree(issue: &IntegrityIssue, tree_name: &str) -> bool {
        issue.message.starts_with(&format!("B-tree '{tree_name}'"))
    }

    fn is_btree_leaf_sibling_issue(issue: &IntegrityIssue) -> bool {
        let message = issue.message.as_str();
        message.contains("leaf right_sibling")
            || message.contains("leaf sibling keys")
            || message.contains("leaf sibling chain")
    }

    fn empty_integrity_report(page_count: u64) -> IntegrityReport {
        IntegrityReport {
            issues: Vec::new(),
            stats: IntegrityStats {
                page_count,
                pages_scanned: 0,
                free_pages: 0,
                btree_pages: 0,
                heap_pages: 0,
                overflow_pages: 0,
                orphan_btree_pages: 0,
                orphan_heap_pages: 0,
                double_allocated_pages: 0,
                wal_records_scanned: 0,
                wal_bytes_scanned: 0,
                page_type_counts: BTreeMap::new(),
            },
        }
    }

    async fn check_shadow_header_integrity(
        &self,
        primary_header: &FileHeader,
        page_count: u64,
        report: &mut IntegrityReport,
    ) -> io::Result<()> {
        if page_count < 2 {
            report.push_error(None, "durable storage is missing file-header shadow page");
            return Ok(());
        }

        let shadow_page_id = page_id_from_u64(page_count - 1)?;
        let guard = match self.buffer_pool.fetch_page_shared(shadow_page_id).await {
            Ok(guard) => guard,
            Err(err) => {
                report.push_error(
                    Some(shadow_page_id),
                    format!("failed to read file-header shadow page: {err}"),
                );
                return Ok(());
            }
        };

        match read_verified_file_header_page(
            guard.data(),
            shadow_page_id,
            PageType::FileHeaderShadow,
        ) {
            Ok(shadow_header) => {
                if shadow_header.as_bytes() != primary_header.as_bytes() {
                    report.push_warning(
                        Some(shadow_page_id),
                        "file-header shadow does not match primary file header",
                    );
                }
            }
            Err(err) => report.push_error(
                Some(shadow_page_id),
                format!("file-header shadow verification failed: {err}"),
            ),
        }

        Ok(())
    }

    // ─── Accessors (for integration layer) ───

    /// Return a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Return a copy of the current file header.
    pub async fn file_header(&self) -> FileHeader {
        self.file_header.lock().await.clone()
    }

    /// Highest primary-source WAL LSN durably applied by this replica.
    pub async fn replication_applied_lsn(&self) -> Lsn {
        self.file_header.lock().await.replication_applied_lsn.get()
    }

    /// Update the file header via a closure, then write page 0 through
    /// the buffer pool.
    pub async fn update_file_header<F>(&self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        let mut fh = self.file_header.lock().await;
        f(&mut fh);
        // Write updated header to page 0 through buffer pool.
        let mut guard = self.buffer_pool.fetch_page_exclusive(0).await?;
        let buf = guard.data_mut();
        write_file_header(buf, &fh);
        // guard.mark_dirty() is called implicitly by data_mut()
        Ok(())
    }

    /// Refresh volatile header metadata and persist the primary/shadow headers.
    ///
    /// This is lighter than a checkpoint: it does not flush every dirty page or
    /// reclaim WAL, but it keeps `page_count`, `free_list_head`, and the durable
    /// file-header shadow coherent after page allocation-heavy maintenance paths.
    pub async fn sync_file_header(&self) -> io::Result<()> {
        let free_list_head = self.free_list.lock().await.head();
        let page_count = self.page_storage.page_count();
        self.update_file_header(|fh| {
            fh.free_list_head = U32::new(free_list_head);
            fh.page_count = U64::new(page_count);
        })
        .await?;
        self.write_file_header_to_page0().await?;
        self.page_storage.sync().await?;
        Ok(())
    }

    /// Return a reference to the WAL writer.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal_writer
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Return a reference to the free list.
    pub fn free_list(&self) -> &Arc<Mutex<FreeList>> {
        &self.free_list
    }

    fn check_page_integrity(
        expected_page_id: PageId,
        page: SlottedPageRef<'_>,
        page_size: usize,
        report: &mut IntegrityReport,
    ) {
        let header = page.header();
        let actual_page_id = header.page_id.get();
        if actual_page_id != expected_page_id {
            report.push_error(
                Some(expected_page_id),
                format!(
                    "page header id {} does not match physical page id {}",
                    actual_page_id, expected_page_id,
                ),
            );
        }

        match page.try_page_type() {
            Some(page_type) => {
                *report
                    .stats
                    .page_type_counts
                    .entry(Self::page_type_name(page_type).to_string())
                    .or_insert(0) += 1;
            }
            None => {
                report.push_error(
                    Some(expected_page_id),
                    format!("invalid page type byte {:#04x}", header.page_type),
                );
            }
        }

        if !page.verify_checksum() {
            report.push_error(Some(expected_page_id), "page checksum mismatch");
        }

        let free_space_start = header.free_space_start.get() as usize;
        let free_space_end = header.free_space_end.get() as usize;
        let num_slots = header.num_slots.get() as usize;
        let slot_dir_end =
            PAGE_HEADER_SIZE.saturating_add(num_slots.saturating_mul(SLOT_ENTRY_SIZE));

        if free_space_start < PAGE_HEADER_SIZE {
            report.push_error(
                Some(expected_page_id),
                format!(
                    "free_space_start {} is before page header end {}",
                    free_space_start, PAGE_HEADER_SIZE,
                ),
            );
        }
        if free_space_end > page_size {
            report.push_error(
                Some(expected_page_id),
                format!(
                    "free_space_end {} exceeds page_size {}",
                    free_space_end, page_size,
                ),
            );
        }
        if free_space_start > free_space_end {
            report.push_error(
                Some(expected_page_id),
                format!(
                    "free_space_start {} is after free_space_end {}",
                    free_space_start, free_space_end,
                ),
            );
        }
        if slot_dir_end > free_space_start {
            report.push_error(
                Some(expected_page_id),
                format!(
                    "slot directory end {} exceeds free_space_start {}",
                    slot_dir_end, free_space_start,
                ),
            );
        }

        let mut live_ranges: Vec<(usize, usize)> = Vec::new();
        for slot in 0..num_slots {
            let dir_offset = PAGE_HEADER_SIZE + slot * SLOT_ENTRY_SIZE;
            if dir_offset + SLOT_ENTRY_SIZE > page_size {
                report.push_error(
                    Some(expected_page_id),
                    format!("slot {slot} directory entry exceeds page bounds"),
                );
                break;
            }

            let entry = page.slot_entry(slot as u16);
            if entry.length == 0 {
                continue;
            }

            let offset = entry.offset as usize;
            let end = offset + entry.length as usize;
            if offset < free_space_end || end > page_size {
                report.push_error(
                    Some(expected_page_id),
                    format!(
                        "slot {slot} data range {}..{} is outside cell area {}..{}",
                        offset, end, free_space_end, page_size,
                    ),
                );
            }
            live_ranges.push((offset, end));
        }

        live_ranges.sort_unstable();
        for window in live_ranges.windows(2) {
            if window[0].1 > window[1].0 {
                report.push_error(
                    Some(expected_page_id),
                    format!(
                        "slot data ranges overlap: {}..{} and {}..{}",
                        window[0].0, window[0].1, window[1].0, window[1].1,
                    ),
                );
            }
        }
    }

    async fn check_free_list_integrity(
        &self,
        free_head: PageId,
        page_count: u64,
        report: &mut IntegrityReport,
    ) -> io::Result<BTreeSet<PageId>> {
        let mut seen = BTreeSet::new();
        let mut current = free_head;

        while current != 0 {
            if current as u64 >= page_count {
                report.push_error(
                    Some(current),
                    format!(
                        "free list page {} is outside page_count {}",
                        current, page_count,
                    ),
                );
                break;
            }

            if !seen.insert(current) {
                report.push_error(Some(current), "free list cycle or duplicate page detected");
                break;
            }

            let guard = match self.buffer_pool.fetch_page_shared(current).await {
                Ok(guard) => guard,
                Err(err) => {
                    report.push_error(
                        Some(current),
                        format!("failed to read free list page: {err}"),
                    );
                    break;
                }
            };
            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(page) => page,
                Err(err) => {
                    report.push_error(
                        Some(current),
                        format!("invalid free list page wrapper: {err}"),
                    );
                    break;
                }
            };

            if page.try_page_type() != Some(PageType::Free) {
                report.push_error(
                    Some(current),
                    format!(
                        "free list page has type {:?}, expected Free",
                        page.try_page_type(),
                    ),
                );
                break;
            }

            report.stats.free_pages += 1;
            current = page.prev_or_ptr();
        }

        Ok(seen)
    }

    async fn check_btree_roots_integrity(
        &self,
        roots: &[IntegrityBTreeRoot],
        page_count: u64,
        free_pages: &BTreeSet<PageId>,
        report: &mut IntegrityReport,
    ) -> io::Result<()> {
        let mut allocated_btree_pages: BTreeMap<PageId, String> = BTreeMap::new();

        for root in roots {
            let reachable = self
                .check_single_btree_integrity(root, page_count, report)
                .await?;
            for page_id in reachable {
                if let Some(first_owner) = allocated_btree_pages.insert(page_id, root.name.clone())
                {
                    report.stats.double_allocated_pages += 1;
                    report.push_error(
                        Some(page_id),
                        format!(
                            "B-tree page is reachable from both '{}' and '{}'",
                            first_owner, root.name,
                        ),
                    );
                }
            }
        }

        report.stats.btree_pages = allocated_btree_pages.len() as u64;

        for page_id in allocated_btree_pages.keys() {
            if free_pages.contains(page_id) {
                report.stats.double_allocated_pages += 1;
                report.push_error(
                    Some(*page_id),
                    "B-tree page is also present in the free list",
                );
            }
        }

        for page_id_u64 in 1..page_count {
            let Ok(page_id) = PageId::try_from(page_id_u64) else {
                continue;
            };
            if allocated_btree_pages.contains_key(&page_id) || free_pages.contains(&page_id) {
                continue;
            }

            let guard = match self.buffer_pool.fetch_page_shared(page_id).await {
                Ok(guard) => guard,
                Err(err) => {
                    report.push_error(
                        Some(page_id),
                        format!("failed to read page during orphan scan: {err}"),
                    );
                    continue;
                }
            };
            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(page) => page,
                Err(err) => {
                    report.push_error(
                        Some(page_id),
                        format!("invalid page wrapper during orphan scan: {err}"),
                    );
                    continue;
                }
            };

            if matches!(
                page.try_page_type(),
                Some(PageType::BTreeInternal) | Some(PageType::BTreeLeaf)
            ) {
                report.stats.orphan_btree_pages += 1;
                report.push_warning(
                    Some(page_id),
                    "B-tree page is not reachable from any supplied B-tree root and is not free",
                );
            }
        }

        Ok(())
    }

    async fn check_single_btree_integrity(
        &self,
        root: &IntegrityBTreeRoot,
        page_count: u64,
        report: &mut IntegrityReport,
    ) -> io::Result<BTreeSet<PageId>> {
        let (seen, leaves) = self
            .collect_single_btree_integrity(root, page_count, report)
            .await?;
        Self::check_btree_leaf_siblings(&root.name, &leaves, report);
        Ok(seen)
    }

    async fn collect_single_btree_integrity(
        &self,
        root: &IntegrityBTreeRoot,
        page_count: u64,
        report: &mut IntegrityReport,
    ) -> io::Result<(BTreeSet<PageId>, BTreeMap<PageId, BTreeLeafSummary>)> {
        if root.root_page == 0 {
            report.push_error(None, format!("B-tree '{}' has root page 0", root.name));
            return Ok((BTreeSet::new(), BTreeMap::new()));
        }
        if root.root_page as u64 >= page_count {
            report.push_error(
                Some(root.root_page),
                format!(
                    "B-tree '{}' root page is outside page_count {}",
                    root.name, page_count,
                ),
            );
            return Ok((BTreeSet::new(), BTreeMap::new()));
        }

        let mut seen = BTreeSet::new();
        let mut stack = vec![root.root_page];
        let mut leaves = BTreeMap::new();

        while let Some(page_id) = stack.pop() {
            if page_id == 0 || page_id as u64 >= page_count {
                report.push_error(
                    Some(page_id),
                    format!("B-tree '{}' references out-of-range page", root.name),
                );
                continue;
            }

            if !seen.insert(page_id) {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' contains a cycle or duplicate child reference",
                        root.name
                    ),
                );
                continue;
            }

            let guard = match self.buffer_pool.fetch_page_shared(page_id).await {
                Ok(guard) => guard,
                Err(err) => {
                    report.push_error(
                        Some(page_id),
                        format!("failed to read B-tree '{}' page: {err}", root.name),
                    );
                    continue;
                }
            };
            let page = match SlottedPageRef::from_buf(guard.data()) {
                Ok(page) => page,
                Err(err) => {
                    report.push_error(
                        Some(page_id),
                        format!("invalid B-tree '{}' page wrapper: {err}", root.name),
                    );
                    continue;
                }
            };

            match page.try_page_type() {
                Some(PageType::BTreeLeaf) => {
                    if let Some(summary) =
                        Self::check_btree_leaf_page(&root.name, page_id, page, guard.data(), report)
                    {
                        leaves.insert(page_id, summary);
                    }
                }
                Some(PageType::BTreeInternal) => {
                    for child in Self::check_btree_internal_page(
                        &root.name,
                        page_id,
                        page,
                        guard.data(),
                        page_count,
                        report,
                    ) {
                        stack.push(child);
                    }
                }
                Some(other) => {
                    report.push_error(
                        Some(page_id),
                        format!(
                            "B-tree '{}' page has type {:?}, expected BTreeLeaf or BTreeInternal",
                            root.name, other,
                        ),
                    );
                }
                None => {
                    report.push_error(
                        Some(page_id),
                        format!("B-tree '{}' page has invalid page type", root.name),
                    );
                }
            }
        }

        Ok((seen, leaves))
    }

    fn check_btree_leaf_page(
        tree_name: &str,
        page_id: PageId,
        page: SlottedPageRef<'_>,
        data: &[u8],
        report: &mut IntegrityReport,
    ) -> Option<BTreeLeafSummary> {
        let mut first_key = None;
        let mut last_key = None;
        let mut previous_key: Option<Vec<u8>> = None;

        for slot in 0..page.num_slots() {
            let Some(cell) = Self::btree_slot_cell(tree_name, page_id, &page, data, slot, report)
            else {
                continue;
            };

            if cell.len() < 2 {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' leaf slot {} is too short for key length",
                        tree_name, slot,
                    ),
                );
                continue;
            }

            let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
            if key_len > cell.len().saturating_sub(2) {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' leaf slot {} key length {} exceeds cell length {}",
                        tree_name,
                        slot,
                        key_len,
                        cell.len(),
                    ),
                );
                continue;
            }

            let key = &cell[2..2 + key_len];
            if let Some(previous) = previous_key.as_deref()
                && previous >= key
            {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' leaf keys are not strictly ascending at slot {}",
                        tree_name, slot,
                    ),
                );
            }

            if first_key.is_none() {
                first_key = Some(key.to_vec());
            }
            last_key = Some(key.to_vec());
            previous_key = Some(key.to_vec());
        }

        Some(BTreeLeafSummary {
            first_key,
            last_key,
            right_sibling: page.prev_or_ptr(),
        })
    }

    fn check_btree_internal_page(
        tree_name: &str,
        page_id: PageId,
        page: SlottedPageRef<'_>,
        data: &[u8],
        page_count: u64,
        report: &mut IntegrityReport,
    ) -> Vec<PageId> {
        let mut children = Vec::with_capacity(page.num_slots() as usize + 1);
        let leftmost_child = page.prev_or_ptr();
        if leftmost_child == 0 || leftmost_child as u64 >= page_count {
            report.push_error(
                Some(page_id),
                format!(
                    "B-tree '{}' internal page has invalid leftmost child {}",
                    tree_name, leftmost_child,
                ),
            );
        } else {
            children.push(leftmost_child);
        }

        let mut previous_key: Option<Vec<u8>> = None;
        for slot in 0..page.num_slots() {
            let Some(cell) = Self::btree_slot_cell(tree_name, page_id, &page, data, slot, report)
            else {
                continue;
            };

            if cell.len() < 6 {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' internal slot {} is too short for key and child",
                        tree_name, slot,
                    ),
                );
                continue;
            }

            let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
            if key_len > cell.len().saturating_sub(6) {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' internal slot {} key length {} exceeds cell length {}",
                        tree_name,
                        slot,
                        key_len,
                        cell.len(),
                    ),
                );
                continue;
            }

            let key = &cell[2..2 + key_len];
            if let Some(previous) = previous_key.as_deref()
                && previous >= key
            {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' internal keys are not strictly ascending at slot {}",
                        tree_name, slot,
                    ),
                );
            }
            previous_key = Some(key.to_vec());

            let child_offset = 2 + key_len;
            let child = u32::from_le_bytes([
                cell[child_offset],
                cell[child_offset + 1],
                cell[child_offset + 2],
                cell[child_offset + 3],
            ]);
            if child == 0 || child as u64 >= page_count {
                report.push_error(
                    Some(page_id),
                    format!(
                        "B-tree '{}' internal slot {} references invalid child {}",
                        tree_name, slot, child,
                    ),
                );
            } else {
                children.push(child);
            }
        }

        children
    }

    fn btree_slot_cell<'a>(
        tree_name: &str,
        page_id: PageId,
        page: &SlottedPageRef<'_>,
        data: &'a [u8],
        slot: u16,
        report: &mut IntegrityReport,
    ) -> Option<&'a [u8]> {
        let entry = page.slot_entry(slot);
        if entry.length == 0 {
            report.push_error(
                Some(page_id),
                format!(
                    "B-tree '{}' slot {} is an unexpected tombstone",
                    tree_name, slot
                ),
            );
            return None;
        }

        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end > data.len() {
            report.push_error(
                Some(page_id),
                format!(
                    "B-tree '{}' slot {} data range {}..{} exceeds page size {}",
                    tree_name,
                    slot,
                    start,
                    end,
                    data.len(),
                ),
            );
            return None;
        }

        Some(&data[start..end])
    }

    fn check_btree_leaf_siblings(
        tree_name: &str,
        leaves: &BTreeMap<PageId, BTreeLeafSummary>,
        report: &mut IntegrityReport,
    ) {
        if let Some(ordered_leaf_ids) = Self::ordered_btree_leaf_ids_by_key(leaves) {
            for (idx, page_id) in ordered_leaf_ids.iter().enumerate() {
                let expected = ordered_leaf_ids.get(idx + 1).copied().unwrap_or(0);
                let actual = leaves
                    .get(page_id)
                    .map(|leaf| leaf.right_sibling)
                    .unwrap_or(0);
                if actual != expected {
                    report.push_error(
                        Some(*page_id),
                        format!(
                            "B-tree '{}' leaf right_sibling {actual} does not match expected next leaf {expected}",
                            tree_name,
                        ),
                    );
                }
            }
        }

        for (page_id, leaf) in leaves {
            let sibling = leaf.right_sibling;
            if sibling == 0 {
                continue;
            }
            if sibling == *page_id {
                report.push_error(
                    Some(*page_id),
                    format!("B-tree '{}' leaf right_sibling points to itself", tree_name),
                );
                continue;
            }

            let Some(sibling_leaf) = leaves.get(&sibling) else {
                report.push_error(
                    Some(*page_id),
                    format!(
                        "B-tree '{}' leaf right_sibling {} is not reachable from the root",
                        tree_name, sibling,
                    ),
                );
                continue;
            };

            if let (Some(left_last), Some(right_first)) =
                (leaf.last_key.as_deref(), sibling_leaf.first_key.as_deref())
                && left_last >= right_first
            {
                report.push_error(
                    Some(*page_id),
                    format!(
                        "B-tree '{}' leaf sibling keys are not ascending into page {}",
                        tree_name, sibling,
                    ),
                );
            }
        }

        for start in leaves.keys() {
            let mut chain_seen = BTreeSet::new();
            let mut current = *start;
            while current != 0 {
                if !chain_seen.insert(current) {
                    report.push_error(
                        Some(current),
                        format!("B-tree '{}' leaf sibling chain contains a cycle", tree_name),
                    );
                    break;
                }
                let Some(leaf) = leaves.get(&current) else {
                    break;
                };
                current = leaf.right_sibling;
            }
        }
    }

    fn ordered_btree_leaf_ids_by_key(
        leaves: &BTreeMap<PageId, BTreeLeafSummary>,
    ) -> Option<Vec<PageId>> {
        if leaves.len() <= 1 {
            return Some(leaves.keys().copied().collect());
        }

        let mut ordered = Vec::with_capacity(leaves.len());
        for (page_id, leaf) in leaves {
            ordered.push((
                *page_id,
                leaf.first_key.as_ref()?.clone(),
                leaf.last_key.as_ref()?.clone(),
            ));
        }
        ordered.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));

        for pair in ordered.windows(2) {
            if pair[0].2 >= pair[1].1 {
                return None;
            }
        }

        Some(ordered.into_iter().map(|(page_id, _, _)| page_id).collect())
    }

    fn page_type_name(page_type: PageType) -> &'static str {
        match page_type {
            PageType::BTreeInternal => "btree_internal",
            PageType::BTreeLeaf => "btree_leaf",
            PageType::Heap => "heap",
            PageType::Overflow => "overflow",
            PageType::Free => "free",
            PageType::FileHeader => "file_header",
            PageType::FileHeaderShadow => "file_header_shadow",
        }
    }

    // ─── Internal: Initialize new database ───

    /// Initialize a brand-new database: extend to page 0, write file header,
    /// create catalog B-trees.
    async fn init_new_database(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        config: StorageConfig,
        is_durable: bool,
        dwb_path: Option<PathBuf>,
        db_path: Option<PathBuf>,
    ) -> io::Result<Self> {
        config.validate()?;
        let page_size = config.page_size;
        let frame_count = config.memory_budget / page_size;

        // Extend page storage to have page 0. Durable engines also keep a
        // trailing page reserved for the file-header shadow copy.
        let initial_page_count = if is_durable { 2 } else { 1 };
        page_storage.extend(initial_page_count).await?;

        // Initialize page 0 as FileHeader page.
        {
            let mut buf = vec![0u8; page_size];
            SlottedPage::init(&mut buf, 0, PageType::FileHeader);
            let fh = FileHeader::new(page_size);
            write_file_header(&mut buf, &fh);
            page_storage.write_page(0, &buf).await?;
        }
        if is_durable {
            let fh = FileHeader::new(page_size);
            write_file_header_page_to_storage(
                page_storage.as_ref(),
                1,
                PageType::FileHeaderShadow,
                page_size,
                &fh,
            )
            .await?;
        }

        // Build buffer pool.
        let buffer_pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size,
                frame_count,
            },
            page_storage.clone(),
        ));

        // Build free list (empty).
        let free_list_state = if is_durable {
            FreeList::new_with_trailing_reservation(0, buffer_pool.clone())
        } else {
            FreeList::new(0, buffer_pool.clone())
        };
        let free_list = Arc::new(Mutex::new(
            free_list_state.with_disk_quota(config.max_disk_usage_bytes, Some(wal_storage.clone())),
        ));

        // Create catalog B-trees.
        let catalog_root_page;
        let catalog_name_root_page;
        {
            let mut fl = free_list.lock().await;
            let id_btree = BTree::create(buffer_pool.clone(), &mut fl).await?;
            catalog_root_page = id_btree.root_page();

            let name_btree = BTree::create(buffer_pool.clone(), &mut fl).await?;
            catalog_name_root_page = name_btree.root_page();
        }

        // Update file header with catalog root pages and current page count.
        let file_header = {
            let mut fh = FileHeader::new(page_size);
            fh.catalog_root_page = U32::new(catalog_root_page);
            fh.catalog_name_root_page = U32::new(catalog_name_root_page);
            fh.page_count = U64::new(page_storage.page_count());
            fh.free_list_head = U32::new(free_list.lock().await.head());
            fh
        };

        // Write updated header to page 0 through buffer pool.
        {
            let mut guard = buffer_pool.fetch_page_exclusive(0).await?;
            let buf = guard.data_mut();
            write_file_header(buf, &file_header);
        }

        // Flush all dirty pages (page 0 + catalog B-tree roots) so they're durable.
        for (page_id, _, _, _) in buffer_pool.dirty_pages() {
            buffer_pool.flush_page(page_id).await?;
        }
        if is_durable {
            let shadow_page_id = page_id_from_u64(file_header.page_count.get() - 1)?;
            write_file_header_page_to_storage(
                page_storage.as_ref(),
                shadow_page_id,
                PageType::FileHeaderShadow,
                page_size,
                &file_header,
            )
            .await?;
            page_storage.sync().await?;
        }

        // Build WAL writer and reader.
        let wal_writer = Arc::new(WalWriter::new(
            wal_storage.clone(),
            WalConfig {
                segment_size: config.wal_segment_size,
            },
        )?);
        let wal_reader = WalReader::new(wal_storage.clone());

        // Build heap (empty).
        let heap = Mutex::new(Heap::new(buffer_pool.clone()));

        // Build checkpoint.
        let dwb = dwb_path.map(|p| DoubleWriteBuffer::new(&p, page_storage.clone(), page_size));
        let checkpoint = Checkpoint::new(buffer_pool.clone(), dwb, wal_writer.clone(), is_durable);

        // Build vacuum.
        let vacuum_task = VacuumTask::new(buffer_pool.clone());

        Ok(StorageEngine {
            page_storage,
            wal_storage,
            buffer_pool,
            free_list,
            wal_writer,
            wal_reader,
            heap,
            checkpoint,
            vacuum_task,
            file_header: Mutex::new(file_header),
            config,
            is_durable,
            replication_retention_lsn: AtomicLsn::new(NO_REPLICATION_RETENTION_LSN),
            path: db_path,
        })
    }

    /// Build a StorageEngine from an existing database (after recovery).
    fn build_from_existing(
        page_storage: Arc<dyn PageStorage>,
        wal_storage: Arc<dyn WalStorage>,
        file_header: FileHeader,
        config: StorageConfig,
        is_durable: bool,
        dwb_path: Option<PathBuf>,
        db_path: Option<PathBuf>,
    ) -> io::Result<Self> {
        let page_size = config.page_size;
        let frame_count = config.memory_budget / page_size;

        // Build buffer pool.
        let buffer_pool = Arc::new(BufferPool::new(
            BufferPoolConfig {
                page_size,
                frame_count,
            },
            page_storage.clone(),
        ));

        // Build free list from saved head.
        let free_list_head = file_header.free_list_head.get();
        let free_list_state = if is_durable {
            FreeList::new_with_trailing_reservation(free_list_head, buffer_pool.clone())
        } else {
            FreeList::new(free_list_head, buffer_pool.clone())
        };
        let free_list = Arc::new(Mutex::new(
            free_list_state.with_disk_quota(config.max_disk_usage_bytes, Some(wal_storage.clone())),
        ));

        // Build WAL writer and reader.
        let wal_writer = Arc::new(WalWriter::new(
            wal_storage.clone(),
            WalConfig {
                segment_size: config.wal_segment_size,
            },
        )?);
        let wal_reader = WalReader::new(wal_storage.clone());

        // Build heap with empty free space map.  The map populates
        // incrementally as store()/free() are called — avoids an O(n_pages)
        // scan that would block startup on large databases.
        let heap = Heap::new(buffer_pool.clone());

        // Build checkpoint.
        let dwb = dwb_path.map(|p| DoubleWriteBuffer::new(&p, page_storage.clone(), page_size));
        let checkpoint = Checkpoint::new(buffer_pool.clone(), dwb, wal_writer.clone(), is_durable);

        // Build vacuum.
        let vacuum_task = VacuumTask::new(buffer_pool.clone());

        Ok(StorageEngine {
            page_storage,
            wal_storage,
            buffer_pool,
            free_list,
            wal_writer,
            wal_reader,
            heap: Mutex::new(heap),
            checkpoint,
            vacuum_task,
            file_header: Mutex::new(file_header),
            config,
            is_durable,
            replication_retention_lsn: AtomicLsn::new(NO_REPLICATION_RETENTION_LSN),
            path: db_path,
        })
    }

    /// Write the current file header to page 0 via the buffer pool and flush.
    async fn write_file_header_to_page0(&self) -> io::Result<()> {
        let fh = self.file_header.lock().await.clone();
        {
            let mut guard = self.buffer_pool.fetch_page_exclusive(0).await?;
            write_file_header(guard.data_mut(), &fh);
        }
        self.buffer_pool.flush_page(0).await?;
        self.write_file_header_shadow_with(&fh).await?;
        Ok(())
    }

    async fn write_file_header_shadow(&self) -> io::Result<()> {
        let fh = self.file_header.lock().await.clone();
        self.write_file_header_shadow_with(&fh).await
    }

    async fn write_file_header_shadow_with(&self, fh: &FileHeader) -> io::Result<()> {
        if !self.is_durable {
            return Ok(());
        }

        let page_count = self.page_storage.page_count();
        if page_count < 2 {
            return Err(crate::error::StorageError::Corruption(
                "durable storage has no room for file-header shadow page".into(),
            )
            .into());
        }

        let shadow_page_id = page_id_from_u64(page_count - 1)?;
        let wrote_to_buffer_pool = match self.buffer_pool.fetch_page_exclusive(shadow_page_id).await
        {
            Ok(mut guard) => {
                {
                    let buf = guard.data_mut();
                    SlottedPage::init(buf, shadow_page_id, PageType::FileHeaderShadow);
                    write_file_header(buf, fh);
                }
                true
            }
            Err(_) => false,
        };

        if wrote_to_buffer_pool {
            self.buffer_pool.flush_page(shadow_page_id).await?;
        } else {
            write_file_header_page_to_storage(
                self.page_storage.as_ref(),
                shadow_page_id,
                PageType::FileHeaderShadow,
                self.config.page_size,
                fh,
            )
            .await?;
        }
        self.page_storage.sync().await?;
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::NoOpHandler;
    use tokio_stream::StreamExt;

    // ─── Test 1: open + close file-backed ───

    #[tokio::test]
    async fn test_open_close_file_backed() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");

        // Open new database.
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        assert!(engine.is_durable());
        let fh = engine.file_header().await;
        assert_eq!(fh.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh.version.get(), FILE_HEADER_VERSION);
        assert_eq!(fh.page_size.get(), 4096);
        assert!(fh.catalog_root_page.get() > 0);
        assert!(fh.catalog_name_root_page.get() > 0);

        // Close.
        engine.close().await.unwrap();

        // Reopen.
        let config2 = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine2 = StorageEngine::open(&path, config2, &mut NoOpHandler)
            .await
            .unwrap();

        let fh2 = engine2.file_header().await;
        assert_eq!(fh2.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh2.catalog_root_page.get(), fh.catalog_root_page.get());
        assert_eq!(
            fh2.catalog_name_root_page.get(),
            fh.catalog_name_root_page.get()
        );

        engine2.close().await.unwrap();
    }

    #[tokio::test]
    async fn durable_engine_writes_matching_file_header_shadow() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let fh = engine.file_header().await;
        let page_count = engine.page_storage.page_count();
        assert!(page_count >= 2);
        assert_eq!(fh.page_count.get(), page_count);

        let shadow_page_id = page_id_from_u64(page_count - 1).unwrap();
        let mut buf = vec![0u8; engine.config.page_size];
        engine
            .page_storage
            .read_page(shadow_page_id, &mut buf)
            .await
            .unwrap();
        let shadow =
            read_verified_file_header_page(&buf, shadow_page_id, PageType::FileHeaderShadow)
                .unwrap();
        assert_eq!(shadow.as_bytes(), fh.as_bytes());

        let report = engine.check_integrity().await.unwrap();
        assert!(
            report.is_ok(),
            "unexpected integrity issues: {:?}",
            report.issues
        );

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_size_retention_cap_overrides_replica_progress_pin() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 256,
            wal_retention_max_size: Some(512),
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let payload = vec![0xAB; 96];
        for _ in 0..16 {
            engine.append_wal(0x01, &payload).await.unwrap();
        }
        let oldest_before = engine.wal_storage.oldest_lsn().unwrap();
        let retained_before = engine.wal_storage.retained_size();
        engine.set_replication_retention_lsn(Some(0));

        engine.checkpoint().await.unwrap();

        let oldest_after = engine.wal_storage.oldest_lsn().unwrap();
        let retained_after = engine.wal_storage.retained_size();
        assert_eq!(oldest_before, 0);
        assert!(retained_before > 512);
        assert!(
            oldest_after > 0,
            "size cap should reclaim sealed WAL segments even when replica progress is pinned at 0"
        );
        assert!(retained_after < retained_before);

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_age_retention_cap_overrides_replica_progress_pin() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 256,
            wal_retention_max_age: Some(Duration::ZERO),
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let payload = vec![0xCD; 96];
        for _ in 0..10 {
            engine.append_wal(0x01, &payload).await.unwrap();
        }
        let oldest_before = engine.wal_storage.oldest_lsn().unwrap();
        let retained_before = engine.wal_storage.retained_size();
        engine.set_replication_retention_lsn(Some(0));

        engine.checkpoint().await.unwrap();

        let oldest_after = engine.wal_storage.oldest_lsn().unwrap();
        let retained_after = engine.wal_storage.retained_size();
        assert_eq!(oldest_before, 0);
        assert!(
            oldest_after > 0,
            "age cap should reclaim sealed WAL segments even when replica progress is pinned at 0"
        );
        assert!(retained_after < retained_before);

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn sync_file_header_refreshes_page_count_and_shadow_after_allocation() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let _btree = engine.create_btree().await.unwrap();
        engine.sync_file_header().await.unwrap();

        let fh = engine.file_header().await;
        let page_count = engine.page_storage.page_count();
        assert_eq!(fh.page_count.get(), page_count);

        let shadow_page_id = page_id_from_u64(page_count - 1).unwrap();
        let mut buf = vec![0u8; engine.config.page_size];
        engine
            .page_storage
            .read_page(shadow_page_id, &mut buf)
            .await
            .unwrap();
        let shadow =
            read_verified_file_header_page(&buf, shadow_page_id, PageType::FileHeaderShadow)
                .unwrap();
        assert_eq!(shadow.as_bytes(), fh.as_bytes());

        let report = engine.check_integrity().await.unwrap();
        assert!(
            report.is_ok(),
            "unexpected integrity issues: {:?}",
            report.issues
        );

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn durable_open_restores_corrupt_primary_header_from_shadow() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        {
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();
            engine.close().await.unwrap();
        }

        let data_path = path.join("data.db");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xA5; 64]).unwrap();
        file.sync_all().unwrap();

        let reopen_config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let reopened = StorageEngine::open(&path, reopen_config, &mut NoOpHandler)
            .await
            .unwrap();
        let fh = reopened.file_header().await;
        assert_eq!(fh.magic.get(), FILE_HEADER_MAGIC);
        assert!(fh.catalog_root_page.get() > 0);
        assert!(fh.catalog_name_root_page.get() > 0);

        let report = reopened.check_integrity().await.unwrap();
        assert!(
            report.is_ok(),
            "unexpected integrity issues: {:?}",
            report.issues
        );

        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn durable_open_rejects_unsupported_file_header_version() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let mut header;
        {
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();
            header = engine.file_header().await;
            engine.close().await.unwrap();
        }

        header.version = U32::new(FILE_HEADER_VERSION + 1);
        let data_path = path.join("data.db");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        let primary = build_file_header_page(0, PageType::FileHeader, 4096, &header);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&primary).unwrap();

        let shadow_page_id = page_id_from_u64(header.page_count.get() - 1).unwrap();
        let shadow =
            build_file_header_page(shadow_page_id, PageType::FileHeaderShadow, 4096, &header);
        file.seek(SeekFrom::Start(shadow_page_id as u64 * 4096))
            .unwrap();
        file.write_all(&shadow).unwrap();
        file.sync_all().unwrap();

        let reopen_config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let err = match StorageEngine::open(&path, reopen_config, &mut NoOpHandler).await {
            Ok(engine) => {
                engine.close().await.unwrap();
                panic!("open unexpectedly accepted unsupported file header version");
            }
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("file header version mismatch"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn integrity_reports_file_header_shadow_mismatch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let mut stale = engine.file_header().await;
        stale.visible_ts = U64::new(stale.visible_ts.get() + 1);
        let shadow_page_id = page_id_from_u64(engine.page_storage.page_count() - 1).unwrap();
        {
            let mut guard = engine
                .buffer_pool
                .fetch_page_exclusive(shadow_page_id)
                .await
                .unwrap();
            let buf = guard.data_mut();
            SlottedPage::init(buf, shadow_page_id, PageType::FileHeaderShadow);
            write_file_header(buf, &stale);
        }
        engine.buffer_pool.flush_page(shadow_page_id).await.unwrap();

        let report = engine.check_integrity().await.unwrap();
        assert!(
            !report.has_errors(),
            "shadow mismatch should be a warning, got {:?}",
            report.issues
        );
        assert!(
            report.issues.iter().any(|issue| issue
                .message
                .contains("file-header shadow does not match primary")),
            "expected shadow mismatch warning, got {:?}",
            report.issues
        );

        engine.close().await.unwrap();
    }

    // ─── Test 2: open_in_memory ───

    #[tokio::test]
    async fn test_open_in_memory() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        assert!(!engine.is_durable());
        let fh = engine.file_header().await;
        assert_eq!(fh.magic.get(), FILE_HEADER_MAGIC);
        assert_eq!(fh.version.get(), FILE_HEADER_VERSION);
        assert!(fh.catalog_root_page.get() > 0);
    }

    // ─── Test 3: create_btree + insert + get ───

    #[tokio::test]
    async fn test_create_btree_insert_get() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let handle = engine.create_btree().await.unwrap();
        handle.insert(b"key1", b"value1").await.unwrap();
        handle.insert(b"key2", b"value2").await.unwrap();
        handle.insert(b"key3", b"value3").await.unwrap();

        assert_eq!(handle.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(handle.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(handle.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        assert_eq!(handle.get(b"key4").await.unwrap(), None);
    }

    // ─── Test 4: Multiple B-trees ───

    #[tokio::test]
    async fn test_multiple_btrees() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let bt1 = engine.create_btree().await.unwrap();
        let bt2 = engine.create_btree().await.unwrap();
        let bt3 = engine.create_btree().await.unwrap();

        bt1.insert(b"a", b"tree1").await.unwrap();
        bt2.insert(b"a", b"tree2").await.unwrap();
        bt3.insert(b"a", b"tree3").await.unwrap();

        assert_eq!(bt1.get(b"a").await.unwrap(), Some(b"tree1".to_vec()));
        assert_eq!(bt2.get(b"a").await.unwrap(), Some(b"tree2".to_vec()));
        assert_eq!(bt3.get(b"a").await.unwrap(), Some(b"tree3".to_vec()));

        // Verify isolation: keys from other trees are not visible.
        assert_eq!(bt1.get(b"b").await.unwrap(), None);
    }

    // ─── Test 5: Heap store + load ───

    #[tokio::test]
    async fn test_heap_store_load() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let data = b"hello, heap storage!";
        let href = engine.heap_store(data).await.unwrap();
        let loaded = engine.heap_load(href).await.unwrap();

        assert_eq!(loaded, data);
    }

    // ─── Test 6: WAL append + read ───

    #[tokio::test]
    async fn test_wal_append_read() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let lsn = engine.append_wal(0x01, b"test-payload").await.unwrap();
        assert_eq!(lsn, 0);

        let mut stream = engine.read_wal_from(0);
        use tokio_stream::StreamExt;
        let record = stream.next().await.unwrap().unwrap();
        assert_eq!(record.lsn, 0);
        assert_eq!(record.record_type, 0x01);
        assert_eq!(record.payload, b"test-payload");

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn usage_reports_page_wal_and_memory_consumption() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        engine.append_wal(0x01, b"usage").await.unwrap();

        let usage = engine.usage();
        assert_eq!(usage.page_size, 4096);
        assert!(usage.page_count >= 3);
        assert_eq!(
            usage.page_store_bytes,
            usage.page_count * usage.page_size as u64
        );
        assert!(usage.wal_retained_bytes >= WAL_FRAME_HEADER_SIZE as u64 + 5);
        assert_eq!(
            usage.disk_usage_bytes,
            usage.page_store_bytes + usage.wal_retained_bytes
        );
        assert_eq!(usage.memory_budget_bytes, 4096 * 64);
        assert!(usage.buffer_pool_used_frames > 0);
    }

    #[tokio::test]
    async fn disk_quota_rejects_wal_growth() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            max_disk_usage_bytes: Some(4096 * 3 + 20),
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        engine.append_wal(0x01, b"small").await.unwrap();
        let err = engine.append_wal(0x01, b"too-large").await.unwrap_err();
        assert!(
            err.to_string().contains("disk usage limit exceeded"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn disk_quota_rejects_page_growth() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            max_disk_usage_bytes: Some(4096 * 3),
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let err = match engine.create_btree().await {
            Ok(_) => panic!("create_btree should fail once disk quota is exhausted"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("disk usage limit exceeded"),
            "unexpected error: {err}"
        );
    }

    // ─── Test 7: Checkpoint persists data ───

    #[tokio::test]
    async fn test_checkpoint_persists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");

        // Open, insert into B-tree, checkpoint, close.
        let root_page;
        {
            let config = StorageConfig {
                page_size: 4096,
                memory_budget: 4096 * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();

            let handle = engine.create_btree().await.unwrap();
            handle
                .insert(b"persist-key", b"persist-value")
                .await
                .unwrap();
            root_page = handle.root_page();

            engine.checkpoint().await.unwrap();
            engine.close().await.unwrap();
        }

        // Reopen and verify data.
        {
            let config = StorageConfig {
                page_size: 4096,
                memory_budget: 4096 * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();

            let handle = engine.open_btree(root_page);
            let value = handle.get(b"persist-key").await.unwrap();
            assert_eq!(value, Some(b"persist-value".to_vec()));

            engine.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn snapshot_export_restore_reconstructs_pages_and_resumes_wal_lsn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let source_path = tmp.path().join("source");
        let restored_path = tmp.path().join("restored");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 256,
            ..Default::default()
        };

        let engine = StorageEngine::open(&source_path, config, &mut NoOpHandler)
            .await
            .unwrap();
        let btree = engine.create_btree().await.unwrap();
        btree.insert(b"alpha", b"one").await.unwrap();
        engine
            .append_wal(crate::wal::WAL_RECORD_TX_COMMIT, b"prior commit")
            .await
            .unwrap();

        let snapshot = engine.export_snapshot().await.unwrap();
        let checkpoint_lsn = snapshot.checkpoint_lsn;
        assert!(checkpoint_lsn > 0);
        let chunks = snapshot.clone().into_chunks(17).unwrap();
        let snapshot = StorageSnapshot::from_chunks(chunks).unwrap();
        engine.close().await.unwrap();

        let restore_config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 256,
            ..Default::default()
        };
        StorageEngine::restore_snapshot(&restored_path, restore_config, snapshot)
            .await
            .unwrap();

        let reopened_config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 256,
            ..Default::default()
        };
        let restored = StorageEngine::open(&restored_path, reopened_config, &mut NoOpHandler)
            .await
            .unwrap();
        let restored_btree = restored.open_btree(btree.root_page());
        assert_eq!(
            restored_btree.get(b"alpha").await.unwrap(),
            Some(b"one".to_vec())
        );

        let next_lsn = restored
            .append_wal(crate::wal::WAL_RECORD_TX_COMMIT, b"after restore")
            .await
            .unwrap();
        assert!(
            next_lsn >= checkpoint_lsn,
            "restored WAL must resume at or after checkpoint LSN"
        );
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_reclaims_sealed_wal_segments() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 128,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let payload = vec![0x5Au8; 80];
        for _ in 0..20 {
            engine.append_wal(0xEE, &payload).await.unwrap();
        }

        let oldest_before = engine.wal_storage.oldest_lsn().unwrap();
        let retained_before = engine.wal_storage.retained_size();
        assert_eq!(oldest_before, 0);

        let checkpoint_lsn = engine.wal_writer.current_lsn();
        engine.checkpoint().await.unwrap();

        let oldest_after = engine.wal_storage.oldest_lsn().unwrap();
        let retained_after = engine.wal_storage.retained_size();
        assert!(
            oldest_after <= checkpoint_lsn,
            "oldest retained LSN {oldest_after} must not exceed checkpoint LSN {checkpoint_lsn}",
        );
        assert!(
            oldest_after > oldest_before,
            "checkpoint should reclaim sealed WAL segments"
        );
        assert!(
            retained_after < retained_before,
            "retained WAL size should shrink after checkpoint"
        );

        let mut header = [0u8; 9];
        let n = engine
            .wal_storage
            .read_from(checkpoint_lsn, &mut header)
            .await
            .unwrap();
        assert_eq!(n, header.len(), "checkpoint record should remain readable");

        engine.close().await.unwrap();

        let reopen_config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 128,
            ..Default::default()
        };
        let reopened = StorageEngine::open(&path, reopen_config, &mut NoOpHandler)
            .await
            .unwrap();
        assert!(reopened.file_header().await.checkpoint_lsn.get() >= checkpoint_lsn);
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_respects_replication_retention_lsn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: 128,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let payload = vec![0xA5u8; 80];
        for _ in 0..20 {
            engine.append_wal(0xEF, &payload).await.unwrap();
        }

        assert_eq!(engine.replication_retention_lsn(), None);
        engine.set_replication_retention_lsn(Some(0));
        assert_eq!(engine.replication_retention_lsn(), Some(0));

        let retained_before = engine.wal_storage.retained_size();
        engine.checkpoint().await.unwrap();

        let oldest_with_replica = engine.wal_storage.oldest_lsn().unwrap();
        let retained_with_replica = engine.wal_storage.retained_size();
        assert_eq!(
            oldest_with_replica, 0,
            "lagging replica at LSN 0 must keep sealed WAL segments"
        );
        assert!(
            retained_with_replica >= retained_before,
            "checkpoint may append a record but must not reclaim while replica needs LSN 0"
        );

        engine.set_replication_retention_lsn(None);
        assert_eq!(engine.replication_retention_lsn(), None);
        engine.checkpoint().await.unwrap();

        let oldest_after_clear = engine.wal_storage.oldest_lsn().unwrap();
        assert!(
            oldest_after_clear > oldest_with_replica,
            "clearing replication retention should allow sealed WAL reclamation"
        );

        engine.close().await.unwrap();
    }

    // ─── Test 8: File header verify ───

    #[test]
    fn test_file_header_verify() {
        let fh = FileHeader::new(8192);
        assert!(fh.verify().is_ok());

        // Bad magic.
        let mut bad_magic = fh.clone();
        bad_magic.magic = U32::new(0xDEADBEEF);
        assert!(bad_magic.verify().is_err());

        // Bad version.
        let mut bad_version = fh.clone();
        bad_version.version = U32::new(99);
        assert!(bad_version.verify().is_err());
    }

    // ─── Test 9: File header updates ───

    #[tokio::test]
    async fn test_file_header_updates() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let fh_before = engine.file_header().await;
        assert_eq!(fh_before.visible_ts.get(), 0);
        assert_eq!(fh_before.replication_applied_lsn.get(), 0);

        engine
            .update_file_header(|fh| {
                fh.visible_ts = U64::new(12345);
                fh.replication_applied_lsn = U64::new(67890);
                fh.next_collection_id = U64::new(100);
            })
            .await
            .unwrap();

        let fh_after = engine.file_header().await;
        assert_eq!(fh_after.visible_ts.get(), 12345);
        assert_eq!(fh_after.replication_applied_lsn.get(), 67890);
        assert_eq!(engine.replication_applied_lsn().await, 67890);
        assert_eq!(fh_after.next_collection_id.get(), 100);

        // Verify catalog root pages are still set.
        assert!(fh_after.catalog_root_page.get() > 0);
        assert!(fh_after.catalog_name_root_page.get() > 0);
    }

    // ─── Test 10: Page size > 65535 should be rejected ───
    // BUG: Page header uses u16 for offsets, so max page size is 65535.
    // No validation exists — larger sizes cause silent truncation/corruption.

    #[tokio::test]
    async fn test_page_size_too_large_rejected() {
        let config = StorageConfig {
            page_size: 65536, // One byte over max u16
            memory_budget: 65536 * 64,
            ..Default::default()
        };
        let result = StorageEngine::open_in_memory(config).await;
        assert!(
            result.is_err(),
            "page_size > 65535 should be rejected (u16 overflow in page header)"
        );
    }

    #[tokio::test]
    async fn test_wal_segment_size_must_exceed_header() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            wal_segment_size: WAL_SEGMENT_HEADER_SIZE as usize,
            ..Default::default()
        };
        let err = match StorageEngine::open_in_memory(config).await {
            Ok(_) => panic!("wal_segment_size equal to header size should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("wal_segment_size 32 must be greater than WAL segment header size 32"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_controls_must_be_nonzero() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            checkpoint_wal_threshold: 0,
            ..Default::default()
        };
        let err = match StorageEngine::open_in_memory(config).await {
            Ok(_) => panic!("zero checkpoint_wal_threshold should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("checkpoint_wal_threshold must be greater than zero"),
            "unexpected error: {err}"
        );

        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            checkpoint_interval: Duration::ZERO,
            ..Default::default()
        };
        let err = match StorageEngine::open_in_memory(config).await {
            Ok(_) => panic!("zero checkpoint_interval should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("checkpoint_interval must be greater than zero"),
            "unexpected error: {err}"
        );
    }

    // ─── Test 11: Page size of exactly 65535 should work ───

    #[tokio::test]
    async fn test_page_size_max_valid() {
        let config = StorageConfig {
            page_size: 65535, // Max valid u16
            memory_budget: 65535 * 64,
            ..Default::default()
        };
        // Should succeed.
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        let fh = engine.file_header().await;
        assert_eq!(fh.page_size.get(), 65535);
    }

    #[tokio::test]
    async fn check_integrity_clean_database_has_no_errors() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let handle = engine.create_btree().await.unwrap();
        handle.insert(b"alpha", b"one").await.unwrap();
        let href = engine.heap_store(b"heap-value").await.unwrap();
        assert_eq!(engine.heap_load(href).await.unwrap(), b"heap-value");

        let report = engine.check_integrity().await.unwrap();
        assert!(
            report.is_ok(),
            "clean database should not have integrity errors: {:?}",
            report.issues
        );
        assert_eq!(report.stats.pages_scanned, report.stats.page_count);
        assert!(
            report
                .stats
                .page_type_counts
                .get("file_header")
                .copied()
                .unwrap_or(0)
                >= 1
        );
    }

    #[tokio::test]
    async fn check_integrity_detects_cold_page_checksum_corruption() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let page_size = 4096;
        let corrupted_page;

        {
            let config = StorageConfig {
                page_size,
                memory_budget: page_size * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();
            let fh = engine.file_header().await;
            corrupted_page = fh.catalog_root_page.get();
            engine.close().await.unwrap();
        }

        {
            let data_path = path.join("data.db");
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)
                .unwrap();
            file.seek(SeekFrom::Start(
                corrupted_page as u64 * page_size as u64 + page_size as u64 - 1,
            ))
            .unwrap();
            file.write_all(&[0xA5]).unwrap();
            file.sync_data().unwrap();
        }

        let config = StorageConfig {
            page_size,
            memory_budget: page_size * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();
        let report = engine.check_integrity().await.unwrap();
        assert!(report.has_errors(), "corruption should be reported");
        assert!(
            report.issues.iter().any(|issue| {
                issue.page_id == Some(corrupted_page) && issue.message.contains("checksum mismatch")
            }),
            "expected checksum finding for page {corrupted_page}: {:?}",
            report.issues
        );
        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn check_integrity_detects_data_file_size_mismatch() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let page_size = 4096;

        {
            let config = StorageConfig {
                page_size,
                memory_budget: page_size * 64,
                ..Default::default()
            };
            let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
                .await
                .unwrap();
            engine.close().await.unwrap();
        }

        {
            let data_path = path.join("data.db");
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)
                .unwrap();
            let len = file.metadata().unwrap().len();
            file.seek(SeekFrom::Start(len)).unwrap();
            file.write_all(&[0xA5]).unwrap();
            file.sync_data().unwrap();
        }

        let config = StorageConfig {
            page_size,
            memory_budget: page_size * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();
        let report = engine.check_integrity().await.unwrap();
        assert!(
            report.has_errors(),
            "data-file size mismatch should be reported"
        );
        assert!(
            report.issues.iter().any(|issue| {
                issue.page_id.is_none() && issue.message.contains("data file size")
            }),
            "expected data-file size finding: {:?}",
            report.issues
        );
        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn check_integrity_scans_retained_wal_and_reports_crc_mismatch() {
        use std::io::{Seek, SeekFrom, Write};

        const WAL_SEGMENT_HEADER_BYTES: u64 = 32;

        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("testdb");
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open(&path, config, &mut NoOpHandler)
            .await
            .unwrap();

        let first_lsn = engine.append_wal(0x42, b"first").await.unwrap();
        let second_lsn = engine.append_wal(0x43, b"second").await.unwrap();
        assert_eq!(first_lsn, 0);
        assert!(second_lsn > first_lsn);

        let wal_path = path.join("wal").join("segment-000001.wal");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.seek(SeekFrom::Start(
            WAL_SEGMENT_HEADER_BYTES + second_lsn + WAL_FRAME_HEADER_SIZE as u64,
        ))
        .unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_data().unwrap();

        let report = engine.check_integrity().await.unwrap();
        assert!(report.has_errors(), "WAL corruption should be reported");
        assert_eq!(report.stats.wal_records_scanned, 1);
        assert_eq!(
            report.stats.wal_bytes_scanned,
            (WAL_FRAME_HEADER_SIZE + b"first".len()) as u64
        );
        assert!(
            report.issues.iter().any(|issue| {
                issue.page_id.is_none()
                    && issue
                        .message
                        .contains(&format!("WAL record CRC mismatch at LSN {second_lsn}"))
            }),
            "expected WAL CRC mismatch finding: {:?}",
            report.issues
        );

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn check_integrity_detects_free_list_cycle() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let page_a = engine.create_btree().await.unwrap().root_page();
        let page_b = engine.create_btree().await.unwrap().root_page();

        {
            let mut free_list = engine.free_list.lock().await;
            free_list.deallocate(page_a).await.unwrap();
            free_list.deallocate(page_b).await.unwrap();
        }

        {
            let mut guard = engine
                .buffer_pool
                .fetch_page_exclusive(page_a)
                .await
                .unwrap();
            let mut page = SlottedPage::from_buf(guard.data_mut()).unwrap();
            page.set_prev_or_ptr(page_b);
            page.stamp_checksum();
        }

        let report = engine.check_integrity().await.unwrap();
        assert!(report.has_errors(), "free-list cycle should be reported");
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.message.contains("cycle")),
            "expected free-list cycle finding: {:?}",
            report.issues
        );
    }

    #[tokio::test]
    async fn repair_integrity_rebuilds_corrupt_free_list_chain() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();

        let page_a = engine.create_btree().await.unwrap().root_page();
        let page_b = engine.create_btree().await.unwrap().root_page();

        {
            let mut free_list = engine.free_list.lock().await;
            free_list.deallocate(page_a).await.unwrap();
            free_list.deallocate(page_b).await.unwrap();
        }

        {
            let mut guard = engine
                .buffer_pool
                .fetch_page_exclusive(page_a)
                .await
                .unwrap();
            let mut page = SlottedPage::from_buf(guard.data_mut()).unwrap();
            page.set_prev_or_ptr(page_b);
            page.stamp_checksum();
        }

        let before = engine.check_integrity().await.unwrap();
        assert!(before.has_errors(), "free-list cycle should be reported");

        let repair = engine.repair_integrity().await.unwrap();
        assert!(
            repair
                .repairs
                .iter()
                .any(|repair| repair.message.contains("rebuilt free-list chain")),
            "expected free-list rebuild repair action: {:?}",
            repair.repairs
        );
        assert!(
            repair.is_clean(),
            "free-list repair should leave storage clean: {:?}",
            repair.remaining_issues
        );

        let free_count = engine.free_list.lock().await.count().await.unwrap();
        assert_eq!(free_count, 2);

        let after = engine.check_integrity().await.unwrap();
        assert!(
            after.is_ok(),
            "free-list rebuild should clear integrity errors: {:?}",
            after.issues
        );
    }

    #[tokio::test]
    async fn check_integrity_with_btree_roots_reports_clean_tree_graph() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        let fh = engine.file_header().await;
        let handle = engine.create_btree().await.unwrap();
        for i in 0..250u32 {
            handle
                .insert(&i.to_be_bytes(), format!("value-{i}").as_bytes())
                .await
                .unwrap();
        }

        let roots = vec![
            IntegrityBTreeRoot {
                name: "catalog_by_id".to_string(),
                root_page: fh.catalog_root_page.get(),
            },
            IntegrityBTreeRoot {
                name: "catalog_by_name".to_string(),
                root_page: fh.catalog_name_root_page.get(),
            },
            IntegrityBTreeRoot {
                name: "user_tree".to_string(),
                root_page: handle.root_page(),
            },
        ];

        let report = engine
            .check_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(
            report.is_ok(),
            "named B-tree roots should have no integrity errors: {:?}",
            report.issues
        );
        assert!(report.stats.btree_pages >= 3);
        assert_eq!(report.stats.orphan_btree_pages, 0);
        assert_eq!(report.stats.double_allocated_pages, 0);
    }

    #[tokio::test]
    async fn repair_integrity_with_btree_roots_rebuilds_leaf_sibling_chain() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        let handle = engine.create_btree().await.unwrap();
        for i in 0..350u32 {
            handle
                .insert(&i.to_be_bytes(), format!("value-{i}").as_bytes())
                .await
                .unwrap();
        }

        let fh = engine.file_header().await;
        let root = IntegrityBTreeRoot {
            name: "user_tree".to_string(),
            root_page: handle.root_page(),
        };
        let roots = vec![
            IntegrityBTreeRoot {
                name: "catalog_by_id".to_string(),
                root_page: fh.catalog_root_page.get(),
            },
            IntegrityBTreeRoot {
                name: "catalog_by_name".to_string(),
                root_page: fh.catalog_name_root_page.get(),
            },
            root.clone(),
        ];
        let mut scratch = StorageEngine::empty_integrity_report(engine.page_storage.page_count());
        let (_, leaves) = engine
            .collect_single_btree_integrity(&root, engine.page_storage.page_count(), &mut scratch)
            .await
            .unwrap();
        assert!(
            !scratch.has_errors(),
            "fresh tree should collect without structural errors: {:?}",
            scratch.issues
        );
        let ordered = StorageEngine::ordered_btree_leaf_ids_by_key(&leaves).unwrap();
        assert!(
            ordered.len() > 1,
            "test setup should create a multi-leaf tree"
        );

        let broken_leaf = ordered[0];
        let expected_sibling = ordered[1];
        {
            let mut guard = engine
                .buffer_pool
                .fetch_page_exclusive(broken_leaf)
                .await
                .unwrap();
            let mut page = SlottedPage::from_buf(guard.data_mut()).unwrap();
            assert_eq!(page.prev_or_ptr(), expected_sibling);
            page.set_prev_or_ptr(0);
            page.stamp_checksum();
        }

        let before = engine
            .check_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(
            before.has_errors(),
            "broken sibling chain should be an integrity error"
        );
        assert!(
            before
                .issues
                .iter()
                .any(|issue| issue.page_id == Some(broken_leaf)
                    && issue.message.contains("does not match expected next leaf")),
            "expected explicit sibling-chain finding: {:?}",
            before.issues
        );

        let repair = engine
            .repair_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(
            repair
                .repairs
                .iter()
                .any(|repair| repair.page_id == Some(broken_leaf)
                    && repair
                        .message
                        .contains("rewrote B-tree 'user_tree' leaf right_sibling")),
            "expected sibling rewrite repair: {:?}",
            repair.repairs
        );
        assert!(
            repair.is_clean(),
            "repair should leave no remaining findings: {:?}",
            repair.remaining_issues
        );

        let values: Vec<_> = handle
            .scan(Bound::Unbounded, Bound::Unbounded, ScanDirection::Forward)
            .collect()
            .await;
        assert_eq!(values.len(), 350);
        for (i, row) in values.into_iter().enumerate() {
            let (key, value) = row.unwrap();
            assert_eq!(key, (i as u32).to_be_bytes());
            assert_eq!(value, format!("value-{i}").into_bytes());
        }

        let after = engine
            .check_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(
            after.is_ok(),
            "repaired sibling chain should pass integrity: {:?}",
            after.issues
        );
    }

    #[tokio::test]
    async fn check_integrity_with_btree_roots_detects_double_allocation() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        let handle = engine.create_btree().await.unwrap();
        let roots = vec![
            IntegrityBTreeRoot {
                name: "first".to_string(),
                root_page: handle.root_page(),
            },
            IntegrityBTreeRoot {
                name: "second".to_string(),
                root_page: handle.root_page(),
            },
        ];

        let report = engine
            .check_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(report.has_errors(), "double allocation should be an error");
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.message.contains("reachable from both")),
            "expected duplicate root finding: {:?}",
            report.issues
        );
    }

    #[tokio::test]
    async fn check_integrity_with_btree_roots_reports_orphan_btree_pages() {
        let config = StorageConfig {
            page_size: 4096,
            memory_budget: 4096 * 64,
            ..Default::default()
        };
        let engine = StorageEngine::open_in_memory(config).await.unwrap();
        let fh = engine.file_header().await;
        let orphan = engine.create_btree().await.unwrap();

        let roots = vec![
            IntegrityBTreeRoot {
                name: "catalog_by_id".to_string(),
                root_page: fh.catalog_root_page.get(),
            },
            IntegrityBTreeRoot {
                name: "catalog_by_name".to_string(),
                root_page: fh.catalog_name_root_page.get(),
            },
        ];

        let report = engine
            .check_integrity_with_btree_roots(&roots)
            .await
            .unwrap();
        assert!(
            !report.has_errors(),
            "orphan B-tree pages are warnings, not structural errors: {:?}",
            report.issues
        );
        assert!(report.stats.orphan_btree_pages >= 1);
        assert!(
            report.issues.iter().any(|issue| {
                issue.page_id == Some(orphan.root_page())
                    && issue.severity == IntegritySeverity::Warning
                    && issue.message.contains("not reachable")
            }),
            "expected orphan finding for page {}: {:?}",
            orphan.root_page(),
            report.issues
        );
    }
}
