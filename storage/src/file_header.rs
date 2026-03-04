//! Database file header (page 0) with shadow copy for crash safety.
//!
//! The file header stores essential database metadata: page size, page count,
//! free list head, catalog root page, and checkpoint LSN. A shadow copy is
//! kept at the last page of the file so that if either copy is corrupted by
//! a torn write, the other can be used to recover.

use crate::types::*;
use crate::buffer_pool::{BufferPoolError, ExclusivePageGuard, SharedPageGuard, PageIO};

#[derive(Debug, thiserror::Error)]
pub enum FileHeaderError {
    #[error("invalid magic: expected {expected:#010x}, found {found:#010x}")]
    InvalidMagic { expected: u32, found: u32 },

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u32),

    #[error("checksum mismatch")]
    ChecksumMismatch,

    #[error("both primary and shadow headers are corrupt")]
    BothCorrupt,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),
}

/// File header fields stored in page 0.
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub free_list_head: Option<PageId>,
    pub catalog_root_page: PageId,
    pub next_collection_id: u64,
    pub next_index_id: u64,
    pub checkpoint_lsn: Lsn,
    pub created_at: u64,
}

impl FileHeader {
    /// Layout offsets.
    const MAGIC_OFF: usize = 0;
    const VERSION_OFF: usize = 4;
    const PAGE_SIZE_OFF: usize = 8;
    const PAGE_COUNT_OFF: usize = 12;
    const FREE_LIST_HEAD_OFF: usize = 20;
    const CATALOG_ROOT_OFF: usize = 24;
    const NEXT_COL_ID_OFF: usize = 28;
    const NEXT_IDX_ID_OFF: usize = 36;
    const CHECKPOINT_LSN_OFF: usize = 44;
    const CREATED_AT_OFF: usize = 52;
    const CHECKSUM_OFF: usize = 60;
    const RESERVED_OFF: usize = 64;

    /// Create a fresh file header for a new database.
    pub fn new_database(page_size: u32, catalog_root_page: PageId) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            magic: DATA_FILE_MAGIC,
            version: 1,
            page_size,
            page_count: 3, // header + catalog root + shadow
            free_list_head: None,
            catalog_root_page,
            next_collection_id: 1,
            next_index_id: 1,
            checkpoint_lsn: Lsn::ZERO,
            created_at: now_ms,
        }
    }

    /// Serialize into a page-sized buffer.
    pub fn serialize(&self, page_size: u32) -> Vec<u8> {
        let mut buf = vec![0u8; page_size as usize];
        self.write_to_buf(&mut buf);
        // Compute checksum over all except the checksum field.
        let crc = self.compute_buf_checksum(&buf);
        buf[Self::CHECKSUM_OFF..Self::CHECKSUM_OFF + 4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn write_to_buf(&self, buf: &mut [u8]) {
        buf[Self::MAGIC_OFF..Self::MAGIC_OFF + 4].copy_from_slice(&self.magic.to_le_bytes());
        buf[Self::VERSION_OFF..Self::VERSION_OFF + 4].copy_from_slice(&self.version.to_le_bytes());
        buf[Self::PAGE_SIZE_OFF..Self::PAGE_SIZE_OFF + 4].copy_from_slice(&self.page_size.to_le_bytes());
        buf[Self::PAGE_COUNT_OFF..Self::PAGE_COUNT_OFF + 8].copy_from_slice(&self.page_count.to_le_bytes());
        let flh = self.free_list_head.map(|p| p.0).unwrap_or(0);
        buf[Self::FREE_LIST_HEAD_OFF..Self::FREE_LIST_HEAD_OFF + 4].copy_from_slice(&flh.to_le_bytes());
        buf[Self::CATALOG_ROOT_OFF..Self::CATALOG_ROOT_OFF + 4].copy_from_slice(&self.catalog_root_page.0.to_le_bytes());
        buf[Self::NEXT_COL_ID_OFF..Self::NEXT_COL_ID_OFF + 8].copy_from_slice(&self.next_collection_id.to_le_bytes());
        buf[Self::NEXT_IDX_ID_OFF..Self::NEXT_IDX_ID_OFF + 8].copy_from_slice(&self.next_index_id.to_le_bytes());
        buf[Self::CHECKPOINT_LSN_OFF..Self::CHECKPOINT_LSN_OFF + 8].copy_from_slice(&self.checkpoint_lsn.0.to_le_bytes());
        buf[Self::CREATED_AT_OFF..Self::CREATED_AT_OFF + 8].copy_from_slice(&self.created_at.to_le_bytes());
    }

    fn compute_buf_checksum(&self, buf: &[u8]) -> u32 {
        let mut digest = 0u32;
        digest = crc32c::crc32c_append(digest, &buf[..Self::CHECKSUM_OFF]);
        digest = crc32c::crc32c_append(digest, &buf[Self::RESERVED_OFF..]);
        digest
    }

    /// Deserialize from a page-sized buffer. Verifies checksum.
    pub fn deserialize(buf: &[u8]) -> Result<Self, FileHeaderError> {
        let magic = u32::from_le_bytes(buf[Self::MAGIC_OFF..Self::MAGIC_OFF + 4].try_into().unwrap());
        if magic != DATA_FILE_MAGIC {
            return Err(FileHeaderError::InvalidMagic {
                expected: DATA_FILE_MAGIC,
                found: magic,
            });
        }

        let version = u32::from_le_bytes(buf[Self::VERSION_OFF..Self::VERSION_OFF + 4].try_into().unwrap());
        if version != 1 {
            return Err(FileHeaderError::UnsupportedVersion(version));
        }

        let stored_crc = u32::from_le_bytes(buf[Self::CHECKSUM_OFF..Self::CHECKSUM_OFF + 4].try_into().unwrap());
        let mut computed = 0u32;
        computed = crc32c::crc32c_append(computed, &buf[..Self::CHECKSUM_OFF]);
        computed = crc32c::crc32c_append(computed, &buf[Self::RESERVED_OFF..]);
        if stored_crc != computed {
            return Err(FileHeaderError::ChecksumMismatch);
        }

        let flh = u32::from_le_bytes(buf[Self::FREE_LIST_HEAD_OFF..Self::FREE_LIST_HEAD_OFF + 4].try_into().unwrap());

        Ok(Self {
            magic,
            version,
            page_size: u32::from_le_bytes(buf[Self::PAGE_SIZE_OFF..Self::PAGE_SIZE_OFF + 4].try_into().unwrap()),
            page_count: u64::from_le_bytes(buf[Self::PAGE_COUNT_OFF..Self::PAGE_COUNT_OFF + 8].try_into().unwrap()),
            free_list_head: if flh == 0 { None } else { Some(PageId(flh)) },
            catalog_root_page: PageId(u32::from_le_bytes(buf[Self::CATALOG_ROOT_OFF..Self::CATALOG_ROOT_OFF + 4].try_into().unwrap())),
            next_collection_id: u64::from_le_bytes(buf[Self::NEXT_COL_ID_OFF..Self::NEXT_COL_ID_OFF + 8].try_into().unwrap()),
            next_index_id: u64::from_le_bytes(buf[Self::NEXT_IDX_ID_OFF..Self::NEXT_IDX_ID_OFF + 8].try_into().unwrap()),
            checkpoint_lsn: Lsn(u64::from_le_bytes(buf[Self::CHECKPOINT_LSN_OFF..Self::CHECKPOINT_LSN_OFF + 8].try_into().unwrap())),
            created_at: u64::from_le_bytes(buf[Self::CREATED_AT_OFF..Self::CREATED_AT_OFF + 8].try_into().unwrap()),
        })
    }

    /// Write header to a page guard.
    pub fn write_to_page(&self, guard: &mut ExclusivePageGuard) {
        let page_size = guard.data().len() as u32;
        let buf = self.serialize(page_size);
        guard.data_mut().copy_from_slice(&buf);
    }

    /// Read from a shared page guard.
    pub fn read_from_page(guard: &SharedPageGuard) -> Result<Self, FileHeaderError> {
        Self::deserialize(guard.data())
    }

    /// Attempt to read the file header, falling back to shadow on corruption.
    pub fn read_with_fallback(io: &dyn PageIO, page_size: u32) -> Result<Self, FileHeaderError> {
        let mut buf = vec![0u8; page_size as usize];

        // Try primary (page 0).
        if io.read_page(PageId(0), &mut buf).is_ok() && Self::deserialize(&buf).is_ok() {
            return Self::deserialize(&buf);
        }

        // Try shadow (last page). Need to figure out file size.
        let page_count = io.page_count();
        if page_count < 2 {
            return Err(FileHeaderError::BothCorrupt);
        }
        let shadow_page = PageId((page_count - 1) as u32);
        if io.read_page(shadow_page, &mut buf).is_ok() && Self::deserialize(&buf).is_ok() {
            // Restore primary from shadow.
            let _ = io.write_page(PageId(0), &buf);
            return Self::deserialize(&buf);
        }

        Err(FileHeaderError::BothCorrupt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_database() {
        let h = FileHeader::new_database(8192, PageId(1));
        assert_eq!(h.magic, DATA_FILE_MAGIC);
        assert_eq!(h.version, 1);
        assert_eq!(h.page_size, 8192);
        assert_eq!(h.checkpoint_lsn, Lsn::ZERO);
    }

    #[test]
    fn test_serialize_roundtrip() {
        let h = FileHeader::new_database(8192, PageId(1));
        let buf = h.serialize(8192);
        let h2 = FileHeader::deserialize(&buf).unwrap();
        assert_eq!(h2.magic, h.magic);
        assert_eq!(h2.page_size, h.page_size);
        assert_eq!(h2.catalog_root_page, h.catalog_root_page);
        assert_eq!(h2.checkpoint_lsn, h.checkpoint_lsn);
    }

    #[test]
    fn test_checksum_corruption() {
        let h = FileHeader::new_database(8192, PageId(1));
        let mut buf = h.serialize(8192);
        buf[100] ^= 0xFF;
        assert!(FileHeader::deserialize(&buf).is_err());
    }

    #[test]
    fn test_shadow_fallback() {
        use crate::buffer_pool::MemPageIO;
        let io = MemPageIO::new(8192, 3);
        let h = FileHeader::new_database(8192, PageId(1));
        let buf = h.serialize(8192);
        // Write valid shadow at page 2.
        io.write_page(PageId(2), &buf).unwrap();
        // Corrupt page 0.
        let mut corrupt = vec![0u8; 8192];
        corrupt[0] = 0xFF; // bad magic
        io.write_page(PageId(0), &corrupt).unwrap();
        let recovered = FileHeader::read_with_fallback(&io, 8192).unwrap();
        assert_eq!(recovered.magic, DATA_FILE_MAGIC);
    }

    #[test]
    fn test_both_corrupt() {
        use crate::buffer_pool::MemPageIO;
        let io = MemPageIO::new(8192, 3);
        // Both pages corrupt.
        let corrupt = vec![0xFFu8; 8192];
        io.write_page(PageId(0), &corrupt).unwrap();
        io.write_page(PageId(2), &corrupt).unwrap();
        let result = FileHeader::read_with_fallback(&io, 8192);
        assert!(result.is_err());
    }
}
