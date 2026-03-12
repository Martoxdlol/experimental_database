//! Double-Write Buffer (DWB) for torn-write protection.
//!
//! Every dirty page is written to the DWB file and fsynced before being
//! scatter-written to the page storage. If a crash interrupts the scatter-write,
//! the DWB has intact copies for recovery.
//!
//! **File-backed only**: In-memory backends skip DWB entirely (no torn writes possible).

use crate::backend::{PageId, PageStorage};
use crate::page::SlottedPageRef;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ─── Constants ───

/// DWB file header magic bytes: "DWB\0".
const DWB_MAGIC: u32 = 0x44574200;

/// Current DWB format version.
const DWB_VERSION: u16 = 1;

/// Size of the DWB header in bytes.
const DWB_HEADER_SIZE: usize = 16;

/// Size of each entry's page_id prefix in bytes.
const DWB_ENTRY_PREFIX_SIZE: usize = 4;

// ─── DwbHeader ───

/// DWB file header: 16 bytes.
/// Serialized manually with from_le_bytes()/to_le_bytes().
#[derive(Debug, Clone, Copy)]
pub struct DwbHeader {
    /// Magic number: 0x44574200 ("DWB\0").
    pub magic: u32,
    /// Format version (currently 1).
    pub version: u16,
    /// Page size; must match the data file.
    pub page_size: u16,
    /// Number of page entries in this DWB batch.
    pub page_count: u32,
    /// CRC-32C of the first 12 bytes of the header.
    pub checksum: u32,
}

impl DwbHeader {
    /// Serialize the header to a 16-byte buffer.
    #[allow(clippy::wrong_self_convention)]
    fn to_bytes(&self) -> [u8; DWB_HEADER_SIZE] {
        let mut buf = [0u8; DWB_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.page_size.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_count.to_le_bytes());
        buf[12..16].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Deserialize a header from a 16-byte buffer.
    fn from_bytes(buf: &[u8; DWB_HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            version: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            page_size: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            page_count: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        }
    }

    /// Compute the CRC-32C of the first 12 bytes (magic + version + page_size + page_count).
    fn compute_checksum(&self) -> u32 {
        let mut buf = [0u8; 12];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.page_size.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_count.to_le_bytes());
        crc32fast::hash(&buf)
    }
}

// ─── DoubleWriteBuffer ───

/// Double-Write Buffer manager.
///
/// Provides torn-write protection for the page store during checkpoint.
pub struct DoubleWriteBuffer {
    dwb_path: PathBuf,
    page_storage: Arc<dyn PageStorage>,
    page_size: usize,
}

impl DoubleWriteBuffer {
    /// Create a DWB manager for the given path.
    pub fn new(path: &Path, page_storage: Arc<dyn PageStorage>, page_size: usize) -> Self {
        Self {
            dwb_path: path.to_path_buf(),
            page_storage,
            page_size,
        }
    }

    /// Write a batch of dirty pages to the DWB file, then scatter-write to page storage.
    ///
    /// Steps:
    ///   1. Write DWB header + all pages to dwb_path sequentially (spawn_blocking)
    ///   2. fsync dwb_path (inside spawn_blocking)
    ///   3. Scatter-write each page to page_storage (async)
    ///   4. page_storage.sync() (async)
    ///   5. Truncate dwb_path to 0 (spawn_blocking)
    ///   6. fsync dwb_path (inside spawn_blocking)
    pub async fn write_pages(&self, pages: &[(PageId, Vec<u8>)]) -> io::Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        // Steps 1-2: Write DWB file and fsync (blocking file I/O).
        let dwb_path = self.dwb_path.clone();
        let page_size = self.page_size;
        let pages_clone: Vec<(PageId, Vec<u8>)> = pages
            .iter()
            .map(|(id, data)| (*id, data.clone()))
            .collect();

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)?;

            // Build and write header.
            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: page_size as u16,
                page_count: pages_clone.len() as u32,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes())?;

            // Write entries: page_id (u32 LE) + page_data.
            for (page_id, page_data) in &pages_clone {
                file.write_all(&page_id.to_le_bytes())?;
                file.write_all(page_data)?;
            }

            // fsync DWB -- critical point.
            file.sync_data()?;
            Ok(())
        })
        .await
        .map_err(|e| io::Error::other(e))??;

        // Step 3: Scatter-write each page to page_storage (async).
        for (page_id, page_data) in pages {
            self.page_storage.write_page(*page_id, page_data).await?;
        }

        // Step 4: Sync page_storage (async).
        self.page_storage.sync().await?;

        // Steps 5-6: Truncate DWB and fsync (blocking file I/O).
        let dwb_path = self.dwb_path.clone();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let file = OpenOptions::new().write(true).open(&dwb_path)?;
            file.set_len(0)?;
            file.sync_data()?;
            Ok(())
        })
        .await
        .map_err(|e| io::Error::other(e))??;

        Ok(())
    }

    /// Recover from a crash: restore any torn pages in page_storage from DWB.
    /// Returns the number of pages restored.
    pub async fn recover(&self) -> io::Result<u32> {
        // Step 1: If DWB doesn't exist or is empty, nothing to recover.
        if !self.dwb_path.exists() {
            return Ok(0);
        }

        let metadata = fs::metadata(&self.dwb_path)?;
        if metadata.len() == 0 {
            return Ok(0);
        }

        // Step 2: Read DWB file contents (blocking file I/O).
        let dwb_path = self.dwb_path.clone();
        let page_size = self.page_size;

        // Read entire DWB file, parse header + entries in spawn_blocking.
        // Returns: Ok(None) if empty/corrupt header, Ok(Some((header, entries))) otherwise.
        let parsed = {
            let dwb_path = dwb_path.clone();
            tokio::task::spawn_blocking(move || -> io::Result<Option<(DwbHeader, Vec<(PageId, Vec<u8>)>)>> {
                let mut file = File::open(&dwb_path)?;
                let mut header_buf = [0u8; DWB_HEADER_SIZE];

                if file.read(&mut header_buf)? < DWB_HEADER_SIZE {
                    // Partial header -- crash during header write. Treat as empty DWB.
                    return Ok(None);
                }

                let header = DwbHeader::from_bytes(&header_buf);

                // Verify magic.
                if header.magic != DWB_MAGIC {
                    return Ok(None);
                }

                // Verify checksum.
                let expected_checksum = header.compute_checksum();
                if header.checksum != expected_checksum {
                    return Ok(None);
                }

                // Read all entries.
                let entry_size = DWB_ENTRY_PREFIX_SIZE + page_size;
                let mut entries = Vec::new();

                for _i in 0..header.page_count {
                    let mut entry_buf = vec![0u8; entry_size];
                    let bytes_read = file.read(&mut entry_buf)?;

                    if bytes_read < entry_size {
                        // Incomplete entry -- crash during DWB write. Stop.
                        break;
                    }

                    let page_id = u32::from_le_bytes(entry_buf[0..4].try_into().unwrap());
                    let page_data = entry_buf[DWB_ENTRY_PREFIX_SIZE..].to_vec();
                    entries.push((page_id, page_data));
                }

                Ok(Some((header, entries)))
            })
            .await
            .map_err(|e| io::Error::other(e))??
        };

        let (header, entries) = match parsed {
            Some(v) => v,
            None => {
                self.truncate()?;
                return Ok(0);
            }
        };

        // Verify page_size matches.
        if header.page_size as usize != self.page_size {
            return Err(crate::error::StorageError::Corruption(format!(
                "DWB page_size mismatch: DWB has {}, expected {}",
                header.page_size, self.page_size
            ))
            .into());
        }

        // Step 3: Process each entry (async page_storage calls).
        let mut restored = 0u32;

        for (page_id, dwb_page_data) in &entries {
            // Verify DWB page checksum via SlottedPageRef.
            let dwb_page_ref = SlottedPageRef::from_buf(dwb_page_data)?;
            if !dwb_page_ref.verify_checksum() {
                // DWB page itself is corrupt (crash during DWB write). Skip.
                continue;
            }

            // Read the same page from page_storage and verify its checksum.
            let mut storage_page = vec![0u8; self.page_size];
            match self.page_storage.read_page(*page_id, &mut storage_page).await {
                Ok(()) => {
                    let storage_ref = SlottedPageRef::from_buf(&storage_page)?;
                    if !storage_ref.verify_checksum() {
                        // Torn write detected -- restore from DWB.
                        self.page_storage.write_page(*page_id, dwb_page_data).await?;
                        restored += 1;
                    }
                    // If storage checksum valid, skip (scatter-write completed before crash).
                }
                Err(_) => {
                    // Can't read page -- restore from DWB.
                    self.page_storage.write_page(*page_id, dwb_page_data).await?;
                    restored += 1;
                }
            }
        }

        // Step 4: Sync page_storage (async).
        self.page_storage.sync().await?;

        // Step 5: Truncate and fsync DWB.
        self.truncate()?;

        Ok(restored)
    }

    /// Truncate the DWB file to zero (called after successful checkpoint).
    pub fn truncate(&self) -> io::Result<()> {
        if !self.dwb_path.exists() {
            return Ok(());
        }
        let file = OpenOptions::new().write(true).open(&self.dwb_path)?;
        file.set_len(0)?;
        file.sync_data()?;
        Ok(())
    }

    /// Check if the DWB file exists and is non-empty.
    pub fn is_empty(&self) -> io::Result<bool> {
        if !self.dwb_path.exists() {
            return Ok(true);
        }
        let metadata = fs::metadata(&self.dwb_path)?;
        Ok(metadata.len() == 0)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryPageStorage;
    use crate::page::{PageType, SlottedPage};
    use tempfile::TempDir;

    const PAGE_SIZE: usize = 4096;

    /// Helper: create a valid page with a correct checksum for the given page_id.
    fn make_valid_page(page_id: PageId) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        let mut page = SlottedPage::init(&mut buf, page_id, PageType::Heap);
        // Write some data to make the page non-trivial.
        let data = format!("page-{}-data", page_id);
        page.insert_slot(data.as_bytes()).unwrap();
        page.stamp_checksum();
        buf
    }

    /// Helper: create a MemoryPageStorage with `n` pages, initialized as valid pages.
    async fn make_storage_with_pages(n: u32) -> Arc<MemoryPageStorage> {
        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(n as u64).await.unwrap();
        for i in 0..n {
            let page = make_valid_page(i);
            storage.write_page(i, &page).await.unwrap();
        }
        storage
    }

    // ─── Test 1: write_pages + verify data.db ───
    // Write 10 dirty pages via DWB. Verify all pages in page_storage match.

    #[tokio::test]
    async fn write_pages_and_verify() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        let storage = make_storage_with_pages(10).await;
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);

        // Create 10 dirty pages with new data.
        let mut pages: Vec<(PageId, Vec<u8>)> = Vec::new();
        for i in 0..10u32 {
            let page_data = make_valid_page(i);
            pages.push((i, page_data));
        }

        dwb.write_pages(&pages).await.unwrap();

        // Verify all pages in page_storage match.
        for (page_id, expected_data) in &pages {
            let mut buf = vec![0u8; PAGE_SIZE];
            storage.read_page(*page_id, &mut buf).await.unwrap();
            assert_eq!(&buf, expected_data, "page {} mismatch", page_id);
        }
    }

    // ─── Test 2: DWB file format ───
    // Write pages, read DWB file manually, verify header and entries.
    // Note: After write_pages, DWB is truncated. We'll write DWB manually for this test.

    #[tokio::test]
    async fn dwb_file_format() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        let _storage = make_storage_with_pages(3).await;
        let page_data: Vec<(PageId, Vec<u8>)> = (0..3u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        // Write to DWB manually (simulating the first part of write_pages).
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: PAGE_SIZE as u16,
                page_count: 3,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes()).unwrap();

            for (page_id, data) in &page_data {
                file.write_all(&page_id.to_le_bytes()).unwrap();
                file.write_all(data).unwrap();
            }
            file.sync_data().unwrap();
        }

        // Read the DWB file manually and verify.
        let mut file = File::open(&dwb_path).unwrap();

        // Read header.
        let mut header_buf = [0u8; DWB_HEADER_SIZE];
        file.read_exact(&mut header_buf).unwrap();
        let header = DwbHeader::from_bytes(&header_buf);

        assert_eq!(header.magic, DWB_MAGIC);
        assert_eq!(header.version, DWB_VERSION);
        assert_eq!(header.page_size, PAGE_SIZE as u16);
        assert_eq!(header.page_count, 3);
        assert_eq!(header.checksum, header.compute_checksum());

        // Read entries.
        for (expected_page_id, expected_data) in &page_data {
            let mut pid_buf = [0u8; 4];
            file.read_exact(&mut pid_buf).unwrap();
            let pid = u32::from_le_bytes(pid_buf);
            assert_eq!(pid, *expected_page_id);

            let mut data_buf = vec![0u8; PAGE_SIZE];
            file.read_exact(&mut data_buf).unwrap();
            assert_eq!(&data_buf, expected_data);
        }

        // Verify total file size.
        let expected_size =
            DWB_HEADER_SIZE + 3 * (DWB_ENTRY_PREFIX_SIZE + PAGE_SIZE);
        let metadata = fs::metadata(&dwb_path).unwrap();
        assert_eq!(metadata.len(), expected_size as u64);
    }

    // ─── Test 3: is_empty after clean checkpoint ───
    // After write_pages completes, DWB should be empty.

    #[tokio::test]
    async fn is_empty_after_clean_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("test.dwb");

        let storage = make_storage_with_pages(5).await;
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);

        let pages: Vec<(PageId, Vec<u8>)> = (0..5u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        dwb.write_pages(&pages).await.unwrap();

        assert!(dwb.is_empty().unwrap(), "DWB should be empty after write_pages completes");
    }

    // ─── Test 4: recover with no DWB ───
    // Call recover when DWB doesn't exist -> returns 0.

    #[tokio::test]
    async fn recover_no_dwb() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("nonexistent.dwb");

        let storage = make_storage_with_pages(1).await;
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage, PAGE_SIZE);

        let restored = dwb.recover().await.unwrap();
        assert_eq!(restored, 0);
    }

    // ─── Test 5: recover with empty DWB ───
    // Create empty file, recover -> returns 0.

    #[tokio::test]
    async fn recover_empty_dwb() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("empty.dwb");

        // Create empty file.
        File::create(&dwb_path).unwrap();

        let storage = make_storage_with_pages(1).await;
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage, PAGE_SIZE);

        let restored = dwb.recover().await.unwrap();
        assert_eq!(restored, 0);
    }

    // ─── Test 6: Simulated torn write recovery ───
    // Write DWB but don't scatter-write (simulate crash between steps 4-5).
    // Corrupt a page in page_storage. Call recover. Verify page restored from DWB.

    #[tokio::test]
    async fn simulated_torn_write_recovery() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("torn.dwb");

        let storage = make_storage_with_pages(5).await;
        let page_data: Vec<(PageId, Vec<u8>)> = (0..5u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        // Simulate: write DWB file but DON'T scatter-write to page storage.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: PAGE_SIZE as u16,
                page_count: page_data.len() as u32,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes()).unwrap();

            for (page_id, data) in &page_data {
                file.write_all(&page_id.to_le_bytes()).unwrap();
                file.write_all(data).unwrap();
            }
            file.sync_data().unwrap();
        }

        // Corrupt page 2 in page_storage (simulate torn write).
        let corrupt_page = vec![0xFFu8; PAGE_SIZE];
        // Put a bad checksum: the all-0xFF page won't have a valid CRC.
        storage.write_page(2, &corrupt_page).await.unwrap();

        // Recover.
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);
        let restored = dwb.recover().await.unwrap();

        // At least page 2 should have been restored.
        assert!(restored >= 1, "expected at least 1 page restored, got {}", restored);

        // Verify page 2 is now correct.
        let mut buf = vec![0u8; PAGE_SIZE];
        storage.read_page(2, &mut buf).await.unwrap();
        assert_eq!(&buf, &page_data[2].1, "page 2 should be restored from DWB");
    }

    // ─── Test 7: Partial DWB recovery ───
    // Write DWB header + 3 entries, truncate file mid-entry (simulate crash
    // during DWB write). Recover should handle gracefully (skip incomplete entry).

    #[tokio::test]
    async fn partial_dwb_recovery() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("partial.dwb");

        let storage = make_storage_with_pages(5).await;
        let page_data: Vec<(PageId, Vec<u8>)> = (0..5u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        // Write DWB header claiming 5 entries, but only write 3 complete + partial 4th.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: PAGE_SIZE as u16,
                page_count: 5,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes()).unwrap();

            // Write 3 complete entries.
            for i in 0..3 {
                file.write_all(&page_data[i].0.to_le_bytes()).unwrap();
                file.write_all(&page_data[i].1).unwrap();
            }

            // Write partial 4th entry (just the page_id, no page data).
            file.write_all(&page_data[3].0.to_le_bytes()).unwrap();
            // Don't write page data -- simulate crash.
            file.sync_data().unwrap();
        }

        // Corrupt page 1 to verify recovery handles the complete entries.
        let corrupt = vec![0xFFu8; PAGE_SIZE];
        storage.write_page(1, &corrupt).await.unwrap();

        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);
        let restored = dwb.recover().await.unwrap();

        // Page 1 was corrupt and should be restored.
        assert!(restored >= 1, "expected at least 1 restored page");

        let mut buf = vec![0u8; PAGE_SIZE];
        storage.read_page(1, &mut buf).await.unwrap();
        assert_eq!(&buf, &page_data[1].1, "page 1 should be restored");

        // DWB should be truncated after recovery.
        assert!(dwb.is_empty().unwrap());
    }

    // ─── Test 8: All pages valid ───
    // Write DWB, scatter-write all pages successfully. Don't truncate DWB.
    // Recover -> 0 pages restored (all checksums valid).

    #[tokio::test]
    async fn all_pages_valid_recovery() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("valid.dwb");

        let storage = make_storage_with_pages(5).await;
        let page_data: Vec<(PageId, Vec<u8>)> = (0..5u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        // Write DWB file.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: PAGE_SIZE as u16,
                page_count: page_data.len() as u32,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes()).unwrap();

            for (page_id, data) in &page_data {
                file.write_all(&page_id.to_le_bytes()).unwrap();
                file.write_all(data).unwrap();
            }
            file.sync_data().unwrap();
        }

        // Also scatter-write all pages to storage (simulating successful write).
        for (page_id, data) in &page_data {
            storage.write_page(*page_id, data).await.unwrap();
        }

        // Don't truncate DWB -- simulate crash after sync but before truncate.
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);
        let restored = dwb.recover().await.unwrap();

        assert_eq!(restored, 0, "all pages should be valid, nothing to restore");
    }

    // ─── Test 9: Large batch ───
    // Write 1000 pages through DWB. Verify correctness.

    #[tokio::test]
    async fn large_batch() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("large.dwb");

        let storage = Arc::new(MemoryPageStorage::new(PAGE_SIZE));
        storage.extend(1000).await.unwrap();

        let dwb = DoubleWriteBuffer::new(&dwb_path, storage.clone(), PAGE_SIZE);

        let pages: Vec<(PageId, Vec<u8>)> = (0..1000u32)
            .map(|i| (i, make_valid_page(i)))
            .collect();

        dwb.write_pages(&pages).await.unwrap();

        // Verify all pages.
        for (page_id, expected) in &pages {
            let mut buf = vec![0u8; PAGE_SIZE];
            storage.read_page(*page_id, &mut buf).await.unwrap();
            assert_eq!(&buf, expected, "page {} mismatch", page_id);
        }

        assert!(dwb.is_empty().unwrap());
    }

    // ─── Test 10: Page size mismatch ───
    // DWB with different page_size than current -> recover returns error.

    #[tokio::test]
    async fn page_size_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dwb_path = tmp.path().join("mismatch.dwb");

        // Write a DWB with page_size = 8192.
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dwb_path)
                .unwrap();

            let mut header = DwbHeader {
                magic: DWB_MAGIC,
                version: DWB_VERSION,
                page_size: 8192,
                page_count: 1,
                checksum: 0,
            };
            header.checksum = header.compute_checksum();
            file.write_all(&header.to_bytes()).unwrap();

            // Write some dummy data.
            file.write_all(&0u32.to_le_bytes()).unwrap();
            file.write_all(&vec![0u8; 8192]).unwrap();
            file.sync_data().unwrap();
        }

        let storage = make_storage_with_pages(1).await;
        let dwb = DoubleWriteBuffer::new(&dwb_path, storage, PAGE_SIZE);

        let result = dwb.recover().await;
        assert!(result.is_err(), "page_size mismatch should return error");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("mismatch"),
            "error should mention mismatch: {}",
            err
        );
    }
}
