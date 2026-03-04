use std::path::Path;

use crate::types::*;
use crate::buffer_pool::{BufferPool, BufferPoolError, PageIO};
use crate::page::SlottedPage;
use crate::file_header::FileHeader;

const DWB_HEADER_SIZE: usize = 16;

/// DWB header: 16 bytes.
#[derive(Debug, Clone)]
pub struct DwbHeader {
    pub magic: u32,
    pub version: u16,
    pub page_size: u16,
    pub page_count: u32,
    pub checksum: u32,
}

impl DwbHeader {
    fn serialize(&self) -> [u8; DWB_HEADER_SIZE] {
        let mut buf = [0u8; DWB_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.page_size.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_count.to_le_bytes());
        let crc = crc32c::crc32c(&buf[..12]);
        buf[12..16].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    fn deserialize(buf: &[u8; DWB_HEADER_SIZE]) -> Result<Self, std::io::Error> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != DWB_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid DWB magic",
            ));
        }
        let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let page_size = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let page_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let computed_crc = crc32c::crc32c(&buf[..12]);
        if stored_crc != computed_crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "DWB header checksum mismatch",
            ));
        }
        Ok(Self {
            magic,
            version,
            page_size,
            page_count,
            checksum: stored_crc,
        })
    }
}

/// Write dirty pages to the double-write buffer file.
pub fn write_dwb(
    dwb_path: &Path,
    page_size: u32,
    pages: &[(PageId, Vec<u8>)],
) -> Result<(), std::io::Error> {
    use std::io::Write;

    let header = DwbHeader {
        magic: DWB_MAGIC,
        version: 1,
        page_size: page_size as u16,
        page_count: pages.len() as u32,
        checksum: 0, // filled by serialize
    };

    let mut file = std::fs::File::create(dwb_path)?;
    file.write_all(&header.serialize())?;

    for (page_id, data) in pages {
        file.write_all(&page_id.0.to_le_bytes())?;
        file.write_all(data)?;
    }

    file.sync_all()?;
    Ok(())
}

/// Read the DWB file for recovery.
pub fn read_dwb(
    dwb_path: &Path,
    page_size: u32,
) -> Result<Vec<(PageId, Vec<u8>)>, std::io::Error> {
    use std::io::Read;

    let mut file = std::fs::File::open(dwb_path)?;
    let mut header_buf = [0u8; DWB_HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    let header = DwbHeader::deserialize(&header_buf)?;

    if header.page_size as u32 != page_size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "DWB page size mismatch",
        ));
    }

    let mut entries = Vec::with_capacity(header.page_count as usize);
    for _ in 0..header.page_count {
        let mut pid_buf = [0u8; 4];
        file.read_exact(&mut pid_buf)?;
        let page_id = PageId(u32::from_le_bytes(pid_buf));

        let mut data = vec![0u8; page_size as usize];
        file.read_exact(&mut data)?;
        entries.push((page_id, data));
    }

    Ok(entries)
}

/// Truncate the DWB file to zero length. fsync.
pub fn clear_dwb(dwb_path: &Path) -> Result<(), std::io::Error> {
    if dwb_path.exists() {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(dwb_path)?;
        file.sync_all()?;
    }
    Ok(())
}

/// Checkpoint configuration.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub wal_size_threshold: u64,
    pub interval: std::time::Duration,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            wal_size_threshold: 64 * 1024 * 1024,
            interval: std::time::Duration::from_secs(300),
        }
    }
}

/// Execute a fuzzy checkpoint.
///
/// Steps:
/// 1. Snapshot dirty frames (in-memory, no I/O).
/// 2. Write snapshot to DWB.
/// 3. Scatter-write pages to data file.
/// 4. fsync data file.
/// 5. Clear DWB.
/// 6. Update file header.
pub fn run_checkpoint(
    db_dir: &Path,
    pool: &BufferPool,
    file_header: &mut FileHeader,
    checkpoint_lsn: Lsn,
) -> Result<(), BufferPoolError> {
    let page_size = pool.page_size();
    let dwb_path = db_dir.join("data.dwb");

    // 1. Snapshot dirty frames.
    let dirty_snapshot = pool.snapshot_dirty_frames();
    if dirty_snapshot.is_empty() {
        return Ok(());
    }

    // 2. Write to DWB.
    let dwb_pages: Vec<(PageId, Vec<u8>)> = dirty_snapshot
        .iter()
        .map(|(pid, data, _lsn)| (*pid, data.clone()))
        .collect();

    write_dwb(&dwb_path, page_size, &dwb_pages).map_err(BufferPoolError::Io)?;

    // 3. Scatter-write: write each page to its position in data.db.
    let io = pool.io();
    for (page_id, data, lsn) in &dirty_snapshot {
        io.write_page(*page_id, data)?;
        pool.mark_clean_if_lsn(*page_id, *lsn);
    }

    // 4. fsync data file.
    io.sync_file()?;

    // 5. Clear DWB.
    clear_dwb(&dwb_path).map_err(BufferPoolError::Io)?;

    // 6. Update file header.
    file_header.checkpoint_lsn = checkpoint_lsn;
    file_header.page_count = io.page_count();
    let header_buf = file_header.serialize(page_size);
    io.write_page(PageId::FILE_HEADER, &header_buf)?;

    // Write shadow header at last page.
    let shadow_page = PageId((file_header.page_count - 1) as u32);
    io.write_page(shadow_page, &header_buf)?;

    io.sync_file()?;

    Ok(())
}

/// DWB recovery: restore torn pages in data.db.
pub fn recover_dwb(
    db_dir: &Path,
    io: &dyn PageIO,
    page_size: u32,
) -> Result<u32, std::io::Error> {
    let dwb_path = db_dir.join("data.dwb");

    if !dwb_path.exists() || std::fs::metadata(&dwb_path)?.len() == 0 {
        return Ok(0);
    }

    let entries = read_dwb(&dwb_path, page_size)?;
    let mut repaired = 0u32;

    for (page_id, dwb_data) in &entries {
        // Read the page from data.db.
        let mut page_buf = vec![0u8; page_size as usize];
        if io.read_page(*page_id, &mut page_buf).is_ok() {
            // Verify checksum.
            let sp = SlottedPage::new(&page_buf, page_size);
            if sp.verify_checksum() {
                continue; // Page is fine, skip.
            }
        }

        // Torn write detected — restore from DWB.
        let _ = io.write_page(*page_id, dwb_data);
        repaired += 1;
    }

    // fsync and clear DWB.
    let _ = io.sync_file();
    clear_dwb(&dwb_path)?;

    Ok(repaired)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dwb_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test.dwb");
        let page_size = 4096u32;

        let pages = vec![
            (PageId(1), vec![0xAAu8; page_size as usize]),
            (PageId(5), vec![0xBBu8; page_size as usize]),
            (PageId(10), vec![0xCCu8; page_size as usize]),
        ];

        write_dwb(&dwb_path, page_size, &pages).unwrap();
        let read_back = read_dwb(&dwb_path, page_size).unwrap();

        assert_eq!(read_back.len(), 3);
        assert_eq!(read_back[0].0, PageId(1));
        assert_eq!(read_back[0].1, vec![0xAAu8; page_size as usize]);
        assert_eq!(read_back[1].0, PageId(5));
        assert_eq!(read_back[2].0, PageId(10));
    }

    #[test]
    fn test_dwb_clear() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test.dwb");
        let page_size = 4096u32;

        let pages = vec![(PageId(1), vec![0u8; page_size as usize])];
        write_dwb(&dwb_path, page_size, &pages).unwrap();

        clear_dwb(&dwb_path).unwrap();
        let meta = std::fs::metadata(&dwb_path).unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[test]
    fn test_dwb_empty_read() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test.dwb");
        let page_size = 4096u32;

        let pages: Vec<(PageId, Vec<u8>)> = vec![];
        write_dwb(&dwb_path, page_size, &pages).unwrap();
        let read_back = read_dwb(&dwb_path, page_size).unwrap();
        assert!(read_back.is_empty());
    }

    #[test]
    fn test_checkpoint_with_memio() {
        use crate::buffer_pool::MemPageIO;
        use crate::page::SlottedPageMut;

        let dir = tempfile::tempdir().unwrap();
        let io = MemPageIO::new(DEFAULT_PAGE_SIZE, 5);
        // Init page 1 as a leaf.
        {
            let mut buf = vec![0u8; DEFAULT_PAGE_SIZE as usize];
            let mut sp = SlottedPageMut::new(&mut buf, DEFAULT_PAGE_SIZE);
            sp.init(PageId(1), PageType::BTreeLeaf);
            sp.compute_checksum();
            io.write_page(PageId(1), &buf).unwrap();
        }

        let pool = BufferPool::new(Box::new(io), DEFAULT_PAGE_SIZE, 8);

        // Dirty a page.
        {
            let mut guard = pool.fetch_page_exclusive(PageId(1)).unwrap();
            let mut sp = guard.as_slotted_page_mut();
            let cell = vec![0x42u8; 20];
            sp.insert_cell(0, &cell).unwrap();
            sp.set_lsn(Lsn(100));
            sp.compute_checksum();
        }

        // Verify it's dirty.
        assert!(!pool.snapshot_dirty_frames().is_empty());

        // Run checkpoint.
        let mut header = FileHeader::new_database(DEFAULT_PAGE_SIZE, PageId(1));
        header.page_count = 5;
        run_checkpoint(dir.path(), &pool, &mut header, Lsn(100)).unwrap();

        // After checkpoint, dirty frames should be clean.
        assert!(pool.snapshot_dirty_frames().is_empty());
        assert_eq!(header.checkpoint_lsn, Lsn(100));
    }

    #[test]
    fn test_recover_dwb_with_torn_page() {
        use crate::buffer_pool::MemPageIO;
        use crate::page::SlottedPageMut;

        let dir = tempfile::tempdir().unwrap();
        let page_size = DEFAULT_PAGE_SIZE;
        let io = MemPageIO::new(page_size, 5);

        // Create a valid page in DWB.
        let mut good_page = vec![0u8; page_size as usize];
        {
            let mut sp = SlottedPageMut::new(&mut good_page, page_size);
            sp.init(PageId(2), PageType::BTreeLeaf);
            sp.compute_checksum();
        }

        let dwb_pages = vec![(PageId(2), good_page.clone())];
        write_dwb(&dir.path().join("data.dwb"), page_size, &dwb_pages).unwrap();

        // Write a corrupt page to "disk" (memio page 2).
        let corrupt_page = vec![0xFFu8; page_size as usize];
        io.write_page(PageId(2), &corrupt_page).unwrap();

        // Recover.
        let repaired = recover_dwb(dir.path(), &io, page_size).unwrap();
        assert_eq!(repaired, 1);

        // Verify the page was restored.
        let mut buf = vec![0u8; page_size as usize];
        io.read_page(PageId(2), &mut buf).unwrap();
        assert_eq!(buf, good_page);
    }
}
