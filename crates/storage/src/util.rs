//! Safe byte-parsing helpers.
//!
//! These replace `try_into().unwrap()` patterns with bounds-checked reads
//! that return `io::Result` on truncated data.

use std::io;

use crate::error::StorageError;

/// Read a little-endian `u16` from `data` at `offset`.
pub fn read_u16_le(data: &[u8], offset: usize) -> io::Result<u16> {
    let end = offset.saturating_add(2);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 2,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u16::from_le_bytes([data[offset], data[offset + 1]]))
}

/// Read a little-endian `u32` from `data` at `offset`.
pub fn read_u32_le(data: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset.saturating_add(4);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 4,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]))
}

/// Read a little-endian `u64` from `data` at `offset`.
pub fn read_u64_le(data: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset.saturating_add(8);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 8,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]))
}

/// Read a big-endian `u32` from `data` at `offset`.
pub fn read_u32_be(data: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset.saturating_add(4);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 4,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]))
}

/// Read a big-endian `u64` from `data` at `offset`.
pub fn read_u64_be(data: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset.saturating_add(8);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 8,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u64::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]))
}

/// Read a little-endian `u16` from `data` at `offset`, returning a big-endian value.
pub fn read_u16_be(data: &[u8], offset: usize) -> io::Result<u16> {
    let end = offset.saturating_add(2);
    if end > data.len() {
        return Err(StorageError::DataTruncated {
            offset,
            needed: 2,
            available: data.len().saturating_sub(offset),
        }
        .into());
    }
    Ok(u16::from_be_bytes([data[offset], data[offset + 1]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_u16_le_ok() {
        let data = [0x01, 0x02];
        assert_eq!(read_u16_le(&data, 0).unwrap(), 0x0201);
    }

    #[test]
    fn read_u16_le_at_offset() {
        let data = [0x00, 0x01, 0x02];
        assert_eq!(read_u16_le(&data, 1).unwrap(), 0x0201);
    }

    #[test]
    fn read_u16_le_truncated() {
        let data = [0x01];
        assert!(read_u16_le(&data, 0).is_err());
    }

    #[test]
    fn read_u32_le_ok() {
        let data = 0x04030201u32.to_le_bytes();
        assert_eq!(read_u32_le(&data, 0).unwrap(), 0x04030201);
    }

    #[test]
    fn read_u32_le_truncated() {
        let data = [0x01, 0x02, 0x03];
        assert!(read_u32_le(&data, 0).is_err());
    }

    #[test]
    fn read_u64_le_ok() {
        let data = 42u64.to_le_bytes();
        assert_eq!(read_u64_le(&data, 0).unwrap(), 42);
    }

    #[test]
    fn read_u64_le_truncated() {
        let data = [0u8; 7];
        assert!(read_u64_le(&data, 0).is_err());
    }

    #[test]
    fn read_u64_be_ok() {
        let data = 42u64.to_be_bytes();
        assert_eq!(read_u64_be(&data, 0).unwrap(), 42);
    }

    #[test]
    fn offset_overflow() {
        let data = [0x01, 0x02];
        assert!(read_u16_le(&data, usize::MAX).is_err());
    }

    #[test]
    fn empty_slice() {
        assert!(read_u16_le(&[], 0).is_err());
        assert!(read_u32_le(&[], 0).is_err());
        assert!(read_u64_le(&[], 0).is_err());
    }
}
