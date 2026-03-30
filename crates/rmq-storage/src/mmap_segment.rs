use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;

/// Default segment size: 8 MB.
pub const DEFAULT_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

/// A memory-mapped append-only segment file.
pub struct MmapSegment {
    path: PathBuf,
    mmap: MmapMut,
    /// Current write position (also the size of valid data).
    size: usize,
    /// Total capacity of the file.
    capacity: usize,
}

impl MmapSegment {
    /// Create a new segment file with the given capacity.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(capacity as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // Hint the kernel for sequential write access
        #[cfg(unix)]
        {
            let _ = mmap.advise(memmap2::Advice::Sequential);
        }

        Ok(Self {
            path,
            mmap,
            size: 0,
            capacity,
        })
    }

    /// Open an existing segment file for reading.
    pub fn open_read(path: impl AsRef<Path>, valid_size: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let metadata = file.metadata()?;
        let capacity = metadata.len() as usize;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            path,
            mmap,
            size: valid_size,
            capacity,
        })
    }

    /// Append data to the segment. Returns the position where data was written.
    /// Note: does NOT fsync. Call `flush()` or `flush_async()` to ensure durability.
    pub fn append(&mut self, data: &[u8]) -> io::Result<u32> {
        if self.size + data.len() > self.capacity {
            return Err(io::Error::other("segment capacity exceeded"));
        }
        let pos = self.size as u32;
        self.mmap[self.size..self.size + data.len()].copy_from_slice(data);
        self.size += data.len();
        Ok(pos)
    }

    /// Append data and immediately flush to disk (synchronous msync).
    /// Use for durable messages where persistence must be guaranteed before ack.
    pub fn append_durable(&mut self, data: &[u8]) -> io::Result<u32> {
        let pos = self.append(data)?;
        self.flush()?;
        Ok(pos)
    }

    /// Read data at the given position and length.
    pub fn read(&self, position: u32, len: usize) -> Option<&[u8]> {
        let start = position as usize;
        let end = start + len;
        if end > self.size {
            return None;
        }
        Some(&self.mmap[start..end])
    }

    /// Get a slice of the valid data region.
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..self.size]
    }

    /// Current write position / valid data size.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Total capacity of the segment.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Remaining space in the segment.
    pub fn remaining(&self) -> usize {
        self.capacity - self.size
    }

    /// Whether the segment has enough room for the given number of bytes.
    pub fn has_room(&self, needed: usize) -> bool {
        self.remaining() >= needed
    }

    /// Flush changes to disk (async msync).
    pub fn flush_async(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    /// Flush changes to disk (sync msync).
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Advise the kernel that we're done with this region (for read segments
    /// that have been fully consumed). Frees page cache.
    #[cfg(unix)]
    pub fn advise_dontneed(&self) -> io::Result<()> {
        unsafe {
            self.mmap
                .unchecked_advise(memmap2::UncheckedAdvice::DontNeed)?;
        }
        Ok(())
    }

    /// Truncate the file to the actual data size (on close).
    pub fn truncate_to_size(&self) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(self.size as u64)?;
        Ok(())
    }

    /// Path to the segment file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for MmapSegment {
    fn drop(&mut self) {
        // Best-effort truncate on drop
        let _ = self.truncate_to_size();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_append() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");
        let mut seg = MmapSegment::create(&path, 1024).unwrap();

        assert_eq!(seg.size(), 0);
        assert_eq!(seg.capacity(), 1024);

        let pos = seg.append(b"hello").unwrap();
        assert_eq!(pos, 0);
        assert_eq!(seg.size(), 5);

        let pos2 = seg.append(b" world").unwrap();
        assert_eq!(pos2, 5);
        assert_eq!(seg.size(), 11);

        assert_eq!(seg.read(0, 5), Some(b"hello".as_slice()));
        assert_eq!(seg.read(5, 6), Some(b" world".as_slice()));
    }

    #[test]
    fn test_capacity_exceeded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");
        let mut seg = MmapSegment::create(&path, 10).unwrap();

        seg.append(b"12345").unwrap();
        assert!(seg.append(b"123456").is_err());
    }

    #[test]
    fn test_truncate_on_drop() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");
        {
            let mut seg = MmapSegment::create(&path, 1024).unwrap();
            seg.append(b"short data").unwrap();
        }
        // After drop, file should be truncated to actual data size
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 10);
    }

    #[test]
    fn test_read_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.seg");
        let mut seg = MmapSegment::create(&path, 1024).unwrap();
        seg.append(b"hello").unwrap();

        assert!(seg.read(0, 6).is_none()); // past valid size
        assert!(seg.read(3, 2).is_some()); // just at boundary (3+2=5=size)
        assert!(seg.read(3, 3).is_none()); // past boundary (3+3=6>5)
    }
}
