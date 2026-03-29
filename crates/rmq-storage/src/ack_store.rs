use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Tracks acknowledged (deleted) message positions within a segment.
///
/// Each acknowledged position is a u32 byte offset into the segment file.
/// Positions are written to an append-only ack file and kept in memory
/// as a sorted set for O(log n) lookup.
pub struct AckStore {
    path: PathBuf,
    file: File,
    deleted: BTreeSet<u32>,
}

impl AckStore {
    /// Create or open an ack file for the given segment.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)?;

        // Load existing acks from file
        let mut deleted = BTreeSet::new();
        let mut buf = Vec::new();
        // Read from start
        let mut reader = OpenOptions::new().read(true).open(&path)?;
        reader.read_to_end(&mut buf)?;

        let mut offset = 0;
        while offset + 4 <= buf.len() {
            let pos = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);
            deleted.insert(pos);
            offset += 4;
        }

        // Truncate any partial write
        if offset != buf.len() {
            file.set_len(offset as u64)?;
        }

        Ok(Self {
            path,
            file,
            deleted,
        })
    }

    /// Mark a message position as acknowledged.
    pub fn ack(&mut self, position: u32) -> io::Result<bool> {
        if !self.deleted.insert(position) {
            return Ok(false); // already acked
        }
        self.file.write_all(&position.to_le_bytes())?;
        Ok(true)
    }

    /// Check if a position has been acknowledged.
    pub fn is_acked(&self, position: u32) -> bool {
        self.deleted.contains(&position)
    }

    /// Number of acknowledged positions.
    pub fn count(&self) -> usize {
        self.deleted.len()
    }

    /// Path to the ack file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush the ack file to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_ack_and_lookup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("acks.0000000001");
        let mut store = AckStore::open(&path).unwrap();

        assert!(!store.is_acked(0));
        assert!(store.ack(0).unwrap());
        assert!(store.is_acked(0));
        assert_eq!(store.count(), 1);

        // Double ack returns false
        assert!(!store.ack(0).unwrap());
        assert_eq!(store.count(), 1);
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("acks.0000000001");

        {
            let mut store = AckStore::open(&path).unwrap();
            store.ack(100).unwrap();
            store.ack(200).unwrap();
            store.ack(300).unwrap();
            store.flush().unwrap();
        }

        // Reopen and verify
        let store = AckStore::open(&path).unwrap();
        assert!(store.is_acked(100));
        assert!(store.is_acked(200));
        assert!(store.is_acked(300));
        assert!(!store.is_acked(400));
        assert_eq!(store.count(), 3);
    }
}
