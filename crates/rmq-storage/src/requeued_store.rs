use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::segment_position::SegmentPosition;

/// Stores requeued message positions in publish order.
///
/// When a message is nacked/rejected with requeue=true, its segment position
/// is inserted back into this store. Messages are consumed from the front.
///
/// Persistence is provided via `save`/`load` using a simple append-only file
/// format: each entry is 12 bytes (segment u32 LE + position u32 LE +
/// bytesize u32 LE), matching the AckStore convention of fixed-size records.
pub struct RequeuedStore {
    queue: VecDeque<SegmentPosition>,
}

/// Size of each serialized entry: 3 x u32 = 12 bytes.
const ENTRY_SIZE: usize = 12;

impl RequeuedStore {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// Requeue a message position, maintaining sorted (publish) order.
    pub fn requeue(&mut self, sp: SegmentPosition) {
        // Insert in sorted position to maintain publish order
        let pos = self.queue.partition_point(|existing| existing < &sp);
        self.queue.insert(pos, sp);
    }

    /// Pop the first (oldest) requeued message, if any.
    pub fn pop_front(&mut self) -> Option<SegmentPosition> {
        self.queue.pop_front()
    }

    /// Peek at the first requeued message without removing it.
    pub fn peek_front(&self) -> Option<&SegmentPosition> {
        self.queue.front()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Save all requeued positions to a file (overwrite).
    /// Uses a simple fixed-size record format for each entry:
    /// [segment: u32 LE][position: u32 LE][bytesize: u32 LE]
    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let mut file = File::create(path)?;
        let mut buf = Vec::with_capacity(self.queue.len() * ENTRY_SIZE);
        for sp in &self.queue {
            buf.extend_from_slice(&sp.segment.to_le_bytes());
            buf.extend_from_slice(&sp.position.to_le_bytes());
            buf.extend_from_slice(&sp.bytesize.to_le_bytes());
        }
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// Load requeued positions from a file. Replaces any existing entries.
    /// Truncates partial records at the end of the file (crash tolerance).
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // Truncate partial record
        let valid_len = (buf.len() / ENTRY_SIZE) * ENTRY_SIZE;
        let mut queue = VecDeque::new();

        let mut offset = 0;
        while offset + ENTRY_SIZE <= valid_len {
            let segment = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);
            let position = u32::from_le_bytes([
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
            ]);
            let bytesize = u32::from_le_bytes([
                buf[offset + 8],
                buf[offset + 9],
                buf[offset + 10],
                buf[offset + 11],
            ]);
            queue.push_back(SegmentPosition::new(segment, position, bytesize));
            offset += ENTRY_SIZE;
        }

        Ok(Self { queue })
    }
}

impl Default for RequeuedStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_requeue_maintains_order() {
        let mut store = RequeuedStore::new();

        // Requeue out of order
        store.requeue(SegmentPosition::new(1, 200, 50));
        store.requeue(SegmentPosition::new(1, 100, 50));
        store.requeue(SegmentPosition::new(1, 300, 50));

        // Should come back in sorted order
        assert_eq!(store.pop_front().unwrap().position, 100);
        assert_eq!(store.pop_front().unwrap().position, 200);
        assert_eq!(store.pop_front().unwrap().position, 300);
        assert!(store.is_empty());
    }

    #[test]
    fn test_requeue_across_segments() {
        let mut store = RequeuedStore::new();

        store.requeue(SegmentPosition::new(2, 0, 50));
        store.requeue(SegmentPosition::new(1, 500, 50));
        store.requeue(SegmentPosition::new(1, 100, 50));

        let first = store.pop_front().unwrap();
        assert_eq!(first.segment, 1);
        assert_eq!(first.position, 100);

        let second = store.pop_front().unwrap();
        assert_eq!(second.segment, 1);
        assert_eq!(second.position, 500);

        let third = store.pop_front().unwrap();
        assert_eq!(third.segment, 2);
        assert_eq!(third.position, 0);
    }

    #[test]
    fn test_save_and_load() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("requeued.dat");

        let mut store = RequeuedStore::new();
        store.requeue(SegmentPosition::new(1, 100, 42));
        store.requeue(SegmentPosition::new(2, 0, 99));
        store.requeue(SegmentPosition::new(1, 500, 77));
        store.save(&path).unwrap();

        let loaded = RequeuedStore::load(&path).unwrap();
        assert_eq!(loaded.len(), 3);

        // Entries should be in the saved order (already sorted by requeue)
        let entries: Vec<_> = loaded.queue.iter().cloned().collect();
        assert_eq!(entries[0], SegmentPosition::new(1, 100, 42));
        assert_eq!(entries[1], SegmentPosition::new(1, 500, 77));
        assert_eq!(entries[2], SegmentPosition::new(2, 0, 99));
    }

    #[test]
    fn test_load_nonexistent_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does_not_exist.dat");
        let store = RequeuedStore::load(&path).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn test_load_truncates_partial_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("partial.dat");

        // Write one full record + some extra bytes
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(&42u32.to_le_bytes());
        buf.extend_from_slice(&[0xFF, 0xFF]); // partial junk
        std::fs::write(&path, &buf).unwrap();

        let loaded = RequeuedStore::load(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        let sp = loaded.queue.front().unwrap();
        assert_eq!(sp.segment, 1);
        assert_eq!(sp.position, 100);
        assert_eq!(sp.bytesize, 42);
    }
}
