use std::collections::HashMap;
use std::path::PathBuf;

use parking_lot::RwLock;

use crate::checksums::sha1_bytes;
#[cfg(test)]
use crate::checksums::sha1_hex;

/// Tracks files and their checksums for replication.
pub struct FileIndex {
    files: RwLock<HashMap<PathBuf, FileEntry>>,
}

struct FileEntry {
    checksum: [u8; 20],
    size: u64,
}

impl FileIndex {
    pub fn new() -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
        }
    }

    /// Register a file with its data.
    pub fn register(&self, path: PathBuf, data: &[u8]) {
        let checksum = sha1_bytes(data);
        self.files.write().insert(
            path,
            FileEntry {
                checksum,
                size: data.len() as u64,
            },
        );
    }

    /// Update a file's checksum after append.
    pub fn update(&self, path: &PathBuf, full_data: &[u8]) {
        let checksum = sha1_bytes(full_data);
        if let Some(entry) = self.files.write().get_mut(path) {
            entry.checksum = checksum;
            entry.size = full_data.len() as u64;
        }
    }

    /// Remove a file from the index.
    pub fn remove(&self, path: &PathBuf) {
        self.files.write().remove(path);
    }

    /// Get the checksum for a file.
    pub fn checksum(&self, path: &PathBuf) -> Option<[u8; 20]> {
        self.files.read().get(path).map(|e| e.checksum)
    }

    /// Get all files and their checksums.
    pub fn all_checksums(&self) -> Vec<(PathBuf, [u8; 20])> {
        self.files
            .read()
            .iter()
            .map(|(p, e)| (p.clone(), e.checksum))
            .collect()
    }

    /// Find files that differ between this index and a remote's checksums.
    pub fn diff(&self, remote: &[(PathBuf, [u8; 20])]) -> SyncPlan {
        let local = self.files.read();
        let remote_map: HashMap<&PathBuf, &[u8; 20]> = remote.iter().map(|(p, h)| (p, h)).collect();

        let mut to_send = Vec::new();
        let mut to_delete = Vec::new();

        // Files we have that remote doesn't, or differ
        for (path, entry) in local.iter() {
            match remote_map.get(path) {
                Some(remote_hash) if **remote_hash == entry.checksum => {}
                _ => to_send.push(path.clone()),
            }
        }

        // Files remote has that we don't
        for (path, _) in remote {
            if !local.contains_key(path) {
                to_delete.push(path.clone());
            }
        }

        SyncPlan { to_send, to_delete }
    }

    pub fn file_count(&self) -> usize {
        self.files.read().len()
    }
}

impl Default for FileIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Plan for synchronizing a follower.
#[derive(Debug)]
pub struct SyncPlan {
    /// Files to send to the follower (new or changed).
    pub to_send: Vec<PathBuf>,
    /// Files to delete on the follower.
    pub to_delete: Vec<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_checksum() {
        let index = FileIndex::new();
        index.register(PathBuf::from("test.txt"), b"hello");
        let hash = index.checksum(&PathBuf::from("test.txt")).unwrap();
        assert_eq!(sha1_hex(&hash), sha1_hex(&sha1_bytes(b"hello")));
    }

    #[test]
    fn test_diff_new_file() {
        let index = FileIndex::new();
        index.register(PathBuf::from("a.txt"), b"aaa");
        index.register(PathBuf::from("b.txt"), b"bbb");

        // Remote has nothing
        let plan = index.diff(&[]);
        assert_eq!(plan.to_send.len(), 2);
        assert!(plan.to_delete.is_empty());
    }

    #[test]
    fn test_diff_changed_file() {
        let index = FileIndex::new();
        index.register(PathBuf::from("a.txt"), b"new content");

        let remote = vec![(PathBuf::from("a.txt"), sha1_bytes(b"old content"))];
        let plan = index.diff(&remote);
        assert_eq!(plan.to_send.len(), 1);
        assert_eq!(plan.to_send[0], PathBuf::from("a.txt"));
    }

    #[test]
    fn test_diff_identical() {
        let index = FileIndex::new();
        index.register(PathBuf::from("a.txt"), b"same");

        let remote = vec![(PathBuf::from("a.txt"), sha1_bytes(b"same"))];
        let plan = index.diff(&remote);
        assert!(plan.to_send.is_empty());
        assert!(plan.to_delete.is_empty());
    }

    #[test]
    fn test_diff_remote_extra() {
        let index = FileIndex::new();
        index.register(PathBuf::from("a.txt"), b"aaa");

        let remote = vec![
            (PathBuf::from("a.txt"), sha1_bytes(b"aaa")),
            (PathBuf::from("orphan.txt"), sha1_bytes(b"xxx")),
        ];
        let plan = index.diff(&remote);
        assert!(plan.to_send.is_empty());
        assert_eq!(plan.to_delete.len(), 1);
        assert_eq!(plan.to_delete[0], PathBuf::from("orphan.txt"));
    }

    #[test]
    fn test_remove() {
        let index = FileIndex::new();
        index.register(PathBuf::from("a.txt"), b"aaa");
        assert_eq!(index.file_count(), 1);
        index.remove(&PathBuf::from("a.txt"));
        assert_eq!(index.file_count(), 0);
    }
}
