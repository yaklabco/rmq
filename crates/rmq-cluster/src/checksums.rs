use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use sha1::{Digest, Sha1};

/// Compute SHA1 checksum of a file.
pub fn sha1_file(path: impl AsRef<Path>) -> io::Result<[u8; 20]> {
    let data = std::fs::read(path)?;
    let mut hasher = Sha1::new();
    hasher.update(&data);
    Ok(hasher.finalize().into())
}

/// Compute SHA1 checksum of a byte slice.
pub fn sha1_bytes(data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Format a SHA1 hash as a hex string.
pub fn sha1_hex(hash: &[u8; 20]) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}

/// Scan a directory and compute SHA1 checksums for all files.
pub fn scan_directory(dir: impl AsRef<Path>) -> io::Result<HashMap<PathBuf, [u8; 20]>> {
    let dir = dir.as_ref();
    let mut checksums = HashMap::new();
    scan_recursive(dir, dir, &mut checksums)?;
    Ok(checksums)
}

fn scan_recursive(
    base: &Path,
    dir: &Path,
    checksums: &mut HashMap<PathBuf, [u8; 20]>,
) -> io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let rel = path.strip_prefix(base).unwrap_or(&path).to_path_buf();
            let hash = sha1_file(&path)?;
            checksums.insert(rel, hash);
        } else if path.is_dir() {
            scan_recursive(base, &path, checksums)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sha1_bytes() {
        let hash = sha1_bytes(b"hello world");
        let hex = sha1_hex(&hash);
        assert_eq!(hex, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[test]
    fn test_sha1_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"hello world").unwrap();
        let hash = sha1_file(&path).unwrap();
        assert_eq!(sha1_hex(&hash), "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[test]
    fn test_scan_directory() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"aaa").unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("b.txt"), b"bbb").unwrap();

        let checksums = scan_directory(dir.path()).unwrap();
        assert_eq!(checksums.len(), 2);
        assert!(checksums.contains_key(&PathBuf::from("a.txt")));
        assert!(checksums.contains_key(&PathBuf::from("sub/b.txt")));
    }
}
