use std::io;
use std::path::{Path, PathBuf};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lz4_flex::compress_prepend_size;
use lz4_flex::decompress_size_prepended;
use tracing::{debug, info};

use crate::actions::ReplicationAction;
use crate::file_index::FileIndex;

/// Verify that the resolved path stays within `data_dir`.
fn validate_path(data_dir: &Path, path: &Path) -> io::Result<std::path::PathBuf> {
    let full_path = data_dir.join(path);
    // Create parent dirs so canonicalize can resolve
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let canonical = full_path.canonicalize().unwrap_or_else(|_| {
        // For new files, canonicalize the parent and append the file name
        if let (Some(parent), Some(file_name)) = (full_path.parent(), full_path.file_name()) {
            if let Ok(canonical_parent) = parent.canonicalize() {
                return canonical_parent.join(file_name);
            }
        }
        full_path.clone()
    });
    let canonical_data_dir = data_dir.canonicalize()?;
    if !canonical.starts_with(&canonical_data_dir) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "path traversal detected: '{}' escapes data directory",
                path.display()
            ),
        ));
    }
    Ok(canonical)
}

/// Apply a replication action to a data directory.
pub fn apply_action(data_dir: &Path, action: &ReplicationAction) -> io::Result<()> {
    match action {
        ReplicationAction::Replace { path, data } => {
            let full_path = validate_path(data_dir, path)?;
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&full_path, data)?;
            debug!("replicated: replace {}", path.display());
        }
        ReplicationAction::Append { path, data } => {
            let full_path = validate_path(data_dir, path)?;
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full_path)?;
            file.write_all(data)?;
            debug!(
                "replicated: append {} bytes to {}",
                data.len(),
                path.display()
            );
        }
        ReplicationAction::Delete { path } => {
            let full_path = validate_path(data_dir, path)?;
            if full_path.exists() {
                std::fs::remove_file(&full_path)?;
                debug!("replicated: delete {}", path.display());
            }
        }
    }
    Ok(())
}

/// Compress a batch of replication actions using LZ4.
/// Prepends a u32 action count header before the action data.
pub fn compress_actions(actions: &[ReplicationAction]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u32(actions.len() as u32); // action count header
    for action in actions {
        action.encode(&mut buf);
    }
    let compressed = compress_prepend_size(&buf);
    Bytes::from(compressed)
}

/// Decompress and decode a batch of replication actions.
/// Validates the action count header matches the number of decoded actions.
pub fn decompress_actions(data: &[u8]) -> io::Result<Vec<ReplicationAction>> {
    let decompressed = decompress_size_prepended(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    let mut buf = Bytes::from(decompressed);
    if buf.remaining() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing action count header",
        ));
    }
    let expected_count = buf.get_u32() as usize;

    let mut actions = Vec::with_capacity(expected_count);
    while buf.has_remaining() {
        match ReplicationAction::decode(&mut buf) {
            Ok(Some(action)) => actions.push(action),
            Ok(None) => break,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "partial decode: decoded {} of {} expected actions: {e}",
                        actions.len(),
                        expected_count
                    ),
                ));
            }
        }
    }

    if actions.len() != expected_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "action count mismatch: expected {expected_count}, decoded {}",
                actions.len()
            ),
        ));
    }

    Ok(actions)
}

/// Perform a full sync from leader to follower directory.
/// Compares checksums and copies only changed files.
pub fn full_sync(
    leader_dir: &Path,
    follower_dir: &Path,
    leader_index: &FileIndex,
    follower_checksums: &[(PathBuf, [u8; 20])],
) -> io::Result<SyncStats> {
    let plan = leader_index.diff(follower_checksums);
    let mut stats = SyncStats::default();

    // Send new/changed files
    for path in &plan.to_send {
        let src = leader_dir.join(path);
        let dst = follower_dir.join(path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(&src, &dst)?;
        stats.files_sent += 1;
        stats.bytes_sent += std::fs::metadata(&dst)?.len();
    }

    // Delete orphaned files on follower
    for path in &plan.to_delete {
        let dst = follower_dir.join(path);
        if dst.exists() {
            std::fs::remove_file(&dst)?;
            stats.files_deleted += 1;
        }
    }

    info!(
        "full sync: sent {} files ({} bytes), deleted {} files",
        stats.files_sent, stats.bytes_sent, stats.files_deleted
    );

    Ok(stats)
}

#[derive(Debug, Default)]
pub struct SyncStats {
    pub files_sent: u32,
    pub bytes_sent: u64,
    pub files_deleted: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksums::sha1_bytes;
    use tempfile::TempDir;

    #[test]
    fn test_apply_replace() {
        let dir = TempDir::new().unwrap();
        let action = ReplicationAction::Replace {
            path: PathBuf::from("sub/test.txt"),
            data: Bytes::from_static(b"hello"),
        };
        apply_action(dir.path(), &action).unwrap();
        let content = std::fs::read(dir.path().join("sub/test.txt")).unwrap();
        assert_eq!(content, b"hello");
    }

    #[test]
    fn test_apply_append() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("append.txt");
        std::fs::write(&path, b"first").unwrap();

        let action = ReplicationAction::Append {
            path: PathBuf::from("append.txt"),
            data: Bytes::from_static(b"-second"),
        };
        apply_action(dir.path(), &action).unwrap();
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"first-second");
    }

    #[test]
    fn test_apply_delete() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("delete_me.txt");
        std::fs::write(&path, b"bye").unwrap();

        let action = ReplicationAction::Delete {
            path: PathBuf::from("delete_me.txt"),
        };
        apply_action(dir.path(), &action).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_compress_decompress_actions() {
        let actions = vec![
            ReplicationAction::Replace {
                path: PathBuf::from("a.txt"),
                data: Bytes::from_static(b"content-a"),
            },
            ReplicationAction::Append {
                path: PathBuf::from("b.txt"),
                data: Bytes::from_static(b"more-data"),
            },
            ReplicationAction::Delete {
                path: PathBuf::from("c.txt"),
            },
        ];

        let compressed = compress_actions(&actions);
        let decompressed = decompress_actions(&compressed).unwrap();
        assert_eq!(decompressed, actions);
    }

    #[test]
    fn test_path_traversal_blocked() {
        let dir = TempDir::new().unwrap();
        let action = ReplicationAction::Replace {
            path: PathBuf::from("../../etc/passwd"),
            data: Bytes::from_static(b"pwned"),
        };
        let err = apply_action(dir.path(), &action).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("path traversal"));
    }

    #[test]
    fn test_path_traversal_absolute_path() {
        let dir = TempDir::new().unwrap();
        let action = ReplicationAction::Delete {
            path: PathBuf::from("/tmp/should_not_delete"),
        };
        let err = apply_action(dir.path(), &action).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_decompress_actions_count_mismatch() {
        // Manually create compressed data with wrong count header
        let mut buf = BytesMut::new();
        buf.put_u32(5); // claim 5 actions
        // but encode only 1
        let action = ReplicationAction::Delete {
            path: PathBuf::from("a.txt"),
        };
        action.encode(&mut buf);
        let compressed = compress_prepend_size(&buf);
        let err = decompress_actions(&compressed).unwrap_err();
        assert!(err.to_string().contains("count mismatch"));
    }

    #[test]
    fn test_decompress_actions_partial_decode() {
        // Create compressed data with correct count but truncated action data
        let mut buf = BytesMut::new();
        buf.put_u32(1); // 1 action expected
        buf.put_u8(1); // TAG_REPLACE
        // path but no data length/data - truncated
        buf.put_u16(4);
        buf.put_slice(b"test");
        // missing data length
        let compressed = compress_prepend_size(&buf);
        let err = decompress_actions(&compressed).unwrap_err();
        assert!(err.to_string().contains("partial decode"));
    }

    #[test]
    fn test_full_sync() {
        let leader_dir = TempDir::new().unwrap();
        let follower_dir = TempDir::new().unwrap();

        // Create files on leader
        std::fs::write(leader_dir.path().join("a.txt"), b"aaa").unwrap();
        std::fs::write(leader_dir.path().join("b.txt"), b"bbb").unwrap();

        // Create old file on follower that leader doesn't have
        std::fs::write(follower_dir.path().join("orphan.txt"), b"old").unwrap();

        // Build leader index
        let leader_index = FileIndex::new();
        leader_index.register(PathBuf::from("a.txt"), b"aaa");
        leader_index.register(PathBuf::from("b.txt"), b"bbb");

        // Follower has orphan + old version of a.txt
        let follower_checksums = vec![
            (PathBuf::from("a.txt"), sha1_bytes(b"old-aaa")),
            (PathBuf::from("orphan.txt"), sha1_bytes(b"old")),
        ];

        let stats = full_sync(
            leader_dir.path(),
            follower_dir.path(),
            &leader_index,
            &follower_checksums,
        )
        .unwrap();

        assert_eq!(stats.files_sent, 2); // a.txt (changed) + b.txt (new)
        assert_eq!(stats.files_deleted, 1); // orphan.txt

        // Verify follower state
        assert_eq!(
            std::fs::read(follower_dir.path().join("a.txt")).unwrap(),
            b"aaa"
        );
        assert_eq!(
            std::fs::read(follower_dir.path().join("b.txt")).unwrap(),
            b"bbb"
        );
        assert!(!follower_dir.path().join("orphan.txt").exists());
    }
}
