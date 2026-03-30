use std::io;
use std::path::PathBuf;

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Replication action types.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationAction {
    /// Replace an entire file.
    Replace { path: PathBuf, data: Bytes },
    /// Append data to a file.
    Append { path: PathBuf, data: Bytes },
    /// Delete a file.
    Delete { path: PathBuf },
}

// Action type tags
const TAG_REPLACE: u8 = 1;
const TAG_APPEND: u8 = 2;
const TAG_DELETE: u8 = 3;

impl ReplicationAction {
    /// Encode an action to bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Replace { path, data } => {
                buf.put_u8(TAG_REPLACE);
                encode_path(buf, path);
                buf.put_u32(data.len() as u32);
                buf.extend_from_slice(data);
            }
            Self::Append { path, data } => {
                buf.put_u8(TAG_APPEND);
                encode_path(buf, path);
                buf.put_u32(data.len() as u32);
                buf.extend_from_slice(data);
            }
            Self::Delete { path } => {
                buf.put_u8(TAG_DELETE);
                encode_path(buf, path);
            }
        }
    }

    /// Decode an action from bytes. Returns `Ok(None)` when the buffer is empty,
    /// or `Err` if insufficient data remains for a complete action.
    pub fn decode(buf: &mut Bytes) -> Result<Option<Self>, io::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }
        let tag = buf.get_u8();
        match tag {
            TAG_REPLACE => {
                let path = decode_path(buf).ok_or_else(|| eof("Replace: missing path"))?;
                check_remaining(buf, 4, "Replace: missing data length")?;
                let len = buf.get_u32() as usize;
                check_remaining(buf, len, "Replace: truncated data")?;
                let data = buf.split_to(len);
                Ok(Some(Self::Replace { path, data }))
            }
            TAG_APPEND => {
                let path = decode_path(buf).ok_or_else(|| eof("Append: missing path"))?;
                check_remaining(buf, 4, "Append: missing data length")?;
                let len = buf.get_u32() as usize;
                check_remaining(buf, len, "Append: truncated data")?;
                let data = buf.split_to(len);
                Ok(Some(Self::Append { path, data }))
            }
            TAG_DELETE => {
                let path = decode_path(buf).ok_or_else(|| eof("Delete: missing path"))?;
                Ok(Some(Self::Delete { path }))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown action tag: {tag}"),
            )),
        }
    }

    /// Encoded byte size.
    pub fn encoded_size(&self) -> usize {
        match self {
            Self::Replace { path, data } => 1 + 2 + path_len(path) + 4 + data.len(),
            Self::Append { path, data } => 1 + 2 + path_len(path) + 4 + data.len(),
            Self::Delete { path } => 1 + 2 + path_len(path),
        }
    }
}

fn eof(detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("malformed action: {detail}"),
    )
}

fn check_remaining(buf: &Bytes, needed: usize, detail: &str) -> Result<(), io::Error> {
    if buf.remaining() < needed {
        Err(eof(detail))
    } else {
        Ok(())
    }
}

fn encode_path(buf: &mut BytesMut, path: &PathBuf) {
    let s = path.to_string_lossy();
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn decode_path(buf: &mut Bytes) -> Option<PathBuf> {
    if buf.remaining() < 2 {
        return None;
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return None;
    }
    let data = buf.split_to(len);
    let s = String::from_utf8(data.to_vec()).ok()?;
    Some(PathBuf::from(s))
}

fn path_len(path: &PathBuf) -> usize {
    path.to_string_lossy().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(action: &ReplicationAction) -> ReplicationAction {
        let mut buf = BytesMut::new();
        action.encode(&mut buf);
        let mut bytes = buf.freeze();
        ReplicationAction::decode(&mut bytes).unwrap().unwrap()
    }

    #[test]
    fn test_replace_round_trip() {
        let action = ReplicationAction::Replace {
            path: PathBuf::from("data/msgs.0000000001"),
            data: Bytes::from_static(b"file contents"),
        };
        assert_eq!(round_trip(&action), action);
    }

    #[test]
    fn test_append_round_trip() {
        let action = ReplicationAction::Append {
            path: PathBuf::from("data/msgs.0000000001"),
            data: Bytes::from_static(b"new data"),
        };
        assert_eq!(round_trip(&action), action);
    }

    #[test]
    fn test_delete_round_trip() {
        let action = ReplicationAction::Delete {
            path: PathBuf::from("data/msgs.0000000001"),
        };
        assert_eq!(round_trip(&action), action);
    }

    #[test]
    fn test_encoded_size() {
        let action = ReplicationAction::Replace {
            path: PathBuf::from("test"),
            data: Bytes::from_static(b"hello"),
        };
        let mut buf = BytesMut::new();
        action.encode(&mut buf);
        assert_eq!(buf.len(), action.encoded_size());
    }

    #[test]
    fn test_decode_empty_buffer() {
        let mut buf = Bytes::new();
        assert!(ReplicationAction::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_decode_replace_truncated_data() {
        let mut buf = BytesMut::new();
        buf.put_u8(TAG_REPLACE);
        buf.put_u16(4); // path length
        buf.put_slice(b"test");
        buf.put_u32(100); // claims 100 bytes of data
        // but only 0 bytes follow
        let mut bytes = buf.freeze();
        let err = ReplicationAction::decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_decode_append_missing_data_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(TAG_APPEND);
        buf.put_u16(4); // path length
        buf.put_slice(b"test");
        // missing data length field
        let mut bytes = buf.freeze();
        let err = ReplicationAction::decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_decode_delete_missing_path() {
        let mut buf = BytesMut::new();
        buf.put_u8(TAG_DELETE);
        // missing path entirely
        let mut bytes = buf.freeze();
        let err = ReplicationAction::decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_decode_unknown_tag() {
        let mut buf = BytesMut::new();
        buf.put_u8(255); // unknown tag
        let mut bytes = buf.freeze();
        let err = ReplicationAction::decode(&mut bytes).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
