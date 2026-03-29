use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use rmq_protocol::field_table::encode_short_string;
use rmq_protocol::properties::BasicProperties;

use crate::ack_store::AckStore;
use crate::message::StoredMessage;
use crate::mmap_segment::{DEFAULT_SEGMENT_SIZE, MmapSegment};
use crate::requeued_store::RequeuedStore;
use crate::segment_position::SegmentPosition;

/// Envelope: a message with its position and redelivered flag.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub segment_position: SegmentPosition,
    pub message: Arc<StoredMessage>,
    pub redelivered: bool,
}

/// Per-segment index of message positions for fast skip-over-acked scanning.
/// Stores (byte_offset, byte_size) for each message in the segment.
struct SegmentIndex {
    /// Sorted list of (position, bytesize) for every message in the segment.
    entries: Vec<(u32, u32)>,
}

impl SegmentIndex {
    fn new() -> Self {
        Self { entries: Vec::new() }
    }

    fn push(&mut self, position: u32, bytesize: u32) {
        self.entries.push((position, bytesize));
    }

    /// Find the index of the first entry at or after `position`.
    fn find_from(&self, position: u32) -> usize {
        self.entries.partition_point(|(pos, _)| *pos < position)
    }
}

/// Persistent message store using memory-mapped segment files.
pub struct MessageStore {
    dir: PathBuf,
    segment_size: usize,

    /// Active write segment.
    write_segment: Option<MmapSegment>,
    write_segment_id: u32,

    /// Read segments cache.
    read_segments: BTreeMap<u32, MmapSegment>,

    /// Per-segment ack tracking.
    ack_stores: BTreeMap<u32, AckStore>,

    /// Per-segment message position index for fast scanning.
    segment_indices: BTreeMap<u32, SegmentIndex>,

    /// Current read position.
    read_segment_id: u32,
    read_position: u32,

    /// Requeued messages.
    requeued: RequeuedStore,

    /// Reusable encode buffer to avoid per-message allocation.
    encode_buf: BytesMut,

    /// Message count and byte size.
    message_count: u64,
    byte_size: u64,
}

impl MessageStore {
    /// Create a new message store in the given directory.
    pub fn new(dir: impl AsRef<Path>, segment_size: usize) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut store = Self {
            dir,
            segment_size,
            write_segment: None,
            write_segment_id: 1,
            read_segments: BTreeMap::new(),
            ack_stores: BTreeMap::new(),
            segment_indices: BTreeMap::new(),
            read_segment_id: 1,
            read_position: 0,
            requeued: RequeuedStore::new(),
            encode_buf: BytesMut::with_capacity(8192),
            message_count: 0,
            byte_size: 0,
        };

        store.load_existing()?;
        Ok(store)
    }

    /// Create with default segment size.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::new(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Push a message into the store. Returns the segment position.
    /// Does NOT fsync — call `flush_async()` or `sync()` separately.
    pub fn push(&mut self, msg: &StoredMessage) -> io::Result<SegmentPosition> {
        self.encode_buf.clear();
        encode_message_to(msg, &mut self.encode_buf);
        let bytesize = self.encode_buf.len() as u32;

        if let Some(ref seg) = self.write_segment {
            if !seg.has_room(self.encode_buf.len()) {
                self.rotate_segment()?;
            }
        }

        if self.write_segment.is_none() {
            self.create_write_segment()?;
        }

        let seg = self.write_segment.as_mut().unwrap();
        let position = seg.append(&self.encode_buf)?;

        let sp = SegmentPosition::new(self.write_segment_id, position, bytesize);
        self.message_count += 1;
        self.byte_size += bytesize as u64;

        // Add to segment index for fast skip-over-acked scanning
        self.segment_indices
            .entry(self.write_segment_id)
            .or_insert_with(SegmentIndex::new)
            .push(position, bytesize);

        Ok(sp)
    }

    /// Read a message at the given segment position.
    pub fn read_at(&mut self, sp: &SegmentPosition) -> io::Result<StoredMessage> {
        let segment = self.get_read_segment(sp.segment)?;
        let data = segment
            .read(sp.position, sp.bytesize as usize)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "invalid segment position"))?;

        decode_message(data)
    }

    /// Get the next message in order (checking requeued first), without removing it.
    pub fn peek(&mut self) -> io::Result<Option<Envelope>> {
        // Check requeued first
        if let Some(sp) = self.requeued.peek_front().copied() {
            let msg = self.read_at(&sp)?;
            return Ok(Some(Envelope {
                segment_position: sp,
                message: Arc::new(msg),
                redelivered: true,
            }));
        }

        self.next_from_segments()
    }

    /// Consume the next message (shift). Returns the envelope or None if empty.
    pub fn shift(&mut self) -> io::Result<Option<Envelope>> {
        // Check requeued first
        if let Some(sp) = self.requeued.pop_front() {
            let msg = self.read_at(&sp)?;
            return Ok(Some(Envelope {
                segment_position: sp,
                message: Arc::new(msg),
                redelivered: true,
            }));
        }

        if let Some(env) = self.next_from_segments()? {
            // Advance read position
            self.read_position = env.segment_position.position + env.segment_position.bytesize;
            Ok(Some(env))
        } else {
            Ok(None)
        }
    }

    /// Acknowledge (delete) a message at the given position.
    pub fn ack(&mut self, sp: &SegmentPosition) -> io::Result<()> {
        let ack_store = self.get_or_create_ack_store(sp.segment)?;
        ack_store.ack(sp.position)?;
        self.message_count = self.message_count.saturating_sub(1);
        self.byte_size = self.byte_size.saturating_sub(sp.bytesize as u64);
        Ok(())
    }

    /// Requeue a message (for nack/reject with requeue=true).
    pub fn requeue(&mut self, sp: SegmentPosition) {
        self.requeued.requeue(sp);
    }

    /// Number of messages in the store.
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Total byte size of messages.
    pub fn byte_size(&self) -> u64 {
        self.byte_size
    }

    /// Flush all segments and ack files to disk.
    /// Sync all data to disk (fsync). Guarantees all writes and acks
    /// survive process crash and power failure.
    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(ref seg) = self.write_segment {
            seg.flush()?; // msync for mmap
        }
        for ack in self.ack_stores.values_mut() {
            ack.sync()?; // fsync for ack files
        }
        Ok(())
    }

    // --- Private ---

    fn segment_path(&self, id: u32) -> PathBuf {
        self.dir.join(format!("msgs.{id:010}"))
    }

    fn ack_path(&self, id: u32) -> PathBuf {
        self.dir.join(format!("acks.{id:010}"))
    }

    fn create_write_segment(&mut self) -> io::Result<()> {
        let path = self.segment_path(self.write_segment_id);
        let seg = MmapSegment::create(&path, self.segment_size)?;
        self.write_segment = Some(seg);
        Ok(())
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        if let Some(seg) = self.write_segment.take() {
            seg.flush()?;
            // Move to read segments cache
            let old_id = self.write_segment_id;
            self.read_segments.insert(old_id, seg);
        }
        self.write_segment_id += 1;
        self.create_write_segment()?;
        Ok(())
    }

    fn get_read_segment(&mut self, segment_id: u32) -> io::Result<&MmapSegment> {
        // Check write segment first
        if segment_id == self.write_segment_id {
            if let Some(ref seg) = self.write_segment {
                return Ok(seg);
            }
        }

        if !self.read_segments.contains_key(&segment_id) {
            let path = self.segment_path(segment_id);
            if !path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment {segment_id} not found"),
                ));
            }
            let metadata = fs::metadata(&path)?;
            let seg = MmapSegment::open_read(&path, metadata.len() as usize)?;
            self.read_segments.insert(segment_id, seg);
        }

        Ok(self.read_segments.get(&segment_id).unwrap())
    }

    fn get_or_create_ack_store(&mut self, segment_id: u32) -> io::Result<&mut AckStore> {
        if !self.ack_stores.contains_key(&segment_id) {
            let path = self.ack_path(segment_id);
            let store = AckStore::open(&path)?;
            self.ack_stores.insert(segment_id, store);
        }
        Ok(self.ack_stores.get_mut(&segment_id).unwrap())
    }

    fn is_acked(&self, segment_id: u32, position: u32) -> bool {
        self.ack_stores
            .get(&segment_id)
            .is_some_and(|store| store.is_acked(position))
    }

    fn next_from_segments(&mut self) -> io::Result<Option<Envelope>> {
        loop {
            let read_seg_id = self.read_segment_id;
            let read_pos = self.read_position;

            // Fast path: use segment index to skip acked messages without
            // touching the mmap data at all
            if let Some(index) = self.segment_indices.get(&read_seg_id) {
                let start_idx = index.find_from(read_pos);
                let mut found = false;

                for &(pos, bytesize) in &index.entries[start_idx..] {
                    if self.is_acked(read_seg_id, pos) {
                        // Skip without reading mmap — just advance past it
                        self.read_position = pos + bytesize;
                        continue;
                    }

                    // Found un-acked message — read its data
                    let data = self.read_segment_data(read_seg_id, pos, bytesize as usize)?;
                    let data = match data {
                        Some(d) => d,
                        None => break,
                    };

                    let message = decode_message(&data)?;
                    self.read_position = pos + bytesize;

                    return Ok(Some(Envelope {
                        segment_position: SegmentPosition::new(read_seg_id, pos, bytesize),
                        message: Arc::new(message),
                        redelivered: false,
                    }));
                }

                // Exhausted this segment's index — move to next
                if self.read_segment_id < self.write_segment_id {
                    self.read_segment_id += 1;
                    self.read_position = 0;
                    continue;
                }
                return Ok(None);
            }

            // Slow path: no index (old segments from before indexing, or recovery)
            let seg_size = self.ensure_read_segment(read_seg_id)?;
            let seg_size = match seg_size {
                Some(s) => s,
                None => return Ok(None),
            };

            if (read_pos as usize) >= seg_size {
                if self.read_segment_id < self.write_segment_id {
                    self.read_segment_id += 1;
                    self.read_position = 0;
                    continue;
                }
                return Ok(None);
            }

            let remaining = seg_size - read_pos as usize;
            if remaining < StoredMessage::MIN_BYTESIZE {
                if self.read_segment_id < self.write_segment_id {
                    self.read_segment_id += 1;
                    self.read_position = 0;
                    continue;
                }
                return Ok(None);
            }

            let data = self.read_segment_data(read_seg_id, read_pos, remaining)?;
            let data = match data {
                Some(d) => d,
                None => return Err(io::Error::new(io::ErrorKind::Other, "read past segment end")),
            };

            let msg_size = peek_message_size(&data)?;
            if msg_size == 0 || msg_size > remaining {
                return Ok(None);
            }

            let sp = SegmentPosition::new(read_seg_id, read_pos, msg_size as u32);

            if self.is_acked(sp.segment, sp.position) {
                self.read_position += sp.bytesize;
                continue;
            }

            let message = decode_message(&data[..msg_size])?;
            self.read_position = read_pos + msg_size as u32;

            return Ok(Some(Envelope {
                segment_position: sp,
                message: Arc::new(message),
                redelivered: false,
            }));
        }
    }

    /// Ensure a read segment is loaded and return its size, or None if not found.
    fn ensure_read_segment(&mut self, segment_id: u32) -> io::Result<Option<usize>> {
        if segment_id == self.write_segment_id {
            return Ok(self.write_segment.as_ref().map(|s| s.size()));
        }
        if let Some(seg) = self.read_segments.get(&segment_id) {
            return Ok(Some(seg.size()));
        }
        let path = self.segment_path(segment_id);
        if !path.exists() {
            return Ok(None);
        }
        let metadata = fs::metadata(&path)?;
        let seg = MmapSegment::open_read(&path, metadata.len() as usize)?;
        let size = seg.size();
        self.read_segments.insert(segment_id, seg);
        Ok(Some(size))
    }

    /// Read and copy segment data to a Vec to avoid borrow issues.
    fn read_segment_data(&self, segment_id: u32, position: u32, len: usize) -> io::Result<Option<Vec<u8>>> {
        let seg = if segment_id == self.write_segment_id {
            self.write_segment.as_ref()
        } else {
            self.read_segments.get(&segment_id)
        };

        match seg {
            Some(seg) => Ok(seg.read(position, len).map(|s| s.to_vec())),
            None => Ok(None),
        }
    }

    fn load_existing(&mut self) -> io::Result<()> {
        let mut segment_ids: Vec<u32> = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(id_str) = name_str.strip_prefix("msgs.") {
                    if let Ok(id) = id_str.parse::<u32>() {
                        segment_ids.push(id);
                    }
                }
            }
        }

        segment_ids.sort();

        if segment_ids.is_empty() {
            return Ok(());
        }

        // Load ack stores
        for &id in &segment_ids {
            let ack_path = self.ack_path(id);
            if ack_path.exists() {
                let store = AckStore::open(&ack_path)?;
                self.ack_stores.insert(id, store);
            }
        }

        // The last segment becomes the write segment
        let last_id = *segment_ids.last().unwrap();
        self.write_segment_id = last_id;
        self.read_segment_id = *segment_ids.first().unwrap();
        self.read_position = 0;

        // Open the write segment
        let write_path = self.segment_path(last_id);
        let metadata = fs::metadata(&write_path)?;
        let seg = MmapSegment::open_read(&write_path, metadata.len() as usize)?;
        self.write_segment = Some(seg);

        // Count messages and build segment indices (scan all segments)
        for &id in &segment_ids {
            let seg_path = self.segment_path(id);
            let data = fs::read(&seg_path)?;
            let mut offset = 0usize;
            let mut index = SegmentIndex::new();
            while offset < data.len() {
                let remaining = &data[offset..];
                if remaining.len() < StoredMessage::MIN_BYTESIZE {
                    break;
                }
                let msg_size = peek_message_size(remaining)?;
                if msg_size == 0 || msg_size > remaining.len() {
                    break;
                }
                index.push(offset as u32, msg_size as u32);
                if !self.is_acked(id, offset as u32) {
                    self.message_count += 1;
                    self.byte_size += msg_size as u64;
                }
                offset += msg_size;
            }
            self.segment_indices.insert(id, index);
        }

        Ok(())
    }
}

// --- Message encoding/decoding ---

fn encode_message_to(msg: &StoredMessage, buf: &mut BytesMut) {
    buf.put_i64(msg.timestamp);
    encode_short_string(buf, &msg.exchange);
    encode_short_string(buf, &msg.routing_key);
    msg.properties.encode(buf);
    buf.put_u64(msg.body.len() as u64);
    buf.put_slice(&msg.body);
}

fn encode_message(msg: &StoredMessage) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(msg.bytesize());
    encode_message_to(msg, &mut buf);
    buf.to_vec()
}

fn decode_message(data: &[u8]) -> io::Result<StoredMessage> {
    use bytes::Buf;

    let mut buf = bytes::Bytes::copy_from_slice(data);

    if buf.remaining() < StoredMessage::MIN_BYTESIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too short",
        ));
    }

    let timestamp = buf.get_i64();

    let exchange = rmq_protocol::field_table::decode_short_string(&mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    let routing_key = rmq_protocol::field_table::decode_short_string(&mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    let properties = BasicProperties::decode(&mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    if buf.remaining() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing bodysize",
        ));
    }
    let bodysize = buf.get_u64() as usize;

    if buf.remaining() < bodysize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "body truncated",
        ));
    }
    let body = buf.split_to(bodysize);

    Ok(StoredMessage {
        timestamp,
        exchange,
        routing_key,
        properties,
        body,
    })
}

/// Peek at the total encoded size of the message at the start of data.
fn peek_message_size(data: &[u8]) -> io::Result<usize> {
    if data.len() < StoredMessage::MIN_BYTESIZE {
        return Ok(0);
    }

    let mut offset = 8; // timestamp

    // exchange short string
    if offset >= data.len() {
        return Ok(0);
    }
    let exchange_len = data[offset] as usize;
    offset += 1 + exchange_len;

    // routing_key short string
    if offset >= data.len() {
        return Ok(0);
    }
    let rk_len = data[offset] as usize;
    offset += 1 + rk_len;

    // properties: need to read flags and then skip fields
    // For simplicity, decode the full properties to get the size
    let mut props_buf = bytes::Bytes::copy_from_slice(&data[offset..]);
    let props = BasicProperties::decode(&mut props_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let props_size = props.encoded_size();
    offset += props_size;

    // bodysize: u64
    if offset + 8 > data.len() {
        return Ok(0);
    }
    let bodysize = u64::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]) as usize;
    offset += 8;

    offset += bodysize;
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn test_message(body: &str) -> StoredMessage {
        StoredMessage {
            timestamp: 1234567890,
            exchange: "amq.direct".into(),
            routing_key: "test-key".into(),
            properties: BasicProperties {
                delivery_mode: Some(2),
                ..Default::default()
            },
            body: Bytes::from(body.as_bytes().to_vec()),
        }
    }

    #[test]
    fn test_push_and_read() {
        let dir = TempDir::new().unwrap();
        let mut store = MessageStore::new(dir.path(), 4096).unwrap();

        let msg = test_message("hello world");
        let sp = store.push(&msg).unwrap();

        let read_msg = store.read_at(&sp).unwrap();
        assert_eq!(read_msg.exchange, "amq.direct");
        assert_eq!(read_msg.routing_key, "test-key");
        assert_eq!(&read_msg.body[..], b"hello world");
        assert_eq!(read_msg.timestamp, 1234567890);
    }

    #[test]
    fn test_shift_order() {
        let dir = TempDir::new().unwrap();
        let mut store = MessageStore::new(dir.path(), 4096).unwrap();

        store.push(&test_message("first")).unwrap();
        store.push(&test_message("second")).unwrap();
        store.push(&test_message("third")).unwrap();

        let env1 = store.shift().unwrap().unwrap();
        assert_eq!(&env1.message.body[..], b"first");
        assert!(!env1.redelivered);

        let env2 = store.shift().unwrap().unwrap();
        assert_eq!(&env2.message.body[..], b"second");

        let env3 = store.shift().unwrap().unwrap();
        assert_eq!(&env3.message.body[..], b"third");

        assert!(store.shift().unwrap().is_none());
    }

    #[test]
    fn test_ack_skips_message() {
        let dir = TempDir::new().unwrap();
        let mut store = MessageStore::new(dir.path(), 4096).unwrap();

        let sp1 = store.push(&test_message("first")).unwrap();
        let _sp2 = store.push(&test_message("second")).unwrap();

        // Ack first message
        store.ack(&sp1).unwrap();

        // Shift should skip the acked message
        let env = store.shift().unwrap().unwrap();
        assert_eq!(&env.message.body[..], b"second");
    }

    #[test]
    fn test_requeue() {
        let dir = TempDir::new().unwrap();
        let mut store = MessageStore::new(dir.path(), 4096).unwrap();

        let sp1 = store.push(&test_message("first")).unwrap();
        let _sp2 = store.push(&test_message("second")).unwrap();

        // Consume first
        let env = store.shift().unwrap().unwrap();
        assert_eq!(&env.message.body[..], b"first");

        // Requeue it
        store.requeue(sp1);

        // Next shift should return requeued message
        let env = store.shift().unwrap().unwrap();
        assert_eq!(&env.message.body[..], b"first");
        assert!(env.redelivered);
    }

    #[test]
    fn test_segment_rotation() {
        let dir = TempDir::new().unwrap();
        // Tiny segment size to force rotation
        let mut store = MessageStore::new(dir.path(), 128).unwrap();

        // Push enough messages to rotate
        let mut positions = Vec::new();
        for i in 0..10 {
            let msg = test_message(&format!("message {i}"));
            let sp = store.push(&msg).unwrap();
            positions.push(sp);
        }

        // Verify we have multiple segments
        assert!(store.write_segment_id > 1, "expected segment rotation");

        // Read all back
        for (i, sp) in positions.iter().enumerate() {
            let msg = store.read_at(sp).unwrap();
            assert_eq!(msg.body, format!("message {i}").as_bytes());
        }
    }

    #[test]
    fn test_message_count() {
        let dir = TempDir::new().unwrap();
        let mut store = MessageStore::new(dir.path(), 4096).unwrap();

        assert_eq!(store.message_count(), 0);

        let sp1 = store.push(&test_message("a")).unwrap();
        let sp2 = store.push(&test_message("b")).unwrap();
        assert_eq!(store.message_count(), 2);

        store.ack(&sp1).unwrap();
        assert_eq!(store.message_count(), 1);

        store.ack(&sp2).unwrap();
        assert_eq!(store.message_count(), 0);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = TempDir::new().unwrap();

        // Write messages
        {
            let mut store = MessageStore::new(dir.path(), 4096).unwrap();
            store.push(&test_message("persistent-1")).unwrap();
            store.push(&test_message("persistent-2")).unwrap();
            store.sync().unwrap();
        }

        // Reopen and read
        {
            let mut store = MessageStore::new(dir.path(), 4096).unwrap();
            assert_eq!(store.message_count(), 2);

            let env = store.shift().unwrap().unwrap();
            assert_eq!(&env.message.body[..], b"persistent-1");

            let env = store.shift().unwrap().unwrap();
            assert_eq!(&env.message.body[..], b"persistent-2");
        }
    }
}
