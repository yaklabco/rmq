use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Protocol magic: "RMQ\x01"
pub const MAGIC: [u8; 4] = [b'R', b'M', b'Q', 0x01];

// Frame types
pub const FRAME_AUTH: u8 = 0x01;
pub const FRAME_AUTH_OK: u8 = 0x02;
pub const FRAME_PUBLISH: u8 = 0x03;
pub const FRAME_PUBLISH_OK: u8 = 0x04;
pub const FRAME_CONSUME: u8 = 0x05;
pub const FRAME_DELIVER: u8 = 0x06;
pub const FRAME_ACK: u8 = 0x07;
pub const FRAME_DISCONNECT: u8 = 0x08;

/// A native protocol frame.
#[derive(Debug, Clone)]
pub enum NativeFrame {
    Auth { username: String, password: String },
    AuthOk,
    Publish { queue: String, messages: Vec<Bytes> },
    PublishOk { count: u32 },
    Consume { queue: String, batch_size: u32, prefetch: u32 },
    Deliver { batch_id: u64, messages: Vec<Bytes> },
    Ack { batch_id: u64 },
    Disconnect,
}

impl NativeFrame {
    /// Encode a frame into the buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Auth { username, password } => {
                let payload_len = 2 + username.len() + 2 + password.len();
                buf.put_u8(FRAME_AUTH);
                buf.put_u32(payload_len as u32);
                put_str(buf, username);
                put_str(buf, password);
            }
            Self::AuthOk => {
                buf.put_u8(FRAME_AUTH_OK);
                buf.put_u32(0);
            }
            Self::Publish { queue, messages } => {
                let mut payload_len = 2 + queue.len() + 4; // queue str + count
                for msg in messages {
                    payload_len += 4 + msg.len(); // len + body
                }
                buf.put_u8(FRAME_PUBLISH);
                buf.put_u32(payload_len as u32);
                put_str(buf, queue);
                buf.put_u32(messages.len() as u32);
                for msg in messages {
                    buf.put_u32(msg.len() as u32);
                    buf.extend_from_slice(msg);
                }
            }
            Self::PublishOk { count } => {
                buf.put_u8(FRAME_PUBLISH_OK);
                buf.put_u32(4);
                buf.put_u32(*count);
            }
            Self::Consume { queue, batch_size, prefetch } => {
                let payload_len = 2 + queue.len() + 4 + 4;
                buf.put_u8(FRAME_CONSUME);
                buf.put_u32(payload_len as u32);
                put_str(buf, queue);
                buf.put_u32(*batch_size);
                buf.put_u32(*prefetch);
            }
            Self::Deliver { batch_id, messages } => {
                let mut payload_len = 8 + 4; // batch_id + count
                for msg in messages {
                    payload_len += 4 + msg.len();
                }
                buf.put_u8(FRAME_DELIVER);
                buf.put_u32(payload_len as u32);
                buf.put_u64(*batch_id);
                buf.put_u32(messages.len() as u32);
                for msg in messages {
                    buf.put_u32(msg.len() as u32);
                    buf.extend_from_slice(msg);
                }
            }
            Self::Ack { batch_id } => {
                buf.put_u8(FRAME_ACK);
                buf.put_u32(8);
                buf.put_u64(*batch_id);
            }
            Self::Disconnect => {
                buf.put_u8(FRAME_DISCONNECT);
                buf.put_u32(0);
            }
        }
    }

    /// Decode a frame from the buffer. Returns None if incomplete.
    pub fn decode(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < 5 {
            return None; // need type + length
        }
        let frame_type = buf[0];
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if buf.len() < 5 + length {
            return None; // incomplete payload
        }

        buf.advance(5); // consume type + length
        let mut payload = buf.split_to(length);

        match frame_type {
            FRAME_AUTH => {
                let username = get_str(&mut payload)?;
                let password = get_str(&mut payload)?;
                Some(Self::Auth { username, password })
            }
            FRAME_AUTH_OK => Some(Self::AuthOk),
            FRAME_PUBLISH => {
                let queue = get_str(&mut payload)?;
                let count = payload.get_u32() as usize;
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    let len = payload.get_u32() as usize;
                    messages.push(payload.split_to(len).freeze());
                }
                Some(Self::Publish { queue, messages })
            }
            FRAME_PUBLISH_OK => {
                let count = payload.get_u32();
                Some(Self::PublishOk { count })
            }
            FRAME_CONSUME => {
                let queue = get_str(&mut payload)?;
                let batch_size = payload.get_u32();
                let prefetch = payload.get_u32();
                Some(Self::Consume { queue, batch_size, prefetch })
            }
            FRAME_DELIVER => {
                let batch_id = payload.get_u64();
                let count = payload.get_u32() as usize;
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    let len = payload.get_u32() as usize;
                    messages.push(payload.split_to(len).freeze());
                }
                Some(Self::Deliver { batch_id, messages })
            }
            FRAME_ACK => {
                let batch_id = payload.get_u64();
                Some(Self::Ack { batch_id })
            }
            FRAME_DISCONNECT => Some(Self::Disconnect),
            _ => None,
        }
    }
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn get_str(buf: &mut BytesMut) -> Option<String> {
    if buf.remaining() < 2 {
        return None;
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return None;
    }
    let data = buf.split_to(len);
    String::from_utf8(data.to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: &NativeFrame) -> NativeFrame {
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        NativeFrame::decode(&mut buf).unwrap()
    }

    #[test]
    fn test_auth_round_trip() {
        let frame = NativeFrame::Auth {
            username: "guest".into(),
            password: "guest".into(),
        };
        match round_trip(&frame) {
            NativeFrame::Auth { username, password } => {
                assert_eq!(username, "guest");
                assert_eq!(password, "guest");
            }
            _ => panic!("wrong frame type"),
        }
    }

    #[test]
    fn test_publish_batch_round_trip() {
        let frame = NativeFrame::Publish {
            queue: "test-q".into(),
            messages: vec![
                Bytes::from_static(b"msg1"),
                Bytes::from_static(b"msg2"),
                Bytes::from_static(b"msg3"),
            ],
        };
        match round_trip(&frame) {
            NativeFrame::Publish { queue, messages } => {
                assert_eq!(queue, "test-q");
                assert_eq!(messages.len(), 3);
                assert_eq!(&messages[0][..], b"msg1");
                assert_eq!(&messages[2][..], b"msg3");
            }
            _ => panic!("wrong frame type"),
        }
    }

    #[test]
    fn test_deliver_round_trip() {
        let frame = NativeFrame::Deliver {
            batch_id: 42,
            messages: vec![Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
        };
        match round_trip(&frame) {
            NativeFrame::Deliver { batch_id, messages } => {
                assert_eq!(batch_id, 42);
                assert_eq!(messages.len(), 2);
            }
            _ => panic!("wrong frame type"),
        }
    }

    #[test]
    fn test_ack_round_trip() {
        let frame = NativeFrame::Ack { batch_id: 99 };
        match round_trip(&frame) {
            NativeFrame::Ack { batch_id } => assert_eq!(batch_id, 99),
            _ => panic!("wrong frame type"),
        }
    }

    #[test]
    fn test_incomplete_frame() {
        let mut buf = BytesMut::from(&[FRAME_AUTH, 0, 0, 0][..]);
        assert!(NativeFrame::decode(&mut buf).is_none());
    }
}
