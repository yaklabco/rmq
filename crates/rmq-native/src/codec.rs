use std::io;

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
    Auth {
        username: String,
        password: String,
    },
    AuthOk,
    Publish {
        queue: String,
        messages: Vec<Bytes>,
    },
    PublishOk {
        count: u32,
    },
    Consume {
        queue: String,
        batch_size: u32,
        prefetch: u32,
    },
    Deliver {
        batch_id: u64,
        messages: Vec<Bytes>,
    },
    Ack {
        batch_id: u64,
    },
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
            Self::Consume {
                queue,
                batch_size,
                prefetch,
            } => {
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

    /// Decode a frame from the buffer. Returns `Ok(None)` if incomplete,
    /// or `Err` if the payload is malformed (insufficient bytes within a frame).
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, io::Error> {
        if buf.len() < 5 {
            return Ok(None); // need type + length
        }
        let frame_type = buf[0];
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if buf.len() < 5 + length {
            return Ok(None); // incomplete payload
        }

        buf.advance(5); // consume type + length
        let mut payload = buf.split_to(length);

        match frame_type {
            FRAME_AUTH => {
                let username =
                    get_str(&mut payload).ok_or_else(|| eof("Auth: missing username"))?;
                let password =
                    get_str(&mut payload).ok_or_else(|| eof("Auth: missing password"))?;
                Ok(Some(Self::Auth { username, password }))
            }
            FRAME_AUTH_OK => Ok(Some(Self::AuthOk)),
            FRAME_PUBLISH => {
                let queue =
                    get_str(&mut payload).ok_or_else(|| eof("Publish: missing queue name"))?;
                check_remaining(&payload, 4, "Publish: missing message count")?;
                let count = payload.get_u32() as usize;
                let mut messages = Vec::with_capacity(count);
                for i in 0..count {
                    check_remaining(
                        &payload,
                        4,
                        &format!("Publish: missing length for message {i}"),
                    )?;
                    let len = payload.get_u32() as usize;
                    check_remaining(&payload, len, &format!("Publish: truncated message {i}"))?;
                    messages.push(payload.split_to(len).freeze());
                }
                Ok(Some(Self::Publish { queue, messages }))
            }
            FRAME_PUBLISH_OK => {
                check_remaining(&payload, 4, "PublishOk: missing count")?;
                let count = payload.get_u32();
                Ok(Some(Self::PublishOk { count }))
            }
            FRAME_CONSUME => {
                let queue =
                    get_str(&mut payload).ok_or_else(|| eof("Consume: missing queue name"))?;
                check_remaining(&payload, 4, "Consume: missing batch_size")?;
                let batch_size = payload.get_u32();
                check_remaining(&payload, 4, "Consume: missing prefetch")?;
                let prefetch = payload.get_u32();
                Ok(Some(Self::Consume {
                    queue,
                    batch_size,
                    prefetch,
                }))
            }
            FRAME_DELIVER => {
                check_remaining(&payload, 8, "Deliver: missing batch_id")?;
                let batch_id = payload.get_u64();
                check_remaining(&payload, 4, "Deliver: missing message count")?;
                let count = payload.get_u32() as usize;
                let mut messages = Vec::with_capacity(count);
                for i in 0..count {
                    check_remaining(
                        &payload,
                        4,
                        &format!("Deliver: missing length for message {i}"),
                    )?;
                    let len = payload.get_u32() as usize;
                    check_remaining(&payload, len, &format!("Deliver: truncated message {i}"))?;
                    messages.push(payload.split_to(len).freeze());
                }
                Ok(Some(Self::Deliver { batch_id, messages }))
            }
            FRAME_ACK => {
                check_remaining(&payload, 8, "Ack: missing batch_id")?;
                let batch_id = payload.get_u64();
                Ok(Some(Self::Ack { batch_id }))
            }
            FRAME_DISCONNECT => Ok(Some(Self::Disconnect)),
            _ => Ok(None),
        }
    }
}

fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn eof(detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("malformed frame: {detail}"),
    )
}

fn check_remaining(buf: &BytesMut, needed: usize, detail: &str) -> Result<(), io::Error> {
    if buf.remaining() < needed {
        Err(eof(detail))
    } else {
        Ok(())
    }
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
        NativeFrame::decode(&mut buf).unwrap().unwrap()
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
        assert!(NativeFrame::decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_malformed_publish_missing_count() {
        // FRAME_PUBLISH with a payload that has a queue name but no message count
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_PUBLISH);
        // payload: just a short queue name "q" (2-byte len + 1 byte)
        buf.put_u32(3); // payload length
        buf.put_u16(1); // string length
        buf.put_u8(b'q');
        let err = NativeFrame::decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_malformed_ack_missing_batch_id() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_ACK);
        buf.put_u32(4); // payload too short for u64
        buf.put_u32(0);
        let err = NativeFrame::decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_malformed_deliver_truncated_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_DELIVER);
        // payload: batch_id(8) + count(4) + msg_len(4) = 16, but msg body missing
        buf.put_u32(16);
        buf.put_u64(1); // batch_id
        buf.put_u32(1); // 1 message
        buf.put_u32(100); // claims 100 bytes but there are none
        let err = NativeFrame::decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_malformed_consume_missing_prefetch() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_CONSUME);
        // payload: queue "q" (3 bytes) + batch_size (4) = 7, missing prefetch
        buf.put_u32(7);
        buf.put_u16(1);
        buf.put_u8(b'q');
        buf.put_u32(10);
        let err = NativeFrame::decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_malformed_publish_ok_empty_payload() {
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_PUBLISH_OK);
        buf.put_u32(0); // empty payload, needs 4 bytes
        let err = NativeFrame::decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
