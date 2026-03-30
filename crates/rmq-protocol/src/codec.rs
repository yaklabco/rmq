use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::ProtocolError;
use crate::frame::AMQPFrame;
use crate::types::{FRAME_END, PROTOCOL_HEADER};

/// Tokio codec for AMQP 0.9.1 frames.
///
/// Handles the initial protocol header detection and then subsequent frame decoding.
pub struct AMQPCodec {
    /// Maximum frame size for validation.
    max_frame_size: u32,
    /// Whether we've received the protocol header.
    header_received: bool,
}

/// Items produced by the decoder.
#[derive(Debug)]
pub enum AMQPCodecItem {
    /// The initial AMQP protocol header from the client.
    ProtocolHeader,
    /// A decoded AMQP frame.
    Frame(AMQPFrame),
}

impl AMQPCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            max_frame_size,
            header_received: false,
        }
    }

    pub fn set_header_received(&mut self, received: bool) {
        self.header_received = received;
    }

    pub fn set_max_frame_size(&mut self, size: u32) {
        self.max_frame_size = size;
    }
}

impl Decoder for AMQPCodec {
    type Item = AMQPCodecItem;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !self.header_received {
            // Look for the 8-byte AMQP protocol header
            if src.len() < 8 {
                return Ok(None);
            }
            if &src[..4] == b"AMQP" {
                let header = src.split_to(8);
                if header[..] == PROTOCOL_HEADER[..] {
                    self.header_received = true;
                    return Ok(Some(AMQPCodecItem::ProtocolHeader));
                }
                return Err(ProtocolError::InvalidHeader);
            }
            return Err(ProtocolError::InvalidHeader);
        }

        // Need at least 7 bytes for frame header (type:1 + channel:2 + size:4)
        if src.len() < 7 {
            return Ok(None);
        }

        // Peek at the size without consuming
        let size = u32::from_be_bytes([src[3], src[4], src[5], src[6]]);

        // Validate frame size
        if size > self.max_frame_size {
            return Err(ProtocolError::FrameTooLarge {
                size,
                max: self.max_frame_size,
            });
        }

        // Total frame: header(7) + payload(size) + end(1)
        let total = 7 + size as usize + 1;
        if src.len() < total {
            // Reserve space for the full frame
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Validate frame end before consuming
        if src[total - 1] != FRAME_END {
            return Err(ProtocolError::InvalidFrameEnd(src[total - 1]));
        }

        let frame_bytes = src.split_to(total);
        let mut bytes = frame_bytes.freeze();
        let frame = AMQPFrame::decode(&mut bytes)?;
        Ok(Some(AMQPCodecItem::Frame(frame)))
    }
}

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: AMQPFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

/// Encoder for raw bytes (used for sending protocol header).
impl Encoder<&[u8]> for AMQPCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: &[u8], dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{ConnectionTune, FramePayload, MethodFrame};
    use crate::types::DEFAULT_FRAME_MAX;

    #[test]
    fn test_codec_protocol_header() {
        let mut codec = AMQPCodec::new(DEFAULT_FRAME_MAX);
        let mut buf = BytesMut::from(&PROTOCOL_HEADER[..]);

        let item = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(item, AMQPCodecItem::ProtocolHeader));
        assert!(codec.header_received);
    }

    #[test]
    fn test_codec_invalid_header() {
        let mut codec = AMQPCodec::new(DEFAULT_FRAME_MAX);
        let mut buf = BytesMut::from(&b"AMQP\x00\x00\x08\x00"[..]);

        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn test_codec_frame_decode() {
        let mut codec = AMQPCodec::new(DEFAULT_FRAME_MAX);
        codec.set_header_received(true);

        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Heartbeat,
        };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let item = codec.decode(&mut buf).unwrap().unwrap();
        match item {
            AMQPCodecItem::Frame(decoded) => assert_eq!(decoded, frame),
            _ => panic!("expected frame"),
        }
    }

    #[test]
    fn test_codec_partial_frame() {
        let mut codec = AMQPCodec::new(DEFAULT_FRAME_MAX);
        codec.set_header_received(true);

        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionTune(ConnectionTune {
                channel_max: 2048,
                frame_max: 131072,
                heartbeat: 60,
            })),
        };
        let mut full = BytesMut::new();
        frame.encode(&mut full);

        // Feed partial data
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&full[..4]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Feed the rest
        buf.extend_from_slice(&full[4..]);
        let item = codec.decode(&mut buf).unwrap().unwrap();
        match item {
            AMQPCodecItem::Frame(decoded) => assert_eq!(decoded, frame),
            _ => panic!("expected frame"),
        }
    }

    #[test]
    fn test_codec_frame_too_large() {
        let mut codec = AMQPCodec::new(100); // tiny max
        codec.set_header_received(true);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[1, 0, 0]); // type=method, channel=0
        buf.extend_from_slice(&200u32.to_be_bytes()); // size > max
        buf.extend_from_slice(&vec![0u8; 201]); // payload + end

        assert!(matches!(
            codec.decode(&mut buf),
            Err(ProtocolError::FrameTooLarge {
                size: 200,
                max: 100
            })
        ));
    }
}
