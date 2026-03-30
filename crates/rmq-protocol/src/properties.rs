use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::field_table::{FieldTable, decode_short_string, encode_short_string};

/// AMQP Basic content properties.
/// Property flags are a bitmask indicating which fields are present.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

// Property flag bits (bit 15 = most significant)
const FLAG_CONTENT_TYPE: u16 = 1 << 15;
const FLAG_CONTENT_ENCODING: u16 = 1 << 14;
const FLAG_HEADERS: u16 = 1 << 13;
const FLAG_DELIVERY_MODE: u16 = 1 << 12;
const FLAG_PRIORITY: u16 = 1 << 11;
const FLAG_CORRELATION_ID: u16 = 1 << 10;
const FLAG_REPLY_TO: u16 = 1 << 9;
const FLAG_EXPIRATION: u16 = 1 << 8;
const FLAG_MESSAGE_ID: u16 = 1 << 7;
const FLAG_TIMESTAMP: u16 = 1 << 6;
const FLAG_MESSAGE_TYPE: u16 = 1 << 5;
const FLAG_USER_ID: u16 = 1 << 4;
const FLAG_APP_ID: u16 = 1 << 3;
const FLAG_CLUSTER_ID: u16 = 1 << 2;

impl BasicProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn flags(&self) -> u16 {
        let mut flags = 0u16;
        if self.content_type.is_some() {
            flags |= FLAG_CONTENT_TYPE;
        }
        if self.content_encoding.is_some() {
            flags |= FLAG_CONTENT_ENCODING;
        }
        if self.headers.is_some() {
            flags |= FLAG_HEADERS;
        }
        if self.delivery_mode.is_some() {
            flags |= FLAG_DELIVERY_MODE;
        }
        if self.priority.is_some() {
            flags |= FLAG_PRIORITY;
        }
        if self.correlation_id.is_some() {
            flags |= FLAG_CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            flags |= FLAG_REPLY_TO;
        }
        if self.expiration.is_some() {
            flags |= FLAG_EXPIRATION;
        }
        if self.message_id.is_some() {
            flags |= FLAG_MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            flags |= FLAG_TIMESTAMP;
        }
        if self.message_type.is_some() {
            flags |= FLAG_MESSAGE_TYPE;
        }
        if self.user_id.is_some() {
            flags |= FLAG_USER_ID;
        }
        if self.app_id.is_some() {
            flags |= FLAG_APP_ID;
        }
        if self.cluster_id.is_some() {
            flags |= FLAG_CLUSTER_ID;
        }
        flags
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let flags = self.flags();
        buf.put_u16(flags);

        if let Some(ref s) = self.content_type {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.content_encoding {
            encode_short_string(buf, s)?;
        }
        if let Some(ref t) = self.headers {
            t.encode(buf)?;
        }
        if let Some(v) = self.delivery_mode {
            buf.put_u8(v);
        }
        if let Some(v) = self.priority {
            buf.put_u8(v);
        }
        if let Some(ref s) = self.correlation_id {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.reply_to {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.expiration {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.message_id {
            encode_short_string(buf, s)?;
        }
        if let Some(v) = self.timestamp {
            buf.put_u64(v);
        }
        if let Some(ref s) = self.message_type {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.user_id {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.app_id {
            encode_short_string(buf, s)?;
        }
        if let Some(ref s) = self.cluster_id {
            encode_short_string(buf, s)?;
        }
        Ok(())
    }

    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 2 {
            return Err(ProtocolError::Incomplete {
                needed: 2,
                available: buf.remaining(),
            });
        }
        let flags = buf.get_u16();
        let mut props = BasicProperties::default();

        if flags & FLAG_CONTENT_TYPE != 0 {
            props.content_type = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_CONTENT_ENCODING != 0 {
            props.content_encoding = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_HEADERS != 0 {
            props.headers = Some(FieldTable::decode(buf)?);
        }
        if flags & FLAG_DELIVERY_MODE != 0 {
            if !buf.has_remaining() {
                return Err(ProtocolError::Incomplete {
                    needed: 1,
                    available: 0,
                });
            }
            props.delivery_mode = Some(buf.get_u8());
        }
        if flags & FLAG_PRIORITY != 0 {
            if !buf.has_remaining() {
                return Err(ProtocolError::Incomplete {
                    needed: 1,
                    available: 0,
                });
            }
            props.priority = Some(buf.get_u8());
        }
        if flags & FLAG_CORRELATION_ID != 0 {
            props.correlation_id = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_REPLY_TO != 0 {
            props.reply_to = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_EXPIRATION != 0 {
            props.expiration = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_MESSAGE_ID != 0 {
            props.message_id = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_TIMESTAMP != 0 {
            if buf.remaining() < 8 {
                return Err(ProtocolError::Incomplete {
                    needed: 8,
                    available: buf.remaining(),
                });
            }
            props.timestamp = Some(buf.get_u64());
        }
        if flags & FLAG_MESSAGE_TYPE != 0 {
            props.message_type = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_USER_ID != 0 {
            props.user_id = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_APP_ID != 0 {
            props.app_id = Some(decode_short_string(buf)?);
        }
        if flags & FLAG_CLUSTER_ID != 0 {
            props.cluster_id = Some(decode_short_string(buf)?);
        }

        Ok(props)
    }

    /// Encoded byte size of properties (including flags u16).
    pub fn encoded_size(&self) -> usize {
        let mut size = 2usize; // flags
        if let Some(ref s) = self.content_type {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.content_encoding {
            size += 1 + s.len();
        }
        if let Some(ref t) = self.headers {
            size += 4 + t.encoded_size() as usize;
        }
        if self.delivery_mode.is_some() {
            size += 1;
        }
        if self.priority.is_some() {
            size += 1;
        }
        if let Some(ref s) = self.correlation_id {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.reply_to {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.expiration {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.message_id {
            size += 1 + s.len();
        }
        if self.timestamp.is_some() {
            size += 8;
        }
        if let Some(ref s) = self.message_type {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.user_id {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.app_id {
            size += 1 + s.len();
        }
        if let Some(ref s) = self.cluster_id {
            size += 1 + s.len();
        }
        size
    }

    /// Returns true if delivery_mode == 2 (persistent).
    pub fn is_persistent(&self) -> bool {
        self.delivery_mode == Some(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field_table::FieldValue;

    #[test]
    fn test_empty_properties_round_trip() {
        let props = BasicProperties::default();
        let mut buf = BytesMut::new();
        props.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(props, decoded);
    }

    #[test]
    fn test_full_properties_round_trip() {
        let mut headers = FieldTable::new();
        headers.insert("x-custom", FieldValue::I32(123));

        let props = BasicProperties {
            content_type: Some("application/json".into()),
            content_encoding: Some("utf-8".into()),
            headers: Some(headers),
            delivery_mode: Some(2),
            priority: Some(5),
            correlation_id: Some("corr-123".into()),
            reply_to: Some("reply-queue".into()),
            expiration: Some("60000".into()),
            message_id: Some("msg-456".into()),
            timestamp: Some(1234567890),
            message_type: Some("order.created".into()),
            user_id: Some("guest".into()),
            app_id: Some("test-app".into()),
            cluster_id: Some("cluster-1".into()),
        };

        let mut buf = BytesMut::new();
        props.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(props, decoded);
    }

    #[test]
    fn test_partial_properties_round_trip() {
        let props = BasicProperties {
            delivery_mode: Some(2),
            priority: Some(0),
            timestamp: Some(9999),
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        props.encode(&mut buf).unwrap();
        let mut bytes = buf.freeze();
        let decoded = BasicProperties::decode(&mut bytes).unwrap();
        assert_eq!(props, decoded);
    }

    #[test]
    fn test_encoded_size_matches_actual() {
        let props = BasicProperties {
            content_type: Some("text/plain".into()),
            delivery_mode: Some(1),
            message_id: Some("abc".into()),
            ..Default::default()
        };

        let expected = props.encoded_size();
        let mut buf = BytesMut::new();
        props.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), expected);
    }
}
