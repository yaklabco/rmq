use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MqttError {
    #[error("incomplete packet: need more data")]
    Incomplete,
    #[error("invalid packet type: {0}")]
    InvalidPacketType(u8),
    #[error("malformed remaining length")]
    MalformedLength,
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// MQTT QoS levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(v: u8) -> Result<Self, MqttError> {
        match v {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(MqttError::ProtocolViolation(format!("invalid QoS: {v}"))),
        }
    }
}

/// MQTT 3.1.1 packet types.
#[derive(Debug, Clone, PartialEq)]
pub enum MqttPacket {
    Connect(ConnectPacket),
    ConnAck(ConnAckPacket),
    Publish(PublishPacket),
    PubAck { packet_id: u16 },
    PubRec { packet_id: u16 },
    PubRel { packet_id: u16 },
    PubComp { packet_id: u16 },
    Subscribe(SubscribePacket),
    SubAck(SubAckPacket),
    Unsubscribe(UnsubscribePacket),
    UnsubAck { packet_id: u16 },
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectPacket {
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub will: Option<WillMessage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WillMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnAckPacket {
    pub session_present: bool,
    pub return_code: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishPacket {
    pub topic: String,
    pub packet_id: Option<u16>,
    pub qos: QoS,
    pub retain: bool,
    pub dup: bool,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscribePacket {
    pub packet_id: u16,
    pub topics: Vec<(String, QoS)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubAckPacket {
    pub packet_id: u16,
    pub return_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribePacket {
    pub packet_id: u16,
    pub topics: Vec<String>,
}

// --- Encoding ---

impl MqttPacket {
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            MqttPacket::ConnAck(p) => {
                buf.put_u8(0x20); // CONNACK
                buf.put_u8(2);    // remaining length
                buf.put_u8(if p.session_present { 1 } else { 0 });
                buf.put_u8(p.return_code);
            }
            MqttPacket::Publish(p) => {
                let mut flags = 0x30u8; // PUBLISH
                if p.dup {
                    flags |= 0x08;
                }
                flags |= (p.qos as u8) << 1;
                if p.retain {
                    flags |= 0x01;
                }
                buf.put_u8(flags);

                let mut payload_buf = BytesMut::new();
                encode_utf8_string(&mut payload_buf, &p.topic);
                if let Some(id) = p.packet_id {
                    payload_buf.put_u16(id);
                }
                payload_buf.extend_from_slice(&p.payload);

                encode_remaining_length(buf, payload_buf.len());
                buf.extend_from_slice(&payload_buf);
            }
            MqttPacket::PubAck { packet_id } => {
                buf.put_u8(0x40);
                buf.put_u8(2);
                buf.put_u16(*packet_id);
            }
            MqttPacket::PubRec { packet_id } => {
                buf.put_u8(0x50);
                buf.put_u8(2);
                buf.put_u16(*packet_id);
            }
            MqttPacket::PubRel { packet_id } => {
                buf.put_u8(0x62); // 0x60 | 0x02 (required flag)
                buf.put_u8(2);
                buf.put_u16(*packet_id);
            }
            MqttPacket::PubComp { packet_id } => {
                buf.put_u8(0x70);
                buf.put_u8(2);
                buf.put_u16(*packet_id);
            }
            MqttPacket::SubAck(p) => {
                buf.put_u8(0x90);
                encode_remaining_length(buf, 2 + p.return_codes.len());
                buf.put_u16(p.packet_id);
                for &code in &p.return_codes {
                    buf.put_u8(code);
                }
            }
            MqttPacket::UnsubAck { packet_id } => {
                buf.put_u8(0xB0);
                buf.put_u8(2);
                buf.put_u16(*packet_id);
            }
            MqttPacket::PingResp => {
                buf.put_u8(0xD0);
                buf.put_u8(0);
            }
            _ => {} // Client-only packets don't need server encoding
        }
    }
}

// --- Decoding ---

/// Decode a single MQTT packet from the buffer.
/// Returns None if there isn't enough data yet.
pub fn decode_packet(buf: &mut BytesMut) -> Result<Option<MqttPacket>, MqttError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Peek at fixed header
    let first_byte = buf[0];
    let packet_type = first_byte >> 4;

    // Try to read remaining length
    let (remaining_len, header_size) = match decode_remaining_length(&buf[1..]) {
        Ok(v) => v,
        Err(MqttError::Incomplete) => return Ok(None),
        Err(e) => return Err(e),
    };

    let total = 1 + header_size + remaining_len;
    if buf.len() < total {
        return Ok(None);
    }

    let mut packet_buf = buf.split_to(total);
    packet_buf.advance(1 + header_size); // skip fixed header

    match packet_type {
        1 => decode_connect(&mut packet_buf).map(|p| Some(MqttPacket::Connect(p))),
        3 => decode_publish(first_byte, &mut packet_buf).map(|p| Some(MqttPacket::Publish(p))),
        4 => Ok(Some(MqttPacket::PubAck {
            packet_id: packet_buf.get_u16(),
        })),
        5 => Ok(Some(MqttPacket::PubRec {
            packet_id: packet_buf.get_u16(),
        })),
        6 => Ok(Some(MqttPacket::PubRel {
            packet_id: packet_buf.get_u16(),
        })),
        7 => Ok(Some(MqttPacket::PubComp {
            packet_id: packet_buf.get_u16(),
        })),
        8 => decode_subscribe(&mut packet_buf).map(|p| Some(MqttPacket::Subscribe(p))),
        10 => decode_unsubscribe(&mut packet_buf).map(|p| Some(MqttPacket::Unsubscribe(p))),
        12 => Ok(Some(MqttPacket::PingReq)),
        14 => Ok(Some(MqttPacket::Disconnect)),
        _ => Err(MqttError::InvalidPacketType(packet_type)),
    }
}

fn decode_connect(buf: &mut BytesMut) -> Result<ConnectPacket, MqttError> {
    // Protocol Name
    let proto = decode_utf8_string(buf)?;
    if proto != "MQTT" && proto != "MQIsdp" {
        return Err(MqttError::ProtocolViolation(format!(
            "unknown protocol: {proto}"
        )));
    }

    // Protocol Level
    let _level = buf.get_u8(); // 4 for MQTT 3.1.1

    // Connect Flags
    let flags = buf.get_u8();
    let clean_session = flags & 0x02 != 0;
    let has_will = flags & 0x04 != 0;
    let will_qos = QoS::from_u8((flags >> 3) & 0x03)?;
    let will_retain = flags & 0x20 != 0;
    let has_password = flags & 0x40 != 0;
    let has_username = flags & 0x80 != 0;

    // Keep Alive
    let keep_alive = buf.get_u16();

    // Client ID
    let client_id = decode_utf8_string(buf)?;

    // Will
    let will = if has_will {
        let topic = decode_utf8_string(buf)?;
        let payload_len = buf.get_u16() as usize;
        let payload = buf.split_to(payload_len).freeze();
        Some(WillMessage {
            topic,
            payload,
            qos: will_qos,
            retain: will_retain,
        })
    } else {
        None
    };

    let username = if has_username {
        Some(decode_utf8_string(buf)?)
    } else {
        None
    };

    let password = if has_password {
        let len = buf.get_u16() as usize;
        Some(buf.split_to(len).freeze())
    } else {
        None
    };

    Ok(ConnectPacket {
        client_id,
        clean_session,
        keep_alive,
        username,
        password,
        will,
    })
}

fn decode_publish(first_byte: u8, buf: &mut BytesMut) -> Result<PublishPacket, MqttError> {
    let dup = first_byte & 0x08 != 0;
    let qos = QoS::from_u8((first_byte >> 1) & 0x03)?;
    let retain = first_byte & 0x01 != 0;

    let topic = decode_utf8_string(buf)?;
    let packet_id = if qos != QoS::AtMostOnce {
        Some(buf.get_u16())
    } else {
        None
    };

    let payload = buf.split_to(buf.remaining()).freeze();

    Ok(PublishPacket {
        topic,
        packet_id,
        qos,
        retain,
        dup,
        payload,
    })
}

fn decode_subscribe(buf: &mut BytesMut) -> Result<SubscribePacket, MqttError> {
    let packet_id = buf.get_u16();
    let mut topics = Vec::new();
    while buf.has_remaining() {
        let topic = decode_utf8_string(buf)?;
        let qos = QoS::from_u8(buf.get_u8())?;
        topics.push((topic, qos));
    }
    Ok(SubscribePacket { packet_id, topics })
}

fn decode_unsubscribe(buf: &mut BytesMut) -> Result<UnsubscribePacket, MqttError> {
    let packet_id = buf.get_u16();
    let mut topics = Vec::new();
    while buf.has_remaining() {
        topics.push(decode_utf8_string(buf)?);
    }
    Ok(UnsubscribePacket { packet_id, topics })
}

// --- Helpers ---

fn encode_utf8_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn decode_utf8_string(buf: &mut BytesMut) -> Result<String, MqttError> {
    if buf.remaining() < 2 {
        return Err(MqttError::Incomplete);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(MqttError::Incomplete);
    }
    let data = buf.split_to(len);
    String::from_utf8(data.to_vec())
        .map_err(|_| MqttError::ProtocolViolation("invalid UTF-8".into()))
}

fn encode_remaining_length(buf: &mut BytesMut, mut len: usize) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if len == 0 {
            break;
        }
    }
}

/// Returns (remaining_length, bytes_consumed_for_length).
fn decode_remaining_length(data: &[u8]) -> Result<(usize, usize), MqttError> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    for (i, &byte) in data.iter().enumerate() {
        value += (byte as usize & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        multiplier *= 128;
        if i >= 3 {
            return Err(MqttError::MalformedLength);
        }
    }
    Err(MqttError::Incomplete)
}

/// Convert MQTT topic to AMQP routing key.
/// MQTT uses `/` as separator, AMQP uses `.`.
/// MQTT `+` wildcard → AMQP `*`, MQTT `#` → AMQP `#`.
pub fn mqtt_topic_to_amqp_routing_key(topic: &str) -> String {
    topic
        .replace('/', ".")
        .replace('+', "*")
}

/// Convert AMQP routing key back to MQTT topic.
pub fn amqp_routing_key_to_mqtt_topic(key: &str) -> String {
    key.replace('.', "/").replace('*', "+")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remaining_length_encode_decode() {
        for len in [0, 1, 127, 128, 16383, 16384, 2_097_151, 268_435_455] {
            let mut buf = BytesMut::new();
            encode_remaining_length(&mut buf, len);
            let (decoded, _) = decode_remaining_length(&buf).unwrap();
            assert_eq!(decoded, len, "round trip failed for {len}");
        }
    }

    #[test]
    fn test_connack_encode() {
        let packet = MqttPacket::ConnAck(ConnAckPacket {
            session_present: false,
            return_code: 0,
        });
        let mut buf = BytesMut::new();
        packet.encode(&mut buf);
        assert_eq!(&buf[..], &[0x20, 0x02, 0x00, 0x00]);
    }

    #[test]
    fn test_publish_round_trip() {
        let packet = MqttPacket::Publish(PublishPacket {
            topic: "test/topic".into(),
            packet_id: Some(42),
            qos: QoS::AtLeastOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"hello"),
        });
        let mut buf = BytesMut::new();
        packet.encode(&mut buf);

        let decoded = decode_packet(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn test_connect_decode() {
        let mut buf = BytesMut::new();
        // CONNECT packet
        buf.put_u8(0x10); // CONNECT type
        // Build payload
        let mut payload = BytesMut::new();
        encode_utf8_string(&mut payload, "MQTT"); // protocol name
        payload.put_u8(4); // protocol level
        payload.put_u8(0x02); // clean session
        payload.put_u16(60); // keep alive
        encode_utf8_string(&mut payload, "test-client"); // client id

        encode_remaining_length(&mut buf, payload.len());
        buf.extend_from_slice(&payload);

        let packet = decode_packet(&mut buf).unwrap().unwrap();
        match packet {
            MqttPacket::Connect(c) => {
                assert_eq!(c.client_id, "test-client");
                assert!(c.clean_session);
                assert_eq!(c.keep_alive, 60);
                assert!(c.username.is_none());
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn test_connect_with_credentials() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x10);
        let mut payload = BytesMut::new();
        encode_utf8_string(&mut payload, "MQTT");
        payload.put_u8(4);
        payload.put_u8(0xC2); // clean session + username + password
        payload.put_u16(60);
        encode_utf8_string(&mut payload, "client1");
        encode_utf8_string(&mut payload, "guest");
        payload.put_u16(5);
        payload.put_slice(b"guest");

        encode_remaining_length(&mut buf, payload.len());
        buf.extend_from_slice(&payload);

        let packet = decode_packet(&mut buf).unwrap().unwrap();
        match packet {
            MqttPacket::Connect(c) => {
                assert_eq!(c.username, Some("guest".into()));
                assert_eq!(c.password, Some(Bytes::from_static(b"guest")));
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn test_subscribe_decode() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x82); // SUBSCRIBE with required flag
        let mut payload = BytesMut::new();
        payload.put_u16(1); // packet id
        encode_utf8_string(&mut payload, "test/+/events");
        payload.put_u8(1); // QoS 1
        encode_utf8_string(&mut payload, "logs/#");
        payload.put_u8(0); // QoS 0

        encode_remaining_length(&mut buf, payload.len());
        buf.extend_from_slice(&payload);

        let packet = decode_packet(&mut buf).unwrap().unwrap();
        match packet {
            MqttPacket::Subscribe(s) => {
                assert_eq!(s.packet_id, 1);
                assert_eq!(s.topics.len(), 2);
                assert_eq!(s.topics[0].0, "test/+/events");
                assert_eq!(s.topics[0].1, QoS::AtLeastOnce);
                assert_eq!(s.topics[1].0, "logs/#");
                assert_eq!(s.topics[1].1, QoS::AtMostOnce);
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_ping_decode() {
        let mut buf = BytesMut::from(&[0xC0, 0x00][..]);
        let packet = decode_packet(&mut buf).unwrap().unwrap();
        assert_eq!(packet, MqttPacket::PingReq);
    }

    #[test]
    fn test_disconnect_decode() {
        let mut buf = BytesMut::from(&[0xE0, 0x00][..]);
        let packet = decode_packet(&mut buf).unwrap().unwrap();
        assert_eq!(packet, MqttPacket::Disconnect);
    }

    #[test]
    fn test_topic_conversion() {
        assert_eq!(mqtt_topic_to_amqp_routing_key("a/b/c"), "a.b.c");
        assert_eq!(mqtt_topic_to_amqp_routing_key("a/+/c"), "a.*.c");
        assert_eq!(mqtt_topic_to_amqp_routing_key("a/#"), "a.#");
        assert_eq!(amqp_routing_key_to_mqtt_topic("a.b.c"), "a/b/c");
        assert_eq!(amqp_routing_key_to_mqtt_topic("a.*.c"), "a/+/c");
    }

    #[test]
    fn test_incomplete_packet() {
        let mut buf = BytesMut::from(&[0x10, 0x0A][..]); // CONNECT, remaining=10, but no data
        let result = decode_packet(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_suback_encode() {
        let packet = MqttPacket::SubAck(SubAckPacket {
            packet_id: 1,
            return_codes: vec![0, 1, 0x80],
        });
        let mut buf = BytesMut::new();
        packet.encode(&mut buf);
        assert_eq!(&buf[..], &[0x90, 5, 0, 1, 0, 1, 0x80]);
    }

    #[test]
    fn test_pingresp_encode() {
        let packet = MqttPacket::PingResp;
        let mut buf = BytesMut::new();
        packet.encode(&mut buf);
        assert_eq!(&buf[..], &[0xD0, 0x00]);
    }
}
