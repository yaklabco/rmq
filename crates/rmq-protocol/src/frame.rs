use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::field_table::{
    FieldTable, decode_long_string, decode_short_string, encode_long_string, encode_short_string,
};
use crate::properties::BasicProperties;
use crate::types::*;

/// An AMQP 0.9.1 frame.
#[derive(Debug, Clone, PartialEq)]
pub struct AMQPFrame {
    pub channel: u16,
    pub payload: FramePayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FramePayload {
    Method(MethodFrame),
    Header(ContentHeader),
    Body(Bytes),
    Heartbeat,
}

/// Content header frame (class_id, weight=0, body_size, properties).
#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties: BasicProperties,
}

/// All supported AMQP method frames.
#[derive(Debug, Clone, PartialEq)]
pub enum MethodFrame {
    // Connection
    ConnectionStart(ConnectionStart),
    ConnectionStartOk(ConnectionStartOk),
    ConnectionTune(ConnectionTune),
    ConnectionTuneOk(ConnectionTuneOk),
    ConnectionOpen(ConnectionOpen),
    ConnectionOpenOk,
    ConnectionClose(ConnectionClose),
    ConnectionCloseOk,
    ConnectionBlocked(ConnectionBlocked),
    ConnectionUnblocked,

    // Channel
    ChannelOpen,
    ChannelOpenOk,
    ChannelFlow(bool),
    ChannelFlowOk(bool),
    ChannelClose(ChannelClose),
    ChannelCloseOk,

    // Exchange
    ExchangeDeclare(ExchangeDeclare),
    ExchangeDeclareOk,
    ExchangeDelete(ExchangeDelete),
    ExchangeDeleteOk,
    ExchangeBind(ExchangeBind),
    ExchangeBindOk,
    ExchangeUnbind(ExchangeUnbind),
    ExchangeUnbindOk,

    // Queue
    QueueDeclare(QueueDeclare),
    QueueDeclareOk(QueueDeclareOk),
    QueueBind(QueueBind),
    QueueBindOk,
    QueueUnbind(QueueUnbind),
    QueueUnbindOk,
    QueuePurge(QueuePurge),
    QueuePurgeOk(u32),
    QueueDelete(QueueDelete),
    QueueDeleteOk(u32),

    // Basic
    BasicQos(BasicQos),
    BasicQosOk,
    BasicConsume(BasicConsume),
    BasicConsumeOk(String),
    BasicCancel(BasicCancel),
    BasicCancelOk(String),
    BasicPublish(BasicPublish),
    BasicReturn(BasicReturn),
    BasicDeliver(BasicDeliver),
    BasicGet(BasicGet),
    BasicGetOk(BasicGetOk),
    BasicGetEmpty,
    BasicAck(BasicAck),
    BasicReject(BasicReject),
    BasicRecoverAsync(bool),
    BasicRecover(bool),
    BasicRecoverOk,
    BasicNack(BasicNack),

    // Confirm
    ConfirmSelect(bool),
    ConfirmSelectOk,

    // Tx
    TxSelect,
    TxSelectOk,
    TxCommit,
    TxCommitOk,
    TxRollback,
    TxRollbackOk,
}

// --- Method payload structs ---

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStart {
    pub version_major: u8,
    pub version_minor: u8,
    pub server_properties: FieldTable,
    pub mechanisms: Bytes,
    pub locales: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStartOk {
    pub client_properties: FieldTable,
    pub mechanism: String,
    pub response: Bytes,
    pub locale: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTuneOk {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionOpen {
    pub virtual_host: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionClose {
    pub reply_code: u16,
    pub reply_text: String,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionBlocked {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelClose {
    pub reply_code: u16,
    pub reply_text: String,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDeclare {
    pub exchange: String,
    pub exchange_type: String,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDelete {
    pub exchange: String,
    pub if_unused: bool,
    pub no_wait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeBind {
    pub destination: String,
    pub source: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeUnbind {
    pub destination: String,
    pub source: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclare {
    pub queue: String,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclareOk {
    pub queue: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueBind {
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueUnbind {
    pub queue: String,
    pub exchange: String,
    pub routing_key: String,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuePurge {
    pub queue: String,
    pub no_wait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDelete {
    pub queue: String,
    pub if_unused: bool,
    pub if_empty: bool,
    pub no_wait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicQos {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsume {
    pub queue: String,
    pub consumer_tag: String,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub no_wait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicCancel {
    pub consumer_tag: String,
    pub no_wait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicPublish {
    pub exchange: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicReturn {
    pub reply_code: u16,
    pub reply_text: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicDeliver {
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicGet {
    pub queue: String,
    pub no_ack: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicGetOk {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub message_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicAck {
    pub delivery_tag: u64,
    pub multiple: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicReject {
    pub delivery_tag: u64,
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicNack {
    pub delivery_tag: u64,
    pub multiple: bool,
    pub requeue: bool,
}

// --- Encoding ---

impl AMQPFrame {
    pub fn encode(&self, buf: &mut BytesMut) {
        match &self.payload {
            FramePayload::Method(method) => {
                // Write header with placeholder size, encode in-place, patch size
                buf.put_u8(FRAME_METHOD);
                buf.put_u16(self.channel);
                let size_offset = buf.len();
                buf.put_u32(0); // placeholder
                let payload_start = buf.len();
                method.encode(buf);
                let payload_len = (buf.len() - payload_start) as u32;
                // Patch the size field
                buf[size_offset..size_offset + 4].copy_from_slice(&payload_len.to_be_bytes());
                buf.put_u8(FRAME_END);
            }
            FramePayload::Header(header) => {
                buf.put_u8(FRAME_HEADER);
                buf.put_u16(self.channel);
                let size_offset = buf.len();
                buf.put_u32(0); // placeholder
                let payload_start = buf.len();
                buf.put_u16(header.class_id);
                buf.put_u16(0); // weight
                buf.put_u64(header.body_size);
                header.properties.encode(buf);
                let payload_len = (buf.len() - payload_start) as u32;
                buf[size_offset..size_offset + 4].copy_from_slice(&payload_len.to_be_bytes());
                buf.put_u8(FRAME_END);
            }
            FramePayload::Body(data) => {
                buf.put_u8(FRAME_BODY);
                buf.put_u16(self.channel);
                buf.put_u32(data.len() as u32);
                buf.extend_from_slice(data);
                buf.put_u8(FRAME_END);
            }
            FramePayload::Heartbeat => {
                buf.put_u8(FRAME_HEARTBEAT);
                buf.put_u16(0);
                buf.put_u32(0);
                buf.put_u8(FRAME_END);
            }
        }
    }

    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 7 {
            return Err(ProtocolError::Incomplete {
                needed: 7,
                available: buf.remaining(),
            });
        }

        let frame_type = buf.get_u8();
        let channel = buf.get_u16();
        let size = buf.get_u32();

        if buf.remaining() < size as usize + 1 {
            return Err(ProtocolError::Incomplete {
                needed: size as usize + 1,
                available: buf.remaining(),
            });
        }

        let mut payload_bytes = buf.split_to(size as usize);
        let frame_end = buf.get_u8();

        if frame_end != FRAME_END {
            return Err(ProtocolError::InvalidFrameEnd(frame_end));
        }

        let payload = match frame_type {
            FRAME_METHOD => {
                let method = MethodFrame::decode(&mut payload_bytes)?;
                FramePayload::Method(method)
            }
            FRAME_HEADER => {
                let class_id = payload_bytes.get_u16();
                let _weight = payload_bytes.get_u16();
                let body_size = payload_bytes.get_u64();
                let properties = BasicProperties::decode(&mut payload_bytes)?;
                FramePayload::Header(ContentHeader {
                    class_id,
                    body_size,
                    properties,
                })
            }
            FRAME_BODY => FramePayload::Body(payload_bytes),
            FRAME_HEARTBEAT => FramePayload::Heartbeat,
            _ => return Err(ProtocolError::InvalidFrameType(frame_type)),
        };

        Ok(AMQPFrame { channel, payload })
    }
}

// --- Method Frame Encoding/Decoding ---

impl MethodFrame {
    pub fn class_method_id(&self) -> (u16, u16) {
        match self {
            Self::ConnectionStart(_) => (CLASS_CONNECTION, METHOD_CONNECTION_START),
            Self::ConnectionStartOk(_) => (CLASS_CONNECTION, METHOD_CONNECTION_START_OK),
            Self::ConnectionTune(_) => (CLASS_CONNECTION, METHOD_CONNECTION_TUNE),
            Self::ConnectionTuneOk(_) => (CLASS_CONNECTION, METHOD_CONNECTION_TUNE_OK),
            Self::ConnectionOpen(_) => (CLASS_CONNECTION, METHOD_CONNECTION_OPEN),
            Self::ConnectionOpenOk => (CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK),
            Self::ConnectionClose(_) => (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE),
            Self::ConnectionCloseOk => (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK),
            Self::ConnectionBlocked(_) => (CLASS_CONNECTION, METHOD_CONNECTION_BLOCKED),
            Self::ConnectionUnblocked => (CLASS_CONNECTION, METHOD_CONNECTION_UNBLOCKED),

            Self::ChannelOpen => (CLASS_CHANNEL, METHOD_CHANNEL_OPEN),
            Self::ChannelOpenOk => (CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK),
            Self::ChannelFlow(_) => (CLASS_CHANNEL, METHOD_CHANNEL_FLOW),
            Self::ChannelFlowOk(_) => (CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK),
            Self::ChannelClose(_) => (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE),
            Self::ChannelCloseOk => (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK),

            Self::ExchangeDeclare(_) => (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE),
            Self::ExchangeDeclareOk => (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK),
            Self::ExchangeDelete(_) => (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE),
            Self::ExchangeDeleteOk => (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE_OK),
            Self::ExchangeBind(_) => (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND),
            Self::ExchangeBindOk => (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND_OK),
            Self::ExchangeUnbind(_) => (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND),
            Self::ExchangeUnbindOk => (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND_OK),

            Self::QueueDeclare(_) => (CLASS_QUEUE, METHOD_QUEUE_DECLARE),
            Self::QueueDeclareOk(_) => (CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK),
            Self::QueueBind(_) => (CLASS_QUEUE, METHOD_QUEUE_BIND),
            Self::QueueBindOk => (CLASS_QUEUE, METHOD_QUEUE_BIND_OK),
            Self::QueueUnbind(_) => (CLASS_QUEUE, METHOD_QUEUE_UNBIND),
            Self::QueueUnbindOk => (CLASS_QUEUE, METHOD_QUEUE_UNBIND_OK),
            Self::QueuePurge(_) => (CLASS_QUEUE, METHOD_QUEUE_PURGE),
            Self::QueuePurgeOk(_) => (CLASS_QUEUE, METHOD_QUEUE_PURGE_OK),
            Self::QueueDelete(_) => (CLASS_QUEUE, METHOD_QUEUE_DELETE),
            Self::QueueDeleteOk(_) => (CLASS_QUEUE, METHOD_QUEUE_DELETE_OK),

            Self::BasicQos(_) => (CLASS_BASIC, METHOD_BASIC_QOS),
            Self::BasicQosOk => (CLASS_BASIC, METHOD_BASIC_QOS_OK),
            Self::BasicConsume(_) => (CLASS_BASIC, METHOD_BASIC_CONSUME),
            Self::BasicConsumeOk(_) => (CLASS_BASIC, METHOD_BASIC_CONSUME_OK),
            Self::BasicCancel(_) => (CLASS_BASIC, METHOD_BASIC_CANCEL),
            Self::BasicCancelOk(_) => (CLASS_BASIC, METHOD_BASIC_CANCEL_OK),
            Self::BasicPublish(_) => (CLASS_BASIC, METHOD_BASIC_PUBLISH),
            Self::BasicReturn(_) => (CLASS_BASIC, METHOD_BASIC_RETURN),
            Self::BasicDeliver(_) => (CLASS_BASIC, METHOD_BASIC_DELIVER),
            Self::BasicGet(_) => (CLASS_BASIC, METHOD_BASIC_GET),
            Self::BasicGetOk(_) => (CLASS_BASIC, METHOD_BASIC_GET_OK),
            Self::BasicGetEmpty => (CLASS_BASIC, METHOD_BASIC_GET_EMPTY),
            Self::BasicAck(_) => (CLASS_BASIC, METHOD_BASIC_ACK),
            Self::BasicReject(_) => (CLASS_BASIC, METHOD_BASIC_REJECT),
            Self::BasicRecoverAsync(_) => (CLASS_BASIC, METHOD_BASIC_RECOVER_ASYNC),
            Self::BasicRecover(_) => (CLASS_BASIC, METHOD_BASIC_RECOVER),
            Self::BasicRecoverOk => (CLASS_BASIC, METHOD_BASIC_RECOVER_OK),
            Self::BasicNack(_) => (CLASS_BASIC, METHOD_BASIC_NACK),

            Self::ConfirmSelect(_) => (CLASS_CONFIRM, METHOD_CONFIRM_SELECT),
            Self::ConfirmSelectOk => (CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK),

            Self::TxSelect => (CLASS_TX, METHOD_TX_SELECT),
            Self::TxSelectOk => (CLASS_TX, METHOD_TX_SELECT_OK),
            Self::TxCommit => (CLASS_TX, METHOD_TX_COMMIT),
            Self::TxCommitOk => (CLASS_TX, METHOD_TX_COMMIT_OK),
            Self::TxRollback => (CLASS_TX, METHOD_TX_ROLLBACK),
            Self::TxRollbackOk => (CLASS_TX, METHOD_TX_ROLLBACK_OK),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        let (class_id, method_id) = self.class_method_id();
        buf.put_u16(class_id);
        buf.put_u16(method_id);

        match self {
            Self::ConnectionStart(m) => {
                buf.put_u8(m.version_major);
                buf.put_u8(m.version_minor);
                m.server_properties.encode(buf);
                encode_long_string(buf, &m.mechanisms);
                encode_long_string(buf, &m.locales);
            }
            Self::ConnectionStartOk(m) => {
                m.client_properties.encode(buf);
                encode_short_string(buf, &m.mechanism);
                encode_long_string(buf, &m.response);
                encode_short_string(buf, &m.locale);
            }
            Self::ConnectionTune(m) => {
                buf.put_u16(m.channel_max);
                buf.put_u32(m.frame_max);
                buf.put_u16(m.heartbeat);
            }
            Self::ConnectionTuneOk(m) => {
                buf.put_u16(m.channel_max);
                buf.put_u32(m.frame_max);
                buf.put_u16(m.heartbeat);
            }
            Self::ConnectionOpen(m) => {
                encode_short_string(buf, &m.virtual_host);
                encode_short_string(buf, ""); // reserved (capabilities)
                buf.put_u8(0); // reserved (insist)
            }
            Self::ConnectionOpenOk => {
                encode_short_string(buf, ""); // reserved
            }
            Self::ConnectionClose(m) => {
                buf.put_u16(m.reply_code);
                encode_short_string(buf, &m.reply_text);
                buf.put_u16(m.class_id);
                buf.put_u16(m.method_id);
            }
            Self::ConnectionCloseOk => {}
            Self::ConnectionBlocked(m) => {
                encode_short_string(buf, &m.reason);
            }
            Self::ConnectionUnblocked => {}

            Self::ChannelOpen => {
                encode_short_string(buf, ""); // reserved
            }
            Self::ChannelOpenOk => {
                encode_long_string(buf, b""); // reserved
            }
            Self::ChannelFlow(active) => {
                buf.put_u8(if *active { 1 } else { 0 });
            }
            Self::ChannelFlowOk(active) => {
                buf.put_u8(if *active { 1 } else { 0 });
            }
            Self::ChannelClose(m) => {
                buf.put_u16(m.reply_code);
                encode_short_string(buf, &m.reply_text);
                buf.put_u16(m.class_id);
                buf.put_u16(m.method_id);
            }
            Self::ChannelCloseOk => {}

            Self::ExchangeDeclare(m) => {
                buf.put_u16(0); // reserved (ticket)
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.exchange_type);
                let mut bits = 0u8;
                if m.passive {
                    bits |= 1;
                }
                if m.durable {
                    bits |= 2;
                }
                if m.auto_delete {
                    bits |= 4;
                }
                if m.internal {
                    bits |= 8;
                }
                if m.no_wait {
                    bits |= 16;
                }
                buf.put_u8(bits);
                m.arguments.encode(buf);
            }
            Self::ExchangeDeclareOk => {}
            Self::ExchangeDelete(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.exchange);
                let mut bits = 0u8;
                if m.if_unused {
                    bits |= 1;
                }
                if m.no_wait {
                    bits |= 2;
                }
                buf.put_u8(bits);
            }
            Self::ExchangeDeleteOk => {}
            Self::ExchangeBind(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.destination);
                encode_short_string(buf, &m.source);
                encode_short_string(buf, &m.routing_key);
                buf.put_u8(if m.no_wait { 1 } else { 0 });
                m.arguments.encode(buf);
            }
            Self::ExchangeBindOk => {}
            Self::ExchangeUnbind(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.destination);
                encode_short_string(buf, &m.source);
                encode_short_string(buf, &m.routing_key);
                buf.put_u8(if m.no_wait { 1 } else { 0 });
                m.arguments.encode(buf);
            }
            Self::ExchangeUnbindOk => {}

            Self::QueueDeclare(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                let mut bits = 0u8;
                if m.passive {
                    bits |= 1;
                }
                if m.durable {
                    bits |= 2;
                }
                if m.exclusive {
                    bits |= 4;
                }
                if m.auto_delete {
                    bits |= 8;
                }
                if m.no_wait {
                    bits |= 16;
                }
                buf.put_u8(bits);
                m.arguments.encode(buf);
            }
            Self::QueueDeclareOk(m) => {
                encode_short_string(buf, &m.queue);
                buf.put_u32(m.message_count);
                buf.put_u32(m.consumer_count);
            }
            Self::QueueBind(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
                buf.put_u8(if m.no_wait { 1 } else { 0 });
                m.arguments.encode(buf);
            }
            Self::QueueBindOk => {}
            Self::QueueUnbind(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
                m.arguments.encode(buf);
            }
            Self::QueueUnbindOk => {}
            Self::QueuePurge(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                buf.put_u8(if m.no_wait { 1 } else { 0 });
            }
            Self::QueuePurgeOk(count) => {
                buf.put_u32(*count);
            }
            Self::QueueDelete(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                let mut bits = 0u8;
                if m.if_unused {
                    bits |= 1;
                }
                if m.if_empty {
                    bits |= 2;
                }
                if m.no_wait {
                    bits |= 4;
                }
                buf.put_u8(bits);
            }
            Self::QueueDeleteOk(count) => {
                buf.put_u32(*count);
            }

            Self::BasicQos(m) => {
                buf.put_u32(m.prefetch_size);
                buf.put_u16(m.prefetch_count);
                buf.put_u8(if m.global { 1 } else { 0 });
            }
            Self::BasicQosOk => {}
            Self::BasicConsume(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                encode_short_string(buf, &m.consumer_tag);
                let mut bits = 0u8;
                if m.no_local {
                    bits |= 1;
                }
                if m.no_ack {
                    bits |= 2;
                }
                if m.exclusive {
                    bits |= 4;
                }
                if m.no_wait {
                    bits |= 8;
                }
                buf.put_u8(bits);
                m.arguments.encode(buf);
            }
            Self::BasicConsumeOk(tag) => {
                encode_short_string(buf, tag);
            }
            Self::BasicCancel(m) => {
                encode_short_string(buf, &m.consumer_tag);
                buf.put_u8(if m.no_wait { 1 } else { 0 });
            }
            Self::BasicCancelOk(tag) => {
                encode_short_string(buf, tag);
            }
            Self::BasicPublish(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
                let mut bits = 0u8;
                if m.mandatory {
                    bits |= 1;
                }
                if m.immediate {
                    bits |= 2;
                }
                buf.put_u8(bits);
            }
            Self::BasicReturn(m) => {
                buf.put_u16(m.reply_code);
                encode_short_string(buf, &m.reply_text);
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
            }
            Self::BasicDeliver(m) => {
                encode_short_string(buf, &m.consumer_tag);
                buf.put_u64(m.delivery_tag);
                buf.put_u8(if m.redelivered { 1 } else { 0 });
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
            }
            Self::BasicGet(m) => {
                buf.put_u16(0); // reserved
                encode_short_string(buf, &m.queue);
                buf.put_u8(if m.no_ack { 1 } else { 0 });
            }
            Self::BasicGetOk(m) => {
                buf.put_u64(m.delivery_tag);
                buf.put_u8(if m.redelivered { 1 } else { 0 });
                encode_short_string(buf, &m.exchange);
                encode_short_string(buf, &m.routing_key);
                buf.put_u32(m.message_count);
            }
            Self::BasicGetEmpty => {
                encode_short_string(buf, ""); // reserved (cluster-id)
            }
            Self::BasicAck(m) => {
                buf.put_u64(m.delivery_tag);
                buf.put_u8(if m.multiple { 1 } else { 0 });
            }
            Self::BasicReject(m) => {
                buf.put_u64(m.delivery_tag);
                buf.put_u8(if m.requeue { 1 } else { 0 });
            }
            Self::BasicRecoverAsync(requeue) | Self::BasicRecover(requeue) => {
                buf.put_u8(if *requeue { 1 } else { 0 });
            }
            Self::BasicRecoverOk => {}
            Self::BasicNack(m) => {
                buf.put_u64(m.delivery_tag);
                let mut bits = 0u8;
                if m.multiple {
                    bits |= 1;
                }
                if m.requeue {
                    bits |= 2;
                }
                buf.put_u8(bits);
            }

            Self::ConfirmSelect(no_wait) => {
                buf.put_u8(if *no_wait { 1 } else { 0 });
            }
            Self::ConfirmSelectOk => {}

            Self::TxSelect | Self::TxSelectOk | Self::TxCommit | Self::TxCommitOk
            | Self::TxRollback | Self::TxRollbackOk => {}
        }
    }

    pub fn decode(buf: &mut Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::Incomplete {
                needed: 4,
                available: buf.remaining(),
            });
        }
        let class_id = buf.get_u16();
        let method_id = buf.get_u16();

        match (class_id, method_id) {
            (CLASS_CONNECTION, METHOD_CONNECTION_START) => {
                let version_major = buf.get_u8();
                let version_minor = buf.get_u8();
                let server_properties = FieldTable::decode(buf)?;
                let mechanisms = decode_long_string(buf)?;
                let locales = decode_long_string(buf)?;
                Ok(Self::ConnectionStart(ConnectionStart {
                    version_major,
                    version_minor,
                    server_properties,
                    mechanisms,
                    locales,
                }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_START_OK) => {
                let client_properties = FieldTable::decode(buf)?;
                let mechanism = decode_short_string(buf)?;
                let response = decode_long_string(buf)?;
                let locale = decode_short_string(buf)?;
                Ok(Self::ConnectionStartOk(ConnectionStartOk {
                    client_properties,
                    mechanism,
                    response,
                    locale,
                }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_TUNE) => {
                let channel_max = buf.get_u16();
                let frame_max = buf.get_u32();
                let heartbeat = buf.get_u16();
                Ok(Self::ConnectionTune(ConnectionTune {
                    channel_max,
                    frame_max,
                    heartbeat,
                }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_TUNE_OK) => {
                let channel_max = buf.get_u16();
                let frame_max = buf.get_u32();
                let heartbeat = buf.get_u16();
                Ok(Self::ConnectionTuneOk(ConnectionTuneOk {
                    channel_max,
                    frame_max,
                    heartbeat,
                }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_OPEN) => {
                let virtual_host = decode_short_string(buf)?;
                let _reserved = decode_short_string(buf)?;
                let _insist = buf.get_u8();
                Ok(Self::ConnectionOpen(ConnectionOpen { virtual_host }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK) => {
                let _reserved = decode_short_string(buf)?;
                Ok(Self::ConnectionOpenOk)
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE) => {
                let reply_code = buf.get_u16();
                let reply_text = decode_short_string(buf)?;
                let class_id = buf.get_u16();
                let method_id = buf.get_u16();
                Ok(Self::ConnectionClose(ConnectionClose {
                    reply_code,
                    reply_text,
                    class_id,
                    method_id,
                }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK) => Ok(Self::ConnectionCloseOk),
            (CLASS_CONNECTION, METHOD_CONNECTION_BLOCKED) => {
                let reason = decode_short_string(buf)?;
                Ok(Self::ConnectionBlocked(ConnectionBlocked { reason }))
            }
            (CLASS_CONNECTION, METHOD_CONNECTION_UNBLOCKED) => Ok(Self::ConnectionUnblocked),

            (CLASS_CHANNEL, METHOD_CHANNEL_OPEN) => {
                let _reserved = decode_short_string(buf)?;
                Ok(Self::ChannelOpen)
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK) => {
                let _reserved = decode_long_string(buf)?;
                Ok(Self::ChannelOpenOk)
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_FLOW) => {
                let active = buf.get_u8() != 0;
                Ok(Self::ChannelFlow(active))
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK) => {
                let active = buf.get_u8() != 0;
                Ok(Self::ChannelFlowOk(active))
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE) => {
                let reply_code = buf.get_u16();
                let reply_text = decode_short_string(buf)?;
                let class_id = buf.get_u16();
                let method_id = buf.get_u16();
                Ok(Self::ChannelClose(ChannelClose {
                    reply_code,
                    reply_text,
                    class_id,
                    method_id,
                }))
            }
            (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK) => Ok(Self::ChannelCloseOk),

            (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) => {
                let _reserved = buf.get_u16();
                let exchange = decode_short_string(buf)?;
                let exchange_type = decode_short_string(buf)?;
                let bits = buf.get_u8();
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::ExchangeDeclare(ExchangeDeclare {
                    exchange,
                    exchange_type,
                    passive: bits & 1 != 0,
                    durable: bits & 2 != 0,
                    auto_delete: bits & 4 != 0,
                    internal: bits & 8 != 0,
                    no_wait: bits & 16 != 0,
                    arguments,
                }))
            }
            (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK) => Ok(Self::ExchangeDeclareOk),
            (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE) => {
                let _reserved = buf.get_u16();
                let exchange = decode_short_string(buf)?;
                let bits = buf.get_u8();
                Ok(Self::ExchangeDelete(ExchangeDelete {
                    exchange,
                    if_unused: bits & 1 != 0,
                    no_wait: bits & 2 != 0,
                }))
            }
            (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE_OK) => Ok(Self::ExchangeDeleteOk),
            (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND) => {
                let _reserved = buf.get_u16();
                let destination = decode_short_string(buf)?;
                let source = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let no_wait = buf.get_u8() != 0;
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::ExchangeBind(ExchangeBind {
                    destination,
                    source,
                    routing_key,
                    no_wait,
                    arguments,
                }))
            }
            (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND_OK) => Ok(Self::ExchangeBindOk),
            (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND) => {
                let _reserved = buf.get_u16();
                let destination = decode_short_string(buf)?;
                let source = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let no_wait = buf.get_u8() != 0;
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::ExchangeUnbind(ExchangeUnbind {
                    destination,
                    source,
                    routing_key,
                    no_wait,
                    arguments,
                }))
            }
            (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND_OK) => Ok(Self::ExchangeUnbindOk),

            (CLASS_QUEUE, METHOD_QUEUE_DECLARE) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let bits = buf.get_u8();
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::QueueDeclare(QueueDeclare {
                    queue,
                    passive: bits & 1 != 0,
                    durable: bits & 2 != 0,
                    exclusive: bits & 4 != 0,
                    auto_delete: bits & 8 != 0,
                    no_wait: bits & 16 != 0,
                    arguments,
                }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK) => {
                let queue = decode_short_string(buf)?;
                let message_count = buf.get_u32();
                let consumer_count = buf.get_u32();
                Ok(Self::QueueDeclareOk(QueueDeclareOk {
                    queue,
                    message_count,
                    consumer_count,
                }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_BIND) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let no_wait = buf.get_u8() != 0;
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::QueueBind(QueueBind {
                    queue,
                    exchange,
                    routing_key,
                    no_wait,
                    arguments,
                }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_BIND_OK) => Ok(Self::QueueBindOk),
            (CLASS_QUEUE, METHOD_QUEUE_UNBIND) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::QueueUnbind(QueueUnbind {
                    queue,
                    exchange,
                    routing_key,
                    arguments,
                }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_UNBIND_OK) => Ok(Self::QueueUnbindOk),
            (CLASS_QUEUE, METHOD_QUEUE_PURGE) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let no_wait = buf.get_u8() != 0;
                Ok(Self::QueuePurge(QueuePurge { queue, no_wait }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_PURGE_OK) => Ok(Self::QueuePurgeOk(buf.get_u32())),
            (CLASS_QUEUE, METHOD_QUEUE_DELETE) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let bits = buf.get_u8();
                Ok(Self::QueueDelete(QueueDelete {
                    queue,
                    if_unused: bits & 1 != 0,
                    if_empty: bits & 2 != 0,
                    no_wait: bits & 4 != 0,
                }))
            }
            (CLASS_QUEUE, METHOD_QUEUE_DELETE_OK) => Ok(Self::QueueDeleteOk(buf.get_u32())),

            (CLASS_BASIC, METHOD_BASIC_QOS) => {
                let prefetch_size = buf.get_u32();
                let prefetch_count = buf.get_u16();
                let global = buf.get_u8() != 0;
                Ok(Self::BasicQos(BasicQos {
                    prefetch_size,
                    prefetch_count,
                    global,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_QOS_OK) => Ok(Self::BasicQosOk),
            (CLASS_BASIC, METHOD_BASIC_CONSUME) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let consumer_tag = decode_short_string(buf)?;
                let bits = buf.get_u8();
                let arguments = FieldTable::decode(buf)?;
                Ok(Self::BasicConsume(BasicConsume {
                    queue,
                    consumer_tag,
                    no_local: bits & 1 != 0,
                    no_ack: bits & 2 != 0,
                    exclusive: bits & 4 != 0,
                    no_wait: bits & 8 != 0,
                    arguments,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_CONSUME_OK) => {
                let tag = decode_short_string(buf)?;
                Ok(Self::BasicConsumeOk(tag))
            }
            (CLASS_BASIC, METHOD_BASIC_CANCEL) => {
                let consumer_tag = decode_short_string(buf)?;
                let no_wait = buf.get_u8() != 0;
                Ok(Self::BasicCancel(BasicCancel {
                    consumer_tag,
                    no_wait,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_CANCEL_OK) => {
                let tag = decode_short_string(buf)?;
                Ok(Self::BasicCancelOk(tag))
            }
            (CLASS_BASIC, METHOD_BASIC_PUBLISH) => {
                let _reserved = buf.get_u16();
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let bits = buf.get_u8();
                Ok(Self::BasicPublish(BasicPublish {
                    exchange,
                    routing_key,
                    mandatory: bits & 1 != 0,
                    immediate: bits & 2 != 0,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_RETURN) => {
                let reply_code = buf.get_u16();
                let reply_text = decode_short_string(buf)?;
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                Ok(Self::BasicReturn(BasicReturn {
                    reply_code,
                    reply_text,
                    exchange,
                    routing_key,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_DELIVER) => {
                let consumer_tag = decode_short_string(buf)?;
                let delivery_tag = buf.get_u64();
                let redelivered = buf.get_u8() != 0;
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                Ok(Self::BasicDeliver(BasicDeliver {
                    consumer_tag,
                    delivery_tag,
                    redelivered,
                    exchange,
                    routing_key,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_GET) => {
                let _reserved = buf.get_u16();
                let queue = decode_short_string(buf)?;
                let no_ack = buf.get_u8() != 0;
                Ok(Self::BasicGet(BasicGet { queue, no_ack }))
            }
            (CLASS_BASIC, METHOD_BASIC_GET_OK) => {
                let delivery_tag = buf.get_u64();
                let redelivered = buf.get_u8() != 0;
                let exchange = decode_short_string(buf)?;
                let routing_key = decode_short_string(buf)?;
                let message_count = buf.get_u32();
                Ok(Self::BasicGetOk(BasicGetOk {
                    delivery_tag,
                    redelivered,
                    exchange,
                    routing_key,
                    message_count,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_GET_EMPTY) => {
                let _reserved = decode_short_string(buf)?;
                Ok(Self::BasicGetEmpty)
            }
            (CLASS_BASIC, METHOD_BASIC_ACK) => {
                let delivery_tag = buf.get_u64();
                let multiple = buf.get_u8() != 0;
                Ok(Self::BasicAck(BasicAck {
                    delivery_tag,
                    multiple,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_REJECT) => {
                let delivery_tag = buf.get_u64();
                let requeue = buf.get_u8() != 0;
                Ok(Self::BasicReject(BasicReject {
                    delivery_tag,
                    requeue,
                }))
            }
            (CLASS_BASIC, METHOD_BASIC_RECOVER_ASYNC) => {
                let requeue = buf.get_u8() != 0;
                Ok(Self::BasicRecoverAsync(requeue))
            }
            (CLASS_BASIC, METHOD_BASIC_RECOVER) => {
                let requeue = buf.get_u8() != 0;
                Ok(Self::BasicRecover(requeue))
            }
            (CLASS_BASIC, METHOD_BASIC_RECOVER_OK) => Ok(Self::BasicRecoverOk),
            (CLASS_BASIC, METHOD_BASIC_NACK) => {
                let delivery_tag = buf.get_u64();
                let bits = buf.get_u8();
                Ok(Self::BasicNack(BasicNack {
                    delivery_tag,
                    multiple: bits & 1 != 0,
                    requeue: bits & 2 != 0,
                }))
            }

            (CLASS_CONFIRM, METHOD_CONFIRM_SELECT) => {
                let no_wait = buf.get_u8() != 0;
                Ok(Self::ConfirmSelect(no_wait))
            }
            (CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK) => Ok(Self::ConfirmSelectOk),

            (CLASS_TX, METHOD_TX_SELECT) => Ok(Self::TxSelect),
            (CLASS_TX, METHOD_TX_SELECT_OK) => Ok(Self::TxSelectOk),
            (CLASS_TX, METHOD_TX_COMMIT) => Ok(Self::TxCommit),
            (CLASS_TX, METHOD_TX_COMMIT_OK) => Ok(Self::TxCommitOk),
            (CLASS_TX, METHOD_TX_ROLLBACK) => Ok(Self::TxRollback),
            (CLASS_TX, METHOD_TX_ROLLBACK_OK) => Ok(Self::TxRollbackOk),

            _ => Err(ProtocolError::UnknownMethod {
                class_id,
                method_id,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: &AMQPFrame) -> AMQPFrame {
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let mut bytes = buf.freeze();
        AMQPFrame::decode(&mut bytes).unwrap()
    }

    #[test]
    fn test_heartbeat_round_trip() {
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Heartbeat,
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_body_frame_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Body(Bytes::from_static(b"hello world")),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_content_header_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Header(ContentHeader {
                class_id: CLASS_BASIC,
                body_size: 42,
                properties: BasicProperties {
                    content_type: Some("text/plain".into()),
                    delivery_mode: Some(2),
                    ..Default::default()
                },
            }),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_connection_start_round_trip() {
        let mut props = FieldTable::new();
        props.insert(
            "product",
            crate::field_table::FieldValue::ShortString("RMQ".into()),
        );
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionStart(ConnectionStart {
                version_major: 0,
                version_minor: 9,
                server_properties: props,
                mechanisms: Bytes::from_static(b"PLAIN"),
                locales: Bytes::from_static(b"en_US"),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_connection_start_ok_round_trip() {
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionStartOk(ConnectionStartOk {
                client_properties: FieldTable::new(),
                mechanism: "PLAIN".into(),
                response: Bytes::from_static(b"\x00guest\x00guest"),
                locale: "en_US".into(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_connection_tune_round_trip() {
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionTune(ConnectionTune {
                channel_max: 2048,
                frame_max: 131072,
                heartbeat: 60,
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_exchange_declare_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::ExchangeDeclare(ExchangeDeclare {
                exchange: "test-exchange".into(),
                exchange_type: "direct".into(),
                passive: false,
                durable: true,
                auto_delete: false,
                internal: false,
                no_wait: false,
                arguments: FieldTable::new(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_queue_declare_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::QueueDeclare(QueueDeclare {
                queue: "test-queue".into(),
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                no_wait: false,
                arguments: FieldTable::new(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_basic_publish_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicPublish(BasicPublish {
                exchange: "amq.direct".into(),
                routing_key: "my-key".into(),
                mandatory: true,
                immediate: false,
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_basic_deliver_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicDeliver(BasicDeliver {
                consumer_tag: "ctag-1".into(),
                delivery_tag: 42,
                redelivered: false,
                exchange: "amq.direct".into(),
                routing_key: "my-key".into(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_basic_ack_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicAck(BasicAck {
                delivery_tag: 5,
                multiple: true,
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_basic_nack_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicNack(BasicNack {
                delivery_tag: 3,
                multiple: false,
                requeue: true,
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_queue_bind_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::QueueBind(QueueBind {
                queue: "my-queue".into(),
                exchange: "amq.direct".into(),
                routing_key: "my-key".into(),
                no_wait: false,
                arguments: FieldTable::new(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_basic_consume_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicConsume(BasicConsume {
                queue: "my-queue".into(),
                consumer_tag: "ctag-1".into(),
                no_local: false,
                no_ack: false,
                exclusive: false,
                no_wait: false,
                arguments: FieldTable::new(),
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_connection_close_round_trip() {
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionClose(ConnectionClose {
                reply_code: 200,
                reply_text: "Normal shutdown".into(),
                class_id: 0,
                method_id: 0,
            })),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_tx_methods_round_trip() {
        for method in [
            MethodFrame::TxSelect,
            MethodFrame::TxSelectOk,
            MethodFrame::TxCommit,
            MethodFrame::TxCommitOk,
            MethodFrame::TxRollback,
            MethodFrame::TxRollbackOk,
        ] {
            let frame = AMQPFrame {
                channel: 1,
                payload: FramePayload::Method(method),
            };
            assert_eq!(round_trip(&frame), frame);
        }
    }

    #[test]
    fn test_confirm_select_round_trip() {
        let frame = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::ConfirmSelect(false)),
        };
        assert_eq!(round_trip(&frame), frame);
    }

    #[test]
    fn test_invalid_frame_end() {
        let frame = AMQPFrame {
            channel: 0,
            payload: FramePayload::Heartbeat,
        };
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        // Corrupt the frame end byte
        let len = buf.len();
        buf[len - 1] = 0xFF;
        let mut bytes = buf.freeze();
        assert!(matches!(
            AMQPFrame::decode(&mut bytes),
            Err(ProtocolError::InvalidFrameEnd(0xFF))
        ));
    }

    #[test]
    fn test_unknown_method() {
        // Create a method frame with bogus class/method
        let mut buf = BytesMut::new();
        buf.put_u8(FRAME_METHOD);
        buf.put_u16(0); // channel
        buf.put_u32(4); // size = just class + method
        buf.put_u16(999); // unknown class
        buf.put_u16(999); // unknown method
        buf.put_u8(FRAME_END);
        let mut bytes = buf.freeze();
        assert!(matches!(
            AMQPFrame::decode(&mut bytes),
            Err(ProtocolError::UnknownMethod {
                class_id: 999,
                method_id: 999
            })
        ));
    }
}
