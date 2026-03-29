use bytes::Bytes;
use rmq_protocol::properties::BasicProperties;

/// A stored message with all metadata.
/// Uses reference-counted types for cheap cloning on the delivery fast-path.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    /// Unix timestamp when the message was stored.
    pub timestamp: i64,
    /// Exchange name the message was published to.
    pub exchange: String,
    /// Routing key used for the message.
    pub routing_key: String,
    /// AMQP message properties.
    pub properties: BasicProperties,
    /// Message body. Uses Bytes for zero-copy sharing between publisher and consumer.
    pub body: Bytes,
}

impl StoredMessage {
    /// Minimum on-disk size: timestamp(8) + exchange_len(1) + routing_key_len(1) +
    /// properties_flags(2) + bodysize(8) + body(min 0) = 20 bytes minimum.
    pub const MIN_BYTESIZE: usize = 20;

    /// Calculate the encoded byte size on disk.
    pub fn bytesize(&self) -> usize {
        8 // timestamp
        + 1 + self.exchange.len() // short string
        + 1 + self.routing_key.len() // short string
        + self.properties.encoded_size() // properties
        + 8 // bodysize
        + self.body.len() // body
    }
}
