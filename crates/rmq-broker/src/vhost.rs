use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use rmq_protocol::field_table::{FieldTable, FieldValue};
use rmq_storage::message::StoredMessage;
use rmq_storage::message_store::MessageStore;
use serde::{Deserialize, Serialize};

use crate::exchange::{
    DefaultExchange, Destination, DirectExchange, Exchange, ExchangeConfig, FanoutExchange,
    HeadersExchange, TopicExchange, create_exchange,
};
use crate::queue::{Queue, QueueConfig};

/// Persisted metadata for durable queues and exchanges.
#[derive(Debug, Serialize, Deserialize, Default)]
struct VHostMetadata {
    queues: Vec<QueueMeta>,
    exchanges: Vec<ExchangeMeta>,
    bindings: Vec<BindingMeta>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueueMeta {
    name: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    #[serde(default)]
    arguments: SerializableFieldTable,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExchangeMeta {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    #[serde(default)]
    arguments: SerializableFieldTable,
}

#[derive(Debug, Serialize, Deserialize)]
struct BindingMeta {
    destination: String,
    #[serde(default)]
    destination_type: DestinationType,
    exchange: String,
    routing_key: String,
    #[serde(default)]
    arguments: SerializableFieldTable,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
enum DestinationType {
    #[default]
    Queue,
    Exchange,
}

/// A serde-compatible representation of FieldTable.
/// We store field values as JSON values for persistence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
struct SerializableFieldTable(BTreeMap<String, serde_json::Value>);

impl From<&FieldTable> for SerializableFieldTable {
    fn from(ft: &FieldTable) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in &ft.0 {
            map.insert(key.clone(), field_value_to_json(value));
        }
        SerializableFieldTable(map)
    }
}

impl SerializableFieldTable {
    fn to_field_table(&self) -> FieldTable {
        let mut ft = FieldTable::new();
        for (key, value) in &self.0 {
            if let Some(fv) = json_to_field_value(value) {
                ft.insert(key.clone(), fv);
            }
        }
        ft
    }
}

fn field_value_to_json(value: &FieldValue) -> serde_json::Value {
    match value {
        FieldValue::Bool(v) => serde_json::Value::Bool(*v),
        FieldValue::I8(v) => serde_json::json!({"type": "i8", "value": v}),
        FieldValue::U8(v) => serde_json::json!({"type": "u8", "value": v}),
        FieldValue::I16(v) => serde_json::json!({"type": "i16", "value": v}),
        FieldValue::U16(v) => serde_json::json!({"type": "u16", "value": v}),
        FieldValue::I32(v) => serde_json::json!({"type": "i32", "value": v}),
        FieldValue::U32(v) => serde_json::json!({"type": "u32", "value": v}),
        FieldValue::I64(v) => serde_json::json!({"type": "i64", "value": v}),
        FieldValue::U64(v) => serde_json::json!({"type": "u64", "value": v}),
        FieldValue::F32(v) => serde_json::json!({"type": "f32", "value": v}),
        FieldValue::F64(v) => serde_json::json!({"type": "f64", "value": v}),
        FieldValue::ShortString(s) => serde_json::Value::String(s.clone()),
        FieldValue::LongString(b) => {
            serde_json::json!({"type": "longstring", "value": base64_encode(b)})
        }
        FieldValue::Timestamp(v) => serde_json::json!({"type": "timestamp", "value": v}),
        FieldValue::FieldTable(t) => {
            serde_json::json!({"type": "table", "value": SerializableFieldTable::from(t)})
        }
        FieldValue::FieldArray(arr) => {
            let items: Vec<serde_json::Value> = arr.iter().map(field_value_to_json).collect();
            serde_json::json!({"type": "array", "value": items})
        }
        FieldValue::Void => serde_json::Value::Null,
    }
}

fn json_to_field_value(value: &serde_json::Value) -> Option<FieldValue> {
    match value {
        serde_json::Value::Null => Some(FieldValue::Void),
        serde_json::Value::Bool(v) => Some(FieldValue::Bool(*v)),
        serde_json::Value::String(s) => Some(FieldValue::ShortString(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(FieldValue::I64(i))
            } else {
                n.as_f64().map(FieldValue::F64)
            }
        }
        serde_json::Value::Object(obj) => {
            let type_str = obj.get("type")?.as_str()?;
            let val = obj.get("value")?;
            match type_str {
                "i8" => Some(FieldValue::I8(val.as_i64()? as i8)),
                "u8" => Some(FieldValue::U8(val.as_u64()? as u8)),
                "i16" => Some(FieldValue::I16(val.as_i64()? as i16)),
                "u16" => Some(FieldValue::U16(val.as_u64()? as u16)),
                "i32" => Some(FieldValue::I32(val.as_i64()? as i32)),
                "u32" => Some(FieldValue::U32(val.as_u64()? as u32)),
                "i64" => Some(FieldValue::I64(val.as_i64()?)),
                "u64" => Some(FieldValue::U64(val.as_u64()?)),
                "f32" => Some(FieldValue::F32(val.as_f64()? as f32)),
                "f64" => Some(FieldValue::F64(val.as_f64()?)),
                "timestamp" => Some(FieldValue::Timestamp(val.as_u64()?)),
                "longstring" => {
                    let encoded = val.as_str()?;
                    let decoded = base64_decode(encoded)?;
                    Some(FieldValue::LongString(bytes::Bytes::from(decoded)))
                }
                "table" => {
                    let sft: SerializableFieldTable = serde_json::from_value(val.clone()).ok()?;
                    Some(FieldValue::FieldTable(sft.to_field_table()))
                }
                "array" => {
                    let arr = val.as_array()?;
                    let items: Vec<FieldValue> =
                        arr.iter().filter_map(json_to_field_value).collect();
                    Some(FieldValue::FieldArray(items))
                }
                _ => None,
            }
        }
        serde_json::Value::Array(_) => None,
    }
}

fn base64_encode(data: &[u8]) -> String {
    // Simple base64 encoding without external dependency
    use std::fmt::Write;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(
            result,
            "{}",
            CHARS[((triple >> 18) & 0x3F) as usize] as char
        );
        let _ = write!(
            result,
            "{}",
            CHARS[((triple >> 12) & 0x3F) as usize] as char
        );
        if chunk.len() > 1 {
            let _ = write!(result, "{}", CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            let _ = write!(result, "{}", CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(data: &str) -> Option<Vec<u8>> {
    fn decode_char(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            b'=' => Some(0),
            _ => None,
        }
    }
    let bytes = data.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut result = Vec::new();
    for chunk in bytes.chunks(4) {
        let a = decode_char(chunk[0])?;
        let b = decode_char(chunk[1])?;
        let c = decode_char(chunk[2])?;
        let d = decode_char(chunk[3])?;
        let triple = (a << 18) | (b << 12) | (c << 6) | d;
        result.push((triple >> 16) as u8);
        if chunk[2] != b'=' {
            result.push((triple >> 8) as u8);
        }
        if chunk[3] != b'=' {
            result.push(triple as u8);
        }
    }
    Some(result)
}

/// A virtual host containing exchanges, queues, and their bindings.
pub struct VHost {
    name: String,
    data_dir: PathBuf,
    exchanges: RwLock<HashMap<String, Arc<RwLock<Box<dyn Exchange>>>>>,
    queues: RwLock<HashMap<String, Arc<Queue>>>,
}

impl VHost {
    pub fn new(name: String, data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let vhost = Self {
            name,
            data_dir,
            exchanges: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
        };

        // Create default exchanges
        vhost.declare_default_exchanges();

        // Restore persisted metadata (durable queues, exchanges, bindings)
        vhost.load_metadata();

        Ok(vhost)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn declare_default_exchanges(&self) {
        let mut exchanges = self.exchanges.write();

        // Default (nameless) exchange
        exchanges.insert(
            String::new(),
            Arc::new(RwLock::new(
                Box::new(DefaultExchange::new()) as Box<dyn Exchange>
            )),
        );

        // amq.direct
        exchanges.insert(
            "amq.direct".into(),
            Arc::new(RwLock::new(Box::new(DirectExchange::new(ExchangeConfig {
                name: "amq.direct".into(),
                exchange_type: "direct".into(),
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })) as Box<dyn Exchange>)),
        );

        // amq.fanout
        exchanges.insert(
            "amq.fanout".into(),
            Arc::new(RwLock::new(Box::new(FanoutExchange::new(ExchangeConfig {
                name: "amq.fanout".into(),
                exchange_type: "fanout".into(),
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })) as Box<dyn Exchange>)),
        );

        // amq.topic
        exchanges.insert(
            "amq.topic".into(),
            Arc::new(RwLock::new(Box::new(TopicExchange::new(ExchangeConfig {
                name: "amq.topic".into(),
                exchange_type: "topic".into(),
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })) as Box<dyn Exchange>)),
        );

        // amq.headers
        exchanges.insert(
            "amq.headers".into(),
            Arc::new(RwLock::new(Box::new(HeadersExchange::new(ExchangeConfig {
                name: "amq.headers".into(),
                exchange_type: "headers".into(),
                durable: true,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })) as Box<dyn Exchange>)),
        );
    }

    /// Declare an exchange. Returns Ok(true) if created, Ok(false) if already existed.
    pub fn declare_exchange(&self, config: ExchangeConfig) -> Result<bool, VHostError> {
        let mut exchanges = self.exchanges.write();

        if let Some(existing) = exchanges.get(&config.name) {
            let existing = existing.read();
            // Verify properties match
            if existing.exchange_type() != config.exchange_type {
                return Err(VHostError::PreconditionFailed(format!(
                    "exchange '{}' already exists with type '{}'",
                    config.name,
                    existing.exchange_type()
                )));
            }
            return Ok(false);
        }

        let exchange = create_exchange(config.clone())
            .map_err(|e| VHostError::ExchangeError(e.to_string()))?;
        exchanges.insert(config.name.clone(), Arc::new(RwLock::new(exchange)));
        drop(exchanges);
        if let Err(e) = self.save_metadata() {
            tracing::error!("failed to save metadata after exchange declaration: {e}");
        }
        Ok(true)
    }

    /// Delete an exchange.
    pub fn delete_exchange(&self, name: &str) -> Result<(), VHostError> {
        if name.is_empty() || name.starts_with("amq.") {
            return Err(VHostError::AccessRefused(
                "cannot delete built-in exchange".into(),
            ));
        }

        let mut exchanges = self.exchanges.write();
        if exchanges.remove(name).is_none() {
            return Err(VHostError::NotFound(format!("exchange '{name}' not found")));
        }
        drop(exchanges);
        if let Err(e) = self.save_metadata() {
            tracing::error!("failed to save metadata after exchange deletion: {e}");
        }
        Ok(())
    }

    /// Get an exchange by name.
    pub fn get_exchange(&self, name: &str) -> Option<Arc<RwLock<Box<dyn Exchange>>>> {
        self.exchanges.read().get(name).cloned()
    }

    /// Check if an exchange exists.
    pub fn exchange_exists(&self, name: &str) -> bool {
        self.exchanges.read().contains_key(name)
    }

    /// Declare a queue.
    pub fn declare_queue(&self, config: QueueConfig) -> Result<Arc<Queue>, VHostError> {
        let mut queues = self.queues.write();

        if let Some(existing) = queues.get(&config.name) {
            // Check for property mismatch
            if existing.is_durable() != config.durable
                || existing.is_exclusive() != config.exclusive
                || existing.is_auto_delete() != config.auto_delete
            {
                return Err(VHostError::PreconditionFailed(format!(
                    "queue '{}' already exists with different properties",
                    config.name
                )));
            }
            return Ok(existing.clone());
        }

        let queue_dir = if config.durable {
            self.data_dir.join(queue_dir_name(&config.name))
        } else {
            self.data_dir
                .join("transient")
                .join(queue_dir_name(&config.name))
        };

        let store =
            MessageStore::open(&queue_dir).map_err(|e| VHostError::InternalError(e.to_string()))?;

        let queue = Arc::new(Queue::new(config.clone(), store));
        queues.insert(config.name.clone(), queue.clone());
        drop(queues);
        if config.durable {
            if let Err(e) = self.save_metadata() {
                tracing::error!("failed to save metadata after queue declaration: {e}");
            }
        }
        Ok(queue)
    }

    /// Delete a queue.
    pub fn delete_queue(&self, name: &str) -> Result<u32, VHostError> {
        let mut queues = self.queues.write();
        let queue = queues
            .remove(name)
            .ok_or_else(|| VHostError::NotFound(format!("queue '{name}' not found")))?;

        let count = queue.message_count() as u32;

        // Remove all bindings for this queue from all exchanges
        let dest = Destination::Queue(name.to_string());
        let exchanges = self.exchanges.read();
        for (_, exchange) in exchanges.iter() {
            let mut ex = exchange.write();
            ex.unbind_destination(&dest);
        }
        drop(exchanges);
        drop(queues);
        if let Err(e) = self.save_metadata() {
            tracing::error!("failed to save metadata after queue deletion: {e}");
        }

        Ok(count)
    }

    /// Get a queue by name.
    pub fn get_queue(&self, name: &str) -> Option<Arc<Queue>> {
        self.queues.read().get(name).cloned()
    }

    /// Check if a queue exists.
    pub fn queue_exists(&self, name: &str) -> bool {
        self.queues.read().contains_key(name)
    }

    /// Bind a queue to an exchange.
    pub fn bind_queue(
        &self,
        queue_name: &str,
        exchange_name: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> Result<(), VHostError> {
        // Verify queue exists
        if !self.queue_exists(queue_name) {
            return Err(VHostError::NotFound(format!(
                "queue '{queue_name}' not found"
            )));
        }

        let exchange = self
            .get_exchange(exchange_name)
            .ok_or_else(|| VHostError::NotFound(format!("exchange '{exchange_name}' not found")))?;

        exchange.write().bind(
            Destination::Queue(queue_name.to_string()),
            routing_key,
            arguments,
        );

        Ok(())
    }

    /// Unbind a queue from an exchange.
    pub fn unbind_queue(
        &self,
        queue_name: &str,
        exchange_name: &str,
        routing_key: &str,
        arguments: &FieldTable,
    ) -> Result<(), VHostError> {
        let exchange = self
            .get_exchange(exchange_name)
            .ok_or_else(|| VHostError::NotFound(format!("exchange '{exchange_name}' not found")))?;

        exchange.write().unbind(
            &Destination::Queue(queue_name.to_string()),
            routing_key,
            arguments,
        );

        Ok(())
    }

    /// Publish a message to an exchange. Returns the number of queues that received it.
    pub fn publish(
        &self,
        exchange_name: &str,
        routing_key: &str,
        msg: &StoredMessage,
    ) -> Result<u32, VHostError> {
        let exchange = self
            .get_exchange(exchange_name)
            .ok_or_else(|| VHostError::NotFound(format!("exchange '{exchange_name}' not found")))?;

        let destinations = exchange
            .read()
            .route(routing_key, msg.properties.headers.as_ref());

        let queues = self.queues.read();
        let mut count = 0u32;
        let mut dead_letters_to_publish = Vec::new();

        for dest in &destinations {
            match dest {
                Destination::Queue(name) => {
                    if let Some(queue) = queues.get(name) {
                        let (result, dead_letters) = queue
                            .publish(msg)
                            .map_err(|e| VHostError::InternalError(e.to_string()))?;
                        if matches!(result, crate::queue::PublishResult::Accepted(_)) {
                            count += 1;
                        }
                        dead_letters_to_publish.extend(dead_letters);
                    }
                }
                Destination::Exchange(_name) => {
                    // Exchange-to-exchange routing
                }
            }
        }

        // Drop the queues read lock before re-publishing dead letters
        drop(queues);

        // Re-publish dead-lettered messages to their DLX
        for dl in dead_letters_to_publish {
            if let Err(e) = self.publish(&dl.exchange, &dl.routing_key, &dl.message) {
                tracing::error!(
                    "failed to republish dead-lettered message to exchange '{}': {e}",
                    dl.exchange
                );
            }
        }

        Ok(count)
    }

    /// List all exchange names.
    pub fn exchange_names(&self) -> Vec<String> {
        self.exchanges.read().keys().cloned().collect()
    }

    /// List all queue names.
    pub fn queue_names(&self) -> Vec<String> {
        self.queues.read().keys().cloned().collect()
    }

    // --- Metadata persistence ---

    fn metadata_path(&self) -> PathBuf {
        self.data_dir.join("metadata.json")
    }

    /// Persist current durable queue/exchange/binding metadata to disk.
    fn save_metadata(&self) -> Result<(), VHostError> {
        let mut meta = VHostMetadata::default();

        // Save durable exchanges (skip builtins) and collect their bindings
        for name in self.exchange_names() {
            if let Some(ex_arc) = self.get_exchange(&name) {
                let ex = ex_arc.read();

                // Save exchange definition (skip builtins)
                if !name.is_empty() && !name.starts_with("amq.") && ex.is_durable() {
                    meta.exchanges.push(ExchangeMeta {
                        name: ex.name().to_string(),
                        exchange_type: ex.exchange_type().to_string(),
                        durable: true,
                        auto_delete: ex.is_auto_delete(),
                        internal: ex.is_internal(),
                        arguments: SerializableFieldTable::from(ex.arguments()),
                    });
                }

                // Collect bindings from ALL exchanges (including builtins)
                for (dest, routing_key, arguments) in ex.bindings() {
                    let (dest_name, dest_type) = match &dest {
                        Destination::Queue(q) => (q.clone(), DestinationType::Queue),
                        Destination::Exchange(e) => (e.clone(), DestinationType::Exchange),
                    };
                    meta.bindings.push(BindingMeta {
                        destination: dest_name,
                        destination_type: dest_type,
                        exchange: name.clone(),
                        routing_key,
                        arguments: SerializableFieldTable::from(&arguments),
                    });
                }
            }
        }

        // Save durable queues
        for name in self.queue_names() {
            if let Some(q) = self.get_queue(&name) {
                if q.is_durable() {
                    meta.queues.push(QueueMeta {
                        name: q.name().to_string(),
                        durable: true,
                        exclusive: q.is_exclusive(),
                        auto_delete: q.is_auto_delete(),
                        arguments: SerializableFieldTable::from(q.arguments()),
                    });
                }
            }
        }

        // Write atomically via temp file
        let path = self.metadata_path();
        let data = serde_json::to_string_pretty(&meta)
            .map_err(|e| VHostError::InternalError(format!("failed to serialize metadata: {e}")))?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &data).map_err(|e| {
            tracing::error!("failed to write metadata temp file: {e}");
            VHostError::InternalError(format!("failed to write metadata: {e}"))
        })?;
        std::fs::rename(&tmp, &path).map_err(|e| {
            tracing::error!("failed to rename metadata file: {e}");
            VHostError::InternalError(format!("failed to rename metadata: {e}"))
        })?;
        Ok(())
    }

    /// Load persisted metadata and re-declare durable queues/exchanges.
    fn load_metadata(&self) {
        let path = self.metadata_path();
        if !path.exists() {
            return;
        }

        let data = match std::fs::read_to_string(&path) {
            Ok(d) => d,
            Err(_) => return,
        };

        let meta: VHostMetadata = match serde_json::from_str(&data) {
            Ok(m) => m,
            Err(_) => return,
        };

        // Re-declare exchanges
        for ex in &meta.exchanges {
            if let Err(e) = self.declare_exchange(ExchangeConfig {
                name: ex.name.clone(),
                exchange_type: ex.exchange_type.clone(),
                durable: ex.durable,
                auto_delete: ex.auto_delete,
                internal: ex.internal,
                arguments: ex.arguments.to_field_table(),
            }) {
                tracing::error!("failed to restore exchange '{}': {e}", ex.name);
            }
        }

        // Re-declare queues (this reopens their message stores)
        for q in &meta.queues {
            if let Err(e) = self.declare_queue(QueueConfig {
                name: q.name.clone(),
                durable: q.durable,
                exclusive: q.exclusive,
                auto_delete: q.auto_delete,
                arguments: q.arguments.to_field_table(),
            }) {
                tracing::error!("failed to restore queue '{}': {e}", q.name);
            }
        }

        // Re-create bindings
        for b in &meta.bindings {
            let arguments = b.arguments.to_field_table();
            match b.destination_type {
                DestinationType::Queue => {
                    if let Err(e) =
                        self.bind_queue(&b.destination, &b.exchange, &b.routing_key, &arguments)
                    {
                        tracing::error!(
                            "failed to restore binding {}->{}: {e}",
                            b.exchange,
                            b.destination
                        );
                    }
                }
                DestinationType::Exchange => {
                    // Exchange-to-exchange binding
                    if let Some(exchange) = self.get_exchange(&b.exchange) {
                        exchange.write().bind(
                            Destination::Exchange(b.destination.clone()),
                            &b.routing_key,
                            &arguments,
                        );
                    }
                }
            }
        }

        if !meta.queues.is_empty() || !meta.exchanges.is_empty() {
            tracing::info!(
                "vhost '{}': restored {} queues, {} exchanges, {} bindings from metadata",
                self.name,
                meta.queues.len(),
                meta.exchanges.len(),
                meta.bindings.len(),
            );
        }
    }
}

/// Generate a directory name for queue storage (SHA1 of queue name, for short paths).
fn queue_dir_name(queue_name: &str) -> String {
    // Simple hash for directory naming
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    queue_name.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[derive(Debug, thiserror::Error)]
pub enum VHostError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    #[error("access refused: {0}")]
    AccessRefused(String),
    #[error("exchange error: {0}")]
    ExchangeError(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rmq_protocol::properties::BasicProperties;
    use tempfile::TempDir;

    fn make_msg(body: &str) -> StoredMessage {
        StoredMessage {
            timestamp: 0,
            exchange: "amq.direct".into(),
            routing_key: "test".into(),
            properties: BasicProperties::default(),
            body: Bytes::from(body.as_bytes().to_vec()),
        }
    }

    #[test]
    fn test_default_exchanges_exist() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        assert!(vhost.exchange_exists(""));
        assert!(vhost.exchange_exists("amq.direct"));
        assert!(vhost.exchange_exists("amq.fanout"));
        assert!(vhost.exchange_exists("amq.topic"));
        assert!(vhost.exchange_exists("amq.headers"));
    }

    #[test]
    fn test_declare_and_use_queue() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        let queue = vhost
            .declare_queue(QueueConfig {
                name: "test-queue".into(),
                durable: true,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        assert_eq!(queue.name(), "test-queue");
        assert!(vhost.queue_exists("test-queue"));
    }

    #[test]
    fn test_publish_via_default_exchange() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        vhost
            .declare_queue(QueueConfig {
                name: "my-queue".into(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        // Publish via default exchange with routing key = queue name
        let count = vhost.publish("", "my-queue", &make_msg("hello")).unwrap();
        assert_eq!(count, 1);

        let queue = vhost.get_queue("my-queue").unwrap();
        let env = queue.shift().unwrap().0.unwrap();
        assert_eq!(&env.message.body[..], b"hello");
    }

    #[test]
    fn test_direct_exchange_bind_and_publish() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        vhost
            .declare_queue(QueueConfig {
                name: "q1".into(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        vhost
            .bind_queue("q1", "amq.direct", "my-key", &FieldTable::new())
            .unwrap();

        let count = vhost
            .publish("amq.direct", "my-key", &make_msg("routed"))
            .unwrap();
        assert_eq!(count, 1);

        let queue = vhost.get_queue("q1").unwrap();
        let env = queue.shift().unwrap().0.unwrap();
        assert_eq!(&env.message.body[..], b"routed");
    }

    #[test]
    fn test_fanout_exchange() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        for name in &["q1", "q2", "q3"] {
            vhost
                .declare_queue(QueueConfig {
                    name: (*name).into(),
                    durable: false,
                    exclusive: false,
                    auto_delete: false,
                    arguments: FieldTable::new(),
                })
                .unwrap();
            vhost
                .bind_queue(name, "amq.fanout", "", &FieldTable::new())
                .unwrap();
        }

        let count = vhost
            .publish("amq.fanout", "ignored", &make_msg("broadcast"))
            .unwrap();
        assert_eq!(count, 3);

        for name in &["q1", "q2", "q3"] {
            let queue = vhost.get_queue(*name).unwrap();
            let env = queue.shift().unwrap().0.unwrap();
            assert_eq!(&env.message.body[..], b"broadcast");
        }
    }

    #[test]
    fn test_delete_queue() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        vhost
            .declare_queue(QueueConfig {
                name: "temp".into(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        assert!(vhost.queue_exists("temp"));
        vhost.delete_queue("temp").unwrap();
        assert!(!vhost.queue_exists("temp"));
    }

    #[test]
    fn test_exchange_precondition_failed() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        vhost
            .declare_exchange(ExchangeConfig {
                name: "my-ex".into(),
                exchange_type: "direct".into(),
                durable: false,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        // Try to redeclare with different type
        let err = vhost
            .declare_exchange(ExchangeConfig {
                name: "my-ex".into(),
                exchange_type: "fanout".into(),
                durable: false,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })
            .unwrap_err();

        assert!(matches!(err, VHostError::PreconditionFailed(_)));
    }

    #[test]
    fn test_binding_persistence_round_trip() {
        let dir = TempDir::new().unwrap();

        // Create vhost with a durable exchange, durable queue, and a binding
        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();
            vhost
                .declare_exchange(ExchangeConfig {
                    name: "persist-ex".into(),
                    exchange_type: "direct".into(),
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    arguments: FieldTable::new(),
                })
                .unwrap();
            vhost
                .declare_queue(QueueConfig {
                    name: "persist-q".into(),
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    arguments: FieldTable::new(),
                })
                .unwrap();
            vhost
                .bind_queue("persist-q", "persist-ex", "persist-key", &FieldTable::new())
                .unwrap();

            // Also bind to a builtin exchange
            vhost
                .bind_queue("persist-q", "amq.direct", "builtin-key", &FieldTable::new())
                .unwrap();

            // Verify binding works before persistence
            let ex = vhost.get_exchange("persist-ex").unwrap();
            assert_eq!(ex.read().binding_count(), 1);

            // Save metadata explicitly to capture bindings
            vhost.save_metadata().unwrap();
        }

        // Create a new vhost from the same directory — should restore everything
        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();

            // Exchange should exist
            assert!(vhost.exchange_exists("persist-ex"));

            // Queue should exist
            assert!(vhost.queue_exists("persist-q"));

            // Binding should be restored — verify by routing
            let ex = vhost.get_exchange("persist-ex").unwrap();
            let destinations = ex.read().route("persist-key", None);
            assert_eq!(destinations.len(), 1);
            assert!(destinations.contains(&Destination::Queue("persist-q".into())));

            // Builtin exchange binding should also be restored
            let amq = vhost.get_exchange("amq.direct").unwrap();
            let destinations = amq.read().route("builtin-key", None);
            assert_eq!(destinations.len(), 1);
            assert!(destinations.contains(&Destination::Queue("persist-q".into())));
        }
    }

    #[test]
    fn test_queue_argument_persistence() {
        let dir = TempDir::new().unwrap();

        // Create a queue with arguments
        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();
            let mut args = FieldTable::new();
            args.insert("x-message-ttl", FieldValue::I32(60000));
            args.insert("x-max-length", FieldValue::I32(1000));
            args.insert(
                "x-dead-letter-exchange",
                FieldValue::ShortString("my-dlx".into()),
            );
            vhost
                .declare_queue(QueueConfig {
                    name: "args-q".into(),
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    arguments: args,
                })
                .unwrap();
        }

        // Restore and verify arguments
        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();
            let queue = vhost.get_queue("args-q").unwrap();
            let args = queue.arguments();

            // After JSON round-trip, integer types normalize to I64
            // Verify the value is correct regardless of exact integer variant
            match args.get("x-max-length") {
                Some(FieldValue::I32(v)) => assert_eq!(*v, 1000),
                Some(FieldValue::I64(v)) => assert_eq!(*v, 1000),
                other => panic!("expected integer 1000, got {:?}", other),
            }
            assert_eq!(
                args.get("x-dead-letter-exchange"),
                Some(&FieldValue::ShortString("my-dlx".into()))
            );
            // Verify the queue actually uses the arguments
            assert_eq!(queue.dead_letter_exchange(), Some("my-dlx"));
        }
    }

    #[test]
    fn test_exchange_argument_persistence() {
        let dir = TempDir::new().unwrap();

        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();
            let mut args = FieldTable::new();
            args.insert(
                "alternate-exchange",
                FieldValue::ShortString("alt-ex".into()),
            );
            vhost
                .declare_exchange(ExchangeConfig {
                    name: "args-ex".into(),
                    exchange_type: "direct".into(),
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    arguments: args,
                })
                .unwrap();
        }

        {
            let vhost = VHost::new("/".into(), dir.path()).unwrap();
            let ex = vhost.get_exchange("args-ex").unwrap();
            let args = ex.read().arguments().clone();
            assert_eq!(
                args.get("alternate-exchange"),
                Some(&FieldValue::ShortString("alt-ex".into()))
            );
        }
    }

    #[test]
    fn test_unknown_exchange_type_rejected() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        let err = vhost
            .declare_exchange(ExchangeConfig {
                name: "bad-ex".into(),
                exchange_type: "nonexistent".into(),
                durable: false,
                auto_delete: false,
                internal: false,
                arguments: FieldTable::new(),
            })
            .unwrap_err();

        assert!(matches!(err, VHostError::ExchangeError(_)));
        assert!(!vhost.exchange_exists("bad-ex"));
    }

    #[test]
    fn test_delete_queue_removes_all_bindings() {
        let dir = TempDir::new().unwrap();
        let vhost = VHost::new("/".into(), dir.path()).unwrap();

        vhost
            .declare_queue(QueueConfig {
                name: "multi-bind-q".into(),
                durable: false,
                exclusive: false,
                auto_delete: false,
                arguments: FieldTable::new(),
            })
            .unwrap();

        // Bind to multiple exchanges with different routing keys
        vhost
            .bind_queue("multi-bind-q", "amq.direct", "key1", &FieldTable::new())
            .unwrap();
        vhost
            .bind_queue("multi-bind-q", "amq.direct", "key2", &FieldTable::new())
            .unwrap();
        vhost
            .bind_queue("multi-bind-q", "amq.fanout", "", &FieldTable::new())
            .unwrap();

        // Verify bindings exist
        let ex = vhost.get_exchange("amq.direct").unwrap();
        assert!(ex.read().binding_count() >= 2);

        // Delete the queue — should remove ALL bindings
        vhost.delete_queue("multi-bind-q").unwrap();

        // Verify all bindings are gone
        let ex = vhost.get_exchange("amq.direct").unwrap();
        let routes = ex.read().route("key1", None);
        assert!(routes.is_empty());
        let routes = ex.read().route("key2", None);
        assert!(routes.is_empty());

        let fan = vhost.get_exchange("amq.fanout").unwrap();
        assert_eq!(fan.read().binding_count(), 0);
    }
}
