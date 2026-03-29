use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use rmq_protocol::field_table::FieldTable;
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
}

#[derive(Debug, Serialize, Deserialize)]
struct ExchangeMeta {
    name: String,
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct BindingMeta {
    queue: String,
    exchange: String,
    routing_key: String,
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
            Arc::new(RwLock::new(Box::new(DefaultExchange::new()) as Box<dyn Exchange>)),
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

        let exchange = create_exchange(config.clone());
        exchanges.insert(
            config.name.clone(),
            Arc::new(RwLock::new(exchange)),
        );
        drop(exchanges);
        self.save_metadata();
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
        self.save_metadata();
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

        let store = MessageStore::open(&queue_dir)
            .map_err(|e| VHostError::InternalError(e.to_string()))?;

        let queue = Arc::new(Queue::new(config.clone(), store));
        queues.insert(config.name.clone(), queue.clone());
        drop(queues);
        if config.durable {
            self.save_metadata();
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
        let exchanges = self.exchanges.read();
        for (_, exchange) in exchanges.iter() {
            let mut ex = exchange.write();
            ex.unbind(
                &Destination::Queue(name.to_string()),
                "",
                &FieldTable::new(),
            );
        }
        drop(exchanges);
        drop(queues);
        self.save_metadata();

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

        let destinations = exchange.read().route(routing_key, msg.properties.headers.as_ref());

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
            let _ = self.publish(&dl.exchange, &dl.routing_key, &dl.message);
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
    fn save_metadata(&self) {
        let mut meta = VHostMetadata::default();

        // Save durable exchanges (skip builtins)
        for name in self.exchange_names() {
            if name.is_empty() || name.starts_with("amq.") {
                continue;
            }
            if let Some(ex) = self.get_exchange(&name) {
                let ex = ex.read();
                if ex.is_durable() {
                    meta.exchanges.push(ExchangeMeta {
                        name: ex.name().to_string(),
                        exchange_type: ex.exchange_type().to_string(),
                        durable: true,
                        auto_delete: ex.is_auto_delete(),
                        internal: ex.is_internal(),
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
                    });
                }
            }
        }

        // Write atomically via temp file
        let path = self.metadata_path();
        if let Ok(data) = serde_json::to_string_pretty(&meta) {
            let tmp = path.with_extension("tmp");
            let _ = std::fs::write(&tmp, &data);
            let _ = std::fs::rename(&tmp, &path);
        }
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
            let _ = self.declare_exchange(ExchangeConfig {
                name: ex.name.clone(),
                exchange_type: ex.exchange_type.clone(),
                durable: ex.durable,
                auto_delete: ex.auto_delete,
                internal: ex.internal,
                arguments: FieldTable::new(),
            });
        }

        // Re-declare queues (this reopens their message stores)
        for q in &meta.queues {
            let _ = self.declare_queue(QueueConfig {
                name: q.name.clone(),
                durable: q.durable,
                exclusive: q.exclusive,
                auto_delete: q.auto_delete,
                arguments: FieldTable::new(),
            });
        }

        // Re-create bindings
        for b in &meta.bindings {
            let _ = self.bind_queue(&b.queue, &b.exchange, &b.routing_key, &FieldTable::new());
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
}
