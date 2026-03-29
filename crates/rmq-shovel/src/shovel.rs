use std::sync::Arc;
use std::time::Duration;

use rmq_broker::queue::PublishResult;
use rmq_broker::vhost::VHost;
use rmq_protocol::properties::BasicProperties;
use rmq_storage::message::StoredMessage;
use tracing::{debug, error, info, warn};

/// A shovel source — reads messages from a queue.
pub struct ShovelSource {
    pub vhost: Arc<VHost>,
    pub queue_name: String,
}

/// A shovel destination — publishes messages to an exchange.
pub struct ShovelDestination {
    pub vhost: Arc<VHost>,
    pub exchange: String,
    pub routing_key: String,
}

/// Shovel configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShovelConfig {
    pub name: String,
    pub src_queue: String,
    pub dest_exchange: String,
    pub dest_routing_key: String,
    /// Prefetch count for the source consumer.
    pub prefetch: u16,
    /// Whether to ack messages on the source after publishing.
    pub ack_mode: AckMode,
    /// Maximum reconnect delay in seconds.
    pub reconnect_delay_max: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AckMode {
    /// Ack after publishing to destination.
    OnConfirm,
    /// Ack immediately on consume (at-most-once).
    OnPublish,
    /// No ack — let the source requeue on disconnect.
    NoAck,
}

impl Default for AckMode {
    fn default() -> Self {
        Self::OnConfirm
    }
}

/// Transfer messages from source queue to destination exchange within the same vhost.
/// Returns the number of messages transferred before stopping.
pub async fn run_shovel(
    vhost: Arc<VHost>,
    config: &ShovelConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> u64 {
    let mut transferred = 0u64;
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(config.reconnect_delay_max.max(1));

    info!("shovel '{}' starting: {} -> {}/{}", config.name, config.src_queue, config.dest_exchange, config.dest_routing_key);

    loop {
        if *cancel.borrow() {
            info!("shovel '{}' cancelled", config.name);
            break;
        }

        let queue = match vhost.get_queue(&config.src_queue) {
            Some(q) => q,
            None => {
                warn!("shovel '{}': source queue '{}' not found, retrying...", config.name, config.src_queue);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        match queue.shift() {
            Ok((Some(env), _dead_letters)) => {
                backoff = Duration::from_millis(100); // reset on success

                // Re-publish to destination
                let msg = StoredMessage {
                    timestamp: env.message.timestamp,
                    exchange: config.dest_exchange.clone(),
                    routing_key: config.dest_routing_key.clone(),
                    properties: env.message.properties.clone(),
                    body: env.message.body.clone(),
                };

                match vhost.publish(&config.dest_exchange, &config.dest_routing_key, &msg) {
                    Ok(_) => {
                        // Ack source
                        if config.ack_mode != AckMode::NoAck {
                            let _ = queue.ack(&env.segment_position);
                        }
                        transferred += 1;
                        debug!("shovel '{}': transferred message #{}", config.name, transferred);
                    }
                    Err(e) => {
                        warn!("shovel '{}': publish failed: {e}, requeueing", config.name);
                        queue.requeue(env.segment_position);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
            Ok((None, _)) => {
                // Wait for a message or cancellation
                tokio::select! {
                    _ = queue.wait_for_message() => {}
                    _ = cancel.changed() => { break; }
                }
            }
            Err(e) => {
                error!("shovel '{}': shift error: {e}", config.name);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }

    transferred
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rmq_broker::queue::QueueConfig;
    use rmq_protocol::field_table::FieldTable;
    use tempfile::TempDir;

    fn setup_vhost(dir: &std::path::Path) -> Arc<VHost> {
        let vhost_dir = dir.join("vhosts").join("default");
        Arc::new(VHost::new("/".into(), &vhost_dir).unwrap())
    }

    fn make_msg(body: &str) -> StoredMessage {
        StoredMessage {
            timestamp: 0,
            exchange: "".into(),
            routing_key: "src-key".into(),
            properties: BasicProperties::default(),
            body: Bytes::from(body.as_bytes().to_vec()),
        }
    }

    #[tokio::test]
    async fn test_shovel_transfers_messages() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        // Create source queue and destination queue
        vhost.declare_queue(QueueConfig {
            name: "src-queue".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();

        vhost.declare_queue(QueueConfig {
            name: "dest-queue".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();

        // Bind dest queue to amq.direct
        vhost.bind_queue("dest-queue", "amq.direct", "shoveled", &FieldTable::new()).unwrap();

        // Publish messages to source
        let src = vhost.get_queue("src-queue").unwrap();
        for i in 0..5 {
            src.publish(&make_msg(&format!("msg-{i}"))).unwrap();
        }

        let config = ShovelConfig {
            name: "test-shovel".into(),
            src_queue: "src-queue".into(),
            dest_exchange: "amq.direct".into(),
            dest_routing_key: "shoveled".into(),
            prefetch: 10,
            ack_mode: AckMode::OnConfirm,
            reconnect_delay_max: 1,
        };

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        // Run shovel briefly then cancel
        let vhost_clone = vhost.clone();
        let config_clone = config.clone();
        let handle = tokio::spawn(async move {
            run_shovel(vhost_clone, &config_clone, cancel_rx).await
        });

        // Wait for messages to transfer
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_tx.send(true).unwrap();
        let transferred = handle.await.unwrap();

        assert_eq!(transferred, 5);

        // Verify dest queue has messages
        let dest = vhost.get_queue("dest-queue").unwrap();
        let (env, _) = dest.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"msg-0");
    }

    #[tokio::test]
    async fn test_shovel_no_ack_mode() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        vhost.declare_queue(QueueConfig {
            name: "na-src".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.declare_queue(QueueConfig {
            name: "na-dest".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.bind_queue("na-dest", "amq.direct", "na-key", &FieldTable::new()).unwrap();

        let src = vhost.get_queue("na-src").unwrap();
        src.publish(&make_msg("noack")).unwrap();

        let config = ShovelConfig {
            name: "noack-shovel".into(),
            src_queue: "na-src".into(),
            dest_exchange: "amq.direct".into(),
            dest_routing_key: "na-key".into(),
            prefetch: 10,
            ack_mode: AckMode::NoAck,
            reconnect_delay_max: 1,
        };

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let vhost_clone = vhost.clone();
        let handle = tokio::spawn(async move {
            run_shovel(vhost_clone, &config, cancel_rx).await
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel_tx.send(true).unwrap();
        let transferred = handle.await.unwrap();
        assert_eq!(transferred, 1);
    }

    #[tokio::test]
    async fn test_shovel_missing_source() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        let config = ShovelConfig {
            name: "broken-shovel".into(),
            src_queue: "nonexistent".into(),
            dest_exchange: "amq.direct".into(),
            dest_routing_key: "key".into(),
            prefetch: 10,
            ack_mode: AckMode::OnConfirm,
            reconnect_delay_max: 1,
        };

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            run_shovel(vhost, &config, cancel_rx).await
        });

        // Cancel quickly — should not panic
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel_tx.send(true).unwrap();
        let transferred = handle.await.unwrap();
        assert_eq!(transferred, 0);
    }
}
