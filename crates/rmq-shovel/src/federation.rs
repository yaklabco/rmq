use std::sync::Arc;
use std::time::Duration;

use rmq_broker::vhost::VHost;
use rmq_protocol::properties::BasicProperties;
use rmq_storage::message::StoredMessage;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Federation upstream configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationUpstream {
    pub name: String,
    /// URI of the upstream broker (for future remote support).
    pub uri: String,
    /// Exchange to federate.
    pub exchange: String,
    /// Max hops to prevent loops.
    pub max_hops: u32,
    /// Prefetch count.
    pub prefetch: u16,
    /// Reconnect delay in seconds.
    pub reconnect_delay: u64,
}

/// Federation link state.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkState {
    Starting,
    Running,
    Stopped,
    Error,
}

/// A federation exchange link.
/// In full implementation, this consumes from a remote broker.
/// For now, it supports local federation (same vhost, different exchanges).
pub struct FederationLink {
    pub config: FederationUpstream,
    pub state: LinkState,
}

impl FederationLink {
    pub fn new(config: FederationUpstream) -> Self {
        Self {
            config,
            state: LinkState::Stopped,
        }
    }
}

/// Run a local federation link: consume from one exchange's bound queue
/// and republish to another exchange, adding hop tracking.
pub async fn run_federation_link(
    vhost: Arc<VHost>,
    upstream: &FederationUpstream,
    src_queue: &str,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> u64 {
    let mut forwarded = 0u64;

    info!(
        "federation link '{}' starting: queue '{}' -> exchange '{}'",
        upstream.name, src_queue, upstream.exchange
    );

    loop {
        if *cancel.borrow() {
            info!("federation link '{}' cancelled", upstream.name);
            break;
        }

        let queue = match vhost.get_queue(src_queue) {
            Some(q) => q,
            None => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        match queue.shift() {
            Ok((Some(env), _)) => {
                // Check hop count to prevent loops
                let hop_count = get_hop_count(&env.message);
                if hop_count >= upstream.max_hops {
                    debug!(
                        "federation '{}': dropping message (hops={} >= max={})",
                        upstream.name, hop_count, upstream.max_hops
                    );
                    let _ = queue.ack(&env.segment_position);
                    continue;
                }

                // Republish with incremented hop count
                let mut msg = env.message.clone();
                increment_hop_count(&mut msg);
                msg.exchange = upstream.exchange.clone();

                match vhost.publish(&upstream.exchange, &msg.routing_key, &msg) {
                    Ok(_) => {
                        let _ = queue.ack(&env.segment_position);
                        forwarded += 1;
                    }
                    Err(e) => {
                        warn!("federation '{}': publish failed: {e}", upstream.name);
                        queue.requeue(env.segment_position);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            Ok((None, _)) => {
                tokio::select! {
                    _ = queue.wait_for_message() => {}
                    _ = cancel.changed() => { break; }
                }
            }
            Err(e) => {
                warn!("federation '{}': shift error: {e}", upstream.name);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    forwarded
}

fn get_hop_count(msg: &StoredMessage) -> u32 {
    msg.properties
        .headers
        .as_ref()
        .and_then(|h| h.get("x-federation-hops"))
        .and_then(|v| match v {
            rmq_protocol::field_table::FieldValue::I32(n) => Some(*n as u32),
            _ => None,
        })
        .unwrap_or(0)
}

fn increment_hop_count(msg: &mut StoredMessage) {
    let current = get_hop_count(msg);
    let headers = msg.properties.headers.get_or_insert_with(Default::default);
    headers.insert(
        "x-federation-hops",
        rmq_protocol::field_table::FieldValue::I32((current + 1) as i32),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmq_broker::queue::QueueConfig;
    use rmq_protocol::field_table::FieldTable;
    use tempfile::TempDir;

    fn setup_vhost(dir: &std::path::Path) -> Arc<VHost> {
        let vhost_dir = dir.join("vhosts").join("default");
        Arc::new(VHost::new("/".into(), &vhost_dir).unwrap())
    }

    #[tokio::test]
    async fn test_federation_link_forwards() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        // Create source and destination infrastructure
        vhost.declare_queue(QueueConfig {
            name: "fed-src".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.declare_queue(QueueConfig {
            name: "fed-dest".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.bind_queue("fed-dest", "amq.direct", "fed-key", &FieldTable::new()).unwrap();

        // Put messages in source
        let src = vhost.get_queue("fed-src").unwrap();
        for i in 0..3 {
            let msg = StoredMessage {
                timestamp: 0,
                exchange: "".into(),
                routing_key: "fed-key".into(),
                properties: BasicProperties::default(),
                body: format!("fed-{i}").into_bytes(),
            };
            src.publish(&msg).unwrap();
        }

        let upstream = FederationUpstream {
            name: "test-fed".into(),
            uri: "amqp://localhost".into(),
            exchange: "amq.direct".into(),
            max_hops: 5,
            prefetch: 10,
            reconnect_delay: 1,
        };

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let vhost_clone = vhost.clone();
        let upstream_clone = upstream.clone();
        let handle = tokio::spawn(async move {
            run_federation_link(vhost_clone, &upstream_clone, "fed-src", cancel_rx).await
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_tx.send(true).unwrap();
        let forwarded = handle.await.unwrap();
        assert_eq!(forwarded, 3);

        // Verify destination has messages with hop header
        let dest = vhost.get_queue("fed-dest").unwrap();
        let (env, _) = dest.shift().unwrap();
        let msg = env.unwrap().message;
        assert_eq!(msg.body, b"fed-0");
        let hops = get_hop_count(&msg);
        assert_eq!(hops, 1);
    }

    #[tokio::test]
    async fn test_federation_hop_limit() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        vhost.declare_queue(QueueConfig {
            name: "hop-src".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();

        // Message already at max hops
        let src = vhost.get_queue("hop-src").unwrap();
        let mut msg = StoredMessage {
            timestamp: 0,
            exchange: "".into(),
            routing_key: "key".into(),
            properties: BasicProperties::default(),
            body: b"looped".to_vec(),
        };
        increment_hop_count(&mut msg);
        increment_hop_count(&mut msg); // hops = 2
        src.publish(&msg).unwrap();

        let upstream = FederationUpstream {
            name: "hop-test".into(),
            uri: "amqp://localhost".into(),
            exchange: "amq.direct".into(),
            max_hops: 2, // limit = 2, message already at 2
            prefetch: 10,
            reconnect_delay: 1,
        };

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let vhost_clone = vhost.clone();
        let handle = tokio::spawn(async move {
            run_federation_link(vhost_clone, &upstream, "hop-src", cancel_rx).await
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel_tx.send(true).unwrap();
        let forwarded = handle.await.unwrap();
        assert_eq!(forwarded, 0, "message at hop limit should be dropped");
    }
}
