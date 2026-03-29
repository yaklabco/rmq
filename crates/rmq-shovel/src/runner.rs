use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::info;

use rmq_broker::vhost::VHost;

use crate::federation::{FederationUpstream, run_federation_link};
use crate::shovel::{ShovelConfig, run_shovel};

/// State of a running shovel or federation link.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RunnerState {
    Running,
    Stopped,
    Error,
}

struct RunningTask {
    cancel_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<u64>,
    state: RunnerState,
}

/// Manages multiple shovels and federation links.
pub struct ShovelRunner {
    vhost: Arc<VHost>,
    shovels: Mutex<HashMap<String, RunningTask>>,
}

impl ShovelRunner {
    pub fn new(vhost: Arc<VHost>) -> Arc<Self> {
        Arc::new(Self {
            vhost,
            shovels: Mutex::new(HashMap::new()),
        })
    }

    /// Start a shovel.
    pub fn start_shovel(self: &Arc<Self>, config: ShovelConfig) {
        let name = config.name.clone();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let vhost = self.vhost.clone();

        let handle = tokio::spawn(async move {
            run_shovel(vhost, &config, cancel_rx).await
        });

        self.shovels.lock().insert(name, RunningTask {
            cancel_tx,
            handle,
            state: RunnerState::Running,
        });
    }

    /// Start a federation link.
    pub fn start_federation(
        self: &Arc<Self>,
        upstream: FederationUpstream,
        src_queue: String,
    ) {
        let name = upstream.name.clone();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let vhost = self.vhost.clone();

        let handle = tokio::spawn(async move {
            run_federation_link(vhost, &upstream, &src_queue, cancel_rx).await
        });

        self.shovels.lock().insert(name, RunningTask {
            cancel_tx,
            handle,
            state: RunnerState::Running,
        });
    }

    /// Stop a shovel or federation link by name.
    pub fn stop(&self, name: &str) -> bool {
        if let Some(task) = self.shovels.lock().remove(name) {
            let _ = task.cancel_tx.send(true);
            task.handle.abort();
            info!("stopped shovel/federation '{name}'");
            true
        } else {
            false
        }
    }

    /// List running shovel/federation names.
    pub fn list(&self) -> Vec<String> {
        self.shovels.lock().keys().cloned().collect()
    }

    /// Check if a shovel/federation is running.
    pub fn is_running(&self, name: &str) -> bool {
        self.shovels.lock().contains_key(name)
    }

    /// Stop all running shovels/federation links.
    pub fn stop_all(&self) {
        let tasks: Vec<_> = self.shovels.lock().drain().collect();
        for (name, task) in tasks {
            let _ = task.cancel_tx.send(true);
            task.handle.abort();
            info!("stopped '{name}'");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rmq_broker::queue::QueueConfig;
    use rmq_protocol::field_table::FieldTable;
    use rmq_protocol::properties::BasicProperties;
    use rmq_storage::message::StoredMessage;
    use tempfile::TempDir;
    use std::time::Duration;

    fn setup_vhost(dir: &std::path::Path) -> Arc<VHost> {
        let vhost_dir = dir.join("vhosts").join("default");
        Arc::new(VHost::new("/".into(), &vhost_dir).unwrap())
    }

    #[tokio::test]
    async fn test_runner_start_stop_shovel() {
        let dir = TempDir::new().unwrap();
        let vhost = setup_vhost(dir.path());

        vhost.declare_queue(QueueConfig {
            name: "r-src".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.declare_queue(QueueConfig {
            name: "r-dest".into(),
            durable: false, exclusive: false, auto_delete: false,
            arguments: FieldTable::new(),
        }).unwrap();
        vhost.bind_queue("r-dest", "amq.direct", "r-key", &FieldTable::new()).unwrap();

        let runner = ShovelRunner::new(vhost.clone());

        runner.start_shovel(ShovelConfig {
            name: "runner-test".into(),
            src_queue: "r-src".into(),
            dest_exchange: "amq.direct".into(),
            dest_routing_key: "r-key".into(),
            prefetch: 10,
            ack_mode: crate::shovel::AckMode::OnConfirm,
            reconnect_delay_max: 1,
        });

        assert!(runner.is_running("runner-test"));
        assert_eq!(runner.list().len(), 1);

        // Publish a message
        let src = vhost.get_queue("r-src").unwrap();
        src.publish(&StoredMessage {
            timestamp: 0,
            exchange: "".into(),
            routing_key: "r-key".into(),
            properties: BasicProperties::default(),
            body: Bytes::from_static(b"runner-msg"),
        }).unwrap();

        tokio::time::sleep(Duration::from_millis(300)).await;

        // Stop
        assert!(runner.stop("runner-test"));
        assert!(!runner.is_running("runner-test"));

        // Verify message was forwarded
        let dest = vhost.get_queue("r-dest").unwrap();
        let (env, _) = dest.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"runner-msg");
    }
}
