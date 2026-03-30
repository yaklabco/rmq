use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::watch;
use tracing::{error, info};

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;
use rmq_mgmt::server::{self as mgmt_server, MgmtConfig};
use rmq_mqtt::broker::MqttBroker;
use rmq_server::listener::{self, ListenerConfig};
use rmq_server::tls;

#[derive(Parser)]
#[command(name = "rmq", about = "RMQ - AMQP 0.9.1 Message Broker")]
struct Cli {
    /// Data directory for persistent storage.
    #[arg(short, long, default_value = "/var/lib/rmq")]
    data_dir: PathBuf,

    /// AMQP listen address.
    #[arg(short, long, default_value = "0.0.0.0:5672")]
    bind: SocketAddr,

    /// AMQPS (TLS) listen address.
    #[arg(long, default_value = "0.0.0.0:5671")]
    tls_bind: SocketAddr,

    /// TLS certificate PEM file.
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS private key PEM file.
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// MQTT listen address.
    #[arg(long, default_value = "0.0.0.0:1883")]
    mqtt_bind: SocketAddr,

    /// Native protocol listen address.
    #[arg(long, default_value = "0.0.0.0:5680")]
    native_bind: SocketAddr,

    /// HTTP management API listen address.
    #[arg(long, default_value = "0.0.0.0:15672")]
    mgmt_bind: SocketAddr,

    /// Flush coordinator interval in milliseconds (must be >= 1).
    #[arg(long, default_value = "2", value_parser = clap::value_parser!(u64).range(1..))]
    flush_interval_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    info!("RMQ v{}", env!("CARGO_PKG_VERSION"));
    info!("data directory: {}", cli.data_dir.display());
    info!("AMQP bind: {}", cli.bind);

    std::fs::create_dir_all(&cli.data_dir)?;

    let users_path = cli.data_dir.join("users.json");
    let user_store = Arc::new(UserStore::open_with_defaults(
        &users_path,
        "guest",
        "guest",
    )?);
    info!("user store initialized");

    let vhost_dir = cli.data_dir.join("vhosts").join("default");
    let vhost = Arc::new(VHost::new("/".into(), &vhost_dir)?);
    info!("default vhost '/' initialized");

    // Shutdown broadcast channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Start flush coordinator — batches fsync at configurable interval for all queues
    let flush_vhost = vhost.clone();
    let flush_interval = tokio::time::Duration::from_millis(cli.flush_interval_ms);
    let mut flush_shutdown = shutdown_rx.clone();
    let flush_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(flush_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for queue_name in flush_vhost.queue_names() {
                        if let Some(queue) = flush_vhost.get_queue(&queue_name) {
                            let _ = queue.flush_if_needed();
                        }
                    }
                }
                _ = flush_shutdown.changed() => {
                    if *flush_shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });

    // Start Management API in background
    let mgmt_vhost = vhost.clone();
    let mgmt_users = user_store.clone();
    let mgmt_bind = cli.mgmt_bind;
    let mgmt_shutdown = shutdown_rx.clone();
    let mgmt_handle = tokio::spawn(async move {
        let config = MgmtConfig {
            bind_addr: mgmt_bind,
        };
        if let Err(e) = mgmt_server::run(config, mgmt_vhost, mgmt_users, mgmt_shutdown).await {
            error!("management API failed on port {}: {e}", mgmt_bind.port());
        }
    });

    // Start TLS listener if cert+key provided
    let tls_handle = if let (Some(cert_path), Some(key_path)) = (&cli.tls_cert, &cli.tls_key) {
        let tls_acceptor = tls::load_tls_config(cert_path, key_path)?;
        let tls_vhost = vhost.clone();
        let tls_users = user_store.clone();
        let tls_bind = cli.tls_bind;
        let tls_shutdown = shutdown_rx.clone();
        info!("AMQPS (TLS) bind: {}", tls_bind);
        Some(tokio::spawn(async move {
            if let Err(e) =
                listener::run_tls(tls_bind, tls_vhost, tls_users, tls_acceptor, tls_shutdown).await
            {
                error!("AMQPS listener failed on port {}: {e}", tls_bind.port());
            }
        }))
    } else {
        None
    };

    // Start MQTT listener in background
    let mqtt_broker = MqttBroker::new(vhost.clone(), user_store.clone());
    let mqtt_bind = cli.mqtt_bind;
    let mqtt_shutdown = shutdown_rx.clone();
    let mqtt_handle = tokio::spawn(async move {
        if let Err(e) = rmq_mqtt::listener::run(mqtt_bind, mqtt_broker, mqtt_shutdown).await {
            error!("MQTT listener failed on port {}: {e}", mqtt_bind.port());
        }
    });

    // Start native protocol listener
    let native_vhost = vhost.clone();
    let native_users = user_store.clone();
    let native_bind = cli.native_bind;
    let native_shutdown = shutdown_rx.clone();
    let native_handle = tokio::spawn(async move {
        if let Err(e) =
            rmq_native::server::run(native_bind, native_vhost, native_users, native_shutdown).await
        {
            error!(
                "native protocol listener failed on port {}: {e}",
                native_bind.port()
            );
        }
    });

    // Start AMQP listener in background (was blocking before)
    let amqp_shutdown = shutdown_rx.clone();
    let amqp_vhost = vhost.clone();
    let amqp_users = user_store.clone();
    let config = ListenerConfig {
        bind_addr: cli.bind,
    };
    let amqp_handle = tokio::spawn(async move {
        if let Err(e) = listener::run(config, amqp_vhost, amqp_users, amqp_shutdown).await {
            error!("AMQP listener failed on port {}: {e}", cli.bind.port());
        }
    });

    // Wait for ctrl-c shutdown signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    info!("Shutting down...");

    // Broadcast shutdown to all listeners
    let _ = shutdown_tx.send(true);

    // Wait for all listeners to drain with a 30-second timeout
    let drain_timeout = tokio::time::Duration::from_secs(30);
    let drain = async {
        let _ = amqp_handle.await;
        let _ = mgmt_handle.await;
        let _ = mqtt_handle.await;
        let _ = native_handle.await;
        let _ = flush_handle.await;
        if let Some(h) = tls_handle {
            let _ = h.await;
        }
    };

    if tokio::time::timeout(drain_timeout, drain).await.is_err() {
        error!("timed out waiting for listeners to drain after 30s");
    }

    // Final flush of all queue stores
    for queue_name in vhost.queue_names() {
        if let Some(queue) = vhost.get_queue(&queue_name) {
            let _ = queue.flush_if_needed();
        }
    }
    info!("all queues flushed, exiting");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_shutdown_signal_stops_listener() {
        let dir = tempfile::TempDir::new().unwrap();
        let vhost_dir = dir.path().join("vhosts").join("default");
        let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());
        let users_path = dir.path().join("users.json");
        let user_store =
            Arc::new(UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Bind to port 0 so the OS picks a free port
        let config = ListenerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
        };

        let handle =
            tokio::spawn(
                async move { listener::run(config, vhost, user_store, shutdown_rx).await },
            );

        // Give the listener time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown signal
        shutdown_tx.send(true).unwrap();

        // The listener should exit cleanly within a reasonable time
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(
            result.is_ok(),
            "listener did not shut down within 5 seconds"
        );

        let inner = result.unwrap();
        assert!(inner.is_ok(), "listener task panicked");
        assert!(
            inner.unwrap().is_ok(),
            "listener returned an error on shutdown"
        );
    }
}
