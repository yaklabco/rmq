use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

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

    // Start flush coordinator — batches fsync every 2ms for all queues
    let flush_vhost = vhost.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(2));
        loop {
            interval.tick().await;
            for queue_name in flush_vhost.queue_names() {
                if let Some(queue) = flush_vhost.get_queue(&queue_name) {
                    let _ = queue.flush_if_needed();
                }
            }
        }
    });

    // Start Management API in background
    let mgmt_vhost = vhost.clone();
    let mgmt_users = user_store.clone();
    tokio::spawn(async move {
        let config = MgmtConfig {
            bind_addr: cli.mgmt_bind,
        };
        if let Err(e) = mgmt_server::run(config, mgmt_vhost, mgmt_users).await {
            tracing::error!("management API error: {e}");
        }
    });

    // Start TLS listener if cert+key provided
    if let (Some(cert_path), Some(key_path)) = (&cli.tls_cert, &cli.tls_key) {
        let tls_acceptor = tls::load_tls_config(cert_path, key_path)?;
        let tls_vhost = vhost.clone();
        let tls_users = user_store.clone();
        let tls_bind = cli.tls_bind;
        info!("AMQPS (TLS) bind: {}", tls_bind);
        tokio::spawn(async move {
            if let Err(e) = listener::run_tls(
                tls_bind,
                tls_vhost,
                tls_users,
                tls_acceptor,
                listener::DEFAULT_MAX_CONNECTIONS,
            )
            .await
            {
                tracing::error!("AMQPS listener error: {e}");
            }
        });
    }

    // Start MQTT listener in background
    let mqtt_broker = MqttBroker::new(vhost.clone(), user_store.clone());
    let mqtt_bind = cli.mqtt_bind;
    tokio::spawn(async move {
        if let Err(e) = rmq_mqtt::listener::run(mqtt_bind, mqtt_broker).await {
            tracing::error!("MQTT listener error: {e}");
        }
    });

    // Start native protocol listener
    let native_vhost = vhost.clone();
    let native_users = user_store.clone();
    let native_bind = cli.native_bind;
    tokio::spawn(async move {
        if let Err(e) = rmq_native::server::run(native_bind, native_vhost, native_users).await {
            tracing::error!("native protocol error: {e}");
        }
    });

    // Start AMQP listener (blocks)
    let config = ListenerConfig {
        bind_addr: cli.bind,
        ..Default::default()
    };
    listener::run(config, vhost, user_store).await?;

    Ok(())
}
