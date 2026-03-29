use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info};

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;

use crate::connection::Connection;

/// Configuration for the AMQP listener.
pub struct ListenerConfig {
    pub bind_addr: SocketAddr,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5672".parse().unwrap(),
        }
    }
}

/// Start the AMQP TCP listener (plaintext).
pub async fn run(
    config: ListenerConfig,
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    info!("AMQP listening on {}", config.bind_addr);

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let vhost = vhost.clone();
                let user_store = user_store.clone();
                tokio::spawn(async move {
                    Connection::handle(stream, vhost, user_store).await;
                });
            }
            Err(e) => {
                error!("accept error: {e}");
            }
        }
    }
}

/// Start the AMQPS (TLS) TCP listener.
pub async fn run_tls(
    bind_addr: SocketAddr,
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
    tls_acceptor: TlsAcceptor,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("AMQPS (TLS) listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((tcp_stream, _addr)) => {
                let vhost = vhost.clone();
                let user_store = user_store.clone();
                let acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    match acceptor.accept(tcp_stream).await {
                        Ok(tls_stream) => {
                            // Convert TLS stream to a type compatible with Connection::handle
                            // We need to adapt — use tokio::io split
                            Connection::handle_tls(tls_stream, vhost, user_store).await;
                        }
                        Err(e) => {
                            error!("TLS handshake failed: {e}");
                        }
                    }
                });
            }
            Err(e) => {
                error!("TLS accept error: {e}");
            }
        }
    }
}
