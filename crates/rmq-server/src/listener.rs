use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;

use crate::connection::Connection;

/// Default maximum number of concurrent connections.
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Configuration for the AMQP listener.
pub struct ListenerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5672".parse().unwrap(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
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
    let conn_semaphore = Arc::new(Semaphore::new(config.max_connections));
    info!(
        "AMQP listening on {} (max_connections={})",
        config.bind_addr, config.max_connections
    );

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let permit = match conn_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!("connection limit reached, rejecting new connection");
                        drop(stream);
                        continue;
                    }
                };
                let vhost = vhost.clone();
                let user_store = user_store.clone();
                tokio::spawn(async move {
                    Connection::handle(stream, vhost, user_store).await;
                    drop(permit);
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
    max_connections: usize,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    let conn_semaphore = Arc::new(Semaphore::new(max_connections));
    info!(
        "AMQPS (TLS) listening on {} (max_connections={})",
        bind_addr, max_connections
    );

    loop {
        match listener.accept().await {
            Ok((tcp_stream, _addr)) => {
                let permit = match conn_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!("TLS connection limit reached, rejecting new connection");
                        drop(tcp_stream);
                        continue;
                    }
                };
                let vhost = vhost.clone();
                let user_store = user_store.clone();
                let acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    match acceptor.accept(tcp_stream).await {
                        Ok(tls_stream) => {
                            Connection::handle_tls(tls_stream, vhost, user_store).await;
                        }
                        Err(e) => {
                            error!("TLS handshake failed: {e}");
                        }
                    }
                    drop(permit);
                });
            }
            Err(e) => {
                error!("TLS accept error: {e}");
            }
        }
    }
}
