use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::info;

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;

use crate::routes::api_router;
use crate::state::AppState;

/// Configuration for the management HTTP server.
pub struct MgmtConfig {
    pub bind_addr: SocketAddr,
}

impl Default for MgmtConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:15672".parse().unwrap(),
        }
    }
}

/// Start the management HTTP server.
pub async fn run(
    config: MgmtConfig,
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let state = AppState { vhost, user_store };
    let app = api_router().with_state(state);

    let listener = TcpListener::bind(config.bind_addr).await?;
    info!("Management HTTP API listening on {}", config.bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|v| *v).await;
            info!("Management HTTP API shutting down");
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}
