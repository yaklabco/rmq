use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

use crate::broker::MqttBroker;

/// Start the MQTT TCP listener.
pub async fn run(
    bind_addr: SocketAddr,
    broker: Arc<MqttBroker>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("MQTT listening on {}", bind_addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let broker = broker.clone();
                        tokio::spawn(async move {
                            broker.handle_client(stream).await;
                        });
                    }
                    Err(e) => {
                        error!("MQTT accept error: {e}");
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("MQTT listener shutting down");
                    break;
                }
            }
        }
    }

    Ok(())
}
