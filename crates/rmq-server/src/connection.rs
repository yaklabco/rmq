use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;
use rmq_protocol::field_table::{FieldTable, FieldValue};
use rmq_protocol::frame::*;
use rmq_protocol::types::*;

use crate::channel::{ChannelError, ServerChannel};

/// Frames to be sent back to the client.
pub type FrameSender = mpsc::Sender<AMQPFrame>;
pub type FrameReceiver = mpsc::Receiver<AMQPFrame>;

/// Bounded channel capacity for outbound frames.
const FRAME_CHANNEL_CAPACITY: usize = 8192;

/// Typed error for connection-level failures.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid channel number")]
    InvalidChannel,
    #[error("channel {0} already open")]
    ChannelAlreadyOpen(u16),
    #[error("channel {0} not open")]
    ChannelNotOpen(u16),
    #[error("frame send failed")]
    SendFailed,
    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),
}

/// An AMQP connection from a client.
#[allow(dead_code)]
pub struct Connection {
    vhost: Arc<VHost>,
    channels: HashMap<u16, ServerChannel>,
    frame_max: u32,
    channel_max: u16,
    heartbeat: u16,
    tx: FrameSender,
}

impl Connection {
    /// Handle a new plaintext TCP connection.
    pub async fn handle(stream: TcpStream, vhost: Arc<VHost>, user_store: Arc<UserStore>) {
        let peer_addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".into());
        // TCP_NODELAY for low-latency writes; BufWriter handles batching.
        let _ = stream.set_nodelay(true);
        Self::handle_stream(stream, vhost, user_store, peer_addr).await;
    }

    /// Handle a TLS-wrapped connection.
    pub async fn handle_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        vhost: Arc<VHost>,
        user_store: Arc<UserStore>,
    ) {
        let peer_addr = stream
            .get_ref()
            .0
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".into());
        Self::handle_stream(stream, vhost, user_store, peer_addr).await;
    }

    /// Core connection handler — works with any async read/write stream.
    async fn handle_stream<S>(
        stream: S,
        vhost: Arc<VHost>,
        user_store: Arc<UserStore>,
        peer_addr: String,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        info!("new connection from {peer_addr}");

        let (reader, writer) = tokio::io::split(stream);
        let mut reader = BufReader::with_capacity(128 * 1024, reader);
        let mut writer = BufWriter::with_capacity(128 * 1024, writer);

        // Read protocol header (8 bytes)
        let mut header = [0u8; 8];
        if let Err(e) = reader.read_exact(&mut header).await {
            error!("failed to read protocol header: {e}");
            return;
        }

        if header != PROTOCOL_HEADER {
            // Send our protocol header and close
            let _ = writer.write_all(&PROTOCOL_HEADER).await;
            let _ = writer.flush().await;
            warn!("invalid protocol header from {peer_addr}");
            return;
        }

        // Reusable frame read buffer
        let mut frame_buf = BytesMut::with_capacity(65536);

        // Create frame sender/receiver for write task
        let (tx, mut rx) = mpsc::channel::<AMQPFrame>(FRAME_CHANNEL_CAPACITY);

        // Spawn coalescing write task — drains all pending frames into one write
        let write_handle = tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(65536);
            loop {
                // Wait for at least one frame
                let frame = match rx.recv().await {
                    Some(f) => f,
                    None => break,
                };
                buf.clear();
                if let Err(e) = frame.encode(&mut buf) {
                    error!("encode error: {e}");
                    continue;
                }

                // Drain all immediately available frames into the same buffer
                while let Ok(frame) = rx.try_recv() {
                    if let Err(e) = frame.encode(&mut buf) {
                        error!("encode error: {e}");
                    }
                }

                // Single write + flush for the entire batch
                if let Err(e) = writer.write_all(&buf).await {
                    error!("write error: {e}");
                    break;
                }
                if let Err(e) = writer.flush().await {
                    error!("flush error: {e}");
                    break;
                }
            }
        });

        // Send Connection.Start
        let mut server_properties = FieldTable::new();
        server_properties.insert(
            "product",
            FieldValue::LongString(Bytes::from_static(b"RMQ")),
        );
        server_properties.insert(
            "version",
            FieldValue::LongString(Bytes::from_static(b"0.1.0")),
        );
        server_properties.insert(
            "platform",
            FieldValue::LongString(Bytes::from_static(b"Rust/Tokio")),
        );

        let mut capabilities = FieldTable::new();
        capabilities.insert("publisher_confirms", FieldValue::Bool(true));
        capabilities.insert("exchange_exchange_bindings", FieldValue::Bool(true));
        capabilities.insert("basic.nack", FieldValue::Bool(true));
        capabilities.insert("consumer_cancel_notify", FieldValue::Bool(true));
        capabilities.insert("connection.blocked", FieldValue::Bool(true));
        capabilities.insert("per_consumer_qos", FieldValue::Bool(true));
        capabilities.insert("direct_reply_to", FieldValue::Bool(true));
        capabilities.insert("authentication_failure_close", FieldValue::Bool(true));
        server_properties.insert("capabilities", FieldValue::FieldTable(capabilities));

        let start = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionStart(ConnectionStart {
                version_major: 0,
                version_minor: 9,
                server_properties,
                mechanisms: Bytes::from_static(b"PLAIN"),
                locales: Bytes::from_static(b"en_US"),
            })),
        };

        if tx.try_send(start).is_err() {
            return;
        }

        // Read Connection.StartOk
        let frame = match read_frame(&mut reader, &mut frame_buf, 0).await {
            Ok(Some(f)) => f,
            _ => {
                error!("failed to read StartOk");
                return;
            }
        };

        let _start_ok = match frame.payload {
            FramePayload::Method(MethodFrame::ConnectionStartOk(ok)) => {
                // Validate credentials
                if !authenticate(&ok.mechanism, &ok.response, &user_store) {
                    let close = AMQPFrame {
                        channel: 0,
                        payload: FramePayload::Method(MethodFrame::ConnectionClose(
                            ConnectionClose {
                                reply_code: ReplyCode::AccessRefused as u16,
                                reply_text: "ACCESS_REFUSED - Login was refused".into(),
                                class_id: CLASS_CONNECTION,
                                method_id: METHOD_CONNECTION_START_OK,
                            },
                        )),
                    };
                    let _ = tx.try_send(close);
                    return;
                }
                ok
            }
            _ => {
                error!("expected ConnectionStartOk, got {:?}", frame.payload);
                return;
            }
        };

        // Send Connection.Tune
        let tune = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionTune(ConnectionTune {
                channel_max: DEFAULT_CHANNEL_MAX,
                frame_max: DEFAULT_FRAME_MAX,
                heartbeat: DEFAULT_HEARTBEAT,
            })),
        };
        if tx.try_send(tune).is_err() {
            return;
        }

        // Read Connection.TuneOk
        let frame = match read_frame(&mut reader, &mut frame_buf, 0).await {
            Ok(Some(f)) => f,
            _ => {
                error!("failed to read TuneOk");
                return;
            }
        };

        let tune_ok = match frame.payload {
            FramePayload::Method(MethodFrame::ConnectionTuneOk(ok)) => ok,
            _ => {
                error!("expected ConnectionTuneOk");
                return;
            }
        };

        let frame_max = if tune_ok.frame_max == 0 {
            DEFAULT_FRAME_MAX
        } else {
            tune_ok.frame_max.max(FRAME_MIN_SIZE)
        };
        let channel_max = if tune_ok.channel_max == 0 {
            DEFAULT_CHANNEL_MAX
        } else {
            tune_ok.channel_max.min(DEFAULT_CHANNEL_MAX)
        };
        let heartbeat = tune_ok.heartbeat;

        // Read Connection.Open
        let frame = match read_frame(&mut reader, &mut frame_buf, 0).await {
            Ok(Some(f)) => f,
            _ => {
                error!("failed to read ConnectionOpen");
                return;
            }
        };

        match frame.payload {
            FramePayload::Method(MethodFrame::ConnectionOpen(open)) => {
                let vhost_name = if open.virtual_host.is_empty() {
                    "/"
                } else {
                    &open.virtual_host
                };
                debug!("client opening vhost: {vhost_name}");
            }
            _ => {
                error!("expected ConnectionOpen");
                return;
            }
        }

        // Send Connection.OpenOk
        let open_ok = AMQPFrame {
            channel: 0,
            payload: FramePayload::Method(MethodFrame::ConnectionOpenOk),
        };
        if tx.try_send(open_ok).is_err() {
            return;
        }

        info!("connection established from {peer_addr}");

        // Create connection state
        let mut conn = Connection {
            vhost,
            channels: HashMap::new(),
            frame_max,
            channel_max,
            heartbeat,
            tx: tx.clone(),
        };

        // Main read loop with heartbeat
        let heartbeat_interval = if heartbeat > 0 {
            Some(tokio::time::Duration::from_secs(heartbeat as u64))
        } else {
            None
        };

        // Spawn heartbeat sender
        let heartbeat_tx = tx.clone();
        let heartbeat_handle = if let Some(interval) = heartbeat_interval {
            let half_interval = interval / 2;
            Some(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(half_interval);
                loop {
                    ticker.tick().await;
                    let hb = AMQPFrame {
                        channel: 0,
                        payload: FramePayload::Heartbeat,
                    };
                    match heartbeat_tx.try_send(hb) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("heartbeat dropped: slow client");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            }))
        } else {
            None
        };

        // Read loop
        let read_timeout = heartbeat_interval.map(|i| i * 2);

        loop {
            let frame_result = if let Some(timeout) = read_timeout {
                match tokio::time::timeout(
                    timeout,
                    read_frame(&mut reader, &mut frame_buf, frame_max),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        info!("heartbeat timeout from {peer_addr}");
                        break;
                    }
                }
            } else {
                read_frame(&mut reader, &mut frame_buf, frame_max).await
            };

            let frame = match frame_result {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    info!("connection closed by {peer_addr}");
                    break;
                }
                Err(e) => {
                    error!("frame read error: {e}");
                    break;
                }
            };

            match &frame.payload {
                FramePayload::Heartbeat => {
                    debug!("received heartbeat from {peer_addr}");
                    continue;
                }
                FramePayload::Method(MethodFrame::ConnectionClose(close)) => {
                    info!(
                        "client closing connection: {} {}",
                        close.reply_code, close.reply_text
                    );
                    let close_ok = AMQPFrame {
                        channel: 0,
                        payload: FramePayload::Method(MethodFrame::ConnectionCloseOk),
                    };
                    let _ = tx.try_send(close_ok);
                    break;
                }
                _ => {}
            }

            if let Err(e) = conn.process_frame(frame) {
                error!("error processing frame: {e}");
                break;
            }
        }

        // Cleanup
        if let Some(h) = heartbeat_handle {
            h.abort();
        }
        drop(tx);
        let _ = write_handle.await;
        info!("connection from {peer_addr} closed");
    }

    fn process_frame(&mut self, frame: AMQPFrame) -> Result<(), ServerError> {
        let channel_id = frame.channel;

        match frame.payload {
            FramePayload::Method(MethodFrame::ChannelOpen) => {
                if channel_id == 0 || channel_id > self.channel_max {
                    return Err(ServerError::InvalidChannel);
                }
                if self.channels.contains_key(&channel_id) {
                    return Err(ServerError::ChannelAlreadyOpen(channel_id));
                }
                let channel = ServerChannel::new(channel_id, self.vhost.clone(), self.tx.clone());
                self.channels.insert(channel_id, channel);

                let open_ok = AMQPFrame {
                    channel: channel_id,
                    payload: FramePayload::Method(MethodFrame::ChannelOpenOk),
                };
                self.tx
                    .try_send(open_ok)
                    .map_err(|_| ServerError::SendFailed)?;
                Ok(())
            }
            FramePayload::Method(MethodFrame::ChannelClose(close)) => {
                debug!(
                    "channel {} close: {} {}",
                    channel_id, close.reply_code, close.reply_text
                );
                self.channels.remove(&channel_id);
                let close_ok = AMQPFrame {
                    channel: channel_id,
                    payload: FramePayload::Method(MethodFrame::ChannelCloseOk),
                };
                self.tx
                    .try_send(close_ok)
                    .map_err(|_| ServerError::SendFailed)?;
                Ok(())
            }
            FramePayload::Method(MethodFrame::ChannelCloseOk) => {
                self.channels.remove(&channel_id);
                Ok(())
            }
            _ => {
                if let Some(channel) = self.channels.get_mut(&channel_id) {
                    channel.process_frame(frame)?;
                    Ok(())
                } else {
                    warn!("frame for unknown channel {channel_id}");
                    Err(ServerError::ChannelNotOpen(channel_id))
                }
            }
        }
    }
}

/// Extract credentials from SASL response and authenticate against user store.
fn authenticate(mechanism: &str, response: &[u8], user_store: &UserStore) -> bool {
    let (username, password) = match extract_credentials(mechanism, response) {
        Some(creds) => creds,
        None => return false,
    };
    user_store.authenticate(&username, &password).is_ok()
}

/// Extract username and password from SASL mechanism response.
fn extract_credentials(mechanism: &str, response: &[u8]) -> Option<(String, String)> {
    match mechanism {
        "PLAIN" => {
            let parts: Vec<&[u8]> = response.splitn(3, |&b| b == 0).collect();
            if parts.len() != 3 {
                return None;
            }
            let username = std::str::from_utf8(parts[1]).ok()?.to_string();
            let password = std::str::from_utf8(parts[2]).ok()?.to_string();
            Some((username, password))
        }
        "AMQPLAIN" => {
            let mut buf = Bytes::copy_from_slice(response);
            let table = FieldTable::decode(&mut buf).ok()?;
            let login = match table.get("LOGIN") {
                Some(FieldValue::ShortString(s)) => s.clone(),
                Some(FieldValue::LongString(b)) => std::str::from_utf8(b).ok()?.to_string(),
                _ => return None,
            };
            let password = match table.get("PASSWORD") {
                Some(FieldValue::ShortString(s)) => s.clone(),
                Some(FieldValue::LongString(b)) => std::str::from_utf8(b).ok()?.to_string(),
                _ => return None,
            };
            Some((login, password))
        }
        _ => None,
    }
}

/// Read a single AMQP frame from the reader using a reusable buffer.
///
/// If `frame_max` is non-zero, frames larger than this limit are rejected
/// with a protocol error to prevent memory exhaustion from oversized frames.
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    frame_buf: &mut BytesMut,
    frame_max: u32,
) -> Result<Option<AMQPFrame>, Box<dyn std::error::Error + Send + Sync>> {
    // Read frame header: type(1) + channel(2) + size(4) = 7 bytes
    let mut header = [0u8; 7];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let size = u32::from_be_bytes([header[3], header[4], header[5], header[6]]);

    // Validate frame size against negotiated maximum to prevent OOM
    if frame_max > 0 && size > frame_max {
        return Err(format!("frame size {size} exceeds negotiated frame_max {frame_max}").into());
    }

    // Reuse the provided buffer to avoid per-frame allocation
    let total = 7 + size as usize + 1;
    frame_buf.clear();
    frame_buf.reserve(total);
    frame_buf.extend_from_slice(&header);

    // Read remaining bytes directly into the BytesMut
    frame_buf.resize(total, 0);
    reader.read_exact(&mut frame_buf[7..]).await?;

    let mut bytes = frame_buf.split().freeze();
    let frame = AMQPFrame::decode(&mut bytes)?;
    Ok(Some(frame))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    /// Build a raw frame byte sequence: type(1) + channel(2) + size(4) + payload + end(1).
    fn build_raw_frame(frame_type: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
        let size = payload.len() as u32;
        let mut buf = Vec::with_capacity(7 + payload.len() + 1);
        buf.push(frame_type);
        buf.extend_from_slice(&channel.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(payload);
        buf.push(0xCE); // frame end
        buf
    }

    #[tokio::test]
    async fn test_frame_size_validation_rejects_oversized_frame() {
        // Build a frame header that claims a huge payload size (1 MiB)
        let huge_size: u32 = 1_048_576;
        let frame_max: u32 = 131_072; // 128 KiB

        // We only need the 7-byte header since validation happens before reading the body
        let mut header_bytes = Vec::new();
        header_bytes.push(1u8); // METHOD frame type
        header_bytes.extend_from_slice(&0u16.to_be_bytes()); // channel 0
        header_bytes.extend_from_slice(&huge_size.to_be_bytes());
        // Add enough dummy bytes so read_exact for the body won't block
        header_bytes.resize(7 + huge_size as usize + 1, 0);

        let cursor = std::io::Cursor::new(header_bytes);
        let mut reader = BufReader::new(cursor);
        let mut frame_buf = BytesMut::with_capacity(1024);

        let result = read_frame(&mut reader, &mut frame_buf, frame_max).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("exceeds negotiated frame_max"),
            "unexpected error: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_frame_size_validation_allows_valid_frame() {
        // Build a valid heartbeat frame (type=8, channel=0, size=0, end=0xCE)
        let raw = build_raw_frame(8, 0, &[]);

        let cursor = std::io::Cursor::new(raw);
        let mut reader = BufReader::new(cursor);
        let mut frame_buf = BytesMut::with_capacity(1024);

        let result = read_frame(&mut reader, &mut frame_buf, 131_072).await;
        assert!(result.is_ok());
        let frame = result.unwrap();
        assert!(frame.is_some());
        assert!(matches!(frame.unwrap().payload, FramePayload::Heartbeat));
    }

    #[tokio::test]
    async fn test_frame_size_validation_disabled_with_zero_frame_max() {
        // frame_max=0 means unlimited — should not reject any frame
        let raw = build_raw_frame(8, 0, &[]);

        let cursor = std::io::Cursor::new(raw);
        let mut reader = BufReader::new(cursor);
        let mut frame_buf = BytesMut::with_capacity(1024);

        let result = read_frame(&mut reader, &mut frame_buf, 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_bounded_channel_capacity() {
        let (tx, _rx) = mpsc::channel::<AMQPFrame>(FRAME_CHANNEL_CAPACITY);
        assert_eq!(tx.capacity(), FRAME_CHANNEL_CAPACITY);
    }
}
