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

use crate::channel::ServerChannel;

/// Frames to be sent back to the client.
pub type FrameSender = mpsc::UnboundedSender<AMQPFrame>;
pub type FrameReceiver = mpsc::UnboundedReceiver<AMQPFrame>;

/// An AMQP connection from a client.
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
        // On Linux: use TCP_CORK for batch-optimized writes (coalesce frames)
        // On macOS: use TCP_NODELAY for low-latency individual writes
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let fd = stream.as_raw_fd();
            unsafe {
                let cork: libc::c_int = 1;
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CORK,
                    &cork as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stream.set_nodelay(true);
        }
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
        let (tx, mut rx) = mpsc::unbounded_channel::<AMQPFrame>();

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
                frame.encode(&mut buf);

                // Drain all immediately available frames into the same buffer
                while let Ok(frame) = rx.try_recv() {
                    frame.encode(&mut buf);
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
        server_properties.insert("product", FieldValue::ShortString("RMQ".into()));
        server_properties.insert("version", FieldValue::ShortString("0.1.0".into()));
        server_properties.insert("platform", FieldValue::ShortString("Rust/Tokio".into()));

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

        if tx.send(start).is_err() {
            return;
        }

        // Read Connection.StartOk
        let frame = match read_frame(&mut reader, &mut frame_buf).await {
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
                    let _ = tx.send(close);
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
        if tx.send(tune).is_err() {
            return;
        }

        // Read Connection.TuneOk
        let frame = match read_frame(&mut reader, &mut frame_buf).await {
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
        let frame = match read_frame(&mut reader, &mut frame_buf).await {
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
        if tx.send(open_ok).is_err() {
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
                    if heartbeat_tx.send(hb).is_err() {
                        break;
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
                match tokio::time::timeout(timeout, read_frame(&mut reader, &mut frame_buf)).await {
                    Ok(result) => result,
                    Err(_) => {
                        info!("heartbeat timeout from {peer_addr}");
                        break;
                    }
                }
            } else {
                read_frame(&mut reader, &mut frame_buf).await
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
                    let _ = tx.send(close_ok);
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

    fn process_frame(&mut self, frame: AMQPFrame) -> Result<(), Box<dyn std::error::Error>> {
        let channel_id = frame.channel;

        match frame.payload {
            FramePayload::Method(MethodFrame::ChannelOpen) => {
                if channel_id == 0 || channel_id > self.channel_max {
                    return Err("invalid channel number".into());
                }
                if self.channels.contains_key(&channel_id) {
                    return Err("channel already open".into());
                }
                let channel = ServerChannel::new(channel_id, self.vhost.clone(), self.tx.clone());
                self.channels.insert(channel_id, channel);

                let open_ok = AMQPFrame {
                    channel: channel_id,
                    payload: FramePayload::Method(MethodFrame::ChannelOpenOk),
                };
                self.tx.send(open_ok)?;
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
                self.tx.send(close_ok)?;
                Ok(())
            }
            FramePayload::Method(MethodFrame::ChannelCloseOk) => {
                self.channels.remove(&channel_id);
                Ok(())
            }
            _ => {
                if let Some(channel) = self.channels.get_mut(&channel_id) {
                    channel.process_frame(frame)
                } else {
                    warn!("frame for unknown channel {channel_id}");
                    Err(format!("channel {channel_id} not open").into())
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
                _ => return None,
            };
            let password = match table.get("PASSWORD") {
                Some(FieldValue::ShortString(s)) => s.clone(),
                _ => return None,
            };
            Some((login, password))
        }
        _ => None,
    }
}

/// Read a single AMQP frame from the reader using a reusable buffer.
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    frame_buf: &mut BytesMut,
) -> Result<Option<AMQPFrame>, Box<dyn std::error::Error + Send + Sync>> {
    // Read frame header: type(1) + channel(2) + size(4) = 7 bytes
    let mut header = [0u8; 7];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let size = u32::from_be_bytes([header[3], header[4], header[5], header[6]]);

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
