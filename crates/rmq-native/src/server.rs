use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use rmq_auth::user_store::UserStore;
use rmq_broker::queue::QueueConfig;
use rmq_broker::vhost::VHost;
use rmq_protocol::field_table::FieldTable;
use rmq_protocol::properties::BasicProperties;
use rmq_storage::message::StoredMessage;
use rmq_storage::segment_position::SegmentPosition;

use crate::codec::{MAGIC, NativeFrame};

/// Start the native protocol listener.
pub async fn run(
    bind_addr: SocketAddr,
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("RMQ native protocol listening on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                let vhost = vhost.clone();
                let user_store = user_store.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, vhost, user_store).await {
                        debug!("native client error: {e}");
                    }
                });
            }
            Err(e) => error!("native accept error: {e}"),
        }
    }
}

async fn handle_client(
    stream: tokio::net::TcpStream,
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(256 * 1024, reader);
    let mut writer = BufWriter::with_capacity(256 * 1024, writer);

    // Read magic
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;
    if magic != MAGIC {
        return Err("invalid magic".into());
    }

    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let mut write_buf = BytesMut::with_capacity(256 * 1024);
    let mut authenticated = false;
    let batch_counter = AtomicU64::new(0);

    // Per-consumer state
    let mut consuming: Option<ConsumerState> = None;

    loop {
        // Read more data
        let mut tmp = [0u8; 65536];
        let n = match reader.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        read_buf.extend_from_slice(&tmp[..n]);

        // Process all complete frames
        while let Some(frame) = NativeFrame::decode(&mut read_buf) {
            match frame {
                NativeFrame::Auth { username, password } => {
                    if user_store.authenticate(&username, &password).is_ok() {
                        authenticated = true;
                        write_buf.clear();
                        NativeFrame::AuthOk.encode(&mut write_buf);
                        writer.write_all(&write_buf).await?;
                        writer.flush().await?;
                    } else {
                        return Err("auth failed".into());
                    }
                }

                NativeFrame::Publish { queue, messages } if authenticated => {
                    // Ensure queue exists (auto-declare)
                    let _ = vhost.declare_queue(QueueConfig {
                        name: queue.clone(),
                        durable: false,
                        exclusive: false,
                        auto_delete: false,
                        arguments: FieldTable::new(),
                    });

                    let q = vhost.get_queue(&queue).ok_or("queue not found")?;
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;

                    let count = messages.len() as u32;
                    for body in messages {
                        let msg = StoredMessage {
                            timestamp,
                            exchange: String::new(),
                            routing_key: queue.clone(),
                            properties: BasicProperties::default(),
                            body,
                        };
                        let _ = q.publish(&msg);
                    }

                    // Single ack for the entire batch
                    write_buf.clear();
                    NativeFrame::PublishOk { count }.encode(&mut write_buf);
                    writer.write_all(&write_buf).await?;
                    writer.flush().await?;
                }

                NativeFrame::Consume {
                    queue,
                    batch_size,
                    prefetch,
                } if authenticated => {
                    // Ensure queue exists
                    let _ = vhost.declare_queue(QueueConfig {
                        name: queue.clone(),
                        durable: false,
                        exclusive: false,
                        auto_delete: false,
                        arguments: FieldTable::new(),
                    });

                    consuming = Some(ConsumerState {
                        queue_name: queue,
                        batch_size,
                        prefetch,
                        unacked_batches: Vec::new(),
                    });

                    // Start delivering immediately
                    if let Some(ref mut state) = consuming {
                        deliver_batch(&vhost, state, &batch_counter, &mut write_buf, &mut writer)
                            .await?;
                    }
                }

                NativeFrame::Ack { batch_id } if authenticated => {
                    if let Some(ref mut state) = consuming {
                        // Ack the batch — remove from unacked and ack in store
                        if let Some(pos) = state
                            .unacked_batches
                            .iter()
                            .position(|b| b.batch_id == batch_id)
                        {
                            let batch = state.unacked_batches.remove(pos);
                            if let Some(q) = vhost.get_queue(&state.queue_name) {
                                let _ = q.ack_batch(&batch.positions);
                            }
                        }

                        // Deliver next batch if we have capacity
                        let total_unacked: usize = state
                            .unacked_batches
                            .iter()
                            .map(|b| b.positions.len())
                            .sum();
                        if total_unacked < state.prefetch as usize {
                            deliver_batch(
                                &vhost,
                                state,
                                &batch_counter,
                                &mut write_buf,
                                &mut writer,
                            )
                            .await?;
                        }
                    }
                }

                NativeFrame::Disconnect => break,
                _ => {}
            }
        }
    }

    Ok(())
}

struct ConsumerState {
    queue_name: String,
    batch_size: u32,
    prefetch: u32,
    unacked_batches: Vec<UnackedBatch>,
}

struct UnackedBatch {
    batch_id: u64,
    positions: Vec<SegmentPosition>,
}

async fn deliver_batch(
    vhost: &Arc<VHost>,
    state: &mut ConsumerState,
    counter: &AtomicU64,
    write_buf: &mut BytesMut,
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let queue = match vhost.get_queue(&state.queue_name) {
        Some(q) => q,
        None => return Ok(()),
    };

    let (envelopes, _dead_letters) = queue.shift_batch(state.batch_size as usize)?;
    if envelopes.is_empty() {
        return Ok(());
    }

    let batch_id = counter.fetch_add(1, Ordering::Relaxed);
    let mut positions = Vec::with_capacity(envelopes.len());
    let mut bodies: Vec<Bytes> = Vec::with_capacity(envelopes.len());

    for env in envelopes {
        positions.push(env.segment_position);
        bodies.push(env.message.body.clone());
    }

    state.unacked_batches.push(UnackedBatch {
        batch_id,
        positions,
    });

    write_buf.clear();
    NativeFrame::Deliver {
        batch_id,
        messages: bodies,
    }
    .encode(write_buf);
    writer.write_all(write_buf).await?;
    writer.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use std::time::Duration;

    async fn start_native_server() -> (SocketAddr, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let vhost_dir = dir.path().join("vhosts").join("default");
        let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());
        let users_path = dir.path().join("users.json");
        let user_store =
            Arc::new(UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    let _ = stream.set_nodelay(true);
                    let v = vhost.clone();
                    let u = user_store.clone();
                    tokio::spawn(async move {
                        let _ = handle_client(stream, v, u).await;
                    });
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr, dir)
    }

    #[tokio::test]
    async fn test_native_publish_and_consume() {
        let (addr, _dir) = start_native_server().await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = stream.set_nodelay(true);

        // Send magic
        stream.write_all(&MAGIC).await.unwrap();

        let mut buf = BytesMut::new();

        // Auth
        NativeFrame::Auth {
            username: "guest".into(),
            password: "guest".into(),
        }
        .encode(&mut buf);
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();

        // Read AuthOk
        let mut resp = [0u8; 5];
        stream.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp[0], crate::codec::FRAME_AUTH_OK);

        // Publish batch of 100 messages
        buf.clear();
        let messages: Vec<Bytes> = (0..100).map(|i| Bytes::from(format!("msg-{i}"))).collect();
        NativeFrame::Publish {
            queue: "native-q".into(),
            messages,
        }
        .encode(&mut buf);
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();

        // Read PublishOk
        let mut resp = [0u8; 9];
        stream.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp[0], crate::codec::FRAME_PUBLISH_OK);

        // Start consuming
        buf.clear();
        NativeFrame::Consume {
            queue: "native-q".into(),
            batch_size: 50,
            prefetch: 100,
        }
        .encode(&mut buf);
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();

        // Read Deliver
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], crate::codec::FRAME_DELIVER);
        let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

        let mut payload = vec![0u8; length];
        stream.read_exact(&mut payload).await.unwrap();

        // Parse delivery
        let mut frame_buf = BytesMut::new();
        frame_buf.put_u8(header[0]);
        frame_buf.extend_from_slice(&header[1..]);
        frame_buf.extend_from_slice(&payload);
        let frame = NativeFrame::decode(&mut frame_buf).unwrap();
        match frame {
            NativeFrame::Deliver { batch_id, messages } => {
                assert_eq!(messages.len(), 50); // batch_size=50
                assert_eq!(&messages[0][..], b"msg-0");

                // Ack the batch
                buf.clear();
                NativeFrame::Ack { batch_id }.encode(&mut buf);
                stream.write_all(&buf).await.unwrap();
                stream.flush().await.unwrap();
            }
            _ => panic!("expected Deliver"),
        }

        // Small delay for second batch
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Disconnect
        buf.clear();
        NativeFrame::Disconnect.encode(&mut buf);
        stream.write_all(&buf).await.unwrap();
    }
}
