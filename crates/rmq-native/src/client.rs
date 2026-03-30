use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

use crate::codec::{MAGIC, NativeFrame};

/// A native protocol client for publishing and consuming batches.
pub struct NativeClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    write_buf: BytesMut,
    read_buf: BytesMut,
}

impl NativeClient {
    /// Connect and authenticate.
    pub async fn connect(
        addr: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        let (r, w) = stream.into_split();
        let mut client = Self {
            reader: BufReader::with_capacity(256 * 1024, r),
            writer: BufWriter::with_capacity(256 * 1024, w),
            write_buf: BytesMut::with_capacity(256 * 1024),
            read_buf: BytesMut::with_capacity(256 * 1024),
        };

        // Send magic
        client.writer.write_all(&MAGIC).await?;

        // Send auth
        client.write_buf.clear();
        NativeFrame::Auth {
            username: username.into(),
            password: password.into(),
        }
        .encode(&mut client.write_buf);
        client.writer.write_all(&client.write_buf).await?;
        client.writer.flush().await?;

        // Read AuthOk
        let frame = client.read_frame().await?;
        match frame {
            NativeFrame::AuthOk => Ok(client),
            _ => Err("auth failed".into()),
        }
    }

    /// Publish a batch of messages to a queue. Returns the count confirmed.
    pub async fn publish_batch(
        &mut self,
        queue: &str,
        messages: Vec<Bytes>,
    ) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        self.write_buf.clear();
        NativeFrame::Publish {
            queue: queue.into(),
            messages,
        }
        .encode(&mut self.write_buf);
        self.writer.write_all(&self.write_buf).await?;
        self.writer.flush().await?;

        let frame = self.read_frame().await?;
        match frame {
            NativeFrame::PublishOk { count } => Ok(count),
            _ => Err("unexpected response".into()),
        }
    }

    /// Start consuming and receive the first batch.
    pub async fn consume(
        &mut self,
        queue: &str,
        batch_size: u32,
        prefetch: u32,
    ) -> Result<(u64, Vec<Bytes>), Box<dyn std::error::Error + Send + Sync>> {
        self.write_buf.clear();
        NativeFrame::Consume {
            queue: queue.into(),
            batch_size,
            prefetch,
        }
        .encode(&mut self.write_buf);
        self.writer.write_all(&self.write_buf).await?;
        self.writer.flush().await?;

        self.recv_deliver().await
    }

    /// Ack a batch and receive the next delivery.
    pub async fn ack_and_recv(
        &mut self,
        batch_id: u64,
    ) -> Result<Option<(u64, Vec<Bytes>)>, Box<dyn std::error::Error + Send + Sync>> {
        self.write_buf.clear();
        NativeFrame::Ack { batch_id }.encode(&mut self.write_buf);
        self.writer.write_all(&self.write_buf).await?;
        self.writer.flush().await?;

        // Try to read next deliver (may get nothing if queue is empty)
        let mut tmp = [0u8; 65536];
        match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            self.reader.read(&mut tmp),
        )
        .await
        {
            Ok(Ok(0)) | Err(_) => Ok(None),
            Ok(Ok(n)) => {
                self.read_buf.extend_from_slice(&tmp[..n]);
                match NativeFrame::decode(&mut self.read_buf) {
                    Ok(Some(NativeFrame::Deliver { batch_id, messages })) => {
                        Ok(Some((batch_id, messages)))
                    }
                    Err(e) => Err(e.into()),
                    _ => Ok(None),
                }
            }
            Ok(Err(e)) => Err(e.into()),
        }
    }

    /// Disconnect cleanly.
    pub async fn disconnect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_buf.clear();
        NativeFrame::Disconnect.encode(&mut self.write_buf);
        self.writer.write_all(&self.write_buf).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn recv_deliver(
        &mut self,
    ) -> Result<(u64, Vec<Bytes>), Box<dyn std::error::Error + Send + Sync>> {
        let frame = self.read_frame().await?;
        match frame {
            NativeFrame::Deliver { batch_id, messages } => Ok((batch_id, messages)),
            _ => Err("expected Deliver".into()),
        }
    }

    async fn read_frame(
        &mut self,
    ) -> Result<NativeFrame, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match NativeFrame::decode(&mut self.read_buf) {
                Ok(Some(frame)) => return Ok(frame),
                Err(e) => return Err(e.into()),
                Ok(None) => {}
            }
            let mut tmp = [0u8; 65536];
            let n = self.reader.read(&mut tmp).await?;
            if n == 0 {
                return Err("connection closed".into());
            }
            self.read_buf.extend_from_slice(&tmp[..n]);
        }
    }
}
