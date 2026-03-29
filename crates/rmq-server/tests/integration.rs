use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use lapin::options::*;
use lapin::types::FieldTable as LapinFieldTable;
use lapin::{BasicProperties as LapinProperties, Connection, ConnectionProperties};
use tokio::net::TcpListener;

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;

/// Start the RMQ server on a random port and return the address.
async fn start_server() -> (SocketAddr, Arc<VHost>, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let vhost_dir = dir.path().join("vhosts").join("default");
    let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());

    let users_path = dir.path().join("users.json");
    let user_store = Arc::new(
        UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap(),
    );

    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let vhost_clone = vhost.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let vhost = vhost_clone.clone();
                    let user_store = user_store.clone();
                    tokio::spawn(async move {
                        rmq_server::connection::Connection::handle(stream, vhost, user_store).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    // Give the server a moment to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, vhost, dir)
}

async fn connect(addr: SocketAddr) -> lapin::Connection {
    let uri = format!("amqp://guest:guest@{}", addr);
    Connection::connect(&uri, ConnectionProperties::default())
        .await
        .expect("failed to connect to RMQ")
}

#[tokio::test]
async fn test_connect_and_open_channel() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();
    assert_eq!(channel.id(), 1);

    // Open a second channel
    let channel2 = conn.create_channel().await.unwrap();
    assert_eq!(channel2.id(), 2);

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_declare_exchange() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .exchange_declare(
            "test-exchange",
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: false,
                ..Default::default()
            },
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_declare_queue() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    let queue = channel
        .queue_declare(
            "test-queue",
            QueueDeclareOptions {
                durable: false,
                ..Default::default()
            },
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    assert_eq!(queue.name().as_str(), "test-queue");
    assert_eq!(queue.message_count(), 0);
    assert_eq!(queue.consumer_count(), 0);

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_bind_and_publish_consume() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    // Declare queue
    channel
        .queue_declare(
            "consume-queue",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Bind to amq.direct
    channel
        .queue_bind(
            "consume-queue",
            "amq.direct",
            "test-key",
            QueueBindOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Publish a message
    channel
        .basic_publish(
            "amq.direct",
            "test-key",
            BasicPublishOptions::default(),
            b"hello from lapin",
            LapinProperties::default(),
        )
        .await
        .unwrap();

    // Small delay for message to be routed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get the message
    let get_result = channel
        .basic_get("consume-queue", BasicGetOptions { no_ack: true })
        .await
        .unwrap();

    let delivery = get_result.expect("expected a message");
    assert_eq!(delivery.data, b"hello from lapin");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_publish_via_default_exchange() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    // Declare queue
    channel
        .queue_declare(
            "default-ex-queue",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Publish via default exchange (routing key = queue name)
    channel
        .basic_publish(
            "",
            "default-ex-queue",
            BasicPublishOptions::default(),
            b"default exchange msg",
            LapinProperties::default(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let get_result = channel
        .basic_get("default-ex-queue", BasicGetOptions { no_ack: true })
        .await
        .unwrap();

    let delivery = get_result.expect("expected a message");
    assert_eq!(delivery.data, b"default exchange msg");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_fanout_exchange() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    // Declare 3 queues and bind all to amq.fanout
    for name in &["fan-q1", "fan-q2", "fan-q3"] {
        channel
            .queue_declare(name, QueueDeclareOptions::default(), LapinFieldTable::default())
            .await
            .unwrap();

        channel
            .queue_bind(
                name,
                "amq.fanout",
                "",
                QueueBindOptions::default(),
                LapinFieldTable::default(),
            )
            .await
            .unwrap();
    }

    // Publish one message
    channel
        .basic_publish(
            "amq.fanout",
            "",
            BasicPublishOptions::default(),
            b"broadcast",
            LapinProperties::default(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // All 3 queues should have the message
    for name in &["fan-q1", "fan-q2", "fan-q3"] {
        let get_result = channel
            .basic_get(name, BasicGetOptions { no_ack: true })
            .await
            .unwrap();
        let delivery = get_result.unwrap_or_else(|| panic!("expected message in {name}"));
        assert_eq!(delivery.data, b"broadcast");
    }

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_basic_ack() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .queue_declare(
            "ack-queue",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Publish
    channel
        .basic_publish(
            "",
            "ack-queue",
            BasicPublishOptions::default(),
            b"ack me",
            LapinProperties::default(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get without auto-ack
    let get_result = channel
        .basic_get("ack-queue", BasicGetOptions { no_ack: false })
        .await
        .unwrap();

    let delivery = get_result.expect("expected a message");
    assert_eq!(delivery.data, b"ack me");

    // Ack the message
    channel
        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
        .await
        .unwrap();

    // Queue should now be empty
    tokio::time::sleep(Duration::from_millis(50)).await;
    let get_result = channel
        .basic_get("ack-queue", BasicGetOptions { no_ack: true })
        .await
        .unwrap();
    assert!(get_result.is_none(), "queue should be empty after ack");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_publisher_confirms() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    // Enable publisher confirms
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    channel
        .queue_declare(
            "confirm-queue",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Publish and wait for confirm
    let confirm = channel
        .basic_publish(
            "",
            "confirm-queue",
            BasicPublishOptions::default(),
            b"confirmed msg",
            LapinProperties::default(),
        )
        .await
        .unwrap();

    // Wait for the confirmation
    let confirmation = confirm.await.unwrap();
    assert!(
        confirmation.is_ack(),
        "expected publisher confirm ack"
    );

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_queue_delete() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .queue_declare(
            "delete-me",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Publish some messages
    for i in 0..5 {
        channel
            .basic_publish(
                "",
                "delete-me",
                BasicPublishOptions::default(),
                format!("msg {i}").as_bytes(),
                LapinProperties::default(),
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = channel
        .queue_delete("delete-me", QueueDeleteOptions::default())
        .await
        .unwrap();

    assert_eq!(result, 5, "should report 5 messages purged on delete");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_queue_purge() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .queue_declare(
            "purge-queue",
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    for i in 0..3 {
        channel
            .basic_publish(
                "",
                "purge-queue",
                BasicPublishOptions::default(),
                format!("msg {i}").as_bytes(),
                LapinProperties::default(),
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = channel
        .queue_purge("purge-queue", QueuePurgeOptions::default())
        .await
        .unwrap();

    assert_eq!(count, 3);

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_multiple_channels() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;

    // Open 5 channels, each with its own queue
    let mut channels = Vec::new();
    for i in 0..5 {
        let ch = conn.create_channel().await.unwrap();
        let queue_name = format!("multi-ch-q{i}");
        ch.queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

        ch.basic_publish(
            "",
            &queue_name,
            BasicPublishOptions::default(),
            format!("channel {i}").as_bytes(),
            LapinProperties::default(),
        )
        .await
        .unwrap();

        channels.push((ch, queue_name));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Each channel should be able to get its own message
    for (ch, queue_name) in &channels {
        let get = ch
            .basic_get(queue_name, BasicGetOptions { no_ack: true })
            .await
            .unwrap();
        assert!(get.is_some(), "expected message in {queue_name}");
    }

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_topic_exchange() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .exchange_declare(
            "test-topic",
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    for name in &["topic-all", "topic-usd", "topic-nyse"] {
        channel
            .queue_declare(name, QueueDeclareOptions::default(), LapinFieldTable::default())
            .await
            .unwrap();
    }

    channel.queue_bind("topic-all", "test-topic", "stock.#", QueueBindOptions::default(), LapinFieldTable::default()).await.unwrap();
    channel.queue_bind("topic-usd", "test-topic", "stock.usd.*", QueueBindOptions::default(), LapinFieldTable::default()).await.unwrap();
    channel.queue_bind("topic-nyse", "test-topic", "*.*.nyse", QueueBindOptions::default(), LapinFieldTable::default()).await.unwrap();

    channel.basic_publish("test-topic", "stock.usd.nyse", BasicPublishOptions::default(), b"usd-nyse", LapinProperties::default()).await.unwrap();
    channel.basic_publish("test-topic", "stock.eur.lse", BasicPublishOptions::default(), b"eur-lse", LapinProperties::default()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    // topic-all: both messages (stock.#)
    let m1 = channel.basic_get("topic-all", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(m1.is_some(), "topic-all should have first message");
    let m2 = channel.basic_get("topic-all", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(m2.is_some(), "topic-all should have second message");

    // topic-usd: only stock.usd.nyse (stock.usd.*)
    let m = channel.basic_get("topic-usd", BasicGetOptions { no_ack: true }).await.unwrap();
    assert_eq!(m.unwrap().data, b"usd-nyse");
    let empty = channel.basic_get("topic-usd", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(empty.is_none());

    // topic-nyse: only stock.usd.nyse (*.*.nyse)
    let m = channel.basic_get("topic-nyse", BasicGetOptions { no_ack: true }).await.unwrap();
    assert_eq!(m.unwrap().data, b"usd-nyse");
    let empty = channel.basic_get("topic-nyse", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(empty.is_none());

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_prefetch_limits_delivery() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    // Set prefetch to 2
    channel
        .basic_qos(2, BasicQosOptions { global: false })
        .await
        .unwrap();

    channel
        .queue_declare("prefetch-q", QueueDeclareOptions::default(), LapinFieldTable::default())
        .await
        .unwrap();

    // Publish 5 messages
    for i in 0..5 {
        channel
            .basic_publish("", "prefetch-q", BasicPublishOptions::default(), format!("msg{i}").as_bytes(), LapinProperties::default())
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start consuming (no auto-ack)
    let mut consumer = channel
        .basic_consume(
            "prefetch-q",
            "prefetch-ctag",
            BasicConsumeOptions { no_ack: false, ..Default::default() },
            LapinFieldTable::default(),
        )
        .await
        .unwrap();

    // Should receive 2 messages (prefetch limit)
    use futures_lite::StreamExt;

    let d1 = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(d1.data, b"msg0");
    let d2 = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(d2.data, b"msg1");

    // Third message should NOT arrive yet (prefetch=2, 2 unacked)
    let timeout_result = tokio::time::timeout(Duration::from_millis(300), consumer.next()).await;
    assert!(timeout_result.is_err(), "should not receive 3rd message while 2 are unacked");

    // Ack first message — should unblock delivery of 3rd
    channel.basic_ack(d1.delivery_tag, BasicAckOptions::default()).await.unwrap();

    let d3 = tokio::time::timeout(Duration::from_secs(2), consumer.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(d3.data, b"msg2");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_transaction_commit() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .queue_declare("tx-queue", QueueDeclareOptions::default(), LapinFieldTable::default())
        .await
        .unwrap();

    // Enable transaction mode
    channel.tx_select().await.unwrap();

    // Publish within transaction
    channel
        .basic_publish("", "tx-queue", BasicPublishOptions::default(), b"tx-msg-1", LapinProperties::default())
        .await
        .unwrap();
    channel
        .basic_publish("", "tx-queue", BasicPublishOptions::default(), b"tx-msg-2", LapinProperties::default())
        .await
        .unwrap();

    // Messages should NOT be visible yet
    tokio::time::sleep(Duration::from_millis(100)).await;
    let get = channel.basic_get("tx-queue", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(get.is_none(), "messages should not be visible before commit");

    // Commit
    channel.tx_commit().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now messages should be visible
    let get1 = channel.basic_get("tx-queue", BasicGetOptions { no_ack: true }).await.unwrap();
    assert_eq!(get1.unwrap().data, b"tx-msg-1");
    let get2 = channel.basic_get("tx-queue", BasicGetOptions { no_ack: true }).await.unwrap();
    assert_eq!(get2.unwrap().data, b"tx-msg-2");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_transaction_rollback() {
    let (addr, _vhost, _dir) = start_server().await;
    let conn = connect(addr).await;
    let channel = conn.create_channel().await.unwrap();

    channel
        .queue_declare("tx-rb-queue", QueueDeclareOptions::default(), LapinFieldTable::default())
        .await
        .unwrap();

    channel.tx_select().await.unwrap();

    channel
        .basic_publish("", "tx-rb-queue", BasicPublishOptions::default(), b"rolled-back", LapinProperties::default())
        .await
        .unwrap();

    // Rollback
    channel.tx_rollback().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Message should NOT exist
    let get = channel.basic_get("tx-rb-queue", BasicGetOptions { no_ack: true }).await.unwrap();
    assert!(get.is_none(), "rolled back messages should not be visible");

    conn.close(200, "OK").await.unwrap();
}

#[tokio::test]
async fn test_tls_connection() {
    use rcgen::generate_simple_self_signed;
    use std::sync::Arc as StdArc;

    let dir = tempfile::TempDir::new().unwrap();
    let vhost_dir = dir.path().join("vhosts").join("default");
    let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());
    let users_path = dir.path().join("users.json");
    let user_store = Arc::new(
        UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap(),
    );

    // Generate self-signed cert
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = generate_simple_self_signed(subject_alt_names).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();

    let tls_acceptor =
        rmq_server::tls::tls_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap();

    // Start TLS listener
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_addr = listener.local_addr().unwrap();

    let vhost_clone = vhost.clone();
    let user_store_clone = user_store.clone();
    let acceptor = tls_acceptor.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let vhost = vhost_clone.clone();
                    let users = user_store_clone.clone();
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                rmq_server::connection::Connection::handle_tls(
                                    tls_stream, vhost, users,
                                )
                                .await;
                            }
                            Err(e) => eprintln!("TLS handshake failed: {e}"),
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect with lapin over TLS
    // lapin supports amqps:// URIs with custom TLS config
    // We need to configure the client to trust our self-signed cert
    // Build a rustls client config that trusts our self-signed cert
    let mut root_store = rustls::RootCertStore::empty();
    let cert_der = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for cert in &cert_der {
        root_store.add(cert.clone()).unwrap();
    }

    // For lapin 2.x, TLS is handled differently — it uses native-tls or rustls-connector
    // The simplest test: just verify the TLS handshake works by connecting raw
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let tcp = tokio::net::TcpStream::connect(tls_addr).await.unwrap();

    rmq_server::tls::ensure_crypto_provider();
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = tokio_rustls::TlsConnector::from(StdArc::new(client_config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let mut tls_stream = connector.connect(server_name, tcp).await.unwrap();

    // Send AMQP protocol header
    tls_stream
        .write_all(&[b'A', b'M', b'Q', b'P', 0, 0, 9, 1])
        .await
        .unwrap();

    // Read Connection.Start frame (type=1, channel=0, then payload)
    let mut header = [0u8; 7];
    tls_stream.read_exact(&mut header).await.unwrap();

    // Verify frame type is METHOD (1) and channel is 0
    assert_eq!(header[0], 1, "expected METHOD frame type");
    assert_eq!(header[1], 0, "expected channel 0 high byte");
    assert_eq!(header[2], 0, "expected channel 0 low byte");

    // The server sent Connection.Start — TLS works!
}
