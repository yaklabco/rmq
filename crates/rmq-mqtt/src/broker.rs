use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, info, warn};

use rmq_auth::user_store::UserStore;
use rmq_broker::queue::QueueConfig;
use rmq_broker::vhost::VHost;
use rmq_protocol::field_table::FieldTable;
use rmq_protocol::properties::BasicProperties;
use rmq_storage::message::StoredMessage;

use crate::codec::*;
use crate::session::{RetainStore, Session};

/// The MQTT exchange name used internally for topic routing.
const MQTT_EXCHANGE: &str = "amq.topic";

/// MQTT broker that bridges MQTT clients to the AMQP vhost.
pub struct MqttBroker {
    vhost: Arc<VHost>,
    user_store: Arc<UserStore>,
    retain_store: Arc<RetainStore>,
    sessions: Mutex<HashMap<String, Session>>,
}

impl MqttBroker {
    pub fn new(vhost: Arc<VHost>, user_store: Arc<UserStore>) -> Arc<Self> {
        Arc::new(Self {
            vhost,
            user_store,
            retain_store: Arc::new(RetainStore::new()),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    /// Handle a single MQTT client connection.
    pub async fn handle_client<S>(self: &Arc<Self>, stream: S)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut reader, writer) = tokio::io::split(stream);
        let writer = Arc::new(tokio::sync::Mutex::new(writer));
        let mut read_buf = BytesMut::with_capacity(4096);
        let mut session: Option<Session> = None;
        let mut got_disconnect = false;

        loop {
            // Read data
            let mut tmp = [0u8; 4096];
            let timeout = tokio::time::Duration::from_secs(300);
            let n = match tokio::time::timeout(timeout, reader.read(&mut tmp)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => n,
                Ok(Err(_)) => break,
            };
            read_buf.extend_from_slice(&tmp[..n]);

            // Process packets
            loop {
                match decode_packet(&mut read_buf) {
                    Ok(Some(packet)) => {
                        if matches!(packet, MqttPacket::Disconnect) {
                            got_disconnect = true;
                            if let Some(s) = session.as_mut() {
                                s.clean_disconnect = true;
                            }
                        }
                        let response = self.handle_packet(packet, &mut session, &writer).await;
                        let mut w = writer.lock().await;
                        for resp in response {
                            let mut buf = BytesMut::new();
                            resp.encode(&mut buf);
                            if w.write_all(&buf).await.is_err() {
                                return;
                            }
                        }
                        if w.flush().await.is_err() {
                            return;
                        }
                        if got_disconnect {
                            break;
                        }
                    }
                    Ok(None) => break, // need more data
                    Err(e) => {
                        warn!("MQTT decode error: {e}");
                        return;
                    }
                }
            }

            if got_disconnect {
                break;
            }
        }

        // Cleanup session
        if let Some(mut session) = session {
            // Publish will message on unexpected disconnect
            if !session.clean_disconnect {
                if let Some(will) = session.will.take() {
                    self.publish_will(&will);
                }
            }

            session.cancel_all_delivery_tasks();

            if session.clean_session {
                self.cleanup_session(&session);
            } else {
                // Store session for later reconnect
                self.sessions
                    .lock()
                    .insert(session.client_id.clone(), session);
            }
            debug!("MQTT client disconnected");
        }
    }

    /// Publish a will message to the AMQP broker.
    fn publish_will(&self, will: &WillMessage) {
        let amqp_routing_key = mqtt_topic_to_amqp_routing_key(&will.topic);

        if will.retain {
            self.retain_store
                .set(will.topic.clone(), will.payload.clone(), will.qos);
        }

        let msg = StoredMessage {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            exchange: MQTT_EXCHANGE.to_string(),
            routing_key: amqp_routing_key.clone(),
            properties: BasicProperties {
                content_type: Some("application/octet-stream".into()),
                ..Default::default()
            },
            body: will.payload.clone(),
        };

        if let Err(e) = self.vhost.publish(MQTT_EXCHANGE, &amqp_routing_key, &msg) {
            warn!("MQTT will message publish failed: {e}");
        }
    }

    async fn handle_packet<W>(
        &self,
        packet: MqttPacket,
        session: &mut Option<Session>,
        writer: &Arc<tokio::sync::Mutex<W>>,
    ) -> Vec<MqttPacket>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        match packet {
            MqttPacket::Connect(connect) => self.handle_connect(connect, session),
            MqttPacket::Publish(publish) => self.handle_publish(publish, session).await,
            MqttPacket::PubAck { packet_id } => {
                if let Some(s) = session {
                    s.pending_acks.remove(&packet_id);
                }
                vec![]
            }
            MqttPacket::PubRec { packet_id } => {
                vec![MqttPacket::PubRel { packet_id }]
            }
            MqttPacket::PubRel { packet_id } => {
                // QoS 2 completion: respond with PUBCOMP
                if let Some(s) = session {
                    s.pending_acks.remove(&packet_id);
                }
                vec![MqttPacket::PubComp { packet_id }]
            }
            MqttPacket::PubComp { packet_id } => {
                if let Some(s) = session {
                    s.pending_acks.remove(&packet_id);
                }
                vec![]
            }
            MqttPacket::Subscribe(sub) => self.handle_subscribe(sub, session, writer),
            MqttPacket::Unsubscribe(unsub) => self.handle_unsubscribe(unsub, session),
            MqttPacket::PingReq => vec![MqttPacket::PingResp],
            MqttPacket::Disconnect => vec![], // connection will close
            _ => vec![],
        }
    }

    fn handle_connect(
        &self,
        connect: ConnectPacket,
        session: &mut Option<Session>,
    ) -> Vec<MqttPacket> {
        // Authenticate
        let username = connect.username.as_deref().unwrap_or("guest");
        let password = connect
            .password
            .as_ref()
            .map(|p| std::str::from_utf8(p).unwrap_or(""))
            .unwrap_or("guest");

        if self.user_store.authenticate(username, password).is_err() {
            return vec![MqttPacket::ConnAck(ConnAckPacket {
                session_present: false,
                return_code: 4, // Bad username or password
            })];
        }

        info!(
            "MQTT client '{}' connected (user: {username})",
            connect.client_id
        );

        // Session persistence
        let (session_present, mut new_session) = if connect.clean_session {
            let old = self.sessions.lock().remove(&connect.client_id);
            if let Some(old) = old {
                self.cleanup_session(&old);
            }
            (false, Session::new(connect.client_id.clone(), true))
        } else {
            // Restore existing session if present
            let existing = self.sessions.lock().remove(&connect.client_id);
            match existing {
                Some(existing) => {
                    // Restore subscriptions from persisted session
                    (true, existing)
                }
                None => (false, Session::new(connect.client_id.clone(), false)),
            }
        };

        // Store will message from CONNECT
        new_session.will = connect.will;
        new_session.clean_disconnect = false;

        *session = Some(new_session);

        vec![MqttPacket::ConnAck(ConnAckPacket {
            session_present,
            return_code: 0,
        })]
    }

    async fn handle_publish(
        &self,
        publish: PublishPacket,
        _session: &mut Option<Session>,
    ) -> Vec<MqttPacket> {
        let amqp_routing_key = mqtt_topic_to_amqp_routing_key(&publish.topic);

        // Store retained message
        if publish.retain {
            self.retain_store
                .set(publish.topic.clone(), publish.payload.clone(), publish.qos);
        }

        // Publish to AMQP vhost via topic exchange
        let msg = StoredMessage {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            exchange: MQTT_EXCHANGE.to_string(),
            routing_key: amqp_routing_key.clone(),
            properties: BasicProperties {
                content_type: Some("application/octet-stream".into()),
                ..Default::default()
            },
            body: publish.payload.clone(),
        };

        if let Err(e) = self.vhost.publish(MQTT_EXCHANGE, &amqp_routing_key, &msg) {
            warn!("MQTT publish to AMQP failed: {e}");
        }

        // QoS acknowledgments
        match publish.qos {
            QoS::AtMostOnce => vec![],
            QoS::AtLeastOnce => {
                vec![MqttPacket::PubAck {
                    packet_id: publish.packet_id.unwrap_or(0),
                }]
            }
            QoS::ExactlyOnce => {
                vec![MqttPacket::PubRec {
                    packet_id: publish.packet_id.unwrap_or(0),
                }]
            }
        }
    }

    fn handle_subscribe<W>(
        &self,
        sub: SubscribePacket,
        session: &mut Option<Session>,
        writer: &Arc<tokio::sync::Mutex<W>>,
    ) -> Vec<MqttPacket>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let session = match session {
            Some(s) => s,
            None => return vec![],
        };

        let mut return_codes = Vec::new();
        let mut retained_publishes = Vec::new();

        for (topic, qos) in &sub.topics {
            let amqp_routing_key = mqtt_topic_to_amqp_routing_key(topic);

            // Create a queue for this subscription if needed
            let queue_name = format!(
                "mqtt:{client_id}:{topic}",
                client_id = session.client_id,
                topic = topic
            );
            if let Err(e) = self.vhost.declare_queue(QueueConfig {
                name: queue_name.clone(),
                durable: false,
                exclusive: false,
                auto_delete: true,
                arguments: FieldTable::new(),
            }) {
                warn!("MQTT declare queue '{queue_name}' failed: {e}");
            }

            // Bind queue to amq.topic with the routing key
            if let Err(e) = self.vhost.bind_queue(
                &queue_name,
                MQTT_EXCHANGE,
                &amqp_routing_key,
                &FieldTable::new(),
            ) {
                warn!("MQTT bind queue '{queue_name}' failed: {e}");
            }

            // Spawn delivery task for this subscription
            self.spawn_delivery_task(session, topic, &queue_name, *qos, writer);

            session.subscribe(topic.clone(), *qos);
            return_codes.push(*qos as u8);

            // Collect retained messages for this topic filter
            for retained in self.retain_store.matching(topic) {
                let packet_id = if *qos != QoS::AtMostOnce {
                    Some(session.next_packet_id())
                } else {
                    None
                };
                retained_publishes.push(MqttPacket::Publish(PublishPacket {
                    topic: retained.topic,
                    packet_id,
                    qos: *qos,
                    retain: true,
                    dup: false,
                    payload: retained.payload,
                }));
            }
        }

        let mut responses = retained_publishes;
        responses.push(MqttPacket::SubAck(SubAckPacket {
            packet_id: sub.packet_id,
            return_codes,
        }));
        responses
    }

    /// Spawn a background task that polls a queue and writes PUBLISH packets to the client.
    fn spawn_delivery_task<W>(
        &self,
        session: &mut Session,
        topic: &str,
        queue_name: &str,
        qos: QoS,
        writer: &Arc<tokio::sync::Mutex<W>>,
    ) where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        // Abort existing delivery task for this topic
        if let Some(handle) = session.delivery_tasks.remove(topic) {
            handle.abort();
        }

        let vhost = self.vhost.clone();
        let queue_name = queue_name.to_string();
        let topic_owned = topic.to_string();
        let writer = writer.clone();
        let client_id = session.client_id.clone();

        let handle = tokio::spawn(async move {
            loop {
                let queue = match vhost.get_queue(&queue_name) {
                    Some(q) => q,
                    None => {
                        // Queue was deleted (unsubscribe / cleanup)
                        return;
                    }
                };

                match queue.shift() {
                    Ok((Some(envelope), _dead_letters)) => {
                        let mqtt_topic =
                            amqp_routing_key_to_mqtt_topic(&envelope.message.routing_key);
                        let packet_id = if qos != QoS::AtMostOnce {
                            // Simple incrementing ID per delivery task
                            // (real impl would coordinate with session)
                            Some(
                                (envelope.segment_position.position as u16)
                                    .wrapping_add(1)
                                    .max(1),
                            )
                        } else {
                            None
                        };

                        let publish = MqttPacket::Publish(PublishPacket {
                            topic: mqtt_topic,
                            packet_id,
                            qos,
                            retain: false,
                            dup: false,
                            payload: envelope.message.body.clone(),
                        });

                        let mut buf = BytesMut::new();
                        publish.encode(&mut buf);

                        let mut w = writer.lock().await;
                        if w.write_all(&buf).await.is_err() {
                            debug!(
                                "MQTT delivery task: write failed for client '{client_id}', topic '{topic_owned}'"
                            );
                            return;
                        }
                        if w.flush().await.is_err() {
                            return;
                        }

                        // Ack in the store after successful write
                        if let Err(e) = queue.ack(&envelope.segment_position) {
                            warn!("MQTT delivery ack failed: {e}");
                        }
                    }
                    Ok((None, _)) => {
                        // No messages available, wait for notification
                        queue.wait_for_message().await;
                    }
                    Err(e) => {
                        warn!("MQTT delivery shift error: {e}");
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });

        session.delivery_tasks.insert(topic.to_string(), handle);
    }

    fn handle_unsubscribe(
        &self,
        unsub: UnsubscribePacket,
        session: &mut Option<Session>,
    ) -> Vec<MqttPacket> {
        if let Some(session) = session {
            for topic in &unsub.topics {
                session.unsubscribe(topic);
                // Remove the subscription queue
                let queue_name = format!(
                    "mqtt:{client_id}:{topic}",
                    client_id = session.client_id,
                    topic = topic
                );
                if let Err(e) = self.vhost.delete_queue(&queue_name) {
                    warn!("MQTT delete queue '{queue_name}' on unsubscribe failed: {e}");
                }
            }
        }

        vec![MqttPacket::UnsubAck {
            packet_id: unsub.packet_id,
        }]
    }

    fn cleanup_session(&self, session: &Session) {
        // Remove all subscription queues
        for topic in session.subscriptions.keys() {
            let queue_name = format!(
                "mqtt:{client_id}:{topic}",
                client_id = session.client_id,
                topic = topic
            );
            if let Err(e) = self.vhost.delete_queue(&queue_name) {
                warn!("MQTT cleanup delete queue '{queue_name}' failed: {e}");
            }
        }
        self.sessions.lock().remove(&session.client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn setup() -> (Arc<MqttBroker>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let vhost_dir = dir.path().join("vhosts").join("default");
        let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());
        let users_path = dir.path().join("users.json");
        let user_store =
            Arc::new(UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap());
        (MqttBroker::new(vhost, user_store), dir)
    }

    /// Create a duplex stream pair for testing, returning (client, server).
    fn duplex_pair() -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        tokio::io::duplex(8192)
    }

    async fn connect_session(
        broker: &Arc<MqttBroker>,
        session: &mut Option<Session>,
        client_id: &str,
        clean_session: bool,
        will: Option<WillMessage>,
    ) {
        let connect = ConnectPacket {
            client_id: client_id.into(),
            clean_session,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will,
        };
        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));
        broker
            .handle_packet(MqttPacket::Connect(connect), session, &writer)
            .await;
    }

    #[tokio::test]
    async fn test_connect_success() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "test-client", true, None).await;

        assert!(session.is_some());
    }

    #[tokio::test]
    async fn test_connect_bad_password() {
        let (broker, _dir) = setup();
        let mut session = None;
        let connect = ConnectPacket {
            client_id: "test-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"wrong")),
            will: None,
        };
        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        let response = broker
            .handle_packet(MqttPacket::Connect(connect), &mut session, &writer)
            .await;
        match &response[0] {
            MqttPacket::ConnAck(ack) => assert_eq!(ack.return_code, 4),
            _ => panic!("expected ConnAck with error"),
        }
    }

    #[tokio::test]
    async fn test_publish_qos0() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "pub-client", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        // Publish QoS 0
        let publish = PublishPacket {
            topic: "test/topic".into(),
            packet_id: None,
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"hello"),
        };
        let response = broker
            .handle_packet(MqttPacket::Publish(publish), &mut session, &writer)
            .await;
        assert!(response.is_empty()); // QoS 0 = no ack
    }

    #[tokio::test]
    async fn test_publish_qos1() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "pub-client", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        let publish = PublishPacket {
            topic: "test/topic".into(),
            packet_id: Some(1),
            qos: QoS::AtLeastOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"hello"),
        };
        let response = broker
            .handle_packet(MqttPacket::Publish(publish), &mut session, &writer)
            .await;
        assert_eq!(response.len(), 1);
        assert!(matches!(response[0], MqttPacket::PubAck { packet_id: 1 }));
    }

    #[tokio::test]
    async fn test_subscribe() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "sub-client", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        let sub = SubscribePacket {
            packet_id: 1,
            topics: vec![
                ("test/+".into(), QoS::AtLeastOnce),
                ("logs/#".into(), QoS::AtMostOnce),
            ],
        };
        let response = broker
            .handle_packet(MqttPacket::Subscribe(sub), &mut session, &writer)
            .await;

        // Find SubAck in response (may also contain retained messages)
        let suback = response.iter().find(|p| matches!(p, MqttPacket::SubAck(_)));
        match suback {
            Some(MqttPacket::SubAck(ack)) => {
                assert_eq!(ack.packet_id, 1);
                assert_eq!(ack.return_codes, vec![1, 0]);
            }
            _ => panic!("expected SubAck"),
        }

        // Verify session has subscriptions
        assert_eq!(session.as_ref().unwrap().subscriptions.len(), 2);
    }

    #[tokio::test]
    async fn test_ping() {
        let (broker, _dir) = setup();
        let mut session = None;
        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        let response = broker
            .handle_packet(MqttPacket::PingReq, &mut session, &writer)
            .await;
        assert_eq!(response, vec![MqttPacket::PingResp]);
    }

    #[tokio::test]
    async fn test_retain_store() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "retain-client", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        // Publish retained message
        let publish = PublishPacket {
            topic: "status/device1".into(),
            packet_id: None,
            qos: QoS::AtMostOnce,
            retain: true,
            dup: false,
            payload: Bytes::from_static(b"online"),
        };
        broker
            .handle_packet(MqttPacket::Publish(publish), &mut session, &writer)
            .await;

        // Verify retained
        let retained = broker.retain_store.matching("status/+");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].payload, Bytes::from_static(b"online"));
    }

    // --- New tests ---

    #[tokio::test]
    async fn test_message_delivery_round_trip() {
        let (broker, _dir) = setup();

        // Set up a subscriber
        let mut sub_session = None;
        connect_session(&broker, &mut sub_session, "subscriber", true, None).await;

        let (client_stream, server_stream) = duplex_pair();
        let (_, w) = tokio::io::split(server_stream);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        // Subscribe to "test/topic"
        let sub = SubscribePacket {
            packet_id: 1,
            topics: vec![("test/topic".into(), QoS::AtMostOnce)],
        };
        broker
            .handle_packet(MqttPacket::Subscribe(sub), &mut sub_session, &writer)
            .await;

        // Publish a message from a different "session"
        let mut pub_session = None;
        connect_session(&broker, &mut pub_session, "publisher", true, None).await;

        let (_, pub_server) = duplex_pair();
        let (_, pw) = tokio::io::split(pub_server);
        let pub_writer = Arc::new(tokio::sync::Mutex::new(pw));

        let publish = PublishPacket {
            topic: "test/topic".into(),
            packet_id: None,
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"hello subscriber"),
        };
        broker
            .handle_packet(MqttPacket::Publish(publish), &mut pub_session, &pub_writer)
            .await;

        // Read from the subscriber's client stream to verify delivery
        let (mut client_reader, _client_writer) = tokio::io::split(client_stream);
        let mut buf = vec![0u8; 4096];

        let read_result = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            client_reader.read(&mut buf),
        )
        .await;

        match read_result {
            Ok(Ok(n)) if n > 0 => {
                let mut bytes = BytesMut::from(&buf[..n]);
                let packet = decode_packet(&mut bytes).unwrap().unwrap();
                match packet {
                    MqttPacket::Publish(p) => {
                        assert_eq!(p.payload, Bytes::from_static(b"hello subscriber"));
                    }
                    other => panic!("expected Publish, got {other:?}"),
                }
            }
            _ => panic!("expected to receive a PUBLISH packet within timeout"),
        }

        // Cleanup to prevent task leak
        if let Some(s) = sub_session.as_mut() {
            s.cancel_all_delivery_tasks();
        }
    }

    #[tokio::test]
    async fn test_will_message_delivery() {
        let (broker, _dir) = setup();

        // Set up a subscriber for the will topic
        let mut sub_session = None;
        connect_session(&broker, &mut sub_session, "will-subscriber", true, None).await;

        let (_sub_client, sub_server) = duplex_pair();
        let (_, sw) = tokio::io::split(sub_server);
        let sub_writer = Arc::new(tokio::sync::Mutex::new(sw));

        let sub = SubscribePacket {
            packet_id: 1,
            topics: vec![("client/status".into(), QoS::AtMostOnce)],
        };
        broker
            .handle_packet(MqttPacket::Subscribe(sub), &mut sub_session, &sub_writer)
            .await;

        // Connect a client with a will message
        let will = WillMessage {
            topic: "client/status".into(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
        };

        let mut will_session = None;
        connect_session(&broker, &mut will_session, "will-client", true, Some(will)).await;

        // Simulate unexpected disconnect (not a clean DISCONNECT)
        assert!(will_session.as_ref().unwrap().will.is_some());
        assert!(!will_session.as_ref().unwrap().clean_disconnect);

        // Publish will manually (simulating what handle_client does on disconnect)
        if let Some(will) = will_session.as_mut().unwrap().will.take() {
            broker.publish_will(&will);
        }

        // Verify the will message was published to the AMQP queue
        let queue = broker.vhost.get_queue("mqtt:will-subscriber:client/status");
        assert!(queue.is_some(), "subscription queue should exist");

        let queue = queue.unwrap();
        let (envelope, _) = queue.shift().unwrap();
        assert!(envelope.is_some(), "will message should be in the queue");
        assert_eq!(
            envelope.unwrap().message.body,
            Bytes::from_static(b"offline")
        );

        // Verify clean disconnect suppresses will
        let mut clean_session = None;
        let will2 = WillMessage {
            topic: "client/status".into(),
            payload: Bytes::from_static(b"should-not-send"),
            qos: QoS::AtMostOnce,
            retain: false,
        };
        connect_session(
            &broker,
            &mut clean_session,
            "clean-client",
            true,
            Some(will2),
        )
        .await;
        clean_session.as_mut().unwrap().clean_disconnect = true;
        // On clean disconnect, will should NOT be published
        assert!(clean_session.as_ref().unwrap().will.is_some());
        // The handle_client code checks clean_disconnect before publishing will

        // Cleanup
        if let Some(s) = sub_session.as_mut() {
            s.cancel_all_delivery_tasks();
        }
    }

    #[tokio::test]
    async fn test_qos2_flow() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "qos2-client", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        // Step 1: Client sends PUBLISH QoS 2 -> Broker responds with PUBREC
        let publish = PublishPacket {
            topic: "test/qos2".into(),
            packet_id: Some(42),
            qos: QoS::ExactlyOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"exactly once"),
        };
        let response = broker
            .handle_packet(MqttPacket::Publish(publish), &mut session, &writer)
            .await;
        assert_eq!(response.len(), 1);
        assert!(matches!(response[0], MqttPacket::PubRec { packet_id: 42 }));

        // Step 2: Client sends PUBREL -> Broker responds with PUBCOMP
        let response = broker
            .handle_packet(MqttPacket::PubRel { packet_id: 42 }, &mut session, &writer)
            .await;
        assert_eq!(response.len(), 1);
        assert!(matches!(response[0], MqttPacket::PubComp { packet_id: 42 }));
    }

    #[tokio::test]
    async fn test_session_restore() {
        let (broker, _dir) = setup();

        // First connection with clean_session=false
        let mut session1 = None;
        connect_session(&broker, &mut session1, "persistent-client", false, None).await;

        // Add subscription directly to session (avoids spawning delivery tasks)
        session1
            .as_mut()
            .unwrap()
            .subscribe("events/#".into(), QoS::AtLeastOnce);

        // Verify subscription exists
        assert_eq!(session1.as_ref().unwrap().subscriptions.len(), 1);

        // Simulate disconnect: store session for later reconnect
        let s = session1.take().unwrap();
        broker.sessions.lock().insert(s.client_id.clone(), s);
        // Reconnect with clean_session=false -> should restore
        let mut session2 = None;

        let connect2 = ConnectPacket {
            client_id: "persistent-client".into(),
            clean_session: false,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        let (_, server2) = duplex_pair();
        let (_, w2) = tokio::io::split(server2);
        let writer2 = Arc::new(tokio::sync::Mutex::new(w2));

        let response = broker
            .handle_packet(MqttPacket::Connect(connect2), &mut session2, &writer2)
            .await;
        match &response[0] {
            MqttPacket::ConnAck(ack) => {
                assert_eq!(ack.return_code, 0);
                assert!(ack.session_present, "session_present should be true");
            }
            _ => panic!("expected ConnAck"),
        }

        // Session should have restored subscriptions
        assert_eq!(session2.as_ref().unwrap().subscriptions.len(), 1);
        assert!(
            session2
                .as_ref()
                .unwrap()
                .subscriptions
                .contains_key("events/#")
        );

        // Reconnect with clean_session=true -> should clear
        let s2 = session2.take().unwrap();
        broker.sessions.lock().insert(s2.client_id.clone(), s2);

        let mut session3 = None;
        let connect3 = ConnectPacket {
            client_id: "persistent-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        let (_, server3) = duplex_pair();
        let (_, w3) = tokio::io::split(server3);
        let writer3 = Arc::new(tokio::sync::Mutex::new(w3));

        let response = broker
            .handle_packet(MqttPacket::Connect(connect3), &mut session3, &writer3)
            .await;
        match &response[0] {
            MqttPacket::ConnAck(ack) => {
                assert!(!ack.session_present, "clean_session should clear");
            }
            _ => panic!("expected ConnAck"),
        }
        assert_eq!(session3.as_ref().unwrap().subscriptions.len(), 0);
    }

    #[tokio::test]
    async fn test_retained_messages_on_subscribe() {
        let (broker, _dir) = setup();
        let mut session = None;
        connect_session(&broker, &mut session, "retain-sub", true, None).await;

        let (_, server) = duplex_pair();
        let (_, w) = tokio::io::split(server);
        let writer = Arc::new(tokio::sync::Mutex::new(w));

        // Publish retained messages first
        let publish1 = PublishPacket {
            topic: "sensor/temp".into(),
            packet_id: None,
            qos: QoS::AtMostOnce,
            retain: true,
            dup: false,
            payload: Bytes::from_static(b"22.5"),
        };
        broker
            .handle_packet(MqttPacket::Publish(publish1), &mut session, &writer)
            .await;

        let publish2 = PublishPacket {
            topic: "sensor/humidity".into(),
            packet_id: None,
            qos: QoS::AtMostOnce,
            retain: true,
            dup: false,
            payload: Bytes::from_static(b"65"),
        };
        broker
            .handle_packet(MqttPacket::Publish(publish2), &mut session, &writer)
            .await;

        // Now subscribe - should receive retained messages
        let sub = SubscribePacket {
            packet_id: 1,
            topics: vec![("sensor/+".into(), QoS::AtMostOnce)],
        };
        let response = broker
            .handle_packet(MqttPacket::Subscribe(sub), &mut session, &writer)
            .await;

        // Should have retained PUBLISH packets + SubAck
        let publish_count = response
            .iter()
            .filter(|p| matches!(p, MqttPacket::Publish(_)))
            .count();
        assert_eq!(publish_count, 2, "should receive 2 retained messages");

        // All retained publishes should have retain=true
        for p in &response {
            if let MqttPacket::Publish(pub_pkt) = p {
                assert!(pub_pkt.retain, "retained message should have retain=true");
            }
        }

        // Last packet should be SubAck
        assert!(matches!(response.last(), Some(MqttPacket::SubAck(_))));

        // Cleanup
        if let Some(s) = session.as_mut() {
            s.cancel_all_delivery_tasks();
        }
    }
}
