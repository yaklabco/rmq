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
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut read_buf = BytesMut::with_capacity(4096);
        let mut session: Option<Session> = None;

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
                        let response = self.handle_packet(packet, &mut session).await;
                        for resp in response {
                            let mut buf = BytesMut::new();
                            resp.encode(&mut buf);
                            if writer.write_all(&buf).await.is_err() {
                                return;
                            }
                        }
                        if writer.flush().await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break, // need more data
                    Err(e) => {
                        warn!("MQTT decode error: {e}");
                        return;
                    }
                }
            }
        }

        // Cleanup session
        if let Some(session) = session {
            if session.clean_session {
                self.cleanup_session(&session);
            }
            debug!("MQTT client '{}' disconnected", session.client_id);
        }
    }

    async fn handle_packet(
        &self,
        packet: MqttPacket,
        session: &mut Option<Session>,
    ) -> Vec<MqttPacket> {
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
            MqttPacket::PubComp { packet_id } => {
                if let Some(s) = session {
                    s.pending_acks.remove(&packet_id);
                }
                vec![]
            }
            MqttPacket::Subscribe(sub) => self.handle_subscribe(sub, session),
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

        // Clean old session if exists
        let session_present = if connect.clean_session {
            if let Some(old) = self.sessions.lock().remove(&connect.client_id) {
                self.cleanup_session(&old);
            }
            false
        } else {
            self.sessions.lock().contains_key(&connect.client_id)
        };

        let new_session = Session::new(connect.client_id.clone(), connect.clean_session);
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

    fn handle_subscribe(
        &self,
        sub: SubscribePacket,
        session: &mut Option<Session>,
    ) -> Vec<MqttPacket> {
        let session = match session {
            Some(s) => s,
            None => return vec![],
        };

        let mut return_codes = Vec::new();

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

            session.subscribe(topic.clone(), *qos);
            return_codes.push(*qos as u8);
        }

        // Send retained messages for matched topics
        // (In a full impl, these would be sent as PUBLISH packets)

        vec![MqttPacket::SubAck(SubAckPacket {
            packet_id: sub.packet_id,
            return_codes,
        })]
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

    #[tokio::test]
    async fn test_connect_success() {
        let (broker, _dir) = setup();
        let mut session = None;
        let connect = ConnectPacket {
            client_id: "test-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };

        let response = broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
            .await;
        assert_eq!(response.len(), 1);
        match &response[0] {
            MqttPacket::ConnAck(ack) => {
                assert_eq!(ack.return_code, 0);
                assert!(!ack.session_present);
            }
            _ => panic!("expected ConnAck"),
        }
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

        let response = broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
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

        // Connect first
        let connect = ConnectPacket {
            client_id: "pub-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
            .await;

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
            .handle_packet(MqttPacket::Publish(publish), &mut session)
            .await;
        assert!(response.is_empty()); // QoS 0 = no ack
    }

    #[tokio::test]
    async fn test_publish_qos1() {
        let (broker, _dir) = setup();
        let mut session = None;

        let connect = ConnectPacket {
            client_id: "pub-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
            .await;

        let publish = PublishPacket {
            topic: "test/topic".into(),
            packet_id: Some(1),
            qos: QoS::AtLeastOnce,
            retain: false,
            dup: false,
            payload: Bytes::from_static(b"hello"),
        };
        let response = broker
            .handle_packet(MqttPacket::Publish(publish), &mut session)
            .await;
        assert_eq!(response.len(), 1);
        assert!(matches!(response[0], MqttPacket::PubAck { packet_id: 1 }));
    }

    #[tokio::test]
    async fn test_subscribe() {
        let (broker, _dir) = setup();
        let mut session = None;

        let connect = ConnectPacket {
            client_id: "sub-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
            .await;

        let sub = SubscribePacket {
            packet_id: 1,
            topics: vec![
                ("test/+".into(), QoS::AtLeastOnce),
                ("logs/#".into(), QoS::AtMostOnce),
            ],
        };
        let response = broker
            .handle_packet(MqttPacket::Subscribe(sub), &mut session)
            .await;
        match &response[0] {
            MqttPacket::SubAck(ack) => {
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
        let response = broker
            .handle_packet(MqttPacket::PingReq, &mut session)
            .await;
        assert_eq!(response, vec![MqttPacket::PingResp]);
    }

    #[tokio::test]
    async fn test_retain_store() {
        let (broker, _dir) = setup();
        let mut session = None;

        let connect = ConnectPacket {
            client_id: "retain-client".into(),
            clean_session: true,
            keep_alive: 60,
            username: Some("guest".into()),
            password: Some(Bytes::from_static(b"guest")),
            will: None,
        };
        broker
            .handle_packet(MqttPacket::Connect(connect), &mut session)
            .await;

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
            .handle_packet(MqttPacket::Publish(publish), &mut session)
            .await;

        // Verify retained
        let retained = broker.retain_store.matching("status/+");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].payload, Bytes::from_static(b"online"));
    }
}
