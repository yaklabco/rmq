use std::collections::HashMap;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::codec::{QoS, WillMessage};

/// MQTT session state for a client.
pub struct Session {
    pub client_id: String,
    pub clean_session: bool,
    /// Topic subscriptions: topic filter -> granted QoS.
    pub subscriptions: HashMap<String, QoS>,
    /// Pending QoS 1/2 messages awaiting acknowledgment.
    pub pending_acks: HashMap<u16, PendingMessage>,
    /// Next packet ID for server-originated messages.
    next_packet_id: u16,
    /// Will message to publish on unexpected disconnect.
    pub will: Option<WillMessage>,
    /// Whether a clean DISCONNECT was received.
    pub clean_disconnect: bool,
    /// Delivery task handles per subscription topic.
    pub delivery_tasks: HashMap<String, JoinHandle<()>>,
}

pub struct PendingMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
}

impl Session {
    pub fn new(client_id: String, clean_session: bool) -> Self {
        Self {
            client_id,
            clean_session,
            subscriptions: HashMap::new(),
            pending_acks: HashMap::new(),
            next_packet_id: 1,
            will: None,
            clean_disconnect: false,
            delivery_tasks: HashMap::new(),
        }
    }

    pub fn next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    pub fn subscribe(&mut self, topic: String, qos: QoS) {
        self.subscriptions.insert(topic, qos);
    }

    pub fn unsubscribe(&mut self, topic: &str) {
        self.subscriptions.remove(topic);
        if let Some(handle) = self.delivery_tasks.remove(topic) {
            handle.abort();
        }
    }

    /// Cancel all delivery tasks.
    pub fn cancel_all_delivery_tasks(&mut self) {
        for (_, handle) in self.delivery_tasks.drain() {
            handle.abort();
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancel_all_delivery_tasks();
    }
}

/// Retained message store.
pub struct RetainStore {
    messages: RwLock<HashMap<String, RetainedMessage>>,
}

pub struct RetainedMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
}

impl RetainStore {
    pub fn new() -> Self {
        Self {
            messages: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, topic: String, payload: Bytes, qos: QoS) {
        if payload.is_empty() {
            // Empty payload removes retained message
            self.messages.write().remove(&topic);
        } else {
            self.messages.write().insert(
                topic.clone(),
                RetainedMessage {
                    topic,
                    payload,
                    qos,
                },
            );
        }
    }

    /// Get all retained messages matching a topic filter.
    pub fn matching(&self, filter: &str) -> Vec<RetainedMessage> {
        let messages = self.messages.read();
        messages
            .values()
            .filter(|m| topic_matches_filter(&m.topic, filter))
            .map(|m| RetainedMessage {
                topic: m.topic.clone(),
                payload: m.payload.clone(),
                qos: m.qos,
            })
            .collect()
    }
}

impl Default for RetainStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if an MQTT topic matches a subscription filter.
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    let topic_parts: Vec<&str> = topic.split('/').collect();
    let filter_parts: Vec<&str> = filter.split('/').collect();
    match_parts(&topic_parts, &filter_parts)
}

fn match_parts(topic: &[&str], filter: &[&str]) -> bool {
    match (topic.first(), filter.first()) {
        (_, Some(&"#")) => true, // # matches everything remaining
        (Some(_), Some(&"+")) => {
            // + matches exactly one level
            match_parts(&topic[1..], &filter[1..])
        }
        (Some(t), Some(f)) => {
            if t == f {
                match_parts(&topic[1..], &filter[1..])
            } else {
                false
            }
        }
        (None, None) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matching() {
        assert!(topic_matches_filter("a/b/c", "a/b/c"));
        assert!(topic_matches_filter("a/b/c", "a/+/c"));
        assert!(topic_matches_filter("a/b/c", "a/#"));
        assert!(topic_matches_filter("a/b/c", "#"));
        assert!(topic_matches_filter("a", "a"));
        assert!(!topic_matches_filter("a/b", "a/c"));
        assert!(!topic_matches_filter("a/b/c", "a/b"));
        assert!(topic_matches_filter("a/b/c/d", "a/+/c/d"));
        assert!(!topic_matches_filter("a/b/c/d", "a/+/d"));
    }

    #[test]
    fn test_retain_store() {
        let store = RetainStore::new();
        store.set(
            "sensor/temp".into(),
            Bytes::from_static(b"22.5"),
            QoS::AtLeastOnce,
        );
        store.set(
            "sensor/humidity".into(),
            Bytes::from_static(b"65"),
            QoS::AtMostOnce,
        );

        let matches = store.matching("sensor/+");
        assert_eq!(matches.len(), 2);

        let matches = store.matching("sensor/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].payload, Bytes::from_static(b"22.5"));

        // Remove retained
        store.set("sensor/temp".into(), Bytes::new(), QoS::AtMostOnce);
        let matches = store.matching("sensor/+");
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_session_packet_id() {
        let mut session = Session::new("test".into(), true);
        assert_eq!(session.next_packet_id(), 1);
        assert_eq!(session.next_packet_id(), 2);
        assert_eq!(session.next_packet_id(), 3);
    }
}
