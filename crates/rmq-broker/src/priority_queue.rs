use std::path::Path;

use rmq_protocol::field_table::FieldTable;
use rmq_storage::message::StoredMessage;
use rmq_storage::message_store::{Envelope, MessageStore};
use rmq_storage::segment_position::SegmentPosition;

use crate::queue::{DeadLetter, PublishResult, Queue, QueueConfig};

/// A priority queue that routes messages to sub-queues based on priority.
/// Priority 0 is lowest, max_priority is highest.
/// Messages without a priority property default to 0.
pub struct PriorityQueue {
    name: String,
    max_priority: u8,
    sub_queues: Vec<Queue>,
    config: QueueConfig,
}

impl PriorityQueue {
    pub fn new(
        config: QueueConfig,
        max_priority: u8,
        base_dir: &Path,
    ) -> std::io::Result<Self> {
        let max_priority = max_priority.min(255);
        let mut sub_queues = Vec::with_capacity(max_priority as usize + 1);

        for priority in 0..=max_priority {
            let sub_dir = base_dir.join(format!("priority-{priority}"));
            let store = MessageStore::new(&sub_dir, 8 * 1024 * 1024)?;
            let sub_config = QueueConfig {
                name: format!("{}-p{priority}", config.name),
                durable: config.durable,
                exclusive: config.exclusive,
                auto_delete: config.auto_delete,
                arguments: FieldTable::new(), // sub-queues don't inherit TTL/DLX args
            };
            sub_queues.push(Queue::new(sub_config, store));
        }

        Ok(Self {
            name: config.name.clone(),
            max_priority,
            sub_queues,
            config,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn max_priority(&self) -> u8 {
        self.max_priority
    }

    /// Publish a message. Routes to the sub-queue matching the message priority.
    pub fn publish(&self, msg: &StoredMessage) -> std::io::Result<(PublishResult, Vec<DeadLetter>)> {
        let priority = msg
            .properties
            .priority
            .unwrap_or(0)
            .min(self.max_priority);
        self.sub_queues[priority as usize].publish(msg)
    }

    /// Consume the next highest-priority message.
    pub fn shift(&self) -> std::io::Result<(Option<Envelope>, Vec<DeadLetter>)> {
        // Iterate from highest priority to lowest
        for priority in (0..=self.max_priority).rev() {
            let (env, dead_letters) = self.sub_queues[priority as usize].shift()?;
            if env.is_some() {
                return Ok((env, dead_letters));
            }
            // Return any dead letters even if no live message
            if !dead_letters.is_empty() {
                return Ok((None, dead_letters));
            }
        }
        Ok((None, vec![]))
    }

    /// Acknowledge a message. Tries all sub-queues.
    pub fn ack(&self, sp: &SegmentPosition) -> std::io::Result<()> {
        for q in &self.sub_queues {
            if q.ack(sp).is_ok() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Requeue a message into the appropriate priority sub-queue.
    pub fn requeue(&self, sp: SegmentPosition, priority: u8) {
        let priority = priority.min(self.max_priority);
        self.sub_queues[priority as usize].requeue(sp);
    }

    /// Total message count across all priorities.
    pub fn message_count(&self) -> u64 {
        self.sub_queues.iter().map(|q| q.message_count()).sum()
    }

    /// Total consumer count.
    pub fn consumer_count(&self) -> u64 {
        // Consumers are on the PriorityQueue, not sub-queues
        // For now, return 0 (consumer tracking is in the server layer)
        0
    }

    /// Purge all messages.
    pub fn purge(&self) -> std::io::Result<u32> {
        let mut total = 0;
        for q in &self.sub_queues {
            total += q.purge()?;
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmq_protocol::properties::BasicProperties;
    use tempfile::TempDir;

    fn make_msg(body: &str, priority: Option<u8>) -> StoredMessage {
        StoredMessage {
            timestamp: 0,
            exchange: "".into(),
            routing_key: "".into(),
            properties: BasicProperties {
                priority,
                ..Default::default()
            },
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_priority_ordering() {
        let dir = TempDir::new().unwrap();
        let config = QueueConfig {
            name: "prio-q".into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        };
        let pq = PriorityQueue::new(config, 9, dir.path()).unwrap();

        // Publish messages with different priorities
        pq.publish(&make_msg("low", Some(1))).unwrap();
        pq.publish(&make_msg("high", Some(9))).unwrap();
        pq.publish(&make_msg("medium", Some(5))).unwrap();
        pq.publish(&make_msg("default", None)).unwrap(); // priority 0

        assert_eq!(pq.message_count(), 4);

        // Should come out highest priority first
        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"high");

        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"medium");

        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"low");

        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"default");

        let (env, _) = pq.shift().unwrap();
        assert!(env.is_none());
    }

    #[test]
    fn test_priority_clamped_to_max() {
        let dir = TempDir::new().unwrap();
        let config = QueueConfig {
            name: "prio-q".into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        };
        let pq = PriorityQueue::new(config, 3, dir.path()).unwrap();

        // Priority 10 > max 3, should be clamped to 3
        pq.publish(&make_msg("clamped", Some(10))).unwrap();
        pq.publish(&make_msg("at-max", Some(3))).unwrap();

        // Both should be at priority 3, so FIFO within same priority
        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"clamped");
        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"at-max");
    }

    #[test]
    fn test_purge() {
        let dir = TempDir::new().unwrap();
        let config = QueueConfig {
            name: "prio-q".into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        };
        let pq = PriorityQueue::new(config, 5, dir.path()).unwrap();

        pq.publish(&make_msg("a", Some(1))).unwrap();
        pq.publish(&make_msg("b", Some(3))).unwrap();
        pq.publish(&make_msg("c", Some(5))).unwrap();

        let count = pq.purge().unwrap();
        assert_eq!(count, 3);
        assert_eq!(pq.message_count(), 0);
    }

    #[test]
    fn test_same_priority_fifo() {
        let dir = TempDir::new().unwrap();
        let config = QueueConfig {
            name: "prio-q".into(),
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        };
        let pq = PriorityQueue::new(config, 5, dir.path()).unwrap();

        pq.publish(&make_msg("first", Some(3))).unwrap();
        pq.publish(&make_msg("second", Some(3))).unwrap();
        pq.publish(&make_msg("third", Some(3))).unwrap();

        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"first");
        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"second");
        let (env, _) = pq.shift().unwrap();
        assert_eq!(env.unwrap().message.body, b"third");
    }
}
