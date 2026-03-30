use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rmq_protocol::field_table::{FieldTable, FieldValue};
use rmq_storage::message::StoredMessage;
use rmq_storage::message_store::{Envelope, MessageStore};
use rmq_storage::segment_position::SegmentPosition;
use tokio::sync::Notify;

/// Queue configuration.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub name: String,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: FieldTable,
}

/// Overflow policy when max-length is reached.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OverflowPolicy {
    DropHead,
    RejectPublish,
}

/// Result of a publish attempt.
#[derive(Debug)]
pub enum PublishResult {
    Accepted(SegmentPosition),
    Rejected,
}

/// Information about a dead-lettered message.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub message: StoredMessage,
    pub exchange: String,
    pub routing_key: String,
}

/// Fast-path ring buffer capacity.
const FAST_PATH_CAPACITY: usize = 131072;

/// A message queue with a fast-path delivery channel.
///
/// Design:
/// - `publish()` persists to mmap store, then pushes to a bounded fast-path channel.
/// - `shift()`/`shift_batch()` reads ONLY from the fast-path channel.
/// - On startup, unacked messages from the store are loaded into the channel.
/// - If the channel is full on publish, we notify consumers and they'll drain it.
///   The store always has the message for crash recovery.
///
/// This eliminates publisher-consumer lock contention entirely on the hot path.
/// The store mutex is only held during publish (write) — never during consume.
pub struct Queue {
    config: QueueConfig,
    store: Mutex<MessageStore>,
    consumer_count: AtomicU64,
    publish_notify: Notify,
    /// Flag indicating pending writes need fsync. The flush coordinator
    /// checks this periodically and batches the fsync.
    pending_flush: std::sync::atomic::AtomicBool,
    /// Notified after a batch fsync completes. Publisher confirms wait on this.
    flush_complete: Notify,

    /// Fast-path: bounded channel from publisher to consumer.
    fast_tx: tokio::sync::mpsc::Sender<Envelope>,
    fast_rx: Mutex<tokio::sync::mpsc::Receiver<Envelope>>,

    /// Positions delivered via the fast-path channel but not yet consumer-acked.
    /// These are skipped by the store fallback to prevent duplicate delivery.
    /// When the consumer acks, positions are moved from here to the store's ack set.
    channel_delivered: Mutex<std::collections::HashSet<(u32, u32)>>,

    // Parsed queue arguments
    message_ttl: Option<u64>,
    max_length: Option<u64>,
    max_length_bytes: Option<u64>,
    overflow: OverflowPolicy,
    dead_letter_exchange: Option<String>,
    dead_letter_routing_key: Option<String>,
    expires: Option<u64>,
    delivery_limit: Option<u64>,
}

impl Queue {
    pub fn new(config: QueueConfig, store: MessageStore) -> Self {
        let message_ttl = get_arg_u64(&config.arguments, "x-message-ttl");
        let max_length = get_arg_u64(&config.arguments, "x-max-length");
        let max_length_bytes = get_arg_u64(&config.arguments, "x-max-length-bytes");
        let overflow = match config.arguments.get("x-overflow") {
            Some(FieldValue::ShortString(s)) if s == "reject-publish" => {
                OverflowPolicy::RejectPublish
            }
            _ => OverflowPolicy::DropHead,
        };
        let dead_letter_exchange = get_arg_string(&config.arguments, "x-dead-letter-exchange");
        let dead_letter_routing_key =
            get_arg_string(&config.arguments, "x-dead-letter-routing-key");
        let expires = get_arg_u64(&config.arguments, "x-expires");
        let delivery_limit = get_arg_u64(&config.arguments, "x-delivery-limit");

        let (fast_tx, fast_rx) = tokio::sync::mpsc::channel(FAST_PATH_CAPACITY);

        // On startup, existing messages are in the mmap store. Consumers will
        // read them via the store fallback path (no need to pre-populate the
        // channel — the channel is for new messages from live publishers).
        {
            let msg_count = store.message_count();
            if msg_count > 0 {
                tracing::debug!(
                    "queue '{}': {} messages available in store for recovery",
                    config.name,
                    msg_count
                );
            }
            let queue = Self {
                config,
                store: Mutex::new(store),
                consumer_count: AtomicU64::new(0),
                publish_notify: Notify::new(),
                pending_flush: std::sync::atomic::AtomicBool::new(false),
                flush_complete: Notify::new(),
                fast_tx,
                fast_rx: Mutex::new(fast_rx),
                channel_delivered: Mutex::new(std::collections::HashSet::new()),
                message_ttl,
                max_length,
                max_length_bytes,
                overflow,
                dead_letter_exchange,
                dead_letter_routing_key,
                expires,
                delivery_limit,
            };
            return queue;
        }
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }
    pub fn is_durable(&self) -> bool {
        self.config.durable
    }
    pub fn is_exclusive(&self) -> bool {
        self.config.exclusive
    }
    pub fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }
    pub fn arguments(&self) -> &FieldTable {
        &self.config.arguments
    }
    pub fn dead_letter_exchange(&self) -> Option<&str> {
        self.dead_letter_exchange.as_deref()
    }
    pub fn dead_letter_routing_key(&self) -> Option<&str> {
        self.dead_letter_routing_key.as_deref()
    }

    /// Publish a message.
    /// 1. Persist to mmap store (under lock) — guarantees durability.
    /// 2. Push Envelope to fast-path channel for consumer delivery.
    pub fn publish(
        &self,
        msg: &StoredMessage,
    ) -> std::io::Result<(PublishResult, Vec<DeadLetter>)> {
        let mut store = self.store.lock();
        let mut dead_letters = Vec::new();

        if self.overflow == OverflowPolicy::RejectPublish {
            if let Some(max) = self.max_length {
                if store.message_count() >= max {
                    return Ok((PublishResult::Rejected, dead_letters));
                }
            }
            if let Some(max_bytes) = self.max_length_bytes {
                if store.byte_size() + msg.bytesize() as u64 > max_bytes {
                    return Ok((PublishResult::Rejected, dead_letters));
                }
            }
        }

        let sp = store.push(msg)?;
        let needs_fsync = self.config.durable && msg.properties.is_persistent();

        // For durable messages: request async flush. The flush coordinator
        // batches multiple messages into a single msync every few ms.
        // The caller (publisher confirm) waits for the flush to complete.
        if needs_fsync {
            self.pending_flush.store(true, Ordering::Release);
        }

        // Drop-head overflow: drop from store AND drain corresponding channel entries
        if self.overflow == OverflowPolicy::DropHead {
            if let Some(max) = self.max_length {
                while store.message_count() > max {
                    // Drop oldest from the channel (matches store order)
                    if let Ok(old_env) = self.fast_rx.lock().try_recv() {
                        store.ack(&old_env.segment_position)?;
                        if self.dead_letter_exchange.is_some() {
                            dead_letters.push(self.make_dead_letter(old_env, "maxlen"));
                        }
                    } else if let Some(env) = store.shift()? {
                        store.ack(&env.segment_position)?;
                        if self.dead_letter_exchange.is_some() {
                            dead_letters.push(self.make_dead_letter(env, "maxlen"));
                        }
                    } else {
                        break;
                    }
                }
            }
            if let Some(max_bytes) = self.max_length_bytes {
                while store.byte_size() > max_bytes {
                    if let Ok(old_env) = self.fast_rx.lock().try_recv() {
                        store.ack(&old_env.segment_position)?;
                        if self.dead_letter_exchange.is_some() {
                            dead_letters.push(self.make_dead_letter(old_env, "maxlen"));
                        }
                    } else if let Some(env) = store.shift()? {
                        store.ack(&env.segment_position)?;
                        if self.dead_letter_exchange.is_some() {
                            dead_letters.push(self.make_dead_letter(env, "maxlen"));
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        drop(store);

        // Push to fast-path channel for immediate consumer delivery.
        // Messages are in BOTH the store and the channel. When the consumer
        // reads from the channel, it acks in the store so the store won't
        // re-deliver. When the channel is full, the consumer falls back to
        // the store (which has all un-acked messages).
        let envelope = Envelope {
            segment_position: sp,
            message: Arc::new(msg.clone()),
            redelivered: false,
        };
        let _ = self.fast_tx.try_send(envelope);

        // Wake consumers
        self.publish_notify.notify_waiters();

        Ok((PublishResult::Accepted(sp), dead_letters))
    }

    /// Wait for a message to be available.
    pub async fn wait_for_message(&self) {
        self.publish_notify.notified().await;
    }

    /// Perform a batched fsync if there are pending durable writes.
    /// Called periodically by a background task. Wakes all publishers
    /// waiting for their confirms.
    pub fn flush_if_needed(&self) -> std::io::Result<bool> {
        if !self.pending_flush.swap(false, Ordering::AcqRel) {
            return Ok(false);
        }
        self.store.lock().sync()?;
        self.flush_complete.notify_waiters();
        Ok(true)
    }

    /// Wait for the next batch fsync to complete.
    /// Publisher confirms call this after publish to ensure durability.
    pub async fn wait_for_flush(&self) {
        self.flush_complete.notified().await;
    }

    /// Consume the next message. Checks fast-path channel first, then falls
    /// back to the store (which has requeued messages and overflow).
    /// When reading from the channel, marks the message as "delivered" in the
    /// store so the store fallback won't re-deliver it.
    pub fn shift(&self) -> std::io::Result<(Option<Envelope>, Vec<DeadLetter>)> {
        let now = now_millis();

        // Fast path: try channel (no store lock)
        if let Ok(env) = self.fast_rx.lock().try_recv() {
            if self.is_expired(&env.message, now) {
                self.store.lock().ack(&env.segment_position)?;
                let dl = if self.dead_letter_exchange.is_some() {
                    vec![self.make_dead_letter(env, "expired")]
                } else {
                    vec![]
                };
                return Ok((None, dl));
            }
            // Track this position so the store fallback skips it.
            // Don't ack in the store yet — the consumer hasn't acked.
            // If the consumer disconnects, ServerChannel::Drop requeues it.
            self.channel_delivered
                .lock()
                .insert((env.segment_position.segment, env.segment_position.position));
            return Ok((Some(env), vec![]));
        }

        // Slow path: check store for requeued messages and overflow
        let delivered_snapshot: std::collections::HashSet<(u32, u32)> =
            self.channel_delivered.lock().clone();
        let mut store = self.store.lock();
        let mut dead_letters = Vec::new();
        loop {
            match store.shift()? {
                Some(env) => {
                    // If already delivered via channel, ack to advance past it
                    if delivered_snapshot
                        .contains(&(env.segment_position.segment, env.segment_position.position))
                    {
                        store.ack(&env.segment_position)?;
                        continue;
                    }
                    if self.is_expired(&env.message, now) {
                        store.ack(&env.segment_position)?;
                        if self.dead_letter_exchange.is_some() {
                            dead_letters.push(self.make_dead_letter(env, "expired"));
                        }
                        continue;
                    }
                    return Ok((Some(env), dead_letters));
                }
                None => return Ok((None, dead_letters)),
            }
        }
    }

    /// Consume up to `max` messages. Fast-path channel first, then store fallback.
    /// Uses a single store lock acquisition for all ack + shift operations.
    pub fn shift_batch(&self, max: usize) -> std::io::Result<(Vec<Envelope>, Vec<DeadLetter>)> {
        let now = now_millis();
        let mut envelopes = Vec::with_capacity(max);
        let mut dead_letters = Vec::new();
        let mut channel_positions = Vec::new();

        // Phase 1: drain fast-path channel (no store lock)
        {
            let mut rx = self.fast_rx.lock();
            while envelopes.len() < max {
                match rx.try_recv() {
                    Ok(env) => {
                        if self.is_expired(&env.message, now) {
                            channel_positions.push(env.segment_position);
                            if self.dead_letter_exchange.is_some() {
                                dead_letters.push(self.make_dead_letter(env, "expired"));
                            }
                            continue;
                        }
                        channel_positions.push(env.segment_position);
                        envelopes.push(env);
                    }
                    Err(_) => break,
                }
            }
        }

        // Track channel-delivered positions (no store lock needed)
        if !channel_positions.is_empty() {
            let mut delivered = self.channel_delivered.lock();
            for sp in &channel_positions {
                delivered.insert((sp.segment, sp.position));
            }
        }

        // Phase 2: store fallback for overflow/requeued messages
        if envelopes.len() < max {
            let delivered_snapshot: std::collections::HashSet<(u32, u32)> =
                self.channel_delivered.lock().clone();
            let mut store = self.store.lock();
            while envelopes.len() < max {
                match store.shift()? {
                    Some(env) => {
                        // If already delivered via channel, ack it in the store
                        // to advance past it (consumer tracks it separately)
                        let key = (env.segment_position.segment, env.segment_position.position);
                        if delivered_snapshot.contains(&key) {
                            store.ack(&env.segment_position)?;
                            continue;
                        }
                        if self.is_expired(&env.message, now) {
                            store.ack(&env.segment_position)?;
                            if self.dead_letter_exchange.is_some() {
                                dead_letters.push(self.make_dead_letter(env, "expired"));
                            }
                            continue;
                        }
                        envelopes.push(env);
                    }
                    None => break,
                }
            }
        }

        Ok((envelopes, dead_letters))
    }

    /// Acknowledge multiple messages.
    pub fn ack_batch(&self, positions: &[SegmentPosition]) -> std::io::Result<()> {
        {
            let mut delivered = self.channel_delivered.lock();
            for sp in positions {
                delivered.remove(&(sp.segment, sp.position));
            }
        }
        let mut store = self.store.lock();
        for sp in positions {
            store.ack(sp)?;
        }
        Ok(())
    }

    /// Flush all channel_delivered positions to the store's ack set.
    /// Call this when the channel is empty to prepare the store for
    /// efficient sequential scanning of remaining overflow messages.
    pub fn flush_channel_delivered(&self) -> std::io::Result<()> {
        let positions: Vec<(u32, u32)> = self.channel_delivered.lock().drain().collect();
        if !positions.is_empty() {
            let mut store = self.store.lock();
            for (seg, pos) in &positions {
                // These may already be acked (from shift_batch Phase 2)
                // so ignore errors from double-ack
                let sp = SegmentPosition::new(*seg, *pos, 0);
                let _ = store.ack(&sp);
            }
        }
        Ok(())
    }

    pub fn peek(&self) -> std::io::Result<Option<Envelope>> {
        self.store.lock().peek()
    }

    pub fn ack(&self, sp: &SegmentPosition) -> std::io::Result<()> {
        self.channel_delivered
            .lock()
            .remove(&(sp.segment, sp.position));
        self.store.lock().ack(sp)
    }

    pub fn requeue(&self, sp: SegmentPosition) {
        self.store.lock().requeue(sp);
        self.publish_notify.notify_waiters();
    }

    pub fn message_count(&self) -> u64 {
        // Store count reflects un-acked messages (including those in-flight
        // via the channel). This is the authoritative count.
        self.store.lock().message_count()
    }

    pub fn consumer_count(&self) -> u64 {
        self.consumer_count.load(Ordering::Relaxed)
    }

    pub fn add_consumer(&self) -> u64 {
        self.consumer_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn remove_consumer(&self) -> u64 {
        self.consumer_count
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1)
    }

    pub fn purge(&self) -> std::io::Result<u32> {
        // Drain fast-path channel and ack those messages in the store
        let mut channel_positions = Vec::new();
        {
            let mut rx = self.fast_rx.lock();
            while let Ok(env) = rx.try_recv() {
                channel_positions.push(env.segment_position);
            }
        }
        let mut store = self.store.lock();
        for sp in &channel_positions {
            store.ack(sp)?;
        }
        let mut count = channel_positions.len() as u32;
        // Drain remaining from store
        while let Some(env) = store.shift()? {
            store.ack(&env.segment_position)?;
            count += 1;
        }
        Ok(count)
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.store.lock().sync()
    }

    fn is_expired(&self, msg: &StoredMessage, now_ms: u64) -> bool {
        if let Some(ref exp) = msg.properties.expiration {
            if let Ok(ttl_ms) = exp.parse::<u64>() {
                let msg_time_ms = (msg.timestamp as u64) * 1000;
                if now_ms > msg_time_ms + ttl_ms {
                    return true;
                }
            }
        }
        if let Some(ttl_ms) = self.message_ttl {
            let msg_time_ms = (msg.timestamp as u64) * 1000;
            if now_ms > msg_time_ms + ttl_ms {
                return true;
            }
        }
        false
    }

    fn make_dead_letter(&self, env: Envelope, reason: &str) -> DeadLetter {
        let mut msg = (*env.message).clone();
        let mut headers = msg.properties.headers.take().unwrap_or_default();
        let mut death = FieldTable::new();
        death.insert("reason", FieldValue::ShortString(reason.to_string()));
        death.insert("queue", FieldValue::ShortString(self.config.name.clone()));
        death.insert("exchange", FieldValue::ShortString(msg.exchange.clone()));
        death.insert(
            "routing-keys",
            FieldValue::FieldArray(vec![FieldValue::ShortString(msg.routing_key.clone())]),
        );
        headers.insert(
            "x-death",
            FieldValue::FieldArray(vec![FieldValue::FieldTable(death)]),
        );
        msg.properties.headers = Some(headers);

        let dlx = self.dead_letter_exchange.clone().unwrap_or_default();
        let dlrk = self
            .dead_letter_routing_key
            .clone()
            .unwrap_or_else(|| msg.routing_key.clone());

        DeadLetter {
            message: msg,
            exchange: dlx,
            routing_key: dlrk,
        }
    }
}

fn get_arg_u64(args: &FieldTable, key: &str) -> Option<u64> {
    match args.get(key)? {
        FieldValue::I32(v) => Some(*v as u64),
        FieldValue::I64(v) => Some(*v as u64),
        FieldValue::U32(v) => Some(*v as u64),
        FieldValue::U64(v) => Some(*v),
        FieldValue::ShortString(s) => s.parse().ok(),
        _ => None,
    }
}

fn get_arg_string(args: &FieldTable, key: &str) -> Option<String> {
    match args.get(key)? {
        FieldValue::ShortString(s) => Some(s.clone()),
        _ => None,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rmq_protocol::properties::BasicProperties;
    use tempfile::TempDir;

    fn make_queue_with_args(dir: &std::path::Path, args: FieldTable) -> Queue {
        let store = MessageStore::new(dir, 4096).unwrap();
        Queue::new(
            QueueConfig {
                name: "test-queue".into(),
                durable: true,
                exclusive: false,
                auto_delete: false,
                arguments: args,
            },
            store,
        )
    }

    fn make_queue(dir: &std::path::Path) -> Queue {
        make_queue_with_args(dir, FieldTable::new())
    }

    fn make_msg(body: &str) -> StoredMessage {
        StoredMessage {
            timestamp: now_millis() as i64 / 1000,
            exchange: "".into(),
            routing_key: "test".into(),
            properties: BasicProperties::default(),
            body: Bytes::from(body.as_bytes().to_vec()),
        }
    }

    #[test]
    fn test_publish_and_consume() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        let (result, _) = queue.publish(&make_msg("hello")).unwrap();
        assert!(matches!(result, PublishResult::Accepted(_)));
        let (result, _) = queue.publish(&make_msg("world")).unwrap();
        assert!(matches!(result, PublishResult::Accepted(_)));

        let (env, _) = queue.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"hello");
        let (env, _) = queue.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"world");
    }

    #[test]
    fn test_ack() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        let (result, _) = queue.publish(&make_msg("msg")).unwrap();
        let sp = match result {
            PublishResult::Accepted(sp) => sp,
            _ => panic!("expected accepted"),
        };
        queue.ack(&sp).unwrap();
    }

    #[test]
    fn test_purge() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        queue.publish(&make_msg("a")).unwrap();
        queue.publish(&make_msg("b")).unwrap();
        queue.publish(&make_msg("c")).unwrap();

        let count = queue.purge().unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_consumer_count() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        assert_eq!(queue.consumer_count(), 0);
        queue.add_consumer();
        assert_eq!(queue.consumer_count(), 1);
        queue.add_consumer();
        assert_eq!(queue.consumer_count(), 2);
        queue.remove_consumer();
        assert_eq!(queue.consumer_count(), 1);
    }

    #[test]
    fn test_max_length_drop_head() {
        let dir = TempDir::new().unwrap();
        let mut args = FieldTable::new();
        args.insert("x-max-length", FieldValue::I32(2));
        let queue = make_queue_with_args(dir.path(), args);

        queue.publish(&make_msg("first")).unwrap();
        queue.publish(&make_msg("second")).unwrap();
        queue.publish(&make_msg("third")).unwrap();

        // "first" should have been dropped
        let (env, _) = queue.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"second");
        let (env, _) = queue.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"third");
        let (env, _) = queue.shift().unwrap();
        assert!(env.is_none());
    }

    #[test]
    fn test_max_length_reject_publish() {
        let dir = TempDir::new().unwrap();
        let mut args = FieldTable::new();
        args.insert("x-max-length", FieldValue::I32(2));
        args.insert(
            "x-overflow",
            FieldValue::ShortString("reject-publish".into()),
        );
        let queue = make_queue_with_args(dir.path(), args);

        queue.publish(&make_msg("first")).unwrap();
        queue.publish(&make_msg("second")).unwrap();
        let (result, _) = queue.publish(&make_msg("third")).unwrap();
        assert!(matches!(result, PublishResult::Rejected));
    }

    #[test]
    fn test_dead_letter_on_overflow() {
        let dir = TempDir::new().unwrap();
        let mut args = FieldTable::new();
        args.insert("x-max-length", FieldValue::I32(1));
        args.insert(
            "x-dead-letter-exchange",
            FieldValue::ShortString("dlx".into()),
        );
        args.insert(
            "x-dead-letter-routing-key",
            FieldValue::ShortString("dead".into()),
        );
        let queue = make_queue_with_args(dir.path(), args);

        queue.publish(&make_msg("first")).unwrap();
        let (_, dead_letters) = queue.publish(&make_msg("second")).unwrap();
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].exchange, "dlx");
        assert_eq!(dead_letters[0].routing_key, "dead");
        assert_eq!(&dead_letters[0].message.body[..], b"first");
    }

    #[test]
    fn test_per_message_ttl_expired() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        let mut msg = make_msg("expired");
        msg.timestamp = (now_millis() / 1000 - 10) as i64;
        msg.properties.expiration = Some("1".to_string());
        queue.publish(&msg).unwrap();

        let (env, _) = queue.shift().unwrap();
        assert!(env.is_none());
    }

    #[test]
    fn test_per_message_ttl_not_expired() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        let mut msg = make_msg("alive");
        msg.properties.expiration = Some("999999999".to_string());
        queue.publish(&msg).unwrap();

        let (env, _) = queue.shift().unwrap();
        assert_eq!(&env.unwrap().message.body[..], b"alive");
    }

    #[test]
    fn test_queue_message_ttl() {
        let dir = TempDir::new().unwrap();
        let mut args = FieldTable::new();
        args.insert("x-message-ttl", FieldValue::I32(1));
        let queue = make_queue_with_args(dir.path(), args);

        let mut msg = make_msg("will-expire");
        msg.timestamp = (now_millis() / 1000 - 10) as i64;
        queue.publish(&msg).unwrap();

        let (env, _) = queue.shift().unwrap();
        assert!(env.is_none());
    }

    #[test]
    fn test_dead_letter_on_expiry() {
        let dir = TempDir::new().unwrap();
        let mut args = FieldTable::new();
        args.insert("x-message-ttl", FieldValue::I32(1));
        args.insert(
            "x-dead-letter-exchange",
            FieldValue::ShortString("dlx".into()),
        );
        let queue = make_queue_with_args(dir.path(), args);

        let mut msg = make_msg("dead-on-expiry");
        msg.timestamp = (now_millis() / 1000 - 10) as i64;
        queue.publish(&msg).unwrap();

        let (env, dead_letters) = queue.shift().unwrap();
        assert!(env.is_none());
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].exchange, "dlx");
    }

    #[test]
    fn test_shift_batch() {
        let dir = TempDir::new().unwrap();
        let queue = make_queue(dir.path());

        for i in 0..10 {
            queue.publish(&make_msg(&format!("msg-{i}"))).unwrap();
        }

        let (batch, _) = queue.shift_batch(5).unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(&batch[0].message.body[..], b"msg-0");
        assert_eq!(&batch[4].message.body[..], b"msg-4");

        let (batch2, _) = queue.shift_batch(10).unwrap();
        assert_eq!(batch2.len(), 5);
        assert_eq!(&batch2[0].message.body[..], b"msg-5");
    }
}
