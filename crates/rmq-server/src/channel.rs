use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::Notify;
use tracing::{debug, warn};

use rmq_broker::exchange::ExchangeConfig;
use rmq_broker::queue::QueueConfig;
use rmq_broker::vhost::{VHost, VHostError};
use rmq_protocol::frame::*;
use rmq_protocol::properties::BasicProperties;
use rmq_protocol::types::*;
use rmq_storage::message::StoredMessage;
use rmq_storage::segment_position::SegmentPosition;

use crate::connection::FrameSender;

/// Maximum allowed message body size (128 MiB).
pub const MAX_MESSAGE_SIZE: u64 = 128 * 1024 * 1024;

/// Typed error for channel-level failures.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("frame send failed")]
    SendFailed,
    #[error("vhost error: {0}")]
    VHost(#[from] rmq_broker::vhost::VHostError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Unacked message tracking.
#[derive(Clone)]
struct Unacked {
    #[allow(dead_code)]
    delivery_tag: u64,
    queue_name: String,
    segment_position: SegmentPosition,
}

/// Consumer state.
#[allow(dead_code)]
struct Consumer {
    tag: String,
    queue_name: String,
    no_ack: bool,
    /// Handle for the delivery task.
    task: Option<tokio::task::JoinHandle<()>>,
}

/// Shared state between channel and its consumer delivery tasks.
struct SharedChannelState {
    /// Number of unacked messages on this channel (for global prefetch).
    unacked_count: AtomicU64,
    /// Notified when a message is acked (so delivery tasks can resume).
    ack_notify: Notify,
    /// Shared unacked message map — keyed by delivery_tag for O(log n) lookups.
    unacked: parking_lot::Mutex<BTreeMap<u64, Unacked>>,
    /// Per-consumer prefetch count, visible to delivery tasks.
    prefetch_count: AtomicU16,
    /// Global prefetch count, visible to delivery tasks.
    global_prefetch_count: AtomicU16,
    /// Flow control state — when false, delivery tasks must pause.
    flow_active: std::sync::atomic::AtomicBool,
    /// Notified when flow state changes.
    flow_notify: Notify,
}

/// Server-side AMQP channel.
pub struct ServerChannel {
    id: u16,
    vhost: Arc<VHost>,
    tx: FrameSender,

    /// Next delivery tag (monotonically increasing per channel).
    /// Shared with delivery tasks via Arc<AtomicU64>.
    next_delivery_tag: Arc<AtomicU64>,

    /// Active consumers.
    consumers: HashMap<String, Consumer>,

    /// Publisher confirm mode.
    confirm_mode: bool,
    confirm_counter: u64,

    /// In-progress publish state.
    publish_state: Option<PublishState>,

    /// Per-consumer prefetch count (Basic.Qos with global=false).
    prefetch_count: u16,
    /// Global prefetch count (Basic.Qos with global=true).
    global_prefetch_count: u16,

    /// Shared state for delivery tasks.
    shared: Arc<SharedChannelState>,

    /// Transaction mode.
    tx_mode: bool,
    /// Buffered publishes during transaction.
    tx_publishes: Vec<TxPublish>,
    /// Buffered acks during transaction.
    tx_acks: Vec<TxAck>,
}

#[allow(dead_code)]
struct TxPublish {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    message: StoredMessage,
}

#[derive(Clone)]
struct TxAck {
    delivery_tag: u64,
    multiple: bool,
    nack: bool,
    requeue: bool,
}

struct PublishState {
    exchange: String,
    routing_key: String,
    mandatory: bool,
    properties: Option<BasicProperties>,
    body_size: u64,
    body: Vec<u8>,
}

impl ServerChannel {
    pub fn new(id: u16, vhost: Arc<VHost>, tx: FrameSender) -> Self {
        Self {
            id,
            vhost,
            tx,
            next_delivery_tag: Arc::new(AtomicU64::new(0)),
            consumers: HashMap::new(),
            confirm_mode: false,
            confirm_counter: 0,
            publish_state: None,
            prefetch_count: 0,
            global_prefetch_count: 0,
            shared: Arc::new(SharedChannelState {
                unacked_count: AtomicU64::new(0),
                ack_notify: Notify::new(),
                unacked: parking_lot::Mutex::new(BTreeMap::new()),
                prefetch_count: AtomicU16::new(0),
                global_prefetch_count: AtomicU16::new(0),
                flow_active: std::sync::atomic::AtomicBool::new(true),
                flow_notify: Notify::new(),
            }),
            tx_mode: false,
            tx_publishes: Vec::new(),
            tx_acks: Vec::new(),
        }
    }

    pub fn process_frame(&mut self, frame: AMQPFrame) -> Result<(), ChannelError> {
        match frame.payload {
            FramePayload::Method(method) => self.handle_method(method),
            FramePayload::Header(header) => self.handle_header(header),
            FramePayload::Body(body) => self.handle_body(body),
            FramePayload::Heartbeat => Ok(()), // shouldn't arrive on a channel
        }
    }

    fn handle_method(&mut self, method: MethodFrame) -> Result<(), ChannelError> {
        match method {
            MethodFrame::ExchangeDeclare(declare) => {
                if declare.passive {
                    if self.vhost.exchange_exists(&declare.exchange) {
                        self.send(MethodFrame::ExchangeDeclareOk)?;
                    } else {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &format!("no exchange '{}'", declare.exchange),
                            CLASS_EXCHANGE,
                            METHOD_EXCHANGE_DECLARE,
                        )?;
                    }
                    return Ok(());
                }

                let config = ExchangeConfig {
                    name: declare.exchange,
                    exchange_type: declare.exchange_type,
                    durable: declare.durable,
                    auto_delete: declare.auto_delete,
                    internal: declare.internal,
                    arguments: declare.arguments,
                };

                match self.vhost.declare_exchange(config) {
                    Ok(_) => {
                        if !declare.no_wait {
                            self.send(MethodFrame::ExchangeDeclareOk)?;
                        }
                    }
                    Err(VHostError::PreconditionFailed(msg)) => {
                        self.close_channel(
                            ReplyCode::PreconditionFailed as u16,
                            &msg,
                            CLASS_EXCHANGE,
                            METHOD_EXCHANGE_DECLARE,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::ExchangeDelete(delete) => {
                match self.vhost.delete_exchange(&delete.exchange) {
                    Ok(()) => {
                        if !delete.no_wait {
                            self.send(MethodFrame::ExchangeDeleteOk)?;
                        }
                    }
                    Err(VHostError::AccessRefused(msg)) => {
                        self.close_channel(
                            ReplyCode::AccessRefused as u16,
                            &msg,
                            CLASS_EXCHANGE,
                            METHOD_EXCHANGE_DELETE,
                        )?;
                    }
                    Err(VHostError::NotFound(msg)) => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &msg,
                            CLASS_EXCHANGE,
                            METHOD_EXCHANGE_DELETE,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::QueueDeclare(declare) => {
                if declare.passive {
                    if let Some(queue) = self.vhost.get_queue(&declare.queue) {
                        self.send(MethodFrame::QueueDeclareOk(QueueDeclareOk {
                            queue: queue.name().to_string(),
                            message_count: queue.message_count() as u32,
                            consumer_count: queue.consumer_count() as u32,
                        }))?;
                    } else {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &format!("no queue '{}'", declare.queue),
                            CLASS_QUEUE,
                            METHOD_QUEUE_DECLARE,
                        )?;
                    }
                    return Ok(());
                }

                let queue_name = if declare.queue.is_empty() {
                    // Auto-generate queue name
                    format!("amq.gen-{:016x}", rand_u64())
                } else {
                    declare.queue
                };

                let config = QueueConfig {
                    name: queue_name.clone(),
                    durable: declare.durable,
                    exclusive: declare.exclusive,
                    auto_delete: declare.auto_delete,
                    arguments: declare.arguments,
                };

                match self.vhost.declare_queue(config) {
                    Ok(queue) => {
                        if !declare.no_wait {
                            self.send(MethodFrame::QueueDeclareOk(QueueDeclareOk {
                                queue: queue.name().to_string(),
                                message_count: queue.message_count() as u32,
                                consumer_count: queue.consumer_count() as u32,
                            }))?;
                        }
                    }
                    Err(VHostError::PreconditionFailed(msg)) => {
                        self.close_channel(
                            ReplyCode::PreconditionFailed as u16,
                            &msg,
                            CLASS_QUEUE,
                            METHOD_QUEUE_DECLARE,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::QueueBind(bind) => {
                match self.vhost.bind_queue(
                    &bind.queue,
                    &bind.exchange,
                    &bind.routing_key,
                    &bind.arguments,
                ) {
                    Ok(()) => {
                        if !bind.no_wait {
                            self.send(MethodFrame::QueueBindOk)?;
                        }
                    }
                    Err(VHostError::NotFound(msg)) => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &msg,
                            CLASS_QUEUE,
                            METHOD_QUEUE_BIND,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::QueueUnbind(unbind) => {
                match self.vhost.unbind_queue(
                    &unbind.queue,
                    &unbind.exchange,
                    &unbind.routing_key,
                    &unbind.arguments,
                ) {
                    Ok(()) => self.send(MethodFrame::QueueUnbindOk)?,
                    Err(VHostError::NotFound(msg)) => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &msg,
                            CLASS_QUEUE,
                            METHOD_QUEUE_UNBIND,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::QueueDelete(delete) => {
                match self.vhost.delete_queue(&delete.queue) {
                    Ok(count) => {
                        if !delete.no_wait {
                            self.send(MethodFrame::QueueDeleteOk(count))?;
                        }
                    }
                    Err(VHostError::NotFound(msg)) => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &msg,
                            CLASS_QUEUE,
                            METHOD_QUEUE_DELETE,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::QueuePurge(purge) => {
                if let Some(queue) = self.vhost.get_queue(&purge.queue) {
                    let count = queue
                        .purge()
                        .map_err(|e| ChannelError::Storage(e.to_string()))?;
                    if !purge.no_wait {
                        self.send(MethodFrame::QueuePurgeOk(count))?;
                    }
                } else {
                    self.close_channel(
                        ReplyCode::NotFound as u16,
                        &format!("no queue '{}'", purge.queue),
                        CLASS_QUEUE,
                        METHOD_QUEUE_PURGE,
                    )?;
                }
                Ok(())
            }

            MethodFrame::BasicPublish(publish) => {
                self.publish_state = Some(PublishState {
                    exchange: publish.exchange,
                    routing_key: publish.routing_key,
                    mandatory: publish.mandatory,
                    properties: None,
                    body_size: 0,
                    body: Vec::new(),
                });
                Ok(())
            }

            MethodFrame::BasicConsume(consume) => {
                let queue_name = consume.queue.clone();
                let consumer_tag = if consume.consumer_tag.is_empty() {
                    format!("ctag-{:016x}", rand_u64())
                } else {
                    consume.consumer_tag
                };
                let no_ack = consume.no_ack;

                let queue = match self.vhost.get_queue(&queue_name) {
                    Some(q) => q,
                    None => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &format!("no queue '{queue_name}'"),
                            CLASS_BASIC,
                            METHOD_BASIC_CONSUME,
                        )?;
                        return Ok(());
                    }
                };

                queue.add_consumer();

                // Send ConsumeOk
                if !consume.no_wait {
                    self.send(MethodFrame::BasicConsumeOk(consumer_tag.clone()))?;
                }

                // Spawn delivery task
                let tx = self.tx.clone();
                let channel_id = self.id;
                let tag = consumer_tag.clone();
                let tag_for_task = tag.clone();
                let next_tag = self.next_delivery_tag.clone();
                let shared = self.shared.clone();
                let queue_name_for_task = queue_name.clone();
                let vhost_for_task = self.vhost.clone();

                let task = tokio::spawn(async move {
                    let tag = tag_for_task;
                    let queue_name = queue_name_for_task;
                    let mut consumer_unacked: u64 = 0;
                    loop {
                        // Check channel flow — pause if flow is off
                        while !shared.flow_active.load(Ordering::Relaxed) {
                            shared.flow_notify.notified().await;
                        }

                        // Wait for prefetch capacity (read from shared atomics)
                        if !no_ack {
                            loop {
                                let prefetch = shared.prefetch_count.load(Ordering::Relaxed);
                                let global_prefetch =
                                    shared.global_prefetch_count.load(Ordering::Relaxed);
                                let global_unacked = shared.unacked_count.load(Ordering::Relaxed);
                                let has_capacity = (prefetch == 0
                                    || consumer_unacked < prefetch as u64)
                                    && (global_prefetch == 0
                                        || global_unacked < global_prefetch as u64);
                                if has_capacity {
                                    break;
                                }
                                shared.ack_notify.notified().await;
                                // Recount consumer unacked from shared map
                                consumer_unacked = shared
                                    .unacked
                                    .lock()
                                    .values()
                                    .filter(|u| u.queue_name == queue_name)
                                    .count()
                                    as u64;
                            }
                        }

                        // Determine batch size based on available capacity
                        let prefetch = shared.prefetch_count.load(Ordering::Relaxed);
                        let batch_size = if prefetch > 0 {
                            ((prefetch as u64).saturating_sub(consumer_unacked)) as usize
                        } else {
                            256 // default batch when no prefetch limit
                        }
                        .max(1);

                        match queue.shift_batch(batch_size) {
                            Ok((envelopes, dead_letters)) => {
                                for dl in dead_letters {
                                    let _ = vhost_for_task.publish(
                                        &dl.exchange,
                                        &dl.routing_key,
                                        &dl.message,
                                    );
                                }

                                if envelopes.is_empty() {
                                    // Flush channel-delivered positions to the store so
                                    // the next shift_batch can scan efficiently
                                    let _ = queue.flush_channel_delivered();

                                    // Brief wait, then retry (the flush may have unblocked
                                    // store messages)
                                    tokio::select! {
                                        biased;
                                        _ = queue.wait_for_message() => {}
                                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(1)) => {}
                                    }
                                    continue;
                                }

                                let mut no_ack_positions = Vec::new();

                                for env in envelopes {
                                    let delivery_tag = next_tag.fetch_add(1, Ordering::Relaxed) + 1;
                                    let sp = env.segment_position;
                                    let redelivered = env.redelivered;
                                    let msg = &env.message;
                                    let body_len = msg.body.len() as u64;

                                    let deliver = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Method(MethodFrame::BasicDeliver(
                                            BasicDeliver {
                                                consumer_tag: tag.clone(),
                                                delivery_tag,
                                                redelivered,
                                                exchange: msg.exchange.clone(),
                                                routing_key: msg.routing_key.clone(),
                                            },
                                        )),
                                    };

                                    let header = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Header(ContentHeader {
                                            class_id: CLASS_BASIC,
                                            body_size: body_len,
                                            properties: msg.properties.clone(),
                                        }),
                                    };

                                    let body = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Body(msg.body.clone()),
                                    };

                                    if tx.send(deliver).await.is_err()
                                        || tx.send(header).await.is_err()
                                        || tx.send(body).await.is_err()
                                    {
                                        return;
                                    }

                                    if no_ack {
                                        no_ack_positions.push(sp);
                                    } else {
                                        shared.unacked.lock().insert(
                                            delivery_tag,
                                            Unacked {
                                                delivery_tag,
                                                queue_name: queue_name.clone(),
                                                segment_position: sp,
                                            },
                                        );
                                        consumer_unacked += 1;
                                        shared.unacked_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }

                                // Batch ack for no_ack mode
                                if !no_ack_positions.is_empty() {
                                    let _ = queue.ack_batch(&no_ack_positions);
                                }
                            }
                            Err(e) => {
                                warn!("delivery error: {e}");
                                break;
                            }
                        }
                    }
                });

                self.consumers.insert(
                    consumer_tag,
                    Consumer {
                        tag: tag.clone(),
                        queue_name,
                        no_ack,
                        task: Some(task),
                    },
                );

                Ok(())
            }

            MethodFrame::BasicCancel(cancel) => {
                if let Some(mut consumer) = self.consumers.remove(&cancel.consumer_tag) {
                    if let Some(task) = consumer.task.take() {
                        task.abort();
                    }
                    if let Some(queue) = self.vhost.get_queue(&consumer.queue_name) {
                        queue.remove_consumer();
                    }
                }
                if !cancel.no_wait {
                    self.send(MethodFrame::BasicCancelOk(cancel.consumer_tag))?;
                }
                Ok(())
            }

            MethodFrame::BasicAck(ack) => {
                if self.tx_mode {
                    self.tx_acks.push(TxAck {
                        delivery_tag: ack.delivery_tag,
                        multiple: ack.multiple,
                        nack: false,
                        requeue: false,
                    });
                    return Ok(());
                }
                let mut unacked = self.shared.unacked.lock();
                if ack.multiple {
                    // Split the map: keys <= delivery_tag go into to_ack
                    let remaining = unacked.split_off(&(ack.delivery_tag + 1));
                    let to_ack: BTreeMap<u64, Unacked> =
                        std::mem::replace(&mut *unacked, remaining);

                    let acked_count = to_ack.len() as u64;
                    for entry in to_ack.values() {
                        if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                            queue
                                .ack(&entry.segment_position)
                                .map_err(|e| ChannelError::Storage(e.to_string()))?;
                        }
                    }
                    drop(unacked);
                    if acked_count > 0 {
                        self.shared
                            .unacked_count
                            .fetch_sub(acked_count, Ordering::Relaxed);
                        self.shared.ack_notify.notify_waiters();
                    }
                } else if let Some(entry) = unacked.remove(&ack.delivery_tag) {
                    drop(unacked);
                    if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                        queue
                            .ack(&entry.segment_position)
                            .map_err(|e| ChannelError::Storage(e.to_string()))?;
                    }
                    self.shared.unacked_count.fetch_sub(1, Ordering::Relaxed);
                    self.shared.ack_notify.notify_waiters();
                }
                Ok(())
            }

            MethodFrame::BasicReject(reject) => {
                let mut unacked = self.shared.unacked.lock();
                if let Some(entry) = unacked.remove(&reject.delivery_tag) {
                    drop(unacked);
                    if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                        if reject.requeue {
                            queue.requeue(entry.segment_position);
                        } else {
                            queue
                                .ack(&entry.segment_position)
                                .map_err(|e| ChannelError::Storage(e.to_string()))?;
                        }
                    }
                    self.shared.unacked_count.fetch_sub(1, Ordering::Relaxed);
                    self.shared.ack_notify.notify_waiters();
                }
                Ok(())
            }

            MethodFrame::BasicNack(nack) => {
                let mut unacked = self.shared.unacked.lock();
                let to_process: Vec<(String, SegmentPosition)> = if nack.multiple {
                    let remaining = unacked.split_off(&(nack.delivery_tag + 1));
                    let removed = std::mem::replace(&mut *unacked, remaining);
                    removed
                        .into_values()
                        .map(|u| (u.queue_name, u.segment_position))
                        .collect()
                } else if let Some(u) = unacked.remove(&nack.delivery_tag) {
                    vec![(u.queue_name, u.segment_position)]
                } else {
                    vec![]
                };
                drop(unacked);

                let nacked_count = to_process.len() as u64;
                for (queue_name, sp) in to_process {
                    if let Some(queue) = self.vhost.get_queue(&queue_name) {
                        if nack.requeue {
                            queue.requeue(sp);
                        } else {
                            queue
                                .ack(&sp)
                                .map_err(|e| ChannelError::Storage(e.to_string()))?;
                        }
                    }
                }
                if nacked_count > 0 {
                    self.shared
                        .unacked_count
                        .fetch_sub(nacked_count, Ordering::Relaxed);
                    self.shared.ack_notify.notify_waiters();
                }
                Ok(())
            }

            MethodFrame::BasicGet(get) => {
                let queue = match self.vhost.get_queue(&get.queue) {
                    Some(q) => q,
                    None => {
                        self.close_channel(
                            ReplyCode::NotFound as u16,
                            &format!("no queue '{}'", get.queue),
                            CLASS_BASIC,
                            METHOD_BASIC_GET,
                        )?;
                        return Ok(());
                    }
                };

                match queue.shift() {
                    Ok((Some(env), dead_letters)) => {
                        for dl in dead_letters {
                            let _ = self
                                .vhost
                                .publish(&dl.exchange, &dl.routing_key, &dl.message);
                        }

                        let delivery_tag =
                            self.next_delivery_tag.fetch_add(1, Ordering::Relaxed) + 1;
                        let sp = env.segment_position;
                        let msg = &env.message;
                        let body_len = msg.body.len() as u64;

                        if !get.no_ack {
                            self.shared.unacked.lock().insert(
                                delivery_tag,
                                Unacked {
                                    delivery_tag,
                                    queue_name: get.queue.clone(),
                                    segment_position: sp,
                                },
                            );
                            self.shared.unacked_count.fetch_add(1, Ordering::Relaxed);
                        }

                        let get_ok = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Method(MethodFrame::BasicGetOk(BasicGetOk {
                                delivery_tag,
                                redelivered: env.redelivered,
                                exchange: msg.exchange.clone(),
                                routing_key: msg.routing_key.clone(),
                                message_count: queue.message_count() as u32,
                            })),
                        };
                        self.tx
                            .try_send(get_ok)
                            .map_err(|_| ChannelError::SendFailed)?;

                        let header = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Header(ContentHeader {
                                class_id: CLASS_BASIC,
                                body_size: body_len,
                                properties: msg.properties.clone(),
                            }),
                        };
                        self.tx
                            .try_send(header)
                            .map_err(|_| ChannelError::SendFailed)?;

                        let body = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Body(msg.body.clone()),
                        };
                        self.tx
                            .try_send(body)
                            .map_err(|_| ChannelError::SendFailed)?;

                        if get.no_ack {
                            queue
                                .ack(&sp)
                                .map_err(|e| ChannelError::Storage(e.to_string()))?;
                        }
                    }
                    Ok((None, dead_letters)) => {
                        for dl in dead_letters {
                            let _ = self
                                .vhost
                                .publish(&dl.exchange, &dl.routing_key, &dl.message);
                        }
                        self.send(MethodFrame::BasicGetEmpty)?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            }

            MethodFrame::BasicQos(qos) => {
                if qos.global {
                    self.global_prefetch_count = qos.prefetch_count;
                    self.shared
                        .global_prefetch_count
                        .store(qos.prefetch_count, Ordering::Relaxed);
                } else {
                    self.prefetch_count = qos.prefetch_count;
                    self.shared
                        .prefetch_count
                        .store(qos.prefetch_count, Ordering::Relaxed);
                }
                debug!(
                    "basic.qos: prefetch_count={}, global={} -> channel prefetch={}, global={}",
                    qos.prefetch_count, qos.global, self.prefetch_count, self.global_prefetch_count
                );
                // Notify delivery tasks in case prefetch was increased
                self.shared.ack_notify.notify_waiters();
                self.send(MethodFrame::BasicQosOk)?;
                Ok(())
            }

            MethodFrame::ConfirmSelect(no_wait) => {
                self.confirm_mode = true;
                self.confirm_counter = 0;
                if !no_wait {
                    self.send(MethodFrame::ConfirmSelectOk)?;
                }
                Ok(())
            }

            MethodFrame::TxSelect => {
                if self.confirm_mode {
                    self.close_channel(
                        ReplyCode::PreconditionFailed as u16,
                        "cannot use tx with publisher confirms",
                        CLASS_TX,
                        METHOD_TX_SELECT,
                    )?;
                    return Ok(());
                }
                self.tx_mode = true;
                self.send(MethodFrame::TxSelectOk)?;
                Ok(())
            }
            MethodFrame::TxCommit => {
                if !self.tx_mode {
                    self.close_channel(
                        ReplyCode::PreconditionFailed as u16,
                        "tx not selected",
                        CLASS_TX,
                        METHOD_TX_COMMIT,
                    )?;
                    return Ok(());
                }

                // Apply buffered publishes — collect errors
                let publishes = std::mem::take(&mut self.tx_publishes);
                let mut tx_errors: Vec<String> = Vec::new();
                for tx_pub in publishes {
                    if let Err(e) =
                        self.vhost
                            .publish(&tx_pub.exchange, &tx_pub.routing_key, &tx_pub.message)
                    {
                        tx_errors.push(format!("publish to '{}': {e}", tx_pub.exchange));
                    }
                }

                // Apply buffered acks — collect errors
                let acks = std::mem::take(&mut self.tx_acks);
                for tx_ack in acks {
                    let mut unacked = self.shared.unacked.lock();
                    let to_process: Vec<Unacked> = if tx_ack.multiple {
                        let remaining = unacked.split_off(&(tx_ack.delivery_tag + 1));
                        let removed = std::mem::replace(&mut *unacked, remaining);
                        removed.into_values().collect()
                    } else if let Some(entry) = unacked.remove(&tx_ack.delivery_tag) {
                        vec![entry]
                    } else {
                        vec![]
                    };
                    drop(unacked);

                    let count = to_process.len() as u64;
                    for entry in to_process {
                        if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                            if tx_ack.nack && tx_ack.requeue {
                                queue.requeue(entry.segment_position);
                            } else if let Err(e) = queue.ack(&entry.segment_position) {
                                tx_errors.push(format!("ack in '{}': {e}", entry.queue_name));
                            }
                        }
                    }
                    if count > 0 {
                        self.shared
                            .unacked_count
                            .fetch_sub(count, Ordering::Relaxed);
                        self.shared.ack_notify.notify_waiters();
                    }
                }

                // If any operation failed, close the channel with an error
                if !tx_errors.is_empty() {
                    let msg = format!("tx.commit failed: {}", tx_errors.join("; "));
                    self.close_channel(
                        ReplyCode::InternalError as u16,
                        &msg,
                        CLASS_TX,
                        METHOD_TX_COMMIT,
                    )?;
                    return Ok(());
                }

                self.send(MethodFrame::TxCommitOk)?;
                Ok(())
            }
            MethodFrame::TxRollback => {
                if !self.tx_mode {
                    self.close_channel(
                        ReplyCode::PreconditionFailed as u16,
                        "tx not selected",
                        CLASS_TX,
                        METHOD_TX_ROLLBACK,
                    )?;
                    return Ok(());
                }
                self.tx_publishes.clear();
                self.tx_acks.clear();
                self.send(MethodFrame::TxRollbackOk)?;
                Ok(())
            }

            MethodFrame::ChannelFlow(active) => {
                self.shared.flow_active.store(active, Ordering::Relaxed);
                if active {
                    // Wake delivery tasks that may be paused
                    self.shared.flow_notify.notify_waiters();
                }
                debug!("channel.flow active={active} on channel {}", self.id);
                self.send(MethodFrame::ChannelFlowOk(active))?;
                Ok(())
            }

            MethodFrame::BasicRecover(requeue) | MethodFrame::BasicRecoverAsync(requeue) => {
                if requeue {
                    let mut unacked = self.shared.unacked.lock();
                    let entries: Vec<Unacked> =
                        std::mem::take(&mut *unacked).into_values().collect();
                    let count = entries.len() as u64;
                    drop(unacked);

                    for entry in entries {
                        if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                            queue.requeue(entry.segment_position);
                        }
                    }
                    if count > 0 {
                        self.shared
                            .unacked_count
                            .fetch_sub(count, Ordering::Relaxed);
                        self.shared.ack_notify.notify_waiters();
                    }
                }
                self.send(MethodFrame::BasicRecoverOk)?;
                Ok(())
            }

            other => {
                warn!("unhandled method on channel {}: {:?}", self.id, other);
                Ok(())
            }
        }
    }

    fn handle_header(&mut self, header: ContentHeader) -> Result<(), ChannelError> {
        if let Some(ref mut state) = self.publish_state {
            // Validate message size to prevent memory exhaustion
            if header.body_size > MAX_MESSAGE_SIZE {
                self.publish_state = None;
                self.close_channel(
                    ReplyCode::PreconditionFailed as u16,
                    &format!(
                        "message size {} exceeds maximum {}",
                        header.body_size, MAX_MESSAGE_SIZE
                    ),
                    CLASS_BASIC,
                    METHOD_BASIC_PUBLISH,
                )?;
                return Ok(());
            }

            state.properties = Some(header.properties);
            state.body_size = header.body_size;
            state.body = Vec::with_capacity(header.body_size as usize);

            // If body_size is 0, finish publish immediately
            if header.body_size == 0 {
                self.finish_publish()?;
            }
        }
        Ok(())
    }

    fn handle_body(&mut self, body: Bytes) -> Result<(), ChannelError> {
        let should_finish = if let Some(ref mut state) = self.publish_state {
            state.body.extend_from_slice(&body);
            state.body.len() as u64 >= state.body_size
        } else {
            false
        };

        if should_finish {
            self.finish_publish()?;
        }
        Ok(())
    }

    fn finish_publish(&mut self) -> Result<(), ChannelError> {
        let state = self
            .publish_state
            .take()
            .ok_or_else(|| ChannelError::Protocol("no publish in progress".into()))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let properties = state.properties.unwrap_or_default();

        let msg = StoredMessage {
            timestamp,
            exchange: state.exchange.clone(),
            routing_key: state.routing_key.clone(),
            properties,
            body: Bytes::from(state.body),
        };

        // In transaction mode, buffer the publish
        if self.tx_mode {
            self.tx_publishes.push(TxPublish {
                exchange: state.exchange,
                routing_key: state.routing_key,
                mandatory: state.mandatory,
                message: msg,
            });
            return Ok(());
        }

        let result = self
            .vhost
            .publish(&state.exchange, &state.routing_key, &msg);

        if self.confirm_mode {
            self.confirm_counter += 1;
            match result {
                Ok(_count) => {
                    self.send(MethodFrame::BasicAck(BasicAck {
                        delivery_tag: self.confirm_counter,
                        multiple: false,
                    }))?;
                }
                Err(_) => {
                    self.send(MethodFrame::BasicNack(BasicNack {
                        delivery_tag: self.confirm_counter,
                        multiple: false,
                        requeue: false,
                    }))?;
                }
            }
        } else if let Err(_) = result {
            // If mandatory and no routes, send Basic.Return
            if state.mandatory {
                let ret = MethodFrame::BasicReturn(BasicReturn {
                    reply_code: 312,
                    reply_text: "NO_ROUTE".into(),
                    exchange: state.exchange,
                    routing_key: state.routing_key,
                });
                self.send(ret)?;
            }
        }

        Ok(())
    }

    fn send(&self, method: MethodFrame) -> Result<(), ChannelError> {
        let frame = AMQPFrame {
            channel: self.id,
            payload: FramePayload::Method(method),
        };
        self.tx
            .try_send(frame)
            .map_err(|_| ChannelError::SendFailed)?;
        Ok(())
    }

    fn close_channel(
        &self,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ChannelError> {
        self.send(MethodFrame::ChannelClose(ChannelClose {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        }))
    }
}

impl Drop for ServerChannel {
    fn drop(&mut self) {
        // Requeue all unacked messages so they become available to other consumers.
        // This is critical for at-least-once delivery guarantee.
        let unacked: Vec<Unacked> = {
            let mut map = self.shared.unacked.lock();
            std::mem::take(&mut *map).into_values().collect()
        };
        for entry in unacked {
            if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                queue.requeue(entry.segment_position);
            }
        }

        // Cancel all consumers
        for (_, mut consumer) in self.consumers.drain() {
            if let Some(task) = consumer.task.take() {
                task.abort();
            }
            if let Some(queue) = self.vhost.get_queue(&consumer.queue_name) {
                queue.remove_consumer();
            }
        }
    }
}

fn rand_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmq_protocol::properties::BasicProperties;

    /// Create a ServerChannel with a dummy vhost for testing.
    fn make_test_channel() -> (ServerChannel, tokio::sync::mpsc::Receiver<AMQPFrame>) {
        let dir = tempfile::TempDir::new().unwrap();
        let vhost = Arc::new(VHost::new("/".into(), dir.path()).unwrap());
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let ch = ServerChannel::new(1, vhost, tx);
        (ch, rx)
    }

    #[test]
    fn test_message_size_cap_rejects_oversized_message() {
        let (mut ch, mut rx) = make_test_channel();

        // Start a publish
        let publish = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicPublish(BasicPublish {
                exchange: String::new(),
                routing_key: "test".into(),
                mandatory: false,
                immediate: false,
            })),
        };
        ch.process_frame(publish).unwrap();

        // Send a content header claiming a body larger than MAX_MESSAGE_SIZE
        let oversized_header = AMQPFrame {
            channel: 1,
            payload: FramePayload::Header(ContentHeader {
                class_id: CLASS_BASIC,
                body_size: MAX_MESSAGE_SIZE + 1,
                properties: BasicProperties::default(),
            }),
        };
        ch.process_frame(oversized_header).unwrap();

        // The channel should have sent a ChannelClose frame
        let frame = rx.try_recv().unwrap();
        match frame.payload {
            FramePayload::Method(MethodFrame::ChannelClose(close)) => {
                assert_eq!(close.reply_code, ReplyCode::PreconditionFailed as u16);
                assert!(
                    close.reply_text.contains("exceeds maximum"),
                    "unexpected close text: {}",
                    close.reply_text
                );
            }
            other => panic!("expected ChannelClose, got {:?}", other),
        }

        // Publish state should be cleared
        assert!(ch.publish_state.is_none());
    }

    #[test]
    fn test_message_size_cap_allows_valid_message() {
        let (mut ch, _rx) = make_test_channel();

        // Start a publish
        let publish = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicPublish(BasicPublish {
                exchange: String::new(),
                routing_key: "test".into(),
                mandatory: false,
                immediate: false,
            })),
        };
        ch.process_frame(publish).unwrap();

        // Send a content header with a small body size
        let small_header = AMQPFrame {
            channel: 1,
            payload: FramePayload::Header(ContentHeader {
                class_id: CLASS_BASIC,
                body_size: 1024,
                properties: BasicProperties::default(),
            }),
        };
        // Should not error — publish state should still be active (waiting for body)
        ch.process_frame(small_header).unwrap();
        assert!(ch.publish_state.is_some());
    }

    #[test]
    fn test_message_size_cap_allows_exactly_max() {
        let (mut ch, _rx) = make_test_channel();

        let publish = AMQPFrame {
            channel: 1,
            payload: FramePayload::Method(MethodFrame::BasicPublish(BasicPublish {
                exchange: String::new(),
                routing_key: "test".into(),
                mandatory: false,
                immediate: false,
            })),
        };
        ch.process_frame(publish).unwrap();

        // Exactly MAX_MESSAGE_SIZE should be allowed
        let header = AMQPFrame {
            channel: 1,
            payload: FramePayload::Header(ContentHeader {
                class_id: CLASS_BASIC,
                body_size: MAX_MESSAGE_SIZE,
                properties: BasicProperties::default(),
            }),
        };
        ch.process_frame(header).unwrap();
        assert!(ch.publish_state.is_some());
    }
}
