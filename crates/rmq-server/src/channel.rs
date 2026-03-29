use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

/// Unacked message tracking.
#[derive(Clone)]
struct Unacked {
    delivery_tag: u64,
    queue_name: String,
    segment_position: SegmentPosition,
}

/// Consumer state.
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
    /// Shared unacked message list — delivery tasks add, ack handler removes.
    unacked: parking_lot::Mutex<Vec<Unacked>>,
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
                unacked: parking_lot::Mutex::new(Vec::new()),
            }),
            tx_mode: false,
            tx_publishes: Vec::new(),
            tx_acks: Vec::new(),
        }
    }

    pub fn process_frame(
        &mut self,
        frame: AMQPFrame,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match frame.payload {
            FramePayload::Method(method) => self.handle_method(method),
            FramePayload::Header(header) => self.handle_header(header),
            FramePayload::Body(body) => self.handle_body(body),
            FramePayload::Heartbeat => Ok(()), // shouldn't arrive on a channel
        }
    }

    fn handle_method(&mut self, method: MethodFrame) -> Result<(), Box<dyn std::error::Error>> {
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
                    let count = queue.purge().map_err(|e| e.to_string())?;
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
                let prefetch = self.prefetch_count;
                let global_prefetch = self.global_prefetch_count;
                let shared = self.shared.clone();
                let queue_name_for_task = queue_name.clone();
                let vhost_for_task = self.vhost.clone();

                let task = tokio::spawn(async move {
                    let tag = tag_for_task;
                    let queue_name = queue_name_for_task;
                    let mut consumer_unacked: u64 = 0;
                    loop {
                        // Wait for prefetch capacity
                        if !no_ack {
                            loop {
                                let global_unacked = shared.unacked_count.load(Ordering::Relaxed);
                                let has_capacity = (prefetch == 0 || consumer_unacked < prefetch as u64)
                                    && (global_prefetch == 0 || global_unacked < global_prefetch as u64);
                                if has_capacity {
                                    break;
                                }
                                shared.ack_notify.notified().await;
                                // Recount consumer unacked from shared list
                                consumer_unacked = shared.unacked.lock().iter()
                                    .filter(|u| u.queue_name == queue_name)
                                    .count() as u64;
                            }
                        }

                        // Determine batch size based on available capacity
                        let batch_size = if prefetch > 0 {
                            ((prefetch as u64).saturating_sub(consumer_unacked)) as usize
                        } else {
                            256 // default batch when no prefetch limit
                        }
                        .max(1);

                        match queue.shift_batch(batch_size) {
                            Ok((envelopes, dead_letters)) => {
                                for dl in dead_letters {
                                    let _ = vhost_for_task.publish(&dl.exchange, &dl.routing_key, &dl.message);
                                }

                                if envelopes.is_empty() {
                                    // Wait briefly for new messages. Use a short timeout
                                    // so we periodically retry the store (which may have
                                    // messages from overflow or requeue).
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
                                    let msg = env.message;
                                    let body_len = msg.body.len() as u64;

                                    let deliver = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Method(MethodFrame::BasicDeliver(
                                            BasicDeliver {
                                                consumer_tag: tag.clone(),
                                                delivery_tag,
                                                redelivered,
                                                exchange: msg.exchange,
                                                routing_key: msg.routing_key,
                                            },
                                        )),
                                    };

                                    let header = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Header(ContentHeader {
                                            class_id: CLASS_BASIC,
                                            body_size: body_len,
                                            properties: msg.properties,
                                        }),
                                    };

                                    let body = AMQPFrame {
                                        channel: channel_id,
                                        payload: FramePayload::Body(msg.body),
                                    };

                                    if tx.send(deliver).is_err()
                                        || tx.send(header).is_err()
                                        || tx.send(body).is_err()
                                    {
                                        return;
                                    }

                                    if no_ack {
                                        no_ack_positions.push(sp);
                                    } else {
                                        shared.unacked.lock().push(Unacked {
                                            delivery_tag,
                                            queue_name: queue_name.clone(),
                                            segment_position: sp,
                                        });
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
                    let to_ack: Vec<_> = unacked
                        .iter()
                        .filter(|u| u.delivery_tag <= ack.delivery_tag)
                        .map(|u| (u.queue_name.clone(), u.segment_position))
                        .collect();

                    let acked_count = to_ack.len() as u64;
                    for (queue_name, sp) in &to_ack {
                        if let Some(queue) = self.vhost.get_queue(queue_name) {
                            queue.ack(sp).map_err(|e| e.to_string())?;
                        }
                    }
                    unacked.retain(|u| u.delivery_tag > ack.delivery_tag);
                    drop(unacked);
                    if acked_count > 0 {
                        self.shared.unacked_count.fetch_sub(acked_count, Ordering::Relaxed);
                        self.shared.ack_notify.notify_waiters();
                    }
                } else if let Some(pos) = unacked
                    .iter()
                    .position(|u| u.delivery_tag == ack.delivery_tag)
                {
                    let entry = unacked.remove(pos);
                    drop(unacked);
                    if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                        queue.ack(&entry.segment_position).map_err(|e| e.to_string())?;
                    }
                    self.shared.unacked_count.fetch_sub(1, Ordering::Relaxed);
                    self.shared.ack_notify.notify_waiters();
                }
                Ok(())
            }

            MethodFrame::BasicReject(reject) => {
                let mut unacked = self.shared.unacked.lock();
                if let Some(pos) = unacked
                    .iter()
                    .position(|u| u.delivery_tag == reject.delivery_tag)
                {
                    let entry = unacked.remove(pos);
                    drop(unacked);
                    if let Some(queue) = self.vhost.get_queue(&entry.queue_name) {
                        if reject.requeue {
                            queue.requeue(entry.segment_position);
                        } else {
                            queue.ack(&entry.segment_position).map_err(|e| e.to_string())?;
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
                    let items: Vec<_> = unacked
                        .iter()
                        .filter(|u| u.delivery_tag <= nack.delivery_tag)
                        .map(|u| (u.queue_name.clone(), u.segment_position))
                        .collect();
                    unacked.retain(|u| u.delivery_tag > nack.delivery_tag);
                    items
                } else if let Some(pos) = unacked
                    .iter()
                    .position(|u| u.delivery_tag == nack.delivery_tag)
                {
                    let u = unacked.remove(pos);
                    vec![(u.queue_name.clone(), u.segment_position)]
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
                            queue.ack(&sp).map_err(|e| e.to_string())?;
                        }
                    }
                }
                if nacked_count > 0 {
                    self.shared.unacked_count.fetch_sub(nacked_count, Ordering::Relaxed);
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
                            let _ = self.vhost.publish(&dl.exchange, &dl.routing_key, &dl.message);
                        }

                        let delivery_tag = self.next_delivery_tag.fetch_add(1, Ordering::Relaxed) + 1;
                        let sp = env.segment_position;
                        let msg = env.message;
                        let body_len = msg.body.len() as u64;

                        if !get.no_ack {
                            self.shared.unacked.lock().push(Unacked {
                                delivery_tag,
                                queue_name: get.queue.clone(),
                                segment_position: sp,
                            });
                            self.shared.unacked_count.fetch_add(1, Ordering::Relaxed);
                        }

                        let get_ok = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Method(MethodFrame::BasicGetOk(BasicGetOk {
                                delivery_tag,
                                redelivered: env.redelivered,
                                exchange: msg.exchange,
                                routing_key: msg.routing_key,
                                message_count: queue.message_count() as u32,
                            })),
                        };
                        self.tx.send(get_ok)?;

                        let header = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Header(ContentHeader {
                                class_id: CLASS_BASIC,
                                body_size: body_len,
                                properties: msg.properties,
                            }),
                        };
                        self.tx.send(header)?;

                        let body = AMQPFrame {
                            channel: self.id,
                            payload: FramePayload::Body(msg.body),
                        };
                        self.tx.send(body)?;

                        if get.no_ack {
                            queue.ack(&sp).map_err(|e| e.to_string())?;
                        }
                    }
                    Ok((None, dead_letters)) => {
                        for dl in dead_letters {
                            let _ = self.vhost.publish(&dl.exchange, &dl.routing_key, &dl.message);
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
                } else {
                    self.prefetch_count = qos.prefetch_count;
                }
                debug!(
                    "basic.qos: prefetch_count={}, global={} -> channel prefetch={}, global={}",
                    qos.prefetch_count, qos.global, self.prefetch_count, self.global_prefetch_count
                );
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
                    if let Err(e) = self.vhost.publish(&tx_pub.exchange, &tx_pub.routing_key, &tx_pub.message) {
                        tx_errors.push(format!("publish to '{}': {e}", tx_pub.exchange));
                    }
                }

                // Apply buffered acks — collect errors
                let acks = std::mem::take(&mut self.tx_acks);
                for tx_ack in acks {
                    let mut unacked = self.shared.unacked.lock();
                    let to_process: Vec<Unacked> = if tx_ack.multiple {
                        let items: Vec<_> = unacked.iter()
                            .filter(|u| u.delivery_tag <= tx_ack.delivery_tag)
                            .cloned().collect();
                        unacked.retain(|u| u.delivery_tag > tx_ack.delivery_tag);
                        items
                    } else if let Some(pos) = unacked.iter().position(|u| u.delivery_tag == tx_ack.delivery_tag) {
                        vec![unacked.remove(pos)]
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
                        self.shared.unacked_count.fetch_sub(count, Ordering::Relaxed);
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
                self.send(MethodFrame::ChannelFlowOk(active))?;
                Ok(())
            }

            other => {
                warn!("unhandled method on channel {}: {:?}", self.id, other);
                Ok(())
            }
        }
    }

    fn handle_header(&mut self, header: ContentHeader) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref mut state) = self.publish_state {
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

    fn handle_body(&mut self, body: Bytes) -> Result<(), Box<dyn std::error::Error>> {
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

    fn finish_publish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let state = self
            .publish_state
            .take()
            .ok_or("no publish in progress")?;

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

        let result = self.vhost.publish(&state.exchange, &state.routing_key, &msg);

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

    fn send(&self, method: MethodFrame) -> Result<(), Box<dyn std::error::Error>> {
        let frame = AMQPFrame {
            channel: self.id,
            payload: FramePayload::Method(method),
        };
        self.tx.send(frame)?;
        Ok(())
    }

    fn close_channel(
        &self,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
        let unacked: Vec<Unacked> = self.shared.unacked.lock().drain(..).collect();
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
