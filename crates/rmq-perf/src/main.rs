use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_lite::StreamExt;
use lapin::options::*;
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};

#[derive(Parser)]
#[command(name = "rmq-perf", about = "RMQ performance testing tool")]
struct Cli {
    /// AMQP URI
    #[arg(short, long, default_value = "amqp://guest:guest@127.0.0.1:5672")]
    uri: String,

    /// Warm-up period in seconds (discard measurements during this time)
    #[arg(long, default_value = "0")]
    warmup: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish messages as fast as possible
    Publish {
        /// Number of messages to publish
        #[arg(short = 'n', long, default_value = "100000")]
        count: u64,
        /// Message body size in bytes
        #[arg(short, long, default_value = "128")]
        size: usize,
        /// Number of concurrent publishers
        #[arg(short, long, default_value = "1")]
        publishers: u32,
        /// Exchange to publish to
        #[arg(short, long, default_value = "")]
        exchange: String,
        /// Routing key
        #[arg(short, long, default_value = "perf-queue")]
        routing_key: String,
        /// Enable publisher confirms
        #[arg(long)]
        confirm: bool,
        /// Use persistent delivery mode
        #[arg(long)]
        persistent: bool,
    },
    /// Consume messages
    Consume {
        /// Queue to consume from
        #[arg(short, long, default_value = "perf-queue")]
        queue: String,
        /// Number of concurrent consumers
        #[arg(short, long, default_value = "1")]
        consumers: u32,
        /// Prefetch count per consumer
        #[arg(short, long, default_value = "100")]
        prefetch: u16,
        /// Auto-ack mode (no manual ack)
        #[arg(long)]
        no_ack: bool,
        /// Stop after this many messages (0 = run forever)
        #[arg(short = 'n', long, default_value = "0")]
        count: u64,
        /// Stop after this many seconds (0 = run forever)
        #[arg(short, long, default_value = "10")]
        duration: u64,
    },
    /// Publish and consume simultaneously
    Throughput {
        /// Number of messages to publish
        #[arg(short = 'n', long, default_value = "100000")]
        count: u64,
        /// Message body size in bytes
        #[arg(short, long, default_value = "128")]
        size: usize,
        /// Number of publisher connections
        #[arg(long, default_value = "1")]
        publishers: u32,
        /// Number of consumer connections
        #[arg(long, default_value = "1")]
        consumers: u32,
        /// Prefetch count
        #[arg(short, long, default_value = "100")]
        prefetch: u16,
    },
    /// Native protocol batch throughput test
    Native {
        /// Native protocol address
        #[arg(long, default_value = "127.0.0.1:5680")]
        addr: String,
        /// Total messages
        #[arg(short = 'n', long, default_value = "1000000")]
        count: u64,
        /// Message body size
        #[arg(short, long, default_value = "128")]
        size: usize,
        /// Messages per publish batch
        #[arg(long, default_value = "1000")]
        batch: u32,
        /// Number of parallel connections
        #[arg(long, default_value = "4")]
        connections: u32,
    },
    /// Sharded throughput: N independent queue pairs in parallel
    Sharded {
        /// Total messages across all shards
        #[arg(short = 'n', long, default_value = "1000000")]
        count: u64,
        /// Message body size in bytes
        #[arg(short, long, default_value = "128")]
        size: usize,
        /// Number of queue shards (each gets 1 publisher + 1 consumer)
        #[arg(long, default_value = "8")]
        shards: u32,
        /// Prefetch count per consumer
        #[arg(short, long, default_value = "500")]
        prefetch: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let cli = Cli::parse();
    let warmup = cli.warmup;

    match cli.command {
        Command::Publish {
            count,
            size,
            publishers,
            exchange,
            routing_key,
            confirm,
            persistent,
        } => {
            run_publish(
                &cli.uri,
                count,
                size,
                publishers,
                &exchange,
                &routing_key,
                confirm,
                persistent,
                warmup,
            )
            .await?;
        }
        Command::Consume {
            queue,
            consumers,
            prefetch,
            no_ack,
            count,
            duration,
        } => {
            run_consume(
                &cli.uri, &queue, consumers, prefetch, no_ack, count, duration, warmup,
            )
            .await?;
        }
        Command::Throughput {
            count,
            size,
            publishers,
            consumers,
            prefetch,
        } => {
            run_throughput(
                &cli.uri, count, size, publishers, consumers, prefetch, warmup,
            )
            .await?;
        }
        Command::Native {
            addr,
            count,
            size,
            batch,
            connections,
        } => {
            run_native(&addr, count, size, batch, connections, warmup).await?;
        }
        Command::Sharded {
            count,
            size,
            shards,
            prefetch,
        } => {
            run_sharded(&cli.uri, count, size, shards, prefetch, warmup).await?;
        }
    }

    Ok(())
}

/// Compute how many messages each worker should handle, distributing remainder.
/// Worker `i` (0-indexed) gets `base + (1 if i < remainder else 0)`.
fn messages_for_worker(total: u64, workers: u32, index: u32) -> u64 {
    let base = total / workers as u64;
    let remainder = total % workers as u64;
    if (index as u64) < remainder {
        base + 1
    } else {
        base
    }
}

/// Apply warmup: given the total elapsed time and warmup duration, return the
/// effective measurement duration and a scaling factor for message counts.
/// If warmup >= elapsed, returns (elapsed, 1.0) (no discarding possible).
fn apply_warmup(elapsed: Duration, warmup_secs: u64) -> (Duration, f64) {
    if warmup_secs == 0 {
        return (elapsed, 1.0);
    }
    let warmup = Duration::from_secs(warmup_secs);
    if warmup >= elapsed {
        eprintln!(
            "Warning: warmup ({warmup_secs}s) >= test duration ({:.2}s), reporting full results",
            elapsed.as_secs_f64()
        );
        return (elapsed, 1.0);
    }
    let effective = elapsed - warmup;
    let scale = effective.as_secs_f64() / elapsed.as_secs_f64();
    (effective, scale)
}

#[allow(clippy::too_many_arguments)]
async fn run_publish(
    uri: &str,
    count: u64,
    size: usize,
    publishers: u32,
    exchange: &str,
    routing_key: &str,
    confirm: bool,
    persistent: bool,
    warmup_secs: u64,
) -> Result<()> {
    let body = vec![b'x'; size];

    println!("Publishing {count} messages ({size} bytes each) with {publishers} publisher(s)...");

    // Declare the queue (setup phase, not measured)
    let conn = Connection::connect(uri, ConnectionProperties::default())
        .await
        .context("failed to connect for queue declaration")?;
    let ch = conn
        .create_channel()
        .await
        .context("failed to create setup channel")?;
    ch.queue_declare(
        routing_key,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .context("failed to declare queue")?;
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));

    // Establish all publisher connections BEFORE starting the timer
    let mut pub_conns: Vec<Connection> = Vec::new();
    let mut pub_channels: Vec<Channel> = Vec::new();
    for i in 0..publishers {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("publisher {i}: failed to connect"))?;
        let ch = conn
            .create_channel()
            .await
            .with_context(|| format!("publisher {i}: failed to create channel"))?;

        if confirm {
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .with_context(|| format!("publisher {i}: failed to enable confirms"))?;
        }
        pub_conns.push(conn);
        pub_channels.push(ch);
    }

    // Start timer AFTER all setup is complete
    let start = Instant::now();

    let mut handles = Vec::new();
    for (i, ch) in pub_channels.into_iter().enumerate() {
        let exchange = exchange.to_string();
        let routing_key = routing_key.to_string();
        let body = body.clone();
        let published = published.clone();
        let msg_count = messages_for_worker(count, publishers, i as u32);

        handles.push(tokio::spawn(async move {
            let props = if persistent {
                BasicProperties::default().with_delivery_mode(2)
            } else {
                BasicProperties::default()
            };

            for _ in 0..msg_count {
                let confirm_fut = ch
                    .basic_publish(
                        &exchange,
                        &routing_key,
                        BasicPublishOptions::default(),
                        &body,
                        props.clone(),
                    )
                    .await
                    .with_context(|| format!("publisher {i}: failed to publish"))?;

                if confirm {
                    confirm_fut
                        .await
                        .with_context(|| format!("publisher {i}: confirm failed"))?;
                }

                published.fetch_add(1, Ordering::Relaxed);
            }

            Ok::<(), anyhow::Error>(())
        }));
    }

    // Progress reporter
    let published_clone = published.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let current = published_clone.load(Ordering::Relaxed);
            let rate = current - last;
            if rate > 0 {
                eprint!("\r  published: {current} ({rate} msg/s)    ");
            }
            last = current;
            if current >= count {
                break;
            }
        }
    });

    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .with_context(|| format!("publisher {i}: task panicked"))?
            .with_context(|| format!("publisher {i}: failed"))?;
    }
    reporter.abort();

    // Keep connections alive until tasks complete
    drop(pub_conns);

    let elapsed = start.elapsed();
    let total = published.load(Ordering::Relaxed);

    let (effective, scale) = apply_warmup(elapsed, warmup_secs);
    let effective_total = (total as f64 * scale) as u64;
    let rate = effective_total as f64 / effective.as_secs_f64();

    println!("\r                                              ");
    println!("Results:");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    if warmup_secs > 0 {
        println!(
            "  Warmup:    {warmup_secs}s (effective measurement: {:.2}s)",
            effective.as_secs_f64()
        );
    }
    println!("  Rate:      {:.0} msg/s", rate);
    println!(
        "  Throughput: {:.1} MB/s",
        (effective_total as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0
    );

    Ok(())
}

async fn run_consume(
    uri: &str,
    queue: &str,
    consumers: u32,
    prefetch: u16,
    no_ack: bool,
    max_count: u64,
    max_duration: u64,
    warmup_secs: u64,
) -> Result<()> {
    println!("Consuming from '{queue}' with {consumers} consumer(s), prefetch={prefetch}...");

    let consumed = Arc::new(AtomicU64::new(0));

    // Establish all consumer connections BEFORE starting the timer
    let mut con_conns: Vec<Connection> = Vec::new();
    let mut con_consumers: Vec<Consumer> = Vec::new();
    for i in 0..consumers {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("consumer {i}: failed to connect"))?;
        let ch = conn
            .create_channel()
            .await
            .with_context(|| format!("consumer {i}: failed to create channel"))?;

        ch.basic_qos(prefetch, BasicQosOptions { global: false })
            .await
            .with_context(|| format!("consumer {i}: failed to set QoS"))?;

        let consumer = ch
            .basic_consume(
                queue,
                &format!("perf-consumer-{i}"),
                BasicConsumeOptions {
                    no_ack,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("consumer {i}: failed to start consuming"))?;

        con_conns.push(conn);
        con_consumers.push(consumer);
    }

    // Start timer AFTER all setup is complete
    let start = Instant::now();

    let mut handles = Vec::new();
    for (i, mut consumer) in con_consumers.into_iter().enumerate() {
        let consumed = consumed.clone();

        handles.push(tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        if !no_ack {
                            if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                tracing::error!("consumer {i}: ack failed: {e}");
                                return Err(anyhow::anyhow!("consumer {i}: ack failed: {e}"));
                            }
                        }
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!("consumer {i}: delivery error: {e}");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Progress reporter + termination
    let consumed_clone = consumed.clone();
    let deadline = if max_duration > 0 {
        Some(Instant::now() + Duration::from_secs(max_duration))
    } else {
        None
    };

    let mut last = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let current = consumed_clone.load(Ordering::Relaxed);
        let rate = current - last;
        eprint!("\r  consumed: {current} ({rate} msg/s)    ");
        last = current;

        if max_count > 0 && current >= max_count {
            break;
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                break;
            }
        }
    }

    for h in handles {
        h.abort();
    }

    // Keep connections alive until tasks are done
    drop(con_conns);

    let elapsed = start.elapsed();
    let total = consumed.load(Ordering::Relaxed);

    let (effective, scale) = apply_warmup(elapsed, warmup_secs);
    let effective_total = (total as f64 * scale) as u64;
    let rate = effective_total as f64 / effective.as_secs_f64();

    println!("\r                                              ");
    println!("Results:");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    if warmup_secs > 0 {
        println!(
            "  Warmup:    {warmup_secs}s (effective measurement: {:.2}s)",
            effective.as_secs_f64()
        );
    }
    println!("  Rate:      {:.0} msg/s", rate);

    Ok(())
}

async fn run_throughput(
    uri: &str,
    count: u64,
    size: usize,
    publishers: u32,
    consumers: u32,
    prefetch: u16,
    warmup_secs: u64,
) -> Result<()> {
    let body = vec![b'x'; size];
    let queue = "perf-throughput";

    println!(
        "Throughput test: {count} msgs x {size}B, {publishers} pub / {consumers} con, prefetch={prefetch}"
    );

    // Setup: declare and purge queue
    let conn = Connection::connect(uri, ConnectionProperties::default())
        .await
        .context("failed to connect for setup")?;
    let ch = conn
        .create_channel()
        .await
        .context("failed to create setup channel")?;
    ch.queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
        .await
        .context("failed to declare queue")?;
    ch.queue_purge(queue, QueuePurgeOptions::default())
        .await
        .context("failed to purge queue")?;
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));

    // Establish all consumer connections BEFORE starting the timer
    let mut keep_alive_conns: Vec<Connection> = Vec::new();
    let mut con_consumers: Vec<Consumer> = Vec::new();
    for i in 0..consumers {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("consumer {i}: failed to connect"))?;
        let ch = conn
            .create_channel()
            .await
            .with_context(|| format!("consumer {i}: failed to create channel"))?;
        ch.basic_qos(prefetch, BasicQosOptions { global: false })
            .await
            .with_context(|| format!("consumer {i}: failed to set QoS"))?;
        let consumer = ch
            .basic_consume(
                queue,
                &format!("tp-con-{i}"),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("consumer {i}: failed to start consuming"))?;
        keep_alive_conns.push(conn);
        con_consumers.push(consumer);
    }

    // Establish all publisher connections BEFORE starting the timer
    let mut pub_channels: Vec<Channel> = Vec::new();
    for i in 0..publishers {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("publisher {i}: failed to connect"))?;
        let ch = conn
            .create_channel()
            .await
            .with_context(|| format!("publisher {i}: failed to create channel"))?;
        keep_alive_conns.push(conn);
        pub_channels.push(ch);
    }

    // Start timer AFTER all setup is complete
    let start = Instant::now();

    // Start consumers
    let mut con_handles = Vec::new();
    for (i, mut consumer) in con_consumers.into_iter().enumerate() {
        let consumed = consumed.clone();

        con_handles.push(tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            tracing::error!("consumer {i}: ack failed: {e}");
                            return Err(anyhow::anyhow!("consumer {i}: ack failed: {e}"));
                        }
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!("consumer {i}: delivery error: {e}");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Start publishers
    let mut pub_handles = Vec::new();
    for (i, ch) in pub_channels.into_iter().enumerate() {
        let body = body.clone();
        let published = published.clone();
        let msg_count = messages_for_worker(count, publishers, i as u32);

        pub_handles.push(tokio::spawn(async move {
            for _ in 0..msg_count {
                ch.basic_publish(
                    "",
                    queue,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await
                .with_context(|| format!("publisher {i}: failed to publish"))?;
                published.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Progress
    let pub_c = published.clone();
    let con_c = consumed.clone();
    let reporter = tokio::spawn(async move {
        let mut last_pub = 0u64;
        let mut last_con = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let p = pub_c.load(Ordering::Relaxed);
            let c = con_c.load(Ordering::Relaxed);
            eprint!(
                "\r  pub: {} ({}/s)  con: {} ({}/s)    ",
                p,
                p - last_pub,
                c,
                c - last_con
            );
            last_pub = p;
            last_con = c;
            if c >= count {
                break;
            }
        }
    });

    // Wait for publishers to finish
    for (i, h) in pub_handles.into_iter().enumerate() {
        h.await
            .with_context(|| format!("publisher {i}: task panicked"))?
            .with_context(|| format!("publisher {i}: failed"))?;
    }

    // Wait for consumers to catch up (with timeout)
    let deadline = Instant::now() + Duration::from_secs(30);
    while consumed.load(Ordering::Relaxed) < count {
        if Instant::now() > deadline {
            eprintln!("\nWarning: consumer didn't finish within timeout");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    reporter.abort();
    for h in con_handles {
        h.abort();
    }

    // Keep connections alive until tasks are done
    drop(keep_alive_conns);

    let elapsed = start.elapsed();
    let total_pub = published.load(Ordering::Relaxed);
    let total_con = consumed.load(Ordering::Relaxed);

    let (effective, scale) = apply_warmup(elapsed, warmup_secs);
    let eff_pub = (total_pub as f64 * scale) as u64;
    let eff_con = (total_con as f64 * scale) as u64;
    let pub_rate = eff_pub as f64 / effective.as_secs_f64();
    let con_rate = eff_con as f64 / effective.as_secs_f64();

    println!("\r                                                          ");
    println!("Results:");
    println!("  Published: {total_pub} ({:.0} msg/s)", pub_rate);
    println!("  Consumed:  {total_con} ({:.0} msg/s)", con_rate);
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    if warmup_secs > 0 {
        println!(
            "  Warmup:    {warmup_secs}s (effective measurement: {:.2}s)",
            effective.as_secs_f64()
        );
    }
    println!(
        "  Throughput: {:.1} MB/s (pub), {:.1} MB/s (con)",
        (eff_pub as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0,
        (eff_con as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0,
    );

    Ok(())
}

async fn run_sharded(
    uri: &str,
    count: u64,
    size: usize,
    shards: u32,
    prefetch: u16,
    warmup_secs: u64,
) -> Result<()> {
    let body = vec![b'x'; size];

    println!(
        "Sharded throughput: {} msgs x {}B across {} independent queues, prefetch={}",
        count, size, shards, prefetch
    );

    // Setup: declare all queues
    let conn = Connection::connect(uri, ConnectionProperties::default())
        .await
        .context("failed to connect for setup")?;
    let ch = conn
        .create_channel()
        .await
        .context("failed to create setup channel")?;
    for i in 0..shards {
        let qname = format!("shard-{i}");
        ch.queue_declare(
            &qname,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("failed to declare queue {qname}"))?;
        ch.queue_purge(&qname, QueuePurgeOptions::default())
            .await
            .with_context(|| format!("failed to purge queue {qname}"))?;
    }
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));

    // Pre-establish all connections BEFORE starting the timer
    let mut keep_alive_conns: Vec<Connection> = Vec::new();
    let mut shard_consumers: Vec<Consumer> = Vec::new();
    let mut shard_pub_channels: Vec<(Channel, String)> = Vec::new();

    for shard in 0..shards {
        let qname = format!("shard-{shard}");

        // Consumer connection
        let con_conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("shard {shard} consumer: failed to connect"))?;
        let con_ch = con_conn
            .create_channel()
            .await
            .with_context(|| format!("shard {shard} consumer: failed to create channel"))?;
        con_ch
            .basic_qos(prefetch, BasicQosOptions { global: false })
            .await
            .with_context(|| format!("shard {shard} consumer: failed to set QoS"))?;
        let consumer = con_ch
            .basic_consume(
                &qname,
                &format!("shard-con-{shard}"),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("shard {shard} consumer: failed to start consuming"))?;
        keep_alive_conns.push(con_conn);
        shard_consumers.push(consumer);

        // Publisher connection
        let pub_conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .with_context(|| format!("shard {shard} publisher: failed to connect"))?;
        let pub_ch = pub_conn
            .create_channel()
            .await
            .with_context(|| format!("shard {shard} publisher: failed to create channel"))?;
        keep_alive_conns.push(pub_conn);
        shard_pub_channels.push((pub_ch, qname));
    }

    // Start timer AFTER all setup is complete
    let start = Instant::now();

    let mut handles = Vec::new();

    for (shard, mut consumer) in shard_consumers.into_iter().enumerate() {
        let consumed = consumed.clone();

        // Consumer for this shard
        handles.push(tokio::spawn(async move {
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            tracing::error!("shard {shard} consumer: ack failed: {e}");
                            return Err(anyhow::anyhow!("shard {shard} consumer: ack failed: {e}"));
                        }
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tracing::error!("shard {shard} consumer: delivery error: {e}");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for (shard, (pub_ch, qname)) in shard_pub_channels.into_iter().enumerate() {
        let body = body.clone();
        let published = published.clone();
        let per_shard = messages_for_worker(count, shards, shard as u32);

        // Publisher for this shard
        handles.push(tokio::spawn(async move {
            for _ in 0..per_shard {
                pub_ch
                    .basic_publish(
                        "",
                        &qname,
                        BasicPublishOptions::default(),
                        &body,
                        BasicProperties::default(),
                    )
                    .await
                    .with_context(|| format!("shard {shard} publisher: failed to publish"))?;
                published.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Progress reporter
    let pub_c = published.clone();
    let con_c = consumed.clone();
    let reporter = tokio::spawn(async move {
        let mut last_pub = 0u64;
        let mut last_con = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let p = pub_c.load(Ordering::Relaxed);
            let c = con_c.load(Ordering::Relaxed);
            eprint!(
                "\r  pub: {} ({}/s)  con: {} ({}/s)    ",
                p,
                p - last_pub,
                c,
                c - last_con
            );
            last_pub = p;
            last_con = c;
            if c >= count {
                break;
            }
        }
    });

    // Wait for all work to complete
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let p = published.load(Ordering::Relaxed);
        let c = consumed.load(Ordering::Relaxed);
        if c >= count {
            break;
        }
        if Instant::now() > deadline {
            eprintln!("\nWarning: didn't finish within timeout (pub={p}, con={c})");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    reporter.abort();
    for h in handles {
        h.abort();
    }

    // Keep connections alive until tasks are done
    drop(keep_alive_conns);

    let elapsed = start.elapsed();
    let total_pub = published.load(Ordering::Relaxed);
    let total_con = consumed.load(Ordering::Relaxed);

    let (effective, scale) = apply_warmup(elapsed, warmup_secs);
    let eff_pub = (total_pub as f64 * scale) as u64;
    let eff_con = (total_con as f64 * scale) as u64;
    let pub_rate = eff_pub as f64 / effective.as_secs_f64();
    let con_rate = eff_con as f64 / effective.as_secs_f64();

    println!("\r                                                          ");
    println!("Results ({shards} shards):");
    println!("  Published: {total_pub} ({:.0} msg/s)", pub_rate);
    println!("  Consumed:  {total_con} ({:.0} msg/s)", con_rate);
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    if warmup_secs > 0 {
        println!(
            "  Warmup:    {warmup_secs}s (effective measurement: {:.2}s)",
            effective.as_secs_f64()
        );
    }
    println!(
        "  Throughput: {:.1} MB/s (pub), {:.1} MB/s (con)",
        (eff_pub as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0,
        (eff_con as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0,
    );
    println!("  Per-shard:  {:.0} msg/s", con_rate / shards as f64);

    Ok(())
}

async fn run_native(
    addr: &str,
    count: u64,
    size: usize,
    batch: u32,
    connections: u32,
    warmup_secs: u64,
) -> Result<()> {
    let body = vec![b'x'; size];

    println!(
        "Native protocol: {} msgs x {}B, batch={}, {} connections",
        count, size, batch, connections
    );

    let published = Arc::new(AtomicU64::new(0));

    // Establish all connections BEFORE starting the timer
    let mut clients = Vec::new();
    for i in 0..connections {
        let client = rmq_native::client::NativeClient::connect(addr, "guest", "guest")
            .await
            .map_err(|e| anyhow::anyhow!("native connection {i}: failed to connect: {e}"))?;
        clients.push(client);
    }

    // Start timer AFTER all setup is complete
    let start = Instant::now();

    let mut handles = Vec::new();
    for (i, mut client) in clients.into_iter().enumerate() {
        let body = body.clone();
        let published = published.clone();
        let per_conn = messages_for_worker(count, connections, i as u32);
        let queue = format!("native-{i}");

        handles.push(tokio::spawn(async move {
            let mut remaining = per_conn;
            while remaining > 0 {
                let this_batch = remaining.min(batch as u64) as usize;
                let messages: Vec<bytes::Bytes> = (0..this_batch)
                    .map(|_| bytes::Bytes::copy_from_slice(&body))
                    .collect();
                let confirmed = client.publish_batch(&queue, messages).await.map_err(|e| {
                    anyhow::anyhow!("native connection {i}: publish_batch failed: {e}")
                })?;
                published.fetch_add(confirmed as u64, Ordering::Relaxed);
                remaining -= this_batch as u64;
            }

            client
                .disconnect()
                .await
                .map_err(|e| anyhow::anyhow!("native connection {i}: disconnect failed: {e}"))?;

            Ok::<(), anyhow::Error>(())
        }));
    }

    // Progress
    let pub_c = published.clone();
    let reporter = tokio::spawn(async move {
        let mut last = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let p = pub_c.load(Ordering::Relaxed);
            let rate = p - last;
            eprint!("\r  published: {} ({}/s)    ", p, rate);
            last = p;
            if p >= count {
                break;
            }
        }
    });

    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .with_context(|| format!("native connection {i}: task panicked"))?
            .with_context(|| format!("native connection {i}: failed"))?;
    }
    reporter.abort();

    let elapsed = start.elapsed();
    let total = published.load(Ordering::Relaxed);

    let (effective, scale) = apply_warmup(elapsed, warmup_secs);
    let effective_total = (total as f64 * scale) as u64;
    let rate = effective_total as f64 / effective.as_secs_f64();

    println!("\r                                              ");
    println!("Results (native protocol):");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    if warmup_secs > 0 {
        println!(
            "  Warmup:    {warmup_secs}s (effective measurement: {:.2}s)",
            effective.as_secs_f64()
        );
    }
    println!("  Rate:      {:.0} msg/s", rate);
    println!(
        "  Throughput: {:.1} MB/s",
        (effective_total as f64 * size as f64) / effective.as_secs_f64() / 1_000_000.0
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_messages_for_worker_even_division() {
        // 100 messages across 4 workers = 25 each
        assert_eq!(messages_for_worker(100, 4, 0), 25);
        assert_eq!(messages_for_worker(100, 4, 1), 25);
        assert_eq!(messages_for_worker(100, 4, 2), 25);
        assert_eq!(messages_for_worker(100, 4, 3), 25);
    }

    #[test]
    fn test_messages_for_worker_with_remainder() {
        // 10 messages across 3 workers: 4, 3, 3
        assert_eq!(messages_for_worker(10, 3, 0), 4);
        assert_eq!(messages_for_worker(10, 3, 1), 3);
        assert_eq!(messages_for_worker(10, 3, 2), 3);

        // Verify total is preserved
        let total: u64 = (0..3).map(|i| messages_for_worker(10, 3, i)).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_messages_for_worker_single() {
        assert_eq!(messages_for_worker(42, 1, 0), 42);
    }

    #[test]
    fn test_messages_for_worker_more_workers_than_messages() {
        // 2 messages across 5 workers: first 2 get 1, rest get 0
        assert_eq!(messages_for_worker(2, 5, 0), 1);
        assert_eq!(messages_for_worker(2, 5, 1), 1);
        assert_eq!(messages_for_worker(2, 5, 2), 0);
        assert_eq!(messages_for_worker(2, 5, 3), 0);
        assert_eq!(messages_for_worker(2, 5, 4), 0);

        let total: u64 = (0..5).map(|i| messages_for_worker(2, 5, i)).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_apply_warmup_zero() {
        let elapsed = Duration::from_secs(10);
        let (effective, scale) = apply_warmup(elapsed, 0);
        assert_eq!(effective, elapsed);
        assert_eq!(scale, 1.0);
    }

    #[test]
    fn test_apply_warmup_normal() {
        let elapsed = Duration::from_secs(10);
        let (effective, scale) = apply_warmup(elapsed, 2);
        assert_eq!(effective, Duration::from_secs(8));
        assert!((scale - 0.8).abs() < 1e-9);
    }

    #[test]
    fn test_apply_warmup_exceeds_duration() {
        let elapsed = Duration::from_secs(5);
        let (effective, scale) = apply_warmup(elapsed, 10);
        assert_eq!(effective, elapsed);
        assert_eq!(scale, 1.0);
    }
}
