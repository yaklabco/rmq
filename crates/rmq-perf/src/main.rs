use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use futures_lite::StreamExt;
use lapin::options::*;
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};

#[derive(Parser)]
#[command(name = "rmq-perf", about = "RMQ performance testing tool")]
struct Cli {
    /// AMQP URI
    #[arg(short, long, default_value = "amqp://guest:guest@127.0.0.1:5672")]
    uri: String,

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let cli = Cli::parse();

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
                &cli.uri, &queue, consumers, prefetch, no_ack, count, duration,
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
            run_throughput(&cli.uri, count, size, publishers, consumers, prefetch).await?;
        }
        Command::Native {
            addr,
            count,
            size,
            batch,
            connections,
        } => {
            run_native(&addr, count, size, batch, connections).await?;
        }
        Command::Sharded {
            count,
            size,
            shards,
            prefetch,
        } => {
            run_sharded(&cli.uri, count, size, shards, prefetch).await?;
        }
    }

    Ok(())
}

async fn run_publish(
    uri: &str,
    count: u64,
    size: usize,
    publishers: u32,
    exchange: &str,
    routing_key: &str,
    confirm: bool,
    persistent: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = vec![b'x'; size];
    let per_publisher = count / publishers as u64;

    println!("Publishing {count} messages ({size} bytes each) with {publishers} publisher(s)...");

    // Declare the queue
    let conn = Connection::connect(uri, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    ch.queue_declare(
        routing_key,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await?;
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..publishers {
        let uri = uri.to_string();
        let exchange = exchange.to_string();
        let routing_key = routing_key.to_string();
        let body = body.clone();
        let published = published.clone();

        handles.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();

            if confirm {
                ch.confirm_select(ConfirmSelectOptions::default())
                    .await
                    .unwrap();
            }

            let props = if persistent {
                BasicProperties::default().with_delivery_mode(2)
            } else {
                BasicProperties::default()
            };

            for _ in 0..per_publisher {
                let confirm_fut = ch
                    .basic_publish(
                        &exchange,
                        &routing_key,
                        BasicPublishOptions::default(),
                        &body,
                        props.clone(),
                    )
                    .await
                    .unwrap();

                if confirm {
                    confirm_fut.await.unwrap();
                }

                published.fetch_add(1, Ordering::Relaxed);
            }
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

    for h in handles {
        h.await?;
    }
    reporter.abort();
    let elapsed = start.elapsed();
    let total = published.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();

    println!("\r                                              ");
    println!("Results:");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    println!("  Rate:      {:.0} msg/s", rate);
    println!(
        "  Throughput: {:.1} MB/s",
        (total as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0
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
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Consuming from '{queue}' with {consumers} consumer(s), prefetch={prefetch}...");

    let consumed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for i in 0..consumers {
        let uri = uri.to_string();
        let queue = queue.to_string();
        let consumed = consumed.clone();

        handles.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();

            ch.basic_qos(prefetch, BasicQosOptions { global: false })
                .await
                .unwrap();

            let mut consumer = ch
                .basic_consume(
                    &queue,
                    &format!("perf-consumer-{i}"),
                    BasicConsumeOptions {
                        no_ack,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();

            while let Some(delivery) = consumer.next().await {
                if let Ok(delivery) = delivery {
                    if !no_ack {
                        delivery.ack(BasicAckOptions::default()).await.unwrap();
                    }
                    consumed.fetch_add(1, Ordering::Relaxed);
                }
            }
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

    let elapsed = start.elapsed();
    let total = consumed.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();

    println!("\r                                              ");
    println!("Results:");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
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
) -> Result<(), Box<dyn std::error::Error>> {
    let body = vec![b'x'; size];
    let per_publisher = count / publishers as u64;
    let queue = "perf-throughput";

    println!(
        "Throughput test: {count} msgs x {size}B, {publishers} pub / {consumers} con, prefetch={prefetch}"
    );

    // Setup
    let conn = Connection::connect(uri, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    ch.queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
        .await?;
    // Purge any leftover messages
    ch.queue_purge(queue, QueuePurgeOptions::default()).await?;
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Start consumers first
    let mut con_handles = Vec::new();
    for i in 0..consumers {
        let uri = uri.to_string();
        let consumed = consumed.clone();

        con_handles.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();
            ch.basic_qos(prefetch, BasicQosOptions { global: false })
                .await
                .unwrap();

            let mut consumer = ch
                .basic_consume(
                    queue,
                    &format!("tp-con-{i}"),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            while let Some(delivery) = consumer.next().await {
                if let Ok(delivery) = delivery {
                    delivery.ack(BasicAckOptions::default()).await.unwrap();
                    consumed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Give consumers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start publishers
    let mut pub_handles = Vec::new();
    for _ in 0..publishers {
        let uri = uri.to_string();
        let body = body.clone();
        let published = published.clone();

        pub_handles.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();

            for _ in 0..per_publisher {
                ch.basic_publish(
                    "",
                    queue,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await
                .unwrap();
                published.fetch_add(1, Ordering::Relaxed);
            }
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
    for h in pub_handles {
        h.await?;
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

    let elapsed = start.elapsed();
    let total_pub = published.load(Ordering::Relaxed);
    let total_con = consumed.load(Ordering::Relaxed);
    let pub_rate = total_pub as f64 / elapsed.as_secs_f64();
    let con_rate = total_con as f64 / elapsed.as_secs_f64();

    println!("\r                                                          ");
    println!("Results:");
    println!("  Published: {total_pub} ({:.0} msg/s)", pub_rate);
    println!("  Consumed:  {total_con} ({:.0} msg/s)", con_rate);
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.1} MB/s (pub), {:.1} MB/s (con)",
        (total_pub as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0,
        (total_con as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0,
    );

    Ok(())
}

async fn run_sharded(
    uri: &str,
    count: u64,
    size: usize,
    shards: u32,
    prefetch: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let per_shard = count / shards as u64;
    let body = vec![b'x'; size];

    println!(
        "Sharded throughput: {} msgs x {}B across {} independent queues, prefetch={}",
        count, size, shards, prefetch
    );

    // Setup: declare all queues
    let conn = Connection::connect(uri, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    for i in 0..shards {
        let qname = format!("shard-{i}");
        ch.queue_declare(
            &qname,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
        ch.queue_purge(&qname, QueuePurgeOptions::default()).await?;
    }
    drop(ch);
    drop(conn);

    let published = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();

    // Each shard gets its own connection pair (1 pub + 1 con)
    for shard in 0..shards {
        let uri_pub = uri.to_string();
        let uri_con = uri.to_string();
        let body = body.clone();
        let published = published.clone();
        let consumed = consumed.clone();
        let qname = format!("shard-{shard}");
        let qname2 = qname.clone();

        // Consumer for this shard
        handles.push(tokio::spawn(async move {
            let conn = Connection::connect(&uri_con, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();
            ch.basic_qos(prefetch, BasicQosOptions { global: false })
                .await
                .unwrap();

            let mut consumer = ch
                .basic_consume(
                    &qname2,
                    &format!("shard-con-{shard}"),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            while let Some(delivery) = consumer.next().await {
                if let Ok(delivery) = delivery {
                    delivery.ack(BasicAckOptions::default()).await.unwrap();
                    consumed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));

        // Publisher for this shard
        handles.push(tokio::spawn(async move {
            // Small delay so consumer starts first
            tokio::time::sleep(Duration::from_millis(100)).await;
            let conn = Connection::connect(&uri_pub, ConnectionProperties::default())
                .await
                .unwrap();
            let ch = conn.create_channel().await.unwrap();

            for _ in 0..per_shard {
                ch.basic_publish(
                    "",
                    &qname,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await
                .unwrap();
                published.fetch_add(1, Ordering::Relaxed);
            }
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

    // Wait for all publishers to finish
    // (consumers run indefinitely until aborted)
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

    let elapsed = start.elapsed();
    let total_pub = published.load(Ordering::Relaxed);
    let total_con = consumed.load(Ordering::Relaxed);
    let pub_rate = total_pub as f64 / elapsed.as_secs_f64();
    let con_rate = total_con as f64 / elapsed.as_secs_f64();

    println!("\r                                                          ");
    println!("Results ({shards} shards):");
    println!("  Published: {total_pub} ({:.0} msg/s)", pub_rate);
    println!("  Consumed:  {total_con} ({:.0} msg/s)", con_rate);
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.1} MB/s (pub), {:.1} MB/s (con)",
        (total_pub as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0,
        (total_con as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0,
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
) -> Result<(), Box<dyn std::error::Error>> {
    let per_conn = count / connections as u64;
    let body = vec![b'x'; size];

    println!(
        "Native protocol: {} msgs x {}B, batch={}, {} connections",
        count, size, batch, connections
    );

    let published = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for i in 0..connections {
        let addr = addr.to_string();
        let body = body.clone();
        let published = published.clone();
        let queue = format!("native-{i}");

        handles.push(tokio::spawn(async move {
            let mut client = rmq_native::client::NativeClient::connect(&addr, "guest", "guest")
                .await
                .unwrap();

            let mut remaining = per_conn;
            while remaining > 0 {
                let this_batch = remaining.min(batch as u64) as usize;
                let messages: Vec<bytes::Bytes> = (0..this_batch)
                    .map(|_| bytes::Bytes::copy_from_slice(&body))
                    .collect();
                let confirmed = client.publish_batch(&queue, messages).await.unwrap();
                published.fetch_add(confirmed as u64, Ordering::Relaxed);
                remaining -= this_batch as u64;
            }

            client.disconnect().await.unwrap();
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

    for h in handles {
        h.await?;
    }
    reporter.abort();

    let elapsed = start.elapsed();
    let total = published.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();

    println!("\r                                              ");
    println!("Results (native protocol):");
    println!("  Messages:  {total}");
    println!("  Time:      {:.2}s", elapsed.as_secs_f64());
    println!("  Rate:      {:.0} msg/s", rate);
    println!(
        "  Throughput: {:.1} MB/s",
        (total as f64 * size as f64) / elapsed.as_secs_f64() / 1_000_000.0
    );

    Ok(())
}
