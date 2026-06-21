//! Traffic generator for demos and testing.
//!
//! Creates a small tree of topics with hierarchical, dotted names — chosen to
//! show off the UI's drill-down topic tree — attaches a subscription to most of
//! them, then continuously publishes and (via streaming pulls) consumes. Every
//! call is made *through the proxy endpoint*, so the monitor observes all of it:
//! publishers, per-publisher and per-topic message counts, live streaming
//! consumers and consumed counts.
//!
//! It deliberately drives traffic with the raw generated gRPC clients (the same
//! `crate::pb` stubs the proxy forwards), so it adds no dependencies and exercises
//! exactly the call shapes the proxy taps.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::cli::LoadgenCli;
use crate::pb::publisher_client::PublisherClient;
use crate::pb::subscriber_client::SubscriberClient;
use crate::pb::{PublishRequest, PubsubMessage, StreamingPullRequest, Subscription, Topic};

/// The demo topic tree. The dotted names nest in the UI: `acme.orders.created`
/// and `acme.orders.shipped` both sit under `acme ▸ orders`, and so on.
const TOPICS: &[&str] = &[
    "acme.orders.created",
    "acme.orders.shipped",
    "acme.orders.cancelled",
    "acme.billing.invoiced",
    "acme.billing.refunded",
    "telemetry.device.heartbeat",
    "telemetry.device.error",
    "telemetry.app.click",
    "logs.audit",
];

/// Topics deliberately left without a subscription/consumer, to show the UI hiding
/// consumers (and topics) that have no consumed traffic.
const NO_CONSUMER: &[&str] = &["logs.audit"];

/// Number of distinct publisher connections. Each is a separate socket, so it is a
/// distinct peer — and thus a distinct publisher — in the monitor's view.
const PUBLISHERS: usize = 2;

/// Stream/ack deadline requested by consumers, in seconds.
const ACK_DEADLINE_SECS: i32 = 10;

/// Run the generator until `duration_secs` elapses (or forever if it is 0).
pub async fn run(cli: LoadgenCli) -> anyhow::Result<()> {
    let url = format!("http://{}", cli.endpoint);

    ensure_topology(&url, &cli.project_id).await?;

    // One streaming consumer per subscribed topic: opens a live consumer and acks
    // whatever is delivered.
    for topic in TOPICS {
        if NO_CONSUMER.contains(topic) {
            continue;
        }
        let channel = connect(&url).await?;
        let subscription = sub_path(&cli.project_id, topic);
        tokio::spawn(consume(channel, subscription));
    }

    // Distinct publisher connections so the UI attributes traffic to more than one
    // publisher across the topic tree. With `--compress`, each gzip-compresses its
    // request bodies, exercising the proxy's compression support (and matching how a
    // real Pub/Sub client with compression enabled publishes).
    let mut publishers = Vec::with_capacity(PUBLISHERS);
    for _ in 0..PUBLISHERS {
        let mut client = PublisherClient::new(connect(&url).await?);
        if cli.compress {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }
        publishers.push(client);
    }

    tracing::info!(
        endpoint = %cli.endpoint,
        topics = TOPICS.len(),
        compress = cli.compress,
        "load generator running"
    );

    let publishing = publish_loop(publishers, cli.project_id.clone(), cli.interval_ms);
    if cli.duration_secs > 0 {
        let _ = tokio::time::timeout(Duration::from_secs(cli.duration_secs), publishing).await;
        tracing::info!("duration elapsed; stopping");
    } else {
        publishing.await;
    }
    Ok(())
}

/// Create every topic and (where applicable) its subscription, ignoring entities
/// that already exist so the generator is re-runnable.
async fn ensure_topology(url: &str, project: &str) -> anyhow::Result<()> {
    let publisher = PublisherClient::new(connect(url).await?);
    let subscriber = SubscriberClient::new(connect(url).await?);

    for topic in TOPICS {
        let name = topic_path(project, topic);
        with_retry(&name, || {
            let mut publisher = publisher.clone();
            let topic = Topic {
                name: name.clone(),
                ..Default::default()
            };
            async move { publisher.create_topic(topic).await.map(|_| ()) }
        })
        .await?;
        tracing::info!(%name, "topic ready");

        if NO_CONSUMER.contains(topic) {
            continue;
        }

        let subscription = sub_path(project, topic);
        with_retry(&subscription, || {
            let mut subscriber = subscriber.clone();
            let sub = Subscription {
                name: subscription.clone(),
                topic: name.clone(),
                ack_deadline_seconds: ACK_DEADLINE_SECS,
                ..Default::default()
            };
            async move { subscriber.create_subscription(sub).await.map(|_| ()) }
        })
        .await?;
        tracing::info!(%subscription, "subscription ready");
    }
    Ok(())
}

/// Run a create call, treating "already exists" as success and retrying while the
/// upstream is still coming up (the proxy forwards lazily, so the emulator may not
/// be reachable for the first second or two of a freshly-started stack).
async fn with_retry<F, Fut>(label: &str, mut call: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), tonic::Status>>,
{
    const ATTEMPTS: u32 = 30;
    for attempt in 1..=ATTEMPTS {
        match call().await {
            Ok(()) => return Ok(()),
            Err(status) if status.code() == tonic::Code::AlreadyExists => return Ok(()),
            Err(status) if status.code() == tonic::Code::Unavailable && attempt < ATTEMPTS => {
                tracing::warn!(%label, attempt, "upstream unavailable; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(status) => return Err(anyhow::anyhow!("{label}: {status}")),
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// Publish a small, varying batch to every topic each tick, forever.
async fn publish_loop(
    mut publishers: Vec<PublisherClient<Channel>>,
    project: String,
    interval_ms: u64,
) {
    let mut ticker = interval(Duration::from_millis(interval_ms.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut round: u64 = 0;
    loop {
        ticker.tick().await;
        for (index, topic) in TOPICS.iter().enumerate() {
            // Vary batch size 1..=4 deterministically so per-topic counts diverge.
            let count = 1 + ((round + index as u64) % 4);
            let messages = (0..count).map(|n| payload_for(topic, round, n)).collect();

            let slot = index % publishers.len();
            let client = &mut publishers[slot];
            let request = PublishRequest {
                topic: topic_path(&project, topic),
                messages,
            };
            if let Err(status) = client.publish(request).await {
                tracing::warn!(%topic, %status, "publish failed");
            }
        }
        round = round.wrapping_add(1);
    }
}

/// Build one demo message for `topic`, choosing a payload shape by topic family so
/// the UI's content viewer exercises all of its formats: most topics carry JSON,
/// `logs.audit` carries plain text, and `telemetry.device.error` carries a binary
/// blob that falls through to the hex view. Each message is tagged with a
/// `content-type` attribute so the detail header has something to show.
fn payload_for(topic: &str, round: u64, n: u64) -> PubsubMessage {
    let (data, content_type) = if topic == "logs.audit" {
        let line = format!("AUDIT user=svc-{n} action=publish topic={topic} seq={round}.{n}");
        (Bytes::from(line), "text/plain")
    } else if topic == "telemetry.device.error" {
        // Deliberately non-UTF-8: a little-endian code followed by raw bytes.
        let code = 0xC0DE_u32.wrapping_add(round as u32);
        let mut bytes = code.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[0xFF, 0x00, 0xBE, 0xEF, n as u8, 0x80, 0x7F, 0x01]);
        (Bytes::from(bytes), "application/octet-stream")
    } else {
        let json = format!(
            r#"{{"topic":"{topic}","round":{round},"index":{n},"amount":{}}}"#,
            (round + n) * 7 % 1000
        );
        (Bytes::from(json), "application/json")
    };

    let mut attributes = HashMap::new();
    attributes.insert("content-type".to_owned(), content_type.to_owned());
    attributes.insert("round".to_owned(), round.to_string());
    PubsubMessage {
        data,
        attributes,
        ..Default::default()
    }
}

/// A long-lived streaming consumer: open the stream, then ack everything it
/// delivers. Holding the request sender keeps the consumer "live" in the UI.
async fn consume(channel: Channel, subscription: String) {
    let mut client = SubscriberClient::new(channel);
    let (requests, request_rx) = mpsc::channel::<StreamingPullRequest>(16);

    // The first frame on a StreamingPull names the subscription and opens it.
    let open = StreamingPullRequest {
        subscription: subscription.clone(),
        stream_ack_deadline_seconds: ACK_DEADLINE_SECS,
        ..Default::default()
    };
    if requests.send(open).await.is_err() {
        return;
    }

    let mut responses = match client.streaming_pull(ReceiverStream::new(request_rx)).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            tracing::warn!(%subscription, %status, "streaming pull failed");
            return;
        }
    };

    while let Some(item) = responses.next().await {
        match item {
            Ok(response) => {
                let ack_ids: Vec<String> = response
                    .received_messages
                    .iter()
                    .map(|message| message.ack_id.clone())
                    .collect();
                if !ack_ids.is_empty() {
                    let ack = StreamingPullRequest {
                        ack_ids,
                        ..Default::default()
                    };
                    if requests.send(ack).await.is_err() {
                        break;
                    }
                }
            }
            Err(status) => {
                tracing::warn!(%subscription, %status, "consumer stream error");
                break;
            }
        }
    }
}

/// Connect to the endpoint, retrying briefly so the generator can start before the
/// monitor's proxy is listening (e.g. when they race up together in a stack).
async fn connect(url: &str) -> anyhow::Result<Channel> {
    const ATTEMPTS: u32 = 30;
    let endpoint = Channel::from_shared(url.to_owned())?;
    for attempt in 1..=ATTEMPTS {
        match endpoint.connect().await {
            Ok(channel) => return Ok(channel),
            Err(err) if attempt < ATTEMPTS => {
                tracing::warn!(%url, attempt, %err, "connect failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
    unreachable!("loop returns on the final attempt")
}

fn topic_path(project: &str, id: &str) -> String {
    format!("projects/{project}/topics/{id}")
}

fn sub_path(project: &str, id: &str) -> String {
    format!("projects/{project}/subscriptions/{id}.worker")
}
