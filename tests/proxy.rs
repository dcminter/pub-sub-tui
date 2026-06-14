//! End-to-end test of the interception proxy against a real Pub/Sub emulator.
//!
//! Spins up the proxy in front of the emulator, drives a client *through* the
//! proxy (publish + pull + ack), and asserts the proxy both forwarded the traffic
//! (the operations succeed) and observed it (the counters reflect the activity).
//!
//! Ignored by default; needs a running emulator (see `tests/emulator.rs`). Run:
//!
//! ```text
//! cargo test --test proxy -- --ignored
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::subscription::SubscriptionConfig;
use pub_sub_tui::{observe, poller, proxy};

fn upstream() -> String {
    std::env::var("PUBSUB_TEST_UPSTREAM").unwrap_or_else(|_| "localhost:8085".to_owned())
}

#[tokio::test]
#[ignore = "requires a running Pub/Sub emulator"]
async fn proxy_forwards_and_observes_traffic() {
    const MESSAGES: usize = 5;
    let listen: SocketAddr = "127.0.0.1:18681".parse().unwrap();

    // Proxy in front of the emulator, feeding a fresh observer.
    let observer = observe::start();
    tokio::spawn(proxy::serve(listen, upstream(), observer.sink.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A client that talks to the PROXY (not the emulator directly).
    let client = poller::connect("test-project", "127.0.0.1:18681")
        .await
        .expect("connect through proxy");

    let topic = match client.create_topic("pst-proxy-topic", None, None).await {
        Ok(topic) => topic,
        Err(_) => client.topic("pst-proxy-topic"),
    };
    let sub = client.subscription("pst-proxy-sub");
    let _ = sub
        .create(
            topic.fully_qualified_name(),
            SubscriptionConfig::default(),
            None,
        )
        .await;

    // Publish a batch through the proxy (one unary Publish carrying N messages).
    let publisher = topic.new_publisher(None);
    let batch: Vec<PubsubMessage> = (0..MESSAGES)
        .map(|i| PubsubMessage {
            data: format!("message-{i}").into_bytes(),
            ..Default::default()
        })
        .collect();
    publisher
        .publish_immediately(batch, None)
        .await
        .expect("publish through proxy");

    // Pull and acknowledge through the proxy until all messages are consumed.
    let mut acked = 0usize;
    for _ in 0..20 {
        let received = sub.pull(10, None).await.expect("pull through proxy");
        for message in &received {
            message.ack().await.expect("ack through proxy");
            acked += 1;
        }
        if acked >= MESSAGES {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        acked, MESSAGES,
        "all messages forwarded and acked via proxy"
    );

    // Let the observation pipeline flush, then inspect the snapshot.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot = observer.snapshots.borrow().clone();

    let topic_state = snapshot
        .topics
        .get(topic.fully_qualified_name())
        .expect("topic observed via proxy");
    assert_eq!(
        topic_state.publish_count, MESSAGES as u64,
        "observed publish count"
    );
    assert!(
        !topic_state.publishers.is_empty(),
        "a publisher peer was recorded"
    );

    let sub_state = snapshot
        .subscriptions
        .get(sub.fully_qualified_name())
        .expect("subscription observed via proxy");
    assert_eq!(sub_state.acked, MESSAGES as u64, "observed consumed count");
    assert!(
        sub_state.delivered >= MESSAGES as u64,
        "observed delivered count"
    );
}

// Exercises the bidirectional `StreamingPull` tap with a raw gRPC client we fully
// control, so the connection lifecycle (open, deliver, ack, close) is deterministic
// — unlike the high-level client, whose pooled connections close lazily.
#[tokio::test]
#[ignore = "requires a running Pub/Sub emulator"]
async fn proxy_observes_streaming_pull_consumers() {
    use pub_sub_tui::pb::StreamingPullRequest;
    use pub_sub_tui::pb::subscriber_client::SubscriberClient;
    use tokio_stream::StreamExt as _;
    use tokio_stream::wrappers::ReceiverStream;

    let listen: SocketAddr = "127.0.0.1:18682".parse().unwrap();

    let observer = observe::start();
    tokio::spawn(proxy::serve(listen, upstream(), observer.sink.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Setup + publish through the proxy via the high-level client.
    let client = poller::connect("test-project", "127.0.0.1:18682")
        .await
        .expect("connect through proxy");
    let topic = match client.create_topic("pst-stream-topic", None, None).await {
        Ok(topic) => topic,
        Err(_) => client.topic("pst-stream-topic"),
    };
    let sub = client.subscription("pst-stream-sub");
    let _ = sub
        .create(
            topic.fully_qualified_name(),
            SubscriptionConfig::default(),
            None,
        )
        .await;
    let sub_name = sub.fully_qualified_name().to_owned();

    let publisher = topic.new_publisher(None);
    let batch: Vec<PubsubMessage> = (0..3)
        .map(|i| PubsubMessage {
            data: format!("stream-{i}").into_bytes(),
            ..Default::default()
        })
        .collect();
    publisher
        .publish_immediately(batch, None)
        .await
        .expect("publish through proxy");

    // Open a streaming pull with a raw client whose request stream we drive.
    let channel = tonic::transport::Channel::from_shared("http://127.0.0.1:18682".to_owned())
        .unwrap()
        .connect()
        .await
        .expect("raw client connects to proxy");
    let mut raw = SubscriberClient::new(channel);
    let (requests, request_rx) = tokio::sync::mpsc::channel::<StreamingPullRequest>(8);
    requests
        .send(StreamingPullRequest {
            subscription: sub_name.clone(),
            stream_ack_deadline_seconds: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut responses = raw
        .streaming_pull(ReceiverStream::new(request_rx))
        .await
        .expect("streaming pull through proxy")
        .into_inner();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        observer.snapshots.borrow().connected_consumers() >= 1,
        "a live streaming consumer was observed"
    );

    // Receive at least one delivery and acknowledge it over the same stream.
    let mut acked = false;
    for _ in 0..10 {
        match tokio::time::timeout(Duration::from_secs(3), responses.next()).await {
            Ok(Some(Ok(response))) => {
                let ack_ids: Vec<String> = response
                    .received_messages
                    .iter()
                    .map(|m| m.ack_id.clone())
                    .collect();
                if !ack_ids.is_empty() {
                    requests
                        .send(StreamingPullRequest {
                            ack_ids,
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                    acked = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(acked, "a message was delivered and acked over the stream");

    tokio::time::sleep(Duration::from_millis(300)).await;
    {
        let snapshot = observer.snapshots.borrow();
        let sub_state = snapshot
            .subscriptions
            .get(&sub_name)
            .expect("subscription observed via proxy");
        assert!(sub_state.delivered >= 1, "streamed deliveries observed");
        assert!(sub_state.acked >= 1, "streamed acks observed");
    }

    // End the request stream and drop the response stream: the consumer closes.
    drop(requests);
    drop(responses);
    let mut connected = u64::MAX;
    for _ in 0..50 {
        connected = observer.snapshots.borrow().connected_consumers();
        if connected == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(connected, 0, "consumer closed after the stream ended");
}
