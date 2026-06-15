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
use pub_sub_tui::{observe, pb, poller, proxy};
use tonic::{Request, Response, Status};

fn upstream() -> String {
    std::env::var("PUBSUB_TEST_UPSTREAM").unwrap_or_else(|_| "localhost:8085".to_owned())
}

#[tokio::test]
#[ignore = "requires a running Pub/Sub emulator"]
async fn proxy_forwards_and_observes_traffic() {
    const MESSAGES: usize = 5;
    let listen: SocketAddr = "127.0.0.1:18681".parse().unwrap();

    // Proxy in front of the emulator, feeding a fresh observer.
    let observer = observe::start(200);
    tokio::spawn(proxy::serve(
        listen,
        upstream(),
        observer.sink.clone(),
        64 * 1024,
    ));
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

// Publishes a payload larger than tonic's default 4 MiB decode limit but within
// Pub/Sub's ~10 MB per-request limit. Such a publish succeeds against the emulator
// directly, so if it fails *through the proxy* the proxy is imposing a stricter
// limit of its own — which surfaces in clients as a (secondary) failure: e.g. an
// ordered publisher poisons the ordering key after the failed RPC and then raises
// `OrderingKeyError` on every subsequent message for that key.
#[tokio::test]
#[ignore = "requires a running Pub/Sub emulator"]
async fn proxy_forwards_large_publish() {
    // Comfortably over tonic's 4 MiB default, comfortably under Pub/Sub's 10 MB cap.
    const PAYLOAD: usize = 6 * 1024 * 1024;
    let listen: SocketAddr = "127.0.0.1:18683".parse().unwrap();

    let observer = observe::start(200);
    tokio::spawn(proxy::serve(
        listen,
        upstream(),
        observer.sink.clone(),
        64 * 1024,
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let client = poller::connect("test-project", "127.0.0.1:18683")
        .await
        .expect("connect through proxy");
    let topic = match client.create_topic("pst-large-topic", None, None).await {
        Ok(topic) => topic,
        Err(_) => client.topic("pst-large-topic"),
    };

    let publisher = topic.new_publisher(None);
    let message = PubsubMessage {
        data: vec![b'x'; PAYLOAD],
        ..Default::default()
    };
    publisher
        .publish_immediately(vec![message], None)
        .await
        .expect("large publish forwarded through proxy");
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

    let observer = observe::start(200);
    tokio::spawn(proxy::serve(
        listen,
        upstream(),
        observer.sink.clone(),
        64 * 1024,
    ));
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

// A minimal upstream `Subscriber` whose `streaming_pull` records the inbound
// `x-goog-request-params` metadata, so we can assert the proxy forwarded it across
// the hop. Every other RPC is an `unimplemented` stub — the test never calls them.
#[derive(Clone, Default)]
struct MetadataCapturingSubscriber {
    request_params: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

#[tonic::async_trait]
impl pb::subscriber_server::Subscriber for MetadataCapturingSubscriber {
    type StreamingPullStream =
        tokio_stream::wrappers::ReceiverStream<Result<pb::StreamingPullResponse, Status>>;

    async fn streaming_pull(
        &self,
        request: Request<tonic::Streaming<pb::StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let params = request
            .metadata()
            .get("x-goog-request-params")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        *self
            .request_params
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = params;
        // An immediately-empty response stream: dropping the sender when this
        // handler returns closes the channel, ending the stream.
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn pull(
        &self,
        _: Request<pb::PullRequest>,
    ) -> Result<Response<pb::PullResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn acknowledge(
        &self,
        _: Request<pb::AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn create_subscription(
        &self,
        _: Request<pb::Subscription>,
    ) -> Result<Response<pb::Subscription>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn get_subscription(
        &self,
        _: Request<pb::GetSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn update_subscription(
        &self,
        _: Request<pb::UpdateSubscriptionRequest>,
    ) -> Result<Response<pb::Subscription>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn list_subscriptions(
        &self,
        _: Request<pb::ListSubscriptionsRequest>,
    ) -> Result<Response<pb::ListSubscriptionsResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn delete_subscription(
        &self,
        _: Request<pb::DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn modify_ack_deadline(
        &self,
        _: Request<pb::ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn modify_push_config(
        &self,
        _: Request<pb::ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn get_snapshot(
        &self,
        _: Request<pb::GetSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn list_snapshots(
        &self,
        _: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn create_snapshot(
        &self,
        _: Request<pb::CreateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn update_snapshot(
        &self,
        _: Request<pb::UpdateSnapshotRequest>,
    ) -> Result<Response<pb::Snapshot>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn delete_snapshot(
        &self,
        _: Request<pb::DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("test stub"))
    }
    async fn seek(
        &self,
        _: Request<pb::SeekRequest>,
    ) -> Result<Response<pb::SeekResponse>, Status> {
        Err(Status::unimplemented("test stub"))
    }
}

// The proxy must forward the client's initial-frame metadata on `StreamingPull`
// (routing via `x-goog-request-params`, and the bearer token against real Pub/Sub).
// Drives a raw client through the proxy to an in-process upstream that records what
// it received — no emulator needed.
#[tokio::test]
async fn proxy_forwards_streaming_pull_metadata() {
    use pub_sub_tui::pb::StreamingPullRequest;
    use pub_sub_tui::pb::subscriber_client::SubscriberClient;
    use pub_sub_tui::pb::subscriber_server::SubscriberServer;
    use tokio_stream::StreamExt as _;
    use tokio_stream::wrappers::ReceiverStream;

    let upstream_addr: SocketAddr = "127.0.0.1:18684".parse().unwrap();
    let proxy_addr: SocketAddr = "127.0.0.1:18685".parse().unwrap();
    let params = "subscription=projects/p/subscriptions/s";

    // Echo upstream that records the streaming metadata it receives.
    let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
    let echo = MetadataCapturingSubscriber {
        request_params: captured.clone(),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SubscriberServer::new(echo))
            .serve(upstream_addr)
            .await
            .unwrap();
    });

    // Proxy in front of the echo upstream.
    let observer = observe::start(200);
    tokio::spawn(proxy::serve(
        proxy_addr,
        upstream_addr.to_string(),
        observer.sink.clone(),
        64 * 1024,
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Raw client → proxy, carrying x-goog-request-params on the streaming request.
    let channel = tonic::transport::Channel::from_shared(format!("http://{proxy_addr}"))
        .unwrap()
        .connect()
        .await
        .expect("raw client connects to proxy");
    let mut raw = SubscriberClient::new(channel);
    let (requests, request_rx) = tokio::sync::mpsc::channel::<StreamingPullRequest>(8);
    let mut request = Request::new(ReceiverStream::new(request_rx));
    request
        .metadata_mut()
        .insert("x-goog-request-params", params.parse().unwrap());
    let mut responses = raw
        .streaming_pull(request)
        .await
        .expect("streaming pull through proxy")
        .into_inner();

    // Send the initial frame so the stream is established end-to-end.
    requests
        .send(StreamingPullRequest {
            subscription: "projects/p/subscriptions/s".to_owned(),
            stream_ack_deadline_seconds: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    // Keep draining the (empty) response stream in the background so the proxy's
    // upstream call is driven to completion, while we poll for the capture. Polling
    // (rather than a single timed read) keeps the test deterministic under load.
    tokio::spawn(async move { while responses.next().await.is_some() {} });

    let mut got = None;
    for _ in 0..120 {
        got = captured
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if got.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        got.as_deref(),
        Some(params),
        "x-goog-request-params forwarded across the proxy's streaming-pull hop"
    );
}
