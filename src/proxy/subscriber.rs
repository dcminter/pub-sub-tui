//! The proxy implementation of the Pub/Sub `Subscriber` service.
//!
//! All RPCs are forwarded faithfully. Several are tapped to build the consumer
//! view of the world:
//! - `Pull` counts delivered messages; `Acknowledge` counts consumed (acked) ones.
//! - `StreamingPull` (bidirectional) is actively pumped in both directions so the
//!   subscription can be latched, acks and deliveries counted, and the live
//!   consumer connection opened/closed — detecting a disconnect on either side
//!   promptly (a passive forward misses the close of an otherwise-idle stream).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::observe::{Observation, ObservationSink};
use crate::pb;
use crate::pb::subscriber_client::SubscriberClient;
use crate::proxy::MAX_MESSAGE_SIZE;
use crate::proxy::forward::proxy_service;

/// Buffer between the two halves of a proxied `StreamingPull`.
const STREAM_BUFFER: usize = 64;

/// Forwards `Subscriber` RPCs to the upstream server while observing traffic.
pub struct ProxySubscriber {
    upstream: SubscriberClient<Channel>,
    sink: ObservationSink,
}

impl ProxySubscriber {
    pub fn new(channel: Channel, sink: ObservationSink) -> Self {
        Self {
            upstream: SubscriberClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
            sink,
        }
    }

    fn upstream(&self) -> SubscriberClient<Channel> {
        self.upstream.clone()
    }
}

proxy_service! {
    service = crate::pb::subscriber_server::Subscriber;
    proxy = ProxySubscriber;
    accessor = upstream;
    forward {
        create_subscription(pb::Subscription) -> pb::Subscription;
        get_subscription(pb::GetSubscriptionRequest) -> pb::Subscription;
        update_subscription(pb::UpdateSubscriptionRequest) -> pb::Subscription;
        list_subscriptions(pb::ListSubscriptionsRequest) -> pb::ListSubscriptionsResponse;
        delete_subscription(pb::DeleteSubscriptionRequest) -> ();
        modify_ack_deadline(pb::ModifyAckDeadlineRequest) -> ();
        modify_push_config(pb::ModifyPushConfigRequest) -> ();
        get_snapshot(pb::GetSnapshotRequest) -> pb::Snapshot;
        list_snapshots(pb::ListSnapshotsRequest) -> pb::ListSnapshotsResponse;
        create_snapshot(pb::CreateSnapshotRequest) -> pb::Snapshot;
        update_snapshot(pb::UpdateSnapshotRequest) -> pb::Snapshot;
        delete_snapshot(pb::DeleteSnapshotRequest) -> ();
        seek(pb::SeekRequest) -> pb::SeekResponse;
    }
    custom {
        type StreamingPullStream = ReceiverStream<Result<pb::StreamingPullResponse, Status>>;

        async fn pull(
            &self,
            request: Request<pb::PullRequest>,
        ) -> Result<Response<pb::PullResponse>, Status> {
            let peer = request.remote_addr();
            let subscription = request.get_ref().subscription.clone();

            let response = self.upstream().pull(request).await?;

            let messages = response.get_ref().received_messages.len() as u64;
            self.sink.observe(Observation::Deliver {
                subscription,
                peer,
                messages,
            });
            Ok(response)
        }

        async fn acknowledge(
            &self,
            request: Request<pb::AcknowledgeRequest>,
        ) -> Result<Response<()>, Status> {
            let peer = request.remote_addr();
            let subscription = request.get_ref().subscription.clone();
            let messages = request.get_ref().ack_ids.len() as u64;

            let response = self.upstream().acknowledge(request).await?;

            self.sink.observe(Observation::Ack {
                subscription,
                peer,
                messages,
            });
            Ok(response)
        }

        async fn streaming_pull(
            &self,
            request: Request<tonic::Streaming<pb::StreamingPullRequest>>,
        ) -> Result<Response<Self::StreamingPullStream>, Status> {
            let peer = request.remote_addr();
            // Preserve the client's initial-frame metadata across the proxy hop:
            // `StreamingPull` carries routing (`x-goog-request-params`) and, against
            // real Pub/Sub, the bearer token there. `into_inner()` would discard it,
            // and a request built from a bare stream starts with empty metadata.
            let metadata = request.metadata().clone();
            let mut inbound = request.into_inner();
            let mut upstream_client = self.upstream();
            let sink = self.sink.clone();
            let subscription = Arc::new(Mutex::new(String::new()));
            let opened = Arc::new(AtomicBool::new(false));

            let (req_tx, req_rx) = mpsc::channel::<pb::StreamingPullRequest>(STREAM_BUFFER);
            let (resp_tx, resp_rx) =
                mpsc::channel::<Result<pb::StreamingPullResponse, Status>>(STREAM_BUFFER);

            tokio::spawn(async move {
                // Pump client→server: latch the subscription, emit open once, count
                // acks, and forward decoded frames upstream.
                let inbound_pump = {
                    let sink = sink.clone();
                    let subscription = subscription.clone();
                    let opened = opened.clone();
                    async move {
                        while let Some(frame) = inbound.next().await {
                            let Ok(req) = frame else { break };
                            if !req.subscription.is_empty() {
                                *subscription.lock().unwrap_or_else(PoisonError::into_inner) =
                                    req.subscription.clone();
                                if !opened.swap(true, Ordering::SeqCst) {
                                    sink.observe(Observation::ConsumerOpen {
                                        subscription: req.subscription.clone(),
                                        peer,
                                    });
                                }
                            }
                            if !req.ack_ids.is_empty() {
                                let subscription =
                                    subscription.lock().unwrap_or_else(PoisonError::into_inner).clone();
                                sink.observe(Observation::Ack {
                                    subscription,
                                    peer,
                                    messages: req.ack_ids.len() as u64,
                                });
                            }
                            if req_tx.send(req).await.is_err() {
                                break;
                            }
                        }
                    }
                };

                // Pump server→client: establish the upstream stream (fed by the
                // request channel, concurrently, to avoid a bidi deadlock) and
                // forward responses while counting deliveries.
                let outbound_pump = {
                    let sink = sink.clone();
                    let subscription = subscription.clone();
                    async move {
                        let mut upstream_req = Request::new(ReceiverStream::new(req_rx));
                        *upstream_req.metadata_mut() = metadata;
                        let upstream = match upstream_client
                            .streaming_pull(upstream_req)
                            .await
                        {
                            Ok(upstream) => upstream,
                            Err(status) => {
                                let _ = resp_tx.send(Err(status)).await;
                                return;
                            }
                        };
                        let mut upstream_in = upstream.into_inner();
                        while let Some(frame) = upstream_in.next().await {
                            match frame {
                                Ok(resp) => {
                                    let messages = resp.received_messages.len() as u64;
                                    if messages > 0 {
                                        let subscription =
                                    subscription.lock().unwrap_or_else(PoisonError::into_inner).clone();
                                        sink.observe(Observation::Deliver {
                                            subscription,
                                            peer,
                                            messages,
                                        });
                                    }
                                    if resp_tx.send(Ok(resp)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(status) => {
                                    let _ = resp_tx.send(Err(status)).await;
                                    break;
                                }
                            }
                        }
                    }
                };

                // Whichever direction ends first tears down the other.
                tokio::select! {
                    () = inbound_pump => {}
                    () = outbound_pump => {}
                }

                if opened.load(Ordering::SeqCst) {
                    let subscription = subscription.lock().unwrap_or_else(PoisonError::into_inner).clone();
                    sink.observe(Observation::ConsumerClose { subscription, peer });
                }
            });

            Ok(Response::new(ReceiverStream::new(resp_rx)))
        }
    }
}
