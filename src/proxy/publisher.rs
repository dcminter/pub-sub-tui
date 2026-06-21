//! The proxy implementation of the Pub/Sub `Publisher` service.
//!
//! All RPCs are forwarded faithfully to the upstream server. `Publish` is also
//! tapped to count messages per topic and attribute them to the publishing peer.

use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::observe::{Observation, ObservationSink, PublishedMessage};
use crate::pb;
use crate::pb::publisher_client::PublisherClient;
use crate::proxy::MAX_MESSAGE_SIZE;
use crate::proxy::forward::proxy_service;

/// Forwards `Publisher` RPCs to the upstream server while observing `Publish`.
pub struct ProxyPublisher {
    upstream: PublisherClient<Channel>,
    sink: ObservationSink,
    /// Per-message payload bytes captured for the recent-messages view; larger
    /// payloads are truncated to this many bytes.
    payload_cap: usize,
}

impl ProxyPublisher {
    pub fn new(channel: Channel, sink: ObservationSink, payload_cap: usize) -> Self {
        Self {
            upstream: PublisherClient::new(channel)
                .accept_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
            sink,
            payload_cap,
        }
    }

    /// Snapshot a published message's payload (capped) and attributes for the
    /// recent-messages view, without disturbing the request being forwarded.
    fn capture(&self, message: &pb::PubsubMessage) -> PublishedMessage {
        let original_len = message.data.len();
        let truncated = original_len > self.payload_cap;
        let data = message.data[..original_len.min(self.payload_cap)].to_vec();
        let attributes = message
            .attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        PublishedMessage {
            data,
            attributes,
            original_len,
            truncated,
        }
    }

    /// A cloned client sharing the underlying (multiplexed) channel.
    fn upstream(&self) -> PublisherClient<Channel> {
        self.upstream.clone()
    }
}

proxy_service! {
    service = crate::pb::publisher_server::Publisher;
    proxy = ProxyPublisher;
    accessor = upstream;
    forward {
        create_topic(pb::Topic) -> pb::Topic;
        update_topic(pb::UpdateTopicRequest) -> pb::Topic;
        get_topic(pb::GetTopicRequest) -> pb::Topic;
        list_topics(pb::ListTopicsRequest) -> pb::ListTopicsResponse;
        list_topic_subscriptions(pb::ListTopicSubscriptionsRequest)
            -> pb::ListTopicSubscriptionsResponse;
        list_topic_snapshots(pb::ListTopicSnapshotsRequest) -> pb::ListTopicSnapshotsResponse;
        delete_topic(pb::DeleteTopicRequest) -> ();
        detach_subscription(pb::DetachSubscriptionRequest) -> pb::DetachSubscriptionResponse;
    }
    custom {
        async fn publish(
            &self,
            request: Request<pb::PublishRequest>,
        ) -> Result<Response<pb::PublishResponse>, Status> {
            let peer = request.remote_addr();
            let topic = request.get_ref().topic.clone();
            let messages: Vec<PublishedMessage> = request
                .get_ref()
                .messages
                .iter()
                .map(|message| self.capture(message))
                .collect();

            // Distinct ordering keys in this batch: a single rejected publish
            // poisons its ordering key client-side, so logging the keys alongside
            // any error status pinpoints which key (if any) was affected.
            let ordering_keys: std::collections::BTreeSet<String> = request
                .get_ref()
                .messages
                .iter()
                .map(|message| message.ordering_key.clone())
                .filter(|key| !key.is_empty())
                .collect();

            let message_count = messages.len();
            let response = match self.upstream().publish(request).await {
                Ok(response) => {
                    tracing::debug!(
                        rpc = "publish",
                        %topic,
                        messages = message_count,
                        message_ids = response.get_ref().message_ids.len(),
                        ordering_keys = ?ordering_keys,
                        "upstream Publish succeeded",
                    );
                    response
                }
                Err(status) => {
                    tracing::warn!(
                        rpc = "publish",
                        %topic,
                        code = ?status.code(),
                        message = status.message(),
                        ordering_keys = ?ordering_keys,
                        "upstream Publish returned error status",
                    );
                    return Err(status);
                }
            };

            self.sink.observe(Observation::Publish {
                topic,
                peer,
                messages,
            });
            Ok(response)
        }
    }
}
