//! The proxy implementation of the Pub/Sub `Publisher` service.
//!
//! All RPCs are forwarded faithfully to the upstream server. `Publish` is also
//! tapped to count messages per topic and attribute them to the publishing peer.

use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::observe::{Observation, ObservationSink};
use crate::pb;
use crate::pb::publisher_client::PublisherClient;
use crate::proxy::MAX_MESSAGE_SIZE;
use crate::proxy::forward::proxy_service;

/// Forwards `Publisher` RPCs to the upstream server while observing `Publish`.
pub struct ProxyPublisher {
    upstream: PublisherClient<Channel>,
    sink: ObservationSink,
}

impl ProxyPublisher {
    pub fn new(channel: Channel, sink: ObservationSink) -> Self {
        Self {
            upstream: PublisherClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
            sink,
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
            let messages = request.get_ref().messages.len() as u64;

            let response = self.upstream().publish(request).await?;

            self.sink.observe(Observation::Publish {
                topic,
                peer,
                messages,
            });
            Ok(response)
        }
    }
}
