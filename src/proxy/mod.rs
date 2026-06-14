//! The transparent gRPC interception proxy.
//!
//! Serves the Pub/Sub `Publisher` and `Subscriber` services on the local listen
//! address and forwards every call to the real upstream server, observing the
//! traffic in passing. A client-under-test that points its `PUBSUB_EMULATOR_HOST`
//! at this proxy behaves identically to one talking to the server directly.

mod forward;
mod publisher;
mod subscriber;

use std::net::SocketAddr;

use tonic::transport::{Channel, Server};

use crate::observe::ObservationSink;
use crate::pb::publisher_server::PublisherServer;
use crate::pb::subscriber_server::SubscriberServer;
use publisher::ProxyPublisher;
use subscriber::ProxySubscriber;

/// Serve the proxy until the process exits, forwarding to `upstream`.
pub async fn serve(
    listen: SocketAddr,
    upstream: String,
    sink: ObservationSink,
) -> anyhow::Result<()> {
    // Lazy connection: the proxy can start before the upstream is reachable and
    // will (re)connect on demand, matching the emulator-first workflow.
    let channel = Channel::from_shared(format!("http://{upstream}"))?.connect_lazy();

    let publisher = ProxyPublisher::new(channel.clone(), sink.clone());
    let subscriber = ProxySubscriber::new(channel, sink);

    tracing::info!(%listen, %upstream, "proxy listening");
    Server::builder()
        .add_service(PublisherServer::new(publisher))
        .add_service(SubscriberServer::new(subscriber))
        .serve(listen)
        .await?;

    Ok(())
}
