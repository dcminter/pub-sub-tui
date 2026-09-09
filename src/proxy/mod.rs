//! The transparent gRPC interception proxy.
//!
//! Serves the Pub/Sub `Publisher` and `Subscriber` services on the local listen
//! address and forwards every call to the real upstream server, observing the
//! traffic in passing. A client-under-test that points its `PUBSUB_EMULATOR_HOST`
//! at this proxy behaves identically to one talking to the server directly.
//!
//! Pub/Sub speaks two protocols on one port — gRPC over HTTP/2 and REST/JSON over
//! HTTP/1.1 — and clients pick either. The listener therefore accepts both: gRPC
//! paths (`/google.pubsub.v1.Publisher/...`) reach the service implementations
//! here, and everything else falls through to the [`rest`] relay.

mod forward;
mod publisher;
mod rest;
mod subscriber;

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::codec::CompressionEncoding;
use tonic::service::Routes;
use tonic::transport::{Channel, Server};

use crate::observe::ObservationSink;
use crate::pb::publisher_server::PublisherServer;
use crate::pb::subscriber_server::SubscriberServer;
use publisher::ProxyPublisher;
use rest::RestProxy;
use subscriber::ProxySubscriber;

/// Per-message gRPC size limit applied to every encoder/decoder in the proxy.
///
/// tonic defaults to 4 MiB, but Pub/Sub permits publish requests up to ~10 MB. A
/// transparent proxy must not be stricter than the upstream it fronts: otherwise a
/// large (but valid) request the server would accept fails at the proxy instead.
/// That failed RPC has knock-on effects in clients — an ordered publisher, for
/// instance, poisons the ordering key and then raises `OrderingKeyError` on every
/// later message for that key. We set the limit well above Pub/Sub's own cap so the
/// upstream, not the proxy, is always the one to enforce a ceiling.
pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Serve the proxy until the process exits, forwarding to `upstream`. `payload_cap`
/// bounds how many bytes of each published message are captured for the UI's
/// recent-messages view.
pub async fn serve(
    listen: SocketAddr,
    upstream: String,
    sink: ObservationSink,
    payload_cap: usize,
) -> anyhow::Result<()> {
    // Lazy connection: the proxy can start before the upstream is reachable and
    // will (re)connect on demand, matching the emulator-first workflow.
    let channel = Channel::from_shared(format!("http://{upstream}"))?.connect_lazy();

    // The REST relay handles every request the gRPC routes below do not claim.
    let rest = Arc::new(RestProxy::new(upstream.clone(), sink.clone(), payload_cap));

    let publisher = ProxyPublisher::new(channel.clone(), sink.clone(), payload_cap);
    let subscriber = ProxySubscriber::new(channel, sink);

    let routes = Routes::new(
        PublisherServer::new(publisher)
            // Pub/Sub clients gzip request payloads (e.g. the Ruby gem's async
            // publisher with `compress: true`); a transparent proxy must decode
            // them or the request body never finishes decoding and the handler is
            // never reached — the publish then stalls.
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE),
    )
    .add_service(
        SubscriberServer::new(subscriber)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE),
    );

    let router = routes
        .into_axum_router()
        .fallback(move |request| Arc::clone(&rest).handle(request));

    tracing::info!(%listen, %upstream, "proxy listening (gRPC and REST)");
    // REST clients speak HTTP/1.1; without `accept_http1` the connection is
    // answered with an HTTP/2 preface the client cannot make sense of, and the
    // request fails before it reaches any handler. gRPC still uses HTTP/2.
    let mut server = Server::builder().accept_http1(true);
    server
        .add_routes(Routes::from(router))
        .serve(listen)
        .await?;

    Ok(())
}
