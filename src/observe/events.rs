//! Observation events emitted by the proxy and the admin poller, plus the sink
//! used to deliver them to the single state-owning task.
//!
//! Proxy handlers are on the hot data path, so the sink never blocks: if the
//! channel is momentarily full the observation is dropped (and traced). Dropped
//! observations only ever cost a little accuracy in the displayed counters; they
//! never affect the traffic being forwarded.

use std::net::SocketAddr;

use tokio::sync::mpsc;

/// A single message captured from a `Publish` call: its (possibly truncated)
/// payload and attributes. The proxy caps `data` at the configured payload limit,
/// recording the original size and whether truncation happened so the UI can say
/// so.
#[derive(Debug, Clone)]
pub struct PublishedMessage {
    /// The message body, truncated to the proxy's payload cap.
    pub data: Vec<u8>,
    /// The message attributes, in the order the client supplied them.
    pub attributes: Vec<(String, String)>,
    /// The payload's true length before any truncation.
    pub original_len: usize,
    /// Whether `data` was truncated to fit the payload cap.
    pub truncated: bool,
}

/// A single thing the proxy or poller observed.
#[derive(Debug, Clone)]
pub enum Observation {
    /// The `messages` published to `topic` by the client at `peer`. Carries the
    /// captured payloads, one per published message; `messages.len()` is the
    /// count that feeds the per-topic/per-publisher totals.
    Publish {
        topic: String,
        peer: Option<SocketAddr>,
        messages: Vec<PublishedMessage>,
    },
    /// `messages` were delivered to a consumer of `subscription` at `peer`.
    Deliver {
        subscription: String,
        peer: Option<SocketAddr>,
        messages: u64,
    },
    /// `messages` were acknowledged on `subscription` by the consumer at `peer`.
    Ack {
        subscription: String,
        peer: Option<SocketAddr>,
        messages: u64,
    },
    /// A streaming consumer opened a connection to `subscription`.
    ConsumerOpen {
        subscription: String,
        peer: Option<SocketAddr>,
    },
    /// A streaming consumer's connection to `subscription` closed.
    ConsumerClose {
        subscription: String,
        peer: Option<SocketAddr>,
    },
    /// The current set of topics and subscriptions, from the admin poller.
    AdminSnapshot {
        topics: Vec<String>,
        subscriptions: Vec<SubscriptionInfo>,
    },
}

/// Metadata for a subscription as reported by the admin poller.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// Fully-qualified subscription name.
    pub name: String,
    /// Fully-qualified name of the topic this subscription is attached to.
    pub topic: String,
}

/// A cheap, cloneable handle for emitting [`Observation`]s without blocking.
#[derive(Clone)]
pub struct ObservationSink {
    tx: mpsc::Sender<Observation>,
}

impl ObservationSink {
    pub(super) fn new(tx: mpsc::Sender<Observation>) -> Self {
        Self { tx }
    }

    /// Record an observation. Never blocks; drops on a full channel.
    pub fn observe(&self, observation: Observation) {
        if let Err(err) = self.tx.try_send(observation) {
            tracing::trace!(%err, "dropped observation (state channel saturated)");
        }
    }
}

/// Render a peer address as a stable identity key for a publisher/consumer.
pub fn peer_key(peer: Option<SocketAddr>) -> String {
    peer.map_or_else(|| "unknown".to_owned(), |addr| addr.to_string())
}
