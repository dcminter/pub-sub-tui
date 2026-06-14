//! The authoritative application state and the single task that owns it.
//!
//! Every mutation happens here, driven by [`Observation`]s drained from an mpsc
//! channel. After applying a burst of observations the task publishes an
//! immutable snapshot through a [`watch`] channel that the TUI reads at its own
//! cadence. This keeps the hot proxy path lock-free and gives the UI a cheap,
//! always-latest view.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::events::{Observation, ObservationSink, SubscriptionInfo, peer_key};

/// A publisher is considered "connected" if it has published within this window.
/// (Pub/Sub `Publish` is a unary call with no long-lived connection, so liveness
/// is defined as recent activity rather than an open socket.)
pub const PUBLISHER_ACTIVE_WINDOW: Duration = Duration::from_secs(10);

/// Per-topic observed and metadata state.
#[derive(Debug, Clone, Default)]
pub struct Topic {
    /// Whether the admin poller currently sees this topic.
    pub exists: bool,
    /// Total messages observed published to this topic.
    pub publish_count: u64,
    /// Per-publisher (peer) published message counts.
    pub publishers: BTreeMap<String, Publisher>,
}

/// A distinct publisher (identified by peer address) to a topic.
#[derive(Debug, Clone)]
pub struct Publisher {
    pub published: u64,
    pub last_seen: Instant,
}

/// Per-subscription observed and metadata state.
#[derive(Debug, Clone, Default)]
pub struct Subscription {
    /// Whether the admin poller currently sees this subscription.
    pub exists: bool,
    /// Fully-qualified parent topic name, once known from the poller.
    pub topic: Option<String>,
    /// Total messages delivered to consumers of this subscription.
    pub delivered: u64,
    /// Total messages acknowledged (i.e. consumed) on this subscription.
    pub acked: u64,
    /// Currently-open streaming consumer connections.
    pub live_consumers: u64,
}

/// The complete observable state of the monitored instance.
#[derive(Debug, Clone, Default)]
pub struct AppState {
    pub topics: BTreeMap<String, Topic>,
    pub subscriptions: BTreeMap<String, Subscription>,
}

impl AppState {
    fn topic_mut(&mut self, name: &str) -> &mut Topic {
        self.topics.entry(name.to_owned()).or_default()
    }

    fn subscription_mut(&mut self, name: &str) -> &mut Subscription {
        self.subscriptions.entry(name.to_owned()).or_default()
    }

    /// Fold a single observation into the state. `now` timestamps publisher
    /// activity for the liveness window.
    pub fn apply(&mut self, observation: Observation, now: Instant) {
        match observation {
            Observation::Publish {
                topic,
                peer,
                messages,
            } => {
                let topic = self.topic_mut(&topic);
                topic.publish_count += messages;
                let publisher = topic.publishers.entry(peer_key(peer)).or_insert(Publisher {
                    published: 0,
                    last_seen: now,
                });
                publisher.published += messages;
                publisher.last_seen = now;
            }
            Observation::Deliver {
                subscription,
                peer: _,
                messages,
            } => {
                self.subscription_mut(&subscription).delivered += messages;
            }
            Observation::Ack {
                subscription,
                peer: _,
                messages,
            } => {
                self.subscription_mut(&subscription).acked += messages;
            }
            Observation::ConsumerOpen {
                subscription,
                peer: _,
            } => {
                self.subscription_mut(&subscription).live_consumers += 1;
            }
            Observation::ConsumerClose {
                subscription,
                peer: _,
            } => {
                let sub = self.subscription_mut(&subscription);
                sub.live_consumers = sub.live_consumers.saturating_sub(1);
            }
            Observation::AdminSnapshot {
                topics,
                subscriptions,
            } => self.apply_admin(topics, subscriptions),
        }
    }

    /// Reconcile the topic/subscription existence and topic mapping with the
    /// latest admin listing. Observed counters are preserved across snapshots.
    fn apply_admin(&mut self, topics: Vec<String>, subscriptions: Vec<SubscriptionInfo>) {
        for topic in self.topics.values_mut() {
            topic.exists = false;
        }
        for name in topics {
            self.topic_mut(&name).exists = true;
        }

        for sub in self.subscriptions.values_mut() {
            sub.exists = false;
        }
        for info in subscriptions {
            let sub = self.subscription_mut(&info.name);
            sub.exists = true;
            sub.topic = Some(info.topic);
        }
    }

    /// Number of topics currently reported by the admin poller.
    pub fn topic_count(&self) -> usize {
        self.topics.values().filter(|t| t.exists).count()
    }

    /// Number of publishers seen publishing within [`PUBLISHER_ACTIVE_WINDOW`].
    pub fn connected_publishers(&self, now: Instant) -> usize {
        self.topics
            .values()
            .flat_map(|t| t.publishers.values())
            .filter(|p| now.duration_since(p.last_seen) <= PUBLISHER_ACTIVE_WINDOW)
            .count()
    }

    /// Number of currently-open streaming consumer connections.
    pub fn connected_consumers(&self) -> u64 {
        self.subscriptions.values().map(|s| s.live_consumers).sum()
    }

    /// Subscriptions attached to `topic` that have consumed at least one message.
    pub fn active_consumers_of<'a>(
        &'a self,
        topic: &'a str,
    ) -> impl Iterator<Item = (&'a String, &'a Subscription)> {
        self.subscriptions
            .iter()
            .filter(move |(_, s)| s.topic.as_deref() == Some(topic) && s.acked > 0)
    }
}

/// Handle returned by [`start`]: emit observations via `sink`, read snapshots via
/// `snapshots`.
pub struct Observer {
    pub sink: ObservationSink,
    pub snapshots: watch::Receiver<Arc<AppState>>,
}

/// Spawn the state-owning task and return handles to feed and read it.
pub fn start() -> Observer {
    let (tx, rx) = mpsc::channel(8192);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(AppState::default()));
    tokio::spawn(run(rx, snap_tx));
    Observer {
        sink: ObservationSink::new(tx),
        snapshots: snap_rx,
    }
}

async fn run(mut rx: mpsc::Receiver<Observation>, snap_tx: watch::Sender<Arc<AppState>>) {
    let mut state = AppState::default();
    while let Some(first) = rx.recv().await {
        let now = Instant::now();
        state.apply(first, now);
        // Coalesce any other immediately-available observations into one snapshot.
        while let Ok(observation) = rx.try_recv() {
            state.apply(observation, now);
        }
        if snap_tx.send(Arc::new(state.clone())).is_err() {
            break; // all readers gone; nothing left to update
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::observe::SubscriptionInfo;

    fn peer(s: &str) -> Option<SocketAddr> {
        Some(s.parse().unwrap())
    }

    fn admin(topics: &[&str], subs: &[(&str, &str)]) -> Observation {
        Observation::AdminSnapshot {
            topics: topics.iter().map(|s| (*s).to_owned()).collect(),
            subscriptions: subs
                .iter()
                .map(|(name, topic)| SubscriptionInfo {
                    name: (*name).to_owned(),
                    topic: (*topic).to_owned(),
                })
                .collect(),
        }
    }

    #[test]
    fn publish_accumulates_per_topic_and_per_publisher() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(
            Observation::Publish {
                topic: "t1".into(),
                peer: peer("127.0.0.1:5000"),
                messages: 3,
            },
            now,
        );
        state.apply(
            Observation::Publish {
                topic: "t1".into(),
                peer: peer("127.0.0.1:5000"),
                messages: 2,
            },
            now,
        );
        state.apply(
            Observation::Publish {
                topic: "t1".into(),
                peer: peer("127.0.0.1:6000"),
                messages: 1,
            },
            now,
        );

        let topic = &state.topics["t1"];
        assert_eq!(topic.publish_count, 6);
        assert_eq!(topic.publishers["127.0.0.1:5000"].published, 5);
        assert_eq!(topic.publishers["127.0.0.1:6000"].published, 1);
        assert_eq!(state.connected_publishers(now), 2);
    }

    #[test]
    fn ack_counts_as_consumed_and_filters_zero() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(admin(&["t1"], &[("s1", "t1"), ("s2", "t1")]), now);
        state.apply(
            Observation::Ack {
                subscription: "s1".into(),
                peer: peer("127.0.0.1:7000"),
                messages: 4,
            },
            now,
        );

        assert_eq!(state.subscriptions["s1"].acked, 4);
        // Only s1 has consumed > 0, so only it is an "active consumer" of t1.
        let active: Vec<_> = state
            .active_consumers_of("t1")
            .map(|(n, _)| n.clone())
            .collect();
        assert_eq!(active, vec!["s1".to_owned()]);
    }

    #[test]
    fn live_consumers_track_open_and_close() {
        let now = Instant::now();
        let mut state = AppState::default();
        let open = |sub: &str| Observation::ConsumerOpen {
            subscription: sub.into(),
            peer: peer("127.0.0.1:8000"),
        };
        state.apply(open("s1"), now);
        state.apply(open("s1"), now);
        assert_eq!(state.connected_consumers(), 2);
        state.apply(
            Observation::ConsumerClose {
                subscription: "s1".into(),
                peer: peer("127.0.0.1:8000"),
            },
            now,
        );
        assert_eq!(state.connected_consumers(), 1);
    }

    #[test]
    fn admin_snapshot_sets_existence_and_topic_count() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(admin(&["t1", "t2"], &[("s1", "t1")]), now);
        assert_eq!(state.topic_count(), 2);
        assert_eq!(state.subscriptions["s1"].topic.as_deref(), Some("t1"));

        // A later snapshot without t2 marks it as gone.
        state.apply(admin(&["t1"], &[("s1", "t1")]), now);
        assert_eq!(state.topic_count(), 1);
    }
}
