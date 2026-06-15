//! The authoritative application state and the single task that owns it.
//!
//! Every mutation happens here, driven by [`Observation`]s drained from an mpsc
//! channel. After applying a burst of observations the task publishes an
//! immutable snapshot through a [`watch`] channel that the TUI reads at its own
//! cadence. This keeps the hot proxy path lock-free and gives the UI a cheap,
//! always-latest view.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use super::events::{Observation, ObservationSink, PublishedMessage, SubscriptionInfo, peer_key};

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

/// A single message observed on the wire, retained for the recent-messages view.
/// Payloads are `Arc`-shared so snapshot clones stay cheap regardless of size.
#[derive(Debug, Clone)]
pub struct RecentMessage {
    /// Monotonic sequence number, assigned in publish order; stable list identity.
    pub seq: u64,
    /// Fully-qualified topic the message was published to.
    pub topic: String,
    /// The (possibly truncated) message body.
    pub data: Vec<u8>,
    /// The message attributes, in the order the publisher supplied them.
    pub attributes: Vec<(String, String)>,
    /// The payload's true length before any truncation.
    pub original_len: usize,
    /// Whether `data` was truncated to the proxy's payload cap.
    pub truncated: bool,
    /// When the message was observed (anchors the displayed age).
    pub seen: Instant,
}

/// The complete observable state of the monitored instance.
#[derive(Debug, Clone, Default)]
pub struct AppState {
    pub topics: BTreeMap<String, Topic>,
    pub subscriptions: BTreeMap<String, Subscription>,
    /// Most-recent published messages, oldest first, newest at the back. Bounded
    /// by the owning task (see [`run`]).
    pub recent_messages: VecDeque<Arc<RecentMessage>>,
    /// Sequence number to assign the next observed message. Only meaningful on the
    /// monitor side, where observations are folded in; the UI leaves it at zero.
    pub next_seq: u64,
}

impl AppState {
    fn topic_mut(&mut self, name: &str) -> &mut Topic {
        self.topics.entry(name.to_owned()).or_default()
    }

    fn subscription_mut(&mut self, name: &str) -> &mut Subscription {
        self.subscriptions.entry(name.to_owned()).or_default()
    }

    /// Append a captured message to the recent-messages buffer, assigning it the
    /// next sequence number. Trimming to the retention bound is the owner's job
    /// (see [`run`]), so this stays cap-agnostic and easy to test.
    fn push_recent(&mut self, topic: &str, message: PublishedMessage, now: Instant) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.recent_messages.push_back(Arc::new(RecentMessage {
            seq,
            topic: topic.to_owned(),
            data: message.data,
            attributes: message.attributes,
            original_len: message.original_len,
            truncated: message.truncated,
            seen: now,
        }));
    }

    /// Drop the oldest messages until at most `cap` remain.
    fn trim_recent(&mut self, cap: usize) {
        while self.recent_messages.len() > cap {
            self.recent_messages.pop_front();
        }
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
                let count = messages.len() as u64;
                let entry = self.topic_mut(&topic);
                entry.publish_count += count;
                let publisher = entry.publishers.entry(peer_key(peer)).or_insert(Publisher {
                    published: 0,
                    last_seen: now,
                });
                publisher.published += count;
                publisher.last_seen = now;
                for message in messages {
                    self.push_recent(&topic, message, now);
                }
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
/// `recent_buffer` bounds how many recently-published messages are retained for
/// the UI's recent-messages view.
pub fn start(recent_buffer: usize) -> Observer {
    let (tx, rx) = mpsc::channel(8192);
    let (snap_tx, snap_rx) = watch::channel(Arc::new(AppState::default()));
    tokio::spawn(run(rx, snap_tx, recent_buffer));
    Observer {
        sink: ObservationSink::new(tx),
        snapshots: snap_rx,
    }
}

async fn run(
    mut rx: mpsc::Receiver<Observation>,
    snap_tx: watch::Sender<Arc<AppState>>,
    recent_buffer: usize,
) {
    let mut state = AppState::default();
    while let Some(first) = rx.recv().await {
        let now = Instant::now();
        state.apply(first, now);
        // Coalesce any other immediately-available observations into one snapshot.
        while let Ok(observation) = rx.try_recv() {
            state.apply(observation, now);
        }
        // Bound the recent-messages buffer once per coalesced burst.
        state.trim_recent(recent_buffer);
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

    /// `n` empty captured messages, for tests that only care about counts.
    fn msgs(n: usize) -> Vec<PublishedMessage> {
        (0..n).map(|_| message(b"")).collect()
    }

    /// A captured message carrying `data`.
    fn message(data: &[u8]) -> PublishedMessage {
        PublishedMessage {
            data: data.to_vec(),
            attributes: Vec::new(),
            original_len: data.len(),
            truncated: false,
        }
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
                messages: msgs(3),
            },
            now,
        );
        state.apply(
            Observation::Publish {
                topic: "t1".into(),
                peer: peer("127.0.0.1:5000"),
                messages: msgs(2),
            },
            now,
        );
        state.apply(
            Observation::Publish {
                topic: "t1".into(),
                peer: peer("127.0.0.1:6000"),
                messages: msgs(1),
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
    fn recent_messages_record_payloads_and_assign_increasing_seq() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(
            Observation::Publish {
                topic: "projects/p/topics/orders".into(),
                peer: peer("127.0.0.1:5000"),
                messages: vec![message(b"first"), message(b"second")],
            },
            now,
        );

        assert_eq!(state.recent_messages.len(), 2);
        let seqs: Vec<u64> = state.recent_messages.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![0, 1]);
        assert_eq!(state.recent_messages[0].topic, "projects/p/topics/orders");
        assert_eq!(state.recent_messages[1].data, b"second");
    }

    #[test]
    fn trim_recent_keeps_the_newest_within_the_cap() {
        let now = Instant::now();
        let mut state = AppState::default();
        for n in 0..5u8 {
            state.apply(
                Observation::Publish {
                    topic: "t".into(),
                    peer: peer("127.0.0.1:5000"),
                    messages: vec![message(&[n])],
                },
                now,
            );
        }
        state.trim_recent(3);

        // Oldest two dropped; the three newest (and their seqs) survive in order.
        let seqs: Vec<u64> = state.recent_messages.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![2, 3, 4]);
        assert_eq!(
            state.recent_messages.front().map(|m| m.data.clone()),
            Some(vec![2])
        );
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
