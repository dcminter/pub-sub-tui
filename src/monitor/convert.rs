//! Conversion between the in-process [`AppState`] and the `monitor.v1` wire types.
//!
//! The wire format is a near-mirror of [`AppState`], with one wrinkle: a
//! publisher's liveness is tracked in-process as an [`Instant`] (`last_seen`),
//! which is meaningless off-host. The server therefore sends *milliseconds since
//! last seen* relative to its own clock at send time, and the UI rebuilds an
//! [`Instant`] against *its* clock on receipt. The recent-activity window that
//! decides whether a publisher is "connected" then keeps working unchanged, with
//! only the (sub-second) stream latency as skew.

use std::time::{Duration, Instant};

use crate::monitor::proto;
use crate::observe::{AppState, Publisher, Subscription, Topic};

/// Encode the current state as a wire snapshot. `now` timestamps the relative
/// publisher-liveness ages.
pub fn to_wire(state: &AppState, now: Instant) -> proto::StateSnapshot {
    let topics = state
        .topics
        .iter()
        .map(|(name, topic)| (name.clone(), topic_to_wire(topic, now)))
        .collect();

    let subscriptions = state
        .subscriptions
        .iter()
        .map(|(name, sub)| (name.clone(), subscription_to_wire(sub)))
        .collect();

    proto::StateSnapshot {
        topics,
        subscriptions,
    }
}

/// Decode a wire snapshot back into an [`AppState`]. `now` anchors the
/// reconstructed publisher `last_seen` instants.
pub fn from_wire(snapshot: proto::StateSnapshot, now: Instant) -> AppState {
    AppState {
        topics: snapshot
            .topics
            .into_iter()
            .map(|(name, topic)| (name, topic_from_wire(topic, now)))
            .collect(),
        subscriptions: snapshot
            .subscriptions
            .into_iter()
            .map(|(name, sub)| (name, subscription_from_wire(sub)))
            .collect(),
    }
}

fn topic_to_wire(topic: &Topic, now: Instant) -> proto::Topic {
    let publishers = topic
        .publishers
        .iter()
        .map(|(peer, pubr)| {
            let age = now.saturating_duration_since(pubr.last_seen);
            (
                peer.clone(),
                proto::Publisher {
                    published: pubr.published,
                    millis_since_last_seen: u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
                },
            )
        })
        .collect();

    proto::Topic {
        exists: topic.exists,
        publish_count: topic.publish_count,
        publishers,
    }
}

fn topic_from_wire(topic: proto::Topic, now: Instant) -> Topic {
    let publishers = topic
        .publishers
        .into_iter()
        .map(|(peer, pubr)| {
            let last_seen = now
                .checked_sub(Duration::from_millis(pubr.millis_since_last_seen))
                .unwrap_or(now);
            (
                peer,
                Publisher {
                    published: pubr.published,
                    last_seen,
                },
            )
        })
        .collect();

    Topic {
        exists: topic.exists,
        publish_count: topic.publish_count,
        publishers,
    }
}

fn subscription_to_wire(sub: &Subscription) -> proto::Subscription {
    proto::Subscription {
        exists: sub.exists,
        topic: sub.topic.clone(),
        delivered: sub.delivered,
        acked: sub.acked,
        live_consumers: sub.live_consumers,
    }
}

fn subscription_from_wire(sub: proto::Subscription) -> Subscription {
    Subscription {
        exists: sub.exists,
        topic: sub.topic,
        delivered: sub.delivered,
        acked: sub.acked,
        live_consumers: sub.live_consumers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::PUBLISHER_ACTIVE_WINDOW;

    #[test]
    fn round_trip_preserves_counts_and_liveness() {
        let now = Instant::now();
        let mut state = AppState::default();

        let topic = state
            .topics
            .entry("projects/p/topics/orders".into())
            .or_default();
        topic.exists = true;
        topic.publish_count = 9;
        topic.publishers.insert(
            "127.0.0.1:5000".into(),
            Publisher {
                published: 9,
                last_seen: now,
            },
        );

        let sub = state
            .subscriptions
            .entry("projects/p/subscriptions/billing".into())
            .or_default();
        sub.exists = true;
        sub.topic = Some("projects/p/topics/orders".into());
        sub.delivered = 5;
        sub.acked = 4;
        sub.live_consumers = 2;

        // Encode at `now`, decode a moment later: counts survive exactly and the
        // freshly-seen publisher is still inside the liveness window.
        let wire = to_wire(&state, now);
        let later = now + Duration::from_millis(50);
        let restored = from_wire(wire, later);

        let topic = &restored.topics["projects/p/topics/orders"];
        assert!(topic.exists);
        assert_eq!(topic.publish_count, 9);
        assert_eq!(topic.publishers["127.0.0.1:5000"].published, 9);
        assert_eq!(restored.connected_publishers(later), 1);

        let sub = &restored.subscriptions["projects/p/subscriptions/billing"];
        assert_eq!(sub.topic.as_deref(), Some("projects/p/topics/orders"));
        assert_eq!(sub.delivered, 5);
        assert_eq!(sub.acked, 4);
        assert_eq!(sub.live_consumers, 2);
    }

    #[test]
    fn stale_publisher_falls_outside_window_after_transit() {
        let now = Instant::now();
        let mut state = AppState::default();
        let topic = state.topics.entry("t".into()).or_default();
        topic.publish_count = 1;
        // Last seen right at the edge of the window when encoded.
        topic.publishers.insert(
            "peer".into(),
            Publisher {
                published: 1,
                last_seen: now - PUBLISHER_ACTIVE_WINDOW,
            },
        );

        let wire = to_wire(&state, now);
        let restored = from_wire(wire, now);
        // An age of exactly the window is still "connected"; a hair more is not.
        assert_eq!(restored.connected_publishers(now), 1);
        assert_eq!(
            restored.connected_publishers(now + Duration::from_millis(1)),
            0
        );
    }
}
