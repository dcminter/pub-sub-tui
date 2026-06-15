//! Conversion between the in-process [`AppState`] and the `monitor.v1` wire types.
//!
//! The wire format is a near-mirror of [`AppState`], with one wrinkle: a
//! publisher's liveness is tracked in-process as an [`Instant`] (`last_seen`),
//! which is meaningless off-host. The server therefore sends *milliseconds since
//! last seen* relative to its own clock at send time, and the UI rebuilds an
//! [`Instant`] against *its* clock on receipt. The recent-activity window that
//! decides whether a publisher is "connected" then keeps working unchanged, with
//! only the (sub-second) stream latency as skew.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::monitor::proto;
use crate::observe::{AppState, Publisher, RecentMessage, Subscription, Topic};

/// Encode the current state as a wire snapshot. `now` timestamps the relative
/// publisher-liveness and message ages.
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

    let recent_messages = state
        .recent_messages
        .iter()
        .map(|message| message_to_wire(message, now))
        .collect();

    proto::StateSnapshot {
        topics,
        subscriptions,
        recent_messages,
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
        recent_messages: snapshot
            .recent_messages
            .into_iter()
            .map(|message| message_from_wire(message, now))
            .collect(),
        ..AppState::default()
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

fn message_to_wire(message: &RecentMessage, now: Instant) -> proto::RecentMessage {
    let age = now.saturating_duration_since(message.seen);
    proto::RecentMessage {
        seq: message.seq,
        topic: message.topic.clone(),
        data: message.data.clone().into(),
        attributes: message
            .attributes
            .iter()
            .map(|(key, value)| proto::Attribute {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        original_len: message.original_len as u64,
        truncated: message.truncated,
        millis_since_seen: u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
    }
}

fn message_from_wire(message: proto::RecentMessage, now: Instant) -> Arc<RecentMessage> {
    let seen = now
        .checked_sub(Duration::from_millis(message.millis_since_seen))
        .unwrap_or(now);
    Arc::new(RecentMessage {
        seq: message.seq,
        topic: message.topic,
        data: message.data.to_vec(),
        attributes: message
            .attributes
            .into_iter()
            .map(|attr| (attr.key, attr.value))
            .collect(),
        original_len: message.original_len as usize,
        truncated: message.truncated,
        seen,
    })
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
    fn round_trip_preserves_recent_messages() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.recent_messages.push_back(Arc::new(RecentMessage {
            seq: 7,
            topic: "projects/p/topics/orders".into(),
            data: b"{\"id\":1}".to_vec(),
            attributes: vec![("content-type".into(), "application/json".into())],
            original_len: 8,
            truncated: false,
            seen: now,
        }));

        let wire = to_wire(&state, now);
        let restored = from_wire(wire, now + Duration::from_millis(20));

        assert_eq!(restored.recent_messages.len(), 1);
        let message = &restored.recent_messages[0];
        assert_eq!(message.seq, 7);
        assert_eq!(message.topic, "projects/p/topics/orders");
        assert_eq!(message.data, b"{\"id\":1}");
        assert_eq!(
            message.attributes,
            vec![("content-type".to_owned(), "application/json".to_owned())]
        );
        assert_eq!(message.original_len, 8);
        assert!(!message.truncated);
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
