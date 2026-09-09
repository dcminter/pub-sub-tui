//! Observation pipeline: events emitted by the proxy/poller, and the single task
//! that folds them into the application state read by the TUI.

mod events;
mod state;

#[cfg(test)]
pub use events::test_sink;
pub use events::{Observation, ObservationSink, PublishedMessage, SubscriptionInfo};
pub use state::{
    AppState, Observer, PUBLISHER_ACTIVE_WINDOW, Publisher, RecentMessage, Subscription, Topic,
    start,
};
