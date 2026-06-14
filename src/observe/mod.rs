//! Observation pipeline: events emitted by the proxy/poller, and the single task
//! that folds them into the application state read by the TUI.

mod events;
mod state;

pub use events::{Observation, ObservationSink, SubscriptionInfo};
pub use state::{AppState, Observer, Subscription, Topic, start};
