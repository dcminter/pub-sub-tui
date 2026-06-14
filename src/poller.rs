//! Admin metadata poller.
//!
//! Uses the `google-cloud-pubsub` crate to enumerate topics and subscriptions on
//! a fixed interval, emitting an [`Observation::AdminSnapshot`]. This is the
//! "polling tick where polling is required" half of the README's update model and
//! the source of the topic count and the topic→subscription mapping (it also
//! catches entities created out-of-band, i.e. not through the proxy).
//!
//! It connects directly to the real upstream via [`Environment::Emulator`], not
//! through this proxy, so polling neither loops back nor pollutes traffic metrics.

use std::time::Duration;

use google_cloud_gax::conn::Environment;
use google_cloud_pubsub::client::{Client, ClientConfig};
use tokio::time::{MissedTickBehavior, interval};

use crate::observe::{Observation, ObservationSink, SubscriptionInfo};

/// What the admin poller needs to know to run.
#[derive(Debug, Clone)]
pub struct PollConfig {
    /// GCP project id whose topics and subscriptions are enumerated.
    pub project_id: String,
    /// The real upstream emulator/instance to poll (never the proxy).
    pub upstream: String,
    /// Poll interval, in milliseconds.
    pub poll_interval_ms: u64,
}

/// Delay between attempts to (re)connect the metadata client.
const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Spawn the poller as a background task.
pub fn spawn(config: PollConfig, sink: ObservationSink) {
    tokio::spawn(async move {
        if let Err(err) = run(config, sink).await {
            tracing::error!(%err, "admin poller stopped");
        }
    });
}

/// Connect a metadata client directly to the real upstream emulator/instance,
/// bypassing the `PUBSUB_EMULATOR_HOST` env var (which points clients at the proxy).
pub async fn connect(project_id: &str, upstream: &str) -> anyhow::Result<Client> {
    let config = ClientConfig {
        project_id: Some(project_id.to_owned()),
        environment: Environment::Emulator(upstream.to_owned()),
        ..Default::default()
    };
    Ok(Client::new(config).await?)
}

/// Like [`connect`], but retries indefinitely so the monitor can start before the
/// upstream emulator is ready (e.g. racing it up in a compose stack).
async fn connect_with_retry(config: &PollConfig) -> Client {
    loop {
        match connect(&config.project_id, &config.upstream).await {
            Ok(client) => return client,
            Err(err) => {
                tracing::warn!(upstream = %config.upstream, %err, "poller connect failed; retrying");
                tokio::time::sleep(CONNECT_RETRY_DELAY).await;
            }
        }
    }
}

async fn run(config: PollConfig, sink: ObservationSink) -> anyhow::Result<()> {
    let client = connect_with_retry(&config).await;
    tracing::info!(upstream = %config.upstream, "admin poller connected");

    let mut ticker = interval(Duration::from_millis(config.poll_interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        match poll_once(&client).await {
            Ok(snapshot) => sink.observe(snapshot),
            Err(err) => tracing::warn!(%err, "admin poll failed"),
        }
    }
}

/// Enumerate topics and subscriptions once, producing an admin snapshot.
pub async fn poll_once(client: &Client) -> anyhow::Result<Observation> {
    let topics = client.get_topics(None).await?;

    let mut subscriptions = Vec::new();
    for sub in client.get_subscriptions(None).await? {
        let name = sub.fully_qualified_name().to_owned();
        match sub.config(None).await {
            Ok((topic, _config)) => subscriptions.push(SubscriptionInfo { name, topic }),
            Err(err) => tracing::debug!(%err, %name, "skipping subscription (config fetch failed)"),
        }
    }

    Ok(Observation::AdminSnapshot {
        topics,
        subscriptions,
    })
}
