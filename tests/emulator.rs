//! End-to-end test of the admin poller against a real Pub/Sub emulator.
//!
//! Ignored by default because it needs a running emulator. Start one and run:
//!
//! ```text
//! docker run --rm -p 8085:8085 google/cloud-sdk:emulators \
//!   gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=test-project
//! cargo test --test emulator -- --ignored
//! ```
//!
//! Override the emulator address with `PUBSUB_TEST_UPSTREAM` (default `localhost:8085`).

use google_cloud_pubsub::subscription::SubscriptionConfig;
use pub_sub_tui::observe::Observation;
use pub_sub_tui::poller;

fn upstream() -> String {
    std::env::var("PUBSUB_TEST_UPSTREAM").unwrap_or_else(|_| "localhost:8085".to_owned())
}

#[tokio::test]
#[ignore = "requires a running Pub/Sub emulator"]
async fn poll_once_lists_seeded_topic_and_subscription() {
    let project = "test-project";
    let client = poller::connect(project, &upstream())
        .await
        .expect("connect to emulator");

    // Seed a topic + subscription. Ignore "already exists" so the test re-runs.
    let topic = match client.create_topic("pst-it-topic", None, None).await {
        Ok(topic) => topic,
        Err(_) => client.topic("pst-it-topic"),
    };
    let sub = client.subscription("pst-it-sub");
    let _ = sub
        .create(
            topic.fully_qualified_name(),
            SubscriptionConfig::default(),
            None,
        )
        .await;

    let Observation::AdminSnapshot {
        topics,
        subscriptions,
    } = poller::poll_once(&client).await.expect("poll once")
    else {
        panic!("expected an AdminSnapshot");
    };

    assert!(
        topics.iter().any(|t| t.ends_with("/topics/pst-it-topic")),
        "seeded topic not listed: {topics:?}"
    );
    let seeded = subscriptions
        .iter()
        .find(|s| s.name.ends_with("/subscriptions/pst-it-sub"))
        .expect("seeded subscription not listed");
    assert!(
        seeded.topic.ends_with("/topics/pst-it-topic"),
        "subscription mapped to wrong topic: {}",
        seeded.topic
    );
}
