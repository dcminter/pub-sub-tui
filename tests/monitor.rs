//! End-to-end test of the monitor↔UI boundary over a real gRPC connection.
//!
//! Serves the state stream from an observer, dials it with the UI-side client,
//! feeds an observation, and asserts the client receives a snapshot reflecting it.
//! Needs no emulator — it is pure localhost gRPC — so it runs in the normal suite.

use std::time::Duration;

use pub_sub_tui::observe::Observation;
use pub_sub_tui::{monitor, observe};

#[tokio::test]
async fn streams_observed_state_to_a_remote_client() {
    let listen = "127.0.0.1:18690";

    // Headless side: an observer whose snapshots are served over gRPC.
    let observer = observe::start();
    tokio::spawn(monitor::serve(
        listen.parse().unwrap(),
        observer.snapshots.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // UI side: stream the state back into a local watch channel.
    let mut snapshots = monitor::stream(listen.to_owned());

    // Feed an observation through the headless observer.
    const TOPIC: &str = "projects/p/topics/acme.orders.created";
    observer.sink.observe(Observation::Publish {
        topic: TOPIC.into(),
        peer: "127.0.0.1:5000".parse().ok(),
        messages: 3,
    });

    // The client should observe a snapshot reflecting the publish.
    let mut observed = 0;
    for _ in 0..50 {
        if tokio::time::timeout(Duration::from_secs(2), snapshots.changed())
            .await
            .is_err()
        {
            break; // no update within the window
        }
        let state = snapshots.borrow_and_update().clone();
        if let Some(topic) = state.topics.get(TOPIC) {
            observed = topic.publish_count;
            if observed == 3 {
                break;
            }
        }
    }

    assert_eq!(observed, 3, "client received the observed publish count");
}
