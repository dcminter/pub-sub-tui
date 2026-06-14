//! End-to-end test of the monitor↔UI boundary over a real gRPC connection.
//!
//! Serves the state stream from an observer, dials it with the UI-side client,
//! feeds an observation, and asserts the client receives a snapshot reflecting it.
//! Needs no emulator — it is pure localhost gRPC — so it runs in the normal suite.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use pub_sub_tui::observe::{AppState, Observation};
use pub_sub_tui::{monitor, observe};

const TOPIC: &str = "projects/p/topics/acme.orders.created";

/// Feed a publish observation of `messages` for [`TOPIC`] into an observer.
fn publish(sink: &observe::ObservationSink, messages: u64) {
    sink.observe(Observation::Publish {
        topic: TOPIC.into(),
        peer: "127.0.0.1:5000".parse().ok(),
        messages,
    });
}

/// Poll the UI-side watch channel until [`TOPIC`]'s publish count reaches at
/// least `target`, or the retry budget is exhausted. Returns the last count seen.
async fn wait_for_publish_count(
    snapshots: &mut watch::Receiver<Arc<AppState>>,
    target: u64,
) -> u64 {
    let mut observed = 0;
    for _ in 0..50 {
        observed = snapshots
            .borrow_and_update()
            .topics
            .get(TOPIC)
            .map_or(0, |t| t.publish_count);
        if observed >= target {
            return observed;
        }
        if tokio::time::timeout(Duration::from_secs(2), snapshots.changed())
            .await
            .is_err()
        {
            break; // no update within the window
        }
    }
    observed
}

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
    publish(&observer.sink, 3);

    // The client should observe a snapshot reflecting the publish.
    let observed = wait_for_publish_count(&mut snapshots, 3).await;
    assert_eq!(observed, 3, "client received the observed publish count");
}

/// The UI may be launched before the monitor exists. The supervised client must
/// keep retrying and connect cleanly once the monitor appears, with no restart.
#[tokio::test]
async fn client_connects_when_monitor_starts_late() {
    let listen = "127.0.0.1:18691";

    // UI side starts first — there is no monitor to dial yet.
    let mut snapshots = monitor::stream(listen.to_owned());

    // Give the supervisor time to attempt (and fail) at least one connection,
    // exercising the retry path rather than a lucky first-try connect.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now bring the monitor up on the endpoint the client is retrying.
    let observer = observe::start();
    tokio::spawn(monitor::serve(
        listen.parse().unwrap(),
        observer.snapshots.clone(),
    ));
    publish(&observer.sink, 3);

    let observed = wait_for_publish_count(&mut snapshots, 3).await;
    assert_eq!(observed, 3, "client connected once the monitor appeared");
}

/// If the monitor vanishes (crash, restart, transient network loss) the client
/// must reconnect on its own and reflect the restarted monitor's state.
///
/// The first monitor is run on its own runtime so it can be made to *vanish*
/// abruptly: dropping that runtime drops the detached per-connection tasks tonic
/// spawns, closing the live socket exactly as a dying monitor process would.
/// (Merely aborting the `serve` future is not enough — it stops the listener but
/// leaves already-accepted connections running, so the client sees no drop.)
#[tokio::test]
async fn client_reconnects_after_monitor_restarts() {
    let listen = "127.0.0.1:18692";
    let addr: std::net::SocketAddr = listen.parse().unwrap();

    // First monitor instance, on a dedicated runtime we can tear down on command.
    let observer1 = observe::start();
    let snap1 = observer1.snapshots.clone();
    let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
    let monitor1 = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.spawn(async move {
            let _ = monitor::serve(addr, snap1).await;
        });
        rt.block_on(async move {
            let _ = kill_rx.await;
        });
        // `rt` drops here: the server and every live connection die, sockets close.
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut snapshots = monitor::stream(listen.to_owned());
    publish(&observer1.sink, 1);
    assert_eq!(
        wait_for_publish_count(&mut snapshots, 1).await,
        1,
        "client connected to the first monitor"
    );

    // The monitor disappears for a bit.
    kill_tx.send(()).unwrap();
    monitor1.join().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ...then comes back on the same endpoint with fresh, higher state.
    let observer2 = observe::start();
    tokio::spawn(monitor::serve(addr, observer2.snapshots.clone()));
    publish(&observer2.sink, 5);

    // The client should reconnect unaided and reflect the new monitor's count.
    let observed = wait_for_publish_count(&mut snapshots, 5).await;
    assert_eq!(observed, 5, "client reconnected to the restarted monitor");
}
