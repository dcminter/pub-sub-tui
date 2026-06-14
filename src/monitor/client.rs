//! The UI-side client: dial a headless monitor and turn its snapshot stream into
//! a local [`watch`] channel, so the rest of the UI reads state exactly as it did
//! when it lived in-process.
//!
//! The connection is supervised: if it drops (or the monitor is not up yet) the
//! task keeps retrying with a fixed backoff, holding the last snapshot in the
//! meantime. The UI therefore starts cleanly even before the monitor exists.
//! HTTP/2 keepalive pings ensure even a silently severed connection faults
//! within a bounded time, so the supervisor can reconnect rather than hang.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tonic::transport::Endpoint;

use crate::monitor::convert;
use crate::monitor::proto;
use crate::observe::AppState;
use proto::monitor_client::MonitorClient;

/// Delay between reconnection attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How often to ping the monitor over an otherwise-quiet connection, and how long
/// to wait for the reply before declaring the connection dead. Snapshots can be
/// sparse, so a connection may sit idle for a long time; without keepalive a
/// silently dropped link (network partition, a host that vanishes without sending
/// a RST) would never surface as an error and the client would hang forever
/// instead of reconnecting. With it, the dead connection faults within roughly
/// `KEEP_ALIVE_INTERVAL + KEEP_ALIVE_TIMEOUT` and the supervisor reconnects.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect to the monitor at `endpoint` (host:port) and stream its state into a
/// [`watch`] channel. Returns the receiver immediately; the connection is driven
/// on a background task that reconnects for as long as the receiver lives.
pub fn stream(endpoint: String) -> watch::Receiver<Arc<AppState>> {
    let (tx, rx) = watch::channel(Arc::new(AppState::default()));
    tokio::spawn(supervise(endpoint, tx));
    rx
}

async fn supervise(endpoint: String, tx: watch::Sender<Arc<AppState>>) {
    let url = format!("http://{endpoint}");
    loop {
        match stream_once(&url, &tx).await {
            Ok(()) => tracing::warn!(%endpoint, "monitor stream ended; reconnecting"),
            Err(err) => tracing::warn!(%endpoint, %err, "monitor connection failed; retrying"),
        }
        // Stop as soon as the UI has dropped its receiver.
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// One connection's lifetime: dial, subscribe, and pump snapshots until the
/// stream ends or errors.
async fn stream_once(url: &str, tx: &watch::Sender<Arc<AppState>>) -> anyhow::Result<()> {
    // Keepalive pings let us notice a connection that has silently gone away,
    // rather than blocking indefinitely on a stream that will never produce data.
    let channel = Endpoint::from_shared(url.to_owned())?
        .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect()
        .await?;
    let mut client = MonitorClient::new(channel);
    tracing::info!(%url, "connected to monitor");

    let mut stream = client
        .stream_state(proto::StreamStateRequest {})
        .await?
        .into_inner();

    while let Some(snapshot) = stream.message().await? {
        let state = convert::from_wire(snapshot, Instant::now());
        if tx.send(Arc::new(state)).is_err() {
            break; // UI gone
        }
    }
    Ok(())
}
