//! The UI-side client: dial a headless monitor and turn its snapshot stream into
//! a local [`watch`] channel, so the rest of the UI reads state exactly as it did
//! when it lived in-process.
//!
//! The connection is supervised: if it drops (or the monitor is not up yet) the
//! task keeps retrying with a fixed backoff, holding the last snapshot in the
//! meantime. The UI therefore starts cleanly even before the monitor exists.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::monitor::convert;
use crate::monitor::proto;
use crate::observe::AppState;
use proto::monitor_client::MonitorClient;

/// Delay between reconnection attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

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
    let mut client = MonitorClient::connect(url.to_owned()).await?;
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
