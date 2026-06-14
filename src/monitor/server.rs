//! The state-streaming gRPC server run by the headless monitor.
//!
//! It implements the `monitor.v1.Monitor` service: each connecting UI gets the
//! current snapshot immediately, then a fresh snapshot every time the observed
//! state changes. The snapshots come straight off the observer's [`watch`]
//! channel, so this server adds no state of its own — it is a pure fan-out tap.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use tokio::sync::watch;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::WatchStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::monitor::convert;
use crate::monitor::proto;
use crate::observe::AppState;
use proto::monitor_server::{Monitor, MonitorServer};

/// Serve the state stream until the process exits.
pub async fn serve(
    listen: SocketAddr,
    snapshots: watch::Receiver<Arc<AppState>>,
) -> anyhow::Result<()> {
    tracing::info!(%listen, "monitor state server listening");
    Server::builder()
        .add_service(MonitorServer::new(MonitorService { snapshots }))
        .serve(listen)
        .await?;
    Ok(())
}

struct MonitorService {
    snapshots: watch::Receiver<Arc<AppState>>,
}

#[tonic::async_trait]
impl Monitor for MonitorService {
    type StreamStateStream =
        Pin<Box<dyn Stream<Item = Result<proto::StateSnapshot, Status>> + Send>>;

    async fn stream_state(
        &self,
        _request: Request<proto::StreamStateRequest>,
    ) -> Result<Response<Self::StreamStateStream>, Status> {
        // `WatchStream` yields the current value immediately, then each change.
        // Cloning the receiver gives this connection its own independent cursor.
        let stream = WatchStream::new(self.snapshots.clone())
            .map(|state| Ok(convert::to_wire(&state, Instant::now())));
        Ok(Response::new(Box::pin(stream)))
    }
}
