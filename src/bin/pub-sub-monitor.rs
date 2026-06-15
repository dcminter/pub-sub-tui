//! `pub-sub-monitor` — the headless monitoring service.
//!
//! Runs the transparent gRPC interception proxy, the admin poller and the single
//! state owner, and exposes the observed state over the `monitor.v1` gRPC service
//! for one or more remote `pub-sub-tui` UIs to display. Designed to run inside a
//! container (e.g. a docker-compose stack); it owns no terminal and logs to
//! stderr. See `docs/architecture.md` for the design.

use anyhow::Context as _;
use clap::Parser as _;
use pub_sub_tui::poller::PollConfig;
use pub_sub_tui::{cli, logging, monitor, observe, poller, proxy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::MonitorCli::parse();
    logging::init_stderr();
    tracing::info!(
        upstream = %cli.upstream,
        listen = %cli.listen,
        state_listen = %cli.state_listen,
        project = %cli.project_id,
        "pub-sub-monitor starting"
    );

    let proxy_listen = cli
        .listen
        .parse()
        .with_context(|| format!("parsing --listen address {:?}", cli.listen))?;
    let state_listen = cli
        .state_listen
        .parse()
        .with_context(|| format!("parsing --state-listen address {:?}", cli.state_listen))?;

    // The single state owner; the poller and the proxy both feed it, and the
    // state server fans its snapshots out to connected UIs.
    let observer = observe::start(cli.recent_buffer);
    poller::spawn(
        PollConfig {
            project_id: cli.project_id.clone(),
            upstream: cli.upstream.clone(),
            poll_interval_ms: cli.poll_interval_ms,
        },
        observer.sink.clone(),
    );

    // The interception proxy: clients point PUBSUB_EMULATOR_HOST here.
    {
        let upstream = cli.upstream.clone();
        let sink = observer.sink.clone();
        let payload_cap = cli.max_payload_bytes;
        tokio::spawn(async move {
            if let Err(err) = proxy::serve(proxy_listen, upstream, sink, payload_cap).await {
                tracing::error!(%err, "proxy server stopped");
            }
        });
    }

    // The state server is the foreground task; Ctrl-C (or SIGTERM in a container)
    // ends the process.
    tokio::select! {
        result = monitor::serve(state_listen, observer.snapshots) => {
            result.context("state server stopped")?;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("interrupted; shutting down");
        }
    }

    Ok(())
}
