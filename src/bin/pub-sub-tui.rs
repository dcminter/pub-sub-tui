//! `pub-sub-tui` — the terminal UI.
//!
//! Connects to a (possibly remote) `pub-sub-monitor` over the `monitor.v1` gRPC
//! service and renders the topics, publishers, consumers and message counts it
//! observes. This binary holds no Pub/Sub logic of its own: it is a pure viewer
//! over a stream of state snapshots. See `docs/architecture.md` for the design.

use clap::Parser as _;
use pub_sub_tui::{cli, logging, monitor, ui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::TuiCli::parse();
    logging::init(&cli.log_file)?;
    tracing::info!(monitor = %cli.monitor, "pub-sub-tui starting");

    // Stream state from the monitor into local watch channels that the render
    // loop reads exactly as it would an in-process observer.
    let stream = monitor::stream(cli.monitor.clone());

    let header = format!("pub-sub-tui   monitor {}", cli.monitor);
    ui::run(header, stream).await?;

    Ok(())
}
