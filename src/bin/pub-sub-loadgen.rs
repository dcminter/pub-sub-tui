//! `pub-sub-loadgen` — a traffic generator for demos and testing.
//!
//! Seeds a tree of hierarchically-named topics, attaches subscriptions, then
//! publishes and consumes through the proxy so the monitor has something to show.
//! See `src/loadgen.rs` for what it generates and `docs/emulator.md` for usage.

use clap::Parser as _;
use pub_sub_tui::{cli, loadgen, logging};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::LoadgenCli::parse();
    logging::init_stderr();
    tracing::info!(
        endpoint = %cli.endpoint,
        project = %cli.project_id,
        "pub-sub-loadgen starting"
    );

    loadgen::run(cli).await
}
