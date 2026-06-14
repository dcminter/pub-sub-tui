//! Thin binary entry point. See the crate-level docs in `lib.rs` and
//! `docs/architecture.md` for the design.

use anyhow::Context as _;
use clap::Parser as _;
use pub_sub_tui::{cli, logging, observe, poller, proxy, ui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::Cli::parse();
    logging::init(&cli.log_file)?;
    tracing::info!(
        upstream = %cli.upstream,
        listen = %cli.listen,
        project = %cli.project_id,
        "pub-sub-tui starting"
    );

    let listen = cli
        .listen
        .parse()
        .with_context(|| format!("parsing --listen address {:?}", cli.listen))?;

    // The single state owner; the poller and the proxy both feed it.
    let observer: observe::Observer = observe::start();
    poller::spawn(cli.clone(), observer.sink.clone());

    // The interception proxy: clients point PUBSUB_EMULATOR_HOST here.
    {
        let upstream = cli.upstream.clone();
        let sink = observer.sink.clone();
        tokio::spawn(async move {
            if let Err(err) = proxy::serve(listen, upstream, sink).await {
                tracing::error!(%err, "proxy server stopped");
            }
        });
    }

    let header = format!(
        "pub-sub-tui   listen {}  \u{2192}  upstream {}",
        cli.listen, cli.upstream
    );
    ui::run(header, observer.snapshots).await?;

    Ok(())
}
