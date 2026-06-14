//! Command-line interface, defined declaratively with `clap`'s derive API.

use clap::Parser;

/// Monitor a Google Pub/Sub instance by transparently proxying its gRPC traffic.
///
/// Point the client-under-test's `PUBSUB_EMULATOR_HOST` at `--listen`; the proxy
/// forwards every call faithfully to `--upstream` while observing the traffic.
#[derive(Debug, Clone, Parser)]
#[command(name = "pub-sub-tui", version, about, long_about = None)]
pub struct Cli {
    /// Address of the real Pub/Sub emulator (or instance) to forward to.
    #[arg(long, default_value = "localhost:8085", env = "PUBSUB_UPSTREAM")]
    pub upstream: String,

    /// Address the proxy listens on. Set the client's `PUBSUB_EMULATOR_HOST` to this.
    #[arg(long, default_value = "127.0.0.1:8681")]
    pub listen: String,

    /// GCP project id whose topics and subscriptions are enumerated.
    #[arg(long, default_value = "test-project", env = "PUBSUB_PROJECT_ID")]
    pub project_id: String,

    /// Admin poll interval, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub poll_interval_ms: u64,

    /// File that diagnostic logs are written to (verbosity via the `LOG_LEVEL` env var).
    #[arg(long, default_value = "pub-sub-tui.log")]
    pub log_file: String,
}
