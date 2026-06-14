//! Command-line interfaces for the three binaries, defined declaratively with
//! `clap`'s derive API. Each binary parses its own struct; they share nothing but
//! a few defaults (the proxy port, the project id).

use clap::Parser;

/// The headless monitor service: a transparent gRPC interception proxy in front
/// of a Pub/Sub instance, plus an admin poller, exposing the observed state over
/// the network for a remote UI to display.
///
/// Point the client-under-test's `PUBSUB_EMULATOR_HOST` at `--listen`; the proxy
/// forwards every call faithfully to `--upstream` while observing the traffic.
/// Point a `pub-sub-tui` UI at `--state-listen` to display what it sees.
#[derive(Debug, Clone, Parser)]
#[command(name = "pub-sub-monitor", version, about, long_about = None)]
pub struct MonitorCli {
    /// Address of the real Pub/Sub emulator (or instance) to forward to.
    #[arg(long, default_value = "localhost:8085", env = "PUBSUB_UPSTREAM")]
    pub upstream: String,

    /// Address the interception proxy listens on. Set clients' `PUBSUB_EMULATOR_HOST`
    /// to this. Defaults to all interfaces so it is reachable inside a container.
    #[arg(long, default_value = "0.0.0.0:8681")]
    pub listen: String,

    /// Address the state-streaming gRPC server listens on (the UI connects here).
    #[arg(long, default_value = "0.0.0.0:8682")]
    pub state_listen: String,

    /// GCP project id whose topics and subscriptions are enumerated.
    #[arg(long, default_value = "test-project", env = "PUBSUB_PROJECT_ID")]
    pub project_id: String,

    /// Admin poll interval, in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub poll_interval_ms: u64,
}

/// The terminal UI. Connects to a (possibly remote) `pub-sub-monitor` and displays
/// the topics, publishers, consumers and message counts it observes.
#[derive(Debug, Clone, Parser)]
#[command(name = "pub-sub-tui", version, about, long_about = None)]
pub struct TuiCli {
    /// Address of the monitor's state server (its `--state-listen`).
    #[arg(long, default_value = "127.0.0.1:8682", env = "PUBSUB_MONITOR")]
    pub monitor: String,

    /// File that diagnostic logs are written to (the UI owns the terminal, so logs
    /// cannot go to stderr). Verbosity via the `LOG_LEVEL` env var.
    #[arg(long, default_value = "pub-sub-tui.log")]
    pub log_file: String,
}

/// A traffic generator for demos and testing: it creates a tree of topics with
/// hierarchical, dotted names, attaches subscriptions, then continuously publishes
/// and consumes — all *through the proxy* so the monitor observes it.
#[derive(Debug, Clone, Parser)]
#[command(name = "pub-sub-loadgen", version, about, long_about = None)]
pub struct LoadgenCli {
    /// Pub/Sub endpoint to drive. Point this at the monitor's proxy `--listen` so
    /// the traffic flows through it and is observed.
    #[arg(long, default_value = "localhost:8681", env = "PUBSUB_ENDPOINT")]
    pub endpoint: String,

    /// GCP project id the topics and subscriptions are created under.
    #[arg(long, default_value = "test-project", env = "PUBSUB_PROJECT_ID")]
    pub project_id: String,

    /// Delay between publish rounds, in milliseconds.
    #[arg(long, default_value_t = 800)]
    pub interval_ms: u64,

    /// Seconds to run before exiting; `0` runs until interrupted.
    #[arg(long, default_value_t = 0)]
    pub duration_secs: u64,
}
