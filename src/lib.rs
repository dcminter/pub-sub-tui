//! `pub-sub-tui` — monitor a Google Pub/Sub instance by acting as a transparent
//! gRPC interception proxy in front of the (typically emulated) server.
//!
//! The work is split across three thin binaries (under `src/bin/`) that are all
//! shells over these modules:
//! - `pub-sub-monitor` — the headless service: interception proxy + admin poller +
//!   observer + the [`monitor`] state-streaming server.
//! - `pub-sub-tui` — the terminal UI: [`monitor::stream`]s state from a (possibly
//!   remote) monitor and renders it.
//! - `pub-sub-loadgen` — a traffic generator that drives demo traffic through the
//!   proxy for the monitor to observe.
//!
//! Exposing the logic as a library also lets the observation pipeline, the admin
//! poller and the wire conversion be exercised by tests. See
//! `docs/architecture.md` for the design.

pub mod cli;
pub mod loadgen;
pub mod logging;
pub mod monitor;
pub mod observe;
pub mod pb;
pub mod poller;
pub mod proxy;
pub mod ui;
