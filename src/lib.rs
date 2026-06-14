//! `pub-sub-tui` — monitor a Google Pub/Sub instance by acting as a transparent
//! gRPC interception proxy in front of the (typically emulated) server.
//!
//! The binary (`main.rs`) is a thin shell over these modules; exposing them as a
//! library lets the observation logic and admin poller be exercised by tests.
//! See `docs/architecture.md` for the design.

pub mod cli;
pub mod logging;
pub mod observe;
pub mod pb;
pub mod poller;
pub mod proxy;
pub mod ui;
