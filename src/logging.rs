//! Diagnostic logging.
//!
//! The `LOG_LEVEL` environment variable selects the verbosity, honoured at
//! `TRACE`, `DEBUG`, `INFO`, `WARN` and `ERROR` (case-insensitively); it defaults
//! to `INFO`.
//!
//! The TUI owns the terminal (alternate screen), so it logs to a *file* via
//! [`init`] to avoid corrupting the display. The headless binaries (the monitor
//! and the load generator) log to *stderr* via [`init_stderr`], which is the
//! natural place for a service — `docker compose logs` then shows them.

use std::fs::OpenOptions;

use anyhow::Context as _;
use tracing_subscriber::EnvFilter;

/// Read `LOG_LEVEL` into an `EnvFilter`, defaulting to `info` if unset or invalid.
fn filter() -> EnvFilter {
    let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned());
    EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialise the global tracing subscriber, appending to `log_file`. Used by the
/// TUI, which cannot write to stderr without corrupting the terminal.
pub fn init(log_file: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .with_context(|| format!("opening log file {log_file}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter())
        .with_ansi(false)
        .with_writer(move || file.try_clone().expect("clone log file handle"))
        .init();

    Ok(())
}

/// Initialise the global tracing subscriber writing to stderr. Used by the
/// headless binaries, which do not own the terminal.
pub fn init_stderr() {
    tracing_subscriber::fmt()
        .with_env_filter(filter())
        .with_writer(std::io::stderr)
        .init();
}
