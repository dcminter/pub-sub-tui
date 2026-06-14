//! Diagnostic logging.
//!
//! The application owns the terminal (alternate screen), so logs are written to a
//! file rather than stderr to avoid corrupting the TUI. The `LOG_LEVEL` environment
//! variable selects the verbosity, honoured at `TRACE`, `DEBUG`, `INFO`, `WARN` and
//! `ERROR` (case-insensitively); it defaults to `INFO`.

use std::fs::OpenOptions;

use anyhow::Context as _;
use tracing_subscriber::EnvFilter;

/// Initialise the global tracing subscriber, appending to `log_file`.
pub fn init(log_file: &str) -> anyhow::Result<()> {
    let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned());
    let filter =
        EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"));

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .with_context(|| format!("opening log file {log_file}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(move || file.try_clone().expect("clone log file handle"))
        .init();

    Ok(())
}
