//! Terminal user interface: setup/teardown, the input thread and the render loop.

mod app;
mod theme;
mod tree;
mod widgets;

use std::sync::Arc;
use std::time::Duration;

use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{self, Event};
use tokio::sync::{mpsc, watch};

use crate::observe::AppState;
use app::App;

/// Run the TUI until the user quits. Restores the terminal on the way out
/// (including on panic, via the hook installed by `ratatui::init`).
pub async fn run(header: String, snapshots: watch::Receiver<Arc<AppState>>) -> anyhow::Result<()> {
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, header, snapshots).await;
    ratatui::restore();
    result
}

async fn run_loop(
    terminal: &mut DefaultTerminal,
    header: String,
    mut snapshots: watch::Receiver<Arc<AppState>>,
) -> anyhow::Result<()> {
    let mut app = App::new(header);
    let mut input = spawn_input_thread();

    // Redraw at least once a second so time-based stats (active-publisher window)
    // stay current even when no events or data arrive.
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut snapshot = snapshots.borrow().clone();

    loop {
        terminal.draw(|frame| app.render(frame, &snapshot))?;

        tokio::select! {
            maybe_event = input.recv() => match maybe_event {
                Some(event) => app.handle_event(&event),
                None => break, // input thread ended
            },
            _ = tick.tick() => {}
            changed = snapshots.changed() => {
                if changed.is_err() {
                    break; // state task gone
                }
                snapshot = snapshots.borrow_and_update().clone();
            }
        }

        if app.should_quit() {
            break;
        }
    }

    Ok(())
}

/// Read terminal events on a dedicated OS thread and forward them to the async
/// loop, so we never block the runtime on stdin.
fn spawn_input_thread() -> mpsc::UnboundedReceiver<Event> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        loop {
            match event::poll(Duration::from_millis(250)) {
                Ok(true) => match event::read() {
                    Ok(event) => {
                        if tx.send(event).is_err() {
                            break; // receiver dropped; UI is shutting down
                        }
                    }
                    Err(_) => break,
                },
                Ok(false) => {}
                Err(_) => break,
            }
        }
    });
    rx
}
