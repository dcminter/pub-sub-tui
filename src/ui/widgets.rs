//! The non-tree chrome: the title bar, the statistics panel and the status bar.

use std::time::Instant;

use ratatui::layout::Alignment;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::observe::AppState;
use crate::ui::theme;

/// The grey title bar across the top of the screen.
pub fn title_bar(header: &str) -> Paragraph<'_> {
    Paragraph::new(Line::from(vec![
        Span::styled(" \u{2261} ", theme::bar()),
        Span::styled(header, theme::bar()),
    ]))
    .style(theme::bar())
}

/// The connection indicator shown at the right of the title bar: a green dot when
/// the monitor stream is up, a red "connecting…" when it is not. This is the
/// on-screen signal that an empty view means "not connected", not "no traffic".
pub fn connection_status(connected: bool) -> Paragraph<'static> {
    let (glyph, text, style) = if connected {
        ("\u{25cf} ", "connected", theme::status_ok())
    } else {
        ("\u{27f3} ", "connecting\u{2026}", theme::status_warn())
    };
    Paragraph::new(Line::from(vec![
        Span::styled(glyph, style),
        Span::styled(text, style),
        Span::styled(" ", theme::bar()),
    ]))
    .style(theme::bar())
    .alignment(Alignment::Right)
}

/// The "Statistics" side panel: total topics, connected publishers/consumers,
/// cumulative message totals and the message-buffer headroom. `width` is the
/// panel's outer width, used to flush the numeric values to the right margin.
pub fn statistics(state: &AppState, now: Instant, width: u16) -> Paragraph<'static> {
    let total_published: u64 = state.topics.values().map(|t| t.publish_count).sum();
    let total_consumed: u64 = state.subscriptions.values().map(|s| s.acked).sum();

    // Inner content width (the block's borders take one column on each side).
    let inner = width.saturating_sub(2) as usize;

    let mut lines = vec![
        stat_line("Topics", state.topic_count().to_string(), inner),
        stat_line(
            "Publishers (live)",
            state.connected_publishers(now).to_string(),
            inner,
        ),
        stat_line(
            "Consumers (live)",
            state.connected_consumers().to_string(),
            inner,
        ),
        stat_line("Messages published", total_published.to_string(), inner),
        stat_line("Messages consumed", total_consumed.to_string(), inner),
        stat_line("Buffer free", buffer_free(state), inner),
    ];
    // Guard against a degenerate (zero-width) area producing empty lines.
    lines.retain(|line| !line.spans.is_empty());

    Paragraph::new(lines).style(theme::base()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::border())
            .title(Span::styled(" Statistics ", theme::title())),
    )
}

/// A statistics row: the label flush left, the value flush right against the
/// panel's inner margin, with the gap between them padded out.
fn stat_line(label: &str, value: String, inner: usize) -> Line<'static> {
    // At least one space always separates the label from its value.
    let used = label.chars().count() + value.chars().count();
    let gap = inner.saturating_sub(used).max(1);
    Line::from(vec![
        Span::styled(label.to_owned(), theme::label()),
        Span::styled(" ".repeat(gap), theme::base()),
        Span::styled(value, theme::count()),
    ])
}

/// How much headroom remains in the recent-messages buffer, e.g. `153 / 200`
/// (remaining out of the monitor's configured capacity). Falls back to the raw
/// count until the capacity is known (before the first real snapshot).
fn buffer_free(state: &AppState) -> String {
    let used = state.recent_messages.len();
    let cap = state.recent_buffer_capacity;
    if cap == 0 {
        return used.to_string();
    }
    format!("{} / {}", cap.saturating_sub(used), cap)
}

/// The grey status bar rendering the given key hints (Borland-style red hot-keys).
pub fn status_bar(hints: &[(&'static str, &'static str)]) -> Paragraph<'static> {
    let spans: Vec<Span<'static>> = hints
        .iter()
        .flat_map(|(key, desc)| {
            [
                Span::styled(format!(" {key} "), theme::hotkey()),
                Span::styled(format!("{desc} "), theme::bar()),
            ]
        })
        .collect();

    Paragraph::new(Line::from(spans))
        .style(theme::bar())
        .alignment(Alignment::Left)
}

/// Placeholder shown in the tree area when no topic has any traffic yet.
pub fn empty_placeholder() -> Paragraph<'static> {
    Paragraph::new(vec![
        Line::from(Span::styled("No topics with messages yet.", theme::label())),
        Line::from(Span::styled(
            "Publish through the monitor's proxy and traffic will appear here.",
            theme::base(),
        )),
    ])
    .alignment(Alignment::Center)
    .style(theme::base())
}
