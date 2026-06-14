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

/// The "Statistics" side panel: total topics, connected publishers/consumers and
/// cumulative message totals.
pub fn statistics(state: &AppState, now: Instant) -> Paragraph<'static> {
    let total_published: u64 = state.topics.values().map(|t| t.publish_count).sum();
    let total_consumed: u64 = state.subscriptions.values().map(|s| s.acked).sum();

    let rows = [
        ("Topics", state.topic_count() as u64),
        ("Publishers (live)", state.connected_publishers(now) as u64),
        ("Consumers (live)", state.connected_consumers()),
        ("Messages published", total_published),
        ("Messages consumed", total_consumed),
    ];

    let lines: Vec<Line<'static>> = rows
        .into_iter()
        .map(|(label, value)| {
            Line::from(vec![
                Span::styled(format!("{label}: "), theme::label()),
                Span::styled(value.to_string(), theme::count()),
            ])
        })
        .collect();

    Paragraph::new(lines).style(theme::base()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme::border())
            .title(Span::styled(" Statistics ", theme::title())),
    )
}

/// The grey status bar with key hints (Borland-style red hot-keys).
pub fn status_bar() -> Paragraph<'static> {
    let hint = |key: &'static str, desc: &'static str| {
        [
            Span::styled(format!(" {key} "), theme::hotkey()),
            Span::styled(format!("{desc} "), theme::bar()),
        ]
    };
    let spans: Vec<Span<'static>> = [
        hint("\u{2191}\u{2193}", "Move"),
        hint("\u{2190}\u{2192}", "Collapse/Expand"),
        hint("Enter", "Toggle"),
        hint("q", "Quit"),
    ]
    .into_iter()
    .flatten()
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
            "Point your client's PUBSUB_EMULATOR_HOST at this proxy and publish.",
            theme::base(),
        )),
    ])
    .alignment(Alignment::Center)
    .style(theme::base())
}
