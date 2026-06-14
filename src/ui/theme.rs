//! A colour scheme aping the classic Borland Turbo IDE: a blue desktop with
//! light-grey text, yellow highlights, cyan selection, and grey menu/status bars
//! with red hot-keys.

use ratatui::style::{Color, Modifier, Style};

const DESKTOP_BG: Color = Color::Blue;
const TEXT: Color = Color::Gray;
const BRIGHT: Color = Color::White;
const ACCENT: Color = Color::Yellow;
const BAR_BG: Color = Color::Gray;
const BAR_FG: Color = Color::Black;
const HOTKEY: Color = Color::Red;
const SELECT_BG: Color = Color::Cyan;
const SELECT_FG: Color = Color::Black;

/// Base desktop style: everything sits on the blue background by default.
pub fn base() -> Style {
    Style::default().bg(DESKTOP_BG).fg(TEXT)
}

/// Window/panel titles.
pub fn title() -> Style {
    Style::default()
        .bg(DESKTOP_BG)
        .fg(ACCENT)
        .add_modifier(Modifier::BOLD)
}

/// Panel borders.
pub fn border() -> Style {
    Style::default().bg(DESKTOP_BG).fg(BRIGHT)
}

/// A plain, bright label on the desktop.
pub fn label() -> Style {
    Style::default().bg(DESKTOP_BG).fg(BRIGHT)
}

/// A numeric count/statistic on the desktop.
pub fn count() -> Style {
    Style::default()
        .bg(DESKTOP_BG)
        .fg(ACCENT)
        .add_modifier(Modifier::BOLD)
}

/// The publisher glyph/label colour (cyan, like Turbo Vision accents).
pub fn publisher() -> Style {
    Style::default().bg(DESKTOP_BG).fg(Color::Cyan)
}

/// The consumer glyph/label colour (green).
pub fn consumer() -> Style {
    Style::default().bg(DESKTOP_BG).fg(Color::Green)
}

/// The currently-selected tree row.
pub fn selected() -> Style {
    Style::default()
        .bg(SELECT_BG)
        .fg(SELECT_FG)
        .add_modifier(Modifier::BOLD)
}

/// The grey menu/status bar.
pub fn bar() -> Style {
    Style::default().bg(BAR_BG).fg(BAR_FG)
}

/// A red hot-key letter within a bar.
pub fn hotkey() -> Style {
    Style::default()
        .bg(BAR_BG)
        .fg(HOTKEY)
        .add_modifier(Modifier::BOLD)
}
