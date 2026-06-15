//! The UI-side view-model: input handling and frame rendering over an
//! [`AppState`] snapshot.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ratatui::Frame;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use tui_tree_widget::{Tree, TreeState};

use crate::observe::{AppState, RecentMessage};
use crate::ui::{message_view, theme, tree, widgets};

/// Which panel currently has keyboard focus.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Focus {
    Tree,
    Messages,
}

/// What the messages panel is currently showing.
enum MessageView {
    /// The live (or, when focused, cursor-able) list of recent messages.
    List,
    /// A single message's contents, captured so it stays stable as the buffer
    /// rotates underneath it.
    Detail {
        message: Arc<RecentMessage>,
        scroll: u16,
    },
}

/// Holds transient UI state (tree expansion/selection, panel focus, the messages
/// panel) and the quit flag.
pub struct App {
    header: String,
    tree_state: TreeState<String>,
    focus: Focus,
    /// The message currently selected while the panel is focused, identified by
    /// its stable sequence number (indices shift as the ring buffer rotates).
    selected_seq: Option<u64>,
    message_view: MessageView,
    should_quit: bool,
}

impl App {
    pub fn new(header: String) -> Self {
        Self {
            header,
            tree_state: TreeState::default(),
            focus: Focus::Tree,
            selected_seq: None,
            message_view: MessageView::List,
            should_quit: false,
        }
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    /// Handle a terminal input event against the latest state snapshot.
    pub fn handle_event(&mut self, event: &Event, state: &AppState) {
        let Event::Key(key) = event else { return };
        if key.kind != KeyEventKind::Press {
            return;
        }
        // Raw mode delivers Ctrl-C as a key event rather than a signal.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        // `q` and Ctrl-C always quit, whatever has focus.
        if key.code == KeyCode::Char('q') {
            self.should_quit = true;
            return;
        }
        if key.code == KeyCode::Tab {
            self.toggle_focus(state);
            return;
        }
        match self.focus {
            Focus::Tree => self.handle_tree_key(key.code),
            Focus::Messages => self.handle_messages_key(key.code, state),
        }
    }

    /// Move focus between the tree and the messages panel. Leaving the messages
    /// panel resets it to live tail-following (auto-scroll resumes); entering it
    /// pauses on the newest message.
    fn toggle_focus(&mut self, state: &AppState) {
        match self.focus {
            Focus::Tree => {
                self.focus = Focus::Messages;
                self.message_view = MessageView::List;
                self.selected_seq = state.recent_messages.back().map(|m| m.seq);
            }
            Focus::Messages => {
                self.focus = Focus::Tree;
                self.message_view = MessageView::List;
                self.selected_seq = None;
            }
        }
    }

    fn handle_tree_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Esc => self.should_quit = true,
            KeyCode::Up | KeyCode::Char('k') => {
                self.tree_state.key_up();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.tree_state.key_down();
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.tree_state.key_left();
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.tree_state.key_right();
            }
            KeyCode::Enter | KeyCode::Char(' ') => {
                self.tree_state.toggle_selected();
            }
            KeyCode::Home => {
                self.tree_state.select_first();
            }
            KeyCode::End => {
                self.tree_state.select_last();
            }
            _ => {}
        }
    }

    fn handle_messages_key(&mut self, code: KeyCode, state: &AppState) {
        match &mut self.message_view {
            MessageView::Detail { message, scroll } => match code {
                KeyCode::Esc | KeyCode::Left | KeyCode::Backspace => {
                    self.message_view = MessageView::List;
                }
                KeyCode::Up | KeyCode::Char('k') => *scroll = scroll.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    let max = detail_lines(message, Instant::now()).len() as u16;
                    *scroll = scroll.saturating_add(1).min(max.saturating_sub(1));
                }
                KeyCode::PageUp => *scroll = scroll.saturating_sub(10),
                KeyCode::PageDown => {
                    let max = detail_lines(message, Instant::now()).len() as u16;
                    *scroll = scroll.saturating_add(10).min(max.saturating_sub(1));
                }
                KeyCode::Home => *scroll = 0,
                _ => {}
            },
            MessageView::List => match code {
                KeyCode::Esc => {
                    // Hand focus back to the tree (auto-scroll resumes).
                    self.focus = Focus::Tree;
                    self.selected_seq = None;
                }
                KeyCode::Up | KeyCode::Char('k') => self.move_selection(state, -1),
                KeyCode::Down | KeyCode::Char('j') => self.move_selection(state, 1),
                KeyCode::Home => {
                    self.selected_seq = state.recent_messages.front().map(|m| m.seq);
                }
                KeyCode::End => {
                    self.selected_seq = state.recent_messages.back().map(|m| m.seq);
                }
                KeyCode::Enter => self.open_selected(state),
                _ => {}
            },
        }
    }

    /// Move the selection `delta` rows through the current buffer, by sequence
    /// number so it stays anchored as the ring rotates.
    fn move_selection(&mut self, state: &AppState, delta: isize) {
        let messages = &state.recent_messages;
        if messages.is_empty() {
            self.selected_seq = None;
            return;
        }
        let current = self
            .selected_seq
            .and_then(|seq| index_of_seq(state, seq))
            .unwrap_or(messages.len() - 1) as isize;
        let next = (current + delta).clamp(0, messages.len() as isize - 1) as usize;
        self.selected_seq = messages.get(next).map(|m| m.seq);
    }

    /// Open the selected message in the detail view, capturing it so it survives
    /// the buffer rotating beneath us.
    fn open_selected(&mut self, state: &AppState) {
        if let Some(message) = self
            .selected_seq
            .and_then(|seq| index_of_seq(state, seq))
            .and_then(|index| state.recent_messages.get(index))
        {
            self.message_view = MessageView::Detail {
                message: Arc::clone(message),
                scroll: 0,
            };
        }
    }

    /// Render a full frame from the latest state snapshot. `connected` drives the
    /// title-bar indicator so an empty view reads as "not connected" when it is.
    pub fn render(&mut self, frame: &mut Frame, state: &AppState, connected: bool) {
        let now = Instant::now();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),  // title bar
                Constraint::Min(3),     // body (tree + stats)
                Constraint::Length(10), // recent-messages panel
                Constraint::Length(1),  // status bar
            ])
            .split(frame.area());

        // Fill the desktop with the blue background.
        frame.render_widget(Block::default().style(theme::base()), frame.area());

        let title = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(0), Constraint::Length(16)])
            .split(chunks[0]);
        frame.render_widget(widgets::title_bar(&self.header), title[0]);
        frame.render_widget(widgets::connection_status(connected), title[1]);

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(40), Constraint::Length(32)])
            .split(chunks[1]);

        self.render_tree(frame, body[0], state);
        frame.render_widget(widgets::statistics(state, now), body[1]);

        self.render_messages(frame, chunks[2], state, now);

        frame.render_widget(widgets::status_bar(self.hints()), chunks[3]);
    }

    fn render_tree(&mut self, frame: &mut Frame, area: Rect, state: &AppState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(self.border_style(Focus::Tree))
            .title(Span::styled(
                format!(" Topics ({}) ", state.topic_count()),
                theme::title(),
            ));

        let items = tree::build(state);
        if items.is_empty() {
            frame.render_widget(widgets::empty_placeholder().block(block), area);
            return;
        }

        let widget = Tree::new(&items)
            .expect("top-level tree identifiers are unique by construction")
            .block(block)
            .style(theme::base())
            .highlight_style(theme::selected())
            .highlight_symbol("\u{00bb} ");

        frame.render_stateful_widget(widget, area, &mut self.tree_state);
    }

    fn render_messages(&mut self, frame: &mut Frame, area: Rect, state: &AppState, now: Instant) {
        let focused = self.focus == Focus::Messages;
        let border_style = self.border_style(Focus::Messages);

        if let MessageView::Detail { message, scroll } = &self.message_view {
            let lines = detail_lines(message, now);
            let max = lines.len() as u16;
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(border_style)
                .title(Span::styled(
                    format!(" Message #{} ", message.seq),
                    theme::title(),
                ));
            let paragraph = Paragraph::new(lines)
                .style(theme::base())
                .block(block)
                .scroll(((*scroll).min(max.saturating_sub(1)), 0));
            frame.render_widget(paragraph, area);
            return;
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(Span::styled(
                format!(" Messages ({}) ", state.recent_messages.len()),
                theme::title(),
            ));

        if state.recent_messages.is_empty() {
            let hint = Paragraph::new(Line::from(Span::styled(
                "Published messages appear here as they arrive.",
                theme::label(),
            )))
            .style(theme::base())
            .block(block);
            frame.render_widget(hint, area);
            return;
        }

        let items: Vec<ListItem<'static>> = state
            .recent_messages
            .iter()
            .map(|message| message_row(message, now))
            .collect();

        // When the panel is focused the user drives the selection; otherwise we
        // tail-follow the newest message (selecting it scrolls the list to the
        // bottom, but with no highlight so it reads as a plain live feed).
        let mut list_state = ListState::default();
        if focused {
            let index = self
                .selected_seq
                .and_then(|seq| index_of_seq(state, seq))
                .unwrap_or(state.recent_messages.len() - 1);
            list_state.select(Some(index));
        } else {
            list_state.select(Some(state.recent_messages.len() - 1));
        }

        let mut list = List::new(items).style(theme::base()).block(block);
        if focused {
            list = list
                .highlight_style(theme::selected())
                .highlight_symbol("\u{00bb} ");
        }
        frame.render_stateful_widget(list, area, &mut list_state);
    }

    /// The border style for `panel`: a bright accent when it holds focus.
    fn border_style(&self, panel: Focus) -> ratatui::style::Style {
        if self.focus == panel {
            theme::border_focused()
        } else {
            theme::border()
        }
    }

    /// The status-bar key hints appropriate to the current focus and view.
    fn hints(&self) -> &'static [(&'static str, &'static str)] {
        match (self.focus, &self.message_view) {
            (Focus::Tree, _) => &[
                ("\u{2191}\u{2193}", "Move"),
                ("\u{2190}\u{2192}", "Collapse/Expand"),
                ("Enter", "Toggle"),
                ("Tab", "Messages"),
                ("q", "Quit"),
            ],
            (Focus::Messages, MessageView::List) => &[
                ("\u{2191}\u{2193}", "Select"),
                ("Enter", "View"),
                ("Tab", "Topics"),
                ("Esc", "Back"),
                ("q", "Quit"),
            ],
            (Focus::Messages, MessageView::Detail { .. }) => &[
                ("\u{2191}\u{2193}", "Scroll"),
                ("Esc", "Back"),
                ("Tab", "Topics"),
                ("q", "Quit"),
            ],
        }
    }
}

/// Find the buffer index of the message with sequence number `seq`, if present.
fn index_of_seq(state: &AppState, seq: u64) -> Option<usize> {
    state.recent_messages.iter().position(|m| m.seq == seq)
}

/// One row in the messages list: the fully-qualified topic plus a dim age/size
/// suffix.
fn message_row(message: &RecentMessage, now: Instant) -> ListItem<'static> {
    let suffix = format!(
        "  {}  {}",
        format_size(message.original_len),
        format_age(now.saturating_duration_since(message.seen)),
    );
    ListItem::new(Line::from(vec![
        Span::styled(message.topic.clone(), theme::label()),
        Span::styled(suffix, theme::base()),
    ]))
}

/// The detail view's lines: a header (topic, age, size, format, attributes) then
/// the formatted body.
fn detail_lines(message: &RecentMessage, now: Instant) -> Vec<Line<'static>> {
    let body = message_view::render_body(&message.data);

    let mut size = format!("{} bytes", message.original_len);
    if message.truncated {
        size.push_str(&format!(" (showing first {})", message.data.len()));
    }

    let mut lines = vec![
        labelled("Topic", message.topic.clone()),
        labelled(
            "Age",
            format_age(now.saturating_duration_since(message.seen)),
        ),
        labelled("Size", size),
        labelled("Format", body.label.to_owned()),
    ];

    if !message.attributes.is_empty() {
        lines.push(Line::from(Span::styled("Attributes:", theme::title())));
        for (key, value) in &message.attributes {
            lines.push(Line::from(vec![
                Span::styled(format!("  {key}: "), theme::label()),
                Span::styled(
                    value.clone(),
                    theme::base().fg(ratatui::style::Color::White),
                ),
            ]));
        }
    }

    lines.push(Line::from(Span::styled(
        "\u{2500}".repeat(8),
        theme::border(),
    )));
    lines.extend(body.lines);
    lines
}

/// A `Label: value` header line.
fn labelled(label: &'static str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), theme::label()),
        Span::styled(value, theme::count()),
    ])
}

/// Compact human age: `now`, `3s`, `4m`, `2h`, or `5d`.
fn format_age(age: Duration) -> String {
    let secs = age.as_secs();
    if secs < 1 {
        "now".to_owned()
    } else if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

/// Compact byte size: `512B`, `1.2KB`, `3.4MB`.
fn format_size(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f < KB {
        format!("{bytes}B")
    } else if bytes_f < MB {
        format!("{:.1}KB", bytes_f / KB)
    } else {
        format!("{:.1}MB", bytes_f / MB)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};

    use super::App;
    use crate::observe::{AppState, Observation, PublishedMessage, SubscriptionInfo};

    fn key(code: KeyCode) -> Event {
        Event::Key(KeyEvent::from(code))
    }

    fn published(data: &[u8]) -> PublishedMessage {
        PublishedMessage {
            data: data.to_vec(),
            attributes: Vec::new(),
            original_len: data.len(),
            truncated: false,
        }
    }

    fn buffer_text(terminal: &Terminal<TestBackend>) -> String {
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn renders_topic_tree_and_stats_without_panic() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(
            Observation::AdminSnapshot {
                topics: vec!["projects/p/topics/orders".into()],
                subscriptions: vec![SubscriptionInfo {
                    name: "projects/p/subscriptions/billing".into(),
                    topic: "projects/p/topics/orders".into(),
                }],
            },
            now,
        );
        state.apply(
            Observation::Publish {
                topic: "projects/p/topics/orders".into(),
                peer: "127.0.0.1:5000".parse().ok(),
                messages: (0..7).map(|_| published(b"hi")).collect(),
            },
            now,
        );
        state.apply(
            Observation::Ack {
                subscription: "projects/p/subscriptions/billing".into(),
                peer: "127.0.0.1:6000".parse().ok(),
                messages: 3,
            },
            now,
        );

        let mut app = App::new("test-header".to_owned());
        let mut terminal = Terminal::new(TestBackend::new(100, 28)).unwrap();
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        // Select the topic and expand it so its publisher/consumer children show.
        app.handle_event(&key(KeyCode::Down), &state);
        app.handle_event(&key(KeyCode::Right), &state);
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        let text = buffer_text(&terminal);
        assert!(text.contains("Topics (1)"), "topic count missing: {text}");
        assert!(text.contains("orders"), "topic name missing");
        assert!(text.contains("billing"), "consumer subscription missing");
        assert!(text.contains("Statistics"), "stats panel missing");
        assert!(
            text.contains("Messages (7)"),
            "messages panel missing: {text}"
        );
    }

    #[test]
    fn tab_into_messages_and_enter_opens_json_detail() {
        let now = Instant::now();
        let mut state = AppState::default();
        state.apply(
            Observation::Publish {
                topic: "projects/p/topics/orders".into(),
                peer: "127.0.0.1:5000".parse().ok(),
                messages: vec![published(br#"{"id":42}"#)],
            },
            now,
        );

        let mut app = App::new("h".to_owned());
        let mut terminal = Terminal::new(TestBackend::new(100, 28)).unwrap();
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        // Tab focuses the messages panel (newest selected); Enter opens detail.
        app.handle_event(&key(KeyCode::Tab), &state);
        app.handle_event(&key(KeyCode::Enter), &state);
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        let text = buffer_text(&terminal);
        assert!(text.contains("Message #0"), "detail title missing: {text}");
        assert!(text.contains("json"), "format label missing: {text}");
        assert!(text.contains("\"id\""), "json body missing: {text}");

        // Tab away resumes the list view.
        app.handle_event(&key(KeyCode::Tab), &state);
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();
        assert!(buffer_text(&terminal).contains("Messages (1)"));
    }

    #[test]
    fn topic_tree_nests_by_dotted_name_segments() {
        let now = Instant::now();
        let mut state = AppState::default();
        for topic in [
            "projects/p/topics/acme.orders.created",
            "projects/p/topics/acme.orders.shipped",
            "projects/p/topics/acme.billing.invoiced",
        ] {
            state.apply(
                Observation::Publish {
                    topic: topic.into(),
                    peer: "127.0.0.1:5000".parse().ok(),
                    messages: vec![published(b"x"), published(b"y")],
                },
                now,
            );
        }

        let mut app = App::new("h".to_owned());
        let mut terminal = Terminal::new(TestBackend::new(100, 28)).unwrap();
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        // Collapsed: only the top-level `acme` group is visible; its descendants
        // (the `orders`/`billing` groups) are hidden until it is expanded.
        let text = buffer_text(&terminal);
        assert!(text.contains("acme"), "top-level group missing: {text}");

        // Drill in: select `acme` and expand it.
        app.handle_event(&key(KeyCode::Down), &state);
        app.handle_event(&key(KeyCode::Right), &state);
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();

        let text = buffer_text(&terminal);
        assert!(text.contains("orders"), "orders group not revealed: {text}");
        assert!(
            text.contains("billing"),
            "billing group not revealed: {text}"
        );
    }

    #[test]
    fn renders_placeholder_when_no_traffic() {
        let mut app = App::new("h".to_owned());
        let state = AppState::default();
        let mut terminal = Terminal::new(TestBackend::new(80, 16)).unwrap();
        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();
        assert!(buffer_text(&terminal).contains("No topics with messages yet"));
    }

    #[test]
    fn title_bar_shows_connection_state() {
        let mut app = App::new("h".to_owned());
        let state = AppState::default();
        let mut terminal = Terminal::new(TestBackend::new(80, 16)).unwrap();

        terminal
            .draw(|frame| app.render(frame, &state, false))
            .unwrap();
        assert!(
            buffer_text(&terminal).contains("connecting"),
            "disconnected indicator missing"
        );

        terminal
            .draw(|frame| app.render(frame, &state, true))
            .unwrap();
        let text = buffer_text(&terminal);
        assert!(text.contains("connected"), "connected indicator missing");
        assert!(!text.contains("connecting"), "stale connecting indicator");
    }
}
