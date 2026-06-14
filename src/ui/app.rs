//! The UI-side view-model: input handling and frame rendering over an
//! [`AppState`] snapshot.

use std::time::Instant;

use ratatui::Frame;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::{Block, Borders};
use tui_tree_widget::{Tree, TreeState};

use crate::observe::AppState;
use crate::ui::{theme, tree, widgets};

/// Holds transient UI state (tree expansion/selection) and the quit flag.
pub struct App {
    header: String,
    tree_state: TreeState<String>,
    should_quit: bool,
}

impl App {
    pub fn new(header: String) -> Self {
        Self {
            header,
            tree_state: TreeState::default(),
            should_quit: false,
        }
    }

    pub fn should_quit(&self) -> bool {
        self.should_quit
    }

    /// Handle a terminal input event.
    pub fn handle_event(&mut self, event: &Event) {
        let Event::Key(key) = event else { return };
        if key.kind != KeyEventKind::Press {
            return;
        }
        // Raw mode delivers Ctrl-C as a key event rather than a signal.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
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

    /// Render a full frame from the latest state snapshot.
    pub fn render(&mut self, frame: &mut Frame, state: &AppState) {
        let now = Instant::now();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // title bar
                Constraint::Min(3),    // body
                Constraint::Length(1), // status bar
            ])
            .split(frame.area());

        // Fill the desktop with the blue background.
        frame.render_widget(Block::default().style(theme::base()), frame.area());

        frame.render_widget(widgets::title_bar(&self.header), chunks[0]);

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(40), Constraint::Length(32)])
            .split(chunks[1]);

        self.render_tree(frame, body[0], state);
        frame.render_widget(widgets::statistics(state, now), body[1]);

        frame.render_widget(widgets::status_bar(), chunks[2]);
    }

    fn render_tree(&mut self, frame: &mut Frame, area: ratatui::layout::Rect, state: &AppState) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(theme::border())
            .title(ratatui::text::Span::styled(
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
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{Event, KeyCode, KeyEvent};

    use super::App;
    use crate::observe::{AppState, Observation, SubscriptionInfo};

    fn key(code: KeyCode) -> Event {
        Event::Key(KeyEvent::from(code))
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
                messages: 7,
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
        let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
        terminal.draw(|frame| app.render(frame, &state)).unwrap();

        // Select the topic and expand it so its publisher/consumer children show.
        app.handle_event(&key(KeyCode::Down));
        app.handle_event(&key(KeyCode::Right));
        terminal.draw(|frame| app.render(frame, &state)).unwrap();

        let text = buffer_text(&terminal);
        assert!(text.contains("Topics (1)"), "topic count missing: {text}");
        assert!(text.contains("orders"), "topic name missing");
        assert!(text.contains("billing"), "consumer subscription missing");
        assert!(text.contains("Statistics"), "stats panel missing");
    }

    #[test]
    fn renders_placeholder_when_no_traffic() {
        let mut app = App::new("h".to_owned());
        let state = AppState::default();
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal.draw(|frame| app.render(frame, &state)).unwrap();
        assert!(buffer_text(&terminal).contains("No topics with messages yet"));
    }
}
