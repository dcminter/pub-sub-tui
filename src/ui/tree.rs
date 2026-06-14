//! Builds the topic→publisher/consumer tree shown in the UI from an [`AppState`]
//! snapshot.
//!
//! Per the README, only topics with a non-zero message count are listed; under
//! each, publishers with a non-zero published count and consumers (subscriptions)
//! with a non-zero consumed count are shown.

use ratatui::text::{Line, Span};
use tui_tree_widget::TreeItem;

use crate::observe::{AppState, Subscription, Topic};
use crate::ui::theme;

/// Build the tree items for the current state. Identifiers are stable across
/// rebuilds so the open/selected state in `TreeState` is preserved.
pub fn build(state: &AppState) -> Vec<TreeItem<'static, String>> {
    let mut items = Vec::new();

    for (name, topic) in &state.topics {
        if topic.publish_count == 0 {
            continue; // tree shows only topics with messages
        }

        let mut children = Vec::new();

        for (peer, publisher) in &topic.publishers {
            if publisher.published == 0 {
                continue;
            }
            children.push(TreeItem::new_leaf(
                format!("{name}\u{1}pub\u{1}{peer}"),
                publisher_line(peer, publisher.published),
            ));
        }

        for (sub_name, sub) in state.active_consumers_of(name) {
            children.push(TreeItem::new_leaf(
                format!("{name}\u{1}sub\u{1}{sub_name}"),
                consumer_line(sub_name, sub),
            ));
        }

        let item = TreeItem::new(
            format!("topic\u{1}{name}"),
            topic_line(name, topic),
            children,
        )
        .expect("tree child identifiers are unique by construction");
        items.push(item);
    }

    items
}

/// Last path segment of a fully-qualified resource name, for compact display.
fn short(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

fn topic_line(name: &str, topic: &Topic) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{}  ", short(name)), theme::label()),
        Span::styled(format!("[{} msgs]", topic.publish_count), theme::count()),
    ])
}

fn publisher_line(peer: &str, published: u64) -> Line<'static> {
    Line::from(vec![
        Span::styled("\u{25b2} pub ", theme::publisher()),
        Span::styled(peer.to_owned(), theme::label()),
        Span::styled(format!("  ({published} published)"), theme::count()),
    ])
}

fn consumer_line(name: &str, sub: &Subscription) -> Line<'static> {
    let mut spans = vec![
        Span::styled("\u{25bc} sub ", theme::consumer()),
        Span::styled(short(name).to_owned(), theme::label()),
        Span::styled(format!("  ({} consumed)", sub.acked), theme::count()),
    ];
    if sub.live_consumers > 0 {
        spans.push(Span::styled(
            format!("  \u{25cf}{} live", sub.live_consumers),
            theme::consumer(),
        ));
    }
    Line::from(spans)
}
