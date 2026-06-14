//! Builds the topic tree shown in the UI from an [`AppState`] snapshot.
//!
//! Per the README, only topics with a non-zero message count are listed; under
//! each, publishers with a non-zero published count and consumers (subscriptions)
//! with a non-zero consumed count are shown.
//!
//! Topic ids are *hierarchical*: a dotted name like `acme.orders.created` nests
//! under intermediate group nodes (`acme` ▸ `orders` ▸ `created`), so a deep tree
//! of related topics can be drilled into. A node can be both a group *and* a topic
//! in its own right (e.g. `acme.orders` exists alongside `acme.orders.created`);
//! such a node shows its own publishers/consumers as well as its child topics.

use std::collections::BTreeMap;

use ratatui::text::{Line, Span};
use tui_tree_widget::TreeItem;

use crate::observe::{AppState, Subscription, Topic};
use crate::ui::theme;

/// Separator between topic-name segments that defines the hierarchy.
const SEGMENT_SEP: char = '.';

/// One node of the topic-name trie built while assembling the tree.
#[derive(Default)]
struct Node<'a> {
    /// Child segments, keyed by segment text (ordered for stable rendering).
    children: BTreeMap<String, Node<'a>>,
    /// Set when this node is itself a topic: its full name and observed state.
    topic: Option<(&'a str, &'a Topic)>,
}

/// Build the tree items for the current state. Identifiers are stable across
/// rebuilds so the open/selected state in `TreeState` is preserved.
pub fn build(state: &AppState) -> Vec<TreeItem<'static, String>> {
    // Assemble the trie from every topic that has observed messages.
    let mut root = Node::default();
    for (name, topic) in &state.topics {
        if topic.publish_count == 0 {
            continue; // tree shows only topics with messages
        }
        let mut node = &mut root;
        for segment in short(name).split(SEGMENT_SEP) {
            node = node.children.entry(segment.to_owned()).or_default();
        }
        node.topic = Some((name.as_str(), topic));
    }

    root.children
        .iter()
        .map(|(segment, child)| item_for(segment, child, state))
        .collect()
}

/// Recursively turn a trie node into a `TreeItem`. The node's children are its
/// nested topic groups; if the node is also a topic, its own publishers and
/// consumers are appended as leaves.
fn item_for(segment: &str, node: &Node<'_>, state: &AppState) -> TreeItem<'static, String> {
    let mut children: Vec<TreeItem<'static, String>> = node
        .children
        .iter()
        .map(|(child_segment, child)| item_for(child_segment, child, state))
        .collect();

    let id = match node.topic {
        // Distinct id spaces for topic vs. pure-group nodes so a node that is both
        // (a topic with child topics) never collides with itself.
        Some((name, _)) => format!("topic\u{1}{name}"),
        None => format!("group\u{1}{}", group_id(segment, node)),
    };

    match node.topic {
        Some((name, topic)) => {
            children.extend(leaves_for_topic(name, topic, state));
            TreeItem::new(id, topic_line(segment, topic), children)
                .expect("tree child identifiers are unique by construction")
        }
        None => TreeItem::new(id, group_line(segment, node), children)
            .expect("tree child identifiers are unique by construction"),
    }
}

/// The publisher and consumer leaves shown beneath a topic node.
fn leaves_for_topic(name: &str, topic: &Topic, state: &AppState) -> Vec<TreeItem<'static, String>> {
    let mut leaves = Vec::new();

    for (peer, publisher) in &topic.publishers {
        if publisher.published == 0 {
            continue;
        }
        leaves.push(TreeItem::new_leaf(
            format!("{name}\u{1}pub\u{1}{peer}"),
            publisher_line(peer, publisher.published),
        ));
    }

    for (sub_name, sub) in state.active_consumers_of(name) {
        leaves.push(TreeItem::new_leaf(
            format!("{name}\u{1}sub\u{1}{sub_name}"),
            consumer_line(sub_name, sub),
        ));
    }

    leaves
}

/// A stable identifier for a group node: the first descendant topic's full name
/// plus this segment uniquely locates the group within the tree.
fn group_id(segment: &str, node: &Node<'_>) -> String {
    match first_topic(node) {
        Some(name) => format!("{name}\u{1}{segment}"),
        None => segment.to_owned(),
    }
}

/// The fully-qualified name of any topic in this subtree (used only for ids).
fn first_topic<'a>(node: &Node<'a>) -> Option<&'a str> {
    if let Some((name, _)) = node.topic {
        return Some(name);
    }
    node.children.values().find_map(first_topic)
}

/// Total observed messages across every topic in a subtree.
fn subtree_messages(node: &Node<'_>) -> u64 {
    let own = node.topic.map_or(0, |(_, t)| t.publish_count);
    own + node.children.values().map(subtree_messages).sum::<u64>()
}

/// Last path segment of a fully-qualified resource name, for compact display.
fn short(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

fn group_line(segment: &str, node: &Node<'_>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{segment}  "), theme::label()),
        Span::styled(format!("[{} msgs]", subtree_messages(node)), theme::count()),
    ])
}

fn topic_line(segment: &str, topic: &Topic) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{segment}  "), theme::label()),
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
