//! Formatting of a captured message payload for the detail view.
//!
//! Picks the most specific representation the bytes fit: pretty-printed and
//! lightly syntax-highlighted JSON, otherwise plain UTF-8 text, otherwise a hex
//! dump as the universal fallback. The highlighter is deliberately hand-rolled
//! (no `syntect`) to keep the dependency surface small.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use crate::ui::theme;

/// A payload rendered for display: a short format label (for the header) and the
/// styled body lines.
pub struct Rendered {
    /// One of `json`, `text`, `hex` — the representation chosen.
    pub label: &'static str,
    /// The styled body, ready to drop into a `Paragraph`.
    pub lines: Vec<Line<'static>>,
}

/// Render `data` as styled lines, choosing JSON → text → hex.
pub fn render_body(data: &[u8]) -> Rendered {
    if data.is_empty() {
        return Rendered {
            label: "empty",
            lines: vec![Line::from(Span::styled("(empty payload)", theme::label()))],
        };
    }
    if let Some(lines) = as_json(data) {
        Rendered {
            label: "json",
            lines,
        }
    } else if let Some(lines) = as_text(data) {
        Rendered {
            label: "text",
            lines,
        }
    } else {
        Rendered {
            label: "hex",
            lines: as_hex(data),
        }
    }
}

/// Parse and pretty-print `data` as JSON, highlighting the result. `None` if the
/// bytes are not valid JSON.
fn as_json(data: &[u8]) -> Option<Vec<Line<'static>>> {
    let value: serde_json::Value = serde_json::from_slice(data).ok()?;
    let pretty = serde_json::to_string_pretty(&value).ok()?;
    Some(pretty.split('\n').map(highlight_json_line).collect())
}

/// Render `data` as plain text lines. `None` unless it is valid UTF-8 with no
/// control characters other than ordinary whitespace.
fn as_text(data: &[u8]) -> Option<Vec<Line<'static>>> {
    let text = std::str::from_utf8(data).ok()?;
    let printable = text
        .chars()
        .all(|c| !c.is_control() || matches!(c, '\t' | '\n' | '\r'));
    if !printable {
        return None;
    }
    Some(
        text.split('\n')
            .map(|line| {
                // Keep tabs visible without breaking column math too badly.
                let line = line.replace('\t', "    ").replace('\r', "");
                Line::from(Span::styled(line, theme::base().fg(Color::White)))
            })
            .collect(),
    )
}

/// A classic `offset  hex×16  |ascii|` dump.
fn as_hex(data: &[u8]) -> Vec<Line<'static>> {
    data.chunks(16)
        .enumerate()
        .map(|(row, chunk)| {
            let mut hex = String::with_capacity(49);
            for (i, byte) in chunk.iter().enumerate() {
                if i == 8 {
                    hex.push(' '); // gap between the two 8-byte groups
                }
                hex.push_str(&format!("{byte:02x} "));
            }
            // Pad a short final row to the full width (16×"xx " + the group gap)
            // so the ascii gutter stays aligned.
            const FULL_WIDTH: usize = 16 * 3 + 1;
            hex.push_str(&" ".repeat(FULL_WIDTH - hex.len()));

            let ascii: String = chunk
                .iter()
                .map(|&b| {
                    if (0x20..0x7f).contains(&b) {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();

            Line::from(vec![
                Span::styled(format!("{:08x}  ", row * 16), theme::base().fg(Color::Cyan)),
                Span::styled(hex, theme::base().fg(Color::Gray)),
                Span::styled(format!(" |{ascii}|"), theme::base().fg(Color::Yellow)),
            ])
        })
        .collect()
}

/// Highlight one line of pretty-printed JSON by scanning its tokens.
fn highlight_json_line(line: &str) -> Line<'static> {
    let chars: Vec<char> = line.chars().collect();
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            let start = i;
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            spans.push(Span::styled(collect(&chars, start, i), theme::base()));
        } else if c == '"' {
            let start = i;
            i += 1;
            let mut escaped = false;
            while i < chars.len() {
                let d = chars[i];
                i += 1;
                if escaped {
                    escaped = false;
                } else if d == '\\' {
                    escaped = true;
                } else if d == '"' {
                    break;
                }
            }
            // A string is a key if the next non-space character is a colon.
            let mut j = i;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            let is_key = chars.get(j) == Some(&':');
            let style = if is_key { json_key() } else { json_string() };
            spans.push(Span::styled(collect(&chars, start, i), style));
        } else if c == '-' || c.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < chars.len()
                && (chars[i].is_ascii_digit() || matches!(chars[i], '.' | 'e' | 'E' | '+' | '-'))
            {
                i += 1;
            }
            spans.push(Span::styled(collect(&chars, start, i), json_number()));
        } else if c.is_ascii_alphabetic() {
            // `true` / `false` / `null`.
            let start = i;
            while i < chars.len() && chars[i].is_ascii_alphabetic() {
                i += 1;
            }
            spans.push(Span::styled(collect(&chars, start, i), json_literal()));
        } else {
            spans.push(Span::styled(c.to_string(), json_punct()));
            i += 1;
        }
    }
    Line::from(spans)
}

fn collect(chars: &[char], start: usize, end: usize) -> String {
    chars[start..end].iter().collect()
}

fn json_key() -> Style {
    theme::base().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

fn json_string() -> Style {
    theme::base().fg(Color::Green)
}

fn json_number() -> Style {
    theme::base().fg(Color::Yellow)
}

fn json_literal() -> Style {
    theme::base().fg(Color::Magenta)
}

fn json_punct() -> Style {
    theme::base().fg(Color::White)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The concatenated text of a rendered line, ignoring styling.
    fn line_text(line: &Line<'static>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn json_is_pretty_printed_and_labelled() {
        let rendered = render_body(br#"{"id":1,"name":"acme"}"#);
        assert_eq!(rendered.label, "json");
        // Pretty-printing breaks the object across multiple lines.
        assert!(rendered.lines.len() > 1, "expected multi-line pretty JSON");
        let joined: String = rendered.lines.iter().map(line_text).collect();
        assert!(joined.contains("\"name\""));
        assert!(joined.contains("acme"));
    }

    #[test]
    fn plain_utf8_is_text() {
        let rendered = render_body(b"hello world\nsecond line");
        assert_eq!(rendered.label, "text");
        assert_eq!(rendered.lines.len(), 2);
        assert_eq!(line_text(&rendered.lines[1]), "second line");
    }

    #[test]
    fn binary_falls_back_to_hex() {
        let rendered = render_body(&[0x00, 0xff, 0x10, b'A']);
        assert_eq!(rendered.label, "hex");
        let text = line_text(&rendered.lines[0]);
        assert!(text.starts_with("00000000"), "offset missing: {text}");
        assert!(text.contains("ff"), "hex bytes missing: {text}");
        assert!(text.contains("|"), "ascii gutter missing: {text}");
    }

    #[test]
    fn empty_payload_is_labelled() {
        let rendered = render_body(b"");
        assert_eq!(rendered.label, "empty");
        assert_eq!(rendered.lines.len(), 1);
    }
}
