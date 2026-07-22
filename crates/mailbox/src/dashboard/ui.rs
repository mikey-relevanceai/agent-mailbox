//! Rendering for `mailbox dashboard` — a live terminal view, and the plain-text
//! snapshot behind `--once`.
//!
//! Both render the same [`Snapshot`], so the thing you paste into an issue is the
//! thing you were looking at. The text form is the testable one: it is a pure
//! `Snapshot -> String`, so the wording of a verdict — the part a human acts on — is
//! asserted in unit tests rather than eyeballed.

use std::io;
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table};
use ratatui::{DefaultTerminal, Frame};

use crate::dashboard::DashboardError;
use crate::dashboard::snapshot::{DaemonState, SessionRow, Snapshot};
use crate::dashboard::wake_health::WakeHealth;
use crate::storage::StorageConfig;

/// How often the live view re-reads the world.
///
/// Two seconds is fast enough to watch a wake land (they arrive within ~1s of a
/// publish) without re-reading the log hard enough to matter. Every refresh is a
/// fresh read-only snapshot; nothing is cached across ticks, so what is on screen is
/// never stale in a way the timestamp would not show.
const REFRESH: Duration = Duration::from_secs(2);

/// How long to block waiting for a keypress before re-rendering. Short enough that
/// `q` feels instant, long enough that an idle dashboard is not a spin loop.
const INPUT_POLL: Duration = Duration::from_millis(120);

/// How many session ID characters to show. Enough to identify a session at a glance
/// and to match against `mailbox whoami`, without letting one column eat the table.
const ID_WIDTH: usize = 8;

/// Which sessions a view shows.
///
/// Shared by the live view and the text snapshot so `--once` and the TUI can never
/// disagree about what a filter means.
#[derive(Debug, Default, Clone, Copy)]
pub struct Filters {
    /// Show only sessions with no observed wake (the `d` key / `--deaf-only`).
    pub deaf_only: bool,
    /// Include sessions with no live watcher — dead sessions whose subscriptions were
    /// never cleaned up (the `a` key / `--all`).
    pub include_dead: bool,
}

impl Filters {
    fn keeps(&self, row: &SessionRow) -> bool {
        (self.include_dead || row.live_waiter) && (!self.deaf_only || row.health.is_suspect())
    }
}

/// Run the live dashboard until the user quits.
pub fn run(config: &StorageConfig) -> Result<(), DashboardError> {
    // Fail BEFORE taking over the terminal: a store that cannot be opened should
    // print a plain error, not a raw-mode screen that is torn down a moment later.
    let mut snapshot = Snapshot::gather(config)?;

    let mut terminal = ratatui::try_init()?;
    let result = event_loop(&mut terminal, config, &mut snapshot);
    // Restore UNCONDITIONALLY, before propagating: an early `?` here would hand the
    // user back a terminal still in raw mode with no cursor.
    ratatui::restore();
    Ok(result?)
}

fn event_loop(
    terminal: &mut DefaultTerminal,
    config: &StorageConfig,
    snapshot: &mut Snapshot,
) -> io::Result<()> {
    let mut view = Filters::default();
    let mut last = Instant::now();

    loop {
        terminal.draw(|frame| render(frame, snapshot, view))?;

        if event::poll(INPUT_POLL)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Char('d') => view.deaf_only = !view.deaf_only,
                KeyCode::Char('a') => view.include_dead = !view.include_dead,
                KeyCode::Char('r') => last = Instant::now() - REFRESH,
                _ => {}
            }
        }

        if last.elapsed() >= REFRESH {
            // A refresh that fails (the daemon deleted the DB, a permissions change)
            // must not kill the view — keep showing the last good snapshot rather than
            // dropping the user out of the tool mid-diagnosis.
            if let Ok(fresh) = Snapshot::gather(config) {
                *snapshot = fresh;
            }
            last = Instant::now();
        }
    }
}

fn render(frame: &mut Frame, snapshot: &Snapshot, view: Filters) {
    let [header, health, table, footer] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(5),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    frame.render_widget(header_widget(snapshot), header);
    frame.render_widget(health_widget(snapshot), health);
    frame.render_widget(session_table(snapshot, view), table);
    frame.render_widget(footer_widget(view), footer);
}

fn header_widget(snapshot: &Snapshot) -> Paragraph<'_> {
    let daemon = match snapshot.daemon {
        DaemonState::Up => Span::styled("daemon up", Style::new().fg(Color::Green)),
        // The one thing that must never be quiet: every number below is still true,
        // but nothing new will arrive until the bridge is back.
        DaemonState::Down => Span::styled(
            "daemon DOWN",
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    };
    Paragraph::new(Line::from(vec![
        Span::styled("MAILBOX  ", Style::new().add_modifier(Modifier::BOLD)),
        daemon,
        Span::raw(format!(
            "   {} live / {} known   {} watches   {} events",
            snapshot.live_count(),
            snapshot.rows.len(),
            snapshot.watches.len(),
            snapshot.events
        )),
    ]))
    .block(Block::bordered())
}

fn health_widget(snapshot: &Snapshot) -> Paragraph<'_> {
    let mut spans = vec![
        Span::styled("WAKE  ", Style::new().add_modifier(Modifier::BOLD)),
        Span::styled(
            format!("✓ {} verified", snapshot.verified),
            Style::new().fg(Color::Green),
        ),
        Span::raw("   "),
        Span::styled(
            format!("✗ {} no wake seen", snapshot.suspect),
            if snapshot.suspect > 0 {
                Style::new().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::new().fg(Color::DarkGray)
            },
        ),
    ];
    if snapshot.wake_truncated {
        // Say so, or "never woken" silently means "not in the part of the log I read".
        spans.push(Span::styled(
            "   (log tail only)",
            Style::new().fg(Color::DarkGray),
        ));
    }
    Paragraph::new(Line::from(spans)).block(Block::bordered())
}

fn session_table(snapshot: &Snapshot, view: Filters) -> Table<'_> {
    let rows: Vec<Row> = snapshot
        .rows
        .iter()
        .filter(|r| view.keeps(r))
        .map(row_widget)
        .collect();

    Table::new(
        rows,
        [
            Constraint::Length(ID_WIDTH as u16 + 2),
            Constraint::Length(16),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Min(6),
        ],
    )
    .header(
        Row::new(["SESSION", "WAKE", "UNREAD", "WAITER", "INBOX", "WATCH"])
            .style(Style::new().add_modifier(Modifier::BOLD)),
    )
    .block(Block::bordered())
}

fn row_widget(row: &SessionRow) -> Row<'_> {
    let (wake_text, wake_style) = wake_cell(&row.health);
    Row::new(vec![
        Cell::from(short_id(&row.session)),
        Cell::from(wake_text).style(wake_style),
        Cell::from(row.unread.to_string()).style(if row.unread > 0 {
            Style::new().fg(Color::Yellow)
        } else {
            Style::new().fg(Color::DarkGray)
        }),
        Cell::from(if row.live_waiter { "live" } else { "-" }).style(if row.live_waiter {
            Style::new().fg(Color::Green)
        } else {
            Style::new().fg(Color::DarkGray)
        }),
        Cell::from(if row.inbox_registered { "reg" } else { "NO" }).style(
            if row.inbox_registered {
                Style::new().fg(Color::DarkGray)
            } else {
                Style::new().fg(Color::Red)
            },
        ),
        Cell::from(row.watch_interest.to_string()).dark_gray(),
    ])
}

/// The wake column's text and colour. Kept beside [`wake_verdict`] so the live view
/// and the text snapshot cannot describe the same state differently.
fn wake_cell(health: &WakeHealth) -> (String, Style) {
    match health {
        WakeHealth::Verified { wakes, .. } => {
            (format!("✓ {wakes} wakes"), Style::new().fg(Color::Green))
        }
        WakeHealth::NoWakeObserved { bumps, .. } => (
            format!("✗ {bumps} bumps"),
            Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        WakeHealth::Unproven => ("- no data".to_string(), Style::new().fg(Color::DarkGray)),
    }
}

fn footer_widget(view: Filters) -> Paragraph<'static> {
    let on = |label: &str, active: bool| {
        if active {
            format!("{label} ON")
        } else {
            label.to_string()
        }
    };
    Paragraph::new(format!(
        "[q]uit  [r]efresh  {}  {}",
        on("[d] no-wake only", view.deaf_only),
        on("[a] incl. dead sessions", view.include_dead),
    ))
    .dark_gray()
}

/// Shorten a session id for display, without ever silently colliding: an id shorter
/// than the window is shown whole.
fn short_id(session: &str) -> String {
    session.chars().take(ID_WIDTH).collect()
}

/// The one-line verdict for a session's wake health, in words rather than colour.
///
/// This is what a human quotes when they report a problem, so it says what the
/// evidence IS rather than asserting a conclusion the log cannot support (see
/// [`crate::dashboard::wake_health`]).
pub fn wake_verdict(health: &WakeHealth) -> String {
    match health {
        WakeHealth::Verified { hook_runs, wakes } => {
            format!("wake verified ({hook_runs} hook runs, {wakes} wakes)")
        }
        WakeHealth::NoWakeObserved { bumps, retriggers } => format!(
            "NO WAKE OBSERVED ({bumps} sentinel bumps, {retriggers} turn-boundary re-triggers, \
             0 hook runs)"
        ),
        WakeHealth::Unproven => "no evidence yet (never bumped)".to_string(),
    }
}

/// The plain-text snapshot behind `--once`, with the default filters (live sessions).
pub fn render_text(snapshot: &Snapshot) -> String {
    render_text_filtered(snapshot, Filters::default())
}

/// The plain-text snapshot: the same information as the live view, in something that
/// survives a copy-paste into an issue.
///
/// The header counts stay WHOLE-FLEET whatever is filtered — "3 of 45" is the number
/// that matters, and a filtered view that also filtered its own totals would
/// understate the problem. Anything the filter hid is reported as a count at the end,
/// so a narrowed view never reads as a complete one.
pub fn render_text_filtered(snapshot: &Snapshot, filters: Filters) -> String {
    let mut out = String::new();
    let daemon = match snapshot.daemon {
        DaemonState::Up => "up",
        DaemonState::Down => "DOWN",
    };
    out.push_str(&format!(
        "MAILBOX  daemon {daemon}  {} live / {} known sessions  {} watches  {} events\n",
        snapshot.live_count(),
        snapshot.rows.len(),
        snapshot.watches.len(),
        snapshot.events
    ));
    out.push_str(&format!(
        "WAKE     {} verified, {} with no wake observed{}\n\n",
        snapshot.verified,
        snapshot.suspect,
        if snapshot.wake_truncated {
            " (log tail only)"
        } else {
            ""
        }
    ));

    out.push_str(&format!(
        "{:<10} {:<8} {:<8} {:<7} {:<7} {}\n",
        "SESSION", "UNREAD", "WAITER", "INBOX", "WATCH", "WAKE"
    ));
    let mut shown = 0usize;
    for row in snapshot.rows.iter().filter(|r| filters.keeps(r)) {
        shown += 1;
        out.push_str(&format!(
            "{:<10} {:<8} {:<8} {:<7} {:<7} {}\n",
            short_id(&row.session),
            row.unread,
            if row.live_waiter { "live" } else { "-" },
            if row.inbox_registered { "reg" } else { "NO" },
            row.watch_interest,
            wake_verdict(&row.health),
        ));
    }

    // Never let a filtered view read as a complete one.
    let hidden = snapshot.rows.len() - shown;
    if hidden > 0 {
        out.push_str(&format!(
            "\n({hidden} session(s) not shown — {}use --all / --deaf-only to change)\n",
            if filters.include_dead {
                ""
            } else {
                "dead sessions hidden; "
            }
        ));
    }

    if !snapshot.watches.is_empty() {
        out.push_str("\nWATCHES\n");
        for w in &snapshot.watches {
            let pid = w
                .child_pid
                .map(|p| format!("pid {p}"))
                .unwrap_or_else(|| "-".to_string());
            out.push_str(&format!(
                "  {} {}#{}  state={} interest={} {}\n",
                w.kind, w.repo, w.pr, w.state, w.interest, pid
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::snapshot::DaemonState;
    use crate::storage::{Fleet, FleetSession};

    fn snapshot_with(lines: &[&str], sessions: Vec<FleetSession>) -> Snapshot {
        Snapshot::assemble(
            Fleet {
                sessions,
                watches: vec![],
                events: 3,
            },
            crate::dashboard::WakeSummary::from_lines(lines.iter().copied()),
            DaemonState::Up,
            &|_| true,
        )
    }

    fn session(id: &str, unread: u64) -> FleetSession {
        FleetSession {
            session: id.to_string(),
            subscriptions: 1,
            inbox_registered: true,
            unread,
            watch_interest: 0,
        }
    }

    /// The verdict must state the EVIDENCE, and must not claim a session is deaf — the
    /// log cannot prove that (see the wake_health module docs). A dashboard that
    /// overstates is exactly as misleading as the bug it exists to find.
    #[test]
    fn the_no_wake_verdict_reports_evidence_and_never_asserts_deafness() {
        let verdict = wake_verdict(&WakeHealth::NoWakeObserved {
            bumps: 7,
            retriggers: 2,
        });
        assert!(verdict.contains('7') && verdict.contains('2'), "{verdict}");
        assert!(
            verdict.contains("NO WAKE OBSERVED"),
            "the headline must be what was observed: {verdict}"
        );
        assert!(
            !verdict.to_lowercase().contains("deaf"),
            "must not assert a conclusion the log cannot support: {verdict}"
        );
    }

    /// A session with no evidence must read as "no evidence", never as a failure —
    /// otherwise every freshly-started session looks broken.
    #[test]
    fn an_unproven_session_reads_as_no_evidence_not_as_a_failure() {
        let verdict = wake_verdict(&WakeHealth::Unproven);
        assert!(verdict.contains("no evidence"), "{verdict}");
        assert!(!verdict.contains("NO WAKE OBSERVED"), "{verdict}");
    }

    /// The text snapshot is what gets pasted into a bug report, so the failing session
    /// must be present, first, and legible.
    #[test]
    fn the_text_snapshot_puts_the_failing_session_first() {
        let text = render_text(&snapshot_with(
            &[
                "INFO FileChanged wake: genuine unread mail session=healthy1",
                r#"INFO wake: watcher wrote the wake sentinel session="broken01" topics="t""#,
            ],
            vec![session("healthy1", 0), session("broken01", 4)],
        ));

        let body: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with("SESSION"))
            .collect();
        assert!(
            body[1].starts_with("broken01"),
            "the failing session must be the first row; got:\n{text}"
        );
        assert!(text.contains("NO WAKE OBSERVED"), "{text}");
        assert!(
            text.contains("1 verified, 1 with no wake observed"),
            "{text}"
        );
    }

    /// A down daemon must be stated in the text form too — it is the first thing that
    /// explains why nothing is arriving.
    #[test]
    fn a_down_daemon_is_stated_in_the_text_snapshot() {
        let snapshot = Snapshot::assemble(
            Fleet {
                sessions: vec![session("s1", 0)],
                watches: vec![],
                events: 0,
            },
            crate::dashboard::WakeSummary::default(),
            DaemonState::Down,
            &|_| true,
        );
        assert!(render_text(&snapshot).contains("daemon DOWN"));
    }

    /// A truncated log must be disclosed wherever the counts are shown, or "never
    /// woken" quietly means "not in the window I read".
    #[test]
    fn a_truncated_log_is_disclosed_in_the_text_snapshot() {
        let mut snapshot = snapshot_with(&[], vec![session("s1", 0)]);
        snapshot.wake_truncated = true;
        assert!(render_text(&snapshot).contains("log tail only"));
    }

    /// An id shorter than the display window must not be padded into a different id.
    #[test]
    fn a_short_session_id_is_shown_whole() {
        assert_eq!(short_id("abc"), "abc");
        assert_eq!(short_id("0123456789abcdef"), "01234567");
    }
}
