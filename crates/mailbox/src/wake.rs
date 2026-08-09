//! The wake path: telling an idle Claude Code session that mail has arrived.
//!
//! # The whole mechanism, in one line
//!
//! `publish` → the `serve` daemon writes the subscriber's Claude Code **inbox
//! socket** → the idle session takes a turn.
//!
//! One hop. Two live components and no file, no watch, no hook, no exit code. What
//! used to be here — a sentinel file, a `watchPaths` registration, a `FileChanged`
//! matcher, an `asyncRewake` hook exiting 2, a store re-check to stop it looping, a
//! turn-boundary re-trigger for the edges it lost while busy, and an active probe to
//! find out whether any of it still worked — is all gone
//! ([ADR-0021](../../docs/adr/0021-delete-the-sentinel-fallback.md)).
//!
//! # What crosses the boundary (and what does not)
//!
//! Wake is **payload-free** (ADR-0001, docs/01-wake.md): the frame carries topic
//! NAMES only. The event body never crosses this boundary — it stays in the durable
//! log and is read later by the agent's `read`. Wake is ingress, not authority.
//!
//! The socket *could* carry a body. It must not: a wake that carried its own payload
//! would become a second, unversioned copy of the event, and the agent would have two
//! places to look for the truth.
//!
//! # Delivery IS the wake, so nothing may be sent idly
//!
//! The retired sentinel was only a TRIGGER — the hook it woke re-read the store and
//! exited 0 when there was nothing unread, so a stray write cost a hook process and
//! no model turn. There is no such second opinion here: the message a session
//! receives *is* the turn it takes. So this module sends nothing for an empty unread
//! set, and nothing it would not spend a model turn on.
//!
//! # A session with no inbox socket cannot be woken at all
//!
//! That is a real state — Claude Code decides which sessions bind one — and it is
//! reported ([`WakeOutcome::NoInbox`]) rather than worked around. `mailbox watch` and
//! `mailbox subscribe` refuse up front for exactly this reason: the moment an agent
//! asks to be woken is the moment to tell it that it cannot be, while it is still
//! awake to hear the answer.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use mailbox_protocol::Topic;

use crate::claude_registry::ClaudeRegistry;
use crate::peer::{self, PeerDeliveryError};
use crate::storage::SessionId;

/// The payload-free reminder delivered on the wake wire.
///
/// Topic NAMES only — never a body — so it is safe to surface verbatim to a model. It
/// is a function rather than a format string at the call site so there is ONE
/// definition of the wake wire's content, and one place for the test that pins it
/// payload-free.
pub fn reminder(topics: &[Topic]) -> String {
    let names: Vec<&str> = topics.iter().map(Topic::as_str).collect();
    format!("mail on topic {}", names.join(", "))
}

/// How one session's wake ended.
///
/// Returned rather than logged in place so [`Waker::wake_all`] can aggregate, and so
/// a test can assert each branch directly. Every variant carries what a reader needs
/// to explain the outcome.
#[derive(Debug)]
pub enum WakeOutcome {
    /// Delivered to the session's inbox socket. An idle session takes a turn.
    Delivered,

    /// The session has no inbox socket, so it cannot be woken by anyone. Not an
    /// error here — `watch`/`subscribe` already refuse for this reason, so reaching
    /// it means the socket went away after the agent subscribed.
    NoInbox,

    /// The socket was there and would not take the frame. The event stays durable and
    /// surfaces on the session's next `read`.
    Failed { error: PeerDeliveryError },
}

/// The DAEMON side of wake: delivers a session's unread topic set to its inbox socket.
///
/// Cheap to clone; it holds one directory path. It never opens the database — it is
/// handed the sessions to wake and what they have unread.
#[derive(Debug, Clone)]
pub struct Waker {
    sessions_dir: PathBuf,
}

impl Waker {
    /// Build a waker over Claude Code's sessions directory (`~/.claude/sessions` by
    /// default; see [`crate::claude_registry`]).
    ///
    /// Resolved ONCE, by the daemon at startup, rather than re-read from the
    /// environment per publish — which also means a test can point it at a tempdir and
    /// never touch the developer's real `~/.claude`, nor deliver a test's wake onto a
    /// live session.
    ///
    /// It is a PATH, not a registry: the *contents* are re-read on every publish,
    /// because sessions start, stop and resume constantly and a wake delivered to a
    /// socket that closed a minute ago is a lost wake.
    pub fn new(sessions_dir: impl Into<PathBuf>) -> Self {
        Self {
            sessions_dir: sessions_dir.into(),
        }
    }

    /// Deliver one session's wake.
    ///
    /// `socket` is that session's inbox socket if Claude Code bound one — see
    /// [`ClaudeRegistry::inbox_socket`]. Passing it in (rather than looking it up here)
    /// keeps the decision a pure function of its inputs, so every branch is testable
    /// without a registry on disk.
    ///
    /// An empty `unread` set is never delivered: see the module docs. There is no
    /// anti-loop between here and the model.
    pub fn deliver(&self, unread: &[Topic], socket: Option<&Path>) -> WakeOutcome {
        if unread.is_empty() {
            return WakeOutcome::NoInbox;
        }
        let Some(socket) = socket else {
            return WakeOutcome::NoInbox;
        };
        match peer::deliver(socket, &reminder(unread)) {
            Ok(()) => WakeOutcome::Delivered,
            Err(error) => WakeOutcome::Failed { error },
        }
    }

    /// Wake every session in `unread_by_session`, then log the aggregate outcome.
    ///
    /// Reads Claude Code's session registry ONCE per publish and never caches it.
    pub fn wake_all(&self, unread_by_session: &[(SessionId, Vec<Topic>)], topic: &Topic) {
        let registry = ClaudeRegistry::read_dir(&self.sessions_dir);
        self.wake_all_with_registry(unread_by_session, topic, &registry);
    }

    /// The injectable core of [`Waker::wake_all`], taking the registry rather than
    /// reading it, so the behaviour is testable against a fixture.
    ///
    /// Payload-free: `topic` is used only for the log line. A failure for one session
    /// is logged and skipped — the event is already durable, so delivery must never
    /// fail a publish, and the other subscribers still get their wake.
    pub fn wake_all_with_registry(
        &self,
        unread_by_session: &[(SessionId, Vec<Topic>)],
        topic: &Topic,
        registry: &ClaudeRegistry,
    ) {
        let (mut delivered, mut no_inbox, mut failed) = (0usize, 0usize, 0usize);

        for (session, unread) in unread_by_session {
            match self.deliver(unread, registry.inbox_socket(session)) {
                WakeOutcome::Delivered => delivered += 1,
                WakeOutcome::NoInbox => {
                    no_inbox += 1;
                    warn!(
                        session = session.as_str(),
                        "a subscriber has no Claude Code inbox socket, so it cannot be \
                         woken; the mail is durable and surfaces on its next read"
                    );
                }
                WakeOutcome::Failed { error } => {
                    failed += 1;
                    warn!(
                        session = session.as_str(),
                        error = %error,
                        "could not deliver a subscriber's wake; it will not wake for this \
                         event (the event is durable and surfaces on its next read)"
                    );
                }
            }
        }

        // Log after the decision point: which topic drove the delivery, and how the
        // subscribers actually fared. Never the body.
        info!(
            topic = topic.as_str(),
            delivered, no_inbox, failed, "woke subscribers after publish"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;

    /// Accept one connection on `socket` and hand back whatever line was written.
    fn listen_once(socket: &Path) -> mpsc::Receiver<String> {
        let listener = UnixListener::bind(socket).unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut line = String::new();
                let _ = BufReader::new(stream).read_line(&mut line);
                let _ = tx.send(line);
            }
        });
        rx
    }

    #[test]
    fn reminder_lists_only_topic_names() {
        let topics = [
            Topic::parse("github.pr.o/r#1").unwrap(),
            Topic::parse("github.pr.o/r#2").unwrap(),
        ];
        let line = reminder(&topics);
        assert_eq!(line, "mail on topic github.pr.o/r#1, github.pr.o/r#2");
        // The wake wire is payload-free by construction: there is no field in which a
        // body could be smuggled, and this pins that the line carries none.
        assert!(!line.contains('{'), "the reminder must be payload-free");
    }

    /// The whole wake path, in one hop.
    #[test]
    fn delivers_the_topic_names_to_the_session_inbox() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("inbox.sock");
        let rx = listen_once(&socket);
        let waker = Waker::new(dir.path());
        let topics = [Topic::parse("t.a").unwrap()];

        let outcome = waker.deliver(&topics, Some(socket.as_path()));

        assert!(matches!(outcome, WakeOutcome::Delivered), "{outcome:?}");
        let line = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the session should have received a frame");
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(parsed["message"]["content"], "mail on topic t.a");
    }

    /// A session Claude Code never gave a socket cannot be woken by anyone. That is
    /// reported, not papered over — `watch`/`subscribe` refuse for the same reason, so
    /// reaching this means the socket went away after the agent subscribed.
    #[test]
    fn a_session_without_an_inbox_cannot_be_woken() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path());
        let topics = [Topic::parse("t.a").unwrap()];

        assert!(matches!(waker.deliver(&topics, None), WakeOutcome::NoInbox));
    }

    /// **Delivery IS the turn.** The retired sentinel was only a trigger, so a stray
    /// write cost nothing; here an empty wake would spend a full model turn announcing
    /// nothing, with no anti-loop anywhere to catch it.
    #[test]
    fn an_empty_unread_set_is_never_delivered() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("inbox.sock");
        let rx = listen_once(&socket);
        let waker = Waker::new(dir.path());

        let outcome = waker.deliver(&[], Some(socket.as_path()));

        assert!(matches!(outcome, WakeOutcome::NoInbox), "{outcome:?}");
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "nothing may be written to the socket when there is nothing unread"
        );
    }

    /// A socket that has gone away between the registry read and the write must
    /// surface as a failure, never a silent success that loses a wake.
    #[test]
    fn a_dead_socket_is_reported_not_swallowed() {
        let dir = tempfile::TempDir::new().unwrap();
        let waker = Waker::new(dir.path());
        let topics = [Topic::parse("t.a").unwrap()];

        let outcome = waker.deliver(&topics, Some(&dir.path().join("gone.sock")));

        assert!(matches!(outcome, WakeOutcome::Failed { .. }), "{outcome:?}");
    }
}
