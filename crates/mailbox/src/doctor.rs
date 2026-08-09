//! Is this session reachable? Read the answer, rather than probing for it
//! ([ADR-0021](../../docs/adr/0021-delete-the-sentinel-fallback.md)).
//!
//! # Why this is now a read and not a probe
//!
//! Under the retired sentinel path, wakeability was an emergent property of six
//! components — a file, a `watchPaths` registration, a basename matcher, a hook, its
//! exit code, and a store re-check — any of which could fail silently. Nothing could
//! be *read* to find out whether it still worked, so ADR-0016 built an active probe:
//! bump each sentinel, wait for the hook to stamp an ack, and call a session deaf if
//! it stayed quiet. That probe existed because the mechanism was unobservable.
//!
//! The inbox socket is observable. A session either has one bound or it does not,
//! Claude Code says which in its own registry, and delivery either succeeds or
//! returns an error at the moment of publish. So the question "can this agent be
//! woken?" is answered by reading two facts:
//!
//! 1. Is its process alive? (the process table — a registry file outlives its process)
//! 2. Did Claude Code bind it an inbox socket, and is that socket still there?
//!
//! No bump, no ack, no waiting, no budget, no turn-boundary stamps to tell "busy"
//! from "deaf" — a session that is mid-turn is just as reachable as an idle one,
//! because the message queues rather than needing an edge to land on.
//!
//! # The one fault worth reporting
//!
//! [`Reachability::NoInbox`]: the process is alive, but Claude Code gave it no socket,
//! so **nothing can wake it** — not this bridge, not a peer agent, not anything. It is
//! the only verdict that is a fault, and it is not something the mailbox can fix: the
//! session must be restarted. `mailbox watch` and `mailbox subscribe` refuse up front
//! for the same reason, so in practice this is only reached by a session whose socket
//! went away after it subscribed.

use std::collections::BTreeSet;

use mailbox_protocol::SessionId;

use crate::claude_registry::ClaudeRegistry;

/// Whether a session can be woken, and if not, why not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachability {
    /// Live process, bound inbox socket. A publish to a topic it subscribes to will
    /// reach it.
    Reachable,

    /// Live process, but Claude Code bound it no inbox socket — so nothing can wake
    /// it. **The fault.** Not fixable from here; the session has to be restarted.
    NoInbox,

    /// No live Claude Code process. Normal — sessions end — and NOT a fault. Reporting
    /// a finished session as broken is what once made a real bug look ten times bigger
    /// than it was.
    Gone,
}

impl Reachability {
    /// Whether this verdict is something a human should act on.
    pub fn is_fault(&self) -> bool {
        matches!(self, Reachability::NoInbox)
    }

    /// A stable one-word label for output and `--json`.
    pub fn label(&self) -> &'static str {
        match self {
            Reachability::Reachable => "reachable",
            Reachability::NoInbox => "no-inbox",
            Reachability::Gone => "gone",
        }
    }

    /// What a human should do about it, or `None` when there is nothing to do.
    pub fn remedy(&self) -> Option<&'static str> {
        match self {
            Reachability::Reachable | Reachability::Gone => None,
            Reachability::NoInbox => Some(
                "Claude Code bound this session no inbox socket, so nothing can wake it. \
                 Restart the session. If it persists, the cross-session messaging feature \
                 is off for it — check `claude --version` (2.1.226+) and that none of \
                 CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC / DISABLE_TELEMETRY / \
                 DO_NOT_TRACK / DISABLE_GROWTHBOOK is set.",
            ),
        }
    }
}

/// One session's verdict.
#[derive(Debug, Clone)]
pub struct SessionReport {
    pub session: SessionId,
    pub reachability: Reachability,
    /// Claude Code's display name, when it has one. For humans reading the table.
    pub name: Option<String>,
}

/// The fleet's verdicts, in the order asked for.
#[derive(Debug, Clone, Default)]
pub struct FleetReport {
    pub sessions: Vec<SessionReport>,
}

impl FleetReport {
    /// Sessions that can be woken.
    pub fn reachable(&self) -> usize {
        self.count(Reachability::Reachable)
    }

    /// Sessions that are alive and cannot be woken — **the fault**.
    pub fn no_inbox(&self) -> usize {
        self.count(Reachability::NoInbox)
    }

    /// Sessions whose process has ended. Not a fault.
    pub fn gone(&self) -> usize {
        self.count(Reachability::Gone)
    }

    fn count(&self, want: Reachability) -> usize {
        self.sessions
            .iter()
            .filter(|r| r.reachability == want)
            .count()
    }

    /// Whether anything here needs a human.
    pub fn has_fault(&self) -> bool {
        self.sessions.iter().any(|r| r.reachability.is_fault())
    }
}

/// Read the reachability of `sessions` from `registry`.
///
/// Pure over its inputs — the registry and the live-process set are both injected —
/// so every verdict is unit-testable without a process table or a real `~/.claude`.
pub fn report(
    sessions: &[SessionId],
    registry: &ClaudeRegistry,
    live: &BTreeSet<String>,
) -> FleetReport {
    let sessions = sessions
        .iter()
        .map(|session| {
            let entry = registry.get(session);
            let reachability = if !live.contains(session.as_str()) {
                // A registry entry outlives its process, so liveness is decided by the
                // process table and never by the presence of a file.
                Reachability::Gone
            } else if entry.and_then(|e| e.inbox_socket()).is_some() {
                Reachability::Reachable
            } else {
                Reachability::NoInbox
            };
            SessionReport {
                session: session.clone(),
                reachability,
                name: entry.and_then(|e| e.name.clone()),
            }
        })
        .collect();
    FleetReport { sessions }
}

/// The session ids that currently have a live Claude Code process.
///
/// Claude Code registers every session in `~/.claude/sessions/<pid>.json`, so the id →
/// pid mapping is published first-hand. This used to be reconstructed by shelling out
/// to `ps ax -o args=` and parsing `--session-id` / `--resume` out of Claude Code's own
/// argv, with the parsing rules guessed and pinned by tests; the registry states it
/// outright.
///
/// **The registry is not liveness** — an entry outlives the process that wrote it (this
/// machine held nineteen, spanning five days) — so every entry is confirmed against the
/// process table before it counts.
///
/// Best-effort by design: a session whose Claude Code is too old to register itself is
/// invisible here and will be treated as gone. That is the accepted cost of the 2.1.226+
/// floor ADR-0021 sets.
pub fn live_claude_sessions() -> Option<BTreeSet<String>> {
    let registry = ClaudeRegistry::open().ok()?;
    Some(live_from(&registry))
}

/// The pure half of [`live_claude_sessions`]: which registered sessions are running.
pub fn live_from(registry: &ClaudeRegistry) -> BTreeSet<String> {
    registry
        .sessions()
        .filter(|s| pid_is_alive(s.pid))
        .map(|s| s.session_id.as_str().to_string())
        .collect()
}

/// Whether `pid` names a running process we can see.
///
/// `kill(pid, 0)` performs the existence and permission checks without sending a
/// signal. `EPERM` means the process exists but belongs to someone else — which cannot
/// be one of our own sessions, so it counts as not ours rather than as alive.
fn pid_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}

/// Every session Claude Code currently knows about, for a fleet-wide `doctor --all`.
pub fn registered_sessions(registry: &ClaudeRegistry) -> Vec<SessionId> {
    registry.sessions().map(|s| s.session_id.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Build a registry from literal entries, so the verdicts are tested against the
    /// shape Claude Code actually writes rather than a hand-built struct.
    fn registry_with(dir: &Path, entries: &[(&str, u32, Option<&Path>)]) -> ClaudeRegistry {
        for (session, pid, socket) in entries {
            let socket_field = match socket {
                Some(path) => format!(r#","messagingSocketPath":"{}""#, path.display()),
                None => String::new(),
            };
            std::fs::write(
                dir.join(format!("{pid}.json")),
                format!(
                    r#"{{"pid":{pid},"sessionId":"{session}","name":"n","updatedAt":1{socket_field}}}"#
                ),
            )
            .unwrap();
        }
        ClaudeRegistry::read_dir(dir)
    }

    fn live(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// The three verdicts, and which one is the fault.
    #[test]
    fn a_live_session_with_a_socket_is_reachable_and_one_without_is_the_fault() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("a.sock");
        std::fs::write(&socket, b"").unwrap();
        let registry = registry_with(
            dir.path(),
            &[("has-inbox", 10, Some(&socket)), ("no-inbox", 11, None)],
        );

        let out = report(
            &[SessionId::new("has-inbox"), SessionId::new("no-inbox")],
            &registry,
            &live(&["has-inbox", "no-inbox"]),
        );

        assert_eq!(out.sessions[0].reachability, Reachability::Reachable);
        assert_eq!(out.sessions[1].reachability, Reachability::NoInbox);
        assert!(
            out.has_fault(),
            "a live session nothing can wake is the one thing worth reporting"
        );
        assert_eq!((out.reachable(), out.no_inbox(), out.gone()), (1, 1, 0));
    }

    /// A finished session is NOT a fault. Reporting one as broken is what once made a
    /// real bug look ten times bigger than it was (ADR-0016's lesson, kept).
    #[test]
    fn a_session_whose_process_has_ended_is_gone_not_a_fault() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("a.sock");
        std::fs::write(&socket, b"").unwrap();
        let registry = registry_with(dir.path(), &[("finished", 10, Some(&socket))]);

        // Registered, socket present — but no live process.
        let out = report(&[SessionId::new("finished")], &registry, &live(&[]));

        assert_eq!(out.sessions[0].reachability, Reachability::Gone);
        assert!(!out.has_fault(), "a session that ended is not a fault");
    }

    /// A registry entry outlives its process, so presence in the registry must never
    /// be read as liveness — the mistake ADR-0017 was written about, restated for this
    /// artefact.
    #[test]
    fn a_stale_registry_entry_does_not_count_as_live() {
        let dir = tempfile::TempDir::new().unwrap();
        // pid 1 is alive but is not ours; a pid that cannot exist is plainly dead.
        let registry = registry_with(dir.path(), &[("ghost", 4_000_000_000, None)]);

        assert!(
            live_from(&registry).is_empty(),
            "an entry whose process is gone must not be reported live"
        );
    }
}
