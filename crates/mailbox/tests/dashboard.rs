//! End-to-end guards for `mailbox dashboard`'s wake-health evidence (ADR-0015).
//!
//! The unit tests in `dashboard::wake_health` prove the classifier is right about
//! handwritten log lines. That is not enough on its own: the classifier keys off
//! SUBSTRINGS of messages emitted somewhere else entirely (`cli`'s hook handlers and
//! the `wake` module), and nothing in the type system ties the two together. If one
//! of those messages is reworded, every unit test still passes while the dashboard
//! quietly reports the whole fleet as never woken — the exact failure mode it exists
//! to catch, turned on itself.
//!
//! So these tests drive the REAL commands and classify their REAL log output. They
//! are the anti-drift guard the `wake_health` module docs point at.

mod common;

use std::process::Stdio;

use common::{Env, mailbox_command};
use mailbox::dashboard::{WakeHealth, WakeSummary};

/// Run the `FileChanged` wake hook for `session` at the log level the harness
/// actually uses.
///
/// Deliberately not `Env::wake_hook`, which pins `RUST_LOG=error` to keep its own
/// assertions quiet: the whole point here is the INFO line that lands in
/// `harness.log`, which is exactly what that setting suppresses.
fn wake_hook_logging(env: &Env, session: &str) -> std::process::Output {
    let mut child: std::process::Child = mailbox_command()
        .args(["harness", "wake"])
        .env("AGENT_MAILBOX_DB", env.db_path())
        .env("MAILBOX_SENTINEL_ROOT", env.sentinel_root())
        .env_remove("RUST_LOG")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mailbox harness wake");
    let payload = format!(r#"{{"session_id":"{session}","hook_event_name":"FileChanged"}}"#);
    {
        use std::io::Write;
        let mut stdin = child.stdin.take().expect("wake stdin");
        stdin.write_all(payload.as_bytes()).expect("write payload");
    }
    child.wait_with_output().expect("wake hook output")
}

fn harness_log(env: &Env) -> std::path::PathBuf {
    env.db_path().parent().expect("db dir").join("harness.log")
}

/// A wake hook that really ran must classify as `Verified`.
///
/// This is the anti-drift guard: it fails if the `FileChanged wake:` message is
/// reworded, if the session field shape changes, or if the log path moves — each of
/// which would otherwise turn every healthy session red on the dashboard with no test
/// noticing.
#[test]
fn a_real_wake_hook_run_is_classified_as_verified() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let session = "s-dashboard-verified";
    let topic = "team.dash";
    env.run_ok(&["subscribe", topic, "--session", session], "subscribe");
    // An adapter publish (no session), so the session genuinely has unread and the
    // hook takes its exit-2 "genuine unread mail" branch.
    env.publish(topic);

    let out = wake_hook_logging(&env, session);
    assert_eq!(
        out.status.code(),
        Some(2),
        "a session with unread mail must be woken by the hook"
    );

    let summary = WakeSummary::from_log(&harness_log(&env));
    match summary.get(session) {
        WakeHealth::Verified { hook_runs, wakes } => {
            assert_eq!(hook_runs, 1, "exactly one hook run happened");
            assert_eq!(wakes, 1, "and it was a real wake");
        }
        other => panic!(
            "a real wake hook run must classify as Verified; got {other:?}.\n\
             The wake log message has probably been reworded — see \
             dashboard::wake_health's marker constants.\nlog:\n{}",
            std::fs::read_to_string(harness_log(&env)).unwrap_or_default()
        ),
    }
    assert_eq!(summary.tally(), (1, 0));

    daemon.stop();
}

/// A hook run that found nothing STILL proves the harness is watching, so it must
/// verify too. Classifying only exit-2 runs as healthy would report every idle
/// session — the common case — as deaf.
#[test]
fn a_wake_hook_run_that_found_nothing_still_verifies_the_watch() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let session = "s-dashboard-nothing";
    env.run_ok(
        &["subscribe", "team.quiet", "--session", session],
        "subscribe",
    );

    // No publish: the hook runs, finds nothing unread, and exits 0.
    let out = wake_hook_logging(&env, session);
    assert_eq!(
        out.status.code(),
        Some(0),
        "a caught-up session must not be woken"
    );

    assert!(
        WakeSummary::from_log(&harness_log(&env))
            .get(session)
            .is_verified(),
        "the hook RAN, which is what proves the watch exists — regardless of outcome.\nlog:\n{}",
        std::fs::read_to_string(harness_log(&env)).unwrap_or_default()
    );

    daemon.stop();
}

/// `--once` must render without a TTY and put a session with unread mail in its
/// output. This is the mode used to capture evidence into an issue, so it has to work
/// when stdout is a pipe — which is precisely when a TUI cannot start.
#[test]
fn the_once_snapshot_renders_without_a_tty_and_names_a_session_with_mail() {
    let env = Env::new();
    let daemon = env.start_daemon();

    let session = "s-dashboard-once";
    let topic = "team.once";
    env.run_ok(&["subscribe", topic, "--session", session], "subscribe");
    env.publish(topic);

    let out: std::process::Output = mailbox_command()
        .args(["dashboard", "--once", "--all"])
        .env("AGENT_MAILBOX_DB", env.db_path())
        .env("MAILBOX_SENTINEL_ROOT", env.sentinel_root())
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run dashboard --once");

    assert!(
        out.status.success(),
        "--once must succeed with stdout piped; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("MAILBOX") && text.contains("daemon up"),
        "the snapshot must lead with the headline state; got:\n{text}"
    );
    assert!(
        text.contains(&session[..8.min(session.len())]),
        "the session with unread mail must appear; got:\n{text}"
    );

    daemon.stop();
}

/// The dashboard must still render with the bridge DOWN — the ADR-0015 property, and
/// the reason it reads the store directly instead of going through the socket.
#[test]
fn the_snapshot_still_renders_when_the_daemon_is_down() {
    let env = Env::new();
    // Create the store, then stop the daemon so only the file remains.
    let daemon = env.start_daemon();
    let session = "s-dashboard-down";
    env.run_ok(
        &["subscribe", "team.down", "--session", session],
        "subscribe",
    );
    daemon.stop();

    let out: std::process::Output = mailbox_command()
        .args(["dashboard", "--once", "--all"])
        .env("AGENT_MAILBOX_DB", env.db_path())
        .env("MAILBOX_SENTINEL_ROOT", env.sentinel_root())
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run dashboard --once with the daemon down");

    assert!(
        out.status.success(),
        "a down daemon must not fail the dashboard; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("daemon DOWN"),
        "the down bridge must be the headline, not a silent omission; got:\n{text}"
    );
    assert!(
        text.contains(&session[..8.min(session.len())]),
        "every row must still render with the bridge down; got:\n{text}"
    );
}
