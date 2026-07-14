//! github-pr adapter end-to-end through the REAL supervisor (card 10).
//!
//! Spawns an actual `mailbox serve` daemon whose github-pr resolver points at the
//! freshly built `mailbox-github-pr-adapter`, with the adapter's `gh` overridden
//! (`MAILBOX_GH_BIN`) to a FAKE that emits recorded fixtures. So it exercises the
//! exact production chain: CLI `watch github-pr` → serve → supervisor → github-pr
//! resolver → spawn the adapter (with the persisted baseline injected into its
//! config) → the adapter polls `gh`, publishes edges, and emits `Baseline` lines →
//! the host relays those to storage → `read` surfaces the edges.
//!
//! It proves the load-bearing card-10 property THROUGH THE BRIDGE:
//! - a conflict transition surfaces in `read` (edge-triggered publish), and
//! - the baseline round-trips (the `adapter_baseline` row is populated by the
//!   adapter's `Baseline` line), so a persistent conflict fires EXACTLY ONCE even
//!   though the fake keeps reporting conflicting on every subsequent poll.
//!
//! Flakiness discipline mirrors `stub_e2e.rs`: poll the socket for readiness and
//! poll `read`/the DB with bounded deadlines; the daemon child is reaped on drop,
//! tearing down the adapter it spawned.

use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

// ---- locating the binaries under test -----------------------------------------

fn mailbox_bin() -> &'static str {
    env!("CARGO_BIN_EXE_mailbox")
}

/// A `mailbox` command with the AMBIENT session environment stripped.
///
/// `cargo test` inherits the developer's environment, and inside a Claude Code
/// session that includes `CLAUDE_CODE_SESSION_ID` — which `mailbox` legitimately
/// resolves as the caller's session (that is the point of auto-resolution, and
/// `publish` now uses it). A test that did not strip it would run its commands as
/// the DEVELOPER's session and behave differently on a laptop than in CI. So every
/// test subprocess starts with NO session unless the test names one itself.
fn mailbox_command() -> Command {
    let mut cmd = Command::new(mailbox_bin());
    cmd.env_remove("CLAUDE_CODE_SESSION_ID")
        .env_remove("MAILBOX_SESSION_ID");
    cmd
}

/// The freshly built github-pr adapter, beside the `mailbox` bin in the shared
/// target dir. A full `cargo test --workspace` builds it in the build phase; the
/// on-demand build is a fallback for `cargo test -p mailbox` alone.
fn github_pr_adapter_bin() -> String {
    let dir = Path::new(mailbox_bin())
        .parent()
        .expect("mailbox bin has a parent dir")
        .to_path_buf();
    let bin = dir.join("mailbox-github-pr-adapter");
    if !bin.exists() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "-p", "mailbox-github-pr-adapter"])
            .status()
            .expect("build mailbox-github-pr-adapter");
        assert!(
            status.success(),
            "failed to build mailbox-github-pr-adapter"
        );
    }
    bin.to_str().expect("adapter bin path is utf8").to_string()
}

// ---- the fake gh (clamping fixture emitter, same shape as the adapter's own) ---

const FAKE_GH: &str = r#"#!/usr/bin/env bash
set -euo pipefail
dir="${MAILBOX_FAKE_GH_DIR:?MAILBOX_FAKE_GH_DIR must be set}"
case "${1:-}" in
  pr) key="pr" ;;
  api)
    case "${2:-}" in
      */pulls/*/reviews) key="reviews" ;;
      */issues/*/comments) key="issue_comments" ;;
      */pulls/*/comments) key="thread_comments" ;;
      *) echo "fake-gh: unknown api path ${2:-}" >&2; exit 2 ;;
    esac ;;
  *) echo "fake-gh: unsupported invocation: $*" >&2; exit 2 ;;
esac
ctr="$dir/counter.$key"
i=0
if [ -f "$ctr" ]; then i="$(cat "$ctr")"; fi
max=0
for f in "$dir/$key".*; do
  [ -e "$f" ] || continue
  n="${f##*.}"
  case "$n" in (*[!0-9]*) continue ;; esac
  if [ "$n" -gt "$max" ]; then max="$n"; fi
done
use="$i"
if [ "$i" -gt "$max" ]; then use="$max"; fi
echo $((i + 1)) > "$ctr"
f="$dir/$key.$use"
if [ ! -f "$f" ]; then echo "fake-gh: missing fixture $f" >&2; exit 3; fi
cat "$f"
"#;

/// Write the fake gh + its fixtures into a dir: poll 0 mergeable, poll 1+
/// conflicting (the clamp repeats the tail, so conflicting persists on every later
/// poll). Review/comment/thread lists are all empty (only mergeability changes).
fn write_fake_gh(dir: &Path) -> PathBuf {
    let script = dir.join("fake-gh.sh");
    std::fs::write(&script, FAKE_GH).expect("write fake gh");
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).expect("chmod fake gh");

    for seq in ["reviews", "issue_comments", "thread_comments"] {
        std::fs::write(dir.join(format!("{seq}.0")), "[]").unwrap();
    }
    std::fs::write(
        dir.join("pr.0"),
        r#"{"mergeable":"MERGEABLE","statusCheckRollup":[]}"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("pr.1"),
        r#"{"mergeable":"CONFLICTING","statusCheckRollup":[]}"#,
    )
    .unwrap();
    script
}

// ---- daemon helpers (self-contained, like stub_e2e.rs) ------------------------

fn socket_for(db_path: &Path) -> PathBuf {
    db_path.parent().unwrap().join("mailbox.sock")
}

fn wait_for_socket(socket: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if StdUnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "daemon socket {} not ready within {timeout:?}",
            socket.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

struct Daemon {
    child: Child,
    db_path: PathBuf,
    _dir: TempDir,
    _gh_dir: TempDir,
}

impl Daemon {
    fn start() -> Self {
        let dir = TempDir::new().expect("tempdir");
        let gh_dir = TempDir::new().expect("gh tempdir");
        let db_path = dir.path().join("mailbox.db");
        let socket_path = socket_for(&db_path);
        let fake_gh = write_fake_gh(gh_dir.path());

        let child = mailbox_command()
            .arg("serve")
            .env("AGENT_MAILBOX_DB", &db_path)
            // The github-pr resolver runs the freshly built adapter...
            .env("MAILBOX_GH_ADAPTER_BIN", github_pr_adapter_bin())
            // ...and the adapter (inheriting serve's env) reaches the FAKE gh.
            .env("MAILBOX_GH_BIN", &fake_gh)
            .env("MAILBOX_FAKE_GH_DIR", gh_dir.path())
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .spawn()
            .expect("spawn mailbox serve");
        let daemon = Daemon {
            child,
            db_path,
            _dir: dir,
            _gh_dir: gh_dir,
        };
        wait_for_socket(&socket_path, Duration::from_secs(10));
        daemon
    }

    fn run(&self, args: &[&str]) -> Output {
        mailbox_command()
            .args(args)
            .env("AGENT_MAILBOX_DB", &self.db_path)
            .env("RUST_LOG", "error")
            .output()
            .expect("run mailbox client")
    }

    /// The raw `adapter_baseline.baseline` JSON text for the single watch, or
    /// `None` if the row is not yet populated. A direct read proving the adapter's
    /// `Baseline` line reached storage through the host relay.
    fn baseline_row(&self) -> Option<String> {
        let conn = rusqlite::Connection::open(&self.db_path).ok()?;
        conn.query_row("SELECT baseline FROM adapter_baseline", [], |row| {
            row.get::<_, String>(0)
        })
        .ok()
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Graceful teardown: SIGTERM lets `serve` run its supervisor shutdown,
        // which tears down and reaps the github-pr adapter it spawned, so no
        // poller (nor its `gh` child) is orphaned. Fall back to SIGKILL if it does
        // not exit promptly.
        let pid = nix::unistd::Pid::from_raw(self.child.id() as i32);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        for _ in 0..150 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn assert_ok(output: &Output, what: &str) {
    assert!(
        output.status.success(),
        "{what} should exit 0; got {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn parse_json(text: &str) -> serde_json::Value {
    serde_json::from_str(text.trim()).unwrap_or_else(|e| panic!("not JSON: {e}\n{text}"))
}

/// Read `session`'s unread events (advancing its cursor) as a JSON array.
fn read_events(daemon: &Daemon, session: &str) -> Vec<serde_json::Value> {
    let out = daemon.run(&["--json", "read", "--session", session]);
    assert_ok(&out, "read");
    let value = parse_json(&String::from_utf8_lossy(&out.stdout));
    value["events"].as_array().cloned().unwrap_or_default()
}

fn count_conflicts(events: &[serde_json::Value]) -> usize {
    events
        .iter()
        .filter(|e| e["body"]["edge"] == "mergeable_conflicting")
        .count()
}

/// The single watch's running adapter pid from `status`, or `None` if it is not
/// currently `running` with a pid.
fn adapter_pid(daemon: &Daemon, session: &str) -> Option<u64> {
    let out = daemon.run(&["--json", "status", "--session", session]);
    assert_ok(&out, "status");
    let value = parse_json(&String::from_utf8_lossy(&out.stdout));
    value["watches"].as_array()?.first()?["pid"].as_u64()
}

/// Poll `f` until `Some`, or panic after `timeout`.
fn poll_until<T>(what: &str, timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = f() {
            return value;
        }
        assert!(Instant::now() < deadline, "condition never held: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ==== the round-trip proof =====================================================

/// A github-pr watch spawns the real adapter (fake gh), the conflict edge surfaces
/// in `read`, the baseline round-trips into `adapter_baseline`, and the persistent
/// conflict fires EXACTLY ONCE across many polls (baseline dedup through the bridge).
#[test]
fn github_pr_conflict_surfaces_once_and_baseline_round_trips() {
    let daemon = Daemon::start();
    let session = "s1";

    // Full production path: watch → serve → supervisor → resolver → spawn adapter.
    assert_ok(
        &daemon.run(&[
            "watch",
            "github-pr",
            "octocat/hello-world#42",
            "--interval",
            "1",
            "--session",
            session,
        ]),
        "watch github-pr",
    );

    // Accumulate reads until the conflict edge surfaces (poll 0 baselines clean,
    // poll ~1 flips to conflicting → one conflict edge).
    let mut conflicts = 0usize;
    let deadline = Instant::now() + Duration::from_secs(15);
    while conflicts == 0 {
        assert!(
            Instant::now() < deadline,
            "the conflict edge never surfaced through the bridge"
        );
        conflicts += count_conflicts(&read_events(&daemon, session));
        if conflicts == 0 {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    assert_eq!(conflicts, 1, "the conflict transition published once");

    // The baseline round-tripped: the adapter's `Baseline` line reached the
    // `adapter_baseline` row via the host relay, now reflecting conflicting.
    let baseline = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(row) = daemon.baseline_row() {
                break row;
            }
            assert!(
                Instant::now() < deadline,
                "the adapter baseline never reached storage"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    assert!(
        baseline.contains("conflicting"),
        "the persisted baseline should reflect the conflicting state: {baseline}"
    );

    // Keep reading across several more polls: the fake keeps reporting conflicting,
    // but the persisted baseline already says conflicting, so NO further conflict
    // edge is published — edge-triggered exactly-once, end to end through the bridge.
    let watch_more = Instant::now() + Duration::from_secs(4);
    while Instant::now() < watch_more {
        conflicts += count_conflicts(&read_events(&daemon, session));
        std::thread::sleep(Duration::from_millis(400));
    }
    assert_eq!(
        conflicts, 1,
        "a persistent conflict must fire exactly once (baseline dedup through the bridge)"
    );
}

// ==== review item E: no re-fire across a REAL adapter restart ===================

/// The whole ac-10-2 composition end to end: a conflict fires and its baseline
/// round-trips to `adapter_baseline`; then the adapter process is SIGKILLed so the
/// supervisor CRASH-RESTARTS it (re-reading the persisted baseline and injecting it
/// into the new child's config); the restarted adapter, seeing the same conflicting
/// state, re-fires NOTHING. Proves `get_baseline`→inject→new process→no re-fire.
#[test]
fn no_refire_across_a_real_adapter_restart() {
    let daemon = Daemon::start();
    let session = "s1";

    assert_ok(
        &daemon.run(&[
            "watch",
            "github-pr",
            "octocat/hello-world#42",
            "--interval",
            "1",
            "--session",
            session,
        ]),
        "watch github-pr",
    );

    // Wait for the conflict to fire AND its baseline to reach storage.
    let mut conflicts = 0usize;
    poll_until("conflict edge surfaced", Duration::from_secs(15), || {
        conflicts += count_conflicts(&read_events(&daemon, session));
        (conflicts >= 1).then_some(())
    });
    assert_eq!(conflicts, 1, "the conflict fired once before the restart");
    let baseline = poll_until("baseline reached storage", Duration::from_secs(10), || {
        daemon
            .baseline_row()
            .filter(|row| row.contains("conflicting"))
    });
    let _ = baseline;

    // SIGKILL the adapter process out from under the supervisor → crash-restart.
    let pid1 = poll_until(
        "adapter running with a pid",
        Duration::from_secs(10),
        || adapter_pid(&daemon, session),
    );
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid1 as i32),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("SIGKILL the adapter");

    // The supervisor restarts it with a NEW pid (default backoff ~1s), re-injecting
    // the persisted baseline.
    let pid2 = poll_until(
        "adapter restarted with a new pid",
        Duration::from_secs(20),
        || adapter_pid(&daemon, session).filter(|&p| p != pid1),
    );
    assert_ne!(pid1, pid2, "the adapter was crash-restarted");

    // Read across several more polls of the restarted child: the fake still reports
    // conflicting, but the injected baseline already says conflicting → NO re-fire.
    let watch_more = Instant::now() + Duration::from_secs(5);
    while Instant::now() < watch_more {
        conflicts += count_conflicts(&read_events(&daemon, session));
        std::thread::sleep(Duration::from_millis(400));
    }
    assert_eq!(
        conflicts, 1,
        "a real restart re-injects the persisted baseline and must not re-fire the conflict"
    );
}
