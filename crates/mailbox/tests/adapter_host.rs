//! Adapter-host integration tests (card 07), exercised against a REAL child
//! process (the `test_adapter` fixture binary).
//!
//! These prove the three acceptance criteria end to end:
//!
//! - AC1 — an adapter started via the transport gets its config, publishes N
//!   events, and exits; all N are durable (and stamped with the host identity).
//! - AC2 — a malformed line from the child is rejected and logged WITHOUT
//!   crashing the bridge; subsequent good lines are still processed.
//! - AC3 — stopping via the host terminates the child (SIGTERM, then SIGKILL
//!   after a grace period) with no orphan process left behind.
//!
//! Real SQLite in a tempdir, real subprocess, bounded timeouts so a hang fails
//! the test rather than wedging CI.

use std::time::Duration;

use mailbox::bus::Bus;
use mailbox::host::subprocess::{AdapterSpec, HostError, HostLimits, SubprocessTransport};
use mailbox::host::{AdapterConfig, AdapterExit, AdapterHost};
use mailbox::storage::{Storage, StorageConfig};
use mailbox_protocol::{AdapterId, Cursor, GithubPr, Timestamp, Topic};
use serde_json::json;
use tempfile::TempDir;

/// SIGTERM / SIGKILL / SIGABRT signal numbers on our targets (Linux + macOS both
/// use these). Asserted on so the exit classification is exact, not just
/// "signalled".
const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;
const SIGABRT: i32 = 6;

/// The identity the HOST stamps at spawn. Distinct from the fixture's
/// self-reported id so a test can prove the host ignores the child's claim.
const SPAWN_IDENTITY: &str = "spawned-identity";

/// Open a fresh bus over a store in a tempdir. Returns the bus, the raw storage
/// handle (to assert durability directly), the DB path (for raw SQL), and the
/// tempdir (kept alive so the files outlive the store).
async fn fresh_bus() -> (Bus, Storage, std::path::PathBuf, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("mailbox.db");
    let storage = Storage::open(StorageConfig::at(&path))
        .await
        .expect("open storage");
    (Bus::new(storage.clone()), storage, path, dir)
}

fn test_topic() -> Topic {
    GithubPr::new("octocat", "hello-world", 1).unwrap().topic()
}

/// A spec that runs the fixture binary under the host identity.
fn fixture_spec() -> AdapterSpec {
    let program = env!("CARGO_BIN_EXE_test_adapter");
    AdapterSpec::new(program, AdapterId(SPAWN_IDENTITY.to_string()))
}

/// Count durable events on a topic by reading its whole log directly.
async fn durable_count(storage: &Storage, topic: &Topic) -> usize {
    storage
        .read_events(topic.clone(), Cursor::Oldest, None)
        .await
        .expect("read events")
        .events
        .len()
}

/// The distinct `adapter` provenance strings stored for a topic's events, read
/// straight from the DB (the domain `Event` type does not surface provenance).
fn stored_adapters(path: &std::path::Path, topic: &Topic) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT DISTINCT adapter FROM event WHERE topic = ?1 ORDER BY adapter")
        .unwrap();
    let rows = stmt
        .query_map([topic.as_str()], |row| row.get::<_, String>(0))
        .unwrap();
    rows.map(Result::unwrap).collect()
}

/// True while `pid` still names a live process. `kill(pid, 0)` performs the
/// permission/existence check without delivering a signal; `ESRCH` means gone.
fn pid_alive(pid: u32) -> bool {
    matches!(
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
        Ok(())
    )
}

/// Poll until `pid` is gone, or fail after a bounded wait — so a lingering
/// orphan fails the test instead of hanging it.
async fn assert_pid_reaped(pid: u32) {
    for _ in 0..100 {
        if !pid_alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("adapter pid {pid} was not reaped; an orphan survived stop()");
}

/// Wrap a host future in a bounded timeout so a wedged child fails the test.
async fn with_timeout<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(Duration::from_secs(30), future)
        .await
        .expect("adapter-host operation timed out")
}

// ---- AC1: config delivered, N events durable, host identity stamped ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac1_config_delivered_and_all_events_durable() {
    let (bus, storage, path, _dir) = fresh_bus().await;
    let topic = test_topic();

    let config = AdapterConfig::new(json!({
        "mode": "publish",
        "count": 5,
        "topic": topic.as_str(),
        "stderr": "adapter starting up",
    }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");

    // Finite adapter: wait for it to publish its 5 events and exit cleanly.
    let exit = with_timeout(transport.wait()).await.expect("wait");
    assert_eq!(exit, AdapterExit::Exited { code: 0 });

    // All five are durable — proving both delivery of the config (the adapter
    // only knows to publish 5 because it read `count` from stdin) and forwarding.
    assert_eq!(durable_count(&storage, &topic).await, 5);

    // Provenance is the HOST identity, not the fixture's self-reported one.
    assert_eq!(
        stored_adapters(&path, &topic),
        vec![SPAWN_IDENTITY.to_string()]
    );
}

// ---- AC2: malformed line rejected, bridge survives, good lines still flow ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_malformed_line_is_skipped_without_crashing_the_bridge() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();

    let config = AdapterConfig::new(json!({
        "mode": "malformed",
        "topic": topic.as_str(),
    }));

    // The fixture emits: good publish, one malformed line, good publish.
    let transport = SubprocessTransport::start(fixture_spec(), config, bus.clone())
        .await
        .expect("start adapter");
    let exit = with_timeout(transport.wait()).await.expect("wait");

    // The adapter exited cleanly; the host did not crash on the bad line.
    assert_eq!(exit, AdapterExit::Exited { code: 0 });

    // The two GOOD publishes are durable — the malformed line between them was
    // skipped and the subsequent good line was still processed.
    assert_eq!(durable_count(&storage, &topic).await, 2);

    // And the bridge is fully alive afterwards: a further publish still works.
    bus.publish(
        topic.clone(),
        AdapterId("after".to_string()),
        Timestamp(0),
        json!({ "after": "the bad line" }),
        None,
    )
    .await
    .expect("bridge still accepts publishes after a malformed adapter line");
    assert_eq!(durable_count(&storage, &topic).await, 3);
}

/// The health snapshot surfaces the rejected-line count for a supervisor: one
/// malformed line is counted (not fatal), two good lines forwarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_health_counts_rejected_and_forwarded_lines() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config = AdapterConfig::new(json!({ "mode": "malformed", "topic": topic.as_str() }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");

    // Poll the live health snapshot (the transport is still owned) until the
    // forwarding settles: two good publishes, one rejected malformed line.
    let health = poll_health(&transport, |h| h.forwarded == 2 && h.rejected == 1).await;
    assert_eq!(health.forwarded, 2, "two good publishes forwarded");
    assert_eq!(health.rejected, 1, "one malformed line rejected");
    assert_eq!(health.publish_failures, 0);

    let exit = with_timeout(transport.wait()).await.expect("wait");
    assert_eq!(exit, AdapterExit::Exited { code: 0 });
}

/// Poll the transport's health snapshot until `pred` holds, or fail after a
/// bounded wait (so a stuck adapter fails the test rather than hanging it).
async fn poll_health(
    transport: &SubprocessTransport,
    pred: impl Fn(mailbox::host::AdapterHealth) -> bool,
) -> mailbox::host::AdapterHealth {
    for _ in 0..500 {
        let health = transport.health();
        if pred(health) {
            return health;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("adapter health never settled: {:?}", transport.health());
}

// ---- Oversized line robustness (no OOM, resync to next line) ------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_line_is_rejected_and_stream_resyncs() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();

    let config = AdapterConfig::new(json!({
        "mode": "oversized",
        "topic": topic.as_str(),
        "size": 100_000,
    }));

    // Tiny cap so the fixture's 100 KB line is over it; the following good line
    // must still be forwarded (stream resynced past the oversized one).
    let limits = HostLimits {
        max_line_bytes: 1024,
        stop_grace: Duration::from_secs(3),
        config_write_timeout: Duration::from_secs(10),
    };
    let transport = SubprocessTransport::start_with_limits(fixture_spec(), config, bus, limits)
        .await
        .expect("start adapter");
    let exit = with_timeout(transport.wait()).await.expect("wait");

    assert_eq!(exit, AdapterExit::Exited { code: 0 });
    // Exactly the one good publish after the oversized line is durable.
    assert_eq!(durable_count(&storage, &topic).await, 1);
}

// ---- AC3: stop terminates the child, no orphan --------------------------------

/// SIGTERM path: a child that sleeps with the default signal disposition is
/// terminated by the host's SIGTERM within the grace, and reaped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_stop_terminates_via_sigterm_no_orphan() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config = AdapterConfig::new(json!({ "mode": "sleep", "topic": topic.as_str() }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");
    assert!(pid_alive(pid), "adapter should be running before stop");

    let exit = with_timeout(transport.stop()).await.expect("stop");
    assert_eq!(
        exit,
        AdapterExit::Signalled { signal: SIGTERM },
        "a cooperative sleeper dies to SIGTERM, not SIGKILL"
    );
    assert_pid_reaped(pid).await;
}

/// SIGKILL path: a child that IGNORES SIGTERM survives the grace, so the host
/// escalates to SIGKILL. A short grace keeps the test fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_stop_escalates_to_sigkill_when_sigterm_ignored() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config = AdapterConfig::new(json!({ "mode": "ignore_sigterm", "topic": topic.as_str() }));

    let limits = HostLimits {
        max_line_bytes: 1024 * 1024,
        stop_grace: Duration::from_millis(300),
        config_write_timeout: Duration::from_secs(10),
    };
    let transport = SubprocessTransport::start_with_limits(fixture_spec(), config, bus, limits)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");

    // Wait for the child's readiness publish, which it emits only AFTER its
    // SIGTERM handler is installed. This removes the start-up race: by the time
    // we stop it, SIGTERM is guaranteed to be ignored.
    poll_health(&transport, |h| h.forwarded >= 1).await;
    assert!(pid_alive(pid), "adapter should be running before stop");

    let exit = with_timeout(transport.stop()).await.expect("stop");
    assert_eq!(
        exit,
        AdapterExit::Signalled { signal: SIGKILL },
        "a child ignoring SIGTERM must be SIGKILLed after the grace"
    );
    assert_pid_reaped(pid).await;
}

// ---- Process-group teardown (the "no zombie pollers" fix, item A) ------------

/// A unique-per-run marker so a grandchild can be found (and asserted gone) with
/// `pgrep -f` without matching anything else on the machine.
fn unique_marker(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("mbx-{tag}-{}-{}", std::process::id(), nanos)
}

/// Whether any process currently has `marker` in its command line.
fn marker_present(marker: &str) -> bool {
    std::process::Command::new("pgrep")
        .arg("-f")
        .arg(marker)
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// Poll until the grandchild marker appears (it is spawned asynchronously).
async fn wait_for_marker_present(marker: &str) {
    for _ in 0..250 {
        if marker_present(marker) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("grandchild marker {marker:?} never appeared");
}

/// Poll until the grandchild marker is gone, or fail — a surviving descendant
/// means the process group was not torn down (an orphan).
async fn assert_marker_gone(marker: &str) {
    for _ in 0..250 {
        if !marker_present(marker) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("grandchild marker {marker:?} is still alive; the process group was not torn down");
}

/// Current process max-RSS high-water mark, in bytes (Linux reports KiB, macOS
/// bytes — normalize).
fn max_rss_bytes() -> u64 {
    let usage = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .expect("getrusage");
    let raw = usage.max_rss() as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

/// The direct child exits while a grandchild it spawned still holds the stdout
/// pipe open. Without the process-group fix, `stop()` HANGS (EOF never arrives)
/// and the grandchild is orphaned. With it: `stop()` returns within a bound and
/// the whole group is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_stop_tears_down_grandchild_and_does_not_hang() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let marker = unique_marker("gc-stop");
    let config = AdapterConfig::new(json!({
        "mode": "spawn_grandchild",
        "topic": topic.as_str(),
        "marker": marker,
        "then": "exit",
    }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");
    // Grandchild is up (and holding stdout); the direct child has since exited.
    wait_for_marker_present(&marker).await;

    // Tight bound: a regression that only signals the direct child would wedge
    // here (the grandchild keeps stdout open forever).
    let exit = tokio::time::timeout(Duration::from_secs(12), transport.stop())
        .await
        .expect("stop() must not hang when a grandchild holds the stdout pipe")
        .expect("stop");
    // The direct child exited on its own before we signalled; either a clean exit
    // or a group-SIGTERM death is acceptable depending on the exact race.
    assert!(matches!(
        exit,
        AdapterExit::Exited { .. } | AdapterExit::Signalled { .. }
    ));

    // No orphan: neither the direct child nor the grandchild survives.
    assert_pid_reaped(pid).await;
    assert_marker_gone(&marker).await;
}

/// Dropping the handle without stop()/wait() still tears down the whole group
/// (the `kill_on_drop` + group-kill backstop): direct child reaped, grandchild
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_handle_reaps_child_and_whole_group() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let marker = unique_marker("gc-drop");
    let config = AdapterConfig::new(json!({
        "mode": "spawn_grandchild",
        "topic": topic.as_str(),
        "marker": marker,
        "then": "sleep",
    }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");
    wait_for_marker_present(&marker).await;
    assert!(
        pid_alive(pid),
        "direct child should be sleeping before drop"
    );

    // No stop()/wait(): the Drop backstop must kill the group and reap.
    drop(transport);

    assert_pid_reaped(pid).await;
    assert_marker_gone(&marker).await;
}

// ---- Config-write to a deaf child (item B) -----------------------------------

/// A child that never reads stdin plus a config larger than the pipe buffer must
/// make `start()` FAIL (config-write timeout), not hang forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_fails_when_deaf_child_never_reads_config() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    // A blob well past any pipe buffer, so `write_all` blocks on a child that
    // never drains stdin.
    let blob = "x".repeat(2 * 1024 * 1024);
    let config = AdapterConfig::new(json!({
        "mode": "sleep",
        "topic": topic.as_str(),
        "blob": blob,
    }));
    let spec = fixture_spec().with_args(["--deaf"]);
    let limits = HostLimits {
        max_line_bytes: 1024 * 1024,
        stop_grace: Duration::from_secs(3),
        config_write_timeout: Duration::from_millis(300),
    };

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        SubprocessTransport::start_with_limits(spec, config, bus, limits),
    )
    .await
    .expect("start() must not hang on a deaf child");

    assert!(
        matches!(result, Err(HostError::ConfigWriteTimeout(_))),
        "expected a config-write timeout, got {result:?}"
    );
}

// ---- Exit reporting: non-zero code and crash signal --------------------------

/// A finite adapter that exits non-zero reports the code, and its publishes are
/// still durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonzero_exit_code_is_reported_and_events_durable() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config = AdapterConfig::new(json!({
        "mode": "publish",
        "count": 3,
        "topic": topic.as_str(),
        "exit_code": 7,
    }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let exit = with_timeout(transport.wait()).await.expect("wait");

    assert_eq!(exit, AdapterExit::Exited { code: 7 });
    assert_eq!(durable_count(&storage, &topic).await, 3);
}

/// A child that crashes (raises SIGABRT on itself) is reported as signalled and
/// reaped cleanly — `wait()` returns rather than hanging on a zombie.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_signal_is_reported_and_reaped_cleanly() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config =
        AdapterConfig::new(json!({ "mode": "crash", "count": 0, "topic": topic.as_str() }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");
    let exit = with_timeout(transport.wait())
        .await
        .expect("wait reaps a crashed child");

    assert_eq!(exit, AdapterExit::Signalled { signal: SIGABRT });
    assert_pid_reaped(pid).await;
}

/// Stopping a child that has ALREADY exited on its own is clean: the group
/// signal hits nothing live (ESRCH — treated as success) and the reap returns
/// the child's real exit rather than a signal death or a hang.
///
/// Determinism: the child publishes one readiness event and then returns from
/// `main` (exit 0). We wait for the host to forward that publish — proving the
/// child reached the end of its work — plus a wide margin for the process to
/// actually `_exit`, so it is reliably gone before `stop()` signals. (The pure
/// ESRCH branch is also covered deterministically by a `subprocess` unit test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_after_child_already_exited_is_clean() {
    let (bus, _storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config =
        AdapterConfig::new(json!({ "mode": "publish", "count": 1, "topic": topic.as_str() }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let pid = transport.pid().expect("child has a pid");

    // The child published its one event then returned; wait for it to be observed
    // and give it a wide margin to fully exit before we stop it.
    poll_health(&transport, |h| h.forwarded >= 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let exit = with_timeout(transport.stop())
        .await
        .expect("stop on an already-exited child");
    assert_eq!(exit, AdapterExit::Exited { code: 0 });
    assert_pid_reaped(pid).await;
}

// ---- Flood robustness (item C) + memory ceiling ------------------------------

/// A child that floods stderr does not wedge or crash the host, and its one
/// stdout publish still lands (the stderr rate-limiting keeps the host reading).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stderr_flood_is_handled_without_wedging() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let config = AdapterConfig::new(json!({
        "mode": "stderr_flood",
        "stderr_lines": 50_000,
        "topic": topic.as_str(),
    }));

    let transport = SubprocessTransport::start(fixture_spec(), config, bus)
        .await
        .expect("start adapter");
    let exit = with_timeout(transport.wait()).await.expect("wait");

    assert_eq!(exit, AdapterExit::Exited { code: 0 });
    assert_eq!(durable_count(&storage, &topic).await, 1);
}

/// A huge single stdout line (no newline) must NOT balloon host memory: the
/// per-line cap discards past the bound. A regression reintroducing full-line
/// buffering would add ~the line size to RSS and trip this ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_stdout_line_does_not_balloon_host_memory() {
    let (bus, storage, _path, _dir) = fresh_bus().await;
    let topic = test_topic();
    let line_bytes: u64 = 200 * 1024 * 1024;
    let config = AdapterConfig::new(json!({
        "mode": "oversized",
        "topic": topic.as_str(),
        "size": line_bytes,
    }));
    let limits = HostLimits {
        max_line_bytes: 1024,
        stop_grace: Duration::from_secs(3),
        config_write_timeout: Duration::from_secs(10),
    };

    let before = max_rss_bytes();
    let transport = SubprocessTransport::start_with_limits(fixture_spec(), config, bus, limits)
        .await
        .expect("start adapter");
    let exit = with_timeout(transport.wait()).await.expect("wait");
    let after = max_rss_bytes();

    assert_eq!(exit, AdapterExit::Exited { code: 0 });
    // The good publish after the oversized line still lands (stream resynced).
    assert_eq!(durable_count(&storage, &topic).await, 1);

    // max_rss is a process-wide high-water mark; the band is wide (far below the
    // line size, far above any parallel-test noise) so it catches a regression
    // without being flaky.
    let delta = after.saturating_sub(before);
    let ceiling = 128 * 1024 * 1024;
    assert!(
        delta < ceiling,
        "host RSS grew {delta} bytes processing a {line_bytes}-byte oversized line; \
         a full-line-buffering regression would add ~{line_bytes} (ceiling {ceiling})"
    );
}
