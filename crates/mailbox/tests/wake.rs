//! Wake integration tests: the four card-05 acceptance criteria plus the
//! hardening from review (non-FIFO node, single-waiter-per-session, coalescing
//! drain, payload-free byte channel, missed-kick boundary stress, spurious-kick
//! re-block, stale-FIFO reuse). Everything runs against a real SQLite store in a
//! tempdir (never the real home directory).
//!
//! Two flavours of test appear here:
//! - **Process tests** spawn the real `mailbox wait` binary (via
//!   `CARGO_BIN_EXE_mailbox`) so we exercise the same process boundary the
//!   harness uses. The test process owns the single writer (a [`Bus`] with a
//!   [`Waker`]) and does the publish + kick, exactly as the bridge will.
//! - **Direct tests** drive [`Waiter::wait`] on a background thread so we can
//!   assert internal outcomes ([`WakeReason`], error variants) a process cannot
//!   reveal, and manipulate the FIFO byte-for-byte.
//!
//! Children/threads are always reaped: every spawned child is `wait`ed or killed
//! on timeout, and every waiter thread is woken and joined.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use mailbox::bus::{Bus, SessionId};
use mailbox::storage::{Storage, StorageConfig};
use mailbox::wake::{KickOutcome, WAKE_BYTE, Waiter, WakeError, WakeReason, Waker};
use mailbox_protocol::{AdapterId, GithubPr, Timestamp, Topic};
use serde_json::json;
use tempfile::TempDir;

/// A distinctive marker that ONLY ever appears inside an event body. If it shows
/// up on the wake channel (child stderr), payload-free wake has been violated.
const BODY_MARKER: &str = "SECRET_BODY_MARKER_DO_NOT_LEAK";

fn marked_body() -> serde_json::Value {
    json!({ "secret": BODY_MARKER, "n": 1 })
}

struct Harness {
    // The bus holds its own `Storage` clone, so the single writer stays alive as
    // long as the bus does — no separate handle is needed here.
    bus: Bus,
    config: StorageConfig,
    _dir: TempDir,
}

/// A fresh store + waker-backed bus in a tempdir. The spawned child's
/// `AGENT_MAILBOX_DB` env points at the same DB path so it derives the same
/// waiters dir.
async fn harness() -> Harness {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("mailbox.db");
    let config = StorageConfig::at(&path);
    let storage = Storage::open(config.clone()).await.expect("open storage");
    let bus = Bus::with_waker(storage, Waker::new(config.waiters_dir()));
    Harness {
        bus,
        config,
        _dir: dir,
    }
}

impl Harness {
    fn waiter(&self, session: &str) -> Waiter {
        Waiter::new(
            self.config.waiters_dir(),
            self.config.path().to_path_buf(),
            SessionId::new(session),
        )
    }

    async fn subscribe(&self, session: &str, topic: &Topic) {
        self.bus
            .subscribe(SessionId::new(session), std::slice::from_ref(topic))
            .await
            .unwrap();
    }

    async fn publish(&self, topic: &Topic, ts: i64, body: serde_json::Value) {
        self.bus
            .publish(topic.clone(), adapter(), Timestamp(ts), body)
            .await
            .unwrap();
    }
}

fn topic(n: u64) -> Topic {
    GithubPr::new("octocat", "hello-world", n).unwrap().topic()
}

fn adapter() -> AdapterId {
    AdapterId("github-watch".to_string())
}

// ---- process-test helpers ----------------------------------------------------

/// Spawn `mailbox wait --session <id>` pointed at this harness's DB. With
/// `debug`, sets `MAILBOX_WAIT_DEBUG=1` so the child appends its wake reason to
/// stderr (test-only; not the harness-facing reminder).
fn spawn_waiter(h: &Harness, session: &str, debug: bool) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mailbox"));
    cmd.args(["wait", "--session", session])
        .env("AGENT_MAILBOX_DB", h.config.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if debug {
        cmd.env("MAILBOX_WAIT_DEBUG", "1");
    }
    cmd.spawn().expect("spawn waiter")
}

struct Exit {
    code: Option<i32>,
    stderr: String,
}

/// Wait for `child` to exit within `timeout`, killing (and reaping) it if it
/// overruns so no stray process survives the test.
fn await_exit(mut child: Child, timeout: Duration) -> Exit {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                return Exit {
                    code: status.code(),
                    stderr,
                };
            }
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("waiter did not exit within {timeout:?}; killed");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// Give a freshly started waiter a moment to reach its blocking `poll`, so a
/// following publish exercises the KICK path rather than the pre-existing-unread
/// path. Bounded and generous; correctness does not depend on it (the
/// open→check→block ordering is race-free either way), only which path is hit.
fn let_waiter_block() {
    std::thread::sleep(Duration::from_millis(300));
}

// ---- direct-test helpers -----------------------------------------------------

type WaitResult = Result<mailbox::wake::WakeOutcome, WakeError>;

/// Run `waiter.wait()` on a background thread, returning the result channel and
/// the join handle. The result is sent when `wait` returns.
fn spawn_wait_thread(waiter: Waiter) -> (mpsc::Receiver<WaitResult>, std::thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _ = tx.send(waiter.wait());
    });
    (rx, handle)
}

/// Block until `path` exists (the waiter has created its FIFO), or panic.
fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() >= deadline {
            panic!("path {} did not appear within {timeout:?}", path.display());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Open a FIFO read-write, non-blocking — shares the single per-inode pipe
/// buffer with the waiter, so writes here are what the waiter drains.
fn open_prober(path: &Path) -> File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(nix::libc::O_NONBLOCK)
        .open(path)
        .expect("open prober")
}

/// Block until a reader has the FIFO open — i.e. an `O_WRONLY | O_NONBLOCK` open
/// succeeds instead of returning `ENXIO`. This is a far more robust "the waiter
/// is up" signal than a fixed sleep: it eliminates process-startup / thread-start
/// / `mkfifo` variance under parallel load, leaving only the tiny gap between the
/// waiter opening the FIFO and completing its first (empty) unread check, which a
/// small `margin` afterwards covers. The probe writes NO byte and closes its
/// write end immediately (harmless — the waiter holds its own writer via O_RDWR).
fn wait_until_reader_present(fifo: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(fifo)
        {
            Ok(_writer) => return,
            Err(e)
                if e.raw_os_error() == Some(nix::libc::ENXIO)
                    || e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("probing FIFO {}: {e}", fifo.display()),
        }
        if Instant::now() >= deadline {
            panic!("no reader on {} within {timeout:?}", fifo.display());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Margin after a reader is present to let the waiter finish its first (empty)
/// unread check and reach the blocking `poll`, so a following kick exercises the
/// KICK path. Generous because a cold read-only SQLite open can be slow under
/// parallel load; only the reason label depends on it, never correctness.
fn margin_after_reader_present() {
    std::thread::sleep(Duration::from_millis(400));
}

// ==== AC1: publish wakes a blocked waiter (exit 2, topic on stderr, < 1s) ======

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac1_publish_wakes_blocked_waiter_via_kick_path() {
    let h = harness().await;
    let t = topic(1);
    h.subscribe("sess-ac1", &t).await;

    let fifo = h.waiter("sess-ac1").fifo_path().to_path_buf();
    let child = spawn_waiter(&h, "sess-ac1", true);
    // Wait until the child has its FIFO open and has had time to reach the
    // blocking poll, so this publish deterministically drives the KICK path.
    wait_until_reader_present(&fifo, Duration::from_secs(5));
    margin_after_reader_present();

    let published = Instant::now();
    h.publish(&t, 0, marked_body()).await;

    let exit = await_exit(child, Duration::from_secs(5));
    assert_eq!(exit.code, Some(2), "stderr: {}", exit.stderr);
    assert!(
        published.elapsed() < Duration::from_secs(1),
        "waiter took {:?} to wake (> 1s)",
        published.elapsed()
    );
    assert!(exit.stderr.contains(t.as_str()), "stderr: {}", exit.stderr);
    // Deterministically the KICK path (it was blocked before the publish).
    assert!(
        exit.stderr.contains("kicked"),
        "expected the kicked reason; stderr: {}",
        exit.stderr
    );
}

// ==== AC2: a waiter started with unread already present exits 2 immediately ====

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac2_waiter_with_preexisting_unread_exits_immediately() {
    let h = harness().await;
    let t = topic(2);
    h.subscribe("sess-ac2", &t).await;
    // Publish BEFORE the waiter starts: the kick finds no reader (no-op), so the
    // ONLY thing that can wake the waiter is its unread check.
    h.publish(&t, 0, marked_body()).await;

    let started = Instant::now();
    let child = spawn_waiter(&h, "sess-ac2", true);
    let exit = await_exit(child, Duration::from_secs(5));

    assert_eq!(exit.code, Some(2), "stderr: {}", exit.stderr);
    assert!(exit.stderr.contains(t.as_str()), "stderr: {}", exit.stderr);
    // Deterministically the EXISTING-UNREAD path (never blocked on the FIFO).
    assert!(
        exit.stderr.contains("existing-unread"),
        "expected the existing-unread reason; stderr: {}",
        exit.stderr
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "waiter took {:?} despite pre-existing unread",
        started.elapsed()
    );
}

// ==== AC3: ten rapid publishes → exactly ONE exit; a later read returns all ====

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac3_ten_publishes_coalesce_to_one_wake_and_read_returns_all() {
    let h = harness().await;
    let t = topic(3);
    h.subscribe("sess-ac3", &t).await;

    let child = spawn_waiter(&h, "sess-ac3", false);
    let_waiter_block();

    for i in 0..10 {
        h.publish(&t, i, json!({ "i": i })).await;
    }

    let exit = await_exit(child, Duration::from_secs(5));
    assert_eq!(exit.code, Some(2), "stderr: {}", exit.stderr);

    // The waiter advanced NO cursor (read-only), so a subsequent read by the
    // agent drains all ten durable events.
    let delivery = h.bus.read(SessionId::new("sess-ac3"), None).await.unwrap();
    assert_eq!(
        delivery.len(),
        10,
        "a read after one wake must return all ten"
    );
}

// ==== AC4: no event body ever crosses the wake channel (process + byte level) ==

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ac4_wake_channel_is_payload_free_on_stderr() {
    let h = harness().await;
    let t = topic(4);
    h.subscribe("sess-ac4", &t).await;

    let child = spawn_waiter(&h, "sess-ac4", false);
    let_waiter_block();
    h.publish(&t, 0, marked_body()).await;

    let exit = await_exit(child, Duration::from_secs(5));
    assert_eq!(exit.code, Some(2), "stderr: {}", exit.stderr);
    assert!(exit.stderr.contains(t.as_str()), "stderr: {}", exit.stderr);
    assert!(
        !exit.stderr.contains(BODY_MARKER),
        "event body leaked across the wake channel: {}",
        exit.stderr
    );
}

/// Byte-level payload-free: a kick writes EXACTLY `[WAKE_BYTE]` and nothing else.
#[test]
fn ac4_kick_writes_exactly_one_wake_byte() {
    let dir = TempDir::new().unwrap();
    let waiters = dir.path().join("waiters");
    std::fs::create_dir_all(&waiters).unwrap();
    let waker = Waker::new(&waiters);
    let session = SessionId::new("byte-test");

    // Build a waiter only to learn the FIFO path deterministically, then create
    // the node and act as its reader.
    let waiter = Waiter::new(&waiters, dir.path().join("mailbox.db"), session.clone());
    let fifo = waiter.fifo_path().to_path_buf();
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let mut reader = open_prober(&fifo);

    assert_eq!(waker.kick(&session), KickOutcome::Delivered);

    let mut buf = [0u8; 8];
    let n = reader.read(&mut buf).expect("read wake byte");
    assert_eq!(
        &buf[..n],
        &[WAKE_BYTE],
        "the channel carries only the wake byte"
    );
    // Nothing else buffered.
    match reader.read(&mut buf) {
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        other => panic!("expected no further bytes, got {other:?}"),
    }
}

/// A kick with no live reader reports `NoReader` (and never errors a publish).
#[test]
fn kick_with_no_reader_reports_no_reader() {
    let dir = TempDir::new().unwrap();
    let waker = Waker::new(dir.path().join("waiters"));
    // No FIFO node exists at all.
    assert_eq!(waker.kick(&SessionId::new("absent")), KickOutcome::NoReader);
}

// ==== A (HIGH): a regular file at the FIFO path errors, does NOT hot-spin ======

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regular_file_at_fifo_path_errors_without_spinning() {
    let h = harness().await;
    let waiter = h.waiter("sess-regfile");
    let fifo = waiter.fifo_path().to_path_buf();
    // Pre-create a REGULAR FILE where the FIFO should be.
    std::fs::create_dir_all(fifo.parent().unwrap()).unwrap();
    std::fs::write(&fifo, b"not a fifo").unwrap();

    // Run on a thread and require it to return promptly — a spin would never
    // return and the recv would time out.
    let (rx, handle) = spawn_wait_thread(waiter);
    let result = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("waiter must return promptly, not hot-spin");
    handle.join().unwrap();

    assert!(
        matches!(result, Err(WakeError::NotAFifo { .. })),
        "expected NotAFifo, got {result:?}"
    );
}

// ==== B (MEDIUM): a second waiter for the same session refuses fast ============

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_waiter_for_same_session_refuses() {
    let h = harness().await;
    let t = topic(30);
    h.subscribe("sess-dup", &t).await;

    // Waiter 1 acquires the lock and blocks (no unread yet).
    let waiter1 = h.waiter("sess-dup");
    let fifo = waiter1.fifo_path().to_path_buf();
    let (rx1, handle1) = spawn_wait_thread(waiter1);
    // FIFO existing implies the lock was already acquired (lock precedes FIFO).
    wait_for_path(&fifo, Duration::from_secs(3));

    // Waiter 2 (same session) must refuse fast rather than attach to the FIFO.
    let result2 = h.waiter("sess-dup").wait();
    assert!(
        matches!(result2, Err(WakeError::AlreadyWaiting { .. })),
        "expected AlreadyWaiting, got {result2:?}"
    );

    // Reap waiter 1: give it mail so it returns (either wake path is fine here —
    // the point of this test is that waiter 2 refused, above).
    h.publish(&t, 0, json!({ "i": 0 })).await;
    rx1.recv_timeout(Duration::from_secs(5))
        .expect("waiter 1 should wake")
        .expect("waiter 1 ok");
    handle1.join().unwrap();
}

// ==== I: direct Kicked path + full drain (coalescing at the byte level) ========

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_returns_kicked_once_and_drains_all_buffered_bytes() {
    // A NON-waker bus so publish does not inject its own kick byte — this test
    // owns every byte in the FIFO, so "the waiter drained exactly what I wrote"
    // is a clean assertion.
    let dir = TempDir::new().unwrap();
    let config = StorageConfig::at(dir.path().join("mailbox.db"));
    let storage = Storage::open(config.clone()).await.unwrap();
    let bus = Bus::new(storage);
    let t = topic(31);
    let session = SessionId::new("sess-drain");
    bus.subscribe(session.clone(), std::slice::from_ref(&t))
        .await
        .unwrap();

    let waiter = Waiter::new(
        config.waiters_dir(),
        config.path().to_path_buf(),
        session.clone(),
    );
    let fifo = waiter.fifo_path().to_path_buf();
    let (rx, handle) = spawn_wait_thread(waiter);
    wait_until_reader_present(&fifo, Duration::from_secs(5));
    // Share the single per-inode pipe buffer with the waiter.
    let mut prober = open_prober(&fifo);
    margin_after_reader_present();

    // Make mail durable (no kick — non-waker bus), THEN place ten coalesced kicks
    // atomically. The single write is < PIPE_BUF, so all ten land at once.
    bus.publish(t.clone(), adapter(), Timestamp(0), json!({ "i": 0 }))
        .await
        .unwrap();
    prober.write_all(&[WAKE_BYTE; 10]).unwrap();

    let outcome = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("waiter should wake")
        .expect("waiter ok");
    assert_eq!(outcome.reason(), WakeReason::Kicked);
    handle.join().unwrap();

    // The waiter drained every buffered byte on that one wake: nothing is left in
    // the shared pipe.
    let mut buf = [0u8; 16];
    match prober.read(&mut buf) {
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        other => panic!("expected a fully drained pipe, got {other:?}"),
    }
}

// ==== I: a spurious kick with no unread re-blocks (does not exit) ==============

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spurious_kick_with_no_unread_reblocks() {
    let h = harness().await;
    let t = topic(32);
    h.subscribe("sess-spurious", &t).await;

    let waiter = h.waiter("sess-spurious");
    let fifo = waiter.fifo_path().to_path_buf();
    let (rx, handle) = spawn_wait_thread(waiter);
    wait_until_reader_present(&fifo, Duration::from_secs(5));
    let mut prober = open_prober(&fifo);
    margin_after_reader_present();

    // A kick with NOTHING unread: the waiter drains it, re-checks, finds nothing,
    // and must go back to blocking rather than exiting.
    prober.write_all(&[WAKE_BYTE]).unwrap();
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "spurious kick must NOT wake the session"
    );

    // Now real mail arrives (publish auto-kicks via the waker bus): it wakes.
    h.publish(&t, 0, json!({ "i": 0 })).await;
    let outcome = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("waiter should wake on real mail")
        .expect("waiter ok");
    assert_eq!(outcome.reason(), WakeReason::Kicked);
    handle.join().unwrap();
}

// ==== I: a second waiter reuses a stale FIFO node with no false wake ===========

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_fifo_is_reused_across_restarts_without_false_wake() {
    let h = harness().await;
    let t = topic(33);
    h.subscribe("sess-reuse", &t).await;

    // Waiter 1: block, then wake on the publish's auto-kick, leaving the FIFO
    // node on disk.
    let waiter1 = h.waiter("sess-reuse");
    let fifo = waiter1.fifo_path().to_path_buf();
    let (rx1, handle1) = spawn_wait_thread(waiter1);
    wait_until_reader_present(&fifo, Duration::from_secs(5));
    margin_after_reader_present();
    h.publish(&t, 0, json!({ "i": 0 })).await;
    rx1.recv_timeout(Duration::from_secs(5))
        .expect("waiter 1 wakes")
        .expect("ok");
    handle1.join().unwrap();

    // Drain the unread so waiter 2 starts with a clean slate.
    assert_eq!(
        h.bus
            .read(SessionId::new("sess-reuse"), None)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(fifo.exists(), "the FIFO node should persist for reuse");

    // Waiter 2 reuses the same FIFO node. No residual byte may falsely wake it.
    let waiter2 = h.waiter("sess-reuse");
    let (rx2, handle2) = spawn_wait_thread(waiter2);
    wait_until_reader_present(&fifo, Duration::from_secs(5));
    margin_after_reader_present();
    assert!(
        matches!(rx2.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "reused FIFO must not falsely wake waiter 2"
    );

    // A fresh publish (auto-kick) still wakes it.
    h.publish(&t, 1, json!({ "i": 1 })).await;
    rx2.recv_timeout(Duration::from_secs(5))
        .expect("waiter 2 wakes")
        .expect("ok");
    handle2.join().unwrap();
}

// ==== E: a zero-subscriber publish is a clean no-op ============================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_subscriber_publish_is_a_clean_noop() {
    let h = harness().await;
    let t = topic(34);
    // Nobody subscribed: publish must still succeed (it kicks no one).
    h.publish(&t, 0, json!({ "i": 0 })).await;
    // And a subscriber that arrives afterwards baselines to head (no replay).
    h.subscribe("late", &t).await;
    assert!(
        h.bus
            .read(SessionId::new("late"), None)
            .await
            .unwrap()
            .is_empty()
    );
}

// ==== I: missed-kick boundary stress (publish immediately after spawn) =========

/// The adversarial ran 120 clean trials; encode a bounded regression. Each trial
/// spawns a waiter and publishes with tiny jitter around its startup — the exact
/// window the open→check→block ordering must cover — and every trial must exit 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missed_kick_boundary_stress() {
    let h = harness().await;
    const TRIALS: u64 = 50;
    for i in 0..TRIALS {
        let session = format!("stress-{i}");
        let t = topic(1000 + i);
        h.subscribe(&session, &t).await;

        let child = spawn_waiter(&h, &session, false);
        // Jitter 0–3ms so the publish lands at varying points around startup.
        std::thread::sleep(Duration::from_millis(i % 4));
        h.publish(&t, 0, json!({ "i": i })).await;

        let exit = await_exit(child, Duration::from_secs(5));
        assert_eq!(exit.code, Some(2), "trial {i} stderr: {}", exit.stderr);
    }
}
