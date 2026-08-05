//! Wake-channel integration tests.
//!
//! What is left here is the **byte-level** contract of the kick channel: a kick
//! carries exactly one meaningless byte and never an event body. Everything else
//! this file used to cover drove `mailbox wait` — the blocking waiter command,
//! removed along with the ADR-0006 re-arm loop. The surviving wake path (the
//! detached watcher blocking on the same FIFO and bumping a sentinel) is covered
//! end-to-end in `filechanged_wake.rs`.

use std::fs::File;
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use mailbox::bus::SessionId;
use mailbox::wake::{KickOutcome, WAKE_BYTE, Waiter, Waker};
use tempfile::TempDir;

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

/// Byte-level payload-free: a kick writes EXACTLY `[WAKE_BYTE]` and nothing else.
#[test]
fn kick_writes_exactly_one_wake_byte() {
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
