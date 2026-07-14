//! Bakes a git-aware version string into the binary so an installed `mailbox`
//! is traceable to the commit it was built from (`mailbox --version`). The plain
//! crate version does not move between builds, so it cannot tell a fresh binary
//! from a stale one — the commit hash and dirty flag can.
//!
//! Freshness: Cargo caches build-script output, so we must tell it when to re-run
//! or `--version` goes stale. We watch our own `src`/`build.rs` (so an edit
//! refreshes the `-dirty` flag) AND the git ref/HEAD/index files (so a commit,
//! checkout, or stage refreshes the hash) — see [`emit_rerun_triggers`]. Every
//! `git` call degrades gracefully: a build with no git or outside a repo (a
//! release tarball) still produces a valid version string.

use std::process::Command;

fn main() {
    emit_rerun_triggers();

    let pkg = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".into());

    let long = match git_short_hash() {
        Some(hash) => {
            let dirty = if git_is_dirty() { "-dirty" } else { "" };
            match git_commit_date() {
                Some(date) => format!("{pkg} (git {hash}{dirty}, {date})"),
                None => format!("{pkg} (git {hash}{dirty})"),
            }
        }
        // Not a git checkout (e.g. built from a packaged source tree).
        None => format!("{pkg} (git unknown)"),
    };

    println!("cargo:rustc-env=MAILBOX_LONG_VERSION={long}");
}

/// Tell Cargo when to re-run this script. Without these, a bare `git commit`
/// (which changes no crate source) would leave `--version` reporting the parent
/// commit and a stale `-dirty`. We watch:
/// - `src` + `build.rs`, so editing the code refreshes the `-dirty` flag; and
/// - the git `HEAD`, the branch ref it points at, and the `index`, so a commit,
///   checkout, or stage refreshes the hash. Missing paths (e.g. a packed ref with
///   no loose file yet) still work: Cargo re-runs when they later appear.
fn emit_rerun_triggers() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");

    let mut git_paths = vec!["HEAD".to_string(), "index".to_string()];
    if let Some(head_ref) = git(&["rev-parse", "--symbolic-full-name", "HEAD"]) {
        git_paths.push(head_ref);
    }
    for spec in &git_paths {
        if let Some(path) = git(&["rev-parse", "--git-path", spec]) {
            // `--git-path` is relative to the cwd (the crate dir); canonicalize so
            // the trigger is unambiguous. If it does not exist yet, emit as-is.
            let emit = std::fs::canonicalize(&path)
                .map(|p| p.display().to_string())
                .unwrap_or(path);
            println!("cargo:rerun-if-changed={emit}");
        }
    }
}

/// The short commit hash, or `None` if git is unavailable or this is not a repo.
fn git_short_hash() -> Option<String> {
    git(&["rev-parse", "--short=12", "HEAD"])
}

/// Whether the working tree has uncommitted changes. A failed probe is treated as
/// clean rather than falsely flagging dirty.
fn git_is_dirty() -> bool {
    git(&["status", "--porcelain"]).is_some_and(|out| !out.is_empty())
}

/// The commit's date (`YYYY-MM-DD`) — the commit's own date, so it needs no
/// build clock and stays reproducible.
fn git_commit_date() -> Option<String> {
    git(&["show", "-s", "--format=%cd", "--date=short", "HEAD"])
}

/// Run a `git` command, returning its trimmed stdout on a clean exit, else `None`.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_string())
}
