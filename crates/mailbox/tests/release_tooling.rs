//! The release path is shell, not Rust: `scripts/render-formula.sh` turns a
//! version and a `SHA256SUMS` into the Homebrew formula, and its own suite lives
//! beside it in bash. This runs that suite from `cargo test` so the one command a
//! contributor already runs still covers everything — the same reason the CLI and
//! e2e tests here drive a real `mailbox` binary rather than asserting on internals.

use std::path::PathBuf;
use std::process::Command;

/// The workspace root, two levels up from this crate's manifest.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("the workspace root is two levels above crates/mailbox")
}

#[test]
fn the_formula_renderer_suite_passes() {
    let root = repo_root();
    let script = root.join("scripts").join("test-render-formula.sh");

    let out = Command::new(&script)
        .current_dir(&root)
        .output()
        .unwrap_or_else(|err| panic!("could not run {}: {err}", script.display()));

    // Forward the suite's own output on failure. It names the failing case, which
    // is the whole reason to shell out rather than re-encode the cases in Rust.
    assert!(
        out.status.success(),
        "scripts/test-render-formula.sh failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
