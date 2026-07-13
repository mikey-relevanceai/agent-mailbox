//! The one atomic file write, shared by both setup commands.
//!
//! Both `install-hooks` (the user's `settings.json`) and `install-skills` (the
//! user's `SKILL.md`) write into the Claude Code config directory, and both need
//! the same guarantee: **the target file is only ever the complete old content or
//! the complete new content.** A half-written `settings.json` is unparseable and a
//! half-written `SKILL.md` is a silently broken skill that Claude Code still
//! loads, so a crash, a full disk, or a SIGKILL mid-write must not be able to
//! produce one.
//!
//! It lives here (rather than being written twice — once per command) because the
//! two copies had already drifted; one guarantee should have one implementation.
//!
//! The write is: create a temp file **in the target's own directory**, write it,
//! `sync_all` it, then `rename` it over the target.
//!
//! - Same directory, because `rename` is only atomic *within* a filesystem.
//! - [`tempfile::NamedTempFile`] rather than a hand-rolled name, because it opens
//!   with `O_EXCL` and a random suffix (two concurrent installers cannot collide,
//!   even from the same pid — a pid-suffixed name is NOT unique across threads or
//!   across pid namespaces sharing a home) and it **removes the temp on drop, on
//!   every failure path**. A hand-rolled temp leaks litter whenever the write, not
//!   just the rename, is what failed.
//! - `sync_all` **before** the rename, because a rename can otherwise be durable
//!   while the data it points at is not — a power loss then leaves a zero-length
//!   file, exactly the corruption this function exists to prevent.
//!
//! # The target's file mode is preserved
//!
//! A temp file is created 0600, and `rename` carries the temp's mode onto the
//! target — so a naive atomic write silently *narrows* the user's `settings.json`
//! from 0644 to 0600. Narrowing is safe but it is still an unannounced mutation of
//! metadata on a file we do not own, so an existing target's permissions are copied
//! onto the temp before the rename. The one thing we force is owner read/write
//! ([`OWNER_RW`]): a mode-000 file exists precisely so that `install-skills` can
//! *repair* it, and republishing it unreadable would defeat that.
//!
//! # Publishing can be guarded (the lost-update problem)
//!
//! Atomicity stops a *torn* write; it does nothing about a *lost* one. Claude Code
//! rewrites `settings.json` itself (a `/config` model change, an "always allow"
//! permission grant), so a read-modify-write of that file can silently discard a
//! concurrent edit. [`write_atomic_guarded`] runs a caller's check in the last
//! moment before the rename, which is what lets the settings merge compare-and-swap
//! (see `install::merge_hooks_file`) rather than blindly clobber.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tempfile::NamedTempFile;

/// The owner read/write bits every file we publish must keep, whatever mode the
/// target we are replacing had. See the module docs.
const OWNER_RW: u32 = 0o600;

/// Atomically write `contents` to `path`, creating the parent directory if
/// needed. See the module docs for why each step is what it is.
///
/// On any failure the target is left exactly as it was and no temp file survives.
pub fn write_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    write_atomic_guarded(path, contents, || Ok(()))
}

/// [`write_atomic`], with a `guard` run in the last moment before the rename.
///
/// A `guard` that returns `Err` **aborts the publish**: the target is left exactly
/// as it was, no temp survives, and the error is returned as-is (so a caller can
/// signal "the file changed underneath me" with a kind it recognises and retry).
/// This is the only place a caller can wedge a check *after* the new content is
/// durable but *before* it becomes visible — which is what a compare-and-swap on
/// the user's `settings.json` needs.
pub fn write_atomic_guarded(
    path: &Path,
    contents: &[u8],
    guard: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;

    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(contents)?;

    // Inherit the target's mode (a temp is 0600, and the rename would carry that
    // onto a 0644 settings.json). Owner read/write is forced back on, so repairing
    // a mode-000 file does not republish it unreadable. `metadata` follows a
    // symlink, which is right: the caller has already resolved where it is writing.
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode() | OWNER_RW;
        tmp.as_file()
            .set_permissions(std::fs::Permissions::from_mode(mode))?;
    }

    // Flush the DATA to disk before the rename publishes the name.
    tmp.as_file().sync_all()?;

    // The last look before the content becomes visible (see the module docs). On
    // Err the temp is dropped — and deleted — without ever touching the target.
    guard()?;

    // `persist` is the rename. Its error carries the temp file back, which we drop
    // — deleting it — so a failed publish leaves no litter either.
    tmp.persist(path).map_err(|err| err.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    /// Every entry in a directory, for asserting no temp file survives.
    fn entries(dir: &Path) -> Vec<String> {
        let mut found: Vec<String> = std::fs::read_dir(dir)
            .expect("read_dir")
            .map(|e| e.expect("entry").file_name().display().to_string())
            .collect();
        found.sort();
        found
    }

    #[test]
    fn writes_the_file_and_creates_missing_parents() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("a").join("b").join("settings.json");

        write_atomic(&path, b"hello").expect("write");

        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        // Only the target — the temp was renamed, not left beside it.
        assert_eq!(entries(path.parent().unwrap()), vec!["settings.json"]);
    }

    #[test]
    fn overwrites_an_existing_file_leaving_no_temp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"old").unwrap();

        write_atomic(&path, b"new").expect("write");

        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert_eq!(entries(dir.path()), vec!["f"]);
    }

    /// The failure path that a hand-rolled temp gets wrong: the temp is created
    /// and written, and then the *publish* fails (here the target is a directory,
    /// so the rename cannot succeed). The temp must still be gone.
    #[test]
    fn a_failed_publish_leaves_no_temp_behind() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("occupied");
        std::fs::create_dir(&target).expect("a directory where the file should go");

        let err = write_atomic(&target, b"content");

        assert!(err.is_err(), "renaming a file over a directory must fail");
        assert!(target.is_dir(), "the target is left exactly as it was");
        // The directory still holds ONLY the pre-existing target: no orphaned temp.
        assert_eq!(entries(dir.path()), vec!["occupied"]);
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// The user's `settings.json` is 0644; publishing our merge over it must not
    /// silently narrow it to the temp file's 0600.
    #[test]
    fn an_existing_targets_mode_is_preserved() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        write_atomic(&path, b"{\"a\":1}").expect("write");

        assert_eq!(mode_of(&path), 0o644, "the target's mode must survive");
    }

    /// ...but a mode-000 file (the one `install-skills` exists to repair) must come
    /// back readable, or the repair would be invisible to the tool that needs it.
    #[test]
    fn a_repaired_file_regains_owner_read_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("SKILL.md");
        std::fs::write(&path, b"corrupt").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        write_atomic(&path, b"repaired").expect("write");

        assert_eq!(mode_of(&path) & 0o600, 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"repaired");
    }

    #[test]
    fn a_new_file_is_created_private() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fresh");
        write_atomic(&path, b"x").expect("write");
        assert_eq!(mode_of(&path), 0o600);
    }

    /// A guard that fails ABORTS the publish: the target keeps its old content and
    /// no temp survives. This is what makes the settings compare-and-swap safe —
    /// losing the race must never mean clobbering the winner.
    #[test]
    fn a_failing_guard_aborts_the_publish_without_touching_the_target() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, b"old").unwrap();

        let err = write_atomic_guarded(&path, b"new", || {
            Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "changed underneath me",
            ))
        })
        .expect_err("the guard must veto the rename");

        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted, "kind survives");
        assert_eq!(std::fs::read(&path).unwrap(), b"old", "target untouched");
        assert_eq!(entries(dir.path()), vec!["f"], "no temp left behind");
    }

    /// Concurrent writers must not collide on a temp name (the regression guard for
    /// a fixed/pid-suffixed temp: these threads all share one pid). Every writer
    /// publishes complete content, and none leaves litter.
    #[test]
    fn concurrent_writers_converge_with_no_litter() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shared");
        let contents = "x".repeat(64 * 1024); // big enough that a torn write would show

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| write_atomic(&path, contents.as_bytes()).expect("concurrent write"));
            }
        });

        // Whichever writer won, the file is COMPLETE (never a truncated interleave).
        assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
        assert_eq!(entries(dir.path()), vec!["shared"]);
    }
}
