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

use std::io::Write;
use std::path::Path;

use tempfile::NamedTempFile;

/// Atomically write `contents` to `path`, creating the parent directory if
/// needed. See the module docs for why each step is what it is.
///
/// On any failure the target is left exactly as it was and no temp file survives.
pub fn write_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;

    let mut tmp = NamedTempFile::new_in(dir)?;
    tmp.write_all(contents)?;
    // Flush the DATA to disk before the rename publishes the name.
    tmp.as_file().sync_all()?;
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
