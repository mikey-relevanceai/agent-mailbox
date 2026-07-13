//! Installing the agent-mailbox Claude Code skill(s) into a skills directory.
//!
//! `install-skills` is the sibling of [`install`](crate::install) (hooks): the
//! hooks make wake *infrastructure*, and the skill teaches the agent the loop it
//! wakes into. Together they are the whole setup.
//!
//! # Why the skill content is EMBEDDED
//!
//! The skill body is baked into the binary with `include_str!` rather than copied
//! from the repo at runtime. A user installs `mailbox` to `~/.local/bin` and then
//! has no checkout to `cp` from — an install command that needed the repo present
//! would only work for people who already had the thing they were installing. The
//! embed also makes the shipped content *versioned with the binary*: a given
//! `mailbox` always installs the skill it was built with, so the two can never
//! drift.
//!
//! # Why this command MUST always be able to run
//!
//! Its entire purpose is to (re)install a known-good skill, so it has to work
//! *especially* when the installed one is broken. It therefore compares **bytes**,
//! never a `String` — a corrupt or non-UTF-8 `SKILL.md` must not be a fatal read
//! error — and treats *any* existing file it cannot read as "differs, replace it".
//! Clobbering is safe because the write is atomic ([`crate::atomic`]). That makes
//! the command self-healing, which is why it needs no `--force`.
//!
//! # Why the file I/O lives here and not in `cli.rs`
//!
//! Its sibling `install-hooks` keeps its file write at the CLI edge, so this reads
//! as asymmetric. The reason is testability: installing a skill is a real
//! compare-then-maybe-write decision with several failure modes (corrupt file,
//! symlink, occupied path, unwritable dir), and keeping it in the library lets the
//! unit tests drive every one of them against a tempdir. `cli.rs` stays a thin
//! dispatcher.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::atomic::write_atomic;
use crate::home::{CLAUDE_DIR, ENV_HOME, harness_home};

/// A skill shipped inside the binary: its directory name and its `SKILL.md` body.
///
/// `name` is a hardcoded constant, never user-supplied — it becomes a path
/// segment, and keeping it a constant is what makes traversal unrepresentable
/// rather than merely unlikely.
#[derive(Debug, Clone, Copy)]
pub struct EmbeddedSkill {
    /// Directory name under the skills dir (`<skills-dir>/<name>/SKILL.md`).
    pub name: &'static str,
    /// The verbatim `SKILL.md` body.
    pub contents: &'static str,
}

/// Every skill this binary ships. A list (not a single constant) so adding a
/// second skill later is one entry here and nothing else — which is why the
/// command is `install-skills`, plural.
pub const SKILLS: &[EmbeddedSkill] = &[EmbeddedSkill {
    name: "agent-mailbox",
    contents: include_str!("../../../skills/agent-mailbox/SKILL.md"),
}];

/// The file each skill installs to, inside its own directory.
const SKILL_FILE: &str = "SKILL.md";

/// The skills subdirectory under Claude Code's config dir: `~/.claude/skills`.
/// (The home itself, and `.claude`, come from [`crate::home`] — the one resolution
/// `install-hooks` shares.)
const SKILLS_SUBDIR: &str = "skills";

/// What installing one skill did to its `SKILL.md`.
///
/// Distinguished (rather than a bare "ok") so a re-run is *visibly* a no-op: the
/// user can tell "already current" from "I just overwrote your edits".
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillOutcome {
    /// Nothing was there; we wrote it.
    Created,
    /// A regular file was there with different (or unreadable / corrupt) content;
    /// we refreshed it to the shipped content.
    Updated,
    /// The `SKILL.md` was a **symlink** whose target did not match the shipped
    /// content, so installing replaced the link with a regular file. Reported
    /// separately from `Updated` because a symlink here is usually deliberate — a
    /// live-edit link into a checkout — and silently swapping it for a copy would
    /// break that setup with no warning. (A symlink already pointing at matching
    /// content is `Unchanged`, so an up-to-date live-edit link survives.)
    ReplacedSymlink,
    /// The file already matched the shipped content byte for byte; we left it
    /// alone — no write syscall at all, so even its mtime is untouched.
    Unchanged,
}

impl SkillOutcome {
    /// A short label for logs and human output.
    pub fn as_str(self) -> &'static str {
        match self {
            SkillOutcome::Created => "created",
            SkillOutcome::Updated => "updated",
            SkillOutcome::ReplacedSymlink => "replaced-symlink",
            SkillOutcome::Unchanged => "unchanged",
        }
    }
}

/// What happened to one skill.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkillInstall {
    /// The skill's name (its directory under the skills dir).
    pub name: String,
    /// The `SKILL.md` path written (or found already current).
    pub path: PathBuf,
    pub outcome: SkillOutcome,
}

/// The result of one `install-skills` run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InstallReport {
    /// The skills directory everything was installed under.
    pub skills_dir: PathBuf,
    /// One entry per skill that installed, in [`SKILLS`] order.
    pub skills: Vec<SkillInstall>,
}

/// Installing ONE skill failed. Every variant names the path it failed on, because
/// "permission denied" with no path is useless when the whole command is about
/// writing to a specific place.
///
/// Note what is NOT here: a corrupt, non-UTF-8, or unreadable existing `SKILL.md`
/// is not an error at all — it is replaced (see the module docs).
#[derive(Debug, thiserror::Error)]
pub enum SkillInstallError {
    /// Neither `AGENT_MAILBOX_HOME` nor `HOME` is set, so there is no default
    /// skills dir to resolve. We error rather than guess a path inside the user's
    /// config space.
    #[error(
        "cannot resolve the default skills directory: neither {ENV_HOME} nor HOME is set (pass --skills-dir)"
    )]
    NoHome,

    /// The skill's own directory could not be made — e.g. `<skills-dir>/<name>`
    /// already exists as a regular file, or the skills dir is not writable.
    #[error("creating skill directory {path}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A non-file (a directory) occupied the `SKILL.md` path and could not be
    /// cleared out of the way — a rename cannot land on a directory.
    #[error("clearing the directory at {path} that occupies the skill's place")]
    ClearTarget {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The atomic write itself failed; the target is untouched.
    #[error("writing the skill to {path}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// One skill's failure, carried alongside the skills that DID install.
#[derive(Debug)]
pub struct SkillFailure {
    pub name: String,
    pub error: SkillInstallError,
}

/// One or more skills failed to install.
///
/// It carries the [`InstallReport`] of what **did** land. A bare error would leave
/// the user unable to tell whether anything was installed — with a single skill
/// that is merely unhelpful, but the moment a second skill exists, a failure on
/// one while the other is durably on disk MUST still report the one that landed.
#[derive(Debug)]
pub struct InstallSkillsError {
    /// What successfully installed, despite the failures.
    pub installed: InstallReport,
    /// Every skill that failed, in [`SKILLS`] order.
    pub failures: Vec<SkillFailure>,
}

impl std::fmt::Display for InstallSkillsError {
    /// Names the count and the skills that failed, but NOT each failure's detail:
    /// the first is carried by [`source`](std::error::Error::source) (so the error
    /// chain prints it once, not twice), and the CLI logs every failure with its
    /// own cause.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total = self.installed.skills.len() + self.failures.len();
        write!(
            f,
            "installed {} of {} skill(s) into {}; failed: ",
            self.installed.skills.len(),
            total,
            self.installed.skills_dir.display()
        )?;
        for (i, failure) in self.failures.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(&failure.name)?;
        }
        Ok(())
    }
}

impl std::error::Error for InstallSkillsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failures.first().map(|failure| &failure.error as _)
    }
}

/// The default skills directory (`<home>/.claude/skills`), resolving home from
/// `AGENT_MAILBOX_HOME` then `HOME`.
pub fn default_skills_dir() -> Result<PathBuf, SkillInstallError> {
    resolve_skills_dir(harness_home())
}

/// The skills dir for an already-resolved home. Pure, so both the layout and the
/// no-home error are testable without mutating the process environment (which is
/// global, and shared with every other test running in parallel).
fn resolve_skills_dir(home: Option<PathBuf>) -> Result<PathBuf, SkillInstallError> {
    let home = home.ok_or(SkillInstallError::NoHome)?;
    Ok(home.join(CLAUDE_DIR).join(SKILLS_SUBDIR))
}

/// Install every [`SKILLS`] entry under `skills_dir`, one directory each.
///
/// Idempotent: a skill whose `SKILL.md` already matches the shipped content is
/// [`SkillOutcome::Unchanged`] and is not rewritten — not even with identical
/// bytes; no write syscall is made at all.
///
/// The only paths this writes are `<skills_dir>/<name>/SKILL.md` and the temp file
/// beside it. (If the user has themselves made `<skills_dir>/<name>` a symlink to
/// somewhere else, the write follows it — honouring their own symlink is the point
/// of having one. Nothing else under their config dir is read or written.)
pub fn install_skills(skills_dir: &Path) -> Result<InstallReport, InstallSkillsError> {
    install_list(skills_dir, SKILLS)
}

/// The engine behind [`install_skills`], generic over the skill list so the
/// multi-skill behaviour — notably that a partial failure still reports what
/// landed — is pinned by tests today, rather than discovered by whoever adds the
/// second skill.
fn install_list(
    skills_dir: &Path,
    skills: &[EmbeddedSkill],
) -> Result<InstallReport, InstallSkillsError> {
    let mut installed = Vec::with_capacity(skills.len());
    let mut failures = Vec::new();

    // Every skill is attempted even after one fails: they are independent, and a
    // user with one broken skill should still get the rest.
    for skill in skills {
        match install_one(skills_dir, skill) {
            Ok(install) => installed.push(install),
            Err(error) => failures.push(SkillFailure {
                name: skill.name.to_string(),
                error,
            }),
        }
    }

    let report = InstallReport {
        skills_dir: skills_dir.to_path_buf(),
        skills: installed,
    };
    if failures.is_empty() {
        Ok(report)
    } else {
        Err(InstallSkillsError {
            installed: report,
            failures,
        })
    }
}

/// What is currently sitting at a skill's `SKILL.md` path.
enum Current {
    /// Nothing is there.
    Absent,
    /// Content that is already exactly the shipped bytes.
    UpToDate,
    /// Anything else — different bytes, corrupt, unreadable, or a directory. All
    /// of it gets replaced; see the module docs on why unreadable must not be
    /// fatal.
    Stale { symlink: bool, directory: bool },
}

/// Inspect the target: its *identity* without following symlinks (so we can tell a
/// link from a file), but its *content* through them (a link pointing at
/// up-to-date content is up to date).
fn inspect(path: &Path, contents: &str) -> Current {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == ErrorKind::NotFound => return Current::Absent,
        // Some other stat failure (e.g. a parent component is not a directory). Do
        // not treat it as absent; fall through to a replace attempt, whose failure
        // is typed and reported rather than silently swallowed.
        Err(_) => {
            return Current::Stale {
                symlink: false,
                directory: false,
            };
        }
    };

    // Bytes, never a String: a non-UTF-8 SKILL.md is CORRUPT, not fatal, and a read
    // error (permissions, a directory) means "cannot be current", not "give up".
    if std::fs::read(path).is_ok_and(|existing| existing == contents.as_bytes()) {
        return Current::UpToDate;
    }

    Current::Stale {
        symlink: meta.file_type().is_symlink(),
        directory: meta.is_dir(),
    }
}

/// Install a single skill: inspect, and write atomically only if it is not already
/// current.
fn install_one(
    skills_dir: &Path,
    skill: &EmbeddedSkill,
) -> Result<SkillInstall, SkillInstallError> {
    let dir = skills_dir.join(skill.name);
    let path = dir.join(SKILL_FILE);

    let (outcome, directory) = match inspect(&path, skill.contents) {
        Current::UpToDate => {
            return Ok(SkillInstall {
                name: skill.name.to_string(),
                path,
                outcome: SkillOutcome::Unchanged,
            });
        }
        Current::Absent => (SkillOutcome::Created, false),
        Current::Stale { symlink, directory } => {
            let outcome = if symlink {
                SkillOutcome::ReplacedSymlink
            } else {
                SkillOutcome::Updated
            };
            (outcome, directory)
        }
    };

    std::fs::create_dir_all(&dir).map_err(|source| SkillInstallError::CreateDir {
        path: dir.clone(),
        source,
    })?;

    // A DIRECTORY where SKILL.md belongs cannot be renamed over, so clear it. This
    // is still only ever the skill's own `SKILL.md` path — nothing else.
    if directory {
        std::fs::remove_dir_all(&path).map_err(|source| SkillInstallError::ClearTarget {
            path: path.clone(),
            source,
        })?;
    }

    write_atomic(&path, skill.contents.as_bytes()).map_err(|source| SkillInstallError::Write {
        path: path.clone(),
        source,
    })?;

    Ok(SkillInstall {
        name: skill.name.to_string(),
        path,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    /// The one shipped skill, for assertions about content.
    fn agent_mailbox() -> &'static EmbeddedSkill {
        SKILLS
            .iter()
            .find(|skill| skill.name == "agent-mailbox")
            .expect("the agent-mailbox skill is shipped")
    }

    fn installed_path(dir: &TempDir) -> PathBuf {
        dir.path().join("agent-mailbox").join("SKILL.md")
    }

    fn outcome(report: &InstallReport) -> SkillOutcome {
        report.skills[0].outcome
    }

    fn shipped() -> &'static [u8] {
        agent_mailbox().contents.as_bytes()
    }

    /// Every entry in a directory tree, relative to its root — used to prove we
    /// wrote nothing outside `<skills-dir>/<name>/SKILL.md` and left no temp file.
    fn tree(root: &Path) -> Vec<String> {
        let mut found = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read_dir") {
                let path = entry.expect("dir entry").path();
                found.push(
                    path.strip_prefix(root)
                        .expect("under root")
                        .display()
                        .to_string(),
                );
                // symlink_metadata, so a symlinked dir is not descended into.
                if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.is_dir()) {
                    stack.push(path);
                }
            }
        }
        found.sort();
        found
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    /// Whether the filesystem actually enforces a read-only directory for us. It
    /// does not when the tests run as root (root ignores the permission bits), so
    /// the permission-failure assertions would be vacuous — skip them rather than
    /// assert something untrue.
    fn permissions_are_enforced(dir: &Path) -> bool {
        std::fs::write(dir.join(".probe"), b"x").is_err()
    }

    #[test]
    fn the_embedded_skill_is_the_repo_skill() {
        // The embed is the whole point: a `mailbox` with no repo present must still
        // ship real skill content (with the frontmatter Claude Code needs).
        let skill = agent_mailbox();
        assert!(
            skill.contents.starts_with("---"),
            "the embedded SKILL.md must carry its YAML frontmatter"
        );
        assert!(skill.contents.contains("name: agent-mailbox"));
        assert!(
            skill.contents.len() > 500,
            "the embed should be the real skill body, not a stub"
        );
    }

    #[test]
    fn install_into_an_empty_dir_creates_the_skill() {
        let dir = TempDir::new().unwrap();
        let report = install_skills(dir.path()).expect("install");

        assert_eq!(report.skills_dir, dir.path());
        assert_eq!(report.skills.len(), SKILLS.len());
        let entry = &report.skills[0];
        assert_eq!(entry.name, "agent-mailbox");
        assert_eq!(entry.outcome, SkillOutcome::Created);
        assert_eq!(entry.path, installed_path(&dir));
        assert_eq!(std::fs::read(installed_path(&dir)).unwrap(), shipped());
    }

    #[test]
    fn reinstalling_the_same_content_is_unchanged() {
        let dir = TempDir::new().unwrap();
        install_skills(dir.path()).expect("first install");

        let report = install_skills(dir.path()).expect("second install");

        assert_eq!(
            outcome(&report),
            SkillOutcome::Unchanged,
            "a re-run must be a visible no-op, not a silent rewrite"
        );
    }

    /// `Unchanged` must mean "no write happened at all", not "rewrote identical
    /// bytes". Proved by making the skill AND its directory read-only: any write
    /// (or temp-file create) would fail hard, so a clean `Unchanged` is evidence
    /// that nothing was written.
    #[test]
    fn unchanged_performs_no_write_at_all() {
        let dir = TempDir::new().unwrap();
        install_skills(dir.path()).expect("install");

        let skill_dir = dir.path().join("agent-mailbox");
        set_mode(&installed_path(&dir), 0o444);
        set_mode(&skill_dir, 0o555);

        if permissions_are_enforced(&skill_dir) {
            let report = install_skills(dir.path()).expect("a no-op must not need write access");
            assert_eq!(outcome(&report), SkillOutcome::Unchanged);
        }

        set_mode(&skill_dir, 0o755); // so the TempDir can clean up
    }

    #[test]
    fn a_changed_file_on_disk_is_refreshed_to_the_shipped_content() {
        let dir = TempDir::new().unwrap();
        install_skills(dir.path()).expect("first install");
        std::fs::write(installed_path(&dir), "stale local edit\n").unwrap();

        let report = install_skills(dir.path()).expect("reinstall");

        assert_eq!(outcome(&report), SkillOutcome::Updated);
        assert_eq!(
            std::fs::read(installed_path(&dir)).unwrap(),
            shipped(),
            "an out-of-date skill is refreshed to the shipped content"
        );
    }

    // ==== self-healing: it must work WHEN the installed skill is broken ===========

    /// A non-UTF-8 `SKILL.md` (corrupt, latin-1, a half-restored backup) must be
    /// REPAIRED, not be a fatal read error. Reading it as a `String` used to make
    /// `install-skills` permanently un-runnable exactly when it was most needed.
    #[test]
    fn a_non_utf8_skill_is_repaired_not_fatal() {
        let dir = TempDir::new().unwrap();
        let path = installed_path(&dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, [0xff, 0xfe, 0x00, 0x9f]).expect("write invalid UTF-8");

        let report = install_skills(dir.path()).expect("a corrupt skill must be repairable");

        assert_eq!(outcome(&report), SkillOutcome::Updated);
        assert_eq!(std::fs::read(&path).unwrap(), shipped());
    }

    /// An UNREADABLE `SKILL.md` (mode 000) is likewise replaced: the atomic rename
    /// needs write access to the *directory*, not to the file it replaces.
    #[test]
    fn an_unreadable_skill_is_repaired_not_fatal() {
        let dir = TempDir::new().unwrap();
        install_skills(dir.path()).expect("install");
        let path = installed_path(&dir);
        set_mode(&path, 0o000);

        let report = install_skills(dir.path()).expect("an unreadable skill must be repairable");

        // As root the read is permitted (→ Unchanged); otherwise it is denied (→
        // Updated). Either way the file ends up correct, which is what matters.
        assert!(matches!(
            outcome(&report),
            SkillOutcome::Updated | SkillOutcome::Unchanged
        ));
        assert_eq!(std::fs::read(&path).unwrap(), shipped());
    }

    /// A DIRECTORY sitting where `SKILL.md` belongs is cleared and replaced (a
    /// rename cannot land on a directory), rather than erroring forever.
    #[test]
    fn a_directory_where_the_skill_belongs_is_replaced() {
        let dir = TempDir::new().unwrap();
        let path = installed_path(&dir);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("junk"), b"x").unwrap();

        let report = install_skills(dir.path()).expect("a directory in the way must be cleared");

        assert_eq!(outcome(&report), SkillOutcome::Updated);
        assert!(path.is_file(), "SKILL.md is now a regular file");
        assert_eq!(std::fs::read(&path).unwrap(), shipped());
    }

    // ==== symlinks: honour a live-edit link, and report replacing one =============

    /// A STALE symlink (a live-edit link whose content has drifted) is replaced by a
    /// regular file — and reported as `replaced-symlink`, not a plain `updated`, so
    /// the user learns their link is gone. The link's TARGET is never written
    /// through: the rename swaps the link itself.
    #[test]
    fn a_stale_symlinked_skill_is_replaced_and_reported() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("checkout-SKILL.md");
        std::fs::write(&source, "live-edit content\n").unwrap();

        let path = installed_path(&dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&source, &path).unwrap();

        let report = install_skills(dir.path()).expect("install over a symlink");

        assert_eq!(
            outcome(&report),
            SkillOutcome::ReplacedSymlink,
            "silently swapping a deliberate symlink for a copy must be reported"
        );
        assert!(
            !std::fs::symlink_metadata(&path).unwrap().is_symlink(),
            "the symlink is replaced by a regular file"
        );
        assert_eq!(std::fs::read(&path).unwrap(), shipped());
        assert_eq!(
            std::fs::read_to_string(&source).unwrap(),
            "live-edit content\n",
            "the symlink's TARGET must never be written through"
        );
    }

    /// A symlink already pointing at the shipped content is `Unchanged` — an
    /// up-to-date live-edit link survives untouched.
    #[test]
    fn an_up_to_date_symlinked_skill_is_left_alone() {
        let dir = TempDir::new().unwrap();
        let source = dir.path().join("checkout-SKILL.md");
        std::fs::write(&source, agent_mailbox().contents).unwrap();

        let path = installed_path(&dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&source, &path).unwrap();

        let report = install_skills(dir.path()).expect("install");

        assert_eq!(outcome(&report), SkillOutcome::Unchanged);
        assert!(
            std::fs::symlink_metadata(&path).unwrap().is_symlink(),
            "an up-to-date live-edit symlink must be preserved"
        );
    }

    // ==== typed errors, no panics ================================================

    #[test]
    fn no_home_is_a_typed_error_not_a_guess() {
        // With no home at all we must NOT invent a path inside someone's config.
        assert!(matches!(
            resolve_skills_dir(None),
            Err(SkillInstallError::NoHome)
        ));
    }

    #[test]
    fn the_default_skills_dir_is_claude_skills_under_home() {
        let dir = TempDir::new().unwrap();
        assert_eq!(
            resolve_skills_dir(Some(dir.path().to_path_buf())).unwrap(),
            dir.path().join(".claude").join("skills"),
        );
    }

    /// `<skills-dir>/<name>` already existing as a REGULAR FILE is a typed
    /// `CreateDir` error — not a panic, and not a silent success.
    #[test]
    fn a_file_where_the_skill_dir_belongs_is_a_typed_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("agent-mailbox"), b"not a dir").unwrap();

        let err = install_skills(dir.path()).expect_err("cannot make a dir over a file");

        assert!(err.installed.skills.is_empty(), "nothing landed");
        assert!(matches!(
            err.failures[0].error,
            SkillInstallError::CreateDir { .. }
        ));
    }

    /// An unwritable skills dir is a typed `CreateDir` error; an unwritable skill
    /// dir is a typed `Write` error. Both must also leave no temp litter.
    #[test]
    fn an_unwritable_dir_is_a_typed_error_with_no_litter() {
        let dir = TempDir::new().unwrap();
        let skills_dir = dir.path().join("skills");
        std::fs::create_dir(&skills_dir).unwrap();
        set_mode(&skills_dir, 0o555);

        if permissions_are_enforced(&skills_dir) {
            let err = install_skills(&skills_dir).expect_err("read-only skills dir");
            assert!(matches!(
                err.failures[0].error,
                SkillInstallError::CreateDir { .. }
            ));
        }
        set_mode(&skills_dir, 0o755);

        // Now the skill's dir exists but is read-only, so the ATOMIC WRITE fails.
        install_skills(&skills_dir).expect("install");
        let skill_dir = skills_dir.join("agent-mailbox");
        std::fs::write(skill_dir.join(SKILL_FILE), b"stale").unwrap();
        set_mode(&skill_dir, 0o555);

        if permissions_are_enforced(&skill_dir) {
            let err = install_skills(&skills_dir).expect_err("read-only skill dir");
            assert!(matches!(
                err.failures[0].error,
                SkillInstallError::Write { .. }
            ));
            set_mode(&skill_dir, 0o755);
            assert_eq!(
                tree(&skill_dir),
                vec![SKILL_FILE],
                "a failed WRITE must leave no temp file behind"
            );
        }
        set_mode(&skill_dir, 0o755);
    }

    // ==== multi-skill: a partial failure still reports what landed ================

    /// The trap the plural name invites: with a second skill, a failure on one must
    /// STILL report the one that is durably on disk. Uses an injected two-skill list
    /// so this is pinned today, not discovered by whoever adds skill #2.
    #[test]
    fn a_partial_failure_still_reports_the_skills_that_landed() {
        let dir = TempDir::new().unwrap();
        // `doomed`'s directory is blocked by a regular file of the same name.
        std::fs::write(dir.path().join("doomed"), b"in the way").unwrap();

        let skills = &[
            EmbeddedSkill {
                name: "good",
                contents: "# good skill\n",
            },
            EmbeddedSkill {
                name: "doomed",
                contents: "# doomed skill\n",
            },
        ];

        let err = install_list(dir.path(), skills).expect_err("the second skill cannot install");

        // The first skill really is on disk, AND the error reports it.
        assert_eq!(err.installed.skills.len(), 1);
        assert_eq!(err.installed.skills[0].name, "good");
        assert_eq!(err.installed.skills[0].outcome, SkillOutcome::Created);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("good").join(SKILL_FILE)).unwrap(),
            "# good skill\n",
        );

        assert_eq!(err.failures.len(), 1);
        assert_eq!(err.failures[0].name, "doomed");

        let message = err.to_string();
        assert!(message.contains("installed 1 of 2 skill(s)"), "{message}");
        assert!(message.contains("doomed"), "{message}");
    }

    // ==== the write touches nothing else =========================================

    #[test]
    fn install_writes_only_the_skill_file_and_leaves_no_temp_behind() {
        let dir = TempDir::new().unwrap();
        install_skills(dir.path()).expect("install");

        assert_eq!(
            tree(dir.path()),
            vec!["agent-mailbox", "agent-mailbox/SKILL.md"],
        );

        // The update path likewise leaves no litter.
        std::fs::write(installed_path(&dir), "changed\n").unwrap();
        install_skills(dir.path()).expect("reinstall");
        assert_eq!(
            tree(dir.path()),
            vec!["agent-mailbox", "agent-mailbox/SKILL.md"],
            "the atomic write must not leave its temp file behind"
        );
    }

    #[test]
    fn a_missing_skills_dir_is_created() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("fresh").join("skills");

        let report = install_skills(&nested).expect("install into a missing dir");

        assert_eq!(outcome(&report), SkillOutcome::Created);
        assert!(nested.join("agent-mailbox").join(SKILL_FILE).is_file());
    }

    #[test]
    fn install_never_touches_unrelated_files_in_the_skills_dir() {
        // The skills dir lives inside the user's ~/.claude: an unrelated skill (or
        // any other file) sitting beside ours must survive untouched.
        let dir = TempDir::new().unwrap();
        let foreign = dir.path().join("someone-elses-skill");
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(foreign.join(SKILL_FILE), "not ours\n").unwrap();

        install_skills(dir.path()).expect("install");

        assert_eq!(
            std::fs::read_to_string(foreign.join(SKILL_FILE)).unwrap(),
            "not ours\n",
            "an unrelated skill must be left exactly as it was"
        );
    }

    /// Concurrent installs into one skills dir converge on the complete content and
    /// leave no litter. These threads share one pid, so this is the regression guard
    /// against a temp name that is only unique per-process.
    #[test]
    fn concurrent_installs_converge_with_no_litter() {
        let dir = TempDir::new().unwrap();

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    install_skills(dir.path()).expect("concurrent install");
                });
            }
        });

        assert_eq!(std::fs::read(installed_path(&dir)).unwrap(), shipped());
        assert_eq!(
            tree(dir.path()),
            vec!["agent-mailbox", "agent-mailbox/SKILL.md"],
        );
    }
}
