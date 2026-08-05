# ADR-0002: MVP crate stack

- Status: Accepted
- Date: 2026-07-09

## Context

We need a boring, current Rust stack for the bridge before implementing the
GitHub-watch MVP. The choices should favour a local CLI/daemon, not a cloud
service.

## Decision

| Concern | Crate | Notes |
|---|---|---|
| Serde | `serde` + `serde_json` | Wire/protocol and NDJSON-friendly CLI I/O |
| Async runtime | `tokio` | Waiters, process supervision, later local socket/HTTP |
| Logging | `tracing` + `tracing-subscriber` | `EnvFilter`; structured later if needed |
| CLI | `clap` (features `derive`, `env`) | `mailbox` binary. (The `env` feature was taken for `--session`'s `MAILBOX_SESSION_ID` fallback; both are since deleted — a session's identity is `$CLAUDE_CODE_SESSION_ID`, read directly.) |
| Library errors | `thiserror` | Typed errors in crates |
| Binary edge errors | `anyhow` | `main` / CLI only |
| SQLite | `rusqlite` with `features = ["bundled"]` | See [ADR-0003](0003-single-writer-sqlite.md) |
| Unix primitives | `nix` with `features = ["fs", "poll", "signal"]` | Safe `mkfifo`/`flock`/`poll`/`O_NONBLOCK` wrappers for the wake FIFO, plus `kill` (`signal`) for the harness to reap a session's waiter on `SessionEnd` (card 11); the workspace `unsafe_code = "deny"` lint forbids the equivalent raw `libc` calls |
| HTTP (later) | `axum` + `tower-http` | Local observability UI only; **not** in MVP |

`bundled` compiles SQLite into the binary so builds and installs do not need a
system `libsqlite3`.

## Consequences

- Dependencies match the 2026 default Tokio-centric stack without pulling a web
  server into the critical path yet.
- SQLite access is sync; async code talks to it through a single writer (ADR-0003).
- Axum stays deferred until we have something worth observing.

## Alternatives considered

- **sqlx** — better if everything is async-first; heavier for a local bus. Rejected for MVP in favour of rusqlite + single writer.
- **Diesel / SeaORM** — more abstraction than we need for append + cursors.
- **Actix / Rocket** — not the default for new local HTTP; Axum when we need it.
- **`log` alone** — insufficient once we have async waiters; use `tracing`.
