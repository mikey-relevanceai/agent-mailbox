# Adapters

Reference and experimental adapters live here. They are **separate processes**
that speak the mailbox protocol (see `docs/02-tech-stack.md`). They must not
import bridge internals.

v0: stub publishers that exercise `mailbox publish` are enough. Real pollers
(GitHub, deploys, …) come later.
