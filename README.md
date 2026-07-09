# Agent Mailbox

Agent mailbox is a system designed to allow agents to be woken up via asynchronous actions. It integrates directly with Claude code, which is the most popular harness in use here, and allows you to have a number of background tasks trigger Claude to react to them. Examples of things that might be interesting to trigger would be:

- changes in pull request state
- the deployment of your changes
- monitoring of those changes in production

## Docs

See [docs/00-index.md](docs/00-index.md) for the full list.

- [Wake and re-arm](docs/01-wake-and-rearm.md)
- [Tech stack](docs/02-tech-stack.md)
