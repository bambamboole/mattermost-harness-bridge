# Security

## Reporting a vulnerability

Report privately through GitHub's [security advisory
form](https://github.com/bambamboole/mattermost-harness-bridge/security/advisories/new),
or by email to manuel@christlieb.eu. Please do not open a public issue for
anything exploitable. Expect a first reply within a few days.

## What this project does

The bridge runs a coding agent on a developer's machine in response to a
Mattermost message. That is remote code execution by design, so the
interesting question is not *whether* code runs but *whose* message can make
it run and *where*.

## Trust model

- **The Mattermost server is trusted.** It authenticates users, and the
  broker believes the user id in a `posted` event, a slash command and an
  interactive-button callback. Anyone who can forge Mattermost events can
  start jobs.
- **The broker is trusted by the harnesses.** It holds every harness token
  hash, the bot tokens and, when onboarding is on, an admin token. Treat the
  broker host and its database as production secrets.
- **A harness trusts only its owner.** Every `job.dispatch` carries the
  requester's Mattermost user id; the harness refuses anything that does not
  match `owner_mm_user_id` in its config, independently of what the broker
  claims. Mentioning someone else's bot is rejected by the broker *and* by
  the harness.
- **The agent's output is untrusted.** Result text and progress snapshots go
  straight into Mattermost posts as Markdown.

## Boundaries

- Harnesses connect outbound only. Nothing listens on a developer machine
  except a Unix socket (mode `0600`) for the permission bridge.
- Harness tokens are stored as SHA-256 hashes on the broker; the plaintext
  exists only in the harness config file (mode `0600`).
- Approval buttons carry an HMAC over `approval_id | decision | nonce` with
  `CALLBACK_SECRET`, and the callback additionally checks that the clicking
  user owns the job. The endpoint can therefore stay unauthenticated at the
  proxy.
- Claude Code runs with the permission bridge; `--dangerously-skip-permissions`
  is never passed. Codex runs inside its own sandbox (`workspace-write` by
  default, no network).
- Jobs run in a configured workspace directory. A workspace is an
  allow-list entry, not a jail: the agent can read outside it, and with
  Claude Code it can write outside it if the owner approves the tool call.
- Prompt content coming from chat is marked as data in the system prompt,
  but that is guidance, not a boundary. Do not treat a workspace as safe
  against a hostile message in a channel your bot can read.

## Secrets in the broker database

The SQLite database holds bot access tokens in plaintext (they are needed to
post as each user's bot) and, briefly, unclaimed harness tokens for pending
onboarding codes. Everything else is hashed. Keep `DB_PATH` on a volume only
the broker can read, and back it up accordingly.

## Supported versions

Only the latest release gets fixes. Releases are cut from `main`; see
[CHANGELOG.md](CHANGELOG.md).
