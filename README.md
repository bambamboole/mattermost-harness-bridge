# mattermost-harness-bridge

Mention a bot in Mattermost, and a coding agent (Claude Code or Codex)
runs the job on **your own machine**: your repos, your logins, your
context. With Claude Code, tool calls that need permission become
Allow/Deny buttons in the thread.

```
Mattermost (OAuth app, multiple bots per owner)
      ^ one WS + incoming/outgoing webhooks per bot / REST posts
      v
Broker (Go, public)                     mhb broker
      ^ jobs + approvals down / hello, progress, results up
      v
Harness (Go, one per developer, NAT)    mhb harness run
      v
claude -p … | codex exec … (subprocess per job)
```

Harnesses connect *outbound* to the broker over WebSocket. Nothing ever
connects to a laptop. Only the owner of a harness can trigger it.

One binary, `mhb`, serves both roles: `mhb broker` on the server,
`mhb harness …` on developer machines.

## Layout

| Path | What |
|---|---|
| `cmd/mhb`, `internal/cli` | The `mhb` binary and its cobra command tree |
| `internal/protocol` | Wire protocol shared by both binaries: envelope, message types, ack rules |
| `internal/store` | Broker persistence interface, CAS-style; `sqlite/` is the implementation, `storetest/` the conformance suite |
| `internal/broker/hub` | WebSocket server for harnesses: auth, heartbeat, outbox delivery, connection replacement |
| `internal/broker/core` | Routing, approvals with signed callbacks, pairing, sweeper (queue TTL, lost harnesses, timeouts) |
| `internal/broker/mattermost` | Thin adapter over the official `model.Client4` and its WebSocket client |
| `internal/harness` | Daemon: broker client with reconnect, workspace and agent selection, permission bridge, thread→session map |
| `internal/harness/agent` | The agent interface; `claude/` drives `claude -p --output-format stream-json`, `codex/` drives `codex exec --json` |
| `internal/harness/permission` | `--permission-prompt-tool` bridge: MCP stdio subcommand → Unix socket → daemon → broker |
| `internal/e2e` | Real hub + real harness + fake `claude` over a live WebSocket |

## Protocol in one paragraph

WebSocket on `/harness/v2`, one JSON envelope per frame
(`id`, `type`, `job_id`, `ref`, `ts`, `payload`). Auth is a bearer token on
the upgrade request. The harness opens with `hello` (version, workspaces,
jobs it still runs); the broker answers `welcome` with `resume`/`abort` per
job and then flushes its outbox. `job.dispatch`, `job.cancel`,
`approval.response`, `job.result` and `approval.request` are at-least-once:
kept in an outbox until an `ack` (or nack) with matching `ref` arrives, and
resent after a reconnect. `job.progress` is a fire-and-forget snapshot;
highest `seq` wins. App-level `ping`/`pong` every 15 s; the broker drops a
harness after 45 s of silence, the harness reconnects after 10 s without a
pong. A newer connection for the same harness replaces the old one.

## Mattermost setup

The broker is a confidential OAuth application. A system administrator installs it
once per team; the application registers `/harness` automatically. Each user can
then create multiple real Mattermost bots. Every bot belongs to one local harness;
a harness can serve several bots owned by the same user.

There is no shared listener bot. Each bot has its own access token, event WebSocket,
incoming webhook, and outgoing webhook. The broker records the human owner
separately from Mattermost's bot creator (the administrator who authorized OAuth).
Only that human owner can submit jobs or approve tool calls.

### 1. Register the OAuth application

In Mattermost, enable OAuth applications, custom slash commands, bot account
creation, incoming webhooks, outgoing webhooks, and personal access tokens in the
System Console. Register an OAuth application under **Integrations → OAuth 2.0
Applications** with callback URL:

```text
https://broker.example.com/oauth/callback
```

Use a confidential client and copy its client ID and client secret. The broker
supports one configured Mattermost server and installations in multiple teams.

### 2. Configure the broker

```sh
export MM_URL=https://mm.example.com
export MM_OAUTH_CLIENT_ID=...
export MM_OAUTH_CLIENT_SECRET=...
export MM_BOT_PROVISIONING_TOKEN=...
export PUBLIC_URL=https://broker.example.com
export CALLBACK_SECRET=$(openssl rand -hex 32)
export DB_PATH=/var/lib/broker/broker.db
mhb broker
```

`MM_BOT_PROVISIONING_TOKEN` is a personal access token of a system administrator.
It is used **only to issue access tokens for newly created bots**. Mattermost
explicitly rejects OAuth sessions at that endpoint, including admin OAuth sessions;
OAuth alone cannot complete real-bot provisioning. All other provisioning uses
the administrator's OAuth grant. See the
[Mattermost token handler](https://github.com/mattermost/mattermost/blob/master/server/channels/api4/user.go).

OAuth grants are encrypted in SQLite using a key derived from `CALLBACK_SECRET`.
Keep that secret stable across restarts. The database also contains bot tokens,
webhook credentials, and command tokens, so restrict access to it and its backups.

The broker needs HTTPS reachable by Mattermost and developer machines. Allow these
routes through the reverse proxy:

- `/oauth/start` and `/oauth/callback`: browser installation.
- `/commands/harness`: slash command callbacks.
- `/webhooks/mattermost/<bot-id>`: outgoing webhook callbacks.
- `/callback/approval`: signed tool-approval callbacks.
- `/init` and `/init/<code>`: local harness pairing.
- `/harness/v2`: the local harness WebSocket.

For private broker addresses, add the hostname to Mattermost's **Allowed untrusted
internal connections** setting. Interactive approval callbacks use the signed
button context and verify the clicking user's ID.

Every setting is also a flag; see `mhb broker --help`. The old `MM_BOT_TOKEN`,
`MM_ADMIN_TOKEN`, and `MM_COMMAND_TOKEN` settings have been replaced.

### 3. Install into a team

Open `https://broker.example.com/oauth/start`, enter the team name from its
Mattermost URL (or its team ID), and authorize as a system administrator who is a
member of that team. The app creates `/harness`, stores its verification token,
and refreshes the OAuth grant when needed. Reinstalling reuses its managed command;
an unrelated existing `/harness` command produces a conflict instead of being
replaced. Repeat for additional teams.

## Harness setup

In Mattermost, run `/harness init` for onboarding instructions. On the machine
that should execute jobs:

```sh
mhb harness init --broker https://broker.example.com --bot my-laptop
# Copy the displayed /harness init <code> into a public or private team channel.
# The broker creates @my-laptop and its webhooks, then pairs this machine.
mhb harness run
```

Pairing codes expire after ten minutes. Polling additionally requires a separate
secret held by the local CLI; the code typed in Mattermost cannot retrieve the
harness credentials. A successful result can be fetched once.

Each `mhb harness init` creates a new bot and a new harness identity. `--bot` picks
an unused username; without it the broker generates a name from the owner's
username and a random suffix. `--name` sets the machine name. `--yes` skips prompts;
`--agent`, `--default-workspace`, and repeated `--workspace name=dir` configure it.
Reinitializing an already configured machine replaces that machine's local pairing.

To add another bot to the same running harness, use its ID from `/harness status`:

```text
/harness bot create my-reviewer <harness-id>
/harness join my-reviewer
/harness status
```

Both bots can run on the same process and use its configured workspaces and agents.
They keep independent conversations, including when mentioned in the same thread.
A bot remains bound to its chosen harness even if another of the owner's harnesses
is online. An offline harness queues jobs until the configured queue TTL expires.

### Webhooks and channels

Every bot has a native incoming webhook at `MM_URL/hooks/<incoming-hook-id>` owned
by its Mattermost bot account. Its outgoing webhook targets the broker's bot-specific
endpoint and uses `@<bot-name>` as an exact first-word trigger in public channels
of its installed team. Treat webhook URLs and tokens as credentials.

The bot's WebSocket also receives DMs, private-channel messages, mentions elsewhere
in a message, and replies in existing bot threads. `/harness join <bot-name>` adds
that bot to another channel in its installed team. A bot must be a channel member
for its API access and event stream to work. Native outgoing webhooks alone do not
support DMs or private channels. See
[Mattermost outgoing webhooks](https://developers.mattermost.com/integrate/webhooks/outgoing/).

WebSocket and webhook deliveries are deduplicated by bot ID and original post ID.
The broker fetches webhook-triggered posts with the bot's API credentials and checks
the owner, channel, and team. Jobs, cancellation, and local agent sessions are
scoped to the bot. Replies without a new mention continue existing conversations;
mention a specific bot when multiple bots share a thread and only one should act.

Status, progress, attachments, and thread replies use the bot REST API, allowing
existing posts to be edited. The incoming webhook remains available for external
messages posted under that bot's identity.

### Upgrading from the shared-bot bridge

This is a breaking change. Upgrade the broker and local harness binaries together:
the wire endpoint is now `/harness/v2`, and every dispatch includes its bot ID.
Start with a fresh broker database: `0001_init.sql` contains the complete schema,
and existing databases are not migrated. Complete OAuth installation, remove the
old manually managed `/harness` command if it conflicts, and initialize new bots.
Old thread-only session entries are not reused. The legacy `mhb harness pair`
command is removed.

Config lives in `~/Library/Application Support/mm-harness/config.json`
(`$XDG_CONFIG_HOME/mm-harness` on Linux). Relevant keys:

- `workspaces`: name → absolute directory, picked with `ws:<name>` in the message.
- `default_workspace`: where jobs run that name no workspace and continue no thread; created on start, default `~/.harness`. `mhb harness run --default-workspace <dir>` overrides it.
- `agent`: `claude` (default) or `codex`, for jobs that name none with `agent:<name>`; `--agent` overrides it. Both agents are registered when their binary is found.
- `allowed_tools`: run without asking (default: Read, Grep, Glob, LS, WebSearch, WebFetch); `disallowed_tools`: never run. Both apply to Claude Code.
- `codex_bin`, `codex_sandbox` (`workspace-write` by default, or `read-only`).
- `max_jobs`, `default_max_turns`, `approval_timeout_min`, `claude_bin`, `model`.

### Agents

**Claude Code** runs `claude -p` with the permission bridge: every tool
call outside `allowed_tools` calls the harness's MCP permission tool, the
harness sends `approval.request`, the broker posts Allow/Deny buttons, the
owner clicks, the decision travels back. `--dangerously-skip-permissions`
is never used.

**Codex** runs `codex exec --json` inside Codex's own sandbox
(`codex_sandbox`, `workspace-write` by default: it can change files in the
workspace, nothing else, and has no network). `codex exec` cannot ask a
human, so there are no approval buttons for Codex jobs; the sandbox is the
guard rail. A human-in-the-loop bridge through `codex app-server` is a
possible later step.

A thread sticks to the agent and workspace of its first job; `agent:` or
`ws:` in a later message starts a fresh session there.

## Using it

- `@my-laptop ws:infra bump the mattermost provider` starts a job in workspace `infra`; without `ws:` it runs in the harness's default folder.
- `@my-laptop agent:codex …` picks the agent; without `agent:` the harness's default is used.
- Reply in the same thread to continue: the harness resumes the agent
  session it kept for that thread. `ws:` and `agent:` can be omitted then.
- Mentioning the bot for the first time inside an existing thread hands the
  agent the thread so far (the last 60 posts, without the bot's own), so
  "do what the thread says" works.
- `@my-laptop cancel` in a thread stops the job.
- DM your bot with a task to start a job; use `/harness status` to list bots and machines.

## Releases

Conventional commits on `main` feed [release-please](https://github.com/googleapis/release-please),
which opens a release PR and, on merge, creates the tag and GitHub release.
The tag triggers `release.yml`: GoReleaser attaches `mhb` archives for
linux/darwin × amd64/arm64 plus `checksums.txt`, and the image (entrypoint
`mhb`, default command `broker`) is pushed to
`ghcr.io/bambamboole/mattermost-harness-bridge` as `<version>`,
`<major>.<minor>` and `latest`.

`release-please.yml` needs a `RELEASE_PLEASE_TOKEN` repository secret (a PAT
with `contents` and `pull-requests` write): tags pushed with the default
`GITHUB_TOKEN` do not trigger other workflows.

```sh
docker run --rm -p 8080:8080 -v broker-data:/data \
  -e MM_URL=... -e MM_OAUTH_CLIENT_ID=... -e MM_OAUTH_CLIENT_SECRET=... \
  -e MM_BOT_PROVISIONING_TOKEN=... -e PUBLIC_URL=... -e CALLBACK_SECRET=... \
  ghcr.io/bambamboole/mattermost-harness-bridge:latest
```

## Development

```sh
go test ./... -race      # unit, store conformance, end-to-end
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.0 run ./...
go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean --skip=publish
docker build -t mhb:local .
```

The permission-tool contract was verified against claude 2.1.263: the tool
receives `{"tool_name","input","tool_use_id"}` and returns a JSON string
`{"behavior":"allow","updatedInput":{...}}` or `{"behavior":"deny","message":"..."}`.

## Not done yet

- launchd/systemd unit for the harness and a self-update path.
- Approval posts are only rewritten on click; expired ones keep their buttons (clicking says so).
- Per-channel default workspace on the broker; today it is `ws:` in the message, else the thread's previous workspace, else the only configured one.
- `--include-partial-messages` streaming; progress currently updates per assistant message.

## License

MIT, see [LICENSE](LICENSE).
