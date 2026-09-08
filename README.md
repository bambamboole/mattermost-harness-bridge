# mattermost-harness-bridge

Mention a bot in Mattermost, and a coding agent (Claude Code or Codex)
runs the job on **your own machine**: your repos, your logins, your
context. With Claude Code, tool calls that need permission become
Allow/Deny buttons in the thread.

```
Mattermost (cloud, bot @harness)
      ^ WS events down / REST posts up
      v
Broker (Go, public)                     mhb broker
      ^ jobs + approvals down / hello, progress, results up
      v
Harness (Go, one per developer, NAT)    mhb harness run
      v
claude -p … | codex exec … (subprocess per job)
```

Harnesses connect *outbound* to the broker over WebSocket. Nothing ever
connects to a laptop. Only the owner of a harness can trigger it —
[SECURITY.md](SECURITY.md) spells out what that does and does not protect.

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

WebSocket on `/harness/v1`, one JSON envelope per frame
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

The broker needs one bot account, membership in the teams and channels
where people mention it, and a way for the Mattermost server to reach the
approval callback. Nothing else on the server changes.

The bridge uses two kinds of bots: one **listener bot** (`harness`) whose
token the broker uses for the event WebSocket, and **one bot per user**
(`harness-<username>`) that `/harness init` creates and that posts on the
owner's behalf. Mentions of a user bot are only visible to the broker in
channels the listener bot is in, which is why `/harness init` and
`/harness join` add both.

### 1. Bot account and token

1. System Console → Integrations → Bot Accounts → *Enable Bot Account
   Creation*: true.
2. Product menu (top left) → Integrations → Bot Accounts → *Add Bot Account*.
   Username `harness` (this is what people mention), display name and icon
   as you like, role *Member*. It never needs `post:all` or admin rights: it only
   posts into threads of channels it belongs to and into direct messages.
3. On the bot, *Create New Token*, description `broker`. Copy it: this is
   `MM_BOT_TOKEN`. The same token authenticates the WebSocket event stream.
4. The bot's owner is the admin who created it. With *Disable bots when
   owner is deactivated* on (the default), deactivating that admin silently
   stops the bridge, so create it from an account that stays.

### 2. Team and channel membership

The broker only receives `posted` events for channels the bot is a member
of. Add the bot to every team (`/invite @harness` from any channel of that team,
or System Console → User Management → Teams) and to every channel where it
should react (`/invite @harness` in the channel, or *Add people*). Private
channels work the same way. Direct messages to the bot need no setup;
pairing (`pair` in a DM) works as soon as the bot exists.

### 3. Approval callbacks

Allow/Deny buttons make the Mattermost *server* POST to
`PUBLIC_URL/callback/approval`. Requirements:

- `PUBLIC_URL` is reachable from the Mattermost server with a certificate
  it trusts. A public hostname behind Caddy or nginx with Let's Encrypt is
  the normal case.
- If the broker lives on a private address or an internal hostname,
  Mattermost refuses to call it unless the host is listed in System Console
  → Environment → Web Server → *Allowed untrusted internal connections*
  (`ServiceSettings.AllowedUntrustedInternalConnections`).
- Interactive message actions are always enabled; there is no switch. Keep
  the default *Outgoing integration requests timeout* (30 s), the callback
  answers immediately.
- The callback is verified by an HMAC in the button context and by the
  clicking user's id, so the endpoint can stay unauthenticated at the
  proxy. Do not put basic auth or an IP allowlist in front of
  `/callback/approval` unless it admits the Mattermost server.

### 4. Onboarding: slash command and admin token

`/harness init` needs two more things on the broker:

- **A slash command** `/harness` (team command, method POST, URL
  `PUBLIC_URL/commands/harness`, autocomplete on). Its token becomes
  `MM_COMMAND_TOKEN`; the broker refuses requests with another token.
- **An admin token** as `MM_ADMIN_TOKEN`: a personal access token of a
  system admin user. Mattermost lets no bot create bots, so this is what
  creates the per-user bots, their tokens, and team and channel
  memberships. The broker touches it only for `/harness init` and
  `/harness join`. Treat it accordingly: a dedicated admin user (say
  `mhb-admin`) whose token lives only in the broker's environment.

Without `MM_ADMIN_TOKEN` onboarding is off and the shared-bot pairing via
DM keeps working.

### 5. Same thing with Pulumi

With [`@bambamboole/pulumi-mattermost`](https://github.com/bambamboole/pulumi-provider-mattermost):

```ts
import * as mattermost from "@bambamboole/pulumi-mattermost";

const harness = new mattermost.Bot("harness", {
    username: "harness",
    displayName: "Claude Code",
    description: "Runs Claude Code jobs on the mentioning user's machine",
});
new mattermost.TeamMember("harness", { teamId: team.id, userId: harness.userId });
new mattermost.ChannelMember("harness-dev", { channelId: dev.id, userId: harness.userId });
const brokerToken = new mattermost.AccessToken("harness-broker", {
    userId: harness.userId,
    description: "broker",
});
export const mmBotToken = pulumi.secret(brokerToken.token); // -> MM_BOT_TOKEN
```

```ts
// Onboarding: the /harness command and the admin token for creating bots.
const command = new mattermost.Command("harness", {
    teamId: team.id, trigger: "harness", method: "P",
    url: "https://broker.example.com/commands/harness",
    autoComplete: true, autoCompleteHint: "init <code> | join | status",
    autoCompleteDesc: "Pair your machine with the Claude Code bridge",
    displayName: "Harness", description: "mhb onboarding",
});
const admin = new mattermost.User("mhb-admin", {
    username: "mhb-admin", email: "mhb-admin@example.com",
    roles: [mattermost.SystemRole.User, mattermost.SystemRole.Admin],
});
const adminToken = new mattermost.AccessToken("mhb-admin", { userId: admin.id, description: "mhb broker onboarding" });
export const mmCommandToken = pulumi.secret(command.token); // -> MM_COMMAND_TOKEN
export const mmAdminToken = pulumi.secret(adminToken.token); // -> MM_ADMIN_TOKEN
```

`enableBotAccountCreation: true` on `mattermost.SystemConfig` is the only
server setting involved.

### 6. Smoke test

1. DM the bot `pair`: it answers with an `mhb harness pair …` line.
2. In a channel the bot is in, post `@harness help`: it answers in a thread.
   No answer means the bot is not a channel member or the WebSocket did not
   connect (check the broker log for `mattermost websocket connected`).
3. Pair a harness, post `@harness ws:<name> run git status`: the status post
   turns into a running state and then a result.
4. Trigger an approval, for example `@harness create a file called hello.txt`,
   and click *Allow*. If the click shows a spinner and nothing happens,
   Mattermost cannot reach `PUBLIC_URL/callback/approval`; the server log
   then contains the outgoing request error.

## Broker setup

Run the broker behind a TLS reverse proxy (Caddy, nginx). Mattermost must
reach `PUBLIC_URL/callback/approval`; harnesses reach
`PUBLIC_URL/harness/v1` and `PUBLIC_URL/pair`.

```sh
export MM_URL=https://mm.example.com
export MM_BOT_TOKEN=...                          # the listener bot
export MM_ADMIN_TOKEN=...                        # optional: enables /harness init (bot per user)
export MM_COMMAND_TOKEN=...                      # optional: the /harness slash command
export PUBLIC_URL=https://broker.example.com
export CALLBACK_SECRET=$(openssl rand -hex 32)   # signs approval buttons
export DB_PATH=/var/lib/broker/broker.db
mhb broker
```

Every setting is also a flag (`mhb broker --help`): `--mm-url`, `--mm-bot-token`,
`--mm-admin-token`, `--command-token`, `--public-url`, `--callback-secret`, `--db`, `--listen`, `--queue-ttl`,
`--grace-period`, `--job-timeout`, `--min-harness-version`. Flags win over
environment variables.

`--min-harness-version` compares dotted numbers and treats anything it
cannot parse as older, so setting it locks out harnesses built from source
(their version is `dev`). `GET /healthz` answers `ok` for a load balancer.

The database at `DB_PATH` holds the per-user bot tokens in plaintext, because
posting as a user's bot needs them. Keep it on a volume only the broker
reads; see [SECURITY.md](SECURITY.md) for the whole trust model.

## Harness setup (each developer)

```sh
# grab mhb_<version>_<os>_<arch>.tar.gz from the GitHub release, or:
go install github.com/bambamboole/mattermost-harness-bridge/cmd/mhb@latest
mhb harness init --broker https://broker.example.com
#   → shows "/harness init K7QX3M2P"; type that in a Mattermost channel
#   → creates your bot @harness-<username>, pairs this machine, asks for
#     the default agent and your workspaces
mhb harness run
```

`init` is a device flow: the laptop asks the broker for a code and polls;
`/harness init <code>` in Mattermost is authenticated by the slash
command's token and carries your user id, so the broker knows who is
pairing without a DM. The first init creates **your own bot** through the
broker's admin token; every job you start posts as that bot, and only you
can trigger it: someone else mentioning `@harness-you` gets "Only @you can
run jobs on this machine". `--bot <name>` picks another bot name, `--name`
another machine name, `--yes` skips the prompts (`--agent`,
`--default-workspace`, `--workspace name=dir` fill the config instead). A
second machine runs `init` again and shares the bot.

- `/harness join` in a channel brings your bot (and the listener bot) into it.
- `/harness status` lists your machines.
- Brokers without `MM_ADMIN_TOKEN` keep the old flow: DM `pair` to the shared bot and `mhb harness pair`.

Config lives in `~/Library/Application Support/mm-harness/config.json`
(`$XDG_CONFIG_HOME/mm-harness` on Linux); `mhb harness config` prints it with
the token redacted (`--show-secrets` to see it). `mhb harness workspace
add|rm|list` edits the directory list. Relevant keys:

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

- `@harness ws:infra bump the mattermost provider` starts a job in workspace `infra`; without `ws:` it runs in the harness's default folder.
- `@harness agent:codex …` picks the agent; without `agent:` the harness's default is used.
- Reply in the same thread to continue: the harness resumes the agent
  session it kept for that thread. `ws:` and `agent:` can be omitted then.
- Mentioning the bot for the first time inside an existing thread hands the
  agent the thread so far (the last 60 posts, without the bot's own), so
  "do what the thread says" works.
- `@harness cancel` in a thread stops the job.
- DM the bot: `pair`, `status`.

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
  -e MM_URL=... -e MM_BOT_TOKEN=... -e PUBLIC_URL=... -e CALLBACK_SECRET=... \
  ghcr.io/bambamboole/mattermost-harness-bridge:latest
```

## Development

```sh
go test ./... -race      # unit, store conformance, end-to-end
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.0 run ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean --skip=publish
docker build -t mhb:local .
```

[CONTRIBUTING.md](CONTRIBUTING.md) has the commit-message rules
release-please depends on and what to do when the wire protocol changes.

The permission-tool contract was verified against claude 2.1.263: the tool
receives `{"tool_name","input","tool_use_id"}` and returns a JSON string
`{"behavior":"allow","updatedInput":{...}}` or `{"behavior":"deny","message":"..."}`.

## Not done yet

- launchd/systemd unit for the harness and a self-update path.
- Approval posts are only rewritten on click; expired ones keep their buttons (clicking says so).
- Per-channel default workspace on the broker; today it is `ws:` in the message, else the thread's previous workspace, else the only configured one.
- `--include-partial-messages` streaming; progress currently updates per assistant message.
- No `mhb harness unpair` / broker-side harness removal; the store supports it, the CLI does not.
- `POST /init` and `GET /init/{code}` are unauthenticated and unthrottled. The
  codes are 40 bits and live 10 minutes, but a rate limit belongs in front of them.
- Retention: acked outbox rows are swept after 24 h, jobs, approvals, expired
  onboarding codes and the audit log are kept forever.

## License

MIT, see [LICENSE](LICENSE).
