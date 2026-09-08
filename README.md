# mattermost-harness-bridge

Mention a bot in Mattermost, and Claude Code runs the job on **your own
machine**: your repos, your Claude login, your context. Tool calls that need
permission become Allow/Deny buttons in the thread.

```
Mattermost (cloud, bot @cc)
      ^ WS events down / REST posts up
      v
Broker (Go, public)                     mhb broker
      ^ jobs + approvals down / hello, progress, results up
      v
Harness (Go, one per developer, NAT)    mhb harness run
      v
claude -p ... (subprocess per job)
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
| `internal/harness` | Daemon: broker client with reconnect, job runner, permission bridge, thread→session map |
| `internal/harness/runner` | Drives `claude -p --output-format stream-json` |
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

### 1. Bot account and token

1. System Console → Integrations → Bot Accounts → *Enable Bot Account
   Creation*: true.
2. Product menu (top left) → Integrations → Bot Accounts → *Add Bot Account*.
   Username `cc` (this is what people mention), display name and icon as you
   like, role *Member*. It never needs `post:all` or admin rights: it only
   posts into threads of channels it belongs to and into direct messages.
3. On the bot, *Create New Token*, description `broker`. Copy it: this is
   `MM_BOT_TOKEN`. The same token authenticates the WebSocket event stream.
4. The bot's owner is the admin who created it. With *Disable bots when
   owner is deactivated* on (the default), deactivating that admin silently
   stops the bridge, so create it from an account that stays.

### 2. Team and channel membership

The broker only receives `posted` events for channels the bot is a member
of. Add the bot to every team (`/invite @cc` from any channel of that team,
or System Console → User Management → Teams) and to every channel where it
should react (`/invite @cc` in the channel, or *Add people*). Private
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

### 4. Same thing with Pulumi

With [`@bambamboole/pulumi-mattermost`](https://github.com/bambamboole/pulumi-provider-mattermost):

```ts
import * as mattermost from "@bambamboole/pulumi-mattermost";

const cc = new mattermost.Bot("cc", {
    username: "cc",
    displayName: "Claude Code",
    description: "Runs Claude Code jobs on the mentioning user's machine",
});
new mattermost.TeamMember("cc", { teamId: team.id, userId: cc.userId });
new mattermost.ChannelMember("cc-dev", { channelId: dev.id, userId: cc.userId });
const brokerToken = new mattermost.AccessToken("cc-broker", {
    userId: cc.userId,
    description: "broker",
});
export const mmBotToken = pulumi.secret(brokerToken.token); // -> MM_BOT_TOKEN
```

`enableBotAccountCreation: true` on `mattermost.SystemConfig` is the only
server setting involved.

### 5. Smoke test

1. DM the bot `pair`: it answers with an `mhb harness pair …` line.
2. In a channel the bot is in, post `@cc help`: it answers in a thread.
   No answer means the bot is not a channel member or the WebSocket did not
   connect (check the broker log for `mattermost websocket connected`).
3. Pair a harness, post `@cc ws:<name> run git status`: the status post
   turns into a running state and then a result.
4. Trigger an approval, for example `@cc create a file called hello.txt`,
   and click *Allow*. If the click shows a spinner and nothing happens,
   Mattermost cannot reach `PUBLIC_URL/callback/approval`; the server log
   then contains the outgoing request error.

## Broker setup

Run the broker behind a TLS reverse proxy (Caddy, nginx). Mattermost must
reach `PUBLIC_URL/callback/approval`; harnesses reach
`PUBLIC_URL/harness/v1` and `PUBLIC_URL/pair`.

```sh
export MM_URL=https://mm.example.com
export MM_BOT_TOKEN=...
export PUBLIC_URL=https://broker.example.com
export CALLBACK_SECRET=$(openssl rand -hex 32)   # signs approval buttons
export DB_PATH=/var/lib/broker/broker.db
mhb broker
```

Every setting is also a flag (`mhb broker --help`): `--mm-url`, `--mm-bot-token`,
`--public-url`, `--callback-secret`, `--db`, `--listen`, `--queue-ttl`,
`--grace-period`, `--job-timeout`, `--min-harness-version`. Flags win over
environment variables.

## Harness setup (each developer)

```sh
# grab mhb_<version>_<os>_<arch>.tar.gz from the GitHub release, or:
go install github.com/bambamboole/mattermost-harness-bridge/cmd/mhb@latest
# In Mattermost, DM the bot: `pair`  → it replies with a one-time code
mhb harness pair --broker https://broker.example.com <code>
mhb harness workspace add infra ~/Projects/artisan-os/infrastructure
mhb harness run
```

Config lives in `~/Library/Application Support/mm-harness/config.json`
(`$XDG_CONFIG_HOME/mm-harness` on Linux). Relevant keys:

- `workspaces`: name → absolute directory. Only these are ever handed to Claude.
- `allowed_tools`: run without asking (default: Read, Grep, Glob, LS, WebSearch, WebFetch).
- `disallowed_tools`: never run.
- `max_jobs`, `default_max_turns`, `approval_timeout_min`, `claude_bin`, `model`.

Everything else goes through the approval flow: the CLI calls the
harness's MCP permission tool, the harness sends `approval.request`, the
broker posts Allow/Deny buttons, the owner clicks, the decision travels
back. `--dangerously-skip-permissions` is never used.

## Using it

- `@cc ws:infra bump the mattermost provider` starts a job in workspace `infra`.
- Reply in the same thread to continue: the harness resumes the Claude
  session it kept for that thread. `ws:` can be omitted then.
- `@cc cancel` in a thread stops the job.
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
