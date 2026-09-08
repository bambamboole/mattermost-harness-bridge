# mattermost-harness-bridge

Mention a bot in Mattermost, and Claude Code runs the job on **your own
machine**: your repos, your Claude login, your context. Tool calls that need
permission become Allow/Deny buttons in the thread.

```
Mattermost (cloud, bot @cc)
      ^ WS events down / REST posts up
      v
Broker (Go, one public binary)          cmd/broker
      ^ jobs + approvals down / hello, progress, results up
      v
Harness (Go, one per developer, NAT)    cmd/harness
      v
claude -p ... (subprocess per job)
```

Harnesses connect *outbound* to the broker over WebSocket. Nothing ever
connects to a laptop. Only the owner of a harness can trigger it.

## Layout

| Path | What |
|---|---|
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

## Broker setup

1. Create a bot account in Mattermost (System Console → Integrations → Bot
   Accounts) and copy its token. Enable *Integrations → Interactive
   messages* if it is off.
2. Run the broker behind a TLS reverse proxy (Caddy, nginx). Mattermost
   must reach `PUBLIC_URL/callback/approval`; harnesses reach
   `PUBLIC_URL/harness/v1` and `PUBLIC_URL/pair`.

```sh
export MM_URL=https://mm.example.com
export MM_BOT_TOKEN=...
export PUBLIC_URL=https://broker.example.com
export CALLBACK_SECRET=$(openssl rand -hex 32)   # signs approval buttons
export DB_PATH=/var/lib/broker/broker.db
# optional: LISTEN_ADDR=:8080 QUEUE_TTL=5m GRACE_PERIOD=10m JOB_TIMEOUT=30m MIN_HARNESS_VERSION=0.1.0
go run ./cmd/broker
```

## Harness setup (each developer)

```sh
go build -o ~/bin/harness ./cmd/harness
# In Mattermost, DM the bot: `pair`  → it replies with a one-time code
harness pair --broker https://broker.example.com <code>
harness workspace add infra ~/Projects/artisan-os/infrastructure
harness run
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
The tag triggers `release.yml`: GoReleaser attaches `broker` and `harness`
archives for linux/darwin × amd64/arm64 plus `checksums.txt`, and the broker
image is pushed to `ghcr.io/bambamboole/mattermost-harness-bridge` as
`<version>`, `<major>.<minor>` and `latest`.

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
docker build -t broker:local .
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
