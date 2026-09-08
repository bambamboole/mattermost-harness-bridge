# Contributing

## Before you open a PR

```sh
go test ./... -race
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.0 run ./...
go mod tidy   # CI fails if go.mod or go.sum change
```

CI runs the same three plus `govulncheck`, a goreleaser config check and a
Docker build.

## Commit messages

[Conventional commits](https://www.conventionalcommits.org/), because
release-please derives the version and the changelog from them:

- `feat(scope): …` → minor bump, "Features" section
- `fix(scope): …` → patch bump, "Bug Fixes"
- `docs|refactor|build|chore(scope): …` → patch bump, own section
- `feat(scope)!: …` or a `BREAKING CHANGE:` trailer → major (minor while 0.x)

Scopes in use: `broker`, `harness`, `protocol`, `store`, `cli`, `onboarding`,
`agent`, `oauth`, `docker`, `ci`.

## Where things go

The [Layout table in the README](README.md#layout) is the map. Two rules
carry most of the weight:

- `internal/protocol` may not import either side. It is the contract; both
  binaries depend on it and nothing else crosses.
- `internal/broker/core` talks to interfaces (`store.Store`,
  `mattermost.API`, `hub.Sender`) only, which is what lets `core_test.go`
  drive the whole broker without a network.

## Changing the wire protocol

Additive changes (a new optional field, a new message type) stay on the
current path (`/harness/v2`); unknown types are answered with an `error` and
ignored.
Anything that breaks an older harness bumps the path in
`internal/protocol.Path` — a broker can then keep the old handler around
during a rollout. `--min-harness-version` is the blunt alternative.

## Tests

- `internal/store/storetest` is a conformance suite: any new `store.Store`
  implementation has to pass it, so new store methods belong there too.
- `internal/e2e` runs the real hub and the real harness over a live
  WebSocket with the test binary standing in for `claude`. Anything that
  touches dispatch, approvals, reconnect or session reuse should be visible
  there.
- Agent drivers are tested against fake CLIs; see
  `internal/harness/agent/claude/claude_test.go` for the pattern.

## Releases

Merging to `main` updates a release-please PR. Merging *that* tags the
release, which triggers GoReleaser (archives) and the image push. Nothing is
released by hand.
