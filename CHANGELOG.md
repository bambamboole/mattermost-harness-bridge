# Changelog

## [0.4.0](https://github.com/bambamboole/mattermost-harness-bridge/compare/v0.3.0...v0.4.0) (2026-09-08)


### ⚠ BREAKING CHANGES

* add OAuth installation and multiple bots per user

### Features

* add OAuth installation and multiple bots per user ([54266ad](https://github.com/bambamboole/mattermost-harness-bridge/commit/54266adf119aea30d1e951e43604c78290cb04a7))


### Bug Fixes

* **broker:** handle posts off the Mattermost event loop ([a3cb451](https://github.com/bambamboole/mattermost-harness-bridge/commit/a3cb4511f823220eee55d61e2928c212f89d7115))
* **broker:** recognise user bots from the [@name](https://github.com/name) in the message ([ec92442](https://github.com/bambamboole/mattermost-harness-bridge/commit/ec9244237c29c7a2579a3254e11993b6522ad708))
* **broker:** rune-safe post budgets, outbox purge, owner naming ([7132186](https://github.com/bambamboole/mattermost-harness-bridge/commit/713218697d312b1e0cff6b39b50ede048adbbd26))
* **harness:** cut chat text on rune boundaries ([0be4400](https://github.com/bambamboole/mattermost-harness-bridge/commit/0be44004c7b6c10e813cc8d618606a743055617b))
* **harness:** redact the token in mhb harness config ([8ea6ba4](https://github.com/bambamboole/mattermost-harness-bridge/commit/8ea6ba4360e6dbc1fcb83b79258ce6dd553fe7e2))
* **harness:** reset the reconnect backoff after a healthy connection ([1f86f67](https://github.com/bambamboole/mattermost-harness-bridge/commit/1f86f67c1a5afe3a8ca3b3c5527367f0306356d5))
* **onboarding:** say so when the command ran in a direct or group message ([6fb5607](https://github.com/bambamboole/mattermost-harness-bridge/commit/6fb56070604f53794f58d141b571a0a8001d3ee3))

## [0.3.0](https://github.com/bambamboole/mattermost-harness-bridge/compare/v0.2.1...v0.3.0) (2026-09-08)


### Features

* **harness:** agent adapters, default workspace, thread history ([69fee8a](https://github.com/bambamboole/mattermost-harness-bridge/commit/69fee8a00a0ff5c4e2620b79628a6d3d66e94d06))
* **onboarding:** mhb harness init with a bot per user ([4d367c4](https://github.com/bambamboole/mattermost-harness-bridge/commit/4d367c4ae0d8d7c616c8a91bcd0a112d457ec090))


### Bug Fixes

* **broker:** accept the ws: token anywhere in the mention ([c9f0b46](https://github.com/bambamboole/mattermost-harness-bridge/commit/c9f0b46981c43ad6749c1509b93b3dc43c9cc7d9))

## [0.2.1](https://github.com/bambamboole/mattermost-harness-bridge/compare/v0.2.0...v0.2.1) (2026-09-08)


### Bug Fixes

* **docker:** let the nonroot broker create its database in /data ([5009191](https://github.com/bambamboole/mattermost-harness-bridge/commit/5009191911a01d90b22eae8767577dd0ed4913fb))

## [0.2.0](https://github.com/bambamboole/mattermost-harness-bridge/compare/v0.1.0...v0.2.0) (2026-09-08)


### Features

* **cli:** single mhb binary with cobra subcommands ([ef0deea](https://github.com/bambamboole/mattermost-harness-bridge/commit/ef0deea3b5b41b9ff177b966088230d154fbb268))

## 0.1.0 (2026-09-08)


### Features

* broker, harness and permission bridge ([2adc026](https://github.com/bambamboole/mattermost-harness-bridge/commit/2adc026d47619d003208b1a70d30338bf44323e7))
* wire protocol and SQLite-backed store with conformance suite ([31e687d](https://github.com/bambamboole/mattermost-harness-bridge/commit/31e687d475894371c64bdb3eb099235317d0e7fa))


### Build System

* start releases at 0.1.0 ([c5d32e5](https://github.com/bambamboole/mattermost-harness-bridge/commit/c5d32e51f7c902454c00046ab2d9fc428d60dd57))
