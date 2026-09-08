CREATE TABLE harnesses (
  id           TEXT PRIMARY KEY,
  mm_user_id   TEXT NOT NULL,
  name         TEXT NOT NULL,
  token_hash   TEXT NOT NULL UNIQUE,
  version      TEXT NOT NULL DEFAULT '',
  created_at   INTEGER NOT NULL,
  last_seen_at INTEGER
);
CREATE INDEX harnesses_user ON harnesses(mm_user_id);

-- Each owner can have multiple bots; each bot binds to one local harness.
CREATE TABLE bots (
  user_id          TEXT PRIMARY KEY,
  mm_user_id       TEXT NOT NULL,
  username         TEXT NOT NULL UNIQUE,
  token            TEXT NOT NULL,
  created_at       INTEGER NOT NULL,
  harness_id       TEXT REFERENCES harnesses(id),
  team_id          TEXT NOT NULL DEFAULT '',
  channel_id       TEXT NOT NULL DEFAULT '',
  incoming_hook_id TEXT NOT NULL DEFAULT '',
  outgoing_hook_id TEXT NOT NULL DEFAULT '',
  outgoing_token   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX bots_owner ON bots(mm_user_id);
CREATE INDEX bots_harness ON bots(harness_id);

CREATE TABLE installations (
  team_id       TEXT PRIMARY KEY,
  user_id       TEXT NOT NULL,
  credentials   TEXT NOT NULL,
  command_id    TEXT NOT NULL,
  command_token TEXT NOT NULL
);

-- The laptop creates a code, the owner claims it with /harness init,
-- and the laptop fetches the result once using its separate polling secret.
CREATE TABLE init_requests (
  code_hash       TEXT PRIMARY KEY,
  bot_name        TEXT NOT NULL DEFAULT '',
  harness_name    TEXT NOT NULL DEFAULT '',
  created_at      INTEGER NOT NULL,
  expires_at      INTEGER NOT NULL,
  claimed_at      INTEGER,
  harness_id      TEXT NOT NULL DEFAULT '',
  harness_token   TEXT NOT NULL DEFAULT '',
  fetched_at      INTEGER,
  bot_user_id     TEXT NOT NULL DEFAULT '',
  poll_token_hash TEXT NOT NULL DEFAULT ''
);

CREATE TABLE pairings (
  code_hash  TEXT PRIMARY KEY,
  mm_user_id TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  used_at    INTEGER
);

CREATE TABLE jobs (
  id              TEXT PRIMARY KEY,
  harness_id      TEXT NOT NULL REFERENCES harnesses(id),
  mm_user_id      TEXT NOT NULL,
  channel_id      TEXT NOT NULL,
  root_post_id    TEXT NOT NULL,
  trigger_post_id TEXT NOT NULL,
  status_post_id  TEXT NOT NULL DEFAULT '',
  bot_user_id     TEXT NOT NULL DEFAULT '',
  workspace       TEXT NOT NULL,
  prompt          TEXT NOT NULL,
  state           TEXT NOT NULL,
  last_seq        INTEGER NOT NULL DEFAULT 0,
  result_text     TEXT NOT NULL DEFAULT '',
  error           TEXT NOT NULL DEFAULT '',
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  expires_at      INTEGER,
  finished_at     INTEGER
);
CREATE INDEX jobs_harness_state ON jobs(harness_id, state);
CREATE INDEX jobs_thread ON jobs(root_post_id);
CREATE INDEX jobs_user ON jobs(mm_user_id, created_at);
CREATE UNIQUE INDEX jobs_bot_trigger ON jobs(bot_user_id, trigger_post_id)
 WHERE bot_user_id <> '' AND trigger_post_id <> '';

CREATE TABLE approvals (
  id           TEXT PRIMARY KEY,
  job_id       TEXT NOT NULL REFERENCES jobs(id),
  tool         TEXT NOT NULL,
  summary      TEXT NOT NULL,
  input        TEXT NOT NULL,
  nonce        TEXT NOT NULL,
  requested_at INTEGER NOT NULL,
  expires_at   INTEGER NOT NULL,
  decision     TEXT,
  decided_by   TEXT,
  decided_at   INTEGER
);
CREATE INDEX approvals_job ON approvals(job_id);

CREATE TABLE outbox (
  id         TEXT PRIMARY KEY,
  harness_id TEXT NOT NULL,
  job_id     TEXT NOT NULL DEFAULT '',
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  acked_at   INTEGER
);
CREATE INDEX outbox_pending ON outbox(harness_id, created_at) WHERE acked_at IS NULL;

CREATE TABLE audit_log (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  ts      INTEGER NOT NULL,
  actor   TEXT NOT NULL,
  action  TEXT NOT NULL,
  job_id  TEXT NOT NULL DEFAULT '',
  details TEXT
);
CREATE INDEX audit_job ON audit_log(job_id);
