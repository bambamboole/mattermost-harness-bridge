-- One bot per user, created by /harness init through the admin token. The
-- token posts on the owner's behalf; the shared listener bot only reads.
CREATE TABLE bots (
  user_id    TEXT PRIMARY KEY,
  mm_user_id TEXT NOT NULL UNIQUE,
  username   TEXT NOT NULL UNIQUE,
  token      TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

-- Which bot posts for a job; '' means the shared listener bot.
ALTER TABLE jobs ADD COLUMN bot_user_id TEXT NOT NULL DEFAULT '';

-- Device-flow onboarding: the laptop creates a code, the owner claims it
-- with /harness init in Mattermost, the laptop fetches the result once.
CREATE TABLE init_requests (
  code_hash     TEXT PRIMARY KEY,
  bot_name      TEXT NOT NULL DEFAULT '',
  harness_name  TEXT NOT NULL DEFAULT '',
  created_at    INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,
  claimed_at    INTEGER,
  harness_id    TEXT NOT NULL DEFAULT '',
  harness_token TEXT NOT NULL DEFAULT '',
  fetched_at    INTEGER
);
