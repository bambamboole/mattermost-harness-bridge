-- Existing bots are retained, but require explicit new onboarding to bind a
-- harness. A user is no longer a unique bot key.
ALTER TABLE bots RENAME TO old_bots;
CREATE TABLE bots (
  user_id TEXT PRIMARY KEY,
  mm_user_id TEXT NOT NULL,
  username TEXT NOT NULL UNIQUE,
  token TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  harness_id TEXT REFERENCES harnesses(id),
  team_id TEXT NOT NULL DEFAULT '',
  channel_id TEXT NOT NULL DEFAULT '',
  incoming_hook_id TEXT NOT NULL DEFAULT '',
  outgoing_hook_id TEXT NOT NULL DEFAULT '',
  outgoing_token TEXT NOT NULL DEFAULT ''
);
INSERT INTO bots (user_id, mm_user_id, username, token, created_at)
 SELECT user_id, mm_user_id, username, token, created_at FROM old_bots;
DROP TABLE old_bots;
CREATE INDEX bots_owner ON bots(mm_user_id);
CREATE INDEX bots_harness ON bots(harness_id);
ALTER TABLE init_requests ADD COLUMN bot_user_id TEXT NOT NULL DEFAULT '';
ALTER TABLE init_requests ADD COLUMN poll_token_hash TEXT NOT NULL DEFAULT '';
CREATE TABLE installations (
 team_id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL,
 credentials TEXT NOT NULL,
 command_id TEXT NOT NULL,
 command_token TEXT NOT NULL
);
CREATE UNIQUE INDEX jobs_bot_trigger ON jobs(bot_user_id, trigger_post_id)
 WHERE bot_user_id <> '' AND trigger_post_id <> '';
