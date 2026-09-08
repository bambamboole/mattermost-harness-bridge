// Package sqlite implements store.Store on modernc.org/sqlite (no cgo).
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	db *sql.DB
}

var _ store.Store = (*Store)(nil)

// Open opens (and creates) the database at path and applies migrations.
// Use ":memory:" for tests.
func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)" +
		"&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)"
	if path == ":memory:" {
		dsn = "file::memory:?_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: a single writer and no SQLITE_BUSY handling in callers.
	// For :memory: it is also what keeps every query on the same database.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, name).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`, name, time.Now().UnixMilli()); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

func ms(t time.Time) int64 { return t.UnixMilli() }

func msPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMilli()
}

func fromMS(v int64) time.Time { return time.UnixMilli(v).UTC() }

func fromMSPtr(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMS(v.Int64)
	return &t
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// --- harnesses -------------------------------------------------------------

const harnessCols = `id, mm_user_id, name, token_hash, version, created_at, last_seen_at`

func scanHarness(row interface{ Scan(...any) error }) (store.Harness, error) {
	var h store.Harness
	var created int64
	var seen sql.NullInt64
	if err := row.Scan(&h.ID, &h.MMUserID, &h.Name, &h.TokenHash, &h.Version, &created, &seen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return h, store.ErrNotFound
		}
		return h, err
	}
	h.CreatedAt = fromMS(created)
	h.LastSeenAt = fromMSPtr(seen)
	return h, nil
}

func (s *Store) CreateHarness(ctx context.Context, h store.Harness) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO harnesses (`+harnessCols+`) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		h.ID, h.MMUserID, h.Name, h.TokenHash, h.Version, ms(h.CreatedAt), msPtr(h.LastSeenAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) HarnessByID(ctx context.Context, id string) (store.Harness, error) {
	return scanHarness(s.db.QueryRowContext(ctx, `SELECT `+harnessCols+` FROM harnesses WHERE id = ?`, id))
}

func (s *Store) HarnessByTokenHash(ctx context.Context, hash string) (store.Harness, error) {
	return scanHarness(s.db.QueryRowContext(ctx, `SELECT `+harnessCols+` FROM harnesses WHERE token_hash = ?`, hash))
}

func (s *Store) HarnessesByUser(ctx context.Context, mmUserID string) ([]store.Harness, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+harnessCols+` FROM harnesses WHERE mm_user_id = ? ORDER BY created_at`, mmUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.Harness
	for rows.Next() {
		h, err := scanHarness(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) TouchHarness(ctx context.Context, id string, seenAt time.Time, version string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE harnesses SET last_seen_at = ?, version = ? WHERE id = ?`, ms(seenAt), version, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) DeleteHarness(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM harnesses WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// --- pairings --------------------------------------------------------------

func (s *Store) CreatePairing(ctx context.Context, codeHash, mmUserID string, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO pairings (code_hash, mm_user_id, expires_at) VALUES (?, ?, ?)`, codeHash, mmUserID, ms(expiresAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) ConsumePairing(ctx context.Context, codeHash string, now time.Time) (string, error) {
	var mmUserID string
	err := s.db.QueryRowContext(ctx,
		`UPDATE pairings SET used_at = ? WHERE code_hash = ? AND used_at IS NULL AND expires_at > ? RETURNING mm_user_id`,
		ms(now), codeHash, ms(now)).Scan(&mmUserID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return mmUserID, err
}

// --- jobs ------------------------------------------------------------------

const jobCols = `id, harness_id, mm_user_id, channel_id, root_post_id, trigger_post_id, status_post_id, bot_user_id,
	workspace, prompt, state, last_seq, result_text, error, created_at, updated_at, expires_at, finished_at`

func scanJob(row interface{ Scan(...any) error }) (store.Job, error) {
	var j store.Job
	var created, updated int64
	var expires, finished sql.NullInt64
	err := row.Scan(&j.ID, &j.HarnessID, &j.MMUserID, &j.ChannelID, &j.RootPostID, &j.TriggerPostID, &j.StatusPostID, &j.BotUserID,
		&j.Workspace, &j.Prompt, (*string)(&j.State), &j.LastSeq, &j.ResultText, &j.Error, &created, &updated, &expires, &finished)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return j, store.ErrNotFound
		}
		return j, err
	}
	j.CreatedAt = fromMS(created)
	j.UpdatedAt = fromMS(updated)
	j.ExpiresAt = fromMSPtr(expires)
	j.FinishedAt = fromMSPtr(finished)
	return j, nil
}

func (s *Store) queryJobs(ctx context.Context, q string, args ...any) ([]store.Job, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) CreateJob(ctx context.Context, j store.Job) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO jobs (`+jobCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.HarnessID, j.MMUserID, j.ChannelID, j.RootPostID, j.TriggerPostID, j.StatusPostID, j.BotUserID,
		j.Workspace, j.Prompt, string(j.State), j.LastSeq, j.ResultText, j.Error,
		ms(j.CreatedAt), ms(j.UpdatedAt), msPtr(j.ExpiresAt), msPtr(j.FinishedAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) JobByID(ctx context.Context, id string) (store.Job, error) {
	return scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
}

func (s *Store) ListJobs(ctx context.Context, f store.JobFilter) ([]store.Job, error) {
	where := []string{"1=1"}
	var args []any
	if f.MMUserID != "" {
		where = append(where, "mm_user_id = ?")
		args = append(args, f.MMUserID)
	}
	if f.HarnessID != "" {
		where = append(where, "harness_id = ?")
		args = append(args, f.HarnessID)
	}
	if f.RootPostID != "" {
		where = append(where, "root_post_id = ?")
		args = append(args, f.RootPostID)
	}
	if len(f.States) > 0 {
		where = append(where, "state IN ("+placeholders(len(f.States))+")")
		for _, st := range f.States {
			args = append(args, string(st))
		}
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	return s.queryJobs(ctx, `SELECT `+jobCols+` FROM jobs WHERE `+strings.Join(where, " AND ")+` ORDER BY created_at DESC LIMIT ?`, args...)
}

func (s *Store) ActiveJobsByHarness(ctx context.Context, harnessID string) ([]store.Job, error) {
	return s.ListJobs(ctx, store.JobFilter{HarnessID: harnessID, States: store.ActiveStates, Limit: 1000})
}

func (s *Store) TransitionJob(ctx context.Context, id string, from []store.JobState, to store.JobState, p store.JobPatch) (store.Job, error) {
	if len(from) == 0 {
		return store.Job{}, errors.New("sqlite: TransitionJob needs at least one from state")
	}
	now := time.Now().UnixMilli()
	set := []string{"state = ?", "updated_at = ?"}
	args := []any{string(to), now}
	if p.StatusPostID != nil {
		set = append(set, "status_post_id = ?")
		args = append(args, *p.StatusPostID)
	}
	if p.ResultText != nil {
		set = append(set, "result_text = ?")
		args = append(args, *p.ResultText)
	}
	if p.Error != nil {
		set = append(set, "error = ?")
		args = append(args, *p.Error)
	}
	if to.Terminal() {
		set = append(set, "finished_at = ?")
		args = append(args, now)
	}
	args = append(args, id)
	for _, f := range from {
		args = append(args, string(f))
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET `+strings.Join(set, ", ")+` WHERE id = ? AND state IN (`+placeholders(len(from))+`)`, args...)
	if err != nil {
		return store.Job{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, err := s.JobByID(ctx, id); err != nil {
			return store.Job{}, err
		}
		return store.Job{}, store.ErrConflict
	}
	return s.JobByID(ctx, id)
}

func (s *Store) RecordProgress(ctx context.Context, id string, seq int64) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET last_seq = ?, updated_at = ? WHERE id = ? AND last_seq < ? AND state IN ('running', 'awaiting_approval')`,
		seq, time.Now().UnixMilli(), id, seq)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) ExpireQueuedJobs(ctx context.Context, now time.Time) ([]store.Job, error) {
	return s.queryJobs(ctx,
		`UPDATE jobs SET state = ?, updated_at = ?, finished_at = ?
		  WHERE state = ? AND expires_at IS NOT NULL AND expires_at <= ?
		  RETURNING `+jobCols,
		string(store.JobExpired), ms(now), ms(now), string(store.JobQueued), ms(now))
}

// --- approvals -------------------------------------------------------------

const approvalCols = `id, job_id, tool, summary, input, nonce, requested_at, expires_at, decision, decided_by, decided_at`

func scanApproval(row interface{ Scan(...any) error }) (store.Approval, error) {
	var a store.Approval
	var input string
	var requested, expires int64
	var decision, decidedBy sql.NullString
	var decidedAt sql.NullInt64
	err := row.Scan(&a.ID, &a.JobID, &a.Tool, &a.Summary, &input, &a.Nonce, &requested, &expires, &decision, &decidedBy, &decidedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return a, store.ErrNotFound
		}
		return a, err
	}
	a.Input = []byte(input)
	a.RequestedAt = fromMS(requested)
	a.ExpiresAt = fromMS(expires)
	a.Decision = store.Decision(decision.String)
	a.DecidedBy = decidedBy.String
	a.DecidedAt = fromMSPtr(decidedAt)
	return a, nil
}

func (s *Store) CreateApproval(ctx context.Context, a store.Approval) error {
	input := a.Input
	if len(input) == 0 {
		input = []byte("null")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO approvals (`+approvalCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.JobID, a.Tool, a.Summary, string(input), a.Nonce, ms(a.RequestedAt), ms(a.ExpiresAt),
		nullString(string(a.Decision)), nullString(a.DecidedBy), msPtr(a.DecidedAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (s *Store) ApprovalByID(ctx context.Context, id string) (store.Approval, error) {
	return scanApproval(s.db.QueryRowContext(ctx, `SELECT `+approvalCols+` FROM approvals WHERE id = ?`, id))
}

func (s *Store) PendingApprovalsByJob(ctx context.Context, jobID string) ([]store.Approval, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+approvalCols+` FROM approvals WHERE job_id = ? AND decision IS NULL ORDER BY requested_at`, jobID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.Approval
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DecideApproval(ctx context.Context, id string, d store.Decision, by string, at time.Time) (store.Approval, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE approvals SET decision = ?, decided_by = ?, decided_at = ? WHERE id = ? AND decision IS NULL AND expires_at > ?`,
		string(d), by, ms(at), id, ms(at))
	if err != nil {
		return store.Approval{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, err := s.ApprovalByID(ctx, id); err != nil {
			return store.Approval{}, err
		}
		return store.Approval{}, store.ErrConflict
	}
	return s.ApprovalByID(ctx, id)
}

// --- outbox ----------------------------------------------------------------

func (s *Store) Enqueue(ctx context.Context, m store.OutboxMessage) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO outbox (id, harness_id, job_id, type, payload, created_at, acked_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.HarnessID, m.JobID, m.Type, string(m.Payload), ms(m.CreatedAt), msPtr(m.AckedAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) PendingOutbox(ctx context.Context, harnessID string) ([]store.OutboxMessage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, harness_id, job_id, type, payload, created_at FROM outbox WHERE harness_id = ? AND acked_at IS NULL ORDER BY created_at, id`, harnessID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.OutboxMessage
	for rows.Next() {
		var m store.OutboxMessage
		var payload string
		var created int64
		if err := rows.Scan(&m.ID, &m.HarnessID, &m.JobID, &m.Type, &payload, &created); err != nil {
			return nil, err
		}
		m.Payload = []byte(payload)
		m.CreatedAt = fromMS(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) AckOutbox(ctx context.Context, msgID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET acked_at = ? WHERE id = ? AND acked_at IS NULL`, ms(at), msgID)
	return err
}

func (s *Store) PurgeOutbox(ctx context.Context, ackedBefore time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE acked_at IS NOT NULL AND acked_at < ?`, ms(ackedBefore))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// --- bots ------------------------------------------------------------------

const botCols = `user_id, mm_user_id, username, token, created_at`

func scanBot(row interface{ Scan(...any) error }) (store.Bot, error) {
	var b store.Bot
	var created int64
	if err := row.Scan(&b.UserID, &b.MMUserID, &b.Username, &b.Token, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return b, store.ErrNotFound
		}
		return b, err
	}
	b.CreatedAt = fromMS(created)
	return b, nil
}

func (s *Store) CreateBot(ctx context.Context, b store.Bot) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO bots (`+botCols+`) VALUES (?, ?, ?, ?, ?)`,
		b.UserID, b.MMUserID, b.Username, b.Token, ms(b.CreatedAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) BotByOwner(ctx context.Context, mmUserID string) (store.Bot, error) {
	return scanBot(s.db.QueryRowContext(ctx, `SELECT `+botCols+` FROM bots WHERE mm_user_id = ?`, mmUserID))
}

func (s *Store) BotByUserID(ctx context.Context, botUserID string) (store.Bot, error) {
	return scanBot(s.db.QueryRowContext(ctx, `SELECT `+botCols+` FROM bots WHERE user_id = ?`, botUserID))
}

func (s *Store) ListBots(ctx context.Context) ([]store.Bot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+botCols+` FROM bots ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.Bot
	for rows.Next() {
		b, err := scanBot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// --- init requests ---------------------------------------------------------

const initCols = `code_hash, bot_name, harness_name, created_at, expires_at, claimed_at, harness_id, harness_token, fetched_at`

func scanInit(row interface{ Scan(...any) error }) (store.InitRequest, error) {
	var r store.InitRequest
	var created, expires int64
	var claimed, fetched sql.NullInt64
	err := row.Scan(&r.CodeHash, &r.BotName, &r.HarnessName, &created, &expires, &claimed, &r.HarnessID, &r.HarnessToken, &fetched)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return r, store.ErrNotFound
		}
		return r, err
	}
	r.CreatedAt = fromMS(created)
	r.ExpiresAt = fromMS(expires)
	r.ClaimedAt = fromMSPtr(claimed)
	r.FetchedAt = fromMSPtr(fetched)
	return r, nil
}

func (s *Store) CreateInit(ctx context.Context, r store.InitRequest) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO init_requests (`+initCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.CodeHash, r.BotName, r.HarnessName, ms(r.CreatedAt), ms(r.ExpiresAt), msPtr(r.ClaimedAt), r.HarnessID, r.HarnessToken, msPtr(r.FetchedAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	return err
}

func (s *Store) ClaimInit(ctx context.Context, codeHash string, now time.Time, harnessID, harnessToken string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE init_requests SET claimed_at = ?, harness_id = ?, harness_token = ?
		  WHERE code_hash = ? AND claimed_at IS NULL AND expires_at > ?`,
		ms(now), harnessID, harnessToken, codeHash, ms(now))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	r, err := s.initByCode(ctx, codeHash)
	if err != nil || r.ExpiresAt.Before(now) || r.ExpiresAt.Equal(now) {
		return store.ErrNotFound
	}
	return store.ErrConflict
}

func (s *Store) InitByCode(ctx context.Context, codeHash string) (store.InitRequest, error) {
	return s.initByCode(ctx, codeHash)
}

func (s *Store) initByCode(ctx context.Context, codeHash string) (store.InitRequest, error) {
	return scanInit(s.db.QueryRowContext(ctx, `SELECT `+initCols+` FROM init_requests WHERE code_hash = ?`, codeHash))
}

func (s *Store) FetchInit(ctx context.Context, codeHash string, now time.Time) (store.InitRequest, error) {
	r, err := s.initByCode(ctx, codeHash)
	if err != nil {
		return r, err
	}
	if !r.ExpiresAt.After(now) {
		return store.InitRequest{}, store.ErrNotFound
	}
	if !r.Claimed() {
		return r, nil
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE init_requests SET fetched_at = ?, harness_token = '' WHERE code_hash = ? AND fetched_at IS NULL`, ms(now), codeHash)
	if err != nil {
		return store.InitRequest{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.InitRequest{}, store.ErrConflict
	}
	fetched := now
	r.FetchedAt = &fetched
	return r, nil
}

func (s *Store) InitByHarness(ctx context.Context, harnessID string) (store.InitRequest, error) {
	return scanInit(s.db.QueryRowContext(ctx, `SELECT `+initCols+` FROM init_requests WHERE harness_id = ? ORDER BY created_at DESC LIMIT 1`, harnessID))
}

// --- audit -----------------------------------------------------------------

func (s *Store) Append(ctx context.Context, e store.AuditEntry) error {
	at := e.At
	if at.IsZero() {
		at = time.Now()
	}
	var details any
	if len(e.Details) > 0 {
		details = string(e.Details)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO audit_log (ts, actor, action, job_id, details) VALUES (?, ?, ?, ?, ?)`,
		ms(at), e.Actor, e.Action, e.JobID, details)
	return err
}
