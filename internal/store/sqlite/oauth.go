package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

func (s *Store) checkBotHarness(ctx context.Context, b store.Bot) error {
	if b.HarnessID == "" {
		return nil
	}
	h, err := s.HarnessByID(ctx, b.HarnessID)
	if err != nil {
		return err
	}
	if h.MMUserID != b.MMUserID {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) BotsByOwner(ctx context.Context, owner string) ([]store.Bot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+botCols+` FROM bots WHERE mm_user_id = ? ORDER BY created_at, user_id`, owner)
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

func (s *Store) BotByUsername(ctx context.Context, username string) (store.Bot, error) {
	return scanBot(s.db.QueryRowContext(ctx, `SELECT `+botCols+` FROM bots WHERE username = ?`, username))
}

func (s *Store) UpdateBot(ctx context.Context, b store.Bot) error {
	if err := s.checkBotHarness(ctx, b); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE bots SET harness_id=?,team_id=?,channel_id=?,token=?,incoming_hook_id=?,outgoing_hook_id=?,outgoing_token=? WHERE user_id=? AND mm_user_id=? AND (harness_id IS NULL OR harness_id=?)`, nullString(b.HarnessID), b.TeamID, b.ChannelID, b.Token, b.IncomingHookID, b.OutgoingHookID, b.OutgoingToken, b.UserID, b.MMUserID, b.HarnessID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	return nil
}

func (s *Store) DeleteBot(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM bots WHERE user_id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) CompleteInit(ctx context.Context, hash string, now time.Time, h store.Harness, botID, token string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := scanInit(tx.QueryRowContext(ctx, `SELECT `+initCols+` FROM init_requests WHERE code_hash=?`, hash))
	if err != nil {
		return err
	}
	if !r.ExpiresAt.After(now) {
		return store.ErrNotFound
	}
	if r.Claimed() {
		return store.ErrConflict
	}
	var owner string
	var bound sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT mm_user_id,harness_id FROM bots WHERE user_id=?`, botID).Scan(&owner, &bound); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}
	if owner != h.MMUserID || bound.Valid {
		return store.ErrConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO harnesses (`+harnessCols+`) VALUES (?,?,?,?,?,?,?)`, h.ID, h.MMUserID, h.Name, h.TokenHash, h.Version, ms(h.CreatedAt), msPtr(h.LastSeenAt))
	if isUniqueViolation(err) {
		return store.ErrConflict
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE bots SET harness_id=? WHERE user_id=?`, h.ID, botID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE init_requests SET claimed_at=?,harness_id=?,harness_token=?,bot_user_id=? WHERE code_hash=?`, ms(now), h.ID, token, botID, hash); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SaveInstallation(ctx context.Context, i store.Installation) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO installations(team_id,user_id,credentials,command_id,command_token) VALUES(?,?,?,?,?) ON CONFLICT(team_id) DO UPDATE SET user_id=excluded.user_id,credentials=excluded.credentials,command_id=excluded.command_id,command_token=excluded.command_token`, i.TeamID, i.UserID, i.Credentials, i.CommandID, i.CommandToken)
	return err
}
func scanInstallation(row interface{ Scan(...any) error }) (store.Installation, error) {
	var i store.Installation
	err := row.Scan(&i.TeamID, &i.UserID, &i.Credentials, &i.CommandID, &i.CommandToken)
	if errors.Is(err, sql.ErrNoRows) {
		err = store.ErrNotFound
	}
	return i, err
}
func (s *Store) InstallationByTeam(ctx context.Context, team string) (store.Installation, error) {
	return scanInstallation(s.db.QueryRowContext(ctx, `SELECT team_id,user_id,credentials,command_id,command_token FROM installations WHERE team_id=?`, team))
}
func (s *Store) ListInstallations(ctx context.Context) ([]store.Installation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT team_id,user_id,credentials,command_id,command_token FROM installations ORDER BY team_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []store.Installation
	for rows.Next() {
		i, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}
