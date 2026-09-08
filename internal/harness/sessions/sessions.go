// Package sessions maps Mattermost threads to local Claude session IDs. The
// broker never sees session IDs; this file is the only place they live.
package sessions

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Entry struct {
	SessionID string    `json:"session_id"`
	Workspace string    `json:"workspace"`
	Agent     string    `json:"agent"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Map struct {
	path string
	mu   sync.Mutex
	m    map[string]Entry // root_post_id -> entry
}

func Open(path string) (*Map, error) {
	s := &Map{path: path, m: map[string]Entry{}}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(b, &s.m); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Map) Get(rootPostID string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[rootPostID]
	return e, ok
}

func (s *Map) Put(rootPostID, sessionID, workspace, agent string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[rootPostID] = Entry{SessionID: sessionID, Workspace: workspace, Agent: agent, UpdatedAt: time.Now().UTC()}
	return s.flush()
}

func (s *Map) Delete(rootPostID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, rootPostID)
	return s.flush()
}

// flush writes atomically; callers hold mu.
func (s *Map) flush() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
