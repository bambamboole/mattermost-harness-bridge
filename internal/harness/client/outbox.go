package client

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
)

// Outbox holds at-least-once envelopes until the broker acks them. It is a
// JSON file so a finished job's result survives a harness restart.
type Outbox struct {
	path string
	mu   sync.Mutex
	m    map[string]protocol.Envelope
}

func OpenOutbox(path string) (*Outbox, error) {
	o := &Outbox{path: path, m: map[string]protocol.Envelope{}}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return o, nil
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(b, &o.m); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Outbox) Add(env protocol.Envelope) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.m[env.ID] = env
	return o.flush()
}

func (o *Outbox) Ack(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.m[id]; !ok {
		return nil
	}
	delete(o.m, id)
	return o.flush()
}

// Pending returns envelopes in send order.
func (o *Outbox) Pending() []protocol.Envelope {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]protocol.Envelope, 0, len(o.m))
	for _, e := range o.m {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID }) // ULIDs sort by time
	return out
}

func (o *Outbox) flush() error {
	if err := os.MkdirAll(filepath.Dir(o.path), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(o.m)
	if err != nil {
		return err
	}
	tmp := o.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, o.path)
}
