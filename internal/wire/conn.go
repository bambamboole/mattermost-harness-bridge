// Package wire wraps a WebSocket connection so both sides exchange
// protocol.Envelope values and nothing else.
package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/coder/websocket"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
)

// MaxFrame bounds a single envelope; attachments are base64 inside it.
const MaxFrame = 12 << 20

type Conn struct {
	ws *websocket.Conn
	wm sync.Mutex
}

func New(ws *websocket.Conn) *Conn {
	ws.SetReadLimit(MaxFrame)
	return &Conn{ws: ws}
}

func (c *Conn) Send(ctx context.Context, env protocol.Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.wm.Lock()
	defer c.wm.Unlock()
	return c.ws.Write(ctx, websocket.MessageText, b)
}

func (c *Conn) Recv(ctx context.Context) (protocol.Envelope, error) {
	typ, b, err := c.ws.Read(ctx)
	if err != nil {
		return protocol.Envelope{}, err
	}
	if typ != websocket.MessageText {
		return protocol.Envelope{}, errors.New("wire: binary frame")
	}
	var env protocol.Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return protocol.Envelope{}, fmt.Errorf("wire: bad envelope: %w", err)
	}
	if env.ID == "" || env.Type == "" {
		return protocol.Envelope{}, errors.New("wire: envelope without id or type")
	}
	return env, nil
}

func (c *Conn) Close(code int, reason string) error {
	return c.ws.Close(websocket.StatusCode(code), reason)
}

// CloseStatus returns the close code of err, or -1.
func CloseStatus(err error) int {
	return int(websocket.CloseStatus(err))
}
