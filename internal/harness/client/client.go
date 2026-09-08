// Package client keeps the harness's outbound WebSocket to the broker alive:
// reconnect with backoff, hello/welcome, heartbeat, and at-least-once
// delivery through the outbox.
package client

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/wire"
)

// Handler receives broker messages. Handlers run on the read loop; anything
// slow must go to a goroutine. Return values become the ack.
type Handler interface {
	OnWelcome(ctx context.Context, w protocol.Welcome)
	OnDispatch(ctx context.Context, jobID string, d protocol.JobDispatch) protocol.Ack
	OnCancel(ctx context.Context, jobID string, c protocol.JobCancel) protocol.Ack
	OnApprovalResponse(ctx context.Context, jobID string, r protocol.ApprovalResponse) protocol.Ack
}

type Client struct {
	URL     string
	Token   string
	Hello   func() protocol.Hello
	Handler Handler
	Outbox  *Outbox
	Log     *slog.Logger

	// PongTimeout closes the connection when a ping goes unanswered.
	PongTimeout time.Duration
	// MaxBackoff caps the reconnect delay.
	MaxBackoff time.Duration

	mu        sync.Mutex
	conn      *wire.Conn
	connected bool
}

// session is the per-connection state; a stale goroutine from a previous
// connection never touches the current one.
type session struct {
	conn      *wire.Conn
	heartbeat time.Duration
	pongCh    chan struct{}
}

var ErrDisconnected = errors.New("client: not connected")

// Run reconnects until ctx ends. Errors from a single connection are logged,
// not returned; a permanent failure (unauthorized, version too old) ends Run.
func (c *Client) Run(ctx context.Context) error {
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.PongTimeout == 0 {
		c.PongTimeout = 10 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 60 * time.Second
	}
	backoff := time.Second
	for {
		err := c.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch wire.CloseStatus(err) {
		case protocol.CloseUnauthorized, protocol.CloseVersionTooOld:
			return err
		}
		c.Log.Warn("broker connection ended", "err", err, "retry_in", backoff)
		jitter := time.Duration(rand.Int64N(int64(backoff / 4)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
		backoff = min(backoff*2, c.MaxBackoff)
		if err == nil {
			backoff = time.Second
		}
	}
}

func (c *Client) session(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	ws, _, err := websocket.Dial(dialCtx, c.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + c.Token}},
	})
	cancel()
	if err != nil {
		return err
	}
	conn := wire.New(ws)
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	defer func() { _ = conn.Close(int(websocket.StatusNormalClosure), "bye") }()

	hello, err := protocol.New(protocol.TypeHello, "", c.Hello())
	if err != nil {
		return err
	}
	if err := conn.Send(sctx, hello); err != nil {
		return err
	}
	env, err := conn.Recv(sctx)
	if err != nil {
		return err
	}
	if env.Type != protocol.TypeWelcome {
		return errors.New("client: expected welcome, got " + env.Type)
	}
	var welcome protocol.Welcome
	if err := env.Decode(&welcome); err != nil {
		return err
	}
	s := &session{conn: conn, heartbeat: time.Duration(welcome.HeartbeatIntervalMS) * time.Millisecond, pongCh: make(chan struct{}, 1)}
	if s.heartbeat <= 0 {
		s.heartbeat = 15 * time.Second
	}
	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connected = false
		c.conn = nil
		c.mu.Unlock()
	}()
	c.Log.Info("connected to broker", "harness_id", welcome.HarnessID)
	c.Handler.OnWelcome(sctx, welcome)

	// Flush at-least-once messages that never got acked.
	for _, pending := range c.Outbox.Pending() {
		if err := conn.Send(sctx, pending); err != nil {
			return err
		}
	}

	errCh := make(chan error, 2)
	go func() { errCh <- c.heartbeatLoop(sctx, s) }()
	go func() { errCh <- c.readLoop(sctx, s) }()
	err = <-errCh
	stop()
	return err
}

func (c *Client) heartbeatLoop(ctx context.Context, s *session) error {
	t := time.NewTicker(s.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		ping, _ := protocol.New(protocol.TypePing, "", protocol.Ping{Jobs: c.jobsForPing()})
		if err := s.conn.Send(ctx, ping); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.pongCh:
		case <-time.After(c.PongTimeout):
			return errors.New("client: pong timeout")
		}
	}
}

func (c *Client) jobsForPing() int {
	if c.Hello == nil {
		return 0
	}
	return len(c.Hello().Jobs)
}

func (c *Client) readLoop(ctx context.Context, s *session) error {
	for {
		env, err := s.conn.Recv(ctx)
		if err != nil {
			return err
		}
		c.handle(ctx, s, env)
	}
}

func (c *Client) handle(ctx context.Context, s *session, env protocol.Envelope) {
	conn := s.conn
	var ack protocol.Ack
	switch env.Type {
	case protocol.TypePong:
		select {
		case s.pongCh <- struct{}{}:
		default:
		}
		return
	case protocol.TypeAck:
		if err := c.Outbox.Ack(env.Ref); err != nil {
			c.Log.Error("outbox ack", "err", err)
		}
		var a protocol.Ack
		if env.Decode(&a) == nil && !a.OK {
			c.Log.Warn("broker nacked", "ref", env.Ref, "code", a.Code, "msg", a.Message)
		}
		return
	case protocol.TypeError:
		var e protocol.Error
		_ = env.Decode(&e)
		c.Log.Warn("broker error", "ref", env.Ref, "code", e.Code, "msg", e.Message)
		return
	case protocol.TypeJobDispatch:
		var d protocol.JobDispatch
		if err := env.Decode(&d); err != nil {
			ack = protocol.Ack{OK: false, Code: protocol.NackBadPayload, Message: err.Error()}
		} else {
			ack = c.Handler.OnDispatch(ctx, env.JobID, d)
		}
	case protocol.TypeJobCancel:
		var jc protocol.JobCancel
		if err := env.Decode(&jc); err != nil {
			ack = protocol.Ack{OK: false, Code: protocol.NackBadPayload, Message: err.Error()}
		} else {
			ack = c.Handler.OnCancel(ctx, env.JobID, jc)
		}
	case protocol.TypeApprovalResponse:
		var r protocol.ApprovalResponse
		if err := env.Decode(&r); err != nil {
			ack = protocol.Ack{OK: false, Code: protocol.NackBadPayload, Message: err.Error()}
		} else {
			ack = c.Handler.OnApprovalResponse(ctx, env.JobID, r)
		}
	default:
		e, _ := protocol.Reply(protocol.TypeError, env, protocol.Error{Code: protocol.ErrUnknownType, Message: env.Type})
		_ = conn.Send(ctx, e)
		return
	}
	reply, _ := protocol.Reply(protocol.TypeAck, env, ack)
	if err := conn.Send(ctx, reply); err != nil {
		c.Log.Error("send ack", "err", err)
	}
}

// Send delivers fire-and-forget messages; it fails when disconnected.
func (c *Client) Send(ctx context.Context, env protocol.Envelope) error {
	c.mu.Lock()
	conn, ok := c.conn, c.connected
	c.mu.Unlock()
	if !ok {
		return ErrDisconnected
	}
	return conn.Send(ctx, env)
}

// SendReliable stores env in the outbox and sends it if connected. The
// outbox flush after reconnect covers the disconnected case.
func (c *Client) SendReliable(ctx context.Context, env protocol.Envelope) error {
	if !protocol.NeedsAck(env.Type) {
		return errors.New("client: SendReliable on fire-and-forget type " + env.Type)
	}
	if err := c.Outbox.Add(env); err != nil {
		return err
	}
	if err := c.Send(ctx, env); err != nil && !errors.Is(err, ErrDisconnected) {
		return err
	}
	return nil
}

func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}
