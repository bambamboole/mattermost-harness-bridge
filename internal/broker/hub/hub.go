// Package hub is the WebSocket server the harnesses connect to. It owns
// connections, authentication, heartbeats and outbox delivery; everything
// job-related is delegated to a Handler.
package hub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/wire"
)

// Handler receives harness messages. Methods run on the connection's read
// loop; the returned Ack is sent for at-least-once types.
type Handler interface {
	// OnHello decides per job the harness still runs; the hub then flushes
	// the harness's outbox.
	OnHello(ctx context.Context, h store.Harness, hello protocol.Hello) []protocol.WelcomeJob
	OnProgress(ctx context.Context, h store.Harness, jobID string, p protocol.JobProgress)
	OnResult(ctx context.Context, h store.Harness, jobID string, r protocol.JobResult) protocol.Ack
	OnApprovalRequest(ctx context.Context, h store.Harness, jobID string, r protocol.ApprovalRequest) protocol.Ack
	OnAck(ctx context.Context, h store.Harness, ref, jobID string, a protocol.Ack)
	OnDisconnect(ctx context.Context, h store.Harness)
}

type Hub struct {
	Store             store.Store
	Handler           Handler
	Log               *slog.Logger
	HeartbeatInterval time.Duration
	ReadTimeout       time.Duration
	MinHarnessVersion string

	mu    sync.Mutex
	conns map[string]*conn // harness id -> live connection
}

type conn struct {
	harness store.Harness
	c       *wire.Conn
	cancel  context.CancelFunc
}

var ErrOffline = errors.New("hub: harness offline")

func New(st store.Store, h Handler, log *slog.Logger) *Hub {
	return &Hub{
		Store: st, Handler: h, Log: log,
		HeartbeatInterval: 15 * time.Second,
		ReadTimeout:       45 * time.Second,
		conns:             map[string]*conn{},
	}
}

func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ServeHTTP upgrades /harness/v1 requests.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	harness, err := h.Store.HarnessByTokenHash(r.Context(), TokenHash(strings.TrimPrefix(auth, "Bearer ")))
	if err != nil {
		http.Error(w, "unknown token", http.StatusUnauthorized)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Harnesses are CLIs, not browsers; origin checks add nothing.
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.Log.Warn("websocket accept", "err", err)
		return
	}
	h.serve(harness, wire.New(ws))
}

func (h *Hub) serve(harness store.Harness, c *wire.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := h.Log.With("harness", harness.ID, "user", harness.MMUserID)

	rctx, rcancel := context.WithTimeout(ctx, h.ReadTimeout)
	env, err := c.Recv(rctx)
	rcancel()
	if err != nil || env.Type != protocol.TypeHello {
		_ = c.Close(protocol.CloseProtocolViolate, "expected hello")
		return
	}
	var hello protocol.Hello
	if err := env.Decode(&hello); err != nil {
		_ = c.Close(protocol.CloseProtocolViolate, "bad hello")
		return
	}
	if h.MinHarnessVersion != "" && versionLess(hello.HarnessVersion, h.MinHarnessVersion) {
		_ = c.Close(protocol.CloseVersionTooOld, "harness version "+hello.HarnessVersion+" < "+h.MinHarnessVersion)
		return
	}

	// A newer connection wins; the old one is usually a half-open socket.
	h.mu.Lock()
	if old, ok := h.conns[harness.ID]; ok {
		old.cancel()
		_ = old.c.Close(protocol.CloseReplaced, "replaced by a newer connection")
	}
	cn := &conn{harness: harness, c: c, cancel: cancel}
	h.conns[harness.ID] = cn
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if h.conns[harness.ID] == cn {
			delete(h.conns, harness.ID)
			h.mu.Unlock()
			h.Handler.OnDisconnect(context.Background(), harness)
			log.Info("harness disconnected")
			return
		}
		h.mu.Unlock()
	}()

	_ = h.Store.TouchHarness(ctx, harness.ID, time.Now(), hello.HarnessVersion)
	jobs := h.Handler.OnHello(ctx, harness, hello)
	welcome, _ := protocol.New(protocol.TypeWelcome, "", protocol.Welcome{
		HarnessID:           harness.ID,
		HeartbeatIntervalMS: int(h.HeartbeatInterval / time.Millisecond),
		ServerTime:          time.Now().UnixMilli(),
		Jobs:                jobs,
	})
	if err := c.Send(ctx, welcome); err != nil {
		return
	}
	log.Info("harness connected", "version", hello.HarnessVersion, "host", hello.Hostname, "jobs", len(hello.Jobs))

	if err := h.flushOutbox(ctx, harness.ID, c); err != nil {
		log.Warn("outbox flush", "err", err)
		return
	}

	for {
		rctx, rcancel := context.WithTimeout(ctx, h.ReadTimeout)
		env, err := c.Recv(rctx)
		rcancel()
		if err != nil {
			if ctx.Err() == nil {
				log.Debug("read ended", "err", err)
			}
			return
		}
		h.handle(ctx, cn, env)
	}
}

func (h *Hub) flushOutbox(ctx context.Context, harnessID string, c *wire.Conn) error {
	pending, err := h.Store.PendingOutbox(ctx, harnessID)
	if err != nil {
		return err
	}
	for _, m := range pending {
		env := protocol.Envelope{ID: m.ID, Type: m.Type, JobID: m.JobID, TS: time.Now().UnixMilli(), Payload: m.Payload}
		if err := c.Send(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hub) handle(ctx context.Context, cn *conn, env protocol.Envelope) {
	harness := cn.harness
	var ack protocol.Ack
	switch env.Type {
	case protocol.TypePing:
		_ = h.Store.TouchHarness(ctx, harness.ID, time.Now(), "")
		pong, _ := protocol.Reply(protocol.TypePong, env, protocol.Pong{ServerTime: time.Now().UnixMilli()})
		_ = cn.c.Send(ctx, pong)
		return
	case protocol.TypeAck:
		var a protocol.Ack
		_ = env.Decode(&a)
		if err := h.Store.AckOutbox(ctx, env.Ref, time.Now()); err != nil {
			h.Log.Error("ack outbox", "err", err)
		}
		h.Handler.OnAck(ctx, harness, env.Ref, env.JobID, a)
		return
	case protocol.TypeError:
		var e protocol.Error
		_ = env.Decode(&e)
		h.Log.Warn("harness reported error", "harness", harness.ID, "ref", env.Ref, "code", e.Code, "msg", e.Message)
		return
	case protocol.TypeJobProgress:
		var p protocol.JobProgress
		if err := env.Decode(&p); err == nil {
			h.Handler.OnProgress(ctx, harness, env.JobID, p)
		}
		return
	case protocol.TypeJobResult:
		var r protocol.JobResult
		if err := env.Decode(&r); err != nil {
			ack = protocol.Ack{OK: false, Code: protocol.NackBadPayload, Message: err.Error()}
		} else {
			ack = h.Handler.OnResult(ctx, harness, env.JobID, r)
		}
	case protocol.TypeApprovalRequest:
		var r protocol.ApprovalRequest
		if err := env.Decode(&r); err != nil {
			ack = protocol.Ack{OK: false, Code: protocol.NackBadPayload, Message: err.Error()}
		} else {
			ack = h.Handler.OnApprovalRequest(ctx, harness, env.JobID, r)
		}
	default:
		e, _ := protocol.Reply(protocol.TypeError, env, protocol.Error{Code: protocol.ErrUnknownType, Message: env.Type})
		_ = cn.c.Send(ctx, e)
		return
	}
	reply, _ := protocol.Reply(protocol.TypeAck, env, ack)
	_ = cn.c.Send(ctx, reply)
}

// Online reports whether the harness has a live connection.
func (h *Hub) Online(harnessID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.conns[harnessID]
	return ok
}

// Send delivers a fire-and-forget envelope; ErrOffline if not connected.
func (h *Hub) Send(ctx context.Context, harnessID string, env protocol.Envelope) error {
	h.mu.Lock()
	cn, ok := h.conns[harnessID]
	h.mu.Unlock()
	if !ok {
		return ErrOffline
	}
	return cn.c.Send(ctx, env)
}

// SendReliable stores env in the harness's outbox and pushes it if the
// harness is online. Delivery after a reconnect comes from the outbox.
func (h *Hub) SendReliable(ctx context.Context, harnessID string, env protocol.Envelope) error {
	if !protocol.NeedsAck(env.Type) {
		return errors.New("hub: SendReliable on fire-and-forget type " + env.Type)
	}
	err := h.Store.Enqueue(ctx, store.OutboxMessage{
		ID: env.ID, HarnessID: harnessID, JobID: env.JobID, Type: env.Type, Payload: env.Payload, CreatedAt: time.Now(),
	})
	if err != nil && !errors.Is(err, store.ErrConflict) {
		return err
	}
	if err := h.Send(ctx, harnessID, env); err != nil && !errors.Is(err, ErrOffline) {
		return err
	}
	return nil
}

// Close disconnects every harness; used on shutdown.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, cn := range h.conns {
		cn.cancel()
		_ = cn.c.Close(int(websocket.StatusGoingAway), "broker shutting down")
		delete(h.conns, id)
	}
}

// versionLess compares dotted numeric versions; anything unparsable is old.
func versionLess(a, b string) bool {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < 3; i++ {
		var x, y int
		if i < len(pa) {
			x = atoi(pa[i])
		}
		if i < len(pb) {
			y = atoi(pb[i])
		}
		if x != y {
			return x < y
		}
	}
	return false
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}
