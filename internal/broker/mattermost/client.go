// Package mattermost adapts the official Mattermost v4 client to the small
// interface the broker core needs, so the core can be tested with a fake.
package mattermost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// MaxMessageLen is Mattermost's default post size limit.
const MaxMessageLen = model.PostMessageMaxRunesV2

// Admin contains the management operations used during onboarding. The team's
// OAuth installation manages bots and memberships; bot access-token creation
// requires a separate non-OAuth credential via WithBotTokenIssuer.
type Admin interface {
	CreateBot(ctx context.Context, username, displayName, description string) (*model.Bot, error)
	CreateBotToken(ctx context.Context, botUserID, description string) (string, error)
	AddTeamMember(ctx context.Context, teamID, userID string) error
	AddChannelMember(ctx context.Context, channelID, userID string) error
	GetChannel(ctx context.Context, id string) (*model.Channel, error)
	GetUserByUsername(ctx context.Context, username string) (*model.User, error)
}

// API is what the broker core uses. Implemented by Client; faked in tests.
type API interface {
	CreatePost(ctx context.Context, p *model.Post) (*model.Post, error)
	UpdatePost(ctx context.Context, id, message string, props map[string]any) error
	GetPost(ctx context.Context, id string) (*model.Post, error)
	// GetThread returns the root and every reply of a thread in post order.
	GetThread(ctx context.Context, rootID string) ([]*model.Post, error)
	GetUser(ctx context.Context, id string) (*model.User, error)
	DirectChannel(ctx context.Context, botID, userID string) (string, error)
	UploadFile(ctx context.Context, channelID, name string, data []byte) (*model.FileInfo, error)
	DownloadFile(ctx context.Context, id string) ([]byte, error)
	GetFileInfo(ctx context.Context, id string) (*model.FileInfo, error)
}

type Client struct {
	c       *model.Client4
	baseURL string
	token   string
}

var (
	_ API   = (*Client)(nil)
	_ Admin = (*Client)(nil)
)

func New(baseURL, token string) *Client {
	c := model.NewAPIv4Client(strings.TrimRight(baseURL, "/"))
	c.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	c.SetToken(token)
	return &Client{c: c, baseURL: strings.TrimRight(baseURL, "/"), token: token}
}

func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("mattermost: %s: %w", op, err)
}

func (c *Client) Me(ctx context.Context) (*model.User, error) {
	u, _, err := c.c.GetMe(ctx, "")
	return u, wrap("get me", err)
}

func (c *Client) GetUser(ctx context.Context, id string) (*model.User, error) {
	u, _, err := c.c.GetUser(ctx, id, "")
	return u, wrap("get user", err)
}

func (c *Client) GetPost(ctx context.Context, id string) (*model.Post, error) {
	p, _, err := c.c.GetPost(ctx, id, "")
	return p, wrap("get post", err)
}

func (c *Client) GetThread(ctx context.Context, rootID string) ([]*model.Post, error) {
	list, _, err := c.c.GetPostThread(ctx, rootID, "", false)
	if err != nil {
		return nil, wrap("get thread", err)
	}
	posts := make([]*model.Post, 0, len(list.Order))
	for _, id := range list.Order {
		if p := list.Posts[id]; p != nil {
			posts = append(posts, p)
		}
	}
	// Order is newest first; callers want the conversation top to bottom.
	sort.Slice(posts, func(i, j int) bool { return posts[i].CreateAt < posts[j].CreateAt })
	return posts, nil
}

func (c *Client) CreatePost(ctx context.Context, p *model.Post) (*model.Post, error) {
	out, _, err := c.c.CreatePost(ctx, p)
	return out, wrap("create post", err)
}

func (c *Client) UpdatePost(ctx context.Context, id, message string, props map[string]any) error {
	patch := &model.PostPatch{Message: &message}
	if props != nil {
		si := model.StringInterface(props)
		patch.Props = &si
	}
	_, _, err := c.c.PatchPost(ctx, id, patch)
	return wrap("patch post", err)
}

func (c *Client) DirectChannel(ctx context.Context, botID, userID string) (string, error) {
	ch, _, err := c.c.CreateDirectChannel(ctx, botID, userID)
	if err != nil {
		return "", wrap("direct channel", err)
	}
	return ch.Id, nil
}

func (c *Client) UploadFile(ctx context.Context, channelID, name string, data []byte) (*model.FileInfo, error) {
	res, _, err := c.c.UploadFile(ctx, data, channelID, name)
	if err != nil {
		return nil, wrap("upload file", err)
	}
	if len(res.FileInfos) == 0 {
		return nil, errors.New("mattermost: upload returned no file info")
	}
	return res.FileInfos[0], nil
}

func (c *Client) DownloadFile(ctx context.Context, id string) ([]byte, error) {
	b, _, err := c.c.GetFile(ctx, id)
	return b, wrap("get file", err)
}

func (c *Client) GetFileInfo(ctx context.Context, id string) (*model.FileInfo, error) {
	fi, _, err := c.c.GetFileInfo(ctx, id)
	return fi, wrap("get file info", err)
}

// --- admin operations -------------------------------------------------------

func (c *Client) CreateBot(ctx context.Context, username, displayName, description string) (*model.Bot, error) {
	b, _, err := c.c.CreateBot(ctx, &model.Bot{Username: username, DisplayName: displayName, Description: description})
	return b, wrap("create bot", err)
}

func (c *Client) CreateBotToken(ctx context.Context, botUserID, description string) (string, error) {
	t, _, err := c.c.CreateUserAccessToken(ctx, botUserID, description, 0)
	if err != nil {
		return "", wrap("create bot token", err)
	}
	return t.Token, nil
}

func (c *Client) AddTeamMember(ctx context.Context, teamID, userID string) error {
	_, _, err := c.c.AddTeamMember(ctx, teamID, userID)
	return wrap("add team member", err)
}

func (c *Client) AddChannelMember(ctx context.Context, channelID, userID string) error {
	_, _, err := c.c.AddChannelMember(ctx, channelID, userID)
	return wrap("add channel member", err)
}

func (c *Client) GetChannel(ctx context.Context, id string) (*model.Channel, error) {
	ch, _, err := c.c.GetChannel(ctx, id)
	return ch, wrap("get channel", err)
}

func (c *Client) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	u, _, err := c.c.GetUserByUsername(ctx, username, "")
	return u, wrap("get user by username", err)
}

// PostedEvent is a new post as delivered over the WebSocket.
type PostedEvent struct {
	Post        *model.Post
	ChannelType string // D (direct), O (open), P (private), G (group)
	SenderName  string
	Mentions    []string // user ids mentioned in the post
}

// Listen consumes the event stream and calls onPost for every `posted`
// event. It reconnects with backoff until ctx ends.
func (c *Client) Listen(ctx context.Context, log *slog.Logger, onPost func(context.Context, PostedEvent)) error {
	wsURL := c.baseURL
	switch {
	case strings.HasPrefix(wsURL, "https://"):
		wsURL = "wss://" + strings.TrimPrefix(wsURL, "https://")
	case strings.HasPrefix(wsURL, "http://"):
		wsURL = "ws://" + strings.TrimPrefix(wsURL, "http://")
	}
	backoff := time.Second
	for {
		err := c.listenOnce(ctx, wsURL, log, onPost)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("mattermost websocket ended", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + time.Duration(rand.Int64N(int64(backoff/4)))):
		}
		backoff = min(backoff*2, 60*time.Second)
	}
}

func (c *Client) listenOnce(ctx context.Context, wsURL string, log *slog.Logger, onPost func(context.Context, PostedEvent)) error {
	ws, err := model.NewWebSocketClient4(wsURL, c.token)
	if err != nil {
		return err
	}
	defer ws.Close()
	ws.Listen()
	log.Info("mattermost websocket connected")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ws.PingTimeoutChannel:
			return errors.New("ping timeout")
		case <-ws.ResponseChannel:
			// Replies to our own requests (none after auth); must be drained.
		case ev, ok := <-ws.EventChannel:
			if !ok {
				if ws.ListenError != nil {
					return ws.ListenError
				}
				return errors.New("event channel closed")
			}
			if ev.EventType() != model.WebsocketEventPosted {
				continue
			}
			data := ev.GetData()
			raw, _ := data["post"].(string)
			var p model.Post
			if err := json.Unmarshal([]byte(raw), &p); err != nil {
				log.Warn("bad posted event", "err", err)
				continue
			}
			ct, _ := data["channel_type"].(string)
			sn, _ := data["sender_name"].(string)
			var mentions []string
			if m, _ := data["mentions"].(string); m != "" {
				_ = json.Unmarshal([]byte(m), &mentions)
			}
			onPost(ctx, PostedEvent{Post: &p, ChannelType: ct, SenderName: sn, Mentions: mentions})
		}
	}
}
