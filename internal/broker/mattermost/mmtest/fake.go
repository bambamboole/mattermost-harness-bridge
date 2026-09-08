// Package mmtest is an in-memory mattermost.API for tests.
package mmtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
)

type Fake struct {
	mu    *sync.Mutex
	seq   *int
	Posts map[string]*model.Post // id -> latest state
	Order *[]string              // post ids in creation order, shared with token views
	Users map[string]*model.User
	Files map[string][]byte
	// Admin side: bots by user id, tokens by bot, memberships as "team:user" / "channel:user".
	Bots        map[string]*model.Bot
	BotTokens   map[string]string
	Memberships map[string]bool
	Channels    map[string]*model.Channel
	// AsBot records the bot token each post was created with ("" for the shared bot).
	Token string
}

func New() *Fake {
	return &Fake{mu: &sync.Mutex{}, seq: new(int), Order: new([]string), Posts: map[string]*model.Post{}, Users: map[string]*model.User{}, Files: map[string][]byte{},
		Bots: map[string]*model.Bot{}, BotTokens: map[string]string{}, Memberships: map[string]bool{}, Channels: map[string]*model.Channel{}}
}

// WithToken returns a view that stamps posts with the given bot token; it
// shares every map with the parent, like a second client on the same server.
func (f *Fake) WithToken(token string) *Fake {
	child := *f
	child.Token = token
	return &child
}

func (f *Fake) next(prefix string) string {
	*f.seq++
	return fmt.Sprintf("%s%d", prefix, *f.seq)
}

func (f *Fake) AddUser(id, username string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Users[id] = &model.User{Id: id, Username: username}
}

func (f *Fake) CreatePost(ctx context.Context, p *model.Post) (*model.Post, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := p.Clone()
	cp.Id = f.next("post")
	if f.Token != "" {
		cp.AddProp("mmtest_token", f.Token)
	}
	f.Posts[cp.Id] = cp
	*f.Order = append(*f.Order, cp.Id)
	return cp.Clone(), nil
}

func (f *Fake) UpdatePost(ctx context.Context, id, message string, props map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.Posts[id]
	if !ok {
		return fmt.Errorf("mmtest: no post %s", id)
	}
	p.Message = message
	if props != nil {
		p.SetProps(props)
	}
	return nil
}

func (f *Fake) GetPost(ctx context.Context, id string) (*model.Post, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.Posts[id]
	if !ok {
		return nil, fmt.Errorf("mmtest: no post %s", id)
	}
	return p.Clone(), nil
}

func (f *Fake) GetThread(ctx context.Context, rootID string) ([]*model.Post, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.Post
	for _, id := range *f.Order {
		p := f.Posts[id]
		if p.Id == rootID || p.RootId == rootID {
			out = append(out, p.Clone())
		}
	}
	return out, nil
}

func (f *Fake) GetUser(ctx context.Context, id string) (*model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.Users[id]
	if !ok {
		return nil, fmt.Errorf("mmtest: no user %s", id)
	}
	return u, nil
}

func (f *Fake) DirectChannel(ctx context.Context, botID, userID string) (string, error) {
	return "dm_" + userID, nil
}

func (f *Fake) UploadFile(ctx context.Context, channelID, name string, data []byte) (*model.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.next("file")
	f.Files[id] = data
	return &model.FileInfo{Id: id, Name: name, Size: int64(len(data))}, nil
}

func (f *Fake) DownloadFile(ctx context.Context, id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.Files[id]
	if !ok {
		return nil, fmt.Errorf("mmtest: no file %s", id)
	}
	return b, nil
}

func (f *Fake) GetFileInfo(ctx context.Context, id string) (*model.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.Files[id]
	if !ok {
		return nil, fmt.Errorf("mmtest: no file %s", id)
	}
	return &model.FileInfo{Id: id, Name: id + ".txt", Size: int64(len(b)), MimeType: "text/plain"}, nil
}

// Message returns the current text of a post.
func (f *Fake) Message(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.Posts[id]; ok {
		return p.Message
	}
	return ""
}

// Last returns the most recently created post.
func (f *Fake) Last() *model.Post {
	f.mu.Lock()
	defer f.mu.Unlock()
	order := *f.Order
	if len(order) == 0 {
		return nil
	}
	return f.Posts[order[len(order)-1]].Clone()
}

// Attachments returns the message attachments of a post, if any.
func (f *Fake) Attachments(id string) []*model.MessageAttachment {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.Posts[id]
	if !ok {
		return nil
	}
	att, _ := p.GetProp(model.PostPropsAttachments).([]*model.MessageAttachment)
	return att
}

// --- admin operations -------------------------------------------------------

func (f *Fake) CreateBot(ctx context.Context, username, displayName, description string) (*model.Bot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.Users {
		if u.Username == username {
			return nil, fmt.Errorf("mmtest: username %s taken", username)
		}
	}
	id := f.next("botuser")
	f.Users[id] = &model.User{Id: id, Username: username, IsBot: true}
	b := &model.Bot{UserId: id, Username: username, DisplayName: displayName, Description: description}
	f.Bots[id] = b
	return b, nil
}

func (f *Fake) CreateBotToken(ctx context.Context, botUserID, description string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Bots[botUserID]; !ok {
		return "", fmt.Errorf("mmtest: no bot %s", botUserID)
	}
	tok := "tok_" + botUserID
	f.BotTokens[botUserID] = tok
	return tok, nil
}

func (f *Fake) AddTeamMember(ctx context.Context, teamID, userID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Memberships[teamID+":"+userID] = true
	return nil
}

func (f *Fake) AddChannelMember(ctx context.Context, channelID, userID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Memberships[channelID+":"+userID] = true
	return nil
}

func (f *Fake) GetChannel(ctx context.Context, id string) (*model.Channel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ch, ok := f.Channels[id]; ok {
		return ch, nil
	}
	return &model.Channel{Id: id, TeamId: "team_1", Type: model.ChannelTypeOpen}, nil
}

func (f *Fake) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.Users {
		if u.Username == username {
			return u, nil
		}
	}
	return nil, fmt.Errorf("mmtest: no user %s", username)
}

// PostToken returns the bot token a post was created with.
func (f *Fake) PostToken(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.Posts[id]; ok {
		t, _ := p.GetProp("mmtest_token").(string)
		return t
	}
	return ""
}
