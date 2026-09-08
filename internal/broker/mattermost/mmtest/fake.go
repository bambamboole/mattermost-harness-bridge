// Package mmtest is an in-memory mattermost.API for tests.
package mmtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
)

type Fake struct {
	mu    sync.Mutex
	seq   int
	Posts map[string]*model.Post // id -> latest state
	Order []string               // post ids in creation order
	Users map[string]*model.User
	Files map[string][]byte
}

func New() *Fake {
	return &Fake{Posts: map[string]*model.Post{}, Users: map[string]*model.User{}, Files: map[string][]byte{}}
}

func (f *Fake) next(prefix string) string {
	f.seq++
	return fmt.Sprintf("%s%d", prefix, f.seq)
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
	f.Posts[cp.Id] = cp
	f.Order = append(f.Order, cp.Id)
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
	if len(f.Order) == 0 {
		return nil
	}
	return f.Posts[f.Order[len(f.Order)-1]].Clone()
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
