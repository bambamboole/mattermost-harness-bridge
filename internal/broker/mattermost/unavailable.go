package mattermost

import (
	"context"
	"errors"

	"github.com/mattermost/mattermost/server/public/model"
)

var ErrBotUnavailable = errors.New("mattermost: credentials for this bot are unavailable")

// UnavailableAPI preserves the API contract when a job's bot credentials no
// longer exist. It never substitutes another bot's identity or sends a request.
type UnavailableAPI struct{}

var _ API = UnavailableAPI{}

func (UnavailableAPI) CreatePost(context.Context, *model.Post) (*model.Post, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) UpdatePost(context.Context, string, string, map[string]any) error {
	return ErrBotUnavailable
}

func (UnavailableAPI) GetPost(context.Context, string) (*model.Post, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) GetThread(context.Context, string) ([]*model.Post, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) GetUser(context.Context, string) (*model.User, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) DirectChannel(context.Context, string, string) (string, error) {
	return "", ErrBotUnavailable
}

func (UnavailableAPI) UploadFile(context.Context, string, string, []byte) (*model.FileInfo, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) DownloadFile(context.Context, string) ([]byte, error) {
	return nil, ErrBotUnavailable
}

func (UnavailableAPI) GetFileInfo(context.Context, string) (*model.FileInfo, error) {
	return nil, ErrBotUnavailable
}
