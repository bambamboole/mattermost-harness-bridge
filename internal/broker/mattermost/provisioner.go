package mattermost

import (
	"context"

	"github.com/mattermost/mattermost/server/public/model"
)

// Provisioner is the OAuth-authorized installation's management client.
type Provisioner interface {
	Admin
	GetUser(context.Context, string) (*model.User, error)
	CreateIncomingWebhook(context.Context, *model.IncomingWebhook) (*model.IncomingWebhook, error)
	CreateOutgoingWebhook(context.Context, *model.OutgoingWebhook) (*model.OutgoingWebhook, error)
	DeleteIncomingWebhook(context.Context, string) error
	DeleteOutgoingWebhook(context.Context, string) error
	DisableBot(context.Context, string) error
}

// BotTokenIssuer uses a non-OAuth administrative session: Mattermost explicitly
// rejects OAuth sessions at POST /users/{user_id}/tokens, regardless of roles.
type BotTokenIssuer interface {
	CreateBotToken(context.Context, string, string) (string, error)
}

type tokenIssuingProvisioner struct {
	Provisioner
	issuer BotTokenIssuer
}

// WithBotTokenIssuer keeps management on the team's OAuth client and delegates
// only bot-token creation to the separately configured non-OAuth credential.
func WithBotTokenIssuer(provisioner Provisioner, issuer BotTokenIssuer) Provisioner {
	return &tokenIssuingProvisioner{Provisioner: provisioner, issuer: issuer}
}

func (p *tokenIssuingProvisioner) CreateBotToken(ctx context.Context, botUserID, description string) (string, error) {
	return p.issuer.CreateBotToken(ctx, botUserID, description)
}

var _ Provisioner = (*Client)(nil)

func (c *Client) CreateIncomingWebhook(ctx context.Context, hook *model.IncomingWebhook) (*model.IncomingWebhook, error) {
	result, _, err := c.c.CreateIncomingWebhook(ctx, hook)
	return result, wrap("create incoming webhook", err)
}
func (c *Client) CreateOutgoingWebhook(ctx context.Context, hook *model.OutgoingWebhook) (*model.OutgoingWebhook, error) {
	result, _, err := c.c.CreateOutgoingWebhook(ctx, hook)
	return result, wrap("create outgoing webhook", err)
}
func (c *Client) DeleteIncomingWebhook(ctx context.Context, id string) error {
	_, err := c.c.DeleteIncomingWebhook(ctx, id)
	return wrap("delete incoming webhook", err)
}
func (c *Client) DeleteOutgoingWebhook(ctx context.Context, id string) error {
	_, err := c.c.DeleteOutgoingWebhook(ctx, id)
	return wrap("delete outgoing webhook", err)
}
func (c *Client) DisableBot(ctx context.Context, id string) error {
	_, _, err := c.c.DisableBot(ctx, id)
	return wrap("disable bot", err)
}
