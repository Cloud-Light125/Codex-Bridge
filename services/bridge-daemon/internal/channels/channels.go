package channels

import (
	"context"
	"time"
)

// ChannelAddress is a platform-neutral destination. TopicID is empty for
// channels which do not support topics/forums.
type ChannelAddress struct {
	ChannelType string `json:"channelType"`
	// ChannelProfileID identifies the Bridge profile which owns this
	// transport.  AccountID is still the provider's bot/App ID; it cannot be
	// used as a profile key because several logical backends may share one
	// Telegram token or QQ application.
	ChannelProfileID string `json:"channelProfileId,omitempty"`
	AccountID        string `json:"accountId"`
	ConversationType string `json:"conversationType"`
	ChatID           string `json:"chatId"`
	TopicID          string `json:"topicId,omitempty"`
	UserID           string `json:"userId,omitempty"`
}

type InboundMessage struct {
	Address     ChannelAddress
	UpdateID    int64
	MessageID   string
	CallbackID  string
	UserID      string
	UserName    string
	Text        string
	Action      string
	Unsupported bool
	Received    time.Time
}

type Button struct {
	Label string
	Value string
}

type ActionRow struct {
	Buttons []Button
}

type OutboundMessage struct {
	Address   ChannelAddress
	Text      string
	ParseMode string
	Silent    bool
	Actions   []ActionRow
}

type OutboundResult struct {
	MessageID string
}

type Status struct {
	Type      string `json:"type"`
	Running   bool   `json:"running"`
	State     string `json:"state"`
	LastError string `json:"lastError,omitempty"`
}

type ChannelAdapter interface {
	Type() string
	Start(context.Context) error
	Stop(context.Context) error
	Status() Status
	SendMessage(context.Context, OutboundMessage) (OutboundResult, error)
	EditMessage(context.Context, string, OutboundMessage) (OutboundResult, error)
}
