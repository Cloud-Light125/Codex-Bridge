package conversation

import "context"

const (
	BackendCodex    = "codex"
	BackendOpenClaw = "openclaw"
)

// Session is the backend-neutral view used by the API, desktop, and channel
// adapters. Key is the opaque backend session key; Codex uses its Thread ID.
type Session struct {
	Backend      string   `json:"backend"`
	Key          string   `json:"key"`
	SessionID    string   `json:"sessionId,omitempty"`
	AgentID      string   `json:"agentId,omitempty"`
	Number       int      `json:"number,omitempty"`
	Title        string   `json:"title"`
	Summary      string   `json:"summary,omitempty"`
	CWD          string   `json:"cwd,omitempty"`
	Model        string   `json:"model,omitempty"`
	CreatedAt    string   `json:"createdAt,omitempty"`
	UpdatedAt    string   `json:"updatedAt,omitempty"`
	Status       string   `json:"status,omitempty"`
	Archived     *bool    `json:"archived,omitempty"`
	HasActiveRun bool     `json:"hasActiveRun"`
	ActiveRunIDs []string `json:"activeRunIds,omitempty"`
}

type Message struct {
	ID        string `json:"id,omitempty"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	Timestamp string `json:"timestamp,omitempty"`
	RunID     string `json:"runId,omitempty"`
}

type Detail struct {
	Session
	Messages []Message `json:"messages"`
}

type SendResult struct {
	Backend    string `json:"backend"`
	SessionKey string `json:"sessionKey"`
	RunID      string `json:"runId"`
	Status     string `json:"status,omitempty"`
	AcceptedAt string `json:"acceptedAt,omitempty"`
}

type AbortResult struct {
	Backend    string `json:"backend"`
	SessionKey string `json:"sessionKey"`
	RunID      string `json:"runId,omitempty"`
	Status     string `json:"status"`
}

// Event is normalized from a backend's native event stream. Delta and Text
// are intentionally separate: OpenClaw can send both incremental text and a
// terminal message envelope.
type Event struct {
	Backend    string         `json:"backend"`
	Type       string         `json:"type"`
	SessionKey string         `json:"sessionKey,omitempty"`
	RunID      string         `json:"runId,omitempty"`
	Delta      string         `json:"delta,omitempty"`
	Text       string         `json:"text,omitempty"`
	StopReason string         `json:"stopReason,omitempty"`
	Error      string         `json:"error,omitempty"`
	Seq        int            `json:"seq,omitempty"`
	Payload    map[string]any `json:"payload,omitempty"`
}

type ConnectionStatus struct {
	Backend            string `json:"backend"`
	Configured         bool   `json:"configured"`
	Running            bool   `json:"running"`
	Connected          bool   `json:"connected"`
	State              string `json:"state"`
	GatewayURL         string `json:"gatewayUrl"`
	Protocol           int    `json:"protocol,omitempty"`
	ServerVersion      string `json:"serverVersion,omitempty"`
	AuthMode           string `json:"authMode,omitempty"`
	SessionCount       int    `json:"sessionCount"`
	AutoReconnect      bool   `json:"autoReconnect"`
	ReconnectCount     int    `json:"reconnectCount"`
	LastConnectedAt    string `json:"lastConnectedAt,omitempty"`
	LastDisconnectedAt string `json:"lastDisconnectedAt,omitempty"`
	LastTickAt         string `json:"lastTickAt,omitempty"`
	LastError          string `json:"lastError,omitempty"`
}

// IConversationBackend is the smallest contract shared by Codex and
// OpenClaw. Backends own their transport and lifecycle; channels only route
// messages and terminal events through this interface.
type IConversationBackend interface {
	Backend() string
	ListSessions(context.Context, int) ([]Session, error)
	ReadSession(context.Context, string) (Detail, error)
	SendMessage(context.Context, string, string) (SendResult, error)
	Abort(context.Context, string, string) (AbortResult, error)
	SubscribeEvents(func(Event)) func()
	ConnectionStatus() ConnectionStatus
}
