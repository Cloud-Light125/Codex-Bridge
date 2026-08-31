package taskcenter

import (
	"strings"
	"time"
)

const (
	BackendCodex    = "codex"
	BackendOpenClaw = "openclaw"
)

const (
	StatusQueued       = "queued"
	StatusRouting      = "routing"
	StatusRunning      = "running"
	StatusWaitingInput = "waiting-input"
	StatusCompleted    = "completed"
	StatusFailed       = "failed"
	StatusCancelled    = "cancelled"
	StatusInterrupted  = "interrupted"
)

const (
	DispatchNotDispatched = "not-dispatched"
	Dispatching           = "dispatching"
	DispatchDispatched    = "dispatched"
)

const (
	ReuseDefault   = "default"
	ReuseLatest    = "latest"
	ReuseCreateNew = "create-new"
	ReuseFixed     = "fixed"
)

// Project is the durable workspace routing configuration. Credentials remain
// owned by the existing channel/profile and backend configuration stores.
type Project struct {
	ProjectID                 string   `json:"projectId"`
	Name                      string   `json:"name"`
	Aliases                   []string `json:"aliases"`
	WorkingDirectory          string   `json:"workingDirectory,omitempty"`
	Description               string   `json:"description,omitempty"`
	DefaultBackend            string   `json:"defaultBackend"`
	DefaultConversationNumber *int     `json:"defaultConversationNumber,omitempty"`
	AutoCreateConversation    bool     `json:"autoCreateConversation"`
	ReuseStrategy             string   `json:"reuseStrategy"`
	Enabled                   bool     `json:"enabled"`
	Tags                      []string `json:"tags,omitempty"`
	CreatedAt                 string   `json:"createdAt"`
	UpdatedAt                 string   `json:"updatedAt"`
}

// ProjectInput is used for both create and update operations. A nil default
// conversation explicitly clears the setting.
type ProjectInput struct {
	Name                      string   `json:"name"`
	Aliases                   []string `json:"aliases"`
	WorkingDirectory          string   `json:"workingDirectory"`
	Description               string   `json:"description"`
	DefaultBackend            string   `json:"defaultBackend"`
	DefaultConversationNumber *int     `json:"defaultConversationNumber"`
	AutoCreateConversation    bool     `json:"autoCreateConversation"`
	ReuseStrategy             string   `json:"reuseStrategy"`
	Enabled                   *bool    `json:"enabled"`
	Tags                      []string `json:"tags"`
}

type Task struct {
	TaskID                 string            `json:"taskId"`
	TaskNumber             int               `json:"taskNumber"`
	Title                  string            `json:"title"`
	Description            string            `json:"description"`
	ProjectID              string            `json:"projectId"`
	ProjectNameSnapshot    string            `json:"projectNameSnapshot,omitempty"`
	Backend                string            `json:"backend,omitempty"`
	ConversationNumber     int               `json:"conversationNumber,omitempty"`
	TargetID               string            `json:"targetId,omitempty"`
	Status                 string            `json:"status"`
	CreatedAt              string            `json:"createdAt"`
	StartedAt              string            `json:"startedAt,omitempty"`
	CompletedAt            string            `json:"completedAt,omitempty"`
	LastActivityAt         string            `json:"lastActivityAt"`
	CreatedFrom            string            `json:"createdFrom"`
	ChannelProfileID       string            `json:"channelProfileId,omitempty"`
	ConversationID         string            `json:"conversationId,omitempty"`
	CurrentRunID           string            `json:"currentRunId,omitempty"`
	ParentTaskNumber       int               `json:"parentTaskNumber,omitempty"`
	RetryOfTaskNumber      int               `json:"retryOfTaskNumber,omitempty"`
	ActionID               string            `json:"actionId,omitempty"`
	ActionSourceTaskNumber int               `json:"actionSourceTaskNumber,omitempty"`
	DispatchState          string            `json:"dispatchState"`
	PendingInteractionID   string            `json:"pendingInteractionId,omitempty"`
	PendingQuestion        string            `json:"pendingQuestion,omitempty"`
	ChannelType            string            `json:"channelType,omitempty"`
	ChannelAccountID       string            `json:"channelAccountId,omitempty"`
	ConversationType       string            `json:"conversationType,omitempty"`
	ChatID                 string            `json:"chatId,omitempty"`
	TopicID                string            `json:"topicId,omitempty"`
	UserID                 string            `json:"userId,omitempty"`
	Summary                TaskSummary       `json:"summary"`
	Result                 TaskResult        `json:"result"`
	LastError              string            `json:"lastError,omitempty"`
	Metadata               map[string]string `json:"metadata,omitempty"`
}

type TaskInput struct {
	Title                  string            `json:"title"`
	Description            string            `json:"description"`
	ProjectID              string            `json:"projectId"`
	Backend                string            `json:"backend"`
	ConversationNumber     int               `json:"conversationNumber"`
	TargetID               string            `json:"targetId"`
	CreatedFrom            string            `json:"createdFrom"`
	ChannelProfileID       string            `json:"channelProfileId"`
	ConversationID         string            `json:"conversationId"`
	ChannelType            string            `json:"channelType"`
	ChannelAccountID       string            `json:"channelAccountId"`
	ConversationType       string            `json:"conversationType"`
	ChatID                 string            `json:"chatId"`
	TopicID                string            `json:"topicId"`
	UserID                 string            `json:"userId"`
	ParentTaskNumber       int               `json:"parentTaskNumber"`
	RetryOfTaskNumber      int               `json:"retryOfTaskNumber"`
	ActionID               string            `json:"actionId"`
	ActionSourceTaskNumber int               `json:"actionSourceTaskNumber"`
	Metadata               map[string]string `json:"metadata"`
}

type TaskSummary struct {
	ChangedFiles []string `json:"changedFiles,omitempty"`
	BuildResults []string `json:"buildResults,omitempty"`
	TestResults  []string `json:"testResults,omitempty"`
	GitSummary   string   `json:"gitSummary,omitempty"`
	Duration     string   `json:"duration,omitempty"`
}

type TaskResult struct {
	Success      bool     `json:"success"`
	FinalText    string   `json:"finalText,omitempty"`
	ChangedFiles []string `json:"changedFiles,omitempty"`
	BuildResults []string `json:"buildResults,omitempty"`
	TestResults  []string `json:"testResults,omitempty"`
	GitSummary   string   `json:"gitSummary,omitempty"`
	Duration     string   `json:"duration,omitempty"`
}

type TaskFilter struct {
	Status string
	Search string
}

func (t Task) NumberLabel() string {
	if t.TaskNumber < 1 {
		return "T?"
	}
	return "T" + itoa(t.TaskNumber)
}

func (t Task) IsActive() bool {
	switch t.Status {
	case StatusQueued, StatusRouting, StatusRunning, StatusWaitingInput:
		return true
	default:
		return false
	}
}

func (t Task) IsTerminal() bool {
	switch t.Status {
	case StatusCompleted, StatusFailed, StatusCancelled, StatusInterrupted:
		return true
	default:
		return false
	}
}

func (t Task) Duration() time.Duration {
	start, err := time.Parse(time.RFC3339Nano, t.StartedAt)
	if err != nil {
		start, err = time.Parse(time.RFC3339Nano, t.CreatedAt)
	}
	if err != nil {
		return 0
	}
	end := time.Now().UTC()
	if t.CompletedAt != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, t.CompletedAt); parseErr == nil {
			end = parsed
		}
	}
	if end.Before(start) {
		return 0
	}
	return end.Sub(start)
}

func normalizeBackend(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func validBackend(value string) bool {
	value = normalizeBackend(value)
	return value == BackendCodex || value == BackendOpenClaw
}

func validStatus(value string) bool {
	switch value {
	case StatusQueued, StatusRouting, StatusRunning, StatusWaitingInput, StatusCompleted, StatusFailed, StatusCancelled, StatusInterrupted:
		return true
	default:
		return false
	}
}

func validDispatchState(value string) bool {
	switch value {
	case DispatchNotDispatched, Dispatching, DispatchDispatched:
		return true
	default:
		return false
	}
}

func validReuseStrategy(value string) bool {
	switch value {
	case ReuseDefault, ReuseLatest, ReuseCreateNew, ReuseFixed:
		return true
	default:
		return false
	}
}

func itoa(value int) string {
	// Task numbers are bounded by the JSON integer range in practice; keeping
	// this helper local avoids making the model depend on formatting packages.
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	buffer := make([]byte, 0, 12)
	for value > 0 {
		buffer = append(buffer, byte('0'+value%10))
		value /= 10
	}
	for left, right := 0, len(buffer)-1; left < right; left, right = left+1, right-1 {
		buffer[left], buffer[right] = buffer[right], buffer[left]
	}
	if negative {
		buffer = append([]byte{'-'}, buffer...)
	}
	return string(buffer)
}
