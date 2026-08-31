package control

const (
	TurnOriginQQ       = "qqbot"
	TurnOriginTelegram = "telegram"
)

type ThreadSummary struct {
	ThreadID    string `json:"threadId"`
	Number      int    `json:"number"`
	Title       string `json:"title"`
	Summary     string `json:"summary,omitempty"`
	CWD         string `json:"cwd"`
	Model       string `json:"model"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
	Archived    *bool  `json:"archived"`
	Status      string `json:"status"`
	SessionID   string `json:"sessionId,omitempty"`
	SourceKind  string `json:"sourceKind,omitempty"`
	RolloutPath string `json:"rolloutPath,omitempty"`
	Ephemeral   bool   `json:"ephemeral"`
}

type ThreadList struct {
	Threads    []ThreadSummary `json:"threads"`
	NextCursor string          `json:"nextCursor,omitempty"`
}

type ThreadDetail struct {
	ThreadSummary
	Turns   []Turn       `json:"turns"`
	Runtime RuntimeState `json:"runtime"`
}

type RuntimeState struct {
	ThreadID                string                   `json:"threadId"`
	State                   string                   `json:"state"`
	TurnID                  string                   `json:"turnId,omitempty"`
	Origin                  string                   `json:"origin,omitempty"`
	StartedAt               string                   `json:"startedAt,omitempty"`
	LastActivityAt          string                   `json:"lastActivityAt,omitempty"`
	Error                   string                   `json:"error,omitempty"`
	CanInterrupt            bool                     `json:"canInterrupt"`
	CanSend                 bool                     `json:"canSend"`
	PendingInteractionCount int                      `json:"pendingInteractionCount"`
	Persistence             *PersistenceVerification `json:"persistence,omitempty"`
}

type StartTurnRequest struct {
	Text              string  `json:"text"`
	CWD               string  `json:"cwd,omitempty"`
	CollaborationMode string  `json:"collaborationMode"`
	Model             *string `json:"model"`
	ReasoningEffort   *string `json:"reasoningEffort"`
	Origin            string  `json:"-"`
}

type TurnAccepted struct {
	ThreadID   string `json:"threadId"`
	TurnID     string `json:"turnId"`
	Status     string `json:"status"`
	AcceptedAt string `json:"acceptedAt"`
}

type InterruptResult struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
	Status   string `json:"status"`
}

type ThreadActivity struct {
	ThreadID        string
	TurnID          string
	Status          string
	Active          bool
	WaitingApproval bool
	WaitingInput    bool
	Archived        bool
}

type Turn struct {
	TurnID    string `json:"turnId"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
	Error     string `json:"-"`
	Items     []Item `json:"items"`
}

type Item struct {
	ItemID string `json:"itemId"`
	Type   string `json:"type"`
	Role   string `json:"role,omitempty"`
	Phase  string `json:"phase,omitempty"`
	Text   string `json:"text,omitempty"`
	Label  string `json:"label,omitempty"`
	Status string `json:"status,omitempty"`
	Output string `json:"output,omitempty"`
}

// PersistenceVerification is a prompt-free, read-only comparison of the
// primary App Server, the rollout file, and a newly started App Server.
type PersistenceVerification struct {
	ThreadID       string                    `json:"threadId"`
	ExpectedTurnID string                    `json:"expectedTurnId,omitempty"`
	Status         string                    `json:"status"`
	Message        string                    `json:"message"`
	Main           ThreadPersistenceSnapshot `json:"main"`
	Rollout        RolloutVerification       `json:"rollout"`
	Probe          ThreadPersistenceSnapshot `json:"probe"`
	Environment    CodexEnvironment          `json:"environment"`
	Warnings       []string                  `json:"warnings"`
	VerifiedAt     string                    `json:"verifiedAt"`
}

type ThreadPersistenceSnapshot struct {
	ThreadID               string `json:"threadId"`
	SessionID              string `json:"sessionId,omitempty"`
	SourceKind             string `json:"sourceKind,omitempty"`
	RolloutPath            string `json:"rolloutPath,omitempty"`
	Ephemeral              bool   `json:"ephemeral"`
	CWD                    string `json:"cwd,omitempty"`
	UpdatedAt              string `json:"updatedAt,omitempty"`
	Status                 string `json:"status,omitempty"`
	TurnStatus             string `json:"turnStatus,omitempty"`
	TurnCount              int    `json:"turnCount"`
	LastTurnID             string `json:"lastTurnId,omitempty"`
	FoundTurn              bool   `json:"foundTurn"`
	UserMessageItemID      string `json:"userMessageItemId,omitempty"`
	AssistantMessageItemID string `json:"assistantMessageItemId,omitempty"`
}

type RolloutVerification struct {
	Path               string `json:"path,omitempty"`
	Exists             bool   `json:"exists"`
	BeforeLength       int64  `json:"beforeLength"`
	AfterLength        int64  `json:"afterLength"`
	LengthIncreased    bool   `json:"lengthIncreased"`
	ModifiedAfterSend  bool   `json:"modifiedAfterSend"`
	ContainsIdentifier bool   `json:"containsIdentifier"`
	Error              string `json:"error,omitempty"`
}

type CodexEnvironment struct {
	CodexCLIPath              string         `json:"codexCliPath"`
	CodexCLIVersion           string         `json:"codexCliVersion,omitempty"`
	Username                  string         `json:"username"`
	UserProfile               string         `json:"userProfile,omitempty"`
	Home                      string         `json:"home,omitempty"`
	CodexHomeExplicit         bool           `json:"codexHomeExplicit"`
	CodexHome                 string         `json:"codexHome,omitempty"`
	ResolvedCodexDataRoot     string         `json:"resolvedCodexDataRoot,omitempty"`
	AppServerWorkingDirectory string         `json:"appServerWorkingDirectory"`
	Processes                 []CodexProcess `json:"processes"`
	DesktopEnvironmentKnown   bool           `json:"desktopEnvironmentKnown"`
	MatchingRolloutPaths      []string       `json:"matchingRolloutPaths"`
	MultipleMatchingRollouts  bool           `json:"multipleMatchingRollouts"`
}

type CodexProcess struct {
	PID            int    `json:"pid"`
	Name           string `json:"name"`
	Username       string `json:"username,omitempty"`
	Version        string `json:"version,omitempty"`
	ExecutablePath string `json:"executablePath,omitempty"`
	CommandLine    string `json:"commandLine,omitempty"`
}
