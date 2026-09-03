package interactions

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	KindCommandApproval     = "command-approval"
	KindFileChangeApproval  = "file-change-approval"
	KindPermissionsApproval = "permissions-approval"
	KindUserInput           = "user-input"
	KindUnknown             = "unknown"
	maxTerminalInteractions = 256
	terminalInteractionTTL  = 5 * time.Minute
)

type FileChange struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
	Diff string `json:"diff,omitempty"`
}

type QuestionOption struct {
	Label       string `json:"label"`
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
	IsOther     bool   `json:"isOther,omitempty"`
}

type Question struct {
	ID       string           `json:"id"`
	Header   string           `json:"header,omitempty"`
	Text     string           `json:"text"`
	Type     string           `json:"type"`
	Required bool             `json:"required"`
	Options  []QuestionOption `json:"options"`
}

type PendingInteraction struct {
	ID          string       `json:"id"`
	Kind        string       `json:"kind"`
	ThreadID    string       `json:"threadId"`
	TurnID      string       `json:"turnId,omitempty"`
	ItemID      string       `json:"itemId,omitempty"`
	Title       string       `json:"title"`
	Description string       `json:"description,omitempty"`
	Command     string       `json:"command,omitempty"`
	CWD         string       `json:"cwd,omitempty"`
	FileChanges []FileChange `json:"fileChanges"`
	Questions   []Question   `json:"questions"`
	CreatedAt   string       `json:"createdAt"`
	ExpiresAt   string       `json:"expiresAt"`
	Status      string       `json:"status"`

	Method          string         `json:"-"`
	ServerRequestID string         `json:"-"`
	Raw             map[string]any `json:"-"`
	completedAt     time.Time
}

type ResponseRequest struct {
	Action  string              `json:"action"`
	Message string              `json:"message,omitempty"`
	Answers map[string][]string `json:"answers,omitempty"`
}

type Store struct {
	mu        sync.RWMutex
	items     map[string]*PendingInteraction
	byRequest map[string]string
}

func NewStore() *Store {
	return &Store{items: make(map[string]*PendingInteraction), byRequest: make(map[string]string)}
}

func (s *Store) Add(method, requestID string, params map[string]any, now time.Time) PendingInteraction {
	interaction := normalize(method, requestID, params, now)
	s.mu.Lock()
	s.pruneTerminalLocked(now)
	s.items[interaction.ID] = &interaction
	s.byRequest[requestID] = interaction.ID
	s.mu.Unlock()
	return clone(interaction)
}

func (s *Store) List(status string) []PendingInteraction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]PendingInteraction, 0, len(s.items))
	for _, item := range s.items {
		if status != "" && item.Status != status {
			continue
		}
		result = append(result, clone(*item))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt < result[j].CreatedAt })
	return result
}

func (s *Store) Get(id string) (PendingInteraction, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.items[id]
	if !ok {
		return PendingInteraction{}, false
	}
	return clone(*item), true
}

func (s *Store) BeginResponse(id string) (PendingInteraction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return PendingInteraction{}, fmt.Errorf("interaction %q was not found", id)
	}
	if item.Status != "pending" {
		return clone(*item), fmt.Errorf("interaction is already %s", item.Status)
	}
	item.Status = "responding"
	return clone(*item), nil
}

func (s *Store) RevertResponse(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item := s.items[id]; item != nil && item.Status == "responding" {
		item.Status = "pending"
	}
}

func (s *Store) Complete(id, status string) (PendingInteraction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[id]
	if !ok {
		return PendingInteraction{}, false
	}
	s.markTerminalLocked(item, status, time.Now().UTC())
	s.pruneTerminalLocked(time.Now().UTC())
	return clone(*item), true
}

func (s *Store) ResolveByRequest(requestID string) (PendingInteraction, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := []string{requestID}
	if !strings.HasPrefix(requestID, "s:") && !strings.HasPrefix(requestID, "n:") {
		keys = append(keys, "s:"+requestID, "n:"+requestID)
	}
	var id string
	for _, key := range keys {
		if found, ok := s.byRequest[key]; ok {
			id = found
			break
		}
	}
	if id == "" {
		return PendingInteraction{}, false
	}
	item := s.items[id]
	if item == nil {
		return PendingInteraction{}, false
	}
	if item.Status == "pending" || item.Status == "responding" {
		s.markTerminalLocked(item, "resolved", time.Now().UTC())
	} else {
		delete(s.byRequest, item.ServerRequestID)
	}
	s.pruneTerminalLocked(time.Now().UTC())
	return clone(*item), true
}

func (s *Store) ExpireDue(now time.Time) []PendingInteraction {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []PendingInteraction{}
	for _, item := range s.items {
		if item.Status != "pending" {
			continue
		}
		expires, err := time.Parse(time.RFC3339Nano, item.ExpiresAt)
		if err != nil || expires.After(now) {
			continue
		}
		item.Status = "expiring"
		result = append(result, clone(*item))
	}
	return result
}

func (s *Store) ExpireAll(status string) []PendingInteraction {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	result := []PendingInteraction{}
	for _, item := range s.items {
		if item.Status != "pending" && item.Status != "responding" && item.Status != "expiring" {
			continue
		}
		s.markTerminalLocked(item, status, now)
		result = append(result, clone(*item))
	}
	s.pruneTerminalLocked(now)
	return result
}

func (s *Store) ClearTurn(threadID, turnID, status string) []PendingInteraction {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []PendingInteraction{}
	for _, item := range s.items {
		if item.ThreadID != threadID || (turnID != "" && item.TurnID != turnID) {
			continue
		}
		if item.Status != "pending" && item.Status != "responding" && item.Status != "expiring" {
			continue
		}
		s.markTerminalLocked(item, status, time.Now().UTC())
		result = append(result, clone(*item))
	}
	s.pruneTerminalLocked(time.Now().UTC())
	return result
}

func (s *Store) PendingCount(threadID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, item := range s.items {
		if item.ThreadID == threadID && (item.Status == "pending" || item.Status == "responding" || item.Status == "expiring") {
			count++
		}
	}
	return count
}

func (s *Store) markTerminalLocked(item *PendingInteraction, status string, now time.Time) {
	item.Status = status
	item.completedAt = now
	// Raw contains the original app-server parameters and can include large
	// command/file payloads. It is needed only while a response is pending.
	item.Raw = nil
	delete(s.byRequest, item.ServerRequestID)
}

func (s *Store) pruneTerminalLocked(now time.Time) {
	terminalCount := 0
	for id, item := range s.items {
		if isPendingStatus(item.Status) {
			continue
		}
		terminalCount++
		if !item.completedAt.IsZero() && now.Sub(item.completedAt) >= terminalInteractionTTL {
			delete(s.items, id)
			terminalCount--
		}
	}
	for terminalCount > maxTerminalInteractions {
		oldestID := ""
		var oldestAt time.Time
		for id, item := range s.items {
			if isPendingStatus(item.Status) {
				continue
			}
			if oldestID == "" || item.completedAt.Before(oldestAt) {
				oldestID, oldestAt = id, item.completedAt
			}
		}
		if oldestID == "" {
			break
		}
		delete(s.items, oldestID)
		terminalCount--
	}
}

func isPendingStatus(status string) bool {
	return status == "pending" || status == "responding" || status == "expiring"
}

func normalize(method, requestID string, params map[string]any, now time.Time) PendingInteraction {
	lower := strings.ToLower(strings.TrimSpace(method))
	kind := KindUnknown
	title := "需要本机处理的 Codex 请求"
	switch {
	case strings.Contains(lower, "commandexecution/requestapproval"), strings.Contains(lower, "exec_command_approval"), strings.Contains(lower, "execcommandapproval"):
		kind, title = KindCommandApproval, "命令执行审批"
	case strings.Contains(lower, "filechange/requestapproval"), strings.Contains(lower, "apply_patch_approval"), strings.Contains(lower, "patchapproval"):
		kind, title = KindFileChangeApproval, "文件修改审批"
	case strings.Contains(lower, "permissions/requestapproval"), strings.Contains(lower, "permissionsapproval"):
		kind, title = KindPermissionsApproval, "权限审批"
	case strings.Contains(lower, "requestuserinput"), strings.Contains(lower, "elicitation/request"):
		kind, title = KindUserInput, "Codex 需要你的回答"
	}
	expires := now.Add(5 * time.Minute)
	if milliseconds := int64Value(params["autoResolutionMs"]); milliseconds > 0 {
		expires = now.Add(time.Duration(milliseconds) * time.Millisecond)
	}
	return PendingInteraction{
		ID: newID(), Kind: kind,
		ThreadID: nestedThreadID(params), TurnID: stringValue(params["turnId"]), ItemID: stringValue(params["itemId"]),
		Title: title, Description: firstString(params, "reason", "description", "message", "question"),
		Command: commandValue(params["command"]), CWD: stringValue(params["cwd"]),
		FileChanges: fileChanges(params), Questions: questions(params),
		CreatedAt: now.UTC().Format(time.RFC3339Nano), ExpiresAt: expires.UTC().Format(time.RFC3339Nano), Status: "pending",
		Method: method, ServerRequestID: requestID, Raw: copyMap(params),
	}
}

func questions(params map[string]any) []Question {
	rawQuestions, _ := params["questions"].([]any)
	result := make([]Question, 0, len(rawQuestions))
	for index, raw := range rawQuestions {
		value, _ := raw.(map[string]any)
		if value == nil {
			continue
		}
		options := []QuestionOption{}
		rawOptions, _ := value["options"].([]any)
		for _, rawOption := range rawOptions {
			option, _ := rawOption.(map[string]any)
			if option == nil {
				continue
			}
			label := firstString(option, "label", "text", "value")
			options = append(options, QuestionOption{
				Label: label, Value: firstNonEmpty(stringValue(option["value"]), label),
				Description: stringValue(option["description"]), IsOther: boolValue(option["isOther"]),
			})
		}
		questionType := strings.ToLower(firstString(value, "type", "kind", "selectionMode"))
		switch {
		case boolValue(value["multiple"]), boolValue(value["multiSelect"]), boolValue(value["allowsMultiple"]), strings.Contains(questionType, "multi"):
			questionType = "multiple-choice"
		case len(options) > 0:
			questionType = "single-choice"
		default:
			questionType = "text"
		}
		required := true
		if rawRequired, ok := value["required"].(bool); ok {
			required = rawRequired
		}
		result = append(result, Question{
			ID:     firstNonEmpty(stringValue(value["id"]), fmt.Sprintf("question-%d", index+1)),
			Header: stringValue(value["header"]), Text: firstNonEmpty(stringValue(value["question"]), stringValue(value["text"]), "请输入回答"),
			Type: questionType, Required: required, Options: options,
		})
	}
	if len(result) == 0 && strings.TrimSpace(firstString(params, "question", "message")) != "" {
		result = append(result, Question{ID: "question-1", Text: firstString(params, "question", "message"), Type: "text", Required: true, Options: []QuestionOption{}})
	}
	return result
}

func fileChanges(params map[string]any) []FileChange {
	raw := params["fileChanges"]
	if raw == nil {
		raw = params["changes"]
	}
	items, _ := raw.([]any)
	result := []FileChange{}
	for _, item := range items {
		value, _ := item.(map[string]any)
		if value == nil {
			continue
		}
		result = append(result, FileChange{Path: stringValue(value["path"]), Kind: stringValue(value["kind"]), Diff: stringValue(value["diff"])})
	}
	return result
}

func nestedThreadID(params map[string]any) string {
	if direct := stringValue(params["threadId"]); direct != "" {
		return direct
	}
	thread, _ := params["thread"].(map[string]any)
	if thread == nil {
		return ""
	}
	return firstNonEmpty(stringValue(thread["id"]), stringValue(thread["threadId"]))
}

func newID() string {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("interaction-%d", time.Now().UnixNano())
	}
	return "interaction-" + hex.EncodeToString(buffer)
}

func commandValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			parts = append(parts, stringValue(item))
		}
		return strings.TrimSpace(strings.Join(parts, " "))
	default:
		return ""
	}
}

func copyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func clone(value PendingInteraction) PendingInteraction {
	value.FileChanges = append([]FileChange(nil), value.FileChanges...)
	questionsCopy := make([]Question, len(value.Questions))
	copy(questionsCopy, value.Questions)
	for index := range questionsCopy {
		questionsCopy[index].Options = append([]QuestionOption(nil), questionsCopy[index].Options...)
	}
	value.Questions = questionsCopy
	value.Raw = copyMap(value.Raw)
	return value
}

func firstString(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := stringValue(value[key]); text != "" {
			return text
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int:
		return int64(typed)
	case int64:
		return typed
	default:
		return 0
	}
}
