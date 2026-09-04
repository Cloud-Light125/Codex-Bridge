package taskcenter

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
)

// AdapterCapabilities describe only capabilities which the task center can
// safely rely on. A missing capability must result in a user-visible error,
// not a guessed protocol call.
type AdapterCapabilities struct {
	CanCreateConversation    bool `json:"canCreateConversation"`
	CanStop                  bool `json:"canStop"`
	CanContinue              bool `json:"canContinue"`
	CanReportWaitingInput    bool `json:"canReportWaitingInput"`
	CanQueryRunState         bool `json:"canQueryRunState"`
	SupportsGitActions       bool `json:"supportsGitActions"`
	SupportsTestAction       bool `json:"supportsTestAction"`
	SupportsContinue         bool `json:"supportsContinue"`
	SupportsRetry            bool `json:"supportsRetry"`
	SupportsCancel           bool `json:"supportsCancel"`
	SupportsOpenConversation bool `json:"supportsOpenConversation"`
}

func (c AdapterCapabilities) SupportsContinueAction() bool {
	return c.SupportsContinue || c.CanContinue
}

func (c AdapterCapabilities) SupportsRetryAction() bool {
	// Retry has always been implemented by Task Center on top of the existing
	// send/resolve adapter path. Keep old adapters compatible while exposing an
	// explicit capability for new adapters.
	return c.SupportsRetry || c.SupportsContinueAction()
}

func (c AdapterCapabilities) SupportsCancelAction() bool { return c.SupportsCancel || c.CanStop }

func (c AdapterCapabilities) SupportsOpenConversationAction() bool {
	return c.SupportsOpenConversation
}

type ConversationRef struct {
	Backend      string
	Number       int
	TargetID     string
	Title        string
	Summary      string
	WorkingDir   string
	UpdatedAt    string
	Status       string
	Archived     bool
	HasActiveRun bool
	ActiveRunID  string
}

type RunState struct {
	Exists       bool
	Active       bool
	WaitingInput bool
	RunID        string
	State        string
	Error        string
}

type TaskBackendAdapter interface {
	Backend() string
	Capabilities() AdapterCapabilities
	ListConversations(context.Context, int) ([]ConversationRef, error)
	ResolveConversation(context.Context, int, string) (ConversationRef, error)
	SendTask(context.Context, ConversationRef, string, string, string) (conversation.SendResult, error)
	CancelTask(context.Context, ConversationRef, string) (conversation.AbortResult, error)
	QueryRunState(context.Context, ConversationRef, string) (RunState, error)
}

// FinalAnswerReader is the optional, protocol-aware completion read path.
// Backends that expose a multi-item history must use it instead of guessing
// from the last assistant item.
type FinalAnswerReader interface {
	ReadFinalAnswer(context.Context, string, string) (control.Item, bool, error)
}

type CodexTaskAdapter struct {
	control  codexTaskControl
	runtime  codexTaskRuntime
	registry any
}

type codexTaskControl interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

type codexTaskRuntime interface {
	RuntimeState(string) control.RuntimeState
	StartTurn(context.Context, string, control.StartTurnRequest) (control.TurnAccepted, error)
	InterruptTurn(context.Context, string, string) (control.InterruptResult, error)
}

func NewCodexTaskAdapter(reader codexTaskControl, runtime codexTaskRuntime, registry any) *CodexTaskAdapter {
	return &CodexTaskAdapter{control: reader, runtime: runtime, registry: registry}
}

func (a *CodexTaskAdapter) Backend() string { return BackendCodex }

func (a *CodexTaskAdapter) Ready() bool {
	provider, ok := a.runtime.(interface{ Status() bridgeruntime.Status })
	return ok && provider.Status().AppServerRunning
}

func (a *CodexTaskAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{
		CanCreateConversation:    false,
		CanStop:                  true,
		CanContinue:              true,
		CanReportWaitingInput:    true,
		CanQueryRunState:         true,
		SupportsGitActions:       true,
		SupportsTestAction:       true,
		SupportsContinue:         true,
		SupportsRetry:            true,
		SupportsCancel:           true,
		SupportsOpenConversation: true,
	}
}

func (a *CodexTaskAdapter) ListConversations(ctx context.Context, limit int) ([]ConversationRef, error) {
	if a.control == nil {
		return nil, errors.New("Codex backend is unavailable")
	}
	if limit < 1 || limit > 200 {
		limit = 100
	}
	threads, err := a.control.ListThreads(ctx, limit, "")
	if err != nil {
		return nil, err
	}
	result := make([]ConversationRef, 0, len(threads.Threads))
	for _, thread := range threads.Threads {
		ref := ConversationRef{
			Backend: BackendCodex, Number: thread.Number, TargetID: thread.ThreadID,
			Title: thread.Title, Summary: thread.Summary, WorkingDir: thread.CWD,
			UpdatedAt: thread.UpdatedAt, Status: thread.Status,
		}
		if thread.Archived != nil {
			ref.Archived = *thread.Archived
		}
		if a.runtime != nil {
			state := a.runtime.RuntimeState(thread.ThreadID)
			ref.Status = firstNonEmpty(state.State, ref.Status)
			ref.HasActiveRun = codexStateActive(state.State) || state.PendingInteractionCount > 0
			if ref.HasActiveRun {
				ref.ActiveRunID = state.TurnID
			}
		}
		result = append(result, ref)
	}
	return result, nil
}

func (a *CodexTaskAdapter) ResolveConversation(ctx context.Context, number int, targetID string) (ConversationRef, error) {
	if a.control == nil {
		return ConversationRef{}, errors.New("Codex backend is unavailable")
	}
	targetID = strings.TrimSpace(targetID)
	if number > 0 {
		store := conversationregistry.ForAny(a.registry)
		if store == nil {
			return ConversationRef{}, errors.New("全局 Conversation Registry 尚未初始化")
		}
		record, ok := store.ByNumber(number)
		if !ok {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 不存在", number)
		}
		if record.Backend != BackendCodex {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 属于 %s，不是 Codex", number, displayBackend(record.Backend))
		}
		if targetID != "" && targetID != record.TargetID {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 与 TargetID 不匹配", number)
		}
		targetID = record.TargetID
	}
	if targetID == "" {
		return ConversationRef{}, errors.New("Codex Conversation 未指定")
	}
	detail, err := a.control.ReadThread(ctx, targetID, false)
	if err != nil {
		return ConversationRef{}, err
	}
	if detail.ThreadID == "" || detail.ThreadID != targetID {
		return ConversationRef{}, errors.New("指定的 Codex Thread 不存在")
	}
	ref := ConversationRef{
		Backend: BackendCodex, Number: detail.Number, TargetID: detail.ThreadID,
		Title: detail.Title, Summary: detail.Summary, WorkingDir: detail.CWD,
		UpdatedAt: detail.UpdatedAt, Status: detail.Runtime.State,
	}
	if detail.Archived != nil {
		ref.Archived = *detail.Archived
	}
	if store := conversationregistry.ForAny(a.registry); store != nil {
		if record, ok := store.ByTarget(BackendCodex, targetID); ok {
			ref.Number = record.Number
		}
	}
	if a.runtime != nil {
		state := a.runtime.RuntimeState(targetID)
		ref.Status = firstNonEmpty(state.State, ref.Status)
		ref.HasActiveRun = codexStateActive(state.State) || state.PendingInteractionCount > 0
		if ref.HasActiveRun {
			ref.ActiveRunID = state.TurnID
		}
	}
	return ref, nil
}

func (a *CodexTaskAdapter) SendTask(ctx context.Context, ref ConversationRef, text, cwd, origin string) (conversation.SendResult, error) {
	if a.runtime == nil {
		return conversation.SendResult{}, errors.New("Codex backend is unavailable")
	}
	accepted, err := a.runtime.StartTurn(ctx, strings.TrimSpace(ref.TargetID), control.StartTurnRequest{
		Text: text, CWD: strings.TrimSpace(cwd), CollaborationMode: "default", Origin: origin,
	})
	if err != nil {
		return conversation.SendResult{}, err
	}
	return conversation.SendResult{Backend: BackendCodex, SessionKey: accepted.ThreadID, RunID: accepted.TurnID, Status: accepted.Status, AcceptedAt: accepted.AcceptedAt}, nil
}

func (a *CodexTaskAdapter) CancelTask(ctx context.Context, ref ConversationRef, runID string) (conversation.AbortResult, error) {
	if a.runtime == nil {
		return conversation.AbortResult{}, errors.New("Codex backend is unavailable")
	}
	result, err := a.runtime.InterruptTurn(ctx, strings.TrimSpace(ref.TargetID), strings.TrimSpace(runID))
	if err != nil {
		return conversation.AbortResult{}, err
	}
	return conversation.AbortResult{Backend: BackendCodex, SessionKey: result.ThreadID, RunID: result.TurnID, Status: result.Status}, nil
}

func (a *CodexTaskAdapter) QueryRunState(ctx context.Context, ref ConversationRef, runID string) (RunState, error) {
	if a.control == nil {
		return RunState{}, errors.New("Codex backend is unavailable")
	}
	detail, err := control.ReadThreadActivity(ctx, a.control, strings.TrimSpace(ref.TargetID))
	if err != nil {
		return RunState{}, err
	}
	if detail.ThreadID == "" {
		return RunState{}, errors.New("指定的 Codex Thread 不存在")
	}
	state := detail.Runtime
	if a.runtime != nil {
		state = a.runtime.RuntimeState(detail.ThreadID)
	}
	requestedRunID := strings.TrimSpace(runID)
	if requestedRunID != "" && state.TurnID != "" && state.TurnID != requestedRunID {
		// A newer or unrelated turn must never be adopted by an older Task.
		return RunState{Exists: true, RunID: state.TurnID, State: "run-mismatch"}, nil
	}
	turn := matchingCodexTurn(detail.Turns, requestedRunID)
	if turn != nil {
		turnStatus := strings.ToLower(strings.TrimSpace(turn.Status))
		if codexTurnWaiting(turnStatus) {
			state.State = bridgeruntime.StateWaitingUserInput
		} else if codexTurnActive(turnStatus) && !codexStateActive(state.State) {
			state.State = bridgeruntime.StateRunningExternal
		}
		if state.Error == "" {
			state.Error = turn.Error
		}
	}
	threadStatus := strings.ToLower(strings.TrimSpace(detail.Status))
	if codexThreadWaiting(threadStatus) {
		state.State = bridgeruntime.StateWaitingUserInput
	} else if codexTurnActive(threadStatus) && !codexStateActive(state.State) {
		state.State = bridgeruntime.StateRunningExternal
	}
	if state.TurnID == "" {
		if turn != nil {
			state.TurnID = turn.TurnID
		} else {
			state.TurnID = requestedRunID
		}
	}
	result := RunState{
		Exists: true, Active: codexStateActive(state.State) || state.PendingInteractionCount > 0,
		WaitingInput: state.State == bridgeruntime.StateWaitingUserInput || state.PendingInteractionCount > 0,
		RunID:        state.TurnID, State: state.State, Error: state.Error,
	}
	return result, nil
}

func (a *CodexTaskAdapter) ReadFinalAnswer(ctx context.Context, threadID, turnID string) (control.Item, bool, error) {
	if a.control == nil {
		return control.Item{}, false, errors.New("Codex backend is unavailable")
	}
	threadID, turnID = strings.TrimSpace(threadID), strings.TrimSpace(turnID)
	if threadID == "" || turnID == "" {
		return control.Item{}, false, errors.New("Codex final answer requires a Thread and Turn")
	}
	detail, err := control.ReadThreadHistory(ctx, a.control, threadID, control.DefaultHistoryTurnLimit)
	if err != nil {
		return control.Item{}, false, err
	}
	mode := control.FinalSelectionModeForHistory(detail.HistoryMode)
	for _, turn := range detail.Turns {
		if turn.TurnID != turnID {
			continue
		}
		item, ok := control.SelectFinalAssistantItem(turn, mode)
		return item, ok, nil
	}
	return control.Item{}, false, nil
}

func matchingCodexTurn(turns []control.Turn, runID string) *control.Turn {
	if len(turns) == 0 {
		return nil
	}
	if runID != "" {
		for index := range turns {
			if turns[index].TurnID == runID {
				return &turns[index]
			}
		}
		return nil
	}
	return &turns[len(turns)-1]
}

func codexTurnWaiting(status string) bool {
	return strings.Contains(status, "waiting") || strings.Contains(status, "input")
}

func codexTurnActive(status string) bool {
	return strings.HasPrefix(status, "active") || strings.Contains(status, "inprogress") || strings.Contains(status, "running") || codexTurnWaiting(status)
}

func codexThreadWaiting(status string) bool {
	return strings.Contains(status, "waitingoninput") || strings.Contains(status, "waitingonuserinput") || codexTurnWaiting(status) && strings.Contains(status, "active")
}

type OpenClawTaskAdapter struct {
	backend  conversation.IConversationBackend
	registry any
}

func NewOpenClawTaskAdapter(backend conversation.IConversationBackend, registry any) *OpenClawTaskAdapter {
	return &OpenClawTaskAdapter{backend: backend, registry: registry}
}

func (a *OpenClawTaskAdapter) Backend() string { return BackendOpenClaw }

func (a *OpenClawTaskAdapter) Ready() bool {
	return a.backend != nil && a.backend.ConnectionStatus().Connected
}

func (a *OpenClawTaskAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{
		CanCreateConversation:    false,
		CanStop:                  true,
		CanContinue:              true,
		CanReportWaitingInput:    false,
		CanQueryRunState:         true,
		SupportsContinue:         true,
		SupportsRetry:            true,
		SupportsCancel:           true,
		SupportsOpenConversation: true,
	}
}

func (a *OpenClawTaskAdapter) ListConversations(ctx context.Context, limit int) ([]ConversationRef, error) {
	if a.backend == nil {
		return nil, errors.New("OpenClaw backend is unavailable")
	}
	sessions, err := a.backend.ListSessions(ctx, limit)
	if err != nil {
		return nil, err
	}
	result := make([]ConversationRef, 0, len(sessions))
	for _, session := range sessions {
		result = append(result, openClawRef(session))
	}
	return result, nil
}

func (a *OpenClawTaskAdapter) ResolveConversation(ctx context.Context, number int, targetID string) (ConversationRef, error) {
	if a.backend == nil {
		return ConversationRef{}, errors.New("OpenClaw backend is unavailable")
	}
	targetID = strings.TrimSpace(targetID)
	if number > 0 {
		store := conversationregistry.ForAny(a.registry)
		if store == nil {
			return ConversationRef{}, errors.New("全局 Conversation Registry 尚未初始化")
		}
		record, ok := store.ByNumber(number)
		if !ok {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 不存在", number)
		}
		if record.Backend != BackendOpenClaw {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 属于 %s，不是 OpenClaw", number, displayBackend(record.Backend))
		}
		if targetID != "" && targetID != record.TargetID {
			return ConversationRef{}, fmt.Errorf("Conversation #%d 与 TargetID 不匹配", number)
		}
		targetID = record.TargetID
	}
	if targetID == "" {
		return ConversationRef{}, errors.New("OpenClaw Conversation 未指定")
	}
	detail, err := a.backend.ReadSession(ctx, targetID)
	if err != nil {
		return ConversationRef{}, err
	}
	ref := openClawRef(detail.Session)
	if ref.TargetID == "" {
		return ConversationRef{}, errors.New("指定的 OpenClaw Session 不存在")
	}
	if store := conversationregistry.ForAny(a.registry); store != nil {
		if record, ok := store.ByTarget(BackendOpenClaw, targetID); ok {
			ref.Number = record.Number
		}
	}
	return ref, nil
}

func (a *OpenClawTaskAdapter) SendTask(ctx context.Context, ref ConversationRef, text, _, _ string) (conversation.SendResult, error) {
	if a.backend == nil {
		return conversation.SendResult{}, errors.New("OpenClaw backend is unavailable")
	}
	return a.backend.SendMessage(ctx, strings.TrimSpace(ref.TargetID), text)
}

func (a *OpenClawTaskAdapter) CancelTask(ctx context.Context, ref ConversationRef, runID string) (conversation.AbortResult, error) {
	if a.backend == nil {
		return conversation.AbortResult{}, errors.New("OpenClaw backend is unavailable")
	}
	return a.backend.Abort(ctx, strings.TrimSpace(ref.TargetID), strings.TrimSpace(runID))
}

func (a *OpenClawTaskAdapter) QueryRunState(ctx context.Context, ref ConversationRef, runID string) (RunState, error) {
	if a.backend == nil {
		return RunState{}, errors.New("OpenClaw backend is unavailable")
	}
	detail, err := a.backend.ReadSession(ctx, strings.TrimSpace(ref.TargetID))
	if err != nil {
		return RunState{}, err
	}
	requestedRunID := strings.TrimSpace(runID)
	actualRunID := firstString(detail.ActiveRunIDs)
	if requestedRunID != "" && len(detail.ActiveRunIDs) > 0 && !containsString(detail.ActiveRunIDs, requestedRunID) {
		return RunState{Exists: detail.Key != "", RunID: actualRunID, State: "run-mismatch"}, nil
	}
	actualRunID = firstNonEmpty(requestedRunID, actualRunID)
	state := strings.ToLower(strings.TrimSpace(detail.Status))
	waiting := strings.Contains(state, "wait") || strings.Contains(state, "input")
	return RunState{Exists: detail.Key != "", Active: detail.HasActiveRun, WaitingInput: waiting, RunID: actualRunID, State: state}, nil
}

func openClawRef(session conversation.Session) ConversationRef {
	return ConversationRef{
		Backend: BackendOpenClaw, Number: session.Number, TargetID: session.Key,
		Title: session.Title, Summary: session.Summary, WorkingDir: session.CWD,
		UpdatedAt: session.UpdatedAt, Status: session.Status, Archived: session.Archived != nil && *session.Archived,
		HasActiveRun: session.HasActiveRun, ActiveRunID: firstString(session.ActiveRunIDs),
	}
}

func codexStateActive(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case bridgeruntime.StateAccepted, bridgeruntime.StateRunning, bridgeruntime.StateRunningExternal,
		bridgeruntime.StateWaitingApproval, bridgeruntime.StateWaitingUserInput, bridgeruntime.StateInterrupting,
		bridgeruntime.StateCompletedUnverified:
		return true
	default:
		return false
	}
}

func displayBackend(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), BackendCodex) {
		return "Codex"
	}
	if strings.EqualFold(strings.TrimSpace(value), BackendOpenClaw) {
		return "OpenClaw"
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func containsString(values []string, wanted string) bool {
	wanted = strings.TrimSpace(wanted)
	for _, value := range values {
		if strings.TrimSpace(value) == wanted {
			return true
		}
	}
	return false
}
