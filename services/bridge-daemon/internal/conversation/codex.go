package conversation

import (
	"context"
	"errors"
	"strings"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
)

// CodexBackend adapts the existing Codex runtime without changing its
// process, persistence, or approval implementation. It is deliberately a
// thin adapter so the OpenClaw implementation cannot regress Codex behavior.
type CodexBackend struct {
	control ControlReader
	runtime RuntimeController
	broker  *events.Broker
}

type ControlReader interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

type RuntimeController interface {
	Status() bridgeruntime.Status
	RuntimeState(string) control.RuntimeState
	StartTurn(context.Context, string, control.StartTurnRequest) (control.TurnAccepted, error)
	InterruptTurn(context.Context, string, string) (control.InterruptResult, error)
}

func NewCodexBackend(reader ControlReader, runtime RuntimeController, broker *events.Broker) *CodexBackend {
	return &CodexBackend{control: reader, runtime: runtime, broker: broker}
}

func (b *CodexBackend) Backend() string { return BackendCodex }

func (b *CodexBackend) ListSessions(ctx context.Context, limit int) ([]Session, error) {
	if b.control == nil {
		return nil, errors.New("Codex backend is unavailable")
	}
	list, err := b.control.ListThreads(ctx, limit, "")
	if err != nil {
		return nil, err
	}
	result := make([]Session, 0, len(list.Threads))
	for _, thread := range list.Threads {
		state := ""
		if b.runtime != nil {
			state = b.runtime.RuntimeState(thread.ThreadID).State
		}
		if state == "" {
			state = thread.Status
		}
		result = append(result, Session{
			Backend: BackendCodex, Key: thread.ThreadID, SessionID: thread.SessionID,
			Number: thread.Number, Title: thread.Title, Summary: thread.Summary,
			CWD: thread.CWD, Model: thread.Model, CreatedAt: thread.CreatedAt,
			UpdatedAt: thread.UpdatedAt, Status: state, Archived: thread.Archived,
			HasActiveRun: b.runtime != nil && b.runtime.RuntimeState(thread.ThreadID).CanInterrupt,
		})
	}
	return result, nil
}

func (b *CodexBackend) ReadSession(ctx context.Context, key string) (Detail, error) {
	if b.control == nil {
		return Detail{}, errors.New("Codex backend is unavailable")
	}
	detail, err := control.ReadThreadHistory(ctx, b.control, strings.TrimSpace(key), control.DefaultHistoryTurnLimit)
	if err != nil {
		return Detail{}, err
	}
	state := ""
	if b.runtime != nil {
		state = b.runtime.RuntimeState(detail.ThreadID).State
	}
	if state == "" {
		state = detail.Status
	}
	result := Detail{Session: Session{
		Backend: BackendCodex, Key: detail.ThreadID, SessionID: detail.SessionID,
		Number: detail.Number, Title: detail.Title, Summary: detail.Summary, CWD: detail.CWD,
		Model: detail.Model, CreatedAt: detail.CreatedAt, UpdatedAt: detail.UpdatedAt,
		Status: state, Archived: detail.Archived,
	}, Messages: []Message{}}
	for _, turn := range detail.Turns {
		for _, item := range turn.Items {
			if item.Type != "userMessage" && item.Type != "agentMessage" {
				continue
			}
			role := "assistant"
			if item.Type == "userMessage" {
				role = "user"
			}
			result.Messages = append(result.Messages, Message{ID: item.ItemID, Role: role, Text: item.Text, Timestamp: turn.UpdatedAt, RunID: turn.TurnID})
		}
	}
	if b.runtime != nil {
		runtimeState := b.runtime.RuntimeState(detail.ThreadID)
		result.HasActiveRun = runtimeState.CanInterrupt
		if runtimeState.TurnID != "" {
			result.ActiveRunIDs = []string{runtimeState.TurnID}
		}
	}
	return result, nil
}

func (b *CodexBackend) SendMessage(ctx context.Context, key, message string) (SendResult, error) {
	if b.runtime == nil {
		return SendResult{}, errors.New("Codex backend is unavailable")
	}
	accepted, err := b.runtime.StartTurn(ctx, strings.TrimSpace(key), control.StartTurnRequest{Text: message, Origin: "conversation-backend"})
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{Backend: BackendCodex, SessionKey: accepted.ThreadID, RunID: accepted.TurnID, Status: accepted.Status, AcceptedAt: accepted.AcceptedAt}, nil
}

func (b *CodexBackend) Abort(ctx context.Context, key, runID string) (AbortResult, error) {
	if b.runtime == nil {
		return AbortResult{}, errors.New("Codex backend is unavailable")
	}
	result, err := b.runtime.InterruptTurn(ctx, strings.TrimSpace(key), strings.TrimSpace(runID))
	if err != nil {
		return AbortResult{}, err
	}
	return AbortResult{Backend: BackendCodex, SessionKey: result.ThreadID, RunID: result.TurnID, Status: result.Status}, nil
}

func (b *CodexBackend) SubscribeEvents(handler func(Event)) func() {
	if b.broker == nil || handler == nil {
		return func() {}
	}
	channel, unsubscribe := b.broker.Subscribe()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case event, ok := <-channel:
				if !ok {
					return
				}
				converted, ok := convertCodexEvent(event)
				if ok {
					handler(converted)
				}
			}
		}
	}()
	return func() { close(stop); unsubscribe() }
}

func (b *CodexBackend) ConnectionStatus() ConnectionStatus {
	status := ConnectionStatus{Backend: BackendCodex}
	if b.runtime == nil {
		return status
	}
	runtimeStatus := b.runtime.Status()
	status.Configured = runtimeStatus.CodexCLIAvailable
	status.Running = runtimeStatus.AppServerRunning
	status.Connected = runtimeStatus.AppServerRunning
	status.State = runtimeStatus.CodexCLIConnectionStatus
	status.ServerVersion = runtimeStatus.CodexCLIVersion
	status.LastError = runtimeStatus.LastError
	return status
}

func convertCodexEvent(event events.Event) (Event, bool) {
	if event.ThreadID == "" {
		return Event{}, false
	}
	result := Event{Backend: BackendCodex, Type: event.EventType, SessionKey: event.ThreadID, RunID: event.TurnID, Payload: event.Payload}
	switch event.EventType {
	case events.AssistantDelta:
		result.Delta = stringValue(event.Payload, "delta")
	case events.AssistantCompleted:
		// Assistant completion is an item-level notification, not proof of a
		// formal Turn answer. Expose text to generic conversation consumers only
		// when the protocol explicitly marks this item as final; the Task Center
		// and history readers use the shared selector for legacy compatibility.
		if control.IsExplicitFinalPhase(stringValue(event.Payload, "phase")) {
			result.Text = stringValue(event.Payload, "text")
		}
	case events.TurnFailed:
		result.Error = stringValue(event.Payload, "error")
	}
	return result, true
}

func stringValue(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return value
}
