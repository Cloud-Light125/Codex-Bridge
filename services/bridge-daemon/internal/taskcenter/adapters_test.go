package taskcenter

import (
	"context"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
)

type persistedCodexControl struct{}

func (persistedCodexControl) ListThreads(context.Context, int, string) (control.ThreadList, error) {
	return control.ThreadList{Threads: []control.ThreadSummary{{ThreadID: "thread-1", Number: 1}}}, nil
}

func (persistedCodexControl) ReadThread(context.Context, string, bool) (control.ThreadDetail, error) {
	return control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread-1", Number: 1}}, nil
}

type persistedCodexRuntime struct{}

func (persistedCodexRuntime) RuntimeState(string) control.RuntimeState {
	return control.RuntimeState{State: bridgeruntime.StatePersisted, TurnID: "old-run"}
}

func (persistedCodexRuntime) StartTurn(context.Context, string, control.StartTurnRequest) (control.TurnAccepted, error) {
	return control.TurnAccepted{}, nil
}

func (persistedCodexRuntime) InterruptTurn(context.Context, string, string) (control.InterruptResult, error) {
	return control.InterruptResult{}, nil
}

func TestCodexTaskAdapterDoesNotTreatPersistedTurnIDAsActive(t *testing.T) {
	adapter := NewCodexTaskAdapter(persistedCodexControl{}, persistedCodexRuntime{}, nil)
	ref, err := adapter.ResolveConversation(context.Background(), 0, "thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if ref.HasActiveRun || ref.ActiveRunID != "" {
		t.Fatalf("persisted conversation reported active: %#v", ref)
	}

	conversations, err := adapter.ListConversations(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(conversations) != 1 || conversations[0].HasActiveRun || conversations[0].ActiveRunID != "" {
		t.Fatalf("persisted conversation list reported active: %#v", conversations)
	}
}
