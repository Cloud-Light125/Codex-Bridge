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

type finalCodexControl struct{ detail control.ThreadDetail }

func (f finalCodexControl) ListThreads(context.Context, int, string) (control.ThreadList, error) {
	return control.ThreadList{Threads: []control.ThreadSummary{f.detail.ThreadSummary}}, nil
}

func (f finalCodexControl) ReadThread(context.Context, string, bool) (control.ThreadDetail, error) {
	return f.detail, nil
}

func (f finalCodexControl) ReadThreadHistory(context.Context, string, int) (control.ThreadDetail, error) {
	return f.detail, nil
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

func TestCodexTaskAdapterReadFinalAnswerUsesSharedSelector(t *testing.T) {
	controlService := finalCodexControl{detail: control.ThreadDetail{
		ThreadSummary: control.ThreadSummary{ThreadID: "thread-1", HistoryMode: "paginated"},
		Turns: []control.Turn{{TurnID: "turn-1", Status: "completed", Items: []control.Item{
			{Type: "agentMessage", Phase: "commentary", Text: "WRONG-1"},
			{Type: "agentMessage", Text: "WRONG-2"},
			{Type: "agentMessage", Phase: "final_answer", ItemID: "final-1", Text: "CORRECT-FINAL"},
		}}},
	}}
	adapter := NewCodexTaskAdapter(controlService, persistedCodexRuntime{}, nil)
	item, found, err := adapter.ReadFinalAnswer(context.Background(), "thread-1", "turn-1")
	if err != nil || !found || item.ItemID != "final-1" || item.Text != "CORRECT-FINAL" {
		t.Fatalf("final answer selection=%#v found=%t err=%v", item, found, err)
	}
	controlService.detail.Turns[0].Items = controlService.detail.Turns[0].Items[:2]
	item, found, err = adapter.ReadFinalAnswer(context.Background(), "thread-1", "turn-1")
	if err != nil || found || item.ItemID != "" {
		t.Fatalf("paginated progress was accepted as final: item=%#v found=%t err=%v", item, found, err)
	}
}
