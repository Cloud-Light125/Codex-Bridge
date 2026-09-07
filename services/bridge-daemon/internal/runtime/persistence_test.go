package runtime

import (
	"errors"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
)

func TestSelectedThreadIdentityFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		selected string
		snapshot control.ThreadPersistenceSnapshot
		want     string
	}{
		{name: "same", selected: "thread-a", snapshot: control.ThreadPersistenceSnapshot{ThreadID: "thread-a"}, want: ""},
		{name: "missing", selected: "thread-a", snapshot: control.ThreadPersistenceSnapshot{}, want: StateThreadMismatch},
		{name: "different", selected: "thread-a", snapshot: control.ThreadPersistenceSnapshot{ThreadID: "thread-b"}, want: StateThreadMismatch},
		{name: "ephemeral", selected: "thread-a", snapshot: control.ThreadPersistenceSnapshot{ThreadID: "thread-a", Ephemeral: true}, want: StateFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, got := selectedThreadIdentity(test.selected, test.snapshot)
			if got != test.want {
				t.Fatalf("selectedThreadIdentity state = %q; want %q", got, test.want)
			}
		})
	}
}

func TestCompletedNotificationRemainsUnverified(t *testing.T) {
	if got := completedNotificationState("completed"); got != StateCompletedUnverified {
		t.Fatalf("completed notification state = %q; want %q", got, StateCompletedUnverified)
	}
	if completedNotificationState("completed") == StatePersisted {
		t.Fatal("turn/completed alone must never be persisted")
	}
}

func TestApplyVerificationStateDoesNotRepublishPersistedTurn(t *testing.T) {
	broker := events.NewBroker()
	manager := &Manager{states: map[string]control.RuntimeState{"thread": {ThreadID: "thread", TurnID: "turn", State: StatePersisted}}, broker: broker, interactions: interactions.NewStore()}
	stream, unsubscribe := broker.Subscribe()
	defer unsubscribe()
	manager.applyVerificationState(control.PersistenceVerification{ThreadID: "thread", ExpectedTurnID: "turn", Status: StatePersisted})
	select {
	case event := <-stream:
		t.Fatalf("duplicate persisted verification published %s", event.EventType)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStartAttemptFailurePreservesPreviousRuntimeProjection(t *testing.T) {
	manager := &Manager{
		states: map[string]control.RuntimeState{
			"thread": {
				ThreadID: "thread", State: StateIdle, TurnID: "last-turn",
				LastTurnResult: "completed", PersistenceStatus: PersistenceConfirmed,
			},
		},
		broker:       events.NewBroker(),
		interactions: interactions.NewStore(),
	}
	previous := manager.RuntimeState("thread")
	manager.recordStartAttemptFailure("thread", previous, errors.New("turn/start transport unavailable"))
	got := manager.RuntimeState("thread")
	if got.State != StateIdle || got.TurnID != "last-turn" || got.LastTurnResult != "completed" || got.PersistenceStatus != PersistenceConfirmed {
		t.Fatalf("start failure changed the previous execution projection: %#v", got)
	}
	if got.LastStartError == "" || got.Error != "" {
		t.Fatalf("start failure diagnostics were not isolated: %#v", got)
	}
}

func TestPendingPersistenceDoesNotPublishTurnFailure(t *testing.T) {
	broker := events.NewBroker()
	manager := &Manager{states: map[string]control.RuntimeState{"thread": {ThreadID: "thread", TurnID: "turn"}}, broker: broker, interactions: interactions.NewStore()}
	stream, unsubscribe := broker.Subscribe()
	defer unsubscribe()
	manager.applyVerificationState(control.PersistenceVerification{ThreadID: "thread", ExpectedTurnID: "turn", Status: StateCompletedUnverified})
	state := manager.RuntimeState("thread")
	if state.State != StateCompletedUnverified || state.PersistenceStatus != PersistencePending || state.LastTurnResult != "completed" {
		t.Fatalf("pending verification changed the wrong state fields: %#v", state)
	}
	for {
		select {
		case event := <-stream:
			if event.EventType == events.TurnFailed {
				t.Fatalf("pending verification published turn failure: %#v", event)
			}
		default:
			return
		}
	}
}

func TestEvaluatePersistenceRequiresIndependentProbeTurn(t *testing.T) {
	main := control.ThreadPersistenceSnapshot{ThreadID: "thread-a", FoundTurn: true, LastTurnID: "turn-a", TurnStatus: "completed", AssistantMessageItemID: "assistant-a"}
	probe := control.ThreadPersistenceSnapshot{ThreadID: "thread-a", FoundTurn: true, LastTurnID: "turn-a", TurnStatus: "completed", AssistantMessageItemID: "assistant-a"}
	status, _ := evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StatePersisted {
		t.Fatalf("independent read result = %q; want %q", status, StatePersisted)
	}

	probe.FoundTurn = false
	status, _ = evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StatePersisted {
		t.Fatalf("missing probe turn result = %q; want %q", status, StatePersisted)
	}

	status, _ = evaluatePersistence("thread-a", "turn-a", main, control.ThreadPersistenceSnapshot{}, errors.New("probe unavailable"), false)
	if status != StatePersisted {
		t.Fatalf("failed probe result = %q; want %q", status, StatePersisted)
	}

	main.TurnStatus = "inProgress"
	probe.TurnStatus = "inProgress"
	probe.FoundTurn = true
	status, _ = evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StateCompletedUnverified {
		t.Fatalf("running turn result = %q; want %q", status, StateCompletedUnverified)
	}
}

func TestEvaluatePersistenceRejectsThreadMismatchButTrustsIndependentEvidenceOverStderr(t *testing.T) {
	main := control.ThreadPersistenceSnapshot{ThreadID: "thread-b", FoundTurn: true, TurnStatus: "completed", AssistantMessageItemID: "assistant-a"}
	probe := control.ThreadPersistenceSnapshot{ThreadID: "thread-a", FoundTurn: true, TurnStatus: "completed", AssistantMessageItemID: "assistant-a"}
	status, _ := evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StateThreadMismatch {
		t.Fatalf("thread mismatch result = %q; want %q", status, StateThreadMismatch)
	}

	main.ThreadID = "thread-a"
	status, _ = evaluatePersistence("thread-a", "turn-a", main, probe, nil, true)
	if status != StatePersisted {
		t.Fatalf("independently verified stderr warning result = %q; want %q", status, StatePersisted)
	}
}

func TestEvaluatePersistenceWaitsForAssistantMessage(t *testing.T) {
	main := control.ThreadPersistenceSnapshot{ThreadID: "thread-a", FoundTurn: true, TurnStatus: "completed"}
	probe := control.ThreadPersistenceSnapshot{ThreadID: "thread-a", FoundTurn: true, TurnStatus: "completed", AssistantMessageItemID: "assistant-a"}
	status, _ := evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StateCompletedUnverified {
		t.Fatalf("missing main assistant result = %q; want %q", status, StateCompletedUnverified)
	}
	main.AssistantMessageItemID = "assistant-a"
	probe.AssistantMessageItemID = ""
	status, _ = evaluatePersistence("thread-a", "turn-a", main, probe, nil, false)
	if status != StatePersisted {
		t.Fatalf("missing probe assistant result = %q; want %q", status, StatePersisted)
	}
}

func TestTurnCompletedErrorFailsClosed(t *testing.T) {
	if !completionErrorPresent(map[string]any{"error": map[string]any{"message": "content is not inspected"}}) {
		t.Fatal("non-empty turn/completed.error must be detected")
	}
	if completionErrorPresent(map[string]any{"error": nil}) {
		t.Fatal("null turn/completed.error must not be treated as an error")
	}
}

func TestPersistenceSnapshotUsesFormalFinalAssistantItem(t *testing.T) {
	raw := map[string]any{"thread": map[string]any{
		"id": "thread-217", "historyMode": "paginated",
		"turns": []any{map[string]any{"id": "turn-217", "status": "completed", "items": []any{
			map[string]any{"id": "commentary", "type": "agentMessage", "role": "assistant", "phase": "commentary", "text": "PROGRESS"},
			map[string]any{"id": "wrong", "type": "agentMessage", "role": "assistant", "text": "WRONG-PROGRESS"},
			map[string]any{"id": "correct", "type": "agentMessage", "role": "assistant", "phase": "final_answer", "text": "CORRECT-FINAL"},
		}}},
	}}
	snapshot := persistenceSnapshot(raw, "turn-217")
	if snapshot.AssistantMessageItemID != "correct" {
		t.Fatalf("persistence snapshot selected %q; want formal final item", snapshot.AssistantMessageItemID)
	}

	rawPaginatedWithoutFinal := map[string]any{"thread": map[string]any{
		"id": "thread-paginated", "historyMode": "paginated",
		"turns": []any{map[string]any{"id": "turn-1", "status": "completed", "items": []any{
			map[string]any{"id": "wrong", "type": "agentMessage", "role": "assistant", "text": "WRONG-PROGRESS"},
		}}},
	}}
	if snapshot := persistenceSnapshot(rawPaginatedWithoutFinal, "turn-1"); snapshot.AssistantMessageItemID != "" {
		t.Fatalf("paginated persistence snapshot accepted unphased assistant %q", snapshot.AssistantMessageItemID)
	}

	rawLegacy := map[string]any{"thread": map[string]any{
		"id": "thread-legacy", "historyMode": "legacy",
		"turns": []any{map[string]any{"id": "turn-1", "status": "completed", "items": []any{
			map[string]any{"id": "legacy-final", "type": "agentMessage", "role": "assistant", "text": "LEGACY-FINAL"},
		}}},
	}}
	if snapshot := persistenceSnapshot(rawLegacy, "turn-1"); snapshot.AssistantMessageItemID != "legacy-final" {
		t.Fatalf("legacy persistence snapshot rejected legacy fallback: %q", snapshot.AssistantMessageItemID)
	}
}
