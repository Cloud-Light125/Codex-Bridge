package runtime

import (
	"errors"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

func TestQQTurnUsesUnrestrictedWithoutApproval(t *testing.T) {
	options, err := turnStartOptions(control.StartTurnRequest{Origin: control.TurnOriginQQ}, "D:/code/project", "read-only")
	if err != nil {
		t.Fatal(err)
	}
	if options.SandboxMode != security.SandboxDangerFullAccess || options.ApprovalPolicy != security.ApprovalNever {
		t.Fatalf("QQ turn security = sandbox %q, approval %q", options.SandboxMode, options.ApprovalPolicy)
	}
}

func TestTelegramTurnUsesUnrestrictedWithoutApproval(t *testing.T) {
	options, err := turnStartOptions(control.StartTurnRequest{Origin: control.TurnOriginTelegram}, "D:/code/project", "workspace-write")
	if err != nil {
		t.Fatal(err)
	}
	if options.SandboxMode != security.SandboxDangerFullAccess || options.ApprovalPolicy != security.ApprovalNever {
		t.Fatalf("Telegram turn security = sandbox %q, approval %q", options.SandboxMode, options.ApprovalPolicy)
	}
}

func TestRuntimeStateKeepsLocalInterruptOwnership(t *testing.T) {
	manager := &Manager{
		status: Status{AppServerRunning: true}, interactions: interactions.NewStore(),
		states: map[string]control.RuntimeState{
			"thread-1": {ThreadID: "thread-1", TurnID: "turn-1", State: StateRunningLocal, Origin: "local"},
		},
	}
	state := manager.RuntimeState("thread-1")
	if !state.CanInterrupt || state.CanSend {
		t.Fatalf("unexpected local running state: %#v", state)
	}
}

func TestBusyErrorHasStableConflictDTOFields(t *testing.T) {
	err := busyError("thread-1", StateRunningExternal)
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Code != "thread_busy" || conflict.ThreadID != "thread-1" || conflict.CurrentState != StateRunningExternal {
		t.Fatalf("unexpected busy conflict: %#v", err)
	}
}

func TestUserInputRequiresSingleUseValidAnswers(t *testing.T) {
	item := interactions.PendingInteraction{
		Kind: interactions.KindUserInput,
		Questions: []interactions.Question{{ID: "choice", Text: "选择", Type: "single-choice", Required: true,
			Options: []interactions.QuestionOption{{Label: "A", Value: "a"}, {Label: "B", Value: "b"}}}},
	}
	if _, _, err := interactionResult(item, interactions.ResponseRequest{Action: "submit", Answers: map[string][]string{"choice": {"a", "b"}}}); err == nil {
		t.Fatal("multiple answers must be rejected for single-choice input")
	}
	result, status, err := interactionResult(item, interactions.ResponseRequest{Action: "submit", Answers: map[string][]string{"choice": {"a"}}})
	if err != nil || status != "submitted" || result["answers"] == nil {
		t.Fatalf("valid answer was rejected: %#v, %s, %v", result, status, err)
	}
}

func TestUserInputCancelReturnsEmptyAnswers(t *testing.T) {
	item := interactions.PendingInteraction{Kind: interactions.KindUserInput}
	result, status, err := interactionResult(item, interactions.ResponseRequest{Action: "cancel"})
	if err != nil || status != "cancelled" {
		t.Fatalf("cancel result = %#v, %q, %v", result, status, err)
	}
	answers, ok := result["answers"].(map[string]any)
	if !ok || len(answers) != 0 {
		t.Fatalf("cancel answers = %#v; want empty map", result["answers"])
	}
}
