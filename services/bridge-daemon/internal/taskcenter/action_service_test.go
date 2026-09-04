package taskcenter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
)

type actionTestAdapter struct{ fakeTaskAdapter }

func (a *actionTestAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{
		CanStop: true, CanContinue: true, CanReportWaitingInput: true, CanQueryRunState: true,
		SupportsGitActions: true, SupportsTestAction: true, SupportsContinue: true, SupportsRetry: true,
		SupportsCancel: true, SupportsOpenConversation: true,
	}
}

func TestTaskActionRegistryAvailability(t *testing.T) {
	registry := NewTaskActionRegistry()
	codexCapabilities := AdapterCapabilities{SupportsGitActions: true, SupportsTestAction: true, SupportsContinue: true, SupportsRetry: true, SupportsCancel: true, SupportsOpenConversation: true}
	contextFor := func(backend, status string, capabilities AdapterCapabilities) TaskActionAvailabilityContext {
		return TaskActionAvailabilityContext{
			Task: Task{Backend: backend, Status: status, ConversationNumber: 203}, Project: Project{ProjectID: "project-1", Enabled: true}, ProjectAvailable: true, Capabilities: capabilities,
			WorkingDirectoryValid: true, GitRepository: true, HasConversation: true,
		}
	}
	ids := func(items []TaskActionAvailability) map[string]bool {
		result := map[string]bool{}
		for _, item := range items {
			result[item.Id] = true
		}
		return result
	}
	completed := ids(registry.Available(contextFor(BackendCodex, StatusCompleted, codexCapabilities)))
	for _, id := range []string{TaskActionContinue, TaskActionTest, TaskActionDiff, TaskActionGitStatus, TaskActionCommit, TaskActionCommitPush, TaskActionOpen} {
		if !completed[id] {
			t.Fatalf("completed Codex missing %s: %#v", id, completed)
		}
	}
	openclaw := ids(registry.Available(contextFor(BackendOpenClaw, StatusCompleted, AdapterCapabilities{SupportsContinue: true, SupportsRetry: true, SupportsCancel: true, SupportsOpenConversation: true})))
	if !openclaw[TaskActionContinue] || !openclaw[TaskActionOpen] || openclaw[TaskActionDiff] || openclaw[TaskActionCommit] {
		t.Fatalf("completed OpenClaw actions=%#v", openclaw)
	}
	running := ids(registry.Available(contextFor(BackendCodex, StatusRunning, codexCapabilities)))
	if !running[TaskActionCancel] || !running[TaskActionOpen] || running[TaskActionCommit] || running[TaskActionTest] {
		t.Fatalf("running actions=%#v", running)
	}
	failed := ids(registry.Available(contextFor(BackendCodex, StatusFailed, codexCapabilities)))
	for _, id := range []string{TaskActionRetry, TaskActionContinue, TaskActionDiff, TaskActionGitStatus, TaskActionOpen} {
		if !failed[id] {
			t.Fatalf("failed Codex missing %s: %#v", id, failed)
		}
	}
	if failed[TaskActionTest] {
		t.Fatalf("failed Codex unexpectedly exposes test: %#v", failed)
	}
	waiting := ids(registry.Available(contextFor(BackendCodex, StatusWaitingInput, codexCapabilities)))
	if !waiting[TaskActionCancel] || !waiting[TaskActionOpen] || waiting[TaskActionGitStatus] || waiting[TaskActionDiff] {
		t.Fatalf("waiting-input actions=%#v", waiting)
	}
	filtered := ids(registry.Available(contextFor(BackendCodex, StatusCompleted, AdapterCapabilities{SupportsContinue: true})))
	if filtered[TaskActionTest] || filtered[TaskActionDiff] || filtered[TaskActionCommit] || filtered[TaskActionCommitPush] || filtered[TaskActionOpen] {
		t.Fatalf("capability filtering leaked actions=%#v", filtered)
	}
}

func TestExecutionActionsCreateChildTasksAndPreserveActionSource(t *testing.T) {
	directory := createActionTestRepository(t)
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, tasks, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Action Repo", DefaultBackend: BackendCodex, WorkingDirectory: directory, DefaultConversationNumber: intPointer(203)})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	if err != nil {
		t.Fatal(err)
	}
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})

	testResponse, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionTest})
	if err != nil || testResponse.Task == nil {
		t.Fatalf("test action response=%#v err=%v", testResponse, err)
	}
	child := *testResponse.Task
	if child.ActionID != TaskActionTest || child.ActionSourceTaskNumber != parent.TaskNumber || child.ParentTaskNumber != parent.TaskNumber || child.Status != StatusRunning || child.Description != testActionPrompt {
		t.Fatalf("test child=%#v", child)
	}
	if stored, ok := tasks.Get(child.TaskNumber); !ok || stored.ActionID != TaskActionTest {
		t.Fatalf("stored child=%#v ok=%v", stored, ok)
	}
}

func TestContinueAndRetryActionsReuseExistingTaskFlow(t *testing.T) {
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Action Flow", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(203)})
	parent, _ := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})

	continuedResponse, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionContinue, Text: "继续处理"})
	if err != nil || continuedResponse.Task == nil || continuedResponse.Task.ParentTaskNumber != parent.TaskNumber || continuedResponse.Task.ActionID != TaskActionContinue || continuedResponse.Task.ActionSourceTaskNumber != parent.TaskNumber {
		t.Fatalf("continue response=%#v err=%v", continuedResponse, err)
	}
	continued := *continuedResponse.Task
	service.HandleEvent(events.Event{EventType: events.TurnFailed, ThreadID: continued.TargetID, TurnID: continued.CurrentRunID, Payload: map[string]any{"error": "retry me"}})

	retriedResponse, err := service.ExecuteAction(context.Background(), continued.TaskNumber, TaskActionRequest{ActionID: TaskActionRetry})
	if err != nil || retriedResponse.Task == nil || retriedResponse.Task.RetryOfTaskNumber != continued.TaskNumber || retriedResponse.Task.ActionID != TaskActionRetry || retriedResponse.Task.ActionSourceTaskNumber != continued.TaskNumber {
		t.Fatalf("retry response=%#v err=%v", retriedResponse, err)
	}
	if len(tasks.List(TaskFilter{})) != 3 {
		t.Fatalf("unexpected action task count=%d", len(tasks.List(TaskFilter{})))
	}
}

func TestCommitPushRequiresConfirmationBeforeCreatingTask(t *testing.T) {
	for _, forbidden := range []string{"force push", "reset --hard", "clean -fd", "tag -f", "rebase", "secret"} {
		if !strings.Contains(commitActionPrompt, forbidden) || !strings.Contains(commitPushActionPrompt, forbidden) {
			t.Fatalf("commit prompts must forbid %q: commit=%q commit-push=%q", forbidden, commitActionPrompt, commitPushActionPrompt)
		}
	}
	directory := createActionTestRepository(t)
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Action Repo", DefaultBackend: BackendCodex, WorkingDirectory: directory, DefaultConversationNumber: intPointer(203)})
	parent, _ := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})
	before := len(tasks.List(TaskFilter{}))
	response, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionCommitPush})
	if err != nil || !response.ConfirmationRequired || len(tasks.List(TaskFilter{})) != before {
		t.Fatalf("unconfirmed response=%#v err=%v tasks=%d", response, err, len(tasks.List(TaskFilter{})))
	}
	confirmed, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionCommitPush, Confirmed: true})
	if err != nil || confirmed.Task == nil || confirmed.Task.ActionID != TaskActionCommitPush || confirmed.Task.Description != commitPushActionPrompt {
		t.Fatalf("confirmed response=%#v err=%v", confirmed, err)
	}
}

func TestCommitActionCreatesChildAndProjectGitStatusIsReadOnly(t *testing.T) {
	directory := createActionTestRepository(t)
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, _, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Action Repo", DefaultBackend: BackendCodex, WorkingDirectory: directory, DefaultConversationNumber: intPointer(203)})
	parent, _ := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})

	status, err := service.GetProjectGitStatus(context.Background(), project.ProjectID)
	if err != nil || !status.GitRepository || status.WorkingTree != "modified" || strings.TrimSpace(status.Branch) == "" || !strings.Contains(status.LatestCommit, "initial") {
		t.Fatalf("project Git status=%#v err=%v", status, err)
	}
	response, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionCommit})
	if err != nil || response.Task == nil || response.Task.ActionID != TaskActionCommit || response.Task.ParentTaskNumber != parent.TaskNumber || response.Task.Description != commitActionPrompt {
		t.Fatalf("commit response=%#v err=%v", response, err)
	}
}

func TestRemoteActionsAndConfirmationAuthorization(t *testing.T) {
	directory := createActionTestRepository(t)
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Action Repo", DefaultBackend: BackendCodex, WorkingDirectory: directory, DefaultConversationNumber: intPointer(203)})
	parent, _ := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})
	commands := commandregistry.NewInMemory()
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "telegram", ChannelProfileID: "profile", AccountID: "account", ConversationType: "private", ChatID: "chat"}, UserID: "owner"}
	invoke := func(message channels.InboundMessage, text string) string {
		t.Helper()
		invocation, ok := commands.Resolve(text)
		if !ok {
			t.Fatalf("command not registered: %s", text)
		}
		result, handled, err := service.ExecuteRemoteCommand(context.Background(), message, invocation)
		if err != nil || !handled {
			t.Fatalf("remote command %s result=%q handled=%v err=%v", text, result, handled, err)
		}
		return result
	}
	if result := invoke(message, "/actions 1"); !strings.Contains(result, "commit-push") || !strings.Contains(result, "test") {
		t.Fatalf("actions result=%q", result)
	}
	confirmation := invoke(message, "/action 1 commit-push")
	match := regexp.MustCompile(`/confirm (A[A-F0-9]+)`).FindStringSubmatch(confirmation)
	if len(match) != 2 {
		t.Fatalf("confirmation=%q", confirmation)
	}
	other := message
	other.UserID = "other"
	if result := invoke(other, "/confirm "+match[1]); !strings.Contains(result, "拒绝") {
		t.Fatalf("other confirmation result=%q", result)
	}
	wrongChannel := message
	wrongChannel.Address.ChannelType = "qqbot"
	if result := invoke(wrongChannel, "/confirm "+match[1]); !strings.Contains(result, "拒绝") {
		t.Fatalf("wrong channel confirmation result=%q", result)
	}
	wrongProfile := message
	wrongProfile.Address.ChannelProfileID = "other-profile"
	if result := invoke(wrongProfile, "/confirm "+match[1]); !strings.Contains(result, "拒绝") {
		t.Fatalf("wrong profile confirmation result=%q", result)
	}
	wrongChat := message
	wrongChat.Address.ChatID = "other-chat"
	if result := invoke(wrongChat, "/confirm "+match[1]); !strings.Contains(result, "拒绝") {
		t.Fatalf("wrong chat confirmation result=%q", result)
	}
	if len(tasks.List(TaskFilter{})) != 1 {
		t.Fatal("unauthorized confirmation created a task")
	}
	if result := invoke(message, "/confirm "+match[1]); !strings.Contains(result, "T2") {
		t.Fatalf("owner confirmation result=%q", result)
	}
	if len(tasks.List(TaskFilter{})) != 2 {
		t.Fatal("owner confirmation did not create one task")
	}
	if result := invoke(message, "/confirm "+match[1]); !strings.Contains(result, "不存在或已过期") {
		t.Fatalf("duplicate confirmation result=%q", result)
	}
	expiredConfirmation := invoke(message, "/action 1 commit-push")
	expiredMatch := regexp.MustCompile(`/confirm (A[A-F0-9]+)`).FindStringSubmatch(expiredConfirmation)
	if len(expiredMatch) != 2 {
		t.Fatalf("expired confirmation=%q", expiredConfirmation)
	}
	service.mu.Lock()
	pending := service.pendingActions[expiredMatch[1]]
	pending.ExpiresAt = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	service.pendingActions[expiredMatch[1]] = pending
	service.mu.Unlock()
	if result := invoke(message, "/confirm "+expiredMatch[1]); !strings.Contains(result, "不存在或已过期") {
		t.Fatalf("expired confirmation result=%q", result)
	}
	if len(tasks.List(TaskFilter{})) != 2 {
		t.Fatal("expired confirmation created a task")
	}
}

func TestRemoteDiffReturnsSummaryAndFullOutputIsBounded(t *testing.T) {
	directory := createActionTestRepository(t)
	adapter := &actionTestAdapter{fakeTaskAdapter: fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 203, TargetID: "thread-203"}}}}
	service, _, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Action Repo", DefaultBackend: BackendCodex, WorkingDirectory: directory, DefaultConversationNumber: intPointer(203)})
	parent, _ := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "原始任务"})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: parent.TargetID, TurnID: parent.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "完成"}})
	response, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionDiff, Remote: true})
	if err != nil || response.Result == nil || !strings.Contains(response.Result.Result, "test.txt") || strings.Contains(response.Result.Result, "完整 Diff：") {
		t.Fatalf("summary response=%#v err=%v", response, err)
	}
	full, err := service.ExecuteAction(context.Background(), parent.TaskNumber, TaskActionRequest{ActionID: TaskActionDiff, Full: true, Remote: true})
	if err != nil || full.Result == nil || len([]rune(full.Result.Result)) > 5200 {
		t.Fatalf("full response=%#v err=%v", full, err)
	}
}

func createActionTestRepository(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	runActionGit(t, directory, "init")
	runActionGit(t, directory, "config", "user.email", "action-test@example.invalid")
	runActionGit(t, directory, "config", "user.name", "Action Test")
	file := filepath.Join(directory, "test.txt")
	if err := os.WriteFile(file, []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runActionGit(t, directory, "add", "test.txt")
	runActionGit(t, directory, "commit", "-m", "initial")
	if err := os.WriteFile(file, []byte("before\nafter\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return directory
}

func runActionGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
