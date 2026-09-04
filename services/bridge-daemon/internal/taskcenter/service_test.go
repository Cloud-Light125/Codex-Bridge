package taskcenter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
)

type fakeTaskAdapter struct {
	backend    string
	refs       []ConversationRef
	sent       []string
	cancelled  []string
	active     bool
	waiting    bool
	nextRun    int
	runID      string
	resolveErr error
	queryErr   error
}

type blockingTaskAdapter struct {
	*fakeTaskAdapter
	started chan struct{}
	release chan struct{}
}

func (f *blockingTaskAdapter) SendTask(ctx context.Context, ref ConversationRef, text, cwd, origin string) (conversation.SendResult, error) {
	close(f.started)
	<-f.release
	return f.fakeTaskAdapter.SendTask(ctx, ref, text, cwd, origin)
}

func (f *fakeTaskAdapter) Backend() string { return f.backend }
func (f *fakeTaskAdapter) Capabilities() AdapterCapabilities {
	return AdapterCapabilities{CanStop: true, CanContinue: true, CanReportWaitingInput: f.backend == BackendCodex, CanQueryRunState: true}
}
func (f *fakeTaskAdapter) ListConversations(context.Context, int) ([]ConversationRef, error) {
	return append([]ConversationRef(nil), f.refs...), nil
}
func (f *fakeTaskAdapter) ResolveConversation(_ context.Context, number int, target string) (ConversationRef, error) {
	if f.resolveErr != nil {
		return ConversationRef{}, f.resolveErr
	}
	for _, ref := range f.refs {
		if (number > 0 && ref.Number == number) || (target != "" && ref.TargetID == target) {
			return ref, nil
		}
	}
	return ConversationRef{}, errors.New("conversation not found")
}
func (f *fakeTaskAdapter) SendTask(_ context.Context, ref ConversationRef, text, _, _ string) (conversation.SendResult, error) {
	f.nextRun++
	f.sent = append(f.sent, ref.TargetID+":"+text)
	f.active = true
	f.runID = "run-" + itoa(f.nextRun)
	return conversation.SendResult{Backend: f.backend, SessionKey: ref.TargetID, RunID: f.runID, Status: "running", AcceptedAt: nowText()}, nil
}
func (f *fakeTaskAdapter) CancelTask(_ context.Context, ref ConversationRef, runID string) (conversation.AbortResult, error) {
	f.cancelled = append(f.cancelled, ref.TargetID+":"+runID)
	f.active = false
	return conversation.AbortResult{Backend: f.backend, SessionKey: ref.TargetID, RunID: runID, Status: "aborted"}, nil
}
func (f *fakeTaskAdapter) QueryRunState(context.Context, ConversationRef, string) (RunState, error) {
	if f.queryErr != nil {
		return RunState{}, f.queryErr
	}
	return RunState{Exists: true, Active: f.active, WaitingInput: f.waiting, RunID: firstNonEmpty(f.runID, "run-1"), State: "running"}, nil
}

type fakeConversationBackend struct {
	detail    conversation.Detail
	connected bool
}

func (f *fakeConversationBackend) Backend() string { return conversation.BackendOpenClaw }
func (f *fakeConversationBackend) ListSessions(context.Context, int) ([]conversation.Session, error) {
	if f.detail.Key == "" {
		return nil, nil
	}
	return []conversation.Session{f.detail.Session}, nil
}
func (f *fakeConversationBackend) ReadSession(context.Context, string) (conversation.Detail, error) {
	if f.detail.Key == "" {
		return conversation.Detail{}, errors.New("session not found")
	}
	return f.detail, nil
}
func (f *fakeConversationBackend) SendMessage(context.Context, string, string) (conversation.SendResult, error) {
	return conversation.SendResult{Backend: conversation.BackendOpenClaw, SessionKey: f.detail.Key, RunID: "open-run-1", Status: "running"}, nil
}
func (f *fakeConversationBackend) Abort(context.Context, string, string) (conversation.AbortResult, error) {
	return conversation.AbortResult{Backend: conversation.BackendOpenClaw, SessionKey: f.detail.Key, Status: "aborted"}, nil
}
func (f *fakeConversationBackend) SubscribeEvents(func(conversation.Event)) func() { return func() {} }
func (f *fakeConversationBackend) ConnectionStatus() conversation.ConnectionStatus {
	return conversation.ConnectionStatus{Backend: conversation.BackendOpenClaw, Connected: f.connected}
}

func newTestService(t *testing.T, adapters ...TaskBackendAdapter) (*Service, *TaskRegistry, *ProjectRegistry) {
	t.Helper()
	projects, err := NewProjectRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewTaskRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return NewService(projects, tasks, events.NewBroker(), nil, adapters...), tasks, projects
}

func TestTaskServiceRoutesAndCompletes(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 7, TargetID: "thread-7"}}}
	service, tasks, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Demo", Aliases: []string{"demo"}, DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(7)})
	if err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "修复统计并运行测试"})
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != StatusRunning || task.TaskNumber != 1 || task.ConversationNumber != 7 || len(adapter.sent) != 1 {
		t.Fatalf("created task=%#v sent=%#v", task, adapter.sent)
	}
	service.HandleEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread-7", TurnID: task.CurrentRunID, Payload: map[string]any{
		"interaction": map[string]any{"id": "input-1", "questions": []any{map[string]any{"text": "选择环境"}}},
	}})
	waiting, _ := tasks.Get(1)
	if waiting.Status != StatusWaitingInput || waiting.PendingInteractionID != "input-1" || waiting.PendingQuestion != "选择环境" {
		t.Fatalf("waiting task=%#v", waiting)
	}
	service.HandleEvent(events.Event{EventType: events.InteractionResolved, ThreadID: "thread-7", TurnID: task.CurrentRunID, Payload: map[string]any{"interaction": map[string]any{"id": "input-1"}}})
	service.HandleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread-7", TurnID: task.CurrentRunID, Payload: map[string]any{"phase": "commentary", "text": "WRONG-PROGRESS"}})
	service.HandleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread-7", TurnID: task.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "已修复\n\ngo test ./... PASS"}})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread-7", TurnID: task.CurrentRunID, Payload: map[string]any{"status": "persisted"}})
	completed, _ := tasks.Get(1)
	if completed.Status != StatusCompleted || !completed.Result.Success || completed.Result.FinalText == "" || strings.Contains(completed.Result.FinalText, "WRONG-PROGRESS") || len(completed.Summary.TestResults) != 1 {
		t.Fatalf("completed task=%#v", completed)
	}
}

func TestTaskServiceWaitsForFormalFinalWhenCompletionArrivesFirst(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 7, TargetID: "thread-7"}}}
	service, tasks, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Late final", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(7)})
	if err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "等待正式回答"})
	if err != nil {
		t.Fatal(err)
	}
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: task.TargetID, TurnID: task.CurrentRunID, Payload: map[string]any{"status": "persisted"}})
	pending, _ := tasks.Get(task.TaskNumber)
	if pending.Status != StatusRunning || pending.Result.FinalText != "" {
		t.Fatalf("completion without formal final was accepted: %#v", pending)
	}
	service.HandleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: task.TargetID, TurnID: task.CurrentRunID, Payload: map[string]any{"phase": "commentary", "text": "WRONG-PROGRESS"}})
	pending, _ = tasks.Get(task.TaskNumber)
	if pending.Status != StatusRunning {
		t.Fatalf("commentary completed the pending task: %#v", pending)
	}
	service.HandleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: task.TargetID, TurnID: task.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "CORRECT-FINAL"}})
	completed, _ := tasks.Get(task.TaskNumber)
	if completed.Status != StatusCompleted || completed.Result.FinalText != "CORRECT-FINAL" {
		t.Fatalf("late formal final was not completed: %#v", completed)
	}
}

func TestTaskServiceRejectsActiveConversationAndSupportsParallelTargets(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{
		{Backend: BackendCodex, Number: 1, TargetID: "thread-1"},
		{Backend: BackendCodex, Number: 2, TargetID: "thread-2"},
	}}
	service, tasks, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Parallel", DefaultBackend: BackendCodex, ReuseStrategy: ReuseDefault})
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, ConversationNumber: 1, Description: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, ConversationNumber: 2, Description: "second"})
	if err != nil || first.TargetID == second.TargetID {
		t.Fatalf("parallel tasks first=%#v second=%#v err=%v", first, second, err)
	}
	third, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, ConversationNumber: 1, Description: "conflict"})
	if err == nil || third.Status != StatusFailed || len(tasks.ActiveForTarget(BackendCodex, "thread-1")) != 1 {
		t.Fatalf("conflicting task=%#v err=%v", third, err)
	}
}

func TestTaskServiceDoesNotResendDispatchingTaskDuringRecovery(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Recovery", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(1)})
	task, err := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, Title: "uncertain", Description: "uncertain"})
	if err != nil {
		t.Fatal(err)
	}
	task.Status = StatusRouting
	task.DispatchState = Dispatching
	task.TargetID = "thread-1"
	task.ConversationNumber = 1
	if _, err := tasks.Update(task); err != nil {
		t.Fatal(err)
	}
	service.Recover(context.Background())
	recovered, _ := tasks.Get(task.TaskNumber)
	if recovered.Status != StatusInterrupted || len(adapter.sent) != 0 {
		t.Fatalf("recovered=%#v sent=%#v", recovered, adapter.sent)
	}
}

func TestTaskServiceRecoveryCannotOverwriteTaskDuringDispatch(t *testing.T) {
	adapter := &blockingTaskAdapter{
		fakeTaskAdapter: &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}},
		started:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	service, tasks, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Dispatch race", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(1)})
	if err != nil {
		t.Fatal(err)
	}

	createdCh := make(chan struct {
		task Task
		err  error
	}, 1)
	go func() {
		created, createErr := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "dispatch race"})
		createdCh <- struct {
			task Task
			err  error
		}{task: created, err: createErr}
	}()
	select {
	case <-adapter.started:
	case <-time.After(time.Second):
		t.Fatal("task did not reach backend dispatch")
	}

	recoveryDone := make(chan struct{})
	go func() {
		service.Recover(context.Background())
		close(recoveryDone)
	}()
	select {
	case <-recoveryDone:
		t.Fatal("recovery bypassed the dispatch mutex")
	default:
	}

	close(adapter.release)
	select {
	case result := <-createdCh:
		if result.err != nil || result.task.Status != StatusRunning {
			t.Fatalf("created task=%#v err=%v", result.task, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("task dispatch did not complete")
	}
	select {
	case <-recoveryDone:
	case <-time.After(time.Second):
		t.Fatal("recovery did not complete")
	}
	recovered, _ := tasks.Get(1)
	if recovered.Status != StatusRunning || recovered.LastError != "" || len(adapter.sent) != 1 {
		t.Fatalf("recovery overwrote dispatched task=%#v sent=%#v", recovered, adapter.sent)
	}
}

func TestTaskServiceRejectsMismatchedGlobalConversationBackend(t *testing.T) {
	registry := conversationregistry.NewInMemory()
	if _, err := registry.Ensure(conversationregistry.Metadata{Backend: BackendCodex, TargetID: "codex-1"}); err != nil {
		t.Fatal(err)
	}
	projects, _ := NewProjectRegistry("")
	project, _ := projects.Create(ProjectInput{Name: "Open", DefaultBackend: BackendOpenClaw, DefaultConversationNumber: intPointer(1)})
	backend := &fakeConversationBackend{detail: conversation.Detail{Session: conversation.Session{Key: "open-1"}}, connected: true}
	service := NewService(projects, mustTasks(t), events.NewBroker(), nil, NewOpenClawTaskAdapter(backend, registry))
	task, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "should fail"})
	if err == nil || task.Status != StatusFailed || !strings.Contains(task.LastError, "不是 OpenClaw") {
		t.Fatalf("expected registry backend mismatch, task=%#v err=%v", task, err)
	}
}

func TestTaskServiceRoutesOpenClawAndRejectsUnsupportedAutoCreate(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendOpenClaw, refs: []ConversationRef{{Backend: BackendOpenClaw, Number: 9, TargetID: "session-9"}}}
	service, _, projects := newTestService(t, adapter)
	project, err := projects.Create(ProjectInput{Name: "Open", DefaultBackend: BackendOpenClaw, DefaultConversationNumber: intPointer(9)})
	if err != nil {
		t.Fatal(err)
	}
	task, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "检查 Gateway"})
	if err != nil || task.Status != StatusRunning || task.Backend != BackendOpenClaw || task.ConversationNumber != 9 {
		t.Fatalf("OpenClaw route task=%#v err=%v", task, err)
	}

	autoProject, err := projects.Create(ProjectInput{Name: "Open Auto", DefaultBackend: BackendOpenClaw, AutoCreateConversation: true, ReuseStrategy: ReuseCreateNew})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := service.CreateTask(context.Background(), TaskInput{ProjectID: autoProject.ProjectID, Description: "必须明确失败"})
	if err == nil || failed.Status != StatusFailed || !strings.Contains(failed.LastError, "不支持由 Bridge 自动创建 Session") {
		t.Fatalf("unsupported OpenClaw auto-create task=%#v err=%v", failed, err)
	}
}

func TestTaskServiceCorrelatesRunsAndCancelsThroughBackend(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Lifecycle", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(1)})
	task, err := service.CreateTask(context.Background(), TaskInput{ProjectID: project.ProjectID, Description: "生命周期"})
	if err != nil {
		t.Fatal(err)
	}
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: task.TargetID, TurnID: "stale-run", Payload: map[string]any{"text": "不能采纳"}})
	current, _ := tasks.Get(task.TaskNumber)
	if current.Status != StatusRunning {
		t.Fatalf("stale terminal event changed task: %#v", current)
	}
	service.HandleEvent(events.Event{EventType: events.TurnFailed, ThreadID: task.TargetID, TurnID: task.CurrentRunID, Payload: map[string]any{"error": "执行失败"}})
	failed, _ := tasks.Get(task.TaskNumber)
	if failed.Status != StatusFailed || failed.LastError != "执行失败" {
		t.Fatalf("failed task=%#v", failed)
	}
	retried, err := service.RetryTask(context.Background(), task.TaskNumber, TaskInput{})
	if err != nil || retried.RetryOfTaskNumber != task.TaskNumber || retried.Status != StatusRunning {
		t.Fatalf("retried task=%#v err=%v", retried, err)
	}
	cancelled, err := service.CancelTask(context.Background(), retried.TaskNumber)
	if err != nil || cancelled.Status != StatusCancelled || len(adapter.cancelled) != 1 || !strings.HasSuffix(adapter.cancelled[0], ":"+retried.CurrentRunID) {
		t.Fatalf("cancelled task=%#v cancelled=%#v err=%v", cancelled, adapter.cancelled, err)
	}
}

func TestTaskServiceCancelDispatchingTaskConfirmsBackendFirst(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}, active: true, runID: "run-dispatching"}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Dispatching", DefaultBackend: BackendCodex})
	task, _ := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, ConversationNumber: 1, TargetID: "thread-1", Description: "uncertain send"})
	task.Status, task.DispatchState = StatusRouting, Dispatching
	if _, err := tasks.Update(task); err != nil {
		t.Fatal(err)
	}
	cancelled, err := service.CancelTask(context.Background(), task.TaskNumber)
	if err != nil || cancelled.Status != StatusCancelled || len(adapter.cancelled) != 1 || !strings.HasSuffix(adapter.cancelled[0], ":run-dispatching") {
		t.Fatalf("dispatching cancellation task=%#v cancelled=%#v err=%v", cancelled, adapter.cancelled, err)
	}
}

func TestTaskServiceRecoveryReconcilesQueuedRunningWaitingAndMissing(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}}
		service, tasks, projects := newTestService(t, adapter)
		project, _ := projects.Create(ProjectInput{Name: "Queued", DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(1)})
		created, err := tasks.Create(TaskInput{ProjectID: project.ProjectID, Description: "queued"})
		if err != nil {
			t.Fatal(err)
		}
		service.Recover(context.Background())
		recovered, _ := tasks.Get(created.TaskNumber)
		if recovered.Status != StatusRunning || len(adapter.sent) != 1 {
			t.Fatalf("queued recovery task=%#v sent=%#v", recovered, adapter.sent)
		}
	})

	t.Run("running", func(t *testing.T) {
		adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}, active: true, runID: "run-existing"}
		service, tasks, projects := newTestService(t, adapter)
		project, _ := projects.Create(ProjectInput{Name: "Running", DefaultBackend: BackendCodex})
		created, _ := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, ConversationNumber: 1, TargetID: "thread-1", Description: "running"})
		created.Status, created.DispatchState, created.CurrentRunID = StatusRunning, DispatchDispatched, "run-existing"
		_, _ = tasks.Update(created)
		service.Recover(context.Background())
		recovered, _ := tasks.Get(created.TaskNumber)
		if recovered.Status != StatusRunning || recovered.CurrentRunID != "run-existing" {
			t.Fatalf("running recovery task=%#v", recovered)
		}
	})

	t.Run("waiting-input", func(t *testing.T) {
		adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}, active: true, waiting: true, runID: "run-waiting"}
		service, tasks, projects := newTestService(t, adapter)
		project, _ := projects.Create(ProjectInput{Name: "Waiting", DefaultBackend: BackendCodex})
		created, _ := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, ConversationNumber: 1, TargetID: "thread-1", Description: "waiting"})
		created.Status, created.DispatchState, created.CurrentRunID = StatusWaitingInput, DispatchDispatched, "run-waiting"
		_, _ = tasks.Update(created)
		service.Recover(context.Background())
		recovered, _ := tasks.Get(created.TaskNumber)
		if recovered.Status != StatusWaitingInput {
			t.Fatalf("waiting recovery task=%#v", recovered)
		}
	})

	t.Run("conversation-missing", func(t *testing.T) {
		adapter := &fakeTaskAdapter{backend: BackendCodex}
		service, tasks, projects := newTestService(t, adapter)
		project, _ := projects.Create(ProjectInput{Name: "Missing", DefaultBackend: BackendCodex})
		created, _ := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, ConversationNumber: 1, TargetID: "deleted-thread", Description: "missing"})
		created.Status, created.DispatchState, created.CurrentRunID = StatusRunning, DispatchDispatched, "run-old"
		_, _ = tasks.Update(created)
		service.Recover(context.Background())
		recovered, _ := tasks.Get(created.TaskNumber)
		if recovered.Status != StatusInterrupted {
			t.Fatalf("missing conversation recovery task=%#v", recovered)
		}
	})
}

func TestTaskServiceRecoveryDefersTransientBackendErrors(t *testing.T) {
	adapter := &fakeTaskAdapter{
		backend:    BackendCodex,
		refs:       []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}},
		resolveErr: errors.New("temporary app-server read failure"),
		runID:      "run-existing",
	}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Transient recovery", DefaultBackend: BackendCodex})
	created, _ := tasks.Create(TaskInput{ProjectID: project.ProjectID, Backend: BackendCodex, ConversationNumber: 1, TargetID: "thread-1", Description: "transient"})
	created.Status, created.DispatchState, created.CurrentRunID = StatusRunning, DispatchDispatched, "run-existing"
	_, _ = tasks.Update(created)

	service.Recover(context.Background())
	deferred, _ := tasks.Get(created.TaskNumber)
	if deferred.Status != StatusRunning || deferred.LastError != "" {
		t.Fatalf("transient lookup changed task=%#v", deferred)
	}

	adapter.resolveErr = nil
	adapter.active = true
	service.Recover(context.Background())
	recovered, _ := tasks.Get(created.TaskNumber)
	if recovered.Status != StatusRunning || recovered.CurrentRunID != "run-existing" {
		t.Fatalf("recovery did not reconcile after transient lookup task=%#v", recovered)
	}
}

func TestTaskServiceRemoteCommandsShareCoreHandler(t *testing.T) {
	adapter := &fakeTaskAdapter{backend: BackendCodex, refs: []ConversationRef{{Backend: BackendCodex, Number: 1, TargetID: "thread-1"}}}
	service, tasks, projects := newTestService(t, adapter)
	project, _ := projects.Create(ProjectInput{Name: "Demo", Aliases: []string{"demo"}, DefaultBackend: BackendCodex, DefaultConversationNumber: intPointer(1)})
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "telegram", ChannelProfileID: "profile", AccountID: "account", ConversationType: "private", ChatID: "chat", UserID: "user"}, UserID: "user"}
	commands := commandregistry.NewInMemory()
	run := func(text string) string {
		t.Helper()
		invocation, ok := commands.Resolve(text)
		if !ok {
			t.Fatalf("command not registered: %s", text)
		}
		result, handled, err := service.ExecuteRemoteCommand(context.Background(), message, invocation)
		if err != nil || !handled {
			t.Fatalf("command %s result=%q handled=%v err=%v", text, result, handled, err)
		}
		return result
	}
	if !strings.Contains(run("/project demo"), "已切换项目：Demo") {
		t.Fatal("project command did not select context")
	}
	if !strings.Contains(run("/new demo first task"), "T1") {
		t.Fatal("new command did not create T1")
	}
	if !strings.Contains(run("/tasks running"), "T1") {
		t.Fatal("tasks command did not list the running task")
	}
	if !strings.Contains(run("/task 1"), "T1") {
		t.Fatal("task command did not show detail")
	}
	if cancelResult := run("/cancel 1"); !strings.Contains(cancelResult, "已取消 T1") {
		t.Fatalf("cancel result=%q", cancelResult)
	}
	continuedResult := run("/continue 1 second task")
	if !strings.Contains(continuedResult, "T2") {
		t.Fatalf("continue result=%q", continuedResult)
	}
	continued, _ := tasks.Get(2)
	if continued.ParentTaskNumber != 1 {
		t.Fatalf("continued task=%#v", continued)
	}
	service.HandleEvent(events.Event{EventType: events.TurnFailed, ThreadID: continued.TargetID, TurnID: continued.CurrentRunID, Payload: map[string]any{"error": "retry me"}})
	if !strings.Contains(run("/tasks failed"), "T2") {
		t.Fatal("tasks failed filter did not list T2")
	}
	if !strings.Contains(run("/retry 2"), "T3") {
		t.Fatal("retry command did not create T3")
	}
	retried, _ := tasks.Get(3)
	if retried.RetryOfTaskNumber != 2 {
		t.Fatalf("retried task=%#v", retried)
	}
	service.HandleEvent(events.Event{EventType: events.InteractionRequested, ThreadID: retried.TargetID, TurnID: retried.CurrentRunID, Payload: map[string]any{
		"interaction": map[string]any{"id": "remote-input", "questions": []any{map[string]any{"text": "继续吗？"}}},
	}})
	if !strings.Contains(run("/tasks waiting"), "T3") {
		t.Fatal("tasks waiting filter did not list T3")
	}
	service.HandleEvent(events.Event{EventType: events.InteractionResolved, ThreadID: retried.TargetID, TurnID: retried.CurrentRunID, Payload: map[string]any{"interaction": map[string]any{"id": "remote-input"}}})
	service.HandleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: retried.TargetID, TurnID: retried.CurrentRunID, Payload: map[string]any{"phase": "final_answer", "text": "completed from remote command test"}})
	service.HandleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: retried.TargetID, TurnID: retried.CurrentRunID, Payload: map[string]any{"status": "persisted"}})
	if !strings.Contains(run("/tasks completed"), "T3") || !strings.Contains(run("/tasks"), "T3") {
		t.Fatal("tasks completed/all filters did not list T3")
	}
	if !strings.Contains(run("/projects"), "Demo") {
		t.Fatal("projects command did not list project")
	}
	if _, handled, err := service.ExecuteRemoteCommand(context.Background(), message, mustInvocation(t, commands, "/cancel")); err != nil || handled {
		t.Fatalf("bare cancel should fall through to legacy interaction handling: handled=%v err=%v", handled, err)
	}
	_ = project
}

func mustInvocation(t *testing.T, registry *commandregistry.Registry, text string) commandregistry.Invocation {
	t.Helper()
	invocation, ok := registry.Resolve(text)
	if !ok {
		t.Fatalf("command not registered: %s", text)
	}
	return invocation
}

func mustTasks(t *testing.T) *TaskRegistry {
	t.Helper()
	tasks, err := NewTaskRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return tasks
}

func intPointer(value int) *int { return &value }
