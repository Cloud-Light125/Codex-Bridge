package qqbot

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgequery "cloudlight.dev/codexbridge/bridge-daemon/internal/query"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

type fakeServiceAdapter struct {
	status AdapterStatus
	sent   []channels.OutboundMessage
}

func (f *fakeServiceAdapter) Configure(request ConfigureRequest) (AdapterStatus, error) {
	return f.status, nil
}
func (f *fakeServiceAdapter) QQBotStatus() AdapterStatus { return f.status }
func (f *fakeServiceAdapter) SetBindingCount(count int) {
	f.status.BindingCount = count
}
func (f *fakeServiceAdapter) Start(context.Context) error { return nil }
func (f *fakeServiceAdapter) Stop(context.Context) error  { return nil }
func (f *fakeServiceAdapter) ClearSecret(context.Context) error {
	return nil
}
func (f *fakeServiceAdapter) Test(context.Context, TestRequest) TestResult {
	return TestResult{Success: true}
}
func (f *fakeServiceAdapter) SendMessage(_ context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	f.sent = append(f.sent, message)
	return channels.OutboundResult{MessageID: "message-1"}, nil
}

type fakeControl struct {
	threads []control.ThreadSummary
	detail  control.ThreadDetail
}

func (f *fakeControl) ListThreads(context.Context, int, string) (control.ThreadList, error) {
	return control.ThreadList{Threads: append([]control.ThreadSummary(nil), f.threads...)}, nil
}
func (f *fakeControl) ReadThread(_ context.Context, threadID string, _ bool) (control.ThreadDetail, error) {
	if f.detail.ThreadID == threadID {
		return f.detail, nil
	}
	for _, thread := range f.threads {
		if thread.ThreadID == threadID {
			return control.ThreadDetail{ThreadSummary: thread}, nil
		}
	}
	return control.ThreadDetail{}, nil
}

type fakeRuntime struct {
	status       bridgeruntime.Status
	state        control.RuntimeState
	accepted     control.TurnAccepted
	startCount   int
	interrupts   int
	interactions map[string]interactions.PendingInteraction
	responses    []interactions.ResponseRequest
	lastThreadID string
}

func (f *fakeRuntime) Status() bridgeruntime.Status { return f.status }
func (f *fakeRuntime) RuntimeState(string) control.RuntimeState {
	return f.state
}
func (f *fakeRuntime) StartTurn(_ context.Context, threadID string, _ control.StartTurnRequest) (control.TurnAccepted, error) {
	f.startCount++
	f.lastThreadID = threadID
	result := f.accepted
	if result.ThreadID == "" {
		result = control.TurnAccepted{ThreadID: threadID, TurnID: "turn-1"}
	}
	return result, nil
}

func TestStableNumberRoutesWithoutBinding(t *testing.T) {
	first := control.ThreadSummary{ThreadID: "thread-1", Title: "One"}
	second := control.ThreadSummary{ThreadID: "thread-2", Title: "Two"}
	runtime := &fakeRuntime{state: control.RuntimeState{State: "idle", CanSend: true}}
	service, _, _ := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{first, second}}, runtime)
	registry, err := threadregistry.New(filepath.Join(t.TempDir(), "thread-numbers.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = registry.EnsureBatch([]threadregistry.Metadata{{ThreadID: "thread-1", CreatedAt: "2026-01-01T00:00:00Z"}, {ThreadID: "thread-2", CreatedAt: "2026-01-02T00:00:00Z"}})
	service.registry = registry
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}, UserID: "200", Text: "#2 do it"}
	service.HandleMessage(context.Background(), message)
	if runtime.startCount != 1 || runtime.lastThreadID != "thread-2" {
		t.Fatalf("starts=%d thread=%q", runtime.startCount, runtime.lastThreadID)
	}
}

func TestMultiplePendingInteractionsRouteByThread(t *testing.T) {
	now := time.Now().Add(time.Minute)
	runtime := &fakeRuntime{interactions: map[string]interactions.PendingInteraction{
		"i1": {ID: "i1", Kind: interactions.KindUserInput, ThreadID: "thread-1", TurnID: "turn-1", Status: "pending"},
		"i2": {ID: "i2", Kind: interactions.KindUserInput, ThreadID: "thread-2", TurnID: "turn-2", Status: "pending"},
	}}
	service, _, _ := newServiceFixture(t, &fakeControl{}, runtime)
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}
	service.flows["i1"] = &interactionFlow{Expires: now, Address: address, UserID: "200", ThreadID: "thread-1", TurnID: "turn-1", InteractionID: "i1", Questions: []interactions.Question{{ID: "q1", Type: "text"}}, Answers: map[string][]string{}}
	service.flows["i2"] = &interactionFlow{Expires: now, Address: address, UserID: "200", ThreadID: "thread-2", TurnID: "turn-2", InteractionID: "i2", Questions: []interactions.Question{{ID: "q2", Type: "text"}}, Answers: map[string][]string{}}
	message := channels.InboundMessage{Address: address, UserID: "200"}
	if !service.answerInteractionForThread(context.Background(), message, "one", "thread-1") || !service.answerInteractionForThread(context.Background(), message, "two", "thread-2") {
		t.Fatal("targeted answers were not consumed")
	}
	if len(runtime.responses) != 2 || runtime.responses[0].Answers["q1"][0] != "one" || runtime.responses[1].Answers["q2"][0] != "two" {
		t.Fatalf("responses=%#v", runtime.responses)
	}
}

func TestCodexDisconnectClearsOnlyCodexInteractionState(t *testing.T) {
	service, _, _ := newServiceFixture(t, &fakeControl{}, &fakeRuntime{})
	codexAddress := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "codex"}
	openClawAddress := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "openclaw"}
	service.routes["codex-turn"] = &turnRoute{Address: codexAddress, UserID: "200", ThreadID: "thread-codex", TurnID: "codex-turn"}
	service.routes["openclaw-run"] = &turnRoute{Address: openClawAddress, UserID: "201", Backend: conversation.BackendOpenClaw, ThreadID: "agent:main:main", TurnID: "openclaw-run"}
	service.flows["codex-interaction"] = &interactionFlow{Address: codexAddress, UserID: "200", TurnID: "codex-turn", InteractionID: "codex-interaction"}
	service.flows["openclaw-interaction"] = &interactionFlow{Address: openClawAddress, UserID: "201", TurnID: "openclaw-run", InteractionID: "openclaw-interaction"}
	service.flowByInput[inputKey(codexAddress, "200")] = "codex-interaction"
	service.flowByInput[inputKey(openClawAddress, "201")] = "openclaw-interaction"
	service.interactionNotified = map[string]bool{"codex-interaction": true, "codex-approval": true}

	service.handleEvent(events.Event{EventType: events.CodexDisconnected})

	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.routes["codex-turn"]; ok {
		t.Fatal("Codex route remained after Codex disconnect")
	}
	if _, ok := service.routes["openclaw-run"]; !ok {
		t.Fatal("OpenClaw route was removed by Codex disconnect")
	}
	if _, ok := service.flows["codex-interaction"]; ok {
		t.Fatal("Codex interaction flow remained after Codex disconnect")
	}
	if _, ok := service.flowByInput[inputKey(codexAddress, "200")]; ok {
		t.Fatal("Codex interaction input route remained after Codex disconnect")
	}
	if service.flowByInput[inputKey(openClawAddress, "201")] != "openclaw-interaction" {
		t.Fatalf("OpenClaw input route was changed: %#v", service.flowByInput)
	}
	if service.interactionNotified["codex-interaction"] {
		t.Fatal("Codex interaction notification remained after Codex disconnect")
	}
	if len(service.interactionNotified) != 0 {
		t.Fatalf("Codex interaction notifications remained after disconnect: %#v", service.interactionNotified)
	}
}

func (f *fakeRuntime) InterruptTurn(context.Context, string, string) (control.InterruptResult, error) {
	f.interrupts++
	return control.InterruptResult{}, nil
}
func (f *fakeRuntime) GetInteraction(id string) (interactions.PendingInteraction, bool) {
	item, ok := f.interactions[id]
	return item, ok
}
func (f *fakeRuntime) ListInteractions(string) []interactions.PendingInteraction { return nil }
func (f *fakeRuntime) RespondInteraction(_ context.Context, id string, response interactions.ResponseRequest) (interactions.PendingInteraction, error) {
	f.responses = append(f.responses, response)
	item := f.interactions[id]
	item.Status = response.Action
	f.interactions[id] = item
	return item, nil
}
func (f *fakeRuntime) VerifyThreadPersistence(context.Context, string) (control.PersistenceVerification, error) {
	return control.PersistenceVerification{}, nil
}

func newServiceFixture(t *testing.T, controlService Control, runtime Runtime) (*Service, *fakeServiceAdapter, *bindings.Repository) {
	t.Helper()
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	adapter := &fakeServiceAdapter{status: AdapterStatus{AppID: "100", SendProgressUpdates: true}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service := &Service{
		transport: adapter, control: controlService, runtime: runtime, bindings: repository, broker: events.NewBroker(),
		ctx: ctx, cancel: cancel, done: make(chan struct{}), routes: make(map[string]*turnRoute), selections: make(map[string]threadSelection),
		flows: make(map[string]*interactionFlow), flowByInput: make(map[string]string),
	}
	return service, adapter, repository
}

func TestQQQueriesUseSharedHelpAndNeverStartTurn(t *testing.T) {
	runtime := &fakeRuntime{}
	service, adapter, _ := newServiceFixture(t, &fakeControl{}, runtime)
	result, handled := service.queryService().Execute(context.Background(), "/help")
	if !handled || len(result.Parts) != 1 || result.Parts[0] != bridgequery.HelpText {
		t.Fatalf("QQ help did not use shared query semantics: %#v", result)
	}
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}}
	service.handleCommand(context.Background(), message, "/running")
	if runtime.startCount != 0 {
		t.Fatalf("query started %d Codex Turns", runtime.startCount)
	}
	if len(adapter.sent) != 1 || adapter.sent[0].Text != "当前没有正在执行的 Codex 任务。" {
		t.Fatalf("unexpected QQ query output: %#v", adapter.sent)
	}
}

func TestQQCustomCommandAndNumberedContextUseSharedRegistry(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-1", Number: 1, Title: "One", Status: "idle"}
	service, adapter, _ := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{thread}}, &fakeRuntime{})
	commands, err := commandregistry.New(filepath.Join(t.TempDir(), "commands.json"))
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	if _, err := commands.Create(commandregistry.Mutation{Name: "/任务", DisplayName: "查看正在执行", Description: "查看任务", Action: commandregistry.ActionThreadRunning, Enabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	if _, err := commands.Create(commandregistry.Mutation{Name: "/详情", DisplayName: "查看会话详情", Description: "查看会话", Action: commandregistry.ActionThreadInfo, Enabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	numbers, err := threadregistry.New(filepath.Join(t.TempDir(), "thread-numbers.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := numbers.Ensure(threadregistry.Metadata{ThreadID: "thread-1", Title: "One", CreatedAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	service.registry = numbers
	service.SetCommandRegistry(commands)
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}
	service.HandleMessage(context.Background(), channels.InboundMessage{Address: address, UserID: "200", Text: "/任务"})
	service.HandleMessage(context.Background(), channels.InboundMessage{Address: address, UserID: "200", Text: "#1 /详情"})
	if len(adapter.sent) != 2 || adapter.sent[0].Text != "当前没有正在执行的 Codex 任务。" || !strings.Contains(adapter.sent[1].Text, "#1 One") {
		t.Fatalf("unexpected custom command output: %#v", adapter.sent)
	}
}

func TestThreadsCacheAndBindAreConversationScoped(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-123456789", Title: "Work", CWD: `C:\work\project`, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	controlService := &fakeControl{threads: []control.ThreadSummary{thread}}
	runtime := &fakeRuntime{}
	service, _, repository := newServiceFixture(t, controlService, runtime)
	private := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}, UserID: "200"}
	group := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "group", ChatID: "200"}, UserID: "300"}
	service.listThreads(context.Background(), private)
	service.bind(context.Background(), private, "1")
	if _, ok := repository.FindAddress("qqbot", "100", "c2c", "200", ""); !ok {
		t.Fatal("private binding was not created")
	}
	service.bind(context.Background(), group, "1")
	if _, ok := repository.FindAddress("qqbot", "100", "group", "200", ""); ok {
		t.Fatal("group must not reuse the private address selection cache")
	}
	service.listThreads(context.Background(), group)
	service.bind(context.Background(), group, "1")
	if _, ok := repository.FindAddress("qqbot", "100", "group", "200", ""); !ok {
		t.Fatal("group binding was not created from its own selection cache")
	}
}

func TestQQCurrentAndThreadsFollowExplicitOpenClawBinding(t *testing.T) {
	service, adapter, repository := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{{ThreadID: "codex-thread", Title: "Codex only"}}}, &fakeRuntime{})
	service.SetOpenClawBackend(&fakeOpenClawBackend{})
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}
	if _, _, err := repository.UpsertAddress(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, TargetID: "agent:main:main", ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c",
		ConversationID: "200", ChatID: "200", ThreadID: "agent:main:main", SessionKey: "agent:main:main",
	}); err != nil {
		t.Fatal(err)
	}
	message := channels.InboundMessage{Address: address, UserID: "200"}
	service.listThreads(context.Background(), message)
	if len(adapter.sent) != 1 || !strings.Contains(adapter.sent[0].Text, "[OpenClaw]") || !strings.Contains(adapter.sent[0].Text, "agent:main:main") {
		t.Fatalf("/threads did not list the current OpenClaw backend sessions: %#v", adapter.sent)
	}
	service.current(context.Background(), message)
	if len(adapter.sent) != 2 || !strings.Contains(adapter.sent[1].Text, "后端：OpenClaw") || !strings.Contains(adapter.sent[1].Text, "agent:main:main") {
		t.Fatalf("/current did not report explicit backend and target: %#v", adapter.sent)
	}
}

func TestQQBindDelegatesToProfilePreparer(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-1", Title: "One"}
	service, adapter, repository := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{thread}}, &fakeRuntime{})
	service.SetBindingPreparer(func(bindings.CreateRequest) (bindings.CreateRequest, error) {
		return bindings.CreateRequest{}, errors.New("not routed to codex")
	})
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}, UserID: "200"}
	service.bind(context.Background(), message, "thread-1")
	if _, ok := repository.FindAddress("qqbot", "100", "c2c", "200", ""); ok {
		t.Fatal("a rejected profile route created a QQ binding")
	}
	if len(adapter.sent) != 1 || !strings.Contains(adapter.sent[0].Text, "未分配给 Codex") {
		t.Fatalf("profile routing rejection was not surfaced to the QQ user: %#v", adapter.sent)
	}
}

func TestParseBindingTargetKeepsBackendExplicit(t *testing.T) {
	backend, target := parseBindingTarget("openclaw agent:main:main")
	if backend != conversation.BackendOpenClaw || target != "agent:main:main" {
		t.Fatalf("explicit OpenClaw target parsed as backend=%q target=%q", backend, target)
	}
	backend, target = parseBindingTarget("codex thread-123")
	if backend != conversation.BackendCodex || target != "thread-123" {
		t.Fatalf("explicit Codex target parsed as backend=%q target=%q", backend, target)
	}
}

func TestBusyThreadRejectsWithoutStartingTurn(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-1"}
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", State: "running", CanSend: false}}
	service, adapter, repository := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{thread}}, runtime)
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}, UserID: "200"}
	service.startTurn(context.Background(), message, "hello")
	if runtime.startCount != 0 || len(adapter.sent) == 0 {
		t.Fatalf("startCount=%d sent=%d", runtime.startCount, len(adapter.sent))
	}
}

func TestStopRequiresExactAddressUserBindingAndRuntimeOwnership(t *testing.T) {
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", TurnID: "turn-1", Origin: "local", CanInterrupt: true}}
	service, _, repository := newServiceFixture(t, &fakeControl{}, runtime)
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qqbot", AccountID: "100", ConversationType: "group", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "group", ChatID: "200"}
	service.routes["turn-1"] = &turnRoute{Address: address, UserID: "300", ThreadID: "thread-1", TurnID: "turn-1"}
	service.stopTurn(context.Background(), channels.InboundMessage{Address: address, UserID: "301"})
	if runtime.interrupts != 0 {
		t.Fatal("another group user interrupted the turn")
	}
	service.stopTurn(context.Background(), channels.InboundMessage{Address: address, UserID: "300"})
	if runtime.interrupts != 1 {
		t.Fatalf("interrupts=%d; want 1", runtime.interrupts)
	}
}

func TestPersistedTurnCompletionIsLeftToMirrorService(t *testing.T) {
	detail := control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread-1"}, Turns: []control.Turn{{TurnID: "turn-1", Items: []control.Item{{Type: "agentMessage", Role: "assistant", Text: "formal answer"}}}}}
	service, adapter, repository := newServiceFixture(t, &fakeControl{detail: detail}, &fakeRuntime{})
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	route := &turnRoute{Address: address, UserID: "200", ThreadID: "thread-1", TurnID: "turn-1"}
	service.routes["turn-1"] = route
	service.handleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread-1", TurnID: "turn-1"})
	if len(adapter.sent) != 0 {
		t.Fatal("assistant completion was delivered before persistence")
	}
	service.handleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread-1", TurnID: "turn-1", Payload: map[string]any{"status": "persisted"}})
	if len(adapter.sent) != 0 {
		t.Fatalf("QQ route duplicated the mirror service final: %#v", adapter.sent)
	}
	if _, exists := service.routes["turn-1"]; exists {
		t.Fatal("completed route was not released")
	}
}

func TestQQUserInputSingleMultiTextAndCancel(t *testing.T) {
	questions := []interactions.Question{
		{ID: "single", Type: "single-choice", Required: true, Options: []interactions.QuestionOption{{Value: "a"}, {Value: "b"}}},
		{ID: "multi", Type: "multiple-choice", Required: true, Options: []interactions.QuestionOption{{Value: "x"}, {Value: "y"}}},
		{ID: "text", Type: "text", Required: true},
	}
	interaction := interactions.PendingInteraction{ID: "input-1", Kind: interactions.KindUserInput, ThreadID: "thread-1", TurnID: "turn-1", Questions: questions, Status: "pending", ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	runtime := &fakeRuntime{interactions: map[string]interactions.PendingInteraction{"input-1": interaction}}
	service, _, _ := newServiceFixture(t, &fakeControl{}, runtime)
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "group", ChatID: "200"}
	service.routes["turn-1"] = &turnRoute{Address: address, UserID: "300", ThreadID: "thread-1", TurnID: "turn-1"}
	service.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread-1", TurnID: "turn-1", Payload: map[string]any{"interaction": interaction}})
	owner := channels.InboundMessage{Address: address, UserID: "300"}
	if !service.answerInteraction(context.Background(), owner, "2") || !service.answerInteraction(context.Background(), owner, "1，2") || !service.answerInteraction(context.Background(), owner, "free text") {
		t.Fatal("owned answers were not consumed")
	}
	if len(runtime.responses) != 1 || runtime.responses[0].Action != "submit" || runtime.responses[0].Answers["single"][0] != "b" || len(runtime.responses[0].Answers["multi"]) != 2 || runtime.responses[0].Answers["text"][0] != "free text" {
		t.Fatalf("response=%#v", runtime.responses)
	}

	interaction.ID = "input-2"
	runtime.interactions[interaction.ID] = interaction
	service.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread-1", TurnID: "turn-1", Payload: map[string]any{"interaction": interaction}})
	other := channels.InboundMessage{Address: address, UserID: "301", Text: "not my answer"}
	service.HandleMessage(context.Background(), other)
	if len(runtime.responses) != 1 {
		t.Fatal("another group user answered the interaction")
	}
	service.cancelInteraction(context.Background(), owner)
	if len(runtime.responses) != 2 || runtime.responses[1].Action != "cancel" {
		t.Fatalf("cancel response=%#v", runtime.responses)
	}
}

func TestInvalidAnswerDoesNotAdvanceFlow(t *testing.T) {
	question := interactions.Question{ID: "q", Type: "single-choice", Required: true, Options: []interactions.QuestionOption{{Value: "a"}}}
	interaction := interactions.PendingInteraction{ID: "input", Kind: interactions.KindUserInput, ThreadID: "thread", TurnID: "turn", Questions: []interactions.Question{question}, Status: "pending", ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	runtime := &fakeRuntime{interactions: map[string]interactions.PendingInteraction{"input": interaction}}
	service, _, _ := newServiceFixture(t, &fakeControl{}, runtime)
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "100", ConversationType: "c2c", ChatID: "200"}
	service.routes["turn"] = &turnRoute{Address: address, UserID: "200", ThreadID: "thread", TurnID: "turn"}
	service.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread", TurnID: "turn", Payload: map[string]any{"interaction": interaction}})
	service.answerInteraction(context.Background(), channels.InboundMessage{Address: address, UserID: "200"}, "2")
	if service.flows["input"].Index != 0 || len(runtime.responses) != 0 {
		t.Fatal("invalid answer advanced or submitted the flow")
	}
}

type fakeOpenClawBackend struct {
	sends  []string
	aborts []string
}

func (f *fakeOpenClawBackend) Backend() string { return conversation.BackendOpenClaw }
func (f *fakeOpenClawBackend) ListSessions(context.Context, int) ([]conversation.Session, error) {
	return []conversation.Session{{Backend: conversation.BackendOpenClaw, Key: "agent:main:main", Title: "Main", Status: "idle"}}, nil
}
func (f *fakeOpenClawBackend) ReadSession(context.Context, string) (conversation.Detail, error) {
	return conversation.Detail{Session: conversation.Session{Backend: conversation.BackendOpenClaw, Key: "agent:main:main", Title: "Main", Status: "idle"}}, nil
}
func (f *fakeOpenClawBackend) SendMessage(_ context.Context, key, message string) (conversation.SendResult, error) {
	f.sends = append(f.sends, key+":"+message)
	return conversation.SendResult{Backend: conversation.BackendOpenClaw, SessionKey: key, RunID: "openclaw-run-" + strconv.Itoa(len(f.sends)), Status: "running"}, nil
}
func (f *fakeOpenClawBackend) Abort(_ context.Context, key, runID string) (conversation.AbortResult, error) {
	f.aborts = append(f.aborts, key+":"+runID)
	return conversation.AbortResult{Backend: conversation.BackendOpenClaw, SessionKey: key, RunID: runID, Status: "aborted"}, nil
}
func (f *fakeOpenClawBackend) SubscribeEvents(func(conversation.Event)) func() { return func() {} }
func (f *fakeOpenClawBackend) ConnectionStatus() conversation.ConnectionStatus {
	return conversation.ConnectionStatus{Backend: conversation.BackendOpenClaw, Configured: true, Running: true, Connected: true, State: "connected"}
}

func TestQQBotRoutesBoundOpenClawSessionAndFinalOrAbort(t *testing.T) {
	service, adapter, repository := newServiceFixture(t, &fakeControl{}, &fakeRuntime{})
	backend := &fakeOpenClawBackend{}
	service.SetOpenClawBackend(backend)
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: "app-1", ConversationType: "c2c", ChatID: "openid-1"}
	if _, _, err := repository.UpsertAddress(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, ChannelType: "qqbot", AccountID: "app-1", ConversationType: "c2c",
		ChatID: "openid-1", ThreadID: "agent:main:main", SessionKey: "agent:main:main",
	}); err != nil {
		t.Fatal(err)
	}
	message := channels.InboundMessage{Address: address, UserID: "user-1", Text: "hello OpenClaw"}
	service.startTurn(context.Background(), message, message.Text)
	if len(backend.sends) != 1 || !strings.HasSuffix(backend.sends[0], ":hello OpenClaw") {
		t.Fatalf("OpenClaw send was not routed from QQ: %#v", backend.sends)
	}
	service.handleEvent(events.Event{EventType: events.OpenClawMessageCompleted, ThreadID: "agent:main:main", TurnID: "openclaw-run-1", Payload: map[string]any{"text": "final answer"}})
	if len(adapter.sent) == 0 || adapter.sent[0].Text != "final answer" {
		t.Fatalf("QQ did not receive the OpenClaw final answer: %#v", adapter.sent)
	}

	service.startTurn(context.Background(), message, "second task")
	service.stopTurn(context.Background(), message)
	if len(backend.aborts) != 1 || backend.aborts[0] != "agent:main:main:openclaw-run-2" {
		t.Fatalf("OpenClaw abort was not routed from QQ: %#v", backend.aborts)
	}
	if len(adapter.sent) < 2 || !strings.Contains(adapter.sent[len(adapter.sent)-1].Text, "已请求停止 OpenClaw") {
		t.Fatalf("QQ did not acknowledge OpenClaw stop: %#v", adapter.sent)
	}
}
