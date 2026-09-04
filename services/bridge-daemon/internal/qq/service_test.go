package qq

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
)

type fakeServiceAdapter struct {
	status AdapterStatus
	sent   []channels.OutboundMessage
}

func (f *fakeServiceAdapter) Configure(request ConfigureRequest) (AdapterStatus, error) {
	return f.status, nil
}
func (f *fakeServiceAdapter) QQStatus() AdapterStatus { return f.status }
func (f *fakeServiceAdapter) SetBindingCount(count int) {
	f.status.BindingCount = count
}
func (f *fakeServiceAdapter) Start(context.Context) error { return nil }
func (f *fakeServiceAdapter) Stop(context.Context) error  { return nil }
func (f *fakeServiceAdapter) ClearToken(context.Context) error {
	return nil
}
func (f *fakeServiceAdapter) Test(context.Context, TestRequest) TestResult {
	return TestResult{OK: true}
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
}

func (f *fakeRuntime) Status() bridgeruntime.Status { return f.status }
func (f *fakeRuntime) RuntimeState(string) control.RuntimeState {
	return f.state
}
func (f *fakeRuntime) StartTurn(_ context.Context, threadID string, _ control.StartTurnRequest) (control.TurnAccepted, error) {
	f.startCount++
	result := f.accepted
	if result.ThreadID == "" {
		result = control.TurnAccepted{ThreadID: threadID, TurnID: "turn-1"}
	}
	return result, nil
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
	adapter := &fakeServiceAdapter{status: AdapterStatus{SelfID: "100", SendProgressUpdates: true}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service := &Service{
		transport: adapter, control: controlService, runtime: runtime, bindings: repository, broker: events.NewBroker(),
		ctx: ctx, cancel: cancel, done: make(chan struct{}), routes: make(map[string]*turnRoute), selections: make(map[string]threadSelection),
		flows: make(map[string]*interactionFlow), flowByInput: make(map[string]string),
	}
	return service, adapter, repository
}

func TestThreadsCacheAndBindAreConversationScoped(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-123456789", Title: "Work", CWD: `C:\work\project`, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	controlService := &fakeControl{threads: []control.ThreadSummary{thread}}
	runtime := &fakeRuntime{}
	service, _, repository := newServiceFixture(t, controlService, runtime)
	private := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200"}, UserID: "200"}
	group := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "group", ChatID: "200"}, UserID: "300"}
	service.listThreads(context.Background(), private)
	service.bind(context.Background(), private, "1")
	if _, ok := repository.FindAddress("qq", "100", "private", "200", ""); !ok {
		t.Fatal("private binding was not created")
	}
	service.bind(context.Background(), group, "1")
	if _, ok := repository.FindAddress("qq", "100", "group", "200", ""); ok {
		t.Fatal("group must not reuse the private address selection cache")
	}
	service.listThreads(context.Background(), group)
	service.bind(context.Background(), group, "1")
	if _, ok := repository.FindAddress("qq", "100", "group", "200", ""); !ok {
		t.Fatal("group binding was not created from its own selection cache")
	}
}

func TestBusyThreadRejectsWithoutStartingTurn(t *testing.T) {
	thread := control.ThreadSummary{ThreadID: "thread-1"}
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", State: "running", CanSend: false}}
	service, adapter, repository := newServiceFixture(t, &fakeControl{threads: []control.ThreadSummary{thread}}, runtime)
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	message := channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200"}, UserID: "200"}
	service.startTurn(context.Background(), message, "hello")
	if runtime.startCount != 0 || len(adapter.sent) == 0 {
		t.Fatalf("startCount=%d sent=%d", runtime.startCount, len(adapter.sent))
	}
}

func TestStopRequiresExactAddressUserBindingAndRuntimeOwnership(t *testing.T) {
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", TurnID: "turn-1", Origin: "local", CanInterrupt: true}}
	service, _, repository := newServiceFixture(t, &fakeControl{}, runtime)
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qq", AccountID: "100", ConversationType: "group", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	address := channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "group", ChatID: "200"}
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

func TestOnlyPersistedTurnCompletedDeliversFormalAssistantText(t *testing.T) {
	detail := control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread-1", HistoryMode: "legacy"}, Turns: []control.Turn{{TurnID: "turn-1", Status: "completed", Items: []control.Item{{Type: "agentMessage", Role: "assistant", Text: "formal answer"}}}}}
	service, adapter, repository := newServiceFixture(t, &fakeControl{detail: detail}, &fakeRuntime{})
	address := channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200"}
	_, _, err := repository.UpsertAddress(bindings.CreateRequest{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	route := &turnRoute{Address: address, UserID: "200", ThreadID: "thread-1", TurnID: "turn-1", LastProgress: map[string]time.Time{}}
	service.routes["turn-1"] = route
	service.handleEvent(events.Event{EventType: events.AssistantCompleted, ThreadID: "thread-1", TurnID: "turn-1"})
	if len(adapter.sent) != 0 {
		t.Fatal("assistant completion was delivered before persistence")
	}
	service.handleEvent(events.Event{EventType: events.TurnCompleted, ThreadID: "thread-1", TurnID: "turn-1", Payload: map[string]any{"status": "persisted"}})
	if len(adapter.sent) != 1 || adapter.sent[0].Text != "任务完成\nformal answer" {
		t.Fatalf("messages=%#v", adapter.sent)
	}
}

func TestFinalAssistantTextSelectsExplicitPhaseFromMultipleAssistantItems(t *testing.T) {
	thread := control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread-1", HistoryMode: "paginated"}, Turns: []control.Turn{{
		TurnID: "turn-1", Status: "completed", Items: []control.Item{
			{Type: "agentMessage", Role: "assistant", Phase: "commentary", Text: "PROGRESS-1"},
			{Type: "agentMessage", Role: "assistant", Text: "WRONG-PROGRESS"},
			{Type: "agentMessage", Role: "assistant", Phase: "final_answer", Text: "CORRECT-FINAL"},
		},
	}}}
	if got := finalAssistantText(thread, "turn-1"); got != "CORRECT-FINAL" {
		t.Fatalf("QQ direct final path selected %q", got)
	}
	thread.Turns[0].Items = thread.Turns[0].Items[:2]
	if got := finalAssistantText(thread, "turn-1"); got != "" {
		t.Fatalf("QQ direct paginated path accepted progress %q", got)
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
	address := channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "group", ChatID: "200"}
	service.routes["turn-1"] = &turnRoute{Address: address, UserID: "300", ThreadID: "thread-1", TurnID: "turn-1", LastProgress: map[string]time.Time{}}
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
	address := channels.ChannelAddress{ChannelType: "qq", AccountID: "100", ConversationType: "private", ChatID: "200"}
	service.routes["turn"] = &turnRoute{Address: address, UserID: "200", ThreadID: "thread", TurnID: "turn", LastProgress: map[string]time.Time{}}
	service.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: "thread", TurnID: "turn", Payload: map[string]any{"interaction": interaction}})
	service.answerInteraction(context.Background(), channels.InboundMessage{Address: address, UserID: "200"}, "2")
	if service.flows["input"].Index != 0 || len(runtime.responses) != 0 {
		t.Fatal("invalid answer advanced or submitted the flow")
	}
}
