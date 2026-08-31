package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgequery "cloudlight.dev/codexbridge/bridge-daemon/internal/query"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestAdapterAllowlistDedupAndTopicRoute(t *testing.T) {
	var mu sync.Mutex
	received := []channels.InboundMessage{}
	adapter := NewAdapter(func(_ context.Context, message channels.InboundMessage) {
		mu.Lock()
		received = append(received, message)
		mu.Unlock()
	})
	token := "123:secret"
	if _, err := adapter.Configure(ConfigureRequest{Token: &token, AllowedUserIDs: []int64{42}, PollingTimeoutSeconds: 10}); err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	adapter.status.BotID = "99"
	adapter.mu.Unlock()
	valid := Update{UpdateID: 7, Message: &Message{MessageID: 8, MessageThreadID: 9, From: &User{ID: 42}, Chat: Chat{ID: -1001}, Text: "hello"}}
	adapter.handleUpdate(context.Background(), valid, 99)
	adapter.handleUpdate(context.Background(), valid, 99)
	adapter.handleUpdate(context.Background(), Update{UpdateID: 8, Message: &Message{From: &User{ID: 43}, Chat: Chat{ID: -1001}, Text: "blocked"}}, 99)
	adapter.handleUpdate(context.Background(), Update{UpdateID: 9, Message: &Message{From: &User{ID: 99, IsBot: true}, Chat: Chat{ID: -1001}, Text: "self"}}, 99)
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("received %d messages, want 1", len(received))
	}
	if received[0].Address.ChatID != "-1001" || received[0].Address.TopicID != "9" || received[0].UserID != "42" {
		t.Fatalf("unexpected route: %#v", received[0])
	}
}

func TestTelegramAddressNormalizesLegacyConversationType(t *testing.T) {
	legacy := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1", TopicID: "2"}
	explicit := legacy
	explicit.ConversationType = "default"
	if !sameAddress(legacy, explicit) {
		t.Fatal("legacy empty Telegram conversationType must match explicit default")
	}
	if waitKey(legacy, "42") != waitKey(explicit, "42") {
		t.Fatal("Telegram wait key must normalize the default conversation type")
	}
	different := explicit
	different.ConversationType = "group"
	if sameAddress(explicit, different) {
		t.Fatal("different explicit conversation types must not match")
	}
}

func TestConfigureDeduplicatesAllowlist(t *testing.T) {
	adapter := NewAdapter(nil)
	status, err := adapter.Configure(ConfigureRequest{AllowedUserIDs: []int64{42, 42}, PollingTimeoutSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.AllowedUserIDs) != 1 || status.AllowedUserIDs[0] != 42 {
		t.Fatalf("allowlist was not deduplicated: %#v", status.AllowedUserIDs)
	}
}

func TestTelegramTokenDoesNotAppearInAPIError(t *testing.T) {
	const token = "123456:very-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{"ok": false, "error_code": 400, "description": "bad token " + token})
	}))
	defer server.Close()
	client := newClientForTest(token, server.URL, server.Client())
	_, err := client.GetMe(context.Background())
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestTelegramTokenDoesNotAppearInTransportError(t *testing.T) {
	const token = "123456:transport-secret"
	client := newClientForTest(token, "https://telegram.invalid", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed: " + request.URL.String() + " authorization=" + request.Header.Get("Authorization"))
	})})
	_, err := client.GetMe(context.Background())
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "telegram.invalid") {
		t.Fatalf("unsafe transport error: %v", err)
	}
}

func TestProxyModesUseDedicatedTransports(t *testing.T) {
	original := http.DefaultTransport
	environment, err := NewClientWithProxy("token", ProxyConfig{Mode: ""})
	if err != nil {
		t.Fatal(err)
	}
	direct, err := NewClientWithProxy("token", ProxyConfig{Mode: ProxyModeDirect})
	if err != nil {
		t.Fatal(err)
	}
	custom, err := NewClientWithProxy("token", ProxyConfig{Mode: ProxyModeCustomHTTP, URL: "http://proxy.example:8080/"})
	if err != nil {
		t.Fatal(err)
	}
	environmentTransport := environment.httpClient.Transport.(*http.Transport)
	directTransport := direct.httpClient.Transport.(*http.Transport)
	customTransport := custom.httpClient.Transport.(*http.Transport)
	if http.DefaultTransport != original || environmentTransport == original || directTransport == original || customTransport == original {
		t.Fatal("Telegram client changed or reused the global default transport")
	}
	if environmentTransport == directTransport || directTransport == customTransport || environmentTransport.Proxy == nil || directTransport.Proxy != nil {
		t.Fatal("proxy modes did not receive isolated proxy behavior")
	}
	request, _ := http.NewRequest(http.MethodGet, "https://api.telegram.org", nil)
	proxyURL, err := customTransport.Proxy(request)
	if err != nil || proxyURL == nil || proxyURL.String() != "http://proxy.example:8080" {
		t.Fatalf("custom proxy = %v, %v", proxyURL, err)
	}
}

func TestCustomHTTPProxyRejectsUnsafeURLs(t *testing.T) {
	invalid := []string{
		"", "https://proxy.example:8080", "http://user:secret@proxy.example:8080",
		"http://proxy.example:8080/path", "http://proxy.example:8080?token=secret", "http://proxy.example:8080/#secret",
	}
	for _, value := range invalid {
		if _, err := normalizeProxyConfig(ProxyConfig{Mode: ProxyModeCustomHTTP, URL: value}); err == nil {
			t.Errorf("accepted unsafe proxy URL %q", value)
		}
	}
}

func TestGetUpdatesAddsLongPollingDeadline(t *testing.T) {
	remaining := make(chan time.Duration, 2)
	client := newClientForTest("token", "https://telegram.invalid", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		if !ok {
			remaining <- 0
		} else {
			remaining <- time.Until(deadline)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":[]}`)), Header: make(http.Header)}, nil
	})})
	for _, test := range []struct {
		pollSeconds int
		minimum     time.Duration
	}{{pollSeconds: 10, minimum: 44 * time.Second}, {pollSeconds: 60, minimum: 74 * time.Second}} {
		if _, err := client.GetUpdates(context.Background(), 0, test.pollSeconds); err != nil {
			t.Fatal(err)
		}
		if got := <-remaining; got < test.minimum {
			t.Fatalf("getUpdates(%ds) deadline remaining = %s, want at least %s", test.pollSeconds, got, test.minimum)
		}
	}
}

func TestConnectionRefusedCategoryUsesEffectiveProxy(t *testing.T) {
	err := errors.New("connectex: No connection could be made because the target machine actively refused it")
	if got := classifyNetworkFailure(err, false); got != "telegram-unreachable" {
		t.Fatalf("direct refusal category = %q", got)
	}
	if got := classifyNetworkFailure(err, true); got != "proxy-refused" {
		t.Fatalf("proxied refusal category = %q", got)
	}
}

func TestStopCancelsBlockedGetUpdates(t *testing.T) {
	pollStarted := make(chan struct{})
	var once sync.Once
	client := newClientForTest("token", "https://telegram.invalid", &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/getMe") {
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"id":99,"is_bot":true}}`)), Header: make(http.Header)}, nil
		}
		once.Do(func() { close(pollStarted) })
		<-request.Context().Done()
		return nil, request.Context().Err()
	})})
	adapter := NewAdapter(nil)
	adapter.clientFn = func(string, ProxyConfig) (*Client, error) { return client, nil }
	token := "token"
	if _, err := adapter.Configure(ConfigureRequest{Token: &token, AllowedUserIDs: []int64{42}, PollingTimeoutSeconds: 10}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pollStarted:
	case <-time.After(time.Second):
		t.Fatal("getUpdates did not start")
	}
	stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adapter.Stop(stopContext); err != nil {
		t.Fatalf("Stop did not cancel getUpdates: %v", err)
	}
	if adapter.TelegramStatus().Running {
		t.Fatal("adapter remained running after Stop")
	}
	if adapter.TelegramStatus().LastErrorCategory != "" {
		t.Fatalf("Stop cancellation was recorded as an error: %q", adapter.TelegramStatus().LastErrorCategory)
	}
}

func TestProxyProbeTreatsAnyHTTPResponseAsReachable(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.Header.Get("Authorization") != "" {
			t.Fatalf("unsafe proxy probe request: method=%s authorization=%q", request.Method, request.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("unavailable")), Header: make(http.Header)}, nil
	})}
	status, _, err := probeTelegramWithClient(context.Background(), ProxyConfig{Mode: ProxyModeDirect}, client, "https://api.telegram.org")
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("probe status=%d err=%v", status, err)
	}
}

func TestSplitMessageIsRuneSafe(t *testing.T) {
	text := strings.Repeat("界", 4097)
	parts := SplitMessage(text, 3900)
	if len(parts) != 2 {
		t.Fatalf("got %d parts", len(parts))
	}
	for _, part := range parts {
		if len([]rune(part)) > 3900 || strings.ToValidUTF8(part, "") != part {
			t.Fatalf("invalid part length or UTF-8")
		}
	}
	if strings.Join(parts, "") != text {
		t.Fatal("split changed message")
	}
}

type fakeControl struct{ thread control.ThreadDetail }

func (f *fakeControl) ListThreads(context.Context, int, string) (control.ThreadList, error) {
	return control.ThreadList{Threads: []control.ThreadSummary{f.thread.ThreadSummary}}, nil
}
func (f *fakeControl) ReadThread(context.Context, string, bool) (control.ThreadDetail, error) {
	return f.thread, nil
}

type fakeRuntime struct {
	state       control.RuntimeState
	starts      int
	interrupts  int
	verifies    int
	interaction interactions.PendingInteraction
	responses   []interactions.ResponseRequest
}

func (f *fakeRuntime) Status() bridgeruntime.Status {
	return bridgeruntime.Status{AppServerRunning: true}
}
func (f *fakeRuntime) RuntimeState(string) control.RuntimeState { return f.state }
func (f *fakeRuntime) StartTurn(context.Context, string, control.StartTurnRequest) (control.TurnAccepted, error) {
	f.starts++
	return control.TurnAccepted{ThreadID: "thread-1", TurnID: "turn-1"}, nil
}
func (f *fakeRuntime) InterruptTurn(context.Context, string, string) (control.InterruptResult, error) {
	f.interrupts++
	return control.InterruptResult{}, nil
}
func (f *fakeRuntime) GetInteraction(string) (interactions.PendingInteraction, bool) {
	return f.interaction, f.interaction.ID != ""
}
func (f *fakeRuntime) ListInteractions(string) []interactions.PendingInteraction { return nil }
func (f *fakeRuntime) RespondInteraction(_ context.Context, _ string, request interactions.ResponseRequest) (interactions.PendingInteraction, error) {
	f.responses = append(f.responses, request)
	return f.interaction, nil
}
func (f *fakeRuntime) VerifyThreadPersistence(context.Context, string) (control.PersistenceVerification, error) {
	f.verifies++
	return control.PersistenceVerification{}, nil
}

func TestTelegramQueriesUseSharedHelpAndNeverStartTurn(t *testing.T) {
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	runtime := &fakeRuntime{}
	service := NewService(&fakeControl{}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	result, handled := service.queryService().Execute(context.Background(), "/help")
	if !handled || len(result.Parts) != 1 || result.Parts[0] != bridgequery.HelpText {
		t.Fatalf("Telegram help did not use shared query semantics: %#v", result)
	}
	service.handleCommand(context.Background(), channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "telegram", ChatID: "1"}}, "/running")
	commands, err := commandregistry.New(filepath.Join(t.TempDir(), "commands.json"))
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	if _, err := commands.Create(commandregistry.Mutation{Name: "/任务", DisplayName: "查看正在执行", Description: "查看任务", Action: commandregistry.ActionThreadRunning, Enabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	service.SetCommandRegistry(commands)
	if custom, handled := service.queryService().Execute(context.Background(), "/任务"); !handled || len(custom.Parts) != 1 || custom.Parts[0] != "当前没有正在执行的 Codex 任务。" {
		t.Fatalf("Telegram did not resolve custom command: %#v", custom)
	}
	if runtime.starts != 0 {
		t.Fatalf("query started %d Codex Turns", runtime.starts)
	}
}

func TestBusyThreadRejectsBeforeRuntimeStart(t *testing.T) {
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = repository.Create(bindings.CreateRequest{ChannelType: "telegram", AccountID: "99", ChatID: "1", ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", State: "running", CanSend: false}}
	service := NewService(&fakeControl{thread: control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "thread-1"}}}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	service.startTurn(context.Background(), channels.InboundMessage{Address: channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1"}, UserID: "42"}, "hello")
	if runtime.starts != 0 {
		t.Fatalf("runtime starts = %d, want 0", runtime.starts)
	}
}

func TestFreeTextRoutesToExactPendingInteraction(t *testing.T) {
	runtime := &fakeRuntime{interaction: interactions.PendingInteraction{ID: "interaction-1"}}
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	service := NewService(&fakeControl{}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1", TopicID: "2"}
	service.waits[waitKey(address, "42")] = inputWait{Expires: time.Now().Add(time.Minute), Address: address, UserID: "42", ThreadID: "thread-1", TurnID: "turn-1", InteractionID: "interaction-1", QuestionID: "q1"}
	service.flows["interaction-1"] = &interactionFlow{Expires: time.Now().Add(time.Minute), Address: address, UserID: "42", ThreadID: "thread-1", TurnID: "turn-1", InteractionID: "interaction-1", Questions: []interactions.Question{{ID: "q1", Type: "free-text"}}, Answers: map[string][]string{}}
	if !service.answerFreeText(context.Background(), channels.InboundMessage{Address: address, UserID: "42"}, "answer") {
		t.Fatal("message was not consumed")
	}
	if len(runtime.responses) != 1 || runtime.responses[0].Answers["q1"][0] != "answer" {
		t.Fatalf("unexpected responses: %#v", runtime.responses)
	}
}

func TestCodexDisconnectClearsOnlyCodexInteractionState(t *testing.T) {
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	service := NewService(&fakeControl{}, &fakeRuntime{}, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	codexAddress := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "codex"}
	openClawAddress := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "openclaw"}
	service.routes["codex-turn"] = &turnRoute{Address: codexAddress, UserID: "42", ThreadID: "thread-codex", TurnID: "codex-turn"}
	service.routes["openclaw-run"] = &turnRoute{Address: openClawAddress, UserID: "43", Backend: conversation.BackendOpenClaw, ThreadID: "agent:main:main", TurnID: "openclaw-run"}
	service.waits[waitKey(codexAddress, "42")] = inputWait{Address: codexAddress, UserID: "42", TurnID: "codex-turn", InteractionID: "codex-interaction"}
	service.waits[waitKey(openClawAddress, "43")] = inputWait{Address: openClawAddress, UserID: "43", TurnID: "openclaw-run", InteractionID: "openclaw-interaction"}
	service.callbacks["codex-callback"] = callbackAction{TurnID: "codex-turn", InteractionID: "codex-interaction"}
	service.callbacks["openclaw-callback"] = callbackAction{TurnID: "openclaw-run", InteractionID: "openclaw-interaction"}
	service.sessions["codex-session"] = &multiSession{TurnID: "codex-turn", InteractionID: "codex-interaction"}
	service.sessions["openclaw-session"] = &multiSession{TurnID: "openclaw-run", InteractionID: "openclaw-interaction"}
	service.flows["codex-interaction"] = &interactionFlow{TurnID: "codex-turn", InteractionID: "codex-interaction"}
	service.flows["openclaw-interaction"] = &interactionFlow{TurnID: "openclaw-run", InteractionID: "openclaw-interaction"}
	service.interactionNotified["codex-interaction"] = true
	service.interactionNotified["codex-approval"] = true

	service.handleEvent(events.Event{EventType: events.CodexDisconnected})

	service.mu.Lock()
	defer service.mu.Unlock()
	if _, ok := service.routes["codex-turn"]; ok {
		t.Fatal("Codex route remained after Codex disconnect")
	}
	if _, ok := service.routes["openclaw-run"]; !ok {
		t.Fatal("OpenClaw route was removed by Codex disconnect")
	}
	if len(service.waits) != 1 || service.waits[waitKey(openClawAddress, "43")].TurnID != "openclaw-run" {
		t.Fatalf("waits after Codex disconnect = %#v", service.waits)
	}
	if _, ok := service.callbacks["codex-callback"]; ok {
		t.Fatal("Codex callback remained after Codex disconnect")
	}
	if _, ok := service.sessions["codex-session"]; ok {
		t.Fatal("Codex multi-session remained after Codex disconnect")
	}
	if _, ok := service.flows["codex-interaction"]; ok {
		t.Fatal("Codex interaction flow remained after Codex disconnect")
	}
	if service.interactionNotified["codex-interaction"] {
		t.Fatal("Codex interaction notification remained after Codex disconnect")
	}
	if len(service.interactionNotified) != 0 {
		t.Fatalf("Codex interaction notifications remained after disconnect: %#v", service.interactionNotified)
	}
}

func TestCallbackTokenIsRemovedOnFirstUse(t *testing.T) {
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	service := NewService(&fakeControl{}, &fakeRuntime{}, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1"}
	token := service.newCallback(callbackAction{Kind: "noop", Address: address, UserID: "42", Expires: time.Now().Add(time.Minute)})
	message := channels.InboundMessage{Address: address, UserID: "42", Action: token}
	service.handleCallback(context.Background(), message)
	service.mu.Lock()
	_, exists := service.callbacks[token]
	service.mu.Unlock()
	if exists {
		t.Fatal("callback token was not consumed")
	}
}

func TestMultiQuestionInteractionSubmitsAllAnswersOnce(t *testing.T) {
	runtime := &fakeRuntime{interaction: interactions.PendingInteraction{ID: "interaction-1"}}
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	service := NewService(&fakeControl{}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1", TopicID: "2"}
	service.flows["interaction-1"] = &interactionFlow{
		Expires: time.Now().Add(time.Minute), Address: address, UserID: "42", ThreadID: "thread-1", TurnID: "turn-1", InteractionID: "interaction-1",
		Questions: []interactions.Question{{ID: "q1", Type: "single-choice"}, {ID: "q2", Type: "free-text"}}, Answers: map[string][]string{},
	}
	service.acceptInteractionAnswer(context.Background(), "interaction-1", "q1", []string{"a"})
	if len(runtime.responses) != 0 {
		t.Fatal("interaction was submitted before all questions were answered")
	}
	service.acceptInteractionAnswer(context.Background(), "interaction-1", "q2", []string{"b"})
	if len(runtime.responses) != 1 || runtime.responses[0].Answers["q1"][0] != "a" || runtime.responses[0].Answers["q2"][0] != "b" {
		t.Fatalf("unexpected combined response: %#v", runtime.responses)
	}
}

func TestStopRequiresOriginatingAllowedUser(t *testing.T) {
	repository, _ := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	_, _ = repository.Create(bindings.CreateRequest{ChannelType: "telegram", AccountID: "99", ChatID: "1", ThreadID: "thread-1"})
	runtime := &fakeRuntime{state: control.RuntimeState{ThreadID: "thread-1", TurnID: "turn-1", State: "running", CanInterrupt: true}}
	service := NewService(&fakeControl{}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "99", ChatID: "1"}
	service.routes["turn-1"] = &turnRoute{Address: address, UserID: "42", ThreadID: "thread-1", TurnID: "turn-1"}
	service.stopTurn(context.Background(), channels.InboundMessage{Address: address, UserID: "43"})
	if runtime.interrupts != 0 {
		t.Fatal("a different allowlisted group user interrupted the Turn")
	}
	service.stopTurn(context.Background(), channels.InboundMessage{Address: address, UserID: "42"})
	if runtime.interrupts != 1 {
		t.Fatalf("originating user interrupts = %d, want 1", runtime.interrupts)
	}
}

type fakeOpenClawBackend struct {
	sends  []string
	aborts []string
}

func (f *fakeOpenClawBackend) Backend() string { return conversation.BackendOpenClaw }
func (f *fakeOpenClawBackend) ListSessions(context.Context, int) ([]conversation.Session, error) {
	return []conversation.Session{{Backend: conversation.BackendOpenClaw, Key: "agent:main:main", Number: 1, Title: "Main", Status: "idle", UpdatedAt: "2026-08-30T00:00:00Z"}}, nil
}
func (f *fakeOpenClawBackend) SessionByNumber(context.Context, int) (conversation.Session, error) {
	return conversation.Session{Backend: conversation.BackendOpenClaw, Key: "agent:main:main", Number: 1, Title: "Main", Status: "idle"}, nil
}
func (f *fakeOpenClawBackend) ReadSession(context.Context, string) (conversation.Detail, error) {
	return conversation.Detail{Session: conversation.Session{Backend: conversation.BackendOpenClaw, Key: "agent:main:main", Number: 1, Title: "Main", Status: "idle", UpdatedAt: "2026-08-30T00:00:00Z"}, Messages: []conversation.Message{{Role: "user", Text: "previous question"}, {Role: "assistant", Text: "previous answer"}}}, nil
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

func TestTelegramRoutesBoundOpenClawSessionAndFinalOrAbort(t *testing.T) {
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	broker := events.NewBroker()
	service := NewService(&fakeControl{}, &fakeRuntime{}, repository, broker, nil)
	defer service.Close(context.Background())
	backend := &fakeOpenClawBackend{}
	service.SetOpenClawBackend(backend)

	sent := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if text, ok := payload["text"].(string); ok {
			sent <- text
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer server.Close()
	service.adapter.mu.Lock()
	service.adapter.client = newClientForTest("telegram-test-token", server.URL, server.Client())
	service.adapter.mu.Unlock()

	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "bot-1", ConversationType: "default", ChatID: "chat-1"}
	if _, _, err := repository.UpsertAddress(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, ChannelType: "telegram", AccountID: "bot-1", ConversationType: "default",
		ChatID: "chat-1", ThreadID: "agent:main:main", SessionKey: "agent:main:main",
	}); err != nil {
		t.Fatal(err)
	}
	message := channels.InboundMessage{Address: address, UserID: "user-1", Text: "hello OpenClaw"}
	service.startTurn(context.Background(), message, message.Text)
	if len(backend.sends) != 1 || !strings.HasSuffix(backend.sends[0], ":hello OpenClaw") {
		t.Fatalf("OpenClaw send was not routed from Telegram: %#v", backend.sends)
	}
	service.handleEvent(events.Event{EventType: events.OpenClawMessageCompleted, ThreadID: "agent:main:main", TurnID: "openclaw-run-1", Payload: map[string]any{"text": "final answer"}})
	select {
	case text := <-sent:
		if text != "final answer" {
			t.Fatalf("Telegram received unexpected OpenClaw final: %q", text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Telegram did not receive the OpenClaw final answer")
	}

	service.startTurn(context.Background(), message, "second task")
	service.stopTurn(context.Background(), message)
	if len(backend.aborts) != 1 || backend.aborts[0] != "agent:main:main:openclaw-run-2" {
		t.Fatalf("OpenClaw abort was not routed from Telegram: %#v", backend.aborts)
	}
	select {
	case text := <-sent:
		if !strings.Contains(text, "已请求停止 OpenClaw") {
			t.Fatalf("Telegram received unexpected stop acknowledgement: %q", text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Telegram did not acknowledge OpenClaw stop")
	}
}

func TestTelegramNumberedPrefixUsesOpenClawOrCodexNamespace(t *testing.T) {
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntime{state: control.RuntimeState{CanSend: true}}
	service := NewService(&fakeControl{thread: control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "codex-thread-1", Title: "Codex one"}}}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	backend := &fakeOpenClawBackend{}
	service.SetOpenClawBackend(backend)
	service.SetBackendResolver(func(string) string { return conversation.BackendOpenClaw })
	openAddress := channels.ChannelAddress{ChannelType: "telegram", AccountID: "bot", ConversationType: "default", ChatID: "open"}
	if !service.handleNumbered(context.Background(), channels.InboundMessage{Address: openAddress, UserID: "user", Text: "[1] hello"}, "[1] hello") {
		t.Fatal("Telegram did not recognize OpenClaw bracket prefix")
	}
	if len(backend.sends) != 1 || backend.sends[0] != "agent:main:main:hello" {
		t.Fatalf("Telegram OpenClaw route=%#v", backend.sends)
	}
	if _, _, err := repository.UpsertAddress(bindings.CreateRequest{Backend: conversation.BackendCodex, ChannelType: "telegram", AccountID: "bot", ConversationType: "default", ChatID: "codex", ThreadID: "codex-thread-1", TargetID: "codex-thread-1"}); err != nil {
		t.Fatal(err)
	}
	numbers, err := threadregistry.New(filepath.Join(t.TempDir(), "thread-numbers.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := numbers.Ensure(threadregistry.Metadata{ThreadID: "codex-thread-1", Title: "Codex one", CreatedAt: "2026-08-30T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	service.registry = numbers
	codexAddress := channels.ChannelAddress{ChannelType: "telegram", AccountID: "bot", ConversationType: "default", ChatID: "codex"}
	if !service.handleNumbered(context.Background(), channels.InboundMessage{Address: codexAddress, UserID: "user", Text: "#1 codex"}, "#1 codex") {
		t.Fatal("Telegram did not recognize Codex hash prefix")
	}
	if runtime.starts != 1 || len(backend.sends) != 1 {
		t.Fatalf("Telegram number spaces crossed: starts=%d sends=%#v", runtime.starts, backend.sends)
	}
}

func TestTelegramGlobalNumberRoutesAndBindsWithoutCurrentBackend(t *testing.T) {
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRuntime{state: control.RuntimeState{CanSend: true}}
	service := NewService(&fakeControl{thread: control.ThreadDetail{ThreadSummary: control.ThreadSummary{ThreadID: "codex-thread-1", Title: "Codex one"}}}, runtime, repository, events.NewBroker(), nil)
	defer service.Close(context.Background())
	backend := &fakeOpenClawBackend{}
	service.SetOpenClawBackend(backend)
	service.SetBackendResolver(func(string) string { return conversation.BackendOpenClaw })
	service.registry = conversationregistry.NewInMemory()
	if _, err := service.registry.(*conversationregistry.Registry).EnsureBatch([]conversationregistry.Metadata{
		{Backend: conversationregistry.BackendCodex, TargetID: "codex-thread-1", CreatedAt: "2026-08-31T00:00:01Z"},
		{Backend: conversationregistry.BackendOpenClaw, TargetID: "agent:main:main", CreatedAt: "2026-08-31T00:00:02Z"},
	}); err != nil {
		t.Fatal(err)
	}
	address := channels.ChannelAddress{ChannelType: "telegram", AccountID: "bot", ConversationType: "default", ChatID: "cross-backend"}
	if !service.handleNumbered(context.Background(), channels.InboundMessage{Address: address, UserID: "user"}, "[2] hello") {
		t.Fatal("Telegram did not recognize the global bracket number")
	}
	if len(backend.sends) != 1 || backend.sends[0] != "agent:main:main:hello" {
		t.Fatalf("global [2] did not route to OpenClaw from the current OpenClaw profile: %#v", backend.sends)
	}
	if !service.handleNumbered(context.Background(), channels.InboundMessage{Address: address, UserID: "user"}, "#1 codex") {
		t.Fatal("Telegram did not recognize the global Codex hash number")
	}
	if runtime.starts != 1 || len(backend.sends) != 1 {
		t.Fatalf("global #1 followed the current OpenClaw profile instead of Codex: starts=%d sends=%#v", runtime.starts, backend.sends)
	}
	service.bind(context.Background(), channels.InboundMessage{Address: address, UserID: "user"}, "2")
	binding, ok := repository.FindAddress("telegram", "bot", "default", "cross-backend", "")
	if !ok || binding.Backend != conversation.BackendOpenClaw || binding.TargetID != "agent:main:main" {
		t.Fatalf("global /bind 2 inferred the current backend or lost target: %#v ok=%t", binding, ok)
	}
}
