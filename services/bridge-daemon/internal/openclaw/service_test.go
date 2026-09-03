package openclaw

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/sessionregistry"
)

type fakeGateway struct {
	t *testing.T

	mu          sync.Mutex
	connections []*websocket.Conn
	authTokens  []string
	sendParams  []map[string]any
	abortParams []map[string]any
	sent        chan struct{}
	aborted     chan struct{}
}

func newFakeGateway(t *testing.T) (*fakeGateway, *httptest.Server) {
	t.Helper()
	fake := &fakeGateway{
		t: t, sent: make(chan struct{}, 2), aborted: make(chan struct{}, 2),
	}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(server.Close)
	return fake, server
}

func (g *fakeGateway) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connection, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		g.t.Errorf("upgrade fake Gateway connection: %v", err)
		return
	}
	g.mu.Lock()
	g.connections = append(g.connections, connection)
	g.mu.Unlock()
	defer connection.Close()

	if err := writeGatewayFrame(connection, map[string]any{
		"type": "event", "event": "connect.challenge", "payload": map[string]any{"nonce": "test-nonce"},
	}); err != nil {
		return
	}

	for {
		_, data, err := connection.ReadMessage()
		if err != nil {
			return
		}
		var requestFrame struct {
			Type   string          `json:"type"`
			ID     string          `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &requestFrame); err != nil || requestFrame.Type != "req" {
			continue
		}
		params := map[string]any{}
		if len(requestFrame.Params) > 0 {
			_ = json.Unmarshal(requestFrame.Params, &params)
		}
		switch requestFrame.Method {
		case "connect":
			if auth, ok := params["auth"].(map[string]any); ok {
				if token, ok := auth["token"].(string); ok {
					g.mu.Lock()
					g.authTokens = append(g.authTokens, token)
					g.mu.Unlock()
				}
			}
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{
				"type": "hello-ok", "protocol": protocolVersion,
				"server":   map[string]any{"version": "test-gateway"},
				"snapshot": map[string]any{"authMode": "token"},
				"policy":   map[string]any{"tickIntervalMs": 1000},
				"auth":     map[string]any{"role": "operator"},
			}); err != nil {
				return
			}
			_ = writeGatewayFrame(connection, map[string]any{
				"type": "event", "event": "tick", "payload": map[string]any{},
			})
		case "sessions.subscribe":
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{"subscribed": true}); err != nil {
				return
			}
		case "sessions.list":
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{
				"sessions": []map[string]any{{
					"sessionKey": "agent:main:main", "sessionId": "session-1", "displayName": "Main",
					"model": "gpt-5.6-luna", "status": "idle", "activeRunIds": []string{},
					"updatedAt": "2026-08-27T00:00:00Z", "archived": false,
				}},
			}); err != nil {
				return
			}
		case "chat.history":
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{
				"messages": []map[string]any{
					{"id": "m-user", "role": "user", "text": "previous question"},
					{"id": "m-assistant", "role": "assistant", "text": "previous answer"},
				},
			}); err != nil {
				return
			}
		case "chat.send":
			g.mu.Lock()
			g.sendParams = append(g.sendParams, params)
			g.mu.Unlock()
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{"runId": "run-1", "status": "running"}); err != nil {
				return
			}
			_ = writeGatewayFrameWithSeq(connection, 1, map[string]any{
				"type": "event", "event": "chat", "payload": map[string]any{
					"state": "delta", "sessionKey": "agent:main:main", "runId": "run-1", "deltaText": "hello ",
				},
			})
			_ = writeGatewayFrameWithSeq(connection, 2, map[string]any{
				"type": "event", "event": "chat", "payload": map[string]any{
					"state": "final", "sessionKey": "agent:main:main", "runId": "run-1", "text": "hello final",
				},
			})
			// The duplicate terminal frame must not create a second channel reply.
			_ = writeGatewayFrameWithSeq(connection, 2, map[string]any{
				"type": "event", "event": "chat", "payload": map[string]any{
					"state": "final", "sessionKey": "agent:main:main", "runId": "run-1", "text": "hello final",
				},
			})
			g.sent <- struct{}{}
		case "chat.abort":
			g.mu.Lock()
			g.abortParams = append(g.abortParams, params)
			g.mu.Unlock()
			if err := writeGatewayResponse(connection, requestFrame.ID, map[string]any{"runId": "run-1", "status": "aborted"}); err != nil {
				return
			}
			_ = writeGatewayFrameWithSeq(connection, 3, map[string]any{
				"type": "event", "event": "chat", "payload": map[string]any{
					"state": "aborted", "sessionKey": "agent:main:main", "runId": "run-1",
				},
			})
			g.aborted <- struct{}{}
		default:
			_ = writeGatewayError(connection, requestFrame.ID, "method_not_found", requestFrame.Method)
		}
	}
}

func writeGatewayFrame(connection *websocket.Conn, value any) error {
	return connection.WriteJSON(value)
}

func writeGatewayFrameWithSeq(connection *websocket.Conn, seq int, value map[string]any) error {
	value["seq"] = seq
	return writeGatewayFrame(connection, value)
}

func writeGatewayResponse(connection *websocket.Conn, id string, payload any) error {
	return writeGatewayFrame(connection, map[string]any{"type": "res", "id": id, "ok": true, "payload": payload})
}

func writeGatewayError(connection *websocket.Conn, id, code, message string) error {
	return writeGatewayFrame(connection, map[string]any{"type": "res", "id": id, "ok": false, "error": map[string]any{"code": code, "message": message}})
}

func (g *fakeGateway) closeFirstConnection() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.connections) > 0 {
		_ = g.connections[0].Close()
	}
}

func (g *fakeGateway) connectionCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.connections)
}

func (g *fakeGateway) firstAuthToken() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.authTokens) == 0 {
		return ""
	}
	return g.authTokens[0]
}

func waitForOpenClaw(t *testing.T, service *Service, predicate func(conversation.ConnectionStatus) bool) conversation.ConnectionStatus {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		status := service.ConnectionStatus()
		if predicate(status) {
			return status
		}
		time.Sleep(20 * time.Millisecond)
	}
	status := service.ConnectionStatus()
	t.Fatalf("OpenClaw status did not reach expected state: %#v", status)
	return status
}

func TestServiceRepeatedStartStopDoesNotAccumulateGoroutines(t *testing.T) {
	_, server := newFakeGateway(t)
	broker := events.NewBroker()
	numbers, err := sessionregistry.New(filepath.Join(t.TempDir(), "openclaw-session-numbers.json"))
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, broker, numbers)
	defer service.Close()

	gatewayURL := "ws" + strings.TrimPrefix(server.URL, "http")
	baseline := runtime.NumGoroutine()
	peak := baseline
	for cycle := 0; cycle < 20; cycle++ {
		if _, err := service.ConfigureConfig(Config{GatewayURL: gatewayURL, Token: "test-token", AutoReconnect: false, Start: true}); err != nil {
			t.Fatalf("configure cycle %d: %v", cycle, err)
		}
		waitForOpenClaw(t, service, func(status conversation.ConnectionStatus) bool { return status.Connected })
		if err := service.Close(); err != nil {
			t.Fatalf("close cycle %d: %v", cycle, err)
		}
		runtime.Gosched()
		if current := runtime.NumGoroutine(); current > peak {
			peak = current
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runtime.NumGoroutine() > baseline+4 {
		runtime.Gosched()
		time.Sleep(20 * time.Millisecond)
	}
	final := runtime.NumGoroutine()
	t.Logf("OpenClaw start/stop cycles=20 goroutines baseline=%d peak=%d final=%d", baseline, peak, final)
	if final > baseline+4 {
		t.Fatalf("goroutines did not return near baseline: baseline=%d final=%d", baseline, final)
	}
}

func TestServiceConnectsListsSendsFinalAbortsAndReconnects(t *testing.T) {
	gateway, server := newFakeGateway(t)
	broker := events.NewBroker()
	numbersPath := filepath.Join(t.TempDir(), "openclaw-session-numbers.json")
	numbers, err := sessionregistry.New(numbersPath)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, broker, numbers)
	eventsChannel := make(chan conversation.Event, 16)
	unsubscribe := service.SubscribeEvents(func(event conversation.Event) { eventsChannel <- event })
	defer unsubscribe()

	gatewayURL := "ws" + strings.TrimPrefix(server.URL, "http")
	if _, err := service.ConfigureConfig(Config{GatewayURL: gatewayURL, Token: "test-token", AutoReconnect: true, Start: true}); err != nil {
		t.Fatalf("configure OpenClaw service: %v", err)
	}
	defer service.Close()

	status := waitForOpenClaw(t, service, func(status conversation.ConnectionStatus) bool { return status.Connected })
	if status.Protocol != protocolVersion || status.ServerVersion != "test-gateway" || status.AuthMode != "token" {
		t.Fatalf("unexpected connected status: %#v", status)
	}
	if gateway.firstAuthToken() != "test-token" {
		t.Fatalf("Gateway did not receive the configured token")
	}
	if status.LastTickAt == "" {
		t.Fatal("heartbeat/tick was not recorded")
	}

	sessions, err := service.ListSessions(t.Context(), 100)
	if err != nil {
		t.Fatalf("list OpenClaw sessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].Key != "agent:main:main" || sessions[0].Title != "Main" || sessions[0].Number != 1 {
		t.Fatalf("unexpected sessions: %#v", sessions)
	}
	reloaded, err := sessionregistry.New(numbersPath)
	if err != nil {
		t.Fatal(err)
	}
	if record, ok := reloaded.BySessionKey("agent:main:main"); !ok || record.Number != 1 {
		t.Fatalf("session number did not survive registry reload: %#v ok=%t", record, ok)
	}
	detail, err := service.ReadSession(t.Context(), "agent:main:main")
	if err != nil || len(detail.Messages) != 2 || detail.Messages[1].Text != "previous answer" {
		t.Fatalf("read OpenClaw session: detail=%#v err=%v", detail, err)
	}

	accepted, err := service.SendMessage(t.Context(), "agent:main:main", "new question")
	if err != nil || accepted.RunID != "run-1" {
		t.Fatalf("send OpenClaw message: accepted=%#v err=%v", accepted, err)
	}
	select {
	case <-gateway.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("fake Gateway did not receive chat.send")
	}

	finalCount := 0
	finalText := ""
	deadline := time.After(2 * time.Second)
	for finalCount == 0 {
		select {
		case event := <-eventsChannel:
			switch event.Type {
			case events.OpenClawMessageCompleted:
				finalCount++
				finalText = event.Text
			}
		case <-deadline:
			t.Fatalf("did not receive final event: count=%d", finalCount)
		}
	}
	if finalCount != 1 || finalText != "hello final" {
		t.Fatalf("message dedup/final text failed: count=%d text=%q", finalCount, finalText)
	}

	abortResult, err := service.Abort(t.Context(), "agent:main:main", "run-1")
	if err != nil || abortResult.Status != "aborted" {
		t.Fatalf("abort OpenClaw message: result=%#v err=%v", abortResult, err)
	}
	select {
	case <-gateway.aborted:
	case <-time.After(2 * time.Second):
		t.Fatal("fake Gateway did not receive chat.abort")
	}
	abortedCount := 0
	deadline = time.After(2 * time.Second)
	for abortedCount == 0 {
		select {
		case event := <-eventsChannel:
			if event.Type == events.OpenClawMessageAborted {
				abortedCount++
			}
		case <-deadline:
			t.Fatal("did not receive aborted event")
		}
	}

	gateway.closeFirstConnection()
	waitForOpenClaw(t, service, func(status conversation.ConnectionStatus) bool {
		return status.Connected && status.ReconnectCount >= 1 && gateway.connectionCount() >= 2
	})
	if gateway.connectionCount() < 2 {
		t.Fatalf("Gateway was not reconnected; connections=%d", gateway.connectionCount())
	}

	if err := service.Stop(t.Context()); err != nil {
		t.Fatalf("stop OpenClaw service: %v", err)
	}
	if service.ConnectionStatus().Running {
		t.Fatal("OpenClaw service remained running after Stop")
	}
}

// This is an opt-in, read-only probe for a developer workstation. It never
// sends chat content or changes Gateway configuration; CI and other machines
// skip it unless both environment variables are explicitly provided.
func TestServiceConnectsToConfiguredLocalGateway(t *testing.T) {
	gatewayURL := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_URL"))
	token := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_TOKEN"))
	if gatewayURL == "" || token == "" {
		t.Skip("set OPENCLAW_GATEWAY_URL and OPENCLAW_GATEWAY_TOKEN to run the local Gateway probe")
	}
	service := NewService(nil, events.NewBroker())
	if _, err := service.ConfigureConfig(Config{GatewayURL: gatewayURL, Token: token, AutoReconnect: false, Start: true}); err != nil {
		t.Fatalf("configure local OpenClaw Gateway: %v", err)
	}
	defer service.Close()
	waitForOpenClaw(t, service, func(status conversation.ConnectionStatus) bool { return status.Connected })
	sessions, err := service.ListSessions(t.Context(), 200)
	if err != nil {
		t.Fatalf("list sessions from local OpenClaw Gateway: %v", err)
	}
	if len(sessions) == 0 {
		t.Fatal("local OpenClaw Gateway returned no sessions")
	}
	t.Logf("local OpenClaw Gateway connected; sessions=%d", len(sessions))
}

// This opt-in smoke test exercises the real local Gateway for the acceptance
// path: list -> stable number -> explicit binding record -> one accepted chat
// message. It is intentionally skipped unless the caller supplies the token.
func TestServiceLocalGatewayNumberBindAndSend(t *testing.T) {
	gatewayURL := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_URL"))
	token := strings.TrimSpace(os.Getenv("OPENCLAW_GATEWAY_TOKEN"))
	if gatewayURL == "" || token == "" {
		t.Skip("set OPENCLAW_GATEWAY_URL and OPENCLAW_GATEWAY_TOKEN to run the local Gateway message probe")
	}
	numbersPath := filepath.Join(t.TempDir(), "openclaw-session-numbers.json")
	numbers, err := sessionregistry.New(numbersPath)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, events.NewBroker(), numbers)
	if _, err := service.ConfigureConfig(Config{GatewayURL: gatewayURL, Token: token, AutoReconnect: false, Start: true}); err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	waitForOpenClaw(t, service, func(status conversation.ConnectionStatus) bool { return status.Connected })
	sessions, err := service.ListSessions(t.Context(), 200)
	if err != nil || len(sessions) == 0 {
		t.Fatalf("real Gateway session list: count=%d err=%v", len(sessions), err)
	}
	selected, err := service.SessionByNumber(t.Context(), sessions[0].Number)
	if err != nil || selected.Key != sessions[0].Key || selected.Number != sessions[0].Number {
		t.Fatalf("real Gateway number lookup: selected=%#v first=%#v err=%v", selected, sessions[0], err)
	}
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	created, _, err := repository.UpsertAddress(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, TargetID: selected.Key, ChannelType: "telegram", ChannelProfileID: "acceptance-profile",
		ResourceID: "acceptance-resource", AccountID: "acceptance-bot", ConversationType: "default", ConversationID: "acceptance-chat",
		ChatID: "acceptance-chat", ThreadID: selected.Key, SessionKey: selected.Key,
	})
	if err != nil || created.Backend != conversation.BackendOpenClaw || created.TargetID != selected.Key {
		t.Fatalf("real Gateway binding record: binding=%#v err=%v", created, err)
	}
	accepted, err := service.SendMessage(t.Context(), selected.Key, "CloudLight Bridge integration check: reply OK only.")
	if err != nil || accepted.SessionKey != selected.Key || accepted.RunID == "" {
		t.Fatalf("real Gateway message send: accepted=%#v err=%v", accepted, err)
	}
	t.Logf("real Gateway acceptance passed; session=#%d key=%s runAccepted=true", selected.Number, selected.Key)
}
