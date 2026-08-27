package qqbot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"github.com/gorilla/websocket"
)

func TestGatewayUsesCurrentEndpointAndPreservesDiagnostics(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		switch request.URL.Path {
		case "/token":
			_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "sensitive-access-token", "expires_in": 7200})
		case gatewayEndpoint:
			if request.Header.Get("Authorization") != "QQBot sensitive-access-token" {
				t.Errorf("unexpected gateway authorization scheme")
			}
			writer.Header().Set("X-Tps-Trace-Id", "trace-safe-123")
			writer.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 11243, "err_code": 40012001, "message": "gateway is unavailable"})
		default:
			t.Fatalf("obsolete gateway endpoint was requested: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	provider := NewTokenProvider(server.Client(), "12345", "sensitive-secret", nil)
	provider.endpoint = server.URL + "/token"
	client := newOfficialClient(server.Client(), provider)
	client.baseURL = server.URL
	client.proxyMode = "direct"
	var diagnostic gatewayDiagnostic
	client.onGatewayDiagnostic = func(value gatewayDiagnostic) { diagnostic = value }
	_, err := client.gateway(context.Background())
	if ClassifyError(err) != "gateway_endpoint_not_found" {
		t.Fatalf("gateway error=%v code=%s", err, ClassifyError(err))
	}
	if len(paths) != 2 || paths[1] != gatewayEndpoint {
		t.Fatalf("request paths=%v; want token then %s only", paths, gatewayEndpoint)
	}
	if diagnostic.RequestPath != gatewayEndpoint || diagnostic.RequestMethod != http.MethodGet || diagnostic.HTTPStatus != http.StatusNotFound || diagnostic.QQCode != 11243 || diagnostic.QQErrCode != 40012001 || diagnostic.QQMessage != "gateway is unavailable" || diagnostic.TraceID != "trace-safe-123" {
		t.Fatalf("diagnostic=%#v", diagnostic)
	}
	if !diagnostic.AuthorizationPresent || diagnostic.AuthorizationScheme != "QQBot" || diagnostic.TokenLength != len("sensitive-access-token") {
		t.Fatalf("authorization diagnostic=%#v", diagnostic)
	}
	serialized, _ := json.Marshal(diagnostic)
	if strings.Contains(string(serialized), "sensitive-secret") || strings.Contains(string(serialized), "sensitive-access-token") || strings.Contains(string(serialized), "QQBot sensitive") {
		t.Fatalf("gateway diagnostic leaked credentials: %s", serialized)
	}
}

func TestDiscoveredIdentitiesEmptyUsesJSONArray(t *testing.T) {
	adapter := NewAdapter(nil)
	identities := adapter.DiscoveredIdentities()
	if identities == nil {
		t.Fatal("empty discovered identities must be a non-nil slice")
	}
	payload, err := json.Marshal(identities)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "[]" {
		t.Fatalf("empty discovered identities JSON=%s; want []", payload)
	}
}

func TestGateway401IsAuthenticationFailureWithSafeRequestMetadata(t *testing.T) {
	var gatewayRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "access-token", "expires_in": 7200})
		case gatewayEndpoint:
			gatewayRequests.Add(1)
			writer.Header().Set("X-Tps-Trace-Id", "trace-auth-401")
			writer.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 11201, "err_code": 40012001, "message": "鉴权失败"})
		default:
			t.Fatalf("unexpected request path %s", request.URL.Path)
		}
	}))
	defer server.Close()
	provider := NewTokenProvider(server.Client(), "12345", "secret", nil)
	provider.endpoint = server.URL + "/token"
	client := newOfficialClient(server.Client(), provider)
	client.baseURL = server.URL
	var diagnostic gatewayDiagnostic
	client.onGatewayDiagnostic = func(value gatewayDiagnostic) { diagnostic = value }
	_, err := client.gateway(context.Background())
	if ClassifyError(err) != "gateway_auth_failed" || gatewayRequests.Load() != 2 {
		t.Fatalf("code=%s gatewayRequests=%d err=%v", ClassifyError(err), gatewayRequests.Load(), err)
	}
	if !diagnostic.AuthorizationPresent || diagnostic.AuthorizationScheme != "QQBot" || diagnostic.TokenLength != len("access-token") || diagnostic.HTTPStatus != http.StatusUnauthorized || diagnostic.QQCode != 11201 || diagnostic.QQErrCode != 40012001 || diagnostic.TraceID != "trace-auth-401" {
		t.Fatalf("diagnostic=%#v", diagnostic)
	}
	if message := SafeErrorMessage(err); !strings.Contains(message, "HTTP 401") || !strings.Contains(message, "Authorization Header") {
		t.Fatalf("safe message=%q", message)
	}
}

func TestGatewayRejectsMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/token":
			_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "token", "expires_in": 7200})
		case gatewayEndpoint:
			_, _ = writer.Write([]byte(`{"unexpected":true}`))
		}
	}))
	defer server.Close()
	provider := NewTokenProvider(server.Client(), "12345", "secret", nil)
	provider.endpoint = server.URL + "/token"
	client := newOfficialClient(server.Client(), provider)
	client.baseURL = server.URL
	if _, err := client.gateway(context.Background()); ClassifyError(err) != "gateway_response_invalid" {
		t.Fatalf("gateway malformed response code=%s err=%v", ClassifyError(err), err)
	}
}

func TestHTTPAndWebSocketUseSameProxyPolicy(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		httpClient, dialer, err := buildNetworkClients("direct", "")
		if err != nil {
			t.Fatal(err)
		}
		transport := httpClient.Transport.(*http.Transport)
		if transport.Proxy != nil || dialer.Proxy != nil {
			t.Fatal("direct mode must disable proxy for HTTP and WebSocket")
		}
	})
	t.Run("custom-http", func(t *testing.T) {
		const configured = "http://127.0.0.1:7897"
		httpClient, dialer, err := buildNetworkClients("custom-http", configured)
		if err != nil {
			t.Fatal(err)
		}
		apiRequest := &http.Request{URL: &url.URL{Scheme: "https", Host: "api.sgroup.qq.com"}}
		wsRequest := &http.Request{URL: &url.URL{Scheme: "wss", Host: "example.gateway.qq.com"}}
		httpProxy, httpErr := httpClient.Transport.(*http.Transport).Proxy(apiRequest)
		wsProxy, wsErr := dialer.Proxy(wsRequest)
		if httpErr != nil || wsErr != nil || httpProxy == nil || wsProxy == nil || httpProxy.String() != configured || wsProxy.String() != configured {
			t.Fatalf("HTTP proxy=%v err=%v; WebSocket proxy=%v err=%v", httpProxy, httpErr, wsProxy, wsErr)
		}
	})
}

func TestTokenProviderCachesSingleFlightAndRefreshes(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode token request: %v", err)
		}
		if body["appId"] != "12345" || body["clientSecret"] != "test-secret" {
			t.Errorf("unexpected token credentials: %#v", body)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "  token-" + time.Now().Format("150405.000000") + " \r\n", "expires_in": "7200"})
	}))
	defer server.Close()

	provider := NewTokenProvider(server.Client(), "12345", "test-secret", nil)
	provider.endpoint = server.URL
	var wait sync.WaitGroup
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if token, _, err := provider.Token(context.Background(), false); err != nil || token == "" || token != strings.TrimSpace(token) {
				t.Errorf("cached token: token=%q err=%v", token, err)
			}
		}()
	}
	wait.Wait()
	if got := requests.Load(); got != 1 {
		t.Fatalf("token requests=%d; want 1", got)
	}
	provider.Invalidate()
	if _, _, err := provider.Token(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("token requests after refresh=%d; want 2", got)
	}
}

func TestOfficialGatewayMessagesReconnectAndStop(t *testing.T) {
	var tokenRequests atomic.Int32
	var gatewayConnections atomic.Int32
	var sendRequests atomic.Int32
	var adapter *Adapter
	inbound := make(chan channels.InboundMessage, 8)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/token":
			sequence := tokenRequests.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{"access_token": "access-" + string(rune('0'+sequence)), "expires_in": 7200})
		case request.URL.Path == "/gateway":
			if request.Header.Get("Authorization") == "" {
				t.Error("gateway request omitted authorization")
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"url": "ws://" + request.Host + "/ws"})
		case request.URL.Path == "/ws":
			connection, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				t.Errorf("upgrade: %v", err)
				return
			}
			defer connection.Close()
			connectionNumber := gatewayConnections.Add(1)
			_ = connection.WriteJSON(map[string]any{"op": opHello, "d": map[string]any{"heartbeat_interval": 40}})
			var identify map[string]any
			if err := connection.ReadJSON(&identify); err != nil {
				return
			}
			wantOpcode := float64(opIdentify)
			if connectionNumber > 1 {
				wantOpcode = float64(opResume)
			}
			if identify["op"] != wantOpcode {
				t.Errorf("connection %d opcode=%v; want %v", connectionNumber, identify["op"], wantOpcode)
			}
			data, _ := identify["d"].(map[string]any)
			if !strings.HasPrefix(data["token"].(string), "QQBot access-") {
				t.Error("identify token scheme is invalid")
			}
			if connectionNumber == 1 {
				if data["intents"] != float64(intentGroupAndC2C) {
					t.Errorf("intents=%v", data["intents"])
				}
				_ = connection.WriteJSON(map[string]any{"op": opDispatch, "s": 1, "t": eventReady, "d": map[string]any{"session_id": "session-abcdef123456"}})
				writeGatewayMessage(connection, 2, eventC2CMessageCreate, map[string]any{"id": "c2c-1", "content": "hello", "timestamp": time.Now().UTC().Format(time.RFC3339), "author": map[string]any{"user_openid": "user-ok", "username": "Alice"}})
				writeGatewayMessage(connection, 3, eventC2CMessageCreate, map[string]any{"id": "c2c-1", "content": "duplicate", "author": map[string]any{"user_openid": "user-ok"}})
				writeGatewayMessage(connection, 4, eventC2CMessageCreate, map[string]any{"id": "c2c-2", "content": "unauthorized", "author": map[string]any{"user_openid": "user-new"}})
				writeGatewayMessage(connection, 5, eventGroupAtMessage, map[string]any{"id": "group-1", "group_openid": "group-ok", "content": "<@!bot> group task", "author": map[string]any{"member_openid": "member-ok", "username": "Bob"}})
				for {
					var payload map[string]any
					if err := connection.ReadJSON(&payload); err != nil {
						return
					}
					if payload["op"] == float64(opHeartbeat) {
						_ = connection.WriteJSON(map[string]any{"op": opHeartbeatACK})
						_ = connection.WriteJSON(map[string]any{"op": opReconnect})
						return
					}
				}
			}
			_ = connection.WriteJSON(map[string]any{"op": opDispatch, "s": 6, "t": eventResumed, "d": map[string]any{}})
			for {
				var payload map[string]any
				if err := connection.ReadJSON(&payload); err != nil {
					return
				}
				if payload["op"] == float64(opHeartbeat) {
					_ = connection.WriteJSON(map[string]any{"op": opHeartbeatACK})
				}
			}
		case strings.HasPrefix(request.URL.Path, "/v2/users/"):
			attempt := sendRequests.Add(1)
			if attempt == 1 {
				writer.WriteHeader(http.StatusUnauthorized)
				return
			}
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			if body["msg_id"] != "c2c-1" || body["msg_seq"] != float64(1) {
				t.Errorf("passive reply fields=%#v", body)
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"id": "reply-1"})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	adapter = NewAdapter(func(_ context.Context, message channels.InboundMessage) { inbound <- message })
	_, err := adapter.Configure(ConfigureRequest{Enabled: true, AppID: "12345", Environment: "production", AllowedUserOpenIDs: []string{"user-ok"}, AllowedGroupOpenIDs: []string{"group-ok"}, AllowedGroupMemberOpenIDs: []string{"member-ok"}, GatewayReconnectEnabled: true, ProxyMode: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.SetSecret("test-secret"); err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	adapter.tokens.endpoint = server.URL + "/token"
	adapter.client.baseURL = server.URL
	adapter.client.allowInsecureGateway = true
	adapter.mu.Unlock()

	if err := adapter.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := receiveInbound(t, inbound)
	second := receiveInbound(t, inbound)
	received := map[string]channels.InboundMessage{
		first.Address.ConversationType:  first,
		second.Address.ConversationType: second,
	}
	if received["c2c"].Text != "hello" || received["group"].Text != "group task" {
		t.Fatalf("inbound messages=%#v %#v", first, second)
	}
	select {
	case unexpected := <-inbound:
		t.Fatalf("duplicate or unauthorized message was routed: %#v", unexpected)
	case <-time.After(100 * time.Millisecond):
	}
	if identities := adapter.DiscoveredIdentities(); len(identities) != 3 {
		t.Fatalf("discovered identities=%d; want 3", len(identities))
	}
	result, err := adapter.SendMessage(context.Background(), channels.OutboundMessage{Address: received["c2c"].Address, Text: "accepted"})
	if err != nil || result.MessageID != "reply-1" {
		t.Fatalf("send result=%#v err=%v", result, err)
	}
	waitUntil(t, 3*time.Second, func() bool { return gatewayConnections.Load() >= 2 && adapter.QQBotStatus().Connected })

	statusJSON, _ := json.Marshal(adapter.QQBotStatus())
	if strings.Contains(string(statusJSON), "test-secret") || strings.Contains(string(statusJSON), "access-") {
		t.Fatalf("status leaked credentials: %s", statusJSON)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := adapter.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	status := adapter.QQBotStatus()
	if status.Running || status.Connected || status.GatewayState != "stopped" {
		t.Fatalf("status after stop=%#v", status)
	}
	if tokenRequests.Load() != 2 {
		t.Fatalf("token requests=%d; want initial plus controlled 401 refresh", tokenRequests.Load())
	}
}

func TestBuildTextRequestSeparatesProactiveAndEventReplyPayloads(t *testing.T) {
	path, body, diagnostic := buildTextRequest("c2c", "user-openid", "hello", "", 1)
	if path != "/v2/users/user-openid/messages" {
		t.Fatalf("proactive path = %q", path)
	}
	if _, ok := body["msg_id"]; ok {
		t.Fatal("proactive payload must not contain msg_id")
	}
	if _, ok := body["event_id"]; ok {
		t.Fatal("proactive payload must not contain event_id")
	}
	if _, ok := body["msg_seq"]; ok {
		t.Fatal("proactive payload must not contain msg_seq")
	}
	if diagnostic.DeliveryMode != "proactive" || diagnostic.TargetOpenIDType != "user_openid" || diagnostic.MsgSeqPresent {
		t.Fatalf("unexpected proactive diagnostic: %+v", diagnostic)
	}

	path, body, diagnostic = buildTextRequest("group", "group-openid", "reply", "event-message-id", 2)
	if path != "/v2/groups/group-openid/messages" {
		t.Fatalf("reply path = %q", path)
	}
	if body["msg_id"] != "event-message-id" || body["msg_seq"] != 2 {
		t.Fatalf("event reply payload = %#v", body)
	}
	if diagnostic.DeliveryMode != "event-reply" || diagnostic.TargetOpenIDType != "group_openid" || !diagnostic.MsgIDPresent || !diagnostic.MsgSeqPresent {
		t.Fatalf("unexpected reply diagnostic: %+v", diagnostic)
	}
}

func writeGatewayMessage(connection *websocket.Conn, sequence int64, event string, data map[string]any) {
	_ = connection.WriteJSON(map[string]any{"op": opDispatch, "s": sequence, "t": event, "d": data})
}

func receiveInbound(t *testing.T, messages <-chan channels.InboundMessage) channels.InboundMessage {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inbound message")
		return channels.InboundMessage{}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
