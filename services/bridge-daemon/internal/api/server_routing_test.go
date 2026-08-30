package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channelprofiles"
)

func TestProfileRoutingAPIAlwaysReturnsArraysForEmptyRoutes(t *testing.T) {
	profiles := channelprofiles.NewManager(nil, nil, nil, nil, nil, nil, nil, nil)
	server := &Server{profiles: profiles}

	assertRoutingResponseUsesArrays(t, server, httptest.NewRequest(http.MethodGet, "/api/v1/channel-routing", nil))

	for _, test := range []struct {
		name string
		body string
	}{
		{
			name: "null fields",
			body: `{"codex":{"telegramProfileIds":null,"qqProfileIds":null},"openClaw":null}`,
		},
		{
			name: "empty arrays",
			body: `{"codex":{"telegramProfileIds":[],"qqProfileIds":[]},"openClaw":{"telegramProfileIds":[],"qqProfileIds":[]}}`,
		},
		{
			name: "both backends empty",
			body: `{"codex":{},"openClaw":{}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/api/v1/channel-routing", bytes.NewBufferString(test.body))
			response := httptest.NewRecorder()
			server.profileRoutingConfigure(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("configure routing returned %d: %s", response.Code, response.Body.String())
			}
			assertRoutingJSONUsesArrays(t, response.Body.String())
		})
	}
}

func TestProfileOperationErrorPreservesCurrentStateForDesktop(t *testing.T) {
	response := httptest.NewRecorder()
	writeProfileOperationError(response, http.StatusConflict, "channel_profile_start_failed", channelprofiles.ProfileStatus{
		ID: "qq-default", Platform: channelprofiles.PlatformQQ, State: "authentication-failed",
		LastError: "QQ access token request returned HTTP 200、错误码 100016：AppID 或 AppSecret 无效或已重置。",
	}, errors.New("QQ authentication failed"))
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		CurrentState string `json:"currentState"`
		LastError    string `json:"lastError"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "channel_profile_start_failed" || body.CurrentState != "authentication-failed" || body.LastError == "" {
		t.Fatalf("operation error response=%#v", body)
	}
}

func assertRoutingResponseUsesArrays(t *testing.T, server *Server, request *http.Request) {
	t.Helper()
	response := httptest.NewRecorder()
	server.profileRouting(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("get routing returned %d: %s", response.Code, response.Body.String())
	}
	assertRoutingJSONUsesArrays(t, response.Body.String())
}

func assertRoutingJSONUsesArrays(t *testing.T, payload string) {
	t.Helper()
	if strings.Contains(payload, "null") || strings.Count(payload, "[]") != 4 {
		t.Fatalf("routing API must return four [] fields, got %s", payload)
	}
}
