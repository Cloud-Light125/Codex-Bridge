package appserver

import (
	"encoding/json"
	"strings"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

func TestCanonicalRPCIDSupportsStringAndNumber(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want string
	}{{`"request-7"`, "s:request-7"}, {`42`, "n:42"}} {
		got, ok := canonicalRPCID(json.RawMessage(test.raw))
		if !ok || got != test.want {
			t.Fatalf("canonicalRPCID(%s) = %q, %v; want %q", test.raw, got, ok, test.want)
		}
	}
}

func TestTurnStartWireValuesMatchInstalledSchema(t *testing.T) {
	params, diagnostic, err := buildTurnStartParams("thread-1", "not logged", TurnStartOptions{
		CWD: "D:/code/project", CollaborationMode: "default", Model: "gpt-5",
		ReasoningEffort: "high", ApprovalPolicy: security.ApprovalOnRequest, SandboxMode: security.SandboxWorkspaceWrite,
	})
	if err != nil {
		t.Fatalf("buildTurnStartParams returned error: %v", err)
	}
	payload, err := json.Marshal(map[string]any{"method": "turn/start", "params": params})
	if err != nil {
		t.Fatalf("marshal turn/start request: %v", err)
	}
	encoded := string(payload)
	if !strings.Contains(encoded, `"approvalPolicy":"on-request"`) {
		t.Fatalf("turn/start request does not contain on-request: %s", encoded)
	}
	if strings.Contains(encoded, `"approvalPolicy":"onRequest"`) {
		t.Fatalf("turn/start request contains invalid onRequest wire value: %s", encoded)
	}
	if !strings.Contains(encoded, `"sandboxPolicy":{"networkAccess":false,"type":"workspaceWrite"`) {
		t.Fatalf("turn/start request does not contain current workspaceWrite schema shape: %s", encoded)
	}
	if _, exists := params["collaborationMode"]; exists {
		t.Fatalf("current TurnStartParams schema does not define collaborationMode: %s", encoded)
	}
	if diagnostic.ApprovalPolicy != "on-request" || diagnostic.SandboxType != "workspaceWrite" || diagnostic.NetworkAccess {
		t.Fatalf("unexpected safe diagnostic: %+v", diagnostic)
	}
}

func TestRemoteTurnWireValuesUseUnrestrictedNeverWithoutModel(t *testing.T) {
	params, diagnostic, err := buildTurnStartParams("thread-1", "remote task", TurnStartOptions{
		ApprovalPolicy: security.ApprovalNever, SandboxMode: security.SandboxDangerFullAccess,
	})
	if err != nil {
		t.Fatalf("buildTurnStartParams returned error: %v", err)
	}
	if params["approvalPolicy"] != "never" {
		t.Fatalf("approvalPolicy = %#v, want never", params["approvalPolicy"])
	}
	sandbox, ok := params["sandboxPolicy"].(map[string]any)
	if !ok || sandbox["type"] != "dangerFullAccess" {
		t.Fatalf("sandboxPolicy = %#v, want dangerFullAccess", params["sandboxPolicy"])
	}
	if _, exists := params["model"]; exists {
		t.Fatalf("remote turn unexpectedly overrides model: %#v", params["model"])
	}
	if diagnostic.ApprovalPolicy != "never" || diagnostic.SandboxType != "dangerFullAccess" || !diagnostic.NetworkAccess || diagnostic.Model != "" {
		t.Fatalf("unexpected remote diagnostic: %+v", diagnostic)
	}
}

func TestTurnStartWireValuesRejectUnknownSecurityValues(t *testing.T) {
	_, _, err := buildTurnStartParams("thread-1", "text", TurnStartOptions{
		ApprovalPolicy: security.ApprovalPolicy("sometimes"), SandboxMode: security.SandboxWorkspaceWrite,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported approval policy") {
		t.Fatalf("unknown approval policy must fail closed; got %v", err)
	}

	_, _, err = buildTurnStartParams("thread-1", "text", TurnStartOptions{
		ApprovalPolicy: security.ApprovalOnRequest, SandboxMode: security.SandboxMode("unknown"),
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported sandbox mode") {
		t.Fatalf("unknown sandbox mode must fail closed; got %v", err)
	}
}

func TestClassifyStderrOnlyFlagsFailedPersistence(t *testing.T) {
	tests := []struct {
		line            string
		wantWarning     bool
		wantPersistence bool
	}{
		{"rollout queue is ready", false, false},
		{"warning: session configuration changed", true, false},
		{"session transport error before HTTPS fallback", false, false},
		{"failed to record rollout items", false, true},
		{"state database error while persisting session", false, true},
		{"configWarning: configured profile is invalid", true, false},
	}
	for _, test := range tests {
		got := classifyStderr(test.line)
		if got.Warning != test.wantWarning || got.PersistenceError != test.wantPersistence {
			t.Fatalf("classifyStderr(%q) = %+v; want warning=%t persistenceError=%t", test.line, got, test.wantWarning, test.wantPersistence)
		}
	}
}

func TestProbeRPCMethodWhitelistCannotStartTurns(t *testing.T) {
	for _, method := range probeRPCMethods {
		if !isProbeRPCMethod(method) {
			t.Fatalf("probe method %q is unexpectedly disallowed", method)
		}
		if strings.Contains(method, "turn/start") || strings.Contains(method, "thread/resume") || strings.Contains(method, "thread/fork") || strings.Contains(method, "thread/start") {
			t.Fatalf("probe method list contains a mutating method: %q", method)
		}
	}
	for _, forbidden := range []string{"turn/start", "thread/resume", "thread/fork", "thread/start"} {
		if isProbeRPCMethod(forbidden) {
			t.Fatalf("probe must not permit %q", forbidden)
		}
	}
}

func TestStderrDiagnosticNeverRetainsUnlabelledContent(t *testing.T) {
	secret := "unlabelled user prompt body"
	got := classifyStderr("warning: " + secret)
	if strings.Contains(got.Message, secret) {
		t.Fatalf("stderr diagnostic leaked message content: %q", got.Message)
	}
	if !got.Warning || got.Category != "warning" {
		t.Fatalf("unexpected safe stderr diagnostic: %+v", got)
	}
}
