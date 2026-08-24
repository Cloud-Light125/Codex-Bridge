package appserver

import (
	"fmt"
	"strings"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

type turnStartDiagnostic struct {
	ApprovalPolicy string
	SandboxType    string
	NetworkAccess  bool
	Model          string
}

func buildTurnStartParams(threadID, text string, options TurnStartOptions) (map[string]any, turnStartDiagnostic, error) {
	approvalPolicy, err := toAppServerApprovalPolicy(options.ApprovalPolicy)
	if err != nil {
		return nil, turnStartDiagnostic{}, err
	}
	sandboxPolicy, sandboxType, networkAccess, err := toAppServerSandboxPolicy(options.SandboxMode, options.CWD)
	if err != nil {
		return nil, turnStartDiagnostic{}, err
	}
	if err := validateTurnStartCollaborationMode(options.CollaborationMode); err != nil {
		return nil, turnStartDiagnostic{}, err
	}

	params := map[string]any{
		"threadId":       threadID,
		"input":          []map[string]any{{"type": "text", "text": text, "text_elements": []any{}}},
		"approvalPolicy": approvalPolicy,
		"sandboxPolicy":  sandboxPolicy,
	}
	if cwd := strings.TrimSpace(options.CWD); cwd != "" {
		params["cwd"] = cwd
	}
	model := strings.TrimSpace(options.Model)
	if model != "" {
		params["model"] = model
	}
	if effort := strings.TrimSpace(options.ReasoningEffort); effort != "" {
		params["effort"] = effort
	}

	return params, turnStartDiagnostic{
		ApprovalPolicy: approvalPolicy,
		SandboxType:    sandboxType,
		NetworkAccess:  networkAccess,
		Model:          model,
	}, nil
}

func toAppServerApprovalPolicy(policy security.ApprovalPolicy) (string, error) {
	switch policy {
	case security.ApprovalNever, security.ApprovalOnRequest:
		return string(policy), nil
	default:
		return "", fmt.Errorf("unsupported approval policy: %s", policy)
	}
}

func toAppServerSandboxPolicy(mode security.SandboxMode, cwd string) (map[string]any, string, bool, error) {
	switch mode {
	case security.SandboxDangerFullAccess:
		return map[string]any{"type": "dangerFullAccess"}, "dangerFullAccess", true, nil
	case security.SandboxReadOnly:
		return map[string]any{"type": "readOnly", "networkAccess": false}, "readOnly", false, nil
	case security.SandboxWorkspaceWrite:
		policy := map[string]any{"type": "workspaceWrite", "networkAccess": false}
		if root := strings.TrimSpace(cwd); root != "" {
			policy["writableRoots"] = []string{root}
		}
		return policy, "workspaceWrite", false, nil
	default:
		return nil, "", false, fmt.Errorf("unsupported sandbox mode: %s", mode)
	}
}

func validateTurnStartCollaborationMode(mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "default":
		return nil
	default:
		return fmt.Errorf("collaboration mode is not supported by the current turn/start protocol: %s", mode)
	}
}

func shortThreadID(threadID string) string {
	threadID = strings.TrimSpace(threadID)
	if len(threadID) <= 8 {
		return threadID
	}
	return threadID[:8]
}
