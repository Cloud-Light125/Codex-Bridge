package security

import (
	"fmt"
	"strings"
)

type ApprovalPolicy string

const (
	ApprovalNever     ApprovalPolicy = "never"
	ApprovalOnRequest ApprovalPolicy = "on-request"
)

type SandboxMode string

const (
	SandboxDangerFullAccess SandboxMode = "danger-full-access"
	SandboxReadOnly         SandboxMode = "read-only"
	SandboxWorkspaceWrite   SandboxMode = "workspace-write"
)

func ParseSandboxMode(value string) (SandboxMode, error) {
	mode := SandboxMode(strings.TrimSpace(value))
	switch mode {
	case SandboxReadOnly, SandboxWorkspaceWrite:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported sandbox mode: %s", value)
	}
}
