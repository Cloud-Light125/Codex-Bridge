package appserver

import (
	"context"
	"errors"
	"strings"

	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

var probeRPCMethods = []string{"initialize", "initialized", "thread/read", "thread/turns/list", "thread/items/list"}

// ProbeResult records how a short-lived, independent app-server was launched.
// Raw is the bounded compatible history result so callers can verify turn IDs
// without treating this process's in-memory state as proof of persistence.
type ProbeResult struct {
	Raw              map[string]any
	PID              int
	CodexPath        string
	CWD              string
	Version          string
	Environment      ProbeEnvironment
	RequestedMethods []string
	ErrorSummary     string
}

// ProbeThread starts a new, short-lived app-server using the exact executable,
// working directory, and inherited environment supplied to the primary client.
// Its protocol is initialize -> initialized -> thread/read(false), followed by
// the read-only paginated history chain when the server advertises it. It
// never resumes, starts, forks, or writes to a thread.
func ProbeThread(ctx context.Context, codexPath, cwd, version, threadID string, logger *bridgelog.SafeLogger) (result ProbeResult, err error) {
	result = ProbeResult{
		CodexPath:        strings.TrimSpace(codexPath),
		CWD:              strings.TrimSpace(cwd),
		Version:          strings.TrimSpace(version),
		Environment:      currentProbeEnvironment(),
		RequestedMethods: append([]string(nil), probeRPCMethods...),
	}
	if strings.TrimSpace(threadID) == "" {
		err = errors.New("thread ID is required for persistence probe")
		result.ErrorSummary = safeProbeError(err)
		return result, err
	}

	client := NewClient(result.CodexPath, result.CWD, result.Version, logger, nil)
	if err = client.Start(ctx); err != nil {
		result.ErrorSummary = safeProbeError(err)
		return result, err
	}
	result.PID = client.PID()
	defer func() {
		closeErr := client.Close()
		if err == nil && closeErr != nil {
			err = closeErr
			result.ErrorSummary = safeProbeError(closeErr)
		}
	}()

	result.Raw, err = client.ThreadReadHistory(ctx, threadID, DefaultHistoryTurnLimit)
	if err != nil {
		result.ErrorSummary = safeProbeError(err)
	}
	return result, err
}

func isProbeRPCMethod(method string) bool {
	for _, allowed := range probeRPCMethods {
		if method == allowed {
			return true
		}
	}
	return false
}
