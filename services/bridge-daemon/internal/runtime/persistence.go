package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/appserver"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

type fileSnapshot struct {
	Path    string
	Exists  bool
	Length  int64
	ModTime time.Time
}

var completedPersistenceRetryDelays = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	3 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

const (
	completedPersistenceInitialDelay = 0
	maxPersistenceRecords            = 512
	traceRetention                   = 30 * time.Minute
)

type turnTrace struct {
	mu sync.Mutex

	SelectedThreadID string
	TurnID           string
	StartedAt        time.Time
	Initial          control.ThreadPersistenceSnapshot
	Resumed          control.ThreadPersistenceSnapshot
	RolloutBefore    fileSnapshot
	UserItemID       string
	AssistantItemID  string
	Warnings         []string
	PersistenceError bool
	VerificationRun  bool
	VerificationDone bool
	Completed        bool
	TerminalState    string
}

func (t *turnTrace) setResume(snapshot control.ThreadPersistenceSnapshot) {
	t.mu.Lock()
	t.Resumed = snapshot
	t.mu.Unlock()
}

func (t *turnTrace) setTurnID(turnID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.TurnID != "" && t.TurnID != turnID {
		return fmt.Errorf("turn/start returned turnId %s after notification turnId %s", turnID, t.TurnID)
	}
	t.TurnID = turnID
	return nil
}

func (t *turnTrace) recordItem(itemType, itemID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch itemType {
	case "userMessage":
		if t.UserItemID == "" {
			t.UserItemID = itemID
		}
	case "agentMessage":
		if t.AssistantItemID == "" {
			t.AssistantItemID = itemID
		}
	}
}

func (t *turnTrace) addStderr(message string, persistenceError bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if message != "" && len(t.Warnings) < 32 {
		t.Warnings = append(t.Warnings, message)
	}
	t.PersistenceError = t.PersistenceError || persistenceError
}

func (t *turnTrace) startVerification() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.VerificationRun {
		return false
	}
	t.VerificationRun = true
	return true
}

func (t *turnTrace) finishVerification() {
	t.mu.Lock()
	t.VerificationDone = true
	t.mu.Unlock()
}

func (t *turnTrace) acceptsDiagnostics() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.VerificationDone && t.TerminalState == ""
}

func (t *turnTrace) markCompleted() {
	t.mu.Lock()
	t.Completed = true
	t.mu.Unlock()
}

func (t *turnTrace) routingActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.Completed && t.TerminalState == ""
}

func (t *turnTrace) setTerminalState(state string) {
	t.mu.Lock()
	t.TerminalState = state
	t.mu.Unlock()
}

func (t *turnTrace) terminalState() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.TerminalState
}

func (t *turnTrace) values() (turnID, userItemID, assistantItemID string, startedAt time.Time, before fileSnapshot, warnings []string, persistenceError bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.TurnID, t.UserItemID, t.AssistantItemID, t.StartedAt, t.RolloutBefore, append([]string(nil), t.Warnings...), t.PersistenceError
}

func (m *Manager) beginTurnTrace(threadID string, initial control.ThreadPersistenceSnapshot) *turnTrace {
	trace := &turnTrace{
		SelectedThreadID: threadID,
		StartedAt:        time.Now().UTC(),
		Initial:          initial,
		RolloutBefore:    statFile(initial.RolloutPath),
	}
	m.traceMu.Lock()
	m.pruneTracesLocked(time.Now().UTC())
	m.traces[threadID] = trace
	m.traceMu.Unlock()
	m.logger.Printf(
		"rpcTrace stage=send/accepted selectedThreadId=%s rolloutPath=%s rolloutExists=%t rolloutLength=%d",
		threadID, safeDiagnosticPath(initial.RolloutPath), trace.RolloutBefore.Exists, trace.RolloutBefore.Length,
	)
	return trace
}

func (m *Manager) pruneTracesLocked(now time.Time) {
	for threadID, trace := range m.traces {
		if trace.routingActive() || trace.StartedAt.IsZero() || now.Sub(trace.StartedAt) < traceRetention {
			continue
		}
		delete(m.traces, threadID)
	}
	for len(m.traces) > maxPersistenceRecords {
		oldestID := ""
		var oldestAt time.Time
		for threadID, trace := range m.traces {
			if trace.routingActive() {
				continue
			}
			if oldestID == "" || trace.StartedAt.Before(oldestAt) {
				oldestID, oldestAt = threadID, trace.StartedAt
			}
		}
		if oldestID == "" {
			return
		}
		delete(m.traces, oldestID)
	}
}

func (m *Manager) turnTrace(threadID string) *turnTrace {
	m.traceMu.RLock()
	defer m.traceMu.RUnlock()
	return m.traces[threadID]
}

func (m *Manager) traceForEvent(threadID, turnID string) (*turnTrace, string) {
	if trace := m.turnTrace(threadID); trace != nil {
		if trace.routingActive() {
			traceTurnID, _, _, _, _, _, _ := trace.values()
			if traceTurnID == "" || turnID == "" || traceTurnID == turnID {
				return trace, threadID
			}
		}
	}
	if turnID == "" {
		return nil, ""
	}
	m.traceMu.RLock()
	defer m.traceMu.RUnlock()
	for selectedID, trace := range m.traces {
		if !trace.routingActive() {
			continue
		}
		traceTurnID, _, _, _, _, _, _ := trace.values()
		if traceTurnID == turnID {
			return trace, selectedID
		}
	}
	return nil, ""
}

func (m *Manager) validateTraceEvent(method, threadID, turnID string) (*turnTrace, bool) {
	if trace := m.turnTrace(threadID); trace != nil {
		if !trace.routingActive() {
			if trace.terminalState() != "" {
				return trace, false
			}
			traceTurnID, _, _, _, _, _, _ := trace.values()
			if turnID == "" || turnID == traceTurnID {
				return trace, false
			}
			return nil, true
		}
		traceTurnID, _, _, _, _, _, _ := trace.values()
		if traceTurnID == "" {
			if turnID != "" {
				if err := trace.setTurnID(turnID); err != nil {
					m.failTurnTrace(threadID, StateThreadMismatch, err.Error())
					return trace, false
				}
			}
			return trace, true
		}
		if turnID == "" || traceTurnID == turnID {
			return trace, true
		}
		message := fmt.Sprintf("%s notification turnId %s does not match expected turnId %s", method, turnID, traceTurnID)
		m.failTurnTrace(threadID, StateThreadMismatch, message)
		m.logger.Printf("rpcTrace stage=%s selectedThreadId=%s notificationThreadId=%s notificationTurnId=%s expectedTurnId=%s result=thread-mismatch", safeMethod(method), threadID, threadID, turnID, traceTurnID)
		return trace, false
	}
	if trace, selectedThreadID := m.traceForEvent(threadID, turnID); trace != nil && selectedThreadID != threadID {
		message := fmt.Sprintf("%s notification threadId %s does not match selected threadId %s", method, threadID, selectedThreadID)
		m.failTurnTrace(selectedThreadID, StateThreadMismatch, message)
		m.logger.Printf("rpcTrace stage=%s selectedThreadId=%s notificationThreadId=%s notificationTurnId=%s result=thread-mismatch", safeMethod(method), selectedThreadID, threadID, turnID)
		return trace, false
	}
	return nil, true
}

func (m *Manager) failTurnTrace(threadID, stateName, message string) {
	if stateName == StateThreadMismatch || stateName == StateFailed {
		if trace := m.turnTrace(threadID); trace != nil {
			trace.setTerminalState(stateName)
		}
	}
	state := m.RuntimeState(threadID)
	state.ThreadID = threadID
	state.State = stateName
	state.Origin = "local"
	state.CanInterrupt = false
	state.Error = bridgelog.Redact(message)
	m.setState(state, true)
}

func (m *Manager) requireSelectedThread(selected string, snapshot control.ThreadPersistenceSnapshot, stage string) error {
	actualThreadID, identityState := selectedThreadIdentity(selected, snapshot)
	if identityState == StateThreadMismatch {
		err := m.threadMismatchError(selected, actualThreadID, stage)
		m.failTurnTrace(selected, StateThreadMismatch, err.Error())
		return err
	}
	if identityState == StateFailed {
		err := &ConflictError{
			Code: "ephemeral_thread", Message: "The selected Codex Thread is ephemeral and cannot be reported as persisted.",
			ThreadID: selected, CurrentState: StateFailed,
		}
		m.failTurnTrace(selected, StateFailed, err.Error())
		return err
	}
	return nil
}

func selectedThreadIdentity(selected string, snapshot control.ThreadPersistenceSnapshot) (string, string) {
	if snapshot.ThreadID == "" || snapshot.ThreadID != selected {
		return snapshot.ThreadID, StateThreadMismatch
	}
	if snapshot.Ephemeral {
		return snapshot.ThreadID, StateFailed
	}
	return snapshot.ThreadID, ""
}

func (m *Manager) threadMismatchError(selected, actual, stage string) *ConflictError {
	actualText := actual
	if actualText == "" {
		actualText = "<missing>"
	}
	return &ConflictError{
		Code:     "thread_mismatch",
		Message:  fmt.Sprintf("Codex %s returned Thread ID %s; selected Thread ID is %s.", stage, actualText, selected),
		ThreadID: selected, CurrentState: StateThreadMismatch,
	}
}

func (m *Manager) logRPCThread(stage, selected string, snapshot control.ThreadPersistenceSnapshot) {
	m.logger.Printf(
		"rpcTrace stage=%s selectedThreadId=%s threadId=%s sessionId=%s sourceKind=%s rolloutPath=%s ephemeral=%t cwd=%s updatedAt=%s status=%s turnCount=%d lastTurnId=%s foundTurn=%t userMessageItemId=%s assistantMessageItemId=%s",
		stage, selected, snapshot.ThreadID, snapshot.SessionID, snapshot.SourceKind,
		safeDiagnosticPath(snapshot.RolloutPath), snapshot.Ephemeral, safeDiagnosticPath(snapshot.CWD),
		snapshot.UpdatedAt, snapshot.Status, snapshot.TurnCount, snapshot.LastTurnID,
		snapshot.FoundTurn, snapshot.UserMessageItemID, snapshot.AssistantMessageItemID,
	)
}

func (m *Manager) lastVerification(threadID string) (control.PersistenceVerification, bool) {
	m.traceMu.RLock()
	defer m.traceMu.RUnlock()
	verification, ok := m.lastVerifications[threadID]
	return verification, ok
}

func (m *Manager) saveVerification(verification control.PersistenceVerification) {
	m.traceMu.Lock()
	m.lastVerifications[verification.ThreadID] = verification
	for len(m.lastVerifications) > maxPersistenceRecords {
		oldestID := ""
		var oldestAt time.Time
		for threadID, candidate := range m.lastVerifications {
			at, err := time.Parse(time.RFC3339Nano, candidate.VerifiedAt)
			if err != nil {
				at = time.Time{}
			}
			if oldestID == "" || at.Before(oldestAt) {
				oldestID, oldestAt = threadID, at
			}
		}
		if oldestID == "" {
			break
		}
		delete(m.lastVerifications, oldestID)
	}
	m.traceMu.Unlock()
}

// VerifyThreadPersistence performs no resume, start, fork, approval, or prompt
// operation. It compares the primary process with a newly initialized App
// Server and, when available, reads the rollout file without modifying it.
func (m *Manager) VerifyThreadPersistence(ctx context.Context, threadID string) (control.PersistenceVerification, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return control.PersistenceVerification{}, &ValidationError{Code: "invalid_thread_id", Message: "Thread ID is required"}
	}
	unlock := m.lockThread(threadID)
	defer unlock()
	stateBefore := m.RuntimeState(threadID)
	if isActiveState(stateBefore.State) && stateBefore.State != StateCompletedUnverified {
		return control.PersistenceVerification{}, busyError(threadID, stateBefore.State)
	}
	client, err := m.runningClient()
	if err != nil {
		return control.PersistenceVerification{}, err
	}
	raw, err := client.ThreadReadHistory(ctx, threadID, appserver.DefaultHistoryTurnLimit)
	if err != nil {
		return control.PersistenceVerification{}, fmt.Errorf("primary thread/read: %w", err)
	}
	mainSnapshot := persistenceSnapshot(raw, "")
	expectedTurnID := mainSnapshot.LastTurnID
	if expectedTurnID != "" {
		mainSnapshot = persistenceSnapshot(raw, expectedTurnID)
	}
	var before fileSnapshot
	var startedAt time.Time
	var warnings []string
	var persistenceError bool
	if trace := m.turnTrace(threadID); trace != nil {
		traceTurnID, _, _, traceStartedAt, traceBefore, traceWarnings, tracePersistenceError := trace.values()
		if traceTurnID != "" {
			expectedTurnID = traceTurnID
			mainSnapshot = persistenceSnapshot(raw, expectedTurnID)
		}
		before, startedAt, warnings, persistenceError = traceBefore, traceStartedAt, traceWarnings, tracePersistenceError
	}
	verification, verifyErr := m.verifySnapshots(ctx, threadID, expectedTurnID, mainSnapshot, before, startedAt, warnings, persistenceError)
	stateAfter := m.RuntimeState(threadID)
	if isActiveState(stateAfter.State) && stateAfter.State != StateCompletedUnverified {
		return control.PersistenceVerification{}, busyError(threadID, stateAfter.State)
	}
	m.saveVerification(verification)
	m.applyVerificationState(verification)
	return verification, verifyErr
}

func (m *Manager) verifyCompletedTurn(threadID, turnID string, trace *turnTrace) {
	if trace == nil || !trace.startVerification() {
		return
	}
	defer trace.finishVerification()
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()
	if !waitForPersistenceWindow(ctx, completedPersistenceInitialDelay) {
		m.completePersistenceFailure(threadID, turnID, "persistence verification was cancelled before the write window elapsed", nil)
		return
	}
	client, err := m.runningClient()
	if err != nil {
		m.completePersistenceFailure(threadID, turnID, fmt.Sprintf("primary App Server unavailable: %v", err), nil)
		return
	}
	_, _, _, startedAt, before, warnings, persistenceError := trace.values()
	var lastVerification *control.PersistenceVerification
	var lastReadError error
	for attempt := 0; attempt <= len(completedPersistenceRetryDelays); attempt++ {
		raw, readErr := client.ThreadReadHistory(ctx, threadID, appserver.DefaultHistoryTurnLimit)
		if readErr != nil {
			lastReadError = readErr
			m.logger.Printf("persistenceRetry selectedThreadId=%s expectedTurnId=%s attempt=%d result=primary-read-failed error=%s", threadID, turnID, attempt+1, bridgelog.Redact(readErr.Error()))
		} else {
			mainSnapshot := persistenceSnapshot(raw, turnID)
			m.logRPCThread(fmt.Sprintf("thread/read-after-completed-attempt-%d", attempt+1), threadID, mainSnapshot)
			verification := m.completedWithoutProbe(threadID, turnID, mainSnapshot, before, startedAt, warnings, persistenceError)
			if verification.Status == StatePersisted {
				m.logger.Printf("latency stage=final_resolved threadId=%s turnId=%s at=%s source=retry", threadID, turnID, nowText())
				m.saveVerification(verification)
				m.applyVerificationState(verification)
				if m.autoPersistenceProbe {
					go m.probeCompletedTurnDiagnostic(threadID, turnID, mainSnapshot, before, startedAt, warnings, persistenceError)
				}
				return
			}
			lastVerification = &verification
			if verification.Status == StatePersisted || !persistenceVerificationRetryable(threadID, mainSnapshot, verification) {
				m.saveVerification(verification)
				m.applyVerificationState(verification)
				return
			}
			m.logger.Printf("persistenceRetry selectedThreadId=%s expectedTurnId=%s attempt=%d result=not-visible-yet status=%s", threadID, turnID, attempt+1, verification.Status)
		}
		if attempt == len(completedPersistenceRetryDelays) || !waitForPersistenceWindow(ctx, completedPersistenceRetryDelays[attempt]) {
			break
		}
	}
	if lastVerification != nil {
		m.saveVerification(*lastVerification)
		m.applyVerificationState(*lastVerification)
		return
	}
	m.completePersistenceFailure(threadID, turnID, fmt.Sprintf("completed thread/read failed after finite retries: %v", lastReadError), nil)
}

func (m *Manager) probeCompletedTurnDiagnostic(threadID, turnID string, main control.ThreadPersistenceSnapshot, before fileSnapshot, startedAt time.Time, warnings []string, persistenceError bool) {
	ctx, cancel := context.WithTimeout(m.ctx, 20*time.Second)
	defer cancel()
	verification, err := m.verifySnapshots(ctx, threadID, turnID, main, before, startedAt, warnings, persistenceError)
	result := verification.Status
	if err != nil {
		result = "diagnostic-failed"
	}
	m.logger.Printf("persistenceProbeDiagnostic selectedThreadId=%s expectedTurnId=%s result=%s", threadID, turnID, result)
}

func waitForPersistenceWindow(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func persistenceVerificationRetryable(threadID string, main control.ThreadPersistenceSnapshot, verification control.PersistenceVerification) bool {
	if verification.Status != StatePersistenceFailed || main.ThreadID != threadID || main.Ephemeral {
		return false
	}
	if verification.Probe.ThreadID != "" && verification.Probe.ThreadID != threadID {
		return false
	}
	return true
}

func (m *Manager) completedWithoutProbe(
	threadID, turnID string,
	mainSnapshot control.ThreadPersistenceSnapshot,
	before fileSnapshot,
	startedAt time.Time,
	warnings []string,
	persistenceError bool,
) control.PersistenceVerification {
	verification := control.PersistenceVerification{
		ThreadID: threadID, ExpectedTurnID: turnID, Status: StateCompletedUnverified,
		Message: "Turn 已完成，正在等待主 App Server 返回正式 assistant message。",
		Main:    mainSnapshot, Warnings: append([]string(nil), warnings...),
		Environment: m.codexEnvironment(threadID), VerifiedAt: nowText(),
	}
	verification.Rollout = verifyRollout(mainSnapshot.RolloutPath, before, startedAt,
		turnID, mainSnapshot.UserMessageItemID, mainSnapshot.AssistantMessageItemID)
	m.logRolloutVerification(threadID, turnID, verification.Rollout)
	switch {
	case mainSnapshot.ThreadID != threadID:
		verification.Status = StateThreadMismatch
		verification.Message = "Thread ID 不一致：完成后的 thread/read 没有返回原选择的 Thread。"
	case mainSnapshot.Ephemeral:
		verification.Status = StatePersistenceFailed
		verification.Message = "持久化失败：Thread 为 ephemeral。"
	case !mainSnapshot.FoundTurn:
		verification.Status = StatePersistenceFailed
		verification.Message = "持久化失败：当前 App Server 的 thread/read 找不到目标 Turn。"
	case !strings.EqualFold(mainSnapshot.TurnStatus, "completed"):
		verification.Status = StatePersistenceFailed
		verification.Message = "持久化失败：完成后的 thread/read 中目标 Turn 状态不是 completed。"
	case mainSnapshot.AssistantMessageItemID == "":
		verification.Status = StatePersistenceFailed
		verification.Message = "持久化失败：完成后的 thread/read 尚未包含正式 assistant message。"
	default:
		verification.Status = StatePersisted
		verification.Message = fmt.Sprintf("已确认：主 App Server 已读取到 Turn %s 的正式 assistant message；独立 probe 不阻塞 Mirror。", turnID)
		if persistenceError {
			verification.Message += " stderr 持久化警告保留用于诊断。"
		}
	}
	return verification
}

func (m *Manager) verifySnapshots(
	ctx context.Context,
	threadID, expectedTurnID string,
	mainSnapshot control.ThreadPersistenceSnapshot,
	before fileSnapshot,
	startedAt time.Time,
	warnings []string,
	persistenceError bool,
) (control.PersistenceVerification, error) {
	verification := control.PersistenceVerification{
		ThreadID: threadID, ExpectedTurnID: expectedTurnID, Status: StateCompletedUnverified,
		Main: mainSnapshot, Warnings: append([]string(nil), warnings...),
		Environment: m.codexEnvironment(threadID), VerifiedAt: nowText(),
	}
	verification.Rollout = verifyRollout(mainSnapshot.RolloutPath, before, startedAt,
		expectedTurnID, mainSnapshot.UserMessageItemID, mainSnapshot.AssistantMessageItemID)
	m.logRolloutVerification(threadID, expectedTurnID, verification.Rollout)

	detection, _ := m.connectionConfig()
	status := m.Status()
	probeResult, probeErr := appserver.ProbeThread(ctx, detection.Path, m.cwd, status.Version, threadID, m.logger)
	if probeErr == nil {
		verification.Probe = persistenceSnapshot(probeResult.Raw, expectedTurnID)
	}
	m.logger.Printf(
		"persistenceProbe selectedThreadId=%s expectedTurnId=%s pid=%d codexPath=%s cwd=%s methods=%s error=%s",
		threadID, expectedTurnID, probeResult.PID, safeDiagnosticPath(probeResult.CodexPath),
		safeDiagnosticPath(probeResult.CWD), strings.Join(probeResult.RequestedMethods, "->"), bridgelog.Redact(probeResult.ErrorSummary),
	)
	if trace := m.turnTrace(threadID); trace != nil {
		_, _, _, _, _, latestWarnings, latestPersistenceError := trace.values()
		persistenceError = persistenceError || latestPersistenceError
		verification.Warnings = mergeWarnings(verification.Warnings, latestWarnings)
	}

	statusName, message := evaluatePersistence(threadID, expectedTurnID, mainSnapshot, verification.Probe, probeErr, persistenceError)
	if trace := m.turnTrace(threadID); trace != nil && trace.terminalState() == StateThreadMismatch {
		statusName = StateThreadMismatch
		message = "Thread ID 不一致：该发送已被拒绝，不能通过后续事件升级为持久化成功。"
	}
	if statusName == StatePersisted && mainSnapshot.RolloutPath != "" && verification.Probe.RolloutPath != "" && !samePath(mainSnapshot.RolloutPath, verification.Probe.RolloutPath) {
		statusName = StatePersistenceFailed
		message = "数据目录不一致：主 App Server 与新 App Server 返回了不同的 rollout path。"
	}
	if statusName == StatePersisted && (!verification.Rollout.Exists || !verification.Rollout.ContainsIdentifier) {
		statusName = StatePersistenceFailed
		message = "未持久化：rollout 尚未包含目标 Turn 或正式消息标识。"
	}
	verification.Status = statusName
	verification.Message = message
	if statusName != StatePersisted {
		return verification, errors.New(message)
	}
	return verification, nil
}

func mergeWarnings(existing, incoming []string) []string {
	seen := make(map[string]bool, len(existing)+len(incoming))
	result := make([]string, 0, len(existing)+len(incoming))
	for _, values := range [][]string{existing, incoming} {
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" || seen[value] || len(result) >= 32 {
				continue
			}
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

func evaluatePersistence(
	selectedThreadID, expectedTurnID string,
	main, probe control.ThreadPersistenceSnapshot,
	probeErr error,
	persistenceError bool,
) (string, string) {
	if main.ThreadID != selectedThreadID || (probe.ThreadID != "" && probe.ThreadID != selectedThreadID) {
		return StateThreadMismatch, "Thread ID 不一致：持久化验证拒绝把结果路由到所选 Thread。"
	}
	if main.Ephemeral || probe.Ephemeral {
		return StatePersistenceFailed, "持久化失败：Thread 为 ephemeral。"
	}
	if expectedTurnID == "" || !main.FoundTurn {
		return StatePersistenceFailed, "未持久化：当前 Bridge App Server 的 thread/read 找不到目标 Turn。"
	}
	if !strings.EqualFold(main.TurnStatus, "completed") {
		return StatePersistenceFailed, "未持久化：当前 Bridge App Server 中的目标 Turn 尚未处于 completed 状态。"
	}
	if main.AssistantMessageItemID == "" {
		return StatePersistenceFailed, "未持久化：当前 Bridge App Server 尚未读取到正式 assistant message。"
	}
	if probeErr != nil {
		return StatePersistenceFailed, "未持久化：新的独立 Codex App Server 验证失败。"
	}
	if probe.ThreadID != selectedThreadID || !probe.FoundTurn {
		return StatePersistenceFailed, "未持久化：当前 Bridge 进程能看到该 Turn，但新的 App Server 无法读取。"
	}
	if !strings.EqualFold(probe.TurnStatus, "completed") {
		return StatePersistenceFailed, "未持久化：新的独立 App Server 读取到目标 Turn，但其状态不是 completed。"
	}
	if probe.AssistantMessageItemID == "" {
		return StatePersistenceFailed, "未持久化：新的独立 App Server 尚未读取到正式 assistant message。"
	}
	if persistenceError {
		return StatePersisted, fmt.Sprintf("已持久化：rollout 与新的独立 Codex App Server 均已读取到 Turn %s 的正式消息；先前 stderr 警告未被持久化证据证实。", expectedTurnID)
	}
	return StatePersisted, fmt.Sprintf("已持久化：新的独立 Codex App Server 已读取到 Turn %s。", expectedTurnID)
}

func completedNotificationState(status string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	switch {
	case status == "completed":
		return StateCompletedUnverified
	case strings.Contains(status, "interrupt"), strings.Contains(status, "cancel"), strings.Contains(status, "fail"), strings.Contains(status, "error"):
		return StateFailed
	default:
		return StateFailed
	}
}

func (m *Manager) applyVerificationState(verification control.PersistenceVerification) {
	current := m.RuntimeState(verification.ThreadID)
	if current.TurnID == verification.ExpectedTurnID && current.State == StatePersisted && verification.Status == StatePersisted {
		return
	}
	state := current
	state.ThreadID = verification.ThreadID
	state.TurnID = verification.ExpectedTurnID
	state.State = verification.Status
	state.Origin = "local"
	state.Error = ""
	state.Persistence = &verification
	state.CanInterrupt = false
	m.setState(state, true)
	m.broker.PublishScoped(events.TurnPersistence, verification.ThreadID, verification.ExpectedTurnID, "", map[string]any{"verification": verification, "runtime": state})
	if verification.Status == StateCompletedUnverified {
		return
	}
	if verification.Status == StatePersisted {
		m.broker.PublishScoped(events.TurnCompleted, verification.ThreadID, verification.ExpectedTurnID, "", map[string]any{"status": StatePersisted, "verification": verification})
	} else {
		m.broker.PublishScoped(events.TurnFailed, verification.ThreadID, verification.ExpectedTurnID, "", map[string]any{"status": verification.Status, "error": verification.Message, "verification": verification})
	}
}

func (m *Manager) completePersistenceFailure(threadID, turnID, message string, verification *control.PersistenceVerification) {
	if verification == nil {
		value := control.PersistenceVerification{
			ThreadID: threadID, ExpectedTurnID: turnID, Status: StatePersistenceFailed,
			Message: bridgelog.Redact(message), Environment: m.codexEnvironment(threadID), Warnings: []string{}, VerifiedAt: nowText(),
		}
		verification = &value
		m.saveVerification(value)
	}
	verification.Status = StatePersistenceFailed
	if verification.Message == "" {
		verification.Message = bridgelog.Redact(message)
	}
	m.applyVerificationState(*verification)
}

func persistenceSnapshot(raw map[string]any, expectedTurnID string) control.ThreadPersistenceSnapshot {
	payload := threadPayload(raw)
	snapshot := control.ThreadPersistenceSnapshot{
		ThreadID: textValue(payload["id"]), SessionID: textValue(payload["sessionId"]),
		SourceKind: sourceKind(payload["source"]), RolloutPath: textValue(payload["path"]),
		Ephemeral: boolText(payload["ephemeral"]), CWD: textValue(payload["cwd"]),
		UpdatedAt: diagnosticTime(payload["updatedAt"]), Status: statusText(payload["status"]),
	}
	turns := objectSlice(payload["turns"])
	snapshot.TurnCount = len(turns)
	if len(turns) > 0 {
		snapshot.LastTurnID = firstNonEmpty(textValue(turns[len(turns)-1]["id"]), textValue(turns[len(turns)-1]["turnId"]))
	}
	detail := control.NormalizeThreadDetail(raw)
	mode := control.FinalSelectionModeForHistory(detail.HistoryMode)
	for _, turn := range detail.Turns {
		turnID := turn.TurnID
		if expectedTurnID != "" && turnID != expectedTurnID {
			continue
		}
		if expectedTurnID == "" && turnID != snapshot.LastTurnID {
			continue
		}
		snapshot.FoundTurn = turnID != ""
		snapshot.TurnStatus = turn.Status
		for _, item := range turn.Items {
			if item.Type == "userMessage" {
				snapshot.UserMessageItemID = item.ItemID
			}
		}
		if item, ok := control.SelectFinalAssistantItem(turn, mode); ok {
			snapshot.AssistantMessageItemID = item.ItemID
		}
		break
	}
	return snapshot
}

func objectSlice(value any) []map[string]any {
	values, _ := value.([]any)
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if object, ok := value.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func sourceKind(value any) string {
	if text := textValue(value); text != "" {
		return text
	}
	object, _ := value.(map[string]any)
	return firstNonEmpty(textValue(object["kind"]), textValue(object["type"]), textValue(object["source"]))
}

func boolText(value any) bool {
	result, _ := value.(bool)
	return result
}

func diagnosticTime(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return time.Unix(int64(typed), 0).UTC().Format(time.RFC3339)
	case int64:
		return time.Unix(typed, 0).UTC().Format(time.RFC3339)
	case int:
		return time.Unix(int64(typed), 0).UTC().Format(time.RFC3339)
	default:
		return ""
	}
}

func statFile(path string) fileSnapshot {
	result := fileSnapshot{Path: strings.TrimSpace(path)}
	if result.Path == "" {
		return result
	}
	info, err := os.Stat(result.Path)
	if err != nil || info.IsDir() {
		return result
	}
	result.Exists = true
	result.Length = info.Size()
	result.ModTime = info.ModTime().UTC()
	return result
}

func verifyRollout(path string, before fileSnapshot, startedAt time.Time, identifiers ...string) control.RolloutVerification {
	after := statFile(path)
	result := control.RolloutVerification{
		Path: path, Exists: after.Exists, BeforeLength: before.Length, AfterLength: after.Length,
		LengthIncreased:   after.Exists && after.Length > before.Length,
		ModifiedAfterSend: after.Exists && !startedAt.IsZero() && !after.ModTime.Before(startedAt),
	}
	if path == "" {
		result.Error = "thread/read did not provide a rollout path"
		return result
	}
	if !after.Exists {
		result.Error = "rollout file does not exist"
		return result
	}
	contains, err := fileContainsAny(path, identifiers...)
	if err != nil {
		result.Error = bridgelog.Redact(err.Error())
		return result
	}
	result.ContainsIdentifier = contains
	return result
}

func (m *Manager) logRolloutVerification(threadID, turnID string, result control.RolloutVerification) {
	m.logger.Printf(
		"persistenceFile selectedThreadId=%s turnId=%s rolloutPath=%s exists=%t beforeLength=%d afterLength=%d lengthIncreased=%t modifiedAfterSend=%t containsIdentifier=%t error=%s",
		threadID, turnID, safeDiagnosticPath(result.Path), result.Exists, result.BeforeLength, result.AfterLength,
		result.LengthIncreased, result.ModifiedAfterSend, result.ContainsIdentifier, bridgelog.Redact(result.Error),
	)
}

func fileContainsAny(path string, identifiers ...string) (bool, error) {
	needles := make([]string, 0, len(identifiers))
	for _, value := range identifiers {
		if value = strings.TrimSpace(value); value != "" {
			needles = append(needles, value)
		}
	}
	if len(needles) == 0 {
		return false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 64*1024)
	for {
		line, readErr := reader.ReadString('\n')
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				return true, nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return false, nil
		}
		if readErr != nil {
			return false, readErr
		}
	}
}

func (m *Manager) codexEnvironment(threadID string) control.CodexEnvironment {
	probeEnvironment := appserver.ProbeEnvironmentSnapshot()
	status := m.Status()
	username := strings.TrimSpace(os.Getenv("USERNAME"))
	if current, err := user.Current(); err == nil && strings.TrimSpace(current.Username) != "" {
		username = current.Username
	}
	environment := control.CodexEnvironment{
		CodexCLIPath: status.CodexCLIPath, CodexCLIVersion: status.CodexCLIVersion,
		Username: username, UserProfile: probeEnvironment.UserProfile, Home: probeEnvironment.Home,
		CodexHomeExplicit: probeEnvironment.CodexHomeExplicit, CodexHome: probeEnvironment.CodexHome,
		ResolvedCodexDataRoot:     probeEnvironment.ResolvedCodexDataRoot,
		AppServerWorkingDirectory: m.cwd, Processes: discoverCodexProcesses(), DesktopEnvironmentKnown: false,
	}
	environment.MatchingRolloutPaths = matchingRolloutPaths(probeEnvironment.ResolvedCodexDataRoot, threadID)
	environment.MultipleMatchingRollouts = len(environment.MatchingRolloutPaths) > 1
	return environment
}

func matchingRolloutPaths(codexRoot, threadID string) []string {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" || strings.TrimSpace(codexRoot) == "" {
		return []string{}
	}
	sessionsRoot := filepath.Join(codexRoot, "sessions")
	result := []string{}
	_ = filepath.WalkDir(sessionsRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() {
			return nil
		}
		if strings.Contains(strings.ToLower(entry.Name()), strings.ToLower(threadID)) && len(result) < 32 {
			result = append(result, path)
		}
		return nil
	})
	return result
}

func safeDiagnosticPath(value string) string {
	value = bridgelog.Redact(strings.TrimSpace(value))
	if len(value) > 1024 {
		return value[:1024] + "…"
	}
	return value
}

func samePath(left, right string) bool {
	leftPath, leftErr := filepath.Abs(left)
	rightPath, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return strings.EqualFold(filepath.Clean(leftPath), filepath.Clean(rightPath))
}

func parsePID(value string) int {
	result, _ := strconv.Atoi(strings.TrimSpace(value))
	return result
}
