package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/appserver"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/security"
)

type ConflictError struct {
	Code         string
	Message      string
	ThreadID     string
	CurrentState string
}

func (e *ConflictError) Error() string { return e.Message }

type ValidationError struct {
	Code    string
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

func (m *Manager) RuntimeState(threadID string) control.RuntimeState {
	pending := m.interactions.PendingCount(threadID)
	m.stateMu.RLock()
	state, ok := m.states[threadID]
	m.stateMu.RUnlock()
	if !ok {
		state = control.RuntimeState{ThreadID: threadID, State: StateIdle}
	}
	if state.Persistence == nil {
		if verification, found := m.lastVerification(threadID); found {
			state.Persistence = &verification
		}
	}
	state.PendingInteractionCount = pending
	connected := m.Status().AppServerRunning
	state.CanInterrupt = controllableOrigin(state.Origin) && isActiveState(state.State) && state.State != StateInterrupting && state.State != StateCompletedUnverified && state.TurnID != "" && connected
	state.CanSend = connected && !isActiveState(state.State) && pending == 0 && state.State != StateUnknown
	return state
}

func (m *Manager) setState(state control.RuntimeState, publish bool) {
	if state.ThreadID == "" {
		return
	}
	state.PendingInteractionCount = m.interactions.PendingCount(state.ThreadID)
	if state.Persistence == nil {
		if verification, found := m.lastVerification(state.ThreadID); found {
			state.Persistence = &verification
		}
	}
	state.LastActivityAt = nowText()
	connected := m.Status().AppServerRunning
	state.CanInterrupt = controllableOrigin(state.Origin) && isActiveState(state.State) && state.State != StateInterrupting && state.State != StateCompletedUnverified && state.TurnID != "" && connected
	state.CanSend = connected && !isActiveState(state.State) && state.PendingInteractionCount == 0 && state.State != StateUnknown
	m.stateMu.Lock()
	previous, existed := m.states[state.ThreadID]
	m.states[state.ThreadID] = state
	m.stateMu.Unlock()
	if publish && (!existed || previous.State != state.State || previous.TurnID != state.TurnID || previous.PendingInteractionCount != state.PendingInteractionCount) {
		m.broker.PublishScoped(events.TurnStatusChanged, state.ThreadID, state.TurnID, "", map[string]any{"runtime": state})
		m.broker.PublishScoped(events.ThreadUpdated, state.ThreadID, state.TurnID, "", map[string]any{"runtime": state})
	}
}

func (m *Manager) reconcileActivity(activity control.ThreadActivity) {
	if activity.ThreadID == "" {
		return
	}
	if strings.EqualFold(activity.Status, "unknown") && activity.TurnID == "" {
		return
	}
	m.stateMu.RLock()
	state, ok := m.states[activity.ThreadID]
	starting := m.starting[activity.ThreadID]
	m.stateMu.RUnlock()
	if !ok {
		state = control.RuntimeState{ThreadID: activity.ThreadID, State: StateIdle}
	}
	if starting {
		return
	}
	if activity.Active {
		if !controllableOrigin(state.Origin) || !isActiveState(state.State) {
			state.State = StateRunningExternal
			state.Origin = "external"
			state.StartedAt = firstNonEmpty(state.StartedAt, nowText())
			state.CanInterrupt = false
		}
		if activity.TurnID != "" {
			state.TurnID = activity.TurnID
		}
		if activity.WaitingApproval {
			state.State = StateWaitingApproval
		}
		if activity.WaitingInput {
			state.State = StateWaitingUserInput
		}
		m.setState(state, true)
		return
	}
	if isActiveState(state.State) {
		if controllableOrigin(state.Origin) {
			state.State = StateCompletedUnverified
		} else {
			state.State = StateIdle
			state.TurnID = ""
			state.Origin = ""
		}
		state.CanInterrupt = false
		m.setState(state, true)
		return
	}
	if state.State == StateUnknown {
		state.State = StateIdle
		state.TurnID = ""
		state.Origin = ""
		state.Error = ""
		m.setState(state, true)
	}
}

func (m *Manager) StartTurn(ctx context.Context, threadID string, request control.StartTurnRequest) (control.TurnAccepted, error) {
	threadID = strings.TrimSpace(threadID)
	text := strings.TrimSpace(request.Text)
	if threadID == "" {
		return control.TurnAccepted{}, &ValidationError{Code: "invalid_thread_id", Message: "Thread ID 无效"}
	}
	if text == "" {
		return control.TurnAccepted{}, &ValidationError{Code: "empty_text", Message: "消息内容不能为空"}
	}
	lock := m.threadLock(threadID)
	lock.Lock()
	defer lock.Unlock()

	client, err := m.runningClient()
	if err != nil {
		return control.TurnAccepted{}, err
	}
	m.logger.Printf("rpcTrace stage=thread/read-request selectedThreadId=%s requestThreadId=%s includeTurns=true", threadID, threadID)
	raw, err := client.ThreadRead(ctx, threadID, true)
	if err != nil {
		return control.TurnAccepted{}, err
	}
	initial := persistenceSnapshot(raw, "")
	m.logRPCThread("thread/read", threadID, initial)
	if err := m.requireSelectedThread(threadID, initial, "thread/read"); err != nil {
		return control.TurnAccepted{}, err
	}
	activity := control.ActivityFromThreadRead(raw)
	if activity.ThreadID == "" {
		return control.TurnAccepted{}, &ValidationError{Code: "thread_not_found", Message: "指定的 Codex Thread 不存在"}
	}
	if activity.Archived {
		return control.TurnAccepted{}, &ConflictError{Code: "thread_archived", Message: "该 Codex Thread 已归档，不能开始新的任务。", ThreadID: threadID, CurrentState: StateIdle}
	}
	current := m.RuntimeState(threadID)
	if activity.Active || isActiveState(current.State) || current.PendingInteractionCount > 0 {
		state := current.State
		if activity.Active && !controllableOrigin(current.Origin) {
			state = StateRunningExternal
			m.setState(control.RuntimeState{ThreadID: threadID, State: state, TurnID: activity.TurnID, Origin: "external", StartedAt: nowText()}, true)
		}
		return control.TurnAccepted{}, busyError(threadID, state)
	}

	payload := threadPayload(raw)
	cwd := textValue(payload["cwd"])
	if strings.TrimSpace(request.CWD) != "" {
		cwd = strings.TrimSpace(request.CWD)
	}
	trace := m.beginTurnTrace(threadID, initial)
	m.logger.Printf("rpcTrace stage=thread/resume-request selectedThreadId=%s requestThreadId=%s", threadID, threadID)
	resumeRaw, err := client.ThreadResume(ctx, threadID, cwd)
	if err != nil {
		m.failTurnTrace(threadID, StateFailed, fmt.Sprintf("thread/resume failed: %v", err))
		return control.TurnAccepted{}, fmt.Errorf("resume Codex thread: %w", err)
	}
	resumed := persistenceSnapshot(resumeRaw, "")
	m.logRPCThread("thread/resume", threadID, resumed)
	if err := m.requireSelectedThread(threadID, resumed, "thread/resume"); err != nil {
		m.failTurnTrace(threadID, StateThreadMismatch, err.Error())
		return control.TurnAccepted{}, err
	}
	trace.setResume(resumed)
	m.stateMu.Lock()
	m.starting[threadID] = true
	m.stateMu.Unlock()
	defer func() {
		m.stateMu.Lock()
		delete(m.starting, threadID)
		m.stateMu.Unlock()
	}()
	options, err := turnStartOptions(request, cwd, m.Status().SandboxMode)
	if err != nil {
		return control.TurnAccepted{}, fmt.Errorf("prepare Codex turn security policy: %w", err)
	}
	m.logger.Printf("rpcTrace stage=turn/start-request selectedThreadId=%s requestThreadId=%s", threadID, threadID)
	result, err := client.TurnStart(ctx, threadID, text, options)
	if err != nil {
		if compatibilityError := turnStartProtocolCompatibilityError(err); compatibilityError != nil {
			state := control.RuntimeState{ThreadID: threadID, State: StateFailed, Origin: "local", Error: compatibilityError.Message}
			m.setState(state, true)
			m.failTurnTrace(threadID, StateFailed, compatibilityError.Message)
			return control.TurnAccepted{}, compatibilityError
		}
		state := control.RuntimeState{ThreadID: threadID, State: StateFailed, Origin: "local", Error: "Turn 启动失败"}
		m.setState(state, true)
		m.failTurnTrace(threadID, StateFailed, err.Error())
		return control.TurnAccepted{}, fmt.Errorf("start Codex turn: %w", err)
	}
	turn := nestedMap(result, "turn")
	turnID := firstNonEmpty(textValue(turn["id"]), textValue(result["turnId"]))
	if turnID == "" {
		state := control.RuntimeState{ThreadID: threadID, State: StateFailed, Origin: "local", Error: "App Server 未返回 Turn ID"}
		m.setState(state, true)
		m.failTurnTrace(threadID, StateFailed, "Codex app-server did not return a turn id")
		return control.TurnAccepted{}, errors.New("Codex app-server did not return a turn id")
	}
	returnedThreadID := firstNonEmpty(
		textValue(result["threadId"]), textValue(nestedMap(result, "thread")["id"]),
		textValue(turn["threadId"]),
	)
	if returnedThreadID != "" && returnedThreadID != threadID {
		err := m.threadMismatchError(threadID, returnedThreadID, "turn/start")
		m.failTurnTrace(threadID, StateThreadMismatch, err.Error())
		return control.TurnAccepted{}, err
	}
	if err := trace.setTurnID(turnID); err != nil {
		m.failTurnTrace(threadID, StateThreadMismatch, err.Error())
		return control.TurnAccepted{}, err
	}
	m.logger.Printf("rpcTrace stage=turn/start selectedThreadId=%s requestThreadId=%s responseThreadId=%s turnId=%s", threadID, threadID, firstNonEmpty(returnedThreadID, "<not-provided>"), turnID)
	acceptedAt := nowText()
	stateName := StateAccepted
	currentAfterStart := m.RuntimeState(threadID)
	origin := strings.ToLower(strings.TrimSpace(request.Origin))
	if origin == "" {
		origin = "bridge"
	}
	state := control.RuntimeState{ThreadID: threadID, State: stateName, TurnID: turnID, Origin: origin, StartedAt: acceptedAt}
	if currentAfterStart.TurnID == turnID && currentAfterStart.State != "" && currentAfterStart.State != StateIdle {
		stateName = currentAfterStart.State
		state = currentAfterStart
	}
	m.setState(state, true)
	m.broker.PublishScoped(events.TurnStarted, threadID, turnID, "", map[string]any{"status": stateName, "source": origin})
	return control.TurnAccepted{ThreadID: threadID, TurnID: turnID, Status: stateName, AcceptedAt: acceptedAt}, nil
}

func turnStartOptions(request control.StartTurnRequest, cwd, configuredSandboxMode string) (appserver.TurnStartOptions, error) {
	options := appserver.TurnStartOptions{
		CWD:               cwd,
		CollaborationMode: request.CollaborationMode,
		ApprovalPolicy:    security.ApprovalOnRequest,
	}
	if request.Model != nil {
		options.Model = strings.TrimSpace(*request.Model)
	}
	if request.ReasoningEffort != nil {
		options.ReasoningEffort = strings.TrimSpace(*request.ReasoningEffort)
	}
	if isRemoteChannelTurn(request.Origin) {
		options.ApprovalPolicy = security.ApprovalNever
		options.SandboxMode = security.SandboxDangerFullAccess
		return options, nil
	}
	sandboxMode, err := security.ParseSandboxMode(configuredSandboxMode)
	if err != nil {
		return appserver.TurnStartOptions{}, err
	}
	options.SandboxMode = sandboxMode
	return options, nil
}

func isRemoteChannelTurn(origin string) bool {
	switch strings.ToLower(strings.TrimSpace(origin)) {
	case control.TurnOriginQQ, control.TurnOriginTelegram:
		return true
	default:
		return false
	}
}

func (m *Manager) InterruptTurn(ctx context.Context, threadID, turnID string) (control.InterruptResult, error) {
	threadID, turnID = strings.TrimSpace(threadID), strings.TrimSpace(turnID)
	lock := m.threadLock(threadID)
	lock.Lock()
	defer lock.Unlock()
	state := m.RuntimeState(threadID)
	if state.TurnID == turnID && (state.State == StateCompleted || state.State == StateFailed) {
		return control.InterruptResult{ThreadID: threadID, TurnID: turnID, Status: state.State}, nil
	}
	if state.TurnID == turnID && state.State == StateInterrupting {
		return control.InterruptResult{ThreadID: threadID, TurnID: turnID, Status: StateInterrupting}, nil
	}
	if !controllableOrigin(state.Origin) || state.TurnID == "" || state.TurnID != turnID || !state.CanInterrupt {
		return control.InterruptResult{}, &ConflictError{
			Code: "turn_not_controllable", Message: "该 Turn 属于外部运行，或后端已经失去控制信息，不能中止。",
			ThreadID: threadID, CurrentState: state.State,
		}
	}
	client, err := m.runningClient()
	if err != nil {
		return control.InterruptResult{}, err
	}
	previous := state
	state.State = StateInterrupting
	m.setState(state, true)
	if err := client.TurnInterrupt(ctx, threadID, turnID); err != nil {
		m.setState(previous, true)
		return control.InterruptResult{}, fmt.Errorf("interrupt Codex turn: %w", err)
	}
	return control.InterruptResult{ThreadID: threadID, TurnID: turnID, Status: StateInterrupting}, nil
}

func (m *Manager) ListInteractions(status string) []interactions.PendingInteraction {
	return m.interactions.List(strings.TrimSpace(status))
}

func (m *Manager) GetInteraction(id string) (interactions.PendingInteraction, bool) {
	return m.interactions.Get(strings.TrimSpace(id))
}

func (m *Manager) RespondInteraction(ctx context.Context, id string, response interactions.ResponseRequest) (interactions.PendingInteraction, error) {
	item, err := m.interactions.BeginResponse(strings.TrimSpace(id))
	if err != nil {
		return interactions.PendingInteraction{}, &ConflictError{Code: "interaction_already_resolved", Message: "该交互请求已处理或不存在。", ThreadID: item.ThreadID, CurrentState: m.RuntimeState(item.ThreadID).State}
	}
	result, finalStatus, err := interactionResult(item, response)
	if err != nil {
		m.interactions.RevertResponse(item.ID)
		return interactions.PendingInteraction{}, err
	}
	client, err := m.runningClient()
	if err != nil {
		m.interactions.RevertResponse(item.ID)
		return interactions.PendingInteraction{}, err
	}
	if err := client.RespondServerRequest(ctx, item.ServerRequestID, result); err != nil {
		m.interactions.RevertResponse(item.ID)
		return interactions.PendingInteraction{}, err
	}
	completed, _ := m.interactions.Complete(item.ID, finalStatus)
	m.afterInteractionChange(completed)
	m.broker.PublishScoped(events.InteractionResolved, completed.ThreadID, completed.TurnID, completed.ItemID, map[string]any{"interaction": completed})
	return completed, nil
}

func interactionResult(item interactions.PendingInteraction, response interactions.ResponseRequest) (map[string]any, string, error) {
	action := strings.ToLower(strings.TrimSpace(response.Action))
	switch item.Kind {
	case interactions.KindCommandApproval, interactions.KindFileChangeApproval:
		if action == "allow" {
			return map[string]any{"decision": "accept"}, "allowed", nil
		}
		if action == "deny" {
			return map[string]any{"decision": "decline"}, "denied", nil
		}
		return nil, "", &ValidationError{Code: "invalid_action", Message: "审批请求只允许 allow 或 deny"}
	case interactions.KindPermissionsApproval:
		if action == "deny" {
			return map[string]any{"permissions": map[string]any{}, "scope": "turn"}, "denied", nil
		}
		if action != "allow" {
			return nil, "", &ValidationError{Code: "invalid_action", Message: "权限审批只允许 allow 或 deny"}
		}
		permissions, ok := item.Raw["permissions"]
		if !ok {
			permissions, ok = item.Raw["requestedPermissions"]
		}
		if !ok {
			return nil, "", &ValidationError{Code: "unsupported_permissions", Message: "无法安全识别请求的权限范围，已拒绝扩大权限"}
		}
		return map[string]any{"permissions": permissions, "scope": "turn"}, "allowed", nil
	case interactions.KindUserInput:
		if action == "cancel" {
			return map[string]any{"answers": map[string]any{}}, "cancelled", nil
		}
		if action != "submit" {
			return nil, "", &ValidationError{Code: "invalid_action", Message: "用户问题只允许 submit 或 cancel"}
		}
		if err := validateAnswers(item.Questions, response.Answers); err != nil {
			return nil, "", err
		}
		answers := make(map[string]any, len(response.Answers))
		for questionID, values := range response.Answers {
			answers[questionID] = map[string]any{"answers": values}
		}
		return map[string]any{"answers": answers}, "submitted", nil
	default:
		if action != "deny" {
			return nil, "", &ValidationError{Code: "unknown_interaction", Message: "未知交互请求只能拒绝，不能批准"}
		}
		return map[string]any{"decision": "decline"}, "denied", nil
	}
}

func validateAnswers(questions []interactions.Question, answers map[string][]string) error {
	for _, question := range questions {
		values := answers[question.ID]
		nonEmpty := make([]string, 0, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				nonEmpty = append(nonEmpty, strings.TrimSpace(value))
			}
		}
		if question.Required && len(nonEmpty) == 0 {
			return &ValidationError{Code: "missing_answer", Message: fmt.Sprintf("问题 %q 尚未回答", question.Text)}
		}
		if question.Type == "single-choice" && len(nonEmpty) > 1 {
			return &ValidationError{Code: "invalid_answer", Message: fmt.Sprintf("问题 %q 只能选择一个答案", question.Text)}
		}
		if (question.Type == "single-choice" || question.Type == "multiple-choice") && len(question.Options) > 0 {
			allowed := map[string]bool{}
			allowOther := false
			for _, option := range question.Options {
				allowed[option.Value] = true
				allowOther = allowOther || option.IsOther
			}
			for _, value := range nonEmpty {
				if !allowed[value] && !allowOther {
					return &ValidationError{Code: "invalid_answer", Message: fmt.Sprintf("问题 %q 包含无效选项", question.Text)}
				}
			}
		}
	}
	return nil
}

func (m *Manager) afterInteractionChange(item interactions.PendingInteraction) {
	if item.ThreadID == "" {
		return
	}
	state := m.RuntimeState(item.ThreadID)
	if state.PendingInteractionCount == 0 && (state.State == StateWaitingApproval || state.State == StateWaitingUserInput) {
		if controllableOrigin(state.Origin) {
			state.State = StateRunningLocal
		} else {
			state.State = StateRunningExternal
		}
	}
	m.setState(state, true)
}

func (m *Manager) expirationLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case now := <-ticker.C:
			for _, item := range m.interactions.ExpireDue(now) {
				m.denyExpired(item)
			}
		}
	}
}

func (m *Manager) denyExpired(item interactions.PendingInteraction) {
	client, err := m.runningClient()
	if err == nil {
		ctx, cancel := context.WithTimeout(m.ctx, 2*time.Second)
		result := map[string]any{"decision": "decline"}
		if item.Kind == interactions.KindPermissionsApproval {
			result = map[string]any{"permissions": map[string]any{}, "scope": "turn"}
		} else if item.Kind == interactions.KindUserInput {
			result = map[string]any{"answers": map[string]any{}}
		}
		err = client.RespondServerRequest(ctx, item.ServerRequestID, result)
		cancel()
	}
	if err != nil {
		m.logger.Printf("expired interaction could not be rejected: kind=%s", item.Kind)
	}
	completed, _ := m.interactions.Complete(item.ID, "expired")
	m.afterInteractionChange(completed)
	m.broker.PublishScoped(events.InteractionResolved, completed.ThreadID, completed.TurnID, completed.ItemID, map[string]any{"interaction": completed})
}

func (m *Manager) expirePendingOnShutdown() {
	for _, item := range m.interactions.List("pending") {
		m.denyExpired(item)
	}
}

func busyError(threadID, state string) error {
	if state == "" || state == StateIdle {
		state = StateRunningExternal
	}
	return &ConflictError{Code: "thread_busy", Message: "该 Codex Thread 当前正在运行，暂时不能开始新的任务。", ThreadID: threadID, CurrentState: state}
}

func threadPayload(raw map[string]any) map[string]any {
	if thread := nestedMap(raw, "thread"); thread != nil {
		return thread
	}
	return raw
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
