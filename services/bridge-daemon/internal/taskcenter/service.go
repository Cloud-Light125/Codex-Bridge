package taskcenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

var (
	ErrTaskNotFound    = errors.New("任务不存在")
	ErrProjectNotFound = errors.New("项目不存在")
)

type Service struct {
	projects *ProjectRegistry
	tasks    *TaskRegistry
	broker   *events.Broker
	logger   *bridgelog.SafeLogger

	mu              sync.RWMutex
	adapters        map[string]TaskBackendAdapter
	projectContexts map[string]string
	started         bool
	cancel          context.CancelFunc
	done            chan struct{}
	unsubscribe     func()
	dispatchMu      sync.Mutex
}

func NewService(projects *ProjectRegistry, tasks *TaskRegistry, broker *events.Broker, logger *bridgelog.SafeLogger, adapters ...TaskBackendAdapter) *Service {
	service := &Service{
		projects: projects, tasks: tasks, broker: broker, logger: logger,
		adapters: make(map[string]TaskBackendAdapter), projectContexts: make(map[string]string), done: make(chan struct{}),
	}
	for _, adapter := range adapters {
		service.SetAdapter(adapter)
	}
	return service
}

func (s *Service) Projects() *ProjectRegistry { return s.projects }
func (s *Service) Tasks() *TaskRegistry       { return s.tasks }

func (s *Service) SetAdapter(adapter TaskBackendAdapter) {
	if adapter == nil {
		return
	}
	s.mu.Lock()
	s.adapters[normalizeBackend(adapter.Backend())] = adapter
	s.mu.Unlock()
}

func (s *Service) Adapter(backend string) (TaskBackendAdapter, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	adapter, ok := s.adapters[normalizeBackend(backend)]
	return adapter, ok
}

func (s *Service) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	if s.broker == nil {
		close(s.done)
		s.mu.Unlock()
		return
	}
	channel, unsubscribe := s.broker.Subscribe()
	s.unsubscribe = unsubscribe
	s.mu.Unlock()
	go s.eventLoop(ctx, channel)
}

func (s *Service) Close() {
	s.mu.Lock()
	cancel := s.cancel
	unsubscribe := s.unsubscribe
	started := s.started
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if unsubscribe != nil {
		unsubscribe()
	}
	if started {
		select {
		case <-s.done:
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Service) eventLoop(ctx context.Context, input <-chan events.Event) {
	defer close(s.done)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-input:
			s.HandleEvent(event)
		}
	}
}

// HandleEvent is exported for deterministic integration tests and is also the
// only place where backend events become task state changes.
func (s *Service) HandleEvent(event events.Event) {
	if event.EventType == events.CodexConnected || event.EventType == events.OpenClawConnected {
		go s.Recover(context.Background())
		return
	}
	task, ok := s.taskForEvent(event)
	if !ok {
		return
	}
	switch event.EventType {
	case events.TurnStarted:
		s.transitionTask(task, StatusRunning, "")
	case events.TurnStatusChanged:
		if status := runtimeStatusFromEvent(event.Payload); strings.Contains(status, "waiting") || strings.Contains(status, "input") {
			s.transitionTask(task, StatusWaitingInput, "")
		} else if status != "" {
			s.transitionTask(task, StatusRunning, "")
		}
	case events.InteractionRequested:
		interactionID := payloadString(event.Payload, "interactionId")
		if interactionID == "" {
			interactionID = nestedPayloadString(event.Payload, "interaction", "id")
		}
		task.PendingInteractionID = interactionID
		task.PendingQuestion = payloadQuestion(event.Payload)
		s.updateTask(task, StatusWaitingInput, "")
	case events.InteractionResolved:
		task.PendingInteractionID = ""
		task.PendingQuestion = ""
		if task.Status == StatusWaitingInput {
			s.updateTask(task, StatusRunning, "")
		} else {
			s.touchTask(task)
		}
	case events.AssistantCompleted:
		if text := firstNonEmpty(payloadString(event.Payload, "text"), payloadString(event.Payload, "message")); text != "" {
			task.Result.FinalText = text
		}
		s.collectSummary(&task, event.Payload)
		s.touchTask(task)
	case events.FileChanged, events.ToolStarted, events.ToolUpdated, events.ToolCompleted, events.AssistantDelta:
		s.collectSummary(&task, event.Payload)
		s.touchTask(task)
	case events.TurnCompleted, events.OpenClawMessageCompleted:
		// Codex emits TurnCompleted only after its existing persistence
		// verification succeeds. OpenClaw emits its terminal message event.
		finalText := firstNonEmpty(payloadString(event.Payload, "finalText"), payloadString(event.Payload, "text"), payloadString(event.Payload, "message"))
		if finalText != "" {
			task.Result.FinalText = finalText
		}
		task.Result.Success = true
		s.collectSummary(&task, event.Payload)
		s.completeTask(task, finalText)
	case events.TurnFailed, events.OpenClawMessageFailed:
		reason := firstNonEmpty(payloadString(event.Payload, "error"), payloadString(event.Payload, "message"), "Backend 返回了执行错误")
		s.collectSummary(&task, event.Payload)
		s.failTask(task, reason)
	case events.TurnInterrupted, events.OpenClawMessageAborted:
		reason := firstNonEmpty(payloadString(event.Payload, "error"), payloadString(event.Payload, "message"), "Backend 运行被中断")
		s.interruptTask(task, reason)
	}
}

func (s *Service) CreateTask(ctx context.Context, input TaskInput) (Task, error) {
	if s.tasks == nil {
		return Task{}, errors.New("任务存储尚未初始化")
	}
	task, err := s.tasks.Create(input)
	if err != nil {
		return Task{}, err
	}
	// The queued record is durable before project/backend resolution starts.
	project, ok := s.resolveProject(input.ProjectID)
	if !ok {
		message := fmt.Sprintf("找不到项目 %q", strings.TrimSpace(input.ProjectID))
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	if !project.Enabled {
		message := fmt.Sprintf("项目 %s 已停用", project.Name)
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	task.ProjectID = project.ProjectID
	task.ProjectNameSnapshot = project.Name
	if task.Backend == "" {
		task.Backend = project.DefaultBackend
	}
	if _, err := s.tasks.Update(task); err != nil {
		return task, err
	}
	return s.routeAndDispatch(ctx, task, project)
}

func (s *Service) routeAndDispatch(ctx context.Context, task Task, project Project) (Task, error) {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	// Recovery can be triggered by both the initial daemon start and a backend
	// connected event. Always re-read the durable record while holding the
	// dispatch mutex so a second recovery pass cannot resend the same task.
	if latest, ok := s.tasks.Get(task.TaskNumber); ok {
		task = latest
	}
	if task.IsTerminal() {
		return task, nil
	}
	if task.Status != StatusQueued && task.Status != StatusRouting {
		return task, nil
	}
	if task.DispatchState != DispatchNotDispatched {
		return task, nil
	}
	s.updateTask(task, StatusRouting, "")
	if latest, ok := s.tasks.Get(task.TaskNumber); ok {
		task = latest
	}
	backend := normalizeBackend(firstNonEmpty(task.Backend, project.DefaultBackend))
	s.logRouting(task, "begin", backend, task.ConversationNumber, "")
	adapter, ok := s.Adapter(backend)
	if !ok {
		message := fmt.Sprintf("项目 %s 的 Backend %s 当前不可用", project.Name, displayBackend(backend))
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	task.Backend = backend
	ref, err := s.allocateConversation(ctx, adapter, project, task)
	if err != nil {
		message := routeErrorMessage(project, backend, err)
		s.logRouting(task, "conversation-failed", backend, task.ConversationNumber, "error="+err.Error())
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	s.logRouting(task, "conversation-resolved", backend, ref.Number, "")
	if ref.Archived {
		message := fmt.Sprintf("项目 %s 选择的 %s Conversation 已归档，请更换默认 Conversation", project.Name, displayBackend(backend))
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	if ref.HasActiveRun {
		message := fmt.Sprintf("%s Conversation #%d 当前已有 Backend 活动运行，请等待完成后再提交", displayBackend(backend), ref.Number)
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	for _, active := range s.tasks.ActiveForTarget(backend, ref.TargetID) {
		if active.TaskNumber == task.TaskNumber {
			continue
		}
		message := fmt.Sprintf("%s Conversation #%d 当前已有活动任务，请等待完成后再提交", displayBackend(backend), ref.Number)
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, errors.New(message)
	}
	task.ConversationNumber = ref.Number
	task.TargetID = ref.TargetID
	if task.ConversationID == "" {
		task.ConversationID = ref.TargetID
	}
	s.updateTask(task, StatusRouting, "")
	task.DispatchState = Dispatching
	s.touchTask(task)
	s.logRouting(task, "dispatching", backend, task.ConversationNumber, "")
	if latest, ok := s.tasks.Get(task.TaskNumber); ok {
		task = latest
	}
	result, err := adapter.SendTask(ctx, ref, task.Description, project.WorkingDirectory, originForTask(task))
	if err != nil {
		message := fmt.Sprintf("发送到 %s 失败：%v", displayBackend(backend), err)
		s.logRouting(task, "send-failed", backend, task.ConversationNumber, "error="+err.Error())
		s.failTask(task, message)
		task = s.latestTask(task)
		return task, err
	}
	task.CurrentRunID = strings.TrimSpace(result.RunID)
	task.DispatchState = DispatchDispatched
	task.StartedAt = firstNonEmpty(result.AcceptedAt, nowText())
	task.Result.Success = false
	s.updateTask(task, StatusRunning, "")
	s.logRouting(task, "dispatched", backend, task.ConversationNumber, task.CurrentRunID)
	if latest, ok := s.tasks.Get(task.TaskNumber); ok {
		task = latest
	}
	return task, nil
}

func (s *Service) allocateConversation(ctx context.Context, adapter TaskBackendAdapter, project Project, task Task) (ConversationRef, error) {
	if task.ConversationNumber > 0 || strings.TrimSpace(task.TargetID) != "" {
		return adapter.ResolveConversation(ctx, task.ConversationNumber, task.TargetID)
	}
	if project.DefaultConversationNumber != nil {
		return adapter.ResolveConversation(ctx, *project.DefaultConversationNumber, "")
	}
	strategy := strings.ToLower(strings.TrimSpace(project.ReuseStrategy))
	if strategy == "" {
		strategy = ReuseDefault
	}
	if strategy == ReuseLatest {
		conversations, err := adapter.ListConversations(ctx, 200)
		if err != nil {
			return ConversationRef{}, err
		}
		workingDir := strings.TrimSpace(project.WorkingDirectory)
		candidates := make([]ConversationRef, 0, len(conversations))
		for _, ref := range conversations {
			if ref.Backend != adapter.Backend() || ref.Archived || ref.HasActiveRun || ref.TargetID == "" {
				continue
			}
			if workingDir != "" && !strings.EqualFold(strings.TrimSpace(ref.WorkingDir), workingDir) {
				continue
			}
			candidates = append(candidates, ref)
		}
		if len(candidates) > 0 {
			sort.Slice(candidates, func(i, j int) bool { return candidates[i].UpdatedAt > candidates[j].UpdatedAt })
			return candidates[0], nil
		}
	}
	if strategy == ReuseCreateNew || project.AutoCreateConversation {
		capabilities := adapter.Capabilities()
		if !capabilities.CanCreateConversation {
			if adapter.Backend() == BackendOpenClaw {
				return ConversationRef{}, errors.New("当前 OpenClaw Gateway 不支持由 Bridge 自动创建 Session，请先设置默认 OpenClaw Session")
			}
			return ConversationRef{}, errors.New("当前 Codex app-server 不支持由 Bridge 自动创建 Thread，请先设置默认 Codex Thread")
		}
		return ConversationRef{}, errors.New("Backend 声明支持创建 Conversation，但当前适配器没有创建实现")
	}
	return ConversationRef{}, errors.New("项目没有可用的默认 Conversation，请先设置默认 Conversation 或选择 latest")
}

func (s *Service) ContinueTask(ctx context.Context, number int, input TaskInput) (Task, error) {
	parent, ok := s.tasks.Get(number)
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	input = inheritTaskInput(parent, input)
	if strings.TrimSpace(input.Description) == "" {
		return Task{}, errors.New("继续任务必须提供新的任务内容")
	}
	input.ParentTaskNumber = parent.TaskNumber
	if strings.TrimSpace(input.Title) == "" {
		input.Title = firstLine(input.Description)
	}
	return s.CreateTask(ctx, input)
}

func (s *Service) RetryTask(ctx context.Context, number int, input TaskInput) (Task, error) {
	parent, ok := s.tasks.Get(number)
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	if parent.Status != StatusFailed && parent.Status != StatusInterrupted {
		return Task{}, fmt.Errorf("只有 failed 或 interrupted 任务可以重试，当前为 %s", parent.Status)
	}
	input = inheritTaskInput(parent, input)
	input.Title = firstNonEmpty(input.Title, parent.Title)
	input.Description = firstNonEmpty(input.Description, parent.Description)
	input.RetryOfTaskNumber = parent.TaskNumber
	// Reuse the original Conversation only when it can still be resolved. A
	// deleted or unavailable target falls back to the Project allocator.
	if adapter, exists := s.Adapter(parent.Backend); exists {
		if _, err := adapter.ResolveConversation(ctx, parent.ConversationNumber, parent.TargetID); err != nil {
			input.ConversationNumber, input.TargetID = 0, ""
		}
	}
	return s.CreateTask(ctx, input)
}

func (s *Service) CancelTask(ctx context.Context, number int) (Task, error) {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	task, ok := s.tasks.Get(number)
	if !ok {
		return Task{}, ErrTaskNotFound
	}
	if task.IsTerminal() {
		return task, nil
	}
	if task.DispatchState == DispatchNotDispatched {
		s.updateTask(task, StatusCancelled, "")
		return s.latestTask(task), nil
	}
	if task.TargetID == "" {
		message := "任务可能已进入 Backend，但没有可用于确认的 Conversation Target"
		s.interruptTask(task, message)
		return s.latestTask(task), errors.New(message)
	}
	adapter, ok := s.Adapter(task.Backend)
	if !ok || !adapter.Capabilities().CanStop {
		task.LastError = fmt.Sprintf("%s 不支持安全停止", displayBackend(task.Backend))
		s.touchTask(task)
		return task, errors.New(task.LastError)
	}
	ref, err := adapter.ResolveConversation(ctx, task.ConversationNumber, task.TargetID)
	if err != nil {
		// A missing Conversation is already a confirmed absence of a live run.
		s.updateTask(task, StatusCancelled, "Conversation 已不存在，已确认没有可继续的运行")
		return s.latestTask(task), nil
	}
	runID := task.CurrentRunID
	if runID == "" {
		if !adapter.Capabilities().CanQueryRunState {
			message := fmt.Sprintf("%s 没有可靠的运行状态查询能力，无法安全取消", displayBackend(task.Backend))
			s.interruptTask(task, message)
			return s.latestTask(task), errors.New(message)
		}
		state, queryErr := adapter.QueryRunState(ctx, ref, "")
		if queryErr != nil {
			message := fmt.Sprintf("无法确认 %s 当前运行状态：%v", displayBackend(task.Backend), queryErr)
			task.LastError = message
			s.touchTask(task)
			return s.latestTask(task), queryErr
		}
		if !state.Exists {
			message := fmt.Sprintf("无法确认 %s Conversation 是否仍存在", displayBackend(task.Backend))
			s.interruptTask(task, message)
			return s.latestTask(task), errors.New(message)
		}
		if !state.Active {
			s.updateTask(task, StatusCancelled, "")
			return s.latestTask(task), nil
		}
		runID = state.RunID
		if strings.TrimSpace(runID) == "" {
			message := fmt.Sprintf("%s 已有活动运行，但 Backend 没有返回可关联的 RunId", displayBackend(task.Backend))
			s.interruptTask(task, message)
			return s.latestTask(task), errors.New(message)
		}
	}
	if _, err := adapter.CancelTask(ctx, ref, runID); err != nil {
		task.LastError = fmt.Sprintf("停止 %s 任务失败：%v", displayBackend(task.Backend), err)
		s.touchTask(task)
		return task, err
	}
	s.updateTask(task, StatusCancelled, "")
	return s.latestTask(task), nil
}

// Recover reconciles only non-terminal durable records. A dispatching record
// is deliberately not resent because the previous process may have reached
// the backend without receiving its response.
func (s *Service) Recover(ctx context.Context) {
	if s.tasks == nil {
		return
	}
	for _, task := range s.tasks.List(TaskFilter{}) {
		if !task.IsActive() {
			continue
		}
		if task.DispatchState == Dispatching {
			s.interruptTask(task, "Bridge 重启时任务仍处于 dispatching，无法安全确认是否已发送")
			continue
		}
		if (task.Status == StatusQueued || task.Status == StatusRouting) && task.DispatchState == DispatchNotDispatched {
			if s.projects == nil {
				s.interruptTask(task, "Project 存储尚未初始化，无法恢复路由")
				continue
			}
			project, ok := s.projects.Get(task.ProjectID)
			if !ok {
				s.interruptTask(task, "任务所属 Project 不存在，无法恢复路由")
				continue
			}
			backend := normalizeBackend(firstNonEmpty(task.Backend, project.DefaultBackend))
			adapter, exists := s.Adapter(backend)
			if !exists {
				if s.logger != nil {
					s.logger.Printf("[task-recovery] %s backend=%s adapter unavailable; queued recovery deferred", task.NumberLabel(), backend)
				}
				continue
			}
			if readiness, known := adapter.(interface{ Ready() bool }); known && !readiness.Ready() {
				if s.logger != nil {
					s.logger.Printf("[task-recovery] %s backend=%s not ready; queued recovery deferred", task.NumberLabel(), backend)
				}
				continue
			}
			_, _ = s.routeAndDispatch(ctx, task, project)
			continue
		}
		adapter, ok := s.Adapter(task.Backend)
		if !ok || !adapter.Capabilities().CanQueryRunState {
			s.interruptTask(task, "Backend 不支持可靠恢复运行状态")
			continue
		}
		if readiness, known := adapter.(interface{ Ready() bool }); known && !readiness.Ready() {
			if s.logger != nil {
				s.logger.Printf("[task-recovery] %s backend=%s not ready; recovery deferred", task.NumberLabel(), task.Backend)
			}
			continue
		}
		ref, err := adapter.ResolveConversation(ctx, task.ConversationNumber, task.TargetID)
		if err != nil {
			s.interruptTask(task, "Conversation 不存在，无法确认原任务是否仍在运行")
			continue
		}
		state, err := adapter.QueryRunState(ctx, ref, task.CurrentRunID)
		if err != nil || !state.Exists {
			s.interruptTask(task, "Bridge 重启后无法确认 Backend 运行状态")
			continue
		}
		if state.Active {
			task.CurrentRunID = firstNonEmpty(state.RunID, task.CurrentRunID)
			if state.WaitingInput && adapter.Capabilities().CanReportWaitingInput {
				s.updateTask(task, StatusWaitingInput, "")
			} else {
				s.updateTask(task, StatusRunning, "")
			}
			continue
		}
		s.interruptTask(task, "Backend 已无活动运行，且没有可靠 Final 信号可用于完成任务")
	}
}

func (s *Service) DeleteProject(id string) (Project, error) {
	if s.projects == nil {
		return Project{}, ErrProjectNotFound
	}
	if s.tasks != nil && s.tasks.HasActiveTasksForProject(id) {
		return Project{}, errors.New("项目仍有活动任务，取消或完成任务后才能删除")
	}
	return s.projects.Delete(id)
}

func (s *Service) resolveProject(reference string) (Project, bool) {
	if s.projects == nil {
		return Project{}, false
	}
	if project, ok := s.projects.Resolve(reference); ok {
		return project, true
	}
	project, ok := s.projects.Get(reference)
	return project, ok && project.Enabled
}

func (s *Service) updateTask(task Task, status, lastError string) Task {
	if s.tasks == nil {
		return task
	}
	previous := task
	if current, ok := s.tasks.Get(task.TaskNumber); ok {
		previous = current
	}
	if status != "" {
		task.Status = status
	}
	if lastError != "" {
		task.LastError = lastError
	} else if status == StatusRunning || status == StatusWaitingInput || status == StatusCompleted {
		task.LastError = ""
	}
	now := nowText()
	task.LastActivityAt = now
	if task.Status == StatusRunning && task.StartedAt == "" {
		task.StartedAt = now
	}
	if task.IsTerminal() && task.CompletedAt == "" {
		task.CompletedAt = now
	}
	if updated, err := s.tasks.Update(task); err == nil {
		task = updated
		if previous.Status != task.Status {
			s.logTransition(previous, task)
			s.publishTaskEvent(events.TaskStateChanged, task, map[string]any{"from": previous.Status, "to": task.Status})
			if task.Status == StatusCompleted {
				s.publishTaskEvent(events.TaskCompleted, task, nil)
			} else if task.Status == StatusFailed {
				s.publishTaskEvent(events.TaskFailed, task, nil)
			} else if task.Status == StatusWaitingInput {
				s.publishTaskEvent(events.TaskWaitingInput, task, nil)
			}
		} else {
			s.publishTaskEvent(events.TaskUpdated, task, nil)
		}
	}
	return task
}

func (s *Service) touchTask(task Task) Task { return s.updateTask(task, task.Status, "") }

func (s *Service) transitionTask(task Task, status, lastError string) {
	if task.Status == status && lastError == "" {
		return
	}
	s.updateTask(task, status, lastError)
}

func (s *Service) completeTask(task Task, message string) {
	if task.IsTerminal() {
		return
	}
	if message != "" {
		task.Result.FinalText = message
	}
	task.Result.Success = true
	task.Summary = summaryFromResult(task)
	s.updateTask(task, StatusCompleted, "")
}

func (s *Service) failTask(task Task, message string) {
	if task.IsTerminal() && task.Status != StatusFailed {
		return
	}
	task.Result.Success = false
	task.Summary = summaryFromResult(task)
	s.updateTask(task, StatusFailed, strings.TrimSpace(message))
}

func (s *Service) interruptTask(task Task, message string) {
	if task.IsTerminal() {
		return
	}
	task.Result.Success = false
	s.updateTask(task, StatusInterrupted, strings.TrimSpace(message))
}

func (s *Service) latestTask(task Task) Task {
	if latest, ok := s.tasks.Get(task.TaskNumber); ok {
		return latest
	}
	return task
}

func (s *Service) taskForEvent(event events.Event) (Task, bool) {
	if s.tasks == nil || strings.TrimSpace(event.ThreadID) == "" {
		return Task{}, false
	}
	backend := backendForEvent(event.EventType)
	runID := strings.TrimSpace(event.TurnID)
	if runID == "" {
		runID = payloadString(event.Payload, "runId")
	}
	var targetMatches []Task
	for _, task := range s.tasks.List(TaskFilter{}) {
		if !task.IsActive() || task.Backend != backend || task.TargetID != event.ThreadID {
			continue
		}
		if runID != "" && task.CurrentRunID == runID {
			return task, true
		}
		targetMatches = append(targetMatches, task)
	}
	// The orchestrator refuses to dispatch two active tasks to one target, so
	// a single exact target match remains safe even when a legacy event lacks a
	// run id. It never selects the last running task globally.
	if runID != "" {
		return Task{}, false
	}
	if len(targetMatches) == 1 {
		return targetMatches[0], true
	}
	return Task{}, false
}

func (s *Service) publishTaskEvent(eventType string, task Task, extra map[string]any) {
	if s.broker == nil {
		return
	}
	payload := map[string]any{"task": task}
	for key, value := range extra {
		payload[key] = value
	}
	s.broker.PublishScoped(eventType, task.TargetID, task.CurrentRunID, "", payload)
}

func (s *Service) logTransition(previous, task Task) {
	if s.logger != nil {
		s.logger.Printf("[task] %s %s -> %s projectId=%s backend=%s conversation=#%d runId=%s", task.NumberLabel(), previous.Status, task.Status, task.ProjectID, task.Backend, task.ConversationNumber, task.CurrentRunID)
	}
}

func (s *Service) logRouting(task Task, stage, backend string, conversationNumber int, detail string) {
	if s.logger == nil {
		return
	}
	s.logger.Printf("[task-routing] stage=%s task=%s projectId=%s backend=%s conversation=#%d runId=%s status=%s %s", stage, task.NumberLabel(), task.ProjectID, backend, conversationNumber, task.CurrentRunID, task.Status, detail)
}

func (s *Service) notificationAddress(task Task) (channels.ChannelAddress, bool) {
	if strings.TrimSpace(task.ChannelType) == "" || strings.TrimSpace(task.ChatID) == "" {
		return channels.ChannelAddress{}, false
	}
	return channels.ChannelAddress{
		ChannelType: task.ChannelType, ChannelProfileID: task.ChannelProfileID, AccountID: task.ChannelAccountID,
		ConversationType: task.ConversationType, ChatID: task.ChatID, TopicID: task.TopicID, UserID: task.UserID,
	}, true
}

// NotificationAddress exposes the durable remote destination without exposing
// provider secrets. Channel services use it to send exactly one completion or
// waiting-input notification through their existing adapter.
func (s *Service) NotificationAddress(task Task) (channels.ChannelAddress, bool) {
	return s.notificationAddress(task)
}

// NotificationText is intentionally compact so QQ and Telegram can both use
// it without duplicating task formatting or exposing the full task payload.
func (s *Service) NotificationText(task Task) string {
	switch task.Status {
	case StatusWaitingInput:
		question := firstNonEmpty(task.PendingQuestion, "Backend 正在等待用户输入")
		return fmt.Sprintf("⏸ %s 正在等待输入\n项目：%s\n问题：%s\n\n使用 /task %d 查看详情", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), truncateText(question, 800), task.TaskNumber)
	case StatusCompleted:
		return fmt.Sprintf("✅ %s 已完成\n项目：%s\nBackend：%s\n会话：%s\n耗时：%s\n%s\n\n使用 /task %d 查看完整详情\n使用 /continue %d <内容> 继续任务", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), displayBackend(task.Backend), conversationLabel(task), durationText(task), completionSummaryLine(task), task.TaskNumber, task.TaskNumber)
	case StatusFailed:
		return fmt.Sprintf("❌ %s 执行失败\n项目：%s\n错误：%s\n\n使用 /task %d 查看详情；/retry %d 重试", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), truncateText(firstNonEmpty(task.LastError, "Backend 返回失败"), 800), task.TaskNumber, task.TaskNumber)
	default:
		return fmt.Sprintf("%s 状态：%s\n项目：%s\n会话：%s", task.NumberLabel(), displayStatus(task.Status), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), conversationLabel(task))
	}
}

func (s *Service) SetProjectContext(address channels.ChannelAddress, projectID string) error {
	project, ok := s.resolveProject(projectID)
	if !ok {
		return ErrProjectNotFound
	}
	s.mu.Lock()
	s.projectContexts[addressKey(address)] = project.ProjectID
	s.mu.Unlock()
	return nil
}

func (s *Service) ProjectContext(address channels.ChannelAddress) (Project, bool) {
	s.mu.RLock()
	id := s.projectContexts[addressKey(address)]
	s.mu.RUnlock()
	if id == "" {
		return Project{}, false
	}
	return s.resolveProject(id)
}

func (s *Service) ExecuteRemoteCommand(ctx context.Context, message channels.InboundMessage, invocation commandregistry.Invocation) (string, bool, error) {
	action := invocation.Definition.Action
	if (action == commandregistry.ActionTaskCancel || action == commandregistry.ActionInteractionCancel) && len(invocation.Arguments) == 0 {
		return "", false, nil
	}
	if !isTaskAction(action) {
		return "", false, nil
	}
	switch action {
	case commandregistry.ActionTasksList:
		status := ""
		if len(invocation.Arguments) > 0 {
			status = normalizeTaskStatus(invocation.Arguments[0])
			if status == "" {
				return "支持的筛选：running、waiting、failed、completed", true, nil
			}
		}
		return formatTaskList(s.tasks.List(TaskFilter{Status: status})), true, nil
	case commandregistry.ActionTaskInfo:
		if len(invocation.Arguments) != 1 {
			return "用法：/task <任务编号>", true, nil
		}
		number, err := parseTaskNumber(invocation.Arguments[0])
		if err != nil {
			return "任务编号无效，例如：/task 102", true, nil
		}
		task, ok := s.tasks.Get(number)
		if !ok {
			return fmt.Sprintf("任务 T%d 不存在。", number), true, nil
		}
		return formatTaskDetail(task), true, nil
	case commandregistry.ActionTaskNew:
		return s.executeRemoteNew(ctx, message, invocation.Arguments)
	case commandregistry.ActionTaskContinue:
		return s.executeRemoteContinue(ctx, message, invocation.Arguments)
	case commandregistry.ActionTaskRetry:
		return s.executeRemoteRetry(ctx, message, invocation.Arguments)
	case commandregistry.ActionTaskCancel, commandregistry.ActionInteractionCancel:
		return s.executeRemoteCancel(ctx, invocation.Arguments)
	case commandregistry.ActionProjectsList:
		return formatProjectList(s.projects.List()), true, nil
	case commandregistry.ActionProjectSelect:
		return s.executeRemoteProject(message, invocation.Arguments)
	default:
		return "", false, nil
	}
}

func (s *Service) executeRemoteNew(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) == 0 {
		return "用法：/new <项目别名> <任务内容>；也可以先用 /project <别名> 设置上下文。", true, nil
	}
	projectReference := ""
	descriptionStart := 0
	if _, ok := s.resolveProject(arguments[0]); ok {
		projectReference, descriptionStart = arguments[0], 1
	} else if project, ok := s.ProjectContext(message.Address); ok {
		projectReference = project.ProjectID
	} else {
		return "请先指定项目，例如：/new xiaomi 修复统计页面；或使用 /project xiaomi 设置上下文。", true, nil
	}
	description := strings.TrimSpace(strings.Join(arguments[descriptionStart:], " "))
	if description == "" {
		return "任务内容不能为空。", true, nil
	}
	task, err := s.CreateTask(ctx, remoteTaskInput(message, projectReference, description))
	if err != nil {
		if task.TaskNumber > 0 {
			return fmt.Sprintf("已创建 %s，但未启动：%s", task.NumberLabel(), err.Error()), true, nil
		}
		return "创建任务失败：" + err.Error(), true, nil
	}
	return formatTaskCreated(task), true, nil
}

func (s *Service) executeRemoteContinue(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) < 2 {
		return "用法：/continue <任务编号> <继续内容>", true, nil
	}
	number, err := parseTaskNumber(arguments[0])
	if err != nil {
		return "任务编号无效。", true, nil
	}
	task, err := s.ContinueTask(ctx, number, remoteTaskInput(message, "", strings.Join(arguments[1:], " ")))
	if err != nil {
		return taskFailureText(task, "继续任务失败：", err), true, nil
	}
	return formatTaskCreated(task), true, nil
}

func (s *Service) executeRemoteRetry(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) != 1 {
		return "用法：/retry <任务编号>", true, nil
	}
	number, err := parseTaskNumber(arguments[0])
	if err != nil {
		return "任务编号无效。", true, nil
	}
	task, err := s.RetryTask(ctx, number, remoteTaskInput(message, "", ""))
	if err != nil {
		return taskFailureText(task, "重试任务失败：", err), true, nil
	}
	return formatTaskCreated(task), true, nil
}

func (s *Service) executeRemoteCancel(ctx context.Context, arguments []string) (string, bool, error) {
	if len(arguments) != 1 {
		return "用法：/cancel <任务编号>", true, nil
	}
	number, err := parseTaskNumber(arguments[0])
	if err != nil {
		return "任务编号无效。", true, nil
	}
	task, err := s.CancelTask(ctx, number)
	if err != nil {
		return taskFailureText(task, "取消任务失败：", err), true, nil
	}
	return fmt.Sprintf("已取消 %s。\n项目：%s\n会话：%s", task.NumberLabel(), task.ProjectNameSnapshot, conversationLabel(task)), true, nil
}

func (s *Service) executeRemoteProject(message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) == 0 {
		if project, ok := s.ProjectContext(message.Address); ok {
			return fmt.Sprintf("当前项目：%s（%s）", project.Name, strings.Join(project.Aliases, ", ")), true, nil
		}
		return "当前未选择项目。使用 /project <别名> 切换。", true, nil
	}
	project, ok := s.resolveProject(arguments[0])
	if !ok {
		return fmt.Sprintf("项目 %q 不存在。使用 /projects 查看。", arguments[0]), true, nil
	}
	s.mu.Lock()
	s.projectContexts[addressKey(message.Address)] = project.ProjectID
	s.mu.Unlock()
	return fmt.Sprintf("已切换项目：%s\n工作目录：%s\nBackend：%s", project.Name, firstNonEmpty(project.WorkingDirectory, "未设置"), displayBackend(project.DefaultBackend)), true, nil
}

func remoteTaskInput(message channels.InboundMessage, projectID, description string) TaskInput {
	address := message.Address
	userID := firstNonEmpty(message.UserID, address.UserID)
	createdFrom := strings.ToLower(strings.TrimSpace(address.ChannelType))
	if createdFrom == "qqbot" {
		createdFrom = "qq"
	}
	return TaskInput{
		Title: firstLine(description), Description: strings.TrimSpace(description), ProjectID: strings.TrimSpace(projectID), CreatedFrom: createdFrom,
		ChannelProfileID: address.ChannelProfileID, ConversationID: address.ChatID, ChannelType: address.ChannelType,
		ChannelAccountID: address.AccountID, ConversationType: address.ConversationType, ChatID: address.ChatID, TopicID: address.TopicID, UserID: userID,
	}
}

func inheritTaskInput(parent Task, input TaskInput) TaskInput {
	if strings.TrimSpace(input.ProjectID) == "" {
		input.ProjectID = parent.ProjectID
	}
	if strings.TrimSpace(input.Backend) == "" {
		input.Backend = parent.Backend
	}
	if input.ConversationNumber == 0 {
		input.ConversationNumber = parent.ConversationNumber
	}
	if strings.TrimSpace(input.TargetID) == "" {
		input.TargetID = parent.TargetID
	}
	if strings.TrimSpace(input.CreatedFrom) == "" {
		input.CreatedFrom = parent.CreatedFrom
	}
	if strings.TrimSpace(input.ChannelProfileID) == "" {
		input.ChannelProfileID = parent.ChannelProfileID
	}
	if strings.TrimSpace(input.ConversationID) == "" {
		input.ConversationID = parent.ConversationID
	}
	if strings.TrimSpace(input.ChannelType) == "" {
		input.ChannelType = parent.ChannelType
	}
	if strings.TrimSpace(input.ChannelAccountID) == "" {
		input.ChannelAccountID = parent.ChannelAccountID
	}
	if strings.TrimSpace(input.ConversationType) == "" {
		input.ConversationType = parent.ConversationType
	}
	if strings.TrimSpace(input.ChatID) == "" {
		input.ChatID = parent.ChatID
	}
	if strings.TrimSpace(input.TopicID) == "" {
		input.TopicID = parent.TopicID
	}
	if strings.TrimSpace(input.UserID) == "" {
		input.UserID = parent.UserID
	}
	return input
}

func (s *Service) collectSummary(task *Task, payload map[string]any) {
	if task == nil || payload == nil {
		return
	}
	for _, path := range payloadPaths(payload) {
		task.Summary.ChangedFiles = uniqueStrings(append(task.Summary.ChangedFiles, path))
		task.Result.ChangedFiles = uniqueStrings(append(task.Result.ChangedFiles, path))
	}
	for _, text := range payloadTexts(payload) {
		for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || len(line) > 300 {
				continue
			}
			lower := strings.ToLower(line)
			switch {
			case strings.Contains(lower, "build") && hasResultWord(lower):
				task.Summary.BuildResults = uniqueStrings(append(task.Summary.BuildResults, line))
				task.Result.BuildResults = uniqueStrings(append(task.Result.BuildResults, line))
			case (strings.Contains(lower, "test") || strings.Contains(lower, "go test") || strings.Contains(lower, "dotnet test")) && hasResultWord(lower):
				task.Summary.TestResults = uniqueStrings(append(task.Summary.TestResults, line))
				task.Result.TestResults = uniqueStrings(append(task.Result.TestResults, line))
			case strings.Contains(lower, "git") && (strings.Contains(lower, "commit") || strings.Contains(lower, "status")):
				task.Summary.GitSummary = line
				task.Result.GitSummary = line
			}
		}
	}
	task.Summary.Duration = durationText(*task)
	task.Result.Duration = task.Summary.Duration
}

func summaryFromResult(task Task) TaskSummary {
	summary := task.Summary
	summary.ChangedFiles = uniqueStrings(append(summary.ChangedFiles, task.Result.ChangedFiles...))
	summary.BuildResults = uniqueStrings(append(summary.BuildResults, task.Result.BuildResults...))
	summary.TestResults = uniqueStrings(append(summary.TestResults, task.Result.TestResults...))
	if summary.GitSummary == "" {
		summary.GitSummary = task.Result.GitSummary
	}
	summary.Duration = durationText(task)
	return summary
}

func payloadPaths(payload map[string]any) []string {
	result := []string{}
	for _, key := range []string{"path", "file", "filename", "filePath"} {
		if value := payloadString(payload, key); looksLikePath(value) {
			result = append(result, value)
		}
	}
	for _, key := range []string{"changes", "files"} {
		values, _ := payload[key].([]any)
		for _, value := range values {
			if item, ok := value.(map[string]any); ok {
				path := firstNonEmpty(payloadString(item, "path"), payloadString(item, "file"), payloadString(item, "filename"))
				if looksLikePath(path) {
					result = append(result, path)
				}
			}
		}
	}
	return uniqueStrings(result)
}

func payloadTexts(payload map[string]any) []string {
	result := []string{}
	for _, key := range []string{"text", "output", "command", "message", "error", "diff", "gitSummary"} {
		if text := payloadString(payload, key); text != "" {
			result = append(result, text)
		}
	}
	return result
}

func payloadQuestion(payload map[string]any) string {
	if question := nestedPayloadString(payload, "interaction", "question"); question != "" {
		return question
	}
	if question := payloadString(payload, "question"); question != "" {
		return question
	}
	value, ok := payload["interaction"]
	if !ok {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	var item map[string]any
	if json.Unmarshal(encoded, &item) != nil {
		return ""
	}
	questions, _ := item["questions"].([]any)
	if len(questions) == 0 {
		return ""
	}
	first, _ := questions[0].(map[string]any)
	return firstNonEmpty(payloadString(first, "text"), payloadString(first, "header"))
}

func payloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	var text string
	if json.Unmarshal(encoded, &text) == nil {
		return strings.TrimSpace(text)
	}
	return ""
}

func nestedPayloadString(payload map[string]any, parent, child string) string {
	value, ok := payload[parent].(map[string]any)
	if !ok {
		encoded, err := json.Marshal(payload[parent])
		if err != nil || json.Unmarshal(encoded, &value) != nil {
			return ""
		}
	}
	return payloadString(value, child)
}

func runtimeStatusFromEvent(payload map[string]any) string {
	status := payloadString(payload, "status")
	if status == "" {
		status = nestedPayloadString(payload, "runtime", "state")
	}
	return strings.ToLower(strings.TrimSpace(status))
}

func taskFromEvent(event events.Event) (Task, bool) {
	if event.Payload == nil {
		return Task{}, false
	}
	value, ok := event.Payload["task"].(Task)
	if ok {
		return value, true
	}
	if pointer, ok := event.Payload["task"].(*Task); ok && pointer != nil {
		return *pointer, true
	}
	encoded, err := json.Marshal(event.Payload["task"])
	if err != nil {
		return Task{}, false
	}
	var task Task
	if json.Unmarshal(encoded, &task) != nil || task.TaskNumber < 1 {
		return Task{}, false
	}
	return task, true
}

// TaskFromEvent is used by channel services and keeps event payload handling
// in one place for both in-process concrete payloads and JSON-like payloads.
func TaskFromEvent(event events.Event) (Task, bool) { return taskFromEvent(event) }

func backendForEvent(eventType string) string {
	if strings.HasPrefix(eventType, "openclaw.") {
		return BackendOpenClaw
	}
	return BackendCodex
}

func originForTask(task Task) string {
	switch strings.ToLower(strings.TrimSpace(task.CreatedFrom)) {
	case "qq", "qqbot":
		return "qqbot"
	case "telegram":
		return "telegram"
	default:
		return "bridge"
	}
}

func routeErrorMessage(project Project, backend string, err error) string {
	message := strings.TrimSpace(err.Error())
	if message == "" {
		message = "未知路由错误"
	}
	if strings.Contains(message, "没有可用") || strings.Contains(message, "不支持") || strings.Contains(message, "Conversation") {
		return fmt.Sprintf("项目 %s 没有可用的 %s Conversation：%s", project.Name, displayBackend(backend), message)
	}
	return fmt.Sprintf("项目 %s 路由失败：%s", project.Name, message)
}

func isTaskAction(action string) bool {
	return action == commandregistry.ActionTasksList || action == commandregistry.ActionTaskInfo || action == commandregistry.ActionTaskNew ||
		action == commandregistry.ActionTaskContinue || action == commandregistry.ActionTaskRetry || action == commandregistry.ActionTaskCancel || action == commandregistry.ActionInteractionCancel ||
		action == commandregistry.ActionProjectsList || action == commandregistry.ActionProjectSelect
}

func normalizeTaskStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "running", "run", "执行中":
		return StatusRunning
	case "waiting", "waiting-input", "input", "等待输入":
		return StatusWaitingInput
	case "failed", "failure", "失败":
		return StatusFailed
	case "completed", "complete", "done", "已完成":
		return StatusCompleted
	default:
		return ""
	}
}

func parseTaskNumber(value string) (int, error) {
	value = strings.TrimSpace(strings.Trim(value, "#Tt[]"))
	if value == "" {
		return 0, errors.New("empty task number")
	}
	number, err := strconv.Atoi(value)
	if err != nil || number < 1 {
		return 0, errors.New("invalid task number")
	}
	return number, nil
}

func formatTaskList(tasks []Task) string {
	if len(tasks) == 0 {
		return "暂无任务。"
	}
	if len(tasks) > 10 {
		tasks = tasks[:10]
	}
	var output strings.Builder
	output.WriteString("最近任务：")
	for _, task := range tasks {
		fmt.Fprintf(&output, "\n\n%s [%s]\n%s\n%s · %s · %s", task.NumberLabel(), displayStatus(task.Status), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), truncateText(task.Title, 80), displayBackend(task.Backend), conversationLabel(task))
		fmt.Fprintf(&output, "\n%s", durationText(task))
	}
	return truncateText(strings.TrimSpace(output.String()), 3800)
}

func formatTaskCreated(task Task) string {
	return fmt.Sprintf("已创建 %s\n项目：%s\nBackend：%s\n会话：%s\n状态：%s", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), displayBackend(task.Backend), conversationLabel(task), displayStatus(task.Status))
}

func formatTaskDetail(task Task) string {
	var output strings.Builder
	fmt.Fprintf(&output, "%s\n状态：%s\n项目：%s\nBackend：%s\n会话：%s\nTarget：%s\n创建：%s\n开始：%s\n完成：%s\n运行：%s\n最近活动：%s\n\n任务：\n%s", task.NumberLabel(), displayStatus(task.Status), firstNonEmpty(task.ProjectNameSnapshot, "项目已删除或未设置"), displayBackend(task.Backend), conversationLabel(task), firstNonEmpty(task.TargetID, "未分配"), displayTime(task.CreatedAt), displayTime(task.StartedAt), displayTime(task.CompletedAt), durationText(task), displayTime(task.LastActivityAt), task.Description)
	if task.Result.FinalText != "" {
		output.WriteString("\n\n最终结果：\n")
		output.WriteString(task.Result.FinalText)
	}
	if len(task.Summary.ChangedFiles)+len(task.Summary.BuildResults)+len(task.Summary.TestResults) > 0 {
		output.WriteString("\n\n摘要：")
		for _, path := range task.Summary.ChangedFiles {
			output.WriteString("\n修改：" + path)
		}
		for _, result := range task.Summary.BuildResults {
			output.WriteString("\n构建：" + result)
		}
		for _, result := range task.Summary.TestResults {
			output.WriteString("\n测试：" + result)
		}
	}
	if task.LastError != "" {
		output.WriteString("\n\n错误：" + task.LastError)
	}
	return truncateText(output.String(), 3800)
}

func formatProjectList(projects []Project) string {
	if len(projects) == 0 {
		return "暂无项目。请在桌面端创建 Project。"
	}
	var output strings.Builder
	output.WriteString("项目列表：")
	for _, project := range projects {
		if !project.Enabled {
			continue
		}
		fmt.Fprintf(&output, "\n\n%s", project.Name)
		if len(project.Aliases) > 0 {
			fmt.Fprintf(&output, "（%s）", strings.Join(project.Aliases, ", "))
		}
		fmt.Fprintf(&output, "\n%s · %s", displayBackend(project.DefaultBackend), firstNonEmpty(project.WorkingDirectory, "未设置工作目录"))
	}
	return truncateText(strings.TrimSpace(output.String()), 3800)
}

func taskFailureText(task Task, prefix string, err error) string {
	if task.TaskNumber > 0 {
		return fmt.Sprintf("%s%s：%s", prefix, task.NumberLabel(), err.Error())
	}
	return prefix + err.Error()
}

func conversationLabel(task Task) string {
	if task.ConversationNumber > 0 {
		return fmt.Sprintf("[%s] #%d", displayBackend(task.Backend), task.ConversationNumber)
	}
	if task.TargetID != "" {
		return fmt.Sprintf("[%s] %s", displayBackend(task.Backend), truncateText(task.TargetID, 48))
	}
	return "未分配"
}

func displayStatus(status string) string {
	switch status {
	case StatusQueued:
		return "排队中"
	case StatusRouting:
		return "路由中"
	case StatusRunning:
		return "运行中"
	case StatusWaitingInput:
		return "等待输入"
	case StatusCompleted:
		return "已完成"
	case StatusFailed:
		return "失败"
	case StatusCancelled:
		return "已取消"
	case StatusInterrupted:
		return "已中断"
	default:
		return status
	}
}

func displayTime(value string) string {
	if strings.TrimSpace(value) == "" {
		return "—"
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.Local().Format("01-02 15:04")
	}
	return value
}

func durationText(task Task) string {
	duration := task.Duration()
	if duration <= 0 {
		return "—"
	}
	seconds := int(duration.Round(time.Second) / time.Second)
	hours, seconds := seconds/3600, seconds%3600
	minutes, seconds := seconds/60, seconds%60
	if hours > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

func truncateText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len([]rune(value)) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit-1]) + "…"
}

func addressKey(address channels.ChannelAddress) string {
	return strings.Join([]string{address.ChannelType, address.ChannelProfileID, address.AccountID, address.ConversationType, address.ChatID, address.TopicID, address.UserID}, "\x00")
}

func looksLikePath(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && (strings.Contains(value, "\\") || strings.Contains(value, "/") || strings.HasSuffix(strings.ToLower(value), ".go") || strings.HasSuffix(strings.ToLower(value), ".cs"))
}

func hasResultWord(value string) bool {
	return strings.Contains(value, "pass") || strings.Contains(value, "fail") || strings.Contains(value, "success") || strings.Contains(value, "succeed") || strings.Contains(value, "error") || strings.Contains(value, "warning") || strings.Contains(value, "通过") || strings.Contains(value, "失败")
}

func completionSummaryLine(task Task) string {
	parts := []string{}
	if len(task.Summary.ChangedFiles) > 0 {
		parts = append(parts, fmt.Sprintf("修改：%d 个文件", len(task.Summary.ChangedFiles)))
	}
	if len(task.Summary.BuildResults) > 0 {
		parts = append(parts, "构建："+truncateText(task.Summary.BuildResults[len(task.Summary.BuildResults)-1], 180))
	}
	if len(task.Summary.TestResults) > 0 {
		parts = append(parts, "测试："+truncateText(task.Summary.TestResults[len(task.Summary.TestResults)-1], 180))
	}
	if len(parts) == 0 {
		return "使用 /task 查看完整结果"
	}
	return strings.Join(parts, "\n")
}
