package taskcenter

import (
	"strings"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
)

type TaskActionExecutionType string

const (
	TaskActionQuery     TaskActionExecutionType = "query"
	TaskActionExecution TaskActionExecutionType = "execution"
)

const (
	TaskActionContinue   = "continue"
	TaskActionRetry      = "retry"
	TaskActionCancel     = "cancel"
	TaskActionTest       = "test"
	TaskActionDiff       = "diff"
	TaskActionGitStatus  = "git-status"
	TaskActionCommit     = "commit"
	TaskActionCommitPush = "commit-push"
	TaskActionOpen       = "open"
)

type TaskActionDefinition struct {
	Id                       string                  `json:"id"`
	DisplayName              string                  `json:"displayName"`
	Description              string                  `json:"description"`
	SupportedBackends        []string                `json:"supportedBackends"`
	RequiredTaskStates       []string                `json:"requiredTaskStates"`
	RequiresWorkingDirectory bool                    `json:"requiresWorkingDirectory"`
	RequiresGitRepository    bool                    `json:"requiresGitRepository"`
	RequiresConfirmation     bool                    `json:"requiresConfirmation"`
	CreatesNewTask           bool                    `json:"createsNewTask"`
	ExecutionType            TaskActionExecutionType `json:"executionType"`
	Enabled                  bool                    `json:"enabled"`
}

type TaskActionAvailability struct {
	Id                       string                  `json:"id"`
	DisplayName              string                  `json:"displayName"`
	Description              string                  `json:"description"`
	SupportedBackends        []string                `json:"supportedBackends"`
	RequiredTaskStates       []string                `json:"requiredTaskStates"`
	RequiresWorkingDirectory bool                    `json:"requiresWorkingDirectory"`
	RequiresGitRepository    bool                    `json:"requiresGitRepository"`
	RequiresConfirmation     bool                    `json:"requiresConfirmation"`
	CreatesNewTask           bool                    `json:"createsNewTask"`
	ExecutionType            TaskActionExecutionType `json:"executionType"`
	Enabled                  bool                    `json:"enabled"`
	Available                bool                    `json:"available"`
	Reason                   string                  `json:"reason,omitempty"`
}

type TaskActionAvailabilityContext struct {
	Task                  Task
	Project               Project
	ProjectAvailable      bool
	Capabilities          AdapterCapabilities
	WorkingDirectoryValid bool
	GitRepository         bool
	HasConversation       bool
}

type TaskActionRequest struct {
	ActionID  string `json:"actionId"`
	Text      string `json:"text,omitempty"`
	Full      bool   `json:"full,omitempty"`
	Confirmed bool   `json:"confirmed,omitempty"`
	Remote    bool   `json:"-"`
}

type TaskActionResult struct {
	Action    string `json:"action"`
	Time      string `json:"time"`
	Result    string `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type TaskActionResponse struct {
	Task                 *Task             `json:"task,omitempty"`
	Result               *TaskActionResult `json:"result,omitempty"`
	ConfirmationRequired bool              `json:"confirmationRequired,omitempty"`
	ConfirmationMessage  string            `json:"confirmationMessage,omitempty"`
}

// PendingTaskAction is intentionally in-memory. It is a short-lived remote
// confirmation and is not part of the task backup format.
type PendingTaskAction struct {
	ActionRequestID  string `json:"actionRequestId"`
	TaskNumber       int    `json:"taskNumber"`
	ActionID         string `json:"actionId"`
	Requester        string `json:"requester"`
	ChannelProfileID string `json:"channelProfileId"`
	CreatedAt        string `json:"createdAt"`
	ExpiresAt        string `json:"expiresAt"`

	address channels.ChannelAddress
	userID  string
}

type TaskActionRegistry struct {
	definitions []TaskActionDefinition
}

func NewTaskActionRegistry() *TaskActionRegistry {
	return &TaskActionRegistry{definitions: []TaskActionDefinition{
		{Id: TaskActionContinue, DisplayName: "继续任务", Description: "在原 Project 与 Conversation 上创建后续 Task", SupportedBackends: []string{BackendCodex, BackendOpenClaw}, RequiredTaskStates: []string{StatusCompleted, StatusFailed, StatusInterrupted}, CreatesNewTask: true, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionRetry, DisplayName: "重试", Description: "重新执行 failed 或 interrupted Task", SupportedBackends: []string{BackendCodex, BackendOpenClaw}, RequiredTaskStates: []string{StatusFailed, StatusInterrupted}, CreatesNewTask: true, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionCancel, DisplayName: "停止任务", Description: "调用现有 Backend Cancel API 停止活动 Task", SupportedBackends: []string{BackendCodex, BackendOpenClaw}, RequiredTaskStates: []string{StatusQueued, StatusRouting, StatusRunning, StatusWaitingInput}, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionTest, DisplayName: "重新测试", Description: "运行当前项目已有测试和构建验证", SupportedBackends: []string{BackendCodex}, RequiredTaskStates: []string{StatusCompleted}, RequiresWorkingDirectory: true, CreatesNewTask: true, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionDiff, DisplayName: "查看 Diff", Description: "读取当前 Project 的 Git diff", SupportedBackends: []string{BackendCodex}, RequiredTaskStates: []string{StatusCompleted, StatusFailed, StatusInterrupted}, RequiresWorkingDirectory: true, RequiresGitRepository: true, ExecutionType: TaskActionQuery, Enabled: true},
		{Id: TaskActionGitStatus, DisplayName: "Git 状态", Description: "读取 Git 状态、当前分支和最新提交", SupportedBackends: []string{BackendCodex}, RequiredTaskStates: []string{StatusCompleted, StatusFailed, StatusInterrupted}, RequiresWorkingDirectory: true, RequiresGitRepository: true, ExecutionType: TaskActionQuery, Enabled: true},
		{Id: TaskActionCommit, DisplayName: "提交", Description: "创建 Task 让 Codex 检查并提交相关正式修改", SupportedBackends: []string{BackendCodex}, RequiredTaskStates: []string{StatusCompleted}, RequiresWorkingDirectory: true, RequiresGitRepository: true, CreatesNewTask: true, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionCommitPush, DisplayName: "提交并推送", Description: "确认后创建 Task 让 Codex 提交并普通 push", SupportedBackends: []string{BackendCodex}, RequiredTaskStates: []string{StatusCompleted}, RequiresWorkingDirectory: true, RequiresGitRepository: true, RequiresConfirmation: true, CreatesNewTask: true, ExecutionType: TaskActionExecution, Enabled: true},
		{Id: TaskActionOpen, DisplayName: "打开会话", Description: "查看当前 Task 关联的 Backend Conversation", SupportedBackends: []string{BackendCodex, BackendOpenClaw}, RequiresConfirmation: false, ExecutionType: TaskActionQuery, Enabled: true},
	}}
}

func (r *TaskActionRegistry) Definitions() []TaskActionDefinition {
	if r == nil {
		return nil
	}
	result := make([]TaskActionDefinition, 0, len(r.definitions))
	for _, definition := range r.definitions {
		definition.SupportedBackends = append([]string(nil), definition.SupportedBackends...)
		definition.RequiredTaskStates = append([]string(nil), definition.RequiredTaskStates...)
		result = append(result, definition)
	}
	return result
}

func (r *TaskActionRegistry) Find(id string) (TaskActionDefinition, bool) {
	if r == nil {
		return TaskActionDefinition{}, false
	}
	id = strings.ToLower(strings.TrimSpace(id))
	for _, definition := range r.definitions {
		if definition.Id == id {
			return definition, true
		}
	}
	return TaskActionDefinition{}, false
}

func (r *TaskActionRegistry) Evaluate(input TaskActionAvailabilityContext) []TaskActionAvailability {
	if r == nil {
		return nil
	}
	result := make([]TaskActionAvailability, 0, len(r.definitions))
	for _, definition := range r.definitions {
		availability := TaskActionAvailability{
			Id: definition.Id, DisplayName: definition.DisplayName, Description: definition.Description,
			SupportedBackends: append([]string(nil), definition.SupportedBackends...), RequiredTaskStates: append([]string(nil), definition.RequiredTaskStates...),
			RequiresWorkingDirectory: definition.RequiresWorkingDirectory, RequiresGitRepository: definition.RequiresGitRepository,
			RequiresConfirmation: definition.RequiresConfirmation, CreatesNewTask: definition.CreatesNewTask,
			ExecutionType: definition.ExecutionType, Enabled: definition.Enabled, Available: definition.Enabled,
		}
		if !definition.Enabled {
			availability.Available, availability.Reason = false, "操作已禁用"
		} else if !supportsBackend(definition, input.Task.Backend) {
			availability.Available, availability.Reason = false, "当前 Backend 不支持"
		} else if !supportsState(definition, input.Task.Status) {
			availability.Available, availability.Reason = false, "当前 Task 状态不支持"
		} else if !supportsCapability(definition.Id, input) {
			availability.Available, availability.Reason = false, "当前 Backend Capability 不支持"
		} else if definition.CreatesNewTask && (!input.ProjectAvailable || !input.Project.Enabled) {
			availability.Available, availability.Reason = false, "Project 不存在或已停用"
		} else if definition.RequiresWorkingDirectory && !input.WorkingDirectoryValid {
			availability.Available, availability.Reason = false, "Project WorkingDirectory 无效"
		} else if definition.RequiresGitRepository && !input.GitRepository {
			availability.Available, availability.Reason = false, "Project WorkingDirectory 不是 Git 仓库"
		} else if definition.Id == TaskActionOpen && !input.HasConversation {
			availability.Available, availability.Reason = false, "Task 没有关联 Conversation"
		}
		result = append(result, availability)
	}
	return result
}

func (r *TaskActionRegistry) Available(input TaskActionAvailabilityContext) []TaskActionAvailability {
	all := r.Evaluate(input)
	result := make([]TaskActionAvailability, 0, len(all))
	for _, action := range all {
		if action.Available {
			result = append(result, action)
		}
	}
	return result
}

func supportsBackend(definition TaskActionDefinition, backend string) bool {
	backend = strings.ToLower(strings.TrimSpace(backend))
	for _, supported := range definition.SupportedBackends {
		if strings.EqualFold(strings.TrimSpace(supported), backend) {
			return true
		}
	}
	return false
}

func supportsState(definition TaskActionDefinition, state string) bool {
	if len(definition.RequiredTaskStates) == 0 {
		return true
	}
	for _, required := range definition.RequiredTaskStates {
		if required == state {
			return true
		}
	}
	return false
}

func supportsCapability(actionID string, input TaskActionAvailabilityContext) bool {
	capabilities := input.Capabilities
	switch actionID {
	case TaskActionContinue:
		return capabilities.SupportsContinueAction()
	case TaskActionRetry:
		return capabilities.SupportsRetryAction()
	case TaskActionCancel:
		return capabilities.SupportsCancelAction()
	case TaskActionTest:
		return capabilities.SupportsTestAction
	case TaskActionDiff, TaskActionGitStatus, TaskActionCommit, TaskActionCommitPush:
		return capabilities.SupportsGitActions
	case TaskActionOpen:
		return capabilities.SupportsOpenConversationAction()
	default:
		return false
	}
}
