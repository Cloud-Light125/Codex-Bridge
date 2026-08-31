package taskcenter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/gitquery"
)

const (
	remoteDiffLimit        = 5000
	remoteActionTTL        = 5 * time.Minute
	testActionPrompt       = "重新运行当前项目已有的测试和构建验证。不要修改代码，除非测试无法运行且需要最小修复；若发现失败，先报告失败原因。完成后总结测试命令和结果。"
	commitActionPrompt     = "检查当前工作区改动，只提交与当前任务链相关的正式文件。不要提交未跟踪临时文件、构建产物、bak、secret、token。禁止 force push、禁止 reset --hard、禁止 clean -fd、禁止 tag -f、禁止 rebase、禁止修改历史 tag。先运行必要检查，然后创建一次 Git commit。不要 push。最后报告 commit SHA 和提交文件。"
	commitPushActionPrompt = "检查当前工作区，仅提交与当前任务相关的正式修改，运行必要测试，创建 commit，并普通 push 当前分支到现有 origin。禁止 force push、禁止 reset --hard、禁止 clean -fd、禁止 tag -f、禁止 rebase、禁止修改历史 tag、禁止提交 secret/临时文件。完成后报告 commit SHA 和 push 结果。"
)

type ProjectGitStatus struct {
	ProjectID        string `json:"projectId"`
	WorkingDirectory string `json:"workingDirectory"`
	GitRepository    bool   `json:"gitRepository"`
	Branch           string `json:"branch,omitempty"`
	WorkingTree      string `json:"workingTree,omitempty"`
	LatestCommit     string `json:"latestCommit,omitempty"`
	Error            string `json:"error,omitempty"`
}

func (s *Service) GetProjectGitStatus(ctx context.Context, projectID string) (ProjectGitStatus, error) {
	if s.projects == nil {
		return ProjectGitStatus{}, ErrProjectNotFound
	}
	project, ok := s.projects.Get(strings.TrimSpace(projectID))
	if !ok {
		return ProjectGitStatus{}, ErrProjectNotFound
	}
	result := ProjectGitStatus{ProjectID: project.ProjectID, WorkingDirectory: project.WorkingDirectory}
	s.mu.RLock()
	gitService := s.git
	s.mu.RUnlock()
	if gitService == nil {
		result.Error = "Git 查询服务尚未初始化"
		return result, nil
	}
	if !gitService.IsWorkingDirectoryValid(project.WorkingDirectory) {
		result.Error = gitquery.ErrInvalidWorkingDirectory.Error()
		return result, nil
	}
	repository, err := gitService.IsRepository(ctx, project.WorkingDirectory)
	if err != nil {
		result.Error = err.Error()
		return result, nil
	}
	if !repository {
		result.Error = gitquery.ErrNotGitRepository.Error()
		return result, nil
	}
	result.GitRepository = true
	status, statusErr := gitService.GetStatus(ctx, project.WorkingDirectory)
	branch, branchErr := gitService.GetBranch(ctx, project.WorkingDirectory)
	latest, latestErr := gitService.GetLatestCommit(ctx, project.WorkingDirectory)
	if statusErr != nil || branchErr != nil || latestErr != nil {
		result.Error = firstNonEmpty(errorText(statusErr), errorText(branchErr), errorText(latestErr))
		return result, nil
	}
	result.Branch = firstNonEmpty(branch, "未命名或 detached")
	result.WorkingTree = "clean"
	if strings.TrimSpace(status) != "" {
		result.WorkingTree = "modified"
	}
	result.LatestCommit = firstNonEmpty(latest, "暂无提交")
	return result, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *Service) AvailableActions(ctx context.Context, number int) (Task, []TaskActionAvailability, error) {
	if s.tasks == nil {
		return Task{}, nil, ErrTaskNotFound
	}
	task, ok := s.tasks.Get(number)
	if !ok {
		return Task{}, nil, ErrTaskNotFound
	}
	return task, s.availableActionsForTask(ctx, task), nil
}

func (s *Service) availableActionsForTask(ctx context.Context, task Task) []TaskActionAvailability {
	if ctx == nil {
		ctx = context.Background()
	}
	project := Project{ProjectID: task.ProjectID, Name: task.ProjectNameSnapshot, WorkingDirectory: ""}
	projectAvailable := false
	if s.projects != nil {
		if resolved, ok := s.projects.Get(task.ProjectID); ok {
			project = resolved
			projectAvailable = true
		}
	}
	capabilities := AdapterCapabilities{}
	if adapter, ok := s.Adapter(task.Backend); ok {
		capabilities = adapter.Capabilities()
	}
	workingDirectoryValid, repository := false, false
	s.mu.RLock()
	gitService := s.git
	registry := s.actions
	s.mu.RUnlock()
	if gitService != nil && (capabilities.SupportsGitActions || capabilities.SupportsTestAction) {
		workingDirectoryValid = gitService.IsWorkingDirectoryValid(project.WorkingDirectory)
		if workingDirectoryValid && capabilities.SupportsGitActions {
			repository, _ = gitService.IsRepository(ctx, project.WorkingDirectory)
		}
	}
	if registry == nil {
		registry = NewTaskActionRegistry()
	}
	return registry.Available(TaskActionAvailabilityContext{
		Task: task, Project: project, ProjectAvailable: projectAvailable, Capabilities: capabilities,
		WorkingDirectoryValid: workingDirectoryValid, GitRepository: repository,
		HasConversation: task.ConversationNumber > 0 || strings.TrimSpace(task.TargetID) != "",
	})
}

func (s *Service) ExecuteAction(ctx context.Context, number int, request TaskActionRequest) (TaskActionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.tasks == nil {
		return TaskActionResponse{}, ErrTaskNotFound
	}
	task, ok := s.tasks.Get(number)
	if !ok {
		return TaskActionResponse{}, ErrTaskNotFound
	}
	actionID := strings.ToLower(strings.TrimSpace(request.ActionID))
	registry := s.ActionRegistry()
	definition, ok := registry.Find(actionID)
	if !ok {
		return TaskActionResponse{}, fmt.Errorf("未知 Task Action：%s", request.ActionID)
	}
	request.ActionID = definition.Id
	available := false
	for _, item := range s.availableActionsForTask(ctx, task) {
		if item.Id == definition.Id {
			available = item.Available
			break
		}
	}
	if !available {
		return TaskActionResponse{}, fmt.Errorf("%s 当前不可用操作：%s", task.NumberLabel(), definition.DisplayName)
	}
	if definition.RequiresConfirmation && !request.Confirmed {
		return TaskActionResponse{ConfirmationRequired: true, ConfirmationMessage: fmt.Sprintf("操作“%s”需要确认后才能执行", definition.DisplayName)}, nil
	}

	switch definition.Id {
	case TaskActionContinue:
		if strings.TrimSpace(request.Text) == "" {
			return TaskActionResponse{}, errors.New("继续任务必须提供新的任务内容")
		}
		requestTask := TaskInput{Description: strings.TrimSpace(request.Text), ActionID: definition.Id, ActionSourceTaskNumber: task.TaskNumber}
		created, err := s.ContinueTask(ctx, task.TaskNumber, requestTask)
		return taskActionTaskResponse(created, err)
	case TaskActionRetry:
		created, err := s.RetryTask(ctx, task.TaskNumber, TaskInput{ActionID: definition.Id, ActionSourceTaskNumber: task.TaskNumber})
		return taskActionTaskResponse(created, err)
	case TaskActionTest:
		return s.createActionTask(ctx, task, definition.Id, "重新测试", testActionPrompt)
	case TaskActionCommit:
		return s.createActionTask(ctx, task, definition.Id, "提交相关修改", commitActionPrompt)
	case TaskActionCommitPush:
		return s.createActionTask(ctx, task, definition.Id, "提交并推送相关修改", commitPushActionPrompt)
	case TaskActionCancel:
		cancelled, err := s.CancelTask(ctx, task.TaskNumber)
		return taskActionTaskResponse(cancelled, err)
	case TaskActionDiff:
		return s.executeDiffAction(ctx, task, request.Full, request.Remote)
	case TaskActionGitStatus:
		return s.executeGitStatusAction(ctx, task, request.Remote)
	case TaskActionOpen:
		return TaskActionResponse{Result: &TaskActionResult{Action: definition.Id, Time: nowText(), Result: formatOpenAction(task)}}, nil
	default:
		return TaskActionResponse{}, fmt.Errorf("Task Action 尚未实现：%s", definition.Id)
	}
}

func (s *Service) createActionTask(ctx context.Context, parent Task, actionID, title, prompt string) (TaskActionResponse, error) {
	input := inheritTaskInput(parent, TaskInput{
		Title: title, Description: prompt, ParentTaskNumber: parent.TaskNumber, ActionID: actionID, ActionSourceTaskNumber: parent.TaskNumber,
	})
	created, err := s.CreateTask(ctx, input)
	return taskActionTaskResponse(created, err)
}

func taskActionTaskResponse(task Task, err error) (TaskActionResponse, error) {
	if err != nil {
		if task.TaskNumber > 0 {
			return TaskActionResponse{Task: &task}, err
		}
		return TaskActionResponse{}, err
	}
	return TaskActionResponse{Task: &task}, nil
}

func (s *Service) executeDiffAction(ctx context.Context, task Task, full, remote bool) (TaskActionResponse, error) {
	if s.projects == nil {
		return TaskActionResponse{}, ErrProjectNotFound
	}
	project, ok := s.projects.Get(task.ProjectID)
	if !ok {
		return TaskActionResponse{}, ErrProjectNotFound
	}
	s.mu.RLock()
	gitService := s.git
	s.mu.RUnlock()
	if gitService == nil {
		return TaskActionResponse{}, errors.New("Git 查询服务尚未初始化")
	}
	snapshot, err := gitService.Inspect(ctx, project.WorkingDirectory, full)
	if err != nil {
		return TaskActionResponse{Result: &TaskActionResult{Action: TaskActionDiff, Time: nowText(), Error: err.Error()}}, nil
	}
	text := formatDiffResult(task, snapshot, full, remote)
	text, remoteTruncated := limitRemoteActionResult(text, remote)
	return TaskActionResponse{Result: &TaskActionResult{Action: TaskActionDiff, Time: nowText(), Result: text, Truncated: snapshot.Truncated || remoteTruncated || (remote && full && len([]rune(snapshot.Diff)) > remoteDiffLimit)}}, nil
}

func (s *Service) executeGitStatusAction(ctx context.Context, task Task, remote bool) (TaskActionResponse, error) {
	if s.projects == nil {
		return TaskActionResponse{}, ErrProjectNotFound
	}
	project, ok := s.projects.Get(task.ProjectID)
	if !ok {
		return TaskActionResponse{}, ErrProjectNotFound
	}
	s.mu.RLock()
	gitService := s.git
	s.mu.RUnlock()
	if gitService == nil {
		return TaskActionResponse{}, errors.New("Git 查询服务尚未初始化")
	}
	snapshot, err := gitService.Inspect(ctx, project.WorkingDirectory, false)
	if err != nil {
		return TaskActionResponse{Result: &TaskActionResult{Action: TaskActionGitStatus, Time: nowText(), Error: err.Error()}}, nil
	}
	text := formatGitStatusResult(task, snapshot, remote)
	text, remoteTruncated := limitRemoteActionResult(text, remote)
	return TaskActionResponse{Result: &TaskActionResult{Action: TaskActionGitStatus, Time: nowText(), Result: text, Truncated: snapshot.Truncated || remoteTruncated}}, nil
}

func formatDiffResult(task Task, snapshot gitquery.Snapshot, full, remote bool) string {
	var output strings.Builder
	fmt.Fprintf(&output, "项目：%s\n工作目录：%s\n\n修改文件：\n%s\n\nDiff Stat：\n%s", firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), snapshot.WorkingDirectory, firstNonEmpty(snapshot.ChangedFiles, "工作区干净"), firstNonEmpty(snapshot.DiffStat, "无未提交 diff"))
	if full {
		diff := snapshot.Diff
		truncated := false
		limit := 200 * 1024
		if remote {
			limit = remoteDiffLimit
		}
		if len([]rune(diff)) > limit {
			diff = truncateText(diff, limit)
			truncated = true
		}
		output.WriteString("\n\n完整 Diff：\n")
		output.WriteString(firstNonEmpty(diff, "无未提交 diff"))
		if truncated || snapshot.Truncated {
			if remote {
				output.WriteString("\n\nDiff 过长，已截断；完整 Diff 请在桌面查看。")
			} else {
				output.WriteString("\n\nDiff 过长，已截断；请在桌面查看完整工作区。")
			}
		}
	}
	return output.String()
}

func formatGitStatusResult(task Task, snapshot gitquery.Snapshot, _ bool) string {
	return fmt.Sprintf("项目：%s\n工作目录：%s\n\n分支：%s\n最新提交：%s\n\n修改：\n%s", firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), snapshot.WorkingDirectory, firstNonEmpty(snapshot.Branch, "未命名或 detached"), firstNonEmpty(snapshot.LatestCommit, "暂无提交"), firstNonEmpty(snapshot.Status, "工作区干净"))
}

func limitRemoteActionResult(value string, remote bool) (string, bool) {
	if !remote || len([]rune(value)) <= remoteDiffLimit {
		return value, false
	}
	return truncateText(value, remoteDiffLimit-80) + "\n\n结果过长，已截断；完整内容请在桌面查看。", true
}

func formatOpenAction(task Task) string {
	conversation := conversationLabel(task)
	if task.ConversationNumber > 0 {
		return fmt.Sprintf("当前 Task：%s\n项目：%s\nConversation：%s\n\n#%d 内容可以直接继续该 Conversation。", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), conversation, task.ConversationNumber)
	}
	return fmt.Sprintf("当前 Task：%s\n项目：%s\nConversation：%s\n\n当前 Task 没有可跳转的数字 Conversation。", task.NumberLabel(), firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), conversation)
}

func (s *Service) executeRemoteActions(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) != 1 {
		return "用法：/actions <任务编号>", true, nil
	}
	number, err := parseTaskNumber(arguments[0])
	if err != nil {
		return "任务编号无效。", true, nil
	}
	task, actions, err := s.AvailableActions(ctx, number)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return fmt.Sprintf("任务 T%d 不存在。", number), true, nil
		}
		return "查询可用操作失败：" + err.Error(), true, nil
	}
	if len(actions) == 0 {
		return fmt.Sprintf("%s 当前没有可用操作。", task.NumberLabel()), true, nil
	}
	lines := []string{fmt.Sprintf("%s 可用操作：", task.NumberLabel())}
	for _, action := range actions {
		lines = append(lines, fmt.Sprintf("%s — %s", action.Id, action.DisplayName))
	}
	return strings.Join(lines, "\n"), true, nil
}

func (s *Service) executeRemoteAction(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) < 2 {
		return "用法：/action <任务编号> <操作> [参数]\n支持：continue、retry、cancel、test、diff [full]、git-status、commit、commit-push、open", true, nil
	}
	number, err := parseTaskNumber(arguments[0])
	if err != nil {
		return "任务编号无效。", true, nil
	}
	actionID := strings.ToLower(strings.TrimSpace(arguments[1]))
	if actionID == "status" {
		actionID = TaskActionGitStatus
	}
	request := TaskActionRequest{ActionID: actionID, Remote: true}
	extra := arguments[2:]
	switch actionID {
	case TaskActionContinue:
		if len(extra) == 0 {
			return "用法：/action <任务编号> continue <继续内容>", true, nil
		}
		request.Text = strings.TrimSpace(strings.Join(extra, " "))
	case TaskActionDiff:
		if len(extra) > 1 || (len(extra) == 1 && !strings.EqualFold(extra[0], "full")) {
			return "用法：/action <任务编号> diff [full]", true, nil
		}
		request.Full = len(extra) == 1
	case TaskActionRetry, TaskActionCancel, TaskActionTest, TaskActionGitStatus, TaskActionCommit, TaskActionCommitPush, TaskActionOpen:
		if len(extra) > 0 {
			return fmt.Sprintf("操作 %s 不接受额外参数。", actionID), true, nil
		}
	default:
		return fmt.Sprintf("未知操作 %q。使用 /actions %d 查看当前可用操作。", actionID, number), true, nil
	}
	if actionID == TaskActionCommitPush {
		return s.createRemotePendingAction(ctx, message, number, request)
	}
	response, err := s.ExecuteAction(ctx, number, request)
	if err != nil {
		return taskFailureTextForAction(number, "操作失败：", err), true, nil
	}
	return formatRemoteActionResponse(response), true, nil
}

func (s *Service) createRemotePendingAction(ctx context.Context, message channels.InboundMessage, number int, _ TaskActionRequest) (string, bool, error) {
	task, actions, err := s.AvailableActions(ctx, number)
	if err != nil {
		return "查询任务失败：" + err.Error(), true, nil
	}
	var available TaskActionAvailability
	for _, action := range actions {
		if action.Id == TaskActionCommitPush {
			available = action
			break
		}
	}
	if available.Id == "" {
		return fmt.Sprintf("%s 当前不可用操作：提交并推送", task.NumberLabel()), true, nil
	}
	userID := firstNonEmpty(message.UserID, message.Address.UserID)
	if strings.TrimSpace(userID) == "" {
		return "无法确认请求用户身份，拒绝创建提交并推送确认。", true, nil
	}
	address := message.Address
	address.UserID = userID
	created := time.Now().UTC()
	requestID, err := newActionRequestID()
	if err != nil {
		return "生成确认编号失败：" + err.Error(), true, nil
	}
	pending := PendingTaskAction{
		ActionRequestID: requestID, TaskNumber: number, ActionID: TaskActionCommitPush, Requester: userID,
		ChannelProfileID: address.ChannelProfileID, CreatedAt: created.Format(time.RFC3339Nano), ExpiresAt: created.Add(remoteActionTTL).Format(time.RFC3339Nano),
		address: address, userID: userID,
	}
	s.mu.Lock()
	s.removeExpiredPendingLocked(created)
	s.pendingActions[strings.ToUpper(requestID)] = pending
	s.mu.Unlock()
	branch := "未命名或 detached"
	if s.projects != nil {
		if project, ok := s.projects.Get(task.ProjectID); ok {
			s.mu.RLock()
			gitService := s.git
			s.mu.RUnlock()
			if gitService != nil {
				if value, branchErr := gitService.GetBranch(ctx, project.WorkingDirectory); branchErr == nil && strings.TrimSpace(value) != "" {
					branch = value
				}
			}
		}
	}
	return fmt.Sprintf("准备提交并推送：\n项目：%s\n分支：%s\n\n操作 ID：%s\n确认请输入：\n/confirm %s\n\n确认有效期：5 分钟", firstNonEmpty(task.ProjectNameSnapshot, "项目未设置"), branch, requestID, requestID), true, nil
}

func (s *Service) executeRemoteConfirm(ctx context.Context, message channels.InboundMessage, arguments []string) (string, bool, error) {
	if len(arguments) != 1 {
		return "用法：/confirm <action-id>", true, nil
	}
	requestID := strings.ToUpper(strings.TrimSpace(arguments[0]))
	if requestID == "" {
		return "确认编号不能为空。", true, nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	pending, ok := s.pendingActions[requestID]
	if ok {
		if expires, parseErr := time.Parse(time.RFC3339Nano, pending.ExpiresAt); parseErr != nil || !now.Before(expires) {
			delete(s.pendingActions, requestID)
			ok = false
		}
	}
	if !ok {
		s.mu.Unlock()
		return "确认编号不存在或已过期，请重新发起 /action。", true, nil
	}
	userID := firstNonEmpty(message.UserID, message.Address.UserID)
	address := message.Address
	address.UserID = userID
	authorized := userID != "" && userID == pending.userID && addressKey(address) == addressKey(pending.address) && address.ChannelProfileID == pending.ChannelProfileID
	if !authorized {
		s.mu.Unlock()
		return "该确认编号不属于当前用户或会话，拒绝执行。", true, nil
	}
	// Consume before dispatch so a duplicate confirmation cannot create two
	// child Tasks, even if the backend operation is still in progress.
	delete(s.pendingActions, requestID)
	s.mu.Unlock()
	response, err := s.ExecuteAction(ctx, pending.TaskNumber, TaskActionRequest{ActionID: pending.ActionID, Confirmed: true, Remote: true})
	if err != nil {
		return taskFailureTextForAction(pending.TaskNumber, "确认操作失败：", err), true, nil
	}
	return formatRemoteActionResponse(response), true, nil
}

func (s *Service) removeExpiredPendingLocked(now time.Time) {
	for id, pending := range s.pendingActions {
		expires, err := time.Parse(time.RFC3339Nano, pending.ExpiresAt)
		if err != nil || !now.Before(expires) {
			delete(s.pendingActions, id)
		}
	}
}

func newActionRequestID() (string, error) {
	buffer := make([]byte, 5)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return "A" + strings.ToUpper(hex.EncodeToString(buffer)), nil
}

func formatRemoteActionResponse(response TaskActionResponse) string {
	if response.Result != nil {
		if response.Result.Error != "" {
			return "操作失败：" + response.Result.Error
		}
		return response.Result.Result
	}
	if response.Task != nil {
		return formatTaskCreated(*response.Task)
	}
	return "操作已完成。"
}

func taskFailureTextForAction(number int, prefix string, err error) string {
	if err == nil {
		return prefix + fmt.Sprintf("T%d", number)
	}
	return prefix + err.Error()
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
