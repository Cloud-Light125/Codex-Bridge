package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
)

var HelpText = commandregistry.NewInMemory().HelpText()

type Control interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

// Runtime deliberately exposes read-only state only. Query code cannot start,
// interrupt, or otherwise modify a Codex Turn.
type Runtime interface {
	Status() bridgeruntime.Status
	RuntimeState(string) control.RuntimeState
	ListInteractions(string) []interactions.PendingInteraction
}

type rateLimitsProvider interface {
	AccountRateLimits(context.Context) (map[string]any, error)
}

type Result struct {
	Parts []string
}

type Service struct {
	control  Control
	runtime  Runtime
	registry *threadregistry.Registry
	commands *commandregistry.Registry
	openclaw conversation.IConversationBackend
	now      func() time.Time
}

// SetOpenClawBackend adds OpenClaw to read-only remote queries while leaving
// all existing Codex query and numbered-thread behavior unchanged.
func (s *Service) SetOpenClawBackend(backend conversation.IConversationBackend) {
	s.openclaw = backend
}

func New(controlService Control, runtime Runtime, registry *threadregistry.Registry, commandRegistries ...*commandregistry.Registry) *Service {
	commands := commandregistry.NewInMemory()
	if len(commandRegistries) > 0 && commandRegistries[0] != nil {
		commands = commandRegistries[0]
	}
	return &Service{control: controlService, runtime: runtime, registry: registry, commands: commands, now: time.Now}
}

func (s *Service) Execute(ctx context.Context, text string) (Result, bool) {
	invocation, found := s.commands.Resolve(text)
	if !found {
		return Result{}, false
	}
	if !invocation.Definition.Enabled {
		return one("指令 " + invocation.Definition.Name + " 当前已停用。"), true
	}
	return s.ExecuteAction(ctx, invocation.Definition.Action, invocation.Arguments)
}

func (s *Service) ExecuteAction(ctx context.Context, action string, arguments []string) (Result, bool) {
	switch action {
	case commandregistry.ActionBridgeHelp:
		return one(s.commands.HelpText()), true
	case commandregistry.ActionThreadsList:
		return one(s.threads(ctx, arguments)), true
	case commandregistry.ActionThreadInfo:
		return one(s.threadInfo(ctx, arguments)), true
	case commandregistry.ActionThreadHistory:
		return s.history(ctx, arguments), true
	case commandregistry.ActionThreadRunning:
		return one(s.running(ctx, arguments)), true
	case commandregistry.ActionThreadWaiting:
		return one(s.waiting(ctx, arguments)), true
	case commandregistry.ActionThreadRecent:
		return one(s.recent(ctx, arguments)), true
	case commandregistry.ActionThreadFailed:
		return one(s.failed(ctx, arguments)), true
	case commandregistry.ActionAccountQuota:
		return one(s.quota(ctx, arguments)), true
	case commandregistry.ActionBridgeStatus:
		if len(arguments) > 0 {
			return one(s.threadInfo(ctx, arguments)), true
		}
		return one(s.connectionStatus()), true
	default:
		return Result{}, false
	}
}

func one(text string) Result { return Result{Parts: []string{strings.TrimSpace(text)}} }

func (s *Service) threads(ctx context.Context, arguments []string) string {
	page := 1
	if len(arguments) > 1 {
		return "用法：/threads [页码]"
	}
	if len(arguments) == 1 {
		parsed, err := strconv.Atoi(arguments[0])
		if err != nil || parsed < 1 {
			return "页码必须是大于 0 的整数。"
		}
		page = parsed
	}
	cursor := ""
	var list control.ThreadList
	var err error
	for current := 1; current <= page; current++ {
		list, err = s.control.ListThreads(ctx, 20, cursor)
		if err != nil && s.openclaw == nil {
			return "无法读取 Codex 会话，请确认 Codex 已连接。"
		}
		if current < page {
			if list.NextCursor == "" || list.NextCursor == cursor {
				return "该页没有会话。"
			}
			cursor = list.NextCursor
		}
	}
	var output strings.Builder
	if len(list.Threads) > 0 {
		s.hydrateActivities(ctx, list.Threads)
		fmt.Fprintf(&output, "[Codex] 会话 · 第 %d 页\n\n", page)
	}
	for _, thread := range list.Threads {
		state := s.runtime.RuntimeState(thread.ThreadID).State
		if state == "" {
			state = thread.Status
		}
		fmt.Fprintf(&output, "#%d %s · %s\n", thread.Number, displayTitle(thread.Title), statusChinese(state))
	}
	if page == 1 && s.openclaw != nil {
		if sessions, openclawErr := s.openclaw.ListSessions(ctx, 20); openclawErr == nil && len(sessions) > 0 {
			if output.Len() > 0 {
				output.WriteString("\n")
			}
			output.WriteString("[OpenClaw] 会话\n\n")
			for _, session := range sessions {
				state := session.Status
				if state == "" {
					state = "idle"
				}
				fmt.Fprintf(&output, "[OpenClaw] %s · %s\n", displayTitle(session.Title), statusChinese(state))
				fmt.Fprintf(&output, "  绑定：/bind oc:%s\n", session.Key)
			}
		}
	}
	if output.Len() == 0 {
		if page == 1 {
			return "当前没有可用的 Codex 或 OpenClaw 会话。"
		}
		return "该页没有会话。"
	}
	output.WriteString("\n回复：\n#编号 你的消息；OpenClaw 使用 /bind oc:<session-key> 后直接发送")
	return output.String()
}

func (s *Service) threadInfo(ctx context.Context, arguments []string) string {
	if len(arguments) != 1 {
		return "用法：/thread <编号>"
	}
	record, message := s.resolve(arguments[0])
	if message != "" {
		return message
	}
	detail, err := s.control.ReadThread(ctx, record.ThreadID, false)
	if err != nil || detail.ThreadID == "" {
		return fmt.Sprintf("聊天编号 #%d 当前不可用。", record.Number)
	}
	if detail.Number == 0 {
		detail.Number = record.Number
	}
	lines := []string{fmt.Sprintf("#%d %s", detail.Number, displayTitle(detail.Title))}
	state := detail.Runtime.State
	if state == "" {
		state = detail.Status
	}
	lines = append(lines, "状态："+statusChinese(state))
	if detail.CWD != "" {
		lines = append(lines, "项目："+detail.CWD)
	}
	if detail.Model != "" {
		lines = append(lines, "模型："+detail.Model)
	}
	updated := firstNonEmpty(detail.UpdatedAt, detail.Runtime.LastActivityAt)
	if updated != "" {
		lines = append(lines, "最后更新："+absoluteLocalTime(updated))
	}
	if detail.ThreadID != "" {
		lines = append(lines, "会话 ID："+shortID(detail.ThreadID))
	}
	return strings.Join(lines, "\n")
}

func (s *Service) history(ctx context.Context, arguments []string) Result {
	if len(arguments) < 1 || len(arguments) > 2 {
		return one("用法：/history <编号> [数量]")
	}
	count := 3
	if len(arguments) == 2 {
		parsed, err := strconv.Atoi(arguments[1])
		if err != nil || parsed < 1 || parsed > 10 {
			return one("聊天轮数仅支持 1～10。")
		}
		count = parsed
	}
	record, message := s.resolve(arguments[0])
	if message != "" {
		return one(message)
	}
	detail, err := s.control.ReadThread(ctx, record.ThreadID, true)
	if err != nil || detail.ThreadID == "" {
		return one(fmt.Sprintf("聊天编号 #%d 当前不可用。", record.Number))
	}
	if detail.Number == 0 {
		detail.Number = record.Number
	}
	type round struct{ user, assistant string }
	rounds := make([]round, 0, len(detail.Turns))
	for _, turn := range detail.Turns {
		user, assistant := historyTexts(turn)
		if user == "" {
			continue
		}
		if assistant == "" {
			assistant = "尚未完成"
		}
		rounds = append(rounds, round{user: user, assistant: assistant})
	}
	if len(rounds) > count {
		rounds = rounds[len(rounds)-count:]
	}
	if len(rounds) == 0 {
		return one(fmt.Sprintf("#%d %s\n暂无可显示的聊天记录。", detail.Number, displayTitle(detail.Title)))
	}
	var body strings.Builder
	for index, item := range rounds {
		fmt.Fprintf(&body, "[%d]\n你：\n%s\n\nCodex：\n%s", index+1, item.user, item.assistant)
		if index < len(rounds)-1 {
			body.WriteString("\n\n")
		}
	}
	prefix := fmt.Sprintf("#%d %s\n最近 %d 轮：\n\n", detail.Number, displayTitle(detail.Title), len(rounds))
	return Result{Parts: splitPrefixed(prefix, body.String(), 3200)}
}

func historyTexts(turn control.Turn) (string, string) {
	user := ""
	final := ""
	legacy := ""
	for _, item := range turn.Items {
		switch item.Type {
		case "userMessage":
			if strings.TrimSpace(item.Text) != "" {
				user = strings.TrimSpace(item.Text)
			}
		case "agentMessage":
			text := strings.TrimSpace(item.Text)
			if text == "" {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(item.Phase)) {
			case "final_answer", "final", "answer":
				final = text
			case "":
				legacy = text
			}
		}
	}
	if final == "" && strings.EqualFold(turn.Status, "completed") {
		final = legacy
	}
	return user, final
}

func (s *Service) running(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/running"
	}
	threads, err := s.recentThreads(ctx, 200)
	if err != nil {
		return "无法读取正在执行的任务，请确认 Codex 已连接。"
	}
	s.hydrateActivities(ctx, threads)
	lines := []string{}
	for _, thread := range threads {
		state := s.runtime.RuntimeState(thread.ThreadID)
		if !isRunningState(state.State) {
			continue
		}
		line := fmt.Sprintf("#%d %s", thread.Number, displayTitle(thread.Title))
		if started, ok := parseTime(state.StartedAt); ok {
			if elapsed := s.now().Sub(started); elapsed >= time.Minute {
				line += fmt.Sprintf(" · 已运行 %d 分钟", int(elapsed/time.Minute))
			}
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return "当前没有正在执行的 Codex 任务。"
	}
	return fmt.Sprintf("正在执行 %d 个任务：\n\n%s", len(lines), strings.Join(lines, "\n"))
}

func (s *Service) waiting(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/waiting"
	}
	threads, err := s.recentThreads(ctx, 200)
	if err != nil {
		return "无法读取等待处理的任务，请确认 Codex 已连接。"
	}
	s.hydrateActivities(ctx, threads)
	pending := map[string]string{}
	for _, item := range s.runtime.ListInteractions("pending") {
		label := "等待桌面端审批"
		if item.Kind == interactions.KindUserInput {
			label = "等待你的回答"
		}
		pending[item.ThreadID] = label
	}
	lines := []string{}
	for _, thread := range threads {
		state := s.runtime.RuntimeState(thread.ThreadID)
		label := pending[thread.ThreadID]
		if label == "" {
			switch state.State {
			case bridgeruntime.StateWaitingUserInput:
				label = "等待你的回答"
			case bridgeruntime.StateWaitingApproval:
				label = "等待桌面端审批"
			}
		}
		if label != "" {
			lines = append(lines, fmt.Sprintf("#%d %s · %s", thread.Number, displayTitle(thread.Title), label))
		}
	}
	if len(lines) == 0 {
		return "当前没有等待处理的 Codex 会话。"
	}
	return "等待处理：\n\n" + strings.Join(lines, "\n")
}

func (s *Service) recent(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/recent"
	}
	threads, err := s.recentThreads(ctx, 10)
	if err != nil {
		return "无法读取最近活动，请确认 Codex 已连接。"
	}
	if len(threads) == 0 {
		return "当前没有 Codex 会话活动。"
	}
	lines := make([]string, 0, len(threads))
	for _, thread := range threads {
		lines = append(lines, fmt.Sprintf("#%d %s · %s", thread.Number, displayTitle(thread.Title), relativeTime(thread.UpdatedAt, s.now())))
	}
	return "最近活动：\n\n" + strings.Join(lines, "\n")
}

type failure struct {
	number int
	title  string
	reason string
	at     string
	time   time.Time
}

func (s *Service) failed(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/failed"
	}
	threads, err := s.recentThreads(ctx, 100)
	if err != nil {
		return "无法读取失败任务，请确认 Codex 已连接。"
	}
	results := make(chan []failure, len(threads))
	semaphore := make(chan struct{}, 8)
	var wait sync.WaitGroup
	for _, summary := range threads {
		summary := summary
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()
			detail, readErr := s.control.ReadThread(ctx, summary.ThreadID, true)
			if readErr != nil || detail.ThreadID == "" {
				return
			}
			number := detail.Number
			if number == 0 {
				number = summary.Number
			}
			found := []failure{}
			for _, turn := range detail.Turns {
				if !strings.EqualFold(turn.Status, "failed") {
					continue
				}
				at := firstNonEmpty(turn.UpdatedAt, turn.CreatedAt, detail.UpdatedAt)
				parsed, _ := parseTime(at)
				found = append(found, failure{number: number, title: displayTitle(detail.Title), reason: safeFailureReason(turn.Error), at: at, time: parsed})
			}
			if len(found) > 0 {
				results <- found
			}
		}()
	}
	wait.Wait()
	close(results)
	failures := []failure{}
	for found := range results {
		failures = append(failures, found...)
	}
	sort.SliceStable(failures, func(i, j int) bool { return failures[i].time.After(failures[j].time) })
	if len(failures) > 10 {
		failures = failures[:10]
	}
	if len(failures) == 0 {
		return "最近没有失败的 Codex 任务。"
	}
	var output strings.Builder
	output.WriteString("最近失败：\n")
	for _, item := range failures {
		fmt.Fprintf(&output, "\n#%d %s\n原因：%s", item.number, item.title, item.reason)
		if item.at != "" {
			fmt.Fprintf(&output, "\n时间：%s", shortLocalTime(item.at))
		}
		output.WriteByte('\n')
	}
	return strings.TrimSpace(output.String())
}

func (s *Service) quota(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/quota"
	}
	provider, ok := s.runtime.(rateLimitsProvider)
	if !ok {
		return "当前版本 Codex 未提供可读取的额度信息。"
	}
	payload, err := provider.AccountRateLimits(ctx)
	if err != nil {
		return "当前无法读取 Codex 额度信息。"
	}
	snapshot := selectRateLimitSnapshot(payload)
	if snapshot == nil {
		return "当前版本 Codex 未提供可读取的额度信息。"
	}
	lines := []string{"Codex 使用额度"}
	if plan := stringValue(snapshot["planType"]); plan != "" && plan != "unknown" {
		lines = append(lines, "套餐："+planName(plan))
	}
	for _, window := range []struct {
		name string
		data map[string]any
	}{{"primary", objectValue(snapshot["primary"])}, {"secondary", objectValue(snapshot["secondary"])}} {
		if window.data == nil {
			continue
		}
		used, ok := integerValue(window.data["usedPercent"])
		if !ok {
			continue
		}
		remaining := 100 - used
		if remaining < 0 {
			remaining = 0
		}
		if remaining > 100 {
			remaining = 100
		}
		duration, _ := integerValue(window.data["windowDurationMins"])
		lines = append(lines, fmt.Sprintf("%s：剩余 %d%%", quotaWindowName(window.name, duration), remaining))
		if reset, ok := integerValue(window.data["resetsAt"]); ok && reset > 0 {
			lines = append(lines, "恢复时间："+friendlyResetTime(time.Unix(int64(reset), 0), s.now()))
		}
	}
	if credits := objectValue(snapshot["credits"]); credits != nil {
		if unlimited, _ := credits["unlimited"].(bool); unlimited {
			lines = append(lines, "Credits：无限")
		} else if balance := stringValue(credits["balance"]); balance != "" {
			lines = append(lines, "可用 Credits："+balance)
		}
	}
	if len(lines) == 1 || (len(lines) == 2 && strings.HasPrefix(lines[1], "套餐：")) {
		return "当前版本 Codex 未提供可读取的额度信息。"
	}
	return strings.Join(lines, "\n")
}

func (s *Service) connectionStatus() string {
	status := s.runtime.Status()
	cli := "不可用"
	if status.CodexCLIAvailable {
		cli = firstNonEmpty(status.CodexCLIVersion, "可用")
	}
	server := "未连接"
	if status.AppServerRunning {
		server = "已连接"
	}
	lines := []string{fmt.Sprintf("Codex Bridge 状态\nBridge：运行中\nCodex CLI：%s\nApp Server：%s", cli, server)}
	if s.openclaw != nil {
		openclawStatus := s.openclaw.ConnectionStatus()
		state := firstNonEmpty(openclawStatus.State, "未配置")
		if openclawStatus.Connected {
			state = "已连接"
		}
		lines = append(lines, "OpenClaw Gateway："+state)
	}
	return strings.Join(lines, "\n")
}

func (s *Service) recentThreads(ctx context.Context, limit int) ([]control.ThreadSummary, error) {
	list, err := s.control.ListThreads(ctx, limit, "")
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

// thread/list intentionally omits turns and can report an idle cached status
// while another Codex process owns an in-progress Turn. A read-only thread/read
// refresh supplies the actual last Turn status to RuntimeState reconciliation.
func (s *Service) hydrateActivities(ctx context.Context, threads []control.ThreadSummary) {
	semaphore := make(chan struct{}, 8)
	var wait sync.WaitGroup
	for _, thread := range threads {
		threadID := thread.ThreadID
		if threadID == "" {
			continue
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()
			_, _ = s.control.ReadThread(ctx, threadID, true)
		}()
	}
	wait.Wait()
}

func (s *Service) resolve(value string) (threadregistry.Record, string) {
	selector := strings.TrimSpace(value)
	if strings.HasPrefix(selector, "[") && strings.HasSuffix(selector, "]") {
		selector = strings.TrimSpace(selector[1 : len(selector)-1])
	}
	selector = strings.TrimPrefix(selector, "#")
	number, err := strconv.Atoi(selector)
	if err != nil || number < 1 {
		return threadregistry.Record{}, "请指定聊天编号，例如 /thread 63。"
	}
	if s.registry == nil {
		return threadregistry.Record{}, "聊天编号尚未初始化。"
	}
	record, ok := s.registry.ByNumber(number)
	if !ok {
		return threadregistry.Record{}, fmt.Sprintf("聊天编号 #%d 不存在。", number)
	}
	return record, ""
}

func isRunningState(state string) bool {
	switch state {
	case bridgeruntime.StateAccepted, bridgeruntime.StateRunning, bridgeruntime.StateRunningExternal, bridgeruntime.StateInterrupting:
		return true
	default:
		return false
	}
}

func statusChinese(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "idle", "notloaded":
		return "空闲"
	case "accepted", "running", "running-local", "running-external", "inprogress", "active":
		return "运行中"
	case "waiting", "waiting-user-input", "waitingonuserinput", "waitingoninput":
		return "等待回答"
	case "waiting-approval", "waitingonapproval":
		return "等待桌面端审批"
	case "failed", "persistence-failed", "thread-mismatch", "systemerror":
		return "失败"
	case "stopped", "interrupted", "cancelled", "canceled":
		return "已停止"
	case "interrupting":
		return "正在停止"
	case "completed-unverified":
		return "正在确认结果"
	case "persisted", "completed":
		return "已完成"
	case "unknown":
		return "状态未知"
	default:
		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "waitingonapproval"):
			return "等待桌面端审批"
		case strings.Contains(lower, "waitingoninput") || strings.Contains(lower, "waitingonuserinput"):
			return "等待回答"
		case strings.HasPrefix(lower, "active") || strings.Contains(lower, "running") || strings.Contains(lower, "inprogress"):
			return "运行中"
		case strings.Contains(lower, "fail") || strings.Contains(lower, "error"):
			return "失败"
		default:
			return "状态未知"
		}
	}
}

func safeFailureReason(value string) string {
	lower := strings.ToLower(value)
	switch {
	case strings.Contains(lower, "build"), strings.Contains(lower, "compile"), strings.Contains(value, "构建"), strings.Contains(value, "编译"):
		return "构建失败"
	case strings.Contains(lower, "test"), strings.Contains(value, "测试"):
		return "测试失败"
	case strings.Contains(lower, "rate limit"), strings.Contains(lower, "usage limit"), strings.Contains(value, "额度"):
		return "Codex 使用额度已达上限"
	case strings.Contains(lower, "auth"), strings.Contains(lower, "unauthorized"), strings.Contains(value, "认证"), strings.Contains(value, "登录"):
		return "Codex 认证失败"
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "timed out"), strings.Contains(value, "超时"):
		return "任务执行超时"
	case strings.Contains(lower, "network"), strings.Contains(lower, "connection"), strings.Contains(value, "网络"):
		return "网络请求失败"
	default:
		return "任务执行失败"
	}
}

func displayTitle(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "未命名会话"
	}
	runes := []rune(value)
	if len(runes) > 34 {
		return string(runes[:33]) + "…"
	}
	return value
}

func shortID(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= 12 {
		return value
	}
	return string(runes[:12]) + "…"
}

func parseTime(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func absoluteLocalTime(value string) string {
	parsed, ok := parseTime(value)
	if !ok {
		return value
	}
	return parsed.Local().Format("2006-01-02 15:04")
}

func shortLocalTime(value string) string {
	parsed, ok := parseTime(value)
	if !ok {
		return value
	}
	return parsed.Local().Format("01-02 15:04")
}

func relativeTime(value string, now time.Time) string {
	parsed, ok := parseTime(value)
	if !ok {
		return absoluteLocalTime(value)
	}
	elapsed := now.Sub(parsed)
	if elapsed < 0 {
		elapsed = 0
	}
	switch {
	case elapsed < time.Minute:
		return "刚刚"
	case elapsed < time.Hour:
		return fmt.Sprintf("%d 分钟前", int(elapsed/time.Minute))
	case elapsed < 24*time.Hour:
		return fmt.Sprintf("%d 小时前", int(elapsed/time.Hour))
	case elapsed < 7*24*time.Hour:
		return fmt.Sprintf("%d 天前", int(elapsed/(24*time.Hour)))
	default:
		return parsed.Local().Format("2006-01-02 15:04")
	}
}

func splitPrefixed(prefix, body string, limit int) []string {
	prefix = strings.TrimSpace(prefix)
	body = strings.TrimSpace(body)
	available := limit - len([]rune(prefix)) - 8
	if available < 256 {
		available = 256
	}
	remaining := []rune(body)
	parts := []string{}
	for len(remaining) > 0 {
		end := available
		if end > len(remaining) {
			end = len(remaining)
		} else {
			for index := end; index > available/2; index-- {
				if remaining[index-1] == '\n' {
					end = index
					break
				}
			}
		}
		chunk := strings.TrimSpace(string(remaining[:end]))
		continuation := ""
		if len(parts) > 0 {
			continuation = "\n（续）"
		}
		parts = append(parts, prefix+continuation+"\n\n"+chunk)
		remaining = remaining[end:]
	}
	if len(parts) == 0 {
		parts = append(parts, prefix)
	}
	return parts
}

func selectRateLimitSnapshot(payload map[string]any) map[string]any {
	if byID := objectValue(payload["rateLimitsByLimitId"]); byID != nil {
		if codex := objectValue(byID["codex"]); codex != nil {
			return codex
		}
		keys := make([]string, 0, len(byID))
		for key := range byID {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if snapshot := objectValue(byID[key]); snapshot != nil {
				return snapshot
			}
		}
	}
	return objectValue(payload["rateLimits"])
}

func objectValue(value any) map[string]any {
	object, _ := value.(map[string]any)
	return object
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func integerValue(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil
	default:
		return 0, false
	}
}

func quotaWindowName(fallback string, minutes int) string {
	switch minutes {
	case 300:
		return "5 小时额度"
	case 10080:
		return "周额度"
	}
	if minutes > 0 && minutes%1440 == 0 {
		return fmt.Sprintf("%d 天额度", minutes/1440)
	}
	if minutes > 0 && minutes%60 == 0 {
		return fmt.Sprintf("%d 小时额度", minutes/60)
	}
	if minutes > 0 {
		return fmt.Sprintf("%d 分钟额度", minutes)
	}
	if fallback == "secondary" {
		return "长期额度"
	}
	return "当前额度"
}

func friendlyResetTime(reset, now time.Time) string {
	reset = reset.Local()
	now = now.Local()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	day := time.Date(reset.Year(), reset.Month(), reset.Day(), 0, 0, 0, 0, reset.Location())
	switch day.Sub(today) {
	case 0:
		return "今天 " + reset.Format("15:04")
	case 24 * time.Hour:
		return "明天 " + reset.Format("15:04")
	default:
		return reset.Format("1 月 2 日 15:04")
	}
}

func planName(value string) string {
	switch strings.ToLower(value) {
	case "free":
		return "Free"
	case "go":
		return "Go"
	case "plus":
		return "Plus"
	case "pro", "prolite":
		return "Pro"
	case "team", "business", "self_serve_business_usage_based":
		return "Business"
	case "edu":
		return "Edu"
	case "enterprise", "ent26", "enterprise_cbp_usage_based":
		return "Enterprise"
	default:
		return value
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
