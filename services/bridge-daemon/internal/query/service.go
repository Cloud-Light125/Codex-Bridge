package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/numberprefix"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/taskcenter"
)

var HelpText = commandregistry.NewInMemory().HelpText()

type Control interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

type readOnlyThreadLister interface {
	ListThreadsReadOnly(context.Context, int, string) (control.ThreadList, error)
}

// Runtime deliberately exposes read-only state only. Query code cannot start,
// interrupt, or otherwise modify a Codex Turn.
type Runtime interface {
	Status() bridgeruntime.Status
	RuntimeState(string) control.RuntimeState
	ListInteractions(string) []interactions.PendingInteraction
}

type liveOutputProvider interface {
	LiveTurnOutput(string, string) (string, bool)
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
	registry any
	commands *commandregistry.Registry
	openclaw conversation.IConversationBackend
	tasks    *taskcenter.Service
	now      func() time.Time
}

// SetOpenClawBackend adds OpenClaw to read-only remote queries while leaving
// all existing Codex query and numbered-thread behavior unchanged.
func (s *Service) SetOpenClawBackend(backend conversation.IConversationBackend) {
	s.openclaw = backend
}

func (s *Service) SetTaskService(service *taskcenter.Service) {
	s.tasks = service
}

func New(controlService Control, runtime Runtime, registry any, commandRegistries ...*commandregistry.Registry) *Service {
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
	return s.ExecuteActionForBackend(ctx, conversation.BackendCodex, "", invocation.Definition.Action, invocation.Arguments)
}

func (s *Service) ExecuteAction(ctx context.Context, action string, arguments []string) (Result, bool) {
	return s.ExecuteActionForBackend(ctx, conversation.BackendCodex, "", action, arguments)
}

// ExecuteActionForBackend is the shared command handler entry point. The
// registry decides whether an action is supported by the selected backend;
// this service then dispatches to the corresponding backend adapter while
// keeping formatting and argument validation in one place for both channels.
func (s *Service) ExecuteActionForBackend(ctx context.Context, backend, target, action string, arguments []string) (Result, bool) {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		backend = conversation.BackendCodex
	}
	if !s.commands.ActionSupportsBackend(action, backend) {
		return one(NotApplicableText(backend)), true
	}
	switch action {
	case commandregistry.ActionBridgeHelp:
		return one(s.commands.HelpTextForBackend(backend)), true
	case commandregistry.ActionThreadsList:
		if backend == conversation.BackendOpenClaw {
			return one(s.openClawThreads(ctx, arguments)), true
		}
		return one(s.threads(ctx, arguments)), true
	case commandregistry.ActionThreadInfo:
		if backend == conversation.BackendOpenClaw {
			return one(s.openClawThreadInfo(ctx, arguments, target)), true
		}
		if len(arguments) == 0 && strings.TrimSpace(target) != "" {
			arguments = []string{target}
		}
		return one(s.threadInfo(ctx, arguments)), true
	case commandregistry.ActionThreadCurrent:
		// Keep the channel-specific unbound response (it knows the address and
		// profile), but use the same read-only status formatter for a bound
		// target on both QQ and Telegram.
		if strings.TrimSpace(target) == "" {
			return Result{}, false
		}
		if backend == conversation.BackendOpenClaw {
			return one(s.openClawThreadInfo(ctx, arguments, target)), true
		}
		if len(arguments) == 0 {
			arguments = []string{target}
		}
		return one(s.threadInfo(ctx, arguments)), true
	case commandregistry.ActionThreadHistory:
		if backend == conversation.BackendOpenClaw {
			return s.openClawHistory(ctx, arguments, target), true
		}
		if len(arguments) == 0 && strings.TrimSpace(target) != "" {
			arguments = []string{target}
		}
		return s.history(ctx, arguments), true
	case commandregistry.ActionThreadRunning:
		return one(s.running(ctx, arguments)), true
	case commandregistry.ActionThreadWaiting:
		return one(s.waiting(ctx, arguments)), true
	case commandregistry.ActionThreadRecent:
		if backend == conversation.BackendOpenClaw {
			return one(s.openClawRecent(ctx, arguments)), true
		}
		return one(s.recent(ctx, arguments)), true
	case commandregistry.ActionThreadFailed:
		return one(s.failed(ctx, arguments)), true
	case commandregistry.ActionAccountQuota:
		return one(s.quota(ctx, arguments)), true
	case commandregistry.ActionBridgeStatus:
		if len(arguments) > 0 || strings.TrimSpace(target) != "" {
			if backend == conversation.BackendOpenClaw {
				return one(s.openClawThreadInfo(ctx, arguments, target)), true
			}
			if len(arguments) == 0 {
				arguments = []string{target}
			}
			return one(s.threadInfo(ctx, arguments)), true
		}
		return one(s.connectionStatusForBackend(backend)), true
	case commandregistry.ActionCurrentOutput:
		return s.output(ctx, arguments, target, false, backend), true
	case commandregistry.ActionLastOutput:
		return s.output(ctx, arguments, target, true, backend), true
	case commandregistry.ActionRecentProjects:
		return one(s.recentProjects(ctx, arguments)), true
	case commandregistry.ActionProjectChats:
		return one(s.projectChats(ctx, arguments)), true
	case commandregistry.ActionOpenClawRefresh:
		return one(s.openClawRefresh(ctx, arguments)), true
	default:
		return Result{}, false
	}
}

func NotApplicableText(backend string) string {
	if backend == conversation.BackendOpenClaw {
		return "该指令不适用于 OpenClaw；它依赖 Codex 的专属运行状态。"
	}
	return "该指令不适用于当前会话后端。"
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
		list, err = s.listThreadsReadOnly(ctx, 20, cursor)
		if err != nil {
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
	store := conversationregistry.ForAny(s.registry)
	if len(list.Threads) > 0 {
		s.hydrateActivities(ctx, list.Threads)
		fmt.Fprintf(&output, "[Codex] 会话 · 第 %d 页\n\n", page)
	}
	for _, thread := range list.Threads {
		number := thread.Number
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendCodex, thread.ThreadID); ok {
				number = record.Number
			}
		}
		state := s.runtime.RuntimeState(thread.ThreadID).State
		if state == "" {
			state = thread.Status
		}
		fmt.Fprintf(&output, "#%d %s · %s\n", number, displayTitle(thread.Title), statusChinese(state))
	}
	if output.Len() == 0 {
		if page == 1 {
			return "当前没有可用的 Codex 会话。"
		}
		return "该页没有会话。"
	}
	output.WriteString("\n回复：\n#编号 你的消息")
	return output.String()
}

func (s *Service) openClawThreads(ctx context.Context, arguments []string) string {
	if len(arguments) > 1 {
		return "用法：/threads [页码]"
	}
	page := 1
	if len(arguments) == 1 {
		parsed, err := strconv.Atoi(strings.TrimSpace(arguments[0]))
		if err != nil || parsed < 1 {
			return "页码必须是大于 0 的整数。"
		}
		page = parsed
	}
	if s.openclaw == nil {
		return "当前后端是 OpenClaw，但 Gateway 尚未配置。"
	}
	sessions, err := s.openclaw.ListSessions(ctx, 200)
	if err != nil {
		return "无法读取 OpenClaw Session，请检查 Gateway 连接。"
	}
	start := (page - 1) * 20
	if start >= len(sessions) {
		if page == 1 {
			return "当前没有可用的 OpenClaw Session。"
		}
		return "该页没有 OpenClaw Session。"
	}
	end := start + 20
	if end > len(sessions) {
		end = len(sessions)
	}
	// A backend may return sessions in activity order. The number, rather than
	// that order, is the stable user-facing selector, so list it numerically.
	store := conversationregistry.ForAny(s.registry)
	if store != nil {
		for index := range sessions {
			if record, ok := store.ByTarget(conversationregistry.BackendOpenClaw, sessions[index].Key); ok {
				sessions[index].Number = record.Number
			}
		}
	}
	sort.SliceStable(sessions, func(i, j int) bool {
		if sessions[i].Number == sessions[j].Number {
			return sessions[i].Key < sessions[j].Key
		}
		return sessions[i].Number < sessions[j].Number
	})
	var output strings.Builder
	fmt.Fprintf(&output, "[OpenClaw] 会话 · 第 %d 页\n\n", page)
	for _, session := range sessions[start:end] {
		fmt.Fprintf(&output, "#%d %s\n    SessionKey：%s\n    状态：%s\n\n", session.Number, displayTitle(session.Title), session.Key, statusChinese(firstNonEmpty(session.Status, "idle")))
	}
	output.WriteString("回复：#12 你的消息；也可使用 /bind 12 绑定。")
	return strings.TrimSpace(output.String())
}

func (s *Service) openClawThreadInfo(ctx context.Context, arguments []string, target string) string {
	if len(arguments) > 1 {
		return "用法：/thread [编号]"
	}
	session, message := s.resolveOpenClawSession(ctx, arguments, target)
	if message != "" {
		return message
	}
	state := statusChinese(firstNonEmpty(session.Status, "idle"))
	lines := []string{fmt.Sprintf("#%d %s", session.Number, displayTitle(session.Title)), "后端：OpenClaw", fmt.Sprintf("Session：#%d %s", session.Number, displayTitle(session.Title)), "状态：" + state}
	if session.Model != "" {
		lines = append(lines, "模型："+session.Model)
	}
	if session.UpdatedAt != "" {
		lines = append(lines, "最后更新："+absoluteLocalTime(session.UpdatedAt))
	}
	lines = append(lines, "SessionKey："+session.Key)
	return strings.Join(lines, "\n")
}

func (s *Service) openClawHistory(ctx context.Context, arguments []string, target string) Result {
	if len(arguments) > 2 {
		return one("用法：/history [编号] [数量]")
	}
	count := 3
	if len(arguments) == 2 {
		parsed, err := strconv.Atoi(arguments[1])
		if err != nil || parsed < 1 || parsed > 10 {
			return one("聊天轮数仅支持 1～10。")
		}
		count = parsed
	}
	session, message := s.resolveOpenClawSession(ctx, arguments, target)
	if message != "" {
		return one(message)
	}
	if s.openclaw == nil {
		return one("当前后端是 OpenClaw，但 Gateway 尚未配置。")
	}
	detail, err := s.openclaw.ReadSession(ctx, session.Key)
	if err != nil || detail.Key == "" {
		return one(fmt.Sprintf("OpenClaw 会话 #%d 当前不可用。", session.Number))
	}
	if detail.Number == 0 {
		detail.Number = session.Number
	}
	type round struct{ user, assistant string }
	rounds := make([]round, 0, len(detail.Messages))
	for _, item := range detail.Messages {
		text := strings.TrimSpace(item.Text)
		if text == "" {
			continue
		}
		if strings.EqualFold(item.Role, "user") {
			rounds = append(rounds, round{user: text})
			continue
		}
		if len(rounds) > 0 {
			if rounds[len(rounds)-1].assistant == "" {
				rounds[len(rounds)-1].assistant = text
			} else {
				rounds[len(rounds)-1].assistant += "\n" + text
			}
		}
	}
	if len(rounds) > count {
		rounds = rounds[len(rounds)-count:]
	}
	if len(rounds) == 0 {
		return one(fmt.Sprintf("#%d %s\n暂无可显示的聊天记录。", detail.Number, displayTitle(detail.Title)))
	}
	var body strings.Builder
	for index, item := range rounds {
		assistant := item.assistant
		if assistant == "" {
			assistant = "尚未完成"
		}
		fmt.Fprintf(&body, "[%d]\n你：\n%s\n\nOpenClaw：\n%s", index+1, item.user, assistant)
		if index < len(rounds)-1 {
			body.WriteString("\n\n")
		}
	}
	prefix := fmt.Sprintf("#%d %s\n最近 %d 轮：\n\n", detail.Number, displayTitle(detail.Title), len(rounds))
	return Result{Parts: splitPrefixed(prefix, body.String(), 3200)}
}

func (s *Service) openClawRecent(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/recent"
	}
	if s.openclaw == nil {
		return "当前后端是 OpenClaw，但 Gateway 尚未配置。"
	}
	sessions, err := s.openclaw.ListSessions(ctx, 200)
	if err != nil {
		return "无法读取最近 OpenClaw Session，请检查 Gateway 连接。"
	}
	sort.SliceStable(sessions, func(i, j int) bool {
		a, aOK := parseTime(sessions[i].UpdatedAt)
		b, bOK := parseTime(sessions[j].UpdatedAt)
		if aOK && bOK && !a.Equal(b) {
			return a.After(b)
		}
		return sessions[i].Number < sessions[j].Number
	})
	if len(sessions) > 10 {
		sessions = sessions[:10]
	}
	if len(sessions) == 0 {
		return "当前没有 OpenClaw Session 活动。"
	}
	lines := make([]string, 0, len(sessions))
	for _, session := range sessions {
		lines = append(lines, fmt.Sprintf("#%d %s · %s", session.Number, displayTitle(session.Title), relativeTime(session.UpdatedAt, s.now())))
	}
	return "最近 OpenClaw 活动：\n\n" + strings.Join(lines, "\n")
}

func (s *Service) openClawRefresh(ctx context.Context, arguments []string) string {
	if len(arguments) != 0 {
		return "用法：/oc-refresh"
	}
	if s.openclaw == nil {
		return "当前后端是 OpenClaw，但 Gateway 尚未配置。"
	}
	sessions, err := s.openclaw.ListSessions(ctx, 200)
	if err != nil {
		return "OpenClaw Session 刷新失败，请检查 Gateway 连接。"
	}
	return fmt.Sprintf("OpenClaw 专属：Session 列表已刷新，共 %d 个。", len(sessions))
}

func (s *Service) resolveOpenClawSession(ctx context.Context, arguments []string, target string) (conversation.Session, string) {
	selector := strings.TrimSpace(target)
	selectorIsTarget := len(arguments) == 0 && selector != ""
	if len(arguments) > 0 {
		selector = strings.TrimSpace(arguments[0])
	}
	if selector == "" {
		return conversation.Session{}, "当前聊天尚未绑定 OpenClaw Session，请先使用 /threads 或 /bind。"
	}
	if !selectorIsTarget {
		for _, prefix := range []string{"oc:", "openclaw:"} {
			if strings.HasPrefix(strings.ToLower(selector), prefix) {
				selector = strings.TrimSpace(selector[len(prefix):])
				selectorIsTarget = true
				break
			}
		}
	}
	if !selectorIsTarget {
		if parsed, recognized, err := numberprefix.Parse(selector); err != nil {
			return conversation.Session{}, err.Error()
		} else if recognized && parsed.Number > 0 {
			if parsed.Content != "" {
				return conversation.Session{}, "请只指定一个 OpenClaw Session 编号。"
			}
			if store := conversationregistry.ForAny(s.registry); store != nil {
				record, ok := store.ByNumber(parsed.Number)
				if !ok {
					return conversation.Session{}, fmt.Sprintf("聊天编号 #%d 不存在。", parsed.Number)
				}
				if record.Backend != conversationregistry.BackendOpenClaw {
					return conversation.Session{}, fmt.Sprintf("聊天编号 #%d 属于 Codex，不是 OpenClaw。", parsed.Number)
				}
			}
			session, err := s.openClawByNumber(ctx, parsed.Number)
			if err != nil {
				return conversation.Session{}, fmt.Sprintf("OpenClaw 会话编号 #%d 不存在。", parsed.Number)
			}
			return session, ""
		}
	}
	if s.openclaw == nil {
		return conversation.Session{}, "当前后端是 OpenClaw，但 Gateway 尚未配置。"
	}
	session, err := s.openclaw.ReadSession(ctx, selector)
	if err != nil || session.Key == "" {
		return conversation.Session{}, "指定的 OpenClaw Session 不存在或当前不可用。"
	}
	return session.Session, ""
}

func (s *Service) openClawByNumber(ctx context.Context, number int) (conversation.Session, error) {
	if s.openclaw == nil {
		return conversation.Session{}, errors.New("OpenClaw backend is unavailable")
	}
	if store := conversationregistry.ForAny(s.registry); store != nil {
		if record, ok := store.ByNumber(number); !ok || record.Backend != conversationregistry.BackendOpenClaw {
			return conversation.Session{}, errors.New("OpenClaw session number not found")
		} else {
			detail, err := s.openclaw.ReadSession(ctx, record.TargetID)
			if err != nil {
				return conversation.Session{}, err
			}
			if detail.Key == "" {
				return conversation.Session{}, errors.New("OpenClaw session not found")
			}
			detail.Number = record.Number
			detail.Backend = conversation.BackendOpenClaw
			return detail.Session, nil
		}
	}
	if numbered, ok := s.openclaw.(conversation.NumberedSessionBackend); ok {
		return numbered.SessionByNumber(ctx, number)
	}
	sessions, err := s.openclaw.ListSessions(ctx, 200)
	if err != nil {
		return conversation.Session{}, err
	}
	for _, session := range sessions {
		if session.Number == number {
			return session, nil
		}
	}
	return conversation.Session{}, errors.New("OpenClaw session number not found")
}

func (s *Service) threadInfo(ctx context.Context, arguments []string) string {
	if len(arguments) != 1 {
		return "用法：/thread <编号>"
	}
	record, message := s.resolve(arguments[0])
	if message != "" {
		return message
	}
	if record.Backend != conversationregistry.BackendCodex {
		return fmt.Sprintf("聊天编号 #%d 属于 OpenClaw，请按 OpenClaw 会话方式查询。", record.Number)
	}
	detail, err := s.control.ReadThread(ctx, record.TargetID, false)
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
	lines = append(lines, "当前状态："+currentStateChinese(detail.Runtime, state))
	lines = append(lines, "最近任务："+lastTurnChinese(detail.Runtime))
	lines = append(lines, "结果确认："+persistenceChinese(detail.Runtime))
	if detail.Runtime.PersistenceStatus == bridgeruntime.PersistenceAbnormal {
		lines = append(lines, "说明：Codex 已完成任务，但 Bridge 暂时无法确认最终回复已写入会话历史。")
	}
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
	if record.Backend != conversationregistry.BackendCodex {
		return one(fmt.Sprintf("聊天编号 #%d 属于 OpenClaw，请按 OpenClaw 会话方式查询。", record.Number))
	}
	detail, err := control.ReadThreadHistory(ctx, s.control, record.TargetID, count)
	if err != nil || detail.ThreadID == "" {
		return one(fmt.Sprintf("聊天编号 #%d 当前不可用。", record.Number))
	}
	if detail.Number == 0 {
		detail.Number = record.Number
	}
	type round struct{ user, assistant string }
	rounds := make([]round, 0, len(detail.Turns))
	selectionMode := control.FinalSelectionModeForHistory(detail.HistoryMode)
	for _, turn := range detail.Turns {
		user, assistant := historyTexts(turn, selectionMode)
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

func historyTexts(turn control.Turn, mode control.FinalSelectionMode) (string, string) {
	user := ""
	for _, item := range turn.Items {
		if item.Type == "userMessage" && strings.TrimSpace(item.Text) != "" {
			user = strings.TrimSpace(item.Text)
		}
	}
	final, ok := control.SelectFinalAssistantItem(turn, mode)
	if !ok {
		return user, ""
	}
	return user, strings.TrimSpace(final.Text)
}

type outputTarget struct {
	Backend  string
	TargetID string
	TurnID   string
	Number   int
	Title    string
	Task     *taskcenter.Task
}

func (s *Service) output(ctx context.Context, arguments []string, target string, last bool, backend string) Result {
	if len(arguments) > 1 {
		name := "/output"
		if last {
			name = "/last-output"
		}
		return one("用法：" + name + " <聊天编号或任务编号>")
	}
	resolved, message := s.resolveOutputTarget(ctx, arguments, target, backend)
	if message != "" {
		return one(message)
	}
	if resolved.Backend == conversation.BackendOpenClaw {
		return s.openClawOutput(ctx, resolved, last)
	}
	return s.codexOutput(ctx, resolved, last)
}

func (s *Service) resolveOutputTarget(ctx context.Context, arguments []string, target, backend string) (outputTarget, string) {
	selector := ""
	if len(arguments) == 1 {
		selector = strings.TrimSpace(arguments[0])
	}
	if selector == "" {
		selector = strings.TrimSpace(target)
	}
	if selector == "" {
		return outputTarget{}, "请指定目标，例如 /output 280 或 /output T108。"
	}
	if len(arguments) == 0 && strings.TrimSpace(target) != "" {
		if store := conversationregistry.ForAny(s.registry); store != nil {
			if record, ok := store.ByTarget(backend, target); ok {
				return outputTarget{Backend: record.Backend, TargetID: record.TargetID, Number: record.Number, Title: record.Title}, ""
			}
		}
	}
	if isTaskSelector(selector) {
		if s.tasks == nil {
			return outputTarget{}, "Task Center 尚未初始化，无法解析任务编号。"
		}
		number, err := strconv.Atoi(strings.TrimSpace(selector[1:]))
		if err != nil || number < 1 {
			return outputTarget{}, "任务编号格式应为 T108。"
		}
		task, ok := s.tasks.Tasks().Get(number)
		if !ok || strings.TrimSpace(task.TargetID) == "" {
			return outputTarget{}, fmt.Sprintf("任务 T%d 不存在或尚未关联运行会话。", number)
		}
		return outputTarget{Backend: firstNonEmpty(task.Backend, conversation.BackendCodex), TargetID: task.TargetID, TurnID: task.CurrentRunID, Number: task.ConversationNumber, Title: task.Title, Task: &task}, ""
	}
	selector = strings.TrimPrefix(selector, "#")
	if number, err := strconv.Atoi(selector); err == nil && number > 0 {
		store := conversationregistry.ForAny(s.registry)
		if store == nil {
			return outputTarget{}, "聊天编号尚未初始化。"
		}
		record, ok := store.ByNumber(number)
		if !ok {
			return outputTarget{}, fmt.Sprintf("聊天编号 #%d 不存在。", number)
		}
		return outputTarget{Backend: record.Backend, TargetID: record.TargetID, Number: record.Number, Title: record.Title}, ""
	}
	if store := conversationregistry.ForAny(s.registry); store != nil {
		if record, ok := store.ByTarget(backend, selector); ok {
			return outputTarget{Backend: record.Backend, TargetID: record.TargetID, Number: record.Number, Title: record.Title}, ""
		}
	}
	return outputTarget{}, "无法解析目标，请使用全局聊天编号（如 280）或任务编号（如 T108）。"
}

func isTaskSelector(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 2 || !strings.EqualFold(value[:1], "T") {
		return false
	}
	_, err := strconv.Atoi(value[1:])
	return err == nil
}

func (s *Service) codexOutput(ctx context.Context, target outputTarget, last bool) Result {
	if target.TargetID == "" {
		return one("Codex 会话目标不可用。")
	}
	state := s.runtime.RuntimeState(target.TargetID)
	turnID := strings.TrimSpace(target.TurnID)
	activeTurnID := firstNonEmpty(state.TurnID, target.TurnID)
	if !last {
		if target.Task != nil && !target.Task.IsActive() {
			return one(fmt.Sprintf("#%d 当前没有正在运行的任务。\n上一次输出请使用 /last-output %d。", target.Number, target.Number))
		}
		if turnID == "" {
			turnID = state.TurnID
		}
		if turnID == "" || !runtimeOutputActive(state.State) {
			label := fmt.Sprintf("#%d", target.Number)
			if target.Task != nil {
				label = target.Task.NumberLabel()
			}
			return one(label + " 当前没有正在运行的任务。\n最近一轮可使用 /last-output。")
		}
	} else if runtimeOutputActive(state.State) || (target.Task != nil && target.Task.IsActive()) {
		// While a Turn is active (including completed-unverified), the
		// current run is not the "last output" yet. Resolve the preceding
		// Turn from bounded history below.
		turnID = ""
	}
	if turnID == "" {
		detail, err := control.ReadThreadHistory(ctx, s.control, target.TargetID, control.DefaultHistoryTurnLimit)
		if err != nil || len(detail.Turns) == 0 {
			return one("无法读取指定会话的运行输出，请确认 Codex 已连接。")
		}
		activeID := activeTurnID
		for index := len(detail.Turns) - 1; index >= 0; index-- {
			if activeID != "" && detail.Turns[index].TurnID == activeID {
				if index > 0 {
					turnID = detail.Turns[index-1].TurnID
				}
				break
			}
		}
		if turnID == "" {
			turnID = detail.Turns[len(detail.Turns)-1].TurnID
		}
	}
	detail, err := s.readSingleTurn(ctx, target.TargetID, turnID)
	if err != nil {
		if live, ok := s.liveOutput(target.TargetID, turnID); ok {
			return s.formatOutput(target, turnID, state.State, live, true)
		}
		return one("无法读取指定 Turn 的输出，请确认 Codex 历史仍可用。")
	}
	text := assistantVisibleOutput(detail.Turns)
	if !last {
		if live, ok := s.liveOutput(target.TargetID, turnID); ok {
			text = live
		}
	}
	if text == "" {
		if live, ok := s.liveOutput(target.TargetID, turnID); ok {
			text = live
		}
	}
	if text == "" {
		text = "（当前 Turn 尚未产生可见的助手文字输出。）"
	}
	return s.formatOutput(target, turnID, firstNonEmpty(turnStatus(detail.Turns), state.State), text, false)
}

func (s *Service) readSingleTurn(ctx context.Context, threadID, turnID string) (control.ThreadDetail, error) {
	if reader, ok := s.control.(control.DetailTurnReader); ok {
		return reader.ReadThreadTurnOutput(ctx, threadID, turnID)
	}
	detail, err := control.ReadThreadHistory(ctx, s.control, threadID, control.DefaultHistoryTurnLimit)
	if err != nil {
		return control.ThreadDetail{}, err
	}
	for _, turn := range detail.Turns {
		if turn.TurnID == turnID {
			detail.Turns = []control.Turn{turn}
			return detail, nil
		}
	}
	return control.ThreadDetail{}, errors.New("requested Turn was not found")
}

func (s *Service) liveOutput(threadID, turnID string) (string, bool) {
	provider, ok := s.runtime.(liveOutputProvider)
	if !ok {
		return "", false
	}
	return provider.LiveTurnOutput(threadID, turnID)
}

func (s *Service) formatOutput(target outputTarget, turnID, status, body string, recovered bool) Result {
	label := fmt.Sprintf("#%d", target.Number)
	if target.Task != nil {
		label = target.Task.NumberLabel()
	} else if target.Number < 1 {
		label = firstNonEmpty(target.Title, "Codex 会话")
	}
	header := label + " "
	if target.Task != nil || !strings.HasPrefix(label, "#") {
		header += "运行输出"
	} else if strings.EqualFold(status, "completed") || strings.EqualFold(status, "persisted") {
		header += "上一次运行输出"
	} else {
		header += "当前运行输出"
	}
	if target.Title != "" {
		header += "\n标题：" + displayTitle(target.Title)
	}
	header += "\nTurn：" + shortID(turnID) + "\n状态：" + statusChinese(status)
	if recovered {
		header += "\n说明：以下为当前能够从 Codex 会话中读取到的输出；Bridge 重启前尚未写入历史的实时片段可能不可恢复。"
	}
	return Result{Parts: splitPrefixed(header, body, 3200)}
}

func assistantVisibleOutput(turns []control.Turn) string {
	var output strings.Builder
	seen := map[string]bool{}
	for _, turn := range turns {
		for _, item := range turn.Items {
			if !control.IsAssistantMessageItem(item) || strings.TrimSpace(item.Text) == "" {
				continue
			}
			if item.ItemID != "" && seen[item.ItemID] {
				continue
			}
			if item.ItemID != "" {
				seen[item.ItemID] = true
			}
			if output.Len() > 0 {
				output.WriteString("\n\n")
			}
			output.WriteString(strings.TrimSpace(item.Text))
			if output.Len() >= 8*1024*1024 {
				return output.String() + "\n\n（输出达到安全上限，以上为当前可读取的完整前缀。）"
			}
		}
	}
	return output.String()
}

func turnStatus(turns []control.Turn) string {
	if len(turns) == 0 {
		return "unknown"
	}
	return turns[0].Status
}

func runtimeOutputActive(state string) bool {
	switch state {
	case bridgeruntime.StateAccepted, bridgeruntime.StateRunning, bridgeruntime.StateRunningExternal,
		bridgeruntime.StateWaitingApproval, bridgeruntime.StateWaitingUserInput, bridgeruntime.StateInterrupting,
		bridgeruntime.StateCompletedUnverified:
		return true
	default:
		return false
	}
}

func (s *Service) openClawOutput(ctx context.Context, target outputTarget, last bool) Result {
	if s.openclaw == nil {
		return one("OpenClaw 后端尚未配置。")
	}
	detail, err := s.openclaw.ReadSession(ctx, target.TargetID)
	if err != nil || detail.Key == "" {
		return one("指定的 OpenClaw Session 当前不可用。")
	}
	if !last && !detail.HasActiveRun {
		return one(fmt.Sprintf("#%d 当前没有正在运行的任务。\n最近一轮可使用 /last-output。", target.Number))
	}
	runID := target.TurnID
	if !last && runID == "" && len(detail.ActiveRunIDs) > 0 {
		runID = detail.ActiveRunIDs[0]
	}
	if last {
		currentRunID := firstNonEmpty(runID, firstString(detail.ActiveRunIDs))
		if detail.HasActiveRun || (target.Task != nil && target.Task.IsActive()) {
			// Select the newest assistant run other than the active one.
			runID = ""
			for index := len(detail.Messages) - 1; index >= 0; index-- {
				item := detail.Messages[index]
				if !strings.EqualFold(item.Role, "assistant") || strings.TrimSpace(item.Text) == "" || item.RunID == "" || item.RunID == currentRunID {
					continue
				}
				runID = item.RunID
				break
			}
		} else if runID == "" {
			for index := len(detail.Messages) - 1; index >= 0; index-- {
				item := detail.Messages[index]
				if strings.EqualFold(item.Role, "assistant") && strings.TrimSpace(item.Text) != "" && item.RunID != "" {
					runID = item.RunID
					break
				}
			}
		}
	}
	if last && (detail.HasActiveRun || (target.Task != nil && target.Task.IsActive())) && runID == "" {
		return one("当前任务尚无可读取的上一轮输出。")
	}
	var parts []string
	for _, item := range detail.Messages {
		if !strings.EqualFold(item.Role, "assistant") || strings.TrimSpace(item.Text) == "" {
			continue
		}
		if runID != "" && item.RunID != "" && item.RunID != runID {
			continue
		}
		parts = append(parts, strings.TrimSpace(item.Text))
	}
	if len(parts) == 0 {
		return one("（当前运行尚未产生可见的助手文字输出。）")
	}
	status := firstNonEmpty(detail.Status, "idle")
	return s.formatOutput(target, runID, status, strings.Join(parts, "\n\n"), false)
}

type projectConversation struct {
	Backend    string
	TargetID   string
	Number     int
	Title      string
	UpdatedAt  string
	Status     string
	TaskNumber int
}

type projectSnapshot struct {
	Name          string
	Aliases       []string
	Path          string
	Configured    bool
	UpdatedAt     string
	Conversations []projectConversation
}

// collectProjectSnapshots combines configured Task Center projects with the
// working directories observed in recent Codex and OpenClaw sessions. It is
// deliberately read-only: no conversation number is allocated here and no
// project or task record is written.
func (s *Service) collectProjectSnapshots(ctx context.Context) ([]projectSnapshot, error) {
	entries := map[string]*projectSnapshot{}
	nameKeys := map[string]string{}
	nameAmbiguous := map[string]bool{}

	registerName := func(name, key string) {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			return
		}
		if prior, ok := nameKeys[name]; ok && prior != key {
			nameAmbiguous[name] = true
			nameKeys[name] = ""
			return
		}
		if !nameAmbiguous[name] {
			nameKeys[name] = key
		}
	}
	lookupName := func(name string) string {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || nameAmbiguous[name] {
			return ""
		}
		return nameKeys[name]
	}
	add := func(name, path string, aliases []string, configured bool, updated string) *projectSnapshot {
		name = strings.TrimSpace(name)
		displayPath := displayProjectPath(path)
		canonicalPath := normalizeProjectPath(path)
		key := ""
		if canonicalPath != "" {
			key = "path:" + canonicalPath
		}
		if key == "" {
			key = lookupName(name)
		}
		if key == "" {
			key = "name:" + strings.ToLower(name)
			if name == "" {
				key = "unknown:" + strconv.Itoa(len(entries)+1)
			}
		}
		entry := entries[key]
		if entry == nil {
			entry = &projectSnapshot{Name: name, Path: displayPath, Aliases: append([]string(nil), aliases...)}
			entries[key] = entry
		} else {
			if entry.Name == "" {
				entry.Name = name
			}
			if entry.Path == "" {
				entry.Path = displayPath
			}
			entry.Aliases = uniqueProjectStrings(append(entry.Aliases, aliases...))
		}
		entry.Configured = entry.Configured || configured
		entry.UpdatedAt = newerTimestamp(entry.UpdatedAt, updated)
		registerName(entry.Name, key)
		for _, alias := range entry.Aliases {
			registerName(alias, key)
		}
		return entry
	}
	addConversation := func(entry *projectSnapshot, item projectConversation) {
		if entry == nil || strings.TrimSpace(item.TargetID) == "" {
			return
		}
		for index := range entry.Conversations {
			current := &entry.Conversations[index]
			if current.Backend == item.Backend && current.TargetID == item.TargetID {
				if current.Title == "" {
					current.Title = item.Title
				}
				current.Number = firstPositive(current.Number, item.Number)
				current.UpdatedAt = newerTimestamp(current.UpdatedAt, item.UpdatedAt)
				current.Status = firstNonEmpty(item.Status, current.Status)
				current.TaskNumber = firstPositive(current.TaskNumber, item.TaskNumber)
				entry.UpdatedAt = newerTimestamp(entry.UpdatedAt, item.UpdatedAt)
				return
			}
		}
		entry.Conversations = append(entry.Conversations, item)
		entry.UpdatedAt = newerTimestamp(entry.UpdatedAt, item.UpdatedAt)
	}

	if s.tasks != nil && s.tasks.Projects() != nil {
		for _, project := range s.tasks.Projects().List() {
			add(project.Name, project.WorkingDirectory, project.Aliases, true, project.UpdatedAt)
		}
	}

	threadList, threadErr := s.listThreadsReadOnly(ctx, 200, "")
	for _, thread := range threadList.Threads {
		if strings.TrimSpace(thread.CWD) == "" {
			continue
		}
		entry := add(projectBaseName(thread.CWD), thread.CWD, nil, false, thread.UpdatedAt)
		addConversation(entry, projectConversation{
			Backend: conversation.BackendCodex, TargetID: thread.ThreadID, Number: thread.Number,
			Title: thread.Title, UpdatedAt: thread.UpdatedAt, Status: thread.Status,
		})
	}
	// A configured project remains useful when Codex is temporarily down; only
	// return the connection error if there is no local project information at
	// all. OpenClaw is likewise best-effort for this read-only aggregate.
	if s.openclaw != nil {
		if sessions, err := s.openclaw.ListSessions(ctx, 200); err == nil {
			for _, session := range sessions {
				if strings.TrimSpace(session.CWD) == "" {
					continue
				}
				entry := add(projectBaseName(session.CWD), session.CWD, nil, false, session.UpdatedAt)
				addConversation(entry, projectConversation{
					Backend: conversation.BackendOpenClaw, TargetID: session.Key, Number: session.Number,
					Title: session.Title, UpdatedAt: session.UpdatedAt, Status: session.Status,
				})
			}
		}
	}

	if s.tasks != nil && s.tasks.Tasks() != nil {
		projectsByID := map[string]taskcenter.Project{}
		if s.tasks.Projects() != nil {
			for _, project := range s.tasks.Projects().List() {
				projectsByID[project.ProjectID] = project
			}
		}
		for _, task := range s.tasks.Tasks().List(taskcenter.TaskFilter{}) {
			if strings.TrimSpace(task.TargetID) == "" {
				continue
			}
			project := projectsByID[task.ProjectID]
			name := firstNonEmpty(task.ProjectNameSnapshot, project.Name, "未命名项目")
			entry := add(name, project.WorkingDirectory, project.Aliases, project.ProjectID != "", task.LastActivityAt)
			addConversation(entry, projectConversation{
				Backend: firstNonEmpty(task.Backend, conversation.BackendCodex), TargetID: task.TargetID,
				Number: task.ConversationNumber, Title: task.Title, UpdatedAt: task.LastActivityAt,
				Status: task.Status, TaskNumber: task.TaskNumber,
			})
		}
	}

	if threadErr != nil && len(entries) == 0 {
		return nil, threadErr
	}
	result := make([]projectSnapshot, 0, len(entries))
	for _, entry := range entries {
		sort.SliceStable(entry.Conversations, func(i, j int) bool {
			return timestampAfter(entry.Conversations[i].UpdatedAt, entry.Conversations[j].UpdatedAt)
		})
		result = append(result, *entry)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].UpdatedAt == result[j].UpdatedAt {
			return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
		}
		return timestampAfter(result[i].UpdatedAt, result[j].UpdatedAt)
	})
	return result, nil
}

func (s *Service) recentProjects(ctx context.Context, arguments []string) string {
	if len(arguments) > 1 {
		return "用法：/recent-projects [数量]"
	}
	limit := 10
	if len(arguments) == 1 {
		parsed, err := strconv.Atoi(arguments[0])
		if err != nil || parsed < 1 || parsed > 50 {
			return "数量必须是 1～50。"
		}
		limit = parsed
	}
	projects, err := s.collectProjectSnapshots(ctx)
	if err != nil {
		return "无法读取最近项目，请确认 Codex 已连接。"
	}
	if len(projects) == 0 {
		return "当前没有可显示的项目。"
	}
	if len(projects) > limit {
		projects = projects[:limit]
	}
	lines := []string{"最近项目："}
	for index, project := range projects {
		name := firstNonEmpty(project.Name, projectBaseName(project.Path), "未命名项目")
		line := fmt.Sprintf("%d. %s", index+1, displayTitle(name))
		if project.Path != "" {
			line += "\n   路径：" + project.Path
		}
		if len(project.Conversations) > 0 {
			latest := project.Conversations[0]
			line += "\n   最近会话：" + projectConversationLabel(latest)
		}
		if project.UpdatedAt != "" {
			line += "\n   最近活动：" + relativeTime(project.UpdatedAt, s.now())
		}
		if !project.Configured {
			line += "\n   来源：最近会话工作目录"
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (s *Service) projectChats(ctx context.Context, arguments []string) string {
	if len(arguments) == 0 {
		return "用法：/project-chats <项目> [数量]"
	}
	limit := 10
	projectArguments := append([]string(nil), arguments...)
	if len(projectArguments) >= 2 {
		if parsed, err := strconv.Atoi(projectArguments[len(projectArguments)-1]); err == nil {
			if parsed < 1 || parsed > 50 {
				return "数量必须是 1～50。"
			}
			limit = parsed
			projectArguments = projectArguments[:len(projectArguments)-1]
		}
	}
	reference := strings.TrimSpace(strings.Join(projectArguments, " "))
	if reference == "" {
		return "用法：/project-chats <项目> [数量]"
	}
	projects, err := s.collectProjectSnapshots(ctx)
	if err != nil {
		return "无法读取项目会话，请确认 Codex 已连接。"
	}
	matches := make([]projectSnapshot, 0, 2)
	for _, project := range projects {
		if projectMatchesReference(project, reference) {
			matches = append(matches, project)
		}
	}
	if len(matches) == 0 {
		return fmt.Sprintf("找不到项目 %q。可先使用 /recent-projects 查看项目名称。", reference)
	}
	if len(matches) > 1 && !projectReferenceIsExactPath(reference) {
		lines := []string{fmt.Sprintf("项目 %q 对应多个工作目录，请使用完整路径：", reference)}
		for _, project := range matches {
			lines = append(lines, "- "+firstNonEmpty(project.Path, project.Name))
		}
		return strings.Join(lines, "\n")
	}
	project := matches[0]
	if len(project.Conversations) == 0 {
		return fmt.Sprintf("项目 %s 当前没有可显示的会话。", firstNonEmpty(project.Name, reference))
	}
	if len(project.Conversations) > limit {
		project.Conversations = project.Conversations[:limit]
	}
	lines := []string{fmt.Sprintf("项目 %s 最近会话：", displayTitle(firstNonEmpty(project.Name, reference)))}
	for index, item := range project.Conversations {
		line := fmt.Sprintf("%d. %s", index+1, projectConversationLabel(item))
		if item.UpdatedAt != "" {
			line += " · " + relativeTime(item.UpdatedAt, s.now())
		}
		if item.Backend == conversation.BackendCodex && item.TargetID != "" {
			state := s.runtime.RuntimeState(item.TargetID)
			if state.State != "" {
				line += "\n   当前状态：" + currentStateChinese(state, state.State)
				line += " · 最近任务：" + lastTurnChinese(state)
			}
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func projectConversationLabel(item projectConversation) string {
	label := ""
	if item.Number > 0 {
		label = fmt.Sprintf("#%d ", item.Number)
	}
	if item.TaskNumber > 0 {
		label += fmt.Sprintf("T%d ", item.TaskNumber)
	}
	label += "[" + firstNonEmpty(item.Backend, "unknown") + "] " + displayTitle(item.Title)
	if item.Status != "" {
		label += " · " + statusChinese(item.Status)
	}
	return strings.TrimSpace(label)
}

func projectMatchesReference(project projectSnapshot, reference string) bool {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return false
	}
	if project.Path != "" && normalizeProjectPath(reference) == normalizeProjectPath(project.Path) {
		return true
	}
	for _, candidate := range append([]string{project.Name}, project.Aliases...) {
		if strings.EqualFold(strings.TrimSpace(candidate), reference) {
			return true
		}
	}
	return false
}

func projectReferenceIsExactPath(value string) bool {
	return strings.Contains(value, `\`) || strings.Contains(value, "/") || filepath.VolumeName(value) != ""
}

func normalizeProjectPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	abs, err := filepath.Abs(value)
	if err == nil {
		value = abs
	}
	return strings.ToLower(filepath.Clean(value))
}

func displayProjectPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	abs, err := filepath.Abs(value)
	if err == nil {
		value = abs
	}
	return filepath.Clean(value)
}

func projectBaseName(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return filepath.Base(filepath.Clean(path))
}

func uniqueProjectStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value != "" && !seen[key] {
			seen[key] = true
			result = append(result, value)
		}
	}
	return result
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func newerTimestamp(current, candidate string) string {
	if timestampAfter(candidate, current) {
		return candidate
	}
	return current
}

func timestampAfter(left, right string) bool {
	leftTime, leftOK := parseTime(left)
	rightTime, rightOK := parseTime(right)
	if leftOK && rightOK {
		return leftTime.After(rightTime)
	}
	if leftOK != rightOK {
		return leftOK
	}
	return strings.Compare(left, right) > 0
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
		line += " · " + statusChinese(state.State)
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return "当前没有正在执行的 Codex 任务。"
	}
	return fmt.Sprintf("运行中或正在确认 %d 个任务：\n\n%s", len(lines), strings.Join(lines, "\n"))
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
			detail, readErr := control.ReadThreadActivityHistory(ctx, s.control, summary.ThreadID, 10)
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

func (s *Service) connectionStatusForBackend(backend string) string {
	if backend != conversation.BackendOpenClaw {
		return s.connectionStatus()
	}
	if s.openclaw == nil {
		return "OpenClaw 状态\nGateway：未配置"
	}
	status := s.openclaw.ConnectionStatus()
	state := firstNonEmpty(status.State, "未配置")
	if status.Connected {
		state = "已连接"
	}
	return fmt.Sprintf("OpenClaw 状态\nGateway：%s\nSession：%d 个\n自动重连：%t", state, status.SessionCount, status.AutoReconnect)
}

func (s *Service) recentThreads(ctx context.Context, limit int) ([]control.ThreadSummary, error) {
	list, err := s.listThreadsReadOnly(ctx, limit, "")
	if err != nil {
		return nil, err
	}
	return list.Threads, nil
}

func (s *Service) listThreadsReadOnly(ctx context.Context, limit int, cursor string) (control.ThreadList, error) {
	if reader, ok := s.control.(readOnlyThreadLister); ok {
		return reader.ListThreadsReadOnly(ctx, limit, cursor)
	}
	// Compatibility fallback for older injected readers. The production
	// control.Service implements the non-allocating method above.
	return s.control.ListThreads(ctx, limit, cursor)
}

// thread/list intentionally omits turns and can report an idle cached status
// while another Codex process owns an in-progress Turn. The activity reader
// refreshes only metadata plus the newest Turn without loading Items.
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
			_, _ = control.ReadThreadActivity(ctx, s.control, threadID)
		}()
	}
	wait.Wait()
}

func (s *Service) resolve(value string) (conversationregistry.Record, string) {
	selector := strings.TrimSpace(value)
	if strings.HasPrefix(selector, "[") && strings.HasSuffix(selector, "]") {
		selector = strings.TrimSpace(selector[1 : len(selector)-1])
	}
	selector = strings.TrimPrefix(selector, "#")
	number, err := strconv.Atoi(selector)
	if err != nil || number < 1 {
		return conversationregistry.Record{}, "请指定聊天编号，例如 /thread 63。"
	}
	store := conversationregistry.ForAny(s.registry)
	if store == nil {
		return conversationregistry.Record{}, "聊天编号尚未初始化。"
	}
	record, ok := store.ByNumber(number)
	if !ok {
		return conversationregistry.Record{}, fmt.Sprintf("聊天编号 #%d 不存在。", number)
	}
	return record, ""
}

func isRunningState(state string) bool {
	switch state {
	case bridgeruntime.StateAccepted, bridgeruntime.StateRunning, bridgeruntime.StateRunningExternal, bridgeruntime.StateInterrupting, bridgeruntime.StateCompletedUnverified:
		return true
	default:
		return false
	}
}

func currentStateChinese(state control.RuntimeState, fallback string) string {
	if strings.TrimSpace(state.State) == "" {
		return statusChinese(fallback)
	}
	return statusChinese(state.State)
}

func lastTurnChinese(state control.RuntimeState) string {
	result := strings.ToLower(strings.TrimSpace(state.LastTurnResult))
	if result == "" && state.Persistence != nil {
		result = strings.ToLower(strings.TrimSpace(state.Persistence.Main.TurnStatus))
	}
	switch result {
	case "completed", "persisted", "success", "succeeded":
		return "已完成"
	case "failed", "error", "systemerror":
		return "失败"
	case "interrupted", "cancelled", "canceled", "stopped":
		return "已中断"
	case "":
		return "暂无结果"
	default:
		return statusChinese(result)
	}
}

func persistenceChinese(state control.RuntimeState) string {
	value := strings.ToLower(strings.TrimSpace(state.PersistenceStatus))
	if value == "" && state.Persistence != nil {
		value = strings.ToLower(strings.TrimSpace(state.Persistence.Status))
	}
	switch value {
	case bridgeruntime.PersistencePending, "completed-unverified", "pending-verification", "verifying":
		return "待确认"
	case bridgeruntime.PersistenceConfirmed, "persisted":
		return "已确认"
	case bridgeruntime.PersistenceAbnormal, "persistence-failed", "thread-mismatch":
		return "异常"
	case "":
		if strings.EqualFold(strings.TrimSpace(state.LastTurnResult), "completed") {
			return "待确认"
		}
		return "暂无结果"
	default:
		return "待确认"
	}
}

func statusChinese(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "idle", "notloaded":
		return "空闲"
	case "accepted", "running", "running-local", "inprogress", "active":
		return "运行中"
	case "running-external":
		return "外部运行中"
	case "waiting", "waiting-user-input", "waitingonuserinput", "waitingoninput":
		return "等待回答"
	case "waiting-approval", "waitingonapproval":
		return "等待桌面端审批"
	case "failed", "systemerror":
		return "失败"
	case "persistence-failed", "thread-mismatch":
		return "结果确认异常"
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

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}
