package qq

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
)

const (
	selectionTTL = 5 * time.Minute
	flowTTL      = 5 * time.Minute
	qqRuneLimit  = 1500
)

type Control interface {
	ListThreads(context.Context, int, string) (control.ThreadList, error)
	ReadThread(context.Context, string, bool) (control.ThreadDetail, error)
}

type Runtime interface {
	Status() bridgeruntime.Status
	RuntimeState(string) control.RuntimeState
	StartTurn(context.Context, string, control.StartTurnRequest) (control.TurnAccepted, error)
	InterruptTurn(context.Context, string, string) (control.InterruptResult, error)
	GetInteraction(string) (interactions.PendingInteraction, bool)
	ListInteractions(string) []interactions.PendingInteraction
	RespondInteraction(context.Context, string, interactions.ResponseRequest) (interactions.PendingInteraction, error)
	VerifyThreadPersistence(context.Context, string) (control.PersistenceVerification, error)
}

type serviceAdapter interface {
	Configure(ConfigureRequest) (AdapterStatus, error)
	QQStatus() AdapterStatus
	SetBindingCount(int)
	Start(context.Context) error
	Stop(context.Context) error
	ClearToken(context.Context) error
	Test(context.Context, TestRequest) TestResult
	SendMessage(context.Context, channels.OutboundMessage) (channels.OutboundResult, error)
}

type threadSelection struct {
	Expires time.Time
	Threads []control.ThreadSummary
}

type turnRoute struct {
	Address               channels.ChannelAddress
	UserID                string
	ThreadID              string
	TurnID                string
	VerificationRequested bool
	LastProgress          map[string]time.Time
	Revoked               bool
}

type interactionFlow struct {
	Expires       time.Time
	Address       channels.ChannelAddress
	UserID        string
	ThreadID      string
	TurnID        string
	InteractionID string
	Questions     []interactions.Question
	Index         int
	Answers       map[string][]string
}

type Service struct {
	adapter   *Adapter
	transport serviceAdapter
	control   Control
	runtime   Runtime
	bindings  *bindings.Repository
	broker    *events.Broker
	logger    *bridgelog.SafeLogger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu             sync.Mutex
	routes         map[string]*turnRoute
	selections     map[string]threadSelection
	flows          map[string]*interactionFlow
	flowByInput    map[string]string
	selfID         string
	reconfiguring  bool
	activeHandlers int
}

func NewService(controlService Control, runtime Runtime, repository *bindings.Repository, broker *events.Broker, logger *bridgelog.SafeLogger) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		control: controlService, runtime: runtime, bindings: repository, broker: broker, logger: logger,
		ctx: ctx, cancel: cancel, done: make(chan struct{}), routes: make(map[string]*turnRoute),
		selections: make(map[string]threadSelection), flows: make(map[string]*interactionFlow), flowByInput: make(map[string]string),
	}
	service.adapter = NewAdapter(service.HandleMessage)
	service.transport = service.adapter
	service.adapter.SetEventHandler(service.handleAdapterEvent)
	go service.eventLoop()
	return service
}

func (s *Service) Adapter() *Adapter { return s.adapter }

func (s *Service) Configure(request ConfigureRequest) (AdapterStatus, error) {
	s.mu.Lock()
	if s.reconfiguring || len(s.routes) > 0 || s.activeHandlers > 0 {
		s.mu.Unlock()
		return AdapterStatus{}, errors.New("QQ cannot be reconfigured while a QQ Turn or message handler is active")
	}
	s.reconfiguring = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.reconfiguring = false
		s.mu.Unlock()
	}()
	status, err := s.transport.Configure(request)
	if err != nil {
		return AdapterStatus{}, err
	}
	s.refreshBindingCount(status.SelfID)
	s.publishChannel(events.ChannelStatusChanged, "")
	return status, nil
}

func (s *Service) Test(ctx context.Context, request TestRequest) TestResult {
	return s.transport.Test(ctx, request)
}

func (s *Service) Start(ctx context.Context) error {
	started := make(chan error, 1)
	go func() { started <- s.transport.Start(s.ctx) }()
	var err error
	select {
	case err = <-started:
	case <-ctx.Done():
		stopContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.transport.Stop(stopContext)
		cancel()
		err = ctx.Err()
	}
	if err != nil {
		s.publishChannel(events.ChannelError, qqErrorCode(err))
		return err
	}
	status := s.transport.QQStatus()
	s.onSelfID(status.SelfID)
	s.refreshBindingCount(status.SelfID)
	return nil
}

func (s *Service) Stop(ctx context.Context) error {
	err := s.transport.Stop(ctx)
	s.clearTransient()
	s.publishChannel(events.ChannelStatusChanged, "")
	return err
}

func (s *Service) Close(ctx context.Context) error {
	err := s.Stop(ctx)
	s.cancel()
	select {
	case <-s.done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}
	return err
}

func (s *Service) DeleteToken(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	if err := s.transport.ClearToken(ctx); err != nil {
		return err
	}
	s.clearTransient()
	s.onSelfID("")
	s.publishChannel(events.ChannelStatusChanged, "")
	return nil
}

func (s *Service) BindingCreated(binding bindings.Binding) {
	if !strings.EqualFold(binding.ChannelType, "qq") {
		return
	}
	s.refreshBindingCount(binding.AccountID)
}

func (s *Service) BindingDeleted(binding bindings.Binding) {
	if !strings.EqualFold(binding.ChannelType, "qq") {
		return
	}
	s.clearAddress(channels.ChannelAddress{
		ChannelType: "qq", AccountID: binding.AccountID, ConversationType: binding.ConversationType, ChatID: binding.ChatID,
	})
	s.refreshBindingCount(binding.AccountID)
}

func (s *Service) HandleMessage(parent context.Context, message channels.InboundMessage) {
	s.mu.Lock()
	if s.reconfiguring {
		s.mu.Unlock()
		return
	}
	s.activeHandlers++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeHandlers--
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil && s.logger != nil {
			s.logger.Printf("QQ message handler recovered messageId=%s", shortID(message.MessageID))
		}
	}()
	text := strings.TrimSpace(message.Text)
	if text == "" || message.Unsupported {
		s.reject(ctx, message, "目前只支持纯文本消息。", "unsupported")
		return
	}
	if strings.HasPrefix(text, "/") {
		s.handleCommand(ctx, message, text)
		return
	}
	if s.answerInteraction(ctx, message, text) {
		return
	}
	s.startTurn(ctx, message, text)
}

func (s *Service) handleCommand(ctx context.Context, message channels.InboundMessage, text string) {
	parts := strings.Fields(text)
	command := strings.ToLower(parts[0])
	argument := ""
	if len(parts) > 1 {
		argument = strings.TrimSpace(strings.Join(parts[1:], " "))
	}
	switch command {
	case "/start":
		if binding, ok := s.findBinding(message.Address); ok {
			s.send(ctx, message.Address, "Codex Bridge 已就绪。当前会话已绑定 Thread "+shortID(binding.ThreadID)+"。\n发送 /help 查看命令。")
		} else {
			s.send(ctx, message.Address, "Codex Bridge 已就绪。当前会话尚未绑定。\n使用 /threads 查看最近会话，再用 /bind 1 绑定。")
		}
	case "/help":
		s.send(ctx, message.Address, "可用命令：\n/threads 最近的 Thread\n/bind <序号、完整 ID 或唯一前缀> 绑定\n/unbind 解除绑定\n/current 当前绑定\n/status 运行状态\n/stop 停止由本 QQ 会话发起的任务\n/cancel 取消正在等待的 QQ 用户输入")
	case "/status":
		s.status(ctx, message)
	case "/threads":
		s.listThreads(ctx, message)
	case "/bind":
		if argument == "" {
			s.send(ctx, message.Address, "用法：/bind <序号、完整 Thread ID 或唯一前缀>。序号必须来自 5 分钟内的 /threads 列表。")
			return
		}
		s.bind(ctx, message, argument)
	case "/unbind":
		s.unbind(ctx, message)
	case "/current":
		s.current(ctx, message)
	case "/stop":
		s.stopTurn(ctx, message)
	case "/cancel":
		s.cancelInteraction(ctx, message)
	default:
		s.send(ctx, message.Address, "未知命令。发送 /help 查看可用命令。")
	}
}

func (s *Service) listThreads(ctx context.Context, message channels.InboundMessage) {
	result, err := s.control.ListThreads(ctx, 10, "")
	if err != nil {
		s.send(ctx, message.Address, "无法读取 Codex Thread，请确认 Codex 已连接。")
		return
	}
	if len(result.Threads) == 0 {
		s.send(ctx, message.Address, "没有找到最近的 Codex Thread。")
		return
	}
	threads := append([]control.ThreadSummary(nil), result.Threads...)
	s.mu.Lock()
	s.selections[addressKey(message.Address)] = threadSelection{Expires: time.Now().Add(selectionTTL), Threads: threads}
	s.mu.Unlock()
	var output strings.Builder
	output.WriteString("最近的 Thread（5 分钟内可用序号绑定）：\n")
	for index, thread := range threads {
		fmt.Fprintf(&output, "%d. %s\n   项目：%s · ID：%s · 更新：%s\n", index+1, displayTitle(thread.Title), projectName(thread.CWD), shortID(thread.ThreadID), displayTime(thread.UpdatedAt))
	}
	s.send(ctx, message.Address, strings.TrimSpace(output.String()))
}

func (s *Service) bind(ctx context.Context, message channels.InboundMessage, selector string) {
	threadID, errText := s.resolveThread(ctx, message.Address, selector)
	if errText != "" {
		s.send(ctx, message.Address, errText)
		return
	}
	thread, err := s.control.ReadThread(ctx, threadID, false)
	if err != nil || thread.ThreadID == "" || thread.ThreadID != threadID {
		s.send(ctx, message.Address, "指定的 Thread 不存在。")
		return
	}
	if thread.Archived != nil && *thread.Archived {
		s.send(ctx, message.Address, "该 Thread 已归档，不能绑定。")
		return
	}
	s.clearAddress(message.Address)
	created, previous, err := s.bindings.UpsertAddress(bindings.CreateRequest{
		ChannelType: "qq", AccountID: message.Address.AccountID, ConversationType: qqConversationType(message.Address.ConversationType),
		ChatID: message.Address.ChatID, TopicID: "", ThreadID: threadID,
	})
	if err != nil {
		s.send(ctx, message.Address, "保存绑定失败。")
		return
	}
	s.refreshBindingCount(created.AccountID)
	payload := safeBindingPayload(created)
	if previous != nil {
		payload["replacedThreadId"] = shortID(previous.ThreadID)
	}
	s.broker.Publish(events.BindingCreated, payload)
	if previous != nil && previous.ThreadID != threadID {
		s.send(ctx, message.Address, fmt.Sprintf("已明确替换原绑定，现绑定到 %s（%s，%s）。", displayTitle(thread.Title), projectName(thread.CWD), shortID(threadID)))
		return
	}
	s.send(ctx, message.Address, fmt.Sprintf("已绑定到 %s（%s，%s）。", displayTitle(thread.Title), projectName(thread.CWD), shortID(threadID)))
}

func (s *Service) resolveThread(ctx context.Context, address channels.ChannelAddress, selector string) (string, string) {
	selector = strings.TrimSpace(selector)
	if number, err := strconv.Atoi(selector); err == nil {
		s.mu.Lock()
		selection, ok := s.selections[addressKey(address)]
		s.mu.Unlock()
		if !ok || time.Now().After(selection.Expires) {
			return "", "序号列表不存在或已过期，请先发送 /threads。"
		}
		if number < 1 || number > len(selection.Threads) {
			return "", "序号超出当前 /threads 列表范围。"
		}
		return selection.Threads[number-1].ThreadID, ""
	}
	if detail, err := s.control.ReadThread(ctx, selector, false); err == nil && detail.ThreadID == selector {
		return selector, ""
	}
	result, err := s.control.ListThreads(ctx, 100, "")
	if err != nil {
		return "", "无法验证 Thread ID，请确认 Codex 已连接。"
	}
	matches := make([]string, 0, 2)
	for _, thread := range result.Threads {
		if strings.HasPrefix(thread.ThreadID, selector) {
			matches = append(matches, thread.ThreadID)
		}
	}
	if len(matches) == 1 {
		return matches[0], ""
	}
	if len(matches) > 1 {
		short := make([]string, 0, len(matches))
		for _, match := range matches {
			short = append(short, shortID(match))
		}
		sort.Strings(short)
		return "", "该前缀有多个匹配，未进行猜测：" + strings.Join(short, "、")
	}
	return "", "指定的 Thread ID 或前缀不存在。"
}

func (s *Service) unbind(ctx context.Context, message channels.InboundMessage) {
	s.clearAddress(message.Address)
	deleted, err := s.bindings.DeleteAddress("qq", message.Address.AccountID, qqConversationType(message.Address.ConversationType), message.Address.ChatID, "")
	if errors.Is(err, bindings.ErrNotFound) {
		s.send(ctx, message.Address, "当前 QQ 会话尚未绑定。")
		return
	}
	if err != nil {
		s.send(ctx, message.Address, "解除绑定失败。")
		return
	}
	s.refreshBindingCount(deleted.AccountID)
	s.broker.Publish(events.BindingDeleted, safeBindingPayload(deleted))
	s.send(ctx, message.Address, "已解除绑定，并清除该 QQ 会话的投递和等待状态；Thread 本身未被删除。")
}

func (s *Service) current(ctx context.Context, message channels.InboundMessage) {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.send(ctx, message.Address, "当前 QQ 会话尚未绑定。使用 /threads 或 /bind。")
		return
	}
	thread, err := control.ReadThreadHistory(ctx, s.control, binding.ThreadID, 1)
	if err != nil || thread.ThreadID == "" {
		s.send(ctx, message.Address, "绑定的 Thread "+shortID(binding.ThreadID)+" 已不存在，请解除绑定或重新绑定。")
		return
	}
	s.send(ctx, message.Address, fmt.Sprintf("当前绑定\n标题：%s\n项目：%s\nThread：%s\n更新：%s\n状态：%s\n最近 Turn 结果：%s", displayTitle(thread.Title), projectName(thread.CWD), shortID(thread.ThreadID), displayTime(thread.UpdatedAt), thread.Runtime.State, latestTurnResult(thread)))
}

func (s *Service) status(ctx context.Context, message channels.InboundMessage) {
	bridge := s.runtime.Status()
	qqStatus := s.transport.QQStatus()
	qqState := strings.TrimSpace(qqStatus.ConnectionState)
	if qqState == "" {
		qqState = "stopped"
	}
	lines := []string{"Bridge：运行中", "QQ Adapter：" + qqState}
	if bridge.AppServerRunning {
		lines = append(lines, "Codex App Server：已连接")
	} else {
		lines = append(lines, "Codex App Server：不可用")
	}
	binding, ok := s.findBinding(message.Address)
	if !ok {
		lines = append(lines, "绑定：无")
		s.send(ctx, message.Address, strings.Join(lines, "\n"))
		return
	}
	thread, err := control.ReadThreadActivity(ctx, s.control, binding.ThreadID)
	if err != nil || thread.ThreadID == "" {
		lines = append(lines, "绑定："+shortID(binding.ThreadID), "Thread：不可用或已删除")
	} else {
		lastResult := latestTurnResult(thread)
		lines = append(lines, "Thread："+displayTitle(thread.Title)+" · "+shortID(thread.ThreadID), "项目："+projectName(thread.CWD), "状态："+thread.Runtime.State, "最近 Turn 结果："+lastResult, fmt.Sprintf("等待用户输入：%t", thread.Runtime.PendingInteractionCount > 0))
		if lastResult == bridgeruntime.StatePersisted {
			lines = append(lines, "消息已写入 Codex 本地会话；Codex Desktop 可能需要完全重启后显示外部写入内容。")
		}
	}
	s.send(ctx, message.Address, strings.Join(lines, "\n"))
}

func (s *Service) stopTurn(ctx context.Context, message channels.InboundMessage) {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.send(ctx, message.Address, "当前 QQ 会话尚未绑定。")
		return
	}
	state := s.runtime.RuntimeState(binding.ThreadID)
	s.mu.Lock()
	route := s.routes[state.TurnID]
	owned := route != nil && sameAddress(route.Address, message.Address) && route.UserID == message.UserID && route.ThreadID == binding.ThreadID
	s.mu.Unlock()
	if !owned || !state.CanInterrupt || state.Origin != "local" || state.TurnID == "" || state.TurnID != route.TurnID {
		s.send(ctx, message.Address, "没有可由此 QQ 会话和当前用户停止的任务。")
		return
	}
	if _, err := s.runtime.InterruptTurn(ctx, binding.ThreadID, state.TurnID); err != nil {
		s.send(ctx, message.Address, "停止请求失败；任务可能已完成或已失去控制权。")
		return
	}
	s.clearTurnInput(state.TurnID)
	s.send(ctx, message.Address, "已请求停止任务。")
}

func (s *Service) startTurn(ctx context.Context, message channels.InboundMessage, text string) {
	if s.hasFlowForAddress(message.Address) {
		s.reject(ctx, message, "Codex 正在等待发起用户回答，暂不能提交新任务。", "waiting-input")
		return
	}
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.reject(ctx, message, "当前 QQ 会话尚未绑定。请使用 /threads 或 /bind。", "unbound")
		return
	}
	thread, err := s.control.ReadThread(ctx, binding.ThreadID, false)
	if err != nil || thread.ThreadID == "" || thread.ThreadID != binding.ThreadID {
		s.reject(ctx, message, "绑定的 Codex Thread 已不存在，请重新绑定。", "missing-thread")
		return
	}
	if thread.Archived != nil && *thread.Archived {
		s.reject(ctx, message, "绑定的 Codex Thread 已归档，请重新绑定。", "archived")
		return
	}
	state := s.runtime.RuntimeState(binding.ThreadID)
	if !state.CanSend || state.PendingInteractionCount > 0 {
		s.reject(ctx, message, "该 Thread 当前忙碌或正在等待交互，暂不能提交新任务。", "busy")
		return
	}
	accepted, err := s.runtime.StartTurn(ctx, binding.ThreadID, control.StartTurnRequest{Text: text})
	if err != nil {
		s.reject(ctx, message, "无法启动任务；Thread 可能正忙、不可用或已归档。", "start-failed")
		return
	}
	route := &turnRoute{Address: message.Address, UserID: message.UserID, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID, LastProgress: make(map[string]time.Time)}
	s.mu.Lock()
	s.routes[accepted.TurnID] = route
	s.mu.Unlock()
	s.send(ctx, message.Address, "任务已提交。")
	s.publishMessage(events.QQMessageRouted, message, "turn-started")
	for _, interaction := range s.runtime.ListInteractions("pending") {
		if interaction.ThreadID == accepted.ThreadID && interaction.TurnID == accepted.TurnID {
			s.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID, Payload: map[string]any{"interaction": interaction}})
		}
	}
}

func (s *Service) cancelInteraction(ctx context.Context, message channels.InboundMessage) {
	key := inputKey(message.Address, message.UserID)
	s.mu.Lock()
	interactionID := s.flowByInput[key]
	flow := s.flows[interactionID]
	s.mu.Unlock()
	if flow == nil || time.Now().After(flow.Expires) || !sameAddress(flow.Address, message.Address) || flow.UserID != message.UserID {
		if interactionID != "" {
			s.clearInteraction(interactionID)
		}
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return
	}
	item, ok := s.runtime.GetInteraction(interactionID)
	if !ok || item.Kind != interactions.KindUserInput || item.ThreadID != flow.ThreadID || item.TurnID != flow.TurnID || item.Status != "pending" {
		s.clearInteraction(interactionID)
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return
	}
	if _, err := s.runtime.RespondInteraction(ctx, interactionID, interactions.ResponseRequest{Action: "cancel"}); err != nil {
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return
	}
	s.clearInteraction(interactionID)
	s.send(ctx, message.Address, "已取消本次 QQ 用户输入。")
}

func (s *Service) answerInteraction(ctx context.Context, message channels.InboundMessage, text string) bool {
	key := inputKey(message.Address, message.UserID)
	s.mu.Lock()
	interactionID := s.flowByInput[key]
	flow := s.flows[interactionID]
	s.mu.Unlock()
	if flow == nil {
		return false
	}
	if time.Now().After(flow.Expires) || !sameAddress(flow.Address, message.Address) || flow.UserID != message.UserID {
		s.clearInteraction(interactionID)
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return true
	}
	item, ok := s.runtime.GetInteraction(interactionID)
	if !ok || item.Status != "pending" || item.Kind != interactions.KindUserInput || item.ThreadID != flow.ThreadID || item.TurnID != flow.TurnID {
		s.clearInteraction(interactionID)
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return true
	}
	s.mu.Lock()
	if flow.Index >= len(flow.Questions) {
		s.mu.Unlock()
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return true
	}
	question := flow.Questions[flow.Index]
	s.mu.Unlock()
	answers, valid := parseQuestionAnswer(question, text)
	if !valid {
		s.send(ctx, message.Address, "回答格式无效，请按当前问题提示重新回答；该问题尚未推进。")
		return true
	}
	s.mu.Lock()
	current := s.flows[interactionID]
	if current == nil || current.Index >= len(current.Questions) || current.Questions[current.Index].ID != question.ID {
		s.mu.Unlock()
		s.send(ctx, message.Address, "该问题已经处理或已经过期。")
		return true
	}
	current.Answers[question.ID] = append([]string(nil), answers...)
	current.Index++
	complete := current.Index == len(current.Questions)
	requestAnswers := cloneAnswers(current.Answers)
	s.mu.Unlock()
	if !complete {
		s.presentQuestion(ctx, interactionID)
		return true
	}
	if _, err := s.runtime.RespondInteraction(ctx, interactionID, interactions.ResponseRequest{Action: "submit", Answers: requestAnswers}); err != nil {
		s.mu.Lock()
		if retry := s.flows[interactionID]; retry != nil && retry.Index > 0 {
			retry.Index--
			delete(retry.Answers, question.ID)
		}
		s.mu.Unlock()
		s.send(ctx, message.Address, "回答提交失败；问题可能已过期或已在其他位置处理。")
		return true
	}
	s.clearInteraction(interactionID)
	s.send(ctx, message.Address, "回答已提交给 Codex。")
	return true
}

func parseQuestionAnswer(question interactions.Question, text string) ([]string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, !question.Required
	}
	switch strings.ToLower(question.Type) {
	case "single", "single-choice":
		index, err := strconv.Atoi(text)
		if err != nil || index < 1 || index > len(question.Options) {
			return nil, false
		}
		return []string{question.Options[index-1].Value}, true
	case "multi", "multiple-choice":
		normalized := strings.NewReplacer("，", ",", "、", ",").Replace(text)
		parts := strings.Split(normalized, ",")
		result := make([]string, 0, len(parts))
		seen := map[int]bool{}
		for _, part := range parts {
			index, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || index < 1 || index > len(question.Options) || seen[index] {
				return nil, false
			}
			seen[index] = true
			result = append(result, question.Options[index-1].Value)
		}
		if question.Required && len(result) == 0 {
			return nil, false
		}
		return result, true
	default:
		return []string{text}, true
	}
}

func (s *Service) eventLoop() {
	defer close(s.done)
	channel, unsubscribe := s.broker.Subscribe()
	defer unsubscribe()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.expireTransient()
		case event := <-channel:
			if qqRelevantEvent(event.EventType) {
				s.handleEvent(event)
			}
		}
	}
}

func qqRelevantEvent(eventType string) bool {
	switch eventType {
	case events.CodexDisconnected, events.InteractionRequested, events.InteractionResolved,
		events.TurnStatusChanged, events.ToolStarted, events.ToolUpdated, events.ToolCompleted,
		events.TurnPersistence, events.TurnCompleted, events.TurnFailed, events.TurnInterrupted:
		return true
	default:
		return false
	}
}

func (s *Service) handleEvent(event events.Event) {
	if event.EventType == events.CodexDisconnected {
		s.finishAllRoutes("Codex App Server 已断开，当前任务控制权已丢失；不会自动重试。")
		return
	}
	if event.EventType == events.InteractionRequested {
		s.handleInteractionEvent(event)
		return
	}
	if event.EventType == events.InteractionResolved {
		if id := interactionIDFromPayload(event.Payload); id != "" {
			s.clearInteraction(id)
		}
		return
	}
	s.mu.Lock()
	route := s.routes[event.TurnID]
	s.mu.Unlock()
	if route == nil || route.ThreadID != event.ThreadID {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	switch event.EventType {
	case events.TurnStatusChanged:
		state := s.runtime.RuntimeState(route.ThreadID)
		if s.transport.QQStatus().SendProgressUpdates && (state.State == bridgeruntime.StateRunning || state.State == bridgeruntime.StateAccepted) {
			s.progress(ctx, route, "running", "Codex 正在处理任务……")
		}
	case events.ToolStarted, events.ToolUpdated, events.ToolCompleted:
		if s.transport.QQStatus().SendProgressUpdates {
			s.progress(ctx, route, "tool", "Codex 正在执行工具步骤……")
		}
	case events.TurnPersistence:
		status, hasVerification := persistenceStatus(event.Payload)
		switch status {
		case bridgeruntime.StateCompletedUnverified:
			s.send(ctx, route.Address, "Codex 已完成响应，正在验证会话持久化……")
			if hasVerification {
				s.requestPersistenceVerification(route)
			}
		case bridgeruntime.StatePersistenceFailed:
			s.finishRoute(ctx, route, "Codex 已完成响应，但会话持久化验证失败。请在 WPF 中查看详情。")
		case bridgeruntime.StateThreadMismatch:
			s.finishRoute(ctx, route, "持久化验证发现 Thread 不匹配，未投递最终回复。")
		}
	case events.TurnCompleted:
		status, _ := persistenceStatus(event.Payload)
		if status == bridgeruntime.StatePersisted {
			s.deliverCompleted(ctx, route)
		}
	case events.TurnFailed:
		status, _ := event.Payload["status"].(string)
		if status == bridgeruntime.StatePersistenceFailed {
			s.finishRoute(ctx, route, "Codex 响应完成，但持久化验证失败。")
		} else if status == bridgeruntime.StateThreadMismatch {
			s.finishRoute(ctx, route, "持久化验证发现 Thread 不匹配，未投递最终回复。")
		} else {
			s.finishRoute(ctx, route, "Codex 任务失败，请在 WPF 中查看详情。")
		}
	case events.TurnInterrupted:
		s.finishRoute(ctx, route, "Codex 任务已停止。")
	}
}

func (s *Service) progress(ctx context.Context, route *turnRoute, category, text string) {
	s.mu.Lock()
	current := s.routes[route.TurnID]
	if current != route || route.Revoked || time.Since(route.LastProgress[category]) < 5*time.Second {
		s.mu.Unlock()
		return
	}
	route.LastProgress[category] = time.Now()
	s.mu.Unlock()
	s.send(ctx, route.Address, text)
}

func (s *Service) requestPersistenceVerification(route *turnRoute) {
	s.mu.Lock()
	current := s.routes[route.TurnID]
	if current != route || route.VerificationRequested {
		s.mu.Unlock()
		return
	}
	route.VerificationRequested = true
	s.mu.Unlock()
	go func(threadID, turnID string) {
		ctx, cancel := context.WithTimeout(s.ctx, 45*time.Second)
		defer cancel()
		verification, err := s.runtime.VerifyThreadPersistence(ctx, threadID)
		if err == nil || verification.ThreadID != "" {
			return
		}
		s.mu.Lock()
		active := s.routes[turnID]
		s.mu.Unlock()
		if active != nil {
			finishCtx, finishCancel := context.WithTimeout(s.ctx, 10*time.Second)
			defer finishCancel()
			s.finishRoute(finishCtx, active, "无法启动独立持久化验证，未投递最终回复。")
		}
	}(route.ThreadID, route.TurnID)
}

func (s *Service) handleInteractionEvent(event events.Event) {
	s.mu.Lock()
	route := s.routes[event.TurnID]
	s.mu.Unlock()
	if route == nil || route.ThreadID != event.ThreadID {
		return
	}
	id := interactionIDFromPayload(event.Payload)
	interaction, ok := s.runtime.GetInteraction(id)
	if !ok || interaction.ThreadID != route.ThreadID || interaction.TurnID != route.TurnID || interaction.Status != "pending" {
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	if interaction.Kind != interactions.KindUserInput {
		s.send(ctx, route.Address, "Codex 正在请求审批。为保证安全，请在 WPF 中允许或拒绝；QQ 不会自动处理审批。")
		return
	}
	if len(interaction.Questions) == 0 {
		s.send(ctx, route.Address, "Codex 请求了用户输入，但该问题需要在 WPF 中处理。")
		return
	}
	expires, err := time.Parse(time.RFC3339Nano, interaction.ExpiresAt)
	if err != nil || expires.Before(time.Now()) {
		expires = time.Now().Add(flowTTL)
	}
	flow := &interactionFlow{
		Expires: expires, Address: route.Address, UserID: route.UserID, ThreadID: route.ThreadID, TurnID: route.TurnID,
		InteractionID: id, Questions: append([]interactions.Question(nil), interaction.Questions...), Answers: make(map[string][]string),
	}
	s.mu.Lock()
	if _, exists := s.flows[id]; exists {
		s.mu.Unlock()
		return
	}
	s.flows[id] = flow
	s.flowByInput[inputKey(route.Address, route.UserID)] = id
	s.mu.Unlock()
	s.presentQuestion(ctx, id)
}

func (s *Service) presentQuestion(ctx context.Context, interactionID string) {
	s.mu.Lock()
	flow := s.flows[interactionID]
	if flow == nil || flow.Index >= len(flow.Questions) || time.Now().After(flow.Expires) {
		s.mu.Unlock()
		if flow != nil {
			s.clearInteraction(interactionID)
		}
		return
	}
	question := flow.Questions[flow.Index]
	address := flow.Address
	position, total := flow.Index+1, len(flow.Questions)
	s.mu.Unlock()
	var prompt strings.Builder
	fmt.Fprintf(&prompt, "Codex 需要你的输入（%d/%d）\n", position, total)
	if strings.TrimSpace(question.Header) != "" {
		prompt.WriteString(strings.TrimSpace(question.Header) + "\n")
	}
	prompt.WriteString(strings.TrimSpace(question.Text))
	for index, option := range question.Options {
		fmt.Fprintf(&prompt, "\n%d. %s", index+1, option.Label)
		if strings.TrimSpace(option.Description) != "" {
			prompt.WriteString(" — " + strings.TrimSpace(option.Description))
		}
	}
	switch strings.ToLower(question.Type) {
	case "single", "single-choice":
		prompt.WriteString("\n请回复一个序号。")
	case "multi", "multiple-choice":
		prompt.WriteString("\n请回复一个或多个序号，可用中文或英文逗号分隔。")
	default:
		prompt.WriteString("\n请直接回复文本。")
	}
	s.send(ctx, address, prompt.String())
}

func (s *Service) deliverCompleted(ctx context.Context, route *turnRoute) {
	if !s.routeActive(route) {
		return
	}
	binding, ok := s.findBinding(route.Address)
	if !ok || binding.ThreadID != route.ThreadID {
		s.removeRoute(route.TurnID)
		return
	}
	thread, err := control.ReadThreadHistory(ctx, s.control, route.ThreadID, 1)
	if err != nil || thread.ThreadID != route.ThreadID {
		s.finishRoute(ctx, route, "任务已持久化，但无法读取正式回复。请在 WPF 中查看。")
		return
	}
	text := finalAssistantText(thread, route.TurnID)
	if text == "" {
		s.finishRoute(ctx, route, "任务已持久化，但未读取到该 Turn 的正式 assistant 回复。请在 WPF 中查看。")
		return
	}
	if !s.send(ctx, route.Address, "任务完成\n"+text) {
		s.removeRoute(route.TurnID)
		return
	}
	s.removeRoute(route.TurnID)
}

func (s *Service) finishRoute(ctx context.Context, route *turnRoute, text string) {
	if !s.routeActive(route) {
		return
	}
	s.send(ctx, route.Address, text)
	s.removeRoute(route.TurnID)
}

func (s *Service) finishAllRoutes(message string) {
	s.mu.Lock()
	routes := make([]*turnRoute, 0, len(s.routes))
	for _, route := range s.routes {
		routes = append(routes, route)
	}
	s.mu.Unlock()
	for _, route := range routes {
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		s.finishRoute(ctx, route, message)
		cancel()
	}
}

func (s *Service) handleAdapterEvent(event AdapterEvent) {
	eventType := ""
	switch event.Kind {
	case "connecting":
		eventType = events.QQConnecting
	case "connected":
		eventType = events.QQConnected
		status := s.transport.QQStatus()
		s.onSelfID(status.SelfID)
		s.refreshBindingCount(status.SelfID)
	case "disconnected":
		eventType = events.QQDisconnected
	case "reconnecting":
		eventType = events.QQReconnecting
	case "stopped":
		eventType = events.QQStopped
		s.clearTransient()
	case "heartbeat":
		eventType = events.QQHeartbeat
	case "message_received":
		eventType = events.QQMessageReceived
	case "rejected":
		eventType = events.QQMessageRejected
	case "action_failed":
		eventType = events.QQActionFailed
	case "error":
		eventType = events.QQError
	}
	if eventType == "" {
		return
	}
	payload := map[string]any{"channelType": "qq"}
	if eventType != events.QQMessageReceived && eventType != events.QQMessageRejected {
		payload["status"] = s.safeEventStatus()
	}
	if event.Code != "" {
		payload["code"] = event.Code
	}
	if event.Reason != "" {
		payload["reason"] = event.Reason
	}
	if event.ConversationType != "" {
		payload["conversationType"] = event.ConversationType
	}
	if event.ChatID != "" {
		payload["chat"] = maskID(event.ChatID)
	}
	if event.UserID != "" {
		payload["user"] = maskID(event.UserID)
	}
	if event.MessageID != "" {
		payload["messageId"] = shortID(event.MessageID)
	}
	s.broker.Publish(eventType, payload)
	switch eventType {
	case events.QQConnected:
		s.publishChannel(events.ChannelConnected, event.Code)
	case events.QQDisconnected, events.QQStopped:
		s.publishChannel(events.ChannelDisconnected, event.Code)
	case events.QQError:
		s.publishChannel(events.ChannelError, event.Code)
	case events.QQMessageReceived:
		s.broker.Publish(events.MessageReceived, payload)
	case events.QQMessageRejected:
		s.broker.Publish(events.MessageRejected, payload)
	}
	switch eventType {
	case events.QQConnecting, events.QQConnected, events.QQDisconnected, events.QQReconnecting,
		events.QQStopped, events.QQActionFailed, events.QQError:
		s.publishChannel(events.ChannelStatusChanged, event.Code)
	}
}

func (s *Service) publishChannel(eventType, code string) {
	payload := map[string]any{"channelType": "qq", "status": s.safeEventStatus()}
	if code != "" {
		payload["code"] = code
	}
	s.broker.Publish(eventType, payload)
}

func (s *Service) safeEventStatus() map[string]any {
	status := s.transport.QQStatus()
	return map[string]any{
		"channelType": "qq", "configured": status.Configured, "running": status.Running,
		"connected": status.Connected, "connectionState": status.ConnectionState,
		"lastConnectedAt": status.LastConnectedAt, "lastHeartbeatAt": status.LastHeartbeatAt,
		"lastEventAt": status.LastEventAt, "reconnectCount": status.ReconnectCount,
		"lastErrorCode": status.LastErrorCode, "allowedPrivateUserCount": status.AllowedPrivateUserCount,
		"allowedGroupCount": status.AllowedGroupCount, "allowedGroupUserCount": status.AllowedGroupUserCount,
		"bindingCount": status.BindingCount,
	}
}

func (s *Service) send(ctx context.Context, address channels.ChannelAddress, text string) bool {
	parts := splitQQMessage(text, qqRuneLimit)
	for index, part := range parts {
		result, err := s.transport.SendMessage(ctx, channels.OutboundMessage{Address: address, Text: part})
		if err != nil {
			if s.logger != nil {
				s.logger.Printf("QQ send failed part=%d parts=%d runes=%d conversation=%s chat=%s", index+1, len(parts), utf8.RuneCountInString(part), qqConversationType(address.ConversationType), maskID(address.ChatID))
			}
			return false
		}
		payload := map[string]any{"channelType": "qq", "conversationType": qqConversationType(address.ConversationType), "chat": maskID(address.ChatID), "messageId": shortID(result.MessageID), "length": utf8.RuneCountInString(part), "part": index + 1, "parts": len(parts)}
		s.broker.Publish(events.MessageSent, payload)
		s.broker.Publish(events.QQMessageSent, payload)
	}
	return true
}

func splitQQMessage(text string, limit int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if limit < 32 {
		limit = qqRuneLimit
	}
	plain := splitRunes(text, limit)
	if len(plain) == 1 {
		return plain
	}
	contentLimit := limit - 16
	plain = splitRunes(text, contentLimit)
	result := make([]string, len(plain))
	for index, part := range plain {
		result[index] = fmt.Sprintf("[%d/%d] %s", index+1, len(plain), part)
	}
	return result
}

func splitRunes(text string, limit int) []string {
	runes := []rune(strings.TrimSpace(text))
	result := []string{}
	for len(runes) > limit {
		cut := limit
		for index := limit; index > limit/2; index-- {
			if runes[index-1] == '\n' {
				cut = index
				break
			}
		}
		if cut == limit {
			for index := limit; index > limit/2; index-- {
				if runes[index-1] == ' ' {
					cut = index
					break
				}
			}
		}
		result = append(result, strings.TrimSpace(string(runes[:cut])))
		runes = runes[cut:]
	}
	if tail := strings.TrimSpace(string(runes)); tail != "" {
		result = append(result, tail)
	}
	return result
}

func (s *Service) reject(ctx context.Context, message channels.InboundMessage, text, reason string) {
	s.send(ctx, message.Address, text)
	s.publishMessage(events.QQMessageRejected, message, reason)
}

func (s *Service) publishMessage(eventType string, message channels.InboundMessage, result string) {
	threadID := ""
	if binding, ok := s.findBinding(message.Address); ok {
		threadID = shortID(binding.ThreadID)
	}
	payload := map[string]any{
		"channelType": "qq", "conversationType": qqConversationType(message.Address.ConversationType),
		"chat": maskID(message.Address.ChatID), "user": maskID(message.UserID), "messageId": shortID(message.MessageID),
		"threadId": threadID, "length": utf8.RuneCountInString(message.Text), "routeResult": result,
	}
	s.broker.Publish(eventType, payload)
	generic := events.MessageRejected
	if eventType == events.QQMessageRouted {
		generic = events.MessageRouted
	} else if eventType == events.QQMessageReceived {
		generic = events.MessageReceived
	}
	s.broker.Publish(generic, payload)
}

func (s *Service) findBinding(address channels.ChannelAddress) (bindings.Binding, bool) {
	binding, ok := s.bindings.FindAddress("qq", address.AccountID, qqConversationType(address.ConversationType), address.ChatID, "")
	return binding, ok && binding.Enabled
}

func (s *Service) refreshBindingCount(accountID string) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		s.transport.SetBindingCount(0)
		return
	}
	s.transport.SetBindingCount(s.bindings.CountChannelAccount("qq", accountID))
}

func (s *Service) onSelfID(selfID string) {
	selfID = strings.TrimSpace(selfID)
	s.mu.Lock()
	changed := s.selfID != "" && selfID != "" && s.selfID != selfID
	s.selfID = selfID
	s.mu.Unlock()
	if changed {
		s.clearTransient()
	}
}

func (s *Service) routeActive(route *turnRoute) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.routes[route.TurnID] == route && !route.Revoked
}

func (s *Service) removeRoute(turnID string) {
	s.mu.Lock()
	delete(s.routes, turnID)
	for id, flow := range s.flows {
		if flow.TurnID == turnID {
			delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func (s *Service) clearTurnInput(turnID string) {
	s.mu.Lock()
	for id, flow := range s.flows {
		if flow.TurnID == turnID {
			delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func (s *Service) clearAddress(address channels.ChannelAddress) {
	s.mu.Lock()
	for turnID, route := range s.routes {
		if sameAddress(route.Address, address) {
			route.Revoked = true
			delete(s.routes, turnID)
		}
	}
	for id, flow := range s.flows {
		if sameAddress(flow.Address, address) {
			delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
			delete(s.flows, id)
		}
	}
	delete(s.selections, addressKey(address))
	s.mu.Unlock()
}

func (s *Service) clearInteraction(interactionID string) {
	s.mu.Lock()
	if flow := s.flows[interactionID]; flow != nil {
		delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
	}
	delete(s.flows, interactionID)
	s.mu.Unlock()
}

func (s *Service) clearTransient() {
	s.mu.Lock()
	s.routes = make(map[string]*turnRoute)
	s.selections = make(map[string]threadSelection)
	s.flows = make(map[string]*interactionFlow)
	s.flowByInput = make(map[string]string)
	s.mu.Unlock()
}

func (s *Service) expireTransient() {
	now := time.Now()
	s.mu.Lock()
	for key, selection := range s.selections {
		if now.After(selection.Expires) {
			delete(s.selections, key)
		}
	}
	for id, flow := range s.flows {
		if now.After(flow.Expires) {
			delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func (s *Service) hasFlowForAddress(address channels.ChannelAddress) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, flow := range s.flows {
		if sameAddress(flow.Address, address) && time.Now().Before(flow.Expires) {
			return true
		}
	}
	return false
}

func finalAssistantText(thread control.ThreadDetail, turnID string) string {
	for _, turn := range thread.Turns {
		if turn.TurnID != turnID {
			continue
		}
		for index := len(turn.Items) - 1; index >= 0; index-- {
			item := turn.Items[index]
			if (strings.EqualFold(item.Role, "assistant") || strings.EqualFold(item.Type, "agentMessage")) && strings.TrimSpace(item.Text) != "" {
				return strings.TrimSpace(item.Text)
			}
		}
	}
	return ""
}

func latestTurnResult(thread control.ThreadDetail) string {
	if len(thread.Turns) == 0 {
		return "无"
	}
	latest := thread.Turns[len(thread.Turns)-1]
	latestAt := turnTimestamp(latest)
	for _, candidate := range thread.Turns[:len(thread.Turns)-1] {
		if candidateAt := turnTimestamp(candidate); candidateAt.After(latestAt) {
			latest = candidate
			latestAt = candidateAt
		}
	}
	result := strings.TrimSpace(latest.Status)
	if verification := thread.Runtime.Persistence; verification != nil &&
		verification.ExpectedTurnID == latest.TurnID && strings.TrimSpace(verification.Status) != "" {
		result = strings.TrimSpace(verification.Status)
	}
	if result == "" {
		return "unknown"
	}
	return result
}

func turnTimestamp(turn control.Turn) time.Time {
	for _, value := range []string{turn.UpdatedAt, turn.CreatedAt} {
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func persistenceStatus(payload map[string]any) (string, bool) {
	if verification, ok := payload["verification"].(control.PersistenceVerification); ok {
		return verification.Status, true
	}
	if verification, ok := payload["verification"].(*control.PersistenceVerification); ok && verification != nil {
		return verification.Status, true
	}
	status, _ := payload["status"].(string)
	return status, false
}

func interactionIDFromPayload(payload map[string]any) string {
	if raw, ok := payload["interaction"].(interactions.PendingInteraction); ok {
		return raw.ID
	}
	if raw, ok := payload["interaction"].(map[string]any); ok {
		value, _ := raw["id"].(string)
		return strings.TrimSpace(value)
	}
	return ""
}

func cloneAnswers(source map[string][]string) map[string][]string {
	result := make(map[string][]string, len(source))
	for id, values := range source {
		result[id] = append([]string(nil), values...)
	}
	return result
}

func safeBindingPayload(binding bindings.Binding) map[string]any {
	return map[string]any{
		"bindingId": binding.ID, "channelType": "qq", "conversationType": binding.ConversationType,
		"account": maskID(binding.AccountID), "chat": maskID(binding.ChatID), "threadId": shortID(binding.ThreadID),
	}
}

func addressKey(address channels.ChannelAddress) string {
	return strings.Join([]string{"qq", address.AccountID, qqConversationType(address.ConversationType), address.ChatID}, "\x00")
}

func inputKey(address channels.ChannelAddress, userID string) string {
	return addressKey(address) + "\x00" + strings.TrimSpace(userID)
}

func sameAddress(left, right channels.ChannelAddress) bool {
	return strings.EqualFold(left.ChannelType, right.ChannelType) && left.AccountID == right.AccountID && qqConversationType(left.ConversationType) == qqConversationType(right.ConversationType) && left.ChatID == right.ChatID
}

func qqConversationType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func displayTitle(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "未命名 Thread"
	}
	return truncateRunes(value, 60)
}

func projectName(cwd string) string {
	value := filepath.Base(filepath.Clean(strings.TrimSpace(cwd)))
	if value == "." || value == string(filepath.Separator) || value == "" {
		return "项目"
	}
	return truncateRunes(value, 30)
}

func displayTime(value string) string {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
		return parsed.Local().Format("2006-01-02 15:04")
	}
	return "未知"
}

func truncateRunes(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit-1]) + "…"
}

func shortID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 10 {
		return value
	}
	return value[:8] + "…"
}

func maskID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 4 {
		return "***"
	}
	return "***" + value[len(value)-4:]
}

func qqErrorCode(err error) string {
	return ClassifyError(err)
}
