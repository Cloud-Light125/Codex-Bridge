package qqbot

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
	"cloudlight.dev/codexbridge/bridge-daemon/internal/commandregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/control"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/numberprefix"
	bridgequery "cloudlight.dev/codexbridge/bridge-daemon/internal/query"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/taskcenter"
)

const (
	selectionTTL   = 5 * time.Minute
	flowTTL        = 5 * time.Minute
	qqbotRuneLimit = officialTextLimit
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
}

// BindingPreparer lets the profile manager resolve the logical Profile and
// validate the backend route before this transport writes a binding.
type BindingPreparer func(bindings.CreateRequest) (bindings.CreateRequest, error)

type serviceAdapter interface {
	Configure(ConfigureRequest) (AdapterStatus, error)
	QQBotStatus() AdapterStatus
	SetBindingCount(int)
	Start(context.Context) error
	Stop(context.Context) error
	ClearSecret(context.Context) error
	Test(context.Context, TestRequest) TestResult
	SendMessage(context.Context, channels.OutboundMessage) (channels.OutboundResult, error)
}

type threadSelection struct {
	Expires time.Time
	Threads []control.ThreadSummary
}

type turnRoute struct {
	Address    channels.ChannelAddress
	UserID     string
	Backend    string
	SessionKey string
	ThreadID   string
	TurnID     string
	TaskNumber int
	Revoked    bool
}

type interactionFlow struct {
	Expires       time.Time
	Address       channels.ChannelAddress
	UserID        string
	ThreadID      string
	TurnID        string
	TaskNumber    int
	InteractionID string
	Questions     []interactions.Question
	Index         int
	Answers       map[string][]string
}

type Service struct {
	adapter         *Adapter
	transport       serviceAdapter
	control         Control
	runtime         Runtime
	bindings        *bindings.Repository
	broker          *events.Broker
	logger          *bridgelog.SafeLogger
	registry        any
	commands        *commandregistry.Registry
	queries         *bridgequery.Service
	openclaw        conversation.IConversationBackend
	taskService     *taskcenter.Service
	backendResolver func(string) string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu                  sync.Mutex
	routes              map[string]*turnRoute
	selections          map[string]threadSelection
	flows               map[string]*interactionFlow
	flowByInput         map[string]string
	interactionNotified map[string]bool
	appID               string
	channelProfileID    string
	bindingPreparer     BindingPreparer
	reconfiguring       bool
	activeHandlers      int
}

func NewService(controlService Control, runtime Runtime, repository *bindings.Repository, broker *events.Broker, logger *bridgelog.SafeLogger, registries ...any) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	var registry any
	if len(registries) > 0 {
		registry = registries[0]
	}
	service := &Service{
		control: controlService, runtime: runtime, bindings: repository, broker: broker, logger: logger, registry: registry,
		commands: commandregistry.NewInMemory(),
		ctx:      ctx, cancel: cancel, done: make(chan struct{}), routes: make(map[string]*turnRoute),
		selections: make(map[string]threadSelection), flows: make(map[string]*interactionFlow), flowByInput: make(map[string]string), interactionNotified: make(map[string]bool),
	}
	service.adapter = NewAdapter(service.HandleMessage)
	service.transport = service.adapter
	service.adapter.SetEventHandler(service.handleAdapterEvent)
	if logger != nil {
		service.adapter.SetDiagnosticLogger(logger.Printf)
	}
	go service.eventLoop()
	return service
}

func (s *Service) Adapter() *Adapter { return s.adapter }

func (s *Service) SetChannelProfileID(profileID string) {
	s.mu.Lock()
	s.channelProfileID = strings.TrimSpace(profileID)
	s.mu.Unlock()
}

func (s *Service) SetBindingPreparer(preparer BindingPreparer) {
	s.mu.Lock()
	s.bindingPreparer = preparer
	s.mu.Unlock()
}

// SetBackendResolver supplies the profile manager's default backend for
// unbound conversations. Existing bindings remain authoritative.
func (s *Service) SetBackendResolver(resolver func(string) string) {
	s.mu.Lock()
	s.backendResolver = resolver
	s.mu.Unlock()
}

func (s *Service) prepareBinding(request bindings.CreateRequest) (bindings.CreateRequest, error) {
	s.mu.Lock()
	preparer := s.bindingPreparer
	s.mu.Unlock()
	if preparer == nil {
		return request, nil
	}
	return preparer(request)
}

func (s *Service) channelProfile() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.channelProfileID
}

func (s *Service) SetCommandRegistry(commands *commandregistry.Registry) {
	if commands == nil {
		return
	}
	s.mu.Lock()
	s.commands = commands
	s.queries = bridgequery.New(s.control, s.runtime, s.registry, commands)
	s.queries.SetOpenClawBackend(s.openclaw)
	s.mu.Unlock()
}

func (s *Service) SetOpenClawBackend(backend conversation.IConversationBackend) {
	s.mu.Lock()
	s.openclaw = backend
	if s.queries != nil {
		s.queries.SetOpenClawBackend(backend)
	}
	s.mu.Unlock()
}

func (s *Service) SetTaskService(service *taskcenter.Service) {
	s.mu.Lock()
	s.taskService = service
	s.mu.Unlock()
}

func (s *Service) Configure(request ConfigureRequest) (AdapterStatus, error) {
	s.mu.Lock()
	if s.reconfiguring || len(s.routes) > 0 || s.activeHandlers > 0 {
		s.mu.Unlock()
		return AdapterStatus{}, errors.New("QQ Official Bot cannot be reconfigured while a QQ Turn or message handler is active")
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
	s.refreshBindingCount()
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
		s.publishChannel(events.ChannelError, qqbotErrorCode(err))
		return err
	}
	status := s.transport.QQBotStatus()
	s.onAppID(status.AppID)
	s.refreshBindingCount()
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

func (s *Service) DeleteSecret(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	if err := s.transport.ClearSecret(ctx); err != nil {
		return err
	}
	s.clearTransient()
	s.onAppID("")
	s.publishChannel(events.ChannelStatusChanged, "")
	return nil
}

func (s *Service) BindingCreated(binding bindings.Binding) {
	if !strings.EqualFold(binding.ChannelType, "qqbot") || !s.ownsBinding(binding) {
		return
	}
	s.refreshBindingCount()
}

func (s *Service) BindingDeleted(binding bindings.Binding) {
	if !strings.EqualFold(binding.ChannelType, "qqbot") || !s.ownsBinding(binding) {
		return
	}
	s.clearAddress(channels.ChannelAddress{
		ChannelType: "qqbot", ChannelProfileID: s.channelProfile(), AccountID: binding.AccountID, ConversationType: binding.ConversationType, ChatID: binding.ChatID,
	})
	s.refreshBindingCount()
}

func (s *Service) HandleMessage(parent context.Context, message channels.InboundMessage) {
	message.Address.ChannelProfileID = s.channelProfile()
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
	prefix := strings.TrimSpace(s.transport.QQBotStatus().CommandPrefix)
	if prefix != "" {
		if strings.EqualFold(text, prefix) {
			text = "/help"
		} else if len(text) > len(prefix) && strings.EqualFold(text[:len(prefix)], prefix) && (text[len(prefix)] == ' ' || text[len(prefix)] == '\t' || text[len(prefix)] == '\n') {
			text = strings.TrimSpace(text[len(prefix):])
		}
	}
	if s.handleNumbered(ctx, message, text) {
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

func (s *Service) handleNumbered(ctx context.Context, message channels.InboundMessage, text string) bool {
	store := conversationregistry.ForAny(s.registry)
	if store != nil {
		record, content, recognized, err := store.ParsePrefix(text)
		if !recognized {
			return false
		}
		if err != nil {
			s.send(ctx, message.Address, err.Error())
			return true
		}
		if strings.HasPrefix(content, "/") {
			s.handleTargetCommand(ctx, message, record, content)
			return true
		}
		if record.Backend == conversation.BackendOpenClaw {
			session, err := s.openClawSessionByNumber(ctx, record.Number)
			if err != nil {
				s.send(ctx, message.Address, "指定的 OpenClaw Session 不存在或当前不可用。")
				return true
			}
			if s.answerInteractionForThread(ctx, message, content, session.Key) {
				return true
			}
			s.startOpenClawTurnForKey(ctx, message, content, session.Key)
			return true
		}
		if record.Backend != conversation.BackendCodex {
			s.send(ctx, message.Address, "该编号的会话后端不可用。")
			return true
		}
		if s.answerInteractionForThread(ctx, message, content, record.TargetID) {
			return true
		}
		s.startTurnNumbered(ctx, message, content, record.TargetID)
		return true
	}
	// Compatibility for test doubles and embedders that have not supplied a
	// registry. The daemon always supplies the global registry, so this path is
	// never used for normal routing.
	if s.backendForMessage(message) == conversation.BackendOpenClaw {
		return s.handleOpenClawNumbered(ctx, message, text)
	}
	return false
}

func (s *Service) handleOpenClawNumbered(ctx context.Context, message channels.InboundMessage, text string) bool {
	prefix, recognized, err := numberprefix.Parse(text)
	if !recognized {
		return false
	}
	if err != nil {
		s.send(ctx, message.Address, err.Error())
		return true
	}
	session, err := s.openClawSessionByNumber(ctx, prefix.Number)
	if err != nil {
		if !prefix.Explicit {
			return false
		}
		s.send(ctx, message.Address, fmt.Sprintf("OpenClaw 会话编号 #%d 不存在。请先发送 /threads。", prefix.Number))
		return true
	}
	if prefix.Content == "" {
		s.send(ctx, message.Address, fmt.Sprintf("请在 #%d 后输入消息或命令。", prefix.Number))
		return true
	}
	if strings.HasPrefix(prefix.Content, "/") {
		s.handleOpenClawTargetCommand(ctx, message, session, prefix.Content)
		return true
	}
	if s.answerInteractionForThread(ctx, message, prefix.Content, session.Key) {
		return true
	}
	s.startOpenClawTurnForKey(ctx, message, prefix.Content, session.Key)
	return true
}

func (s *Service) openClawSessionByNumber(ctx context.Context, number int) (conversation.Session, error) {
	backend := s.openClawBackend()
	if backend == nil {
		return conversation.Session{}, errors.New("OpenClaw backend unavailable")
	}
	if store := conversationregistry.ForAny(s.registry); store != nil {
		record, ok := store.ByNumber(number)
		if !ok || record.Backend != conversationregistry.BackendOpenClaw {
			return conversation.Session{}, errors.New("OpenClaw session number not found")
		}
		detail, err := backend.ReadSession(ctx, record.TargetID)
		if err != nil || detail.Key == "" {
			if err == nil {
				err = errors.New("OpenClaw session not found")
			}
			return conversation.Session{}, err
		}
		detail.Number = record.Number
		detail.Backend = conversation.BackendOpenClaw
		return detail.Session, nil
	}
	if numbered, ok := backend.(conversation.NumberedSessionBackend); ok {
		return numbered.SessionByNumber(ctx, number)
	}
	sessions, err := backend.ListSessions(ctx, 200)
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

func (s *Service) handleOpenClawTargetCommand(ctx context.Context, message channels.InboundMessage, session conversation.Session, text string) {
	invocation, found := s.commandRegistry().Resolve(text)
	if !found {
		s.send(ctx, message.Address, fmt.Sprintf("#%d 不支持该指令。", session.Number))
		return
	}
	if !invocation.Definition.Enabled {
		s.send(ctx, message.Address, "指令 "+invocation.Definition.Name+" 当前已停用。")
		return
	}
	if !commandregistry.SupportsBackend(invocation.Definition.BackendCapability, conversation.BackendOpenClaw) {
		s.send(ctx, message.Address, "该指令不适用于 OpenClaw；它依赖 Codex 的专属运行状态。")
		return
	}
	selector := strconv.Itoa(session.Number)
	switch invocation.Definition.Action {
	case commandregistry.ActionThreadStop:
		s.stopOpenClawSession(ctx, message, session.Key)
	case commandregistry.ActionBridgeStatus, commandregistry.ActionThreadInfo:
		s.runQueryActionForBackend(ctx, message, conversation.BackendOpenClaw, session.Key, commandregistry.ActionThreadInfo, []string{selector})
	case commandregistry.ActionThreadHistory:
		args := append([]string{selector}, invocation.Arguments...)
		s.runQueryActionForBackend(ctx, message, conversation.BackendOpenClaw, session.Key, commandregistry.ActionThreadHistory, args)
	default:
		s.send(ctx, message.Address, fmt.Sprintf("指令 %s 不支持 #编号 会话上下文。", invocation.Definition.Name))
	}
}

func (s *Service) handleTargetCommand(ctx context.Context, message channels.InboundMessage, record conversationregistry.Record, text string) {
	if record.Backend == conversation.BackendOpenClaw {
		session, err := s.openClawSessionByNumber(ctx, record.Number)
		if err != nil {
			s.send(ctx, message.Address, "指定的 OpenClaw Session 不存在或当前不可用。")
			return
		}
		s.handleOpenClawTargetCommand(ctx, message, session, text)
		return
	}
	invocation, found := s.commandRegistry().Resolve(text)
	if !found {
		s.send(ctx, message.Address, fmt.Sprintf("#%d 不支持该指令。", record.Number))
		return
	}
	if !invocation.Definition.Enabled {
		s.send(ctx, message.Address, "指令 "+invocation.Definition.Name+" 当前已停用。")
		return
	}
	if !commandregistry.SupportsBackend(invocation.Definition.BackendCapability, record.Backend) {
		s.send(ctx, message.Address, "该指令不适用于 Codex。")
		return
	}
	switch invocation.Definition.Action {
	case commandregistry.ActionBridgeStatus, commandregistry.ActionThreadInfo:
		s.runQueryActionForBackend(ctx, message, record.Backend, record.TargetID, commandregistry.ActionThreadInfo, []string{strconv.Itoa(record.Number)})
	case commandregistry.ActionThreadHistory:
		s.runQueryActionForBackend(ctx, message, record.Backend, record.TargetID, commandregistry.ActionThreadHistory, append([]string{strconv.Itoa(record.Number)}, invocation.Arguments...))
	case commandregistry.ActionThreadStop:
		s.stopThread(ctx, message, record.TargetID)
	case commandregistry.ActionTaskCancel, commandregistry.ActionInteractionCancel:
		s.cancelThreadInteraction(ctx, message, record.TargetID)
	default:
		s.send(ctx, message.Address, fmt.Sprintf("指令 %s 不支持 #编号 会话上下文。", invocation.Definition.Name))
	}
}

func (s *Service) handleCommand(ctx context.Context, message channels.InboundMessage, text string) {
	invocation, found := s.commandRegistry().Resolve(text)
	if !found {
		s.send(ctx, message.Address, "未知命令。发送 /help 查看可用命令。")
		return
	}
	if !invocation.Definition.Enabled {
		s.send(ctx, message.Address, "指令 "+invocation.Definition.Name+" 当前已停用。")
		return
	}
	s.mu.Lock()
	tasks := s.taskService
	s.mu.Unlock()
	if tasks != nil {
		if result, handled, err := tasks.ExecuteRemoteCommand(ctx, message, invocation); handled {
			if err != nil {
				s.send(ctx, message.Address, "任务操作失败："+err.Error())
			} else {
				s.send(ctx, message.Address, result)
			}
			return
		}
	}
	backend := s.backendForMessage(message)
	target := s.targetForMessage(message)
	if !commandregistry.SupportsBackend(invocation.Definition.BackendCapability, backend) {
		s.send(ctx, message.Address, bridgequery.NotApplicableText(backend))
		return
	}
	argument := strings.TrimSpace(strings.Join(invocation.Arguments, " "))
	if result, handled := s.queryService().ExecuteActionForBackend(ctx, backend, target, invocation.Definition.Action, invocation.Arguments); handled {
		s.sendQuery(ctx, message.Address, result)
		return
	}
	switch invocation.Definition.Action {
	case commandregistry.ActionBridgeStart:
		if binding, ok := s.findBinding(message.Address); ok {
			label := firstNonEmpty(binding.ThreadID, binding.SessionKey)
			s.send(ctx, message.Address, fmt.Sprintf("Codex Bridge 已就绪。当前会话已绑定 %s [%s]。\n发送 /help 查看命令。", backend, shortID(label)))
		} else {
			s.send(ctx, message.Address, "Codex Bridge 已就绪。当前会话尚未绑定。\n使用 /threads 查看当前后端会话，再用 /bind 1 绑定。")
		}
	case commandregistry.ActionThreadBind:
		if argument == "" {
			s.send(ctx, message.Address, "用法：/bind <编号或 ID>；OpenClaw 也可使用 /bind oc:<SessionKey>。/threads 会显示当前后端会话。")
			return
		}
		s.bind(ctx, message, argument)
	case commandregistry.ActionThreadUnbind:
		s.unbind(ctx, message)
	case commandregistry.ActionThreadCurrent:
		s.current(ctx, message)
	case commandregistry.ActionThreadStop:
		if argument != "" {
			s.commandThread(ctx, message, argument, "stop")
		} else {
			s.stopTurn(ctx, message)
		}
	case commandregistry.ActionTaskCancel:
		if backend == conversation.BackendOpenClaw {
			s.send(ctx, message.Address, bridgequery.NotApplicableText(backend))
		} else if argument != "" {
			s.commandThread(ctx, message, argument, "cancel")
		} else {
			s.cancelInteraction(ctx, message)
		}
	case commandregistry.ActionInteractionCancel:
		if argument != "" {
			s.commandThread(ctx, message, argument, "cancel")
		} else {
			s.cancelInteraction(ctx, message)
		}
	default:
		s.send(ctx, message.Address, "未知命令。发送 /help 查看可用命令。")
	}
}

func (s *Service) listThreads(ctx context.Context, message channels.InboundMessage) {
	if binding, ok := s.findBinding(message.Address); ok && bindingBackend(binding) == conversation.BackendOpenClaw {
		s.listOpenClawSessions(ctx, message)
		return
	}
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
	output.WriteString("最近的 [Codex] Thread（5 分钟内可用序号绑定）：\n")
	store := conversationregistry.ForAny(s.registry)
	for index, thread := range threads {
		number := thread.Number
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendCodex, thread.ThreadID); ok {
				number = record.Number
			}
		}
		fmt.Fprintf(&output, "%d. [Codex] #%d %s\n   项目：%s · ID：%s · 更新：%s\n", index+1, number, displayTitle(thread.Title), projectName(thread.CWD), shortID(thread.ThreadID), displayTime(thread.UpdatedAt))
	}
	s.send(ctx, message.Address, strings.TrimSpace(output.String()))
}

func (s *Service) listOpenClawSessions(ctx context.Context, message channels.InboundMessage) {
	backend := s.openClawBackend()
	if backend == nil {
		s.send(ctx, message.Address, "当前后端是 OpenClaw，但 Gateway 尚未配置。")
		return
	}
	sessions, err := backend.ListSessions(ctx, 20)
	if err != nil {
		s.send(ctx, message.Address, "无法读取 OpenClaw Session，请检查 Gateway 连接。")
		return
	}
	if len(sessions) == 0 {
		s.send(ctx, message.Address, "当前没有可用的 OpenClaw Session。")
		return
	}
	var output strings.Builder
	output.WriteString("[OpenClaw] 当前后端 Session：\n")
	store := conversationregistry.ForAny(s.registry)
	for _, session := range sessions {
		number := session.Number
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendOpenClaw, session.Key); ok {
				number = record.Number
			}
		}
		fmt.Fprintf(&output, "\n• [OpenClaw] #%d %s · %s\n  Session：%s", number, displayTitle(session.Title), firstNonEmpty(session.Status, "idle"), session.Key)
	}
	output.WriteString("\n\n重新绑定：/bind openclaw <Session Key>")
	s.send(ctx, message.Address, output.String())
}

func (s *Service) listNumberedThreads(ctx context.Context, message channels.InboundMessage, argument string) {
	page := 1
	if parsed, err := strconv.Atoi(strings.TrimSpace(argument)); err == nil && parsed > 0 {
		page = parsed
	}
	result, err := s.control.ListThreads(ctx, 200, "")
	if err != nil {
		s.send(ctx, message.Address, "无法读取 Codex 会话。")
		return
	}
	start := (page - 1) * 20
	if start >= len(result.Threads) {
		s.send(ctx, message.Address, "该页没有会话。")
		return
	}
	end := start + 20
	if end > len(result.Threads) {
		end = len(result.Threads)
	}
	var output strings.Builder
	for _, thread := range result.Threads[start:end] {
		fmt.Fprintf(&output, "#%d  %s\n     %s\n     %s\n\n", thread.Number, displayTitle(thread.Title), thread.CWD, thread.Status)
	}
	output.WriteString("回复：\n#12 你的消息\n\n即可发送到该会话。")
	s.send(ctx, message.Address, strings.TrimSpace(output.String()))
}

func (s *Service) commandThread(ctx context.Context, message channels.InboundMessage, selector, action string) {
	if record, found, errText := s.globalNumberSelector(selector); errText != "" {
		s.send(ctx, message.Address, errText)
		return
	} else if found {
		if record.Backend == conversation.BackendOpenClaw {
			s.commandOpenClawTarget(ctx, message, strconv.Itoa(record.Number), action)
			return
		}
		switch action {
		case "thread", "status":
			s.statusThread(ctx, message, record.TargetID)
		case "stop":
			s.stopThread(ctx, message, record.TargetID)
		case "cancel":
			s.cancelThreadInteraction(ctx, message, record.TargetID)
		}
		return
	}
	if s.backendForMessage(message) == conversation.BackendOpenClaw {
		s.commandOpenClawTarget(ctx, message, selector, action)
		return
	}
	store := conversationregistry.ForAny(s.registry)
	if store == nil {
		s.send(ctx, message.Address, "聊天编号尚未初始化。")
		return
	}
	selector = strings.TrimSpace(strings.Trim(selector, "#[]"))
	number, err := strconv.Atoi(selector)
	if err != nil {
		s.send(ctx, message.Address, "请指定聊天编号，例如 /thread 12。")
		return
	}
	record, ok := store.ByNumber(number)
	if !ok {
		s.send(ctx, message.Address, fmt.Sprintf("聊天编号 #%d 不存在。", number))
		return
	}
	switch action {
	case "thread", "status":
		s.statusThread(ctx, message, record.TargetID)
	case "stop":
		s.stopThread(ctx, message, record.TargetID)
	case "cancel":
		s.cancelThreadInteraction(ctx, message, record.TargetID)
	}
}

func (s *Service) commandOpenClawTarget(ctx context.Context, message channels.InboundMessage, selector, action string) {
	selector = strings.TrimSpace(selector)
	var session conversation.Session
	var err error
	if parsed, recognized, parseErr := numberprefix.Parse(selector); parseErr != nil {
		s.send(ctx, message.Address, parseErr.Error())
		return
	} else if recognized && parsed.Number > 0 && parsed.Content == "" {
		session, err = s.openClawSessionByNumber(ctx, parsed.Number)
	} else {
		backend := s.openClawBackend()
		if backend != nil {
			detail, readErr := backend.ReadSession(ctx, strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(selector, "oc:"), "openclaw:")))
			err = readErr
			session = detail.Session
		}
	}
	if err != nil || session.Key == "" {
		s.send(ctx, message.Address, "指定的 OpenClaw Session 不存在或当前不可用。")
		return
	}
	switch action {
	case "thread", "status":
		s.runQueryActionForBackend(ctx, message, conversation.BackendOpenClaw, session.Key, commandregistry.ActionThreadInfo, []string{strconv.Itoa(session.Number)})
	case "stop":
		s.stopOpenClawSession(ctx, message, session.Key)
	default:
		s.send(ctx, message.Address, "该 OpenClaw Session 不支持此操作。")
	}
}

func (s *Service) statusThread(ctx context.Context, message channels.InboundMessage, threadID string) {
	thread, err := control.ReadThreadActivity(ctx, s.control, threadID)
	if err != nil || thread.ThreadID == "" {
		s.send(ctx, message.Address, "指定会话不可用。")
		return
	}
	last := thread.UpdatedAt
	if last == "" {
		last = thread.Runtime.LastActivityAt
	}
	s.send(ctx, message.Address, fmt.Sprintf("#%d %s\nThread: %s\ncwd: %s\n状态: %s\n最后消息: %s", thread.Number, displayTitle(thread.Title), shortID(thread.ThreadID), thread.CWD, thread.Runtime.State, displayTime(last)))
}

func (s *Service) stopThread(ctx context.Context, message channels.InboundMessage, threadID string) {
	state := s.runtime.RuntimeState(threadID)
	s.mu.Lock()
	route := s.routes[routeMapKey(conversation.BackendCodex, threadID, state.TurnID)]
	if route == nil {
		route = s.routes[state.TurnID] // legacy in-memory route compatibility
	}
	owned := route != nil && route.ThreadID == threadID && sameAddress(route.Address, message.Address) && route.UserID == message.UserID
	s.mu.Unlock()
	if !owned || !state.CanInterrupt || state.TurnID == "" {
		s.send(ctx, message.Address, "没有可由当前 QQ 用户停止的该会话任务。")
		return
	}
	if _, err := s.runtime.InterruptTurn(ctx, threadID, state.TurnID); err != nil {
		s.send(ctx, message.Address, "停止请求失败；任务可能已经结束。")
		return
	}
	s.clearTurnInput(state.TurnID)
}

func (s *Service) cancelThreadInteraction(ctx context.Context, message channels.InboundMessage, threadID string) {
	for _, item := range s.runtime.ListInteractions("pending") {
		if item.ThreadID == threadID && item.Kind == interactions.KindUserInput {
			if _, err := s.runtime.RespondInteraction(ctx, item.ID, interactions.ResponseRequest{Action: "cancel"}); err == nil {
				s.clearInteraction(item.ID)
				s.send(ctx, message.Address, "已取消该会话的用户输入。")
				return
			}
		}
	}
	s.send(ctx, message.Address, "该会话没有等待中的用户输入。")
}

func (s *Service) bind(ctx context.Context, message channels.InboundMessage, selector string) {
	var backendKind, target string
	resolvedByRegistry := false
	if record, found, errText := s.globalNumberSelector(selector); errText != "" {
		s.send(ctx, message.Address, errText)
		return
	} else if found {
		// A global number carries its own backend and target. Current Profile
		// routing is only a fallback for unprefixed non-numeric selectors.
		backendKind, target = record.Backend, record.TargetID
		resolvedByRegistry = true
	} else {
		backendKind, target = parseBindingTarget(selector, s.backendForMessage(message))
	}
	if backendKind == conversation.BackendOpenClaw {
		sessionKey := target
		backend := s.openClawBackend()
		if backend == nil {
			s.send(ctx, message.Address, "OpenClaw 后端尚未配置，请先在设置页测试连接。")
			return
		}
		var detail conversation.Detail
		var err error
		if resolvedByRegistry {
			// TargetID is opaque. Once the global registry has resolved the
			// number, do not parse a numeric-looking SessionKey as another
			// number.
			detail, err = backend.ReadSession(ctx, sessionKey)
		} else if prefix, recognized, parseErr := numberprefix.Parse(sessionKey); parseErr != nil {
			s.send(ctx, message.Address, parseErr.Error())
			return
		} else if recognized && prefix.Number > 0 && prefix.Content == "" {
			session, lookupErr := s.openClawSessionByNumber(ctx, prefix.Number)
			err = lookupErr
			if err == nil {
				detail, err = backend.ReadSession(ctx, session.Key)
			}
		} else {
			detail, err = backend.ReadSession(ctx, sessionKey)
		}
		if err != nil || detail.Key == "" {
			s.send(ctx, message.Address, "指定的 OpenClaw Session 不存在或当前不可用。请先发送 /threads 查看 Session。")
			return
		}
		if detail.Archived != nil && *detail.Archived {
			s.send(ctx, message.Address, "指定的 OpenClaw Session 已归档，不能绑定。")
			return
		}
		input, err := s.prepareBinding(bindings.CreateRequest{
			Backend: conversation.BackendOpenClaw, TargetID: detail.Key, ChannelType: "qqbot", ChannelProfileID: message.Address.ChannelProfileID,
			ResourceID: message.Address.ChannelProfileID, AccountID: message.Address.AccountID,
			ConversationType: qqbotConversationType(message.Address.ConversationType), ConversationID: message.Address.ChatID, ChatID: message.Address.ChatID,
			ThreadID: detail.Key, SessionKey: detail.Key,
		})
		if err != nil {
			s.send(ctx, message.Address, "当前 QQ Profile 未分配给 OpenClaw，无法创建该绑定。")
			return
		}
		s.clearAddress(message.Address)
		created, previous, err := s.bindings.UpsertAddress(input)
		if err != nil {
			s.send(ctx, message.Address, "保存 OpenClaw 绑定失败。")
			return
		}
		s.refreshBindingCount()
		payload := safeBindingPayload(created)
		if previous != nil {
			payload["replacedSessionKey"] = shortID(firstNonEmpty(previous.SessionKey, previous.ThreadID))
		}
		s.broker.Publish(events.BindingCreated, payload)
		s.send(ctx, message.Address, fmt.Sprintf("已绑定到 [OpenClaw] %s（Session：%s）。", displayTitle(detail.Title), detail.Key))
		return
	}
	threadID, errText := target, ""
	if !resolvedByRegistry {
		threadID, errText = s.resolveThread(ctx, message.Address, target)
		if errText != "" {
			s.send(ctx, message.Address, errText)
			return
		}
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
	input, err := s.prepareBinding(bindings.CreateRequest{
		Backend: conversation.BackendCodex, TargetID: threadID, ChannelType: "qqbot", ChannelProfileID: message.Address.ChannelProfileID,
		ResourceID: message.Address.ChannelProfileID, AccountID: message.Address.AccountID, ConversationType: qqbotConversationType(message.Address.ConversationType),
		ConversationID: message.Address.ChatID, ChatID: message.Address.ChatID, TopicID: "", ThreadID: threadID,
	})
	if err != nil {
		s.send(ctx, message.Address, "当前 QQ Profile 未分配给 Codex，无法创建该绑定。")
		return
	}
	s.clearAddress(message.Address)
	created, previous, err := s.bindings.UpsertAddress(input)
	if err != nil {
		s.send(ctx, message.Address, "保存绑定失败。")
		return
	}
	s.refreshBindingCount()
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
	store := conversationregistry.ForAny(s.registry)
	if number, err := strconv.Atoi(selector); err == nil {
		if store != nil {
			if record, found := store.ByNumber(number); found {
				if record.Backend != conversationregistry.BackendCodex {
					return "", fmt.Sprintf("聊天编号 #%d 属于 OpenClaw，不能按 Codex Thread 绑定。", number)
				}
				return record.TargetID, ""
			}
			return "", fmt.Sprintf("聊天编号 #%d 不存在。", number)
		}
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
	if store != nil && (strings.HasPrefix(selector, "#") || strings.HasPrefix(selector, "[")) {
		trimmed := strings.TrimSpace(strings.Trim(selector, "#[]"))
		if number, err := strconv.Atoi(trimmed); err == nil {
			if record, found := store.ByNumber(number); found {
				if record.Backend != conversationregistry.BackendCodex {
					return "", fmt.Sprintf("聊天编号 #%d 属于 OpenClaw，不能按 Codex Thread 绑定。", number)
				}
				return record.TargetID, ""
			}
			return "", fmt.Sprintf("聊天编号 #%d 不存在。", number)
		}
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
	deleted, err := s.bindings.DeleteProfileAddress("qqbot", message.Address.ChannelProfileID, qqbotConversationType(message.Address.ConversationType), message.Address.ChatID, "")
	if errors.Is(err, bindings.ErrNotFound) {
		s.send(ctx, message.Address, "当前 QQ 会话尚未绑定。")
		return
	}
	if err != nil {
		s.send(ctx, message.Address, "解除绑定失败。")
		return
	}
	s.refreshBindingCount()
	s.broker.Publish(events.BindingDeleted, safeBindingPayload(deleted))
	s.send(ctx, message.Address, "已解除绑定，并清除该 QQ 会话的投递和等待状态；Thread 本身未被删除。")
}

func (s *Service) current(ctx context.Context, message channels.InboundMessage) {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.send(ctx, message.Address, "当前 QQ 会话尚未绑定。使用 /threads 或 /bind。")
		return
	}
	if bindingBackend(binding) == conversation.BackendOpenClaw {
		backend := s.openClawBackend()
		if backend == nil {
			s.send(ctx, message.Address, "OpenClaw 后端尚未配置。")
			return
		}
		detail, err := backend.ReadSession(ctx, firstNonEmpty(binding.SessionKey, binding.ThreadID))
		if err != nil || detail.Key == "" {
			s.send(ctx, message.Address, "绑定的 OpenClaw Session 已不可用，请重新绑定。")
			return
		}
		number := detail.Number
		if store := conversationregistry.ForAny(s.registry); store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendOpenClaw, detail.Key); ok {
				number = record.Number
			}
		}
		s.send(ctx, message.Address, fmt.Sprintf("当前绑定：OpenClaw #%d\n当前绑定\n后端：OpenClaw\nProfile：%s\nSession：#%d %s\nSessionKey：%s\n更新：%s\n状态：%s", number, firstNonEmpty(binding.ChannelProfileID, "默认"), number, displayTitle(detail.Title), detail.Key, displayTime(detail.UpdatedAt), firstNonEmpty(detail.Status, "idle")))
		return
	}
	thread, err := control.ReadThreadHistory(ctx, s.control, binding.ThreadID, 1)
	if err != nil || thread.ThreadID == "" {
		s.send(ctx, message.Address, "绑定的 Thread "+shortID(binding.ThreadID)+" 已不存在，请解除绑定或重新绑定。")
		return
	}
	number := thread.Number
	if store := conversationregistry.ForAny(s.registry); store != nil {
		if record, ok := store.ByTarget(conversationregistry.BackendCodex, thread.ThreadID); ok {
			number = record.Number
		}
	}
	s.send(ctx, message.Address, fmt.Sprintf("当前绑定：Codex #%d\n当前绑定\n后端：Codex\nProfile：%s\n标题：%s\n项目：%s\nThread：%s\n更新：%s\n状态：%s\n最近 Turn 结果：%s", number, firstNonEmpty(binding.ChannelProfileID, "默认"), displayTitle(thread.Title), projectName(thread.CWD), shortID(thread.ThreadID), displayTime(thread.UpdatedAt), thread.Runtime.State, latestTurnResult(thread)))
}

func (s *Service) status(ctx context.Context, message channels.InboundMessage) {
	bridge := s.runtime.Status()
	qqStatus := s.transport.QQBotStatus()
	qqState := strings.TrimSpace(qqStatus.ConnectionState)
	if qqState == "" {
		qqState = "stopped"
	}
	lines := []string{"Bridge：运行中", "QQ 官方机器人：" + qqState}
	if bridge.AppServerRunning {
		lines = append(lines, "Codex App Server：已连接")
	} else {
		lines = append(lines, "Codex App Server：不可用")
	}
	if backend := s.openClawBackend(); backend != nil {
		status := backend.ConnectionStatus()
		state := status.State
		if status.Connected {
			state = "已连接"
		}
		lines = append(lines, "OpenClaw Gateway："+firstNonEmpty(state, "未配置"))
	}
	binding, ok := s.findBinding(message.Address)
	if !ok {
		lines = append(lines, "绑定：无")
		s.send(ctx, message.Address, strings.Join(lines, "\n"))
		return
	}
	if bindingBackend(binding) == conversation.BackendOpenClaw {
		backend := s.openClawBackend()
		if backend == nil {
			lines = append(lines, "绑定：[OpenClaw] 后端未配置")
			s.send(ctx, message.Address, strings.Join(lines, "\n"))
			return
		}
		detail, err := backend.ReadSession(ctx, firstNonEmpty(binding.SessionKey, binding.ThreadID))
		if err != nil || detail.Key == "" {
			lines = append(lines, "绑定：[OpenClaw] "+shortID(firstNonEmpty(binding.SessionKey, binding.ThreadID)), "Session：不可用或已删除")
		} else {
			lines = append(lines, fmt.Sprintf("Session：[OpenClaw] #%d %s · %s", detail.Number, displayTitle(detail.Title), shortID(detail.Key)), "状态："+firstNonEmpty(detail.Status, "idle"))
		}
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
	if bindingBackend(binding) == conversation.BackendOpenClaw {
		key := firstNonEmpty(binding.SessionKey, binding.ThreadID)
		s.stopOpenClawSession(ctx, message, key)
		return
	}
	state := s.runtime.RuntimeState(binding.ThreadID)
	s.mu.Lock()
	route := s.routes[routeMapKey(conversation.BackendCodex, binding.ThreadID, state.TurnID)]
	if route == nil {
		route = s.routes[state.TurnID] // legacy in-memory route compatibility
	}
	owned := route != nil && sameAddress(route.Address, message.Address) && route.UserID == message.UserID && route.ThreadID == binding.ThreadID
	s.mu.Unlock()
	if !owned || !state.CanInterrupt || (state.Origin != "local" && state.Origin != "qqbot") || state.TurnID == "" || state.TurnID != route.TurnID {
		s.send(ctx, message.Address, "没有可由此 QQ 会话和当前用户停止的任务。")
		return
	}
	if _, err := s.runtime.InterruptTurn(ctx, binding.ThreadID, state.TurnID); err != nil {
		s.send(ctx, message.Address, "停止请求失败；任务可能已完成或已失去控制权。")
		return
	}
	s.clearTurnInput(state.TurnID)
}

func (s *Service) stopOpenClawSession(ctx context.Context, message channels.InboundMessage, key string) {
	backend := s.openClawBackend()
	if backend == nil {
		s.send(ctx, message.Address, "OpenClaw 后端尚未连接。")
		return
	}
	s.mu.Lock()
	var route *turnRoute
	for _, candidate := range s.routes {
		if candidate.Backend == conversation.BackendOpenClaw && candidate.SessionKey == key && sameAddress(candidate.Address, message.Address) && candidate.UserID == message.UserID {
			route = candidate
			break
		}
	}
	s.mu.Unlock()
	runID := ""
	if route != nil {
		runID = route.TurnID
	}
	if runID == "" {
		if detail, err := backend.ReadSession(ctx, key); err == nil && detail.HasActiveRun {
			runID = firstNonEmpty(detail.ActiveRunIDs...)
		}
	}
	if runID == "" {
		s.send(ctx, message.Address, "没有可由当前 QQ 会话停止的 OpenClaw 任务。")
		return
	}
	if _, err := backend.Abort(ctx, key, runID); err != nil {
		s.send(ctx, message.Address, "OpenClaw 停止请求失败；任务可能已经结束。")
		return
	}
	s.send(ctx, message.Address, "已请求停止 OpenClaw 任务。")
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
	if bindingBackend(binding) == conversation.BackendOpenClaw {
		s.startOpenClawTurn(ctx, message, text, binding)
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
	accepted, err := s.runtime.StartTurn(ctx, binding.ThreadID, control.StartTurnRequest{Text: text, Origin: control.TurnOriginQQ})
	if err != nil {
		s.reject(ctx, message, "无法启动任务；Thread 可能正忙、不可用或已归档。", "start-failed")
		return
	}
	route := &turnRoute{Address: message.Address, UserID: message.UserID, Backend: conversation.BackendCodex, SessionKey: accepted.ThreadID, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID}
	s.mu.Lock()
	s.routes[routeMapKey(route.Backend, route.SessionKey, route.TurnID)] = route
	s.mu.Unlock()
	s.publishMessage(events.QQBotMessageRouted, message, "turn-started")
	for _, interaction := range s.runtime.ListInteractions("pending") {
		if interaction.ThreadID == accepted.ThreadID && interaction.TurnID == accepted.TurnID {
			s.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID, Payload: map[string]any{"interaction": interaction}})
		}
	}
}

func (s *Service) startOpenClawTurn(ctx context.Context, message channels.InboundMessage, text string, binding bindings.Binding) {
	s.startOpenClawTurnForKey(ctx, message, text, firstNonEmpty(binding.SessionKey, binding.ThreadID))
}

func (s *Service) startOpenClawTurnForKey(ctx context.Context, message channels.InboundMessage, text, key string) {
	backend := s.openClawBackend()
	if backend == nil {
		s.reject(ctx, message, "OpenClaw 后端尚未配置，请先在设置页测试连接。", "openclaw-unavailable")
		return
	}
	detail, err := backend.ReadSession(ctx, key)
	if err != nil || detail.Key == "" {
		s.reject(ctx, message, "绑定的 OpenClaw Session 不存在或当前不可用，请重新绑定。", "missing-session")
		return
	}
	if detail.Archived != nil && *detail.Archived {
		s.reject(ctx, message, "绑定的 OpenClaw Session 已归档，请重新绑定。", "archived")
		return
	}
	if detail.HasActiveRun {
		s.reject(ctx, message, "该 OpenClaw Session 当前正在执行任务，请先等待或使用 /stop。", "busy")
		return
	}
	accepted, err := backend.SendMessage(ctx, key, text)
	if err != nil {
		s.reject(ctx, message, "OpenClaw 无法启动任务；Session 可能正忙或 Gateway 已断开。", "start-failed")
		return
	}
	route := &turnRoute{Address: message.Address, UserID: message.UserID, Backend: conversation.BackendOpenClaw, SessionKey: key, ThreadID: key, TurnID: accepted.RunID}
	s.mu.Lock()
	s.routes[routeMapKey(route.Backend, route.SessionKey, route.TurnID)] = route
	s.mu.Unlock()
	s.publishMessage(events.QQBotMessageRouted, message, "openclaw-turn-started")
}

func (s *Service) startTurnNumbered(ctx context.Context, message channels.InboundMessage, text, threadID string) {
	thread, err := s.control.ReadThread(ctx, threadID, false)
	if err != nil || thread.ThreadID == "" || thread.ThreadID != threadID {
		s.reject(ctx, message, "指定的 Codex 会话不存在。", "missing-thread")
		return
	}
	if thread.Archived != nil && *thread.Archived {
		s.reject(ctx, message, "指定的 Codex 会话已归档。", "archived")
		return
	}
	state := s.runtime.RuntimeState(threadID)
	if !state.CanSend || state.PendingInteractionCount > 0 {
		s.reject(ctx, message, "该会话正在运行或等待输入，暂不能提交新任务。", "busy")
		return
	}
	accepted, err := s.runtime.StartTurn(ctx, threadID, control.StartTurnRequest{Text: text, Origin: control.TurnOriginQQ})
	if err != nil {
		s.reject(ctx, message, "无法启动任务；会话可能忙碌或不可用。", "start-failed")
		return
	}
	route := &turnRoute{Address: message.Address, UserID: message.UserID, Backend: conversation.BackendCodex, SessionKey: accepted.ThreadID, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID}
	s.mu.Lock()
	s.routes[routeMapKey(route.Backend, route.SessionKey, route.TurnID)] = route
	s.mu.Unlock()
	s.publishMessage(events.QQBotMessageRouted, message, "turn-started")
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
	return true
}

func (s *Service) answerInteractionForThread(ctx context.Context, message channels.InboundMessage, text, threadID string) bool {
	key := inputKey(message.Address, message.UserID)
	s.mu.Lock()
	interactionID := ""
	for id, flow := range s.flows {
		if flow.ThreadID == threadID && sameAddress(flow.Address, message.Address) && flow.UserID == message.UserID && time.Now().Before(flow.Expires) {
			interactionID = id
			break
		}
	}
	if interactionID != "" {
		s.flowByInput[key] = interactionID
	}
	s.mu.Unlock()
	if interactionID == "" {
		return false
	}
	return s.answerInteraction(ctx, message, text)
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
			if qqbotRelevantEvent(event.EventType) {
				s.handleEvent(event)
			}
		}
	}
}

func qqbotRelevantEvent(eventType string) bool {
	switch eventType {
	case events.CodexDisconnected, events.InteractionRequested, events.InteractionResolved,
		events.TurnCompleted, events.TurnFailed, events.TurnInterrupted,
		events.OpenClawDisconnected, events.OpenClawMessageDelta, events.OpenClawMessageCompleted,
		events.OpenClawMessageAborted, events.OpenClawMessageFailed:
		return true
	case events.TaskWaitingInput, events.TaskCompleted, events.TaskFailed:
		return true
	default:
		return false
	}
}

func (s *Service) handleEvent(event events.Event) {
	if event.EventType == events.TaskWaitingInput || event.EventType == events.TaskCompleted || event.EventType == events.TaskFailed {
		s.handleTaskNotification(event)
		return
	}
	if event.EventType == events.CodexDisconnected {
		s.finishRoutesForBackend(conversation.BackendCodex)
		return
	}
	if event.EventType == events.OpenClawDisconnected {
		// A Gateway reconnect must not revoke the originating QQ route. The
		// Gateway may finish the same run after transport recovery.
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
	route := s.routeForEventLocked(event)
	s.mu.Unlock()
	if route == nil || route.ThreadID != event.ThreadID {
		return
	}
	if route.Backend == conversation.BackendOpenClaw {
		s.handleOpenClawEvent(event, route)
		return
	}
	switch event.EventType {
	case events.TurnCompleted, events.TurnFailed, events.TurnInterrupted:
		s.removeRoute(route)
	}
}

func (s *Service) handleTaskNotification(event events.Event) {
	s.mu.Lock()
	tasks := s.taskService
	profileID := s.channelProfileID
	s.mu.Unlock()
	if tasks == nil {
		return
	}
	task, ok := taskcenter.TaskFromEvent(event)
	if !ok || !strings.EqualFold(task.ChannelType, "qqbot") || (task.ChannelProfileID != "" && task.ChannelProfileID != profileID) {
		return
	}
	address, ok := tasks.NotificationAddress(task)
	if !ok {
		return
	}
	if task.Status == taskcenter.StatusWaitingInput && attachTaskInteraction(s, task, address) {
		return
	}
	if task.Status == taskcenter.StatusCompleted || task.Status == taskcenter.StatusFailed {
		s.removeTaskRoute(task)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	s.send(ctx, address, tasks.NotificationText(task))
}

// attachTaskInteraction hands a Task-created Codex interaction to the same
// guarded QQ flow used by ordinary bound turns. This keeps multi-question
// parsing and user/Address authorization in one existing implementation.
func attachTaskInteraction(s *Service, task taskcenter.Task, address channels.ChannelAddress) bool {
	if task.Backend != conversation.BackendCodex || task.TargetID == "" || task.CurrentRunID == "" || task.PendingInteractionID == "" {
		return false
	}
	route := &turnRoute{Address: address, UserID: firstNonEmpty(task.UserID, address.UserID), Backend: conversation.BackendCodex, SessionKey: task.TargetID, ThreadID: task.TargetID, TurnID: task.CurrentRunID, TaskNumber: task.TaskNumber}
	key := routeMapKey(route.Backend, route.SessionKey, route.TurnID)
	s.mu.Lock()
	if existing := s.routes[key]; existing != nil {
		if !sameAddress(existing.Address, address) || existing.UserID != route.UserID {
			s.mu.Unlock()
			return false
		}
		route = existing
	} else {
		s.routes[key] = route
	}
	s.mu.Unlock()
	s.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: task.TargetID, TurnID: task.CurrentRunID, Payload: map[string]any{"interaction": map[string]any{"id": task.PendingInteractionID}}})
	return true
}

func (s *Service) removeTaskRoute(task taskcenter.Task) {
	if task.TargetID == "" || task.CurrentRunID == "" {
		return
	}
	s.mu.Lock()
	route := s.routes[routeMapKey(task.Backend, task.TargetID, task.CurrentRunID)]
	s.mu.Unlock()
	if route != nil {
		s.removeRoute(route)
	}
}

func (s *Service) handleOpenClawEvent(event events.Event, route *turnRoute) {
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Second)
	defer cancel()
	switch event.EventType {
	case events.OpenClawMessageCompleted:
		text := payloadString(event.Payload, "text")
		if text == "" {
			if backend := s.openClawBackend(); backend != nil {
				if detail, err := backend.ReadSession(ctx, route.SessionKey); err == nil {
					for index := len(detail.Messages) - 1; index >= 0; index-- {
						if detail.Messages[index].Role == "assistant" && strings.TrimSpace(detail.Messages[index].Text) != "" {
							text = detail.Messages[index].Text
							break
						}
					}
				}
			}
		}
		if text == "" {
			text = "OpenClaw 任务已完成，但 Gateway 没有返回可显示的文本。"
		}
		s.send(ctx, route.Address, text)
		s.removeRoute(route)
	case events.OpenClawMessageAborted:
		s.send(ctx, route.Address, "OpenClaw 任务已停止。")
		s.removeRoute(route)
	case events.OpenClawMessageFailed:
		reason := payloadString(event.Payload, "error")
		if reason == "" {
			reason = "Gateway 返回了错误。"
		}
		s.send(ctx, route.Address, "OpenClaw 任务失败："+reason)
		s.removeRoute(route)
	}
}

func (s *Service) handleInteractionEvent(event events.Event) {
	s.mu.Lock()
	route := s.routeForEventLocked(event)
	s.mu.Unlock()
	if route == nil || route.ThreadID != event.ThreadID {
		return
	}
	id := interactionIDFromPayload(event.Payload)
	interaction, ok := s.runtime.GetInteraction(id)
	if !ok || interaction.ThreadID != route.ThreadID || interaction.TurnID != route.TurnID || interaction.Status != "pending" {
		return
	}
	s.mu.Lock()
	if s.interactionNotified == nil {
		s.interactionNotified = make(map[string]bool)
	}
	if s.interactionNotified[interaction.ID] {
		s.mu.Unlock()
		return
	}
	s.interactionNotified[interaction.ID] = true
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	if interaction.Kind != interactions.KindUserInput {
		s.send(ctx, route.Address, taskInteractionPrefix(route.TaskNumber)+"Codex 正在请求审批。为保证安全，请在 WPF 中允许或拒绝；QQ 不会自动处理审批。")
		return
	}
	if len(interaction.Questions) == 0 {
		s.send(ctx, route.Address, taskInteractionPrefix(route.TaskNumber)+"Codex 请求了用户输入，但该问题需要在 WPF 中处理。")
		return
	}
	expires, err := time.Parse(time.RFC3339Nano, interaction.ExpiresAt)
	if err != nil || expires.Before(time.Now()) {
		expires = time.Now().Add(flowTTL)
	}
	flow := &interactionFlow{
		Expires: expires, Address: route.Address, UserID: route.UserID, ThreadID: route.ThreadID, TurnID: route.TurnID,
		TaskNumber:    route.TaskNumber,
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
	taskNumber := flow.TaskNumber
	s.mu.Unlock()
	var prompt strings.Builder
	fmt.Fprintf(&prompt, "%sCodex 需要你的输入（%d/%d）\n", taskInteractionPrefix(taskNumber), position, total)
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

func taskInteractionPrefix(number int) string {
	if number < 1 {
		return ""
	}
	return fmt.Sprintf("T%d：", number)
}

func (s *Service) finishAllRoutes(_ string) {
	s.mu.Lock()
	s.routes = make(map[string]*turnRoute)
	s.flows = make(map[string]*interactionFlow)
	s.flowByInput = make(map[string]string)
	s.interactionNotified = make(map[string]bool)
	s.mu.Unlock()
}

func (s *Service) finishRoutesForBackend(backend string) {
	s.mu.Lock()
	removedTurns := make(map[string]struct{})
	for turnID, route := range s.routes {
		routeBackend := route.Backend
		if routeBackend == "" {
			routeBackend = conversation.BackendCodex
		}
		if routeBackend == backend {
			removedTurns[turnID] = struct{}{}
			if route.TurnID != "" {
				removedTurns[route.TurnID] = struct{}{}
			}
			delete(s.routes, turnID)
		}
	}
	if len(removedTurns) == 0 {
		s.mu.Unlock()
		return
	}
	wasRemoved := func(turnID string) bool {
		_, ok := removedTurns[turnID]
		return ok
	}
	for interactionID, flow := range s.flows {
		if flow == nil || !wasRemoved(flow.TurnID) {
			continue
		}
		key := inputKey(flow.Address, flow.UserID)
		if s.flowByInput[key] == interactionID {
			delete(s.flowByInput, key)
		}
		delete(s.flows, interactionID)
		delete(s.interactionNotified, interactionID)
	}
	if backend == conversation.BackendCodex {
		// This map only de-duplicates Codex interaction events. Clear any
		// approval-only markers that have no input flow so a reconnect can
		// notify the channel again.
		s.interactionNotified = make(map[string]bool)
	}
	s.mu.Unlock()
}

func (s *Service) handleAdapterEvent(event AdapterEvent) {
	eventType := ""
	switch event.Kind {
	case "authenticating":
		eventType = events.QQBotAuthenticating
	case "token_refreshed":
		eventType = events.QQBotTokenRefreshed
	case "connecting":
		eventType = events.QQBotConnecting
	case "identifying":
		eventType = events.QQBotConnecting
	case "connected":
		eventType = events.QQBotConnected
		status := s.transport.QQBotStatus()
		s.onAppID(status.AppID)
		s.refreshBindingCount()
	case "ready":
		eventType = events.QQBotReady
	case "disconnected":
		eventType = events.QQBotDisconnected
	case "reconnecting":
		eventType = events.QQBotReconnecting
	case "stopped":
		eventType = events.QQBotStopped
		s.clearTransient()
	case "heartbeat":
		eventType = events.QQBotHeartbeat
	case "message_received":
		eventType = events.QQBotMessageReceived
	case "rejected":
		eventType = events.QQBotMessageRejected
	case "action_failed":
		eventType = events.QQBotActionFailed
	case "error":
		eventType = events.QQBotError
	}
	if eventType == "" {
		return
	}
	payload := map[string]any{"channelType": "qqbot"}
	if profileID := s.channelProfile(); profileID != "" {
		payload["channelProfileId"] = profileID
	}
	if eventType != events.QQBotMessageReceived && eventType != events.QQBotMessageRejected {
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
	case events.QQBotConnected:
		s.publishChannel(events.ChannelConnected, event.Code)
	case events.QQBotDisconnected, events.QQBotStopped:
		s.publishChannel(events.ChannelDisconnected, event.Code)
	case events.QQBotError:
		s.publishChannel(events.ChannelError, event.Code)
	case events.QQBotMessageReceived:
		s.broker.Publish(events.MessageReceived, payload)
	case events.QQBotMessageRejected:
		s.broker.Publish(events.MessageRejected, payload)
	}
	switch eventType {
	case events.QQBotConnecting, events.QQBotConnected, events.QQBotDisconnected, events.QQBotReconnecting,
		events.QQBotStopped, events.QQBotActionFailed, events.QQBotError:
		s.publishChannel(events.ChannelStatusChanged, event.Code)
	}
}

func (s *Service) publishChannel(eventType, code string) {
	payload := map[string]any{"channelType": "qqbot", "status": s.safeEventStatus()}
	if profileID := s.channelProfile(); profileID != "" {
		payload["channelProfileId"] = profileID
	}
	if code != "" {
		payload["code"] = code
	}
	s.broker.Publish(eventType, payload)
}

func (s *Service) safeEventStatus() map[string]any {
	status := s.transport.QQBotStatus()
	payload := map[string]any{
		"channelType": "qqbot", "configured": status.Configured, "running": status.Running,
		"connected": status.Connected, "connectionState": status.ConnectionState,
		"lastConnectedAt": status.LastConnectedAt, "lastHeartbeatAt": status.LastHeartbeatAt,
		"lastDispatchAt": status.LastDispatchAt, "reconnectCount": status.ReconnectCount,
		"lastErrorCode": status.LastErrorCode, "allowedUserCount": status.AllowedUserCount,
		"allowedGroupCount": status.AllowedGroupCount, "allowedGroupMemberCount": status.AllowedGroupMemberCount,
		"bindingCount": status.BindingCount,
	}
	if profileID := s.channelProfile(); profileID != "" {
		payload["channelProfileId"] = profileID
	}
	return payload
}

func (s *Service) send(ctx context.Context, address channels.ChannelAddress, text string) bool {
	parts := splitOfficialQqMessage(text, qqbotRuneLimit)
	for index, part := range parts {
		result, err := s.transport.SendMessage(ctx, channels.OutboundMessage{Address: address, Text: part})
		if err != nil {
			if s.logger != nil {
				s.logger.Printf("QQ send failed part=%d parts=%d runes=%d conversation=%s chat=%s", index+1, len(parts), utf8.RuneCountInString(part), qqbotConversationType(address.ConversationType), maskID(address.ChatID))
			}
			return false
		}
		payload := map[string]any{"channelType": "qqbot", "conversationType": qqbotConversationType(address.ConversationType), "chat": maskID(address.ChatID), "messageId": shortID(result.MessageID), "length": utf8.RuneCountInString(part), "part": index + 1, "parts": len(parts)}
		if profileID := s.channelProfile(); profileID != "" {
			payload["channelProfileId"] = profileID
		}
		s.broker.Publish(events.MessageSent, payload)
		s.broker.Publish(events.QQBotMessageSent, payload)
	}
	return true
}

func (s *Service) queryService() *bridgequery.Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queries == nil {
		s.queries = bridgequery.New(s.control, s.runtime, s.registry, s.commands)
		s.queries.SetOpenClawBackend(s.openclaw)
	}
	return s.queries
}

func (s *Service) commandRegistry() *commandregistry.Registry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commands == nil {
		s.commands = commandregistry.NewInMemory()
	}
	return s.commands
}

func (s *Service) runQueryAction(ctx context.Context, message channels.InboundMessage, action string, arguments []string) {
	if result, handled := s.queryService().ExecuteActionForBackend(ctx, s.backendForMessage(message), s.targetForMessage(message), action, arguments); handled {
		s.sendQuery(ctx, message.Address, result)
	}
}

func (s *Service) runQueryActionForBackend(ctx context.Context, message channels.InboundMessage, backend, target, action string, arguments []string) {
	if result, handled := s.queryService().ExecuteActionForBackend(ctx, backend, target, action, arguments); handled {
		s.sendQuery(ctx, message.Address, result)
	}
}

func (s *Service) runQuery(ctx context.Context, message channels.InboundMessage, text string) {
	if result, handled := s.queryService().Execute(ctx, text); handled {
		s.sendQuery(ctx, message.Address, result)
	}
}

func (s *Service) sendQuery(ctx context.Context, address channels.ChannelAddress, result bridgequery.Result) {
	for _, part := range result.Parts {
		s.send(ctx, address, part)
	}
}

func splitOfficialQqMessage(text string, limit int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if limit < 32 {
		limit = qqbotRuneLimit
	}
	plain := splitOfficialRunes(text, limit)
	if len(plain) == 1 {
		return plain
	}
	contentLimit := limit - 16
	plain = splitOfficialRunes(text, contentLimit)
	result := make([]string, len(plain))
	for index, part := range plain {
		result[index] = fmt.Sprintf("[%d/%d] %s", index+1, len(plain), part)
	}
	return result
}

func splitOfficialRunes(text string, limit int) []string {
	runes := []rune(strings.TrimSpace(text))
	result := []string{}
	for len(runes) > limit {
		cut := limit
		if index := lastParagraphBoundary(runes, limit); index > limit/2 {
			cut = index
		}
		for _, separators := range [][]rune{{'\n'}, {'。', '！', '？', '.', '!', '?'}, {' ', '\t'}} {
			if cut != limit {
				break
			}
			found := false
			for index := limit; index > limit/2; index-- {
				for _, separator := range separators {
					if runes[index-1] == separator {
						cut, found = index, true
						break
					}
				}
				if found {
					break
				}
			}
			if found {
				break
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

func lastParagraphBoundary(runes []rune, limit int) int {
	for index := limit; index > 1; index-- {
		if runes[index-2] == '\n' && runes[index-1] == '\n' {
			return index
		}
	}
	return 0
}

func (s *Service) reject(ctx context.Context, message channels.InboundMessage, text, reason string) {
	s.send(ctx, message.Address, text)
	s.publishMessage(events.QQBotMessageRejected, message, reason)
}

func (s *Service) publishMessage(eventType string, message channels.InboundMessage, result string) {
	threadID := ""
	if binding, ok := s.findBinding(message.Address); ok {
		threadID = shortID(binding.ThreadID)
	}
	payload := map[string]any{
		"channelType": "qqbot", "conversationType": qqbotConversationType(message.Address.ConversationType),
		"chat": maskID(message.Address.ChatID), "user": maskID(message.UserID), "messageId": shortID(message.MessageID),
		"threadId": threadID, "length": utf8.RuneCountInString(message.Text), "routeResult": result,
	}
	if profileID := s.channelProfile(); profileID != "" {
		payload["channelProfileId"] = profileID
	}
	s.broker.Publish(eventType, payload)
	generic := events.MessageRejected
	if eventType == events.QQBotMessageRouted {
		generic = events.MessageRouted
	} else if eventType == events.QQBotMessageReceived {
		generic = events.MessageReceived
	}
	s.broker.Publish(generic, payload)
}

func (s *Service) findBinding(address channels.ChannelAddress) (bindings.Binding, bool) {
	owner := strings.TrimSpace(address.ChannelProfileID)
	if owner == "" {
		owner = address.AccountID
	}
	binding, ok := s.bindings.FindProfileAddress("qqbot", owner, qqbotConversationType(address.ConversationType), address.ChatID, "")
	if !ok && address.ChannelProfileID == "" {
		binding, ok = s.bindings.FindAddress("qqbot", address.AccountID, qqbotConversationType(address.ConversationType), address.ChatID, "")
	}
	return binding, ok && binding.Enabled
}

func (s *Service) ownsBinding(binding bindings.Binding) bool {
	profileID := s.channelProfile()
	if profileID == "" {
		return binding.ChannelProfileID == "" && binding.ResourceID == ""
	}
	return binding.ResourceID == profileID || binding.ChannelProfileID == profileID
}

func (s *Service) refreshBindingCount() {
	profileID := s.channelProfile()
	if profileID != "" {
		s.transport.SetBindingCount(s.bindings.CountChannelProfile("qqbot", profileID))
		return
	}
	status := s.transport.QQBotStatus()
	accountID := strings.TrimSpace(status.AppID)
	if accountID == "" {
		s.transport.SetBindingCount(0)
		return
	}
	s.transport.SetBindingCount(s.bindings.CountChannelAccount("qqbot", accountID))
}

func (s *Service) onAppID(appID string) {
	appID = strings.TrimSpace(appID)
	s.mu.Lock()
	changed := s.appID != "" && appID != "" && s.appID != appID
	s.appID = appID
	s.mu.Unlock()
	if changed {
		s.clearTransient()
	}
}

func (s *Service) routeActive(route *turnRoute) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.routes[routeMapKey(route.Backend, route.SessionKey, route.TurnID)]
	if current == nil {
		current = s.routes[route.TurnID]
	}
	return current == route && !route.Revoked
}

func (s *Service) removeRoute(route *turnRoute) {
	if route == nil {
		return
	}
	turnID := route.TurnID
	s.mu.Lock()
	delete(s.routes, routeMapKey(route.Backend, route.SessionKey, route.TurnID))
	if s.routes[turnID] == route {
		delete(s.routes, turnID)
	}
	for id, flow := range s.flows {
		if flow.TurnID == turnID {
			delete(s.flowByInput, inputKey(flow.Address, flow.UserID))
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func (s *Service) routeForEventLocked(event events.Event) *turnRoute {
	if route := s.routes[routeMapKey(eventBackend(event), event.ThreadID, event.TurnID)]; route != nil {
		return route
	}
	return s.routes[event.TurnID] // legacy in-memory route compatibility
}

func routeMapKey(backend, targetID, turnID string) string {
	return strings.Join([]string{strings.ToLower(strings.TrimSpace(backend)), strings.TrimSpace(targetID), strings.TrimSpace(turnID)}, "\x00")
}

func eventBackend(event events.Event) string {
	if event.Payload != nil {
		if backend, ok := event.Payload["backend"].(string); ok && strings.TrimSpace(backend) != "" {
			return strings.ToLower(strings.TrimSpace(backend))
		}
	}
	if strings.HasPrefix(event.EventType, "openclaw.") {
		return conversation.BackendOpenClaw
	}
	return conversation.BackendCodex
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
	backend := bindingBackend(binding)
	return map[string]any{
		"bindingId": binding.ID, "backend": backend, "targetId": shortID(firstNonEmpty(binding.TargetID, binding.SessionKey, binding.ThreadID)), "sessionKey": shortID(firstNonEmpty(binding.SessionKey, binding.ThreadID)), "channelType": "qqbot", "channelProfileId": binding.ChannelProfileID, "conversationType": binding.ConversationType,
		"conversationId": maskID(binding.ConversationID), "account": maskID(binding.AccountID), "chat": maskID(binding.ChatID), "threadId": shortID(binding.ThreadID),
	}
}

func (s *Service) openClawBackend() conversation.IConversationBackend {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openclaw
}

func (s *Service) backendForMessage(message channels.InboundMessage) string {
	if binding, ok := s.findBinding(message.Address); ok {
		return bindingBackend(binding)
	}
	s.mu.Lock()
	resolver := s.backendResolver
	s.mu.Unlock()
	if resolver != nil {
		if backend := strings.ToLower(strings.TrimSpace(resolver(message.Address.ChannelProfileID))); backend == conversation.BackendOpenClaw || backend == conversation.BackendCodex {
			return backend
		}
	}
	return conversation.BackendCodex
}

func (s *Service) targetForMessage(message channels.InboundMessage) string {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		return ""
	}
	return firstNonEmpty(binding.SessionKey, binding.ThreadID)
}

func bindingBackend(binding bindings.Binding) string {
	if strings.TrimSpace(binding.Backend) == "" {
		return conversation.BackendCodex
	}
	return strings.ToLower(strings.TrimSpace(binding.Backend))
}

func parseOpenClawSelector(value string) (string, bool) {
	value = strings.TrimSpace(value)
	lower := strings.ToLower(value)
	for _, prefix := range []string{"oc:", "openclaw:"} {
		if strings.HasPrefix(lower, prefix) {
			key := strings.TrimSpace(value[len(prefix):])
			return key, key != ""
		}
	}
	return "", false
}

func (s *Service) globalNumberSelector(value string) (conversationregistry.Record, bool, string) {
	store := conversationregistry.ForAny(s.registry)
	if store == nil {
		return conversationregistry.Record{}, false, ""
	}
	prefix, recognized, err := numberprefix.Parse(strings.TrimSpace(value))
	if err != nil {
		return conversationregistry.Record{}, true, err.Error()
	}
	if !recognized {
		return conversationregistry.Record{}, false, ""
	}
	if prefix.Number < 1 {
		if prefix.Explicit {
			return conversationregistry.Record{}, true, "无效的聊天编号"
		}
		return conversationregistry.Record{}, false, ""
	}
	if prefix.Content != "" {
		return conversationregistry.Record{}, false, ""
	}
	record, ok := store.ByNumber(prefix.Number)
	if !ok {
		return conversationregistry.Record{}, true, fmt.Sprintf("聊天编号 #%d 不存在。", prefix.Number)
	}
	return record, true, ""
}

func parseBindingTarget(value string, current ...string) (backend, target string) {
	value = strings.TrimSpace(value)
	parts := strings.Fields(value)
	if len(parts) >= 2 {
		switch strings.ToLower(parts[0]) {
		case conversation.BackendCodex:
			return conversation.BackendCodex, strings.Join(parts[1:], " ")
		case conversation.BackendOpenClaw:
			return conversation.BackendOpenClaw, strings.Join(parts[1:], " ")
		}
	}
	if key, ok := parseOpenClawSelector(value); ok {
		return conversation.BackendOpenClaw, key
	}
	backend = conversation.BackendCodex
	if len(current) > 0 && strings.EqualFold(strings.TrimSpace(current[0]), conversation.BackendOpenClaw) {
		backend = conversation.BackendOpenClaw
	}
	return backend, value
}

func payloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return strings.TrimSpace(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func addressKey(address channels.ChannelAddress) string {
	return strings.Join([]string{"qqbot", address.ChannelProfileID, address.AccountID, qqbotConversationType(address.ConversationType), address.ChatID}, "\x00")
}

func inputKey(address channels.ChannelAddress, userID string) string {
	return addressKey(address) + "\x00" + strings.TrimSpace(userID)
}

func sameAddress(left, right channels.ChannelAddress) bool {
	return strings.EqualFold(left.ChannelType, right.ChannelType) && left.ChannelProfileID == right.ChannelProfileID && left.AccountID == right.AccountID && qqbotConversationType(left.ConversationType) == qqbotConversationType(right.ConversationType) && left.ChatID == right.ChatID
}

func qqbotConversationType(value string) string {
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

func qqbotErrorCode(err error) string {
	return ClassifyError(err)
}
