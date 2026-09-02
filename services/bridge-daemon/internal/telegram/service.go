package telegram

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
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

type callbackAction struct {
	Expires       time.Time
	Kind          string
	Address       channels.ChannelAddress
	UserID        string
	ThreadID      string
	TurnID        string
	InteractionID string
	QuestionID    string
	Value         string
	SessionID     string
}

type inputWait struct {
	Expires       time.Time
	Address       channels.ChannelAddress
	UserID        string
	ThreadID      string
	TurnID        string
	InteractionID string
	QuestionID    string
}

type multiSession struct {
	Expires       time.Time
	Address       channels.ChannelAddress
	UserID        string
	ThreadID      string
	TurnID        string
	TaskNumber    int
	InteractionID string
	Question      interactions.Question
	Selected      map[string]bool
	MessageID     string
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
	callbacks           map[string]callbackAction
	waits               map[string]inputWait
	sessions            map[string]*multiSession
	flows               map[string]*interactionFlow
	interactionNotified map[string]bool
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
		callbacks: make(map[string]callbackAction), waits: make(map[string]inputWait), sessions: make(map[string]*multiSession), flows: make(map[string]*interactionFlow), interactionNotified: make(map[string]bool),
	}
	service.adapter = NewAdapter(service.HandleMessage)
	service.adapter.SetEventHandler(service.handleAdapterEvent)
	go service.eventLoop()
	return service
}

func (s *Service) Adapter() *Adapter { return s.adapter }

// SetChannelProfileID attaches this service instance to a physical channel
// resource.  The adapter still reports the provider Bot ID in AccountID; the
// profile ID is what keeps shared-bot bindings unambiguous.
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
	commands.AddChangeListener(func() { go s.syncCommandMenu() })
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
	var err error
	request, err = normalizeConfigureRequest(request)
	if err != nil {
		return AdapterStatus{}, err
	}
	s.mu.Lock()
	if s.reconfiguring || len(s.routes) > 0 || s.activeHandlers > 0 {
		s.mu.Unlock()
		return AdapterStatus{}, errors.New("Telegram cannot be reconfigured while a Telegram Turn or update handler is active")
	}
	s.reconfiguring = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.reconfiguring = false
		s.mu.Unlock()
	}()
	wasRunning := s.adapter.TelegramStatus().Running
	if wasRunning {
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		if err := s.adapter.Stop(ctx); err != nil {
			cancel()
			return AdapterStatus{}, err
		}
		cancel()
		s.clearTransient()
	}
	status, err := s.adapter.Configure(request)
	if err != nil {
		return AdapterStatus{}, err
	}
	s.refreshBindingSummary()
	s.broker.Publish(events.TelegramConfigured, map[string]any{
		"allowedUserCount": len(status.AllowedUserIDs), "pollingTimeoutSeconds": status.PollingTimeoutSeconds,
		"sendProgressUpdates": status.SendProgressUpdates, "autoStart": status.AutoStart, "tokenSet": status.TokenSet,
		"proxyMode": status.ProxyMode, "effectiveProxyMode": status.EffectiveProxyMode, "maskedProxyAddress": status.MaskedProxyAddress,
	})
	if wasRunning {
		if err := s.adapter.Start(s.ctx); err != nil {
			s.publishChannelState(events.ChannelError, errorCategory(err))
			return s.adapter.TelegramStatus(), err
		}
		status = s.adapter.TelegramStatus()
		s.broker.Publish(events.TelegramPollingStarted, map[string]any{"channelType": "telegram"})
		s.publishChannelState(events.ChannelConnected, "")
	}
	s.publishChannelState(events.ChannelStatusChanged, "")
	if status.Running {
		go s.syncCommandMenu()
	}
	return status, nil
}

func (s *Service) Start(ctx context.Context) error {
	if err := s.adapter.Start(s.ctx); err != nil {
		s.broker.Publish(events.TelegramStartFailed, map[string]any{"category": errorCategory(err)})
		s.publishChannelState(events.ChannelError, errorCategory(err))
		return err
	}
	s.refreshBindingSummary()
	s.broker.Publish(events.TelegramStarted, map[string]any{"botId": shortID(s.adapter.TelegramStatus().BotID)})
	s.broker.Publish(events.TelegramPollingStarted, map[string]any{"channelType": "telegram"})
	s.publishChannelState(events.ChannelConnected, "")
	go s.syncCommandMenu()
	return nil
}

func (s *Service) syncCommandMenu() {
	s.mu.Lock()
	commands := s.commands
	s.mu.Unlock()
	if commands == nil {
		return
	}
	menu := make([]BotCommand, 0, 100)
	for _, item := range commands.List().Commands {
		if !item.Enabled || !item.TelegramMenuEligible {
			continue
		}
		description := strings.TrimSpace(item.DisplayName)
		if description == "" {
			description = strings.TrimSpace(item.Description)
		}
		for _, trigger := range append([]string{item.Name}, item.Aliases...) {
			if len(menu) >= 100 || !commandregistry.TelegramMenuTriggerEligible(trigger) {
				continue
			}
			menu = append(menu, BotCommand{Command: strings.TrimPrefix(trigger, "/"), Description: truncateRunes(description, 256)})
		}
	}
	ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
	defer cancel()
	if err := s.adapter.SyncCommands(ctx, menu); err != nil && s.logger != nil {
		s.logger.Printf("Telegram command menu sync failed: %s", errorCategory(err))
	}
}

func (s *Service) Stop(ctx context.Context) error {
	err := s.adapter.Stop(ctx)
	s.clearTransient()
	s.broker.Publish(events.TelegramStopped, map[string]any{})
	s.publishChannelState(events.ChannelStatusChanged, "")
	return err
}

func (s *Service) Close(ctx context.Context) error {
	err := s.Stop(ctx)
	if clearErr := s.adapter.DeleteToken(ctx); err == nil && clearErr != nil {
		err = clearErr
	}
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
	if err := s.adapter.DeleteToken(ctx); err != nil {
		return err
	}
	s.clearTransient()
	s.broker.Publish(events.TelegramTokenDeleted, map[string]any{})
	s.publishChannelState(events.ChannelStatusChanged, "")
	return nil
}

// BindingDeleted is called by the local API after a Telegram binding is
// removed outside the adapter. It revokes in-memory delivery rights before a
// terminal Turn event can send content to the old address.
func (s *Service) BindingDeleted(binding bindings.Binding) {
	if !strings.EqualFold(binding.ChannelType, "telegram") || !s.ownsBinding(binding) {
		return
	}
	s.clearAddress(channels.ChannelAddress{ChannelType: "telegram", ChannelProfileID: s.channelProfile(), AccountID: binding.AccountID, ConversationType: binding.ConversationType, ChatID: binding.ChatID, TopicID: binding.TopicID})
	s.refreshBindingSummary()
}

func (s *Service) BindingCreated(binding bindings.Binding) {
	if strings.EqualFold(binding.ChannelType, "telegram") && s.ownsBinding(binding) {
		s.refreshBindingSummary()
	}
}

func (s *Service) HandleMessage(ctx context.Context, message channels.InboundMessage) {
	message.Address.ChannelProfileID = s.channelProfile()
	s.mu.Lock()
	if s.reconfiguring {
		s.mu.Unlock()
		s.publishMessageEvent(events.TelegramMessageRejected, message, "reconfiguring")
		return
	}
	s.activeHandlers++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeHandlers--
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Printf("telegram update handler recovered updateId=%d", message.UpdateID)
		}
	}()
	s.publishMessageEvent(events.TelegramMessageReceived, message, "received")
	if message.Action != "" {
		s.handleCallback(ctx, message)
		return
	}
	text := strings.TrimSpace(message.Text)
	if text == "" {
		s.reject(ctx, message, "Only text messages are supported.", "unsupported")
		return
	}
	if s.handleNumbered(ctx, message, text) {
		return
	}
	if strings.HasPrefix(text, "/") {
		s.handleCommand(ctx, message, text)
		return
	}
	if s.answerFreeText(ctx, message, text) {
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
			if s.answerFreeTextForThread(ctx, message, content, session.Key) {
				return true
			}
			s.startOpenClawTurnForKey(ctx, message, content, session.Key)
			return true
		}
		if record.Backend != conversation.BackendCodex {
			s.send(ctx, message.Address, "该编号的会话后端不可用。")
			return true
		}
		if s.answerFreeTextForThread(ctx, message, content, record.TargetID) {
			return true
		}
		s.startCodexTurnForThread(ctx, message, content, record.TargetID)
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
	if s.answerFreeTextForThread(ctx, message, prefix.Content, session.Key) {
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
		s.send(ctx, message.Address, "Unknown command. Use /help.")
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
			s.send(ctx, message.Address, fmt.Sprintf("CloudLight Codex Bridge is ready. This chat/topic is bound to %s [%s].\nUse /help to list commands.", backend, shortID(label)))
		} else {
			s.send(ctx, message.Address, "CloudLight Codex Bridge is ready. This chat/topic is not bound yet.\nUse /threads to choose a session, or /bind <number or ID>.\nUse /help to list commands.")
		}
	case commandregistry.ActionThreadBind:
		if argument == "" {
			s.send(ctx, message.Address, "Usage: /bind <number or ID>. For OpenClaw you can also use /bind oc:<SessionKey>. /threads lists the current backend.")
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
			s.send(ctx, message.Address, "请使用 #编号 /cancel 指定等待输入的会话。")
		}
	case commandregistry.ActionInteractionCancel:
		if argument != "" {
			s.commandThread(ctx, message, argument, "cancel")
		} else {
			s.send(ctx, message.Address, "请使用 #编号 /cancel 指定等待输入的会话。")
		}
	default:
		s.send(ctx, message.Address, "Unknown command. Use /help.")
	}
}

func (s *Service) listThreads(ctx context.Context, message channels.InboundMessage) {
	if binding, ok := s.findBinding(message.Address); ok && bindingBackend(binding) == conversation.BackendOpenClaw {
		s.listOpenClawSessions(ctx, message)
		return
	}
	threads, err := s.control.ListThreads(ctx, 10, "")
	if err != nil {
		s.send(ctx, message.Address, "Could not list Codex Threads. Check that Codex is connected.")
		return
	}
	if len(threads.Threads) == 0 {
		s.send(ctx, message.Address, "No recent Codex Threads were found.")
		return
	}
	var text strings.Builder
	text.WriteString("Recent [Codex] Threads:\n")
	store := conversationregistry.ForAny(s.registry)
	rows := make([]channels.ActionRow, 0, len(threads.Threads))
	for _, thread := range threads.Threads {
		label := projectName(thread.CWD) + " · " + shortID(thread.ThreadID)
		if title := truncateRunes(strings.TrimSpace(thread.Title), 36); title != "" {
			label += " · " + title
		}
		number := thread.Number
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendCodex, thread.ThreadID); ok {
				number = record.Number
			}
		}
		text.WriteString(fmt.Sprintf("• [Codex] #%d %s\n  Updated: %s\n", number, label, displayTime(thread.UpdatedAt)))
		token := s.newCallback(callbackAction{Kind: "bind", Address: message.Address, UserID: message.UserID, ThreadID: thread.ThreadID, Expires: time.Now().Add(5 * time.Minute)})
		rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: "Bind " + shortID(thread.ThreadID), Value: token}}})
	}
	_, _ = s.sendOutbound(ctx, channels.OutboundMessage{Address: message.Address, Text: strings.TrimSpace(text.String()), Actions: rows})
}

func (s *Service) listOpenClawSessions(ctx context.Context, message channels.InboundMessage) {
	backend := s.openClawBackend()
	if backend == nil {
		s.send(ctx, message.Address, "The current backend is OpenClaw, but its Gateway is not configured.")
		return
	}
	sessions, err := backend.ListSessions(ctx, 20)
	if err != nil {
		s.send(ctx, message.Address, "Could not list OpenClaw Sessions. Check the Gateway connection.")
		return
	}
	if len(sessions) == 0 {
		s.send(ctx, message.Address, "No OpenClaw Sessions are available.")
		return
	}
	var output strings.Builder
	output.WriteString("[OpenClaw] Current backend Sessions:\n")
	store := conversationregistry.ForAny(s.registry)
	for _, session := range sessions {
		number := session.Number
		if store != nil {
			if record, ok := store.ByTarget(conversationregistry.BackendOpenClaw, session.Key); ok {
				number = record.Number
			}
		}
		fmt.Fprintf(&output, "\n• [OpenClaw] #%d %s · %s\n  Session: %s", number, displayTitle(session.Title), firstNonEmpty(session.Status, "idle"), session.Key)
	}
	output.WriteString("\n\nRebind with: /bind openclaw <Session Key>")
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
		s.send(ctx, message.Address, "没有可由当前 Telegram 用户停止的该会话任务。")
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
			Backend: conversation.BackendOpenClaw, TargetID: detail.Key, ChannelType: "telegram", ChannelProfileID: message.Address.ChannelProfileID,
			ResourceID: message.Address.ChannelProfileID, AccountID: message.Address.AccountID,
			ConversationType: "default", ConversationID: message.Address.ChatID, ChatID: message.Address.ChatID, TopicID: message.Address.TopicID,
			ThreadID: detail.Key, SessionKey: detail.Key,
		})
		if err != nil {
			s.send(ctx, message.Address, "当前 Telegram Profile 未分配给 OpenClaw，无法创建该绑定。")
			return
		}
		s.clearAddress(message.Address)
		created, previous, err := s.bindings.UpsertAddress(input)
		if err != nil {
			s.send(ctx, message.Address, "保存 OpenClaw 绑定失败。")
			return
		}
		s.refreshBindingSummary()
		payload := safeBindingPayload(created)
		if previous != nil {
			payload["replacedSessionKey"] = shortID(previous.SessionKey)
		}
		s.broker.Publish(events.BindingCreated, payload)
		s.send(ctx, message.Address, fmt.Sprintf("已绑定到 [OpenClaw] %s（Session：%s）。", displayTitle(detail.Title), detail.Key))
		return
	}
	threadID := target
	if !resolvedByRegistry {
		if store := conversationregistry.ForAny(s.registry); store != nil {
			selector := strings.TrimSpace(strings.Trim(threadID, "#[]"))
			if number, err := strconv.Atoi(selector); err == nil {
				if record, ok := store.ByNumber(number); ok && record.Backend == conversationregistry.BackendCodex {
					threadID = record.TargetID
				}
			}
		}
	}
	thread, err := s.control.ReadThread(ctx, threadID, false)
	if err != nil || thread.ThreadID == "" || thread.ThreadID != threadID {
		s.send(ctx, message.Address, "That full Thread ID does not exist.")
		return
	}
	if thread.Archived != nil && *thread.Archived {
		s.send(ctx, message.Address, "That Thread is archived and cannot be bound.")
		return
	}
	input, err := s.prepareBinding(bindings.CreateRequest{
		Backend: conversation.BackendCodex, TargetID: threadID, ChannelType: "telegram", ChannelProfileID: message.Address.ChannelProfileID,
		ResourceID: message.Address.ChannelProfileID, AccountID: message.Address.AccountID, ConversationType: "default", ConversationID: message.Address.ChatID, ChatID: message.Address.ChatID,
		TopicID: message.Address.TopicID, ThreadID: threadID,
	})
	if err != nil {
		s.send(ctx, message.Address, "当前 Telegram Profile 未分配给 Codex，无法创建该绑定。")
		return
	}
	s.clearAddress(message.Address)
	created, previous, err := s.bindings.UpsertAddress(input)
	if err != nil {
		s.send(ctx, message.Address, "Could not save the binding.")
		return
	}
	s.refreshBindingSummary()
	payload := safeBindingPayload(created)
	if previous != nil {
		payload["replacedThreadId"] = shortID(previous.ThreadID)
	}
	s.broker.Publish(events.BindingCreated, payload)
	verb := "Bound"
	if previous != nil && previous.ThreadID != threadID {
		verb = "Replaced the previous binding with"
	}
	s.send(ctx, message.Address, fmt.Sprintf("%s %s · %s (%s).", verb, projectName(thread.CWD), shortID(threadID), thread.Status))
}

func (s *Service) unbind(ctx context.Context, message channels.InboundMessage) {
	s.clearAddress(message.Address)
	deleted, err := s.bindings.DeleteProfileAddress("telegram", message.Address.ChannelProfileID, telegramConversationType(message.Address.ConversationType), message.Address.ChatID, message.Address.TopicID)
	if errors.Is(err, bindings.ErrNotFound) {
		s.send(ctx, message.Address, "This chat/topic is not bound.")
		return
	}
	if err != nil {
		s.send(ctx, message.Address, "Could not remove the binding.")
		return
	}
	s.refreshBindingSummary()
	s.broker.Publish(events.BindingDeleted, safeBindingPayload(deleted))
	s.send(ctx, message.Address, "Binding removed. Pending Telegram input for this address was cleared.")
}

func (s *Service) current(ctx context.Context, message channels.InboundMessage) {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.send(ctx, message.Address, "This chat/topic is not bound. Use /threads or /bind.")
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
		s.send(ctx, message.Address, fmt.Sprintf("当前绑定：OpenClaw #%d\nCurrent binding\nBackend: OpenClaw\nProfile: %s\nSession: #%d %s\nSessionKey: %s\nUpdated: %s\nState: %s", number, firstNonEmpty(binding.ChannelProfileID, "default"), number, displayTitle(detail.Title), detail.Key, displayTime(detail.UpdatedAt), firstNonEmpty(detail.Status, "idle")))
		return
	}
	thread, err := control.ReadThreadHistory(ctx, s.control, binding.ThreadID, 1)
	if err != nil || thread.ThreadID == "" {
		s.send(ctx, message.Address, "Bound Thread "+shortID(binding.ThreadID)+" is no longer available. Use /unbind or bind another Thread.")
		return
	}
	number := thread.Number
	if store := conversationregistry.ForAny(s.registry); store != nil {
		if record, ok := store.ByTarget(conversationregistry.BackendCodex, thread.ThreadID); ok {
			number = record.Number
		}
	}
	s.send(ctx, message.Address, fmt.Sprintf("当前绑定：Codex #%d\nCurrent binding\nBackend: Codex\nProfile: %s\nTitle: %s\nThread: %s\nProject: %s\nUpdated: %s\nState: %s\nLatest Turn: %s",
		number, firstNonEmpty(binding.ChannelProfileID, "default"), displayTitle(thread.Title), shortID(thread.ThreadID), projectName(thread.CWD), displayTime(thread.UpdatedAt), thread.Runtime.State, latestTurnStatus(thread)))
}

func (s *Service) status(ctx context.Context, message channels.InboundMessage) {
	bridge := s.runtime.Status()
	lines := []string{"Bridge: running"}
	if bridge.AppServerRunning {
		lines = append(lines, "Codex App Server: connected")
	} else {
		lines = append(lines, "Codex App Server: unavailable")
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
		lines = append(lines, "Binding: none", "Use /threads or /bind <full-thread-id>.")
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
	thread, err := s.control.ReadThread(ctx, binding.ThreadID, false)
	if err != nil || thread.ThreadID == "" {
		lines = append(lines, "Binding: "+shortID(binding.ThreadID), "Thread: unavailable or deleted")
		s.send(ctx, message.Address, strings.Join(lines, "\n"))
		return
	}
	state := thread.Runtime
	lastResult := state.State
	if state.Persistence != nil && state.Persistence.Status != "" {
		lastResult = state.Persistence.Status
	}
	lines = append(lines,
		"Thread: "+displayTitle(thread.Title)+" · "+shortID(thread.ThreadID),
		"Project: "+projectName(thread.CWD),
		"State: "+state.State,
		"Latest result: "+lastResult,
		fmt.Sprintf("Waiting for user input: %t", state.PendingInteractionCount > 0),
	)
	if state.Persistence != nil && state.Persistence.Status == "persisted" {
		lines = append(lines, "The message is persisted; Codex Desktop may need a restart to show content written by another App Server.")
	}
	s.send(ctx, message.Address, strings.Join(lines, "\n"))
}

func (s *Service) stopTurn(ctx context.Context, message channels.InboundMessage) {
	binding, ok := s.findBinding(message.Address)
	if !ok {
		s.send(ctx, message.Address, "This chat/topic is not bound.")
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
	owned := route != nil && sameAddress(route.Address, message.Address) && route.UserID == message.UserID
	s.mu.Unlock()
	if !owned || !state.CanInterrupt {
		s.send(ctx, message.Address, "There is no controllable Turn started from this Telegram address.")
		return
	}
	if _, err := s.runtime.InterruptTurn(ctx, binding.ThreadID, state.TurnID); err != nil {
		s.send(ctx, message.Address, "Could not interrupt the Turn; it may already be finishing.")
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
		s.send(ctx, message.Address, "没有可由当前 Telegram 会话停止的 OpenClaw 任务。")
		return
	}
	if _, err := backend.Abort(ctx, key, runID); err != nil {
		s.send(ctx, message.Address, "OpenClaw 停止请求失败；任务可能已经结束。")
		return
	}
	s.send(ctx, message.Address, "已请求停止 OpenClaw 任务。")
}

func (s *Service) startTurn(ctx context.Context, message channels.InboundMessage, text string, target ...string) {
	if s.backendForMessage(message) == conversation.BackendOpenClaw {
		key := ""
		if len(target) > 0 {
			key = strings.TrimSpace(target[0])
		} else if binding, ok := s.findBinding(message.Address); ok {
			key = firstNonEmpty(binding.SessionKey, binding.ThreadID)
		}
		if key == "" {
			s.reject(ctx, message, "请指定 OpenClaw Session 编号，例如：\n\n#12 继续处理\n\n发送 /threads 查看编号。", "unbound")
			return
		}
		s.startOpenClawTurnForKey(ctx, message, text, key)
		return
	}
	if len(target) == 0 {
		if binding, ok := s.findBinding(message.Address); ok && bindingBackend(binding) == conversation.BackendOpenClaw {
			s.startOpenClawTurn(ctx, message, text, binding)
			return
		}
	}
	threadID := ""
	if len(target) > 0 {
		threadID = strings.TrimSpace(target[0])
	} else if binding, ok := s.findBinding(message.Address); ok {
		threadID = binding.ThreadID
	}
	if threadID == "" {
		s.reject(ctx, message, "请指定聊天编号，例如：\n\n#12 修复这个Bug\n\n发送 /threads 查看编号。", "unbound")
		return
	}
	s.startCodexTurnForThread(ctx, message, text, threadID)
}

// startCodexTurnForThread is the backend-explicit path used by global-number
// routing. It must not consult the current profile, because a global number
// may intentionally target Codex while the current profile is OpenClaw.
func (s *Service) startCodexTurnForThread(ctx context.Context, message channels.InboundMessage, text, threadID string) {
	thread, err := s.control.ReadThread(ctx, threadID, false)
	if err != nil || thread.ThreadID == "" {
		s.reject(ctx, message, "The bound Codex Thread no longer exists. Bind another Thread.", "missing-thread")
		return
	}
	if thread.Archived != nil && *thread.Archived {
		s.reject(ctx, message, "The bound Codex Thread is archived. Bind another Thread.", "archived")
		return
	}
	state := s.runtime.RuntimeState(threadID)
	if !state.CanSend || state.PendingInteractionCount > 0 {
		s.reject(ctx, message, "This Thread cannot accept a new task. Current state: "+state.State+". Finish the current interaction or wait for the active Turn.", "busy")
		return
	}
	accepted, err := s.runtime.StartTurn(ctx, threadID, control.StartTurnRequest{Text: text, CollaborationMode: "default", Origin: control.TurnOriginTelegram})
	if err != nil {
		s.reject(ctx, message, "Codex could not start this Turn. The Thread may be busy or unavailable.", "start-failed")
		s.publishMessageEvent(events.TelegramMessageRejected, message, "start-failed")
		return
	}
	route := &turnRoute{Address: message.Address, UserID: message.UserID, Backend: conversation.BackendCodex, SessionKey: accepted.ThreadID, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID}
	s.mu.Lock()
	s.routes[routeMapKey(route.Backend, route.SessionKey, route.TurnID)] = route
	s.mu.Unlock()
	s.publishMessageEvent(events.TelegramMessageRouted, message, "turn-started")
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
	s.publishMessageEvent(events.TelegramMessageRouted, message, "openclaw-turn-started")
}

func (s *Service) answerFreeText(ctx context.Context, message channels.InboundMessage, text string) bool {
	s.mu.Lock()
	var wait inputWait
	ok := false
	seen := map[string]bool{}
	for _, candidate := range s.waits {
		if sameAddress(candidate.Address, message.Address) && candidate.UserID == message.UserID && !seen[candidate.InteractionID] {
			seen[candidate.InteractionID] = true
			if ok {
				s.mu.Unlock()
				s.send(ctx, message.Address, "存在多个等待回答的会话，请使用 #编号 回答。")
				return true
			}
			wait = candidate
			ok = true
		}
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	if time.Now().After(wait.Expires) {
		s.clearInteraction(wait.InteractionID)
		s.send(ctx, message.Address, "That input request expired. Return to WPF if Codex still needs an answer.")
		return true
	}
	if !sameAddress(wait.Address, message.Address) || wait.UserID != message.UserID {
		return false
	}
	s.acceptInteractionAnswer(ctx, wait.InteractionID, wait.QuestionID, []string{text})
	return true
}

func (s *Service) answerFreeTextForThread(ctx context.Context, message channels.InboundMessage, text, threadID string) bool {
	s.mu.Lock()
	var wait inputWait
	ok := false
	for _, candidate := range s.waits {
		if candidate.ThreadID == threadID && sameAddress(candidate.Address, message.Address) && candidate.UserID == message.UserID {
			wait = candidate
			ok = true
			break
		}
	}
	s.mu.Unlock()
	if !ok {
		return false
	}
	if time.Now().After(wait.Expires) {
		s.clearInteraction(wait.InteractionID)
		s.send(ctx, message.Address, "该输入请求已经过期。")
		return true
	}
	s.acceptInteractionAnswer(ctx, wait.InteractionID, wait.QuestionID, []string{text})
	return true
}

func (s *Service) handleCallback(ctx context.Context, message channels.InboundMessage) {
	s.mu.Lock()
	action, ok := s.callbacks[message.Action]
	if ok {
		delete(s.callbacks, message.Action)
	}
	s.mu.Unlock()
	if !ok || time.Now().After(action.Expires) || !sameAddress(action.Address, message.Address) || action.UserID != message.UserID {
		_ = s.adapter.AnswerCallback(ctx, message.CallbackID, "该问题已经处理或已经过期。")
		return
	}
	switch action.Kind {
	case "bind":
		_ = s.adapter.AnswerCallback(ctx, message.CallbackID, "Binding…")
		s.bind(ctx, message, action.ThreadID)
	case "flow-answer":
		_ = s.adapter.AnswerCallback(ctx, message.CallbackID, "Submitting…")
		s.acceptInteractionAnswer(ctx, action.InteractionID, action.QuestionID, []string{action.Value})
	case "toggle":
		_ = s.adapter.AnswerCallback(ctx, message.CallbackID, "Selection updated")
		s.toggleMulti(ctx, action)
	case "submit-multi":
		_ = s.adapter.AnswerCallback(ctx, message.CallbackID, "Submitting…")
		s.submitMulti(ctx, action)
	}
}

func (s *Service) eventLoop() {
	defer close(s.done)
	channel, unsubscribe := s.broker.Subscribe()
	defer unsubscribe()
	work := make(chan events.Event, 128)
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for event := range work {
			s.handleEvent(event)
		}
	}()
	defer func() {
		close(work)
		worker.Wait()
	}()
	cleanup := time.NewTicker(time.Minute)
	defer cleanup.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-cleanup.C:
			s.expireTransient()
		case event, open := <-channel:
			if !open {
				return
			}
			if !telegramRelevantEvent(event.EventType) {
				continue
			}
			select {
			case work <- event:
			case <-s.ctx.Done():
				return
			}
		}
	}
}

func telegramRelevantEvent(eventType string) bool {
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
		// Keep channel routes alive across a Gateway reconnect. The run may still
		// finish on the Gateway and the next connection will deliver its terminal
		// event; only application shutdown revokes the route.
		return
	}
	if event.EventType == events.InteractionRequested {
		s.handleInteractionEvent(event)
		return
	}
	if event.EventType == events.InteractionResolved {
		if interactionID := interactionIDFromPayload(event.Payload); interactionID != "" {
			s.clearInteraction(interactionID)
		}
		return
	}
	s.mu.Lock()
	route := s.routeForEventLocked(event)
	s.mu.Unlock()
	if route == nil || event.ThreadID != route.ThreadID {
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
	if !ok || !strings.EqualFold(task.ChannelType, "telegram") || (task.ChannelProfileID != "" && task.ChannelProfileID != profileID) {
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
// guarded Telegram flow used by ordinary bound turns. This keeps multi-question
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

func (s *Service) finishAllRoutes(_ string) {
	s.mu.Lock()
	s.routes = make(map[string]*turnRoute)
	s.waits = make(map[string]inputWait)
	s.callbacks = make(map[string]callbackAction)
	s.sessions = make(map[string]*multiSession)
	s.flows = make(map[string]*interactionFlow)
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
	interactionIDs := make(map[string]struct{})
	rememberInteraction := func(interactionID string) {
		if interactionID != "" {
			interactionIDs[interactionID] = struct{}{}
		}
	}
	for key, wait := range s.waits {
		if wasRemoved(wait.TurnID) {
			rememberInteraction(wait.InteractionID)
			delete(s.waits, key)
		}
	}
	for token, action := range s.callbacks {
		if wasRemoved(action.TurnID) {
			rememberInteraction(action.InteractionID)
			delete(s.callbacks, token)
		}
	}
	for id, session := range s.sessions {
		if wasRemoved(session.TurnID) {
			rememberInteraction(session.InteractionID)
			delete(s.sessions, id)
		}
	}
	for id, flow := range s.flows {
		if flow != nil && wasRemoved(flow.TurnID) {
			rememberInteraction(flow.InteractionID)
			delete(s.flows, id)
		}
	}
	for interactionID := range interactionIDs {
		delete(s.interactionNotified, interactionID)
	}
	if backend == conversation.BackendCodex {
		// interactionNotified only de-duplicates Codex interaction events. A
		// disconnected Codex backend may replay a still-pending approval after
		// it reconnects, so retaining an unassociated notification marker would
		// suppress the renewed prompt.
		s.interactionNotified = make(map[string]bool)
	}
	s.mu.Unlock()
}

func (s *Service) handleInteractionEvent(event events.Event) {
	s.mu.Lock()
	route := s.routeForEventLocked(event)
	s.mu.Unlock()
	if route == nil || route.ThreadID != event.ThreadID {
		return
	}
	interactionID := ""
	if raw, ok := event.Payload["interaction"].(interactions.PendingInteraction); ok {
		interactionID = raw.ID
	} else if raw, ok := event.Payload["interaction"].(map[string]any); ok {
		interactionID, _ = raw["id"].(string)
	}
	if interactionID == "" {
		return
	}
	interaction, found := s.runtime.GetInteraction(interactionID)
	if !found || interaction.ThreadID != route.ThreadID || interaction.TurnID != route.TurnID {
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
		s.send(ctx, route.Address, taskInteractionPrefix(route.TaskNumber)+"Codex needs approval. For safety, approve or deny this request in the WPF app.")
		return
	}
	if len(interaction.Questions) == 0 {
		s.send(ctx, route.Address, taskInteractionPrefix(route.TaskNumber)+"Codex requested input, but this question must be handled in WPF.")
		return
	}
	expires, err := time.Parse(time.RFC3339Nano, interaction.ExpiresAt)
	if err != nil {
		expires = time.Now().Add(5 * time.Minute)
	}
	s.mu.Lock()
	if _, exists := s.flows[interaction.ID]; exists {
		s.mu.Unlock()
		return
	}
	s.flows[interaction.ID] = &interactionFlow{
		Expires: expires, Address: route.Address, UserID: route.UserID, ThreadID: route.ThreadID,
		TurnID: route.TurnID, TaskNumber: route.TaskNumber, InteractionID: interaction.ID, Questions: append([]interactions.Question(nil), interaction.Questions...), Answers: make(map[string][]string),
	}
	s.mu.Unlock()
	s.presentInteractionQuestion(ctx, interaction.ID)
}

func (s *Service) presentInteractionQuestion(ctx context.Context, interactionID string) {
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
	address, userID, threadID, turnID, expires, taskNumber := flow.Address, flow.UserID, flow.ThreadID, flow.TurnID, flow.Expires, flow.TaskNumber
	s.mu.Unlock()
	switch question.Type {
	case "single-choice":
		rows := []channels.ActionRow{}
		for _, option := range question.Options {
			token := s.newCallback(callbackAction{Kind: "flow-answer", Address: address, UserID: userID, ThreadID: threadID, TurnID: turnID, InteractionID: interactionID, QuestionID: question.ID, Value: option.Value, Expires: expires})
			rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: truncateRunes(option.Label, 54), Value: token}}})
		}
		_, _ = s.sendOutbound(ctx, channels.OutboundMessage{Address: address, Text: taskQuestionPrompt(taskNumber, question), Actions: rows})
	case "multiple-choice":
		sessionID := randomToken()
		session := &multiSession{Expires: expires, Address: address, UserID: userID, ThreadID: threadID, TurnID: turnID, TaskNumber: taskNumber, InteractionID: interactionID, Question: question, Selected: make(map[string]bool)}
		s.mu.Lock()
		s.sessions[sessionID] = session
		s.mu.Unlock()
		s.renderMulti(ctx, sessionID, session)
	default:
		s.mu.Lock()
		wait := inputWait{Expires: expires, Address: address, UserID: userID, ThreadID: threadID, TurnID: turnID, InteractionID: interactionID, QuestionID: question.ID}
		s.waits[waitKey(address, userID, threadID)] = wait
		s.waits[waitKey(address, userID)] = wait
		s.mu.Unlock()
		s.send(ctx, address, taskQuestionPrompt(taskNumber, question)+"\nReply with your next text message.")
	}
}

func (s *Service) renderMulti(ctx context.Context, sessionID string, session *multiSession) {
	s.mu.Lock()
	for token, action := range s.callbacks {
		if action.SessionID == sessionID {
			delete(s.callbacks, token)
		}
	}
	s.mu.Unlock()
	rows := []channels.ActionRow{}
	for _, option := range session.Question.Options {
		label := "☐ " + option.Label
		if session.Selected[option.Value] {
			label = "☑ " + option.Label
		}
		token := s.newCallback(callbackAction{Kind: "toggle", Address: session.Address, UserID: session.UserID, ThreadID: session.ThreadID, TurnID: session.TurnID, InteractionID: session.InteractionID, QuestionID: session.Question.ID, Value: option.Value, SessionID: sessionID, Expires: session.Expires})
		rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: truncateRunes(label, 54), Value: token}}})
	}
	submit := s.newCallback(callbackAction{Kind: "submit-multi", Address: session.Address, UserID: session.UserID, ThreadID: session.ThreadID, TurnID: session.TurnID, InteractionID: session.InteractionID, QuestionID: session.Question.ID, SessionID: sessionID, Expires: session.Expires})
	rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: "Submit", Value: submit}}})
	message := channels.OutboundMessage{Address: session.Address, Text: taskQuestionPrompt(session.TaskNumber, session.Question), Actions: rows}
	if session.MessageID == "" {
		result, err := s.sendOutbound(ctx, message)
		if err == nil {
			session.MessageID = result.MessageID
		}
	} else {
		_, _ = s.editOutbound(ctx, session.MessageID, message)
	}
}

func (s *Service) toggleMulti(ctx context.Context, action callbackAction) {
	s.mu.Lock()
	session := s.sessions[action.SessionID]
	if session != nil {
		session.Selected[action.Value] = !session.Selected[action.Value]
	}
	s.mu.Unlock()
	if session == nil || time.Now().After(session.Expires) {
		return
	}
	s.renderMulti(ctx, action.SessionID, session)
}

func (s *Service) submitMulti(ctx context.Context, action callbackAction) {
	s.mu.Lock()
	session := s.sessions[action.SessionID]
	s.mu.Unlock()
	if session == nil || time.Now().After(session.Expires) {
		return
	}
	answers := []string{}
	for _, option := range session.Question.Options {
		if session.Selected[option.Value] {
			answers = append(answers, option.Value)
		}
	}
	if session.Question.Required && len(answers) == 0 {
		s.send(ctx, session.Address, "Choose at least one option before submitting.")
		s.renderMulti(ctx, action.SessionID, session)
		return
	}
	s.mu.Lock()
	delete(s.sessions, action.SessionID)
	s.mu.Unlock()
	s.acceptInteractionAnswer(ctx, session.InteractionID, session.Question.ID, answers)
}

func (s *Service) acceptInteractionAnswer(ctx context.Context, interactionID, questionID string, answers []string) {
	s.mu.Lock()
	flow := s.flows[interactionID]
	if flow == nil || flow.Index >= len(flow.Questions) || time.Now().After(flow.Expires) || flow.Questions[flow.Index].ID != questionID {
		s.mu.Unlock()
		s.clearInteraction(interactionID)
		if flow != nil {
			s.send(ctx, flow.Address, "该问题已经处理或已经过期。")
		}
		return
	}
	flow.Answers[questionID] = append([]string(nil), answers...)
	flow.Index++
	complete := flow.Index >= len(flow.Questions)
	address := flow.Address
	turnID := flow.TurnID
	requestAnswers := make(map[string][]string, len(flow.Answers))
	for id, values := range flow.Answers {
		requestAnswers[id] = append([]string(nil), values...)
	}
	s.mu.Unlock()
	s.clearQuestionControls(interactionID)
	if !complete {
		s.presentInteractionQuestion(ctx, interactionID)
		return
	}
	_, err := s.runtime.RespondInteraction(ctx, interactionID, interactions.ResponseRequest{Action: "submit", Answers: requestAnswers})
	if err != nil {
		s.mu.Lock()
		if retry := s.flows[interactionID]; retry != nil && retry.Index > 0 {
			retry.Index--
		}
		s.mu.Unlock()
		s.send(ctx, address, "The input could not be submitted; it may have expired or been resolved elsewhere.")
		if _, pending := s.runtime.GetInteraction(interactionID); pending {
			s.presentInteractionQuestion(ctx, interactionID)
		} else {
			s.clearInteraction(interactionID)
		}
		return
	}
	s.clearInteraction(interactionID)
	_ = turnID
}

func (s *Service) clearQuestionControls(interactionID string) {
	s.mu.Lock()
	for token, action := range s.callbacks {
		if action.InteractionID == interactionID {
			delete(s.callbacks, token)
		}
	}
	for key, wait := range s.waits {
		if wait.InteractionID == interactionID {
			delete(s.waits, key)
		}
	}
	for id, session := range s.sessions {
		if session.InteractionID == interactionID {
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()
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
	for key, wait := range s.waits {
		if wait.TurnID == turnID {
			delete(s.waits, key)
		}
	}
	for id, action := range s.callbacks {
		if action.TurnID == turnID {
			delete(s.callbacks, id)
		}
	}
	for id, session := range s.sessions {
		if session.TurnID == turnID {
			delete(s.sessions, id)
		}
	}
	for id, flow := range s.flows {
		if flow.TurnID == turnID {
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
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

func (s *Service) routeForEventLocked(event events.Event) *turnRoute {
	backend := eventBackend(event)
	if route := s.routes[routeMapKey(backend, event.ThreadID, event.TurnID)]; route != nil {
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

func (s *Service) findBinding(address channels.ChannelAddress) (bindings.Binding, bool) {
	owner := strings.TrimSpace(address.ChannelProfileID)
	if owner == "" {
		owner = address.AccountID
	}
	binding, ok := s.bindings.FindProfileAddress("telegram", owner, telegramConversationType(address.ConversationType), address.ChatID, address.TopicID)
	if !ok && address.ChannelProfileID == "" {
		binding, ok = s.bindings.FindAddress("telegram", address.AccountID, telegramConversationType(address.ConversationType), address.ChatID, address.TopicID)
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

// /bind accepts an explicit backend, the legacy oc:/openclaw: selector, or a
// global number. Only an unprefixed non-numeric ID falls back to the current
// profile backend.
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

func (s *Service) send(ctx context.Context, address channels.ChannelAddress, text string) {
	for _, part := range SplitMessage(text, 3900) {
		_, _ = s.sendOutbound(ctx, channels.OutboundMessage{Address: address, Text: part})
	}
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

func (s *Service) sendOutbound(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	result, err := s.adapter.SendMessage(ctx, message)
	if err == nil {
		s.broker.Publish(events.MessageSent, map[string]any{
			"channelType": "telegram", "chat": maskID(message.Address.ChatID), "topic": maskID(message.Address.TopicID),
			"messageId": shortID(result.MessageID), "length": utf8.RuneCountInString(message.Text), "operation": "send",
		})
	}
	return result, err
}

func (s *Service) editOutbound(ctx context.Context, messageID string, message channels.OutboundMessage) (channels.OutboundResult, error) {
	result, err := s.adapter.EditMessage(ctx, messageID, message)
	if err == nil {
		s.broker.Publish(events.MessageSent, map[string]any{
			"channelType": "telegram", "chat": maskID(message.Address.ChatID), "topic": maskID(message.Address.TopicID),
			"messageId": shortID(messageID), "length": utf8.RuneCountInString(message.Text), "operation": "edit",
		})
	}
	return result, err
}

func (s *Service) reject(ctx context.Context, message channels.InboundMessage, text, reason string) {
	s.send(ctx, message.Address, text)
	s.publishMessageEvent(events.TelegramMessageRejected, message, reason)
}

func (s *Service) publishMessageEvent(eventType string, message channels.InboundMessage, result string) {
	threadID := ""
	if binding, ok := s.findBinding(message.Address); ok {
		threadID = shortID(binding.ThreadID)
	}
	s.broker.Publish(eventType, map[string]any{
		"updateId": message.UpdateID, "chat": maskID(message.Address.ChatID), "user": maskID(message.UserID),
		"threadId": threadID, "length": utf8.RuneCountInString(message.Text), "routeResult": result,
	})
}

func (s *Service) refreshBindingSummary() {
	status := s.adapter.TelegramStatus()
	profileID := s.channelProfile()
	if status.BotID == "" && profileID == "" {
		s.adapter.SetBindingSummary(nil)
		return
	}
	var items []bindings.Binding
	if profileID != "" {
		items = s.bindings.ListChannelProfile("telegram", profileID)
	} else {
		items = s.bindings.ListChannelAccount("telegram", status.BotID)
	}
	summaries := make([]string, 0, len(items))
	for _, item := range items {
		summaries = append(summaries, maskID(item.ChatID)+"/"+maskID(item.TopicID)+"→"+shortID(item.ThreadID))
	}
	s.adapter.SetBindingSummary(summaries)
}

func (s *Service) newCallback(action callbackAction) string {
	token := "cb:" + randomToken()
	s.mu.Lock()
	s.callbacks[token] = action
	s.mu.Unlock()
	return token
}

func (s *Service) clearTransient() {
	s.mu.Lock()
	s.routes = make(map[string]*turnRoute)
	s.callbacks = make(map[string]callbackAction)
	s.waits = make(map[string]inputWait)
	s.sessions = make(map[string]*multiSession)
	s.flows = make(map[string]*interactionFlow)
	s.interactionNotified = make(map[string]bool)
	s.mu.Unlock()
}

func (s *Service) clearTurnInput(turnID string) {
	s.mu.Lock()
	for key, wait := range s.waits {
		if wait.TurnID == turnID {
			delete(s.waits, key)
		}
	}
	for token, action := range s.callbacks {
		if action.TurnID == turnID {
			delete(s.callbacks, token)
		}
	}
	for id, session := range s.sessions {
		if session.TurnID == turnID {
			delete(s.sessions, id)
		}
	}
	for id, flow := range s.flows {
		if flow.TurnID == turnID {
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
	for key, wait := range s.waits {
		if sameAddress(wait.Address, address) {
			delete(s.waits, key)
		}
	}
	for token, action := range s.callbacks {
		if sameAddress(action.Address, address) {
			delete(s.callbacks, token)
		}
	}
	for id, session := range s.sessions {
		if sameAddress(session.Address, address) {
			delete(s.sessions, id)
		}
	}
	for id, flow := range s.flows {
		if sameAddress(flow.Address, address) {
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func (s *Service) clearInteraction(interactionID string) {
	s.mu.Lock()
	for token, action := range s.callbacks {
		if action.InteractionID == interactionID {
			delete(s.callbacks, token)
		}
	}
	for key, wait := range s.waits {
		if wait.InteractionID == interactionID {
			delete(s.waits, key)
		}
	}
	for id, session := range s.sessions {
		if session.InteractionID == interactionID {
			delete(s.sessions, id)
		}
	}
	delete(s.flows, interactionID)
	s.mu.Unlock()
}

func (s *Service) handleAdapterEvent(event AdapterEvent) {
	switch event.Kind {
	case "rate-limited":
		s.broker.Publish(events.TelegramRateLimited, map[string]any{
			"channelType": "telegram", "retryAfterSeconds": int(event.RetryAfter.Seconds()),
		})
	case "error":
		s.broker.Publish(events.TelegramPollingStopped, map[string]any{"channelType": "telegram", "category": event.Category})
		s.publishChannelState(events.ChannelError, event.Category)
		s.publishChannelState(events.ChannelDisconnected, event.Category)
		s.finishAllRoutes("Telegram polling stopped and remote control was lost. The Codex Turn will not be retried.")
	case "transient-error":
		s.publishChannelState(events.ChannelError, event.Category)
		s.publishChannelState(events.ChannelDisconnected, event.Category)
	case "recovered":
		s.publishChannelState(events.ChannelConnected, "")
	case "status":
		s.publishChannelState(events.ChannelStatusChanged, "")
	case "stopped":
		s.broker.Publish(events.TelegramPollingStopped, map[string]any{"channelType": "telegram"})
		s.publishChannelState(events.ChannelDisconnected, "")
	}
}

func (s *Service) publishChannelState(eventType, category string) {
	payload := map[string]any{"channelType": "telegram", "status": s.adapter.TelegramStatus()}
	if category != "" {
		payload["category"] = category
	}
	s.broker.Publish(eventType, payload)
}

func (s *Service) expireTransient() {
	now := time.Now()
	s.mu.Lock()
	for token, action := range s.callbacks {
		if now.After(action.Expires) {
			delete(s.callbacks, token)
		}
	}
	for key, wait := range s.waits {
		if now.After(wait.Expires) {
			delete(s.waits, key)
		}
	}
	for id, session := range s.sessions {
		if now.After(session.Expires) {
			delete(s.sessions, id)
		}
	}
	for id, flow := range s.flows {
		if now.After(flow.Expires) {
			delete(s.flows, id)
		}
	}
	s.mu.Unlock()
}

func SplitMessage(text string, limit int) []string {
	if limit < 1 {
		limit = 3900
	}
	runes := []rune(strings.TrimSpace(text))
	if len(runes) == 0 {
		return nil
	}
	result := []string{}
	for len(runes) > limit {
		cut := limit
		for index := limit; index > limit/2; index-- {
			if runes[index-1] == '\n' || runes[index-1] == ' ' {
				cut = index
				break
			}
		}
		result = append(result, string(runes[:cut]))
		runes = runes[cut:]
	}
	if len(runes) > 0 {
		result = append(result, string(runes))
	}
	return result
}

func questionPrompt(question interactions.Question) string {
	if question.Header != "" {
		return question.Header + "\n" + question.Text
	}
	return question.Text
}

func taskQuestionPrompt(number int, question interactions.Question) string {
	prompt := questionPrompt(question)
	if number < 1 {
		return prompt
	}
	return fmt.Sprintf("T%d：%s", number, prompt)
}

func taskInteractionPrefix(number int) string {
	if number < 1 {
		return ""
	}
	return fmt.Sprintf("T%d: ", number)
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

func safeBindingPayload(binding bindings.Binding) map[string]any {
	backend := bindingBackend(binding)
	return map[string]any{"bindingId": binding.ID, "backend": backend, "targetId": shortID(firstNonEmpty(binding.TargetID, binding.SessionKey, binding.ThreadID)), "sessionKey": shortID(firstNonEmpty(binding.SessionKey, binding.ThreadID)), "channelType": binding.ChannelType, "channelProfileId": binding.ChannelProfileID, "conversationType": binding.ConversationType, "conversationId": maskID(binding.ConversationID), "account": shortID(binding.AccountID), "chat": maskID(binding.ChatID), "topic": maskID(binding.TopicID), "threadId": shortID(binding.ThreadID)}
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

func projectName(cwd string) string {
	value := filepath.Base(filepath.Clean(strings.TrimSpace(cwd)))
	if value == "." || value == string(filepath.Separator) || value == "" {
		return "project"
	}
	return truncateRunes(value, 30)
}

func displayTitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return "Untitled Thread"
	}
	return truncateRunes(title, 80)
}

func displayTime(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.Local().Format("2006-01-02 15:04")
	}
	return truncateRunes(value, 32)
}

func latestTurnStatus(thread control.ThreadDetail) string {
	if len(thread.Turns) == 0 {
		if thread.Runtime.Persistence != nil && thread.Runtime.Persistence.Status != "" {
			return thread.Runtime.Persistence.Status
		}
		return "none"
	}
	return thread.Turns[len(thread.Turns)-1].Status
}

func shortID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 10 {
		return value
	}
	return value[:8] + "…"
}

func maskID(value string) string {
	if value == "" {
		return ""
	}
	negative := strings.HasPrefix(value, "-")
	digits := strings.TrimPrefix(value, "-")
	if len(digits) <= 4 {
		return "***"
	}
	prefix := ""
	if negative {
		prefix = "-"
	}
	return prefix + "***" + digits[len(digits)-4:]
}

func waitKey(address channels.ChannelAddress, userID string, threads ...string) string {
	parts := []string{address.ChannelProfileID, address.AccountID, telegramConversationType(address.ConversationType), address.ChatID, address.TopicID, userID}
	if len(threads) > 0 {
		parts = append(parts, threads[0])
	}
	return strings.Join(parts, "\x00")
}

func sameAddress(left, right channels.ChannelAddress) bool {
	return left.ChannelType == right.ChannelType && left.ChannelProfileID == right.ChannelProfileID && left.AccountID == right.AccountID && telegramConversationType(left.ConversationType) == telegramConversationType(right.ConversationType) && left.ChatID == right.ChatID && left.TopicID == right.TopicID
}

func telegramConversationType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func randomToken() string {
	buffer := make([]byte, 9)
	if _, err := rand.Read(buffer); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buffer)
}
