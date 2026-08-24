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
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/interactions"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
	bridgequery "cloudlight.dev/codexbridge/bridge-daemon/internal/query"
	bridgeruntime "cloudlight.dev/codexbridge/bridge-daemon/internal/runtime"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/threadregistry"
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

type turnRoute struct {
	Address  channels.ChannelAddress
	UserID   string
	ThreadID string
	TurnID   string
	Revoked  bool
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
	InteractionID string
	Questions     []interactions.Question
	Index         int
	Answers       map[string][]string
}

type Service struct {
	adapter  *Adapter
	control  Control
	runtime  Runtime
	bindings *bindings.Repository
	broker   *events.Broker
	logger   *bridgelog.SafeLogger
	registry *threadregistry.Registry
	commands *commandregistry.Registry
	queries  *bridgequery.Service

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
	reconfiguring       bool
	activeHandlers      int
}

func NewService(controlService Control, runtime Runtime, repository *bindings.Repository, broker *events.Broker, logger *bridgelog.SafeLogger, registries ...*threadregistry.Registry) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	var registry *threadregistry.Registry
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

func (s *Service) SetCommandRegistry(commands *commandregistry.Registry) {
	if commands == nil {
		return
	}
	s.mu.Lock()
	s.commands = commands
	s.queries = bridgequery.New(s.control, s.runtime, s.registry, commands)
	s.mu.Unlock()
	commands.AddChangeListener(func() { go s.syncCommandMenu() })
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
	if !strings.EqualFold(binding.ChannelType, "telegram") {
		return
	}
	s.clearAddress(channels.ChannelAddress{ChannelType: "telegram", AccountID: binding.AccountID, ConversationType: binding.ConversationType, ChatID: binding.ChatID, TopicID: binding.TopicID})
	s.refreshBindingSummary()
}

func (s *Service) BindingCreated(binding bindings.Binding) {
	if strings.EqualFold(binding.ChannelType, "telegram") {
		s.refreshBindingSummary()
	}
}

func (s *Service) HandleMessage(ctx context.Context, message channels.InboundMessage) {
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
	if s.registry == nil {
		return false
	}
	record, content, recognized, err := s.registry.ParsePrefix(text)
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
	if s.answerFreeTextForThread(ctx, message, content, record.ThreadID) {
		return true
	}
	s.startTurn(ctx, message, content, record.ThreadID)
	return true
}

func (s *Service) handleTargetCommand(ctx context.Context, message channels.InboundMessage, record threadregistry.Record, text string) {
	invocation, found := s.commandRegistry().Resolve(text)
	if !found {
		s.send(ctx, message.Address, fmt.Sprintf("#%d 不支持该指令。", record.Number))
		return
	}
	if !invocation.Definition.Enabled {
		s.send(ctx, message.Address, "指令 "+invocation.Definition.Name+" 当前已停用。")
		return
	}
	switch invocation.Definition.Action {
	case commandregistry.ActionBridgeStatus, commandregistry.ActionThreadInfo:
		s.runQueryAction(ctx, message, commandregistry.ActionThreadInfo, []string{strconv.Itoa(record.Number)})
	case commandregistry.ActionThreadHistory:
		s.runQueryAction(ctx, message, commandregistry.ActionThreadHistory, append([]string{strconv.Itoa(record.Number)}, invocation.Arguments...))
	case commandregistry.ActionThreadStop:
		s.stopThread(ctx, message, record.ThreadID)
	case commandregistry.ActionInteractionCancel:
		s.cancelThreadInteraction(ctx, message, record.ThreadID)
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
	argument := strings.TrimSpace(strings.Join(invocation.Arguments, " "))
	if result, handled := s.queryService().ExecuteAction(ctx, invocation.Definition.Action, invocation.Arguments); handled {
		s.sendQuery(ctx, message.Address, result)
		return
	}
	switch invocation.Definition.Action {
	case commandregistry.ActionBridgeStart:
		if binding, ok := s.findBinding(message.Address); ok {
			s.send(ctx, message.Address, "CloudLight Codex Bridge is ready. This chat/topic is bound to Thread "+shortID(binding.ThreadID)+".\nUse /help to list commands.")
		} else {
			s.send(ctx, message.Address, "CloudLight Codex Bridge is ready. This chat/topic is not bound yet.\nUse /threads to choose a Thread, or /bind <full-thread-id>.\nUse /help to list commands.")
		}
	case commandregistry.ActionThreadBind:
		if argument == "" {
			s.send(ctx, message.Address, "Usage: /bind <full-thread-id>. Use /threads to pick from recent Threads.")
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
	text.WriteString("Recent Threads:\n")
	rows := make([]channels.ActionRow, 0, len(threads.Threads))
	for _, thread := range threads.Threads {
		label := projectName(thread.CWD) + " · " + shortID(thread.ThreadID)
		if title := truncateRunes(strings.TrimSpace(thread.Title), 36); title != "" {
			label += " · " + title
		}
		text.WriteString("• " + label + "\n  Updated: " + displayTime(thread.UpdatedAt) + "\n")
		token := s.newCallback(callbackAction{Kind: "bind", Address: message.Address, UserID: message.UserID, ThreadID: thread.ThreadID, Expires: time.Now().Add(5 * time.Minute)})
		rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: "Bind " + shortID(thread.ThreadID), Value: token}}})
	}
	_, _ = s.sendOutbound(ctx, channels.OutboundMessage{Address: message.Address, Text: strings.TrimSpace(text.String()), Actions: rows})
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
	if s.registry == nil {
		s.send(ctx, message.Address, "聊天编号尚未初始化。")
		return
	}
	selector = strings.TrimSpace(strings.Trim(selector, "#[]"))
	number, err := strconv.Atoi(selector)
	if err != nil {
		s.send(ctx, message.Address, "请指定聊天编号，例如 /thread 12。")
		return
	}
	record, ok := s.registry.ByNumber(number)
	if !ok {
		s.send(ctx, message.Address, fmt.Sprintf("聊天编号 #%d 不存在。", number))
		return
	}
	switch action {
	case "thread", "status":
		s.statusThread(ctx, message, record.ThreadID)
	case "stop":
		s.stopThread(ctx, message, record.ThreadID)
	case "cancel":
		s.cancelThreadInteraction(ctx, message, record.ThreadID)
	}
}

func (s *Service) statusThread(ctx context.Context, message channels.InboundMessage, threadID string) {
	thread, err := s.control.ReadThread(ctx, threadID, true)
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
	route := s.routes[state.TurnID]
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

func (s *Service) bind(ctx context.Context, message channels.InboundMessage, threadID string) {
	threadID = strings.TrimSpace(threadID)
	if s.registry != nil {
		selector := strings.TrimSpace(strings.Trim(threadID, "#[]"))
		if number, err := strconv.Atoi(selector); err == nil {
			if record, ok := s.registry.ByNumber(number); ok {
				threadID = record.ThreadID
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
	s.clearAddress(message.Address)
	created, previous, err := s.bindings.UpsertAddress(bindings.CreateRequest{
		ChannelType: "telegram", AccountID: message.Address.AccountID, ConversationType: "default", ChatID: message.Address.ChatID,
		TopicID: message.Address.TopicID, ThreadID: threadID,
	})
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
	deleted, err := s.bindings.DeleteAddress("telegram", message.Address.AccountID, telegramConversationType(message.Address.ConversationType), message.Address.ChatID, message.Address.TopicID)
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
	thread, err := s.control.ReadThread(ctx, binding.ThreadID, true)
	if err != nil || thread.ThreadID == "" {
		s.send(ctx, message.Address, "Bound Thread "+shortID(binding.ThreadID)+" is no longer available. Use /unbind or bind another Thread.")
		return
	}
	s.send(ctx, message.Address, fmt.Sprintf("Current binding\nTitle: %s\nThread: %s\nProject: %s\nUpdated: %s\nState: %s\nLatest Turn: %s",
		displayTitle(thread.Title), shortID(thread.ThreadID), projectName(thread.CWD), displayTime(thread.UpdatedAt), thread.Runtime.State, latestTurnStatus(thread)))
}

func (s *Service) status(ctx context.Context, message channels.InboundMessage) {
	bridge := s.runtime.Status()
	lines := []string{"Bridge: running"}
	if bridge.AppServerRunning {
		lines = append(lines, "Codex App Server: connected")
	} else {
		lines = append(lines, "Codex App Server: unavailable")
	}
	binding, ok := s.findBinding(message.Address)
	if !ok {
		lines = append(lines, "Binding: none", "Use /threads or /bind <full-thread-id>.")
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
	state := s.runtime.RuntimeState(binding.ThreadID)
	s.mu.Lock()
	route := s.routes[state.TurnID]
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

func (s *Service) startTurn(ctx context.Context, message channels.InboundMessage, text string, target ...string) {
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
	route := &turnRoute{Address: message.Address, UserID: message.UserID, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID}
	s.mu.Lock()
	s.routes[accepted.TurnID] = route
	s.mu.Unlock()
	s.publishMessageEvent(events.TelegramMessageRouted, message, "turn-started")
	for _, interaction := range s.runtime.ListInteractions("pending") {
		if interaction.ThreadID == accepted.ThreadID && interaction.TurnID == accepted.TurnID {
			s.handleInteractionEvent(events.Event{EventType: events.InteractionRequested, ThreadID: accepted.ThreadID, TurnID: accepted.TurnID, Payload: map[string]any{"interaction": interaction}})
		}
	}
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
		events.TurnCompleted, events.TurnFailed, events.TurnInterrupted:
		return true
	default:
		return false
	}
}

func (s *Service) handleEvent(event events.Event) {
	if event.EventType == events.CodexDisconnected {
		s.finishAllRoutes("")
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
	route := s.routes[event.TurnID]
	s.mu.Unlock()
	if route == nil || event.ThreadID != route.ThreadID {
		return
	}
	switch event.EventType {
	case events.TurnCompleted, events.TurnFailed, events.TurnInterrupted:
		s.removeRoute(route.TurnID)
	}
}

func (s *Service) finishAllRoutes(_ string) {
	s.mu.Lock()
	s.routes = make(map[string]*turnRoute)
	s.waits = make(map[string]inputWait)
	s.callbacks = make(map[string]callbackAction)
	s.sessions = make(map[string]*multiSession)
	s.mu.Unlock()
}

func (s *Service) handleInteractionEvent(event events.Event) {
	s.mu.Lock()
	route := s.routes[event.TurnID]
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
		s.send(ctx, route.Address, "Codex needs approval. For safety, approve or deny this request in the WPF app.")
		return
	}
	if len(interaction.Questions) == 0 {
		s.send(ctx, route.Address, "Codex requested input, but this question must be handled in WPF.")
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
		TurnID: route.TurnID, InteractionID: interaction.ID, Questions: append([]interactions.Question(nil), interaction.Questions...), Answers: make(map[string][]string),
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
	address, userID, threadID, turnID, expires := flow.Address, flow.UserID, flow.ThreadID, flow.TurnID, flow.Expires
	s.mu.Unlock()
	switch question.Type {
	case "single-choice":
		rows := []channels.ActionRow{}
		for _, option := range question.Options {
			token := s.newCallback(callbackAction{Kind: "flow-answer", Address: address, UserID: userID, ThreadID: threadID, TurnID: turnID, InteractionID: interactionID, QuestionID: question.ID, Value: option.Value, Expires: expires})
			rows = append(rows, channels.ActionRow{Buttons: []channels.Button{{Label: truncateRunes(option.Label, 54), Value: token}}})
		}
		_, _ = s.sendOutbound(ctx, channels.OutboundMessage{Address: address, Text: questionPrompt(question), Actions: rows})
	case "multiple-choice":
		sessionID := randomToken()
		session := &multiSession{Expires: expires, Address: address, UserID: userID, ThreadID: threadID, TurnID: turnID, InteractionID: interactionID, Question: question, Selected: make(map[string]bool)}
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
		s.send(ctx, address, questionPrompt(question)+"\nReply with your next text message.")
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
	message := channels.OutboundMessage{Address: session.Address, Text: questionPrompt(session.Question), Actions: rows}
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

func (s *Service) removeRoute(turnID string) {
	s.mu.Lock()
	delete(s.routes, turnID)
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
	return s.routes[route.TurnID] == route && !route.Revoked
}

func (s *Service) findBinding(address channels.ChannelAddress) (bindings.Binding, bool) {
	binding, ok := s.bindings.FindAddress("telegram", address.AccountID, telegramConversationType(address.ConversationType), address.ChatID, address.TopicID)
	return binding, ok && binding.Enabled
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
	if result, handled := s.queryService().ExecuteAction(ctx, action, arguments); handled {
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
	if status.BotID == "" {
		s.adapter.SetBindingSummary(nil)
		return
	}
	items := s.bindings.ListChannelAccount("telegram", status.BotID)
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
	return map[string]any{"bindingId": binding.ID, "channelType": binding.ChannelType, "conversationType": binding.ConversationType, "account": shortID(binding.AccountID), "chat": maskID(binding.ChatID), "topic": maskID(binding.TopicID), "threadId": shortID(binding.ThreadID)}
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
	parts := []string{address.AccountID, telegramConversationType(address.ConversationType), address.ChatID, address.TopicID, userID}
	if len(threads) > 0 {
		parts = append(parts, threads[0])
	}
	return strings.Join(parts, "\x00")
}

func sameAddress(left, right channels.ChannelAddress) bool {
	return left.ChannelType == right.ChannelType && left.AccountID == right.AccountID && telegramConversationType(left.ConversationType) == telegramConversationType(right.ConversationType) && left.ChatID == right.ChatID && left.TopicID == right.TopicID
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
