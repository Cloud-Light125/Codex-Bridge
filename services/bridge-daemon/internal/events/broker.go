package events

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DaemonStarted            = "daemon.started"
	DaemonStopped            = "daemon.stopped"
	CodexConnected           = "codex.connected"
	CodexDisconnected        = "codex.disconnected"
	CodexConfigUpdated       = "codex.config_updated"
	OpenClawConnecting       = "openclaw.connecting"
	OpenClawConnected        = "openclaw.connected"
	OpenClawDisconnected     = "openclaw.disconnected"
	OpenClawReconnecting     = "openclaw.reconnecting"
	OpenClawError            = "openclaw.error"
	OpenClawHeartbeat        = "openclaw.heartbeat"
	OpenClawSessionUpdated   = "openclaw.session.updated"
	OpenClawMessageDelta     = "openclaw.message.delta"
	OpenClawMessageCompleted = "openclaw.message.completed"
	OpenClawMessageAborted   = "openclaw.message.aborted"
	OpenClawMessageFailed    = "openclaw.message.failed"
	ThreadUpdated            = "thread.updated"
	TurnStarted              = "turn.started"
	TurnStatusChanged        = "turn.status_changed"
	AssistantDelta           = "assistant.delta"
	AssistantCompleted       = "assistant.completed"
	ToolStarted              = "tool.started"
	ToolUpdated              = "tool.updated"
	ToolCompleted            = "tool.completed"
	FileChanged              = "file.changed"
	InteractionRequested     = "interaction.requested"
	InteractionResolved      = "interaction.resolved"
	TurnInterrupted          = "turn.interrupted"
	TurnCompleted            = "turn.completed"
	TurnFailed               = "turn.failed"
	TurnPersistence          = "turn.persistence_changed"
	TaskUpdated              = "task.updated"
	TaskStateChanged         = "task.state_changed"
	TaskWaitingInput         = "task.waiting_input"
	TaskCompleted            = "task.completed"
	TaskFailed               = "task.failed"
	Error                    = "error"
	BindingCreated           = "binding.created"
	BindingDeleted           = "binding.deleted"
	ChannelStatusChanged     = "channel.status_changed"
	ChannelConnected         = "channel.connected"
	ChannelDisconnected      = "channel.disconnected"
	ChannelError             = "channel.error"
	MessageReceived          = "message.received"
	MessageRouted            = "message.routed"
	MessageRejected          = "message.rejected"
	MessageSent              = "message.sent"
	TelegramPollingStarted   = "telegram.polling_started"
	TelegramPollingStopped   = "telegram.polling_stopped"
	TelegramRateLimited      = "telegram.rate_limited"
	TelegramConfigured       = "channel.telegram.configured"
	TelegramTested           = "channel.telegram.tested"
	TelegramStarted          = "channel.telegram.started"
	TelegramStartFailed      = "channel.telegram.start_failed"
	TelegramStopped          = "channel.telegram.stopped"
	TelegramTokenDeleted     = "channel.telegram.token_deleted"
	TelegramMessageReceived  = MessageReceived
	TelegramMessageRouted    = MessageRouted
	TelegramMessageRejected  = MessageRejected
	QQConnecting             = "qq.connecting"
	QQConnected              = "qq.connected"
	QQDisconnected           = "qq.disconnected"
	QQReconnecting           = "qq.reconnecting"
	QQStopped                = "qq.stopped"
	QQHeartbeat              = "qq.heartbeat"
	QQMessageReceived        = "qq.message_received"
	QQMessageRejected        = "qq.message_rejected"
	QQMessageRouted          = "qq.message_routed"
	QQMessageSent            = "qq.message_sent"
	QQActionFailed           = "qq.action_failed"
	QQError                  = "qq.error"
	QQBotAuthenticating      = "qqbot.authenticating"
	QQBotTokenRefreshed      = "qqbot.token_refreshed"
	QQBotConnecting          = "qqbot.connecting"
	QQBotConnected           = "qqbot.connected"
	QQBotReady               = "qqbot.ready"
	QQBotDisconnected        = "qqbot.disconnected"
	QQBotReconnecting        = "qqbot.reconnecting"
	QQBotStopped             = "qqbot.stopped"
	QQBotHeartbeat           = "qqbot.heartbeat"
	QQBotMessageReceived     = "qqbot.message_received"
	QQBotMessageRejected     = "qqbot.message_rejected"
	QQBotMessageRouted       = "qqbot.message_routed"
	QQBotMessageSent         = "qqbot.message_sent"
	QQBotActionFailed        = "qqbot.action_failed"
	QQBotError               = "qqbot.error"
)

// Event is the stable bridge event envelope. Payloads are normalized before
// they cross the local API boundary; App Server's raw protocol is never the UI contract.
type Event struct {
	EventID   string         `json:"eventId"`
	EventType string         `json:"eventType"`
	Timestamp string         `json:"timestamp"`
	ThreadID  string         `json:"threadId,omitempty"`
	TurnID    string         `json:"turnId,omitempty"`
	ItemID    string         `json:"itemId,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type Broker struct {
	mu          sync.RWMutex
	nextSubID   uint64
	nextEventID atomic.Uint64
	subscribers map[uint64]*subscription
	history     []Event
}

type subscription struct {
	mu     sync.Mutex
	output chan Event
	wake   chan struct{}
	done   chan struct{}
	queue  []Event
	closed bool
}

func NewBroker() *Broker {
	return &Broker{subscribers: make(map[uint64]*subscription)}
}

func (b *Broker) Publish(eventType string, payload map[string]any) {
	b.PublishScoped(eventType, "", "", "", payload)
}

func (b *Broker) PublishScoped(eventType, threadID, turnID, itemID string, payload map[string]any) {
	now := time.Now().UTC()
	event := Event{
		EventID:   fmt.Sprintf("%d-%d", now.UnixMilli(), b.nextEventID.Add(1)),
		EventType: eventType,
		Timestamp: now.Format(time.RFC3339Nano),
		ThreadID:  threadID,
		TurnID:    turnID,
		ItemID:    itemID,
		Payload:   payload,
	}
	b.mu.Lock()
	b.history = append(b.history, event)
	if len(b.history) > 64 {
		b.history = b.history[len(b.history)-64:]
	}
	subscribers := make([]*subscription, 0, len(b.subscribers))
	for _, subscriber := range b.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	b.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.enqueue(event)
	}
}

func (b *Broker) Subscribe() (<-chan Event, func()) {
	b.mu.Lock()
	b.nextSubID++
	id := b.nextSubID
	subscriber := newSubscription()
	b.subscribers[id] = subscriber
	b.mu.Unlock()
	return subscriber.output, func() {
		b.mu.Lock()
		if _, ok := b.subscribers[id]; ok {
			delete(b.subscribers, id)
		}
		b.mu.Unlock()
		subscriber.stop()
	}
}

func newSubscription() *subscription {
	subscriber := &subscription{output: make(chan Event, 128), wake: make(chan struct{}, 1), done: make(chan struct{}), queue: make([]Event, 0, 256)}
	go subscriber.run()
	return subscriber
}

func (s *subscription) enqueue(event Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	const softLimit = 256
	if len(s.queue) >= softLimit {
		discard := -1
		for index, queued := range s.queue {
			if !reliableEvent(queued.EventType) {
				discard = index
				break
			}
		}
		if discard >= 0 {
			copy(s.queue[discard:], s.queue[discard+1:])
			s.queue = s.queue[:len(s.queue)-1]
		} else if !reliableEvent(event.EventType) {
			s.mu.Unlock()
			return
		}
		// If the queue contains only reliable events, let it grow rather than
		// falsifying a terminal, interaction, binding, or channel state.
	}
	s.queue = append(s.queue, event)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscription) run() {
	for {
		select {
		case <-s.done:
			return
		case <-s.wake:
			for {
				s.mu.Lock()
				if len(s.queue) == 0 || s.closed {
					s.mu.Unlock()
					break
				}
				event := s.queue[0]
				s.queue = s.queue[1:]
				s.mu.Unlock()
				select {
				case s.output <- event:
				case <-s.done:
					return
				}
			}
		}
	}
}

func (s *subscription) stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.queue = nil
	close(s.done)
	s.mu.Unlock()
}

func reliableEvent(eventType string) bool {
	switch eventType {
	case CodexDisconnected, InteractionRequested, InteractionResolved,
		TurnInterrupted, TurnCompleted, TurnFailed, TurnPersistence,
		TaskUpdated, TaskStateChanged, TaskWaitingInput, TaskCompleted, TaskFailed,
		BindingCreated, BindingDeleted,
		ChannelStatusChanged, ChannelConnected, ChannelDisconnected, ChannelError,
		TelegramPollingStarted, TelegramPollingStopped, TelegramRateLimited,
		TelegramConfigured, TelegramTested, TelegramStarted, TelegramStartFailed, TelegramStopped, TelegramTokenDeleted,
		QQConnecting, QQConnected, QQDisconnected, QQReconnecting, QQStopped, QQActionFailed, QQError,
		QQBotAuthenticating, QQBotTokenRefreshed, QQBotConnecting, QQBotConnected, QQBotReady,
		QQBotDisconnected, QQBotReconnecting, QQBotStopped, QQBotActionFailed, QQBotError,
		OpenClawConnecting, OpenClawConnected, OpenClawDisconnected, OpenClawReconnecting,
		OpenClawError, OpenClawSessionUpdated, OpenClawMessageCompleted, OpenClawMessageAborted,
		OpenClawMessageFailed,
		Error:
		return true
	default:
		return false
	}
}
