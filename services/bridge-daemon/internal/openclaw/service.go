package openclaw

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversationregistry"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

var (
	ErrNotConfigured   = errors.New("OpenClaw Gateway is not configured")
	ErrNotConnected    = errors.New("OpenClaw Gateway is not connected")
	ErrSessionNotFound = errors.New("OpenClaw session was not found")
)

const (
	protocolVersion       = 4
	defaultRequestTimeout = 30 * time.Second
	defaultTickInterval   = 30 * time.Second
	maxReconnectBackoff   = 30 * time.Second
	maxSeenEvents         = 4096
	maxHistoryMessages    = 200
)

type Config struct {
	GatewayURL    string
	Token         string
	Password      string
	AutoReconnect bool
	Start         bool
}

type Service struct {
	logger *bridgelog.SafeLogger
	broker *events.Broker

	mu       sync.RWMutex
	cfg      Config
	status   conversation.ConnectionStatus
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stopping bool

	connection *wsConnection
	pending    map[string]pendingRequest
	writeMu    sync.Mutex

	subscribers map[uint64]*eventSubscriber
	nextSubID   uint64
	sessions    map[string]conversation.Session
	runText     map[string]string
	seenEvents  map[string]time.Time
	numbers     any
	reconnects  int
	lastTick    time.Time
	tickEvery   time.Duration
}

type wsConnection struct {
	conn       *websocket.Conn
	generation uint64
	challenge  sync.Once
}

type pendingRequest struct {
	generation uint64
	response   chan rpcResponse
}

type eventSubscriber struct {
	handler  func(conversation.Event)
	queue    chan conversation.Event
	done     chan struct{}
	stopOnce sync.Once
}

func newEventSubscriber(handler func(conversation.Event)) *eventSubscriber {
	subscriber := &eventSubscriber{handler: handler, queue: make(chan conversation.Event, 128), done: make(chan struct{})}
	go subscriber.run()
	return subscriber
}

func (s *eventSubscriber) enqueue(event conversation.Event) {
	select {
	case s.queue <- event:
	case <-s.done:
	}
}

func (s *eventSubscriber) run() {
	for {
		select {
		case <-s.done:
			return
		case event := <-s.queue:
			s.handler(event)
		}
	}
}

func (s *eventSubscriber) stop() { s.stopOnce.Do(func() { close(s.done) }) }

type rpcResponse struct {
	ok      bool
	payload json.RawMessage
	err     error
}

type frame struct {
	Type    string          `json:"type"`
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Event   string          `json:"event,omitempty"`
	OK      bool            `json:"ok,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Error   *frameError     `json:"error,omitempty"`
	Seq     int             `json:"seq,omitempty"`
}

type frameError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type helloOK struct {
	Type     string `json:"type"`
	Protocol int    `json:"protocol"`
	Server   struct {
		Version string `json:"version"`
	} `json:"server"`
	Snapshot struct {
		AuthMode string `json:"authMode"`
	} `json:"snapshot"`
	Policy struct {
		TickIntervalMs int `json:"tickIntervalMs"`
	} `json:"policy"`
	Auth struct {
		Role string `json:"role"`
	} `json:"auth"`
}

type ConfigureRequest struct {
	GatewayURL    string `json:"gatewayUrl"`
	Token         string `json:"token,omitempty"`
	Password      string `json:"password,omitempty"`
	AutoReconnect *bool  `json:"autoReconnect,omitempty"`
	Start         *bool  `json:"start,omitempty"`
}

func NewService(logger *bridgelog.SafeLogger, broker *events.Broker, registries ...any) *Service {
	var numbers any
	if len(registries) > 0 {
		numbers = registries[0]
	}
	if numbers == nil {
		numbers = conversationregistry.NewInMemory()
	}
	return &Service{
		logger: logger, broker: broker,
		status:  conversation.ConnectionStatus{Backend: conversation.BackendOpenClaw, State: "not-configured", AutoReconnect: true},
		pending: make(map[string]pendingRequest), subscribers: make(map[uint64]*eventSubscriber),
		sessions: make(map[string]conversation.Session), runText: make(map[string]string),
		seenEvents: make(map[string]time.Time), tickEvery: defaultTickInterval, numbers: numbers,
	}
}

func (s *Service) Backend() string { return conversation.BackendOpenClaw }

func (s *Service) Configure(request ConfigureRequest) (conversation.ConnectionStatus, error) {
	start := request.Start == nil || *request.Start
	autoReconnect := request.AutoReconnect == nil || *request.AutoReconnect
	return s.ConfigureConfig(Config{
		GatewayURL: request.GatewayURL, Token: request.Token, Password: request.Password,
		AutoReconnect: autoReconnect, Start: start,
	})
}

func (s *Service) ConfigureConfig(config Config) (conversation.ConnectionStatus, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return conversation.ConnectionStatus{}, err
	}
	if err := s.Stop(context.Background()); err != nil {
		return conversation.ConnectionStatus{}, err
	}
	s.mu.Lock()
	s.cfg = normalized
	s.reconnects = 0
	s.sessions = make(map[string]conversation.Session)
	s.runText = make(map[string]string)
	s.seenEvents = make(map[string]time.Time)
	s.status = conversation.ConnectionStatus{
		Backend: conversation.BackendOpenClaw, Configured: true, State: "stopped",
		GatewayURL: normalized.GatewayURL, AutoReconnect: normalized.AutoReconnect,
	}
	s.mu.Unlock()
	if normalized.Start {
		if err := s.Start(); err != nil {
			return s.ConnectionStatus(), err
		}
	}
	return s.ConnectionStatus(), nil
}

func normalizeConfig(config Config) (Config, error) {
	value, err := normalizeGatewayURL(config.GatewayURL)
	if err != nil {
		return Config{}, err
	}
	config.GatewayURL = value
	config.Token = strings.TrimSpace(config.Token)
	config.Password = strings.TrimSpace(config.Password)
	if config.Token == "" && config.Password == "" {
		return Config{}, errors.New("OpenClaw token or password is required")
	}
	return config, nil
}

func normalizeGatewayURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("OpenClaw Gateway address is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", errors.New("OpenClaw Gateway address must be a ws:// or wss:// URL")
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", errors.New("OpenClaw Gateway address must use ws:// or wss://")
	}
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	return parsed.String(), nil
}

func (s *Service) Start() error {
	s.mu.Lock()
	if !s.status.Configured {
		s.mu.Unlock()
		return ErrNotConfigured
	}
	if s.cancel != nil {
		s.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx, s.cancel, s.done, s.stopping = ctx, cancel, make(chan struct{}), false
	s.status.Running = true
	s.status.State = "connecting"
	s.mu.Unlock()
	go s.run(ctx)
	go s.watchdog(ctx)
	s.publishConnection(events.OpenClawConnecting, map[string]any{"gatewayUrl": s.safeURL()})
	return nil
}

func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel, done, connection := s.cancel, s.done, s.connection
	if cancel == nil {
		s.status.Running = false
		if s.status.Configured {
			s.status.State = "stopped"
		}
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	s.status.Running = false
	s.status.State = "stopping"
	s.mu.Unlock()
	cancel()
	if connection != nil {
		_ = connection.conn.Close()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) Close() error { return s.Stop(context.Background()) }

func (s *Service) ConnectionStatus() conversation.ConnectionStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := s.status
	status.SessionCount = len(s.sessions)
	status.LastTickAt = formatTime(s.lastTick)
	return status
}

func (s *Service) Test(ctx context.Context, request ConfigureRequest) (conversation.ConnectionStatus, error) {
	s.mu.RLock()
	current := s.cfg
	s.mu.RUnlock()
	config := Config{GatewayURL: request.GatewayURL, Token: request.Token, Password: request.Password, AutoReconnect: false, Start: true}
	if strings.TrimSpace(config.GatewayURL) == "" {
		config.GatewayURL = current.GatewayURL
	}
	if config.Token == "" {
		config.Token = current.Token
	}
	if config.Password == "" {
		config.Password = current.Password
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return conversation.ConnectionStatus{}, err
	}
	temporary := NewService(s.logger, s.broker)
	if _, err := temporary.ConfigureConfig(normalized); err != nil {
		return conversation.ConnectionStatus{}, err
	}
	defer temporary.Close()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := temporary.ConnectionStatus()
		if status.Connected {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-deadline.C:
			if status.LastError == "" {
				status.LastError = "OpenClaw Gateway connection timed out"
			}
			return status, errors.New(status.LastError)
		case <-ticker.C:
		}
	}
}

func (s *Service) run(ctx context.Context) {
	defer func() {
		s.mu.Lock()
		// A stopped run must release its lifecycle handles so a later settings
		// save can start a fresh connection. Guard the context because a future
		// Configure/Start may have installed a new run before this defer closes.
		if s.ctx != ctx {
			s.mu.Unlock()
			return
		}
		if !s.status.Running || s.stopping || !s.cfg.AutoReconnect {
			s.status.Running = false
			s.status.State = "stopped"
		}
		done := s.done
		s.ctx = nil
		s.cancel = nil
		s.done = nil
		s.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	backoff := time.Second
	for ctx.Err() == nil {
		err := s.connectOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = ErrNotConnected
		}
		s.markDisconnected(err)
		s.mu.RLock()
		autoReconnect := s.cfg.AutoReconnect
		stopping := s.stopping
		s.mu.RUnlock()
		if stopping || !autoReconnect {
			return
		}
		s.mu.Lock()
		s.reconnects++
		s.status.ReconnectCount = s.reconnects
		s.status.State = "reconnecting"
		s.mu.Unlock()
		s.publishConnection(events.OpenClawReconnecting, map[string]any{"delayMs": backoff.Milliseconds(), "attempt": s.reconnects})
		if !waitContext(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > maxReconnectBackoff {
			backoff = maxReconnectBackoff
		}
	}
}

func (s *Service) connectOnce(ctx context.Context) error {
	s.mu.RLock()
	gatewayURL := s.cfg.GatewayURL
	s.mu.RUnlock()
	s.mu.Lock()
	s.status.State = "connecting"
	s.mu.Unlock()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, gatewayURL, nil)
	if err != nil {
		return fmt.Errorf("dial OpenClaw Gateway: %w", err)
	}
	s.mu.Lock()
	generation := s.status.ReconnectCount + 1
	if generation <= 0 {
		generation = 1
	}
	connection := &wsConnection{conn: conn, generation: uint64(generation)}
	s.connection = connection
	s.mu.Unlock()
	readDone := make(chan error, 1)
	go func() { readDone <- s.readLoop(ctx, connection) }()
	select {
	case err := <-readDone:
		return err
	case <-ctx.Done():
		_ = conn.Close()
		<-readDone
		return ctx.Err()
	}
}

func (s *Service) readLoop(ctx context.Context, connection *wsConnection) error {
	for {
		messageType, data, err := connection.conn.ReadMessage()
		if err != nil {
			s.disconnect(connection, err)
			return err
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		var incoming frame
		if err := json.Unmarshal(data, &incoming); err != nil {
			s.logger.Printf("[openclaw] ignored invalid Gateway frame: %v", err)
			continue
		}
		switch incoming.Type {
		case "event":
			s.handleGatewayEvent(ctx, connection, incoming)
		case "res":
			s.resolvePending(connection, incoming)
		}
	}
}

func (s *Service) handleGatewayEvent(ctx context.Context, connection *wsConnection, incoming frame) {
	if incoming.Event == "connect.challenge" {
		var payload struct {
			Nonce string `json:"nonce"`
		}
		_ = json.Unmarshal(incoming.Payload, &payload)
		connection.challenge.Do(func() { go s.authenticate(ctx, connection) })
		return
	}
	if incoming.Event == "tick" {
		now := time.Now().UTC()
		s.mu.Lock()
		s.lastTick = now
		s.status.LastTickAt = formatTime(s.lastTick)
		s.mu.Unlock()
		s.publishConnection(events.OpenClawHeartbeat, map[string]any{"lastTickAt": formatTime(now)})
		return
	}
	if incoming.Event == "sessions.changed" {
		go s.refreshSessions(ctx, connection)
		return
	}
	if incoming.Event == "chat" {
		s.handleChatEvent(incoming.Payload, incoming.Seq)
	}
}

func (s *Service) authenticate(ctx context.Context, connection *wsConnection) {
	s.mu.RLock()
	config := s.cfg
	s.mu.RUnlock()
	auth := map[string]any{}
	if config.Token != "" {
		auth["token"] = config.Token
	}
	if config.Password != "" {
		auth["password"] = config.Password
	}
	params := map[string]any{
		"minProtocol": protocolVersion, "maxProtocol": protocolVersion,
		"client": map[string]any{
			"id": "gateway-client", "displayName": "CloudLight Codex Bridge",
			"version": "1.0", "platform": runtime.GOOS, "mode": "backend",
		},
		"role": "operator", "scopes": []string{"operator.admin", "operator.read", "operator.write"},
		"auth": auth,
	}
	// The current local Gateway accepts shared-token authentication without a
	// device signature. Do not invent a device identity for this bridge; the
	// Tray's Ed25519 identity is a separate node/device credential.
	response, err := s.requestOnConnection(ctx, connection, "connect", params, 15*time.Second)
	if err != nil {
		s.signalConnectionError(connection, err)
		_ = connection.conn.Close()
		return
	}
	var hello helloOK
	if err := json.Unmarshal(response, &hello); err != nil || hello.Type != "hello-ok" || hello.Protocol != protocolVersion {
		if err == nil {
			err = fmt.Errorf("unsupported OpenClaw hello protocol %d", hello.Protocol)
		}
		s.signalConnectionError(connection, err)
		_ = connection.conn.Close()
		return
	}
	s.mu.Lock()
	if s.connection != connection {
		s.mu.Unlock()
		return
	}
	s.lastTick = time.Now().UTC()
	s.tickEvery = defaultTickInterval
	if hello.Policy.TickIntervalMs > 0 {
		s.tickEvery = time.Duration(hello.Policy.TickIntervalMs) * time.Millisecond
	}
	s.status.Connected = true
	s.status.State = "connected"
	s.status.Protocol = hello.Protocol
	s.status.ServerVersion = hello.Server.Version
	s.status.AuthMode = hello.Snapshot.AuthMode
	s.status.LastConnectedAt = formatTime(time.Now().UTC())
	s.status.LastError = ""
	s.mu.Unlock()
	s.publishConnection(events.OpenClawConnected, map[string]any{"protocol": hello.Protocol, "serverVersion": hello.Server.Version, "authMode": hello.Snapshot.AuthMode})
	go s.afterConnected(ctx, connection)
}

func (s *Service) afterConnected(ctx context.Context, connection *wsConnection) {
	requestContext, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := s.requestOnConnection(requestContext, connection, "sessions.subscribe", map[string]any{}, defaultRequestTimeout); err != nil {
		s.logger.Printf("[openclaw] sessions.subscribe failed: %v", err)
	}
	s.refreshSessions(requestContext, connection)
}

func (s *Service) refreshSessions(ctx context.Context, connection *wsConnection) {
	response, err := s.requestOnConnection(ctx, connection, "sessions.list", map[string]any{"limit": 200}, defaultRequestTimeout)
	if err != nil {
		return
	}
	sessions, err := decodeSessions(response)
	if err != nil {
		s.logger.Printf("[openclaw] decode sessions.list failed: %v", err)
		return
	}
	sessions, err = s.numberSessions(sessions)
	if err != nil {
		s.logger.Printf("[openclaw] persist session numbers failed: %v", err)
		return
	}
	s.mu.Lock()
	if s.connection != connection {
		s.mu.Unlock()
		return
	}
	s.sessions = make(map[string]conversation.Session, len(sessions))
	for _, session := range sessions {
		s.sessions[session.Key] = session
	}
	s.status.SessionCount = len(s.sessions)
	s.mu.Unlock()
	for _, session := range sessions {
		s.publishSession(session)
	}
}

func (s *Service) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	s.mu.RLock()
	connection := s.connection
	connected := s.status.Connected
	s.mu.RUnlock()
	if connection == nil || !connected {
		return nil, ErrNotConnected
	}
	return s.requestOnConnection(ctx, connection, method, params, defaultRequestTimeout)
}

func (s *Service) requestOnConnection(ctx context.Context, connection *wsConnection, method string, params any, timeout time.Duration) (json.RawMessage, error) {
	s.mu.RLock()
	current := s.connection == connection
	s.mu.RUnlock()
	if !current {
		return nil, ErrNotConnected
	}
	id := newID()
	response := make(chan rpcResponse, 1)
	s.mu.Lock()
	s.pending[id] = pendingRequest{generation: connection.generation, response: response}
	s.mu.Unlock()
	request := map[string]any{"type": "req", "id": id, "method": method}
	if params != nil {
		request["params"] = params
	}
	if err := s.write(connection, request); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-response:
		return result.payload, result.err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, ctx.Err()
	case <-timer.C:
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		return nil, fmt.Errorf("OpenClaw Gateway request %s timed out", method)
	}
}

func (s *Service) write(connection *wsConnection, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	current := s.connection == connection
	s.mu.RUnlock()
	if !current {
		return ErrNotConnected
	}
	if err := connection.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		s.logger.Printf("[openclaw] Gateway write failed: %v", err)
		return err
	}
	return nil
}

func (s *Service) resolvePending(connection *wsConnection, incoming frame) {
	s.mu.Lock()
	pending, ok := s.pending[incoming.ID]
	if ok {
		delete(s.pending, incoming.ID)
	}
	s.mu.Unlock()
	if !ok || pending.generation != connection.generation {
		return
	}
	if incoming.OK {
		pending.response <- rpcResponse{ok: true, payload: incoming.Payload}
		return
	}
	message := "OpenClaw Gateway request failed"
	if incoming.Error != nil && incoming.Error.Message != "" {
		message = incoming.Error.Message
	}
	pending.response <- rpcResponse{err: errors.New(message)}
}

func (s *Service) disconnect(connection *wsConnection, err error) {
	s.mu.Lock()
	if s.connection != connection {
		s.mu.Unlock()
		return
	}
	s.connection = nil
	s.status.Connected = false
	s.status.LastDisconnectedAt = formatTime(time.Now().UTC())
	if !s.stopping {
		s.status.State = "disconnected"
	}
	if err != nil && !s.stopping {
		s.status.LastError = bridgelog.Redact(err.Error())
	}
	pending := make([]pendingRequest, 0, len(s.pending))
	for id, item := range s.pending {
		if item.generation == connection.generation {
			pending = append(pending, item)
			delete(s.pending, id)
		}
	}
	s.mu.Unlock()
	for _, item := range pending {
		item.response <- rpcResponse{err: ErrNotConnected}
	}
	if !s.isStopping() {
		payload := map[string]any{}
		if err != nil {
			payload["message"] = bridgelog.Redact(err.Error())
		}
		s.publishConnection(events.OpenClawDisconnected, payload)
	}
}

func (s *Service) markDisconnected(err error) {
	s.mu.Lock()
	if s.status.Connected {
		s.status.Connected = false
	}
	if err != nil && !s.stopping {
		s.status.LastError = bridgelog.Redact(err.Error())
	}
	s.mu.Unlock()
}

func (s *Service) signalConnectionError(connection *wsConnection, err error) {
	s.mu.Lock()
	if s.connection == connection && err != nil {
		s.status.LastError = bridgelog.Redact(err.Error())
		s.status.State = "error"
	}
	s.mu.Unlock()
	s.publishConnection(events.OpenClawError, map[string]any{"message": bridgelog.Redact(err.Error())})
}

func (s *Service) isStopping() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stopping
}

func (s *Service) watchdog(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			connection, lastTick, interval, connected := s.connection, s.lastTick, s.tickEvery, s.status.Connected
			s.mu.RUnlock()
			if connected && connection != nil && !lastTick.IsZero() && time.Since(lastTick) > 2*interval {
				s.logger.Printf("[openclaw] heartbeat watchdog closed a stale Gateway connection")
				_ = connection.conn.Close()
			}
		}
	}
}

func (s *Service) ListSessions(ctx context.Context, limit int) ([]conversation.Session, error) {
	if limit < 1 || limit > 200 {
		limit = 100
	}
	response, err := s.request(ctx, "sessions.list", map[string]any{"limit": limit})
	if err != nil {
		return nil, err
	}
	result, err := decodeSessions(response)
	if err != nil {
		return nil, err
	}
	result, err = s.numberSessions(result)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	// The Gateway list is a snapshot. Replacing the cache releases sessions
	// that were deleted or archived instead of retaining every key ever seen.
	s.sessions = make(map[string]conversation.Session, len(result))
	for _, session := range result {
		s.sessions[session.Key] = session
	}
	s.status.SessionCount = len(s.sessions)
	s.mu.Unlock()
	return result, nil
}

// SessionByNumber resolves a user-facing OpenClaw number only after refreshing
// the Gateway list. The returned key is the opaque SessionKey that all
// transport calls must use.
func (s *Service) SessionByNumber(ctx context.Context, number int) (conversation.Session, error) {
	if number < 1 {
		return conversation.Session{}, ErrSessionNotFound
	}
	store := conversationregistry.ForOpenClaw(s.numbers)
	if store != nil {
		record, ok := store.ByNumber(number)
		if !ok || record.Backend != conversationregistry.BackendOpenClaw {
			return conversation.Session{}, ErrSessionNotFound
		}
		sessions, err := s.ListSessions(ctx, 200)
		if err != nil {
			return conversation.Session{}, err
		}
		for _, session := range sessions {
			if session.Key == record.TargetID {
				session.Number = record.Number
				return session, nil
			}
		}
		return conversation.Session{}, ErrSessionNotFound
	}
	sessions, err := s.ListSessions(ctx, 200)
	if err != nil {
		return conversation.Session{}, err
	}
	for _, session := range sessions {
		if session.Number == number {
			return session, nil
		}
	}
	return conversation.Session{}, ErrSessionNotFound
}

func (s *Service) ReadSession(ctx context.Context, key string) (conversation.Detail, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return conversation.Detail{}, ErrSessionNotFound
	}
	sessions, err := s.ListSessions(ctx, 200)
	if err != nil {
		return conversation.Detail{}, err
	}
	var session conversation.Session
	found := false
	for _, candidate := range sessions {
		if candidate.Key == key {
			session, found = candidate, true
			break
		}
	}
	if !found {
		return conversation.Detail{}, ErrSessionNotFound
	}
	response, err := s.request(ctx, "chat.history", map[string]any{"sessionKey": key, "limit": 200})
	if err != nil {
		return conversation.Detail{Session: session}, err
	}
	messages := latestMessages(decodeMessages(response), maxHistoryMessages)
	return conversation.Detail{Session: session, Messages: messages}, nil
}

func (s *Service) SendMessage(ctx context.Context, key, message string) (conversation.SendResult, error) {
	key, message = strings.TrimSpace(key), strings.TrimSpace(message)
	if key == "" || message == "" {
		return conversation.SendResult{}, errors.New("OpenClaw session key and message are required")
	}
	idempotencyKey := newID()
	response, err := s.request(ctx, "chat.send", map[string]any{
		"sessionKey": key, "message": message, "idempotencyKey": idempotencyKey,
	})
	if err != nil {
		return conversation.SendResult{}, err
	}
	var accepted map[string]any
	_ = json.Unmarshal(response, &accepted)
	runID := firstString(accepted, "runId", "runID", "id")
	if runID == "" {
		runID = idempotencyKey
	}
	status := firstString(accepted, "status")
	return conversation.SendResult{Backend: conversation.BackendOpenClaw, SessionKey: key, RunID: runID, Status: status, AcceptedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}

func (s *Service) Abort(ctx context.Context, key, runID string) (conversation.AbortResult, error) {
	key, runID = strings.TrimSpace(key), strings.TrimSpace(runID)
	if key == "" {
		return conversation.AbortResult{}, errors.New("OpenClaw session key is required")
	}
	params := map[string]any{"sessionKey": key}
	if runID != "" {
		params["runId"] = runID
	}
	response, err := s.request(ctx, "chat.abort", params)
	if err != nil {
		return conversation.AbortResult{}, err
	}
	var result map[string]any
	_ = json.Unmarshal(response, &result)
	actualRunID := firstString(result, "abortedRunId", "runId")
	if actualRunID == "" {
		actualRunID = runID
	}
	status := firstString(result, "status")
	if status == "" {
		status = "aborted"
	}
	return conversation.AbortResult{Backend: conversation.BackendOpenClaw, SessionKey: key, RunID: actualRunID, Status: status}, nil
}

func (s *Service) SubscribeEvents(handler func(conversation.Event)) func() {
	if handler == nil {
		return func() {}
	}
	subscriber := newEventSubscriber(handler)
	s.mu.Lock()
	s.nextSubID++
	id := s.nextSubID
	s.subscribers[id] = subscriber
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		if current, ok := s.subscribers[id]; ok && current == subscriber {
			delete(s.subscribers, id)
		}
		s.mu.Unlock()
		subscriber.stop()
	}
}

func (s *Service) handleChatEvent(raw json.RawMessage, envelopeSeq int) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return
	}
	state := strings.ToLower(firstString(payload, "state"))
	key := firstString(payload, "sessionKey", "key")
	runID := firstString(payload, "runId", "runID")
	seq := firstInt(payload, "seq")
	if seq == 0 {
		seq = envelopeSeq
	}
	delta := firstString(payload, "deltaText", "delta")
	text := firstString(payload, "text")
	if message, ok := payload["message"]; ok {
		if messageText := extractMessageText(message); messageText != "" {
			text = messageText
		}
	}
	if state == "" {
		return
	}
	if !s.rememberEvent(state, key, runID, seq, delta, text) {
		return
	}
	if runID != "" {
		s.mu.Lock()
		terminal := state != "delta"
		if state == "delta" {
			if rawReplace, ok := payload["replace"].(bool); ok && rawReplace {
				s.runText[runID] = delta
			} else {
				s.runText[runID] += delta
			}
		} else if text != "" {
			s.runText[runID] = text
		}
		if text == "" && state != "delta" {
			text = s.runText[runID]
		}
		if terminal {
			delete(s.runText, runID)
		}
		s.mu.Unlock()
	}
	eventType := events.OpenClawMessageFailed
	switch state {
	case "delta":
		eventType = events.OpenClawMessageDelta
	case "final", "completed", "done":
		eventType = events.OpenClawMessageCompleted
	case "aborted", "abort", "cancelled":
		eventType = events.OpenClawMessageAborted
	}
	errorText := firstString(payload, "errorMessage", "error")
	stopReason := firstString(payload, "stopReason")
	normalized := conversation.Event{Backend: conversation.BackendOpenClaw, Type: eventType, SessionKey: key, RunID: runID, Delta: delta, Text: text, StopReason: stopReason, Error: errorText, Seq: seq, Payload: payload}
	s.publishBackendEvent(normalized)
}

func (s *Service) rememberEvent(state, key, runID string, seq int, delta, text string) bool {
	now := time.Now().UTC()
	eventKey := fmt.Sprintf("%s:%s:%s", key, runID, state)
	if seq > 0 {
		eventKey += ":" + strconv.Itoa(seq)
	} else if state == "delta" {
		eventKey += ":" + delta
	} else {
		eventKey += ":terminal"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for seen, at := range s.seenEvents {
		if now.Sub(at) > 15*time.Minute {
			delete(s.seenEvents, seen)
		}
	}
	if _, exists := s.seenEvents[eventKey]; exists {
		return false
	}
	s.seenEvents[eventKey] = now
	if len(s.seenEvents) > maxSeenEvents {
		oldestKey := ""
		var oldestAt time.Time
		for seen, at := range s.seenEvents {
			if oldestKey == "" || at.Before(oldestAt) {
				oldestKey, oldestAt = seen, at
			}
		}
		if oldestKey != "" {
			delete(s.seenEvents, oldestKey)
		}
	}
	return true
}

func (s *Service) publishBackendEvent(event conversation.Event) {
	payload := event.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	payload["backend"] = conversation.BackendOpenClaw
	if event.Delta != "" {
		payload["delta"] = event.Delta
	}
	if event.Text != "" {
		payload["text"] = event.Text
	}
	if event.Error != "" {
		payload["error"] = event.Error
	}
	if s.broker != nil {
		s.broker.PublishScoped(event.Type, event.SessionKey, event.RunID, "", payload)
	}
	s.mu.RLock()
	subscribers := make([]*eventSubscriber, 0, len(s.subscribers))
	for _, subscriber := range s.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	s.mu.RUnlock()
	for _, subscriber := range subscribers {
		subscriber.enqueue(event)
	}
}

func (s *Service) publishSession(session conversation.Session) {
	payload := map[string]any{"backend": conversation.BackendOpenClaw, "session": session}
	if s.broker != nil {
		s.broker.PublishScoped(events.OpenClawSessionUpdated, session.Key, "", "", payload)
	}
}

func (s *Service) numberSessions(sessions []conversation.Session) ([]conversation.Session, error) {
	store := conversationregistry.ForOpenClaw(s.numbers)
	if store == nil || len(sessions) == 0 {
		return sessions, nil
	}
	metadata := make([]conversationregistry.Metadata, 0, len(sessions))
	for _, session := range sessions {
		metadata = append(metadata, conversationregistry.Metadata{
			Backend: conversationregistry.BackendOpenClaw, TargetID: session.Key, Title: session.Title, CreatedAt: session.CreatedAt,
			LastSeenAt: session.UpdatedAt, Status: session.Status,
		})
	}
	records, err := store.EnsureBatchBackend(conversationregistry.BackendOpenClaw, metadata)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]conversationregistry.Record, len(records))
	for _, record := range records {
		byKey[record.TargetID] = record
	}
	for index := range sessions {
		if record, ok := byKey[sessions[index].Key]; ok {
			sessions[index].Number = record.Number
		}
	}
	return sessions, nil
}

func (s *Service) publishConnection(eventType string, payload map[string]any) {
	if s.broker != nil {
		s.broker.Publish(eventType, payload)
	}
}

func (s *Service) safeURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.GatewayURL
}

func decodeSessions(raw json.RawMessage) ([]conversation.Session, error) {
	var wrapper struct {
		Sessions []json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, err
	}
	result := make([]conversation.Session, 0, len(wrapper.Sessions))
	for _, item := range wrapper.Sessions {
		var object map[string]any
		if err := json.Unmarshal(item, &object); err != nil {
			continue
		}
		key := firstString(object, "sessionKey", "key")
		if key == "" {
			continue
		}
		activeIDs := stringSlice(object, "activeRunIds", "activeRunIDs")
		active := firstBool(object, "hasActiveRun", "activeRun") || len(activeIDs) > 0
		status := firstString(object, "status")
		if status == "" {
			status = "idle"
			if active {
				status = "running"
			}
		}
		title := firstString(object, "displayName", "derivedTitle", "title")
		if title == "" {
			title = key
		}
		result = append(result, conversation.Session{
			Backend: conversation.BackendOpenClaw, Key: key,
			SessionID: firstString(object, "sessionId", "sessionID"), AgentID: firstString(object, "agentId", "agentID"),
			Title: title, Summary: firstString(object, "summary", "lastMessagePreview"),
			Model: firstString(object, "model", "modelName"), CreatedAt: valueTime(object, "createdAt"), UpdatedAt: valueTime(object, "updatedAt", "lastActivityAt"),
			Status: status, Archived: boolPointer(object, "archived"), HasActiveRun: active, ActiveRunIDs: activeIDs,
		})
	}
	return result, nil
}

func decodeMessages(raw json.RawMessage) []conversation.Message {
	var wrapper struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(raw, &wrapper) != nil {
		return nil
	}
	result := make([]conversation.Message, 0, minOpenClawInt(len(wrapper.Messages), maxHistoryMessages))
	for _, item := range wrapper.Messages {
		var object map[string]any
		if json.Unmarshal(item, &object) != nil {
			continue
		}
		text := extractMessageText(object)
		if text == "" {
			continue
		}
		role := firstString(object, "role")
		if role == "" {
			role = "assistant"
		}
		message := conversation.Message{ID: firstString(object, "id", "messageId"), Role: role, Text: text, Timestamp: valueTime(object, "timestamp", "createdAt"), RunID: firstString(object, "runId", "runID")}
		if len(result) == maxHistoryMessages {
			copy(result, result[1:])
			result[len(result)-1] = message
		} else {
			result = append(result, message)
		}
	}
	return result
}

func latestMessages(messages []conversation.Message, limit int) []conversation.Message {
	if limit <= 0 || len(messages) <= limit {
		return messages
	}
	start := len(messages) - limit
	result := make([]conversation.Message, limit)
	copy(result, messages[start:])
	return result
}

func minOpenClawInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func extractMessageText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case map[string]any:
		for _, key := range []string{"text", "content", "message"} {
			if nested, ok := typed[key]; ok {
				if text := extractMessageText(nested); text != "" {
					return text
				}
			}
		}
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := extractMessageText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	}
	return ""
}

func firstString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstInt(object map[string]any, keys ...string) int {
	for _, key := range keys {
		switch value := object[key].(type) {
		case float64:
			return int(value)
		case json.Number:
			parsed, _ := strconv.Atoi(string(value))
			return parsed
		}
	}
	return 0
}

func firstBool(object map[string]any, keys ...string) bool {
	for _, key := range keys {
		if value, ok := object[key].(bool); ok {
			return value
		}
	}
	return false
}

func boolPointer(object map[string]any, key string) *bool {
	value, ok := object[key].(bool)
	if !ok {
		return nil
	}
	return &value
}

func stringSlice(object map[string]any, keys ...string) []string {
	for _, key := range keys {
		if raw, ok := object[key].([]any); ok {
			result := make([]string, 0, len(raw))
			for _, item := range raw {
				if value, ok := item.(string); ok && value != "" {
					result = append(result, value)
				}
			}
			return result
		}
	}
	return nil
}

func valueTime(object map[string]any, keys ...string) string {
	for _, key := range keys {
		switch value := object[key].(type) {
		case string:
			return value
		case float64:
			if value > 1e12 {
				return time.UnixMilli(int64(value)).UTC().Format(time.RFC3339Nano)
			}
			return time.Unix(int64(value), 0).UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func newID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err == nil {
		return hex.EncodeToString(buffer)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
