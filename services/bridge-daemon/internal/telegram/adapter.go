package telegram

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
)

const (
	minPollingTimeout   = 10
	maxPollingTimeout   = 60
	recentIdentityLimit = 50
)

type ConfigureRequest struct {
	Token                 *string `json:"token,omitempty"`
	AllowedUserIDs        []int64 `json:"allowedUserIds"`
	PollingTimeoutSeconds int     `json:"pollingTimeoutSeconds"`
	SendProgressUpdates   bool    `json:"sendProgressUpdates"`
	AutoStart             bool    `json:"autoStart"`
	ProxyMode             string  `json:"proxyMode,omitempty"`
	ProxyURL              string  `json:"proxyUrl,omitempty"`
}

type TestRequest struct {
	Token *string `json:"token,omitempty"`
}

type TestResult struct {
	OK          bool   `json:"ok"`
	Category    string `json:"category"`
	BotID       string `json:"botId,omitempty"`
	Username    string `json:"username,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Message     string `json:"message"`
}

type ProxyTestRequest struct {
	ProxyMode string `json:"proxyMode,omitempty"`
	ProxyURL  string `json:"proxyUrl,omitempty"`
}

type ProxyTestResult struct {
	OK                 bool   `json:"ok"`
	Category           string `json:"category"`
	Message            string `json:"message"`
	StatusCode         int    `json:"statusCode"`
	DurationMS         int64  `json:"durationMs"`
	EffectiveProxyMode string `json:"effectiveProxyMode"`
	MaskedProxyAddress string `json:"maskedProxyAddress"`
}

type AdapterEvent struct {
	Kind       string
	Category   string
	RetryAfter time.Duration
}

type AdapterStatus struct {
	Type                  string           `json:"type"`
	ChannelType           string           `json:"channelType"`
	Configured            bool             `json:"configured"`
	Running               bool             `json:"running"`
	Connected             bool             `json:"connected"`
	State                 string           `json:"state"`
	PollingState          string           `json:"pollingState"`
	TokenSet              bool             `json:"tokenSet"`
	TokenFingerprint      string           `json:"tokenFingerprint,omitempty"`
	BotID                 string           `json:"botId"`
	BotUsername           string           `json:"botUsername"`
	BotDisplayName        string           `json:"botDisplayName"`
	AllowedUserIDs        []int64          `json:"allowedUserIds"`
	AllowedUserCount      int              `json:"allowedUserCount"`
	PollingTimeoutSeconds int              `json:"pollingTimeoutSeconds"`
	SendProgressUpdates   bool             `json:"sendProgressUpdates"`
	AutoStart             bool             `json:"autoStart"`
	StartedAt             string           `json:"startedAt"`
	StoppedAt             string           `json:"stoppedAt,omitempty"`
	LastUpdateAt          string           `json:"lastUpdateAt"`
	LastError             string           `json:"lastError"`
	LastErrorCategory     string           `json:"lastErrorCategory,omitempty"`
	ProxyMode             string           `json:"proxyMode"`
	MaskedProxyAddress    string           `json:"maskedProxyAddress"`
	EffectiveProxyMode    string           `json:"effectiveProxyMode"`
	LastNetworkStage      string           `json:"lastNetworkStage"`
	LastRequestDurationMS int64            `json:"lastRequestDurationMs"`
	BindingCount          int              `json:"bindingCount"`
	BoundAddressSummaries []string         `json:"boundAddressSummaries"`
	RecentIdentities      []RecentIdentity `json:"recentIdentities"`
}

// RecentIdentity contains only bounded contact metadata from inbound
// updates. It intentionally excludes message bodies and bot credentials.
type RecentIdentity struct {
	UserID       int64  `json:"userId"`
	ChatID       int64  `json:"chatId"`
	ChatType     string `json:"chatType,omitempty"`
	ChatTitle    string `json:"chatTitle,omitempty"`
	ChatUsername string `json:"chatUsername,omitempty"`
	FirstName    string `json:"firstName,omitempty"`
	LastName     string `json:"lastName,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	Username     string `json:"username,omitempty"`
	LastSeenAt   string `json:"lastSeenAt"`
}

type MessageHandler func(context.Context, channels.InboundMessage)

type Adapter struct {
	mu       sync.RWMutex
	config   ConfigureRequest
	token    string
	client   *Client
	status   AdapterStatus
	cancel   context.CancelFunc
	done     chan struct{}
	handler  MessageHandler
	events   func(AdapterEvent)
	seen     *dedupSet
	baseURL  string
	clientFn func(string, ProxyConfig) (*Client, error)
	probeFn  func(context.Context, ProxyConfig) (int, time.Duration, error)
	recent   []RecentIdentity
}

func NewAdapter(handler MessageHandler) *Adapter {
	return &Adapter{
		handler: handler, seen: newDedupSet(4096), baseURL: defaultAPIBase,
		clientFn: NewClientWithProxy, probeFn: probeTelegram,
		config: ConfigureRequest{PollingTimeoutSeconds: 30, ProxyMode: ProxyModeEnvironment},
		status: AdapterStatus{Type: "telegram", ChannelType: "telegram", State: "stopped", PollingState: "stopped", PollingTimeoutSeconds: 30, ProxyMode: ProxyModeEnvironment, EffectiveProxyMode: ProxyModeEnvironment, AllowedUserIDs: []int64{}, BoundAddressSummaries: []string{}, RecentIdentities: []RecentIdentity{}},
	}
}

func (a *Adapter) Type() string { return "telegram" }

func (a *Adapter) Configure(request ConfigureRequest) (AdapterStatus, error) {
	request, err := normalizeConfigureRequest(request)
	if err != nil {
		return AdapterStatus{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status.Running {
		if request.Token != nil {
			token := strings.TrimSpace(*request.Token)
			if token == "" {
				return AdapterStatus{}, errors.New("token cannot be empty; use DELETE token")
			}
			if token != a.token {
				return AdapterStatus{}, errors.New("stop Telegram before changing its token")
			}
		}
		request.Token = nil
		a.config = request
		a.refreshStatusLocked()
		a.status.LastError = ""
		a.status.LastErrorCategory = ""
		return cloneStatus(a.status), nil
	}
	if request.Token != nil {
		token := strings.TrimSpace(*request.Token)
		if token == "" {
			return AdapterStatus{}, errors.New("token cannot be empty; use DELETE token")
		}
		a.token = token
	}
	request.Token = nil
	a.config = request
	a.refreshStatusLocked()
	a.status.LastError = ""
	a.status.LastErrorCategory = ""
	return cloneStatus(a.status), nil
}

func normalizeConfigureRequest(request ConfigureRequest) (ConfigureRequest, error) {
	if request.PollingTimeoutSeconds == 0 {
		request.PollingTimeoutSeconds = 30
	}
	if request.PollingTimeoutSeconds < minPollingTimeout || request.PollingTimeoutSeconds > maxPollingTimeout {
		return ConfigureRequest{}, fmt.Errorf("pollingTimeoutSeconds must be between %d and %d", minPollingTimeout, maxPollingTimeout)
	}
	allowed := make(map[int64]struct{}, len(request.AllowedUserIDs))
	normalized := make([]int64, 0, len(request.AllowedUserIDs))
	for _, id := range request.AllowedUserIDs {
		if id <= 0 {
			return ConfigureRequest{}, errors.New("allowedUserIds must contain positive numeric Telegram user IDs")
		}
		if _, duplicate := allowed[id]; duplicate {
			continue
		}
		allowed[id] = struct{}{}
		normalized = append(normalized, id)
	}
	request.AllowedUserIDs = normalized
	if request.Token != nil {
		token := strings.TrimSpace(*request.Token)
		if token == "" {
			return ConfigureRequest{}, errors.New("token cannot be empty; use DELETE token")
		}
		request.Token = &token
	}
	proxy, err := normalizeProxyConfig(ProxyConfig{Mode: request.ProxyMode, URL: request.ProxyURL})
	if err != nil {
		return ConfigureRequest{}, err
	}
	request.ProxyMode = proxy.Mode
	request.ProxyURL = proxy.URL
	return request, nil
}

func (a *Adapter) DeleteToken(ctx context.Context) error {
	if err := a.Stop(ctx); err != nil {
		return err
	}
	a.mu.Lock()
	a.token = ""
	a.client = nil
	a.status.BotID = ""
	a.status.BotUsername = ""
	a.status.BotDisplayName = ""
	a.status.LastError = ""
	a.status.LastErrorCategory = ""
	a.refreshStatusLocked()
	a.mu.Unlock()
	return nil
}

func (a *Adapter) Start(parent context.Context) error {
	a.mu.Lock()
	if a.status.Running {
		a.mu.Unlock()
		return nil
	}
	if a.token == "" {
		a.mu.Unlock()
		return errors.New("Telegram token is required")
	}
	if len(a.config.AllowedUserIDs) == 0 {
		a.mu.Unlock()
		return errors.New("at least one allowed Telegram user ID is required")
	}
	client, err := a.clientFn(a.token, a.proxyConfigLocked())
	if err != nil {
		a.mu.Unlock()
		return err
	}
	client.setObserver(a.observeNetwork)
	ctx, cancel := context.WithCancel(parent)
	a.cancel = cancel
	a.done = make(chan struct{})
	a.client = client
	a.status.Running = true
	a.status.State = "starting"
	a.status.PollingState = "starting"
	a.status.Connected = false
	a.status.StartedAt = nowText()
	a.status.StoppedAt = ""
	a.status.LastError = ""
	a.status.LastErrorCategory = ""
	a.mu.Unlock()

	identityContext, cancelIdentity := context.WithTimeout(ctx, 15*time.Second)
	me, err := client.GetMe(identityContext)
	cancelIdentity()
	if err != nil {
		cancel()
		a.finish(err)
		close(a.done)
		return err
	}
	a.mu.Lock()
	a.status.BotID = int64Text(me.ID)
	a.status.BotUsername = me.Username
	a.status.BotDisplayName = strings.TrimSpace(me.FirstName)
	a.status.State = "running"
	a.status.PollingState = "polling"
	a.status.Connected = true
	a.mu.Unlock()
	go a.poll(ctx, client, me.ID)
	return nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.RLock()
	cancel, done, running := a.cancel, a.done, a.status.Running
	a.mu.RUnlock()
	if !running {
		return nil
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (a *Adapter) Status() channels.Status {
	status := a.TelegramStatus()
	return channels.Status{Type: "telegram", Running: status.Running, State: status.State, LastError: status.LastError}
}

func (a *Adapter) TelegramStatus() AdapterStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return cloneStatus(a.status)
}

func (a *Adapter) SetEventHandler(handler func(AdapterEvent)) {
	a.mu.Lock()
	a.events = handler
	a.mu.Unlock()
}

func (a *Adapter) SyncCommands(ctx context.Context, commands []BotCommand) error {
	a.mu.RLock()
	client, running := a.client, a.status.Running && a.status.Connected
	a.mu.RUnlock()
	if !running || client == nil {
		return nil
	}
	return client.SetMyCommands(ctx, commands)
}

func (a *Adapter) SetBindingSummary(summaries []string) {
	a.mu.Lock()
	a.status.BindingCount = len(summaries)
	a.status.BoundAddressSummaries = append([]string(nil), summaries...)
	a.mu.Unlock()
}

func (a *Adapter) Test(ctx context.Context, request TestRequest) TestResult {
	a.mu.RLock()
	token := a.token
	proxy := a.proxyConfigLocked()
	a.mu.RUnlock()
	if request.Token != nil {
		token = strings.TrimSpace(*request.Token)
	}
	if token == "" {
		return TestResult{Category: "invalid-token", Message: "Telegram token is required"}
	}
	client, err := a.clientFn(token, proxy)
	if err != nil {
		return TestResult{Category: "invalid-proxy", Message: "Telegram proxy configuration is invalid"}
	}
	client.setObserver(a.observeNetwork)
	me, err := client.GetMe(ctx)
	if err != nil {
		category := errorCategory(err)
		return TestResult{Category: category, Message: safeErrorMessage(err)}
	}
	a.mu.Lock()
	if request.Token == nil || token == a.token {
		a.status.BotID = int64Text(me.ID)
		a.status.BotUsername = me.Username
		a.status.BotDisplayName = strings.TrimSpace(me.FirstName)
	}
	a.mu.Unlock()
	return TestResult{OK: true, Category: "ok", BotID: int64Text(me.ID), Username: me.Username, DisplayName: strings.TrimSpace(me.FirstName), Message: "Telegram Bot API is reachable"}
}

func (a *Adapter) TestProxy(ctx context.Context, request ProxyTestRequest) ProxyTestResult {
	proxy, err := normalizeProxyConfig(ProxyConfig{Mode: request.ProxyMode, URL: request.ProxyURL})
	if err != nil {
		mode := strings.ToLower(strings.TrimSpace(request.ProxyMode))
		if mode == "" {
			mode = ProxyModeEnvironment
		}
		return ProxyTestResult{Category: "invalid-proxy", Message: proxyTestMessage("invalid-proxy"), EffectiveProxyMode: mode}
	}
	statusCode, duration, err := a.probeFn(ctx, proxy)
	a.observeNetwork(NetworkObservation{Stage: "proxy-test", Duration: duration, Err: err})
	result := ProxyTestResult{
		OK: err == nil, Category: "ok", Message: "已连接到 Telegram。", StatusCode: statusCode,
		DurationMS: duration.Milliseconds(), EffectiveProxyMode: proxy.Mode, MaskedProxyAddress: maskedProxyAddress(proxy),
	}
	if err != nil {
		result.Category = errorCategory(err)
		result.Message = proxyTestMessage(result.Category)
	}
	return result
}

func (a *Adapter) SendMessage(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()
	if client == nil {
		return channels.OutboundResult{}, errors.New("Telegram adapter is not started")
	}
	parts := SplitMessage(message.Text, 3900)
	if len(parts) == 0 {
		return channels.OutboundResult{}, errors.New("Telegram message is empty")
	}
	var result Message
	for index, part := range parts {
		outbound := message
		outbound.Text = part
		if index != len(parts)-1 {
			outbound.Actions = nil
		}
		var err error
		result, err = client.SendMessage(ctx, outbound)
		if err != nil {
			return channels.OutboundResult{}, err
		}
	}
	return channels.OutboundResult{MessageID: int64Text(result.MessageID)}, nil
}

func (a *Adapter) EditMessage(ctx context.Context, messageID string, message channels.OutboundMessage) (channels.OutboundResult, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()
	if client == nil {
		return channels.OutboundResult{}, errors.New("Telegram adapter is not started")
	}
	result, err := client.EditMessage(ctx, messageID, message)
	return channels.OutboundResult{MessageID: int64Text(result.MessageID)}, err
}

func (a *Adapter) AnswerCallback(ctx context.Context, callbackID, text string) error {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()
	if client == nil || callbackID == "" {
		return nil
	}
	return client.AnswerCallback(ctx, callbackID, text)
}

func (a *Adapter) poll(ctx context.Context, client *Client, botID int64) {
	var terminalErr error
	defer close(a.done)
	defer func() { a.finish(terminalErr) }()
	var offset int64
	backoff := time.Second
	for ctx.Err() == nil {
		a.mu.RLock()
		timeout := a.config.PollingTimeoutSeconds
		a.mu.RUnlock()
		updates, err := client.GetUpdates(ctx, offset, timeout)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrInvalidToken) || errors.Is(err, ErrConflict) {
				terminalErr = err
				return
			}
			var apiErr *APIError
			delay := backoff
			if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
				delay = apiErr.RetryAfter
				a.emit(AdapterEvent{Kind: "rate-limited", Category: "rate-limit", RetryAfter: delay})
			} else if backoff < 30*time.Second {
				backoff *= 2
			}
			a.recordError(err)
			if !waitContext(ctx, delay) {
				return
			}
			continue
		}
		backoff = time.Second
		a.markPollingHealthy()
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			a.handleUpdate(ctx, update, botID)
		}
	}
}

func (a *Adapter) handleUpdate(ctx context.Context, update Update, botID int64) {
	a.mu.RLock()
	accountID := a.status.BotID
	allowed := append([]int64(nil), a.config.AllowedUserIDs...)
	handler := a.handler
	a.mu.RUnlock()
	if update.UpdateID == 0 || !a.seen.Add(accountID+":"+strconv.FormatInt(update.UpdateID, 10)) {
		return
	}
	inbound := channels.InboundMessage{UpdateID: update.UpdateID, Received: time.Now().UTC()}
	var user *User
	if update.Message != nil {
		user = update.Message.From
		inbound.MessageID = int64Text(update.Message.MessageID)
		inbound.Text = update.Message.Text
		inbound.Address = address(accountID, update.Message.Chat.ID, update.Message.MessageThreadID)
	} else if update.CallbackQuery != nil && update.CallbackQuery.Message != nil {
		user = &update.CallbackQuery.From
		inbound.CallbackID = update.CallbackQuery.ID
		inbound.Action = update.CallbackQuery.Data
		inbound.MessageID = int64Text(update.CallbackQuery.Message.MessageID)
		inbound.Address = address(accountID, update.CallbackQuery.Message.Chat.ID, update.CallbackQuery.Message.MessageThreadID)
	} else {
		return
	}
	if user == nil || user.IsBot || user.ID == botID {
		return
	}
	a.rememberIdentity(*user, update)
	if !containsID(allowed, user.ID) {
		return
	}
	inbound.UserID = int64Text(user.ID)
	inbound.Address.UserID = inbound.UserID
	if handler != nil {
		func() {
			defer func() {
				if recover() != nil {
					a.recordError(&APIError{Kind: "handler", Message: "bad update was ignored"})
				}
			}()
			handler(ctx, inbound)
		}()
	}
	a.mu.Lock()
	a.status.LastUpdateAt = nowText()
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "status"})
}

func (a *Adapter) finish(err error) {
	a.mu.Lock()
	if err != nil {
		a.status.LastError = safeErrorMessage(err)
		a.status.LastErrorCategory = errorCategory(err)
	}
	a.status.Running = false
	a.status.Connected = false
	if errors.Is(err, ErrInvalidToken) {
		a.status.State = "invalid-token"
		a.status.PollingState = "invalid-token"
	} else if errors.Is(err, ErrConflict) {
		a.status.State = "conflict"
		a.status.PollingState = "conflict"
	} else if a.status.State != "invalid-token" && a.status.State != "conflict" {
		a.status.State = "stopped"
		a.status.PollingState = "stopped"
	}
	a.status.StoppedAt = nowText()
	a.cancel = nil
	event := AdapterEvent{Kind: "stopped", Category: a.status.LastErrorCategory}
	if err != nil {
		event.Kind = "error"
	}
	handler := a.events
	a.mu.Unlock()
	if handler != nil {
		handler(event)
	}
}

func (a *Adapter) recordError(err error) {
	a.mu.Lock()
	a.status.LastError = safeErrorMessage(err)
	a.status.LastErrorCategory = errorCategory(err)
	a.status.Connected = false
	a.status.PollingState = "backoff"
	category := a.status.LastErrorCategory
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "transient-error", Category: category})
}

func (a *Adapter) markPollingHealthy() {
	a.mu.Lock()
	recovered := !a.status.Connected || a.status.PollingState != "polling"
	a.status.Connected = true
	a.status.State = "running"
	a.status.PollingState = "polling"
	a.status.LastError = ""
	a.status.LastErrorCategory = ""
	a.mu.Unlock()
	if recovered {
		a.emit(AdapterEvent{Kind: "recovered"})
	}
}

func (a *Adapter) observeNetwork(observation NetworkObservation) {
	a.mu.Lock()
	a.status.LastNetworkStage = observation.Stage
	a.status.LastRequestDurationMS = observation.Duration.Milliseconds()
	if errors.Is(observation.Err, context.Canceled) {
		a.mu.Unlock()
		return
	}
	if observation.Err != nil {
		a.status.LastError = safeErrorMessage(observation.Err)
		a.status.LastErrorCategory = errorCategory(observation.Err)
	} else {
		a.status.LastError = ""
		a.status.LastErrorCategory = ""
	}
	a.mu.Unlock()
}

func (a *Adapter) refreshStatusLocked() {
	a.status.Configured = a.token != ""
	a.status.TokenSet = a.token != ""
	a.status.TokenFingerprint = tokenFingerprint(a.token)
	a.status.AllowedUserIDs = append([]int64(nil), a.config.AllowedUserIDs...)
	a.status.AllowedUserCount = len(a.config.AllowedUserIDs)
	a.status.PollingTimeoutSeconds = a.config.PollingTimeoutSeconds
	a.status.SendProgressUpdates = a.config.SendProgressUpdates
	a.status.AutoStart = a.config.AutoStart
	a.status.ProxyMode = a.config.ProxyMode
	a.status.EffectiveProxyMode = a.config.ProxyMode
	a.status.MaskedProxyAddress = maskedProxyAddress(a.proxyConfigLocked())
}

func (a *Adapter) proxyConfigLocked() ProxyConfig {
	return ProxyConfig{Mode: a.config.ProxyMode, URL: a.config.ProxyURL}
}

func (a *Adapter) emit(event AdapterEvent) {
	a.mu.RLock()
	handler := a.events
	a.mu.RUnlock()
	if handler != nil {
		handler(event)
	}
}

func tokenFingerprint(token string) string {
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:4])
}

func address(accountID string, chatID, topicID int64) channels.ChannelAddress {
	result := channels.ChannelAddress{ChannelType: "telegram", AccountID: accountID, ChatID: int64Text(chatID)}
	if topicID != 0 {
		result.TopicID = int64Text(topicID)
	}
	return result
}

func containsID(values []int64, value int64) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func errorCategory(err error) string {
	if errors.Is(err, ErrInvalidToken) {
		return "invalid-token"
	}
	if errors.Is(err, ErrConflict) {
		return "telegram-api"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Kind {
		case "network":
			return "network"
		case "invalid-proxy":
			return "invalid-proxy"
		case "proxy-refused":
			return "proxy-refused"
		case "tls":
			return "tls"
		case "telegram-unreachable":
			return "telegram-unreachable"
		case "timeout":
			return "timeout"
		default:
			return "telegram-api"
		}
	}
	return "unknown"
}

func safeErrorMessage(err error) string {
	switch errorCategory(err) {
	case "invalid-token":
		return "Telegram rejected the bot token"
	case "timeout":
		return "Telegram request timed out"
	case "network":
		return "Telegram network request failed"
	case "invalid-proxy":
		return "Telegram proxy configuration is invalid"
	case "proxy-refused":
		return "Telegram proxy connection was refused"
	case "tls":
		return "Telegram TLS connection failed"
	case "telegram-unreachable":
		return "Telegram is unreachable"
	case "telegram-api":
		return err.Error()
	default:
		return "Telegram request failed"
	}
}

func proxyTestMessage(category string) string {
	switch category {
	case "invalid-proxy":
		return "代理地址无效，请使用不含凭据、查询参数或额外路径的 http URL。"
	case "proxy-refused":
		return "代理连接被拒绝，请检查代理地址和端口。"
	case "timeout":
		return "连接 Telegram 超时，请检查代理或网络。"
	case "tls":
		return "Telegram TLS 握手失败，请检查代理证书或系统时间。"
	default:
		return "无法连接 Telegram，请检查网络和代理设置。"
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func cloneStatus(status AdapterStatus) AdapterStatus {
	status.AllowedUserIDs = append([]int64(nil), status.AllowedUserIDs...)
	status.BoundAddressSummaries = append([]string(nil), status.BoundAddressSummaries...)
	status.RecentIdentities = append([]RecentIdentity(nil), status.RecentIdentities...)
	return status
}

func (a *Adapter) rememberIdentity(user User, update Update) {
	chat := Chat{}
	if update.Message != nil {
		chat = update.Message.Chat
	} else if update.CallbackQuery != nil && update.CallbackQuery.Message != nil {
		chat = update.CallbackQuery.Message.Chat
	}
	if user.ID <= 0 || chat.ID == 0 {
		return
	}
	displayName := strings.TrimSpace(strings.Join([]string{user.FirstName, user.LastName}, " "))
	if displayName == "" {
		displayName = strings.TrimSpace(user.Username)
	}
	chatTitle := strings.TrimSpace(chat.Title)
	if chatTitle == "" {
		chatTitle = strings.TrimSpace(strings.Join([]string{chat.FirstName, chat.LastName}, " "))
	}
	if chatTitle == "" {
		chatTitle = strings.TrimSpace(chat.Username)
	}
	identity := RecentIdentity{UserID: user.ID, ChatID: chat.ID, ChatType: strings.TrimSpace(chat.Type), ChatTitle: chatTitle, ChatUsername: strings.TrimSpace(chat.Username), FirstName: strings.TrimSpace(user.FirstName), LastName: strings.TrimSpace(user.LastName), DisplayName: displayName, Username: strings.TrimSpace(user.Username), LastSeenAt: nowText()}
	a.mu.Lock()
	defer a.mu.Unlock()
	for index, existing := range a.recent {
		if existing.UserID == identity.UserID && existing.ChatID == identity.ChatID {
			a.recent = append(a.recent[:index], a.recent[index+1:]...)
			break
		}
	}
	a.recent = append([]RecentIdentity{identity}, a.recent...)
	if len(a.recent) > recentIdentityLimit {
		a.recent = a.recent[:recentIdentityLimit]
	}
	a.status.RecentIdentities = append([]RecentIdentity(nil), a.recent...)
}

type dedupSet struct {
	mu       sync.Mutex
	capacity int
	order    []string
	items    map[string]struct{}
}

func newDedupSet(capacity int) *dedupSet {
	return &dedupSet{capacity: capacity, items: make(map[string]struct{})}
}

func (d *dedupSet) Add(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.items[key]; exists {
		return false
	}
	d.items[key] = struct{}{}
	d.order = append(d.order, key)
	if len(d.order) > d.capacity {
		delete(d.items, d.order[0])
		d.order = d.order[1:]
	}
	return true
}
