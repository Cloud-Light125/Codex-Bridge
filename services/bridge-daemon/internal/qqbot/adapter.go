package qqbot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"github.com/gorilla/websocket"
)

type ConfigureRequest struct {
	Enabled                   bool     `json:"enabled"`
	AutoStart                 bool     `json:"autoStart"`
	AppID                     string   `json:"appId"`
	Environment               string   `json:"environment"`
	AllowedUserOpenIDs        []string `json:"allowedUserOpenIds"`
	AllowedGroupOpenIDs       []string `json:"allowedGroupOpenIds"`
	AllowedGroupMemberOpenIDs []string `json:"allowedGroupMemberOpenIds"`
	GroupTriggerMode          string   `json:"groupTriggerMode"`
	CommandPrefix             string   `json:"commandPrefix"`
	SendProgressUpdates       bool     `json:"sendProgressUpdates"`
	GatewayReconnectEnabled   bool     `json:"gatewayReconnectEnabled"`
	ProxyMode                 string   `json:"proxyMode"`
	ProxyURL                  string   `json:"proxyUrl"`
}

type SecretRequest struct {
	AppSecret string `json:"appSecret"`
}

type TestRequest struct{}

type TestResult struct {
	Success          bool   `json:"success"`
	Code             string `json:"code,omitempty"`
	AppID            string `json:"appId,omitempty"`
	Environment      string `json:"environment,omitempty"`
	GatewayAvailable bool   `json:"gatewayAvailable"`
	TokenExpiresIn   int64  `json:"tokenExpiresIn,omitempty"`
	GatewayHost      string `json:"gatewayHost,omitempty"`
	Message          string `json:"message"`
}

type NetworkTestResult struct {
	Success            bool   `json:"success"`
	Code               string `json:"code,omitempty"`
	Message            string `json:"message"`
	DurationMs         int64  `json:"durationMs"`
	EffectiveProxyMode string `json:"effectiveProxyMode"`
	MaskedProxyAddress string `json:"maskedProxyAddress,omitempty"`
}

type AdapterStatus struct {
	ChannelType             string `json:"channelType"`
	Type                    string `json:"type"`
	Configured              bool   `json:"configured"`
	Running                 bool   `json:"running"`
	Connected               bool   `json:"connected"`
	SecretConfigured        bool   `json:"secretConfigured"`
	AppID                   string `json:"appId,omitempty"`
	Environment             string `json:"environment"`
	GatewayState            string `json:"gatewayState"`
	ConnectionState         string `json:"connectionState"`
	SessionIDShort          string `json:"sessionIdShort,omitempty"`
	LastHelloAt             string `json:"lastHelloAt,omitempty"`
	HeartbeatIntervalMs     int    `json:"heartbeatIntervalMs,omitempty"`
	LastHeartbeatAt         string `json:"lastHeartbeatAt,omitempty"`
	LastHeartbeatAckAt      string `json:"lastHeartbeatAckAt,omitempty"`
	LastDispatchAt          string `json:"lastDispatchAt,omitempty"`
	LastConnectedAt         string `json:"lastConnectedAt,omitempty"`
	LastDisconnectedAt      string `json:"lastDisconnectedAt,omitempty"`
	ReconnectCount          int    `json:"reconnectCount"`
	AccessTokenExpiresAt    string `json:"accessTokenExpiresAt,omitempty"`
	AllowedUserCount        int    `json:"allowedUserCount"`
	AllowedGroupCount       int    `json:"allowedGroupCount"`
	AllowedGroupMemberCount int    `json:"allowedGroupMemberCount"`
	BindingCount            int    `json:"bindingCount"`
	LastErrorCode           string `json:"lastErrorCode,omitempty"`
	LastErrorMessage        string `json:"lastErrorMessage,omitempty"`
	AutoStart               bool   `json:"autoStart"`
	GatewayReconnectEnabled bool   `json:"gatewayReconnectEnabled"`
	SendProgressUpdates     bool   `json:"sendProgressUpdates"`
	GroupTriggerMode        string `json:"groupTriggerMode"`
	CommandPrefix           string `json:"commandPrefix"`
	ProxyMode               string `json:"proxyMode"`
	EffectiveProxyMode      string `json:"effectiveProxyMode"`
	MaskedProxyAddress      string `json:"maskedProxyAddress,omitempty"`
}

type DiscoveredIdentity struct {
	Type              string `json:"type"`
	DisplayName       string `json:"displayName,omitempty"`
	UserOpenID        string `json:"userOpenId,omitempty"`
	GroupOpenID       string `json:"groupOpenId,omitempty"`
	GroupMemberOpenID string `json:"groupMemberOpenId,omitempty"`
	DiscoveredAt      string `json:"discoveredAt"`
}

type AdapterEvent struct {
	Kind             string
	Code             string
	Reason           string
	ConversationType string
	ChatID           string
	UserID           string
	MessageID        string
}

type replyState struct {
	MessageID string
	CreatedAt time.Time
	Count     int
}

type Adapter struct {
	mu               sync.Mutex
	writeMu          sync.Mutex
	sendMu           sync.Mutex
	config           ConfigureRequest
	secret           string
	status           AdapterStatus
	handler          func(context.Context, channels.InboundMessage)
	eventHandler     func(AdapterEvent)
	diagnosticLogger func(string, ...any)
	httpClient       *http.Client
	tokens           *TokenProvider
	client           *officialClient
	dialer           *websocket.Dialer
	runCtx           context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	conn             *websocket.Conn
	runID            uint64
	sessionID        string
	sequence         int64
	gatewayURL       string
	replies          map[string]*replyState
	dedup            map[string]time.Time
	discovered       []DiscoveredIdentity
}

func NewAdapter(handler func(context.Context, channels.InboundMessage)) *Adapter {
	return &Adapter{
		handler: handler, replies: make(map[string]*replyState), dedup: make(map[string]time.Time),
		status: AdapterStatus{ChannelType: "qqbot", Type: "qqbot", Environment: "production", GatewayState: "not-configured", ConnectionState: "not-configured", GroupTriggerMode: "official-at", CommandPrefix: "/codex", ProxyMode: "environment", EffectiveProxyMode: "environment", GatewayReconnectEnabled: true},
	}
}

func (a *Adapter) Type() string { return "qqbot" }

func (a *Adapter) Status() channels.Status {
	status := a.QQBotStatus()
	return channels.Status{Type: "qqbot", Running: status.Running, State: status.GatewayState, LastError: status.LastErrorMessage}
}

func (a *Adapter) QQBotStatus() AdapterStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *Adapter) Configure(request ConfigureRequest) (AdapterStatus, error) {
	normalized, err := normalizeConfiguration(request)
	if err != nil {
		return AdapterStatus{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status.Running {
		if normalized.Enabled != a.config.Enabled || normalized.AppID != a.config.AppID ||
			normalized.Environment != a.config.Environment {
			return AdapterStatus{}, errors.New("stop QQ Official Bot before changing AppID, environment, or enabled state")
		}
		a.config = normalized
		a.applyConfigurationStatusLocked(normalized)
		return a.status, nil
	}
	a.config = normalized
	a.rebuildClientsLocked()
	a.applyConfigurationStatusLocked(normalized)
	if !a.status.Configured {
		a.setStateLocked("not-configured")
	} else {
		a.setStateLocked("stopped")
	}
	return a.status, nil
}

func (a *Adapter) applyConfigurationStatusLocked(config ConfigureRequest) {
	a.status.Configured = config.Enabled && config.AppID != "" && a.secret != ""
	a.status.AppID = config.AppID
	a.status.Environment = config.Environment
	a.status.SecretConfigured = a.secret != ""
	a.status.AutoStart = config.AutoStart
	a.status.GatewayReconnectEnabled = config.GatewayReconnectEnabled
	a.status.SendProgressUpdates = config.SendProgressUpdates
	a.status.GroupTriggerMode = config.GroupTriggerMode
	a.status.CommandPrefix = config.CommandPrefix
	a.status.ProxyMode = config.ProxyMode
	a.status.EffectiveProxyMode = config.ProxyMode
	a.status.MaskedProxyAddress = maskProxy(config.ProxyURL)
	a.status.AllowedUserCount = len(config.AllowedUserOpenIDs)
	a.status.AllowedGroupCount = len(config.AllowedGroupOpenIDs)
	a.status.AllowedGroupMemberCount = len(config.AllowedGroupMemberOpenIDs)
}

func (a *Adapter) SetSecret(secret string) (AdapterStatus, error) {
	if strings.TrimSpace(secret) == "" {
		return AdapterStatus{}, newError("secret_invalid", "QQ Bot AppSecret is required", nil)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status.Running {
		return AdapterStatus{}, errors.New("QQ Official Bot must be stopped before changing AppSecret")
	}
	a.secret = secret
	a.rebuildClientsLocked()
	a.status.SecretConfigured = true
	a.status.Configured = a.config.Enabled && a.config.AppID != ""
	if a.status.Configured {
		a.setStateLocked("stopped")
	}
	return a.status, nil
}

func (a *Adapter) ClearSecret(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.status.Running {
		return errors.New("QQ Official Bot is still running")
	}
	a.secret = ""
	if a.tokens != nil {
		a.tokens.Invalidate()
	}
	a.tokens, a.client = nil, nil
	a.status.SecretConfigured = false
	a.status.Configured = false
	a.status.AccessTokenExpiresAt = ""
	a.setStateLocked("not-configured")
	return nil
}

func (a *Adapter) SetBindingCount(count int) {
	a.mu.Lock()
	a.status.BindingCount = count
	a.mu.Unlock()
}

func (a *Adapter) SetEventHandler(handler func(AdapterEvent)) {
	a.mu.Lock()
	a.eventHandler = handler
	a.mu.Unlock()
}

func (a *Adapter) SetDiagnosticLogger(logger func(string, ...any)) {
	a.mu.Lock()
	a.diagnosticLogger = logger
	a.mu.Unlock()
}

func (a *Adapter) Test(ctx context.Context, _ TestRequest) TestResult {
	a.mu.Lock()
	appID, secret, cfg := a.config.AppID, a.secret, a.config
	a.mu.Unlock()
	if appID == "" {
		return TestResult{Code: "qqbot_appid_invalid", Message: "请先填写有效的 QQ Bot AppID。"}
	}
	if secret == "" {
		return TestResult{Code: "qqbot_credentials_missing", Message: "请先安全保存 QQ Bot AppSecret。"}
	}
	httpClient, _, err := buildNetworkClients(cfg.ProxyMode, cfg.ProxyURL)
	if err != nil {
		return TestResult{Code: ClassifyError(err), Message: safeChineseError(err)}
	}
	provider := NewTokenProvider(httpClient, appID, secret, nil)
	token, expiry, err := provider.Token(ctx, true)
	if err != nil {
		return TestResult{Code: ClassifyError(err), AppID: appID, Environment: "production", Message: safeChineseError(err)}
	}
	client := newOfficialClient(httpClient, provider)
	a.prepareOfficialClient(client, cfg)
	_ = token
	gatewayURL, err := client.gateway(ctx)
	if err != nil {
		return TestResult{Code: ClassifyError(err), AppID: appID, Environment: "production", TokenExpiresIn: int64(time.Until(expiry).Seconds()), Message: safeChineseError(err)}
	}
	return TestResult{Success: true, AppID: appID, Environment: "production", GatewayAvailable: true, TokenExpiresIn: int64(time.Until(expiry).Seconds()), GatewayHost: gatewayHost(gatewayURL), Message: "凭据有效，QQ Gateway 可用；未启动长期连接，也未发送消息。"}
}

func (a *Adapter) TestNetwork(ctx context.Context) NetworkTestResult {
	start := time.Now()
	result := a.Test(ctx, TestRequest{})
	status := a.QQBotStatus()
	return NetworkTestResult{Success: result.Success, Code: result.Code, Message: result.Message, DurationMs: time.Since(start).Milliseconds(), EffectiveProxyMode: status.EffectiveProxyMode, MaskedProxyAddress: status.MaskedProxyAddress}
}

func (a *Adapter) DiscoveredIdentities() []DiscoveredIdentity {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := make([]DiscoveredIdentity, len(a.discovered))
	copy(result, a.discovered)
	return result
}

func (a *Adapter) Start(parent context.Context) error {
	a.mu.Lock()
	if a.status.Running {
		a.mu.Unlock()
		return errors.New("QQ Official Bot is already started")
	}
	if !a.config.Enabled || a.config.AppID == "" || a.secret == "" || a.client == nil {
		a.mu.Unlock()
		return newError("credentials_missing", "QQ Official Bot credentials are missing", nil)
	}
	a.runID++
	runID := a.runID
	a.runCtx, a.cancel = context.WithCancel(parent)
	a.done = make(chan struct{})
	a.status.Running = true
	a.status.Connected = false
	a.status.LastErrorCode = ""
	a.status.LastErrorMessage = ""
	a.setStateLocked("authenticating")
	ctx := a.runCtx
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "authenticating"})

	conn, interval, err := a.connectGateway(ctx, runID, false)
	if err != nil {
		a.failStart(runID, err)
		return err
	}
	go a.supervise(ctx, runID, conn, interval)
	return nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	a.mu.Lock()
	if !a.status.Running {
		a.status.Connected = false
		if a.status.Configured {
			a.setStateLocked("stopped")
		} else {
			a.setStateLocked("not-configured")
		}
		a.mu.Unlock()
		return nil
	}
	a.setStateLocked("stopping")
	cancel, conn, done := a.cancel, a.conn, a.done
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
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

func (a *Adapter) SendMessage(ctx context.Context, message channels.OutboundMessage) (channels.OutboundResult, error) {
	text := strings.TrimSpace(message.Text)
	if text == "" {
		return channels.OutboundResult{}, newError("message_send_failed", "QQ message text is empty", nil)
	}
	// QQ requires msg_seq to be monotonic for each passive-reply msg_id.  Keep
	// allocation and HTTP delivery serialized so concurrent progress/final
	// replies cannot race and reuse the same sequence number.
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	a.mu.Lock()
	if !a.status.Running || a.client == nil {
		a.mu.Unlock()
		return channels.OutboundResult{}, newError("message_send_failed", "QQ Official Bot is not running", nil)
	}
	client := a.client
	key := replyKey(message.Address)
	reply := a.replies[key]
	msgID, seq := "", 1
	if reply != nil && time.Since(reply.CreatedAt) <= passiveReplyTTL && reply.Count < passiveReplyLimit {
		reply.Count++
		msgID, seq = reply.MessageID, reply.Count
	}
	a.mu.Unlock()
	result, diagnostic, err := client.sendText(ctx, qqbotConversationType(message.Address.ConversationType), message.Address.ChatID, text, msgID, seq)
	for err != nil && msgID != "" && isDuplicateReplyError(err) {
		a.mu.Lock()
		reply = a.replies[key]
		if reply == nil || time.Since(reply.CreatedAt) > passiveReplyTTL || reply.Count >= passiveReplyLimit {
			a.mu.Unlock()
			break
		}
		reply.Count++
		seq = reply.Count
		a.mu.Unlock()
		result, diagnostic, err = client.sendText(ctx, qqbotConversationType(message.Address.ConversationType), message.Address.ChatID, text, msgID, seq)
	}
	if err != nil {
		a.logMessageSendDiagnostic(diagnostic, err)
		a.recordError(err)
		a.emit(AdapterEvent{Kind: "action_failed", Code: ClassifyError(err), ConversationType: message.Address.ConversationType, ChatID: message.Address.ChatID, UserID: message.Address.UserID})
		return channels.OutboundResult{}, err
	}
	a.mu.Lock()
	a.status.LastErrorCode = ""
	a.status.LastErrorMessage = ""
	a.mu.Unlock()
	messageID := strings.TrimSpace(result.ID)
	if messageID == "" {
		messageID = strings.TrimSpace(result.MsgID)
	}
	a.emit(AdapterEvent{Kind: "message_sent", ConversationType: message.Address.ConversationType, ChatID: message.Address.ChatID, UserID: message.Address.UserID, MessageID: messageID})
	return channels.OutboundResult{MessageID: messageID}, nil
}

func (a *Adapter) logMessageSendDiagnostic(diagnostic messageSendDiagnostic, err error) {
	a.mu.Lock()
	logger := a.diagnosticLogger
	a.mu.Unlock()
	if logger == nil {
		return
	}
	httpStatus, qqCode, qqErrCode := 0, 0, 0
	message, traceID := sanitizeDiagnosticText(err.Error()), ""
	var typed *Error
	if asQQBotError(err, &typed) {
		httpStatus, qqCode, qqErrCode = typed.HTTPStatus, typed.QQCode, typed.QQErrCode
		if typed.QQMessage != "" {
			message = sanitizeDiagnosticText(typed.QQMessage)
		}
		traceID = sanitizeDiagnosticText(typed.TraceID)
	}
	msgSeq := "omitted"
	if diagnostic.MsgSeqPresent {
		msgSeq = fmt.Sprint(diagnostic.MsgSeq)
	}
	logger("QQBot message request failed: endpoint=%s conversation_type=%s target_openid_type=%s msg_type=%d msg_id_present=%t event_id_present=%t msg_seq=%s http_status=%d qq_code=%d qq_err_code=%d message=%q trace_id=%s delivery_mode=%s",
		diagnostic.Endpoint, diagnostic.ConversationType, diagnostic.TargetOpenIDType, diagnostic.MsgType,
		diagnostic.MsgIDPresent, diagnostic.EventIDPresent, msgSeq, httpStatus, qqCode, qqErrCode,
		message, traceID, diagnostic.DeliveryMode)
}

func (a *Adapter) EditMessage(context.Context, string, channels.OutboundMessage) (channels.OutboundResult, error) {
	return channels.OutboundResult{}, errors.New("QQ Official Bot does not support editing text replies in this adapter")
}

func (a *Adapter) rebuildClientsLocked() {
	httpClient, dialer, err := buildNetworkClients(a.config.ProxyMode, a.config.ProxyURL)
	if err != nil {
		a.httpClient, a.dialer, a.tokens, a.client = nil, nil, nil, nil
		a.status.LastErrorCode = ClassifyError(err)
		a.status.LastErrorMessage = safeChineseError(err)
		a.conn = nil
		a.cancel = nil
		a.runCtx = nil
		return
	}
	a.httpClient, a.dialer = httpClient, dialer
	a.tokens = NewTokenProvider(httpClient, a.config.AppID, a.secret, func(expiry time.Time) {
		a.mu.Lock()
		a.status.AccessTokenExpiresAt = expiry.UTC().Format(time.RFC3339Nano)
		a.mu.Unlock()
		a.emit(AdapterEvent{Kind: "token_refreshed"})
	})
	a.tokens.proxyMode = normalizeProxyMode(a.config.ProxyMode)
	a.tokens.usingProxy = proxyUsedForURL(a.tokens.proxyMode, a.config.ProxyURL, tokenEndpoint)
	a.tokens.onDiagnostic = a.logTokenDiagnostic
	a.client = newOfficialClient(httpClient, a.tokens)
	a.prepareOfficialClient(a.client, a.config)
}

func (a *Adapter) prepareOfficialClient(client *officialClient, config ConfigureRequest) {
	client.proxyMode = normalizeProxyMode(config.ProxyMode)
	client.usingProxy = proxyUsedForURL(client.proxyMode, config.ProxyURL, apiBaseProduction+gatewayEndpoint)
	client.onGatewayDiagnostic = a.logGatewayDiagnostic
}

func (a *Adapter) logGatewayDiagnostic(diagnostic gatewayDiagnostic) {
	a.mu.Lock()
	logger := a.diagnosticLogger
	a.mu.Unlock()
	if logger == nil {
		return
	}
	phase := diagnostic.Phase
	if phase == "" {
		phase = "gateway-http"
	}
	logger("QQBot gateway request: phase=%s host=%s path=%s method=%s authorization_present=%t authorization_scheme=%s token_length=%d http_status=%d qq_code=%d qq_err_code=%d qq_message=%q trace_id=%s network_category=%s proxy_mode=%s using_proxy=%t",
		phase, diagnostic.RequestHost, diagnostic.RequestPath, diagnostic.RequestMethod,
		diagnostic.AuthorizationPresent, diagnostic.AuthorizationScheme, diagnostic.TokenLength,
		diagnostic.HTTPStatus, diagnostic.QQCode, diagnostic.QQErrCode,
		sanitizeDiagnosticText(diagnostic.QQMessage), sanitizeDiagnosticText(diagnostic.TraceID),
		diagnostic.NetworkCategory, diagnostic.ProxyMode, diagnostic.UsingProxy)
}

func (a *Adapter) logTokenDiagnostic(diagnostic tokenDiagnostic) {
	a.mu.Lock()
	logger := a.diagnosticLogger
	a.mu.Unlock()
	if logger == nil {
		return
	}
	logger("QQBot token request: host=%s path=%s method=%s http_status=%d qq_code=%d qq_err_code=%d qq_message=%q trace_id=%s network_category=%s response_kind=%s proxy_mode=%s using_proxy=%t",
		diagnostic.RequestHost, diagnostic.RequestPath, diagnostic.RequestMethod,
		diagnostic.HTTPStatus, diagnostic.QQCode, diagnostic.QQErrCode,
		sanitizeDiagnosticText(diagnostic.QQMessage), sanitizeDiagnosticText(diagnostic.TraceID),
		diagnostic.NetworkCategory, diagnostic.ResponseKind, diagnostic.ProxyMode, diagnostic.UsingProxy)
}

func proxyUsedForURL(mode, rawProxyURL, target string) bool {
	switch normalizeProxyMode(mode) {
	case "direct":
		return false
	case "custom-http":
		return strings.TrimSpace(rawProxyURL) != ""
	default:
		request, err := http.NewRequest(http.MethodGet, target, nil)
		if err != nil {
			return false
		}
		proxyURL, err := http.ProxyFromEnvironment(request)
		return err == nil && proxyURL != nil
	}
}

func buildNetworkClients(mode, rawURL string) (*http.Client, *websocket.Dialer, error) {
	mode = normalizeProxyMode(mode)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := *websocket.DefaultDialer
	switch mode {
	case "direct":
		transport.Proxy = nil
		dialer.Proxy = nil
	case "custom-http":
		parsed, err := validateProxyURL(rawURL)
		if err != nil {
			return nil, nil, err
		}
		transport.Proxy = http.ProxyURL(parsed)
		dialer.Proxy = http.ProxyURL(parsed)
	default:
		transport.Proxy = http.ProxyFromEnvironment
		dialer.Proxy = http.ProxyFromEnvironment
	}
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &http.Client{Transport: transport, Timeout: 20 * time.Second}, &dialer, nil
}

func normalizeConfiguration(request ConfigureRequest) (ConfigureRequest, error) {
	request.AppID = strings.TrimSpace(request.AppID)
	if request.Enabled && !validAppID(request.AppID) {
		return ConfigureRequest{}, newError("appid_invalid", "QQ Bot AppID must contain 5 to 32 digits", nil)
	}
	request.Environment = strings.ToLower(strings.TrimSpace(request.Environment))
	if request.Environment == "" {
		request.Environment = "production"
	}
	if request.Environment != "production" {
		return ConfigureRequest{}, newError("protocol_incompatible", "The current QQ Official Bot protocol supports production environment only", nil)
	}
	request.AllowedUserOpenIDs = normalizeOpenIDs(request.AllowedUserOpenIDs)
	request.AllowedGroupOpenIDs = normalizeOpenIDs(request.AllowedGroupOpenIDs)
	request.AllowedGroupMemberOpenIDs = normalizeOpenIDs(request.AllowedGroupMemberOpenIDs)
	request.GroupTriggerMode = "official-at"
	request.CommandPrefix = strings.TrimSpace(request.CommandPrefix)
	if request.CommandPrefix == "" {
		request.CommandPrefix = "/codex"
	}
	request.ProxyMode = normalizeProxyMode(request.ProxyMode)
	request.ProxyURL = strings.TrimSpace(request.ProxyURL)
	if request.ProxyMode == "custom-http" {
		if _, err := validateProxyURL(request.ProxyURL); err != nil {
			return ConfigureRequest{}, err
		}
	} else {
		request.ProxyURL = ""
	}
	return request, nil
}

func validAppID(value string) bool {
	if len(value) < 5 || len(value) > 32 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func normalizeOpenIDs(values []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > 256 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func normalizeProxyMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "direct":
		return "direct"
	case "custom-http":
		return "custom-http"
	default:
		return "environment"
	}
}

func validateProxyURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, newError("proxy_failed", "Custom QQ proxy must be an HTTP URL without credentials, path, query, or fragment", err)
	}
	return parsed, nil
}

func maskProxy(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

func (a *Adapter) setStateLocked(state string) {
	a.status.GatewayState = state
	a.status.ConnectionState = state
}

func (a *Adapter) emit(event AdapterEvent) {
	a.mu.Lock()
	handler := a.eventHandler
	a.mu.Unlock()
	if handler != nil {
		handler(event)
	}
}

func (a *Adapter) recordError(err error) {
	a.mu.Lock()
	a.status.LastErrorCode = ClassifyError(err)
	a.status.LastErrorMessage = safeChineseError(err)
	a.mu.Unlock()
}

func (a *Adapter) failStart(runID uint64, err error) {
	a.mu.Lock()
	if a.runID == runID {
		a.status.Running = false
		a.status.Connected = false
		a.status.LastErrorCode = ClassifyError(err)
		a.status.LastErrorMessage = safeChineseError(err)
		state := "gateway-failed"
		if strings.Contains(ClassifyError(err), "auth") || strings.Contains(ClassifyError(err), "secret") || strings.Contains(ClassifyError(err), "credentials") {
			state = "authentication-failed"
		}
		a.setStateLocked(state)
		if a.done != nil {
			close(a.done)
			a.done = nil
		}
	}
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "error", Code: ClassifyError(err), Reason: safeChineseError(err)})
}

func (a *Adapter) rememberIdentity(identity DiscoveredIdentity) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := identity.Type + "\x00" + identity.UserOpenID + "\x00" + identity.GroupOpenID + "\x00" + identity.GroupMemberOpenID
	for index, existing := range a.discovered {
		existingKey := existing.Type + "\x00" + existing.UserOpenID + "\x00" + existing.GroupOpenID + "\x00" + existing.GroupMemberOpenID
		if existingKey == key {
			a.discovered = append(a.discovered[:index], a.discovered[index+1:]...)
			break
		}
	}
	a.discovered = append([]DiscoveredIdentity{identity}, a.discovered...)
	if len(a.discovered) > discoveredIdentityLimit {
		a.discovered = a.discovered[:discoveredIdentityLimit]
	}
}

func containsID(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func replyKey(address channels.ChannelAddress) string {
	return strings.Join([]string{address.AccountID, qqbotConversationType(address.ConversationType), address.ChatID, address.UserID}, "\x00")
}

func safeChineseError(err error) string {
	return SafeErrorMessage(err)
}

var _ channels.ChannelAdapter = (*Adapter)(nil)

func shortSessionID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 10 {
		return value
	}
	return value[:6] + "…" + value[len(value)-4:]
}

func timeText(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func gatewayCloseError(code int, text string) error {
	switch code {
	case 4004:
		return newError("token_expired", "QQ Gateway rejected the access token", nil)
	case 4006, 4007, 4009:
		return newError("gateway_session_invalid", "QQ Gateway session is invalid", nil)
	case 4008:
		return newError("rate_limited", "QQ Gateway rate limited the connection", nil)
	case 4914, 4915:
		return newError("intent_not_enabled", "QQ Bot lacks the required Group/C2C intent permission", nil)
	default:
		return newError("gateway_closed", fmt.Sprintf("QQ Gateway closed (%d %s)", code, strings.TrimSpace(text)), nil)
	}
}
