package telegram

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
)

const (
	defaultAPIBase       = "https://api.telegram.org"
	ProxyModeEnvironment = "environment"
	ProxyModeDirect      = "direct"
	ProxyModeCustomHTTP  = "custom-http"
)

var (
	ErrInvalidToken = errors.New("telegram bot token is invalid")
	ErrConflict     = errors.New("telegram polling conflict: another getUpdates consumer is active")
)

type APIError struct {
	Code       int
	Kind       string
	RetryAfter time.Duration
	Message    string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return "telegram API " + e.Kind + ": " + e.Message
	}
	return "telegram API " + e.Kind
}

type User struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

type Chat struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

type Message struct {
	MessageID       int64  `json:"message_id"`
	MessageThreadID int64  `json:"message_thread_id"`
	From            *User  `json:"from"`
	Chat            Chat   `json:"chat"`
	Text            string `json:"text"`
}

type CallbackQuery struct {
	ID      string   `json:"id"`
	From    User     `json:"from"`
	Message *Message `json:"message"`
	Data    string   `json:"data"`
}

type Update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *Message       `json:"message"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}

type BotCommand struct {
	Command     string `json:"command"`
	Description string `json:"description"`
}

type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
	observer   func(NetworkObservation)
	proxy      ProxyConfig
}

type ProxyConfig struct {
	Mode string
	URL  string
}

type NetworkObservation struct {
	Stage    string
	Duration time.Duration
	Err      error
}

func NewClient(token string) *Client {
	client, _ := NewClientWithProxy(token, ProxyConfig{Mode: ProxyModeEnvironment})
	return client
}

func NewClientWithProxy(token string, proxy ProxyConfig) (*Client, error) {
	proxy, err := normalizeProxyConfig(proxy)
	if err != nil {
		return nil, err
	}
	transport := cloneDefaultTransport()
	switch proxy.Mode {
	case ProxyModeEnvironment:
		transport.Proxy = http.ProxyFromEnvironment
	case ProxyModeDirect:
		transport.Proxy = nil
	case ProxyModeCustomHTTP:
		parsed, _ := url.Parse(proxy.URL)
		transport.Proxy = http.ProxyURL(parsed)
	}
	return &Client{
		token: strings.TrimSpace(token), baseURL: defaultAPIBase,
		httpClient: &http.Client{Transport: transport}, proxy: proxy,
	}, nil
}

func cloneDefaultTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		return transport.Clone()
	}
	return &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second,
	}
}

func normalizeProxyConfig(proxy ProxyConfig) (ProxyConfig, error) {
	mode := strings.ToLower(strings.TrimSpace(proxy.Mode))
	if mode == "" {
		mode = ProxyModeEnvironment
	}
	proxyURL := strings.TrimSpace(proxy.URL)
	switch mode {
	case ProxyModeEnvironment, ProxyModeDirect:
		return ProxyConfig{Mode: mode}, nil
	case ProxyModeCustomHTTP:
		parsed, err := url.ParseRequestURI(proxyURL)
		if err != nil || parsed == nil || !strings.EqualFold(parsed.Scheme, "http") || parsed.Host == "" || parsed.Hostname() == "" || parsed.Opaque != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || (parsed.RawPath != "" && parsed.RawPath != "/") {
			return ProxyConfig{}, errors.New("invalid Telegram HTTP proxy URL")
		}
		parsed.Scheme = "http"
		parsed.Path = ""
		parsed.RawPath = ""
		return ProxyConfig{Mode: mode, URL: parsed.String()}, nil
	default:
		return ProxyConfig{}, errors.New("Telegram proxyMode must be environment, direct, or custom-http")
	}
}

func maskedProxyAddress(proxy ProxyConfig) string {
	proxy, err := normalizeProxyConfig(proxy)
	if err != nil || proxy.Mode != ProxyModeCustomHTTP {
		return ""
	}
	parsed, _ := url.Parse(proxy.URL)
	port := parsed.Port()
	if port != "" {
		return "http://***:" + port
	}
	return "http://***"
}

func newClientForTest(token, baseURL string, client *http.Client) *Client {
	return &Client{token: token, baseURL: strings.TrimRight(baseURL, "/"), httpClient: client}
}

func (c *Client) setObserver(observer func(NetworkObservation)) {
	c.observer = observer
}

func (c *Client) GetMe(ctx context.Context) (User, error) {
	var result User
	err := c.call(ctx, "getMe", map[string]any{}, &result)
	return result, err
}

func (c *Client) GetUpdates(ctx context.Context, offset int64, timeoutSeconds int) ([]Update, error) {
	requestTimeout := time.Duration(timeoutSeconds+15) * time.Second
	if requestTimeout < 45*time.Second {
		requestTimeout = 45 * time.Second
	}
	requestContext, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var result []Update
	err := c.call(requestContext, "getUpdates", map[string]any{
		"offset": offset, "timeout": timeoutSeconds,
		"allowed_updates": []string{"message", "callback_query"},
	}, &result)
	return result, err
}

func (c *Client) SendMessage(ctx context.Context, message channels.OutboundMessage) (Message, error) {
	payload := messagePayload(message)
	var result Message
	err := c.call(ctx, "sendMessage", payload, &result)
	if err != nil && message.ParseMode != "" && isParseModeError(err) {
		delete(payload, "parse_mode")
		err = c.call(ctx, "sendMessage", payload, &result)
	}
	return result, err
}

func (c *Client) EditMessage(ctx context.Context, messageID string, message channels.OutboundMessage) (Message, error) {
	payload := messagePayload(message)
	payload["message_id"] = messageID
	var result Message
	err := c.call(ctx, "editMessageText", payload, &result)
	if err != nil && message.ParseMode != "" && isParseModeError(err) {
		delete(payload, "parse_mode")
		err = c.call(ctx, "editMessageText", payload, &result)
	}
	return result, err
}

func (c *Client) AnswerCallback(ctx context.Context, callbackID, text string) error {
	payload := map[string]any{"callback_query_id": callbackID}
	if strings.TrimSpace(text) != "" {
		payload["text"] = truncateRunes(text, 180)
	}
	return c.call(ctx, "answerCallbackQuery", payload, nil)
}

func (c *Client) SetMyCommands(ctx context.Context, commands []BotCommand) error {
	return c.call(ctx, "setMyCommands", map[string]any{"commands": commands}, nil)
}

func messagePayload(message channels.OutboundMessage) map[string]any {
	payload := map[string]any{"chat_id": message.Address.ChatID, "text": message.Text}
	if message.Address.TopicID != "" {
		payload["message_thread_id"] = message.Address.TopicID
	}
	if message.ParseMode != "" {
		payload["parse_mode"] = message.ParseMode
	}
	if message.Silent {
		payload["disable_notification"] = true
	}
	if len(message.Actions) > 0 {
		rows := make([][]map[string]string, 0, len(message.Actions))
		for _, row := range message.Actions {
			buttons := make([]map[string]string, 0, len(row.Buttons))
			for _, button := range row.Buttons {
				buttons = append(buttons, map[string]string{"text": button.Label, "callback_data": button.Value})
			}
			if len(buttons) > 0 {
				rows = append(rows, buttons)
			}
		}
		payload["reply_markup"] = map[string]any{"inline_keyboard": rows}
	}
	return payload
}

type apiEnvelope struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (c *Client) call(ctx context.Context, method string, payload any, target any) (resultErr error) {
	started := time.Now()
	defer func() {
		if c.observer != nil {
			c.observer(NetworkObservation{Stage: method, Duration: time.Since(started), Err: resultErr})
		}
	}()
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode telegram request: %w", err)
	}
	// The Bot API requires the token in its path. Never return the request URL
	// or an http.Client error containing that URL to callers.
	endpoint := c.baseURL + "/bot" + c.token + "/" + method
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.New("create telegram request")
	}
	request.Header.Set("Content-Type", "application/json")
	usesProxy := requestUsesProxy(c.httpClient, request, c.proxy)
	response, err := c.httpClient.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return context.Canceled
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &APIError{Kind: "timeout", Message: "request timed out"}
		}
		category := classifyNetworkFailure(err, usesProxy)
		if category == "tls" {
			return &APIError{Kind: "tls", Message: "TLS handshake failed"}
		}
		if category == "proxy-refused" {
			return &APIError{Kind: "proxy-refused", Message: "proxy connection was refused"}
		}
		return &APIError{Kind: "network", Message: "request failed"}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return &APIError{Kind: "network", Message: "read failed"}
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return &APIError{Code: response.StatusCode, Kind: "invalid-response", Message: "invalid response"}
	}
	if !envelope.OK {
		classified := classifyAPIError(response.StatusCode, envelope)
		var apiErr *APIError
		if errors.As(classified, &apiErr) && c.token != "" {
			apiErr.Message = strings.ReplaceAll(apiErr.Message, c.token, "[REDACTED]")
		}
		return classified
	}
	if target != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, target); err != nil {
			return &APIError{Kind: "invalid-response", Message: "invalid result"}
		}
	}
	return nil
}

func probeTelegram(ctx context.Context, proxy ProxyConfig) (statusCode int, duration time.Duration, resultErr error) {
	proxy, err := normalizeProxyConfig(proxy)
	if err != nil {
		return 0, 0, &APIError{Kind: "invalid-proxy", Message: "invalid proxy configuration"}
	}
	client, err := NewClientWithProxy("", proxy)
	if err != nil {
		return 0, 0, &APIError{Kind: "invalid-proxy", Message: "invalid proxy configuration"}
	}
	defer client.httpClient.CloseIdleConnections()
	return probeTelegramWithClient(ctx, proxy, client.httpClient, defaultAPIBase)
}

func probeTelegramWithClient(ctx context.Context, proxy ProxyConfig, client *http.Client, endpoint string) (statusCode int, duration time.Duration, resultErr error) {
	started := time.Now()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, time.Since(started), &APIError{Kind: "telegram-unreachable", Message: "create probe request failed"}
	}
	usesProxy := requestUsesProxy(client, request, proxy)
	response, err := client.Do(request)
	duration = time.Since(started)
	if err != nil {
		category := classifyNetworkFailure(err, usesProxy)
		return 0, duration, &APIError{Kind: category, Message: "Telegram connectivity probe failed"}
	}
	if response.Body != nil {
		response.Body.Close()
	}
	return response.StatusCode, duration, nil
}

func requestUsesProxy(client *http.Client, request *http.Request, configured ProxyConfig) bool {
	if configured.Mode == ProxyModeCustomHTTP {
		return true
	}
	if configured.Mode == ProxyModeDirect || client == nil {
		return false
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		return false
	}
	proxyURL, err := transport.Proxy(request)
	return err == nil && proxyURL != nil
}

func classifyNetworkFailure(err error, usesProxy bool) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var certificateErr x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var recordErr tls.RecordHeaderError
	if errors.As(err, &certificateErr) || errors.As(err, &hostnameErr) || errors.As(err, &recordErr) || strings.Contains(strings.ToLower(err.Error()), "tls") || strings.Contains(strings.ToLower(err.Error()), "certificate") {
		return "tls"
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "proxyconnect") || (usesProxy && (strings.Contains(lower, "connection refused") || strings.Contains(lower, "actively refused"))) {
		return "proxy-refused"
	}
	return "telegram-unreachable"
}

func classifyAPIError(status int, envelope apiEnvelope) error {
	code := envelope.ErrorCode
	if code == 0 {
		code = status
	}
	switch code {
	case http.StatusUnauthorized:
		return ErrInvalidToken
	case http.StatusConflict:
		return ErrConflict
	case http.StatusTooManyRequests:
		delay := time.Duration(envelope.Parameters.RetryAfter) * time.Second
		if delay <= 0 {
			delay = time.Second
		}
		return &APIError{Code: code, Kind: "rate-limit", RetryAfter: delay, Message: "retry later"}
	default:
		kind := "telegram-api"
		if code >= 500 {
			kind = "server"
		}
		return &APIError{Code: code, Kind: kind, Message: sanitizeDescription(envelope.Description)}
	}
}

func sanitizeDescription(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "request rejected"
	}
	return truncateRunes(value, 240)
}

func isParseModeError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusBadRequest && strings.Contains(strings.ToLower(apiErr.Message), "parse")
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func int64Text(value int64) string { return strconv.FormatInt(value, 10) }
