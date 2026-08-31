package qqbot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type officialClient struct {
	http                 *http.Client
	tokens               *TokenProvider
	baseURL              string
	userAgent            string
	allowInsecureGateway bool
	proxyMode            string
	usingProxy           bool
	onGatewayDiagnostic  func(gatewayDiagnostic)
}

type gatewayDiagnostic struct {
	Phase                string
	RequestHost          string
	RequestPath          string
	RequestMethod        string
	AuthorizationPresent bool
	AuthorizationScheme  string
	TokenLength          int
	HTTPStatus           int
	QQCode               int
	QQErrCode            int
	QQMessage            string
	TraceID              string
	NetworkCategory      string
	ProxyMode            string
	UsingProxy           bool
}

type requestAuthMetadata struct {
	Present     bool
	Scheme      string
	TokenLength int
}

func newOfficialClient(httpClient *http.Client, tokens *TokenProvider) *officialClient {
	return &officialClient{http: httpClient, tokens: tokens, baseURL: apiBaseProduction, userAgent: "CloudLight-Codex-Bridge/1.2.0"}
}

func (c *officialClient) gateway(ctx context.Context) (string, error) {
	var result gatewayLookup
	if err := c.requestJSON(ctx, http.MethodGet, gatewayEndpoint, nil, &result); err != nil {
		return "", err
	}
	parsed, parseErr := url.Parse(strings.TrimSpace(result.URL))
	if parseErr != nil || parsed.Host == "" ||
		(parsed.Scheme != "wss" && !(c.allowInsecureGateway && parsed.Scheme == "ws")) {
		err := &Error{
			Code: "gateway_response_invalid", Message: "QQ Gateway response contained an invalid WebSocket URL",
			HTTPStatus: http.StatusOK, RequestHost: c.apiHost(), RequestPath: gatewayEndpoint,
			ProxyMode: c.proxyMode, UsingProxy: c.usingProxy, Cause: parseErr,
		}
		c.reportGatewayDiagnostic(err)
		return "", err
	}
	return strings.TrimSpace(result.URL), nil
}

type messageSendDiagnostic struct {
	Endpoint         string
	ConversationType string
	TargetOpenIDType string
	MsgType          int
	MsgIDPresent     bool
	EventIDPresent   bool
	MsgSeqPresent    bool
	MsgSeq           int
	DeliveryMode     string
}

func buildTextRequest(scope, target, text, msgID string, msgSeq int) (string, map[string]any, messageSendDiagnostic) {
	scope = qqbotConversationType(scope)
	path := fmt.Sprintf("/v2/users/%s/messages", url.PathEscape(target))
	diagnostic := messageSendDiagnostic{
		Endpoint: "/v2/users/{user_openid}/messages", ConversationType: "c2c",
		TargetOpenIDType: "user_openid", MsgType: 0, DeliveryMode: "proactive",
	}
	if scope == "group" {
		path = fmt.Sprintf("/v2/groups/%s/messages", url.PathEscape(target))
		diagnostic.Endpoint = "/v2/groups/{group_openid}/messages"
		diagnostic.ConversationType = "group"
		diagnostic.TargetOpenIDType = "group_openid"
	}
	body := map[string]any{"content": text, "msg_type": 0}
	if msgID != "" {
		if msgSeq < 1 {
			msgSeq = 1
		}
		body["msg_id"] = msgID
		body["msg_seq"] = msgSeq
		diagnostic.MsgIDPresent = true
		diagnostic.MsgSeqPresent = true
		diagnostic.MsgSeq = msgSeq
		diagnostic.DeliveryMode = "event-reply"
	}
	return path, body, diagnostic
}

func (c *officialClient) sendText(ctx context.Context, scope, target, text, msgID string, msgSeq int) (sendResponse, messageSendDiagnostic, error) {
	path, body, diagnostic := buildTextRequest(scope, target, text, msgID, msgSeq)
	var result sendResponse
	if err := c.requestJSON(ctx, http.MethodPost, path, body, &result); err != nil {
		return sendResponse{}, diagnostic, err
	}
	return result, diagnostic, nil
}

func (c *officialClient) requestJSON(ctx context.Context, method, path string, body any, target any) error {
	return c.requestAttempt(ctx, method, path, body, target, false)
}

func (c *officialClient) requestAttempt(parent context.Context, method, path string, body any, target any, retried bool) error {
	token, _, err := c.tokens.Token(parent, false)
	if err != nil {
		return err
	}
	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return newError("qqbot_protocol_error", "Unable to encode QQ API request", err)
		}
	}
	ctx, cancel := context.WithTimeout(parent, defaultRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return newError("qqbot_protocol_error", "Unable to create QQ API request", err)
	}
	req.Header.Set("Authorization", "QQBot "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	auth := inspectRequestAuthorization(req, token)
	response, err := c.http.Do(req)
	if err != nil {
		category := networkErrorCategory(err)
		if ctx.Err() != nil {
			category = "network_timeout"
			err = ctx.Err()
		}
		requestErr := &Error{
			Code: category, Message: "QQ API network request failed", RequestHost: c.apiHost(), RequestPath: path, RequestMethod: method,
			AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
			NetworkCategory: category, ProxyMode: c.proxyMode, UsingProxy: c.usingProxy, Cause: err,
		}
		if path == gatewayEndpoint {
			c.reportGatewayDiagnostic(requestErr)
		}
		return requestErr
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 128*1024))
	if readErr != nil {
		requestErr := &Error{
			Code: "protocol_incompatible", Message: "Unable to read QQ API response", HTTPStatus: response.StatusCode,
			RequestHost: c.apiHost(), RequestPath: path, RequestMethod: method,
			AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
			TraceID: sanitizeDiagnosticText(response.Header.Get("X-Tps-Trace-Id")), ProxyMode: c.proxyMode, UsingProxy: c.usingProxy, Cause: readErr,
		}
		if path == gatewayEndpoint {
			c.reportGatewayDiagnostic(requestErr)
		}
		return requestErr
	}
	// QQ may return a JSON error envelope with HTTP 2xx (the token endpoint
	// does this and Gateway/API responses can follow the same convention).
	// Inspect the envelope before decoding the endpoint-specific success DTO so
	// callers retain the real QQ code/message and diagnostic trace id.
	var apiBody apiErrorBody
	if response.StatusCode >= 200 && response.StatusCode < 300 &&
		json.Unmarshal(raw, &apiBody) == nil && (apiBody.Code != 0 || apiBody.ErrCode != 0) {
		err := safeAPIError(response.StatusCode, apiBody, c.apiHost(), path, method, auth, response.Header.Get("X-Tps-Trace-Id"))
		if typed, ok := err.(*Error); ok {
			typed.ProxyMode = c.proxyMode
			typed.UsingProxy = c.usingProxy
		}
		if path == gatewayEndpoint {
			c.reportGatewayDiagnostic(err)
		}
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = json.Unmarshal(raw, &apiBody)
		err := safeAPIError(response.StatusCode, apiBody, c.apiHost(), path, method, auth, response.Header.Get("X-Tps-Trace-Id"))
		if typed, ok := err.(*Error); ok {
			typed.ProxyMode = c.proxyMode
			typed.UsingProxy = c.usingProxy
		}
		if path == gatewayEndpoint {
			c.reportGatewayDiagnostic(err)
		}
		if response.StatusCode == http.StatusUnauthorized && !retried {
			c.tokens.Invalidate()
			if _, _, refreshErr := c.tokens.Token(parent, true); refreshErr != nil {
				return newError("token_refresh_failed", "QQ access token refresh failed", refreshErr)
			}
			return c.requestAttempt(parent, method, path, body, target, true)
		}
		return err
	}
	if target == nil || len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		code := "protocol_incompatible"
		if path == gatewayEndpoint {
			code = "gateway_response_invalid"
		}
		requestErr := &Error{
			Code: code, Message: "QQ API response is incompatible", HTTPStatus: response.StatusCode,
			RequestHost: c.apiHost(), RequestPath: path, RequestMethod: method,
			AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
			TraceID:   sanitizeDiagnosticText(response.Header.Get("X-Tps-Trace-Id")),
			ProxyMode: c.proxyMode, UsingProxy: c.usingProxy, Cause: err,
		}
		if path == gatewayEndpoint {
			c.reportGatewayDiagnostic(requestErr)
		}
		return requestErr
	}
	if path == gatewayEndpoint && c.onGatewayDiagnostic != nil {
		c.onGatewayDiagnostic(gatewayDiagnostic{
			Phase:       "gateway-http",
			RequestHost: c.apiHost(), RequestPath: path, RequestMethod: method,
			AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
			HTTPStatus: response.StatusCode, TraceID: sanitizeDiagnosticText(response.Header.Get("X-Tps-Trace-Id")),
			ProxyMode: c.proxyMode, UsingProxy: c.usingProxy,
		})
	}
	return nil
}

func (c *officialClient) apiHost() string {
	parsed, err := url.Parse(c.baseURL)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func (c *officialClient) reportGatewayDiagnostic(err error) {
	if c.onGatewayDiagnostic == nil {
		return
	}
	diagnostic := gatewayDiagnostic{Phase: "gateway-http", RequestHost: c.apiHost(), RequestPath: gatewayEndpoint, RequestMethod: http.MethodGet, ProxyMode: c.proxyMode, UsingProxy: c.usingProxy}
	var typed *Error
	if asQQBotError(err, &typed) {
		diagnostic.RequestHost = typed.RequestHost
		diagnostic.RequestPath = typed.RequestPath
		diagnostic.RequestMethod = typed.RequestMethod
		diagnostic.AuthorizationPresent = typed.AuthorizationPresent
		diagnostic.AuthorizationScheme = typed.AuthorizationScheme
		diagnostic.TokenLength = typed.TokenLength
		diagnostic.HTTPStatus = typed.HTTPStatus
		diagnostic.QQCode = typed.QQCode
		diagnostic.QQErrCode = typed.QQErrCode
		diagnostic.QQMessage = sanitizeDiagnosticText(typed.QQMessage)
		diagnostic.TraceID = sanitizeDiagnosticText(typed.TraceID)
		diagnostic.NetworkCategory = typed.NetworkCategory
	}
	c.onGatewayDiagnostic(diagnostic)
}

func inspectRequestAuthorization(request *http.Request, token string) requestAuthMetadata {
	value := strings.TrimSpace(request.Header.Get("Authorization"))
	metadata := requestAuthMetadata{Present: value != "", TokenLength: len(token)}
	fields := strings.Fields(value)
	if len(fields) < 2 {
		if metadata.Present {
			metadata.Scheme = "missing-scheme"
		}
		return metadata
	}
	switch strings.ToLower(fields[0]) {
	case "qqbot":
		metadata.Scheme = "QQBot"
	case "bearer":
		metadata.Scheme = "Bearer"
	default:
		metadata.Scheme = "unrecognized"
	}
	return metadata
}

func asQQBotError(err error, target **Error) bool {
	for err != nil {
		if typed, ok := err.(*Error); ok {
			*target = typed
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func gatewayHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func unixMillis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}
