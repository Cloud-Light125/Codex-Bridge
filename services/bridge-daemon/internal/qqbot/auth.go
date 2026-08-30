package qqbot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TokenProvider struct {
	mu           sync.Mutex
	http         *http.Client
	appID        string
	secret       string
	token        string
	expiresAt    time.Time
	refreshing   chan struct{}
	onRefresh    func(time.Time)
	endpoint     string
	proxyMode    string
	usingProxy   bool
	onDiagnostic func(tokenDiagnostic)
}

// tokenDiagnostic contains only transport and protocol metadata.  It must
// never carry an AppSecret or access token because callers write it to the
// daemon log when authentication fails.
type tokenDiagnostic struct {
	RequestHost     string
	RequestPath     string
	RequestMethod   string
	HTTPStatus      int
	QQCode          int
	QQErrCode       int
	QQMessage       string
	TraceID         string
	NetworkCategory string
	ResponseKind    string
	ProxyMode       string
	UsingProxy      bool
}

func NewTokenProvider(client *http.Client, appID, secret string, onRefresh func(time.Time)) *TokenProvider {
	// AppSecret is opaque. Preserve it byte-for-byte after the settings/DPAPI
	// boundary; only the empty check below may reject it.
	return &TokenProvider{http: client, appID: strings.TrimSpace(appID), secret: secret, onRefresh: onRefresh, endpoint: tokenEndpoint}
}

func (p *TokenProvider) Token(ctx context.Context, force bool) (string, time.Time, error) {
	for {
		p.mu.Lock()
		if !force && p.token != "" && time.Now().Before(p.expiresAt.Add(-tokenRefreshMargin)) {
			token, expiry := p.token, p.expiresAt
			p.mu.Unlock()
			return token, expiry, nil
		}
		if pending := p.refreshing; pending != nil {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", time.Time{}, newError("qqbot_token_timeout", "QQ access token refresh timed out", ctx.Err())
			case <-pending:
				force = false
				continue
			}
		}
		pending := make(chan struct{})
		p.refreshing = pending
		p.mu.Unlock()

		token, expiry, err := p.fetch(ctx)
		p.mu.Lock()
		if err == nil {
			p.token, p.expiresAt = token, expiry
		}
		p.refreshing = nil
		close(pending)
		p.mu.Unlock()
		if err != nil {
			return "", time.Time{}, err
		}
		if p.onRefresh != nil {
			p.onRefresh(expiry)
		}
		return token, expiry, nil
	}
}

func (p *TokenProvider) Invalidate() {
	p.mu.Lock()
	p.token = ""
	p.expiresAt = time.Time{}
	p.mu.Unlock()
}

func (p *TokenProvider) ExpiresAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expiresAt
}

func (p *TokenProvider) fetch(parent context.Context) (string, time.Time, error) {
	endpoint := p.endpoint
	if endpoint == "" {
		endpoint = tokenEndpoint
	}
	host, path := tokenRequestLocation(endpoint)
	if p.appID == "" {
		err := newError("qqbot_appid_invalid", "QQ Bot AppID is required", nil)
		p.reportDiagnostic(endpoint, "credentials-missing", err)
		return "", time.Time{}, err
	}
	if strings.TrimSpace(p.secret) == "" {
		err := newError("qqbot_credentials_missing", "QQ Bot AppSecret is not configured", nil)
		p.reportDiagnostic(endpoint, "credentials-missing", err)
		return "", time.Time{}, err
	}
	body, _ := json.Marshal(map[string]string{"appId": p.appID, "clientSecret": p.secret})
	ctx, cancel := context.WithTimeout(parent, defaultRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		requestErr := &Error{Code: "qqbot_protocol_error", Message: "Unable to create QQ token request", RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, Cause: err}
		p.reportDiagnostic(endpoint, "request-create-failed", requestErr)
		return "", time.Time{}, requestErr
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "CloudLight-Codex-Bridge/1.0.1")
	response, err := p.http.Do(req)
	if err != nil {
		category := networkErrorCategory(err)
		if ctx.Err() != nil {
			category = "qqbot_token_timeout"
			err = ctx.Err()
		}
		requestErr := &Error{Code: category, Message: "Unable to reach the QQ token service", RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, NetworkCategory: category, Cause: err}
		p.reportDiagnostic(endpoint, "network-error", requestErr)
		return "", time.Time{}, requestErr
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		requestErr := &Error{Code: "qqbot_protocol_error", Message: "Unable to read QQ access token response", HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(response.Header.Get("X-Tps-Trace-Id")), Cause: err}
		p.reportDiagnostic(endpoint, "response-read-failed", requestErr)
		return "", time.Time{}, requestErr
	}
	traceID := response.Header.Get("X-Tps-Trace-Id")
	var apiBody apiErrorBody
	if json.Unmarshal(raw, &apiBody) == nil && (apiBody.Code != 0 || apiBody.ErrCode != 0) {
		requestErr := tokenAPIError(response.StatusCode, apiBody, host, path, traceID)
		p.reportDiagnostic(endpoint, "qq-error-envelope", requestErr)
		return "", time.Time{}, requestErr
	}
	if response.StatusCode == http.StatusTooManyRequests {
		requestErr := &Error{Code: "qqbot_rate_limited", Message: "QQ token service rate limited the request", HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(traceID)}
		p.reportDiagnostic(endpoint, "http-error", requestErr)
		return "", time.Time{}, requestErr
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusBadRequest {
		requestErr := &Error{Code: "qqbot_secret_invalid", Message: "QQ rejected the AppID or AppSecret", HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(traceID)}
		p.reportDiagnostic(endpoint, "http-error", requestErr)
		return "", time.Time{}, requestErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		requestErr := &Error{Code: "qqbot_auth_failed", Message: fmt.Sprintf("QQ token service returned HTTP %d", response.StatusCode), HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(traceID)}
		p.reportDiagnostic(endpoint, "http-error", requestErr)
		return "", time.Time{}, requestErr
	}
	var result tokenResponse
	if err := json.Unmarshal(raw, &result); err != nil || strings.TrimSpace(result.AccessToken) == "" {
		if err == nil {
			err = fmt.Errorf("access_token is missing")
		}
		requestErr := &Error{Code: "qqbot_protocol_error", Message: "QQ token response is incompatible", HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(traceID), Cause: err}
		p.reportDiagnostic(endpoint, "protocol-incompatible", requestErr)
		return "", time.Time{}, requestErr
	}
	result.AccessToken = strings.TrimSpace(result.AccessToken)
	expires, valid := parseExpiresIn(result.ExpiresIn)
	if !valid {
		requestErr := &Error{Code: "qqbot_protocol_error", Message: "QQ token response has an invalid expires_in value", HTTPStatus: response.StatusCode, RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost, TraceID: sanitizeDiagnosticText(traceID)}
		p.reportDiagnostic(endpoint, "protocol-incompatible", requestErr)
		return "", time.Time{}, requestErr
	}
	// Successful exchanges are useful when diagnosing the full startup chain;
	// emit only transport/protocol metadata, never the access token itself.
	p.reportDiagnosticMetadata(endpoint, "success", response.StatusCode, traceID)
	return result.AccessToken, time.Now().Add(time.Duration(expires) * time.Second), nil
}

func tokenAPIError(status int, body apiErrorBody, host, path, traceID string) error {
	if body.TraceID != "" {
		traceID = body.TraceID
	}
	if body.TraceIDV2 != "" {
		traceID = body.TraceIDV2
	}
	qqCode := body.Code
	if qqCode == 0 {
		qqCode = body.ErrCode
	}
	code := "qqbot_auth_failed"
	switch qqCode {
	case 100001:
		code = "qqbot_rate_limited"
	case 100007, 10004:
		code = "qqbot_appid_invalid"
	case 100016:
		code = "qqbot_secret_invalid"
	}
	message := sanitizeDiagnosticText(body.Message)
	if message == "" {
		message = fmt.Sprintf("QQ token service returned HTTP %d", status)
	}
	return &Error{
		Code: code, Message: message, HTTPStatus: status, QQCode: body.Code, QQErrCode: body.ErrCode,
		QQMessage: message, TraceID: sanitizeDiagnosticText(traceID), RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost,
	}
}

func tokenRequestLocation(endpoint string) (string, string) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", ""
	}
	path := parsed.EscapedPath()
	if path == "" {
		path = "/"
	}
	return parsed.Hostname(), path
}

func (p *TokenProvider) reportDiagnostic(endpoint, responseKind string, err error) {
	p.mu.Lock()
	handler := p.onDiagnostic
	proxyMode, usingProxy := p.proxyMode, p.usingProxy
	secret := p.secret
	p.mu.Unlock()
	if handler == nil {
		return
	}
	host, path := tokenRequestLocation(endpoint)
	diagnostic := tokenDiagnostic{
		RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost,
		ResponseKind: responseKind, ProxyMode: proxyMode, UsingProxy: usingProxy,
	}
	var typed *Error
	if asQQBotError(err, &typed) {
		diagnostic.RequestHost = typed.RequestHost
		diagnostic.RequestPath = typed.RequestPath
		diagnostic.RequestMethod = typed.RequestMethod
		diagnostic.HTTPStatus = typed.HTTPStatus
		diagnostic.QQCode = typed.QQCode
		diagnostic.QQErrCode = typed.QQErrCode
		diagnostic.QQMessage = redactKnownSecret(typed.QQMessage, secret)
		diagnostic.TraceID = sanitizeDiagnosticText(typed.TraceID)
		diagnostic.NetworkCategory = typed.NetworkCategory
	}
	handler(diagnostic)
}

func (p *TokenProvider) reportDiagnosticMetadata(endpoint, responseKind string, status int, traceID string) {
	p.mu.Lock()
	handler := p.onDiagnostic
	proxyMode, usingProxy := p.proxyMode, p.usingProxy
	p.mu.Unlock()
	if handler == nil {
		return
	}
	host, path := tokenRequestLocation(endpoint)
	handler(tokenDiagnostic{
		RequestHost: host, RequestPath: path, RequestMethod: http.MethodPost,
		HTTPStatus: status, TraceID: sanitizeDiagnosticText(traceID), ResponseKind: responseKind,
		ProxyMode: proxyMode, UsingProxy: usingProxy,
	})
}

// redactKnownSecret covers the case where a remote service echoes a credential
// without a key name (the generic logger redactor cannot identify that form).
func redactKnownSecret(value, secret string) string {
	for _, candidate := range []string{secret, strings.TrimSpace(secret)} {
		if candidate != "" {
			value = strings.ReplaceAll(value, candidate, "[REDACTED]")
		}
	}
	return sanitizeDiagnosticText(value)
}

func parseExpiresIn(raw json.RawMessage) (int64, bool) {
	value := strings.Trim(strings.TrimSpace(string(raw)), "\"")
	expires, err := strconv.ParseInt(value, 10, 64)
	if err != nil || expires <= 0 {
		return 0, false
	}
	return expires, true
}
