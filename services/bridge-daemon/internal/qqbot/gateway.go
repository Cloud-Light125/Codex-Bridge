package qqbot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/channels"
	"github.com/gorilla/websocket"
)

func (a *Adapter) connectGateway(parent context.Context, runID uint64, resume bool) (*websocket.Conn, time.Duration, error) {
	a.mu.Lock()
	if a.runID != runID || a.client == nil || a.tokens == nil || a.dialer == nil {
		a.mu.Unlock()
		return nil, 0, context.Canceled
	}
	a.setStateLocked("authenticating")
	tokens, client, dialer := a.tokens, a.client, a.dialer
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "authenticating"})

	accessToken, expiry, err := tokens.Token(parent, false)
	if err != nil {
		return nil, 0, err
	}
	a.mu.Lock()
	a.status.AccessTokenExpiresAt = timeText(expiry)
	a.setStateLocked("connecting")
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "connecting"})

	gatewayURL, err := client.gateway(parent)
	if err != nil {
		return nil, 0, err
	}
	a.mu.Lock()
	a.gatewayURL = gatewayURL
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(parent, defaultConnectTimeout)
	defer cancel()
	headers := http.Header{"User-Agent": []string{"CloudLight-Codex-Bridge/1.1.4"}}
	conn, response, err := dialer.DialContext(ctx, gatewayURL, headers)
	if err != nil {
		return nil, 0, a.gatewayHandshakeError(gatewayURL, response, accessToken, client, err)
	}
	handshakeStatus := 0
	handshakeTraceID := ""
	if response != nil {
		handshakeStatus = response.StatusCode
		handshakeTraceID = response.Header.Get("X-Tps-Trace-Id")
	}
	// Record the real WebSocket upgrade status (normally HTTP 101) after the
	// dialer accepts it.  Keep the same safe auth metadata as failed handshakes:
	// never write the access token or AppSecret to the daemon log.
	a.logGatewayDiagnostic(gatewayDiagnostic{
		Phase: "websocket-handshake", RequestHost: gatewayHost(gatewayURL), RequestPath: gatewayEndpoint,
		RequestMethod: http.MethodGet, AuthorizationPresent: accessToken != "", AuthorizationScheme: "QQBot",
		TokenLength: len(accessToken), HTTPStatus: handshakeStatus, TraceID: sanitizeDiagnosticText(handshakeTraceID),
		ProxyMode: client.proxyMode, UsingProxy: client.usingProxy,
	})
	a.mu.Lock()
	if a.runID != runID || a.runCtx == nil || a.runCtx.Err() != nil {
		a.mu.Unlock()
		_ = conn.Close()
		return nil, 0, context.Canceled
	}
	a.conn = conn
	a.mu.Unlock()

	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	var hello gatewayPayload
	if err := conn.ReadJSON(&hello); err != nil {
		_ = conn.Close()
		return nil, 0, newError("gateway_connect_failed", "QQ Gateway Hello was not received", err)
	}
	if hello.Op != opHello {
		_ = conn.Close()
		return nil, 0, newError("protocol_incompatible", "QQ Gateway did not send opcode 10 Hello", nil)
	}
	var data gatewayHello
	if err := json.Unmarshal(hello.D, &data); err != nil || data.HeartbeatInterval <= 0 {
		_ = conn.Close()
		return nil, 0, newError("protocol_incompatible", "QQ Gateway Hello heartbeat interval is invalid", err)
	}
	interval := time.Duration(data.HeartbeatInterval) * time.Millisecond
	a.mu.Lock()
	a.status.LastHelloAt = timeText(time.Now())
	a.status.HeartbeatIntervalMs = data.HeartbeatInterval
	a.setStateLocked("identifying")
	sessionID, seq := a.sessionID, a.sequence
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "identifying"})

	token, _, err := tokens.Token(parent, false)
	if err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	identify := map[string]any{"op": opIdentify, "d": map[string]any{"token": "QQBot " + token, "intents": intentGroupAndC2C, "shard": [2]int{0, 1}}}
	if resume && sessionID != "" && seq > 0 {
		identify = map[string]any{"op": opResume, "d": map[string]any{"token": "QQBot " + token, "session_id": sessionID, "seq": seq}}
	}
	a.writeMu.Lock()
	err = conn.WriteJSON(identify)
	a.writeMu.Unlock()
	if err != nil {
		_ = conn.Close()
		return nil, 0, newError("gateway_identify_failed", "Unable to identify with QQ Gateway", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for {
		var payload gatewayPayload
		if err := conn.ReadJSON(&payload); err != nil {
			_ = conn.Close()
			return nil, 0, newError("gateway_identify_failed", "QQ Gateway READY was not received", err)
		}
		if payload.S != nil {
			a.mu.Lock()
			a.sequence = *payload.S
			a.mu.Unlock()
		}
		if payload.Op == opInvalidSession {
			_ = conn.Close()
			return nil, 0, newError("gateway_session_invalid", "QQ Gateway rejected the session", nil)
		}
		if payload.Op != opDispatch || (payload.T != eventReady && payload.T != eventResumed) {
			continue
		}
		if payload.T == eventReady {
			var ready gatewayReady
			if err := json.Unmarshal(payload.D, &ready); err != nil || strings.TrimSpace(ready.SessionID) == "" {
				_ = conn.Close()
				return nil, 0, newError("protocol_incompatible", "QQ Gateway READY session is invalid", err)
			}
			a.mu.Lock()
			a.sessionID = ready.SessionID
			a.mu.Unlock()
		}
		a.mu.Lock()
		a.status.LastDispatchAt = timeText(time.Now())
		a.mu.Unlock()
		break
	}
	_ = conn.SetReadDeadline(time.Time{})
	now := time.Now()
	a.mu.Lock()
	if a.runID != runID {
		a.mu.Unlock()
		_ = conn.Close()
		return nil, 0, context.Canceled
	}
	a.status.Running = true
	a.status.Connected = true
	a.status.LastConnectedAt = timeText(now)
	a.status.LastErrorCode = ""
	a.status.LastErrorMessage = ""
	a.status.SessionIDShort = shortSessionID(a.sessionID)
	a.setStateLocked("connected")
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "connected"})
	a.emit(AdapterEvent{Kind: "ready"})
	return conn, interval, nil
}

// gatewayHandshakeError turns a failed WebSocket upgrade into the same safe
// transport/protocol metadata used for the preceding /gateway HTTP request.
// QQ may return an error envelope from the upgrade endpoint, including HTTP
// 2xx responses; retain its status/code/trace without retaining response body,
// access token, or AppSecret.
func (a *Adapter) gatewayHandshakeError(gatewayURL string, response *http.Response, token string, client *officialClient, cause error) error {
	host := gatewayHost(gatewayURL)
	status, traceID := 0, ""
	var body apiErrorBody
	if response != nil {
		status = response.StatusCode
		traceID = response.Header.Get("X-Tps-Trace-Id")
		if response.Body != nil {
			raw, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
			_ = response.Body.Close()
			_ = json.Unmarshal(raw, &body)
		}
	}
	auth := requestAuthMetadata{Present: token != "", Scheme: "QQBot", TokenLength: len(token)}
	var requestErr *Error
	if body.Code != 0 || body.ErrCode != 0 {
		classified := safeAPIError(status, body, host, gatewayEndpoint, http.MethodGet, auth, traceID)
		if typed, ok := classified.(*Error); ok {
			requestErr = typed
			requestErr.ProxyMode = client.proxyMode
			requestErr.UsingProxy = client.usingProxy
		}
	}
	if requestErr == nil {
		category := networkErrorCategory(cause)
		switch status {
		case http.StatusUnauthorized:
			category = "gateway_auth_failed"
		case http.StatusForbidden:
			category = "gateway_permission_denied"
		case http.StatusNotFound:
			category = "gateway_endpoint_not_found"
		case http.StatusTooManyRequests:
			category = "rate_limited"
		case 0:
		default:
			category = "gateway_connect_failed"
		}
		requestErr = &Error{
			Code: category, Message: "Unable to connect to QQ Gateway", HTTPStatus: status,
			RequestHost: host, RequestPath: gatewayEndpoint, RequestMethod: http.MethodGet,
			AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
			TraceID: sanitizeDiagnosticText(traceID), NetworkCategory: category, ProxyMode: client.proxyMode, UsingProxy: client.usingProxy, Cause: cause,
		}
	}
	a.logGatewayDiagnostic(gatewayDiagnostic{
		Phase: "websocket-handshake", RequestHost: requestErr.RequestHost, RequestPath: requestErr.RequestPath,
		RequestMethod: requestErr.RequestMethod, AuthorizationPresent: requestErr.AuthorizationPresent,
		AuthorizationScheme: requestErr.AuthorizationScheme, TokenLength: requestErr.TokenLength,
		HTTPStatus: requestErr.HTTPStatus, QQCode: requestErr.QQCode, QQErrCode: requestErr.QQErrCode,
		QQMessage: requestErr.QQMessage, TraceID: requestErr.TraceID, NetworkCategory: requestErr.NetworkCategory,
		ProxyMode: requestErr.ProxyMode, UsingProxy: requestErr.UsingProxy,
	})
	return requestErr
}

func (a *Adapter) supervise(ctx context.Context, runID uint64, conn *websocket.Conn, interval time.Duration) {
	a.mu.Lock()
	done := a.done
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		if a.runID == runID {
			a.conn = nil
			a.cancel = nil
			a.runCtx = nil
			a.status.Running = false
			a.status.Connected = false
			a.status.LastDisconnectedAt = timeText(time.Now())
			if a.status.Configured {
				a.setStateLocked("stopped")
			} else {
				a.setStateLocked("not-configured")
			}
			a.done = nil
		}
		a.mu.Unlock()
		if done != nil {
			close(done)
		}
		a.emit(AdapterEvent{Kind: "stopped"})
	}()

	backoffs := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second}
	for {
		err := a.readConnection(ctx, conn, interval)
		_ = conn.Close()
		if ctx.Err() != nil {
			return
		}
		a.mu.Lock()
		if a.runID != runID {
			a.mu.Unlock()
			return
		}
		a.status.Connected = false
		a.status.LastDisconnectedAt = timeText(time.Now())
		a.status.LastErrorCode = ClassifyError(err)
		a.status.LastErrorMessage = safeChineseError(err)
		reconnect := a.config.GatewayReconnectEnabled
		if !reconnect {
			a.setStateLocked("gateway-failed")
		}
		a.mu.Unlock()
		a.emit(AdapterEvent{Kind: "disconnected", Code: ClassifyError(err), Reason: safeChineseError(err)})
		if !reconnect {
			return
		}

		a.mu.Lock()
		a.status.ReconnectCount++
		attempt := 1
		a.setStateLocked("reconnecting")
		if ClassifyError(err) == "gateway_session_invalid" {
			a.sessionID = ""
			a.sequence = 0
		}
		if ClassifyError(err) == "token_expired" && a.tokens != nil {
			a.tokens.Invalidate()
		}
		a.mu.Unlock()
		a.emit(AdapterEvent{Kind: "reconnecting", Code: ClassifyError(err)})
		resume := ClassifyError(err) != "gateway_session_invalid"
		for {
			delay := backoffs[min(attempt-1, len(backoffs)-1)]
			if ClassifyError(err) == "rate_limited" {
				delay = 60 * time.Second
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			var connectErr error
			conn, interval, connectErr = a.connectGateway(ctx, runID, resume)
			if connectErr == nil {
				break
			}
			err = connectErr
			a.mu.Lock()
			a.status.ReconnectCount++
			attempt++
			a.status.LastErrorCode = ClassifyError(err)
			a.status.LastErrorMessage = safeChineseError(err)
			a.setStateLocked("reconnecting")
			a.mu.Unlock()
			a.emit(AdapterEvent{Kind: "reconnecting", Code: ClassifyError(err), Reason: safeChineseError(err)})
			resume = resume && ClassifyError(err) != "gateway_session_invalid"
		}
	}
}

func (a *Adapter) readConnection(ctx context.Context, conn *websocket.Conn, interval time.Duration) error {
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	go a.heartbeatLoop(hbCtx, conn, interval, heartbeatErr)
	for {
		var payload gatewayPayload
		if err := conn.ReadJSON(&payload); err != nil {
			select {
			case hbErr := <-heartbeatErr:
				if hbErr != nil {
					return hbErr
				}
			default:
			}
			var closeErr *websocket.CloseError
			if errors.As(err, &closeErr) {
				return gatewayCloseError(closeErr.Code, closeErr.Text)
			}
			return newError("gateway_closed", "QQ Gateway connection ended", err)
		}
		if payload.S != nil {
			a.mu.Lock()
			a.sequence = *payload.S
			a.mu.Unlock()
		}
		switch payload.Op {
		case opDispatch:
			a.mu.Lock()
			a.status.LastDispatchAt = timeText(time.Now())
			a.mu.Unlock()
			if payload.T == eventC2CMessageCreate || payload.T == eventGroupAtMessage {
				a.handleMessageDispatch(ctx, payload.T, payload.D)
			}
		case opHeartbeat:
			if err := a.sendHeartbeat(conn); err != nil {
				return err
			}
		case opHeartbeatACK:
			a.mu.Lock()
			a.status.LastHeartbeatAckAt = timeText(time.Now())
			a.mu.Unlock()
		case opReconnect:
			return newError("gateway_closed", "QQ Gateway requested reconnect", nil)
		case opInvalidSession:
			return newError("gateway_session_invalid", "QQ Gateway session is invalid", nil)
		}
	}
}

func (a *Adapter) heartbeatLoop(ctx context.Context, conn *websocket.Conn, interval time.Duration, result chan<- error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			lastSent := a.status.LastHeartbeatAt
			lastAck := a.status.LastHeartbeatAckAt
			a.mu.Unlock()
			if heartbeatOutstanding(lastSent, lastAck) {
				_ = conn.Close()
				select {
				case result <- newError("gateway_closed", "QQ Gateway heartbeat acknowledgement timed out", nil):
				default:
				}
				return
			}
			if err := a.sendHeartbeat(conn); err != nil {
				_ = conn.Close()
				select {
				case result <- err:
				default:
				}
				return
			}
		}
	}
}

func heartbeatOutstanding(lastSent, lastAck string) bool {
	if lastSent == "" {
		return false
	}
	sent, sentErr := time.Parse(time.RFC3339Nano, lastSent)
	ack, ackErr := time.Parse(time.RFC3339Nano, lastAck)
	return sentErr == nil && (ackErr != nil || ack.Before(sent))
}

func (a *Adapter) sendHeartbeat(conn *websocket.Conn) error {
	a.mu.Lock()
	sequence := a.sequence
	a.mu.Unlock()
	var data any
	if sequence > 0 {
		data = sequence
	}
	a.writeMu.Lock()
	err := conn.WriteJSON(map[string]any{"op": opHeartbeat, "d": data})
	a.writeMu.Unlock()
	if err != nil {
		return newError("gateway_closed", "Unable to send QQ Gateway heartbeat", err)
	}
	a.mu.Lock()
	a.status.LastHeartbeatAt = timeText(time.Now())
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "heartbeat"})
	return nil
}

func (a *Adapter) handleMessageDispatch(ctx context.Context, eventType string, raw json.RawMessage) {
	var event messageEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		a.recordError(newError("protocol_incompatible", "Unable to decode QQ message event", err))
		a.emit(AdapterEvent{Kind: "error", Code: "protocol_incompatible"})
		return
	}
	conversation := "c2c"
	chatID, userID := strings.TrimSpace(event.Author.UserOpenID), strings.TrimSpace(event.Author.UserOpenID)
	identity := DiscoveredIdentity{Type: "c2c", DisplayName: strings.TrimSpace(event.Author.Username), UserOpenID: userID, DiscoveredAt: timeText(time.Now())}
	if eventType == eventGroupAtMessage {
		conversation = "group"
		chatID, userID = strings.TrimSpace(event.GroupOpenID), strings.TrimSpace(event.Author.MemberOpenID)
		identity = DiscoveredIdentity{Type: "group", DisplayName: strings.TrimSpace(event.Author.Username), GroupOpenID: chatID, GroupMemberOpenID: userID, DiscoveredAt: timeText(time.Now())}
	}
	if strings.TrimSpace(event.ID) == "" || chatID == "" || userID == "" {
		return
	}
	a.rememberIdentity(identity)
	a.mu.Lock()
	appID, cfg := a.config.AppID, a.config
	dedupKey := strings.Join([]string{appID, conversation, chatID, event.ID}, "\x00")
	now := time.Now()
	for key, expires := range a.dedup {
		if now.After(expires) {
			delete(a.dedup, key)
		}
	}
	if _, duplicate := a.dedup[dedupKey]; duplicate {
		a.mu.Unlock()
		return
	}
	if len(a.dedup) >= 4096 {
		for key := range a.dedup {
			delete(a.dedup, key)
			break
		}
	}
	a.dedup[dedupKey] = now.Add(dedupTTL)
	authorized := containsID(cfg.AllowedUserOpenIDs, userID)
	if conversation == "group" {
		authorized = containsID(cfg.AllowedGroupOpenIDs, chatID) && containsID(cfg.AllowedGroupMemberOpenIDs, userID)
	}
	a.mu.Unlock()
	if !authorized {
		a.emit(AdapterEvent{Kind: "rejected", Code: "unauthorized_openid", Reason: "allowlist", ConversationType: conversation, ChatID: chatID, UserID: userID, MessageID: event.ID})
		return
	}
	text := strings.TrimSpace(event.Content)
	if conversation == "group" {
		text = stripOfficialMention(text)
	}
	if text == "" {
		a.emit(AdapterEvent{Kind: "rejected", Code: "unsupported_message", Reason: "text-only", ConversationType: conversation, ChatID: chatID, UserID: userID, MessageID: event.ID})
		return
	}
	received := time.Now()
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(event.Timestamp)); err == nil {
		received = parsed
	}
	address := channels.ChannelAddress{ChannelType: "qqbot", AccountID: appID, ConversationType: conversation, ChatID: chatID, UserID: userID}
	a.mu.Lock()
	a.replies[replyKey(address)] = &replyState{MessageID: event.ID, CreatedAt: now}
	a.mu.Unlock()
	a.emit(AdapterEvent{Kind: "message_received", ConversationType: conversation, ChatID: chatID, UserID: userID, MessageID: event.ID})
	message := channels.InboundMessage{Address: address, MessageID: event.ID, UserID: userID, UserName: identity.DisplayName, Text: text, Received: received}
	if a.handler != nil {
		go a.handler(ctx, message)
	}
}

func stripOfficialMention(content string) string {
	content = strings.TrimSpace(content)
	for strings.HasPrefix(content, "<@") {
		end := strings.IndexByte(content, '>')
		if end < 0 {
			break
		}
		content = strings.TrimSpace(content[end+1:])
	}
	return content
}
