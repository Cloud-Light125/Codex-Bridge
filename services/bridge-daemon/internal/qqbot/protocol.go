package qqbot

import (
	"encoding/json"
	"time"
)

const (
	// QQ's current official Bot API uses api.bot.qq.com for the access-token
	// exchange and the HTTP Gateway lookup.  The lookup may return a WebSocket
	// URL on api.sgroup.qq.com; that host is selected by QQ and is not guessed.
	apiBaseProduction = "https://api.bot.qq.com"
	tokenEndpoint     = "https://api.bot.qq.com/app/getAppAccessToken"
	// The current official Bot API exposes the sharded gateway lookup here.
	// QQ may return a legacy api.sgroup.qq.com WebSocket URL from this call;
	// always use the URL in the response rather than constructing one locally.
	gatewayEndpoint   = "/gateway/bot"
	intentGroupAndC2C = 1 << 25
	officialTextLimit = 5000

	opDispatch       = 0
	opHeartbeat      = 1
	opIdentify       = 2
	opResume         = 6
	opReconnect      = 7
	opInvalidSession = 9
	opHello          = 10
	opHeartbeatACK   = 11
)

const (
	eventReady              = "READY"
	eventResumed            = "RESUMED"
	eventC2CMessageCreate   = "C2C_MESSAGE_CREATE"
	eventGroupAtMessage     = "GROUP_AT_MESSAGE_CREATE"
	defaultRequestTimeout   = 15 * time.Second
	defaultConnectTimeout   = 20 * time.Second
	tokenRefreshMargin      = 5 * time.Minute
	dedupTTL                = 15 * time.Minute
	discoveredIdentityLimit = 20
	passiveReplyLimit       = 4
	passiveReplyTTL         = time.Hour
)

type gatewayPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type gatewayHello struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type gatewayReady struct {
	SessionID string `json:"session_id"`
}

type messageAuthor struct {
	UserOpenID   string `json:"user_openid"`
	MemberOpenID string `json:"member_openid"`
	Username     string `json:"username"`
}

type messageEvent struct {
	ID          string        `json:"id"`
	GroupOpenID string        `json:"group_openid"`
	Content     string        `json:"content"`
	Timestamp   string        `json:"timestamp"`
	Author      messageAuthor `json:"author"`
}

type gatewayLookup struct {
	URL string `json:"url"`
}

type tokenResponse struct {
	AccessToken string          `json:"access_token"`
	ExpiresIn   json.RawMessage `json:"expires_in"`
}

type sendResponse struct {
	ID    string `json:"id"`
	MsgID string `json:"msg_id"`
}

type apiErrorBody struct {
	Code      int    `json:"code"`
	ErrCode   int    `json:"err_code"`
	Message   string `json:"message"`
	TraceID   string `json:"trace_id"`
	TraceIDV2 string `json:"traceId"`
}
