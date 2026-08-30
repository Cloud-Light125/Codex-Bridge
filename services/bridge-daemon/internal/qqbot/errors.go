package qqbot

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	bridgelog "cloudlight.dev/codexbridge/bridge-daemon/internal/logging"
)

type Error struct {
	Code                 string
	Message              string
	HTTPStatus           int
	QQCode               int
	QQErrCode            int
	QQMessage            string
	TraceID              string
	RequestHost          string
	RequestPath          string
	RequestMethod        string
	AuthorizationPresent bool
	AuthorizationScheme  string
	TokenLength          int
	NetworkCategory      string
	ProxyMode            string
	UsingProxy           bool
	Cause                error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// BridgeErrorCode lets platform-neutral services expose a stable, sanitized
// state without depending on QQ response bodies.
func (e *Error) BridgeErrorCode() string {
	if e == nil {
		return ""
	}
	return e.Code
}

func newError(code, message string, cause error) error {
	return &Error{Code: code, Message: message, Cause: cause}
}

func httpError(code, message string, status int) error {
	return &Error{Code: code, Message: message, HTTPStatus: status}
}

func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	var typed *Error
	if errors.As(err, &typed) && typed.Code != "" {
		return typed.Code
	}
	return networkErrorCategory(err)
}

func networkErrorCategory(err error) string {
	if err == nil {
		return "unknown_qqbot_error"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "network_timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns_failed"
	}
	var recordErr tls.RecordHeaderError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var certificateErr x509.CertificateInvalidError
	if errors.As(err, &recordErr) || errors.As(err, &unknownAuthority) || errors.As(err, &hostnameErr) || errors.As(err, &certificateErr) {
		return "tls_failed"
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "proxyconnect"), strings.Contains(message, "proxy"):
		return "proxy_failed"
	case strings.Contains(message, "tls"), strings.Contains(message, "certificate"), strings.Contains(message, "x509"):
		return "tls_failed"
	case strings.Contains(message, "no such host"), strings.Contains(message, "server misbehaving"):
		return "dns_failed"
	case strings.Contains(message, "timeout"), strings.Contains(message, "deadline"):
		return "network_timeout"
	case strings.Contains(message, "intent"):
		return "intent_not_enabled"
	default:
		return "network_error"
	}
}

func safeAPIError(status int, body apiErrorBody, host, path, method string, auth requestAuthMetadata, traceID string) error {
	qqCode := body.Code
	if qqCode == 0 {
		qqCode = body.ErrCode
	}
	qqMessage := sanitizeDiagnosticText(body.Message)
	if traceID == "" {
		traceID = body.TraceID
	}
	if traceID == "" {
		traceID = body.TraceIDV2
	}
	traceID = sanitizeDiagnosticText(traceID)
	code := "unknown_qqbot_error"
	if path == gatewayEndpoint {
		switch status {
		case http.StatusUnauthorized:
			code = "gateway_auth_failed"
		case http.StatusForbidden:
			code = "gateway_permission_denied"
		case http.StatusNotFound:
			code = "gateway_endpoint_not_found"
		case http.StatusTooManyRequests:
			code = "rate_limited"
		default:
			code = "gateway_lookup_failed"
		}
	} else {
		switch status {
		case http.StatusUnauthorized:
			code = "token_expired"
		case http.StatusTooManyRequests:
			code = "rate_limited"
		case http.StatusForbidden:
			code = "permission_not_granted"
		case http.StatusBadRequest:
			code = "protocol_incompatible"
		}
		if qqCode == 40054005 {
			code = "qqbot_message_duplicate"
		}
	}
	message := qqMessage
	if message == "" {
		message = fmt.Sprintf("QQ Official API %s returned HTTP %d", path, status)
	}
	return &Error{
		Code: code, Message: message, HTTPStatus: status, QQCode: qqCode, QQErrCode: body.ErrCode,
		QQMessage: qqMessage, TraceID: traceID, RequestHost: host, RequestPath: path, RequestMethod: method,
		AuthorizationPresent: auth.Present, AuthorizationScheme: auth.Scheme, TokenLength: auth.TokenLength,
	}
}

func SafeErrorMessage(err error) string {
	var typed *Error
	_ = errors.As(err, &typed)
	host := "api.bot.qq.com"
	if typed != nil && typed.RequestHost != "" {
		host = typed.RequestHost
	}
	detail := ""
	if typed != nil && typed.QQMessage != "" {
		detail = "：" + typed.QQMessage
	}
	status := 0
	qqCode := 0
	if typed != nil {
		status = typed.HTTPStatus
		qqCode = typed.QQCode
		if qqCode == 0 {
			qqCode = typed.QQErrCode
		}
	}
	switch ClassifyError(err) {
	case "credentials_missing", "qqbot_credentials_missing":
		return "缺少 AppID 或 AppSecret，请先保存凭据。"
	case "appid_invalid", "qqbot_appid_invalid":
		if typed != nil && qqCode != 0 {
			return fmt.Sprintf("QQ 访问凭证请求返回 HTTP %d、错误码 %d：AppID 无效、机器人不存在或状态不可用。", status, qqCode)
		}
		return "AppID 格式无效，请复制 QQ 开放平台显示的 AppID。"
	case "secret_invalid", "qqbot_secret_invalid":
		if typed != nil && qqCode != 0 {
			return fmt.Sprintf("QQ 访问凭证请求返回 HTTP %d、错误码 %d：AppID 或 AppSecret 无效或已重置。", status, qqCode)
		}
		return "AppSecret 无效，请在 QQ 开放平台重新复制或生成。"
	case "qqbot_auth_failed", "auth_failed":
		return "QQ 凭据认证失败，请检查 AppID、AppSecret 和机器人状态。"
	case "gateway_auth_failed":
		return fmt.Sprintf("HTTP %d：QQ Gateway 鉴权失败，请检查 Access Token 请求和 Authorization Header%s", status, detail)
	case "gateway_permission_denied":
		return fmt.Sprintf("QQ Gateway 请求返回 %d：机器人权限或状态不允许%s", status, detail)
	case "gateway_endpoint_not_found":
		return fmt.Sprintf("QQ Gateway 请求返回 %d：Gateway API 路径与当前协议不兼容%s", status, detail)
	case "gateway_response_invalid":
		return "Gateway 返回格式无法解析。"
	case "gateway_connect_failed":
		if status > 0 {
			return fmt.Sprintf("QQ Gateway WebSocket 握手返回 HTTP %d，请检查网络、代理和机器人状态。", status)
		}
		return "QQ Gateway WebSocket 握手失败，请检查网络、代理和机器人状态。"
	case "gateway_lookup_failed", "qqbot_gateway_failed":
		if status > 0 {
			return fmt.Sprintf("QQ Gateway 请求返回 HTTP %d%s", status, detail)
		}
		return "无法获取 QQ Gateway，请检查机器人状态、权限和网络。"
	case "network_timeout", "qqbot_token_timeout":
		return fmt.Sprintf("连接 %s 超时，请检查网络或代理。", host)
	case "dns_failed":
		return fmt.Sprintf("无法解析 %s，请检查 DNS。", host)
	case "tls_failed":
		return fmt.Sprintf("连接 %s 的 TLS 校验失败，请检查系统时间、证书或代理。", host)
	case "proxy_failed", "qqbot_proxy_failed":
		return "代理连接失败，请检查 QQ 代理模式和地址。"
	case "network_error", "qqbot_network_error":
		return fmt.Sprintf("无法连接 %s，请检查网络或代理。", host)
	case "rate_limited", "qqbot_rate_limited":
		return "QQ 平台已限流，请稍后再试。"
	case "qqbot_message_duplicate":
		if qqCode != 0 {
			return fmt.Sprintf("QQ 被动回复序号冲突（HTTP %d、错误码 %d），已尝试递增 msg_seq。请重新发送消息以建立新的回复链。", status, qqCode)
		}
		return "QQ 被动回复序号冲突，请重新发送消息。"
	case "permission_not_granted", "intent_not_enabled":
		return "机器人应用尚未获得群聊/C2C 消息权限，请在 QQ 开放平台启用后重试。"
	case "protocol_incompatible", "qqbot_protocol_error":
		return "QQ 官方协议响应与当前版本不兼容，请查看日志并升级。"
	default:
		return "QQ 官方机器人操作失败，请检查状态与本地日志。"
	}
}

func isDuplicateReplyError(err error) bool {
	var typed *Error
	if !errors.As(err, &typed) || typed == nil {
		return false
	}
	return typed.QQCode == 40054005 || typed.QQErrCode == 40054005
}

func sanitizeDiagnosticText(value string) string {
	value = bridgelog.Redact(value)
	value = strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == '\t' || r < 0x20 {
			return ' '
		}
		return r
	}, strings.TrimSpace(value))
	if len([]rune(value)) > 240 {
		return string([]rune(value)[:240])
	}
	return value
}
