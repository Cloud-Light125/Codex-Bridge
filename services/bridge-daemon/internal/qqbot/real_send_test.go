package qqbot

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRealQQMirrorSend is intentionally opt-in because it sends one real QQ
// message. Credentials are supplied only through the child process environment
// and are never printed.
func TestRealQQMirrorSend(t *testing.T) {
	if os.Getenv("CODEX_BRIDGE_REAL_QQ_TEST") != "1" {
		t.Skip("real QQ send is disabled")
	}
	appID := strings.TrimSpace(os.Getenv("CODEX_BRIDGE_REAL_QQ_APP_ID"))
	secret := strings.TrimSpace(os.Getenv("CODEX_BRIDGE_REAL_QQ_SECRET"))
	target := strings.TrimSpace(os.Getenv("CODEX_BRIDGE_REAL_QQ_OPEN_ID"))
	conversation := qqbotConversationType(os.Getenv("CODEX_BRIDGE_REAL_QQ_CONVERSATION"))
	if appID == "" || secret == "" || target == "" {
		t.Fatal("real QQ test credentials or target are missing")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	httpClient := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	client := newOfficialClient(httpClient, NewTokenProvider(httpClient, appID, secret, nil))
	result, diagnostic, err := client.sendText(
		context.Background(), conversation, target,
		"#测试\nCloudLight Codex Bridge 1.1.5 QQ transport probe", "", 0,
	)
	if err != nil {
		httpStatus, qqCode, qqErrCode, message, traceID := 0, 0, 0, sanitizeDiagnosticText(err.Error()), ""
		var typed *Error
		if asQQBotError(err, &typed) {
			httpStatus, qqCode, qqErrCode = typed.HTTPStatus, typed.QQCode, typed.QQErrCode
			if typed.QQMessage != "" {
				message = sanitizeDiagnosticText(typed.QQMessage)
			}
			traceID = sanitizeDiagnosticText(typed.TraceID)
		}
		t.Fatalf("real QQ send failed: endpoint=%s conversation_type=%s target_openid_type=%s msg_type=%d msg_id_present=%t event_id_present=%t msg_seq=omitted http_status=%d qq_code=%d qq_err_code=%d message=%q trace_id=%s delivery_mode=%s",
			diagnostic.Endpoint, diagnostic.ConversationType, diagnostic.TargetOpenIDType, diagnostic.MsgType,
			diagnostic.MsgIDPresent, diagnostic.EventIDPresent, httpStatus, qqCode, qqErrCode, message, traceID, diagnostic.DeliveryMode)
	}
	t.Logf("real QQ send succeeded: endpoint=%s conversation_type=%s target_openid_type=%s payload=proactive-text message_id_present=%t",
		diagnostic.Endpoint, diagnostic.ConversationType, diagnostic.TargetOpenIDType,
		strings.TrimSpace(result.ID) != "" || strings.TrimSpace(result.MsgID) != "")
}
