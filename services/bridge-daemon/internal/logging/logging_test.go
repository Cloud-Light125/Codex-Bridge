package logging

import (
	"strings"
	"testing"
)

func TestRedactCredentials(t *testing.T) {
	input := "Authorization: Bearer secret-value token=abc123 password=opaque-password appSecret=qq-app-secret client_secret=qq-client-secret sk-examplecredential123"
	result := Redact(input)
	for _, secret := range []string{"secret-value", "abc123", "opaque-password", "qq-app-secret", "qq-client-secret", "sk-examplecredential123"} {
		if strings.Contains(result, secret) {
			t.Fatalf("redaction leaked %q in %q", secret, result)
		}
	}
}

func TestRedactPromptAndMessageContent(t *testing.T) {
	input := `warning request={"prompt":"do not log me","input":"secret user message","text":"assistant body","content":"message content"}`
	result := Redact(input)
	for _, secret := range []string{"do not log me", "secret user message", "assistant body", "message content"} {
		if strings.Contains(result, secret) {
			t.Fatalf("content redaction leaked %q in %q", secret, result)
		}
	}
}

func TestRedactTelegramBotURL(t *testing.T) {
	const secret = "123456:telegram-secret"
	result := Redact("Post https://api.telegram.org/bot" + secret + "/getMe failed")
	if strings.Contains(result, secret) {
		t.Fatalf("Telegram token leaked in %q", result)
	}
}
