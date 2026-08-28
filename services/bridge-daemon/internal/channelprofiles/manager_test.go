package channelprofiles

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"cloudlight.dev/codexbridge/bridge-daemon/internal/bindings"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/conversation"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/events"
	"cloudlight.dev/codexbridge/bridge-daemon/internal/qqbot"
)

func TestSharedTelegramTokenUsesOnePhysicalPollerResource(t *testing.T) {
	manager := newTestManager(t)
	token := "123456:shared-telegram-token"
	configureTelegram(t, manager, "telegram-1", "Telegram-1", token)
	status, err := manager.Configure(ConfigureRequest{
		ID: "telegram-2", Name: "Telegram-2", Platform: PlatformTelegram, Enabled: true,
		Telegram: &TelegramConfig{Token: &token, PollingTimeoutSeconds: 30, SendProgressUpdates: true},
	})
	if err != nil {
		t.Fatalf("configure shared Telegram profile: %v", err)
	}
	if got := manager.ResourceCount(PlatformTelegram); got != 1 {
		t.Fatalf("same Telegram token created %d physical resources; want one poller resource", got)
	}
	if status.SharedWithProfileID != "telegram-1" || status.ResourceID != "telegram-1" {
		t.Fatalf("shared Telegram profile did not resolve to its canonical resource: %#v", status)
	}
}

func TestSharedQQCredentialsUseOnePhysicalGatewayResource(t *testing.T) {
	manager := newTestManager(t)
	secret := "shared-qq-secret"
	configureQQ(t, manager, "qq-1", "QQ-1", "10001", secret)
	status, err := manager.Configure(ConfigureRequest{
		ID: "qq-2", Name: "QQ-2", Platform: PlatformQQ, Enabled: true,
		QQ: defaultQQConfig("10001", &secret),
	})
	if err != nil {
		t.Fatalf("configure shared QQ profile: %v", err)
	}
	if got := manager.ResourceCount(PlatformQQ); got != 1 {
		t.Fatalf("same QQ credentials created %d physical resources; want one gateway resource", got)
	}
	if status.SharedWithProfileID != "qq-1" || status.ResourceID != "qq-1" {
		t.Fatalf("shared QQ profile did not resolve to its canonical resource: %#v", status)
	}
}

func TestTwoQQProfilesAndSharedTelegramCanRouteBothBackends(t *testing.T) {
	manager := newTestManager(t)
	telegramToken := "123456:shared-telegram-token"
	qqSecretA, qqSecretB := "qq-secret-a", "qq-secret-b"
	configureTelegram(t, manager, "telegram-1", "Telegram-1", telegramToken)
	configureQQ(t, manager, "qq-1", "QQ-1", "10001", qqSecretA)
	configureQQ(t, manager, "qq-2", "QQ-2", "10002", qqSecretB)

	routing, err := manager.SetRouting(BackendRouting{
		Codex:    BackendRoute{TelegramProfileIDs: []string{"telegram-1"}, QQProfileIDs: []string{"qq-1"}},
		OpenClaw: BackendRoute{TelegramProfileIDs: []string{"telegram-1"}, QQProfileIDs: []string{"qq-2"}},
	})
	if err != nil {
		t.Fatalf("set routing: %v", err)
	}
	if got := manager.ResourceCount(PlatformTelegram); got != 1 {
		t.Fatalf("shared Telegram route has %d resources; want one", got)
	}
	if got := manager.ResourceCount(PlatformQQ); got != 2 {
		t.Fatalf("distinct QQ credentials have %d resources; want two", got)
	}
	if len(routing.Codex.TelegramProfileIDs) != 1 || routing.Codex.TelegramProfileIDs[0] != "telegram-1" ||
		len(routing.OpenClaw.TelegramProfileIDs) != 1 || routing.OpenClaw.TelegramProfileIDs[0] != "telegram-1" ||
		len(routing.Codex.QQProfileIDs) != 1 || routing.Codex.QQProfileIDs[0] != "qq-1" ||
		len(routing.OpenClaw.QQProfileIDs) != 1 || routing.OpenClaw.QQProfileIDs[0] != "qq-2" {
		t.Fatalf("unexpected backend routing: %#v", routing)
	}
}

func TestRoutingResponsesNormalizeNullEmptyAndPartialSlices(t *testing.T) {
	manager := newTestManager(t)

	// A newly started daemon has no routing configuration yet. Both the
	// dedicated routing response and the profile-list response must still be
	// safe for clients that enumerate every route collection.
	assertRoutingSlices(t, manager.Routing())
	assertEmptyRouting(t, manager.Routing(), "unconfigured routing")
	assertRoutingJSONUsesArrays(t, manager.Routing())
	assertRoutingSlices(t, manager.List().Routing)
	assertRoutingJSONUsesArrays(t, manager.List())

	for _, test := range []struct {
		name string
		json string
	}{
		{
			name: "null fields",
			json: `{"codex":{"telegramProfileIds":null,"qqProfileIds":null},"openClaw":null}`,
		},
		{
			name: "empty arrays",
			json: `{"codex":{"telegramProfileIds":[],"qqProfileIds":[]},"openClaw":{"telegramProfileIds":[],"qqProfileIds":[]}}`,
		},
		{
			name: "both backends empty",
			json: `{"codex":{},"openClaw":{}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var input BackendRouting
			if err := json.Unmarshal([]byte(test.json), &input); err != nil {
				t.Fatalf("decode route input: %v", err)
			}
			routing, err := manager.SetRouting(input)
			if err != nil {
				t.Fatalf("set routing: %v", err)
			}
			assertRoutingSlices(t, routing)
			assertEmptyRouting(t, routing, test.name)
			assertRoutingJSONUsesArrays(t, routing)
		})
	}

	configureTelegram(t, manager, "telegram-1", "Telegram-1", "123456:routing-test-token")
	configureQQ(t, manager, "qq-1", "QQ-1", "10001", "routing-test-secret")

	var qqOnly BackendRouting
	if err := json.Unmarshal([]byte(`{"codex":{"qqProfileIds":["qq-1"]}}`), &qqOnly); err != nil {
		t.Fatalf("decode QQ-only route: %v", err)
	}
	routing, err := manager.SetRouting(qqOnly)
	if err != nil {
		t.Fatalf("set QQ-only route: %v", err)
	}
	assertRoutingSlices(t, routing)
	assertRoutingJSONUsesArrays(t, routing)
	if len(routing.Codex.TelegramProfileIDs) != 0 || len(routing.OpenClaw.TelegramProfileIDs) != 0 ||
		len(routing.OpenClaw.QQProfileIDs) != 0 || len(routing.Codex.QQProfileIDs) != 1 || routing.Codex.QQProfileIDs[0] != "qq-1" {
		t.Fatalf("unexpected QQ-only route: %#v", routing)
	}

	var telegramOnly BackendRouting
	if err := json.Unmarshal([]byte(`{"openClaw":{"telegramProfileIds":["telegram-1"]}}`), &telegramOnly); err != nil {
		t.Fatalf("decode Telegram-only route: %v", err)
	}
	routing, err = manager.SetRouting(telegramOnly)
	if err != nil {
		t.Fatalf("set Telegram-only route: %v", err)
	}
	assertRoutingSlices(t, routing)
	assertRoutingJSONUsesArrays(t, routing)
	if len(routing.Codex.TelegramProfileIDs) != 0 || len(routing.Codex.QQProfileIDs) != 0 ||
		len(routing.OpenClaw.QQProfileIDs) != 0 || len(routing.OpenClaw.TelegramProfileIDs) != 1 || routing.OpenClaw.TelegramProfileIDs[0] != "telegram-1" {
		t.Fatalf("unexpected Telegram-only route: %#v", routing)
	}
}

func TestPrepareBindingUsesExplicitBackendRoutingWithoutCrossingResources(t *testing.T) {
	manager := newTestManager(t)
	token := "123456:shared-telegram-token"
	configureTelegram(t, manager, "telegram-1", "Telegram-1", token)
	configureTelegram(t, manager, "telegram-2", "Telegram-2", token)
	if _, err := manager.SetRouting(BackendRouting{
		Codex:    BackendRoute{TelegramProfileIDs: []string{"telegram-1"}},
		OpenClaw: BackendRoute{TelegramProfileIDs: []string{"telegram-2"}},
	}); err != nil {
		t.Fatalf("set routing: %v", err)
	}

	prepared, err := manager.PrepareBinding(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, TargetID: "agent:main:session-1", ChannelType: "telegram", ChannelProfileID: "telegram-1",
		ConversationType: "default", ConversationID: "chat-1", ChatID: "chat-1",
	})
	if err != nil {
		t.Fatalf("prepare shared-resource OpenClaw binding: %v", err)
	}
	if prepared.ChannelProfileID != "telegram-2" || prepared.ResourceID != "telegram-1" {
		t.Fatalf("shared bot binding did not select the OpenClaw-routed profile while retaining its physical resource: %#v", prepared)
	}

	if _, err := manager.SetRouting(BackendRouting{Codex: BackendRoute{TelegramProfileIDs: []string{"telegram-1"}}}); err != nil {
		t.Fatalf("remove OpenClaw route: %v", err)
	}
	if _, err := manager.PrepareBinding(bindings.CreateRequest{
		Backend: conversation.BackendOpenClaw, TargetID: "agent:main:session-2", ChannelType: "telegram", ChannelProfileID: "telegram-1",
		ConversationType: "default", ConversationID: "chat-1", ChatID: "chat-1",
	}); err == nil {
		t.Fatal("OpenClaw binding must fail when no profile on the physical bot is routed to OpenClaw")
	}
}

func TestLegacyQQSecretUpgradeSharesGatewayWithProfileAPI(t *testing.T) {
	manager := newTestManager(t)
	if _, err := manager.ConfigureDefaultQQ(qqbotRequest("10001"), nil); err != nil {
		t.Fatalf("legacy QQ configure: %v", err)
	}
	if _, err := manager.SetDefaultQQSecret("shared-qq-secret"); err != nil {
		t.Fatalf("legacy QQ secret: %v", err)
	}
	secret := "shared-qq-secret"
	if _, err := manager.Configure(ConfigureRequest{ID: "qq-2", Name: "QQ-2", Platform: PlatformQQ, Enabled: true, QQ: defaultQQConfig("10001", &secret)}); err != nil {
		t.Fatalf("profile QQ configure: %v", err)
	}
	if got := manager.ResourceCount(PlatformQQ); got != 1 {
		t.Fatalf("legacy QQ migration and matching profile created %d gateways; want one", got)
	}
	routing := manager.Routing()
	if len(routing.Codex.QQProfileIDs) != 1 || routing.Codex.QQProfileIDs[0] != DefaultQQProfileID ||
		len(routing.OpenClaw.QQProfileIDs) != 1 || routing.OpenClaw.QQProfileIDs[0] != DefaultQQProfileID {
		t.Fatalf("legacy QQ API did not retain a usable default backend route: %#v", routing)
	}
}

func TestProfileMetadataDoesNotRetainCredentialInputs(t *testing.T) {
	manager := newTestManager(t)
	token, secret := "123456:secret-telegram-token", "secret-qq-app-secret"
	configureTelegram(t, manager, "telegram-1", "Telegram-1", token)
	configureQQ(t, manager, "qq-1", "QQ-1", "10001", secret)
	if profile := manager.profiles["telegram-1"]; profile == nil || profile.request.Telegram == nil || profile.request.Telegram.Token != nil {
		t.Fatalf("Telegram credential remained in profile metadata: %#v", profile)
	}
	if profile := manager.profiles["qq-1"]; profile == nil || profile.request.QQ == nil || profile.request.QQ.AppSecret != nil {
		t.Fatalf("QQ credential remained in profile metadata: %#v", profile)
	}
	payload, err := json.Marshal(manager.List())
	if err != nil {
		t.Fatalf("marshal profile status: %v", err)
	}
	if strings.Contains(string(payload), token) || strings.Contains(string(payload), secret) {
		t.Fatalf("credential leaked from profile status: %s", payload)
	}
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	repository, err := bindings.NewRepository(filepath.Join(t.TempDir(), "bindings.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(nil, nil, repository, events.NewBroker(), nil, nil, nil, nil)
	t.Cleanup(func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})
	return manager
}

func configureTelegram(t *testing.T, manager *Manager, id, name, token string) {
	t.Helper()
	if _, err := manager.Configure(ConfigureRequest{
		ID: id, Name: name, Platform: PlatformTelegram, Enabled: true,
		Telegram: &TelegramConfig{Token: &token, PollingTimeoutSeconds: 30, SendProgressUpdates: true},
	}); err != nil {
		t.Fatalf("configure Telegram %s: %v", id, err)
	}
}

func configureQQ(t *testing.T, manager *Manager, id, name, appID, secret string) {
	t.Helper()
	if _, err := manager.Configure(ConfigureRequest{
		ID: id, Name: name, Platform: PlatformQQ, Enabled: true, QQ: defaultQQConfig(appID, &secret),
	}); err != nil {
		t.Fatalf("configure QQ %s: %v", id, err)
	}
}

func defaultQQConfig(appID string, secret *string) *QQConfig {
	return &QQConfig{
		AppID: appID, AppSecret: secret, Enabled: true, Environment: "production", GroupTriggerMode: "official-at",
		CommandPrefix: "/codex", GatewayReconnectEnabled: true, SendProgressUpdates: true,
	}
}

func assertRoutingSlices(t *testing.T, routing BackendRouting) {
	t.Helper()
	for _, route := range []struct {
		name string
		ids  []string
	}{
		{"codex.telegramProfileIds", routing.Codex.TelegramProfileIDs},
		{"codex.qqProfileIds", routing.Codex.QQProfileIDs},
		{"openClaw.telegramProfileIds", routing.OpenClaw.TelegramProfileIDs},
		{"openClaw.qqProfileIds", routing.OpenClaw.QQProfileIDs},
	} {
		if route.ids == nil {
			t.Errorf("%s must be an empty slice, not nil", route.name)
		}
	}
}

func assertEmptyRouting(t *testing.T, routing BackendRouting, scenario string) {
	t.Helper()
	if len(routing.Codex.TelegramProfileIDs) != 0 || len(routing.Codex.QQProfileIDs) != 0 ||
		len(routing.OpenClaw.TelegramProfileIDs) != 0 || len(routing.OpenClaw.QQProfileIDs) != 0 {
		t.Fatalf("%s produced non-empty routing: %#v", scenario, routing)
	}
}

func assertRoutingJSONUsesArrays(t *testing.T, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal routing response: %v", err)
	}
	if strings.Contains(string(payload), "null") {
		t.Fatalf("routing response contains null instead of []: %s", payload)
	}
}

func qqbotRequest(appID string) qqbot.ConfigureRequest {
	return qqbot.ConfigureRequest{
		Enabled: true, AppID: appID, Environment: "production", GroupTriggerMode: "official-at",
		CommandPrefix: "/codex", GatewayReconnectEnabled: true, SendProgressUpdates: true,
	}
}
