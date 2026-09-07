package commandregistry

import (
	"path/filepath"
	"strings"
	"testing"
)

func boolValue(value bool) *bool { return &value }

func TestRegistryLifecyclePersistenceAndIndependentRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.json")
	registry, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	initial := registry.List()
	if len(initial.Commands) != len(BuiltInDefaults()) {
		t.Fatalf("loaded %d defaults, want %d", len(initial.Commands), len(BuiltInDefaults()))
	}
	for _, command := range initial.Commands {
		if !command.BuiltIn || !command.Locked || !command.Enabled {
			t.Fatalf("unexpected initial command: %#v", command)
		}
		if command.Aliases == nil {
			t.Fatalf("aliases must be an empty JSON array, not null: %#v", command)
		}
	}
	if invocation, ok := registry.Resolve("/commands"); !ok || invocation.Definition.Action != ActionBridgeHelp {
		t.Fatalf("default alias was not resolved: %#v %v", invocation, ok)
	}

	if _, err := registry.SetLocked("builtin.threads", false); err != nil {
		t.Fatal(err)
	}
	threads, err := registry.Update("builtin.threads", Mutation{
		Name: "/聊天", DisplayName: "查看会话列表", Aliases: []string{"/threads"}, Description: "查看聊天编号和标题",
		ParameterHelp: "[页码]", Action: ActionThreadFailed, Enabled: boolValue(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if threads.Action != ActionThreadsList {
		t.Fatalf("built-in action changed to %s", threads.Action)
	}
	custom, err := registry.Create(Mutation{Name: "/会话", DisplayName: "会话列表", Description: "查看会话", Action: ActionThreadsList, Enabled: boolValue(true)})
	if err != nil {
		t.Fatal(err)
	}
	for _, trigger := range []string{"/聊天", "/threads", "/会话"} {
		invocation, ok := registry.Resolve(trigger + " 2")
		if !ok || invocation.Definition.Action != ActionThreadsList || len(invocation.Arguments) != 1 || invocation.Arguments[0] != "2" {
			t.Fatalf("trigger %s did not preserve action/arguments: %#v", trigger, invocation)
		}
	}
	if _, err := registry.Create(Mutation{Name: "/聊天", DisplayName: "冲突", Description: "冲突", Action: ActionThreadsList, Enabled: boolValue(true)}); err == nil || !strings.Contains(err.Error(), "已被") {
		t.Fatalf("duplicate trigger was accepted: %v", err)
	}

	if _, err := registry.SetLocked("builtin.history", false); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Update("builtin.history", Mutation{Name: "/记录", DisplayName: "查看聊天记录", Description: "查看指定聊天最近几次对话", ParameterHelp: "<聊天编号> [数量]", Action: ActionThreadHistory, Enabled: boolValue(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Update(custom.ID, Mutation{Name: "/正在", DisplayName: "会话列表", Description: "查看会话", Action: ActionThreadsList, Enabled: boolValue(false)}); err != nil {
		t.Fatal(err)
	}
	if invocation, ok := registry.Resolve("/正在"); !ok || invocation.Definition.Enabled {
		t.Fatalf("disabled command was not retained: %#v %v", invocation, ok)
	}
	if strings.Contains(registry.HelpText(), "/正在") {
		t.Fatal("dynamic help included a disabled command")
	}

	restarted, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if invocation, ok := restarted.Resolve("/聊天"); !ok || invocation.Definition.Locked {
		t.Fatalf("built-in override did not survive restart: %#v %v", invocation, ok)
	}
	if _, err := restarted.Restore("builtin.threads"); err != nil {
		t.Fatal(err)
	}
	if invocation, ok := restarted.Resolve("/threads"); !ok || !invocation.Definition.Locked || invocation.Definition.Name != "/threads" {
		t.Fatalf("built-in restore failed: %#v %v", invocation, ok)
	}
	if _, ok := restarted.Resolve("/聊天"); ok {
		t.Fatal("restored trigger still resolved")
	}
	if invocation, ok := restarted.Resolve("/记录"); !ok || invocation.Definition.ID != "builtin.history" {
		t.Fatalf("restoring threads affected history: %#v %v", invocation, ok)
	}
	if _, err := restarted.Restore(custom.ID); err != nil {
		t.Fatal(err)
	}
	if invocation, ok := restarted.Resolve("/会话"); !ok || !invocation.Definition.Enabled {
		t.Fatalf("custom baseline restore failed: %#v %v", invocation, ok)
	}
	if err := restarted.Delete("builtin.threads"); err == nil {
		t.Fatal("built-in command was deleted")
	}
	if err := restarted.Delete(custom.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.Resolve("/会话"); ok {
		t.Fatal("custom command still resolves after delete")
	}
}

func TestTelegramMenuEligibilityDoesNotLimitBridgeParsing(t *testing.T) {
	registry, err := New(filepath.Join(t.TempDir(), "commands.json"))
	if err != nil {
		t.Fatal(err)
	}
	custom, err := registry.Create(Mutation{Name: "/中文指令", DisplayName: "中文指令", Description: "可在聊天中使用", Action: ActionBridgeStatus, Enabled: boolValue(true)})
	if err != nil {
		t.Fatal(err)
	}
	if custom.TelegramMenuEligible || custom.TelegramMenuNotice == "" {
		t.Fatalf("unexpected Telegram menu state: %#v", custom)
	}
	if _, ok := registry.Resolve("/中文指令"); !ok {
		t.Fatal("Bridge did not resolve a command excluded from Telegram menu")
	}
}

func TestTaskActionCommandsAreSharedBuiltIns(t *testing.T) {
	registry := NewInMemory()
	for _, input := range []struct {
		text   string
		action string
		count  int
	}{
		{"/actions 108", ActionTaskActions, 1},
		{"/action 108 diff full", ActionTaskAction, 3},
		{"/confirm A12", ActionTaskConfirm, 1},
	} {
		invocation, ok := registry.Resolve(input.text)
		if !ok || invocation.Definition.Action != input.action || len(invocation.Arguments) != input.count {
			t.Fatalf("resolve %q: ok=%v action=%q args=%#v", input.text, ok, invocation.Definition.Action, invocation.Arguments)
		}
	}
}

func TestResolvePreservesQuotedProjectNamesAndWindowsPaths(t *testing.T) {
	registry := NewInMemory()
	invocation, ok := registry.Resolve(`/project-chats "CloudLight QQ History" 5`)
	if !ok || invocation.Definition.Action != ActionProjectChats || len(invocation.Arguments) != 2 || invocation.Arguments[0] != "CloudLight QQ History" || invocation.Arguments[1] != "5" {
		t.Fatalf("quoted project selector was not preserved: %#v %v", invocation, ok)
	}
	invocation, ok = registry.Resolve(`/project-chats "D:\\code\\Codex Bridge" 3`)
	if !ok || len(invocation.Arguments) != 2 || invocation.Arguments[0] != `D:\code\Codex Bridge` {
		t.Fatalf("Windows path was parsed as an escape sequence: %#v %v", invocation, ok)
	}
}

func TestBackendCapabilitiesFilterCommandsAndHelp(t *testing.T) {
	registry := NewInMemory()
	openClaw := registry.ListForBackend(BackendCapabilityOpenClaw)
	if len(openClaw.Commands) == 0 || len(openClaw.Actions) == 0 {
		t.Fatal("OpenClaw command projection is empty")
	}
	for _, command := range openClaw.Commands {
		if command.BackendCapability == BackendCapabilityCodex {
			t.Fatalf("Codex-only command leaked into OpenClaw projection: %#v", command)
		}
	}
	for _, action := range openClaw.Actions {
		if action.BackendCapability == BackendCapabilityCodex {
			t.Fatalf("Codex-only action leaked into OpenClaw projection: %#v", action)
		}
	}
	if !registry.ActionSupportsBackend(ActionOpenClawRefresh, BackendCapabilityOpenClaw) || registry.ActionSupportsBackend(ActionAccountQuota, BackendCapabilityOpenClaw) {
		t.Fatal("backend capability gate returned the wrong result")
	}
	if !containsCommand(openClaw.Commands, "/oc-refresh") || !containsCommand(openClaw.Commands, "/tasks") || !containsCommand(openClaw.Commands, "/new") || !containsCommand(openClaw.Commands, "/project") || containsCommand(openClaw.Commands, "/running") || containsCommand(openClaw.Commands, "/waiting") || containsCommand(openClaw.Commands, "/failed") || containsCommand(openClaw.Commands, "/quota") || !containsCommand(openClaw.Commands, "/cancel") {
		t.Fatalf("unexpected OpenClaw command projection: %#v", openClaw.Commands)
	}
	codex := registry.ListForBackend(BackendCapabilityCodex)
	if containsCommand(codex.Commands, "/oc-refresh") || !containsCommand(codex.Commands, "/tasks") || !containsCommand(codex.Commands, "/new") || !containsCommand(codex.Commands, "/project") {
		t.Fatalf("OpenClaw-only command leaked into Codex projection: %#v", codex.Commands)
	}
	help := registry.HelpTextForBackend(BackendCapabilityOpenClaw)
	if !strings.Contains(help, "/threads") || !strings.Contains(help, "/oc-refresh") || strings.Contains(help, "/quota") {
		t.Fatalf("unexpected OpenClaw help: %s", help)
	}
}

func TestHelpTextIsCompleteAndReadable(t *testing.T) {
	help := NewInMemory().HelpText()
	for _, want := range []string{
		"CloudLight Codex Bridge 指令帮助",
		"当前后端：Codex",
		"参数说明：",
		"【会话与绑定】",
		"【运行与输出】",
		"【Task Center 任务】",
		"/output [聊天编号或任务编号]",
		"/last-output [聊天编号或任务编号]",
		"/recent-projects [数量]",
		"/project-chats <项目> [数量]",
		"/tasks [状态]",
		"常用示例：",
		"/status #12",
		"/new demo 修复统计页面",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help is missing %q:\n%s", want, help)
		}
	}
	if strings.Contains(help, "/oc-refresh") {
		t.Fatal("Codex help included an OpenClaw-only command")
	}
}

func containsCommand(commands []Definition, name string) bool {
	for _, command := range commands {
		if command.Name == name {
			return true
		}
	}
	return false
}
