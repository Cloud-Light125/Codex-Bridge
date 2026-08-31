using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using CloudLight.CodexBridge.ViewModels;

namespace CloudLight.CodexBridge.Models;

public sealed class BridgeStatus
{
    public string Version { get; set; } = "";
	public string CodexCliPath { get; set; } = "";
	public string CodexCliVersion { get; set; } = "";
    public bool CodexCliAvailable { get; set; }
    public string CodexCliPathSource { get; set; } = "";
    public string CodexCliValidationStatus { get; set; } = "";
    public string CodexCliConnectionStatus { get; set; } = "";
    public bool AppServerRunning { get; set; }
    public int AppServerPid { get; set; }
    public string LastError { get; set; } = "";
    public string StartedAt { get; set; } = "";
    public string ListenAddress { get; set; } = "";
    public string SandboxMode { get; set; } = "workspace-write";
    public string ApprovalPolicy { get; set; } = "on-request";
    public bool DangerFullAccess { get; set; }
    public bool RemoteApproval { get; set; }
}

public sealed class ThreadListResponse
{
    public List<ThreadSummary> Threads { get; set; } = [];
    public string NextCursor { get; set; } = "";
}

public class ThreadSummary
{
	public string Backend { get; set; } = "codex";
    public string ThreadId { get; set; } = "";
	public string SessionKey { get; set; } = "";
	public int Number { get; set; }
    public string Title { get; set; } = "";
    public string Summary { get; set; } = "";
    public string Cwd { get; set; } = "";
    public string Model { get; set; } = "";
    public string CreatedAt { get; set; } = "";
    public string UpdatedAt { get; set; } = "";
    public bool? Archived { get; set; }
    public string Status { get; set; } = "";
	public string NumberPrefix => Number > 0 ? $"[Codex] #{Number}" : "[Codex] #?";
	public string NumberedTitle => $"{NumberPrefix}  {Title}";
    public string CreatedAtDisplay => UiText.LocalDateTime(CreatedAt);
    public string UpdatedAtDisplay => UiText.LocalDateTime(UpdatedAt);
    public string StatusDisplay => UiText.Status(Status);
    public Visibility ModelVisibility => string.IsNullOrWhiteSpace(Model) ? Visibility.Collapsed : Visibility.Visible;
}

public sealed class ThreadDetail : ThreadSummary
{
    public List<TurnDetail> Turns { get; set; } = [];
    public ThreadRuntime Runtime { get; set; } = new();
}

public sealed class OpenClawConnectionStatus
{
    public string Backend { get; set; } = "openclaw";
    public bool Configured { get; set; }
    public bool Running { get; set; }
    public bool Connected { get; set; }
    public string State { get; set; } = "not-configured";
    public string GatewayUrl { get; set; } = "";
    public int Protocol { get; set; }
    public string ServerVersion { get; set; } = "";
    public string AuthMode { get; set; } = "";
    public int SessionCount { get; set; }
    public bool AutoReconnect { get; set; }
    public int ReconnectCount { get; set; }
    public string LastConnectedAt { get; set; } = "";
    public string LastDisconnectedAt { get; set; } = "";
    public string LastTickAt { get; set; } = "";
    public string LastError { get; set; } = "";
}

public sealed class OpenClawConfigureRequest
{
    public string GatewayUrl { get; set; } = "";
    public string Token { get; set; } = "";
    public string Password { get; set; } = "";
    public bool AutoReconnect { get; set; } = true;
    public bool? Start { get; set; } = true;
}

public sealed class OpenClawSessionListResponse
{
    public List<OpenClawSessionSummary> Sessions { get; set; } = [];
}

public static class BackendListProjection
{
    // Retain an explicit boundary even if an older daemon or test double ever
    // returns a mixed value: a Codex view never renders an OpenClaw session.
    public static IEnumerable<ThreadSummary> CodexThreads(IEnumerable<ThreadSummary>? threads) =>
        (threads ?? []).Where(thread => string.IsNullOrWhiteSpace(thread.Backend) || string.Equals(thread.Backend, "codex", StringComparison.OrdinalIgnoreCase));

    // The OpenClaw page must remain isolated even if an older daemon or test
    // double accidentally returns a mixed list.
    public static IEnumerable<OpenClawSessionSummary> OpenClawSessions(IEnumerable<OpenClawSessionSummary>? sessions) =>
        (sessions ?? []).Where(session => string.IsNullOrWhiteSpace(session.Backend) || string.Equals(session.Backend, "openclaw", StringComparison.OrdinalIgnoreCase));
}

public class OpenClawSessionSummary
{
    public string Backend { get; set; } = "openclaw";
    public string Key { get; set; } = "";
    public string SessionId { get; set; } = "";
    public string AgentId { get; set; } = "";
    public int Number { get; set; }
    public string Title { get; set; } = "";
    public string Summary { get; set; } = "";
    public string Cwd { get; set; } = "";
    public string Model { get; set; } = "";
    public string CreatedAt { get; set; } = "";
    public string UpdatedAt { get; set; } = "";
    public string Status { get; set; } = "idle";
    public bool? Archived { get; set; }
    public bool HasActiveRun { get; set; }
    public List<string> ActiveRunIds { get; set; } = [];
	public string NumberPrefix => Number > 0 ? $"[OpenClaw] #{Number}" : "[OpenClaw] #?";
	public string NumberedTitle => $"{NumberPrefix}  {Title}";
}

public sealed class OpenClawSessionDetail : OpenClawSessionSummary
{
    public List<OpenClawMessage> Messages { get; set; } = [];
}

public sealed class OpenClawMessage
{
    public string Id { get; set; } = "";
    public string Role { get; set; } = "assistant";
    public string Text { get; set; } = "";
    public string Timestamp { get; set; } = "";
    public string RunId { get; set; } = "";
}

public sealed class OpenClawSendResult
{
    public string Backend { get; set; } = "openclaw";
    public string SessionKey { get; set; } = "";
    public string RunId { get; set; } = "";
    public string Status { get; set; } = "";
    public string AcceptedAt { get; set; } = "";
}

public sealed class OpenClawAbortResult
{
    public string Backend { get; set; } = "openclaw";
    public string SessionKey { get; set; } = "";
    public string RunId { get; set; } = "";
    public string Status { get; set; } = "";
}

public sealed class ThreadRuntime
{
    public string ThreadId { get; set; } = "";
    public string State { get; set; } = "idle";
    public string TurnId { get; set; } = "";
    public string Origin { get; set; } = "";
    public string StartedAt { get; set; } = "";
    public string LastActivityAt { get; set; } = "";
    public string Error { get; set; } = "";
    public bool CanInterrupt { get; set; }
    public bool CanSend { get; set; }
    public int PendingInteractionCount { get; set; }
}

public sealed class TurnDetail
{
    public string TurnId { get; set; } = "";
    public string Status { get; set; } = "";
    public string CreatedAt { get; set; } = "";
    public string UpdatedAt { get; set; } = "";
    public List<ItemDetail> Items { get; set; } = [];
}

public sealed class ItemDetail
{
    public string ItemId { get; set; } = "";
    public string Type { get; set; } = "";
    public string Role { get; set; } = "";
    public string Phase { get; set; } = "";
    public string Text { get; set; } = "";
    public string Label { get; set; } = "";
    public string Status { get; set; } = "";
    public string Output { get; set; } = "";
}

public sealed class StartTurnRequest
{
    public string Text { get; set; } = "";
    public string CollaborationMode { get; set; } = "default";
    public string? Model { get; set; }
    public string? ReasoningEffort { get; set; }
}

public sealed class TurnAccepted
{
    public string ThreadId { get; set; } = "";
    public string TurnId { get; set; } = "";
    public string Status { get; set; } = "";
    public string AcceptedAt { get; set; } = "";
}

// This response is deliberately diagnostic-only.  It contains identifiers and
// filesystem metadata, but never rollout contents or message text.
public sealed class PersistenceVerification
{
    public string ThreadId { get; set; } = "";
    public string ExpectedTurnId { get; set; } = "";
    public string Status { get; set; } = "";
    public string Message { get; set; } = "";
    public ThreadPersistenceSnapshot? Main { get; set; }
    public RolloutPersistenceSnapshot? Rollout { get; set; }
    public ThreadPersistenceSnapshot? Probe { get; set; }
    public CodexEnvironmentDiagnostic? Environment { get; set; }
    public List<string> Warnings { get; set; } = [];
}

public sealed class ThreadPersistenceSnapshot
{
    public string ThreadId { get; set; } = "";
    public string SessionId { get; set; } = "";
    public string SourceKind { get; set; } = "";
    public string RolloutPath { get; set; } = "";
    public bool Ephemeral { get; set; }
    public string Cwd { get; set; } = "";
    public string UpdatedAt { get; set; } = "";
    public string Status { get; set; } = "";
    public string TurnStatus { get; set; } = "";
    public int TurnCount { get; set; }
    public string LastTurnId { get; set; } = "";
    public bool FoundTurn { get; set; }
    public string UserMessageItemId { get; set; } = "";
    public string AssistantMessageItemId { get; set; } = "";
}

public sealed class RolloutPersistenceSnapshot
{
    public string Path { get; set; } = "";
    public bool Exists { get; set; }
    public long BeforeLength { get; set; }
    public long AfterLength { get; set; }
    public bool LengthIncreased { get; set; }
    public bool ModifiedAfterSend { get; set; }
    public bool ContainsIdentifier { get; set; }
    public string Error { get; set; } = "";
}

public sealed class CodexEnvironmentDiagnostic
{
    public string CodexCliPath { get; set; } = "";
    public string CodexCliVersion { get; set; } = "";
    public string Username { get; set; } = "";
    public string UserProfile { get; set; } = "";
    public string Home { get; set; } = "";
    public bool CodexHomeExplicit { get; set; }
    public string CodexHome { get; set; } = "";
    public string ResolvedCodexDataRoot { get; set; } = "";
    public string AppServerWorkingDirectory { get; set; } = "";
    public bool DesktopEnvironmentKnown { get; set; }
    public List<CodexProcessDiagnostic> Processes { get; set; } = [];
    public List<string> MatchingRolloutPaths { get; set; } = [];
    public bool MultipleMatchingRollouts { get; set; }
}

public sealed class CodexProcessDiagnostic
{
    public int Pid { get; set; }
    public string Name { get; set; } = "";
    public string Username { get; set; } = "";
    public string Version { get; set; } = "";
    public string ExecutablePath { get; set; } = "";
    public string CommandLine { get; set; } = "";
}

public sealed class InterruptResult
{
    public string ThreadId { get; set; } = "";
    public string TurnId { get; set; } = "";
    public string Status { get; set; } = "";
}

public sealed class BridgeEvent
{
    public string EventId { get; set; } = "";
    public string EventType { get; set; } = "";
    public string Timestamp { get; set; } = "";
    public string ThreadId { get; set; } = "";
    public string TurnId { get; set; } = "";
    public string ItemId { get; set; } = "";
    public JsonElement Payload { get; set; }
}

public sealed class InteractionListResponse
{
    public List<PendingInteraction> Interactions { get; set; } = [];
}

public sealed class PendingInteraction
{
    public string Id { get; set; } = "";
    public string Kind { get; set; } = "unknown";
    public string ThreadId { get; set; } = "";
    public string TurnId { get; set; } = "";
    public string ItemId { get; set; } = "";
    public string Title { get; set; } = "";
    public string Description { get; set; } = "";
    public string Command { get; set; } = "";
    public string Cwd { get; set; } = "";
    public List<InteractionFileChange> FileChanges { get; set; } = [];
    public List<InteractionQuestion> Questions { get; set; } = [];
    public string CreatedAt { get; set; } = "";
    public string ExpiresAt { get; set; } = "";
    public string Status { get; set; } = "pending";
}

public sealed class InteractionFileChange
{
    public string Path { get; set; } = "";
    public string Kind { get; set; } = "";
    public string Diff { get; set; } = "";
}

public sealed class InteractionQuestion
{
    public string Id { get; set; } = "";
    public string Header { get; set; } = "";
    public string Text { get; set; } = "";
    public string Type { get; set; } = "text";
    public bool Required { get; set; }
    public List<InteractionQuestionOption> Options { get; set; } = [];
}

public sealed class InteractionQuestionOption
{
    public string Label { get; set; } = "";
    public string Value { get; set; } = "";
    public string Description { get; set; } = "";
    public bool IsOther { get; set; }
}

public sealed class InteractionResponse
{
    public string Action { get; set; } = "";
    public string? Message { get; set; }
    public Dictionary<string, string[]>? Answers { get; set; }
}

public sealed class SecuritySettingsRequest
{
    public string SandboxMode { get; set; } = "workspace-write";
}

public sealed class CodexSettingsRequest
{
    public string Path { get; set; } = "";
    public string Source { get; set; } = "Manual";
}

public sealed class RemoteCommandListResponse
{
    public int SchemaVersion { get; set; }
    public List<RemoteCommandDefinition> Commands { get; set; } = [];
    public List<RemoteCommandAction> Actions { get; set; } = [];
}

public sealed class RemoteCommandAction
{
    public string Id { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public bool TargetSupport { get; set; }
    public string BackendCapability { get; set; } = "";
}

public sealed class RemoteCommandDefinition
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public List<string> Aliases { get; set; } = [];
    public string Description { get; set; } = "";
    public string ParameterHelp { get; set; } = "";
    public string Action { get; set; } = "";
    public string BackendCapability { get; set; } = "";
    public bool BuiltIn { get; set; }
    public bool Locked { get; set; }
    public bool Enabled { get; set; }
    public bool Modified { get; set; }
    public bool CanDelete { get; set; }
    public bool CanRestore { get; set; }
    public bool TelegramMenuEligible { get; set; }
    public string TelegramMenuNotice { get; set; } = "";
}

public sealed class RemoteCommandMutation
{
    public string Name { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public List<string> Aliases { get; set; } = [];
    public string Description { get; set; } = "";
    public string ParameterHelp { get; set; } = "";
    public string Action { get; set; } = "";
    public bool Enabled { get; set; } = true;
}

public sealed class MirrorMessageTypes
{
	public bool User { get; set; }
	public bool Assistant { get; set; } = true;
	public bool Status { get; set; }
	public bool RequestUserInput { get; set; } = true;
	public bool Error { get; set; } = true;
}
public sealed class TelegramMirrorConfig { public bool Enabled { get; set; } public string ChatId { get; set; } = ""; }
public sealed class QqMirrorConfig { public bool Enabled { get; set; } public string ConversationType { get; set; } = "c2c"; public string OpenId { get; set; } = ""; }
public sealed class MirrorConfig
{
	public bool Enabled { get; set; }
	public bool RequireThreadNumber { get; set; } = true;
	public MirrorMessageTypes Messages { get; set; } = new();
	public TelegramMirrorConfig Telegram { get; set; } = new();
	public QqMirrorConfig Qq { get; set; } = new();
}
public sealed class MirrorStatus
{
	public MirrorConfig Config { get; set; } = new();
	public string TelegramState { get; set; } = "disabled";
	public string QqState { get; set; } = "disabled";
	public string QqCapabilityNotice { get; set; } = "";
	public string LastTelegramError { get; set; } = "";
	public string LastQqError { get; set; } = "";
	public string LastQqErrorCode { get; set; } = "";
}

public sealed class UserSettings
{
    public string CodexCustomPath { get; set; } = "";
    public string DetectedCodexPath { get; set; } = "";
	public string OpenClawGatewayUrl { get; set; } = "ws://127.0.0.1:18789";
	public bool OpenClawAutoDiscover { get; set; } = true;
	public bool OpenClawAutoReconnect { get; set; } = true;
    public string Language { get; set; } = "zh-CN";
    public string SandboxMode { get; set; } = "workspace-write";
    public List<long> TelegramAllowedUserIds { get; set; } = [];
    public int TelegramPollingTimeoutSeconds { get; set; } = 30;
    public bool TelegramSendProgressUpdates { get; set; } = true;
    public bool TelegramAutoStart { get; set; }
    public string TelegramProxyMode { get; set; } = "environment";
    public string TelegramProxyUrl { get; set; } = "";
	public string QqAppId { get; set; } = "";
	public string QqEnvironment { get; set; } = "production";
	public bool QqAutoStart { get; set; }
	public bool QqReconnectEnabled { get; set; } = true;
	public bool QqSendProgressUpdates { get; set; } = true;
	public List<string> QqAllowedUserOpenIds { get; set; } = [];
	public List<string> QqAllowedGroupOpenIds { get; set; } = [];
	public List<string> QqAllowedGroupMemberOpenIds { get; set; } = [];
	public string QqGroupTriggerMode { get; set; } = "official-at";
	public string QqCommandPrefix { get; set; } = "/codex";
	public string QqProxyMode { get; set; } = "environment";
	public string QqProxyUrl { get; set; } = "";
	// Telegram tokens remain in profile-scoped DPAPI stores. QQ AppSecrets are
	// deliberately persisted with their profile so they survive a reinstall.
	public List<ChannelProfileSettings> ChannelProfiles { get; set; } = [];
	public BackendChannelRoutingSettings ChannelRouting { get; set; } = new();
	// Prevent a user who deliberately removed every profile from being treated
	// as an un-migrated pre-Profile configuration on the next launch.
	public bool ChannelProfilesMigrated { get; set; }
	public bool StartWithWindows { get; set; }
	public bool SilentStartup { get; set; } = true;
	public bool MirrorAutoStart { get; set; }
	public bool CloseToTray { get; set; } = true;
	public bool RestoreLastPage { get; set; } = true;
	public bool AutoRefreshThreads { get; set; } = true;
	public int ThreadRefreshIntervalSeconds { get; set; } = 30;
	public string Theme { get; set; } = "system";
	public double WindowWidth { get; set; } = 1280;
	public double WindowHeight { get; set; } = 800;
	public double WindowLeft { get; set; } = double.NaN;
	public double WindowTop { get; set; } = double.NaN;
	public bool WindowMaximized { get; set; }
	public string LastPage { get; set; } = "overview";
}

public sealed class ChannelProfileSettings
{
	public string Id { get; set; } = "";
	public string Name { get; set; } = "";
	public string Platform { get; set; } = "telegram";
	public bool Enabled { get; set; } = true;
	public TelegramProfileSettings Telegram { get; set; } = new();
	public QqProfileSettings Qq { get; set; } = new();
}

public sealed class TelegramProfileSettings
{
	public List<long> AllowedUserIds { get; set; } = [];
	public int PollingTimeoutSeconds { get; set; } = 30;
	public bool SendProgressUpdates { get; set; } = true;
	public bool AutoStart { get; set; }
	public string ProxyMode { get; set; } = "environment";
	public string ProxyUrl { get; set; } = "";
}

public sealed class QqProfileSettings
{
	public string AppId { get; set; } = "";
	public string AppSecret { get; set; } = "";
	public bool AutoStart { get; set; }
	public bool ReconnectEnabled { get; set; } = true;
	public bool SendProgressUpdates { get; set; } = true;
	public List<string> AllowedUserOpenIds { get; set; } = [];
	public List<string> AllowedGroupOpenIds { get; set; } = [];
	public List<string> AllowedGroupMemberOpenIds { get; set; } = [];
	public string GroupTriggerMode { get; set; } = "official-at";
	public string CommandPrefix { get; set; } = "/codex";
	public string ProxyMode { get; set; } = "environment";
	public string ProxyUrl { get; set; } = "";
}

public sealed class BackendChannelRouteSettings
{
	private List<string> _telegramProfileIds = [];
	private List<string> _qqProfileIds = [];

	// System.Text.Json assigns explicit JSON null values through the setter.
	// Keep routing collections usable for older daemon responses as well as
	// partially populated settings files.
	public List<string> TelegramProfileIds
	{
		get => _telegramProfileIds;
		set => _telegramProfileIds = value ?? [];
	}

	public List<string> QqProfileIds
	{
		get => _qqProfileIds;
		set => _qqProfileIds = value ?? [];
	}
}

public sealed class ProjectListResponse
{
    public List<ProjectModel> Projects { get; set; } = [];
}

public sealed class ProjectModel
{
    public string ProjectId { get; set; } = "";
    public string Name { get; set; } = "";
    public List<string> Aliases { get; set; } = [];
    public string WorkingDirectory { get; set; } = "";
    public string Description { get; set; } = "";
    public string DefaultBackend { get; set; } = "codex";
    public int? DefaultConversationNumber { get; set; }
    public bool AutoCreateConversation { get; set; }
    public string ReuseStrategy { get; set; } = "default";
    public bool Enabled { get; set; } = true;
    public List<string> Tags { get; set; } = [];
    public string CreatedAt { get; set; } = "";
    public string UpdatedAt { get; set; } = "";
    public string AliasText => string.Join(", ", Aliases);
    public string BackendDisplay => DefaultBackend.Equals("openclaw", StringComparison.OrdinalIgnoreCase) ? "OpenClaw" : "Codex";
}

public sealed class ProjectInput
{
    public string Name { get; set; } = "";
    public List<string> Aliases { get; set; } = [];
    public string WorkingDirectory { get; set; } = "";
    public string Description { get; set; } = "";
    public string DefaultBackend { get; set; } = "codex";
    public int? DefaultConversationNumber { get; set; }
    public bool AutoCreateConversation { get; set; }
    public string ReuseStrategy { get; set; } = "default";
    public bool? Enabled { get; set; } = true;
    public List<string> Tags { get; set; } = [];
}

public sealed class TaskListResponse
{
    public List<BridgeTask> Tasks { get; set; } = [];
}

public sealed class BridgeTask
{
    public string TaskId { get; set; } = "";
    public int TaskNumber { get; set; }
    public string Title { get; set; } = "";
    public string Description { get; set; } = "";
    public string ProjectId { get; set; } = "";
    public string ProjectNameSnapshot { get; set; } = "";
    public string Backend { get; set; } = "";
    public int ConversationNumber { get; set; }
    public string TargetId { get; set; } = "";
    public string Status { get; set; } = "";
    public string CreatedAt { get; set; } = "";
    public string StartedAt { get; set; } = "";
    public string CompletedAt { get; set; } = "";
    public string LastActivityAt { get; set; } = "";
    public string CreatedFrom { get; set; } = "";
    public string ChannelProfileId { get; set; } = "";
    public string ConversationId { get; set; } = "";
    public string CurrentRunId { get; set; } = "";
    public int ParentTaskNumber { get; set; }
    public int RetryOfTaskNumber { get; set; }
    public string DispatchState { get; set; } = "";
    public string PendingInteractionId { get; set; } = "";
    public string PendingQuestion { get; set; } = "";
    public TaskSummaryModel Summary { get; set; } = new();
    public TaskResultModel Result { get; set; } = new();
    public string LastError { get; set; } = "";
    public Dictionary<string, string> Metadata { get; set; } = [];
    public string NumberLabel => TaskNumber > 0 ? $"T{TaskNumber}" : "T?";
    public string BackendDisplay => Backend.Equals("openclaw", StringComparison.OrdinalIgnoreCase) ? "OpenClaw" : "Codex";
    public string ConversationDisplay => ConversationNumber > 0 ? $"[{BackendDisplay}] #{ConversationNumber}" : string.IsNullOrWhiteSpace(TargetId) ? "未分配" : $"[{BackendDisplay}] {TargetId}";
    public string StatusDisplay => Status switch
    {
        "queued" => "排队中", "routing" => "路由中", "running" => "运行中", "waiting-input" => "等待输入",
        "completed" => "已完成", "failed" => "失败", "cancelled" => "已取消", "interrupted" => "已中断", _ => Status
    };
    public string CreatedAtDisplay => UiText.LocalDateTime(CreatedAt);
    public string StartedAtDisplay => UiText.LocalDateTime(StartedAt);
    public string CompletedAtDisplay => UiText.LocalDateTime(CompletedAt);
    public string LastActivityAtDisplay => UiText.LocalDateTime(LastActivityAt);
    public string DurationDisplay => !string.IsNullOrWhiteSpace(Result.Duration) ? Result.Duration : !string.IsNullOrWhiteSpace(Summary.Duration) ? Summary.Duration : Status is "running" or "waiting-input" ? "进行中" : "—";
    public string SummaryText => string.Join(Environment.NewLine, new[]
    {
        ChangedFilesText(), BuildResultsText(), TestResultsText(), string.IsNullOrWhiteSpace(Summary.GitSummary) ? "" : $"Git：{Summary.GitSummary}",
    }.Where(text => !string.IsNullOrWhiteSpace(text)));
    private string ChangedFilesText() => Summary.ChangedFiles.Count == 0 ? "" : $"修改：{string.Join(", ", Summary.ChangedFiles)}";
    private string BuildResultsText() => Summary.BuildResults.Count == 0 ? "" : $"构建：{string.Join("；", Summary.BuildResults)}";
    private string TestResultsText() => Summary.TestResults.Count == 0 ? "" : $"测试：{string.Join("；", Summary.TestResults)}";
}

public sealed class TaskSummaryModel
{
    public List<string> ChangedFiles { get; set; } = [];
    public List<string> BuildResults { get; set; } = [];
    public List<string> TestResults { get; set; } = [];
    public string GitSummary { get; set; } = "";
    public string Duration { get; set; } = "";
}

public sealed class TaskResultModel
{
    public bool Success { get; set; }
    public string FinalText { get; set; } = "";
    public List<string> ChangedFiles { get; set; } = [];
    public List<string> BuildResults { get; set; } = [];
    public List<string> TestResults { get; set; } = [];
    public string GitSummary { get; set; } = "";
    public string Duration { get; set; } = "";
}

public sealed class TaskInput
{
    public string Title { get; set; } = "";
    public string Description { get; set; } = "";
    public string ProjectId { get; set; } = "";
    public string Backend { get; set; } = "";
    public int ConversationNumber { get; set; }
    public string TargetId { get; set; } = "";
    public string CreatedFrom { get; set; } = "desktop";
    public int ParentTaskNumber { get; set; }
    public int RetryOfTaskNumber { get; set; }
}

public sealed class BackendChannelRoutingSettings
{
	private BackendChannelRouteSettings _codex = new();
	private BackendChannelRouteSettings _openClaw = new();

	public BackendChannelRouteSettings Codex
	{
		get => _codex;
		set => _codex = value ?? new();
	}

	public BackendChannelRouteSettings OpenClaw
	{
		get => _openClaw;
		set => _openClaw = value ?? new();
	}
}

public sealed class ChannelProfilesResponse
{
	private List<ChannelProfileStatus> _profiles = [];
	public List<ChannelProfileStatus> Profiles
	{
		get => _profiles;
		set => _profiles = value ?? [];
	}
	private BackendChannelRoutingSettings _routing = new();
	public BackendChannelRoutingSettings Routing
	{
		get => _routing;
		set => _routing = value ?? new();
	}
}

// These are display-safe daemon responses.  They never contain a Telegram
// token or QQ AppSecret.
public sealed class ChannelProfileStatus
{
	public string Id { get; set; } = "";
	public string Name { get; set; } = "";
	public string Platform { get; set; } = "";
	public bool Enabled { get; set; }
	public string ResourceId { get; set; } = "";
	public string SharedWithProfileId { get; set; } = "";
	public List<string> AssignedBackends { get; set; } = [];
	public bool Configured { get; set; }
	public bool Running { get; set; }
	public bool Connected { get; set; }
	public string State { get; set; } = "";
	public bool TokenSet { get; set; }
	public bool SecretConfigured { get; set; }
	public string AccountId { get; set; } = "";
	public string BotUsername { get; set; } = "";
	public string LastConnectedAt { get; set; } = "";
	public string LastUpdateAt { get; set; } = "";
	public int ReconnectCount { get; set; }
	public string LastError { get; set; } = "";
	public string ProxyMode { get; set; } = "";
	public string MaskedProxyAddress { get; set; } = "";
	public int BindingCount { get; set; }
}

public sealed class ChannelProfileConfigureRequest
{
	public string Name { get; set; } = "";
	public string Platform { get; set; } = "";
	public bool Enabled { get; set; } = true;
	public TelegramProfileConfigureRequest? Telegram { get; set; }
	public QqProfileConfigureRequest? Qq { get; set; }
}

public sealed class TelegramProfileConfigureRequest
{
	[JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
	public string? Token { get; set; }
	public List<long> AllowedUserIds { get; set; } = [];
	public int PollingTimeoutSeconds { get; set; } = 30;
	public bool SendProgressUpdates { get; set; } = true;
	public bool AutoStart { get; set; }
	public string ProxyMode { get; set; } = "environment";
	public string ProxyUrl { get; set; } = "";
}

public sealed class QqProfileConfigureRequest
{
	[JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
	public string? AppSecret { get; set; }
	public bool Enabled { get; set; } = true;
	public bool AutoStart { get; set; }
	public string AppId { get; set; } = "";
	public string Environment { get; set; } = "production";
	public List<string> AllowedUserOpenIds { get; set; } = [];
	public List<string> AllowedGroupOpenIds { get; set; } = [];
	public List<string> AllowedGroupMemberOpenIds { get; set; } = [];
	public string GroupTriggerMode { get; set; } = "official-at";
	public string CommandPrefix { get; set; } = "/codex";
	public bool SendProgressUpdates { get; set; } = true;
	public bool GatewayReconnectEnabled { get; set; } = true;
	public string ProxyMode { get; set; } = "environment";
	public string ProxyUrl { get; set; } = "";
}

public sealed class ChannelListResponse
{
    public List<JsonElement> Channels { get; set; } = [];
}

// The Telegram DTO intentionally contains only display-safe metadata. The bot
// token is accepted only by TelegramConfigureRequest and is never returned.
public sealed class TelegramChannelStatus
{
    public string ChannelType { get; set; } = "telegram";
    public string Type { get; set; } = "";
    public bool Configured { get; set; }
    public bool Running { get; set; }
    public bool? Connected { get; set; }
    public string State { get; set; } = "";
    public bool TokenSet { get; set; }
    public string TokenFingerprint { get; set; } = "";
    public JsonElement BotId { get; set; }
    public string BotUsername { get; set; } = "";
    public string BotDisplayName { get; set; } = "";
    public string TokenSummary { get; set; } = "";
    public string StartedAt { get; set; } = "";
    public string StoppedAt { get; set; } = "";
    public string LastUpdateAt { get; set; } = "";
    public string LastError { get; set; } = "";
    public string ProxyMode { get; set; } = "environment";
    public string MaskedProxyAddress { get; set; } = "";
    public string EffectiveProxyMode { get; set; } = "";
    public string LastNetworkStage { get; set; } = "";
    public long LastRequestDurationMs { get; set; }
    public string LastErrorCategory { get; set; } = "";
    public int PollingTimeoutSeconds { get; set; } = 30;
    public bool SendProgressUpdates { get; set; }
    public bool AutoStart { get; set; }
    public int AllowedUserCount { get; set; }
    public List<long> AllowedUserIds { get; set; } = [];
    public int BindingCount { get; set; }
    public string PollingState { get; set; } = "";
    public List<string> BoundAddressSummaries { get; set; } = [];
    public List<ChannelBinding>? Bindings { get; set; }
}

public sealed class TelegramConfigureRequest
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Token { get; set; }
    public List<long> AllowedUserIds { get; set; } = [];
    public int PollingTimeoutSeconds { get; set; } = 30;
    public bool SendProgressUpdates { get; set; }
    public bool AutoStart { get; set; }
    public string ProxyMode { get; set; } = "environment";
    public string ProxyUrl { get; set; } = "";
}

public sealed class TelegramProxyTestRequest
{
    public string ProxyMode { get; set; } = "environment";
    public string ProxyUrl { get; set; } = "";
}

public sealed class TelegramProxyTestResult
{
    public bool Ok { get; set; }
    public string Category { get; set; } = "";
    public string Message { get; set; } = "";
    public int StatusCode { get; set; }
    public long DurationMs { get; set; }
    public string EffectiveProxyMode { get; set; } = "";
    public string MaskedProxyAddress { get; set; } = "";
}

public sealed class TelegramTestResult
{
    public bool Ok { get; set; }
    public string Category { get; set; } = "";
    public JsonElement BotId { get; set; }
    public string Username { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Message { get; set; } = "";
}

// QQ Official Bot DTOs contain display-safe state only. AppSecret and access
// tokens are never returned by daemon APIs.
public sealed class QqChannelStatus
{
	public string ChannelType { get; set; } = "qqbot";
	public string Type { get; set; } = "qqbot";
	public bool Configured { get; set; }
	public bool Running { get; set; }
	public bool Connected { get; set; }
	public bool SecretConfigured { get; set; }
	public string AppId { get; set; } = "";
	public string Environment { get; set; } = "production";
	public string GatewayState { get; set; } = "not-configured";
	public string ConnectionState { get; set; } = "";
	public string SessionIdShort { get; set; } = "";
	public string LastHelloAt { get; set; } = "";
	public int HeartbeatIntervalMs { get; set; }
	public string LastConnectedAt { get; set; } = "";
	public string LastHeartbeatAt { get; set; } = "";
	public string LastHeartbeatAckAt { get; set; } = "";
	public string LastDispatchAt { get; set; } = "";
	public string LastDisconnectedAt { get; set; } = "";
	public int ReconnectCount { get; set; }
	public string AccessTokenExpiresAt { get; set; } = "";
	public string LastErrorCode { get; set; } = "";
	public string LastErrorMessage { get; set; } = "";
	public int AllowedUserCount { get; set; }
	public int AllowedGroupCount { get; set; }
	public int AllowedGroupMemberCount { get; set; }
	public int BindingCount { get; set; }
	public bool AutoStart { get; set; }
	public bool GatewayReconnectEnabled { get; set; }
	public bool SendProgressUpdates { get; set; }
	public string GroupTriggerMode { get; set; } = "official-at";
	public string CommandPrefix { get; set; } = "/codex";
	public string ProxyMode { get; set; } = "environment";
	public string EffectiveProxyMode { get; set; } = "environment";
	public string MaskedProxyAddress { get; set; } = "";
}

public sealed class QqConfigureRequest
{
	public bool Enabled { get; set; } = true;
	public bool AutoStart { get; set; }
	public string AppId { get; set; } = "";
	public string Environment { get; set; } = "production";
	public bool GatewayReconnectEnabled { get; set; } = true;
	public List<string> AllowedUserOpenIds { get; set; } = [];
	public List<string> AllowedGroupOpenIds { get; set; } = [];
	public List<string> AllowedGroupMemberOpenIds { get; set; } = [];
	public string GroupTriggerMode { get; set; } = "official-at";
	public string CommandPrefix { get; set; } = "/codex";
	public bool SendProgressUpdates { get; set; } = true;
	public string ProxyMode { get; set; } = "environment";
	public string ProxyUrl { get; set; } = "";
}

public sealed class QqTestResult
{
	public bool Success { get; set; }
	public string Code { get; set; } = "";
	public string Message { get; set; } = "";
	public string AppId { get; set; } = "";
	public string Environment { get; set; } = "production";
	public bool GatewayAvailable { get; set; }
	public long TokenExpiresIn { get; set; }
	public string GatewayHost { get; set; } = "";
}

public sealed class QqSecretRequest { public string AppSecret { get; set; } = ""; }
public sealed class QqSecretStatus { public bool SecretConfigured { get; set; } }

public sealed class QqNetworkTestResult
{
	public bool Success { get; set; }
	public string Code { get; set; } = "";
	public string Message { get; set; } = "";
	public long DurationMs { get; set; }
	public string EffectiveProxyMode { get; set; } = "";
	public string MaskedProxyAddress { get; set; } = "";
}

public sealed class QqDiscoveredIdentityList { public List<QqDiscoveredIdentity>? Identities { get; set; } = []; }

public sealed class QqDiscoveredIdentity
{
	public string Type { get; set; } = "";
	public string DisplayName { get; set; } = "";
	public string UserOpenId { get; set; } = "";
	public string GroupOpenId { get; set; } = "";
	public string GroupMemberOpenId { get; set; } = "";
	public string DiscoveredAt { get; set; } = "";
    public string TypeDisplay => UiText.ConversationType(Type);
    public string DiscoveredAtDisplay => UiText.LocalDateTime(DiscoveredAt);
}

public sealed class BindingListResponse
{
    public List<ChannelBinding>? Bindings { get; set; } = [];
}

public sealed class CreateBindingRequest
{
	public string Backend { get; set; } = "codex";
	public string TargetId { get; set; } = "";
	public string ChannelType { get; set; } = "";
	public string ChannelProfileId { get; set; } = "";
	public string ConversationId { get; set; } = "";
	public string AccountId { get; set; } = "";
	public string ConversationType { get; set; } = "default";
	public string ChatId { get; set; } = "";
	public string TopicId { get; set; } = "";
	public string ThreadId { get; set; } = "";
	public string SessionKey { get; set; } = "";
	public bool? Enabled { get; set; }
}

public sealed class ChannelBinding
{
    public string Id { get; set; } = "";
	public string Backend { get; set; } = "codex";
	public string TargetId { get; set; } = "";
    public string ChannelType { get; set; } = "";
	public string ChannelProfileId { get; set; } = "";
	public string ResourceId { get; set; } = "";
    public string ConversationType { get; set; } = "";
	public string ConversationId { get; set; } = "";
    public string AccountId { get; set; } = "";
    public string ChatId { get; set; } = "";
    public string TopicId { get; set; } = "";
    public string ThreadId { get; set; } = "";
	public string SessionKey { get; set; } = "";
    public string ThreadTitle { get; set; } = "";
	public bool Enabled { get; set; }
	public bool Legacy { get; set; }
    public string CreatedAt { get; set; } = "";
    public string UpdatedAt { get; set; } = "";

    public string ShortId => Abbreviate(Id);
    public string LegacyLabel => Legacy || string.Equals(ChannelType, "qq", StringComparison.OrdinalIgnoreCase)
        ? "已失效的旧版绑定"
        : string.Empty;
    public string ShortThreadId => Abbreviate(ThreadId);
	public bool IsOpenClaw => string.Equals(Backend, "openclaw", StringComparison.OrdinalIgnoreCase);
	public string BackendLabel => IsOpenClaw ? "[OpenClaw]" : "[Codex]";
	public string BoundTargetId => !string.IsNullOrWhiteSpace(TargetId) ? TargetId : IsOpenClaw && !string.IsNullOrWhiteSpace(SessionKey) ? SessionKey : ThreadId;
    public string SafeChatSummary => SafeSummary(ChatId);
    public string DisplayThreadTitle => string.IsNullOrWhiteSpace(ThreadTitle) ? "未提供标题" : ThreadTitle;
    public string CreatedAtDisplay => UiText.LocalDateTime(CreatedAt);
    public string ConversationTypeDisplay => UiText.ConversationType(ConversationType);
    public string ChannelTypeDisplay => string.Equals(ChannelType, "telegram", StringComparison.OrdinalIgnoreCase) ? "Telegram" : "QQ";

    private static string Abbreviate(string value) =>
        string.IsNullOrWhiteSpace(value) || value.Length <= 12 ? value : $"{value[..8]}…{value[^4..]}";

    private static string SafeSummary(string value) =>
        string.IsNullOrWhiteSpace(value) ? "—" : value.Length <= 8 ? $"••••{value[Math.Max(0, value.Length - 4)..]}" : $"{value[..4]}…{value[^4..]}";
}

public sealed class ReadyMessage
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";
    [JsonPropertyName("address")]
    public string Address { get; set; } = "";
    [JsonPropertyName("token")]
    public string Token { get; set; } = "";
    [JsonPropertyName("pid")]
    public int Pid { get; set; }
}

public sealed record LogEntry(DateTimeOffset Timestamp, string Source, string Message)
{
    public string Display => $"{Timestamp:HH:mm:ss}  [{Source}]  {Message}";
}

public sealed class BridgeApiException(HttpStatusCode statusCode, string code, string message, string currentState = "", string lastError = "") : Exception(message)
{
    public HttpStatusCode StatusCode { get; } = statusCode;
    public string Code { get; } = code;
    public string CurrentState { get; } = currentState;
	public string LastError { get; } = lastError;
}
