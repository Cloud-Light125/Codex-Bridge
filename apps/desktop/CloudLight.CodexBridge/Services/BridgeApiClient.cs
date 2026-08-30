using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.Services;

public sealed class BridgeApiClient(LogService logs) : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private HttpClient? _client;
    private CancellationTokenSource? _eventsCancellation;
    public event EventHandler<BridgeEvent>? EventReceived;
    public event EventHandler<bool>? EventStreamConnectionChanged;

    public void Connect(Uri address, string token)
    {
        _client?.Dispose();
        _client = new HttpClient { BaseAddress = address, Timeout = TimeSpan.FromSeconds(40) };
        _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
    }

    public Task<BridgeStatus> GetStatusAsync(CancellationToken cancellationToken = default) =>
        GetAsync<BridgeStatus>("/api/v1/status", cancellationToken);

    public Task<OpenClawConnectionStatus> GetOpenClawStatusAsync(CancellationToken cancellationToken = default) =>
        GetAsync<OpenClawConnectionStatus>("/api/v1/openclaw/status", cancellationToken);

    public Task<OpenClawConnectionStatus> ConfigureOpenClawAsync(OpenClawConfigureRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<OpenClawConnectionStatus>(HttpMethod.Put, "/api/v1/settings/openclaw", input, cancellationToken);

    public Task<OpenClawConnectionStatus> TestOpenClawAsync(OpenClawConfigureRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<OpenClawConnectionStatus>(HttpMethod.Post, "/api/v1/openclaw/test", input, cancellationToken);

    public async Task<OpenClawSessionListResponse> GetOpenClawSessionsAsync(int limit = 100, CancellationToken cancellationToken = default)
    {
        var result = await GetAsync<OpenClawSessionListResponse>($"/api/v1/openclaw/sessions?limit={limit}", cancellationToken);
        result.Sessions ??= [];
        return result;
    }

    public Task<OpenClawSessionDetail> GetOpenClawSessionAsync(string sessionKey, CancellationToken cancellationToken = default) =>
        GetAsync<OpenClawSessionDetail>($"/api/v1/openclaw/sessions/{Uri.EscapeDataString(sessionKey)}", cancellationToken);

    public Task<OpenClawSendResult> SendOpenClawMessageAsync(string sessionKey, string message, CancellationToken cancellationToken = default) =>
        SendJsonAsync<OpenClawSendResult>(HttpMethod.Post, $"/api/v1/openclaw/sessions/{Uri.EscapeDataString(sessionKey)}/messages", new { message }, cancellationToken);

    public Task<OpenClawAbortResult> AbortOpenClawAsync(string sessionKey, string runId = "", CancellationToken cancellationToken = default) =>
        SendJsonAsync<OpenClawAbortResult>(HttpMethod.Post, $"/api/v1/openclaw/sessions/{Uri.EscapeDataString(sessionKey)}/abort", new { runId }, cancellationToken);

    public Task<ThreadListResponse> GetThreadsAsync(int limit = 100, string cursor = "", CancellationToken cancellationToken = default)
    {
        var uri = $"/api/v1/threads?limit={limit}";
        if (!string.IsNullOrWhiteSpace(cursor)) uri += $"&cursor={Uri.EscapeDataString(cursor)}";
        return GetAsync<ThreadListResponse>(uri, cancellationToken);
    }

    public Task<ThreadDetail> GetThreadAsync(string threadId, CancellationToken cancellationToken = default) =>
        GetAsync<ThreadDetail>($"/api/v1/threads/{Uri.EscapeDataString(threadId)}?includeTurns=true", cancellationToken);

    public Task<TurnAccepted> StartTurnAsync(string threadId, StartTurnRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<TurnAccepted>(HttpMethod.Post, $"/api/v1/threads/{Uri.EscapeDataString(threadId)}/turns", input, cancellationToken);

    public Task<PersistenceVerification> VerifyThreadPersistenceAsync(string threadId, CancellationToken cancellationToken = default) =>
        SendJsonAsync<PersistenceVerification>(HttpMethod.Post, $"/api/v1/threads/{Uri.EscapeDataString(threadId)}/persistence/verify", null, cancellationToken);

    public Task<InterruptResult> InterruptTurnAsync(string threadId, string turnId, CancellationToken cancellationToken = default) =>
        SendJsonAsync<InterruptResult>(HttpMethod.Post, $"/api/v1/threads/{Uri.EscapeDataString(threadId)}/turns/{Uri.EscapeDataString(turnId)}/interrupt", null, cancellationToken);

    public Task<InteractionListResponse> GetInteractionsAsync(string status = "pending", CancellationToken cancellationToken = default) =>
        GetAsync<InteractionListResponse>($"/api/v1/interactions?status={Uri.EscapeDataString(status)}", cancellationToken);

    public Task<PendingInteraction> RespondInteractionAsync(string interactionId, InteractionResponse input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<PendingInteraction>(HttpMethod.Post, $"/api/v1/interactions/{Uri.EscapeDataString(interactionId)}/respond", input, cancellationToken);

    public Task<BridgeStatus> UpdateSecurityAsync(string sandboxMode, CancellationToken cancellationToken = default) =>
        SendJsonAsync<BridgeStatus>(HttpMethod.Put, "/api/v1/settings/security", new SecuritySettingsRequest { SandboxMode = sandboxMode }, cancellationToken);

    public Task<BridgeStatus> ApplyCodexPathAsync(string path, string source, CancellationToken cancellationToken = default) =>
        SendJsonAsync<BridgeStatus>(HttpMethod.Put, "/api/v1/settings/codex", new CodexSettingsRequest { Path = path, Source = source }, cancellationToken);

    public Task<RemoteCommandListResponse> GetCommandsAsync(CancellationToken cancellationToken = default) =>
        GetAsync<RemoteCommandListResponse>("/api/v1/commands", cancellationToken);

    public Task<RemoteCommandListResponse> GetCommandsForBackendAsync(string backend, CancellationToken cancellationToken = default) =>
        GetAsync<RemoteCommandListResponse>($"/api/v1/commands?backend={Uri.EscapeDataString(backend)}", cancellationToken);

    public Task<RemoteCommandDefinition> CreateCommandAsync(RemoteCommandMutation input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<RemoteCommandDefinition>(HttpMethod.Post, "/api/v1/commands", input, cancellationToken);

    public Task<RemoteCommandDefinition> UpdateCommandAsync(string id, RemoteCommandMutation input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<RemoteCommandDefinition>(HttpMethod.Put, $"/api/v1/commands/{Uri.EscapeDataString(id)}", input, cancellationToken);

    public Task<RemoteCommandDefinition> SetCommandLockedAsync(string id, bool locked, CancellationToken cancellationToken = default) =>
        SendJsonAsync<RemoteCommandDefinition>(HttpMethod.Post, $"/api/v1/commands/{Uri.EscapeDataString(id)}/{(locked ? "lock" : "unlock")}", null, cancellationToken);

    public Task<RemoteCommandDefinition> RestoreCommandAsync(string id, CancellationToken cancellationToken = default) =>
        SendJsonAsync<RemoteCommandDefinition>(HttpMethod.Post, $"/api/v1/commands/{Uri.EscapeDataString(id)}/restore", null, cancellationToken);

    public Task DeleteCommandAsync(string id, CancellationToken cancellationToken = default) =>
        SendNoContentAsync(HttpMethod.Delete, $"/api/v1/commands/{Uri.EscapeDataString(id)}", cancellationToken);

	public Task<MirrorStatus> GetMirrorAsync(CancellationToken cancellationToken = default) => GetAsync<MirrorStatus>("/api/v1/mirror", cancellationToken);
	public Task<MirrorStatus> ConfigureMirrorAsync(MirrorConfig input, CancellationToken cancellationToken = default) => SendJsonAsync<MirrorStatus>(HttpMethod.Put, "/api/v1/mirror", input, cancellationToken);

    public Task<ChannelListResponse> GetChannelsAsync(CancellationToken cancellationToken = default) =>
        GetAsync<ChannelListResponse>("/api/v1/channels", cancellationToken);

    public Task<ChannelProfilesResponse> GetChannelProfilesAsync(CancellationToken cancellationToken = default) =>
        GetAsync<ChannelProfilesResponse>("/api/v1/channel-profiles", cancellationToken);

    public Task<ChannelProfileStatus> ConfigureChannelProfileAsync(string profileId, ChannelProfileConfigureRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<ChannelProfileStatus>(HttpMethod.Put, $"/api/v1/channel-profiles/{Uri.EscapeDataString(profileId)}", input, cancellationToken);

    public Task<ChannelProfileStatus> StartChannelProfileAsync(string profileId, CancellationToken cancellationToken = default) =>
        SendJsonAsync<ChannelProfileStatus>(HttpMethod.Post, $"/api/v1/channel-profiles/{Uri.EscapeDataString(profileId)}/start", null, cancellationToken);

    public Task<ChannelProfileStatus> StopChannelProfileAsync(string profileId, CancellationToken cancellationToken = default) =>
        SendJsonAsync<ChannelProfileStatus>(HttpMethod.Post, $"/api/v1/channel-profiles/{Uri.EscapeDataString(profileId)}/stop", null, cancellationToken);

    public Task DeleteChannelProfileAsync(string profileId, CancellationToken cancellationToken = default) =>
        SendNoContentAsync(HttpMethod.Delete, $"/api/v1/channel-profiles/{Uri.EscapeDataString(profileId)}", cancellationToken);

    public Task<BackendChannelRoutingSettings> GetChannelRoutingAsync(CancellationToken cancellationToken = default) =>
        GetAsync<BackendChannelRoutingSettings>("/api/v1/channel-routing", cancellationToken);

    public Task<BackendChannelRoutingSettings> ConfigureChannelRoutingAsync(BackendChannelRoutingSettings input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<BackendChannelRoutingSettings>(HttpMethod.Put, "/api/v1/channel-routing", input, cancellationToken);

    public Task<TelegramChannelStatus> GetTelegramStatusAsync(CancellationToken cancellationToken = default) =>
        GetAsync<TelegramChannelStatus>("/api/v1/channels/telegram/status", cancellationToken);

    public Task<TelegramChannelStatus> ConfigureTelegramAsync(TelegramConfigureRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<TelegramChannelStatus>(HttpMethod.Post, "/api/v1/channels/telegram/configure", input, cancellationToken);

    public Task<TelegramTestResult> TestTelegramAsync(CancellationToken cancellationToken = default) =>
        SendJsonAsync<TelegramTestResult>(HttpMethod.Post, "/api/v1/channels/telegram/test", new { }, cancellationToken);

    public Task<TelegramProxyTestResult> TestTelegramProxyAsync(TelegramProxyTestRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<TelegramProxyTestResult>(HttpMethod.Post, "/api/v1/channels/telegram/test-proxy", input, cancellationToken);

    public Task<TelegramChannelStatus> StartTelegramAsync(CancellationToken cancellationToken = default) =>
        SendJsonAsync<TelegramChannelStatus>(HttpMethod.Post, "/api/v1/channels/telegram/start", null, cancellationToken);

    public Task<TelegramChannelStatus> StopTelegramAsync(CancellationToken cancellationToken = default) =>
        SendJsonAsync<TelegramChannelStatus>(HttpMethod.Post, "/api/v1/channels/telegram/stop", null, cancellationToken);

    public Task DeleteTelegramTokenAsync(CancellationToken cancellationToken = default) =>
        SendNoContentAsync(HttpMethod.Delete, "/api/v1/channels/telegram/token", cancellationToken);

	public Task<QqChannelStatus> GetQqStatusAsync(CancellationToken cancellationToken = default) =>
		GetAsync<QqChannelStatus>("/api/v1/channels/qqbot/status", cancellationToken);

	public Task<QqChannelStatus> ConfigureQqAsync(QqConfigureRequest input, CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqChannelStatus>(HttpMethod.Post, "/api/v1/channels/qqbot/configure", input, cancellationToken);

	public Task<QqSecretStatus> SetQqSecretAsync(string appSecret, CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqSecretStatus>(HttpMethod.Post, "/api/v1/channels/qqbot/secret", new QqSecretRequest { AppSecret = appSecret }, cancellationToken);

	public Task<QqTestResult> TestQqAsync(CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqTestResult>(HttpMethod.Post, "/api/v1/channels/qqbot/test", new { }, cancellationToken);

	public Task<QqNetworkTestResult> TestQqNetworkAsync(CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqNetworkTestResult>(HttpMethod.Post, "/api/v1/channels/qqbot/network-test", new { }, cancellationToken);

	public Task<QqChannelStatus> StartQqAsync(CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqChannelStatus>(HttpMethod.Post, "/api/v1/channels/qqbot/start", null, cancellationToken);

	public Task<QqChannelStatus> StopQqAsync(CancellationToken cancellationToken = default) =>
		SendJsonAsync<QqChannelStatus>(HttpMethod.Post, "/api/v1/channels/qqbot/stop", null, cancellationToken);

	public Task DeleteQqSecretAsync(CancellationToken cancellationToken = default) =>
		SendNoContentAsync(HttpMethod.Delete, "/api/v1/channels/qqbot/secret", cancellationToken);

	public async Task<QqDiscoveredIdentityList> GetQqDiscoveredIdentitiesAsync(CancellationToken cancellationToken = default)
	{
		var result = await GetAsync<QqDiscoveredIdentityList>("/api/v1/channels/qqbot/discovered-identities", cancellationToken);
		result.Identities ??= [];
		return result;
	}

	public async Task<BindingListResponse> GetBindingsAsync(CancellationToken cancellationToken = default)
	{
		var result = await GetAsync<BindingListResponse>("/api/v1/bindings", cancellationToken);
		result.Bindings ??= [];
		return result;
	}

    public Task<ChannelBinding> CreateBindingAsync(CreateBindingRequest input, CancellationToken cancellationToken = default) =>
        SendJsonAsync<ChannelBinding>(HttpMethod.Post, "/api/v1/bindings", input, cancellationToken);

    public Task DeleteBindingAsync(string bindingId, CancellationToken cancellationToken = default) =>
        SendNoContentAsync(HttpMethod.Delete, $"/api/v1/bindings/{Uri.EscapeDataString(bindingId)}", cancellationToken);

    private async Task<T> GetAsync<T>(string uri, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("尚未连接本地后端。");
        using var response = await client.GetAsync(uri, cancellationToken);
        return await ReadResponseAsync<T>(response, cancellationToken);
    }

    private async Task<T> SendJsonAsync<T>(HttpMethod method, string uri, object? value, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("尚未连接本地后端。");
        using var request = new HttpRequestMessage(method, uri);
        if (value is not null)
        {
            request.Content = JsonContent.Create(value, options: JsonOptions);
        }
        using var response = await client.SendAsync(request, cancellationToken);
        return await ReadResponseAsync<T>(response, cancellationToken);
    }

    private async Task SendNoContentAsync(HttpMethod method, string uri, CancellationToken cancellationToken)
    {
        var client = _client ?? throw new InvalidOperationException("尚未连接本地后端。");
        using var request = new HttpRequestMessage(method, uri);
        using var response = await client.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            _ = await ReadResponseAsync<JsonElement>(response, cancellationToken);
        }
    }

    private static async Task<T> ReadResponseAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            var code = "api_error";
            var message = $"本地 API 返回 {(int)response.StatusCode}";
            var currentState = "";
			var lastError = "";
            try
            {
                using var document = JsonDocument.Parse(body);
                var root = document.RootElement;
                if (root.TryGetProperty("code", out var codeValue)) code = codeValue.GetString() ?? code;
                if (root.TryGetProperty("message", out var messageValue)) message = messageValue.GetString() ?? message;
                if (root.TryGetProperty("currentState", out var stateValue)) currentState = stateValue.GetString() ?? "";
				if (root.TryGetProperty("lastError", out var lastErrorValue)) lastError = lastErrorValue.GetString() ?? "";
            }
            catch (JsonException)
            {
                message = LogService.Redact(body);
            }
			throw new BridgeApiException(response.StatusCode, code, LogService.Redact(message), currentState, LogService.Redact(lastError));
        }
        return await response.Content.ReadFromJsonAsync<T>(JsonOptions, cancellationToken)
            ?? throw new JsonException("本地 API 返回了空 JSON。");
    }

    public void StartEventStream()
    {
        _eventsCancellation?.Cancel();
        _eventsCancellation?.Dispose();
        _eventsCancellation = new CancellationTokenSource();
        _ = ReadEventsLoopAsync(_eventsCancellation.Token);
    }

    private async Task ReadEventsLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ReadOneEventStreamAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logs.Add("desktop", $"SSE 事件流已断开：{LogService.Redact(exception.Message)}");
            }
            EventStreamConnectionChanged?.Invoke(this, false);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ReadOneEventStreamAsync(CancellationToken cancellationToken)
    {
        var client = _client;
        if (client is null) return;
        using var request = new HttpRequestMessage(HttpMethod.Get, "/api/v1/events");
        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await ReadResponseHeadersAsync(response, cancellationToken);
        EventStreamConnectionChanged?.Invoke(this, true);
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream);
        while (!cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null) break;
            if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;
            var json = line[6..];
            var bridgeEvent = JsonSerializer.Deserialize<BridgeEvent>(json, JsonOptions);
            if (bridgeEvent is null || string.IsNullOrWhiteSpace(bridgeEvent.EventType)) continue;
            logs.Add("event", bridgeEvent.EventType);
            EventReceived?.Invoke(this, bridgeEvent);
        }
    }

    private static async Task ReadResponseHeadersAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) return;
        _ = await ReadResponseAsync<JsonElement>(response, cancellationToken);
    }

    public void Dispose()
    {
        _eventsCancellation?.Cancel();
        _eventsCancellation?.Dispose();
        _client?.Dispose();
    }
}
