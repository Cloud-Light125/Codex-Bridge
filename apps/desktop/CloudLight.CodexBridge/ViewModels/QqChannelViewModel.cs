using System.Collections.ObjectModel;
using System.Net;
using System.Text.Json;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class QqChannelViewModel : ObservableObject
{
    private readonly BridgeApiClient _api;
    private readonly SettingsService _settingsService;
    private readonly QqSecretService _secretService;
    private readonly UserSettings _settings;
    private readonly LogService _logs;
    private readonly Func<CancellationToken, Task> _refreshBindings;
    private readonly Action _scheduleRefresh;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _initializeLock = new();
    private Task? _initialization;

    private string _appId;
    private string _allowedUserOpenIdsText;
    private string _allowedGroupOpenIdsText;
    private string _allowedGroupMemberOpenIdsText;
    private bool _autoStart;
    private bool _gatewayReconnectEnabled;
    private bool _sendProgressUpdates;
    private string _proxyMode;
    private string _proxyUrl;
    private bool _configured;
    private bool _running;
    private bool _connected;
    private bool _secretConfigured;
    private string _gatewayState = "not-configured";
    private string _sessionIdShort = "—";
    private string _lastConnectedAt = "—";
    private string _lastHeartbeatAt = "—";
    private string _lastDispatchAt = "—";
    private string _accessTokenExpiresAt = "—";
    private string _lastError = "";
    private int _reconnectCount;
    private int _allowedUserCount;
    private int _allowedGroupCount;
    private int _allowedGroupMemberCount;
    private int _bindingCount;
    private string _effectiveProxy = "—";
    private string _operationMessage = "";
    private bool _isLoading;
    private string _loadError = "";
    private string _loadErrorType = "";
	private string _discoveredLoadError = "";
	private string _bindingLoadError = "";

    public QqChannelViewModel(
        BridgeApiClient api,
        SettingsService settingsService,
        QqSecretService secretService,
        UserSettings settings,
        LogService logs,
        Func<CancellationToken, Task> refreshBindings,
        Action scheduleRefresh)
    {
        _api = api;
        _settingsService = settingsService;
        _secretService = secretService;
        _settings = settings;
        _logs = logs;
        _refreshBindings = refreshBindings;
        _scheduleRefresh = scheduleRefresh;
        _appId = settings.QqAppId;
        _allowedUserOpenIdsText = JoinLines(settings.QqAllowedUserOpenIds);
        _allowedGroupOpenIdsText = JoinLines(settings.QqAllowedGroupOpenIds);
        _allowedGroupMemberOpenIdsText = JoinLines(settings.QqAllowedGroupMemberOpenIds);
        _autoStart = settings.QqAutoStart;
        _gatewayReconnectEnabled = settings.QqReconnectEnabled;
        _sendProgressUpdates = settings.QqSendProgressUpdates;
        _proxyMode = NormalizeProxyMode(settings.QqProxyMode);
        _proxyUrl = _proxyMode == "custom-http" ? settings.QqProxyUrl : "";

        SaveSettingsCommand = new AsyncRelayCommand(SaveSettingsAsync);
        TestCommand = new AsyncRelayCommand(TestAsync);
        TestNetworkCommand = new AsyncRelayCommand(TestNetworkAsync);
        StartCommand = new AsyncRelayCommand(StartAsync);
        StopCommand = new AsyncRelayCommand(StopAdapterAsync);
        RefreshCommand = new AsyncRelayCommand(() => RefreshStatusAsync());
        RetryCommand = new AsyncRelayCommand(() => EnsureInitializedAsync(forceRetry: true));
    }

    public ObservableCollection<ChannelBinding> Bindings { get; } = [];
    public ObservableCollection<QqDiscoveredIdentity> DiscoveredIdentities { get; } = [];
    public ICommand SaveSettingsCommand { get; }
    public ICommand TestCommand { get; }
    public ICommand TestNetworkCommand { get; }
    public ICommand StartCommand { get; }
    public ICommand StopCommand { get; }
    public ICommand RefreshCommand { get; }
    public ICommand RetryCommand { get; }

    public string AppId { get => _appId; set => SetProperty(ref _appId, value); }
    public string Environment => "production";
    public string EnvironmentText => "正式环境";
    public string AllowedUserOpenIdsText { get => _allowedUserOpenIdsText; set => SetProperty(ref _allowedUserOpenIdsText, value); }
    public string AllowedGroupOpenIdsText { get => _allowedGroupOpenIdsText; set => SetProperty(ref _allowedGroupOpenIdsText, value); }
    public string AllowedGroupMemberOpenIdsText { get => _allowedGroupMemberOpenIdsText; set => SetProperty(ref _allowedGroupMemberOpenIdsText, value); }
    public bool AutoStart { get => _autoStart; set => SetProperty(ref _autoStart, value); }
    public bool GatewayReconnectEnabled { get => _gatewayReconnectEnabled; set => SetProperty(ref _gatewayReconnectEnabled, value); }
    public bool SendProgressUpdates { get => _sendProgressUpdates; set => SetProperty(ref _sendProgressUpdates, value); }
    public string ProxyMode
    {
        get => _proxyMode;
        set
        {
            if (!SetProperty(ref _proxyMode, NormalizeProxyMode(value))) return;
            OnPropertyChanged(nameof(CustomProxyVisibility));
        }
    }
    public string ProxyUrl { get => _proxyUrl; set => SetProperty(ref _proxyUrl, value); }
    public Visibility CustomProxyVisibility => ProxyMode == "custom-http" ? Visibility.Visible : Visibility.Collapsed;

    public bool Configured { get => _configured; private set { if (SetProperty(ref _configured, value)) NotifyStatus(); } }
    public bool Running { get => _running; private set { if (SetProperty(ref _running, value)) NotifyStatus(); } }
    public bool Connected { get => _connected; private set { if (SetProperty(ref _connected, value)) NotifyStatus(); } }
    public bool SecretConfigured { get => _secretConfigured; private set { if (SetProperty(ref _secretConfigured, value)) OnPropertyChanged(nameof(SecretStatusText)); } }
    public string StatusText => !Configured ? "未配置" : Running ? (Connected ? "运行中 · 已连接" : "运行中 · 正在连接") : "已配置 · 已停止";
    public string SecretStatusText => SecretConfigured ? "应用密钥已安全保存到本机" : "未保存";
    public string GatewayState { get => _gatewayState; private set => SetProperty(ref _gatewayState, value); }
    public string SessionIdShort { get => _sessionIdShort; private set => SetProperty(ref _sessionIdShort, value); }
    public string LastConnectedAt { get => _lastConnectedAt; private set => SetProperty(ref _lastConnectedAt, value); }
    public string LastHeartbeatAt { get => _lastHeartbeatAt; private set => SetProperty(ref _lastHeartbeatAt, value); }
    public string LastDispatchAt { get => _lastDispatchAt; private set => SetProperty(ref _lastDispatchAt, value); }
    public string AccessTokenExpiresAt { get => _accessTokenExpiresAt; private set => SetProperty(ref _accessTokenExpiresAt, value); }
    public string LastError { get => _lastError; private set => SetProperty(ref _lastError, value); }
    public int ReconnectCount { get => _reconnectCount; private set => SetProperty(ref _reconnectCount, value); }
    public int AllowedUserCount { get => _allowedUserCount; private set => SetProperty(ref _allowedUserCount, value); }
    public int AllowedGroupCount { get => _allowedGroupCount; private set => SetProperty(ref _allowedGroupCount, value); }
    public int AllowedGroupMemberCount { get => _allowedGroupMemberCount; private set => SetProperty(ref _allowedGroupMemberCount, value); }
    public int BindingCount { get => _bindingCount; private set => SetProperty(ref _bindingCount, value); }
    public string EffectiveProxy { get => _effectiveProxy; private set => SetProperty(ref _effectiveProxy, value); }
    public string OperationMessage { get => _operationMessage; private set => SetProperty(ref _operationMessage, value); }
    public bool IsLoading { get => _isLoading; private set { if (SetProperty(ref _isLoading, value)) NotifyLoadVisibility(); } }
    public string LoadError { get => _loadError; private set { if (SetProperty(ref _loadError, value)) NotifyLoadVisibility(); } }
    public string LoadErrorType { get => _loadErrorType; private set { if (SetProperty(ref _loadErrorType, value)) OnPropertyChanged(nameof(SecretRecoveryVisibility)); } }
    public Visibility LoadingVisibility => IsLoading ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ErrorVisibility => !IsLoading && !string.IsNullOrWhiteSpace(LoadError) ? Visibility.Visible : Visibility.Collapsed;
    public Visibility SecretRecoveryVisibility => LoadErrorType == "DPAPI" ? Visibility.Visible : Visibility.Collapsed;
	public string AuxiliaryLoadMessage => string.Join(System.Environment.NewLine, new[] { _discoveredLoadError, _bindingLoadError }.Where(value => !string.IsNullOrWhiteSpace(value)));
	public Visibility AuxiliaryErrorVisibility => string.IsNullOrWhiteSpace(AuxiliaryLoadMessage) ? Visibility.Collapsed : Visibility.Visible;

    public Task EnsureInitializedAsync(CancellationToken cancellationToken = default, bool forceRetry = false)
    {
        lock (_initializeLock)
        {
            if (forceRetry) _initialization = null;
            return _initialization ??= InitializeCoreHandledAsync(cancellationToken);
        }
    }

    private async Task InitializeCoreHandledAsync(CancellationToken cancellationToken)
    {
		await RunOnUiAsync(() => { IsLoading = true; LoadError = ""; LoadErrorType = ""; });
		using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
		linked.CancelAfter(TimeSpan.FromSeconds(25));
		QqChannelStatus status;
        try
        {
            var secret = await _secretService.LoadAsync(linked.Token).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(secret))
                await _api.SetQqSecretAsync(secret, linked.Token).ConfigureAwait(false);
			status = await _api.ConfigureQqAsync(BuildRequest(), linked.Token).ConfigureAwait(false);
            if (_settings.QqAutoStart && status.Configured && !status.Running)
                status = await _api.StartQqAsync(linked.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
			_logs.AddException("desktop", "加载 QQ 官方机器人主状态失败。", exception);
            var (message, type) = DescribeLoadError(exception);
            await RunOnUiAsync(() => { IsLoading = false; LoadError = message; LoadErrorType = type; });
			return;
        }

		await RunOnUiAsync(() =>
		{
			ApplyStatus(status);
			LoadError = "";
			LoadErrorType = "";
			IsLoading = false;
		});
		await RefreshDiscoveredIdentitiesHandledAsync(linked.Token).ConfigureAwait(false);
		await _refreshBindings(linked.Token).ConfigureAwait(false);
    }

    public async Task<bool> SaveSecretAsync(string secret, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(secret))
        {
            OperationMessage = "AppSecret 不能为空。";
            return false;
        }
        try
        {
            await _secretService.SaveAsync(secret.Trim(), cancellationToken).ConfigureAwait(false);
            var result = await _api.SetQqSecretAsync(secret.Trim(), cancellationToken).ConfigureAwait(false);
            await RunOnUiAsync(() =>
            {
                SecretConfigured = result.SecretConfigured;
                OperationMessage = "应用密钥已安全保存到本机。";
            });
            return true;
        }
        catch (Exception exception) { await ReportErrorAsync("保存 AppSecret", exception); return false; }
    }

    public async Task DeleteSecretAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _api.DeleteQqSecretAsync(cancellationToken).ConfigureAwait(false);
            await _secretService.DeleteAsync(cancellationToken).ConfigureAwait(false);
            await RunOnUiAsync(() => { SecretConfigured = false; Configured = false; Running = false; Connected = false; OperationMessage = "QQ 应用密钥已删除。Telegram 设置未更改。"; });
        }
        catch (Exception exception) { await ReportErrorAsync("删除 AppSecret", exception); }
    }

    public async Task DeleteBindingAsync(string bindingId, CancellationToken cancellationToken = default)
    {
        try
        {
            await _api.DeleteBindingAsync(bindingId, cancellationToken).ConfigureAwait(false);
            await _refreshBindings(cancellationToken).ConfigureAwait(false);
            await RunOnUiAsync(() => OperationMessage = "关联会话已删除；Codex 会话未删除。");
        }
        catch (Exception exception) { await ReportErrorAsync("删除绑定", exception); }
    }

    public async Task AddDiscoveredIdentityAsync(QqDiscoveredIdentity identity)
    {
        if (identity.Type == "c2c" && !string.IsNullOrWhiteSpace(identity.UserOpenId))
            AllowedUserOpenIdsText = AppendLine(AllowedUserOpenIdsText, identity.UserOpenId);
        if (identity.Type == "group")
        {
            if (!string.IsNullOrWhiteSpace(identity.GroupOpenId)) AllowedGroupOpenIdsText = AppendLine(AllowedGroupOpenIdsText, identity.GroupOpenId);
            if (!string.IsNullOrWhiteSpace(identity.GroupMemberOpenId)) AllowedGroupMemberOpenIdsText = AppendLine(AllowedGroupMemberOpenIdsText, identity.GroupMemberOpenId);
        }
        await SaveSettingsAsync();
    }

    public async Task RefreshStatusAsync(CancellationToken cancellationToken = default, bool preserveOperationMessage = false)
    {
        try
        {
			var status = await _api.GetQqStatusAsync(cancellationToken).ConfigureAwait(false);
            await RunOnUiAsync(() =>
            {
				ApplyStatus(status);
				LoadError = "";
				LoadErrorType = "";
                if (!preserveOperationMessage) OperationMessage = "状态已刷新。";
            });
        }
		catch (Exception exception)
		{
			_logs.AddException("desktop", "刷新 QQ Bot 主状态失败。", exception);
			if (!preserveOperationMessage)
				await SetOperationErrorAsync("刷新 QQ Bot 状态", exception);
		}
		await RefreshDiscoveredIdentitiesHandledAsync(cancellationToken).ConfigureAwait(false);
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (bridgeEvent.EventType.StartsWith("qqbot.", StringComparison.OrdinalIgnoreCase) ||
            (bridgeEvent.Payload.ValueKind == JsonValueKind.Object && bridgeEvent.Payload.TryGetProperty("channelType", out var type) && type.GetString() == "qqbot"))
            _scheduleRefresh();
    }

	public void ReplaceBindings(IEnumerable<ChannelBinding>? bindings)
    {
        Bindings.Clear();
		foreach (var binding in (bindings ?? [])
			.Where(item => !item.Legacy && string.Equals(item.ChannelType, "qqbot", StringComparison.OrdinalIgnoreCase))
			.OrderByDescending(item => item.CreatedAt, StringComparer.Ordinal)) Bindings.Add(binding);
    }

	public Task ClearBindingLoadErrorAsync() => RunOnUiAsync(() =>
	{
		_bindingLoadError = "";
		NotifyAuxiliaryLoadError();
	});

	public async Task ReportBindingLoadErrorAsync(Exception exception)
	{
		_logs.AddException("desktop", "关联会话列表加载失败。", exception);
		await RunOnUiAsync(() =>
		{
			_bindingLoadError = $"无法读取关联会话：{SafeExceptionMessage(exception)}";
			NotifyAuxiliaryLoadError();
		});
	}

    public async Task StopForShutdownAsync(CancellationToken cancellationToken = default)
    {
        if (!Running) return;
        try { await _api.StopQqAsync(cancellationToken).ConfigureAwait(false); }
        catch (Exception exception) { _logs.AddException("desktop", "退出时停止 QQ Official Bot 失败。", exception); }
    }

    public void CancelPendingOperations() => _lifetime.Cancel();
    public Task DisposeAsync() { _lifetime.Cancel(); _lifetime.Dispose(); return Task.CompletedTask; }

    private async Task SaveSettingsAsync()
    {
        if (!TryCaptureSettings(out var error)) { OperationMessage = error; return; }
        try
        {
            await _settingsService.SaveAsync(_settings).ConfigureAwait(false);
            var status = await _api.ConfigureQqAsync(BuildRequest(), _lifetime.Token).ConfigureAwait(false);
            await RunOnUiAsync(() => { ApplyStatus(status); OperationMessage = "QQ 官方机器人配置已保存。"; });
        }
        catch (Exception exception) { await ReportErrorAsync("保存 QQ 机器人设置", exception); }
    }

    private async Task TestAsync()
    {
        if (!TryCaptureSettings(out var error)) { OperationMessage = error; return; }
        try
        {
            await _settingsService.SaveAsync(_settings).ConfigureAwait(false);
            await _api.ConfigureQqAsync(BuildRequest(), _lifetime.Token).ConfigureAwait(false);
            var result = await _api.TestQqAsync(_lifetime.Token).ConfigureAwait(false);
            await RunOnUiAsync(() => OperationMessage = result.Success
                ? "QQ 应用凭据有效。"
                : DescribeCategory(result.Code, result.Message));
        }
        catch (Exception exception) { await ReportErrorAsync("测试 QQ Bot 凭据", exception); }
    }

    private async Task TestNetworkAsync()
    {
        if (!TryCaptureSettings(out var error)) { OperationMessage = error; return; }
        try
        {
            await _api.ConfigureQqAsync(BuildRequest(), _lifetime.Token).ConfigureAwait(false);
            var result = await _api.TestQqNetworkAsync(_lifetime.Token).ConfigureAwait(false);
            await RunOnUiAsync(() => OperationMessage = result.Success
                ? $"QQ 网络可用（{result.DurationMs} ms，{FormatProxy(result.EffectiveProxyMode, result.MaskedProxyAddress)}）。"
                : DescribeCategory(result.Code, result.Message));
        }
        catch (Exception exception) { await ReportErrorAsync("测试 QQ 网络", exception); }
    }

    private async Task StartAsync()
    {
        if (!TryCaptureSettings(out var error)) { OperationMessage = error; return; }
        try
        {
            await _settingsService.SaveAsync(_settings).ConfigureAwait(false);
            await _api.ConfigureQqAsync(BuildRequest(), _lifetime.Token).ConfigureAwait(false);
            var status = await _api.StartQqAsync(_lifetime.Token).ConfigureAwait(false);
            await RunOnUiAsync(() => { ApplyStatus(status); OperationMessage = "QQ 机器人已启动并连接。"; });
        }
        catch (Exception exception) { await ReportErrorAsync("启动 QQ 官方机器人", exception); }
    }

    private async Task StopAdapterAsync()
    {
        try
        {
            var status = await _api.StopQqAsync(_lifetime.Token).ConfigureAwait(false);
            await RunOnUiAsync(() => { ApplyStatus(status); OperationMessage = "QQ 官方机器人已停止；Telegram 不受影响。"; });
        }
        catch (Exception exception) { await ReportErrorAsync("停止 QQ 官方机器人", exception); }
    }

    private bool TryCaptureSettings(out string error)
    {
        var appId = AppId.Trim();
        if (appId.Length > 0 && (appId.Length is < 5 or > 32 || appId.Any(character => !char.IsAsciiDigit(character))))
        {
            error = "AppID 必须是 5 到 32 位数字。";
            return false;
        }
        if (ProxyMode == "custom-http" && !ValidProxy(ProxyUrl))
        {
            error = "自定义代理必须是无凭据、无路径和查询参数的 http:// 地址。";
            return false;
        }
        _settings.QqAppId = appId;
        _settings.QqEnvironment = "production";
        _settings.QqAllowedUserOpenIds = ParseLines(AllowedUserOpenIdsText);
        _settings.QqAllowedGroupOpenIds = ParseLines(AllowedGroupOpenIdsText);
        _settings.QqAllowedGroupMemberOpenIds = ParseLines(AllowedGroupMemberOpenIdsText);
        _settings.QqAutoStart = AutoStart;
        _settings.QqReconnectEnabled = GatewayReconnectEnabled;
        _settings.QqSendProgressUpdates = SendProgressUpdates;
        _settings.QqGroupTriggerMode = "official-at";
        _settings.QqCommandPrefix = "/codex";
        _settings.QqProxyMode = ProxyMode;
        _settings.QqProxyUrl = ProxyMode == "custom-http" ? ProxyUrl.Trim() : "";
        error = "";
        return true;
    }

    private QqConfigureRequest BuildRequest() => new()
    {
        Enabled = !string.IsNullOrWhiteSpace(_settings.QqAppId),
        AutoStart = _settings.QqAutoStart,
        AppId = _settings.QqAppId,
        Environment = "production",
        GatewayReconnectEnabled = _settings.QqReconnectEnabled,
        SendProgressUpdates = _settings.QqSendProgressUpdates,
        AllowedUserOpenIds = [.. _settings.QqAllowedUserOpenIds],
        AllowedGroupOpenIds = [.. _settings.QqAllowedGroupOpenIds],
        AllowedGroupMemberOpenIds = [.. _settings.QqAllowedGroupMemberOpenIds],
        GroupTriggerMode = "official-at",
        CommandPrefix = "/codex",
        ProxyMode = _settings.QqProxyMode,
        ProxyUrl = _settings.QqProxyUrl
    };

    private void ApplyStatus(QqChannelStatus status)
    {
        Configured = status.Configured;
        Running = status.Running;
        Connected = status.Connected;
        SecretConfigured = status.SecretConfigured;
        GatewayState = UiText.Status(status.GatewayState);
        SessionIdShort = Empty(status.SessionIdShort);
        LastConnectedAt = UiText.LocalDateTime(status.LastConnectedAt);
        LastHeartbeatAt = UiText.LocalDateTime(status.LastHeartbeatAt);
        LastDispatchAt = UiText.LocalDateTime(status.LastDispatchAt);
        AccessTokenExpiresAt = UiText.LocalDateTime(status.AccessTokenExpiresAt);
        ReconnectCount = status.ReconnectCount;
        AllowedUserCount = status.AllowedUserCount;
        AllowedGroupCount = status.AllowedGroupCount;
        AllowedGroupMemberCount = status.AllowedGroupMemberCount;
        BindingCount = status.BindingCount;
        EffectiveProxy = FormatProxy(status.EffectiveProxyMode, status.MaskedProxyAddress);
        LastError = string.IsNullOrWhiteSpace(status.LastErrorCode) ? "" : $"{DescribeCategory(status.LastErrorCode, status.LastErrorMessage)}";
    }

	private void ReplaceDiscovered(IEnumerable<QqDiscoveredIdentity>? identities)
    {
        DiscoveredIdentities.Clear();
		foreach (var identity in identities ?? []) DiscoveredIdentities.Add(identity);
    }

	private async Task RefreshDiscoveredIdentitiesHandledAsync(CancellationToken cancellationToken)
	{
		try
		{
			var discovered = await _api.GetQqDiscoveredIdentitiesAsync(cancellationToken).ConfigureAwait(false);
			await RunOnUiAsync(() =>
			{
				ReplaceDiscovered(discovered.Identities);
				_discoveredLoadError = "";
				NotifyAuxiliaryLoadError();
			});
		}
		catch (OperationCanceledException) when (_lifetime.IsCancellationRequested || cancellationToken.IsCancellationRequested)
		{
		}
		catch (Exception exception)
		{
			_logs.AddException("desktop", "最近发现身份加载失败。", exception);
			await RunOnUiAsync(() =>
			{
			_discoveredLoadError = $"无法读取最近发现的 QQ 账号：{SafeExceptionMessage(exception)}";
				NotifyAuxiliaryLoadError();
			});
		}
	}

	private async Task ReportErrorAsync(string action, Exception exception)
	{
		_logs.AddException("desktop", $"{action}失败。", exception);
		await SetOperationErrorAsync(action, exception);
	}

	private Task SetOperationErrorAsync(string action, Exception exception) => RunOnUiAsync(() =>
    {
		OperationMessage = UiText.UserError(exception, action);
    });

	private static string SafeExceptionMessage(Exception exception) =>
		exception is BridgeApiException api ? DescribeCategory(api.Code, api.Message) : UiText.UserError(exception);

    private static string DescribeCategory(string? code, string? fallback) => code?.Trim().ToLowerInvariant().Replace('-', '_') switch
    {
        "credentials_missing" or "qqbot_credentials_missing" => "缺少 AppID 或 AppSecret，请先保存凭据。",
        "appid_invalid" or "qqbot_appid_invalid" => "AppID 无效，请复制 QQ 开放平台显示的 AppID。",
        "secret_invalid" or "qqbot_secret_invalid" => "AppSecret 无效，请重新复制或生成。",
        "auth_failed" or "qqbot_auth_failed" => "无法连接 QQ 机器人，请检查 AppID、AppSecret（应用密钥）和机器人状态。",
		"token_expired" or "token_refresh_failed" => "QQ 登录凭据已失效，请重新测试应用凭据。",
		"gateway_lookup_failed" or "qqbot_gateway_failed" => "无法连接 QQ 机器人，请检查机器人状态、权限和网络。",
		"gateway_auth_failed" => "QQ 应用凭据无效，请重新检查。",
		"gateway_permission_denied" => "QQ 机器人权限不足，请检查开放平台中的消息权限。",
		"gateway_endpoint_not_found" or "gateway_response_invalid" => "当前版本与 QQ 服务不兼容，请查看运行日志并升级软件。",
		"gateway_connect_failed" => "无法连接 QQ 机器人，请检查网络或代理。",
		"gateway_identify_failed" => "QQ 机器人连接失败，请检查凭据和消息权限。",
		"gateway_closed" => "QQ 连接已断开；开启自动重连后会继续尝试恢复。",
		"gateway_session_invalid" => "QQ 连接已失效，正在重新连接。",
        "intent_not_enabled" or "permission_not_granted" => "当前机器人应用尚未获得群聊/C2C 消息权限，请在 QQ 开放平台启用。",
        "rate_limited" or "qqbot_rate_limited" => "QQ 平台已限流，请稍后重试。",
        "network_timeout" or "qqbot_token_timeout" => "请求超时，请检查网络或代理。",
		"dns_failed" => "无法连接 QQ 服务，请检查网络设置。",
		"tls_failed" => "安全连接失败，请检查系统时间、网络或代理。",
		"network_error" or "qqbot_network_error" => "无法连接 QQ 服务，请检查网络或代理。",
        "proxy_failed" or "qqbot_proxy_failed" => "代理连接失败，请检查代理地址。",
		"message_send_failed" => "QQ 消息发送失败，请检查目标标识、消息窗口和平台状态。",
		"message_window_expired" => "原消息回复窗口已过期，请在 QQ 中重新发送一条消息。",
		"invalid_openid" => "用户或群聊标识无效，请从最近发现的 QQ 账号重新添加。",
        "protocol_incompatible" or "qqbot_protocol_error" => "QQ 官方协议响应不兼容，请查看日志并升级。",
        _ => "QQ 机器人操作失败，请重试。详情已写入运行日志。"
    };

    private static (string Message, string Type) DescribeLoadError(Exception exception)
    {
        if (exception is QqSecretException) return ("无法读取已保存的应用密钥；你可以删除后重新保存。", "DPAPI");
		if (exception is BridgeApiException api && api.StatusCode == HttpStatusCode.NotFound) return ("当前软件版本不支持 QQ 机器人，请升级后重试。", "HTTP");
		return (UiText.UserError(exception, "读取 QQ 机器人设置"), exception.GetType().Name);
    }

	private static List<string> ParseLines(string? value) => (value ?? "")
        .Split(['\r', '\n', ',', '，'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Where(item => item.Length <= 256)
        .Distinct(StringComparer.Ordinal)
        .ToList();

	private static string JoinLines(IEnumerable<string>? values) => string.Join(System.Environment.NewLine, (values ?? []).Distinct(StringComparer.Ordinal));
    private static string AppendLine(string current, string value) => JoinLines(ParseLines(current).Append(value.Trim()));
    private static string NormalizeProxyMode(string? value) => value?.Trim().ToLowerInvariant() switch { "direct" => "direct", "custom-http" => "custom-http", _ => "environment" };
    private static bool ValidProxy(string value) => Uri.TryCreate(value.Trim(), UriKind.Absolute, out var uri) && uri.Scheme == "http" && string.IsNullOrEmpty(uri.UserInfo) && string.IsNullOrEmpty(uri.Query) && string.IsNullOrEmpty(uri.Fragment) && (uri.AbsolutePath == "/" || string.IsNullOrEmpty(uri.AbsolutePath));
    private static string FormatProxy(string mode, string address) => mode switch { "direct" => "直接连接", "custom-http" => $"自定义 HTTP 代理 {Empty(address)}", _ => "环境变量" };
    private static string Empty(string? value) => string.IsNullOrWhiteSpace(value) ? "—" : value;

	private void NotifyStatus() => OnPropertyChanged(nameof(StatusText));
	private void NotifyLoadVisibility() { OnPropertyChanged(nameof(LoadingVisibility)); OnPropertyChanged(nameof(ErrorVisibility)); }
	private void NotifyAuxiliaryLoadError() { OnPropertyChanged(nameof(AuxiliaryLoadMessage)); OnPropertyChanged(nameof(AuxiliaryErrorVisibility)); }
    private static Task RunOnUiAsync(Action action)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null || dispatcher.CheckAccess()) { action(); return Task.CompletedTask; }
        return dispatcher.InvokeAsync(action).Task;
    }
}
