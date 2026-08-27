using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class ChannelsViewModel : ObservableObject
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly BridgeApiClient _api;
    private readonly SettingsService _settingsService;
    private readonly TelegramSecretService _secretService;
    private readonly UserSettings _settings;
    private readonly LogService _logs;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _initializationLock = new();
    private Task? _initializationTask;
    private CancellationTokenSource? _eventRefresh;
    private bool _initialized;
    private string _allowedUserIdsText;
    private int _pollingTimeout;
    private bool _autoStart;
    private bool _sendProgressUpdates;
    private string _proxyMode;
    private string _proxyUrl;
    private bool _configured;
    private bool _running;
    private bool _connected;
    private string _botName = "—";
    private string _botUsername = "—";
    private string _tokenSummary = "未保存";
    private string _pollingState = "未启动";
    private string _lastUpdateAt = "—";
    private string _lastError = "";
    private string _startedAt = "—";
    private string _configuredProxyMode = "—";
    private string _maskedProxyAddress = "—";
    private string _effectiveProxyMode = "—";
    private string _lastNetworkStage = "—";
    private string _lastRequestDuration = "—";
    private string _lastErrorCategory = "—";
    private int _allowedUserCount;
    private int _bindingCount;
    private string _operationMessage = "";
    private bool _stopped;
    private bool _isLoading;
    private bool _isContentReady;
    private string _loadError = "";
    private string _loadErrorType = "";

    public ChannelsViewModel(
        BridgeApiClient api,
        SettingsService settingsService,
        TelegramSecretService secretService,
        QqSecretService qqSecretService,
        UserSettings settings,
        LogService logs)
    {
        ArgumentNullException.ThrowIfNull(api);
        ArgumentNullException.ThrowIfNull(settingsService);
        ArgumentNullException.ThrowIfNull(secretService);
        ArgumentNullException.ThrowIfNull(qqSecretService);
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(logs);
        _api = api;
        _settingsService = settingsService;
        _secretService = secretService;
        _settings = settings;
        _logs = logs;
        _allowedUserIdsText = string.Join(Environment.NewLine, settings.TelegramAllowedUserIds.Distinct());
        _pollingTimeout = Math.Clamp(settings.TelegramPollingTimeoutSeconds, 10, 60);
        _autoStart = settings.TelegramAutoStart;
        _sendProgressUpdates = settings.TelegramSendProgressUpdates;
        _proxyMode = NormalizeProxyMode(settings.TelegramProxyMode);
        _proxyUrl = _proxyMode == "custom-http" ? settings.TelegramProxyUrl : "";
        Qq = new QqChannelViewModel(
            api, settingsService, qqSecretService, settings, logs,
            RefreshBindingsHandledAsync, ScheduleEventRefresh);

        SaveSettingsCommand = new AsyncRelayCommand(SaveSettingsAsync);
        TestCommand = new AsyncRelayCommand(TestAsync);
        TestProxyCommand = new AsyncRelayCommand(TestProxyAsync);
        StartCommand = new AsyncRelayCommand(StartAsync);
        StopCommand = new AsyncRelayCommand(StopAdapterAsync);
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync());
        RetryCommand = new AsyncRelayCommand(() => InitializeCoreHandledAsync(_lifetime.Token));
        OpenLogsCommand = new AsyncRelayCommand(OpenLogsAsync);
    }

    public ObservableCollection<ChannelBinding> Bindings { get; } = [];
    public QqChannelViewModel Qq { get; }
    public IReadOnlyList<int> PollingTimeoutOptions { get; } = [10, 15, 20, 30, 45, 60];
    public ICommand SaveSettingsCommand { get; }
    public ICommand TestCommand { get; }
    public ICommand TestProxyCommand { get; }
    public ICommand StartCommand { get; }
    public ICommand StopCommand { get; }
    public ICommand RefreshCommand { get; }
    public ICommand RetryCommand { get; }
    public ICommand OpenLogsCommand { get; }

    public bool IsLoading { get => _isLoading; private set { if (SetProperty(ref _isLoading, value)) NotifyLoadVisibility(); } }
    public bool IsContentReady { get => _isContentReady; private set { if (SetProperty(ref _isContentReady, value)) NotifyLoadVisibility(); } }
    public string LoadError { get => _loadError; private set { if (SetProperty(ref _loadError, value)) NotifyLoadVisibility(); } }
    public string LoadErrorType { get => _loadErrorType; private set { if (SetProperty(ref _loadErrorType, value)) OnPropertyChanged(nameof(TokenRecoveryVisibility)); } }
    public Visibility LoadingVisibility => IsLoading ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ContentVisibility => Visibility.Visible;
    public Visibility ErrorVisibility => !IsLoading && !string.IsNullOrWhiteSpace(LoadError)
        ? Visibility.Visible : Visibility.Collapsed;
    public Visibility TokenRecoveryVisibility => LoadErrorType == "DPAPI" ? Visibility.Visible : Visibility.Collapsed;

    public string AllowedUserIdsText
    {
        get => _allowedUserIdsText;
        set => SetProperty(ref _allowedUserIdsText, value);
    }

    public int PollingTimeout
    {
        get => _pollingTimeout;
        set => SetProperty(ref _pollingTimeout, value);
    }

    public bool AutoStart
    {
        get => _autoStart;
        set => SetProperty(ref _autoStart, value);
    }

    public bool SendProgressUpdates
    {
        get => _sendProgressUpdates;
        set => SetProperty(ref _sendProgressUpdates, value);
    }

    public string ProxyMode
    {
        get => _proxyMode;
        set
        {
            var normalized = NormalizeProxyMode(value);
            if (!SetProperty(ref _proxyMode, normalized)) return;
            OnPropertyChanged(nameof(CustomProxyVisibility));
            OnPropertyChanged(nameof(ProxyModeDescription));
        }
    }

    public string ProxyUrl
    {
        get => _proxyUrl;
        set => SetProperty(ref _proxyUrl, value);
    }

    public Visibility CustomProxyVisibility => ProxyMode == "custom-http" ? Visibility.Visible : Visibility.Collapsed;
    public string ProxyModeDescription => ProxyMode switch
    {
        "direct" => "直连会忽略 HTTP_PROXY、HTTPS_PROXY 和 NO_PROXY 环境变量。",
        "custom-http" => "仅对此 Telegram 通道使用下面的 HTTP 代理地址。",
        _ => "使用 HTTP_PROXY、HTTPS_PROXY 和 NO_PROXY 环境变量决定连接方式。"
    };

    public bool Configured
    {
        get => _configured;
        private set { if (SetProperty(ref _configured, value)) OnPropertyChanged(nameof(StatusText)); }
    }

    public bool Running
    {
        get => _running;
        private set { if (SetProperty(ref _running, value)) OnPropertyChanged(nameof(StatusText)); }
    }

    public bool Connected
    {
        get => _connected;
        private set { if (SetProperty(ref _connected, value)) OnPropertyChanged(nameof(StatusText)); }
    }

    public string StatusText => !Configured ? "未配置" : Running ? (Connected ? "运行中 · 已连接" : "运行中 · 等待连接") : "已配置 · 已停止";
    public string BotName { get => _botName; private set => SetProperty(ref _botName, value); }
    public string BotUsername { get => _botUsername; private set => SetProperty(ref _botUsername, value); }
    public string TokenSummary { get => _tokenSummary; private set => SetProperty(ref _tokenSummary, value); }
    public string PollingState { get => _pollingState; private set => SetProperty(ref _pollingState, value); }
    public string LastUpdateAt { get => _lastUpdateAt; private set => SetProperty(ref _lastUpdateAt, value); }
    public string LastError { get => _lastError; private set => SetProperty(ref _lastError, value); }
    public string StartedAt { get => _startedAt; private set => SetProperty(ref _startedAt, value); }
    public string ConfiguredProxyMode { get => _configuredProxyMode; private set => SetProperty(ref _configuredProxyMode, value); }
    public string MaskedProxyAddress { get => _maskedProxyAddress; private set => SetProperty(ref _maskedProxyAddress, value); }
    public string EffectiveProxyMode { get => _effectiveProxyMode; private set => SetProperty(ref _effectiveProxyMode, value); }
    public string LastNetworkStage { get => _lastNetworkStage; private set => SetProperty(ref _lastNetworkStage, value); }
    public string LastRequestDuration { get => _lastRequestDuration; private set => SetProperty(ref _lastRequestDuration, value); }
    public string LastErrorCategory { get => _lastErrorCategory; private set => SetProperty(ref _lastErrorCategory, value); }
    public int AllowedUserCount { get => _allowedUserCount; private set => SetProperty(ref _allowedUserCount, value); }
    public int BindingCount { get => _bindingCount; private set => SetProperty(ref _bindingCount, value); }
    public string OperationMessage { get => _operationMessage; private set => SetProperty(ref _operationMessage, value); }

    public Task InitializeAsync(CancellationToken cancellationToken = default) => EnsureInitializedAsync(cancellationToken);

    public Task EnsureInitializedAsync(CancellationToken cancellationToken = default, bool forceRetry = false)
    {
        lock (_initializationLock)
        {
            if (_stopped) return Task.CompletedTask;
            if (_initialized && !forceRetry) return Task.CompletedTask;
            if (_initializationTask is { IsCompleted: false }) return _initializationTask;
            _initializationTask = InitializeAllHandledAsync(cancellationToken, forceRetry);
            return _initializationTask;
        }
    }

    private async Task InitializeAllHandledAsync(CancellationToken cancellationToken, bool forceRetry)
    {
        var telegramTask = InitializeCoreHandledAsync(cancellationToken);
        var qqTask = Qq.EnsureInitializedAsync(cancellationToken, forceRetry);
        await Task.WhenAll(telegramTask, qqTask).ConfigureAwait(false);
        _initialized = true;
    }

    private async Task InitializeCoreHandledAsync(CancellationToken cancellationToken)
    {
        try
        {
            await SetLoadStateAsync(true, false, "", "");
            var token = await _secretService.LoadAsync(cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(token))
            {
                using var configureTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
                configureTimeout.CancelAfter(TimeSpan.FromSeconds(15));
                await _api.ConfigureTelegramAsync(BuildRequest(token), configureTimeout.Token).ConfigureAwait(false);
            }

            using (var refreshTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token))
            {
                refreshTimeout.CancelAfter(TimeSpan.FromSeconds(15));
                var status = await _api.GetTelegramStatusAsync(refreshTimeout.Token).ConfigureAwait(false);
                await RunOnUiAsync(() =>
                {
                    ApplyStatus(status);
                    if (string.IsNullOrWhiteSpace(token))
                        OperationMessage = "未找到已保存的 Token；可以在此完成配置。";
                });
            }

            if (!string.IsNullOrWhiteSpace(token) && _settings.TelegramAutoStart)
            {
                if (_settings.TelegramAllowedUserIds.Count == 0)
                    await RunOnUiAsync(() => OperationMessage = "自动启动已跳过：请先填写至少一个允许的 Telegram 用户 ID。");
                else
                {
                    using var startTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
                    startTimeout.CancelAfter(TimeSpan.FromSeconds(15));
                    var started = await _api.StartTelegramAsync(startTimeout.Token).ConfigureAwait(false);
                    await RunOnUiAsync(() => ApplyStatus(started));
                }
            }
            await SetLoadStateAsync(false, true, "", "");
        }
        catch (OperationCanceledException) when (_stopped || _lifetime.IsCancellationRequested || cancellationToken.IsCancellationRequested)
        {
            await SetLoadStateAsync(false, false, "", "");
        }
        catch (Exception exception)
        {
            var (message, type) = DescribeLoadError(exception);
            _logs.AddException("desktop", "初始化远程渠道失败。", exception);
            await SetLoadStateAsync(false, false, message, type);
        }
    }

    public async Task<bool> SaveTokenAsync(string token, CancellationToken cancellationToken = default)
    {
        token = token.Trim();
        if (string.IsNullOrWhiteSpace(token))
        {
            OperationMessage = "请输入 Telegram Bot Token。";
            return false;
        }
        if (!TryApplyEditorSettings(requireAllowedUser: false)) return false;

        try
        {
            var status = await _api.ConfigureTelegramAsync(BuildRequest(token), cancellationToken);
            try
            {
                await _secretService.SaveAsync(token, cancellationToken);
            }
            catch (Exception saveException)
            {
                try
                {
                    await _api.DeleteTelegramTokenAsync(cancellationToken);
                }
                catch (Exception rollbackException)
                {
                    _logs.AddException("desktop", "DPAPI 保存失败后，清除后端 Telegram Token 也失败。", rollbackException);
                    throw new InvalidOperationException(
                        "无法保存 DPAPI Token，且后端 Token 回滚失败；后端可能仍暂时持有该 Token。请停止 Bridge 后重新配置。",
                        new AggregateException(saveException, rollbackException));
                }
                throw;
            }
            await _settingsService.SaveAsync(_settings);
            ApplyStatus(status);
            OperationMessage = "Telegram 机器人密钥已安全保存到本机，输入框已清空。";
            _logs.Add("desktop", "Telegram Token 已安全保存并应用（未记录凭据）。");
            return true;
        }
        catch (Exception exception)
        {
            ReportError("保存 Telegram Token", exception);
            return false;
        }
    }

    public async Task DeleteTokenAsync(CancellationToken cancellationToken = default)
    {
        var localDeleted = false;
        try
        {
            // Delete the durable copy first so a transient backend error cannot
            // leave a token behind after the user explicitly asked to remove it.
            await _secretService.DeleteAsync();
            localDeleted = true;
            await _api.DeleteTelegramTokenAsync(cancellationToken);
            Configured = false;
            Running = false;
            Connected = false;
            TokenSummary = "未保存";
            OperationMessage = "Telegram Token 已删除。";
            await InitializeCoreHandledAsync(cancellationToken);
        }
        catch (Exception exception)
        {
            ReportError("删除 Telegram Token（本地副本已删除）", exception);
            if (localDeleted && !_stopped)
                await RefreshAsync(cancellationToken, preserveOperationMessage: true);
        }
    }

    public async Task DeleteBindingAsync(string bindingId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(bindingId)) return;
        try
        {
            await _api.DeleteBindingAsync(bindingId, cancellationToken);
            var existing = Bindings.FirstOrDefault(binding => binding.Id == bindingId);
            if (existing is not null) Bindings.Remove(existing);
            BindingCount = Bindings.Count;
            OperationMessage = "关联会话已删除。";
        }
        catch (Exception exception)
        {
            ReportError("删除关联会话", exception);
        }
    }

    public async Task RefreshAsync(CancellationToken cancellationToken = default, bool preserveOperationMessage = false)
    {
        var telegramTask = RefreshTelegramStatusHandledAsync(cancellationToken, preserveOperationMessage);
        var qqTask = Qq.RefreshStatusAsync(cancellationToken, preserveOperationMessage);
        var bindingsTask = RefreshBindingsHandledAsync(cancellationToken);
        await Task.WhenAll(telegramTask, qqTask, bindingsTask);
    }

    private async Task RefreshTelegramStatusHandledAsync(CancellationToken cancellationToken, bool preserveOperationMessage)
    {
        try
        {
            ApplyStatus(await _api.GetTelegramStatusAsync(cancellationToken));
            LoadError = "";
            LoadErrorType = "";
            if (!preserveOperationMessage) OperationMessage = "Telegram 状态已刷新。";
        }
        catch (Exception exception)
        {
            ReportError("刷新 Telegram 状态", exception);
        }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (_stopped || !IsContentReady) return;
        var eventType = bridgeEvent.EventType.ToLowerInvariant();
        if (eventType.StartsWith("binding.", StringComparison.Ordinal))
        {
            if (eventType is "binding.created" or "binding.deleted" or "binding.updated") ScheduleEventRefresh();
            return;
        }
        var payloadChannelType = ReadPayloadChannelType(bridgeEvent.Payload);
		if (eventType.StartsWith("qqbot.", StringComparison.Ordinal) ||
			eventType.StartsWith("channel.qqbot.", StringComparison.Ordinal) ||
			IsChannelNamespace(payloadChannelType, "qqbot"))
        {
            Qq.ApplyEvent(bridgeEvent);
            return;
        }
        if (!eventType.StartsWith("channel.", StringComparison.Ordinal) &&
            !eventType.StartsWith("telegram.", StringComparison.Ordinal)) return;
        if (!eventType.StartsWith("telegram.", StringComparison.Ordinal) &&
            !eventType.StartsWith("channel.telegram.", StringComparison.Ordinal) &&
            !IsChannelNamespace(payloadChannelType, "telegram"))
        {
            ScheduleEventRefresh();
            return;
        }
        if (eventType.Contains("message", StringComparison.Ordinal) || eventType.Contains("delta", StringComparison.Ordinal) ||
            eventType.Contains("progress", StringComparison.Ordinal) || eventType.Contains("update.received", StringComparison.Ordinal)) return;

        if (TryReadStatus(bridgeEvent.Payload, out var status))
        {
            ApplyStatus(status);
            return;
        }
        if (eventType is "telegram.started" or "telegram.stopped" or "telegram.connected" or "telegram.disconnected" or
            "channel.started" or "channel.stopped" or "channel.connected" or "channel.disconnected" or "channel.configured" or
            "channel.telegram.configured" or "channel.telegram.started" or "channel.telegram.stopped" or
            "channel.telegram.start_failed" or "channel.telegram.token_deleted")
        {
            ScheduleEventRefresh();
        }
    }

    public async Task StopAsync()
    {
        if (_stopped) return;
        _stopped = true;
        _lifetime.Cancel();
        Qq.CancelPendingOperations();
        var telegramStop = StopTelegramForShutdownAsync();
        var qqStop = Qq.StopForShutdownAsync();
        await Task.WhenAll(telegramStop, qqStop).ConfigureAwait(false);
        _eventRefresh?.Cancel();
        _eventRefresh?.Dispose();
        _eventRefresh = null;
        var qqDispose = Qq.DisposeAsync();
        Task? initialization;
        lock (_initializationLock) initialization = _initializationTask;
        if (initialization is not null)
        {
            try { await initialization.ConfigureAwait(false); }
            catch { /* Initialization owns and logs its failures. */ }
        }
        await qqDispose.ConfigureAwait(false);
        _lifetime.Dispose();
    }

    private async Task StopTelegramForShutdownAsync()
    {
        using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await _api.StopTelegramAsync(stopTimeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logs.AddException("desktop", "退出时停止 Telegram Long Polling 失败；QQ 与 daemon 关闭流程不受影响。", exception);
        }
    }

    private async Task SaveSettingsAsync()
    {
        if (!TryApplyEditorSettings(requireAllowedUser: false)) return;
        try
        {
            await _settingsService.SaveAsync(_settings);
            var token = await _secretService.LoadAsync(_lifetime.Token);
            ApplyStatus(await _api.ConfigureTelegramAsync(BuildRequest(token), _lifetime.Token));
            OperationMessage = "Telegram 配置已保存并应用。";
        }
        catch (Exception exception)
        {
            ReportError("保存 Telegram 配置", exception);
        }
    }

    private async Task TestAsync()
    {
        if (!TryApplyEditorSettings(requireAllowedUser: false)) return;
        try
        {
            var token = await _secretService.LoadAsync(_lifetime.Token);
            if (string.IsNullOrWhiteSpace(token))
            {
                OperationMessage = "请先保存 Telegram Bot Token。";
                return;
            }
            await _settingsService.SaveAsync(_settings);
            ApplyStatus(await _api.ConfigureTelegramAsync(BuildRequest(token), _lifetime.Token));
            var result = await _api.TestTelegramAsync(_lifetime.Token);
            if (!string.IsNullOrWhiteSpace(result.DisplayName)) BotName = result.DisplayName;
            BotUsername = string.IsNullOrWhiteSpace(result.Username) ? "—" : $"@{result.Username.TrimStart('@')}";
            OperationMessage = $"连接测试成功：{BotName} {BotUsername}";
        }
        catch (Exception exception)
        {
            ReportError("测试 Telegram", exception);
        }
    }

    private async Task TestProxyAsync()
    {
        if (!TryBuildProxySettings(out var proxyMode, out var proxyUrl)) return;
        try
        {
            var result = await _api.TestTelegramProxyAsync(new TelegramProxyTestRequest
            {
                ProxyMode = proxyMode,
                ProxyUrl = proxyUrl
            }, _lifetime.Token);
            var mode = FormatProxyMode(result.EffectiveProxyMode);
            var address = string.IsNullOrWhiteSpace(result.MaskedProxyAddress) ? "未使用代理地址" : result.MaskedProxyAddress;
            var duration = result.DurationMs > 0 ? $"{result.DurationMs} ms" : "未提供耗时";
            if (result.Ok)
            {
                OperationMessage = $"代理测试成功：实际模式 {mode}，{address}，耗时 {duration}。";
                return;
            }

            OperationMessage = UiText.UserError(result.Message, "测试代理");
            _logs.Add("desktop", $"Telegram 代理测试失败：{result.Category} {result.Message}".Trim());
        }
        catch (Exception exception)
        {
            ReportError("测试 Telegram 代理", exception);
        }
    }

    private async Task StartAsync()
    {
        if (!TryApplyEditorSettings(requireAllowedUser: true)) return;
        try
        {
            var token = await _secretService.LoadAsync(_lifetime.Token);
            if (string.IsNullOrWhiteSpace(token))
            {
                OperationMessage = "启动前必须先保存 Telegram Bot Token。";
                return;
            }
            await _settingsService.SaveAsync(_settings);
            ApplyStatus(await _api.ConfigureTelegramAsync(BuildRequest(token), _lifetime.Token));
            ApplyStatus(await _api.StartTelegramAsync(_lifetime.Token));
            OperationMessage = "Telegram 已启动。";
        }
        catch (Exception exception)
        {
            ReportError("启动 Telegram", exception);
        }
    }

    private async Task StopAdapterAsync()
    {
        try
        {
            ApplyStatus(await _api.StopTelegramAsync(_lifetime.Token));
            OperationMessage = "Telegram 已停止。";
        }
        catch (Exception exception)
        {
            ReportError("停止 Telegram", exception);
        }
    }

    private TelegramConfigureRequest BuildRequest(string? token) => new()
    {
        Token = string.IsNullOrWhiteSpace(token) ? null : token,
        AllowedUserIds = [.. _settings.TelegramAllowedUserIds],
        PollingTimeoutSeconds = _settings.TelegramPollingTimeoutSeconds,
        SendProgressUpdates = _settings.TelegramSendProgressUpdates,
        AutoStart = _settings.TelegramAutoStart,
        ProxyMode = _settings.TelegramProxyMode,
        ProxyUrl = _settings.TelegramProxyMode == "custom-http" ? _settings.TelegramProxyUrl : ""
    };

    private bool TryApplyEditorSettings(bool requireAllowedUser)
    {
        if (PollingTimeout is < 10 or > 60)
        {
            OperationMessage = "消息接收等待时间必须在 10 到 60 秒之间。";
            return false;
        }

        var values = new List<long>();
        var seen = new HashSet<long>();
        var lines = AllowedUserIdsText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        for (var index = 0; index < lines.Length; index++)
        {
            var value = lines[index].Trim();
            if (value.Length == 0) continue;
            if (value.Any(character => character is < '0' or > '9') ||
                !long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            {
                OperationMessage = $"允许用户 ID 的第 {index + 1} 行无效：请输入正整数。";
                return false;
            }
            if (seen.Add(parsed)) values.Add(parsed);
        }
        if (requireAllowedUser && values.Count == 0)
        {
            OperationMessage = "启动前至少需要一个合法的 Telegram 用户 ID。";
            return false;
        }
        if (!TryBuildProxySettings(out var proxyMode, out var proxyUrl)) return false;

        _settings.TelegramAllowedUserIds = values;
        _settings.TelegramPollingTimeoutSeconds = PollingTimeout;
        _settings.TelegramSendProgressUpdates = SendProgressUpdates;
        _settings.TelegramAutoStart = AutoStart;
        _settings.TelegramProxyMode = proxyMode;
        _settings.TelegramProxyUrl = proxyMode == "custom-http" ? proxyUrl : "";
        AllowedUserIdsText = string.Join(Environment.NewLine, values);
        return true;
    }

    private bool TryBuildProxySettings(out string proxyMode, out string proxyUrl)
    {
        proxyMode = NormalizeProxyMode(ProxyMode);
        proxyUrl = "";
        if (proxyMode != "custom-http") return true;

        var candidate = (ProxyUrl ?? "").Trim();
        if (!Uri.TryCreate(candidate, UriKind.Absolute, out var uri) ||
            !string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(uri.Host) ||
            candidate.Contains('@') ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment) ||
            (!string.IsNullOrEmpty(uri.AbsolutePath) && uri.AbsolutePath != "/"))
        {
            OperationMessage = "自定义代理地址必须是仅含主机和可选端口的 http 绝对 URL，例如 http://127.0.0.1:7897；不能包含账号、查询、片段或非根路径。";
            return false;
        }

        proxyUrl = candidate.TrimEnd('/');
        ProxyUrl = proxyUrl;
        return true;
    }

    private void ApplyStatus(TelegramChannelStatus status)
    {
        Configured = status.Configured || status.TokenSet;
        Running = status.Running;
        Connected = status.Connected ?? (status.Running && string.Equals(status.State, "running", StringComparison.OrdinalIgnoreCase));
        BotName = EmptyAsDash(status.BotDisplayName);
        BotUsername = string.IsNullOrWhiteSpace(status.BotUsername) ? "—" : $"@{status.BotUsername.TrimStart('@')}";
        TokenSummary = !string.IsNullOrWhiteSpace(status.TokenSummary)
            ? status.TokenSummary
            : !string.IsNullOrWhiteSpace(status.TokenFingerprint)
                ? $"已安全配置 · {status.TokenFingerprint}"
                : Configured ? "已安全配置" : "未保存";
        PollingState = UiText.Status(string.IsNullOrWhiteSpace(status.PollingState) ? status.State : status.PollingState);
        LastUpdateAt = UiText.LocalDateTime(status.LastUpdateAt);
        LastError = string.IsNullOrWhiteSpace(status.LastError) ? "" : UiText.UserError(status.LastError, "Telegram");
        StartedAt = UiText.LocalDateTime(status.StartedAt);
        ConfiguredProxyMode = FormatProxyMode(status.ProxyMode);
        MaskedProxyAddress = EmptyAsDash(status.MaskedProxyAddress);
        EffectiveProxyMode = FormatProxyMode(status.EffectiveProxyMode);
        LastNetworkStage = FormatNetworkStage(status.LastNetworkStage);
        LastRequestDuration = status.LastRequestDurationMs > 0 ? $"{status.LastRequestDurationMs} ms" : "—";
        LastErrorCategory = string.IsNullOrWhiteSpace(status.LastErrorCategory)
            ? "—"
            : DescribeProxyCategory(status.LastErrorCategory);
        AllowedUserCount = status.AllowedUserCount > 0 ? status.AllowedUserCount : status.AllowedUserIds?.Count ?? 0;
        BindingCount = status.BindingCount;
        if (status.Bindings is not null)
            ReplaceBindings(status.Bindings.Where(binding => string.Equals(binding.ChannelType, "telegram", StringComparison.OrdinalIgnoreCase)));
    }

    private async Task RefreshBindingsHandledAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var refreshTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
            refreshTimeout.CancelAfter(TimeSpan.FromSeconds(15));
            var response = await _api.GetBindingsAsync(refreshTimeout.Token).ConfigureAwait(false);
            var allBindings = response.Bindings ?? [];
            await PopulateThreadTitlesAsync(allBindings, refreshTimeout.Token).ConfigureAwait(false);
            var telegramBindings = allBindings.Where(binding =>
                string.Equals(binding.ChannelType, "telegram", StringComparison.OrdinalIgnoreCase)).ToList();
			var qqBindings = allBindings.Where(binding =>
				string.Equals(binding.ChannelType, "qqbot", StringComparison.OrdinalIgnoreCase) ||
				string.Equals(binding.ChannelType, "qq", StringComparison.OrdinalIgnoreCase)).ToList();
            await RunOnUiAsync(() =>
            {
                ReplaceBindings(telegramBindings);
                Qq.ReplaceBindings(qqBindings);
            });
			await Qq.ClearBindingLoadErrorAsync();
        }
        catch (OperationCanceledException) when (_stopped || _lifetime.IsCancellationRequested || cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
			await Qq.ReportBindingLoadErrorAsync(exception);
        }
    }

    private void ReplaceBindings(IEnumerable<ChannelBinding> bindings)
    {
        var ordered = bindings.OrderByDescending(binding => binding.CreatedAt, StringComparer.Ordinal).ToList();
        Bindings.Clear();
        foreach (var binding in ordered) Bindings.Add(binding);
        BindingCount = Bindings.Count;
    }

    private async Task PopulateThreadTitlesAsync(IEnumerable<ChannelBinding> bindings, CancellationToken cancellationToken)
    {
        var groups = bindings.Where(binding => !string.IsNullOrWhiteSpace(binding.TargetId))
			.GroupBy(binding => $"{binding.Backend}:{binding.TargetId}", StringComparer.Ordinal);
        foreach (var group in groups)
        {
            try
            {
				var first = group.First();
				if (first.IsOpenClaw)
				{
					var session = await _api.GetOpenClawSessionAsync(first.TargetId, cancellationToken);
					foreach (var binding in group) binding.ThreadTitle = session.Title;
				}
				else
				{
					var thread = await _api.GetThreadAsync(first.TargetId, cancellationToken);
					foreach (var binding in group) binding.ThreadTitle = thread.Title;
				}
            }
            catch (BridgeApiException)
            {
                // A deleted/inaccessible target is still shown by short ID.
            }
        }
    }

    private static bool TryReadStatus(JsonElement payload, out TelegramChannelStatus status)
    {
        status = new TelegramChannelStatus();
        if (payload.ValueKind != JsonValueKind.Object) return false;
        if (payload.TryGetProperty("status", out var nested) && nested.ValueKind == JsonValueKind.Object) payload = nested;
        if (payload.TryGetProperty("channelType", out var channelType) &&
            !string.Equals(channelType.GetString(), "telegram", StringComparison.OrdinalIgnoreCase)) return false;
        // Only apply complete, display-safe status payloads. Partial state events
        // are refreshed instead of overwriting known fields with DTO defaults.
        if (!payload.TryGetProperty("configured", out _) || !payload.TryGetProperty("running", out _)) return false;
        try
        {
            status = payload.Deserialize<TelegramChannelStatus>(JsonOptions) ?? new TelegramChannelStatus();
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string ReadPayloadChannelType(JsonElement payload)
    {
        if (payload.ValueKind != JsonValueKind.Object) return "";
        if (payload.TryGetProperty("status", out var nested) && nested.ValueKind == JsonValueKind.Object) payload = nested;
        if (payload.TryGetProperty("channelType", out var value) && value.ValueKind == JsonValueKind.String)
            return value.GetString() ?? "";
        if (payload.TryGetProperty("namespace", out value) && value.ValueKind == JsonValueKind.String)
            return value.GetString() ?? "";
        return "";
    }

    private static bool IsChannelNamespace(string value, string channelType) =>
        string.Equals(value, channelType, StringComparison.OrdinalIgnoreCase) ||
        value.StartsWith(channelType + ".", StringComparison.OrdinalIgnoreCase) ||
        value.StartsWith("channel." + channelType, StringComparison.OrdinalIgnoreCase);

    private void ScheduleEventRefresh()
    {
        if (_stopped || !IsContentReady || _lifetime.IsCancellationRequested) return;
        _eventRefresh?.Cancel();
        _eventRefresh?.Dispose();
        _eventRefresh = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        var cancellationToken = _eventRefresh.Token;
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(300, cancellationToken);
                await (await Application.Current.Dispatcher.InvokeAsync(() => RefreshAsync(cancellationToken, preserveOperationMessage: true)));
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (Exception exception)
            {
                _logs.AddException("desktop", "刷新远程渠道事件失败。", exception);
            }
        }, cancellationToken);
    }

    private void ReportError(string action, Exception exception)
    {
        OperationMessage = UiText.UserError(exception, action);
        _logs.Add("desktop", $"{action}失败：{LogService.Redact(exception.Message)}");
    }

    private async Task OpenLogsAsync()
    {
        try
        {
            await Task.Run(() =>
            {
                Directory.CreateDirectory(_settingsService.LogDirectory);
                Process.Start(new ProcessStartInfo
                {
                    FileName = _settingsService.LogDirectory,
                    UseShellExecute = true
                });
            });
        }
        catch (Exception exception)
        {
            await RunOnUiAsync(() => ReportError("打开日志目录", exception));
        }
    }

    private Task SetLoadStateAsync(bool isLoading, bool isReady, string error, string errorType) => RunOnUiAsync(() =>
    {
        IsLoading = isLoading;
        IsContentReady = true;
        LoadError = error;
        LoadErrorType = errorType;
    });

    private void NotifyLoadVisibility()
    {
        OnPropertyChanged(nameof(LoadingVisibility));
        OnPropertyChanged(nameof(ContentVisibility));
        OnPropertyChanged(nameof(ErrorVisibility));
    }

    private static (string Message, string Type) DescribeLoadError(Exception exception)
    {
        if (exception is TelegramSecretException)
            return ("无法读取已保存的 Telegram 机器人密钥。你可以删除后重新保存。", "DPAPI");
        if (exception is BridgeApiException apiException)
        {
            var label = apiException.StatusCode switch
            {
                HttpStatusCode.Unauthorized => "本地服务连接已失效",
                HttpStatusCode.NotFound => "当前版本不支持此设置",
                HttpStatusCode.Conflict => "当前状态暂时无法执行此操作",
                HttpStatusCode.InternalServerError => "本地服务未能完成处理",
                _ => "本地服务请求失败"
            };
            return ($"{label}，请重试。详情已写入运行日志。", "HTTP");
        }
        if (exception is TaskCanceledException or TimeoutException)
            return ("读取远程渠道设置超时，请稍后重试。", "Timeout");
        if (exception is InvalidOperationException)
            return ("本地服务尚未就绪，请稍后重试。", "Backend");
        return (UiText.UserError(exception, "读取远程渠道设置"), exception.GetType().Name);
    }

    private static Task RunOnUiAsync(Action action)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null || dispatcher.CheckAccess())
        {
            action();
            return Task.CompletedTask;
        }
        return dispatcher.InvokeAsync(action).Task;
    }

    private static string EmptyAsDash(string? value) => string.IsNullOrWhiteSpace(value) ? "—" : value;

    private static string NormalizeProxyMode(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "direct" => "direct",
        "custom-http" => "custom-http",
        _ => "environment"
    };

    private static string FormatProxyMode(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "environment" => "环境变量",
        "direct" => "直连",
        "custom-http" => "自定义 HTTP 代理",
        "no-proxy" => "直连（NO_PROXY）",
        null or "" => "—",
        _ => value!
    };

    private static string FormatNetworkStage(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "getme" => "获取 Bot 信息（getMe）",
        "getupdates" => "拉取更新（getUpdates）",
        "sendmessage" => "发送消息（sendMessage）",
        "editmessagetext" => "更新消息（editMessageText）",
        "answercallbackquery" => "响应按钮（answerCallbackQuery）",
        "proxy-connect" => "连接代理",
        "dns" => "DNS 解析",
        "connect" => "建立连接",
        "tls" => "TLS 握手",
        "request" => "发送请求",
        "response" => "接收响应",
        "complete" => "完成",
        null or "" => "—",
        _ => value!
    };

    private static string DescribeProxyCategory(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "invalid_proxy" or "invalid-proxy" or "validation" => "代理配置无效",
        "proxy-refused" => "代理连接被拒绝",
        "proxy_connect" or "proxy-connect" => "无法连接代理服务器",
        "proxy_auth" or "proxy-auth" => "代理认证失败",
        "dns" => "DNS 解析失败",
        "connect" or "network" => "网络连接失败",
        "telegram-unreachable" => "无法连接 Telegram",
        "tls" => "TLS 握手失败",
        "timeout" => "请求超时",
        "telegram_api" or "telegram-api" => "Telegram API 返回错误",
        "http_status" or "http-status" => "HTTP 状态异常",
        null or "" => "未知错误",
        _ => $"其他错误（{value}）"
    };
}
