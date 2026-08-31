using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class MainViewModel : ObservableObject
{
    private readonly DaemonProcessManager _daemon;
    private readonly BridgeApiClient _api;
    private readonly UserSettings _settings;
    private readonly SettingsService _settingsService;
    private readonly LogService _logs;
    private readonly CodexDiscoveryService _codexDiscoveryService;
    private readonly CodexDiscoveryRetryRunner _codexDiscoveryRetryRunner;
    private CodexDiscoveryResult _codexDiscovery;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _codexRetrySync = new();
    private Task? _codexRetryTask;
    private CancellationTokenSource? _codexRetryCancellation;
    private CancellationTokenSource? _eventRefresh;
    private object _currentPage;
    private string _backendState = "正在启动";
    private string _codexCliState = "检测中";
    private string _appServerState = "等待连接";
    private string _errorMessage = "";
    private bool _stopped;
    private bool _initialized;
    private bool _isCodexDiscoveryRetrying;

    public MainViewModel(DaemonProcessManager daemon, BridgeApiClient api, SessionsViewModel sessions, OpenClawViewModel openClaw,
        ChannelProfilesViewModel channelProfiles, CommandsViewModel commands, CommandsViewModel openClawCommands, OverviewViewModel overview,
        TasksViewModel tasks, ProjectsViewModel projects, MirrorViewModel mirror, BackupViewModel backup,
        SettingsViewModel settingsViewModel, LogsViewModel logsViewModel, UserSettings settings,
        SettingsService settingsService, LogService logs, CodexDiscoveryService codexDiscoveryService,
        CodexDiscoveryResult codexDiscovery)
    {
        _daemon = daemon;
        _api = api;
        Sessions = sessions;
        OpenClaw = openClaw;
        ChannelProfiles = channelProfiles;
        Commands = commands;
        OpenClawCommands = openClawCommands;
        Overview = overview;
        Tasks = tasks;
        Projects = projects;
        Mirror = mirror;
        Backup = backup;
        Settings = settingsViewModel;
        Logs = logsViewModel;
        _settings = settings;
        _settingsService = settingsService;
        _logs = logs;
        _codexDiscoveryService = codexDiscoveryService;
        _codexDiscoveryRetryRunner = new CodexDiscoveryRetryRunner(logs);
        _codexDiscovery = codexDiscovery;
        CurrentPageKey = settings.RestoreLastPage ? NormalizePage(settings.LastPage) : "overview";
        _currentPage = ResolvePage(CurrentPageKey);
        NavigateCommand = new RelayCommand(Navigate);
        RefreshCommand = new AsyncRelayCommand(RefreshAsync);
        _api.EventReceived += OnEventReceived;
        _api.EventStreamConnectionChanged += OnEventStreamConnectionChanged;
        Tasks.ConversationRequested += OnConversationRequested;
        Overview.TasksRequested += () => Navigate("tasks");
    }

    public SessionsViewModel Sessions { get; }
    public OpenClawViewModel OpenClaw { get; }
    public ChannelProfilesViewModel ChannelProfiles { get; }
    public CommandsViewModel Commands { get; }
    public CommandsViewModel OpenClawCommands { get; }
    public OverviewViewModel Overview { get; }
    public TasksViewModel Tasks { get; }
    public ProjectsViewModel Projects { get; }
    public MirrorViewModel Mirror { get; }
    public BackupViewModel Backup { get; }
    public SettingsViewModel Settings { get; }
    public LogsViewModel Logs { get; }
    public ICommand NavigateCommand { get; }
    public ICommand RefreshCommand { get; }
    public string CurrentPageKey { get; private set; }
    public string PageTitle => CurrentPageKey switch { "tasks" => "任务中心", "projects" => "项目工作区", "sessions" => "Codex", "openclaw" => "OpenClaw", "qq" => "QQ", "telegram" => "Telegram", "commands" => "Codex 指令", "openclaw-commands" => "OpenClaw 指令", "mirror" => "消息同步", "backup" => "备份与恢复", "settings" => "设置", "logs" => "运行日志", _ => "概览" };
    public string PageDescription => CurrentPageKey switch { "tasks" => "统一查看、创建、继续、重试和取消 Codex / OpenClaw 任务", "projects" => "配置工作目录、默认后端、会话分配和项目级复用策略", "sessions" => "只显示 Codex Thread 与 Codex 消息历史", "openclaw" => "独立查看 OpenClaw Gateway、Session 与消息历史", "qq" => "管理 QQ Bot Profile、Gateway 连接与后端分配", "telegram" => "管理 Telegram Profile、Long Polling 与后端分配", "commands" => "只管理 Codex 路由可用的 QQ / Telegram 远程指令", "openclaw-commands" => "只管理 OpenClaw 路由可用的 QQ / Telegram 远程指令", "mirror" => "将 Codex 的最终回答发送到指定渠道", "backup" => "备份或恢复 Codex 与应用数据", "settings" => "管理启动、窗口、外观、OpenClaw 与消息渠道", "logs" => "查看本机运行信息和错误详情", _ => "查看 Codex、OpenClaw、远程渠道与消息同步状态" };
    public bool IsOverviewPage => CurrentPageKey == "overview";
    public bool IsTasksPage => CurrentPageKey == "tasks";
    public bool IsProjectsPage => CurrentPageKey == "projects";
    public bool IsSessionsPage => CurrentPageKey == "sessions";
    public bool IsOpenClawPage => CurrentPageKey == "openclaw";
    public bool IsQqPage => CurrentPageKey == "qq";
    public bool IsTelegramPage => CurrentPageKey == "telegram";
    public bool IsCommandsPage => CurrentPageKey == "commands";
    public bool IsOpenClawCommandsPage => CurrentPageKey == "openclaw-commands";
    public bool IsMirrorPage => CurrentPageKey == "mirror";
    public bool IsBackupPage => CurrentPageKey == "backup";
    public bool IsSettingsPage => CurrentPageKey == "settings";
    public bool IsLogsPage => CurrentPageKey == "logs";

    public object CurrentPage { get => _currentPage; private set => SetProperty(ref _currentPage, value); }
    public string BackendState { get => _backendState; private set => SetProperty(ref _backendState, value); }
    public string CodexCliState { get => _codexCliState; private set => SetProperty(ref _codexCliState, value); }
    public string AppServerState { get => _appServerState; private set => SetProperty(ref _appServerState, value); }
    public string ErrorMessage { get => _errorMessage; private set { if (SetProperty(ref _errorMessage, value)) OnPropertyChanged(nameof(ErrorVisibility)); } }
    public bool IsCodexDiscoveryRetrying { get => _isCodexDiscoveryRetrying; private set => SetProperty(ref _isCodexDiscoveryRetrying, value); }
    public Visibility ErrorVisibility => string.IsNullOrWhiteSpace(ErrorMessage) ? Visibility.Collapsed : Visibility.Visible;

    public async Task InitializeAsync()
    {
        try
        {
            var startupCodexPath = _codexDiscovery.Found ? _codexDiscovery.Path : "";
            var ready = await _daemon.StartAsync(_settings, startupCodexPath, _lifetime.Token);
            _api.Connect(new Uri(ready.Address), _daemon.Token);
            _api.StartEventStream();
            if (!_codexDiscovery.Found)
            {
                _codexDiscovery = await DiscoverInBackgroundAsync(_lifetime.Token);
                if (_codexDiscovery.Found)
                {
                    Settings.UpdateDiscovery(_codexDiscovery);
                    _logs.Add("codex-config", $"[codex-config] runtime path updated path={_codexDiscovery.Path} target=desktop-settings");
                    await RememberAutomaticDiscoveryAsync(_codexDiscovery);
                }
            }
            if (_codexDiscovery.Found)
            {
                _logs.Add("codex-daemon", $"[codex-daemon] applying new Codex path path={_codexDiscovery.Path}");
                var applied = await _api.ApplyCodexPathAsync(_codexDiscovery.Path, _codexDiscovery.RuntimeSource, _lifetime.Token);
                Settings.UpdateRuntimeStatus(applied, BackendState);
            }
            BackendState = "运行中";
            if (!_codexDiscovery.Found) EnsureCodexDiscoveryRetryStarted();
            await RefreshAsync();
            await InitializeRemoteChannelsAsync(forceRetry: false);
            await Settings.InitializeOpenClawAsync(_lifetime.Token);
            await OpenClaw.RefreshAsync(_lifetime.Token);
            if (CurrentPageKey == "commands") await Commands.EnsureInitializedAsync(_lifetime.Token);
            if (CurrentPageKey == "openclaw-commands") await OpenClawCommands.EnsureInitializedAsync(_lifetime.Token);
            _initialized = true;
        }
        catch (Exception exception)
        {
            BackendState = "启动失败";
            CodexCliState = "无法检测";
            AppServerState = "未运行";
            Overview.CodexState = "启动失败";
            ErrorMessage = UiText.UserError(exception, "启动");
            _logs.Add("desktop", exception.Message);
        }
    }

    private void Navigate(object? parameter)
    {
        CurrentPageKey = NormalizePage(parameter?.ToString());
        CurrentPage = ResolvePage(CurrentPageKey);
        OnPropertyChanged(nameof(CurrentPageKey));
        OnPropertyChanged(nameof(PageTitle));
        OnPropertyChanged(nameof(PageDescription));
        OnPropertyChanged(nameof(IsOverviewPage)); OnPropertyChanged(nameof(IsTasksPage)); OnPropertyChanged(nameof(IsProjectsPage)); OnPropertyChanged(nameof(IsSessionsPage)); OnPropertyChanged(nameof(IsOpenClawPage)); OnPropertyChanged(nameof(IsQqPage)); OnPropertyChanged(nameof(IsTelegramPage)); OnPropertyChanged(nameof(IsCommandsPage)); OnPropertyChanged(nameof(IsOpenClawCommandsPage));
        OnPropertyChanged(nameof(IsMirrorPage)); OnPropertyChanged(nameof(IsBackupPage)); OnPropertyChanged(nameof(IsSettingsPage)); OnPropertyChanged(nameof(IsLogsPage));
        _settings.LastPage = CurrentPageKey;
        _ = _settingsService.SaveAsync(_settings);
        if (ReferenceEquals(CurrentPage, Commands)) _ = Commands.EnsureInitializedAsync(_lifetime.Token);
        if (ReferenceEquals(CurrentPage, OpenClawCommands)) _ = OpenClawCommands.EnsureInitializedAsync(_lifetime.Token);
        if (ReferenceEquals(CurrentPage, Mirror)) _ = Settings.RefreshMirrorAsync();
    }

    private void OnConversationRequested(string backend, int number)
    {
        if (string.Equals(backend, "openclaw", StringComparison.OrdinalIgnoreCase))
        {
            Navigate("openclaw");
            QueueUiTask(() => OpenClaw.SelectSessionByNumberAsync(number, _lifetime.Token));
            return;
        }
        Navigate("sessions");
        QueueUiTask(() => Sessions.SelectThreadByNumberAsync(number, _lifetime.Token));
    }

    private object ResolvePage(string key) => key switch
    {
        "tasks" => Tasks,
        "projects" => Projects,
        "sessions" => Sessions,
        "openclaw" => OpenClaw,
        "qq" => SelectChannelPage("qqbot"),
        "telegram" => SelectChannelPage("telegram"),
        "commands" => Commands, "openclaw-commands" => OpenClawCommands, "mirror" => Mirror, "backup" => Backup,
        "settings" => Settings, "logs" => Logs, _ => Overview
    };
    private object SelectChannelPage(string platform) { ChannelProfiles.SelectPlatform(platform); return ChannelProfiles; }
    private static string NormalizePage(string? key) => key == "channels" ? "telegram" : key is "overview" or "tasks" or "projects" or "sessions" or "openclaw" or "qq" or "telegram" or "commands" or "openclaw-commands" or "mirror" or "backup" or "settings" or "logs" ? key : "overview";

    public async Task RefreshAsync()
    {
        try
        {
            var status = await _api.GetStatusAsync(_lifetime.Token);
            BackendState = $"运行中 · v{status.Version}";
            CodexCliState = status.CodexCliAvailable ? $"已找到 · {status.CodexCliPath}" : "未找到";
            AppServerState = status.AppServerRunning ? "已连接" : "未连接";
            Overview.CodexState = status.AppServerRunning ? "已连接" : status.CodexCliAvailable ? "CLI 已就绪" : "未连接";
            Settings.UpdateRuntimeStatus(status, BackendState);
            if (!status.CodexCliAvailable && !_codexDiscovery.Found) EnsureCodexDiscoveryRetryStarted();
            if (IsCodexDiscoveryRetrying && !status.CodexCliAvailable)
            {
                CodexCliState = "正在后台检测";
                Overview.CodexState = "正在检测";
                Settings.UpdateDiscoveryRetrying(true);
                ErrorMessage = UiText.CodexDiscoveryRetrying;
            }
            else
            {
                ErrorMessage = string.IsNullOrWhiteSpace(status.LastError) ? "" : UiText.UserError(status.LastError, "连接");
            }
			await Sessions.RefreshAsync(_lifetime.Token);
			await OpenClaw.RefreshAsync(_lifetime.Token);
			await Tasks.RefreshAsync();
			await Projects.RefreshAsync();
			await Settings.RefreshOpenClawAsync(_lifetime.Token);
        }
        catch (Exception exception)
        {
            ErrorMessage = UiText.UserError(exception, "刷新");
            _logs.Add("desktop", $"刷新失败：{exception.Message}");
        }
    }

    public async Task PauseRuntimeAsync()
    {
        await CancelCodexDiscoveryRetryAsync();
        await _daemon.StopAsync();
        BackendState = "已暂停以恢复数据";
        AppServerState = "已停止";
    }

    public async Task ResumeRuntimeAsync()
    {
        var restored = await _settingsService.LoadAsync();
        foreach (var property in typeof(UserSettings).GetProperties().Where(property => property.CanRead && property.CanWrite))
            property.SetValue(_settings, property.GetValue(restored));
        Settings.ReloadUserPreferences(_settings);
        _codexDiscovery = await DiscoverInBackgroundAsync(_lifetime.Token);
        Settings.UpdateDiscovery(_codexDiscovery);
        var effectiveCodexPath = _codexDiscovery.Found ? _codexDiscovery.Path : "";
        var ready = await _daemon.StartAsync(_settings, effectiveCodexPath, _lifetime.Token);
        _api.Connect(new Uri(ready.Address), _daemon.Token);
        _api.StartEventStream();
        if (_codexDiscovery.Found)
        {
            await _api.ApplyCodexPathAsync(_codexDiscovery.Path, _codexDiscovery.RuntimeSource, _lifetime.Token);
            await RememberAutomaticDiscoveryAsync(_codexDiscovery);
        }
        else
        {
            EnsureCodexDiscoveryRetryStarted();
        }
        await RefreshAsync();
        await InitializeRemoteChannelsAsync(forceRetry: true);
        await Settings.InitializeOpenClawAsync(_lifetime.Token);
        await OpenClaw.RefreshAsync(_lifetime.Token);
        await Commands.RefreshAsync(_lifetime.Token);
        await OpenClawCommands.RefreshAsync(_lifetime.Token);
    }

    private async Task InitializeRemoteChannelsAsync(bool forceRetry)
    {
        // Restore profile metadata and DPAPI credentials after the daemon is
        // connected. The daemon deduplicates shared physical transports before
        // any poller or QQ Gateway is started.
        await ChannelProfiles.EnsureInitializedAsync(_lifetime.Token, forceRetry).ConfigureAwait(false);
        await Settings.InitializeMirrorAsync(_settings.MirrorAutoStart, _lifetime.Token).ConfigureAwait(false);
    }

    private void OnEventReceived(object? sender, BridgeEvent bridgeEvent)
    {
        QueueUiAction(() => Sessions.ApplyEvent(bridgeEvent));
        QueueUiAction(() => OpenClaw.ApplyEvent(bridgeEvent));
        QueueUiAction(() => Tasks.ApplyEvent(bridgeEvent));
        QueueUiAction(() => Projects.ApplyEvent(bridgeEvent));
        if (bridgeEvent.EventType.StartsWith("channel.", StringComparison.OrdinalIgnoreCase) || bridgeEvent.EventType.StartsWith("binding.", StringComparison.OrdinalIgnoreCase) || bridgeEvent.EventType.StartsWith("telegram.", StringComparison.OrdinalIgnoreCase) || bridgeEvent.EventType.StartsWith("qq", StringComparison.OrdinalIgnoreCase))
            QueueUiAction(() => ChannelProfiles.ApplyEvent(bridgeEvent));
        if (bridgeEvent.EventType is "codex.connected" or "codex.disconnected" or "codex.config_updated" or "openclaw.connected" or "openclaw.disconnected" or "openclaw.reconnecting" or "openclaw.session.updated" or "error") { QueueUiTask(RefreshAsync); return; }
        if (bridgeEvent.EventType != "thread.updated") return;
        _eventRefresh?.Cancel(); _eventRefresh?.Dispose();
        _eventRefresh = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        var token = _eventRefresh.Token;
        _ = Task.Run(async () =>
        {
            try { await Task.Delay(1200, token); await RunUiTaskAsync(() => Sessions.RefreshAsync(token, reloadSelected: false)); }
            catch (OperationCanceledException) { } catch (ObjectDisposedException) { }
            catch (Exception exception) { _logs.AddException("desktop", "刷新 Codex 会话事件失败。", exception); }
        }, token);
    }

    private void OnEventStreamConnectionChanged(object? sender, bool connected)
    {
        if (!connected || !_initialized) return;
        QueueUiTask(RefreshAsync);
        QueueUiTask(() => ChannelProfiles.RefreshAsync(_lifetime.Token, preserveStatus: true));
    }
    private void QueueUiAction(Action action) => _ = RunUiActionAsync(action);
    private void QueueUiTask(Func<Task> action) => _ = RunUiTaskAsync(action);
    private async Task RunUiActionAsync(Action action)
    {
        try { await Application.Current.Dispatcher.InvokeAsync(action); }
        catch (Exception exception) when (_stopped && exception is (TaskCanceledException or OperationCanceledException or ObjectDisposedException)) { }
        catch (Exception exception) { _logs.AddException("desktop", "UI 状态更新失败。", exception); }
    }
    private async Task RunUiTaskAsync(Func<Task> action)
    {
        try { await (await Application.Current.Dispatcher.InvokeAsync(action)); }
        catch (Exception exception) when (_stopped && exception is (TaskCanceledException or OperationCanceledException or ObjectDisposedException)) { }
        catch (Exception exception) { _logs.AddException("desktop", "UI 异步刷新失败。", exception); }
    }
    public void ReportRecoverableUiException(Exception exception) => ErrorMessage = UiText.UserError(exception);

    private Task<CodexDiscoveryResult> DiscoverInBackgroundAsync(CancellationToken cancellationToken)
    {
        var customPath = _settings.CodexCustomPath;
        var detectedPath = _settings.DetectedCodexPath;
        return Task.Run(() => _codexDiscoveryService.DiscoverAsync(customPath, detectedPath, cancellationToken), cancellationToken);
    }

    private void EnsureCodexDiscoveryRetryStarted()
    {
        lock (_codexRetrySync)
        {
            if (_stopped || _lifetime.IsCancellationRequested || _codexDiscovery.Found ||
                _codexRetryTask is { IsCompleted: false }) return;

            IsCodexDiscoveryRetrying = true;
            Settings.UpdateDiscoveryRetrying(true);
            ErrorMessage = UiText.CodexDiscoveryRetrying;
            _codexRetryCancellation?.Dispose();
            _codexRetryCancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
            _codexRetryTask = RunCodexDiscoveryRetryAsync(_codexRetryCancellation.Token);
        }
    }

    private async Task RunCodexDiscoveryRetryAsync(CancellationToken cancellationToken)
    {
        var recovered = false;
        try
        {
            recovered = await _codexDiscoveryRetryRunner.RunAsync(
                DiscoverInBackgroundAsync,
                (discovery, token) => InvokeOnUiAsync(() => ApplyRecoveredDiscoveryAsync(discovery, token)),
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logs.Add("codex-discovery", "[codex-discovery] retry cancelled");
        }
        catch (Exception exception)
        {
            _logs.AddException("codex-discovery", "Codex 后台检测任务异常。", exception);
        }
        finally
        {
            try
            {
                await InvokeOnUiAsync(() =>
                {
                    IsCodexDiscoveryRetrying = false;
                    if (!recovered && !_stopped && !_codexDiscovery.Found)
                    {
                        Settings.UpdateDiscovery(new CodexDiscoveryResult(false, "", "", CodexDiscoverySource.None));
                        ErrorMessage = UiText.CodexDiscoveryNotFound;
                    }
                    return Task.CompletedTask;
                }).ConfigureAwait(false);
            }
            catch (Exception exception) when (_stopped && exception is (TaskCanceledException or OperationCanceledException or ObjectDisposedException)) { }
        }
    }

    private async Task ApplyRecoveredDiscoveryAsync(CodexDiscoveryResult discovery, CancellationToken cancellationToken)
    {
        _logs.Add("codex-daemon", $"[codex-daemon] applying new Codex path path={discovery.Path}");
        var applied = await _api.ApplyCodexPathAsync(discovery.Path, discovery.RuntimeSource, cancellationToken);
        _codexDiscovery = discovery;
        Settings.UpdateDiscovery(discovery);
        Settings.UpdateRuntimeStatus(applied, BackendState);
        ErrorMessage = "";
        await RememberAutomaticDiscoveryAsync(discovery);
        await RefreshAsync();
    }

    private async Task RememberAutomaticDiscoveryAsync(CodexDiscoveryResult discovery)
    {
        if (!CodexPathSettings.RememberAutomaticDiscovery(_settings, discovery)) return;
        try
        {
            await _settingsService.SaveAsync(_settings);
            _logs.Add("codex-config", $"[codex-config] persisted detected path={_settings.DetectedCodexPath}");
        }
        catch (Exception exception)
        {
            _logs.AddException("codex-config", "持久化自动发现的 Codex 路径失败；仍将应用到当前运行时。", exception);
        }
    }

    private static async Task InvokeOnUiAsync(Func<Task> action)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher is null || dispatcher.CheckAccess())
        {
            await action();
            return;
        }
        await await dispatcher.InvokeAsync(action);
    }

    private async Task CancelCodexDiscoveryRetryAsync()
    {
        Task? retryTask;
        lock (_codexRetrySync)
        {
            _codexRetryCancellation?.Cancel();
            retryTask = _codexRetryTask;
        }
        if (retryTask is null) return;
        try { await retryTask.ConfigureAwait(false); }
        catch (OperationCanceledException) { }
    }

    public async Task StopAsync()
    {
        if (_stopped) return;
        _stopped = true;
        _eventRefresh?.Cancel(); _eventRefresh?.Dispose();
        _api.EventReceived -= OnEventReceived; _api.EventStreamConnectionChanged -= OnEventStreamConnectionChanged;
        Tasks.ConversationRequested -= OnConversationRequested;
        _lifetime.Cancel();
        await CancelCodexDiscoveryRetryAsync().ConfigureAwait(false);
        _codexRetryCancellation?.Dispose();
        _api.Dispose(); await _daemon.StopAsync().ConfigureAwait(false); _lifetime.Dispose();
    }
}
