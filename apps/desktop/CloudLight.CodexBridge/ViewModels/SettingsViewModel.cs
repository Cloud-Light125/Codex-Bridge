using System.Diagnostics;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class SettingsViewModel : ObservableObject
{
    private readonly SettingsService _service;
    private readonly BridgeApiClient _api;
    private readonly UserSettings _settings;
    private readonly LogService _logs;
    private readonly StartupService _startup;
    private readonly CodexDiscoveryService _codexDiscoveryService;
	private readonly OpenClawSecretService _openClawSecrets;
	private readonly OpenClawDiscoveryService _openClawDiscoveryService;
	private readonly SemaphoreSlim _mirrorOperationLock = new(1, 1);
    private string _codexCustomPath;
    private string _sandboxMode;
    private string _runtimeCodexPath = "";
    private string _codexPathSource = "";
    private string _codexValidationStatus = "pending";
    private string _codexConnectionStatus = "waiting";
    private string _codexDetection = "等待后端检测";
    private string _backendStatus = "正在启动";
    private string _saveResult = "";
    private string _approvalPolicy = "on-request";
	private string _openClawGatewayUrl;
	private string _openClawToken = "";
	private string _openClawPassword = "";
	private string _openClawStatusText = "未配置";
	private string _openClawDiscoverySource = "";
	private bool _openClawCredentialsConfigured;
	private bool _openClawAutoDiscover;
	private bool _openClawAutoReconnect;
	private bool _mirrorEnabled;
	private bool _telegramMirrorEnabled;
	private string _telegramMirrorChatId = "";
	private bool _qqMirrorEnabled;
	private string _qqMirrorConversationType = "c2c";
	private string _qqMirrorOpenId = "";
	private bool _mirrorAssistant = true, _mirrorInput = true, _mirrorError = true;
	private bool _requireThreadNumber = true;
	private string _mirrorStatusText = "等待读取";
	private string _qqCapabilityNotice = "QQ 主动消息受平台权限、回复窗口和发送额度限制。";
    private bool _startWithWindows, _silentStartup, _closeToTray, _restoreLastPage, _autoRefreshThreads, _mirrorAutoStart;
    private int _threadRefreshIntervalSeconds;
    private string _theme;

    public SettingsViewModel(SettingsService service, BridgeApiClient api, UserSettings settings, LogService logs,
        StartupService startup, CodexDiscoveryService codexDiscoveryService,
		OpenClawSecretService? openClawSecrets = null, OpenClawDiscoveryService? openClawDiscoveryService = null)
    {
        _service = service;
        _api = api;
        _settings = settings;
        _logs = logs;
        _startup = startup;
        _codexDiscoveryService = codexDiscoveryService;
		_openClawSecrets = openClawSecrets ?? new OpenClawSecretService();
		_openClawDiscoveryService = openClawDiscoveryService ?? new OpenClawDiscoveryService(logs);
        _codexCustomPath = settings.CodexCustomPath;
        _sandboxMode = settings.SandboxMode is "read-only" ? "read-only" : "workspace-write";
        _startWithWindows = startup.IsEnabled;
        _silentStartup = settings.SilentStartup;
        _closeToTray = settings.CloseToTray;
        _restoreLastPage = settings.RestoreLastPage;
        _autoRefreshThreads = settings.AutoRefreshThreads;
        _threadRefreshIntervalSeconds = settings.ThreadRefreshIntervalSeconds;
        _theme = settings.Theme;
        _mirrorAutoStart = settings.MirrorAutoStart;
		_openClawGatewayUrl = settings.OpenClawGatewayUrl;
		_openClawAutoDiscover = settings.OpenClawAutoDiscover;
		_openClawAutoReconnect = settings.OpenClawAutoReconnect;
        SaveCommand = new AsyncRelayCommand(SaveAsync);
        OpenDataDirectoryCommand = new RelayCommand(_ => OpenDirectory(DataDirectory));
        OpenLogDirectoryCommand = new RelayCommand(_ => OpenDirectory(LogDirectory));
		DiscoverOpenClawCommand = new AsyncRelayCommand(DiscoverOpenClawAsync);
		TestOpenClawCommand = new AsyncRelayCommand(TestOpenClawAsync);
		ClearOpenClawCredentialsCommand = new AsyncRelayCommand(ClearOpenClawCredentialsAsync);
    }

    public ICommand SaveCommand { get; }
    public ICommand OpenDataDirectoryCommand { get; }
    public ICommand OpenLogDirectoryCommand { get; }
	public ICommand DiscoverOpenClawCommand { get; }
	public ICommand TestOpenClawCommand { get; }
	public ICommand ClearOpenClawCredentialsCommand { get; }
    public IReadOnlyList<string> SandboxModes { get; } = ["workspace-write", "read-only"];
    public string DataDirectory => _service.DataDirectory;
    public string LogDirectory => _service.LogDirectory;
    public IReadOnlyList<string> Themes { get; } = ["system", "light", "dark"];
    public IReadOnlyList<int> ThreadRefreshIntervals { get; } = [10, 15, 30, 60, 120, 300];
    public bool StartWithWindows { get => _startWithWindows; set => SetProperty(ref _startWithWindows, value); }
    public bool SilentStartup { get => _silentStartup; set => SetProperty(ref _silentStartup, value); }
    public bool CloseToTray { get => _closeToTray; set => SetProperty(ref _closeToTray, value); }
    public bool RestoreLastPage { get => _restoreLastPage; set => SetProperty(ref _restoreLastPage, value); }
    public bool AutoRefreshThreads { get => _autoRefreshThreads; set => SetProperty(ref _autoRefreshThreads, value); }
    public int ThreadRefreshIntervalSeconds { get => _threadRefreshIntervalSeconds; set => SetProperty(ref _threadRefreshIntervalSeconds, value); }
    public string Theme { get => _theme; set { if (SetProperty(ref _theme, value)) App.ApplyTheme(value); } }
    public bool MirrorAutoStart { get => _mirrorAutoStart; set => SetProperty(ref _mirrorAutoStart, value); }
    public bool TelegramAutoStart { get => _settings.TelegramAutoStart; set { _settings.TelegramAutoStart = value; OnPropertyChanged(); } }
    public bool QqAutoStart { get => _settings.QqAutoStart; set { _settings.QqAutoStart = value; OnPropertyChanged(); } }
	public string OpenClawGatewayUrl { get => _openClawGatewayUrl; set => SetProperty(ref _openClawGatewayUrl, value); }
	public bool OpenClawAutoDiscover { get => _openClawAutoDiscover; set => SetProperty(ref _openClawAutoDiscover, value); }
	public bool OpenClawAutoReconnect { get => _openClawAutoReconnect; set => SetProperty(ref _openClawAutoReconnect, value); }
	public string OpenClawStatusText { get => _openClawStatusText; private set => SetProperty(ref _openClawStatusText, value); }
	public string OpenClawDiscoverySource { get => _openClawDiscoverySource; private set => SetProperty(ref _openClawDiscoverySource, value); }
	public string OpenClawCredentialSummary => _openClawCredentialsConfigured || !string.IsNullOrWhiteSpace(_openClawToken) || !string.IsNullOrWhiteSpace(_openClawPassword) ? "已配置（Bridge 使用 DPAPI 保存）" : "未配置 Token/Password";

	public void SetOpenClawToken(string value)
	{
		_openClawToken = value?.Trim() ?? "";
		_openClawCredentialsConfigured = _openClawToken.Length > 0 || _openClawPassword.Length > 0;
		OnPropertyChanged(nameof(OpenClawCredentialSummary));
	}

	public void SetOpenClawPassword(string value)
	{
		_openClawPassword = value?.Trim() ?? "";
		_openClawCredentialsConfigured = _openClawToken.Length > 0 || _openClawPassword.Length > 0;
		OnPropertyChanged(nameof(OpenClawCredentialSummary));
	}

    public void ReloadUserPreferences(UserSettings settings)
    {
        CodexCustomPath = settings.CodexCustomPath;
        SandboxMode = settings.SandboxMode;
        StartWithWindows = settings.StartWithWindows;
        SilentStartup = settings.SilentStartup;
        CloseToTray = settings.CloseToTray;
        RestoreLastPage = settings.RestoreLastPage;
        AutoRefreshThreads = settings.AutoRefreshThreads;
        ThreadRefreshIntervalSeconds = settings.ThreadRefreshIntervalSeconds;
        Theme = settings.Theme;
        MirrorAutoStart = settings.MirrorAutoStart;
		OpenClawGatewayUrl = settings.OpenClawGatewayUrl;
		OpenClawAutoDiscover = settings.OpenClawAutoDiscover;
		OpenClawAutoReconnect = settings.OpenClawAutoReconnect;
        OnPropertyChanged(nameof(TelegramAutoStart));
        OnPropertyChanged(nameof(QqAutoStart));
    }

    public string CodexCustomPath
    {
        get => _codexCustomPath;
        set => SetProperty(ref _codexCustomPath, value);
    }

    public string SandboxMode
    {
        get => _sandboxMode;
        set => SetProperty(ref _sandboxMode, value);
    }

    public string CodexDetection
    {
        get => _codexDetection;
        private set => SetProperty(ref _codexDetection, value);
    }

    public string CodexPathSource { get => _codexPathSource; private set => SetProperty(ref _codexPathSource, value); }
    public string CodexValidationStatus { get => _codexValidationStatus; private set => SetProperty(ref _codexValidationStatus, value); }
    public string CodexConnectionStatus { get => _codexConnectionStatus; private set => SetProperty(ref _codexConnectionStatus, value); }
    public string CodexCliPath { get => _runtimeCodexPath; private set => SetProperty(ref _runtimeCodexPath, value); }

    public void UpdateDiscovery(CodexDiscoveryResult discovery)
    {
        if (discovery.Found)
        {
            CodexCliPath = discovery.Path;
            CodexPathSource = discovery.RuntimeSource;
            CodexValidationStatus = "succeeded";
            CodexConnectionStatus = "waiting";
        }
        else
        {
            CodexCliPath = "";
            CodexPathSource = "";
            CodexValidationStatus = "failed";
            CodexConnectionStatus = "not-found";
        }
        RebuildCodexDetection();
    }

    public void UpdateDiscoveryRetrying(bool retrying)
    {
        if (!retrying || !string.IsNullOrWhiteSpace(CodexCliPath)) return;
        CodexValidationStatus = "pending";
        CodexConnectionStatus = "reconnecting";
        CodexDetection = "暂未找到 Codex，正在后台重新检测。";
    }

	public async Task InitializeOpenClawAsync(CancellationToken cancellationToken = default)
	{
		try
		{
			var credentials = await _openClawSecrets.LoadAsync(cancellationToken).ConfigureAwait(false);
			_openClawToken = credentials.Token;
			_openClawPassword = credentials.Password;
			_openClawCredentialsConfigured = _openClawToken.Length > 0 || _openClawPassword.Length > 0;
			await DiscoverOpenClawCoreAsync(onlyIfNeeded: true, cancellationToken).ConfigureAwait(false);
			if (!_openClawCredentialsConfigured)
			{
				await RunOnUiAsync(() => { OpenClawStatusText = "未配置 Token/Password"; OnPropertyChanged(nameof(OpenClawCredentialSummary)); }).ConfigureAwait(false);
				return;
			}
			var status = await _api.ConfigureOpenClawAsync(new OpenClawConfigureRequest
			{
				GatewayUrl = OpenClawGatewayUrl, Token = _openClawToken, Password = _openClawPassword,
				AutoReconnect = OpenClawAutoReconnect, Start = true
			}, cancellationToken).ConfigureAwait(false);
			await RunOnUiAsync(() => ApplyOpenClawStatus(status)).ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
		catch (Exception exception)
		{
			_logs.AddException("openclaw", "初始化 OpenClaw Gateway 失败。", exception);
			await RunOnUiAsync(() => OpenClawStatusText = UiText.UserError(exception, "OpenClaw 连接")).ConfigureAwait(false);
		}
	}

	public async Task RefreshOpenClawAsync(CancellationToken cancellationToken = default)
	{
		try
		{
			var status = await _api.GetOpenClawStatusAsync(cancellationToken).ConfigureAwait(false);
			await RunOnUiAsync(() => ApplyOpenClawStatus(status)).ConfigureAwait(false);
		}
		catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
		catch (Exception exception)
		{
			_logs.Add("openclaw", $"读取 OpenClaw 状态失败：{exception.Message}");
		}
	}

	private async Task DiscoverOpenClawAsync()
	{
		try
		{
			await DiscoverOpenClawCoreAsync(onlyIfNeeded: false, CancellationToken.None).ConfigureAwait(false);
			SaveResult = OpenClawDiscoverySource.Length == 0 ? "未发现 OpenClawTray Gateway。" : $"已发现 {OpenClawGatewayUrl}（{OpenClawDiscoverySource}）。";
		}
		catch (Exception exception)
		{
			SaveResult = UiText.UserError(exception, "自动发现 OpenClaw");
			_logs.AddException("openclaw", "自动发现 OpenClaw Gateway 失败。", exception);
		}
	}

	private async Task DiscoverOpenClawCoreAsync(bool onlyIfNeeded, CancellationToken cancellationToken)
	{
		if (onlyIfNeeded && !OpenClawAutoDiscover && !string.IsNullOrWhiteSpace(OpenClawGatewayUrl)) return;
		var discovery = await _openClawDiscoveryService.DiscoverAsync(cancellationToken).ConfigureAwait(false);
		if (!discovery.Found) return;
		await RunOnUiAsync(() =>
		{
			if (!onlyIfNeeded || string.IsNullOrWhiteSpace(OpenClawGatewayUrl) || OpenClawGatewayUrl == "ws://127.0.0.1:18789")
				OpenClawGatewayUrl = discovery.GatewayUrl;
			if (string.IsNullOrWhiteSpace(_openClawToken) && string.IsNullOrWhiteSpace(_openClawPassword))
			{
				_openClawToken = discovery.Token;
				_openClawCredentialsConfigured = true;
			}
			OpenClawDiscoverySource = string.IsNullOrWhiteSpace(discovery.FriendlyName) ? discovery.Source : $"{discovery.Source} · {discovery.FriendlyName}";
			OnPropertyChanged(nameof(OpenClawCredentialSummary));
		}).ConfigureAwait(false);
	}

	private async Task TestOpenClawAsync()
	{
		try
		{
			await DiscoverOpenClawCoreAsync(onlyIfNeeded: true, CancellationToken.None).ConfigureAwait(false);
			var status = await _api.TestOpenClawAsync(new OpenClawConfigureRequest
			{
				GatewayUrl = OpenClawGatewayUrl, Token = _openClawToken, Password = _openClawPassword,
				AutoReconnect = false, Start = true
			}).ConfigureAwait(false);
			ApplyOpenClawStatus(status);
			SaveResult = status.Connected ? "OpenClaw Gateway 测试连接成功。" : "OpenClaw Gateway 测试未连接，请查看状态和日志。";
		}
		catch (Exception exception)
		{
			OpenClawStatusText = UiText.UserError(exception, "测试 OpenClaw 连接");
			SaveResult = OpenClawStatusText;
			_logs.AddException("openclaw", "测试 OpenClaw Gateway 失败。", exception);
		}
	}

	private async Task ClearOpenClawCredentialsAsync()
	{
		try
		{
			await _openClawSecrets.DeleteAsync();
			_openClawToken = "";
			_openClawPassword = "";
			_openClawCredentialsConfigured = false;
			OnPropertyChanged(nameof(OpenClawCredentialSummary));
			SaveResult = "已清除 Bridge 保存的 OpenClaw 凭据。";
		}
		catch (Exception exception) { SaveResult = UiText.UserError(exception, "清除 OpenClaw 凭据"); }
	}

	private void ApplyOpenClawStatus(OpenClawConnectionStatus status)
	{
		if (!string.IsNullOrWhiteSpace(status.GatewayUrl)) OpenClawGatewayUrl = status.GatewayUrl;
		var connection = status.Connected ? "已连接" : status.Running ? "正在连接/重连" : status.Configured ? "已停止" : "未配置";
		OpenClawStatusText = $"{connection} · {OpenClawGatewayUrl}" +
			(string.IsNullOrWhiteSpace(status.ServerVersion) ? "" : $" · Gateway {status.ServerVersion}") +
			(string.IsNullOrWhiteSpace(status.LastError) ? "" : $"\n{UiText.UserError(status.LastError, "OpenClaw")}");
		OnPropertyChanged(nameof(OpenClawCredentialSummary));
	}

    public string BackendStatus
    {
        get => _backendStatus;
        private set => SetProperty(ref _backendStatus, value);
    }

    public string ApprovalPolicy
    {
        get => _approvalPolicy;
        private set { if (SetProperty(ref _approvalPolicy, value)) OnPropertyChanged(nameof(ApprovalPolicyDisplay)); }
    }
    public string ApprovalPolicyDisplay => ApprovalPolicy switch
    {
        "on-request" => "需要时询问",
        "never" => "不询问",
        "untrusted" => "仅确认受限操作",
        _ => "使用当前设置"
    };

    public string SaveResult
    {
        get => _saveResult;
        private set => SetProperty(ref _saveResult, value);
    }
	public bool MirrorEnabled { get=>_mirrorEnabled; set=>SetProperty(ref _mirrorEnabled,value); }
	public bool TelegramMirrorEnabled { get=>_telegramMirrorEnabled; set=>SetProperty(ref _telegramMirrorEnabled,value); }
	public string TelegramMirrorChatId { get=>_telegramMirrorChatId; set=>SetProperty(ref _telegramMirrorChatId,value); }
	public bool QqMirrorEnabled { get=>_qqMirrorEnabled; set=>SetProperty(ref _qqMirrorEnabled,value); }
	public string QqMirrorConversationType { get=>_qqMirrorConversationType; set=>SetProperty(ref _qqMirrorConversationType,value); }
	public IReadOnlyList<string> QqMirrorConversationTypes { get; } = ["c2c","group"];
	public string QqMirrorOpenId { get=>_qqMirrorOpenId; set=>SetProperty(ref _qqMirrorOpenId,value); }
	public bool MirrorAssistant { get=>_mirrorAssistant; set=>SetProperty(ref _mirrorAssistant,value); }
	public bool MirrorInput { get=>_mirrorInput; set=>SetProperty(ref _mirrorInput,value); }
	public bool MirrorError { get=>_mirrorError; set=>SetProperty(ref _mirrorError,value); }
	public bool RequireThreadNumber { get=>_requireThreadNumber; set=>SetProperty(ref _requireThreadNumber,value); }
	public string MirrorStatusText { get=>_mirrorStatusText; private set=>SetProperty(ref _mirrorStatusText,value); }
	public string QqCapabilityNotice { get=>_qqCapabilityNotice; private set=>SetProperty(ref _qqCapabilityNotice,value); }

	public Task RefreshMirrorAsync(CancellationToken cancellationToken = default) =>
		LoadMirrorAsync(autoStart: false, cancellationToken);

	public Task InitializeMirrorAsync(bool autoStart, CancellationToken cancellationToken = default) =>
		LoadMirrorAsync(autoStart, cancellationToken);

	private async Task LoadMirrorAsync(bool autoStart, CancellationToken cancellationToken)
	{
		try
		{
			await _mirrorOperationLock.WaitAsync(cancellationToken).ConfigureAwait(false);
			try
			{
				var status = await _api.GetMirrorAsync(cancellationToken).ConfigureAwait(false);
				if (autoStart && !status.Config.Enabled)
				{
					status.Config.Enabled = true;
					status = await _api.ConfigureMirrorAsync(status.Config, cancellationToken).ConfigureAwait(false);
				}
				await RunOnUiAsync(() => ApplyMirrorStatus(status));
			}
			finally
			{
				_mirrorOperationLock.Release();
			}
		}
		catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
		{
		}
		catch (Exception exception)
		{
			_logs.AddException("desktop", "初始化消息同步失败。", exception);
			await RunOnUiAsync(() => MirrorStatusText = UiText.UserError(exception, "读取同步状态"));
		}
	}

	private void ApplyMirrorStatus(MirrorStatus status)
	{
		MirrorEnabled=status.Config.Enabled;RequireThreadNumber=status.Config.RequireThreadNumber;
		TelegramMirrorEnabled=status.Config.Telegram.Enabled;TelegramMirrorChatId=status.Config.Telegram.ChatId;
		QqMirrorEnabled=status.Config.Qq.Enabled;QqMirrorConversationType=status.Config.Qq.ConversationType;QqMirrorOpenId=status.Config.Qq.OpenId;
		MirrorAssistant=status.Config.Messages.Assistant;MirrorInput=status.Config.Messages.RequestUserInput;MirrorError=status.Config.Messages.Error;
		MirrorStatusText=$"Telegram：{FormatMirrorState(status.TelegramState)}；QQ：{FormatQqMirrorState(status.QqState)}" + (string.IsNullOrWhiteSpace(status.LastQqError) ? "" : $"；{UiText.UserError(status.LastQqError, "QQ 同步")}");
		QqCapabilityNotice="QQ 主动消息受平台权限、回复窗口和发送额度限制。";
	}

	private static string FormatMirrorState(string state) => state switch
	{
		"ready" or "running" or "enabled" => "已启用",
		"disabled" or "stopped" => "未启用",
		"failed" => "失败",
		_ => UiText.Status(state)
	};

	private static string FormatQqMirrorState(string state) => state switch
	{
		"platform-capability-limited" => "平台能力受限",
		"target-invalid" => "同步目标无效",
		"platform-rejected" => "平台拒绝",
		"ready-limited" => "已就绪（主动消息受平台限制）",
		_ => FormatMirrorState(state)
	};

    public void UpdateRuntimeStatus(BridgeStatus status, string backendStatus)
    {
        BackendStatus = backendStatus;
        CodexCliPath = status.CodexCliPath;
        if (!string.IsNullOrWhiteSpace(status.CodexCliPathSource)) CodexPathSource = status.CodexCliPathSource;
        CodexValidationStatus = string.IsNullOrWhiteSpace(status.CodexCliValidationStatus)
            ? status.CodexCliAvailable ? "succeeded" : "failed"
            : status.CodexCliValidationStatus;
        CodexConnectionStatus = string.IsNullOrWhiteSpace(status.CodexCliConnectionStatus)
            ? status.AppServerRunning ? "connected" : status.CodexCliAvailable ? "connecting" : "not-found"
            : status.CodexCliConnectionStatus;
        RebuildCodexDetection(status.CodexCliVersion);
        SandboxMode = status.SandboxMode;
        ApprovalPolicy = status.ApprovalPolicy;
    }

    private void RebuildCodexDetection(string version = "")
    {
        if (string.IsNullOrWhiteSpace(CodexCliPath))
        {
            CodexDetection = "未找到 Codex。请确认已安装，或手动选择程序路径。";
            return;
        }
        var source = CodexPathSource switch
        {
            "RunningChatGPT" => "正在运行的 ChatGPT",
            "RunningCodex" => "正在运行的 Codex",
            "SavedPath" => "已保存路径",
            "PATH" => "系统 PATH",
            "Manual" => "手动设置",
            _ => string.IsNullOrWhiteSpace(CodexPathSource) ? "未知" : CodexPathSource
        };
        var validation = CodexValidationStatus == "succeeded" ? "已通过" : CodexValidationStatus == "failed" ? "失败" : "验证中";
        var connection = CodexConnectionStatus switch
        {
            "connected" => "已连接",
            "reconnecting" => "正在重新连接",
            "connecting" => "正在连接",
            "not-found" => "未找到",
            "disconnected" => "未连接",
            _ => "等待 daemon"
        };
        var versionText = string.IsNullOrWhiteSpace(version) ? "" : $"\n版本：{version}";
        CodexDetection = $"路径：{CodexCliPath}\n来源：{source}\n验证：{validation}\n连接：{connection}{versionText}";
    }

    private async Task SaveAsync()
    {
        var requestedManualPath = CodexCustomPath.Trim();
        var manualValidation = string.IsNullOrWhiteSpace(requestedManualPath)
            ? new CodexDiscoveryResult(false, "", "", CodexDiscoverySource.None)
            : await _codexDiscoveryService.ValidateManualPathAsync(requestedManualPath);
        if (!CodexPathSettings.TryPrepareManualSave(
                _settings, requestedManualPath, manualValidation, out var manualPathToApply))
        {
            SaveResult = "自定义 Codex 路径不存在、不可执行或不是可直接启动的 CLI；设置未保存。请留空使用自动检测。";
            _logs.Add("codex-config", $"[codex-config] manual path rejected before save path={LogService.Redact(requestedManualPath)}");
            return;
        }

        CodexCustomPath = _settings.CodexCustomPath;
        _settings.SandboxMode = SandboxMode is "read-only" ? "read-only" : "workspace-write";
        _settings.StartWithWindows = StartWithWindows;
        _settings.SilentStartup = SilentStartup;
        _settings.CloseToTray = CloseToTray;
        _settings.RestoreLastPage = RestoreLastPage;
        _settings.AutoRefreshThreads = AutoRefreshThreads;
        _settings.ThreadRefreshIntervalSeconds = ThreadRefreshIntervalSeconds;
        _settings.Theme = Theme;
        _settings.MirrorAutoStart = MirrorAutoStart;
		_settings.OpenClawGatewayUrl = OpenClawGatewayUrl.Trim();
		_settings.OpenClawAutoDiscover = OpenClawAutoDiscover;
		_settings.OpenClawAutoReconnect = OpenClawAutoReconnect;
        try { _startup.Configure(StartWithWindows, SilentStartup); }
        catch (Exception exception) { SaveResult = $"启动项更新失败：{exception.Message}"; return; }
        await _service.SaveAsync(_settings);
		if (!string.IsNullOrWhiteSpace(_openClawToken) || !string.IsNullOrWhiteSpace(_openClawPassword))
		{
			await _openClawSecrets.SaveAsync(_openClawToken, _openClawPassword);
			_openClawCredentialsConfigured = true;
		}
        _logs.Add("codex-config", $"[codex-config] persisted path={_settings.CodexCustomPath}");
        try
        {
            if (!string.IsNullOrWhiteSpace(manualPathToApply))
            {
                var codexStatus = await _api.ApplyCodexPathAsync(manualPathToApply, "Manual");
                UpdateRuntimeStatus(codexStatus, BackendStatus);
                _logs.Add("codex-config", $"[codex-config] runtime path updated path={codexStatus.CodexCliPath} target=daemon");
            }
			var mirror = await _api.ConfigureMirrorAsync(new MirrorConfig { Enabled=MirrorEnabled,RequireThreadNumber=RequireThreadNumber,Telegram=new TelegramMirrorConfig{Enabled=TelegramMirrorEnabled,ChatId=TelegramMirrorChatId.Trim()},Qq=new QqMirrorConfig{Enabled=QqMirrorEnabled,ConversationType=QqMirrorConversationType,OpenId=QqMirrorOpenId.Trim()},Messages=new MirrorMessageTypes{User=false,Assistant=MirrorAssistant,Status=false,RequestUserInput=MirrorInput,Error=MirrorError} });
			ApplyMirrorStatus(mirror);
			var status = await _api.UpdateSecurityAsync(_settings.SandboxMode);
			ApprovalPolicy = status.ApprovalPolicy;
			if (_openClawCredentialsConfigured)
			{
				var openClawStatus = await _api.ConfigureOpenClawAsync(new OpenClawConfigureRequest
				{
					GatewayUrl = _settings.OpenClawGatewayUrl, Token = _openClawToken, Password = _openClawPassword,
					AutoReconnect = _settings.OpenClawAutoReconnect, Start = true
				});
				ApplyOpenClawStatus(openClawStatus);
			}
            SaveResult = string.IsNullOrWhiteSpace(manualPathToApply)
                ? "设置已保存；Codex 路径保持自动检测，不会提交旧的手动路径。"
                : "设置已保存，Codex 程序路径已立即应用到当前运行时。";
        }
        catch (Exception exception)
        {
            SaveResult = "设置已保存，但当前运行时应用失败；请查看日志后重试。";
            _logs.AddException("desktop", "应用设置时本地服务不可用。", exception);
        }
        _logs.Add("desktop", "用户设置已保存（未记录凭据或消息正文）。");
    }

    private static void OpenDirectory(string path)
    {
        Directory.CreateDirectory(path);
        var info = new ProcessStartInfo("explorer.exe") { UseShellExecute = true };
        info.ArgumentList.Add(path);
        Process.Start(info);
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
}
