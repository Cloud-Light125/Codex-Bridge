using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

// Owns the user-facing profile/routing model.  The daemon owns the live
// physical transport de-duplication; this view model deliberately stores only
// metadata and profile options in settings.json.  Credentials stay in the
// profile-scoped DPAPI services.
public sealed class ChannelProfilesViewModel : ObservableObject
{
    private readonly BridgeApiClient _api;
    private readonly SettingsService _settingsService;
    private readonly UserSettings _settings;
    private readonly LogService _logs;
    private string _selectedPlatform = "telegram";
    private string _statusMessage = "尚未连接本地后端。";
    private bool _isBusy;
    private bool _initialized;

    public ChannelProfilesViewModel(BridgeApiClient api, SettingsService settingsService, UserSettings settings, LogService logs)
    {
        _api = api;
        _settingsService = settingsService;
        _settings = settings;
        _logs = logs;
        ProfilesView = CollectionViewSource.GetDefaultView(Profiles);
        ProfilesView.Filter = MatchesPlatform;
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync());
        AddTelegramCommand = new RelayCommand(_ => AddProfile("telegram"));
        AddQqCommand = new RelayCommand(_ => AddProfile("qqbot"));
		AddCurrentProfileCommand = new RelayCommand(_ => AddProfile(SelectedPlatform));
        SaveProfileCommand = new RelayCommand(value => _ = SaveProfileAsync(value as ChannelProfileViewModel));
        DeleteProfileCommand = new RelayCommand(value => _ = DeleteProfileAsync(value as ChannelProfileViewModel));
        StartProfileCommand = new RelayCommand(value => _ = StartProfileAsync(value as ChannelProfileViewModel));
        StopProfileCommand = new RelayCommand(value => _ = StopProfileAsync(value as ChannelProfileViewModel));
        SaveRoutingCommand = new AsyncRelayCommand(SaveRoutingAsync);
        LoadFromSettings();
    }

    public ObservableCollection<ChannelProfileViewModel> Profiles { get; } = [];
    public ICollectionView ProfilesView { get; }
    public ICommand RefreshCommand { get; }
    public ICommand AddTelegramCommand { get; }
    public ICommand AddQqCommand { get; }
	public ICommand AddCurrentProfileCommand { get; }
    public ICommand SaveProfileCommand { get; }
    public ICommand DeleteProfileCommand { get; }
    public ICommand StartProfileCommand { get; }
    public ICommand StopProfileCommand { get; }
    public ICommand SaveRoutingCommand { get; }

    public string SelectedPlatform
    {
        get => _selectedPlatform;
        private set
        {
            if (!SetProperty(ref _selectedPlatform, value)) return;
            ProfilesView.Refresh();
            OnPropertyChanged(nameof(PageTitle));
            OnPropertyChanged(nameof(PageDescription));
            OnPropertyChanged(nameof(AddButtonText));
            OnPropertyChanged(nameof(EmptyText));
        }
    }

    public string PageTitle => SelectedPlatform == "qqbot" ? "QQ Bot" : "Telegram";
    public string PageDescription => SelectedPlatform == "qqbot"
        ? "管理 QQ Bot Profile、Gateway 状态以及分配给 Codex / OpenClaw 的路由"
        : "管理 Telegram Profile、Long Polling 状态以及分配给 Codex / OpenClaw 的路由";
    public string AddButtonText => SelectedPlatform == "qqbot" ? "添加 QQ Profile" : "添加 Telegram Profile";
    public string EmptyText => SelectedPlatform == "qqbot" ? "还没有 QQ Profile。" : "还没有 Telegram Profile。";
    public string StatusMessage { get => _statusMessage; private set => SetProperty(ref _statusMessage, value); }
    public bool IsBusy { get => _isBusy; private set => SetProperty(ref _isBusy, value); }
    public Visibility BusyVisibility => IsBusy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility EmptyVisibility => ProfilesView.IsEmpty ? Visibility.Visible : Visibility.Collapsed;

    public void SelectPlatform(string platform)
    {
        SelectedPlatform = string.Equals(platform, "qqbot", StringComparison.OrdinalIgnoreCase) || string.Equals(platform, "qq", StringComparison.OrdinalIgnoreCase)
            ? "qqbot"
            : "telegram";
    }

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default, bool force = false)
    {
        if (_initialized && !force) return;
        IsBusy = true;
        StatusMessage = "正在恢复消息渠道 Profile…";
        try
        {
            var restoredProfiles = new List<(ChannelProfileViewModel Profile, string? Secret)>();
            var migratedQqSecret = false;
            foreach (var profile in Profiles)
            {
                var restored = await LoadSecretAsync(profile, cancellationToken);
                restoredProfiles.Add((profile, restored.Secret));
                migratedQqSecret |= restored.MigratedFromDpapi;
            }
            if (migratedQqSecret)
            {
                PersistProfilesToSettings();
                await _settingsService.SaveAsync(_settings);
            }
            foreach (var (profile, secret) in restoredProfiles)
            {
                profile.SetCredentialConfigured(!string.IsNullOrWhiteSpace(secret));
                var status = await _api.ConfigureChannelProfileAsync(profile.Id, profile.ToRequest(secret), cancellationToken);
                profile.ApplyStatus(status);
            }
            await _api.ConfigureChannelRoutingAsync(BuildRouting(), cancellationToken);
            foreach (var profile in Profiles.Where(profile => profile.Enabled && profile.AutoStart))
            {
                var status = await _api.StartChannelProfileAsync(profile.Id, cancellationToken);
                profile.ApplyStatus(status);
            }
            await RefreshAsync(cancellationToken, preserveStatus: true);
            _initialized = true;
            StatusMessage = "Profile、路由与已保存凭据已恢复。";
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            StatusMessage = UiText.UserError(exception, "初始化消息渠道");
            _logs.AddException("channels", "初始化消息渠道 Profile 失败。", exception);
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(BusyVisibility));
        }
    }

    public async Task RefreshAsync(CancellationToken cancellationToken = default, bool preserveStatus = false)
    {
        IsBusy = true;
        OnPropertyChanged(nameof(BusyVisibility));
        try
        {
            var response = await _api.GetChannelProfilesAsync(cancellationToken);
            ApplyResponse(response);
            if (!preserveStatus) StatusMessage = "已刷新渠道 Profile、连接状态和后端分配。";
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            StatusMessage = UiText.UserError(exception, "刷新消息渠道");
            _logs.AddException("channels", "刷新渠道 Profile 失败。", exception);
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(BusyVisibility));
        }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (!bridgeEvent.EventType.StartsWith("channel.", StringComparison.OrdinalIgnoreCase) &&
            !bridgeEvent.EventType.StartsWith("telegram.", StringComparison.OrdinalIgnoreCase) &&
            !bridgeEvent.EventType.StartsWith("qq", StringComparison.OrdinalIgnoreCase)) return;
        _ = RefreshAsync(preserveStatus: true);
    }

    private void LoadFromSettings()
    {
        Profiles.Clear();
        foreach (var profile in _settings.ChannelProfiles)
        {
            var viewModel = new ChannelProfileViewModel(profile);
            viewModel.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, viewModel);
            viewModel.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, viewModel);
            Profiles.Add(viewModel);
        }
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
    }

    private void ApplyResponse(ChannelProfilesResponse response)
    {
        _settings.ChannelRouting = response.Routing ?? new BackendChannelRoutingSettings();
        foreach (var profile in Profiles)
        {
            var status = response.Profiles.FirstOrDefault(item => string.Equals(item.Id, profile.Id, StringComparison.OrdinalIgnoreCase));
            if (status is not null) profile.ApplyStatus(status);
            profile.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, profile);
            profile.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, profile);
        }
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
    }

    private void AddProfile(string platform)
    {
        var index = Profiles.Count(profile => profile.Platform == platform) + 1;
        var prefix = platform == "qqbot" ? "qq" : "telegram";
        var id = $"{prefix}-{index}";
        while (Profiles.Any(profile => string.Equals(profile.Id, id, StringComparison.OrdinalIgnoreCase))) id = $"{prefix}-{++index}";
        var settings = new ChannelProfileSettings
        {
            Id = id,
            Name = platform == "qqbot" ? $"QQ-{index}" : $"Telegram-{index}",
            Platform = platform,
            Enabled = true,
            Telegram = new TelegramProfileSettings(),
            Qq = new QqProfileSettings()
        };
        var profile = new ChannelProfileViewModel(settings);
        Profiles.Add(profile);
        PersistProfilesToSettings();
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
        StatusMessage = "已添加 Profile；输入凭据并保存后即可连接。";
    }

    private async Task SaveProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        IsBusy = true;
        OnPropertyChanged(nameof(BusyVisibility));
        try
        {
            if (profile.HasPendingSecret)
            {
                await SaveSecretAsync(profile, profile.TakePendingSecret());
                if (profile.IsQq)
                {
                    PersistProfilesToSettings();
                    await _settingsService.SaveAsync(_settings);
                }
            }
            var restored = await LoadSecretAsync(profile);
            var status = await _api.ConfigureChannelProfileAsync(profile.Id, profile.ToRequest(restored.Secret));
            profile.ApplyStatus(status);
            profile.SetCredentialConfigured(!string.IsNullOrWhiteSpace(restored.Secret));
            PersistProfilesToSettings();
            await _settingsService.SaveAsync(_settings);
            if (profile.Enabled && profile.AutoStart)
            {
                profile.ApplyStatus(await _api.StartChannelProfileAsync(profile.Id));
            }
            await SaveRoutingAsync();
            StatusMessage = profile.IsQq
                ? $"已保存 {profile.Name}；AppSecret 已保存到 settings.json 和 DPAPI。"
                : $"已保存 {profile.Name}；凭据仍只保存在 DPAPI 安全存储。";
        }
        catch (Exception exception)
        {
            StatusMessage = UiText.UserError(exception, $"保存 {profile.Name}");
            _logs.AddException("channels", "保存渠道 Profile 失败。", exception);
        }
        finally
        {
            IsBusy = false;
            OnPropertyChanged(nameof(BusyVisibility));
        }
    }

    private async Task DeleteProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        try
        {
            await _api.DeleteChannelProfileAsync(profile.Id);
            await DeleteSecretAsync(profile);
            Profiles.Remove(profile);
            PersistProfilesToSettings();
            await _settingsService.SaveAsync(_settings);
            await SaveRoutingAsync();
            ProfilesView.Refresh();
            OnPropertyChanged(nameof(EmptyVisibility));
            StatusMessage = $"已删除 {profile.Name}。";
        }
        catch (Exception exception)
        {
            StatusMessage = UiText.UserError(exception, $"删除 {profile.Name}");
        }
    }

    private async Task StartProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        try
        {
            profile.ApplyStatus(await _api.StartChannelProfileAsync(profile.Id));
            StatusMessage = $"已请求启动 {profile.Name}。";
        }
        catch (Exception exception) { StatusMessage = UiText.UserError(exception, $"启动 {profile.Name}"); }
    }

    private async Task StopProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        try
        {
            profile.ApplyStatus(await _api.StopChannelProfileAsync(profile.Id));
            StatusMessage = $"已停止 {profile.Name}。共享凭据只会在没有 Profile 使用时断开物理连接。";
        }
        catch (Exception exception) { StatusMessage = UiText.UserError(exception, $"停止 {profile.Name}"); }
    }

    private async Task SaveRoutingAsync()
    {
        try
        {
            var routing = BuildRouting();
            _settings.ChannelRouting = await _api.ConfigureChannelRoutingAsync(routing);
            PersistProfilesToSettings();
            await _settingsService.SaveAsync(_settings);
            foreach (var profile in Profiles)
            {
                profile.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, profile);
                profile.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, profile);
            }
            StatusMessage = "已保存 Codex / OpenClaw 的 Channel Profile 分配。";
        }
        catch (Exception exception)
        {
            StatusMessage = UiText.UserError(exception, "保存后端路由");
            _logs.AddException("channels", "保存 Channel Profile 路由失败。", exception);
        }
    }

    private BackendChannelRoutingSettings BuildRouting() => new()
    {
        Codex = new BackendChannelRouteSettings
        {
            TelegramProfileIds = Profiles.Where(profile => profile.IsTelegram && profile.CodexAssigned).Select(profile => profile.Id).ToList(),
            QqProfileIds = Profiles.Where(profile => profile.IsQq && profile.CodexAssigned).Select(profile => profile.Id).ToList()
        },
        OpenClaw = new BackendChannelRouteSettings
        {
            TelegramProfileIds = Profiles.Where(profile => profile.IsTelegram && profile.OpenClawAssigned).Select(profile => profile.Id).ToList(),
            QqProfileIds = Profiles.Where(profile => profile.IsQq && profile.OpenClawAssigned).Select(profile => profile.Id).ToList()
        }
    };

    private void PersistProfilesToSettings()
    {
        _settings.ChannelProfiles = Profiles.Select(profile => profile.ToSettings()).ToList();
        _settings.ChannelRouting = BuildRouting();
    }

    private static bool ContainsRoute(BackendChannelRouteSettings? route, ChannelProfileViewModel profile) =>
        (profile.IsTelegram ? route?.TelegramProfileIds : route?.QqProfileIds)?
            .Any(id => string.Equals(id, profile.Id, StringComparison.OrdinalIgnoreCase)) == true;

    private bool MatchesPlatform(object item) => item is ChannelProfileViewModel profile && profile.Platform == SelectedPlatform;

    private static async Task<(string? Secret, bool MigratedFromDpapi)> LoadSecretAsync(ChannelProfileViewModel profile, CancellationToken cancellationToken = default)
    {
        if (profile.IsTelegram)
            return (await new TelegramSecretService(profile.Id).LoadAsync(cancellationToken), false);
        return await ResolveQqAppSecretAsync(
            profile.QqSettings,
            token => new QqSecretService(profile.Id).LoadAsync(token),
            cancellationToken);
    }

    internal static async Task<(string? Secret, bool MigratedFromDpapi)> ResolveQqAppSecretAsync(
        QqProfileSettings settings,
        Func<CancellationToken, Task<string?>> loadDpapiAsync,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(loadDpapiAsync);

        var storedSecret = settings.AppSecret?.Trim() ?? "";
        if (storedSecret.Length > 0)
        {
            settings.AppSecret = storedSecret;
            return (storedSecret, false);
        }

        var legacySecret = (await loadDpapiAsync(cancellationToken))?.Trim() ?? "";
        if (legacySecret.Length == 0) return (null, false);

        settings.AppSecret = legacySecret;
        return (legacySecret, true);
    }

    private static async Task SaveSecretAsync(ChannelProfileViewModel profile, string secret) {
        if (profile.IsTelegram) await new TelegramSecretService(profile.Id).SaveAsync(secret);
        else
        {
            var appSecret = secret.Trim();
            await new QqSecretService(profile.Id).SaveAsync(appSecret);
            profile.QqSettings.AppSecret = appSecret;
        }
    }

    private static async Task DeleteSecretAsync(ChannelProfileViewModel profile) {
        if (profile.IsTelegram) await new TelegramSecretService(profile.Id).DeleteAsync();
        else
        {
            await new QqSecretService(profile.Id).DeleteAsync();
            profile.QqSettings.AppSecret = "";
        }
    }
}

public sealed class ChannelProfileViewModel : ObservableObject
{
    private readonly ChannelProfileSettings _settings;
    private string _allowedIdsText;
    private string _pendingSecret = "";
    private bool _credentialConfigured;
    private bool _codexAssigned;
    private bool _openClawAssigned;
    private ChannelProfileStatus _status = new();

    public ChannelProfileViewModel(ChannelProfileSettings settings)
    {
        _settings = settings;
        _settings.Telegram ??= new TelegramProfileSettings();
        _settings.Qq ??= new QqProfileSettings();
        _allowedIdsText = IsTelegram
            ? string.Join(Environment.NewLine, _settings.Telegram.AllowedUserIds)
            : string.Join(Environment.NewLine, _settings.Qq.AllowedUserOpenIds);
    }

    public string Id => _settings.Id;
    public string Platform => _settings.Platform;
    public bool IsTelegram => Platform == "telegram";
    public bool IsQq => Platform == "qqbot";
    public string PlatformLabel => IsTelegram ? "Telegram" : "QQ Bot";
    public string Name { get => _settings.Name; set { if (_settings.Name == value) return; _settings.Name = value?.Trim() ?? ""; OnPropertyChanged(); } }
    public bool Enabled { get => _settings.Enabled; set { if (_settings.Enabled == value) return; _settings.Enabled = value; OnPropertyChanged(); } }
    public bool AutoStart { get => IsTelegram ? _settings.Telegram.AutoStart : _settings.Qq.AutoStart; set { if (IsTelegram) _settings.Telegram.AutoStart = value; else _settings.Qq.AutoStart = value; OnPropertyChanged(); } }
    public bool SendProgressUpdates { get => IsTelegram ? _settings.Telegram.SendProgressUpdates : _settings.Qq.SendProgressUpdates; set { if (IsTelegram) _settings.Telegram.SendProgressUpdates = value; else _settings.Qq.SendProgressUpdates = value; OnPropertyChanged(); } }
    public bool ReconnectEnabled { get => _settings.Qq.ReconnectEnabled; set { _settings.Qq.ReconnectEnabled = value; OnPropertyChanged(); } }
    public string AppId { get => _settings.Qq.AppId; set { _settings.Qq.AppId = value?.Trim() ?? ""; OnPropertyChanged(); } }
    internal QqProfileSettings QqSettings => _settings.Qq;
    public int PollingTimeoutSeconds { get => _settings.Telegram.PollingTimeoutSeconds; set { _settings.Telegram.PollingTimeoutSeconds = Math.Clamp(value, 10, 60); OnPropertyChanged(); } }
    public string ProxyMode { get => IsTelegram ? _settings.Telegram.ProxyMode : _settings.Qq.ProxyMode; set { if (IsTelegram) _settings.Telegram.ProxyMode = value; else _settings.Qq.ProxyMode = value; OnPropertyChanged(); } }
    public string ProxyUrl { get => IsTelegram ? _settings.Telegram.ProxyUrl : _settings.Qq.ProxyUrl; set { if (IsTelegram) _settings.Telegram.ProxyUrl = value?.Trim() ?? ""; else _settings.Qq.ProxyUrl = value?.Trim() ?? ""; OnPropertyChanged(); } }
    public string AllowedIdsText { get => _allowedIdsText; set => SetProperty(ref _allowedIdsText, value ?? ""); }
    public bool CodexAssigned { get => _codexAssigned; set => SetProperty(ref _codexAssigned, value); }
    public bool OpenClawAssigned { get => _openClawAssigned; set => SetProperty(ref _openClawAssigned, value); }
    public string StatusText => string.IsNullOrWhiteSpace(_status.State) ? "未配置" : UiText.Status(_status.State);
    public string ConnectionText => _status.Connected ? "已连接" : _status.Running ? "连接中 / 重连中" : "已停止";
    public string SharedText => string.IsNullOrWhiteSpace(_status.SharedWithProfileId) ? "独立连接" : $"与 { _status.SharedWithProfileId } 共用凭据与连接";
    public string CredentialSummary => HasPendingSecret ? "有未保存的凭据" : _credentialConfigured
        ? IsQq ? "AppSecret 已保存到 settings.json 和 DPAPI" : "凭据已由 DPAPI 安全保存"
        : "尚未保存凭据";
    public string AccountText => IsTelegram ? (string.IsNullOrWhiteSpace(_status.BotUsername) ? _status.AccountId : $"@{_status.BotUsername}") : _status.AccountId;
    public string LastError => _status.LastError;
    public string BindingCountText => $"{_status.BindingCount} 个聊天绑定";
    public bool HasPendingSecret => !string.IsNullOrWhiteSpace(_pendingSecret);
    public Visibility TelegramVisibility => IsTelegram ? Visibility.Visible : Visibility.Collapsed;
    public Visibility QqVisibility => IsQq ? Visibility.Visible : Visibility.Collapsed;

    public void SetPendingSecret(string value)
    {
        _pendingSecret = value?.Trim() ?? "";
        OnPropertyChanged(nameof(HasPendingSecret));
        OnPropertyChanged(nameof(CredentialSummary));
    }

    public string TakePendingSecret()
    {
        var result = _pendingSecret;
        _pendingSecret = "";
        OnPropertyChanged(nameof(HasPendingSecret));
        OnPropertyChanged(nameof(CredentialSummary));
        return result;
    }

    public void SetCredentialConfigured(bool configured)
    {
        _credentialConfigured = configured;
        OnPropertyChanged(nameof(CredentialSummary));
    }

    public void ApplyStatus(ChannelProfileStatus status)
    {
        _status = status;
        if (IsTelegram) _credentialConfigured = status.TokenSet;
        else _credentialConfigured = status.SecretConfigured;
        OnPropertyChanged(nameof(StatusText));
        OnPropertyChanged(nameof(ConnectionText));
        OnPropertyChanged(nameof(SharedText));
        OnPropertyChanged(nameof(CredentialSummary));
        OnPropertyChanged(nameof(AccountText));
        OnPropertyChanged(nameof(LastError));
        OnPropertyChanged(nameof(BindingCountText));
    }

    public ChannelProfileConfigureRequest ToRequest(string? secret)
    {
        CommitTextValues();
        return new ChannelProfileConfigureRequest
        {
            Name = Name,
            Platform = Platform,
            Enabled = Enabled,
            Telegram = IsTelegram ? new TelegramProfileConfigureRequest
            {
                Token = string.IsNullOrWhiteSpace(secret) ? null : secret,
                AllowedUserIds = [.. _settings.Telegram.AllowedUserIds], PollingTimeoutSeconds = PollingTimeoutSeconds,
                SendProgressUpdates = SendProgressUpdates, AutoStart = AutoStart, ProxyMode = ProxyMode, ProxyUrl = ProxyUrl
            } : null,
            Qq = IsQq ? new QqProfileConfigureRequest
            {
                AppSecret = string.IsNullOrWhiteSpace(secret) ? null : secret,
                Enabled = Enabled, AutoStart = AutoStart, AppId = AppId, Environment = "production",
                AllowedUserOpenIds = [.. _settings.Qq.AllowedUserOpenIds], AllowedGroupOpenIds = [.. _settings.Qq.AllowedGroupOpenIds],
                AllowedGroupMemberOpenIds = [.. _settings.Qq.AllowedGroupMemberOpenIds], GroupTriggerMode = "official-at",
                CommandPrefix = string.IsNullOrWhiteSpace(_settings.Qq.CommandPrefix) ? "/codex" : _settings.Qq.CommandPrefix,
                SendProgressUpdates = SendProgressUpdates, GatewayReconnectEnabled = ReconnectEnabled, ProxyMode = ProxyMode, ProxyUrl = ProxyUrl
            } : null
        };
    }

    public ChannelProfileSettings ToSettings()
    {
        CommitTextValues();
        return new ChannelProfileSettings
        {
            Id = Id, Name = Name, Platform = Platform, Enabled = Enabled,
            Telegram = new TelegramProfileSettings
            {
                AllowedUserIds = [.. _settings.Telegram.AllowedUserIds], PollingTimeoutSeconds = PollingTimeoutSeconds,
                SendProgressUpdates = _settings.Telegram.SendProgressUpdates, AutoStart = _settings.Telegram.AutoStart,
                ProxyMode = _settings.Telegram.ProxyMode, ProxyUrl = _settings.Telegram.ProxyUrl
            },
            Qq = new QqProfileSettings
            {
                AppId = _settings.Qq.AppId, AppSecret = _settings.Qq.AppSecret, AutoStart = _settings.Qq.AutoStart, ReconnectEnabled = _settings.Qq.ReconnectEnabled,
                SendProgressUpdates = _settings.Qq.SendProgressUpdates, AllowedUserOpenIds = [.. _settings.Qq.AllowedUserOpenIds],
                AllowedGroupOpenIds = [.. _settings.Qq.AllowedGroupOpenIds], AllowedGroupMemberOpenIds = [.. _settings.Qq.AllowedGroupMemberOpenIds],
                GroupTriggerMode = _settings.Qq.GroupTriggerMode, CommandPrefix = _settings.Qq.CommandPrefix,
                ProxyMode = _settings.Qq.ProxyMode, ProxyUrl = _settings.Qq.ProxyUrl
            }
        };
    }

    private void CommitTextValues()
    {
        if (IsTelegram)
        {
            _settings.Telegram.AllowedUserIds = _allowedIdsText.Split(['\r', '\n', ',', ';', ' '], StringSplitOptions.RemoveEmptyEntries)
                .Select(value => long.TryParse(value.Trim(), out var id) ? id : 0).Where(id => id > 0).Distinct().ToList();
        }
        else
        {
            _settings.Qq.AllowedUserOpenIds = _allowedIdsText.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries)
                .Select(value => value.Trim()).Where(value => value.Length > 0).Distinct(StringComparer.Ordinal).ToList();
        }
    }
}
