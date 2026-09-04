using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Controls;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

// Owns the user-facing robot settings. The daemon still owns live transport
// sharing; this view model keeps the screen focused on one robot at a time.
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
            OnPropertyChanged(nameof(TutorialHeader));
            OnPropertyChanged(nameof(QqTutorialVisibility));
            OnPropertyChanged(nameof(TelegramTutorialVisibility));
        }
    }

    public string PageTitle => SelectedPlatform == "qqbot" ? "QQ 机器人" : "Telegram 机器人";
    public string PageDescription => SelectedPlatform == "qqbot"
        ? "连接 QQ 机器人，并设置哪些用户可以通过它使用 Codex 或 OpenClaw。"
        : "连接 Telegram 机器人，并设置哪些账号可以通过它使用 Codex 或 OpenClaw。";
    public string AddButtonText => SelectedPlatform == "qqbot" ? "添加 QQ 机器人" : "添加 Telegram 机器人";
    public string EmptyText => SelectedPlatform == "qqbot" ? "还没有 QQ 机器人设置。" : "还没有 Telegram 机器人设置。";
    public string TutorialHeader => SelectedPlatform == "qqbot"
        ? "第一次使用？查看 QQ 机器人申请与填写教程"
        : "第一次使用？查看 Telegram 机器人申请与填写教程";
    public Visibility QqTutorialVisibility => SelectedPlatform == "qqbot" ? Visibility.Visible : Visibility.Collapsed;
    public Visibility TelegramTutorialVisibility => SelectedPlatform == "telegram" ? Visibility.Visible : Visibility.Collapsed;
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
        StatusMessage = "正在恢复机器人设置…";
        try
        {
            var restoredProfiles = new List<(ChannelProfileViewModel Profile, string? Secret, bool SecretLoadFailed)>();
            var migratedQqSecret = false;
            var hasWarnings = false;
            foreach (var profile in Profiles)
            {
                try
                {
                    var restored = await LoadSecretAsync(profile, cancellationToken);
                    restoredProfiles.Add((profile, restored.Secret, false));
                    migratedQqSecret |= restored.MigratedFromDpapi;
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    // A damaged credential for one channel must not prevent a
                    // different enabled Profile (for example QQ) from being
                    // configured and auto-started.
                    hasWarnings = true;
                    profile.SetCredentialConfigured(false);
                    profile.ApplyOperationFailure("authentication-failed", "无法读取已保存的机器人密钥；请在机器人设置中重新保存密钥。");
                    restoredProfiles.Add((profile, null, true));
                    _logs.AddException("channels", $"恢复 {profile.Name} 的已保存凭据失败。", exception);
                }
            }
            if (migratedQqSecret)
            {
                try
                {
                    PersistProfilesToSettings();
                    await _settingsService.SaveAsync(_settings);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    hasWarnings = true;
                    _logs.AddException("channels", "迁移 QQ 机器人密钥失败。", exception);
                }
            }
            var configuredProfiles = new List<(ChannelProfileViewModel Profile, bool HasCredential, bool SecretLoadFailed)>();
            foreach (var (profile, secret, secretLoadFailed) in restoredProfiles)
            {
                try
                {
                    profile.SetCredentialConfigured(!string.IsNullOrWhiteSpace(secret));
                    var status = await _api.ConfigureChannelProfileAsync(profile.Id, profile.ToRequest(secret), cancellationToken);
                    profile.ApplyStatus(status);
                    if (secretLoadFailed)
                        profile.ApplyOperationFailure("authentication-failed", "无法读取已保存的机器人密钥；请在机器人设置中重新保存密钥。");
                    configuredProfiles.Add((profile, !string.IsNullOrWhiteSpace(secret), secretLoadFailed));
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    hasWarnings = true;
                    ApplyOperationFailure(profile, exception);
                    _logs.AddException("channels", $"配置 {profile.Name} 失败。", exception);
                }
            }
            try
            {
                await _api.ConfigureChannelRoutingAsync(BuildRouting(), cancellationToken);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                hasWarnings = true;
                _logs.AddException("channels", "恢复 Channel Profile 路由失败。", exception);
            }
            foreach (var (profile, hasCredential, secretLoadFailed) in configuredProfiles.Where(item => item.Profile.Enabled && item.Profile.AutoStart))
            {
                if (!hasCredential || secretLoadFailed) continue;
                try
                {
                    var status = await _api.StartChannelProfileAsync(profile.Id, cancellationToken);
                    profile.ApplyStatus(status);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    hasWarnings = true;
                    ApplyOperationFailure(profile, exception);
                    _logs.AddException("channels", $"自动启动 {profile.Name} 失败。", exception);
                }
            }
            await RefreshAsync(cancellationToken, preserveStatus: true);
            _initialized = true;
            StatusMessage = hasWarnings
                ? "机器人设置已恢复；部分密钥、处理方式或连接失败，请查看对应机器人。"
                : "机器人设置、处理方式与已保存密钥已恢复。";
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
            await RefreshRecentIdentitiesAsync(cancellationToken);
            if (!preserveStatus) StatusMessage = "已刷新机器人连接和处理方式。";
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
            var viewModel = new ChannelProfileViewModel(profile, AllowRecentIdentityAsync);
            viewModel.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, viewModel);
            viewModel.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, viewModel);
            Profiles.Add(viewModel);
        }
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
    }

    private void ApplyResponse(ChannelProfilesResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        _settings.ChannelRouting = response.Routing ?? new BackendChannelRoutingSettings();
        foreach (var profile in Profiles)
        {
            var status = (response.Profiles ?? []).FirstOrDefault(item => string.Equals(item.Id, profile.Id, StringComparison.OrdinalIgnoreCase));
            if (status is not null)
            {
                profile.ApplyStatus(status);
            }
            profile.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, profile);
            profile.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, profile);
        }
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
    }

    private async Task RefreshRecentIdentitiesAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _api.GetQqDiscoveredIdentitiesAsync(cancellationToken);
            var identities = response.Identities ?? [];
            var qqProfiles = Profiles.Where(item => item.IsQq).ToList();
            var fallback = qqProfiles.FirstOrDefault(item => string.Equals(item.Id, "qq-default", StringComparison.OrdinalIgnoreCase))
                ?? (qqProfiles.Count == 1 ? qqProfiles[0] : null);
            fallback?.SetRecentQqIdentities(identities);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
        {
            _logs.AddException("channels", "读取最近联系过机器人的 QQ 用户失败。", exception);
        }
    }

    private async Task AllowRecentIdentityAsync(ChannelProfileViewModel profile, object? identity)
    {
        if (profile is null || identity is null) return;
        if (identity is TelegramRecentIdentity telegram && telegram.UserId > 0)
            profile.AddAllowedTelegramUser(telegram.UserId);
        else if (identity is QqDiscoveredIdentity qq)
            profile.AddAllowedQqIdentity(qq);
        else
            return;
        await SaveProfileAsync(profile);
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
        var profile = new ChannelProfileViewModel(settings, AllowRecentIdentityAsync);
        Profiles.Add(profile);
        PersistProfilesToSettings();
        ProfilesView.Refresh();
        OnPropertyChanged(nameof(EmptyVisibility));
        StatusMessage = "已添加机器人设置；填写密钥并保存后即可连接。";
    }

    private async Task SaveProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        IsBusy = true;
        OnPropertyChanged(nameof(BusyVisibility));
        var enteredSecret = "";
        try
        {
            enteredSecret = profile.HasPendingSecret ? profile.TakePendingSecret() : "";
            var restored = await LoadSecretAsync(profile);
            var credentialChanged = ChannelCredentialChange.HasChanged(
                profile.IsQq,
                profile.SavedAppId,
                profile.AppId,
                enteredSecret,
                restored.Secret);
            var enteredSecretChanged = !string.IsNullOrWhiteSpace(enteredSecret) &&
                !string.Equals(enteredSecret, restored.Secret, StringComparison.Ordinal);
            var wasRunning = profile.IsRunning;
            var stoppedForSave = wasRunning && (credentialChanged || !profile.Enabled);

            if (credentialChanged && wasRunning && !ReconnectConfirmationDialog.Show(profile.PlatformLabel))
            {
                if (!string.IsNullOrWhiteSpace(enteredSecret)) profile.SetPendingSecret(enteredSecret);
                if (profile.IsQq) profile.AppId = profile.SavedAppId;
                StatusMessage = "已取消保存机器人密钥更改。";
                return;
            }

            if (stoppedForSave)
                profile.ApplyStatus(await _api.StopChannelProfileAsync(profile.Id));

            if (enteredSecretChanged)
                await SaveSecretAsync(profile, enteredSecret);

            // An AppID change needs the existing secret in the request so the
            // daemon can derive a new physical-resource key.  For ordinary
            // saves, null means “keep the stored secret” and avoids asking a
            // running adapter to replace it.
            var secretForRequest = credentialChanged
                ? (!string.IsNullOrWhiteSpace(enteredSecret) ? enteredSecret : restored.Secret)
                : null;
            var status = await _api.ConfigureChannelProfileAsync(profile.Id, profile.ToRequest(secretForRequest));
            profile.ApplyStatus(status);
            profile.SetCredentialConfigured(!string.IsNullOrWhiteSpace(enteredSecret) || !string.IsNullOrWhiteSpace(restored.Secret));
            profile.MarkSavedCredentialBaseline();
            PersistProfilesToSettings();
            await _settingsService.SaveAsync(_settings);

            var routingSaved = true;
            try
            {
                await SaveRoutingCoreAsync();
            }
            catch (Exception routingException) when (routingException is not OperationCanceledException)
            {
                routingSaved = false;
                _logs.AddException("channels", "保存机器人处理方式失败。", routingException);
            }

            var shouldStart = profile.Enabled && (stoppedForSave ? wasRunning : !wasRunning && profile.AutoStart);
            if (shouldStart)
                profile.ApplyStatus(await _api.StartChannelProfileAsync(profile.Id));

            await RefreshAsync(preserveStatus: true);
            StatusMessage = routingSaved
                ? stoppedForSave && wasRunning ? "机器人设置已保存，机器人已重新连接。" : "机器人设置已保存。"
                : "基本设置已保存，但处理方式保存失败，请重试。";
        }
        catch (Exception exception)
        {
            if (!string.IsNullOrWhiteSpace(enteredSecret) && !profile.HasPendingSecret)
                profile.SetPendingSecret(enteredSecret);
            ApplyOperationFailure(profile, exception);
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
        catch (Exception exception)
        {
            ApplyOperationFailure(profile, exception);
            StatusMessage = $"启动 {profile.Name} 失败：{profile.LastError}";
            _logs.AddException("channels", $"启动 {profile.Name} 失败。", exception);
        }
    }

    private static void ApplyOperationFailure(ChannelProfileViewModel profile, Exception exception)
    {
        var detail = exception is BridgeApiException api
            ? $"{api.Code} {(!string.IsNullOrWhiteSpace(api.LastError) ? api.LastError : api.Message)}"
            : UiText.UserError(exception, $"启动 {profile.Name}");
		var message = UiText.UserError(detail, $"连接 {profile.PlatformLabel}");
		var state = exception is BridgeApiException bridgeState ? bridgeState.CurrentState : "";
        profile.ApplyOperationFailure(state, message);
    }

    private async Task StopProfileAsync(ChannelProfileViewModel? profile)
    {
        if (profile is null) return;
        try
        {
            profile.ApplyStatus(await _api.StopChannelProfileAsync(profile.Id));
            StatusMessage = $"已停止 {profile.Name}，机器人当前未连接。";
        }
        catch (Exception exception) { StatusMessage = UiText.UserError(exception, $"停止 {profile.Name}"); }
    }

    private async Task SaveRoutingAsync()
    {
        try
        {
            await SaveRoutingCoreAsync();
            StatusMessage = "已保存机器人处理方式。";
        }
        catch (Exception exception)
        {
            StatusMessage = UiText.UserError(exception, "保存机器人处理方式");
            _logs.AddException("channels", "保存机器人处理方式失败。", exception);
        }
    }

    private async Task SaveRoutingCoreAsync(CancellationToken cancellationToken = default)
    {
        var routing = BuildRouting();
        _settings.ChannelRouting = await _api.ConfigureChannelRoutingAsync(routing, cancellationToken);
        foreach (var profile in Profiles)
        {
            profile.CodexAssigned = ContainsRoute(_settings.ChannelRouting.Codex, profile);
            profile.OpenClawAssigned = ContainsRoute(_settings.ChannelRouting.OpenClaw, profile);
        }
        PersistProfilesToSettings();
        await _settingsService.SaveAsync(_settings);
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

        var storedSecret = settings.AppSecret ?? "";
        if (!string.IsNullOrWhiteSpace(storedSecret))
        {
            return (storedSecret, false);
        }

        var legacySecret = await loadDpapiAsync(cancellationToken) ?? "";
        if (string.IsNullOrWhiteSpace(legacySecret)) return (null, false);

        settings.AppSecret = legacySecret;
        return (legacySecret, true);
    }

    private static async Task SaveSecretAsync(ChannelProfileViewModel profile, string secret) {
        if (profile.IsTelegram) await new TelegramSecretService(profile.Id).SaveAsync(secret);
        else
        {
            // QQ treats AppSecret as an opaque credential. Keep its exact
            // value identical in settings.json and DPAPI rather than trimming
            // it on the way to either store.
            var appSecret = secret;
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

internal static class ChannelCredentialChange
{
    internal static bool HasChanged(bool isQq, string savedAppId, string currentAppId, string? enteredSecret, string? storedSecret)
    {
        if (isQq && !string.Equals(savedAppId?.Trim(), currentAppId?.Trim(), StringComparison.Ordinal))
            return true;
        if (string.IsNullOrWhiteSpace(enteredSecret)) return false;
        return !string.Equals(enteredSecret, storedSecret, StringComparison.Ordinal);
    }
}

public sealed class ChannelProfileViewModel : ObservableObject
{
    private readonly ChannelProfileSettings _settings;
    private readonly Func<ChannelProfileViewModel, object?, Task>? _allowRecentIdentity;
    private string _allowedIdsText;
    private string _allowedGroupIdsText;
    private string _allowedGroupMemberIdsText;
    private string _pendingSecret = "";
    private string _savedAppId;
    private bool _credentialConfigured;
    private bool _codexAssigned;
    private bool _openClawAssigned;
    private ChannelProfileStatus _status = new();

    public ChannelProfileViewModel(ChannelProfileSettings settings, Func<ChannelProfileViewModel, object?, Task>? allowRecentIdentity = null)
    {
        _settings = settings;
        _allowRecentIdentity = allowRecentIdentity;
        _settings.Telegram ??= new TelegramProfileSettings();
        _settings.Qq ??= new QqProfileSettings();
        _savedAppId = _settings.Qq.AppId;
        _allowedIdsText = IsTelegram
            ? string.Join(Environment.NewLine, _settings.Telegram.AllowedUserIds)
            : string.Join(Environment.NewLine, _settings.Qq.AllowedUserOpenIds);
        _allowedGroupIdsText = string.Join(Environment.NewLine, _settings.Qq.AllowedGroupOpenIds);
        _allowedGroupMemberIdsText = string.Join(Environment.NewLine, _settings.Qq.AllowedGroupMemberOpenIds);
        AllowRecentIdentityCommand = new RelayCommand(value =>
        {
            if (_allowRecentIdentity is not null) _ = _allowRecentIdentity(this, value);
        });
    }

    public string Id => _settings.Id;
    public string Platform => _settings.Platform;
    public bool IsTelegram => Platform == "telegram";
    public bool IsQq => Platform == "qqbot";
    public string PlatformLabel => IsTelegram ? "Telegram 机器人" : "QQ 机器人";
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
    public string AllowedGroupIdsText { get => _allowedGroupIdsText; set => SetProperty(ref _allowedGroupIdsText, value ?? ""); }
    public string AllowedGroupMemberIdsText { get => _allowedGroupMemberIdsText; set => SetProperty(ref _allowedGroupMemberIdsText, value ?? ""); }
    public bool CodexAssigned { get => _codexAssigned; set => SetProperty(ref _codexAssigned, value); }
    public bool OpenClawAssigned { get => _openClawAssigned; set => SetProperty(ref _openClawAssigned, value); }
    public string StatusText => string.IsNullOrWhiteSpace(_status.State) ? "尚未连接" : UiText.Status(_status.State);
    public string ConnectionText => _status.Connected ? "已连接" : _status.Running ? "正在连接" : "未连接";
    public string SharedText => string.IsNullOrWhiteSpace(_status.SharedWithProfileId) ? "此机器人单独使用连接" : "与其他同密钥机器人共用连接";
    public string CredentialSummary => HasPendingSecret ? "有未保存的机器人密钥" : _credentialConfigured
        ? "机器人密钥已保存"
        : "尚未填写机器人密钥";
    public string AccountText => IsTelegram ? (string.IsNullOrWhiteSpace(_status.BotUsername) ? _status.AccountId : $"@{_status.BotUsername}") : _status.AccountId;
    public string LastError => string.IsNullOrWhiteSpace(_status.LastError) ? "" : UiText.UserError(_status.LastError, $"连接 {PlatformLabel}");
    public string BindingCountText => $"已关联 {_status.BindingCount} 个聊天";
    public bool HasPendingSecret => !string.IsNullOrWhiteSpace(_pendingSecret);
    public bool IsRunning => _status.Running;
    internal string SavedAppId => _savedAppId;
    public ObservableCollection<QqDiscoveredIdentity> RecentQqIdentities { get; } = [];
    public ObservableCollection<TelegramRecentIdentity> RecentTelegramIdentities { get; } = [];
    public ICommand AllowRecentIdentityCommand { get; }
    public Visibility TelegramVisibility => IsTelegram ? Visibility.Visible : Visibility.Collapsed;
    public Visibility QqVisibility => IsQq ? Visibility.Visible : Visibility.Collapsed;

    public void SetPendingSecret(string value)
    {
        // Preserve QQ credentials exactly. Telegram keeps the historical
        // whitespace-normalization behavior.
        _pendingSecret = IsQq ? value ?? "" : value?.Trim() ?? "";
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

    public void MarkSavedCredentialBaseline() => _savedAppId = AppId;

    public void ApplyStatus(ChannelProfileStatus status)
    {
        _status = status;
        if (IsTelegram)
        {
            _credentialConfigured = status.TokenSet;
            SetRecentTelegramIdentities(status.RecentIdentities);
        }
        else
        {
            _credentialConfigured = status.SecretConfigured;
            SetRecentQqIdentities(status.RecentQqIdentities);
        }
        OnPropertyChanged(nameof(StatusText));
        OnPropertyChanged(nameof(ConnectionText));
        OnPropertyChanged(nameof(SharedText));
        OnPropertyChanged(nameof(CredentialSummary));
        OnPropertyChanged(nameof(AccountText));
        OnPropertyChanged(nameof(LastError));
        OnPropertyChanged(nameof(BindingCountText));
        OnPropertyChanged(nameof(IsRunning));
    }

    public void ApplyOperationFailure(string? state, string? error)
    {
        _status.State = string.IsNullOrWhiteSpace(state) ? "failed" : state;
        _status.Running = false;
        _status.Connected = false;
        _status.LastError = LogService.Redact(error ?? "");
        OnPropertyChanged(nameof(StatusText));
        OnPropertyChanged(nameof(ConnectionText));
        OnPropertyChanged(nameof(SharedText));
        OnPropertyChanged(nameof(CredentialSummary));
        OnPropertyChanged(nameof(AccountText));
        OnPropertyChanged(nameof(LastError));
        OnPropertyChanged(nameof(BindingCountText));
        OnPropertyChanged(nameof(IsRunning));
    }

    public void SetRecentQqIdentities(IEnumerable<QqDiscoveredIdentity> identities)
    {
        RecentQqIdentities.Clear();
        foreach (var identity in identities.Take(20)) RecentQqIdentities.Add(identity);
    }

    public void SetRecentTelegramIdentities(IEnumerable<TelegramRecentIdentity> identities)
    {
        RecentTelegramIdentities.Clear();
        foreach (var identity in identities.Take(20)) RecentTelegramIdentities.Add(identity);
    }

    public void AddAllowedTelegramUser(long userId)
    {
        if (userId <= 0) return;
        var values = ParseLines(_allowedIdsText).ToList();
        var text = userId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        if (!values.Contains(text, StringComparer.Ordinal)) values.Add(text);
        AllowedIdsText = string.Join(Environment.NewLine, values);
    }

    public void AddAllowedQqIdentity(QqDiscoveredIdentity identity)
    {
        if (string.Equals(identity.Type, "group", StringComparison.OrdinalIgnoreCase))
        {
            AddLine(ref _allowedGroupIdsText, identity.GroupOpenId);
            AddLine(ref _allowedGroupMemberIdsText, identity.GroupMemberOpenId);
            OnPropertyChanged(nameof(AllowedGroupIdsText));
            OnPropertyChanged(nameof(AllowedGroupMemberIdsText));
            return;
        }
        AddLine(ref _allowedIdsText, identity.UserOpenId);
        OnPropertyChanged(nameof(AllowedIdsText));
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
            _settings.Telegram.AllowedUserIds = ParseLines(_allowedIdsText)
                .Select(value => long.TryParse(value, out var id) ? id : 0).Where(id => id > 0).Distinct().ToList();
        }
        else
        {
            _settings.Qq.AllowedUserOpenIds = ParseLines(_allowedIdsText).ToList();
            _settings.Qq.AllowedGroupOpenIds = ParseLines(_allowedGroupIdsText).ToList();
            _settings.Qq.AllowedGroupMemberOpenIds = ParseLines(_allowedGroupMemberIdsText).ToList();
        }
    }

    private static IEnumerable<string> ParseLines(string? value) => (value ?? "").Split(['\r', '\n', ',', ';', ' '], StringSplitOptions.RemoveEmptyEntries)
        .Select(item => item.Trim()).Where(item => item.Length > 0).Distinct(StringComparer.Ordinal);

    private static void AddLine(ref string target, string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return;
        var values = ParseLines(target).ToList();
        if (!values.Contains(value.Trim(), StringComparer.Ordinal)) values.Add(value.Trim());
        target = string.Join(Environment.NewLine, values);
    }
}
