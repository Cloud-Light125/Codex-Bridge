using System.Collections.Specialized;
using System.ComponentModel;
using CloudLight.CodexBridge.Infrastructure;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class OverviewViewModel : ObservableObject
{
    private readonly SessionsViewModel _sessions;
    private readonly OpenClawViewModel _openClaw;
    private readonly ChannelProfilesViewModel _channelProfiles;
    private readonly SettingsViewModel _settings;
    private string _codexState = "正在启动";

    public OverviewViewModel(SessionsViewModel sessions, OpenClawViewModel openClaw, ChannelProfilesViewModel channelProfiles, SettingsViewModel settings)
    {
        _sessions = sessions;
        _openClaw = openClaw;
        _channelProfiles = channelProfiles;
        _settings = settings;
        sessions.Threads.CollectionChanged += OnChanged;
        sessions.PropertyChanged += OnChanged;
        openClaw.Sessions.CollectionChanged += OnChanged;
        openClaw.PropertyChanged += OnChanged;
        channelProfiles.Profiles.CollectionChanged += OnChanged;
        channelProfiles.PropertyChanged += OnChanged;
        settings.PropertyChanged += OnChanged;
    }

    public string CodexState { get => _codexState; set => SetProperty(ref _codexState, value); }
    public string SessionCount => $"{_sessions.Threads.Count:N0} 个会话";
    public string CurrentThread => _sessions.SelectedThread is null ? "未选择会话" : $"{_sessions.SelectedThread.NumberPrefix}  {_sessions.SelectedThread.Title}";
    public string RunningTurn => _sessions.CanStop ? "正在运行" : "当前无运行任务";
    public string OpenClawState => $"{_openClaw.Sessions.Count:N0} 个 Session · {_openClaw.GatewayStatusText}";
    public string TelegramState => ProfileState("telegram");
    public string QqState => ProfileState("qqbot");
    public string MirrorState => _settings.MirrorEnabled ? "已启用 · 仅同步最终回答" : "未启用";
    public string RecentActivity => _sessions.SelectedThread is null ? "等待选择 Codex 会话" : $"最近查看：{_sessions.SelectedThread.Title}";

    private void OnChanged(object? sender, EventArgs e) => NotifyAll();
    private void OnChanged(object? sender, NotifyCollectionChangedEventArgs e) => NotifyAll();
    private void NotifyAll()
    {
        OnPropertyChanged(nameof(SessionCount));
        OnPropertyChanged(nameof(CurrentThread));
        OnPropertyChanged(nameof(RunningTurn));
		OnPropertyChanged(nameof(OpenClawState));
        OnPropertyChanged(nameof(TelegramState));
        OnPropertyChanged(nameof(QqState));
        OnPropertyChanged(nameof(MirrorState));
        OnPropertyChanged(nameof(RecentActivity));
    }

	private string ProfileState(string platform)
	{
		var profile = _channelProfiles.Profiles.FirstOrDefault(item => item.Platform == platform);
		return profile is null ? "未配置" : $"{profile.Name} · {profile.StatusText}";
	}
}

public sealed class MirrorViewModel(SettingsViewModel settings)
{
    public SettingsViewModel Settings { get; } = settings;
}
