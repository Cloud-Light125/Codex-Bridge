using System.Collections.Specialized;
using System.ComponentModel;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class RecentTaskCard(BridgeTask task)
{
    public BridgeTask Task { get; } = task;
    public string QuickActionId => Task.Status == "running" ? "cancel" : Task.Status == "waiting-input" ? "answer" : Task.Status == "completed" ? "continue" : "";
    public string QuickActionLabel => Task.Status switch { "running" => "停止", "waiting-input" => "回答", "completed" => "继续", _ => "" };
    public Visibility QuickActionVisibility => string.IsNullOrWhiteSpace(QuickActionId) ? Visibility.Collapsed : Visibility.Visible;
}

public sealed class OverviewViewModel : ObservableObject
{
    private readonly SessionsViewModel _sessions;
    private readonly OpenClawViewModel _openClaw;
    private readonly ChannelProfilesViewModel _channelProfiles;
    private readonly SettingsViewModel _settings;
    private readonly TasksViewModel _tasks;
    private readonly RelayCommand _quickTaskActionCommand;
    private string _codexState = "正在启动";

    public OverviewViewModel(SessionsViewModel sessions, OpenClawViewModel openClaw, ChannelProfilesViewModel channelProfiles, SettingsViewModel settings, TasksViewModel tasks)
    {
        _sessions = sessions;
        _openClaw = openClaw;
        _channelProfiles = channelProfiles;
        _settings = settings;
        _tasks = tasks;
        _quickTaskActionCommand = new RelayCommand(parameter => _ = ExecuteQuickTaskActionAsync(parameter));
        QuickTaskActionCommand = _quickTaskActionCommand;
        sessions.Threads.CollectionChanged += OnChanged;
        sessions.PropertyChanged += OnChanged;
        openClaw.Sessions.CollectionChanged += OnChanged;
        openClaw.PropertyChanged += OnChanged;
        channelProfiles.Profiles.CollectionChanged += OnChanged;
        channelProfiles.PropertyChanged += OnChanged;
        settings.PropertyChanged += OnChanged;
        tasks.Tasks.CollectionChanged += OnChanged;
        tasks.PropertyChanged += OnChanged;
    }

    public string CodexState { get => _codexState; set => SetProperty(ref _codexState, value); }
    public string SessionCount => $"{_sessions.Threads.Count:N0} 个会话";
    public string CurrentThread => _sessions.SelectedThread is null ? "未选择会话" : $"{_sessions.SelectedThread.NumberPrefix}  {_sessions.SelectedThread.Title}";
    public string RunningTurn => _sessions.CanStop ? "正在运行" : "当前无运行任务";
    public string OpenClawState => $"{_openClaw.Sessions.Count:N0} 个会话 · {_openClaw.GatewayStatusText}";
    public string TelegramState => ProfileState("telegram");
    public string QqState => ProfileState("qqbot");
    public string MirrorState => !_settings.MirrorEnabled
        ? "未启用"
        : _settings.MirrorFinalOnly ? "已启用\n仅同步最终消息" : "已启用\n包含额外提醒";
    public string RecentActivity => _sessions.SelectedThread is null ? "等待选择 Codex 会话" : $"最近查看：{_sessions.SelectedThread.Title}";
    public string RecentTasks => _tasks.Tasks.Count == 0
        ? "暂无任务"
        : string.Join(Environment.NewLine, _tasks.Tasks
            .OrderByDescending(task => task.TaskNumber)
            .Take(5)
            .Select(task => $"{task.NumberLabel} · {FirstNonEmpty(task.Title, task.Description, "未命名任务")} · {task.StatusDisplay}"));
    public IEnumerable<RecentTaskCard> RecentTaskItems => _tasks.Tasks
        .OrderByDescending(task => task.TaskNumber)
        .Take(5)
        .Select(task => new RecentTaskCard(task));
    public ICommand QuickTaskActionCommand { get; }
    public event Action? TasksRequested;
    public string TaskSummary => $"运行中 {_tasks.RunningCount} · 等待输入 {_tasks.WaitingCount} · 已完成 {_tasks.CompletedCount} · 失败 {_tasks.FailedCount}";

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
        OnPropertyChanged(nameof(RecentTasks));
        OnPropertyChanged(nameof(RecentTaskItems));
        OnPropertyChanged(nameof(TaskSummary));
    }

    private async Task ExecuteQuickTaskActionAsync(object? parameter)
    {
        if (parameter is not RecentTaskCard card) return;
        _tasks.SelectedTask = _tasks.Tasks.FirstOrDefault(task => task.TaskNumber == card.Task.TaskNumber);
        if (card.QuickActionId == "cancel")
        {
            await _tasks.ExecuteQuickActionAsync(card.Task.TaskNumber, card.QuickActionId);
            return;
        }
        TasksRequested?.Invoke();
    }

	private static string FirstNonEmpty(params string[] values) => values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value)) ?? "";

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
