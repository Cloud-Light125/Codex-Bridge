using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

// This view model is intentionally independent from SessionsViewModel. Codex
// threads and OpenClaw sessions have different APIs and runtime semantics;
// their shared global number is rendered with an explicit backend label while
// keeping each list and event stream isolated.
public sealed class OpenClawViewModel : ObservableObject
{
    private readonly BridgeApiClient _api;
    private readonly LogService _logs;
    private readonly AsyncRelayCommand _sendCommand;
    private readonly AsyncRelayCommand _stopCommand;
    private string _searchText = "";
    private OpenClawSessionSummary? _selectedSession;
    private OpenClawSessionDetail? _selectedDetail;
    private OpenClawConnectionStatus _gateway = new();
    private string _messageText = "";
    private string _errorText = "";
    private bool _isBusy;
    private bool _isSending;
    private long _selectionVersion;
    private CancellationTokenSource? _detailCancellation;

    public OpenClawViewModel(BridgeApiClient api, LogService logs)
    {
        _api = api;
        _logs = logs;
        SessionsView = CollectionViewSource.GetDefaultView(Sessions);
        SessionsView.Filter = MatchesSearch;
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync());
        _sendCommand = new AsyncRelayCommand(SendAsync, () => CanSend);
        SendCommand = _sendCommand;
        _stopCommand = new AsyncRelayCommand(StopAsync, () => CanStop);
        StopCommand = _stopCommand;
        CopySessionKeyCommand = new RelayCommand(_ => CopySessionKey());
    }

    public ObservableCollection<OpenClawSessionSummary> Sessions { get; } = [];
    public ObservableCollection<TimelineEntry> Messages { get; } = [];
    public ICollectionView SessionsView { get; }
    public ICommand RefreshCommand { get; }
    public ICommand SendCommand { get; }
    public ICommand StopCommand { get; }
    public ICommand CopySessionKeyCommand { get; }

    public string SearchText { get => _searchText; set { if (SetProperty(ref _searchText, value)) SessionsView.Refresh(); } }
    public OpenClawSessionSummary? SelectedSession
    {
        get => _selectedSession;
        set
        {
            if (!SetProperty(ref _selectedSession, value)) return;
            if (value is null)
            {
                CancelDetailLoad();
                SelectedDetail = null;
                Messages.Clear();
                RefreshCommandStates();
                return;
            }
            BeginLoadSession(value.Key);
        }
    }
    public OpenClawSessionDetail? SelectedDetail { get => _selectedDetail; private set { if (SetProperty(ref _selectedDetail, value)) RefreshCommandStates(); } }
    public string MessageText { get => _messageText; set { if (SetProperty(ref _messageText, value)) RefreshCommandStates(); } }
    public string ErrorText { get => _errorText; private set => SetProperty(ref _errorText, value); }
    public bool IsBusy
    {
        get => _isBusy;
        private set
        {
            if (!SetProperty(ref _isBusy, value)) return;
            OnPropertyChanged(nameof(BusyVisibility));
            OnPropertyChanged(nameof(EmptyVisibility));
            OnPropertyChanged(nameof(DetailsVisibility));
        }
    }
    public Visibility BusyVisibility => IsBusy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility EmptyVisibility => SelectedSession is null && !IsBusy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility DetailsVisibility => SelectedDetail is not null && !IsBusy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ErrorVisibility => !string.IsNullOrWhiteSpace(ErrorText) ? Visibility.Visible : Visibility.Collapsed;
    public bool CanSend => SelectedDetail is not null && !_isSending && !_gateway.Connected.Equals(false) && !SelectedDetail.HasActiveRun && SelectedDetail.Archived != true && !string.IsNullOrWhiteSpace(MessageText);
    public bool CanStop => SelectedDetail?.HasActiveRun == true && !_isSending;
    public string GatewayStatusText => $"{UiText.Status(_gateway.State)} · {(_gateway.Connected ? "已连接" : _gateway.Running ? "重连中" : "未连接")}";
    public string GatewayDetailText => string.Join(" · ", new[]
    {
        string.IsNullOrWhiteSpace(_gateway.GatewayUrl) ? "未设置 Gateway 地址" : _gateway.GatewayUrl,
        _gateway.SessionCount > 0 ? $"{_gateway.SessionCount} 个 Session" : "未读取 Session",
        _gateway.ReconnectCount > 0 ? $"已重连 {_gateway.ReconnectCount} 次" : "自动重连待命"
    });
    public string GatewayError => _gateway.LastError;
    public string CurrentRunText => SelectedDetail?.HasActiveRun == true ? $"运行中 · {SelectedDetail.ActiveRunIds.FirstOrDefault()}" : "空闲";

    public async Task RefreshAsync(CancellationToken cancellationToken = default, bool reloadSelected = true)
    {
        IsBusy = true;
        ErrorText = "";
        try
        {
            // Gateway health remains useful even when listing sessions fails
            // (for example while the remote daemon is reconnecting), so update
            // the two pieces independently instead of hiding the status behind
            // a Task.WhenAll failure.
            _gateway = await _api.GetOpenClawStatusAsync(cancellationToken);
            OnGatewayChanged();
            var response = await _api.GetOpenClawSessionsAsync(100, cancellationToken);
            var selectedKey = SelectedSession?.Key;
            Sessions.Clear();
            foreach (var session in BackendListProjection.OpenClawSessions(response.Sessions).OrderBy(item => item.Number == 0 ? int.MaxValue : item.Number).ThenBy(item => item.Key)) Sessions.Add(session);
            SelectedSession = string.IsNullOrWhiteSpace(selectedKey) ? null : Sessions.FirstOrDefault(item => item.Key == selectedKey);
            if (SelectedSession is not null && reloadSelected) BeginLoadSession(SelectedSession.Key);
            OnPropertyChanged(nameof(EmptyVisibility));
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            ErrorText = UiText.UserError(exception, "读取 OpenClaw");
            _logs.AddException("openclaw", "刷新 OpenClaw 页面失败。", exception);
        }
        finally { IsBusy = false; }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (!bridgeEvent.EventType.StartsWith("openclaw.", StringComparison.OrdinalIgnoreCase)) return;
        if (bridgeEvent.EventType is "openclaw.connected" or "openclaw.disconnected" or "openclaw.reconnecting" or "openclaw.session.updated")
        {
            _ = RefreshAsync(reloadSelected: false);
            return;
        }
        if (SelectedSession is null || !string.Equals(SelectedSession.Key, bridgeEvent.ThreadId, StringComparison.Ordinal)) return;
        switch (bridgeEvent.EventType)
        {
            case "openclaw.message.delta":
                AppendMessage(bridgeEvent, "OpenClaw", PayloadText(bridgeEvent, "delta"), "正在回复", "assistant");
                SetActiveRun(bridgeEvent.TurnId, true);
                break;
            case "openclaw.message.completed":
                AppendMessage(bridgeEvent, "OpenClaw", PayloadText(bridgeEvent, "text"), "回复完成", "assistant", replace: true);
                SetActiveRun(bridgeEvent.TurnId, false);
                break;
            case "openclaw.message.aborted":
                AppendMessage(bridgeEvent, "OpenClaw", "任务已停止", "已停止", "status");
                SetActiveRun(bridgeEvent.TurnId, false);
                break;
            case "openclaw.message.failed":
                AppendMessage(bridgeEvent, "OpenClaw", PayloadText(bridgeEvent, "error", "Gateway 返回错误"), "失败", "status", failure: true);
                SetActiveRun(bridgeEvent.TurnId, false);
                break;
        }
    }

    private void BeginLoadSession(string key)
    {
        CancelDetailLoad();
        SelectedDetail = null;
        Messages.Clear();
        ErrorText = "";
        var version = Interlocked.Increment(ref _selectionVersion);
        _detailCancellation = new CancellationTokenSource();
        _ = LoadSessionAsync(key, version, _detailCancellation.Token);
    }

    private async Task LoadSessionAsync(string key, long version, CancellationToken cancellationToken)
    {
        try
        {
            var detail = await _api.GetOpenClawSessionAsync(key, cancellationToken);
            if (!IsCurrentSelection(key, version)) return;
            SelectedDetail = detail;
            Messages.Clear();
            foreach (var message in detail.Messages)
            {
                var user = string.Equals(message.Role, "user", StringComparison.OrdinalIgnoreCase);
                Messages.Add(new TimelineEntry
                {
                    Key = string.IsNullOrWhiteSpace(message.Id) ? $"{message.RunId}:{message.Timestamp}" : message.Id,
                    TurnId = message.RunId, Timestamp = message.Timestamp, Kind = user ? "user" : "assistant",
                    Title = user ? "用户" : "OpenClaw", Text = message.Text, Status = "消息"
                });
            }
            RefreshCommandStates();
            OnPropertyChanged(nameof(DetailsVisibility));
            OnPropertyChanged(nameof(EmptyVisibility));
            OnPropertyChanged(nameof(CurrentRunText));
        }
        catch (OperationCanceledException) { }
        catch (Exception exception)
        {
            if (!IsCurrentSelection(key, version)) return;
            ErrorText = UiText.UserError(exception, "读取 OpenClaw Session");
            _logs.AddException("openclaw", "读取 OpenClaw Session 失败。", exception);
        }
    }

    private async Task SendAsync()
    {
        if (!CanSend || SelectedDetail is null) return;
        var text = MessageText.Trim();
        _isSending = true;
        RefreshCommandStates();
        try
        {
            var accepted = await _api.SendOpenClawMessageAsync(SelectedDetail.Key, text);
            Messages.Add(new TimelineEntry { Key = $"pending-user-{accepted.RunId}", TurnId = accepted.RunId, Kind = "user", Title = "用户", Text = text, Status = "已发送", IsTemporary = true });
            MessageText = "";
            SetActiveRun(accepted.RunId, true);
        }
        catch (Exception exception)
        {
            ErrorText = UiText.UserError(exception, "发送 OpenClaw 消息");
            _logs.AddException("openclaw", "发送 OpenClaw 消息失败（正文未记录）。", exception);
        }
        finally { _isSending = false; RefreshCommandStates(); }
    }

    private async Task StopAsync()
    {
        if (!CanStop || SelectedDetail is null) return;
        try
        {
            var result = await _api.AbortOpenClawAsync(SelectedDetail.Key, SelectedDetail.ActiveRunIds.FirstOrDefault() ?? "");
            SetActiveRun(result.RunId, false);
        }
        catch (Exception exception)
        {
            ErrorText = UiText.UserError(exception, "停止 OpenClaw 任务");
        }
    }

    private void SetActiveRun(string runId, bool active)
    {
        if (SelectedDetail is null) return;
        SelectedDetail.HasActiveRun = active;
        SelectedDetail.ActiveRunIds = active && !string.IsNullOrWhiteSpace(runId) ? [runId] : [];
        RefreshCommandStates();
        OnPropertyChanged(nameof(CurrentRunText));
    }

    private void AppendMessage(BridgeEvent bridgeEvent, string title, string text, string status, string kind, bool replace = false, bool failure = false)
    {
        var key = $"{bridgeEvent.TurnId}:{kind}";
        var entry = Messages.LastOrDefault(item => item.Key == key);
        if (entry is null)
        {
            entry = new TimelineEntry { Key = key, TurnId = bridgeEvent.TurnId, Timestamp = bridgeEvent.Timestamp, Kind = kind, Title = title, IsTemporary = true };
            Messages.Add(entry);
        }
        if (!string.IsNullOrWhiteSpace(text)) entry.Text = replace ? text : entry.Text + text;
        entry.Status = status;
        entry.IsFailure = failure;
    }

    private static string PayloadText(BridgeEvent bridgeEvent, string name, string fallback = "") =>
        bridgeEvent.Payload.ValueKind == System.Text.Json.JsonValueKind.Object && bridgeEvent.Payload.TryGetProperty(name, out var value) && value.ValueKind == System.Text.Json.JsonValueKind.String
            ? value.GetString() ?? fallback : fallback;

    private bool MatchesSearch(object item) => item is OpenClawSessionSummary session &&
        (string.IsNullOrWhiteSpace(SearchText) || session.Title.Contains(SearchText, StringComparison.CurrentCultureIgnoreCase) || session.Key.Contains(SearchText, StringComparison.OrdinalIgnoreCase) || session.Cwd.Contains(SearchText, StringComparison.CurrentCultureIgnoreCase));

    private void CopySessionKey()
    {
        if (SelectedDetail is null) return;
        try { Clipboard.SetText(SelectedDetail.Key); }
        catch (Exception exception) { ErrorText = $"复制 Session Key 失败：{exception.Message}"; }
    }

    private void CancelDetailLoad()
    {
        Interlocked.Increment(ref _selectionVersion);
        _detailCancellation?.Cancel();
        _detailCancellation?.Dispose();
        _detailCancellation = null;
    }

    private bool IsCurrentSelection(string key, long version) => version == Interlocked.Read(ref _selectionVersion) && SelectedSession?.Key == key;
    private void OnGatewayChanged()
    {
        OnPropertyChanged(nameof(GatewayStatusText));
        OnPropertyChanged(nameof(GatewayDetailText));
        OnPropertyChanged(nameof(GatewayError));
        RefreshCommandStates();
    }
    private void RefreshCommandStates()
    {
        OnPropertyChanged(nameof(CanSend));
        OnPropertyChanged(nameof(CanStop));
        _sendCommand.RaiseCanExecuteChanged();
        _stopCommand.RaiseCanExecuteChanged();
    }
}
