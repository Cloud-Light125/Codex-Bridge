using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class SessionsViewModel : ObservableObject
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly BridgeApiClient _api;
    private readonly LogService _logs;
    private readonly AsyncRelayCommand _sendCommand;
    private readonly AsyncRelayCommand _stopCommand;
    private readonly AsyncRelayCommand _retryCommand;
    private readonly AsyncRelayCommand _verifyPersistenceCommand;
    private string _searchText = "";
    private ThreadSummary? _selectedThread;
    private ThreadDetail? _selectedDetail;
    private ThreadRuntime _runtime = new();
    private string _viewState = "empty";
    private string _errorText = "";
    private string _actionError = "";
    private string _messageText = "";
    private bool _isListBusy;
    private bool _isSending;
    private bool _suppressSelectionLoad;
    private long _selectionVersion;
    private CancellationTokenSource? _detailCancellation;
    private CancellationTokenSource? _reloadCancellation;
    private readonly HashSet<string> _persistedTurns = new(StringComparer.Ordinal);

    public SessionsViewModel(BridgeApiClient api, LogService logs)
    {
        _api = api;
        _logs = logs;
        ThreadsView = CollectionViewSource.GetDefaultView(Threads);
        ThreadsView.Filter = MatchesSearch;
        RefreshCommand = new AsyncRelayCommand(() => RefreshAsync());
        _retryCommand = new AsyncRelayCommand(RetryAsync);
        RetryCommand = _retryCommand;
        CopyThreadIdCommand = new RelayCommand(_ => CopyThreadId());
		CopyThreadPrefixCommand = new RelayCommand(_ => CopyThreadPrefix());
        OpenDirectoryCommand = new RelayCommand(_ => OpenDirectory());
        _sendCommand = new AsyncRelayCommand(SendAsync, () => CanSend);
        SendCommand = _sendCommand;
        _stopCommand = new AsyncRelayCommand(StopAsync, () => CanStop);
        StopCommand = _stopCommand;
        _verifyPersistenceCommand = new AsyncRelayCommand(VerifyPersistenceAsync, () => CanVerifyPersistence);
        VerifyPersistenceCommand = _verifyPersistenceCommand;
        AllowInteractionCommand = new RelayCommand(value => _ = RespondInteractionAsync(value as PendingInteractionViewModel, "allow"));
        DenyInteractionCommand = new RelayCommand(value => _ = RespondInteractionAsync(value as PendingInteractionViewModel, "deny"));
        SubmitAnswersCommand = new RelayCommand(value => _ = RespondInteractionAsync(value as PendingInteractionViewModel, "submit"));
    }

    public ObservableCollection<ThreadSummary> Threads { get; } = [];
    public ObservableCollection<TimelineEntry> Timeline { get; } = [];
    public ObservableCollection<PendingInteractionViewModel> PendingInteractions { get; } = [];
    public ICollectionView ThreadsView { get; }
    public ICommand RefreshCommand { get; }
    public ICommand RetryCommand { get; }
    public ICommand CopyThreadIdCommand { get; }
	public ICommand CopyThreadPrefixCommand { get; }
    public ICommand OpenDirectoryCommand { get; }
    public ICommand SendCommand { get; }
    public ICommand StopCommand { get; }
    public ICommand VerifyPersistenceCommand { get; }
    public ICommand AllowInteractionCommand { get; }
    public ICommand DenyInteractionCommand { get; }
    public ICommand SubmitAnswersCommand { get; }

    public string SearchText
    {
        get => _searchText;
        set
        {
            if (SetProperty(ref _searchText, value)) ThreadsView.Refresh();
        }
    }

    public ThreadSummary? SelectedThread
    {
        get => _selectedThread;
        set
        {
            if (!SetProperty(ref _selectedThread, value)) return;
            if (_suppressSelectionLoad) return;
            if (value is null)
            {
                CancelDetailLoad();
                SelectedDetail = null;
                Timeline.Clear();
                PendingInteractions.Clear();
                _persistedTurns.Clear();
                SetPersistenceVerificationText("");
                SetViewState("empty");
                return;
            }
            BeginLoadDetail(value.ThreadId);
        }
    }

    public ThreadDetail? SelectedDetail
    {
        get => _selectedDetail;
        private set
        {
            if (!SetProperty(ref _selectedDetail, value)) return;
        }
    }

    public Visibility EmptyVisibility => _viewState == "empty" ? Visibility.Visible : Visibility.Collapsed;
    public Visibility LoadingVisibility => _viewState == "loading" ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ErrorVisibility => _viewState == "error" ? Visibility.Visible : Visibility.Collapsed;
    public Visibility DetailsVisibility => _viewState == "details" ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ActionErrorVisibility => string.IsNullOrWhiteSpace(ActionError) ? Visibility.Collapsed : Visibility.Visible;

    public string ErrorText
    {
        get => _errorText;
        private set => SetProperty(ref _errorText, value);
    }

    public string ActionError
    {
        get => _actionError;
        private set
        {
            if (SetProperty(ref _actionError, value)) OnPropertyChanged(nameof(ActionErrorVisibility));
        }
    }

    public string MessageText
    {
        get => _messageText;
        set
        {
            if (SetProperty(ref _messageText, value)) RefreshCommandStates();
        }
    }

    public bool IsListBusy
    {
        get => _isListBusy;
        private set => SetProperty(ref _isListBusy, value);
    }

    public bool CanSend => SelectedDetail is not null && _runtime.CanSend && !_isSending && !string.IsNullOrWhiteSpace(MessageText);
    public bool CanStop => SelectedDetail is not null && _runtime.CanInterrupt && !_isSending && !string.IsNullOrWhiteSpace(_runtime.TurnId);
    public bool CanVerifyPersistence => SelectedDetail is not null;
    public string RuntimeStateText => StateText(_runtime.State);
    public string RuntimeSourceText => _runtime.Origin switch { "local" => "由本程序发起", "external" => "由其他 Codex 客户端发起", _ => "" };
    public string CurrentTurnId => _runtime.TurnId;
    public string PersistenceVerificationText { get; private set; } = "";
    public Visibility PersistenceVerificationVisibility => string.IsNullOrWhiteSpace(PersistenceVerificationText) ? Visibility.Collapsed : Visibility.Visible;

    public async Task RefreshAsync(CancellationToken cancellationToken = default, bool reloadSelected = true)
    {
        IsListBusy = true;
        ActionError = "";
        try
        {
            var selectedId = SelectedThread?.ThreadId;
            var response = await _api.GetThreadsAsync(100, cancellationToken: cancellationToken);
            _suppressSelectionLoad = true;
            try
            {
                Threads.Clear();
                foreach (var thread in BackendListProjection.CodexThreads(response.Threads))
                {
                    // The Codex endpoint is deliberately the only source for
                    // this page.  OpenClaw sessions have their own view model
                    // and page, never a synthetic ThreadSummary here.
                    thread.Backend = "codex";
                    Threads.Add(thread);
                }
                SelectedThread = string.IsNullOrWhiteSpace(selectedId)
                    ? null
                    : Threads.FirstOrDefault(thread => thread.ThreadId == selectedId);
            }
            finally
            {
                _suppressSelectionLoad = false;
            }

            if (SelectedThread is null)
            {
                SelectedDetail = null;
                Timeline.Clear();
                PendingInteractions.Clear();
                SetViewState("empty");
            }
            else if (reloadSelected)
            {
                BeginLoadDetail(SelectedThread.ThreadId);
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            ErrorText = UiText.UserError(exception, "读取会话");
            SetViewState("error");
            _logs.Add("desktop", $"读取会话列表失败：{exception.Message}");
        }
        finally
        {
            IsListBusy = false;
        }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (bridgeEvent.EventType.StartsWith("openclaw.", StringComparison.OrdinalIgnoreCase)) return;
        if (SelectedThread?.ThreadId != bridgeEvent.ThreadId) return;

        if (TryPayload<ThreadRuntime>(bridgeEvent.Payload, "runtime", out var runtime))
        {
            SetRuntime(runtime);
            // Newer backends carry the persistence state in runtime as well as
            // in turn.persistence_changed.  Treat it with the same rule.
            if (runtime.State is "persisted" or "completed-unverified" or "persistence-failed" or "thread-mismatch")
            {
                HandlePersistenceStatus(new BridgeEvent
                {
                    ThreadId = bridgeEvent.ThreadId,
                    TurnId = string.IsNullOrWhiteSpace(bridgeEvent.TurnId) ? runtime.TurnId : bridgeEvent.TurnId
                }, runtime.State);
            }
        }
        switch (bridgeEvent.EventType)
        {
            case "turn.started":
                SetRuntime(new ThreadRuntime
                {
                    ThreadId = bridgeEvent.ThreadId, TurnId = bridgeEvent.TurnId,
                    State = PayloadText(bridgeEvent.Payload, "status", "running-local"),
                    Origin = PayloadText(bridgeEvent.Payload, "source", "local"), CanSend = false,
                    CanInterrupt = PayloadText(bridgeEvent.Payload, "source", "local") == "local"
                });
                AddOrUpdateTimeline(bridgeEvent, "status", "任务已开始", "正在运行", false);
                break;
            case "assistant.delta":
                AppendAssistantDelta(bridgeEvent, PayloadText(bridgeEvent.Payload, "delta"));
                break;
            case "assistant.completed":
                CompleteAssistant(bridgeEvent);
                break;
            case "tool.started":
                AddOrUpdateTimeline(bridgeEvent, "tool", ToolTitle(bridgeEvent.Payload), ToolBody(bridgeEvent.Payload), true, "正在执行");
                break;
            case "tool.updated":
                AppendToolOutput(bridgeEvent, PayloadText(bridgeEvent.Payload, "outputDelta"));
                break;
            case "tool.completed":
                AddOrUpdateTimeline(bridgeEvent, "tool", ToolTitle(bridgeEvent.Payload), ToolBody(bridgeEvent.Payload), true, "已完成");
                break;
            case "file.changed":
                AddOrUpdateTimeline(bridgeEvent, "file", "文件修改", FileBody(bridgeEvent.Payload), true, PayloadText(bridgeEvent.Payload, "status", "已更新"));
                break;
            case "interaction.requested":
                if (TryPayload<PendingInteraction>(bridgeEvent.Payload, "interaction", out var interaction) && PendingInteractions.All(item => item.Id != interaction.Id))
                {
                    PendingInteractions.Add(new PendingInteractionViewModel(interaction));
                }
                break;
            case "interaction.resolved":
                if (TryPayload<PendingInteraction>(bridgeEvent.Payload, "interaction", out var resolved))
                {
                    var existing = PendingInteractions.FirstOrDefault(item => item.Id == resolved.Id);
                    if (existing is not null) PendingInteractions.Remove(existing);
                }
                break;
            case "turn.interrupted":
                AddOrUpdateTimeline(bridgeEvent, "status", "任务已停止", "停止请求已生效", false, "已停止");
                ScheduleRecalibration();
                break;
            case "turn.completed":
                if (PayloadText(bridgeEvent.Payload, "status") == "persisted")
                    HandlePersistenceStatus(bridgeEvent, "persisted");
                else
                    MarkTurnCompletedUnverified(bridgeEvent);
                break;
            case "turn.failed":
            {
                AddOrUpdateTimeline(bridgeEvent, "error", "任务失败", UiText.UserError(PayloadText(bridgeEvent.Payload, "error", "Codex 任务失败"), "任务"), false, "失败");
                var failurePersistenceStatus = PayloadTextInObject(bridgeEvent.Payload, "verification", "status");
				if (string.IsNullOrWhiteSpace(failurePersistenceStatus))
					failurePersistenceStatus = PayloadText(bridgeEvent.Payload, "status", "failed");
                HandlePersistenceStatus(bridgeEvent, failurePersistenceStatus);
                var failureLabel = failurePersistenceStatus switch
                {
                    "persistence-failed" => "保存状态检查失败",
                    "thread-mismatch" => "会话不一致：未确认写入",
                    _ => "发送失败"
                };
                MarkTemporaryTurn(bridgeEvent.TurnId, failureLabel, true);
                ScheduleRecalibration();
                break;
            }
            case "turn.persistence_changed":
                if (TryPayload<PersistenceVerification>(bridgeEvent.Payload, "verification", out var verification))
                {
                    SetPersistenceVerificationText(FormatPersistenceVerification(verification));
                    HandlePersistenceStatus(bridgeEvent, verification.Status);
                }
                else
                {
                    HandlePersistenceStatus(bridgeEvent, PayloadText(bridgeEvent.Payload, "status"));
                }
                break;
            case "persistence.updated":
            case "turn.persistence":
            case "turn.persistence.updated":
                HandlePersistenceStatus(bridgeEvent, PayloadText(bridgeEvent.Payload, "status"));
                break;
            case "thread.updated":
                if (PayloadText(bridgeEvent.Payload, "source") == "poll") ScheduleRecalibration();
                break;
            case "error":
                ActionError = UiText.UserError(PayloadText(bridgeEvent.Payload, "message", "Codex 操作失败"));
                break;
        }
        // Completion messages are not persistence evidence.  Only an explicit
        // persistence state can promote temporary cards into server history.
        HandlePersistenceStatus(bridgeEvent, PayloadText(bridgeEvent.Payload, "persistenceStatus"));
        RefreshCommandStates();
    }

    private void BeginLoadDetail(string threadId)
    {
        CancelDetailLoad();
        SelectedDetail = null;
        Timeline.Clear();
        _persistedTurns.Clear();
        ErrorText = "";
        ActionError = "";
        SetPersistenceVerificationText("");
        SetViewState("loading");
        var version = Interlocked.Increment(ref _selectionVersion);
        _detailCancellation = new CancellationTokenSource();
        _ = LoadDetailAsync(threadId, version, _detailCancellation.Token);
    }

    private async Task LoadDetailAsync(string threadId, long version, CancellationToken cancellationToken)
    {
        try
        {
            var detailTask = _api.GetThreadAsync(threadId, cancellationToken);
            var interactionsTask = _api.GetInteractionsAsync("pending", cancellationToken);
            var detail = await detailTask;
            InteractionListResponse? interactionList = null;
            try { interactionList = await interactionsTask; }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                _logs.Add("desktop", $"读取待处理交互失败：{exception.Message}");
            }
            if (!IsCurrentSelection(threadId, version)) return;
            SelectedDetail = detail;
            SetRuntime(detail.Runtime);
            RebuildTimeline(detail);
            PendingInteractions.Clear();
            foreach (var interaction in interactionList?.Interactions.Where(item => item.ThreadId == threadId) ?? [])
            {
                PendingInteractions.Add(new PendingInteractionViewModel(interaction));
            }
            SetViewState("details");
        }
        catch (OperationCanceledException) { }
        catch (Exception exception)
        {
            if (!IsCurrentSelection(threadId, version)) return;
            ErrorText = UiText.UserError(exception, "读取会话");
            SetViewState("error");
            _logs.Add("desktop", $"读取 Thread 详情失败：{exception.Message}");
        }
    }

    private async Task RetryAsync()
    {
        if (SelectedThread is null) await RefreshAsync();
        else BeginLoadDetail(SelectedThread.ThreadId);
    }

    private async Task VerifyPersistenceAsync()
    {
        if (SelectedThread is null) return;
        ActionError = "";
        try
        {
            var result = await _api.VerifyThreadPersistenceAsync(SelectedThread.ThreadId);
            SetPersistenceVerificationText(FormatPersistenceVerification(result));
            SetRuntime(new ThreadRuntime
            {
                ThreadId = result.ThreadId,
                TurnId = result.ExpectedTurnId,
                State = result.Status,
                Origin = "local",
                CanInterrupt = false,
                CanSend = result.Status is "persisted" or "persistence-failed" or "thread-mismatch" or "failed"
            });
            HandlePersistenceStatus(new BridgeEvent { ThreadId = result.ThreadId, TurnId = result.ExpectedTurnId }, result.Status);
            if (result.Status is "persisted")
            {
                if (!string.IsNullOrWhiteSpace(result.ExpectedTurnId)) _persistedTurns.Add(result.ExpectedTurnId);
                // The diagnostic API has already used a separate app-server.
                // Re-read official history to replace only matching temporary cards.
                ScheduleRecalibration();
            }
            else if (result.Status is "persistence-failed" or "thread-mismatch" or "failed")
            {
                MarkTemporaryTurn(result.ExpectedTurnId, result.Status == "thread-mismatch" ? "会话不一致：未确认写入" : "保存状态检查失败", true);
            }
        }
        catch (Exception exception)
        {
            ActionError = UiText.UserError(exception, "重新检查会话");
            SetPersistenceVerificationText(ActionError);
            _logs.Add("desktop", $"重新验证当前 Thread 持久化失败：{exception.Message}");
        }
    }

    private void SetPersistenceVerificationText(string value)
    {
        PersistenceVerificationText = value;
        OnPropertyChanged(nameof(PersistenceVerificationText));
        OnPropertyChanged(nameof(PersistenceVerificationVisibility));
    }

    private static string FormatPersistenceVerification(PersistenceVerification result) => result.Status switch
    {
        "persisted" => "会话保存状态正常。",
        "completed-unverified" => "任务已经完成，正在等待确认保存状态。",
        "thread-mismatch" => "检查到会话不一致，本次结果不会显示为当前会话的成功内容。",
        "persistence-failed" or "failed" => "无法确认会话是否已保存，请稍后重新检查。",
        _ => "会话检查已完成。"
    };

    private async Task SendAsync()
    {
        if (!CanSend || SelectedThread is null) return;
        var text = MessageText.Trim();
        _isSending = true;
        ActionError = "";
        RefreshCommandStates();
        try
        {
            var selectedThreadId = SelectedThread.ThreadId;
            var accepted = await _api.StartTurnAsync(selectedThreadId, new StartTurnRequest { Text = text, CollaborationMode = "default" });
            if (!string.Equals(accepted.ThreadId, selectedThreadId, StringComparison.Ordinal))
            {
                Timeline.Add(new TimelineEntry
                {
                    Key = $"pending-user-{Guid.NewGuid():N}", TurnId = accepted.TurnId,
                    Kind = "user", Title = "用户", Text = text,
                    Status = "会话不一致：未确认写入", IsTemporary = true, IsFailure = true
                });
                ActionError = "收到的结果不属于当前会话，因此没有显示为成功内容。详情已写入运行日志。";
                SetRuntime(new ThreadRuntime { ThreadId = selectedThreadId, TurnId = accepted.TurnId, State = "thread-mismatch", CanSend = false });
                return;
            }
            Timeline.Add(new TimelineEntry
            {
                Key = $"pending-user-{accepted.TurnId}", TurnId = accepted.TurnId,
                Kind = "user", Title = "用户", Text = text, Status = "待确认", IsTemporary = true
            });
            MessageText = "";
            SetRuntime(new ThreadRuntime
            {
                ThreadId = accepted.ThreadId, TurnId = accepted.TurnId, State = accepted.Status,
                Origin = "local", StartedAt = accepted.AcceptedAt, CanInterrupt = true, CanSend = false
            });
        }
        catch (BridgeApiException exception) when (exception.Code == "thread_busy")
        {
            ActionError = $"该会话当前忙碌（{StateText(exception.CurrentState)}），请等待任务结束。";
        }
        catch (Exception exception)
        {
            ActionError = UiText.UserError(exception, "发送消息");
            _logs.Add("desktop", $"发送消息失败（正文未记录）：{exception.Message}");
        }
        finally
        {
            _isSending = false;
            RefreshCommandStates();
        }
    }

    private async Task StopAsync()
    {
        if (!CanStop || SelectedThread is null) return;
        ActionError = "";
        try
        {
            var result = await _api.InterruptTurnAsync(SelectedThread.ThreadId, _runtime.TurnId);
            _runtime.State = result.Status;
            _runtime.CanInterrupt = false;
            SetRuntime(_runtime);
        }
        catch (Exception exception)
        {
            ActionError = UiText.UserError(exception, "停止任务");
            _logs.Add("desktop", $"停止 Turn 失败：{exception.Message}");
        }
    }

    private async Task RespondInteractionAsync(PendingInteractionViewModel? viewModel, string action)
    {
        if (viewModel is null || viewModel.IsResponding) return;
        if (action == "submit" && !viewModel.CanSubmit) return;
        viewModel.IsResponding = true;
        ActionError = "";
        try
        {
            var request = new InteractionResponse { Action = action };
            if (action == "deny") request.Message = "用户在本机桌面端拒绝";
            if (action == "submit") request.Answers = viewModel.GetAnswers();
            await _api.RespondInteractionAsync(viewModel.Id, request);
            PendingInteractions.Remove(viewModel);
        }
        catch (Exception exception)
        {
            viewModel.IsResponding = false;
            ActionError = UiText.UserError(exception, "提交处理结果");
            _logs.Add("desktop", $"响应交互请求失败：{exception.Message}");
        }
    }

    private void RebuildTimeline(ThreadDetail detail)
    {
        // Keep only unconfirmed cards.  A card is removed when authoritative
        // thread/read now contains the same turn, never merely on a streamed
        // assistant or turn/completed event.
        var allTemporary = Timeline.Where(entry => entry.IsTemporary).ToArray();
        var temporary = allTemporary
            .Where(entry => entry.IsFailure || !_persistedTurns.Contains(entry.TurnId) || !detail.Turns.Any(turn => turn.TurnId == entry.TurnId))
            .ToArray();
        var unverifiedTurnIds = temporary.Select(entry => entry.TurnId).Where(id => !string.IsNullOrWhiteSpace(id)).ToHashSet(StringComparer.Ordinal);
        Timeline.Clear();
        foreach (var turn in detail.Turns)
        {
            if (unverifiedTurnIds.Contains(turn.TurnId)) continue;
            foreach (var item in turn.Items)
            {
                var type = item.Type;
                var expandable = type is "commandExecution" or "fileChange" or "dynamicToolCall" or "mcpToolCall" or "webSearch" or "collabAgentToolCall";
                var known = type is "userMessage" or "agentMessage" or "commandExecution" or "fileChange" or "dynamicToolCall" or "mcpToolCall" or "webSearch" or "collabAgentToolCall";
                var kind = type switch { "userMessage" => "user", "agentMessage" => "assistant", "fileChange" => "file", _ when expandable => "tool", _ => "unknown" };
                var title = type switch
                {
                    "userMessage" => "用户",
                    "agentMessage" => "Codex",
                    "commandExecution" => "命令执行",
                    "fileChange" => "文件修改",
                    "dynamicToolCall" => "动态工具调用",
                    "mcpToolCall" => "MCP 工具调用",
                    "webSearch" => "网页搜索",
                    "collabAgentToolCall" => "协作代理工具调用",
                    _ => string.IsNullOrWhiteSpace(type) ? "未识别项目" : type
                };
                Timeline.Add(new TimelineEntry
                {
                    Key = $"{turn.TurnId}:{item.ItemId}", TurnId = turn.TurnId, ItemId = item.ItemId,
                    Timestamp = string.IsNullOrWhiteSpace(turn.UpdatedAt) ? turn.CreatedAt : turn.UpdatedAt,
                    Kind = kind, Title = title, Text = string.Join(Environment.NewLine, new[] { item.Text, item.Label, item.Output }.Where(value => !string.IsNullOrWhiteSpace(value))),
                    // Item state and turn state are different concepts.  Do not
                    // borrow turn.Status here: it made unknown cards look like a
                    // completed message.
					Status = string.IsNullOrWhiteSpace(item.Status) || item.Status.Equals("unknown", StringComparison.OrdinalIgnoreCase)
                        ? known ? (type is "userMessage" or "agentMessage" ? "消息" : "工具") : "未识别项目"
                        : UiText.Status(item.Status),
                    IsExpandable = expandable
                });
            }
        }
        foreach (var entry in temporary) Timeline.Add(entry);
    }

    private void AppendAssistantDelta(BridgeEvent bridgeEvent, string delta)
    {
        if (string.IsNullOrEmpty(delta)) return;
        var entry = FindTimeline(bridgeEvent) ?? CreateTimeline(bridgeEvent, "assistant", "Codex", false);
        entry.Text += delta;
        entry.Status = "待确认（正在回复）";
    }

    private void CompleteAssistant(BridgeEvent bridgeEvent)
    {
        var entry = FindTimeline(bridgeEvent) ?? CreateTimeline(bridgeEvent, "assistant", "Codex", false);
        var text = PayloadText(bridgeEvent.Payload, "text");
        if (!string.IsNullOrWhiteSpace(text)) entry.Text = text;
        entry.Status = "待确认（回复已完成）";
    }

    private void AppendToolOutput(BridgeEvent bridgeEvent, string delta)
    {
        if (string.IsNullOrEmpty(delta)) return;
        var entry = FindTimeline(bridgeEvent) ?? CreateTimeline(bridgeEvent, "tool", "工具调用", true);
        entry.Text += delta;
        entry.Status = "正在执行";
    }

    private void AddOrUpdateTimeline(BridgeEvent bridgeEvent, string kind, string title, string text, bool expandable, string status = "")
    {
        var entry = FindTimeline(bridgeEvent) ?? CreateTimeline(bridgeEvent, kind, title, expandable);
        if (!string.IsNullOrWhiteSpace(text)) entry.Text = text;
        if (!string.IsNullOrWhiteSpace(status)) entry.Status = status;
    }

    private TimelineEntry? FindTimeline(BridgeEvent bridgeEvent)
    {
        var key = EventKey(bridgeEvent);
        return Timeline.LastOrDefault(item => item.Key == key);
    }

    private TimelineEntry CreateTimeline(BridgeEvent bridgeEvent, string kind, string title, bool expandable)
    {
        var entry = new TimelineEntry
        {
            Key = EventKey(bridgeEvent), TurnId = bridgeEvent.TurnId, ItemId = bridgeEvent.ItemId, Timestamp = bridgeEvent.Timestamp,
            Kind = kind, Title = title, IsExpandable = expandable, IsTemporary = true
        };
        Timeline.Add(entry);
        return entry;
    }

    private void MarkTurnCompletedUnverified(BridgeEvent bridgeEvent)
    {
        MarkTemporaryTurn(bridgeEvent.TurnId, "待确认（任务已完成，正在确认保存状态）", false);
    }

    private void MarkTemporaryTurn(string turnId, string status, bool failure)
    {
        if (string.IsNullOrWhiteSpace(turnId)) return;
        foreach (var entry in Timeline.Where(entry => entry.TurnId == turnId && entry.IsTemporary))
        {
            entry.Status = status;
            entry.IsFailure = failure;
        }
    }

    private void HandlePersistenceStatus(BridgeEvent bridgeEvent, string status)
    {
        if (string.IsNullOrWhiteSpace(status) || string.IsNullOrWhiteSpace(bridgeEvent.TurnId)) return;
        switch (status)
        {
            case "persisted":
                _persistedTurns.Add(bridgeEvent.TurnId);
                ScheduleRecalibration();
                break;
            case "completed-unverified":
                MarkTemporaryTurn(bridgeEvent.TurnId, "待确认（已完成，正在确认保存状态）", false);
                break;
            case "persistence-failed":
                MarkTemporaryTurn(bridgeEvent.TurnId, "保存状态检查失败", true);
                SetRuntime(new ThreadRuntime { ThreadId = bridgeEvent.ThreadId, TurnId = bridgeEvent.TurnId, State = "persistence-failed", CanSend = true });
                break;
            case "thread-mismatch":
                MarkTemporaryTurn(bridgeEvent.TurnId, "会话不一致：未确认写入", true);
                ActionError = "检查到会话不一致，本次结果不会显示为当前会话的成功内容。";
                SetRuntime(new ThreadRuntime { ThreadId = bridgeEvent.ThreadId, TurnId = bridgeEvent.TurnId, State = "thread-mismatch", CanSend = true });
                break;
            case "failed":
                MarkTemporaryTurn(bridgeEvent.TurnId, "发送失败", true);
                break;
        }
    }

    private static string EventKey(BridgeEvent bridgeEvent) =>
        $"{bridgeEvent.TurnId}:{(string.IsNullOrWhiteSpace(bridgeEvent.ItemId) ? bridgeEvent.EventType : bridgeEvent.ItemId)}";

    private void SetRuntime(ThreadRuntime runtime)
    {
        _runtime = runtime;
        if (SelectedDetail is not null) SelectedDetail.Runtime = runtime;
        OnPropertyChanged(nameof(RuntimeStateText));
        OnPropertyChanged(nameof(RuntimeSourceText));
        OnPropertyChanged(nameof(CurrentTurnId));
        RefreshCommandStates();
    }

    private void SetViewState(string state)
    {
        if (_viewState == state) return;
        _viewState = state;
        OnPropertyChanged(nameof(EmptyVisibility));
        OnPropertyChanged(nameof(LoadingVisibility));
        OnPropertyChanged(nameof(ErrorVisibility));
        OnPropertyChanged(nameof(DetailsVisibility));
    }

    private void RefreshCommandStates()
    {
        OnPropertyChanged(nameof(CanSend));
        OnPropertyChanged(nameof(CanStop));
        OnPropertyChanged(nameof(CanVerifyPersistence));
        _sendCommand.RaiseCanExecuteChanged();
        _stopCommand.RaiseCanExecuteChanged();
        _retryCommand.RaiseCanExecuteChanged();
        _verifyPersistenceCommand.RaiseCanExecuteChanged();
    }

    private void CancelDetailLoad()
    {
        Interlocked.Increment(ref _selectionVersion);
        _detailCancellation?.Cancel();
        _detailCancellation?.Dispose();
        _detailCancellation = null;
    }

    private bool IsCurrentSelection(string threadId, long version) =>
        version == Interlocked.Read(ref _selectionVersion) && SelectedThread?.ThreadId == threadId;

    private void ScheduleRecalibration()
    {
        _reloadCancellation?.Cancel();
        _reloadCancellation?.Dispose();
        _reloadCancellation = new CancellationTokenSource();
        var token = _reloadCancellation.Token;
        var threadId = SelectedThread?.ThreadId;
        if (string.IsNullOrWhiteSpace(threadId)) return;
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(500, token);
                var detail = await _api.GetThreadAsync(threadId, token);
                var interactions = await _api.GetInteractionsAsync("pending", token);
                await Application.Current.Dispatcher.InvokeAsync(() =>
                {
                    if (SelectedThread?.ThreadId != threadId) return;
                    SelectedDetail = detail;
                    SetRuntime(detail.Runtime);
                    RebuildTimeline(detail);
                    PendingInteractions.Clear();
                    foreach (var item in interactions.Interactions.Where(item => item.ThreadId == threadId))
                        PendingInteractions.Add(new PendingInteractionViewModel(item));
                    SetViewState("details");
                });
            }
            catch (OperationCanceledException) { }
            catch (Exception exception)
            {
                _logs.Add("desktop", $"重新校准 Thread 状态失败：{exception.Message}");
            }
        }, token);
    }

    private static bool TryPayload<T>(JsonElement payload, string property, out T value)
    {
        value = default!;
        if (payload.ValueKind != JsonValueKind.Object || !payload.TryGetProperty(property, out var element)) return false;
        try
        {
            value = element.Deserialize<T>(JsonOptions)!;
            return value is not null;
        }
        catch (JsonException) { return false; }
    }

    private static string PayloadText(JsonElement payload, string property, string fallback = "")
    {
        if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String)
            return value.GetString() ?? fallback;
        return fallback;
    }

    private static string PayloadTextInObject(JsonElement payload, string objectProperty, string property, string fallback = "")
    {
        if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty(objectProperty, out var nested))
            return PayloadText(nested, property, fallback);
        return fallback;
    }

    private static string ToolTitle(JsonElement payload)
    {
        var type = PayloadText(payload, "type");
        return type switch { "commandExecution" => "命令执行", "webSearch" => "网页搜索", _ => PayloadText(payload, "name", "工具调用") };
    }

    private static string ToolBody(JsonElement payload) => string.Join(Environment.NewLine,
        new[] { PayloadText(payload, "command"), PayloadText(payload, "cwd"), PayloadText(payload, "aggregatedOutput"), PayloadText(payload, "output") }
            .Where(value => !string.IsNullOrWhiteSpace(value)));

    private static string FileBody(JsonElement payload)
    {
        var direct = string.Join(Environment.NewLine, new[] { PayloadText(payload, "path"), PayloadText(payload, "diff") }.Where(value => !string.IsNullOrWhiteSpace(value)));
        if (!string.IsNullOrWhiteSpace(direct)) return direct;
        if (payload.ValueKind == JsonValueKind.Object && payload.TryGetProperty("changes", out var changes)) return changes.ToString();
        return "文件状态已更新";
    }

    private bool MatchesSearch(object item)
    {
        if (item is not ThreadSummary thread || string.IsNullOrWhiteSpace(SearchText)) return true;
        return thread.Title.Contains(SearchText, StringComparison.CurrentCultureIgnoreCase) ||
               thread.Cwd.Contains(SearchText, StringComparison.CurrentCultureIgnoreCase) ||
               thread.ThreadId.Contains(SearchText, StringComparison.OrdinalIgnoreCase);
    }

    private void CopyThreadId()
    {
        if (SelectedDetail is null) return;
        try { Clipboard.SetText(SelectedDetail.ThreadId); }
        catch (Exception exception) { ActionError = $"复制失败：{exception.Message}"; }
    }

    private void CopyThreadPrefix()
	{
		if (SelectedDetail is null || SelectedDetail.Number < 1) return;
		try { Clipboard.SetText($"#{SelectedDetail.Number}"); }
		catch (Exception exception) { ActionError = $"复制会话前缀失败：{exception.Message}"; }
	}

    private void OpenDirectory()
    {
        var directory = SelectedDetail?.Cwd;
        if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
        {
            ActionError = "项目目录不存在或当前不可访问。";
            return;
        }
        var info = new ProcessStartInfo("explorer.exe") { UseShellExecute = true };
        info.ArgumentList.Add(directory);
        Process.Start(info);
    }

    private static string StateText(string state) => UiText.Status(state);
}
