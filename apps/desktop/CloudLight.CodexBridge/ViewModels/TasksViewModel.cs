using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Text.Json;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class TasksViewModel : ObservableObject
{
    private const int MaximumTasks = 100;
    private const int MaximumCachedActionResults = 128;
    private readonly BridgeApiClient _api;
    private readonly LogService _logs;
    private readonly SessionsViewModel _sessions;
    private readonly OpenClawViewModel _openClaw;
    private readonly AsyncRelayCommand _refreshCommand;
    private readonly AsyncRelayCommand _createCommand;
    private readonly AsyncRelayCommand _continueCommand;
    private readonly AsyncRelayCommand _retryCommand;
    private readonly AsyncRelayCommand _cancelCommand;
    private readonly AsyncRelayCommand _answerCommand;
    private readonly RelayCommand _actionCommand;
    private readonly RelayCommand _confirmActionCommand;
    private readonly RelayCommand _cancelActionConfirmationCommand;
    private string _searchText = "";
    private string _statusFilter = "";
    private BridgeTask? _selectedTask;
    private ProjectModel? _selectedProject;
    private string _newTitle = "";
    private string _newDescription = "";
    private string _newBackend = "codex";
    private int _newConversationNumber;
    private string _continueText = "";
    private string _taskAnswerText = "";
    private string _errorText = "";
    private bool _busy;
    private TaskActionModel? _pendingConfirmationAction;
    private TaskActionResultModel? _actionResult;
    private readonly Dictionary<int, TaskActionResultModel> _actionResults = [];

    public TasksViewModel(BridgeApiClient api, LogService logs, SessionsViewModel sessions, OpenClawViewModel openClaw)
    {
        _api = api; _logs = logs; _sessions = sessions; _openClaw = openClaw;
        TasksView = CollectionViewSource.GetDefaultView(Tasks);
        TasksView.Filter = MatchesFilter;
        ConversationView = CollectionViewSource.GetDefaultView(ConversationChoices);
        ConversationView.Filter = MatchesConversation;
        _refreshCommand = new AsyncRelayCommand(RefreshAsync, () => !Busy);
        _createCommand = new AsyncRelayCommand(CreateAsync, () => !Busy && SelectedProject is not null && !string.IsNullOrWhiteSpace(NewDescription));
        _continueCommand = new AsyncRelayCommand(ContinueAsync, () => !Busy && SelectedTask is not null && !string.IsNullOrWhiteSpace(ContinueText));
        _retryCommand = new AsyncRelayCommand(RetryAsync, () => !Busy && SelectedTask is { Status: "failed" or "interrupted" });
        _cancelCommand = new AsyncRelayCommand(CancelAsync, () => !Busy && SelectedTask is { Status: "queued" or "routing" or "running" or "waiting-input" });
        _answerCommand = new AsyncRelayCommand(AnswerAsync, () => !Busy && SelectedTask is { Status: "waiting-input" } && !string.IsNullOrWhiteSpace(TaskAnswerText));
        _actionCommand = new RelayCommand(parameter => _ = ExecuteActionAsync(parameter as TaskActionModel), parameter => !Busy && PendingConfirmationAction is null && parameter is TaskActionModel action && (!action.Id.Equals("continue", StringComparison.OrdinalIgnoreCase) || !string.IsNullOrWhiteSpace(ContinueText)));
        _confirmActionCommand = new RelayCommand(_ => _ = ConfirmActionAsync(), _ => !Busy && PendingConfirmationAction is not null);
        _cancelActionConfirmationCommand = new RelayCommand(_ => CancelActionConfirmation(), _ => PendingConfirmationAction is not null);
        RefreshCommand = _refreshCommand;
        CreateCommand = _createCommand;
        ContinueCommand = _continueCommand;
        RetryCommand = _retryCommand;
        CancelCommand = _cancelCommand;
        AnswerCommand = _answerCommand;
        ActionCommand = _actionCommand;
        ConfirmActionCommand = _confirmActionCommand;
        CancelActionConfirmationCommand = _cancelActionConfirmationCommand;
        OpenConversationCommand = new RelayCommand(_ => OpenConversation());
        sessions.Threads.CollectionChanged += (_, _) => RefreshConversationChoices();
        openClaw.Sessions.CollectionChanged += (_, _) => RefreshConversationChoices();
        RefreshConversationChoices();
    }

    public ObservableCollection<BridgeTask> Tasks { get; } = [];
    public ObservableCollection<ProjectModel> Projects { get; } = [];
    public ObservableCollection<ConversationChoice> ConversationChoices { get; } = [];
    public ObservableCollection<TaskActionModel> AvailableActions { get; } = [];
    public ICollectionView TasksView { get; }
    public ICollectionView ConversationView { get; }
    public ICommand RefreshCommand { get; }
    public ICommand CreateCommand { get; }
    public ICommand ContinueCommand { get; }
    public ICommand RetryCommand { get; }
    public ICommand CancelCommand { get; }
    public ICommand AnswerCommand { get; }
    public ICommand ActionCommand { get; }
    public ICommand ConfirmActionCommand { get; }
    public ICommand CancelActionConfirmationCommand { get; }
    public ICommand OpenConversationCommand { get; }
    public event Action<string, int>? ConversationRequested;

    public BridgeTask? SelectedTask
    {
        get => _selectedTask;
        set
        {
            if (!SetProperty(ref _selectedTask, value)) return;
            PendingConfirmationAction = null;
            ActionResult = value is not null && _actionResults.TryGetValue(value.TaskNumber, out var result) ? result : null;
            AvailableActions.Clear();
            RefreshCommandStates();
            if (value is not null) _ = RefreshAvailableActionsAsync(value.TaskNumber);
        }
    }

    public ProjectModel? SelectedProject
    {
        get => _selectedProject;
        set
        {
            if (SetProperty(ref _selectedProject, value))
            {
                if (value is not null) NewBackend = value.DefaultBackend;
                NewConversationNumber = value?.DefaultConversationNumber ?? 0;
                RefreshConversationChoices();
                RefreshCommandStates();
            }
        }
    }

    public string SearchText
    {
        get => _searchText;
        set
        {
            if (SetProperty(ref _searchText, value)) TasksView.Refresh();
        }
    }

    public string StatusFilter
    {
        get => _statusFilter;
        set
        {
            if (SetProperty(ref _statusFilter, value ?? "")) TasksView.Refresh();
        }
    }

    public string NewTitle { get => _newTitle; set => SetProperty(ref _newTitle, value); }
    public string NewDescription
    {
        get => _newDescription;
        set { if (SetProperty(ref _newDescription, value)) RefreshCommandStates(); }
    }

    public string NewBackend
    {
        get => _newBackend;
        set
        {
            if (!SetProperty(ref _newBackend, value)) return;
            RefreshConversationChoices();
        }
    }

    public int NewConversationNumber { get => _newConversationNumber; set => SetProperty(ref _newConversationNumber, value); }
    public string ContinueText
    {
        get => _continueText;
        set { if (SetProperty(ref _continueText, value)) RefreshCommandStates(); }
    }

    public string TaskAnswerText
    {
        get => _taskAnswerText;
        set { if (SetProperty(ref _taskAnswerText, value)) RefreshCommandStates(); }
    }

    public string ErrorText
    {
        get => _errorText;
        private set
        {
            if (SetProperty(ref _errorText, value)) OnPropertyChanged(nameof(ErrorVisibility));
        }
    }

    public bool Busy { get => _busy; private set { if (SetProperty(ref _busy, value)) { OnPropertyChanged(nameof(BusyVisibility)); RefreshCommandStates(); } } }
    public Visibility BusyVisibility => Busy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility ErrorVisibility => string.IsNullOrWhiteSpace(ErrorText) ? Visibility.Collapsed : Visibility.Visible;
    public Visibility TaskAnswerVisibility => SelectedTask is { Status: "waiting-input" } && !string.IsNullOrWhiteSpace(SelectedTask.PendingInteractionId) ? Visibility.Visible : Visibility.Collapsed;
    public TaskActionModel? PendingConfirmationAction
    {
        get => _pendingConfirmationAction;
        private set
        {
            if (SetProperty(ref _pendingConfirmationAction, value))
            {
                OnPropertyChanged(nameof(ActionConfirmationVisibility));
                _confirmActionCommand.RaiseCanExecuteChanged();
                _cancelActionConfirmationCommand.RaiseCanExecuteChanged();
                _actionCommand.RaiseCanExecuteChanged();
            }
        }
    }
    public Visibility ActionConfirmationVisibility => PendingConfirmationAction is null ? Visibility.Collapsed : Visibility.Visible;
    public TaskActionResultModel? ActionResult
    {
        get => _actionResult;
        private set
        {
            if (SetProperty(ref _actionResult, value))
            {
                OnPropertyChanged(nameof(ActionResultVisibility));
                OnPropertyChanged(nameof(ActionResultHeader));
                OnPropertyChanged(nameof(ActionResultText));
            }
        }
    }
    public Visibility ActionResultVisibility => ActionResult is null ? Visibility.Collapsed : Visibility.Visible;
    public string ActionResultHeader => ActionResult is null ? "" : $"{ActionResult.Action} · {ActionResult.TimeDisplay}";
    public string ActionResultText => ActionResult is null ? "" : string.IsNullOrWhiteSpace(ActionResult.Error) ? ActionResult.Result : $"错误：{ActionResult.Error}";
    public string RunningCount => Tasks.Count(task => task.Status == "running").ToString();
    public string WaitingCount => Tasks.Count(task => task.Status == "waiting-input").ToString();
    public string CompletedCount => Tasks.Count(task => task.Status == "completed").ToString();
    public string FailedCount => Tasks.Count(task => task.Status == "failed").ToString();
    public string TaskCountsText => $"运行中 {RunningCount}  ·  等待输入 {WaitingCount}  ·  已完成 {CompletedCount}  ·  失败 {FailedCount}";

    public async Task RefreshAsync()
    {
        Busy = true;
        ErrorText = "";
        var selectedNumber = SelectedTask?.TaskNumber;
        try
        {
            var result = await _api.GetTasksAsync(StatusFilter, SearchText, 100);
            var projects = await _api.GetProjectsAsync();
            Tasks.Clear();
            foreach (var task in result.Tasks) Tasks.Add(task);
            Projects.Clear();
            foreach (var project in projects.Projects) Projects.Add(project);
            SelectedTask = selectedNumber is null ? Tasks.FirstOrDefault() : Tasks.FirstOrDefault(task => task.TaskNumber == selectedNumber) ?? Tasks.FirstOrDefault();
            if (SelectedProject is not null) SelectedProject = Projects.FirstOrDefault(project => project.ProjectId == SelectedProject.ProjectId);
            NotifyCounts();
        }
        catch (Exception exception) { ErrorText = UiText.UserError(exception, "读取任务"); _logs.Add("tasks", exception.Message); }
        finally { Busy = false; }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (!bridgeEvent.EventType.StartsWith("task.", StringComparison.OrdinalIgnoreCase)) return;
        if (TryTask(bridgeEvent.Payload, out var task))
        {
            var existing = Tasks.FirstOrDefault(item => item.TaskNumber == task.TaskNumber);
            if (existing is not null) Tasks[Tasks.IndexOf(existing)] = task;
            else Tasks.Insert(0, task);
            while (Tasks.Count > MaximumTasks) Tasks.RemoveAt(Tasks.Count - 1);
            SelectedTask = Tasks.FirstOrDefault(item => item.TaskNumber == SelectedTask?.TaskNumber) ?? SelectedTask;
            TasksView.Refresh();
            NotifyCounts();
        }
        else _ = RefreshAsync();
    }

    private async Task CreateAsync()
    {
        if (SelectedProject is null) return;
        await RunMutationAsync(async () =>
        {
            var task = await _api.CreateTaskAsync(new TaskInput
            {
                Title = NewTitle, Description = NewDescription, ProjectId = SelectedProject.ProjectId,
                Backend = NewBackend, ConversationNumber = NewConversationNumber
            });
            NewTitle = ""; NewDescription = ""; NewConversationNumber = 0;
            await RefreshAsync();
            SelectedTask = Tasks.FirstOrDefault(item => item.TaskNumber == task.TaskNumber) ?? task;
        }, "创建任务");
    }

    private async Task ContinueAsync()
    {
        if (SelectedTask is null) return;
        await RunMutationAsync(async () =>
        {
            var task = await _api.ContinueTaskAsync(SelectedTask.TaskNumber, new TaskInput { Description = ContinueText });
            ContinueText = "";
            await RefreshAsync();
            SelectedTask = Tasks.FirstOrDefault(item => item.TaskNumber == task.TaskNumber) ?? task;
        }, "继续任务");
    }

    private async Task RetryAsync()
    {
        if (SelectedTask is null) return;
        await RunMutationAsync(async () =>
        {
            var task = await _api.RetryTaskAsync(SelectedTask.TaskNumber, new TaskInput());
            await RefreshAsync();
            SelectedTask = Tasks.FirstOrDefault(item => item.TaskNumber == task.TaskNumber) ?? task;
        }, "重试任务");
    }

    private async Task CancelAsync()
    {
        if (SelectedTask is null) return;
        await RunMutationAsync(async () =>
        {
            await _api.CancelTaskAsync(SelectedTask.TaskNumber);
            await RefreshAsync();
        }, "取消任务");
    }

    private async Task AnswerAsync()
    {
        var task = SelectedTask;
        if (task is null || string.IsNullOrWhiteSpace(task.PendingInteractionId)) return;
        await RunMutationAsync(async () =>
        {
            var pending = await _api.GetInteractionsAsync("pending");
            var interaction = pending.Interactions.FirstOrDefault(item => item.Id == task.PendingInteractionId)
                ?? throw new InvalidOperationException("原始交互请求已不存在，请在 Codex 页面重新检查。" );
            var answers = BuildAnswers(interaction, TaskAnswerText);
            await _api.RespondInteractionAsync(interaction.Id, new InteractionResponse { Action = "submit", Answers = answers });
            TaskAnswerText = "";
            await RefreshAsync();
        }, "提交回答");
    }

    private async Task RefreshAvailableActionsAsync(int taskNumber)
    {
        try
        {
            var response = await _api.GetTaskActionsAsync(taskNumber);
            if (SelectedTask?.TaskNumber != taskNumber) return;
            AvailableActions.Clear();
            foreach (var action in response.Actions.Where(action => action.Available)) AvailableActions.Add(action);
        }
        catch (Exception exception)
        {
            if (SelectedTask?.TaskNumber == taskNumber) AvailableActions.Clear();
            _logs.Add("tasks", $"读取快捷操作失败：{exception.Message}");
        }
    }

    private async Task ExecuteActionAsync(TaskActionModel? action)
    {
        if (action is null || SelectedTask is null || Busy) return;
        if (action.RequiresConfirmation)
        {
            PendingConfirmationAction = action;
            return;
        }
        await RunTaskActionAsync(action, false);
    }

    public async Task ExecuteQuickActionAsync(int taskNumber, string actionId)
    {
        var task = Tasks.FirstOrDefault(item => item.TaskNumber == taskNumber);
        if (task is null || !actionId.Equals("cancel", StringComparison.OrdinalIgnoreCase)) return;
        SelectedTask = task;
        await ExecuteActionAsync(new TaskActionModel { Id = actionId, DisplayName = "停止任务" });
    }

    private async Task ConfirmActionAsync()
    {
        var action = PendingConfirmationAction;
        if (action is null) return;
        PendingConfirmationAction = null;
        await RunTaskActionAsync(action, true);
    }

    private void CancelActionConfirmation() => PendingConfirmationAction = null;

    private async Task RunTaskActionAsync(TaskActionModel action, bool confirmed)
    {
        var selected = SelectedTask;
        if (selected is null) return;
        await RunMutationAsync(async () =>
        {
            var response = await _api.ExecuteTaskActionAsync(selected.TaskNumber, new TaskActionRequest
            {
                ActionId = action.Id,
                Text = action.Id.Equals("continue", StringComparison.OrdinalIgnoreCase) ? ContinueText : "",
                Full = action.Id.Equals("diff", StringComparison.OrdinalIgnoreCase),
                Confirmed = confirmed
            });
            if (response.Result is not null)
            {
                _actionResults[selected.TaskNumber] = response.Result;
                while (_actionResults.Count > MaximumCachedActionResults)
                {
                    var oldest = _actionResults
                        .OrderBy(pair => DateTimeOffset.TryParse(pair.Value.Time, out var time) ? time : DateTimeOffset.MinValue)
                        .First();
                    _actionResults.Remove(oldest.Key);
                }
                if (SelectedTask?.TaskNumber == selected.TaskNumber) ActionResult = response.Result;
                if (action.Id.Equals("open", StringComparison.OrdinalIgnoreCase)) OpenConversation();
            }
            if (response.Task is not null)
            {
                ContinueText = "";
                await RefreshAsync();
                SelectedTask = Tasks.FirstOrDefault(item => item.TaskNumber == response.Task.TaskNumber) ?? response.Task;
            }
            else
            {
                await RefreshAvailableActionsAsync(selected.TaskNumber);
            }
        }, action.DisplayName);
    }

    private void RefreshConversationChoices()
    {
        var selected = NewConversationNumber;
        ConversationChoices.Clear();
        ConversationChoices.Add(new ConversationChoice { Backend = NewBackend, Number = 0 });
        foreach (var thread in _sessions.Threads.Where(thread => thread.Number > 0))
            ConversationChoices.Add(new ConversationChoice { Backend = "codex", Number = thread.Number });
        foreach (var session in _openClaw.Sessions.Where(session => session.Number > 0))
            ConversationChoices.Add(new ConversationChoice { Backend = "openclaw", Number = session.Number });
        ConversationView.Refresh();
        if (selected > 0 && !ConversationChoices.Any(choice => choice.Number == selected && choice.Backend.Equals(NewBackend, StringComparison.OrdinalIgnoreCase)))
            NewConversationNumber = 0;
    }

    private bool MatchesConversation(object value) => value is ConversationChoice choice && choice.Backend.Equals(NewBackend, StringComparison.OrdinalIgnoreCase);

    private void OpenConversation()
    {
        if (SelectedTask is { ConversationNumber: > 0 } task) ConversationRequested?.Invoke(task.Backend, task.ConversationNumber);
    }

    private async Task RunMutationAsync(Func<Task> action, string operation)
    {
        if (Busy) return;
        Busy = true; ErrorText = "";
        try { await action(); }
        catch (Exception exception) { ErrorText = UiText.UserError(exception, operation); _logs.Add("tasks", $"{operation}失败：{exception.Message}"); }
        finally { Busy = false; }
    }

    private bool MatchesFilter(object value)
    {
        if (value is not BridgeTask task) return false;
        if (!string.IsNullOrWhiteSpace(StatusFilter) && task.Status != StatusFilter) return false;
        if (string.IsNullOrWhiteSpace(SearchText)) return true;
        var search = SearchText.Trim();
        return task.NumberLabel.Contains(search, StringComparison.OrdinalIgnoreCase) || task.Title.Contains(search, StringComparison.OrdinalIgnoreCase) || task.ProjectNameSnapshot.Contains(search, StringComparison.OrdinalIgnoreCase);
    }

    private void RefreshCommandStates()
    {
        _refreshCommand.RaiseCanExecuteChanged(); _createCommand.RaiseCanExecuteChanged(); _continueCommand.RaiseCanExecuteChanged(); _retryCommand.RaiseCanExecuteChanged(); _cancelCommand.RaiseCanExecuteChanged(); _answerCommand.RaiseCanExecuteChanged(); _actionCommand.RaiseCanExecuteChanged(); _confirmActionCommand.RaiseCanExecuteChanged(); _cancelActionConfirmationCommand.RaiseCanExecuteChanged();
        OnPropertyChanged(nameof(TaskAnswerVisibility));
    }

    private void NotifyCounts()
    {
        OnPropertyChanged(nameof(RunningCount)); OnPropertyChanged(nameof(WaitingCount)); OnPropertyChanged(nameof(CompletedCount)); OnPropertyChanged(nameof(FailedCount)); OnPropertyChanged(nameof(TaskCountsText));
    }

    private static bool TryTask(JsonElement payload, out BridgeTask task)
    {
        task = new BridgeTask();
        if (payload.ValueKind != JsonValueKind.Object || !payload.TryGetProperty("task", out var value)) return false;
        try { task = value.Deserialize<BridgeTask>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new BridgeTask(); return task.TaskNumber > 0; }
        catch (JsonException) { return false; }
    }

    private static Dictionary<string, string[]> BuildAnswers(PendingInteraction interaction, string answerText)
    {
        var questions = interaction.Questions ?? [];
        if (questions.Count == 0) return [];
        var lines = answerText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length == 0) throw new InvalidOperationException("回答不能为空。" );
        if (questions.Count > 1 && lines.Length < questions.Count)
            throw new InvalidOperationException("该交互包含多个问题，请每行回答一个问题。" );
        var result = new Dictionary<string, string[]>();
        for (var index = 0; index < questions.Count; index++)
        {
            var answer = questions.Count == 1 ? answerText.Trim() : lines[index];
            result[questions[index].Id] = ParseQuestionAnswer(questions[index], answer);
        }
        return result;
    }

    private static string[] ParseQuestionAnswer(InteractionQuestion question, string answer)
    {
        if (question.Type.Equals("single-choice", StringComparison.OrdinalIgnoreCase))
            return [ResolveOption(question, answer)];
        if (question.Type.Equals("multiple-choice", StringComparison.OrdinalIgnoreCase))
        {
            var values = answer.Replace('，', ',').Replace('、', ',').Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return values.Select(value => ResolveOption(question, value)).Distinct(StringComparer.Ordinal).ToArray();
        }
        if (question.Required && string.IsNullOrWhiteSpace(answer)) throw new InvalidOperationException("必填问题不能为空。" );
        return [answer.Trim()];
    }

    private static string ResolveOption(InteractionQuestion question, string value)
    {
        if (int.TryParse(value.Trim(), out var index) && index >= 1 && index <= question.Options.Count)
            return question.Options[index - 1].Value;
        var option = question.Options.FirstOrDefault(item => item.Value.Equals(value.Trim(), StringComparison.OrdinalIgnoreCase) || item.Label.Equals(value.Trim(), StringComparison.OrdinalIgnoreCase));
        return option?.Value ?? throw new InvalidOperationException($"问题“{question.Text}”的选项无效。" );
    }
}
