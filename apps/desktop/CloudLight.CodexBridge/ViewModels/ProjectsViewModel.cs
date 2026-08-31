using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Input;
using CloudLight.CodexBridge.Infrastructure;
using CloudLight.CodexBridge.Models;
using CloudLight.CodexBridge.Services;
using Forms = System.Windows.Forms;

namespace CloudLight.CodexBridge.ViewModels;

public sealed class ConversationChoice
{
    public string Backend { get; init; } = "codex";
    public int Number { get; init; }
    public string Label => Number < 1 ? "自动（按项目策略）" : $"[{(Backend.Equals("openclaw", StringComparison.OrdinalIgnoreCase) ? "OpenClaw" : "Codex")}] #{Number}";
}

public sealed class ProjectsViewModel : ObservableObject
{
    private readonly BridgeApiClient _api;
    private readonly LogService _logs;
    private readonly SessionsViewModel _sessions;
    private readonly OpenClawViewModel _openClaw;
    private readonly AsyncRelayCommand _refreshCommand;
    private readonly AsyncRelayCommand _saveCommand;
    private readonly AsyncRelayCommand _deleteCommand;
    private readonly AsyncRelayCommand _gitRefreshCommand;
    private ProjectModel? _selectedProject;
    private string _name = "";
    private string _aliasesText = "";
    private string _workingDirectory = "";
    private string _description = "";
    private string _defaultBackend = "codex";
    private int? _defaultConversationNumber;
    private bool _autoCreateConversation;
    private string _reuseStrategy = "default";
    private bool _enabled = true;
    private string _tagsText = "";
    private string _errorText = "";
    private bool _busy;
    private bool _gitStatusBusy;
    private ProjectGitStatusModel _gitStatus = new();

    public ProjectsViewModel(BridgeApiClient api, LogService logs, SessionsViewModel sessions, OpenClawViewModel openClaw)
    {
        _api = api; _logs = logs; _sessions = sessions; _openClaw = openClaw;
        ConversationView = CollectionViewSource.GetDefaultView(ConversationChoices);
        ConversationView.Filter = value => value is ConversationChoice choice && choice.Backend.Equals(DefaultBackend, StringComparison.OrdinalIgnoreCase);
        _refreshCommand = new AsyncRelayCommand(RefreshAsync, () => !Busy);
        _saveCommand = new AsyncRelayCommand(SaveAsync, () => !Busy && !string.IsNullOrWhiteSpace(Name));
        _deleteCommand = new AsyncRelayCommand(DeleteAsync, () => !Busy && SelectedProject is not null);
        _gitRefreshCommand = new AsyncRelayCommand(RefreshGitStatusAsync, () => !GitStatusBusy && SelectedProject is not null);
        RefreshCommand = _refreshCommand; SaveCommand = _saveCommand; DeleteCommand = _deleteCommand; RefreshGitStatusCommand = _gitRefreshCommand;
        ChooseDirectoryCommand = new RelayCommand(_ => ChooseDirectory());
        NewCommand = new RelayCommand(_ => NewProject());
        sessions.Threads.CollectionChanged += (_, _) => RefreshConversationChoices();
        openClaw.Sessions.CollectionChanged += (_, _) => RefreshConversationChoices();
    }

    public ObservableCollection<ProjectModel> Projects { get; } = [];
    public ObservableCollection<ConversationChoice> ConversationChoices { get; } = [];
    public ICollectionView ConversationView { get; }
    public ICommand RefreshCommand { get; }
    public ICommand SaveCommand { get; }
    public ICommand DeleteCommand { get; }
    public ICommand RefreshGitStatusCommand { get; }
    public ICommand ChooseDirectoryCommand { get; }
    public ICommand NewCommand { get; }

    public ProjectModel? SelectedProject
    {
        get => _selectedProject;
        set
        {
            if (!SetProperty(ref _selectedProject, value)) return;
            if (value is null)
            {
                ClearForm();
                GitStatus = new();
            }
            else
            {
                LoadForm(value);
                _ = RefreshGitStatusAsync(value.ProjectId);
            }
            _deleteCommand.RaiseCanExecuteChanged();
            _gitRefreshCommand.RaiseCanExecuteChanged();
        }
    }

    public string Name { get => _name; set { if (SetProperty(ref _name, value)) _saveCommand.RaiseCanExecuteChanged(); } }
    public string AliasesText { get => _aliasesText; set => SetProperty(ref _aliasesText, value); }
    public string WorkingDirectory { get => _workingDirectory; set => SetProperty(ref _workingDirectory, value); }
    public string Description { get => _description; set => SetProperty(ref _description, value); }

    public string DefaultBackend
    {
        get => _defaultBackend;
        set
        {
            var normalized = value.Equals("openclaw", StringComparison.OrdinalIgnoreCase) ? "openclaw" : "codex";
            if (!SetProperty(ref _defaultBackend, normalized)) return;
            if (_defaultConversationNumber is not null && !ConversationChoices.Any(choice => choice.Backend.Equals(normalized, StringComparison.OrdinalIgnoreCase) && choice.Number == _defaultConversationNumber))
                DefaultConversationNumber = null;
            RefreshConversationChoices();
        }
    }

    public int? DefaultConversationNumber { get => _defaultConversationNumber; set => SetProperty(ref _defaultConversationNumber, value); }
    public bool AutoCreateConversation { get => _autoCreateConversation; set => SetProperty(ref _autoCreateConversation, value); }
    public string ReuseStrategy { get => _reuseStrategy; set => SetProperty(ref _reuseStrategy, value); }
    public bool Enabled { get => _enabled; set => SetProperty(ref _enabled, value); }
    public string TagsText { get => _tagsText; set => SetProperty(ref _tagsText, value); }
    public bool Busy { get => _busy; private set { if (SetProperty(ref _busy, value)) { OnPropertyChanged(nameof(BusyVisibility)); _refreshCommand.RaiseCanExecuteChanged(); _saveCommand.RaiseCanExecuteChanged(); _deleteCommand.RaiseCanExecuteChanged(); } } }
    public Visibility BusyVisibility => Busy ? Visibility.Visible : Visibility.Collapsed;
    public string ErrorText { get => _errorText; private set { if (SetProperty(ref _errorText, value)) OnPropertyChanged(nameof(ErrorVisibility)); } }
    public Visibility ErrorVisibility => string.IsNullOrWhiteSpace(ErrorText) ? Visibility.Collapsed : Visibility.Visible;
    public ProjectGitStatusModel GitStatus
    {
        get => _gitStatus;
        private set
        {
            if (SetProperty(ref _gitStatus, value)) OnPropertyChanged(nameof(GitStatusErrorVisibility));
        }
    }
    public bool GitStatusBusy { get => _gitStatusBusy; private set { if (SetProperty(ref _gitStatusBusy, value)) { OnPropertyChanged(nameof(GitStatusBusyVisibility)); _gitRefreshCommand.RaiseCanExecuteChanged(); } } }
    public Visibility GitStatusBusyVisibility => GitStatusBusy ? Visibility.Visible : Visibility.Collapsed;
    public Visibility GitStatusErrorVisibility => string.IsNullOrWhiteSpace(GitStatus.Error) ? Visibility.Collapsed : Visibility.Visible;

    public async Task RefreshAsync()
    {
        Busy = true; ErrorText = "";
        var selectedID = SelectedProject?.ProjectId;
        try
        {
            var response = await _api.GetProjectsAsync();
            Projects.Clear(); foreach (var project in response.Projects) Projects.Add(project);
            RefreshConversationChoices();
            SelectedProject = selectedID is null ? Projects.FirstOrDefault() : Projects.FirstOrDefault(project => project.ProjectId == selectedID) ?? Projects.FirstOrDefault();
        }
        catch (Exception exception) { ErrorText = UiText.UserError(exception, "读取项目"); _logs.Add("projects", exception.Message); }
        finally { Busy = false; }
    }

    public void ApplyEvent(BridgeEvent bridgeEvent)
    {
        if (!bridgeEvent.EventType.StartsWith("task.", StringComparison.OrdinalIgnoreCase) && !bridgeEvent.EventType.StartsWith("thread.", StringComparison.OrdinalIgnoreCase) && !bridgeEvent.EventType.StartsWith("openclaw.session", StringComparison.OrdinalIgnoreCase)) return;
        RefreshConversationChoices();
    }

    public async Task RefreshGitStatusAsync()
    {
        if (SelectedProject is not null) await RefreshGitStatusAsync(SelectedProject.ProjectId);
    }

    private async Task RefreshGitStatusAsync(string projectId)
    {
        if (string.IsNullOrWhiteSpace(projectId)) return;
        GitStatusBusy = true;
        try
        {
            var result = await _api.GetProjectGitStatusAsync(projectId);
            if (SelectedProject?.ProjectId == projectId) GitStatus = result;
        }
        catch (Exception exception)
        {
            if (SelectedProject?.ProjectId == projectId) GitStatus = new ProjectGitStatusModel { ProjectId = projectId, Error = exception.Message };
            _logs.Add("projects", $"读取 Git 状态失败：{exception.Message}");
        }
        finally { GitStatusBusy = false; }
    }

    private async Task SaveAsync()
    {
        Busy = true; ErrorText = "";
        try
        {
            var input = new ProjectInput
            {
                Name = Name.Trim(), Aliases = SplitList(AliasesText), WorkingDirectory = WorkingDirectory.Trim(), Description = Description.Trim(),
                DefaultBackend = DefaultBackend, DefaultConversationNumber = DefaultConversationNumber, AutoCreateConversation = AutoCreateConversation,
                ReuseStrategy = ReuseStrategy, Enabled = Enabled, Tags = SplitList(TagsText)
            };
            var project = SelectedProject is null ? await _api.CreateProjectAsync(input) : await _api.UpdateProjectAsync(SelectedProject.ProjectId, input);
            await RefreshAsync();
            SelectedProject = Projects.FirstOrDefault(item => item.ProjectId == project.ProjectId) ?? project;
        }
        catch (Exception exception) { ErrorText = UiText.UserError(exception, "保存项目"); _logs.Add("projects", $"保存项目失败：{exception.Message}"); }
        finally { Busy = false; }
    }

    private async Task DeleteAsync()
    {
        if (SelectedProject is null) return;
        Busy = true; ErrorText = "";
        try { await _api.DeleteProjectAsync(SelectedProject.ProjectId); await RefreshAsync(); }
        catch (Exception exception) { ErrorText = UiText.UserError(exception, "删除项目"); _logs.Add("projects", $"删除项目失败：{exception.Message}"); }
        finally { Busy = false; }
    }

    private void NewProject()
    {
        SelectedProject = null;
        ClearForm();
        ErrorText = "";
    }

    private void ChooseDirectory()
    {
        using var dialog = new Forms.FolderBrowserDialog { Description = "选择 Project 工作目录", UseDescriptionForTitle = true, SelectedPath = WorkingDirectory };
        if (dialog.ShowDialog() == Forms.DialogResult.OK) WorkingDirectory = dialog.SelectedPath;
    }

    private void RefreshConversationChoices()
    {
        var selected = DefaultConversationNumber;
        ConversationChoices.Clear();
        foreach (var thread in _sessions.Threads.Where(thread => thread.Number > 0)) ConversationChoices.Add(new ConversationChoice { Backend = "codex", Number = thread.Number });
        foreach (var session in _openClaw.Sessions.Where(session => session.Number > 0)) ConversationChoices.Add(new ConversationChoice { Backend = "openclaw", Number = session.Number });
        ConversationView.Refresh();
        if (selected is not null && !ConversationChoices.Any(choice => choice.Backend.Equals(DefaultBackend, StringComparison.OrdinalIgnoreCase) && choice.Number == selected)) DefaultConversationNumber = null;
    }

    private void LoadForm(ProjectModel project)
    {
        Name = project.Name; AliasesText = string.Join(", ", project.Aliases); WorkingDirectory = project.WorkingDirectory; Description = project.Description;
        _defaultBackend = project.DefaultBackend; OnPropertyChanged(nameof(DefaultBackend));
        DefaultConversationNumber = project.DefaultConversationNumber; AutoCreateConversation = project.AutoCreateConversation; ReuseStrategy = project.ReuseStrategy; Enabled = project.Enabled; TagsText = string.Join(", ", project.Tags); RefreshConversationChoices();
    }

    private void ClearForm()
    {
        Name = ""; AliasesText = ""; WorkingDirectory = ""; Description = ""; _defaultBackend = "codex"; OnPropertyChanged(nameof(DefaultBackend)); DefaultConversationNumber = null; AutoCreateConversation = false; ReuseStrategy = "default"; Enabled = true; TagsText = ""; RefreshConversationChoices();
    }

    private static List<string> SplitList(string value) => value.Split([',', ';', '\n', '\r'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
}
